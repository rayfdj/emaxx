use super::eval::{Interpreter, error_condition_value};
use super::json::{self, JsonArrayType, JsonObjectType, JsonParseOptions};
use super::sqlite;
use super::types::{Env, LispError, SharedStringState, StringPropertySpan, Value};
use crate::buffer::TextPropertySpan;
use chrono::{Datelike, FixedOffset, Local, TimeZone, Timelike, Utc};
use fancy_regex::Regex as FancyRegex;
use flate2::read::GzDecoder;
use num_bigint::{BigInt, Sign};
use num_traits::{Signed, ToPrimitive, Zero};
use regex::Regex;
use roxmltree::{Document, Node, NodeType};
use std::fs;
use std::io::ErrorKind;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{cell::RefCell, rc::Rc};
use unicode_width::UnicodeWidthChar;

const RAW_CHAR_SENTINEL: char = '\u{F8FF}';
const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
static SYSTEM_CONFIGURATION: OnceLock<String> = OnceLock::new();
const TREESIT_LINECOL_CACHE_VAR: &str = "emaxx--treesit-linecol-cache";
const BUFFER_MENU_BUFFER_NAME: &str = "*Buffer List*";
const BUFFER_MENU_ENTRIES_VAR: &str = "emaxx--buffer-menu-entries";

fn is_sqlite_builtin(name: &str) -> bool {
    matches!(
        name,
        "sqlite-open"
            | "sqlite-close"
            | "sqlite-execute"
            | "sqlite-select"
            | "sqlite-execute-batch"
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
            | "decode-time"
            | "encode-time"
            | "float-time"
            | "format-time-string"
            | "time-add"
            | "time-convert"
            | "time-equal-p"
            | "time-less-p"
            | "time-subtract"
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

pub(crate) fn prefer_builtin_override(name: &str) -> bool {
    matches!(
        name,
        "user-error" | "byte-compile" | "byte-compile-check-lambda-list"
    )
}

const RAW_BYTE8_BASE: u32 = 0x3FFF00;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CaseAction {
    Up,
    Down,
    Capitalize,
    UpcaseInitials,
}

fn is_raw_like_byte_char(code: u32) -> bool {
    matches!(code, 0x00CF | 0x00EF | 0x00FF)
}

fn normalize_case_key(key: u32) -> u32 {
    if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xFF).contains(&key) {
        key - RAW_BYTE8_BASE
    } else {
        key
    }
}

fn denormalize_case_key(template: u32, mapped: u32) -> u32 {
    if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xFF).contains(&template) && mapped <= 0xFF {
        RAW_BYTE8_BASE + mapped
    } else {
        mapped
    }
}

fn alternate_case_key(key: u32) -> Option<u32> {
    if key <= 0xFF {
        Some(RAW_BYTE8_BASE + key)
    } else if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xFF).contains(&key) {
        Some(key - RAW_BYTE8_BASE)
    } else {
        None
    }
}

fn raw_byte_regex_char(byte: u8) -> char {
    char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
        .expect("raw byte regex marker is a valid private-use character")
}

fn raw_byte_from_regex_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&code) {
        Some((code - RAW_BYTE_REGEX_BASE) as u8)
    } else {
        None
    }
}

fn is_raw_byte_regex_char(ch: char) -> bool {
    raw_byte_from_regex_char(ch).is_some()
}

fn single_char_case_mapping(iter: impl Iterator<Item = char>, fallback: u32) -> u32 {
    let mut iter = iter;
    match (iter.next(), iter.next()) {
        (Some(mapped), None) => mapped as u32,
        _ => fallback,
    }
}

fn simple_upcase_char(code: u32) -> u32 {
    let code = normalize_case_key(code);
    match code {
        0x00DF => 0x1E9E,
        0x01C4..=0x01C6 => 0x01C4,
        0x03C2 | 0x03C3 => 0x03A3,
        0x2177 => 0x2167,
        _ if is_raw_like_byte_char(code) => code,
        _ => char::from_u32(code)
            .map(|ch| single_char_case_mapping(ch.to_uppercase(), code))
            .unwrap_or(code),
    }
}

fn simple_downcase_char(code: u32, final_sigma: bool) -> u32 {
    let code = normalize_case_key(code);
    match code {
        0x1E9E => 0x00DF,
        0x0130 => 0x0069,
        0x01C4 | 0x01C5 => 0x01C6,
        0x03A3 => {
            if final_sigma {
                0x03C2
            } else {
                0x03C3
            }
        }
        0x2167 => 0x2177,
        _ if is_raw_like_byte_char(code) => code,
        _ => char::from_u32(code)
            .map(|ch| single_char_case_mapping(ch.to_lowercase(), code))
            .unwrap_or(code),
    }
}

fn simple_titlecase_char(code: u32) -> u32 {
    let code = normalize_case_key(code);
    match code {
        0x01C4..=0x01C6 => 0x01C5,
        _ => simple_upcase_char(code),
    }
}

pub(crate) fn case_table_default_value(subtype: Option<&str>, key: u32) -> Option<Value> {
    let mapped = match subtype {
        Some("case-table") => simple_downcase_char(key, false),
        Some("case-table-up") => simple_upcase_char(key),
        _ => return None,
    };
    Some(Value::Integer(denormalize_case_key(key, mapped) as i64))
}

fn current_case_table_ids(interp: &mut Interpreter) -> Result<(u64, u64), LispError> {
    let down = interp.current_case_table_id();
    let up = match interp.char_table_extra_slot(down, 0) {
        Some(Value::CharTable(id)) => id,
        _ => down,
    };
    Ok((down, up))
}

fn explicit_case_table_mapping(interp: &Interpreter, table_id: u64, code: u32) -> Option<u32> {
    for candidate in [Some(code), alternate_case_key(code)].into_iter().flatten() {
        let Some(Value::Integer(mapped)) = interp.char_table_explicit_get(table_id, candidate)
        else {
            continue;
        };
        let mapped = u32::try_from(mapped).ok()?;
        return Some(normalize_case_key(mapped));
    }
    None
}

fn case_symbols_as_words_enabled(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("case-symbols-as-words", env)
        .is_some_and(|value| value.is_truthy())
}

fn case_word_char(interp: &Interpreter, ch: char, case_symbols_as_words: bool) -> bool {
    ch.is_alphanumeric()
        || (case_symbols_as_words && ch == '_')
        || interp.is_syntax_word_char(normalize_case_key(ch as u32))
}

fn full_upcase_string(interp: &Interpreter, up_table: u64, ch: char) -> String {
    let code = ch as u32;
    if let Some(mapped) = explicit_case_table_mapping(interp, up_table, code) {
        return char::from_u32(mapped).unwrap_or(ch).to_string();
    }
    match code {
        _ if is_raw_like_byte_char(code) => ch.to_string(),
        _ => ch.to_uppercase().collect(),
    }
}

fn full_downcase_string(
    interp: &Interpreter,
    down_table: u64,
    ch: char,
    final_sigma: bool,
) -> String {
    let code = ch as u32;
    if let Some(mapped) = explicit_case_table_mapping(interp, down_table, code) {
        return char::from_u32(mapped).unwrap_or(ch).to_string();
    }
    match code {
        0x03A3 => char::from_u32(simple_downcase_char(code, final_sigma))
            .unwrap_or(ch)
            .to_string(),
        _ if is_raw_like_byte_char(code) => ch.to_string(),
        _ => ch.to_lowercase().collect(),
    }
}

fn full_titlecase_string(interp: &Interpreter, up_table: u64, ch: char) -> String {
    let code = ch as u32;
    if let Some(mapped) = explicit_case_table_mapping(interp, up_table, code) {
        return char::from_u32(mapped).unwrap_or(ch).to_string();
    }
    match code {
        0x00DF => "Ss".into(),
        0xFB01 => "Fi".into(),
        0x01C4..=0x01C6 => '\u{01C5}'.to_string(),
        _ if is_raw_like_byte_char(code) => ch.to_string(),
        _ => char::from_u32(simple_titlecase_char(code))
            .unwrap_or(ch)
            .to_string(),
    }
}

fn simple_case_char_for_action(
    interp: &Interpreter,
    down_table: u64,
    up_table: u64,
    code: u32,
    action: CaseAction,
) -> u32 {
    match action {
        CaseAction::Up => explicit_case_table_mapping(interp, up_table, code)
            .unwrap_or_else(|| simple_upcase_char(code)),
        CaseAction::Down => explicit_case_table_mapping(interp, down_table, code)
            .unwrap_or_else(|| simple_downcase_char(code, false)),
        CaseAction::Capitalize | CaseAction::UpcaseInitials => {
            explicit_case_table_mapping(interp, up_table, code)
                .unwrap_or_else(|| simple_titlecase_char(code))
        }
    }
}

fn casify_string(
    interp: &mut Interpreter,
    input: &str,
    action: CaseAction,
    env: &Env,
) -> Result<String, LispError> {
    let case_symbols_as_words = case_symbols_as_words_enabled(interp, env);
    let (down_table, up_table) = current_case_table_ids(interp)?;
    let chars: Vec<char> = input.chars().collect();
    let mut output = String::new();
    let mut in_word = false;
    for (idx, ch) in chars.iter().copied().enumerate() {
        let is_word = case_word_char(interp, ch, case_symbols_as_words);
        let next_is_word = chars
            .get(idx + 1)
            .copied()
            .is_some_and(|next| case_word_char(interp, next, case_symbols_as_words));
        let piece = match action {
            CaseAction::Up => full_upcase_string(interp, up_table, ch),
            CaseAction::Down => {
                full_downcase_string(interp, down_table, ch, in_word && !next_is_word)
            }
            CaseAction::Capitalize => {
                if is_word && !in_word {
                    full_titlecase_string(interp, up_table, ch)
                } else {
                    full_downcase_string(interp, down_table, ch, in_word && !next_is_word)
                }
            }
            CaseAction::UpcaseInitials => {
                if is_word && !in_word {
                    full_titlecase_string(interp, up_table, ch)
                } else {
                    ch.to_string()
                }
            }
        };
        output.push_str(&piece);
        in_word = is_word;
    }
    Ok(output)
}

fn casify_value(
    interp: &mut Interpreter,
    value: &Value,
    action: CaseAction,
    env: &Env,
) -> Result<Value, LispError> {
    if let Ok(integer) = value.as_integer() {
        let code = u32::try_from(integer)
            .map_err(|_| LispError::Signal(format!("Invalid character: {integer}")))?;
        let (down_table, up_table) = current_case_table_ids(interp)?;
        return Ok(Value::Integer(simple_case_char_for_action(
            interp, down_table, up_table, code, action,
        ) as i64));
    }
    let input = string_text(value)?;
    Ok(Value::String(casify_string(interp, &input, action, env)?))
}

fn replace_buffer_region_with_text(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    text: &str,
) -> Result<usize, LispError> {
    interp.buffer.goto_char(start);
    interp
        .buffer
        .delete_region(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    interp.buffer.insert(text);
    Ok(start + text.chars().count())
}

fn casify_buffer_region(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    action: CaseAction,
    env: &Env,
) -> Result<usize, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    if lo >= hi {
        return Ok(hi);
    }
    let text = interp
        .buffer
        .buffer_substring(lo, hi)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let mapped = casify_string(interp, &text, action, env)?;
    replace_buffer_region_with_text(interp, lo, hi, &mapped)
}

fn parse_region_bound(value: &Value) -> Result<(usize, usize), LispError> {
    let Value::Cons(start, end) = value else {
        return Err(LispError::Signal("Invalid region bounds".into()));
    };
    let start = start
        .as_integer()
        .map_err(|_| LispError::Signal("Invalid region bounds".into()))?;
    let end = end
        .as_integer()
        .map_err(|_| LispError::Signal("Invalid region bounds".into()))?;
    if start < 0 || end < 0 {
        return Err(LispError::Signal("Invalid region bounds".into()));
    }
    Ok((start as usize, end as usize))
}

fn parse_region_bounds(value: &Value) -> Result<Vec<(usize, usize)>, LispError> {
    let mut cursor = value.clone();
    let mut bounds = Vec::new();
    for _ in 0..1024 {
        match cursor {
            Value::Nil => return Ok(bounds),
            Value::Cons(car, cdr) => {
                bounds.push(parse_region_bound(&car)?);
                cursor = *cdr;
            }
            _ => return Err(LispError::Signal("Invalid region bounds".into())),
        }
    }
    Err(LispError::Signal("Invalid region bounds".into()))
}

fn case_word_region(interp: &Interpreter, point: usize, count: i64, env: &Env) -> (usize, usize) {
    let case_symbols_as_words = case_symbols_as_words_enabled(interp, env);
    let is_word = |ch: char| case_word_char(interp, ch, case_symbols_as_words);
    let mut cursor = point;
    let mut remaining = count.unsigned_abs();
    if count >= 0 {
        while remaining > 0 {
            while let Some(ch) = interp.buffer.char_at(cursor) {
                if is_word(ch) {
                    break;
                }
                cursor += 1;
            }
            while let Some(ch) = interp.buffer.char_at(cursor) {
                if !is_word(ch) {
                    break;
                }
                cursor += 1;
            }
            remaining -= 1;
        }
        (point, cursor)
    } else {
        while remaining > 0 {
            while cursor > interp.buffer.point_min() {
                let Some(ch) = interp.buffer.char_at(cursor - 1) else {
                    break;
                };
                if is_word(ch) {
                    break;
                }
                cursor -= 1;
            }
            while cursor > interp.buffer.point_min() {
                let Some(ch) = interp.buffer.char_at(cursor - 1) else {
                    break;
                };
                if !is_word(ch) {
                    break;
                }
                cursor -= 1;
            }
            remaining -= 1;
        }
        (cursor, point)
    }
}

/// Check if a name is a known builtin.
pub fn is_builtin(name: &str) -> bool {
    matches!(
        name,
        // Arithmetic
        "+"  | "-"
            | "*"
            | "/"
            | "%"
            | "mod"
            | "1+"
            | "1-"
            | "max"
            | "min"
            | "abs"
            | "sin"
            | "cos"
            | "tan"
            | "asin"
            | "acos"
            | "atan"
            | "copysign"
            | "isnan"
            | "exp"
            | "expt"
            | "log"
            | "sqrt"
            | "float"
            | "prefix-numeric-value"
            | "frexp"
            | "ldexp"
            | "logb"
            | "ceiling"
            | "floor"
            | "round"
            | "truncate"
            | "fceiling"
            | "ffloor"
            | "fround"
            | "ftruncate"
            | "ash"
            | "logand"
            | "logior"
            | "logxor"
            | "lognot"
            // Comparison
            | "="
            | "<"
            | ">"
            | "<="
            | ">="
            | "/="
            | "version<="
            | "emacs-version"
            // Equality
            | "eq"
            | "eql"
            | "equal"
            | "equal-including-properties"
            | "string="
            | "string-equal"
            | "string<"
            | "string-search"
            | "xor"
            // Type predicates
            | "null"
            | "not"
            | "integerp"
            | "numberp"
            | "floatp"
            | "stringp"
            | "symbolp"
            | "keywordp"
            | "functionp"
            | "compiled-function-p"
            | "closurep"
            | "commandp"
            | "boundp"
            | "fboundp"
            | "default-boundp"
            | "special-variable-p"
            | "featurep"
            | "consp"
            | "listp"
            | "bufferp"
            | "buffer-live-p"
            | "minibufferp"
            | "keymapp"
            | "zerop"
            | "natnump"
            | "atom"
            | "nlistp"
            | "characterp"
            | "markerp"
            | "recordp"
            | "charsetp"
            // List operations
            | "cons"
            | "car"
            | "cdr"
            | "car-safe"
            | "cdr-safe"
            | "list"
            | "append"
            | "nth"
            | "elt"
            | "nthcdr"
            | "last"
            | "length"
            | "reverse"
            | "copy-tree"
            | "delete-dups"
            | "memq"
            | "member"
            | "assq"
            | "assoc"
            | "alist-get"
            | "cl-set-exclusive-or"
            | "mapcar"
            | "cl-mapcar"
            | "cl-mapcan"
            | "cl-some"
            | "seq-some"
            | "mapc"
            | "cl-reduce"
            | "apply"
            | "apply-partially"
            | "funcall"
            | "funcall-interactively"
            | "call-interactively"
            | "keyboard-quit"
            | "fset"
            | "eval"
            | "define-keymap"
            | "read-event"
            | "read-char"
            | "read-char-exclusive"
            | "identity"
            | "mapconcat"
            | "kbd"
            | "key-description"
            | "single-key-description"
            | "ensure-list"
            | "seq-find"
            | "seq-contains-p"
            | "seq-take"
            | "seq-position"
            | "treesit-language-available-p"
            | "treesit--linecol-cache"
            | "treesit--linecol-cache-set"
            | "treesit--linecol-at"
            // Allocation
            | "make-string"
            | "make-vector"
            | "make-keymap"
            | "make-sparse-keymap"
            | "make-mode-line-mouse-map"
            | "vconcat"
            | "record"
            | "make-record"
            | "make-finalizer"
            // String operations
            | "concat"
            | "string"
            | "substring"
            | "substring-no-properties"
            | "string-to-multibyte"
            | "string-as-unibyte"
            | "unibyte-string"
            | "string-to-number"
            | "number-to-string"
            | "string-match"
            | "string-match-p"
            | "string-prefix-p"
            | "string-suffix-p"
            | "split-string"
            | "string-width"
            | "format"
            | "format-spec"
            | "char-to-string"
            | "string-to-char"
            | "string-to-list"
            | "string-replace"
            | "string-equal-ignore-case"
            | "replace-regexp-in-string"
            | "edmacro-parse-keys"
            | "string-bytes"
            | "byte-to-string"
            | "multibyte-string-p"
            | "multibyte-char-to-unibyte"
            | "unibyte-char-to-multibyte"
            | "upcase"
            | "downcase"
            | "capitalize"
            | "upcase-initials"
            | "get-char-code-property"
            | "char-resolve-modifiers"
            | "string-trim"
            | "url-hexify-string"
            // Buffer operations
            | "insert"
            | "insert-and-inherit"
            | "insert-char"
            | "insert-byte"
            | "insert-buffer-substring"
            | "insert-before-markers-and-inherit"
            | "point"
            | "point-min"
            | "point-max"
            | "goto-char"
            | "forward-char"
            | "backward-char"
            | "forward-word"
            | "skip-chars-forward"
            | "skip-chars-backward"
            | "skip-syntax-forward"
            | "skip-syntax-backward"
            | "beginning-of-line"
            | "end-of-line"
            | "forward-line"
            | "search-forward"
            | "search-backward"
            | "re-search-forward"
            | "re-search-backward"
            | "search-forward-regexp"
            | "forward-comment"
            | "match-string"
            | "match-beginning"
            | "match-end"
            | "buffer-string"
            | "buffer-substring"
            | "buffer-substring-no-properties"
            | "add-to-invisibility-spec"
            | "buffer-size"
            | "buffer-name"
            | "set-buffer-multibyte"
            | "toggle-enable-multibyte-characters"
            | "buffer-enable-undo"
            | "char-after"
            | "char-before"
            | "derived-mode-p"
            | "bobp"
            | "eobp"
            | "bolp"
            | "eolp"
            | "delete-region"
            | "delete-and-extract-region"
            | "delete-char"
            | "delete-forward-char"
            | "kill-word"
            | "kill-region"
            | "erase-buffer"
            | "upcase-region"
            | "downcase-region"
            | "capitalize-region"
            | "upcase-initials-region"
            | "upcase-word"
            | "downcase-word"
            | "capitalize-word"
            | "current-column"
            | "current-indentation"
            | "move-to-column"
            | "indent-rigidly"
            | "line-number-at-pos"
            | "line-beginning-position"
            | "line-end-position"
            | "pos-bol"
            | "pos-eol"
            | "narrow-to-region"
            | "widen"
            | "buffer-modified-p"
            | "set-buffer-modified-p"
            | "buffer-chars-modified-tick"
            | "buffer-modified-tick"
            | "restore-buffer-modified-p"
            | "gap-position"
            | "gap-size"
            | "position-bytes"
            | "byte-to-position"
            | "max-char"
            | "c-mode"
            | "get-pos-property"
            | "get-char-property"
            | "get-text-property"
            | "next-single-property-change"
            | "text-properties-at"
            | "put-text-property"
            | "add-text-properties"
            | "set-text-properties"
            | "remove-list-of-text-properties"
            | "font-lock-append-text-property"
            | "font-lock-prepend-text-property"
            | "font-lock--remove-face-from-text-property"
            | "put"
            | "zlib-available-p"
            | "zlib-decompress-region"
            | "libxml-parse-xml-region"
            | "compare-buffer-substrings"
            | "field-beginning"
            | "field-end"
            | "field-string-no-properties"
            | "delete-field"
            | "constrain-to-field"
            | "current-buffer"
            | "generate-new-buffer"
            | "get-buffer"
            | "get-buffer-create"
            | "generate-new-buffer-name"
            | "make-indirect-buffer"
            | "rename-buffer"
            | "other-buffer"
            | "buffer-base-buffer"
            | "buffer-swap-text"
            | "buffer-local-value"
            | "buffer-local-variables"
            | "kill-local-variable"
            | "buffer-list"
            | "list-buffers"
            | "list-buffers-noselect"
            | "Buffer-menu-buffer"
            | "decode-char"
            | "char-charset"
            | "charset-id-internal"
            | "charset-plist"
            | "charset-priority-list"
            | "charset-after"
            | "find-charset-string"
            | "find-charset-region"
            | "map-charset-chars"
            | "define-charset-internal"
            | "define-charset-alias"
            | "set-charset-plist"
            | "unify-charset"
            | "get-unused-iso-final-char"
            | "declare-equiv-charset"
            | "iso-charset"
            | "split-char"
            | "clear-charset-maps"
            | "set-charset-priority"
            | "sort-charsets"
            | "coding-system-p"
            | "check-coding-system"
            | "coding-system-priority-list"
            | "coding-system-aliases"
            | "coding-system-plist"
            | "coding-system-put"
            | "coding-system-eol-type"
            | "coding-system-base"
            | "coding-system-equal"
            | "check-coding-systems-region"
            | "detect-coding-string"
            | "detect-coding-region"
            | "find-coding-systems-region-internal"
            | "decode-sjis-char"
            | "encode-sjis-char"
            | "decode-big5-char"
            | "encode-big5-char"
            | "terminal-coding-system"
            | "set-terminal-coding-system-internal"
            | "set-safe-terminal-coding-system-internal"
            | "keyboard-coding-system"
            | "set-keyboard-coding-system-internal"
            | "find-operation-coding-system"
            | "set-coding-system-priority"
            | "define-coding-system-internal"
            | "define-coding-system-alias"
            | "char-table-p"
            | "char-table-subtype"
            | "char-table-parent"
            | "set-char-table-parent"
            | "char-table-extra-slot"
            | "set-char-table-extra-slot"
            | "char-table-range"
            | "set-char-table-range"
            | "make-category-table"
            | "category-table-p"
            | "standard-category-table"
            | "category-table"
            | "set-category-table"
            | "define-category"
            | "category-docstring"
            | "make-category-set"
            | "category-set-mnemonics"
            | "modify-category-entry"
            | "char-category-set"
            | "copy-category-table"
            | "current-case-table"
            | "standard-case-table"
            | "set-case-table"
            | "set-standard-case-table"
            | "make-syntax-table"
            | "copy-syntax-table"
            | "standard-syntax-table"
            | "modify-syntax-entry"
            | "setcdr"
            | "emaxx-default-region-extract-function"
            | "connection-local-value"
            | "propertized-buffer-identification"
            | "set-buffer"
            | "switch-to-buffer"
            | "pop-to-buffer-same-window"
            | "create-file-buffer"
            | "buffer-file-name"
            | "visited-file-modtime"
            | "set-visited-file-modtime"
            | "set-buffer-file-coding-system"
            | "find-file"
            | "find-file-noselect"
            | "file-locked-p"
            | "expand-file-name"
            | "abbreviate-file-name"
            | "files--name-absolute-system-p"
            | "substitute-in-file-name"
            | "files--use-insert-directory-program-p"
            | "file-name-directory"
            | "file-name-nondirectory"
            | "file-name-as-directory"
            | "directory-file-name"
            | "directory-name-p"
            | "file-name-absolute-p"
            | "file-name-case-insensitive-p"
            | "insert-directory-wildcard-in-dir-p"
            | "file-name-concat"
            | "file-name-unquote"
            | "file-remote-p"
            | "shell-quote-argument"
            | "locate-user-emacs-file"
            | "locate-library"
            | "ert-resource-directory"
            | "ert-resource-file"
            | "ert-fail"
            | "load"
            | "file-directory-p"
            | "file-accessible-directory-p"
            | "file-readable-p"
            | "file-exists-p"
            | "file-executable-p"
            | "file-attributes"
            | "file-attribute-type"
            | "file-attribute-link-number"
            | "file-attribute-user-id"
            | "file-attribute-group-id"
            | "file-attribute-access-time"
            | "file-attribute-modification-time"
            | "file-attribute-status-change-time"
            | "file-attribute-size"
            | "file-attribute-modes"
            | "file-attribute-inode-number"
            | "file-attribute-device-number"
            | "file-attribute-file-identifier"
            | "get-free-disk-space"
            | "file-symlink-p"
            | "make-symbolic-link"
            | "delete-file"
            | "delete-file-internal"
            | "delete-directory"
            | "delete-directory-internal"
            | "make-directory"
            | "mkdir"
            | "make-directory-internal"
            | "make-temp-file-internal"
            | "insert-directory"
            | "insert-file-contents"
            | "insert-file-contents-literally"
            | "set-binary-mode"
            | "write-region"
            | "write-file"
            | "call-process"
            | "call-process-region"
            | "process-lines"
            | "get-locale-names"
            | "shell-command"
            | "kill-buffer"
            | "lock-buffer"
            | "revert-buffer"
            | "set-mark"
            | "push-mark"
            | "mark"
            | "region-beginning"
            | "region-end"
            | "region-active-p"
            | "make-marker"
            | "copy-marker"
            | "point-marker"
            | "mark-marker"
            | "point-min-marker"
            | "point-max-marker"
            | "marker-buffer"
            | "marker-position"
            | "marker-last-position"
            | "marker-insertion-type"
            | "set-marker-insertion-type"
            | "set-marker"
            | "move-marker"
            // Output
            | "message"
            | "substitute-command-keys"
            | "prin1-to-string"
            | "princ"
            | "print"
            | "read-char-choice"
            | "y-or-n-p"
            | "yes-or-no-p"
            | "read-from-minibuffer"
            | "read-no-blanks-input"
            // Reader
            | "read"
            | "read-from-string"
            // More string/char ops
            | "char-equal"
            | "number-sequence"
            // More buffer ops
            | "following-char"
            | "preceding-char"
            | "buffer-last-name"
            // Stubs for terminal/display
            | "display-graphic-p"
            | "display-images-p"
            | "frame-parameter"
            | "frame-char-width"
            | "selected-window"
            | "window-buffer"
            | "selected-frame"
            | "windowp"
            | "window-display-table"
            | "transient-mark-mode"
            | "font-lock-mode"
            | "find-image"
            | "image-size"
            | "image-mask-p"
            | "image-metadata"
            | "imagemagick-types"
            | "init-image-library"
            | "window-start"
            | "window-end"
            | "window-width"
            | "window-text-pixel-size"
            | "get-display-property"
            | "bidi-find-overridden-directionality"
            | "redisplay"
            | "font-spec"
            | "font-get"
            | "set-face-attribute"
            | "color-distance"
            | "color-values-from-color-spec"
            | "facemenu-add-face"
            | "get-buffer-window"
            | "minibuffer-window"
            | "active-minibuffer-window"
            | "set-window-start"
            | "set-window-point"
            | "read-string"
            // Overlay operations
            | "make-overlay"
            | "overlayp"
            | "overlay-buffer"
            | "overlay-start"
            | "overlay-end"
            | "move-overlay"
            | "delete-overlay"
            | "delete-all-overlays"
            | "overlay-put"
            | "overlay-get"
            | "overlay-properties"
            | "overlays-at"
            | "overlays-in"
            | "next-overlay-change"
            | "previous-overlay-change"
            | "overlay-lists"
            | "overlay-recenter"
            | "remove-overlays"
            // Plist operations
            | "plist-get"
            | "plist-put"
            | "plist-member"
            // Sorting
            | "sort"
            | "cl-sort"
            // Misc
            | "error"
            | "user-error"
            | "signal"
            | "throw"
            | "take"
            | "add-hook"
            | "remove-hook"
            | "run-hooks"
            | "run-mode-hooks"
            | "run-hook-wrapped"
            | "mapatoms"
            | "eval-after-load"
            | "define-error"
            | "describe-function"
            | "executable-find"
            | "run-with-timer"
            | "run-with-idle-timer"
            | "cancel-timer"
            | "timerp"
            | "lossage-size"
            | "ignore"
            | "make-obsolete"
            | "make-obsolete-variable"
            | "define-obsolete-function-alias"
            | "define-obsolete-variable-alias"
            | "macroexp-warn-and-return"
            | "macroexp-quote"
            | "macroexp-progn"
            | "macroexp-compiling-p"
            | "macroexp--dynamic-variable-p"
            | "macroexpand-1"
            | "macroexpand-all"
            | "intern"
            | "intern-soft"
            | "autoload"
            | "autoloadp"
            | "custom-autoload"
            | "customize-set-variable"
            | "documentation"
            | "documentation-property"
            | "getenv"
            | "getenv-internal"
            | "user-login-name"
            | "user-full-name"
            | "symbol-value"
            | "symbol-function"
            | "symbol-name"
            | "default-value"
            | "interactive-form"
            | "macroexp-file-name"
            | "hash-table-p"
            | "char-from-name"
            | "always"
            | "evenp"
            | "seq-subseq"
            | "text-quoting-style"
            | "type-of"
            | "file-truename"
            | "save-buffer"
            | "unlock-buffer"
            | "ask-user-about-supersession-threat"
            | "advice-add"
            | "advice-remove"
            | "remove-function"
            | "userlock--handle-unlock-error"
            | "recent-auto-save-p"
            | "set-buffer-auto-saved"
            | "clear-buffer-auto-save-failure"
            | "define-key"
            | "define-key-after"
            | "keymap-set"
            | "keymap-unset"
            | "lookup-key"
            | "key-binding"
            | "keymap-lookup"
            | "keymap-parent"
            | "set-keymap-parent"
            | "suppress-keymap"
            | "use-local-map"
            | "current-local-map"
            | "copy-keymap"
            | "current-global-map"
            | "global-set-key"
            | "local-set-key"
            | "global-unset-key"
            | "local-unset-key"
            | "substitute-key-definition"
            | "easy-menu-add-item"
            | "tool-bar-local-item-from-menu"
            | "define-widget"
            | "define-button-type"
            | "defined-colors"
            | "color-defined-p"
            | "next-read-file-uses-dialog-p"
            | "auto-save-mode"
            | "do-auto-save"
            | "unix-sync"
            | "called-interactively-p"
            | "kill-all-local-variables"
            | "hack-dir-local-variables-non-file-buffer"
            | "force-mode-line-update"
            | "group-gid"
            | "group-name"
            | "random"
            | "set"
            | "set-default"
            | "get"
            | "makunbound"
            | "defvaralias"
            | "indirect-variable"
            | "internal-delete-indirect-variable"
            | "internal--define-uninitialized-variable"
            | "defvar-1"
            | "defconst-1"
            | "internal-make-var-non-special"
            | "make-symbol"
            | "make-interpreted-closure"
            | "obarray-make"
            | "make-hash-table"
            | "gethash"
            | "puthash"
            | "hash-table-count"
            | "hash-table-keys"
            | "completion-table-case-fold"
            | "try-completion"
            | "all-completions"
            | "test-completion"
            | "map-pairs"
            | "profiler-memory-running-p"
            | "profiler-memory-start"
            | "profiler-memory-stop"
            | "profiler-memory-log"
            | "profiler-cpu-running-p"
            | "profiler-cpu-start"
            | "profiler-cpu-stop"
            | "profiler-cpu-log"
            | "make-char-table"
            | "translate-region-internal"
            | "propertize"
            | "regexp-quote"
            | "regexp-opt"
            | "convert-standard-filename"
            | "vector"
            | "aref"
            | "aset"
            | "seq-every-p"
            | "seq-into"
            | "nreverse"
            | "copy-sequence"
            | "delete"
            | "delq"
            | "make-list"
            | "looking-at"
            | "looking-at-p"
            | "replace-match"
            | "replace-region-contents"
            | "transpose-regions"
            | "subst-char-in-region"
            | "flush-lines"
            | "insert-before-markers"
            | "internal--labeled-narrow-to-region"
            | "internal--labeled-widen"
            | "dabbrev-expand"
            | "encode-coding-region"
            | "decode-coding-region"
            | "encode-coding-string"
            | "decode-coding-string"
            | "json-parse-string"
            | "json-parse-buffer"
            | "json-serialize"
            | "json-insert"
            | "garbage-collect"
            | "num-processors"
            | "byte-compile"
            | "byte-compile-check-lambda-list"
            | "funcall-with-delayed-message"
            | "handler-bind-1"
            | "debugger-trap"
            | "backtrace-frame--internal"
            | "backtrace-debug"
            | "backtrace-eval"
            | "backtrace--locals"
            | "current-thread"
            | "backtrace--frames-from-thread"
            | "undo-boundary"
            | "undo"
            | "undo-more"
    ) || is_composed_accessor_name(name)
        || is_time_builtin(name)
        || is_sqlite_builtin(name)
}

/// Dispatch a builtin function call.
pub fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut super::types::Env,
) -> Result<Value, LispError> {
    // Helper: check if any argument is a float
    let has_float = |args: &[Value]| args.iter().any(|a| matches!(a, Value::Float(_)));
    let has_big_integer = |args: &[Value]| args.iter().any(|a| matches!(a, Value::BigInteger(_)));

    if sqlite::is_builtin(name) {
        return sqlite::call(interp, name, args, env);
    }

    if is_time_builtin(name) {
        return call_time_builtin(interp, name, args, env);
    }

    if matches!(
        name,
        "undo-boundary"
            | "insert"
            | "insert-char"
            | "insert-before-markers"
            | "insert-before-markers-and-inherit"
            | "delete-region"
            | "delete-and-extract-region"
            | "kill-region"
            | "delete-char"
            | "delete-forward-char"
            | "kill-word"
            | "erase-buffer"
            | "put-text-property"
            | "add-text-properties"
            | "set-text-properties"
            | "remove-list-of-text-properties"
            | "font-lock-append-text-property"
            | "font-lock-prepend-text-property"
            | "font-lock--remove-face-from-text-property"
            | "set-buffer-multibyte"
            | "write-region"
            | "save-buffer"
    ) {
        interp.reset_undo_sequence();
    }

    match name {
        // ── Arithmetic ──
        "+" => {
            if has_float(args) {
                let mut sum = 0.0;
                for a in args {
                    sum += numeric_to_f64(interp, a)?;
                }
                Ok(Value::Float(sum))
            } else {
                let mut sum = BigInt::zero();
                for a in args {
                    sum += integer_like_bigint(interp, a)?;
                }
                Ok(normalize_bigint_value(sum))
            }
        }
        "-" => {
            if args.is_empty() {
                return Ok(Value::Integer(0));
            }
            if has_float(args) {
                if args.len() == 1 {
                    return Ok(Value::Float(-numeric_to_f64(interp, &args[0])?));
                }
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    result -= numeric_to_f64(interp, a)?;
                }
                Ok(Value::Float(result))
            } else {
                if args.len() == 1 {
                    return Ok(normalize_bigint_value(-integer_like_bigint(
                        interp, &args[0],
                    )?));
                }
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    result -= integer_like_bigint(interp, a)?;
                }
                Ok(normalize_bigint_value(result))
            }
        }
        "*" => {
            if has_float(args) {
                let mut product = 1.0;
                for a in args {
                    product *= numeric_to_f64(interp, a)?;
                }
                Ok(Value::Float(product))
            } else {
                let mut product = BigInt::from(1u8);
                for a in args {
                    product *= integer_like_bigint(interp, a)?;
                }
                Ok(normalize_bigint_value(product))
            }
        }
        "/" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("/".into(), args.len()));
            }
            if has_float(args) {
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    result /= numeric_to_f64(interp, a)?;
                }
                Ok(Value::Float(result))
            } else if has_big_integer(args) {
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    let divisor = integer_like_bigint(interp, a)?;
                    if divisor.is_zero() {
                        return Err(LispError::Signal("Division by zero".into()));
                    }
                    result /= divisor;
                }
                Ok(normalize_bigint_value(result))
            } else {
                let mut result = integer_like_i64(interp, &args[0])?;
                for a in &args[1..] {
                    let divisor = integer_like_i64(interp, a)?;
                    if divisor == 0 {
                        return Err(LispError::Signal("Division by zero".into()));
                    }
                    result /= divisor;
                }
                Ok(Value::Integer(result))
            }
        }
        "%" | "mod" => {
            need_args(name, args, 2)?;
            if has_float(args) {
                let a = numeric_to_f64(interp, &args[0])?;
                let b = numeric_to_f64(interp, &args[1])?;
                if b == 0.0 {
                    return Err(LispError::Signal("Division by zero".into()));
                }
                let mut remainder = a % b;
                if remainder != 0.0 && (remainder.is_sign_negative() != b.is_sign_negative()) {
                    remainder += b;
                }
                return Ok(Value::Float(remainder));
            }
            if has_big_integer(args) {
                let a = integer_like_bigint(interp, &args[0])?;
                let b = integer_like_bigint(interp, &args[1])?;
                if b.is_zero() {
                    return Err(LispError::Signal("Division by zero".into()));
                }
                let r = ((&a % &b) + &b) % &b;
                return Ok(normalize_bigint_value(r));
            }
            let a = integer_like_i64(interp, &args[0])?;
            let b = integer_like_i64(interp, &args[1])?;
            if b == 0 {
                return Err(LispError::Signal("Division by zero".into()));
            }
            Ok(Value::Integer(a.rem_euclid(b)))
        }
        "1+" => {
            need_args(name, args, 1)?;
            if matches!(args[0], Value::Float(_)) {
                Ok(Value::Float(numeric_to_f64(interp, &args[0])? + 1.0))
            } else {
                Ok(normalize_bigint_value(
                    integer_like_bigint(interp, &args[0])? + 1,
                ))
            }
        }
        "1-" => {
            need_args(name, args, 1)?;
            if matches!(args[0], Value::Float(_)) {
                Ok(Value::Float(numeric_to_f64(interp, &args[0])? - 1.0))
            } else {
                Ok(normalize_bigint_value(
                    integer_like_bigint(interp, &args[0])? - 1,
                ))
            }
        }
        "max" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("max".into(), 0));
            }
            if has_float(args) {
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.max(numeric_to_f64(interp, a)?);
                }
                return Ok(Value::Float(result));
            }
            if has_big_integer(args) {
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.max(integer_like_bigint(interp, a)?);
                }
                return Ok(normalize_bigint_value(result));
            }
            let mut result = integer_like_i64(interp, &args[0])?;
            for a in &args[1..] {
                result = result.max(integer_like_i64(interp, a)?);
            }
            Ok(Value::Integer(result))
        }
        "min" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("min".into(), 0));
            }
            if has_float(args) {
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.min(numeric_to_f64(interp, a)?);
                }
                return Ok(Value::Float(result));
            }
            if has_big_integer(args) {
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.min(integer_like_bigint(interp, a)?);
                }
                return Ok(normalize_bigint_value(result));
            }
            let mut result = integer_like_i64(interp, &args[0])?;
            for a in &args[1..] {
                result = result.min(integer_like_i64(interp, a)?);
            }
            Ok(Value::Integer(result))
        }
        "abs" => {
            need_args(name, args, 1)?;
            if let Value::Float(value) = args[0] {
                Ok(Value::Float(value.abs()))
            } else if matches!(args[0], Value::BigInteger(_)) {
                Ok(normalize_bigint_value(
                    integer_like_bigint(interp, &args[0])?.abs(),
                ))
            } else {
                let value = integer_like_i64(interp, &args[0])?;
                match value.checked_abs() {
                    Some(abs) => Ok(Value::Integer(abs)),
                    None => Ok(normalize_bigint_value(BigInt::from(value).abs())),
                }
            }
        }
        "sin" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.sin()))
        }
        "cos" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.cos()))
        }
        "tan" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.tan()))
        }
        "asin" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.asin()))
        }
        "acos" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.acos()))
        }
        "atan" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let y = numeric_to_f64(interp, &args[0])?;
            Ok(Value::Float(if let Some(x) = args.get(1) {
                y.atan2(numeric_to_f64(interp, x)?)
            } else {
                y.atan()
            }))
        }
        "copysign" => {
            need_args(name, args, 2)?;
            Ok(Value::Float(
                numeric_to_f64(interp, &args[0])?.copysign(numeric_to_f64(interp, &args[1])?),
            ))
        }
        "isnan" => {
            need_args(name, args, 1)?;
            let value = match &args[0] {
                Value::Float(value) => *value,
                Value::Integer(_) | Value::BigInteger(_) => {
                    return Ok(Value::Nil);
                }
                _ => return Err(LispError::TypeError("number".into(), args[0].type_name())),
            };
            Ok(if value.is_nan() { Value::T } else { Value::Nil })
        }
        "exp" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.exp()))
        }
        "expt" => {
            need_args(name, args, 2)?;
            Ok(expt_value(interp, &args[0], &args[1])?)
        }
        "log" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let value = numeric_to_f64(interp, &args[0])?;
            let result = if let Some(base) = args.get(1) {
                let base = numeric_to_f64(interp, base)?;
                if base == 10.0 {
                    value.log10()
                } else if base == 2.0 {
                    value.log2()
                } else {
                    value.log(base)
                }
            } else {
                value.ln()
            };
            Ok(Value::Float(result))
        }
        "sqrt" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.sqrt()))
        }
        "float" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?))
        }
        "frexp" => {
            need_args(name, args, 1)?;
            let (sig, exp) = frexp_parts(numeric_to_f64(interp, &args[0])?);
            Ok(Value::cons(Value::Float(sig), Value::Integer(exp)))
        }
        "ldexp" => {
            need_args(name, args, 2)?;
            let significand = numeric_to_f64(interp, &args[0])?;
            let exponent = integer_like_i64(interp, &args[1])?;
            Ok(Value::Float(ldexp_value(significand, exponent)))
        }
        "logb" => {
            need_args(name, args, 1)?;
            Ok(logb_value(interp, &args[0])?)
        }
        "ceiling" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Ceiling,
            args,
            false,
        )?),
        "floor" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Floor,
            args,
            false,
        )?),
        "round" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Round,
            args,
            false,
        )?),
        "truncate" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Truncate,
            args,
            false,
        )?),
        "fceiling" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Ceiling,
            args,
            true,
        )?),
        "ffloor" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Floor,
            args,
            true,
        )?),
        "fround" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Round,
            args,
            true,
        )?),
        "ftruncate" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Truncate,
            args,
            true,
        )?),
        "ash" => {
            need_args(name, args, 2)?;
            let value = integer_like_bigint(interp, &args[0])?;
            let shift = integer_like_i64(interp, &args[1])?;
            let shifted = if shift >= 0 {
                value << shift as usize
            } else {
                value >> (-shift) as usize
            };
            Ok(normalize_bigint_value(shifted))
        }
        "logand" => {
            let mut result = BigInt::from(-1);
            for arg in args {
                result &= integer_like_bigint(interp, arg)?;
            }
            Ok(normalize_bigint_value(result))
        }
        "logior" => {
            let mut result = 0i64;
            for arg in args {
                result |= arg.as_integer()?;
            }
            Ok(Value::Integer(result))
        }
        "logxor" => {
            let mut result = BigInt::from(0);
            for arg in args {
                result ^= integer_like_bigint(interp, arg)?;
            }
            Ok(normalize_bigint_value(result))
        }
        "lognot" => {
            need_args(name, args, 1)?;
            Ok(normalize_bigint_value(!integer_like_bigint(
                interp, &args[0],
            )?))
        }
        "prefix-numeric-value" => {
            need_args(name, args, 1)?;
            prefix_numeric_value(&args[0])
        }

        // ── Comparison ──
        "=" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            for pair in args.windows(2) {
                if !numeric_eq(interp, &pair[0], &pair[1])? {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }
        "version<=" => {
            need_args(name, args, 2)?;
            Ok(
                if version_leq(&string_text(&args[0])?, &string_text(&args[1])?) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "emacs-version" => {
            need_args(name, args, 0)?;
            Ok(Value::String(emacs_version_description()))
        }
        "<" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            for pair in args.windows(2) {
                if !numeric_lt(interp, &pair[0], &pair[1])? {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }
        ">" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            for pair in args.windows(2) {
                if !numeric_gt(interp, &pair[0], &pair[1])? {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }
        "<=" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            for pair in args.windows(2) {
                if !numeric_lte(interp, &pair[0], &pair[1])? {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }
        ">=" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            for pair in args.windows(2) {
                if !numeric_gte(interp, &pair[0], &pair[1])? {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }
        "/=" => {
            need_args(name, args, 2)?;
            for index in 0..args.len() {
                for other in index + 1..args.len() {
                    if numeric_eq(interp, &args[index], &args[other])? {
                        return Ok(Value::Nil);
                    }
                }
            }
            Ok(Value::T)
        }

        // ── Equality ──
        "eq" => {
            need_args(name, args, 2)?;
            let equal = match (&args[0], &args[1]) {
                (Value::StringObject(left), Value::StringObject(right)) => Rc::ptr_eq(left, right),
                (Value::String(_), Value::String(_))
                | (Value::String(_), Value::StringObject(_))
                | (Value::StringObject(_), Value::String(_)) => false,
                _ => args[0] == args[1],
            };
            Ok(if equal { Value::T } else { Value::Nil })
        }
        "eql" => {
            need_args(name, args, 2)?;
            Ok(if args[0] == args[1] {
                Value::T
            } else {
                Value::Nil
            })
        }
        "equal" => {
            need_args(name, args, 2)?;
            Ok(if values_equal(interp, &args[0], &args[1]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "equal-including-properties" => {
            need_args(name, args, 2)?;
            Ok(if values_equal_including_properties(&args[0], &args[1]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "string=" | "string-equal" => {
            need_args(name, args, 2)?;
            let a = string_comparison_text(&args[0])?;
            let b = string_comparison_text(&args[1])?;
            Ok(if a == b { Value::T } else { Value::Nil })
        }
        "string-equal-ignore-case" => {
            need_args(name, args, 2)?;
            let a = string_text(&args[0])?;
            let b = string_text(&args[1])?;
            Ok(if a.to_lowercase() == b.to_lowercase() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "string<" => {
            need_args(name, args, 2)?;
            let a = string_comparison_text(&args[0])?;
            let b = string_comparison_text(&args[1])?;
            Ok(if a < b { Value::T } else { Value::Nil })
        }
        "string-search" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let needle = string_text(&args[0])?;
            let haystack = string_text(&args[1])?;
            let hay_chars: Vec<char> = haystack.chars().collect();
            let start = if args.len() == 3 {
                let start = args[2].as_integer()?;
                if start < 0 || start as usize > hay_chars.len() {
                    return Err(LispError::Signal("Args out of range".into()));
                }
                start as usize
            } else {
                0
            };
            if needle.is_empty() {
                return Ok(Value::Integer(start as i64));
            }
            let suffix: String = hay_chars[start..].iter().collect();
            match suffix.find(&needle) {
                Some(byte_offset) => {
                    let char_offset = suffix[..byte_offset].chars().count();
                    Ok(Value::Integer((start + char_offset) as i64))
                }
                None => Ok(Value::Nil),
            }
        }

        // ── Type predicates ──
        "null" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_nil() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "not" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_nil() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "xor" => {
            need_args(name, args, 2)?;
            Ok(if args[0].is_truthy() ^ args[1].is_truthy() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "integerp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_integer() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "numberp" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].is_integer() || matches!(args[0], Value::Float(_)) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "floatp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Float(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "stringp" => {
            need_args(name, args, 1)?;
            Ok(if string_like(&args[0]).is_some() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "symbolp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_symbol() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "keywordp" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Symbol(symbol) if symbol.starts_with(':')) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "functionp" => {
            need_args(name, args, 1)?;
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            Ok(
                if matches!(value, Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "compiled-function-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "closurep" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Lambda(_, _, _)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "keymapp" => {
            need_args(name, args, 1)?;
            Ok(if is_keymap_value(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "commandp" => {
            need_args(name, args, 1)?;
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            Ok(
                if autoload_command_p(&value) || interactive_form_items(&value).is_some() {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "boundp" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(
                if interp.lookup_var(symbol, env).is_some()
                    || matches!(
                        symbol,
                        "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum"
                    )
                    || symbol == "buffer-undo-list"
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "default-boundp" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if interp.is_default_bound(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "special-variable-p" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if interp.is_special_variable(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "fboundp" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(
                if interp.lookup_function(symbol, env).is_ok() || is_builtin(symbol) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "seq-some" => {
            need_args(name, args, 2)?;
            let predicate = args[0].clone();
            for element in args[1].to_vec()? {
                let result = call_function_value(interp, &predicate, &[element], env)?;
                if result.is_truthy() {
                    return Ok(result);
                }
            }
            Ok(Value::Nil)
        }
        "featurep" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if interp.has_feature(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "zlib-available-p" => Ok(Value::T),
        "consp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_cons() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "listp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_list() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "bufferp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Buffer(_, _)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "buffer-live-p" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Buffer(id, _) if interp.has_buffer_id(*id)) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "minibufferp" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let buffer_id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            let is_minibuffer = interp
                .get_buffer_by_id(buffer_id)
                .map(|buffer| buffer.name.starts_with(" *Minibuf"))
                .unwrap_or(false);
            Ok(if is_minibuffer { Value::T } else { Value::Nil })
        }

        "zerop" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Integer(0) => Value::T,
                Value::BigInteger(n) if n.is_zero() => Value::T,
                Value::Float(f) if *f == 0.0 => Value::T,
                _ => Value::Nil,
            })
        }

        "natnump" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Integer(n) if *n >= 0 => Value::T,
                Value::BigInteger(n) if n.sign() != Sign::Minus => Value::T,
                _ => Value::Nil,
            })
        }

        "atom" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_cons() {
                Value::Nil
            } else {
                Value::T
            })
        }

        "nlistp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_list() {
                Value::Nil
            } else {
                Value::T
            })
        }

        "characterp" => {
            need_args(name, args, 1)?;
            // In Emacs, characters are integers 0..#x3FFFFF
            Ok(match &args[0] {
                Value::Integer(n) if *n >= 0 && *n <= 0x3F_FFFF => Value::T,
                Value::BigInteger(n)
                    if n.sign() != Sign::Minus && n <= &BigInt::from(0x3F_FFFFu32) =>
                {
                    Value::T
                }
                _ => Value::Nil,
            })
        }
        "markerp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Marker(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "recordp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Record(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }

        // ── List operations ──
        "cons" => {
            need_args(name, args, 2)?;
            Ok(Value::cons(args[0].clone(), args[1].clone()))
        }
        "car" => {
            need_args(name, args, 1)?;
            args[0].car()
        }
        "cdr" => {
            need_args(name, args, 1)?;
            args[0].cdr()
        }
        "car-safe" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(car, _) => *car.clone(),
                _ => Value::Nil,
            })
        }
        "cdr-safe" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(_, cdr) => *cdr.clone(),
                _ => Value::Nil,
            })
        }
        "identity" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "list" => Ok(Value::list(args.iter().cloned())),
        "append" => {
            let mut items: Vec<Value> = Vec::new();
            for (i, a) in args.iter().enumerate() {
                if i == args.len() - 1 {
                    // Last arg can be a non-list (dotted)
                    if a.is_list() {
                        items.extend(a.to_vec()?);
                    } else {
                        // Build the list so far and cons the last as cdr
                        let mut result = a.clone();
                        for item in items.into_iter().rev() {
                            result = Value::cons(item, result);
                        }
                        return Ok(result);
                    }
                } else {
                    items.extend(a.to_vec()?);
                }
            }
            Ok(Value::list(items))
        }
        "nth" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()? as usize;
            let list = args[1].to_vec()?;
            Ok(list.get(n).cloned().unwrap_or(Value::Nil))
        }
        "elt" => {
            need_args(name, args, 2)?;
            if matches!(args[0], Value::Cons(_, _))
                && matches!(
                    args[0].to_vec().ok().and_then(|items| items.first().cloned()),
                    Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
                )
            {
                call(interp, "aref", args, env)
            } else if matches!(args[0], Value::Nil | Value::Cons(_, _)) {
                let n = args[1].as_integer()? as usize;
                let list = args[0].to_vec()?;
                Ok(list.get(n).cloned().unwrap_or(Value::Nil))
            } else {
                call(interp, "aref", args, env)
            }
        }
        "nthcdr" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()? as usize;
            let mut current = args[1].clone();
            for _ in 0..n {
                current = current.cdr()?;
            }
            Ok(current)
        }
        "last" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let n = args
                .get(1)
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1)
                .max(0) as usize;
            let items = args[0].to_vec()?;
            if items.is_empty() {
                return Ok(Value::Nil);
            }
            let start = items.len().saturating_sub(n.max(1));
            Ok(Value::list(items[start..].iter().cloned()))
        }
        "length" => {
            need_args(name, args, 1)?;
            match &args[0] {
                value if string_like(value).is_some() => {
                    Ok(Value::Integer(string_text(value)?.chars().count() as i64))
                }
                Value::Nil => Ok(Value::Integer(0)),
                Value::Cons(_, _) if is_vector_value(&args[0]) => {
                    Ok(Value::Integer(vector_items(&args[0])?.len() as i64))
                }
                Value::Cons(_, _) => Ok(Value::Integer(args[0].to_vec()?.len() as i64)),
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    Ok(Value::Integer((record.slots.len() + 1) as i64))
                }
                _ => Err(LispError::TypeError("sequence".into(), args[0].type_name())),
            }
        }
        "reverse" => {
            need_args(name, args, 1)?;
            if string_like(&args[0]).is_some() {
                reverse_string_like_value(&args[0])
            } else {
                let mut items = args[0].to_vec()?;
                items.reverse();
                Ok(Value::list(items))
            }
        }
        "copy-tree" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "delete-dups" => {
            need_args(name, args, 1)?;
            let mut deduped = Vec::new();
            for item in args[0].to_vec()? {
                if !deduped.iter().any(|existing| existing == &item) {
                    deduped.push(item);
                }
            }
            Ok(Value::list(deduped))
        }
        "memq" | "member" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            for (i, item) in items.iter().enumerate() {
                if *item == args[0] {
                    return Ok(Value::list(items[i..].iter().cloned()));
                }
            }
            Ok(Value::Nil)
        }
        "assq" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            for item in &items {
                if item.car()? == args[0] {
                    return Ok(item.clone());
                }
            }
            Ok(Value::Nil)
        }
        "assoc" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            for item in &items {
                if values_equal(interp, &item.car()?, &args[0]) {
                    return Ok(item.clone());
                }
            }
            Ok(Value::Nil)
        }
        "alist-get" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let default = args.get(2).cloned().unwrap_or(Value::Nil);
            let testfn = args.get(4);
            let items = args[1].to_vec()?;
            for item in items {
                let Value::Cons(car, cdr) = item else {
                    continue;
                };
                if value_matches_with_test(interp, &args[0], car.as_ref(), testfn, env)? {
                    return Ok(*cdr);
                }
            }
            Ok(default)
        }
        "cl-set-exclusive-or" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let left = args[0].to_vec()?;
            let right = args[1].to_vec()?;
            let mut test = Value::BuiltinFunc("equal".into());
            let mut index = 2usize;
            while index + 1 < args.len() {
                if matches!(&args[index], Value::Symbol(keyword) if keyword == ":test") {
                    test = resolve_callable(interp, &args[index + 1], env)?;
                }
                index += 2;
            }
            let mut result = Vec::new();
            for item in &left {
                if !list_contains_with(interp, &right, item, &test, env)? {
                    result.push(item.clone());
                }
            }
            for item in &right {
                if !list_contains_with(interp, &left, item, &test, env)? {
                    result.push(item.clone());
                }
            }
            Ok(Value::list(result))
        }
        "mapcar" => {
            need_args(name, args, 2)?;
            let list = sequence_values(&args[1])?;
            let mut results = Vec::new();
            for item in list {
                results.push(call_function_value(interp, &args[0], &[item], env)?);
            }
            Ok(Value::list(results))
        }
        "cl-mapcar" => {
            need_args(name, args, 2)?;
            let lists = args[1..]
                .iter()
                .map(sequence_values)
                .collect::<Result<Vec<_>, _>>()?;
            let len = lists.iter().map(Vec::len).min().unwrap_or(0);
            let mut results = Vec::with_capacity(len);
            for index in 0..len {
                let call_args = lists
                    .iter()
                    .map(|list| list[index].clone())
                    .collect::<Vec<_>>();
                results.push(call_function_value(interp, &args[0], &call_args, env)?);
            }
            Ok(Value::list(results))
        }
        "cl-mapcan" => {
            need_args(name, args, 2)?;
            let mapped = call(interp, "cl-mapcar", args, env)?.to_vec()?;
            let mut flattened = Vec::new();
            for item in mapped {
                flattened.extend(item.to_vec()?);
            }
            Ok(Value::list(flattened))
        }
        "cl-some" => {
            need_args(name, args, 2)?;
            let sequences = args[1..]
                .iter()
                .map(sequence_values)
                .collect::<Result<Vec<_>, _>>()?;
            let len = sequences.iter().map(Vec::len).min().unwrap_or(0);
            for index in 0..len {
                let call_args = sequences
                    .iter()
                    .map(|sequence| sequence[index].clone())
                    .collect::<Vec<_>>();
                let result = call_function_value(interp, &args[0], &call_args, env)?;
                if result.is_truthy() {
                    return Ok(result);
                }
            }
            Ok(Value::Nil)
        }
        "mapc" => {
            need_args(name, args, 2)?;
            let list = sequence_values(&args[1])?;
            for item in &list {
                let _ = call_function_value(interp, &args[0], std::slice::from_ref(item), env)?;
            }
            Ok(args[1].clone())
        }
        "cl-reduce" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            let Some((first, rest)) = items.split_first() else {
                return Ok(Value::Nil);
            };
            let mut acc = first.clone();
            for item in rest {
                acc = call_function_value(interp, &args[0], &[acc.clone(), item.clone()], env)?;
            }
            Ok(acc)
        }
        "eval" => eval_impl(interp, args, env),
        "mapconcat" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let list = call(interp, "mapcar", &args[..2], env)?.to_vec()?;
            let sep = if args.len() == 3 {
                let text = string_text(&args[2])?;
                let multibyte = text.chars().any(|ch| (ch as u32) > 0x7F);
                string_like(&args[2]).unwrap_or(StringLike {
                    text,
                    props: Vec::new(),
                    multibyte,
                })
            } else {
                StringLike {
                    text: String::new(),
                    props: Vec::new(),
                    multibyte: false,
                }
            };
            let mut result = String::new();
            let mut props = Vec::new();
            for (index, item) in list.iter().enumerate() {
                if index > 0 {
                    let offset = result.chars().count();
                    result.push_str(&sep.text);
                    props.extend(shift_string_props(&sep.props, offset));
                }
                if let Some(string) = string_like(item) {
                    let offset = result.chars().count();
                    result.push_str(&string.text);
                    props.extend(shift_string_props(&string.props, offset));
                } else {
                    result.push_str(&item.to_string());
                }
            }
            Ok(string_like_value(result, merge_string_props(props)))
        }
        "ensure-list" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].is_nil() || matches!(args[0], Value::Cons(_, _)) {
                    args[0].clone()
                } else {
                    Value::list([args[0].clone()])
                },
            )
        }
        "seq-find" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let predicate = resolve_callable(interp, &args[0], env)?;
            if let Ok(items) = vector_items(&args[1]) {
                for item in items {
                    if interp
                        .call_function_value(
                            predicate.clone(),
                            args[0].as_symbol().ok(),
                            std::slice::from_ref(&item),
                            env,
                        )?
                        .is_truthy()
                    {
                        return Ok(item);
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = sequence_string_like(&args[1]) {
                for ch in string.text.chars() {
                    let item = string_sequence_value(&string, ch);
                    if interp
                        .call_function_value(
                            predicate.clone(),
                            args[0].as_symbol().ok(),
                            std::slice::from_ref(&item),
                            env,
                        )?
                        .is_truthy()
                    {
                        return Ok(item);
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[1].type_name()))
            }
        }
        "seq-contains-p" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Ok(items) = vector_items(&args[0]) {
                for item in items {
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &item, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &item, &args[1])
                    };
                    if matches {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = sequence_string_like(&args[0]) {
                for ch in string.text.chars() {
                    let candidate = string_sequence_value(&string, ch);
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &candidate, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &candidate, &args[1])
                    };
                    if matches {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "seq-take" => {
            need_args(name, args, 2)?;
            let count = args[1].as_integer()?.max(0) as usize;
            if let Ok(items) = args[0].to_vec() {
                Ok(Value::list(items.into_iter().take(count)))
            } else if let Some(string) = string_like(&args[0]) {
                let text: String = string.text.chars().take(count).collect();
                let props = slice_string_props(&string.props, 0, text.chars().count());
                Ok(string_like_value(text, props))
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "seq-position" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Ok(items) = args[0].to_vec() {
                for (index, item) in items.into_iter().enumerate() {
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &item, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &item, &args[1])
                    };
                    if matches {
                        return Ok(Value::Integer(index as i64));
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = string_like(&args[0]) {
                for (index, ch) in string.text.chars().enumerate() {
                    let candidate = string_sequence_value(&string, ch);
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &candidate, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &candidate, &args[1])
                    };
                    if matches {
                        return Ok(Value::Integer(index as i64));
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "treesit-language-available-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "treesit--linecol-cache" => {
            need_args(name, args, 0)?;
            Ok(interp
                .buffer_local_value(interp.current_buffer_id(), TREESIT_LINECOL_CACHE_VAR)
                .unwrap_or_else(treesit_default_linecol_cache))
        }
        "treesit--linecol-cache-set" => {
            need_args(name, args, 3)?;
            let cache = treesit_linecol_cache_value(
                args[0].as_integer()?,
                args[1].as_integer()?,
                args[2].as_integer()?,
            );
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                TREESIT_LINECOL_CACHE_VAR,
                cache,
            );
            Ok(Value::Nil)
        }
        "treesit--linecol-at" => {
            if args.is_empty() || args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .map(|value| value.max(1) as usize)
                .unwrap_or_else(|| interp.current_buffer().point());
            treesit_linecol_at(interp, pos)
        }
        "apply" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("apply".into(), args.len()));
            }
            let func = &args[0];
            let last = &args[args.len() - 1];
            let mut all_args: Vec<Value> = args[1..args.len() - 1].to_vec();
            if let Some(string) = string_like(last) {
                all_args.extend(string_sequence_values(&string));
            } else {
                all_args.extend(vector_items(last)?);
            }
            let resolved = resolve_callable(interp, func, env)?;
            let original_name = func.as_symbol().ok();
            interp.call_function_value(resolved, original_name, &all_args, env)
        }
        "apply-partially" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let rest_name = "__emaxx-apply-partially-rest".to_string();
            let mut body = vec![Value::Symbol("apply".into()), literal_form(&args[0])];
            body.extend(args[1..].iter().map(literal_form));
            body.push(Value::Symbol(rest_name.clone()));
            Ok(Value::Lambda(
                vec!["&rest".into(), rest_name],
                vec![Value::list(body)],
                env.clone(),
            ))
        }
        "funcall" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("funcall".into(), 0));
            }
            let resolved = resolve_callable(interp, &args[0], env)?;
            let original_name = args[0].as_symbol().ok();
            interp.call_function_value(resolved, original_name, &args[1..], env)
        }
        "fset" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            if args[1].is_nil() {
                interp.set_function_binding(symbol, None);
                Ok(Value::Nil)
            } else {
                interp.set_function_binding(symbol, Some(args[1].clone()));
                Ok(args[1].clone())
            }
        }
        "funcall-interactively" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let func = resolve_callable(interp, &args[0], env)?;
            invoke_function_value(interp, &func, &args[1..], env)
        }
        "call-interactively" => call_interactively_impl(interp, args, env),
        "keyboard-quit" => Err(LispError::SignalValue(Value::list([
            Value::Symbol("quit".into()),
            Value::Nil,
        ]))),
        "define-keymap" => Ok(keymap_placeholder(None)),
        "read-event" | "read-char" | "read-char-exclusive" => {
            ensure_interaction_allowed(interp, env)?;
            let event = pop_unread_command_event(interp, env)?;
            Ok(Value::Integer(event as i64))
        }
        "read-string" | "read-from-minibuffer" | "read-no-blanks-input" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            ensure_interaction_allowed(interp, env)?;
            Ok(Value::String(String::new()))
        }

        // ── Allocation ──
        "make-string" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "make-string".into(),
                    args.len(),
                ));
            }
            let length = args[0].as_integer()?;
            if length < 0 {
                return Err(LispError::Signal("Wrong type argument: natnump".into()));
            }
            let init = args[1].as_integer()?;
            let c = char::from_u32(init as u32).unwrap_or('\0');
            let s: String = std::iter::repeat_n(c, length as usize).collect();
            Ok(Value::String(s))
        }
        "make-vector" => {
            need_args(name, args, 2)?;
            let length = args[0].as_integer()?;
            if length < 0 {
                return Err(LispError::Signal("Wrong type argument: natnump".into()));
            }
            let init = args[1].clone();
            let items: Vec<Value> = std::iter::repeat_n(init, length as usize).collect();
            // Represent as (vector el1 el2 ...) like the reader does for #(...)
            let mut result = vec![Value::symbol("vector")];
            result.extend(items);
            Ok(Value::list(result))
        }
        "make-keymap" | "make-sparse-keymap" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(make_runtime_keymap(
                interp,
                args.first()
                    .and_then(string_like)
                    .map(|string| string.text)
                    .as_deref(),
            ))
        }
        "make-mode-line-mouse-map" => {
            need_args(name, args, 2)?;
            Ok(keymap_placeholder(Some("mode-line-mouse-map")))
        }
        "vconcat" => {
            let mut items = vec![Value::symbol("vector")];
            for value in args {
                if let Ok(vector) = vector_items(value) {
                    items.extend(vector);
                    continue;
                }
                if let Some(string) = string_like(value) {
                    items.extend(string_sequence_values(&string));
                    continue;
                }
                match value {
                    Value::Nil => {}
                    Value::Cons(_, _) => items.extend(value.to_vec()?),
                    _ => {
                        return Err(LispError::TypeError("sequence".into(), value.type_name()));
                    }
                }
            }
            Ok(Value::list(items))
        }
        "copy-keymap" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Record(id)
                    if interp
                        .find_record(*id)
                        .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
                {
                    interp.copy_record(*id)
                }
                _ => Ok(args[0].clone()),
            }
        }
        "record" => {
            need_args(name, args, 1)?;
            let type_name = args[0].as_symbol()?;
            Ok(interp.create_record(type_name, args[1..].to_vec()))
        }
        "make-record" => {
            need_args(name, args, 3)?;
            let type_name = args[0].as_symbol()?;
            let length = args[1].as_integer()?;
            if length < 0 {
                return Err(LispError::Signal("Wrong type argument: natnump".into()));
            }
            Ok(interp.create_record(
                type_name,
                std::iter::repeat_n(args[2].clone(), length as usize).collect(),
            ))
        }
        "make-finalizer" => {
            need_args(name, args, 1)?;
            Ok(Value::Finalizer(interp.alloc_finalizer_id()))
        }

        // ── String operations ──
        "concat" => {
            let mut result = String::new();
            let mut props = Vec::new();
            for a in args {
                if let Some(string) = string_like(a) {
                    let offset = result.chars().count();
                    result.push_str(&string.text);
                    props.extend(shift_string_props(&string.props, offset));
                } else {
                    match a {
                        Value::Nil => {}
                        _ => result.push_str(&a.to_string()),
                    }
                }
            }
            Ok(string_like_value(result, merge_string_props(props)))
        }
        "string-match" => string_match_impl(interp, args, env, true),
        "string-match-p" => string_match_impl(interp, args, env, false),
        "string-prefix-p" | "string-suffix-p" => {
            need_arg_range(name, args, 2, 3)?;
            let affix = string_text(&args[0])?;
            let text = string_text(&args[1])?;
            let ignore_case = args.get(2).is_some_and(Value::is_truthy);
            let (affix, text) = if ignore_case {
                (affix.to_lowercase(), text.to_lowercase())
            } else {
                (affix, text)
            };
            let matches = if name == "string-prefix-p" {
                text.starts_with(&affix)
            } else {
                text.ends_with(&affix)
            };
            Ok(if matches { Value::T } else { Value::Nil })
        }
        "split-string" => {
            if args.is_empty() || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            split_string_impl(&args[0], args.get(1), args.get(2))
        }
        "string-width" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "string-width".into(),
                    args.len(),
                ));
            }
            let text = string_text(&args[0])?;
            let chars: Vec<char> = text.chars().collect();
            let len = chars.len() as i64;
            let start = normalize_string_index(args.get(1), 0, len)?;
            let end = normalize_string_index(args.get(2), len, len)?;
            if end < start {
                return Err(LispError::Signal("Args out of range".into()));
            }
            let tab_width = interp
                .lookup_var("tab-width", &Vec::new())
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(8)
                .max(1) as usize;
            let mut width = 0usize;
            for ch in chars[start as usize..end as usize].iter().copied() {
                if ch == '\t' {
                    width += tab_width;
                } else {
                    width += ch.width().unwrap_or(0);
                }
            }
            Ok(Value::Integer(width as i64))
        }
        "string" => {
            let mut result = String::new();
            for arg in args {
                let ch = arg.as_integer()?;
                let ch = char::from_u32(ch as u32)
                    .ok_or_else(|| LispError::Signal(format!("Invalid character: {ch}")))?;
                result.push(ch);
            }
            Ok(Value::String(result))
        }
        "string-to-list" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            Ok(Value::list(string_sequence_values(&string)))
        }
        "substring" | "substring-no-properties" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs("substring".into(), args.len()));
            }
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let chars: Vec<char> = string.text.chars().collect();
            let len = chars.len() as i64;
            let from = normalize_string_index(args.get(1), 0, len)? as usize;
            let to = normalize_string_index(args.get(2), len, len)? as usize;
            let props = if name == "substring-no-properties" {
                Vec::new()
            } else {
                slice_string_props(&string.props, from, to)
            };
            Ok(string_like_value(chars[from..to].iter().collect(), props))
        }
        "string-to-multibyte" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            Ok(make_shared_string_value_with_multibyte(
                string.text,
                string.props,
                true,
            ))
        }
        "string-as-unibyte" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let bytes = encode_raw_text_bytes(&string.text)?;
            Ok(bytes_to_unibyte_value(&bytes))
        }
        "unibyte-string" => {
            let bytes = args
                .iter()
                .map(|value| {
                    let byte = value.as_integer()?;
                    u8::try_from(byte).map_err(|_| LispError::Signal("Invalid byte".into()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(bytes_to_unibyte_value(&bytes))
        }
        "multibyte-char-to-unibyte" => {
            need_args(name, args, 1)?;
            let ch = args[0].as_integer()?;
            Ok(if (0..=255).contains(&ch) {
                Value::Integer(ch)
            } else {
                Value::Integer(-1)
            })
        }
        "string-to-number" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let s = string_text(&args[0])?;
            let base = match args.get(1) {
                None | Some(Value::Nil) => None,
                Some(value) => Some(value.as_integer()?),
            };
            parse_string_to_number_value(&s, base)
        }
        "number-to-string" => {
            need_args(name, args, 1)?;
            Ok(Value::String(number_to_string(&args[0])?))
        }
        "format" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("format".into(), 0));
            }
            let fmt_value = &args[0];
            let fmt = string_text(fmt_value)?;
            let mut result = String::new();
            let mut result_props = Vec::new();
            let mut arg_idx = 1;
            let chars: Vec<char> = fmt.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] != '%' || i + 1 >= chars.len() {
                    let start = result.chars().count();
                    result.push(chars[i]);
                    if let Some(props) = format_source_props(fmt_value, i, i + 1) {
                        result_props.push(TextPropertySpan {
                            start,
                            end: start + 1,
                            props,
                        });
                    }
                    i += 1;
                    continue;
                }
                let spec_start = i;
                i += 1; // skip '%'

                if chars[i] == '%' {
                    let start = result.chars().count();
                    result.push('%');
                    if let Some(props) = format_source_props(fmt_value, spec_start, i + 1) {
                        result_props.push(TextPropertySpan {
                            start,
                            end: start + 1,
                            props,
                        });
                    }
                    i += 1;
                    continue;
                }

                // Parse optional N$ positional arg
                let mut positional: Option<usize> = None;
                if chars[i].is_ascii_digit() {
                    let mut n = 0u64;
                    let digit_start = i;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        n = n
                            .saturating_mul(10)
                            .saturating_add(chars[i] as u64 - '0' as u64);
                        i += 1;
                    }
                    if i < chars.len() && chars[i] == '$' {
                        if n == 0 || n > args.len() as u64 {
                            return Err(LispError::Signal(
                                "Not enough arguments for format string".into(),
                            ));
                        }
                        positional = Some(n as usize);
                        i += 1;
                    } else {
                        i = digit_start; // not positional, rewind
                    }
                } else if chars[i] == '$' {
                    return Err(LispError::Signal("Invalid format operation %$".into()));
                } else if chars[i] == '-' {
                    // Check for %-N$s which is invalid
                    let save = i;
                    i += 1;
                    let mut has_digits = false;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        has_digits = true;
                        i += 1;
                    }
                    if has_digits && i < chars.len() && chars[i] == '$' {
                        return Err(LispError::Signal("Invalid format operation %$".into()));
                    }
                    i = save; // rewind, handle as flag below
                }

                // Parse flags
                let mut flag_hash = false;
                let mut flag_zero = false;
                let mut flag_minus = false;
                let mut flag_plus = false;
                let mut flag_space = false;
                while i < chars.len() {
                    match chars[i] {
                        '#' => flag_hash = true,
                        '0' => flag_zero = true,
                        '-' => flag_minus = true,
                        '+' => flag_plus = true,
                        ' ' => flag_space = true,
                        _ => break,
                    }
                    i += 1;
                }

                // Parse width
                let mut width: usize = 0;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    width = width * 10 + (chars[i] as usize - '0' as usize);
                    i += 1;
                }

                let mut precision = None;
                if i < chars.len() && chars[i] == '.' {
                    i += 1;
                    let mut parsed_precision = 0usize;
                    let mut saw_precision = false;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        parsed_precision =
                            parsed_precision * 10 + (chars[i] as usize - '0' as usize);
                        saw_precision = true;
                        i += 1;
                    }
                    precision = Some(if saw_precision { parsed_precision } else { 0 });
                }

                if i >= chars.len() {
                    break;
                }

                let conv = chars[i];
                i += 1;

                // Get the argument
                let aidx = if let Some(n) = positional {
                    n
                } else {
                    let idx = arg_idx;
                    arg_idx += 1;
                    idx
                };
                if aidx >= args.len() {
                    return Err(LispError::Signal(
                        "Not enough arguments for format string".into(),
                    ));
                }
                let arg = &args[aidx];

                let (mut formatted, mut formatted_props) = match conv {
                    's' => format_s_conversion(arg, precision)?,
                    'S' => (render_prin1(interp, arg, env)?, Vec::new()),
                    'd' | 'o' | 'x' | 'X' | 'b' | 'B' => (
                        format_numeric_conversion(
                            interp, arg, conv, flag_hash, flag_plus, flag_space, precision,
                        )?,
                        Vec::new(),
                    ),
                    'c' => (format_char_conversion(arg)?, Vec::new()),
                    _ => {
                        // Unknown conversion, pass through
                        if let Some(pos) = positional {
                            (format!("%{}${}", pos, conv), Vec::new())
                        } else {
                            (format!("%{}", conv), Vec::new())
                        }
                    }
                };

                // Apply width/padding
                let formatted_len = formatted.chars().count();
                if width > 0 && formatted_len < width {
                    let padding = width - formatted_len;
                    if flag_minus {
                        // Left-align: content then spaces
                        if formatted_len > 0 {
                            let trailing_props =
                                props_at_string_offset(&formatted_props, formatted_len - 1);
                            if !trailing_props.is_empty() {
                                formatted_props.push(TextPropertySpan {
                                    start: formatted_len,
                                    end: formatted_len + padding,
                                    props: trailing_props,
                                });
                            }
                        }
                        formatted.push_str(&" ".repeat(padding));
                    } else if flag_zero && !flag_minus {
                        // Zero-pad: put zeros after sign/prefix, before digits
                        // Find the split point: sign + prefix
                        let s = &formatted;
                        let mut prefix_end = 0;
                        if s.starts_with('-') || s.starts_with('+') {
                            prefix_end = 1;
                        }
                        if s[prefix_end..].starts_with("0x")
                            || s[prefix_end..].starts_with("0X")
                            || s[prefix_end..].starts_with("0b")
                            || s[prefix_end..].starts_with("0B")
                        {
                            prefix_end += 2;
                        }
                        formatted = format!(
                            "{}{}{}",
                            &s[..prefix_end],
                            "0".repeat(padding),
                            &s[prefix_end..]
                        );
                    } else {
                        formatted = format!("{}{}", " ".repeat(padding), formatted);
                        formatted_props = shift_string_props(&formatted_props, padding);
                    }
                }
                if let Some(props) = format_source_props(fmt_value, spec_start, i) {
                    formatted_props.push(TextPropertySpan {
                        start: 0,
                        end: formatted.chars().count(),
                        props,
                    });
                }
                let start = result.chars().count();
                result.push_str(&formatted);
                result_props.extend(shift_string_props(
                    &merge_string_props(formatted_props),
                    start,
                ));
            }
            Ok(string_like_value(result, merge_string_props(result_props)))
        }
        "format-spec" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let format = string_text(&args[0])?;
            let entries = args[1].to_vec()?;
            let mut result = String::new();
            let mut chars = format.chars();
            while let Some(ch) = chars.next() {
                if ch != '%' {
                    result.push(ch);
                    continue;
                }
                let Some(specifier) = chars.next() else {
                    result.push('%');
                    break;
                };
                if specifier == '%' {
                    result.push('%');
                    continue;
                }
                let replacement = entries.iter().find_map(|entry| {
                    let Value::Cons(key, value) = entry else {
                        return None;
                    };
                    let key_char = match key.as_ref() {
                        Value::Integer(code) => char::from_u32(*code as u32),
                        Value::String(text) => text.chars().next(),
                        Value::StringObject(state) => state.borrow().text.chars().next(),
                        _ => None,
                    }?;
                    if key_char == specifier {
                        Some(
                            string_like(value)
                                .map(|value| value.text)
                                .unwrap_or_else(|| value.to_string()),
                        )
                    } else {
                        None
                    }
                });
                if let Some(replacement) = replacement {
                    result.push_str(&replacement);
                } else {
                    result.push('%');
                    result.push(specifier);
                }
            }
            Ok(Value::String(result))
        }
        "char-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            let c = char::from_u32(n as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid character: {}", n)))?;
            Ok(Value::String(c.to_string()))
        }
        "string-replace" => {
            need_args(name, args, 3)?;
            let from = string_text(&args[0])?;
            let to = string_text(&args[1])?;
            let input = string_text(&args[2])?;
            Ok(Value::String(input.replace(&from, &to)))
        }
        "replace-regexp-in-string" => {
            if args.len() < 3 || args.len() > 7 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pattern = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let replacement = string_like(&args[1])
                .ok_or_else(|| LispError::TypeError("string".into(), args[1].type_name()))?;
            let source = string_like(&args[2])
                .ok_or_else(|| LispError::TypeError("string".into(), args[2].type_name()))?;
            let literal = args.get(4).is_some_and(Value::is_truthy);
            let subexp = args
                .get(5)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(0)
                .max(0) as usize;
            let source_len = source.text.chars().count() as i64;
            let start = normalize_string_index(args.get(6), 0, source_len)? as usize;
            validate_elisp_regex(&pattern.text)?;
            let regex = compile_elisp_regex(interp, &pattern, env, "")?;
            let mut result = source.text.chars().take(start).collect::<String>();
            let mut search_pos = start;
            let mut tail = source.text.chars().skip(start).collect::<String>();

            while let Some(captures) = regex
                .captures(&tail)
                .map_err(|error| LispError::Signal(error.to_string()))?
            {
                let Some(full_match) = captures.get(0) else {
                    break;
                };
                let full_start = search_pos + tail[..full_match.start()].chars().count();
                let full_end = search_pos + tail[..full_match.end()].chars().count();
                let match_data = (0..captures.len())
                    .map(|index| {
                        captures.get(index).map(|matched| {
                            (
                                search_pos + tail[..matched.start()].chars().count(),
                                search_pos + tail[..matched.end()].chars().count(),
                            )
                        })
                    })
                    .collect::<Vec<_>>();
                let (replace_start, replace_end) = match_data
                    .get(subexp)
                    .and_then(|entry| *entry)
                    .or_else(|| match_data.first().and_then(|entry| *entry))
                    .ok_or_else(|| LispError::Signal("No previous search".into()))?;

                result.push_str(&slice_string_chars(&source.text, search_pos, replace_start));
                result.push_str(&expand_replace_match_text(
                    &replacement.text,
                    &match_data,
                    literal,
                    &source.text,
                )?);
                result.push_str(&slice_string_chars(&source.text, replace_end, full_end));

                if full_start == full_end {
                    if let Some(ch) = tail.chars().next() {
                        result.push(ch);
                        search_pos += 1;
                        tail = tail.chars().skip(1).collect();
                        continue;
                    }
                    break;
                }

                search_pos = full_end;
                tail = source.text.chars().skip(search_pos).collect();
            }

            result.push_str(&tail);
            Ok(Value::String(result))
        }
        "edmacro-parse-keys" => {
            need_arg_range(name, args, 1, 2)?;
            parse_edmacro_key_sequence(&string_text(&args[0])?)
        }
        "string-trim" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?.trim().to_string()))
        }
        "url-hexify-string" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let input = string_text(&args[0])?;
            let allowed = args
                .get(1)
                .and_then(string_like)
                .map(|allowed| allowed.text)
                .unwrap_or_default();
            let mut output = String::new();
            for ch in input.chars() {
                if ch.is_ascii_alphanumeric()
                    || matches!(ch, '-' | '_' | '.' | '~')
                    || allowed.contains(ch)
                {
                    output.push(ch);
                } else {
                    for byte in ch.to_string().bytes() {
                        output.push('%');
                        output.push_str(&format!("{byte:02X}"));
                    }
                }
            }
            Ok(Value::String(output))
        }
        "byte-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            if !(0..=255).contains(&n) {
                return Err(LispError::Signal("Byte value out of range".into()));
            }
            let c = char::from_u32(n as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid byte: {}", n)))?;
            Ok(Value::String(c.to_string()))
        }
        "string-to-char" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            Ok(string
                .text
                .chars()
                .next()
                .map(|ch| string_sequence_value(&string, ch))
                .unwrap_or(Value::Integer(0)))
        }
        "string-bytes" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(string_text(&args[0])?.len() as i64))
        }
        "multibyte-string-p" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            Ok(if string.multibyte {
                Value::T
            } else {
                Value::Nil
            })
        }
        "unibyte-char-to-multibyte" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            if !(0..=255).contains(&n) {
                return Err(LispError::Signal("Byte value out of range".into()));
            }
            Ok(Value::Integer(n))
        }
        "upcase" => {
            need_args(name, args, 1)?;
            casify_value(interp, &args[0], CaseAction::Up, env)
        }
        "downcase" => {
            need_args(name, args, 1)?;
            casify_value(interp, &args[0], CaseAction::Down, env)
        }
        "capitalize" => {
            need_args(name, args, 1)?;
            casify_value(interp, &args[0], CaseAction::Capitalize, env)
        }
        "upcase-initials" => {
            need_args(name, args, 1)?;
            casify_value(interp, &args[0], CaseAction::UpcaseInitials, env)
        }
        "get-char-code-property" => {
            need_args(name, args, 2)?;
            let ch = u32::try_from(args[0].as_integer()?)
                .map_err(|_| LispError::Signal("Invalid character".into()))?;
            let property = args[1].as_symbol()?;
            let value = match (normalize_case_key(ch), property) {
                (code, "uppercase") => {
                    if code == 0x00DF {
                        Value::Nil
                    } else {
                        let mapped = simple_upcase_char(code);
                        if mapped == code {
                            Value::Nil
                        } else {
                            Value::Integer(mapped as i64)
                        }
                    }
                }
                (code, "lowercase") => {
                    let mapped = simple_downcase_char(code, false);
                    if mapped == code {
                        Value::Nil
                    } else {
                        Value::Integer(mapped as i64)
                    }
                }
                (code, "titlecase") => {
                    if code == 0x00DF {
                        Value::Nil
                    } else if code == 0x01C5 {
                        Value::Integer(code as i64)
                    } else {
                        let mapped = simple_titlecase_char(code);
                        if mapped == code {
                            Value::Nil
                        } else {
                            Value::Integer(mapped as i64)
                        }
                    }
                }
                (0x00DF, "special-uppercase") => Value::String("SS".into()),
                (0x00DF, "special-titlecase") => Value::String("Ss".into()),
                (0x00DF, "special-lowercase") => Value::Nil,
                (0x00DF, _) => Value::Nil,
                (0x00CF, _) | (0x00EF, _) | (0x00FF, _) => Value::Nil,
                (0x0130, "special-lowercase") => Value::String("i\u{307}".into()),
                (0x0130, _) => Value::Nil,
                (0xFB01, "special-uppercase") => Value::String("FI".into()),
                (0xFB01, "special-titlecase") => Value::String("Fi".into()),
                (0xFB01, _) => Value::Nil,
                _ => Value::Nil,
            };
            Ok(value)
        }
        "char-resolve-modifiers" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(resolve_char_modifiers(
                args[0].as_integer()?,
            )))
        }

        // ── Buffer operations ──
        "insert" => insert_impl(interp, args, env, false, false),
        "insert-and-inherit" => insert_impl(interp, args, env, true, false),
        "insert-char" => insert_char_impl(interp, args, env),
        "insert-byte" => {
            need_args(name, args, 2)?;
            let byte = args[0].as_integer()?;
            if !(0..=255).contains(&byte) {
                return Err(LispError::Signal("Byte value out of range".into()));
            }
            let count = args[1].as_integer()?.max(0) as usize;
            let c = char::from_u32(byte as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid byte: {}", byte)))?;
            let text: String = std::iter::repeat_n(c, count).collect();
            insert_text_with_hooks(interp, &text, &[], false, false, env)?;
            Ok(Value::Nil)
        }
        "insert-buffer-substring" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let buffer_id = interp.resolve_buffer_id(&args[0])?;
            let source = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
            let start = if args.len() > 1 {
                position_from_value(interp, &args[1])?
            } else {
                source.point_min()
            };
            let end = if args.len() > 2 {
                position_from_value(interp, &args[2])?
            } else {
                source.point_max()
            };
            let text = source
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let props = source.substring_property_spans(start, end);
            insert_text_with_hooks(interp, &text, &props, false, false, env)?;
            Ok(Value::Nil)
        }
        "point" => Ok(Value::Integer(interp.buffer.point() as i64)),
        "point-min" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
        "point-max" => Ok(Value::Integer(interp.buffer.point_max() as i64)),
        "goto-char" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            interp.buffer.goto_char(pos);
            Ok(Value::Integer(interp.buffer.point() as i64))
        }
        "forward-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            match interp.buffer.forward_char(n as isize) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "forward-word" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let case_symbols_as_words = case_symbols_as_words_enabled(interp, env);
            let syntax_word_chars = interp.syntax_word_chars();
            let is_word = |ch: char| {
                ch.is_alphanumeric()
                    || (case_symbols_as_words && ch == '_')
                    || syntax_word_chars
                        .iter()
                        .any(|code| *code == normalize_case_key(ch as u32))
            };
            let forward = n >= 0;
            let mut remaining = n.unsigned_abs();
            while remaining > 0 {
                if forward {
                    while let Some(ch) = interp.buffer.char_at(interp.buffer.point()) {
                        if is_word(ch) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(1);
                    }
                    while let Some(ch) = interp.buffer.char_at(interp.buffer.point()) {
                        if !is_word(ch) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(1);
                    }
                } else {
                    while interp.buffer.point() > interp.buffer.point_min() {
                        if matches!(interp.buffer.char_before(), Some(ch) if is_word(ch)) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(-1);
                    }
                    while interp.buffer.point() > interp.buffer.point_min() {
                        if !matches!(interp.buffer.char_before(), Some(ch) if is_word(ch)) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(-1);
                    }
                }
                remaining -= 1;
            }
            Ok(Value::Nil)
        }
        "skip-chars-forward" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            skip_chars_forward_impl(interp, &args[0], args.get(1))
        }
        "skip-chars-backward" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            skip_chars_backward_impl(interp, &args[0], args.get(1))
        }
        "skip-syntax-forward" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            skip_syntax_impl(interp, &args[0], args.get(1), true)
        }
        "skip-syntax-backward" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            skip_syntax_impl(interp, &args[0], args.get(1), false)
        }
        "backward-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            match interp.buffer.forward_char(-(n as isize)) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "beginning-of-line" => {
            interp.buffer.beginning_of_line();
            Ok(Value::Nil)
        }
        "end-of-line" => {
            interp.buffer.end_of_line();
            Ok(Value::Nil)
        }
        "forward-line" => {
            let n = if args.is_empty() {
                BigInt::from(1u8)
            } else {
                integer_like_bigint(interp, &args[0])?
            };
            Ok(normalize_bigint_value(forward_line_bigint(
                &mut interp.buffer,
                n,
            )))
        }
        "search-forward" | "search-backward" => {
            if args.is_empty() || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let needle = string_text(&args[0])?;
            let haystack = interp.buffer.buffer_string();
            let point = interp.buffer.point();
            let noerror = args.get(2).is_some_and(Value::is_truthy);
            let result = if name == "search-forward" {
                let offset = haystack
                    .chars()
                    .take(point.saturating_sub(1))
                    .map(|ch| ch.len_utf8())
                    .sum::<usize>();
                haystack[offset..].find(&needle).map(|found| {
                    let match_start_chars = haystack[offset..offset + found].chars().count();
                    let match_end_chars = haystack[offset..offset + found + needle.len()]
                        .chars()
                        .count();
                    (point + match_start_chars, point + match_end_chars)
                })
            } else {
                let prefix: String = haystack.chars().take(point.saturating_sub(1)).collect();
                prefix.rfind(&needle).map(|found| {
                    let start = prefix[..found].chars().count() + 1;
                    let end = start + needle.chars().count();
                    (start, end)
                })
            };
            match result {
                Some((start, end)) => {
                    interp.last_match_data = Some(vec![Some((start, end))]);
                    let point = if name == "search-backward" {
                        start
                    } else {
                        end
                    };
                    interp.buffer.goto_char(point);
                    Ok(Value::Integer(point as i64))
                }
                None if noerror => Ok(Value::Nil),
                None => Err(LispError::SignalValue(Value::list([
                    Value::Symbol("search-failed".into()),
                    Value::String(needle),
                ]))),
            }
        }
        "re-search-forward" | "search-forward-regexp" => {
            buffer_regex_search(interp, args, env, true)
        }
        "re-search-backward" => buffer_regex_search(interp, args, env, false),
        "forward-comment" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            forward_comment_impl(interp, args.first(), env)
        }
        "buffer-string" => Ok(string_like_value(
            interp.buffer.buffer_string(),
            interp
                .buffer
                .substring_property_spans(interp.buffer.point_min(), interp.buffer.point_max()),
        )),
        "buffer-substring" | "buffer-substring-no-properties" => {
            need_args(name, args, 2)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            match interp.buffer.buffer_substring(from, to) {
                Ok(s) => {
                    if name == "buffer-substring" {
                        Ok(string_like_value(
                            s,
                            interp.buffer.substring_property_spans(from, to),
                        ))
                    } else {
                        Ok(Value::String(s))
                    }
                }
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "add-to-invisibility-spec" => {
            need_args(name, args, 1)?;
            let current = interp
                .lookup_var("buffer-invisibility-spec", env)
                .unwrap_or(Value::T);
            let updated = match current {
                Value::Nil => Value::list([args[0].clone()]),
                Value::T => Value::list([Value::T, args[0].clone()]),
                other => {
                    let mut items = other.to_vec()?;
                    if !items.iter().any(|item| item == &args[0]) {
                        items.push(args[0].clone());
                    }
                    Value::list(items)
                }
            };
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-invisibility-spec",
                updated,
            );
            Ok(Value::Nil)
        }
        "derived-mode-p" => {
            if args.is_empty() {
                return Ok(Value::Nil);
            }
            let current_mode = interp
                .lookup_var("major-mode", env)
                .and_then(|value| value.as_symbol().ok().map(str::to_string));
            Ok(
                if args.iter().any(|value| {
                    value
                        .as_symbol()
                        .ok()
                        .zip(current_mode.as_deref())
                        .is_some_and(|(candidate, current)| candidate == current)
                }) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "buffer-size" => Ok(Value::Integer(interp.buffer.buffer_size() as i64)),
        "buffer-enable-undo" => {
            interp.buffer.enable_undo();
            Ok(Value::Nil)
        }
        "gap-position" => Ok(Value::Integer(interp.buffer.point() as i64)),
        "gap-size" => Ok(Value::Integer(0)),
        "max-char" => Ok(Value::Integer(0x3F_FFFF)),
        "position-bytes" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            Ok(position_bytes(interp, pos)
                .map(|byte_pos| Value::Integer(byte_pos as i64))
                .unwrap_or(Value::Nil))
        }
        "byte-to-position" => {
            need_args(name, args, 1)?;
            let byte = args[0].as_integer()?;
            if byte <= 0 {
                return Ok(Value::Nil);
            }
            Ok(byte_to_position(interp, byte as usize)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "c-mode" => {
            if !interp.has_feature("newcomment")
                && interp.resolve_load_target("newcomment").is_some()
            {
                interp.load_target("newcomment")?;
            }
            let buffer_id = interp.current_buffer_id();
            interp.set_buffer_local_value(buffer_id, "major-mode", Value::Symbol("c-mode".into()));
            interp.set_buffer_local_value(buffer_id, "mode-name", Value::String("C".into()));
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("/* ".into()));
            interp.set_buffer_local_value(buffer_id, "comment-end", Value::String(" */".into()));
            interp.set_buffer_local_value(
                buffer_id,
                "comment-start-skip",
                Value::String("\\(?://+\\|/\\*+\\)\\s *".into()),
            );
            interp.set_buffer_local_value(
                buffer_id,
                "comment-end-skip",
                Value::String("[ \t]*\\*+/".into()),
            );
            interp.set_buffer_local_value(buffer_id, "comment-use-syntax", Value::Nil);
            interp.set_buffer_local_value(
                buffer_id,
                "comment-style",
                Value::Symbol("indent".into()),
            );
            interp.set_buffer_local_value(buffer_id, "comment-multi-line", Value::T);
            Ok(Value::Nil)
        }
        "buffer-name" => {
            if !args.is_empty()
                && let Value::Buffer(_, name) = &args[0]
            {
                return Ok(Value::String(name.clone()));
            }
            Ok(Value::String(interp.buffer.name.clone()))
        }
        "set-buffer-multibyte" => {
            let enabled = args.first().is_none_or(Value::is_truthy);
            interp.buffer.set_multibyte(enabled);
            interp
                .buffer
                .push_undo_entry(crate::buffer::UndoEntry::Combined {
                    display: Value::Nil,
                    entries: Vec::new(),
                });
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "toggle-enable-multibyte-characters" => {
            let enabled = !interp.buffer.is_multibyte();
            interp.buffer.set_multibyte(enabled);
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "char-after" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                args[0].as_integer()? as usize
            };
            match interp.buffer.char_at(pos) {
                Some(c) => Ok(Value::Integer(c as i64)),
                None => Ok(Value::Nil),
            }
        }
        "char-before" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                args[0].as_integer()? as usize
            };
            if pos <= interp.buffer.point_min() {
                Ok(Value::Nil)
            } else {
                match interp.buffer.char_at(pos - 1) {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Nil),
                }
            }
        }
        "bobp" => Ok(if interp.buffer.bobp() {
            Value::T
        } else {
            Value::Nil
        }),
        "eobp" => Ok(if interp.buffer.eobp() {
            Value::T
        } else {
            Value::Nil
        }),
        "bolp" => Ok(if interp.buffer.bolp() {
            Value::T
        } else {
            Value::Nil
        }),
        "eolp" => Ok(if interp.buffer.eolp() {
            Value::T
        } else {
            Value::Nil
        }),
        "delete-region" => {
            need_args(name, args, 2)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            ensure_region_modifiable(interp, from, to, env)?;
            delete_region_with_hooks(interp, from, to, env)?;
            Ok(Value::Nil)
        }
        "delete-and-extract-region" => {
            need_args(name, args, 2)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            ensure_region_modifiable(interp, from, to, env)?;
            Ok(string_like_value(
                delete_region_with_hooks(interp, from, to, env)?,
                Vec::new(),
            ))
        }
        "kill-region" => call(interp, "delete-region", args, env),
        "delete-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let point = interp.buffer.point();
            if n >= 0 {
                let to = point + n as usize;
                if to > interp.buffer.point_max() {
                    Err(LispError::Signal("End of buffer".into()))
                } else {
                    delete_region_with_hooks(interp, point, to, env)?;
                    Ok(Value::Nil)
                }
            } else {
                let count = (-n) as usize;
                if point < interp.buffer.point_min() + count {
                    Err(LispError::Signal("Beginning of buffer".into()))
                } else {
                    delete_region_with_hooks(interp, point - count, point, env)?;
                    Ok(Value::Nil)
                }
            }
        }
        "delete-forward-char" => {
            if interp.buffer.mark_active()
                && interp
                    .lookup_var("transient-mark-mode", env)
                    .is_some_and(|value| value.is_truthy())
                && let Some((start, end)) = interp.buffer.region()
            {
                interp.buffer.deactivate_mark();
                return call(
                    interp,
                    "delete-region",
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                );
            }
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            call(interp, "delete-char", &[Value::Integer(n)], env)
        }
        "kill-word" => {
            let count = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let start = interp.buffer.point();
            call(interp, "forward-word", &[Value::Integer(count)], env)?;
            let end = interp.buffer.point();
            interp.buffer.goto_char(start);
            call(
                interp,
                "delete-region",
                &[Value::Integer(start as i64), Value::Integer(end as i64)],
                env,
            )
        }
        "erase-buffer" => {
            let size = interp.buffer.buffer_size();
            if size > 0 {
                let min = interp.buffer.point_min();
                let max = interp.buffer.point_max();
                delete_region_with_hooks(interp, min, max, env)?;
            }
            Ok(Value::Nil)
        }
        "upcase-region" | "downcase-region" | "capitalize-region" | "upcase-initials-region" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let action = match name {
                "upcase-region" => CaseAction::Up,
                "downcase-region" => CaseAction::Down,
                "capitalize-region" => CaseAction::Capitalize,
                _ => CaseAction::UpcaseInitials,
            };
            if args.get(2).is_some_and(Value::is_truthy) {
                let extractor = interp
                    .lookup_var("region-extract-function", env)
                    .ok_or_else(|| LispError::Void("region-extract-function".into()))?;
                let bounds = call_function_value(
                    interp,
                    &extractor,
                    &[Value::Symbol("bounds".into())],
                    env,
                )?;
                for (start, end) in parse_region_bounds(&bounds)? {
                    casify_buffer_region(interp, start, end, action, env)?;
                }
                Ok(Value::Nil)
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                casify_buffer_region(interp, start, end, action, env)?;
                Ok(Value::Nil)
            }
        }
        "upcase-word" | "downcase-word" | "capitalize-word" => {
            need_args(name, args, 1)?;
            let action = match name {
                "upcase-word" => CaseAction::Up,
                "downcase-word" => CaseAction::Down,
                _ => CaseAction::Capitalize,
            };
            let count = args[0].as_integer()?;
            let point = interp.buffer.point();
            let (start, end) = case_word_region(interp, point, count, env);
            let new_end = casify_buffer_region(interp, start, end, action, env)?;
            if count >= 0 {
                interp.buffer.goto_char(new_end);
            } else {
                interp.buffer.goto_char(point);
            }
            Ok(Value::Nil)
        }
        "current-column" => {
            let pt = interp.buffer.point();
            let bol = {
                let saved = interp.buffer.point();
                interp.buffer.beginning_of_line();
                let bol = interp.buffer.point();
                interp.buffer.goto_char(saved);
                bol
            };
            Ok(Value::Integer(column_at(interp, env, bol, pt) as i64))
        }
        "current-indentation" => {
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            while matches!(
                interp.buffer.char_at(interp.buffer.point()),
                Some(' ' | '\t')
            ) {
                let _ = interp.buffer.forward_char(1);
            }
            let pt = interp.buffer.point();
            let bol = {
                let saved = interp.buffer.point();
                interp.buffer.beginning_of_line();
                let bol = interp.buffer.point();
                interp.buffer.goto_char(saved);
                bol
            };
            let indentation = column_at(interp, env, bol, pt) as i64;
            interp.buffer.goto_char(saved);
            Ok(Value::Integer(indentation))
        }
        "move-to-column" => {
            need_args(name, args, 1)?;
            let target = args[0].as_integer()?.max(0) as usize;
            let force = args.get(1).is_some_and(Value::is_truthy);
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            let start = interp.buffer.point();
            interp.buffer.goto_char(saved);
            let mut pos = start;
            while pos < interp.buffer.point_max() {
                let current_col = column_at(interp, env, start, pos);
                if current_col >= target {
                    break;
                }
                let Some(ch) = interp.buffer.char_at(pos) else {
                    break;
                };
                if ch == '\n' {
                    break;
                }
                let next_col = column_after(interp, env, current_col, pos, ch);
                if next_col > target && force && ch == '\t' && !char_is_invisible(interp, pos) {
                    interp.buffer.goto_char(pos);
                    interp.insert_current_buffer(&" ".repeat(target - current_col));
                    pos = interp.buffer.point();
                    break;
                }
                pos += 1;
            }
            if force {
                let current_col = column_at(interp, env, start, pos);
                if current_col < target {
                    interp.buffer.goto_char(pos);
                    interp.insert_current_buffer(&" ".repeat(target - current_col));
                    pos = interp.buffer.point();
                }
            }
            interp.buffer.goto_char(pos);
            Ok(Value::Integer(column_at(interp, env, start, pos) as i64))
        }
        "indent-rigidly" => {
            need_arg_range(name, args, 3, 4)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let count = args[2].as_integer()?;
            let text = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let adjusted = if count > 0 {
                let prefix = " ".repeat(count as usize);
                text.split_inclusive('\n')
                    .map(|line| format!("{prefix}{line}"))
                    .collect::<String>()
            } else if count < 0 {
                let mut adjusted = String::new();
                for line in text.split_inclusive('\n') {
                    let mut remove = (-count) as usize;
                    let mut start_idx = 0usize;
                    for (index, ch) in line.char_indices() {
                        if remove == 0 || !matches!(ch, ' ' | '\t') {
                            start_idx = index;
                            break;
                        }
                        remove -= 1;
                        start_idx = index + ch.len_utf8();
                    }
                    adjusted.push_str(&line[start_idx..]);
                }
                adjusted
            } else {
                text
            };
            interp
                .delete_region_current_buffer(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            interp.buffer.goto_char(start);
            interp.insert_current_buffer(&adjusted);
            Ok(Value::Nil)
        }
        "line-number-at-pos" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                args[0].as_integer()? as usize
            };
            Ok(Value::Integer(interp.buffer.line_number_at_pos(pos) as i64))
        }
        "line-beginning-position" | "pos-bol" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let saved = interp.buffer.point();
            let count = (n - 1) as isize;
            let shortage = if count != 0 {
                interp.buffer.forward_line(count)
            } else {
                0
            };
            // If forward_line overshot (couldn't find enough lines),
            // point is already at point-max/point-min — don't move it back.
            if shortage == 0 || (count > 0 && interp.buffer.point() < interp.buffer.point_max()) {
                interp.buffer.beginning_of_line();
            }
            let result = interp.buffer.point();
            interp.buffer.goto_char(saved);
            Ok(Value::Integer(result as i64))
        }
        "line-end-position" | "pos-eol" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let saved = interp.buffer.point();
            let count = (n - 1) as isize;
            if count != 0 {
                interp.buffer.forward_line(count);
            }
            interp.buffer.end_of_line();
            let result = interp.buffer.point();
            interp.buffer.goto_char(saved);
            Ok(Value::Integer(result as i64))
        }
        "narrow-to-region" => {
            need_args(name, args, 2)?;
            let mut start = args[0].as_integer()? as usize;
            let mut end = args[1].as_integer()? as usize;
            if let Some((clamp_start, clamp_end)) =
                interp.effective_labeled_restriction(interp.current_buffer_id(), None)
            {
                start = start.max(clamp_start);
                end = end.min(clamp_end);
            } else if let Some(active) =
                interp.lookup_var("__emaxx-active-labeled-restriction", env)
            {
                let values = active.to_vec()?;
                let clamp_start = values
                    .first()
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or(1) as usize;
                let clamp_end = values
                    .get(1)
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or((interp.buffer.size_total() + 1) as i64)
                    as usize;
                start = start.max(clamp_start);
                end = end.min(clamp_end);
            }
            interp.buffer.narrow_to_region(start, end);
            Ok(Value::Nil)
        }
        "widen" => {
            if let Some((start, end)) =
                interp.effective_labeled_restriction(interp.current_buffer_id(), None)
            {
                interp.buffer.narrow_to_region(start, end);
            } else {
                interp.buffer.widen();
            }
            Ok(Value::Nil)
        }
        "buffer-modified-p" => Ok(if interp.buffer.is_autosaved() {
            Value::Symbol("autosaved".into())
        } else if interp.buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        }),
        "buffer-chars-modified-tick" | "buffer-modified-tick" => {
            Ok(Value::Integer(interp.buffer.modified_tick() as i64))
        }
        "set-buffer-modified-p" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                interp.buffer.set_unmodified();
            } else {
                interp.buffer.set_modified();
                let _ = maybe_lock_current_buffer(interp, env);
            }
            Ok(Value::Nil)
        }
        "restore-buffer-modified-p" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                interp.buffer.set_unmodified();
            } else if matches!(&args[0], Value::Symbol(symbol) if symbol == "autosaved") {
                interp.buffer.set_modified();
                interp.buffer.set_autosaved();
            } else {
                interp.buffer.set_modified();
            }
            Ok(Value::Nil)
        }
        "get-pos-property" | "get-char-property" => {
            need_args(name, args, 2)?;
            let pos = args[0].as_integer()? as usize;
            let prop = args[1].as_symbol()?.to_string();
            let buffer_id = if let Some(object) = args.get(2) {
                if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                }
            } else {
                interp.current_buffer_id()
            };
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
            Ok(
                highest_priority_overlay_property(buffer, pos, &prop, name == "get-pos-property")
                    .or_else(|| buffer.text_property_at(pos, &prop))
                    .unwrap_or(Value::Nil),
            )
        }
        "get-text-property" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args[0].as_integer()? as usize;
            let prop = args[1].as_symbol()?.to_string();
            if let Some(object) = args.get(2) {
                if string_like(object).is_some() {
                    Ok(string_property_at(object, pos, &prop).unwrap_or(Value::Nil))
                } else {
                    let buffer_id = if object.is_nil() {
                        interp.current_buffer_id()
                    } else {
                        interp.resolve_buffer_id(object)?
                    };
                    let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", buffer_id))
                    })?;
                    Ok(buffer.text_property_at(pos, &prop).unwrap_or(Value::Nil))
                }
            } else {
                Ok(interp
                    .buffer
                    .text_property_at(pos, &prop)
                    .unwrap_or(Value::Nil))
            }
        }
        "next-single-property-change" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args[0].as_integer()?.max(1) as usize;
            let prop = args[1].as_symbol()?.to_string();
            let object = args.get(2).unwrap_or(&Value::Nil);
            let limit = args
                .get(3)
                .filter(|value| !value.is_nil())
                .map(|value| value.as_integer().map(|value| value.max(1) as usize))
                .transpose()?;
            let (initial, max_pos) = if string_like(object).is_some() {
                let text = string_text(object)?;
                let text_len = text.chars().count() + 1;
                (
                    string_property_at(object, pos, &prop).unwrap_or(Value::Nil),
                    limit.unwrap_or(text_len),
                )
            } else {
                let buffer_id = if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                (
                    buffer.text_property_at(pos, &prop).unwrap_or(Value::Nil),
                    limit.unwrap_or(buffer.point_max()),
                )
            };
            for cursor in pos.saturating_add(1)..max_pos {
                let current = if string_like(object).is_some() {
                    string_property_at(object, cursor, &prop).unwrap_or(Value::Nil)
                } else if object.is_nil() {
                    interp
                        .buffer
                        .text_property_at(cursor, &prop)
                        .unwrap_or(Value::Nil)
                } else {
                    let buffer_id = interp.resolve_buffer_id(object)?;
                    let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", buffer_id))
                    })?;
                    buffer.text_property_at(cursor, &prop).unwrap_or(Value::Nil)
                };
                if current != initial {
                    return Ok(Value::Integer(cursor as i64));
                }
            }
            Ok(limit
                .map(|value| Value::Integer(value as i64))
                .unwrap_or(Value::Nil))
        }
        "text-properties-at" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args[0].as_integer()? as usize;
            let props = if let Some(object) = args.get(1) {
                if string_like(object).is_some() {
                    string_properties_at(object, pos)
                } else {
                    let buffer_id = if object.is_nil() {
                        interp.current_buffer_id()
                    } else {
                        interp.resolve_buffer_id(object)?
                    };
                    interp
                        .get_buffer_by_id(buffer_id)
                        .ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?
                        .text_properties_at(pos)
                }
            } else {
                interp.buffer.text_properties_at(pos)
            };
            Ok(plist_value(&props))
        }
        "put-text-property" => {
            if args.len() < 4 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let prop = args[2].as_symbol()?.to_string();
            if let Some(object) = args.get(4) {
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |mut current| {
                        current.retain(|(key, _)| key != &prop);
                        current.push((prop.clone(), args[3].clone()));
                        current
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp
                        .buffer
                        .put_text_property(start, end, &prop, args[3].clone());
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .put_text_property(start, end, &prop, args[3].clone());
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "add-text-properties" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let props = plist_pairs(&args[2])?;
            if let Some(object) = args.get(3) {
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |mut current| {
                        for (name, value) in &props {
                            if let Some((_, existing)) =
                                current.iter_mut().find(|(key, _)| key == name)
                            {
                                *existing = value.clone();
                            } else {
                                current.push((name.clone(), value.clone()));
                            }
                        }
                        current
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.buffer.add_text_properties(start, end, &props);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp.buffer.add_text_properties(start, end, &props);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "set-text-properties" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let props = plist_pairs(&args[2])?;
            if let Some(object) = args.get(3) {
                if matches!(object, Value::String(_)) {
                    return Ok(Value::T);
                }
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |_| props.clone())?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.buffer.set_text_properties(start, end, &props);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp.buffer.set_text_properties(start, end, &props);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "remove-list-of-text-properties" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let names = args[2]
                .to_vec()?
                .into_iter()
                .map(|value| value.as_symbol().map(|s| s.to_string()))
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(object) = args.get(3) {
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |current| {
                        current
                            .into_iter()
                            .filter(|(key, _)| !names.iter().any(|name| name == key))
                            .collect()
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp
                        .buffer
                        .remove_list_of_text_properties(start, end, &names);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .remove_list_of_text_properties(start, end, &names);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "font-lock-append-text-property" => font_lock_add_text_property(interp, name, args, true),
        "font-lock-prepend-text-property" => font_lock_add_text_property(interp, name, args, false),
        "font-lock--remove-face-from-text-property" => {
            need_arg_range(name, args, 4, 5)?;
            let prop = args[2].as_symbol()?.to_string();
            let face = args[3].clone();
            if let Some(object) = args.get(4)
                && string_like(object).is_some()
            {
                let start = args[0].as_integer()?.max(0) as usize;
                let end = args[1].as_integer()?.max(0) as usize;
                modify_shared_string_properties(object, start, end, |mut current| {
                    if let Some(index) = current.iter().position(|(key, _)| key == &prop) {
                        let updated = remove_face_value(current[index].1.clone(), &face);
                        if updated.is_nil() {
                            current.remove(index);
                        } else {
                            current[index].1 = updated;
                        }
                    }
                    current
                })?;
                return Ok(Value::Nil);
            }

            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let buffer_id = font_lock_target_buffer_id(interp, args.get(4))?;
            let mut cursor = start;
            while cursor < end {
                let (previous, next) =
                    font_lock_buffer_segment(interp, buffer_id, cursor, end, &prop)?;
                let updated = remove_face_value(previous, &face);
                font_lock_put_buffer_property(interp, buffer_id, cursor, next, &prop, updated)?;
                cursor = next;
            }
            font_lock_push_buffer_undo_entry(interp, buffer_id)?;
            Ok(Value::Nil)
        }
        "put" => {
            need_args(name, args, 3)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            interp.put_symbol_property(symbol, property, args[2].clone());
            Ok(args[2].clone())
        }
        "compare-buffer-substrings" => {
            need_args(name, args, 6)?;
            let left_id = interp.resolve_buffer_id(&args[0])?;
            let left_start = position_from_value(interp, &args[1])?;
            let left_end = position_from_value(interp, &args[2])?;
            let right_id = interp.resolve_buffer_id(&args[3])?;
            let right_start = position_from_value(interp, &args[4])?;
            let right_end = position_from_value(interp, &args[5])?;
            let left = interp
                .get_buffer_by_id(left_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?
                .buffer_substring(left_start, left_end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let right = interp
                .get_buffer_by_id(right_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", right_id)))?
                .buffer_substring(right_start, right_end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(Value::Integer(compare_buffer_substrings(&left, &right)))
        }
        "field-beginning" | "field-end" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            let field = interp.buffer.text_property_at(pos, "field");
            let mut cursor = pos;
            if name == "field-beginning" {
                while cursor > interp.buffer.point_min()
                    && interp.buffer.text_property_at(cursor - 1, "field") == field
                {
                    cursor -= 1;
                }
            } else {
                while cursor < interp.buffer.point_max()
                    && interp.buffer.text_property_at(cursor, "field") == field
                {
                    cursor += 1;
                }
            }
            Ok(Value::Integer(cursor as i64))
        }
        "field-string-no-properties" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            let start = call(
                interp,
                "field-beginning",
                &[Value::Integer(pos as i64)],
                env,
            )?
            .as_integer()? as usize;
            let end = call(interp, "field-end", &[Value::Integer(pos as i64)], env)?.as_integer()?
                as usize;
            Ok(Value::String(
                interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|e| LispError::Signal(e.to_string()))?,
            ))
        }
        "delete-field" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            let start = call(
                interp,
                "field-beginning",
                &[Value::Integer(pos as i64)],
                env,
            )?
            .as_integer()? as usize;
            let end = call(interp, "field-end", &[Value::Integer(pos as i64)], env)?.as_integer()?
                as usize;
            interp
                .delete_region_current_buffer(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(Value::Nil)
        }
        "constrain-to-field" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let new_pos = if args[0].is_nil() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            let old_pos = if args.len() > 1 {
                position_from_value(interp, &args[1])?
            } else {
                interp.buffer.point()
            };
            let inhibit_motion = interp
                .lookup_var("inhibit-field-text-motion", env)
                .is_some_and(|value| value.is_truthy());
            let mut constrained = new_pos;
            if !inhibit_motion
                && interp.buffer.text_property_at(new_pos, "field")
                    != interp.buffer.text_property_at(old_pos, "field")
            {
                constrained = if new_pos < old_pos {
                    call(
                        interp,
                        "field-beginning",
                        &[Value::Integer(old_pos as i64)],
                        env,
                    )?
                    .as_integer()? as usize
                } else {
                    call(interp, "field-end", &[Value::Integer(old_pos as i64)], env)?
                        .as_integer()? as usize
                };
            }
            if args[0].is_nil() {
                interp.buffer.goto_char(constrained);
            }
            Ok(Value::Integer(constrained as i64))
        }
        "current-buffer" => Ok(Value::Buffer(
            interp.current_buffer_id(),
            interp.buffer.name.clone(),
        )),
        "generate-new-buffer" => {
            need_args(name, args, 1)?;
            let base = args[0].as_string()?;
            let inhibit_hooks = args.get(1).is_some_and(|value| value.is_truthy());
            let buf_name = if interp.has_buffer(base) {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.has_buffer(&candidate) {
                        break candidate;
                    }
                    n += 1;
                }
            } else {
                base.to_string()
            };
            let (id, _) = interp.create_buffer(&buf_name);
            interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
            Ok(Value::Buffer(id, buf_name))
        }
        "get-buffer" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Buffer(id, _) if interp.has_buffer_id(*id) => Ok(args[0].clone()),
                Value::Buffer(_, _) => Ok(Value::Nil),
                _ => match string_like(&args[0]) {
                    Some(name) => match interp.find_buffer(&name.text) {
                        Some((id, buffer_name)) => Ok(Value::Buffer(id, buffer_name)),
                        None => Ok(Value::Nil),
                    },
                    None => Err(LispError::TypeError(
                        "string-or-buffer".into(),
                        args[0].type_name(),
                    )),
                },
            }
        }
        "get-buffer-create" => {
            need_args(name, args, 1)?;
            let inhibit_hooks = args.get(1).is_some_and(|value| value.is_truthy());
            let buf_name = match &args[0] {
                Value::Buffer(_, n) => n.clone(),
                _ => string_text(&args[0]).map_err(|_| {
                    LispError::TypeError("string-or-buffer".into(), args[0].type_name())
                })?,
            };
            if let Some((id, name)) = interp.find_buffer(&buf_name) {
                Ok(Value::Buffer(id, name))
            } else {
                let (id, _) = interp.create_buffer(&buf_name);
                interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
                if !inhibit_hooks {
                    run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
                }
                Ok(Value::Buffer(id, buf_name))
            }
        }
        "generate-new-buffer-name" => {
            need_args(name, args, 1)?;
            let base = args[0].as_string()?;
            let ignore = if args.len() > 1 {
                args[1].as_string().ok().map(|s| s.to_string())
            } else {
                None
            };
            if !interp.has_buffer(base) || ignore.as_deref() == Some(base) {
                Ok(Value::String(base.to_string()))
            } else {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.has_buffer(&candidate) || ignore.as_deref() == Some(&candidate) {
                        break Ok(Value::String(candidate));
                    }
                    n += 1;
                }
            }
        }
        "make-indirect-buffer" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let base_id = interp.resolve_buffer_id(&args[0])?;
            let new_name = string_text(&args[1])?;
            let clone = args.get(2).is_some_and(|value| value.is_truthy());
            let inhibit_hooks = args.get(3).is_some_and(|value| value.is_truthy());
            let (new_id, _) = interp.create_buffer(&new_name);
            let (text, props, point, mark, file, base_overlays) = {
                let base = interp
                    .get_buffer_by_id(base_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", base_id)))?;
                (
                    base.buffer_string(),
                    base.substring_property_spans(base.point_min(), base.point_max()),
                    base.point(),
                    base.mark(),
                    base.file.clone(),
                    base.overlays.clone(),
                )
            };
            let overlays = if clone {
                base_overlays
                    .into_iter()
                    .map(|mut overlay| {
                        overlay.id = interp.alloc_overlay_id();
                        overlay.buffer_id = Some(new_id);
                        overlay
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if let Some(buffer) = interp.get_buffer_by_id_mut(new_id) {
                *buffer = crate::buffer::Buffer::from_text(&new_name, &text);
                buffer.file = file;
                buffer.inhibit_hooks = inhibit_hooks;
                buffer.goto_char(point);
                if let Some(mark) = mark {
                    buffer.set_mark(mark);
                }
                for span in props {
                    buffer.add_text_properties(span.start + 1, span.end + 1, &span.props);
                }
                buffer.overlays = overlays;
            }
            interp.register_indirect_buffer(new_id, base_id);
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
            Ok(Value::Buffer(new_id, new_name))
        }
        "rename-buffer" => {
            need_args(name, args, 1)?;
            let new_name = args[0].as_string()?;
            if new_name.is_empty() {
                return Err(LispError::Signal("Empty string for buffer name".into()));
            }
            let old_name = interp.buffer.name.clone();
            let unique = args.len() > 1 && args[1].is_truthy();
            let final_name = if interp.has_buffer(new_name) && new_name != old_name {
                if unique {
                    let mut n = 2;
                    loop {
                        let candidate = format!("{}<{}>", new_name, n);
                        if !interp.has_buffer(&candidate) {
                            break candidate;
                        }
                        n += 1;
                    }
                } else {
                    return Err(LispError::Signal(format!(
                        "Buffer name `{}' is in use",
                        new_name
                    )));
                }
            } else {
                new_name.to_string()
            };
            if let Some(pos) = interp.buffer_list.iter().position(|(_, n)| *n == old_name) {
                interp.buffer_list[pos].1 = final_name.clone();
            }
            interp.buffer.last_name = Some(old_name);
            interp.buffer.name = final_name.clone();
            Ok(Value::String(final_name))
        }
        "other-buffer" => {
            let exclude = if !args.is_empty() {
                match &args[0] {
                    Value::Buffer(_, n) => n.clone(),
                    _ => interp.buffer.name.clone(),
                }
            } else {
                interp.buffer.name.clone()
            };
            for (id, buf_name) in &interp.buffer_list {
                if *buf_name != exclude && !buf_name.starts_with(' ') {
                    return Ok(Value::Buffer(*id, buf_name.clone()));
                }
            }
            Ok(Value::Buffer(0, "*scratch*".into()))
        }
        "buffer-base-buffer" => {
            let buffer_id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            Ok(interp
                .buffer_base_id(buffer_id)
                .and_then(|base_id| {
                    interp
                        .get_buffer_by_id(base_id)
                        .map(|buffer| Value::Buffer(base_id, buffer.name.clone()))
                })
                .unwrap_or(Value::Nil))
        }
        "buffer-swap-text" => {
            need_args(name, args, 1)?;
            let other_id = interp.resolve_buffer_id(&args[0])?;
            let current_id = interp.current_buffer_id();
            interp.swap_buffer_text_state(current_id, other_id)?;
            Ok(Value::Nil)
        }
        "buffer-local-value" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?.to_string();
            let buffer_id = interp.resolve_buffer_id(&args[1])?;
            Ok(interp
                .buffer_local_value(buffer_id, &symbol)
                .or_else(|| interp.lookup_var(&symbol, env))
                .unwrap_or(Value::Nil))
        }
        "buffer-local-variables" => {
            let mut vars = interp
                .buffer_local_variables(interp.current_buffer_id())
                .into_iter()
                .map(|(name, value)| Value::cons(Value::Symbol(name), value))
                .collect::<Vec<_>>();
            vars.push(Value::cons(
                Value::Symbol("buffer-undo-list".into()),
                buffer_undo_list_value(&interp.buffer),
            ));
            Ok(Value::list(vars))
        }
        "kill-local-variable" => {
            need_args(name, args, 1)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            interp.remove_buffer_local_value(interp.current_buffer_id(), &symbol);
            Ok(Value::Symbol(symbol))
        }
        "buffer-list" => {
            let bufs: Vec<Value> = interp
                .buffer_list
                .iter()
                .map(|(id, n)| Value::Buffer(*id, n.clone()))
                .collect();
            Ok(Value::list(bufs))
        }
        "list-buffers" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let files_only = args.first().is_some_and(Value::is_truthy);
            let _ = refresh_buffer_menu(interp, files_only, None, None, env)?;
            Ok(Value::Symbol("window".into()))
        }
        "list-buffers-noselect" => {
            if args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let files_only = args.first().is_some_and(Value::is_truthy);
            refresh_buffer_menu(interp, files_only, args.get(1), args.get(2), env)
        }
        "Buffer-menu-buffer" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let error_if_non_existent = args.first().is_some_and(Value::is_truthy);
            let Some(entries) =
                interp.buffer_local_value(interp.current_buffer_id(), BUFFER_MENU_ENTRIES_VAR)
            else {
                return if error_if_non_existent {
                    Err(LispError::Signal("No buffer on this line".into()))
                } else {
                    Ok(Value::Nil)
                };
            };
            let entries = entries.to_vec()?;
            let line_index = interp
                .buffer
                .line_number_at_pos(interp.buffer.point())
                .saturating_sub(1);
            let Some(entry) = entries.get(line_index).cloned() else {
                return if error_if_non_existent {
                    Err(LispError::Signal("No buffer on this line".into()))
                } else {
                    Ok(Value::Nil)
                };
            };
            match entry {
                Value::Buffer(id, _) if interp.has_buffer_id(id) => Ok(entry),
                Value::Buffer(_, _) if error_if_non_existent => {
                    Err(LispError::Signal("This buffer has been killed".into()))
                }
                Value::Buffer(_, _) => Ok(Value::Nil),
                other => Err(LispError::TypeError("buffer".into(), other.type_name())),
            }
        }
        "decode-char" => {
            need_args(name, args, 2)?;
            let charset = args[0].as_symbol()?;
            let code = args[1].as_integer()?;
            Ok(match interp.charset_canonical_name(charset).as_deref() {
                Some("ascii") if (0..=0x7f).contains(&code) => Value::Integer(code),
                Some("unicode") if code >= 0 => Value::Integer(code),
                Some(_) | None => Value::Nil,
            })
        }
        "char-charset" => {
            need_args(name, args, 1)?;
            Ok(Value::Symbol(
                charset_for_char(args[0].as_integer()? as u32).into(),
            ))
        }
        "charsetp" => {
            need_args(name, args, 1)?;
            let Value::Symbol(symbol) = &args[0] else {
                return Ok(Value::Nil);
            };
            Ok(if interp.has_charset(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "charset-id-internal" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(interp
                .charset_id(symbol)
                .map(Value::Integer)
                .unwrap_or(Value::Nil))
        }
        "charset-plist" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(interp
                .charset_plist_value(symbol)
                .unwrap_or_else(|| default_charset_plist(symbol, interp).unwrap_or(Value::Nil)))
        }
        "charset-priority-list" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let priority = interp.charset_priority_list();
            if args.first().is_some_and(Value::is_truthy) {
                Ok(priority
                    .first()
                    .cloned()
                    .map(Value::Symbol)
                    .unwrap_or(Value::Nil))
            } else {
                Ok(Value::list(
                    priority.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
                ))
            }
        }
        "charset-after" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(interp.buffer.point() as i64);
            Ok(match interp.buffer.char_at(pos as usize) {
                Some(ch) => Value::Symbol(charset_for_char(ch as u32).into()),
                None => Value::Nil,
            })
        }
        "find-charset-string" => {
            need_args(name, args, 1)?;
            Ok(Value::list(charsets_for_text(
                &string_text(&args[0])?,
                interp,
            )))
        }
        "find-charset-region" => {
            need_args(name, args, 2)?;
            let from = args[0].as_integer()?;
            let to = args[1].as_integer()?;
            let (start, end) = clamp_overlay_range(&interp.buffer, from, to);
            let mut text = String::new();
            for pos in start..end {
                if let Some(ch) = interp.buffer.char_at(pos) {
                    text.push(ch);
                }
            }
            Ok(Value::list(charsets_for_text(&text, interp)))
        }
        "map-charset-chars" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let function = args[0].clone();
            let charset = args[1].as_symbol()?.to_string();
            let arg = args.get(2).cloned().unwrap_or(Value::Nil);
            let from = args.get(3).map(Value::as_integer).transpose()?.unwrap_or(0);
            let to = args
                .get(4)
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(charset_max_codepoint(&charset));
            let ranges = charset_ranges_for(&charset, from, to, interp)?;
            for (start, end) in ranges {
                call_function_value(
                    interp,
                    &function,
                    &[
                        Value::cons(Value::Integer(start), Value::Integer(end)),
                        arg.clone(),
                    ],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "define-charset-internal" => Err(LispError::WrongNumberOfArgs(name.into(), args.len())),
        "define-charset-alias" => {
            need_args(name, args, 2)?;
            let alias = args[0].as_symbol()?;
            let target = args[1].as_symbol()?;
            interp.define_charset_alias(alias, target)?;
            Ok(Value::Symbol(alias.to_string()))
        }
        "set-charset-plist" => {
            need_args(name, args, 2)?;
            let charset = args[0].as_symbol()?;
            interp.set_charset_plist_value(charset, args[1].clone())?;
            Ok(args[1].clone())
        }
        "unify-charset" => {
            need_args(name, args, 1)?;
            Err(LispError::Signal("Cannot unify charset".into()))
        }
        "get-unused-iso-final-char" => {
            need_args(name, args, 2)?;
            Ok(Value::Integer('0' as i64))
        }
        "declare-equiv-charset" => {
            need_args(name, args, 4)?;
            let dimension = args[0].as_integer()?;
            let chars = args[1].as_integer()?;
            let final_char = args[2].as_integer()?;
            let charset = args[3].as_symbol()?;
            interp.declare_iso_charset(dimension, chars, final_char as u32, charset);
            Ok(Value::Nil)
        }
        "iso-charset" => {
            need_args(name, args, 3)?;
            let dimension = args[0].as_integer()?;
            let chars = args[1].as_integer()?;
            let final_char = args[2].as_integer()?;
            Ok(interp
                .iso_charset(dimension, chars, final_char as u32)
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }
        "split-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            Ok(Value::list([
                Value::Symbol(charset_for_char(code as u32).into()),
                Value::Integer(code),
            ]))
        }
        "clear-charset-maps" => Ok(Value::Nil),
        "set-charset-priority" => {
            let names = args
                .iter()
                .map(Value::as_symbol)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>();
            interp.set_charset_priority(&names);
            Ok(Value::Nil)
        }
        "sort-charsets" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.sort_by_key(|value| {
                value
                    .as_symbol()
                    .map(|name| interp.charset_priority_rank(name))
                    .unwrap_or(usize::MAX)
            });
            Ok(Value::list(items))
        }
        "coding-system-p" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_nil() {
                Value::T
            } else if let Value::Symbol(symbol) = &args[0] {
                if interp.has_coding_system(symbol) {
                    Value::T
                } else {
                    Value::Nil
                }
            } else {
                Value::Nil
            })
        }
        "check-coding-system" => {
            need_args(name, args, 1)?;
            Ok(match checked_coding_name(interp, &args[0])? {
                Some(coding) => Value::Symbol(coding),
                None => Value::Nil,
            })
        }
        "coding-system-priority-list" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let priority = interp.coding_system_priority_list();
            if args.first().is_some_and(Value::is_truthy) {
                Ok(priority
                    .first()
                    .cloned()
                    .map(Value::Symbol)
                    .unwrap_or(Value::Nil))
            } else {
                Ok(Value::list(
                    priority.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
                ))
            }
        }
        "coding-system-aliases" => {
            need_args(name, args, 1)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(Value::list(
                interp
                    .coding_system_alias_list(&coding)
                    .unwrap_or_default()
                    .into_iter()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            ))
        }
        "coding-system-plist" => {
            need_args(name, args, 1)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(interp
                .coding_system_plist_value(&coding)
                .unwrap_or(Value::Nil))
        }
        "coding-system-put" => {
            need_args(name, args, 3)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            let key = args[1].as_symbol()?;
            interp.set_coding_system_plist_property(&coding, key, args[2].clone())?;
            Ok(args[2].clone())
        }
        "coding-system-eol-type" => {
            need_args(name, args, 1)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(interp
                .coding_system_eol_type_value(&coding)
                .map(Value::Integer)
                .unwrap_or(Value::Nil))
        }
        "coding-system-base" => {
            need_args(name, args, 1)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(interp
                .coding_system_base_name(&coding)
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }
        "coding-system-equal" => {
            need_args(name, args, 2)?;
            let equal = match (
                checked_coding_name(interp, &args[0])?,
                checked_coding_name(interp, &args[1])?,
            ) {
                (None, None) => true,
                (Some(left), Some(right)) => {
                    left == right
                        || (interp.coding_system_plist_value(&left)
                            == interp.coding_system_plist_value(&right)
                            && interp.coding_system_eol_type_value(&left)
                                == interp.coding_system_eol_type_value(&right))
                }
                _ => false,
            };
            Ok(if equal { Value::T } else { Value::Nil })
        }
        "check-coding-systems-region" => {
            need_args(name, args, 3)?;
            check_coding_systems_region_value(interp, &args[0], args.get(1), &args[2])
        }
        "detect-coding-string" => {
            need_args(name, args, 1)?;
            detect_coding_string_value(interp, &args[0], args.get(1), env)
        }
        "detect-coding-region" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            detect_coding_region_value(interp, &args[0], &args[1], args.get(2), env)
        }
        "find-coding-systems-region-internal" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            find_coding_systems_region_internal_value(interp, &args[0])
        }
        "decode-sjis-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                0x82A0 => Ok(Value::Integer('あ' as i64)),
                _ => Err(LispError::Signal("Invalid Shift_JIS character".into())),
            }
        }
        "encode-sjis-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                x if x == 'あ' as i64 => Ok(Value::Integer(0x82A0)),
                _ => Err(LispError::Signal(
                    "Character cannot be encoded in Shift_JIS".into(),
                )),
            }
        }
        "decode-big5-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                _ => Err(LispError::Signal("Invalid Big5 character".into())),
            }
        }
        "encode-big5-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                _ => Err(LispError::Signal(
                    "Character cannot be encoded in Big5".into(),
                )),
            }
        }
        "terminal-coding-system" => Ok(interp
            .terminal_coding_system()
            .map(Value::Symbol)
            .unwrap_or(Value::Nil)),
        "set-terminal-coding-system-internal" | "set-safe-terminal-coding-system-internal" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[0])?;
            interp.set_terminal_coding_system(coding.clone());
            Ok(coding.map(Value::Symbol).unwrap_or(Value::Nil))
        }
        "keyboard-coding-system" => Ok(interp
            .keyboard_coding_system()
            .map(Value::Symbol)
            .unwrap_or(Value::Nil)),
        "set-keyboard-coding-system-internal" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[0])?;
            interp.set_keyboard_coding_system(coding.clone());
            Ok(coding.map(Value::Symbol).unwrap_or(Value::Nil))
        }
        "find-operation-coding-system" => find_operation_coding_system_value(interp, args, env),
        "set-coding-system-priority" => {
            let names = args
                .iter()
                .map(|value| checked_coding_symbol(interp, value))
                .collect::<Result<Vec<_>, _>>()?;
            interp.set_coding_system_priority(&names)?;
            Ok(Value::Nil)
        }
        "define-coding-system-internal" => {
            if args.len() < 13 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = args[0].as_symbol()?;
            let mnemonic = args[1].as_integer()?;
            let kind = args[2].as_symbol()?;
            let plist = args[11].clone();
            let eol_type = match args[12].as_symbol()? {
                "unix" => Some(0),
                "dos" => Some(1),
                "mac" => Some(2),
                _ => None,
            };
            interp.define_coding_system(coding, mnemonic, kind, plist, eol_type)?;
            Ok(Value::Symbol(coding.to_string()))
        }
        "define-coding-system-alias" => {
            need_args(name, args, 2)?;
            let alias = args[0].as_symbol()?;
            let target = args[1].as_symbol()?;
            interp.define_coding_system_alias(alias, target)?;
            Ok(Value::Symbol(alias.to_string()))
        }
        "set-buffer" => {
            need_args(name, args, 1)?;
            let id = interp.resolve_buffer_id(&args[0])?;
            interp.switch_to_buffer_id(id)?;
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "switch-to-buffer" => {
            need_args(name, args, 1)?;
            let id = if let Some(name) = string_like(&args[0]).map(|string| string.text) {
                interp
                    .find_buffer(&name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&name).0)
            } else {
                interp.resolve_buffer_id(&args[0])?
            };
            interp.switch_to_buffer_id(id)?;
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "pop-to-buffer-same-window" => {
            need_args(name, args, 1)?;
            let id = if let Some(name) = string_like(&args[0]).map(|string| string.text) {
                interp
                    .find_buffer(&name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&name).0)
            } else {
                interp.resolve_buffer_id(&args[0])?
            };
            interp.switch_to_buffer_id(id)?;
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "create-file-buffer" => {
            need_args(name, args, 1)?;
            let filename = string_text(&args[0])?;
            let path = std::path::Path::new(&filename);
            let basename = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .unwrap_or(filename.as_str());
            let basename = if basename.starts_with(' ') {
                format!("|{basename}")
            } else {
                basename.to_string()
            };
            let buf_name = if interp.has_buffer(&basename) {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", basename, n);
                    if !interp.has_buffer(&candidate) {
                        break candidate;
                    }
                    n += 1;
                }
            } else {
                basename
            };
            let (id, _) = interp.create_buffer(&buf_name);
            Ok(Value::Buffer(id, buf_name))
        }
        "buffer-file-name" => Ok(interp
            .buffer
            .file
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Nil)),
        "visited-file-modtime" => Ok(interp
            .buffer
            .visited_file_modtime()
            .and_then(|modtime| system_time_seconds_value(modtime.modified).ok())
            .unwrap_or(Value::Integer(0))),
        "set-visited-file-modtime" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let modtime = match args.first() {
                None | Some(Value::Nil) => {
                    if let Some(path) = interp.buffer.file.clone() {
                        file_modtime(&path)?
                    } else {
                        None
                    }
                }
                Some(Value::Integer(0)) => None,
                Some(value) => Some(file_modtime_from_value(interp, value)?),
            };
            interp.buffer.set_visited_file_modtime(modtime);
            Ok(Value::T)
        }
        "set-buffer-file-coding-system" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[0])?;
            let value = coding
                .as_ref()
                .map(|coding| Value::Symbol(coding.clone()))
                .unwrap_or(Value::Nil);
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-file-coding-system",
                value,
            );
            if !args.get(2).is_some_and(Value::is_truthy) {
                interp.buffer.set_modified();
            }
            Ok(coding.map(Value::Symbol).unwrap_or(Value::Nil))
        }
        "find-file-noselect" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            if let Some((id, name)) = interp.find_buffer(&path) {
                return Ok(Value::Buffer(id, name));
            }
            let (id, _) = interp.create_buffer(&path);
            let contents = std::fs::read_to_string(&path).unwrap_or_default();
            if let Some(buffer) = interp.get_buffer_by_id_mut(id) {
                *buffer = crate::buffer::Buffer::from_text(&path, &contents);
                buffer.file = Some(path.clone());
                buffer.file_truename = Some(path.clone());
                buffer.set_unmodified();
                buffer.set_visited_file_modtime(file_modtime(&path)?);
            }
            Ok(Value::Buffer(id, path))
        }
        "find-file" => {
            need_args(name, args, 1)?;
            let buffer = call(interp, "find-file-noselect", args, env)?;
            let id = interp.resolve_buffer_id(&buffer)?;
            interp.switch_to_buffer_id(id)?;
            Ok(buffer)
        }
        "expand-file-name" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[0])?;
            let base = match args.get(1) {
                Some(value) if !value.is_nil() => Some(string_text(value)?),
                _ => interp
                    .lookup_var("default-directory", env)
                    .and_then(|value| string_like(&value).map(|string| string.text)),
            };
            Ok(Value::String(expand_file_name_runtime(
                interp,
                env,
                &path,
                base.as_deref(),
            )?))
        }
        "substitute-in-file-name" => {
            need_args(name, args, 1)?;
            Ok(Value::String(substitute_in_file_name(&string_text(
                &args[0],
            )?)))
        }
        "file-name-directory" => {
            need_args(name, args, 1)?;
            Ok(file_name_directory(&string_text(&args[0])?)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "file-name-nondirectory" => {
            need_args(name, args, 1)?;
            Ok(Value::String(file_name_nondirectory(&string_text(
                &args[0],
            )?)))
        }
        "file-name-as-directory" => {
            need_args(name, args, 1)?;
            Ok(Value::String(file_name_as_directory(&string_text(
                &args[0],
            )?)))
        }
        "directory-file-name" => {
            need_args(name, args, 1)?;
            Ok(Value::String(directory_file_name(&string_text(&args[0])?)))
        }
        "directory-name-p" => {
            need_args(name, args, 1)?;
            Ok(if directory_name_p(&string_text(&args[0])?) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-name-absolute-p" => {
            need_args(name, args, 1)?;
            Ok(if file_name_absolute_p(&string_text(&args[0])?) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-name-case-insensitive-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "file-name-concat" => Ok(Value::String(file_name_concat(
            &args
                .iter()
                .filter(|value| !value.is_nil())
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        "file-name-unquote" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?))
        }
        "file-remote-p" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::Nil)
        }
        "shell-quote-argument" => {
            need_args(name, args, 1)?;
            let argument = string_text(&args[0])?;
            Ok(Value::String(shell_quote_argument(&argument)))
        }
        "locate-user-emacs-file" => {
            need_arg_range(name, args, 1, 2)?;
            let user_emacs_directory = interp
                .lookup_var("user-emacs-directory", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(default_directory);
            let resolved = match &args[0] {
                Value::Nil => {
                    return Err(LispError::TypeError("stringp".into(), args[0].type_name()));
                }
                Value::Cons(_, _) => {
                    let names = args[0]
                        .to_vec()?
                        .into_iter()
                        .map(|value| string_text(&value))
                        .collect::<Result<Vec<_>, _>>()?;
                    let Some(default_name) = names.first() else {
                        return Err(LispError::TypeError("consp".into(), args[0].type_name()));
                    };
                    names
                        .iter()
                        .rev()
                        .map(|name| expand_file_name(name, Some(&user_emacs_directory)))
                        .find(|path| Path::new(path).exists())
                        .unwrap_or_else(|| expand_file_name(default_name, Some(&user_emacs_directory)))
                }
                _ => expand_file_name(&string_text(&args[0])?, Some(&user_emacs_directory)),
            };
            if let Some(old_name) = args.get(1).filter(|value| !value.is_nil()) {
                let home = expand_home_prefix("~");
                let legacy = expand_file_name(&string_text(old_name)?, Some(&home));
                if !file_readable_p(&resolved) && file_readable_p(&legacy) {
                    return Ok(Value::String(legacy));
                }
            }
            Ok(Value::String(resolved))
        }
        "ert-resource-directory" => {
            need_args(name, args, 0)?;
            Ok(ert_resource_directory(interp)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "ert-resource-file" => {
            need_args(name, args, 1)?;
            let file = string_text(&args[0])?;
            let Some(directory) = ert_resource_directory(interp) else {
                return Err(LispError::Signal(
                    "Cannot determine the current ERT resource directory".into(),
                ));
            };
            Ok(Value::String(expand_file_name(&file, Some(&directory))))
        }
        "ert-fail" => {
            need_args(name, args, 1)?;
            let message = match &args[0] {
                Value::String(message) => message.clone(),
                Value::StringObject(state) => state.borrow().text.clone(),
                value => value.to_string(),
            };
            Err(LispError::Signal(message))
        }
        "locate-library" => {
            if args.is_empty() || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let library = string_text(&args[0])?;
            Ok(interp
                .resolve_load_target(&library)
                .map(|path| Value::String(path.display().to_string()))
                .unwrap_or(Value::Nil))
        }
        "load" => {
            if args.is_empty() || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let target = string_text(&args[0])?;
            let _loaded = interp.load_target(&target)?;
            Ok(Value::T)
        }
        "file-directory-p" | "file-accessible-directory-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            Ok(
                if fs::metadata(&path)
                    .map(|metadata| metadata.is_dir())
                    .unwrap_or(false)
                    && (name == "file-directory-p" || file_readable_p(&path))
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "file-readable-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if file_readable_p(&path) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-exists-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            Ok(if fs::metadata(path).is_ok() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-executable-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if file_executable_p(&path) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-attributes" => {
            need_arg_range(name, args, 1, 3)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Value::Nil),
                Err(error) => return Err(LispError::Signal(error.to_string())),
            };
            let file_type = metadata.file_type();
            let type_value = if file_type.is_dir() {
                Value::T
            } else if file_type.is_symlink() {
                fs::read_link(&path)
                    .ok()
                    .map(|target| Value::String(target.to_string_lossy().into_owned()))
                    .unwrap_or(Value::String(path.clone()))
            } else {
                Value::Nil
            };
            let accessed = metadata
                .accessed()
                .ok()
                .map(system_time_seconds_value)
                .transpose()?
                .unwrap_or(Value::Integer(0));
            let modified = metadata
                .modified()
                .ok()
                .map(system_time_seconds_value)
                .transpose()?
                .unwrap_or(Value::Integer(0));
            let changed = metadata
                .created()
                .ok()
                .map(system_time_seconds_value)
                .transpose()?
                .unwrap_or_else(|| modified.clone());
            Ok(Value::list([
                type_value,
                Value::Integer(1),
                Value::Integer(0),
                Value::Integer(0),
                accessed,
                modified,
                changed,
                Value::Integer(metadata.len() as i64),
                Value::String(if file_type.is_dir() {
                    "drwxr-xr-x".into()
                } else {
                    "-rw-r--r--".into()
                }),
                Value::Nil,
                Value::Integer(0),
                Value::Integer(0),
            ]))
        }
        "file-attribute-type" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 0)
        }
        "file-attribute-link-number" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 1)
        }
        "file-attribute-user-id" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 2)
        }
        "file-attribute-group-id" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 3)
        }
        "file-attribute-access-time" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 4)
        }
        "file-attribute-modification-time" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 5)
        }
        "file-attribute-status-change-time" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 6)
        }
        "file-attribute-size" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 7)
        }
        "file-attribute-modes" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 8)
        }
        "file-attribute-inode-number" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 10)
        }
        "file-attribute-device-number" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 11)
        }
        "file-attribute-file-identifier" => {
            need_args(name, args, 1)?;
            Ok(Value::cons(
                file_attribute_field(&args[0], 10)?,
                file_attribute_field(&args[0], 11)?,
            ))
        }
        "delete-file" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            fs::remove_file(path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "delete-file-internal" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            fs::remove_file(path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "delete-directory" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            if args.get(1).is_some_and(Value::is_truthy) {
                fs::remove_dir_all(path).map_err(|error| LispError::Signal(error.to_string()))?;
            } else {
                fs::remove_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            Ok(Value::Nil)
        }
        "delete-directory-internal" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            fs::remove_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "make-directory" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            if args.get(1).is_some_and(Value::is_truthy) {
                fs::create_dir_all(path).map_err(|error| LispError::Signal(error.to_string()))?;
            } else {
                fs::create_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            Ok(Value::Nil)
        }
        "mkdir" => call(interp, "make-directory", args, env),
        "make-directory-internal" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            fs::create_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "make-temp-file-internal" => {
            need_args(name, args, 4)?;
            let prefix = string_text(&args[0])?;
            let suffix = string_text(&args[2])?;
            validate_file_name(&prefix)?;
            validate_file_name(&suffix)?;
            Ok(Value::String(make_temp_file_internal(
                &prefix,
                &args[1],
                &suffix,
                args.get(3),
            )?))
        }
        "file-locked-p" => {
            need_args(name, args, 1)?;
            file_locked_p(&string_text(&args[0])?)
        }
        "write-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            write_region_value(interp, args, env)
        }
        "write-file" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            write_file_value(interp, args, env)
        }
        "insert-directory" => {
            need_arg_range(name, args, 2, 4)?;
            let file = string_text(&args[0])?;
            let switches = string_text(&args[1])?;
            let program = interp
                .lookup_var("insert-directory-program", env)
                .filter(|value| value.is_truthy())
                .map(|value| string_text(&value))
                .transpose()?
                .unwrap_or_else(|| "ls".into());
            let argv = switches
                .split_whitespace()
                .map(str::to_string)
                .chain(std::iter::once(file))
                .collect::<Vec<_>>();
            let process_output = run_external_process(interp, &program, &argv, None, env)?;
            if !process_output.status.success() {
                let stderr = String::from_utf8_lossy(&process_output.stderr)
                    .trim()
                    .to_string();
                return Err(LispError::Signal(if stderr.is_empty() {
                    format!(
                        "{program} exited with status {}",
                        exit_status_code(&process_output.status)
                    )
                } else {
                    stderr
                }));
            }
            interp.insert_current_buffer(&String::from_utf8_lossy(&process_output.stdout));
            Ok(Value::Nil)
        }
        "insert-file-contents" => insert_file_contents(interp, env, args, false),
        "insert-file-contents-literally" => insert_file_contents(interp, env, args, true),
        "get-free-disk-space" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "file-symlink-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            validate_file_name(&path)?;
            let target = fs::symlink_metadata(&path)
                .ok()
                .filter(|metadata| metadata.file_type().is_symlink())
                .and_then(|_| fs::read_link(&path).ok());
            Ok(target
                .map(|path| Value::String(path.to_string_lossy().into_owned()))
                .unwrap_or(Value::Nil))
        }
        "make-symbolic-link" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let target = string_text(&args[0])?;
            let link = string_text(&args[1])?;
            validate_file_name(&target)?;
            validate_file_name(&link)?;
            if args.get(2).is_some_and(Value::is_truthy) && fs::symlink_metadata(&link).is_ok() {
                fs::remove_file(&link).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            #[cfg(unix)]
            {
                symlink(&target, &link).map_err(|error| LispError::Signal(error.to_string()))?;
                Ok(Value::Nil)
            }
            #[cfg(not(unix))]
            {
                Err(LispError::Signal("make-symbolic-link not supported".into()))
            }
        }
        "call-process" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let program = string_text(&args[0])?;
            let input = match args.get(1) {
                Some(value) if !value.is_nil() => match value {
                    Value::Integer(0) => None,
                    _ => Some(
                        fs::read(string_text(value)?)
                            .map_err(|error| LispError::Signal(error.to_string()))?,
                    ),
                },
                _ => None,
            };
            let destination = args.get(2).unwrap_or(&Value::Nil);
            let argv = args
                .get(4..)
                .unwrap_or(&[])
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output =
                run_external_process(interp, &program, &argv, input.as_deref(), env)?;
            write_process_output(
                interp,
                destination,
                &process_output.stdout,
                &process_output.stderr,
            )?;
            Ok(Value::Integer(exit_status_code(&process_output.status)))
        }
        "process-lines" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let program = string_text(&args[0])?;
            let argv = args[1..]
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output = run_external_process(interp, &program, &argv, None, env)?;
            if !process_output.status.success() {
                let stderr = String::from_utf8_lossy(&process_output.stderr)
                    .trim()
                    .to_string();
                return Err(LispError::Signal(if stderr.is_empty() {
                    format!(
                        "process-lines exited with status {}",
                        exit_status_code(&process_output.status)
                    )
                } else {
                    stderr
                }));
            }
            let lines = String::from_utf8_lossy(&process_output.stdout)
                .lines()
                .map(|line| Value::String(line.to_string()))
                .collect::<Vec<_>>();
            Ok(Value::list(lines))
        }
        "get-locale-names" => {
            if !args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let output = Command::new("locale")
                .arg("-a")
                .output()
                .map_err(|error| LispError::Signal(error.to_string()))?;
            if !output.status.success() {
                return Ok(Value::Nil);
            }
            let locales = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(|line| Value::String(line.to_string()))
                .collect::<Vec<_>>();
            Ok(Value::list(locales))
        }
        "zlib-decompress-region" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let compressed = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let input = compressed
                .chars()
                .map(|ch| {
                    u8::try_from(ch as u32)
                        .map_err(|_| LispError::Signal("Invalid byte in compressed data".into()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut decoder = GzDecoder::new(&input[..]);
            let mut output = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut output)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            ensure_region_modifiable(interp, start, end, env)?;
            delete_region_with_hooks(interp, start, end, env)?;
            let text = output
                .iter()
                .map(|byte| char::from(*byte))
                .collect::<String>();
            insert_text_with_hooks(interp, &text, &[], false, false, env)?;
            Ok(Value::Nil)
        }
        "libxml-parse-xml-region" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let xml = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            parse_xml_region(&xml).map_err(|error| LispError::Signal(error.to_string()))
        }
        "call-process-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let (start, end) = if args[0].is_nil() && args[1].is_nil() {
                (interp.buffer.point_min(), interp.buffer.point_max())
            } else {
                (
                    position_from_value(interp, &args[0])?,
                    position_from_value(interp, &args[1])?,
                )
            };
            let input = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let program = string_text(&args[2])?;
            let delete_region = args.get(3).is_some_and(Value::is_truthy);
            let destination = args.get(4).unwrap_or(&Value::Nil);
            let argv = args
                .get(6..)
                .unwrap_or(&[])
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output =
                run_external_process(interp, &program, &argv, Some(input.as_bytes()), env)?;
            if delete_region {
                interp
                    .buffer
                    .delete_region(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
            write_process_output(
                interp,
                destination,
                &process_output.stdout,
                &process_output.stderr,
            )?;
            Ok(Value::Integer(exit_status_code(&process_output.status)))
        }
        "shell-command" => {
            need_args(name, args, 1)?;
            let command = string_text(&args[0])?;
            let status = Command::new("sh")
                .arg("-c")
                .arg(&command)
                .status()
                .map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Integer(status.code().unwrap_or(1) as i64))
        }
        "kill-buffer" => {
            let id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            let inhibit_hooks = interp.buffer_hooks_inhibited(id);
            let auto_save = interp.buffer_local_value(id, "buffer-auto-save-file-name");
            let auto_save_path = auto_save.as_ref().and_then(|value| string_text(value).ok());
            let modified = interp
                .get_buffer_by_id(id)
                .map(|buffer| buffer.is_modified())
                .unwrap_or(false);
            if modified {
                let answer = call_named_function(
                    interp,
                    "yes-or-no-p",
                    &[Value::String("Buffer modified; kill anyway?".into())],
                    env,
                )?;
                if answer.is_nil() {
                    return Ok(Value::Nil);
                }
                if let Some(path) = auto_save_path.as_ref()
                    && fs::metadata(path).is_ok()
                    && interp
                        .lookup_var("kill-buffer-delete-auto-save-files", env)
                        .is_some_and(|value| value.is_truthy())
                {
                    let delete = call_named_function(
                        interp,
                        "yes-or-no-p",
                        &[Value::String("Delete auto-save file?".into())],
                        env,
                    )?;
                    if delete.is_truthy() {
                        let _ = fs::remove_file(path);
                    }
                }
                if id == interp.current_buffer_id() {
                    unlock_current_buffer(interp, env)?;
                }
            }
            if !inhibit_hooks {
                for hook in hook_values(interp, "kill-buffer-query-functions", env, Some(id)) {
                    let result = call_function_value(interp, &hook, &[], env)?;
                    if result.is_nil() {
                        return Ok(Value::Nil);
                    }
                }
                run_named_hooks(interp, "kill-buffer-hook", env, Some(id))?;
            }
            interp.kill_buffer_id(id);
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
            Ok(Value::T)
        }
        "set-mark" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            interp.buffer.set_mark(pos);
            Ok(Value::Nil)
        }
        "push-mark" => {
            let pos = if args.is_empty() || args[0].is_nil() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            interp.buffer.set_mark(pos);
            if !args.get(2).is_some_and(Value::is_truthy) {
                interp.buffer.deactivate_mark();
            }
            Ok(Value::Nil)
        }
        "mark" => Ok(match interp.buffer.mark() {
            Some(m) => Value::Integer(m as i64),
            None => Value::Nil,
        }),
        "make-marker" => Ok(interp.make_marker()),
        "copy-marker" => {
            need_args(name, args, 1)?;
            let insertion_type = args.get(1).is_some_and(Value::is_truthy);
            interp.copy_marker_value(&args[0], insertion_type)
        }
        "point-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point() as i64), false)
        }
        "mark-marker" => match interp.buffer.mark() {
            Some(pos) => interp.copy_marker_value(&Value::Integer(pos as i64), false),
            None => interp.copy_marker_value(&Value::Nil, false),
        },
        "point-min-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point_min() as i64), false)
        }
        "point-max-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point_max() as i64), false)
        }
        "marker-buffer" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            match interp.marker_buffer_id(marker_id) {
                Some(buffer_id) => {
                    let buffer_name = interp
                        .buffer_list
                        .iter()
                        .find(|(id, _)| *id == buffer_id)
                        .map(|(_, name)| name.clone())
                        .unwrap_or_else(|| "*unknown*".to_string());
                    Ok(Value::Buffer(buffer_id, buffer_name))
                }
                None => Ok(Value::Nil),
            }
        }
        "marker-position" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(interp
                .marker_position(marker_id)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "marker-last-position" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(interp
                .marker_last_position(marker_id)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "marker-insertion-type" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(
                if interp.marker_insertion_type(marker_id).unwrap_or(false) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "set-marker-insertion-type" => {
            need_args(name, args, 2)?;
            let marker_id = marker_id_from_value(&args[0])?;
            let insertion_type = args[1].is_truthy();
            interp.set_marker_insertion_type(marker_id, insertion_type);
            Ok(if insertion_type { Value::T } else { Value::Nil })
        }
        "set-marker" | "move-marker" => {
            need_args(name, args, 2)?;
            let marker_id = marker_id_from_value(&args[0])?;
            let (position, buffer_id) = marker_target(interp, &args[1], args.get(2))?;
            interp.set_marker(marker_id, position, buffer_id)?;
            Ok(args[0].clone())
        }
        "region-beginning" => match interp.buffer.region() {
            Some((beg, _)) => Ok(Value::Integer(beg as i64)),
            None => Err(LispError::Signal("The mark is not set now".into())),
        },
        "region-end" => match interp.buffer.region() {
            Some((_, end)) => Ok(Value::Integer(end as i64)),
            None => Err(LispError::Signal("The mark is not set now".into())),
        },
        "region-active-p" => Ok(
            if interp.buffer.mark_active()
                && interp
                    .lookup_var("transient-mark-mode", env)
                    .unwrap_or(Value::T)
                    .is_truthy()
            {
                Value::T
            } else {
                Value::Nil
            },
        ),

        // ── Output ──
        "substitute-command-keys" => {
            need_arg_range(name, args, 1, 3)?;
            Ok(args[0].clone())
        }
        "message" => {
            let text = if args.is_empty() {
                String::new()
            } else {
                string_text(&call(interp, "format", args, env)?)?
            };
            let buffer_name = interp
                .lookup_var("messages-buffer-name", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(|| "*Messages*".into());
            let buffer_id = interp
                .find_buffer(&buffer_name)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(&buffer_name).0);
            if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                let end = buffer.point_max();
                buffer.goto_char(end);
                buffer.insert(&(text.clone() + "\n"));
            }
            if let Some(captured) = interp.message_capture_stack.last_mut() {
                captured.push_str(&text);
                captured.push('\n');
            }
            Ok(Value::String(text))
        }
        "princ" | "print" => {
            if args.is_empty() {
                Ok(Value::Nil)
            } else {
                Ok(args[0].clone())
            }
        }
        "prin1-to-string" => {
            need_args(name, args, 1)?;
            Ok(Value::String(render_prin1(interp, &args[0], env)?))
        }
        "read-char-choice" => {
            need_args(name, args, 2)?;
            ensure_interaction_allowed(interp, env)?;
            Ok(first_choice_value(&args[1]).unwrap_or(Value::Integer('y' as i64)))
        }
        "y-or-n-p" | "yes-or-no-p" => {
            need_args(name, args, 1)?;
            ensure_interaction_allowed(interp, env)?;
            let _ = call(interp, "message", args, env)?;
            Ok(Value::T)
        }

        // ── More string/char ops ──
        "char-equal" => {
            need_args(name, args, 2)?;
            let a = args[0].as_integer()?;
            let b = args[1].as_integer()?;
            let case_fold = interp
                .lookup_var("case-fold-search", env)
                .map(|v| v.is_truthy())
                .unwrap_or(false);
            let eq = if case_fold {
                a == b || (a as u8 as char).eq_ignore_ascii_case(&(b as u8 as char))
            } else {
                a == b
            };
            Ok(if eq { Value::T } else { Value::Nil })
        }
        "number-sequence" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "number-sequence".into(),
                    args.len(),
                ));
            }
            let from = args[0].as_integer()?;
            let to = if args.len() > 1 {
                args[1].as_integer()?
            } else {
                from
            };
            let step = if args.len() > 2 {
                args[2].as_integer()?
            } else if from <= to {
                1
            } else {
                -1
            };
            if step == 0 {
                return Err(LispError::Signal(
                    "number-sequence: step must not be 0".into(),
                ));
            }
            let mut result = Vec::new();
            let mut i = from;
            if step > 0 {
                while i <= to {
                    result.push(Value::Integer(i));
                    i += step;
                }
            } else {
                while i >= to {
                    result.push(Value::Integer(i));
                    i += step;
                }
            }
            Ok(Value::list(result))
        }
        "kbd" => {
            need_args(name, args, 1)?;
            parse_kbd_sequence(&string_text(&args[0])?)
        }
        "key-description" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(
                    "key-description".into(),
                    args.len(),
                ));
            }
            let mut parts = Vec::new();
            if let Some(prefix) = args.get(1) {
                append_key_description_parts(prefix, &mut parts)?;
            }
            append_key_description_parts(&args[0], &mut parts)?;
            Ok(Value::String(parts.join(" ")))
        }
        "single-key-description" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(
                    "single-key-description".into(),
                    args.len(),
                ));
            }
            let no_angles = args.get(1).is_some_and(Value::is_truthy);
            Ok(Value::String(single_key_description_text(
                &args[0], no_angles,
            )?))
        }

        // ── More buffer ops ──
        "following-char" => match interp.buffer.char_at(interp.buffer.point()) {
            Some(c) => Ok(Value::Integer(c as i64)),
            None => Ok(Value::Integer(0)),
        },
        "preceding-char" => {
            let pt = interp.buffer.point();
            if pt <= interp.buffer.point_min() {
                Ok(Value::Integer(0))
            } else {
                match interp.buffer.char_at(pt - 1) {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Integer(0)),
                }
            }
        }
        "buffer-last-name" => Ok(Value::String(
            interp
                .buffer
                .last_name
                .clone()
                .unwrap_or_else(|| interp.buffer.name.clone()),
        )),

        // ── Display stubs ──
        "display-graphic-p" | "display-images-p" | "frame-parameter" => Ok(Value::Nil),
        "frame-char-width" => Ok(Value::Integer(1)),
        "transient-mark-mode" => {
            let enabled = args.first().is_some_and(Value::is_truthy);
            interp.set_variable(
                "transient-mark-mode",
                if enabled { Value::T } else { Value::Nil },
                env,
            );
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "font-lock-mode" => Ok(Value::Nil),
        "find-image" => {
            need_args(name, args, 1)?;
            let specs = args[0].to_vec()?;
            Ok(specs.into_iter().next().unwrap_or(Value::Nil))
        }
        "image-size" | "image-mask-p" | "image-metadata" => Err(LispError::Signal(
            "Images are unavailable on a nongraphical display".into(),
        )),
        "imagemagick-types" => Ok(Value::list([
            Value::Symbol("png".into()),
            Value::Symbol("jpeg".into()),
            Value::Symbol("gif".into()),
        ])),
        "init-image-library" => {
            need_args(name, args, 1)?;
            let image_type = args[0].as_symbol()?;
            Ok(
                if matches!(
                    image_type,
                    "pbm" | "png" | "jpeg" | "gif" | "svg" | "xbm" | "xpm" | "webp" | "tiff"
                ) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-start" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
        "window-end" => Ok(Value::Integer(interp.buffer.point_max() as i64)),
        "window-width" => Ok(Value::Integer(80)),
        "window-text-pixel-size" => {
            let width = interp
                .buffer
                .buffer_string()
                .lines()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0);
            let height = interp.buffer.buffer_string().lines().count().max(1);
            Ok(Value::cons(
                Value::Integer(width as i64),
                Value::Integer(height as i64),
            ))
        }
        "get-display-property" => {
            need_args(name, args, 2)?;
            let pos = args[0].as_integer()?.max(0) as usize;
            let property = args[1].as_symbol()?;
            let display = interp
                .buffer
                .text_property_at(pos, "display")
                .unwrap_or(Value::Nil);
            Ok(display_property_value(&display, property).unwrap_or(Value::Nil))
        }
        "bidi-find-overridden-directionality" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            Ok(find_bidi_override(interp, start, end)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "redisplay" => Ok(Value::Nil),
        "font-spec" => {
            let mut name_spec = None;
            let mut index = 0;
            while index + 1 < args.len() {
                if let Value::Symbol(keyword) = &args[index]
                    && keyword == ":name"
                {
                    name_spec = Some(string_text(&args[index + 1])?);
                }
                index += 2;
            }
            Ok(interp.create_record(
                "font-spec",
                vec![Value::String(name_spec.unwrap_or_default())],
            ))
        }
        "font-get" => {
            need_args(name, args, 2)?;
            let property = args[1].as_symbol()?;
            let info = font_spec_info(interp, &args[0])?;
            Ok(match property {
                ":family" => info.family.map(Value::Symbol).unwrap_or(Value::Nil),
                ":size" => info.size.map(Value::Float).unwrap_or(Value::Nil),
                ":weight" => info.weight.map(Value::Symbol).unwrap_or(Value::Nil),
                ":slant" => info.slant.map(Value::Symbol).unwrap_or(Value::Nil),
                ":spacing" => info.spacing.map(Value::Integer).unwrap_or(Value::Nil),
                ":foundry" => info.foundry.map(Value::Symbol).unwrap_or(Value::Nil),
                _ => Value::Nil,
            })
        }
        "set-face-attribute" => {
            if args.len() < 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?.to_string();
            let mut index = 2;
            while index + 1 < args.len() {
                let attribute = args[index].as_symbol()?;
                let value = &args[index + 1];
                if attribute == ":inherit" {
                    let inherit = match value {
                        Value::Nil => None,
                        Value::Symbol(symbol) => Some(symbol.clone()),
                        _ => return Err(LispError::TypeError("symbol".into(), value.type_name())),
                    };
                    interp.set_face_inherit_target(&face, inherit)?;
                }
                index += 2;
            }
            Ok(Value::Nil)
        }
        "color-distance" => {
            need_args(name, args, 2)?;
            let left = parse_color_spec(&string_text(&args[0])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let right = parse_color_spec(&string_text(&args[1])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let distance = left
                .into_iter()
                .zip(right)
                .map(|(a, b)| {
                    let diff = i64::from(a) - i64::from(b);
                    diff * diff
                })
                .sum::<i64>();
            Ok(Value::Integer(distance))
        }
        "color-values-from-color-spec" => {
            need_args(name, args, 1)?;
            Ok(parse_color_spec(&string_text(&args[0])?)
                .map(|[r, g, b]| {
                    Value::list([
                        Value::Integer(i64::from(r)),
                        Value::Integer(i64::from(g)),
                        Value::Integer(i64::from(b)),
                    ])
                })
                .unwrap_or(Value::Nil))
        }
        "selected-window" => Ok(Value::Symbol("window".into())),
        "window-buffer" => {
            need_args(name, args, 1)?;
            if is_window_value(interp, &args[0]) {
                Ok(Value::Buffer(
                    interp.current_buffer_id(),
                    interp.buffer.name.clone(),
                ))
            } else {
                Err(LispError::TypeError(
                    "window".into(),
                    args[0].type_name(),
                ))
            }
        }
        "selected-frame" => Ok(Value::Symbol("frame".into())),
        "windowp" => {
            need_args(name, args, 1)?;
            Ok(if is_window_value(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "window-display-table" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::Nil)
        }
        "get-buffer-window" | "minibuffer-window" => Ok(Value::Symbol("window".into())),
        "active-minibuffer-window" => Ok(Value::Nil),
        "set-window-start" | "set-window-point" => Ok(Value::T),
        "facemenu-add-face" => {
            need_args(name, args, 3)?;
            let face = args[0].clone();
            let start = position_from_value(interp, &args[1])?;
            let end = position_from_value(interp, &args[2])?;
            interp.buffer.put_text_property(start, end, "face", face);
            Ok(Value::Nil)
        }

        // ── Reader ──
        "read" => {
            need_args(name, args, 1)?;
            let s = args[0].as_string()?;
            let mut reader = super::reader::Reader::new(s);
            match reader.read()? {
                Some(val) => Ok(val),
                None => Err(LispError::EndOfInput),
            }
        }
        "read-from-string" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let s = string_text(&args[0])?;
            let chars: Vec<char> = s.chars().collect();
            let start = normalize_string_index(args.get(1), 0, chars.len() as i64)? as usize;
            let end = normalize_string_index(args.get(2), chars.len() as i64, chars.len() as i64)?
                as usize;
            let slice: String = chars[start..end].iter().collect();
            let mut reader = super::reader::Reader::new(&slice);
            match reader.read()? {
                Some(val) => Ok(Value::cons(
                    val,
                    Value::Integer((start + reader.position()) as i64),
                )),
                None => Err(LispError::EndOfInput),
            }
        }

        // ── Misc ──
        "error" => {
            let msg = if args.is_empty() {
                "error".to_string()
            } else if let Ok(fmt) = args[0].as_string() {
                // Simple format
                fmt.to_string()
            } else {
                args[0].to_string()
            };
            Err(LispError::Signal(msg))
        }
        "user-error" => {
            let msg = if args.is_empty() {
                "user-error".to_string()
            } else if let Ok(fmt) = string_text(&args[0]) {
                fmt
            } else {
                args[0].to_string()
            };
            Err(LispError::SignalValue(Value::list([
                Value::Symbol("user-error".into()),
                Value::String(msg),
            ])))
        }
        "signal" => {
            if args.is_empty() {
                return Err(LispError::Signal("signal".into()));
            }
            let condition = args[0].clone();
            let data = args.get(1).cloned().unwrap_or(Value::Nil);
            let value = if let Ok(items) = data.to_vec() {
                Value::cons(condition, Value::list(items))
            } else {
                Value::list([condition, data])
            };
            Err(LispError::SignalValue(value))
        }
        "throw" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("throw".into(), args.len()));
            }
            Err(LispError::Throw(args[0].clone(), args[1].clone()))
        }
        "define-error" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "intern" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol_name = string_text(&args[0])?;
            match args.get(1) {
                Some(obarray) if !obarray.is_nil() => {
                    intern_in_obarray(interp, obarray, &symbol_name)
                }
                _ => Ok(Value::Symbol(symbol_name)),
            }
        }
        "intern-soft" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol_name = string_text(&args[0])?;
            match args.get(1) {
                Some(obarray) if !obarray.is_nil() => {
                    intern_soft_in_obarray(interp, obarray, &symbol_name)
                }
                _ => Ok(default_intern_soft_result(interp, &symbol_name, env)),
            }
        }
        "make-symbol" => {
            need_args(name, args, 1)?;
            let s = args[0].as_string()?;
            Ok(Value::Symbol(s.to_string()))
        }
        "autoload" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let function = args[0].as_symbol()?.to_string();
            let file = string_text(&args[1])?;
            let docstring = args.get(2).cloned().unwrap_or(Value::Nil);
            let interactive = args.get(3).cloned().unwrap_or(Value::Nil);
            let kind = args.get(4).cloned().unwrap_or(Value::Nil);
            interp.push_function_binding(
                &function,
                Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String(file),
                    docstring,
                    interactive,
                    kind,
                ]),
            );
            Ok(Value::Symbol(function))
        }
        "set" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            let value = args[1].clone();
            interp.set_variable(symbol, value.clone(), env);
            Ok(value)
        }
        "set-default" => {
            need_args(name, args, 2)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            let value = args[1].clone();
            interp.remove_buffer_local_value(interp.current_buffer_id(), &symbol);
            interp.set_variable(&symbol, value.clone(), &mut Vec::new());
            Ok(value)
        }
        "customize-set-variable" => {
            need_arg_range(name, args, 2, 3)?;
            let symbol = args[0].as_symbol()?;
            interp.set_custom_option(symbol, args[1].clone(), env)
        }
        "symbol-value" => {
            need_args(name, args, 1)?;
            interp.lookup(args[0].as_symbol()?, env)
        }
        "default-value" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            interp
                .default_value(symbol)
                .ok_or_else(|| LispError::Void(symbol.to_string()))
        }
        "interactive-form" => {
            need_args(name, args, 1)?;
            let mut value = resolve_callable(interp, &args[0], env)?;
            if let (Some(symbol), Some((file, _, _))) =
                (args[0].as_symbol().ok(), autoload_parts(&value))
            {
                interp.load_target(&file)?;
                value = interp.lookup_function(symbol, env)?;
            }
            Ok(interactive_form_items(&value)
                .map(Value::list)
                .unwrap_or(Value::Nil))
        }
        "autoloadp" => {
            need_args(name, args, 1)?;
            let autoload = autoload_parts(&args[0]).is_some();
            Ok(if autoload { Value::T } else { Value::Nil })
        }
        "custom-autoload" => {
            need_arg_range(name, args, 2, 3)?;
            let symbol = args[0].as_symbol()?;
            let load = args[1].clone();
            let autoload_flag = if args.get(2).is_some_and(Value::is_truthy) {
                Value::Symbol("noset".into())
            } else {
                Value::T
            };
            interp.put_symbol_property(symbol, "custom-autoload", autoload_flag);

            let existing = interp
                .get_symbol_property(symbol, "custom-loads")
                .unwrap_or(Value::Nil);
            let already_present = existing
                .to_vec()
                .map(|items| items.iter().any(|item| item == &load))
                .unwrap_or(existing == load);
            if !already_present {
                interp.put_symbol_property(symbol, "custom-loads", Value::cons(load, existing));
            }
            Ok(Value::Nil)
        }
        "documentation" => {
            need_args(name, args, 1)?;
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            Ok(function_documentation(interp, &value, env).unwrap_or(Value::Nil))
        }
        "documentation-property" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            Ok(interp
                .get_symbol_property(symbol, property)
                .unwrap_or(Value::Nil))
        }
        "get" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            Ok(interp
                .get_symbol_property(symbol, property)
                .unwrap_or(Value::Nil))
        }
        "makunbound" => {
            need_args(name, args, 1)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            if interp
                .buffer_local_value(interp.current_buffer_id(), &symbol)
                .is_some()
            {
                interp.remove_buffer_local_value(interp.current_buffer_id(), &symbol);
            } else {
                interp.remove_global_binding(&symbol);
            }
            Ok(Value::Symbol(symbol))
        }
        "defvaralias" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let alias = args[0].as_symbol()?.to_string();
            let target = args[1].as_symbol()?.to_string();
            let alias_value = interp.lookup_var(&alias, env);
            let target_value = interp.lookup_var(&target, env);
            interp.set_variable_alias(&alias, &target)?;
            interp.remove_global_binding(&alias);
            interp.remove_buffer_local_value(interp.current_buffer_id(), &alias);
            if let Some(doc) = args.get(2).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(&alias, "variable-documentation", doc.clone());
            }
            if alias_value
                .as_ref()
                .zip(target_value.as_ref())
                .is_some_and(|(left, right)| left != right)
            {
                let warning = Value::list([
                    Value::Symbol("defvaralias".into()),
                    Value::Symbol("losing-value".into()),
                    Value::Symbol(alias.clone()),
                ]);
                call_named_function(interp, "display-warning", &[warning], env)?;
            }
            Ok(Value::Symbol(alias))
        }
        "indirect-variable" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(Value::Symbol(interp.indirect_variable_name(symbol)?))
        }
        "internal-delete-indirect-variable" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            if !interp.remove_variable_alias(symbol) {
                return Err(LispError::Signal("Variable is not indirect".into()));
            }
            interp.remove_global_binding(symbol);
            interp.remove_buffer_local_value(interp.current_buffer_id(), symbol);
            interp.remove_symbol_property(symbol, "variable-documentation");
            Ok(Value::Symbol(symbol.to_string()))
        }
        "internal--define-uninitialized-variable" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            interp.mark_special_variable(symbol);
            if !args[1].is_nil() {
                interp.put_symbol_property(symbol, "variable-documentation", args[1].clone());
            }
            Ok(Value::Symbol(symbol.to_string()))
        }
        "defvar-1" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol = args[0].as_symbol()?;
            interp.mark_special_variable(symbol);
            if interp.lookup_var(symbol, env).is_none() {
                interp.set_variable(symbol, args[1].clone(), &mut Vec::new());
            }
            if let Some(doc) = args.get(2).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(symbol, "variable-documentation", doc.clone());
            }
            Ok(Value::Symbol(symbol.to_string()))
        }
        "defconst-1" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol = args[0].as_symbol()?;
            interp.mark_special_variable(symbol);
            interp.set_variable(symbol, args[1].clone(), &mut Vec::new());
            if let Some(doc) = args.get(2).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(symbol, "variable-documentation", doc.clone());
            }
            interp.put_symbol_property(symbol, "risky-local-variable", Value::T);
            Ok(Value::Symbol(symbol.to_string()))
        }
        "internal-make-var-non-special" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            interp.unmark_special_variable(symbol);
            Ok(Value::Symbol(symbol.to_string()))
        }
        "make-interpreted-closure" => {
            need_arg_range(name, args, 3, 5)?;
            let params = parse_lambda_params_value(&args[0])?;
            let body = args[1].to_vec()?;
            let captured_env = closure_env_from_alist(&args[2])?;
            let mut lambda_body = Vec::new();
            if let Some(doc) = args.get(3).filter(|value| !value.is_nil()) {
                lambda_body.push(doc.clone());
            }
            if let Some(spec) = args.get(4).filter(|value| !value.is_nil()) {
                if spec
                    .to_vec()
                    .ok()
                    .is_some_and(|items| matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "interactive"))
                {
                    lambda_body.push(spec.clone());
                } else {
                    lambda_body.push(Value::list([
                        Value::Symbol("interactive".into()),
                        spec.clone(),
                    ]));
                }
            }
            lambda_body.extend(body);
            Ok(Value::Lambda(params, lambda_body, captured_env))
        }
        "getenv" | "getenv-internal" => {
            need_args(name, args, 1)?;
            let variable = string_text(&args[0])?;
            Ok(std::env::var(&variable)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "ignore" => Ok(Value::Nil),
        "make-obsolete"
        | "make-obsolete-variable"
        | "define-obsolete-function-alias"
        | "define-obsolete-variable-alias" => Ok(Value::Nil),
        "macroexp-warn-and-return" => Ok(args.get(1).cloned().unwrap_or(Value::Nil)),
        "describe-function" => {
            let _ = get_or_create_buffer(interp, "*Help*");
            Ok(Value::Nil)
        }
        "macroexp-quote" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(_, _) | Value::Symbol(_) => {
                    Value::list([Value::Symbol("quote".into()), args[0].clone()])
                }
                other => other.clone(),
            })
        }
        "macroexp-progn" => {
            need_args(name, args, 1)?;
            let forms = args[0].to_vec().unwrap_or_default();
            Ok(match forms.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                many => Value::list(
                    std::iter::once(Value::Symbol("progn".into())).chain(many.iter().cloned()),
                ),
            })
        }
        "macroexp-compiling-p" | "macroexp--dynamic-variable-p" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::Nil)
        }
        "macroexpand-1" | "macroexpand-all" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[0].clone())
        }
        "run-with-timer" | "run-with-idle-timer" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::String("#<timer>".into()))
        }
        "cancel-timer" => Ok(Value::Nil),
        "timerp" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::String(text) if text == "#<timer>")
                    || matches!(&args[0], Value::StringObject(state) if state.borrow().text == "#<timer>")
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "lossage-size" => {
            if args.is_empty() {
                return Ok(Value::Integer(interp.lossage_size));
            }
            let new_size = args[0].as_integer()?;
            if new_size < 100 {
                return Err(LispError::Signal("lossage-size must be >= 100".into()));
            }
            interp.lossage_size = new_size;
            Ok(Value::Integer(new_size))
        }
        "executable-find" => {
            need_args(name, args, 1)?;
            let executable = args[0].as_string()?;
            Ok(find_executable(executable)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "add-hook" => {
            need_args(name, args, 2)?;
            let hook_name = args[0].as_symbol()?.to_string();
            let function = args[1].clone();
            let append = args.get(2).is_some_and(|value| {
                value.is_truthy() && !matches!(value, Value::Symbol(symbol) if symbol == ":local")
            });
            let local = args
                .get(2)
                .is_some_and(|value| matches!(value, Value::Symbol(symbol) if symbol == ":local"))
                || args.get(3).is_some_and(|value| value.is_truthy());
            let mut hooks = if local {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .unwrap_or_default()
            } else {
                interp
                    .lookup_var(&hook_name, env)
                    .map(|value| value.to_vec().unwrap_or_default())
                    .unwrap_or_default()
            };
            if !hooks.contains(&function) {
                if append {
                    hooks.push(function);
                } else {
                    hooks.insert(0, function);
                }
            }
            if local {
                interp.set_buffer_local_hook(interp.current_buffer_id(), &hook_name, hooks);
            } else {
                interp.set_variable(&hook_name, Value::list(hooks), &mut Vec::new());
            }
            Ok(Value::Nil)
        }
        "run-hooks" | "run-mode-hooks" => {
            for hook in args {
                if let Ok(hook_name) = hook.as_symbol() {
                    run_named_hooks(interp, hook_name, env, Some(interp.current_buffer_id()))?;
                }
            }
            Ok(Value::Nil)
        }
        "eval-after-load" => Ok(Value::Nil),
        "run-hook-wrapped" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let hook_name = args[0].as_symbol()?;
            let wrapper = resolve_callable(interp, &args[1], env)?;
            let hook_values = interp
                .lookup_var(hook_name, env)
                .map(|value| value.to_vec().unwrap_or_default())
                .unwrap_or_default();
            for hook in hook_values {
                let mut wrapper_args = vec![hook];
                wrapper_args.extend_from_slice(&args[2..]);
                let value =
                    interp.call_function_value(wrapper.clone(), None, &wrapper_args, env)?;
                if value.is_truthy() {
                    return Ok(value);
                }
            }
            Ok(Value::Nil)
        }
        "mapatoms" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "remove-hook" => {
            need_args(name, args, 2)?;
            let hook_name = args[0].as_symbol()?.to_string();
            let function = args[1].clone();
            let local = args
                .get(2)
                .is_some_and(|value| matches!(value, Value::Symbol(symbol) if symbol == ":local"))
                || args.get(3).is_some_and(|value| value.is_truthy());
            let mut hooks = if local {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .unwrap_or_default()
            } else {
                interp
                    .lookup_var(&hook_name, env)
                    .map(|value| value.to_vec().unwrap_or_default())
                    .unwrap_or_default()
            };
            hooks.retain(|hook| hook != &function);
            if local {
                interp.set_buffer_local_hook(interp.current_buffer_id(), &hook_name, hooks);
            } else {
                interp.set_variable(&hook_name, Value::list(hooks), &mut Vec::new());
            }
            Ok(Value::Nil)
        }
        "define-key" | "define-key-after" | "keymap-set" => {
            if args.len() < 3 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let key = key_sequence_binding_text(&args[1])?;
            keymap_define_binding(interp, &args[0], &key, args[2].clone())?;
            Ok(args[2].clone())
        }
        "keymap-unset" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let key = key_sequence_binding_text(&args[1])?;
            keymap_remove_binding(interp, &args[0], &key)?;
            Ok(Value::Nil)
        }
        "lookup-key" | "keymap-lookup" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let key = key_sequence_binding_text(&args[1])?;
            keymap_lookup_binding(interp, &args[0], &key)
        }
        "key-binding" => {
            need_arg_range(name, args, 1, 3)?;
            let key = key_sequence_binding_text(&args[0])?;
            key_binding(interp, &key, env)
        }
        "keymap-parent" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Record(id)
                    if interp
                        .find_record(*id)
                        .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
                {
                    Ok(interp
                        .find_record(*id)
                        .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT).cloned())
                        .unwrap_or(Value::Nil))
                }
                _ => Ok(Value::Nil),
            }
        }
        "set-keymap-parent" => {
            need_args(name, args, 2)?;
            if let Value::Record(id) = &args[0]
                && let Some(record) = interp.find_record_mut(*id)
                && record.type_name == KEYMAP_RECORD_TYPE
            {
                if record.slots.len() <= KEYMAP_PARENT_SLOT {
                    record.slots.resize(KEYMAP_PARENT_SLOT + 1, Value::Nil);
                }
                record.slots[KEYMAP_PARENT_SLOT] = args[1].clone();
            }
            Ok(Value::Nil)
        }
        "suppress-keymap" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[0].clone())
        }
        "use-local-map" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "current-local-map" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "current-global-map" => {
            need_args(name, args, 0)?;
            Ok(keymap_placeholder(Some("global-map")))
        }
        "global-set-key" | "local-set-key" => {
            need_args(name, args, 2)?;
            Ok(args[1].clone())
        }
        "global-unset-key" | "local-unset-key" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "substitute-key-definition" => {
            if args.len() < 3 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[2].clone())
        }
        "easy-menu-add-item" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[2].clone())
        }
        "tool-bar-local-item-from-menu" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[2].clone())
        }
        "define-widget" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let name = args[0].as_symbol()?;
            let class = args[1].clone();
            let doc = args[2].clone();
            let widget_type = if args.len() > 3 {
                Value::cons(class, Value::list(args[3..].to_vec()))
            } else {
                Value::list([class])
            };
            interp.put_symbol_property(name, "widget-type", widget_type);
            interp.put_symbol_property(name, "widget-documentation", doc);
            Ok(Value::Symbol(name.to_string()))
        }
        "define-button-type" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "defined-colors" => {
            need_args(name, args, 0)?;
            Ok(Value::list([
                Value::String("black".into()),
                Value::String("white".into()),
                Value::String("red".into()),
                Value::String("green".into()),
                Value::String("blue".into()),
            ]))
        }
        "color-defined-p" => {
            need_args(name, args, 1)?;
            let color = string_text(&args[0])?;
            Ok(
                if ["black", "white", "red", "green", "blue"]
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(&color))
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "symbol-function" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(match interp.lookup_function(symbol, env) {
                Ok(value) => value,
                Err(_) if symbol == "benchmark-run" => Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String("benchmark.el".into()),
                    Value::String("Autoloaded benchmark-run.".into()),
                    Value::Nil,
                    Value::Nil,
                ]),
                Err(_) if symbol == "tetris" => Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String("tetris.el".into()),
                    Value::String("Autoloaded tetris.".into()),
                    Value::T,
                    Value::Nil,
                ]),
                Err(_) => Value::String(format!("#<function {}>", symbol)),
            })
        }
        "symbol-name" => {
            need_args(name, args, 1)?;
            let s = args[0].as_symbol()?;
            Ok(Value::String(s.to_string()))
        }
        "user-login-name" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::String(
                current_user_login_name().unwrap_or_else(|| "user".into()),
            ))
        }
        "user-full-name" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let requested = args.first().and_then(|value| {
                if value.is_nil() {
                    None
                } else {
                    string_text(value).ok()
                }
            });
            Ok(user_full_name(requested.as_deref())
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "macroexp-file-name" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("macroexp-file-name", env)
                .unwrap_or(Value::Nil))
        }
        "char-from-name" => {
            need_args(name, args, 1)?;
            let name = args[0].as_string()?;
            let ch = match name {
                "SMILE" => 0x263A,
                _ => return Ok(Value::Nil),
            };
            Ok(Value::Integer(ch))
        }
        "always" => Ok(Value::T),
        "evenp" => {
            need_args(name, args, 1)?;
            Ok(
                if (&integer_like_bigint(interp, &args[0])? & BigInt::from(1u8)).is_zero() {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "seq-subseq" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            seq_subseq(
                &args[0],
                args[1].as_integer()?,
                args.get(2).map(Value::as_integer).transpose()?,
            )
        }
        "text-quoting-style" => Ok(Value::Symbol("grave".into())),
        "file-truename" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].as_string()?.to_string()))
        }
        "group-gid" => Ok(Value::Integer(current_group_id()? as i64)),
        "group-name" => {
            need_args(name, args, 1)?;
            let gid = match &args[0] {
                Value::Integer(value) => *value,
                Value::Float(value) => *value as i64,
                _ => return Err(LispError::Signal("Invalid GID specification".into())),
            };
            Ok(group_name_from_gid(gid)?
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "save-buffer" => {
            let Some(path) = interp.buffer.file.clone() else {
                return Ok(Value::Nil);
            };
            ensure_no_supersession_threat(interp, env)?;
            std::fs::write(&path, interp.buffer.buffer_string())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.set_unmodified();
            interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
            unlock_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "revert-buffer" => {
            revert_current_buffer(interp)?;
            Ok(Value::Nil)
        }
        "lock-buffer" => {
            maybe_lock_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "unlock-buffer" => unlock_current_buffer(interp, env),
        "ask-user-about-supersession-threat" => {
            need_args(name, args, 1)?;
            Ok(Value::T)
        }
        "advice-add" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let function_name = args[0].as_symbol()?.to_string();
            if args[1].as_symbol()? != ":override" {
                return Ok(Value::Nil);
            }
            let advice = match &args[2] {
                Value::Symbol(symbol) => interp.lookup_function(symbol, env)?,
                other => other.clone(),
            };
            interp.push_function_binding(&function_name, advice);
            Ok(Value::Nil)
        }
        "advice-remove" => {
            need_args(name, args, 2)?;
            let function_name = args[0].as_symbol()?.to_string();
            interp.pop_function_binding(&function_name);
            Ok(Value::Nil)
        }
        "remove-function" => {
            need_args(name, args, 2)?;
            Ok(Value::Nil)
        }
        "userlock--handle-unlock-error" => Ok(Value::Nil),
        "recent-auto-save-p" => {
            need_args(name, args, 0)?;
            Ok(if interp.buffer.is_autosaved() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "set-buffer-auto-saved" => {
            need_args(name, args, 0)?;
            interp.buffer.set_autosaved();
            Ok(Value::Nil)
        }
        "clear-buffer-auto-save-failure" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "next-read-file-uses-dialog-p" => {
            need_args(name, args, 0)?;
            let use_dialog = interp
                .lookup_var("use-dialog-box", env)
                .is_some_and(|value| value.is_truthy());
            let use_file_dialog = interp
                .lookup_var("use-file-dialog", env)
                .is_some_and(|value| value.is_truthy());
            Ok(if use_dialog && use_file_dialog {
                Value::T
            } else {
                Value::Nil
            })
        }
        "auto-save-mode" => {
            let enabled = args.first().is_none_or(Value::is_truthy);
            if enabled {
                let path = auto_save_path_for_buffer(&interp.buffer);
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::String(path),
                );
                Ok(Value::T)
            } else {
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::Nil,
                );
                Ok(Value::Nil)
            }
        }
        "do-auto-save" => {
            let path = interp
                .buffer_local_value(interp.current_buffer_id(), "buffer-auto-save-file-name")
                .and_then(|value| string_text(&value).ok())
                .unwrap_or_else(|| auto_save_path_for_buffer(&interp.buffer));
            std::fs::write(&path, interp.buffer.buffer_string())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-auto-save-file-name",
                Value::String(path),
            );
            interp.buffer.set_autosaved();
            Ok(Value::Nil)
        }
        "unix-sync" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "set-binary-mode" => {
            need_args(name, args, 2)?;
            match &args[0] {
                Value::Symbol(stream)
                    if matches!(stream.as_str(), "stdin" | "stdout" | "stderr") =>
                {
                    Ok(Value::Nil)
                }
                _ => Err(LispError::Signal("Invalid stream".into())),
            }
        }
        "obarray-make" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Some(size) = args.first()
                && size.as_integer()? < 0
            {
                return Err(LispError::TypeError("natnump".into(), size.type_name()));
            }
            Ok(make_obarray(interp))
        }
        "make-hash-table" => {
            let mut test = "eql".to_string();
            let mut index = 0usize;
            while index + 1 < args.len() {
                let key = args[index].as_symbol()?;
                if key == ":test" {
                    test = match &args[index + 1] {
                        Value::Symbol(name) => name.clone(),
                        Value::BuiltinFunc(name) => name.clone(),
                        other => {
                            return Err(LispError::TypeError("symbol".into(), other.type_name()));
                        }
                    };
                }
                index += 2;
            }
            Ok(json::make_hash_table(interp, &test, Vec::new()))
        }
        "hash-table-p" => {
            need_args(name, args, 1)?;
            Ok(if json::is_hash_table(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "gethash" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[1].type_name(),
                ));
            };
            let default = args.get(2).cloned().unwrap_or(Value::Nil);
            let value = entries
                .into_iter()
                .find(|(existing_key, _)| {
                    if test == "equal" {
                        values_equal(interp, existing_key, &args[0])
                    } else {
                        existing_key == &args[0]
                    }
                })
                .map(|(_, value)| value)
                .unwrap_or(default);
            Ok(value)
        }
        "puthash" => {
            need_args(name, args, 3)?;
            json::hash_table_put(interp, &args[2], args[0].clone(), args[1].clone())
        }
        "completion-table-case-fold" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[0].clone())
        }
        "hash-table-count" => {
            need_args(name, args, 1)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(Value::Integer(entries.len() as i64))
        }
        "hash-table-keys" => {
            need_args(name, args, 1)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(Value::list(entries.into_iter().map(|(key, _)| key)))
        }
        "try-completion" => try_completion(interp, args, env),
        "all-completions" => all_completions(interp, args, env),
        "test-completion" => test_completion(interp, args, env),
        "map-pairs" => {
            need_args(name, args, 1)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(Value::list(
                entries
                    .into_iter()
                    .map(|(key, value)| Value::cons(key, value)),
            ))
        }
        "profiler-memory-running-p" => Ok(if interp.profiler_memory_running {
            Value::T
        } else {
            Value::Nil
        }),
        "profiler-memory-start" => {
            if interp.profiler_memory_running {
                return Err(LispError::Signal("Memory profiler already running".into()));
            }
            interp.profiler_memory_running = true;
            interp.profiler_memory_log_pending = true;
            Ok(Value::Nil)
        }
        "profiler-memory-stop" => {
            let was_running = interp.profiler_memory_running;
            interp.profiler_memory_running = false;
            Ok(if was_running { Value::T } else { Value::Nil })
        }
        "profiler-memory-log" => {
            if interp.profiler_memory_running || interp.profiler_memory_log_pending {
                if !interp.profiler_memory_running {
                    interp.profiler_memory_log_pending = false;
                }
                Ok(Value::String("#<hash-table>".into()))
            } else {
                Ok(Value::Nil)
            }
        }
        "profiler-cpu-running-p" => Ok(if interp.profiler_cpu_running {
            Value::T
        } else {
            Value::Nil
        }),
        "profiler-cpu-start" => {
            if interp.profiler_cpu_running {
                return Err(LispError::Signal("CPU profiler already running".into()));
            }
            interp.profiler_cpu_running = true;
            interp.profiler_cpu_log_pending = true;
            if let Some(interval) = args.first() {
                interp.set_variable(
                    "profiler-sampling-interval",
                    interval.clone(),
                    &mut Vec::new(),
                );
            }
            Ok(Value::Nil)
        }
        "profiler-cpu-stop" => {
            let was_running = interp.profiler_cpu_running;
            interp.profiler_cpu_running = false;
            Ok(if was_running { Value::T } else { Value::Nil })
        }
        "profiler-cpu-log" => {
            if interp.profiler_cpu_running || interp.profiler_cpu_log_pending {
                if !interp.profiler_cpu_running {
                    interp.profiler_cpu_log_pending = false;
                }
                Ok(Value::String("#<hash-table>".into()))
            } else {
                Ok(Value::Nil)
            }
        }
        "byte-compile-check-lambda-list" => {
            need_args(name, args, 1)?;
            validate_lambda_params(&args[0])?;
            Ok(Value::Nil)
        }
        "byte-compile" => {
            need_args(name, args, 1)?;
            if is_lambda_value(&args[0]) {
                validate_lambda_form(&args[0])?;
            }
            Ok(args[0].clone())
        }
        "funcall-with-delayed-message" => {
            need_args(name, args, 3)?;
            let timeout = numeric_to_f64(interp, &args[0])?;
            let delayed = string_text(&args[1])?;
            let callback = resolve_callable(interp, &args[2], env)?;
            let buffer_id = interp
                .find_buffer("*Messages*")
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer("*Messages*").0);
            let before = interp
                .get_buffer_by_id(buffer_id)
                .map(|buffer| buffer.buffer_string())
                .unwrap_or_default();
            let start = Instant::now();
            let result = interp.call_function_value(callback, None, &[], env)?;
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed >= timeout
                && let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id)
            {
                let current = buffer.buffer_string();
                let suffix = current
                    .strip_prefix(&before)
                    .map(str::to_string)
                    .unwrap_or(current);
                let rewritten = if suffix.is_empty() {
                    format!("{delayed}\n")
                } else {
                    format!("{delayed}\n{suffix}")
                };
                let end = buffer.point_max();
                let _ = buffer.delete_region(1, end);
                buffer.goto_char(1);
                buffer.insert(&(before + &rewritten));
            }
            Ok(result)
        }
        "handler-bind-1" => {
            if args.len() != 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let thunk = resolve_callable(interp, &args[0], env)?;
            let condition = args[1].as_symbol()?.to_string();
            let handler = resolve_callable(interp, &args[2], env)?;
            match interp.call_function_value(thunk, None, &[], env) {
                Ok(value) => Ok(value),
                Err(error) => {
                    let error_type = error.condition_type();
                    if condition != "error" && condition != error_type {
                        return Err(error);
                    }
                    let error_value = error_condition_value(&error);
                    let _ = interp.call_function_value(handler, None, &[error_value], env)?;
                    Err(error)
                }
            }
        }
        "debugger-trap" => Ok(Value::Nil),
        "backtrace-frame--internal" => {
            need_args(name, args, 3)?;
            let callback = resolve_callable(interp, &args[0], env)?;
            let Some((function, frame_args, debug_on_exit)) = interp.current_backtrace_frame()
            else {
                return Ok(Value::Nil);
            };
            let flags = if debug_on_exit {
                Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
            } else {
                Value::Nil
            };
            let function = function.map(Value::Symbol).unwrap_or(Value::Nil);
            interp.call_function_value(
                callback,
                None,
                &[Value::T, function, Value::list(frame_args), flags],
                env,
            )
        }
        "backtrace-debug" => {
            need_arg_range(name, args, 2, 3)?;
            interp.set_current_backtrace_debug(args[1].is_truthy());
            Ok(Value::Nil)
        }
        "backtrace-eval" => {
            need_args(name, args, 3)?;
            interp.lookup(args[0].as_symbol()?, env)
        }
        "backtrace--locals" => {
            need_args(name, args, 2)?;
            let mut locals = Vec::new();
            for frame in env.iter().rev() {
                for (name, value) in frame.iter().rev() {
                    if !locals.iter().any(|entry: &Value| {
                        entry.to_vec().ok().and_then(|items| items.first().cloned())
                            == Some(Value::Symbol(name.clone()))
                    }) {
                        locals.push(Value::cons(Value::Symbol(name.clone()), value.clone()));
                    }
                }
            }
            for name in interp.special_variable_names() {
                if locals.iter().any(|entry: &Value| {
                    entry.to_vec().ok().and_then(|items| items.first().cloned())
                        == Some(Value::Symbol(name.clone()))
                }) {
                    continue;
                }
                if let Some(value) = interp.lookup_var(&name, env) {
                    locals.push(Value::cons(Value::Symbol(name), value));
                }
            }
            Ok(Value::list(locals))
        }
        "current-thread" => Ok(Value::Symbol("main-thread".into())),
        "backtrace--frames-from-thread" => {
            need_args(name, args, 1)?;
            let frames = interp
                .backtrace_frames_snapshot()
                .into_iter()
                .map(|(function, frame_args, debug_on_exit)| {
                    let flags = if debug_on_exit {
                        Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
                    } else {
                        Value::Nil
                    };
                    Value::list([
                        Value::T,
                        function
                            .map(Value::Symbol)
                            .unwrap_or(Value::Symbol("identity".into())),
                        Value::list(frame_args),
                        flags,
                    ])
                })
                .collect::<Vec<_>>();
            Ok(Value::list(if frames.is_empty() {
                vec![Value::list([
                    Value::T,
                    Value::Symbol("identity".into()),
                    Value::Nil,
                    Value::Nil,
                ])]
            } else {
                frames
            }))
        }
        "regexp-quote" => {
            need_args(name, args, 1)?;
            Ok(Value::String(regexp_quote_elisp(&string_text(&args[0])?)))
        }
        "regexp-opt" => {
            need_arg_range(name, args, 1, 2)?;
            let strings = args[0].to_vec()?;
            let mut patterns = strings
                .iter()
                .map(|value| string_text(value).map(|text| regexp_quote_elisp(&text)))
                .collect::<Result<Vec<_>, _>>()?;
            if patterns.is_empty() {
                return Ok(Value::String(String::new()));
            }
            patterns.sort();
            patterns.dedup();
            Ok(Value::String(if patterns.len() == 1 {
                patterns[0].clone()
            } else {
                format!("\\(?:{}\\)", patterns.join("\\|"))
            }))
        }
        "convert-standard-filename" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "abbreviate-file-name" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "files--name-absolute-system-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if file_name_absolute_p(&path) && !path.starts_with('~') {
                Value::T
            } else {
                Value::Nil
            })
        }
        "files--use-insert-directory-program-p" => {
            need_args(name, args, 0)?;
            Ok(
                if interp
                    .lookup_var("ls-lisp-use-insert-directory-program", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp
                        .lookup_var("insert-directory-program", env)
                        .is_some_and(|value| value.is_truthy())
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "insert-directory-wildcard-in-dir-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "connection-local-value" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(args[0].clone())
        }
        "propertized-buffer-identification" => {
            need_args(name, args, 1)?;
            Ok(Value::list([args[0].clone()]))
        }
        "called-interactively-p" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "kill-all-local-variables" => {
            need_args(name, args, 0)?;
            interp.clear_buffer_local_state(interp.current_buffer_id());
            Ok(Value::Nil)
        }
        "hack-dir-local-variables-non-file-buffer" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "force-mode-line-update" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "garbage-collect" => Ok(Value::Nil),
        "num-processors" => {
            need_args(name, args, 0)?;
            let count = std::thread::available_parallelism()
                .map(|count| count.get() as i64)
                .unwrap_or(1);
            Ok(Value::Integer(count.max(1)))
        }
        "type-of" => {
            need_args(name, args, 1)?;
            let name = match &args[0] {
                Value::Nil => "symbol",
                Value::T => "symbol",
                Value::Integer(_) => "integer",
                Value::BigInteger(_) => "integer",
                Value::Float(_) => "float",
                Value::String(_) => "string",
                Value::StringObject(_) => "string",
                Value::Symbol(_) => "symbol",
                Value::Cons(_, _) if is_vector_value(&args[0]) => "vector",
                Value::Cons(_, _) => "cons",
                Value::BuiltinFunc(_) => "subr",
                Value::Lambda(_, _, _) => "cons", // Emacs closures are cons cells
                Value::Buffer(_, _) => "buffer",
                Value::Marker(_) => "marker",
                Value::Overlay(_) => "overlay",
                Value::CharTable(_) => "char-table",
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    return Ok(Value::Symbol(record.type_name.clone()));
                }
                Value::Finalizer(_) => "finalizer",
            };
            Ok(Value::Symbol(name.into()))
        }

        // ── Overlay operations ──
        "make-overlay" => {
            // (make-overlay BEG END &optional BUFFER FRONT-ADVANCE REAR-ADVANCE)
            if !(2..=5).contains(&args.len()) {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let beg = args[0].as_integer()?;
            let end = args[1].as_integer()?;
            let buffer_id = if let Some(buffer_arg) = args.get(2) {
                if buffer_arg.is_nil() {
                    interp.current_buffer_id()
                } else if matches!(buffer_arg, Value::Buffer(_, _)) {
                    interp.resolve_buffer_id(buffer_arg)?
                } else {
                    return Err(LispError::TypeError(
                        "buffer".into(),
                        buffer_arg.type_name(),
                    ));
                }
            } else {
                interp.current_buffer_id()
            };
            let front_advance = args.get(3).is_some_and(|v| v.is_truthy());
            let rear_advance = args.get(4).is_some_and(|v| v.is_truthy());
            let ov_id = interp.alloc_overlay_id();
            let (beg, end) = {
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                clamp_overlay_range(buffer, beg, end)
            };
            let ov = crate::overlay::Overlay::new(
                ov_id,
                beg,
                end,
                buffer_id,
                front_advance,
                rear_advance,
            );
            interp
                .get_buffer_by_id_mut(buffer_id)
                .expect("resolved live buffer id")
                .overlays
                .push(ov);
            Ok(Value::Overlay(ov_id))
        }

        "overlayp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(&args[0], Value::Overlay(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }

        "overlay-buffer" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) if !ov.is_dead() => {
                    let buf_id = ov.buffer_id.unwrap_or(0);
                    let buf_name = interp
                        .buffer_list
                        .iter()
                        .find(|(id, _)| *id == buf_id)
                        .map_or("*unknown*".to_string(), |(_, n)| n.clone());
                    Ok(Value::Buffer(buf_id, buf_name))
                }
                _ => Ok(Value::Nil),
            }
        }

        "overlay-start" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) if !ov.is_dead() => {
                    let pos = if let Some(buffer_id) = ov.buffer_id {
                        let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?;
                        if buffer.is_multibyte() {
                            ov.beg
                        } else {
                            buffer_position_to_byte(buffer, ov.beg).unwrap_or(ov.beg)
                        }
                    } else {
                        ov.beg
                    };
                    Ok(Value::Integer(pos as i64))
                }
                _ => Ok(Value::Nil),
            }
        }

        "overlay-end" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) if !ov.is_dead() => {
                    let pos = if let Some(buffer_id) = ov.buffer_id {
                        let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?;
                        if buffer.is_multibyte() {
                            ov.end
                        } else {
                            buffer_position_to_byte(buffer, ov.end).unwrap_or(ov.end)
                        }
                    } else {
                        ov.end
                    };
                    Ok(Value::Integer(pos as i64))
                }
                _ => Ok(Value::Nil),
            }
        }

        "move-overlay" => {
            // (move-overlay OVERLAY BEG END &optional BUFFER)
            if !(3..=4).contains(&args.len()) {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            let target_buffer_id = if let Some(buffer_arg) = args.get(3) {
                if buffer_arg.is_nil() {
                    interp.current_buffer_id()
                } else if matches!(buffer_arg, Value::Buffer(_, _)) {
                    interp.resolve_buffer_id(buffer_arg)?
                } else {
                    return Err(LispError::TypeError(
                        "buffer".into(),
                        buffer_arg.type_name(),
                    ));
                }
            } else {
                interp.current_buffer_id()
            };
            let beg = args[1].as_integer()?;
            let end = args[2].as_integer()?;
            let (beg, end) = {
                let buffer = interp.get_buffer_by_id(target_buffer_id).ok_or_else(|| {
                    LispError::Signal(format!("No buffer with id {}", target_buffer_id))
                })?;
                clamp_overlay_range(buffer, beg, end)
            };
            let mut overlay = take_overlay(interp, ov_id).unwrap_or_else(|| {
                crate::overlay::Overlay::new(ov_id, beg, end, target_buffer_id, false, false)
            });
            overlay.beg = beg;
            overlay.end = end;
            overlay.buffer_id = Some(target_buffer_id);
            interp
                .get_buffer_by_id_mut(target_buffer_id)
                .expect("resolved live buffer id")
                .overlays
                .push(overlay);
            Ok(Value::Overlay(ov_id))
        }

        "delete-overlay" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            if let Some(ov) = interp.find_overlay_mut(ov_id) {
                ov.buffer_id = None;
            }
            Ok(Value::Nil)
        }

        "delete-all-overlays" => {
            // Remove all overlays (or mark them dead)
            interp.buffer.overlays.clear();
            Ok(Value::Nil)
        }

        "overlay-put" => {
            need_args(name, args, 3)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            let key = match &args[1] {
                Value::Symbol(s) => s.clone(),
                _ => return Err(LispError::TypeError("symbol".into(), args[1].type_name())),
            };
            let value = args[2].clone();
            if let Some(ov) = interp.find_overlay_mut(ov_id) {
                ov.put_prop(&key, value.clone());
            }
            Ok(value)
        }

        "overlay-get" => {
            need_args(name, args, 2)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            let key = match &args[1] {
                Value::Symbol(s) => s.clone(),
                _ => return Err(LispError::TypeError("symbol".into(), args[1].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) => Ok(ov.get_prop(&key).cloned().unwrap_or(Value::Nil)),
                None => Ok(Value::Nil),
            }
        }

        "overlay-properties" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) => {
                    let mut items = Vec::new();
                    for (k, v) in &ov.plist {
                        items.push(Value::Symbol(k.clone()));
                        items.push(v.clone());
                    }
                    Ok(Value::list(items))
                }
                None => Ok(Value::Nil),
            }
        }

        "overlays-at" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            let result: Vec<Value> = interp
                .buffer
                .overlays
                .iter()
                .filter(|ov| !ov.is_dead() && ov.beg <= pos && pos < ov.end)
                .map(|ov| Value::Overlay(ov.id))
                .collect();
            Ok(Value::list(result))
        }

        "overlays-in" => {
            need_args(name, args, 2)?;
            let beg = args[0].as_integer()? as usize;
            let end = args[1].as_integer()? as usize;
            // Z is the un-narrowed buffer end (1-based).
            let z = interp.buffer.size_total() + 1;
            let result: Vec<Value> = interp
                .buffer
                .overlays
                .iter()
                .filter(|ov| {
                    if ov.is_dead() {
                        return false;
                    }
                    if ov.beg == ov.end {
                        // Zero-length overlay at pos P:
                        // Include if P is in [beg, end), or if beg==end and P==beg,
                        // or if P==end and end >= Z (at the real buffer end).
                        return (ov.beg >= beg && ov.beg < end)
                            || (beg == end && ov.beg == beg)
                            || (ov.beg == end && end >= z);
                    }
                    // Non-empty overlay: include if it overlaps [beg, end)
                    ov.beg < end && ov.end > beg
                })
                .map(|ov| Value::Overlay(ov.id))
                .collect();
            Ok(Value::list(result))
        }

        "next-overlay-change" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            let zv = interp.buffer.point_max();
            let mut next = zv;
            for ov in &interp.buffer.overlays {
                if ov.is_dead() {
                    continue;
                }
                if ov.beg > pos && ov.beg < next {
                    next = ov.beg;
                }
                if ov.end > pos && ov.end < next {
                    next = ov.end;
                }
            }
            Ok(Value::Integer(next as i64))
        }

        "previous-overlay-change" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            let begv = interp.buffer.point_min();
            let mut prev = begv;
            for ov in &interp.buffer.overlays {
                if ov.is_dead() {
                    continue;
                }
                if ov.beg < pos && ov.beg > prev {
                    prev = ov.beg;
                }
                if ov.end < pos && ov.end > prev {
                    prev = ov.end;
                }
            }
            Ok(Value::Integer(prev as i64))
        }

        "overlay-lists" => {
            // Returns (BEFORE-LIST . AFTER-LIST) relative to point.
            let pt = interp.buffer.point();
            let mut before = Vec::new();
            let mut after = Vec::new();
            for ov in &interp.buffer.overlays {
                if ov.is_dead() {
                    continue;
                }
                if ov.end <= pt {
                    before.push(Value::Overlay(ov.id));
                } else {
                    after.push(Value::Overlay(ov.id));
                }
            }
            Ok(Value::cons(Value::list(before), Value::list(after)))
        }

        "overlay-recenter" => {
            // In real Emacs this recenters the overlay cache. We're a no-op.
            Ok(Value::Nil)
        }

        "remove-overlays" => {
            // (remove-overlays &optional BEG END NAME VAL)
            let beg = if args.is_empty() || args[0].is_nil() {
                interp.buffer.point_min()
            } else {
                args[0].as_integer()? as usize
            };
            let end = if args.len() < 2 || args[1].is_nil() {
                interp.buffer.point_max()
            } else {
                args[1].as_integer()? as usize
            };
            let filter_name = if args.len() >= 3 {
                args[2].as_symbol().ok().map(|s| s.to_string())
            } else {
                None
            };
            let filter_val = args.get(3).cloned();
            let zv = interp.buffer.point_max();

            // Collect IDs to delete (fully contained or matching)
            let ids_to_delete: Vec<u64> = interp
                .buffer
                .overlays
                .iter()
                .filter(|ov| {
                    if ov.is_dead() {
                        return false;
                    }
                    // Check property filter
                    if let Some(ref fname) = filter_name {
                        let val = ov.get_prop(fname).cloned().unwrap_or(Value::Nil);
                        if let Some(ref fval) = filter_val
                            && val != *fval
                        {
                            return false;
                        }
                    }
                    // Check containment
                    if ov.beg == ov.end {
                        // Zero-length: include if within range
                        ov.beg >= beg && (ov.beg < end || (ov.beg == end && end == zv))
                    } else {
                        ov.beg >= beg && ov.end <= end
                    }
                })
                .map(|ov| ov.id)
                .collect();

            for id in &ids_to_delete {
                if let Some(ov) = interp.find_overlay_mut(*id) {
                    ov.buffer_id = None;
                }
            }
            interp
                .buffer
                .overlays
                .retain(|ov| !ids_to_delete.contains(&ov.id));
            Ok(Value::Nil)
        }

        // ── Plist operations ──
        "plist-get" => {
            need_args(name, args, 2)?;
            let plist = args[0].to_vec()?;
            let key = &args[1];
            let mut i = 0;
            while i + 1 < plist.len() {
                if plist[i] == *key {
                    return Ok(plist[i + 1].clone());
                }
                i += 2;
            }
            Ok(Value::Nil)
        }

        "plist-put" => {
            need_args(name, args, 3)?;
            let mut plist = args[0].to_vec()?;
            let key = &args[1];
            let val = &args[2];
            let mut i = 0;
            let mut found = false;
            while i + 1 < plist.len() {
                if plist[i] == *key {
                    plist[i + 1] = val.clone();
                    found = true;
                    break;
                }
                i += 2;
            }
            if !found {
                plist.push(key.clone());
                plist.push(val.clone());
            }
            Ok(Value::list(plist))
        }

        "plist-member" => {
            need_args(name, args, 2)?;
            let key = &args[1];
            let mut current = args[0].clone();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        if *car == *key {
                            return Ok(Value::Cons(car, cdr));
                        }
                        // Skip the value
                        match *cdr {
                            Value::Cons(_, next_cdr) => current = *next_cdr,
                            _ => return Ok(Value::Nil),
                        }
                    }
                    _ => return Ok(Value::Nil),
                }
            }
        }

        // ── Sort ──
        "sort" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let mut items = args[0].to_vec()?;
            let mut pred = None;
            let mut index = 1usize;
            if let Some(arg) = args.get(index)
                && !matches!(arg, Value::Symbol(symbol) if symbol.starts_with(':'))
            {
                pred = Some(arg.clone());
                index += 1;
            }
            while index + 1 < args.len() {
                match &args[index] {
                    Value::Symbol(keyword) if keyword == ":in-place" => {}
                    Value::Symbol(keyword) if keyword.starts_with(':') => {}
                    _ => return Err(LispError::WrongNumberOfArgs(name.into(), args.len())),
                }
                index += 2;
            }
            if index != args.len() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            // Sort using the predicate. We need to call back into the interpreter.
            // Use a simple insertion sort to avoid issues with the borrow checker
            // and Rust's sort requiring Fn (not FnMut with &mut self).
            let len = items.len();
            for i in 1..len {
                let mut j = i;
                while j > 0 {
                    let result = if let Some(pred) = &pred {
                        let pred_args = [items[j - 1].clone(), items[j].clone()];
                        call_function_value(interp, pred, &pred_args, env)?
                    } else if default_sort_lt(interp, &items[j - 1], &items[j])? {
                        Value::T
                    } else {
                        Value::Nil
                    };
                    // If pred(items[j-1], items[j]) is nil, swap
                    if result.is_nil() {
                        items.swap(j - 1, j);
                        j -= 1;
                    } else {
                        break;
                    }
                }
            }
            Ok(Value::list(items))
        }
        "cl-sort" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let mut key_fn: Option<Value> = None;
            let mut index = 2usize;
            while index + 1 < args.len() {
                if args[index].as_symbol()? == ":key" {
                    key_fn = Some(args[index + 1].clone());
                }
                index += 2;
            }
            let mut items = args[0].to_vec()?;
            let len = items.len();
            for i in 1..len {
                let mut j = i;
                while j > 0 {
                    let left = if let Some(function) = &key_fn {
                        call_function_value(interp, function, &[items[j - 1].clone()], env)?
                    } else {
                        items[j - 1].clone()
                    };
                    let right = if let Some(function) = &key_fn {
                        call_function_value(interp, function, &[items[j].clone()], env)?
                    } else {
                        items[j].clone()
                    };
                    let result = call_function_value(interp, &args[1], &[left, right], env)?;
                    if result.is_nil() {
                        items.swap(j - 1, j);
                        j -= 1;
                    } else {
                        break;
                    }
                }
            }
            Ok(Value::list(items))
        }

        "random" => {
            if args.is_empty() {
                Ok(Value::Integer(rand_simple()))
            } else {
                let limit = args[0].as_integer()?;
                if limit <= 0 {
                    Ok(Value::Integer(0))
                } else {
                    Ok(Value::Integer(rand_simple().unsigned_abs() as i64 % limit))
                }
            }
        }

        "vector" => {
            let mut items = vec![Value::symbol("vector")];
            items.extend(args.iter().cloned());
            Ok(Value::list(items))
        }

        "aref" => {
            need_args(name, args, 2)?;
            let idx = args[1].as_integer()? as usize;
            // Support both list-vectors and strings
            match &args[0] {
                Value::String(_) | Value::StringObject(_) => {
                    match string_like(&args[0])
                        .and_then(|string| string.text.chars().nth(idx).map(|ch| (string, ch)))
                    {
                        Some((string, ch)) => Ok(string_sequence_value(&string, ch)),
                        None => Err(LispError::Signal("Args out of range".into())),
                    }
                }
                Value::CharTable(id) => {
                    let key = args[1].as_integer()? as u32;
                    Ok(interp.char_table_get(*id, key).unwrap_or(Value::Nil))
                }
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    if idx == 0 {
                        Ok(Value::Symbol(record.type_name.clone()))
                    } else {
                        record
                            .slots
                            .get(idx - 1)
                            .cloned()
                            .ok_or_else(|| LispError::Signal("Args out of range".into()))
                    }
                }
                _ => {
                    let items = vector_items(&args[0])?;
                    items
                        .get(idx)
                        .cloned()
                        .ok_or_else(|| LispError::Signal("Args out of range".into()))
                }
            }
        }

        "aset" => {
            need_args(name, args, 3)?;
            match &args[0] {
                Value::CharTable(id) => {
                    let key = args[1].as_integer()? as u32;
                    interp.char_table_set(*id, key, args[2].clone())?;
                    Ok(args[2].clone())
                }
                _ => Ok(args[2].clone()),
            }
        }

        "seq-every-p" => {
            need_args(name, args, 2)?;
            let pred = args[0].clone();
            let seq = args[1].to_vec()?;
            for item in &seq {
                let result = match &pred {
                    Value::BuiltinFunc(fname) => {
                        call(interp, fname, std::slice::from_ref(item), env)?
                    }
                    Value::Lambda(_, _, _) => {
                        call_function_value(interp, &pred, std::slice::from_ref(item), env)?
                    }
                    _ => return Err(LispError::TypeError("function".into(), pred.type_name())),
                };
                if result.is_nil() {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }

        "seq-into" => {
            need_args(name, args, 2)?;
            let items = sequence_values(&args[0])?;
            match args[1].as_symbol()? {
                "list" => Ok(Value::list(items)),
                "vector" => {
                    let mut vector = vec![Value::symbol("vector")];
                    vector.extend(items);
                    Ok(Value::list(vector))
                }
                "string" => {
                    let mut text = String::new();
                    for item in items {
                        let code = item.as_integer()?;
                        let ch = char::from_u32(code as u32).ok_or_else(|| {
                            LispError::Signal(format!("Invalid character: {code}"))
                        })?;
                        text.push(ch);
                    }
                    Ok(Value::String(text))
                }
                kind => Err(LispError::Signal(format!(
                    "seq-into unsupported target type: {kind}"
                ))),
            }
        }

        "nreverse" => {
            need_args(name, args, 1)?;
            if string_like(&args[0]).is_some() {
                reverse_string_like_value(&args[0])
            } else {
                let mut items = args[0].to_vec()?;
                items.reverse();
                Ok(Value::list(items))
            }
        }

        "copy-sequence" => {
            need_args(name, args, 1)?;
            if let Some(string) = string_like(&args[0]) {
                Ok(make_shared_string_value_with_multibyte(
                    string.text,
                    string.props,
                    string.multibyte,
                ))
            } else {
                match &args[0] {
                    Value::CharTable(id) => interp.clone_char_table(*id),
                    Value::Record(id) => interp.copy_record(*id),
                    _ => Ok(args[0].clone()),
                }
            }
        }

        "propertize" => {
            if args.is_empty() || args.len().is_multiple_of(2) {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let text = string_text(&args[0])?;
            let props = args[1..]
                .chunks(2)
                .map(|pair| Ok((pair[0].as_symbol()?.to_string(), pair[1].clone())))
                .collect::<Result<Vec<_>, LispError>>()?;
            let len = text.chars().count();
            Ok(string_like_value(
                text,
                vec![TextPropertySpan {
                    start: 0,
                    end: len,
                    props,
                }],
            ))
        }

        "make-char-table" => {
            need_args(name, args, 1)?;
            let subtype = match &args[0] {
                Value::Nil => None,
                Value::Symbol(symbol) => Some(symbol.clone()),
                other => return Err(LispError::TypeError("symbol".into(), other.type_name())),
            };
            let default = args.get(1).cloned().unwrap_or(Value::Nil);
            Ok(interp.make_char_table(subtype, default))
        }

        "char-table-p" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::CharTable(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }

        "char-table-subtype" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(interp
                .char_table_subtype(id)
                .flatten()
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }

        "char-table-parent" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(interp
                .char_table_parent(id)
                .flatten()
                .and_then(|parent_id| {
                    interp
                        .find_char_table(parent_id)
                        .map(|_| Value::CharTable(parent_id))
                })
                .unwrap_or(Value::Nil))
        }

        "set-char-table-parent" => {
            need_args(name, args, 2)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            let parent = match &args[1] {
                Value::Nil => None,
                Value::CharTable(parent_id) => Some(*parent_id),
                other => {
                    return Err(LispError::TypeError("char-table".into(), other.type_name()));
                }
            };
            interp.set_char_table_parent(id, parent)?;
            Ok(args[1].clone())
        }

        "char-table-extra-slot" => {
            need_args(name, args, 2)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            let slot = args[1].as_integer()?.max(0) as usize;
            Ok(interp.char_table_extra_slot(id, slot).unwrap_or(Value::Nil))
        }

        "set-char-table-extra-slot" => {
            need_args(name, args, 3)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            let slot = args[1].as_integer()?.max(0) as usize;
            interp.set_char_table_extra_slot(id, slot, args[2].clone())?;
            Ok(args[2].clone())
        }

        "char-table-range" => {
            need_args(name, args, 2)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            match char_table_range_spec(&args[1])? {
                None => Ok(interp
                    .find_char_table(id)
                    .map(|table| table.default.clone())
                    .unwrap_or(Value::Nil)),
                Some((start, end)) if start == end => {
                    Ok(interp.char_table_get(id, start).unwrap_or(Value::Nil))
                }
                Some((start, end)) => Ok(interp
                    .char_table_range(id, start, end)
                    .unwrap_or(Value::Nil)),
            }
        }

        "set-char-table-range" => {
            need_args(name, args, 3)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            match char_table_range_spec(&args[1])? {
                None => interp.char_table_set_default(id, args[2].clone())?,
                Some((start, end)) => {
                    interp.char_table_set_range(id, start, end, args[2].clone())?
                }
            }
            Ok(args[2].clone())
        }

        "current-case-table" => Ok(Value::CharTable(interp.current_case_table_id())),

        "standard-case-table" => Ok(Value::CharTable(interp.standard_case_table_id())),

        "set-case-table" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            interp.set_current_case_table(id);
            Ok(args[0].clone())
        }

        "set-standard-case-table" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            interp.set_standard_case_table(id);
            Ok(args[0].clone())
        }

        "make-syntax-table" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| Value::Symbol("emaxx-standard-syntax-table".into())))
        }

        "copy-syntax-table" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| Value::Symbol("emaxx-standard-syntax-table".into())))
        }

        "standard-syntax-table" => Ok(Value::Symbol("emaxx-standard-syntax-table".into())),

        "modify-syntax-entry" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let code = u32::try_from(args[0].as_integer()?)
                .map_err(|_| LispError::Signal("Invalid character".into()))?;
            let syntax = string_text(&args[1])?;
            interp.set_syntax_word_char(normalize_case_key(code), syntax.starts_with('w'));
            Ok(Value::Nil)
        }

        "setcdr" => {
            need_args(name, args, 2)?;
            let updated = if args[0] == args[1] {
                interp.create_record("circular-list", Vec::new())
            } else {
                let Value::Cons(car, _) = &args[0] else {
                    return Err(LispError::TypeError("cons".into(), args[0].type_name()));
                };
                Value::Cons(car.clone(), Box::new(args[1].clone()))
            };
            replace_matching_bindings(env, &args[0], updated.clone());
            Ok(updated)
        }

        "emaxx-default-region-extract-function" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Symbol(method) if method == "bounds" => {
                    let (start, end) = interp
                        .buffer
                        .region()
                        .unwrap_or((interp.buffer.point(), interp.buffer.point()));
                    Ok(Value::list([Value::cons(
                        Value::Integer(start as i64),
                        Value::Integer(end as i64),
                    )]))
                }
                _ => Ok(Value::String(String::new())),
            }
        }

        "make-category-table" => {
            Ok(interp.make_char_table(Some("category-table".into()), Value::String(String::new())))
        }

        "category-table-p" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::CharTable(id)
                    if interp.char_table_subtype(*id).flatten().as_deref()
                        == Some("category-table") =>
                {
                    Value::T
                }
                _ => Value::Nil,
            })
        }

        "standard-category-table" => Ok(Value::CharTable(interp.ensure_standard_category_table())),

        "category-table" => {
            let table = interp
                .buffer_local_value(interp.current_buffer_id(), "category-table")
                .and_then(|value| match value {
                    Value::CharTable(id) => Some(Value::CharTable(id)),
                    _ => None,
                })
                .unwrap_or_else(|| Value::CharTable(interp.ensure_standard_category_table()));
            Ok(table)
        }

        "set-category-table" => {
            need_args(name, args, 1)?;
            let table = match &args[0] {
                Value::CharTable(id) => Value::CharTable(*id),
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "category-table",
                table.clone(),
            );
            Ok(table)
        }

        "define-category" => {
            need_args(name, args, 3)?;
            let category = args[0].as_integer()?;
            let doc = string_text(&args[1])?;
            let table = match args.get(2) {
                Some(Value::CharTable(id)) => *id,
                Some(Value::Nil) | None => interp.ensure_standard_category_table(),
                Some(other) => {
                    return Err(LispError::TypeError("char-table".into(), other.type_name()));
                }
            };
            interp.define_category(table, category as u32, doc)?;
            Ok(Value::Nil)
        }

        "category-docstring" => {
            need_args(name, args, 2)?;
            let category = args[0].as_integer()? as u32;
            let table = match &args[1] {
                Value::CharTable(id) => *id,
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            Ok(interp
                .category_docstring(table, category)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }

        "make-category-set" => {
            need_args(name, args, 1)?;
            let text = string_text(&args[0])?;
            Ok(Value::String(normalize_category_set(&text)))
        }

        "category-set-mnemonics" => {
            need_args(name, args, 1)?;
            let text = string_text(&args[0])?;
            Ok(Value::String(normalize_category_set(&text)))
        }

        "modify-category-entry" => {
            need_args(name, args, 3)?;
            let character = args[0].as_integer()? as u32;
            let category = args[1].as_integer()? as u32;
            let table = match &args[2] {
                Value::CharTable(id) => *id,
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            let reset = args.get(3).is_some_and(Value::is_truthy);
            let category_char = char::from_u32(category)
                .ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let current = interp
                .char_table_get(table, character)
                .and_then(|value| string_like(&value).map(|s| s.text))
                .unwrap_or_default();
            let mut chars: Vec<char> = current.chars().collect();
            if reset {
                chars.retain(|existing| *existing != category_char);
            } else if !chars.contains(&category_char) {
                chars.push(category_char);
            }
            chars.sort_unstable();
            let updated = chars.into_iter().collect::<String>();
            interp.char_table_set(table, character, Value::String(updated))?;
            Ok(Value::Nil)
        }

        "char-category-set" => {
            need_args(name, args, 1)?;
            let character = args[0].as_integer()? as u32;
            let table_id = interp
                .buffer_local_value(interp.current_buffer_id(), "category-table")
                .and_then(|value| match value {
                    Value::CharTable(id) => Some(id),
                    _ => None,
                })
                .unwrap_or_else(|| interp.ensure_standard_category_table());
            Ok(interp
                .char_table_get(table_id, character)
                .unwrap_or_else(|| Value::String(String::new())))
        }

        "copy-category-table" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            interp.clone_char_table(id)
        }

        "translate-region-internal" => {
            need_args(name, args, 3)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let table_id = match &args[2] {
                Value::CharTable(id) => *id,
                _ => {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[2].type_name(),
                    ));
                }
            };
            if interp.char_table_purpose(table_id) != Some("translation-table") {
                return Err(LispError::Signal("Not a translation table".into()));
            }
            let mut changed = 0i64;
            let mut translated = String::new();
            for pos in from..to {
                let source_char = interp
                    .buffer
                    .text_property_at(pos, "emaxx-raw-char")
                    .and_then(|value| value.as_integer().ok())
                    .map(|value| value as u32)
                    .or_else(|| interp.buffer.char_at(pos).map(|ch| ch as u32))
                    .unwrap_or_default();
                let mapped = interp
                    .char_table_get(table_id, source_char)
                    .and_then(|value| value.as_integer().ok())
                    .map(|value| value as u32)
                    .unwrap_or(source_char);
                if mapped != source_char {
                    changed += 1;
                }
                if let Some(mapped_char) = char::from_u32(mapped) {
                    translated.push(mapped_char);
                }
            }
            interp
                .delete_region_current_buffer(from, to)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.goto_char(from);
            interp.insert_current_buffer(&translated);
            Ok(Value::Integer(changed))
        }

        "undo-boundary" => {
            interp.buffer.push_undo_boundary();
            Ok(Value::Nil)
        }

        "undo" => {
            interp.undo_current_buffer()?;
            Ok(Value::Nil)
        }

        "undo-more" => {
            let count = if args.is_empty() {
                1
            } else {
                match &args[0] {
                    Value::Nil => {
                        return Err(LispError::TypeError(
                            "number-or-marker-p".into(),
                            "nil".into(),
                        ));
                    }
                    value => value.as_integer()?,
                }
            };
            for _ in 0..count.max(0) {
                interp.undo_more_current_buffer()?;
            }
            Ok(Value::Nil)
        }

        "take" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()?.max(0) as usize;
            let items = args[1].to_vec()?;
            Ok(Value::list(items.into_iter().take(n)))
        }

        "delete" | "delq" => {
            need_args(name, args, 2)?;
            let elt = &args[0];
            let items = args[1].to_vec()?;
            let filtered: Vec<Value> = items.into_iter().filter(|x| x != elt).collect();
            Ok(Value::list(filtered))
        }

        "make-list" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()?;
            let val = args[1].clone();
            let items: Vec<Value> = (0..n).map(|_| val.clone()).collect();
            Ok(Value::list(items))
        }

        "match-beginning" | "match-end" => {
            need_args(name, args, 1)?;
            let index = args[0].as_integer()?;
            if index < 0 {
                return Err(LispError::Signal("Args out of range".into()));
            }
            let match_data = interp.last_match_data.as_ref().ok_or_else(|| {
                LispError::Signal("No match data, because no search succeeded".into())
            })?;
            let result = match_data
                .get(index as usize)
                .and_then(|entry| *entry)
                .map(|(start, end)| {
                    if name == "match-beginning" {
                        Value::Integer(start as i64)
                    } else {
                        Value::Integer(end as i64)
                    }
                })
                .unwrap_or(Value::Nil);
            Ok(result)
        }
        "match-string" => match_string_impl(interp, args),

        "looking-at" => {
            need_args(name, args, 1)?;
            let pattern = string_text(&args[0])?;
            interp.set_variable(
                "last-looking-at-pattern",
                Value::String(pattern.clone()),
                &mut env.clone(),
            );
            looking_at_impl(interp, &args[0], env)
        }
        "looking-at-p" => {
            need_args(name, args, 1)?;
            let saved_match_data = interp.last_match_data.clone();
            let result = looking_at_impl(interp, &args[0], env);
            interp.last_match_data = saved_match_data;
            result
        }

        "replace-match" => {
            need_args(name, args, 1)?;
            let replacement = string_text(&args[0])?;
            let literal = args.get(2).is_some_and(Value::is_truthy);
            let replace_index = args
                .get(4)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(0)
                .max(0) as usize;
            let match_data = interp
                .last_match_data
                .clone()
                .ok_or_else(|| LispError::Signal("No previous search".into()))?;
            let (start, end) = match_data
                .get(replace_index)
                .and_then(|entry| *entry)
                .or_else(|| match_data.first().and_then(|entry| *entry))
                .ok_or_else(|| LispError::Signal("No previous search".into()))?;
            let replacement = expand_replace_match(interp, &replacement, &match_data, literal)?;
            let replacement_len = replacement.chars().count();
            let overlay_calls =
                overlay_change_hook_calls(&interp.buffer, start, end, start + replacement_len);
            run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
            run_change_hooks(
                interp,
                "before-change-functions",
                &[Value::Integer(start as i64), Value::Integer(end as i64)],
                env,
            )?;
            interp
                .delete_region_current_buffer(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.goto_char(start);
            interp.insert_current_buffer(&replacement);
            run_change_hooks(
                interp,
                "after-change-functions",
                &[
                    Value::Integer(start as i64),
                    Value::Integer((start + replacement_len) as i64),
                    Value::Integer((end - start) as i64),
                ],
                env,
            )?;
            run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
            interp.last_match_data = Some(update_match_data_after_replace(
                &match_data,
                replace_index,
                start,
                end,
                replacement_len,
            ));
            Ok(Value::Nil)
        }

        "replace-region-contents" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let replacement = replacement_content(interp, &args[2])?;
            let saved_point = interp.buffer.point();
            let saved_markers = interp.live_marker_positions_for_buffer(interp.current_buffer_id());
            let removed_len = to.saturating_sub(from);
            let inserted_len = replacement.text.chars().count();
            delete_region_with_hooks(interp, from, to, env)?;
            interp.buffer.goto_char(from);
            insert_text_with_hooks(
                interp,
                &replacement.text,
                &replacement.props,
                false,
                false,
                env,
            )?;
            for (marker_id, original) in saved_markers {
                let Some(original_pos) = original else {
                    continue;
                };
                let insertion_type = interp.marker_insertion_type(marker_id).unwrap_or(false);
                let new_pos = if original_pos < from {
                    original_pos
                } else if original_pos == from {
                    from
                } else if original_pos < to {
                    if insertion_type {
                        from + inserted_len
                    } else {
                        from
                    }
                } else {
                    ((original_pos as isize) + inserted_len as isize - removed_len as isize)
                        .max(from as isize) as usize
                };
                let _ =
                    interp.set_marker(marker_id, Some(new_pos), Some(interp.current_buffer_id()));
            }
            if saved_point > to {
                let target = ((saved_point as isize) + inserted_len as isize - removed_len as isize)
                    .max(from as isize) as usize;
                interp.buffer.goto_char(target);
            } else if (from..=to).contains(&saved_point) {
                let trailing = to.saturating_sub(saved_point);
                let target = from + inserted_len.saturating_sub(trailing);
                interp.buffer.goto_char(target);
            }
            Ok(Value::Nil)
        }
        "flush-lines" => {
            need_args(name, args, 3)?;
            let pattern = string_text(&args[0])?;
            let start = position_from_value(interp, &args[1])?;
            let end = position_from_value(interp, &args[2])?;
            let regex = Regex::new(&translate_elisp_regex(&pattern))
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let text = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let filtered = text
                .split_inclusive('\n')
                .filter(|line| !regex.is_match(&line.to_lowercase()))
                .collect::<String>();
            delete_region_with_hooks(interp, start, end, env)?;
            insert_text_with_hooks(interp, &filtered, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "subst-char-in-region" => {
            need_args(name, args, 4)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let old = args[2].as_integer()? as u32;
            let new = args[3].as_integer()? as u32;
            let old =
                char::from_u32(old).ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let new =
                char::from_u32(new).ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let text = interp
                .buffer
                .buffer_substring(from, to)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let replaced: String = text
                .chars()
                .map(|ch| if ch == old { new } else { ch })
                .collect();
            delete_region_with_hooks(interp, from, to, env)?;
            insert_text_with_hooks(interp, &replaced, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "internal--labeled-narrow-to-region" => {
            need_args(name, args, 3)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let label = args[2].as_symbol()?.to_string();
            let state = Value::list([
                Value::Integer(interp.buffer.point_min() as i64),
                Value::Integer(interp.buffer.point_max() as i64),
            ]);
            interp.set_variable(
                &format!("__emaxx-labeled-restriction-{label}"),
                state,
                &mut env.clone(),
            );
            interp.set_variable(
                "__emaxx-active-labeled-restriction",
                Value::list([Value::Integer(start as i64), Value::Integer(end as i64)]),
                &mut env.clone(),
            );
            interp.buffer.narrow_to_region(start, end);
            Ok(Value::Nil)
        }

        "internal--labeled-widen" => {
            need_args(name, args, 1)?;
            let label = args[0].as_symbol()?.to_string();
            interp.set_variable(
                "__emaxx-active-labeled-restriction",
                Value::Nil,
                &mut env.clone(),
            );
            if let Some(state) =
                interp.lookup_var(&format!("__emaxx-labeled-restriction-{label}"), env)
            {
                let values = state.to_vec()?;
                let start = values
                    .first()
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or(1) as usize;
                let end = values
                    .get(1)
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or((interp.buffer.size_total() + 1) as i64)
                    as usize;
                interp.buffer.narrow_to_region(start, end);
            } else {
                interp.buffer.widen();
            }
            Ok(Value::Nil)
        }

        "transpose-regions" => {
            if args.len() < 4 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let mut start1 = position_from_value(interp, &args[0])?;
            let mut end1 = position_from_value(interp, &args[1])?;
            let mut start2 = position_from_value(interp, &args[2])?;
            let mut end2 = position_from_value(interp, &args[3])?;
            if start1 > end1 {
                std::mem::swap(&mut start1, &mut end1);
            }
            if start2 > end2 {
                std::mem::swap(&mut start2, &mut end2);
            }
            if start2 < end1 {
                std::mem::swap(&mut start1, &mut start2);
                std::mem::swap(&mut end1, &mut end2);
            }
            if start2 < end1 {
                return Err(LispError::Signal("Transposed regions overlap".into()));
            }
            if (start1 == end1 || start2 == end2) && end1 == start2 {
                return Ok(Value::Nil);
            }
            let leave_markers = args.get(4).is_some_and(|value| value.is_truthy());
            let saved_markers = interp.live_marker_positions_for_buffer(interp.current_buffer_id());
            let region1_text = interp
                .buffer
                .buffer_substring(start1, end1)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let region1_props = interp.buffer.substring_property_spans(start1, end1);
            let region2_text = interp
                .buffer
                .buffer_substring(start2, end2)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let region2_props = interp.buffer.substring_property_spans(start2, end2);
            let gap = interp
                .buffer
                .buffer_substring(end1, start2)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let gap_len = gap.chars().count();
            interp
                .delete_region_current_buffer(start2, end2)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp
                .delete_region_current_buffer(start1, end1)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.goto_char(start1);
            interp.insert_current_buffer(&region2_text);
            for span in &region2_props {
                interp.buffer.add_text_properties(
                    start1 + span.start,
                    start1 + span.end,
                    &span.props,
                );
            }
            let insert_region1_at = start1 + region2_text.chars().count() + gap_len;
            interp.buffer.goto_char(insert_region1_at);
            interp.insert_current_buffer(&region1_text);
            for span in &region1_props {
                interp.buffer.add_text_properties(
                    insert_region1_at + span.start,
                    insert_region1_at + span.end,
                    &span.props,
                );
            }
            let len1 = end1 - start1;
            let len2 = end2 - start2;
            let diff = len2 as isize - len1 as isize;
            let amt1 = len2 + (start2 - end1);
            let amt2 = len1 + (start2 - end1);
            for (marker_id, original) in saved_markers {
                let Some(original_pos) = original else {
                    continue;
                };
                let new_pos = if leave_markers || original_pos < start1 || original_pos >= end2 {
                    original_pos
                } else if original_pos < end1 {
                    original_pos + amt1
                } else if original_pos < start2 {
                    ((original_pos as isize) + diff) as usize
                } else {
                    original_pos - amt2
                };
                let _ =
                    interp.set_marker(marker_id, Some(new_pos), Some(interp.current_buffer_id()));
            }
            Ok(Value::Nil)
        }

        "dabbrev-expand" => {
            let point = interp.buffer.point();
            let mut start = point;
            while start > interp.buffer.point_min() {
                let Some(ch) = interp.buffer.char_at(start - 1) else {
                    break;
                };
                if !(ch.is_alphanumeric() || ch == '-' || ch == '_') {
                    break;
                }
                start -= 1;
            }
            let prefix = interp
                .buffer
                .buffer_substring(start, point)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            if prefix.is_empty() {
                return Ok(Value::Nil);
            }
            let haystack = interp.buffer.buffer_string();
            let prefix_start = haystack
                .chars()
                .take(start.saturating_sub(1))
                .map(char::len_utf8)
                .sum::<usize>();
            if let Some(found) = haystack[..prefix_start].rfind(&prefix)
                && let Some(expansion) = expand_symbol_at(&haystack, found, &prefix)
                && expansion != prefix
            {
                delete_region_with_hooks(interp, start, point, env)?;
                interp.buffer.goto_char(start);
                insert_text_with_hooks(interp, &expansion, &[], false, false, env)?;
            }
            Ok(Value::Nil)
        }

        "encode-coding-region" | "decode-coding-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let _ = position_from_value(interp, &args[0])?;
            let _ = position_from_value(interp, &args[1])?;
            let _ = args[2].as_symbol()?;
            Ok(Value::Nil)
        }

        "encode-coding-string" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[1])?;
            let nocopy = args.get(2).is_some_and(Value::is_truthy);
            encode_coding_value(interp, &args[0], coding.as_deref(), nocopy, env)
        }

        "decode-coding-string" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[1])?;
            let nocopy = args.get(2).is_some_and(Value::is_truthy);
            let decoded = decode_coding_text(interp, &args[0], coding.as_deref(), nocopy, env)?;
            if let Some(buffer) = args.get(3)
                && !buffer.is_nil()
            {
                let buffer_id = interp.resolve_buffer_id(buffer)?;
                let saved_buffer_id = interp.current_buffer_id();
                interp.switch_to_buffer_id(buffer_id)?;
                let insert_at = interp.buffer.point();
                let decoded_text = string_text(&decoded)?;
                insert_text_with_hooks(interp, &decoded_text, &[], false, false, env)?;
                interp.buffer.goto_char(insert_at);
                let _ = interp.switch_to_buffer_id(saved_buffer_id);
            }
            Ok(decoded)
        }
        "json-parse-string" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let options = json_parse_options(&args[1..])?;
            Ok(json::parse_value_source(interp, &args[0], &options, true)?.value)
        }
        "json-parse-buffer" => {
            let options = json_parse_options(args)?;
            let start = interp.buffer.point();
            let text = interp
                .buffer
                .buffer_substring(start, interp.buffer.point_max())
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let parsed = json::parse_text_source(
                interp,
                &text,
                interp.buffer.is_multibyte(),
                &options,
                false,
            )?;
            interp
                .buffer
                .goto_char(start + parsed.consumed_source_pos.saturating_sub(1));
            Ok(parsed.value)
        }
        "json-serialize" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let (null_object, false_object) = json_serialize_options(&args[1..])?;
            Ok(json::serialize(interp, &args[0], &null_object, &false_object)?.bytes_value)
        }
        "json-insert" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let (null_object, false_object) = json_serialize_options(&args[1..])?;
            let serialized = json::serialize(interp, &args[0], &null_object, &false_object)?;
            let text = if interp.buffer.is_multibyte() {
                &serialized.text
            } else {
                &serialized.bytes_text
            };
            insert_text_with_hooks(interp, text, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "insert-before-markers" => insert_impl(interp, args, env, false, true),
        "insert-before-markers-and-inherit" => insert_impl(interp, args, env, true, true),

        _ if is_composed_accessor_name(name) => call_composed_accessor(name, args),

        _ => Err(LispError::Signal(format!("Unknown function: {}", name))),
    }
}

fn get_or_create_buffer(interp: &mut Interpreter, name: &str) -> (u64, String) {
    interp
        .find_buffer(name)
        .unwrap_or_else(|| interp.create_buffer(name))
}

fn clamp_overlay_range(buffer: &crate::buffer::Buffer, beg: i64, end: i64) -> (usize, usize) {
    let beg = if buffer.is_multibyte() {
        beg as usize
    } else {
        buffer_byte_to_position_boundary(buffer, beg.max(0) as usize).unwrap_or(1)
    } as i64;
    let end = if buffer.is_multibyte() {
        end as usize
    } else {
        buffer_byte_to_position_boundary(buffer, end.max(0) as usize).unwrap_or(1)
    } as i64;
    let min = buffer.point_min() as i64;
    let max = buffer.point_max() as i64;
    let clamp = |pos: i64| pos.clamp(min, max) as usize;
    let beg = clamp(beg);
    let end = clamp(end);
    if beg > end { (end, beg) } else { (beg, end) }
}

fn take_overlay(interp: &mut Interpreter, overlay_id: u64) -> Option<crate::overlay::Overlay> {
    interp.take_overlay(overlay_id)
}

fn highest_priority_overlay_property(
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
    include_empty_rear_advance: bool,
) -> Option<Value> {
    let mut overlays: Vec<&crate::overlay::Overlay> = buffer
        .overlays
        .iter()
        .filter(|overlay| {
            !overlay.is_dead() && overlay_covers_position(overlay, pos, include_empty_rear_advance)
        })
        .collect();
    overlays.sort_by(|a, b| {
        a.priority()
            .cmp(&b.priority())
            .then_with(|| a.id.cmp(&b.id))
    });
    overlays
        .last()
        .and_then(|overlay| overlay.get_prop(prop).cloned())
}

fn overlay_covers_position(
    overlay: &crate::overlay::Overlay,
    pos: usize,
    include_empty_rear_advance: bool,
) -> bool {
    if overlay.beg == overlay.end {
        include_empty_rear_advance && overlay.rear_advance && overlay.beg == pos
    } else {
        overlay.beg <= pos && pos < overlay.end
    }
}

fn position_from_value(interp: &Interpreter, value: &Value) -> Result<usize, LispError> {
    match value {
        Value::Integer(pos) if *pos >= 0 => Ok(*pos as usize),
        Value::Marker(id) => interp
            .marker_position(*id)
            .ok_or_else(|| LispError::TypeError("integer-or-marker-p".into(), value.type_name())),
        _ => Err(LispError::TypeError(
            "integer-or-marker-p".into(),
            value.type_name(),
        )),
    }
}

fn marker_id_from_value(value: &Value) -> Result<u64, LispError> {
    match value {
        Value::Marker(id) => Ok(*id),
        _ => Err(LispError::TypeError("marker".into(), value.type_name())),
    }
}

fn marker_target(
    interp: &Interpreter,
    value: &Value,
    buffer: Option<&Value>,
) -> Result<(Option<usize>, Option<u64>), LispError> {
    match value {
        Value::Nil => Ok((None, None)),
        Value::Marker(marker_id) => Ok((
            interp.marker_position(*marker_id),
            interp.marker_buffer_id(*marker_id),
        )),
        _ => {
            let position = position_from_value(interp, value)?;
            let buffer_id = if let Some(buffer) = buffer {
                if buffer.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(buffer)?
                }
            } else {
                interp.current_buffer_id()
            };
            Ok((Some(position), Some(buffer_id)))
        }
    }
}

pub(crate) fn vector_items(value: &Value) -> Result<Vec<Value>, LispError> {
    let items = value.to_vec()?;
    if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal")
    {
        Ok(items.into_iter().skip(1).collect())
    } else {
        Ok(items)
    }
}

fn buffer_position_to_byte(buffer: &crate::buffer::Buffer, pos: usize) -> Option<usize> {
    let text = buffer.buffer_string();
    let char_len = text.chars().count();
    if pos == 0 || pos > char_len + 1 {
        return None;
    }
    Some(
        1 + text
            .chars()
            .take(pos - 1)
            .map(char::len_utf8)
            .sum::<usize>(),
    )
}

fn buffer_byte_to_position(buffer: &crate::buffer::Buffer, byte: usize) -> Option<usize> {
    if byte == 0 {
        return None;
    }
    let text = buffer.buffer_string();
    let total_bytes = text.len();
    if byte > total_bytes + 1 {
        return None;
    }
    if byte == total_bytes + 1 {
        return Some(text.chars().count() + 1);
    }
    let mut current_byte = 1usize;
    for (index, ch) in text.chars().enumerate() {
        let next = current_byte + ch.len_utf8();
        if byte == current_byte {
            return Some(index + 1);
        }
        if byte < next {
            return Some(index + 1);
        }
        current_byte = next;
    }
    Some(text.chars().count() + 1)
}

fn buffer_byte_to_position_boundary(buffer: &crate::buffer::Buffer, byte: usize) -> Option<usize> {
    if byte == 0 {
        return None;
    }
    let text = buffer.buffer_string();
    let total_bytes = text.len();
    if byte > total_bytes + 1 {
        return None;
    }
    if byte == total_bytes + 1 {
        return Some(text.chars().count() + 1);
    }
    let mut current_byte = 1usize;
    for (index, ch) in text.chars().enumerate() {
        let next = current_byte + ch.len_utf8();
        if byte == current_byte {
            return Some(index + 1);
        }
        if byte < next {
            return Some(index + 2);
        }
        current_byte = next;
    }
    Some(text.chars().count() + 1)
}

fn char_table_range_spec(value: &Value) -> Result<Option<(u32, u32)>, LispError> {
    match value {
        Value::Nil => Ok(None),
        Value::Integer(codepoint) if *codepoint >= 0 => {
            Ok(Some((*codepoint as u32, *codepoint as u32)))
        }
        Value::Cons(car, cdr) => {
            let start = car.as_integer()?;
            let end = cdr.as_integer()?;
            if start < 0 || end < 0 {
                return Err(LispError::Signal("Args out of range".into()));
            }
            Ok(Some((start as u32, end as u32)))
        }
        other => Err(LispError::TypeError(
            "character-or-cons-or-nil".into(),
            other.type_name(),
        )),
    }
}

fn normalize_category_set(text: &str) -> String {
    let mut chars: Vec<char> = text.chars().collect();
    chars.sort_unstable();
    chars.dedup();
    chars.into_iter().collect()
}

fn normalize_string_index(arg: Option<&Value>, default: i64, len: i64) -> Result<i64, LispError> {
    let Some(value) = arg else {
        return Ok(default);
    };
    if value.is_nil() {
        return Ok(default);
    }
    let raw = value.as_integer()?;
    let index = if raw < 0 { len + raw } else { raw };
    if !(0..=len).contains(&index) {
        return Err(LispError::Signal("Args out of range".into()));
    }
    Ok(index)
}

fn resolve_char_modifiers(value: i64) -> i64 {
    const SHIFT_BIT: i64 = 1 << 25;
    const CTRL_BIT: i64 = 1 << 26;
    const META_BIT: i64 = 1 << 27;
    const CHAR_MASK: i64 = 0x3F_FFFF;

    let mut base = value & CHAR_MASK;
    let meta = value & META_BIT;
    let shift = value & SHIFT_BIT != 0;
    let ctrl = value & CTRL_BIT != 0;

    if shift
        && let Some(ch) = char::from_u32(base as u32)
        && ch.is_ascii_lowercase()
    {
        base = ch.to_ascii_uppercase() as i64;
    }

    if ctrl {
        base = match base {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    }

    base | meta
}

fn position_bytes(interp: &Interpreter, pos: usize) -> Option<usize> {
    buffer_position_to_byte(&interp.buffer, pos)
}

fn byte_to_position(interp: &Interpreter, byte: usize) -> Option<usize> {
    buffer_byte_to_position(&interp.buffer, byte)
}

fn column_at(interp: &Interpreter, env: &Env, line_start: usize, pos: usize) -> usize {
    let mut col = 0usize;
    for p in line_start..pos {
        match interp.buffer.char_at(p) {
            Some(ch) => col = column_after(interp, env, col, p, ch),
            None => break,
        }
    }
    col
}

fn column_after(
    interp: &Interpreter,
    env: &Env,
    current_col: usize,
    pos: usize,
    ch: char,
) -> usize {
    if char_is_invisible(interp, pos) {
        current_col
    } else if ch == '\t' {
        let tab_width = interp
            .lookup_var("tab-width", env)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(8)
            .max(1) as usize;
        (current_col / tab_width + 1) * tab_width
    } else {
        current_col + 1
    }
}

fn char_is_invisible(interp: &Interpreter, pos: usize) -> bool {
    interp
        .buffer
        .text_property_at(pos, "invisible")
        .is_some_and(|value| value.is_truthy())
}

fn compare_buffer_substrings(left: &str, right: &str) -> i64 {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();
    let min_len = left_chars.len().min(right_chars.len());
    for index in 0..min_len {
        if left_chars[index] != right_chars[index] {
            let offset = (index + 1) as i64;
            return if left_chars[index] < right_chars[index] {
                -offset
            } else {
                offset
            };
        }
    }
    if left_chars.len() == right_chars.len() {
        0
    } else {
        let offset = (min_len + 1) as i64;
        if left_chars.len() < right_chars.len() {
            -offset
        } else {
            offset
        }
    }
}

const REGEX_WORD_CLASS: &str = r"[\p{Alphabetic}\p{Number}_\x{2620}]";
const REGEX_NON_WORD_CLASS: &str = r"[^\p{Alphabetic}\p{Number}_\x{2620}]";
const REGEX_WHITESPACE_CLASS: &str = r"[\p{White_Space}]";
const REGEX_NON_WHITESPACE_CLASS: &str = r"[^\p{White_Space}]";

#[derive(Clone)]
enum RegexClassAtom {
    Char(char),
    Posix(String),
}

fn translate_elisp_regex(pattern: &str) -> String {
    translate_elisp_regex_with_point(pattern, "")
}

fn translate_elisp_regex_with_point(pattern: &str, point_assertion: &str) -> String {
    let mut translated = String::new();
    let mut chars = pattern.chars().peekable();
    let mut at_branch_start = true;
    let mut can_repeat_previous = false;
    let mut last_was_quantifier = false;
    while let Some(ch) = chars.next() {
        if ch == '[' {
            translated.push_str(&translate_bracket_expression(&mut chars));
            at_branch_start = false;
            can_repeat_previous = true;
            last_was_quantifier = false;
            continue;
        }
        if ch == '\\' {
            match chars.next() {
                Some('`') => {
                    translated.push_str(r"\A");
                    can_repeat_previous =
                        literalize_postfix_after_absolute_anchor(&mut translated, &mut chars);
                    at_branch_start = false;
                    last_was_quantifier = false;
                }
                Some('\'') => {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, r"\z"));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('(') => {
                    if chars.peek() == Some(&'?') {
                        let mut preview = chars.clone();
                        preview.next();
                        if preview.peek() == Some(&':') {
                            chars.next();
                            chars.next();
                            translated.push_str("(?:");
                            at_branch_start = true;
                            can_repeat_previous = false;
                            last_was_quantifier = false;
                            continue;
                        }
                    }
                    translated.push('(');
                    at_branch_start = true;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some(')') => {
                    translated.push(')');
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('|') => {
                    translated.push('|');
                    at_branch_start = true;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('{') => {
                    translated.push('{');
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('}') => {
                    translated.push('}');
                    if chars.peek() == Some(&'?') {
                        chars.next();
                        if lazy_interval_has_following_context(&chars) {
                            translated.push('?');
                        }
                    } else if let Some(next) = chars.peek().copied()
                        && matches!(next, '*' | '+')
                    {
                        translated.push('\\');
                        translated.push(next);
                        chars.next();
                    }
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = true;
                }
                Some('s') => {
                    translated.push_str(regex_syntax_class(&mut chars, false));
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('S') => {
                    translated.push_str(regex_syntax_class(&mut chars, true));
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('w') => {
                    translated.push_str(REGEX_WORD_CLASS);
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('W') => {
                    translated.push_str(REGEX_NON_WORD_CLASS);
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('b') => {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, r"\b"));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('B') => {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, r"\B"));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('<') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        r"(?<![\p{Alphabetic}\p{Number}_\x{2620}])(?=[\p{Alphabetic}\p{Number}_\x{2620}])",
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('>') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        r"(?<=[\p{Alphabetic}\p{Number}_\x{2620}])(?![\p{Alphabetic}\p{Number}_\x{2620}])",
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('_') => match chars.next() {
                    Some('<') => {
                        translated.push_str(&translate_zero_width_assertion(
                            &mut chars,
                            r"(?<![\p{Alphabetic}\p{Number}_\x{2620}])(?=[\p{Alphabetic}\p{Number}_\x{2620}])",
                        ));
                        at_branch_start = false;
                        can_repeat_previous = false;
                        last_was_quantifier = false;
                    }
                    Some('>') => {
                        translated.push_str(&translate_zero_width_assertion(
                            &mut chars,
                            r"(?<=[\p{Alphabetic}\p{Number}_\x{2620}])(?![\p{Alphabetic}\p{Number}_\x{2620}])",
                        ));
                        at_branch_start = false;
                        can_repeat_previous = false;
                        last_was_quantifier = false;
                    }
                    Some(other) => {
                        translated.push_str(r"\_");
                        translated.push(other);
                        at_branch_start = false;
                        can_repeat_previous = true;
                        last_was_quantifier = false;
                    }
                    None => {
                        translated.push_str(r"\_");
                        at_branch_start = false;
                        can_repeat_previous = true;
                        last_was_quantifier = false;
                    }
                },
                Some('=') => {
                    translated
                        .push_str(&translate_zero_width_assertion(&mut chars, point_assertion));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some(other) => {
                    if other.is_ascii_alphabetic() {
                        translated.push(other);
                    } else {
                        translated.push('\\');
                        translated.push(other);
                    }
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                None => {
                    translated.push('\\');
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
            }
            continue;
        }

        match ch {
            '^' => {
                if at_branch_start {
                    translated.push('^');
                    can_repeat_previous =
                        literalize_postfix_after_absolute_anchor(&mut translated, &mut chars);
                } else {
                    translated.push_str(r"\^");
                    can_repeat_previous = true;
                }
                at_branch_start = false;
                last_was_quantifier = false;
            }
            '$' => {
                if is_dollar_anchor_position(&chars) {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, "$"));
                    can_repeat_previous = false;
                } else {
                    translated.push_str(r"\$");
                    can_repeat_previous = true;
                }
                at_branch_start = false;
                last_was_quantifier = false;
            }
            '*' | '+' | '?' => {
                if can_repeat_previous {
                    if last_was_quantifier {
                        match ch {
                            '?' => translated.push('?'),
                            '*' => {}
                            '+' => {
                                translated.push('\\');
                                translated.push('+');
                                can_repeat_previous = true;
                                last_was_quantifier = false;
                            }
                            _ => {}
                        }
                    } else {
                        translated.push(ch);
                        last_was_quantifier = true;
                    }
                } else {
                    translated.push('\\');
                    translated.push(ch);
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                at_branch_start = false;
            }
            '(' | ')' | '{' | '}' | '|' => {
                translated.push('\\');
                translated.push(ch);
                at_branch_start = false;
                can_repeat_previous = true;
                last_was_quantifier = false;
            }
            _ => {
                translated.push(ch);
                at_branch_start = false;
                can_repeat_previous = true;
                last_was_quantifier = false;
            }
        }
    }
    translated
}

fn is_dollar_anchor_position(chars: &std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let mut preview = chars.clone();
    match preview.next() {
        None => true,
        Some('\\') => matches!(preview.next(), Some(')') | Some('|')),
        _ => false,
    }
}

fn literalize_postfix_after_absolute_anchor(
    translated: &mut String,
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> bool {
    if let Some(next) = chars.peek().copied()
        && matches!(next, '*' | '+' | '?')
    {
        translated.push('\\');
        translated.push(next);
        chars.next();
        return true;
    }
    false
}

fn lazy_interval_has_following_context(chars: &std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let mut preview = chars.clone();
    match preview.next() {
        None => false,
        Some('\\') => !matches!(preview.next(), Some(')') | Some('|')),
        _ => true,
    }
}

fn translate_zero_width_assertion(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    assertion: &str,
) -> String {
    match chars.peek().copied() {
        Some('*') => {
            chars.next();
            if chars.peek() == Some(&'?') {
                chars.next();
            }
            if assertion.is_empty() {
                String::new()
            } else {
                format!("(?:{assertion}|)")
            }
        }
        Some('+') => {
            chars.next();
            if chars.peek() == Some(&'?') {
                chars.next();
            }
            if assertion.is_empty() {
                String::new()
            } else {
                assertion.to_string()
            }
        }
        Some('?') => {
            chars.next();
            if assertion.is_empty() {
                String::new()
            } else {
                format!("(?:{assertion}|)")
            }
        }
        _ => {
            if assertion.is_empty() {
                "(?:)".into()
            } else {
                format!("(?:{assertion})")
            }
        }
    }
}

fn regex_syntax_class(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    negated: bool,
) -> &'static str {
    match chars.next() {
        Some('w') => {
            if negated {
                REGEX_NON_WORD_CLASS
            } else {
                REGEX_WORD_CLASS
            }
        }
        Some('-') => {
            if negated {
                REGEX_NON_WHITESPACE_CLASS
            } else {
                REGEX_WHITESPACE_CLASS
            }
        }
        Some(_) | None => {
            if negated {
                REGEX_NON_WORD_CLASS
            } else {
                REGEX_WORD_CLASS
            }
        }
    }
}

fn regex_posix_class_fragment(name: &str) -> Option<&'static str> {
    match name {
        "alnum" => Some(r"\p{Alphabetic}\p{Number}"),
        "alpha" => Some(r"\p{Alphabetic}"),
        "ascii" => Some(r"\x00-\x7F"),
        "blank" => Some(r"\t\p{Zs}"),
        "cntrl" => Some(r"\x00-\x1F"),
        "digit" => Some("0-9"),
        "graph" => Some(r"\p{Alphabetic}\p{Number}\p{Punctuation}\p{Symbol}\p{Mark}"),
        "lower" => Some(r"\p{Lowercase}"),
        "multibyte" => Some(r"\x{0080}-\x{D7FF}\x{E100}-\x{10FFFF}"),
        "nonascii" => Some(r"\x{0080}-\x{10FFFF}"),
        "print" => Some(r"\p{Alphabetic}\p{Number}\p{Punctuation}\p{Symbol}\p{Mark}\p{Zs}"),
        "punct" => Some(r"\p{Punctuation}"),
        "space" => Some(r"\p{White_Space}"),
        "unibyte" => Some(r"\x00-\x7F\x{E080}-\x{E0FF}"),
        "upper" => Some(r"\p{Uppercase}"),
        "word" => Some(r"\p{Alphabetic}\p{Number}_\x{2620}"),
        "xdigit" => Some("0-9A-Fa-f"),
        _ => None,
    }
}

fn translate_bracket_expression(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut translated = String::from("[");
    let mut saw_atom = false;
    if chars.peek() == Some(&'^') {
        translated.push('^');
        chars.next();
    }

    while let Some(ch) = chars.peek().copied() {
        if ch == ']' && saw_atom {
            chars.next();
            translated.push(']');
            return translated;
        }
        let atom_is_first = !saw_atom;
        if atom_is_first {
            let mut preview = chars.clone();
            if preview.next() == Some('-')
                && preview.next() == Some('-')
                && preview.peek().copied() != Some(']')
                && let Some(RegexClassAtom::Char(end)) = consume_regex_class_atom(&mut preview)
                && let Some(range) = bracket_range_fragment('-', end)
            {
                translated.push_str(&range);
                *chars = preview;
                saw_atom = true;
                continue;
            }
        }
        let Some(atom) = consume_regex_class_atom(chars) else {
            break;
        };
        let mut preview = chars.clone();
        if preview.next() == Some('-')
            && preview.peek().copied() != Some(']')
            && !(atom_is_first && matches!(atom, RegexClassAtom::Char('-' | ']')))
            && let Some(end_atom) = consume_regex_class_atom(&mut preview)
            && let (RegexClassAtom::Char(start), RegexClassAtom::Char(end)) = (&atom, &end_atom)
        {
            if let Some(range) = bracket_range_fragment(*start, *end) {
                translated.push_str(&range);
            } else if is_empty_unicode_raw_range(*start, *end) {
                return "(?!)".into();
            } else {
                return "[".into();
            }
            *chars = preview;
            saw_atom = true;
            continue;
        }
        if let Some(fragment) = regex_class_atom_fragment(&atom) {
            translated.push_str(&fragment);
        } else {
            return "[".into();
        }
        saw_atom = true;
    }

    "[".into()
}

fn consume_regex_class_atom(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<RegexClassAtom> {
    match chars.next()? {
        '[' if chars.peek() == Some(&':') => {
            chars.next();
            let mut name = String::new();
            while let Some(ch) = chars.next() {
                if ch == ':' && chars.peek() == Some(&']') {
                    chars.next();
                    return Some(RegexClassAtom::Posix(name));
                }
                name.push(ch);
            }
            Some(RegexClassAtom::Char('['))
        }
        '\\' => Some(RegexClassAtom::Char('\\')),
        ch => Some(RegexClassAtom::Char(ch)),
    }
}

fn regex_class_atom_fragment(atom: &RegexClassAtom) -> Option<String> {
    match atom {
        RegexClassAtom::Char(ch) => Some(bracket_char_fragment(*ch)),
        RegexClassAtom::Posix(name) => regex_posix_class_fragment(name)
            .map(str::to_string)
            .or(None),
    }
}

fn bracket_char_fragment(ch: char) -> String {
    match ch {
        '[' => r"\[".into(),
        '\\' => r"\\".into(),
        '-' => r"\-".into(),
        ']' => r"\]".into(),
        '^' => r"\^".into(),
        _ => ch.to_string(),
    }
}

fn bracket_range_endpoint_fragment(ch: char) -> String {
    match ch {
        '\\' => r"\\".into(),
        ']' => r"\]".into(),
        _ => ch.to_string(),
    }
}

fn bracket_range_fragment(start: char, end: char) -> Option<String> {
    if (start as u32) <= 0x7F && (end as u32) <= 0x7F && start <= end {
        let mut expanded = String::new();
        for code in (start as u32)..=(end as u32) {
            expanded.push_str(&bracket_char_fragment(char::from_u32(code)?));
        }
        return Some(expanded);
    }
    match (
        raw_byte_from_regex_char(start),
        raw_byte_from_regex_char(end),
    ) {
        (Some(start), Some(end)) if start <= end => Some(format!(
            "{}-{}",
            bracket_range_endpoint_fragment(raw_byte_regex_char(start)),
            bracket_range_endpoint_fragment(raw_byte_regex_char(end))
        )),
        (None, Some(end)) if (start as u32) <= 0x7F => Some(format!(
            "{}-\\x7F{}-{}",
            bracket_range_endpoint_fragment(start),
            bracket_range_endpoint_fragment(raw_byte_regex_char(0x80)),
            bracket_range_endpoint_fragment(raw_byte_regex_char(end))
        )),
        (None, None) if start <= end => Some(format!(
            "{}-{}",
            bracket_range_endpoint_fragment(start),
            bracket_range_endpoint_fragment(end)
        )),
        _ => None,
    }
}

fn is_empty_unicode_raw_range(start: char, end: char) -> bool {
    match (
        raw_byte_from_regex_char(start),
        raw_byte_from_regex_char(end),
    ) {
        (None, Some(_)) => (start as u32) > 0x7F,
        (Some(_), None) => (end as u32) > 0x7F,
        _ => false,
    }
}

fn regexp_quote_elisp(pattern: &str) -> String {
    let mut quoted = String::new();
    for ch in pattern.chars() {
        match ch {
            '(' => quoted.push_str("[(]"),
            ')' => quoted.push_str("[)]"),
            '[' => quoted.push_str("\\["),
            ']' => quoted.push_str("\\]"),
            '{' => quoted.push_str("\\{"),
            '}' => quoted.push_str("\\}"),
            '\\' => quoted.push_str("[\\\\]"),
            '.' | '*' | '+' | '?' | '^' | '$' | '|' => {
                quoted.push('\\');
                quoted.push(ch);
            }
            _ => quoted.push(ch),
        }
    }
    quoted
}

fn invalid_regexp_error(message: impl Into<String>) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("invalid-regexp".into()),
        Value::String(message.into()),
    ]))
}

fn validate_elisp_regex(pattern: &str) -> Result<(), LispError> {
    let mut chars = pattern.chars().peekable();
    let mut next_group = 0usize;
    let mut max_closed_group = 0usize;
    let mut open_groups = Vec::new();
    let mut in_class = false;
    while let Some(ch) = chars.next() {
        if in_class {
            match ch {
                '[' if chars.peek() == Some(&':') => {
                    chars.next();
                    let mut name = String::new();
                    while let Some(next) = chars.next() {
                        if next == ':' && chars.peek() == Some(&']') {
                            chars.next();
                            if regex_posix_class_fragment(&name).is_none() {
                                return Err(invalid_regexp_error("Invalid character class"));
                            }
                            break;
                        }
                        name.push(next);
                    }
                }
                ']' => in_class = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '[' => in_class = true,
            '\\' => match chars.next() {
                Some('(') => {
                    next_group += 1;
                    open_groups.push(next_group);
                }
                Some(')') => {
                    let Some(group) = open_groups.pop() else {
                        return Err(invalid_regexp_error("Unmatched )"));
                    };
                    max_closed_group = max_closed_group.max(group);
                }
                Some(digit @ '1'..='9') => {
                    let backref = digit.to_digit(10).unwrap_or(0) as usize;
                    if backref > max_closed_group {
                        return Err(invalid_regexp_error("Invalid back reference"));
                    }
                }
                Some('{') => {
                    let mut preview = chars.clone();
                    let mut found_close = false;
                    while let Some(next) = preview.next() {
                        if next == '\\' && preview.next() == Some('}') {
                            found_close = true;
                            break;
                        }
                    }
                    if !found_close {
                        return Err(invalid_regexp_error("Unmatched \\{"));
                    }
                }
                Some(_) | None => {}
            },
            _ => {}
        }
    }
    if !open_groups.is_empty() {
        return Err(invalid_regexp_error("Unmatched ("));
    }
    Ok(())
}

fn enforce_elisp_repeat_limit(pattern: &str) -> Result<(), LispError> {
    static REPEAT_PATTERN: OnceLock<Regex> = OnceLock::new();
    let regex = REPEAT_PATTERN.get_or_init(|| {
        Regex::new(r"\\\{([0-9]+)(?:,([0-9]*))?\\\}").expect("repeat limit regex is valid")
    });
    for captures in regex.captures_iter(pattern) {
        let lower = captures
            .get(1)
            .and_then(|value| value.as_str().parse::<usize>().ok())
            .unwrap_or(0);
        let upper = captures.get(2).and_then(|value| {
            let raw = value.as_str();
            if raw.is_empty() {
                None
            } else {
                raw.parse::<usize>().ok()
            }
        });
        if lower > 65_535 || upper.is_some_and(|value| value > 65_535) {
            return Err(invalid_regexp_error("Repeat count too large"));
        }
    }
    Ok(())
}

fn compile_elisp_regex(
    interp: &Interpreter,
    pattern: &StringLike,
    env: &Env,
    point_assertion: &str,
) -> Result<FancyRegex, LispError> {
    enforce_elisp_repeat_limit(&pattern.text)?;
    let translated = translate_elisp_regex_with_point(&pattern.text, point_assertion);
    let case_fold = interp
        .lookup_var("case-fold-search", env)
        .is_some_and(|value| value.is_truthy());
    let rendered = if case_fold {
        format!("(?mi:{translated})")
    } else {
        format!("(?m:{translated})")
    };
    FancyRegex::new(&rendered).map_err(|error| invalid_regexp_error(error.to_string()))
}

fn set_match_data(
    interp: &mut Interpreter,
    start_pos: usize,
    haystack: &str,
    captures: &fancy_regex::Captures<'_>,
) {
    interp.last_match_data = Some(
        (0..captures.len())
            .map(|index| captures.get(index))
            .map(|matched| {
                matched.map(|matched| {
                    let start = start_pos + haystack[..matched.start()].chars().count();
                    let end = start_pos + haystack[..matched.end()].chars().count();
                    (start, end)
                })
            })
            .collect(),
    );
}

fn string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
    update_match_data: bool,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            if update_match_data {
                "string-match".into()
            } else {
                "string-match-p".into()
            },
            args.len(),
        ));
    }
    let pattern = string_like(&args[0])
        .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
    let haystack = string_like(&args[1])
        .ok_or_else(|| LispError::TypeError("string".into(), args[1].type_name()))?;
    validate_elisp_regex(&pattern.text)?;
    let haystack_len = haystack.text.chars().count() as i64;
    let start = normalize_string_index(args.get(2), 0, haystack_len)? as usize;
    let tail: String = haystack.text.chars().skip(start).collect();
    let regex = compile_elisp_regex(interp, &pattern, env, "")?;
    let captures = regex
        .captures(&tail)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if let Some(captures) = captures
        && let Some(matched) = captures.get(0)
    {
        let match_start = start + tail[..matched.start()].chars().count();
        if update_match_data {
            set_match_data(interp, start, &tail, &captures);
        }
        Ok(Value::Integer(match_start as i64))
    } else {
        Ok(Value::Nil)
    }
}

fn split_string_impl(
    string: &Value,
    separator: Option<&Value>,
    omit_nulls: Option<&Value>,
) -> Result<Value, LispError> {
    let text = string_text(string)?;
    let separator = separator
        .filter(|value| !value.is_nil())
        .map(string_text)
        .transpose()?;
    let omit_nulls = omit_nulls.is_some_and(Value::is_truthy);
    let parts = if let Some(separator) = separator {
        if separator.is_empty() {
            text.chars()
                .map(|ch| ch.to_string())
                .filter(|part| !(omit_nulls && part.is_empty()))
                .map(Value::String)
                .collect::<Vec<_>>()
        } else {
            text.split(&separator)
                .filter(|part| !(omit_nulls && part.is_empty()))
                .map(|part| Value::String(part.to_string()))
                .collect::<Vec<_>>()
        }
    } else {
        text.split_whitespace()
            .map(|part| Value::String(part.to_string()))
            .collect::<Vec<_>>()
    };
    Ok(Value::list(parts))
}

struct SkipCharsSpec {
    negate: bool,
    literals: Vec<char>,
    ranges: Vec<(char, char)>,
    classes: Vec<String>,
}

fn parse_skip_chars_spec(spec: &str) -> SkipCharsSpec {
    let mut chars = spec.chars().peekable();
    let negate = if chars.peek() == Some(&'^') {
        chars.next();
        true
    } else {
        false
    };
    let mut literals = Vec::new();
    let mut ranges = Vec::new();
    let mut classes = Vec::new();

    while let Some(ch) = chars.next() {
        if ch == '[' && chars.peek() == Some(&':') {
            chars.next();
            let mut name = String::new();
            while let Some(next) = chars.next() {
                if next == ':' && chars.peek() == Some(&']') {
                    chars.next();
                    classes.push(name);
                    break;
                }
                name.push(next);
            }
            continue;
        }
        let mut preview = chars.clone();
        if preview.next() == Some('-')
            && let Some(end) = preview.next()
        {
            chars.next();
            chars.next();
            ranges.push((ch, end));
            continue;
        }
        literals.push(ch);
    }

    SkipCharsSpec {
        negate,
        literals,
        ranges,
        classes,
    }
}

fn skip_char_matches_class(ch: char, class: &str) -> bool {
    let code = raw_byte_from_regex_char(ch)
        .map(u32::from)
        .unwrap_or(ch as u32);
    match class {
        "alnum" => ch.is_alphanumeric(),
        "alpha" => ch.is_alphabetic(),
        "ascii" => code <= 0x7F,
        "blank" => matches!(ch, ' ' | '\t') || (ch.is_whitespace() && ch != '\n' && ch != '\r'),
        "cntrl" => code <= 0x1F,
        "digit" => ch.is_ascii_digit(),
        "graph" => !skip_char_matches_class(ch, "space") && !skip_char_matches_class(ch, "cntrl"),
        "lower" => ch.is_lowercase(),
        "multibyte" => !is_raw_byte_regex_char(ch) && (ch as u32) > 0xFF,
        "nonascii" => code > 0x7F || (!is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F),
        "print" => {
            !skip_char_matches_class(ch, "cntrl")
                && !matches!(ch, '\n' | '\r' | '\t' | '\u{000B}' | '\u{000C}')
        }
        "punct" => ch.is_ascii_punctuation(),
        "space" => ch.is_whitespace(),
        "unibyte" => code <= 0xFF,
        "upper" => ch.is_uppercase(),
        "word" => ch.is_alphanumeric() || ch == '_' || ch == '\u{2620}',
        "xdigit" => ch.is_ascii_hexdigit(),
        _ => false,
    }
}

fn skip_char_matches_spec(ch: char, spec: &SkipCharsSpec) -> bool {
    let literal_match = spec.literals.contains(&ch);
    let range_match = spec
        .ranges
        .iter()
        .any(|(start, end)| *start <= ch && ch <= *end);
    let class_match = spec
        .classes
        .iter()
        .any(|class| skip_char_matches_class(ch, class));
    let matched = literal_match || range_match || class_match;
    if spec.negate { !matched } else { matched }
}

fn skip_chars_forward_impl(
    interp: &mut Interpreter,
    spec_value: &Value,
    limit_value: Option<&Value>,
) -> Result<Value, LispError> {
    let spec = parse_skip_chars_spec(&string_text(spec_value)?);
    let limit = if let Some(limit_value) = limit_value {
        if limit_value.is_nil() {
            interp.buffer.point_max()
        } else {
            position_from_value(interp, limit_value)?
        }
    } else {
        interp.buffer.point_max()
    };
    let start = interp.buffer.point();
    while interp.buffer.point() < limit {
        let Some(ch) = interp.buffer.char_at(interp.buffer.point()) else {
            break;
        };
        if !skip_char_matches_spec(ch, &spec) {
            break;
        }
        let _ = interp.buffer.forward_char(1);
    }
    Ok(Value::Integer(
        interp.buffer.point().saturating_sub(start) as i64
    ))
}

fn skip_chars_backward_impl(
    interp: &mut Interpreter,
    spec_value: &Value,
    limit_value: Option<&Value>,
) -> Result<Value, LispError> {
    let spec = parse_skip_chars_spec(&string_text(spec_value)?);
    let limit = if let Some(limit_value) = limit_value {
        if limit_value.is_nil() {
            interp.buffer.point_min()
        } else {
            position_from_value(interp, limit_value)?
        }
    } else {
        interp.buffer.point_min()
    };
    let start = interp.buffer.point();
    while interp.buffer.point() > limit {
        let Some(ch) = interp.buffer.char_before() else {
            break;
        };
        if !skip_char_matches_spec(ch, &spec) {
            break;
        }
        let _ = interp.buffer.forward_char(-1);
    }
    Ok(Value::Integer(interp.buffer.point() as i64 - start as i64))
}

fn syntax_class_matches(spec: &str, ch: char) -> bool {
    match spec {
        " " => matches!(ch, ' ' | '\t' | '\n' | '\r' | '\u{000B}' | '\u{000C}'),
        _ => false,
    }
}

fn skip_syntax_impl(
    interp: &mut Interpreter,
    syntax_value: &Value,
    limit_value: Option<&Value>,
    forward: bool,
) -> Result<Value, LispError> {
    let syntax = string_text(syntax_value)?;
    let limit = if let Some(limit_value) = limit_value {
        if limit_value.is_nil() {
            if forward {
                interp.buffer.point_max()
            } else {
                interp.buffer.point_min()
            }
        } else {
            position_from_value(interp, limit_value)?
        }
    } else if forward {
        interp.buffer.point_max()
    } else {
        interp.buffer.point_min()
    };
    let start = interp.buffer.point();
    if forward {
        while interp.buffer.point() < limit {
            let Some(ch) = interp.buffer.char_at(interp.buffer.point()) else {
                break;
            };
            if !syntax_class_matches(&syntax, ch) {
                break;
            }
            let _ = interp.buffer.forward_char(1);
        }
    } else {
        while interp.buffer.point() > limit {
            let Some(ch) = interp.buffer.char_before() else {
                break;
            };
            if !syntax_class_matches(&syntax, ch) {
                break;
            }
            let _ = interp.buffer.forward_char(-1);
        }
    }
    Ok(Value::Integer(interp.buffer.point() as i64 - start as i64))
}

fn forward_comment_impl(
    interp: &mut Interpreter,
    count_value: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let count = count_value.map_or(Ok(1), Value::as_integer)?;
    if count < 0 {
        return Ok(Value::Nil);
    }
    let mut remaining = count as usize;
    while remaining > 0 {
        let _ = skip_syntax_impl(interp, &Value::String(" ".into()), None, true)?;
        let Some(start_skip) = interp.lookup_var("comment-start-skip", env) else {
            return Ok(Value::Nil);
        };
        let Some(start_skip) = string_like(&start_skip) else {
            return Ok(Value::Nil);
        };
        let regex = compile_elisp_regex(interp, &start_skip, env, r"\A")?;
        let pos = interp.buffer.point();
        let tail = interp
            .buffer
            .buffer_substring(pos, interp.buffer.point_max())
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let Some(captures) = regex
            .captures(&tail)
            .map_err(|error| LispError::Signal(error.to_string()))?
        else {
            return Ok(Value::Nil);
        };
        let Some(matched) = captures.get(0) else {
            return Ok(Value::Nil);
        };
        if matched.start() != 0 {
            return Ok(Value::Nil);
        }
        set_match_data(interp, pos, &tail, &captures);
        interp
            .buffer
            .goto_char(pos + tail[..matched.end()].chars().count());

        let Some(end_skip) = interp.lookup_var("comment-end-skip", env) else {
            return Ok(Value::Nil);
        };
        let Some(end_skip) = string_like(&end_skip) else {
            return Ok(Value::Nil);
        };
        let regex = compile_elisp_regex(interp, &end_skip, env, "")?;
        let pos = interp.buffer.point();
        let tail = interp
            .buffer
            .buffer_substring(pos, interp.buffer.point_max())
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let Some(captures) = regex
            .captures(&tail)
            .map_err(|error| LispError::Signal(error.to_string()))?
        else {
            return Ok(Value::Nil);
        };
        let Some(matched) = captures.get(0) else {
            return Ok(Value::Nil);
        };
        set_match_data(interp, pos, &tail, &captures);
        interp
            .buffer
            .goto_char(pos + tail[..matched.end()].chars().count());
        remaining -= 1;
    }
    Ok(Value::T)
}

fn prefix_numeric_value(value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Nil => Ok(Value::Integer(1)),
        Value::Integer(_) | Value::BigInteger(_) => Ok(value.clone()),
        Value::Symbol(symbol) if symbol == "-" => Ok(Value::Integer(-1)),
        Value::Cons(_, _) => {
            let items = value.to_vec()?;
            if items.len() == 1 {
                prefix_numeric_value(&items[0])
            } else {
                Err(LispError::TypeError("number".into(), value.type_name()))
            }
        }
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn match_string_impl(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 3 {
        return Err(LispError::WrongNumberOfArgs(
            "match-string".into(),
            args.len(),
        ));
    }
    let index = args[0].as_integer()?;
    if index < 0 {
        return Err(LispError::Signal("Args out of range".into()));
    }
    let match_data = interp
        .last_match_data
        .as_ref()
        .ok_or_else(|| LispError::Signal("No match data, because no search succeeded".into()))?;
    let Some((start, end)) = match_data.get(index as usize).and_then(|entry| *entry) else {
        return Ok(Value::Nil);
    };
    if let Some(string) = args.get(1).filter(|value| !value.is_nil()) {
        let string = string_like(string)
            .ok_or_else(|| LispError::TypeError("string".into(), string.type_name()))?;
        let chars: Vec<char> = string.text.chars().collect();
        if end > chars.len() {
            return Ok(Value::Nil);
        }
        return Ok(Value::String(chars[start..end].iter().collect()));
    }
    interp
        .buffer
        .buffer_substring(start, end)
        .map(Value::String)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn looking_at_impl(
    interp: &mut Interpreter,
    pattern_value: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let pattern = string_like(pattern_value)
        .ok_or_else(|| LispError::TypeError("string".into(), pattern_value.type_name()))?;
    let regex = compile_elisp_regex(interp, &pattern, env, r"\A")?;
    let pos = interp.buffer.point();
    let tail = interp
        .buffer
        .buffer_substring(pos, interp.buffer.point_max())
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if let Some(captures) = regex
        .captures(&tail)
        .map_err(|error| LispError::Signal(error.to_string()))?
        && let Some(matched) = captures.get(0)
        && matched.start() == 0
    {
        set_match_data(interp, pos, &tail, &captures);
        Ok(Value::T)
    } else {
        interp.last_match_data = None;
        Ok(Value::Nil)
    }
}

fn buffer_regex_search(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
    forward: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            if forward {
                "re-search-forward".into()
            } else {
                "re-search-backward".into()
            },
            args.len(),
        ));
    }
    let pattern = string_like(&args[0])
        .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
    let regex = compile_elisp_regex(interp, &pattern, env, if forward { r"\A" } else { r"\z" })?;
    let noerror = args.get(2).is_some_and(Value::is_truthy);

    if forward {
        let start = interp.buffer.point();
        let tail = interp
            .buffer
            .buffer_substring(start, interp.buffer.point_max())
            .map_err(|error| LispError::Signal(error.to_string()))?;
        if let Some(captures) = regex
            .captures(&tail)
            .map_err(|error| LispError::Signal(error.to_string()))?
            && let Some(matched) = captures.get(0)
        {
            let pos = start + tail[..matched.end()].chars().count();
            set_match_data(interp, start, &tail, &captures);
            interp.buffer.goto_char(pos);
            return Ok(Value::Integer(pos as i64));
        }
    } else {
        let prefix = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let prefix_chars: Vec<char> = prefix.chars().collect();
        let mut best_match: Option<(usize, usize, usize)> = None;
        let prefix_len = prefix_chars.len();
        for offset in 0..=prefix_len {
            let tail: String = prefix_chars[offset..].iter().collect();
            let Some(captures) = regex
                .captures(&tail)
                .map_err(|error| LispError::Signal(error.to_string()))?
            else {
                continue;
            };
            let Some(matched) = captures.get(0) else {
                continue;
            };
            let match_start = 1 + offset + tail[..matched.start()].chars().count();
            let match_end = 1 + offset + tail[..matched.end()].chars().count();
            if match_end <= interp.buffer.point()
                && best_match.is_none_or(|(best_start, best_end, _)| {
                    match_start > best_start || (match_start == best_start && match_end > best_end)
                })
            {
                best_match = Some((match_start, match_end, match_start.saturating_sub(1)));
            }
        }
        if let Some((match_start, _, offset)) = best_match {
            let tail: String = prefix_chars[offset..].iter().collect();
            if let Some(captures) = regex
                .captures(&tail)
                .map_err(|error| LispError::Signal(error.to_string()))?
                && captures.get(0).is_some()
            {
                let start_pos = 1 + offset;
                set_match_data(interp, start_pos, &tail, &captures);
                interp.buffer.goto_char(match_start);
                return Ok(Value::Integer(match_start as i64));
            }
        }
    }

    interp.last_match_data = None;
    if noerror {
        Ok(Value::Nil)
    } else {
        Err(LispError::SignalValue(Value::list([
            Value::Symbol("search-failed".into()),
            Value::String(pattern.text),
        ])))
    }
}

fn expand_replace_match(
    interp: &Interpreter,
    replacement: &str,
    match_data: &[Option<(usize, usize)>],
    literal: bool,
) -> Result<String, LispError> {
    if literal {
        return Ok(replacement.to_string());
    }
    let chars: Vec<char> = replacement.chars().collect();
    let mut expanded = String::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '\\'
            && let Some(next) = chars.get(index + 1).copied()
        {
            match next {
                '&' => expanded.push_str(&match_text_from_buffer(interp, match_data, 0)?),
                '1'..='9' => {
                    let capture_index = next.to_digit(10).unwrap_or(0) as usize;
                    expanded.push_str(&match_text_from_buffer(interp, match_data, capture_index)?);
                }
                '\\' => expanded.push('\\'),
                other => expanded.push(other),
            }
            index += 2;
            continue;
        }
        expanded.push(chars[index]);
        index += 1;
    }
    Ok(expanded)
}

fn expand_replace_match_text(
    replacement: &str,
    match_data: &[Option<(usize, usize)>],
    literal: bool,
    source: &str,
) -> Result<String, LispError> {
    if literal {
        return Ok(replacement.to_string());
    }
    let chars: Vec<char> = replacement.chars().collect();
    let mut expanded = String::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '\\'
            && let Some(next) = chars.get(index + 1).copied()
        {
            match next {
                '&' => expanded.push_str(&match_text_from_string(source, match_data, 0)),
                '1'..='9' => {
                    let capture_index = next.to_digit(10).unwrap_or(0) as usize;
                    expanded.push_str(&match_text_from_string(source, match_data, capture_index));
                }
                '\\' => expanded.push('\\'),
                other => expanded.push(other),
            }
            index += 2;
            continue;
        }
        expanded.push(chars[index]);
        index += 1;
    }
    Ok(expanded)
}

fn match_text_from_buffer(
    interp: &Interpreter,
    match_data: &[Option<(usize, usize)>],
    index: usize,
) -> Result<String, LispError> {
    let Some((start, end)) = match_data.get(index).and_then(|entry| *entry) else {
        return Ok(String::new());
    };
    interp
        .buffer
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn match_text_from_string(
    source: &str,
    match_data: &[Option<(usize, usize)>],
    index: usize,
) -> String {
    let Some((start, end)) = match_data.get(index).and_then(|entry| *entry) else {
        return String::new();
    };
    slice_string_chars(source, start, end)
}

fn slice_string_chars(source: &str, start: usize, end: usize) -> String {
    source
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn update_match_data_after_replace(
    match_data: &[Option<(usize, usize)>],
    replace_index: usize,
    start: usize,
    end: usize,
    replacement_len: usize,
) -> Vec<Option<(usize, usize)>> {
    let new_end = start + replacement_len;
    let delta = replacement_len as isize - end.saturating_sub(start) as isize;
    match_data
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let Some((group_start, group_end)) = entry else {
                return None;
            };
            if index == replace_index {
                return Some((start, new_end));
            }
            if *group_start == *group_end && *group_start == start && start == end {
                return Some((start, new_end));
            }
            if start == end && *group_start == start && *group_end > end {
                return Some((start, group_end.saturating_add_signed(delta)));
            }
            if start == end && *group_end == start && *group_start < start {
                return Some((*group_start, group_end.saturating_add_signed(delta)));
            }
            if *group_end <= start {
                return Some((*group_start, *group_end));
            }
            if *group_start >= end {
                return Some((
                    group_start.saturating_add_signed(delta),
                    group_end.saturating_add_signed(delta),
                ));
            }
            if *group_start >= start && *group_end <= end {
                return Some((start, new_end));
            }
            let updated_start = if *group_start > start {
                start
            } else {
                *group_start
            };
            let updated_end = if *group_end < end {
                new_end
            } else {
                group_end.saturating_add_signed(delta)
            };
            Some((updated_start, updated_end))
        })
        .collect()
}

fn expand_symbol_at(haystack: &str, found: usize, prefix: &str) -> Option<String> {
    let tail = &haystack[found..];
    let end = tail
        .char_indices()
        .take_while(|(_, ch)| ch.is_alphanumeric() || *ch == '-' || *ch == '_')
        .last()
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(prefix.len());
    let expansion = &tail[..end];
    if expansion.starts_with(prefix) {
        Some(expansion.to_string())
    } else {
        None
    }
}

fn coding_system_error(name: impl Into<String>) -> LispError {
    let name = name.into();
    LispError::SignalValue(Value::list([
        Value::Symbol("coding-system-error".into()),
        Value::String(format!("Invalid coding system: {name}")),
    ]))
}

fn checked_coding_name(interp: &Interpreter, value: &Value) -> Result<Option<String>, LispError> {
    if value.is_nil() {
        return Ok(None);
    }
    let symbol = value.as_symbol()?.to_string();
    interp
        .coding_system_canonical_name(&symbol)
        .ok_or_else(|| coding_system_error(symbol.clone()))
        .map(Some)
}

fn checked_coding_symbol(interp: &Interpreter, value: &Value) -> Result<String, LispError> {
    checked_coding_name(interp, value)?.ok_or_else(|| coding_system_error("nil"))
}

fn coding_variant_name(interp: &Interpreter, base: &str, eol_type: Option<i64>) -> String {
    if let Some(eol_type) = eol_type {
        let suffix = match eol_type {
            0 => Some("unix"),
            1 => Some("dos"),
            2 => Some("mac"),
            _ => None,
        };
        if let Some(suffix) = suffix {
            let candidate = format!("{base}-{suffix}");
            if let Some(canonical) = interp.coding_system_canonical_name(&candidate) {
                return canonical;
            }
        }
    }
    interp
        .coding_system_canonical_name(base)
        .unwrap_or_else(|| base.to_string())
}

fn set_last_coding_system_used(interp: &mut Interpreter, coding: &str, env: &mut Env) {
    interp.set_variable(
        "last-coding-system-used",
        Value::Symbol(coding.to_string()),
        env,
    );
}

fn shared_string_copy(value: &Value) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    Ok(make_shared_string_value_with_multibyte(
        string.text,
        string.props,
        string.multibyte,
    ))
}

fn bytes_to_unibyte_value(bytes: &[u8]) -> Value {
    let mut text = String::new();
    let mut has_raw_bytes = false;
    for &byte in bytes {
        if byte <= 0x7F {
            text.push(byte as char);
        } else {
            has_raw_bytes = true;
            text.push(raw_byte_regex_char(byte));
        }
    }
    if has_raw_bytes {
        make_shared_string_value_with_multibyte(text, Vec::new(), false)
    } else {
        Value::String(text)
    }
}

fn ascii_only_text(text: &str) -> bool {
    text.chars()
        .all(|ch| raw_byte_from_regex_char(ch).unwrap_or_default() <= 0x7F && (ch as u32) <= 0x7F)
}

fn strip_utf8_bom(bytes: &[u8]) -> (bool, &[u8]) {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        (true, &bytes[3..])
    } else {
        (false, bytes)
    }
}

fn detect_eol_type(bytes: &[u8]) -> i64 {
    if bytes.windows(2).any(|window| window == b"\r\n") {
        1
    } else if bytes.contains(&b'\r') {
        2
    } else {
        0
    }
}

fn decode_bytes_with_explicit_eol(bytes: &[u8], eol_type: i64) -> Vec<u8> {
    match eol_type {
        1 => {
            let mut decoded = Vec::with_capacity(bytes.len());
            let mut index = 0usize;
            while index < bytes.len() {
                if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
                    decoded.push(b'\n');
                    index += 2;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            decoded
        }
        2 => bytes
            .iter()
            .map(|byte| if *byte == b'\r' { b'\n' } else { *byte })
            .collect(),
        _ => bytes.to_vec(),
    }
}

fn encode_text_with_eol(text: &str, eol_type: Option<i64>) -> String {
    match eol_type {
        Some(1) => text.replace('\n', "\r\n"),
        Some(2) => text.replace('\n', "\r"),
        _ => text.to_string(),
    }
}

fn encode_raw_text_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::new();
    for ch in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            bytes.push(byte);
        } else if (ch as u32) <= 0xFF {
            bytes.push(ch as u8);
        } else if ch == json::INVALID_UNICODE_SENTINEL {
            return Err(LispError::TypeError("character".into(), "string".into()));
        } else {
            return Err(LispError::Signal("Character cannot be encoded".into()));
        }
    }
    Ok(bytes)
}

fn encode_iso_latin_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    encode_raw_text_bytes(text)
}

fn encode_ascii_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::new();
    for ch in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            if byte > 0x7F {
                return Err(LispError::Signal("Character cannot be encoded".into()));
            }
            bytes.push(byte);
        } else if (ch as u32) <= 0x7F {
            bytes.push(ch as u8);
        } else {
            return Err(LispError::Signal("Character cannot be encoded".into()));
        }
    }
    Ok(bytes)
}

fn encode_utf8_bytes(text: &str, with_bom: bool) -> Result<Vec<u8>, LispError> {
    let mut bytes = if with_bom {
        vec![0xEF, 0xBB, 0xBF]
    } else {
        Vec::new()
    };
    for ch in text.chars() {
        if ch == json::INVALID_UNICODE_SENTINEL {
            return Err(LispError::TypeError("character".into(), "string".into()));
        }
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            bytes.push(byte);
        } else {
            bytes.extend(ch.to_string().into_bytes());
        }
    }
    Ok(bytes)
}

fn encode_euc_jp_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::new();
    for ch in text.chars() {
        match ch {
            'あ' => bytes.extend([0xA4, 0xA2]),
            _ if (ch as u32) <= 0x7F => bytes.push(ch as u8),
            _ => return Err(LispError::Signal("Character cannot be encoded".into())),
        }
    }
    Ok(bytes)
}

fn decode_raw_text_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| {
            if *byte <= 0x7F {
                char::from(*byte)
            } else {
                raw_byte_regex_char(*byte)
            }
        })
        .collect()
}

fn decode_latin_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| char::from(*byte)).collect()
}

fn decode_utf8_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn encode_text_bytes(interp: &Interpreter, text: &str, coding: &str) -> Result<Vec<u8>, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_else(|| canonical.clone());
    let eol_type = interp.coding_system_eol_type_value(&canonical);
    let text = encode_text_with_eol(text, eol_type);
    match kind.as_str() {
        "utf-8" | "prefer-utf-8" | "utf-8-auto" => encode_utf8_bytes(&text, false),
        "utf-8-with-signature" => encode_utf8_bytes(&text, true),
        "iso-latin-1" => encode_iso_latin_bytes(&text),
        "us-ascii" => encode_ascii_bytes(&text),
        "raw-text" | "no-conversion" => encode_raw_text_bytes(&text),
        "euc-jp" => encode_euc_jp_bytes(&text),
        _ => encode_raw_text_bytes(&text),
    }
}

fn decode_text_bytes(
    interp: &Interpreter,
    bytes: &[u8],
    coding: &str,
) -> Result<String, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_else(|| canonical.clone());
    match kind.as_str() {
        "utf-8" | "prefer-utf-8" | "utf-8-auto" | "utf-8-with-signature" => {
            Ok(decode_utf8_bytes(bytes))
        }
        "iso-latin-1" => Ok(decode_latin_bytes(bytes)),
        "us-ascii" => Ok(bytes.iter().map(|byte| char::from(*byte)).collect()),
        "raw-text" | "no-conversion" | "euc-jp" => Ok(decode_raw_text_bytes(bytes)),
        _ => Ok(decode_raw_text_bytes(bytes)),
    }
}

fn string_unencodable_positions(
    text: &str,
    coding: &str,
    interp: &Interpreter,
) -> Result<Vec<i64>, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_else(|| canonical.clone());
    let mut failures = Vec::new();
    for (index, ch) in text.chars().enumerate() {
        let raw_byte = raw_byte_from_regex_char(ch);
        let code = ch as u32;
        let representable = match kind.as_str() {
            "utf-8" | "utf-8-with-signature" | "utf-8-auto" | "prefer-utf-8" | "undecided" => {
                ch != json::INVALID_UNICODE_SENTINEL
            }
            "iso-latin-1" | "raw-text" | "no-conversion" => raw_byte.is_some() || code <= 0xFF,
            "us-ascii" => raw_byte.is_some_and(|byte| byte <= 0x7F) || code <= 0x7F,
            "sjis" => raw_byte.is_some_and(|byte| byte <= 0x7F) || code <= 0x7F || ch == 'あ',
            "big5" | "iso-2022-7bit" => raw_byte.is_some_and(|byte| byte <= 0x7F) || code <= 0x7F,
            "euc-jp" => raw_byte.is_some_and(|byte| byte <= 0x7F) || code <= 0x7F || ch == 'あ',
            _ => true,
        };
        if !representable {
            failures.push(index as i64);
        }
    }
    Ok(failures)
}

fn string_identity_for_coding(
    text: &str,
    coding: &str,
    interp: &Interpreter,
    encode: bool,
) -> bool {
    let eol_type = interp.coding_system_eol_type_value(coding);
    let kind = interp
        .coding_system_kind_name(coding)
        .unwrap_or_else(|| coding.to_string());
    if encode {
        if matches!(eol_type, Some(1) | Some(2)) && text.contains('\n') {
            return false;
        }
        if kind == "utf-8-with-signature" {
            return false;
        }
    } else if matches!(eol_type, Some(1) | Some(2)) && text.contains('\r') {
        return false;
    }
    true
}

fn preferred_ascii_detection_base(interp: &Interpreter) -> String {
    let priorities = interp.coding_system_priority_list();
    if priorities
        .first()
        .is_some_and(|coding| coding == "utf-8-auto")
    {
        "__eol__".into()
    } else if priorities
        .iter()
        .any(|coding| interp.coding_system_base_name(coding).as_deref() == Some("prefer-utf-8"))
    {
        "prefer-utf-8".into()
    } else {
        "__eol__".into()
    }
}

fn auto_detect_coding(interp: &Interpreter, bytes: &[u8]) -> (String, Vec<u8>) {
    let actual_eol = detect_eol_type(bytes);
    let normalized = decode_bytes_with_explicit_eol(bytes, actual_eol);
    let (has_bom, bomless) = strip_utf8_bom(&normalized);
    if has_bom {
        return (
            coding_variant_name(interp, "utf-8-with-signature", Some(actual_eol)),
            bomless.to_vec(),
        );
    }
    if normalized.contains(&0) {
        return (
            coding_variant_name(interp, "no-conversion", Some(actual_eol)),
            normalized,
        );
    }
    if bomless
        .windows(4)
        .any(|window| window == [0x1B, b'$', b'B', b'A'])
        || bomless
            .windows(4)
            .any(|window| window == [0x1B, b'(', b'B', 0x1B])
        || bomless
            .windows(3)
            .any(|window| window == [0x1B, b'$', b'B'])
    {
        return ("iso-2022-7bit".into(), normalized);
    }
    if std::str::from_utf8(bomless).is_ok() {
        let text = decode_utf8_bytes(bomless);
        if ascii_only_text(&text) {
            let base = preferred_ascii_detection_base(interp);
            if base == "__eol__" {
                let base = match actual_eol {
                    1 => "dos",
                    2 => "mac",
                    _ => "unix",
                };
                return (base.into(), normalized);
            }
            return (
                coding_variant_name(interp, &base, Some(actual_eol)),
                normalized,
            );
        }
        return (
            coding_variant_name(interp, "utf-8", Some(actual_eol)),
            normalized,
        );
    }
    (
        coding_variant_name(interp, "raw-text", Some(actual_eol)),
        normalized,
    )
}

fn text_from_region_or_string(
    interp: &Interpreter,
    start_or_string: &Value,
    end: Option<&Value>,
) -> Result<String, LispError> {
    if let Some(string) = string_like(start_or_string) {
        return Ok(string.text);
    }
    let start = position_from_value(interp, start_or_string)?;
    let end = end
        .map(|value| position_from_value(interp, value))
        .transpose()?
        .unwrap_or(start);
    interp
        .buffer
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn detect_coding_names_for_text(interp: &Interpreter, text: &str, env: &Env) -> Vec<String> {
    let inhibit_null = interp
        .lookup_var("inhibit-null-byte-detection", env)
        .is_some_and(|value| value.is_truthy());
    if !inhibit_null && text.chars().any(|ch| ch == '\0') {
        return vec!["no-conversion".into()];
    }
    let inhibit_iso = interp
        .lookup_var("inhibit-iso-escape-detection", env)
        .is_some_and(|value| value.is_truthy());
    if !inhibit_iso && text.contains("\u{1b}$B") && text.contains("\u{1b}(B") {
        return vec!["iso-2022-7bit".into()];
    }
    if ascii_only_text(text) {
        return vec!["undecided".into()];
    }
    if string_unencodable_positions(text, "utf-8", interp)
        .map(|positions| positions.is_empty())
        .unwrap_or(false)
    {
        vec!["utf-8".into()]
    } else {
        vec!["raw-text".into()]
    }
}

fn detect_coding_string_value(
    interp: &Interpreter,
    value: &Value,
    highest: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let text = string_text(value)?;
    let codings = detect_coding_names_for_text(interp, &text, env);
    if highest.is_some_and(Value::is_truthy) {
        Ok(codings
            .first()
            .cloned()
            .map(Value::Symbol)
            .unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            codings.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
        ))
    }
}

fn detect_coding_region_value(
    interp: &Interpreter,
    start: &Value,
    end: &Value,
    highest: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let text = text_from_region_or_string(interp, start, Some(end))?;
    let codings = detect_coding_names_for_text(interp, &text, env);
    if highest.is_some_and(Value::is_truthy) {
        Ok(codings
            .first()
            .cloned()
            .map(Value::Symbol)
            .unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            codings.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
        ))
    }
}

fn find_coding_systems_region_internal_value(
    interp: &Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    let text = string_text(value)?;
    if ascii_only_text(&text) {
        return Ok(Value::T);
    }
    let mut codings = Vec::new();
    for coding in interp.coding_system_priority_list() {
        let Some(base) = interp.coding_system_base_name(&coding) else {
            continue;
        };
        if matches!(base.as_str(), "undecided" | "utf-8-auto" | "no-conversion") {
            continue;
        }
        if codings.iter().any(|existing: &String| existing == &base) {
            continue;
        }
        if string_unencodable_positions(&text, &base, interp)?.is_empty() {
            codings.push(base);
        }
    }
    Ok(Value::list(
        codings.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
    ))
}

fn check_coding_systems_region_value(
    interp: &Interpreter,
    start_or_string: &Value,
    end: Option<&Value>,
    coding_list: &Value,
) -> Result<Value, LispError> {
    let text = text_from_region_or_string(interp, start_or_string, end)?;
    let mut failures = Vec::new();
    for coding in coding_list.to_vec()? {
        let symbol = coding.as_symbol()?.to_string();
        let canonical = interp
            .coding_system_canonical_name(&symbol)
            .ok_or_else(|| coding_system_error(symbol.clone()))?;
        let positions = string_unencodable_positions(&text, &canonical, interp)?;
        if !positions.is_empty() {
            let mut items = vec![Value::Symbol(canonical)];
            items.extend(positions.into_iter().map(Value::Integer));
            failures.push(Value::list(items));
        }
    }
    Ok(if failures.is_empty() {
        Value::Nil
    } else {
        Value::list(failures)
    })
}

fn find_operation_coding_system_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs(
            "find-operation-coding-system".into(),
            0,
        ));
    }
    let _operation = args[0].as_symbol()?;
    let Some(file) = args.get(1) else {
        return Ok(Value::Nil);
    };
    let file = string_text(file)?;
    let Some(alist) = interp.lookup_var("file-coding-system-alist", env) else {
        return Ok(Value::Nil);
    };
    for entry in alist.to_vec()? {
        let Value::Cons(pattern, target) = entry else {
            continue;
        };
        let pattern = string_text(&pattern)?;
        let Ok(regex) = Regex::new(&translate_elisp_regex(&pattern)) else {
            continue;
        };
        if !regex.is_match(&file) {
            continue;
        }
        let target = match *target {
            Value::Cons(value, tail) if matches!(*tail, Value::Nil) => *value,
            other => other,
        };
        let coding = match target {
            Value::Symbol(symbol) if interp.has_coding_system(&symbol) => interp
                .coding_system_canonical_name(&symbol)
                .unwrap_or(symbol),
            Value::Symbol(symbol) => {
                let result =
                    call_named_function(interp, &symbol, &[Value::list(args[1..].to_vec())], env)?;
                checked_coding_symbol(interp, &result)?
            }
            other => {
                let result =
                    call_function_value(interp, &other, &[Value::list(args[1..].to_vec())], env)?;
                checked_coding_symbol(interp, &result)?
            }
        };
        return Ok(Value::cons(
            Value::Symbol(coding.clone()),
            Value::Symbol(coding),
        ));
    }
    Ok(Value::Nil)
}

fn encode_coding_value(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    let Some(coding) = coding else {
        return if nocopy {
            Ok(value.clone())
        } else {
            shared_string_copy(value)
        };
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    let failures = string_unencodable_positions(&string.text, &canonical, interp)?;
    if !failures.is_empty() {
        return Err(LispError::Signal("Character cannot be encoded".into()));
    }
    set_last_coding_system_used(interp, &canonical, env);
    if nocopy && string_identity_for_coding(&string.text, &canonical, interp, true) {
        Ok(value.clone())
    } else {
        shared_string_copy(value)
    }
}

fn decode_coding_text(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    let Some(coding) = coding else {
        return if nocopy {
            Ok(value.clone())
        } else {
            shared_string_copy(value)
        };
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    set_last_coding_system_used(interp, &canonical, env);
    if nocopy && string_identity_for_coding(&string.text, &canonical, interp, false) {
        Ok(value.clone())
    } else {
        shared_string_copy(value)
    }
}

fn json_parse_options(args: &[Value]) -> Result<JsonParseOptions, LispError> {
    let mut options = JsonParseOptions {
        object_type: JsonObjectType::HashTable,
        array_type: JsonArrayType::Vector,
        null_object: Value::Symbol(":null".into()),
        false_object: Value::Symbol(":false".into()),
    };
    let mut index = 0usize;
    while index + 1 < args.len() {
        let key = args[index].as_symbol()?;
        let value = args[index + 1].clone();
        match key {
            ":object-type" => {
                options.object_type = match &value {
                    Value::Symbol(symbol) if symbol == "hash-table" => JsonObjectType::HashTable,
                    Value::Symbol(symbol) if symbol == "alist" => JsonObjectType::Alist,
                    Value::Symbol(symbol) if symbol == "plist" => JsonObjectType::Plist,
                    other => {
                        return Err(LispError::TypeError("symbol".into(), other.type_name()));
                    }
                };
            }
            ":array-type" => {
                options.array_type = match &value {
                    Value::Symbol(symbol) if symbol == "vector" => JsonArrayType::Vector,
                    Value::Symbol(symbol) if symbol == "list" => JsonArrayType::List,
                    other => {
                        return Err(LispError::TypeError("symbol".into(), other.type_name()));
                    }
                };
            }
            ":null-object" => options.null_object = value,
            ":false-object" => options.false_object = value,
            _ => {
                return Err(LispError::TypeError("json-option".into(), key.into()));
            }
        }
        index += 2;
    }
    Ok(options)
}

fn json_serialize_options(args: &[Value]) -> Result<(Value, Value), LispError> {
    let mut null_object = Value::Symbol(":null".into());
    let mut false_object = Value::Symbol(":false".into());
    let mut index = 0usize;
    while index + 1 < args.len() {
        let key = args[index].as_symbol()?;
        let value = args[index + 1].clone();
        match key {
            ":null-object" => null_object = value,
            ":false-object" => false_object = value,
            _ => {
                return Err(LispError::TypeError("json-option".into(), key.into()));
            }
        }
        index += 2;
    }
    Ok((null_object, false_object))
}

fn current_group_id() -> Result<u32, LispError> {
    let output = Command::new("id")
        .arg("-g")
        .output()
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if !output.status.success() {
        return Err(LispError::Signal("Failed to determine current gid".into()));
    }
    let value = String::from_utf8_lossy(&output.stdout);
    value
        .trim()
        .parse::<u32>()
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn group_name_from_gid(gid: i64) -> Result<Option<String>, LispError> {
    if cfg!(target_os = "macos") {
        let output = Command::new("dscacheutil")
            .args(["-q", "group", "-a", "gid", &gid.to_string()])
            .output();
        if let Ok(output) = output
            && output.status.success()
        {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                if let Some(name) = line.strip_prefix("name:") {
                    return Ok(Some(name.trim().to_string()));
                }
            }
        }
    }

    let output = Command::new("getent")
        .args(["group", &gid.to_string()])
        .output();
    if let Ok(output) = output
        && output.status.success()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        if let Some(name) = text.split(':').next()
            && !name.is_empty()
        {
            return Ok(Some(name.to_string()));
        }
    }

    if let Ok(groups) = std::fs::read_to_string("/etc/group") {
        for line in groups.lines() {
            let mut parts = line.split(':');
            let Some(name) = parts.next() else { continue };
            let _ = parts.next();
            let Some(entry_gid) = parts.next() else {
                continue;
            };
            if entry_gid.parse::<i64>().ok() == Some(gid) {
                return Ok(Some(name.to_string()));
            }
        }
    }

    Ok(None)
}

pub(crate) fn find_executable(name: &str) -> Option<String> {
    if name.contains(std::path::MAIN_SEPARATOR) && std::path::Path::new(name).exists() {
        return Some(name.to_string());
    }
    let path = std::env::var_os("PATH")?;
    for entry in std::env::split_paths(&path) {
        let candidate = entry.join(name);
        if candidate.exists() {
            return Some(candidate.display().to_string());
        }
    }
    None
}

pub(crate) fn default_directory() -> String {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    path_to_directory_string(&cwd)
}

fn default_system_configuration() -> String {
    let machine = uname_value("-m").unwrap_or_else(|| std::env::consts::ARCH.to_string());
    match std::env::consts::OS {
        "macos" => {
            let release = uname_value("-r").unwrap_or_else(|| "0".into());
            format!("{machine}-apple-darwin{release}")
        }
        "linux" => format!("{machine}-unknown-linux-gnu"),
        "freebsd" => format!("{machine}-unknown-freebsd"),
        "windows" => format!("{machine}-pc-windows-msvc"),
        os => format!("{machine}-{os}"),
    }
}

fn uname_value(flag: &str) -> Option<String> {
    let output = Command::new("uname").arg(flag).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn compat_repo_root_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    PathBuf::from(test_directory)
        .parent()
        .map(Path::to_path_buf)
}

pub(crate) fn compat_data_directory() -> Option<String> {
    std::env::var("EMACS_TEST_DIRECTORY")
        .ok()
        .and_then(|test_directory| compat_repo_root_from_test_directory(&test_directory))
        .map(|repo_root| path_to_directory_string(&repo_root.join("etc")))
}

fn compat_invocation_path_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    let repo_root = compat_repo_root_from_test_directory(test_directory)?;
    let candidate = repo_root.join("src").join("emacs");
    candidate.exists().then_some(candidate)
}

fn compat_emacsclient_path_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    let repo_root = compat_repo_root_from_test_directory(test_directory)?;
    let candidate = repo_root.join("lib-src").join("emacsclient");
    candidate.exists().then_some(candidate)
}

pub(crate) fn compat_emacsclient_program_name() -> Option<String> {
    std::env::var("EMACS_TEST_DIRECTORY")
        .ok()
        .and_then(|test_directory| compat_emacsclient_path_from_test_directory(&test_directory))
        .map(|path| path.display().to_string())
}

pub(crate) fn current_invocation_name() -> Option<String> {
    current_invocation_path()
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

pub(crate) fn current_invocation_directory() -> Option<String> {
    current_invocation_path()
        .parent()
        .map(path_to_directory_string)
}

fn current_invocation_path() -> PathBuf {
    if let Ok(test_directory) = std::env::var("EMACS_TEST_DIRECTORY")
        && let Some(path) = compat_invocation_path_from_test_directory(&test_directory)
    {
        return path;
    }
    std::env::current_exe().unwrap_or_else(|_| PathBuf::from("emaxx"))
}

fn expand_file_name(path: &str, base: Option<&str>) -> String {
    let preserve_directory_syntax = path.ends_with(std::path::MAIN_SEPARATOR);
    let expanded = expand_home_prefix(path);
    let candidate = PathBuf::from(expanded);
    let absolute = if candidate.is_absolute() {
        candidate
    } else {
        let base_dir = base
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| PathBuf::from(default_directory()));
        if base_dir.is_absolute() {
            base_dir.join(candidate)
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(base_dir)
                .join(candidate)
        }
    };
    if preserve_directory_syntax {
        path_to_directory_string(&absolute)
    } else {
        normalize_path(&absolute).display().to_string()
    }
}

fn expand_file_name_runtime(
    interp: &mut Interpreter,
    env: &mut Env,
    path: &str,
    base: Option<&str>,
) -> Result<String, LispError> {
    validate_file_name(path)?;
    if let Some(base) = base {
        validate_file_name(base)?;
    }
    if let Some(handler) = find_file_name_handler(interp, env, path) {
        let function = match handler {
            Value::Symbol(symbol) => interp.lookup_function(&symbol, env)?,
            other => other,
        };
        let handled = call_function_value(
            interp,
            &function,
            &[
                Value::Symbol("expand-file-name".into()),
                Value::String(path.to_string()),
                base.map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Nil),
            ],
            env,
        )?;
        return string_text(&handled);
    }
    Ok(expand_file_name(path, base))
}

fn substitute_in_file_name(path: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = path.chars().collect();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] != '$' {
            result.push(chars[index]);
            index += 1;
            continue;
        }
        if index + 1 < chars.len() && chars[index + 1] == '$' {
            result.push('$');
            index += 2;
            continue;
        }
        if index + 1 < chars.len() && chars[index + 1] == '{' {
            let mut end = index + 2;
            while end < chars.len() && chars[end] != '}' {
                end += 1;
            }
            if end < chars.len() && chars[end] == '}' {
                let name: String = chars[index + 2..end].iter().collect();
                result.push_str(&std::env::var(&name).unwrap_or_default());
                index = end + 1;
                continue;
            }
        }
        let mut end = index + 1;
        while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        if end == index + 1 {
            result.push('$');
            index += 1;
            continue;
        }
        let name: String = chars[index + 1..end].iter().collect();
        result.push_str(&std::env::var(&name).unwrap_or_default());
        index = end;
    }
    result
}

fn expand_home_prefix(path: &str) -> String {
    if path == "~" {
        return std::env::var("HOME").unwrap_or_else(|_| path.to_string());
    }
    if let Some(suffix) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return PathBuf::from(home).join(suffix).display().to_string();
    }
    if let Some(rest) = path.strip_prefix('~') {
        let (user, suffix) = rest
            .split_once('/')
            .map(|(user, suffix)| (user, Some(suffix)))
            .unwrap_or((rest, None));
        if user_exists(user)
            && let Ok(home) = std::env::var("HOME")
        {
            return suffix.map_or(home.clone(), |suffix| {
                PathBuf::from(home).join(suffix).display().to_string()
            });
        }
    }
    path.to_string()
}

pub(crate) fn current_user_login_name() -> Option<String> {
    std::env::var("LOGNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var("USER").ok().filter(|value| !value.is_empty()))
}

pub(crate) fn current_user_full_name() -> Option<String> {
    std::env::var("EMAXX_USER_FULL_NAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(current_user_login_name)
}

pub(crate) fn emacs_version_value() -> String {
    std::env::var("EMAXX_EMACS_VERSION")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

pub(crate) fn system_configuration() -> String {
    if let Ok(value) = std::env::var("EMAXX_SYSTEM_CONFIGURATION")
        && !value.is_empty()
    {
        return value;
    }
    SYSTEM_CONFIGURATION
        .get_or_init(default_system_configuration)
        .clone()
}

pub(crate) fn emacs_version_description() -> String {
    format!(
        "GNU Emacs {} ({})",
        emacs_version_value(),
        system_configuration()
    )
}

fn user_exists(name: &str) -> bool {
    current_user_login_name().is_some_and(|login| login == name)
}

fn user_full_name(name: Option<&str>) -> Option<String> {
    match name {
        None | Some("") => current_user_full_name(),
        Some(name) if user_exists(name) => current_user_full_name(),
        _ => None,
    }
}

fn file_name_directory(path: &str) -> Option<String> {
    path.rfind('/').map(|index| path[..=index].to_string())
}

fn file_name_nondirectory(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn file_name_as_directory(path: &str) -> String {
    if path.is_empty() {
        "./".into()
    } else if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

fn directory_file_name(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    if path.chars().all(|ch| ch == '/') {
        return if path.len() == 2 {
            "//".into()
        } else {
            "/".into()
        };
    }
    path.trim_end_matches('/').to_string()
}

fn directory_name_p(path: &str) -> bool {
    path.ends_with('/')
}

fn file_name_absolute_p(path: &str) -> bool {
    if path.starts_with('/') {
        return true;
    }
    if path == "~" || path.starts_with("~/") {
        return true;
    }
    if let Some(rest) = path.strip_prefix('~') {
        let user = rest.split('/').next().unwrap_or_default();
        return user_exists(user);
    }
    false
}

fn file_name_concat(parts: &[String]) -> String {
    let mut iter = parts.iter().filter(|part| !part.is_empty());
    let Some(first) = iter.next() else {
        return String::new();
    };
    let mut result = first.clone();
    for part in iter {
        if result.is_empty() {
            result = part.clone();
        } else if result.ends_with('/') {
            result.push_str(part.trim_start_matches('/'));
        } else {
            result.push('/');
            result.push_str(part.trim_start_matches('/'));
        }
    }
    result
}

fn validate_file_name(path: &str) -> Result<(), LispError> {
    if path.contains('\0') {
        Err(LispError::TypeError("string".into(), path.to_string()))
    } else {
        Ok(())
    }
}

fn find_file_name_handler(interp: &Interpreter, env: &Env, file: &str) -> Option<Value> {
    let handlers = interp.lookup_var("file-name-handler-alist", env)?;
    let entries = handlers.to_vec().ok()?;
    for entry in entries {
        let Value::Cons(pattern, handler) = entry else {
            continue;
        };
        let Ok(pattern) = string_text(&pattern) else {
            continue;
        };
        let Ok(regex) = Regex::new(&pattern) else {
            continue;
        };
        if regex.is_match(file) {
            return Some((*handler).clone());
        }
    }
    None
}

fn ert_resource_directory(interp: &Interpreter) -> Option<String> {
    let testfile = interp
        .current_load_file()
        .or(interp.buffer.file.as_deref())?;
    Some(ert_resource_directory_for(testfile))
}

fn ert_resource_directory_for(testfile: &str) -> String {
    let expanded = PathBuf::from(expand_file_name(testfile, None));
    let sibling_resources = expanded
        .parent()
        .map(|parent| parent.join("resources"))
        .filter(|path| path.is_dir());
    let resource_dir = sibling_resources.unwrap_or_else(|| {
        let rendered = expanded.display().to_string();
        let trimmed = rendered
            .strip_suffix(".el")
            .map(|path| {
                path.strip_suffix("-tests")
                    .or_else(|| path.strip_suffix("-test"))
                    .unwrap_or(path)
            })
            .unwrap_or(rendered.as_str());
        PathBuf::from(format!("{trimmed}-resources"))
    });
    path_to_directory_string(&resource_dir)
}

fn charset_for_char(code: u32) -> &'static str {
    if code <= 0x7f { "ascii" } else { "unicode" }
}

fn default_charset_plist(name: &str, interp: &Interpreter) -> Option<Value> {
    match interp.charset_canonical_name(name)?.as_str() {
        "ascii" => Some(Value::list([
            Value::Symbol(":short-name".into()),
            Value::String("ASCII".into()),
        ])),
        "unicode" => Some(Value::list([
            Value::Symbol(":short-name".into()),
            Value::String("Unicode".into()),
        ])),
        _ => None,
    }
}

fn charsets_for_text(text: &str, interp: &Interpreter) -> Vec<Value> {
    let mut names = Vec::new();
    if text.chars().any(|ch| (ch as u32) <= 0x7f) {
        names.push("ascii".to_string());
    }
    if text.chars().any(|ch| (ch as u32) > 0x7f) {
        names.push("unicode".to_string());
    }
    if names.is_empty() {
        names.push("ascii".to_string());
    }
    names.sort_by_key(|name| interp.charset_priority_rank(name));
    names.dedup();
    names.into_iter().map(Value::Symbol).collect()
}

fn charset_max_codepoint(name: &str) -> i64 {
    match name {
        "ascii" => 0x7f,
        _ => 0x10ffff,
    }
}

fn charset_ranges_for(
    charset: &str,
    from: i64,
    to: i64,
    interp: &Interpreter,
) -> Result<Vec<(i64, i64)>, LispError> {
    let canonical = interp
        .charset_canonical_name(charset)
        .ok_or_else(|| LispError::Void(charset.to_string()))?;
    let (lower, upper) = if from <= to { (from, to) } else { (to, from) };
    let range = match canonical.as_str() {
        "ascii" => {
            let start = lower.max(0);
            let end = upper.min(0x7f);
            if start <= end {
                Some((start, end))
            } else {
                None
            }
        }
        "unicode" => {
            let start = lower.max(0);
            let end = upper.min(0x10ffff);
            if start <= end {
                Some((start, end))
            } else {
                None
            }
        }
        _ => None,
    };
    Ok(range.into_iter().collect())
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

pub(crate) fn path_to_directory_string(path: &Path) -> String {
    let mut rendered = normalize_path(path).display().to_string();
    if !rendered.ends_with(std::path::MAIN_SEPARATOR) {
        rendered.push(std::path::MAIN_SEPARATOR);
    }
    rendered
}

fn file_readable_p(path: &str) -> bool {
    fs::File::open(path).is_ok()
}

fn file_executable_p(path: &str) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn run_external_process(
    interp: &Interpreter,
    program: &str,
    argv: &[String],
    input: Option<&[u8]>,
    env: &Env,
) -> Result<std::process::Output, LispError> {
    let mut command = Command::new(program);
    command.args(argv);
    if let Some(default_directory) = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .filter(|directory| !directory.is_empty())
    {
        command.current_dir(default_directory);
    }
    command.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    apply_process_environment(interp, env, &mut command);
    let mut child = command
        .spawn()
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if let Some(stdin_data) = input
        && let Some(mut stdin) = child.stdin.take()
    {
        stdin
            .write_all(stdin_data)
            .map_err(|error| LispError::Signal(error.to_string()))?;
    }
    child
        .wait_with_output()
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn apply_process_environment(interp: &Interpreter, env: &Env, command: &mut Command) {
    let Some(process_environment) = interp.lookup_var("process-environment", env) else {
        return;
    };
    let Ok(items) = process_environment.to_vec() else {
        return;
    };
    command.env_clear();
    for item in items {
        let Ok(entry) = string_text(&item) else {
            continue;
        };
        if let Some((name, value)) = entry.split_once('=') {
            command.env(name, value);
        }
    }
}

fn write_process_output(
    interp: &mut Interpreter,
    destination: &Value,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<(), LispError> {
    if destination.is_nil() {
        return Ok(());
    }
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    );
    if output.is_empty() {
        return Ok(());
    }
    let target_id = match destination {
        Value::T => Some(interp.current_buffer_id()),
        Value::Buffer(_, _) => Some(interp.resolve_buffer_id(destination)?),
        Value::String(name) => Some(
            interp
                .find_buffer(name)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(name).0),
        ),
        _ => {
            return Err(LispError::TypeError(
                "buffer-or-name".into(),
                destination.type_name(),
            ));
        }
    };
    let Some(target_id) = target_id else {
        return Ok(());
    };
    let original_id = interp.current_buffer_id();
    if target_id != original_id {
        interp.switch_to_buffer_id(target_id)?;
    }
    interp.insert_current_buffer(&output);
    if target_id != original_id {
        interp.switch_to_buffer_id(original_id)?;
    }
    Ok(())
}

fn exit_status_code(status: &std::process::ExitStatus) -> i64 {
    status
        .code()
        .unwrap_or(if status.success() { 0 } else { 1 }) as i64
}

fn default_sort_lt(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    let left_marker = if let Value::Marker(id) = left {
        interp
            .marker_position(*id)
            .or_else(|| interp.marker_last_position(*id))
    } else {
        None
    };
    let right_marker = if let Value::Marker(id) = right {
        interp
            .marker_position(*id)
            .or_else(|| interp.marker_last_position(*id))
    } else {
        None
    };
    if let (Some(left), Some(right)) = (left_marker, right_marker) {
        return Ok(left < right);
    }
    if let (Ok(left), Ok(right)) = (left.as_integer(), right.as_integer()) {
        return Ok(left < right);
    }
    if let (Some(left), Some(right)) = (string_like(left), string_like(right)) {
        return Ok(left.text < right.text);
    }
    Ok(left.to_string() < right.to_string())
}

fn parse_color_spec(spec: &str) -> Option<[u16; 3]> {
    if let Some(rest) = spec.strip_prefix('#') {
        if rest.is_empty() || rest.len() % 3 != 0 {
            return None;
        }
        let digits = rest.len() / 3;
        if !(1..=4).contains(&digits) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, chunk) in rest.as_bytes().chunks(digits).enumerate() {
            let text = std::str::from_utf8(chunk).ok()?;
            if !text.chars().all(|ch| ch.is_ascii_hexdigit()) {
                return None;
            }
            values[index] = expand_hex_component(text)?;
        }
        return Some(values);
    }

    if let Some(rest) = spec.strip_prefix("rgb:") {
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, part) in parts.iter().enumerate() {
            if !part.chars().all(|ch| ch.is_ascii_hexdigit()) || part.len() > 4 {
                return None;
            }
            values[index] = expand_hex_component(part)?;
        }
        return Some(values);
    }

    if let Some(rest) = spec.strip_prefix("rgbi:") {
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, part) in parts.iter().enumerate() {
            if part.chars().any(char::is_whitespace) {
                return None;
            }
            let value = part.parse::<f64>().ok()?;
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return None;
            }
            values[index] = (value * 65535.0).round() as u16;
        }
        return Some(values);
    }

    None
}

fn expand_hex_component(component: &str) -> Option<u16> {
    if component.is_empty() || component.len() > 4 {
        return None;
    }
    let value = u32::from_str_radix(component, 16).ok()?;
    let bits = 4 * component.len();
    let max_value = (1u32 << bits) - 1;
    Some(((value * 0xFFFF) / max_value) as u16)
}

#[derive(Default)]
struct FontSpecInfo {
    family: Option<String>,
    size: Option<f64>,
    weight: Option<String>,
    slant: Option<String>,
    spacing: Option<i64>,
    foundry: Option<String>,
}

fn font_spec_info(interp: &Interpreter, value: &Value) -> Result<FontSpecInfo, LispError> {
    let Value::Record(id) = value else {
        return Err(LispError::TypeError("font-spec".into(), value.type_name()));
    };
    let record = interp
        .find_record(*id)
        .ok_or_else(|| LispError::TypeError("font-spec".into(), value.type_name()))?;
    if record.type_name != "font-spec" {
        return Err(LispError::TypeError("font-spec".into(), value.type_name()));
    }
    let name = record
        .slots
        .first()
        .map(string_text)
        .transpose()?
        .unwrap_or_default();
    Ok(parse_font_name(&name))
}

fn parse_font_name(name: &str) -> FontSpecInfo {
    if name.starts_with('-') {
        return parse_xlfd_font_name(name);
    }
    if name.chars().next().is_some_and(char::is_whitespace)
        || name.chars().last().is_some_and(char::is_whitespace)
    {
        return FontSpecInfo {
            family: Some(name.to_string()),
            ..FontSpecInfo::default()
        };
    }
    if name.contains(':')
        || name
            .rsplit_once('-')
            .is_some_and(|(family, size)| !family.is_empty() && size.parse::<f64>().is_ok())
    {
        return parse_fontconfig_name(name);
    }
    parse_gtk_font_name(name)
}

fn parse_xlfd_font_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let parts = name.split('-').skip(1).collect::<Vec<_>>();
    if parts.len() < 3 {
        return info;
    }
    info.foundry = parts.first().map(|part| (*part).to_string());
    let weight_index = parts
        .iter()
        .enumerate()
        .skip(2)
        .find(|(index, part)| {
            is_weight_name(part)
                && parts
                    .get(index + 1)
                    .is_some_and(|next| is_slant_name(next) || is_width_name(next))
        })
        .map(|(index, _)| index)
        .unwrap_or(2);
    if weight_index > 1 {
        info.family = Some(parts[1..weight_index].join("-"));
    }
    info.weight = parts
        .get(weight_index)
        .and_then(|part| normalize_weight(part));
    info
}

fn parse_fontconfig_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let mut sections = name.split(':');
    let base = sections.next().unwrap_or_default();
    parse_family_and_size_segment(base, &mut info);
    for section in sections {
        apply_font_attr(section, &mut info);
    }
    info
}

fn parse_gtk_font_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let mut tokens = name
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        if !name.is_empty() {
            info.family = Some(name.to_string());
        }
        return info;
    }
    if tokens
        .last()
        .is_some_and(|token| token.parse::<f64>().is_ok())
    {
        info.size = tokens.pop().and_then(|token| token.parse::<f64>().ok());
    }
    loop {
        let Some(token) = tokens.last().cloned() else {
            break;
        };
        if let Some(weight) = normalize_weight(&token) {
            if info.weight.is_none() {
                info.weight = Some(weight);
            }
            tokens.pop();
            continue;
        }
        if let Some(slant) = normalize_slant(&token) {
            if info.slant.is_none() {
                info.slant = Some(slant);
            }
            tokens.pop();
            continue;
        }
        if let Some(spacing) = normalize_spacing(&token) {
            if info.spacing.is_none() {
                info.spacing = Some(spacing);
            }
            tokens.pop();
            continue;
        }
        if is_width_name(&token) {
            tokens.pop();
            continue;
        }
        break;
    }
    if !tokens.is_empty() {
        info.family = Some(tokens.join(" "));
    }
    info
}

fn parse_family_and_size_segment(base: &str, info: &mut FontSpecInfo) {
    if base.parse::<f64>().is_ok() {
        info.size = base.parse::<f64>().ok();
        return;
    }
    if let Some((family, size)) = base.rsplit_once('-')
        && !family.is_empty()
        && size.parse::<f64>().is_ok()
    {
        info.family = Some(family.to_string());
        info.size = size.parse::<f64>().ok();
        return;
    }
    if !base.is_empty() {
        info.family = Some(base.to_string());
    }
}

fn apply_font_attr(section: &str, info: &mut FontSpecInfo) {
    if let Some((key, value)) = section.split_once('=') {
        match key {
            "weight" => info.weight = normalize_weight(value),
            "slant" => info.slant = normalize_slant(value),
            _ => {}
        }
        return;
    }
    if let Some(weight) = normalize_weight(section) {
        info.weight = Some(weight);
    } else if let Some(slant) = normalize_slant(section) {
        info.slant = Some(slant);
    } else if let Some(spacing) = normalize_spacing(section) {
        info.spacing = Some(spacing);
    }
}

fn normalize_weight(token: &str) -> Option<String> {
    match token.to_ascii_lowercase().as_str() {
        "ultra-light" => Some("ultra-light".into()),
        "light" => Some("light".into()),
        "book" => Some("book".into()),
        "medium" => Some("medium".into()),
        "demibold" => Some("demibold".into()),
        "semi-bold" | "semibold" => Some("semi-bold".into()),
        "bold" => Some("bold".into()),
        "black" => Some("black".into()),
        "normal" => Some("normal".into()),
        _ => None,
    }
}

fn normalize_slant(token: &str) -> Option<String> {
    match token.to_ascii_lowercase().as_str() {
        "italic" => Some("italic".into()),
        "oblique" => Some("oblique".into()),
        "roman" => Some("roman".into()),
        "normal" => Some("normal".into()),
        _ => None,
    }
}

fn normalize_spacing(token: &str) -> Option<i64> {
    match token.to_ascii_lowercase().as_str() {
        "mono" => Some(100),
        "proportional" => Some(0),
        _ => None,
    }
}

fn is_weight_name(token: &str) -> bool {
    normalize_weight(token).is_some()
}

fn is_slant_name(token: &str) -> bool {
    normalize_slant(token).is_some()
}

fn is_width_name(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "normal" | "semi-condensed"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lisp::reader::Reader;

    #[test]
    fn regex_resource_helper_patterns_compile() {
        for literal in [
            r#""\\(?:^\\|[^\\]\\)\\(?:\\\\\\\\\\)*\\\\""#,
            r#""\\(?:^\\|[^\\]\\)\\(?:\\\\\\\\\\)*\\\\.\\=""#,
        ] {
            let pattern = Reader::new(literal)
                .read()
                .expect("pattern literal should parse")
                .expect("pattern literal should contain a value");
            let pattern = string_text(&pattern).expect("pattern literal should be a string");
            let translated = translate_elisp_regex(&pattern);
            let rendered = format!("(?m:{translated})");
            assert!(
                FancyRegex::new(&rendered).is_ok(),
                "failed to compile `{pattern}` as `{rendered}`"
            );
        }
    }

    #[test]
    fn parse_font_names_matches_core_upstream_cases() {
        let cases = [
            (" ", Some(" "), None, None, None, None, None),
            ("Monospace", Some("Monospace"), None, None, None, None, None),
            (
                "Monospace Serif",
                Some("Monospace Serif"),
                None,
                None,
                None,
                None,
                None,
            ),
            ("Foo1", Some("Foo1"), None, None, None, None, None),
            ("12", None, Some(12.0), None, None, None, None),
            ("12 ", Some("12 "), None, None, None, None, None),
            ("Foo:", Some("Foo"), None, None, None, None, None),
            ("Foo-8", Some("Foo"), Some(8.0), None, None, None, None),
            ("Foo-18:", Some("Foo"), Some(18.0), None, None, None, None),
            (
                "Foo-18:light",
                Some("Foo"),
                Some(18.0),
                Some("light"),
                None,
                None,
                None,
            ),
            (
                "Foo 10:weight=bold",
                Some("Foo 10"),
                None,
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "Foo-12:weight=bold",
                Some("Foo"),
                Some(12.0),
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "Foo 8-20:slant=oblique",
                Some("Foo 8"),
                Some(20.0),
                None,
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:light:roman",
                Some("Foo"),
                None,
                Some("light"),
                Some("roman"),
                None,
                None,
            ),
            (
                "Foo:italic:roman",
                Some("Foo"),
                None,
                None,
                Some("roman"),
                None,
                None,
            ),
            (
                "Foo 12:light:oblique",
                Some("Foo 12"),
                None,
                Some("light"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo-12:demibold:oblique",
                Some("Foo"),
                Some(12.0),
                Some("demibold"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:black:proportional",
                Some("Foo"),
                None,
                Some("black"),
                None,
                Some(0),
                None,
            ),
            (
                "Foo-10:black:proportional",
                Some("Foo"),
                Some(10.0),
                Some("black"),
                None,
                Some(0),
                None,
            ),
            (
                "Foo:weight=normal",
                Some("Foo"),
                None,
                Some("normal"),
                None,
                None,
                None,
            ),
            (
                "Foo:weight=bold",
                Some("Foo"),
                None,
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "Foo:weight=bold:slant=italic",
                Some("Foo"),
                None,
                Some("bold"),
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo:weight=bold:slant=italic:mono",
                Some("Foo"),
                None,
                Some("bold"),
                Some("italic"),
                Some(100),
                None,
            ),
            (
                "Foo-10:demibold:slant=normal",
                Some("Foo"),
                Some(10.0),
                Some("demibold"),
                Some("normal"),
                None,
                None,
            ),
            (
                "Foo 11-16:oblique:weight=bold",
                Some("Foo 11"),
                Some(16.0),
                Some("bold"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:oblique:randomprop=randomtag:weight=bold",
                Some("Foo"),
                None,
                Some("bold"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:randomprop=randomtag:bar=baz",
                Some("Foo"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo Book Light:bar=baz",
                Some("Foo Book Light"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo Book Light 10:bar=baz",
                Some("Foo Book Light 10"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo Book Light-10:bar=baz",
                Some("Foo Book Light"),
                Some(10.0),
                None,
                None,
                None,
                None,
            ),
            ("Oblique", None, None, None, Some("oblique"), None, None),
            ("Bold 17", None, Some(17.0), Some("bold"), None, None, None),
            ("17 Bold", Some("17"), None, Some("bold"), None, None, None),
            (
                "Book Oblique 2",
                None,
                Some(2.0),
                Some("book"),
                Some("oblique"),
                None,
                None,
            ),
            ("Bar 7", Some("Bar"), Some(7.0), None, None, None, None),
            (
                "Bar Ultra-Light",
                Some("Bar"),
                None,
                Some("ultra-light"),
                None,
                None,
                None,
            ),
            (
                "Bar Light 8",
                Some("Bar"),
                Some(8.0),
                Some("light"),
                None,
                None,
                None,
            ),
            (
                "Bar Book Medium 9",
                Some("Bar"),
                Some(9.0),
                Some("medium"),
                None,
                None,
                None,
            ),
            (
                "Bar Semi-Bold Italic 10",
                Some("Bar"),
                Some(10.0),
                Some("semi-bold"),
                Some("italic"),
                None,
                None,
            ),
            (
                "Bar Semi-Condensed Bold Italic 11",
                Some("Bar"),
                Some(11.0),
                Some("bold"),
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo 10 11",
                Some("Foo 10"),
                Some(11.0),
                None,
                None,
                None,
                None,
            ),
            (
                "Foo 1985 Book",
                Some("Foo 1985"),
                None,
                Some("book"),
                None,
                None,
                None,
            ),
            (
                "Foo 1985 A Book",
                Some("Foo 1985 A"),
                None,
                Some("book"),
                None,
                None,
                None,
            ),
            (
                "Foo 1 Book 12",
                Some("Foo 1"),
                Some(12.0),
                Some("book"),
                None,
                None,
                None,
            ),
            (
                "Foo A Book 12 A",
                Some("Foo A Book 12 A"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo 1985 Book 12 Oblique",
                Some("Foo 1985 Book 12"),
                None,
                None,
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo 1985 Book 12 Italic 10",
                Some("Foo 1985 Book 12"),
                Some(10.0),
                None,
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo Book Bar 6 Italic",
                Some("Foo Book Bar 6"),
                None,
                None,
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo Book Bar Bold",
                Some("Foo Book Bar"),
                None,
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "-GNU -FreeSans-semibold-italic-normal-*-*-*-*-*-*-0-iso10646-1",
                Some("FreeSans"),
                None,
                Some("semi-bold"),
                None,
                None,
                Some("GNU "),
            ),
            (
                "-Take-mikachan-PS-normal-normal-normal-*-*-*-*-*-*-0-iso10646-1",
                Some("mikachan-PS"),
                None,
                Some("normal"),
                None,
                None,
                Some("Take"),
            ),
            (
                "-foundry-name-with-lots-of-dashes-normal-normal-normal-*-*-*-*-*-*-0-iso10646-1",
                Some("name-with-lots-of-dashes"),
                None,
                Some("normal"),
                None,
                None,
                Some("foundry"),
            ),
        ];

        for (name, family, size, weight, slant, spacing, foundry) in cases {
            let actual = parse_font_name(name);
            assert_eq!(
                actual.family.as_deref(),
                family,
                "family mismatch for {name:?}"
            );
            assert_eq!(actual.size, size, "size mismatch for {name:?}");
            assert_eq!(
                actual.weight.as_deref(),
                weight,
                "weight mismatch for {name:?}"
            );
            assert_eq!(
                actual.slant.as_deref(),
                slant,
                "slant mismatch for {name:?}"
            );
            assert_eq!(actual.spacing, spacing, "spacing mismatch for {name:?}");
            assert_eq!(
                actual.foundry.as_deref(),
                foundry,
                "foundry mismatch for {name:?}"
            );
        }
    }

    #[test]
    fn file_name_path_helpers_match_core_unix_cases() {
        assert_eq!(file_name_directory("/abc"), Some("/".into()));
        assert_eq!(file_name_directory("/abc/"), Some("/abc/".into()));
        assert_eq!(file_name_directory("abc"), None);

        assert_eq!(file_name_as_directory(""), "./");
        assert_eq!(file_name_as_directory("/abc"), "/abc/");
        assert_eq!(file_name_as_directory("/abc/"), "/abc/");

        assert_eq!(directory_file_name("/"), "/");
        assert_eq!(directory_file_name("//"), "//");
        assert_eq!(directory_file_name("///"), "/");
        assert_eq!(directory_file_name("/abc/"), "/abc");

        assert_eq!(file_name_concat(&["foo".into(), "bar".into()]), "foo/bar");
        assert_eq!(file_name_concat(&["foo/".into(), "bar".into()]), "foo/bar");
        assert_eq!(
            file_name_concat(&["foo//".into(), "bar".into()]),
            "foo//bar"
        );
        assert_eq!(expand_file_name("/abc/", None), "/abc/");
        assert_eq!(expand_file_name("abc/", Some("/tmp/")), "/tmp/abc/");
        assert!(file_name_absolute_p("/tmp/example"));
        assert!(file_name_absolute_p("~/example"));
        assert!(!"~/example".starts_with('/'));
    }

    #[test]
    fn ert_resource_directory_prefers_sibling_resources_dir() {
        assert_eq!(
            ert_resource_directory_for("/tmp/example-tests.el"),
            "/tmp/example-resources/"
        );
        assert_eq!(
            ert_resource_directory_for("/Users/alpha/CodexProjects/emacs/test/src/syntax-tests.el"),
            "/Users/alpha/CodexProjects/emacs/test/src/syntax-resources/"
        );
    }

    #[test]
    fn ert_resource_directory_trims_test_suffixes_like_emacs() {
        assert_eq!(
            ert_resource_directory_for("/tmp/foo-test.el"),
            "/tmp/foo-resources/"
        );
        assert_eq!(
            ert_resource_directory_for("/tmp/foo-tests.el"),
            "/tmp/foo-resources/"
        );
        assert_eq!(
            ert_resource_directory_for("/tmp/bookmark.el"),
            "/tmp/bookmark-resources/"
        );
    }

    #[test]
    fn charset_helpers_cover_ascii_unicode_and_priority_mutation() {
        let mut interp = Interpreter::new();
        assert!(interp.has_charset("ascii"));
        assert_eq!(interp.charset_id("unicode"), Some(1));
        assert_eq!(charset_for_char('A' as u32), "ascii");
        assert_eq!(charset_for_char('あ' as u32), "unicode");

        interp
            .define_charset_alias("latin", "ascii")
            .expect("ascii alias should be accepted");
        assert!(interp.has_charset("latin"));

        interp.set_charset_priority(&["ascii".into(), "unicode".into()]);
        assert_eq!(interp.charset_priority_list(), vec!["ascii", "unicode"]);
        assert_eq!(
            charsets_for_text("Aあ", &interp),
            vec![
                Value::Symbol("ascii".into()),
                Value::Symbol("unicode".into())
            ]
        );
    }

    #[test]
    fn substitute_in_file_name_expands_shell_style_env_vars() {
        let old = std::env::var("EMAXX_SUBST_TEST").ok();
        unsafe {
            std::env::set_var("EMAXX_SUBST_TEST", "value");
        }
        assert_eq!(
            substitute_in_file_name("$EMAXX_SUBST_TEST/${EMAXX_SUBST_TEST}/$$"),
            "value/value/$"
        );
        if let Some(value) = old {
            unsafe {
                std::env::set_var("EMAXX_SUBST_TEST", value);
            }
        } else {
            unsafe {
                std::env::remove_var("EMAXX_SUBST_TEST");
            }
        }
    }

    #[test]
    fn compat_paths_follow_emacs_test_directory_layout() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let repo_root = std::env::temp_dir().join(format!("emaxx-compat-paths-{unique}"));
        let test_dir = repo_root.join("test");
        let src_dir = repo_root.join("src");
        let lib_src_dir = repo_root.join("lib-src");
        std::fs::create_dir_all(&test_dir).expect("create test directory");
        std::fs::create_dir_all(&src_dir).expect("create src directory");
        std::fs::create_dir_all(&lib_src_dir).expect("create lib-src directory");
        std::fs::write(src_dir.join("emacs"), "").expect("write fake emacs binary");
        std::fs::write(lib_src_dir.join("emacsclient"), "").expect("write fake emacsclient binary");

        let test_directory = test_dir.display().to_string();
        assert_eq!(
            compat_invocation_path_from_test_directory(&test_directory),
            Some(src_dir.join("emacs"))
        );
        assert_eq!(
            compat_emacsclient_path_from_test_directory(&test_directory),
            Some(lib_src_dir.join("emacsclient"))
        );

        let _ = std::fs::remove_dir_all(&repo_root);
    }

    #[cfg(unix)]
    #[test]
    fn process_lines_uses_default_directory_as_subprocess_cwd() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let cwd = std::env::temp_dir().join(format!("emaxx-process-lines-{unique}"));
        std::fs::create_dir_all(&cwd).expect("create temp cwd");
        let expected = std::fs::canonicalize(&cwd)
            .expect("canonical temp cwd")
            .display()
            .to_string();

        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        interp.set_variable(
            "default-directory",
            Value::String(cwd.display().to_string()),
            &mut env,
        );

        let result = call(
            &mut interp,
            "process-lines",
            &[Value::String("/bin/pwd".into())],
            &mut env,
        )
        .expect("process-lines should succeed");

        assert_eq!(result, Value::list([Value::String(expected)]));

        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[test]
    fn indent_rigidly_shifts_each_line_in_region() {
        let mut interp = Interpreter::new();
        interp.buffer = crate::buffer::Buffer::from_text("*test*", "a\nb\n");
        let mut env = Vec::new();

        call(
            &mut interp,
            "indent-rigidly",
            &[Value::Integer(1), Value::Integer(5), Value::Integer(2)],
            &mut env,
        )
        .expect("indent-rigidly should succeed");

        assert_eq!(
            interp
                .buffer
                .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
                .expect("buffer contents"),
            "  a\n  b\n"
        );
    }

    #[test]
    fn inhibit_read_only_allows_buffer_read_only_edits() {
        let mut interp = Interpreter::new();
        interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
        let mut env = Vec::new();
        interp.set_variable("buffer-read-only", Value::T, &mut env);
        interp.set_variable("inhibit-read-only", Value::T, &mut env);
        interp.buffer.goto_char(1);

        call(&mut interp, "delete-char", &[Value::Integer(1)], &mut env)
            .expect("delete-char should ignore buffer-read-only when inhibited");

        assert_eq!(
            interp
                .buffer
                .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
                .expect("buffer contents"),
            "bc"
        );
    }

    #[test]
    fn looking_at_p_preserves_existing_match_data() {
        let mut interp = Interpreter::new();
        interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
        let mut env = Vec::new();
        call(
            &mut interp,
            "re-search-forward",
            &[Value::String("a".into())],
            &mut env,
        )
        .expect("re-search-forward should set match data");
        let saved = interp.last_match_data.clone();
        interp.buffer.goto_char(1);

        let result = call(
            &mut interp,
            "looking-at-p",
            &[Value::String("z".into())],
            &mut env,
        )
        .expect("looking-at-p should return nil for a failed match");

        assert_eq!(result, Value::Nil);
        assert_eq!(interp.last_match_data, saved);
    }

    #[test]
    fn set_text_properties_replaces_existing_properties() {
        let mut interp = Interpreter::new();
        interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
        let mut env = Vec::new();

        call(
            &mut interp,
            "add-text-properties",
            &[
                Value::Integer(1),
                Value::Integer(3),
                Value::list([
                    Value::Symbol("face".into()),
                    Value::Symbol("bold".into()),
                    Value::Symbol("mouse-face".into()),
                    Value::Symbol("highlight".into()),
                ]),
            ],
            &mut env,
        )
        .expect("add-text-properties should seed buffer props");

        call(
            &mut interp,
            "set-text-properties",
            &[
                Value::Integer(1),
                Value::Integer(3),
                Value::list([Value::Symbol("face".into()), Value::Symbol("italic".into())]),
            ],
            &mut env,
        )
        .expect("set-text-properties should replace buffer props");

        assert_eq!(
            interp.buffer.text_properties_at(1),
            vec![("face".into(), Value::Symbol("italic".into()))]
        );

        let string = call(
            &mut interp,
            "buffer-substring",
            &[Value::Integer(1), Value::Integer(3)],
            &mut env,
        )
        .expect("buffer-substring should preserve text properties");

        call(
            &mut interp,
            "set-text-properties",
            &[
                Value::Integer(0),
                Value::Integer(2),
                Value::list([
                    Value::Symbol("face".into()),
                    Value::Symbol("underline".into()),
                ]),
                string.clone(),
            ],
            &mut env,
        )
        .expect("set-text-properties should replace substring props");

        let props = call(
            &mut interp,
            "text-properties-at",
            &[Value::Integer(0), string],
            &mut env,
        )
        .expect("text-properties-at should read string props");
        assert_eq!(
            props,
            Value::list([
                Value::Symbol("face".into()),
                Value::Symbol("underline".into()),
            ])
        );
    }

    #[test]
    fn font_lock_text_property_helpers_keep_anonymous_faces_atomic() {
        let mut interp = Interpreter::new();
        interp.buffer = crate::buffer::Buffer::from_text("*test*", "foo");
        let mut env = Vec::new();

        call(
            &mut interp,
            "add-text-properties",
            &[
                Value::Integer(1),
                Value::Integer(3),
                Value::list([Value::Symbol("face".into()), Value::Symbol("italic".into())]),
            ],
            &mut env,
        )
        .expect("add-text-properties should seed a face property");

        call(
            &mut interp,
            "font-lock-append-text-property",
            &[
                Value::Integer(1),
                Value::Integer(3),
                Value::Symbol("face".into()),
                Value::list([Value::Symbol(":strike-through".into()), Value::T]),
            ],
            &mut env,
        )
        .expect("font-lock-append-text-property should accept an omitted object");

        assert_eq!(
            interp.buffer.text_property_at(1, "face"),
            Some(Value::list([
                Value::Symbol("italic".into()),
                Value::list([Value::Symbol(":strike-through".into()), Value::T,]),
            ]))
        );

        call(
            &mut interp,
            "font-lock-prepend-text-property",
            &[
                Value::Integer(1),
                Value::Integer(3),
                Value::Symbol("face".into()),
                Value::list([Value::Symbol(":underline".into()), Value::T]),
            ],
            &mut env,
        )
        .expect("font-lock-prepend-text-property should accept an omitted object");

        assert_eq!(
            interp.buffer.text_property_at(1, "face"),
            Some(Value::list([
                Value::list([Value::Symbol(":underline".into()), Value::T]),
                Value::Symbol("italic".into()),
                Value::list([Value::Symbol(":strike-through".into()), Value::T,]),
            ]))
        );
    }

    #[test]
    fn bidi_override_positions_match_upstream_cases() {
        let cases = [
            (
                "int main() {\n  bool isAdmin = false;\n  /*\u{202e} }\u{2066}if (isAdmin)\u{2069} \u{2066} begin admins only */\n  printf(\"You are an admin.\\\\n\");\n  /* end admins only \u{202e} { \u{2066}*/\n  return 0;\n}",
                Some(46),
            ),
            (
                "#define is_restricted_user(user)\t\t\t\\\\\n  !strcmp (user, \"root\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"admin\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"superuser\u{202e}\u{2066}? 0 : 1\u{2069} \u{2066}\")\u{2069}\u{202c}\n\nint main () {\n  printf (\"root: %d\\\\n\", is_restricted_user (\"root\"));\n  printf (\"admin: %d\\\\n\", is_restricted_user (\"admin\"));\n  printf (\"superuser: %d\\\\n\", is_restricted_user (\"superuser\"));\n  printf (\"luser: %d\\\\n\", is_restricted_user (\"luser\"));\n  printf (\"nobody: %d\\\\n\", is_restricted_user (\"nobody\"));\n}",
                None,
            ),
            (
                "#define is_restricted_user(user)\t\t\t\\\\\n  !strcmp (user, \"root\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"admin\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"superuser\u{202e}\u{2066}? '#' : '!'\u{2069} \u{2066}\")\u{2069}\u{202c}\n\nint main () {\n  printf (\"root: %d\\\\n\", is_restricted_user (\"root\"));\n  printf (\"admin: %d\\\\n\", is_restricted_user (\"admin\"));\n  printf (\"superuser: %d\\\\n\", is_restricted_user (\"superuser\"));\n  printf (\"luser: %d\\\\n\", is_restricted_user (\"luser\"));\n  printf (\"nobody: %d\\\\n\", is_restricted_user (\"nobody\"));\n}",
                None,
            ),
        ];

        for (index, (text, expected_exact)) in cases.into_iter().enumerate() {
            let mut interp = Interpreter::new();
            interp.buffer = crate::buffer::Buffer::from_text("*test*", text);
            let found = find_bidi_override(
                &interp,
                interp.buffer.point_min(),
                interp.buffer.point_max(),
            );
            if let Some(expected) = expected_exact {
                assert_eq!(found, Some(expected));
            } else {
                assert!(
                    found.is_some(),
                    "case {index} should report a suspicious bidi override position"
                );
            }
        }
    }

    #[test]
    fn key_description_formats_follow_prefix_defaults() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let result = call(
            &mut interp,
            "key-description",
            &[Value::String("\u{3}.".into())],
            &mut env,
        )
        .expect("key-description should accept control-char strings");
        assert_eq!(result, Value::String("C-c .".into()));
    }

    #[test]
    fn key_description_matches_upstream_string_and_vector_cases() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let prefixed = call(
            &mut interp,
            "key-description",
            &[
                Value::list([
                    Value::Symbol("vector".into()),
                    Value::Symbol("right".into()),
                ]),
                Value::list([Value::Symbol("vector".into()), Value::Integer(0x18)]),
            ],
            &mut env,
        )
        .expect("key-description should format prefixed vector keys");
        assert_eq!(prefixed, Value::String("C-x <right>".into()));

        let raw_byte = call(
            &mut interp,
            "key-description",
            &[bytes_to_unibyte_value(&[0xE1])],
            &mut env,
        )
        .expect("key-description should normalize raw unibyte meta bytes");
        assert_eq!(raw_byte, Value::String("M-a".into()));
    }

    #[test]
    fn single_key_description_matches_symbol_cases() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let home = call(
            &mut interp,
            "single-key-description",
            &[Value::Symbol("home".into())],
            &mut env,
        )
        .expect("single-key-description should wrap event symbols");
        assert_eq!(home, Value::String("<home>".into()));

        let plain = call(
            &mut interp,
            "single-key-description",
            &[Value::Symbol("home".into()), Value::T],
            &mut env,
        )
        .expect("single-key-description should honor no-angles");
        assert_eq!(plain, Value::String("home".into()));
    }
}

fn replacement_content(interp: &Interpreter, source: &Value) -> Result<StringLike, LispError> {
    if let Some(string) = string_like(source) {
        return Ok(string);
    }
    match source {
        Value::Buffer(id, _) => {
            let buffer = interp
                .get_buffer_by_id(*id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", id)))?;
            let text = buffer
                .buffer_substring(buffer.point_min(), buffer.point_max())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(StringLike {
                multibyte: text.chars().any(|ch| (ch as u32) > 0x7F),
                text,
                props: buffer.substring_property_spans(buffer.point_min(), buffer.point_max()),
            })
        }
        _ => {
            let items = vector_items(source)?;
            if items.len() >= 3 {
                let buffer_id = interp.resolve_buffer_id(&items[0])?;
                let start = position_from_value(interp, &items[1])?;
                let end = position_from_value(interp, &items[2])?;
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                let text = buffer
                    .buffer_substring(start, end)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                Ok(StringLike {
                    multibyte: text.chars().any(|ch| (ch as u32) > 0x7F),
                    text,
                    props: buffer.substring_property_spans(start, end),
                })
            } else {
                Err(LispError::TypeError(
                    "string-or-buffer".into(),
                    source.type_name(),
                ))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StringLike {
    pub(crate) text: String,
    pub(crate) props: Vec<TextPropertySpan>,
    pub(crate) multibyte: bool,
}

pub(crate) fn string_like(value: &Value) -> Option<StringLike> {
    match value {
        Value::String(text) => Some(StringLike {
            text: text.clone(),
            props: Vec::new(),
            multibyte: text
                .chars()
                .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F),
        }),
        Value::StringObject(state) => {
            let state = state.borrow();
            Some(StringLike {
                text: state.text.clone(),
                props: state
                    .props
                    .iter()
                    .map(|span| TextPropertySpan {
                        start: span.start,
                        end: span.end,
                        props: span.props.clone(),
                    })
                    .collect(),
                multibyte: state.multibyte,
            })
        }
        Value::Cons(_, _) if is_vector_value(value) => {
            let items = vector_items(value).ok()?;
            let Value::String(text) = items.first()?.clone() else {
                return None;
            };
            let mut props = Vec::new();
            let mut i = 1;
            while i + 2 < items.len() {
                let start = items[i].as_integer().ok()? as usize;
                let end = items[i + 1].as_integer().ok()? as usize;
                let plist = plist_pairs(&items[i + 2]).ok()?;
                props.push(TextPropertySpan {
                    start,
                    end,
                    props: plist,
                });
                i += 3;
            }
            let multibyte = text
                .chars()
                .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
            Some(StringLike {
                text,
                props,
                multibyte,
            })
        }
        Value::Cons(_, _) => None,
        _ => None,
    }
}

pub(crate) fn string_text(value: &Value) -> Result<String, LispError> {
    string_like(value)
        .map(|string| string.text)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))
}

fn string_comparison_text(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Nil => Ok("nil".into()),
        Value::T => Ok("t".into()),
        Value::Symbol(name) => Ok(name.clone()),
        _ => string_text(value),
    }
}

pub(crate) fn aset_string_value(
    target: &Value,
    index: usize,
    new_value: &Value,
) -> Result<Value, LispError> {
    let mut string = string_like(target)
        .ok_or_else(|| LispError::TypeError("string".into(), target.type_name()))?;
    let code = new_value.as_integer()?;
    let ch = if !string.multibyte && (0..=255).contains(&code) {
        let byte = code as u8;
        if byte <= 0x7F {
            byte as char
        } else {
            raw_byte_regex_char(byte)
        }
    } else {
        char::from_u32(code as u32).ok_or_else(|| LispError::Signal("Invalid character".into()))?
    };
    let mut chars: Vec<char> = string.text.chars().collect();
    if index >= chars.len() {
        return Err(LispError::Signal("Args out of range".into()));
    }
    chars[index] = ch;
    string.text = chars.into_iter().collect();
    Ok(make_shared_string_value_with_multibyte(
        string.text,
        string.props,
        string.multibyte,
    ))
}

fn shared_string_props(props: &[TextPropertySpan]) -> Vec<StringPropertySpan> {
    props
        .iter()
        .map(|span| StringPropertySpan {
            start: span.start,
            end: span.end,
            props: span.props.clone(),
        })
        .collect()
}

pub(crate) fn make_shared_string_value_with_multibyte(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
) -> Value {
    Value::StringObject(Rc::new(RefCell::new(SharedStringState {
        text,
        props: shared_string_props(&props),
        multibyte,
    })))
}

fn string_like_value(text: String, props: Vec<TextPropertySpan>) -> Value {
    if props.is_empty() {
        Value::String(text)
    } else {
        let multibyte = text
            .chars()
            .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
        make_shared_string_value_with_multibyte(text, merge_string_props(props), multibyte)
    }
}

fn reverse_string_like_value(value: &Value) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    let len = string.text.chars().count();
    let text = string.text.chars().rev().collect::<String>();
    let props = string
        .props
        .into_iter()
        .map(|span| TextPropertySpan {
            start: len - span.end,
            end: len - span.start,
            props: span.props,
        })
        .collect();
    Ok(string_like_value(text, merge_string_props(props)))
}

fn plist_pairs(value: &Value) -> Result<Vec<(String, Value)>, LispError> {
    let items = value.to_vec()?;
    let mut props = Vec::new();
    let mut i = 0;
    while i + 1 < items.len() {
        let key = items[i].as_symbol()?.to_string();
        props.push((key, items[i + 1].clone()));
        i += 2;
    }
    Ok(props)
}

fn plist_value(props: &[(String, Value)]) -> Value {
    let mut items = Vec::new();
    for (key, value) in props {
        items.push(Value::Symbol(key.clone()));
        items.push(value.clone());
    }
    Value::list(items)
}

fn shift_string_props(props: &[TextPropertySpan], offset: usize) -> Vec<TextPropertySpan> {
    props
        .iter()
        .map(|span| TextPropertySpan {
            start: span.start + offset,
            end: span.end + offset,
            props: span.props.clone(),
        })
        .collect()
}

fn slice_string_props(props: &[TextPropertySpan], from: usize, to: usize) -> Vec<TextPropertySpan> {
    let mut sliced = Vec::new();
    for span in props {
        let start = span.start.max(from);
        let end = span.end.min(to);
        if start < end {
            sliced.push(TextPropertySpan {
                start: start - from,
                end: end - from,
                props: span.props.clone(),
            });
        }
    }
    merge_string_props(sliced)
}

fn merge_string_props(mut props: Vec<TextPropertySpan>) -> Vec<TextPropertySpan> {
    props.retain(|span| span.start < span.end && !span.props.is_empty());
    props.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<TextPropertySpan> = Vec::new();
    for span in props {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && last.props == span.props
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
    }
    merged
}

fn string_property_at(value: &Value, pos: usize, prop: &str) -> Option<Value> {
    let string = string_like(value)?;
    string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .and_then(|span| {
            span.props
                .iter()
                .find(|(name, _)| name == prop)
                .map(|(_, value)| value.clone())
        })
}

fn string_properties_at(value: &Value, pos: usize) -> Vec<(String, Value)> {
    string_like(value)
        .and_then(|string| {
            string
                .props
                .iter()
                .find(|span| span.start <= pos && pos < span.end)
                .map(|span| span.props.clone())
        })
        .unwrap_or_default()
}

fn merge_string_object_props(mut spans: Vec<StringPropertySpan>) -> Vec<StringPropertySpan> {
    spans.retain(|span| span.start < span.end && !span.props.is_empty());
    spans.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<StringPropertySpan> = Vec::new();
    for span in spans {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && last.props == span.props
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
    }
    merged
}

fn string_object_properties_at(spans: &[StringPropertySpan], pos: usize) -> Vec<(String, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

fn modify_shared_string_properties<F>(
    value: &Value,
    start: usize,
    end: usize,
    mut f: F,
) -> Result<(), LispError>
where
    F: FnMut(Vec<(String, Value)>) -> Vec<(String, Value)>,
{
    let Value::StringObject(state) = value else {
        return Err(LispError::TypeError("string".into(), value.type_name()));
    };
    let mut state = state.borrow_mut();
    let len = state.text.chars().count();
    let start = start.min(len);
    let end = end.min(len);
    if start >= end {
        return Ok(());
    }

    let original = state.props.clone();
    let mut updated = Vec::new();
    for span in &original {
        if span.end <= start || span.start >= end {
            updated.push(span.clone());
        } else {
            if span.start < start {
                updated.push(StringPropertySpan {
                    start: span.start,
                    end: start,
                    props: span.props.clone(),
                });
            }
            if span.end > end {
                updated.push(StringPropertySpan {
                    start: end,
                    end: span.end,
                    props: span.props.clone(),
                });
            }
        }
    }

    let mut boundaries = vec![start, end];
    for span in &original {
        if span.end <= start || span.start >= end {
            continue;
        }
        boundaries.push(span.start.max(start));
        boundaries.push(span.end.min(end));
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    for window in boundaries.windows(2) {
        let seg_start = window[0];
        let seg_end = window[1];
        if seg_start >= seg_end {
            continue;
        }
        let current = string_object_properties_at(&original, seg_start);
        let next = f(current);
        if !next.is_empty() {
            updated.push(StringPropertySpan {
                start: seg_start,
                end: seg_end,
                props: next,
            });
        }
    }

    state.props = merge_string_object_props(updated);
    Ok(())
}

pub(crate) fn call_function_value(
    interp: &mut Interpreter,
    function: &Value,
    args: &[Value],
    env: &mut super::types::Env,
) -> Result<Value, LispError> {
    interp.call_function_value(function.clone(), None, args, env)
}

fn run_change_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    args: &[Value],
    env: &mut super::types::Env,
) -> Result<(), LispError> {
    if interp.change_hooks_are_running() {
        return Ok(());
    }
    let hook_values = interp
        .lookup_var(hook_name, env)
        .map(|value| value.to_vec().unwrap_or_default())
        .or_else(|| interp.buffer_local_hook(interp.current_buffer_id(), hook_name))
        .unwrap_or_default();
    if hook_values.is_empty() {
        return Ok(());
    }
    interp.enter_change_hooks();
    let mut result = Ok(());
    for hook in hook_values {
        if let Err(error) = call_function_value(interp, &hook, args, env) {
            result = Err(error);
            break;
        }
    }
    interp.leave_change_hooks();
    result
}

fn hook_values(
    interp: &Interpreter,
    hook_name: &str,
    env: &super::types::Env,
    buffer_id: Option<u64>,
) -> Vec<Value> {
    let mut hooks = interp
        .lookup_var(hook_name, env)
        .map(|value| value.to_vec().unwrap_or_default())
        .unwrap_or_default();
    if let Some(id) = buffer_id
        && let Some(local) = interp.buffer_local_hook(id, hook_name)
    {
        hooks.extend(local);
    }
    hooks
}

fn run_named_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    env: &mut super::types::Env,
    buffer_id: Option<u64>,
) -> Result<(), LispError> {
    for hook in hook_values(interp, hook_name, env, buffer_id) {
        call_function_value(interp, &hook, &[], env)?;
    }
    Ok(())
}

fn call_named_function(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut super::types::Env,
) -> Result<Value, LispError> {
    match interp.lookup_function(name, env) {
        Ok(function) => call_function_value(interp, &function, args, env),
        Err(_) => Ok(Value::T),
    }
}

#[derive(Clone)]
struct OverlayHookCall {
    overlay_id: u64,
    functions: Vec<Value>,
    before_tail: Vec<Value>,
    after_tail: Vec<Value>,
}

fn overlay_hook_functions(overlay: &crate::overlay::Overlay, property: &str) -> Vec<Value> {
    match overlay.get_prop(property) {
        Some(value) => value
            .to_vec()
            .unwrap_or_else(|_| vec![value.clone()])
            .into_iter()
            .filter(|value| value.is_truthy())
            .collect(),
        None => Vec::new(),
    }
}

fn overlay_insert_hook_calls(
    buffer: &crate::buffer::Buffer,
    pos: usize,
    inserted_len: usize,
) -> Vec<OverlayHookCall> {
    let mut calls = Vec::new();
    for overlay in &buffer.overlays {
        if overlay.is_dead() {
            continue;
        }
        if overlay.beg == overlay.end && overlay.beg == pos {
            for property in ["insert-in-front-hooks", "insert-behind-hooks"] {
                let functions = overlay_hook_functions(overlay, property);
                if !functions.is_empty() {
                    calls.push(OverlayHookCall {
                        overlay_id: overlay.id,
                        functions,
                        before_tail: vec![Value::Integer(pos as i64), Value::Integer(pos as i64)],
                        after_tail: vec![
                            Value::Integer(pos as i64),
                            Value::Integer((pos + inserted_len) as i64),
                            Value::Integer(0),
                        ],
                    });
                }
            }
            continue;
        }
        let property = if pos == overlay.beg {
            Some("insert-in-front-hooks")
        } else if pos == overlay.end {
            Some("insert-behind-hooks")
        } else if overlay.beg < pos && pos < overlay.end {
            Some("modification-hooks")
        } else {
            None
        };
        let Some(property) = property else {
            continue;
        };
        let functions = overlay_hook_functions(overlay, property);
        if functions.is_empty() {
            continue;
        }
        calls.push(OverlayHookCall {
            overlay_id: overlay.id,
            functions,
            before_tail: vec![Value::Integer(pos as i64), Value::Integer(pos as i64)],
            after_tail: vec![
                Value::Integer(pos as i64),
                Value::Integer((pos + inserted_len) as i64),
                Value::Integer(0),
            ],
        });
    }
    calls
}

fn overlay_change_hook_calls(
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
    new_end: usize,
) -> Vec<OverlayHookCall> {
    let mut calls = Vec::new();
    let old_len = to.saturating_sub(from);
    for overlay in &buffer.overlays {
        if overlay.is_dead() || overlay.beg == overlay.end {
            continue;
        }
        if overlay.beg < to && from < overlay.end {
            let functions = overlay_hook_functions(overlay, "modification-hooks");
            if functions.is_empty() {
                continue;
            }
            calls.push(OverlayHookCall {
                overlay_id: overlay.id,
                functions,
                before_tail: vec![Value::Integer(from as i64), Value::Integer(to as i64)],
                after_tail: vec![
                    Value::Integer(from as i64),
                    Value::Integer(new_end as i64),
                    Value::Integer(old_len as i64),
                ],
            });
        }
    }
    calls
}

fn run_overlay_hook_calls(
    interp: &mut Interpreter,
    calls: &[OverlayHookCall],
    after: bool,
    env: &mut super::types::Env,
) -> Result<(), LispError> {
    env.push(vec![("inhibit-modification-hooks".into(), Value::T)]);
    for call in calls {
        if after && interp.find_overlay(call.overlay_id).is_none() {
            continue;
        }
        for function in &call.functions {
            let mut args = vec![
                Value::Overlay(call.overlay_id),
                if after { Value::T } else { Value::Nil },
            ];
            args.extend(if after {
                call.after_tail.clone()
            } else {
                call.before_tail.clone()
            });
            call_function_value(interp, function, &args, env)?;
        }
    }
    env.pop();
    Ok(())
}

fn delete_region_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut super::types::Env,
) -> Result<String, LispError> {
    if from >= to {
        return Ok(String::new());
    }
    let overlay_calls = overlay_change_hook_calls(&interp.buffer, from, to, from);
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
    let has_before_hooks = interp
        .lookup_var("before-change-functions", env)
        .map(|value| !value.to_vec().unwrap_or_default().is_empty())
        .or_else(|| {
            interp
                .buffer_local_hook(interp.current_buffer_id(), "before-change-functions")
                .map(|hooks| !hooks.is_empty())
        })
        .unwrap_or(false);
    if has_before_hooks {
        let start_marker = interp.make_marker();
        let end_marker = interp.make_marker();
        let dead_marker = interp.make_marker();
        if let (Value::Marker(start_id), Value::Marker(end_id), Value::Marker(dead_id)) =
            (start_marker, end_marker, dead_marker)
        {
            let buffer_id = interp.current_buffer_id();
            let _ = interp.set_marker(start_id, Some(from), Some(buffer_id));
            let _ = interp.set_marker(end_id, Some(to), Some(buffer_id));
            let _ = interp.set_marker(dead_id, None, None);
            interp.buffer.push_undo_meta(Value::cons(
                Value::Marker(start_id),
                Value::Integer(-(from as i64)),
            ));
            interp.buffer.push_undo_meta(Value::cons(
                Value::Marker(end_id),
                Value::Integer(-(to as i64)),
            ));
            interp
                .buffer
                .push_undo_meta(Value::cons(Value::Marker(dead_id), Value::Integer(-1)));
        }
    }
    run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(from as i64), Value::Integer(to as i64)],
        env,
    )?;
    let deleted = interp
        .delete_region_current_buffer(from, to)
        .map_err(|e| LispError::Signal(e.to_string()))?;
    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(from as i64),
            Value::Integer(from as i64),
            Value::Integer((to - from) as i64),
        ],
        env,
    )?;
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
    Ok(deleted)
}

fn ensure_region_modifiable(
    interp: &Interpreter,
    from: usize,
    to: usize,
    env: &mut super::types::Env,
) -> Result<(), LispError> {
    let from = from.max(interp.buffer.point_min());
    let to = to.min(interp.buffer.point_max());
    if from >= to {
        return Ok(());
    }
    let buffer_read_only = interp
        .lookup_var("buffer-read-only", env)
        .is_some_and(|value| value.is_truthy());
    let inhibit_read_only = interp
        .lookup_var("inhibit-read-only", env)
        .unwrap_or(Value::Nil);

    for pos in from..to {
        let read_only = interp.buffer.text_property_at(pos, "read-only");
        let suppressor = interp.buffer.text_property_at(pos, "inhibit-read-only");
        if let Some(read_only_value) = read_only {
            if suppressor.is_some_and(|value| value.is_truthy())
                || inhibit_read_only_matches(&inhibit_read_only, &read_only_value)
            {
                continue;
            }
            return Err(LispError::Signal("Text is read-only".into()));
        }
        if buffer_read_only
            && !inhibit_read_only.is_truthy()
            && !suppressor.is_some_and(|value| value.is_truthy())
        {
            return Err(LispError::Signal("Text is read-only".into()));
        }
    }
    Ok(())
}

fn inhibit_read_only_matches(inhibit: &Value, property: &Value) -> bool {
    if inhibit.is_nil() {
        return false;
    }
    if matches!(inhibit, Value::T) {
        return true;
    }
    if let Ok(items) = inhibit.to_vec() {
        return items.into_iter().any(|item| item == *property);
    }
    inhibit == property
}

fn font_lock_add_text_property(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    append: bool,
) -> Result<Value, LispError> {
    need_arg_range(name, args, 4, 5)?;
    let prop = args[2].as_symbol()?.to_string();
    let value = args[3].clone();
    if let Some(object) = args.get(4)
        && string_like(object).is_some()
    {
        let start = args[0].as_integer()?.max(0) as usize;
        let end = args[1].as_integer()?.max(0) as usize;
        let mut cursor = start;
        while cursor < end {
            let previous = string_property_at(object, cursor, &prop).unwrap_or(Value::Nil);
            let next = font_lock_next_string_property_change(object, cursor, end, &prop);
            let updated = combine_font_lock_property_value(&prop, previous, &value, append);
            modify_shared_string_properties(object, cursor, next, |mut current| {
                current.retain(|(key, _)| key != &prop);
                current.push((prop.clone(), updated.clone()));
                current
            })?;
            cursor = next;
        }
        return Ok(Value::Nil);
    }

    let start = position_from_value(interp, &args[0])?;
    let end = position_from_value(interp, &args[1])?;
    let buffer_id = font_lock_target_buffer_id(interp, args.get(4))?;
    let mut cursor = start;
    while cursor < end {
        let (previous, next) = font_lock_buffer_segment(interp, buffer_id, cursor, end, &prop)?;
        let updated = combine_font_lock_property_value(&prop, previous, &value, append);
        font_lock_put_buffer_property(interp, buffer_id, cursor, next, &prop, updated)?;
        cursor = next;
    }
    font_lock_push_buffer_undo_entry(interp, buffer_id)?;
    Ok(Value::Nil)
}

fn font_lock_target_buffer_id(
    interp: &Interpreter,
    object: Option<&Value>,
) -> Result<u64, LispError> {
    match object {
        Some(value) if !value.is_nil() => interp.resolve_buffer_id(value),
        _ => Ok(interp.current_buffer_id()),
    }
}

fn font_lock_buffer_segment(
    interp: &Interpreter,
    buffer_id: u64,
    start: usize,
    end: usize,
    prop: &str,
) -> Result<(Value, usize), LispError> {
    let buffer = interp
        .get_buffer_by_id(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
    let previous = buffer.text_property_at(start, prop).unwrap_or(Value::Nil);
    let next = font_lock_next_buffer_property_change(buffer, start, end, prop);
    Ok((previous, next))
}

fn font_lock_put_buffer_property(
    interp: &mut Interpreter,
    buffer_id: u64,
    start: usize,
    end: usize,
    prop: &str,
    value: Value,
) -> Result<(), LispError> {
    if buffer_id == interp.current_buffer_id() {
        if value.is_nil() {
            interp
                .buffer
                .remove_list_of_text_properties(start, end, &[prop.to_string()]);
        } else {
            interp.buffer.put_text_property(start, end, prop, value);
        }
        return Ok(());
    }

    let buffer = interp
        .get_buffer_by_id_mut(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
    if value.is_nil() {
        buffer.remove_list_of_text_properties(start, end, &[prop.to_string()]);
    } else {
        buffer.put_text_property(start, end, prop, value);
    }
    Ok(())
}

fn font_lock_push_buffer_undo_entry(
    interp: &mut Interpreter,
    buffer_id: u64,
) -> Result<(), LispError> {
    let entry = crate::buffer::UndoEntry::Combined {
        display: Value::Nil,
        entries: Vec::new(),
    };
    if buffer_id == interp.current_buffer_id() {
        interp.buffer.push_undo_entry(entry);
        return Ok(());
    }
    let buffer = interp
        .get_buffer_by_id_mut(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
    buffer.push_undo_entry(entry);
    Ok(())
}

fn font_lock_next_buffer_property_change(
    buffer: &crate::buffer::Buffer,
    start: usize,
    end: usize,
    prop: &str,
) -> usize {
    let initial = buffer.text_property_at(start, prop).unwrap_or(Value::Nil);
    for cursor in start.saturating_add(1)..end {
        if buffer.text_property_at(cursor, prop).unwrap_or(Value::Nil) != initial {
            return cursor;
        }
    }
    end
}

fn font_lock_next_string_property_change(
    value: &Value,
    start: usize,
    end: usize,
    prop: &str,
) -> usize {
    let initial = string_property_at(value, start, prop).unwrap_or(Value::Nil);
    for cursor in start.saturating_add(1)..end {
        if string_property_at(value, cursor, prop).unwrap_or(Value::Nil) != initial {
            return cursor;
        }
    }
    end
}

fn combine_font_lock_property_value(
    prop: &str,
    previous: Value,
    value: &Value,
    append: bool,
) -> Value {
    let mut previous_items = font_lock_previous_property_items(prop, previous);
    let mut value_items = font_lock_value_items(value);
    if append {
        previous_items.append(&mut value_items);
        Value::list(previous_items)
    } else {
        value_items.append(&mut previous_items);
        Value::list(value_items)
    }
}

fn font_lock_previous_property_items(prop: &str, previous: Value) -> Vec<Value> {
    if matches!(prop, "face" | "font-lock-face") && anonymous_font_lock_face(&previous) {
        return vec![previous];
    }
    previous.to_vec().unwrap_or_else(|_| vec![previous])
}

fn font_lock_value_items(value: &Value) -> Vec<Value> {
    match value.to_vec() {
        Ok(items) if !matches!(items.first(), Some(Value::Symbol(symbol)) if symbol.starts_with(':')) => {
            items
        }
        _ => vec![value.clone()],
    }
}

fn anonymous_font_lock_face(value: &Value) -> bool {
    let Ok(items) = value.to_vec() else {
        return false;
    };
    matches!(
        items.first(),
        Some(Value::Symbol(symbol))
            if symbol.starts_with(':')
                || matches!(symbol.as_str(), "foreground-color" | "background-color")
    )
}

fn remove_face_value(existing: Value, face: &Value) -> Value {
    match face_list_items(&existing) {
        Ok(items) => {
            let filtered = items
                .into_iter()
                .filter(|item| !values_equal_including_properties(item, face))
                .collect::<Vec<_>>();
            match filtered.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                _ => Value::list(filtered),
            }
        }
        Err(_) => {
            if values_equal_including_properties(&existing, face) {
                Value::Nil
            } else {
                existing
            }
        }
    }
}

fn face_list_items(value: &Value) -> Result<Vec<Value>, LispError> {
    if plist_like_face(value) {
        Err(LispError::TypeError("face-list".into(), "plist".into()))
    } else {
        value.to_vec()
    }
}

fn plist_like_face(value: &Value) -> bool {
    let Ok(items) = value.to_vec() else {
        return false;
    };
    !items.is_empty()
        && items.len().is_multiple_of(2)
        && items
            .iter()
            .step_by(2)
            .all(|item| matches!(item, Value::Symbol(symbol) if symbol.starts_with(':')))
}

fn parse_xml_region(xml: &str) -> Result<Value, roxmltree::Error> {
    let doc = Document::parse(xml)?;
    let children = xml_child_values(doc.root())?;
    if children.len() == 1 && matches!(children.first(), Some(Value::Cons(_, _))) {
        Ok(children.into_iter().next().unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            [Value::Symbol("top".into()), Value::Nil]
                .into_iter()
                .chain(children),
        ))
    }
}

fn xml_child_values(node: Node<'_, '_>) -> Result<Vec<Value>, roxmltree::Error> {
    let mut children = Vec::new();
    for child in node.children() {
        match child.node_type() {
            NodeType::Element => children.push(xml_element_value(child)?),
            NodeType::Comment => {
                children.push(Value::list([
                    Value::Symbol("comment".into()),
                    Value::Nil,
                    Value::String(child.text().unwrap_or_default().to_string()),
                ]));
            }
            NodeType::Text => {
                let text = child.text().unwrap_or_default();
                if !text.trim().is_empty() {
                    children.push(Value::String(text.to_string()));
                }
            }
            _ => {}
        }
    }
    Ok(children)
}

fn xml_element_value(node: Node<'_, '_>) -> Result<Value, roxmltree::Error> {
    let attrs = if node.attributes().len() == 0 {
        Value::Nil
    } else {
        Value::list(node.attributes().map(|attr| {
            Value::cons(
                Value::Symbol(attr.name().to_string()),
                Value::String(attr.value().to_string()),
            )
        }))
    };
    let children = xml_child_values(node)?;
    Ok(Value::list(
        [Value::Symbol(node.tag_name().name().to_string()), attrs]
            .into_iter()
            .chain(children),
    ))
}

fn display_property_value(value: &Value, property: &str) -> Option<Value> {
    if let Ok(items) = value.to_vec() {
        if let Some(Value::Symbol(name)) = items.first()
            && name == property
        {
            return items.get(1).cloned();
        }
        if matches!(items.first(), Some(Value::Symbol(name)) if name == "vector-literal")
            || matches!(items.first(), Some(Value::Symbol(name)) if name == "vector")
        {
            for item in items.iter().skip(1) {
                if let Some(found) = display_property_value(item, property) {
                    return Some(found);
                }
            }
            return None;
        }
        for item in items {
            if let Some(found) = display_property_value(&item, property) {
                return Some(found);
            }
        }
    }
    None
}

fn find_bidi_override(interp: &Interpreter, start: usize, end: usize) -> Option<usize> {
    let text = interp.buffer.buffer_substring(start, end).ok()?;
    let chars = text.chars().collect::<Vec<_>>();
    let control_index = chars
        .iter()
        .position(|ch| matches!(*ch as u32, 0x202A..=0x202E | 0x2066..=0x2069))?;
    chars[control_index..]
        .iter()
        .position(|ch| {
            !matches!((*ch) as u32, 0x202A..=0x202E | 0x2066..=0x2069)
                && !ch.is_whitespace()
                && !matches!(*ch, '{' | '}')
        })
        .map(|offset| start + control_index + offset)
}

pub(crate) fn insert_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut super::types::Env,
    inherit: bool,
    before_markers: bool,
) -> Result<Value, LispError> {
    let combined = combine_insert_args(args)?;
    let insert_at = interp.buffer.point();
    let nchars = combined.text.chars().count();
    insert_text_with_hooks(
        interp,
        &combined.text,
        &combined.props,
        inherit,
        before_markers,
        env,
    )?;
    if before_markers {
        for overlay in &mut interp.buffer.overlays {
            if overlay.is_dead() {
                continue;
            }
            if overlay.beg == insert_at {
                overlay.beg += nchars;
            }
            if overlay.end == insert_at {
                overlay.end += nchars;
            }
        }
    }
    Ok(Value::Nil)
}

pub(crate) fn insert_char_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut super::types::Env,
) -> Result<Value, LispError> {
    need_args("insert-char", args, 1)?;
    let ch = args[0].as_integer()?;
    let count = if args.len() > 1 {
        args[1].as_integer()?.max(0) as usize
    } else {
        1
    };
    if let Some(c) = char::from_u32(ch as u32) {
        let text: String = std::iter::repeat_n(c, count).collect();
        insert_text_with_hooks(interp, &text, &[], false, false, env)?;
    } else if (0..=0x3F_FFFF).contains(&ch) {
        let text: String = std::iter::repeat_n(RAW_CHAR_SENTINEL, count).collect();
        let props = vec![TextPropertySpan {
            start: 0,
            end: count,
            props: vec![("emaxx-raw-char".into(), Value::Integer(ch))],
        }];
        insert_text_with_hooks(interp, &text, &props, false, false, env)?;
    } else {
        return Err(LispError::Signal(format!("Invalid character: {}", ch)));
    }
    Ok(Value::Nil)
}

fn insert_text_with_hooks(
    interp: &mut Interpreter,
    text: &str,
    props: &[TextPropertySpan],
    inherit: bool,
    before_markers: bool,
    env: &mut super::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    ensure_no_supersession_threat(interp, env)?;
    let start = interp.buffer.point();
    let overlay_calls = overlay_insert_hook_calls(&interp.buffer, start, text.chars().count());
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
    run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(start as i64), Value::Integer(start as i64)],
        env,
    )?;
    if before_markers {
        if inherit {
            interp.insert_current_buffer_before_markers_and_inherit(text);
        } else {
            interp.insert_current_buffer_before_markers(text);
        }
    } else if inherit {
        interp.insert_current_buffer_and_inherit(text);
    } else {
        interp.insert_current_buffer(text);
    }
    for span in props {
        interp
            .buffer
            .add_text_properties(start + span.start, start + span.end, &span.props);
    }
    let end = start + text.chars().count();
    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(start as i64),
            Value::Integer(end as i64),
            Value::Integer(0),
        ],
        env,
    )?;
    let _ = maybe_lock_current_buffer(interp, env);
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
    Ok(())
}

fn combine_insert_args(args: &[Value]) -> Result<StringLike, LispError> {
    let mut text = String::new();
    let mut props = Vec::new();
    for arg in args {
        if let Some(string) = string_like(arg) {
            let offset = text.chars().count();
            text.push_str(&string.text);
            props.extend(shift_string_props(&string.props, offset));
        } else {
            let fragment = match arg {
                Value::Integer(n) => {
                    let offset = text.chars().count();
                    if let Some(c) = char::from_u32(*n as u32) {
                        c.to_string()
                    } else if (0..=0x3F_FFFF).contains(n) {
                        props.push(TextPropertySpan {
                            start: offset,
                            end: offset + 1,
                            props: vec![("emaxx-raw-char".into(), Value::Integer(*n))],
                        });
                        RAW_CHAR_SENTINEL.to_string()
                    } else {
                        String::new()
                    }
                }
                Value::Nil => String::new(),
                _ => arg.to_string(),
            };
            text.push_str(&fragment);
        }
    }
    Ok(StringLike {
        multibyte: text.chars().any(|ch| (ch as u32) > 0x7F),
        text,
        props: merge_string_props(props),
    })
}

#[derive(Clone, Copy)]
enum RoundingKind {
    Ceiling,
    Floor,
    Round,
    Truncate,
}

fn normalize_bigint_value(value: BigInt) -> Value {
    value
        .to_i64()
        .map(Value::Integer)
        .unwrap_or(Value::BigInteger(value))
}

fn version_leq(left: &str, right: &str) -> bool {
    let left = parse_version_components(left);
    let right = parse_version_components(right);
    let width = left.len().max(right.len());
    for index in 0..width {
        let a = *left.get(index).unwrap_or(&0);
        let b = *right.get(index).unwrap_or(&0);
        if a < b {
            return true;
        }
        if a > b {
            return false;
        }
    }
    true
}

fn parse_version_components(version: &str) -> Vec<i64> {
    version
        .split('.')
        .map(|segment| {
            let digits = segment
                .chars()
                .take_while(|ch| ch.is_ascii_digit())
                .collect::<String>();
            if digits.is_empty() {
                0
            } else {
                digits.parse::<i64>().unwrap_or(0)
            }
        })
        .collect()
}

fn integer_like_i64(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value {
        Value::Integer(n) => Ok(*n),
        Value::Marker(id) => interp
            .marker_position(*id)
            .map(|pos| pos as i64)
            .ok_or_else(|| LispError::TypeError("number-or-marker-p".into(), value.type_name())),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn integer_like_bigint(interp: &Interpreter, value: &Value) -> Result<BigInt, LispError> {
    match value {
        Value::Integer(n) => Ok(BigInt::from(*n)),
        Value::BigInteger(n) => Ok(n.clone()),
        Value::Marker(id) => interp
            .marker_position(*id)
            .map(BigInt::from)
            .ok_or_else(|| LispError::TypeError("number-or-marker-p".into(), value.type_name())),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn numeric_to_f64(interp: &Interpreter, value: &Value) -> Result<f64, LispError> {
    match value {
        Value::Float(f) => Ok(*f),
        Value::BigInteger(n) => n
            .to_f64()
            .ok_or_else(|| LispError::TypeError("number".into(), value.type_name())),
        _ => Ok(integer_like_i64(interp, value)? as f64),
    }
}

fn bigint_from_integral_float(value: f64) -> Option<BigInt> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    bigint_from_truncated_float(value).ok()
}

fn apply_rounding_kind(kind: RoundingKind, value: f64) -> Result<f64, LispError> {
    if !value.is_finite() {
        return Err(LispError::Signal("Floating-point overflow".into()));
    }
    Ok(match kind {
        RoundingKind::Ceiling => value.ceil(),
        RoundingKind::Floor => value.floor(),
        RoundingKind::Round => value.round_ties_even(),
        RoundingKind::Truncate => value.trunc(),
    })
}

fn integer_rounding_value(
    interp: &Interpreter,
    kind: RoundingKind,
    args: &[Value],
    float_result: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(
            match (kind, float_result) {
                (RoundingKind::Ceiling, false) => "ceiling",
                (RoundingKind::Floor, false) => "floor",
                (RoundingKind::Round, false) => "round",
                (RoundingKind::Truncate, false) => "truncate",
                (RoundingKind::Ceiling, true) => "fceiling",
                (RoundingKind::Floor, true) => "ffloor",
                (RoundingKind::Round, true) => "fround",
                (RoundingKind::Truncate, true) => "ftruncate",
            }
            .into(),
            args.len(),
        ));
    }
    if float_result && !matches!(args[0], Value::Float(_)) {
        return Err(LispError::TypeError("float".into(), args[0].type_name()));
    }

    if args.len() == 1 {
        if float_result {
            return Ok(Value::Float(apply_rounding_kind(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )?));
        }
        return match &args[0] {
            Value::Integer(_) | Value::BigInteger(_) => Ok(args[0].clone()),
            _ => rounded_f64_to_number_value(apply_rounding_kind(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )?),
        };
    }

    if let Some(rounded) = exact_numeric_division_round(interp, kind, &args[0], &args[1])? {
        return if float_result {
            Ok(Value::Float(numeric_to_f64(interp, &rounded)?))
        } else {
            Ok(rounded)
        };
    }

    if let (Some(numerator), Some(divisor)) = (
        integer_like_bigint_for_rounding(interp, &args[0]),
        integer_like_bigint_for_rounding(interp, &args[1]),
    ) {
        if divisor.is_zero() {
            return Err(LispError::Signal("Division by zero".into()));
        }
        let rounded = exact_integer_division_round(kind, numerator, divisor);
        return if float_result {
            Ok(Value::Float(
                numeric_to_f64(interp, &rounded).unwrap_or(f64::NAN),
            ))
        } else {
            Ok(rounded)
        };
    }

    let divisor = numeric_to_f64(interp, &args[1])?;
    if divisor == 0.0 || divisor.is_nan() {
        return Err(LispError::Signal("Division by zero".into()));
    }
    let quotient = numeric_to_f64(interp, &args[0])? / divisor;
    let rounded = apply_rounding_kind(kind, quotient)?;
    if float_result {
        Ok(Value::Float(rounded))
    } else {
        rounded_f64_to_number_value(rounded)
    }
}

fn rounded_f64_to_number_value(value: f64) -> Result<Value, LispError> {
    Ok(normalize_bigint_value(bigint_from_truncated_float(value)?))
}

fn integer_like_bigint_for_rounding(interp: &Interpreter, value: &Value) -> Option<BigInt> {
    match value {
        Value::Float(value) => bigint_from_integral_float(*value),
        _ => integer_like_bigint(interp, value).ok(),
    }
}

fn exact_numeric_division_round(
    interp: &Interpreter,
    kind: RoundingKind,
    numerator: &Value,
    divisor: &Value,
) -> Result<Option<Value>, LispError> {
    let Some((num_sig, num_exp)) = exact_binary_rational(interp, numerator)? else {
        return Ok(None);
    };
    let Some((div_sig, div_exp)) = exact_binary_rational(interp, divisor)? else {
        return Ok(None);
    };
    if div_sig.is_zero() {
        return Err(LispError::Signal("Division by zero".into()));
    }
    let mut scaled_num = num_sig;
    let mut scaled_div = div_sig;
    let exponent_delta = num_exp - div_exp;
    if exponent_delta >= 0 {
        scaled_num <<= exponent_delta as usize;
    } else {
        scaled_div <<= (-exponent_delta) as usize;
    }
    Ok(Some(exact_integer_division_round(
        kind, scaled_num, scaled_div,
    )))
}

fn exact_integer_division_round(kind: RoundingKind, numerator: BigInt, divisor: BigInt) -> Value {
    let quotient = &numerator / &divisor;
    let remainder = &numerator % &divisor;
    if remainder.is_zero() {
        return normalize_bigint_value(quotient);
    }
    let same_sign = numerator.sign() == divisor.sign();
    let adjusted = match kind {
        RoundingKind::Truncate => quotient,
        RoundingKind::Floor => {
            if same_sign {
                quotient
            } else {
                quotient - 1
            }
        }
        RoundingKind::Ceiling => {
            if same_sign {
                quotient + 1
            } else {
                quotient
            }
        }
        RoundingKind::Round => {
            let twice_remainder = remainder.abs() * 2;
            let divisor_abs = divisor.abs();
            if twice_remainder < divisor_abs {
                quotient
            } else if twice_remainder > divisor_abs {
                if same_sign {
                    quotient + 1
                } else {
                    quotient - 1
                }
            } else if (&quotient & BigInt::from(1u8)).is_zero() {
                quotient
            } else if same_sign {
                quotient + 1
            } else {
                quotient - 1
            }
        }
    };
    normalize_bigint_value(adjusted)
}

fn frexp_parts(value: f64) -> (f64, i64) {
    if value == 0.0 {
        return (value, 0);
    }
    let exponent = value.abs().log2().floor() as i64 + 1;
    let significand = value / ldexp_value(1.0, exponent);
    (significand, exponent)
}

fn ldexp_value(significand: f64, exponent: i64) -> f64 {
    if exponent > i32::MAX as i64 {
        return if significand == 0.0 {
            0.0
        } else if significand.is_sign_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if exponent < i32::MIN as i64 {
        return 0.0_f64.copysign(significand);
    }
    significand * 2.0_f64.powi(exponent as i32)
}

fn logb_value(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Integer(number) => {
            if *number == 0 {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            Ok(Value::Integer(
                i64::BITS as i64 - 1 - number.unsigned_abs().leading_zeros() as i64,
            ))
        }
        Value::BigInteger(number) => {
            if number.is_zero() {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            Ok(Value::Integer(
                number.abs().to_str_radix(2).len() as i64 - 1,
            ))
        }
        _ => {
            let value = numeric_to_f64(interp, value)?;
            if !value.is_finite() || value == 0.0 {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            let (_sig, exponent) = frexp_parts(value.abs());
            Ok(Value::Integer(exponent - 1))
        }
    }
}

fn expt_value(interp: &Interpreter, base: &Value, exponent: &Value) -> Result<Value, LispError> {
    let exponent_bigint = integer_like_bigint(interp, exponent);
    if matches!(base, Value::Float(_)) || matches!(exponent, Value::Float(_)) {
        return Ok(Value::Float(
            numeric_to_f64(interp, base)?.powf(numeric_to_f64(interp, exponent)?),
        ));
    }
    let exponent_bigint = exponent_bigint?;
    if exponent_bigint.is_negative() {
        let base_value = integer_like_i64(interp, base)? as f64;
        let exponent_value = exponent_bigint
            .to_f64()
            .ok_or_else(|| LispError::TypeError("number".into(), exponent.type_name()))?;
        return Ok(Value::Float(base_value.powf(exponent_value)));
    }

    let base_bigint = integer_like_bigint(interp, base)?;
    if exponent_bigint.is_zero() {
        return Ok(Value::Integer(1));
    }
    if base_bigint.is_zero() {
        return Ok(Value::Integer(0));
    }
    if base_bigint == BigInt::from(1) {
        return Ok(Value::Integer(1));
    }
    if base_bigint == BigInt::from(-1) {
        let even = (&exponent_bigint & BigInt::from(1u8)).is_zero();
        return Ok(Value::Integer(if even { 1 } else { -1 }));
    }
    let exponent_u32 = exponent_bigint
        .to_u32()
        .ok_or_else(|| LispError::Signal("Exponent too large".into()))?;
    Ok(normalize_bigint_value(base_bigint.pow(exponent_u32)))
}

fn exact_binary_rational(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<(BigInt, i32)>, LispError> {
    match value {
        Value::Float(value) => Ok(exact_float_binary_rational(*value)),
        Value::Integer(value) => Ok(Some((BigInt::from(*value), 0))),
        Value::BigInteger(value) => Ok(Some((value.clone(), 0))),
        Value::Marker(_) => Ok(Some((BigInt::from(integer_like_i64(interp, value)?), 0))),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn exact_float_binary_rational(value: f64) -> Option<(BigInt, i32)> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some((BigInt::zero(), 0));
    }
    let bits = value.to_bits();
    let negative = bits >> 63 != 0;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = bits & ((1u64 << 52) - 1);
    let (significand, exponent) = if exponent_bits == 0 {
        (mantissa, 1 - 1023 - 52)
    } else {
        ((1u64 << 52) | mantissa, exponent_bits - 1023 - 52)
    };
    let mut bigint = BigInt::from(significand);
    if negative {
        bigint = -bigint;
    }
    Some((bigint, exponent))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExactTimeValue {
    ticks: BigInt,
    hz: BigInt,
}

#[derive(Clone, Debug)]
struct ZoneSpec {
    offset_seconds: i32,
    abbreviation: String,
}

fn exact_time_value(ticks: BigInt, hz: BigInt) -> Result<ExactTimeValue, LispError> {
    if hz <= BigInt::zero() {
        return Err(LispError::Signal("Invalid time resolution".into()));
    }
    if ticks.is_zero() {
        return Ok(ExactTimeValue {
            ticks: BigInt::zero(),
            hz: BigInt::from(1u8),
        });
    }
    let divisor = bigint_gcd(ticks.abs(), hz.clone());
    Ok(ExactTimeValue {
        ticks: ticks / &divisor,
        hz: hz / divisor,
    })
}

fn bigint_gcd(mut left: BigInt, mut right: BigInt) -> BigInt {
    while !right.is_zero() {
        let remainder = left % &right;
        left = right;
        right = remainder;
    }
    left.abs()
}

fn current_time_value() -> Result<ExactTimeValue, LispError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let ticks = BigInt::from(now.as_secs()) * BigInt::from(1_000_000_000u64)
        + BigInt::from(now.subsec_nanos());
    exact_time_value(ticks, BigInt::from(1_000_000_000u64))
}

fn exact_time_from_float(value: f64) -> Result<ExactTimeValue, LispError> {
    let Some((significand, exponent)) = exact_float_binary_rational(value) else {
        return Err(LispError::TypeError("number".into(), "float".into()));
    };
    if exponent >= 0 {
        exact_time_value(significand << exponent as usize, BigInt::from(1u8))
    } else {
        exact_time_value(significand, BigInt::from(1u8) << (-exponent) as usize)
    }
}

fn exact_time_from_old_style(
    interp: &Interpreter,
    items: &[Value],
) -> Result<ExactTimeValue, LispError> {
    if !(2..=4).contains(&items.len()) {
        return Err(LispError::TypeError(
            "time-value".into(),
            Value::list(items.to_vec()).type_name(),
        ));
    }
    let high = integer_like_bigint(interp, &items[0])?;
    let low = integer_like_bigint(interp, &items[1])?;
    let micros = if items.len() >= 3 {
        integer_like_bigint(interp, &items[2])?
    } else {
        BigInt::zero()
    };
    let picos = if items.len() >= 4 {
        integer_like_bigint(interp, &items[3])?
    } else {
        BigInt::zero()
    };
    let ticks = (high * BigInt::from(65_536u32) + low) * BigInt::from(1_000_000_000_000u64)
        + micros * BigInt::from(1_000_000u32)
        + picos;
    exact_time_value(ticks, BigInt::from(1_000_000_000_000u64))
}

fn exact_time_from_value(
    interp: &Interpreter,
    value: &Value,
    now: &ExactTimeValue,
) -> Result<ExactTimeValue, LispError> {
    match value {
        Value::Nil => Ok(now.clone()),
        Value::Integer(value) => exact_time_value(BigInt::from(*value), BigInt::from(1u8)),
        Value::BigInteger(value) => exact_time_value(value.clone(), BigInt::from(1u8)),
        Value::Float(value) => exact_time_from_float(*value),
        Value::Cons(car, cdr) => {
            if let Ok(items) = value.to_vec()
                && (2..=4).contains(&items.len())
            {
                return exact_time_from_old_style(interp, &items);
            }
            exact_time_value(
                integer_like_bigint(interp, car)?,
                integer_like_bigint(interp, cdr)?,
            )
        }
        _ => Err(LispError::TypeError("time-value".into(), value.type_name())),
    }
}

fn floor_div_mod(value: &BigInt, divisor: &BigInt) -> (BigInt, BigInt) {
    let mut quotient = value / divisor;
    let mut remainder = value % divisor;
    if remainder.sign() == Sign::Minus {
        quotient -= 1;
        remainder += divisor;
    }
    (quotient, remainder)
}

fn time_floor_parts(time: &ExactTimeValue) -> (BigInt, BigInt) {
    floor_div_mod(&time.ticks, &time.hz)
}

fn exact_time_to_value(time: &ExactTimeValue) -> Value {
    if time.hz == BigInt::from(1u8) {
        normalize_bigint_value(time.ticks.clone())
    } else {
        Value::cons(
            normalize_bigint_value(time.ticks.clone()),
            normalize_bigint_value(time.hz.clone()),
        )
    }
}

fn exact_time_to_tick_pair(time: &ExactTimeValue) -> Value {
    Value::cons(
        normalize_bigint_value(time.ticks.clone()),
        normalize_bigint_value(time.hz.clone()),
    )
}

fn exact_time_floor_integer_value(time: &ExactTimeValue) -> Value {
    let (whole, _) = time_floor_parts(time);
    normalize_bigint_value(whole)
}

fn exact_time_to_scaled_pair(time: &ExactTimeValue, hz: &BigInt) -> Result<Value, LispError> {
    if hz <= &BigInt::zero() {
        return Err(LispError::Signal("Invalid time resolution".into()));
    }
    let scaled = &time.ticks * hz;
    let (ticks, remainder) = floor_div_mod(&scaled, &time.hz);
    if !remainder.is_zero() {
        return Err(LispError::Signal("Time conversion lost precision".into()));
    }
    Ok(Value::cons(
        normalize_bigint_value(ticks),
        normalize_bigint_value(hz.clone()),
    ))
}

fn exact_time_to_old_style(time: &ExactTimeValue) -> Result<Value, LispError> {
    let scaled = &time.ticks * BigInt::from(1_000_000_000_000u64);
    let (picoseconds, remainder) = floor_div_mod(&scaled, &time.hz);
    if !remainder.is_zero() {
        return Err(LispError::Signal("Time conversion lost precision".into()));
    }
    let (whole_seconds, fractional_picoseconds) =
        floor_div_mod(&picoseconds, &BigInt::from(1_000_000_000_000u64));
    let (high, low) = floor_div_mod(&whole_seconds, &BigInt::from(65_536u32));
    let (micros, picos) = floor_div_mod(&fractional_picoseconds, &BigInt::from(1_000_000u32));
    Ok(Value::list([
        normalize_bigint_value(high),
        normalize_bigint_value(low),
        normalize_bigint_value(micros),
        normalize_bigint_value(picos),
    ]))
}

fn power_of_two_exponent(value: &BigInt) -> Option<i32> {
    if value <= &BigInt::zero() {
        return None;
    }
    let mut exponent = 0i32;
    let mut current = value.clone();
    let two = BigInt::from(2u8);
    while (&current % &two).is_zero() {
        current /= &two;
        exponent += 1;
    }
    (current == BigInt::from(1u8)).then_some(exponent)
}

fn exact_time_to_f64(time: &ExactTimeValue) -> f64 {
    if let Some(exponent) = power_of_two_exponent(&time.hz)
        && let Some(ticks) = time.ticks.to_f64()
    {
        let mut value = ticks;
        let mut remaining = exponent;
        while remaining > 0 {
            let chunk = remaining.min(1022);
            value *= 2f64.powi(-chunk);
            remaining -= chunk;
        }
        return value;
    }
    let ticks = time.ticks.to_f64().unwrap_or_else(|| {
        if time.ticks.sign() == Sign::Minus {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    });
    let hz = time.hz.to_f64().unwrap_or(f64::INFINITY);
    ticks / hz
}

fn exact_time_equal(left: &ExactTimeValue, right: &ExactTimeValue) -> bool {
    left.ticks.clone() * &right.hz == right.ticks.clone() * &left.hz
}

fn exact_time_less(left: &ExactTimeValue, right: &ExactTimeValue) -> bool {
    left.ticks.clone() * &right.hz < right.ticks.clone() * &left.hz
}

fn local_zone_spec(time: Option<&ExactTimeValue>) -> ZoneSpec {
    let offset_seconds = time
        .and_then(|value| {
            let (whole_seconds, _) = time_floor_parts(value);
            whole_seconds.to_i64()
        })
        .and_then(|seconds| Local.timestamp_opt(seconds, 0).single())
        .map(|datetime| datetime.offset().local_minus_utc())
        .unwrap_or_else(|| Local::now().offset().local_minus_utc());
    ZoneSpec {
        offset_seconds,
        abbreviation: format_numeric_zone_name(offset_seconds),
    }
}

fn format_numeric_zone_name(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.abs();
    let hours = abs / 3600;
    let minutes = (abs % 3600) / 60;
    let seconds = abs % 60;
    if seconds != 0 {
        format!("{sign}{hours:02}{minutes:02}{seconds:02}")
    } else if minutes != 0 {
        format!("{sign}{hours:02}{minutes:02}")
    } else {
        format!("{sign}{hours:02}")
    }
}

fn parse_posix_zone_string(value: &str) -> Option<ZoneSpec> {
    let abbr_end = value
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(value.len());
    let abbreviation = value[..abbr_end].to_string();
    if abbreviation.is_empty() {
        return None;
    }
    let rest = &value[abbr_end..];
    if rest.is_empty() {
        return Some(ZoneSpec {
            offset_seconds: 0,
            abbreviation,
        });
    }
    let (sign, digits) = match rest.chars().next() {
        Some('+') | Some('-') => (rest.chars().next()?, &rest[1..]),
        Some(_) => ('+', rest),
        None => ('+', ""),
    };
    let mut parts = digits.split(':');
    let hours = parts.next()?.parse::<i32>().ok()?;
    let minutes = parts
        .next()
        .map_or(Some(0), |part| part.parse::<i32>().ok())?;
    let seconds = parts
        .next()
        .map_or(Some(0), |part| part.parse::<i32>().ok())?;
    if parts.next().is_some() {
        return None;
    }
    let magnitude = hours * 3600 + minutes * 60 + seconds;
    let offset_seconds = if sign == '-' { magnitude } else { -magnitude };
    Some(ZoneSpec {
        offset_seconds,
        abbreviation,
    })
}

fn zone_spec_from_value(
    zone: &Value,
    time: Option<&ExactTimeValue>,
) -> Result<ZoneSpec, LispError> {
    match zone {
        Value::Nil => Ok(local_zone_spec(time)),
        Value::T => Ok(ZoneSpec {
            offset_seconds: 0,
            abbreviation: "UTC".into(),
        }),
        Value::Integer(value) => Ok(ZoneSpec {
            offset_seconds: *value as i32,
            abbreviation: format_numeric_zone_name(*value as i32),
        }),
        Value::BigInteger(value) => {
            let offset = value
                .to_i32()
                .ok_or_else(|| LispError::TypeError("integer".into(), zone.type_name()))?;
            Ok(ZoneSpec {
                offset_seconds: offset,
                abbreviation: format_numeric_zone_name(offset),
            })
        }
        Value::String(value) => Ok(parse_posix_zone_string(value).unwrap_or(ZoneSpec {
            offset_seconds: 0,
            abbreviation: "UTC".into(),
        })),
        Value::Symbol(symbol) if symbol == "-" => Ok(local_zone_spec(time)),
        Value::Cons(_, _) => {
            let items = zone.to_vec()?;
            if items.is_empty() {
                return Ok(local_zone_spec(time));
            }
            let offset = match &items[0] {
                Value::Integer(value) => *value as i32,
                Value::BigInteger(value) => value
                    .to_i32()
                    .ok_or_else(|| LispError::TypeError("integer".into(), items[0].type_name()))?,
                _ => return Err(LispError::TypeError("integer".into(), items[0].type_name())),
            };
            let abbreviation = items
                .get(1)
                .and_then(|value| string_text(value).ok())
                .unwrap_or_else(|| format_numeric_zone_name(offset));
            Ok(ZoneSpec {
                offset_seconds: offset,
                abbreviation,
            })
        }
        _ => Err(LispError::TypeError("time-zone".into(), zone.type_name())),
    }
}

fn zone_offset(zone: &ZoneSpec) -> Result<FixedOffset, LispError> {
    FixedOffset::east_opt(zone.offset_seconds)
        .ok_or_else(|| LispError::Signal("Invalid time zone".into()))
}

fn time_local_datetime(
    time: &ExactTimeValue,
    zone: &ZoneSpec,
) -> Result<(chrono::DateTime<FixedOffset>, BigInt), LispError> {
    let (whole_seconds, fractional_ticks) = time_floor_parts(time);
    let whole_seconds = whole_seconds
        .to_i64()
        .ok_or_else(|| LispError::Signal("Time out of range".into()))?;
    let offset = zone_offset(zone)?;
    let utc = Utc
        .timestamp_opt(whole_seconds, 0)
        .single()
        .ok_or_else(|| LispError::Signal("Time out of range".into()))?;
    Ok((utc.with_timezone(&offset), fractional_ticks))
}

fn fraction_picoseconds(time: &ExactTimeValue) -> BigInt {
    let (_, fractional_ticks) = time_floor_parts(time);
    (&fractional_ticks * BigInt::from(1_000_000_000_000u64)) / &time.hz
}

fn format_fraction_digits(picoseconds: &BigInt, width: usize) -> String {
    let base = format!("{:012}", picoseconds.to_u64().unwrap_or(0));
    if width <= 12 {
        base[..width].to_string()
    } else {
        format!("{base}{}", "0".repeat(width - 12))
    }
}

fn trim_trailing_zeros(text: &str) -> String {
    let trimmed = text.trim_end_matches('0');
    if trimmed.is_empty() {
        "0".into()
    } else {
        trimmed.into()
    }
}

fn strip_leading_zeros(text: &str) -> String {
    let trimmed = text.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".into()
    } else {
        trimmed.into()
    }
}

fn parse_time_format_spec(
    spec: &[char],
    index: &mut usize,
) -> (bool, char, usize, Option<usize>, char) {
    let mut minimal = false;
    let mut pad = '0';
    let mut colons = 0usize;
    while *index < spec.len() {
        match spec[*index] {
            '-' => {
                minimal = true;
                *index += 1;
            }
            '_' => {
                pad = ' ';
                *index += 1;
            }
            ':' => {
                colons += 1;
                *index += 1;
            }
            _ => break,
        }
    }
    let width_start = *index;
    while *index < spec.len() && spec[*index].is_ascii_digit() {
        *index += 1;
    }
    let width = if *index > width_start {
        spec[width_start..*index]
            .iter()
            .collect::<String>()
            .parse::<usize>()
            .ok()
    } else {
        None
    };
    let conv = spec.get(*index).copied().unwrap_or('%');
    (minimal, pad, colons, width, conv)
}

fn format_zone_offset(
    offset_seconds: i32,
    colons: usize,
    minimal: bool,
    pad: char,
    width: Option<usize>,
) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.abs();
    let hours = abs / 3600;
    let minutes = (abs % 3600) / 60;
    let seconds = abs % 60;
    let canonical_digits = if seconds != 0 {
        format!("{hours:02}{minutes:02}{seconds:02}")
    } else {
        format!("{hours:02}{minutes:02}")
    };
    let minimal_digits = strip_leading_zeros(&canonical_digits);
    let mut rendered = if colons == 3 {
        let body = if seconds != 0 {
            format!("{hours}:{minutes:02}:{seconds:02}")
        } else if minutes != 0 {
            format!("{hours}:{minutes:02}")
        } else {
            hours.to_string()
        };
        format!("{sign}{body}")
    } else {
        let use_minimal =
            minimal || width.is_some_and(|target| target < canonical_digits.len() + 1);
        format!(
            "{sign}{}",
            if use_minimal {
                minimal_digits
            } else {
                canonical_digits
            }
        )
    };
    if let Some(target_width) = width
        && rendered.len() < target_width
    {
        let padding = target_width - rendered.len();
        rendered = if pad == ' ' {
            format!("{}{}", " ".repeat(padding), rendered)
        } else if colons == 0 {
            format!("{sign}{}{}", "0".repeat(padding), &rendered[1..])
        } else {
            format!("{}{}", "0".repeat(padding), rendered)
        };
    }
    rendered
}

fn format_time_string_value(
    interp: &Interpreter,
    format: &str,
    time: &ExactTimeValue,
    zone: &ZoneSpec,
) -> Result<String, LispError> {
    let _ = interp;
    let (datetime, _) = time_local_datetime(time, zone)?;
    let picoseconds = fraction_picoseconds(time);
    let chars = format.chars().collect::<Vec<_>>();
    let mut result = String::new();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] != '%' {
            result.push(chars[index]);
            index += 1;
            continue;
        }
        index += 1;
        if index >= chars.len() {
            break;
        }
        if chars[index] == '%' {
            result.push('%');
            index += 1;
            continue;
        }
        let (minimal, pad, colons, width, conv) = parse_time_format_spec(&chars, &mut index);
        let field = match conv {
            'Y' => datetime.year().to_string(),
            'm' => {
                let width = width.unwrap_or(2);
                let digits = datetime.month().to_string();
                if minimal {
                    strip_leading_zeros(&digits)
                } else if digits.len() >= width {
                    digits
                } else {
                    let fill = if pad == ' ' { ' ' } else { '0' };
                    format!(
                        "{}{}",
                        fill.to_string().repeat(width - digits.len()),
                        digits
                    )
                }
            }
            'd' => format!("{:02}", datetime.day()),
            'H' => format!("{:02}", datetime.hour()),
            'M' => format!("{:02}", datetime.minute()),
            'S' => format!("{:02}", datetime.second()),
            'Z' => zone.abbreviation.clone(),
            'z' => format_zone_offset(zone.offset_seconds, colons, minimal, pad, width),
            'N' => {
                let width = width.unwrap_or(9);
                let digits = format_fraction_digits(&picoseconds, width);
                if minimal {
                    trim_trailing_zeros(&digits)
                } else if pad == ' ' {
                    let trimmed = trim_trailing_zeros(&digits);
                    format!(
                        "{}{}",
                        trimmed,
                        " ".repeat(width.saturating_sub(trimmed.len()))
                    )
                } else {
                    digits
                }
            }
            other => {
                result.push('%');
                result.push(other);
                index += 1;
                continue;
            }
        };
        result.push_str(&field);
        index += 1;
    }
    Ok(result)
}

fn decode_time_value(
    time: &ExactTimeValue,
    zone: &ZoneSpec,
    form: &Value,
) -> Result<Value, LispError> {
    let (datetime, fractional_ticks) = time_local_datetime(time, zone)?;
    let seconds = exact_time_value(
        BigInt::from(datetime.second()) * time.hz.clone() + fractional_ticks,
        time.hz.clone(),
    )?;
    let second_field = match form {
        Value::Symbol(symbol) if symbol == "integer" => exact_time_floor_integer_value(&seconds),
        Value::T => exact_time_to_tick_pair(&seconds),
        _ => exact_time_to_value(&seconds),
    };
    Ok(Value::list([
        second_field,
        Value::Integer(datetime.minute() as i64),
        Value::Integer(datetime.hour() as i64),
        Value::Integer(datetime.day() as i64),
        Value::Integer(datetime.month() as i64),
        Value::Integer(datetime.year() as i64),
        Value::Integer(datetime.weekday().num_days_from_sunday() as i64),
        Value::Nil,
        Value::Integer(zone.offset_seconds as i64),
    ]))
}

fn decoded_seconds_value(interp: &Interpreter, value: &Value) -> Result<ExactTimeValue, LispError> {
    exact_time_from_value(
        interp,
        value,
        &ExactTimeValue {
            ticks: BigInt::zero(),
            hz: BigInt::from(1u8),
        },
    )
}

fn integer_field(interp: &Interpreter, value: &Value) -> Result<i32, LispError> {
    integer_like_bigint(interp, value)?
        .to_i32()
        .ok_or_else(|| LispError::TypeError("integer".into(), value.type_name()))
}

fn value_is_unspecified(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Nil) => true,
        Some(Value::Symbol(symbol)) if symbol == "-" => true,
        _ => false,
    }
}

fn time_convert_value(time: &ExactTimeValue, form: &Value) -> Result<Value, LispError> {
    match form {
        Value::Nil => Ok(exact_time_to_value(time)),
        Value::T => Ok(exact_time_to_tick_pair(time)),
        Value::Symbol(symbol) if symbol == "integer" => Ok(exact_time_floor_integer_value(time)),
        Value::Symbol(symbol) if symbol == "list" => exact_time_to_old_style(time),
        Value::Integer(value) if *value == 4 => exact_time_to_old_style(time),
        Value::BigInteger(value) if value == &BigInt::from(4u8) => exact_time_to_old_style(time),
        Value::Integer(value) => exact_time_to_scaled_pair(time, &BigInt::from(*value)),
        Value::BigInteger(value) => exact_time_to_scaled_pair(time, value),
        _ => Err(LispError::TypeError(
            "time-convert form".into(),
            form.type_name(),
        )),
    }
}

fn call_time_builtin(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    _env: &mut Env,
) -> Result<Value, LispError> {
    let now = current_time_value()?;
    match name {
        "time-add" | "time-subtract" => {
            need_args(name, args, 2)?;
            let left = exact_time_from_value(interp, &args[0], &now)?;
            let right = exact_time_from_value(interp, &args[1], &now)?;
            let ticks = if name == "time-add" {
                left.ticks.clone() * &right.hz + right.ticks.clone() * &left.hz
            } else {
                left.ticks.clone() * &right.hz - right.ticks.clone() * &left.hz
            };
            let hz = left.hz.clone() * right.hz.clone();
            Ok(exact_time_to_value(&exact_time_value(ticks, hz)?))
        }
        "time-equal-p" => {
            need_args(name, args, 2)?;
            let left = exact_time_from_value(interp, &args[0], &now)?;
            let right = exact_time_from_value(interp, &args[1], &now)?;
            Ok(if exact_time_equal(&left, &right) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "time-less-p" => {
            need_args(name, args, 2)?;
            let left = exact_time_from_value(interp, &args[0], &now)?;
            let right = exact_time_from_value(interp, &args[1], &now)?;
            Ok(if exact_time_less(&left, &right) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "float-time" => {
            need_arg_range(name, args, 0, 1)?;
            let value = args.first().unwrap_or(&Value::Nil);
            Ok(Value::Float(exact_time_to_f64(&exact_time_from_value(
                interp, value, &now,
            )?)))
        }
        "time-convert" => {
            need_arg_range(name, args, 1, 2)?;
            let time = exact_time_from_value(interp, &args[0], &now)?;
            let form = args.get(1).unwrap_or(&Value::Nil);
            time_convert_value(&time, form)
        }
        "decode-time" => {
            need_arg_range(name, args, 0, 3)?;
            let time = exact_time_from_value(interp, args.first().unwrap_or(&Value::Nil), &now)?;
            let zone = zone_spec_from_value(args.get(1).unwrap_or(&Value::Nil), Some(&time))?;
            let form = args.get(2).unwrap_or(&Value::Nil);
            decode_time_value(&time, &zone, form)
        }
        "encode-time" => {
            need_arg_range(name, args, 1, 9)?;
            let fields = if args.len() == 1 {
                args[0].to_vec()?
            } else {
                args.to_vec()
            };
            if fields.len() < 6 || fields.len() > 9 {
                return Err(LispError::WrongNumberOfArgs(name.into(), fields.len()));
            }
            let seconds = decoded_seconds_value(interp, &fields[0])?;
            let (whole_seconds, fractional_ticks) = time_floor_parts(&seconds);
            let second = whole_seconds
                .to_i64()
                .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
            if !(0..=59).contains(&second) {
                return Err(LispError::Signal("Invalid decoded time".into()));
            }
            let minute = integer_field(interp, &fields[1])?;
            let hour = integer_field(interp, &fields[2])?;
            let day = integer_field(interp, &fields[3])?;
            let month = integer_field(interp, &fields[4])?;
            let year = integer_field(interp, &fields[5])?;
            let zone = if value_is_unspecified(fields.get(8)) {
                local_zone_spec(None)
            } else {
                zone_spec_from_value(fields.get(8).unwrap_or(&Value::Nil), None)?
            };
            let offset = zone_offset(&zone)?;
            let date = chrono::NaiveDate::from_ymd_opt(year, month as u32, day as u32)
                .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
            let time = date
                .and_hms_opt(hour as u32, minute as u32, second as u32)
                .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
            let local = offset
                .from_local_datetime(&time)
                .single()
                .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
            Ok(exact_time_to_value(&exact_time_value(
                BigInt::from(local.timestamp()) * seconds.hz.clone() + fractional_ticks,
                seconds.hz,
            )?))
        }
        "format-time-string" => {
            need_arg_range(name, args, 1, 3)?;
            let format = string_text(&args[0])?;
            let time = exact_time_from_value(interp, args.get(1).unwrap_or(&Value::Nil), &now)?;
            let zone = zone_spec_from_value(args.get(2).unwrap_or(&Value::Nil), Some(&time))?;
            Ok(Value::String(format_time_string_value(
                interp, &format, &time, &zone,
            )?))
        }
        "current-time-zone" => {
            need_arg_range(name, args, 0, 1)?;
            let zone = if let Some(value) = args.first() {
                let time = exact_time_from_value(interp, value, &now)?;
                local_zone_spec(Some(&time))
            } else {
                local_zone_spec(None)
            };
            Ok(Value::list([
                Value::Integer(zone.offset_seconds as i64),
                Value::String(zone.abbreviation),
            ]))
        }
        _ => Err(LispError::Void(name.into())),
    }
}

fn numeric_lt(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? < numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? < integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? < integer_like_i64(interp, right)?)
}

fn numeric_eq(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? == numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? == integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? == integer_like_i64(interp, right)?)
}

fn numeric_gt(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? > numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? > integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? > integer_like_i64(interp, right)?)
}

fn numeric_lte(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? <= numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? <= integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? <= integer_like_i64(interp, right)?)
}

fn numeric_gte(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? >= numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? >= integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? >= integer_like_i64(interp, right)?)
}

fn parse_string_to_number_value(text: &str, base: Option<i64>) -> Result<Value, LispError> {
    match base.unwrap_or(10) {
        10 => Ok(parse_decimal_string_to_number(text)),
        base if (2..=16).contains(&base) => Ok(parse_integer_string_with_base(text, base as u32)),
        _ => Err(LispError::Signal("Args out of range".into())),
    }
}

fn parse_decimal_string_to_number(text: &str) -> Value {
    let text = text.trim_start_matches([' ', '\t']);
    let Some(prefix) = decimal_number_prefix(text) else {
        return Value::Integer(0);
    };
    if prefix.contains(['.', 'e', 'E']) {
        prefix
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or(Value::Integer(0))
    } else if let Ok(value) = prefix.parse::<i64>() {
        Value::Integer(value)
    } else if let Ok(value) = prefix.parse::<BigInt>() {
        normalize_bigint_value(value)
    } else {
        Value::Integer(0)
    }
}

fn decimal_number_prefix(text: &str) -> Option<&str> {
    let mut index = 0usize;
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    if let Some((_, sign)) = chars.get(index)
        && matches!(sign, '+' | '-')
    {
        index += 1;
    }

    let integer_start = index;
    while let Some((_, ch)) = chars.get(index)
        && ch.is_ascii_digit()
    {
        index += 1;
    }
    let integer_digits = index - integer_start;

    let mut fractional_digits = 0usize;
    if let Some((_, '.')) = chars.get(index) {
        index += 1;
        let fraction_start = index;
        while let Some((_, ch)) = chars.get(index)
            && ch.is_ascii_digit()
        {
            index += 1;
        }
        fractional_digits = index - fraction_start;
    }

    if integer_digits == 0 && fractional_digits == 0 {
        return None;
    }

    let mut end = index;
    if matches!(chars.get(index), Some((_, 'e' | 'E'))) {
        let exponent_marker = index;
        index += 1;
        if let Some((_, sign)) = chars.get(index)
            && matches!(sign, '+' | '-')
        {
            index += 1;
        }
        let exponent_start = index;
        while let Some((_, ch)) = chars.get(index)
            && ch.is_ascii_digit()
        {
            index += 1;
        }
        if index > exponent_start {
            end = index;
        } else {
            end = exponent_marker;
        }
    }

    Some(&text[..chars.get(end).map_or(text.len(), |(offset, _)| *offset)])
}

fn parse_integer_string_with_base(text: &str, base: u32) -> Value {
    let text = text.trim_start_matches([' ', '\t']);
    let mut chars = text.chars().peekable();
    let negative = match chars.peek() {
        Some('+') => {
            chars.next();
            false
        }
        Some('-') => {
            chars.next();
            true
        }
        _ => false,
    };

    let mut value = BigInt::zero();
    let mut saw_digit = false;
    while let Some(&ch) = chars.peek() {
        let Some(digit) = digit_value_for_base(ch, base) else {
            break;
        };
        saw_digit = true;
        value = value * base + BigInt::from(digit);
        chars.next();
    }

    if !saw_digit {
        return Value::Integer(0);
    }
    if negative {
        value = -value;
    }
    normalize_bigint_value(value)
}

fn digit_value_for_base(ch: char, base: u32) -> Option<u32> {
    let digit = match ch {
        '0'..='9' => ch as u32 - '0' as u32,
        'a'..='f' => 10 + (ch as u32 - 'a' as u32),
        'A'..='F' => 10 + (ch as u32 - 'A' as u32),
        _ => return None,
    };
    (digit < base).then_some(digit)
}

fn number_to_string(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Integer(n) => Ok(n.to_string()),
        Value::BigInteger(n) => Ok(n.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn format_source_props(value: &Value, from: usize, to: usize) -> Option<Vec<(String, Value)>> {
    let string = string_like(value)?;
    let mut props = Vec::new();
    for span in string.props {
        if span.start < to && from < span.end {
            for (name, value) in span.props {
                if !props.iter().any(|(existing, _)| existing == &name) {
                    props.push((name, value));
                }
            }
        }
    }
    if props.is_empty() { None } else { Some(props) }
}

fn props_at_string_offset(spans: &[TextPropertySpan], pos: usize) -> Vec<(String, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

fn file_modtime(path: &str) -> Result<Option<crate::buffer::FileModTime>, LispError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(crate::buffer::FileModTime {
            modified: metadata
                .modified()
                .map_err(|error| LispError::Signal(error.to_string()))?,
        })),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn system_time_seconds_value(time: SystemTime) -> Result<Value, LispError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    Ok(Value::Integer(duration.as_secs() as i64))
}

fn file_attribute_field(attributes: &Value, index: usize) -> Result<Value, LispError> {
    Ok(attributes
        .to_vec()?
        .get(index)
        .cloned()
        .unwrap_or(Value::Nil))
}

fn file_modtime_from_value(
    interp: &Interpreter,
    value: &Value,
) -> Result<crate::buffer::FileModTime, LispError> {
    let now = current_time_value()?;
    let exact = exact_time_from_value(interp, value, &now)?;
    let (whole_seconds, _) = time_floor_parts(&exact);
    let seconds = whole_seconds
        .to_i64()
        .ok_or_else(|| LispError::Signal("Time out of range".into()))?;
    let modified = if seconds >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds as u64))
            .ok_or_else(|| LispError::Signal("Time out of range".into()))?
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_secs(seconds.unsigned_abs()))
            .ok_or_else(|| LispError::Signal("Time out of range".into()))?
    };
    Ok(crate::buffer::FileModTime { modified })
}

fn lock_path_for_file(path: &str) -> PathBuf {
    let expanded = PathBuf::from(expand_file_name(path, None));
    let directory = expanded.parent().map(Path::to_path_buf).unwrap_or_default();
    let file_name = expanded
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(path);
    directory.join(format!(".#{file_name}"))
}

fn file_error_value(message: &str, path: &str) -> Value {
    Value::list([
        Value::Symbol("file-error".into()),
        Value::String(message.into()),
        Value::String(path.into()),
    ])
}

fn file_error_with_detail_value(message: &str, detail: &str, path: &str) -> Value {
    Value::list([
        Value::Symbol("file-error".into()),
        Value::String(message.into()),
        Value::String(detail.into()),
        Value::String(path.into()),
    ])
}

fn file_locked_p(path: &str) -> Result<Value, LispError> {
    let lock_path = lock_path_for_file(path);
    match fs::metadata(&lock_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                Err(LispError::SignalValue(file_error_value(
                    "Testing file lock",
                    path,
                )))
            } else {
                Ok(Value::T)
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn replace_matching_bindings(env: &mut Env, original: &Value, replacement: Value) {
    for frame in env.iter_mut().rev() {
        for (_, value) in frame.iter_mut().rev() {
            if *value == *original {
                *value = replacement.clone();
            }
        }
    }
}

fn is_circular_list_value(interp: &Interpreter, value: &Value) -> bool {
    match value {
        Value::Record(id) => interp
            .find_record(*id)
            .is_some_and(|record| record.type_name == "circular-list"),
        _ => false,
    }
}

fn make_temp_file_internal(
    prefix: &str,
    dir_flag: &Value,
    suffix: &str,
    text: Option<&Value>,
) -> Result<String, LispError> {
    let mut attempt = 0u64;
    loop {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LispError::Signal(error.to_string()))?
            .as_nanos();
        let path = format!("{prefix}{stamp:x}{attempt:x}{suffix}");
        let candidate = PathBuf::from(&path);
        if candidate.exists() {
            attempt = attempt.saturating_add(1);
            continue;
        }
        if dir_flag.is_nil() {
            let mut file = fs::File::create(&candidate)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            if let Some(text) = text.and_then(string_like) {
                file.write_all(text.text.as_bytes())
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
        } else if !matches!(dir_flag, Value::Integer(0)) {
            fs::create_dir(&candidate).map_err(|error| LispError::Signal(error.to_string()))?;
        }
        return Ok(path);
    }
}

fn maybe_prompt_supersession_threat(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let Some(current_modtime) = file_modtime(&path)? else {
        return Ok(());
    };
    if interp.buffer.visited_file_modtime() != Some(current_modtime) {
        let _ = call_named_function(
            interp,
            "ask-user-about-supersession-threat",
            &[Value::String(path)],
            env,
        )?;
    }
    Ok(())
}

fn decode_inserted_bytes(bytes: &[u8], multibyte: bool, literal: bool) -> String {
    if literal || !multibyte {
        return bytes.iter().map(|byte| char::from(*byte)).collect();
    }
    String::from_utf8(bytes.to_vec())
        .unwrap_or_else(|_| bytes.iter().map(|byte| char::from(*byte)).collect())
}

fn read_insert_file_bytes(
    path: &str,
    start: Option<usize>,
    end: Option<usize>,
) -> Result<Vec<u8>, LispError> {
    validate_file_name(path)?;
    let metadata = fs::metadata(path).map_err(|error| LispError::Signal(error.to_string()))?;
    if metadata.is_dir() {
        return Err(LispError::SignalValue(file_error_with_detail_value(
            "Read error",
            "Is a directory",
            path,
        )));
    }
    if metadata.file_type().is_file() {
        let mut bytes = fs::read(path).map_err(|error| LispError::Signal(error.to_string()))?;
        let start = start.unwrap_or(0).min(bytes.len());
        let end = end.unwrap_or(bytes.len()).clamp(start, bytes.len());
        bytes.truncate(end);
        bytes.drain(..start);
        return Ok(bytes);
    }
    if start.is_some() {
        return Err(LispError::Signal("Cannot seek in non-regular file".into()));
    }
    let limit = end.unwrap_or(8192);
    let mut file = fs::File::open(path).map_err(|error| LispError::Signal(error.to_string()))?;
    let mut buffer = vec![0; limit];
    let read = file
        .read(&mut buffer)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    buffer.truncate(read);
    Ok(buffer)
}

fn coding_tag_from_buffer_text(text: &str) -> Option<String> {
    static CODING_TAG: OnceLock<Regex> = OnceLock::new();
    let regex = CODING_TAG.get_or_init(|| {
        Regex::new(r"coding:\s*([[:alnum:]-]+)").expect("coding tag regex is valid")
    });
    text.lines()
        .take(2)
        .find_map(|line| regex.captures(line))
        .and_then(|captures| captures.get(1).map(|value| value.as_str().to_string()))
}

fn current_write_coding(
    interp: &Interpreter,
    env: &Env,
    text: &str,
    for_write_file: bool,
) -> Result<String, LispError> {
    if for_write_file && let Some(tag) = coding_tag_from_buffer_text(text) {
        let canonical = interp
            .coding_system_canonical_name(&tag)
            .ok_or_else(|| coding_system_error(tag.clone()))?;
        let base = interp
            .coding_system_base_name(&canonical)
            .unwrap_or(canonical.clone());
        let eol = interp.coding_system_eol_type_value(&canonical).or(Some(0));
        return Ok(coding_variant_name(interp, &base, eol));
    }
    if let Some(value) = interp.lookup_var("coding-system-for-write", env)
        && !value.is_nil()
    {
        return checked_coding_symbol(interp, &value);
    }
    if let Some(value) = interp.lookup_var("buffer-file-coding-system", env)
        && !value.is_nil()
    {
        let current = checked_coding_symbol(interp, &value)?;
        let base = interp
            .coding_system_base_name(&current)
            .unwrap_or(current.clone());
        let eol = interp.coding_system_eol_type_value(&current).or(Some(0));
        if for_write_file && base == "prefer-utf-8" && !ascii_only_text(text) {
            return Ok(coding_variant_name(interp, "utf-8", eol));
        }
        return Ok(coding_variant_name(interp, &base, eol));
    }
    if ascii_only_text(text) {
        Ok(coding_variant_name(interp, "prefer-utf-8", Some(0)))
    } else {
        Ok(coding_variant_name(interp, "utf-8", Some(0)))
    }
}

fn write_region_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let path = string_text(&args[2])?;
    validate_file_name(&path)?;
    let text = if args[0].is_nil() && args.get(1).is_none_or(Value::is_nil) {
        interp.buffer.buffer_string()
    } else if string_like(&args[0]).is_some() && args.get(1).is_none_or(Value::is_nil) {
        string_text(&args[0])?
    } else {
        let start = position_from_value(interp, &args[0])?;
        let end = position_from_value(interp, &args[1])?;
        interp
            .buffer
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?
    };
    let coding = current_write_coding(interp, env, &text, false)?;
    let bytes = encode_text_bytes(interp, &text, &coding)?;
    if args.get(3).is_some_and(Value::is_truthy) {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        file.write_all(&bytes)
            .map_err(|error| LispError::Signal(error.to_string()))?;
    } else {
        fs::write(&path, &bytes).map_err(|error| LispError::Signal(error.to_string()))?;
    }
    set_last_coding_system_used(interp, &coding, env);
    Ok(Value::String(path))
}

fn write_file_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut path = string_text(&args[0])?;
    if directory_name_p(&path) {
        let base = interp
            .buffer
            .file
            .as_deref()
            .map(file_name_nondirectory)
            .unwrap_or_else(|| file_name_nondirectory(&interp.buffer.name));
        path = file_name_concat(&[path, base]);
    }
    let text = interp.buffer.buffer_string();
    let coding = current_write_coding(interp, env, &text, true)?;
    let bytes = encode_text_bytes(interp, &text, &coding)?;
    fs::write(&path, &bytes).map_err(|error| LispError::Signal(error.to_string()))?;
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(path.clone());
    interp.buffer.set_unmodified();
    interp.set_buffer_local_value(
        interp.current_buffer_id(),
        "buffer-file-coding-system",
        Value::Symbol(coding.clone()),
    );
    set_last_coding_system_used(interp, &coding, env);
    Ok(Value::String(path))
}

fn decode_file_contents(
    interp: &Interpreter,
    env: &Env,
    bytes: &[u8],
    literal: bool,
) -> Result<(String, String), LispError> {
    if literal {
        return Ok((
            decode_inserted_bytes(bytes, interp.buffer.is_multibyte(), true),
            "no-conversion".into(),
        ));
    }
    let requested = interp
        .lookup_var("coding-system-for-read", env)
        .map(|value| checked_coding_name(interp, &value))
        .transpose()?
        .flatten();
    if let Some(requested) = requested {
        if requested == "undecided" {
            let (detected, normalized) = auto_detect_coding(interp, bytes);
            return Ok((decode_text_bytes(interp, &normalized, &detected)?, detected));
        }
        if interp.coding_system_kind_name(&requested).as_deref() == Some("utf-8-auto") {
            let actual_eol = detect_eol_type(bytes);
            let normalized = decode_bytes_with_explicit_eol(bytes, actual_eol);
            let (has_bom, bomless) = strip_utf8_bom(&normalized);
            let detected = coding_variant_name(
                interp,
                if has_bom {
                    "utf-8-with-signature"
                } else {
                    "utf-8"
                },
                Some(actual_eol),
            );
            return Ok((
                decode_utf8_bytes(if has_bom { bomless } else { &normalized }),
                detected,
            ));
        }
        let actual_eol = detect_eol_type(bytes);
        let explicit_eol = interp.coding_system_eol_type_value(&requested);
        let requested_base = interp
            .coding_system_base_name(&requested)
            .unwrap_or(requested.clone());
        if matches!(requested_base.as_str(), "unix" | "dos" | "mac") {
            let eol = explicit_eol.unwrap_or(0);
            let normalized = decode_bytes_with_explicit_eol(bytes, eol);
            let detected_base = if std::str::from_utf8(&normalized).is_ok() {
                let decoded = decode_utf8_bytes(&normalized);
                if ascii_only_text(&decoded) {
                    requested_base.clone()
                } else {
                    "utf-8".into()
                }
            } else if normalized.iter().any(|byte| *byte > 0x7F) {
                "raw-text".into()
            } else {
                requested_base.clone()
            };
            let detected = if matches!(detected_base.as_str(), "unix" | "dos" | "mac") {
                requested.clone()
            } else {
                coding_variant_name(interp, &detected_base, Some(eol))
            };
            let text = match detected_base.as_str() {
                "utf-8" => decode_utf8_bytes(&normalized),
                "raw-text" => decode_raw_text_bytes(&normalized),
                _ => normalized.iter().map(|byte| char::from(*byte)).collect(),
            };
            return Ok((text, detected));
        }
        let normalized = decode_bytes_with_explicit_eol(bytes, explicit_eol.unwrap_or(actual_eol));
        let detected = if explicit_eol.is_some() {
            requested.clone()
        } else {
            coding_variant_name(interp, &requested_base, Some(actual_eol))
        };
        if interp.coding_system_kind_name(&requested).as_deref() == Some("utf-8-with-signature") {
            let (_, bomless) = strip_utf8_bom(&normalized);
            return Ok((decode_utf8_bytes(bomless), detected));
        }
        return Ok((
            decode_text_bytes(interp, &normalized, &requested)?,
            detected,
        ));
    }
    let (detected, normalized) = auto_detect_coding(interp, bytes);
    Ok((decode_text_bytes(interp, &normalized, &detected)?, detected))
}

fn insert_file_contents(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
    literal: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 5 {
        return Err(LispError::WrongNumberOfArgs(
            if literal {
                "insert-file-contents-literally".into()
            } else {
                "insert-file-contents".into()
            },
            args.len(),
        ));
    }
    let path = string_text(&args[0])?;
    let start = args
        .get(2)
        .filter(|value| !value.is_nil())
        .map(|value| value.as_integer().map(|value| value.max(0) as usize))
        .transpose()?;
    let end = args
        .get(3)
        .filter(|value| !value.is_nil())
        .map(|value| value.as_integer().map(|value| value.max(0) as usize))
        .transpose()?;
    let replace = args.get(4).is_some_and(Value::is_truthy);
    if let Some(coding) = interp.lookup_var("coding-system-for-read", env)
        && !coding.is_nil()
    {
        let _ = checked_coding_symbol(interp, &coding)?;
    }
    let bytes = read_insert_file_bytes(&path, start, end)?;
    let (text, detected) = decode_file_contents(interp, env, &bytes, literal)?;
    if replace {
        maybe_prompt_supersession_threat(interp, env)?;
        let start = interp.buffer.point_min();
        let end = interp.buffer.point_max();
        interp.buffer.goto_char(start);
        interp
            .delete_region_current_buffer(start, end)
            .map_err(LispError::from)?;
        interp.buffer.goto_char(start);
    }
    if let Some(hooks) = interp.lookup_var("after-insert-file-functions", env)
        && is_circular_list_value(interp, &hooks)
    {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("circular-list".into()),
            Value::String("Circular list".into()),
        ])));
    }
    interp.insert_current_buffer(&text);
    interp.set_buffer_local_value(
        interp.current_buffer_id(),
        "buffer-file-coding-system",
        Value::Symbol(detected.clone()),
    );
    set_last_coding_system_used(interp, &detected, env);
    Ok(Value::list([
        Value::String(path),
        Value::Integer(text.chars().count() as i64),
    ]))
}

fn current_buffer_file(interp: &Interpreter) -> Option<&str> {
    interp
        .buffer
        .file_truename
        .as_deref()
        .or(interp.buffer.file.as_deref())
}

fn maybe_lock_current_buffer(interp: &mut Interpreter, env: &Env) -> Result<(), LispError> {
    if !interp
        .lookup_var("create-lockfiles", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    if !interp.buffer.is_modified() {
        return Ok(());
    }
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let lock_path = lock_path_for_file(&path);
    match fs::metadata(&lock_path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            fs::write(lock_path, format!("emaxx:{}", std::process::id()))
                .map_err(|err| LispError::Signal(err.to_string()))
        }
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn unlock_current_buffer(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(Value::Nil);
    };
    let lock_path = lock_path_for_file(&path);
    match fs::metadata(&lock_path) {
        Ok(metadata) if metadata.is_dir() => {
            call_named_function(
                interp,
                "userlock--handle-unlock-error",
                &[file_error_value("Unlocking file", &path)],
                env,
            )?;
            Ok(Value::Nil)
        }
        Ok(_) => {
            fs::remove_file(&lock_path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn ensure_no_supersession_threat(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let Some(current_modtime) = file_modtime(&path)? else {
        return Ok(());
    };
    if interp.buffer.visited_file_modtime() == Some(current_modtime) {
        return Ok(());
    }
    let disk_text =
        fs::read_to_string(&path).map_err(|error| LispError::Signal(error.to_string()))?;
    if disk_text == interp.buffer.saved_text() {
        interp
            .buffer
            .set_visited_file_modtime(Some(current_modtime));
        return Ok(());
    }
    if interp
        .lookup_var("noninteractive", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::Signal(
            "Cannot resolve conflict in batch mode".into(),
        ));
    }
    let prompt = format!(
        "{} changed on disk; really edit the buffer?",
        file_name_nondirectory(&path)
    );
    let answer = call_named_function(
        interp,
        "read-char-choice",
        &[
            Value::String(prompt),
            Value::list([
                Value::Integer('y' as i64),
                Value::Integer('n' as i64),
                Value::Integer('r' as i64),
            ]),
        ],
        env,
    )?;
    match answer.as_integer()? as u8 as char {
        'y' => {
            let _ = call(
                interp,
                "message",
                &[Value::String(
                    "File on disk now will become a backup file if you save these changes.".into(),
                )],
                env,
            )?;
            interp
                .buffer
                .set_visited_file_modtime(Some(current_modtime));
            Ok(())
        }
        'r' => {
            revert_current_buffer(interp)?;
            Err(LispError::SignalValue(Value::list([
                Value::Symbol("file-supersession".into()),
                Value::String("File reverted".into()),
                Value::String(path),
            ])))
        }
        _ => Err(LispError::SignalValue(Value::list([
            Value::Symbol("file-supersession".into()),
            Value::String("File changed on disk".into()),
            Value::String(path),
        ]))),
    }
}

fn revert_current_buffer(interp: &mut Interpreter) -> Result<(), LispError> {
    let Some(path) = interp.buffer.file.clone() else {
        return Ok(());
    };
    let text = fs::read_to_string(&path).map_err(|error| LispError::Signal(error.to_string()))?;
    let name = interp.buffer.name.clone();
    let file = interp.buffer.file.clone();
    let file_truename = interp.buffer.file_truename.clone();
    let inhibit_hooks = interp.buffer.inhibit_hooks;
    interp.buffer = crate::buffer::Buffer::from_text(&name, &text);
    interp.buffer.file = file;
    interp.buffer.file_truename = file_truename;
    interp.buffer.inhibit_hooks = inhibit_hooks;
    interp.buffer.set_unmodified();
    interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
    Ok(())
}

fn shell_quote_argument(argument: &str) -> String {
    format!("'{}'", argument.replace('\'', "'\"'\"'"))
}

fn first_choice_value(choices: &Value) -> Option<Value> {
    choices
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|item| {
            item.to_vec()
                .ok()
                .and_then(|nested| nested.first().cloned())
                .or(Some(item))
        })
}

const KEY_DESCRIPTION_ALT_BIT: i64 = 0x0400000;
const KEY_DESCRIPTION_SUPER_BIT: i64 = 0x0800000;
const KEY_DESCRIPTION_HYPER_BIT: i64 = 0x1000000;
const KEY_DESCRIPTION_SHIFT_BIT: i64 = 0x2000000;
const KEY_DESCRIPTION_CTRL_BIT: i64 = 0x4000000;
const KEY_DESCRIPTION_META_BIT: i64 = 0x8000000;
const KEY_DESCRIPTION_MODIFIER_MASK: i64 = KEY_DESCRIPTION_ALT_BIT
    | KEY_DESCRIPTION_SUPER_BIT
    | KEY_DESCRIPTION_HYPER_BIT
    | KEY_DESCRIPTION_SHIFT_BIT
    | KEY_DESCRIPTION_CTRL_BIT
    | KEY_DESCRIPTION_META_BIT;
const KEY_DESCRIPTION_META_PREFIX: i64 = 0x1B;

fn parse_kbd_sequence(text: &str) -> Result<Value, LispError> {
    let mut items = vec![Value::Symbol("vector".into())];
    for token in text.split_whitespace() {
        items.extend(parse_kbd_token(token));
    }
    Ok(Value::list(items))
}

fn parse_kbd_token(token: &str) -> Vec<Value> {
    if token.chars().count() == 1 {
        return token.chars().map(|ch| Value::Integer(ch as i64)).collect();
    }
    let (modifiers, rest, saw_prefix) = parse_kbd_prefixes(token);
    if saw_prefix {
        if rest.starts_with('<') && rest.ends_with('>') && rest.len() >= 2 {
            return vec![Value::Symbol(symbolic_kbd_event(
                modifiers,
                &rest[1..rest.len() - 1],
            ))];
        }
        if rest == "ESC" {
            return vec![Value::Symbol(symbolic_kbd_event(modifiers, "escape"))];
        }
        if let Some(code) = named_kbd_key_code(rest) {
            return vec![Value::Integer(code | modifiers)];
        }
        if rest.chars().count() == 1 {
            return rest
                .chars()
                .map(|ch| Value::Integer(ch as i64 | modifiers))
                .collect();
        }
        return vec![Value::Symbol(symbolic_kbd_event(modifiers, rest))];
    }
    if token.starts_with('<') && token.ends_with('>') && token.len() >= 2 {
        return vec![Value::Symbol(token[1..token.len() - 1].to_string())];
    }
    if token == "ESC" {
        return vec![Value::Symbol("escape".into())];
    }
    if let Some(code) = named_kbd_key_code(token) {
        return vec![Value::Integer(code)];
    }
    token.chars().map(|ch| Value::Integer(ch as i64)).collect()
}

fn parse_kbd_prefixes(token: &str) -> (i64, &str, bool) {
    let mut modifiers = 0;
    let mut rest = token;
    let mut saw_prefix = false;
    while rest.len() >= 3 && rest.as_bytes()[1] == b'-' {
        let prefix = rest.as_bytes()[0] as char;
        let bit = match prefix {
            'A' => KEY_DESCRIPTION_ALT_BIT,
            'C' => KEY_DESCRIPTION_CTRL_BIT,
            'H' => KEY_DESCRIPTION_HYPER_BIT,
            'M' => KEY_DESCRIPTION_META_BIT,
            'S' => KEY_DESCRIPTION_SHIFT_BIT,
            's' => KEY_DESCRIPTION_SUPER_BIT,
            _ => break,
        };
        modifiers |= bit;
        rest = &rest[2..];
        saw_prefix = true;
    }
    (modifiers, rest, saw_prefix)
}

fn named_kbd_key_code(token: &str) -> Option<i64> {
    match token {
        "RET" => Some('\r' as i64),
        "LFD" => Some('\n' as i64),
        "TAB" => Some('\t' as i64),
        "DEL" => Some(0x7F),
        "SPC" => Some(0x20),
        _ => None,
    }
}

fn symbolic_kbd_event(modifiers: i64, name: &str) -> String {
    let mut symbol = String::new();
    if modifiers & KEY_DESCRIPTION_ALT_BIT != 0 {
        symbol.push_str("A-");
    }
    if modifiers & KEY_DESCRIPTION_CTRL_BIT != 0 {
        symbol.push_str("C-");
    }
    if modifiers & KEY_DESCRIPTION_HYPER_BIT != 0 {
        symbol.push_str("H-");
    }
    if modifiers & KEY_DESCRIPTION_META_BIT != 0 {
        symbol.push_str("M-");
    }
    if modifiers & KEY_DESCRIPTION_SHIFT_BIT != 0 {
        symbol.push_str("S-");
    }
    if modifiers & KEY_DESCRIPTION_SUPER_BIT != 0 {
        symbol.push_str("s-");
    }
    symbol.push_str(name);
    symbol
}

fn key_sequence_binding_text(value: &Value) -> Result<String, LispError> {
    if string_like(value).is_some() {
        return string_text(value);
    }
    let mut parts = Vec::new();
    append_key_description_parts(value, &mut parts)?;
    Ok(parts.join(" "))
}

fn append_key_description_parts(
    sequence: &Value,
    output: &mut Vec<String>,
) -> Result<(), LispError> {
    let events = key_description_events(sequence)?;
    let mut add_meta = false;
    for event in events {
        if add_meta {
            match event {
                Value::Integer(code)
                    if code != KEY_DESCRIPTION_META_PREFIX
                        && code & KEY_DESCRIPTION_META_BIT == 0 =>
                {
                    output.push(describe_key_code(code | KEY_DESCRIPTION_META_BIT));
                }
                other => {
                    output.push(describe_key_code(KEY_DESCRIPTION_META_PREFIX));
                    if !matches!(other, Value::Integer(code) if code == KEY_DESCRIPTION_META_PREFIX)
                    {
                        output.push(single_key_description_text(&other, false)?);
                    }
                }
            }
            add_meta = false;
            continue;
        }

        if matches!(&event, Value::Integer(code) if *code == KEY_DESCRIPTION_META_PREFIX) {
            add_meta = true;
            continue;
        }

        output.push(single_key_description_text(&event, false)?);
    }

    if add_meta {
        output.push(describe_key_code(KEY_DESCRIPTION_META_PREFIX));
    }

    Ok(())
}

fn key_description_events(sequence: &Value) -> Result<Vec<Value>, LispError> {
    if let Some(string) = string_like(sequence) {
        let mut events = Vec::new();
        for ch in string.text.chars() {
            if !string.multibyte {
                if let Some(byte) = raw_byte_from_regex_char(ch) {
                    let code = if byte & 0x80 != 0 {
                        ((byte ^ 0x80) as i64) | KEY_DESCRIPTION_META_BIT
                    } else {
                        byte as i64
                    };
                    events.push(Value::Integer(code));
                    continue;
                }
                let code = ch as u32;
                if code <= 0xFF {
                    let byte = code as u8;
                    let normalized = if byte & 0x80 != 0 {
                        ((byte ^ 0x80) as i64) | KEY_DESCRIPTION_META_BIT
                    } else {
                        byte as i64
                    };
                    events.push(Value::Integer(normalized));
                    continue;
                }
            }
            events.push(Value::Integer(ch as i64));
        }
        return Ok(events);
    }

    match sequence {
        Value::Nil => Ok(Vec::new()),
        Value::Cons(_, _) => Ok(vector_items(sequence)?),
        Value::Integer(_) | Value::Symbol(_) => Ok(vec![sequence.clone()]),
        _ => Err(LispError::TypeError("array".into(), sequence.type_name())),
    }
}

fn sequence_values(sequence: &Value) -> Result<Vec<Value>, LispError> {
    if let Some(string) = sequence_string_like(sequence) {
        Ok(string_sequence_values(&string))
    } else {
        vector_items(sequence)
    }
}

fn string_sequence_values(string: &StringLike) -> Vec<Value> {
    string
        .text
        .chars()
        .map(|ch| string_sequence_value(string, ch))
        .collect()
}

fn string_sequence_value(string: &StringLike, ch: char) -> Value {
    let code = if !string.multibyte {
        raw_byte_from_regex_char(ch)
            .map(i64::from)
            .unwrap_or(ch as i64)
    } else {
        ch as i64
    };
    Value::Integer(code)
}

fn sequence_string_like(value: &Value) -> Option<StringLike> {
    match value {
        Value::String(_) | Value::StringObject(_) => string_like(value),
        Value::Cons(_, _) => {
            let items = value.to_vec().ok()?;
            if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector")
                && matches!(items.get(1), Some(Value::String(_)))
            {
                string_like(value)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn single_key_description_text(key: &Value, no_angles: bool) -> Result<String, LispError> {
    match key {
        Value::Integer(code) => Ok(describe_key_code(*code)),
        Value::Symbol(symbol) => Ok(describe_symbolic_key(symbol, no_angles)),
        Value::String(text) => Ok(text.clone()),
        Value::StringObject(state) => Ok(state.borrow().text.clone()),
        _ => Err(LispError::TypeError(
            "integer, symbol, or string".into(),
            key.type_name(),
        )),
    }
}

fn describe_symbolic_key(symbol: &str, no_angles: bool) -> String {
    if no_angles {
        return symbol.to_string();
    }

    let bytes = symbol.as_bytes();
    let mut prefix_len = 0usize;
    while prefix_len + 3 <= bytes.len()
        && bytes[prefix_len + 1] == b'-'
        && matches!(bytes[prefix_len], b'C' | b'M' | b'S' | b's' | b'H' | b'A')
    {
        prefix_len += 2;
    }

    format!("{}<{}>", &symbol[..prefix_len], &symbol[prefix_len..])
}

fn describe_key_code(code: i64) -> String {
    let mut text = String::new();
    let mut code = code & (KEY_DESCRIPTION_META_BIT | !-KEY_DESCRIPTION_META_BIT);
    let base = code & !KEY_DESCRIPTION_MODIFIER_MASK;
    let Some(_) = char::from_u32(base as u32) else {
        return format!("[{code}]");
    };

    let tab_as_ci = base == '\t' as i64 && code & KEY_DESCRIPTION_META_BIT != 0;

    if code & KEY_DESCRIPTION_ALT_BIT != 0 {
        text.push_str("A-");
        code &= !KEY_DESCRIPTION_ALT_BIT;
    }
    if code & KEY_DESCRIPTION_CTRL_BIT != 0
        || (base < ' ' as i64
            && base != KEY_DESCRIPTION_META_PREFIX
            && base != '\t' as i64
            && base != '\r' as i64)
        || tab_as_ci
    {
        text.push_str("C-");
        code &= !KEY_DESCRIPTION_CTRL_BIT;
    }
    if code & KEY_DESCRIPTION_HYPER_BIT != 0 {
        text.push_str("H-");
        code &= !KEY_DESCRIPTION_HYPER_BIT;
    }
    if code & KEY_DESCRIPTION_META_BIT != 0 {
        text.push_str("M-");
        code &= !KEY_DESCRIPTION_META_BIT;
    }
    if code & KEY_DESCRIPTION_SHIFT_BIT != 0 {
        text.push_str("S-");
        code &= !KEY_DESCRIPTION_SHIFT_BIT;
    }
    if code & KEY_DESCRIPTION_SUPER_BIT != 0 {
        text.push_str("s-");
        code &= !KEY_DESCRIPTION_SUPER_BIT;
    }

    match code {
        0x00..=0x1F => {
            if code == KEY_DESCRIPTION_META_PREFIX {
                text.push_str("ESC");
            } else if tab_as_ci {
                text.push('i');
            } else if code == '\t' as i64 {
                text.push_str("TAB");
            } else if code == '\r' as i64 {
                text.push_str("RET");
            } else if (1..=26).contains(&code) {
                text.push((code as u8 + b'`') as char);
            } else {
                text.push((code as u8 + b'@') as char);
            }
        }
        0x20 => text.push_str("SPC"),
        0x7F => text.push_str("DEL"),
        0x21..=0x7E => text.push(char::from_u32(code as u32).expect("ascii codepoint is valid")),
        _ => {
            if let Some(ch) = char::from_u32(code as u32) {
                text.push(ch);
            } else {
                return format!("[{code}]");
            }
        }
    }

    text
}

fn seq_subseq(sequence: &Value, start: i64, end: Option<i64>) -> Result<Value, LispError> {
    let start = start.max(0) as usize;
    match sequence {
        Value::String(text) => {
            let chars = text.chars().collect::<Vec<_>>();
            let end = end.unwrap_or(chars.len() as i64).max(start as i64) as usize;
            let bounded_end = end.min(chars.len());
            Ok(Value::String(
                chars[start.min(bounded_end)..bounded_end].iter().collect(),
            ))
        }
        _ => {
            let items = sequence.to_vec()?;
            let end = end.unwrap_or(items.len() as i64).max(start as i64) as usize;
            let bounded_end = end.min(items.len());
            Ok(Value::list(
                items[start.min(bounded_end)..bounded_end].iter().cloned(),
            ))
        }
    }
}

fn auto_save_path_for_buffer(buffer: &crate::buffer::Buffer) -> String {
    if let Some(path) = &buffer.file {
        format!("{path}#")
    } else {
        std::env::temp_dir()
            .join(format!("{}.autosave", buffer.name.replace('/', "_")))
            .display()
            .to_string()
    }
}

fn forward_line_bigint(buffer: &mut crate::buffer::Buffer, n: BigInt) -> BigInt {
    if n.is_zero() {
        let _ = buffer.forward_line(0);
        return BigInt::zero();
    }

    let max_isize = BigInt::from(isize::MAX);
    let min_isize = BigInt::from(isize::MIN);
    if n >= min_isize && n <= max_isize {
        let step = match n.to_isize() {
            Some(value) => value,
            None => return BigInt::zero(),
        };
        return BigInt::from(buffer.forward_line(step) as i64);
    }

    if n.sign() == Sign::Minus {
        let available = count_backward_line_moves(buffer);
        move_line_steps(buffer, available, false);
        n + BigInt::from(available)
    } else {
        let available = count_forward_line_moves(buffer);
        move_line_steps(buffer, available, true);
        n - BigInt::from(available)
    }
}

fn move_line_steps(buffer: &mut crate::buffer::Buffer, mut steps: usize, forward: bool) {
    while steps > 0 {
        let chunk = steps.min(isize::MAX as usize);
        let _ = buffer.forward_line(if forward {
            chunk as isize
        } else {
            -(chunk as isize)
        });
        steps -= chunk;
    }
}

fn count_forward_line_moves(buffer: &crate::buffer::Buffer) -> usize {
    let mut count = 0;
    let mut pos = buffer.point();
    while pos < buffer.point_max() {
        if buffer.char_at(pos) == Some('\n') {
            count += 1;
        }
        pos += 1;
    }
    count
}

fn count_backward_line_moves(buffer: &crate::buffer::Buffer) -> usize {
    let mut line_start = buffer.point();
    while line_start > buffer.point_min() {
        if buffer.char_at(line_start - 1) == Some('\n') {
            break;
        }
        line_start -= 1;
    }

    let mut count = 0;
    let mut pos = buffer.point_min();
    while pos < line_start {
        if buffer.char_at(pos) == Some('\n') {
            count += 1;
        }
        pos += 1;
    }
    count
}

fn render_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &super::types::Env,
) -> Result<String, LispError> {
    match value {
        Value::String(text) => Ok(format!("{:?}", text)),
        Value::StringObject(state) => {
            let (text, props) = {
                let state = state.borrow();
                (state.text.clone(), state.props.clone())
            };
            if props.is_empty() {
                return Ok(format!("{:?}", text));
            }
            let mut rendered = vec![format!("{:?}", text)];
            for span in props {
                rendered.push(span.start.to_string());
                rendered.push(span.end.to_string());
                rendered.push(render_prin1(interp, &plist_value(&span.props), env)?);
            }
            Ok(format!("#({})", rendered.join(" ")))
        }
        Value::Cons(_, _) => {
            let raw_items = value.to_vec()?;
            if matches!(
                raw_items.first(),
                Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
            ) {
                let items = vector_items(value)?;
                Ok(format!(
                    "[{}]",
                    items
                        .iter()
                        .map(|item| render_prin1(interp, item, env))
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" ")
                ))
            } else {
                Ok(value.to_string())
            }
        }
        Value::BuiltinFunc(_)
        | Value::Lambda(_, _, _)
        | Value::Buffer(_, _)
        | Value::Marker(_)
        | Value::Overlay(_)
        | Value::CharTable(_) => {
            if let Some(function) = interp.lookup_var("print-unreadable-function", env)
                && function.is_truthy()
            {
                let mut call_env = env.clone();
                let rendered = call_function_value(interp, &function, &[], &mut call_env)?;
                return string_text(&rendered);
            }
            Ok(value.to_string())
        }
        _ => Ok(value.to_string()),
    }
}

fn format_char_conversion(arg: &Value) -> Result<String, LispError> {
    let n = match arg {
        Value::Integer(n) => *n,
        Value::BigInteger(n) => n
            .to_i64()
            .ok_or_else(|| LispError::TypeError("character".into(), arg.type_name()))?,
        Value::Float(_) => {
            return Err(LispError::TypeError("integer".into(), "float".into()));
        }
        _ => {
            return Err(LispError::TypeError("integer".into(), arg.type_name()));
        }
    };
    char::from_u32(n as u32)
        .map(|c| c.to_string())
        .ok_or_else(|| LispError::Signal(format!("Invalid character: {}", n)))
}

fn format_s_conversion(
    arg: &Value,
    precision: Option<usize>,
) -> Result<(String, Vec<TextPropertySpan>), LispError> {
    if let Some(string) = string_like(arg) {
        let end = precision
            .unwrap_or_else(|| string.text.chars().count())
            .min(string.text.chars().count());
        let text = string.text.chars().take(end).collect::<String>();
        let props = slice_string_props(&string.props, 0, end);
        return Ok((text, props));
    }
    let mut text = number_to_string(arg).unwrap_or_else(|_| arg.to_string());
    if let Some(precision) = precision {
        text = text.chars().take(precision).collect();
    }
    Ok((text, Vec::new()))
}

fn bigint_from_truncated_float(value: f64) -> Result<BigInt, LispError> {
    if !value.is_finite() {
        return Err(LispError::TypeError("integer".into(), "float".into()));
    }
    let bits = value.to_bits();
    let sign = if bits >> 63 == 0 { 1 } else { -1 };
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = bits & ((1u64 << 52) - 1);
    if exponent == 0 || exponent < 1023 {
        return Ok(BigInt::zero());
    }
    let significand = (1u64 << 52) | mantissa;
    let shift = exponent - 1023 - 52;
    let mut result = BigInt::from(significand);
    if shift >= 0 {
        result <<= shift as usize;
    } else {
        result >>= (-shift) as usize;
    }
    if sign < 0 {
        result = -result;
    }
    Ok(result)
}

fn integer_for_format(
    interp: &Interpreter,
    value: &Value,
) -> Result<(Option<i64>, BigInt), LispError> {
    match value {
        Value::Integer(n) => Ok((Some(*n), BigInt::from(*n))),
        Value::BigInteger(n) => Ok((None, n.clone())),
        Value::Float(f) => Ok((None, bigint_from_truncated_float(*f)?)),
        Value::Marker(_) => {
            let n = integer_like_i64(interp, value)?;
            Ok((Some(n), BigInt::from(n)))
        }
        _ => Err(LispError::TypeError("integer".into(), value.type_name())),
    }
}

fn format_bigint_radix(value: &BigInt, radix: u32, upper: bool) -> String {
    let mut text = value.abs().to_str_radix(radix);
    if upper {
        text.make_ascii_uppercase();
    }
    text
}

fn apply_precision(mut digits: String, precision: Option<usize>) -> String {
    if let Some(precision) = precision
        && digits.len() < precision
    {
        digits = format!("{}{}", "0".repeat(precision - digits.len()), digits);
    }
    digits
}

fn format_numeric_conversion(
    interp: &Interpreter,
    arg: &Value,
    conv: char,
    flag_hash: bool,
    flag_plus: bool,
    flag_space: bool,
    precision: Option<usize>,
) -> Result<String, LispError> {
    let (_fixnum, bigint) = integer_for_format(interp, arg)?;
    let positive_sign = if flag_plus {
        "+"
    } else if flag_space {
        " "
    } else {
        ""
    };
    match conv {
        'd' => {
            let mut digits = apply_precision(bigint.abs().to_string(), precision);
            if bigint.sign() == Sign::Minus {
                digits.insert(0, '-');
            } else if !positive_sign.is_empty() {
                digits.insert_str(0, positive_sign);
            }
            Ok(digits)
        }
        'o' | 'x' | 'X' | 'b' | 'B' => {
            let radix = match conv.to_ascii_lowercase() {
                'o' => 8,
                'x' => 16,
                'b' => 2,
                _ => unreachable!(),
            };
            let upper = conv.is_ascii_uppercase();
            let digit_precision = if bigint.sign() == Sign::Minus {
                precision.map(|value| value.saturating_sub(1))
            } else {
                precision
            };
            let digits =
                apply_precision(format_bigint_radix(&bigint, radix, upper), digit_precision);
            let prefix = if flag_hash && !bigint.is_zero() {
                match conv {
                    'x' => "0x",
                    'X' => "0X",
                    'b' => "0b",
                    'B' => "0B",
                    _ => "0",
                }
            } else {
                ""
            };
            let sign = if bigint.sign() == Sign::Minus {
                "-"
            } else {
                positive_sign
            };
            Ok(format!("{}{}{}", sign, prefix, digits))
        }
        _ => Err(LispError::Signal(format!(
            "Invalid format operation %{}",
            conv
        ))),
    }
}

pub fn buffer_undo_list_value(buffer: &crate::buffer::Buffer) -> Value {
    let mut entries = buffer
        .undo_entries()
        .iter()
        .rev()
        .map(|entry| match entry {
            crate::buffer::UndoEntry::Insert { pos, len } => {
                Value::cons(Value::Integer(*pos as i64), Value::Integer(*len as i64))
            }
            crate::buffer::UndoEntry::Delete { pos, text, .. } => {
                Value::cons(Value::String(text.clone()), Value::Integer(*pos as i64))
            }
            crate::buffer::UndoEntry::Combined { display, .. }
            | crate::buffer::UndoEntry::Opaque(display) => display.clone(),
            crate::buffer::UndoEntry::Boundary => Value::Nil,
        })
        .collect::<Vec<_>>();
    entries.extend(buffer.undo_meta_entries().iter().rev().cloned());
    if buffer.file.is_some()
        && buffer.undo_entries().iter().any(|entry| {
            matches!(
                entry,
                crate::buffer::UndoEntry::Insert { .. } | crate::buffer::UndoEntry::Delete { .. }
            )
        })
    {
        entries.push(Value::list([
            Value::T,
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
        ]));
    }
    Value::list(entries)
}

pub(crate) fn values_equal(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    if let (Some(left_string), Some(right_string)) = (string_like(left), string_like(right)) {
        return left_string.text == right_string.text;
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left, Value::Cons(_, _))
        && matches!(right, Value::Cons(_, _))
    {
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| values_equal(interp, left, right));
    }
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            &BigInt::from(*a) == b
        }
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Buffer(a, _), Value::Buffer(b, _)) => a == b,
        (Value::Marker(a), Value::Marker(b)) => markers_equal(interp, *a, *b),
        (Value::Overlay(a), Value::Overlay(b)) => overlays_equal(interp, *a, *b),
        (Value::Cons(a_car, a_cdr), Value::Cons(b_car, b_cdr)) => {
            values_equal(interp, a_car, b_car) && values_equal(interp, a_cdr, b_cdr)
        }
        _ => false,
    }
}

fn values_equal_including_properties(left: &Value, right: &Value) -> bool {
    if let (Some(left_string), Some(right_string)) = (string_like(left), string_like(right)) {
        return left_string.text == right_string.text && left_string.props == right_string.props;
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left, Value::Cons(_, _))
        && matches!(right, Value::Cons(_, _))
    {
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| values_equal_including_properties(left, right));
    }
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            &BigInt::from(*a) == b
        }
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Cons(a_car, a_cdr), Value::Cons(b_car, b_cdr)) => {
            values_equal_including_properties(a_car, b_car)
                && values_equal_including_properties(a_cdr, b_cdr)
        }
        _ => left == right,
    }
}

fn markers_equal(interp: &Interpreter, left_id: u64, right_id: u64) -> bool {
    let Some(left) = interp.find_marker(left_id) else {
        return left_id == right_id;
    };
    let Some(right) = interp.find_marker(right_id) else {
        return false;
    };
    left.buffer_id == right.buffer_id && left.position == right.position
}

fn overlays_equal(interp: &Interpreter, left_id: u64, right_id: u64) -> bool {
    let Some(left) = interp.find_overlay(left_id) else {
        return left_id == right_id;
    };
    let Some(right) = interp.find_overlay(right_id) else {
        return false;
    };
    left.beg == right.beg
        && left.end == right.end
        && left.buffer_id == right.buffer_id
        && left.plist.len() == right.plist.len()
        && left.plist.iter().zip(&right.plist).all(
            |((left_key, left_value), (right_key, right_value))| {
                left_key == right_key && values_equal(interp, left_value, right_value)
            },
        )
}

fn resolve_callable(interp: &Interpreter, value: &Value, env: &Env) -> Result<Value, LispError> {
    match value {
        Value::Symbol(name) => interp.lookup_function(name, env),
        _ => Ok(value.clone()),
    }
}

fn literal_form(value: &Value) -> Value {
    match value {
        Value::Cons(_, _) | Value::Symbol(_) => {
            Value::list([Value::Symbol("quote".into()), value.clone()])
        }
        other => other.clone(),
    }
}

pub(crate) fn value_matches_with_test(
    interp: &mut Interpreter,
    left: &Value,
    right: &Value,
    testfn: Option<&Value>,
    env: &mut Env,
) -> Result<bool, LispError> {
    match testfn.filter(|value| !value.is_nil()) {
        None => Ok(left == right),
        Some(Value::Symbol(name)) | Some(Value::BuiltinFunc(name)) => match name.as_str() {
            "eq" | "eql" => Ok(left == right),
            "equal" => Ok(values_equal(interp, left, right)),
            _ => {
                let func = resolve_callable(interp, testfn.expect("checked Some"), env)?;
                Ok(
                    invoke_function_value(interp, &func, &[left.clone(), right.clone()], env)?
                        .is_truthy(),
                )
            }
        },
        Some(other) => {
            let func = resolve_callable(interp, other, env)?;
            Ok(
                invoke_function_value(interp, &func, &[left.clone(), right.clone()], env)?
                    .is_truthy(),
            )
        }
    }
}

fn invoke_function_value(
    interp: &mut Interpreter,
    func: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.call_function_value(func.clone(), None, args, env)
}

fn callable_name(original: &Value, resolved: &Value) -> Option<String> {
    match original {
        Value::Symbol(name) => Some(name.clone()),
        _ => match resolved {
            Value::BuiltinFunc(name) => Some(name.clone()),
            _ => None,
        },
    }
}

pub(crate) fn keymap_placeholder(name: Option<&str>) -> Value {
    let mut items = vec![Value::Symbol("keymap".into())];
    if let Some(name) = name {
        items.push(Value::String(name.into()));
    }
    Value::list(items)
}

const KEYMAP_RECORD_TYPE: &str = "keymap";
const KEYMAP_PARENT_SLOT: usize = 1;
const KEYMAP_BINDINGS_SLOT: usize = 2;

pub(crate) fn make_runtime_keymap(interp: &mut Interpreter, name: Option<&str>) -> Value {
    interp.create_record(
        KEYMAP_RECORD_TYPE,
        vec![
            name.map(Value::string).unwrap_or(Value::Nil),
            Value::Nil,
            Value::Nil,
        ],
    )
}

fn is_keymap_placeholder(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "keymap"),
    )
}

fn keymap_record_id(interp: &Interpreter, value: &Value) -> Option<u64> {
    let Value::Record(id) = value else {
        return None;
    };
    interp
        .find_record(*id)
        .filter(|record| record.type_name == KEYMAP_RECORD_TYPE)
        .map(|_| *id)
}

pub(crate) fn is_keymap_value(interp: &Interpreter, value: &Value) -> bool {
    is_keymap_placeholder(value) || keymap_record_id(interp, value).is_some()
}

fn keymap_bindings(record: &super::eval::RecordState) -> Result<Vec<(String, Value)>, LispError> {
    let bindings = record
        .slots
        .get(KEYMAP_BINDINGS_SLOT)
        .cloned()
        .unwrap_or(Value::Nil);
    let mut result = Vec::new();
    for entry in bindings.to_vec()? {
        let key = string_text(&entry.car()?)?;
        result.push((key, entry.cdr()?));
    }
    Ok(result)
}

fn keymap_bindings_value(bindings: Vec<(String, Value)>) -> Value {
    Value::list(
        bindings
            .into_iter()
            .map(|(key, value)| Value::cons(Value::String(key), value)),
    )
}

pub(crate) fn keymap_define_binding(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
    binding: Value,
) -> Result<(), LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(());
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Ok(());
    };
    let mut bindings = keymap_bindings(record)?;
    bindings.retain(|(existing, _)| existing != key);
    bindings.push((key.to_string(), binding));
    if record.slots.len() <= KEYMAP_BINDINGS_SLOT {
        record.slots.resize(KEYMAP_BINDINGS_SLOT + 1, Value::Nil);
    }
    record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
    Ok(())
}

pub(crate) fn keymap_remove_binding(
    interp: &mut Interpreter,
    keymap: &Value,
    key: &str,
) -> Result<(), LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(());
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Ok(());
    };
    let mut bindings = keymap_bindings(record)?;
    bindings.retain(|(existing, _)| existing != key);
    if record.slots.len() <= KEYMAP_BINDINGS_SLOT {
        record.slots.resize(KEYMAP_BINDINGS_SLOT + 1, Value::Nil);
    }
    record.slots[KEYMAP_BINDINGS_SLOT] = keymap_bindings_value(bindings);
    Ok(())
}

pub(crate) fn keymap_lookup_binding(
    interp: &Interpreter,
    keymap: &Value,
    key: &str,
) -> Result<Value, LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(Value::Nil);
    };
    let Some(record) = interp.find_record(id) else {
        return Ok(Value::Nil);
    };
    for (existing, binding) in keymap_bindings(record)?.into_iter().rev() {
        if existing == key {
            return Ok(binding);
        }
    }
    match record.slots.get(KEYMAP_PARENT_SLOT) {
        Some(Value::Nil) | None => Ok(Value::Nil),
        Some(parent) => keymap_lookup_binding(interp, parent, key),
    }
}

fn active_minor_mode_maps(interp: &Interpreter, env: &Env) -> Result<Vec<Value>, LispError> {
    let Some(alist) = interp.lookup_var("minor-mode-map-alist", env) else {
        return Ok(Vec::new());
    };
    let mut maps = Vec::new();
    for entry in alist.to_vec()? {
        let Value::Cons(mode, map) = entry else {
            continue;
        };
        let Value::Symbol(mode_name) = *mode else {
            continue;
        };
        if interp
            .lookup_var(&mode_name, env)
            .is_some_and(|value| value.is_truthy())
            && is_keymap_value(interp, &map)
        {
            maps.push(*map);
        }
    }
    Ok(maps)
}

fn default_global_binding_for_key(key: &str) -> Option<&'static str> {
    match key {
        "C-x 4 d" => Some("dired-other-window"),
        "C-x 5 d" => Some("dired-other-frame"),
        "C-x 5 C-o" => Some("display-buffer-other-frame"),
        _ => None,
    }
}

fn remap_key_binding_text(command: &str) -> String {
    format!("<remap> <{command}>")
}

fn key_binding(interp: &Interpreter, key: &str, env: &Env) -> Result<Value, LispError> {
    let maps = active_minor_mode_maps(interp, env)?;
    for map in &maps {
        let binding = keymap_lookup_binding(interp, map, key)?;
        if !binding.is_nil() {
            return Ok(binding);
        }
    }

    let Some(command) = default_global_binding_for_key(key) else {
        return Ok(Value::Nil);
    };
    let remap_key = remap_key_binding_text(command);
    for map in &maps {
        let binding = keymap_lookup_binding(interp, map, &remap_key)?;
        if !binding.is_nil() {
            return Ok(binding);
        }
    }

    Ok(Value::Symbol(command.into()))
}

pub(crate) fn autoload_parts(value: &Value) -> Option<(String, Value, Value)> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "autoload") {
        return None;
    }
    let file = string_like(items.get(1)?)
        .map(|string| string.text)
        .filter(|text| !text.is_empty())?;
    let interactive = items.get(3).cloned().unwrap_or(Value::Nil);
    let kind = items.get(4).cloned().unwrap_or(Value::Nil);
    Some((file, interactive, kind))
}

fn autoload_command_p(value: &Value) -> bool {
    autoload_parts(value).is_some_and(|(_, interactive, kind)| {
        interactive.is_truthy() || matches!(kind, Value::Symbol(symbol) if symbol == "keymap")
    })
}

fn collect_interactive_args(
    interp: &mut Interpreter,
    func: &Value,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let Some(spec) = interactive_spec_form(func) else {
        return Ok(Vec::new());
    };
    match spec {
        Value::String(spec) => parse_interactive_string(&spec, interp, env),
        Value::StringObject(state) => parse_interactive_string(&state.borrow().text, interp, env),
        _ => {
            if let Some(items) = interactive_list_form_items(&spec) {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(eval_callable_metadata_form(interp, func, &item, env)?);
                }
                Ok(values)
            } else {
                eval_callable_metadata_form(interp, func, &spec, env)?.to_vec()
            }
        }
    }
}

pub(crate) fn call_interactively_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs("call-interactively".into(), 0));
    }
    let mut func = resolve_callable(interp, &args[0], env)?;
    if let (Some(symbol), Some((file, _, _))) = (args[0].as_symbol().ok(), autoload_parts(&func)) {
        interp.load_target(&file)?;
        func = interp.lookup_function(symbol, env)?;
    }
    let interactive_args = collect_interactive_args(interp, &func, env)?;
    let result = invoke_function_value(interp, &func, &interactive_args, env)?;
    if args.get(1).is_some_and(Value::is_truthy)
        && let Some(function_name) = callable_name(&args[0], &func)
    {
        let history_args = history_args_for_call(interp, &func, &interactive_args, env)?;
        record_command_history(interp, &function_name, history_args, env);
    }
    Ok(result)
}

pub(crate) fn eval_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs("eval".into(), args.len()));
    }
    interp.eval(&args[0], env)
}

fn history_args_for_call(
    interp: &mut Interpreter,
    func: &Value,
    actual_args: &[Value],
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut recorded = actual_args.to_vec();
    let Value::Lambda(params, _, _) = func else {
        return Ok(recorded);
    };
    let positional_params = params
        .iter()
        .filter(|param| *param != "&optional" && *param != "&rest")
        .cloned()
        .collect::<Vec<_>>();
    for (name, form) in interactive_args_overrides(func) {
        if let Some(index) = positional_params.iter().position(|param| param == &name) {
            let value = eval_callable_metadata_form(interp, func, &form, env)?;
            if index >= recorded.len() {
                recorded.resize(index + 1, Value::Nil);
            }
            recorded[index] = value;
        }
    }
    Ok(recorded)
}

const OBARRAY_RECORD_TYPE: &str = "obarray";

#[derive(Clone)]
struct CompletionCandidate {
    name: String,
    predicate_args: Vec<Value>,
}

fn ensure_interaction_allowed(interp: &Interpreter, env: &Env) -> Result<(), LispError> {
    if interp
        .lookup_var("inhibit-interaction", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("inhibited-interaction".into()),
            Value::String("Interaction inhibited".into()),
        ])));
    }
    Ok(())
}

fn refresh_buffer_menu(
    interp: &mut Interpreter,
    files_only: bool,
    buffer_list: Option<&Value>,
    filter_predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let entries =
        collect_buffer_menu_entries(interp, files_only, buffer_list, filter_predicate, env)?;
    let rendered = entries
        .iter()
        .filter_map(|entry| match entry {
            Value::Buffer(id, _) => interp
                .get_buffer_by_id(*id)
                .map(|buffer| buffer.name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    let menu_buffer = match interp.find_buffer(BUFFER_MENU_BUFFER_NAME) {
        Some((id, name)) => Value::Buffer(id, name),
        None => {
            let (id, _) = interp.create_buffer(BUFFER_MENU_BUFFER_NAME);
            Value::Buffer(id, BUFFER_MENU_BUFFER_NAME.into())
        }
    };
    let menu_buffer_id = interp.resolve_buffer_id(&menu_buffer)?;
    {
        let buffer = interp
            .get_buffer_by_id_mut(menu_buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", menu_buffer_id)))?;
        let end = buffer.point_max();
        if end > 1 {
            buffer
                .delete_region(1, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
        }
        buffer.goto_char(1);
        buffer.insert(&rendered);
        buffer.goto_char(1);
        buffer.set_unmodified();
    }
    interp.set_buffer_local_value(
        menu_buffer_id,
        BUFFER_MENU_ENTRIES_VAR,
        Value::list(entries),
    );
    Ok(menu_buffer)
}

fn collect_buffer_menu_entries(
    interp: &mut Interpreter,
    files_only: bool,
    buffer_list: Option<&Value>,
    filter_predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let current = Value::Buffer(interp.current_buffer_id(), interp.buffer.name.clone());
    let candidates = match buffer_list {
        Some(value) if !value.is_nil() => resolve_buffer_menu_source(interp, value, env)?,
        _ => {
            let mut ordered = vec![current];
            for (id, name) in interp.buffer_list.clone() {
                if id != interp.current_buffer_id() {
                    ordered.push(Value::Buffer(id, name));
                }
            }
            ordered
        }
    };

    let mut entries = Vec::new();
    for candidate in candidates {
        let buffer_id = interp.resolve_buffer_id(&candidate)?;
        let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
            continue;
        };
        let name = buffer.name.clone();
        let file = buffer.file.clone();
        if name == BUFFER_MENU_BUFFER_NAME {
            continue;
        }
        if name.starts_with(' ') && file.is_none() {
            continue;
        }
        if files_only && file.is_none() {
            continue;
        }
        let buffer_value = Value::Buffer(buffer_id, name);
        if let Some(predicate) = filter_predicate.filter(|value| !value.is_nil()) {
            let keep = interp.call_function_value(
                predicate.clone(),
                None,
                std::slice::from_ref(&buffer_value),
                env,
            )?;
            if !keep.is_truthy() {
                continue;
            }
        }
        if entries
            .iter()
            .any(|entry| matches!(entry, Value::Buffer(id, _) if *id == buffer_id))
        {
            continue;
        }
        entries.push(buffer_value);
    }

    Ok(entries)
}

fn resolve_buffer_menu_source(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let source = match value {
        Value::BuiltinFunc(_) | Value::Lambda(_, _, _) => {
            interp.call_function_value(value.clone(), None, &[], env)?
        }
        Value::Symbol(symbol) if interp.lookup_function(symbol, env).is_ok() => {
            interp.call_function_value(value.clone(), None, &[], env)?
        }
        other => other.clone(),
    };
    source.to_vec()
}

fn is_window_value(interp: &Interpreter, value: &Value) -> bool {
    matches!(value, Value::Symbol(symbol) if symbol == "window")
        || matches!(value, Value::Record(id) if interp.find_record(*id).is_some_and(|record| record.type_name == "window"))
}

fn make_obarray(interp: &mut Interpreter) -> Value {
    interp.create_record(OBARRAY_RECORD_TYPE, vec![Value::Nil])
}

fn obarray_symbols(interp: &Interpreter, obarray: &Value) -> Result<Vec<Value>, LispError> {
    let Value::Record(id) = obarray else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    if record.type_name != OBARRAY_RECORD_TYPE {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    }
    record.slots.first().cloned().unwrap_or(Value::Nil).to_vec()
}

fn intern_in_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
    let Value::Record(id) = obarray else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    if record.type_name != OBARRAY_RECORD_TYPE {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    }
    let mut symbols = record
        .slots
        .first()
        .cloned()
        .unwrap_or(Value::Nil)
        .to_vec()?;
    if let Some(existing) = symbols
        .iter()
        .find(|value| matches!(value, Value::Symbol(name) if name == symbol_name))
        .cloned()
    {
        return Ok(existing);
    }
    let symbol = Value::Symbol(symbol_name.into());
    symbols.push(symbol.clone());
    if record.slots.is_empty() {
        record.slots.push(Value::list(symbols));
    } else {
        record.slots[0] = Value::list(symbols);
    }
    Ok(symbol)
}

fn intern_soft_in_obarray(
    interp: &Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
    Ok(obarray_symbols(interp, obarray)?
        .into_iter()
        .find(|value| matches!(value, Value::Symbol(name) if name == symbol_name))
        .unwrap_or(Value::Nil))
}

fn default_intern_soft_result(interp: &Interpreter, symbol_name: &str, env: &Env) -> Value {
    if matches!(symbol_name, "nil" | "t")
        || symbol_name.starts_with(':')
        || interp.lookup_var(symbol_name, env).is_some()
        || interp.lookup_function(symbol_name, env).is_ok()
        || is_builtin(symbol_name)
    {
        Value::Symbol(symbol_name.into())
    } else {
        Value::Nil
    }
}

fn completion_display_name(value: &Value) -> Result<String, LispError> {
    match value {
        Value::String(_) | Value::StringObject(_) => string_text(value),
        Value::Symbol(symbol) => Ok(symbol.clone()),
        _ => Err(LispError::TypeError(
            "string-or-symbol".into(),
            value.type_name(),
        )),
    }
}

fn completion_candidates(
    interp: &Interpreter,
    collection: &Value,
) -> Result<Vec<CompletionCandidate>, LispError> {
    if let Some((_, entries)) = json::hash_table_entries(interp, collection) {
        return entries
            .into_iter()
            .map(|(key, value)| {
                Ok(CompletionCandidate {
                    name: completion_display_name(&key)?,
                    predicate_args: vec![key, value],
                })
            })
            .collect();
    }
    match obarray_symbols(interp, collection) {
        Ok(symbols) => {
            return symbols
                .into_iter()
                .map(|symbol| {
                    Ok(CompletionCandidate {
                        name: completion_display_name(&symbol)?,
                        predicate_args: vec![symbol],
                    })
                })
                .collect();
        }
        Err(LispError::TypeError(expected, _)) if expected == "obarray" => {}
        Err(error) => return Err(error),
    }
    collection
        .to_vec()?
        .into_iter()
        .map(|item| {
            if matches!(item, Value::Cons(_, _)) {
                let key = item.car()?;
                Ok(CompletionCandidate {
                    name: completion_display_name(&key)?,
                    predicate_args: vec![item],
                })
            } else {
                Ok(CompletionCandidate {
                    name: completion_display_name(&item)?,
                    predicate_args: vec![item],
                })
            }
        })
        .collect()
}

fn completion_ignores_case(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("completion-ignore-case", env)
        .is_some_and(|value| value.is_truthy())
}

fn completion_matches_prefix(input: &str, candidate: &str, ignore_case: bool) -> bool {
    let input_chars: Vec<char> = input.chars().collect();
    let candidate_chars: Vec<char> = candidate.chars().collect();
    input_chars.len() <= candidate_chars.len()
        && input_chars
            .iter()
            .zip(candidate_chars.iter())
            .all(|(left, right)| {
                if ignore_case {
                    left.eq_ignore_ascii_case(right)
                } else {
                    left == right
                }
            })
}

fn completion_strings_equal(left: &str, right: &str, ignore_case: bool) -> bool {
    if ignore_case {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

fn completion_regex_matches(
    interp: &Interpreter,
    env: &Env,
    candidate: &str,
    pattern: &Value,
) -> Result<bool, LispError> {
    let pattern = string_like(pattern)
        .ok_or_else(|| LispError::TypeError("string".into(), pattern.type_name()))?;
    let regex = compile_elisp_regex(interp, &pattern, env, "")?;
    regex
        .is_match(candidate)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn completion_common_prefix(
    matches: &[CompletionCandidate],
    input: &str,
    ignore_case: bool,
) -> String {
    let match_chars = matches
        .iter()
        .map(|candidate| candidate.name.chars().collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let input_chars = input.chars().collect::<Vec<_>>();
    let mut prefix = String::new();
    let max_len = match_chars.iter().map(Vec::len).min().unwrap_or(0);

    for index in 0..max_len {
        let first = match_chars[0][index];
        let same_actual = match_chars.iter().all(|chars| chars[index] == first);
        let same_folded = match_chars.iter().all(|chars| {
            if ignore_case {
                chars[index].eq_ignore_ascii_case(&first)
            } else {
                chars[index] == first
            }
        });
        if !same_folded {
            break;
        }
        if !ignore_case || same_actual {
            prefix.push(first);
            continue;
        }
        if let Some(input_char) = input_chars
            .get(index)
            .copied()
            .filter(|input_char| input_char.eq_ignore_ascii_case(&first))
        {
            prefix.push(input_char);
        } else {
            prefix.push(first.to_ascii_lowercase());
        }
    }

    prefix
}

fn filtered_completion_matches(
    interp: &mut Interpreter,
    input: &str,
    collection: &Value,
    predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Vec<CompletionCandidate>, LispError> {
    let ignore_case = completion_ignores_case(interp, env);
    let regexp_list = interp
        .lookup_var("completion-regexp-list", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let predicate = predicate.filter(|value| !value.is_nil()).cloned();
    let mut matches = Vec::new();

    for candidate in completion_candidates(interp, collection)? {
        if !completion_matches_prefix(input, &candidate.name, ignore_case) {
            continue;
        }
        let mut regex_match = true;
        for pattern in &regexp_list {
            if !completion_regex_matches(interp, env, &candidate.name, pattern)? {
                regex_match = false;
                break;
            }
        }
        if !regex_match {
            continue;
        }
        if let Some(predicate) = &predicate {
            let predicate = resolve_callable(interp, predicate, env)?;
            if !invoke_function_value(interp, &predicate, &candidate.predicate_args, env)?
                .is_truthy()
            {
                continue;
            }
        }
        matches.push(candidate);
    }

    Ok(matches)
}

fn try_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "try-completion".into(),
            args.len(),
        ));
    }
    let input = string_text(&args[0])?;
    let matches = filtered_completion_matches(interp, &input, &args[1], args.get(2), env)?;
    if matches.is_empty() {
        return Ok(Value::Nil);
    }

    let ignore_case = completion_ignores_case(interp, env);
    if ignore_case {
        if let Some(candidate) = matches.iter().find(|candidate| candidate.name == input) {
            if matches.len() == 1 {
                return Ok(Value::T);
            }
            return Ok(Value::String(candidate.name.clone()));
        }
        if let Some(candidate) = matches
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&input))
        {
            return Ok(Value::String(candidate.name.clone()));
        }
    } else if matches.len() == 1 && matches[0].name == input {
        return Ok(Value::T);
    }

    Ok(Value::String(completion_common_prefix(
        &matches,
        &input,
        ignore_case,
    )))
}

fn all_completions(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "all-completions".into(),
            args.len(),
        ));
    }
    let input = string_text(&args[0])?;
    Ok(Value::list(
        filtered_completion_matches(interp, &input, &args[1], args.get(2), env)?
            .into_iter()
            .map(|candidate| Value::String(candidate.name)),
    ))
}

fn test_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "test-completion".into(),
            args.len(),
        ));
    }
    let input = string_text(&args[0])?;
    let ignore_case = completion_ignores_case(interp, env);
    let matches = filtered_completion_matches(interp, &input, &args[1], args.get(2), env)?;
    Ok(
        if matches
            .iter()
            .any(|candidate| completion_strings_equal(&candidate.name, &input, ignore_case))
        {
            Value::T
        } else {
            Value::Nil
        },
    )
}

fn list_contains_with(
    interp: &mut Interpreter,
    items: &[Value],
    needle: &Value,
    test: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    for item in items {
        if call_function_value(interp, test, &[needle.clone(), item.clone()], env)?.is_truthy() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn interactive_form_items(func: &Value) -> Option<Vec<Value>> {
    let Value::Lambda(_, body, _) = func else {
        return None;
    };
    for form in body {
        if matches!(form, Value::String(_) | Value::StringObject(_)) {
            continue;
        }
        if is_declare_form(form) {
            continue;
        }
        let Ok(items) = form.to_vec() else {
            break;
        };
        if matches!(items.first(), Some(Value::Symbol(name)) if name == "interactive") {
            return Some(items);
        }
        break;
    }
    None
}

fn interactive_spec_form(func: &Value) -> Option<Value> {
    interactive_form_items(func)
        .map(|items| items.get(1).cloned().unwrap_or(Value::Nil))
}

fn interactive_list_form_items(form: &Value) -> Option<Vec<Value>> {
    let items = form.to_vec().ok()?;
    matches!(items.first(), Some(Value::Symbol(name)) if name == "list")
        .then(|| items[1..].to_vec())
}

fn interactive_args_overrides(func: &Value) -> Vec<(String, Value)> {
    let Value::Lambda(_, body, _) = func else {
        return Vec::new();
    };
    let mut overrides = Vec::new();
    for form in body {
        if matches!(form, Value::String(_) | Value::StringObject(_)) {
            continue;
        }
        if !is_declare_form(form) {
            break;
        }
        let Ok(items) = form.to_vec() else {
            continue;
        };
        for decl in &items[1..] {
            let Ok(parts) = decl.to_vec() else {
                continue;
            };
            if !matches!(parts.first(), Some(Value::Symbol(name)) if name == "interactive-args") {
                continue;
            }
            for arg in &parts[1..] {
                let Ok(entry) = arg.to_vec() else {
                    continue;
                };
                if entry.len() >= 2
                    && let Value::Symbol(name) = &entry[0]
                {
                    overrides.push((name.clone(), entry[1].clone()));
                }
            }
        }
    }
    overrides
}

fn function_documentation(interp: &Interpreter, value: &Value, env: &Env) -> Option<Value> {
    let value = match value {
        Value::Symbol(symbol) => interp.lookup_function(symbol, env).ok()?,
        other => other.clone(),
    };
    let Value::Lambda(_, body, _) = value else {
        return None;
    };
    body.iter().find_map(|form| match form {
        Value::String(text) => Some(Value::String(text.clone())),
        Value::StringObject(state) => Some(Value::String(state.borrow().text.clone())),
        _ => None,
    })
}

fn is_vector_value(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(|items| {
        matches!(
            items.first(),
            Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
        )
    })
}

fn is_lambda_value(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "lambda"),
    )
}

fn validate_lambda_params(params: &Value) -> Result<(), LispError> {
    let items = params.to_vec()?;
    validate_lambda_list_items(params, &items)
}

fn validate_lambda_form(form: &Value) -> Result<(), LispError> {
    let items = form.to_vec()?;
    let Some(params) = items.get(1) else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("invalid-function".into()),
            form.clone(),
        ])));
    };
    validate_lambda_params(params)
}

fn validate_lambda_list_items(spec: &Value, items: &[Value]) -> Result<(), LispError> {
    let invalid = || {
        LispError::SignalValue(Value::list([
            Value::Symbol("invalid-function".into()),
            spec.clone(),
        ]))
    };
    let mut seen_optional = false;
    let mut seen_rest = false;
    let mut needs_rest_arg = false;
    let mut rest_arg_seen = false;

    for item in items {
        let Value::Symbol(symbol) = item else {
            return Err(invalid());
        };
        match symbol.as_str() {
            "&optional" => {
                if seen_optional || seen_rest {
                    return Err(invalid());
                }
                seen_optional = true;
            }
            "&rest" => {
                if seen_rest {
                    return Err(invalid());
                }
                seen_rest = true;
                needs_rest_arg = true;
            }
            _ => {
                if needs_rest_arg {
                    needs_rest_arg = false;
                    rest_arg_seen = true;
                } else if rest_arg_seen {
                    return Err(invalid());
                }
            }
        }
    }

    if needs_rest_arg {
        return Err(invalid());
    }

    Ok(())
}

fn parse_lambda_params_value(value: &Value) -> Result<Vec<String>, LispError> {
    let items = value.to_vec()?;
    validate_lambda_list_items(value, &items)?;
    items
        .into_iter()
        .map(|item| match item {
            Value::Symbol(symbol) => Ok(symbol),
            _ => Err(LispError::Signal("Invalid lambda parameter".into())),
        })
        .collect()
}

fn closure_env_from_alist(value: &Value) -> Result<Env, LispError> {
    let entries = value.to_vec()?;
    let mut frame = Vec::new();
    for entry in entries {
        match entry {
            Value::Cons(car, cdr) => {
                let name = car.as_symbol()?.to_string();
                let value = match *cdr {
                    Value::Cons(value, tail) if matches!(*tail, Value::Nil) => *value,
                    other => other,
                };
                frame.push((name, value));
            }
            _ => continue,
        }
    }
    Ok(if frame.is_empty() {
        Vec::new()
    } else {
        vec![frame]
    })
}

fn eval_callable_metadata_form(
    interp: &mut Interpreter,
    func: &Value,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if let Value::Lambda(_, _, closure_env) = func {
        let mut captured_frames = 0;
        for captured in closure_env.iter().rev() {
            env.insert(0, captured.clone());
            captured_frames += 1;
        }
        let result = interp.eval(form, env);
        for _ in 0..captured_frames {
            env.remove(0);
        }
        result
    } else {
        interp.eval(form, env)
    }
}

fn parse_interactive_string(
    spec: &str,
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut values = Vec::new();
    for line in spec.split('\n') {
        if line.is_empty() {
            continue;
        }
        let Some(code) = line.chars().next() else {
            continue;
        };
        match code {
            'k' => {
                let ch = pop_unread_command_event(interp, env)?;
                values.push(Value::String(ch.to_string()));
            }
            _ => return Err(invalid_interactive_control_letter(code)),
        }
    }
    Ok(values)
}

fn pop_unread_command_event(interp: &mut Interpreter, env: &mut Env) -> Result<char, LispError> {
    let unread = interp
        .lookup_var("unread-command-events", env)
        .unwrap_or(Value::Nil);
    let mut events = unread.to_vec()?;
    if events.is_empty() {
        return Err(LispError::Signal(
            "No unread-command-events available for interactive input".into(),
        ));
    }
    let event = events.remove(0);
    interp.set_variable("unread-command-events", Value::list(events), env);
    match event {
        Value::Integer(code) if code >= 0 => char::from_u32(code as u32)
            .ok_or_else(|| LispError::Signal(format!("Invalid unread command event {}", code))),
        Value::Cons(car, cdr) if matches!(*car, Value::T) => match *cdr {
            Value::Integer(code) if code >= 0 => char::from_u32(code as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid unread command event {}", code))),
            other => Err(LispError::Signal(format!(
                "Invalid unread command event {}",
                other
            ))),
        },
        Value::String(text) => text
            .chars()
            .next()
            .ok_or_else(|| LispError::Signal("Invalid unread command event".into())),
        _ => Err(LispError::Signal(format!(
            "Invalid unread command event {}",
            event
        ))),
    }
}

fn record_command_history(
    interp: &mut Interpreter,
    function_name: &str,
    args: Vec<Value>,
    env: &mut Env,
) {
    let mut history = interp
        .lookup_var("command-history", env)
        .unwrap_or(Value::Nil)
        .to_vec()
        .unwrap_or_default();
    let mut entry = vec![Value::Symbol(function_name.to_string())];
    entry.extend(args);
    history.insert(0, Value::list(entry));
    if let Some(Value::Integer(length)) = interp.lookup_var("history-length", env) {
        let length = length.max(0) as usize;
        history.truncate(length);
    }
    interp.set_variable("command-history", Value::list(history), env);
}

fn is_declare_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "declare"),
    )
}

fn invalid_interactive_control_letter(ch: char) -> LispError {
    let code = ch as u32;
    LispError::Signal(format!(
        "Invalid control letter `{ch}' (#o{code:03o}, #x{code:04x}) in interactive calling string"
    ))
}

fn need_args(name: &str, args: &[Value], n: usize) -> Result<(), LispError> {
    if args.len() < n {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

fn need_arg_range(name: &str, args: &[Value], min: usize, max: usize) -> Result<(), LispError> {
    if args.len() < min || args.len() > max {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

fn parse_edmacro_key_sequence(source: &str) -> Result<Value, LispError> {
    let mut parser = EdmacroKeyParser::new(source);
    let items = parser.parse()?;
    let mut vector = vec![Value::symbol("vector")];
    vector.extend(items);
    Ok(Value::list(vector))
}

struct EdmacroKeyParser<'a> {
    source: &'a str,
    pos: usize,
}

impl<'a> EdmacroKeyParser<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, pos: 0 }
    }

    fn parse(&mut self) -> Result<Vec<Value>, LispError> {
        let mut items = Vec::new();
        while self.pos < self.source.len() {
            self.skip_whitespace();
            if self.pos >= self.source.len() {
                break;
            }
            if self.starts_comment() {
                self.skip_comment();
                continue;
            }
            let repeat = self.parse_repeat_prefix()?;
            if self.starts_comment() {
                self.skip_comment();
                continue;
            }
            let token = self.read_token();
            if token.is_empty() {
                break;
            }
            let parsed = parse_edmacro_token(token)?;
            for _ in 0..repeat {
                items.extend(parsed.iter().cloned());
            }
        }
        Ok(items)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek_char() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn starts_comment(&self) -> bool {
        let rest = &self.source[self.pos..];
        if rest.starts_with(";;") {
            return true;
        }
        if !rest.starts_with("REM") {
            return false;
        }
        match rest.get(3..).and_then(|tail| tail.chars().next()) {
            None => true,
            Some(ch) => ch.is_whitespace(),
        }
    }

    fn skip_comment(&mut self) {
        while let Some(ch) = self.peek_char() {
            self.pos += ch.len_utf8();
            if ch == '\n' {
                break;
            }
        }
    }

    fn parse_repeat_prefix(&mut self) -> Result<usize, LispError> {
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if !ch.is_ascii_digit() {
                break;
            }
            self.pos += ch.len_utf8();
        }
        if self.pos == start || self.peek_char() != Some('*') {
            self.pos = start;
            return Ok(1);
        }
        let count = self.source[start..self.pos]
            .parse::<usize>()
            .map_err(|error| LispError::Signal(format!("Invalid repetition count: {error}")))?;
        self.pos += 1;
        Ok(count)
    }

    fn read_token(&mut self) -> &'a str {
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
        &self.source[start..self.pos]
    }

    fn peek_char(&self) -> Option<char> {
        self.source[self.pos..].chars().next()
    }
}

fn parse_edmacro_token(token: &str) -> Result<Vec<Value>, LispError> {
    if token.starts_with("<<") && token.ends_with(">>") && token.len() >= 4 {
        let command = &token[2..token.len() - 2];
        let mut items = vec![Value::Integer(apply_edmacro_modifiers(
            'x' as i64, false, true,
        ))];
        items.extend(command.chars().map(|ch| Value::Integer(ch as i64)));
        items.push(Value::Integer('\r' as i64));
        return Ok(items);
    }

    if let Some(key) = parse_modified_edmacro_key(token)? {
        return Ok(vec![Value::Integer(key)]);
    }

    if let Some(key) = parse_named_edmacro_key(token) {
        return Ok(vec![Value::Integer(key)]);
    }

    Ok(token.chars().map(|ch| Value::Integer(ch as i64)).collect())
}

fn parse_modified_edmacro_key(token: &str) -> Result<Option<i64>, LispError> {
    let mut ctrl = false;
    let mut meta = false;
    let mut shift = false;
    let mut super_key = false;
    let mut rest = token;

    loop {
        let Some((prefix, tail)) = rest.split_once('-') else {
            break;
        };
        match prefix {
            "C" => ctrl = true,
            "M" => meta = true,
            "S" => shift = true,
            "s" => super_key = true,
            _ => return Ok(None),
        }
        rest = tail;
    }

    if !(ctrl || meta || shift || super_key) {
        return Ok(None);
    }

    let base = if let Some(key) = parse_named_edmacro_key(rest) {
        key
    } else if rest.chars().count() == 1 {
        rest.chars().next().expect("count checked") as i64
    } else {
        return Ok(None);
    };

    let mut key = apply_edmacro_modifiers(base, ctrl, meta);
    if shift {
        key |= 1 << 25;
    }
    if super_key {
        key |= 1 << 23;
    }
    Ok(Some(key))
}

fn apply_edmacro_modifiers(mut value: i64, ctrl: bool, meta: bool) -> i64 {
    if ctrl && value != 0 {
        value = match value {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    }
    if meta {
        value |= 1 << 27;
    }
    value
}

fn parse_named_edmacro_key(token: &str) -> Option<i64> {
    match token {
        "NUL" => Some(0),
        "TAB" => Some('\t' as i64),
        "LFD" => Some('\n' as i64),
        "RET" => Some('\r' as i64),
        "ESC" => Some(0x1b),
        "SPC" => Some(' ' as i64),
        "DEL" => Some(0x7f),
        _ => None,
    }
}

fn is_composed_accessor_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() >= 3
        && bytes.first() == Some(&b'c')
        && bytes.last() == Some(&b'r')
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| matches!(byte, b'a' | b'd'))
}

fn call_composed_accessor(name: &str, args: &[Value]) -> Result<Value, LispError> {
    need_args(name, args, 1)?;
    let mut value = args[0].clone();
    for op in name[1..name.len() - 1].bytes().rev() {
        value = match op {
            b'a' => value.car()?,
            b'd' => value.cdr()?,
            _ => unreachable!("validated by is_composed_accessor_name"),
        };
    }
    Ok(value)
}

/// Simple pseudo-random number (xorshift64).
fn rand_simple() -> i64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);
    let mut s = STATE.load(Ordering::Relaxed);
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    STATE.store(s, Ordering::Relaxed);
    s as i64
}

#[cfg(test)]
mod lcms_response_tests {
    use super::*;

    #[test]
    fn edmacro_parser_handles_comments_commands_and_repetition() {
        assert_eq!(
            parse_edmacro_key_sequence("x REM ignored").expect("parse x with comment"),
            Value::list([Value::symbol("vector"), Value::Integer('x' as i64),])
        );
        assert_eq!(
            parse_edmacro_key_sequence("<<goto-line>>").expect("parse command shortcut"),
            Value::list([
                Value::symbol("vector"),
                Value::Integer((1 << 27) | ('x' as i64)),
                Value::Integer('g' as i64),
                Value::Integer('o' as i64),
                Value::Integer('t' as i64),
                Value::Integer('o' as i64),
                Value::Integer('-' as i64),
                Value::Integer('l' as i64),
                Value::Integer('i' as i64),
                Value::Integer('n' as i64),
                Value::Integer('e' as i64),
                Value::Integer('\r' as i64),
            ])
        );
        assert_eq!(
            parse_edmacro_key_sequence("3*C-m").expect("parse repeated control key"),
            Value::list([
                Value::symbol("vector"),
                Value::Integer('\r' as i64),
                Value::Integer('\r' as i64),
                Value::Integer('\r' as i64),
            ])
        );
    }
}
