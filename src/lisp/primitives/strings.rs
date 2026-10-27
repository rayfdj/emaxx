use super::*;
use crate::lisp::types::{Kind, PropertyKey, PropertyName};

/// alloc.c:memory_full signals with the existing Vmemory_signal_data. Rust
/// storage builders have no interpreter, so resolve their typed failure at
/// the primitive body, including generated direct and native dispatch.
pub(crate) fn string_storage_error(
    interp: &Interpreter,
    env: &Env,
    error: crate::lisp::types::string_data::StringError,
) -> LispError {
    use crate::lisp::types::string_data::StringError;
    match error {
        StringError::Condition(error) => error,
        StringError::AllocationFailed => LispError::SignalValue(
            interp
                .lookup_var("memory-signal-data", env)
                .unwrap_or(Value::Nil),
        ),
    }
}

/// fns.c:validate_subarray checks both index types before their joint range.
pub(crate) fn validate_subarray(
    array: Value,
    from: Value,
    to: Value,
    length: usize,
) -> Result<(usize, usize), LispError> {
    let index = |value: Value, default: i64| match value.kind() {
        Kind::Nil => Ok(default),
        Kind::Integer(index) => Ok(if index < 0 {
            length as i64 + index
        } else {
            index
        }),
        _ => Err(LispError::WrongTypeArgument("integerp".into(), value)),
    };
    let start = index(from, 0)?;
    let end = index(to, length as i64)?;
    if !(0 <= start && start <= end && end <= length as i64) {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            array,
            from,
            to,
        ])));
    }
    Ok((start as usize, end as usize))
}

pub(crate) fn substring_value(
    array: Value,
    from: Value,
    to: Value,
    properties: bool,
) -> Result<Value, LispError> {
    match array.kind() {
        Kind::Vector(vector) if properties => {
            let (from, to) = validate_subarray(array, from, to, vector.len())?;
            Ok(Value::vector(vector.slots().skip(from).take(to - from)))
        }
        Kind::StringObject(state) => {
            let result = {
                let state = state.borrow();
                let (from, to) = validate_subarray(array, from, to, state.len())?;
                state.substring(from, to, properties)
            };
            Ok(Value::StringObject(result))
        }
        _ => Err(LispError::WrongTypeArgument(
            if properties { "arrayp" } else { "stringp" }.into(),
            array,
        )),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StringLike {
    pub(crate) text: String,
    pub(crate) props: Vec<TextPropertySpan>,
    pub(crate) multibyte: bool,
    pub(crate) extended_chars: Vec<(usize, u32)>,
}

impl StringLike {
    pub(crate) fn character_codes(&self) -> Vec<i64> {
        self.character_codes_iter().collect()
    }

    pub(crate) fn character_codes_iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.text.chars().enumerate().map(|(index, ch)| {
            self.extended_chars
                .binary_search_by_key(&index, |(position, _)| *position)
                .ok()
                .map(|position| i64::from(self.extended_chars[position].1))
                .unwrap_or_else(|| string_character_code(self.multibyte, ch))
        })
    }

    pub(crate) fn byte_len(&self) -> Result<usize, LispError> {
        lisp_string_byte_len(&self.text, self.multibyte, &self.extended_chars)
    }
}

pub(crate) fn lisp_string_byte_len(
    text: &str,
    multibyte: bool,
    extended_chars: &[(usize, u32)],
) -> Result<usize, LispError> {
    if !multibyte {
        return Ok(encode_raw_text_bytes(text)?.len());
    }
    text.chars()
        .enumerate()
        .try_fold(0usize, |len, (index, ch)| {
            let code = extended_chars
                .binary_search_by_key(&index, |(position, _)| *position)
                .ok()
                .map(|position| extended_chars[position].1)
                .unwrap_or_else(|| string_character_code(true, ch) as u32);
            Ok(len + emacs_multibyte_char_len(code)?)
        })
}

/// Infallible storage census counterpart of SBYTES.  Emaxx can temporarily
/// represent an unibyte character above 255 as one Rust scalar while editing;
/// it still occupies one logical Emacs byte and must not make GC fallible.
pub(crate) fn lisp_string_storage_byte_len(
    text: &str,
    multibyte: bool,
    extended_chars: &[(usize, u32)],
) -> usize {
    if !multibyte {
        return text.chars().count();
    }
    if extended_chars.is_empty() {
        // A character takes as many bytes as its UTF-8 but for those of
        // U+E000..U+E0FF, read as raw-byte codes below: two bytes for
        // U+E080..U+E0FF, five for U+E000..U+E07F.  Their UTF-8 is three
        // bytes led by 0xEE, which leads no other character's bytes but
        // U+E100..U+EFFF's.
        let bytes = text.as_bytes();
        let mut len = bytes.len();
        for at in memchr::memchr_iter(0xEE, bytes) {
            match bytes.get(at + 1) {
                Some(0x80 | 0x81) => len += 2,
                Some(0x82 | 0x83) => len -= 1,
                _ => {}
            }
        }
        return len;
    }
    text.chars()
        .enumerate()
        .map(|(index, ch)| {
            let code = extended_chars
                .binary_search_by_key(&index, |(position, _)| *position)
                .ok()
                .map(|position| extended_chars[position].1)
                .unwrap_or_else(|| string_character_code(true, ch) as u32);
            emacs_multibyte_char_len(code).unwrap_or(1)
        })
        .sum()
}

pub(crate) fn emacs_multibyte_char_len(code: u32) -> Result<usize, LispError> {
    Ok(match code {
        0x000000..=0x00007F => 1,
        0x000080..=0x0007FF => 2,
        0x000800..=0x00FFFF => 3,
        0x010000..=0x1FFFFF => 4,
        0x200000..=0x3FFF7F => 5,
        0x3FFF80..=0x3FFFFF => 2,
        _ => return Err(LispError::Signal("Invalid character".into())),
    })
}

pub(crate) fn push_emacs_multibyte_char(output: &mut Vec<u8>, code: u32) -> Result<(), LispError> {
    match emacs_multibyte_char_len(code)? {
        1 => output.push(code as u8),
        2 if code <= 0x7FF => {
            output.push(0xC0 | (code >> 6) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        2 => {
            let payload = code - 0x3F_FF80;
            output.push(0xC0 | (payload >> 6) as u8);
            output.push(0x80 | (payload & 0x3F) as u8);
        }
        3 => {
            output.push(0xE0 | (code >> 12) as u8);
            output.push(0x80 | ((code >> 6) & 0x3F) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        4 => {
            output.push(0xF0 | (code >> 18) as u8);
            output.push(0x80 | ((code >> 12) & 0x3F) as u8);
            output.push(0x80 | ((code >> 6) & 0x3F) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        5 => {
            output.push(0xF8);
            output.push(0x80 | ((code >> 18) & 0x0F) as u8);
            output.push(0x80 | ((code >> 12) & 0x3F) as u8);
            output.push(0x80 | ((code >> 6) & 0x3F) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        _ => unreachable!("validated Emacs multibyte length"),
    }
    Ok(())
}

pub(crate) fn string_character_code(multibyte: bool, ch: char) -> i64 {
    if let Some(byte) = raw_byte_from_regex_char(ch) {
        if multibyte {
            RAW_BYTE8_BASE as i64 + i64::from(byte)
        } else {
            i64::from(byte)
        }
    } else {
        ch as i64
    }
}

/// The character codes of the current buffer's text [FROM, TO), as
/// FETCH_CHAR reads them (bytes in a unibyte buffer).
pub(crate) fn buffer_region_codes(interp: &Interpreter, from: usize, to: usize) -> Vec<u32> {
    let buffer = interp.buffer.borrow();
    StringLike {
        text: buffer.buffer_substring(from, to).unwrap_or_default(),
        props: Vec::new(),
        multibyte: buffer.is_multibyte(),
        extended_chars: buffer.substring_extended_chars(from, to),
    }
    .character_codes_iter()
    .map(|code| code as u32)
    .collect()
}

/// A text view and side table holding CODES, multibyte or (each code a
/// byte) unibyte.
pub(crate) fn text_from_codes(codes: &[u32], multibyte: bool) -> (String, Vec<(usize, u32)>) {
    let mut text = String::with_capacity(codes.len());
    let mut extended = Vec::new();
    for (index, &code) in codes.iter().enumerate() {
        if multibyte {
            push_character_code(&mut text, &mut extended, index, code);
        } else if code < 0x80 {
            text.push(char::from(code as u8));
        } else {
            text.push(raw_byte_regex_char(code as u8));
        }
    }
    (text, extended)
}

/// alloc.c make_specified_string with NCHARS -1: the internal BYTES as a
/// string of multibyteness MULTIBYTE, its characters counted from them.
pub(crate) fn make_specified_string(
    bytes: Vec<u8>,
    multibyte: bool,
) -> Result<Value, crate::lisp::types::string_data::StringError> {
    let characters = if multibyte {
        let mut offset = 0;
        let mut count = 0;
        while offset < bytes.len() {
            offset +=
                crate::lisp::types::string_data::multibyte_length(&bytes[offset..], true).max(1);
            count += 1;
        }
        count
    } else {
        bytes.len()
    };
    StringObjectRef::from_storage(bytes, characters, multibyte).map(Value::StringObject)
}

/// alloc.c make_string (build_string): multibyte when BYTES hold a
/// non-ASCII character and parse_str_as_multibyte finds every byte part of
/// a valid multibyte form (eight-bit forms not counted), else unibyte.
pub(crate) fn make_string(
    bytes: Vec<u8>,
) -> Result<Value, crate::lisp::types::string_data::StringError> {
    let mut offset = 0;
    let mut characters = 0;
    let mut multibyte_bytes = 0;
    while offset < bytes.len() {
        match crate::lisp::types::string_data::multibyte_length(&bytes[offset..], false) {
            0 => {
                offset += 1;
                multibyte_bytes += 2;
            }
            length => {
                offset += length;
                multibyte_bytes += length;
            }
        }
        characters += 1;
    }
    let multibyte = characters != bytes.len() && multibyte_bytes == bytes.len();
    make_specified_string(bytes, multibyte)
}

/// Append the character CODE to a Rust text view at character INDEX, as
/// string_data's decode_bytes builds one: a raw byte as its marker, and a
/// character the view cannot carry (beyond Unicode, a surrogate, a real
/// U+E000..U+E0FF) as its placeholder with an entry in EXTENDED.
pub(crate) fn push_character_code(
    text: &mut String,
    extended: &mut Vec<(usize, u32)>,
    index: usize,
    code: u32,
) {
    if (RAW_BYTE8_BASE + 0x80..=RAW_BYTE8_BASE + 0xFF).contains(&code) {
        text.push(raw_byte_regex_char((code - RAW_BYTE8_BASE) as u8));
    } else if let Some(ch) = char::from_u32(code) {
        text.push(ch);
        if raw_byte_from_regex_char(ch).is_some() {
            extended.push((index, code));
        }
    } else {
        text.push(crate::lisp::json::INVALID_UNICODE_SENTINEL);
        extended.push((index, code));
    }
}

/// Append character CODE as a buffer holds it: in a multibyte buffer as
/// push_character_code does, in a unibyte one as the byte CHAR_TO_BYTE8
/// makes of it (a raw byte's own, else the code's low byte).
pub(crate) fn push_buffer_character(
    text: &mut String,
    extended: &mut Vec<(usize, u32)>,
    index: usize,
    code: u32,
    multibyte: bool,
) {
    if multibyte {
        push_character_code(text, extended, index, code);
    } else {
        let byte = if (RAW_BYTE8_BASE + 0x80..=RAW_BYTE8_BASE + 0xFF).contains(&code) {
            code - RAW_BYTE8_BASE
        } else {
            code & 0xFF
        };
        text.push(raw_byte_regex_char(byte as u8));
    }
}

/// The side table of a string's characters its Rust text view cannot
/// carry, read in place: empty for a unibyte or ASCII string and for
/// UTF-8 without a real U+E000..U+E0FF.
pub(crate) fn string_extended_chars(value: &Value) -> Vec<(usize, u32)> {
    let Kind::StringObject(state) = value.kind() else {
        return Vec::new();
    };
    let state = state.borrow();
    let bytes = state.bytes();
    if !state.is_multibyte() || bytes.is_ascii() {
        return Vec::new();
    }
    if std::str::from_utf8(bytes).is_ok()
        && !memchr::memchr_iter(0xEE, bytes)
            .any(|at| matches!(bytes.get(at + 1), Some(0x80..=0x83)))
    {
        return Vec::new();
    }
    state.text_parts().1
}

pub(crate) fn string_like(value: &Value) -> Option<StringLike> {
    match value.kind() {
        Kind::StringObject(state) => {
            let state = state.borrow();
            let (text, extended_chars) = state.text_parts();
            Some(StringLike {
                text,
                props: state
                    .props
                    .iter()
                    .map(|span| TextPropertySpan {
                        start: span.start,
                        end: span.end,
                        props: span.props.clone(),
                    })
                    .collect(),
                multibyte: state.is_multibyte(),
                extended_chars,
            })
        }
        // lisp.h:CHECK_STRING rejects every other object class. The reader
        // already constructs StringObjects for strings with properties;
        // ordinary vectors must never be reinterpreted from their contents.
        _ => None,
    }
}

/// The character code at INDEX of a string, read in place (Faref on a
/// string: no copy of the text).  `None' for a non-string or an index past
/// the end.
pub(crate) fn string_char_code_at_in_place(value: &Value, index: usize) -> Option<i64> {
    match value.kind() {
        Kind::StringObject(state) => state.borrow().character_at(index),
        _ => None,
    }
}

/// fns.c:Fstring_equal accepts SYMBOLP arguments and reads SYMBOL_NAME,
/// which is the original Lisp string, not the symbol's host lookup key.
/// lisp.h:SYMBOLP accepts positioned symbols only while the C flag is set.
pub(crate) fn string_comparison_object(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<crate::lisp::types::StringObjectRef, LispError> {
    let symbol = match value.kind() {
        Kind::StringObject(string) => return Ok(string),
        Kind::Nil | Kind::T | Kind::Symbol(_) | Kind::SymbolWithPos(_) => {
            checked_symbol_identity(interp, value, env)
                .map_err(|_| LispError::WrongTypeArgument("stringp".into(), *value))?
        }
        _ => return Err(LispError::WrongTypeArgument("stringp".into(), *value)),
    };
    let Kind::StringObject(string) = symbol.lisp_name().kind() else {
        unreachable!("a symbol's name is a Lisp string")
    };
    Ok(string)
}

pub(crate) fn string_text(value: &Value) -> Result<String, LispError> {
    string_like(value)
        .map(|string| string.text)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))
}

/// The text `string_text' would copy, read in place: F gets it while the
/// string is borrowed, so F must not run Lisp.  The stored bytes are that
/// text when they are ASCII, or valid UTF-8 in a multibyte string (the
/// internal encoding of every Unicode character); only a string holding
/// raw bytes or characters beyond Unicode is decoded into a copy.  `None'
/// for any other object.
pub(crate) fn with_string_text<R>(value: &Value, f: impl FnOnce(&str) -> R) -> Option<R> {
    let Kind::StringObject(state) = value.kind() else {
        return None;
    };
    let state = state.borrow();
    let bytes = state.bytes();
    let text = if bytes.is_ascii() {
        // SAFETY: ASCII is UTF-8.
        Some(unsafe { std::str::from_utf8_unchecked(bytes) })
    } else if state.is_multibyte() {
        std::str::from_utf8(bytes).ok()
    } else {
        None
    };
    Some(match text {
        Some(text) => f(text),
        None => f(&state.text()),
    })
}

/// Whether VALUE is a multibyte string, read in place; `None' for any
/// other object.
pub(crate) fn string_is_multibyte(value: &Value) -> Option<bool> {
    match value.kind() {
        Kind::StringObject(state) => Some(state.borrow().is_multibyte()),
        _ => None,
    }
}

/// Whether VALUE is a string without text properties, read in place.
pub(crate) fn string_without_properties(value: &Value) -> bool {
    matches!(value.kind(), Kind::StringObject(state) if state.borrow().props.is_empty())
}

pub(crate) fn char_from_integer(code: i64) -> Result<char, LispError> {
    if code < 0 {
        return Err(LispError::Signal("Invalid character".into()));
    }
    if (RAW_BYTE8_BASE as i64 + 0x80..=RAW_BYTE8_BASE as i64 + 0xFF).contains(&code) {
        return Ok(raw_byte_regex_char((code - RAW_BYTE8_BASE as i64) as u8));
    }
    char::from_u32(code as u32).ok_or_else(|| LispError::Signal("Invalid character".into()))
}

/// Whether a string argument (or a symbol's name, for the comparisons
/// that accept symbols) is multibyte: fns.c compares bytes, and the same
/// non-ASCII characters have different bytes in the two representations.
pub(crate) fn string_argument_multibyte(value: &Value) -> bool {
    match value.kind() {
        Kind::StringObject(state) => state.borrow().is_multibyte(),
        Kind::Symbol(name) => !name.as_str().is_ascii(),
        _ => false,
    }
}

/// fns.c's string_cmp: two unibyte or all-ASCII strings compare bytewise;
/// otherwise character by character, a unibyte string's characters being
/// its bytes (0 to 255) and a multibyte string's its decoded characters
/// (a raw byte among them is 0x3FFF80 and above).  Symbols compare by
/// their names.
pub(crate) fn string_order(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Result<std::cmp::Ordering, LispError> {
    let left = string_comparison_object(interp, left, env)?;
    let right = string_comparison_object(interp, right, env)?;
    Ok(left.borrow().compare_contents(&right.borrow()))
}

pub(crate) fn fold_string_compare_code(code: i64, ignore_case: bool) -> i64 {
    if !ignore_case {
        return code;
    }
    let Some(codepoint) = u32::try_from(code).ok() else {
        return code;
    };
    simple_upcase_char(codepoint) as i64
}

pub(crate) fn string_compare_codes(
    value: &Value,
    start: Option<&Value>,
    end: Option<&Value>,
    ignore_case: bool,
) -> Result<Vec<i64>, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let codes = string_sequence_values(&string)
        .into_iter()
        .map(|value| value.as_integer())
        .collect::<Result<Vec<_>, _>>()?;
    let len = codes.len() as i64;
    let start = normalize_string_index(start, 0, len)? as usize;
    let end = normalize_string_index(end, len, len)? as usize;
    if start > end {
        return Err(LispError::Signal("Args out of range".into()));
    }
    Ok(codes[start..end]
        .iter()
        .copied()
        .map(|code| fold_string_compare_code(code, ignore_case))
        .collect())
}

// Only the non-Linux string-collate fallback compares this way; the
// GNU/Linux build collates through str_collate below.
#[cfg(not(target_os = "linux"))]
pub(crate) fn string_compare_ordering(
    left: &Value,
    right: &Value,
    ignore_case: bool,
) -> Result<Ordering, LispError> {
    Ok(
        string_compare_codes(left, None, None, ignore_case)?.cmp(&string_compare_codes(
            right,
            None,
            None,
            ignore_case,
        )?),
    )
}

pub(crate) fn compare_strings_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    // fns.c:Fcompare_strings checks both string types before either
    // range, then clamps only too-large positive fixnum end arguments.
    // The dispatch caller has already checked the six/seven argument arity.
    let Kind::StringObject(left) = args[0].kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
    };
    let Kind::StringObject(right) = args[3].kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), args[3]));
    };
    let clamp_end = |end: Value, length: usize| match end.kind() {
        Kind::Integer(index) if index > length as i64 => Value::Integer(length as i64),
        _ => end,
    };
    let left_end = clamp_end(args[2], left.borrow().len());
    let right_end = clamp_end(args[5], right.borrow().len());
    let (left_start, left_end) = {
        let state = left.borrow();
        validate_subarray(args[0], args[1], left_end, state.len())?
    };
    let (right_start, right_end) = {
        let state = right.borrow();
        validate_subarray(args[3], args[4], right_end, state.len())?
    };
    let ignore_case = args.get(6).is_some_and(Value::is_truthy);
    compare_string_ranges(
        interp,
        &args[0],
        (left_start, left_end),
        &args[3],
        (right_start, right_end),
        ignore_case,
        env,
    )
}

/// The comparison of fns.c:Fcompare_strings over character ranges of two
/// strings already validated: t when they are equal, else the 1-based
/// position of the first difference, negative when LEFT is less.
pub(crate) fn compare_string_ranges(
    interp: &mut Interpreter,
    left: &Value,
    (left_start, left_end): (usize, usize),
    right: &Value,
    (right_start, right_end): (usize, usize),
    ignore_case: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Kind::StringObject(left) = left.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *left));
    };
    let Kind::StringObject(right) = right.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *right));
    };
    let mut left_byte = left
        .borrow()
        .byte_offset(left_start)
        .expect("validated string index");
    let mut right_byte = right
        .borrow()
        .byte_offset(right_start)
        .expect("validated string index");
    let left_length = left_end - left_start;
    let right_length = right_end - right_start;
    let common_length = left_length.min(right_length);
    for matched in 0..common_length {
        let mut a = left
            .borrow()
            .character_as_multibyte_advance(&mut left_byte)
            .expect("validated string range");
        let mut b = right
            .borrow()
            .character_as_multibyte_advance(&mut right_byte)
            .expect("validated string range");
        if a == b {
            continue;
        }
        if ignore_case {
            // GNU calls Fupcase on the promoted character only after a
            // mismatch. Use the same live buffer case tables and numeric
            // casing path, releasing string borrows before either call.
            a = casify_value(interp, &Value::Integer(a), CaseAction::Up, env)?.as_integer()?;
            b = casify_value(interp, &Value::Integer(b), CaseAction::Up, env)?.as_integer()?;
        }
        match a.cmp(&b) {
            Ordering::Less => return Ok(Value::Integer(-((matched + 1) as i64))),
            Ordering::Greater => return Ok(Value::Integer((matched + 1) as i64)),
            Ordering::Equal => {}
        }
    }
    match left_length.cmp(&right_length) {
        Ordering::Less => Ok(Value::Integer(-((common_length + 1) as i64))),
        Ordering::Greater => Ok(Value::Integer((common_length + 1) as i64)),
        Ordering::Equal => Ok(Value::T),
    }
}

// sysdep.c str_collate exists only under __STDC_ISO_10646__ (wchar_t is
// ISO 10646 code points), which GNU/Linux defines and Darwin does not:
// on Darwin GNU itself falls back to plain string-lessp/string-equal,
// which is the non-Linux path below.
#[cfg(target_os = "linux")]
mod collate_ffi {
    // glibc locale.h: LC_CTYPE is category 0 and LC_COLLATE category 3;
    // each *_MASK is 1 << category.
    pub(super) const LC_CTYPE_MASK: libc::c_int = 1 << 0;
    pub(super) const LC_COLLATE_MASK: libc::c_int = 1 << 3;
    // glibc bits/types/wint_t.h: wint_t is unsigned int.
    pub(super) type WintT = libc::c_uint;
    unsafe extern "C" {
        pub(super) fn wcscoll_l(
            left: *const libc::wchar_t,
            right: *const libc::wchar_t,
            locale: libc::locale_t,
        ) -> libc::c_int;
        pub(super) fn towlower_l(ch: WintT, locale: libc::locale_t) -> WintT;
        pub(super) fn wcscoll(
            left: *const libc::wchar_t,
            right: *const libc::wchar_t,
        ) -> libc::c_int;
        pub(super) fn towlower(ch: WintT) -> WintT;
    }
}

/// fns.c Fstring_collate_lessp/equalp: symbol arguments (nil and t
/// included) collate by their print names.
#[cfg(target_os = "linux")]
fn collate_operand_codes(value: &Value) -> Result<Vec<i64>, LispError> {
    let named;
    let value = match value.kind() {
        Kind::Symbol(name) => {
            named = Value::String(name.to_string().into());
            &named
        }
        Kind::Nil => {
            named = Value::String("nil".into());
            &named
        }
        Kind::T => {
            named = Value::String("t".into());
            &named
        }
        other => &other.value(),
    };
    string_compare_codes(value, None, None, false)
}

/// sysdep.c str_collate (GNU/Linux): widen both strings to code-point
/// arrays, lowercase them with towlower_l/towlower when IGNORE-CASE, and
/// compare with wcscoll_l in LOCALE (newlocale of LC_COLLATE|LC_CTYPE;
/// an unresolvable locale signals "Invalid locale ...") or with wcscoll
/// in the process's current locale when LOCALE is nil.
#[cfg(target_os = "linux")]
pub(crate) fn str_collate(
    left: &Value,
    right: &Value,
    locale: Option<&Value>,
    ignore_case: bool,
) -> Result<Ordering, LispError> {
    let mut left_wide: Vec<libc::wchar_t> = collate_operand_codes(left)?
        .into_iter()
        .map(|code| code as libc::wchar_t)
        .chain(std::iter::once(0))
        .collect();
    let mut right_wide: Vec<libc::wchar_t> = collate_operand_codes(right)?
        .into_iter()
        .map(|code| code as libc::wchar_t)
        .chain(std::iter::once(0))
        .collect();
    // GNU's case fold walks each wide string up to its NUL terminator.
    fn fold(codes: &mut [libc::wchar_t], lower: impl Fn(collate_ffi::WintT) -> collate_ffi::WintT) {
        for code in codes {
            if *code == 0 {
                break;
            }
            *code = lower(*code as collate_ffi::WintT) as libc::wchar_t;
        }
    }
    let result = match locale.filter(|value| !value.is_nil()) {
        Some(locale_value) => {
            let Some(_) = string_like(locale_value) else {
                // fns.c: `if (!NILP (locale)) CHECK_STRING (locale)'.
                return Err(LispError::WrongTypeArgument(
                    "stringp".into(),
                    *locale_value,
                ));
            };
            let locale_text = string_text(locale_value)?;
            // C sees the bytes up to the first NUL.
            let locale_c =
                std::ffi::CString::new(locale_text.split('\0').next().unwrap_or_default())
                    .expect("NUL split above");
            let loc = unsafe {
                libc::newlocale(
                    collate_ffi::LC_COLLATE_MASK | collate_ffi::LC_CTYPE_MASK,
                    locale_c.as_ptr(),
                    std::ptr::null_mut(),
                )
            };
            if loc.is_null() {
                let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                let strerror = unsafe { std::ffi::CStr::from_ptr(libc::strerror(errno)) };
                return Err(LispError::Signal(format!(
                    "Invalid locale {locale_text}: {}",
                    strerror.to_string_lossy()
                )));
            }
            if ignore_case {
                fold(&mut left_wide, |code| unsafe {
                    collate_ffi::towlower_l(code, loc)
                });
                fold(&mut right_wide, |code| unsafe {
                    collate_ffi::towlower_l(code, loc)
                });
            }
            let result =
                unsafe { collate_ffi::wcscoll_l(left_wide.as_ptr(), right_wide.as_ptr(), loc) };
            unsafe { libc::freelocale(loc) };
            result
        }
        None => {
            if ignore_case {
                fold(&mut left_wide, |code| unsafe {
                    collate_ffi::towlower(code)
                });
                fold(&mut right_wide, |code| unsafe {
                    collate_ffi::towlower(code)
                });
            }
            unsafe { collate_ffi::wcscoll(left_wide.as_ptr(), right_wide.as_ptr()) }
        }
    };
    Ok(result.cmp(&0))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn validate_collation_locale(locale: Option<&Value>) -> Result<(), LispError> {
    if locale.is_some_and(|value| {
        !(value.is_nil() || matches!(value.kind(), Kind::T) || string_like(value).is_some())
    }) {
        return Err(LispError::TypeError(
            "string".into(),
            locale.expect("checked above").type_name(),
        ));
    }
    Ok(())
}

/// data.c's Faset signals `args-out-of-range' with the array and the index.
fn args_out_of_range_for_aset(target: &Value, index: usize) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("args-out-of-range".into()),
        *target,
        Value::Integer(index as i64),
    ]))
}

pub(crate) fn aset_string_value(
    target: &Value,
    index: usize,
    new_value: &Value,
) -> Result<Value, crate::lisp::types::string_data::StringError> {
    let Kind::StringObject(state) = target.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *target).into());
    };
    state.check_impure()?;
    let mut state = state.borrow_mut();
    // data.c:Faset checks the existing index before NEWELT.
    if index >= state.len() {
        drop(state);
        return Err(args_out_of_range_for_aset(target, index).into());
    }
    let code = crate::lisp::types::string_data::character_code(*new_value)?;
    if !state.store_character(index, code)? {
        drop(state);
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            *target,
            *new_value,
        ]))
        .into());
    }
    Ok(*target)
}

pub(crate) fn shared_string_props(props: &[TextPropertySpan]) -> Vec<StringPropertySpan> {
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
    make_shared_string_value_with_extended_chars(text, props, multibyte, Vec::new())
}

pub(crate) fn make_shared_string_value_with_extended_chars(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
    extended_chars: Vec<(usize, u32)>,
) -> Value {
    Value::StringObject(StringObjectRef::from_text(
        text,
        shared_string_props(&props),
        multibyte,
        extended_chars,
    ))
}

pub(crate) fn string_like_value_with_extended_chars(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
    extended_chars: Vec<(usize, u32)>,
) -> Value {
    if extended_chars.is_empty() {
        string_like_value_with_multibyte(text, props, multibyte)
    } else {
        make_shared_string_value_with_extended_chars(text, props, multibyte, extended_chars)
    }
}

pub(crate) fn string_like_value_with_multibyte(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
) -> Value {
    // GNU string-producing primitives allocate mutable Lisp string objects
    // even when the result is ASCII and has no properties yet.  Collapsing
    // that case into Emaxx's immutable host-text representation breaks
    // subsequent `aset' and text-property mutation through aliases.
    make_shared_string_value_with_multibyte(text, props, multibyte)
}

pub(crate) fn string_like_value(text: String, props: Vec<TextPropertySpan>) -> Value {
    let multibyte = text
        .chars()
        .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
    string_like_value_with_multibyte(text, merge_string_props(props), multibyte)
}

pub(crate) fn reverse_string_like_value(value: &Value) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
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

pub(crate) fn reverse_sequence_value(value: &Value) -> Result<Value, LispError> {
    if string_like(value).is_some() {
        return reverse_string_like_value(value);
    }
    if is_bool_vector_value(value) {
        let mut bits = bool_vector_bits(value)?;
        bits.reverse();
        return Ok(make_bool_vector_value(bits));
    }
    match value.kind() {
        Kind::Vector(_) | Kind::Cons(_) if is_vector_value(value) => {
            let mut items = value.to_vec()?;
            items[1..].reverse();
            Ok(Value::list(items))
        }
        Kind::Nil | Kind::Cons(_) => {
            let mut items = value.to_vec()?;
            items.reverse();
            Ok(Value::list(items))
        }
        _ => Err(LispError::WrongTypeArgument("sequencep".into(), *value)),
    }
}

pub(crate) fn nreverse_sequence_value(value: &Value) -> Result<Value, LispError> {
    if string_like(value).is_some() {
        return reverse_string_like_value(value);
    }
    // fns.c:Fnreverse: the bits swapped in place.
    if let Kind::BoolVector(vector) = value.kind() {
        let size = vector.len();
        for index in 0..size / 2 {
            let (low, high) = (vector.get(index), vector.get(size - index - 1));
            vector.set(index, high == Some(true));
            vector.set(size - index - 1, low == Some(true));
        }
        return Ok(*value);
    }
    match value.kind() {
        Kind::Vector(_) | Kind::Cons(_) if is_vector_value(value) => {
            let mut items = vector_items(value)?;
            items.reverse();
            for (index, item) in items.into_iter().enumerate() {
                aset_vector_value(value, index, item)?;
            }
            Ok(*value)
        }
        Kind::Nil | Kind::Cons(_) => nreverse_list_cells(value),
        _ => Err(LispError::WrongTypeArgument("sequencep".into(), *value)),
    }
}

fn nreverse_list_cells(value: &Value) -> Result<Value, LispError> {
    let mut current = *value;
    let mut reversed = Value::Nil;
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        let cell = match current.kind() {
            Kind::Nil => return Ok(reversed),
            Kind::Cons(cell) => cell,
            other => return Err(LispError::WrongTypeArgument("listp".into(), other.value())),
        };
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("circular-list".into()),
                Value::String("Circular list".into()),
            ])));
        }
        let next = cell.cdr.get();
        cell.cdr.set(reversed);
        reversed = Value::Cons(cell);
        current = next;
    }
}

/// A property list's pairs: any object names a property, as textprop.c
/// takes each plist's names as they are.
pub(crate) fn plist_pairs(value: &Value) -> Result<Vec<(PropertyName, Value)>, LispError> {
    let items = value.to_vec()?;
    let mut props = Vec::new();
    let mut i = 0;
    while i + 1 < items.len() {
        props.push((PropertyName::new(items[i]), items[i + 1]));
        i += 2;
    }
    Ok(props)
}

pub(crate) fn plist_value(props: &[(PropertyName, Value)]) -> Value {
    Value::list(props.iter().flat_map(|(key, value)| [key.value(), *value]))
}

pub(crate) fn object_intervals_value(
    interp: &mut Interpreter,
    object: &Value,
) -> Result<Value, LispError> {
    // A string keeps its intervals when their properties are removed;
    // traverse_intervals then visits them with empty plists.
    let (len, spans, has_intervals) = if let Some(string) = string_like(object) {
        let has_intervals = matches!(object.kind(), Kind::StringObject(state)
            if state.borrow().props.has_intervals());
        (
            string.text.chars().count(),
            merge_string_props(string.props),
            has_intervals,
        )
    } else {
        let buffer_id = interp.resolve_buffer_id(object)?;
        let buffer = interp
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
        let spans = buffer.substring_property_spans(buffer.point_min(), buffer.point_max());
        let has_intervals = !spans.is_empty();
        (
            buffer.point_max() - buffer.point_min(),
            spans,
            has_intervals,
        )
    };

    if !has_intervals {
        return Ok(Value::Nil);
    }

    let mut boundaries = vec![0usize, len];
    for span in &spans {
        boundaries.push(span.start);
        boundaries.push(span.end);
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut intervals = Vec::new();
    for window in boundaries.windows(2) {
        let start = window[0];
        let end = window[1];
        if start >= end {
            continue;
        }
        let props = spans
            .iter()
            .find(|span| span.start <= start && start < span.end)
            .map(|span| plist_value(&span.props))
            .unwrap_or(Value::Nil);
        intervals.push(Value::list([
            Value::Integer(start as i64),
            Value::Integer(end as i64),
            props,
        ]));
    }

    Ok(Value::list(intervals))
}

pub(crate) fn shift_string_props(
    props: &[TextPropertySpan],
    offset: usize,
) -> Vec<TextPropertySpan> {
    props
        .iter()
        .map(|span| TextPropertySpan {
            start: span.start + offset,
            end: span.end + offset,
            props: span.props.clone(),
        })
        .collect()
}

/// textprop.c copy_text_properties (Fsubstring) and fns.c
/// concat_to_string (`concat', `mapconcat') hand each source interval's
/// plist to `add_text_properties' on the fresh result, and add_properties
/// conses every property it did not find onto the head of the plist: a
/// copied span comes out with its pairs reversed, so
/// `(substring (propertize "x" 'a 1 'b 2) 0)' prints as (b 2 a 1) where
/// `copy-sequence' (copy_intervals) keeps (a 1 b 2).
pub(crate) fn copied_string_props(
    props: &[TextPropertySpan],
    offset: usize,
) -> Vec<TextPropertySpan> {
    props
        .iter()
        .map(|span| TextPropertySpan {
            start: span.start + offset,
            end: span.end + offset,
            props: span.props.iter().rev().cloned().collect(),
        })
        .collect()
}

pub(crate) fn slice_string_props(
    props: &[TextPropertySpan],
    from: usize,
    to: usize,
) -> Vec<TextPropertySpan> {
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

/// editfns.c styled_format's property layering: the format string's
/// properties cover each substituted span, and the argument string's own
/// properties are then ADDED over them (add-text-properties semantics:
/// shared keys take the argument's value, and the argument's keys print
/// first).  The format builder records these as OVERLAPPING spans in
/// arrival order (argument spans first, the covering spec span last);
/// flatten them into disjoint spans whose plists carry the union, with
/// the earliest covering span winning each key.
pub(crate) fn flatten_overlapping_string_props(
    spans: Vec<TextPropertySpan>,
) -> Vec<TextPropertySpan> {
    let mut bounds: Vec<usize> = spans
        .iter()
        .filter(|span| span.start < span.end)
        .flat_map(|span| [span.start, span.end])
        .collect();
    bounds.sort_unstable();
    bounds.dedup();
    let mut result = Vec::new();
    for window in bounds.windows(2) {
        let (start, end) = (window[0], window[1]);
        let mut plist: Vec<(PropertyName, Value)> = Vec::new();
        for span in &spans {
            if span.start <= start && end <= span.end {
                for (key, value) in &span.props {
                    if !plist.iter().any(|(existing, _)| existing == key) {
                        plist.push((*key, *value));
                    }
                }
            }
        }
        if !plist.is_empty() {
            result.push(TextPropertySpan {
                start,
                end,
                props: plist,
            });
        }
    }
    merge_string_props(result)
}

pub(crate) fn merge_string_props(mut props: Vec<TextPropertySpan>) -> Vec<TextPropertySpan> {
    props.retain(|span| span.start < span.end && !span.props.is_empty());
    props.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<TextPropertySpan> = Vec::new();
    for span in props {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && crate::buffer::text_property_plists_eq(&last.props, &span.props)
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
    }
    merged
}

pub(crate) fn property_from_category_symbol(
    interp: &Interpreter,
    props: &[(PropertyName, Value)],
    prop: impl PropertyKey,
) -> Option<Value> {
    if prop.is_named("category") {
        return None;
    }
    let category = props
        .iter()
        .find(|(name, _)| *name == "category")
        .and_then(|(_, value)| value.symbol_str())?;
    interp.get_symbol_property_by(category, prop)
}

/// intervals.c:textget, lookup_char_property for a text property: PROP's
/// value in the interval's PROPS; else the non-nil PROP of its `category'
/// symbol; else the first non-nil value in PROPS of an alternative that
/// `char-property-alias-alist' (read as the C variable, in the current
/// buffer) gives PROP, with no category consulted; else PROP's value in
/// `default-text-properties', which an empty interval reads too.  None
/// where every one of them is nil.
pub(crate) fn property_from_props_with_category(
    interp: &Interpreter,
    props: &[(PropertyName, Value)],
    prop: impl PropertyKey,
) -> Option<Value> {
    if let Some((_, value)) = props.iter().find(|(name, _)| prop.names(name)) {
        return Some(*value);
    }
    if !props.is_empty() {
        if let Some(value) =
            property_from_category_symbol(interp, props, prop).filter(|value| !value.is_nil())
        {
            return Some(value);
        }
        let aliases = interp
            .symbol_value_key(cached_symbol!("char-property-alias-alist"))
            .unwrap_or(Value::Nil);
        if let Some(value) = alias_property(props, prop, aliases) {
            return Some(value);
        }
    }
    let defaults = interp
        .symbol_value_key(cached_symbol!("default-text-properties"))
        .unwrap_or(Value::Nil);
    Some(lisp_plist_get(defaults, prop)).filter(|value| !value.is_nil())
}

/// lookup_char_property's alternatives: `Fassq' of PROP in ALIASES, then
/// the first name of its cdr with a non-nil value in PROPS.
fn alias_property(
    props: &[(PropertyName, Value)],
    prop: impl PropertyKey,
    aliases: Value,
) -> Option<Value> {
    let mut seen = crate::lisp::types::CycleGuard::new();
    let mut tail = aliases;
    let names = loop {
        let Kind::Cons(cell) = tail.kind() else {
            return None;
        };
        if let Kind::Cons(entry) = cell.car.get().kind()
            && prop.is(entry.car.get())
        {
            break entry.cdr.get();
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return None;
        }
        tail = cell.cdr.get();
    };
    let mut seen = crate::lisp::types::CycleGuard::new();
    let mut tail = names;
    while let Kind::Cons(cell) = tail.kind() {
        let alias = PropertyName::new(cell.car.get());
        if let Some((_, value)) = props.iter().find(|(name, _)| *name == alias)
            && !value.is_nil()
        {
            return Some(*value);
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return None;
        }
        tail = cell.cdr.get();
    }
    None
}

/// fns.c:plist_get of PROP in the Lisp property list PLIST.
fn lisp_plist_get(plist: Value, prop: impl PropertyKey) -> Value {
    let mut seen = crate::lisp::types::CycleGuard::new();
    let mut tail = plist;
    while let Kind::Cons(cell) = tail.kind() {
        let Kind::Cons(rest) = cell.cdr.get().kind() else {
            break;
        };
        if prop.is(cell.car.get()) {
            return rest.car.get();
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        tail = rest.cdr.get();
    }
    Value::Nil
}

pub(crate) fn buffer_property_at_with_category(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: impl PropertyKey,
) -> Option<Value> {
    // Out of the accessible text there is no interval: the empty plist's
    // textget, `default-text-properties' alone (Ftext_properties_at's nil
    // at the end).
    property_from_props_with_category(interp, buffer.text_properties_at_ref(pos), prop)
}

pub(crate) fn buffer_char_property_at(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: impl PropertyKey,
) -> Value {
    buffer_char_property_at_with_overlay_id(interp, buffer, pos, prop).0
}

pub(crate) fn buffer_char_property_at_with_overlay_id(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: impl PropertyKey,
) -> (Value, Option<crate::overlay::OverlayRef>) {
    if let Some((value, overlay_id)) =
        highest_priority_overlay_property_with_id(interp, buffer, pos, prop, false, None)
    {
        return (value, Some(overlay_id));
    }
    (
        buffer_property_at_with_category(interp, buffer, pos, prop).unwrap_or(Value::Nil),
        None,
    )
}

/// buffer.c:Foverlay_get, intervals.c:lookup_char_property over the
/// overlay's plist without `default-text-properties': PROP's value, else
/// the non-nil PROP of its `category' symbol, else the first non-nil
/// value in the plist of an alternative `char-property-alias-alist' gives
/// PROP.  None where that is nil.
pub(crate) fn overlay_property_with_category(
    interp: &Interpreter,
    overlay: &crate::overlay::OverlayRef,
    prop: impl PropertyKey,
) -> Option<Value> {
    let plist = overlay.plist();
    let mut fallback = Value::Nil;
    let mut seen = crate::lisp::types::CycleGuard::new();
    let mut tail = plist;
    while let Kind::Cons(cell) = tail.kind() {
        let name = cell.car.get();
        let rest = cell.cdr.get();
        let value = match rest.kind() {
            Kind::Cons(rest) => rest.car.get(),
            _ => Value::Nil,
        };
        if prop.is(name) {
            return Some(value).filter(|value| !value.is_nil());
        }
        if matches!(name.kind(), Kind::Symbol(symbol) if symbol == "category")
            && let Some(category) = value.symbol_str()
        {
            fallback = interp
                .get_symbol_property_by(category, prop)
                .unwrap_or(Value::Nil);
        }
        let Kind::Cons(rest) = rest.kind() else {
            break;
        };
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        tail = rest.cdr.get();
    }
    if !fallback.is_nil() {
        return Some(fallback);
    }
    let aliases = interp
        .symbol_value_key(cached_symbol!("char-property-alias-alist"))
        .unwrap_or(Value::Nil);
    let mut seen = crate::lisp::types::CycleGuard::new();
    let mut tail = aliases;
    while let Kind::Cons(cell) = tail.kind() {
        if let Kind::Cons(entry) = cell.car.get().kind()
            && prop.is(entry.car.get())
        {
            let mut names = entry.cdr.get();
            let mut seen = crate::lisp::types::CycleGuard::new();
            while let Kind::Cons(alias) = names.kind() {
                let value = lisp_plist_get(plist, PropertyName::new(alias.car.get()));
                if !value.is_nil() {
                    return Some(value);
                }
                if seen.step(crate::lisp::types::ConsCell::identity(&alias)) {
                    break;
                }
                names = alias.cdr.get();
            }
            return None;
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        tail = cell.cdr.get();
    }
    None
}

pub(crate) fn string_property_at(
    value: &Value,
    pos: usize,
    prop: impl PropertyKey,
) -> Option<Value> {
    let string = string_like(value)?;
    string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .and_then(|span| {
            span.props
                .iter()
                .find(|(name, _)| prop.names(name))
                .map(|(_, value)| *value)
        })
}

pub(crate) fn string_property_at_with_category(
    interp: &Interpreter,
    value: &Value,
    pos: usize,
    prop: impl PropertyKey,
) -> Option<Value> {
    let string = string_like(value)?;
    // A position no interval covers has the empty plist, whose textget
    // still reads `default-text-properties'.
    let props = string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map_or(&[][..], |span| span.props.as_slice());
    property_from_props_with_category(interp, props, prop)
}

pub(crate) fn merge_string_object_props(
    mut spans: Vec<StringPropertySpan>,
) -> Vec<StringPropertySpan> {
    spans.retain(|span| span.start < span.end && !span.props.is_empty());
    spans.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<StringPropertySpan> = Vec::new();
    for span in spans {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && crate::buffer::text_property_plists_eq(&last.props, &span.props)
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
    }
    merged
}

pub(crate) fn string_object_properties_at(
    spans: &[StringPropertySpan],
    pos: usize,
) -> Vec<(PropertyName, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

pub(crate) fn modify_shared_string_properties<F>(
    value: &Value,
    start: usize,
    end: usize,
    mut f: F,
) -> Result<bool, LispError>
where
    F: FnMut(Vec<(PropertyName, Value)>) -> Vec<(PropertyName, Value)>,
{
    let Kind::StringObject(state) = value.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *value));
    };
    let len = state.borrow().len();
    let start = start.min(len);
    let end = end.min(len);
    if start >= end {
        return Ok(false);
    }

    // Only the spans meeting [START, END) change, as textprop.c edits only
    // the intervals there: they are replaced by their parts outside the
    // range and the range's segments as F leaves them, merged with each
    // other and with the equal neighbours they now touch.
    let (first, last, replacement, changed, total) = {
        let borrowed = state.borrow();
        let spans = &borrowed.props[..];
        let lo = spans.partition_point(|span| span.end <= start);
        let hi = spans.partition_point(|span| span.start < end).max(lo);
        let affected = &spans[lo..hi];
        let mut pieces = Vec::new();
        if let Some(span) = affected.first()
            && span.start < start
        {
            pieces.push(StringPropertySpan {
                start: span.start,
                end: start,
                props: span.props.clone(),
            });
        }
        let mut boundaries = vec![start, end];
        for span in affected {
            boundaries.push(span.start.max(start));
            boundaries.push(span.end.min(end));
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut changed = false;
        for window in boundaries.windows(2) {
            let (seg_start, seg_end) = (window[0], window[1]);
            if seg_start >= seg_end {
                continue;
            }
            let current = string_object_properties_at(affected, seg_start);
            let next = f(current.clone());
            changed |= !crate::buffer::text_property_plists_eq(&current, &next);
            if !next.is_empty() {
                pieces.push(StringPropertySpan {
                    start: seg_start,
                    end: seg_end,
                    props: next,
                });
            }
        }
        if let Some(span) = affected.last()
            && span.end > end
        {
            pieces.push(StringPropertySpan {
                start: end,
                end: span.end,
                props: span.props.clone(),
            });
        }
        // Take in the neighbours a merge may join.
        let mut first = lo;
        let mut last = hi;
        if first > 0 {
            first -= 1;
            pieces.insert(0, spans[first].clone());
        }
        if last < spans.len() {
            pieces.push(spans[last].clone());
            last += 1;
        }
        let replacement = merge_string_object_props(pieces);
        let total = spans.len() - (last - first) + replacement.len();
        (first, last, replacement, changed, total)
    };

    // A pure string has no intervals. GNU soft removal therefore returns
    // without writing; creating intervals invokes CHECK_IMPURE instead.
    if state.is_pure() && total == 0 {
        return Ok(false);
    }
    state.check_impure()?;
    let mut state = state.borrow_mut();
    let had_intervals = state.props.has_intervals();
    let mut spans = std::mem::take(&mut state.props).into_vec();
    spans.splice(first..last, replacement);
    // Intervals stay when their properties go (textprop.c removes plist
    // entries; only `set-text-properties' of the whole string drops them).
    state.props = if spans.is_empty() && had_intervals {
        crate::lisp::types::string_data::StringProperties::empty_tree()
    } else {
        spans.into()
    };
    Ok(changed)
}
