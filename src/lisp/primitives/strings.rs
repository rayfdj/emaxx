use super::*;
use crate::lisp::types::Kind;

/// fns.c:validate_subarray checks both index types before their joint range.
fn validate_subarray(
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
            if result.len() == 0 && !result.is_multibyte() {
                Ok(Value::String("".into()))
            } else {
                Ok(crate::lisp::types::string_object_value(result))
            }
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

/// A transient Rust text view of canonical bytes. It is never retained as
/// mutable string state; consumers needing characters or bytes use the payload.
pub(crate) fn borrowed_text(value: &Value) -> Option<std::borrow::Cow<'_, str>> {
    match value.kind() {
        Kind::StringObject(state) => Some(std::borrow::Cow::Owned(state.borrow().text())),
        _ => None,
    }
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

/// `equal' on two strings, compared in place.  fns.c's internal_equal
/// compares the character count, the byte count and the bytes: two
/// strings of the same characters differ when one is unibyte and the
/// other multibyte and any character is not ASCII (a raw byte is one
/// byte unibyte and two multibyte).  `None' when either is not a string.
pub(crate) fn string_texts_equal_in_place(left: &Value, right: &Value) -> Option<bool> {
    let (Kind::StringObject(left), Kind::StringObject(right)) = (left.kind(), right.kind()) else {
        return None;
    };
    Some(left == right)
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

pub(crate) fn char_from_integer(code: i64) -> Result<char, LispError> {
    if code < 0 {
        return Err(LispError::Signal("Invalid character".into()));
    }
    if (RAW_BYTE8_BASE as i64..=RAW_BYTE8_BASE as i64 + 0xFF).contains(&code) {
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
pub(crate) fn string_order(left: &Value, right: &Value) -> Result<std::cmp::Ordering, LispError> {
    let left_text = string_comparison_text(left)?;
    let right_text = string_comparison_text(right)?;
    if left_text.is_ascii() && right_text.is_ascii() {
        return Ok(left_text.as_bytes().cmp(right_text.as_bytes()));
    }
    let codes = |value: &Value, text: &str| -> Vec<i64> {
        match string_like(value) {
            Some(string) => string.character_codes(),
            None => {
                let multibyte = !text.is_ascii();
                text.chars()
                    .map(|ch| string_character_code(multibyte, ch))
                    .collect()
            }
        }
    };
    Ok(codes(left, &left_text).cmp(&codes(right, &right_text)))
}

pub(crate) fn string_comparison_text(value: &Value) -> Result<String, LispError> {
    match value.kind() {
        Kind::Nil => Ok("nil".into()),
        Kind::T => Ok("t".into()),
        Kind::Symbol(name) => Ok(crate::lisp::types::visible_symbol_name(&name).to_string()),
        _ => string_text(value),
    }
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

pub(crate) fn normalize_compare_strings_end(
    arg: Option<&Value>,
    len: i64,
) -> Result<i64, LispError> {
    let Some(value) = arg else {
        return Ok(len);
    };
    if value.is_nil() {
        return Ok(len);
    }
    let raw = value.as_integer()?;
    let index = if raw < 0 { len + raw } else { raw };
    Ok(index.clamp(0, len))
}

pub(crate) fn string_compare_codes(
    value: &Value,
    start: Option<&Value>,
    end: Option<&Value>,
    ignore_case: bool,
    clamp_end: bool,
) -> Result<Vec<i64>, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let codes = string_sequence_values(&string)
        .into_iter()
        .map(|value| value.as_integer())
        .collect::<Result<Vec<_>, _>>()?;
    let len = codes.len() as i64;
    let start = normalize_string_index(start, 0, len)? as usize;
    let end = if clamp_end {
        normalize_compare_strings_end(end, len)?
    } else {
        normalize_string_index(end, len, len)?
    } as usize;
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
        string_compare_codes(left, None, None, ignore_case, false)?.cmp(&string_compare_codes(
            right,
            None,
            None,
            ignore_case,
            false,
        )?),
    )
}

pub(crate) fn compare_strings_value(
    left: &Value,
    left_start: Option<&Value>,
    left_end: Option<&Value>,
    right: &Value,
    right_start: Option<&Value>,
    right_end: Option<&Value>,
    ignore_case: bool,
) -> Result<Value, LispError> {
    // fns.c reads both strings with fetch_string_char_as_multibyte_advance:
    // a unibyte string's byte above 127 is the raw-byte character.
    let promote = |codes: Vec<i64>, value: &Value| -> Vec<i64> {
        if string_argument_multibyte(value) {
            return codes;
        }
        codes
            .into_iter()
            .map(|code| {
                if (0x80..=0xFF).contains(&code) {
                    RAW_BYTE8_BASE as i64 + code
                } else {
                    code
                }
            })
            .collect()
    };
    let left = promote(
        string_compare_codes(left, left_start, left_end, ignore_case, true)?,
        left,
    );
    let right = promote(
        string_compare_codes(right, right_start, right_end, ignore_case, true)?,
        right,
    );
    let common_len = left.len().min(right.len());

    for index in 0..common_len {
        match left[index].cmp(&right[index]) {
            Ordering::Less => return Ok(Value::Integer(-((index + 1) as i64))),
            Ordering::Greater => return Ok(Value::Integer((index + 1) as i64)),
            Ordering::Equal => {}
        }
    }

    match left.len().cmp(&right.len()) {
        Ordering::Less => Ok(Value::Integer(-((common_len + 1) as i64))),
        Ordering::Greater => Ok(Value::Integer((common_len + 1) as i64)),
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
    string_compare_codes(value, None, None, false, false)
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

pub(crate) fn assoc_string_text(value: &Value) -> Result<String, LispError> {
    match value.kind() {
        Kind::Nil => Ok("nil".into()),
        Kind::T => Ok("t".into()),
        Kind::Symbol(name) => Ok(name.to_string()),
        _ => string_text(value),
    }
}

pub(crate) fn assoc_string_candidate_text(value: &Value) -> Option<String> {
    match value.kind() {
        Kind::Nil => Some("nil".into()),
        Kind::T => Some("t".into()),
        Kind::Symbol(name) => Some(name.to_string()),
        _ => string_like(value).map(|string| string.text),
    }
}

pub(crate) fn assoc_string_folded_text(
    interp: &mut Interpreter,
    text: &str,
) -> Result<String, LispError> {
    // fns.c's assoc-string folds through the same casing machinery, so it uses
    // the same prepared context (GNU: `casify_object' with CASE_DOWN).
    let context = case::CasingContext::prepare(
        interp,
        case::CaseAction::Down,
        &mut crate::lisp::types::Env::new(),
    );
    let (down_table, _) = current_case_table_ids(interp)?;
    let mut folded = String::new();
    let chars: Vec<char> = text.chars().collect();
    for (index, ch) in chars.iter().copied().enumerate() {
        let next_is_word = chars
            .get(index + 1)
            .copied()
            .is_some_and(|next| interp.is_syntax_word_char(normalize_case_key(next as u32)));
        folded.push_str(&full_downcase_string(
            interp,
            &context,
            down_table,
            ch,
            interp.is_syntax_word_char(normalize_case_key(ch as u32)) && !next_is_word,
        ));
    }
    Ok(folded)
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
) -> Result<Value, LispError> {
    let Kind::StringObject(state) = target.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *target));
    };
    let mut state = state.borrow_mut();
    // data.c:Faset checks the existing index before NEWELT.
    if index >= state.len() {
        drop(state);
        return Err(args_out_of_range_for_aset(target, index));
    }
    let code = crate::lisp::types::string_data::character_code(*new_value)?;
    if !state.store_character(index, code) {
        drop(state);
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            *target,
            *new_value,
        ])));
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
    crate::lisp::types::string_object_value(SharedStringState::new(
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

pub(crate) fn reverse_sequence_value(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    if string_like(value).is_some() {
        return reverse_string_like_value(value);
    }
    if is_bool_vector_value(interp, value) {
        let mut bits = bool_vector_bits(interp, value)?;
        bits.reverse();
        return Ok(make_bool_vector_value(interp, bits));
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

pub(crate) fn nreverse_sequence_value(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    if string_like(value).is_some() {
        return reverse_string_like_value(value);
    }
    if let Kind::Record(id) = value.kind()
        && is_bool_vector_value(interp, value)
    {
        let record = interp
            .find_record_mut(id)
            .ok_or_else(|| LispError::WrongTypeArgument("bool-vector-p".into(), *value))?;
        record.slots.reverse();
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

pub(crate) fn plist_pairs(value: &Value) -> Result<Vec<(String, Value)>, LispError> {
    let items = value.to_vec()?;
    let mut props = Vec::new();
    let mut i = 0;
    while i + 1 < items.len() {
        let key = items[i].as_symbol()?.to_string();
        props.push((key, items[i + 1]));
        i += 2;
    }
    Ok(props)
}

pub(crate) fn plist_value(props: &[(String, Value)]) -> Value {
    let mut items = Vec::new();
    for (key, value) in props {
        items.push(Value::Symbol(key.clone().into()));
        items.push(*value);
    }
    Value::list(items)
}

pub(crate) fn object_intervals_value(
    interp: &mut Interpreter,
    object: &Value,
) -> Result<Value, LispError> {
    let (len, spans) = if let Some(string) = string_like(object) {
        (
            string.text.chars().count(),
            merge_string_props(string.props),
        )
    } else {
        let buffer_id = interp.resolve_buffer_id(object)?;
        let buffer = interp
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
        (
            buffer.point_max() - buffer.point_min(),
            buffer.substring_property_spans(buffer.point_min(), buffer.point_max()),
        )
    };

    if spans.is_empty() {
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
        let mut plist: Vec<(String, Value)> = Vec::new();
        for span in &spans {
            if span.start <= start && end <= span.end {
                for (key, value) in &span.props {
                    if !plist.iter().any(|(existing, _)| existing == key) {
                        plist.push((key.clone(), *value));
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
    props: &[(String, Value)],
    prop: &str,
) -> Option<Value> {
    if prop == "category" {
        return None;
    }
    let category = props
        .iter()
        .find(|(name, _)| name == "category")
        .and_then(|(_, value)| value.as_symbol().ok())?;
    interp.get_symbol_property(category, prop)
}

pub(crate) fn property_from_props_with_category(
    interp: &Interpreter,
    props: &[(String, Value)],
    prop: &str,
) -> Option<Value> {
    // Categories and aliases can only redirect a property already present in
    // this interval.  Most buffer positions have no properties at all, so do
    // not perform a buffer-local alias lookup for an empty interval.
    if props.is_empty() {
        return None;
    }
    let direct = props
        .iter()
        .find(|(name, _)| name == prop)
        .map(|(_, value)| *value)
        .or_else(|| property_from_category_symbol(interp, props, prop));
    if direct.is_some() {
        return direct;
    }
    let aliases = interp
        .buffer_local_value(interp.current_buffer_id(), "char-property-alias-alist")
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let aliases = aliases.into_iter().find_map(|entry| {
        let key = entry.car().ok()?;
        matches!(key.kind(), Kind::Symbol(name) if name == prop)
            .then(|| entry.cdr().ok()?.to_vec().ok())?
    })?;
    aliases.into_iter().find_map(|alias| {
        let alias = alias.as_symbol().ok()?;
        props
            .iter()
            .find(|(name, _)| name == alias)
            .map(|(_, value)| *value)
            .or_else(|| property_from_category_symbol(interp, props, alias))
    })
}

pub(crate) fn buffer_property_at_with_category(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
) -> Option<Value> {
    if pos < buffer.point_min() || pos >= buffer.point_max() {
        return None;
    }
    property_from_props_with_category(interp, buffer.text_properties_at_ref(pos), prop)
}

pub(crate) fn buffer_char_property_at(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
) -> Value {
    buffer_char_property_at_with_overlay_id(interp, buffer, pos, prop).0
}

pub(crate) fn buffer_char_property_at_with_overlay_id(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
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

pub(crate) fn overlay_property_with_category(
    interp: &Interpreter,
    overlay: &crate::overlay::OverlayRef,
    prop: &str,
) -> Option<Value> {
    let direct = overlay.get_symbol_prop(prop);
    if direct.is_some() || prop == "category" {
        return direct;
    }
    let category = overlay.get_symbol_prop("category")?;
    interp.get_symbol_property(category.as_symbol().ok()?, prop)
}

pub(crate) fn string_property_at(value: &Value, pos: usize, prop: &str) -> Option<Value> {
    let string = string_like(value)?;
    string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .and_then(|span| {
            span.props
                .iter()
                .find(|(name, _)| name == prop)
                .map(|(_, value)| *value)
        })
}

pub(crate) fn string_property_at_with_category(
    interp: &Interpreter,
    value: &Value,
    pos: usize,
    prop: &str,
) -> Option<Value> {
    let string = string_like(value)?;
    let span = string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)?;
    property_from_props_with_category(interp, &span.props, prop)
}

pub(crate) fn text_property_search_buffer(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    start: usize,
    end: usize,
    prop: &str,
    wanted: &Value,
    want_match: bool,
) -> Option<usize> {
    let start = start.max(buffer.point_min());
    let end = end.min(buffer.point_max());
    if start >= end {
        return None;
    }
    for pos in start..end {
        // textprop.c's Ftext_property_any/not_all read each position
        // through textget, so `char-property-alias-alist' entries (the
        // face -> font-lock-face alias font-lock-mode installs) answer
        // here exactly as for `get-text-property'.
        // GNU compares property values with EQ, including distinct but
        // structurally equal records, strings and other aggregate values.
        let matches = values_eq_in_env(
            interp,
            &buffer_property_at_with_category(interp, buffer, pos, prop).unwrap_or(Value::Nil),
            wanted,
            &Env::new(),
        );
        if matches == want_match {
            return Some(pos);
        }
    }
    None
}

pub(crate) fn text_property_search_string(
    interp: &Interpreter,
    value: &Value,
    start: usize,
    end: usize,
    prop: &str,
    wanted: &Value,
    want_match: bool,
) -> Option<usize> {
    let len = string_text(value).ok()?.chars().count();
    let start = start.min(len);
    let end = end.min(len);
    if start >= end {
        return None;
    }
    for pos in start..end {
        let matches = values_eq_in_env(
            interp,
            &string_property_at_with_category(interp, value, pos, prop).unwrap_or(Value::Nil),
            wanted,
            &Env::new(),
        );
        if matches == want_match {
            return Some(pos);
        }
    }
    None
}

pub(crate) fn string_properties_at(value: &Value, pos: usize) -> Vec<(String, Value)> {
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
) -> Vec<(String, Value)> {
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
) -> Result<(), LispError>
where
    F: FnMut(Vec<(String, Value)>) -> Vec<(String, Value)>,
{
    let Kind::StringObject(state) = value.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *value));
    };
    let mut state = state.borrow_mut();
    let len = state.len();
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
