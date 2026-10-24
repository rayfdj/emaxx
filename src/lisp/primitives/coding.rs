use super::*;
use crate::lisp::types::Kind;

mod charset_table;
mod detect;
mod iso2022;

pub(crate) use charset_table::{CharsetInfo, charset_info};

pub(crate) use detect::DetectSource;
pub(crate) use iso2022::charset_dimension;

/// character.h MAX_UNICODE_CHAR, MAX_5_BYTE_CHAR and MAX_CHAR: the last
/// Unicode character, the last character of the `emacs' charset (raw
/// bytes follow at #x3fff80..#x3fffff) and the last character.
pub(crate) const MAX_UNICODE_CHAR: u32 = 0x10_FFFF;
pub(crate) const MAX_5_BYTE_CHAR: u32 = 0x3F_FF7F;
pub(crate) const MAX_CHAR: u32 = 0x3F_FFFF;

pub(crate) fn coding_system_error(coding: Value) -> LispError {
    // coding.c:Fcheck_coding_system signals with the actual offending
    // object, preserving even an uninterned symbol's identity.
    LispError::SignalValue(Value::list([Value::symbol("coding-system-error"), coding]))
}

fn charset_plist_property(interp: &Interpreter, charset: &str, property: &str) -> Option<Value> {
    let items = interp.charset_plist_value(charset)?.to_vec().ok()?;
    items.windows(2).find_map(|pair| {
        matches!(pair[0].kind(), Kind::Symbol(name) if name == property).then(|| pair[1])
    })
}

fn parse_charset_map_number(value: &str) -> Option<u32> {
    u32::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}

/// charset.c load_charset_map over a map file: a `FROM-TO C' line maps the
/// codes whose code-space INDEX runs from FROM's to TO's onto the
/// characters from C on, one per index (codes are not consecutive integers
/// once a dimension has gaps: GB180304.map's `0x81308130-0x81308435 0x80'
/// is 36 characters, not 774); a line whose codes fall outside the code
/// space is skipped (`from_index < 0 || to_index < 0').
fn parse_charset_map(contents: &str, bounds: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut mappings = Vec::new();
    for line in contents.lines() {
        let mut fields = line
            .split('#')
            .next()
            .unwrap_or_default()
            .split_whitespace();
        let (Some(code), Some(character)) = (fields.next(), fields.next()) else {
            continue;
        };
        let (code_start, code_end) = match code.split_once('-') {
            Some((start, end)) => {
                let (Some(start), Some(end)) = (
                    parse_charset_map_number(start),
                    parse_charset_map_number(end),
                ) else {
                    continue;
                };
                (start, end)
            }
            None => {
                let Some(code) = parse_charset_map_number(code) else {
                    continue;
                };
                (code, code)
            }
        };
        let Some(character_start) = parse_charset_map_number(character) else {
            continue;
        };
        let (Some(from_index), Some(to_index)) = (
            code_point_to_raw_index(bounds, code_start),
            code_point_to_raw_index(bounds, code_end),
        ) else {
            continue;
        };
        for (offset, index) in (from_index..=to_index).enumerate() {
            if let Some(code) = raw_index_to_code_point(bounds, index) {
                mappings.push((code, character_start + offset as u32));
            }
        }
    }
    mappings
}

/// charset.c CODE_POINT_TO_INDEX before its `char_index_offset': the
/// position of CODE in the code space BOUNDS (one (min, max) byte range
/// per dimension, least significant first), or None when some byte lies
/// outside its range.
fn code_point_to_raw_index(bounds: &[(u32, u32)], code: u32) -> Option<u32> {
    let mut index = 0u32;
    let mut stride = 1u32;
    for (dimension, (min, max)) in bounds.iter().enumerate() {
        let byte = (code >> (8 * dimension)) & 0xFF;
        if byte < *min || byte > *max {
            return None;
        }
        index = index.checked_add((byte - min).checked_mul(stride)?)?;
        stride = stride.checked_mul(max - min + 1)?;
    }
    Some(index)
}

/// charset.c INDEX_TO_CODE_POINT: the inverse of `code_point_to_raw_index'.
fn raw_index_to_code_point(bounds: &[(u32, u32)], index: u32) -> Option<u32> {
    let mut code = 0u32;
    let mut remaining = index;
    for (dimension, (min, max)) in bounds.iter().enumerate() {
        let size = max - min + 1;
        code |= (min + remaining % size) << (8 * dimension);
        remaining /= size;
    }
    (remaining == 0).then_some(code)
}

/// A `:subset (PARENT MIN MAX OFFSET)' charset's parent and window.
fn charset_subset(interp: &Interpreter, charset: &str) -> Option<(String, i64, i64, i64)> {
    charset_info(interp, charset)?.subset.clone()
}

/// A `:superset' charset's children and their offsets.
fn charset_superset(interp: &Interpreter, charset: &str) -> Option<Vec<(String, i64)>> {
    charset_info(interp, charset)?.superset.clone()
}

/// charset.c:Fmake_char assembles position codes in most-significant-byte
/// order, filling omitted dimensions from the charset's code space.
pub(crate) fn make_charset_character(
    interp: &Interpreter,
    charset: &Value,
    positions: &[Value],
) -> Result<Value, LispError> {
    let name = charset.as_symbol()?;
    let canonical = interp
        .charset_canonical_name(name)
        .ok_or_else(|| LispError::WrongTypeArgument("charsetp".into(), *charset))?;
    let info = charset_info(interp, &canonical)
        .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?;
    let bounds = info
        .code_space
        .as_deref()
        .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?;
    let dimension = info.dimension as usize;
    let first = positions.first().unwrap_or(&Value::Nil);
    let mut code = if first.is_nil() {
        let ascii_compatible = info.ascii_compatible_p.unwrap_or(matches!(
            canonical.as_str(),
            "ascii" | "iso-8859-1" | "unicode" | "emacs"
        ));
        if ascii_compatible {
            0
        } else {
            info.code_bounds
                .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?
                .0
        }
    } else {
        let mut code = 0u32;
        for index in 0..dimension {
            let position = positions.get(index).unwrap_or(&Value::Nil);
            let byte = if position.is_nil() {
                bounds[dimension - index - 1].0
            } else {
                let Kind::Integer(number) = position.kind() else {
                    return Err(LispError::WrongTypeArgument("wholenump".into(), *position));
                };
                if number < 0 {
                    return Err(LispError::WrongTypeArgument("wholenump".into(), *position));
                }
                if number >= 256 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        Value::Integer(255),
                        *position,
                    ])));
                }
                number as u32
            };
            code = (code << 8) | byte;
        }
        code
    };
    if iso2022::charset_iso_final(interp, &canonical).is_some() {
        code &= 0x7f7f7f7f;
    }
    let character = info
        .decode(interp, code)
        .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?;
    let character = if RAW_BYTE_MARKERS.contains(&character) {
        RAW_BYTE8_BASE + character - RAW_BYTE_REGEX_BASE
    } else {
        character
    };
    Ok(Value::Integer(i64::from(character)))
}

/// DECODE_CHAR: the character CODE is in CHARSET.
pub(crate) fn decode_charset_code(interp: &Interpreter, charset: &str, code: u32) -> Option<u32> {
    charset_info(interp, charset)?.decode(interp, code)
}

/// The charset's `:code-space' as (min, max) byte ranges, least
/// significant first (charset.c's CODE_POINT_TO_INDEX counts the first
/// byte fastest: jisx0208's hole 0x222F is index 108, so its non-unified
/// character is code-offset + 108, not code-offset + 0x222F).
fn charset_code_space(interp: &Interpreter, charset: &str) -> Option<Vec<(u32, u32)>> {
    charset_info(interp, charset)?.code_space.clone()
}

pub(crate) fn encode_charset_char(
    interp: &Interpreter,
    charset: &str,
    character: u32,
) -> Option<u32> {
    // CHARACTER may be a Rust-internal char (a raw byte spelled in the
    // regex-internal range) or GNU's own character number (a raw byte at
    // #x3fff80..#x3fffff, as `encode-char' and `char-charset' receive
    // it); both spellings name the same raw byte, which no code space but
    // `eight-bit' contains.
    encode_charset_character_number(interp, charset, gnu_character_number(character))
}

/// Charset membership for an actual Emacs character number. Canonical
/// string consumers must not reinterpret Unicode private-use characters
/// as the older Rust-text adapter's raw-byte sentinels.
fn encode_charset_character_number(
    interp: &Interpreter,
    charset: &str,
    character: u32,
) -> Option<u32> {
    charset_info(interp, charset)?.encode(interp, character)
}

/// coding-system-get: `plist-get' on the coding system's plist.
pub(crate) fn coding_system_property(
    interp: &Interpreter,
    coding: &str,
    property: &str,
) -> Option<Value> {
    let mut tail = interp.coding_system(coding)?.plist;
    while let Some((key, rest)) = tail.cons_values() {
        let (value, next) = rest.cons_values()?;
        if matches!(key.kind(), Kind::Symbol(name) if name == property) {
            return Some(value);
        }
        tail = next;
    }
    None
}

/// CODING_ATTR_DEFAULT_CHAR: Fdefine_coding_system_internal stores a
/// space when `:default-char' is nil.
fn coding_system_default_char(interp: &Interpreter, coding: &str) -> u32 {
    coding_system_property(interp, coding, ":default-char")
        .and_then(|value| value.integer_value())
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(u32::from(b' '))
}

fn coding_system_requires_bom(interp: &Interpreter, coding: &str) -> bool {
    coding_system_property(interp, coding, ":bom")
        .is_some_and(|value| !value.is_nil() && value.cons_values().is_none())
}

pub(crate) fn coding_system_auto_detects_bom(interp: &Interpreter, coding: &str) -> bool {
    coding_system_property(interp, coding, ":bom")
        .is_some_and(|value| value.cons_values().is_some())
}

fn coding_system_consumes_bom(interp: &Interpreter, coding: &str) -> bool {
    coding_system_requires_bom(interp, coding) || coding_system_auto_detects_bom(interp, coding)
}

fn coding_system_is_ascii_compatible(interp: &Interpreter, coding: &str) -> bool {
    coding_system_property(interp, coding, ":ascii-compatible-p")
        .is_some_and(|value| value.is_truthy())
}

fn coding_system_utf16_options(
    interp: &Interpreter,
    coding: &str,
    kind: &str,
) -> (bool, bool, bool) {
    let big_endian = match coding_system_property(interp, coding, ":endian").map(|v| v.kind()) {
        Some(Kind::Symbol(endian)) => endian != "little",
        _ => !matches!(kind, "utf-16le"),
    };
    let bom = coding_system_property(interp, coding, ":bom");
    let detect_bom = bom
        .as_ref()
        .is_some_and(|value| value.cons_values().is_some());
    let with_bom = bom.as_ref().is_some_and(|value| !value.is_nil())
        || (bom.is_none() && interp.coding_system_base_name(coding).as_deref() == Some("utf-16"));
    (big_endian, with_bom, detect_bom)
}

/// The (roman kana kanji ...) charsets of coding.c's Vsjis_coding_system,
/// for the sjis-char primitives.
pub(crate) fn sjis_primitive_charsets(interp: &Interpreter) -> Option<(String, String, String)> {
    let coding = interp.sjis_coding_system.clone();
    let charsets = coding_system_charset_names(interp, &coding);
    Some((
        charsets.first()?.clone(),
        charsets.get(1)?.clone(),
        charsets.get(2)?.clone(),
    ))
}

pub(crate) fn coding_system_charset_names(interp: &Interpreter, coding: &str) -> Vec<String> {
    let Some(charsets) = coding_system_property(interp, coding, ":charset-list") else {
        return Vec::new();
    };
    charsets
        .to_vec()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| value.symbol_str().map(str::to_string))
        .collect()
}

fn run_coding_conversion(
    interp: &mut Interpreter,
    source: &StringLike,
    function: &Value,
    pre_write: bool,
    env: &mut Env,
) -> Result<StringLike, LispError> {
    if function.is_nil() {
        return Ok(source.clone());
    }
    let saved_buffer_id = interp.current_buffer_id();
    let base_name = " *code-conversion-work*";
    let temp_name = if interp.has_buffer(base_name) {
        let mut suffix = 2;
        loop {
            let candidate = format!("{base_name}<{suffix}>");
            if !interp.has_buffer(&candidate) {
                break candidate;
            }
            suffix += 1;
        }
    } else {
        base_name.into()
    };
    let (temp_id, _) = interp.create_buffer(&temp_name);
    interp.set_buffer_hooks_inhibited(temp_id, true);
    interp.set_current_buffer_id(temp_id)?;
    interp.buffer.borrow_mut().set_multibyte(source.multibyte);
    interp.insert_current_buffer(&source.text);
    interp.set_inserted_extended_chars(1, &source.extended_chars);
    for span in &source.props {
        interp
            .buffer
            .borrow_mut()
            .graft_text_properties(1 + span.start, 1 + span.end, &span.props);
    }
    // GNU invokes a post-read conversion with point at the beginning of the
    // newly decoded span.  Lisp decoders such as `utf-7-decode' consume LEN
    // bytes starting at point, so leaving point after the insertion silently
    // turns the conversion into a no-op.
    if !pre_write {
        {
            let mut buffer = interp.buffer.borrow_mut();
            let position = buffer.point_min();
            buffer.goto_char(position);
        }
    }
    let arguments = if pre_write {
        vec![
            Value::Integer(interp.buffer.borrow().point_min() as i64),
            Value::Integer(interp.buffer.borrow().point_max() as i64),
        ]
    } else {
        // coding.c:decode_coding_object passes produced_char, not bytes.
        // The conversion buffer already records the decoded character count.
        vec![Value::Integer(interp.buffer.borrow().buffer_size() as i64)]
    };
    let result = interp.call_function_value(*function, None, &arguments, env);
    let result_buffer_id = interp.current_buffer_id();
    let converted = {
        let buffer = interp.buffer.borrow();
        StringLike {
            text: buffer.buffer_string(),
            props: Vec::new(),
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(buffer.point_min(), buffer.point_max()),
        }
    };
    let _ = interp.set_current_buffer_id(saved_buffer_id);
    if result_buffer_id != saved_buffer_id && result_buffer_id != temp_id {
        interp.kill_buffer_id(result_buffer_id);
    }
    interp.kill_buffer_id(temp_id);
    result?;
    Ok(converted)
}

/// encode_coding_charset: an ASCII character as itself when the coding is
/// ASCII-compatible, an eight-bit one as its byte, any other through the
/// first charset of the coding's list that encodes it (one to four bytes
/// by the charset's dimension), and else the default character, whose
/// low eight bits EMIT_ONE_BYTE stores.
fn encode_coding_charset(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let charsets: Vec<std::rc::Rc<CharsetInfo>> = coding_system_charset_names(interp, coding)
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect();
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    let default_char = coding_system_default_char(interp, coding) as u8;
    let mut encoded = Vec::with_capacity(codes.len());
    for &c in codes {
        if ascii_compatible && c < 0x80 {
            encoded.push(c as u8);
        } else if char_byte8_p(c) {
            encoded.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            match charsets.iter().find_map(|charset| {
                charset
                    .encode(interp, c)
                    .map(|code| (charset.dimension, code))
            }) {
                Some((dimension, code)) => {
                    let bytes = code.to_be_bytes();
                    encoded.extend_from_slice(&bytes[4 - dimension.clamp(1, 4) as usize..]);
                }
                None => encoded.push(default_char),
            }
        }
    }
    encoded
}

/// CODING_CHAR_CHARSET over a coding's charset list: the first charset
/// that encodes C, with C's code point in it.
fn coding_char_charset<'a>(
    interp: &Interpreter,
    charsets: &'a [std::rc::Rc<CharsetInfo>],
    c: u32,
) -> Option<(&'a CharsetInfo, u32)> {
    charsets
        .iter()
        .find_map(|charset| charset.encode(interp, c).map(|code| (&**charset, code)))
}

/// coding.c decode_coding_charset: each first byte selects, through the
/// `charset_valids' table, the charset (or the dimension-sorted charsets)
/// whose following bytes complete the code; a byte outside the table or a
/// code the charset does not decode comes through as itself, and every
/// character of a non-ASCII charset is annotated with it.
fn decode_charset_coding_bytes(interp: &Interpreter, bytes: &[u8], coding: &str) -> DecodedText {
    let valids = charset_valids(interp, coding);
    let mut out = DecodedText::new();
    let mut runs = CharsetRuns::new();
    let mut p = 0usize;
    while p < bytes.len() {
        let src_base = p;
        let c = bytes[p];
        p += 1;
        let decoded = valids[usize::from(c)].as_ref().and_then(|charsets| {
            let mut code = u32::from(c);
            let mut len = 1usize;
            for charset in charsets {
                let dim = charset.dimension as usize;
                while len < dim {
                    let &next = bytes.get(p)?;
                    p += 1;
                    code = (code << 8) | u32::from(next);
                    len += 1;
                }
                if let Some(ch) = charset.decode(interp, code) {
                    return Some((charset, ch));
                }
            }
            None
        });
        match decoded {
            Some((charset, ch)) => {
                runs.note(&charset.name, out.char_count());
                out.push_code(ch);
            }
            None => {
                // invalid_code: the first byte as itself, resynchronize.
                p = src_base + 1;
                out.push_byte(c);
            }
        }
    }
    out.charsets = runs.finish(out.char_count());
    out
}

pub(crate) fn checked_coding_name(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<String>, LispError> {
    if value.is_nil() {
        return Ok(None);
    }
    let symbol = value.as_symbol()?.to_string();
    interp
        .coding_system_canonical_name(&symbol)
        .ok_or_else(|| coding_system_error(*value))
        .map(Some)
}

/// A coding system VALUE as given (an alias keeps its name), once
/// CHECK_CODING_SYSTEM accepts it; None for nil.
pub(crate) fn checked_coding_given(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<String>, LispError> {
    Ok(checked_coding_name(interp, value)?
        .map(|_| value.symbol_str().map(str::to_string).unwrap_or_default()))
}

/// coding.c raw_text_coding_system: what a unibyte buffer reads CODING
/// as -- a raw-text coding stays, one with an undecided eol becomes
/// `raw-text', and any other the raw-text subsidiary of its eol type.
pub(crate) fn raw_text_coding_system(interp: &Interpreter, coding: &str) -> String {
    if interp.coding_system_kind_name(coding).as_deref() == Some("raw-text") {
        return coding.to_string();
    }
    match coding_eol_type(interp, coding) {
        None => "raw-text".into(),
        Some(eol) => coding_variant_name(interp, "raw-text", Some(eol)),
    }
}

pub(crate) fn checked_coding_symbol(
    interp: &Interpreter,
    value: &Value,
) -> Result<String, LispError> {
    checked_coding_name(interp, value)?.ok_or_else(|| coding_system_error(*value))
}

pub(crate) fn coding_variant_name(
    interp: &Interpreter,
    base: &str,
    eol_type: Option<i64>,
) -> String {
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

pub(crate) fn set_last_coding_system_used(interp: &mut Interpreter, coding: &str, env: &mut Env) {
    interp.set_variable(
        "last-coding-system-used",
        Value::Symbol(coding.to_string().into()),
        env,
    );
}

pub(crate) fn shared_string_copy(value: &Value) -> Result<Value, LispError> {
    // fns.c:Fcopy_sequence copies the actual payload and intervals. A Rust
    // text projection cannot preserve all Lisp characters or byte8 identity.
    // Its ordinary constructors return the normal empty singleton even
    // when the source is a distinct pure or restored zero-length header.
    let Kind::StringObject(object) = value.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *value));
    };
    let copy = object.borrow().copy_with_properties();
    Ok(Value::StringObject(copy))
}

pub(crate) fn bytes_to_unibyte_value(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::String("".into());
    }
    bytes_to_shared_unibyte_value(bytes)
}

pub(crate) fn bytes_to_shared_unibyte_value(bytes: &[u8]) -> Value {
    Value::StringObject(StringObjectRef::from_unibyte(bytes.to_vec()))
}

/// gen_tempname's random segment: exactly six characters from
/// [a-zA-Z0-9].  Both `make-temp-name' and `make-temp-file-internal'
/// draw from here; the SHAPE is observable (tests embed fixture paths in
/// compared output), so the six-character form matters, not just
/// uniqueness.
pub(crate) fn random_temp_suffix() -> String {
    const LETTERS: &[u8; 62] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let counter = TEMP_NAME_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut seed =
        (nanos as u64) ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ rand_simple().unsigned_abs();
    let mut suffix = String::with_capacity(6);
    for _ in 0..6 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        suffix.push(char::from(LETTERS[(seed >> 33) as usize % 62]));
    }
    suffix
}

pub(crate) fn make_temp_name(prefix: &str) -> String {
    // fileio.c Fmake_temp_name -> gen_tempname (GT_NOCREATE): PREFIX plus
    // the six-character random segment, regenerated while a file of that
    // name exists.
    let mut candidate = String::new();
    for _ in 0..62 * 62 {
        candidate = format!("{prefix}{}", random_temp_suffix());
        if !std::path::Path::new(&candidate).exists() {
            return candidate;
        }
    }
    candidate
}

pub(crate) fn aset_vector_value(
    target: &Value,
    index: usize,
    new_value: Value,
) -> Result<(), LispError> {
    let Kind::Vector(vector) = target.kind() else {
        return Err(LispError::WrongTypeArgument("arrayp".into(), *target));
    };
    if index >= vector.len() {
        return Err(LispError::Signal("Args out of range".into()));
    }
    vector.set(index, new_value);
    Ok(())
}

pub(crate) fn base64_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C)
}

pub(crate) fn base64_char_value(byte: u8, base64url: bool) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' if !base64url => Some(62),
        b'/' if !base64url => Some(63),
        b'-' if base64url => Some(62),
        b'_' if base64url => Some(63),
        _ => None,
    }
}

pub(crate) fn base64_value_char(value: u8, base64url: bool) -> char {
    const STANDARD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let alphabet = if base64url { URL } else { STANDARD };
    alphabet[value as usize] as char
}

pub(crate) fn encode_base64_bytes(
    bytes: &[u8],
    line_break: bool,
    pad: bool,
    base64url: bool,
) -> String {
    const GROUPS_PER_LINE: usize = 76 / 4;

    let mut encoded = String::with_capacity(bytes.len() + (bytes.len() / 3) + 8);
    let mut cursor = 0usize;
    let mut groups_on_line = 0usize;

    while cursor < bytes.len() {
        if line_break && groups_on_line == GROUPS_PER_LINE {
            encoded.push('\n');
            groups_on_line = 0;
        }

        let first = bytes[cursor];
        cursor += 1;
        encoded.push(base64_value_char(first >> 2, base64url));
        let mut value = (first & 0x03) << 4;

        if cursor == bytes.len() {
            encoded.push(base64_value_char(value, base64url));
            if pad {
                encoded.push('=');
                encoded.push('=');
            }
            break;
        }

        let second = bytes[cursor];
        cursor += 1;
        encoded.push(base64_value_char(value | (second >> 4), base64url));
        value = (second & 0x0F) << 2;

        if cursor == bytes.len() {
            encoded.push(base64_value_char(value, base64url));
            if pad {
                encoded.push('=');
            }
            break;
        }

        let third = bytes[cursor];
        cursor += 1;
        encoded.push(base64_value_char(value | (third >> 6), base64url));
        encoded.push(base64_value_char(third & 0x3F, base64url));
        groups_on_line += 1;
    }

    encoded
}

pub(crate) fn encode_base64_source_bytes(
    text: &str,
    multibyte: bool,
) -> Result<Vec<u8>, LispError> {
    if !multibyte {
        return encode_raw_text_bytes(text);
    }

    let mut bytes = Vec::with_capacity(text.chars().count());
    for ch in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            bytes.push(byte);
        } else if (ch as u32) <= 0x7F {
            bytes.push(ch as u8);
        } else {
            return Err(LispError::Signal("Character cannot be encoded".into()));
        }
    }
    Ok(bytes)
}

pub(crate) fn base64_encode_string_value(
    value: &Value,
    line_break: bool,
    pad: bool,
    base64url: bool,
) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let bytes = encode_base64_source_bytes(&string.text, string.multibyte)?;
    Ok(Value::String(
        encode_base64_bytes(&bytes, line_break, pad, base64url).into(),
    ))
}

pub(crate) fn base64_encode_region_value(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    line_break: bool,
    pad: bool,
    base64url: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    let text = interp
        .buffer
        .borrow()
        .buffer_substring(lo, hi)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let bytes = encode_base64_source_bytes(&text, interp.buffer.borrow().is_multibyte())?;
    let encoded = encode_base64_bytes(&bytes, line_break, pad, base64url);
    let inserted = insert_then_delete_region(interp, lo, hi, &encoded, env)?;
    Ok(Value::Integer(inserted as i64))
}

/// fns.c's base64 region edit: the new text inserted before the old
/// (keeping the markers with it), then the old deleted, each a change of
/// its own; point is restored when outside the region, else at its start.
/// The characters inserted.
fn insert_then_delete_region(
    interp: &mut Interpreter,
    lo: usize,
    hi: usize,
    text: &str,
    env: &mut Env,
) -> Result<usize, LispError> {
    let old_pos = interp.buffer.borrow().point();
    interp.buffer.borrow_mut().goto_char(lo);
    insert_text_with_hooks(interp, text, &[], &[], false, false, env)?;
    let inserted = text.chars().count();
    delete_region_with_hooks(interp, lo + inserted, hi + inserted, env)?;
    let pos = if old_pos >= hi {
        old_pos + inserted - (hi - lo)
    } else if old_pos > lo {
        lo
    } else {
        old_pos
    };
    let zv = interp.buffer.borrow().point_max();
    interp.buffer.borrow_mut().goto_char(pos.min(zv));
    Ok(inserted)
}

pub(crate) fn next_base64_byte(
    bytes: &[u8],
    cursor: &mut usize,
    base64url: bool,
    ignore_invalid: bool,
) -> Result<Option<u8>, LispError> {
    while let Some(&byte) = bytes.get(*cursor) {
        *cursor += 1;
        if base64_whitespace(byte) {
            continue;
        }
        if let Some(value) = base64_char_value(byte, base64url) {
            return Ok(Some(value));
        }
        if ignore_invalid {
            continue;
        }
        return Err(LispError::Signal("Invalid base64 data".into()));
    }
    Ok(None)
}

pub(crate) fn next_base64_tail(
    bytes: &[u8],
    cursor: &mut usize,
    base64url: bool,
    ignore_invalid: bool,
) -> Result<Option<Base64Tail>, LispError> {
    while let Some(&byte) = bytes.get(*cursor) {
        *cursor += 1;
        if base64_whitespace(byte) {
            continue;
        }
        if let Some(value) = base64_char_value(byte, base64url) {
            return Ok(Some(Base64Tail::Value(value)));
        }
        if !ignore_invalid && byte == b'=' {
            return Ok(Some(Base64Tail::Padding));
        }
        if ignore_invalid {
            continue;
        }
        return Err(LispError::Signal("Invalid base64 data".into()));
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Base64Tail {
    Value(u8),
    Padding,
}

pub(crate) fn decode_base64_string_value(
    value: &Value,
    base64url: bool,
    ignore_invalid: bool,
) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let bytes = encode_raw_text_bytes(&string.text)?;
    let mut cursor = 0usize;
    let mut decoded = Vec::with_capacity((bytes.len() / 4) * 3);

    while let Some(first) = next_base64_byte(&bytes, &mut cursor, base64url, ignore_invalid)? {
        let Some(second) = next_base64_byte(&bytes, &mut cursor, base64url, ignore_invalid)? else {
            return Err(LispError::Signal("Invalid base64 data".into()));
        };
        decoded.push((first << 2) | (second >> 4));

        match next_base64_tail(&bytes, &mut cursor, base64url, ignore_invalid)? {
            Some(Base64Tail::Value(third)) => {
                decoded.push(((second & 0x0F) << 4) | (third >> 2));
                match next_base64_tail(&bytes, &mut cursor, base64url, ignore_invalid)? {
                    Some(Base64Tail::Value(fourth)) => {
                        decoded.push(((third & 0x03) << 6) | fourth);
                    }
                    Some(Base64Tail::Padding) => {}
                    None if base64url || ignore_invalid => break,
                    None => return Err(LispError::Signal("Invalid base64 data".into())),
                }
            }
            Some(Base64Tail::Padding) => {
                match next_base64_tail(&bytes, &mut cursor, base64url, ignore_invalid)? {
                    Some(Base64Tail::Padding) => {}
                    _ => return Err(LispError::Signal("Invalid base64 data".into())),
                }
            }
            None if base64url || ignore_invalid => break,
            None => return Err(LispError::Signal("Invalid base64 data".into())),
        }
    }

    Ok(bytes_to_unibyte_value(&decoded))
}

pub(crate) fn base64_decode_region_value(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    base64url: bool,
    ignore_invalid: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    let text = interp
        .buffer
        .borrow()
        .buffer_substring(lo, hi)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let decoded =
        decode_base64_string_value(&Value::String(text.into()), base64url, ignore_invalid)?;
    let decoded_text = string_text(&decoded)?;
    let inserted = insert_then_delete_region(interp, lo, hi, &decoded_text, env)?;
    Ok(Value::Integer(inserted as i64))
}

pub(crate) fn strip_utf8_bom(bytes: &[u8]) -> (bool, &[u8]) {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        (true, &bytes[3..])
    } else {
        (false, bytes)
    }
}

pub(crate) fn encode_raw_text_bytes(text: &str) -> Result<Vec<u8>, LispError> {
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

/// Bytes stored by a multibyte Emacs buffer when conversion is disabled.
/// Ordinary characters use Emacs's UTF-8-compatible internal spelling;
/// byte8 marker characters remain their original single octet.  Treating
/// every Latin-1 character as a byte corrupts no-conversion `.elc' output
/// (for example U+00A7 must be C2 A7, not A7).
pub(crate) fn encode_internal_multibyte_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::with_capacity(text.len());
    for ch in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            bytes.push(byte);
        } else if ch == json::INVALID_UNICODE_SENTINEL {
            return Err(LispError::TypeError("character".into(), "string".into()));
        } else {
            let mut encoded = [0u8; 4];
            bytes.extend_from_slice(ch.encode_utf8(&mut encoded).as_bytes());
        }
    }
    Ok(bytes)
}

pub(crate) fn encode_utf8_bytes(text: &str, with_bom: bool) -> Result<Vec<u8>, LispError> {
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

/// coding.h:567 SJIS_TO_JIS.
pub(crate) fn sjis_to_jis(code: u32) -> u32 {
    let (s1, s2) = (code >> 8, code & 0xFF);
    let (j1, j2) = if s2 >= 0x9F {
        (s1 * 2 - if s1 >= 0xE0 { 0x160 } else { 0xE0 }, s2 - 0x7E)
    } else {
        (
            s1 * 2 - if s1 >= 0xE0 { 0x161 } else { 0xE1 },
            s2 - if s2 >= 0x7F { 0x20 } else { 0x1F },
        )
    };
    (j1 << 8) | j2
}

/// coding.h:608 JIS_TO_SJIS.  Like GNU, this is applied blindly to
/// whatever code the charset search returned -- a katakana-jisx0201
/// code 0x31 becomes 0x70AF through the same arithmetic, and the
/// oracle's encode-sjis-char answers exactly that.
pub(crate) fn jis_to_sjis(code: u32) -> u32 {
    let (j1, j2) = (code >> 8, code & 0xFF);
    let (s1, s2) = if j1 & 1 != 0 {
        (
            j1 / 2 + if j1 < 0x5F { 0x71 } else { 0xB1 },
            j2 + if j2 >= 0x60 { 0x20 } else { 0x1F },
        )
    } else {
        (j1 / 2 + if j1 < 0x5F { 0x70 } else { 0xB0 }, j2 + 0x7E)
    };
    (s1 << 8) | s2
}

/// coding.h JIS_TO_SJIS2, for the fourth (JIS X 0213 plane 2) charset.
fn jis_to_sjis2(code: u32) -> u32 {
    let (j1, j2) = (code >> 8, code & 0xFF);
    let (s1, s2) = if j1 & 1 != 0 {
        (
            if j1 <= 0x25 {
                0xF0 + (j1 - 0x21) / 2
            } else if j1 <= 0x2F {
                0xF3 + (j1 - 0x2D) / 2
            } else {
                0xF5 + (j1 - 0x6F) / 2
            },
            j2 + if j2 >= 0x60 { 0x20 } else { 0x1F },
        )
    } else {
        (
            match j1 {
                0x28 => 0xF0,
                0x24 => 0xF1,
                0x2C => 0xF2,
                0x2E => 0xF3,
                _ => 0xF4 + (j1 - 0x6E) / 2,
            },
            j2 + 0x7E,
        )
    };
    (s1 << 8) | s2
}

/// encode_coding_sjis over the coding's (roman kana kanji [kanji2])
/// charsets: an ASCII character as itself when the coding is
/// ASCII-compatible, an eight-bit one as its byte; otherwise the charset
/// list's first charset for the character, or for the default character
/// when none has it: kanji through JIS_TO_SJIS, kana as its code + 0x80,
/// kanji2 through JIS_TO_SJIS2 in the rows JIS X 0213 plane 2 occupies,
/// anything else as its low seven bits.
fn encode_coding_sjis(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let names = coding_system_charset_names(interp, coding);
    let charsets: Vec<std::rc::Rc<CharsetInfo>> = names
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect();
    let kana = names.get(1).map(String::as_str);
    let kanji = names.get(2).map(String::as_str);
    let kanji2 = names.get(3).map(String::as_str);
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    let default_char = coding_system_default_char(interp, coding);
    let mut out = Vec::with_capacity(codes.len());
    for &c in codes {
        if c < 0x80 && ascii_compatible {
            out.push(c as u8);
        } else if char_byte8_p(c) {
            out.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            let Some((charset, code)) = coding_char_charset(interp, &charsets, c)
                .or_else(|| coding_char_charset(interp, &charsets, default_char))
            else {
                continue;
            };
            let name = Some(charset.name.as_str());
            if name == kanji {
                let sjis = jis_to_sjis(code);
                out.extend([(sjis >> 8) as u8, (sjis & 0xFF) as u8]);
            } else if name == kana {
                out.push((code | 0x80) as u8);
            } else if kanji2.is_some() && name == kanji2 {
                let c1 = code >> 8;
                if c1 == 0x21
                    || (0x23..=0x25).contains(&c1)
                    || c1 == 0x28
                    || (0x2C..=0x2F).contains(&c1)
                    || c1 >= 0x6E
                {
                    let sjis = jis_to_sjis2(code);
                    out.extend([(sjis >> 8) as u8, (sjis & 0xFF) as u8]);
                } else {
                    out.push((code & 0x7F) as u8);
                }
            } else {
                out.push((code & 0x7F) as u8);
            }
        }
    }
    out
}

/// coding.h SJIS_TO_JIS2 for the fourth (JIS X 0213 plane 2) charset.
fn sjis_to_jis2(code: u32) -> u32 {
    let (s1, s2) = (code >> 8, code & 0xFF);
    let (j1, j2) = if s2 >= 0x9F {
        (
            match s1 {
                0xF0 => 0x28,
                0xF1 => 0x24,
                0xF2 => 0x2C,
                0xF3 => 0x2E,
                _ => 0x6E + (s1 - 0xF4) * 2,
            },
            s2 - 0x7E,
        )
    } else {
        (
            if s1 <= 0xF2 {
                0x21 + (s1 - 0xF0) * 2
            } else if s1 <= 0xF4 {
                0x2D + (s1 - 0xF3) * 2
            } else {
                0x6F + (s1 - 0xF5) * 2
            },
            s2 - if s2 >= 0x7F { 0x20 } else { 0x1F },
        )
    };
    (j1 << 8) | j2
}

/// coding.c decode_coding_sjis over the coding system's (roman kana kanji
/// [kanji2]) charsets: 0xA1..0xDF is halfwidth katakana, 0x81..0x9F and
/// 0xE0..0xEF lead a two-byte code (trail 0x40..0xFC except 0x7F) decoded
/// through SJIS_TO_JIS, 0xF0..0xFC one through SJIS_TO_JIS2 when a fourth
/// charset exists; anything else is a raw byte and the scan
/// resynchronizes.  An unmapped code decodes in GNU to a supra-Unicode
/// codepoint, which emaxx strings cannot hold: those fall back to
/// raw-byte markers (the ledger's disclosed limitation).
pub(crate) fn decode_sjis_bytes(interp: &Interpreter, bytes: &[u8], coding: &str) -> DecodedText {
    let charsets = coding_system_charset_names(interp, coding);
    let roman = charsets.first().cloned().unwrap_or_else(|| "ascii".into());
    let kana = charsets
        .get(1)
        .cloned()
        .unwrap_or_else(|| "katakana-jisx0201".into());
    let kanji = charsets
        .get(2)
        .cloned()
        .unwrap_or_else(|| "japanese-jisx0208".into());
    let kanji2 = charsets.get(3).cloned();
    let mut out = DecodedText::new();
    let mut runs = CharsetRuns::new();
    let mut rest = bytes;
    let trail_ok = |trail: u8| (0x40..=0xFC).contains(&trail) && trail != 0x7F;
    while let Some((&lead, tail)) = rest.split_first() {
        let (decoded, consumed): (Option<(&str, u32)>, usize) = match lead {
            0x00..=0x7F => (
                decode_charset_code(interp, &roman, u32::from(lead)).map(|ch| (roman.as_str(), ch)),
                1,
            ),
            0x80 | 0xA0 => (None, 1),
            0xA1..=0xDF => (
                decode_charset_code(interp, &kana, u32::from(lead & 0x7F))
                    .map(|ch| (kana.as_str(), ch)),
                1,
            ),
            0x81..=0xEF => match tail.first() {
                Some(&trail) if trail_ok(trail) => (
                    decode_charset_code(
                        interp,
                        &kanji,
                        sjis_to_jis(u32::from(lead) << 8 | u32::from(trail)),
                    )
                    .map(|ch| (kanji.as_str(), ch)),
                    2,
                ),
                _ => (None, 1),
            },
            0xF0..=0xFC if kanji2.is_some() => match tail.first() {
                Some(&trail) if trail_ok(trail) => {
                    let kanji2 = kanji2.as_deref().unwrap_or_default();
                    (
                        decode_charset_code(
                            interp,
                            kanji2,
                            sjis_to_jis2(u32::from(lead) << 8 | u32::from(trail)),
                        )
                        .map(|ch| (kanji2, ch)),
                        2,
                    )
                }
                _ => (None, 1),
            },
            _ => (None, 1),
        };
        match decoded {
            Some((charset, ch)) => {
                runs.note(charset, out.char_count());
                out.push_code(ch);
                rest = &rest[consumed..];
            }
            None => {
                out.push_byte(lead);
                rest = &rest[1..];
            }
        }
    }
    out.charsets = runs.finish(out.char_count());
    out
}

/// encode_coding_big5 over the coding's (roman big5) charsets: an ASCII
/// character as itself when the coding is ASCII-compatible, an eight-bit
/// one as its byte; otherwise the list's first charset for the character
/// or else for the default character: big5 as its two-byte code, any
/// other charset as the code's low seven bits.
fn encode_coding_big5(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let names = coding_system_charset_names(interp, coding);
    let charsets: Vec<std::rc::Rc<CharsetInfo>> = names
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect();
    let big5 = names.get(1).map(String::as_str);
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    let default_char = coding_system_default_char(interp, coding);
    let mut out = Vec::with_capacity(codes.len());
    for &c in codes {
        if c < 0x80 && ascii_compatible {
            out.push(c as u8);
        } else if char_byte8_p(c) {
            out.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            let Some((charset, code)) = coding_char_charset(interp, &charsets, c)
                .or_else(|| coding_char_charset(interp, &charsets, default_char))
            else {
                continue;
            };
            if Some(charset.name.as_str()) == big5 {
                out.extend([(code >> 8) as u8, (code & 0xFF) as u8]);
            } else {
                out.push((code & 0x7F) as u8);
            }
        }
    }
    out
}

/// coding.c decode_coding_big5 over the coding system's (roman big5)
/// charsets: a lead 0xA1..0xFE with trail 0x40..0x7E or 0xA1..0xFE decodes
/// through the second charset; anything else is a raw byte and the scan
/// resynchronizes.
pub(crate) fn decode_big5_bytes(interp: &Interpreter, bytes: &[u8], coding: &str) -> DecodedText {
    let charsets = coding_system_charset_names(interp, coding);
    let roman = charsets.first().cloned().unwrap_or_else(|| "ascii".into());
    let big5 = charsets.get(1).cloned().unwrap_or_else(|| "big5".into());
    let mut out = DecodedText::new();
    let mut runs = CharsetRuns::new();
    let mut rest = bytes;
    while let Some((&lead, tail)) = rest.split_first() {
        let (decoded, consumed): (Option<(&str, u32)>, usize) = match lead {
            0x00..=0x7F => (
                decode_charset_code(interp, &roman, u32::from(lead)).map(|ch| (roman.as_str(), ch)),
                1,
            ),
            0xA1..=0xFE => match tail.first() {
                Some(&trail)
                    if (0x40..=0x7E).contains(&trail) || (0xA1..=0xFE).contains(&trail) =>
                {
                    (
                        decode_charset_code(interp, &big5, u32::from(lead) << 8 | u32::from(trail))
                            .map(|ch| (big5.as_str(), ch)),
                        2,
                    )
                }
                _ => (None, 1),
            },
            _ => (None, 1),
        };
        match decoded {
            Some((charset, ch)) => {
                runs.note(charset, out.char_count());
                out.push_code(ch);
                rest = &rest[consumed..];
            }
            None => {
                out.push_byte(lead);
                rest = &rest[1..];
            }
        }
    }
    out.charsets = runs.finish(out.char_count());
    out
}

/// coding.c decode_coding_raw_text: each byte as itself, a non-ASCII one
/// as its eight-bit character.
/// The length of the ASCII run BYTES starts with, a word at a time.
fn ascii_prefix_len(bytes: &[u8]) -> usize {
    let mut i = 0;
    while let Some(word) = bytes.get(i..i + 8) {
        let word = u64::from_ne_bytes(word.try_into().expect("eight bytes"));
        if word & 0x8080_8080_8080_8080 != 0 {
            break;
        }
        i += 8;
    }
    while bytes.get(i).is_some_and(u8::is_ascii) {
        i += 1;
    }
    i
}

pub(crate) fn decode_coding_raw_text(bytes: &[u8]) -> DecodedText {
    let mut out = DecodedText::new();
    out.text.reserve(bytes.len());
    let mut rest = bytes;
    while !rest.is_empty() {
        let ascii = ascii_prefix_len(rest);
        out.push_ascii(&rest[..ascii]);
        rest = &rest[ascii..];
        if let Some((&byte, after)) = rest.split_first() {
            out.push_byte(byte);
            rest = after;
        }
    }
    out
}

pub(crate) fn decode_raw_text_bytes(bytes: &[u8]) -> String {
    decode_coding_raw_text(bytes).text
}

/// coding.c decode_coding_utf_8 after its BOM check: the standard
/// sequences, and Emacs's four-octet ones up to #x1FFFFF and five-octet
/// ones from #x200000 to MAX_CHAR; an overlong sequence, a surrogate or
/// any other invalid octet is that octet's eight-bit character, decoding
/// resuming after it.
pub(crate) fn decode_coding_utf_8(bytes: &[u8]) -> DecodedText {
    let mut out = DecodedText::new();
    out.text.reserve(bytes.len());
    let mut p = 0;
    while p < bytes.len() {
        // The standard sequences, a valid run at a time.
        let valid_up_to = match std::str::from_utf8(&bytes[p..]) {
            Ok(_) => bytes.len() - p,
            Err(error) => error.valid_up_to(),
        };
        // SAFETY: from_utf8 validated these bytes.
        out.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[p..p + valid_up_to]) });
        p += valid_up_to;
        let Some(&c1) = bytes.get(p) else {
            break;
        };
        let extra = |index: usize| {
            bytes
                .get(p + index)
                .copied()
                .filter(|byte| (0x80..0xC0).contains(byte))
                .map(|byte| u32::from(byte & 0x3F))
        };
        let longer = if c1 & 0xF8 == 0xF0 {
            (|| {
                let c =
                    (u32::from(c1 & 0x7) << 18) | (extra(1)? << 12) | (extra(2)? << 6) | extra(3)?;
                (c >= 0x10000).then_some((c, 4))
            })()
        } else if c1 & 0xFC == 0xF8 {
            (|| {
                let c = (u32::from(c1 & 0x3) << 24)
                    | (extra(1)? << 18)
                    | (extra(2)? << 12)
                    | (extra(3)? << 6)
                    | extra(4)?;
                (0x20_0000..=MAX_CHAR).contains(&c).then_some((c, 5))
            })()
        } else {
            None
        };
        match longer {
            Some((c, length)) => {
                out.push_code(c);
                p += length;
            }
            None => {
                // invalid_code.
                out.push_byte(c1);
                p += 1;
            }
        }
    }
    out
}

pub(crate) fn decode_utf8_bytes(bytes: &[u8]) -> String {
    decode_coding_utf_8(bytes).text
}

/// coding.c consume_chars: the character codes an encoder reads from
/// SOURCE.  A multibyte source gives its characters, an eight-bit one as
/// #x3FFF80..#x3FFFFF.  A unibyte source gives the raw-text encoder its
/// bytes; every other encoder gets each internal multibyte sequence the
/// bytes spell as that character and the remaining bytes as eight-bit
/// characters.  A newline becomes CR LF under a dos EOL_TYPE and CR under
/// mac; SELECTIVE_DISPLAY (write-region in a buffer whose
/// `selective-display' is t) reads a CR as a newline first.  Then
/// TRANSLATION, get_translation_table's tables for the encoder (none for
/// the raw-text one), maps each character.  PREFERRED, the `charset'
/// property of each source character, comes back aligned with the codes
/// (empty when PREFERRED is).
fn consume_chars(
    source: &StringLike,
    eol_type: Option<i64>,
    raw_text_encoder: bool,
    selective_display: bool,
    translation: &[Value],
    preferred: &[Option<String>],
) -> (Vec<u32>, Vec<Option<String>>) {
    // Each character as the source spells it, with the index of the
    // source position it starts at.
    let mut chars: Vec<(u32, usize)> = Vec::with_capacity(source.text.len());
    if source.multibyte {
        chars.extend(
            source
                .character_codes_iter()
                .enumerate()
                .map(|(index, code)| (code as u32, index)),
        );
    } else {
        let bytes: Vec<u8> = source
            .text
            .chars()
            .map(|ch| raw_byte_from_regex_char(ch).unwrap_or(ch as u8))
            .collect();
        if raw_text_encoder {
            chars.extend(
                bytes
                    .iter()
                    .enumerate()
                    .map(|(index, &byte)| (u32::from(byte), index)),
            );
        } else {
            let mut pos = 0;
            while pos < bytes.len() {
                let len = crate::lisp::types::string_data::multibyte_length(&bytes[pos..], true);
                if len > 0 {
                    let (c, _) =
                        crate::lisp::types::string_data::decode_character(&bytes[pos..pos + len])
                            .expect("multibyte_length validated the sequence");
                    chars.push((c, pos));
                    pos += len;
                } else {
                    // BYTE8_TO_CHAR.
                    chars.push((RAW_BYTE8_BASE + u32::from(bytes[pos]), pos));
                    pos += 1;
                }
            }
        }
    }
    let translation = if raw_text_encoder { &[] } else { translation };
    let max_lookup = translation_max_lookup(translation);
    let annotate = !preferred.is_empty();
    let mut codes = Vec::with_capacity(chars.len());
    let mut aligned = Vec::with_capacity(if annotate { chars.len() } else { 0 });
    let mut next = 0;
    while next < chars.len() {
        let (mut c, index) = chars[next];
        next += 1;
        if c == u32::from(b'\r') && selective_display {
            c = u32::from(b'\n');
        }
        let preference = annotate.then(|| preferred.get(index).cloned().flatten());
        if c == u32::from(b'\n') {
            match eol_type {
                Some(1) => {
                    codes.push(u32::from(b'\r'));
                    if let Some(preference) = &preference {
                        aligned.push(preference.clone());
                    }
                }
                Some(2) => c = u32::from(b'\r'),
                _ => {}
            }
        }
        // LOOKUP_TRANSLATION_TABLE: a character entry replaces C and the
        // next table sees it; any other entry is a sequence translation.
        let mut trans = Value::Nil;
        for table in translation {
            if let Kind::CharTable(table) = table.kind() {
                trans = table.get(c);
                if let Some(to) = translation_character(trans) {
                    c = to;
                    trans = Value::Nil;
                } else if !trans.is_nil() {
                    break;
                }
            }
        }
        let start = codes.len();
        if trans.is_nil() {
            codes.push(c);
        } else {
            let lookup: Vec<u32> = std::iter::once(c)
                .chain(chars[next..].iter().map(|&(code, _)| code))
                .take(max_lookup)
                .collect();
            match get_translation(trans, &lookup) {
                Some((to, from_nchars)) => {
                    if let Some(to) = translation_character(to) {
                        codes.push(to);
                    } else if let Kind::Vector(items) = to.kind() {
                        codes.extend(
                            items
                                .slots()
                                .filter_map(|item| item.integer_value())
                                .map(|item| item as u32),
                        );
                    } else {
                        codes.push(c);
                    }
                    next += from_nchars - 1;
                }
                // consume_chars stops consuming at a sequence it cannot
                // complete (and its caller would retry it forever); the
                // character goes through untranslated instead.
                None => codes.push(c),
            }
        }
        if let Some(preference) = preference {
            aligned.extend(std::iter::repeat_n(preference, codes.len() - start));
        }
    }
    (codes, aligned)
}

/// CHARACTERP of a translation table entry.
fn translation_character(value: Value) -> Option<u32> {
    value
        .integer_value()
        .filter(|&code| (0..=i64::from(RAW_BYTE8_BASE + 0xFF)).contains(&code))
        .map(|code| code as u32)
}

/// get_translation_table's MAX_LOOKUP: the longest source sequence a
/// table's second extra slot declares, at least one character.
fn translation_max_lookup(tables: &[Value]) -> usize {
    tables
        .iter()
        .filter_map(|table| match table.kind() {
            Kind::CharTable(table) if table.extra_count() > 1 => table.extra(1),
            _ => None,
        })
        .filter_map(|value| value.integer_value())
        .filter_map(|value| usize::try_from(value).ok())
        .fold(1, |longest, value| longest.max(value.min(4096)))
}

/// coding.c get_translation: a character or vector entry translates the
/// one character; an alist of (FROM-VECTOR . TO) the first FROM sequence
/// LOOKUP starts with.  The value and the number of characters it
/// replaces, or None when no sequence completes.
fn get_translation(trans: Value, lookup: &[u32]) -> Option<(Value, usize)> {
    if trans.integer_value().is_some() || matches!(trans.kind(), Kind::Vector(_)) {
        return Some((trans, 1));
    }
    let mut tail = trans;
    while let Some((entry, rest)) = tail.cons_values() {
        tail = rest;
        let (from, to) = entry.cons_values()?;
        let Kind::Vector(from) = from.kind() else {
            continue;
        };
        let len = from.len();
        let matches = len <= lookup.len()
            && from
                .slots()
                .zip(lookup)
                .all(|(item, &c)| item.integer_value() == Some(i64::from(c)));
        if len > lookup.len() {
            return None;
        }
        if matches {
            return Some((to, len.max(1)));
        }
    }
    None
}

/// CHAR_BYTE8_P.
fn char_byte8_p(c: u32) -> bool {
    RAW_BYTE8_CHARS.contains(&c)
}

/// CHAR_STRING (CHAR_STRING_ADVANCE_NO_UNIFY): C's internal spelling, an
/// eight-bit character as its byte (encode_coding_utf_8 and
/// encode_coding_raw_text for a multibyte source).
fn emit_internal_chars(out: &mut Vec<u8>, codes: &[u32]) -> Result<(), LispError> {
    for &c in codes {
        if c < 0x80 {
            out.push(c as u8);
        } else if char_byte8_p(c) {
            out.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            let (encoded, width) = crate::lisp::types::string_data::encode_character(c)?;
            out.extend_from_slice(&encoded[..width]);
        }
    }
    Ok(())
}

/// encode_coding_raw_text: a multibyte source's characters in their
/// internal spelling, a unibyte source's bytes as they are.
fn encode_coding_raw_text(codes: &[u32], src_multibyte: bool) -> Result<Vec<u8>, LispError> {
    if !src_multibyte {
        return Ok(codes.iter().map(|&c| c as u8).collect());
    }
    let mut out = Vec::with_capacity(codes.len());
    emit_internal_chars(&mut out, codes)?;
    Ok(out)
}

/// encode_coding_utf_8.
fn encode_coding_utf_8(codes: &[u32], bom: bool) -> Result<Vec<u8>, LispError> {
    let mut out = Vec::with_capacity(codes.len() + 3);
    if bom {
        out.extend_from_slice(&[0xEF, 0xBB, 0xBF]);
    }
    emit_internal_chars(&mut out, codes)?;
    Ok(out)
}

/// Encode SOURCE with CODING. Its multibyte flag distinguishes characters
/// from octets: encode_coding_raw_text emits a multibyte source's non-ASCII
/// characters in their internal (utf-8-emacs) spelling and a unibyte
/// source's characters as their bytes.
pub(crate) fn encode_text_bytes(
    interp: &Interpreter,
    source: &StringLike,
    coding: &str,
    inhibit_eol_conversion: bool,
    selective_display: bool,
    translation: &[Value],
) -> Result<Vec<u8>, LispError> {
    encode_text_bytes_with_charsets(
        interp,
        source,
        coding,
        inhibit_eol_conversion,
        selective_display,
        translation,
        &[],
    )
}

/// encode_coding: consume_chars, then the encoder of CODING's type.
/// TRANSLATION is get_translation_table's for CODING
/// (`encode_translation_tables').  PREFERRED holds the per-character
/// `charset' text property values of SOURCE, which the ISO-2022 encoder
/// consumes as CODING_ANNOTATE_CHARSET annotations.
pub(crate) fn encode_text_bytes_with_charsets(
    interp: &Interpreter,
    source: &StringLike,
    coding: &str,
    inhibit_eol_conversion: bool,
    selective_display: bool,
    translation: &[Value],
    preferred: &[Option<String>],
) -> Result<Vec<u8>, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_else(|| canonical.clone());
    // setup_coding_system chooses the encoder by the coding type.
    let encoder = kind.as_str();
    let eol_type = (!inhibit_eol_conversion)
        .then(|| interp.coding_system_eol_type_value(&canonical))
        .flatten();
    // consume_chars hands the raw-text and CCL encoders a unibyte
    // source's bytes undecoded.
    let raw_text_encoder = matches!(encoder, "raw-text" | "undecided" | "ccl");
    let preferred = if encoder == "iso-2022" {
        preferred
    } else {
        &[]
    };
    let (codes, preferred) = consume_chars(
        source,
        eol_type,
        raw_text_encoder,
        selective_display,
        translation,
        preferred,
    );
    match encoder {
        "utf-8" => encode_coding_utf_8(
            &codes,
            kind == "utf-8-with-signature" || coding_system_requires_bom(interp, &canonical),
        ),
        "utf-16" => {
            let (big_endian, with_bom, _) = coding_system_utf16_options(interp, &canonical, &kind);
            let default_char = coding_system_default_char(interp, &canonical);
            Ok(encode_coding_utf_16(
                &codes,
                big_endian,
                with_bom,
                default_char,
            ))
        }
        "iso-2022" => iso2022::encode(interp, &codes, &canonical, eol_type, &preferred),
        "charset" => Ok(encode_coding_charset(interp, &codes, &canonical)),
        "shift-jis" => Ok(encode_coding_sjis(interp, &codes, &canonical)),
        "big5" => Ok(encode_coding_big5(interp, &codes, &canonical)),
        "emacs-mule" => Ok(encode_coding_emacs_mule(interp, &codes, &canonical)),
        // raw-text, undecided and (with no CCL interpreter) ccl all encode
        // through encode_coding_raw_text.
        _ => encode_coding_raw_text(&codes, source.multibyte),
    }
}

/// encode_coding_utf_16: a character beyond MAX_UNICODE_CHAR (eight-bit
/// ones included) is the coding's default character.
fn encode_coding_utf_16(codes: &[u32], big_endian: bool, bom: bool, default_char: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(codes.len() * 2 + 2);
    let mut emit = |unit: u32| {
        let unit = unit as u16;
        if big_endian {
            out.extend_from_slice(&unit.to_be_bytes());
        } else {
            out.extend_from_slice(&unit.to_le_bytes());
        }
    };
    if bom {
        emit(0xFEFF);
    }
    for &c in codes {
        let c = if c > MAX_UNICODE_CHAR {
            default_char
        } else {
            c
        };
        if c < 0x10000 {
            emit(c);
        } else {
            let c = c - 0x10000;
            emit((c >> 10) + 0xD800);
            emit((c & 0x3FF) + 0xDC00);
        }
    }
    out
}

/// detect_coding for the utf-8-auto and utf-16-auto categories (a utf-8
/// or utf-16 coding whose `:bom' is a cons): the coding the source's
/// signature picks from that cons, carrying CODING's own eol type over
/// (adjust_coding_eol_type); None when CODING stays.
pub(crate) fn detect_auto_bom_coding(
    interp: &Interpreter,
    coding: &str,
    source: &detect::DetectSource,
    env: &Env,
) -> Option<String> {
    if !coding_system_auto_detects_bom(interp, coding)
        || interp.coding_system_kind_name(coding).as_deref() == Some("undecided")
    {
        return None;
    }
    let found = detect::detect_coding(interp, source, coding, env)?;
    Some(adjust_coding_eol_type(
        interp,
        &found,
        interp.coding_system_eol_type_value(coding),
    ))
}

/// setup_coding_system (CODING) then adjust_coding_eol_type for EOL: a
/// coding whose eol is undecided becomes its EOL subsidiary (that of the
/// coding an alias names, whose eol-type vector the alias shares), one
/// whose eol is settled stays.
pub(crate) fn adjust_coding_eol_type(
    interp: &Interpreter,
    coding: &str,
    eol: Option<i64>,
) -> String {
    match eol {
        Some(eol) if coding_eol_type(interp, coding).is_none() => {
            let canonical = interp
                .coding_system_canonical_name(coding)
                .unwrap_or_else(|| coding.to_string());
            let base = interp
                .coding_system_base_name(&canonical)
                .unwrap_or(canonical);
            coding_variant_name(interp, &base, Some(eol))
        }
        _ => coding.to_string(),
    }
}

/// coding.c decode_coding_utf_16, once detect_coding has settled the byte
/// order: a coding with a BOM skips one that matches its order; a
/// surrogate pair is its character; a lone low surrogate is its own
/// character; a high surrogate before anything but a low one comes out
/// as its two bytes (as characters 0..255), and, as CODING_UTF_16_SURROGATE
/// is cleared only by the next high or low surrogate, so does every unit
/// after it until then; one still pending at the end is dropped, and an
/// odd final byte is flushed as raw.
fn decode_coding_utf_16(bytes: &[u8], big_endian: bool, with_bom: bool) -> DecodedText {
    let mut out = DecodedText::new();
    let mut p = 0;
    if with_bom && bytes.len() >= 2 {
        let c = (u32::from(bytes[0]) << 8) | u32::from(bytes[1]);
        if (big_endian && c == 0xFEFF) || (!big_endian && c == 0xFFFE) {
            p = 2;
        }
    }
    let is_high = |c: u32| (0xD800..0xDC00).contains(&c);
    let is_low = |c: u32| (0xDC00..0xE000).contains(&c);
    let mut surrogate = 0u32;
    while p + 1 < bytes.len() {
        let (c1, c2) = (u32::from(bytes[p]), u32::from(bytes[p + 1]));
        p += 2;
        let c = if big_endian {
            (c1 << 8) | c2
        } else {
            (c2 << 8) | c1
        };
        if surrogate != 0 {
            if !is_low(c) {
                let (first, second) = if big_endian {
                    (surrogate >> 8, surrogate & 0xFF)
                } else {
                    (surrogate & 0xFF, surrogate >> 8)
                };
                out.push_code(first);
                out.push_code(second);
                if is_high(c) {
                    surrogate = c;
                } else {
                    out.push_code(c);
                }
            } else {
                out.push_code(0x10000 + (((surrogate - 0xD800) << 10) | (c - 0xDC00)));
                surrogate = 0;
            }
        } else if is_high(c) {
            surrogate = c;
        } else {
            out.push_code(c);
        }
    }
    if let Some(&byte) = bytes.get(p) {
        out.push_byte(byte);
    }
    out
}

/// A decoder's characters, as produce_chars stores them: the text, with
/// the characters it cannot spell kept by character index in EXTENDED
/// (one beyond Unicode or a surrogate, spelled as the invalid-Unicode
/// sentinel, and a real U+E080..U+E0FF, which would read back as a raw
/// byte), the way strings and buffers keep them; and the `charset' runs
/// the decoder annotated (character offsets), which produce_charset turns
/// into text properties.
#[derive(Default)]
pub(crate) struct DecodedText {
    pub(crate) text: String,
    pub(crate) charsets: Vec<(usize, usize, String)>,
    pub(crate) extended: Vec<(usize, u32)>,
    chars: usize,
}

impl DecodedText {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The number of characters produced so far.
    pub(crate) fn char_count(&self) -> usize {
        self.chars
    }

    /// Store the character whose code is C.
    pub(crate) fn push_code(&mut self, c: u32) {
        if c < 0x80 {
            self.text.push(char::from(c as u8));
        } else if RAW_BYTE8_CHARS.contains(&c) {
            self.text
                .push(raw_byte_regex_char((c - RAW_BYTE8_BASE) as u8));
        } else {
            match char::from_u32(c) {
                Some(ch) => {
                    self.text.push(ch);
                    if raw_byte_from_regex_char(ch).is_some() {
                        self.extended.push((self.chars, c));
                    }
                }
                None => {
                    self.text.push(json::INVALID_UNICODE_SENTINEL);
                    self.extended.push((self.chars, c));
                }
            }
        }
        self.chars += 1;
    }

    /// Store BYTE as a decoder's invalid octet: ASCII as itself, any other
    /// as its eight-bit character.
    pub(crate) fn push_byte(&mut self, byte: u8) {
        if byte < 0x80 {
            self.text.push(char::from(byte));
        } else {
            self.text.push(raw_byte_regex_char(byte));
        }
        self.chars += 1;
    }

    /// Store the characters of RUN.
    pub(crate) fn push_str(&mut self, run: &str) {
        if run.as_bytes().contains(&0xEE)
            && run.chars().any(|ch| raw_byte_from_regex_char(ch).is_some())
        {
            for ch in run.chars() {
                self.push_code(ch as u32);
            }
        } else {
            self.text.push_str(run);
            self.chars += run.chars().count();
        }
    }

    /// Store the ASCII run RUN.
    pub(crate) fn push_ascii(&mut self, run: &[u8]) {
        debug_assert!(run.is_ascii());
        // SAFETY: ASCII is UTF-8.
        self.text
            .push_str(unsafe { std::str::from_utf8_unchecked(run) });
        self.chars += run.len();
    }

    /// decode_eol's scan of the produced text: the EOL_SEEN bits, with
    /// CRLF counted once and CR LF's second character skipped.
    fn eol_seen(&self) -> u8 {
        // CR and LF are never part of another character's UTF-8.
        let bytes = self.text.as_bytes();
        let mut seen = detect::EOL_SEEN_NONE;
        let mut i = 0;
        while let Some(offset) = memchr::memchr2(b'\n', b'\r', &bytes[i..]) {
            let p = i + offset;
            if bytes[p] == b'\n' {
                seen |= detect::EOL_SEEN_LF;
                i = p + 1;
            } else if bytes.get(p + 1) == Some(&b'\n') {
                seen |= detect::EOL_SEEN_CRLF;
                i = p + 2;
            } else {
                seen |= detect::EOL_SEEN_CR;
                i = p + 1;
            }
            if seen == detect::EOL_SEEN_LF | detect::EOL_SEEN_CR | detect::EOL_SEEN_CRLF {
                break;
            }
        }
        seen
    }

    /// decode_eol's conversion for EOL_TYPE (1 dos, 2 mac): a CR becomes a
    /// newline under mac, and under dos each CR followed by a newline goes,
    /// the side table and the charset runs moving with the characters
    /// after it.
    fn convert_eol(&mut self, eol_type: i64) {
        match eol_type {
            2 => self.text = self.text.replace('\r', "\n"),
            1 if self.text.contains("\r\n") => {
                // removed[i]: the CRs deleted before character index I.
                let mut removed = Vec::with_capacity(self.chars + 1);
                let mut count = 0;
                let mut chars = self.text.chars().peekable();
                while let Some(ch) = chars.next() {
                    removed.push(count);
                    if ch == '\r' && chars.peek() == Some(&'\n') {
                        count += 1;
                    }
                }
                removed.push(count);
                let shift = |index: usize| index - removed[index.min(removed.len() - 1)];
                for entry in &mut self.extended {
                    entry.0 = shift(entry.0);
                }
                for run in &mut self.charsets {
                    run.0 = shift(run.0);
                    run.1 = shift(run.1);
                }
                self.text = self.text.replace("\r\n", "\n");
                self.chars -= count;
            }
            _ => {}
        }
    }

    /// Append OTHER, shifting its character offsets past these.
    pub(crate) fn append(&mut self, other: DecodedText) {
        let base = self.chars;
        self.text.push_str(&other.text);
        self.extended.extend(
            other
                .extended
                .into_iter()
                .map(|(index, code)| (base + index, code)),
        );
        self.charsets.extend(
            other
                .charsets
                .into_iter()
                .map(|(start, end, charset)| (base + start, base + end, charset)),
        );
        self.chars += other.chars;
    }
}

pub(crate) fn decode_text_bytes(
    interp: &Interpreter,
    bytes: &[u8],
    coding: &str,
) -> Result<String, LispError> {
    decode_text_bytes_annotated(interp, bytes, coding).map(|decoded| decoded.text)
}

/// The `charset' runs a decoder annotates (coding.c ADD_CHARSET_DATA): a
/// run opens at the first character of a non-ASCII charset and closes
/// when a character of a different non-ASCII charset appears; ASCII
/// characters never close one (except in emacs-mule, whose decoder
/// compares every character's charset).
pub(crate) struct CharsetRuns {
    last: Option<String>,
    last_offset: usize,
    spans: Vec<(usize, usize, String)>,
}

impl CharsetRuns {
    pub(crate) fn new() -> Self {
        CharsetRuns {
            last: None,
            last_offset: 0,
            spans: Vec::new(),
        }
    }

    pub(crate) fn note(&mut self, charset: &str, offset: usize) {
        if charset == "ascii" || self.last.as_deref() == Some(charset) {
            return;
        }
        self.close(offset);
        self.last = Some(charset.to_string());
        self.last_offset = offset;
    }

    /// decode_coding_emacs_mule's variant: an ASCII character ends the
    /// current run too.
    pub(crate) fn note_strict(&mut self, charset: &str, offset: usize) {
        if self.last.as_deref() == Some(charset) || (charset == "ascii" && self.last.is_none()) {
            return;
        }
        self.close(offset);
        if charset != "ascii" {
            self.last = Some(charset.to_string());
            self.last_offset = offset;
        }
    }

    pub(crate) fn close(&mut self, offset: usize) {
        if let Some(previous) = self.last.take()
            && offset > self.last_offset
        {
            self.spans.push((self.last_offset, offset, previous));
        }
    }

    pub(crate) fn finish(mut self, offset: usize) -> Vec<(usize, usize, String)> {
        self.close(offset);
        self.spans
    }
}

/// charset.c CHAR_CHARSET / char_charset (c, Qnil): ASCII is `ascii';
/// otherwise the charsets of the ordered list are tried in order, a
/// Unicode character answering `unicode' once the non-preferred part of
/// the order is reached, and `eight-bit' is the fallback.  ORDERED and
/// NON_PREFERRED_HEAD come from the interpreter (a caller working over a
/// text fetches them once).
pub(crate) fn char_charset_ordered(
    interp: &Interpreter,
    ordered: &[String],
    non_preferred_head: Option<&str>,
    ch: u32,
) -> (String, u32) {
    // CH may carry a raw byte in its Rust-internal spelling; GNU's
    // character number decides the MAX_UNICODE_CHAR test below.
    let ch = gnu_character_number(ch);
    if ch <= 0x7F {
        return ("ascii".into(), ch);
    }
    let unicode_char = ch <= MAX_UNICODE_CHAR;
    for (index, charset) in ordered.iter().enumerate() {
        if let Some(code) = encode_charset_char(interp, charset, ch) {
            return (charset.clone(), code);
        }
        if unicode_char
            && let Some(next) = ordered.get(index + 1)
            && Some(next.as_str()) == non_preferred_head
        {
            return ("unicode".into(), ch);
        }
    }
    // Past the whole list: `emacs' up to MAX_5_BYTE_CHAR, `eight-bit'
    // beyond (both normally answer from inside the list already).
    if ch <= MAX_5_BYTE_CHAR {
        ("emacs".into(), ch)
    } else {
        ("eight-bit".into(), ch - RAW_BYTE8_BASE)
    }
}

/// GNU's number for CH: a raw byte spelled in the Rust-internal regex
/// range becomes #x3fff80..#x3fffff; every other value is its own number.
pub(crate) fn gnu_character_number(ch: u32) -> u32 {
    if (RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xff).contains(&ch) {
        RAW_BYTE8_BASE + ch - RAW_BYTE_REGEX_BASE
    } else {
        ch
    }
}

pub(crate) fn char_charset(interp: &Interpreter, ch: u32) -> (String, u32) {
    let ordered = interp.charset_priority_list();
    let head = interp.charset_non_preferred_head();
    char_charset_ordered(interp, &ordered, head.as_deref(), ch)
}

/// Fdefine_coding_system_internal's `charset_valids' vector for a
/// charset-type coding: per first byte, the charsets it may start,
/// smaller dimensions first.
pub(crate) fn charset_valids(
    interp: &Interpreter,
    coding: &str,
) -> Vec<Option<Vec<std::rc::Rc<CharsetInfo>>>> {
    let mut valids: Vec<Option<Vec<std::rc::Rc<CharsetInfo>>>> = vec![None; 256];
    for charset in coding_system_charset_names(interp, coding) {
        let Some(info) = charset_info(interp, &charset) else {
            continue;
        };
        let dim = info.dimension as usize;
        let Some(&(min, max)) = info
            .code_space
            .as_ref()
            .and_then(|bounds| bounds.get(dim - 1))
        else {
            continue;
        };
        for byte in min..=max.min(255) {
            let entry = valids[byte as usize].get_or_insert_with(Vec::new);
            let position = entry
                .iter()
                .position(|existing| existing.dimension as usize > dim)
                .unwrap_or(entry.len());
            entry.insert(position, info.clone());
        }
    }
    valids
}

/// coding.c:get_translation_table for decoding, as far as whether there
/// is one: none while `enable-character-translation' is nil; else the
/// coding's `:decode-translation-table' (a symbol naming its
/// `translation-table' property, a list of tables, or the table) with
/// `standard-translation-table-for-decode' after it.
fn decode_translation_table_p(interp: &Interpreter, coding: &str, env: &Env) -> bool {
    if interp
        .lookup_var("enable-character-translation", env)
        .is_none_or(|value| value.is_nil())
    {
        return false;
    }
    let standard = interp
        .lookup_var("standard-translation-table-for-decode", env)
        .unwrap_or(Value::Nil);
    let table =
        coding_system_property(interp, coding, ":decode-translation-table").unwrap_or(Value::Nil);
    if table.is_nil() {
        return !standard.is_nil();
    }
    if !standard.is_nil() {
        return true;
    }
    match table.kind() {
        Kind::Symbol(name) => interp
            .get_symbol_property(&name, "translation-table")
            .is_some_and(|table| !table.is_nil()),
        Kind::T => interp
            .get_symbol_property("t", "translation-table")
            .is_some_and(|table| !table.is_nil()),
        // A list stays a list after its symbols are resolved.
        _ => true,
    }
}

pub(crate) fn decode_text_bytes_annotated(
    interp: &Interpreter,
    bytes: &[u8],
    coding: &str,
) -> Result<DecodedText, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    // detect_coding settles an auto-BOM coding before it decodes.
    let canonical = detect_auto_bom_coding(
        interp,
        &canonical,
        &detect::DetectSource::from_bytes(bytes),
        &Env::new(),
    )
    .unwrap_or(canonical);
    decode_bytes_with_coding(interp, bytes, &canonical)
}

/// The decoder of the coding system CANONICAL over BYTES, with no
/// detection.
fn decode_bytes_with_coding(
    interp: &Interpreter,
    bytes: &[u8],
    canonical: &str,
) -> Result<DecodedText, LispError> {
    let canonical = canonical.to_string();
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_else(|| canonical.clone());
    // decode_coding_charset covers every charset-type coding system, the
    // single-byte ones (koi8-r, windows-125x) included: their `charset'
    // property is the coding's charset, as the oracle reports for
    // `(decode-coding-string "a\301" 'koi8-r)'.
    match kind.as_str() {
        "iso-2022" => return Ok(iso2022::decode(interp, bytes, &canonical)),
        "shift-jis" => return Ok(decode_sjis_bytes(interp, bytes, &canonical)),
        "big5" => return Ok(decode_big5_bytes(interp, bytes, &canonical)),
        "emacs-mule" => return Ok(decode_emacs_mule_bytes(interp, bytes)),
        "charset" => return Ok(decode_charset_coding_bytes(interp, bytes, &canonical)),
        _ => {}
    }
    decode_text_bytes_plain(interp, bytes, &canonical, &kind)
}

fn decode_text_bytes_plain(
    interp: &Interpreter,
    bytes: &[u8],
    canonical: &str,
    kind: &str,
) -> Result<DecodedText, LispError> {
    let canonical = canonical.to_string();
    match kind {
        "utf-8" | "prefer-utf-8" | "utf-8-auto" => {
            let bytes = if coding_system_consumes_bom(interp, &canonical) {
                strip_utf8_bom(bytes).1
            } else {
                bytes
            };
            Ok(decode_coding_utf_8(bytes))
        }
        "utf-8-with-signature" => Ok(decode_coding_utf_8(strip_utf8_bom(bytes).1)),
        "utf-16" => {
            // An auto-BOM coding reaching the decoder is one detect_coding
            // could not settle (utf_detect_bom): it decodes without a BOM in
            // its own byte order.
            let (big_endian, with_bom, detect_bom) =
                coding_system_utf16_options(interp, &canonical, kind);
            Ok(decode_coding_utf_16(
                bytes,
                big_endian,
                with_bom && !detect_bom,
            ))
        }
        _ => Ok(decode_coding_raw_text(bytes)),
    }
}

/// CODING_ATTR_CHARSET_LIST: the charsets CODING was defined with, the
/// symbols `iso-2022' and `emacs-mule' standing for those lists.
pub(crate) fn coding_attr_charset_list(interp: &Interpreter, coding: &str) -> Vec<String> {
    let Some(state) = interp.coding_system(coding) else {
        return Vec::new();
    };
    match state.charset_list.kind() {
        Kind::Symbol(name) if name == "iso-2022" => interp.iso_2022_charset_list(),
        Kind::Symbol(name) if name == "emacs-mule" => emacs_mule_charset_list(interp),
        _ => coding_system_charset_names(interp, coding),
    }
}

fn charset_infos(interp: &Interpreter, names: &[String]) -> Vec<std::rc::Rc<CharsetInfo>> {
    names
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect()
}

/// coding.c char_encodable_p: some charset of CHARSETS contains C once the
/// encode translation TABLES have translated it.
fn char_encodable_p(
    interp: &Interpreter,
    c: u32,
    charsets: &[std::rc::Rc<CharsetInfo>],
    tables: &[Value],
) -> bool {
    let c = tables
        .iter()
        .fold(c, |code, table| translate_safety_character(*table, code));
    charsets
        .iter()
        .any(|charset| charset.encode(interp, c).is_some())
}

/// term.c's test of a character against the terminal's charset list
/// (coding_charset_list of its coding): char_charset over that list, which
/// for a coding without one is the ordered list and always succeeds.
pub(crate) fn terminal_char_encodable(interp: &Interpreter, coding: &str, c: u32) -> bool {
    let names = match iso2022::iso_attributes(interp, coding) {
        Some(attrs) if attrs.flags & crate::lisp::eval::coding::ISO_FLAG_FULL_SUPPORT != 0 => {
            interp.iso_2022_charset_list()
        }
        _ => coding_attr_charset_list(interp, coding),
    };
    names.is_empty() || char_encodable_p(interp, c, &charset_infos(interp, &names), &[])
}

/// The character codes of the current buffer's text FROM..TO, absolute
/// positions read past any narrowing.
fn buffer_character_codes(interp: &Interpreter, from: usize, to: usize) -> Vec<u32> {
    let buffer = interp.buffer.borrow();
    let source = StringLike {
        text: String::from(buffer.text_rope().slice(from - 1..to - 1)),
        props: Vec::new(),
        multibyte: buffer.is_multibyte(),
        extended_chars: buffer.substring_extended_chars(from, to),
    };
    source
        .character_codes_iter()
        .map(|code| code as u32)
        .collect()
}

/// coding.c Funencodable_char_position.
pub(crate) fn unencodable_char_position_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let coding = checked_coding_symbol(interp, &args[2])?;
    let canonical = interp
        .coding_system_canonical_name(&coding)
        .ok_or_else(|| coding_system_error(args[2]))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_default();
    if kind == "raw-text" {
        return Ok(Value::Nil);
    }
    let ascii_compatible = coding_system_is_ascii_compatible(interp, &canonical);
    let charsets = charset_infos(interp, &coding_attr_charset_list(interp, &canonical));
    let tables = encode_translation_tables(interp, &canonical, env)?;
    let string = args.get(4).copied().filter(|value| !value.is_nil());
    let (codes, from) = match string {
        None => {
            // validate_region.
            let mut start = position_from_value(interp, &args[0])?;
            let mut end = position_from_value(interp, &args[1])?;
            if start > end {
                std::mem::swap(&mut start, &mut end);
            }
            let (begv, zv, multibyte) = {
                let buffer = interp.buffer.borrow();
                (
                    buffer.point_min(),
                    buffer.point_max(),
                    buffer.is_multibyte(),
                )
            };
            if !(begv <= start && end <= zv) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("args-out-of-range"),
                    Value::Buffer(interp.buffer),
                    Value::Integer(start as i64),
                    Value::Integer(end as i64),
                ])));
            }
            if !multibyte {
                return Ok(Value::Nil);
            }
            (buffer_character_codes(interp, start, end), start)
        }
        Some(string) => {
            let source = string_like(&string)
                .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), string))?;
            let length = source.text.chars().count();
            let (from, to) = validate_subarray(string, args[0], args[1], length)?;
            if !source.multibyte {
                return Ok(Value::Nil);
            }
            let codes: Vec<u32> = source
                .character_codes_iter()
                .skip(from)
                .take(to - from)
                .map(|code| code as u32)
                .collect();
            (codes, from)
        }
    };
    if ascii_compatible && codes.iter().all(|&c| c < 0x80) {
        return Ok(Value::Nil);
    }
    let mut n = match args.get(3).map(|value| value.kind()) {
        None | Some(Kind::Nil) => 1,
        Some(Kind::Integer(count)) if count >= 0 => count,
        Some(other) => return Err(wrong_type_argument("wholenump", other.value())),
    };
    let mut positions = Vec::new();
    for (index, &c) in codes.iter().enumerate() {
        if !(c < 0x80 && ascii_compatible) && !char_encodable_p(interp, c, &charsets, &tables) {
            positions.push(Value::Integer((from + index) as i64));
            n -= 1;
            if n == 0 {
                break;
            }
        }
    }
    Ok(if args.get(3).is_none_or(Value::is_nil) {
        positions.first().copied().unwrap_or(Value::Nil)
    } else {
        Value::list(positions)
    })
}

struct EmacsMuleLayout {
    lengths: [usize; 256],
    charsets: Vec<Option<String>>,
}

fn emacs_mule_layout(interp: &Interpreter) -> EmacsMuleLayout {
    // charset.c keeps these two tables in lockstep with `define-charset'.
    // The Lisp table is the exact last-definition-wins charset map; deriving
    // lengths from its live entries also honors packages which add charsets.
    let mut lengths = [1_usize; 256];
    lengths[0x9A] = 3;
    lengths[0x9B] = 3;
    lengths[0x9C] = 4;
    lengths[0x9D] = 4;
    let mut charsets = vec![None; 256];
    if let Some(table) =
        interp.lookup_var("emacs-mule-charset-table", &crate::lisp::types::Env::new())
        && let Ok(entries) = vector_items(&table)
    {
        for (id, entry) in entries.into_iter().take(256).enumerate() {
            let Ok(charset) = entry.as_symbol() else {
                continue;
            };
            let Some(dimension) = charset_plist_property(interp, charset, ":dimension")
                .and_then(|value| value.integer_value())
                .and_then(|value| usize::try_from(value).ok())
            else {
                continue;
            };
            charsets[id] = Some(charset.to_string());
            lengths[id] = dimension + usize::from(id >= 0xA0) + 1;
        }
    }
    EmacsMuleLayout { lengths, charsets }
}

/// charset.c Vemacs_mule_charset_list: the charsets that carry an
/// `:emacs-mule-id', in the current priority order (Fset_charset_priority
/// rebuilds the list in Vcharset_ordered_list order).
pub(crate) fn emacs_mule_charset_list(interp: &Interpreter) -> Vec<String> {
    interp
        .charset_priority_list()
        .into_iter()
        .filter(|charset| {
            charset_info(interp, charset).is_some_and(|info| info.emacs_mule_id.is_some())
        })
        .collect()
}

/// coding.c encode_coding_emacs_mule: ASCII and raw bytes as themselves;
/// any other character through the first charset of
/// Vemacs_mule_charset_list that encodes it, written as
/// EMACS_MULE_LEADING_CODES of the charset's emacs-mule id followed by
/// the code bytes with their high bits set; an unencodable character is
/// the coding's default char.  The encoder's `charset' annotation branch
/// never runs: setup_coding_system sets CODING_ANNOTATE_CHARSET_MASK for
/// ISO-2022 designation codings only, so a `charset' text property does
/// not steer emacs-mule (the oracle re-encodes a jisx0208-annotated
/// hiragana through chinese-gb2312, the list's first match).
fn encode_coding_emacs_mule(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let charsets: Vec<(std::rc::Rc<CharsetInfo>, u8)> = emacs_mule_charset_list(interp)
        .iter()
        .filter_map(|charset| {
            let info = charset_info(interp, charset)?;
            let id = info.emacs_mule_id?;
            Some((info, id))
        })
        .collect();
    // char_charset (C, Vemacs_mule_charset_list, &code).
    let char_charset = |c: u32| {
        charsets.iter().find_map(|(charset, id)| {
            charset
                .encode(interp, c)
                .map(|code| (charset.dimension, *id, code))
        })
    };
    let default_char = coding_system_default_char(interp, coding);
    let mut out = Vec::with_capacity(codes.len());
    for &code in codes {
        if code < 0x80 {
            out.push(code as u8);
            continue;
        }
        if char_byte8_p(code) {
            out.push((code - RAW_BYTE8_BASE) as u8);
            continue;
        }
        let Some((dimension, id, charset_code)) = char_charset(code).or_else(|| {
            if default_char < 0x80 {
                out.push(default_char as u8);
                return None;
            }
            char_charset(default_char)
        }) else {
            continue;
        };
        // EMACS_MULE_LEADING_CODES
        match id {
            0..=0x9F => out.push(id),
            0xA0..=0xDF => out.extend([0x9A, id]),
            0xE0..=0xEF => out.extend([0x9B, id]),
            0xF0..=0xF4 => out.extend([0x9C, id]),
            _ => out.extend([0x9D, id]),
        }
        if dimension == 1 {
            out.push((charset_code | 0x80) as u8);
        } else {
            let charset_code = charset_code | 0x8080;
            out.extend([(charset_code >> 8) as u8, (charset_code & 0xFF) as u8]);
        }
    }
    out
}

pub(crate) fn decode_emacs_mule_bytes(interp: &Interpreter, bytes: &[u8]) -> DecodedText {
    // coding.c:emacs_mule_char.  Invalid or unmappable sequences are retried
    // from the next byte, which preserves each offending byte as GNU's
    // eight-bit character instead of consuming a superficially valid run.
    // decode_coding_emacs_mule annotates every character's charset, so an
    // ASCII character closes a run.
    let layout = emacs_mule_layout(interp);
    let mut out = DecodedText::new();
    let mut runs = CharsetRuns::new();
    let mut index = 0;
    while index < bytes.len() {
        let lead = bytes[index];
        if lead < 0x80 {
            runs.note_strict("ascii", out.char_count());
            out.push_byte(lead);
            index += 1;
            continue;
        }

        let decoded = match lead {
            0x9A | 0x9B => bytes
                .get(index + 1..index + 3)
                .filter(|tail| tail[0] >= 0xA0 && tail[1] >= 0xA0)
                .and_then(|tail| {
                    layout.charsets[usize::from(tail[0])]
                        .as_deref()
                        .map(|charset| (charset, tail))
                })
                .and_then(|(charset, tail)| {
                    decode_charset_code(interp, charset, u32::from(tail[1] & 0x7F))
                        .map(|character| (charset, character, 3))
                }),
            0x9C | 0x9D => bytes
                .get(index + 1..index + 4)
                .filter(|tail| tail[1] >= 0xA0 && tail[2] >= 0xA0)
                .and_then(|tail| {
                    layout.charsets[usize::from(tail[0])]
                        .as_deref()
                        .map(|charset| (charset, tail))
                })
                .and_then(|(charset, tail)| {
                    let code = u32::from(tail[1] & 0x7F) << 8 | u32::from(tail[2] & 0x7F);
                    decode_charset_code(interp, charset, code)
                        .map(|character| (charset, character, 4))
                }),
            _ if lead < 0xA0 && layout.lengths[usize::from(lead)] > 1 => {
                let length = layout.lengths[usize::from(lead)];
                bytes
                    .get(index + 1..index + length)
                    .filter(|tail| tail.iter().all(|byte| *byte >= 0xA0))
                    .and_then(|tail| {
                        let charset = layout.charsets[usize::from(lead)].as_deref()?;
                        let code = tail
                            .iter()
                            .fold(0_u32, |code, byte| code << 8 | u32::from(byte & 0x7F));
                        decode_charset_code(interp, charset, code)
                            .map(|character| (charset, character, length))
                    })
            }
            _ => None,
        };
        if let Some((charset, character, consumed)) = decoded {
            runs.note_strict(charset, out.char_count());
            out.push_code(character);
            index += consumed;
        } else {
            runs.note_strict("eight-bit", out.char_count());
            out.push_byte(lead);
            index += 1;
        }
    }
    out.charsets = runs.finish(out.char_count());
    out
}

pub(crate) fn text_from_region_or_string(
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
        .borrow()
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn detected_names_value(names: Vec<String>, highest: bool) -> Value {
    if highest {
        names
            .into_iter()
            .next()
            .map(|name| Value::Symbol(name.into()))
            .unwrap_or(Value::Nil)
    } else {
        Value::list(
            names
                .into_iter()
                .map(|name| Value::Symbol(name.into()))
                .collect::<Vec<_>>(),
        )
    }
}

pub(crate) fn detect_coding_string_value(
    interp: &Interpreter,
    value: &Value,
    highest: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let string = string_like(value).ok_or_else(|| wrong_type_argument("stringp", *value))?;
    let highest = highest.is_some_and(Value::is_truthy);
    let src = DetectSource::from_text(&string.text, string.multibyte);
    Ok(detected_names_value(
        detect::detect_coding_system(interp, &src, highest, None, env),
        highest,
    ))
}

pub(crate) fn detect_coding_region_value(
    interp: &Interpreter,
    start: &Value,
    end: &Value,
    highest: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let text = text_from_region_or_string(interp, start, Some(end))?;
    let highest = highest.is_some_and(Value::is_truthy);
    let src = DetectSource::from_text(&text, interp.buffer.borrow().is_multibyte());
    Ok(detected_names_value(
        detect::detect_coding_system(interp, &src, highest, None, env),
        highest,
    ))
}

/// coding.c:get_translation_table (ATTRS, true, ...): the tables the
/// encoders and the encodability checks translate characters through.
/// Only a coding's own symbol/list entries are resolved through symbol
/// properties; a standard table used on its own is passed through as-is.
pub(crate) fn encode_translation_tables(
    interp: &mut Interpreter,
    coding: &str,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    if interp
        .lookup_var("enable-character-translation", env)
        .is_some_and(|value| value.is_nil())
    {
        return Ok(Vec::new());
    }
    let standard = interp
        .lookup_var("standard-translation-table-for-encode", env)
        .unwrap_or(Value::Nil);
    let table =
        coding_system_property(interp, coding, ":encode-translation-table").unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(if standard.is_nil() {
            Vec::new()
        } else {
            vec![standard]
        });
    }
    let mut resolve = |value: Value| {
        // SYMBOLP includes nil/t and enabled positioned symbols. Fget
        // also observes the current overriding-plist-environment; use
        // the same primitive path instead of reading the raw plist.
        if value.is_symbol()
            || (symbols_with_pos_enabled(interp, env)
                && symbol_with_pos_parts(interp, &value).is_some())
        {
            dispatch::misc::direct_get(interp, &[value, Value::symbol("translation-table")], env)
        } else {
            Ok(value)
        }
    };
    let mut tables = if table.cons_values().is_some() {
        table
            .to_vec()?
            .into_iter()
            .map(resolve)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![resolve(table)?]
    };
    if matches!(standard.kind(), Kind::CharTable(_)) {
        tables.push(standard);
    }
    Ok(tables)
}

/// character.c:translate_char applies table lists in order, replacing a
/// character only with another valid character, and reading live slots.
fn translate_safety_character(mut table: Value, mut character: u32) -> u32 {
    if let Kind::CharTable(table) = table.kind() {
        if let Kind::Integer(translated) = table.get(character).kind()
            && (0..=i64::from(RAW_BYTE8_BASE + 0xff)).contains(&translated)
        {
            return translated as u32;
        }
        return character;
    }
    while let Some((head, tail)) = table.cons_values() {
        character = translate_safety_character(head, character);
        table = tail;
    }
    character
}

pub(crate) fn find_coding_systems_region_internal_value(
    interp: &mut Interpreter,
    start: &Value,
    end: &Value,
    exclude: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let source = if let Some(string) = string_like(start) {
        string
    } else {
        let from = position_from_value(interp, start)?;
        let to = position_from_value(interp, end)?;
        let buffer = interp.buffer.borrow();
        let text = buffer.text_rope();
        // coding.c checks BEG..Z, including text outside narrowing, and
        // rejects reversed bounds. No Lisp callback runs while reading it.
        if from == 0 || from > to || to > text.len_chars() + 1 {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                *start,
                *end,
            ])));
        }
        StringLike {
            text: String::from(text.slice(from - 1..to - 1)),
            props: Vec::new(),
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(from, to),
        }
    };
    if !source.multibyte {
        return Ok(Value::T);
    }
    // GNU's work table checks each non-ASCII character only once. Keep
    // actual Emacs character numbers, including non-Unicode and byte8.
    let characters: std::collections::BTreeSet<u32> = source
        .character_codes_iter()
        .map(|code| code as u32)
        .filter(|code| *code >= 0x80)
        .collect();
    if characters.is_empty() {
        return Ok(Value::T);
    }
    let excluded = exclude
        .filter(|value| !value.is_nil())
        .map(|value| value.to_vec())
        .transpose()?
        .unwrap_or_default();
    let mut codings = Vec::new();
    // coding.c walks the actual Lisp registration list, retaining its
    // order and duplicates, including a dynamically bound list.
    let mut registered = interp
        .lookup_var("coding-system-list", env)
        .unwrap_or(Value::Nil);
    while let Some((head, tail)) = registered.cons_values() {
        let coding_value = head;
        registered = tail;
        if excluded
            .iter()
            .any(|candidate| candidate.eq_value(coding_value))
        {
            continue;
        }
        let coding = coding_value.as_symbol()?;
        let Some(state) = interp.coding_system(coding) else {
            continue;
        };
        if state.base != coding {
            continue;
        }
        let charsets = match state.charset_list.kind() {
            Kind::Symbol(name) if name == "iso-2022" => interp.iso_2022_charset_list(),
            Kind::Symbol(name) if name == "emacs-mule" => emacs_mule_charset_list(interp),
            _ => coding_system_charset_names(interp, coding),
        };
        let translations = encode_translation_tables(interp, coding, env)?;
        // coding.c:char_encodable_p tests declared charset membership;
        // an encoder's fallback/substitution is not proof of safe encoding.
        if characters.iter().all(|&character| {
            let character = translations.iter().fold(character, |code, table| {
                translate_safety_character(*table, code)
            });
            charsets.iter().any(|charset| {
                encode_charset_character_number(interp, charset, character).is_some()
            })
        }) {
            codings.push(coding_value);
        }
    }
    // coding.c always supplies these two fallbacks, including when EXCLUDE
    // names them. They do not need a charset-list to preserve the bytes.
    codings.extend([Value::symbol("raw-text"), Value::symbol("no-conversion")]);
    Ok(Value::list(codings))
}

/// coding.c Fcheck_coding_systems_region: for each coding of
/// CODING_LIST that cannot encode some non-ASCII character of the region
/// (absolute positions, past any narrowing) or string, the coding as
/// given followed by those positions.
pub(crate) fn check_coding_systems_region_value(
    interp: &mut Interpreter,
    start: &Value,
    end: Option<&Value>,
    coding_list: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let (codes, base) = if let Some(string) = string_like(start) {
        if !string.multibyte {
            return Ok(Value::Nil);
        }
        let codes: Vec<u32> = string
            .character_codes_iter()
            .map(|code| code as u32)
            .collect();
        (codes, 0)
    } else {
        let end_value = end.copied().unwrap_or(Value::Nil);
        let s = position_from_value(interp, start)?;
        let e = position_from_value(interp, &end_value)?;
        let (z, multibyte) = {
            let buffer = interp.buffer.borrow();
            (buffer.text_rope().len_chars() + 1, buffer.is_multibyte())
        };
        if !(1 <= s && s <= e && e <= z) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                *start,
                end_value,
            ])));
        }
        if !multibyte {
            return Ok(Value::Nil);
        }
        (buffer_character_codes(interp, s, e), s)
    };
    if codes.iter().all(|&c| c < 0x80) {
        return Ok(Value::Nil);
    }
    let mut checks = Vec::new();
    let mut tail = *coding_list;
    while let Some((coding, rest)) = tail.cons_values() {
        tail = rest;
        let canonical = coding
            .symbol_str()
            .and_then(|name| interp.coding_system_canonical_name(name))
            .ok_or_else(|| coding_system_error(coding))?;
        let charsets = charset_infos(interp, &coding_attr_charset_list(interp, &canonical));
        let tables = encode_translation_tables(interp, &canonical, env)?;
        checks.push((coding, charsets, tables, Vec::new()));
    }
    for (index, &c) in codes.iter().enumerate() {
        if c < 0x80 {
            continue;
        }
        for (_, charsets, tables, positions) in &mut checks {
            if !char_encodable_p(interp, c, charsets, tables) {
                positions.push(Value::Integer((base + index) as i64));
            }
        }
    }
    Ok(Value::list(
        checks
            .into_iter()
            .filter(|(_, _, _, positions)| !positions.is_empty())
            .map(|(coding, _, _, positions)| Value::list(std::iter::once(coding).chain(positions)))
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn find_operation_coding_system_value(
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
    let operation = args[0].as_symbol()?;
    let (target_index, alist_name) = match operation {
        "insert-file-contents" => (0, "file-coding-system-alist"),
        "write-region" => (2, "file-coding-system-alist"),
        "call-process" => (0, "process-coding-system-alist"),
        "call-process-region" | "start-process" => (2, "process-coding-system-alist"),
        "open-network-stream" => (3, "network-coding-system-alist"),
        _ => {
            return Err(LispError::Signal(format!(
                "Invalid first argument: {operation}"
            )));
        }
    };
    let Some(target) = args.get(target_index + 1) else {
        return Err(LispError::Signal(format!(
            "Too few arguments for operation `{operation}'"
        )));
    };
    let operation_target = match target.kind() {
        Kind::Cons(cell) if operation == "insert-file-contents" => cell.car.get(),
        other => other.value(),
    };
    let Some(alist) = interp.lookup_var(alist_name, env) else {
        return Ok(Value::Nil);
    };
    for entry in alist.to_vec()? {
        let Some((pattern, target)) = entry.cons_values() else {
            continue;
        };
        let matches = match (pattern.kind(), operation_target.kind()) {
            (Kind::Integer(pattern), Kind::Integer(target)) => pattern == target,
            (Kind::StringObject(_), Kind::StringObject(_)) => {
                search::fast_string_match(interp, env, &pattern, &operation_target, None, false)?
                    .is_some()
            }
            _ => continue,
        };
        if !matches {
            continue;
        }
        // A cons is already the requested (DECODING . ENCODING) pair.
        if matches!(target.kind(), Kind::Cons(_)) {
            return Ok(target);
        }
        let coding = match target.kind() {
            Kind::Symbol(symbol) if interp.has_coding_system(&symbol) => interp
                .coding_system_canonical_name(&symbol)
                .unwrap_or_else(|| symbol.to_string()),
            Kind::Symbol(symbol) => {
                let result =
                    call_named_function(interp, &symbol, &[Value::list(args.to_vec())], env)?;
                if let Some((decode, encode)) = result.cons_values() {
                    return Ok(Value::cons(decode, encode));
                }
                checked_coding_symbol(interp, &result)?
            }
            other => {
                let result = call_function_value(
                    interp,
                    &other.value(),
                    &[Value::list(args.to_vec())],
                    env,
                )?;
                if let Some((decode, encode)) = result.cons_values() {
                    return Ok(Value::cons(decode, encode));
                }
                checked_coding_symbol(interp, &result)?
            }
        };
        return Ok(Value::cons(
            Value::Symbol(coding.clone().into()),
            Value::Symbol(coding.into()),
        ));
    }
    Ok(Value::Nil)
}

/// The `charset' text property per character of STRING: the annotation
/// CODING_ANNOTATE_CHARSET_MASK hands the encoder as the preferred
/// charset.
fn charset_preferences(string: &StringLike) -> Vec<Option<String>> {
    if string.props.is_empty() {
        return Vec::new();
    }
    let count = string.text.chars().count();
    let mut preferred = vec![None; count];
    for span in &string.props {
        if let Some((_, value)) = span.props.iter().find(|(name, _)| name == "charset")
            && let Ok(charset) = value.as_symbol()
        {
            for slot in preferred
                .iter_mut()
                .take(span.end.min(count))
                .skip(span.start)
            {
                *slot = Some(charset.to_string());
            }
        }
    }
    preferred
}

pub(crate) fn encode_coding_value(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    encode_coding_value_recording(interp, value, coding, nocopy, true, env)
}

pub(crate) fn encode_coding_value_recording(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    record_used: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let Some(coding) = coding else {
        if record_used {
            set_last_coding_system_used(interp, "no-conversion", env);
        }
        return if nocopy {
            Ok(*value)
        } else {
            shared_string_copy(value)
        };
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    // The requested spelling, not the canonical name (the oracle answers
    // `euc-jp' for (encode-coding-string "a" 'euc-jp)).
    if record_used {
        set_last_coding_system_used(interp, coding, env);
    }
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // code_convert_string only preserves identity for ASCII-compatible,
    // ASCII-only input with no EOL work. It runs before pre-write conversion.
    let fast_path = coding_system_is_ascii_compatible(interp, &canonical)
        && string.text.is_ascii()
        && string.extended_chars.is_empty()
        && (interp.coding_system_eol_type_value(&canonical) == Some(0)
            || matches!(canonical.as_str(), "no-conversion" | "binary")
            || inhibit_eol_conversion
            || !string.text.contains('\n'));
    if fast_path {
        return Ok(if nocopy {
            *value
        } else {
            bytes_to_shared_unibyte_value(string.text.as_bytes())
        });
    }
    let pre_write =
        coding_system_property(interp, &canonical, ":pre-write-conversion").unwrap_or(Value::Nil);
    let converted = run_coding_conversion(interp, &string, &pre_write, true, env)?;
    let preferred = if pre_write.is_nil() {
        charset_preferences(&string)
    } else {
        Vec::new()
    };
    // encode_coding substitutes the default character for what the
    // coding cannot encode; it never signals.
    let translation = encode_translation_tables(interp, &canonical, env)?;
    let encoded = encode_text_bytes_with_charsets(
        interp,
        &converted,
        &canonical,
        inhibit_eol_conversion,
        false,
        &translation,
        &preferred,
    )?;
    Ok(bytes_to_shared_unibyte_value(&encoded))
}

/// coding.c's `ONE_MORE_BYTE' under `multibytep' (coding->src_multibyte,
/// set by decode_coding_object from `chars < bytes'): an ASCII character
/// is a source byte, a raw byte8 character is its own octet (the macro
/// recovers it from the C0/C1 lead), and any other character leaves the
/// byte stream -- the macro hands the decoder the NEGATIVE character code,
/// which every decoder passes through unchanged.  So the decoder really
/// runs over the byte runs BETWEEN the multibyte characters, and a unibyte
/// destination stores each passed-through code's low eight bits.
/// A multibyte source (a multibyte buffer region, or a multibyte string):
/// coding.c's ONE_MORE_BYTE hands the decoder each raw-byte character as
/// its byte and every other non-ASCII character as an invalid (negative)
/// code that comes through unchanged, so the byte runs decode and the
/// characters between them pass.  The decoder's `charset' runs are kept,
/// re-based on the output.  RAW_TO_UNIBYTE is a raw-text decode into a
/// unibyte destination, which produce_chars fills with the bytes
/// ONE_MORE_BYTE reads (a character's negative code's low eight bits).
fn decode_multibyte_source_text(
    interp: &Interpreter,
    source: &StringLike,
    coding: &str,
    raw_to_unibyte: bool,
) -> Result<DecodedText, LispError> {
    let mut decoded = DecodedText::new();
    let mut run: Vec<u8> = Vec::new();
    for code in source.character_codes_iter() {
        let code = code as u32;
        if code < 0x80 {
            run.push(code as u8);
            continue;
        }
        if RAW_BYTE8_CHARS.contains(&code) {
            run.push((code - RAW_BYTE8_BASE) as u8);
            continue;
        }
        flush_decoded_source_run(interp, &mut run, &mut decoded, coding, raw_to_unibyte)?;
        if raw_to_unibyte {
            decoded.push_byte(((-i64::from(code)) & 0xFF) as u8);
        } else {
            decoded.push_code(code);
        }
    }
    flush_decoded_source_run(interp, &mut run, &mut decoded, coding, raw_to_unibyte)?;
    Ok(decoded)
}

fn flush_decoded_source_run(
    interp: &Interpreter,
    run: &mut Vec<u8>,
    decoded: &mut DecodedText,
    coding: &str,
    for_unibyte: bool,
) -> Result<(), LispError> {
    if run.is_empty() {
        return Ok(());
    }
    if for_unibyte {
        for &byte in run.iter() {
            decoded.push_byte(byte);
        }
    } else {
        decoded.append(decode_bytes_with_coding(interp, run, coding)?);
    }
    run.clear();
    Ok(())
}

/// For each character index of STAGED, how many characters the eol
/// conversion (CRLF -> LF when CRLF, else CR -> LF) removes before it:
/// the offsets of annotations made on the staged text move by that much
/// once decode_eol has deleted the CRs.
fn eol_removed_prefix(staged: &str, crlf: bool) -> Vec<usize> {
    let mut removed = Vec::with_capacity(staged.len() + 1);
    let mut count = 0;
    let mut previous = '\0';
    for ch in staged.chars() {
        removed.push(count);
        if crlf && previous == '\r' && ch == '\n' {
            count += 1;
        }
        previous = ch;
    }
    removed.push(count);
    removed
}

/// The `charset' runs as text property spans on the converted text.
fn charset_spans_to_props(
    charsets: Vec<(usize, usize, String)>,
    staged: &str,
    crlf_conversion: Option<bool>,
) -> Vec<TextPropertySpan> {
    let removed = crlf_conversion.map(|crlf| eol_removed_prefix(staged, crlf));
    let adjust = |offset: usize| match &removed {
        Some(removed) => {
            let index = offset.min(removed.len() - 1);
            offset - removed[index]
        }
        None => offset,
    };
    charsets
        .into_iter()
        .filter_map(|(start, end, charset)| {
            let (start, end) = (adjust(start), adjust(end));
            (end > start).then(|| TextPropertySpan {
                start,
                end,
                props: vec![(
                    crate::lisp::types::PropertyName::symbol("charset"),
                    Value::Symbol(charset.into()),
                )],
            })
        })
        .collect()
}

/// A text decode_coding_object reads.
pub(crate) enum DecodeSource<'a> {
    /// The bytes of a unibyte text: a unibyte string's or buffer's, a
    /// file's or a process's.
    Bytes(&'a [u8]),
    /// A string's or a buffer region's characters.
    Text(&'a StringLike),
}

/// What decode_coding_object leaves behind.
pub(crate) struct CodingDecode {
    pub(crate) decoded: DecodedText,
    /// CODING_ID_NAME once detect_coding and decode_eol have settled it:
    /// the coding as given (an alias stays its name) unless detection
    /// found another or the eol decided a subsidiary.
    pub(crate) coding: String,
    /// The settled coding's CODING_FOR_UNIBYTE: a string decoded with it
    /// is unibyte.
    pub(crate) for_unibyte: bool,
}

/// CODING_ID_EOL_TYPE as a settled type (0 unix, 1 dos, 2 mac), or None
/// for an undecided one's vector of subsidiaries.
pub(crate) fn coding_eol_type(interp: &Interpreter, coding: &str) -> Option<i64> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .unwrap_or_else(|| coding.to_string());
    if matches!(canonical.as_str(), "no-conversion" | "binary") {
        // `binary' and `no-conversion' are unix in GNU; the native
        // accessor keeps them out of the eol-variant table.
        return Some(0);
    }
    interp.coding_system_eol_type_value(&canonical)
}

/// coding.c decode_coding_object without its destination: SOURCE decoded
/// by CODING into a destination whose multibyteness is DST_MULTIBYTE (a
/// buffer's), or None for a string, multibyte unless the settled coding
/// is for unibyte text.  detect_coding settles an undecided or auto-BOM
/// coding first, carrying CODING's specified eol type over; the settled
/// coding's decoder runs; then decode_eol, unless the eol is unix or
/// `inhibit-eol-conversion' is set, decides an undecided eol from the
/// decoded text (naming its subsidiary) and converts.
pub(crate) fn decode_coding_object(
    interp: &Interpreter,
    env: &Env,
    source: DecodeSource<'_>,
    coding: &str,
    dst_multibyte: Option<bool>,
) -> Result<CodingDecode, LispError> {
    decode_coding(interp, env, source, coding, dst_multibyte, false)
}

/// decode_coding_gap: decode_coding_object for a file's BYTES, read into
/// a buffer whose multibyteness is DST_MULTIBYTE, with its ASCII
/// optimization.
pub(crate) fn decode_coding_gap(
    interp: &Interpreter,
    env: &Env,
    bytes: &[u8],
    coding: &str,
    dst_multibyte: bool,
) -> Result<CodingDecode, LispError> {
    decode_coding(
        interp,
        env,
        DecodeSource::Bytes(bytes),
        coding,
        Some(dst_multibyte),
        true,
    )
}

fn decode_coding(
    interp: &Interpreter,
    env: &Env,
    source: DecodeSource<'_>,
    coding: &str,
    dst_multibyte: Option<bool>,
    gap: bool,
) -> Result<CodingDecode, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    // coding->src_multibyte = chars < bytes: a multibyte text of ASCII
    // characters alone is read as its bytes.
    let (bytes, text) = match source {
        DecodeSource::Bytes(bytes) => (std::borrow::Cow::Borrowed(bytes), None),
        DecodeSource::Text(string) if !string.multibyte => (
            std::borrow::Cow::Owned(encode_raw_text_bytes(&string.text)?),
            None,
        ),
        DecodeSource::Text(string) if string.text.is_ascii() => {
            (std::borrow::Cow::Borrowed(string.text.as_bytes()), None)
        }
        DecodeSource::Text(string) => (std::borrow::Cow::Borrowed(&[][..]), Some(string)),
    };
    let mut name = coding.to_string();
    let mut settled = canonical.clone();
    let mut detection = detect::DetectOutcome::default();
    if interp.coding_system_kind_name(&canonical).as_deref() == Some("undecided")
        || coding_system_auto_detects_bom(interp, &canonical)
    {
        let src = match text {
            Some(string) => detect::DetectSource::from_codes(string.character_codes_iter()),
            None => detect::DetectSource::from_bytes(&bytes),
        };
        // decode_coding_gap detects before it sets CODING_MODE_LAST_BLOCK.
        let src = if gap { src.not_last_block() } else { src };
        detection = detect::detect_coding_outcome(interp, &src, &canonical, env);
        if let Some(found) = &detection.found {
            name = adjust_coding_eol_type(interp, found, coding_eol_type(interp, &canonical));
            settled = interp
                .coding_system_canonical_name(&name)
                .unwrap_or_else(|| name.clone());
        }
    }
    let for_unibyte = coding_system_property(interp, &settled, ":for-unibyte")
        .is_some_and(|value| value.is_truthy());
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // decode_coding_gap's ASCII optimization: with an ascii-compatible
    // coding, no post-read conversion and no translation table, ASCII
    // bytes -- or bytes detect_coding_utf_8 accepted to their end, for a
    // utf-8 coding -- go in as they are.  The eol bits detection saw (its
    // head scan, or the check_ascii over the ASCII head of an auto-BOM
    // detection), else check_ascii's (only LF under
    // `inhibit-eol-conversion' or a settled eol), still name an undecided
    // eol's subsidiary, and a dos or mac eol still converts.
    let utf_8_whole = detection.utf8_whole
        && interp.coding_system_kind_name(&settled).as_deref() == Some("utf-8");
    if gap
        && text.is_none()
        && (bytes.is_ascii() || utf_8_whole)
        && interp
            .lookup_var("disable-ascii-optimization", env)
            .is_none_or(|value| value.is_nil())
        && coding_system_is_ascii_compatible(interp, &settled)
        && coding_system_property(interp, &settled, ":post-read-conversion")
            .is_none_or(|value| value.is_nil())
        && !decode_translation_table_p(interp, &settled, env)
    {
        let mut decoded = if bytes.is_ascii() {
            let mut decoded = DecodedText::new();
            decoded.push_ascii(&bytes);
            decoded
        } else {
            decode_coding_utf_8(&bytes)
        };
        let mut eol_type = coding_eol_type(interp, &settled);
        if eol_type.is_none() {
            let seen = match detection.eol_seen {
                Some(seen) => seen,
                None if inhibit_eol_conversion => {
                    if bytes.contains(&b'\n') {
                        detect::EOL_SEEN_LF
                    } else {
                        detect::EOL_SEEN_NONE
                    }
                }
                None => decoded.eol_seen(),
            };
            eol_type = detect::eol_type_for_seen(seen);
            if eol_type.is_some() {
                name = adjust_coding_eol_type(interp, &name, eol_type);
            }
        }
        if let Some(eol_type) = eol_type {
            decoded.convert_eol(eol_type);
        }
        return Ok(CodingDecode {
            decoded,
            coding: name,
            for_unibyte,
        });
    }
    // produce_chars copies the source of a raw-text decode (undecided
    // that detected nothing decodes as raw text too): unchanged into a
    // multibyte destination, and from a multibyte source into a unibyte
    // one as the bytes ONE_MORE_BYTE reads.
    let dst_multibyte = dst_multibyte.unwrap_or(!for_unibyte);
    let mut decoded = match text {
        Some(string) => decode_multibyte_source_text(
            interp,
            string,
            &settled,
            !dst_multibyte
                && matches!(
                    interp.coding_system_kind_name(&settled).as_deref(),
                    Some("raw-text" | "undecided")
                ),
        )?,
        None => decode_bytes_with_coding(interp, &bytes, &settled)?,
    };
    if !inhibit_eol_conversion {
        let eol_type = match coding_eol_type(interp, &settled) {
            Some(eol_type) => Some(eol_type),
            None => {
                // Stray CRs among CRLFs are dos; any other mixture is unix.
                let seen = match decoded.eol_seen() {
                    seen if seen == detect::EOL_SEEN_CRLF | detect::EOL_SEEN_CR => {
                        detect::EOL_SEEN_CRLF
                    }
                    detect::EOL_SEEN_NONE => detect::EOL_SEEN_NONE,
                    seen @ (detect::EOL_SEEN_LF | detect::EOL_SEEN_CRLF | detect::EOL_SEEN_CR) => {
                        seen
                    }
                    _ => detect::EOL_SEEN_LF,
                };
                let eol_type = detect::eol_type_for_seen(seen);
                if eol_type.is_some() {
                    name = adjust_coding_eol_type(interp, &name, eol_type);
                }
                eol_type
            }
        };
        if let Some(eol_type) = eol_type {
            decoded.convert_eol(eol_type);
        }
    }
    Ok(CodingDecode {
        decoded,
        coding: name,
        for_unibyte,
    })
}

/// Decode VALUE with CODING. code_convert_string's ASCII fast path applies
/// only to a string result; a source region or a buffer destination goes
/// straight through decode_coding_object (BUFFER_CONVERSION).
pub(crate) fn decode_coding_text(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    buffer_conversion: bool,
    dst_multibyte: Option<bool>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let Some(coding) = coding else {
        set_last_coding_system_used(interp, "no-conversion", env);
        return if nocopy {
            Ok(*value)
        } else {
            shared_string_copy(value)
        };
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // code_convert_string's fast path: an ascii-compatible coding, an
    // ASCII string (a multibyte one whose characters are its bytes) and
    // nothing for eol conversion to do return the string as it is -- the
    // decoder never runs, and neither does detection.  A region or a
    // buffer destination never takes it.
    let fast_path = !buffer_conversion
        && coding_system_is_ascii_compatible(interp, &canonical)
        && string.text.is_ascii()
        && (coding_eol_type(interp, &canonical) == Some(0)
            || inhibit_eol_conversion
            || !string.text.contains('\r'));
    if fast_path {
        // A copied result is a fresh, unpropertized multibyte ASCII string;
        // NOCOPY gives back the original object, properties and all.
        set_last_coding_system_used(interp, coding, env);
        return Ok(if nocopy {
            *value
        } else {
            make_shared_string_value_with_multibyte(string.text, Vec::new(), true)
        });
    }
    let CodingDecode {
        decoded,
        coding: actual_coding,
        for_unibyte,
    } = decode_coding_object(
        interp,
        env,
        DecodeSource::Text(&string),
        coding,
        dst_multibyte,
    )?;
    // produce_annotation's `charset' properties of the decoded text.
    let charset_props = charset_spans_to_props(decoded.charsets, &decoded.text, None);
    // decode_coding_object makes the work buffer multibyte unless the
    // coding detect_coding settled on is for unibyte text.
    let post_read = coding_system_property(interp, &actual_coding, ":post-read-conversion")
        .unwrap_or(Value::Nil);
    let conversion_ran = !post_read.is_nil();
    let decoded = StringLike {
        text: decoded.text,
        props: Vec::new(),
        // A buffer destination takes the characters produced (written
        // into a unibyte one in their multibyte form, by the caller); a
        // string is unibyte for a coding for unibyte text.
        multibyte: dst_multibyte.is_some() || !for_unibyte,
        extended_chars: decoded.extended,
    };
    let converted = run_coding_conversion(interp, &decoded, &post_read, false, env)?;
    // code_convert_string records CODING_ID_NAME once decode_coding_object,
    // post-read conversion included, has returned.
    set_last_coding_system_used(interp, &actual_coding, env);
    // A post-read conversion rewrites the text, after which the decoder's
    // annotations cannot be placed.
    let props = if conversion_ran {
        Vec::new()
    } else {
        charset_props
    };
    Ok(make_shared_string_value_with_extended_chars(
        converted.text,
        props,
        converted.multibyte,
        converted.extended_chars,
    ))
}

/// charset.c CHARSET_MIN_CODE / CHARSET_MAX_CODE.
fn charset_code_bounds(interp: &Interpreter, charset: &str) -> Option<(u32, u32)> {
    charset_info(interp, charset)?.code_bounds
}

fn push_sorted_char_ranges(chars: &mut Vec<u32>, ranges: &mut Vec<(i64, i64)>) {
    chars.sort_unstable();
    chars.dedup();
    let mut index = 0;
    while index < chars.len() {
        let start = chars[index];
        let mut end = start;
        while index + 1 < chars.len() && chars[index + 1] == end + 1 {
            index += 1;
            end = chars[index];
        }
        ranges.push((i64::from(start), i64::from(end)));
        index += 1;
    }
}

/// charset.c map_charset_chars: the character ranges FUNCTION receives for
/// codes FROM..TO of CHARSET.  A MAP charset walks its encoder -- maximal
/// unicode-ascending runs of the mapped characters; a unified OFFSET
/// charset walks its deunifier the same way and then appends the raw
/// code-offset range; a plain OFFSET charset yields the arithmetic range;
/// SUBSET and SUPERSET charsets recurse through their relatives.
/// (characters.el's CJK syntax/category rules run through this: the "_"
/// rows of korean-ksc5601 carry the euro sign among others, which is how
/// GNU's standard syntax table gives U+20AC symbol syntax.)
pub(crate) fn map_charset_char_ranges(
    interp: &Interpreter,
    charset: &str,
    from: i64,
    to: i64,
) -> Option<Vec<(i64, i64)>> {
    let canonical = interp.charset_canonical_name(charset)?;
    let mut ranges = Vec::new();

    if let Some((child, min, max, offset)) = charset_subset(interp, &canonical) {
        let from = (from - offset).max(min);
        let to = (to - offset).min(max);
        if from <= to {
            ranges.extend(map_charset_char_ranges(interp, &child, from, to)?);
        }
        return Some(ranges);
    }
    if let Some(children) = charset_superset(interp, &canonical) {
        for (child, offset) in children {
            let Some((child_min, child_max)) = charset_code_bounds(interp, &child) else {
                continue;
            };
            let from = (from - offset).max(i64::from(child_min));
            let to = (to - offset).min(i64::from(child_max));
            if from <= to
                && let Some(child_ranges) = map_charset_char_ranges(interp, &child, from, to)
            {
                ranges.extend(child_ranges);
            }
        }
        return Some(ranges);
    }

    let (min_code, max_code) = charset_code_bounds(interp, &canonical)?;
    let from = u32::try_from(from.max(i64::from(min_code))).ok()?;
    let to = u32::try_from(to.min(i64::from(max_code))).ok()?;
    if from > to {
        return Some(ranges);
    }

    let info = charset_info(interp, &canonical)?;
    let offset = info.code_offset;
    let has_direct_map = info.has_direct_map;
    let unified = info.unified && offset.is_some();
    if (has_direct_map || unified)
        && let Some(map) = info.map(interp)
    {
        let mut chars: Vec<u32> = map
            .entries
            .iter()
            .filter(|(code, _)| (from..=to).contains(code))
            .map(|(_, character)| *character)
            .collect();
        push_sorted_char_ranges(&mut chars, &mut ranges);
    }
    if let Some(offset) = offset.filter(|_| !has_direct_map) {
        let from_index = info.code_to_index(from)?;
        let to_index = info.code_to_index(to)?;
        ranges.push((i64::from(from_index) + offset, i64::from(to_index) + offset));
    }
    Some(ranges)
}
