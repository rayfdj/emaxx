use super::*;
use encoding_rs::{Encoding, KOI8_R, WINDOWS_1251, WINDOWS_1252};

pub(crate) fn coding_system_error(name: impl Into<String>) -> LispError {
    let name = name.into();
    LispError::SignalValue(Value::list([
        Value::Symbol("coding-system-error".into()),
        Value::String(format!("Invalid coding system: {name}").into()),
    ]))
}

fn charset_plist_property(interp: &Interpreter, charset: &str, property: &str) -> Option<Value> {
    let items = interp.charset_plist_value(charset)?.to_vec().ok()?;
    items.windows(2).find_map(|pair| {
        matches!(&pair[0], Value::Symbol(name) if name == property).then(|| pair[1].clone())
    })
}

fn parse_charset_map_number(value: &str) -> Option<u32> {
    u32::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}

fn parse_charset_map(contents: &str) -> Vec<(u32, u32)> {
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
        mappings.extend(
            (code_start..=code_end)
                .enumerate()
                .map(|(offset, code)| (code, character_start + offset as u32)),
        );
    }
    mappings
}

fn charset_map(interp: &Interpreter, charset: &str) -> Option<Vec<(u32, u32)>> {
    // Unified charsets (charset.c's UNIFIED_P, set by `unify-charset' --
    // mule-conf.el unifies the CJK offset charsets at load) convert
    // through their :unify-map table; only codes absent from the table
    // fall back to the code-offset rule.
    let map = charset_plist_property(interp, charset, ":map").or_else(|| {
        interp
            .charset_is_unified(charset)
            .then(|| charset_plist_property(interp, charset, ":unify-map"))
            .flatten()
    })?;
    if is_vector_value(&map) {
        let values = map.to_vec().ok()?;
        let values = values
            .strip_prefix(&[Value::Symbol("vector-literal".into())])
            .unwrap_or(&values);
        return Some(
            values
                .chunks_exact(2)
                .filter_map(|pair| {
                    Some((
                        u32::try_from(pair[0].as_integer().ok()?).ok()?,
                        u32::try_from(pair[1].as_integer().ok()?).ok()?,
                    ))
                })
                .collect(),
        );
    }
    let map_name = string_text(&map).ok()?;
    let data_directory = interp
        .lookup_var("data-directory", &Vec::new())
        .and_then(|value| string_like(&value).map(|string| PathBuf::from(string.text)))
        .or_else(|| compat_data_directory().map(PathBuf::from))?;
    let path = data_directory
        .join("charsets")
        .join(format!("{map_name}.map"));

    type CharsetMapCache = HashMap<PathBuf, Vec<(u32, u32)>>;
    static CACHE: OnceLock<Mutex<CharsetMapCache>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(map) = cache.lock().ok()?.get(&path).cloned() {
        return Some(map);
    }
    let parsed = parse_charset_map(&fs::read_to_string(&path).ok()?);
    cache.lock().ok()?.insert(path, parsed.clone());
    Some(parsed)
}

/// A `:subset (PARENT MIN MAX OFFSET)' charset (e.g. latin-jisx0201 and
/// katakana-jisx0201, both windows onto jisx0201) converts through PARENT:
/// its code C corresponds to PARENT's code C - OFFSET, valid when the
/// parent code lies in MIN..=MAX.
fn charset_subset(interp: &Interpreter, charset: &str) -> Option<(String, i64, i64, i64)> {
    let subset = charset_plist_property(interp, charset, ":subset")?;
    let items = subset.to_vec().ok()?;
    if items.len() != 4 {
        return None;
    }
    Some((
        items[0].as_symbol().ok()?.to_string(),
        items[1].as_integer().ok()?,
        items[2].as_integer().ok()?,
        items[3].as_integer().ok()?,
    ))
}

/// A `:superset (CHILD ...)' charset (japanese-jisx0213.2004-1 is the
/// superset of jisx0213-a and jisx0213-1) shares its children's code
/// space: conversion tries each child in order.  An entry may also be
/// (CHILD . OFFSET), shifting the codes by OFFSET.
fn charset_superset(interp: &Interpreter, charset: &str) -> Option<Vec<(String, i64)>> {
    let superset = charset_plist_property(interp, charset, ":superset")?;
    let items = superset.to_vec().ok()?;
    let children: Vec<(String, i64)> = items
        .iter()
        .filter_map(|item| match item {
            Value::Symbol(name) => Some((name.to_string(), 0)),
            other => {
                let (car, cdr) = other.cons_values()?;
                Some((car.as_symbol().ok()?.to_string(), cdr.as_integer().ok()?))
            }
        })
        .collect();
    (!children.is_empty()).then_some(children)
}

pub(crate) fn decode_charset_code(interp: &Interpreter, charset: &str, code: u32) -> Option<u32> {
    let canonical = interp.charset_canonical_name(charset)?;
    match canonical.as_str() {
        "ascii" if code <= 0x7f => return Some(code),
        "ascii" => return None,
        "iso-8859-1" if code <= 0xff => return Some(code),
        "iso-8859-1" => return None,
        "unicode" if code <= 0x10_ffff => return Some(code),
        "emacs" if code <= 0x3f_ffff => return Some(code),
        "eight-bit" if (0x80..=0xff).contains(&code) => {
            return Some(RAW_BYTE_REGEX_BASE + code);
        }
        "eight-bit" => return None,
        _ => {}
    }
    if let Some(map) = charset_map(interp, &canonical)
        && let Some((_, character)) = map.iter().find(|(mapped_code, _)| *mapped_code == code)
    {
        return Some(*character);
    }
    if let Some(children) = charset_superset(interp, &canonical) {
        return children.iter().find_map(|(child, offset)| {
            let child_code = u32::try_from(i64::from(code).checked_sub(*offset)?).ok()?;
            decode_charset_code(interp, child, child_code)
        });
    }
    if let Some((parent, min, max, offset)) = charset_subset(interp, &canonical) {
        let parent_code = i64::from(code).checked_sub(offset)?;
        if !(min..=max).contains(&parent_code) {
            return None;
        }
        return decode_charset_code(interp, &parent, u32::try_from(parent_code).ok()?);
    }
    let offset = charset_plist_property(interp, &canonical, ":code-offset")?
        .as_integer()
        .ok()?;
    let index = charset_code_to_index(interp, &canonical, code).unwrap_or(code);
    u32::try_from(i64::from(index).checked_add(offset)?).ok()
}

/// charset.c's CODE_POINT_TO_INDEX: the ordinal of CODE within the
/// charset's :code-space, counting the first (least significant) byte
/// fastest.  jisx0208's hole 0x222F is index 108, so its non-unified
/// character is code-offset + 108, not code-offset + 0x222F.
fn charset_code_space(interp: &Interpreter, charset: &str) -> Option<Vec<(u32, u32)>> {
    let space = charset_plist_property(interp, charset, ":code-space")?;
    let values = space.to_vec().ok()?;
    let values = values
        .strip_prefix(&[Value::Symbol("vector-literal".into())])
        .unwrap_or(&values);
    let bounds: Vec<(u32, u32)> = values
        .chunks_exact(2)
        .filter_map(|pair| {
            Some((
                u32::try_from(pair[0].as_integer().ok()?).ok()?,
                u32::try_from(pair[1].as_integer().ok()?).ok()?,
            ))
        })
        .collect();
    (!bounds.is_empty()).then_some(bounds)
}

fn charset_code_to_index(interp: &Interpreter, charset: &str, code: u32) -> Option<u32> {
    let bounds = charset_code_space(interp, charset)?;
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

fn charset_index_to_code(interp: &Interpreter, charset: &str, index: u32) -> Option<u32> {
    let bounds = charset_code_space(interp, charset)?;
    let mut code = 0u32;
    let mut remaining = index;
    for (dimension, (min, max)) in bounds.iter().enumerate() {
        let size = max - min + 1;
        code |= (min + remaining % size) << (8 * dimension);
        remaining /= size;
    }
    (remaining == 0).then_some(code)
}

pub(crate) fn encode_charset_char(
    interp: &Interpreter,
    charset: &str,
    character: u32,
) -> Option<u32> {
    let canonical = interp.charset_canonical_name(charset)?;
    match canonical.as_str() {
        "ascii" if character <= 0x7f => return Some(character),
        "ascii" => return None,
        "iso-8859-1" if character <= 0xff => return Some(character),
        "iso-8859-1" => return None,
        "unicode" if character <= 0x10_ffff => return Some(character),
        "emacs" if character <= 0x3f_ffff => return Some(character),
        "eight-bit"
            if (RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xff).contains(&character) =>
        {
            return Some(character - RAW_BYTE_REGEX_BASE);
        }
        "eight-bit" => return None,
        _ => {}
    }
    if let Some(map) = charset_map(interp, &canonical)
        && let Some((code, _)) = map
            .iter()
            .find(|(_, mapped_character)| *mapped_character == character)
    {
        return Some(*code);
    }
    if let Some(children) = charset_superset(interp, &canonical) {
        return children.iter().find_map(|(child, offset)| {
            let child_code = encode_charset_char(interp, child, character)?;
            u32::try_from(i64::from(child_code).checked_add(*offset)?).ok()
        });
    }
    if let Some((parent, min, max, offset)) = charset_subset(interp, &canonical) {
        let parent_code = i64::from(encode_charset_char(interp, &parent, character)?);
        if !(min..=max).contains(&parent_code) {
            return None;
        }
        return u32::try_from(parent_code.checked_add(offset)?).ok();
    }
    let offset = charset_plist_property(interp, &canonical, ":code-offset")?
        .as_integer()
        .ok()?;
    let index = u32::try_from(i64::from(character).checked_sub(offset)?).ok()?;
    Some(charset_index_to_code(interp, &canonical, index).unwrap_or(index))
}

fn coding_system_property(interp: &Interpreter, coding: &str, property: &str) -> Option<Value> {
    let items = interp.coding_system_plist_value(coding)?.to_vec().ok()?;
    items.windows(2).find_map(|pair| {
        matches!(&pair[0], Value::Symbol(name) if name == property).then(|| pair[1].clone())
    })
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
    let big_endian = match coding_system_property(interp, coding, ":endian") {
        Some(Value::Symbol(endian)) => endian != "little",
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
        .filter_map(|value| value.as_symbol().ok().map(str::to_string))
        .collect()
}

fn run_coding_conversion(
    interp: &mut Interpreter,
    text: &str,
    function: &Value,
    pre_write: bool,
    env: &mut Env,
) -> Result<String, LispError> {
    if function.is_nil() {
        return Ok(text.to_string());
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
    interp.insert_current_buffer(text);
    // GNU invokes a post-read conversion with point at the beginning of the
    // newly decoded span.  Lisp decoders such as `utf-7-decode' consume LEN
    // bytes starting at point, so leaving point after the insertion silently
    // turns the conversion into a no-op.
    if !pre_write {
        interp.buffer.goto_char(interp.buffer.point_min());
    }
    let arguments = if pre_write {
        vec![
            Value::Integer(interp.buffer.point_min() as i64),
            Value::Integer(interp.buffer.point_max() as i64),
        ]
    } else {
        vec![Value::Integer(text.len() as i64)]
    };
    let result = interp.call_function_value(function.clone(), None, &arguments, env);
    let result_buffer_id = interp.current_buffer_id();
    let converted = interp.buffer.buffer_string();
    let _ = interp.set_current_buffer_id(saved_buffer_id);
    if result_buffer_id != saved_buffer_id && result_buffer_id != temp_id {
        interp.kill_buffer_id(result_buffer_id);
    }
    interp.kill_buffer_id(temp_id);
    result?;
    Ok(converted)
}

fn encode_charset_coding_bytes(
    interp: &Interpreter,
    text: &str,
    coding: &str,
) -> Result<Vec<u8>, LispError> {
    let charsets = coding_system_charset_names(interp, coding);
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    let mut encoded = Vec::new();
    for character in text.chars() {
        let scalar = raw_byte_from_regex_char(character)
            .map(u32::from)
            .unwrap_or(character as u32);
        if ascii_compatible && scalar <= 0x7f {
            encoded.push(scalar as u8);
            continue;
        }
        let code = charsets
            .iter()
            .find_map(|charset| encode_charset_char(interp, charset, scalar));
        let code = if let Some(code) = code {
            code
        } else if ascii_compatible {
            encoded.push(b' ');
            continue;
        } else {
            charsets
                .iter()
                .find_map(|charset| encode_charset_char(interp, charset, u32::from(b' ')))
                .ok_or_else(|| LispError::Signal("Character cannot be encoded".into()))?
        };
        let bytes = code.to_be_bytes();
        let first = bytes
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(bytes.len() - 1);
        encoded.extend_from_slice(&bytes[first..]);
    }
    Ok(encoded)
}

fn decode_charset_coding_bytes(interp: &Interpreter, bytes: &[u8], coding: &str) -> String {
    let charsets = coding_system_charset_names(interp, coding);
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    bytes
        .iter()
        .map(|byte| {
            if ascii_compatible && *byte <= 0x7f {
                return char::from(*byte);
            }
            charsets
                .iter()
                .find_map(|charset| decode_charset_code(interp, charset, u32::from(*byte)))
                .and_then(char::from_u32)
                .unwrap_or_else(|| raw_byte_regex_char(*byte))
        })
        .collect()
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
        .ok_or_else(|| coding_system_error(symbol.clone()))
        .map(Some)
}

pub(crate) fn checked_coding_symbol(
    interp: &Interpreter,
    value: &Value,
) -> Result<String, LispError> {
    checked_coding_name(interp, value)?.ok_or_else(|| coding_system_error("nil"))
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
    let string = string_like(value)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value.clone()))?;
    Ok(make_shared_string_value_with_multibyte(
        string.text,
        string.props,
        string.multibyte,
    ))
}

pub(crate) fn bytes_to_unibyte_value(bytes: &[u8]) -> Value {
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
        Value::String(text.into())
    }
}

pub(crate) fn bytes_to_shared_unibyte_value(bytes: &[u8]) -> Value {
    let mut text = String::new();
    for &byte in bytes {
        if byte <= 0x7F {
            text.push(byte as char);
        } else {
            text.push(raw_byte_regex_char(byte));
        }
    }
    make_shared_string_value_with_multibyte(text, Vec::new(), false)
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
    if is_vector_value(target) {
        let slot = vector_slot_refs(target)?
            .get(index)
            .cloned()
            .ok_or_else(|| LispError::Signal("Args out of range".into()))?;
        *slot.borrow_mut() = new_value;
        return Ok(());
    }

    let mut current = target.clone();
    let mut offset = 0usize;
    loop {
        match current {
            Value::Cons(cons_cell) => {
                let car = &cons_cell.car;
                let cdr = &cons_cell.cdr;
                if offset == index {
                    *car.borrow_mut() = new_value;
                    return Ok(());
                }
                offset += 1;
                current = cdr.borrow().clone();
            }
            Value::Nil => return Err(LispError::Signal("Args out of range".into())),
            _ => {
                return Err(LispError::WrongTypeArgument(
                    "arrayp".into(),
                    target.clone(),
                ));
            }
        }
    }
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
    let string = string_like(value)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value.clone()))?;
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
) -> Result<Value, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    let text = interp
        .buffer
        .buffer_substring(lo, hi)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let bytes = encode_base64_source_bytes(&text, interp.buffer.is_multibyte())?;
    let encoded = encode_base64_bytes(&bytes, line_break, pad, base64url);
    let new_end = replace_buffer_region_with_text(interp, lo, hi, &encoded)?;
    Ok(Value::Integer((new_end - lo) as i64))
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
    let string = string_like(value)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value.clone()))?;
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
) -> Result<Value, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    let text = interp
        .buffer
        .buffer_substring(lo, hi)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let decoded =
        decode_base64_string_value(&Value::String(text.into()), base64url, ignore_invalid)?;
    let decoded_text = string_text(&decoded)?;
    let new_end = replace_buffer_region_with_text(interp, lo, hi, &decoded_text)?;
    Ok(Value::Integer((new_end - lo) as i64))
}

pub(crate) fn ascii_only_text(text: &str) -> bool {
    text.chars()
        .all(|ch| raw_byte_from_regex_char(ch).unwrap_or_default() <= 0x7F && (ch as u32) <= 0x7F)
}

pub(crate) fn strip_utf8_bom(bytes: &[u8]) -> (bool, &[u8]) {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        (true, &bytes[3..])
    } else {
        (false, bytes)
    }
}

/// Like `detect_eol_type', but reports None when the bytes contain no
/// line-ending byte at all: GNU's detector leaves the eol UNDECIDED then,
/// and the detected coding keeps its bare base name (`undecided', `utf-8')
/// instead of gaining a `-unix' suffix.
pub(crate) fn detect_eol_type_opt(bytes: &[u8]) -> Option<i64> {
    bytes
        .iter()
        .any(|byte| matches!(byte, b'\r' | b'\n'))
        .then(|| detect_eol_type(bytes))
}

pub(crate) fn detect_eol_type(bytes: &[u8]) -> i64 {
    let mut saw_crlf = false;
    let mut saw_lf = false;
    let mut saw_cr = false;
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'\r' if bytes.get(index + 1) == Some(&b'\n') => {
                saw_crlf = true;
                index += 2;
                continue;
            }
            b'\r' => saw_cr = true,
            b'\n' => saw_lf = true,
            _ => {}
        }
        index += 1;
    }

    if saw_crlf && !saw_lf {
        1
    } else if saw_cr && !saw_crlf && !saw_lf {
        2
    } else {
        0
    }
}

pub(crate) fn decode_bytes_with_explicit_eol(bytes: &[u8], eol_type: i64) -> Vec<u8> {
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

pub(crate) fn encode_text_with_eol(text: &str, eol_type: Option<i64>) -> String {
    match eol_type {
        Some(1) => text.replace('\n', "\r\n"),
        Some(2) => text.replace('\n', "\r"),
        _ => text.to_string(),
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

pub(crate) fn encode_iso_latin_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    encode_raw_text_bytes(text)
}

fn legacy_single_byte_encoding(interp: &Interpreter, coding: &str) -> Option<&'static Encoding> {
    match interp.coding_system_base_name(coding)?.as_str() {
        "cyrillic-koi8" => Some(KOI8_R),
        "windows-1251" => Some(WINDOWS_1251),
        "windows-1252" => Some(WINDOWS_1252),
        _ => None,
    }
}

fn encode_legacy_single_byte_bytes(
    encoding: &'static Encoding,
    text: &str,
) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::with_capacity(text.len());
    for character in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(character) {
            bytes.push(byte);
            continue;
        }
        let text = character.to_string();
        let (encoded, _, had_errors) = encoding.encode(&text);
        if had_errors {
            return Err(LispError::Signal("Character cannot be encoded".into()));
        }
        bytes.extend_from_slice(&encoded);
    }
    Ok(bytes)
}

fn decode_legacy_single_byte_bytes(encoding: &'static Encoding, bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| {
            encoding
                .decode_without_bom_handling_and_without_replacement(std::slice::from_ref(byte))
                .and_then(|decoded| decoded.chars().next())
                .unwrap_or_else(|| raw_byte_regex_char(*byte))
        })
        .collect()
}

pub(crate) fn encode_ascii_bytes(text: &str) -> Result<Vec<u8>, LispError> {
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

/// EUC-JP through the charset tables (coding.c's iso-2022 encoder for
/// japanese-iso-8bit): ASCII stays single-byte, JIS X 0208 becomes the
/// two-byte high bank, halfwidth katakana rides SS2 (0x8E), JIS X 0212
/// rides SS3 (0x8F).  Characters of latin-jisx0201 (yen sign, overline)
/// are designated into G0 with ESC ( J and restored to ASCII with
/// ESC ( B before controls/eol and at the end (the coding system's
/// ascii-at-eol/ascii-at-cntl flags); an unencodable character becomes
/// a space, and raw-byte markers pass through as their byte.  Each of
/// these is the oracle's own answer -- see the regression contract.
pub(crate) fn encode_euc_jp_bytes(interp: &Interpreter, text: &str) -> Result<Vec<u8>, LispError> {
    let mut out = Vec::new();
    let mut g0_latin = false;
    let restore_ascii = |out: &mut Vec<u8>, g0_latin: &mut bool| {
        if *g0_latin {
            out.extend([0x1B, b'(', b'B']);
            *g0_latin = false;
        }
    };
    for ch in text.chars() {
        let code = ch as u32;
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            out.push(byte);
            continue;
        }
        if code < 0x20 || code == 0x7F {
            restore_ascii(&mut out, &mut g0_latin);
            out.push(code as u8);
            continue;
        }
        if code <= 0x7F {
            out.push(code as u8);
            continue;
        }
        if let Some(code) = encode_charset_char(interp, "japanese-jisx0208", code) {
            out.extend([(code >> 8) as u8 | 0x80, (code & 0xFF) as u8 | 0x80]);
            continue;
        }
        if let Some(code) = encode_charset_char(interp, "katakana-jisx0201", code) {
            out.extend([0x8E, code as u8 | 0x80]);
            continue;
        }
        if let Some(code) = encode_charset_char(interp, "japanese-jisx0212", code) {
            out.extend([0x8F, (code >> 8) as u8 | 0x80, (code & 0xFF) as u8 | 0x80]);
            continue;
        }
        if let Some(code) = encode_charset_char(interp, "latin-jisx0201", code) {
            if !g0_latin {
                out.extend([0x1B, b'(', b'J']);
                g0_latin = true;
            }
            out.push(code as u8);
            continue;
        }
        out.push(b' ');
    }
    restore_ascii(&mut out, &mut g0_latin);
    Ok(out)
}

/// The euc-jp decoder does NOT interpret ISO-2022 escapes (the coding
/// system lacks the `designation' flag): ESC sequences pass through as
/// literal characters, per the oracle.  A byte that cannot start or
/// complete a valid EUC sequence decodes as its raw-byte marker and the
/// scan resynchronizes at the next byte.
pub(crate) fn decode_euc_jp_bytes(interp: &Interpreter, bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut rest = bytes;
    let is_high = |byte: u8| (0xA1..=0xFE).contains(&byte);
    while let Some((&lead, tail)) = rest.split_first() {
        let (decoded, consumed) = match lead {
            0x00..=0x7F => (Some(char::from(lead)), 1),
            0x8E => match tail.first() {
                Some(&byte) if is_high(byte) => (
                    decode_charset_code(interp, "katakana-jisx0201", u32::from(byte & 0x7F))
                        .and_then(char::from_u32),
                    2,
                ),
                _ => (None, 1),
            },
            0x8F => match (tail.first(), tail.get(1)) {
                (Some(&hi), Some(&lo)) if is_high(hi) && is_high(lo) => (
                    decode_charset_code(
                        interp,
                        "japanese-jisx0212",
                        u32::from(hi & 0x7F) << 8 | u32::from(lo & 0x7F),
                    )
                    .and_then(char::from_u32),
                    3,
                ),
                _ => (None, 1),
            },
            _ if is_high(lead) => match tail.first() {
                Some(&lo) if is_high(lo) => (
                    decode_charset_code(
                        interp,
                        "japanese-jisx0208",
                        u32::from(lead & 0x7F) << 8 | u32::from(lo & 0x7F),
                    )
                    .and_then(char::from_u32),
                    2,
                ),
                _ => (None, 1),
            },
            _ => (None, 1),
        };
        match decoded {
            Some(ch) => {
                out.push(ch);
                rest = &rest[consumed..];
            }
            None => {
                out.push(raw_byte_regex_char(lead));
                rest = &rest[1..];
            }
        }
    }
    out
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

/// Shift-JIS through the same charset tables (coding.c's shift-jis codec
/// for japanese-shift-jis): ASCII single-byte, halfwidth katakana as its
/// code + 0x80, JIS X 0208 as the two-byte code through JIS_TO_SJIS, a
/// space for unencodable characters and raw-byte markers as their byte.
pub(crate) fn encode_sjis_bytes(interp: &Interpreter, text: &str) -> Result<Vec<u8>, LispError> {
    let mut out = Vec::new();
    for ch in text.chars() {
        let code = ch as u32;
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            out.push(byte);
            continue;
        }
        if code <= 0x7F {
            out.push(code as u8);
            continue;
        }
        if let Some(code) = encode_charset_char(interp, "katakana-jisx0201", code) {
            out.push(code as u8 | 0x80);
            continue;
        }
        if let Some(code) = encode_charset_char(interp, "japanese-jisx0208", code) {
            let sjis = jis_to_sjis(code);
            out.extend([(sjis >> 8) as u8, (sjis & 0xFF) as u8]);
            continue;
        }
        out.push(b' ');
    }
    Ok(out)
}

/// The Shift-JIS decoder: 0xA1..0xDF is halfwidth katakana, 0x81..0x9F
/// and 0xE0..0xEF lead a two-byte code (trail 0x40..0xFC except 0x7F)
/// decoded through SJIS_TO_JIS and JIS X 0208; anything else is a raw
/// byte and the scan resynchronizes.  An unmapped code decodes in GNU
/// to a supra-Unicode codepoint, which emaxx strings cannot hold: those
/// fall back to raw-byte markers (the ledger's disclosed limitation).
pub(crate) fn decode_sjis_bytes(interp: &Interpreter, bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut rest = bytes;
    while let Some((&lead, tail)) = rest.split_first() {
        let (decoded, consumed) = match lead {
            0x00..=0x7F => (Some(char::from(lead)), 1),
            0xA1..=0xDF => (
                decode_charset_code(interp, "katakana-jisx0201", u32::from(lead) - 0x80)
                    .and_then(char::from_u32),
                1,
            ),
            0x81..=0x9F | 0xE0..=0xEF => match tail.first() {
                Some(&trail) if (0x40..=0xFC).contains(&trail) && trail != 0x7F => (
                    decode_charset_code(
                        interp,
                        "japanese-jisx0208",
                        sjis_to_jis(u32::from(lead) << 8 | u32::from(trail)),
                    )
                    .and_then(char::from_u32),
                    2,
                ),
                _ => (None, 1),
            },
            _ => (None, 1),
        };
        match decoded {
            Some(ch) => {
                out.push(ch);
                rest = &rest[consumed..];
            }
            None => {
                out.push(raw_byte_regex_char(lead));
                rest = &rest[1..];
            }
        }
    }
    out
}

/// Big5 through the BIG5 charset map: ASCII single-byte, everything the
/// map encodes as its two-byte code, a space for unencodable characters.
pub(crate) fn encode_big5_bytes(interp: &Interpreter, text: &str) -> Result<Vec<u8>, LispError> {
    let mut out = Vec::new();
    for ch in text.chars() {
        let code = ch as u32;
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            out.push(byte);
            continue;
        }
        if code <= 0x7F {
            out.push(code as u8);
            continue;
        }
        if let Some(code) = encode_charset_char(interp, "big5", code) {
            out.extend([(code >> 8) as u8, (code & 0xFF) as u8]);
            continue;
        }
        out.push(b' ');
    }
    Ok(out)
}

/// The Big5 decoder: a lead 0xA1..0xFE with trail 0x40..0x7E or
/// 0xA1..0xFE decodes through the BIG5 map; anything else is a raw byte
/// and the scan resynchronizes.
pub(crate) fn decode_big5_bytes(interp: &Interpreter, bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut rest = bytes;
    while let Some((&lead, tail)) = rest.split_first() {
        let (decoded, consumed) = match lead {
            0x00..=0x7F => (Some(char::from(lead)), 1),
            0xA1..=0xFE => match tail.first() {
                Some(&trail)
                    if (0x40..=0x7E).contains(&trail) || (0xA1..=0xFE).contains(&trail) =>
                {
                    (
                        decode_charset_code(
                            interp,
                            "big5",
                            u32::from(lead) << 8 | u32::from(trail),
                        )
                        .and_then(char::from_u32),
                        2,
                    )
                }
                _ => (None, 1),
            },
            _ => (None, 1),
        };
        match decoded {
            Some(ch) => {
                out.push(ch);
                rest = &rest[consumed..];
            }
            None => {
                out.push(raw_byte_regex_char(lead));
                rest = &rest[1..];
            }
        }
    }
    out
}

pub(crate) fn decode_raw_text_bytes(bytes: &[u8]) -> String {
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

pub(crate) fn decode_latin_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| char::from(*byte)).collect()
}

pub(crate) fn decode_utf8_bytes(bytes: &[u8]) -> String {
    let mut decoded = String::new();
    let mut remaining = bytes;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                decoded.push_str(valid);
                break;
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if valid_up_to > 0 {
                    decoded.push_str(
                        std::str::from_utf8(&remaining[..valid_up_to])
                            .expect("valid_up_to prefix is valid utf-8"),
                    );
                }
                let invalid_len = error.error_len().unwrap_or(1);
                for byte in &remaining[valid_up_to..valid_up_to + invalid_len] {
                    decoded.push(raw_byte_regex_char(*byte));
                }
                remaining = &remaining[valid_up_to + invalid_len..];
            }
        }
    }
    decoded
}

pub(crate) fn encode_text_bytes(
    interp: &Interpreter,
    text: &str,
    coding: &str,
    inhibit_eol_conversion: bool,
) -> Result<Vec<u8>, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_else(|| canonical.clone());
    let eol_type = (!inhibit_eol_conversion)
        .then(|| interp.coding_system_eol_type_value(&canonical))
        .flatten();
    let text = encode_text_with_eol(text, eol_type);
    if let Some(encoding) = legacy_single_byte_encoding(interp, &canonical) {
        return encode_legacy_single_byte_bytes(encoding, &text);
    }
    match kind.as_str() {
        "utf-8" | "prefer-utf-8" | "utf-8-auto" => {
            encode_utf8_bytes(&text, coding_system_requires_bom(interp, &canonical))
        }
        "utf-8-with-signature" => encode_utf8_bytes(&text, true),
        "utf-16" => {
            let (big_endian, with_bom, _) = coding_system_utf16_options(interp, &canonical, &kind);
            encode_utf16_bytes(&text, big_endian, with_bom)
        }
        "utf-16be" => encode_utf16_bytes(&text, true, false),
        "utf-16le" => encode_utf16_bytes(&text, false, false),
        "iso-latin-1" => encode_iso_latin_bytes(&text),
        "us-ascii" => encode_ascii_bytes(&text),
        "raw-text" | "no-conversion" => encode_raw_text_bytes(&text),
        "euc-jp" => encode_euc_jp_bytes(interp, &text),
        "sjis" => encode_sjis_bytes(interp, &text),
        "big5" => encode_big5_bytes(interp, &text),
        "charset" => encode_charset_coding_bytes(interp, &text, &canonical),
        _ => encode_raw_text_bytes(&text),
    }
}

/// Encode TEXT as UTF-16 (big- or little-endian), optionally prefixing a
/// byte-order mark.  Raw-byte marker chars encode as their byte value.
fn encode_utf16_bytes(text: &str, big_endian: bool, bom: bool) -> Result<Vec<u8>, LispError> {
    let mut out = Vec::new();
    if bom {
        // U+FEFF
        if big_endian {
            out.extend_from_slice(&[0xFE, 0xFF]);
        } else {
            out.extend_from_slice(&[0xFF, 0xFE]);
        }
    }
    for ch in text.chars() {
        let scalar = if let Some(byte) = raw_byte_from_regex_char(ch) {
            byte as u32
        } else {
            ch as u32
        };
        if scalar <= 0xFFFF {
            let unit = scalar as u16;
            if big_endian {
                out.extend_from_slice(&unit.to_be_bytes());
            } else {
                out.extend_from_slice(&unit.to_le_bytes());
            }
        } else {
            // Surrogate pair for astral codepoints.
            let v = scalar - 0x1_0000;
            let hi = 0xD800 + ((v >> 10) as u16);
            let lo = 0xDC00 + ((v & 0x3FF) as u16);
            for unit in [hi, lo] {
                if big_endian {
                    out.extend_from_slice(&unit.to_be_bytes());
                } else {
                    out.extend_from_slice(&unit.to_le_bytes());
                }
            }
        }
    }
    Ok(out)
}

fn push_invalid_utf16_unit(out: &mut String, unit: u16, big_endian: bool) {
    let bytes = if big_endian {
        unit.to_be_bytes()
    } else {
        unit.to_le_bytes()
    };
    for byte in bytes {
        out.push(if byte <= 0x7f {
            char::from(byte)
        } else {
            raw_byte_regex_char(byte)
        });
    }
}

fn decode_utf16_bytes(
    bytes: &[u8],
    mut big_endian: bool,
    with_bom: bool,
    detect_bom: bool,
) -> String {
    let mut offset = 0usize;
    if detect_bom {
        match bytes.get(..2) {
            Some([0xfe, 0xff]) => {
                big_endian = true;
                offset = 2;
            }
            Some([0xff, 0xfe]) => {
                big_endian = false;
                offset = 2;
            }
            _ => {}
        }
    } else if with_bom {
        let expected = if big_endian {
            [0xfe, 0xff]
        } else {
            [0xff, 0xfe]
        };
        if bytes.starts_with(&expected) {
            offset = 2;
        }
    }

    let mut out = String::new();
    let mut units = Vec::with_capacity((bytes.len().saturating_sub(offset)) / 2);
    let chunks = bytes[offset..].chunks_exact(2);
    let remainder = chunks.remainder();
    for chunk in chunks {
        units.push(if big_endian {
            u16::from_be_bytes([chunk[0], chunk[1]])
        } else {
            u16::from_le_bytes([chunk[0], chunk[1]])
        });
    }
    let mut index = 0usize;
    while index < units.len() {
        let unit = units[index];
        if (0xd800..=0xdbff).contains(&unit)
            && let Some(low) = units.get(index + 1).copied()
            && (0xdc00..=0xdfff).contains(&low)
        {
            let scalar =
                0x1_0000 + (((u32::from(unit) - 0xd800) << 10) | (u32::from(low) - 0xdc00));
            if let Some(character) = char::from_u32(scalar) {
                out.push(character);
            }
            index += 2;
            continue;
        }
        if (0xd800..=0xdfff).contains(&unit) {
            push_invalid_utf16_unit(&mut out, unit, big_endian);
        } else if let Some(character) = char::from_u32(u32::from(unit)) {
            out.push(character);
        }
        index += 1;
    }
    if let Some(byte) = remainder.first() {
        out.push(if *byte <= 0x7f {
            char::from(*byte)
        } else {
            raw_byte_regex_char(*byte)
        });
    }
    out
}

fn encode_string_text_for_coding(interp: &Interpreter, text: &str, coding: &str) -> String {
    let kind = interp
        .coding_system_kind_name(coding)
        .unwrap_or_else(|| coding.to_string());
    match kind.as_str() {
        "iso-latin-1" => text
            .chars()
            .map(|ch| {
                if raw_byte_from_regex_char(ch).is_some() || (ch as u32) <= 0xFF {
                    ch
                } else {
                    ' '
                }
            })
            .collect(),
        "euc-jp" => text
            .chars()
            .map(|ch| {
                let code = ch as u32;
                if raw_byte_from_regex_char(ch).is_some()
                    || code <= 0x7F
                    || [
                        "japanese-jisx0208",
                        "katakana-jisx0201",
                        "japanese-jisx0212",
                        "latin-jisx0201",
                    ]
                    .iter()
                    .any(|charset| encode_charset_char(interp, charset, code).is_some())
                {
                    ch
                } else {
                    // coding.c's iso-2022 encoder emits a space for an
                    // unencodable character (the oracle's answer for
                    // (encode-coding-string "\u{20AC}" 'euc-jp)).
                    ' '
                }
            })
            .collect(),
        "sjis" => text
            .chars()
            .map(|ch| {
                let code = ch as u32;
                if raw_byte_from_regex_char(ch).is_some()
                    || code <= 0x7F
                    || ["japanese-jisx0208", "katakana-jisx0201"]
                        .iter()
                        .any(|charset| encode_charset_char(interp, charset, code).is_some())
                {
                    ch
                } else {
                    ' '
                }
            })
            .collect(),
        "big5" => text
            .chars()
            .map(|ch| {
                let code = ch as u32;
                if raw_byte_from_regex_char(ch).is_some()
                    || code <= 0x7F
                    || encode_charset_char(interp, "big5", code).is_some()
                {
                    ch
                } else {
                    ' '
                }
            })
            .collect(),
        "charset" => {
            let charsets = coding_system_charset_names(interp, coding);
            let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
            text.chars()
                .map(|ch| {
                    let scalar = raw_byte_from_regex_char(ch)
                        .map(u32::from)
                        .unwrap_or(ch as u32);
                    if ascii_compatible && scalar <= 0x7f
                        || charsets
                            .iter()
                            .any(|charset| encode_charset_char(interp, charset, scalar).is_some())
                    {
                        ch
                    } else {
                        // GNU's charset coders use a space as their default
                        // replacement for an unrepresentable character.  The
                        // selected coding system remains authoritative; this
                        // is not an implicit fallback to UTF-8.
                        ' '
                    }
                })
                .collect()
        }
        "us-ascii" => text
            .chars()
            .map(|ch| {
                if raw_byte_from_regex_char(ch).is_some_and(|byte| byte <= 0x7F)
                    || (ch as u32) <= 0x7F
                {
                    ch
                } else {
                    '?'
                }
            })
            .collect(),
        _ => text.to_string(),
    }
}

pub(crate) fn decode_text_bytes(
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
    if let Some(encoding) = legacy_single_byte_encoding(interp, &canonical) {
        return Ok(decode_legacy_single_byte_bytes(encoding, bytes));
    }
    match kind.as_str() {
        "utf-8" | "prefer-utf-8" | "utf-8-auto" => {
            let bytes = if coding_system_consumes_bom(interp, &canonical) {
                strip_utf8_bom(bytes).1
            } else {
                bytes
            };
            Ok(decode_utf8_bytes(bytes))
        }
        "utf-8-with-signature" => Ok(decode_utf8_bytes(strip_utf8_bom(bytes).1)),
        "utf-16" => {
            let (big_endian, with_bom, detect_bom) =
                coding_system_utf16_options(interp, &canonical, &kind);
            Ok(decode_utf16_bytes(bytes, big_endian, with_bom, detect_bom))
        }
        "utf-16be" => Ok(decode_utf16_bytes(bytes, true, false, false)),
        "utf-16le" => Ok(decode_utf16_bytes(bytes, false, false, false)),
        "iso-latin-1" => Ok(decode_latin_bytes(bytes)),
        "us-ascii" => Ok(bytes.iter().map(|byte| char::from(*byte)).collect()),
        "raw-text" | "no-conversion" => Ok(decode_raw_text_bytes(bytes)),
        "euc-jp" => Ok(decode_euc_jp_bytes(interp, bytes)),
        "sjis" => Ok(decode_sjis_bytes(interp, bytes)),
        "big5" => Ok(decode_big5_bytes(interp, bytes)),
        "charset" => Ok(decode_charset_coding_bytes(interp, bytes, &canonical)),
        _ => Ok(decode_raw_text_bytes(bytes)),
    }
}

pub(crate) fn string_unencodable_positions(
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
    let legacy_single_byte = legacy_single_byte_encoding(interp, &canonical);
    let mut failures = Vec::new();
    for (index, ch) in text.chars().enumerate() {
        let raw_byte = raw_byte_from_regex_char(ch);
        let code = ch as u32;
        let representable = match kind.as_str() {
            "utf-8" | "utf-8-with-signature" | "utf-8-auto" | "prefer-utf-8" | "undecided" => {
                ch != json::INVALID_UNICODE_SENTINEL
            }
            _ if let Some(encoding) = legacy_single_byte => {
                raw_byte.is_some() || !encoding.encode(&ch.to_string()).2
            }
            "iso-latin-1" | "raw-text" | "no-conversion" => raw_byte.is_some() || code <= 0xFF,
            "us-ascii" => raw_byte.is_some_and(|byte| byte <= 0x7F) || code <= 0x7F,
            "sjis" => {
                raw_byte.is_some()
                    || code <= 0x7F
                    || ["japanese-jisx0208", "katakana-jisx0201"]
                        .iter()
                        .any(|charset| encode_charset_char(interp, charset, code).is_some())
            }
            "iso-2022-7bit" => raw_byte.is_some_and(|byte| byte <= 0x7F) || code <= 0x7F,
            "big5" => {
                raw_byte.is_some()
                    || code <= 0x7F
                    || encode_charset_char(interp, "big5", code).is_some()
            }
            "euc-jp" => {
                raw_byte.is_some()
                    || code <= 0x7F
                    || [
                        "japanese-jisx0208",
                        "katakana-jisx0201",
                        "japanese-jisx0212",
                        "latin-jisx0201",
                    ]
                    .iter()
                    .any(|charset| encode_charset_char(interp, charset, code).is_some())
            }
            "charset" => {
                (coding_system_is_ascii_compatible(interp, &canonical)
                    && (raw_byte.is_some_and(|byte| byte <= 0x7f) || code <= 0x7f))
                    || raw_byte.is_some()
                    || coding_system_charset_names(interp, &canonical)
                        .iter()
                        .any(|charset| encode_charset_char(interp, charset, code).is_some())
            }
            _ => true,
        };
        if !representable {
            failures.push(index as i64);
        }
    }
    Ok(failures)
}

pub(crate) fn string_identity_for_coding(
    text: &str,
    coding: &str,
    interp: &Interpreter,
    encode: bool,
    inhibit_eol_conversion: bool,
) -> bool {
    let eol_type = if inhibit_eol_conversion {
        None
    } else {
        interp.coding_system_eol_type_value(coding)
    };
    let kind = interp
        .coding_system_kind_name(coding)
        .unwrap_or_else(|| coding.to_string());
    let single_byte_translation = legacy_single_byte_encoding(interp, coding).is_some()
        || matches!(kind.as_str(), "iso-latin-1" | "charset");
    if encode {
        if matches!(eol_type, Some(1) | Some(2)) && text.contains('\n') {
            return false;
        }
        if kind == "utf-8-with-signature"
            || (kind == "utf-8" && coding_system_requires_bom(interp, coding))
            || matches!(kind.as_str(), "utf-16" | "utf-16be" | "utf-16le")
        {
            return false;
        }
        if single_byte_translation
            && text
                .chars()
                .any(|ch| raw_byte_from_regex_char(ch).is_some() || !ch.is_ascii())
        {
            return false;
        }
    } else if (matches!(eol_type, Some(1) | Some(2)) && text.contains('\r'))
        || (single_byte_translation && text.chars().any(is_raw_byte_regex_char))
        || matches!(kind.as_str(), "utf-16" | "utf-16be" | "utf-16le")
        || ((kind == "utf-8" || kind == "utf-8-with-signature")
            && coding_system_consumes_bom(interp, coding))
    {
        return false;
    }
    true
}

pub(crate) fn auto_detect_coding(interp: &Interpreter, bytes: &[u8]) -> (String, Vec<u8>) {
    // None when no eol byte exists: the name then stays the bare base,
    // which is what GNU records in last-coding-system-used and what lets
    // the file reader know that nothing was decided.
    let actual_eol = detect_eol_type_opt(bytes);
    let normalized = decode_bytes_with_explicit_eol(bytes, actual_eol.unwrap_or(0));
    let (has_bom, bomless) = strip_utf8_bom(&normalized);
    if has_bom {
        return (
            coding_variant_name(interp, "utf-8-with-signature", actual_eol),
            bomless.to_vec(),
        );
    }
    if normalized.contains(&0) {
        return (
            coding_variant_name(interp, "no-conversion", actual_eol),
            normalized,
        );
    }
    if let Some(tag) = coding_tag_from_bytes(&normalized)
        && let Some(canonical) = interp.coding_system_canonical_name(&tag)
    {
        let base = interp
            .coding_system_base_name(&canonical)
            .unwrap_or(canonical);
        return (coding_variant_name(interp, &base, actual_eol), normalized);
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
            // Pure-ASCII text decides nothing: GNU records `undecided'
            // with the detected eol, whatever the coding priorities say
            // (prefer-utf-8 cannot even be preferred, and a preferred
            // utf-8-auto still answers undecided-unix).
            return (
                coding_variant_name(interp, "undecided", actual_eol),
                normalized,
            );
        }
        return (coding_variant_name(interp, "utf-8", actual_eol), normalized);
    }
    // Non-UTF-8 8-bit data without a null byte: GNU's detector falls to
    // the highest-priority charset coding, iso-latin-1 under the harness's
    // LANG=C environment -- every byte decodes (mojibake, not raw bytes).
    // A byte in 0x80..=0x9F is a C1 control, which no ISO 8859 text uses:
    // its presence rejects the latin-1 category and the read stays
    // raw-text (the oracle: (97 255) is latin-1, (97 129) is raw-text,
    // and a stray valid UTF-8 sequence like C3 80 forces raw-text through
    // its 0x80 continuation byte).
    if bomless.iter().any(|byte| (0x80..=0x9F).contains(byte)) {
        return (
            coding_variant_name(interp, "raw-text", actual_eol),
            normalized,
        );
    }
    (
        coding_variant_name(interp, "iso-latin-1", actual_eol),
        normalized,
    )
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
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))
}

pub(crate) fn detect_coding_names_for_text(
    interp: &Interpreter,
    text: &str,
    env: &Env,
) -> Vec<String> {
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

pub(crate) fn detect_coding_string_value(
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
            .map(|value| Value::Symbol(value.into()))
            .unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            codings
                .into_iter()
                .map(|value| Value::Symbol(value.into()))
                .collect::<Vec<_>>(),
        ))
    }
}

pub(crate) fn detect_coding_region_value(
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
            .map(|value| Value::Symbol(value.into()))
            .unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            codings
                .into_iter()
                .map(|value| Value::Symbol(value.into()))
                .collect::<Vec<_>>(),
        ))
    }
}

pub(crate) fn find_coding_systems_region_internal_value(
    interp: &Interpreter,
    start: &Value,
    end: &Value,
    exclude: Option<&Value>,
) -> Result<Value, LispError> {
    let (text, multibyte) = if let Some(string) = string_like(start) {
        (string.text, string.multibyte)
    } else {
        (
            text_from_region_or_string(interp, start, Some(end))?,
            interp.buffer.is_multibyte(),
        )
    };
    // GNU returns t for an ASCII-only or unibyte source: every coding
    // system can represent it, so the Lisp wrapper yields `(undecided)'.
    if !multibyte || ascii_only_text(&text) {
        return Ok(Value::T);
    }
    let excluded = exclude
        .filter(|value| !value.is_nil())
        .map(Value::to_vec)
        .transpose()?
        .unwrap_or_default();
    let mut codings = Vec::new();
    for coding in interp.coding_system_priority_list() {
        if excluded
            .iter()
            .any(|candidate| candidate.as_symbol().ok() == Some(coding.as_str()))
        {
            continue;
        }
        let Some(base) = interp.coding_system_base_name(&coding) else {
            continue;
        };
        if !interp.has_coding_system(&base) {
            continue;
        }
        if excluded
            .iter()
            .any(|candidate| candidate.as_symbol().ok() == Some(base.as_str()))
        {
            continue;
        }
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
        codings
            .into_iter()
            .map(|value| Value::Symbol(value.into()))
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn check_coding_systems_region_value(
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
            let mut items = vec![Value::Symbol(canonical.into())];
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
    let operation_target = match target {
        Value::Cons(cell) if operation == "insert-file-contents" => cell.car.borrow().clone(),
        other => other.clone(),
    };
    let Some(alist) = interp.lookup_var(alist_name, env) else {
        return Ok(Value::Nil);
    };
    for entry in alist.to_vec()? {
        let Some((pattern, target)) = entry.cons_values() else {
            continue;
        };
        let matches = match (&pattern, &operation_target) {
            (Value::Integer(pattern), Value::Integer(target)) => pattern == target,
            (pattern, target) => {
                let (Ok(pattern), Ok(target)) = (string_text(pattern), string_text(target)) else {
                    continue;
                };
                Regex::new(&regexp::translate_elisp_regex(&pattern))
                    .is_ok_and(|regex| regex.is_match(&target))
            }
        };
        if !matches {
            continue;
        }
        // A cons is already the requested (DECODING . ENCODING) pair.
        if matches!(target, Value::Cons(_)) {
            return Ok(target);
        }
        let coding = match target {
            Value::Symbol(symbol) if interp.has_coding_system(&symbol) => interp
                .coding_system_canonical_name(&symbol)
                .unwrap_or_else(|| symbol.to_string()),
            Value::Symbol(symbol) => {
                let result =
                    call_named_function(interp, &symbol, &[Value::list(args.to_vec())], env)?;
                if let Some((decode, encode)) = result.cons_values() {
                    return Ok(Value::cons(decode, encode));
                }
                checked_coding_symbol(interp, &result)?
            }
            other => {
                let result =
                    call_function_value(interp, &other, &[Value::list(args.to_vec())], env)?;
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

pub(crate) fn encode_coding_value(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value.clone()))?;
    let Some(coding) = coding else {
        set_last_coding_system_used(interp, "no-conversion", env);
        return if nocopy {
            Ok(value.clone())
        } else {
            shared_string_copy(value)
        };
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    // The requested spelling, not the canonical name (the oracle answers
    // `euc-jp' for (encode-coding-string "a" 'euc-jp)).
    set_last_coding_system_used(interp, coding, env);
    let pre_write =
        coding_system_property(interp, &canonical, ":pre-write-conversion").unwrap_or(Value::Nil);
    let converted_text = run_coding_conversion(interp, &string.text, &pre_write, true, env)?;
    let conversion_ran = !pre_write.is_nil();
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    if interp
        .coding_system(&canonical)
        .is_some_and(|coding| coding.kind == "raw-text")
    {
        let text = decode_raw_text_bytes(&encode_text_bytes(
            interp,
            &converted_text,
            &canonical,
            inhibit_eol_conversion,
        )?);
        return Ok(make_shared_string_value_with_multibyte(
            text,
            string.props,
            false,
        ));
    }
    let failures = string_unencodable_positions(&converted_text, &canonical, interp)?;
    if !failures.is_empty() {
        let substituted = encode_string_text_for_coding(interp, &converted_text, &canonical);
        if substituted == converted_text {
            return Err(LispError::Signal("Character cannot be encoded".into()));
        }
        return Ok(bytes_to_shared_unibyte_value(&encode_text_bytes(
            interp,
            &substituted,
            &canonical,
            inhibit_eol_conversion,
        )?));
    }
    if nocopy
        && !conversion_ran
        && string_identity_for_coding(
            &string.text,
            &canonical,
            interp,
            true,
            inhibit_eol_conversion,
        )
    {
        Ok(value.clone())
    } else {
        Ok(bytes_to_shared_unibyte_value(&encode_text_bytes(
            interp,
            &converted_text,
            &canonical,
            inhibit_eol_conversion,
        )?))
    }
}

pub(crate) fn decode_coding_text(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value.clone()))?;
    let Some(coding) = coding else {
        set_last_coding_system_used(interp, "no-conversion", env);
        return if nocopy {
            Ok(value.clone())
        } else {
            shared_string_copy(value)
        };
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(coding))?;
    let undecided_bytes =
        if interp.coding_system_kind_name(&canonical).as_deref() == Some("undecided") {
            let bytes = encode_raw_text_bytes(&string.text)?;
            let (detected, normalized) = auto_detect_coding(interp, &bytes);
            Some((detected, normalized))
        } else {
            None
        };
    let detected_undecided = undecided_bytes.is_some();
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // String decoding names `last-coding-system-used' differently from a
    // file read (the oracle's contract): pure-ASCII input without a CR
    // never re-resolves the name -- the requested spelling survives, alias
    // and all (euc-jp stays `euc-jp', LF included) -- and a converted text
    // gains the canonical eol subsidiary only when an eol byte was seen.
    let actual_coding = if let Some((detected, _)) = &undecided_bytes {
        let bytes = encode_raw_text_bytes(&string.text)?;
        if bytes.iter().all(u8::is_ascii) && !bytes.contains(&b'\r') {
            interp
                .coding_system_base_name(detected)
                .unwrap_or_else(|| detected.clone())
        } else {
            detected.clone()
        }
    } else if interp.coding_system_eol_type_value(&canonical).is_none()
        && !matches!(canonical.as_str(), "no-conversion" | "binary")
    {
        let bytes = encode_raw_text_bytes(&string.text)?;
        // The pure-ASCII shortcut is really "the decoder never ran": it
        // needs the coding to be ascii-compatible.  iso-2022-7bit is not
        // (its ESC sequences convert), so even an all-ASCII decode with a
        // LF re-resolves to iso-2022-7bit-unix, per the oracle.
        let untouched =
            bytes.iter().all(u8::is_ascii) && coding_system_is_ascii_compatible(interp, &canonical);
        match detect_eol_type_opt(&bytes) {
            Some(eol) if bytes.contains(&b'\r') || !untouched => {
                let base = interp
                    .coding_system_base_name(&canonical)
                    .unwrap_or_else(|| canonical.clone());
                coding_variant_name(interp, &base, Some(eol))
            }
            _ => coding.to_string(),
        }
    } else {
        // Explicit eol (or binary): the requested spelling survives.
        coding.to_string()
    };
    set_last_coding_system_used(interp, &actual_coding, env);
    let text = if let Some((_, normalized)) = undecided_bytes {
        decode_text_bytes(interp, &normalized, &actual_coding)?
    } else if inhibit_eol_conversion {
        string.text.clone()
    } else {
        match interp.coding_system_eol_type_value(&canonical) {
            Some(1) if string.text.contains('\r') => string.text.replace("\r\n", "\n"),
            Some(2) if string.text.contains('\r') => string.text.replace('\r', "\n"),
            // GNU detects the EOL convention for codings with an unspecified
            // eol type; only no-conversion/binary keep raw CR bytes.
            None if string.text.contains('\r')
                && !matches!(canonical.as_str(), "no-conversion" | "binary") =>
            {
                if string.text.contains("\r\n") {
                    string.text.replace("\r\n", "\n")
                } else {
                    string.text.replace('\r', "\n")
                }
            }
            _ => string.text.clone(),
        }
    };
    // A unibyte Lisp string is a byte stream even when its internal scalar
    // happens to be in U+0080..U+00FF.  Literal file insertion and generated
    // unibyte strings must therefore share the same decoder boundary.
    // UTF-16 is also necessarily byte-oriented even when all input octets
    // happen to be ASCII (e.g. 30 42 encodes U+3042 in UTF-16BE).  Buffer
    // regions do not retain the unibyte provenance of those ASCII octets.
    let byte_oriented_multibyte = interp
        .coding_system_kind_name(&canonical)
        .is_some_and(|kind| matches!(kind.as_str(), "utf-16" | "utf-16be" | "utf-16le"));
    let text = if !detected_undecided
        && (!string.multibyte
            || byte_oriented_multibyte
            || text.chars().any(is_raw_byte_regex_char))
    {
        decode_text_bytes(interp, &encode_raw_text_bytes(&text)?, &canonical)?
    } else {
        text
    };
    let post_read = coding_system_property(interp, &actual_coding, ":post-read-conversion")
        .unwrap_or(Value::Nil);
    let conversion_ran = !post_read.is_nil();
    let text = run_coding_conversion(interp, &text, &post_read, false, env)?;
    if nocopy
        && !conversion_ran
        && text == string.text
        && string_identity_for_coding(
            &string.text,
            &canonical,
            interp,
            false,
            inhibit_eol_conversion,
        )
    {
        Ok(value.clone())
    } else {
        Ok(make_shared_string_value_with_multibyte(
            text,
            string.props,
            true,
        ))
    }
}
