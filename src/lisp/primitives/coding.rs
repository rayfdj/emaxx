use super::*;

pub(crate) fn coding_system_error(name: impl Into<String>) -> LispError {
    let name = name.into();
    LispError::SignalValue(Value::list([
        Value::Symbol("coding-system-error".into()),
        Value::String(format!("Invalid coding system: {name}")),
    ]))
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

pub(crate) fn first_valid_coding_candidate(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<String>, LispError> {
    match value {
        Value::Cons(_, _) => {
            for candidate in value.to_vec()? {
                if candidate == Value::T || candidate.is_nil() {
                    continue;
                }
                if checked_coding_name(interp, &candidate)?.is_some() {
                    return Ok(Some(candidate.as_symbol()?.to_string()));
                }
            }
            Ok(None)
        }
        Value::Nil | Value::T => Ok(None),
        _ => Ok(checked_coding_name(interp, value)?.map(|_| {
            value
                .as_symbol()
                .map(str::to_string)
                .unwrap_or_else(|_| "utf-8".into())
        })),
    }
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
        Value::Symbol(coding.to_string()),
        env,
    );
}

pub(crate) fn shared_string_copy(value: &Value) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
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
        Value::String(text)
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

pub(crate) fn make_temp_name(prefix: &str) -> String {
    let counter = TEMP_NAME_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let random = rand_simple().unsigned_abs();
    format!("{prefix}{nanos:x}{counter:x}{random:x}")
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
            Value::Cons(car, cdr) => {
                if offset == index {
                    *car.borrow_mut() = new_value;
                    return Ok(());
                }
                offset += 1;
                current = cdr.borrow().clone();
            }
            Value::Nil => return Err(LispError::Signal("Args out of range".into())),
            _ => return Err(LispError::TypeError("array".into(), target.type_name())),
        }
    }
}

pub(crate) fn base64_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C)
}

pub(crate) fn url_encode_url(input: &str) -> String {
    let mut output = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || "-._~:/?#[]@!$&'()*+,;=%".contains(ch) {
            output.push(ch);
        } else {
            for byte in ch.to_string().bytes() {
                output.push('%');
                output.push_str(&format!("{byte:02X}"));
            }
        }
    }
    output
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
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    let bytes = encode_base64_source_bytes(&string.text, string.multibyte)?;
    Ok(Value::String(encode_base64_bytes(
        &bytes, line_break, pad, base64url,
    )))
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
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
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
    let decoded = decode_base64_string_value(&Value::String(text), base64url, ignore_invalid)?;
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

pub(crate) fn encode_iso_latin_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    encode_raw_text_bytes(text)
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

pub(crate) fn encode_euc_jp_bytes(text: &str) -> Result<Vec<u8>, LispError> {
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
) -> Result<Vec<u8>, LispError> {
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

pub(crate) fn string_identity_for_coding(
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

pub(crate) fn preferred_ascii_detection_base(interp: &Interpreter) -> String {
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

pub(crate) fn auto_detect_coding(interp: &Interpreter, bytes: &[u8]) -> (String, Vec<u8>) {
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
    if let Some(tag) = coding_tag_from_bytes(&normalized)
        && let Some(canonical) = interp.coding_system_canonical_name(&tag)
    {
        let base = interp
            .coding_system_base_name(&canonical)
            .unwrap_or(canonical);
        return (
            coding_variant_name(interp, &base, Some(actual_eol)),
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
            .map(Value::Symbol)
            .unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            codings.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
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
            .map(Value::Symbol)
            .unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            codings.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
        ))
    }
}

pub(crate) fn find_coding_systems_region_internal_value(
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
    let _operation = args[0].as_symbol()?;
    let Some(file) = args.get(1) else {
        return Ok(Value::Nil);
    };
    let file = match file {
        Value::Cons(car, _) => string_text(&car.borrow())?,
        _ => string_text(file)?,
    };
    let Some(alist) = interp.lookup_var("file-coding-system-alist", env) else {
        return Ok(Value::Nil);
    };
    for entry in alist.to_vec()? {
        let Some((pattern, target)) = entry.cons_values() else {
            continue;
        };
        let pattern = string_text(&pattern)?;
        let Ok(regex) = Regex::new(&regexp::translate_elisp_regex(&pattern)) else {
            continue;
        };
        if !regex.is_match(&file) {
            continue;
        }
        let target = match target {
            Value::Cons(value, tail) if matches!(*tail.borrow(), Value::Nil) => {
                value.borrow().clone()
            }
            // (REGEXP DECODING . ENCODING): return the pair verbatim.
            Value::Cons(decode, encode) => {
                return Ok(Value::cons(
                    decode.borrow().clone(),
                    encode.borrow().clone(),
                ));
            }
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

pub(crate) fn encode_coding_value(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
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
    set_last_coding_system_used(interp, &canonical, env);
    if interp
        .coding_system(&canonical)
        .is_some_and(|coding| coding.kind == "raw-text")
    {
        let text = decode_raw_text_bytes(&encode_text_bytes(interp, &string.text, &canonical)?);
        return Ok(make_shared_string_value_with_multibyte(
            text,
            string.props,
            false,
        ));
    }
    let failures = string_unencodable_positions(&string.text, &canonical, interp)?;
    if !failures.is_empty() {
        let substituted = encode_string_text_for_coding(interp, &string.text, &canonical);
        if substituted == string.text {
            return Err(LispError::Signal("Character cannot be encoded".into()));
        }
        return Ok(bytes_to_shared_unibyte_value(&encode_text_bytes(
            interp,
            &substituted,
            &canonical,
        )?));
    }
    if nocopy && string_identity_for_coding(&string.text, &canonical, interp, true) {
        Ok(value.clone())
    } else {
        Ok(bytes_to_shared_unibyte_value(&encode_text_bytes(
            interp,
            &string.text,
            &canonical,
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
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
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
    set_last_coding_system_used(interp, &canonical, env);
    let text = match interp.coding_system_eol_type_value(&canonical) {
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
    };
    if nocopy
        && text == string.text
        && string_identity_for_coding(&string.text, &canonical, interp, false)
    {
        Ok(value.clone())
    } else if text == string.text {
        shared_string_copy(value)
    } else {
        Ok(string_like_value(text, string.props))
    }
}
