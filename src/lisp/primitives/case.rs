use super::*;

pub(crate) const RAW_BYTE8_BASE: u32 = 0x3FFF00;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CaseAction {
    Up,
    Down,
    Capitalize,
    UpcaseInitials,
}

pub(crate) fn is_raw_like_byte_char(code: u32) -> bool {
    matches!(code, 0x00CF | 0x00EF | 0x00FF)
}

pub(crate) fn raw_case_byte(key: u32) -> Option<u32> {
    if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xFF).contains(&key) {
        Some(key - RAW_BYTE8_BASE)
    } else if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&key) {
        Some(key - RAW_BYTE_REGEX_BASE)
    } else {
        None
    }
}

pub(crate) fn normalize_case_key(key: u32) -> u32 {
    raw_case_byte(key).unwrap_or(key)
}

pub(crate) fn denormalize_case_key(template: u32, mapped: u32) -> u32 {
    if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xFF).contains(&template) && mapped <= 0xFF {
        RAW_BYTE8_BASE + mapped
    } else if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&template)
        && mapped <= 0xFF
    {
        RAW_BYTE_REGEX_BASE + mapped
    } else {
        mapped
    }
}

pub(crate) fn alternate_case_key(key: u32) -> Option<u32> {
    if key <= 0xFF {
        Some(RAW_BYTE8_BASE + key)
    } else if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xFF).contains(&key) {
        Some(key - RAW_BYTE8_BASE)
    } else if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&key) {
        Some(RAW_BYTE8_BASE + (key - RAW_BYTE_REGEX_BASE))
    } else {
        None
    }
}

pub(crate) fn raw_byte_regex_char(byte: u8) -> char {
    char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
        .expect("raw byte regex marker is a valid private-use character")
}

pub(crate) fn raw_byte_from_regex_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&code) {
        Some((code - RAW_BYTE_REGEX_BASE) as u8)
    } else {
        None
    }
}

pub(crate) fn is_raw_byte_regex_char(ch: char) -> bool {
    raw_byte_from_regex_char(ch).is_some()
}

pub(crate) fn public_buffer_char_code(ch: char, multibyte: bool) -> i64 {
    match raw_byte_from_regex_char(ch) {
        Some(byte) if multibyte => i64::from(RAW_BYTE8_BASE + u32::from(byte)),
        Some(byte) => i64::from(byte),
        None => ch as i64,
    }
}

/// Return the Lisp-visible character at a buffer position.
///
/// Most characters live directly in the rope.  Emacs characters outside
/// Unicode's scalar range use a placeholder plus typed buffer metadata, so
/// every public character accessor must consult that sidecar through this
/// one boundary rather than leaking the placeholder value.
pub(crate) fn public_buffer_char_code_at(interp: &Interpreter, position: usize) -> Option<i64> {
    interp
        .buffer
        .extended_char_at(position)
        .map(i64::from)
        .or_else(|| {
            interp
                .buffer
                .char_at(position)
                .map(|ch| public_buffer_char_code(ch, interp.buffer.is_multibyte()))
        })
}

pub(crate) fn single_char_case_mapping(iter: impl Iterator<Item = char>, fallback: u32) -> u32 {
    let mut iter = iter;
    match (iter.next(), iter.next()) {
        (Some(mapped), None) => mapped as u32,
        _ => fallback,
    }
}

pub(crate) fn simple_upcase_char(code: u32) -> u32 {
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

pub(crate) fn simple_downcase_char(code: u32, final_sigma: bool) -> u32 {
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

pub(crate) fn simple_titlecase_char(code: u32) -> u32 {
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

pub(crate) fn current_case_table_ids(interp: &mut Interpreter) -> Result<(u64, u64), LispError> {
    let down = interp.current_case_table_id();
    let up = match interp.char_table_extra_slot(down, 0) {
        Some(Value::CharTable(id)) => id,
        _ => down,
    };
    Ok((down, up))
}

pub(crate) fn explicit_case_table_mapping(
    interp: &Interpreter,
    table_id: u64,
    code: u32,
) -> Option<u32> {
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

pub(crate) fn case_symbols_as_words_enabled(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("case-symbols-as-words", env)
        .is_some_and(|value| value.is_truthy())
}

pub(crate) fn case_word_char(interp: &Interpreter, ch: char, case_symbols_as_words: bool) -> bool {
    syntax::current_syntax_word_char(interp, normalize_case_key(ch as u32), case_symbols_as_words)
}

/// GNU's unconditional Unicode special-casing tables.  Keep the small native
/// bootstrap fallback in one place so `get-char-code-property' and the
/// string/region casers cannot disagree about one-to-many mappings.
pub(crate) fn unicode_special_case_mapping(code: u32, property: &str) -> Option<&'static str> {
    match (normalize_case_key(code), property) {
        (0x00DF, "special-uppercase") => Some("SS"),
        (0x00DF, "special-titlecase") => Some("Ss"),
        (0x0130, "special-lowercase") => Some("i\u{307}"),
        (0xFB01, "special-uppercase") => Some("FI"),
        (0xFB01, "special-titlecase") => Some("Fi"),
        _ => None,
    }
}

pub(crate) fn full_upcase_string(interp: &Interpreter, up_table: u64, ch: char) -> String {
    let code = ch as u32;
    if let Some(mapped) = unicode_special_case_mapping(code, "special-uppercase") {
        return mapped.to_string();
    }
    if let Some(mapped) = explicit_case_table_mapping(interp, up_table, code) {
        return char::from_u32(denormalize_case_key(code, mapped))
            .unwrap_or(ch)
            .to_string();
    }
    match code {
        _ if is_raw_like_byte_char(code) => ch.to_string(),
        _ => ch.to_uppercase().collect(),
    }
}

pub(crate) fn full_downcase_string(
    interp: &Interpreter,
    down_table: u64,
    ch: char,
    final_sigma: bool,
) -> String {
    let code = ch as u32;
    if let Some(mapped) = unicode_special_case_mapping(code, "special-lowercase") {
        return mapped.to_string();
    }
    if let Some(mapped) = explicit_case_table_mapping(interp, down_table, code) {
        return char::from_u32(denormalize_case_key(code, mapped))
            .unwrap_or(ch)
            .to_string();
    }
    match code {
        0x03A3 => char::from_u32(simple_downcase_char(code, final_sigma))
            .unwrap_or(ch)
            .to_string(),
        _ if is_raw_like_byte_char(code) => ch.to_string(),
        _ => ch.to_lowercase().collect(),
    }
}

pub(crate) fn full_titlecase_string(interp: &Interpreter, up_table: u64, ch: char) -> String {
    let code = ch as u32;
    if let Some(mapped) = unicode_special_case_mapping(code, "special-titlecase") {
        return mapped.to_string();
    }
    if let Some(mapped) = explicit_case_table_mapping(interp, up_table, code) {
        return char::from_u32(denormalize_case_key(code, mapped))
            .unwrap_or(ch)
            .to_string();
    }
    match code {
        0x01C4..=0x01C6 => '\u{01C5}'.to_string(),
        _ if is_raw_like_byte_char(code) => ch.to_string(),
        _ => char::from_u32(denormalize_case_key(code, simple_titlecase_char(code)))
            .unwrap_or(ch)
            .to_string(),
    }
}

pub(crate) fn simple_case_char_for_action(
    interp: &Interpreter,
    down_table: u64,
    up_table: u64,
    code: u32,
    action: CaseAction,
) -> u32 {
    let mapped = match action {
        CaseAction::Up => explicit_case_table_mapping(interp, up_table, code)
            .unwrap_or_else(|| simple_upcase_char(code)),
        CaseAction::Down => explicit_case_table_mapping(interp, down_table, code)
            .unwrap_or_else(|| simple_downcase_char(code, false)),
        CaseAction::Capitalize | CaseAction::UpcaseInitials => {
            explicit_case_table_mapping(interp, up_table, code)
                .unwrap_or_else(|| simple_titlecase_char(code))
        }
    };
    denormalize_case_key(code, mapped)
}

pub(crate) fn casify_string(
    interp: &mut Interpreter,
    input: &str,
    action: CaseAction,
    env: &Env,
) -> Result<String, LispError> {
    let case_symbols_as_words = case_symbols_as_words_enabled(interp, env);
    let (down_table, up_table) = current_case_table_ids(interp)?;
    let ascii_only = interp.is_ascii_case_table(down_table);
    let chars: Vec<char> = input.chars().collect();
    let mut output = String::new();
    let mut in_word = false;
    for (idx, ch) in chars.iter().copied().enumerate() {
        let is_word = case_word_char(interp, ch, case_symbols_as_words);
        let next_is_word = chars
            .get(idx + 1)
            .copied()
            .is_some_and(|next| case_word_char(interp, next, case_symbols_as_words));
        let explicit_non_ascii_mapping = match action {
            CaseAction::Down => explicit_case_table_mapping(interp, down_table, ch as u32),
            CaseAction::Up | CaseAction::Capitalize | CaseAction::UpcaseInitials => {
                explicit_case_table_mapping(interp, up_table, ch as u32)
            }
        };
        let piece = if ascii_only && !ch.is_ascii() && explicit_non_ascii_mapping.is_none() {
            ch.to_string()
        } else {
            match action {
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
            }
        };
        output.push_str(&piece);
        in_word = is_word;
    }
    Ok(output)
}

pub(crate) fn casify_value(
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
    Ok(Value::String(
        casify_string(interp, &input, action, env)?.into(),
    ))
}

pub(crate) fn replace_buffer_region_with_text(
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

pub(crate) fn casify_buffer_region(
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

pub(crate) fn parse_region_bound(value: &Value) -> Result<(usize, usize), LispError> {
    let Some((start, end)) = value.cons_values() else {
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

pub(crate) fn parse_region_bounds(value: &Value) -> Result<Vec<(usize, usize)>, LispError> {
    let mut cursor = value.clone();
    let mut bounds = Vec::new();
    for _ in 0..1024 {
        match cursor {
            Value::Nil => return Ok(bounds),
            Value::Cons(cons_cell) => {
                let car = &cons_cell.car;
                let cdr = &cons_cell.cdr;
                bounds.push(parse_region_bound(&car.borrow())?);
                cursor = cdr.borrow().clone();
            }
            _ => return Err(LispError::Signal("Invalid region bounds".into())),
        }
    }
    Err(LispError::Signal("Invalid region bounds".into()))
}

pub(crate) fn case_word_region(
    interp: &Interpreter,
    point: usize,
    count: i64,
    env: &Env,
) -> (usize, usize) {
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
