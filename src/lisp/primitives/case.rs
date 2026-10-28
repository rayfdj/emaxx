use super::*;
use crate::lisp::types::CharTableRef;
use crate::lisp::types::Kind;

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

/// The eight-bit characters, #x3FFF80 to #x3FFFFF (CHAR_BYTE8_P); #x3FFF00
/// to #x3FFF7F are ordinary characters.
pub(crate) const RAW_BYTE8_CHARS: std::ops::RangeInclusive<u32> =
    RAW_BYTE8_BASE + 0x80..=RAW_BYTE8_BASE + 0xFF;

/// The stored markers of the eight-bit characters, U+E080 to U+E0FF; a
/// U+E000 to U+E07F is the private-use character itself.
pub(crate) const RAW_BYTE_MARKERS: std::ops::RangeInclusive<u32> =
    RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xFF;

pub(crate) fn raw_case_byte(key: u32) -> Option<u32> {
    if RAW_BYTE8_CHARS.contains(&key) {
        Some(key - RAW_BYTE8_BASE)
    } else if RAW_BYTE_MARKERS.contains(&key) {
        Some(key - RAW_BYTE_REGEX_BASE)
    } else {
        None
    }
}

pub(crate) fn normalize_case_key(key: u32) -> u32 {
    raw_case_byte(key).unwrap_or(key)
}

pub(crate) fn denormalize_case_key(template: u32, mapped: u32) -> u32 {
    if RAW_BYTE8_CHARS.contains(&template) && (0x80..=0xFF).contains(&mapped) {
        RAW_BYTE8_BASE + mapped
    } else if RAW_BYTE_MARKERS.contains(&template) && (0x80..=0xFF).contains(&mapped) {
        RAW_BYTE_REGEX_BASE + mapped
    } else {
        mapped
    }
}

pub(crate) fn alternate_case_key(key: u32) -> Option<u32> {
    if (0x80..=0xFF).contains(&key) {
        Some(RAW_BYTE8_BASE + key)
    } else if RAW_BYTE8_CHARS.contains(&key) {
        Some(key - RAW_BYTE8_BASE)
    } else if RAW_BYTE_MARKERS.contains(&key) {
        Some(RAW_BYTE8_BASE + (key - RAW_BYTE_REGEX_BASE))
    } else {
        None
    }
}

/// The stored form of BYTE: its marker above 0x7F, an ASCII byte itself.
pub(crate) fn raw_byte_regex_char(byte: u8) -> char {
    if byte < 0x80 {
        return byte as char;
    }
    char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
        .expect("raw byte regex marker is a valid private-use character")
}

pub(crate) fn raw_byte_from_regex_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if RAW_BYTE_MARKERS.contains(&code) {
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
        .borrow()
        .extended_char_at(position)
        .map(i64::from)
        .or_else(|| {
            interp
                .buffer
                .borrow()
                .char_at(position)
                .map(|ch| public_buffer_char_code(ch, interp.buffer.borrow().is_multibyte()))
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

/// casefiddle.c's `struct casing_context': the Unicode property tables the
/// current casing operation may consult, resolved once up front.  GNU builds
/// this per operation (`prepare_casing_context'), never per character, and
/// leaves a table nil when the operation cannot use it.
#[derive(Default, Clone, Copy)]
pub(crate) struct CasingContext {
    titlecase: Option<CharTableRef>,
    special_upcase: Option<CharTableRef>,
    special_downcase: Option<CharTableRef>,
    special_titlecase: Option<CharTableRef>,
}

impl CasingContext {
    /// Mirror of casefiddle.c:70-85, including which tables each action nils.
    pub(crate) fn prepare(interp: &mut Interpreter, action: CaseAction, env: &mut Env) -> Self {
        // A table that is not a case table any more keeps its slots, as
        // GNU's upcase/downcase go on reading them.
        let _ = super::casetab::prepare_case_tables(interp);
        let capitalizing = matches!(action, CaseAction::Capitalize | CaseAction::UpcaseInitials);
        let table = |interp: &mut Interpreter,
                     property: crate::lisp::types::symbols::SymbolKey,
                     env: &mut Env| {
            let property = property.with(|symbol| Value::Symbol(*symbol));
            crate::lisp::primitives::dispatch::strings::uniprop_table_id(interp, property, env)
        };
        Self {
            titlecase: capitalizing
                .then(|| table(interp, cached_symbol!("titlecase"), env))
                .flatten(),
            special_upcase: (!matches!(action, CaseAction::Down))
                .then(|| table(interp, cached_symbol!("special-uppercase"), env))
                .flatten(),
            special_downcase: (!matches!(action, CaseAction::Up))
                .then(|| table(interp, cached_symbol!("special-lowercase"), env))
                .flatten(),
            special_titlecase: capitalizing
                .then(|| table(interp, cached_symbol!("special-titlecase"), env))
                .flatten(),
        }
    }

    fn titlecase_char(&self, interp: &Interpreter, code: u32) -> Option<u32> {
        let value = crate::lisp::primitives::dispatch::strings::uniprop_table_ref(
            interp,
            self.titlecase?,
            code,
        )?;
        u32::try_from(value.integer_value()?).ok()
    }

    fn special_string(&self, interp: &Interpreter, property: &str, code: u32) -> Option<String> {
        let table_id = match property {
            "special-uppercase" => self.special_upcase,
            "special-lowercase" => self.special_downcase,
            _ => self.special_titlecase,
        }?;
        let value =
            crate::lisp::primitives::dispatch::strings::uniprop_table_ref(interp, table_id, code)?;
        crate::lisp::primitives::string_like(&value).map(|string| string.text)
    }
}

pub(crate) fn current_case_table_ids(
    interp: &mut Interpreter,
) -> Result<(CharTableRef, CharTableRef), LispError> {
    // BVAR (current_buffer, downcase_table) and upcase_table.
    let tables = interp.current_case_tables();
    Ok((tables.down, tables.up))
}

pub(crate) fn case_table_mapping(
    interp: &Interpreter,
    table_id: CharTableRef,
    code: u32,
) -> Option<u32> {
    for candidate in [Some(code), alternate_case_key(code)].into_iter().flatten() {
        let Some(Kind::Integer(mapped)) = interp
            .char_table_explicit_get(table_id, candidate)
            .map(|v| v.kind())
        else {
            continue;
        };
        let mapped = u32::try_from(mapped).ok()?;
        return Some(normalize_case_key(mapped));
    }
    interp
        .char_table_get(table_id, code)
        .and_then(|value| value.integer_value())
        .and_then(|mapped| u32::try_from(mapped).ok())
        .map(normalize_case_key)
}

/// buffer.h: uppercasep and lowercasep use the current case tables, not
/// Unicode character properties. A character can belong to only one class.
pub(crate) fn current_case_classes(interp: &Interpreter, code: u32) -> (bool, bool) {
    let Some(tables) = interp.initialized_current_case_tables() else {
        return (matches!(code, 0x61..=0x7a), matches!(code, 0x41..=0x5a));
    };
    let original = normalize_case_key(code);
    let uppercase = case_table_mapping(interp, tables.down, code).unwrap_or(original) != original;
    let lowercase =
        !uppercase && case_table_mapping(interp, tables.up, code).unwrap_or(original) != original;
    (lowercase, uppercase)
}

pub(crate) fn case_symbols_as_words_enabled(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("case-symbols-as-words", env)
        .is_some_and(|value| value.is_truthy())
}

pub(crate) fn case_word_char(interp: &Interpreter, ch: char, case_symbols_as_words: bool) -> bool {
    syntax::current_syntax_word_char(interp, normalize_case_key(ch as u32), case_symbols_as_words)
}

pub(crate) fn full_upcase_string(
    interp: &Interpreter,
    context: &CasingContext,
    up_table: CharTableRef,
    ch: char,
) -> String {
    let code = ch as u32;
    if let Some(mapped) = context.special_string(interp, "special-uppercase", code) {
        return mapped;
    }
    if let Some(mapped) = case_table_mapping(interp, up_table, code) {
        return char::from_u32(denormalize_case_key(code, mapped))
            .unwrap_or(ch)
            .to_string();
    }
    ch.to_string()
}

pub(crate) fn full_downcase_string(
    interp: &Interpreter,
    context: &CasingContext,
    down_table: CharTableRef,
    ch: char,
    final_sigma: bool,
) -> String {
    let code = ch as u32;
    let mapped = context
        .special_string(interp, "special-lowercase", code)
        .unwrap_or_else(|| {
            case_table_mapping(interp, down_table, code)
                .and_then(|mapped| char::from_u32(denormalize_case_key(code, mapped)))
                .unwrap_or(ch)
                .to_string()
        });
    // casefiddle.c applies final sigma only if casing actually changed it.
    if code == 0x03A3 && final_sigma && mapped != ch.to_string() {
        '\u{03C2}'.to_string()
    } else {
        mapped
    }
}

pub(crate) fn full_titlecase_string(
    interp: &Interpreter,
    context: &CasingContext,
    up_table: CharTableRef,
    ch: char,
) -> String {
    let code = ch as u32;
    if let Some(mapped) = context.special_string(interp, "special-titlecase", code) {
        return mapped;
    }
    // casefiddle.c consults the `titlecase' uniprop before the case-table
    // upcase mapping.
    if let Some(mapped) = context.titlecase_char(interp, code) {
        return char::from_u32(mapped).unwrap_or(ch).to_string();
    }
    if let Some(mapped) = case_table_mapping(interp, up_table, code) {
        return char::from_u32(denormalize_case_key(code, mapped))
            .unwrap_or(ch)
            .to_string();
    }
    ch.to_string()
}

pub(crate) fn simple_case_char_for_action(
    interp: &Interpreter,
    context: &CasingContext,
    down_table: CharTableRef,
    up_table: CharTableRef,
    code: u32,
    action: CaseAction,
) -> u32 {
    // This path receives an actual Lisp character, not a host-text byte
    // placeholder. buffer.h:upcase/downcase consult exactly that table
    // entry (including its default/parent) and leave a non-natnum unmapped.
    let mapping = |table: CharTableRef| match table.get(code).kind() {
        Kind::Integer(mapped) if mapped >= 0 => mapped as u32,
        _ => code,
    };
    match action {
        CaseAction::Up => mapping(up_table),
        CaseAction::Down => mapping(down_table),
        // casefiddle.c:case_character_impl checks the `titlecase' uniprop
        // before falling back to the case-table upcase mapping.
        CaseAction::Capitalize | CaseAction::UpcaseInitials => context
            .titlecase_char(interp, code)
            .unwrap_or_else(|| mapping(up_table)),
    }
}

/// The text INPUT cased, with EXTENDED its side table (offsets of the
/// characters its Rust view cannot carry, which case by their codes);
/// returns the cased text and its side table.
pub(crate) fn casify_string(
    interp: &mut Interpreter,
    input: &str,
    extended: &[(usize, u32)],
    action: CaseAction,
    env: &mut Env,
) -> Result<(String, Vec<(usize, u32)>), LispError> {
    // GNU prepares the casing context once per operation, never per character.
    let context = CasingContext::prepare(interp, action, env);
    casify_string_with_context(interp, input, extended, action, env, &context)
}

fn casify_string_with_context(
    interp: &mut Interpreter,
    input: &str,
    extended: &[(usize, u32)],
    action: CaseAction,
    env: &mut Env,
    context: &CasingContext,
) -> Result<(String, Vec<(usize, u32)>), LispError> {
    let case_symbols_as_words = case_symbols_as_words_enabled(interp, env);
    let (down_table, up_table) = current_case_table_ids(interp)?;
    let chars: Vec<char> = input.chars().collect();
    let code_at = |idx: usize| {
        extended
            .binary_search_by_key(&idx, |(at, _)| *at)
            .ok()
            .map(|found| extended[found].1)
    };
    let word_at = |idx: usize| match code_at(idx) {
        Some(code) => syntax::current_syntax_word_char(interp, code, case_symbols_as_words),
        None => case_word_char(interp, chars[idx], case_symbols_as_words),
    };
    let mut output = String::new();
    let mut output_extended = Vec::new();
    let mut output_len = 0;
    let mut in_word = false;
    for (idx, ch) in chars.iter().copied().enumerate() {
        let is_word = word_at(idx);
        if let Some(code) = code_at(idx) {
            // casefiddle.c's case_character on the character's code: the
            // case table's mapping, if any.
            let table = match action {
                CaseAction::Up => Some(up_table),
                CaseAction::Down => Some(down_table),
                CaseAction::Capitalize | CaseAction::UpcaseInitials if is_word && !in_word => {
                    Some(up_table)
                }
                CaseAction::Capitalize => Some(down_table),
                CaseAction::UpcaseInitials => None,
            };
            let mapped = table
                .and_then(|table| case_table_mapping(interp, table, code))
                .unwrap_or(code);
            push_character_code(&mut output, &mut output_extended, output_len, mapped);
            output_len += 1;
            in_word = is_word;
            continue;
        }
        let next_is_word = idx + 1 < chars.len() && word_at(idx + 1);
        let piece = match action {
            CaseAction::Up => full_upcase_string(interp, context, up_table, ch),
            CaseAction::Down => {
                full_downcase_string(interp, context, down_table, ch, in_word && !next_is_word)
            }
            CaseAction::Capitalize => {
                if is_word && !in_word {
                    full_titlecase_string(interp, context, up_table, ch)
                } else {
                    full_downcase_string(interp, context, down_table, ch, in_word && !next_is_word)
                }
            }
            CaseAction::UpcaseInitials => {
                if is_word && !in_word {
                    full_titlecase_string(interp, context, up_table, ch)
                } else {
                    ch.to_string()
                }
            }
        };
        output_len += piece.chars().count();
        output.push_str(&piece);
        in_word = is_word;
    }
    Ok((output, output_extended))
}

pub(crate) fn casify_value(
    interp: &mut Interpreter,
    value: &Value,
    action: CaseAction,
    env: &mut Env,
) -> Result<Value, LispError> {
    let context = CasingContext::prepare(interp, action, env);
    if let Kind::Integer(integer) = value.kind()
        && integer >= 0
    {
        // casefiddle.c:do_casify_natnum narrows to C int, strips the six
        // event modifier bits before lookup, and restores them only when
        // casing changes the character. Higher bits otherwise preserve OBJ.
        const FLAG_BITS: u32 = 0x0fc0_0000;
        let character = integer as i32;
        if !(0..=FLAG_BITS as i32).contains(&character) {
            return Ok(*value);
        }
        let flags = character as u32 & FLAG_BITS;
        let mut code = character as u32 & !FLAG_BITS;
        let multibyte = code >= 256 || interp.buffer.borrow().is_multibyte();
        if !multibyte && code >= 128 {
            code += RAW_BYTE8_BASE;
        }
        let (down_table, up_table) = current_case_table_ids(interp)?;
        let mut mapped =
            simple_case_char_for_action(interp, &context, down_table, up_table, code, action);
        if mapped == code {
            return Ok(*value);
        }
        if !multibyte {
            mapped &= 0xff;
        }
        return Ok(Value::Integer(i64::from(mapped | flags)));
    }
    let input = string_like(value)
        .ok_or_else(|| LispError::WrongTypeArgument("char-or-string-p".into(), *value))?;
    if !input.multibyte {
        // casefiddle.c:do_casify_unibyte_string uses one-to-one casing,
        // preserving bytes and intervals. In locales such as Turkish,
        // an ASCII-to-wide mapping falls back to the Unicode ASCII table.
        return interp.with_lisp_stack_roots(value, |interp| {
            let (down, up) = current_case_table_ids(interp)?;
            let symbols_as_words = case_symbols_as_words_enabled(interp, env);
            let mut in_word = false;
            let mut output = String::new();
            for ch in input.text.chars() {
                let byte = string_character_code(false, ch) as u32;
                let code = if byte < 128 {
                    byte
                } else {
                    RAW_BYTE8_BASE + byte
                };
                let was_in_word = in_word;
                in_word = case_word_char(interp, ch, symbols_as_words);
                let flag = match action {
                    CaseAction::Capitalize if was_in_word => CaseAction::Down,
                    CaseAction::UpcaseInitials if was_in_word => {
                        output.push(ch);
                        continue;
                    }
                    other => other,
                };
                // GNU converts high bytes to byte8 character codes before
                // indexing the table. A Latin-1 mapping at the byte's numeric
                // value must not act as a fallback for an unset byte8 entry.
                let table = if flag == CaseAction::Down { down } else { up };
                let mapped = interp
                    .char_table_get(table, code)
                    .and_then(|value| value.integer_value())
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(code);
                let mut mapped =
                    if matches!(flag, CaseAction::Capitalize | CaseAction::UpcaseInitials) {
                        context.titlecase_char(interp, code).unwrap_or(mapped)
                    } else {
                        mapped
                    };
                if code < 128 && mapped >= 256 {
                    let property = if flag == CaseAction::Down {
                        cached_symbol!("lowercase")
                    } else {
                        cached_symbol!("uppercase")
                    }
                    .with(|symbol| Value::Symbol(*symbol));
                    mapped = dispatch::strings::uniprop_table_id(interp, property, env)
                        .and_then(|table| interp.char_table_get(table, code))
                        .and_then(|value| value.integer_value())
                        .and_then(|value| u32::try_from(value).ok())
                        .unwrap_or(code);
                }
                let byte = (mapped & 0xff) as u8;
                output.push(if byte < 128 {
                    char::from(byte)
                } else {
                    raw_byte_regex_char(byte)
                });
            }
            Ok(make_shared_string_value_with_multibyte(
                output,
                input.props,
                false,
            ))
        });
    }
    let input_len = input.text.chars().count();
    let (output, output_extended) = casify_string_with_context(
        interp,
        &input.text,
        &input.extended_chars,
        action,
        env,
        &context,
    )?;
    // casefiddle.c copies intervals while casing can stay in the source
    // string's character footprint.  A full Unicode mapping that changes
    // the character count (for example, sharp-s -> "SS") takes GNU's
    // rebuilt-string path and drops all intervals instead.
    let props = if output.chars().count() == input_len {
        input.props
    } else {
        Vec::new()
    };
    Ok(string_like_value_with_extended_chars(
        output,
        props,
        input.multibyte,
        output_extended,
    ))
}

pub(crate) fn replace_buffer_region_with_text(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    text: &str,
    extended: &[(usize, u32)],
) -> Result<usize, LispError> {
    interp.buffer.borrow_mut().goto_char(start);
    interp
        .buffer
        .borrow_mut()
        .delete_region(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    interp.buffer.borrow_mut().insert(text);
    interp
        .buffer
        .borrow_mut()
        .set_inserted_extended_chars(start, extended);
    Ok(start + text.chars().count())
}

pub(crate) fn casify_buffer_region(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    action: CaseAction,
    env: &mut Env,
) -> Result<usize, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    if lo >= hi {
        return Ok(hi);
    }
    // casefiddle.c:casify_region: modify_text over the region, the text
    // cased as the hooks left it, then a signal of the characters that
    // changed, none when none did.
    modify_current_buffer(interp, lo, hi, false, env, |interp, _, env| {
        let hi = hi.min(interp.buffer.borrow().point_max()).max(lo);
        let (text, extended) = {
            let buffer = interp.buffer.borrow();
            (
                buffer
                    .buffer_substring(lo, hi)
                    .map_err(|error| LispError::Signal(error.to_string()))?,
                buffer.substring_extended_chars(lo, hi),
            )
        };
        let (mapped, mapped_extended) = casify_string(interp, &text, &extended, action, env)?;
        if mapped == text && mapped_extended == extended {
            return Ok((None, hi));
        }
        // The texts compare by character code.
        let codes = |text: &str, extended: &[(usize, u32)]| {
            StringLike {
                text: text.to_owned(),
                props: Vec::new(),
                multibyte: true,
                extended_chars: extended.to_vec(),
            }
            .character_codes()
        };
        let old_codes = codes(&text, &extended);
        let new_codes = codes(&mapped, &mapped_extended);
        let old_length = hi - lo;
        let new_length = new_codes.len();
        let new_end = lo + new_length;
        let unchanged_prefix = old_codes
            .iter()
            .zip(&new_codes)
            .take_while(|(old, new)| old == new)
            .count();
        let unchanged_suffix = old_codes
            .iter()
            .rev()
            .zip(new_codes.iter().rev())
            .take(old_length.min(new_length) - unchanged_prefix)
            .take_while(|(old, new)| old == new)
            .count();
        if new_length == old_length {
            interp.buffer.borrow_mut().replace_region_in_place(
                lo,
                hi,
                &mapped,
                &mapped_extended,
                false,
            );
        } else {
            replace_buffer_region_with_text(interp, lo, hi, &mapped, &mapped_extended)?;
        }
        let changed_start = lo + unchanged_prefix;
        Ok((
            Some(AfterChange::new(
                changed_start,
                hi - unchanged_suffix - changed_start,
                new_end - unchanged_suffix - changed_start,
            )),
            new_end,
        ))
    })
}
