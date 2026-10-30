use super::*;
use crate::lisp::types::Kind;

pub(crate) fn internal_text_bytes(text: &str, multibyte: bool) -> Result<Vec<u8>, LispError> {
    if multibyte {
        let mut bytes = Vec::with_capacity(text.len());
        for ch in text.chars() {
            let code = public_buffer_char_code(ch, true) as u32;
            push_emacs_multibyte_char(&mut bytes, code)?;
        }
        Ok(bytes)
    } else {
        encode_raw_text_bytes(text)
    }
}

pub(crate) fn internal_string_bytes(string: &StringLike) -> Result<Vec<u8>, LispError> {
    if !string.multibyte {
        return encode_raw_text_bytes(&string.text);
    }
    let mut bytes = Vec::with_capacity(string.byte_len()?);
    for code in string.character_codes() {
        let code =
            u32::try_from(code).map_err(|_| LispError::Signal("Invalid character".into()))?;
        push_emacs_multibyte_char(&mut bytes, code)?;
    }
    Ok(bytes)
}

/// fns.c:extract_data_from_object's (iv-auto LENGTH): LENGTH bytes from
/// getrandom.
pub(crate) fn iv_auto_bytes(length: usize) -> Result<Vec<u8>, LispError> {
    let mut bytes = vec![0u8; length];
    getrandom::fill(&mut bytes)
        .map_err(|error| LispError::Signal(format!("Getting random data: {error}")))?;
    Ok(bytes)
}

pub(crate) fn secure_hash_digest(algorithm: &str, input: &[u8]) -> Result<Vec<u8>, LispError> {
    Ok(match algorithm {
        "md5" => crate::lisp::md5::md5_buffer(input).to_vec(),
        "sha1" => Sha1::digest(input).to_vec(),
        "sha224" => Sha224::digest(input).to_vec(),
        "sha256" => Sha256::digest(input).to_vec(),
        "sha384" => Sha384::digest(input).to_vec(),
        "sha512" => Sha512::digest(input).to_vec(),
        "sha3-224" => Sha3_224::digest(input).to_vec(),
        "sha3-256" => Sha3_256::digest(input).to_vec(),
        "sha3-384" => Sha3_384::digest(input).to_vec(),
        "sha3-512" => Sha3_512::digest(input).to_vec(),
        "streebog-256" => streebog::Streebog256::digest(input).to_vec(),
        "streebog-512" => streebog::Streebog512::digest(input).to_vec(),
        "gost94-cryptopro" => gost94::Gost94CryptoPro::digest(input).to_vec(),
        _ => {
            return Err(LispError::Signal(format!(
                "Invalid algorithm arg: {algorithm}"
            )));
        }
    })
}

/// fns.c:hexbuf_digest.
pub(crate) fn digest_hex(bytes: &[u8]) -> String {
    const HEXDIGIT: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(char::from(HEXDIGIT[usize::from(byte >> 4)]));
        output.push(char::from(HEXDIGIT[usize::from(byte & 0xf)]));
    }
    output
}

/// fns.c:Fsecure_hash: secure_hash of extract_data_from_object's bytes,
/// as `md5' reads them without a coding system or NOERROR.
pub(crate) fn secure_hash_value(
    interp: &mut Interpreter,
    algorithm: &str,
    source: &Value,
    start: Option<&Value>,
    end: Option<&Value>,
    binary: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let spec = [
        *source,
        start.copied().unwrap_or(Value::Nil),
        end.copied().unwrap_or(Value::Nil),
    ];
    let input = md5_source_bytes(interp, &spec, env)?;
    let digest = secure_hash_digest(algorithm, &input)?;
    if binary.is_some_and(Value::is_truthy) {
        Ok(bytes_to_shared_unibyte_value(&digest))
    } else {
        Ok(Value::String(digest_hex(&digest).into()))
    }
}

/// fns.c:Fbuffer_hash: the SHA-1 of the buffer's whole text, narrowing
/// aside, as it is held.
pub(crate) fn buffer_hash_value(
    interp: &mut Interpreter,
    buffer_or_name: Option<&Value>,
) -> Result<Value, LispError> {
    // Fget_buffer and nsberror (GNU reads a killed buffer's freed text).
    let buffer_id = match buffer_or_name {
        Some(value) if !value.is_nil() => {
            let buffer = interp.get_buffer_or_error(value)?;
            interp
                .live_buffer_id(buffer)
                .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))?
        }
        _ => interp.current_buffer_id(),
    };
    let bytes = {
        let buffer = interp
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
        internal_text_bytes(&buffer.full_buffer_string(), buffer.is_multibyte())?
    };
    let digest = secure_hash_digest("sha1", &bytes)?;
    Ok(Value::String(digest_hex(&digest).into()))
}

pub(crate) fn text_byte_len(ch: char, multibyte: bool) -> usize {
    if multibyte {
        ch.len_utf8()
    } else if raw_byte_from_regex_char(ch).is_some() || (ch as u32) <= 0xFF {
        1
    } else {
        ch.len_utf8()
    }
}

pub(crate) fn levenshtein_distance<T: Eq>(left: &[T], right: &[T]) -> usize {
    if left.is_empty() {
        return right.len();
    }
    if right.is_empty() {
        return left.len();
    }

    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0usize; right.len() + 1];

    for (left_index, left_item) in left.iter().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_item) in right.iter().enumerate() {
            let substitution_cost = usize::from(left_item != right_item);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + substitution_cost);
        }
        std::mem::swap(&mut previous, &mut current);
    }

    previous[right.len()]
}

pub(crate) fn string_distance_value(
    left: &Value,
    right: &Value,
    compare_bytes: bool,
) -> Result<Value, LispError> {
    let left_string =
        string_like(left).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *left))?;
    let right_string =
        string_like(right).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *right))?;

    let distance = if compare_bytes {
        let left_bytes = internal_string_bytes(&left_string)?;
        let right_bytes = internal_string_bytes(&right_string)?;
        levenshtein_distance(&left_bytes, &right_bytes)
    } else {
        let left_chars = left_string.text.chars().collect::<Vec<_>>();
        let right_chars = right_string.text.chars().collect::<Vec<_>>();
        levenshtein_distance(&left_chars, &right_chars)
    };

    Ok(Value::Integer(distance as i64))
}

pub(crate) fn buffer_line_statistics_value(
    interp: &mut Interpreter,
    buffer_or_name: Option<&Value>,
) -> Result<Value, LispError> {
    let (text, multibyte) = match buffer_or_name {
        Some(value) if !value.is_nil() => {
            let buffer = interp.get_buffer_or_error(value)?;
            let buffer_id = interp
                .live_buffer_id(buffer)
                .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))?;
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            (buffer.buffer_string(), buffer.is_multibyte())
        }
        _ => (
            interp.buffer.borrow().buffer_string(),
            interp.buffer.borrow().is_multibyte(),
        ),
    };

    if text.is_empty() {
        return Ok(Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::float(0.0),
        ]));
    }

    let mut lines = 0usize;
    let mut longest = 0usize;
    let mut total = 0usize;
    let mut current = 0usize;

    for ch in text.chars() {
        if ch == '\n' {
            lines += 1;
            longest = longest.max(current);
            total += current;
            current = 0;
        } else {
            current += text_byte_len(ch, multibyte);
        }
    }

    if !text.ends_with('\n') {
        lines += 1;
        longest = longest.max(current);
        total += current;
    }

    let mean = if lines == 0 {
        0.0
    } else {
        total as f64 / lines as f64
    };

    Ok(Value::list([
        Value::Integer(lines as i64),
        Value::Integer(longest as i64),
        Value::float(mean),
    ]))
}

/// buffer.h CHARACTER_WIDTH: the columns the character C takes in the
/// current buffer: 1 for printable ASCII, `char-width-table''s entry
/// (sanitized) above DEL, the buffer's tab width for TAB, 0 for newline,
/// and 2 or 4 for another control character as `ctl-arrow' is set.
pub(crate) fn character_width(interp: &Interpreter, c: u32, env: &Env) -> usize {
    if (0x20..0x7F).contains(&c) {
        return 1;
    }
    if c > 0x7F {
        let width = interp
            .lookup_var("char-width-table", env)
            .and_then(|table| match table.kind() {
                Kind::CharTable(table) => interp.char_table_get(table, c),
                _ => None,
            })
            .and_then(|width| width.integer_value())
            .unwrap_or(0);
        // sanitize_char_width.
        return if (0..=1000).contains(&width) {
            width as usize
        } else {
            1000
        };
    }
    if c == u32::from(b'\t') {
        // SANE_TAB_WIDTH.
        return interp
            .lookup_var("tab-width", env)
            .and_then(|width| width.integer_value())
            .filter(|width| (1..=1000).contains(width))
            .unwrap_or(8) as usize;
    }
    if c == u32::from(b'\n') {
        return 0;
    }
    if interp
        .lookup_var("ctl-arrow", env)
        .is_some_and(|value| value.is_truthy())
    {
        2
    } else {
        4
    }
}

/// window.c buffer_display_table: the buffer's `buffer-display-table'
/// when it is a display table, else `standard-display-table' when that is
/// (DISP_TABLE_P: purpose `display-table' and its six extra slots).
pub(crate) fn buffer_display_table(
    interp: &Interpreter,
    env: &Env,
) -> Option<crate::lisp::types::CharTableRef> {
    [
        cached_symbol!("buffer-display-table"),
        cached_symbol!("standard-display-table"),
    ]
    .into_iter()
    .find_map(|key| match interp.lookup_var_key(key, env)?.kind() {
        Kind::CharTable(table)
            if table.has_purpose("display-table") && table.extra_count() == 6 =>
        {
            Some(table)
        }
        _ => None,
    })
}

/// character.c char_width: CHARACTER_WIDTH of C, or, when the display
/// table DP shows C as a vector, the sum of its glyphs' characters' widths.
pub(crate) fn display_char_width(
    interp: &Interpreter,
    c: u32,
    dp: Option<crate::lisp::types::CharTableRef>,
    env: &Env,
) -> usize {
    let Some(entry) = dp.and_then(|dp| interp.char_table_get(dp, c)) else {
        return character_width(interp, c, env);
    };
    if !crate::lisp::primitives::interactive::is_vector_value(&entry) {
        return character_width(interp, c, env);
    }
    let glyphs = crate::lisp::primitives::buffers::vector_items(&entry).unwrap_or_default();
    glyphs
        .iter()
        .filter_map(|glyph| match glyph.kind() {
            // GLYPH_CODE_P and GLYPH_CODE_CHAR: (CHAR . FACE) or the
            // character in a glyph code's low 22 bits.
            Kind::Cons(cell) => match (cell.car.get().kind(), cell.cdr.get().kind()) {
                (Kind::Integer(c), Kind::Integer(face))
                    if (0..=MAX_CHAR as i64).contains(&c) && face >= 0 =>
                {
                    Some(c as u32)
                }
                _ => None,
            },
            Kind::Integer(code) if code >= 0 => Some((code & 0x3F_FFFF) as u32),
            _ => None,
        })
        .map(|c| character_width(interp, c, env))
        .sum()
}

/// editfns.c styled_format's %c: the character, with the side-table
/// entry a character beyond the Rust text view needs.
pub(crate) fn format_char_conversion(
    arg: &Value,
) -> Result<(String, Vec<(usize, u32)>), LispError> {
    let n = match arg.kind() {
        Kind::Integer(n) => n,
        Kind::BigInteger(n) => n
            .to_i64()
            .ok_or_else(|| LispError::WrongTypeArgument("characterp".into(), *arg))?,
        Kind::Float(_) => {
            return Err(LispError::TypeError("integer".into(), "float".into()));
        }
        _ => {
            return Err(LispError::WrongTypeArgument("integerp".into(), *arg));
        }
    };
    if !(0..=MAX_CHAR as i64).contains(&n) {
        return Err(LispError::Signal(format!("Invalid character: {n}")));
    }
    let mut text = String::new();
    let mut extended = Vec::new();
    push_character_code(&mut text, &mut extended, 0, n as u32);
    Ok((text, extended))
}

pub(crate) fn bigint_from_truncated_float(value: f64) -> Result<BigInt, LispError> {
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
