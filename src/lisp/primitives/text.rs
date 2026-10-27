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
    let buffer_id = match buffer_or_name {
        Some(value) if !value.is_nil() => interp.resolve_buffer_id(value)?,
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
            let buffer_id = interp.resolve_buffer_id(value)?;
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
    ["buffer-display-table", "standard-display-table"]
        .into_iter()
        .find_map(|name| match interp.lookup_var(name, env)?.kind() {
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

/// A conversion's text, its properties and its side table.
pub(crate) type FormattedText = (String, Vec<TextPropertySpan>, Vec<(usize, u32)>);

/// styled_format's %s: a string argument's text up to PRECISION
/// characters with its properties and side table, else `princ''s.
pub(crate) fn format_s_conversion(
    interp: &mut Interpreter,
    arg: &Value,
    precision: Option<usize>,
    env: &mut crate::lisp::types::Env,
) -> Result<FormattedText, LispError> {
    // A whole string without properties is its text, copied once.
    if precision.is_none()
        && string_without_properties(arg)
        && let Some(text) = with_string_text(arg, str::to_owned)
    {
        return Ok((text, Vec::new(), string_extended_chars(arg)));
    }
    if let Some(string) = string_like(arg) {
        let end = precision
            .unwrap_or_else(|| string.text.chars().count())
            .min(string.text.chars().count());
        let text = string.text.chars().take(end).collect::<String>();
        // styled_format copies an argument's intervals with
        // add_text_properties_from_list: onto the fresh result, each
        // span's pairs come out reversed (`copied_string_props').
        let props = copied_string_props(&slice_string_props(&string.props, 0, end), 0);
        let mut extended = string.extended_chars;
        extended.retain(|(at, _)| *at < end);
        return Ok((text, props, extended));
    }
    // format.c's %s is print.c's `print_object' with `escapeflag' cleared,
    // which is exactly `princ' -- including for nested elements, so
    // (format "%s" (list "a")) yields `(a)' and not `("a")'.
    let (mut text, mut extended) = match number_to_string(arg) {
        Ok(number) => (number, Vec::new()),
        Err(_) => {
            crate::lisp::primitives::print::render_printer_output(interp, arg, env, false, false)?
                .text_parts()
        }
    };
    if let Some(precision) = precision {
        text = text.chars().take(precision).collect();
        extended.retain(|(at, _)| *at < precision);
    }
    Ok((text, Vec::new(), extended))
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

pub(crate) fn integer_for_format(
    interp: &Interpreter,
    value: &Value,
) -> Result<(Option<i64>, BigInt), LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok((Some(n), BigInt::from(n))),
        Kind::BigInteger(n) => Ok((None, (n).into())),
        Kind::Float(f) => Ok((None, bigint_from_truncated_float(f.get())?)),
        Kind::Marker(_) => {
            let n = integer_like_i64(interp, value)?;
            Ok((Some(n), BigInt::from(n)))
        }
        _ => Err(LispError::WrongTypeArgument("integerp".into(), *value)),
    }
}

pub(crate) fn format_bigint_radix(value: &BigInt, radix: u32, upper: bool) -> String {
    let mut text = value.abs().to_str_radix(radix);
    if upper {
        text.make_ascii_uppercase();
    }
    text
}

pub(crate) fn apply_precision(mut digits: String, precision: Option<usize>) -> String {
    if let Some(precision) = precision
        && digits.len() < precision
    {
        digits = format!("{}{}", "0".repeat(precision - digits.len()), digits);
    }
    digits
}

pub(crate) fn format_numeric_conversion(
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

pub(crate) fn format_float_conversion(
    interp: &Interpreter,
    arg: &Value,
    flag_plus: bool,
    flag_space: bool,
    precision: Option<usize>,
) -> Result<String, LispError> {
    let value = numeric_to_f64(interp, arg)?;
    let precision = precision.unwrap_or(6);
    let mut text = format!("{value:.precision$}");
    if !text.starts_with('-') {
        if flag_plus {
            text.insert(0, '+');
        } else if flag_space {
            text.insert(0, ' ');
        }
    }
    Ok(text)
}

/// C-style %e / %g conversions ((format "%e" 3.5) => "3.500000e+00").
pub(crate) fn format_exponential_conversion(
    interp: &Interpreter,
    arg: &Value,
    conv: char,
    flag_plus: bool,
    flag_space: bool,
    precision: Option<usize>,
) -> Result<String, LispError> {
    let value = numeric_to_f64(interp, arg)?;
    let mut text = if conv == 'e' {
        let precision = precision.unwrap_or(6);
        let raw = format!("{value:.precision$e}");
        // Rust renders "3.5e0"; C uses a sign and at least two exponent
        // digits ("3.500000e+00").
        match raw.split_once('e') {
            Some((mantissa, exponent)) => {
                let (sign, digits) = match exponent.strip_prefix('-') {
                    Some(rest) => ('-', rest),
                    None => ('+', exponent),
                };
                format!("{mantissa}e{sign}{:0>2}", digits)
            }
            None => raw,
        }
    } else {
        // %g: %e for very large/small magnitudes, otherwise %f with
        // trailing zeros trimmed; Rust's default float Display matches
        // the common cases.
        if value != 0.0 && (value.abs() >= 1e6 || value.abs() < 1e-4) {
            format!("{value:e}")
        } else {
            format!("{value}")
        }
    };
    if !text.starts_with('-') {
        if flag_plus {
            text.insert(0, '+');
        } else if flag_space {
            text.insert(0, ' ');
        }
    }
    Ok(text)
}
