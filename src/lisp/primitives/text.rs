use super::*;

pub(crate) fn string_slice_chars(
    text: &str,
    start: Option<&Value>,
    end: Option<&Value>,
) -> Result<String, LispError> {
    let chars: Vec<char> = text.chars().collect();
    let start = normalize_string_index(start, 0, chars.len() as i64)? as usize;
    let end = normalize_string_index(end, chars.len() as i64, chars.len() as i64)? as usize;
    Ok(chars[start..end].iter().collect())
}

pub(crate) fn internal_text_bytes(text: &str, multibyte: bool) -> Result<Vec<u8>, LispError> {
    if multibyte {
        Ok(text.as_bytes().to_vec())
    } else {
        encode_raw_text_bytes(text)
    }
}

pub(crate) fn secure_hash_source_bytes(
    interp: &mut Interpreter,
    source: &Value,
    start: Option<&Value>,
    end: Option<&Value>,
) -> Result<Vec<u8>, LispError> {
    if let Value::Symbol(symbol) = source
        && symbol == "iv-auto"
    {
        let length = start
            .ok_or_else(|| LispError::Signal("Without a length, `iv-auto' can't be used".into()))?
            .as_integer()? as usize;
        let mut bytes = vec![0u8; length];
        getrandom::fill(&mut bytes)
            .map_err(|error| LispError::Signal(format!("Getting random data: {error}")))?;
        return Ok(bytes);
    }

    match source {
        Value::Buffer(_) => {
            let buffer_id = interp.resolve_buffer_id(source)?;
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            let start = start
                .filter(|value| !value.is_nil())
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| buffer.point_min());
            let end = end
                .filter(|value| !value.is_nil())
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| buffer.point_max());
            let text = buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            internal_text_bytes(&text, buffer.is_multibyte())
        }
        _ => {
            let string = string_like(source)
                .ok_or_else(|| LispError::TypeError("string".into(), source.type_name()))?;
            let slice = string_slice_chars(&string.text, start, end)?;
            internal_text_bytes(&slice, string.multibyte)
        }
    }
}

pub(crate) fn secure_hash_digest(algorithm: &str, input: &[u8]) -> Result<Vec<u8>, LispError> {
    Ok(match algorithm {
        "md5" => md5::compute(input).0.to_vec(),
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

pub(crate) fn digest_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

pub(crate) fn secure_hash_value(
    interp: &mut Interpreter,
    algorithm: &str,
    source: &Value,
    start: Option<&Value>,
    end: Option<&Value>,
    binary: Option<&Value>,
) -> Result<Value, LispError> {
    let input = secure_hash_source_bytes(interp, source, start, end)?;
    let digest = secure_hash_digest(algorithm, &input)?;
    if binary.is_some_and(Value::is_truthy) {
        Ok(bytes_to_shared_unibyte_value(&digest))
    } else {
        Ok(Value::String(digest_hex(&digest).into()))
    }
}

pub(crate) fn buffer_hash_value(
    interp: &mut Interpreter,
    buffer_or_name: Option<&Value>,
) -> Result<Value, LispError> {
    let bytes = match buffer_or_name {
        Some(value) if !value.is_nil() => secure_hash_source_bytes(interp, value, None, None)?,
        _ => internal_text_bytes(&interp.buffer.buffer_string(), interp.buffer.is_multibyte())?,
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
        string_like(left).ok_or_else(|| LispError::TypeError("string".into(), left.type_name()))?;
    let right_string = string_like(right)
        .ok_or_else(|| LispError::TypeError("string".into(), right.type_name()))?;

    let distance = if compare_bytes {
        let left_bytes = internal_text_bytes(&left_string.text, left_string.multibyte)?;
        let right_bytes = internal_text_bytes(&right_string.text, right_string.multibyte)?;
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
        _ => (interp.buffer.buffer_string(), interp.buffer.is_multibyte()),
    };

    if text.is_empty() {
        return Ok(Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::Float(0.0),
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
        Value::Float(mean),
    ]))
}

pub(crate) fn format_char_conversion(arg: &Value) -> Result<String, LispError> {
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
    char_from_integer(n)
        .map(|character| character.to_string())
        .map_err(|_| LispError::Signal(format!("Invalid character: {n}")))
}

pub(crate) fn format_s_conversion(
    interp: &mut Interpreter,
    arg: &Value,
    precision: Option<usize>,
    env: &mut crate::lisp::types::Env,
) -> Result<(String, Vec<TextPropertySpan>), LispError> {
    if let Some(string) = string_like(arg) {
        let end = precision
            .unwrap_or_else(|| string.text.chars().count())
            .min(string.text.chars().count());
        let text = string.text.chars().take(end).collect::<String>();
        let props = slice_string_props(&string.props, 0, end);
        return Ok((text, props));
    }
    // `%s' uses princ semantics: a buffer prints as its name.
    if let Value::Buffer(buffer) = arg {
        let mut text = buffer.name.to_string();
        if let Some(precision) = precision {
            text = text.chars().take(precision).collect();
        }
        return Ok((text, Vec::new()));
    }
    let mut text = if ["print-circle", "print-gensym"].into_iter().any(|name| {
        interp
            .lookup_var(name, env)
            .is_some_and(|value| value.is_truthy())
    }) {
        render_prin1_ephemeral(interp, arg, env)?
    } else {
        number_to_string(arg).unwrap_or_else(|_| arg.to_string())
    };
    if let Some(precision) = precision {
        text = text.chars().take(precision).collect();
    }
    Ok((text, Vec::new()))
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
    match value {
        Value::Integer(n) => Ok((Some(*n), BigInt::from(*n))),
        Value::BigInteger(n) => Ok((None, n.clone().into())),
        Value::Float(f) => Ok((None, bigint_from_truncated_float(*f)?)),
        Value::Marker(_) => {
            let n = integer_like_i64(interp, value)?;
            Ok((Some(n), BigInt::from(n)))
        }
        _ => Err(LispError::TypeError("integer".into(), value.type_name())),
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
