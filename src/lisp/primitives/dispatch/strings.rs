use super::*;

/// Map an Emacs character code to a Rust char, translating the raw-byte
/// range (RAW_BYTE8_BASE #x3FFF00..) to the internal private-use marker.
fn char_for_codepoint(n: i64) -> Result<char, LispError> {
    char_from_integer(n).map_err(|_| LispError::Signal(format!("Invalid character: {n}")))
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Allocation ──
            "make-string" => {
                if args.is_empty() || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(
                        "make-string".into(),
                        args.len(),
                    ));
                }
                let length = args[0].as_integer()?;
                if length < 0 {
                    return Err(LispError::Signal("Wrong type argument: natnump".into()));
                }
                let init = args[1].as_integer()?;
                let c = char_for_codepoint(init)?;
                let s: String = std::iter::repeat_n(c, length as usize).collect();
                let multibyte = s
                    .chars()
                    .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
                Ok(make_shared_string_value_with_multibyte(
                    s,
                    Vec::new(),
                    multibyte,
                ))
            }
            "make-temp-name" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    make_temp_name(&string_text(&args[0])?).into(),
                ))
            }
            "make-vector" => {
                need_args(name, args, 2)?;
                let length = args[0].as_integer()?;
                if length < 0 {
                    return Err(LispError::Signal("Wrong type argument: natnump".into()));
                }
                let init = args[1].clone();
                let items: Vec<Value> = std::iter::repeat_n(init, length as usize).collect();
                let mut result = vec![Value::symbol("vector-literal")];
                result.extend(items);
                Ok(Value::list(result))
            }
            "make-bool-vector" => {
                need_args(name, args, 2)?;
                let length = args[0].as_integer()?;
                if length < 0 {
                    return Err(LispError::Signal("Wrong type argument: natnump".into()));
                }
                Ok(make_bool_vector_value(
                    interp,
                    std::iter::repeat_n(args[1].is_truthy(), length as usize),
                ))
            }
            "bool-vector" => Ok(make_bool_vector_value(
                interp,
                args.iter().map(Value::is_truthy),
            )),
            "make-keymap" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(make_runtime_full_keymap(
                    interp,
                    args.first()
                        .and_then(string_like)
                        .map(|string| string.text)
                        .as_deref(),
                ))
            }
            "make-sparse-keymap" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(make_runtime_keymap(
                    interp,
                    args.first()
                        .and_then(string_like)
                        .map(|string| string.text)
                        .as_deref(),
                ))
            }
            "vconcat" => {
                let mut items = vec![Value::symbol("vector-literal")];
                for value in args {
                    items.extend(sequence_values(interp, value)?);
                }
                Ok(Value::list(items))
            }
            "copy-keymap" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::Record(id)
                        if interp.find_record(*id).is_some_and(|record| {
                            record.kind == crate::lisp::eval::RecordKind::Keymap
                        }) =>
                    {
                        interp.copy_record(*id)
                    }
                    _ => Ok(args[0].clone()),
                }
            }
            "record" => {
                need_args(name, args, 1)?;
                Ok(interp.create_record_with_type(args[0].clone(), args[1..].to_vec()))
            }
            "make-record" => {
                need_args(name, args, 3)?;
                let length = args[1].as_integer()?;
                if length < 0 {
                    return Err(LispError::Signal("Wrong type argument: natnump".into()));
                }
                Ok(interp.create_record_with_type(
                    args[0].clone(),
                    std::iter::repeat_n(args[2].clone(), length as usize).collect(),
                ))
            }
            "make-finalizer" => {
                need_args(name, args, 1)?;
                Ok(Value::Finalizer(interp.alloc_finalizer_id()))
            }

            // ── String operations ──
            "concat" => {
                let mut result = String::new();
                let mut props = Vec::new();
                let mut multibyte = false;
                // Plain-string arguments only need one text copy; the generic
                // StringLike route below clones each argument and re-derives
                // property offsets, which made repeated accumulation
                // (`(setq s (concat "x" s))') quadratic with a large constant.
                // The final value can skip the result re-scan only when every
                // argument's multibyte verdict came from an authoritative scan
                // here, so track that.
                let mut all_plain_scanned = true;
                for a in args {
                    match a {
                        Value::String(text) => {
                            if !multibyte && !text.is_ascii() {
                                multibyte |= text
                                    .chars()
                                    .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7f);
                            }
                            result.push_str(text);
                            continue;
                        }
                        Value::StringObject(state) if state.borrow().props.is_empty() => {
                            let state = state.borrow();
                            // Cached flags may be stale relative to the text, so
                            // route the final value through the re-scanning
                            // constructor below.
                            all_plain_scanned = false;
                            multibyte |= state.multibyte;
                            result.push_str(&state.text);
                            continue;
                        }
                        _ => {}
                    }
                    all_plain_scanned = false;
                    if let Some(string) = string_like(a) {
                        let offset = result.chars().count();
                        result.push_str(&string.text);
                        props.extend(copied_string_props(&string.props, offset));
                        multibyte |= string.multibyte;
                    } else if a.is_nil() {
                    } else if matches!(a, Value::Cons(_))
                        || is_vector_value(a)
                        || is_bool_vector_value(interp, a)
                    {
                        let (text, text_multibyte) = concat_sequence_string(interp, a)?;
                        result.push_str(&text);
                        multibyte |= text_multibyte;
                    } else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("sequencep".into()),
                            a.clone(),
                        ])));
                    }
                }
                if all_plain_scanned && !multibyte {
                    return Ok(Value::String(result.into()));
                }
                Ok(string_like_value_with_multibyte(
                    result,
                    merge_string_props(props),
                    multibyte,
                ))
            }
            "string-match" => regexp::string_match_impl(interp, args, env, true),

            "posix-string-match" => regexp::posix_string_match_impl(interp, args, env),
            "string-width" => {
                if args.is_empty() || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(
                        "string-width".into(),
                        args.len(),
                    ));
                }
                let text = string_text(&args[0])?;
                let chars: Vec<char> = text.chars().collect();
                let len = chars.len() as i64;
                let start = normalize_string_index(args.get(1), 0, len)?;
                let end = normalize_string_index(args.get(2), len, len)?;
                if end < start {
                    return Err(LispError::Signal("Args out of range".into()));
                }
                let tab_width = interp
                    .lookup_var("tab-width", &Vec::new())
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(8)
                    .max(1) as usize;
                let mut width = 0usize;
                for ch in chars[start as usize..end as usize].iter().copied() {
                    if ch == '\t' {
                        width += tab_width;
                    } else {
                        width += ch.width().unwrap_or(0);
                    }
                }
                Ok(Value::Integer(width as i64))
            }
            "char-width" => {
                need_args(name, args, 1)?;
                let codepoint = args[0].as_integer()?;
                let ch = char::from_u32(codepoint as u32)
                    .ok_or_else(|| LispError::Signal(format!("Invalid character: {codepoint}")))?;
                let width = if ch == '\t' {
                    interp
                        .lookup_var("tab-width", &Vec::new())
                        .and_then(|value| value.as_integer().ok())
                        .unwrap_or(8)
                        .max(1) as usize
                } else {
                    ch.width().unwrap_or(0)
                };
                Ok(Value::Integer(width as i64))
            }
            "string" => {
                let mut result = String::new();
                let mut multibyte = false;
                for arg in args {
                    let code = arg.as_integer()?;
                    result.push(char_for_codepoint(code)?);
                    multibyte |= code > 0x7F;
                }
                Ok(string_like_value_with_multibyte(
                    result,
                    Vec::new(),
                    multibyte,
                ))
            }
            "substring" | "substring-no-properties" => {
                if args.is_empty() || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs("substring".into(), args.len()));
                }
                if is_vector_value(&args[0]) {
                    let items = vector_items(&args[0])?;
                    let len = items.len() as i64;
                    let from = normalize_string_index(args.get(1), 0, len)? as usize;
                    let to = normalize_string_index(args.get(2), len, len)? as usize;
                    return Ok(Value::list(
                        std::iter::once(Value::symbol("vector-literal"))
                            .chain(items[from..to].iter().cloned()),
                    ));
                }
                if is_bool_vector_value(interp, &args[0]) {
                    let items = bool_vector_values(interp, &args[0])?;
                    let len = items.len() as i64;
                    let from = normalize_string_index(args.get(1), 0, len)? as usize;
                    let to = normalize_string_index(args.get(2), len, len)? as usize;
                    return Ok(make_bool_vector_value(
                        interp,
                        items[from..to].iter().map(Value::is_truthy),
                    ));
                }
                // editfns.c Fsubstring's check is CHECK_ARRAY: `arrayp'.
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("arrayp".into(), args[0].clone())
                })?;
                let chars: Vec<char> = string.text.chars().collect();
                let len = chars.len() as i64;
                let from = normalize_string_index(args.get(1), 0, len)? as usize;
                let to = normalize_string_index(args.get(2), len, len)? as usize;
                let props = if name == "substring-no-properties" {
                    Vec::new()
                } else {
                    // Fsubstring copies through copy_text_properties.
                    copied_string_props(&slice_string_props(&string.props, from, to), 0)
                };
                let text = chars[from..to].iter().collect();
                if matches!(args[0], Value::StringObject(_)) {
                    Ok(make_shared_string_value_with_multibyte(
                        text,
                        props,
                        string.multibyte,
                    ))
                } else {
                    Ok(string_like_value(text, props))
                }
            }
            "string-to-unibyte" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                if !string.multibyte {
                    return Ok(string_like_value_with_multibyte(
                        string.text,
                        string.props,
                        false,
                    ));
                }
                let mut text = String::new();
                for ch in string.text.chars() {
                    if let Some(byte) = raw_case_byte(ch as u32) {
                        text.push(raw_byte_regex_char(byte as u8));
                    } else if (ch as u32) <= 0x7F {
                        text.push(ch);
                    } else {
                        return Err(LispError::Signal("Character cannot be encoded".into()));
                    }
                }
                Ok(string_like_value_with_multibyte(text, string.props, false))
            }
            "string-to-multibyte" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                Ok(make_shared_string_value_with_multibyte(
                    string.text,
                    string.props,
                    true,
                ))
            }
            "string-make-multibyte" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                // character.c's unibyte_char_to_multibyte under the
                // harness's unibyte environment: a non-ASCII byte becomes
                // an eight-bit character ((string-make-multibyte "\300")
                // is the raw byte 4194240), not a latin-1 character.
                let bytes = encode_raw_text_bytes(&string.text)?;
                let text = bytes
                    .iter()
                    .map(|&byte| {
                        if byte <= 0x7F {
                            char::from(byte)
                        } else {
                            raw_byte_regex_char(byte)
                        }
                    })
                    .collect::<String>();
                let multibyte = text.chars().any(|ch| (ch as u32) > 0x7F);
                Ok(if string.props.is_empty() {
                    if multibyte {
                        make_shared_string_value_with_multibyte(text, Vec::new(), true)
                    } else {
                        Value::String(text.into())
                    }
                } else {
                    make_shared_string_value_with_multibyte(text, string.props, multibyte)
                })
            }
            "string-as-multibyte" => {
                // character.c str_as_multibyte: reinterpret the unibyte
                // string's bytes as the internal (UTF-8) encoding -- valid
                // sequences become their characters ((195 128) reads back
                // as U+00C0), stray bytes stay eight-bit characters.  What
                // stood here pushed RAW_BYTE8_BASE + byte into a char,
                // which is beyond char::MAX and panicked on any 8-bit byte.
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                if string.multibyte {
                    return Ok(string_like_value_with_multibyte(
                        string.text,
                        string.props,
                        true,
                    ));
                }
                let bytes = encode_raw_text_bytes(&string.text)?;
                Ok(make_shared_string_value_with_multibyte(
                    decode_utf8_bytes(&bytes),
                    string.props,
                    true,
                ))
            }
            "string-make-unibyte" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                if !string.multibyte {
                    return Ok(string_like_value_with_multibyte(
                        string.text,
                        string.props,
                        false,
                    ));
                }
                // charset.c CHAR_TO_BYTE8's fallback: a character with no
                // unibyte equivalent contributes its low byte (the oracle:
                // (string-make-unibyte (string 12354)) is "\102").
                let bytes = string
                    .text
                    .chars()
                    .map(|ch| raw_byte_from_regex_char(ch).unwrap_or((ch as u32 & 0xFF) as u8))
                    .collect::<Vec<u8>>();
                Ok(bytes_to_shared_unibyte_value(&bytes))
            }
            "string-as-unibyte" => {
                // character.c str_as_unibyte: the string's INTERNAL bytes.
                // A character contributes its UTF-8 encoding ("\300" reads
                // as (195 128)), an eight-bit character its single byte.
                // What stood here used the latin-1 byte for U+0080..U+00FF
                // and signalled beyond, which is `string-make-unibyte's
                // business, not this primitive's.
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                if !string.multibyte {
                    return Ok(string_like_value_with_multibyte(
                        string.text,
                        string.props,
                        false,
                    ));
                }
                let mut bytes = Vec::new();
                for ch in string.text.chars() {
                    if let Some(byte) = raw_byte_from_regex_char(ch) {
                        bytes.push(byte);
                    } else {
                        let mut buffer = [0u8; 4];
                        bytes.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
                    }
                }
                Ok(bytes_to_unibyte_value(&bytes))
            }
            "unibyte-string" => {
                let bytes = args
                    .iter()
                    .map(|value| {
                        let byte = value.as_integer()?;
                        u8::try_from(byte).map_err(|_| LispError::Signal("Invalid byte".into()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(bytes_to_unibyte_value(&bytes))
            }
            "multibyte-char-to-unibyte" => {
                need_args(name, args, 1)?;
                let ch = args[0].as_integer()?;
                Ok(if (0..=255).contains(&ch) {
                    Value::Integer(ch)
                } else if (RAW_BYTE8_BASE as i64..=RAW_BYTE8_BASE as i64 + 0xFF).contains(&ch) {
                    Value::Integer(ch - RAW_BYTE8_BASE as i64)
                } else {
                    Value::Integer(-1)
                })
            }
            "string-to-number" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let s = string_text(&args[0])?;
                let base = match args.get(1) {
                    None | Some(Value::Nil) => None,
                    Some(value) => Some(value.as_integer()?),
                };
                parse_string_to_number_value(&s, base)
            }
            "number-to-string" => {
                need_args(name, args, 1)?;
                Ok(Value::String(number_to_string(&args[0])?.into()))
            }
            "format" | "format-message" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                let fmt_value = &args[0];
                let fmt = string_text(fmt_value)?;
                // editfns.c styled_format: a format string that is exactly
                // "%s" without text properties returns a string argument
                // itself (`(eq s (format "%s" s))' is t in the oracle), so
                // the argument's properties come through untouched.
                if fmt == "%s"
                    && string_like(fmt_value).is_some_and(|string| string.props.is_empty())
                    && let Some(arg) = args.get(1)
                    && string_like(arg).is_some()
                {
                    return Ok(arg.clone());
                }
                let mut result = String::new();
                let mut result_props = Vec::new();
                let mut result_multibyte = string_like(fmt_value).is_some_and(|s| s.multibyte);
                let mut arg_idx = 1;
                let chars: Vec<char> = fmt.chars().collect();
                let quoting_style =
                    (name == "format-message").then(|| effective_text_quoting_style(interp, env));
                let mut i = 0;
                while i < chars.len() {
                    if chars[i] != '%' || i + 1 >= chars.len() {
                        let start = result.chars().count();
                        let literal = match (quoting_style, chars[i]) {
                            (Some("curve"), '`') => '‘',
                            (Some("curve"), '\'') => '’',
                            (Some("straight"), '`') => '\'',
                            _ => chars[i],
                        };
                        result.push(literal);
                        if let Some(props) = format_source_props(fmt_value, i, i + 1) {
                            result_props.push(TextPropertySpan {
                                start,
                                end: start + 1,
                                props,
                            });
                        }
                        i += 1;
                        continue;
                    }
                    let spec_start = i;
                    i += 1; // skip '%'

                    if chars[i] == '%' {
                        let start = result.chars().count();
                        result.push('%');
                        if let Some(props) = format_source_props(fmt_value, spec_start, i + 1) {
                            result_props.push(TextPropertySpan {
                                start,
                                end: start + 1,
                                props,
                            });
                        }
                        i += 1;
                        continue;
                    }

                    // Parse optional N$ positional arg
                    let mut positional: Option<usize> = None;
                    if chars[i].is_ascii_digit() {
                        let mut n = 0u64;
                        let digit_start = i;
                        while i < chars.len() && chars[i].is_ascii_digit() {
                            n = n
                                .saturating_mul(10)
                                .saturating_add(chars[i] as u64 - '0' as u64);
                            i += 1;
                        }
                        if i < chars.len() && chars[i] == '$' {
                            if n == 0 || n > args.len() as u64 {
                                return Err(LispError::Signal(
                                    "Not enough arguments for format string".into(),
                                ));
                            }
                            positional = Some(n as usize);
                            i += 1;
                        } else {
                            i = digit_start; // not positional, rewind
                        }
                    } else if chars[i] == '$' {
                        return Err(LispError::Signal("Invalid format operation %$".into()));
                    } else if chars[i] == '-' {
                        // Check for %-N$s which is invalid
                        let save = i;
                        i += 1;
                        let mut has_digits = false;
                        while i < chars.len() && chars[i].is_ascii_digit() {
                            has_digits = true;
                            i += 1;
                        }
                        if has_digits && i < chars.len() && chars[i] == '$' {
                            return Err(LispError::Signal("Invalid format operation %$".into()));
                        }
                        i = save; // rewind, handle as flag below
                    }

                    // Parse flags
                    let mut flag_hash = false;
                    let mut flag_zero = false;
                    let mut flag_minus = false;
                    let mut flag_plus = false;
                    let mut flag_space = false;
                    while i < chars.len() {
                        match chars[i] {
                            '#' => flag_hash = true,
                            '0' => flag_zero = true,
                            '-' => flag_minus = true,
                            '+' => flag_plus = true,
                            ' ' => flag_space = true,
                            _ => break,
                        }
                        i += 1;
                    }

                    // Parse width
                    let mut width: usize = 0;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        width = width * 10 + (chars[i] as usize - '0' as usize);
                        i += 1;
                    }

                    let mut precision = None;
                    if i < chars.len() && chars[i] == '.' {
                        i += 1;
                        let mut parsed_precision = 0usize;
                        let mut saw_precision = false;
                        while i < chars.len() && chars[i].is_ascii_digit() {
                            parsed_precision =
                                parsed_precision * 10 + (chars[i] as usize - '0' as usize);
                            saw_precision = true;
                            i += 1;
                        }
                        precision = Some(if saw_precision { parsed_precision } else { 0 });
                    }

                    if i >= chars.len() {
                        break;
                    }

                    let conv = chars[i];
                    i += 1;

                    // Get the argument
                    let aidx = if let Some(n) = positional {
                        n
                    } else {
                        let idx = arg_idx;
                        arg_idx += 1;
                        idx
                    };
                    if aidx >= args.len() {
                        return Err(LispError::Signal(
                            "Not enough arguments for format string".into(),
                        ));
                    }
                    let arg = &args[aidx];

                    result_multibyte |= match conv {
                        's' => string_like(arg).is_some_and(|string| string.multibyte),
                        'c' => arg.as_integer().is_ok_and(|code| code > 0x7F),
                        _ => false,
                    };

                    let (mut formatted, mut formatted_props) = match conv {
                        's' => format_s_conversion(interp, arg, precision, env)?,
                        'S' => (render_prin1_ephemeral(interp, arg, env)?, Vec::new()),
                        // GNU accepts %i as a synonym for %d (erc-backend's
                        // define-erc-response-handler formats "%03i").
                        'd' | 'i' | 'o' | 'x' | 'X' | 'b' | 'B' => (
                            format_numeric_conversion(
                                interp,
                                arg,
                                if conv == 'i' { 'd' } else { conv },
                                flag_hash,
                                flag_plus,
                                flag_space,
                                precision,
                            )?,
                            Vec::new(),
                        ),
                        'f' => (
                            format_float_conversion(interp, arg, flag_plus, flag_space, precision)?,
                            Vec::new(),
                        ),
                        'e' | 'g' => (
                            format_exponential_conversion(
                                interp, arg, conv, flag_plus, flag_space, precision,
                            )?,
                            Vec::new(),
                        ),
                        'c' => (format_char_conversion(arg)?, Vec::new()),
                        _ => {
                            // Unknown conversion, pass through
                            if let Some(pos) = positional {
                                (format!("%{}${}", pos, conv), Vec::new())
                            } else {
                                (format!("%{}", conv), Vec::new())
                            }
                        }
                    };

                    // Apply width/padding
                    let formatted_len = formatted.chars().count();
                    if width > 0 && formatted_len < width {
                        let padding = width - formatted_len;
                        if flag_minus {
                            // Left-align: content then spaces
                            if formatted_len > 0 {
                                let trailing_props =
                                    props_at_string_offset(&formatted_props, formatted_len - 1);
                                if !trailing_props.is_empty() {
                                    formatted_props.push(TextPropertySpan {
                                        start: formatted_len,
                                        end: formatted_len + padding,
                                        props: trailing_props,
                                    });
                                }
                            }
                            formatted.push_str(&" ".repeat(padding));
                        } else if flag_zero && !flag_minus {
                            // Zero-pad: put zeros after sign/prefix, before digits
                            // Find the split point: sign + prefix
                            let s = &formatted;
                            let mut prefix_end = 0;
                            if s.starts_with('-') || s.starts_with('+') {
                                prefix_end = 1;
                            }
                            if s[prefix_end..].starts_with("0x")
                                || s[prefix_end..].starts_with("0X")
                                || s[prefix_end..].starts_with("0b")
                                || s[prefix_end..].starts_with("0B")
                            {
                                prefix_end += 2;
                            }
                            formatted = format!(
                                "{}{}{}",
                                &s[..prefix_end],
                                "0".repeat(padding),
                                &s[prefix_end..]
                            );
                        } else {
                            formatted = format!("{}{}", " ".repeat(padding), formatted);
                            formatted_props = shift_string_props(&formatted_props, padding);
                        }
                    }
                    if let Some(props) = format_source_props(fmt_value, spec_start, i) {
                        formatted_props.push(TextPropertySpan {
                            start: 0,
                            end: formatted.chars().count(),
                            props,
                        });
                    }
                    let start = result.chars().count();
                    result.push_str(&formatted);
                    result_props.extend(shift_string_props(
                        &crate::lisp::primitives::strings::flatten_overlapping_string_props(
                            formatted_props,
                        ),
                        start,
                    ));
                }
                result_multibyte |= result
                    .chars()
                    .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
                Ok(string_like_value_with_multibyte(
                    result,
                    merge_string_props(result_props),
                    result_multibyte,
                ))
            }
            "format-network-address" => {
                need_arg_range(name, args, 1, 2)?;
                let items = sequence_values(interp, &args[0])?;
                if items.len() < 4 {
                    return Err(LispError::Signal("Invalid network address".into()));
                }
                let octets: Result<Vec<String>, LispError> = items
                    .iter()
                    .take(4)
                    .map(|item| Ok(item.as_integer()?.to_string()))
                    .collect();
                let mut address = octets?.join(".");
                if !args.get(1).is_some_and(Value::is_truthy)
                    && let Some(port) = items.get(4)
                {
                    address.push(':');
                    address.push_str(&port.as_integer()?.to_string());
                }
                Ok(Value::String(address.into()))
            }
            "ngettext" => {
                need_args(name, args, 3)?;
                let singular = string_text(&args[0])?;
                let plural = string_text(&args[1])?;
                let count = args[2].as_integer()?;
                Ok(Value::String(
                    (if count == 1 { singular } else { plural }).into(),
                ))
            }
            "char-to-string" => {
                need_args(name, args, 1)?;
                let n = args[0].as_integer()?;
                let c = char_for_codepoint(n)?;
                Ok(string_like_value_with_multibyte(
                    c.to_string(),
                    Vec::new(),
                    n > 0x7F,
                ))
            }
            "base64-encode-region" => {
                need_arg_range(name, args, 2, 3)?;
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                let no_line_break = args.get(2).is_some_and(Value::is_truthy);
                base64_encode_region_value(interp, start, end, !no_line_break, true, false)
            }
            "base64url-encode-region" => {
                need_arg_range(name, args, 2, 3)?;
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                let no_pad = args.get(2).is_some_and(Value::is_truthy);
                base64_encode_region_value(interp, start, end, false, !no_pad, true)
            }
            "base64-encode-string" => {
                need_arg_range(name, args, 1, 2)?;
                let no_line_break = args.get(1).is_some_and(Value::is_truthy);
                base64_encode_string_value(&args[0], !no_line_break, true, false)
            }
            "base64url-encode-string" => {
                need_arg_range(name, args, 1, 2)?;
                let no_pad = args.get(1).is_some_and(Value::is_truthy);
                base64_encode_string_value(&args[0], false, !no_pad, true)
            }
            "base64-decode-region" => {
                need_arg_range(name, args, 2, 4)?;
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                let base64url = args.get(2).is_some_and(Value::is_truthy);
                let ignore_invalid = args.get(3).is_some_and(Value::is_truthy);
                base64_decode_region_value(interp, start, end, base64url, ignore_invalid)
            }
            "base64-decode-string" => {
                need_arg_range(name, args, 1, 3)?;
                let base64url = args.get(1).is_some_and(Value::is_truthy);
                let ignore_invalid = args.get(2).is_some_and(Value::is_truthy);
                decode_base64_string_value(&args[0], base64url, ignore_invalid)
            }
            "byte-to-string" => {
                need_args(name, args, 1)?;
                let n = args[0].as_integer()?;
                if !(0..=255).contains(&n) {
                    return Err(LispError::Signal("Invalid byte".into()));
                }
                Ok(bytes_to_unibyte_value(&[n as u8]))
            }
            "make-char" => {
                need_arg_range(name, args, 1, 2)?;
                let _charset = args[0].as_symbol()?;
                let code = args.get(1).map(Value::as_integer).transpose()?.unwrap_or(0);
                Ok(Value::Integer(code))
            }
            "string-to-char" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                Ok(string
                    .text
                    .chars()
                    .next()
                    .map(|ch| string_sequence_value(&string, ch))
                    .unwrap_or(Value::Integer(0)))
            }
            "char-syntax" => {
                need_args(name, args, 1)?;
                let mut code = u32::try_from(args[0].as_integer()?)
                    .map_err(|_| LispError::Signal("Invalid character".into()))?;
                // GNU syntax.c promotes a raw byte to its byte8 character
                // before consulting the current table in a unibyte buffer.
                // This keeps entries installed with
                // `unibyte-char-to-multibyte' visible to `char-syntax'.
                if !interp.buffer.is_multibyte() && (0x80..=0xFF).contains(&code) {
                    code += RAW_BYTE8_BASE;
                }
                let class =
                    syntax::syntax_entry_for_code(interp, interp.current_syntax_table_id(), code)
                        .class;
                Ok(Value::Integer(syntax::syntax_class_char(class) as i64))
            }
            "string-to-syntax" => {
                need_args(name, args, 1)?;
                let spec = string_text(&args[0])?;
                let entry = syntax::parse_syntax_spec(&spec).ok_or_else(|| {
                    let letter = spec.chars().next().unwrap_or('\0');
                    LispError::Signal(format!("Invalid syntax description letter: {letter}"))
                })?;
                Ok(syntax::syntax_entry_value(entry))
            }
            "internal-describe-syntax-value" => {
                need_args(name, args, 1)?;
                let (mut description, prefix) = syntax::describe_syntax_value(&args[0]);
                if prefix {
                    // GNU syntax.c: insert1 (call1 (Qsubstitute_command_keys,
                    // prefixdoc)) — a funcall through the symbol's cell, which
                    // reaches help.el's Lisp owner.
                    let suffix = interp.call_function_value(
                        Value::symbol("substitute-command-keys"),
                        Some("substitute-command-keys"),
                        &[Value::String(
                            ",\n\t  is a prefix character for `backward-prefix-chars'".into(),
                        )],
                        env,
                    )?;
                    description.push_str(&string_text(&suffix)?);
                }
                interp.insert_current_buffer(&description);
                Ok(args[0].clone())
            }
            "syntax-class-to-char" => {
                need_args(name, args, 1)?;
                let class = match args[0].as_integer()? {
                    0 => syntax::SyntaxClass::Whitespace,
                    1 => syntax::SyntaxClass::Punctuation,
                    2 => syntax::SyntaxClass::Word,
                    3 => syntax::SyntaxClass::Symbol,
                    4 => syntax::SyntaxClass::OpenParen,
                    5 => syntax::SyntaxClass::CloseParen,
                    6 => syntax::SyntaxClass::Quote,
                    7 => syntax::SyntaxClass::StringQuote,
                    8 => syntax::SyntaxClass::PairedDelimiter,
                    9 => syntax::SyntaxClass::Escape,
                    10 => syntax::SyntaxClass::CharQuote,
                    11 => syntax::SyntaxClass::CommentStart,
                    12 => syntax::SyntaxClass::CommentEnd,
                    13 => syntax::SyntaxClass::Inherit,
                    14 => syntax::SyntaxClass::GenericCommentDelimiter,
                    15 => syntax::SyntaxClass::GenericStringDelimiter,
                    _ => return Err(LispError::Signal("Invalid syntax class".into())),
                };
                Ok(Value::Integer(syntax::syntax_class_char(class) as i64))
            }
            "string-bytes" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
                Ok(Value::Integer(string.byte_len()? as i64))
            }
            "multibyte-string-p" => {
                need_args(name, args, 1)?;
                let Some(string) = string_like(&args[0]) else {
                    return Ok(Value::Nil);
                };
                Ok(if string.multibyte {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "unibyte-char-to-multibyte" => {
                need_args(name, args, 1)?;
                let n = args[0].as_integer()?;
                if !(0..=255).contains(&n) {
                    return Err(LispError::Signal("Byte value out of range".into()));
                }
                // GNU maps bytes 0x80..0xFF to the raw-byte (eight-bit)
                // codepoints at #x3FFF00; ASCII stays as-is.
                Ok(Value::Integer(if n >= 0x80 { 0x3FFF00 + n } else { n }))
            }
            "upcase" => {
                need_args(name, args, 1)?;
                casify_value(interp, &args[0], CaseAction::Up, env)
            }
            "downcase" => {
                need_args(name, args, 1)?;
                casify_value(interp, &args[0], CaseAction::Down, env)
            }
            "capitalize" => {
                need_args(name, args, 1)?;
                casify_value(interp, &args[0], CaseAction::Capitalize, env)
            }
            "upcase-initials" => {
                need_args(name, args, 1)?;
                casify_value(interp, &args[0], CaseAction::UpcaseInitials, env)
            }
            "unicode-property-table-internal" => {
                need_args(name, args, 1)?;
                let property = args[0].as_symbol()?;
                Ok(registered_unicode_property(interp, property, env)?.unwrap_or(Value::Nil))
            }
            "get-unicode-property-internal" => {
                need_args(name, args, 2)?;
                let Value::CharTable(table_id) = args[0] else {
                    return Err(LispError::WrongTypeArgument(
                        "char-table-p".into(),
                        args[0].clone(),
                    ));
                };
                if interp.char_table_purpose(table_id) != Some("char-code-property-table") {
                    return Err(LispError::Signal("Invalid Unicode property table".into()));
                }
                let character = unicode_property_character(&args[1])?;
                let value = interp
                    .char_table_get(table_id, character)
                    .unwrap_or(Value::Nil);
                decode_unicode_property_value(interp, table_id, value)
            }
            "put-unicode-property-internal" => {
                need_args(name, args, 3)?;
                let Value::CharTable(table_id) = args[0] else {
                    return Err(LispError::WrongTypeArgument(
                        "char-table-p".into(),
                        args[0].clone(),
                    ));
                };
                if interp.char_table_purpose(table_id) != Some("char-code-property-table") {
                    return Err(LispError::Signal("Invalid Unicode property table".into()));
                }
                let character = unicode_property_character(&args[1])?;
                let encoded = encode_unicode_property_value(interp, table_id, &args[2])?;
                interp.char_table_set(table_id, character, encoded)?;
                Ok(Value::Nil)
            }
            "char-resolve-modifiers" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(resolve_char_modifiers(
                    args[0].as_integer()?,
                )))
            }
        }
    }
);

/// casefiddle.c's `uniprop_table (Qtitlecase)' and friends: resolve a Unicode
/// property char-table, loading its generated `uni-*.el' owner on demand.
/// An unavailable property yields None exactly as GNU's nil table does, and
/// the caller then falls back to the case table.
pub(crate) fn uniprop_table_id(
    interp: &mut Interpreter,
    property: &str,
    env: &mut Env,
) -> Option<u64> {
    match registered_unicode_property(interp, property, env).ok()?? {
        Value::CharTable(table_id) => Some(table_id),
        _ => None,
    }
}

/// CHAR_TABLE_REF over a Unicode property table, decoding the compressed
/// representation the generated `uni-*.el' tables use.
pub(crate) fn uniprop_table_ref(interp: &Interpreter, table_id: u64, code: u32) -> Option<Value> {
    let raw = interp.char_table_get(table_id, code)?;
    let decoded = decode_unicode_property_value(interp, table_id, raw).ok()?;
    (!decoded.is_nil()).then_some(decoded)
}

fn registered_unicode_property(
    interp: &mut Interpreter,
    property: &str,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let Some(mut registered) = find_registered_unicode_property(interp, property, env) else {
        return Ok(None);
    };
    let filename = match &registered {
        Value::String(_) | Value::StringObject(_) => string_text(&registered)?,
        _ => return Ok(Some(registered)),
    };
    let target = format!("international/{filename}");
    let Some(path) = resolve_load_target_in_env(interp, &target, env)
        .or_else(|| resolve_load_target_in_env(interp, &filename, env))
    else {
        return Ok(Some(registered));
    };
    crate::lisp::load_file_strict(interp, &path)?;
    registered = find_registered_unicode_property(interp, property, env)
        .unwrap_or(Value::String(filename.into()));
    Ok(Some(registered))
}

fn find_registered_unicode_property(
    interp: &Interpreter,
    property: &str,
    env: &Env,
) -> Option<Value> {
    let mut alist = interp.lookup_var("char-code-property-alist", env)?;
    loop {
        let (entry, rest) = alist.cons_values()?;
        if let Some((key, value)) = entry.cons_values()
            && key.as_symbol().ok() == Some(property)
        {
            return Some(value);
        }
        alist = rest;
        if alist.is_nil() {
            return None;
        }
    }
}

fn unicode_property_character(value: &Value) -> Result<u32, LispError> {
    match value {
        Value::Integer(character) if (0..=0x3f_ffff).contains(character) => Ok(*character as u32),
        _ => Err(wrong_type_argument("characterp", value.clone())),
    }
}

pub(crate) fn decode_unicode_property_value(
    interp: &Interpreter,
    table_id: u64,
    value: Value,
) -> Result<Value, LispError> {
    if interp.char_table_extra_slot(table_id, 1) != Some(Value::Integer(0)) {
        return Ok(value);
    }
    let index = usize::try_from(value.as_integer()?)
        .map_err(|_| LispError::Signal("Invalid Unicode property value".into()))?;
    let Some(vector) = interp.char_table_extra_slot(table_id, 4) else {
        return Ok(value);
    };
    let items = vector.to_vec()?;
    let values = if matches!(
        items.first(),
        Some(Value::Symbol(symbol)) if symbol == "vector-literal"
    ) {
        &items[1..]
    } else {
        &items
    };
    Ok(values.get(index).cloned().unwrap_or(value))
}

fn unicode_property_vector_values(value: &Value) -> Result<Vec<Value>, LispError> {
    let items = value.to_vec()?;
    Ok(
        if matches!(
            items.first(),
            Some(Value::Symbol(symbol)) if symbol == "vector-literal"
        ) {
            items[1..].to_vec()
        } else {
            items
        },
    )
}

fn encode_unicode_property_value(
    interp: &mut Interpreter,
    table_id: u64,
    value: &Value,
) -> Result<Value, LispError> {
    let Some(Value::Integer(encoder)) = interp.char_table_extra_slot(table_id, 2) else {
        return Ok(value.clone());
    };
    match encoder {
        0 => {
            if value.is_nil()
                || matches!(value, Value::Integer(character) if (0..=0x3f_ffff).contains(character))
            {
                Ok(value.clone())
            } else {
                Err(wrong_type_argument("integerp", value.clone()))
            }
        }
        1 | 2 => {
            if encoder == 2 && !matches!(value, Value::Integer(_)) {
                return Err(wrong_type_argument("fixnump", value.clone()));
            }
            let vector = interp
                .char_table_extra_slot(table_id, 4)
                .ok_or_else(|| LispError::Signal("Invalid Unicode property table".into()))?;
            let mut values = unicode_property_vector_values(&vector)?;
            let index = values
                .iter()
                .position(|candidate| values_eql(candidate, value));
            let index = match index {
                Some(index) => index,
                None if encoder == 2 => {
                    let index = values.len();
                    // GNU's numeric encoder extends the decoder vector with
                    // the newly allocated encoded index, not with the input
                    // number itself (chartab.c:uniprop_encode_value_numeric).
                    values.push(Value::Integer(index as i64));
                    let mut public_vector = vec![Value::Symbol("vector-literal".into())];
                    public_vector.extend(values);
                    interp.set_char_table_extra_slot(table_id, 4, Value::list(public_vector))?;
                    index
                }
                None => {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::String("Unicode property value".into()),
                        value.clone(),
                    ])));
                }
            };
            Ok(Value::Integer(index as i64))
        }
        _ => Ok(value.clone()),
    }
}
