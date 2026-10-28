use super::*;
use crate::lisp::types::CharTableRef;
use crate::lisp::types::Kind;
use crate::lisp::types::string_data::character_code;

/// BYTE as a multibyte string's bytes: itself when ASCII, else the
/// two-byte form of its eight-bit character (BYTE8_STRING).
fn push_byte_as_multibyte(bytes: &mut Vec<u8>, byte: u8) {
    if byte < 0x80 {
        bytes.push(byte);
    } else {
        bytes.push(0xC0 | ((byte >> 6) & 1));
        bytes.push(0x80 | (byte & 0x3F));
    }
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
                need_arg_range(name, args, 2, 3)?;
                // GNU checks LENGTH before INIT, including at length zero.
                let length = match args[0].kind() {
                    Kind::Integer(length) if length >= 0 => length as usize,
                    _ => return Err(wrong_type_argument("wholenump", args[0])),
                };
                let init = character_code(args[1])?;
                Ok(Value::StringObject(
                    StringObjectRef::repeated_character(
                        init,
                        length,
                        args.get(2).is_some_and(Value::is_truthy),
                    )
                    .map_err(|error| string_storage_error(interp, env, error))?,
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
                Ok(Value::vector(std::iter::repeat_n(args[1], length as usize)))
            }
            // alloc.c:Fmake_bool_vector: CHECK_FIXNAT, then the bits filled.
            "make-bool-vector" => {
                need_args(name, args, 2)?;
                let Kind::Integer(length @ 0..) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("wholenump".into(), args[0]));
                };
                Ok(Value::BoolVector(
                    crate::lisp::types::BoolVectorRef::filled(length as usize, args[1].is_truthy()),
                ))
            }
            "bool-vector" => Ok(make_bool_vector_value(args.iter().map(Value::is_truthy))),
            "make-keymap" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(make_runtime_full_keymap(
                    interp,
                    args.first().copied().unwrap_or(Value::Nil),
                ))
            }
            "make-sparse-keymap" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(make_runtime_keymap(
                    interp,
                    args.first().copied().unwrap_or(Value::Nil),
                ))
            }
            "vconcat" => {
                let mut items = Vec::new();
                for value in args {
                    items.extend(sequence_values(value)?);
                }
                Ok(Value::vector(items))
            }
            "copy-keymap" => {
                need_args(name, args, 1)?;
                // keymap.c:Fcopy_keymap copies own entries and shares the parent.
                crate::lisp::primitives::keys::copy_keymap_value(interp, &args[0], env)
            }
            "record" => {
                need_args(name, args, 1)?;
                check_record_data_slots(args.len() - 1)?;
                Ok(Value::LispRecord(crate::lisp::types::LispRecordRef::new(
                    args[0],
                    &args[1..],
                )))
            }
            "make-record" => {
                need_args(name, args, 3)?;
                // alloc.c:Fmake_record uses CHECK_FIXNAT (wholenump),
                // before either checking the record limit or allocating.
                let length = match args[1].kind() {
                    Kind::Integer(length) if length >= 0 => length as usize,
                    _ => return Err(LispError::WrongTypeArgument("wholenump".into(), args[1])),
                };
                check_record_data_slots(length)?;
                Ok(Value::LispRecord(
                    crate::lisp::types::LispRecordRef::filled(args[0], length, args[2]),
                ))
            }
            "make-finalizer" => {
                need_args(name, args, 1)?;
                // alloc.c:Fmake_finalizer: CHECK_TYPE (FUNCTIONP (function),
                // Qfunctionp, function), then the object joins `finalizers'.
                if !function_value_p(interp, &args[0], env) {
                    return Err(LispError::WrongTypeArgument("functionp".into(), args[0]));
                }
                Ok(interp.make_finalizer(args[0]))
            }

            // ── String operations ──
            "concat" => {
                let result = StringObjectRef::concatenate(args)
                    .map_err(|error| string_storage_error(interp, env, error))?;
                Ok(Value::StringObject(result))
            }
            "string-match" => regexp::string_match_impl(interp, args, env, true),

            "posix-string-match" => regexp::posix_string_match_impl(interp, args, env),
            "string-width" => {
                // character.c Fstring_width and lisp_string_width: each
                // character's char_width with the buffer's display table; a
                // string whose bytes are its characters is read bytewise.
                need_arg_range(name, args, 1, 3)?;
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let length = state.borrow().len();
                let (from, to) = validate_subarray(
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                    length,
                )?;
                let codes = {
                    let state = state.borrow();
                    let bytewise = state.bytes().len() == state.len();
                    let mut offset = state.byte_offset(from).unwrap_or(0);
                    let mut codes = Vec::with_capacity(to - from);
                    for _ in from..to {
                        if bytewise {
                            codes.push(u32::from(state.bytes()[offset]));
                            offset += 1;
                        } else if let Some(code) = state.character_advance(&mut offset) {
                            codes.push(code as u32);
                        }
                    }
                    codes
                };
                let dp = buffer_display_table(interp, env);
                let width: usize = codes
                    .into_iter()
                    .map(|c| display_char_width(interp, c, dp, env))
                    .sum();
                Ok(Value::Integer(width as i64))
            }
            "char-width" => {
                need_args(name, args, 1)?;
                let c = match args[0].kind() {
                    Kind::Integer(c) if (0..=MAX_CHAR as i64).contains(&c) => c as u32,
                    _ => return Err(LispError::WrongTypeArgument("characterp".into(), args[0])),
                };
                let dp = buffer_display_table(interp, env);
                Ok(Value::Integer(display_char_width(interp, c, dp, env) as i64))
            }
            "string" => Ok(Value::StringObject(
                StringObjectRef::from_characters(args)
                    .map_err(|error| string_storage_error(interp, env, error))?,
            )),
            "substring" | "substring-no-properties" => {
                need_arg_range(name, args, 1, 3)?;
                substring_value(
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                    name == "substring",
                )
            }
            "string-to-unibyte" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?;
                // fns.c:Fstring_to_unibyte: a unibyte STRING itself, else a
                // new string without properties of its ASCII and eight-bit
                // characters' bytes.
                if !string.multibyte {
                    return Ok(args[0]);
                }
                let mut bytes = Vec::new();
                for (index, code) in string.character_codes_iter().enumerate() {
                    if code <= 0x7F {
                        bytes.push(code as u8);
                    } else if (0x3F_FF80..=0x3F_FFFF).contains(&code) {
                        bytes.push((code - 0x3F_FF00) as u8);
                    } else {
                        return Err(LispError::Signal(format!(
                            "Cannot convert character at index {index} to unibyte"
                        )));
                    }
                }
                Ok(bytes_to_unibyte_value(&bytes))
            }
            "string-to-multibyte" => {
                // fns.c string_to_multibyte: STRING itself when multibyte,
                // else a new string of its bytes, each non-ASCII byte an
                // eight-bit character, without properties.
                need_args(name, args, 1)?;
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let state = state.borrow();
                if state.is_multibyte() {
                    return Ok(args[0]);
                }
                let mut bytes = Vec::with_capacity(state.bytes().len());
                for &byte in state.bytes() {
                    push_byte_as_multibyte(&mut bytes, byte);
                }
                StringObjectRef::from_storage(bytes, state.len(), true)
                    .map(Value::StringObject)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "string-make-multibyte" => {
                need_args(name, args, 1)?;
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?;
                // fns.c:Fstring_make_multibyte preserves the original
                // object and properties for multibyte or all-ASCII input.
                if string.multibyte || string.text.is_ascii() {
                    return Ok(args[0]);
                }
                let bytes = encode_raw_text_bytes(&string.text)?;
                let text = bytes
                    .into_iter()
                    .map(|byte| {
                        if byte <= 0x7F {
                            char::from(byte)
                        } else {
                            raw_byte_regex_char(byte)
                        }
                    })
                    .collect();
                // Converting actual non-ASCII bytes allocates a new string
                // with no intervals, as GNU's make_uninit_multibyte_string.
                Ok(make_shared_string_value_with_multibyte(
                    text,
                    Vec::new(),
                    true,
                ))
            }
            "string-as-multibyte" => {
                // fns.c Fstring_as_multibyte: STRING itself when multibyte,
                // else its bytes read as the internal encoding
                // (parse_str_as_multibyte), each byte outside a sequence an
                // eight-bit character, without properties.
                need_args(name, args, 1)?;
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let state = state.borrow();
                if state.is_multibyte() {
                    return Ok(args[0]);
                }
                let source = state.bytes();
                let mut bytes = Vec::with_capacity(source.len());
                let mut characters = 0;
                let mut offset = 0;
                while offset < source.len() {
                    let width =
                        crate::lisp::types::string_data::multibyte_length(&source[offset..], true);
                    if width > 0 {
                        bytes.extend_from_slice(&source[offset..offset + width]);
                        offset += width;
                    } else {
                        push_byte_as_multibyte(&mut bytes, source[offset]);
                        offset += 1;
                    }
                    characters += 1;
                }
                StringObjectRef::from_storage(bytes, characters, true)
                    .map(Value::StringObject)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "string-make-unibyte" => {
                // fns.c string_make_unibyte: STRING itself when unibyte,
                // else each character's CHAR_TO_BYTE8 (an eight-bit
                // character's byte, another's low eight bits), without
                // properties.
                need_args(name, args, 1)?;
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let state = state.borrow();
                if !state.is_multibyte() {
                    return Ok(args[0]);
                }
                let mut bytes = Vec::with_capacity(state.len());
                let mut offset = 0;
                while let Some(code) = state.character_advance(&mut offset) {
                    let code = code as u32;
                    bytes.push(
                        if (RAW_BYTE8_BASE + 0x80..=RAW_BYTE8_BASE + 0xFF).contains(&code) {
                            (code - RAW_BYTE8_BASE) as u8
                        } else {
                            (code & 0xFF) as u8
                        },
                    );
                }
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
                if let Kind::StringObject(state) = args[0].kind() {
                    let state = state.borrow();
                    if !state.is_multibyte() {
                        return Ok(args[0]);
                    }
                    return Ok(bytes_to_unibyte_value(&state.as_unibyte_bytes()));
                }
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?;
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
                } else if (RAW_BYTE8_BASE as i64 + 0x80..=RAW_BYTE8_BASE as i64 + 0xFF)
                    .contains(&ch)
                {
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
                // CHECK_FIXNUM (base), then xsignal1 (Qargs_out_of_range,
                // base) outside 2 to 16.
                let base = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => None,
                    Some(Kind::Integer(base)) if (2..=16).contains(&base) => Some(base),
                    Some(Kind::Integer(_)) => {
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("args-out-of-range"),
                            args[1],
                        ])));
                    }
                    Some(_) => return Err(wrong_type_argument("fixnump", args[1])),
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
                // The format string is read in place: its characters are
                // what the conversion loop walks (on the stack when short,
                // an ASCII text's by its bytes), and its length in bytes the
                // result's first capacity.
                let mut chars = smallvec::SmallVec::<[char; 64]>::new();
                let fmt_bytes = with_string_text(fmt_value, |fmt| {
                    if fmt.is_ascii() {
                        chars.extend(fmt.bytes().map(char::from));
                    } else {
                        chars.extend(fmt.chars());
                    }
                    fmt.len()
                })
                .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *fmt_value))?;
                // Only a format string with text properties lends them to
                // the result.
                let fmt_has_props = !string_without_properties(fmt_value);
                // The characters the format string's text view cannot carry.
                let fmt_extended = string_extended_chars(fmt_value);
                // editfns.c styled_format: a format string that is exactly
                // "%s" without text properties returns a string argument
                // itself (`(eq s (format "%s" s))' is t in the oracle), so
                // the argument's properties come through untouched.
                if chars[..] == ['%', 's']
                    && !fmt_has_props
                    && let Some(arg) = args.get(1)
                    && matches!(arg.kind(), Kind::StringObject(_))
                {
                    return Ok(*arg);
                }
                let mut result = String::with_capacity(fmt_bytes + 16);
                // The result's length in characters, kept as it grows (a
                // count of the whole result at each piece was quadratic).
                let mut result_chars = 0usize;
                let mut result_props = Vec::new();
                let mut result_extended = Vec::new();
                let mut result_multibyte = string_is_multibyte(fmt_value) == Some(true);
                let mut arg_idx = 1;
                let quoting_style =
                    (name == "format-message").then(|| effective_text_quoting_style(interp, env));
                let mut i = 0;
                while i < chars.len() {
                    if chars[i] != '%' || i + 1 >= chars.len() {
                        let start = result_chars;
                        let literal = match (quoting_style, chars[i]) {
                            (Some("curve"), '`') => '‘',
                            (Some("curve"), '\'') => '’',
                            (Some("straight"), '`') => '\'',
                            _ => chars[i],
                        };
                        result.push(literal);
                        if let Ok(found) = fmt_extended.binary_search_by_key(&i, |(at, _)| *at) {
                            result_extended.push((start, fmt_extended[found].1));
                        }
                        result_chars += 1;
                        if fmt_has_props
                            && let Some(props) = format_source_props(fmt_value, i, i + 1)
                        {
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
                        let start = result_chars;
                        result.push('%');
                        result_chars += 1;
                        if fmt_has_props
                            && let Some(props) = format_source_props(fmt_value, spec_start, i + 1)
                        {
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
                        's' => string_is_multibyte(arg) == Some(true),
                        'c' => arg.as_integer().is_ok_and(|code| code > 0x7F),
                        _ => false,
                    };

                    let mut formatted_extended = Vec::new();
                    let (mut formatted, mut formatted_props) = match conv {
                        's' => {
                            let (text, props, extended) =
                                format_s_conversion(interp, arg, precision, env)?;
                            formatted_extended = extended;
                            (text, props)
                        }
                        'S' => {
                            let (text, extended) = render_prin1_parts_ephemeral(interp, arg, env)?;
                            formatted_extended = extended;
                            (text, Vec::new())
                        }
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
                        'c' => {
                            let (text, extended) = format_char_conversion(arg)?;
                            formatted_extended = extended;
                            (text, Vec::new())
                        }
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
                            for (at, _) in &mut formatted_extended {
                                *at += padding;
                            }
                        }
                    }
                    // Each padding adds its count of one-byte characters.
                    let formatted_chars = formatted_len.max(width);
                    if fmt_has_props
                        && let Some(props) = format_source_props(fmt_value, spec_start, i)
                    {
                        formatted_props.push(TextPropertySpan {
                            start: 0,
                            end: formatted_chars,
                            props,
                        });
                    }
                    let start = result_chars;
                    result.push_str(&formatted);
                    result_extended.extend(
                        formatted_extended
                            .into_iter()
                            .map(|(at, code)| (start + at, code)),
                    );
                    result_chars += formatted_chars;
                    result_props.extend(shift_string_props(
                        &crate::lisp::primitives::strings::flatten_overlapping_string_props(
                            formatted_props,
                        ),
                        start,
                    ));
                }
                result_multibyte |= !result_extended.is_empty()
                    || (!result.is_ascii()
                        && result
                            .chars()
                            .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F));
                Ok(string_like_value_with_extended_chars(
                    result,
                    merge_string_props(result_props),
                    result_multibyte,
                    result_extended,
                ))
            }
            "format-network-address" => {
                need_arg_range(name, args, 1, 2)?;
                let items = sequence_values(&args[0])?;
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
                Ok(Value::StringObject(
                    StringObjectRef::from_characters(args)
                        .map_err(|error| string_storage_error(interp, env, error))?,
                ))
            }
            "base64-encode-region" => {
                need_arg_range(name, args, 2, 3)?;
                // fns.c's validate_region.
                let (start, end) = validate_region(interp, &args[0], &args[1])?;
                let no_line_break = args.get(2).is_some_and(Value::is_truthy);
                base64_encode_region_value(interp, start, end, !no_line_break, true, false, env)
            }
            "base64url-encode-region" => {
                need_arg_range(name, args, 2, 3)?;
                // fns.c's validate_region.
                let (start, end) = validate_region(interp, &args[0], &args[1])?;
                let no_pad = args.get(2).is_some_and(Value::is_truthy);
                base64_encode_region_value(interp, start, end, false, !no_pad, true, env)
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
                // fns.c's validate_region.
                let (start, end) = validate_region(interp, &args[0], &args[1])?;
                let base64url = args.get(2).is_some_and(Value::is_truthy);
                let ignore_invalid = args.get(3).is_some_and(Value::is_truthy);
                base64_decode_region_value(interp, start, end, base64url, ignore_invalid, env)
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
                need_arg_range(name, args, 1, 5)?;
                make_charset_character(interp, &args[0], &args[1..])
            }
            "string-to-char" => {
                need_args(name, args, 1)?;
                // editfns.c:Fstring_to_char: CHECK_STRING, the first
                // character (a unibyte string's byte), 0 for "".
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                Ok(Value::Integer(state.borrow().character_at(0).unwrap_or(0)))
            }
            "char-syntax" => {
                need_args(name, args, 1)?;
                let mut code = u32::try_from(args[0].as_integer()?)
                    .map_err(|_| LispError::Signal("Invalid character".into()))?;
                // GNU syntax.c promotes a raw byte to its byte8 character
                // before consulting the current table in a unibyte buffer.
                // This keeps entries installed with
                // `unibyte-char-to-multibyte' visible to `char-syntax'.
                if !interp.buffer.borrow().is_multibyte() && (0x80..=0xFF).contains(&code) {
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
                Ok(syntax::syntax_entry_value(interp, entry))
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
                Ok(args[0])
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
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?;
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
                // Funicode_property_table_internal: the table, else the
                // entry's own value (a file that would not load).
                match uniprop_table_or_entry(interp, args[0], env)? {
                    Ok(table) => Ok(Value::CharTable(table)),
                    Err(Some(entry)) => Ok(entry.cdr.get()),
                    Err(None) => Ok(char_code_property_entry(interp, args[0], env)?
                        .map_or(Value::Nil, |entry| entry.cdr.get())),
                }
            }
            "get-unicode-property-internal" => {
                need_args(name, args, 2)?;
                let Kind::CharTable(table_id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                let character = unicode_property_character(&args[1])?;
                if !uniprop_table_p(table_id) {
                    return Err(LispError::Signal("Invalid Unicode property table".into()));
                }
                Ok(get_unicode_property(interp, table_id, character))
            }
            "put-unicode-property-internal" => {
                need_args(name, args, 3)?;
                let Kind::CharTable(table_id) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("char-table-p".into(), args[0]));
                };
                let character = unicode_property_character(&args[1])?;
                if !uniprop_table_p(table_id) {
                    return Err(LispError::Signal("Invalid Unicode property table".into()));
                }
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

/// casefiddle.c's `uniprop_table (Qtitlecase)' and friends: PROPERTY's
/// Unicode property char-table, its generated `uni-*.el' loaded on
/// demand; None for GNU's nil table, whereupon the caller falls back to
/// the case table.
pub(crate) fn uniprop_table_id(
    interp: &mut Interpreter,
    property: Value,
    env: &mut Env,
) -> Option<CharTableRef> {
    uniprop_table(interp, property, env).ok().flatten()
}

/// CHAR_TABLE_REF of a Unicode property table, as casefiddle.c reads
/// the special-casing and titlecase tables: the stored value, not
/// decoded; None for nil.
pub(crate) fn uniprop_table_ref(
    interp: &Interpreter,
    table_id: CharTableRef,
    code: u32,
) -> Option<Value> {
    interp
        .char_table_get(table_id, code)
        .filter(|value| !value.is_nil())
}

/// Fassq (PROP, Vchar_code_property_alist): the entry, the alist read
/// from the C-owned symbol value cell (by the symbol held here, as the C
/// global is), so a caller's same-named lexical binding is not part of
/// this state.  Fassq's own walk, which signals for a circular or dotted
/// alist.
fn char_code_property_entry(
    interp: &mut Interpreter,
    prop: Value,
    env: &mut Env,
) -> Result<Option<crate::lisp::alloc::ConsRef>, LispError> {
    let Ok(alist) = cached_symbol!("char-code-property-alist")
        .with(|symbol| interp.symbol_value_cell_symbol(symbol))
    else {
        return Ok(None);
    };
    let entry = super::lists::direct_assq_family(interp, &[prop, alist], env, "assq")?;
    Ok(entry.cons_ref())
}

/// chartab.c's UNIPROP_TABLE_P: a `char-code-property-table' with its
/// five extra slots.
fn uniprop_table_p(table: CharTableRef) -> bool {
    cached_symbol!("char-code-property-table")
        .with(|symbol| table.purpose().word() == Value::Symbol(*symbol).word())
        && table.extra_count() == 5
}

/// chartab.c:uniprop_table: PROP's Unicode property table.  An entry
/// naming a file loads `international/' FILE (save_match_data_load with
/// NOERROR, NOMESSAGE and NOSUFFIX), which stores the table in the same
/// entry.  None when PROP has no entry, the load fails, or the value is
/// not a property table whose decoder (extra slot 1) is nil or one
/// uniprop_get_decoder knows.
fn uniprop_table(
    interp: &mut Interpreter,
    prop: Value,
    env: &mut Env,
) -> Result<Option<CharTableRef>, LispError> {
    Ok(uniprop_table_or_entry(interp, prop, env)?.ok())
}

/// `uniprop_table', and where it gives nil, the entry
/// Funicode_property_table_internal's Fassq would find again: the one
/// read, unless a file was loaded (Lisp ran), when it is read anew (None).
fn uniprop_table_or_entry(
    interp: &mut Interpreter,
    prop: Value,
    env: &mut Env,
) -> Result<Result<CharTableRef, Option<crate::lisp::alloc::ConsRef>>, LispError> {
    let Some(entry) = char_code_property_entry(interp, prop, env)? else {
        return Ok(Err(None));
    };
    let mut loaded = false;
    let mut table = entry.cdr.get();
    if let Kind::StringObject(_) = table.kind() {
        let file = Value::string(&format!("international/{}", string_text(&table)?));
        let saved = crate::lisp::primitives::call(interp, "match-data", &[], env)?;
        let held = vec![saved, file];
        let result = interp.with_lisp_stack_roots(&held, |interp| {
            crate::lisp::primitives::call(
                interp,
                "load",
                &[file, Value::T, Value::T, Value::T, Value::T],
                env,
            )
        });
        crate::lisp::primitives::call(interp, "set-match-data", &[saved, Value::T], env)?;
        if result?.is_nil() {
            return Ok(Err(None));
        }
        loaded = true;
        table = entry.cdr.get();
    }
    let fallback = Err((!loaded).then_some(entry));
    let Kind::CharTable(table) = table.kind() else {
        return Ok(fallback);
    };
    if !uniprop_table_p(table) {
        return Ok(fallback);
    }
    match interp
        .char_table_extra_slot(table, 1)
        .map(|decoder| decoder.kind())
    {
        None | Some(Kind::Nil | Kind::Integer(0)) => Ok(Ok(table)),
        _ => Ok(fallback),
    }
}

/// chartab.c:get_unicode_property: TABLE's element for CODE, through the
/// table's decoder: uniprop_decode_value_run_length when extra slot 1 is
/// 0, which reads a value in range of extra slot 4's vector as an index
/// into it.  XFIXNUM of nil, whose word is 0, is index 0; any other
/// non-fixnum gives an index out of range and stands.
fn get_unicode_property(interp: &Interpreter, table: CharTableRef, code: u32) -> Value {
    let value = interp.char_table_get(table, code).unwrap_or(Value::Nil);
    if interp.char_table_extra_slot(table, 1) != Some(Value::Integer(0)) {
        return value;
    }
    let Some(Kind::Vector(values)) = interp
        .char_table_extra_slot(table, 4)
        .map(|values| values.kind())
    else {
        return value;
    };
    let index = match value.kind() {
        Kind::Integer(index) => index,
        Kind::Nil => 0,
        _ => return value,
    };
    usize::try_from(index)
        .ok()
        .and_then(|index| values.get(index))
        .unwrap_or(value)
}

fn unicode_property_character(value: &Value) -> Result<u32, LispError> {
    match value.kind() {
        Kind::Integer(character) if (0..=0x3f_ffff).contains(&character) => Ok(character as u32),
        _ => Err(wrong_type_argument("characterp", *value)),
    }
}

fn unicode_property_vector_values(value: &Value) -> Result<Vec<Value>, LispError> {
    vector_or_list_items(value)
}

fn encode_unicode_property_value(
    interp: &mut Interpreter,
    table_id: CharTableRef,
    value: &Value,
) -> Result<Value, LispError> {
    let Some(Kind::Integer(encoder)) = interp.char_table_extra_slot(table_id, 2).map(|v| v.kind())
    else {
        return Ok(*value);
    };
    match encoder {
        0 => {
            if value.is_nil()
                || matches!(value.kind(), Kind::Integer(character) if (0..=0x3f_ffff).contains(&character))
            {
                Ok(*value)
            } else {
                Err(wrong_type_argument("integerp", *value))
            }
        }
        1 | 2 => {
            if encoder == 2 && !matches!(value.kind(), Kind::Integer(_)) {
                return Err(wrong_type_argument("fixnump", *value));
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
                    interp.set_char_table_extra_slot(table_id, 4, Value::vector(values))?;
                    index
                }
                None => {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::String("Unicode property value".into()),
                        *value,
                    ])));
                }
            };
            Ok(Value::Integer(index as i64))
        }
        _ => Ok(*value),
    }
}

/// `string-match', callable directly.
pub(super) fn direct_string_match(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    regexp::string_match_impl(interp, args, env, true)
}

/// `string-match-p', callable directly.
pub(super) fn direct_string_match_p(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    regexp::string_match_impl(interp, args, env, false)
}
