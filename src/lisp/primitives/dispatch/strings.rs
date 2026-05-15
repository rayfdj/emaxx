use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "make-abbrev-table"
            | "make-string"
            | "make-temp-name"
            | "make-vector"
            | "make-bool-vector"
            | "bool-vector"
            | "make-keymap"
            | "make-sparse-keymap"
            | "make-mode-line-mouse-map"
            | "vconcat"
            | "copy-keymap"
            | "record"
            | "make-record"
            | "make-finalizer"
            | "concat"
            | "string-match"
            | "string-match-p"
            | "subregexp-context-p"
            | "isearch-no-upper-case-p"
            | "string-empty-p"
            | "string-prefix-p"
            | "string-suffix-p"
            | "string-limit"
            | "split-string"
            | "string-split"
            | "string-width"
            | "char-width"
            | "truncate-string-to-width"
            | "string"
            | "string-to-list"
            | "substring"
            | "substring-no-properties"
            | "string-to-unibyte"
            | "string-to-multibyte"
            | "string-make-multibyte"
            | "string-as-multibyte"
            | "string-make-unibyte"
            | "string-as-unibyte"
            | "unibyte-string"
            | "multibyte-char-to-unibyte"
            | "string-to-number"
            | "number-to-string"
            | "int-to-string"
            | "format"
            | "format-message"
            | "internal--format-docstring-line"
            | "ngettext"
            | "format-spec"
            | "char-to-string"
            | "string-replace"
            | "subst-char-in-string"
            | "replace-regexp-in-string"
            | "edmacro-parse-keys"
            | "string-trim-left"
            | "string-trim-right"
            | "string-trim"
            | "string-clean-whitespace"
            | "url-hexify-string"
            | "url-encode-url"
            | "base64-encode-region"
            | "base64url-encode-region"
            | "base64-encode-string"
            | "base64url-encode-string"
            | "base64-decode-region"
            | "base64-decode-string"
            | "abbrev-table-p"
            | "abbrev-table-empty-p"
            | "abbrev-table-get"
            | "abbrev-table-put"
            | "define-abbrev"
            | "abbrev-expansion"
            | "clear-abbrev-table"
            | "copy-abbrev-table"
            | "abbrev-table-name"
            | "byte-to-string"
            | "string-to-char"
            | "char-syntax"
            | "string-to-syntax"
            | "syntax-class-to-char"
            | "string-bytes"
            | "multibyte-string-p"
            | "unibyte-char-to-multibyte"
            | "upcase"
            | "downcase"
            | "capitalize"
            | "upcase-initials"
            | "get-char-code-property"
            | "char-code-property-description"
            | "char-resolve-modifiers"
    )
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        // ── Allocation ──
        "make-abbrev-table" => {
            need_arg_range(name, args, 0, 1)?;
            let props = args.first().cloned().unwrap_or(Value::Nil);
            Ok(make_runtime_abbrev_table(interp, None, props))
        }
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
            let c = char::from_u32(init as u32).unwrap_or('\0');
            let s: String = std::iter::repeat_n(c, length as usize).collect();
            Ok(Value::String(s))
        }
        "make-temp-name" => {
            need_args(name, args, 1)?;
            Ok(Value::String(make_temp_name(&string_text(&args[0])?)))
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
        "make-mode-line-mouse-map" => {
            need_args(name, args, 2)?;
            Ok(keymap_placeholder(Some("mode-line-mouse-map")))
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
                    if interp
                        .find_record(*id)
                        .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
                {
                    interp.copy_record(*id)
                }
                _ => Ok(args[0].clone()),
            }
        }
        "record" => {
            need_args(name, args, 1)?;
            if let Ok(type_name) = args[0].as_symbol() {
                Ok(interp.create_record(type_name, args[1..].to_vec()))
            } else {
                Ok(interp.create_record("literal-record", args.to_vec()))
            }
        }
        "make-record" => {
            need_args(name, args, 3)?;
            let type_name = args[0].as_symbol()?;
            let length = args[1].as_integer()?;
            if length < 0 {
                return Err(LispError::Signal("Wrong type argument: natnump".into()));
            }
            Ok(interp.create_record(
                type_name,
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
            for a in args {
                if let Some(string) = string_like(a) {
                    let offset = result.chars().count();
                    result.push_str(&string.text);
                    props.extend(shift_string_props(&string.props, offset));
                    multibyte |= string.multibyte;
                } else if a.is_nil() {
                } else if matches!(a, Value::Cons(_, _))
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
            Ok(string_like_value_with_multibyte(
                result,
                merge_string_props(props),
                multibyte,
            ))
        }
        "string-match" => regexp::string_match_impl(interp, args, env, true),
        "string-match-p" => regexp::string_match_impl(interp, args, env, false),
        "subregexp-context-p" => {
            need_arg_range(name, args, 2, 3)?;
            let regexp = string_text(&args[0])?;
            let pos = args[1].as_integer()?;
            let start = args
                .get(2)
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(0);
            if start < 0 || pos < start {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("args-out-of-range".into()),
                    Value::Nil,
                ])));
            }
            let prefix: String = regexp
                .chars()
                .skip(start as usize)
                .take((pos - start) as usize)
                .collect();
            match regexp::validate_elisp_regex(&prefix) {
                Ok(()) => Ok(Value::T),
                Err(error) if regexp::non_subregexp_context_error(&error) => Ok(Value::Nil),
                Err(_) => Ok(Value::T),
            }
        }
        "isearch-no-upper-case-p" => {
            need_args(name, args, 2)?;
            Ok(
                if regexp::isearch_no_upper_case_p(&string_text(&args[0])?, args[1].is_truthy()) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "string-empty-p" => {
            need_args(name, args, 1)?;
            Ok(if string_text(&args[0])?.is_empty() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "string-prefix-p" | "string-suffix-p" => {
            need_arg_range(name, args, 2, 3)?;
            let affix = string_text(&args[0])?;
            let text = string_text(&args[1])?;
            let ignore_case = args.get(2).is_some_and(Value::is_truthy);
            let (affix, text) = if ignore_case {
                (affix.to_lowercase(), text.to_lowercase())
            } else {
                (affix, text)
            };
            let matches = if name == "string-prefix-p" {
                text.starts_with(&affix)
            } else {
                text.ends_with(&affix)
            };
            Ok(if matches { Value::T } else { Value::Nil })
        }
        "string-limit" => {
            need_arg_range(name, args, 2, 4)?;
            let text = string_text(&args[0])?;
            let length = args[1].as_integer()?;
            if length < 0 {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("wrong-type-argument".into()),
                    Value::Symbol("natnump".into()),
                    args[1].clone(),
                ])));
            }
            let limit = length as usize;
            let end = args.get(2).is_some_and(Value::is_truthy);
            let coding_system = args.get(3).is_some_and(Value::is_truthy);
            if coding_system {
                let bytes = text.as_bytes();
                if bytes.len() <= limit {
                    return Ok(Value::String(text));
                }
                if end {
                    let mut start = bytes.len().saturating_sub(limit);
                    while start < bytes.len() && !text.is_char_boundary(start) {
                        start += 1;
                    }
                    return Ok(Value::String(text[start..].to_string()));
                }
                let mut end_byte = limit.min(bytes.len());
                while end_byte > 0 && !text.is_char_boundary(end_byte) {
                    end_byte -= 1;
                }
                return Ok(Value::String(text[..end_byte].to_string()));
            }
            let char_len = text.chars().count();
            if char_len <= limit {
                return Ok(Value::String(text));
            }
            let limited = if end {
                text.chars()
                    .skip(char_len.saturating_sub(limit))
                    .collect::<String>()
            } else {
                text.chars().take(limit).collect::<String>()
            };
            Ok(Value::String(limited))
        }
        "split-string" => {
            if args.is_empty() || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            regexp::split_string_impl(interp, &args[0], args.get(1), args.get(2), env)
        }
        "string-split" => {
            if args.is_empty() || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            regexp::split_string_impl(interp, &args[0], args.get(1), args.get(2), env)
        }
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
        "truncate-string-to-width" => {
            need_arg_range(name, args, 2, 5)?;
            let text = string_text(&args[0])?;
            let end_column = args[1].as_integer()?.max(0) as usize;
            let start_column = args
                .get(2)
                .filter(|value| !matches!(value, Value::Nil))
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(0)
                .max(0) as usize;
            let padding = args
                .get(3)
                .filter(|value| !matches!(value, Value::Nil))
                .map(|value| {
                    if matches!(value, Value::T) {
                        Ok(' ')
                    } else {
                        let codepoint = value.as_integer()?;
                        char::from_u32(codepoint as u32).ok_or_else(|| {
                            LispError::Signal(format!("Invalid character: {codepoint}"))
                        })
                    }
                })
                .transpose()?;
            let ellipsis = args
                .get(4)
                .filter(|value| !matches!(value, Value::Nil))
                .map(string_text)
                .transpose()?;

            let mut result = String::new();
            let mut column = 0usize;
            let mut result_width = 0usize;
            for ch in text.chars() {
                let width = if ch == '\t' {
                    interp
                        .lookup_var("tab-width", &Vec::new())
                        .and_then(|value| value.as_integer().ok())
                        .unwrap_or(8)
                        .max(1) as usize
                } else {
                    ch.width().unwrap_or(0)
                };
                let next_column = column + width;
                if next_column <= start_column {
                    column = next_column;
                    continue;
                }
                if next_column > end_column {
                    if let Some(ellipsis) = &ellipsis {
                        let ellipsis_width = ellipsis
                            .chars()
                            .map(|ch| ch.width().unwrap_or(0))
                            .sum::<usize>();
                        if result_width + ellipsis_width <= end_column.saturating_sub(start_column)
                        {
                            result.push_str(ellipsis);
                            result_width += ellipsis_width;
                        }
                    }
                    break;
                }
                result.push(ch);
                result_width += width;
                column = next_column;
            }
            if let Some(padding) = padding {
                let target_width = end_column.saturating_sub(start_column);
                let pad_width = padding.width().unwrap_or(1).max(1);
                while result_width + pad_width <= target_width {
                    result.push(padding);
                    result_width += pad_width;
                }
            }
            Ok(Value::String(result))
        }
        "string" => {
            let mut result = String::new();
            for arg in args {
                let ch = arg.as_integer()?;
                let ch = char::from_u32(ch as u32)
                    .ok_or_else(|| LispError::Signal(format!("Invalid character: {ch}")))?;
                result.push(ch);
            }
            Ok(Value::String(result))
        }
        "string-to-list" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            Ok(Value::list(string_sequence_values(&string)))
        }
        "substring" | "substring-no-properties" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs("substring".into(), args.len()));
            }
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let chars: Vec<char> = string.text.chars().collect();
            let len = chars.len() as i64;
            let from = normalize_string_index(args.get(1), 0, len)? as usize;
            let to = normalize_string_index(args.get(2), len, len)? as usize;
            let props = if name == "substring-no-properties" {
                Vec::new()
            } else {
                slice_string_props(&string.props, from, to)
            };
            Ok(string_like_value(chars[from..to].iter().collect(), props))
        }
        "string-to-unibyte" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
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
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            Ok(make_shared_string_value_with_multibyte(
                string.text,
                string.props,
                true,
            ))
        }
        "string-make-multibyte" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let bytes = encode_raw_text_bytes(&string.text)?;
            let text = decode_latin_bytes(&bytes);
            let multibyte = text.chars().any(|ch| (ch as u32) > 0x7F);
            Ok(if string.props.is_empty() {
                if multibyte {
                    make_shared_string_value_with_multibyte(text, Vec::new(), true)
                } else {
                    Value::String(text)
                }
            } else {
                make_shared_string_value_with_multibyte(text, string.props, multibyte)
            })
        }
        "string-as-multibyte" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let bytes = encode_raw_text_bytes(&string.text)?;
            let text = bytes
                .into_iter()
                .map(|byte| {
                    if byte <= 0x7F {
                        char::from(byte)
                    } else {
                        char::from_u32(RAW_BYTE8_BASE + byte as u32)
                            .expect("raw byte8 marker should be valid")
                    }
                })
                .collect::<String>();
            Ok(make_shared_string_value_with_multibyte(
                text,
                Vec::new(),
                true,
            ))
        }
        "string-make-unibyte" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let bytes = encode_raw_text_bytes(&string.text)?;
            Ok(bytes_to_shared_unibyte_value(&bytes))
        }
        "string-as-unibyte" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let bytes = encode_raw_text_bytes(&string.text)?;
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
        "number-to-string" | "int-to-string" => {
            need_args(name, args, 1)?;
            Ok(Value::String(number_to_string(&args[0])?))
        }
        "format" | "format-message" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let fmt_value = &args[0];
            let fmt = string_text(fmt_value)?;
            let mut result = String::new();
            let mut result_props = Vec::new();
            let mut arg_idx = 1;
            let chars: Vec<char> = fmt.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] != '%' || i + 1 >= chars.len() {
                    let start = result.chars().count();
                    result.push(chars[i]);
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

                let (mut formatted, mut formatted_props) = match conv {
                    's' => format_s_conversion(arg, precision)?,
                    'S' => (render_prin1_ephemeral(interp, arg, env)?, Vec::new()),
                    'd' | 'o' | 'x' | 'X' | 'b' | 'B' => (
                        format_numeric_conversion(
                            interp, arg, conv, flag_hash, flag_plus, flag_space, precision,
                        )?,
                        Vec::new(),
                    ),
                    'f' => (
                        format_float_conversion(interp, arg, flag_plus, flag_space, precision)?,
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
                    &merge_string_props(formatted_props),
                    start,
                ));
            }
            Ok(string_like_value(result, merge_string_props(result_props)))
        }
        "internal--format-docstring-line" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let template = string_text(&args[0])?;
            if template.contains('\n') {
                return Err(LispError::Signal(format!(
                    "Unable to fill string containing newline: {template:?}"
                )));
            }
            super::call(interp, "format", args, env)
        }
        "ngettext" => {
            need_args(name, args, 3)?;
            let singular = string_text(&args[0])?;
            let plural = string_text(&args[1])?;
            let count = args[2].as_integer()?;
            Ok(Value::String(if count == 1 { singular } else { plural }))
        }
        "format-spec" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let format = string_text(&args[0])?;
            let entries = args[1].to_vec()?;
            let ignore_missing = args.get(2).unwrap_or(&Value::Nil);
            let split = args.get(3).is_some_and(Value::is_truthy);
            let chars: Vec<char> = format.chars().collect();
            let mut result = String::new();
            let mut split_start = 0usize;
            let mut split_result = Vec::new();
            let mut i = 0usize;
            while i < chars.len() {
                let ch = chars[i];
                if ch != '%' {
                    result.push(ch);
                    i += 1;
                    continue;
                }
                let spec_start = i;
                i += 1;
                if i >= chars.len() {
                    result.push('%');
                    break;
                }
                if chars[i] == '%' {
                    if format_spec_collapses_quoted_percent(ignore_missing) {
                        result.push('%');
                    } else {
                        result.push('%');
                        result.push('%');
                    }
                    i += 1;
                    continue;
                }

                let Some(parsed) = parse_format_spec(&chars, i) else {
                    if ignore_missing.is_nil() {
                        return Err(LispError::Signal("Invalid format string".into()));
                    }
                    result.push('%');
                    continue;
                };
                i = parsed.end;

                if split && result.chars().count() > split_start {
                    let part = result
                        .chars()
                        .skip(split_start)
                        .take(result.chars().count() - split_start)
                        .collect::<String>();
                    split_result.push(Value::String(part));
                }

                let replacement = format_spec_replacement(interp, env, &entries, parsed.specifier)?;
                if let Some(replacement) = replacement {
                    let formatted = apply_format_spec_flags(replacement, &parsed);
                    result.push_str(&formatted);
                    if split {
                        split_result.push(Value::String(formatted));
                        split_start = result.chars().count();
                    }
                } else if matches!(ignore_missing, Value::Symbol(symbol) if symbol == "delete") {
                    if split {
                        split_result.push(Value::String(String::new()));
                        split_start = result.chars().count();
                    }
                } else if ignore_missing.is_nil() {
                    return Err(LispError::Signal(format!(
                        "Invalid format character: `%{}'",
                        parsed.specifier
                    )));
                } else {
                    let original = chars[spec_start..parsed.end].iter().collect::<String>();
                    result.push_str(&original);
                    if split {
                        split_result.push(Value::String(original));
                        split_start = result.chars().count();
                    }
                }
            }
            if split {
                let result_len = result.chars().count();
                if split_start < result_len {
                    split_result.push(Value::String(
                        result
                            .chars()
                            .skip(split_start)
                            .take(result_len - split_start)
                            .collect(),
                    ));
                }
                Ok(Value::list(split_result))
            } else {
                Ok(Value::String(result))
            }
        }
        "char-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            let c = char::from_u32(n as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid character: {}", n)))?;
            Ok(Value::String(c.to_string()))
        }
        "string-replace" => {
            need_args(name, args, 3)?;
            let from = string_text(&args[0])?;
            let to = string_text(&args[1])?;
            let input = string_text(&args[2])?;
            Ok(Value::String(input.replace(&from, &to)))
        }
        "subst-char-in-string" => {
            need_arg_range(name, args, 3, 4)?;
            let from = char_from_integer(args[0].as_integer()?)?;
            let to = char_from_integer(args[1].as_integer()?)?;
            let mut string = string_like(&args[2])
                .ok_or_else(|| LispError::TypeError("string".into(), args[2].type_name()))?;
            string.text = string
                .text
                .chars()
                .map(|ch| if ch == from { to } else { ch })
                .collect();
            if args.get(3).is_some_and(Value::is_truthy) {
                if let Value::StringObject(state) = &args[2] {
                    state.borrow_mut().text = string.text.clone();
                }
                Ok(args[2].clone())
            } else {
                Ok(string_like_value_with_multibyte(
                    string.text,
                    string.props,
                    string.multibyte,
                ))
            }
        }
        "replace-regexp-in-string" => {
            if args.len() < 3 || args.len() > 7 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pattern = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let replacement = string_like(&args[1]);
            let source = string_like(&args[2])
                .ok_or_else(|| LispError::TypeError("string".into(), args[2].type_name()))?;
            let literal = args.get(4).is_some_and(Value::is_truthy);
            let subexp = args
                .get(5)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(0)
                .max(0) as usize;
            let source_len = source.text.chars().count() as i64;
            let start = normalize_string_index(args.get(6), 0, source_len)? as usize;
            regexp::validate_elisp_regex(&pattern.text)?;
            let regex = regexp::compile_elisp_regex(interp, &pattern, env, "", true)?;
            let mut result = source.text.chars().take(start).collect::<String>();
            let mut search_pos = start;
            let mut search_byte = regexp::byte_index_for_char(&source.text, start);

            while let Some(captures) = regex
                .captures_from_pos(&source.text, search_byte)
                .map_err(|error| LispError::Signal(error.to_string()))?
            {
                let Some(full_match) = captures.get(0) else {
                    break;
                };
                let full_start = source.text[..full_match.start()].chars().count();
                let full_end = source.text[..full_match.end()].chars().count();
                let match_data = regexp::match_data_from_captures(
                    0,
                    &source.text,
                    &captures,
                    regex.capture_mapping(),
                );
                let (replace_start, replace_end) = match_data
                    .get(subexp)
                    .and_then(|entry| *entry)
                    .or_else(|| match_data.first().and_then(|entry| *entry))
                    .ok_or_else(|| LispError::Signal("No previous search".into()))?;

                result.push_str(&regexp::slice_string_chars(
                    &source.text,
                    search_pos,
                    replace_start,
                ));
                if let Some(replacement) = &replacement {
                    result.push_str(&regexp::expand_replace_match_text(
                        &replacement.text,
                        &match_data,
                        literal,
                        &source.text,
                    )?);
                } else {
                    regexp::set_match_data(
                        interp,
                        0,
                        &source.text,
                        &captures,
                        regex.capture_mapping(),
                        None,
                    );
                    let matched_text =
                        regexp::slice_string_chars(&source.text, replace_start, replace_end);
                    let value =
                        call_function_value(interp, &args[1], &[Value::String(matched_text)], env)?;
                    result.push_str(&string_text(&value)?);
                }
                result.push_str(&regexp::slice_string_chars(
                    &source.text,
                    replace_end,
                    full_end,
                ));

                if full_start == full_end {
                    if let Some(ch) = source.text.chars().nth(search_pos) {
                        result.push(ch);
                        search_pos += 1;
                        search_byte = regexp::byte_index_for_char(&source.text, search_pos);
                        continue;
                    }
                    break;
                }

                search_pos = full_end;
                search_byte = regexp::byte_index_for_char(&source.text, search_pos);
            }

            result.push_str(&regexp::slice_string_chars(
                &source.text,
                search_pos,
                source.text.chars().count(),
            ));
            Ok(Value::String(result))
        }
        "edmacro-parse-keys" => {
            need_arg_range(name, args, 1, 2)?;
            parse_edmacro_key_sequence(&string_text(&args[0])?)
        }
        "string-trim-left" => {
            need_arg_range(name, args, 1, 2)?;
            regexp::string_trim_left_value(interp, &args[0], args.get(1), env)
        }
        "string-trim-right" => {
            need_arg_range(name, args, 1, 2)?;
            regexp::string_trim_right_value(interp, &args[0], args.get(1), env)
        }
        "string-trim" => {
            need_arg_range(name, args, 1, 3)?;
            let trimmed = regexp::string_trim_left_value(interp, &args[0], args.get(1), env)?;
            regexp::string_trim_right_value(interp, &trimmed, args.get(2), env)
        }
        "string-clean-whitespace" => {
            need_args(name, args, 1)?;
            let cleaned = string_text(&args[0])?
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            Ok(Value::String(cleaned))
        }
        "url-hexify-string" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let input = string_text(&args[0])?;
            let allowed = args
                .get(1)
                .and_then(string_like)
                .map(|allowed| allowed.text)
                .unwrap_or_default();
            let mut output = String::new();
            for ch in input.chars() {
                if ch.is_ascii_alphanumeric()
                    || matches!(ch, '-' | '_' | '.' | '~')
                    || allowed.contains(ch)
                {
                    output.push(ch);
                } else {
                    for byte in ch.to_string().bytes() {
                        output.push('%');
                        output.push_str(&format!("{byte:02X}"));
                    }
                }
            }
            Ok(Value::String(output))
        }
        "url-encode-url" => {
            need_args(name, args, 1)?;
            Ok(Value::String(url_encode_url(&string_text(&args[0])?)))
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
        "abbrev-table-p" => {
            need_args(name, args, 1)?;
            Ok(if is_abbrev_table_value(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "abbrev-table-empty-p" => {
            need_args(name, args, 1)?;
            Ok(if abbrev_table_entries(interp, &args[0])?.is_empty() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "abbrev-table-get" => {
            need_args(name, args, 2)?;
            Ok(abbrev_table_property(interp, &args[0], &args[1]).unwrap_or(Value::Nil))
        }
        "abbrev-table-put" => {
            need_args(name, args, 3)?;
            set_abbrev_table_property(interp, &args[0], &args[1], args[2].clone())?;
            Ok(args[2].clone())
        }
        "define-abbrev" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let hook = args.get(3).cloned().unwrap_or(Value::Nil);
            let props = abbrev_props_from_parts(Some(hook), &args[4..])?;
            define_abbrev_entry(
                interp,
                &args[0],
                &string_text(&args[1])?,
                args[2].clone(),
                props,
            )?;
            Ok(args[2].clone())
        }
        "abbrev-expansion" => {
            need_args(name, args, 2)?;
            Ok(abbrev_expansion(interp, &args[1], &string_text(&args[0])?)?.unwrap_or(Value::Nil))
        }
        "clear-abbrev-table" => {
            need_args(name, args, 1)?;
            set_abbrev_table_entries(interp, &args[0], Vec::new())?;
            Ok(Value::Nil)
        }
        "copy-abbrev-table" => {
            need_args(name, args, 1)?;
            copy_abbrev_table(interp, &args[0])
        }
        "abbrev-table-name" => {
            need_args(name, args, 1)?;
            Ok(abbrev_table_name_value(interp, &args[0]).unwrap_or(Value::Nil))
        }
        "byte-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            if !(0..=255).contains(&n) {
                return Err(LispError::Signal("Byte value out of range".into()));
            }
            let c = char::from_u32(n as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid byte: {}", n)))?;
            Ok(Value::String(c.to_string()))
        }
        "string-to-char" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            Ok(string
                .text
                .chars()
                .next()
                .map(|ch| string_sequence_value(&string, ch))
                .unwrap_or(Value::Integer(0)))
        }
        "char-syntax" => {
            need_args(name, args, 1)?;
            let code = u32::try_from(args[0].as_integer()?)
                .map_err(|_| LispError::Signal("Invalid character".into()))?;
            let class =
                syntax::syntax_entry_for_code(interp, interp.current_syntax_table_id(), code).class;
            Ok(Value::Integer(syntax::syntax_class_char(class) as i64))
        }
        "string-to-syntax" => {
            need_args(name, args, 1)?;
            let spec = string_text(&args[0])?;
            let Some(entry) = syntax::parse_syntax_spec(&spec) else {
                return Ok(Value::Nil);
            };
            Ok(syntax::syntax_entry_value(entry))
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
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let len: usize = if string.multibyte {
                string.text.len()
            } else {
                string
                    .text
                    .chars()
                    .map(|ch| {
                        if raw_byte_from_regex_char(ch).is_some() || (ch as u32) <= 0xFF {
                            1usize
                        } else {
                            ch.len_utf8()
                        }
                    })
                    .sum()
            };
            Ok(Value::Integer(len as i64))
        }
        "multibyte-string-p" => {
            need_args(name, args, 1)?;
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
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
            Ok(Value::Integer(n))
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
        "get-char-code-property" => {
            need_args(name, args, 2)?;
            let ch = u32::try_from(args[0].as_integer()?)
                .map_err(|_| LispError::Signal("Invalid character".into()))?;
            let property = args[1].as_symbol()?;
            let value = match property {
                "name" => unicode_character_name(ch)
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
                "general-category" => unicode_general_category_symbol(ch)
                    .map(|symbol| Value::Symbol(symbol.into()))
                    .unwrap_or(Value::Nil),
                _ => match (normalize_case_key(ch), property) {
                    (code, "uppercase") => {
                        if code == 0x00DF {
                            Value::Nil
                        } else {
                            let mapped = simple_upcase_char(code);
                            if mapped == code {
                                Value::Nil
                            } else {
                                Value::Integer(mapped as i64)
                            }
                        }
                    }
                    (code, "lowercase") => {
                        let mapped = simple_downcase_char(code, false);
                        if mapped == code {
                            Value::Nil
                        } else {
                            Value::Integer(mapped as i64)
                        }
                    }
                    (code, "titlecase") => {
                        if code == 0x00DF {
                            Value::Nil
                        } else if code == 0x01C5 {
                            Value::Integer(code as i64)
                        } else {
                            let mapped = simple_titlecase_char(code);
                            if mapped == code {
                                Value::Nil
                            } else {
                                Value::Integer(mapped as i64)
                            }
                        }
                    }
                    (0x00DF, "special-uppercase") => Value::String("SS".into()),
                    (0x00DF, "special-titlecase") => Value::String("Ss".into()),
                    (0x00DF, "special-lowercase") => Value::Nil,
                    (0x00DF, _) => Value::Nil,
                    (0x00CF, _) | (0x00EF, _) | (0x00FF, _) => Value::Nil,
                    (0x0130, "special-lowercase") => Value::String("i\u{307}".into()),
                    (0x0130, _) => Value::Nil,
                    (0xFB01, "special-uppercase") => Value::String("FI".into()),
                    (0xFB01, "special-titlecase") => Value::String("Fi".into()),
                    (0xFB01, _) => Value::Nil,
                    _ => Value::Nil,
                },
            };
            Ok(value)
        }
        "char-code-property-description" => {
            need_args(name, args, 2)?;
            let property = args[0].as_symbol()?;
            Ok(unicode_property_description(property, &args[1])
                .map(|description| Value::String(description.into()))
                .unwrap_or(Value::Nil))
        }
        "char-resolve-modifiers" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(resolve_char_modifiers(
                args[0].as_integer()?,
            )))
        }

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}
