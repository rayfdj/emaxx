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
            | "format-network-address"
            | "internal--format-docstring-line"
            | "ngettext"
            | "format-spec"
            | "char-to-string"
            | "find-composition-internal"
            | "ucs-normalize-NFC-string"
            | "ucs-normalize-NFD-string"
            | "string-replace"
            | "subst-char-in-string"
            | "replace-regexp-in-string"
            | "edmacro-parse-keys"
            | "read-kbd-macro"
            | "string-trim-left"
            | "string-trim-right"
            | "string-trim"
            | "string-clean-whitespace"
            | "url-hexify-string"
            | "url-insert-entities-in-string"
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
            | "make-char"
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
            | "unicode-property-table-internal"
            | "get-char-code-property"
            | "char-code-property-description"
            | "char-resolve-modifiers"
    )
}

/// Map an Emacs character code to a Rust char, translating the raw-byte
/// range (RAW_BYTE8_BASE #x3FFF00..) to the internal private-use marker.
fn char_for_codepoint(n: i64) -> Result<char, LispError> {
    let code = n as u32;
    if (0x3FFF00..=0x3FFFFF).contains(&code) {
        let byte = (code - 0x3FFF00) as u8;
        return Ok(char::from_u32(0xE000 + byte as u32).expect("raw byte marker"));
    }
    char::from_u32(code).ok_or_else(|| LispError::Signal(format!("Invalid character: {n}")))
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
            // GNU takes a sixth ELLIPSIS-TEXT-PROPERTY argument (it only
            // affects display properties on the ellipsis).
            need_arg_range(name, args, 2, 6)?;
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
                .map(|value| {
                    // GNU: a non-string ELLIPSIS means "use the default"
                    // (the `truncate-string-ellipsis' function).
                    string_text(value).or_else(|_: LispError| {
                        Ok::<String, LispError>(
                            interp
                                .lookup_var("truncate-string-ellipsis", env)
                                .and_then(|value| string_like(&value).map(|text| text.text.clone()))
                                .unwrap_or_else(|| "\u{2026}".to_string()),
                        )
                    })
                })
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
                result.push(char_for_codepoint(arg.as_integer()?)?);
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
                    &merge_string_props(formatted_props),
                    start,
                ));
            }
            Ok(string_like_value(result, merge_string_props(result_props)))
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
            Ok(Value::String(address))
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
            // GNU builds the result in a buffer seeded with FORMAT, so text
            // properties survive: literals keep their own props, a
            // replacement inherits the spec text's props (insert-and-inherit
            // next to the "%"), and a collapsed "%%" keeps the first "%"'s.
            // Track, for every output char, the FORMAT char it derives its
            // properties from.
            let format_props: Vec<crate::lisp::types::StringPropertySpan> = match &args[0] {
                Value::StringObject(state) => state.borrow().props.clone(),
                _ => Vec::new(),
            };
            let format_multibyte = match &args[0] {
                Value::StringObject(state) => state.borrow().multibyte,
                _ => false,
            };
            let entries = args[1].to_vec()?;
            let ignore_missing = args.get(2).unwrap_or(&Value::Nil);
            let split = args.get(3).is_some_and(Value::is_truthy);
            let chars: Vec<char> = format.chars().collect();
            let mut result = String::new();
            // Per output char: (instance, rep_char, src).  Instance 0 =
            // FORMAT literal at char SRC; instance K>0 = the K'th spec
            // replacement (own props at REP_CHAR when Some, inheriting the
            // FORMAT props at SRC = the spec's "%").  Splice boundaries
            // never merge, matching the interval structure GNU's
            // buffer-based build leaves behind.
            let mut provenance: Vec<(usize, Option<usize>, usize)> = Vec::new();
            let mut rep_props: Vec<Vec<crate::lisp::types::StringPropertySpan>> = Vec::new();
            let mut split_start = 0usize;
            let mut split_result = Vec::new();
            let mut i = 0usize;
            while i < chars.len() {
                let ch = chars[i];
                if ch != '%' {
                    result.push(ch);
                    provenance.push((0, None, i));
                    i += 1;
                    continue;
                }
                let spec_start = i;
                i += 1;
                if i >= chars.len() {
                    result.push('%');
                    provenance.push((0, None, spec_start));
                    break;
                }
                if chars[i] == '%' {
                    if format_spec_collapses_quoted_percent(ignore_missing) {
                        result.push('%');
                        provenance.push((0, None, spec_start));
                    } else {
                        result.push('%');
                        result.push('%');
                        provenance.push((0, None, spec_start));
                        provenance.push((0, None, i));
                    }
                    i += 1;
                    continue;
                }

                let Some(parsed) = parse_format_spec(&chars, i) else {
                    if ignore_missing.is_nil() {
                        return Err(LispError::Signal("Invalid format string".into()));
                    }
                    result.push('%');
                    provenance.push((0, None, spec_start));
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
                if let Some((replacement, replacement_props)) = replacement {
                    let (formatted, sources) =
                        apply_format_spec_flags_indexed(replacement, &parsed);
                    rep_props.push(replacement_props);
                    let instance = rep_props.len();
                    provenance.extend(sources.iter().map(|source| (instance, *source, spec_start)));
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
                    provenance.extend((spec_start..parsed.end).map(|index| (0, None, index)));
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
            } else if format_props.is_empty() && rep_props.iter().all(|props| props.is_empty()) {
                Ok(Value::String(result))
            } else {
                // Merge runs by SOURCE INTERVAL IDENTITY, not value
                // equality: GNU's buffer-based implementation carries the
                // template's and each replacement's interval structure into
                // the output, so adjacent spans with `equal' but not `eq'
                // values stay separate intervals, and splice boundaries
                // never coalesce (erc snapshots compare the
                // `object-intervals' fragmentation).
                let span_ids_at = |spans: &[crate::lisp::types::StringPropertySpan],
                                   index: usize|
                 -> Vec<usize> {
                    spans
                        .iter()
                        .enumerate()
                        .filter(|(_, span)| span.start <= index && index < span.end)
                        .map(|(id, _)| id)
                        .collect()
                };
                // Run key: (instance, own-span ids).  Instance 0 reads ids
                // against FORMAT's spans; instance K>0 against replacement
                // K's own spans, inheriting FORMAT props at the spec's "%".
                let key_for = |&(instance, rep_char, src): &(usize, Option<usize>, usize)| {
                    let ids = if instance == 0 {
                        span_ids_at(&format_props, src)
                    } else {
                        rep_char
                            .map(|index| span_ids_at(&rep_props[instance - 1], index))
                            .unwrap_or_default()
                    };
                    (instance, ids)
                };
                let props_for = |(instance, ids): &(usize, Vec<usize>), src: usize| {
                    let mut collected: Vec<(String, Value)> = Vec::new();
                    if *instance == 0 {
                        for &id in ids {
                            collected.extend(format_props[id].props.iter().cloned());
                        }
                    } else {
                        for &id in ids {
                            collected.extend(rep_props[*instance - 1][id].props.iter().cloned());
                        }
                        // insert-and-inherit: inherited FORMAT props fill in
                        // keys the replacement's own props don't set.
                        for inherit_id in span_ids_at(&format_props, src) {
                            for (key, value) in &format_props[inherit_id].props {
                                if !collected.iter().any(|(existing, _)| existing == key) {
                                    collected.push((key.clone(), value.clone()));
                                }
                            }
                        }
                    }
                    collected
                };
                let mut spans = Vec::new();
                let mut run_start = 0usize;
                let mut run_key: Option<(usize, Vec<usize>)> = None;
                let mut run_src = 0usize;
                for (out_index, source) in provenance.iter().enumerate() {
                    let key = key_for(source);
                    match &run_key {
                        Some(current) if *current == key => {}
                        _ => {
                            if let Some(current) = run_key.take() {
                                let props = props_for(&current, run_src);
                                if !props.is_empty() && run_start < out_index {
                                    spans.push(crate::lisp::types::StringPropertySpan {
                                        start: run_start,
                                        end: out_index,
                                        props,
                                    });
                                }
                            }
                            run_start = out_index;
                            run_src = source.2;
                            run_key = Some(key);
                        }
                    }
                }
                if let Some(current) = run_key {
                    let props = props_for(&current, run_src);
                    if !props.is_empty() && run_start < provenance.len() {
                        spans.push(crate::lisp::types::StringPropertySpan {
                            start: run_start,
                            end: provenance.len(),
                            props,
                        });
                    }
                }
                if spans.is_empty() {
                    Ok(Value::String(result))
                } else {
                    Ok(Value::StringObject(std::rc::Rc::new(
                        std::cell::RefCell::new(crate::lisp::types::SharedStringState {
                            text: result,
                            props: spans,
                            multibyte: format_multibyte,
                        }),
                    )))
                }
            }
        }
        "char-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            let c = char_for_codepoint(n)?;
            Ok(Value::String(c.to_string()))
        }
        "find-composition-internal" => {
            // (find-composition-internal POS LIMIT STRING DETAIL-P): report
            // the grapheme cluster containing POS.  String positions are
            // zero-based, while buffer positions are one-based.  Emacs uses
            // this both for `string-glyph-split' and to keep byte-limited ERC
            // lines from splitting a base character from its combining mark.
            need_arg_range(name, args, 4, 4)?;
            let raw_pos = args[0].as_integer()?.max(0) as usize;
            let (text, pos, position_base) = if let Some(string) = string_like(&args[2]) {
                (string.text, raw_pos, 0usize)
            } else if args[2].is_nil() {
                (
                    interp.current_buffer().full_buffer_string(),
                    raw_pos.saturating_sub(1),
                    1usize,
                )
            } else {
                return Err(LispError::TypeError("string".into(), args[2].type_name()));
            };
            use unicode_segmentation::UnicodeSegmentation;
            if pos >= text.chars().count() {
                return Ok(Value::Nil);
            }
            // Walk grapheme clusters, tracking character offsets, to find the
            // one containing POS.
            let mut char_offset = 0usize;
            for cluster in text.graphemes(true) {
                let cluster_len = cluster.chars().count();
                if (char_offset..char_offset + cluster_len).contains(&pos) {
                    if cluster_len > 1 {
                        return Ok(Value::list([
                            Value::Integer((char_offset + position_base) as i64),
                            Value::Integer((char_offset + cluster_len + position_base) as i64),
                            Value::Nil,
                        ]));
                    }
                    return Ok(Value::Nil);
                }
                if char_offset > pos {
                    break;
                }
                char_offset += cluster_len;
            }
            Ok(Value::Nil)
        }
        "ucs-normalize-NFC-string" => {
            need_args(name, args, 1)?;
            use unicode_normalization::UnicodeNormalization;
            let input = string_text(&args[0])?;
            Ok(Value::String(input.nfc().collect()))
        }
        "ucs-normalize-NFD-string" => {
            need_args(name, args, 1)?;
            use unicode_normalization::UnicodeNormalization;
            let input = string_text(&args[0])?;
            Ok(Value::String(input.nfd().collect()))
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
                    // An empty match may sit past search_pos (anchors like
                    // `$'); resume after it, consuming one char to advance.
                    search_pos = full_end;
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
        "read-kbd-macro" => {
            // The Lisp calling convention with a string START returns the
            // parsed macro instead of installing it.
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
        "url-insert-entities-in-string" => {
            need_args(name, args, 1)?;
            let input = string_text(&args[0])?;
            let mut output = String::new();
            for ch in input.chars() {
                match ch {
                    '"' => output.push_str("&quot;"),
                    '&' => output.push_str("&amp;"),
                    '<' => output.push_str("&lt;"),
                    '>' => output.push_str("&gt;"),
                    _ => output.push(ch),
                }
            }
            Ok(Value::String(output))
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
        "make-char" => {
            need_arg_range(name, args, 1, 2)?;
            let _charset = args[0].as_symbol()?;
            let code = args.get(1).map(Value::as_integer).transpose()?.unwrap_or(0);
            Ok(Value::Integer(code))
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
            let table = interp.make_char_table(Some(property.into()), Value::Nil);
            if property == "decomposition" {
                populate_unicode_decomposition_table(interp, &table)?;
            }
            Ok(table)
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
                "canonical-combining-class" => canonical_combining_class(ch)
                    .map(Value::Integer)
                    .unwrap_or(Value::Integer(0)),
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

fn populate_unicode_decomposition_table(
    interp: &mut Interpreter,
    table: &Value,
) -> Result<(), LispError> {
    let Value::CharTable(id) = table else {
        unreachable!("make_char_table returns a char-table");
    };

    for range in [b'0'..=b'9', b'A'..=b'Z', b'a'..=b'z'] {
        for code in range {
            let code = code as u32;
            interp.char_table_set(
                *id,
                0xff00 + code - 0x20,
                Value::list([Value::symbol("wide"), Value::Integer(code as i64)]),
            )?;
        }
    }

    for (code, decomposition) in unicode_decomposition_entries() {
        interp.char_table_set(*id, code, Value::list(decomposition))?;
    }
    Ok(())
}

fn unicode_decomposition_entries() -> Vec<(u32, Vec<Value>)> {
    let compat = |chars: &[u32]| {
        let mut values = vec![Value::symbol("compat")];
        values.extend(chars.iter().map(|ch| Value::Integer(*ch as i64)));
        values
    };
    let canonical = |chars: &[u32]| {
        chars
            .iter()
            .map(|ch| Value::Integer(*ch as i64))
            .collect::<Vec<_>>()
    };

    vec![
        (0x00e4, canonical(&[b'a' as u32, 0x0308])),
        (0x00e5, canonical(&[b'a' as u32, 0x030a])),
        (0x00eb, canonical(&[b'e' as u32, 0x0308])),
        (0x00f1, canonical(&[b'n' as u32, 0x0303])),
        (0x00f6, canonical(&[b'o' as u32, 0x0308])),
        (0x0113, canonical(&[b'e' as u32, 0x0304])),
        (0x03af, canonical(&[0x03b9, 0x0301])),
        (0x03ca, canonical(&[0x03b9, 0x0308])),
        (0x0439, canonical(&[0x0438, 0x0306])),
        (0x0451, canonical(&[0x0435, 0x0308])),
        (0x1e17, canonical(&[0x0113, 0x0301])),
        (0x1f77, canonical(&[0x03b9, 0x0301])),
        (0x1fd3, canonical(&[0x03ca, 0x0301])),
        (0x212f, compat(&[b'e' as u32])),
        (0xfb00, compat(&[b'f' as u32, b'f' as u32])),
        (0xfb01, compat(&[b'f' as u32, b'i' as u32])),
        (0xfb02, compat(&[b'f' as u32, b'l' as u32])),
        (0xfb03, compat(&[b'f' as u32, b'f' as u32, b'i' as u32])),
        (0xfb04, compat(&[b'f' as u32, b'f' as u32, b'l' as u32])),
    ]
}

fn canonical_combining_class(ch: u32) -> Option<i64> {
    match ch {
        0x0300..=0x0314 | 0x031b | 0x0323..=0x0328 | 0x032d..=0x0338 | 0x0342 | 0x0345 => Some(230),
        0x0315 | 0x031a => Some(232),
        0x0316..=0x0319 | 0x031c..=0x0322 | 0x0329..=0x032c => Some(220),
        _ => None,
    }
}
