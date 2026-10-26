mod object;
mod output;
pub(crate) use object::PrintGlobals;
pub(crate) use output::PrintOutput;

use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

/// print.c's DEFVAR_BOOLs a string literal is escaped by.
#[derive(Clone, Copy)]
pub(crate) struct StringEscapes {
    multibyte: bool,
    newlines: bool,
    control: bool,
    nonascii: bool,
}

impl StringEscapes {
    /// The variables' dynamic values, as C reads them: no lexical binding
    /// of a special variable to search for.
    pub(crate) fn current(interp: &Interpreter) -> Self {
        let enabled = |key| {
            interp
                .symbol_value_key(key)
                .is_some_and(|value| value.is_truthy())
        };
        Self {
            multibyte: enabled(cached_symbol!("print-escape-multibyte")),
            newlines: enabled(cached_symbol!("print-escape-newlines")),
            control: enabled(cached_symbol!("print-escape-control-characters")),
            nonascii: enabled(cached_symbol!("print-escape-nonascii")),
        }
    }
}

pub(crate) fn render_prin1_string(
    escapes: StringEscapes,
    state: &crate::lisp::types::SharedStringState,
    rendered: &mut PrintOutput,
) {
    let StringEscapes {
        multibyte: escape_multibyte,
        newlines: escape_newlines,
        control: escape_control,
        nonascii: escape_nonascii,
    } = escapes;
    let multibyte = state.is_multibyte();
    rendered.push('"');
    let mut offset = 0;
    let mut need_nonhex = false;
    while offset < state.bytes().len() {
        // print.c:fetch_string_char_advance, before any escaping decision.
        let byte = state.bytes()[offset];
        let (code, width) = if multibyte && byte >= 0x80 {
            crate::lisp::types::string_data::decode_character(&state.bytes()[offset..])
                .expect("stored string character")
        } else {
            (u32::from(byte), 1)
        };
        offset += width;
        let raw = if multibyte && (0x3fff80..=0x3fffff).contains(&code) {
            Some(code - 0x3fff00)
        } else if !multibyte && code >= 128 && escape_nonascii {
            Some(code)
        } else {
            None
        };
        if let Some(byte) = raw {
            rendered.push_str(&format!("\\{byte:03o}"));
            need_nonhex = false;
        } else if multibyte && code >= 128 && escape_multibyte {
            rendered.push_str(&format!("\\x{code:04x}"));
            need_nonhex = true;
        } else {
            if code <= 127 && (code as u8).is_ascii_hexdigit() && need_nonhex {
                rendered.push_str("\\ ");
            }
            need_nonhex = false;
            match code {
                34 => rendered.push_str("\\\""),
                92 => rendered.push_str("\\\\"),
                10 if escape_newlines => rendered.push_str("\\n"),
                12 if escape_newlines => rendered.push_str("\\f"),
                code if escape_control && (code < 32 || code == 127) => {
                    let next_is_octal = state
                        .bytes()
                        .get(offset)
                        .is_some_and(|next| (b'0'..=b'7').contains(next));
                    let digits = if code > 0o77 || next_is_octal {
                        3
                    } else if code > 0o7 {
                        2
                    } else {
                        1
                    };
                    rendered.push('\\');
                    for shift in (0..digits).rev() {
                        rendered.push_code(u32::from(b'0') + ((code >> (3 * shift)) & 7));
                    }
                }
                code if !multibyte && code >= 128 => rendered.push_code(0x3fff00 + code),
                code => rendered.push_code(code),
            }
        }
    }
    rendered.push('"');
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PrintOptions {
    /// print.c passes `escapeflag' to every `print_object' call: `prin1'
    /// sets it, `princ' clears it, and it reaches nested elements too, so
    /// `(princ (list "a"))' prints `(a)'.
    escape: bool,
    circle: bool,
    continuous_numbering: bool,
    gensym: bool,
    integers_as_characters: bool,
    symbols_bare: bool,
    length: Option<usize>,
    level: Option<usize>,
    quoted: bool,
    /// print_string sends unibyte character codes to function streams,
    /// while buffer/stdout output preserves their byte8 representation.
    output_is_function: bool,
}

fn render_princ_string(
    interp: &Interpreter,
    state: &crate::lisp::types::SharedStringState,
    env: &Env,
    output_is_function: bool,
) -> PrintOutput {
    let escape_nonascii = !output_is_function
        && interp
            .lookup_var_key(cached_symbol!("print-escape-nonascii"), env)
            .is_some_and(|value| value.is_truthy());
    let mut rendered = PrintOutput::with_capacity(state.bytes().len());
    let mut offset = 0;
    while offset < state.bytes().len() {
        let byte = state.bytes()[offset];
        let (code, width) = if state.is_multibyte() && byte >= 0x80 {
            crate::lisp::types::string_data::decode_character(&state.bytes()[offset..])
                .expect("stored string character")
        } else {
            (u32::from(byte), 1)
        };
        offset += width;
        let raw = if (0x3fff80..=0x3fffff).contains(&code) {
            Some(code - 0x3fff00)
        } else if !state.is_multibyte() && code >= 128 {
            Some(code)
        } else {
            None
        };
        if let Some(byte) = raw {
            if escape_nonascii {
                rendered.push_str(&format!("\\{byte:03o}"));
            } else if output_is_function && !state.is_multibyte() {
                rendered.push_code(byte);
            } else {
                rendered.push_code(0x3fff00 + byte);
            }
        } else {
            rendered.push_code(code);
        }
    }
    rendered
}

/// print.c reads the printer's DEFVARs as C variables: their dynamic
/// values, never a lexical binding.
pub(crate) fn print_options(interp: &Interpreter) -> PrintOptions {
    PrintOptions {
        escape: true,
        circle: interp
            .symbol_value_key(cached_symbol!("print-circle"))
            .is_some_and(|value| value.is_truthy()),
        continuous_numbering: interp
            .symbol_value_key(cached_symbol!("print-continuous-numbering"))
            .is_some_and(|value| value.is_truthy()),
        gensym: interp
            .symbol_value_key(cached_symbol!("print-gensym"))
            .is_some_and(|value| value.is_truthy()),
        integers_as_characters: interp
            .symbol_value_key(cached_symbol!("print-integers-as-characters"))
            .is_some_and(|value| value.is_truthy()),
        symbols_bare: interp
            .symbol_value_key(cached_symbol!("print-symbols-bare"))
            .is_some_and(|value| value.is_truthy()),
        length: interp
            .symbol_value_key(cached_symbol!("print-length"))
            .and_then(|value| value.integer_value())
            .and_then(|value| usize::try_from(value).ok()),
        level: interp
            .symbol_value_key(cached_symbol!("print-level"))
            .and_then(|value| value.integer_value())
            .and_then(|value| usize::try_from(value).ok()),
        quoted: interp
            .symbol_value_key(cached_symbol!("print-quoted"))
            .is_some_and(|value| value.is_truthy()),
        output_is_function: false,
    }
}

pub(crate) fn record_prin1_fields(interp: &Interpreter, id: u64) -> Option<Vec<Value>> {
    let record = interp.find_record(id)?;
    match record.kind {
        crate::lisp::eval::RecordKind::Thread
        | crate::lisp::eval::RecordKind::Mutex
        | crate::lisp::eval::RecordKind::ConditionVariable
        | crate::lisp::eval::RecordKind::Process
        | crate::lisp::eval::RecordKind::Obarray => None,
        _ => Some(
            std::iter::once(record.type_tag)
                .chain(record.slots.iter().cloned())
                .collect(),
        ),
    }
}

/// print.c:Fprint_preprocess: `print-number-table' for OBJECT when
/// `print-circle' is non-nil, numbering from zero.
pub(crate) fn print_preprocess(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let options = print_options(interp);
    if options.circle {
        interp.print_number_index = 0;
        object::print_preprocess(interp, env, *value, options)?;
    }
    Ok(Value::Nil)
}

pub(crate) fn symbol_name_looks_like_number(name: &str) -> bool {
    let bytes = name.as_bytes();
    let signed = matches!(bytes.first(), Some(b'+' | b'-'));
    let Some(first) = bytes.get(signed as usize).copied() else {
        return false;
    };
    if !first.is_ascii_digit() && first != b'.' {
        return false;
    }
    decimal_number_prefix(name).is_some_and(|prefix| prefix.len() == name.len())
        || crate::lisp::reader::parse_special_float_token(name).is_some()
}

fn render_integer_as_character(value: &Value, escape: bool) -> Option<String> {
    let code = match value.kind() {
        Kind::Integer(value) => value,
        Kind::BigInteger(value) => value.to_i64()?,
        _ => return None,
    };
    let codepoint = u32::try_from(code).ok()?;
    let ch = char::from_u32(codepoint)?;
    let body = match ch {
        '?' => "?".into(),
        ' ' => "\\s".into(),
        '\n' => "\\n".into(),
        '\r' => "\\r".into(),
        '\t' => "\\t".into(),
        '\u{0008}' => "\\b".into(),
        '\u{000C}' => "\\f".into(),
        '\'' | '"' | '\\' | ';' | '(' | ')' | '{' | '}' | '[' | ']' if escape => {
            format!("\\{ch}")
        }
        _ => {
            if matches!(code, 7 | 11 | 27 | 127) {
                return None;
            }
            match get_general_category(ch).abbreviation() {
                "Cc" | "Cf" | "Cn" | "Co" | "Cs" | "Mc" | "Me" | "Mn" | "Zl" | "Zp" | "Zs" => {
                    return None;
                }
                _ => ch.to_string(),
            }
        }
    };
    Some(format!("?{body}"))
}

pub(crate) fn render_princ_integer_as_character(value: &Value) -> Option<String> {
    render_integer_as_character(value, false)
}

pub(crate) fn render_prin1_symbol(
    symbol: &crate::lisp::types::SymbolName,
    options: PrintOptions,
    out: &mut PrintOutput,
) {
    let visible = crate::lisp::types::visible_symbol_name(symbol);
    // print.c: `#:' under `print-gensym' for a symbol not interned in the
    // initial obarray, else `##' for the empty name.
    let gensym = options.gensym
        && (symbol.cell().is_uninterned() || crate::lisp::types::is_private_obarray_symbol(symbol));
    if gensym {
        out.push_str("#:");
    } else if visible.is_empty() {
        out.push_str("##");
        return;
    }
    // print.c quotes a symbol's confusing characters only under
    // `escapeflag'; `princ' writes the name as it stands.
    if !options.escape {
        out.push_str(visible);
        return;
    }
    let first = visible.chars().next();
    let mut confusing =
        symbol_name_looks_like_number(visible) || first == Some('?') || first == Some('.');
    let quoted = |ch: char| {
        matches!(
            ch,
            '"' | '\\' | '\'' | ';' | '#' | '(' | ')' | ',' | '`' | '[' | ']'
        ) || ch <= ' '
            || ch == '\u{00A0}'
    };
    if visible.is_ascii() {
        for byte in visible.bytes() {
            if quoted(char::from(byte)) || confusing {
                out.push('\\');
                confusing = false;
            }
            out.push_code(u32::from(byte));
        }
        return;
    }
    let mut rendered = String::with_capacity(visible.len() + 2);
    for ch in visible.chars() {
        if quoted(ch) || confusing {
            rendered.push('\\');
            confusing = false;
        }
        rendered.push(ch);
    }
    out.push_str(&rendered);
}

/// print.c print_prune_string_charset: a string's `charset' properties
/// print only when `print-charset-text-property' is t, or when it is
/// `default' and some property is "unsafe" -- a non-ASCII character in
/// its span belongs (by CHAR_CHARSET) to a different charset.  The
/// decision is made once for the whole string.
pub(crate) fn charset_text_properties_print(
    interp: &Interpreter,
    env: &Env,
    codes: &[u32],
    props: &[StringPropertySpan],
) -> bool {
    let setting = interp
        .lookup_var_key(cached_symbol!("print-charset-text-property"), env)
        .unwrap_or(Value::Nil);
    if setting.is_nil() {
        return false;
    }
    if matches!(setting.kind(), Kind::T) {
        return true;
    }
    let ordered = interp.charset_priority_list();
    let head = interp.charset_non_preferred_head();
    props.iter().any(|span| {
        span.props.iter().any(|(name, value)| {
            if name != "charset" {
                return false;
            }
            let Ok(charset) = value.as_symbol() else {
                return true;
            };
            let expected = interp
                .charset_canonical_name(charset)
                .unwrap_or_else(|| charset.to_string());
            // CODES as fetch_string_char_advance reads them: a unibyte
            // string's bytes (0xE9 is U+00E9 there), a multibyte one's
            // characters, raw bytes included.
            codes
                .iter()
                .skip(span.start)
                .take(span.end.saturating_sub(span.start))
                .any(|&code| {
                    code >= 0x80
                        && char_charset_ordered(interp, &ordered, head.as_deref(), code).0
                            != expected
                })
        })
    })
}

/// `print_bool_vector' (print.c): the bits are packed eight to a byte,
/// low-order bit first, and each byte is written with the escape rules
/// `octalout' applies.
fn render_bool_vector_prin1(
    interp: &Interpreter,
    env: &Env,
    bits: &[bool],
    options: PrintOptions,
) -> String {
    let escape_newlines = interp
        .lookup_var_key(cached_symbol!("print-escape-newlines"), env)
        .is_some_and(|value| value.is_truthy());
    let escape_control = interp
        .lookup_var_key(cached_symbol!("print-escape-control-characters"), env)
        .is_some_and(|value| value.is_truthy());

    let size = bits.len();
    let real_size_in_bytes = size.div_ceil(8);
    let mut data = vec![0u8; real_size_in_bytes];
    for (index, bit) in bits.iter().enumerate() {
        if *bit {
            data[index / 8] |= 1 << (index % 8);
        }
    }
    let size_in_bytes = options
        .length
        .map_or(real_size_in_bytes, |limit| limit.min(real_size_in_bytes));

    let mut rendered = format!("#&{size}\"");
    for index in 0..size_in_bytes {
        let byte = data[index];
        if byte == b'\n' && escape_newlines {
            rendered.push_str("\\n");
        } else if byte == 0x0c && escape_newlines {
            rendered.push_str("\\f");
        } else if byte > 0o177 || (escape_control && byte.is_ascii_control()) {
            let digits = if byte > 0o77
                || data
                    .get(index + 1)
                    .is_some_and(|next| index + 1 < size_in_bytes && (b'0'..=b'7').contains(next))
            {
                3
            } else if byte > 0o7 {
                2
            } else {
                1
            };
            rendered.push('\\');
            for shift in (0..digits).rev() {
                rendered.push(char::from(b'0' + ((byte >> (3 * shift)) & 7)));
            }
        } else {
            if byte == b'"' || byte == b'\\' {
                rendered.push('\\');
            }
            rendered.push(char::from(byte));
        }
    }
    if size_in_bytes < real_size_in_bytes {
        rendered.push_str(" ...");
    }
    rendered.push('"');
    rendered
}

fn prepare_print_numbering(interp: &mut Interpreter, env: &mut Env, options: PrintOptions) {
    // print.c resets both pieces of state unless continuous numbering has a
    // live public table.  A nil table therefore starts numbering over even
    // when `print-continuous-numbering' itself is non-nil.  The reset is a
    // store into the C variable, which runs no watcher: a table already
    // nil is left as it is.
    let table = interp.symbol_value_key(cached_symbol!("print-number-table"));
    if !options.continuous_numbering || table.is_none_or(|value| value.is_nil()) {
        interp.print_number_index = 0;
        if table.is_none_or(|value| !value.is_nil()) {
            interp.set_variable("print-number-table", Value::Nil, env);
        }
    }
}

pub(crate) fn render_prin1_output(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<PrintOutput, LispError> {
    with_printer_buffer_escape(interp, env, Some(true), |interp, env| {
        render_printer_output(interp, value, env, true, false)
    })
}

pub(crate) fn render_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    with_printer_buffer_escape(interp, env, Some(true), |interp, env| {
        render_printer_object(interp, value, env, true, false)
    })
}

/// keyboard.c:cmd_error: `print-level' and `print-length' are 10 while
/// cmd_error_internal reports the error ("Avoid unquittable loop if data
/// contains a circular list"), assigned and assigned back, as C does, when
/// the report returns.
pub(crate) fn with_cmd_error_print_limits<T>(
    interp: &mut Interpreter,
    report: impl FnOnce(&mut Interpreter) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let old_level = interp
        .symbol_value_key(cached_symbol!("print-level"))
        .unwrap_or(Value::Nil);
    let old_length = interp
        .symbol_value_key(cached_symbol!("print-length"))
        .unwrap_or(Value::Nil);
    interp.set_c_variable("print-level", Value::Integer(10));
    interp.set_c_variable("print-length", Value::Integer(10));
    let result = report(interp)?;
    interp.set_c_variable("print-level", old_level);
    interp.set_c_variable("print-length", old_length);
    Ok(result)
}

/// print.c:print_prepare binds the escape flag required by a buffer's
/// encoding. Function and stdout streams do not impose either flag.
pub(crate) fn with_printer_buffer_escape<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    multibyte: Option<bool>,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let Some(multibyte) = multibyte else {
        return body(interp, env);
    };
    // `! print_escape_nonascii': the C variable, by the symbol interned
    // once, not its name.
    let key = if multibyte {
        cached_symbol!("print-escape-nonascii")
    } else {
        cached_symbol!("print-escape-multibyte")
    };
    if interp
        .symbol_value_key(key)
        .is_some_and(|value| value.is_truthy())
    {
        return body(interp, env);
    }
    let restore = key.with(|symbol| interp.bind_special_symbol(symbol, Value::T, env))?;
    let result = body(interp, env);
    let restored = interp.restore_special_dynamic(restore, env);
    restored.and(result)
}

pub(crate) fn render_printer_object(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    escape: bool,
    output_is_function: bool,
) -> Result<String, LispError> {
    render_printer_output(interp, value, env, escape, output_is_function)
        .map(|output| output.host_text())
}

pub(crate) fn render_printer_output(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    escape: bool,
    output_is_function: bool,
) -> Result<PrintOutput, LispError> {
    let mut options = print_options(interp);
    options.escape = escape;
    options.output_is_function = output_is_function;
    object::print(interp, env, *value, options)
}

/// `princ': print.c's `print_object' with `escapeflag' cleared.  This is the
/// same traversal `prin1' uses, so the flag reaches nested elements.
pub(crate) fn render_princ_object(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    render_printer_object(interp, value, env, false, false)
}

/// `prin1' of VALUE as a Rust text view and its side table.
pub(crate) fn render_prin1_parts_ephemeral(
    interp: &mut Interpreter,
    value: &Value,
    env: &crate::lisp::types::Env,
) -> Result<(String, Vec<(usize, u32)>), LispError> {
    let mut env = env.clone();
    render_prin1_output(interp, value, &mut env).map(|output| output.text_parts())
}

pub(crate) fn render_prin1_ephemeral(
    interp: &mut Interpreter,
    value: &Value,
    env: &crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut env = env.clone();
    render_prin1(interp, value, &mut env)
}

/// lread.c:end_of_file_error: `(end-of-file FILE)' while a file is being
/// loaded (`load-true-file-name' a string), else `(end-of-file)'.
/// A reader's end-of-input as lread.c's end_of_file_error signals it;
/// any other error is ERROR.
pub(crate) fn reader_end_of_file(interp: &Interpreter, error: LispError, env: &Env) -> LispError {
    if matches!(error.kind(), LispErrorKind::EndOfInput) {
        end_of_file_error(interp, env)
    } else {
        error
    }
}

/// Whether ERROR is an `end-of-file' signal.
pub(crate) fn is_end_of_file_error(error: &LispError) -> bool {
    match error.kind() {
        LispErrorKind::EndOfInput => true,
        LispErrorKind::SignalValue(data) => {
            matches!(data.car().map(|head| head.kind()), Ok(Kind::Symbol(name)) if name == "end-of-file")
        }
        _ => false,
    }
}

fn end_of_file_error(interp: &Interpreter, env: &Env) -> LispError {
    match interp
        .lookup_var("load-true-file-name", env)
        .map(|v| v.kind())
    {
        Some(file @ Kind::StringObject(_)) => LispError::SignalValue(Value::list([
            Value::Symbol("end-of-file".into()),
            file.value(),
        ])),
        _ => LispError::EndOfInput(),
    }
}

/// lread.c:readchar returns bytes 0..255 from an unibyte string, rather
/// than the byte8 character codes it returns from an unibyte buffer.
/// The reader must therefore see literal Latin-1 characters in this source;
/// multibyte raw-byte characters and explicit escapes keep their meaning.
pub(crate) fn reader_string_source_text(source: &Value) -> Result<String, LispError> {
    let text = string_text(source)?;
    if string_argument_multibyte(source) || text.is_ascii() {
        return Ok(text);
    }
    Ok(text
        .chars()
        .map(|character| raw_byte_from_regex_char(character).map_or(character, char::from))
        .collect())
}

pub(crate) fn read_encoded_form_in_env(
    interp: &mut Interpreter,
    bytes: &[u8],
    multibyte: bool,
    env: &mut Env,
) -> Result<(Value, usize), LispError> {
    read_one_from_reader(
        interp,
        LispReaderInput::Encoded(bytes, multibyte),
        &mut 0,
        env,
    )
}

pub(crate) enum LispReaderInput<'a> {
    Text(&'a str),
    Encoded(&'a [u8], bool),
    /// A buffer's text, read in place.
    Rope(ropey::RopeSlice<'a>),
}

fn read_one_from_reader(
    interp: &mut Interpreter,
    input: LispReaderInput<'_>,
    stopped: &mut usize,
    env: &mut Env,
) -> Result<(Value, usize), LispError> {
    // lread.c resolves each atom in the selected obarray. A post-read walk
    // would first intern a private name in the standard table and leak it.
    let shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let read = (|| {
        let mut context = crate::lisp::eval::LispReadContext::current(interp, env);
        let reader = match input {
            LispReaderInput::Text(text) => {
                crate::lisp::reader::Reader::with_symbol_shorthands(text, shorthands)
            }
            LispReaderInput::Encoded(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded(bytes, multibyte, shorthands)
            }
            LispReaderInput::Rope(rope) => crate::lisp::reader::Reader::from_rope(rope, shorthands),
        };
        let mut reader = reader.with_read_context(&mut context);
        let value = match reader.read() {
            Ok(value) => value,
            Err(error) => {
                *stopped = reader.characters_consumed();
                return Err(error);
            }
        };
        let unescaped = reader
            .unescaped_character_literals()
            .map(Value::Integer)
            .collect::<Vec<_>>();
        Ok((
            value,
            reader.emitted_reader_forms(),
            unescaped,
            reader.characters_consumed(),
        ))
    })();
    let (value, needs_materialization, unescaped, consumed) =
        read.map_err(|error| reader_end_of_file(interp, error, env))?;
    let value = value.ok_or_else(|| end_of_file_error(interp, env))?;
    // Allocate identity-bearing literals before resolving references into them.
    let value = if needs_materialization {
        interp.materialize_read_object_literals(value, env)?
    } else {
        value
    };
    // read_char_literal conses each new literal onto the variable, which
    // `load' (or the caller) binds; `read' itself never resets it.
    record_unescaped_character_literals(
        interp,
        unescaped.iter().filter_map(Value::integer_value),
        env,
    );
    Ok((value, consumed))
}

/// A buffer's text from START to END as the reader reads it, the way
/// lread.c's readchar takes each character from the buffer.  The rope is
/// read in place (a clone shares it) unless the range holds a character
/// it cannot carry (one beyond Unicode, or a real U+E000..U+E0FF): then
/// the reader takes the range's internal multibyte bytes.
pub(crate) enum BufferReadSource {
    /// The rope and the byte range of the text in it.
    Rope(ropey::Rope, std::ops::Range<usize>),
    Encoded(Vec<u8>),
}

impl BufferReadSource {
    pub(crate) fn new(
        interp: &Interpreter,
        buffer_id: u64,
        start: usize,
        end: usize,
    ) -> Result<Self, LispError> {
        let buffer = interp
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
        // As buffer_substring takes the range: ordered, then clipped.
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        let (start, end) = (start.max(buffer.point_min()), end.min(buffer.point_max()));
        if start > end {
            return Err(LispError::Signal(
                crate::buffer::BufferError::InvalidPosition(start).to_string(),
            ));
        }
        if !buffer.has_extended_chars_in(start, end) {
            let rope = buffer.text_rope();
            let range = rope.char_to_byte(start - 1)..rope.char_to_byte(end - 1);
            return Ok(Self::Rope(rope, range));
        }
        let text = buffer
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let extended = buffer.substring_extended_chars(start, end);
        Ok(Self::Encoded(crate::lisp::types::string_data::encode_text(
            &text, true, &extended,
        )?))
    }

    pub(crate) fn input(&self) -> LispReaderInput<'_> {
        self.input_from(0)
    }

    /// The source from byte OFFSET on.
    pub(crate) fn input_from(&self, offset: usize) -> LispReaderInput<'_> {
        match self {
            Self::Rope(rope, range) => {
                LispReaderInput::Rope(rope.byte_slice(range.start + offset..range.end))
            }
            Self::Encoded(bytes) => LispReaderInput::Encoded(&bytes[offset..], true),
        }
    }

    /// The characters before byte OFFSET.
    pub(crate) fn characters_before(&self, offset: usize) -> usize {
        match self {
            Self::Rope(rope, range) => {
                rope.byte_to_char(range.start + offset) - rope.byte_to_char(range.start)
            }
            Self::Encoded(bytes) => {
                let mut position = 0;
                let mut count = 0;
                while position < offset {
                    position +=
                        crate::lisp::types::string_data::multibyte_length(&bytes[position..], true)
                            .max(1);
                    count += 1;
                }
                count
            }
        }
    }
}

/// lread.c invalid_syntax_lisp with a buffer READCHARFUN: the data of
/// an `invalid-read-syntax' gains the line and column of the buffer's
/// point, where reading stopped (count_lines from BEGV, plus one, and
/// current_column, both in that buffer).  Any other error is ERROR.
pub(crate) fn invalid_syntax_in_buffer(
    interp: &mut Interpreter,
    buffer_id: u64,
    error: LispError,
    env: &mut Env,
) -> LispError {
    let LispErrorKind::ReadError(message) = error.kind() else {
        return error;
    };
    let message = message.clone();
    let current = interp.current_buffer_id();
    if current != buffer_id && interp.set_current_buffer_id(buffer_id).is_err() {
        return error;
    }
    let line = {
        let buffer = interp.buffer.borrow();
        buffer
            .buffer_substring(buffer.point_min(), buffer.point())
            .map(|text| text.matches('\n').count())
            .unwrap_or(0)
            + 1
    };
    let column = call(interp, "current-column", &[], env).unwrap_or(Value::Integer(0));
    if current != buffer_id {
        let _ = interp.set_current_buffer_id(current);
    }
    LispError::SignalValue(Value::list([
        Value::Symbol("invalid-read-syntax".into()),
        Value::String(message.into()),
        Value::Integer(line as i64),
        column,
    ]))
}

pub(crate) fn read_positioning_symbols_from_lisp_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match source.kind() {
        Kind::Buffer(_) => {
            let buffer_id = interp.resolve_buffer_id(source)?;
            let (start, end) = {
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                (buffer.point(), buffer.point_max())
            };
            let text = BufferReadSource::new(interp, buffer_id, start, end)?;
            let mut stopped = 0;
            let result =
                read_one_positioned_form(interp, env, text.input(), start as i64, &mut stopped);
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(_) if result.as_ref().is_err_and(is_end_of_file_error) => end - start,
                Err(_) => stopped,
            };
            if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            result
                .map(|(value, _)| value)
                .map_err(|error| invalid_syntax_in_buffer(interp, buffer_id, error, env))
        }
        Kind::Marker(id) => {
            let (buffer_id, start) = {
                let marker = id;
                let buffer_id = marker
                    .buffer()
                    .map(|buffer| buffer.id)
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                let start = marker
                    .position()
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                (buffer_id, start)
            };
            let end = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
                .point_max();
            let text = BufferReadSource::new(interp, buffer_id, start, end)?;
            // lread.c:read_internal_start uses an absolute base only for
            // BUFFERP. Marker streams count from zero for each read.
            let mut stopped = 0;
            let result = read_one_positioned_form(interp, env, text.input(), 0, &mut stopped);
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(_) if result.as_ref().is_err_and(is_end_of_file_error) => end - start,
                Err(_) => stopped,
            };
            interp.set_marker(id, Some((start + consumed).min(end)), Some(buffer_id))?;
            result.map(|(value, _)| value)
        }
        Kind::StringObject(_) => {
            let text = reader_string_source_text(source)?;
            read_one_positioned_form(interp, env, LispReaderInput::Text(&text), 0, &mut 0)
                .map(|(value, _)| value)
        }
        // lread.c:readchar calls the remaining stream objects, including
        // bytecode closures, through the ordinary function dispatcher.
        _ => read_callable_source(interp, source, env, true),
    }
}

fn read_one_positioned_form(
    interp: &mut Interpreter,
    env: &mut Env,
    input: LispReaderInput<'_>,
    base_position: i64,
    stopped: &mut usize,
) -> Result<(Value, usize), LispError> {
    // read0 with LOCATE_SYMS: the reader itself wraps each symbol
    // occurrence with its character position (the retired token-stream
    // zip desynced on any non-symbol token — a number, `t' — and then
    // silently dropped every later position; bytecomp warnings inherited
    // the enclosing defun's position instead of the offending form's).
    let symbol_shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let read = (|| {
        let mut context = crate::lisp::eval::LispReadContext::current(interp, env);
        let reader = match input {
            LispReaderInput::Text(text) => crate::lisp::reader::Reader::with_positioned_symbols(
                text,
                symbol_shorthands,
                base_position,
            ),
            LispReaderInput::Encoded(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded_positioned(
                    bytes,
                    multibyte,
                    symbol_shorthands,
                    base_position,
                )
            }
            LispReaderInput::Rope(rope) => crate::lisp::reader::Reader::from_rope_positioned(
                rope,
                symbol_shorthands,
                base_position,
            ),
        };
        let mut reader = reader.with_read_context(&mut context);
        let value = match reader.read() {
            Ok(value) => value,
            Err(error) => {
                *stopped = reader.characters_consumed();
                return Err(error);
            }
        };
        let consumed = reader.characters_consumed();
        let unescaped = reader
            .unescaped_character_literals()
            .map(Value::Integer)
            .collect::<Vec<_>>();
        Ok((value, consumed, unescaped, reader.emitted_reader_forms()))
    })();
    let (value, consumed, unescaped, needs_materialization) =
        read.map_err(|error| reader_end_of_file(interp, error, env))?;
    let value = value.ok_or_else(|| end_of_file_error(interp, env))?;
    // read_char_literal conses each new literal onto the variable, which
    // `load' (or the caller) binds; `read' itself never resets it.
    record_unescaped_character_literals(
        interp,
        unescaped.iter().filter_map(Value::integer_value),
        env,
    );
    // GNU's reader constructs `#s(...)', `#^[...]', and bool-vector objects
    // before read-positioning-symbols returns.  In particular, the byte
    // compiler must receive an actual hash table constant rather than
    // Emaxx's parser-private ReaderForm marker.  The reader builds them
    // itself unless a circular label made it leave placeholders.
    let value = if needs_materialization {
        interp.materialize_read_object_literals(value, env)?
    } else {
        value
    };
    Ok((value, consumed))
}

pub(crate) fn record_literal_items(value: &Value) -> Option<Vec<Value>> {
    let Kind::ReaderForm(form) = value.kind() else {
        return None;
    };
    let crate::lisp::types::ReaderForm::Record { slots } = form.as_ref() else {
        return None;
    };
    Some(
        std::iter::once(Value::Nil)
            .chain(slots.iter().cloned())
            .collect(),
    )
}

pub(crate) fn record_literal_slot_data(value: &Value) -> Value {
    if let Ok(items) = value.to_vec()
        && let [Kind::Symbol(symbol), inner] = items
            .as_slice()
            .iter()
            .map(|v| v.kind())
            .collect::<Vec<_>>()
            .as_slice()
        && symbol == "quote"
    {
        return inner.value();
    }
    *value
}

pub(crate) fn record_literal_aref(
    object: &Value,
    items: &[Value],
    idx: usize,
    idx_value: &Value,
) -> Result<Value, LispError> {
    let slot = items.get(idx + 1).cloned().ok_or_else(|| {
        LispError::SignalValue(Value::list([
            Value::Symbol("args-out-of-range".into()),
            *object,
            *idx_value,
        ]))
    })?;
    Ok(record_literal_slot_data(&slot))
}

pub(crate) fn read_from_callable_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    read_callable_source(interp, source, env, false)
}

struct CallableReader<'a> {
    interp: &'a mut Interpreter,
    source: Value,
    env: &'a mut Env,
}

impl CallableReader<'_> {
    fn call(&mut self, args: &[Value]) -> Result<Value, LispError> {
        // GNU call0/call1 resolve a symbol stream on every invocation. The
        // callback can redefine that function, including during unread.
        let callable = resolve_callable(self.interp, &self.source, self.env)?;
        self.interp
            .call_function_value(callable, self.source.symbol_str(), args, self.env)
    }
}

impl crate::lisp::reader::ReadContext for CallableReader<'_> {
    fn intern(&mut self, name: &str) -> Result<Value, LispError> {
        crate::lisp::reader::ReaderStream::intern_symbol(self, name, false)
    }

    fn build(&mut self, form: crate::lisp::types::ReaderForm) -> Result<Value, LispError> {
        self.interp.build_read_object(form, self.env)
    }

    fn load_file_name(&mut self) -> Value {
        self.interp.read_load_file_name(self.env)
    }

    fn force_doc_strings(&mut self) -> bool {
        self.interp.read_force_doc_strings(self.env)
    }

    fn doc_string(&mut self, reference: Value) -> Result<Value, LispError> {
        crate::lisp::primitives::unibyte_doc_string(self.interp, &reference, self.env)
    }

    fn char_from_name(&mut self, name: &str) -> Result<Value, LispError> {
        self.interp.read_char_from_name(name, self.env)
    }
}

impl crate::lisp::reader::ReaderStream for CallableReader<'_> {
    fn read_character(&mut self) -> Result<Option<i64>, LispError> {
        match self.call(&[])?.kind() {
            Kind::Nil => Ok(None),
            Kind::Integer(code) if code < 0 => Ok(None),
            Kind::Integer(code) => Ok(Some(code)),
            other => Err(LispError::WrongTypeArgument(
                "integerp".into(),
                other.value(),
            )),
        }
    }

    fn unread_character(&mut self, character: i64) -> Result<(), LispError> {
        self.call(&[Value::Integer(character)]).map(|_| ())
    }

    fn intern_symbol(&mut self, name: &str, shorthand: bool) -> Result<Value, LispError> {
        let name = if shorthand {
            let shorthands = read_symbol_shorthands_in_env(self.interp, self.env)?;
            crate::lisp::reader::apply_symbol_shorthands_to_token(name.to_owned(), &shorthands)
        } else {
            name.to_owned()
        };
        let obarray = self
            .interp
            .lookup_var("obarray", self.env)
            .unwrap_or(Value::Nil);
        intern_in_obarray(self.interp, &obarray, &name)
    }
}

fn read_callable_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
    locate_symbols: bool,
) -> Result<Value, LispError> {
    let (value, unescaped, needs_materialization) = {
        let mut stream = CallableReader {
            interp,
            source: *source,
            env,
        };
        let mut reader = crate::lisp::reader::Reader::from_stream(&mut stream, locate_symbols);
        let value = reader.read()?;
        reader.finish_stream()?;
        let unescaped = reader
            .unescaped_character_literals()
            .map(Value::Integer)
            .collect::<Vec<_>>();
        (value, unescaped, reader.emitted_reader_forms())
    };
    let value = value.ok_or_else(|| end_of_file_error(interp, env))?;
    // read_char_literal conses each new literal onto the variable, which
    // `load' (or the caller) binds; `read' itself never resets it.
    record_unescaped_character_literals(
        interp,
        unescaped.iter().filter_map(Value::integer_value),
        env,
    );
    if needs_materialization {
        interp.materialize_read_object_literals(value, env)
    } else {
        Ok(value)
    }
}

pub(crate) fn read_from_lisp_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    read_from_lisp_source_raw(interp, source, env)
}

// GNU's reader constructs real hash tables for `#s(hash-table ...)' input;
// emaxx's reader leaves a quoted literal form behind, which is fine for
// loaded code but wrong for `read' consumers that treat the result as data
// (e.g. `eieio-persistent-read').  Convert those literals into hash-table
// records after reading.
pub(crate) fn materialize_read_hash_table_literals(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut seen = HashSet::new();
    materialize_hash_table_literals_inner(interp, value, env, &mut seen)
}

const CHAR_TABLE_STANDARD_SLOTS: usize = 68;

// Reader forms become the actual root/subtable graph. Each object is
// published before its fields so repeated references keep their identity.
pub(crate) fn materialize_read_char_table_literals(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    materialize_char_table_literals_inner(interp, value, env, &mut HashMap::new())
}

/// lread.c's char-table and sub-char-table construction (`#^[...]' and
/// `#^^[...]'), from fields the reader has read.
pub(crate) fn char_table_from_read_fields(
    fields: &[Value],
    sub_table: bool,
) -> Result<Value, LispError> {
    let (table, skip) = if sub_table {
        let depth = fields[0].as_integer()? as usize;
        let minimum = fields[1].as_integer()? as u32;
        (
            Value::SubCharTable(crate::lisp::types::SubCharTableRef::new(
                depth,
                minimum,
                Value::Nil,
            )),
            2,
        )
    } else {
        if !(CHAR_TABLE_STANDARD_SLOTS
            ..=crate::lisp::alloc::vectors::char_tables::CHAR_TABLE_MAX_SLOTS)
            .contains(&fields.len())
        {
            return Err(LispError::ReadError("Invalid size char-table".into()));
        }
        (
            Value::CharTable(crate::lisp::types::CharTableRef::new(
                Value::Nil,
                Value::Nil,
                fields.len() - CHAR_TABLE_STANDARD_SLOTS,
            )),
            0,
        )
    };
    for (index, field) in fields.iter().skip(skip).enumerate() {
        match table.kind() {
            Kind::CharTable(table) => table.set_slot(index, *field),
            Kind::SubCharTable(table) => table.set_slot(index, *field),
            _ => unreachable!(),
        }
    }
    Ok(table)
}

fn materialize_char_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    seen: &mut HashMap<usize, Value>,
) -> Result<Value, LispError> {
    if let Some(copy) = seen.get(&value.word()) {
        return Ok(*copy);
    }
    if let Kind::ReaderForm(form) = value.kind() {
        let (copy, fields, skip) = match form.as_ref() {
            crate::lisp::types::ReaderForm::CharTable { fields } => {
                if !(CHAR_TABLE_STANDARD_SLOTS
                    ..=crate::lisp::alloc::vectors::char_tables::CHAR_TABLE_MAX_SLOTS)
                    .contains(&fields.len())
                {
                    return Err(LispError::ReadError("Invalid size char-table".into()));
                }
                (
                    Value::CharTable(crate::lisp::types::CharTableRef::new(
                        Value::Nil,
                        Value::Nil,
                        fields.len() - CHAR_TABLE_STANDARD_SLOTS,
                    )),
                    fields,
                    0,
                )
            }
            crate::lisp::types::ReaderForm::SubCharTable { fields } => {
                let depth = fields[0].as_integer()? as usize;
                let minimum = fields[1].as_integer()? as u32;
                (
                    Value::SubCharTable(crate::lisp::types::SubCharTableRef::new(
                        depth,
                        minimum,
                        Value::Nil,
                    )),
                    fields,
                    2,
                )
            }
            _ => return Ok(*value),
        };
        seen.insert(value.word(), copy);
        for (index, field) in fields.iter().skip(skip).enumerate() {
            let field = interp.materialize_read_record_literals(field, env)?;
            let field = materialize_read_hash_table_literals(interp, &field, env)?;
            let field = materialize_char_table_literals_inner(interp, &field, env, seen)?;
            match copy.kind() {
                Kind::CharTable(table) => table.set_slot(index, field),
                Kind::SubCharTable(table) => table.set_slot(index, field),
                _ => unreachable!(),
            }
        }
        return Ok(copy);
    }
    seen.insert(value.word(), *value);
    match value.kind() {
        Kind::Vector(vector) => {
            for (index, field) in vector.slots().enumerate() {
                vector.set(
                    index,
                    materialize_char_table_literals_inner(interp, &field, env, seen)?,
                );
            }
        }
        Kind::Cons(cell) => {
            cell.car.set(materialize_char_table_literals_inner(
                interp,
                &cell.car.get(),
                env,
                seen,
            )?);
            cell.cdr.set(materialize_char_table_literals_inner(
                interp,
                &cell.cdr.get(),
                env,
                seen,
            )?);
        }
        Kind::CharTable(table) => {
            for (index, field) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    materialize_char_table_literals_inner(interp, &field, env, seen)?,
                );
            }
        }
        Kind::SubCharTable(table) => {
            for (index, field) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    materialize_char_table_literals_inner(interp, &field, env, seen)?,
                );
            }
        }
        _ => {}
    }
    Ok(*value)
}

fn quoted_hash_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    let kinds = items.iter().map(|v| v.kind()).collect::<Vec<_>>();
    let [Kind::Symbol(head), literal] = kinds.as_slice() else {
        return None;
    };
    if head != "quote" {
        return None;
    }
    match *literal {
        Kind::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::HashTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn bare_hash_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    match value.kind() {
        Kind::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::HashTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn materialize_hash_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    if let Some(fields) = quoted_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, env, seen);
    }
    if let Some(fields) = bare_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, env, seen);
    }
    if let Kind::Vector(vector) = value.kind() {
        if !seen.insert(vector.identity()) {
            return Ok(*value);
        }
        let slots = vector.slots().collect::<Vec<_>>();
        for (index, slot) in slots.iter().enumerate() {
            vector.set(
                index,
                materialize_hash_table_literals_inner(interp, slot, env, seen)?,
            );
        }
        return Ok(*value);
    }
    let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
        return Ok(*value);
    };
    let ptr = car_cell.cell_id();
    if !seen.insert(ptr) {
        return Ok(*value);
    }
    let car = car_cell.get();
    let new_car = materialize_hash_table_literals_inner(interp, &car, env, seen)?;
    car_cell.set(new_car);
    let cdr = cdr_cell.get();
    let new_cdr = materialize_hash_table_literals_inner(interp, &cdr, env, seen)?;
    cdr_cell.set(new_cdr);
    Ok(*value)
}

fn hash_table_from_literal_fields(
    interp: &mut Interpreter,
    fields: &[Value],
    env: &mut Env,
    _seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    hash_table_from_plist(interp, fields, env, true)
}

/// lread.c:hash_table_from_plist over the fields of a `#s(hash-table ...)'
/// the reader has just read, its data already the objects they read as.
pub(crate) fn hash_table_from_read_fields(
    interp: &mut Interpreter,
    fields: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    hash_table_from_plist(interp, fields, env, false)
}

/// lread.c:hash_table_from_plist; MATERIALIZE finishes reader forms left in
/// the data by a read that deferred them.
fn hash_table_from_plist(
    interp: &mut Interpreter,
    fields: &[Value],
    env: &mut Env,
    materialize: bool,
) -> Result<Value, LispError> {
    let mut test = "eql".to_string();
    let mut weakness = Value::Nil;
    let mut purecopy = Value::Nil;
    let mut entries = Vec::new();
    let mut index = 0usize;
    while index + 1 < fields.len() {
        let Ok(key) = fields[index].as_symbol() else {
            index += 2;
            continue;
        };
        let key = key.to_string();
        let field_value = fields[index + 1];
        match key.as_str() {
            "test" => test = field_value.as_symbol()?.to_string(),
            "weakness" => weakness = field_value,
            "purecopy" => purecopy = field_value,
            "data" => {
                let items = field_value.to_vec()?;
                let mut cursor = 0usize;
                while cursor + 1 < items.len() {
                    let (entry_key, entry_value) = if materialize {
                        (
                            interp.materialize_read_object_literals(items[cursor], env)?,
                            interp.materialize_read_object_literals(items[cursor + 1], env)?,
                        )
                    } else {
                        (items[cursor], items[cursor + 1])
                    };
                    entries.push((entry_key, entry_value));
                    cursor += 2;
                }
            }
            _ => {}
        }
        index += 2;
    }
    // lread.c:hash_table_from_plist ignores serialized size/rehash metadata,
    // creates the table with exactly one slot per DATA pair, then calls
    // Fputhash for every pair.  In particular, a user-defined test runs its
    // hash/comparison functions here, while the object is read; delaying that
    // work until the first lookup changes both side effects and bucket state.
    let capacity = entries.len();
    let table = make_hash_table_value(
        interp,
        Value::symbol(&test),
        capacity,
        weakness,
        purecopy,
        env,
    )?;
    let object = check_hash_table(&table)?;
    for (key, value) in entries {
        hash_table_put(interp, object, key, value, env)?;
    }
    Ok(table)
}

fn read_from_lisp_source_raw(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match source.kind() {
        Kind::Buffer(_) => {
            let buffer_id = interp.resolve_buffer_id(source)?;
            let (start, end) = {
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                (buffer.point(), buffer.point_max())
            };
            let text = BufferReadSource::new(interp, buffer_id, start, end)?;
            let mut stopped = 0;
            let result = read_one_from_reader(interp, text.input(), &mut stopped, env);
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(_) if result.as_ref().is_err_and(is_end_of_file_error) => end - start,
                Err(_) => stopped,
            };
            if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            result
                .map(|(value, _)| value)
                .map_err(|error| invalid_syntax_in_buffer(interp, buffer_id, error, env))
        }
        Kind::Marker(id) => {
            let (buffer_id, start) = {
                let marker = id;
                let buffer_id = marker
                    .buffer()
                    .map(|buffer| buffer.id)
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                let start = marker
                    .position()
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                (buffer_id, start)
            };
            let end = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
                .point_max();
            let text = BufferReadSource::new(interp, buffer_id, start, end)?;
            let mut stopped = 0;
            let result = read_one_from_reader(interp, text.input(), &mut stopped, env);
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(_) if result.as_ref().is_err_and(is_end_of_file_error) => end - start,
                Err(_) => stopped,
            };
            interp.set_marker(id, Some((start + consumed).min(end)), Some(buffer_id))?;
            result.map(|(value, _)| value)
        }
        Kind::StringObject(state) => {
            let contents = state.borrow();
            read_encoded_form_in_env(interp, contents.bytes(), contents.is_multibyte(), env)
                .map(|(value, _)| value)
        }
        _ => read_from_callable_source(interp, source, env),
    }
}

/// fns.c:extract_data_from_object's MD5 coding policy. String endpoints
/// address the encoded string; buffer endpoints address the narrowed buffer.
pub(crate) fn md5_source_bytes(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    let source = &args[0];
    let start = args.get(1).unwrap_or(&Value::Nil);
    let end = args.get(2).unwrap_or(&Value::Nil);
    let mut coding = args.get(3).cloned().unwrap_or(Value::Nil);
    let noerror = args.get(4).is_some_and(Value::is_truthy);
    let validate = |interp: &Interpreter, coding: Value| -> Result<Value, LispError> {
        if coding.is_nil()
            || coding
                .as_symbol()
                .ok()
                .is_some_and(|name| interp.has_coding_system(name))
        {
            Ok(coding)
        } else if noerror {
            Ok(Value::symbol("raw-text"))
        } else {
            Err(LispError::SignalValue(Value::list([
                Value::symbol("coding-system-error"),
                coding,
            ])))
        }
    };
    let buffer_source = matches!(source.kind(), Kind::Buffer(_));
    let object = if buffer_source {
        let id = interp.resolve_buffer_id(source)?;
        let saved = interp.current_buffer_id();
        interp.set_current_buffer_id(id)?;
        let result = (|| {
            let buffer = interp.get_buffer_by_id(id).expect("resolved live buffer");
            let mut b = if start.is_nil() {
                buffer.point_min()
            } else {
                position_from_value(interp, start)?
            };
            let mut e = if end.is_nil() {
                buffer.point_max()
            } else {
                position_from_value(interp, end)?
            };
            if b > e {
                std::mem::swap(&mut b, &mut e);
            }
            if b < buffer.point_min() || e > buffer.point_max() {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("args-out-of-range"),
                    *start,
                    *end,
                ])));
            }
            let multibyte = buffer.is_multibyte();
            drop(buffer);
            let b = Value::Integer(b as i64);
            let e = Value::Integer(e as i64);
            if coding.is_nil() {
                coding = interp
                    .lookup_var("coding-system-for-write", env)
                    .unwrap_or(Value::Nil);
                if coding.is_nil() {
                    let default = interp
                        .lookup_var("buffer-file-coding-system", env)
                        .unwrap_or(Value::Nil);
                    coding = default;
                    let local = call(
                        interp,
                        "local-variable-p",
                        &[Value::symbol("buffer-file-coding-system")],
                        env,
                    )?
                    .is_truthy();
                    let force_raw = (coding.is_nil() || !local) && !multibyte;
                    if !local {
                        coding = Value::Nil;
                    }
                    let filename = call(
                        interp,
                        "buffer-file-name",
                        std::slice::from_ref(source),
                        env,
                    )?;
                    if coding.is_nil() && filename.is_truthy() {
                        let selected = interp.call_function_value(
                            Value::symbol("find-operation-coding-system"),
                            Some("find-operation-coding-system"),
                            &[Value::symbol("write-region"), b, e, filename],
                            env,
                        )?;
                        if let Some((_, value)) = selected.cons_values()
                            && value.is_truthy()
                        {
                            coding = value;
                        }
                    }
                    if coding.is_nil() {
                        coding = default;
                    }
                    let selector = interp
                        .lookup_var("select-safe-coding-system-function", env)
                        .unwrap_or(Value::Nil);
                    if !force_raw
                        && call(interp, "fboundp", std::slice::from_ref(&selector), env)?
                            .is_truthy()
                    {
                        coding = interp.call_function_value(
                            selector,
                            None,
                            &[b, e, coding, Value::Nil],
                            env,
                        )?;
                    }
                    if force_raw {
                        coding = Value::symbol("raw-text");
                    }
                }
                coding = validate(interp, coding)?;
            }
            call(interp, "buffer-substring-no-properties", &[b, e], env)
        })();
        if interp.has_buffer_id(saved) {
            interp.set_current_buffer_id(saved)?;
        }
        result?
    } else if let Some(string) = string_like(source) {
        if coding.is_nil() {
            coding = if string.multibyte {
                interp
                    .coding_system_priority_list()
                    .first()
                    .map(|name| Value::symbol(name))
                    .unwrap_or(Value::Nil)
            } else {
                Value::symbol("raw-text")
            };
        }
        coding = validate(interp, coding)?;
        *source
    } else if matches!(source.kind(), Kind::Symbol(name) if name == "iv-auto") {
        if !matches!(start.kind(), Kind::Integer(number) if number >= 0) {
            return Err(LispError::Signal(
                "Without a length, `iv-auto' can't be used; see ELisp manual".into(),
            ));
        }
        return crate::lisp::primitives::text::secure_hash_source_bytes(
            interp,
            source,
            Some(start),
            Some(end),
        );
    } else {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("error"),
            Value::string("Invalid object argument"),
            if source.is_nil() {
                Value::string("nil")
            } else {
                *source
            },
        ])));
    };
    let object = if string_like(&object).expect("extracted string").multibyte {
        // Explicit buffer coding is checked by code_convert_string only
        // when the extracted string is multibyte. NOERROR does not alter
        // that check, and the requested alias is what gets recorded.
        let coding_name = if coding.is_nil() {
            None
        } else {
            let name = coding.as_symbol()?;
            if !interp.has_coding_system(name) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("coding-system-error"),
                    coding,
                ])));
            }
            Some(name)
        };
        encode_coding_value_recording(interp, &object, coding_name, false, buffer_source, env)?
    } else {
        object
    };
    let bytes = crate::lisp::primitives::text::internal_string_bytes(
        &string_like(&object).expect("encoded string"),
    )?;
    if buffer_source {
        return Ok(bytes);
    }
    let endpoint = |value: &Value, default: i64| -> Result<i64, LispError> {
        match value.kind() {
            Kind::Nil => Ok(default),
            Kind::Integer(index) => Ok(if index < 0 {
                index + bytes.len() as i64
            } else {
                index
            }),
            _ => Err(LispError::WrongTypeArgument("integerp".into(), *value)),
        }
    };
    let b = endpoint(start, 0)?;
    let e = endpoint(end, bytes.len() as i64)?;
    if b < 0 || b > e || e > bytes.len() as i64 {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            object,
            *start,
            *end,
        ])));
    }
    Ok(bytes[b as usize..e as usize].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn preprocess_env(interp: &mut Interpreter, table: Value) -> Env {
        interp.set_symbol_value_cell("print-circle", Value::T);
        interp.set_symbol_value_cell("print-continuous-numbering", Value::Nil);
        interp.set_symbol_value_cell("print-gensym", Value::Nil);
        interp.set_symbol_value_cell("print-number-table", table);
        Env::new()
    }

    fn table_state(interp: &Interpreter, table: &Value, object: &Value) -> Option<Value> {
        json::hash_table_entries(interp, table)?
            .1
            .into_iter()
            .find_map(|(candidate, state)| (candidate.word() == object.word()).then_some(state))
    }

    #[test]
    fn print_preprocess_numbers_repeated_objects_in_second_encounter_order() {
        let mut interp = Interpreter::new();
        let a = Value::list([Value::symbol("a")]);
        let b = Value::list([Value::symbol("b")]);
        let object = Value::list([a, b, a, b]);
        let mut env = preprocess_env(&mut interp, Value::Nil);

        print_preprocess(&mut interp, &object, &mut env).expect("preprocess shared list");

        let table = interp
            .lookup_var("print-number-table", &env)
            .expect("public number table");
        assert_eq!(table_state(&interp, &table, &a), Some(Value::Integer(-1)));
        assert_eq!(table_state(&interp, &table, &b), Some(Value::Integer(-2)));
    }

    #[test]
    fn print_preprocess_terminates_and_numbers_a_materialized_cycle() {
        let mut interp = Interpreter::new();
        let object = Value::cons(Value::Integer(1), Value::Nil);
        let (_, cdr) = object.cons_cells().expect("cons");
        cdr.set(object);
        let mut env = preprocess_env(&mut interp, Value::Nil);

        print_preprocess(&mut interp, &object, &mut env).expect("preprocess cyclic list");

        let table = interp
            .lookup_var("print-number-table", &env)
            .expect("public number table");
        assert_eq!(
            table_state(&interp, &table, &object),
            Some(Value::Integer(-1))
        );
    }

    #[test]
    fn print_preprocess_respects_existing_states_and_resets_new_numbering() {
        let mut interp = Interpreter::new();
        let existing = Value::list([Value::symbol("existing")]);
        let repeated = Value::list([Value::symbol("repeated")]);
        let table = json::make_hash_table(&mut interp, "eq", vec![(existing, Value::Integer(-7))]);
        let object = Value::list([existing, repeated, repeated]);
        let mut env = preprocess_env(&mut interp, table);

        print_preprocess(&mut interp, &object, &mut env).expect("preprocess existing table");

        assert_eq!(
            table_state(&interp, &table, &existing),
            Some(Value::Integer(-7))
        );
        assert_eq!(
            table_state(&interp, &table, &repeated),
            Some(Value::Integer(-1))
        );
    }

    #[test]
    fn print_object_walks_deep_nesting_on_its_stack() {
        // print-tests.el's print-deeply-nested, forty times deeper: the
        // printer's explicit stack and one output, neither host
        // recursion nor a copy of the inner text at each level.
        let mut interp = Interpreter::new();
        let mut env = preprocess_env(&mut interp, Value::Nil);
        let levels = 400_000;
        let mut object = Value::symbol("a");
        for _ in 0..levels {
            let record = Value::LispRecord(crate::lisp::types::LispRecordRef::new(
                Value::symbol("r"),
                &[object],
            ));
            object = Value::list([Value::vector(vec![record])]);
        }
        let printed = render_printer_output(&mut interp, &object, &mut env, true, false)
            .expect("print a deep object")
            .host_text();
        let expected = format!("{}a{}", "([#s(r ".repeat(levels), ")])".repeat(levels));
        assert!(printed == expected, "deep object printed as GNU prints it");
    }
}
