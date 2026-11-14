mod object;
mod output;
pub(crate) use object::PrintGlobals;
pub(crate) use output::{Piece, PrintOutput};

use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

/// print.c's PRINTCHARFUN as print_prepare leaves it.
#[derive(Clone, Copy)]
pub(crate) enum PrintSink {
    /// nil: print_buffer, which print_finish inserts in the buffer (a
    /// buffer's or a marker's) that print_prepare made current.
    Buffer,
    /// t in a batch session: stdout.
    Stdout,
    /// t in an interactive session: the echo area and the message log.
    Echo,
    /// Anything else: a function, called with each character.
    Function(Value),
}

impl PrintSink {
    /// The sink of STREAM: nil is `standard-output', and that nil is t.
    /// (GNU's batch test is its C flag `noninteractive'; Emaxx has the
    /// Lisp variable alone.)
    pub(crate) fn of(interp: &Interpreter, stream: Option<&Value>) -> Self {
        let stream = crate::lisp::primitives::file_io::printer_stream_value(interp, stream);
        match stream.map(|stream| stream.kind()) {
            Some(Kind::Buffer(_) | Kind::Marker(_)) => PrintSink::Buffer,
            None | Some(Kind::Nil | Kind::T) => {
                if interp
                    .symbol_value_key(cached_symbol!("noninteractive"))
                    .is_some_and(|value| value.is_truthy())
                {
                    PrintSink::Stdout
                } else {
                    PrintSink::Echo
                }
            }
            Some(function) => PrintSink::Function(function.value()),
        }
    }
}

/// print.c's DEFVAR_BOOLs a string literal is escaped by.
#[derive(Clone, Copy, Default)]
pub(crate) struct StringEscapes {
    multibyte: bool,
    newlines: bool,
    control: bool,
    nonascii: bool,
}

/// print.c:octalout's digits for C: three when it needs them or the
/// character after it is an octal digit.
fn octal_digits(c: u32, next_is_octal: bool) -> u32 {
    if c > 0o77 || next_is_octal {
        3
    } else if c > 0o7 {
        2
    } else {
        1
    }
}

/// print.c:octalout of C: printchars.
fn octalout(c: u32, next_is_octal: bool, out: &mut PrintOutput) {
    out.push('\\');
    for shift in (0..octal_digits(c, next_is_octal)).rev() {
        out.push_code(u32::from(b'0') + ((c >> (3 * shift)) & 7));
    }
}

/// One turn of print_object's loop over a string printed with
/// `escapeflag': the character at byte OFFSET of STATE
/// (fetch_string_char_advance; at the string's end its terminating NUL)
/// as ESCAPES have it written, the pieces print.c's (printchars, but a
/// hex escape and the `\ ' after one, which are strouts).  MULTIBYTE is
/// STRING_MULTIBYTE as it was when the print of the string began.
#[inline]
pub(crate) fn prin1_string_char(
    escapes: StringEscapes,
    multibyte: bool,
    state: &crate::lisp::types::SharedStringState,
    offset: &mut usize,
    need_nonhex: &mut bool,
    out: &mut PrintOutput,
) {
    let bytes = state.bytes();
    let byte = bytes.get(*offset).copied().unwrap_or(0);
    // The usual character: itself, one printchar.
    if (0x20..0x7f).contains(&byte) && byte != b'"' && byte != b'\\' && !*need_nonhex {
        *offset += 1;
        out.push_code(u32::from(byte));
        return;
    }
    let (code, width) = if byte >= 0x80 && state.is_multibyte() {
        crate::lisp::types::string_data::decode_character(&bytes[*offset..])
            .unwrap_or((u32::from(byte), 1))
    } else {
        (u32::from(byte), 1)
    };
    *offset += width;
    let next_is_octal = || {
        bytes
            .get(*offset)
            .is_some_and(|next| (b'0'..=b'7').contains(next))
    };
    // When printing a raw 8-bit byte in a multibyte buffer, or (when
    // requested) a non-ASCII character in a unibyte buffer, print
    // single-byte non-ASCII string chars using octal escapes.
    let raw = if multibyte {
        (0x3fff80..=0x3fffff)
            .contains(&code)
            .then(|| code - 0x3fff00)
    } else {
        ((0x80..0x100).contains(&code) && escapes.nonascii).then_some(code)
    };
    if let Some(byte) = raw {
        octalout(byte, next_is_octal(), out);
        *need_nonhex = false;
    } else if multibyte && code >= 0x80 && escapes.multibyte {
        // When requested, print multibyte chars using hex escapes.
        out.push_str(&format!("\\x{code:04x}"));
        *need_nonhex = true;
    } else {
        // If we just had a hex escape, and this character could be taken
        // as part of it, output `\ ' to prevent that.
        if code < 0x80 && (code as u8).is_ascii_hexdigit() {
            if *need_nonhex {
                out.push_str("\\ ");
            }
            out.push_code(code);
        } else if code == 0x0a && escapes.newlines {
            out.push('\\');
            out.push('n');
        } else if code == 0x0c && escapes.newlines {
            out.push('\\');
            out.push('f');
        } else if code == 0x22 || code == 0x5c {
            out.push('\\');
            out.push_code(code);
        } else if escapes.control && (code < 0x20 || code == 0x7f) {
            octalout(code, next_is_octal(), out);
        } else if !multibyte && (0x80..0x100).contains(&code) {
            out.push_code(0x3fff00 + code);
        } else {
            out.push_code(code);
        }
        *need_nonhex = false;
    }
}

/// print_object's Lisp_String case under `escapeflag', for a string no
/// Lisp can change while it is printed: the quotes and every character
/// between them.
pub(crate) fn render_prin1_string(
    escapes: StringEscapes,
    multibyte: bool,
    state: &crate::lisp::types::SharedStringState,
    rendered: &mut PrintOutput,
) {
    rendered.push('"');
    let size_byte = state.bytes().len();
    let mut offset = 0;
    let mut need_nonhex = false;
    while offset < size_byte {
        prin1_string_char(
            escapes,
            multibyte,
            state,
            &mut offset,
            &mut need_nonhex,
            rendered,
        );
    }
    rendered.push('"');
}

/// print.c's printer variables, as the printer last read them.
#[derive(Clone, Copy, Default)]
pub(crate) struct PrintOptions {
    circle: bool,
    gensym: bool,
    integers_as_characters: bool,
    symbols_bare: bool,
    /// `print-length' when FIXNATP.
    length: Option<usize>,
    /// `print-level' when FIXNUMP: any fixnum, a negative one below
    /// every depth.
    level: Option<i64>,
    quoted: bool,
}

/// print.c:print_string's strout of the string STATE onto a sink that is
/// no function: its text as `princ' prints it, one block.  Under
/// `print-escape-nonascii' (which print_prepare binds for a multibyte
/// buffer) string_escape_byte8's octal escapes of its raw bytes; else a
/// unibyte string's bytes past 127 as the raw bytes they are.
fn strout_princ_string(
    interp: &Interpreter,
    state: &crate::lisp::types::SharedStringState,
    out: &mut PrintOutput,
) {
    let bytes = state.bytes();
    if bytes.is_ascii() {
        return out.strout_block(|block| block.push_ascii(bytes));
    }
    let escape_nonascii = print_flag(interp, ESCAPE_NONASCII);
    let multibyte = state.is_multibyte();
    out.strout_block(|block| {
        let mut offset = 0;
        while offset < bytes.len() {
            let byte = bytes[offset];
            let (code, width) = if multibyte && byte >= 0x80 {
                crate::lisp::types::string_data::decode_character(&bytes[offset..])
                    .expect("stored string character")
            } else {
                (u32::from(byte), 1)
            };
            offset += width;
            let raw = if (0x3fff80..=0x3fffff).contains(&code) {
                Some(code - 0x3fff00)
            } else if !multibyte && code >= 128 {
                Some(code)
            } else {
                None
            };
            match raw {
                Some(byte) if escape_nonascii => {
                    block.push('\\');
                    for shift in (0..3).rev() {
                        block.push_code(u32::from(b'0') + ((byte >> (3 * shift)) & 7));
                    }
                }
                Some(byte) => block.push_code(0x3fff00 + byte),
                None => block.push_code(code),
            }
        }
    });
}

/// The printer's variables, a bit each: what a print has read since Lisp
/// last ran.
const CIRCLE: u16 = 1 << 0;
const GENSYM: u16 = 1 << 1;
const INTEGERS_AS_CHARACTERS: u16 = 1 << 2;
const SYMBOLS_BARE: u16 = 1 << 3;
const QUOTED: u16 = 1 << 4;
const LENGTH: u16 = 1 << 5;
const LEVEL: u16 = 1 << 6;
const ESCAPE_MULTIBYTE: u16 = 1 << 7;
const ESCAPE_NEWLINES: u16 = 1 << 8;
const ESCAPE_CONTROL: u16 = 1 << 9;
const ESCAPE_NONASCII: u16 = 1 << 10;
const ESCAPES: u16 = ESCAPE_MULTIBYTE | ESCAPE_NEWLINES | ESCAPE_CONTROL | ESCAPE_NONASCII;

/// print.c reads the printer's DEFVARs as C variables: their dynamic
/// values, never a lexical binding.  The boolean one FLAG names.
#[inline(always)]
fn print_flag(interp: &Interpreter, flag: u16) -> bool {
    match flag {
        CIRCLE => interp
            .symbol_value_key(cached_symbol!("print-circle"))
            .is_some_and(|value| value.is_truthy()),
        GENSYM => interp
            .symbol_value_key(cached_symbol!("print-gensym"))
            .is_some_and(|value| value.is_truthy()),
        INTEGERS_AS_CHARACTERS => interp
            .symbol_value_key(cached_symbol!("print-integers-as-characters"))
            .is_some_and(|value| value.is_truthy()),
        SYMBOLS_BARE => interp
            .symbol_value_key(cached_symbol!("print-symbols-bare"))
            .is_some_and(|value| value.is_truthy()),
        QUOTED => interp
            .symbol_value_key(cached_symbol!("print-quoted"))
            .is_some_and(|value| value.is_truthy()),
        ESCAPE_MULTIBYTE => interp
            .symbol_value_key(cached_symbol!("print-escape-multibyte"))
            .is_some_and(|value| value.is_truthy()),
        ESCAPE_NEWLINES => interp
            .symbol_value_key(cached_symbol!("print-escape-newlines"))
            .is_some_and(|value| value.is_truthy()),
        ESCAPE_CONTROL => interp
            .symbol_value_key(cached_symbol!("print-escape-control-characters"))
            .is_some_and(|value| value.is_truthy()),
        ESCAPE_NONASCII => interp
            .symbol_value_key(cached_symbol!("print-escape-nonascii"))
            .is_some_and(|value| value.is_truthy()),
        _ => false,
    }
}

/// `print-continuous-numbering', a DEFVAR_LISP: non-nil.
fn print_continuous_numbering(interp: &Interpreter) -> bool {
    interp
        .symbol_value_key(cached_symbol!("print-continuous-numbering"))
        .is_some_and(|value| value.is_truthy())
}

/// The variables of BITS as they are now, into OPTIONS and ESCAPES: the
/// printer reads each where print.c first reads it, and again once Lisp
/// has run.
#[inline(always)]
fn read_print_variables(
    interp: &Interpreter,
    bits: u16,
    options: &mut PrintOptions,
    escapes: &mut StringEscapes,
) {
    if bits & CIRCLE != 0 {
        options.circle = print_flag(interp, CIRCLE);
    }
    if bits & GENSYM != 0 {
        options.gensym = print_flag(interp, GENSYM);
    }
    if bits & INTEGERS_AS_CHARACTERS != 0 {
        options.integers_as_characters = print_flag(interp, INTEGERS_AS_CHARACTERS);
    }
    if bits & SYMBOLS_BARE != 0 {
        options.symbols_bare = print_flag(interp, SYMBOLS_BARE);
    }
    if bits & QUOTED != 0 {
        options.quoted = print_flag(interp, QUOTED);
    }
    if bits & LENGTH != 0 {
        // FIXNATP (Vprint_length).
        options.length = interp
            .symbol_value_key(cached_symbol!("print-length"))
            .and_then(|value| value.integer_value())
            .and_then(|value| usize::try_from(value).ok());
    }
    if bits & LEVEL != 0 {
        // FIXNUMP (Vprint_level).
        options.level = interp
            .symbol_value_key(cached_symbol!("print-level"))
            .and_then(|value| value.fixnum());
    }
    if bits & ESCAPE_MULTIBYTE != 0 {
        escapes.multibyte = print_flag(interp, ESCAPE_MULTIBYTE);
    }
    if bits & ESCAPE_NEWLINES != 0 {
        escapes.newlines = print_flag(interp, ESCAPE_NEWLINES);
    }
    if bits & ESCAPE_CONTROL != 0 {
        escapes.control = print_flag(interp, ESCAPE_CONTROL);
    }
    if bits & ESCAPE_NONASCII != 0 {
        escapes.nonascii = print_flag(interp, ESCAPE_NONASCII);
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
    if print_flag(interp, CIRCLE) {
        interp.print_number_index = 0;
        object::print_preprocess(interp, env, *value)?;
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

/// print.c print_prune_string_charset for STRING printed with
/// `escapeflag': the string print_object goes on with.  Unless
/// `print-charset-text-property' is t (the function is not called), or
/// is non-nil and a `charset' property is unsafe, that is a copy
/// (Fcopy_sequence) with its `charset' properties removed, or all its
/// intervals where it has no other: a string of its own, which nothing
/// Lisp does to STRING while it is printed changes, and whose intervals
/// the collector marks with it.
fn print_pruned_string(
    interp: &Interpreter,
    string: crate::lisp::types::StringObjectRef,
) -> Result<crate::lisp::types::StringObjectRef, LispError> {
    let setting = interp
        .symbol_value_key(cached_symbol!("print-charset-text-property"))
        .unwrap_or(Value::Nil);
    if matches!(setting.kind(), Kind::T) {
        return Ok(string);
    }
    let state = string.borrow();
    let pruned = match state.props.intervals() {
        None => None,
        Some(intervals) => {
            match print_prune_string_charset(interp, setting, &state.codes(), intervals)? {
                (_, true) => return Ok(string),
                (pruned, false) => pruned,
            }
        }
    };
    Ok(state.copy_with_intervals(pruned))
}

/// print.c print_prune_string_charset: the intervals a string prints
/// with, from a string with INTERVALS whose characters are CODES (as
/// fetch_string_char_advance reads them), under a
/// `print-charset-text-property' SETTING that is not t:
/// print_check_string_charset_prop looks for a property other than
/// `charset' and for a `charset' that a non-ASCII character of its
/// interval does not belong to (CHAR_CHARSET); with the variable nil or
/// no such charset, a copy of the string loses its `charset' properties,
/// or all its intervals where it has no other.  None for a string that
/// prints without them; and whether the string was left as it is.
fn print_prune_string_charset(
    interp: &Interpreter,
    setting: Value,
    codes: &[u32],
    intervals: &crate::intervals::Intervals,
) -> Result<(Option<crate::intervals::Intervals>, bool), LispError> {
    let charset_symbol = Value::symbol("charset");
    let (mut non_charset, mut unsafe_charset) = (false, false);
    let mut ordered = None;
    for (start, end, plist) in intervals.iter() {
        if plist.is_nil() || (non_charset && unsafe_charset) {
            continue;
        }
        let mut val = plist;
        while let Some(cell) = val.cons_ref() {
            if crate::lisp::types::eq(cell.car.get(), charset_symbol) {
                break;
            }
            val = cell
                .cdr
                .get()
                .cons_ref()
                .map_or(Value::Nil, |rest| rest.cdr.get());
        }
        let Some(cell) = val.cons_ref() else {
            non_charset = true;
            continue;
        };
        let value_cell = cell.cdr.get().cons_ref();
        if !non_charset
            && (val.word() != plist.word()
                || value_cell
                    .as_ref()
                    .is_some_and(|rest| rest.cdr.get().cons_ref().is_some()))
        {
            non_charset = true;
        }
        if !unsafe_charset {
            let charset = value_cell.map_or(Value::Nil, |rest| rest.car.get());
            let expected = crate::lisp::eval::coding::CharsetRef::charset_symbol(&charset)
                .map(|name| interp.charset_canonical_name(&name).unwrap_or(name));
            let (ordered, head) = ordered.get_or_insert_with(|| {
                (
                    interp.charset_priority_list(),
                    interp.charset_non_preferred_head(),
                )
            });
            unsafe_charset = codes.iter().skip(start).take(end - start).any(|&code| {
                code >= 0x80
                    && Some(char_charset_ordered(interp, ordered, *head, code).0) != expected
            });
        }
    }
    if !setting.is_nil() && unsafe_charset {
        return Ok((Some(intervals.clone()), true));
    }
    if !non_charset {
        // Fset_text_properties of nil over the whole copy, which
        // Fcopy_sequence gave every interval (copy_intervals).
        crate::lisp::alloc::counters::note_intervals(intervals.count());
        return Ok((None, false));
    }
    // Fremove_text_properties of (charset) over the whole copy.
    let Some(mut copy) = intervals.copy(0, intervals.total_length())? else {
        return Ok((None, false));
    };
    let len = copy.total_length();
    crate::intervals::remove_text_properties_range(
        &mut copy,
        0,
        len,
        Value::list([charset_symbol]),
        Value::Nil,
        &mut crate::intervals::NoRecord,
    )?;
    Ok((Some(copy), false))
}

/// print.c:print's reset of the numbering: both pieces of state unless
/// continuous numbering has a live public table.  A nil table therefore
/// starts numbering over even when `print-continuous-numbering' itself
/// is non-nil.  The reset is a store into the C variable, which runs no
/// watcher: a table already nil is left as it is.
fn prepare_print_numbering(interp: &mut Interpreter, env: &mut Env) {
    let table = interp.symbol_value_key(cached_symbol!("print-number-table"));
    if !print_continuous_numbering(interp) || table.is_none_or(|value| value.is_nil()) {
        interp.print_number_index = 0;
        if table.is_none_or(|value| !value.is_nil()) {
            interp.set_variable_key(cached_symbol!("print-number-table"), Value::Nil, env);
        }
    }
}

pub(crate) fn render_prin1_output(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<PrintOutput, LispError> {
    with_printer_buffer_escape(interp, env, Some(true), |interp, env| {
        render_printer_output(interp, value, env, true)
    })
}

pub(crate) fn render_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    render_prin1_output(interp, value, env).map(|output| output.host_text())
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

/// VALUE's text as `prin1' (ESCAPE) or `princ' prints it, for host code
/// that wants the text and prints nowhere: printed into a print_buffer
/// of its own, the one of a print under way left as it is.
pub(crate) fn render_printer_output(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    escape: bool,
) -> Result<PrintOutput, LispError> {
    let under_way = interp.print_globals.buffer.replace(PrintOutput::plain());
    let printed = object::print(interp, env, *value, escape, PrintSink::Buffer);
    let text = std::mem::replace(&mut interp.print_globals.buffer, under_way);
    printed.map(|()| text.unwrap_or_default())
}

pub(crate) fn render_prin1_ephemeral(
    interp: &mut Interpreter,
    value: &Value,
    env: &crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut env = env.clone();
    render_prin1(interp, value, &mut env)
}

/// print.c:print of VALUE onto SINK, between a print_prepare and its
/// print_finish.
pub(crate) fn print_to_sink(
    interp: &mut Interpreter,
    env: &mut Env,
    value: &Value,
    escape: bool,
    sink: PrintSink,
) -> Result<(), LispError> {
    object::print(interp, env, *value, escape, sink)
}

/// print.c's printchar and strout of OUTPUT's pieces onto SINK.
pub(crate) fn deliver(
    interp: &mut Interpreter,
    env: &mut Env,
    sink: PrintSink,
    output: &PrintOutput,
) -> Result<(), LispError> {
    match sink {
        PrintSink::Buffer => {
            interp
                .print_globals
                .buffer
                .get_or_insert_with(PrintOutput::plain)
                .append_strout(output);
            Ok(())
        }
        PrintSink::Stdout => {
            if !interp.message_capture_stack.is_empty() {
                interp.append_message_capture(&output.host_text(), false, env);
            }
            if standard_display_table_p(interp) {
                return deliver_through_display_table(interp, env, output);
            }
            for piece in output.pieces() {
                match piece {
                    Piece::Chars(chars) => {
                        crate::lisp::primitives::batch_stdout::write_chars(chars)
                    }
                    Piece::Strout(block) => crate::lisp::primitives::batch_stdout::write(block),
                    Piece::Octets(block) => {
                        crate::lisp::primitives::batch_stdout::write(&raw_octets(block))
                    }
                }
            }
            if let Some(last) = output.last_printchar() {
                interp.batch_standard_output_last_char = Some(last);
            }
            interp.batch_stdout_need_newline = true;
            Ok(())
        }
        PrintSink::Echo => {
            let text = output.host_text();
            interp.append_message_capture(&text, false, env);
            if !output.has_octets() {
                crate::lisp::primitives::echo_area_print_logged(interp, &text, env);
                return Ok(());
            }
            // strout of a C string into the echo area: message_dolog of
            // its bytes as the multibyte text they are, and insert_char
            // of each byte as the character of that code.
            for piece in output.pieces() {
                match piece {
                    Piece::Octets(block) => {
                        let octets = raw_octets(block);
                        let logged = PrintOutput::from_memory(&octets, true).host_text();
                        let shown: String = octets.iter().map(|&byte| char::from(byte)).collect();
                        crate::lisp::primitives::echo_area_print_logged_as(
                            interp, &logged, &shown, env,
                        );
                    }
                    Piece::Chars(block) | Piece::Strout(block) => {
                        let text = PrintOutput::from_memory(block, true).host_text();
                        crate::lisp::primitives::echo_area_print_logged(interp, &text, env);
                    }
                }
            }
            Ok(())
        }
        PrintSink::Function(function) => {
            for code in output.codes() {
                interp.call_function_value(
                    function,
                    None,
                    &[Value::Integer(i64::from(code))],
                    env,
                )?;
            }
            Ok(())
        }
    }
}

/// print.c:print_string of the string STATE onto SINK: one strout of
/// its text (as `princ' prints it) onto a buffer, stdout or the echo
/// area; a printchar a character onto a function.
pub(crate) fn print_string(
    interp: &mut Interpreter,
    env: &mut Env,
    state: crate::lisp::types::StringObjectRef,
    sink: PrintSink,
) -> Result<(), LispError> {
    if let PrintSink::Function(function) = sink {
        return print_string_to_function(interp, env, state, function);
    }
    let mut output = interp.print_globals.take_pending();
    strout_princ_string(interp, &state.borrow(), &mut output);
    let delivered = deliver(interp, env, sink, &output);
    interp.print_globals.give_pending(output);
    delivered
}

/// print_string's loop for a function: each character handed over
/// before the next is read, the string looked at again each time
/// ("string may be relocated by printing one char"); `size' and
/// `size_byte' are read once.  A unibyte string's bytes are the
/// characters of those codes.
pub(crate) fn print_string_to_function(
    interp: &mut Interpreter,
    env: &mut Env,
    state: crate::lisp::types::StringObjectRef,
    function: Value,
) -> Result<(), LispError> {
    let (size, size_byte) = {
        let state = state.borrow();
        (state.len(), state.bytes().len())
    };
    let mut offset = 0;
    loop {
        let code = {
            let state = state.borrow();
            let bytes = state.bytes();
            // The loop runs to the `size_byte' read at the start.  A
            // string the function made shorter than that is read at its
            // terminating NUL next; nothing past that is the string's.
            if offset >= size_byte || offset > bytes.len() {
                return Ok(());
            }
            let byte = bytes.get(offset).copied().unwrap_or(0);
            if size == size_byte || byte < 0x80 {
                offset += 1;
                u32::from(byte)
            } else {
                let (code, width) =
                    crate::lisp::types::string_data::decode_character(&bytes[offset..])
                        .unwrap_or((u32::from(byte), 1));
                offset += width;
                code
            }
        };
        interp.call_function_value(function, None, &[Value::Integer(i64::from(code))], env)?;
    }
}

/// print.c:printchar of CH onto SINK.
pub(crate) fn printchar(
    interp: &mut Interpreter,
    env: &mut Env,
    sink: PrintSink,
    ch: u32,
) -> Result<(), LispError> {
    if let PrintSink::Function(function) = sink {
        return interp
            .call_function_value(function, None, &[Value::Integer(i64::from(ch))], env)
            .map(drop);
    }
    let mut one = interp.print_globals.take_pending();
    one.push_code(ch);
    let delivered = deliver(interp, env, sink, &one);
    interp.print_globals.give_pending(one);
    delivered
}

/// `DISP_TABLE_P (Vstandard_display_table)'.
fn standard_display_table_p(interp: &Interpreter) -> bool {
    matches!(
        interp
            .symbol_value_key(cached_symbol!("standard-display-table"))
            .map(|value| value.kind()),
        Some(Kind::CharTable(table)) if table.is_display_table()
    )
}

/// The octets a strout of a C string wrote, from the raw-byte characters
/// the output holds them as.
fn raw_octets(block: &[u8]) -> Vec<u8> {
    let mut octets = Vec::with_capacity(block.len());
    let mut rest = block;
    while let Ok((code, width)) = crate::lisp::types::string_data::decode_character(rest) {
        octets.push(if code >= 0x3fff80 {
            (code - 0x3fff00) as u8
        } else {
            code as u8
        });
        rest = &rest[width..];
        if rest.is_empty() {
            break;
        }
    }
    octets
}

/// printchar and strout onto stdout with a display table: each character
/// through printchar_to_stream (strout reads its block a character at a
/// time with string_char_and_length), which can signal (the coding
/// system it encodes with).  printchar sets `printchar_stdout_last'
/// before it writes and `noninteractive_need_newline' after; strout sets
/// the latter after its whole block.
fn deliver_through_display_table(
    interp: &mut Interpreter,
    env: &mut Env,
    output: &PrintOutput,
) -> Result<(), LispError> {
    for piece in output.pieces() {
        let printchars = matches!(piece, Piece::Chars(_));
        let octets = match piece {
            Piece::Octets(block) => Some(raw_octets(block)),
            Piece::Chars(_) | Piece::Strout(_) => None,
        };
        let mut rest = match (&octets, piece) {
            (Some(octets), _) => &octets[..],
            (None, Piece::Chars(chars) | Piece::Strout(chars) | Piece::Octets(chars)) => chars,
        };
        while let Some(&byte) = rest.first() {
            let (code, width) = crate::lisp::types::string_data::decode_character(rest)
                .unwrap_or((u32::from(byte), 1));
            rest = &rest[width..];
            if printchars {
                interp.batch_standard_output_last_char = Some(code);
            }
            crate::lisp::primitives::file_io::printchar_to_stream(interp, code, true, env)?;
            if printchars {
                interp.batch_stdout_need_newline = true;
            }
        }
        if !printchars {
            interp.batch_stdout_need_newline = true;
        }
    }
    // A strout of nothing among them.
    if output.strout_called() {
        interp.batch_stdout_need_newline = true;
    }
    Ok(())
}

/// print.c's print_buffer around BODY, for a print whose printcharfun is
/// nil: as print_prepare leaves it, empty (the text of a print under way
/// set aside, where print_prepare keeps it in a string), and when BODY is
/// done, by an error too, as the unwinding leaves it: freed, or with the
/// text set aside copied back over it (print_unwind copies SCHARS bytes
/// and leaves `pos' and `pos_byte' as BODY left them, so the print under
/// way goes on from where this one ended).  The text BODY printed.
pub(crate) fn with_print_buffer<T>(
    interp: &mut Interpreter,
    body: impl FnOnce(&mut Interpreter) -> Result<T, LispError>,
) -> Result<(T, PrintOutput), LispError> {
    let saved = interp.print_globals.buffer.replace(PrintOutput::plain());
    // A print run by a hook of print_finish's insertion: that insertion
    // copies from print_buffer's memory when the hooks are done, so from
    // here on what the memory holds past `pos_byte' matters.
    if interp.print_globals.inserting > 0
        && interp.print_globals.memory.is_none()
        && let Some(saved) = &saved
    {
        interp.print_globals.memory = Some(saved.bytes().to_vec());
    }
    let result = body(interp);
    let text = interp.print_globals.buffer.take().unwrap_or_default();
    if let (Some(memory), Some(saved)) = (&mut interp.print_globals.memory, &saved) {
        // What BODY wrote, then print_unwind's copy.
        let restored = saved.restored_len();
        if memory.len() < text.len().max(restored) {
            memory.resize(text.len().max(restored), 0);
        }
        memory[..text.len()].copy_from_slice(text.bytes());
        memory[..restored].copy_from_slice(&saved.bytes()[..restored]);
    }
    interp.print_globals.buffer = saved.map(|saved| text.clone().overwritten_by(&saved));
    result.map(|value| (value, text))
}

/// print.c:print_finish's text for insert_1_both, read once
/// prepare_to_modify_buffer has run: the NBYTES bytes (MULTIBYTE as the
/// text was) insert_1_both was told of before, as print_buffer's memory
/// holds them now, which a print run by a before-change hook has written
/// over and print_unwind only partly restored.  FINISH is given the text
/// and, when such a print ran, the `pos' print_buffer has now.
pub(crate) fn print_finish_text<R>(
    interp: &mut Interpreter,
    nbytes: usize,
    multibyte: bool,
    finish: impl FnOnce(&PrintOutput, Option<usize>) -> R,
) -> R {
    match interp.print_globals.memory.as_ref() {
        Some(memory) => {
            let held = PrintOutput::from_memory(&memory[..nbytes.min(memory.len())], multibyte);
            let pos = interp
                .print_globals
                .buffer
                .as_ref()
                .map_or(0, PrintOutput::pos);
            finish(&held, Some(pos))
        }
        None => match interp.print_globals.buffer.as_ref() {
            Some(text) => finish(text, None),
            None => finish(&PrintOutput::default(), None),
        },
    }
}

/// print.c:printchar of the unsigned int CH that `write-char' was given,
/// onto a SINK that is no function.  CHAR_STRING of a code that is no
/// character: char_string resolves a modifier bit and drops what is left
/// of them, and signals for a code still past MAX_CHAR; a negative int
/// (`eassume (0 <= c)' does not hold) is stored as its low byte alone,
/// which fwrite puts on stdout as it is.  With `standard-display-table'
/// it is printchar_to_stream's to write.
pub(crate) fn printchar_unsigned(
    interp: &mut Interpreter,
    env: &mut Env,
    sink: PrintSink,
    ch: u32,
) -> Result<(), LispError> {
    if (ch as i32) < 0 && matches!(sink, PrintSink::Stdout) {
        interp.batch_standard_output_last_char = Some(ch);
        if standard_display_table_p(interp) {
            crate::lisp::primitives::file_io::printchar_to_stream(interp, ch, true, env)?;
        } else {
            crate::lisp::primitives::batch_stdout::write(&[ch as u8]);
        }
        interp.batch_stdout_need_newline = true;
        return Ok(());
    }
    let mut one = PrintOutput::default();
    one.push_code(crate::lisp::primitives::ccl::written_character(ch as i32)?);
    deliver(interp, env, sink, &one)
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
            matches!(data.car().map(|head| head.kind()), Ok(Kind::Symbol(name)) if name.is_named("end-of-file"))
        }
        _ => false,
    }
}

fn end_of_file_error(interp: &Interpreter, env: &Env) -> LispError {
    match interp
        .lookup_var_key(cached_symbol!("load-true-file-name"), env)
        .map(|v| v.kind())
    {
        Some(file @ Kind::StringObject(_)) => LispError::SignalValue(Value::list([
            Value::Symbol("end-of-file".into()),
            file.value(),
        ])),
        _ => LispError::EndOfInput(),
    }
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
    Encoded(&'a [u8], bool),
    /// A buffer's text, read in place in its parts before the gap and
    /// after it, and whether the buffer is multibyte.
    Buffer(crate::lisp::regex_emacs::GapRange<'a>, bool),
    /// A buffer's text in its internal multibyte bytes, and whether the
    /// buffer is multibyte.
    BufferBytes(&'a [u8], bool),
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
            LispReaderInput::Encoded(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded(bytes, multibyte, shorthands)
            }
            LispReaderInput::Buffer(parts, multibyte) => {
                crate::lisp::reader::Reader::from_buffer_parts(parts, multibyte, shorthands)
            }
            LispReaderInput::BufferBytes(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded(bytes, true, shorthands)
                    .reporting_multibyte(multibyte)
            }
        };
        let mut reader = reader.with_read_context(&mut context);
        let value = match reader.read() {
            Ok(value) => value,
            Err(error) => {
                *stopped = reader.characters_consumed();
                return Err(error);
            }
        };
        Ok((
            value,
            reader.emitted_reader_forms(),
            reader.characters_consumed(),
        ))
    })();
    let (value, needs_materialization, consumed) =
        read.map_err(|error| reader_end_of_file(interp, error, env))?;
    let value = value.ok_or_else(|| end_of_file_error(interp, env))?;
    // Allocate identity-bearing literals before resolving references into them.
    let value = if needs_materialization {
        interp.materialize_read_object_literals(value, env)?
    } else {
        value
    };
    Ok((value, consumed))
}

/// A buffer's text from START to END as the reader reads it, the way
/// lread.c's readchar takes each character from the buffer.  The text is
/// read in place (a scan's copy shares it) unless the range holds a character
/// it cannot carry (one beyond Unicode, or a real U+E000..U+E0FF): then
/// the reader takes the range's internal multibyte bytes.
pub(crate) enum BufferReadSource {
    /// The buffer's text and the byte range of the range in it, and
    /// whether the buffer is multibyte.
    Buffer(crate::buffer::ScanText, std::ops::Range<usize>, bool),
    Encoded(Vec<u8>, bool),
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
            let text = buffer.scan_text();
            let byte = |pos: usize| {
                buffer
                    .storage_byte_position(pos)
                    .expect("an accessible position")
                    - 1
            };
            let range = byte(start)..byte(end);
            return Ok(Self::Buffer(text, range, buffer.is_multibyte()));
        }
        let text = buffer
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let extended = buffer.substring_extended_chars(start, end);
        Ok(Self::Encoded(
            crate::lisp::types::string_data::encode_text(&text, true, &extended)?,
            buffer.is_multibyte(),
        ))
    }

    pub(crate) fn input(&self) -> LispReaderInput<'_> {
        self.input_from(0)
    }

    /// The source from byte OFFSET on.
    pub(crate) fn input_from(&self, offset: usize) -> LispReaderInput<'_> {
        match self {
            Self::Buffer(text, range, multibyte) => {
                let (first, second) = text.range_halves(range.start + offset..range.end);
                LispReaderInput::Buffer(
                    crate::lisp::regex_emacs::GapRange::new(first, second),
                    *multibyte,
                )
            }
            Self::Encoded(bytes, multibyte) => {
                LispReaderInput::BufferBytes(&bytes[offset..], *multibyte)
            }
        }
    }

    /// The characters from byte FROM to byte TO.
    pub(crate) fn characters_between(&self, from: usize, to: usize) -> usize {
        match self {
            Self::Buffer(text, range, _) => text.chars_in(range.start + from..range.start + to),
            Self::Encoded(bytes, _) => {
                let mut position = from;
                let mut count = 0;
                while position < to {
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
        Kind::Buffer(buffer) => {
            // lread.c:readchar reads a killed buffer as at its end.
            let buffer_id = interp
                .live_buffer_id(buffer)
                .ok_or_else(|| end_of_file_error(interp, env))?;
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
        // readchar's report for a string is its multibyteness, as for
        // `read'.
        Kind::StringObject(state) => {
            let contents = state.borrow();
            read_one_positioned_form(
                interp,
                env,
                LispReaderInput::Encoded(contents.bytes(), contents.is_multibyte()),
                0,
                &mut 0,
            )
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
            LispReaderInput::Encoded(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded_positioned(
                    bytes,
                    multibyte,
                    symbol_shorthands,
                    base_position,
                )
            }
            LispReaderInput::Buffer(parts, multibyte) => {
                crate::lisp::reader::Reader::from_buffer_parts_positioned(
                    parts,
                    multibyte,
                    symbol_shorthands,
                    base_position,
                )
            }
            LispReaderInput::BufferBytes(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded_positioned(
                    bytes,
                    true,
                    symbol_shorthands,
                    base_position,
                )
                .reporting_multibyte(multibyte)
            }
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
        Ok((value, consumed, reader.emitted_reader_forms()))
    })();
    let (value, consumed, needs_materialization) =
        read.map_err(|error| reader_end_of_file(interp, error, env))?;
    let value = value.ok_or_else(|| end_of_file_error(interp, env))?;
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
        && symbol.is_named("quote")
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
        // A symbol is called as itself, as Ffuncall's ORIGINAL_FUN.
        if self.source.is_symbol() {
            return self
                .interp
                .call_function_value(self.source, None, args, self.env);
        }
        let callable = resolve_callable(self.interp, &self.source, self.env)?;
        self.interp
            .call_function_value(callable, self.source.symbol_str(), args, self.env)
    }
}

impl crate::lisp::reader::ReadContext for CallableReader<'_> {
    fn symbols_with_pos_enabled(&mut self) -> bool {
        crate::lisp::primitives::symbols_with_pos_enabled(self.interp, self.env)
    }

    fn intern(&mut self, name: &str, _multibyte: bool) -> Result<Value, LispError> {
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

    fn read_circle(&mut self) -> bool {
        self.interp.read_circle(self.env)
    }

    fn unescaped_character_literal(&mut self, character: u8) -> Result<(), LispError> {
        note_unescaped_character_literal(self.interp, character, self.env)
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
            .lookup_var_key(cached_symbol!("obarray"), self.env)
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
    let (value, needs_materialization) = {
        let mut stream = CallableReader {
            interp,
            source: *source,
            env,
        };
        let mut reader = crate::lisp::reader::Reader::from_stream(&mut stream, locate_symbols);
        let value = reader.read().and_then(|value| {
            reader.finish_stream()?;
            Ok(value)
        });
        (value, reader.emitted_reader_forms())
    };
    // end_of_file_error names `load-true-file-name' whatever the stream.
    let value = value
        .map_err(|error| reader_end_of_file(interp, error, env))?
        .ok_or_else(|| end_of_file_error(interp, env))?;
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
    if !head.is_named("quote") {
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
/// the data by a read that deferred them.  plist_get finds each parameter's
/// first value by EQ; `test', `weakness' and `purecopy' go to
/// Fmake_hash_table, with `:size' the number of DATA pairs, and each pair
/// is Fputhash'd in order.
fn hash_table_from_plist(
    interp: &mut Interpreter,
    fields: &[Value],
    env: &mut Env,
    materialize: bool,
) -> Result<Value, LispError> {
    let plist_get = |name: &str| {
        let prop = Value::symbol(name);
        fields
            .as_chunks::<2>()
            .0
            .iter()
            .find(|[key, _]| crate::lisp::types::eq(*key, prop))
            .map_or(Value::Nil, |[_, value]| *value)
    };
    let mut params = Vec::new();
    for (name, keyword) in [
        ("test", ":test"),
        ("weakness", ":weakness"),
        ("purecopy", ":purecopy"),
    ] {
        let value = plist_get(name);
        if !value.is_nil() {
            params.push(Value::symbol(keyword));
            params.push(value);
        }
    }
    let data = plist_get("data");
    if !(data.is_nil() || data.is_cons()) {
        return Err(LispError::Signal("Hash table data is not a list".into()));
    }
    // list_length, which refuses a dotted or circular list as Flength.
    call(interp, "length", &[data], env)?;
    let items = data.to_vec()?;
    if items.len() % 2 == 1 {
        return Err(LispError::Signal("Hash table data length is odd".into()));
    }
    params.push(Value::symbol(":size"));
    params.push(Value::Integer((items.len() / 2) as i64));
    let table = call(interp, "make-hash-table", &params, env)?;
    let object = check_hash_table(&table)?;
    for &[key, value] in items.as_chunks::<2>().0 {
        let (key, value) = if materialize {
            (
                interp.materialize_read_object_literals(key, env)?,
                interp.materialize_read_object_literals(value, env)?,
            )
        } else {
            (key, value)
        };
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
        Kind::Buffer(buffer) => {
            // lread.c:readchar reads a killed buffer as at its end.
            let buffer_id = interp
                .live_buffer_id(buffer)
                .ok_or_else(|| end_of_file_error(interp, env))?;
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

/// STRING_MULTIBYTE, or `None' for a non-string.
fn string_multibyte_p(value: &Value) -> Option<bool> {
    match value.kind() {
        Kind::StringObject(state) => Some(state.borrow().is_multibyte()),
        _ => None,
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
                    .lookup_var_key(cached_symbol!("coding-system-for-write"), env)
                    .unwrap_or(Value::Nil);
                if coding.is_nil() {
                    let default = interp
                        .lookup_var_key(cached_symbol!("buffer-file-coding-system"), env)
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
                        .lookup_var_key(cached_symbol!("select-safe-coding-system-function"), env)
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
    } else if let Some(multibyte) = string_multibyte_p(source) {
        if coding.is_nil() {
            coding = if multibyte {
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
    } else if crate::lisp::types::eq_symbol_named(source, "iv-auto") {
        let Kind::Integer(length @ 0..) = start.kind() else {
            return Err(crate::lisp::primitives::values::doprnt_error(
                interp,
                "Without a length, `iv-auto' can't be used; see ELisp manual",
                &[],
            ));
        };
        return crate::lisp::primitives::text::iv_auto_bytes(length as usize);
    } else {
        // signal_error ("Invalid object argument", NILP (object) ?
        // build_string ("nil") : object).
        return Err(crate::lisp::primitives::values::signal_error(
            "Invalid object argument",
            if source.is_nil() {
                Value::string("nil")
            } else {
                *source
            },
        ));
    };
    let object = if string_multibyte_p(&object).expect("extracted string") {
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
    let Kind::StringObject(state) = object.kind() else {
        unreachable!("encoded string");
    };
    let state = state.borrow();
    let bytes = state.bytes();
    if buffer_source {
        return Ok(bytes.to_vec());
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
            .lookup_var_key(cached_symbol!("print-number-table"), &env)
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
            .lookup_var_key(cached_symbol!("print-number-table"), &env)
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
        let printed = render_printer_output(&mut interp, &object, &mut env, true)
            .expect("print a deep object")
            .host_text();
        let expected = format!("{}a{}", "([#s(r ".repeat(levels), ")])".repeat(levels));
        assert!(printed == expected, "deep object printed as GNU prints it");
    }
}
