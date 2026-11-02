use super::*;
use crate::lisp::types::Kind;

mod charset_table;
#[macro_use]
mod units;
mod decoders;
mod detect;
mod iso2022;

pub(crate) use charset_table::{CharsetInfo, charset_info};

pub(crate) use detect::DetectSource;
pub(crate) use iso2022::charset_dimension;

/// character.h MAX_UNICODE_CHAR, MAX_5_BYTE_CHAR and MAX_CHAR: the last
/// Unicode character, the last character of the `emacs' charset (raw
/// bytes follow at #x3fff80..#x3fffff) and the last character.
pub(crate) const MAX_UNICODE_CHAR: u32 = 0x10_FFFF;
pub(crate) const MAX_5_BYTE_CHAR: u32 = 0x3F_FF7F;
pub(crate) const MAX_CHAR: u32 = 0x3F_FFFF;

pub(crate) fn coding_system_error(coding: Value) -> LispError {
    // coding.c:Fcheck_coding_system signals with the actual offending
    // object, preserving even an uninterned symbol's identity.
    LispError::SignalValue(Value::list([Value::symbol("coding-system-error"), coding]))
}

fn charset_plist_property(interp: &Interpreter, charset: &str, property: &str) -> Option<Value> {
    let items = interp.charset_plist_value(charset)?.to_vec().ok()?;
    items.windows(2).find_map(|pair| {
        matches!(pair[0].kind(), Kind::Symbol(name) if name == property).then(|| pair[1])
    })
}

/// charset.c:read_hex over a map file's BYTES from *POS: LOOKAHEAD (or
/// the next byte) starts a scan to the next `0x', a `#' skipping to the
/// end of its line; the hex digits after it are the number, and the byte
/// that ends them (None at the end of the file) is the terminator.
/// OVERFLOW is set when a digit shifts a set bit out.
fn read_charset_map_hex(
    bytes: &[u8],
    pos: &mut usize,
    lookahead: Option<u8>,
    overflow: &mut bool,
) -> (u32, Option<u8>) {
    let mut getc = || {
        let byte = bytes.get(*pos).copied();
        *pos += usize::from(byte.is_some());
        byte
    };
    let mut c = lookahead.or_else(&mut getc);
    while let Some(byte) = c {
        if byte == b'#' {
            c = getc();
            while c.is_some_and(|byte| byte != b'\n') {
                c = getc();
            }
        } else if byte == b'0' {
            c = getc();
            if c.is_none_or(|byte| byte == b'x') {
                break;
            }
        }
        if c.is_none() {
            break;
        }
        c = getc();
    }
    let mut n = 0u32;
    if c.is_some() {
        loop {
            c = getc();
            let Some(digit) = c.and_then(|byte| char::from(byte).to_digit(16)) else {
                break;
            };
            *overflow |= n >> 28 != 0;
            n = (n << 4) + digit;
        }
    }
    (n, c)
}

/// charset.c load_charset_map_from_file's reading, then load_charset_map
/// over a map file: each `FROM[-TO] C' entry (read_hex's numbers, an entry
/// whose number overflowed, whose FROM exceeds TO or whose C is no
/// character skipped, and one the end of the file cuts short dropped) maps
/// the codes whose code-space INDEX runs from FROM's to TO's onto the
/// characters from C on, one per index (codes are not consecutive integers
/// once a dimension has gaps: GB180304.map's `0x81308130-0x81308435 0x80'
/// is 36 characters, not 774); an entry whose codes fall outside the code
/// space is skipped (`from_index < 0 || to_index < 0').
fn parse_charset_map(contents: &[u8], bounds: &[(u32, u32)]) -> Vec<(u32, u32)> {
    const MAX_CHAR: u32 = 0x3F_FFFF;
    let mut mappings = Vec::new();
    let mut pos = 0;
    let mut ch = None;
    loop {
        let mut overflow = false;
        let (code_start, terminator) = read_charset_map_hex(contents, &mut pos, ch, &mut overflow);
        let Some(terminator) = terminator else {
            break;
        };
        let code_end = if terminator == b'-' {
            let (code_end, terminator) =
                read_charset_map_hex(contents, &mut pos, None, &mut overflow);
            if terminator.is_none() {
                break;
            }
            ch = terminator;
            code_end
        } else {
            ch = None;
            code_start
        };
        let (character_start, terminator) =
            read_charset_map_hex(contents, &mut pos, ch.take(), &mut overflow);
        if terminator.is_none() {
            break;
        }
        ch = terminator;
        if overflow || code_start > code_end || character_start > MAX_CHAR {
            continue;
        }
        let (Some(from_index), Some(to_index)) = (
            code_point_to_raw_index(bounds, code_start),
            code_point_to_raw_index(bounds, code_end),
        ) else {
            continue;
        };
        for (offset, index) in (from_index..=to_index).enumerate() {
            if let Some(code) = raw_index_to_code_point(bounds, index) {
                mappings.push((code, character_start + offset as u32));
            }
        }
    }
    mappings
}

/// charset.c CODE_POINT_TO_INDEX before its `char_index_offset': the
/// position of CODE in the code space BOUNDS (one (min, max) byte range
/// per dimension, least significant first), or None when some byte lies
/// outside its range.
fn code_point_to_raw_index(bounds: &[(u32, u32)], code: u32) -> Option<u32> {
    let mut index = 0u32;
    let mut stride = 1u32;
    for (dimension, (min, max)) in bounds.iter().enumerate() {
        let byte = (code >> (8 * dimension)) & 0xFF;
        if byte < *min || byte > *max {
            return None;
        }
        index = index.checked_add((byte - min).checked_mul(stride)?)?;
        stride = stride.checked_mul(max - min + 1)?;
    }
    Some(index)
}

/// charset.c INDEX_TO_CODE_POINT: the inverse of `code_point_to_raw_index'.
fn raw_index_to_code_point(bounds: &[(u32, u32)], index: u32) -> Option<u32> {
    let mut code = 0u32;
    let mut remaining = index;
    for (dimension, (min, max)) in bounds.iter().enumerate() {
        let size = max - min + 1;
        code |= (min + remaining % size) << (8 * dimension);
        remaining /= size;
    }
    (remaining == 0).then_some(code)
}

/// A `:subset (PARENT MIN MAX OFFSET)' charset's parent and window.
fn charset_subset(interp: &Interpreter, charset: &str) -> Option<(String, i64, i64, i64)> {
    charset_info(interp, charset)?.subset.clone()
}

/// A `:superset' charset's children and their offsets.
fn charset_superset(interp: &Interpreter, charset: &str) -> Option<Vec<(String, i64)>> {
    charset_info(interp, charset)?.superset.clone()
}

/// charset.c:Fmake_char assembles position codes in most-significant-byte
/// order, filling omitted dimensions from the charset's code space.
/// charset.h:CHECK_CHARSET: a symbol naming a charset, any other object
/// `charsetp'.
pub(crate) fn check_charset<'a>(
    interp: &Interpreter,
    charset: &'a Value,
) -> Result<&'a str, LispError> {
    match charset.kind() {
        Kind::Nil | Kind::T | Kind::Symbol(_) => {
            let name = charset.as_symbol()?;
            if interp.has_charset(name) {
                return Ok(name);
            }
        }
        _ => {}
    }
    Err(LispError::WrongTypeArgument("charsetp".into(), *charset))
}

pub(crate) fn make_charset_character(
    interp: &Interpreter,
    charset: &Value,
    positions: &[Value],
) -> Result<Value, LispError> {
    let name = check_charset(interp, charset)?;
    let canonical = interp
        .charset_canonical_name(name)
        .ok_or_else(|| LispError::WrongTypeArgument("charsetp".into(), *charset))?;
    let info = charset_info(interp, &canonical)
        .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?;
    let bounds = info
        .code_space
        .as_deref()
        .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?;
    let dimension = info.dimension as usize;
    let first = positions.first().unwrap_or(&Value::Nil);
    let mut code = if first.is_nil() {
        let ascii_compatible = info.ascii_compatible_p.unwrap_or(matches!(
            canonical.as_str(),
            "ascii" | "iso-8859-1" | "unicode" | "emacs"
        ));
        if ascii_compatible {
            0
        } else {
            info.code_bounds
                .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?
                .0
        }
    } else {
        let mut code = 0u32;
        for index in 0..dimension {
            let position = positions.get(index).unwrap_or(&Value::Nil);
            let byte = if position.is_nil() {
                bounds[dimension - index - 1].0
            } else {
                let Kind::Integer(number) = position.kind() else {
                    return Err(LispError::WrongTypeArgument("wholenump".into(), *position));
                };
                if number < 0 {
                    return Err(LispError::WrongTypeArgument("wholenump".into(), *position));
                }
                if number >= 256 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        Value::Integer(255),
                        *position,
                    ])));
                }
                number as u32
            };
            code = (code << 8) | byte;
        }
        code
    };
    if iso2022::charset_iso_final(interp, &canonical).is_some() {
        code &= 0x7f7f7f7f;
    }
    let character = info
        .decode(interp, code)
        .ok_or_else(|| LispError::Signal("Invalid code(s)".into()))?;
    Ok(Value::Integer(i64::from(character)))
}

/// DECODE_CHAR: the character CODE is in CHARSET.
pub(crate) fn decode_charset_code(interp: &Interpreter, charset: &str, code: u32) -> Option<u32> {
    charset_info(interp, charset)?.decode(interp, code)
}

/// The charset's `:code-space' as (min, max) byte ranges, least
/// significant first (charset.c's CODE_POINT_TO_INDEX counts the first
/// byte fastest: jisx0208's hole 0x222F is index 108, so its non-unified
/// character is code-offset + 108, not code-offset + 0x222F).
fn charset_code_space(interp: &Interpreter, charset: &str) -> Option<Vec<(u32, u32)>> {
    charset_info(interp, charset)?.code_space.clone()
}

pub(crate) fn encode_charset_char(
    interp: &Interpreter,
    charset: &str,
    character: u32,
) -> Option<u32> {
    // CHARACTER is GNU's character number: a raw byte is #x3fff80..#x3fffff,
    // which no code space but `eight-bit' contains.
    encode_charset_character_number(interp, charset, character)
}

/// Charset membership for an actual Emacs character number. Canonical
/// string consumers must not reinterpret Unicode private-use characters
/// as the older Rust-text adapter's raw-byte sentinels.
fn encode_charset_character_number(
    interp: &Interpreter,
    charset: &str,
    character: u32,
) -> Option<u32> {
    charset_info(interp, charset)?.encode(interp, character)
}

/// coding-system-get: `plist-get' on the coding system's plist.
pub(crate) fn coding_system_property(
    interp: &Interpreter,
    coding: &str,
    property: &str,
) -> Option<Value> {
    let mut tail = interp.coding_system(coding)?.plist;
    while let Some((key, rest)) = tail.cons_values() {
        let (value, next) = rest.cons_values()?;
        if matches!(key.kind(), Kind::Symbol(name) if name == property) {
            return Some(value);
        }
        tail = next;
    }
    None
}

/// CODING_ATTR_DEFAULT_CHAR: Fdefine_coding_system_internal stores a
/// space when `:default-char' is nil.
fn coding_system_default_char(interp: &Interpreter, coding: &str) -> u32 {
    coding_system_property(interp, coding, ":default-char")
        .and_then(|value| value.integer_value())
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(u32::from(b' '))
}

fn coding_system_requires_bom(interp: &Interpreter, coding: &str) -> bool {
    coding_system_property(interp, coding, ":bom")
        .is_some_and(|value| !value.is_nil() && value.cons_values().is_none())
}

pub(crate) fn coding_system_auto_detects_bom(interp: &Interpreter, coding: &str) -> bool {
    coding_system_property(interp, coding, ":bom")
        .is_some_and(|value| value.cons_values().is_some())
}

fn coding_system_consumes_bom(interp: &Interpreter, coding: &str) -> bool {
    coding_system_requires_bom(interp, coding) || coding_system_auto_detects_bom(interp, coding)
}

pub(crate) fn coding_system_is_ascii_compatible(interp: &Interpreter, coding: &str) -> bool {
    coding_system_property(interp, coding, ":ascii-compatible-p")
        .is_some_and(|value| value.is_truthy())
}

fn coding_system_utf16_options(
    interp: &Interpreter,
    coding: &str,
    kind: &str,
) -> (bool, bool, bool) {
    let big_endian = match coding_system_property(interp, coding, ":endian").map(|v| v.kind()) {
        Some(Kind::Symbol(endian)) => endian != "little",
        _ => !matches!(kind, "utf-16le"),
    };
    let bom = coding_system_property(interp, coding, ":bom");
    let detect_bom = bom
        .as_ref()
        .is_some_and(|value| value.cons_values().is_some());
    let with_bom = bom.as_ref().is_some_and(|value| !value.is_nil())
        || (bom.is_none() && interp.coding_system_base_name(coding).as_deref() == Some("utf-16"));
    (big_endian, with_bom, detect_bom)
}

/// The (roman kana kanji ...) charsets of coding.c's Vsjis_coding_system,
/// for the sjis-char primitives.
pub(crate) fn sjis_primitive_charsets(interp: &Interpreter) -> Option<(String, String, String)> {
    let coding = interp.sjis_coding_system.clone();
    let charsets = coding_system_charset_names(interp, &coding);
    Some((
        charsets.first()?.clone(),
        charsets.get(1)?.clone(),
        charsets.get(2)?.clone(),
    ))
}

pub(crate) fn coding_system_charset_names(interp: &Interpreter, coding: &str) -> Vec<String> {
    let Some(charsets) = coding_system_property(interp, coding, ":charset-list") else {
        return Vec::new();
    };
    charsets
        .to_vec()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| value.symbol_str().map(str::to_string))
        .collect()
}

/// coding.c's work buffer for one conversion, as code_conversion_save
/// made it, with the buffer that was current then.
pub(crate) struct ConversionWorkBuffer {
    pub(crate) id: u64,
    reused: bool,
    saved_current: u64,
}

/// coding.c:code_conversion_save (true, MULTIBYTE): the reused
/// " *code-conversion-work*" buffer, made the first time and never
/// killed, or while it is in use a buffer named after it that
/// code_conversion_restore kills; made with its hooks inhibited, then
/// `inhibit-modification-hooks' local and t, erased, undo off and
/// MULTIBYTE.  The current buffer stays current.
pub(crate) fn code_conversion_save(
    interp: &mut Interpreter,
    multibyte: bool,
    env: &mut Env,
) -> Result<ConversionWorkBuffer, LispError> {
    const NAME: &str = " *code-conversion-work*";
    let saved_current = interp.current_buffer_id();
    let reused = !interp.reused_workbuf_in_use;
    let workbuf = if reused {
        match interp
            .code_conversion_reused_workbuf
            .and_then(|id| interp.buffer_object(id))
        {
            Some(buffer) => Value::Buffer(buffer),
            None => {
                let buffer = call(
                    interp,
                    "get-buffer-create",
                    &[Value::string(NAME), Value::T],
                    env,
                )?;
                interp.code_conversion_reused_workbuf = Some(interp.resolve_buffer_id(&buffer)?);
                buffer
            }
        }
    } else {
        let name = call(
            interp,
            "generate-new-buffer-name",
            &[Value::string(NAME)],
            env,
        )?;
        call(interp, "get-buffer-create", &[name, Value::T], env)?
    };
    let id = interp.resolve_buffer_id(&workbuf)?;
    interp.set_current_buffer_id(id)?;
    let prepared = (|| -> Result<(), LispError> {
        let symbol = call(
            interp,
            "make-local-variable",
            &[Value::symbol("inhibit-modification-hooks")],
            env,
        )?;
        call(interp, "set", &[symbol, Value::T], env)?;
        call(interp, "erase-buffer", &[], env)?;
        let mut buffer = interp.buffer.borrow_mut();
        buffer.set_undo_list(Value::T);
        buffer.set_multibyte(multibyte);
        Ok(())
    })();
    if reused {
        interp.reused_workbuf_in_use = true;
    }
    let work = ConversionWorkBuffer {
        id,
        reused,
        saved_current,
    };
    if let Err(error) = prepared {
        code_conversion_restore(interp, work, env)?;
        return Err(error);
    }
    interp.set_current_buffer_id(saved_current)?;
    Ok(work)
}

/// coding.c:code_conversion_restore: the reused work buffer is free again
/// (its text stays), another one is killed, and the buffer current at
/// code_conversion_save is current again.
pub(crate) fn code_conversion_restore(
    interp: &mut Interpreter,
    work: ConversionWorkBuffer,
    env: &mut Env,
) -> Result<(), LispError> {
    let killed = if work.reused {
        interp.reused_workbuf_in_use = false;
        Ok(())
    } else if let Some(buffer) = interp.buffer_object(work.id) {
        call(interp, "kill-buffer", &[Value::Buffer(buffer)], env).map(|_| ())
    } else {
        Ok(())
    };
    if interp.has_buffer_id(work.saved_current) {
        interp.set_current_buffer_id(work.saved_current)?;
    }
    killed
}

/// insert_from_string of SOURCE, its text properties included and none
/// inherited, at point in the current buffer.
fn insert_conversion_source(interp: &mut Interpreter, source: &StringLike) {
    let at = interp.buffer.borrow().point();
    let props = match &source.props {
        Some(intervals) => crate::buffer::InsertedProperties::Intervals {
            source: intervals,
            inherit: false,
        },
        None => crate::buffer::InsertedProperties::PLAIN,
    };
    interp.insert_current_buffer_with(&source.text, props, false);
    interp.set_inserted_extended_chars(at, &source.extended_chars);
}

/// The current buffer's whole text, BEG to Z, with its properties.
fn whole_buffer_text(interp: &Interpreter) -> StringLike {
    let buffer = interp.buffer.borrow();
    let z = buffer.size_total() + 1;
    StringLike {
        text: buffer.text_to_z(1),
        props: buffer.copy_intervals(1, z),
        multibyte: buffer.is_multibyte(),
        extended_chars: buffer.substring_extended_chars(1, z),
    }
}

/// encode_coding_object's pre-write conversion: SOURCE inserted in a work
/// buffer (multibyte when it has more bytes than characters), FUNCTION
/// called there through safe_calln with BEG and Z, and what is to be
/// encoded the whole text of the buffer current afterwards -- killed when
/// it is not the work buffer.  `deactivate-mark' is kept.
pub(crate) fn pre_write_convert(
    interp: &mut Interpreter,
    source: &StringLike,
    function: Value,
    env: &mut Env,
) -> Result<StringLike, LispError> {
    let old_deactivate_mark = interp
        .lookup_var("deactivate-mark", env)
        .unwrap_or(Value::Nil);
    let src_multibyte =
        source.multibyte && (!source.text.is_ascii() || !source.extended_chars.is_empty());
    let work = code_conversion_save(interp, src_multibyte, env)?;
    let converted = (|| -> Result<(StringLike, u64), LispError> {
        interp.set_current_buffer_id(work.id)?;
        insert_conversion_source(interp, source);
        let z = interp.buffer.borrow().size_total() as i64 + 1;
        interp.safe_funcall(function, &[Value::Integer(1), Value::Integer(z)], env)?;
        Ok((whole_buffer_text(interp), interp.current_buffer_id()))
    })();
    let killed = match &converted {
        Ok((_, current)) if *current != work.id => interp
            .buffer_object(*current)
            .map(|buffer| call(interp, "kill-buffer", &[Value::Buffer(buffer)], env).map(|_| ()))
            .unwrap_or(Ok(())),
        _ => Ok(()),
    };
    interp.set_variable("deactivate-mark", old_deactivate_mark, env);
    let restored = code_conversion_restore(interp, work, env);
    let (converted, _) = converted?;
    killed?;
    restored?;
    Ok(converted)
}

/// e_write's encode_coding_object of SOURCE by CODING for a file: the
/// coding's pre-write conversion, when it has one, run on the text with
/// its properties (PROPERTIES gives a buffer region's) before encoding.
pub(crate) fn pre_write_for_file(
    interp: &mut Interpreter,
    source: StringLike,
    properties: impl FnOnce(&Interpreter) -> Option<crate::intervals::Intervals>,
    coding: &str,
    env: &mut Env,
) -> Result<StringLike, LispError> {
    let pre_write = interp
        .coding_system_canonical_name(coding)
        .and_then(|canonical| coding_system_property(interp, &canonical, ":pre-write-conversion"))
        .unwrap_or(Value::Nil);
    if pre_write.is_nil() {
        return Ok(source);
    }
    let mut source = source;
    if source.props.is_none() {
        source.props = properties(interp);
    }
    pre_write_convert(interp, &source, pre_write, env)
}

/// decode_coding_object for a string destination: DECODED, with its
/// `charset' properties, is the text of a work buffer (multibyte unless
/// the coding is for unibyte text), put there a decoder call at a time
/// as a buffer destination's is when LAYOUT says the decode took more
/// than one insertion or decode_eol changed it; a post-read conversion runs
/// through safe_calln with the characters produced and point at BEG; its
/// value must be a natural number (CHECK_FIXNAT).  The result is
/// Fbuffer_string of the buffer current then.
pub(crate) fn decode_into_work_buffer(
    interp: &mut Interpreter,
    decoded: &StringLike,
    layout: Option<&DecodeLayout>,
    post_read: Value,
    env: &mut Env,
) -> Result<StringLike, LispError> {
    let old_deactivate_mark = interp
        .lookup_var("deactivate-mark", env)
        .unwrap_or(Value::Nil);
    let work = code_conversion_save(interp, decoded.multibyte, env)?;
    let result = (|| -> Result<StringLike, LispError> {
        interp.set_current_buffer_id(work.id)?;
        match layout {
            // decode_coding's insert_from_gap a call at a time into the
            // work buffer, then decode_eol's change there.
            Some(layout) => {
                let start = interp.buffer.borrow().point();
                crate::lisp::primitives::insert_converted_text_at(
                    interp,
                    start,
                    &decoded.text,
                    None,
                    &decoded.extended_chars,
                    decoded.text.chars().count(),
                    true,
                    Some(layout),
                    env,
                )?;
            }
            None => insert_conversion_source(interp, decoded),
        }
        if !post_read.is_nil() {
            let produced = decoded.text.chars().count() as i64;
            {
                let mut buffer = interp.buffer.borrow_mut();
                buffer.goto_char(1);
            }
            let value = interp.safe_funcall(post_read, &[Value::Integer(produced)], env)?;
            if !matches!(value.kind(), Kind::Integer(n) if n >= 0) {
                return Err(wrong_type_argument("wholenump", value));
            }
        }
        let buffer = interp.buffer.borrow();
        let (begv, zv) = (buffer.point_min(), buffer.point_max());
        Ok(StringLike {
            text: buffer.buffer_string(),
            props: buffer.copy_intervals(begv, zv),
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(begv, zv),
        })
    })();
    interp.set_variable("deactivate-mark", old_deactivate_mark, env);
    let restored = code_conversion_restore(interp, work, env);
    let result = result?;
    restored?;
    Ok(result)
}

/// decode_coding_object's post-read conversion in a buffer destination:
/// in the current buffer, with undo off for the call and point at DST_POS
/// where PRODUCED characters were decoded, FUNCTION through safe_calln;
/// its value must be a natural number.  Returns the characters produced,
/// grown by what the conversion inserted.
pub(crate) fn post_read_in_buffer(
    interp: &mut Interpreter,
    function: Value,
    dst_pos: usize,
    produced: usize,
    env: &mut Env,
) -> Result<usize, LispError> {
    let buffer_id = interp.current_buffer_id();
    let (prev_z, undo_list) = {
        let mut buffer = interp.buffer.borrow_mut();
        let undo_list = buffer.undo_list();
        buffer.set_undo_list(Value::T);
        buffer.goto_char(dst_pos);
        (buffer.size_total(), undo_list)
    };
    let value = interp.with_lisp_stack_roots(&undo_list, |interp| {
        interp.safe_funcall(function, &[Value::Integer(produced as i64)], env)
    });
    // coding_restore_undo_list, into the buffer that was current.
    let grown = interp.get_buffer_by_id_mut(buffer_id).map(|mut buffer| {
        buffer.set_undo_list(undo_list);
        buffer.size_total()
    });
    let value = value?;
    if !matches!(value.kind(), Kind::Integer(n) if n >= 0) {
        return Err(wrong_type_argument("wholenump", value));
    }
    Ok((produced as isize + grown.unwrap_or(prev_z) as isize - prev_z as isize).max(0) as usize)
}

/// encode_coding_charset: an ASCII character as itself when the coding is
/// ASCII-compatible, an eight-bit one as its byte, any other through the
/// first charset of the coding's list that encodes it (one to four bytes
/// by the charset's dimension), and else the default character, whose
/// low eight bits EMIT_ONE_BYTE stores.
fn encode_coding_charset(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let charsets: Vec<std::rc::Rc<CharsetInfo>> = coding_system_charset_names(interp, coding)
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect();
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    let default_char = coding_system_default_char(interp, coding) as u8;
    let mut encoded = Vec::with_capacity(codes.len());
    for &c in codes {
        if ascii_compatible && c < 0x80 {
            encoded.push(c as u8);
        } else if char_byte8_p(c) {
            encoded.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            match charsets.iter().find_map(|charset| {
                charset
                    .encode(interp, c)
                    .map(|code| (charset.dimension, code))
            }) {
                Some((dimension, code)) => {
                    let bytes = code.to_be_bytes();
                    encoded.extend_from_slice(&bytes[4 - dimension.clamp(1, 4) as usize..]);
                }
                None => encoded.push(default_char),
            }
        }
    }
    encoded
}

/// CODING_CHAR_CHARSET over a coding's charset list: the first charset
/// that encodes C, with C's code point in it.
fn coding_char_charset<'a>(
    interp: &Interpreter,
    charsets: &'a [std::rc::Rc<CharsetInfo>],
    c: u32,
) -> Option<(&'a CharsetInfo, u32)> {
    charsets
        .iter()
        .find_map(|charset| charset.encode(interp, c).map(|code| (&**charset, code)))
}

pub(crate) fn checked_coding_name(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<String>, LispError> {
    if value.is_nil() {
        return Ok(None);
    }
    let symbol = value.as_symbol()?.to_string();
    interp
        .coding_system_canonical_name(&symbol)
        .ok_or_else(|| coding_system_error(*value))
        .map(Some)
}

/// A coding system VALUE as given (an alias keeps its name), once
/// CHECK_CODING_SYSTEM accepts it; None for nil.
pub(crate) fn checked_coding_given(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<String>, LispError> {
    Ok(checked_coding_name(interp, value)?
        .map(|_| value.symbol_str().map(str::to_string).unwrap_or_default()))
}

/// coding.c raw_text_coding_system: what a unibyte buffer reads CODING
/// as -- a raw-text coding stays, one with an undecided eol becomes
/// `raw-text', and any other the raw-text subsidiary of its eol type.
pub(crate) fn raw_text_coding_system(interp: &Interpreter, coding: &str) -> String {
    if interp.coding_system_kind_name(coding).as_deref() == Some("raw-text") {
        return coding.to_string();
    }
    match coding_eol_type(interp, coding) {
        None => "raw-text".into(),
        Some(eol) => coding_variant_name(interp, "raw-text", Some(eol)),
    }
}

/// coding.h CHECK_CODING_SYSTEM: the canonical name of the coding system
/// VALUE names.  nil, which Fcheck_coding_system answers without a spec,
/// is `(wrong-type-argument coding-system-p nil)'.
pub(crate) fn checked_coding_symbol(
    interp: &Interpreter,
    value: &Value,
) -> Result<String, LispError> {
    checked_coding_name(interp, value)?
        .ok_or_else(|| LispError::WrongTypeArgument("coding-system-p".into(), *value))
}

pub(crate) fn coding_variant_name(
    interp: &Interpreter,
    base: &str,
    eol_type: Option<i64>,
) -> String {
    if let Some(eol_type) = eol_type {
        let suffix = match eol_type {
            0 => Some("unix"),
            1 => Some("dos"),
            2 => Some("mac"),
            _ => None,
        };
        if let Some(suffix) = suffix {
            let candidate = format!("{base}-{suffix}");
            if let Some(canonical) = interp.coding_system_canonical_name(&candidate) {
                return canonical;
            }
        }
    }
    interp
        .coding_system_canonical_name(base)
        .unwrap_or_else(|| base.to_string())
}

pub(crate) fn set_last_coding_system_used(interp: &mut Interpreter, coding: &str, env: &mut Env) {
    interp.set_variable(
        "last-coding-system-used",
        Value::Symbol(coding.to_string().into()),
        env,
    );
}

pub(crate) fn shared_string_copy(value: &Value) -> Result<Value, LispError> {
    // fns.c:Fcopy_sequence copies the actual payload and intervals. A Rust
    // text projection cannot preserve all Lisp characters or byte8 identity.
    // Its ordinary constructors return the normal empty singleton even
    // when the source is a distinct pure or restored zero-length header.
    let Kind::StringObject(object) = value.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *value));
    };
    let copy = object.borrow().copy_with_properties();
    Ok(Value::StringObject(copy))
}

pub(crate) fn bytes_to_unibyte_value(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::String("".into());
    }
    bytes_to_shared_unibyte_value(bytes)
}

pub(crate) fn bytes_to_shared_unibyte_value(bytes: &[u8]) -> Value {
    Value::StringObject(StringObjectRef::from_unibyte(bytes.to_vec()))
}

/// gen_tempname's random segment: exactly six characters from
/// [a-zA-Z0-9].  Both `make-temp-name' and `make-temp-file-internal'
/// draw from here; the SHAPE is observable (tests embed fixture paths in
/// compared output), so the six-character form matters, not just
/// uniqueness.
pub(crate) fn random_temp_suffix() -> String {
    const LETTERS: &[u8; 62] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let counter = TEMP_NAME_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // gen_tempname draws its own bits; `random' is left as it was.
    let mut seed = (nanos as u64)
        ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(std::process::id()).rotate_left(32);
    let mut suffix = String::with_capacity(6);
    for _ in 0..6 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        suffix.push(char::from(LETTERS[(seed >> 33) as usize % 62]));
    }
    suffix
}

pub(crate) fn make_temp_name(prefix: &str) -> String {
    // fileio.c Fmake_temp_name -> gen_tempname (GT_NOCREATE): PREFIX plus
    // the six-character random segment, regenerated while a file of that
    // name exists.
    let mut candidate = String::new();
    for _ in 0..62 * 62 {
        candidate = format!("{prefix}{}", random_temp_suffix());
        if !std::path::Path::new(&candidate).exists() {
            return candidate;
        }
    }
    candidate
}

pub(crate) fn aset_vector_value(
    target: &Value,
    index: usize,
    new_value: Value,
) -> Result<(), LispError> {
    let Kind::Vector(vector) = target.kind() else {
        return Err(LispError::WrongTypeArgument("arrayp".into(), *target));
    };
    if index >= vector.len() {
        return Err(LispError::Signal("Args out of range".into()));
    }
    vector.set(index, new_value);
    Ok(())
}

pub(crate) fn base64_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C)
}

pub(crate) fn base64_char_value(byte: u8, base64url: bool) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' if !base64url => Some(62),
        b'/' if !base64url => Some(63),
        b'-' if base64url => Some(62),
        b'_' if base64url => Some(63),
        _ => None,
    }
}

pub(crate) fn base64_value_char(value: u8, base64url: bool) -> char {
    const STANDARD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let alphabet = if base64url { URL } else { STANDARD };
    alphabet[value as usize] as char
}

pub(crate) fn encode_base64_bytes(
    bytes: &[u8],
    line_break: bool,
    pad: bool,
    base64url: bool,
) -> String {
    const GROUPS_PER_LINE: usize = 76 / 4;

    let mut encoded = String::with_capacity(bytes.len() + (bytes.len() / 3) + 8);
    let mut cursor = 0usize;
    let mut groups_on_line = 0usize;

    while cursor < bytes.len() {
        if line_break && groups_on_line == GROUPS_PER_LINE {
            encoded.push('\n');
            groups_on_line = 0;
        }

        let first = bytes[cursor];
        cursor += 1;
        encoded.push(base64_value_char(first >> 2, base64url));
        let mut value = (first & 0x03) << 4;

        if cursor == bytes.len() {
            encoded.push(base64_value_char(value, base64url));
            if pad {
                encoded.push('=');
                encoded.push('=');
            }
            break;
        }

        let second = bytes[cursor];
        cursor += 1;
        encoded.push(base64_value_char(value | (second >> 4), base64url));
        value = (second & 0x0F) << 2;

        if cursor == bytes.len() {
            encoded.push(base64_value_char(value, base64url));
            if pad {
                encoded.push('=');
            }
            break;
        }

        let third = bytes[cursor];
        cursor += 1;
        encoded.push(base64_value_char(value | (third >> 6), base64url));
        encoded.push(base64_value_char(third & 0x3F, base64url));
        groups_on_line += 1;
    }

    encoded
}

/// fns.c base64_encode_1's reading of its source: the characters of a
/// multibyte source as bytes, an eight-bit character as its byte, any other
/// non-ASCII character failing the encoding.
pub(crate) fn encode_base64_source_bytes(
    codes: impl IntoIterator<Item = i64>,
    multibyte: bool,
) -> Result<Vec<u8>, LispError> {
    codes
        .into_iter()
        .map(|code| {
            if !multibyte {
                Ok(code as u8)
            } else if (0x3F_FF80..=0x3F_FFFF).contains(&code) {
                Ok((code - 0x3F_FF00) as u8)
            } else if code >= 128 {
                Err(LispError::Signal(
                    "Multibyte character in data for base64 encoding".into(),
                ))
            } else {
                Ok(code as u8)
            }
        })
        .collect()
}

pub(crate) fn base64_encode_string_value(
    value: &Value,
    line_break: bool,
    pad: bool,
    base64url: bool,
) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let bytes = encode_base64_source_bytes(string.character_codes_iter(), string.multibyte)?;
    Ok(Value::String(
        encode_base64_bytes(&bytes, line_break, pad, base64url).into(),
    ))
}

pub(crate) fn base64_encode_region_value(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    line_break: bool,
    pad: bool,
    base64url: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    let codes = buffer_region_codes(interp, lo, hi);
    let multibyte = interp.buffer.borrow().is_multibyte();
    let bytes = encode_base64_source_bytes(codes.into_iter().map(i64::from), multibyte)?;
    let encoded = encode_base64_bytes(&bytes, line_break, pad, base64url);
    let inserted = insert_then_delete_region(interp, lo, hi, &encoded, env)?;
    Ok(Value::Integer(inserted as i64))
}

/// fns.c's base64 region edit: the new text inserted before the old
/// (keeping the markers with it), then the old deleted, each a change of
/// its own; point is restored when outside the region, else at its start.
/// The characters inserted.
fn insert_then_delete_region(
    interp: &mut Interpreter,
    lo: usize,
    hi: usize,
    text: &str,
    env: &mut Env,
) -> Result<usize, LispError> {
    let old_pos = interp.buffer.borrow().point();
    interp.buffer.borrow_mut().goto_char(lo);
    insert_text_with_hooks(interp, text, None, &[], false, false, env)?;
    let inserted = text.chars().count();
    delete_region_with_hooks(interp, lo + inserted, hi + inserted, env)?;
    let pos = if old_pos >= hi {
        old_pos + inserted - (hi - lo)
    } else if old_pos > lo {
        lo
    } else {
        old_pos
    };
    let zv = interp.buffer.borrow().point_max();
    interp.buffer.borrow_mut().goto_char(pos.min(zv));
    Ok(inserted)
}

pub(crate) fn next_base64_byte(
    bytes: &[u8],
    cursor: &mut usize,
    base64url: bool,
    ignore_invalid: bool,
) -> Result<Option<u8>, LispError> {
    while let Some(&byte) = bytes.get(*cursor) {
        *cursor += 1;
        if base64_whitespace(byte) {
            continue;
        }
        if let Some(value) = base64_char_value(byte, base64url) {
            return Ok(Some(value));
        }
        if ignore_invalid {
            continue;
        }
        return Err(LispError::Signal("Invalid base64 data".into()));
    }
    Ok(None)
}

pub(crate) fn next_base64_tail(
    bytes: &[u8],
    cursor: &mut usize,
    base64url: bool,
    ignore_invalid: bool,
) -> Result<Option<Base64Tail>, LispError> {
    while let Some(&byte) = bytes.get(*cursor) {
        *cursor += 1;
        if base64_whitespace(byte) {
            continue;
        }
        if let Some(value) = base64_char_value(byte, base64url) {
            return Ok(Some(Base64Tail::Value(value)));
        }
        if !ignore_invalid && byte == b'=' {
            return Ok(Some(Base64Tail::Padding));
        }
        if ignore_invalid {
            continue;
        }
        return Err(LispError::Signal("Invalid base64 data".into()));
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Base64Tail {
    Value(u8),
    Padding,
}

pub(crate) fn decode_base64_string_value(
    value: &Value,
    base64url: bool,
    ignore_invalid: bool,
) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let bytes = encode_raw_text_bytes(&string.text)?;
    let mut cursor = 0usize;
    let mut decoded = Vec::with_capacity((bytes.len() / 4) * 3);

    while let Some(first) = next_base64_byte(&bytes, &mut cursor, base64url, ignore_invalid)? {
        let Some(second) = next_base64_byte(&bytes, &mut cursor, base64url, ignore_invalid)? else {
            return Err(LispError::Signal("Invalid base64 data".into()));
        };
        decoded.push((first << 2) | (second >> 4));

        match next_base64_tail(&bytes, &mut cursor, base64url, ignore_invalid)? {
            Some(Base64Tail::Value(third)) => {
                decoded.push(((second & 0x0F) << 4) | (third >> 2));
                match next_base64_tail(&bytes, &mut cursor, base64url, ignore_invalid)? {
                    Some(Base64Tail::Value(fourth)) => {
                        decoded.push(((third & 0x03) << 6) | fourth);
                    }
                    Some(Base64Tail::Padding) => {}
                    None if base64url || ignore_invalid => break,
                    None => return Err(LispError::Signal("Invalid base64 data".into())),
                }
            }
            Some(Base64Tail::Padding) => {
                match next_base64_tail(&bytes, &mut cursor, base64url, ignore_invalid)? {
                    Some(Base64Tail::Padding) => {}
                    _ => return Err(LispError::Signal("Invalid base64 data".into())),
                }
            }
            None if base64url || ignore_invalid => break,
            None => return Err(LispError::Signal("Invalid base64 data".into())),
        }
    }

    Ok(bytes_to_unibyte_value(&decoded))
}

pub(crate) fn base64_decode_region_value(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    base64url: bool,
    ignore_invalid: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    let text = interp
        .buffer
        .borrow()
        .buffer_substring(lo, hi)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let decoded =
        decode_base64_string_value(&Value::String(text.into()), base64url, ignore_invalid)?;
    let decoded_text = string_text(&decoded)?;
    let inserted = insert_then_delete_region(interp, lo, hi, &decoded_text, env)?;
    Ok(Value::Integer(inserted as i64))
}

pub(crate) fn encode_raw_text_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::new();
    for ch in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            bytes.push(byte);
        } else if (ch as u32) <= 0xFF {
            bytes.push(ch as u8);
        } else if ch == json::INVALID_UNICODE_SENTINEL {
            return Err(LispError::TypeError("character".into(), "string".into()));
        } else {
            return Err(LispError::Signal("Character cannot be encoded".into()));
        }
    }
    Ok(bytes)
}

/// Bytes stored by a multibyte Emacs buffer when conversion is disabled.
/// Ordinary characters use Emacs's UTF-8-compatible internal spelling;
/// byte8 marker characters remain their original single octet.  Treating
/// every Latin-1 character as a byte corrupts no-conversion `.elc' output
/// (for example U+00A7 must be C2 A7, not A7).
pub(crate) fn encode_internal_multibyte_bytes(text: &str) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::with_capacity(text.len());
    for ch in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            bytes.push(byte);
        } else if ch == json::INVALID_UNICODE_SENTINEL {
            return Err(LispError::TypeError("character".into(), "string".into()));
        } else {
            let mut encoded = [0u8; 4];
            bytes.extend_from_slice(ch.encode_utf8(&mut encoded).as_bytes());
        }
    }
    Ok(bytes)
}

/// coding.h:567 SJIS_TO_JIS.
pub(crate) fn sjis_to_jis(code: u32) -> u32 {
    let (s1, s2) = (code >> 8, code & 0xFF);
    let (j1, j2) = if s2 >= 0x9F {
        (s1 * 2 - if s1 >= 0xE0 { 0x160 } else { 0xE0 }, s2 - 0x7E)
    } else {
        (
            s1 * 2 - if s1 >= 0xE0 { 0x161 } else { 0xE1 },
            s2 - if s2 >= 0x7F { 0x20 } else { 0x1F },
        )
    };
    (j1 << 8) | j2
}

/// coding.h:608 JIS_TO_SJIS.  Like GNU, this is applied blindly to
/// whatever code the charset search returned -- a katakana-jisx0201
/// code 0x31 becomes 0x70AF through the same arithmetic, and the
/// oracle's encode-sjis-char answers exactly that.
pub(crate) fn jis_to_sjis(code: u32) -> u32 {
    let (j1, j2) = (code >> 8, code & 0xFF);
    let (s1, s2) = if j1 & 1 != 0 {
        (
            j1 / 2 + if j1 < 0x5F { 0x71 } else { 0xB1 },
            j2 + if j2 >= 0x60 { 0x20 } else { 0x1F },
        )
    } else {
        (j1 / 2 + if j1 < 0x5F { 0x70 } else { 0xB0 }, j2 + 0x7E)
    };
    (s1 << 8) | s2
}

/// coding.h JIS_TO_SJIS2, for the fourth (JIS X 0213 plane 2) charset.
fn jis_to_sjis2(code: u32) -> u32 {
    let (j1, j2) = (code >> 8, code & 0xFF);
    let (s1, s2) = if j1 & 1 != 0 {
        (
            if j1 <= 0x25 {
                0xF0 + (j1 - 0x21) / 2
            } else if j1 <= 0x2F {
                0xF3 + (j1 - 0x2D) / 2
            } else {
                0xF5 + (j1 - 0x6F) / 2
            },
            j2 + if j2 >= 0x60 { 0x20 } else { 0x1F },
        )
    } else {
        (
            match j1 {
                0x28 => 0xF0,
                0x24 => 0xF1,
                0x2C => 0xF2,
                0x2E => 0xF3,
                _ => 0xF4 + (j1 - 0x6E) / 2,
            },
            j2 + 0x7E,
        )
    };
    (s1 << 8) | s2
}

/// encode_coding_sjis over the coding's (roman kana kanji [kanji2])
/// charsets: an ASCII character as itself when the coding is
/// ASCII-compatible, an eight-bit one as its byte; otherwise the charset
/// list's first charset for the character, or for the default character
/// when none has it: kanji through JIS_TO_SJIS, kana as its code + 0x80,
/// kanji2 through JIS_TO_SJIS2 in the rows JIS X 0213 plane 2 occupies,
/// anything else as its low seven bits.
fn encode_coding_sjis(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let names = coding_system_charset_names(interp, coding);
    let charsets: Vec<std::rc::Rc<CharsetInfo>> = names
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect();
    let kana = names.get(1).map(String::as_str);
    let kanji = names.get(2).map(String::as_str);
    let kanji2 = names.get(3).map(String::as_str);
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    let default_char = coding_system_default_char(interp, coding);
    let mut out = Vec::with_capacity(codes.len());
    for &c in codes {
        if c < 0x80 && ascii_compatible {
            out.push(c as u8);
        } else if char_byte8_p(c) {
            out.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            let Some((charset, code)) = coding_char_charset(interp, &charsets, c)
                .or_else(|| coding_char_charset(interp, &charsets, default_char))
            else {
                continue;
            };
            let name = Some(charset.name.as_str());
            if name == kanji {
                let sjis = jis_to_sjis(code);
                out.extend([(sjis >> 8) as u8, (sjis & 0xFF) as u8]);
            } else if name == kana {
                out.push((code | 0x80) as u8);
            } else if kanji2.is_some() && name == kanji2 {
                let c1 = code >> 8;
                if c1 == 0x21
                    || (0x23..=0x25).contains(&c1)
                    || c1 == 0x28
                    || (0x2C..=0x2F).contains(&c1)
                    || c1 >= 0x6E
                {
                    let sjis = jis_to_sjis2(code);
                    out.extend([(sjis >> 8) as u8, (sjis & 0xFF) as u8]);
                } else {
                    out.push((code & 0x7F) as u8);
                }
            } else {
                out.push((code & 0x7F) as u8);
            }
        }
    }
    out
}

/// coding.h SJIS_TO_JIS2 for the fourth (JIS X 0213 plane 2) charset.
fn sjis_to_jis2(code: u32) -> u32 {
    let (s1, s2) = (code >> 8, code & 0xFF);
    let (j1, j2) = if s2 >= 0x9F {
        (
            match s1 {
                0xF0 => 0x28,
                0xF1 => 0x24,
                0xF2 => 0x2C,
                0xF3 => 0x2E,
                _ => 0x6E + (s1 - 0xF4) * 2,
            },
            s2 - 0x7E,
        )
    } else {
        (
            if s1 <= 0xF2 {
                0x21 + (s1 - 0xF0) * 2
            } else if s1 <= 0xF4 {
                0x2D + (s1 - 0xF3) * 2
            } else {
                0x6F + (s1 - 0xF5) * 2
            },
            s2 - if s2 >= 0x7F { 0x20 } else { 0x1F },
        )
    };
    (j1 << 8) | j2
}

/// encode_coding_big5 over the coding's (roman big5) charsets: an ASCII
/// character as itself when the coding is ASCII-compatible, an eight-bit
/// one as its byte; otherwise the list's first charset for the character
/// or else for the default character: big5 as its two-byte code, any
/// other charset as the code's low seven bits.
fn encode_coding_big5(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let names = coding_system_charset_names(interp, coding);
    let charsets: Vec<std::rc::Rc<CharsetInfo>> = names
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect();
    let big5 = names.get(1).map(String::as_str);
    let ascii_compatible = coding_system_is_ascii_compatible(interp, coding);
    let default_char = coding_system_default_char(interp, coding);
    let mut out = Vec::with_capacity(codes.len());
    for &c in codes {
        if c < 0x80 && ascii_compatible {
            out.push(c as u8);
        } else if char_byte8_p(c) {
            out.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            let Some((charset, code)) = coding_char_charset(interp, &charsets, c)
                .or_else(|| coding_char_charset(interp, &charsets, default_char))
            else {
                continue;
            };
            if Some(charset.name.as_str()) == big5 {
                out.extend([(code >> 8) as u8, (code & 0xFF) as u8]);
            } else {
                out.push((code & 0x7F) as u8);
            }
        }
    }
    out
}

/// The bytes of BYTES above 0x7F.
pub(crate) fn high_byte_count(bytes: &[u8]) -> usize {
    bytes
        .chunks(1 << 20)
        .map(|chunk| chunk.iter().map(|&byte| u32::from(byte >> 7)).sum::<u32>() as usize)
        .sum()
}

/// The text view of BYTES written into DST, its length: an ASCII byte
/// as itself, any other as the three bytes of its raw-byte character
/// (EE 82 80 to EE 83 BF), a word of ASCII bytes at a time.
pub(crate) fn write_raw_byte_text(dst: &mut [u8], bytes: &[u8]) {
    let mut at = 0;
    for chunk in bytes.chunks(8) {
        if let Ok(word) = <[u8; 8]>::try_from(chunk)
            && u64::from_ne_bytes(word) & 0x8080_8080_8080_8080 == 0
        {
            dst[at..at + 8].copy_from_slice(chunk);
            at += 8;
            continue;
        }
        for &byte in chunk {
            if byte < 0x80 {
                dst[at] = byte;
                at += 1;
            } else {
                dst[at..at + 3].copy_from_slice(&[0xEE, 0x80 | (byte >> 6), 0x80 | (byte & 0x3F)]);
                at += 3;
            }
        }
    }
    debug_assert_eq!(at, dst.len());
}

/// coding.c decode_coding_raw_text: each byte as itself, a non-ASCII one
/// as its eight-bit character.
pub(crate) fn decode_coding_raw_text(bytes: &[u8]) -> DecodedText {
    let mut out = DecodedText::new();
    out.push_raw_bytes(bytes);
    out
}

pub(crate) fn decode_raw_text_bytes(bytes: &[u8]) -> String {
    decode_coding_raw_text(bytes).text
}

/// coding.c decode_coding_utf_8 over a whole unibyte text.
pub(crate) fn decode_coding_utf_8(bytes: &[u8]) -> DecodedText {
    decoders::decode_utf_8_whole(bytes)
}

pub(crate) fn decode_utf8_bytes(bytes: &[u8]) -> String {
    decode_coding_utf_8(bytes).text
}

/// coding.c consume_chars: the character codes an encoder reads from
/// SOURCE.  A multibyte source gives its characters, an eight-bit one as
/// #x3FFF80..#x3FFFFF.  A unibyte source gives the raw-text encoder its
/// bytes; every other encoder gets each internal multibyte sequence the
/// bytes spell as that character and the remaining bytes as eight-bit
/// characters.  A newline becomes CR LF under a dos EOL_TYPE and CR under
/// mac; SELECTIVE_DISPLAY (write-region in a buffer whose
/// `selective-display' is t) reads a CR as a newline first.  Then
/// TRANSLATION, get_translation_table's tables for the encoder (none for
/// the raw-text one), maps each character.  PREFERRED, the `charset'
/// property of each source character, comes back aligned with the codes
/// (empty when PREFERRED is).
fn consume_chars(
    source: &StringLike,
    eol_type: Option<i64>,
    raw_text_encoder: bool,
    selective_display: bool,
    translation: &[Value],
    preferred: &[Option<String>],
) -> (Vec<u32>, Vec<Option<String>>) {
    let translation = if raw_text_encoder { &[] } else { translation };
    if source.multibyte && translation.is_empty() && preferred.is_empty() {
        // No table and no annotation: each character goes in as it is, a
        // newline as the eol type makes it.
        let mut codes = Vec::with_capacity(source.text.len());
        for code in source.character_codes_iter() {
            let mut c = code as u32;
            if c == u32::from(b'\r') && selective_display {
                c = u32::from(b'\n');
            }
            if c == u32::from(b'\n') {
                match eol_type {
                    Some(1) => codes.push(u32::from(b'\r')),
                    Some(2) => c = u32::from(b'\r'),
                    _ => {}
                }
            }
            codes.push(c);
        }
        return (codes, Vec::new());
    }
    // Each character as the source spells it, with the index of the
    // source position it starts at.
    let mut chars: Vec<(u32, usize)> = Vec::with_capacity(source.text.len());
    if source.multibyte {
        chars.extend(
            source
                .character_codes_iter()
                .enumerate()
                .map(|(index, code)| (code as u32, index)),
        );
    } else {
        let bytes: Vec<u8> = source
            .text
            .chars()
            .map(|ch| raw_byte_from_regex_char(ch).unwrap_or(ch as u8))
            .collect();
        if raw_text_encoder {
            chars.extend(
                bytes
                    .iter()
                    .enumerate()
                    .map(|(index, &byte)| (u32::from(byte), index)),
            );
        } else {
            let mut pos = 0;
            while pos < bytes.len() {
                let len = crate::lisp::types::string_data::multibyte_length(&bytes[pos..], true);
                if len > 0 {
                    let (c, _) =
                        crate::lisp::types::string_data::decode_character(&bytes[pos..pos + len])
                            .expect("multibyte_length validated the sequence");
                    chars.push((c, pos));
                    pos += len;
                } else {
                    // BYTE8_TO_CHAR.
                    chars.push((RAW_BYTE8_BASE + u32::from(bytes[pos]), pos));
                    pos += 1;
                }
            }
        }
    }
    let max_lookup = translation_max_lookup(translation);
    let annotate = !preferred.is_empty();
    let mut codes = Vec::with_capacity(chars.len());
    let mut aligned = Vec::with_capacity(if annotate { chars.len() } else { 0 });
    let mut next = 0;
    while next < chars.len() {
        let (mut c, index) = chars[next];
        next += 1;
        if c == u32::from(b'\r') && selective_display {
            c = u32::from(b'\n');
        }
        let preference = annotate.then(|| preferred.get(index).cloned().flatten());
        if c == u32::from(b'\n') {
            match eol_type {
                Some(1) => {
                    codes.push(u32::from(b'\r'));
                    if let Some(preference) = &preference {
                        aligned.push(preference.clone());
                    }
                }
                Some(2) => c = u32::from(b'\r'),
                _ => {}
            }
        }
        // LOOKUP_TRANSLATION_TABLE: a character entry replaces C and the
        // next table sees it; any other entry is a sequence translation.
        let mut trans = Value::Nil;
        for table in translation {
            if let Kind::CharTable(table) = table.kind() {
                trans = table.get(c);
                if let Some(to) = translation_character(trans) {
                    c = to;
                    trans = Value::Nil;
                } else if !trans.is_nil() {
                    break;
                }
            }
        }
        let start = codes.len();
        if trans.is_nil() {
            codes.push(c);
        } else {
            let lookup: Vec<u32> = std::iter::once(c)
                .chain(chars[next..].iter().map(|&(code, _)| code))
                .take(max_lookup)
                .collect();
            match get_translation(trans, &lookup) {
                Some((to, from_nchars)) => {
                    if let Some(to) = translation_character(to) {
                        codes.push(to);
                    } else if let Kind::Vector(items) = to.kind() {
                        codes.extend(
                            items
                                .slots()
                                .filter_map(|item| item.integer_value())
                                .map(|item| item as u32),
                        );
                    } else {
                        codes.push(c);
                    }
                    next += from_nchars - 1;
                }
                // consume_chars stops consuming at a sequence it cannot
                // complete (and its caller would retry it forever); the
                // character goes through untranslated instead.
                None => codes.push(c),
            }
        }
        if let Some(preference) = preference {
            aligned.extend(std::iter::repeat_n(preference, codes.len() - start));
        }
    }
    (codes, aligned)
}

/// CHARACTERP of a translation table entry.
fn translation_character(value: Value) -> Option<u32> {
    value
        .integer_value()
        .filter(|&code| (0..=i64::from(RAW_BYTE8_BASE + 0xFF)).contains(&code))
        .map(|code| code as u32)
}

/// get_translation_table's MAX_LOOKUP: the longest source sequence a
/// table's second extra slot declares, at least one character.
fn translation_max_lookup(tables: &[Value]) -> usize {
    tables
        .iter()
        .filter_map(|table| match table.kind() {
            Kind::CharTable(table) if table.extra_count() > 1 => table.extra(1),
            _ => None,
        })
        .filter_map(|value| value.integer_value())
        .filter_map(|value| usize::try_from(value).ok())
        .fold(1, |longest, value| longest.max(value.min(4096)))
}

/// coding.c get_translation: a character or vector entry translates the
/// one character; an alist of (FROM-VECTOR . TO) the first FROM sequence
/// LOOKUP starts with.  The value and the number of characters it
/// replaces, or None when no sequence completes.
fn get_translation(trans: Value, lookup: &[u32]) -> Option<(Value, usize)> {
    if trans.integer_value().is_some() || matches!(trans.kind(), Kind::Vector(_)) {
        return Some((trans, 1));
    }
    let mut tail = trans;
    while let Some((entry, rest)) = tail.cons_values() {
        tail = rest;
        let (from, to) = entry.cons_values()?;
        let Kind::Vector(from) = from.kind() else {
            continue;
        };
        let len = from.len();
        let matches = len <= lookup.len()
            && from
                .slots()
                .zip(lookup)
                .all(|(item, &c)| item.integer_value() == Some(i64::from(c)));
        if len > lookup.len() {
            return None;
        }
        if matches {
            return Some((to, len.max(1)));
        }
    }
    None
}

/// CHAR_BYTE8_P.
fn char_byte8_p(c: u32) -> bool {
    RAW_BYTE8_CHARS.contains(&c)
}

/// CHAR_STRING (CHAR_STRING_ADVANCE_NO_UNIFY): C's internal spelling, an
/// eight-bit character as its byte (encode_coding_utf_8 and
/// encode_coding_raw_text for a multibyte source).
fn emit_internal_chars(out: &mut Vec<u8>, codes: &[u32]) -> Result<(), LispError> {
    for &c in codes {
        if c < 0x80 {
            out.push(c as u8);
        } else if char_byte8_p(c) {
            out.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            let (encoded, width) = crate::lisp::types::string_data::encode_character(c)?;
            out.extend_from_slice(&encoded[..width]);
        }
    }
    Ok(())
}

/// encode_coding_raw_text: a multibyte source's characters in their
/// internal spelling, a unibyte source's bytes as they are.
fn encode_coding_raw_text(codes: &[u32], src_multibyte: bool) -> Result<Vec<u8>, LispError> {
    if !src_multibyte {
        return Ok(codes.iter().map(|&c| c as u8).collect());
    }
    let mut out = Vec::with_capacity(codes.len());
    emit_internal_chars(&mut out, codes)?;
    Ok(out)
}

/// encode_coding_utf_8.
fn encode_coding_utf_8(
    codes: &[u32],
    bom: bool,
    spec: &mut EncoderSpec,
) -> Result<Vec<u8>, LispError> {
    let mut out = Vec::with_capacity(codes.len() + 3);
    // CODING_UTF_8_BOM becomes utf_without_bom once the BOM is written.
    if bom && !spec.bom_done {
        out.extend_from_slice(&[0xEF, 0xBB, 0xBF]);
    }
    spec.bom_done = true;
    emit_internal_chars(&mut out, codes)?;
    Ok(out)
}

/// Encode SOURCE with CODING. Its multibyte flag distinguishes characters
/// from octets: encode_coding_raw_text emits a multibyte source's non-ASCII
/// characters in their internal (utf-8-emacs) spelling and a unibyte
/// source's characters as their bytes.
pub(crate) fn encode_text_bytes(
    interp: &Interpreter,
    source: &StringLike,
    coding: &str,
    inhibit_eol_conversion: bool,
    selective_display: bool,
    translation: &[Value],
) -> Result<Vec<u8>, LispError> {
    encode_text_bytes_with_charsets(
        interp,
        source,
        coding,
        inhibit_eol_conversion,
        selective_display,
        translation,
        &[],
        None,
    )
}

/// encode_coding: consume_chars, then the encoder of CODING's type.
/// TRANSLATION is get_translation_table's for CODING
/// (`encode_translation_tables').  PREFERRED holds the per-character
/// `charset' text property values of SOURCE, which the ISO-2022 encoder
/// consumes as CODING_ANNOTATE_CHARSET annotations.
/// BLOCK, when given, is a stream's encoder state, the encoding its last
/// block only when its CODING_MODE_LAST_BLOCK is set (send_process leaves
/// it unset); otherwise the text is encoded whole from the initial state.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_text_bytes_with_charsets(
    interp: &Interpreter,
    source: &StringLike,
    coding: &str,
    inhibit_eol_conversion: bool,
    selective_display: bool,
    translation: &[Value],
    preferred: &[Option<String>],
    block: Option<&mut EncoderSpec>,
) -> Result<Vec<u8>, LispError> {
    let last = block.is_none();
    let mut whole = EncoderSpec::default();
    let spec = block.unwrap_or(&mut whole);
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_else(|| canonical.clone());
    // setup_coding_system chooses the encoder by the coding type.
    let encoder = kind.as_str();
    let eol_type = (!inhibit_eol_conversion)
        .then(|| interp.coding_system_eol_type_value(&canonical))
        .flatten();
    // consume_chars hands the raw-text and CCL encoders a unibyte
    // source's bytes undecoded.
    let raw_text_encoder = matches!(encoder, "raw-text" | "undecided" | "ccl");
    let preferred = if encoder == "iso-2022" {
        preferred
    } else {
        &[]
    };
    // encode_coding_utf_8 writes a Unicode character as its UTF-8, which
    // is how a multibyte text of such characters already spells it: with
    // no translation, nothing for selective display and no eight-bit or
    // non-Unicode character, the text's bytes are the encoding, each LF
    // first made what the eol type makes it.
    if encoder == "utf-8"
        && source.multibyte
        && source.extended_chars.is_empty()
        && translation.is_empty()
        && !(selective_display && source.text.contains('\r'))
        && !crate::lisp::types::string_data::has_raw_byte_markers(source.text.as_bytes())
    {
        let bom = (kind == "utf-8-with-signature"
            || coding_system_property(interp, &canonical, ":bom").is_some_and(|bom| !bom.is_nil()))
            && !spec.bom_done;
        spec.bom_done = true;
        let text = source.text.as_bytes();
        let mut out = Vec::with_capacity(text.len() + 3);
        if bom {
            out.extend_from_slice(&[0xEF, 0xBB, 0xBF]);
        }
        match eol_type {
            Some(eol @ (1 | 2)) => {
                let mut copied = 0;
                for at in memchr::memchr_iter(b'\n', text) {
                    out.extend_from_slice(&text[copied..at]);
                    out.extend_from_slice(if eol == 1 { b"\r\n" } else { b"\r" });
                    copied = at + 1;
                }
                out.extend_from_slice(&text[copied..]);
            }
            _ => out.extend_from_slice(text),
        }
        return Ok(out);
    }
    let (codes, preferred) = consume_chars(
        source,
        eol_type,
        raw_text_encoder,
        selective_display,
        translation,
        preferred,
    );
    match encoder {
        // encode_coding_utf_8 writes the BOM unless CODING_UTF_8_BOM is
        // utf_without_bom: a `:bom' of t, or the (WITH . WITHOUT) pair of
        // an auto-detecting coding.
        "utf-8" => encode_coding_utf_8(
            &codes,
            kind == "utf-8-with-signature"
                || coding_system_property(interp, &canonical, ":bom")
                    .is_some_and(|bom| !bom.is_nil()),
            spec,
        ),
        "utf-16" => {
            let (big_endian, with_bom, _) = coding_system_utf16_options(interp, &canonical, &kind);
            let default_char = coding_system_default_char(interp, &canonical);
            // CODING_UTF_16_BOM becomes utf_without_bom once written.
            let bom = with_bom && !spec.bom_done;
            spec.bom_done = true;
            Ok(encode_coding_utf_16(&codes, big_endian, bom, default_char))
        }
        "iso-2022" => iso2022::encode(
            interp,
            &codes,
            &canonical,
            eol_type,
            &preferred,
            &mut spec.iso2022,
            last,
        ),
        "charset" => Ok(encode_coding_charset(interp, &codes, &canonical)),
        "shift-jis" => Ok(encode_coding_sjis(interp, &codes, &canonical)),
        "big5" => Ok(encode_coding_big5(interp, &codes, &canonical)),
        "emacs-mule" => Ok(encode_coding_emacs_mule(interp, &codes, &canonical)),
        // raw-text, undecided and (with no CCL interpreter) ccl all encode
        // through encode_coding_raw_text.
        _ => encode_coding_raw_text(&codes, source.multibyte),
    }
}

/// encode_coding_utf_16: a character beyond MAX_UNICODE_CHAR (eight-bit
/// ones included) is the coding's default character.
fn encode_coding_utf_16(codes: &[u32], big_endian: bool, bom: bool, default_char: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(codes.len() * 2 + 2);
    let mut emit = |unit: u32| {
        let unit = unit as u16;
        if big_endian {
            out.extend_from_slice(&unit.to_be_bytes());
        } else {
            out.extend_from_slice(&unit.to_le_bytes());
        }
    };
    if bom {
        emit(0xFEFF);
    }
    for &c in codes {
        let c = if c > MAX_UNICODE_CHAR {
            default_char
        } else {
            c
        };
        if c < 0x10000 {
            emit(c);
        } else {
            let c = c - 0x10000;
            emit((c >> 10) + 0xD800);
            emit((c & 0x3FF) + 0xDC00);
        }
    }
    out
}

/// detect_coding for the utf-8-auto and utf-16-auto categories (a utf-8
/// or utf-16 coding whose `:bom' is a cons): the coding the source's
/// signature picks from that cons, carrying CODING's own eol type over
/// (adjust_coding_eol_type); None when CODING stays.
pub(crate) fn detect_auto_bom_coding(
    interp: &Interpreter,
    coding: &str,
    source: &detect::DetectSource,
    env: &Env,
) -> Option<String> {
    if !coding_system_auto_detects_bom(interp, coding)
        || interp.coding_system_kind_name(coding).as_deref() == Some("undecided")
    {
        return None;
    }
    let found = detect::detect_coding(interp, source, coding, env)?;
    Some(adjust_coding_eol_type(
        interp,
        &found,
        interp.coding_system_eol_type_value(coding),
    ))
}

/// setup_coding_system (CODING) then adjust_coding_eol_type for EOL: a
/// coding whose eol is undecided becomes its EOL subsidiary (that of the
/// coding an alias names, whose eol-type vector the alias shares), one
/// whose eol is settled stays.
pub(crate) fn adjust_coding_eol_type(
    interp: &Interpreter,
    coding: &str,
    eol: Option<i64>,
) -> String {
    match eol {
        Some(eol) if coding_eol_type(interp, coding).is_none() => {
            let canonical = interp
                .coding_system_canonical_name(coding)
                .unwrap_or_else(|| coding.to_string());
            let base = interp
                .coding_system_base_name(&canonical)
                .unwrap_or(canonical);
            coding_variant_name(interp, &base, Some(eol))
        }
        _ => coding.to_string(),
    }
}

/// What a decoder reads and leaves of coding.c's `struct coding_system'
/// for one block of a longer text.  LAST is CODING_MODE_LAST_BLOCK: what
/// the decoder could not finish at the end is decode_coding's to flush as
/// binary characters, where otherwise it stays unconsumed for the next
/// block; EOL_DOS is a dos eol without `inhibit-eol-conversion', under
/// which a decoder reads the unit after a CR with it.  SPEC is the
/// coding->spec state one block leaves the next, and the decoder sets
/// CONSUMED (coding->consumed).
pub(crate) struct DecodeBlock<'a> {
    pub(crate) last: bool,
    pub(crate) eol_dos: bool,
    pub(crate) spec: &'a mut DecoderSpec,
    pub(crate) consumed: usize,
}

impl<'a> DecodeBlock<'a> {
    /// A whole text, as decode_coding_object decodes one.
    pub(crate) fn whole(spec: &'a mut DecoderSpec, eol_dos: bool) -> Self {
        DecodeBlock {
            last: true,
            eol_dos,
            spec,
            consumed: 0,
        }
    }
}

/// coding->spec as one block leaves it for the next.
#[derive(Default)]
pub(crate) struct DecoderSpec {
    /// CODING_UTF_8_BOM or CODING_UTF_16_BOM is utf_without_bom: the first
    /// block's BOM check is done.
    pub(crate) bom_done: bool,
    /// CODING_UTF_16_SURROGATE.
    pub(crate) surrogate: u32,
    /// coding->spec.iso_2022.
    pub(crate) iso2022: Option<Box<iso2022::Iso2022Carry>>,
    /// coding->spec.emacs_mule.cmp_status of a composition a block left
    /// unfinished, its carryover with it.
    pub(crate) emacs_mule: Option<Box<decoders::EmacsMuleComposing>>,
}

/// A stream decoded a block at a time: read_process_output's
/// proc_decode_coding_system[] and p->decoding_buf, call_process's
/// process_coding and carryover.  CARRYOVER is what the last block left
/// undecoded (at most coding->carryover's 64 bytes), LAST_BLOCK is
/// CODING_MODE_LAST_BLOCK, and the spec state belongs to the coding the
/// last block settled on (setup_coding_system starts another afresh).
#[derive(Default)]
pub(crate) struct BlockDecoder {
    pub(crate) carryover: Vec<u8>,
    pub(crate) last_block: bool,
    spec_coding: Option<String>,
    spec: DecoderSpec,
}

impl BlockDecoder {
    /// setup_process_coding_systems: a fresh coding struct, but the
    /// decoding carryover (p->decoding_buf) survives it.
    pub(crate) fn reset_keeping_carryover(&mut self) {
        let carryover = std::mem::take(&mut self.carryover);
        *self = Self {
            carryover,
            ..Default::default()
        };
    }
}

/// The encoder side of a process's coding struct
/// (proc_encode_coding_system[]) as one send_process leaves it for the
/// next: the coding it is set up with, when not the process's encoding
/// itself (a unibyte send's raw-text), and the coding->spec state of a
/// block that is not the last -- the ISO-2022 designations and the BOM
/// already written.  No send is a last block: Fprocess_send_eof's flush
/// sets CODING_MODE_LAST_BLOCK, but send_process's object t encodes
/// nothing, and the raw-text setup after it clears the mode.
#[derive(Default)]
pub(crate) struct EncoderSpec {
    pub(crate) coding: Option<Value>,
    pub(crate) bom_done: bool,
    pub(crate) iso2022: Option<Box<iso2022::Iso2022EncodeCarry>>,
}

/// sizeof coding->carryover.
const CODING_CARRYOVER_BYTES: usize = 64;

/// A decoder's characters, as produce_chars stores them: the text, with
/// the characters it cannot spell kept by character index in EXTENDED
/// (one beyond Unicode or a surrogate, spelled as the invalid-Unicode
/// sentinel, and a real U+E080..U+E0FF, which would read back as a raw
/// byte), the way strings and buffers keep them; and the `charset' runs
/// the decoder annotated (character offsets), which produce_charset turns
/// into text properties.  CHUNKS are the characters each decoder call
/// produced, which produce_chars inserts into a buffer destination one
/// insert_from_gap at a time, and EOL what decode_eol changed after them.
/// COMPOSITIONS are the composition annotations among the runs.
#[derive(Default)]
pub(crate) struct DecodedText {
    pub(crate) text: String,
    pub(crate) charsets: Vec<(usize, usize, String)>,
    pub(crate) compositions: Vec<DecodedComposition>,
    pub(crate) extended: Vec<(usize, u32)>,
    chars: usize,
    pub(crate) chunks: Vec<usize>,
    pub(crate) eol: Option<Box<EolConversion>>,
}

/// produce_composition's COMPONENTS for compose_text: nil for a relative
/// composition, else the characters and rules (a rule as its code modulo
/// #x100) up to the first -1, Fstring of them when all are characters and
/// Fvector of them otherwise.
#[derive(Clone, Debug)]
pub(crate) enum CompositionComponents {
    Relative,
    Chars(Vec<u32>),
    Vector(Vec<u32>),
}

/// A decoder's composition annotation: produce_composition's compose_text
/// over the characters START..END with COMPONENTS, which produce_annotation
/// reaches after the first SEQ `charset' runs.
#[derive(Clone, Debug)]
pub(crate) struct DecodedComposition {
    pub(crate) seq: usize,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) components: CompositionComponents,
}

impl DecodedComposition {
    /// compose_text's `composition' value, ((LENGTH . COMPONENTS)); None
    /// for components Fstring refuses.
    pub(crate) fn property(&self) -> Option<Value> {
        let codes = |codes: &[u32]| -> Vec<Value> {
            codes
                .iter()
                .map(|&code| Value::Integer(i64::from(code)))
                .collect()
        };
        let components = match &self.components {
            CompositionComponents::Relative => Value::Nil,
            CompositionComponents::Chars(chars) => {
                Value::StringObject(StringObjectRef::from_characters(&codes(chars)).ok()?)
            }
            CompositionComponents::Vector(items) => Value::vector(codes(items)),
        };
        let length = Value::Integer((self.end - self.start) as i64);
        Some(Value::cons(Value::cons(length, components), Value::Nil))
    }
}

/// A decoded text's annotation, as produce_annotation reaches it.
#[derive(Clone, Copy)]
pub(crate) enum Annotation<'a> {
    Charset(usize, usize, &'a str),
    Composition(&'a DecodedComposition),
}

/// The `charset' runs and the compositions in produce_annotation's order.
pub(crate) fn annotations<'a>(
    charsets: &'a [(usize, usize, String)],
    compositions: &'a [DecodedComposition],
) -> impl Iterator<Item = Annotation<'a>> {
    let mut charsets = charsets.iter().enumerate().peekable();
    let mut compositions = compositions.iter().peekable();
    std::iter::from_fn(move || {
        let composition_first = match (charsets.peek(), compositions.peek()) {
            (Some(&(index, _)), Some(composition)) => composition.seq <= index,
            (None, Some(_)) => true,
            (_, None) => false,
        };
        if composition_first {
            compositions.next().map(Annotation::Composition)
        } else {
            charsets
                .next()
                .map(|(_, (from, to, charset))| Annotation::Charset(*from, *to, charset))
        }
    })
}

/// How decode_coding puts a decoded text into a buffer destination when
/// it takes more than one insert_from_gap, decode_eol changed it or a
/// composition is among its annotations: the characters of each
/// insertion, the `charset' runs and the compositions over the text as
/// inserted, and decode_eol's change after them.
pub(crate) struct DecodeLayout {
    pub(crate) chunks: Vec<usize>,
    pub(crate) charsets: Vec<(usize, usize, String)>,
    pub(crate) compositions: Vec<DecodedComposition>,
    pub(crate) eol: Option<Box<EolConversion>>,
}

/// decode_eol's change to a decoded text, which a buffer destination
/// makes once the text is in it: under dos (DOS) the CRs before a newline
/// deleted, under mac the CRs made newlines, CRS their indices in the
/// text before the change; the text, its `charset' runs, its compositions
/// and its side table of characters as they were before it.
pub(crate) struct EolConversion {
    pub(crate) dos: bool,
    pub(crate) crs: Vec<usize>,
    pub(crate) text: String,
    pub(crate) charsets: Vec<(usize, usize, String)>,
    pub(crate) compositions: Vec<DecodedComposition>,
    pub(crate) extended: Vec<(usize, u32)>,
}

/// decode_eol's pass over TEXT: under DOS each CR before a newline
/// deleted, else each CR made a newline; the text it leaves and the
/// character indices in TEXT of the CRs it changed.
fn eol_converted(text: &str, dos: bool) -> (String, Vec<usize>) {
    let bytes = text.as_bytes();
    let mut converted = String::with_capacity(bytes.len());
    let mut crs = Vec::new();
    // The character index of byte AT, and the bytes up to COPIED done.
    let mut index = 0;
    let mut copied = 0;
    for (at, &byte) in bytes.iter().enumerate() {
        if byte == b'\r' && (!dos || bytes.get(at + 1) == Some(&b'\n')) {
            crs.push(index);
            converted.push_str(&text[copied..at]);
            if !dos {
                converted.push('\n');
            }
            copied = at + 1;
        }
        index += usize::from((byte as i8) >= -0x40);
    }
    converted.push_str(&text[copied..]);
    (converted, crs)
}

impl DecodedText {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// An empty text with room for BYTES bytes of characters.
    pub(crate) fn with_capacity(bytes: usize) -> Self {
        Self {
            text: String::with_capacity(bytes),
            ..Self::default()
        }
    }

    /// TEXT, of CHARS characters and none of U+E080..U+E0FF, as it is.
    pub(crate) fn from_plain_text(text: String, chars: usize) -> Self {
        debug_assert_eq!(text.chars().count(), chars);
        debug_assert!(!crate::lisp::types::string_data::has_raw_byte_markers(
            text.as_bytes()
        ));
        Self {
            text,
            chars,
            ..Self::default()
        }
    }

    /// The number of characters produced so far.
    pub(crate) fn char_count(&self) -> usize {
        self.chars
    }

    /// What a buffer destination needs beyond the text and its runs to
    /// take it as decode_coding inserts it, decode_eol's record taken from
    /// the text: None for one insertion that decode_eol left as it was,
    /// with no composition among its annotations.
    pub(crate) fn take_layout(&mut self) -> Option<DecodeLayout> {
        if self.chunks.len() <= 1 && self.eol.is_none() && self.compositions.is_empty() {
            return None;
        }
        let mut eol = self.eol.take();
        let (charsets, compositions) = match &mut eol {
            Some(eol) => (
                std::mem::take(&mut eol.charsets),
                std::mem::take(&mut eol.compositions),
            ),
            None => (self.charsets.clone(), self.compositions.clone()),
        };
        Some(DecodeLayout {
            chunks: self.chunks.clone(),
            charsets,
            compositions,
            eol,
        })
    }

    /// Store the character whose code is C.
    pub(crate) fn push_code(&mut self, c: u32) {
        if c < 0x80 {
            self.text.push(char::from(c as u8));
        } else if RAW_BYTE8_CHARS.contains(&c) {
            self.text
                .push(raw_byte_regex_char((c - RAW_BYTE8_BASE) as u8));
        } else {
            match char::from_u32(c) {
                Some(ch) => {
                    self.text.push(ch);
                    if raw_byte_from_regex_char(ch).is_some() {
                        self.extended.push((self.chars, c));
                    }
                }
                None => {
                    self.text.push(json::INVALID_UNICODE_SENTINEL);
                    self.extended.push((self.chars, c));
                }
            }
        }
        self.chars += 1;
    }

    /// Store BYTE as a decoder's invalid octet: ASCII as itself, any other
    /// as its eight-bit character.
    pub(crate) fn push_byte(&mut self, byte: u8) {
        if byte < 0x80 {
            self.text.push(char::from(byte));
        } else {
            self.text.push(raw_byte_regex_char(byte));
        }
        self.chars += 1;
    }

    /// Store the characters of RUN.
    pub(crate) fn push_str(&mut self, run: &str) {
        if run.as_bytes().contains(&0xEE)
            && run.chars().any(|ch| raw_byte_from_regex_char(ch).is_some())
        {
            for ch in run.chars() {
                self.push_code(ch as u32);
            }
        } else {
            self.text.push_str(run);
            self.chars += run.chars().count();
        }
    }

    /// Store each byte of RUN as `push_byte' does, writing the UTF-8 of
    /// a non-ASCII byte's character, U+E080..U+E0FF, as its three bytes;
    /// the count of those bytes.
    pub(crate) fn push_raw_bytes(&mut self, run: &[u8]) -> usize {
        let high = high_byte_count(run);
        // SAFETY: only ASCII bytes and the whole sequences EE 82 80 to
        // EE 83 BF go in.
        let out = unsafe { self.text.as_mut_vec() };
        let len = out.len();
        out.resize(len + run.len() + 2 * high, 0);
        write_raw_byte_text(&mut out[len..], run);
        self.chars += run.len();
        high
    }

    /// decode_eol's scan of the produced text: the EOL_SEEN bits, with
    /// CRLF counted once and CR LF's second character skipped.
    fn eol_seen(&self) -> u8 {
        // CR and LF are never part of another character's UTF-8.
        let bytes = self.text.as_bytes();
        let mut seen = detect::EOL_SEEN_NONE;
        let mut i = 0;
        while let Some(offset) = memchr::memchr2(b'\n', b'\r', &bytes[i..]) {
            let p = i + offset;
            if bytes[p] == b'\n' {
                seen |= detect::EOL_SEEN_LF;
                i = p + 1;
            } else if bytes.get(p + 1) == Some(&b'\n') {
                seen |= detect::EOL_SEEN_CRLF;
                i = p + 2;
            } else {
                seen |= detect::EOL_SEEN_CR;
                i = p + 1;
            }
            if seen == detect::EOL_SEEN_LF | detect::EOL_SEEN_CR | detect::EOL_SEEN_CRLF {
                break;
            }
        }
        seen
    }

    /// decode_eol's conversion for EOL_TYPE (1 dos, 2 mac): a CR becomes a
    /// newline under mac, and under dos each CR followed by a newline goes,
    /// the side table, the charset runs and the compositions moving with
    /// the characters after it.
    fn convert_eol(&mut self, eol_type: i64) {
        match eol_type {
            2 if self.text.contains('\r') => {
                let (text, crs) = eol_converted(&self.text, false);
                self.eol = Some(Box::new(EolConversion {
                    dos: false,
                    crs,
                    text: std::mem::replace(&mut self.text, text),
                    charsets: self.charsets.clone(),
                    compositions: self.compositions.clone(),
                    extended: self.extended.clone(),
                }));
            }
            1 if self.text.contains("\r\n") => {
                let (text, crs) = eol_converted(&self.text, true);
                let charsets = self.charsets.clone();
                let compositions = self.compositions.clone();
                let extended = self.extended.clone();
                // The CRs deleted before character index INDEX.
                let shift = |index: usize| index - crs.partition_point(|&cr| cr < index);
                for entry in &mut self.extended {
                    entry.0 = shift(entry.0);
                }
                for run in &mut self.charsets {
                    run.0 = shift(run.0);
                    run.1 = shift(run.1);
                }
                for composition in &mut self.compositions {
                    composition.start = shift(composition.start);
                    composition.end = shift(composition.end);
                }
                self.chars -= crs.len();
                self.eol = Some(Box::new(EolConversion {
                    dos: true,
                    crs,
                    text: std::mem::replace(&mut self.text, text),
                    charsets,
                    compositions,
                    extended,
                }));
            }
            _ => {}
        }
    }
}

pub(crate) fn decode_text_bytes(
    interp: &Interpreter,
    bytes: &[u8],
    coding: &str,
) -> Result<String, LispError> {
    decode_text_bytes_annotated(interp, bytes, coding).map(|decoded| decoded.text)
}

/// charset.c CHAR_CHARSET / char_charset (c, Qnil): ASCII is `ascii';
/// otherwise the charsets of the ordered list are tried in order, a
/// Unicode character answering `unicode' once the non-preferred part of
/// the order is reached, and `eight-bit' is the fallback.  ORDERED and
/// NON_PREFERRED_HEAD come from the interpreter (a caller working over a
/// text fetches them once).  CH is GNU's character number.
pub(crate) fn char_charset_ordered(
    interp: &Interpreter,
    ordered: &[String],
    non_preferred_head: Option<&str>,
    ch: u32,
) -> (String, u32) {
    if ch <= 0x7F {
        return ("ascii".into(), ch);
    }
    let unicode_char = ch <= MAX_UNICODE_CHAR;
    for (index, charset) in ordered.iter().enumerate() {
        if let Some(code) = encode_charset_char(interp, charset, ch) {
            return (charset.clone(), code);
        }
        if unicode_char
            && let Some(next) = ordered.get(index + 1)
            && Some(next.as_str()) == non_preferred_head
        {
            return ("unicode".into(), ch);
        }
    }
    // Past the whole list: `emacs' up to MAX_5_BYTE_CHAR, `eight-bit'
    // beyond (both normally answer from inside the list already).
    if ch <= MAX_5_BYTE_CHAR {
        ("emacs".into(), ch)
    } else {
        ("eight-bit".into(), ch - RAW_BYTE8_BASE)
    }
}

pub(crate) fn char_charset(interp: &Interpreter, ch: u32) -> (String, u32) {
    char_charset_ordered(
        interp,
        interp.charset_priority(),
        interp.charset_non_preferred_head_ref(),
        ch,
    )
}

/// Fdefine_coding_system_internal's `charset_valids' vector for a
/// charset-type coding: per first byte, the charsets it may start,
/// smaller dimensions first.
pub(crate) fn charset_valids(
    interp: &Interpreter,
    coding: &str,
) -> Vec<Option<Vec<std::rc::Rc<CharsetInfo>>>> {
    let mut valids: Vec<Option<Vec<std::rc::Rc<CharsetInfo>>>> = vec![None; 256];
    for charset in coding_system_charset_names(interp, coding) {
        let Some(info) = charset_info(interp, &charset) else {
            continue;
        };
        let dim = info.dimension as usize;
        let Some(&(min, max)) = info
            .code_space
            .as_ref()
            .and_then(|bounds| bounds.get(dim - 1))
        else {
            continue;
        };
        for byte in min..=max.min(255) {
            let entry = valids[byte as usize].get_or_insert_with(Vec::new);
            let position = entry
                .iter()
                .position(|existing| existing.dimension as usize > dim)
                .unwrap_or(entry.len());
            entry.insert(position, info.clone());
        }
    }
    valids
}

/// coding.c:get_translation_table for decoding, as far as whether there
/// is one: none while `enable-character-translation' is nil; else the
/// coding's `:decode-translation-table' (a symbol naming its
/// `translation-table' property, a list of tables, or the table) with
/// `standard-translation-table-for-decode' after it.
fn decode_translation_table_p(interp: &Interpreter, coding: &str, env: &Env) -> bool {
    if interp
        .lookup_var("enable-character-translation", env)
        .is_none_or(|value| value.is_nil())
    {
        return false;
    }
    let standard = interp
        .lookup_var("standard-translation-table-for-decode", env)
        .unwrap_or(Value::Nil);
    let table =
        coding_system_property(interp, coding, ":decode-translation-table").unwrap_or(Value::Nil);
    if table.is_nil() {
        return !standard.is_nil();
    }
    if !standard.is_nil() {
        return true;
    }
    match table.kind() {
        Kind::Symbol(name) => interp
            .get_symbol_property(&name, "translation-table")
            .is_some_and(|table| !table.is_nil()),
        Kind::T => interp
            .get_symbol_property("t", "translation-table")
            .is_some_and(|table| !table.is_nil()),
        // A list stays a list after its symbols are resolved.
        _ => true,
    }
}

pub(crate) fn decode_text_bytes_annotated(
    interp: &Interpreter,
    bytes: &[u8],
    coding: &str,
) -> Result<DecodedText, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    // detect_coding settles an auto-BOM coding before it decodes.
    let canonical = detect_auto_bom_coding(
        interp,
        &canonical,
        &detect::DetectSource::from_bytes(bytes),
        &Env::new(),
    )
    .unwrap_or(canonical);
    decode_bytes_with_coding(interp, bytes, &canonical)
}

/// The decoder of the coding system CANONICAL over BYTES, with no
/// detection, as a whole text.
fn decode_bytes_with_coding(
    interp: &Interpreter,
    bytes: &[u8],
    canonical: &str,
) -> Result<DecodedText, LispError> {
    decode_bytes_with_coding_block(
        interp,
        bytes,
        canonical,
        &mut DecodeBlock::whole(&mut DecoderSpec::default(), false),
    )
}

/// The decoder of the coding system CANONICAL over the block BYTES, with
/// no detection.
fn decode_bytes_with_coding_block(
    interp: &Interpreter,
    bytes: &[u8],
    canonical: &str,
    block: &mut DecodeBlock,
) -> Result<DecodedText, LispError> {
    Ok(decoders::decode_units(interp, bytes, canonical, block))
}

/// CODING_ATTR_CHARSET_LIST: the charsets CODING was defined with, the
/// symbols `iso-2022' and `emacs-mule' standing for those lists.
pub(crate) fn coding_attr_charset_list(interp: &Interpreter, coding: &str) -> Vec<String> {
    let Some(state) = interp.coding_system(coding) else {
        return Vec::new();
    };
    match state.charset_list.kind() {
        Kind::Symbol(name) if name == "iso-2022" => interp.iso_2022_charset_list(),
        Kind::Symbol(name) if name == "emacs-mule" => emacs_mule_charset_list(interp),
        _ => coding_system_charset_names(interp, coding),
    }
}

fn charset_infos(interp: &Interpreter, names: &[String]) -> Vec<std::rc::Rc<CharsetInfo>> {
    names
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect()
}

/// coding.c char_encodable_p: some charset of CHARSETS contains C once the
/// encode translation TABLES have translated it.
fn char_encodable_p(
    interp: &Interpreter,
    c: u32,
    charsets: &[std::rc::Rc<CharsetInfo>],
    tables: &[Value],
) -> bool {
    let c = tables
        .iter()
        .fold(c, |code, table| translate_safety_character(*table, code));
    charsets
        .iter()
        .any(|charset| charset.encode(interp, c).is_some())
}

/// term.c's test of a character against the terminal's charset list
/// (coding_charset_list of its coding): char_charset over that list, which
/// for a coding without one is the ordered list and always succeeds.
pub(crate) fn terminal_char_encodable(interp: &Interpreter, coding: &str, c: u32) -> bool {
    let names = match iso2022::iso_attributes(interp, coding) {
        Some(attrs) if attrs.flags & crate::lisp::eval::coding::ISO_FLAG_FULL_SUPPORT != 0 => {
            interp.iso_2022_charset_list()
        }
        _ => coding_attr_charset_list(interp, coding),
    };
    names.is_empty() || char_encodable_p(interp, c, &charset_infos(interp, &names), &[])
}

/// The character codes of the current buffer's text FROM..TO, absolute
/// positions read past any narrowing.
fn buffer_character_codes(interp: &Interpreter, from: usize, to: usize) -> Vec<u32> {
    let buffer = interp.buffer.borrow();
    let source = StringLike {
        text: buffer.absolute_text_range(from, to),
        props: None,
        multibyte: buffer.is_multibyte(),
        extended_chars: buffer.substring_extended_chars(from, to),
    };
    source
        .character_codes_iter()
        .map(|code| code as u32)
        .collect()
}

/// coding.c Funencodable_char_position.
pub(crate) fn unencodable_char_position_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    // setup_coding_system reads nil as `undecided'.
    let coding = if args[2].is_nil() {
        "undecided".to_string()
    } else {
        checked_coding_symbol(interp, &args[2])?
    };
    let canonical = interp
        .coding_system_canonical_name(&coding)
        .ok_or_else(|| coding_system_error(args[2]))?;
    let kind = interp
        .coding_system_kind_name(&canonical)
        .unwrap_or_default();
    if kind == "raw-text" {
        return Ok(Value::Nil);
    }
    let ascii_compatible = coding_system_is_ascii_compatible(interp, &canonical);
    let charsets = charset_infos(interp, &coding_attr_charset_list(interp, &canonical));
    let tables = encode_translation_tables(interp, &canonical, env)?;
    let string = args.get(4).copied().filter(|value| !value.is_nil());
    let (codes, from) = match string {
        None => {
            let (start, end) = validate_region(interp, &args[0], &args[1])?;
            if !interp.buffer.borrow().is_multibyte() {
                return Ok(Value::Nil);
            }
            (buffer_character_codes(interp, start, end), start)
        }
        Some(string) => {
            let source = string_like(&string)
                .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), string))?;
            let length = source.text.chars().count();
            let (from, to) = validate_subarray(string, args[0], args[1], length)?;
            if !source.multibyte {
                return Ok(Value::Nil);
            }
            let codes: Vec<u32> = source
                .character_codes_iter()
                .skip(from)
                .take(to - from)
                .map(|code| code as u32)
                .collect();
            (codes, from)
        }
    };
    if ascii_compatible && codes.iter().all(|&c| c < 0x80) {
        return Ok(Value::Nil);
    }
    let mut n = match args.get(3).map(|value| value.kind()) {
        None | Some(Kind::Nil) => 1,
        Some(Kind::Integer(count)) if count >= 0 => count,
        Some(other) => return Err(wrong_type_argument("wholenump", other.value())),
    };
    let mut positions = Vec::new();
    for (index, &c) in codes.iter().enumerate() {
        if !(c < 0x80 && ascii_compatible) && !char_encodable_p(interp, c, &charsets, &tables) {
            positions.push(Value::Integer((from + index) as i64));
            n -= 1;
            if n == 0 {
                break;
            }
        }
    }
    Ok(if args.get(3).is_none_or(Value::is_nil) {
        positions.first().copied().unwrap_or(Value::Nil)
    } else {
        Value::list(positions)
    })
}

struct EmacsMuleLayout {
    lengths: [usize; 256],
    charsets: Vec<Option<String>>,
}

fn emacs_mule_layout(interp: &Interpreter) -> EmacsMuleLayout {
    // charset.c keeps these two tables in lockstep with `define-charset'.
    // The Lisp table is the exact last-definition-wins charset map; deriving
    // lengths from its live entries also honors packages which add charsets.
    let mut lengths = [1_usize; 256];
    lengths[0x9A] = 3;
    lengths[0x9B] = 3;
    lengths[0x9C] = 4;
    lengths[0x9D] = 4;
    let mut charsets = vec![None; 256];
    if let Some(table) =
        interp.lookup_var("emacs-mule-charset-table", &crate::lisp::types::Env::new())
        && let Ok(entries) = vector_items(&table)
    {
        for (id, entry) in entries.into_iter().take(256).enumerate() {
            let Ok(charset) = entry.as_symbol() else {
                continue;
            };
            let Some(dimension) = charset_plist_property(interp, charset, ":dimension")
                .and_then(|value| value.integer_value())
                .and_then(|value| usize::try_from(value).ok())
            else {
                continue;
            };
            charsets[id] = Some(charset.to_string());
            lengths[id] = dimension + usize::from(id >= 0xA0) + 1;
        }
    }
    EmacsMuleLayout { lengths, charsets }
}

/// charset.c Vemacs_mule_charset_list: the charsets that carry an
/// `:emacs-mule-id', in the current priority order (Fset_charset_priority
/// rebuilds the list in Vcharset_ordered_list order).
pub(crate) fn emacs_mule_charset_list(interp: &Interpreter) -> Vec<String> {
    interp
        .charset_priority_list()
        .into_iter()
        .filter(|charset| {
            charset_info(interp, charset).is_some_and(|info| info.emacs_mule_id.is_some())
        })
        .collect()
}

/// coding.c encode_coding_emacs_mule: ASCII and raw bytes as themselves;
/// any other character through the first charset of
/// Vemacs_mule_charset_list that encodes it, written as
/// EMACS_MULE_LEADING_CODES of the charset's emacs-mule id followed by
/// the code bytes with their high bits set; an unencodable character is
/// the coding's default char.  The encoder's `charset' annotation branch
/// never runs: setup_coding_system sets CODING_ANNOTATE_CHARSET_MASK for
/// ISO-2022 designation codings only, so a `charset' text property does
/// not steer emacs-mule (the oracle re-encodes a jisx0208-annotated
/// hiragana through chinese-gb2312, the list's first match).
fn encode_coding_emacs_mule(interp: &Interpreter, codes: &[u32], coding: &str) -> Vec<u8> {
    let charsets: Vec<(std::rc::Rc<CharsetInfo>, u8)> = emacs_mule_charset_list(interp)
        .iter()
        .filter_map(|charset| {
            let info = charset_info(interp, charset)?;
            let id = info.emacs_mule_id?;
            Some((info, id))
        })
        .collect();
    // char_charset (C, Vemacs_mule_charset_list, &code).
    let char_charset = |c: u32| {
        charsets.iter().find_map(|(charset, id)| {
            charset
                .encode(interp, c)
                .map(|code| (charset.dimension, *id, code))
        })
    };
    let default_char = coding_system_default_char(interp, coding);
    let mut out = Vec::with_capacity(codes.len());
    for &code in codes {
        if code < 0x80 {
            out.push(code as u8);
            continue;
        }
        if char_byte8_p(code) {
            out.push((code - RAW_BYTE8_BASE) as u8);
            continue;
        }
        let Some((dimension, id, charset_code)) = char_charset(code).or_else(|| {
            if default_char < 0x80 {
                out.push(default_char as u8);
                return None;
            }
            char_charset(default_char)
        }) else {
            continue;
        };
        // EMACS_MULE_LEADING_CODES
        match id {
            0..=0x9F => out.push(id),
            0xA0..=0xDF => out.extend([0x9A, id]),
            0xE0..=0xEF => out.extend([0x9B, id]),
            0xF0..=0xF4 => out.extend([0x9C, id]),
            _ => out.extend([0x9D, id]),
        }
        if dimension == 1 {
            out.push((charset_code | 0x80) as u8);
        } else {
            let charset_code = charset_code | 0x8080;
            out.extend([(charset_code >> 8) as u8, (charset_code & 0xFF) as u8]);
        }
    }
    out
}

fn detected_names_value(names: Vec<String>, highest: bool) -> Value {
    if highest {
        names
            .into_iter()
            .next()
            .map(|name| Value::Symbol(name.into()))
            .unwrap_or(Value::Nil)
    } else {
        Value::list(
            names
                .into_iter()
                .map(|name| Value::Symbol(name.into()))
                .collect::<Vec<_>>(),
        )
    }
}

pub(crate) fn detect_coding_string_value(
    interp: &Interpreter,
    value: &Value,
    highest: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let string = string_like(value).ok_or_else(|| wrong_type_argument("stringp", *value))?;
    let highest = highest.is_some_and(Value::is_truthy);
    let src = DetectSource::from_text(&string.text, string.multibyte);
    Ok(detected_names_value(
        detect::detect_coding_system(interp, &src, highest, None, env),
        highest,
    ))
}

pub(crate) fn detect_coding_region_value(
    interp: &Interpreter,
    start: &Value,
    end: &Value,
    highest: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    // coding.c:Fdetect_coding_region: validate_region, then the text.
    let (start, end) = validate_region(interp, start, end)?;
    let text = interp
        .buffer
        .borrow()
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let highest = highest.is_some_and(Value::is_truthy);
    let src = DetectSource::from_text(&text, interp.buffer.borrow().is_multibyte());
    Ok(detected_names_value(
        detect::detect_coding_system(interp, &src, highest, None, env),
        highest,
    ))
}

/// coding.c:get_translation_table (ATTRS, true, ...): the tables the
/// encoders and the encodability checks translate characters through.
/// Only a coding's own symbol/list entries are resolved through symbol
/// properties; a standard table used on its own is passed through as-is.
pub(crate) fn encode_translation_tables(
    interp: &mut Interpreter,
    coding: &str,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    if interp
        .lookup_var("enable-character-translation", env)
        .is_some_and(|value| value.is_nil())
    {
        return Ok(Vec::new());
    }
    let standard = interp
        .lookup_var("standard-translation-table-for-encode", env)
        .unwrap_or(Value::Nil);
    let table =
        coding_system_property(interp, coding, ":encode-translation-table").unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(if standard.is_nil() {
            Vec::new()
        } else {
            vec![standard]
        });
    }
    let mut resolve = |value: Value| {
        // SYMBOLP includes nil/t and enabled positioned symbols. Fget
        // also observes the current overriding-plist-environment; use
        // the same primitive path instead of reading the raw plist.
        if value.is_symbol()
            || (symbols_with_pos_enabled(interp, env)
                && symbol_with_pos_parts(interp, &value).is_some())
        {
            dispatch::misc::direct_get(interp, &[value, Value::symbol("translation-table")], env)
        } else {
            Ok(value)
        }
    };
    let mut tables = if table.cons_values().is_some() {
        table
            .to_vec()?
            .into_iter()
            .map(resolve)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![resolve(table)?]
    };
    if matches!(standard.kind(), Kind::CharTable(_)) {
        tables.push(standard);
    }
    Ok(tables)
}

/// character.c:translate_char applies table lists in order, replacing a
/// character only with another valid character, and reading live slots.
fn translate_safety_character(mut table: Value, mut character: u32) -> u32 {
    if let Kind::CharTable(table) = table.kind() {
        if let Kind::Integer(translated) = table.get(character).kind()
            && (0..=i64::from(RAW_BYTE8_BASE + 0xff)).contains(&translated)
        {
            return translated as u32;
        }
        return character;
    }
    while let Some((head, tail)) = table.cons_values() {
        character = translate_safety_character(head, character);
        table = tail;
    }
    character
}

pub(crate) fn find_coding_systems_region_internal_value(
    interp: &mut Interpreter,
    start: &Value,
    end: &Value,
    exclude: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let source = if let Some(string) = string_like(start) {
        string
    } else {
        let from = position_from_value(interp, start)?;
        let to = position_from_value(interp, end)?;
        let buffer = interp.buffer.borrow();
        // coding.c checks BEG..Z, including text outside narrowing, and
        // rejects reversed bounds. No Lisp callback runs while reading it.
        if from == 0 || from > to || to > buffer.size_total() + 1 {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                *start,
                *end,
            ])));
        }
        StringLike {
            text: buffer.absolute_text_range(from, to),
            props: None,
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(from, to),
        }
    };
    if !source.multibyte {
        return Ok(Value::T);
    }
    // GNU's work table checks each non-ASCII character only once. Keep
    // actual Emacs character numbers, including non-Unicode and byte8.
    let characters: std::collections::BTreeSet<u32> = source
        .character_codes_iter()
        .map(|code| code as u32)
        .filter(|code| *code >= 0x80)
        .collect();
    if characters.is_empty() {
        return Ok(Value::T);
    }
    let excluded = exclude
        .filter(|value| !value.is_nil())
        .map(|value| value.to_vec())
        .transpose()?
        .unwrap_or_default();
    let mut codings = Vec::new();
    // coding.c walks the actual Lisp registration list, retaining its
    // order and duplicates, including a dynamically bound list.
    let mut registered = interp
        .lookup_var("coding-system-list", env)
        .unwrap_or(Value::Nil);
    while let Some((head, tail)) = registered.cons_values() {
        let coding_value = head;
        registered = tail;
        if excluded
            .iter()
            .any(|candidate| candidate.eq_value(coding_value))
        {
            continue;
        }
        let coding = coding_value.as_symbol()?;
        let Some(state) = interp.coding_system(coding) else {
            continue;
        };
        if state.base != coding {
            continue;
        }
        let charsets = match state.charset_list.kind() {
            Kind::Symbol(name) if name == "iso-2022" => interp.iso_2022_charset_list(),
            Kind::Symbol(name) if name == "emacs-mule" => emacs_mule_charset_list(interp),
            _ => coding_system_charset_names(interp, coding),
        };
        let translations = encode_translation_tables(interp, coding, env)?;
        // coding.c:char_encodable_p tests declared charset membership;
        // an encoder's fallback/substitution is not proof of safe encoding.
        if characters.iter().all(|&character| {
            let character = translations.iter().fold(character, |code, table| {
                translate_safety_character(*table, code)
            });
            charsets.iter().any(|charset| {
                encode_charset_character_number(interp, charset, character).is_some()
            })
        }) {
            codings.push(coding_value);
        }
    }
    // coding.c always supplies these two fallbacks, including when EXCLUDE
    // names them. They do not need a charset-list to preserve the bytes.
    codings.extend([Value::symbol("raw-text"), Value::symbol("no-conversion")]);
    Ok(Value::list(codings))
}

/// coding.c Fcheck_coding_systems_region: for each coding of
/// CODING_LIST that cannot encode some non-ASCII character of the region
/// (absolute positions, past any narrowing) or string, the coding as
/// given followed by those positions.
pub(crate) fn check_coding_systems_region_value(
    interp: &mut Interpreter,
    start: &Value,
    end: Option<&Value>,
    coding_list: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let (codes, base) = if let Some(string) = string_like(start) {
        if !string.multibyte {
            return Ok(Value::Nil);
        }
        let codes: Vec<u32> = string
            .character_codes_iter()
            .map(|code| code as u32)
            .collect();
        (codes, 0)
    } else {
        let end_value = end.copied().unwrap_or(Value::Nil);
        let s = position_from_value(interp, start)?;
        let e = position_from_value(interp, &end_value)?;
        let (z, multibyte) = {
            let buffer = interp.buffer.borrow();
            (buffer.size_total() + 1, buffer.is_multibyte())
        };
        if !(1 <= s && s <= e && e <= z) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                *start,
                end_value,
            ])));
        }
        if !multibyte {
            return Ok(Value::Nil);
        }
        (buffer_character_codes(interp, s, e), s)
    };
    if codes.iter().all(|&c| c < 0x80) {
        return Ok(Value::Nil);
    }
    let mut checks = Vec::new();
    let mut tail = *coding_list;
    while let Some((coding, rest)) = tail.cons_values() {
        tail = rest;
        let canonical = coding
            .symbol_str()
            .and_then(|name| interp.coding_system_canonical_name(name))
            .ok_or_else(|| coding_system_error(coding))?;
        let charsets = charset_infos(interp, &coding_attr_charset_list(interp, &canonical));
        let tables = encode_translation_tables(interp, &canonical, env)?;
        checks.push((coding, charsets, tables, Vec::new()));
    }
    for (index, &c) in codes.iter().enumerate() {
        if c < 0x80 {
            continue;
        }
        for (_, charsets, tables, positions) in &mut checks {
            if !char_encodable_p(interp, c, charsets, tables) {
                positions.push(Value::Integer((base + index) as i64));
            }
        }
    }
    Ok(Value::list(
        checks
            .into_iter()
            .filter(|(_, _, _, positions)| !positions.is_empty())
            .map(|(coding, _, _, positions)| Value::list(std::iter::once(coding).chain(positions)))
            .collect::<Vec<_>>(),
    ))
}

/// coding.c:Ffind_operation_coding_system: OPERATION's `target-idx'
/// property names the argument whose coding systems the operation's alist
/// gives.
pub(crate) fn find_operation_coding_system_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 {
        return Err(LispError::Signal("Too few arguments".into()));
    }
    let operation = args[0];
    let Kind::Symbol(operation_name) = operation.kind() else {
        return Err(LispError::Signal("Invalid first argument".into()));
    };
    let Some(index) = interp
        .get_symbol_property(operation_name.as_str(), "target-idx")
        .and_then(|index| index.fixnum())
        .and_then(|index| usize::try_from(index).ok())
    else {
        return Err(LispError::Signal("Invalid first argument".into()));
    };
    if args.len() <= 1 + index {
        return Err(LispError::Signal(format!(
            "Too few arguments for operation `{operation_name}'"
        )));
    }
    let insert = operation_name == "insert-file-contents";
    let network = operation_name == "open-network-stream";
    let mut target = args[index + 1];
    let is_string = |value: &Value| matches!(value.kind(), Kind::StringObject(_));
    let valid = is_string(&target)
        || (insert
            && target.cons_values().is_some_and(|(file, buffer)| {
                is_string(&file) && matches!(buffer.kind(), Kind::Buffer(_))
            }))
        || (network && (target.fixnum().is_some() || matches!(target.kind(), Kind::T)));
    if !valid {
        return Err(LispError::Signal(format!(
            "Invalid argument {} of operation `{operation_name}'",
            index + 1
        )));
    }
    if let Some((file, _)) = target.cons_values() {
        target = file;
    }
    let alist_name = if insert || operation_name == "write-region" {
        "file-coding-system-alist"
    } else if network {
        "network-coding-system-alist"
    } else {
        "process-coding-system-alist"
    };
    let mut chain = interp.lookup_var(alist_name, env).unwrap_or(Value::Nil);
    while let Some((elt, rest)) = chain.cons_values() {
        chain = rest;
        let Some((key, val)) = elt.cons_values() else {
            continue;
        };
        let matches = if is_string(&target) {
            is_string(&key)
                && search::fast_string_match(interp, env, &key, &target, None, false)?.is_some()
        } else {
            target.fixnum().is_some() && target.eq_value(key)
        };
        if !matches {
            continue;
        }
        // VAL is a coding system before it is a function to call.
        if val.is_cons() {
            return Ok(val);
        }
        if !val.is_symbol() {
            return Ok(Value::Nil);
        }
        if coding_system_p(interp, &val) {
            return Ok(Value::cons(val, val));
        }
        let Kind::Symbol(function) = val.kind() else {
            unreachable!("VAL was checked to be a symbol")
        };
        if interp
            .raw_function_binding_symbol(&function, env)
            .is_some_and(|binding| !binding.is_nil())
        {
            let result = call_function_value(interp, &val, &[Value::list(args.to_vec())], env)?;
            if result.is_cons() {
                return Ok(result);
            }
            if result.is_symbol() && coding_system_p(interp, &result) {
                return Ok(Value::cons(result, result));
            }
        }
        return Ok(Value::Nil);
    }
    Ok(Value::Nil)
}

/// coding.c:Fcoding_system_p.
pub(crate) fn coding_system_p(interp: &Interpreter, value: &Value) -> bool {
    value.is_nil()
        || matches!(value.kind(), Kind::Symbol(symbol) if interp.has_coding_system(&symbol))
}

/// The `charset' text property per character of STRING: the annotation
/// CODING_ANNOTATE_CHARSET_MASK hands the encoder as the preferred
/// charset.
fn charset_preferences(string: &StringLike) -> Vec<Option<String>> {
    let Some(intervals) = &string.props else {
        return Vec::new();
    };
    let count = string.text.chars().count();
    let mut preferred = vec![None; count];
    for (start, end, plist) in intervals.iter() {
        let value = crate::lisp::primitives::strings::lisp_plist_get(plist, "charset");
        if let Ok(charset) = value.as_symbol() {
            for slot in preferred.iter_mut().take(end.min(count)).skip(start) {
                *slot = Some(charset.to_string());
            }
        }
    }
    preferred
}

pub(crate) fn encode_coding_value(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    encode_coding_value_recording(interp, value, coding, nocopy, true, env)
}

pub(crate) fn encode_coding_value_recording(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    record_used: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    encode_coding_converted(interp, value, coding, nocopy, record_used, true, env)
}

/// code_convert_region's encode of the region text VALUE: no fast path,
/// the pre-write conversion always run.
pub(crate) fn encode_coding_region_value(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    env: &mut Env,
) -> Result<Value, LispError> {
    encode_coding_converted(interp, value, coding, false, true, false, env)
}

/// encode_coding_object of VALUE by CODING, with code_convert_string's
/// ASCII fast path when STRING_CONVERSION; `last-coding-system-used' is
/// the coding given, set (when RECORD_USED) once the encoding, pre-write
/// conversion included, is done.
fn encode_coding_converted(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    record_used: bool,
    string_conversion: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let Some(coding) = coding else {
        if record_used {
            set_last_coding_system_used(interp, "no-conversion", env);
        }
        return if nocopy {
            Ok(*value)
        } else {
            shared_string_copy(value)
        };
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // code_convert_string only preserves identity for ASCII-compatible,
    // ASCII-only input with no EOL work. It runs before pre-write conversion.
    let fast_path = string_conversion
        && coding_system_is_ascii_compatible(interp, &canonical)
        && string.text.is_ascii()
        && string.extended_chars.is_empty()
        && (interp.coding_system_eol_type_value(&canonical) == Some(0)
            || matches!(canonical.as_str(), "no-conversion" | "binary")
            || inhibit_eol_conversion
            || !string.text.contains('\n'));
    // The requested spelling, not the canonical name (the oracle answers
    // `euc-jp' for (encode-coding-string "a" 'euc-jp)).
    if fast_path {
        if record_used {
            set_last_coding_system_used(interp, coding, env);
        }
        return Ok(if nocopy {
            *value
        } else {
            bytes_to_shared_unibyte_value(string.text.as_bytes())
        });
    }
    let pre_write =
        coding_system_property(interp, &canonical, ":pre-write-conversion").unwrap_or(Value::Nil);
    let converted = if pre_write.is_nil() {
        std::borrow::Cow::Borrowed(&string)
    } else {
        std::borrow::Cow::Owned(pre_write_convert(interp, &string, pre_write, env)?)
    };
    let preferred = charset_preferences(&converted);
    // encode_coding substitutes the default character for what the
    // coding cannot encode; it never signals.
    let translation = encode_translation_tables(interp, &canonical, env)?;
    let encoded = encode_text_bytes_with_charsets(
        interp,
        &converted,
        &canonical,
        inhibit_eol_conversion,
        false,
        &translation,
        &preferred,
        None,
    )?;
    if record_used {
        set_last_coding_system_used(interp, coding, env);
    }
    Ok(bytes_to_shared_unibyte_value(&encoded))
}

/// send_process's encode_coding_object of the string VALUE by CODING
/// through a process's encoder SPEC, as a block that is not the last: the
/// pre-write conversion, then the encoder resuming from and leaving SPEC's
/// state.  `last-coding-system-used' is the caller's.
pub(crate) fn encode_process_block(
    interp: &mut Interpreter,
    value: &Value,
    coding: &str,
    spec: &mut EncoderSpec,
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    let string =
        string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?;
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    let pre_write =
        coding_system_property(interp, &canonical, ":pre-write-conversion").unwrap_or(Value::Nil);
    let converted = if pre_write.is_nil() {
        std::borrow::Cow::Borrowed(&string)
    } else {
        std::borrow::Cow::Owned(pre_write_convert(interp, &string, pre_write, env)?)
    };
    let preferred = charset_preferences(&converted);
    let translation = encode_translation_tables(interp, &canonical, env)?;
    encode_text_bytes_with_charsets(
        interp,
        &converted,
        &canonical,
        inhibit_eol_conversion,
        false,
        &translation,
        &preferred,
        Some(spec),
    )
}

/// CODING_REQUIRE_FLUSHING: setup_coding_system sets it for the
/// ISO-2022 and CCL coding types.
pub(crate) fn coding_requires_flushing(interp: &Interpreter, coding: &str) -> bool {
    matches!(
        interp.coding_system_kind_name(coding).as_deref(),
        Some("iso-2022" | "ccl")
    )
}

/// CODING_REQUIRE_ENCODING of a coding struct set up for CODING (nil
/// standing for `undecided'): every type but undecided and raw-text, any
/// coding with a pre-write conversion, and those two with a dos or mac
/// eol unless `inhibit-eol-conversion' makes every eol unix.
pub(crate) fn coding_requires_encoding(interp: &Interpreter, coding: &Value, env: &Env) -> bool {
    let name = coding
        .symbol_str()
        .filter(|_| !coding.is_nil())
        .unwrap_or("undecided");
    let Some(canonical) = interp.coding_system_canonical_name(name) else {
        return false;
    };
    if coding_system_property(interp, &canonical, ":pre-write-conversion")
        .is_some_and(|function| !function.is_nil())
    {
        return true;
    }
    if !matches!(
        interp.coding_system_kind_name(&canonical).as_deref(),
        Some("undecided" | "raw-text")
    ) {
        return true;
    }
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    !inhibit_eol_conversion && matches!(coding_eol_type(interp, &canonical), Some(1 | 2))
}

/// A multibyte source (a multibyte buffer region, or a multibyte string;
/// decode_coding_object's src_multibyte), decoded in one pass over the
/// units ONE_MORE_BYTE reads from it.  RAW_TO_UNIBYTE is a raw-text
/// decode into a unibyte destination, which produce_chars fills with the
/// bytes ONE_MORE_BYTE reads (a character's negative code's low eight
/// bits).
fn decode_multibyte_source_text(
    interp: &Interpreter,
    source: &StringLike,
    coding: &str,
    raw_to_unibyte: bool,
    eol_dos: bool,
) -> DecodedText {
    let codes = source.character_codes_iter().map(|code| code as u32);
    if raw_to_unibyte {
        let mut decoded = DecodedText::new();
        for code in codes {
            if code < 0x80 {
                decoded.push_byte(code as u8);
            } else if RAW_BYTE8_CHARS.contains(&code) {
                decoded.push_byte((code - RAW_BYTE8_BASE) as u8);
            } else {
                decoded.push_byte(((-i64::from(code)) & 0xFF) as u8);
            }
        }
        return decoded;
    }
    let units = units::CharUnits::from_codes(codes);
    let mut spec = DecoderSpec::default();
    decoders::decode_units(
        interp,
        &units,
        coding,
        &mut DecodeBlock::whole(&mut spec, eol_dos),
    )
}

/// The annotations of DECODED as produce_annotation puts them on its
/// text, Fput_text_property of each `charset' run and each composition in
/// turn: the intervals of the text, none for no annotation.
pub(crate) fn decoded_annotation_props(
    decoded: &mut DecodedText,
) -> Option<crate::intervals::Intervals> {
    if decoded.charsets.is_empty() && decoded.compositions.is_empty() {
        return None;
    }
    let len = decoded.text.chars().count();
    let mut props = crate::lisp::types::string_data::StringProperties::default();
    for annotation in annotations(&decoded.charsets, &decoded.compositions) {
        let (start, end, plist) = match annotation {
            Annotation::Charset(start, end, charset) => (
                start,
                end,
                Value::list([Value::symbol("charset"), Value::Symbol(charset.into())]),
            ),
            Annotation::Composition(composition) => {
                let Some(value) = composition.property() else {
                    continue;
                };
                (
                    composition.start,
                    composition.end,
                    Value::list([Value::symbol("composition"), value]),
                )
            }
        };
        let end = end.min(len);
        if end > start {
            let _ = crate::intervals::add_string_text_properties(
                &mut props,
                len,
                start,
                end,
                plist,
                crate::intervals::PropertySetType::Replace,
                true,
            );
        }
    }
    decoded.charsets.clear();
    decoded.compositions.clear();
    props.take()
}

/// decode_coding_object's string destination for a decode without a
/// post-read conversion: DECODED with its annotations, made in the work
/// buffer a decoder call at a time when it took more than one insertion,
/// decode_eol changed it or a composition is among its annotations.
pub(crate) fn decoded_string_value(
    interp: &mut Interpreter,
    mut decoded: DecodedText,
    multibyte: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(layout) = decoded.take_layout() else {
        let props = decoded_annotation_props(&mut decoded);
        return Ok(make_shared_string_value_with_extended_chars(
            decoded.text,
            props,
            multibyte,
            decoded.extended,
        ));
    };
    let staged = StringLike {
        text: decoded.text,
        props: None,
        multibyte,
        extended_chars: decoded.extended,
    };
    let converted = decode_into_work_buffer(interp, &staged, Some(&layout), Value::Nil, env)?;
    Ok(make_shared_string_value_with_extended_chars(
        converted.text,
        converted.props,
        converted.multibyte,
        converted.extended_chars,
    ))
}

/// A text decode_coding_object reads.
pub(crate) enum DecodeSource<'a> {
    /// The bytes of a unibyte text: a unibyte string's or buffer's, a
    /// file's or a process's.
    Bytes(&'a [u8]),
    /// A file's bytes, read into the gap: what decode_coding_gap inserts
    /// as they are becomes the text without a copy.
    Gap(Vec<u8>),
    /// A string's or a buffer region's characters.
    Text(&'a StringLike),
}

/// What decode_coding_object leaves behind.
pub(crate) struct CodingDecode {
    pub(crate) decoded: DecodedText,
    /// CODING_ID_NAME once detect_coding and decode_eol have settled it:
    /// the coding as given (an alias stays its name) unless detection
    /// found another or the eol decided a subsidiary.
    pub(crate) coding: String,
    /// The settled coding's CODING_FOR_UNIBYTE: a string decoded with it
    /// is unibyte.
    pub(crate) for_unibyte: bool,
}

/// CODING_ID_EOL_TYPE as a settled type (0 unix, 1 dos, 2 mac), or None
/// for an undecided one's vector of subsidiaries.
pub(crate) fn coding_eol_type(interp: &Interpreter, coding: &str) -> Option<i64> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .unwrap_or_else(|| coding.to_string());
    if matches!(canonical.as_str(), "no-conversion" | "binary") {
        // `binary' and `no-conversion' are unix in GNU; the native
        // accessor keeps them out of the eol-variant table.
        return Some(0);
    }
    interp.coding_system_eol_type_value(&canonical)
}

/// coding.c decode_coding_object without its destination: SOURCE decoded
/// by CODING into a destination whose multibyteness is DST_MULTIBYTE (a
/// buffer's), or None for a string, multibyte unless the settled coding
/// is for unibyte text.  detect_coding settles an undecided or auto-BOM
/// coding first, carrying CODING's specified eol type over; the settled
/// coding's decoder runs; then decode_eol, unless the eol is unix or
/// `inhibit-eol-conversion' is set, decides an undecided eol from the
/// decoded text (naming its subsidiary) and converts.
pub(crate) fn decode_coding_object(
    interp: &Interpreter,
    env: &Env,
    source: DecodeSource<'_>,
    coding: &str,
    dst_multibyte: Option<bool>,
) -> Result<CodingDecode, LispError> {
    decode_coding(interp, env, source, coding, dst_multibyte, false, None)
}

/// decode_coding_c_string of one block of a stream: BYTES after what
/// DECODER carried over from the block before, decoded as a block that is
/// the last one when DECODER's CODING_MODE_LAST_BLOCK is set; what the
/// decoder leaves unconsumed is DECODER's carryover for the next.
pub(crate) fn decode_coding_block(
    interp: &Interpreter,
    env: &Env,
    bytes: &[u8],
    coding: &str,
    dst_multibyte: Option<bool>,
    decoder: &mut BlockDecoder,
) -> Result<CodingDecode, LispError> {
    decode_coding(
        interp,
        env,
        DecodeSource::Bytes(bytes),
        coding,
        dst_multibyte,
        false,
        Some(decoder),
    )
}

/// decode_coding_gap: decode_coding_object for a file's BYTES, read into
/// a buffer whose multibyteness is DST_MULTIBYTE, with its ASCII
/// optimization.
pub(crate) fn decode_coding_gap(
    interp: &Interpreter,
    env: &Env,
    bytes: Vec<u8>,
    coding: &str,
    dst_multibyte: bool,
) -> Result<CodingDecode, LispError> {
    decode_coding(
        interp,
        env,
        DecodeSource::Gap(bytes),
        coding,
        Some(dst_multibyte),
        true,
        None,
    )
}

fn decode_coding(
    interp: &Interpreter,
    env: &Env,
    source: DecodeSource<'_>,
    coding: &str,
    dst_multibyte: Option<bool>,
    gap: bool,
    mut decoder: Option<&mut BlockDecoder>,
) -> Result<CodingDecode, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    let last_block = decoder.as_ref().is_none_or(|decoder| decoder.last_block);
    // coding->src_multibyte = chars < bytes: a multibyte text of ASCII
    // characters alone is read as its bytes.
    let (bytes, text) = match source {
        DecodeSource::Bytes(bytes) => match decoder.as_mut() {
            // read_process_output: the carryover, then the bytes read.
            Some(decoder) if !decoder.carryover.is_empty() => {
                let mut joined = std::mem::take(&mut decoder.carryover);
                joined.extend_from_slice(bytes);
                (std::borrow::Cow::Owned(joined), None)
            }
            _ => (std::borrow::Cow::Borrowed(bytes), None),
        },
        DecodeSource::Gap(bytes) => (std::borrow::Cow::Owned(bytes), None),
        DecodeSource::Text(string) if !string.multibyte => (
            std::borrow::Cow::Owned(encode_raw_text_bytes(&string.text)?),
            None,
        ),
        DecodeSource::Text(string) if string.text.is_ascii() => {
            (std::borrow::Cow::Borrowed(string.text.as_bytes()), None)
        }
        DecodeSource::Text(string) => (std::borrow::Cow::Borrowed(&[][..]), Some(string)),
    };
    let mut name = coding.to_string();
    let mut settled = canonical.clone();
    let mut detection = detect::DetectOutcome::default();
    if interp.coding_system_kind_name(&canonical).as_deref() == Some("undecided")
        || coding_system_auto_detects_bom(interp, &canonical)
    {
        let src = match text {
            Some(string) => detect::DetectSource::from_codes(string.character_codes_iter()),
            None => detect::DetectSource::from_bytes(&bytes),
        };
        // decode_coding_gap detects before it sets CODING_MODE_LAST_BLOCK.
        let src = if gap || !last_block {
            src.not_last_block()
        } else {
            src
        };
        detection = detect::detect_coding_outcome(interp, &src, &canonical, env);
        if let Some(found) = &detection.found {
            name = adjust_coding_eol_type(interp, found, coding_eol_type(interp, &canonical));
            settled = interp
                .coding_system_canonical_name(&name)
                .unwrap_or_else(|| name.clone());
        }
    }
    let for_unibyte = coding_system_property(interp, &settled, ":for-unibyte")
        .is_some_and(|value| value.is_truthy());
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // decode_coding_gap's ASCII optimization: with an ascii-compatible
    // coding, no post-read conversion and no translation table, ASCII
    // bytes -- or bytes detect_coding_utf_8 accepted to their end, for a
    // utf-8 coding -- go in as they are.  The eol bits detection saw (its
    // head scan, or the check_ascii over the ASCII head of an auto-BOM
    // detection), else check_ascii's (only LF under
    // `inhibit-eol-conversion' or a settled eol), still name an undecided
    // eol's subsidiary, and a dos or mac eol still converts.
    let utf_8_whole = detection.utf8_whole
        && interp.coding_system_kind_name(&settled).as_deref() == Some("utf-8");
    if gap
        && text.is_none()
        && (bytes.is_ascii() || utf_8_whole)
        && interp
            .lookup_var("disable-ascii-optimization", env)
            .is_none_or(|value| value.is_nil())
        && coding_system_is_ascii_compatible(interp, &settled)
        && coding_system_property(interp, &settled, ":post-read-conversion")
            .is_none_or(|value| value.is_nil())
        && !decode_translation_table_p(interp, &settled, env)
    {
        let mut decoded = if bytes.is_ascii() {
            let len = bytes.len();
            // SAFETY: ASCII is UTF-8.
            let text = unsafe { String::from_utf8_unchecked(bytes.into_owned()) };
            DecodedText::from_plain_text(text, len)
        } else {
            // insert_from_gap of the bytes as they are, less the BOM at
            // their head when the settled coding has one
            // (CODING_UTF_8_BOM not utf_without_bom).
            let bom = bytes.starts_with(&[0xEF, 0xBB, 0xBF])
                && coding_system_property(interp, &settled, ":bom")
                    .is_some_and(|value| !value.is_nil());
            let skip = if bom { 3 } else { 0 };
            if detection.utf8_standard {
                let mut bytes = bytes.into_owned();
                bytes.drain(..skip);
                // SAFETY: detect_coding_utf_8 found each sequence standard.
                let text = unsafe { String::from_utf8_unchecked(bytes) };
                DecodedText::from_plain_text(text, detection.utf8_chars - usize::from(bom))
            } else {
                decoders::decode_detected_utf_8(&bytes[skip..])
            }
        };
        let mut eol_type = coding_eol_type(interp, &settled);
        if eol_type.is_none() {
            let seen = match detection.eol_seen {
                Some(seen) => seen,
                // The text holds the bytes' newlines as they are.
                None if inhibit_eol_conversion => {
                    if decoded.text.as_bytes().contains(&b'\n') {
                        detect::EOL_SEEN_LF
                    } else {
                        detect::EOL_SEEN_NONE
                    }
                }
                None => decoded.eol_seen(),
            };
            eol_type = detect::eol_type_for_seen(seen);
            if eol_type.is_some() {
                name = adjust_coding_eol_type(interp, &name, eol_type);
            }
        }
        if let Some(eol_type) = eol_type {
            // The CRs go in the gap, before the one insert_from_gap.
            decoded.convert_eol(eol_type);
            decoded.eol = None;
        }
        return Ok(CodingDecode {
            decoded,
            coding: name,
            for_unibyte,
        });
    }
    // produce_chars copies the source of a raw-text decode (undecided
    // that detected nothing decodes as raw text too): unchanged into a
    // multibyte destination, and from a multibyte source into a unibyte
    // one as the bytes ONE_MORE_BYTE reads.
    let dst_multibyte = dst_multibyte.unwrap_or(!for_unibyte);
    let eol_dos = !inhibit_eol_conversion && coding_eol_type(interp, &settled) == Some(1);
    let mut decoded = match text {
        Some(string) => decode_multibyte_source_text(
            interp,
            string,
            &settled,
            !dst_multibyte
                && matches!(
                    interp.coding_system_kind_name(&settled).as_deref(),
                    Some("raw-text" | "undecided")
                ),
            eol_dos,
        ),
        None => {
            // decode_coding's decoder over the block, CODING_MODE_LAST_BLOCK
            // unless a stream's block is not the last, with the spec state
            // the stream's last block left when it settled the same coding.
            let mut whole = DecoderSpec::default();
            let spec = match decoder.as_mut() {
                Some(decoder) => {
                    if decoder.spec_coding.as_deref() != Some(settled.as_str()) {
                        decoder.spec = DecoderSpec::default();
                        decoder.spec_coding = Some(settled.clone());
                    }
                    &mut decoder.spec
                }
                None => &mut whole,
            };
            let mut block = DecodeBlock {
                last: last_block,
                eol_dos,
                spec,
                consumed: 0,
            };
            let decoded = decode_bytes_with_coding_block(interp, &bytes, &settled, &mut block)?;
            let consumed = block.consumed;
            if let Some(decoder) = decoder.as_mut() {
                // coding->carryover holds at most its 64 bytes.
                let rest = &bytes[consumed..];
                decoder.carryover = rest[..rest.len().min(CODING_CARRYOVER_BYTES)].to_vec();
            }
            decoded
        }
    };
    if !inhibit_eol_conversion {
        let eol_type = match coding_eol_type(interp, &settled) {
            Some(eol_type) => Some(eol_type),
            None => {
                // Stray CRs among CRLFs are dos; any other mixture is unix.
                let seen = match decoded.eol_seen() {
                    seen if seen == detect::EOL_SEEN_CRLF | detect::EOL_SEEN_CR => {
                        detect::EOL_SEEN_CRLF
                    }
                    detect::EOL_SEEN_NONE => detect::EOL_SEEN_NONE,
                    seen @ (detect::EOL_SEEN_LF | detect::EOL_SEEN_CRLF | detect::EOL_SEEN_CR) => {
                        seen
                    }
                    _ => detect::EOL_SEEN_LF,
                };
                let eol_type = detect::eol_type_for_seen(seen);
                if eol_type.is_some() {
                    name = adjust_coding_eol_type(interp, &name, eol_type);
                }
                eol_type
            }
        };
        if let Some(eol_type) = eol_type {
            decoded.convert_eol(eol_type);
        }
    }
    Ok(CodingDecode {
        decoded,
        coding: name,
        for_unibyte,
    })
}

/// Decode VALUE with CODING. code_convert_string's ASCII fast path applies
/// only to a string result; a source region or a buffer destination goes
/// straight through decode_coding_object (BUFFER_CONVERSION).
pub(crate) fn decode_coding_text(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    buffer_conversion: bool,
    dst_multibyte: Option<bool>,
    env: &mut Env,
) -> Result<Value, LispError> {
    decode_coding_text_recording(
        interp,
        value,
        coding,
        nocopy,
        buffer_conversion,
        dst_multibyte,
        true,
        env,
    )
}

/// `decode_coding_text', `last-coding-system-used' set only when
/// RECORD_USED (code_convert_string's NORECORD false).
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_coding_text_recording(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    buffer_conversion: bool,
    dst_multibyte: Option<bool>,
    record_used: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    debug_assert!(
        dst_multibyte.is_none(),
        "a buffer destination decodes through decode_coding_text_for_buffer"
    );
    decode_coding_parts(
        interp,
        value,
        coding,
        nocopy,
        buffer_conversion,
        dst_multibyte,
        record_used,
        env,
    )
    .map(|decoded| decoded.value)
}

#[allow(clippy::too_many_arguments)]
fn decode_coding_parts(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    nocopy: bool,
    buffer_conversion: bool,
    dst_multibyte: Option<bool>,
    record_used: bool,
    env: &mut Env,
) -> Result<BufferDecode, LispError> {
    // What the decoder reads: a unibyte string's bytes, a multibyte one's
    // characters.
    let unibyte = match value.kind() {
        Kind::StringObject(state) => {
            let state = state.borrow();
            (!state.is_multibyte()).then(|| state.bytes().to_vec())
        }
        _ => None,
    };
    let string = match unibyte {
        Some(_) => None,
        None => Some(
            string_like(value)
                .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))?,
        ),
    };
    let (ascii, has_cr) = match (&unibyte, &string) {
        (Some(bytes), _) => (bytes.is_ascii(), bytes.contains(&b'\r')),
        (None, Some(string)) => (string.text.is_ascii(), string.text.contains('\r')),
        (None, None) => unreachable!("a string is unibyte or multibyte"),
    };
    let Some(coding) = coding else {
        if record_used {
            set_last_coding_system_used(interp, "no-conversion", env);
        }
        return Ok(BufferDecode {
            value: if nocopy {
                *value
            } else {
                shared_string_copy(value)?
            },
            coding: "no-conversion".into(),
            post_read: Value::Nil,
            layout: None,
        });
    };
    let canonical = interp
        .coding_system_canonical_name(coding)
        .ok_or_else(|| coding_system_error(Value::symbol(coding)))?;
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // code_convert_string's fast path: an ascii-compatible coding, an
    // ASCII string (a multibyte one whose characters are its bytes) and
    // nothing for eol conversion to do return the string as it is -- the
    // decoder never runs, and neither does detection.  A region or a
    // buffer destination never takes it.
    let fast_path = !buffer_conversion
        && coding_system_is_ascii_compatible(interp, &canonical)
        && ascii
        && (coding_eol_type(interp, &canonical) == Some(0) || inhibit_eol_conversion || !has_cr);
    if fast_path {
        // A copied result is a fresh, unpropertized multibyte ASCII string;
        // NOCOPY gives back the original object, properties and all.
        if record_used {
            set_last_coding_system_used(interp, coding, env);
        }
        return Ok(BufferDecode {
            value: if nocopy {
                *value
            } else {
                let text = match (unibyte, string) {
                    (Some(bytes), _) => String::from_utf8(bytes).expect("ASCII bytes are UTF-8"),
                    (None, Some(string)) => string.text,
                    (None, None) => unreachable!("a string is unibyte or multibyte"),
                };
                make_shared_string_value_with_multibyte(text, None, true)
            },
            coding: coding.into(),
            post_read: Value::Nil,
            layout: None,
        });
    }
    let CodingDecode {
        mut decoded,
        coding: actual_coding,
        for_unibyte,
    } = decode_coding_object(
        interp,
        env,
        match (&unibyte, &string) {
            (Some(bytes), _) => DecodeSource::Bytes(bytes),
            (None, Some(string)) => DecodeSource::Text(string),
            (None, None) => unreachable!("a string is unibyte or multibyte"),
        },
        coding,
        dst_multibyte,
    )?;
    // produce_annotation's `charset' properties of the decoded text.
    let layout = decoded.take_layout();
    let charset_props = decoded_annotation_props(&mut decoded);
    let post_read = coding_system_property(interp, &actual_coding, ":post-read-conversion")
        .unwrap_or(Value::Nil);
    let decoded = StringLike {
        text: decoded.text,
        props: charset_props,
        // A buffer destination takes the characters produced (written
        // into a unibyte one in their multibyte form, by the caller); a
        // string is unibyte for a coding for unibyte text.
        multibyte: dst_multibyte.is_some() || !for_unibyte,
        extended_chars: decoded.extended,
    };
    if dst_multibyte.is_some() {
        // The destination buffer's decode is finished by
        // finish_buffer_decode, once the text is in it.
        return Ok(BufferDecode {
            value: make_shared_string_value_with_extended_chars(
                decoded.text,
                decoded.props,
                decoded.multibyte,
                decoded.extended_chars,
            ),
            coding: actual_coding,
            post_read,
            layout,
        });
    }
    // A string destination: decode_coding_object decodes into the work
    // buffer, multibyte unless the coding detect_coding settled on is for
    // unibyte text, and the post-read conversion runs there.
    let converted = decode_into_work_buffer(interp, &decoded, layout.as_ref(), post_read, env)?;
    // code_convert_string records CODING_ID_NAME once decode_coding_object,
    // post-read conversion included, has returned.
    if record_used {
        set_last_coding_system_used(interp, &actual_coding, env);
    }
    Ok(BufferDecode {
        value: make_shared_string_value_with_extended_chars(
            converted.text,
            converted.props,
            converted.multibyte,
            converted.extended_chars,
        ),
        coding: actual_coding,
        post_read: Value::Nil,
        layout: None,
    })
}

/// A decode for a buffer destination: the decoded text, the coding
/// detect_coding settled on and that coding's post-read conversion.
pub(crate) struct BufferDecode {
    pub(crate) value: Value,
    coding: String,
    post_read: Value,
    /// How decode_coding puts the text into the buffer, when it takes
    /// more than one insertion (see `DecodedText::layout').
    pub(crate) layout: Option<DecodeLayout>,
}

/// decode_coding_object into a buffer destination: VALUE decoded with
/// CODING for a buffer of DST_MULTIBYTE, to be inserted by the caller
/// and finished with finish_buffer_decode.
pub(crate) fn decode_coding_text_for_buffer(
    interp: &mut Interpreter,
    value: &Value,
    coding: Option<&str>,
    dst_multibyte: bool,
    env: &mut Env,
) -> Result<BufferDecode, LispError> {
    decode_coding_parts(
        interp,
        value,
        coding,
        false,
        true,
        Some(dst_multibyte),
        false,
        env,
    )
}

/// The rest of decode_coding_object and code_convert_region for a buffer
/// destination holding DECODE's text, PRODUCED characters at DST_POS of
/// the current buffer: the post-read conversion there, then
/// `last-coding-system-used'.  Returns the characters produced.
pub(crate) fn finish_buffer_decode(
    interp: &mut Interpreter,
    decode: &BufferDecode,
    dst_pos: usize,
    produced: usize,
    env: &mut Env,
) -> Result<usize, LispError> {
    let produced = if decode.post_read.is_nil() {
        produced
    } else {
        post_read_in_buffer(interp, decode.post_read, dst_pos, produced, env)?
    };
    set_last_coding_system_used(interp, &decode.coding, env);
    Ok(produced)
}

/// charset.c CHARSET_MIN_CODE / CHARSET_MAX_CODE.
pub(crate) fn charset_code_bounds(interp: &Interpreter, charset: &str) -> Option<(u32, u32)> {
    charset_info(interp, charset)?.code_bounds
}

fn push_sorted_char_ranges(chars: &mut Vec<u32>, ranges: &mut Vec<(i64, i64)>) {
    chars.sort_unstable();
    chars.dedup();
    let mut index = 0;
    while index < chars.len() {
        let start = chars[index];
        let mut end = start;
        while index + 1 < chars.len() && chars[index + 1] == end + 1 {
            index += 1;
            end = chars[index];
        }
        ranges.push((i64::from(start), i64::from(end)));
        index += 1;
    }
}

/// charset.c map_charset_chars: the character ranges FUNCTION receives for
/// codes FROM..TO of CHARSET.  A MAP charset walks its encoder -- maximal
/// unicode-ascending runs of the mapped characters; a unified OFFSET
/// charset walks its deunifier the same way and then appends the raw
/// code-offset range; a plain OFFSET charset yields the arithmetic range;
/// SUBSET and SUPERSET charsets recurse through their relatives.
/// (characters.el's CJK syntax/category rules run through this: the "_"
/// rows of korean-ksc5601 carry the euro sign among others, which is how
/// GNU's standard syntax table gives U+20AC symbol syntax.)
pub(crate) fn map_charset_char_ranges(
    interp: &Interpreter,
    charset: &str,
    from: i64,
    to: i64,
) -> Option<Vec<(i64, i64)>> {
    let canonical = interp.charset_canonical_name(charset)?;
    let mut ranges = Vec::new();

    if let Some((child, min, max, offset)) = charset_subset(interp, &canonical) {
        let from = (from - offset).max(min);
        let to = (to - offset).min(max);
        if from <= to {
            ranges.extend(map_charset_char_ranges(interp, &child, from, to)?);
        }
        return Some(ranges);
    }
    if let Some(children) = charset_superset(interp, &canonical) {
        for (child, offset) in children {
            let Some((child_min, child_max)) = charset_code_bounds(interp, &child) else {
                continue;
            };
            let from = (from - offset).max(i64::from(child_min));
            let to = (to - offset).min(i64::from(child_max));
            if from <= to
                && let Some(child_ranges) = map_charset_char_ranges(interp, &child, from, to)
            {
                ranges.extend(child_ranges);
            }
        }
        return Some(ranges);
    }

    let (min_code, max_code) = charset_code_bounds(interp, &canonical)?;
    let from = u32::try_from(from.max(i64::from(min_code))).ok()?;
    let to = u32::try_from(to.min(i64::from(max_code))).ok()?;
    if from > to {
        return Some(ranges);
    }

    let info = charset_info(interp, &canonical)?;
    let offset = info.code_offset;
    let has_direct_map = info.has_direct_map;
    let unified = info.unified && offset.is_some();
    if (has_direct_map || unified)
        && let Some(map) = info.map(interp)
    {
        let mut chars: Vec<u32> = map
            .entries
            .iter()
            .filter(|(code, _)| (from..=to).contains(code))
            .map(|(_, character)| *character)
            .collect();
        push_sorted_char_ranges(&mut chars, &mut ranges);
    }
    if let Some(offset) = offset.filter(|_| !has_direct_map) {
        let from_index = info.code_to_index(from)?;
        let to_index = info.code_to_index(to)?;
        ranges.push((i64::from(from_index) + offset, i64::from(to_index) + offset));
    }
    Some(ranges)
}
