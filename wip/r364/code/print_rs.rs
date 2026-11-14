// print.rs: reference code of the first implementation.

// deliver()'s arms other than Buffer (Buffer appends OUTPUT to
// interp.print_globals.buffer.get_or_insert_with(PrintOutput::plain)).
        PrintSink::Stdout => {
            if !interp.message_capture_stack.is_empty() {
                interp.append_message_capture(&output.host_text(), false, env);
            }
            if standard_display_table_p(interp) {
                return deliver_through_display_table(interp, env, output);
            }
            for piece in output.pieces() {
                match piece {
                    Piece::Chars(chars) => crate::lisp::primitives::batch_stdout::write_chars(chars),
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
                call_function_value(interp, &function, &[Value::Integer(i64::from(code))], env)?;
            }
            Ok(())
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
    let mut output = PrintOutput::default();
    output.append_strout(&render_princ_string(interp, &state.borrow(), env, false));
    deliver(interp, env, sink, &output)
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
        call_function_value(interp, &function, &[Value::Integer(i64::from(code))], env)?;
    }
}

/// print.c:printchar of CH onto SINK.
pub(crate) fn printchar(
    interp: &mut Interpreter,
    env: &mut Env,
    sink: PrintSink,
    ch: u32,
) -> Result<(), LispError> {
    let mut one = PrintOutput::default();
    one.push_code(ch);
    deliver(interp, env, sink, &one)
}

/// `DISP_TABLE_P (Vstandard_display_table)'.
fn standard_display_table_p(interp: &Interpreter) -> bool {
    matches!(
        interp
            .symbol_value_key(cached_symbol!("standard-display-table"))
            .map(|value| value.kind()),
        Some(Kind::CharTable(table)) if table.has_purpose("display-table")
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
        let (mut rest, octets) = match piece {
            Piece::Chars(chars) | Piece::Strout(chars) => (chars, None),
            Piece::Octets(block) => (&[][..], Some(raw_octets(block))),
        };
        if let Some(octets) = &octets {
            rest = octets;
        }
        while !rest.is_empty() {
            let (code, width) = crate::lisp::types::string_data::decode_character(rest)
                .unwrap_or((u32::from(rest[0]), 1));
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
                .map_or(0, |now| now.codes().count());
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

// ccl.rs
pub(crate) fn written_character(c: i32) -> Result<u32, LispError> {
    const CHAR_MODIFIER_MASK: u32 = 0xFC0_0000;
    const MAX_CHAR: u32 = 0x3F_FFFF;
    if c < 0 {
        let byte = c as u32 & 0xFF;
        return Ok(if byte < 0x80 { byte } else { 0x3F_FF00 + byte });
    }
    if c < 0x1_0000 {
        return Ok(c as u32);
    }
    let mut c = c as u32;
    if c & CHAR_MODIFIER_MASK != 0 {
        c = char_resolve_modifier_mask(i64::from(c)) as u32;
        c &= !CHAR_MODIFIER_MASK;
    }
    if c <= MAX_CHAR {
        Ok(c)
    } else {
        Err(LispError::Signal(format!("Invalid character: {c:x}")))
    }
}
