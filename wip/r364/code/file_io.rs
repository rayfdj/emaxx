// file_io.rs / hash_insert.rs / display.rs: reference code of the first implementation.

/// print.c printchar_to_stream onto stdout (TO_STDOUT) or stderr: CH, or
/// the characters among the elements of the vector
/// `standard-display-table' shows it as; an ASCII character as its byte,
/// any other as its multibyte form, encoded by `coding-system-for-write',
/// else `locale-coding-system', when either is set
/// (code_convert_string_norecord).
pub(crate) fn printchar_to_stream(
    interp: &mut Interpreter,
    ch: u32,
    to_stdout: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let mut chars = vec![ch];
    if ch <= MAX_CHAR
        && let Some(Kind::CharTable(table)) = interp
            .symbol_value_key(cached_symbol!("standard-display-table"))
            .map(|value| value.kind())
        && table.has_purpose("display-table")
        && let Some(entry) = interp.char_table_get(table, ch)
        && crate::lisp::primitives::interactive::is_vector_value(&entry)
    {
        chars = crate::lisp::primitives::buffers::vector_items(&entry)?
            .iter()
            .filter_map(|element| match element.kind() {
                Kind::Integer(c) if (0..=i64::from(MAX_CHAR)).contains(&c) => Some(c as u32),
                _ => None,
            })
            .collect();
    }
    let coding = interp
        .forwarded_c_value("coding-system-for-write", env)
        .filter(|value| !value.is_nil())
        .or_else(|| interp.forwarded_c_value("locale-coding-system", env))
        .filter(|value| !value.is_nil());
    // putc for an ASCII character and fwrite for any other, each its own
    // call on the stream (on the unbuffered stderr its own write);
    // print.c reads neither's result.
    let stream_write = |bytes: &[u8]| {
        if to_stdout {
            crate::lisp::primitives::batch_stdout::write(bytes)
        } else {
            crate::lisp::primitives::batch_stdout::stderr_write(bytes)
        }
    };
    for c in chars {
        if c < 0x80 {
            stream_write(&[c as u8]);
            continue;
        }
        let code = if (c as i32) < 0 {
            // CHAR_STRING of a negative int, here where it is known to
            // be no ASCII character: its two-byte form's arithmetic
            // (`0xC0 | (c >> 6)', `0x80 | (c & 0x3F)').  That is some
            // character's form when its first byte is a two-byte
            // character's; otherwise the bytes are no character's and
            // go out as they are.
            let c = c as i32;
            let form = [0xC0 | (c >> 6) as u8, 0x80 | (c & 0x3F) as u8];
            match crate::lisp::types::string_data::decode_character(&form) {
                Ok((code, 2)) if form[0] < 0xE0 => code,
                _ => {
                    stream_write(&form);
                    continue;
                }
            }
        } else {
            // CHAR_STRING's multibyte form: char_string resolves the
            // modifier bits of a code that has them and drops what is
            // left of them, and a code past MAX_CHAR even so is an
            // error.
            crate::lisp::primitives::ccl::written_character(c as i32)?
        };
        let mut one = PrintOutput::default();
        one.push_code(code);
        let Some(coding) = &coding else {
            stream_write(one.bytes());
            continue;
        };
        let string = one.into_value(interp, env)?;
        // The coding as given, or the one an alias no name names stands
        // for (`checked_coding_given').
        let given = crate::lisp::primitives::coding::checked_coding_given(interp, coding)?;
        let encoded =
            encode_coding_value_recording(interp, &string, given.as_deref(), true, false, env)?;
        stream_write(&crate::lisp::primitives::text::internal_string_bytes(
            &string_like(&encoded).expect("an encoded string"),
        )?);
    }
    Ok(())
}

/// print.c's print_prepare, BODY writing to the sink it resolves STREAM
/// to, and print_finish.
pub(crate) fn with_print_stream<T>(
    interp: &mut Interpreter,
    stream: Option<&Value>,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env, PrintSink) -> Result<T, LispError>,
) -> Result<T, LispError> {
    print_prepared(interp, stream, env, body).map(|(value, _)| value)
}

/// `with_print_stream', with the octets of a text print_finish inserted
/// in a multibyte buffer a character each (`print_object_to_buffer').
fn print_prepared<T>(
    interp: &mut Interpreter,
    stream: Option<&Value>,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env, PrintSink) -> Result<T, LispError>,
) -> Result<(T, Option<Vec<u8>>), LispError> {
    let sink = PrintSink::of(interp, stream, env);
    with_printer_destination(interp, stream, env, |interp, env| {
        if !matches!(sink, PrintSink::Buffer) {
            if matches!(sink, PrintSink::Echo) {
                crate::lisp::primitives::setup_echo_area_for_printing(interp, env);
            }
            return body(interp, env, sink).map(|value| (value, None));
        }
        let multibyte = interp.buffer.borrow().is_multibyte();
        with_printer_buffer_escape(interp, env, Some(multibyte), |interp, env| {
            // print_finish runs before the unwinding that gives
            // print_buffer back.
            with_print_buffer(interp, |interp| {
                let value = body(interp, env, sink)?;
                let octets = print_finish_insertion(interp, env)?;
                Ok((value, octets))
            })
            .map(|(result, _)| result)
        })
    })
}

/// print.c:print_finish for a print into a buffer: `insert_1_both
/// (print_buffer.buffer, pos, pos_byte, 0, 1, 0)' at point of the buffer
/// current now, and `signal_after_change (PT - print_buffer.pos, 0,
/// print_buffer.pos)'.  insert_1_both of no characters returns at once
/// (no prepare_to_modify_buffer), and the signal still runs; otherwise
/// the hooks of prepare_to_modify_buffer run before the text is copied
/// out of print_buffer, so a print one of them runs into a buffer
/// changes both the text and the `pos' the signal is given.  No
/// update_compositions follows.  The octets of a text whose bytes
/// strout counted a character each, when the buffer is multibyte.
fn print_finish_insertion(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<Vec<u8>>, LispError> {
    let (nbytes, was_multibyte) = interp
        .print_globals
        .buffer
        .as_ref()
        .map_or((0, false), |text| (text.len(), text.is_multibyte()));
    if nbytes == 0 {
        let point = interp.buffer.borrow().point();
        super::insdel::signal_after_change(
            interp,
            super::insdel::AfterChange::new(point, 0, 0),
            &super::hooks_overlays::OverlayHookCalls::none(),
            env,
        )?;
        return Ok(None);
    }
    // The text waits on prepare_to_modify_buffer from here until
    // insert_1_both copies it (the after-change hooks run later).  A
    // print that is itself run by a hook of a waiting one has written
    // its text over the start of print_buffer's memory.
    interp.print_globals.inserting += 1;
    let globals = &mut interp.print_globals;
    if let (Some(memory), Some(text)) = (globals.memory.as_mut(), globals.buffer.as_ref()) {
        if memory.len() < nbytes {
            memory.resize(nbytes, 0);
        }
        memory[..nbytes].copy_from_slice(text.bytes());
    }
    let copied = |interp: &mut Interpreter| {
        interp.print_globals.inserting -= 1;
        if interp.print_globals.inserting == 0 {
            interp.print_globals.memory = None;
        }
    };
    let mut waiting = true;
    let inserted = insert_print_buffer(interp, env, |interp| {
        let multibyte = interp.buffer.borrow().is_multibyte();
        let text = crate::lisp::primitives::print::print_finish_text(
            interp,
            nbytes,
            was_multibyte,
            |text, pos| {
                let octets = (multibyte && !text.is_multibyte() && !text.bytes().is_ascii())
                    .then(|| text.octets());
                let (parts, extended) = text.text_parts();
                (parts, extended, pos, octets)
            },
        );
        waiting = false;
        copied(interp);
        text
    });
    if waiting {
        // prepare_to_modify_buffer signaled.
        copied(interp);
    }
    inserted
}

/// print.c:print_prepare binds print--unreadable-callback-buffer to the
/// buffer current before it selects the output's, for BODY.
pub(crate) fn with_printer_destination<T>(
    interp: &mut Interpreter,
    stream: Option<&Value>,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let caller = interp.current_buffer_id();
    let callback = interp.print_unreadable_callback_buffer.replace(caller);
    let result = select_printer_destination(interp, stream, env, body);
    interp.print_unreadable_callback_buffer = callback;
    // record_unwind_current_buffer: whatever BODY (a function printed
    // to, `print-unreadable-function') made current, the buffer current
    // at print_prepare is current again, if it lives.
    let restored = if interp.current_buffer_id() != caller && interp.has_buffer_id(caller) {
        interp.set_current_buffer_id(caller)
    } else {
        Ok(())
    };
    match (result, restored) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

// select_printer_destination: as at 654b4b0, with two changes:
//   the out-of-range marker error carries its datum:
            if !(state.point_min()..=state.point_max()).contains(&position) {
                // signal_error (..., printcharfun).
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("error".into()),
                    Value::String("Marker is outside the accessible part of the buffer".into()),
                    *stream.expect("matched Some"),
                ])));
            }
//   and the marker is set in the buffer current after the insertion:
        if let Some((marker, start, old_point)) = marker_position {
            // print_finish: `set_marker_both (old_printcharfun, Qnil, PT,
            // PT_BYTE)' and SET_PT_BOTH, in the buffer current now: the
            // one the insertion's hooks left, or where a destination
            // killed meanwhile left the print.
            let end = interp.buffer.borrow().point();
            let current = interp.current_buffer_id();
            interp.set_marker(marker, Some(end), Some(current))?;
            let restored_point = if old_point >= start {
                old_point.saturating_add(end).saturating_sub(start)
            } else {
                old_point
            };
            interp.buffer.borrow_mut().goto_char(restored_point);
        }

/// print.c:write_string of the C string DATA, a Lisp string's SSDATA: to
/// its first NUL, as strlen measures it, in one strout whose bytes are a
/// character each, a function handed each as the C `char' it is; in a
/// print_prepare and print_finish of its own.
pub(crate) fn write_c_string(
    interp: &mut Interpreter,
    data: &[u8],
    stream: Option<&Value>,
    env: &mut Env,
) -> Result<(), LispError> {
    let data = &data[..data.iter().position(|&byte| byte == 0).unwrap_or(data.len())];
    with_print_stream(interp, stream, env, |interp, env, sink| {
        let mut output = PrintOutput::default();
        if matches!(sink, PrintSink::Function(_)) {
            for &byte in data {
                output.push_c_function_byte(byte);
            }
        } else {
            output.push_octets(data);
        }
        deliver(interp, env, sink, &output)
    })
}

// hash_insert.rs
/// print.c:print_finish's insertion at point: insert_1_both with PREPARE
/// of the text TEXT gives once prepare_to_modify_buffer has run (its
/// characters, the extended ones among them, the `pos' print_buffer has
/// by then when it is not that text's length, and a value to pass on),
/// with no properties inherited, and then `signal_after_change (PT -
/// pos, 0, pos)' alone: print_finish calls no update_compositions.
pub(crate) fn insert_print_buffer<R>(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    text: impl FnOnce(&mut Interpreter) -> (String, Vec<(usize, u32)>, Option<usize>, R),
) -> Result<R, LispError> {
    let point = interp.buffer.borrow().point();
    modify_current_buffer(interp, point, point, false, env, |interp, _, env| {
        let (text, extended_chars, pos, value) = text(interp);
        let start = interp.buffer.borrow().point();
        // insdel.c:copy_text: a unibyte buffer takes each character's byte.
        let unibyte_copy = if interp.buffer.borrow().is_multibyte() {
            None
        } else {
            crate::lisp::primitives::unibyte_buffer_copy(&text, &extended_chars)
        };
        let (text, extended_chars) = match &unibyte_copy {
            Some(copy) => (copy.as_str(), &[][..]),
            None => (text.as_str(), &extended_chars[..]),
        };
        let reserved = interp.buffer.borrow_mut().try_reserve_insertion(text.len());
        if let Err(nbytes) = reserved {
            return Err(memory_full(interp, env, nbytes));
        }
        interp.insert_current_buffer_with(
            text,
            crate::buffer::InsertedProperties::None { inherit: false },
            false,
        );
        interp.set_inserted_extended_chars(start, extended_chars);
        let point = interp.buffer.borrow().point();
        let pos = pos.unwrap_or(point - start);
        Ok((
            Some(AfterChange::signed(point as i64 - pos as i64, 0, pos as i64)),
            value,
        ))
    })
}

// display.rs
pub(crate) fn echo_area_print_logged(interp: &mut Interpreter, text: &str, env: &Env) {
    echo_area_print_logged_as(interp, text, text, env);
}

/// `echo_area_print_logged' of a C string's bytes, which message_dolog
/// takes as text (LOGGED) and the echo area a character each (SHOWN).
pub(crate) fn echo_area_print_logged_as(
    interp: &mut Interpreter,
    logged: &str,
    shown: &str,
    env: &Env,
) {
    if !ECHO_FROM_PRINT.with(std::cell::Cell::get) {
        message_log_maybe_newline(interp, env);
    }
    log_message_partial(interp, logged, env);
    echo_area_print(shown);
}

/// xdisp.c setup_echo_area_for_printing, as print_prepare calls it for
/// a print to t before anything is printed: when a message has held the
/// echo area since the last print, the log line printing left open is
/// ended and the echo area starts empty; otherwise the print goes on
/// where the last one stopped.
pub(crate) fn setup_echo_area_for_printing(interp: &mut Interpreter, env: &Env) {
    if !ECHO_FROM_PRINT.with(std::cell::Cell::get) {
        message_log_maybe_newline(interp, env);
    }
    echo_area_print("");
}

// echo_area_print's tail: xdisp.c current_message_1: the echo area
// buffer's text, or nil when it holds none.
    ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| {
        *slot = ECHO_AREA_MESSAGE.with_borrow(|current| {
            current
                .as_ref()
                .filter(|(text, _)| !text.is_empty())
                .map(|(text, _)| {
                    crate::lisp::primitives::strings::string_like_value(text.clone(), None)
                })
        })
    });
    ECHO_FROM_PRINT.with(|flag| flag.set(true));

// terpri's ENSURE test (inside with_print_stream; `sink' the resolved one):
//   Function -> error; batch non-buffer -> last char != '\n' from
//   interp.batch_standard_output_last_char; then
                            } else if matches!(sink, PrintSink::Echo) {
                                // NILP (Fbolp ()) in the echo area buffer,
                                // which setup_echo_area_for_printing made
                                // current: at the end of what is printed
                                // there.
                                echo_area_message()
                                    .is_some_and(|text| !text.is_empty() && !text.ends_with('\n'))
                            } else {
                                // NILP (Fbolp ()).
                                !printer_stream_at_line_start(interp, None)?
                            }

// print_error_message's head:
    use crate::lisp::primitives::file_io::{write_c_string, write_printer_output};
    if let Some(context) = context {
        write_c_string(interp, context, Some(&stream), env)?;
    }
    // If we know from where the error was signaled, show it in
    // *Messages*.
    match caller.kind() {
        Kind::Symbol(caller) => {
            log_message_partial(interp, caller.as_ref(), env);
            log_message_partial(interp, ": ", env);
        }
        Kind::T => log_message_partial(interp, "t: ", env),
        _ => {}
    }
// command-error-default-function's context: CHECK_STRING, then SSDATA:
                string_text(&args[1])?;
                let context = match args[1].kind() {
                    Kind::StringObject(state) => state.borrow().bytes().to_vec(),
                    _ => Vec::new(),
                };
// write-char's non-function arm: super::super::print::printchar_unsigned(interp, env, sink, code)?
