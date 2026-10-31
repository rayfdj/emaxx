use super::*;
use crate::lisp::primitives::processes::wait_pumping_processes;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

mod frame_size;
mod mode_line;
pub(crate) use frame_size::{
    adjust_frame_size, change_frame_size, do_pending_window_change, frame_top_margin,
    set_menu_bar_lines, set_tab_bar_lines,
};
pub(crate) use mode_line::ModeLineCells;

// The live echo-area line of an interactive session.  GNU keeps this in
// the echo buffer that redisplay paints; the terminal frontend reads it
// after every command and inside blocking event reads so prompts appear
// before input is consumed.  Batch sessions never write it — their
// `message' contract (stderr + *Messages*) is unchanged.
/// (START, END, FACE) face runs in char offsets, the currency between
/// renderers and the frontend's paint layer.
pub(crate) type FaceSpans = Vec<(usize, usize, Value)>;
pub(crate) type EchoSpans = FaceSpans;

thread_local! {
    static ECHO_AREA_MESSAGE: std::cell::RefCell<Option<(String, EchoSpans)>> =
        const { std::cell::RefCell::new(None) };
    /// The Lisp string behind the current echo message.  `current-message'
    /// returns this value with its text properties intact; with-temp-message
    /// saves and later re-formats it when restoring a message.
    static ECHO_AREA_MESSAGE_VALUE: std::cell::RefCell<Option<Value>> =
        const { std::cell::RefCell::new(None) };
    // xdisp.c's echo_area_buffer is staticpro'd; the value behind the
    // current message is a root of every collection (`mark_echo_area_roots').
    /// GNU's echo_area_buffer[1]: the last message shown on the glass.
    /// `redisplay' from Lisp is redisplay_preserve_echo_area — when the
    /// current message was wiped by input arrival, it re-displays this
    /// one instead of clearing the row (menu-bar-open relies on it).
    static ECHO_AREA_LAST_DISPLAYED: std::cell::RefCell<Option<(String, EchoSpans)>> =
        const { std::cell::RefCell::new(None) };
    /// The frontend is redrawing under Fredisplay's preserve semantics.
    static ECHO_PRESERVE_REDISPLAY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The current echo text came from printing to the `t' stream;
    /// further prints append (GNU's echo buffer accumulates a print
    /// sequence) while any `message' replaces it and resets this.
    static ECHO_FROM_PRINT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// xdisp.c's message_log_need_newline: the log's last line was
    /// written by printing to the echo area and is not finished.
    static MESSAGE_LOG_NEED_NEWLINE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Monotonic count of message emissions.  GNU's message3 repaints
    /// the echo area directly (display_echo_area) even while a tty menu
    /// holds redisplay frozen, whereas read_char's input-arrival
    /// clear_message only takes effect at the next redisplay; the
    /// frontend's menu loop tells the two apart by whether this moved.
    static ECHO_MESSAGE_TICK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn bump_echo_message_tick() {
    ECHO_MESSAGE_TICK.with(|tick| tick.set(tick.get().wrapping_add(1)));
}

pub(crate) fn echo_area_message_tick() -> u64 {
    ECHO_MESSAGE_TICK.with(std::cell::Cell::get)
}

pub(crate) fn set_echo_area_message(text: Option<String>) {
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    bump_echo_message_tick();
    let value = text
        .as_ref()
        .map(|text| crate::lisp::primitives::strings::string_like_value(text.clone(), None));
    ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| *slot = value);
    let message = text.map(|text| (text, Vec::new()));
    // message3 displays right away: the new message (or, for a clear,
    // nothing) becomes the last-displayed one too.
    ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| slot.clone_from(&message));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = message);
}

/// A message carrying face spans (a propertized `message' — isearch's
/// prompt); the frontend paints the spans on the echo row.
pub(crate) fn set_echo_area_message_with_spans(text: String, spans: EchoSpans) {
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    bump_echo_message_tick();
    let len = text.chars().count();
    let mut props = crate::lisp::types::string_data::StringProperties::default();
    for (start, end, face) in &spans {
        let _ = crate::intervals::add_string_text_properties(
            &mut props,
            len,
            *start,
            (*end).min(len),
            Value::list([Value::symbol("face"), *face]),
            crate::intervals::PropertySetType::Replace,
            true,
        );
    }
    let value = crate::lisp::primitives::strings::string_like_value(text.clone(), props.take());
    ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| *slot = Some(value));
    let message = Some((text, spans));
    ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| slot.clone_from(&message));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = message);
}

pub(crate) fn set_echo_area_message_value(value: Value) -> Result<(), LispError> {
    let text = string_text(&value)?;
    let spans = string_face_spans(&value);
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    bump_echo_message_tick();
    ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| *slot = Some(value));
    let message = Some((text, spans));
    ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| slot.clone_from(&message));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = message);
    Ok(())
}

/// keyboard.c read_char's wipe when the next input event arrives: the
/// message channel empties without counting as an emission, so nothing
/// repaints until the next redisplay — a menu freezing redisplay keeps
/// the old pixels on the glass exactly as GNU does.
pub(crate) fn expire_echo_area_message() {
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = None);
    ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| *slot = None);
}

/// GNU xdisp.c:clear_message. Clearing is not a new message emission and
/// need not repaint immediately. The two echo buffers have independent
/// clearing flags, even when the Lisp callback preserves the current one.
pub(crate) fn clear_message(
    interp: &mut Interpreter,
    env: &mut Env,
    current: bool,
    last_displayed: bool,
) -> Result<(), LispError> {
    if current {
        let function = interp
            .forwarded_c_value("clear-message-function", &Env::new())
            .unwrap_or(Value::Nil);
        let preserve = if function_value_p(interp, &function, env)
            && !interp.garbage_collection_is_inhibited()
        {
            let restore = interp.bind_special_dynamic("inhibit-quit", Value::T, env)?;
            // A specbind watcher may have changed Vclear_message_function.
            let function = interp
                .forwarded_c_value("clear-message-function", &Env::new())
                .unwrap_or(Value::Nil);
            let result = redisplay_safe_call(interp, function, &[], env);
            interp.restore_special_dynamic(restore, env)?;
            result?
        } else {
            Value::Nil
        };
        if !values_eq_in_env(interp, &preserve, &Value::symbol("dont-clear-message"), env) {
            ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = None);
            ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| *slot = None);
        }
    }
    if last_displayed {
        ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| *slot = None);
    }
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    Ok(())
}

/// xdisp.c:dsafe__call / dsafe_calln with inhibit_quit=false. The caller
/// supplies any outer quit binding. Signals are logged without another
/// echo message; nonlocal control flow propagates to its enclosing target.
fn redisplay_safe_call(
    interp: &mut Interpreter,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let with_backtrace = interp
        .forwarded_c_value("backtrace-on-redisplay-error", &Env::new())
        .is_some_and(|value| value.is_truthy());
    if interp
        .forwarded_c_value("inhibit-eval-during-redisplay", &Env::new())
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(Value::Nil);
    }
    let restore = interp.bind_special_dynamic("inhibit-redisplay", Value::T, env)?;
    let handler_start =
        interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
    if with_backtrace {
        interp
            .push_handler_bindings(&[(vec!["error".into()], Value::symbol("debug-early--muted"))]);
    }
    let depth = env.len();
    let result = interp.call_function_value(function, None, args, env);
    interp.pop_handler_bindings(handler_start);
    env.truncate(depth);
    let result = match result.map_err(LispError::into_kind) {
        Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
            Err(LispError::from(error))
        }
        Err(error) => {
            // dsafe_eval_handler -> add_to_log -> Fformat_message and
            // message_dolog. Calling Fmessage here would run message hooks
            // and overwrite the echo area while handling its callback.
            interp.clear_batch_error_backtrace();
            super::call(
                interp,
                "format-message",
                &[
                    Value::String("Error during redisplay: %S signaled %S".into()),
                    Value::list(std::iter::once(function).chain(args.iter().cloned())),
                    crate::lisp::eval::error_condition_value(&LispError::from(error.clone())),
                ],
                env,
            )
            .and_then(|message| {
                let text = string_text(&message)?;
                log_message_text(interp, &text, env);
                Ok(Value::Nil)
            })
        }
        Ok(value) => Ok(value),
    };
    interp.restore_special_dynamic(restore, env)?;
    result
}

/// xdisp.c:message_to_stderr: a newline first when stdout was written
/// since, then MESSAGE's bytes, encoded with `coding-system-for-write' or
/// else `locale-coding-system' when either is set (without recording
/// `last-coding-system-used'), then a newline.
pub(crate) fn message_to_stderr(
    interp: &mut Interpreter,
    message: Option<&Value>,
    env: &mut Env,
) -> Result<(), LispError> {
    let bytes = match message {
        Some(message) => {
            let coding = interp
                .forwarded_c_value_key(cached_symbol!("coding-system-for-write"))
                .filter(|value| !value.is_nil())
                .or_else(|| interp.forwarded_c_value_key(cached_symbol!("locale-coding-system")))
                .unwrap_or(Value::Nil);
            let encoded = if coding.is_nil() {
                *message
            } else {
                encode_coding_value_recording(
                    interp,
                    message,
                    Some(coding.as_symbol()?),
                    false,
                    false,
                    env,
                )?
            };
            crate::lisp::primitives::text::internal_string_bytes(
                &string_like(&encoded).expect("a message is a string"),
            )?
        }
        None => Vec::new(),
    };
    let mut stderr = std::io::stderr().lock();
    if interp.batch_stdout_need_newline {
        interp.batch_stdout_need_newline = false;
        stderr
            .write_all(b"\n")
            .map_err(|error| LispError::Signal(error.to_string()))?;
    }
    stderr
        .write_all(&bytes)
        .and_then(|_| stderr.write_all(b"\n"))
        .map_err(|error| LispError::Signal(error.to_string()))
}

/// xdisp.c:message_with_string with logging enabled. Unlike Fmessage, its
/// batch path ignores inhibit-message and neither logs nor calls Lisp message
/// advice. Encoding is the locale or explicit write coding, without recording
/// last-coding-system-used.
pub(crate) fn message_with_string(
    interp: &mut Interpreter,
    format: &str,
    argument: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let batch = interp
        .forwarded_c_value("noninteractive", &Env::new())
        .is_some_and(|v| v.is_truthy());
    if !batch && interactive_window_metrics().is_none() {
        return Ok(());
    }
    let message = super::call(
        interp,
        "format-message",
        &[Value::string(format), argument],
        env,
    )?;
    if batch {
        message_to_stderr(interp, Some(&message), env)?;
        return Ok(());
    }
    let log = string_text(&message)?;
    message3(interp, message, &log, env)
}

/// xdisp.c:message3 of MESSAGE in the echo area, logged in *Messages* as
/// LOG (print_error_message logs the signalling function before the text
/// it shows).
pub(crate) fn message3(
    interp: &mut Interpreter,
    message: Value,
    log: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    // The interactive message3 path can run redisplay callbacks. Keep the
    // formatted object rooted across those calls using the existing stack roots.
    interp.with_lisp_stack_roots(&message, |interp| {
        clear_message(interp, env, true, true)?;
        // First flush out any partial line written with print.
        message_log_maybe_newline(interp, env);
        log_message_text(interp, log, env);
        if interp
            .forwarded_c_value("inhibit-message", &Env::new())
            .is_some_and(|value| value.is_truthy())
        {
            return Ok(());
        }
        let mut displayed = message;
        if let Some(function) = interp
            .forwarded_c_value("set-message-function", &Env::new())
            .filter(|value| function_value_p(interp, value, env))
            && !interp.garbage_collection_is_inhibited()
        {
            let restore = interp.bind_special_dynamic("inhibit-quit", Value::T, env)?;
            // A binding watcher can change the C function cell.
            let function = interp
                .forwarded_c_value("set-message-function", &Env::new())
                .unwrap_or(function);
            let result = redisplay_safe_call(interp, function, &[message], env);
            interp.restore_special_dynamic(restore, env)?;
            let result = result?;
            if result.is_string() {
                displayed = result;
            } else if result.is_truthy() {
                return Ok(());
            }
        }
        set_echo_area_message_value(displayed)
    })
}

// Shared existing message-log sink: xdisp.c message_dolog with NLFLAG, which
// Fmessage, message3 and add_to_log all reach. This is deliberately separate
// from message's echo/capture/stderr path, as xdisp.c:add_to_log requires.
// Creating the log buffer does not yet call `messages-buffer-mode'.
/// xdisp.c:message1 (message3): TEXT logged in *Messages*, then shown
/// unless `inhibit-message' -- in batch on stderr.
pub(crate) fn message1(
    interp: &mut Interpreter,
    text: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    let batch = interp
        .forwarded_c_value("noninteractive", &Env::new())
        .is_some_and(|v| v.is_truthy());
    if !batch {
        return message_with_string(interp, "%s", Value::string(text), env);
    }
    message_log_maybe_newline(interp, env);
    log_message_text(interp, text, env);
    if interp
        .forwarded_c_value("inhibit-message", &Env::new())
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    message_to_stderr(interp, Some(&Value::string(text)), env)
}

/// xdisp.c message_dolog of TEXT with NLFLAG: a line of its own (after a
/// partial printed line it is that line's end, as add_to_log's is).
pub(crate) fn log_message_text(interp: &mut Interpreter, text: &str, env: &Env) {
    log_message_dolog(interp, text, true, env);
}

/// xdisp.c message_log_maybe_newline: end the log line printing left open.
pub(crate) fn message_log_maybe_newline(interp: &mut Interpreter, env: &Env) {
    if MESSAGE_LOG_NEED_NEWLINE.with(std::cell::Cell::get) {
        log_message_dolog(interp, "", true, env);
    }
}

/// xdisp.c message_dolog of TEXT without NLFLAG: the log line left open
/// (print_error_message logs the signalling function this way).
pub(crate) fn log_message_partial(interp: &mut Interpreter, text: &str, env: &Env) {
    log_message_dolog(interp, text, false, env);
}

/// print.c's output to the echo area (PRINTCHARFUN t, interactive):
/// setup_echo_area_for_printing, which first ends the log line printing
/// left open when a message has held the echo area since, then
/// message_dolog of TEXT, its line left open, and TEXT in the echo area.
pub(crate) fn echo_area_print_logged(interp: &mut Interpreter, text: &str, env: &Env) {
    if !ECHO_FROM_PRINT.with(std::cell::Cell::get) {
        message_log_maybe_newline(interp, env);
    }
    log_message_partial(interp, text, env);
    echo_area_print(text);
}

fn log_message_dolog(interp: &mut Interpreter, text: &str, nlflag: bool, _env: &Env) {
    // message_dolog reads the two C variables (DEFVAR_LISP, so never
    // lexically bound) by their symbols.
    let buffer_name = interp
        .symbol_value_key(cached_symbol!("messages-buffer-name"))
        .and_then(|value| string_like(&value).map(|string| string.text))
        .unwrap_or_else(|| "*Messages*".into());
    let log_max = interp
        .symbol_value_key(cached_symbol!("message-log-max"))
        .unwrap_or(Value::T);
    if log_max.is_nil() {
        return;
    }
    let buffer_id = interp
        .find_buffer(&buffer_name)
        .map(|(id, _)| id)
        .unwrap_or_else(|| interp.create_buffer(&buffer_name).0);
    // xdisp.c message_dolog: `buffer-undo-list' is t and
    // `cache-long-scans' nil in the log buffer on every message.
    cached_symbol!("cache-long-scans")
        .with(|symbol| interp.set_buffer_local_value_symbol(buffer_id, symbol, Value::Nil));
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        buffer.set_undo_list(Value::T);
        // FIXNATP: any other non-nil value keeps every line.
        let max_lines = log_max.fixnum().and_then(|max| usize::try_from(max).ok());
        message_dolog(&mut buffer, text, nlflag, max_lines);
    }
    MESSAGE_LOG_NEED_NEWLINE.with(|flag| flag.set(!nlflag));
}

/// Append TEXT at the end of the widened log buffer; with NLFLAG a newline
/// after it, the line folded into an identical previous one (" [N times]")
/// or replacing a previous "...", and at most MAX_LINES lines kept. Point,
/// BEGV and ZV are restored as GNU's insertion-type-nil markers leave
/// them, and point or ZV at the end stays at the end.
fn message_dolog(
    buffer: &mut crate::buffer::Buffer,
    text: &str,
    nlflag: bool,
    max_lines: Option<usize>,
) {
    fn after_delete(position: &mut usize, from: usize, to: usize) {
        if *position > to {
            *position -= to - from;
        } else if *position > from {
            *position = from;
        }
    }
    let end = buffer.size_total() + 1;
    let (begv, zv) = buffer.restriction();
    let point_at_end = buffer.point() == end;
    let zv_at_end = zv == end;
    // Insertion at Z moves none of these: each is at most Z.
    let mut saved = [buffer.point(), begv, zv];
    buffer.widen();
    buffer.goto_char(end);
    if !nlflag {
        buffer.insert(text);
        let end = buffer.size_total() + 1;
        let [point, begv, zv] = saved;
        buffer.restore_restriction(begv, if zv_at_end { end } else { zv });
        buffer.goto_char(if point_at_end { end } else { point });
        return;
    }
    let mut line = String::with_capacity(text.len() + 1);
    line.push_str(text);
    line.push('\n');
    buffer.insert(&line);
    let end = buffer.size_total() + 1;
    let this_bol = buffer.line_start_at(end - 1);
    if this_bol > 1 {
        let prev_bol = buffer.line_start_at(this_bol - 1);
        let lines = buffer.buffer_substring(prev_bol, end).unwrap_or_default();
        // The previous line is the start of LINES, THIS_BOL - PREV_BOL
        // characters long.
        let this_bol_byte = lines
            .char_indices()
            .nth(this_bol - prev_bol)
            .map_or(lines.len(), |(byte, _)| byte);
        let dups = message_log_check_duplicate(lines.as_bytes(), this_bol_byte);
        if dups > 0 {
            let _ = buffer.delete_region(prev_bol, this_bol);
            for position in &mut saved {
                after_delete(position, prev_bol, this_bol);
            }
            if dups > 1 {
                // Inserted before markers, before the final newline.
                let count = format!(" [{dups} times]");
                let at = buffer.size_total();
                buffer.goto_char(at);
                buffer.insert_before_markers(&count);
                for position in &mut saved {
                    if *position >= at {
                        *position += count.len();
                    }
                }
            }
        }
    }
    if let Some(cut) =
        max_lines.and_then(|max| buffer.after_newlines_from_end(max.saturating_add(1)))
    {
        let _ = buffer.delete_region(1, cut);
        for position in &mut saved {
            after_delete(position, 1, cut);
        }
    }
    let end = buffer.size_total() + 1;
    let [point, begv, zv] = saved;
    buffer.restore_restriction(begv, if zv_at_end { end } else { zv });
    buffer.goto_char(if point_at_end { end } else { point });
}

/// xdisp.c message_log_check_duplicate. LINES runs from the previous line's
/// start to the end of the log, the newline after the line at THIS_BOL.
/// 0 if the lines differ, 1 if the new line replaces a previous line that
/// matched it through "...", or N > 1 for " [N times]".
fn message_log_check_duplicate(lines: &[u8], this_bol: usize) -> i64 {
    let len = lines.len() - 1 - this_bol;
    let mut seen_dots = false;
    for i in 0..len {
        if i >= 3 && lines[i - 3..i] == *b"..." {
            seen_dots = true;
        }
        if lines[i] != lines[this_bol + i] {
            return i64::from(seen_dots);
        }
    }
    let rest = &lines[len..];
    if rest[0] == b'\n' {
        return 2;
    }
    if let Some(digits) = rest.strip_prefix(b" [") {
        let (n, tail) = strtoimax(digits);
        if 0 < n && n < i64::MAX && tail.starts_with(b" times]\n") {
            return n + 1;
        }
    }
    0
}

/// C strtoimax in base 10: leading `isspace', an optional sign, then digits,
/// saturating on overflow. Without digits, 0 and the whole input.
fn strtoimax(input: &[u8]) -> (i64, &[u8]) {
    let start = input
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'))
        .unwrap_or(input.len());
    let mut rest = &input[start..];
    let negative = match rest.first() {
        Some(b'-') => {
            rest = &rest[1..];
            true
        }
        Some(b'+') => {
            rest = &rest[1..];
            false
        }
        _ => false,
    };
    let digits = rest.iter().take_while(|byte| byte.is_ascii_digit()).count();
    if digits == 0 {
        return (0, input);
    }
    let value = rest[..digits].iter().try_fold(0i64, |value, digit| {
        let digit = i64::from(digit - b'0');
        value.checked_mul(10).and_then(|value| {
            if negative {
                value.checked_sub(digit)
            } else {
                value.checked_add(digit)
            }
        })
    });
    let value = value.unwrap_or(if negative { i64::MIN } else { i64::MAX });
    (value, &rest[digits..])
}

/// Printing to the `t' stream in an interactive session displays in the
/// echo area (GNU's print_string to Qt); consecutive prints of one
/// sequence — eval-expression's value then its print format — append.
/// dispnew.c:ring_bell: `ring-bell-function' when set, called with the
/// variable nil meanwhile so that an error in it is not repeated, else
/// the terminal's bell.
pub(crate) fn ring_bell(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let function = interp
        .lookup_var("ring-bell-function", env)
        .unwrap_or(Value::Nil);
    if function.is_nil() {
        // FRAME_TERMINAL (f)->ring_bell_hook: the initial terminal (the
        // batch frame's) has none.
        if interp
            .decode_terminal(&Value::Nil)
            .is_some_and(|terminal| terminal.borrow().kind.is_none())
        {
            return Ok(());
        }
        use std::io::Write as _;
        let mut output = crate::lisp::eval::terminal::output()
            .map_err(|error| LispError::Signal(error.to_string()))?;
        return output
            .write_all(b"\x07")
            .and_then(|()| output.flush())
            .map_err(|error| LispError::Signal(error.to_string()));
    }
    interp.set_variable("ring-bell-function", Value::Nil, env);
    interp.call_function_value(function, None, &[], env)?;
    interp.set_variable("ring-bell-function", function, env);
    Ok(())
}

/// dispnew.c:bitch_at_user: in batch a BEL on stdout; while a keyboard
/// macro runs, the user-error that ends it; else the bell.
pub(crate) fn bitch_at_user(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if interp
        .forwarded_c_value("noninteractive", env)
        .is_some_and(|value| value.is_truthy())
    {
        return crate::lisp::primitives::batch_stdout::write(b"\x07")
            .map_err(|error| LispError::Signal(error.to_string()));
    }
    if interp
        .lookup_var("executing-kbd-macro", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("user-error"),
            Value::string("Keyboard macro terminated by a command ringing the bell"),
        ])));
    }
    ring_bell(interp, env)
}

pub(crate) fn echo_area_print(text: &str) {
    let appending = ECHO_FROM_PRINT.with(std::cell::Cell::get);
    bump_echo_message_tick();
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| match slot {
        Some((existing, _)) if appending => existing.push_str(text),
        _ => *slot = Some((text.to_string(), Vec::new())),
    });
    ECHO_AREA_LAST_DISPLAYED
        .with_borrow_mut(|slot| *slot = ECHO_AREA_MESSAGE.with_borrow(|current| current.clone()));
    ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| {
        *slot = ECHO_AREA_MESSAGE.with_borrow(|current| {
            current.as_ref().map(|(text, _)| {
                crate::lisp::primitives::strings::string_like_value(text.clone(), None)
            })
        })
    });
    ECHO_FROM_PRINT.with(|flag| flag.set(true));
}

pub(crate) fn echo_area_message() -> Option<String> {
    ECHO_AREA_MESSAGE.with_borrow(|slot| slot.as_ref().map(|(text, _)| text.clone()))
}

fn echo_area_message_value() -> Option<Value> {
    ECHO_AREA_MESSAGE_VALUE.with_borrow(|slot| *slot)
}

#[cfg(test)]
pub(crate) fn echo_area_message_with_spans() -> Option<(String, EchoSpans)> {
    ECHO_AREA_MESSAGE.with_borrow(|slot| slot.clone())
}

/// What redisplay shows on the echo row — xdisp.c's echo_area_display.
/// Normally the current message, which then becomes the last-displayed
/// one (emptiness after an input-arrival wipe clears it).  Under
/// Fredisplay's preserve semantics a wiped-but-last-displayed message
/// shows again instead, leaving the wipe pending for the next normal
/// redisplay (redisplay_preserve_echo_area).
pub(crate) fn echo_display_message() -> Option<(String, EchoSpans)> {
    let current = ECHO_AREA_MESSAGE.with_borrow(|slot| slot.clone());
    if current.is_none() && ECHO_PRESERVE_REDISPLAY.with(std::cell::Cell::get) {
        return ECHO_AREA_LAST_DISPLAYED.with_borrow(|slot| slot.clone());
    }
    ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| slot.clone_from(&current));
    current
}

/// Run F with `redisplay's preserve-echo-area semantics in force
/// (dispnew.c's Fredisplay calls redisplay_preserve_echo_area).
pub(crate) fn with_preserved_echo_redisplay<T>(f: impl FnOnce() -> T) -> T {
    let previous = ECHO_PRESERVE_REDISPLAY.with(|flag| flag.replace(true));
    let result = f();
    ECHO_PRESERVE_REDISPLAY.with(|flag| flag.set(previous));
    result
}

/// The face runs of a propertized string value, in char offsets.
pub(crate) fn string_face_spans(value: &Value) -> EchoSpans {
    let Kind::StringObject(state) = value.kind() else {
        return Vec::new();
    };
    let state = state.borrow();
    let Some(intervals) = state.props.intervals() else {
        return Vec::new();
    };
    intervals
        .iter()
        .filter_map(|(start, end, plist)| {
            let face = crate::lisp::primitives::strings::lisp_plist_get(plist, "face");
            let face = if face.is_nil() {
                crate::lisp::primitives::strings::lisp_plist_get(plist, "font-lock-face")
            } else {
                face
            };
            (!face.is_nil()).then_some((start, end, face))
        })
        .collect()
}

fn valid_image_spec(interp: &Interpreter, spec: &Value, env: &Env) -> bool {
    let Ok(items) = spec.to_vec() else {
        return false;
    };
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head == "image")
        || items.len() < 5
        || (items.len() - 1) % 2 != 0
    {
        return false;
    }
    let mut properties = std::collections::HashMap::new();
    for pair in items[1..].as_chunks::<2>().0 {
        let Ok(key) = pair[0].as_symbol() else {
            return false;
        };
        if !key.starts_with(':') || properties.insert(key, &pair[1]).is_some() {
            return false;
        }
    }
    let Some(Kind::Symbol(image_type)) = properties.get(":type").copied().map(|v| v.kind()) else {
        return false;
    };
    let type_supported = interp
        .lookup_var("image-types", env)
        .and_then(|types| types.to_vec().ok())
        .is_some_and(|types| {
            types
                .iter()
                .any(|value| matches!(value.kind(), Kind::Symbol(name) if name == image_type))
        });
    let file = properties.get(":file").copied();
    let data = properties.get(":data").copied();
    type_supported
        && matches!(
            (file.map(|v| v.kind()), data.map(|v| v.kind())),
            (Some(Kind::StringObject(_)), None) | (None, Some(Kind::StringObject(_)))
        )
}

fn current_bidi_paragraph_direction_value(interp: &Interpreter, env: &Env) -> Value {
    if !interp.buffer.borrow().is_multibyte()
        || interp
            .lookup_var("bidi-display-reordering", env)
            .is_some_and(|value| value.is_nil())
    {
        return Value::symbol("left-to-right");
    }
    if let Some(direction) = interp
        .lookup_var("bidi-paragraph-direction", env)
        .filter(|value| !value.is_nil())
    {
        return direction;
    }

    let point_min = interp.buffer.borrow().point_min();
    let point_max = interp.buffer.borrow().point_max();
    let position = interp.buffer.borrow().point().clamp(point_min, point_max);
    // GNU display paragraphs span ordinary newlines.  A blank line starts a
    // new paragraph only once point has reached nonblank text after it; the
    // separator itself and trailing blank lines retain the preceding
    // paragraph's direction.
    let mut start = point_min;
    let mut scan = point_min;
    while scan < point_max {
        if interp.buffer.borrow().char_at(scan) != Some('\n') {
            scan += 1;
            continue;
        }
        let mut separator_end = scan + 1;
        while separator_end < point_max
            && matches!(
                interp.buffer.borrow().char_at(separator_end),
                Some(' ' | '\t' | '\u{c}')
            )
        {
            separator_end += 1;
        }
        if interp.buffer.borrow().char_at(separator_end) != Some('\n') {
            scan += 1;
            continue;
        }
        let mut next_text = separator_end + 1;
        while next_text < point_max
            && matches!(
                interp.buffer.borrow().char_at(next_text),
                Some('\n' | ' ' | '\t' | '\u{c}')
            )
        {
            next_text += 1;
        }
        if next_text < point_max && next_text <= position {
            start = next_text;
            scan = next_text;
        } else {
            break;
        }
    }
    let text = interp
        .buffer
        .borrow()
        .buffer_substring(start, point_max)
        .unwrap_or_default();
    let bidi = unicode_bidi::BidiInfo::new(&text, None);
    if bidi
        .paragraphs
        .first()
        .is_some_and(|paragraph| paragraph.level.is_rtl())
    {
        Value::symbol("right-to-left")
    } else {
        Value::symbol("left-to-right")
    }
}

fn with_selected_window_buffer<T>(
    interp: &mut Interpreter,
    operation: impl FnOnce(&mut Interpreter) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let selected_buffer = interp.selected_window_buffer_id();
    let saved_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(selected_buffer)?;
    let result = operation(interp);
    let restore = interp.set_current_buffer_id(saved_buffer);
    match (result, restore) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn image_hot_spot_contains(area: &Value, x: i64, y: i64) -> bool {
    let Ok(kind) = area
        .car()
        .and_then(|value| value.as_symbol().map(str::to_string))
    else {
        return false;
    };
    let Ok(shape) = area.cdr() else {
        return false;
    };
    match kind.as_str() {
        "rect" => {
            let Ok((top_left, bottom_right)) = shape
                .cons_values()
                .ok_or_else(|| wrong_type_argument("consp", shape))
            else {
                return false;
            };
            let Ok((x0, y0)) = top_left
                .cons_values()
                .ok_or_else(|| wrong_type_argument("consp", top_left))
                .and_then(|(x, y)| Ok((x.as_integer()?, y.as_integer()?)))
            else {
                return false;
            };
            let Ok((x1, y1)) = bottom_right
                .cons_values()
                .ok_or_else(|| wrong_type_argument("consp", bottom_right))
                .and_then(|(x, y)| Ok((x.as_integer()?, y.as_integer()?)))
            else {
                return false;
            };
            x0 <= x && x <= x1 && y0 <= y && y <= y1
        }
        "circle" => {
            let Some((center, radius)) = shape.cons_values() else {
                return false;
            };
            let Some((x0, y0)) = center.cons_values() else {
                return false;
            };
            let (Ok(x0), Ok(y0), Ok(radius)) =
                (x0.as_integer(), y0.as_integer(), radius.as_float())
            else {
                return false;
            };
            let dx = x0 as f64 - x as f64;
            let dy = y0 as f64 - y as f64;
            dx * dx + dy * dy <= radius * radius
        }
        "poly" => {
            let Ok(coordinates) = vector_items(&shape) else {
                return false;
            };
            if coordinates.len() < 6 || coordinates.len() % 2 != 0 {
                return false;
            }
            let Ok(mut x0) = coordinates[coordinates.len() - 2].as_integer() else {
                return false;
            };
            let Ok(mut y0) = coordinates[coordinates.len() - 1].as_integer() else {
                return false;
            };
            let mut inside = false;
            for pair in coordinates.as_chunks::<2>().0 {
                let (x1, y1) = (x0, y0);
                let (Ok(next_x), Ok(next_y)) = (pair[0].as_integer(), pair[1].as_integer()) else {
                    return false;
                };
                x0 = next_x;
                y0 = next_y;
                if (x0 >= x && x1 >= x) || (x0 < x && x1 < x) || (y > y0 && y > y1) {
                    continue;
                }
                if y < y0 + ((y1 - y0) * (x - x0)) / (x1 - x0) {
                    inside = !inside;
                }
            }
            inside
        }
        _ => false,
    }
}

fn lookup_image_map(map: &Value, x: i64, y: i64) -> Value {
    let Ok(entries) = map.to_vec() else {
        return Value::Nil;
    };
    entries
        .into_iter()
        .find(|entry| {
            entry
                .car()
                .is_ok_and(|area| image_hot_spot_contains(&area, x, y))
        })
        .unwrap_or(Value::Nil)
}

fn decode_live_frame(
    interp: &Interpreter,
    frame: Option<&Value>,
    nil_defaults_to_selected: bool,
) -> Result<Value, LispError> {
    match frame.map(|v| v.kind()) {
        None => Ok(interp.selected_frame_value()),
        Some(Kind::Nil) if nil_defaults_to_selected => Ok(interp.selected_frame_value()),
        Some(Kind::Frame(id)) if interp.frame_is_live(id) => Ok(Value::Frame(id)),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.value())),
    }
}

fn require_live_terminal(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<crate::lisp::types::TerminalRef, LispError> {
    let value = value.unwrap_or(&Value::Nil);
    interp
        .decode_terminal(value)
        .ok_or_else(|| wrong_type_argument("terminal-live-p", *value))
}

// window.c accepts either a live frame or a valid leaf/interior window in
// frame-root-window, frame-first-window, and frame-selected-window.
fn frame_or_window_id(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<crate::lisp::types::FrameRef, LispError> {
    if let Some(value) = value
        && let Some(window) = window_record_id_from_value(interp, value)
        && !matches!(
            window_slot_value(interp, window, WINDOW_KIND_SLOT).kind(),
            Kind::Symbol(kind) if kind == DELETED_WINDOW_KIND
        )
    {
        return Ok(interp
            .window_frame_id(window)
            .expect("valid window stores its owning frame"));
    }
    super::frames::decode_live_frame(interp, value, true)
}

fn current_frame_and_buffer_state(interp: &Interpreter) -> Vec<Value> {
    let mut state = Vec::new();
    if interp.terminal_live() {
        state.push(interp.selected_frame_value());
        state.push(
            interp
                .frame_parameter_override("name")
                .unwrap_or_else(|| interp.frame_name_value()),
        );
    }
    for (buffer_id, name) in &interp.buffer_list {
        if name.starts_with(' ') {
            continue;
        }
        let Some(buffer) = interp.get_buffer_by_id(*buffer_id) else {
            continue;
        };
        state.push(interp.buffer_value(*buffer_id).expect("live buffer object"));
        state.push(
            interp
                .buffer_local_value(*buffer_id, "buffer-read-only")
                .or_else(|| interp.default_value("buffer-read-only"))
                .unwrap_or(Value::Nil),
        );
        state.push(if buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        });
    }
    state
}

fn frame_or_buffer_changed(
    interp: &mut Interpreter,
    variable: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let variable_name = match variable.map(|v| v.kind()) {
        None | Some(Kind::Nil) => None,
        Some(value) => Some(value.value().as_symbol()?.to_string()),
    };
    let old_state = if let Some(name) = variable_name.as_deref() {
        interp.lookup(name, env)?
    } else {
        interp.frame_and_buffer_state()
    };
    let current = current_frame_and_buffer_state(interp);
    let old_items = is_vector_value(&old_state)
        .then(|| vector_items(&old_state))
        .transpose()?;
    let unchanged = old_items.as_ref().is_some_and(|items| {
        items.len() > current.len()
            && current
                .iter()
                .zip(items)
                .all(|(new, old)| values_eq_in_env(interp, new, old, env))
            && matches!(
                items.get(current.len()).map(|v| v.kind()),
                Some(Kind::Symbol(symbol)) if symbol == "lambda"
            )
    });
    if unchanged {
        return Ok(Value::Nil);
    }

    let required = current.len() + 1;
    let reusable = old_items.as_ref().is_some_and(|items| {
        required <= items.len() && required.saturating_add(20) >= items.len() / 2
    });
    let state = if reusable {
        old_state
    } else {
        Value::vector(std::iter::repeat_n(Value::symbol("lambda"), required + 20))
    };
    for (index, value) in current.into_iter().enumerate() {
        aset_vector_value(&state, index, value)?;
    }
    let slots = vector_items(&state)?.len();
    for index in required - 1..slots {
        aset_vector_value(&state, index, Value::symbol("lambda"))?;
    }

    if !reusable {
        if let Some(name) = variable_name {
            interp.set_variable(&name, state, env);
        } else {
            interp.set_frame_and_buffer_state(state);
        }
    }
    Ok(Value::T)
}

fn window_id_or_selected(interp: &Interpreter, value: &Value) -> Result<u64, LispError> {
    if value.is_nil() {
        return Ok(interp.selected_window_id());
    }
    window_record_id_from_value(interp, value)
        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), *value))
}

/// decode_valid_window: a window record not deleted, nil the selected one.
fn valid_window_or_selected(
    interp: &Interpreter,
    value: &Value,
) -> Result<crate::lisp::types::RecordRef, LispError> {
    let window = if value.is_nil() {
        interp.record_ref(interp.selected_window_id())
    } else {
        window_record(value)
    };
    window
        .filter(|window| !window_kind_is(*window, DELETED_WINDOW_KIND))
        .ok_or_else(|| wrong_type_argument("window-valid-p", *value))
}

/// Whether WINDOW's kind is KIND.
fn window_kind_is(window: crate::lisp::types::RecordRef, kind: &str) -> bool {
    matches!(
        window.slots.get(WINDOW_KIND_SLOT).map(|slot| slot.kind()),
        Some(Kind::Symbol(own)) if own == kind
    )
}

/// window.c:Fwindow_parameter: PARAMETER's entry found by `assq'.
fn window_parameter_value(interp: &Interpreter, window_id: u64, parameter: &Value) -> Value {
    window_parameter_entry(interp, window_id, parameter)
        .and_then(|entry| entry.cdr().ok())
        .unwrap_or(Value::Nil)
}

fn window_parameter_entry(
    interp: &Interpreter,
    window_id: u64,
    parameter: &Value,
) -> Option<Value> {
    // Fassq (parameter, w->window_parameters), walking the alist in place.
    let mut params = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
        .copied()?;
    while let Some((item, rest)) = params.cons_values() {
        if item.car().is_ok_and(|key| key.eq_value(*parameter)) {
            return Some(item);
        }
        params = rest;
    }
    None
}

/// window.c:Fset_window_parameter: change PARAMETER's `assq' entry in
/// place, or push a new one on the front (a nil VALUE stays an entry).
fn set_window_parameter_value(
    interp: &mut Interpreter,
    window_id: u64,
    parameter: Value,
    value: Value,
) -> Result<Value, LispError> {
    if let Some(entry) = window_parameter_entry(interp, window_id, &parameter) {
        entry.set_cdr(value)?;
        return Ok(value);
    }
    let existing = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
        .copied()
        .unwrap_or(Value::Nil);
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= WINDOW_PARAMETERS_SLOT {
        record.slots.resize(WINDOW_PARAMETERS_SLOT + 1, Value::Nil);
    }
    record.slots[WINDOW_PARAMETERS_SLOT] = Value::cons(Value::cons(parameter, value), existing);
    Ok(value)
}

fn window_slot_value(interp: &Interpreter, window_id: u64, slot: usize) -> Value {
    interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(slot))
        .cloned()
        .unwrap_or(Value::Nil)
}

fn set_window_slot_value(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
    value: Value,
) -> Result<Value, LispError> {
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= slot {
        record.slots.resize(slot + 1, Value::Nil);
    }
    record.slots[slot] = value;
    Ok(value)
}

fn window_link(interp: &Interpreter, window_id: u64, slot: usize) -> Option<u64> {
    match window_slot_value(interp, window_id, slot).kind() {
        Kind::Record(id) => Some(id.id),
        _ => None,
    }
}

fn window_geometry(interp: &Interpreter, window_id: u64) -> (i64, i64, i64, i64) {
    let integer_slot = |slot, fallback| {
        window_slot_value(interp, window_id, slot)
            .as_integer()
            .unwrap_or(fallback)
    };
    (
        integer_slot(WINDOW_PIXEL_WIDTH_SLOT, interp.frame_width()),
        integer_slot(WINDOW_PIXEL_HEIGHT_SLOT, interp.frame_height()),
        integer_slot(WINDOW_PIXEL_LEFT_SLOT, 0),
        integer_slot(WINDOW_PIXEL_TOP_SLOT, 0),
    )
}

fn window_at_frame_coordinates(
    interp: &Interpreter,
    frame: crate::lisp::types::FrameRef,
    x: i64,
    y: i64,
) -> Option<u64> {
    frame_window_tree_leaf_ids(interp, frame)
        .into_iter()
        .chain(std::iter::once(frame.borrow().minibuffer_window_id()))
        .find(|id| {
            let (width, height, left, top) = window_geometry(interp, *id);
            x >= left && x < left + width && y >= top && y < top + height
        })
}

fn position_at_coordinates(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut x = super::collections::fixnum_index_arg(&args[0])?;
    let mut y = super::collections::fixnum_index_arg(&args[1])?;
    if x < -1 {
        return Err(wrong_type_argument("natnump", args[0]));
    }
    if y < 0 {
        return Err(wrong_type_argument("natnump", args[1]));
    }
    let requested = args.get(2).copied().unwrap_or(Value::Nil);
    let frame = if requested.is_nil() || is_window_value(interp, &requested) {
        let window = live_window_id_or_selected(interp, Some(&requested))?;
        let (_, _, left, top) = window_geometry(interp, window);
        x += left;
        y += top;
        if args.get(3).is_none_or(|value| value.is_nil()) {
            x += interp.window_margins(window).0.unwrap_or(0);
        }
        interp.window_frame_id(window).expect("live window frame")
    } else {
        super::frames::decode_live_frame(interp, Some(&requested), false)?
    };
    let Some(window_id) = window_at_frame_coordinates(interp, frame, x, y) else {
        return Ok(Value::list([
            Value::Frame(frame),
            Value::Nil,
            Value::cons(Value::Integer(x), Value::Integer(y)),
            Value::Integer(0),
        ]));
    };
    let window = interp.record_value(window_id);
    let (_, _, left, top) = window_geometry(interp, window_id);
    let buffer = window_buffer_id(interp, &window)
        .ok_or_else(|| wrong_type_argument("window-live-p", window))?;
    let area = super::call(
        interp,
        "coordinates-in-window-p",
        &[Value::cons(Value::Integer(x), Value::Integer(y)), window],
        env,
    )?;
    let local_x = x - left;
    let local_y = y - top;
    if area.is_symbol() {
        return Ok(Value::list([
            window,
            area,
            Value::cons(Value::Integer(local_x), Value::Integer(local_y)),
            Value::Integer(0),
            Value::Nil,
            Value::Nil,
            Value::cons(Value::Integer(local_x), Value::Integer(local_y)),
            Value::Nil,
            Value::cons(Value::Integer(-1), Value::Integer(-1)),
            Value::cons(Value::Integer(-1), Value::Integer(-1)),
        ]));
    }
    let text_x = area.car()?.as_integer()?;
    let text_y = local_y
        - window_line_height(interp, buffer, "tab-line-format", env)
        - window_line_height(interp, buffer, "header-line-format", env);
    let old_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(buffer)?;
    let result = (|| {
        let start = window_start(interp, Some(&window))?;
        let end = interp.buffer.borrow().point_max();
        let hscroll = window_hscroll_state(interp, window_id).hscroll.max(0);
        let motion_args = crate::lisp::alloc::RootedVec::from_vec(vec![
            Value::Integer(start as i64),
            Value::cons(Value::Integer(-hscroll), Value::Integer(0)),
            Value::Integer(end as i64),
            Value::cons(Value::Integer(text_x), Value::Integer(text_y)),
            Value::Nil,
            Value::cons(Value::Integer(hscroll), Value::Integer(0)),
            window,
        ]);
        let motion = super::buffer_edit::display_motion(interp, env, &motion_args, true)?;
        let dx = text_x - motion.hpos;
        let dy = text_y - motion.vpos;
        // dispnew.c obtains dimensions from a realized glyph matrix.
        // Batch has no such matrix, even though position traversal works.
        let realized = interactive_window_metrics().is_some();
        let width = if realized {
            interp
                .buffer
                .borrow()
                .char_at(motion.position)
                .map_or(0, |character| {
                    if character == '\t' || character == '\n' {
                        1
                    } else {
                        unicode_width::UnicodeWidthChar::width(character).unwrap_or(0) as i64
                    }
                })
        } else {
            0
        };
        let position = Value::Integer(motion.position as i64);
        Ok(Value::list([
            window,
            position,
            Value::cons(Value::Integer(text_x), Value::Integer(text_y)),
            Value::Integer(0),
            Value::Nil,
            position,
            Value::cons(
                Value::Integer(text_x.max(motion.hpos)),
                Value::Integer(motion.vpos),
            ),
            Value::Nil,
            Value::cons(Value::Integer(dx), Value::Integer(dy)),
            Value::cons(Value::Integer(width), Value::Integer(i64::from(realized))),
        ]))
    })();
    interp.with_lisp_stack_roots(&result, |interp| interp.set_current_buffer_id(old_buffer))?;
    result
}

fn set_window_geometry(
    interp: &mut Interpreter,
    window_id: u64,
    geometry: (i64, i64, i64, i64),
) -> Result<(), LispError> {
    for (slot, value) in [
        (WINDOW_PIXEL_WIDTH_SLOT, geometry.0),
        (WINDOW_PIXEL_HEIGHT_SLOT, geometry.1),
        (WINDOW_PIXEL_LEFT_SLOT, geometry.2),
        (WINDOW_PIXEL_TOP_SLOT, geometry.3),
    ] {
        set_window_slot_value(interp, window_id, slot, Value::Integer(value))?;
    }
    Ok(())
}

fn frame_root_window_value(interp: &Interpreter) -> Value {
    interp.root_window_value()
}

fn is_live_ordinary_window(interp: &Interpreter, id: u64) -> bool {
    let kind = window_slot_value(interp, id, WINDOW_KIND_SLOT);
    !matches!(
        kind.kind(),
        Kind::Symbol(ref kind)
            if matches!(
                kind.as_str(),
                MINIBUFFER_WINDOW_KIND
                    | INTERNAL_HORIZONTAL_WINDOW_KIND
                    | INTERNAL_VERTICAL_WINDOW_KIND
                    | DELETED_WINDOW_KIND
            )
    ) && window_buffer_id(interp, &interp.record_value(id)).is_some()
}

/// Leaf windows of the frame's window tree in GNU's canonical order — a
/// depth-first walk that descends first children and follows sibling
/// chains.  `next-window' cycles and `window-list' both follow it, and it
/// is the top-to-bottom, left-to-right order the glass paints.
fn window_tree_leaf_ids(interp: &Interpreter) -> Vec<u64> {
    frame_window_tree_leaf_ids(interp, interp.selected_frame_id)
}

fn frame_window_tree_leaf_ids(
    interp: &Interpreter,
    frame: crate::lisp::types::FrameRef,
) -> Vec<u64> {
    let root_id = frame.borrow().root_window_id();
    // A frame of one window is its root.
    if window_link(interp, root_id, WINDOW_FIRST_CHILD_SLOT).is_none() {
        return vec![root_id];
    }
    // A malformed tree (stale sibling links) must not loop forever; no
    // healthy tree revisits a window record.
    let budget = interp.record_count_by_type("window").saturating_add(1);
    let mut leaves = Vec::new();
    let mut stack = vec![root_id];
    let mut visited = 0usize;
    while let Some(id) = stack.pop() {
        visited += 1;
        if visited > budget {
            return Vec::new();
        }
        match window_link(interp, id, WINDOW_FIRST_CHILD_SLOT) {
            Some(child) => {
                let mut chain = Vec::new();
                let mut walk = Some(child);
                while let Some(node) = walk {
                    chain.push(node);
                    if chain.len() > budget {
                        return Vec::new();
                    }
                    walk = window_link(interp, node, WINDOW_NEXT_SIBLING_SLOT);
                }
                stack.extend(chain.into_iter().rev());
            }
            None => leaves.push(id),
        }
    }
    leaves
}

pub(crate) fn live_ordinary_window_ids(interp: &Interpreter) -> Vec<u64> {
    let from_tree: Vec<u64> = window_tree_leaf_ids(interp)
        .into_iter()
        .filter(|id| is_live_ordinary_window(interp, *id))
        .collect();
    if !from_tree.is_empty() {
        return from_tree;
    }
    // No usable tree (no root binding yet, or a degenerate walk): fall
    // back to record order, which equals tree order for a lone window.
    interp
        .record_ids_by_type("window")
        .into_iter()
        .filter(|id| is_live_ordinary_window(interp, *id))
        .collect()
}

/// One live window as the terminal frontend renders it: its frame rect,
/// buffer, and display anchors.  Geometry counts total lines and columns
/// — the mode line is the rect's last row, and a window not flush with
/// the frame's right edge spends its last column on the vertical border.
#[derive(Clone, Copy)]
pub(crate) struct WindowRenderInfo {
    pub(crate) window_id: u64,
    pub(crate) buffer_id: u64,
    pub(crate) left: usize,
    pub(crate) top: usize,
    pub(crate) width: usize,
    pub(crate) height: usize,
    /// The window's start marker (outside a later narrowing until
    /// redisplay moves it).
    pub(crate) start: usize,
    /// The window's own point: the live buffer point for the selected
    /// window, the saved window-point slot otherwise.
    pub(crate) point: usize,
    pub(crate) selected: bool,
}

/// The frame's live windows in paint order (the tree walk), with the
/// geometry and content anchors redisplay needs.
pub(crate) fn window_render_layout(interp: &Interpreter) -> Vec<WindowRenderInfo> {
    let selected_id = interp.selected_window_id();
    live_ordinary_window_ids(interp)
        .into_iter()
        .filter_map(|window_id| {
            let selected = window_id == selected_id;
            let buffer_id = window_buffer_id(interp, &interp.record_value(window_id))?;
            let (width, height, left, top) = window_geometry(interp, window_id);
            let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
            let clamp = |value: i64| value.clamp(point_min as i64, point_max as i64) as usize;
            let start = window_start_marker(interp, window_id, buffer_id);
            let live_point = if buffer_id == interp.current_buffer_id() {
                interp.buffer.borrow().point()
            } else {
                interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.point())
                    .unwrap_or(point_min)
            };
            let point = if selected {
                live_point
            } else {
                window_slot_value(interp, window_id, WINDOW_POINT_SLOT)
                    .as_integer()
                    .map(clamp)
                    .unwrap_or(live_point)
            };
            Some(WindowRenderInfo {
                window_id,
                buffer_id,
                left: left.max(0) as usize,
                top: top.max(0) as usize,
                width: width.max(0) as usize,
                height: height.max(0) as usize,
                start,
                point,
                selected,
            })
        })
        .collect()
}

/// A face's realized tty attributes, the frontend's paint unit: ANSI
/// color indexes plus the boolean attributes a terminal can show.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub(crate) struct TtyFaceAttrs {
    pub(crate) foreground: Option<u8>,
    pub(crate) background: Option<u8>,
    pub(crate) bold: bool,
    pub(crate) underline: bool,
    pub(crate) reverse: bool,
    /// GNU's `:extend': the face keeps painting past the end of line to
    /// the window edge (the region's whole-row highlight).
    pub(crate) extend: bool,
}

/// xfaces.c tty_supports_face_attributes_p: whether the requested
/// attributes both render differently from the default face and lie
/// within the terminal's capabilities.  The capability set mirrors GNU
/// on the frontend's terminal class (TERM=xterm family): bold, italic,
/// inverse video, and strike-through pass; dim and underline fail the
/// capability test there; family, foundry, stipple, height, width,
/// overline, and box never exist on a tty.  Color names must resolve
/// on the terminal's palette and differ from the default face's.
fn tty_supports_face_attributes(
    interp: &mut Interpreter,
    env: &mut Env,
    attributes: &Value,
) -> Result<Value, LispError> {
    let mut pairs: Vec<(String, Value)> = Vec::new();
    // merge_face_ref accepts both a bare plist and a precedence-ordered
    // list of face references.  `supports' display conditions pass their
    // cdr verbatim, so the widespread `(supports (:box t))' spelling
    // arrives here as the one-element face-reference list `((:box t))'.
    // Walk those lists just as merge_face_ref does instead of mistaking an
    // empty top-level plist for "every requested attribute is supported".
    let mut pending = vec![*attributes];
    while let Some(reference) = pending.pop() {
        let Ok(items) = reference.to_vec() else {
            continue;
        };
        if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name.starts_with(':'))
        {
            pending.extend(items.into_iter().rev());
            continue;
        }
        for pair in items.as_chunks::<2>().0 {
            if let Kind::Symbol(key) = pair[0].kind() {
                pairs.push((key.to_string(), pair[1]));
            }
        }
    }
    let lookup = |wanted: &str| {
        pairs
            .iter()
            .find(|(key, _)| key == wanted)
            .map(|(_, value)| *value)
    };
    for (key, _) in &pairs {
        if matches!(
            key.as_str(),
            ":family" | ":foundry" | ":stipple" | ":height" | ":width" | ":overline" | ":box"
        ) {
            return Ok(Value::Nil);
        }
    }
    if let Some(weight) = lookup(":weight") {
        // FONT_WEIGHT_NAME_NUMERIC: heavier than normal needs bold
        // (supported), lighter needs dim (not on this terminal), and
        // normal weights are the default's own.
        match weight.as_symbol().unwrap_or("") {
            "semi-bold" | "bold" | "extra-bold" | "ultra-bold" | "heavy" | "ultra-heavy"
            | "black" => {}
            "thin" | "ultra-light" | "extra-light" | "light" | "semi-light" => {
                return Ok(Value::Nil);
            }
            "normal" | "medium" | "regular" | "book" => return Ok(Value::Nil),
            _ => {}
        }
    }
    if let Some(slant) = lookup(":slant") {
        // Italic and oblique differ from the default's normal slant and
        // the terminal renders italics.
        if matches!(slant.as_symbol().unwrap_or(""), "normal") {
            return Ok(Value::Nil);
        }
    }
    if lookup(":underline").is_some() {
        // The capability test fails for underline on this terminal
        // class, styled variants included.
        return Ok(Value::Nil);
    }
    for color_key in [":foreground", ":background"] {
        if let Some(color) = lookup(color_key) {
            if !color.is_string() {
                return Ok(Value::Nil);
            }
            let default_color = interp
                .call_function_value(
                    Value::Symbol("face-attribute".into()),
                    Some("face-attribute"),
                    &[
                        Value::Symbol("default".into()),
                        Value::Symbol(color_key.into()),
                    ],
                    env,
                )
                .ok();
            // face_attr_equal_p compares color strings case-insensitively.
            let text_of = |value: &Value| -> Option<String> {
                match value.kind() {
                    Kind::StringObject(state) => Some(state.borrow().text()),
                    _ => None,
                }
            };
            if let (Some(default_text), Some(color_text)) =
                (default_color.as_ref().and_then(text_of), text_of(&color))
                && default_text.eq_ignore_ascii_case(&color_text)
            {
                return Ok(Value::Nil);
            }
            let index = interp
                .call_function_value(
                    Value::Symbol("tty-color-translate".into()),
                    None,
                    std::slice::from_ref(&color),
                    env,
                )?
                .as_integer()
                .unwrap_or(-1);
            if index < 0 {
                return Ok(Value::Nil);
            }
        }
    }
    Ok(Value::T)
}

/// Resolve FACE to its tty attributes through the runtime's own face
/// machinery: `face-attribute' answers the realized (inherit-merged)
/// attributes, and `tty-color-translate' maps color names onto the
/// terminal's ANSI palette exactly as GNU's tty color support does.
pub(crate) fn resolve_tty_face_attrs(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
) -> TtyFaceAttrs {
    // face-remap.el installs buffer-local substitutions such as Magit's
    // `(header-line magit-header-line header-line)'.  Redisplay realizes
    // the cdr as the effective precedence-ordered face list; the trailing
    // base face is deliberately resolved without recursively remapping it.
    if let Kind::Symbol(requested) = face.kind()
        && let Some(remapped) = interp
            .lookup_var("face-remapping-alist", env)
            .and_then(|alist| alist.to_vec().ok())
            .and_then(|entries| {
                entries.into_iter().find_map(|entry| {
                    let key = entry.car_if_list()?;
                    matches!(key.kind(), Kind::Symbol(name) if name == requested)
                        .then(|| entry.cdr_if_list())?
                })
            })
    {
        let options = resolve_tty_face_reference_options(interp, env, &remapped, 0);
        return TtyFaceAttrs {
            foreground: options.foreground,
            background: options.background,
            bold: options.bold.unwrap_or(false),
            underline: options.underline.unwrap_or(false),
            reverse: options.reverse.unwrap_or(false),
            extend: options.extend.unwrap_or(false),
        };
    }
    // xfaces.c merge_face_ref: a face reference may be a LIST of face
    // references, merged left to right with the entries nearer the
    // front taking precedence (comint's prompt carries
    // `(comint-highlight-prompt comint-highlight-prompt)').  Realize
    // each member and fold, letting an earlier member's set attributes
    // override a later one's.
    if matches!(face.kind(), Kind::Cons(_)) {
        let options = resolve_tty_face_reference_options(interp, env, face, 0);
        return TtyFaceAttrs {
            foreground: options.foreground,
            background: options.background,
            bold: options.bold.unwrap_or(false),
            underline: options.underline.unwrap_or(false),
            reverse: options.reverse.unwrap_or(false),
            extend: options.extend.unwrap_or(false),
        };
    }
    // `face-attribute' is GNU faces.el's; reach it through the ordinary
    // function cell, never the native dispatcher.
    let attribute = |interp: &mut Interpreter, env: &mut Env, name: &str| {
        interp
            .call_function_value(
                Value::Symbol("face-attribute".into()),
                Some("face-attribute"),
                &[*face, Value::Symbol(name.into()), Value::Nil, Value::T],
                env,
            )
            .ok()
            .filter(|value| !matches!(value.kind(), Kind::Symbol(s) if s == "unspecified"))
            .filter(|value| !value.is_nil())
    };
    let color_index = |interp: &mut Interpreter, env: &mut Env, value: Option<Value>| {
        tty_face_color_index(interp, env, value.as_ref()?)
    };
    let foreground = attribute(interp, env, ":foreground");
    let background = attribute(interp, env, ":background");
    TtyFaceAttrs {
        foreground: color_index(interp, env, foreground),
        background: color_index(interp, env, background),
        bold: attribute(interp, env, ":weight")
            .is_some_and(|weight| matches!(weight.kind(), Kind::Symbol(s) if s == "bold" || s == "semi-bold" || s == "extra-bold" || s == "ultra-bold")),
        underline: attribute(interp, env, ":underline").is_some_and(|value| value.is_truthy()),
        reverse: attribute(interp, env, ":inverse-video").is_some_and(|value| value.is_truthy()),
        extend: attribute(interp, env, ":extend").is_some_and(|value| value.is_truthy()),
    }
}

/// One face member's tty attributes with per-attribute specificity: a
/// `None' means the face leaves that attribute unspecified (inherit
/// merged), a `Some' is an explicit value — explicit nil included, so
/// a list merge can turn attributes off.
#[derive(Default)]
pub(crate) struct TtyFaceAttrOptions {
    pub(crate) foreground: Option<u8>,
    pub(crate) background: Option<u8>,
    pub(crate) bold: Option<bool>,
    pub(crate) underline: Option<bool>,
    pub(crate) reverse: Option<bool>,
    pub(crate) extend: Option<bool>,
}

fn merge_tty_face_options(base: &mut TtyFaceAttrOptions, overlay: TtyFaceAttrOptions) {
    if overlay.foreground.is_some() {
        base.foreground = overlay.foreground;
    }
    if overlay.background.is_some() {
        base.background = overlay.background;
    }
    if overlay.bold.is_some() {
        base.bold = overlay.bold;
    }
    if overlay.underline.is_some() {
        base.underline = overlay.underline;
    }
    if overlay.reverse.is_some() {
        base.reverse = overlay.reverse;
    }
    if overlay.extend.is_some() {
        base.extend = overlay.extend;
    }
}

/// Translate one realized tty face color through tty-colors.el's registered
/// palette and approximation machinery.
fn tty_face_color_index(interp: &mut Interpreter, env: &mut Env, value: &Value) -> Option<u8> {
    string_text(value).ok()?;
    interp
        .call_function_value(
            Value::Symbol("tty-color-translate".into()),
            None,
            std::slice::from_ref(value),
            env,
        )
        .ok()
        .and_then(|index| index.integer_value())
        .and_then(|index| u8::try_from(index).ok())
}

/// Resolve the named, precedence-ordered list, and anonymous attribute
/// plist face-reference forms used by the TTY renderer.  Propertized
/// strings use the last form heavily (Flymake wraps its compilation face
/// in `(:inherit ((FACE) default))').
fn resolve_tty_face_reference_options(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
    depth: usize,
) -> TtyFaceAttrOptions {
    if depth >= 32 {
        return TtyFaceAttrOptions::default();
    }
    let Kind::Cons(_) = face.kind() else {
        return resolve_tty_face_attr_options(interp, env, face);
    };
    let Ok(items) = face.to_vec() else {
        return TtyFaceAttrOptions::default();
    };
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name.starts_with(':'))
    {
        let mut merged = TtyFaceAttrOptions::default();
        for item in items.iter().rev() {
            let options = resolve_tty_face_reference_options(interp, env, item, depth + 1);
            merge_tty_face_options(&mut merged, options);
        }
        return merged;
    }

    let pairs = items.as_chunks::<2>().0;
    let inherited = pairs
        .iter()
        .find(|pair| matches!(pair[0].kind(), Kind::Symbol(name) if name == ":inherit"))
        .map(|pair| &pair[1])
        .filter(|value| value.is_truthy());
    let mut merged = inherited
        .map(|value| resolve_tty_face_reference_options(interp, env, value, depth + 1))
        .unwrap_or_default();
    let color_index = tty_face_color_index;
    for pair in pairs {
        let Kind::Symbol(name) = pair[0].kind() else {
            continue;
        };
        let value = &pair[1];
        if matches!(value.kind(), Kind::Symbol(name) if name == "unspecified") {
            continue;
        }
        match name.as_ref() {
            ":foreground" => merged.foreground = color_index(interp, env, value),
            ":background" => merged.background = color_index(interp, env, value),
            ":weight" => {
                merged.bold = Some(matches!(value.kind(), Kind::Symbol(weight)
                    if weight == "bold"
                        || weight == "semi-bold"
                        || weight == "extra-bold"
                        || weight == "ultra-bold"));
            }
            ":underline" => merged.underline = Some(value.is_truthy()),
            ":inverse-video" => merged.reverse = Some(value.is_truthy()),
            ":extend" => merged.extend = Some(value.is_truthy()),
            _ => {}
        }
    }
    merged
}

fn resolve_tty_face_attr_options(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
) -> TtyFaceAttrOptions {
    let attribute = |interp: &mut Interpreter, env: &mut Env, name: &str| {
        interp
            .call_function_value(
                Value::Symbol("face-attribute".into()),
                Some("face-attribute"),
                &[*face, Value::Symbol(name.into()), Value::Nil, Value::T],
                env,
            )
            .ok()
            .filter(|value| !matches!(value.kind(), Kind::Symbol(s) if s == "unspecified"))
    };
    let color_index = |interp: &mut Interpreter, env: &mut Env, value: Option<Value>| {
        tty_face_color_index(interp, env, value.as_ref()?)
    };
    let foreground = attribute(interp, env, ":foreground").filter(|value| !value.is_nil());
    let background = attribute(interp, env, ":background").filter(|value| !value.is_nil());
    TtyFaceAttrOptions {
        foreground: color_index(interp, env, foreground),
        background: color_index(interp, env, background),
        bold: attribute(interp, env, ":weight").map(|weight| {
            matches!(weight.kind(), Kind::Symbol(ref s)
                if s == "bold" || s == "semi-bold" || s == "extra-bold" || s == "ultra-bold")
        }),
        underline: attribute(interp, env, ":underline").map(|value| value.is_truthy()),
        reverse: attribute(interp, env, ":inverse-video").map(|value| value.is_truthy()),
        extend: attribute(interp, env, ":extend").map(|value| value.is_truthy()),
    }
}

/// The face spans redisplay paints over a window's text in [FROM, TO):
/// buffer `face' text properties first, the selected window's active
/// region above them, and overlay faces last in ascending priority —
/// the stacking GNU's face merging produces (isearch's priority-1001
/// overlay wins over the region, which wins over font-lock properties).
pub(crate) fn window_face_spans(
    interp: &mut Interpreter,
    env: &mut Env,
    buffer_id: u64,
    from: usize,
    to: usize,
    region_for_selected: bool,
) -> Vec<(usize, usize, Value)> {
    let mut spans = Vec::new();
    let is_current = buffer_id == interp.current_buffer_id();
    let mut overlay_spans: Vec<(i64, usize, usize, usize, Value)> = Vec::new();
    {
        let buffer = if is_current {
            interp.buffer.borrow()
        } else {
            match interp.get_buffer_by_id(buffer_id) {
                Some(buffer) => buffer,
                None => return spans,
            }
        };
        // font-core.el:165: font-lock-mode installs `font-lock-face' as
        // an alias of `face' in the buffer-local
        // `char-property-alias-alist'; the display engine resolves the
        // face for each position through the same alias chain textget
        // uses, which is how comint's font-lock-face-only output reaches
        // the glass.
        let face_alias_names: Vec<String> = interp
            .buffer_local_value(buffer_id, "char-property-alias-alist")
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                let key = entry.car_if_list()?;
                matches!(key.kind(), Kind::Symbol(name) if name == "face")
                    .then(|| entry.cdr_if_list()?.to_vec().ok())?
            })
            .flatten()
            .filter_map(|alias| alias.symbol_str().map(str::to_string))
            .collect();
        // The common frame — a plain buffer, no overlays, no active
        // region — must not pay a per-character property walk on every
        // redisplay.
        if !buffer.has_text_property_named("face")
            && !buffer.has_text_property_named("category")
            && face_alias_names
                .iter()
                .all(|name| !buffer.has_text_property_named(name))
            && buffer.overlays.iter().all(|overlay| overlay.is_dead())
            && !(region_for_selected
                && is_current
                && interp
                    .lookup_var("mark-active", env)
                    .is_some_and(|active| active.is_truthy()))
        {
            return spans;
        }
        // Redisplay reads a property whose lookup signals as nil.
        let face_at = |pos: usize| {
            crate::lisp::primitives::strings::buffer_property_at_with_category(
                interp, &buffer, pos, "face",
            )
            .ok()
            .flatten()
            .filter(|face| !face.is_nil())
            .or_else(|| {
                face_alias_names.iter().find_map(|name| {
                    crate::lisp::primitives::strings::buffer_property_at_with_category(
                        interp, &buffer, pos, name,
                    )
                    .ok()
                    .flatten()
                    .filter(|face| !face.is_nil())
                })
            })
        };
        let mut pos = from;
        while pos < to {
            let face = face_at(pos);
            let mut end = pos + 1;
            while end < to && face_at(end) == face {
                end += 1;
            }
            if let Some(face) = face {
                spans.push((pos, end, face));
            }
            pos = end;
        }
        for overlay in &buffer.overlays {
            if overlay.is_dead() || overlay.beg() >= to || overlay.end() <= from {
                continue;
            }
            let Some(face) = crate::lisp::primitives::strings::overlay_property_with_category(
                interp, &overlay, "face",
            )
            .ok()
            .flatten()
            .filter(|face| !face.is_nil())
            .or_else(|| {
                face_alias_names.iter().find_map(|name| {
                    crate::lisp::primitives::strings::overlay_property_with_category(
                        interp, &overlay, name,
                    )
                    .ok()
                    .flatten()
                    .filter(|face| !face.is_nil())
                })
            }) else {
                continue;
            };
            let priority = crate::lisp::primitives::strings::overlay_property_with_category(
                interp, &overlay, "priority",
            )
            .ok()
            .flatten()
            .and_then(|priority| priority.integer_value())
            .unwrap_or(0);
            overlay_spans.push((
                priority,
                overlay.identity(),
                overlay.beg().max(from),
                overlay.end().min(to),
                face,
            ));
        }
    }
    // The active region, redisplay's own quasi-overlay under real ones.
    if region_for_selected
        && is_current
        && interp
            .lookup_var("transient-mark-mode", env)
            .is_some_and(|mode| mode.is_truthy())
        && interp
            .lookup_var("mark-active", env)
            .is_some_and(|active| active.is_truthy())
        // The buffer's mark marker is native buffer.c state; `mark' itself
        // is GNU simple.el's and must not enter the native dispatcher.
        && let Some(mark) = interp.buffer.borrow().mark()
    {
        let point = interp.buffer.borrow().point();
        let mark = mark.max(1);
        let (beg, end) = if mark <= point {
            (mark, point)
        } else {
            (point, mark)
        };
        let beg = beg.max(from);
        let end = end.min(to);
        if beg < end {
            spans.push((beg, end, Value::Symbol("region".into())));
        }
    }
    overlay_spans.sort_by_key(|(priority, id, ..)| (*priority, *id));
    spans.extend(
        overlay_spans
            .into_iter()
            .map(|(_, _, beg, end, face)| (beg, end, face)),
    );
    spans
}

/// A window's mode line as the display engine paints it, WIDTH cells
/// wide.  GNU renders each mode line with that window selected and its
/// buffer current (display_mode_line's context); this swaps both in —
/// plus the window's point and live metrics — and restores every piece
/// afterwards.
pub(crate) fn render_window_mode_line(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    point: usize,
    metrics: InteractiveWindowMetrics,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(
        interp,
        env,
        window_id,
        point,
        metrics,
        "mode-line-format",
        width,
        cells,
    )
}

/// The window's header line, rendered by the same machinery as its mode
/// line (xdisp.c's display_mode_lines drives both through
/// display_mode_line with the window's `header-line-format').
pub(crate) fn render_window_header_line(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    point: usize,
    metrics: InteractiveWindowMetrics,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(
        interp,
        env,
        window_id,
        point,
        metrics,
        "header-line-format",
        width,
        cells,
    )
}

/// The window's tab line, rendered above its header and text through the
/// same display-mode-line machinery as GNU's `tab-line-format'.
pub(crate) fn render_window_tab_line(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    point: usize,
    metrics: InteractiveWindowMetrics,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(
        interp,
        env,
        window_id,
        point,
        metrics,
        "tab-line-format",
        width,
        cells,
    )
}

#[allow(clippy::too_many_arguments)]
fn render_window_line_with_format(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    point: usize,
    metrics: InteractiveWindowMetrics,
    format_variable: &str,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    let saved_window = interp.selected_window_id();
    let saved_buffer = interp.current_buffer_id();
    let saved_metrics = interactive_window_metrics();
    let buffer_id =
        window_buffer_id(interp, &interp.record_value(window_id)).unwrap_or(saved_buffer);
    interp.set_selected_window_id(window_id);
    let switched = buffer_id != saved_buffer && interp.set_current_buffer_id(buffer_id).is_ok();
    let saved_point = interp.buffer.borrow().point();
    interp.buffer.borrow_mut().goto_char(point);
    set_interactive_window_metrics(Some(metrics));
    mode_line::record_window_end(interp, window_id, metrics.window_end);
    let result = {
        // display_mode_lines: the window's parameter, else the buffer's
        // format.
        let parameter = window_parameter_value(interp, window_id, &Value::symbol(format_variable));
        let format = if parameter.is_nil() {
            interp
                .lookup_var(format_variable, &Env::new())
                .unwrap_or(Value::Nil)
        } else {
            parameter
        };
        mode_line::display_mode_line(
            interp,
            env,
            window_id,
            format,
            width,
            format_variable == "mode-line-format",
            cells,
        )
    };
    interp.buffer.borrow_mut().goto_char(saved_point);
    // Restore selection before switching the current buffer back.  While a
    // non-selected window's mode line is rendered we temporarily select it;
    // switching away with that temporary selection still installed makes
    // `set_current_buffer_id' save the buffer's live point into the window's
    // independent point slot, corrupting the slot as a redisplay side effect.
    interp.set_selected_window_id(saved_window);
    if switched {
        let _ = interp.set_current_buffer_id(saved_buffer);
    }
    set_interactive_window_metrics(saved_metrics);
    result
}

/// The selected window's mode line on a row of WIDTH cells of one column
/// a character: the glass the frontend paints, for tests.
#[cfg(test)]
pub(crate) fn render_mode_line_glass(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<String, LispError> {
    struct UnitCells;
    impl ModeLineCells for UnitCells {
        fn widths(&self, _: &Interpreter, text: &[char]) -> Vec<usize> {
            vec![1; text.len()]
        }
        fn stretch(&self, _: &Interpreter, plist: &[Value], x: usize) -> usize {
            plist
                .chunks(2)
                .find_map(
                    |pair| match (pair[0].kind(), pair.get(1).map(|v| v.kind())) {
                        (Kind::Symbol(key), Some(Kind::Integer(target))) if key == ":align-to" => {
                            Some((target.max(0) as usize).saturating_sub(x))
                        }
                        (Kind::Symbol(key), Some(Kind::Integer(width))) if key == ":width" => {
                            Some(width.max(0) as usize)
                        }
                        _ => None,
                    },
                )
                .unwrap_or(1)
        }
        fn width_of(&self, _: &Interpreter, spec: &Value) -> Option<f64> {
            spec.as_float().ok()
        }
    }
    let window_id = interp.selected_window_id();
    let format = interp
        .lookup_var("mode-line-format", &Env::new())
        .unwrap_or(Value::Nil);
    let (text, _) =
        mode_line::display_mode_line(interp, env, window_id, format, 200, true, &UnitCells)?;
    Ok(text.trim_end().to_string())
}

fn live_window_id_or_selected(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<u64, LispError> {
    let window = value
        .filter(|window| !window.is_nil())
        .cloned()
        .unwrap_or_else(|| interp.selected_window_value());
    // WINDOW_LIVE_P of the record: a leaf showing a live buffer.
    window_record(&window)
        .filter(|record| {
            !matches!(
                record.slots.get(WINDOW_KIND_SLOT).map(|slot| slot.kind()),
                Some(Kind::Symbol(kind))
                    if matches!(
                        kind.as_str(),
                        INTERNAL_HORIZONTAL_WINDOW_KIND
                            | INTERNAL_VERTICAL_WINDOW_KIND
                            | DELETED_WINDOW_KIND
                    )
            ) && window_buffer_id(interp, &window).is_some()
        })
        .map(|record| record.id)
        .ok_or_else(|| LispError::WrongTypeArgument("window-live-p".into(), window))
}

fn select_window_value(
    interp: &mut Interpreter,
    window: &Value,
    norecord: bool,
) -> Result<Value, LispError> {
    let window_id = live_window_id_or_selected(interp, Some(window))?;
    let previous_window_id = interp.selected_window_id();
    if window_id != previous_window_id {
        // select_window marks both windows for redisplay, the newly
        // selected one while it is still another window.
        interp.redisplay_other_windows();
        let previous_buffer_id = interp.selected_window_buffer_id();
        let previous_point = if previous_buffer_id == interp.current_buffer_id() {
            interp.buffer.borrow().point()
        } else {
            interp
                .get_buffer_by_id(previous_buffer_id)
                .map(|buffer| buffer.point())
                .unwrap_or(1)
        };
        set_window_slot_value(
            interp,
            previous_window_id,
            WINDOW_POINT_SLOT,
            Value::Integer(previous_point as i64),
        )?;
    }
    // The selected window's live point is the point of its current buffer;
    // its saved window-point slot is only authoritative while unselected.
    // In particular, reselecting the already-selected window must not rewind
    // point to a stale slot (GNU's `save-selected-window' relies on this).
    let target_buffer_id = window_buffer_id(interp, window);
    let target_point = if window_id == previous_window_id
        && target_buffer_id == Some(interp.current_buffer_id())
    {
        interp.buffer.borrow().point()
    } else {
        window_slot_value(interp, window_id, WINDOW_POINT_SLOT)
            .as_integer()
            .unwrap_or(1)
            .max(1) as usize
    };
    if let Some(frame) = interp.window_frame_id(window_id) {
        interp.note_selected_frame(frame);
    }
    interp.set_selected_window_id(window_id);
    if !norecord {
        interp.record_window_selection(window_id);
    }
    if let Some(buffer_id) = target_buffer_id
        && interp.has_buffer_id(buffer_id)
    {
        interp.switch_to_buffer_id_preserving_window_history(buffer_id)?;
        interp.buffer.borrow_mut().goto_char(target_point);
        let point = interp.buffer.borrow().point() as i64;
        set_window_slot_value(interp, window_id, WINDOW_POINT_SLOT, Value::Integer(point))?;
        if !norecord {
            interp.record_buffer_front(buffer_id);
        }
    }
    Ok(*window)
}

/// window.c's decode_valid_window: a live or internal window, nil being
/// the selected one.
fn valid_window_id(interp: &Interpreter, value: &Value) -> Result<u64, LispError> {
    let id = window_id_or_selected(interp, value)
        .map_err(|_| wrong_type_argument("window-valid-p", *value))?;
    if window_is_deleted(interp, id) {
        return Err(wrong_type_argument("window-valid-p", *value));
    }
    Ok(id)
}

fn window_is_deleted(interp: &Interpreter, id: u64) -> bool {
    matches!(
        window_slot_value(interp, id, WINDOW_KIND_SLOT).kind(),
        Kind::Symbol(ref kind) if kind == DELETED_WINDOW_KIND
    )
}

fn window_is_minibuffer(interp: &Interpreter, id: u64) -> bool {
    matches!(
        window_slot_value(interp, id, WINDOW_KIND_SLOT).kind(),
        Kind::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND
    )
}

/// WINDOW_HORIZONTAL_COMBINATION_P / WINDOW_VERTICAL_COMBINATION_P: the
/// combination's direction for an internal window, None for a leaf.
fn window_combination(interp: &Interpreter, id: u64) -> Option<bool> {
    match window_slot_value(interp, id, WINDOW_KIND_SLOT).kind() {
        Kind::Symbol(ref kind) if kind == INTERNAL_HORIZONTAL_WINDOW_KIND => Some(true),
        Kind::Symbol(ref kind) if kind == INTERNAL_VERTICAL_WINDOW_KIND => Some(false),
        _ => None,
    }
}

fn window_pixel_size(interp: &Interpreter, id: u64, horflag: bool) -> i64 {
    let (width, height, _, _) = window_geometry(interp, id);
    if horflag { width } else { height }
}

fn window_new_pixel(interp: &Interpreter, id: u64) -> i64 {
    window_slot_value(interp, id, WINDOW_NEW_PIXEL_SLOT)
        .as_integer()
        .unwrap_or(0)
}

fn set_window_link(
    interp: &mut Interpreter,
    id: u64,
    slot: usize,
    target: Option<u64>,
) -> Result<(), LispError> {
    let value = target.map_or(Value::Nil, |target| interp.record_value(target));
    set_window_slot_value(interp, id, slot, value)?;
    Ok(())
}

/// wset_combination (W, HORFLAG, CHILD): make W an internal window of
/// HORFLAG's direction whose first child is CHILD, or (CHILD nil) leave
/// it without contents, which marks a deleted window.
fn set_window_combination(
    interp: &mut Interpreter,
    id: u64,
    horflag: bool,
    child: Option<u64>,
) -> Result<(), LispError> {
    let kind = match child {
        Some(_) if horflag => INTERNAL_HORIZONTAL_WINDOW_KIND,
        Some(_) => INTERNAL_VERTICAL_WINDOW_KIND,
        None => DELETED_WINDOW_KIND,
    };
    set_window_slot_value(interp, id, WINDOW_KIND_SLOT, Value::Symbol(kind.into()))?;
    set_window_link(interp, id, WINDOW_FIRST_CHILD_SLOT, child)
}

/// A window without contents keeps no links (GNU leaves them dangling
/// and refuses every accessor on a deleted window).
fn clear_deleted_window(interp: &mut Interpreter, id: u64) -> Result<(), LispError> {
    set_window_combination(interp, id, false, None)?;
    set_window_slot_value(interp, id, WINDOW_BUFFER_SLOT, Value::Nil)?;
    for slot in [
        WINDOW_PARENT_SLOT,
        WINDOW_PREV_SIBLING_SLOT,
        WINDOW_NEXT_SIBLING_SLOT,
    ] {
        set_window_slot_value(interp, id, slot, Value::Nil)?;
    }
    Ok(())
}

/// window.c:window_resize_check: whether the new pixel sizes staged in
/// W's subtree add up (an iso-combination's children sum to W's, an
/// ortho-combination's children each equal W's), and no leaf gets
/// smaller than `window-safe-min-width' (2) or `-height' (1).
fn window_resize_check(interp: &Interpreter, id: u64, horflag: bool) -> bool {
    let new_pixel = window_new_pixel(interp, id);
    let Some(horizontal) = window_combination(interp, id) else {
        return new_pixel >= if horflag { 2 } else { 1 };
    };
    let mut child = window_link(interp, id, WINDOW_FIRST_CHILD_SLOT);
    if horizontal == horflag {
        let mut remaining = new_pixel;
        while let Some(child_id) = child {
            if !window_resize_check(interp, child_id, horflag) {
                return false;
            }
            remaining -= window_new_pixel(interp, child_id);
            if remaining < 0 {
                return false;
            }
            child = window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT);
        }
        remaining == 0
    } else {
        while let Some(child_id) = child {
            if window_new_pixel(interp, child_id) != new_pixel
                || !window_resize_check(interp, child_id, horflag)
            {
                return false;
            }
            child = window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT);
        }
        true
    }
}

/// window.c:window_resize_apply: commit W's staged new pixel size (and
/// a numeric new normal size) in HORFLAG's dimension, its total size
/// from that, then place its children (pixel edge and column or line)
/// from W's edge, advancing the edge across an iso-combination.
fn window_resize_apply(interp: &mut Interpreter, id: u64, horflag: bool) -> Result<(), LispError> {
    let new_pixel = window_new_pixel(interp, id);
    let (mut width, mut height, left, top) = window_geometry(interp, id);
    if horflag {
        width = new_pixel;
    } else {
        height = new_pixel;
    }
    set_window_geometry(interp, id, (width, height, left, top))?;
    set_window_char_slot(
        interp,
        id,
        if horflag {
            WINDOW_TOTAL_COLS_SLOT
        } else {
            WINDOW_TOTAL_LINES_SLOT
        },
        new_pixel,
    );
    let new_normal = window_slot_value(interp, id, WINDOW_NEW_NORMAL_SLOT);
    if matches!(new_normal.kind(), Kind::Integer(_) | Kind::Float(_)) {
        let slot = if horflag {
            WINDOW_NORMAL_WIDTH_SLOT
        } else {
            WINDOW_NORMAL_HEIGHT_SLOT
        };
        set_window_slot_value(interp, id, slot, new_normal)?;
    }
    let Some(horizontal) = window_combination(interp, id) else {
        return Ok(());
    };
    let mut edge = if horflag { left } else { top };
    let mut child = window_link(interp, id, WINDOW_FIRST_CHILD_SLOT);
    while let Some(child_id) = child {
        let (child_width, child_height, child_left, child_top) = window_geometry(interp, child_id);
        let placed = if horflag {
            (child_width, child_height, edge, child_top)
        } else {
            (child_width, child_height, child_left, edge)
        };
        set_window_geometry(interp, child_id, placed)?;
        set_window_char_slot(
            interp,
            child_id,
            if horflag {
                WINDOW_LEFT_COL_SLOT
            } else {
                WINDOW_TOP_LINE_SLOT
            },
            edge,
        );
        window_resize_apply(interp, child_id, horflag)?;
        if horizontal == horflag {
            edge += window_pixel_size(interp, child_id, horflag);
        }
        child = window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT);
    }
    Ok(())
}

/// window.c:grow_mini_window's commit: once `window--resize-root-window-
/// vertically' has staged the root's new height, recovering GROW pixels
/// for the root, the mini window takes -GROW when the staged sizes add
/// up (resize_mini_window_apply runs window_resize_apply on the root
/// whatever its current height).
pub(crate) fn apply_staged_root_window_height(
    interp: &mut Interpreter,
    grow: i64,
) -> Result<bool, LispError> {
    let frame = interp.selected_frame_id;
    let root = frame.borrow().root_window_id();
    if grow == 0 || !window_resize_check(interp, root, false) {
        return Ok(false);
    }
    let mini = frame.borrow().minibuffer_window_id();
    resize_mini_window_apply(interp, frame, mini, -grow)?;
    Ok(true)
}

/// window.c:resize_mini_window_apply: mini window W grows by DELTA
/// pixels, the root window tree takes its staged heights and W stays
/// right below it.
fn resize_mini_window_apply(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    mini: u64,
    delta: i64,
) -> Result<(), LispError> {
    let root = frame.borrow().root_window_id();
    let (width, height, left, top) = window_geometry(interp, mini);
    set_window_geometry(interp, mini, (width, height + delta, left, top))?;
    set_window_char_slot(interp, mini, WINDOW_TOTAL_LINES_SLOT, height + delta);
    window_resize_apply(interp, root, false)?;
    let (_, root_height, _, root_top) = window_geometry(interp, root);
    let (_, root_lines, _, root_top_line) = window_char_geometry(interp, root);
    set_window_geometry(
        interp,
        mini,
        (width, height + delta, left, root_top + root_height),
    )?;
    set_window_char_slot(
        interp,
        mini,
        WINDOW_TOP_LINE_SLOT,
        root_top_line + root_lines,
    );
    // fset_redisplay (f).
    interp.redisplay_other_windows();
    Ok(())
}

/// window.c:replace_window: NEW takes OLD's place among its siblings and
/// under its parent (and as its frame's root); with SETFLAG, also OLD's
/// edges and normal sizes, OLD's normal sizes reverting to 1.0.
fn replace_window(
    interp: &mut Interpreter,
    old: u64,
    new: u64,
    setflag: bool,
) -> Result<(), LispError> {
    let frame = interp
        .window_frame_id(old)
        .expect("window stores its owning frame");
    if frame.borrow().root_window_id() == old {
        frame.borrow_mut().root_window = interp.record_ref(new);
    }
    if setflag {
        set_window_geometry(interp, new, window_geometry(interp, old))?;
        let (total_cols, total_lines, left_col, top_line) = window_char_geometry(interp, old);
        for (slot, value) in [
            (WINDOW_LEFT_COL_SLOT, left_col),
            (WINDOW_TOP_LINE_SLOT, top_line),
            (WINDOW_TOTAL_COLS_SLOT, total_cols),
            (WINDOW_TOTAL_LINES_SLOT, total_lines),
        ] {
            set_window_char_slot(interp, new, slot, value);
        }
        for slot in [WINDOW_NORMAL_WIDTH_SLOT, WINDOW_NORMAL_HEIGHT_SLOT] {
            let normal = window_slot_value(interp, old, slot);
            set_window_slot_value(interp, new, slot, normal)?;
            set_window_slot_value(interp, old, slot, Value::float(1.0))?;
        }
    }
    let next = window_link(interp, old, WINDOW_NEXT_SIBLING_SLOT);
    set_window_link(interp, new, WINDOW_NEXT_SIBLING_SLOT, next)?;
    if let Some(next) = next {
        set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, Some(new))?;
    }
    let prev = window_link(interp, old, WINDOW_PREV_SIBLING_SLOT);
    set_window_link(interp, new, WINDOW_PREV_SIBLING_SLOT, prev)?;
    if let Some(prev) = prev {
        set_window_link(interp, prev, WINDOW_NEXT_SIBLING_SLOT, Some(new))?;
    }
    let parent = window_link(interp, old, WINDOW_PARENT_SLOT);
    set_window_link(interp, new, WINDOW_PARENT_SLOT, parent)?;
    if let Some(parent) = parent
        && window_link(interp, parent, WINDOW_FIRST_CHILD_SLOT) == Some(old)
    {
        set_window_link(interp, parent, WINDOW_FIRST_CHILD_SLOT, Some(new))?;
    }
    Ok(())
}

/// window.c:make_parent_window: a copy of WINDOW (window.c memcpy's the
/// whole struct) replaces it in the tree as an internal window of
/// HORFLAG's direction whose only child is WINDOW.
fn make_parent_window(
    interp: &mut Interpreter,
    window: u64,
    horflag: bool,
) -> Result<u64, LispError> {
    let slots = interp
        .find_record(window)
        .map(|record| record.slots.clone())
        .unwrap_or_default();
    let parent = interp.create_pseudovector(crate::lisp::eval::RecordKind::Window, "window", slots);
    let Kind::Record(parent) = parent.kind() else {
        unreachable!("window records use Value::Record");
    };
    let parent = parent.id;
    replace_window(interp, window, parent, true)?;
    set_window_link(interp, window, WINDOW_NEXT_SIBLING_SLOT, None)?;
    set_window_link(interp, window, WINDOW_PREV_SIBLING_SLOT, None)?;
    set_window_link(interp, window, WINDOW_PARENT_SLOT, Some(parent))?;
    for slot in [
        WINDOW_START_SLOT,
        WINDOW_OLD_POINT_SLOT,
        WINDOW_POINT_SLOT,
        WINDOW_BUFFER_SLOT,
        WINDOW_COMBINATION_LIMIT_SLOT,
        WINDOW_PREV_BUFFERS_SLOT,
        WINDOW_NEXT_BUFFERS_SLOT,
        WINDOW_PARAMETERS_SLOT,
    ] {
        set_window_slot_value(interp, parent, slot, Value::Nil)?;
    }
    set_window_combination(interp, parent, horflag, Some(window))?;
    Ok(parent)
}

/// window.c:recombine_windows: an internal WINDOW whose parent combines
/// in the same direction (and whose combination limit is nil) hands its
/// children to that parent, each child's normal size becoming its share
/// of the parent, and WINDOW is deleted.
fn recombine_windows(interp: &mut Interpreter, window: u64) -> Result<(), LispError> {
    let Some(parent) = window_link(interp, window, WINDOW_PARENT_SLOT) else {
        return Ok(());
    };
    if window_slot_value(interp, window, WINDOW_COMBINATION_LIMIT_SLOT).is_truthy() {
        return Ok(());
    }
    let (Some(parent_horizontal), Some(horflag)) = (
        window_combination(interp, parent),
        window_combination(interp, window),
    ) else {
        return Ok(());
    };
    if parent_horizontal != horflag {
        return Ok(());
    }
    let first = window_link(interp, window, WINDOW_FIRST_CHILD_SLOT)
        .expect("an internal window has children");
    let prev = window_link(interp, window, WINDOW_PREV_SIBLING_SLOT);
    let next = window_link(interp, window, WINDOW_NEXT_SIBLING_SLOT);
    match prev {
        None => set_window_link(interp, parent, WINDOW_FIRST_CHILD_SLOT, Some(first))?,
        Some(prev) => {
            set_window_link(interp, first, WINDOW_PREV_SIBLING_SLOT, Some(prev))?;
            set_window_link(interp, prev, WINDOW_NEXT_SIBLING_SLOT, Some(first))?;
        }
    }
    let parent_size = window_pixel_size(interp, parent, horflag) as f64;
    let mut child = Some(first);
    while let Some(child_id) = child {
        set_window_link(interp, child_id, WINDOW_PARENT_SLOT, Some(parent))?;
        let normal = window_pixel_size(interp, child_id, horflag) as f64 / parent_size;
        let slot = if horflag {
            WINDOW_NORMAL_WIDTH_SLOT
        } else {
            WINDOW_NORMAL_HEIGHT_SLOT
        };
        set_window_slot_value(interp, child_id, slot, Value::float(normal))?;
        child = window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT);
        if child.is_none()
            && let Some(next) = next
        {
            set_window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT, Some(next))?;
            set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, Some(child_id))?;
        }
    }
    clear_deleted_window(interp, window)
}

/// window.c:unshow_buffer: a window going away leaves its point in its
/// buffer, unless that buffer is the selected window's (whose point is
/// the buffer's own).
fn unshow_buffer(interp: &mut Interpreter, window: u64) {
    let Some(buffer_id) = window_buffer_id(interp, &interp.record_value(window)) else {
        return;
    };
    if buffer_id == interp.selected_window_buffer_id() {
        return;
    }
    let Ok(point) = window_slot_value(interp, window, WINDOW_POINT_SLOT).as_integer() else {
        return;
    };
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        buffer.goto_char(point.max(1) as usize);
    }
}

/// window.c:delete_all_child_windows: delete WINDOW, the siblings after
/// it and all their descendants, children after their later siblings.
fn delete_all_child_windows(interp: &mut Interpreter, window: u64) -> Result<(), LispError> {
    if let Some(next) = window_link(interp, window, WINDOW_NEXT_SIBLING_SLOT) {
        delete_all_child_windows(interp, next)?;
    }
    if let Some(child) = window_link(interp, window, WINDOW_FIRST_CHILD_SLOT) {
        delete_all_child_windows(interp, child)?;
    } else {
        unshow_buffer(interp, window);
    }
    clear_deleted_window(interp, window)
}

/// Fselect_window for a window that is its frame's choice: select it when
/// its frame is the selected frame, else only record it as the frame's
/// selected window.
fn select_window_for_frame(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    window: u64,
    norecord: bool,
) -> Result<(), LispError> {
    if frame == interp.selected_frame_id {
        select_window_value(interp, &interp.record_value(window), norecord)?;
    } else {
        frame.borrow_mut().selected_window = interp.record_ref(window);
    }
    Ok(())
}

/// window.c:Fsplit_window_internal.  `split-window' has staged the new
/// pixel and normal sizes of OLD (and of its siblings when resizing its
/// combination); a new parent is made only when `window-combination-limit'
/// is t, OLD has no parent or OLD's parent combines the other way.
fn split_window_tree(
    interp: &mut Interpreter,
    old: &Value,
    pixel_size: &Value,
    side: &Value,
    normal_size: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let horflag = matches!(side.kind(), Kind::T)
        || matches!(side.kind(), Kind::Symbol(side) if matches!(side.as_str(), "left" | "right"));
    let before =
        matches!(side.kind(), Kind::Symbol(side) if matches!(side.as_str(), "above" | "left"));
    if old.is_nil() || window_record_id_from_value(interp, old).is_none() {
        return Err(wrong_type_argument("windowp", *old));
    }
    let old_id = window_id_or_selected(interp, old)?;
    let frame = interp
        .window_frame_id(old_id)
        .expect("window stores its owning frame");
    let pixel_size = match pixel_size.kind() {
        Kind::Integer(size) => size,
        _ => return Err(wrong_type_argument("fixnump", *pixel_size)),
    };
    let total_size = pixel_size;
    let old_parent = window_link(interp, old_id, WINDOW_PARENT_SLOT);
    let limit_is_t = matches!(
        interp
            .lookup_var("window-combination-limit", env)
            .map(|value| value.kind()),
        Some(Kind::T)
    );
    let combination_limit = limit_is_t
        || match old_parent {
            None => true,
            Some(parent) => window_combination(interp, parent) != Some(horflag),
        };
    let old_live = window_buffer_id(interp, old).is_some() && !window_is_minibuffer(interp, old_id);
    let reference = if old_live {
        old_id
    } else {
        frame.borrow().selected_window_id()
    };
    let resize = interp
        .lookup_var("window-combination-resize", env)
        .is_some_and(|value| value.is_truthy());
    if window_is_minibuffer(interp, old_id) {
        return Err(LispError::Signal(
            "Attempt to split minibuffer window".into(),
        ));
    } else if total_size < if horflag { 2 } else { 1 } {
        return Err(LispError::Signal(
            "Size of new window too small (after split)".into(),
        ));
    } else if !combination_limit && resize {
        let parent = old_parent.expect("a combined window has a parent");
        let parent_size = window_pixel_size(interp, parent, horflag);
        set_window_slot_value(
            interp,
            parent,
            WINDOW_NEW_PIXEL_SLOT,
            Value::Integer(parent_size - pixel_size),
        )?;
        let fits = window_resize_check(interp, parent, horflag);
        if !fits {
            return Err(LispError::Signal("Window sizes don't fit".into()));
        }
        set_window_slot_value(
            interp,
            parent,
            WINDOW_NEW_PIXEL_SLOT,
            Value::Integer(parent_size),
        )?;
    } else if !window_resize_check(interp, old_id, horflag) {
        return Err(LispError::Signal("Resizing old window failed".into()));
    } else if pixel_size + window_new_pixel(interp, old_id)
        != window_pixel_size(interp, old_id, horflag)
    {
        return Err(LispError::Signal(
            "Sum of sizes of old and new window don't fit".into(),
        ));
    }

    let parent = if combination_limit {
        let new_normal_slot = if horflag {
            WINDOW_NORMAL_WIDTH_SLOT
        } else {
            WINDOW_NORMAL_HEIGHT_SLOT
        };
        let new_normal = window_slot_value(interp, old_id, new_normal_slot);
        let parent = make_parent_window(interp, old_id, horflag)?;
        if limit_is_t {
            set_window_slot_value(interp, parent, WINDOW_COMBINATION_LIMIT_SLOT, Value::T)?;
        }
        let size = window_pixel_size(interp, old_id, horflag);
        let (old_cols, old_lines, _, _) = window_char_geometry(interp, old_id);
        let total = if horflag { old_cols } else { old_lines };
        set_window_slot_value(interp, parent, WINDOW_NEW_PIXEL_SLOT, Value::Integer(size))?;
        set_window_slot_value(interp, parent, WINDOW_NEW_TOTAL_SLOT, Value::Integer(total))?;
        set_window_slot_value(interp, parent, WINDOW_NEW_NORMAL_SLOT, new_normal)?;
        parent
    } else {
        old_parent.expect("a combined window has a parent")
    };

    let reference_buffer = window_buffer_id(interp, &interp.record_value(reference))
        .ok_or_else(|| LispError::Signal("Attempt to split a non-live window".into()))?;
    let start = window_slot_value(interp, reference, WINDOW_START_SLOT)
        .as_integer()
        .unwrap_or(1)
        .max(1) as usize;
    let (old_width, old_height, old_left, old_top) = window_geometry(interp, old_id);
    // Orthogonal coordinates and sizes come from OLD; the iso ones are
    // assigned by window_resize_apply below.
    let geometry = if horflag {
        (0, old_height, 0, old_top)
    } else {
        (old_width, 0, old_left, 0)
    };
    let new = interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Window,
        "window",
        window_record_slots(Some(reference_buffer), start, Value::Nil, geometry),
    );
    let Kind::Record(new_id) = new.kind() else {
        unreachable!("window records use Value::Record");
    };
    let new_id = new_id.id;
    set_window_slot_value(interp, new_id, WINDOW_FRAME_SLOT, Value::Frame(frame))?;
    let (old_cols, old_lines, old_left_col, old_top_line) = window_char_geometry(interp, old_id);
    let orthogonal = if horflag {
        [
            (WINDOW_TOP_LINE_SLOT, old_top_line),
            (WINDOW_TOTAL_LINES_SLOT, old_lines),
        ]
    } else {
        [
            (WINDOW_LEFT_COL_SLOT, old_left_col),
            (WINDOW_TOTAL_COLS_SLOT, old_cols),
        ]
    };
    for (slot, value) in orthogonal {
        set_window_char_slot(interp, new_id, slot, value);
    }
    let old_parent = window_link(interp, old_id, WINDOW_PARENT_SLOT);
    set_window_link(interp, new_id, WINDOW_PARENT_SLOT, old_parent)?;
    if before {
        let prev = window_link(interp, old_id, WINDOW_PREV_SIBLING_SLOT);
        set_window_link(interp, new_id, WINDOW_PREV_SIBLING_SLOT, prev)?;
        match prev {
            None => set_window_combination(interp, parent, horflag, Some(new_id))?,
            Some(prev) => set_window_link(interp, prev, WINDOW_NEXT_SIBLING_SLOT, Some(new_id))?,
        }
        set_window_link(interp, new_id, WINDOW_NEXT_SIBLING_SLOT, Some(old_id))?;
        set_window_link(interp, old_id, WINDOW_PREV_SIBLING_SLOT, Some(new_id))?;
    } else {
        let next = window_link(interp, old_id, WINDOW_NEXT_SIBLING_SLOT);
        set_window_link(interp, new_id, WINDOW_NEXT_SIBLING_SLOT, next)?;
        if let Some(next) = next {
            set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, Some(new_id))?;
        }
        set_window_link(interp, new_id, WINDOW_PREV_SIBLING_SLOT, Some(old_id))?;
        set_window_link(interp, old_id, WINDOW_NEXT_SIBLING_SLOT, Some(new_id))?;
    }
    set_window_slot_value(
        interp,
        new_id,
        WINDOW_NEW_PIXEL_SLOT,
        Value::Integer(pixel_size),
    )?;
    let mut sum = 0;
    let mut child = window_link(interp, parent, WINDOW_FIRST_CHILD_SLOT);
    while let Some(child_id) = child {
        if child_id != new_id {
            sum += window_slot_value(interp, child_id, WINDOW_NEW_TOTAL_SLOT)
                .as_integer()
                .unwrap_or(0);
        }
        child = window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT);
    }
    let (parent_cols, parent_lines, _, _) = window_char_geometry(interp, parent);
    set_window_slot_value(
        interp,
        new_id,
        WINDOW_NEW_TOTAL_SLOT,
        Value::Integer(if horflag { parent_cols } else { parent_lines } - sum),
    )?;
    set_window_slot_value(interp, new_id, WINDOW_NEW_NORMAL_SLOT, *normal_size)?;
    window_resize_apply(interp, parent, horflag)?;
    // NEW takes REFERENCE's margins (fringes and scroll bars have no
    // terminal width); set_window_buffer (new, r->contents, true, true)
    // keeps them, and NEW, having had no buffer, takes the buffer's
    // last_window_start and point.
    let (left_margin, right_margin) = interp.window_margins(reference);
    interp.set_window_margins(new_id, left_margin, right_margin);
    interp.show_buffer_in_window(new_id, reference_buffer, false);
    set_window_buffer_hooks(interp, new_id, reference_buffer, env)?;
    Ok(interp.record_value(new_id))
}

/// window.c:set_window_margins: WINDOW's margins become LEFT_WIDTH and
/// RIGHT_WIDTH columns (nil for none), unless that changes nothing or
/// would leave the window under MIN_SAFE_WINDOW_WIDTH columns; true when
/// the margins changed (apply_window_adjustment then redisplays it).
fn set_window_margins(
    interp: &mut Interpreter,
    window_id: u64,
    left_width: Value,
    right_width: Value,
) -> Result<bool, LispError> {
    // extract_dimension: check_integer_range (dimension, 0, INT_MAX).
    let extract = |dimension: Value| -> Result<i64, LispError> {
        if dimension.is_nil() {
            return Ok(0);
        }
        let Kind::Integer(cols) = dimension.kind() else {
            return Err(LispError::WrongTypeArgument("integerp".into(), dimension));
        };
        if !(0..=i64::from(i32::MAX)).contains(&cols) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                dimension,
                Value::Integer(0),
                Value::Integer(i64::from(i32::MAX)),
            ])));
        }
        Ok(cols)
    };
    let left = extract(left_width)?;
    let right = extract(right_width)?;
    let (old_left, old_right) = interp.window_margins(window_id);
    if old_left.unwrap_or(0) == left && old_right.unwrap_or(0) == right {
        return Ok(false);
    }
    // A text window has no fringes or scroll bars; MIN_SAFE_WINDOW_WIDTH
    // is two columns.
    if window_geometry(interp, window_id).0 - (left + right) < 2 {
        return Ok(false);
    }
    interp.set_window_margins(window_id, Some(left), Some(right));
    interp.wset_redisplay(window_id);
    Ok(true)
}

/// set_window_buffer's run_hooks_p: `window-scroll-functions' see the
/// window's new start with BUFFER current.
fn set_window_buffer_hooks(
    interp: &mut Interpreter,
    window_id: u64,
    buffer_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    let hooks = hook_values(interp, "window-scroll-functions", env, Some(buffer_id));
    if hooks.is_empty() {
        return Ok(());
    }
    let window = interp.record_value(window_id);
    let start = Value::Integer(window_start_marker(interp, window_id, buffer_id) as i64);
    let original_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(buffer_id)?;
    let mut result = Ok(());
    for hook in hooks {
        if let Err(error) = call_function_value(interp, &hook, &[window, start], env) {
            result = Err(error);
            break;
        }
    }
    if interp.has_buffer_id(original_buffer) {
        interp.set_current_buffer_id(original_buffer)?;
    }
    result
}

/// window.c:Fdelete_window_internal.  `delete-window' has staged the new
/// sizes of WINDOW's siblings; a parent left with one child is replaced
/// by it, which then merges into its new parent when that combines in
/// the same direction.
fn delete_window_from_tree(interp: &mut Interpreter, window: &Value) -> Result<Value, LispError> {
    let window_id = window_id_or_selected(interp, window)?;
    if window_is_deleted(interp, window_id) {
        return Ok(Value::Nil);
    }
    let Some(parent) = window_link(interp, window_id, WINDOW_PARENT_SLOT) else {
        return Err(LispError::Signal(
            "Attempt to delete minibuffer or sole ordinary window".into(),
        ));
    };
    let prev = window_link(interp, window_id, WINDOW_PREV_SIBLING_SLOT);
    let next = window_link(interp, window_id, WINDOW_NEXT_SIBLING_SLOT);
    if prev.is_none() && next.is_none() {
        return Err(LispError::Signal(
            "Attempt to delete sole window of parent".into(),
        ));
    }
    let horflag = window_combination(interp, parent) == Some(true);
    let frame = interp
        .window_frame_id(window_id)
        .expect("window stores its owning frame");
    let root = frame.borrow().root_window_id();

    let before_sibling = prev.is_none();
    let sibling = if let Some(prev) = prev {
        set_window_link(interp, prev, WINDOW_NEXT_SIBLING_SLOT, next)?;
        if let Some(next) = next {
            set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, Some(prev))?;
        }
        prev
    } else {
        let next = next.expect("a window with no previous sibling has a next one");
        set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, None)?;
        set_window_combination(interp, parent, horflag, Some(next))?;
        next
    };

    if !(window_resize_check(interp, root, horflag)
        && window_new_pixel(interp, root) == window_pixel_size(interp, root, horflag))
    {
        if before_sibling {
            set_window_link(interp, sibling, WINDOW_PREV_SIBLING_SLOT, Some(window_id))?;
            set_window_combination(interp, parent, horflag, Some(window_id))?;
        } else {
            set_window_link(interp, sibling, WINDOW_NEXT_SIBLING_SLOT, Some(window_id))?;
            if let Some(next) = next {
                set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, Some(window_id))?;
            }
        }
        return Err(LispError::Signal("Deletion failed".into()));
    }

    window_resize_apply(interp, parent, horflag)?;
    set_window_link(interp, window_id, WINDOW_NEXT_SIBLING_SLOT, None)?;
    if let Some(child) = window_link(interp, window_id, WINDOW_FIRST_CHILD_SLOT) {
        delete_all_child_windows(interp, child)?;
    } else {
        unshow_buffer(interp, window_id);
    }
    clear_deleted_window(interp, window_id)?;

    if window_link(interp, sibling, WINDOW_PREV_SIBLING_SLOT).is_none()
        && window_link(interp, sibling, WINDOW_NEXT_SIBLING_SLOT).is_none()
    {
        replace_window(interp, parent, sibling, false)?;
        for slot in [WINDOW_NORMAL_WIDTH_SLOT, WINDOW_NORMAL_HEIGHT_SLOT] {
            let normal = window_slot_value(interp, parent, slot);
            set_window_slot_value(interp, sibling, slot, normal)?;
        }
        clear_deleted_window(interp, parent)?;
        recombine_windows(interp, sibling)?;
    }

    let selected = frame.borrow().selected_window_id();
    if !is_live_ordinary_window(interp, selected) {
        let first = frame_window_tree_leaf_ids(interp, frame)
            .into_iter()
            .next()
            .expect("a frame keeps a live window");
        if selected == interp.selected_window_id() {
            select_window_value(interp, &interp.record_value(first), true)?;
        } else {
            frame.borrow_mut().selected_window = interp.record_ref(first);
        }
    }
    Ok(Value::Nil)
}

/// window.c:Fdelete_other_windows_internal: WINDOW replaces ROOT (the
/// frame's root window by default), whose other descendants are
/// deleted; windows outside ROOT stay.
fn delete_other_windows_from_tree(
    interp: &mut Interpreter,
    window: &Value,
    root: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut window_id = valid_window_id(interp, window)?;
    let frame = interp
        .window_frame_id(window_id)
        .expect("window stores its owning frame");
    let root_id = if root.is_nil() {
        frame.borrow().root_window_id()
    } else {
        let root_id = valid_window_id(interp, root)?;
        let mut ancestor = window_link(interp, window_id, WINDOW_PARENT_SLOT);
        while let Some(id) = ancestor
            && id != root_id
        {
            ancestor = window_link(interp, id, WINDOW_PARENT_SLOT);
        }
        if ancestor != Some(root_id) {
            return Err(LispError::Signal(
                "Specified root is not an ancestor of specified window".into(),
            ));
        }
        root_id
    };
    if window_id == root_id {
        return Ok(Value::Nil);
    }
    if window_is_minibuffer(interp, window_id) {
        return Err(LispError::Signal(
            "Can't expand minibuffer to full frame".into(),
        ));
    }

    let live = window_combination(interp, window_id).is_none();
    // A live WINDOW's start and top line, for keeping the text at its
    // start where it was on the frame.
    let frame_top = window_geometry(interp, frame.borrow().root_window_id()).3;
    let start_and_top = live.then(|| {
        let buffer_id = window_buffer_id(interp, &interp.record_value(window_id))
            .unwrap_or(interp.current_buffer_id());
        (
            buffer_id,
            window_start_marker(interp, window_id, buffer_id),
            window_geometry(interp, window_id).3 - frame_top,
        )
    });
    let mut swindow = window_id;
    if live {
        if window_id != frame.borrow().selected_window_id() {
            select_window_for_frame(interp, frame, window_id, false)?;
        }
    } else {
        // A selected window in WINDOW's subtree stays selected, else the
        // nearest one before it in the cyclic order.
        swindow = frame.borrow().selected_window_id();
        let leaves = frame_window_tree_leaf_ids(interp, frame);
        loop {
            let mut ancestor = Some(swindow);
            while let Some(id) = ancestor
                && id != window_id
            {
                ancestor = window_link(interp, id, WINDOW_PARENT_SLOT);
            }
            if ancestor == Some(window_id) {
                break;
            }
            let position = leaves.iter().position(|id| *id == swindow).unwrap_or(0);
            swindow = leaves[(position + leaves.len() - 1) % leaves.len()];
        }
        if swindow != frame.borrow().selected_window_id() {
            select_window_for_frame(interp, frame, swindow, false)?;
        }
    }

    let mut resize_failed = false;
    if !live {
        let (root_width, root_height, root_left, root_top) = window_geometry(interp, root_id);
        let (_, _, root_left_col, root_top_line) = window_char_geometry(interp, root_id);
        for horflag in [false, true] {
            let (width, height, left, top) = window_geometry(interp, window_id);
            let delta = if horflag {
                set_window_char_slot(interp, window_id, WINDOW_LEFT_COL_SLOT, root_left_col);
                set_window_geometry(interp, window_id, (width, height, root_left, top))?;
                root_width - width
            } else {
                set_window_char_slot(interp, window_id, WINDOW_TOP_LINE_SLOT, root_top_line);
                set_window_geometry(interp, window_id, (width, height, left, root_top))?;
                root_height - height
            };
            let mut resized = false;
            for ignore in [Value::Nil, Value::T] {
                crate::lisp::primitives::hooks_overlays::call_named_function(
                    interp,
                    "window--resize-root-window",
                    &[
                        interp.record_value(window_id),
                        Value::Integer(delta),
                        if horflag { Value::T } else { Value::Nil },
                        ignore,
                        Value::T,
                    ],
                    env,
                )?;
                if window_resize_check(interp, window_id, horflag) {
                    window_resize_apply(interp, window_id, horflag)?;
                    resized = true;
                    break;
                }
            }
            if !resized {
                resize_failed = true;
                break;
            }
        }
        if resize_failed {
            window_id = swindow;
        }
    }

    // Unlink WINDOW from its siblings.
    let parent = window_link(interp, window_id, WINDOW_PARENT_SLOT);
    let next = window_link(interp, window_id, WINDOW_NEXT_SIBLING_SLOT);
    if let Some(prev) = window_link(interp, window_id, WINDOW_PREV_SIBLING_SLOT) {
        set_window_link(interp, prev, WINDOW_NEXT_SIBLING_SLOT, next)?;
        if let Some(next) = next {
            set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, Some(prev))?;
        }
    } else if let (Some(parent), Some(next)) = (parent, next) {
        set_window_link(interp, next, WINDOW_PREV_SIBLING_SLOT, None)?;
        let horizontal = window_combination(interp, parent) == Some(true);
        set_window_combination(interp, parent, horizontal, Some(next))?;
    }
    if let Some(child) = window_link(interp, root_id, WINDOW_FIRST_CHILD_SLOT) {
        delete_all_child_windows(interp, child)?;
    }
    replace_window(interp, root_id, window_id, true)?;
    clear_deleted_window(interp, root_id)?;
    if let Some((buffer_id, startpos, top)) = start_and_top {
        let new_top = window_geometry(interp, window_id).3
            - window_geometry(interp, frame.borrow().root_window_id()).3;
        let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
        if new_top != top && startpos >= point_min && startpos <= point_max {
            let saved = interp.current_buffer_id();
            interp.set_current_buffer_id(buffer_id)?;
            let (start, _) = crate::lisp::primitives::window::move_screen_lines_in(
                interp,
                env,
                window_id,
                startpos,
                -(top as isize),
            );
            let at_line_beg = {
                let buffer = interp.buffer.borrow();
                start <= buffer.point_min() || buffer.char_at(start - 1) == Some('\n')
            };
            interp.set_current_buffer_id(saved)?;
            set_window_start_marker(interp, window_id, start);
            set_window_flag(
                interp,
                window_id,
                WINDOW_START_AT_LINE_BEG_SLOT,
                at_line_beg,
            );
            // So that redisplay runs the window-scroll-functions.
            set_window_flag(interp, window_id, WINDOW_OPTIONAL_NEW_START_SLOT, true);
        }
    }
    Ok(Value::Nil)
}

/// eval.c:safe_funcall: call FUNCTION with ARGS, an error being logged
/// ("Error muted by safe_call") rather than signaled.
fn window_change_safe_call(
    interp: &mut Interpreter,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let handler_start =
        interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
    let depth = env.len();
    let result = interp.call_function_value(function, None, args, env);
    interp.pop_handler_bindings(handler_start);
    env.truncate(depth);
    match result.map_err(LispError::into_kind) {
        Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
            Err(LispError::from(error))
        }
        Err(error) => {
            interp.clear_batch_error_backtrace();
            let message = super::call(
                interp,
                "format-message",
                &[
                    Value::String("Error muted by safe_call: %S signaled %S".into()),
                    Value::list(std::iter::once(function).chain(args.iter().cloned())),
                    crate::lisp::eval::error_condition_value(&LispError::from(error)),
                ],
                env,
            )?;
            let text = string_text(&message)?;
            log_message_text(interp, &text, env);
            Ok(())
        }
        Ok(_) => Ok(()),
    }
}

fn window_body_pixel_size(interp: &Interpreter, window: u64, env: &Env) -> (i64, i64) {
    let (_, height, _, _) = window_geometry(interp, window);
    let body_height = window_buffer_id(interp, &interp.record_value(window))
        .map_or(height, |buffer| {
            height - window_non_body_height(interp, buffer, env)
        });
    (window_text_width_columns(interp, window), body_height)
}

fn window_integer_slot(interp: &Interpreter, window: u64, slot: usize) -> i64 {
    window_slot_value(interp, window, slot)
        .as_integer()
        .unwrap_or(0)
}

/// window.c:run_window_change_functions_1: the functions of SYMBOL's
/// default value with frame argument (BUFFER nil), or of its buffer-local
/// value in BUFFER with WINDOW's argument; `t' and a dead argument skip.
fn run_window_change_functions_1(
    interp: &mut Interpreter,
    symbol: &str,
    buffer: Option<u64>,
    window_or_frame: Value,
    record_frames: &mut bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let functions = match buffer {
        None => interp
            .default_value(symbol)
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default(),
        Some(buffer) => interp
            .buffer_local_hook(buffer, symbol)
            .or_else(|| {
                interp
                    .buffer_local_value(buffer, symbol)
                    .and_then(|value| value.to_vec().ok())
            })
            .unwrap_or_default(),
    };
    for function in functions {
        let live = match window_or_frame.kind() {
            Kind::Frame(frame) => interp.frame_is_live(frame),
            Kind::Record(window) => is_live_ordinary_window(interp, window.id),
            _ => false,
        };
        if matches!(function.kind(), Kind::T) || !live {
            continue;
        }
        *record_frames = true;
        window_change_safe_call(interp, function, &[window_or_frame], env)?;
    }
    Ok(())
}

/// window.c:window_change_record: each frame records its selected
/// window, bumps its change stamp and stamps its live windows with their
/// buffer and sizes; then the selected window and frame become the old
/// ones.
fn window_change_record(interp: &mut Interpreter, record_frames: bool, env: &Env) {
    if record_frames {
        let frames: Vec<_> = interp
            .frame_states
            .iter()
            .filter(|frame| frame.is_live())
            .copied()
            .collect();
        for frame in frames {
            let stamp = {
                let mut state = frame.borrow_mut();
                state.old_selected_window = state.selected_window;
                state.change_stamp = state.change_stamp.wrapping_add(1).max(1);
                state.change_stamp
            };
            let windows = frame_window_tree_leaf_ids(interp, frame);
            for &window in &windows {
                let buffer = window_buffer_id(interp, &interp.record_value(window))
                    .and_then(|buffer| interp.buffer_value(buffer))
                    .unwrap_or(Value::Nil);
                let (width, height, _, _) = window_geometry(interp, window);
                let (body_width, body_height) = window_body_pixel_size(interp, window, env);
                for (slot, value) in [
                    (WINDOW_CHANGE_STAMP_SLOT, Value::Integer(stamp)),
                    (WINDOW_OLD_BUFFER_SLOT, buffer),
                    (WINDOW_OLD_PIXEL_WIDTH_SLOT, Value::Integer(width)),
                    (WINDOW_OLD_PIXEL_HEIGHT_SLOT, Value::Integer(height)),
                    (WINDOW_OLD_BODY_PIXEL_WIDTH_SLOT, Value::Integer(body_width)),
                    (
                        WINDOW_OLD_BODY_PIXEL_HEIGHT_SLOT,
                        Value::Integer(body_height),
                    ),
                ] {
                    let _ = set_window_slot_value(interp, window, slot, value);
                }
            }
            let mut state = frame.borrow_mut();
            state.number_of_windows = windows.len();
            state.window_state_change = false;
        }
    }
    interp.old_selected_window_id = interp.selected_window_id();
    interp.old_selected_frame_id = interp.selected_frame_id;
}

/// FRAME_WINDOW_CHANGE: window.c raises it whenever a window of FRAME is
/// split, deleted, resized or shown another buffer, and xdisp.c when a
/// window's body size changed; here the same events are read off the
/// state window_change_record left.
fn frame_window_change(
    interp: &Interpreter,
    windows: &[u64],
    number_of_windows: usize,
    stamp: i64,
    env: &Env,
) -> bool {
    windows.len() != number_of_windows
        || windows.iter().any(|&window| {
            let buffer = window_buffer_id(interp, &interp.record_value(window))
                .and_then(|buffer| interp.buffer_value(buffer))
                .unwrap_or(Value::Nil);
            let (width, height, _, _) = window_geometry(interp, window);
            let (body_width, body_height) = window_body_pixel_size(interp, window, env);
            window_integer_slot(interp, window, WINDOW_CHANGE_STAMP_SLOT) != stamp
                || !buffer.eq_value(window_slot_value(interp, window, WINDOW_OLD_BUFFER_SLOT))
                || width != window_integer_slot(interp, window, WINDOW_OLD_PIXEL_WIDTH_SLOT)
                || height != window_integer_slot(interp, window, WINDOW_OLD_PIXEL_HEIGHT_SLOT)
                || body_width
                    != window_integer_slot(interp, window, WINDOW_OLD_BODY_PIXEL_WIDTH_SLOT)
                || body_height
                    != window_integer_slot(interp, window, WINDOW_OLD_BODY_PIXEL_HEIGHT_SLOT)
        })
}

/// window.c:run_window_change_functions, called from redisplay: for each
/// frame with a window change, a selection change or a raised window
/// state change flag, run `window-buffer-change-functions',
/// `window-size-change-functions', `window-selection-change-functions'
/// and `window-state-change-functions' buffer-locally per window and then
/// globally per frame, then `window-configuration-change-hook', and
/// finally `window-state-change-hook'; window_change_record runs last.
pub(crate) fn run_window_change_functions(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let mut record_frames = false;
    let restore = interp.bind_special_dynamic("inhibit-redisplay", Value::T, env)?;
    let result = run_window_change_functions_body(interp, &mut record_frames, env);
    window_change_record(interp, record_frames, env);
    let restored = interp.restore_special_dynamic(restore, env);
    result.and(restored)
}

fn run_window_change_functions_body(
    interp: &mut Interpreter,
    record_frames: &mut bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let selected_frame = interp.selected_frame_id;
    let old_selected_frame = interp.old_selected_frame_id;
    let selected_frame_change = selected_frame != old_selected_frame;
    let mut run_window_state_change_hook = false;
    let frames: Vec<_> = interp
        .frame_states
        .iter()
        .filter(|frame| frame.is_live())
        .copied()
        .collect();
    for frame in frames {
        if !frame.is_live() || !frame.borrow().after_make_frame {
            continue;
        }
        let (stamp, number_of_windows, old_selected, selected, state_change) = {
            let state = frame.borrow();
            (
                state.change_stamp,
                state.number_of_windows,
                state.old_selected_window.map(|window| window.id),
                state.selected_window_id(),
                state.window_state_change,
            )
        };
        let windows = frame_window_tree_leaf_ids(interp, frame);
        let window_change = frame_window_change(interp, &windows, number_of_windows, stamp, env);
        let frame_selected_change =
            selected_frame_change && (frame == old_selected_frame || frame == selected_frame);
        let frame_selected_window_change = old_selected != Some(selected);
        if !(window_change || frame_selected_change || frame_selected_window_change || state_change)
        {
            continue;
        }
        let old_selected_window = interp.old_selected_window_id;
        let selected_window = interp.selected_window_id();
        let mut frame_buffer_change = false;
        let mut frame_size_change = false;
        let mut counted = 0usize;
        for &window in &windows {
            counted += 1;
            if !is_live_ordinary_window(interp, window) {
                continue;
            }
            let window_value = interp.record_value(window);
            let buffer_id = window_buffer_id(interp, &window_value);
            let buffer = buffer_id
                .and_then(|buffer| interp.buffer_value(buffer))
                .unwrap_or(Value::Nil);
            let buffer_change = window_change
                && (!buffer.eq_value(window_slot_value(interp, window, WINDOW_OLD_BUFFER_SLOT))
                    || window_integer_slot(interp, window, WINDOW_CHANGE_STAMP_SLOT) != stamp);
            let (width, height, _, _) = window_geometry(interp, window);
            let (body_width, body_height) = window_body_pixel_size(interp, window, env);
            let size_change = window_change
                && (buffer_change
                    || width != window_integer_slot(interp, window, WINDOW_OLD_PIXEL_WIDTH_SLOT)
                    || height != window_integer_slot(interp, window, WINDOW_OLD_PIXEL_HEIGHT_SLOT)
                    || body_width
                        != window_integer_slot(interp, window, WINDOW_OLD_BODY_PIXEL_WIDTH_SLOT)
                    || body_height
                        != window_integer_slot(interp, window, WINDOW_OLD_BODY_PIXEL_HEIGHT_SLOT));
            frame_buffer_change |= buffer_change;
            frame_size_change |= size_change;
            if buffer_change {
                run_window_change_functions_1(
                    interp,
                    "window-buffer-change-functions",
                    buffer_id,
                    window_value,
                    record_frames,
                    env,
                )?;
            }
            if size_change && is_live_ordinary_window(interp, window) {
                run_window_change_functions_1(
                    interp,
                    "window-size-change-functions",
                    buffer_id,
                    window_value,
                    record_frames,
                    env,
                )?;
            }
            let selection_change = (frame_selected_change
                && (window == old_selected_window || window == selected_window))
                || (frame_selected_window_change
                    && (Some(window) == old_selected || window == selected));
            if selection_change && is_live_ordinary_window(interp, window) {
                run_window_change_functions_1(
                    interp,
                    "window-selection-change-functions",
                    buffer_id,
                    window_value,
                    record_frames,
                    env,
                )?;
            }
            if (buffer_change || size_change || selection_change)
                && is_live_ordinary_window(interp, window)
            {
                run_window_change_functions_1(
                    interp,
                    "window-state-change-functions",
                    buffer_id,
                    window_value,
                    record_frames,
                    env,
                )?;
            }
        }
        let window_deleted = counted < number_of_windows;
        let frame_value = Value::Frame(frame);
        if (frame_buffer_change || window_deleted) && frame.is_live() {
            run_window_change_functions_1(
                interp,
                "window-buffer-change-functions",
                None,
                frame_value,
                record_frames,
                env,
            )?;
        }
        if frame_size_change && frame.is_live() {
            run_window_change_functions_1(
                interp,
                "window-size-change-functions",
                None,
                frame_value,
                record_frames,
                env,
            )?;
        }
        if (frame_selected_change || frame_selected_window_change) && frame.is_live() {
            run_window_change_functions_1(
                interp,
                "window-selection-change-functions",
                None,
                frame_value,
                record_frames,
                env,
            )?;
        }
        if (frame_selected_change
            || frame_selected_window_change
            || frame_buffer_change
            || window_deleted
            || frame_size_change
            || state_change)
            && frame.is_live()
        {
            run_window_change_functions_1(
                interp,
                "window-state-change-functions",
                None,
                frame_value,
                record_frames,
                env,
            )?;
            run_window_state_change_hook = true;
            *record_frames = true;
        }
        if (frame_size_change || window_deleted) && frame.is_live() {
            super::call(
                interp,
                "run-window-configuration-change-hook",
                &[frame_value],
                env,
            )?;
        }
    }
    if run_window_state_change_hook
        && interp
            .lookup_var("window-state-change-hook", env)
            .is_some_and(|value| value.is_truthy())
    {
        crate::batch::safe_run_hooks(interp, "window-state-change-hook")?;
    }
    Ok(())
}

fn window_buffer_id_or_selected(
    interp: &Interpreter,
    window: Option<&Value>,
) -> Result<u64, LispError> {
    match window.map(|v| v.kind()) {
        None | Some(Kind::Nil) => Ok(interp.selected_window_buffer_id()),
        Some(window) => {
            window_id_or_selected(interp, &window.value())?;
            window_buffer_id(interp, &window.value())
                .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), window.value()))
        }
    }
}

fn window_line_height(
    interp: &Interpreter,
    buffer_id: u64,
    format_variable: &str,
    env: &Env,
) -> i64 {
    let format = if buffer_id == interp.current_buffer_id() {
        // BVAR (current_buffer, ...): the symbol's loaded binding.
        match format_variable {
            "mode-line-format" => {
                interp.local_variable_value_key(cached_symbol!("mode-line-format"))
            }
            "header-line-format" => {
                interp.local_variable_value_key(cached_symbol!("header-line-format"))
            }
            "tab-line-format" => interp.local_variable_value_key(cached_symbol!("tab-line-format")),
            _ => interp.lookup_var(format_variable, env),
        }
    } else {
        interp
            .buffer_local_toplevel_value(buffer_id, format_variable)
            .or_else(|| interp.global_binding_value(format_variable))
    };
    i64::from(format.is_some_and(|format| !format.is_nil()))
}

fn window_non_body_height(interp: &Interpreter, buffer_id: u64, env: &Env) -> i64 {
    ["mode-line-format", "header-line-format", "tab-line-format"]
        .into_iter()
        .map(|variable| window_line_height(interp, buffer_id, variable, env))
        .sum()
}

/// Screen rows owned by the window selected *now*.  During a minibuffer read,
/// packages can temporarily select the shrunken ordinary window and call
/// `recenter'.  The last published glyph metrics then belong to an earlier
/// redisplay and can still describe the pre-minibuffer height; the live window
/// tree is GNU's authoritative source for command-time geometry.  Batch mode
/// has no realized window tree, so it retains the published/dumb-frame model.
fn selected_command_text_height(interp: &Interpreter, env: &Env) -> usize {
    if interp
        .lookup_var_key(cached_symbol!("noninteractive"), env)
        .is_some_and(|value| value.is_truthy())
    {
        return selected_window_text_height().max(1);
    }
    let window_id = interp.selected_window_id();
    let buffer_id = interp.selected_window_buffer_id();
    (window_geometry(interp, window_id).1 - window_non_body_height(interp, buffer_id, env)).max(1)
        as usize
}

/// WINDOW_TOTAL_COLS of WINDOW.
pub(crate) fn window_total_columns(interp: &Interpreter, window_id: u64) -> i64 {
    window_geometry(interp, window_id).0
}

/// window_box_width of WINDOW's text area in terminal cells.
pub(crate) fn window_text_area_columns(interp: &Interpreter, window_id: u64) -> usize {
    window_text_width_columns(interp, window_id).max(0) as usize
}

fn window_text_width_columns(interp: &Interpreter, window_id: u64) -> i64 {
    let (total, _, left, _) = window_geometry(interp, window_id);
    let root_id = match interp.root_window_value().kind() {
        Kind::Record(id) => id.id,
        _ => window_id,
    };
    let (root_width, _, root_left, _) = window_geometry(interp, root_id);
    // GNU reserves one terminal column for the vertical separator of every
    // live window whose right edge is not the root window's right edge.  The
    // separator is part of WINDOW-TOTAL-WIDTH but not WINDOW-BODY-WIDTH (see
    // window_body_width in window.c).  Emaxx currently exposes terminal
    // frames, so there are no GUI fringes or scroll-bar pixels to subtract.
    let vertical_separator =
        i64::from(left.saturating_add(total) < root_left.saturating_add(root_width));
    let (left, right) = interp.window_margins(window_id);
    (total - vertical_separator - left.unwrap_or(0) - right.unwrap_or(0)).max(0)
}

/// The per-window horizontal-scroll state xdisp.c's hscroll_window_tree
/// reads and writes: w->hscroll, w->min_hscroll, w->suspend_auto_hscroll,
/// and w->old_pointm (the point the last redisplay saw, which decides
/// when an explicit scroll's suspension lifts).
pub(crate) struct WindowHscrollState {
    pub(crate) hscroll: i64,
    pub(crate) min_hscroll: i64,
    pub(crate) suspended: bool,
    pub(crate) old_point: Option<i64>,
}

pub(crate) fn window_hscroll_state(interp: &Interpreter, window_id: u64) -> WindowHscrollState {
    WindowHscrollState {
        hscroll: window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
            .as_integer()
            .unwrap_or(0)
            .max(0),
        min_hscroll: window_slot_value(interp, window_id, WINDOW_MIN_HSCROLL_SLOT)
            .as_integer()
            .unwrap_or(0)
            .max(0),
        suspended: window_slot_value(interp, window_id, WINDOW_SUSPEND_AUTO_HSCROLL_SLOT)
            .is_truthy(),
        old_point: window_slot_value(interp, window_id, WINDOW_OLD_POINT_SLOT)
            .as_integer()
            .ok(),
    }
}

/// hscroll_window_tree's write-back: the recomputed hscroll, the
/// (possibly lifted) suspension, and the remembered window point.
pub(crate) fn store_window_hscroll_state(
    interp: &mut Interpreter,
    window_id: u64,
    hscroll: i64,
    suspended: bool,
    old_point: i64,
) {
    let _ = set_window_slot_value(
        interp,
        window_id,
        WINDOW_HSCROLL_SLOT,
        Value::Integer(hscroll.max(0)),
    );
    let _ = set_window_slot_value(
        interp,
        window_id,
        WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
        if suspended { Value::T } else { Value::Nil },
    );
    let _ = set_window_slot_value(
        interp,
        window_id,
        WINDOW_OLD_POINT_SLOT,
        Value::Integer(old_point),
    );
}

/// Which numbers `display-line-numbers' asks for (xdisp.c's
/// Vdisplay_line_numbers): absolute buffer lines, distances from the
/// line showing point, or distances in screen lines.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineNumberMode {
    Absolute,
    Relative,
    Visual,
}

/// The line-number column a window reserves this redisplay, resolved
/// from the buffer's `display-line-numbers' family of variables the way
/// maybe_produce_line_number does.  `width' is the digit field
/// (it->lnum_width); the column costs `width + 2' screen columns — the
/// number right-justified in a width+1 field plus a blank separator
/// (pint2str with lnum_width + 1, then the appended space).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct LineNumberLayout {
    pub(crate) mode: LineNumberMode,
    pub(crate) width: usize,
    pub(crate) cols: usize,
    pub(crate) current_absolute: bool,
    pub(crate) offset: i64,
    pub(crate) widen: bool,
    pub(crate) major_tick: i64,
    pub(crate) minor_tick: i64,
}

/// maybe_produce_line_number's width computation for one window.
/// TOP_LINE and POINT_LINE are 1-based absolute buffer lines,
/// BEGV_LINE the absolute line where the accessible region begins:
/// numbers count from there unless the layout widens.  TEXT_ROWS is
/// the window's body height.  The maximal number a window row can show
/// is the one belonging to its last row — a 0-based this_line at vpos
/// 0 plus the matrix rows: this_line + nrows - 1 with nrows =
/// text_rows + 1.
pub(crate) fn window_line_number_layout(
    interp: &Interpreter,
    buffer_id: u64,
    top_line: usize,
    point_line: usize,
    begv_line: usize,
    text_rows: usize,
) -> Option<LineNumberLayout> {
    let buffer_local = |name: &str| {
        interp
            .buffer_local_value(buffer_id, name)
            .or_else(|| interp.default_value(name))
            .unwrap_or(Value::Nil)
    };
    let mode = match buffer_local("display-line-numbers").kind() {
        Kind::Nil => return None,
        Kind::Symbol(ref name) if name == "relative" => LineNumberMode::Relative,
        Kind::Symbol(ref name) if name == "visual" => LineNumberMode::Visual,
        _ => LineNumberMode::Absolute,
    };
    let current_absolute = buffer_local("display-line-numbers-current-absolute").is_truthy();
    let offset = match mode {
        LineNumberMode::Absolute => buffer_local("display-line-numbers-offset")
            .as_integer()
            .unwrap_or(0),
        _ => 0,
    };
    // A non-zero offset forces counting from the buffer's beginning, as
    // if display-line-numbers-widen were non-nil.
    let widen = buffer_local("display-line-numbers-widen").is_truthy() || offset != 0;
    let displayed = |line: usize| {
        if widen {
            line
        } else {
            line.saturating_sub(begv_line) + 1
        }
    };
    let max_lnum = match mode {
        LineNumberMode::Relative | LineNumberMode::Visual if !current_absolute => {
            text_rows.saturating_sub(1)
        }
        LineNumberMode::Visual => displayed(point_line).saturating_add(text_rows) - 1,
        _ => displayed(top_line).saturating_add(text_rows) - 1,
    };
    let needed = max_lnum.max(1).to_string().len();
    let explicit = buffer_local("display-line-numbers-width")
        .as_integer()
        .ok()
        .filter(|width| *width >= 0)
        .unwrap_or(0) as usize;
    let width = needed.max(explicit);
    let tick = |name: &str| {
        interp
            .lookup_var(name, &crate::lisp::types::Env::new())
            .and_then(|value| value.integer_value())
            .unwrap_or(0)
    };
    Some(LineNumberLayout {
        mode,
        width,
        cols: width + 2,
        current_absolute,
        offset,
        widen,
        major_tick: tick("display-line-numbers-major-tick"),
        minor_tick: tick("display-line-numbers-minor-tick"),
    })
}

fn set_window_hscroll_value(
    interp: &mut Interpreter,
    window_id: u64,
    requested: i64,
) -> Result<Value, LispError> {
    let hscroll = requested.max(0);
    if window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT).integer_value() != Some(hscroll) {
        // set_window_hscroll: a changed hscroll redisplays the window.
        interp.wset_redisplay(window_id);
    }
    set_window_slot_value(
        interp,
        window_id,
        WINDOW_HSCROLL_SLOT,
        Value::Integer(hscroll),
    )?;
    set_window_slot_value(
        interp,
        window_id,
        WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
        Value::T,
    )?;
    Ok(Value::Integer(hscroll))
}

fn valid_window_cursor_type(value: &Value) -> bool {
    match value.kind() {
        Kind::Nil | Kind::T => true,
        Kind::Symbol(symbol) => matches!(symbol.as_str(), "box" | "hollow" | "bar" | "hbar"),
        Kind::Cons(_) => {
            let Ok(kind) = value
                .car()
                .and_then(|kind| kind.as_symbol().map(str::to_owned))
            else {
                return false;
            };
            matches!(kind.as_str(), "box" | "bar" | "hbar")
                && value.cdr().is_ok_and(|width| width.as_integer().is_ok())
        }
        _ => false,
    }
}

fn window_list_value(
    interp: &Interpreter,
    minibuf: Option<&Value>,
    start: Option<&Value>,
    frames: &[crate::lisp::types::FrameRef],
) -> Value {
    Value::list(
        window_list_ids(interp, minibuf, start, frames)
            .into_iter()
            .map(|id| interp.record_value(id)),
    )
}

/// The frames FRAME_FILTER (window-list-1's ALL-FRAMES) names, in the
/// frame list's order: every live frame for t, FRAME itself, the selected
/// terminal's for 0 or `visible', else OWN_FRAME, the frame of the window
/// the walk starts from.
fn window_list_frames(
    interp: &Interpreter,
    own_frame: crate::lisp::types::FrameRef,
    frame_filter: &Value,
) -> Vec<crate::lisp::types::FrameRef> {
    interp
        .frame_states
        .iter()
        .filter(|frame| frame.is_live())
        .filter(|frame| match frame_filter.kind() {
            Kind::T => true,
            Kind::Frame(id) => **frame == id,
            Kind::Integer(0) => {
                frame.borrow().terminal.expect("live frame terminal").id
                    == interp.selected_terminal_id()
            }
            Kind::Symbol(name) if name == "visible" => {
                frame.borrow().terminal.expect("live frame terminal").id
                    == interp.selected_terminal_id()
            }
            _ => **frame == own_frame,
        })
        .copied()
        .collect()
}

/// window.c:Fget_buffer_window's window_loop (GET_BUFFER_WINDOW): among
/// ALL-FRAMES's windows in their cyclic order from the selected window of
/// the frame ALL-FRAMES names (else of the selected frame), the selected
/// window if it shows BUFFER_ID, else the first that does on the selected
/// frame, else the first that does at all.  A minibuffer window counts
/// only while a minibuffer is active and only as minibuf_window.
pub(super) fn buffer_window(
    interp: &Interpreter,
    buffer_id: u64,
    all_frames: &Value,
) -> Option<Value> {
    let selected_frame = interp.selected_frame_id;
    let frame = match all_frames.kind() {
        Kind::Frame(frame) if frame.is_live() => frame,
        _ => selected_frame,
    };
    let selected = interp.selected_window_id();
    let minibuffer_active = interp.active_minibuffer_buffer_id().is_some();
    let minibuffer = interp.minibuffer_window_id();
    let shows = |id: u64| {
        (is_live_ordinary_window(interp, id) || (minibuffer_active && id == minibuffer))
            && window_buffer_id(interp, &interp.record_value(id)) == Some(buffer_id)
    };
    // The selected window is returned wherever the walk meets it, and it
    // is among the windows unless ALL-FRAMES names another frame.
    if frame == selected_frame && shows(selected) {
        return Some(interp.record_value(selected));
    }
    let start = interp.record_value(frame.borrow().selected_window_id());
    let frames = window_list_frames(interp, frame, all_frames);
    let mut best = None;
    let mut on_selected_frame = false;
    for id in window_list_ids(interp, Some(&Value::T), Some(&start), &frames) {
        if !shows(id) {
            continue;
        }
        if id == selected {
            return Some(interp.record_value(id));
        }
        if !on_selected_frame && interp.window_frame_id(id) == Some(selected_frame) {
            best = Some(id);
            on_selected_frame = true;
        } else if best.is_none() {
            best = Some(id);
        }
    }
    best.map(|id| interp.record_value(id))
}

/// The windows `window_list_value' lists, by record id.
fn window_list_ids(
    interp: &Interpreter,
    minibuf: Option<&Value>,
    start: Option<&Value>,
    frames: &[crate::lisp::types::FrameRef],
) -> Vec<u64> {
    let mut ids = Vec::new();
    for frame in frames {
        let mut leaves = frame_window_tree_leaf_ids(interp, *frame);
        leaves.retain(|id| is_live_ordinary_window(interp, *id));
        if ids.is_empty() {
            ids = leaves;
        } else {
            ids.extend(leaves);
        }
        // GNU window.c includes the active minibuffer when MINIBUF is nil or
        // omitted, and includes it unconditionally for t.  Any other non-nil
        // value excludes it.  Add it before rotating so an active, selected
        // minibuffer is first in the cyclic order; window.el's
        // `get-buffer-window-list' relies on precisely this default contract.
        let include_minibuffer = match minibuf.map(|v| v.kind()) {
            Some(Kind::T) => true,
            None | Some(Kind::Nil) => interp.active_minibuffer_buffer_id().is_some(),
            Some(_) => false,
        };
        if include_minibuffer {
            let minibuffer_id = frame.borrow().minibuffer_window_id();
            if !ids.contains(&minibuffer_id) {
                ids.push(minibuffer_id);
            }
        }
    }
    // GNU lists windows in cyclic order starting from WINDOW (default
    // the selected window), not from the tree's first leaf.
    let start_id = start
        .filter(|window| !window.is_nil())
        .and_then(|window| window_record_id_from_value(interp, window))
        .unwrap_or_else(|| interp.selected_window_id());
    if let Some(index) = ids.iter().position(|id| *id == start_id) {
        ids.rotate_left(index);
    }
    ids
}

/// print.c Fprin1_to_string: OBJECT as `prin1' prints it, or as `princ'
/// does with NOESCAPE (no quoting), with OVERRIDES bound either way, as
/// Fprin1_to_string binds them before printing.
pub(crate) fn prin1_to_string(
    interp: &mut Interpreter,
    env: &mut Env,
    object: &Value,
    noescape: bool,
    overrides: Option<&Value>,
) -> Result<Value, LispError> {
    let rendered = with_print_overrides(interp, env, overrides, |interp, env| {
        if noescape {
            with_printer_buffer_escape(interp, env, Some(true), |interp, env| {
                render_printer_output(interp, object, env, false, false)
            })
        } else {
            render_prin1_output(interp, object, env)
        }
    })?;
    rendered.into_value(interp, env)
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "format-mode-line" => mode_line::format_mode_line(interp, args, env),
            // ── Output ──
            "message" => {
                let (text, formatted) =
                    if args.is_empty() || args.first().is_some_and(Value::is_nil) {
                        (String::new(), None)
                    } else {
                        // editfns.c Fmessage formats through Fformat_message,
                        // so grave quotes follow `text-quoting-style'.
                        let formatted = super::call(interp, "format-message", args, env)?;
                        (string_text(&formatted)?, Some(formatted))
                    };
                // Fmessage: `(message nil)' and `(message "")' clear the
                // echo area (message1 (0)) without logging, which edebug
                // relies on and the upstream capture advice ignores. Any
                // other format logs, even when it formats to "".
                let capturable = !args.is_empty()
                    && !args.first().is_some_and(Value::is_nil)
                    && args
                        .first()
                        .and_then(|format| with_string_text(format, |text| !text.is_empty()))
                        .unwrap_or(true);
                // message3: first flush out any partial line written with
                // print, whatever the message.
                message_log_maybe_newline(interp, env);
                if capturable {
                    log_message_text(interp, &text, env);
                    interp.append_message_capture(&text, true, env);
                }
                // An interactive session shows the message in the echo area
                // (an empty or nil MESSAGE clears it, GNU's documented way
                // to wipe the echo line).
                if interp
                    .lookup_var_key(cached_symbol!("noninteractive"), env)
                    .is_none_or(|value| value.is_nil())
                {
                    if text.is_empty() {
                        // xdisp.c clear_message: `clear-message-function'
                        // clears its own display first (the minibuffer's
                        // transient overlay); `dont-clear-message' keeps
                        // the echo area untouched.
                        let kept = interp
                            .lookup_var_key(cached_symbol!("clear-message-function"), env)
                            .filter(|function| !function.is_nil())
                            .and_then(|function| {
                                interp.call_function_value(function, None, &[], env).ok()
                            })
                            .is_some_and(|result| {
                                matches!(result.kind(), Kind::Symbol(answer)
                                    if answer == "dont-clear-message")
                            });
                        if !kept {
                            set_echo_area_message(None);
                        }
                    } else {
                        // xdisp.c set_message: `set-message-function' may
                        // replace the string or consume it entirely
                        // (set-minibuffer-message displays it inside the
                        // active minibuffer instead of the echo area).
                        let mut display = formatted;
                        if let Some(function) = interp
                            .lookup_var_key(cached_symbol!("set-message-function"), env)
                            .filter(|function| !function.is_nil())
                        {
                            let string =
                                formatted.unwrap_or_else(|| Value::String(text.clone().into()));
                            if let Ok(result) =
                                interp.call_function_value(function, None, &[string], env)
                            {
                                if result.is_string() {
                                    display = Some(result);
                                } else if result.is_truthy() {
                                    display = None;
                                }
                            }
                        }
                        if let Some(value) = display {
                            set_echo_area_message_value(value)?;
                        }
                    }
                }
                // There is no echo area in batch mode.  GNU writes messages to
                // the process stderr instead (xdisp.c message_to_stderr),
                // including the newline used to clear an empty message, and
                // first a newline of its own when stdout was written since
                // the previous message (`noninteractive_need_newline').
                // Message capture remains independent of `inhibit-message',
                // matching the advice used by ERT.
                if interp
                    .lookup_var_key(cached_symbol!("noninteractive"), env)
                    .is_some_and(|value| value.is_truthy())
                    && interp
                        .lookup_var_key(cached_symbol!("inhibit-message"), env)
                        .is_none_or(|value| value.is_nil())
                {
                    message_to_stderr(interp, formatted.as_ref(), env)?;
                }
                if args.first().is_some_and(Value::is_nil) {
                    Ok(Value::Nil)
                } else {
                    Ok(formatted.unwrap_or_else(|| Value::String(text.into())))
                }
            }
            "message-box" | "message-or-box" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                // Emaxx currently has no graphical frame, so GNU's documented
                // dialog fallback is the ordinary echo-area message path.
                super::call(interp, "message", args, env)
            }
            "current-message" => {
                need_args(name, args, 0)?;
                // GNU batch mode has no echo area; `current-message' is nil.
                if interp
                    .lookup_var_key(cached_symbol!("noninteractive"), env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return Ok(Value::Nil);
                }
                // The interactive echo area is authoritative: unlike the
                // *Messages* tail it reflects `(message nil)' clears and
                // messages suppressed from the log by `message-log-max'.
                Ok(echo_area_message_value().unwrap_or(Value::Nil))
            }
            "error-message-string" => {
                need_args(name, args, 1)?;
                if let Err(LispErrorKind::SignalValue(signal)) =
                    args[0].to_vec().map_err(LispError::into_kind)
                    && circular_list_signal_p(&signal)
                {
                    return Err(LispError::SignalValue(signal));
                }
                // print.c's print_error_message: plain `error' promotes its
                // first datum to the message; other conditions lead with
                // their error-message property.  Remaining data is printed
                // under the condition-specific princ/prin1 rule below.
                let Ok(items) = args[0].to_vec() else {
                    return Ok(Value::String(args[0].to_string().into()));
                };
                let Some(Kind::Symbol(condition)) = items.first().map(|v| v.kind()) else {
                    return Ok(Value::String(args[0].to_string().into()));
                };
                // print.c:Ferror_message_string returns the original object
                // for (error STRING), without allocating or losing properties.
                if condition == "error" && items.len() == 2 && items[1].is_string() {
                    return Ok(items[1]);
                }
                // A file-error condition promotes its first datum to the
                // message ("Opening input file: ...").
                let file_error = interp
                    .get_symbol_property(&condition, "error-conditions")
                    .and_then(|conditions| conditions.to_vec().ok())
                    .is_some_and(|conditions| {
                        conditions.iter().any(
                            |entry| matches!(entry.kind(), Kind::Symbol(name) if name == "file-error"),
                        )
                    });
                let mut data = &items[1..];
                let mut text = if condition == "error" {
                    let message = data
                        .first()
                        .and_then(string_like)
                        .map(|message| message.text)
                        .unwrap_or_else(|| "peculiar error".to_string());
                    data = data.get(1..).unwrap_or_default();
                    message
                } else {
                    let message = interp
                        .get_symbol_property(&condition, "error-message")
                        .as_ref()
                        .and_then(string_like)
                        .map(|message| message.text)
                        .unwrap_or_else(|| "peculiar error".to_string());
                    // print.c passes the error-message property through
                    // `substitute-command-keys' when it is fboundp (so the
                    // apostrophe in "Symbol's" follows `text-quoting-style'),
                    // keeping the raw text if that call fails.
                    if interp
                        .lookup_function("substitute-command-keys", env)
                        .is_ok()
                        && let Ok(substituted) = interp.call_function_value(
                            Value::Symbol("substitute-command-keys".into()),
                            Some("substitute-command-keys"),
                            &[Value::String(message.clone().into())],
                            env,
                        )
                        && let Some(substituted) = string_like(&substituted)
                    {
                        substituted.text
                    } else {
                        message
                    }
                };
                if file_error && !data.is_empty() {
                    text =
                        crate::lisp::primitives::print::render_princ_object(interp, &data[0], env)?;
                    data = &data[1..];
                }
                if !data.is_empty() {
                    if !text.is_empty() {
                        text.push_str(": ");
                    }
                    // print.c princ's file/end-of-file/user-error data and
                    // prin1's every other condition's data.  The distinction
                    // matters for custom conditions carrying strings: Magit's
                    // multi-part git errors retain quotes and escaping.
                    let use_princ =
                        file_error || condition == "end-of-file" || condition == "user-error";
                    let mut rendered = Vec::with_capacity(data.len());
                    for datum in data {
                        rendered.push(if use_princ {
                            crate::lisp::primitives::print::render_princ_object(interp, datum, env)?
                        } else {
                            crate::lisp::primitives::print::render_prin1_ephemeral(
                                interp, datum, env,
                            )?
                        });
                    }
                    text.push_str(&rendered.join(", "));
                }
                // The general C path returns Fbuffer_string from the
                // multibyte prin1 buffer. It is already mutable at creation;
                // bytecode argument passing only copies the Lisp_Object.
                Ok(make_shared_string_value_with_multibyte(text, None, true))
            }
            "command-error-default-function" => {
                need_args(name, args, 3)?;
                let conditions = match args[0].cons_ref() {
                    Some(cell) => super::call(
                        interp,
                        "get",
                        &[cell.car.get(), Value::symbol("error-conditions")],
                        env,
                    )?,
                    None => Value::Nil,
                };
                let minibuffer_quit = conditions.to_vec().unwrap_or_default().iter().any(
                    |condition| matches!(condition.kind(), Kind::Symbol(name) if name == "minibuffer-quit"),
                );
                let context = string_text(&args[1])?;
                // keyboard.c command-error-default-function, noninteractive
                // branch: print CONTEXT plus the error to stderr and
                // kill-emacs -1.  The previous arm computed the message and
                // discarded it, silently converting a should-fail batch run
                // into a clean exit (2026-08-23 audit finding 80).
                let message = super::call(interp, "error-message-string", &[args[0]], env)?;
                let rendered = string_text(&message).unwrap_or_default();
                if !minibuffer_quit
                    && interp
                        .lookup_var_key(cached_symbol!("noninteractive"), env)
                        .is_some_and(|value| value.is_truthy())
                {
                    use std::io::Write;
                    let _ = writeln!(std::io::stderr(), "{context}{rendered}");
                    return super::call(interp, "kill-emacs", &[Value::Integer(-1)], env);
                }
                // clear_message (1, 0), then bitch_at_user (or `ding' with
                // ARG t for a minibuffer-quit), then print_error_message to
                // the echo area: CONTEXT and the message shown, the
                // signalling function before the message in *Messages*.
                clear_message(interp, env, true, false)?;
                message_log_maybe_newline(interp, env);
                if minibuffer_quit {
                    super::call(interp, "ding", &[Value::T], env)?;
                } else {
                    super::call(interp, "ding", &[], env)?;
                }
                // print_error_message (data, Qt, context, signal): printed
                // to the echo area (CONTEXT, empty or not, first), the
                // signalling function only logged.
                echo_area_print_logged(interp, &context, env);
                if let Kind::Symbol(caller) = args[2].kind() {
                    log_message_partial(interp, caller.as_ref(), env);
                    log_message_partial(interp, ": ", env);
                }
                echo_area_print_logged(interp, &rendered, env);
                Ok(Value::Nil)
            }
            "ding" => {
                need_arg_range(name, args, 0, 1)?;
                // dispnew.c:Fding: ARG non-nil rings the bell without
                // ending a keyboard macro (a BEL on stdout in batch).
                if args.first().is_some_and(Value::is_truthy) {
                    if interp
                        .forwarded_c_value("noninteractive", env)
                        .is_some_and(|value| value.is_truthy())
                    {
                        crate::lisp::primitives::batch_stdout::write(b"\x07")
                            .map_err(|error| LispError::Signal(error.to_string()))?;
                    } else {
                        ring_bell(interp, env)?;
                    }
                } else {
                    bitch_at_user(interp, env)?;
                }
                Ok(Value::Nil)
            }
            "sleep-for" => {
                need_arg_range(name, args, 1, 2)?;
                let duration = wait_duration(args)?;
                if duration.is_zero() {
                    // Fsleep_for enters wait_reading_process_output only for
                    // a positive duration and otherwise returns nil at once:
                    // no subprocess output, no timers (in batch `sit-for'
                    // is this call, and the oracle leaves a due
                    // `run-at-time' timer unrun across it).
                    return Ok(Value::Nil);
                }
                // GNU processes subprocess output whenever it waits; epg relies
                // on the trailing (sleep-for 0.1) in epg-wait-for-completion to
                // flush gpg's final status lines through the process filter.
                // wait_reading_process_output records READ_KBD 0 for the
                // whole call, even when it nests inside a keyboard read.
                let previous_wait = interp.set_waiting_for_user_input(false);
                let wait_result =
                    wait_pumping_processes(interp, env, Some(duration), false, None, None, true);
                interp.set_waiting_for_user_input(previous_wait);
                wait_result?;
                Ok(Value::Nil)
            }
            "accept-process-output" => {
                need_arg_range(name, args, 0, 4)?;
                // GNU: (accept-process-output &optional PROCESS SECONDS MILLISEC
                // JUST-THIS-ONE) - the wait always comes from args 2 and 3.
                let duration_args = if args.len() > 1 {
                    &args[1..args.len().min(3)]
                } else {
                    &[]
                };
                let target_process_id = args
                    .first()
                    .filter(|process| !process.is_nil())
                    .map(|process| interp.resolve_process_id(process))
                    .transpose()?;
                if let Some(process_id) = target_process_id {
                    interp.ensure_process_owned_by_current_thread(process_id)?;
                }
                let timeout = if args.get(1).is_some_and(|seconds| !seconds.is_nil()) {
                    Some(wait_duration(duration_args)?)
                } else if target_process_id.is_none() {
                    // With neither PROCESS nor a timeout GNU performs one event
                    // pump (including due timers) and returns.  Only a specified
                    // process makes the timeout-less form wait indefinitely.
                    Some(std::time::Duration::ZERO)
                } else {
                    None
                };
                // A non-nil JUST-THIS-ONE suspends output from every other
                // process.  Its integer spelling also suppresses timers.
                // GNU ignores the flag when PROCESS itself is nil.
                let just_this_one = if target_process_id.is_some() {
                    args.get(3).filter(|value| value.is_truthy())
                } else {
                    None
                };
                let only_process_id = just_this_one.and(target_process_id);
                let run_timers =
                    just_this_one.is_none_or(|value| !matches!(value.kind(), Kind::Integer(_)));
                // A READ_KBD 0 wait: it never selects the keyboard-class
                // notification descriptor, even when a callback nests it
                // inside a keyboard read.
                let previous_wait = interp.set_waiting_for_user_input(false);
                let wait_result = wait_pumping_processes(
                    interp,
                    env,
                    timeout,
                    true,
                    target_process_id,
                    only_process_id,
                    run_timers,
                );
                interp.set_waiting_for_user_input(previous_wait);
                let delivered = wait_result?;
                Ok(if delivered { Value::T } else { Value::Nil })
            }
            "input-pending-p" => {
                need_arg_range(name, args, 0, 1)?;
                // keyboard.c's process_special_events handles selection
                // events only: buffered file notifications wait for read_char
                // and the kernel notification queue is not read here at all
                // (the oracle answers nil and runs no callback for a watched
                // file written moments earlier).  A non-nil CHECK-TIMERS is
                // READABLE_EVENTS_DO_TIMERS_NOW; `sit-for' deliberately calls
                // `(input-pending-p t)', even for a zero-length no-redisplay
                // wait.
                if args.first().is_some_and(Value::is_truthy) {
                    interp.run_pending_timer_events(env)?;
                }
                Ok(
                    if unread_command_events(interp, env)?.is_empty()
                        && !crate::lisp::primitives::tty_input_pending()
                    {
                        Value::Nil
                    } else {
                        Value::T
                    },
                )
            }
            "discard-input" => {
                need_args(name, args, 0)?;
                if interp
                    .lookup_var("defining-kbd-macro", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    {
                        let state = &mut **interp;
                        state
                            .kbd_macro_definition
                            .truncate(state.kbd_macro_committed_len);
                    }
                    let last_macro = Value::vector(interp.kbd_macro_definition.iter().cloned());
                    interp.set_variable("last-kbd-macro", last_macro, env);
                }
                interp.set_variable("unread-command-events", Value::Nil, env);
                interp.set_variable("defining-kbd-macro", Value::Nil, env);
                crate::lisp::primitives::discard_tty_input();
                Ok(Value::Nil)
            }
            "prin1" => {
                need_arg_range(name, args, 1, 3)?;
                with_print_overrides(interp, env, args.get(2), |interp, env| {
                    let stream = printer_stream_value(interp, env, args.get(1));
                    print_object_to_stream(interp, &args[0], stream, env, true, false)
                })
            }
            "princ" => {
                if args.is_empty() {
                    return Ok(Value::Nil);
                }
                let stream = printer_stream_value(interp, env, args.get(1));
                print_object_to_stream(interp, &args[0], stream, env, false, false)
            }
            "print" => {
                if args.is_empty() {
                    return Ok(Value::Nil);
                }
                let stream = printer_stream_value(interp, env, args.get(1));
                print_object_to_stream(interp, &args[0], stream, env, true, true)
            }
            "terpri" => {
                need_arg_range(name, args, 0, 2)?;
                let stream = printer_stream_value(interp, env, args.first());
                if args.get(1).is_some_and(Value::is_truthy) {
                    if let Some(function) = stream.as_ref()
                        && super::call(interp, "functionp", std::slice::from_ref(function), env)?
                            .is_truthy()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("error".into()),
                            Value::String("Unsupported function argument".into()),
                            *function,
                        ])));
                    }
                    let noninteractive_stdout = interp
                        .lookup_var_key(cached_symbol!("noninteractive"), env)
                        .is_some_and(|value| value.is_truthy())
                        && stream
                            .as_ref()
                            .is_some_and(|value| matches!(value.kind(), Kind::T));
                    let at_line_start = if noninteractive_stdout {
                        interp.batch_standard_output_last_char == Some(u32::from(b'\n'))
                    } else {
                        printer_stream_at_line_start(interp, stream.as_ref())?
                    };
                    if at_line_start {
                        return Ok(Value::Nil);
                    }
                }
                write_printer_output(interp, "\n", stream.as_ref(), env)?;
                record_batch_standard_output_char(interp, stream.as_ref(), env, u32::from(b'\n'));
                Ok(Value::T)
            }
            "prin1-to-string" => {
                need_arg_range(name, args, 1, 3)?;
                let noescape = args.get(1).is_some_and(|value| value.is_truthy());
                prin1_to_string(interp, env, &args[0], noescape, args.get(2))
            }
            "write-char" => {
                need_arg_range(name, args, 1, 2)?;
                // print.c Fwrite_char: printchar of the character's code.
                format_char_conversion(&args[0])?;
                let code = args[0].as_integer()? as u32;
                let mut rendered = PrintOutput::default();
                rendered.push_code(code);
                let stream = printer_stream_value(interp, env, args.get(1));
                write_printer_output_codes(interp, &rendered, stream.as_ref(), env)?;
                record_batch_standard_output_char(interp, stream.as_ref(), env, code);
                Ok(args[0])
            }
            "redirect-debugging-output" => {
                need_arg_range(name, args, 1, 2)?;
                #[cfg(unix)]
                super::super::file_io::redirect_debugging_output(
                    interp,
                    &args[0],
                    args.get(1).is_some_and(Value::is_truthy),
                    env,
                )?;
                Ok(Value::Nil)
            }
            "external-debugging-output" => {
                need_args(name, args, 1)?;
                // print.c: CHECK_FIXNUM, then printchar_to_stream of the
                // code as an unsigned int.
                let Kind::Integer(code) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                };
                super::super::file_io::printchar_to_stderr(interp, code as u32, env)?;
                Ok(args[0])
            }
            "print--preprocess" => {
                need_args(name, args, 1)?;
                print_preprocess(interp, &args[0], env)
            }
            "yes-or-no-p" => {
                need_args(name, args, 1)?;
                ensure_interaction_allowed(interp, env)?;
                // fns.c:3521 Fyes_or_no_p.  The old arm answered t whenever
                // no unread event supplied a `y' or `n' -- silence became
                // consent (finding 11).  GNU never invents an answer: it
                // honors `use-short-answers', appends `yes-or-no-prompt',
                // and loops on `read-from-minibuffer' until the user types
                // yes or no; in batch, EOF on stdin signals end-of-file.
                if interp
                    .lookup_var("use-short-answers", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return crate::lisp::primitives::call_named_function(
                        interp, "y-or-n-p", args, env,
                    );
                }
                let mut prompt = string_text(&args[0])?;
                if !prompt.is_empty() && !prompt.ends_with(char::is_whitespace) {
                    prompt.push(' ');
                }
                let suffix = interp
                    .lookup_var("yes-or-no-prompt", env)
                    .and_then(|value| string_like(&value).map(|text| text.text))
                    .unwrap_or_else(|| "(yes or no) ".into());
                prompt.push_str(&suffix);
                loop {
                    let answer = super::call(
                        interp,
                        "read-from-minibuffer",
                        &[
                            Value::String(prompt.clone().into()),
                            Value::Nil,
                            Value::Nil,
                            Value::Nil,
                            Value::symbol("yes-or-no-p-history"),
                        ],
                        env,
                    )?;
                    let answer = string_like(&answer)
                        .map(|text| text.text.to_lowercase())
                        .unwrap_or_default();
                    match answer.as_str() {
                        "yes" => return Ok(Value::T),
                        "no" => return Ok(Value::Nil),
                        _ => {}
                    }
                    let _ = super::call(interp, "ding", &[Value::Nil], env);
                    let _ = super::call(interp, "discard-input", &[], env);
                    let _ = crate::lisp::primitives::call_named_function(
                        interp,
                        "message",
                        &[Value::String("Please answer yes or no.".into())],
                        env,
                    );
                    let _ = super::call(interp, "sleep-for", &[Value::Integer(2)], env);
                }
            }

            // ── More string/char ops ──
            "char-equal" => {
                need_args(name, args, 2)?;
                let a = args[0].as_integer()?;
                let b = args[1].as_integer()?;
                let case_fold = interp
                    .lookup_var("case-fold-search", env)
                    .map(|v| v.is_truthy())
                    .unwrap_or(false);
                let eq = if case_fold {
                    a == b || (a as u8 as char).eq_ignore_ascii_case(&(b as u8 as char))
                } else {
                    a == b
                };
                Ok(if eq { Value::T } else { Value::Nil })
            }
            "key-description" => {
                need_arg_range(name, args, 1, 2)?;
                key_description(
                    interp,
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    env,
                )
            }
            "single-key-description" => {
                need_arg_range(name, args, 1, 2)?;
                let no_angles = args.get(1).is_some_and(Value::is_truthy);
                single_key_description(interp, args[0], no_angles, env)
            }
            "text-char-description" => {
                need_args(name, args, 1)?;
                text_char_description(interp, &args[0], env)
            }

            // ── More buffer ops ──
            "following-char" => direct_following_char(interp, args, env),
            "preceding-char" => direct_preceding_char(interp, args, env),
            "buffer-last-name" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c: BVAR (decode_buffer (BUFFER), last_name), the
                // name a buffer was made, renamed or killed with.
                Ok(interp
                    .decode_buffer(args.first())?
                    .borrow()
                    .last_name
                    .clone()
                    .map_or(Value::Nil, |name| Value::String(name.into())))
            }

            // ── Display stubs ──
            "window-system" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "tty-type" => {
                need_arg_range(name, args, 0, 1)?;
                let terminal = require_live_terminal(interp, args.first())?;
                Ok(terminal
                    .borrow()
                    .kind
                    .as_deref()
                    .map(Value::string)
                    .unwrap_or(Value::Nil))
            }
            "tty-top-frame" => {
                need_arg_range(name, args, 0, 1)?;
                let terminal = require_live_terminal(interp, args.first())?;
                let terminal = terminal.borrow();
                Ok(if terminal.kind.is_some() {
                    terminal.top_frame.map(Value::Frame).unwrap_or(Value::Nil)
                } else {
                    Value::Nil
                })
            }
            "controlling-tty-p" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(Value::Nil)
            }
            "tty-display-color-p" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(if interp.tty_display_color_cells() > 0 {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "tty-display-color-cells" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(Value::Integer(interp.tty_display_color_cells()))
            }
            "tty-no-underline" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(Value::Nil)
            }
            "suspend-tty" | "resume-tty" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Err(LispError::Signal(format!(
                    "Attempt to {} a non-text terminal device",
                    if name == "suspend-tty" {
                        "suspend"
                    } else {
                        "resume"
                    }
                )))
            }
            "tty--output-buffer-size" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Err(LispError::Signal("Not a tty terminal".into()))
            }
            "tty--set-output-buffer-size" => {
                need_arg_range(name, args, 1, 2)?;
                if !matches!(args[0].kind(), Kind::Integer(size) if size >= 0) {
                    return Err(LispError::Signal("Invalid output buffer size".into()));
                }
                require_live_terminal(interp, args.get(1))?;
                Err(LispError::Signal(
                    "Attempt to suspend a non-text terminal device".into(),
                ))
            }
            // Batch sessions have no color support (GNU: nil / 0).

            // xfaces.c Fdisplay_supports_face_attributes_p: batch gives
            // up before frames exist (noninteractive -> nil); a live tty
            // frame dispatches to tty_supports_face_attributes_p.
            "display-supports-face-attributes-p" => {
                need_arg_range(name, args, 1, 2)?;
                if interp.tty_display_color_cells() <= 0 {
                    return Ok(Value::Nil);
                }
                tty_supports_face_attributes(interp, env, &args[0])
            }
            "internal-char-font" => {
                need_arg_range(name, args, 1, 2)?;
                if args[0].is_nil() {
                    let character = args.get(1).unwrap_or(&Value::Nil);
                    // font.c:Finternal_char_font uses CHECK_CHARACTER, not
                    // Rust's Unicode scalar range (GNU also accepts surrogates
                    // and characters up to character.h:MAX_CHAR).
                    let valid = matches!(character.kind(), Kind::Integer(codepoint)
                        if (0..=0x3f_ffff).contains(&codepoint));
                    if !valid {
                        return Err(LispError::WrongTypeArgument(
                            "characterp".into(),
                            *character,
                        ));
                    }
                } else {
                    let position = position_from_value(interp, &args[0])?;
                    if position < interp.buffer.borrow().point_min()
                        || position >= interp.buffer.borrow().point_max()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("args-out-of-range".into()),
                            args[0],
                            Value::Integer(interp.buffer.borrow().point_min() as i64),
                            Value::Integer(interp.buffer.borrow().point_max() as i64),
                        ])));
                    }
                    if let Some(character) = args.get(1)
                        && !character.is_nil()
                    {
                        // With POSITION, GNU uses CHECK_FIXNAT instead.
                        // Values outside CHAR_VALID_P subsequently return nil.
                        let valid = matches!(character.kind(), Kind::Integer(codepoint)
                            if codepoint >= 0);
                        if !valid {
                            return Err(LispError::WrongTypeArgument(
                                "wholenump".into(),
                                *character,
                            ));
                        }
                    }
                }
                // terminal.c:terminal_glyph_code returns nil for terminals
                // without glyph-code reporting (including the initial terminal
                // and the pinned Darwin build). Linux console glyph tables and
                // graphical font selection remain separate, unfinished paths.
                Ok(Value::Nil)
            }
            "current-input-mode" => {
                need_args(name, args, 0)?;
                Ok(Value::list([
                    if interp.input_interrupt_mode() {
                        Value::T
                    } else {
                        Value::Nil
                    },
                    Value::Nil,
                    Value::T,
                    Value::Integer(7),
                ]))
            }
            "set-input-interrupt-mode" => {
                need_args(name, args, 1)?;
                interp.set_input_interrupt_mode(args[0].is_truthy());
                Ok(Value::Nil)
            }
            "set-output-flow-control" | "set-input-meta-mode" => {
                need_arg_range(name, args, 1, 2)?;
                // GNU's batch frame has no decoded tty terminal, so these
                // terminal-local setters are successful no-ops.
                Ok(Value::Nil)
            }
            "set-input-mode" => {
                need_arg_range(name, args, 3, 4)?;
                interp.set_input_interrupt_mode(args[0].is_truthy());
                Ok(Value::Nil)
            }
            "set-quit-char" => {
                need_args(name, args, 1)?;
                // No controlling tty in batch mode; GNU returns before
                // validating or changing its tty-local quit character.
                Ok(Value::Nil)
            }

            "menu-or-popup-active-p" => {
                need_args(name, args, 0)?;
                // A headless Emaxx process cannot have an active native menu.
                Ok(Value::Nil)
            }
            "imagep" => {
                need_args(name, args, 1)?;
                Ok(if valid_image_spec(interp, &args[0], env) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "clear-image-cache" => {
                need_arg_range(name, args, 0, 2)?;
                if let Some(animation_cache) = args.get(1).filter(|value| !value.is_nil()) {
                    if animation_cache.cons_values().is_none() {
                        return Err(wrong_type_argument("consp", *animation_cache));
                    }
                    return Ok(Value::Nil);
                }
                let filter = args.first().cloned().unwrap_or(Value::Nil);
                if filter.is_nil()
                    || matches!(filter.kind(), Kind::Frame(id) if interp.frame_is_live(id))
                {
                    return Err(LispError::Signal(
                        "Window system frame should be used".into(),
                    ));
                }
                Ok(Value::Nil)
            }
            "image-cache-size" => {
                need_args(name, args, 0)?;
                // The single batch/TTY frame has no native image cache.
                Ok(Value::Integer(0))
            }
            "image-flush" => {
                need_arg_range(name, args, 1, 2)?;
                if !valid_image_spec(interp, &args[0], env) {
                    return Err(LispError::Signal("Invalid image specification".into()));
                }
                if matches!(args.get(1).map(|v| v.kind()), Some(Kind::T)) {
                    return Ok(Value::Nil);
                }
                Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ))
            }
            "image-transforms-p" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "image-size" | "image-mask-p" | "image-metadata" => Err(LispError::Signal(
                "Images are unavailable on a nongraphical display".into(),
            )),

            "init-image-library" => {
                need_args(name, args, 1)?;
                let image_type = args[0].as_symbol()?;
                Ok(
                    if matches!(
                        image_type,
                        "pbm" | "png" | "jpeg" | "gif" | "svg" | "xbm" | "xpm" | "webp" | "tiff"
                    ) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "window-start" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Integer(window_start(interp, args.first())? as i64))
            }
            "window-end" => {
                need_arg_range(name, args, 0, 2)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                let z = buffer_z(interp, buffer_id);
                // xdisp.c Fwindow_end: UPDATE on a real frame displays the
                // window's body from its start; otherwise the answer is Z
                // less the end the last redisplay recorded (none before
                // one, so a batch session answers Z).
                if args.get(1).is_some_and(Value::is_truthy)
                    && interactive_window_metrics().is_some()
                    && !interp
                        .lookup_var_key(cached_symbol!("noninteractive"), env)
                        .is_some_and(|value| value.is_truthy())
                {
                    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
                    let start = window_start_marker(interp, window_id, buffer_id)
                        .clamp(point_min, point_max);
                    let height = (window_geometry(interp, window_id).1
                        - window_non_body_height(interp, buffer_id, env))
                    .max(1) as isize;
                    let saved = interp.current_buffer_id();
                    interp.set_current_buffer_id(buffer_id)?;
                    let (row, shortage) = crate::lisp::primitives::window::move_screen_lines_in(
                        interp, env, window_id, start, height,
                    );
                    interp.set_current_buffer_id(saved)?;
                    // Fewer rows than the window holds: move_it_past_eol
                    // takes the end past the last line, to ZV.
                    let end = if shortage > 0 { point_max } else { row };
                    return Ok(Value::Integer(end as i64));
                }
                let end_pos = window_slot_value(interp, window_id, WINDOW_END_POS_SLOT)
                    .integer_value()
                    .unwrap_or(0);
                Ok(Value::Integer(z as i64 - end_pos))
            }
            "window-point" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let buffer_id = window_buffer_id(interp, &interp.record_value(window_id))
                    .ok_or_else(|| LispError::TypeError("window".into(), "deleted".into()))?;
                let point = if window_id == interp.selected_window_id() {
                    if buffer_id == interp.current_buffer_id() {
                        interp.buffer.borrow().point()
                    } else {
                        interp
                            .get_buffer_by_id(buffer_id)
                            .map(|buffer| buffer.point())
                            .unwrap_or(1)
                    }
                } else {
                    window_slot_value(interp, window_id, WINDOW_POINT_SLOT)
                        .as_integer()
                        .unwrap_or(1)
                        .max(1) as usize
                };
                Ok(Value::Integer(point as i64))
            }
            "window-hscroll" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
                    .as_integer()
                    .map(Value::Integer)
                    .unwrap_or(Value::Integer(0)))
            }
            "window-vscroll" => {
                need_arg_range(name, args, 0, 2)?;
                live_window_id_or_selected(interp, args.first())?;
                // A terminal frame has no smooth pixel scrolling; GNU returns
                // zero in both canonical-line and pixel modes.
                Ok(Value::Integer(0))
            }
            "set-window-hscroll" => {
                need_args(name, args, 2)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let Kind::Integer(requested) = args[1].kind() else {
                    return Err(wrong_type_argument("fixnump", args[1]));
                };
                set_window_hscroll_value(interp, window_id, requested)
            }
            "scroll-left" | "scroll-right" => {
                need_arg_range(name, args, 0, 2)?;
                let window_id = live_window_id_or_selected(interp, None)?;
                let requested = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => window_text_width_columns(interp, window_id) - 2,
                    Some(value) => prefix_numeric_value(&value.value())?.as_integer()?,
                };
                let current = window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
                    .as_integer()
                    .unwrap_or(0);
                let requested = if name == "scroll-left" {
                    current.saturating_add(requested)
                } else {
                    current.saturating_sub(requested)
                };
                let result = set_window_hscroll_value(interp, window_id, requested)?;
                if args.get(1).is_some_and(Value::is_truthy) {
                    set_window_slot_value(interp, window_id, WINDOW_MIN_HSCROLL_SLOT, result)?;
                }
                Ok(result)
            }
            "other-window-for-scrolling" => {
                need_args(name, args, 0)?;
                let selected_id = live_window_id_or_selected(interp, None)?;
                let selected_kind = window_slot_value(interp, selected_id, WINDOW_KIND_SLOT);
                let mut candidate = if matches!(
                    selected_kind.kind(),
                    Kind::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND
                ) {
                    interp
                        .lookup_var("minibuffer-scroll-window", env)
                        .filter(Value::is_truthy)
                } else {
                    None
                };

                if candidate.is_none()
                    && let Some(buffer @ Kind::Buffer(_)) = interp
                        .lookup_var("other-window-scroll-buffer", env)
                        .map(|v| v.kind())
                    && let Kind::Buffer(buffer_value) = buffer
                    && interp.has_buffer_id(buffer_value.id)
                {
                    candidate = live_ordinary_window_ids(interp)
                        .into_iter()
                        .find(|window_id| {
                            window_buffer_id(interp, &interp.record_value(*window_id))
                                == Some(buffer_value.id)
                        })
                        .map(|id| interp.record_value(id));
                    if candidate.is_none() {
                        candidate = Some(call_function_value(
                            interp,
                            &Value::symbol("display-buffer"),
                            &[buffer.value(), Value::T],
                            env,
                        )?);
                    }
                }

                if candidate.is_none()
                    && let Some(default) = interp
                        .lookup_var("other-window-scroll-default", env)
                        .filter(Value::is_truthy)
                    && crate::lisp::primitives::call(
                        interp,
                        "functionp",
                        std::slice::from_ref(&default),
                        env,
                    )?
                    .is_truthy()
                {
                    candidate = Some(call_function_value(interp, &default, &[], env)?);
                }

                let candidate = candidate.unwrap_or_else(|| {
                    let ids = live_ordinary_window_ids(interp);
                    let index = ids.iter().position(|id| *id == selected_id).unwrap_or(0);
                    ids.get((index + 1) % ids.len())
                        .copied()
                        .map(|id| interp.record_value(id))
                        .unwrap_or_else(|| interp.selected_window_value())
                });
                let candidate_id = live_window_id_or_selected(interp, Some(&candidate))?;
                if candidate_id == selected_id {
                    return Err(LispError::Signal("There is no other window".into()));
                }
                Ok(interp.record_value(candidate_id))
            }
            "set-window-margins" => {
                need_arg_range(name, args, 2, 3)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let changed = set_window_margins(
                    interp,
                    window_id,
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )?;
                Ok(if changed { Value::T } else { Value::Nil })
            }
            "window-margins" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let (left, right) = interp.window_margins(window_id);
                let to_value =
                    |margin: Option<i64>| margin.map(Value::Integer).unwrap_or(Value::Nil);
                Ok(Value::cons(to_value(left), to_value(right)))
            }
            "coordinates-in-window-p" => {
                need_args(name, args, 2)?;
                let window_id = live_window_id_or_selected(interp, Some(&args[1]))?;
                let x = args[0].car()?.as_float()?.trunc() as i64;
                let y = args[0].cdr()?.as_float()?.trunc() as i64;
                let (width, height, left, top) = window_geometry(interp, window_id);
                if x < left || x >= left + width || y < top || y >= top + height {
                    return Ok(Value::Nil);
                }
                let local_x = x - left;
                let local_y = y - top;
                let buffer_id = window_buffer_id(interp, &interp.record_value(window_id))
                    .ok_or_else(|| wrong_type_argument("window-live-p", args[1]))?;
                let tab_line = window_line_height(interp, buffer_id, "tab-line-format", env);
                let header_line = window_line_height(interp, buffer_id, "header-line-format", env);
                let mode_line = window_line_height(interp, buffer_id, "mode-line-format", env);
                if tab_line > 0 && local_y < tab_line {
                    return Ok(Value::symbol("tab-line"));
                }
                if header_line > 0 && local_y < tab_line + header_line {
                    return Ok(Value::symbol("header-line"));
                }
                if mode_line > 0 && local_y >= height - mode_line {
                    return Ok(Value::symbol("mode-line"));
                }
                let (left_margin, right_margin) = interp.window_margins(window_id);
                let left_margin = left_margin.unwrap_or(0);
                let right_margin = right_margin.unwrap_or(0);
                if local_x < left_margin {
                    return Ok(Value::symbol("left-margin"));
                }
                if local_x >= width - right_margin {
                    return Ok(Value::symbol("right-margin"));
                }
                Ok(Value::cons(
                    Value::Integer(local_x - left_margin),
                    Value::Integer(local_y),
                ))
            }
            "pos-visible-in-window-p" => {
                need_arg_range(name, args, 0, 3)?;
                if interp
                    .lookup_var_key(cached_symbol!("noninteractive"), env)
                    .is_some_and(|value| !value.is_nil())
                {
                    return Ok(Value::Nil);
                }
                let window = args.get(1).filter(|value| !value.is_nil());
                let window_id = live_window_id_or_selected(interp, window)?;
                let buffer_id = if let Some(window) = window {
                    window_buffer_id(interp, window)
                        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), *window))?
                } else {
                    interp.selected_window_buffer_id()
                };
                let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
                let pos = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => interp.buffer.borrow().point(),
                    Some(Kind::T) => point_max,
                    Some(value) => position_from_value(interp, &value.value())?,
                };
                let start = window_start(interp, window)?;
                let first_visible = start.max(point_min);
                // GNU walks DISPLAY lines from the window start (its
                // start_display iterator): invisible text collapses, so
                // the end of a folded org subtree sits a couple of rows
                // down, not the raw line count away —
                // org-subtree-end-visible-p decides whether org-cycle
                // recenters on exactly this answer.
                // The global interactive metrics describe only the last
                // redrawn selected window.  Help asks about its newly split,
                // non-selected window before the next redisplay; reusing the
                // old selected height falsely declared Help's point-max
                // visible and changed help-window-display-message.  The live
                // window tree already owns the queried window's exact body
                // height.
                let limit = (window_geometry(interp, window_id).1
                    - window_non_body_height(interp, buffer_id, env))
                .max(1) as usize;
                let rows = {
                    let saved_buffer = interp.current_buffer_id();
                    let switched = saved_buffer != buffer_id
                        && interp.set_current_buffer_id(buffer_id).is_ok();
                    let mut rows = 0usize;
                    let mut cursor = first_visible;
                    while rows < limit && pos >= first_visible {
                        let (next, remaining) = crate::lisp::primitives::window::move_screen_lines(
                            interp, env, cursor, 1,
                        );
                        if remaining != 0 || next <= cursor || pos < next {
                            break;
                        }
                        cursor = next;
                        rows += 1;
                    }
                    if switched {
                        let _ = interp.set_current_buffer_id(saved_buffer);
                    }
                    rows
                };
                let visible = pos >= first_visible && pos <= point_max && rows < limit;
                if !visible {
                    return Ok(Value::Nil);
                }
                // With PARTIALLY non-nil GNU answers a list (X Y [RTOP RBOT
                // ROWH VPOS]) — two elements for a fully visible position.
                // Text-terminal frames measure pixels in character cells, so
                // X is the display column and Y the window row; simple.el's
                // line-move-partial consumes exactly this shape.
                if args.get(2).is_some_and(Value::is_truthy) {
                    let saved = interp.buffer.borrow().point();
                    interp.buffer.borrow_mut().goto_char(pos);
                    let x = super::call(interp, "current-column", &[], env)
                        .ok()
                        .and_then(|value| value.integer_value())
                        .unwrap_or(0);
                    interp.buffer.borrow_mut().goto_char(saved);
                    return Ok(Value::list([
                        Value::Integer(x),
                        Value::Integer(rows as i64),
                    ]));
                }
                Ok(Value::T)
            }
            "window-total-width" | "window-total-height" => {
                // window.c:Fwindow_total_width, Fwindow_total_height: the
                // total size, or with ROUND `floor' or `ceiling' the pixel
                // size in units (a text frame's unit is one pixel).
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let horizontal = name == "window-total-width";
                let pixel_rounded = args.get(1).is_some_and(|round| {
                    matches!(round.kind(), Kind::Symbol(ref round) if round == "floor" || round == "ceiling")
                });
                Ok(Value::Integer(if pixel_rounded {
                    window_pixel_size(interp, window_id, horizontal)
                } else {
                    let (cols, lines, _, _) = window_char_geometry(interp, window_id);
                    if horizontal { cols } else { lines }
                }))
            }
            "window-use-time" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(interp.window_use_time(window_id)))
            }
            "window-bump-use-time" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(interp
                    .bump_window_use_time(window_id)
                    .map(Value::Integer)
                    .unwrap_or(Value::Nil))
            }
            "window-body-width" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(window_text_width_columns(interp, window_id)))
            }
            "window-body-height" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(
                    window_geometry(interp, window_id).1
                        - window_non_body_height(interp, buffer_id, env),
                ))
            }
            "window-text-width" => {
                need_arg_range(name, args, 0, 2)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(window_text_width_columns(interp, window_id)))
            }
            "window-text-height" => {
                need_arg_range(name, args, 0, 2)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(
                    window_geometry(interp, window_id).1
                        - window_non_body_height(interp, buffer_id, env),
                ))
            }
            "window-line-height" => {
                need_arg_range(name, args, 0, 2)?;
                live_window_id_or_selected(interp, args.get(1))?;
                // GNU has no current glyph matrix in batch mode.
                Ok(Value::Nil)
            }
            "window-lines-pixel-dimensions" => {
                need_arg_range(name, args, 0, 6)?;
                live_window_id_or_selected(interp, args.first())?;
                // GNU has no current glyph matrix in batch mode.
                Ok(Value::Nil)
            }
            "window-pixel-width" | "window-pixel-height" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let geometry = window_geometry(interp, window_id);
                Ok(Value::Integer(if name == "window-pixel-width" {
                    geometry.0
                } else {
                    geometry.1
                }))
            }
            "window-pixel-left" | "window-pixel-top" | "window-left-column" | "window-top-line" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let (_, _, left, top) = if matches!(name, "window-pixel-left" | "window-pixel-top")
                {
                    window_geometry(interp, window_id)
                } else {
                    window_char_geometry(interp, window_id)
                };
                Ok(Value::Integer(
                    if matches!(name, "window-pixel-left" | "window-left-column") {
                        left
                    } else {
                        top
                    },
                ))
            }
            "window-old-pixel-width" | "window-old-pixel-height" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().copied().unwrap_or(Value::Nil);
                let window_id = valid_window_id(interp, &window)?;
                let slot = if name == "window-old-pixel-width" {
                    WINDOW_OLD_PIXEL_WIDTH_SLOT
                } else {
                    WINDOW_OLD_PIXEL_HEIGHT_SLOT
                };
                Ok(Value::Integer(window_integer_slot(interp, window_id, slot)))
            }
            "window-old-body-pixel-width" | "window-old-body-pixel-height" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let slot = if name == "window-old-body-pixel-width" {
                    WINDOW_OLD_BODY_PIXEL_WIDTH_SLOT
                } else {
                    WINDOW_OLD_BODY_PIXEL_HEIGHT_SLOT
                };
                Ok(Value::Integer(window_integer_slot(interp, window_id, slot)))
            }
            "window-new-pixel" | "window-new-total" | "window-new-normal" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = match name {
                    "window-new-pixel" => WINDOW_NEW_PIXEL_SLOT,
                    "window-new-total" => WINDOW_NEW_TOTAL_SLOT,
                    _ => WINDOW_NEW_NORMAL_SLOT,
                };
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(slot))
                    .cloned()
                    .unwrap_or(Value::Integer(0)))
            }
            "window-normal-size" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = if args.get(1).is_some_and(Value::is_truthy) {
                    WINDOW_NORMAL_WIDTH_SLOT
                } else {
                    WINDOW_NORMAL_HEIGHT_SLOT
                };
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(slot))
                    .cloned()
                    .unwrap_or(Value::float(1.0)))
            }
            "set-window-new-pixel" | "set-window-new-total" => {
                need_arg_range(name, args, 2, 3)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                let requested = args[1].as_integer()?;
                let slot = if name == "set-window-new-pixel" {
                    WINDOW_NEW_PIXEL_SLOT
                } else {
                    WINDOW_NEW_TOTAL_SLOT
                };
                let current = window_slot_value(interp, window_id, slot)
                    .as_integer()
                    .unwrap_or(0);
                let requested = if args.get(2).is_some_and(Value::is_truthy) {
                    current.saturating_add(requested)
                } else {
                    requested
                };
                if name == "set-window-new-pixel" && requested < 0 {
                    return Err(LispError::Signal(format!("Args out of range: {requested}")));
                }
                set_window_slot_value(interp, window_id, slot, Value::Integer(requested))
            }
            "set-window-new-normal" => {
                need_arg_range(name, args, 1, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(
                    interp,
                    window_id,
                    WINDOW_NEW_NORMAL_SLOT,
                    args.get(1).cloned().unwrap_or(Value::Nil),
                )
            }
            "window-resize-apply" => {
                // fset_redisplay: the frame's window tree changes.
                interp.redisplay_other_windows();
                need_arg_range(name, args, 0, 2)?;
                if let Some(frame) = args.first().filter(|frame| !frame.is_nil())
                    && !matches!(frame.kind(), Kind::Frame(id) if interp.frame_is_live(id))
                {
                    return Err(LispError::WrongTypeArgument("framep".into(), *frame));
                }
                // window.c:Fwindow_resize_apply: commit the sizes staged
                // in FRAME's window tree when they add up to its root's.
                let horflag = args.get(1).is_some_and(Value::is_truthy);
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                let root_id = frame.borrow().root_window_id();
                if !window_resize_check(interp, root_id, horflag)
                    || window_new_pixel(interp, root_id)
                        != window_pixel_size(interp, root_id, horflag)
                {
                    return Ok(Value::Nil);
                }
                window_resize_apply(interp, root_id, horflag)?;
                Ok(Value::T)
            }
            "window-resize-apply-total" => {
                need_arg_range(name, args, 0, 2)?;
                if let Some(frame) = args.first().filter(|frame| !frame.is_nil())
                    && !matches!(frame.kind(), Kind::Frame(id) if interp.frame_is_live(id))
                {
                    return Err(LispError::WrongTypeArgument("framep".into(), *frame));
                }
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                frame_size::window_resize_apply_total_for_frame(
                    interp,
                    frame,
                    args.get(1).is_some_and(Value::is_truthy),
                );
                Ok(Value::T)
            }
            "window-mode-line-height" | "window-header-line-height" | "window-tab-line-height" => {
                need_arg_range(name, args, 0, 1)?;
                // window.c window_wants_mode_line/header_line/tab_line all
                // require !MINI_WINDOW_P: a minibuffer window has none of
                // these lines whatever its buffer's format variables say.
                // decode_live_window.
                let window_id = live_window_id_or_selected(interp, args.first())?;
                if interp
                    .record_ref(window_id)
                    .is_some_and(|window| window_kind_is(window, MINIBUFFER_WINDOW_KIND))
                {
                    return Ok(Value::Integer(0));
                }
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                let format_variable = match name {
                    "window-mode-line-height" => "mode-line-format",
                    "window-header-line-height" => "header-line-format",
                    _ => "tab-line-format",
                };
                Ok(Value::Integer(window_line_height(
                    interp,
                    buffer_id,
                    format_variable,
                    env,
                )))
            }
            "window-right-divider-width"
            | "window-bottom-divider-width"
            | "window-scroll-bar-width"
            | "window-scroll-bar-height" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(window) = args.first() {
                    window_id_or_selected(interp, window)?;
                }
                Ok(Value::Integer(0))
            }
            "move-to-window-line" => {
                need_arg_range(name, args, 1, 1)?;
                // window.c Fmove_to_window_line.
                let window_id = interp.selected_window_id();
                if interp.selected_window_buffer_id() != interp.current_buffer_id() {
                    return Err(LispError::Signal(
                        "move-to-window-line called from unrelated buffer".into(),
                    ));
                }
                let height = selected_command_text_height(interp, env);
                let (point_min, point_max) = {
                    let buffer = interp.buffer.borrow();
                    (buffer.point_min(), buffer.point_max())
                };
                let start = current_window_start(interp);
                if start < point_min || start > point_max {
                    visual_vertical_motion(interp, env, window_id, -((height / 2) as i64), None)?;
                    let (start, bolp) = {
                        let buffer = interp.buffer.borrow();
                        let pt = buffer.point();
                        (
                            pt,
                            pt <= buffer.point_min() || buffer.char_at(pt - 1) == Some('\n'),
                        )
                    };
                    interp.set_selected_window_start(start);
                    set_window_flag(interp, window_id, WINDOW_START_AT_LINE_BEG_SLOT, bolp);
                    set_window_flag(interp, window_id, WINDOW_FORCE_START_SLOT, true);
                } else {
                    interp.buffer.borrow_mut().goto_char(start);
                }
                // displayed_window_lines: the window's rows, a window the
                // text does not fill counting one row fewer.
                let from = interp.buffer.borrow().point();
                let (_, shortage) = crate::lisp::primitives::window::move_screen_lines_in(
                    interp,
                    env,
                    window_id,
                    from,
                    height as isize,
                );
                let lines = if shortage > 1 {
                    height as i64 - 1
                } else {
                    height as i64
                };
                let target = match args[0].kind() {
                    Kind::Nil => lines / 2,
                    _ => {
                        let iarg = prefix_numeric_value(&args[0])?.as_integer()?;
                        if iarg < 0 { iarg + lines } else { iarg }
                    }
                };
                let moved = visual_vertical_motion(interp, env, window_id, target, None)?;
                Ok(Value::Integer(moved))
            }
            "recenter" => {
                need_arg_range(name, args, 0, 2)?;
                let height = selected_command_text_height(interp, env);
                let interactive = !interp
                    .lookup_var_key(cached_symbol!("noninteractive"), env)
                    .is_some_and(|value| value.is_truthy());
                crate::lisp::primitives::window::recenter_selected_window(
                    interp,
                    env,
                    args.first().copied().unwrap_or(Value::Nil),
                    args.get(1).is_some_and(Value::is_truthy),
                    height,
                    interactive,
                )?;
                Ok(Value::Nil)
            }
            "scroll-up" | "scroll-down" => {
                need_arg_range(name, args, 0, 1)?;
                // window.c scroll_command on the selected window.
                let direction: i64 = if name == "scroll-up" { 1 } else { -1 };
                let (n, whole) = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => (direction, true),
                    Some(Kind::Symbol(minus)) if minus == "-" => (-direction, true),
                    Some(value) => (
                        prefix_numeric_value(&value.value())?.as_integer()? * direction,
                        false,
                    ),
                };
                let height = selected_command_text_height(interp, env);
                let window_id = interp.selected_window_id();
                let total_lines = window_geometry(interp, window_id).1;
                let hscroll = window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
                    .as_integer()
                    .unwrap_or(0);
                crate::lisp::primitives::window::window_scroll(
                    interp,
                    env,
                    n,
                    whole,
                    false,
                    height,
                    total_lines,
                    hscroll,
                )?;
                Ok(Value::Nil)
            }
            "window-text-pixel-size" => {
                // (window-text-pixel-size &optional WINDOW FROM TO X-LIMIT
                //  Y-LIMIT MODE-LINES IGNORE-LINE-AT-END).  Without a graphical frame a cell
                // is the pixel unit: the widest line's character count and
                // the line count stand for the pixel size of WINDOW's
                // buffer text.  MODE-LINES non-nil adds the window's mode,
                // header, and tab lines — `fit-window-to-buffer' sizes the
                // whole window from that total.
                need_arg_range(name, args, 0, 7)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let buffer_id = window_buffer_id(interp, &interp.record_value(window_id))
                    .unwrap_or(interp.current_buffer_id());
                let text = if buffer_id == interp.current_buffer_id() {
                    interp.buffer.borrow().buffer_string()
                } else {
                    interp
                        .get_buffer_by_id(buffer_id)
                        .map(|buffer| buffer.buffer_string())
                        .unwrap_or_default()
                };
                let mut width = text
                    .lines()
                    .map(|line| line.chars().count())
                    .max()
                    .unwrap_or(0) as i64;
                if let Some(limit) = args.get(3).and_then(|limit| limit.integer_value()) {
                    width = width.min(limit);
                }
                let mut height = text.lines().count().max(1) as i64;
                if args.get(5).is_some_and(Value::is_truthy) {
                    for format_variable in
                        ["mode-line-format", "header-line-format", "tab-line-format"]
                    {
                        height += window_line_height(interp, buffer_id, format_variable, env);
                    }
                }
                if let Some(limit) = args.get(4).and_then(|limit| limit.integer_value()) {
                    height = height.min(limit);
                }
                Ok(Value::cons(Value::Integer(width), Value::Integer(height)))
            }
            "buffer-text-pixel-size" => {
                // (buffer-text-pixel-size &optional BUFFER-OR-NAME WINDOW
                // X-LIMIT Y-LIMIT).  Without a graphical frame there is no
                // font, so report the widest line's character count as the
                // pixel width and the line count as the pixel height (one
                // nominal unit per character).  Like GNU, honor `display'
                // replacements: a string spec substitutes for the covered
                // text and a margin spec removes it from the line flow.
                need_arg_range(name, args, 0, 4)?;
                let window_id = live_window_id_or_selected(interp, args.get(1))?;
                // xdisp.c: Fget_buffer (GNU reads no such buffer, or a
                // killed one, through XBUFFER regardless).
                let buffer_id = match args.first().filter(|value| !value.is_nil()) {
                    Some(buffer) => {
                        let buffer = interp.get_buffer_or_error(buffer)?;
                        interp
                            .live_buffer_id(buffer)
                            .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))?
                    }
                    None => interp.current_buffer_id(),
                };
                let saved_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let size = buffer_text_size(interp);
                interp.set_current_buffer_id(saved_buffer)?;
                // unwind_with_echo_area_buffer: WINDOW's points and start
                // come back through set_marker_restricted_both, inside its
                // buffer's accessible region (ERC's fill-wrap measures in a
                // narrowed buffer and so moves the start to BEGV).
                if let Some(window_buffer) =
                    window_buffer_id(interp, &interp.record_value(window_id))
                {
                    let (point_min, point_max) = buffer_point_bounds(interp, window_buffer);
                    let start = window_start_marker(interp, window_id, window_buffer);
                    set_window_start_marker(interp, window_id, start.clamp(point_min, point_max));
                    for slot in [WINDOW_POINT_SLOT, WINDOW_OLD_POINT_SLOT] {
                        if let Some(point) =
                            window_slot_value(interp, window_id, slot).integer_value()
                        {
                            let clamped = (point.max(1) as usize).clamp(point_min, point_max);
                            set_window_slot_value(
                                interp,
                                window_id,
                                slot,
                                Value::Integer(clamped as i64),
                            )?;
                        }
                    }
                }
                Ok(size)
            }
            "get-display-property" => {
                need_args(name, args, 2)?;
                let pos = args[0].as_integer()?.max(0) as usize;
                let property = args[1].as_symbol()?;
                let display = interp
                    .buffer
                    .borrow()
                    .text_property_at(pos, "display")
                    .unwrap_or(Value::Nil);
                Ok(display_property_value(&display, property).unwrap_or(Value::Nil))
            }
            "bidi-find-overridden-directionality" => {
                need_arg_range(name, args, 3, 4)?;
                // xdisp.c: OBJECT a string, or a buffer (a window's, nil
                // for the current one); a unibyte one, or a buffer without
                // `bidi-display-reordering', has nothing overridden.  A
                // string's FROM and TO are validate_subarray's, a buffer's
                // validate_region's in that buffer, and FROM at the end
                // finds nothing.
                let object = args[2];
                let right_to_left =
                    args.get(3).and_then(Value::symbol_str) == Some("right-to-left");
                if let Kind::StringObject(state) = object.kind() {
                    let (text, multibyte, length) = {
                        let state = state.borrow();
                        (state.text(), state.is_multibyte(), state.len())
                    };
                    if !multibyte {
                        return Ok(Value::Nil);
                    }
                    let (from, to) = validate_subarray(object, args[0], args[1], length)?;
                    if from >= length {
                        return Ok(Value::Nil);
                    }
                    return Ok(find_bidi_override(&text, 0, from, to, right_to_left)
                        .map(|pos| Value::Integer(pos as i64))
                        .unwrap_or(Value::Nil));
                }
                let buffer_id = match object.kind() {
                    Kind::Nil => interp.current_buffer_id(),
                    Kind::Buffer(_) => interp.resolve_buffer_id(&object)?,
                    _ if crate::lisp::primitives::window::window_buffer_id(interp, &object)
                        .is_some() =>
                    {
                        crate::lisp::primitives::window::window_buffer_id(interp, &object)
                            .expect("a live window's buffer")
                    }
                    _ => return Err(wrong_type_argument("stringp", object)),
                };
                let saved = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let result = (|| {
                    if !interp
                        .lookup_var("bidi-display-reordering", env)
                        .is_some_and(|value| value.is_truthy())
                        || !interp.buffer.borrow().is_multibyte()
                    {
                        return Ok(Value::Nil);
                    }
                    let (from, to) = validate_region(interp, &args[0], &args[1])?;
                    let buffer = interp.buffer.borrow();
                    if from >= buffer.point_max() {
                        return Ok(Value::Nil);
                    }
                    // The paragraph starts after the newline before FROM.
                    let mut paragraph = from;
                    while paragraph > buffer.point_min()
                        && buffer.char_at(paragraph - 1) != Some('\n')
                    {
                        paragraph -= 1;
                    }
                    let text = buffer
                        .buffer_substring(paragraph, to)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                    Ok(
                        find_bidi_override(&text, paragraph, from, to, right_to_left)
                            .map(|pos| Value::Integer(pos as i64))
                            .unwrap_or(Value::Nil),
                    )
                })();
                interp.set_current_buffer_id(saved)?;
                result
            }
            "current-bidi-paragraph-direction" => {
                need_arg_range(name, args, 0, 1)?;
                let Some(buffer) = args.first().filter(|value| !value.is_nil()) else {
                    return Ok(current_bidi_paragraph_direction_value(interp, env));
                };
                let Kind::Buffer(buffer_value) = buffer.kind() else {
                    return Err(wrong_type_argument("bufferp", *buffer));
                };
                let buffer_id = buffer_value.id;
                if !interp.has_buffer_id(buffer_id) {
                    return Err(wrong_type_argument("bufferp", *buffer));
                }
                let saved_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let direction = current_bidi_paragraph_direction_value(interp, env);
                interp.set_current_buffer_id(saved_buffer)?;
                Ok(direction)
            }
            "bidi-resolved-levels" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(vpos) = args.first().filter(|value| !value.is_nil()) {
                    vpos.as_integer()?;
                }
                // Emaxx's headless renderer intentionally retains no glyph
                // matrix between evaluations, exactly the stale-display case
                // for which GNU documents a nil result.
                Ok(Value::Nil)
            }
            "line-pixel-height" => {
                need_args(name, args, 0)?;
                // The canonical terminal cell is one pixel high in the
                // headless frame used by both compatibility runners.
                Ok(Value::Integer(1))
            }
            "display--line-is-continued-p" => {
                need_args(name, args, 0)?;
                with_selected_window_buffer(interp, |interp| {
                    let original_point = interp.buffer.borrow().point();
                    let result = (|| -> Result<Value, LispError> {
                        super::call(interp, "vertical-motion", &[Value::Integer(0)], env)?;
                        let screen_line_start = interp.buffer.borrow().point();
                        let moved =
                            super::call(interp, "vertical-motion", &[Value::Integer(1)], env)?
                                .as_integer()?;
                        let next_screen_line = interp.buffer.borrow().point();
                        let crosses_logical_line = (screen_line_start..next_screen_line)
                            .any(|position| interp.buffer.borrow().char_at(position) == Some('\n'));
                        Ok(
                            if moved == 1
                                && next_screen_line < interp.buffer.borrow().point_max()
                                && !crosses_logical_line
                            {
                                Value::T
                            } else {
                                Value::Nil
                            },
                        )
                    })();
                    interp.buffer.borrow_mut().goto_char(original_point);
                    result
                })
            }
            "move-point-visually" => {
                need_args(name, args, 1)?;
                let direction = args[0].as_integer()?;
                with_selected_window_buffer(interp, |interp| {
                    let right = direction > 0;
                    let right_to_left = matches!(
                        current_bidi_paragraph_direction_value(interp, env).kind(),
                        Kind::Symbol(ref direction) if direction == "right-to-left"
                    );
                    let logical_forward = right != right_to_left;
                    let point = interp.buffer.borrow().point();
                    let target = if logical_forward {
                        if point >= interp.buffer.borrow().point_max() {
                            return Err(signal_condition("end-of-buffer"));
                        }
                        point + 1
                    } else {
                        if point <= interp.buffer.borrow().point_min() {
                            return Err(signal_condition("beginning-of-buffer"));
                        }
                        point - 1
                    };
                    interp.buffer.borrow_mut().goto_char(target);
                    Ok(Value::Integer(target as i64))
                })
            }
            "long-line-optimizations-p" => {
                need_args(name, args, 0)?;
                // GNU only raises this redisplay-owned flag while maintaining a
                // glyph matrix.  The cache-free headless renderer never does.
                Ok(Value::Nil)
            }
            "tab-bar-height" | "tool-bar-height" => {
                need_arg_range(name, args, 0, 2)?;
                decode_live_frame(interp, args.first(), true)?;
                // The bootstrap terminal frame has no tab/tool-bar window.
                Ok(Value::Integer(0))
            }
            "lookup-image-map" => {
                need_args(name, args, 3)?;
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                let x = args[1].as_integer()?;
                let y = args[2].as_integer()?;
                Ok(lookup_image_map(&args[0], x, y))
            }
            "redisplay" => {
                need_arg_range(name, args, 0, 1)?;
                // `sit-for 0' redisplays instead of entering read-event, but
                // GNU still dispatches timers that were already ripe.
                interp.run_pending_timer_events(env)?;
                // A live frontend repaints through its redraw hook
                // (sit-for redisplays before waiting); the cache-free
                // batch renderer cannot be preempted by pending terminal
                // input, so redisplay always completes.  Fredisplay is
                // redisplay_preserve_echo_area: a message wiped by input
                // arrival but still last-displayed shows again instead
                // of clearing the row (menu-bar-open relies on it).
                with_preserved_echo_redisplay(|| {
                    crate::lisp::primitives::run_tty_frame_redraw(interp, env)
                });
                Ok(Value::T)
            }
            "redraw-frame" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                // GNU clears glyph matrices and immediately recomputes them.
                // Emaxx retains no glyph matrix between batch evaluations.
                Ok(Value::Nil)
            }
            "redraw-display" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "display--update-for-mouse-movement" => {
                need_args(name, args, 2)?;
                for coordinate in args {
                    if !matches!(coordinate.kind(), Kind::Integer(_)) {
                        return Err(wrong_type_argument("fixnump", *coordinate));
                    }
                }
                // Mouse-face/help-echo redisplay is absent from the headless
                // renderer, but the native fixnum surface remains observable.
                Ok(Value::Nil)
            }
            "open-termscript" => {
                need_args(name, args, 1)?;
                // GNU's batch bootstrap frame is output_initial, not termcap;
                // it rejects both a filename and nil before inspecting FILE.
                Err(LispError::Signal(
                    "Current frame is not on a tty device".into(),
                ))
            }
            "frame-or-buffer-changed-p" => {
                need_arg_range(name, args, 0, 1)?;
                frame_or_buffer_changed(interp, args.first(), env)
            }
            "internal-show-cursor" => {
                need_args(name, args, 2)?;
                let window_id = if args[0].is_nil() {
                    interp.selected_window_id()
                } else {
                    window_record_id_from_value(interp, &args[0])
                        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), args[0]))?
                };
                interp.set_window_cursor_visible(window_id, args[1].is_truthy());
                Ok(Value::Nil)
            }
            "internal-show-cursor-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => interp.selected_window_id(),
                    Some(window) => window_record_id_from_value(interp, &window.value())
                        .ok_or_else(|| {
                            LispError::WrongTypeArgument("windowp".into(), window.value())
                        })?,
                };
                Ok(if interp.window_cursor_visible(window_id) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-buffer-redisplay" => {
                need_args(name, args, 4)?;
                // GNU's xdisp.c primitive invalidates redisplay caches.  Emaxx's
                // batch renderer has no retained glyph cache, so the observable
                // watcher contract is the arity and nil return value.
                Ok(Value::Nil)
            }
            "color-distance" => {
                need_arg_range(name, args, 2, 4)?;
                // xfaces.c Fcolor_distance: a (R G B) list of fixnums
                // (parse_rgb_list, stored into unsigned shorts) or a string
                // through the tty frame's defined_color_hook; anything else
                // is (error "Invalid color" COLOR).  METRIC, when given, is
                // called with the two color lists; otherwise the distance
                // is Riemersma's metric in 64-bit arithmetic.
                let mut color = |value: &Value| -> Result<[u16; 3], LispError> {
                    let rgb = match value.to_vec() {
                        Ok(items) if value.cons_values().is_some() => match items
                            .as_slice()
                            .iter()
                            .map(|v| v.kind())
                            .collect::<Vec<_>>()
                            .as_slice()
                        {
                            [Kind::Integer(r), Kind::Integer(g), Kind::Integer(b), ..] => {
                                Some([*r as u16, *g as u16, *b as u16])
                            }
                            _ => None,
                        },
                        _ => match string_like(value) {
                            Some(text) => super::faces::tty_defined_color(interp, env, &text.text)?,
                            None => None,
                        },
                    };
                    rgb.ok_or_else(|| {
                        LispError::SignalValue(Value::list([
                            Value::symbol("error"),
                            Value::string("Invalid color"),
                            *value,
                        ]))
                    })
                };
                let left = color(&args[0])?;
                let right = color(&args[1])?;
                let as_list = |[r, g, b]: [u16; 3]| {
                    Value::list([
                        Value::Integer(i64::from(r)),
                        Value::Integer(i64::from(g)),
                        Value::Integer(i64::from(b)),
                    ])
                };
                if let Some(metric) = args.get(3).filter(|metric| !metric.is_nil()) {
                    return interp.call_function_value(
                        *metric,
                        metric.symbol_str(),
                        &[as_list(left), as_list(right)],
                        env,
                    );
                }
                let (r, g, b) = (
                    i64::from(left[0]) - i64::from(right[0]),
                    i64::from(left[1]) - i64::from(right[1]),
                    i64::from(left[2]) - i64::from(right[2]),
                );
                let r_mean = (i64::from(left[0]) + i64::from(right[0])) >> 1;
                let distance = ((((2 * 65536 + r_mean) * r * r) >> 16)
                    + 4 * g * g
                    + (((2 * 65536 + 65535 - r_mean) * b * b) >> 16))
                    >> 16;
                Ok(Value::Integer(distance))
            }
            "color-values-from-color-spec" => {
                need_args(name, args, 1)?;
                Ok(parse_color_spec(&string_text(&args[0])?)
                    .map(|[r, g, b]| {
                        Value::list([
                            Value::Integer(i64::from(r)),
                            Value::Integer(i64::from(g)),
                            Value::Integer(i64::from(b)),
                        ])
                    })
                    .unwrap_or(Value::Nil))
            }
            "selected-window" => Ok(interp.selected_window_value()),
            "old-selected-window" => Ok(interp.old_selected_window_value()),
            "frame-selected-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = frame_or_window_id(interp, args.first())?;
                Ok(id
                    .borrow()
                    .selected_window
                    .map(Value::Record)
                    .unwrap_or(Value::Nil))
            }
            "frame-old-selected-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = super::frames::decode_live_frame(interp, args.first(), true)?;
                Ok(id
                    .borrow()
                    .old_selected_window
                    .map(Value::Record)
                    .unwrap_or(Value::Nil))
            }
            "set-frame-selected-window" => {
                need_arg_range(name, args, 2, 3)?;
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                let window = live_window_id_or_selected(interp, args.get(1))?;
                if interp.window_frame_id(window) != Some(frame) {
                    return Err(LispError::Signal(
                        "In `set-frame-selected-window', WINDOW is not on FRAME".into(),
                    ));
                }
                if frame == interp.selected_frame_id {
                    select_window_value(interp, &args[1], args.get(2).is_some_and(Value::is_truthy))
                } else {
                    frame.borrow_mut().selected_window = interp.record_ref(window);
                    Ok(args[1])
                }
            }
            "select-window" => {
                need_arg_range(name, args, 1, 2)?;
                select_window_value(interp, &args[0], args.get(1).is_some_and(Value::is_truthy))
            }
            "current-window-configuration" => {
                need_arg_range(name, args, 0, 1)?;
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                Ok(interp.window_configuration_value(frame))
            }
            "set-window-configuration" => {
                need_arg_range(name, args, 1, 3)?;
                interp.redisplay_other_windows();
                let selected = interp.selected_frame_id;
                let restored = interp.apply_window_configuration_value(&args[0])?;
                if args.get(1).is_some_and(Value::is_truthy) && interp.frame_is_live(selected) {
                    interp.note_selected_frame(selected);
                    let window = selected.borrow().selected_window_id();
                    interp.set_selected_window_id(window);
                }
                Ok(if restored { Value::T } else { Value::Nil })
            }
            "window-configuration-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.is_window_configuration_value(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "window-configuration-equal-p" => {
                need_args(name, args, 2)?;
                Ok(if interp.window_configurations_equal(&args[0], &args[1])? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "window-configuration-frame" => {
                need_args(name, args, 1)?;
                if !interp.is_window_configuration_value(&args[0]) {
                    return Err(wrong_type_argument("window-configuration-p", args[0]));
                }
                let Kind::Record(id) = args[0].kind() else {
                    unreachable!()
                };
                Ok(interp
                    .find_record(id)
                    .expect("allocated window has a record")
                    .slots[7])
            }
            "force-window-update" => {
                need_arg_range(name, args, 0, 1)?;
                let Some(object) = args.first().filter(|value| !value.is_nil()) else {
                    interp.redisplay_other_windows();
                    return Ok(Value::T);
                };
                if let Ok(window_id) = live_window_id_or_selected(interp, Some(object)) {
                    interp.wset_redisplay(window_id);
                    return Ok(Value::T);
                }
                let buffer_id = match object.kind() {
                    Kind::Buffer(buffer) if interp.has_buffer_id(buffer.id) => Some(buffer.id),
                    _ => string_like(object)
                        .and_then(|string| interp.find_buffer(&string.text).map(|(id, _)| id)),
                };
                let showing: Vec<u64> = buffer_id
                    .map(|buffer_id| {
                        live_ordinary_window_ids(interp)
                            .into_iter()
                            .filter(|window_id| {
                                window_buffer_id(interp, &interp.record_value(*window_id))
                                    == Some(buffer_id)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                for window_id in &showing {
                    interp.wset_redisplay(*window_id);
                }
                Ok(if showing.is_empty() {
                    Value::Nil
                } else {
                    Value::T
                })
            }
            "run-window-configuration-change-hook" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(frame) = args.first()
                    && !frame.is_nil()
                    && !matches!(frame.kind(), Kind::Frame(id) if interp.frame_is_live(id))
                {
                    return Err(wrong_type_argument("frame-live-p", *frame));
                }
                let original_window = interp.selected_window_id();
                let original_buffer = interp.current_buffer_id();
                let mut result = Ok(());
                for window_id in live_ordinary_window_ids(interp) {
                    let Some(buffer_id) = window_buffer_id(interp, &interp.record_value(window_id))
                    else {
                        continue;
                    };
                    // GNU Elisp's `add-hook' LOCAL registration lives in
                    // the buffer-local value cell; the native depth mirror
                    // only sees hooks installed through the Rust bootstrap.
                    let local_hooks = interp
                        .buffer_local_hook(buffer_id, "window-configuration-change-hook")
                        .or_else(|| {
                            interp
                                .buffer_local_value(buffer_id, "window-configuration-change-hook")
                                .map(|value| value.to_vec().unwrap_or_else(|_| vec![value]))
                        });
                    let Some(local_hooks) = local_hooks else {
                        continue;
                    };
                    interp.set_selected_window_id(window_id);
                    if let Err(error) = interp.set_current_buffer_id(buffer_id) {
                        result = Err(error);
                        break;
                    }
                    for hook in local_hooks
                        .into_iter()
                        .filter(|hook| !matches!(hook.kind(), Kind::T))
                    {
                        if let Err(error) = call_function_value(interp, &hook, &[], env) {
                            result = Err(error);
                            break;
                        }
                    }
                    if result.is_err() {
                        break;
                    }
                }

                interp.set_selected_window_id(original_window);
                if result.is_ok()
                    && let Some(selected_buffer) =
                        window_buffer_id(interp, &interp.record_value(original_window))
                {
                    result = interp.set_current_buffer_id(selected_buffer);
                }
                if result.is_ok() {
                    let default_hooks = interp
                        .default_value("window-configuration-change-hook")
                        .map(|value| value.to_vec().unwrap_or_else(|_| vec![value]))
                        .unwrap_or_default();
                    for hook in default_hooks
                        .into_iter()
                        .filter(|hook| !matches!(hook.kind(), Kind::T))
                    {
                        if let Err(error) = call_function_value(interp, &hook, &[], env) {
                            result = Err(error);
                            break;
                        }
                    }
                }
                interp.set_selected_window_id(original_window);
                if interp.has_buffer_id(original_buffer) {
                    let restore = interp.set_current_buffer_id(original_buffer);
                    if result.is_ok() {
                        result = restore;
                    }
                }
                result.map(|()| Value::Nil)
            }
            "resize-mini-window-internal" => {
                need_args(name, args, 1)?;
                let window_id = live_window_id_or_selected(interp, Some(&args[0]))?;
                let frame = interp
                    .window_frame_id(window_id)
                    .expect("window stores its owning frame");
                if frame.borrow().minibuffer_window_id() != window_id {
                    return Err(LispError::Signal("Not a valid minibuffer window".into()));
                }
                // window.c:Fresize_mini_window_internal: the staged root
                // and mini heights must add up to their current sum.
                let root_id = frame.borrow().root_window_id();
                let old_height =
                    window_geometry(interp, root_id).1 + window_geometry(interp, window_id).1;
                let new_pixel = window_new_pixel(interp, window_id);
                let delta = new_pixel - window_geometry(interp, window_id).1;
                if window_resize_check(interp, root_id, false)
                    && new_pixel > 0
                    && old_height == window_new_pixel(interp, root_id) + new_pixel
                {
                    resize_mini_window_apply(interp, frame, window_id, delta)?;
                    Ok(Value::T)
                } else {
                    Err(LispError::Signal("Cannot resize mini window".into()))
                }
            }
            "window-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .filter(|window| !window.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                if window_record(&window).is_none() {
                    return Err(LispError::WrongTypeArgument("windowp".into(), window));
                }
                // A live buffer by window_buffer_id's own test.
                Ok(window_buffer_id(interp, &window)
                    .map(|buffer_id| interp.buffer_value(buffer_id).expect("live buffer object"))
                    .unwrap_or(Value::Nil))
            }
            "window-old-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let old_buffer = window_slot_value(interp, window_id, WINDOW_OLD_BUFFER_SLOT);
                let frame = interp
                    .window_frame_id(window_id)
                    .expect("window stores its owning frame");
                Ok(if old_buffer.is_nil() {
                    Value::Nil
                } else if window_integer_slot(interp, window_id, WINDOW_CHANGE_STAMP_SLOT)
                    != frame.borrow().change_stamp
                {
                    // A window restored from a configuration.
                    Value::T
                } else {
                    old_buffer
                })
            }
            "window-old-point" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(window_slot_value(interp, window_id, WINDOW_OLD_POINT_SLOT))
            }
            "set-window-buffer" => {
                need_arg_range(name, args, 2, 3)?;
                let keep_margins = args.get(2).is_some_and(Value::is_truthy);
                let window = if args[0].is_nil() {
                    interp.selected_window_value()
                } else {
                    args[0]
                };
                let Some(window_id) = window_record_id_from_value(interp, &window) else {
                    return Err(LispError::WrongTypeArgument("windowp".into(), window));
                };
                // window.c:Fset_window_buffer: Fget_buffer, CHECK_BUFFER,
                // then the buffer's liveness.
                let buffer = interp
                    .get_buffer(&args[1])?
                    .ok_or_else(|| LispError::WrongTypeArgument("bufferp".into(), Value::Nil))?;
                let buffer_id = interp
                    .live_buffer_id(buffer)
                    .ok_or_else(|| LispError::Signal("Attempt to display deleted buffer".into()))?;
                let changes_buffer =
                    window_buffer_id(interp, &window).is_some_and(|previous| previous != buffer_id);
                if changes_buffer {
                    if matches!(
                        window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT).kind(),
                        Kind::T
                    ) {
                        return Err(LispError::Signal(
                            "Window is strongly dedicated to its buffer".into(),
                        ));
                    }
                    set_window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT, Value::Nil)?;
                    if let Ok(record_window_buffer) =
                        interp.lookup_function("record-window-buffer", env)
                    {
                        interp.call_function_value(
                            record_window_buffer,
                            Some("record-window-buffer"),
                            std::slice::from_ref(&window),
                            env,
                        )?;
                    }
                }
                // window.c Fset_window_buffer: unshow_buffer, then
                // set_window_buffer.
                interp.unshow_window_buffer(window_id);
                interp.show_buffer_in_window(window_id, buffer_id, keep_margins && !changes_buffer);
                if !keep_margins {
                    // GNU resets margins from the displayed buffer by
                    // default, including on a same-buffer refresh.  A true
                    // KEEP-MARGINS third argument preserves the window's
                    // explicit widths (Magit relies on that during refresh).
                    let margin_width = |name: &str| {
                        interp
                            .buffer_local_value(buffer_id, name)
                            .unwrap_or(Value::Nil)
                    };
                    let (left, right) = (
                        margin_width("left-margin-width"),
                        margin_width("right-margin-width"),
                    );
                    set_window_margins(interp, window_id, left, right)?;
                }
                set_window_buffer_hooks(interp, window_id, buffer_id, env)?;
                Ok(Value::Nil)
            }
            "window-list" => {
                need_arg_range(name, args, 0, 3)?;
                let frame = args
                    .first()
                    .filter(|frame| !frame.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_frame_value());
                let start = args
                    .get(2)
                    .filter(|window| !window.is_nil())
                    .cloned()
                    .unwrap_or_else(|| {
                        if let Kind::Frame(id) = frame.kind() {
                            id.borrow()
                                .selected_window
                                .map(Value::Record)
                                .unwrap_or(Value::Nil)
                        } else {
                            interp.selected_window_value()
                        }
                    });
                let window = window_id_or_selected(interp, &start)?;
                if Some(frame) != interp.window_frame_id(window).map(Value::Frame) {
                    return Err(LispError::Signal("Window is on a different frame".into()));
                }
                let Kind::Frame(frame) = frame.kind() else {
                    unreachable!()
                };
                Ok(window_list_value(
                    interp,
                    args.get(1),
                    Some(&start),
                    &[frame],
                ))
            }
            "window-list-1" | "next-window" | "previous-window" => {
                need_arg_range(name, args, 0, 3)?;
                let current = args
                    .first()
                    .filter(|window| !window.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let window = live_window_id_or_selected(interp, Some(&current))?;
                let own_frame = interp
                    .window_frame_id(window)
                    .expect("window stores its owning frame");
                let frames =
                    window_list_frames(interp, own_frame, args.get(2).unwrap_or(&Value::Nil));
                let windows = window_list_value(interp, args.get(1), Some(&current), &frames);
                if name == "window-list-1" {
                    return Ok(windows);
                }
                let windows = windows.to_vec()?;
                Ok(if name == "next-window" {
                    windows.get(1).or(windows.first())
                } else {
                    windows.last()
                }
                .cloned()
                .unwrap_or(current))
            }
            "frame-root-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = frame_or_window_id(interp, args.first())?;
                Ok(id
                    .borrow()
                    .root_window
                    .map(Value::Record)
                    .unwrap_or(Value::Nil))
            }
            "frame-first-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = frame_or_window_id(interp, args.first())?;
                let mut window = id
                    .borrow()
                    .root_window
                    .map(Value::Record)
                    .unwrap_or(Value::Nil);
                while let Kind::Record(id) = window.kind() {
                    let Some(child) = window_link(interp, id.id, WINDOW_FIRST_CHILD_SLOT) else {
                        return Ok(Value::Record(id));
                    };
                    window = interp.record_value(child);
                }
                Ok(interp.selected_window_value())
            }
            "window-prev-buffers" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(window_slot_value(
                    interp,
                    window_id,
                    WINDOW_PREV_BUFFERS_SLOT,
                ))
            }
            "set-window-prev-buffers" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(interp, window_id, WINDOW_PREV_BUFFERS_SLOT, args[1])
            }
            "window-next-buffers" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(window_slot_value(
                    interp,
                    window_id,
                    WINDOW_NEXT_BUFFERS_SLOT,
                ))
            }
            "set-window-next-buffers" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(interp, window_id, WINDOW_NEXT_BUFFERS_SLOT, args[1])
            }
            "window-parameter" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                Ok(window_parameter_value(interp, window_id, &args[1]))
            }
            "set-window-parameter" => {
                need_args(name, args, 3)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_parameter_value(interp, window_id, args[1], args[2])
            }
            "window-parameters" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .filter(|window| !window.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
                    .cloned()
                    .unwrap_or(Value::Nil))
            }
            "window-frame" => {
                need_arg_range(name, args, 0, 1)?;
                let window = valid_window_or_selected(interp, args.first().unwrap_or(&Value::Nil))?;
                Ok(
                    match window.slots.get(WINDOW_FRAME_SLOT).map(|slot| slot.kind()) {
                        Some(Kind::Frame(frame)) => Value::Frame(frame),
                        _ => Value::Nil,
                    },
                )
            }
            "windowp" | "window-live-p" | "window-valid-p" => {
                need_args(name, args, 1)?;
                // WINDOWP, WINDOW_VALID_P and WINDOW_LIVE_P of the record.
                let Some(window) = window_record(&args[0]) else {
                    return Ok(Value::Nil);
                };
                let valid = || !window_kind_is(window, DELETED_WINDOW_KIND);
                let answer = match name {
                    "windowp" => true,
                    "window-valid-p" => valid(),
                    _ => valid() && window_buffer_id(interp, &args[0]).is_some(),
                };
                Ok(if answer { Value::T } else { Value::Nil })
            }
            "window-parent" | "window-prev-sibling" | "window-next-sibling" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = match name {
                    "window-parent" => WINDOW_PARENT_SLOT,
                    "window-prev-sibling" => WINDOW_PREV_SIBLING_SLOT,
                    _ => WINDOW_NEXT_SIBLING_SLOT,
                };
                if let Some(linked) = window_link(interp, window_id, slot) {
                    return Ok(interp.record_value(linked));
                }
                // frame.c make_frame links a frame's root and its own
                // minibuffer window as siblings (wset_next (rw, mini_window);
                // wset_prev (mw, root_window)); window.el's
                // frame-windows-min-size reaches the minibuffer through that
                // link.
                let root = frame_root_window_value(interp);
                let minibuffer = interp.minibuffer_window_value();
                Ok(match name {
                    "window-next-sibling" if root == interp.record_value(window_id) => minibuffer,
                    "window-prev-sibling" if minibuffer == interp.record_value(window_id) => root,
                    _ => Value::Nil,
                })
            }
            "window-top-child" | "window-left-child" => {
                need_arg_range(name, args, 0, 1)?;
                let window = valid_window_or_selected(interp, args.first().unwrap_or(&Value::Nil))?;
                let orientation = if name == "window-top-child" {
                    INTERNAL_VERTICAL_WINDOW_KIND
                } else {
                    INTERNAL_HORIZONTAL_WINDOW_KIND
                };
                Ok(
                    match window
                        .slots
                        .get(WINDOW_FIRST_CHILD_SLOT)
                        .map(|slot| slot.kind())
                    {
                        Some(Kind::Record(child)) if window_kind_is(window, orientation) => {
                            interp.record_value(child.id)
                        }
                        _ => Value::Nil,
                    },
                )
            }
            "window-combination-limit" | "set-window-combination-limit" => {
                need_args(
                    name,
                    args,
                    if name == "window-combination-limit" {
                        1
                    } else {
                        2
                    },
                )?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
                if !matches!(
                    kind.kind(),
                    Kind::Symbol(ref kind)
                        if matches!(
                            kind.as_str(),
                            INTERNAL_HORIZONTAL_WINDOW_KIND | INTERNAL_VERTICAL_WINDOW_KIND
                        )
                ) {
                    return Err(LispError::Signal(
                        "Combination limit is meaningful for internal windows only".into(),
                    ));
                }
                if name == "set-window-combination-limit" {
                    set_window_slot_value(interp, window_id, WINDOW_COMBINATION_LIMIT_SLOT, args[1])
                } else {
                    Ok(window_slot_value(
                        interp,
                        window_id,
                        WINDOW_COMBINATION_LIMIT_SLOT,
                    ))
                }
            }
            "window-minibuffer-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .filter(|window| !window.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let Some(window_id) = window_record_id_from_value(interp, &window) else {
                    return Err(LispError::WrongTypeArgument("windowp".into(), window));
                };
                let is_minibuffer = interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot.kind(), Kind::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
                Ok(if is_minibuffer { Value::T } else { Value::Nil })
            }
            "window-at" => {
                need_arg_range(name, args, 2, 3)?;
                let x = args[0].as_float()?.floor() as i64;
                let y = args[1].as_float()?.floor() as i64;
                let frame = super::frames::decode_live_frame(interp, args.get(2), true)?;
                Ok(window_at_frame_coordinates(interp, frame, x, y)
                    .map_or(Value::Nil, |id| interp.record_value(id)))
            }
            "split-window-internal" => {
                // fset_redisplay: the frame's window tree changes.
                interp.redisplay_other_windows();
                need_args(name, args, 4)?;
                split_window_tree(interp, &args[0], &args[1], &args[2], &args[3], env)
            }
            "delete-window-internal" => {
                // fset_redisplay: the frame's window tree changes.
                interp.redisplay_other_windows();
                need_args(name, args, 1)?;
                delete_window_from_tree(interp, &args[0])
            }
            "delete-other-windows-internal" => {
                // fset_redisplay: the frame's window tree changes.
                interp.redisplay_other_windows();
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let root = args.get(1).cloned().unwrap_or(Value::Nil);
                delete_other_windows_from_tree(interp, &window, &root, env)
            }
            "run-window-scroll-functions" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let buffer_id = window_buffer_id(interp, &window).ok_or_else(|| {
                    LispError::TypeError("window-live-p".into(), window.type_name())
                })?;
                let start = Value::Integer(window_start(interp, Some(&window))? as i64);
                let original_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let hooks = hook_values(interp, "window-scroll-functions", env, Some(buffer_id));
                let mut result = Ok(Value::Nil);
                for hook in hooks {
                    if let Err(error) = call_function_value(interp, &hook, &[window, start], env) {
                        result = Err(error);
                        break;
                    }
                }
                interp.set_current_buffer_id(original_buffer)?;
                result
            }
            "window-dedicated-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT))
            }
            "set-window-dedicated-p" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT, args[1])
            }
            "posn-at-x-y" => {
                need_arg_range(name, args, 2, 4)?;
                position_at_coordinates(interp, args, env)
            }
            "posn-at-point" => {
                need_arg_range(name, args, 0, 2)?;
                // Like GNU --batch, Emaxx has no realized glyph matrix from
                // which to derive a screen position.  An interactive
                // session publishes live window geometry, from which the
                // posn's screen coordinates follow (tty pixels are cells).
                let Some(metrics) = interactive_window_metrics() else {
                    return Ok(Value::Nil);
                };
                let pos = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => interp.buffer.borrow().point(),
                    Some(value) => position_from_value(interp, &value.value())?,
                };
                let point_min = interp.buffer.borrow().point_min();
                let point_max = interp.buffer.borrow().point_max();
                let start = crate::lisp::primitives::current_window_start(interp)
                    .clamp(point_min, point_max);
                let visible = pos >= start
                    && (pos < metrics.window_end
                        || (pos <= point_max && metrics.window_end >= point_max));
                if !visible {
                    // GNU answers nil for positions outside the window.
                    return Ok(Value::Nil);
                }
                let window_id = interp.selected_window_id();
                let (bol, _) = super::buffer_edit::visual_line_bounds(interp, pos);
                let line = super::buffer_edit::interactive_screen_line(interp, env, window_id, bol);
                let (seg_index, x) = line.position_cell(pos);
                // A position past a truncated row's edge reports the last
                // visible column; X counts the line-number gutter's glyphs.
                let x = if line.truncate {
                    x.min(line.text_width.saturating_sub(1))
                } else {
                    x
                } + line.lnum_cols;
                // Row within the window: visual rows between window-start
                // and POS's screen line, capped at the window height.
                let mut y = 0usize;
                let (mut walk_bol, mut walk_eol) =
                    super::buffer_edit::visual_line_bounds(interp, start);
                let walk_starts = super::buffer_edit::screen_row_starts(
                    interp, env, window_id, walk_bol, walk_eol,
                );
                let mut walk_seg = super::buffer_edit::row_index_of(&walk_starts, start);
                while y < metrics.text_height {
                    if walk_bol == bol {
                        y += seg_index.saturating_sub(walk_seg);
                        break;
                    }
                    let segs = super::buffer_edit::screen_row_starts(
                        interp, env, window_id, walk_bol, walk_eol,
                    )
                    .len();
                    y += segs.saturating_sub(walk_seg);
                    walk_seg = 0;
                    if walk_eol >= point_max {
                        break;
                    }
                    let bounds = super::buffer_edit::visual_line_bounds(interp, walk_eol + 1);
                    walk_bol = bounds.0;
                    walk_eol = bounds.1;
                }
                let y = y.min(metrics.text_height.saturating_sub(1));
                // keyboard.c's make_lispy_position for a text-area point:
                // (WINDOW POS (X . Y) TIME OBJECT POS (COL . ROW) IMAGE
                //  (DX . DY) (WIDTH . HEIGHT)).
                let xy = Value::cons(Value::Integer(x as i64), Value::Integer(y as i64));
                Ok(Value::list([
                    interp.selected_window_value(),
                    Value::Integer(pos as i64),
                    xy,
                    Value::Integer(0),
                    Value::Nil,
                    Value::Integer(pos as i64),
                    xy,
                    Value::Nil,
                    Value::cons(Value::Integer(0), Value::Integer(0)),
                    Value::cons(Value::Integer(1), Value::Integer(1)),
                ]))
            }
            "window-display-table" | "window-cursor-type" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = if name == "window-display-table" {
                    WINDOW_DISPLAY_TABLE_SLOT
                } else {
                    WINDOW_CURSOR_TYPE_SLOT
                };
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(slot))
                    .cloned()
                    .unwrap_or(if name == "window-cursor-type" {
                        Value::T
                    } else {
                        Value::Nil
                    }))
            }
            "set-window-display-table" | "set-window-cursor-type" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                if name == "set-window-cursor-type" && !valid_window_cursor_type(&args[1]) {
                    return Err(LispError::Signal("Invalid cursor type".into()));
                }
                let slot = if name == "set-window-display-table" {
                    WINDOW_DISPLAY_TABLE_SLOT
                } else {
                    WINDOW_CURSOR_TYPE_SLOT
                };
                set_window_slot_value(interp, window_id, slot, args[1])
            }
            "window-fringes" => {
                need_arg_range(name, args, 0, 1)?;
                window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Nil,
                    Value::Nil,
                ]))
            }
            "set-window-fringes" => {
                need_arg_range(name, args, 2, 5)?;
                window_id_or_selected(interp, &args[0])?;
                // GNU's TTY implementation leaves fringe settings unchanged.
                Ok(Value::Nil)
            }
            "window-scroll-bars" => {
                need_arg_range(name, args, 0, 1)?;
                window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::list([
                    Value::Nil,
                    Value::Integer(0),
                    Value::T,
                    Value::Nil,
                    Value::Integer(0),
                    Value::T,
                    Value::Nil,
                ]))
            }
            "set-window-scroll-bars" => {
                need_arg_range(name, args, 1, 6)?;
                window_id_or_selected(interp, &args[0])?;
                // GNU's TTY implementation leaves scroll-bar settings unchanged.
                Ok(Value::Nil)
            }
            "send-string-to-terminal" => {
                need_arg_range(name, args, 1, 2)?;
                let _ = string_text(&args[0])?;
                Ok(Value::Nil)
            }
            "get-buffer-window" => {
                need_arg_range(name, args, 0, 2)?;
                let buffer_id = match args
                    .first()
                    .filter(|buffer| !buffer.is_nil())
                    .map(|v| v.kind())
                {
                    None => Some(interp.current_buffer_id()),
                    Some(Kind::Buffer(buffer)) => {
                        interp.has_buffer_id(buffer.id).then_some(buffer.id)
                    }
                    Some(value) => {
                        let name = string_text(&value.value())?;
                        interp.find_buffer(&name).map(|(id, _)| id)
                    }
                };
                let Some(buffer_id) = buffer_id else {
                    return Ok(Value::Nil);
                };
                Ok(
                    buffer_window(interp, buffer_id, args.get(1).unwrap_or(&Value::Nil))
                        .unwrap_or(Value::Nil),
                )
            }
            "minibuffer-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = super::frames::decode_live_frame(interp, args.first(), true)?;
                Ok(id
                    .borrow()
                    .minibuffer_window
                    .map(Value::Record)
                    .unwrap_or(Value::Nil))
            }
            "set-minibuffer-window" => {
                need_args(name, args, 1)?;
                let Some(window_id) = window_record_id_from_value(interp, &args[0]) else {
                    return Err(wrong_type_argument("windowp", args[0]));
                };
                let is_minibuffer = interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot.kind(), Kind::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
                if !is_minibuffer {
                    return Err(LispError::Signal(
                        "Window is not a minibuffer window".into(),
                    ));
                }
                interp.set_minibuffer_window_id(window_id);
                Ok(args[0])
            }
            "minibuffer-selected-window" => Ok(interp
                .minibuffer_selected_window_id()
                .map(|id| interp.record_value(id))
                .unwrap_or_else(|| interp.selected_window_value())),
            "active-minibuffer-window" => {
                // Non-nil while a minibuffer-with-setup-hook hook runs (the
                // approximation of GNU's activated minibuffer).
                if interp.active_minibuffer_buffer_id().is_some() {
                    Ok(interp
                        .active_minibuffer_window_value()
                        .unwrap_or_else(|| interp.selected_window_value()))
                } else {
                    Ok(Value::Nil)
                }
            }
            "set-window-start" => {
                need_arg_range(name, args, 2, 3)?;
                live_window_id_or_selected(interp, args.first().filter(|w| !w.is_nil()))?;
                let start = position_from_value(interp, &args[1])?;
                let noforce = args.get(2).is_some_and(Value::is_truthy);
                set_window_start_value(interp, &args[0], start, noforce)?;
                Ok(args[1])
            }
            "set-window-point" => {
                need_args(name, args, 2)?;
                let window_id =
                    live_window_id_or_selected(interp, args.first().filter(|w| !w.is_nil()))?;
                let pos = position_from_value(interp, &args[1])?;
                let buffer_id = window_buffer_id(interp, &interp.record_value(window_id))
                    .unwrap_or_else(|| interp.current_buffer_id());
                // window.c Fset_window_point: the selected window's point is
                // its buffer's point (Fgoto_char there); another window's is
                // its own marker, clamped to the accessible region.
                if window_id == interp.selected_window_id() {
                    let point = if buffer_id == interp.current_buffer_id() {
                        let mut buffer = interp.buffer.borrow_mut();
                        buffer.goto_char(pos);
                        buffer.point()
                    } else if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                        buffer.goto_char(pos);
                        buffer.point()
                    } else {
                        pos
                    };
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_POINT_SLOT,
                        Value::Integer(point as i64),
                    )?;
                } else {
                    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_POINT_SLOT,
                        Value::Integer(pos.clamp(point_min, point_max) as i64),
                    )?;
                    interp.wset_redisplay(window_id);
                }
                Ok(args[1])
            }
            "set-window-vscroll" => {
                need_arg_range(name, args, 2, 4)?;
                let _ = args[1].as_integer()?;
                Ok(Value::Integer(0))
            }
        }
    }
);

/// The `preceding-char' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_preceding_char(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let pt = interp.buffer.borrow().point();
    if pt <= interp.buffer.borrow().point_min() {
        Ok(Value::Integer(0))
    } else {
        match public_buffer_char_code_at(interp, pt - 1) {
            Some(code) => Ok(Value::Integer(code)),
            None => Ok(Value::Integer(0)),
        }
    }
}

/// The `following-char' primitive, callable directly (a subr's function
/// pointer).
pub(super) fn direct_following_char(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match public_buffer_char_code_at(interp, interp.buffer.borrow().point()) {
        Some(code) => Ok(Value::Integer(code)),
        None => Ok(Value::Integer(0)),
    }
}

/// The echo area's Lisp value, a root of every collection (xdisp.c
/// staticpros `echo_area_buffer').
pub(crate) fn mark_echo_area_roots(mark: &mut dyn FnMut(&Value)) {
    ECHO_AREA_MESSAGE_VALUE.with_borrow(|value| {
        if let Some(value) = value {
            mark(value);
        }
    });
}

/// keymap.c Fkey_description: the single-key descriptions of PREFIX and
/// KEYS joined by spaces, a `meta-prefix-char' event folded into the meta
/// bit of the character event after it.
fn key_description(
    interp: &mut Interpreter,
    keys: Value,
    prefix: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let nkeys = sequence_length_value(&keys)?;
    let nprefix = sequence_length_value(&prefix)?;
    let meta_prefix_char = interp
        .lookup_var("meta-prefix-char", env)
        .unwrap_or(Value::Integer(KEY_DESCRIPTION_META_PREFIX));
    let sep = Value::String(" ".into());
    let mut args = Vec::new();
    let mut add_meta = false;
    for (list, listlen) in [(prefix, nprefix), (keys, nkeys)] {
        let events: Vec<Value> = if let Kind::StringObject(state) = list.kind() {
            let string = string_like(&list).expect("a string object");
            let multibyte = state.borrow().is_multibyte();
            string
                .character_codes()
                .into_iter()
                .map(|code| {
                    if !multibyte && code & 0o200 != 0 {
                        Value::Integer(code ^ (0o200 | KEY_DESCRIPTION_META_BIT))
                    } else {
                        Value::Integer(code)
                    }
                })
                .collect()
        } else if is_vector_value(&list) {
            vector_items(&list)?
        } else if matches!(list.kind(), Kind::Nil | Kind::Cons(_)) {
            list.to_vec()?
        } else {
            return Err(wrong_type_argument("arrayp", list));
        };
        for mut key in events.into_iter().take(listlen as usize) {
            if add_meta {
                let fixnum = match key.kind() {
                    Kind::Integer(code) => Some(code),
                    _ => None,
                };
                if fixnum.is_none_or(|code| code & KEY_DESCRIPTION_META_BIT != 0)
                    || values_eq_plain(&key, &meta_prefix_char)
                {
                    args.push(single_key_description(
                        interp,
                        meta_prefix_char,
                        false,
                        env,
                    )?);
                    args.push(sep);
                    if values_eq_plain(&key, &meta_prefix_char) {
                        continue;
                    }
                } else if let Some(code) = fixnum {
                    key = Value::Integer(code | KEY_DESCRIPTION_META_BIT);
                }
                add_meta = false;
            } else if values_eq_plain(&key, &meta_prefix_char) {
                add_meta = true;
                continue;
            }
            args.push(single_key_description(interp, key, false, env)?);
            args.push(sep);
        }
    }
    if add_meta {
        args.push(single_key_description(
            interp,
            meta_prefix_char,
            false,
            env,
        )?);
    } else if args.is_empty() {
        return Ok(Value::String("".into()));
    } else {
        args.pop();
    }
    StringObjectRef::concatenate(&args)
        .map(Value::StringObject)
        .map_err(|error| string_storage_error(interp, env, error))
}

/// keymap.c Fsingle_key_description.
pub(crate) fn single_key_description(
    interp: &mut Interpreter,
    key: Value,
    no_angles: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut key = key;
    if matches!(key.kind(), Kind::Cons(_)) && lucid_event_type_list_p(&key) {
        key = event_convert_list_value(interp, &key)?;
    }
    if let Some((start, end)) = key.cons_values()
        && matches!(start.kind(), Kind::Integer(_))
        && matches!(end.kind(), Kind::Integer(_))
    {
        // An interval from a map-char-table.
        let start = single_key_description(interp, start, no_angles, env)?;
        let end = single_key_description(interp, end, no_angles, env)?;
        return StringObjectRef::concatenate(&[start, Value::String("..".into()), end])
            .map(Value::StringObject)
            .map_err(|error| string_storage_error(interp, env, error));
    }
    // EVENT_HEAD.
    if matches!(key.kind(), Kind::Cons(_)) {
        key = key.car()?;
    }
    match key.kind() {
        // Normal character.
        Kind::Integer(code) => {
            let mut bytes = Vec::new();
            push_key_description(code, &mut bytes);
            make_specified_string(bytes, true)
                .map_err(|error| string_storage_error(interp, env, error))
        }
        // Function key or event-symbol.
        Kind::Nil | Kind::T | Kind::Symbol(_) => {
            let name = super::misc_keymaps::direct_symbol_name(interp, &[key], env)?;
            if no_angles {
                return Ok(name);
            }
            let Kind::StringObject(state) = name.kind() else {
                return Ok(name);
            };
            let sym = state.borrow().bytes().to_vec();
            let len = sym.len() as isize;
            // Find the extent of the modifier prefix, like "C-M-"; strchr
            // also finds the terminating NUL.
            let mut i = 0isize;
            while i < len - 3
                && sym[i as usize + 1] == b'-'
                && (sym[i as usize] == 0 || b"CMSsHA".contains(&sym[i as usize]))
            {
                i += 2;
            }
            // First I bytes of SYM are modifiers; put <> around the rest.
            let i = i as usize;
            let mut buffer = Vec::with_capacity(sym.len() + 2);
            buffer.extend_from_slice(&sym[..i]);
            buffer.push(b'<');
            buffer.extend_from_slice(&sym[i..]);
            buffer.push(b'>');
            // build_string reads up to the first NUL.
            if let Some(nul) = buffer.iter().position(|&byte| byte == 0) {
                buffer.truncate(nul);
            }
            make_string(buffer).map_err(|error| string_storage_error(interp, env, error))
        }
        // Buffer names in the menubar.
        Kind::StringObject(_) => copy_sequence_value(interp, &key),
        _ => Err(LispError::Signal(
            "KEY must be an integer, cons, symbol, or string".into(),
        )),
    }
}

/// keymap.c Ftext_char_description: "^C" for a control character, the
/// character itself otherwise.
fn text_char_description(
    interp: &Interpreter,
    character: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let code = match character.kind() {
        Kind::Integer(code)
            if (0..=i64::from(crate::lisp::primitives::coding::MAX_CHAR)).contains(&code) =>
        {
            code
        }
        _ => return Err(wrong_type_argument("characterp", *character)),
    };
    let result = if code >= 0x80 {
        let (bytes, length) = crate::lisp::types::string_data::encode_character(code as u32)?;
        make_specified_string(bytes[..length].to_vec(), true)
    } else {
        // push_text_char_description.
        let desc = match code as u8 {
            byte @ 0..0o40 => vec![b'^', byte + 64],
            0o177 => b"^?".to_vec(),
            byte => vec![byte],
        };
        make_string(desc)
    };
    result.map_err(|error| string_storage_error(interp, env, error))
}

/// Fbuffer_text_pixel_size's measurement of the current buffer's
/// accessible text on a terminal: the widest line's columns by the line
/// count.
fn buffer_text_size(interp: &Interpreter) -> Value {
    let point_min = interp.buffer.borrow().point_min();
    let point_max = interp.buffer.borrow().point_max();
    let mut effective = String::new();
    let mut pos = point_min;
    while pos < point_max {
        let display = interp.buffer.borrow().text_property_at(pos, "display");
        if let Some(display_value) = display.filter(|value| !value.is_nil()) {
            let mut end = pos;
            while end < point_max
                && interp.buffer.borrow().text_property_at(end, "display") == display
            {
                end += 1;
            }
            if let Some(replacement) = string_like(&display_value) {
                effective.push_str(&replacement.text);
                pos = end;
                continue;
            }
            let in_margin = display_value.to_vec().is_ok_and(|items| {
                items.first().is_some_and(|head| {
                    head.to_vec().is_ok_and(|spec| {
                        matches!(spec.first().map(|v| v.kind()),
                        Some(Kind::Symbol(kind)) if kind == "margin")
                    })
                })
            });
            if in_margin {
                pos = end;
                continue;
            }
        }
        match interp.buffer.borrow().char_at(pos) {
            Some(ch) => effective.push(ch),
            None => break,
        }
        pos += 1;
    }
    let width = effective
        .lines()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);
    let height = effective.lines().count().max(1);
    Value::cons(Value::Integer(width as i64), Value::Integer(height as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytecode_callback(interp: &mut Interpreter, code: &[u8], constants: Vec<Value>) -> Value {
        super::super::call(
            interp,
            "make-byte-code",
            &[
                Value::Integer(0),
                crate::lisp::primitives::coding::bytes_to_unibyte_value(code),
                Value::vector(constants),
                Value::Integer(5),
            ],
            &mut Env::new(),
        )
        .expect("construct callback from GNU bytecode instructions")
    }

    #[test]
    fn clear_message_keeps_current_and_last_displayed_flags_independent() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        for (current, last) in [(false, false), (true, false), (false, true), (true, true)] {
            set_echo_area_message(Some("previous".into()));
            ECHO_FROM_PRINT.with(|flag| flag.set(true));
            let tick = echo_area_message_tick();
            clear_message(&mut interp, &mut env, current, last).expect("clear requested buffers");
            assert_eq!(echo_area_message().is_none(), current);
            assert_eq!(ECHO_AREA_LAST_DISPLAYED.with_borrow(Option::is_none), last);
            assert!(!ECHO_FROM_PRINT.with(std::cell::Cell::get));
            assert_eq!(echo_area_message_tick(), tick, "clearing is not emission");
        }
    }

    #[test]
    fn clear_message_callback_preserves_only_current_and_sees_dynamic_guards() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let observed = Value::vector([Value::Nil]);
        // bytecode.c: Bconstant0/1, Bvarref2/3, Blist2, Baset,
        // Bdiscard, Bconstant4, Breturn. No Elisp source or form evaluation.
        let callback = bytecode_callback(
            &mut interp,
            &[192, 193, 10, 11, 68, 73, 136, 196, 135],
            vec![
                observed,
                Value::Integer(0),
                Value::symbol("inhibit-quit"),
                Value::symbol("inhibit-redisplay"),
                Value::symbol("dont-clear-message"),
            ],
        );
        interp.set_variable("clear-message-function", callback, &mut env);
        interp.set_variable("inhibit-quit", Value::Nil, &mut env);
        interp.set_variable("inhibit-redisplay", Value::Nil, &mut env);
        set_echo_area_message(Some("preserved".into()));
        let tick = echo_area_message_tick();
        clear_message(&mut interp, &mut env, true, true).expect("run preserving callback");
        let guards = super::super::call(
            &mut interp,
            "aref",
            &[observed, Value::Integer(0)],
            &mut env,
        )
        .expect("inspect callback's recorded guards");
        assert_eq!(guards, Value::list([Value::T, Value::T]));
        assert_eq!(interp.lookup_var("inhibit-quit", &env), Some(Value::Nil));
        assert_eq!(
            interp.lookup_var("inhibit-redisplay", &env),
            Some(Value::Nil)
        );
        assert_eq!(echo_area_message().as_deref(), Some("preserved"));
        assert!(ECHO_AREA_LAST_DISPLAYED.with_borrow(Option::is_none));
        assert_eq!(echo_area_message_tick(), tick);
        set_echo_area_message(None);
    }

    #[test]
    fn clear_message_skips_callbacks_when_gc_or_redisplay_evaluation_is_inhibited() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let callback = bytecode_callback(
            &mut interp,
            &[192, 135],
            vec![Value::symbol("dont-clear-message")],
        );
        interp.set_variable("clear-message-function", callback, &mut env);
        for gc_inhibited in [true, false] {
            set_echo_area_message(Some("must clear".into()));
            if gc_inhibited {
                interp.inhibit_garbage_collection();
            } else {
                interp.set_variable("inhibit-eval-during-redisplay", Value::T, &mut env);
            }
            let result = clear_message(&mut interp, &mut env, true, true);
            if gc_inhibited {
                interp.allow_garbage_collection();
            }
            result.expect("inhibited callbacks do not preserve the message");
            assert!(echo_area_message().is_none());
        }
        interp.set_variable("inhibit-eval-during-redisplay", Value::Nil, &mut env);
        interp.set_variable("clear-message-function", Value::Integer(42), &mut env);
        set_echo_area_message(Some("nonfunction".into()));
        clear_message(&mut interp, &mut env, true, true).expect("FUNCTIONP rejects nonfunctions");
        assert!(echo_area_message().is_none());
    }

    #[test]
    fn clear_message_logs_callback_signals_without_emitting_another_message() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let callback = bytecode_callback(
            &mut interp,
            &[192, 193, 194, 34, 135],
            vec![
                Value::symbol("signal"),
                Value::symbol("error"),
                Value::list([Value::String("callback failed".into())]),
            ],
        );
        super::super::call(
            &mut interp,
            "fset",
            &[Value::symbol("ignore"), callback],
            &mut env,
        )
        .expect("install a test-owned bytecode callback");
        interp.set_variable("clear-message-function", Value::symbol("ignore"), &mut env);
        interp.set_variable("inhibit-quit", Value::Nil, &mut env);
        interp.set_variable("inhibit-redisplay", Value::Nil, &mut env);
        set_echo_area_message(Some("old message".into()));
        let tick = echo_area_message_tick();
        clear_message(&mut interp, &mut env, true, true).expect("callback signal is caught");
        assert!(echo_area_message().is_none());
        assert_eq!(echo_area_message_tick(), tick);
        assert_eq!(interp.lookup_var("inhibit-quit", &env), Some(Value::Nil));
        assert_eq!(
            interp.lookup_var("inhibit-redisplay", &env),
            Some(Value::Nil)
        );
        let (id, _) = interp.find_buffer("*Messages*").expect("error was logged");
        let log = interp
            .get_buffer_by_id(id)
            .expect("live log buffer")
            .full_buffer_string();
        assert_eq!(
            log,
            "Error during redisplay: (ignore) signaled (error \"callback failed\")\n"
        );
    }

    #[test]
    fn clear_message_unwinds_guards_and_propagates_nonlocal_exits() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let tag = Value::symbol("exit");
        // GNU Bconstant0/1/2, Bcall2, Breturn invokes the ordinary throw subr.
        let callback = bytecode_callback(
            &mut interp,
            &[192, 193, 194, 34, 135],
            vec![Value::symbol("throw"), tag, Value::Integer(7)],
        );
        interp.set_variable("clear-message-function", callback, &mut env);
        interp.set_variable("inhibit-quit", Value::Nil, &mut env);
        interp.set_variable("inhibit-redisplay", Value::Nil, &mut env);
        set_echo_area_message(Some("not reached".into()));
        interp.push_active_catch_tag(tag);
        let result = clear_message(&mut interp, &mut env, true, true);
        interp.pop_active_catch_tag();
        assert!(matches!(
            result.as_ref().map_err(LispError::kind),
            Err(LispErrorKind::Throw(actual, value)) if values_eql(actual, &tag) && *value == Value::Integer(7)
        ));
        assert_eq!(interp.lookup_var("inhibit-quit", &env), Some(Value::Nil));
        assert_eq!(
            interp.lookup_var("inhibit-redisplay", &env),
            Some(Value::Nil)
        );
        assert_eq!(echo_area_message().as_deref(), Some("not reached"));
        set_echo_area_message(None);
    }

    #[test]
    fn clear_message_during_minibuffer_entry_observes_activation_before_keymap() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let observed = Value::vector([Value::Nil]);
        // Record active window, selected window and the not-yet-installed map.
        let callback = bytecode_callback(
            &mut interp,
            &[192, 193, 194, 32, 195, 32, 196, 32, 69, 73, 135],
            vec![
                observed,
                Value::Integer(0),
                Value::symbol("active-minibuffer-window"),
                Value::symbol("selected-window"),
                Value::symbol("current-local-map"),
            ],
        );
        interp.set_variable("clear-message-function", callback, &mut env);
        let map = super::super::call(&mut interp, "make-sparse-keymap", &[], &mut env)
            .expect("allocate the minibuffer keymap");
        let active = crate::lisp::primitives::activate_minibuffer(
            &mut interp,
            &Value::String("Prompt: ".into()),
            &Value::String("input".into()),
            map,
            &mut env,
        )
        .expect("activate the minibuffer before calling its clearing hook");
        let recorded = super::super::call(
            &mut interp,
            "aref",
            &[observed, Value::Integer(0)],
            &mut env,
        )
        .expect("read callback observations");
        assert_eq!(
            recorded,
            Value::list([
                interp.minibuffer_window_value(),
                interp.minibuffer_window_value(),
                Value::Nil,
            ])
        );
        crate::lisp::primitives::restore_active_minibuffer(&mut interp, active, &mut env);
    }

    #[test]
    fn clear_message_nonlocal_exit_restores_minibuffer_entry_state() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let saved_buffer = interp.current_buffer_id();
        let saved_window = interp.selected_window_id();
        let tag = Value::symbol("exit");
        let callback = bytecode_callback(
            &mut interp,
            &[192, 193, 194, 34, 135],
            vec![Value::symbol("throw"), tag, Value::Integer(7)],
        );
        interp.set_variable("clear-message-function", callback, &mut env);
        interp.push_active_catch_tag(tag);
        let result = crate::lisp::primitives::activate_minibuffer(
            &mut interp,
            &Value::String("Prompt: ".into()),
            &Value::String("input".into()),
            Value::Nil,
            &mut env,
        );
        interp.pop_active_catch_tag();
        assert!(matches!(
            result.as_ref().map_err(LispError::kind),
            Err(LispErrorKind::Throw(actual, value)) if values_eql(actual, &tag) && *value == Value::Integer(7)
        ));
        assert_eq!(interp.current_buffer_id(), saved_buffer);
        assert_eq!(interp.selected_window_id(), saved_window);
        assert_eq!(interp.minibuffer_depth(), 0);
        assert!(interp.active_minibuffer_buffer_id().is_none());
    }

    #[test]
    fn clear_message_reads_the_c_slot_after_lexical_shadowing_and_detachment() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        assert_eq!(
            interp
                .symbol_value_cell("clear-message-function")
                .expect("C DEFVAR exists"),
            Value::Nil
        );
        let callback = bytecode_callback(
            &mut interp,
            &[192, 135],
            vec![Value::symbol("dont-clear-message")],
        );
        interp.set_variable("clear-message-function", callback, &mut env);
        crate::lisp::eval::Interpreter::push_bindings(
            &mut env,
            vec![
                ("clear-message-function".into(), Value::Nil),
                ("inhibit-eval-during-redisplay".into(), Value::T),
            ],
        );
        for detached in [false, true] {
            if detached {
                super::super::call(
                    &mut interp,
                    "makunbound",
                    &[Value::symbol("clear-message-function")],
                    &mut Env::new(),
                )
                .expect("detach the Lisp symbol without destroying the C slot");
                interp.set_symbol_value_cell("clear-message-function", Value::Nil);
            }
            set_echo_area_message(Some("C-owned callback".into()));
            clear_message(&mut interp, &mut env, true, true).expect("call the original C slot");
            assert_eq!(
                echo_area_message().as_deref(),
                Some("C-owned callback"),
                "detached={detached}"
            );
        }
        set_echo_area_message(None);
    }
}
