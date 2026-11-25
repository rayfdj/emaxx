use super::*;
use crate::lisp::primitives::image::valid_image_p;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

mod frame_size;
mod mode_line;
pub(crate) use frame_size::{
    adjust_frame_size, change_frame_size, do_pending_window_change, frame_top_margin,
    set_menu_bar_lines, set_tab_bar_lines,
};
pub(crate) use mode_line::{
    ModeLineCells, begin_display_mode_lines, display_prop_intangible_p, end_display_mode_lines,
    window_base_line_pos,
};

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

#[cfg(test)]
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

/// xdisp.c:truncate_echo_area: no message for NCHARS 0 (echo_area_buffer
/// nil, shown at the next redisplay); else, interactively and while there
/// is a message, the message cut to its first NCHARS characters in place
/// (truncate_message_1), none when that leaves it empty.
pub(crate) fn truncate_echo_area(
    interp: &mut Interpreter,
    nchars: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    if nchars == 0 {
        expire_echo_area_message();
        return Ok(());
    }
    let noninteractive = interp
        .forwarded_c_value("noninteractive")
        .is_some_and(|value| value.is_truthy());
    let executing_macro = interp
        .symbol_value_key(cached_symbol!("executing-kbd-macro"))
        .is_some_and(|value| value.is_truthy());
    let Some(message) = echo_area_message_value() else {
        return Ok(());
    };
    if noninteractive || executing_macro {
        return Ok(());
    }
    if string_text(&message).map_or(0, |text| text.chars().count()) <= nchars {
        return Ok(());
    }
    let cut = crate::lisp::primitives::call(
        interp,
        "substring",
        &[message, Value::Integer(0), Value::Integer(nchars as i64)],
        env,
    )?;
    let text = string_text(&cut)?;
    let spans = string_face_spans(&cut);
    ECHO_AREA_MESSAGE_VALUE.with_borrow_mut(|slot| *slot = Some(cut));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = Some((text, spans)));
    Ok(())
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
            .forwarded_c_value("clear-message-function")
            .unwrap_or(Value::Nil);
        let preserve = if function_value_p(interp, &function, env)
            && !interp.garbage_collection_is_inhibited()
        {
            let restore = interp.bind_special_dynamic("inhibit-quit", Value::T, env)?;
            // A specbind watcher may have changed Vclear_message_function.
            let function = interp
                .forwarded_c_value("clear-message-function")
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
pub(crate) fn redisplay_safe_call(
    interp: &mut Interpreter,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let with_backtrace = interp
        .forwarded_c_value("backtrace-on-redisplay-error")
        .is_some_and(|value| value.is_truthy());
    if interp
        .forwarded_c_value("inhibit-eval-during-redisplay")
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(Value::Nil);
    }
    let restore = interp.bind_special_dynamic("inhibit-redisplay", Value::T, env)?;
    let handler_start =
        interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
    if with_backtrace {
        interp.push_handler_bindings(&[(
            Value::symbol("error"),
            Value::symbol("debug-early--muted"),
        )]);
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
    // errputc and errwrite: sysdep.c's line-buffered error stream, so the
    // message and its newline are one write.  A write that fails is the
    // stream's to remember, not the message's to report.
    use crate::lisp::primitives::batch_stdout::{errputc, errwrite};
    if interp.batch_stdout_need_newline {
        interp.batch_stdout_need_newline = false;
        errputc(b'\n');
    }
    // The line ends after a message, and after none unless the cursor is
    // to stay in the echo area.
    let newline = message.is_some()
        || !interp
            .forwarded_c_value("cursor-in-echo-area")
            .is_some_and(|value| value.is_truthy());
    errwrite(&bytes);
    if newline {
        errputc(b'\n');
    }
    Ok(())
}

/// xdisp.c:message3_nolog of nil (message1_nolog (0)): the echo area
/// cleared, which in batch ends a stderr line.
pub(crate) fn clear_message_nolog(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp
        .forwarded_c_value("noninteractive")
        .is_some_and(|value| value.is_truthy())
    {
        return message_to_stderr(interp, None, env);
    }
    if interactive_window_metrics().is_none() {
        return Ok(());
    }
    clear_message(interp, env, true, true)
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
    message_with_string_1(interp, format, argument, true, env)
}

/// xdisp.c:message_with_string with LOG false: shown through
/// message3_nolog, so neither logged nor withheld by `inhibit-message'.
pub(crate) fn message_with_string_nolog(
    interp: &mut Interpreter,
    format: &str,
    argument: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    message_with_string_1(interp, format, argument, false, env)
}

fn message_with_string_1(
    interp: &mut Interpreter,
    format: &str,
    argument: Value,
    log: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    if !argument.is_string() {
        return Err(wrong_type_argument("stringp", argument));
    }
    let batch = interp
        .forwarded_c_value("noninteractive")
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
    if !log {
        return message3_nolog(interp, message, env);
    }
    let log = string_text(&message)?;
    message3(interp, message, Some(&log), env)
}

/// xdisp.c:message3 of MESSAGE in the echo area (none for nil), logged in
/// *Messages* as LOG (print_error_message logs the signalling function
/// before the text it shows): the echo area cleared and echoing cancelled
/// first, and nothing shown under `inhibit-message'.
pub(crate) fn message3(
    interp: &mut Interpreter,
    message: Value,
    log: Option<&str>,
    env: &mut Env,
) -> Result<(), LispError> {
    // The interactive message3 path can run redisplay callbacks. Keep the
    // formatted object rooted across those calls using the existing stack roots.
    interp.with_lisp_stack_roots(&message, |interp| {
        clear_message(interp, env, true, true)?;
        crate::lisp::primitives::cancel_echoing(interp);
        // First flush out any partial line written with print.
        message_log_maybe_newline(interp, env);
        if let Some(log) = log {
            log_message_text(interp, log, env);
        }
        if interp
            .forwarded_c_value("inhibit-message")
            .is_some_and(|value| value.is_truthy())
        {
            return Ok(());
        }
        message3_nolog(interp, message, env)
    })
}

/// xdisp.c:vmessage of the bytes doprnt made of a C format: in batch
/// written to standard error as they are (a newline first after output to
/// stdout, and one after them unless `cursor-in-echo-area'), not logged;
/// otherwise message3 of make_string of them.
pub(crate) fn vmessage(
    interp: &mut Interpreter,
    bytes: Vec<u8>,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp
        .forwarded_c_value("noninteractive")
        .is_some_and(|value| value.is_truthy())
    {
        // putc, vfprintf and putc to stderr, then its fflush.
        use crate::lisp::primitives::batch_stdout::stderr_write;
        if interp.batch_stdout_need_newline {
            interp.batch_stdout_need_newline = false;
            stderr_write(b"\n");
        }
        stderr_write(&bytes);
        if !interp
            .forwarded_c_value("cursor-in-echo-area")
            .is_some_and(|value| value.is_truthy())
        {
            stderr_write(b"\n");
        }
        return Ok(());
    }
    let message = crate::lisp::primitives::strings::make_string(bytes)
        .map_err(|error| crate::lisp::primitives::string_storage_error(interp, error))?;
    let log = crate::lisp::primitives::string_text(&message)?;
    message3(interp, message, Some(&log), env)
}

/// xdisp.c:message3_nolog: MESSAGE in the echo area, neither logged nor
/// withheld by `inhibit-message'; on the initial frame (a batch session's)
/// written to standard error (message_to_stderr).
pub(crate) fn message3_nolog(
    interp: &mut Interpreter,
    message: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp
        .forwarded_c_value("noninteractive")
        .is_some_and(|value| value.is_truthy())
    {
        return message_to_stderr(interp, message.is_string().then_some(&message), env);
    }
    // INTERACTIVE: nothing is shown while a keyboard macro executes.
    if interp
        .symbol_value_key(cached_symbol!("executing-kbd-macro"))
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    // A string with characters is set; anything else clears the echo area.
    if message.is_string() && string_text(&message).is_ok_and(|text| !text.is_empty()) {
        interp.with_lisp_stack_roots(&message, |interp| set_message(interp, message, env))?;
    } else {
        clear_message(interp, env, true, true)?;
    }
    echo_area_display(interp, env);
    Ok(())
}

/// The terminal frontend's xdisp.c:echo_area_display (true): the echo
/// area displayed and the frame updated now, unless redisplay is
/// inhibited or running.
pub(crate) type EchoAreaDisplay = Box<dyn FnMut(&mut Interpreter, &mut Env)>;

thread_local! {
    static ECHO_AREA_DISPLAY: std::cell::RefCell<Option<EchoAreaDisplay>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_echo_area_display(display: Option<EchoAreaDisplay>) {
    ECHO_AREA_DISPLAY.with_borrow_mut(|slot| *slot = display);
}

/// message3_nolog's echo_area_display (true): with a current message, or
/// no minibuffer active, the echo area shown on the glass at once; an
/// active minibuffer without a message waits for the next redisplay
/// (wset_redisplay of the mini-window).
fn echo_area_display(interp: &mut Interpreter, env: &mut Env) {
    if echo_area_message_value().is_none() && interp.minibuffer_depth() > 0 {
        return;
    }
    let Some(mut display) = ECHO_AREA_DISPLAY.with_borrow_mut(Option::take) else {
        return;
    };
    display(interp, env);
    ECHO_AREA_DISPLAY.with_borrow_mut(|slot| {
        if slot.is_none() {
            *slot = Some(display);
        }
    });
}

/// xdisp.c:set_message: `set-message-function' may replace MESSAGE or
/// take over showing it.
fn set_message(interp: &mut Interpreter, message: Value, env: &mut Env) -> Result<(), LispError> {
    let mut displayed = message;
    if let Some(function) = interp
        .forwarded_c_value("set-message-function")
        .filter(|value| function_value_p(interp, value, env))
        && !interp.garbage_collection_is_inhibited()
    {
        let restore = interp.bind_special_dynamic("inhibit-quit", Value::T, env)?;
        // A binding watcher can change the C function cell.
        let function = interp
            .forwarded_c_value("set-message-function")
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
        .forwarded_c_value("noninteractive")
        .is_some_and(|v| v.is_truthy());
    if !batch {
        return message_with_string(interp, "%s", Value::string(text), env);
    }
    message_log_maybe_newline(interp, env);
    log_message_text(interp, text, env);
    if interp
        .forwarded_c_value("inhibit-message")
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

/// xdisp.c:add_to_log: FORMAT with ARGS made a message by
/// `format-message' and logged in *Messages* as a line of its own,
/// never shown.
pub(crate) fn add_to_log(
    interp: &mut Interpreter,
    format: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let mut message_args = Vec::with_capacity(args.len() + 1);
    message_args.push(Value::string(format));
    message_args.extend_from_slice(args);
    let message = super::call(interp, "format-message", &message_args, env)?;
    if let Some(text) = string_like(&message) {
        log_message_text(interp, &text.text, env);
    }
    Ok(())
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

fn log_message_dolog(interp: &mut Interpreter, text: &str, nlflag: bool, _env: &Env) {
    // message_dolog reads the two C variables (DEFVAR_LISP, so never
    // lexically bound) by their symbols.
    let buffer_name = interp
        .symbol_value_key(cached_symbol!("messages-buffer-name"))
        .filter(|value| matches!(value.kind(), Kind::StringObject(_)));
    let log_max = interp
        .symbol_value_key(cached_symbol!("message-log-max"))
        .unwrap_or(Value::T);
    if log_max.is_nil() {
        return;
    }
    let old_windows_or_buffers_changed = interp.windows_or_buffers_changed;
    // Fget_buffer_create (Vmessages_buffer_name, Qnil).
    let buffer_id = match buffer_name {
        Some(name) => interp
            .find_buffer_named(&name)
            .unwrap_or_else(|| interp.create_buffer_named(name)),
        None => interp
            .find_buffer_id("*Messages*")
            .unwrap_or_else(|| interp.create_buffer("*Messages*").0),
    };
    // xdisp.c message_dolog: `buffer-undo-list' is t and
    // `cache-long-scans' nil in the log buffer on every message.
    cached_symbol!("cache-long-scans")
        .with(|symbol| interp.set_buffer_local_value_symbol(buffer_id, symbol, Value::Nil));
    // The log is the current buffer for its changes, whose marker
    // adjustments make adjust_suspend_auto_hscroll's check.
    let old_point = interp.suspended_old_point_in(buffer_id);
    let touched = match interp.get_buffer_by_id_mut(buffer_id) {
        Some(mut buffer) => {
            buffer.set_undo_list(Value::T);
            // FIXNATP: any other non-nil value keeps every line.
            let max_lines = log_max.fixnum().and_then(|max| usize::try_from(max).ok());
            message_dolog(&mut buffer, text, nlflag, max_lines, old_point)
        }
        None => false,
    };
    if touched {
        interp.resume_auto_hscroll();
    }
    // The insertion prepared nothing, so nothing counted the change in a
    // window showing the log: windows_or_buffers_changed as it was, and
    // the log buffer marked for redisplay.
    interp.windows_or_buffers_changed = old_windows_or_buffers_changed;
    interp.bset_redisplay(buffer_id);
    MESSAGE_LOG_NEED_NEWLINE.with(|flag| flag.set(!nlflag));
}

/// Append TEXT at the end of the widened log buffer; with NLFLAG a newline
/// after it, the line folded into an identical previous one (" [N times]")
/// or replacing a previous "...", and at most MAX_LINES lines kept. Point,
/// BEGV and ZV are restored as GNU's insertion-type-nil markers leave
/// them, and point or ZV at the end stays at the end.  Answers whether
/// a change covered or touched OLD_POINT, the selected window's old point
/// while it shows the log with its auto-hscroll suspended, as each change
/// moves it (adjust_suspend_auto_hscroll).
fn message_dolog(
    buffer: &mut crate::buffer::Buffer,
    text: &str,
    nlflag: bool,
    max_lines: Option<usize>,
    mut old_point: Option<usize>,
) -> bool {
    fn after_delete(position: &mut usize, from: usize, to: usize) {
        if *position > to {
            *position -= to - from;
        } else if *position > from {
            *position = from;
        }
    }
    let mut touched = false;
    let mut changed = |old_point: &Option<usize>, from: usize, to: usize| {
        touched |= old_point.is_some_and(|old| from <= old && old <= to);
    };
    let end = buffer.size_total() + 1;
    let (begv, zv) = buffer.restriction();
    let point_at_end = buffer.point() == end;
    let zv_at_end = zv == end;
    // Insertion at Z moves none of these: each is at most Z.
    let mut saved = [buffer.point(), begv, zv];
    buffer.widen();
    buffer.goto_char(end);
    if !nlflag {
        changed(&old_point, end, end + text.chars().count());
        buffer.insert(text);
        let end = buffer.size_total() + 1;
        let [point, begv, zv] = saved;
        buffer.restore_restriction(begv, if zv_at_end { end } else { zv });
        buffer.goto_char(if point_at_end { end } else { point });
        return touched;
    }
    let mut line = String::with_capacity(text.len() + 1);
    line.push_str(text);
    line.push('\n');
    changed(&old_point, end, end + line.chars().count());
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
            changed(&old_point, prev_bol, this_bol);
            let _ = buffer.delete_region(prev_bol, this_bol);
            for position in saved.iter_mut().chain(old_point.as_mut()) {
                after_delete(position, prev_bol, this_bol);
            }
            if dups > 1 {
                // Inserted before markers, before the final newline.
                let count = format!(" [{dups} times]");
                let at = buffer.size_total();
                buffer.goto_char(at);
                changed(&old_point, at, at + count.len());
                buffer.insert_before_markers(&count);
                for position in saved.iter_mut().chain(old_point.as_mut()) {
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
        changed(&old_point, 1, cut);
        let _ = buffer.delete_region(1, cut);
        for position in &mut saved {
            after_delete(position, 1, cut);
        }
    }
    let end = buffer.size_total() + 1;
    let [point, begv, zv] = saved;
    buffer.restore_restriction(begv, if zv_at_end { end } else { zv });
    buffer.goto_char(if point_at_end { end } else { point });
    touched
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
        .symbol_value_key(cached_symbol!("ring-bell-function"))
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
    interp.set_symbol_value_key(cached_symbol!("ring-bell-function"), Value::Nil);
    interp.call_function_value(function, None, &[], env)?;
    interp.set_symbol_value_key(cached_symbol!("ring-bell-function"), function);
    Ok(())
}

/// dispnew.c:bitch_at_user: in batch a BEL on stdout; while a keyboard
/// macro runs, the user-error that ends it; else the bell.
pub(crate) fn bitch_at_user(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if interp
        .forwarded_c_value("noninteractive")
        .is_some_and(|value| value.is_truthy())
    {
        // putchar (07).
        crate::lisp::primitives::batch_stdout::write(b"\x07");
        return Ok(());
    }
    if interp
        .symbol_value_key(cached_symbol!("executing-kbd-macro"))
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
    // xdisp.c current_message_1: the echo area buffer's text, or nil
    // when it holds none.
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
}

pub(crate) fn echo_area_message() -> Option<String> {
    ECHO_AREA_MESSAGE.with_borrow(|slot| slot.as_ref().map(|(text, _)| text.clone()))
}

pub(crate) fn echo_area_message_value() -> Option<Value> {
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

fn current_bidi_paragraph_direction_value(interp: &Interpreter) -> Value {
    if !interp.current_buffer().multibyte()
        || interp
            .symbol_value_key(cached_symbol!("bidi-display-reordering"))
            .is_some_and(|value| value.is_nil())
    {
        return Value::symbol("left-to-right");
    }
    if let Some(direction) = interp
        .symbol_value_key(cached_symbol!("bidi-paragraph-direction"))
        .filter(|value| !value.is_nil())
    {
        return direction;
    }

    let point_min = interp.current_buffer().begv();
    let point_max = interp.current_buffer().zv();
    let position = interp.current_buffer().pt().clamp(point_min, point_max);
    // GNU display paragraphs span ordinary newlines.  A blank line starts a
    // new paragraph only once point has reached nonblank text after it; the
    // separator itself and trailing blank lines retain the preceding
    // paragraph's direction.
    let mut start = point_min;
    let mut scan = point_min;
    while scan < point_max {
        if interp.current_buffer().char_at(scan) != Some('\n') {
            scan += 1;
            continue;
        }
        let mut separator_end = scan + 1;
        while separator_end < point_max
            && matches!(
                interp.current_buffer().char_at(separator_end),
                Some(' ' | '\t' | '\u{c}')
            )
        {
            separator_end += 1;
        }
        if interp.current_buffer().char_at(separator_end) != Some('\n') {
            scan += 1;
            continue;
        }
        let mut next_text = separator_end + 1;
        while next_text < point_max
            && matches!(
                interp.current_buffer().char_at(next_text),
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

/// xdisp.c's `set_buffer_internal_1 (XBUFFER (w->contents))' of the
/// selected window around OPERATION, then the buffer that was current, as
/// Fdisplay__line_is_continued_p makes the window's buffer current.  A
/// deleted selected window's nil contents have no XBUFFER (GNU's checking
/// build aborts there); the current buffer stays.
fn with_selected_window_buffer<T>(
    interp: &mut Interpreter,
    operation: impl FnOnce(&mut Interpreter) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let saved_buffer = interp.current_buffer_id();
    let selected_buffer = interp
        .selected_window_contents_buffer_id()
        .unwrap_or(saved_buffer);
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

/// xdisp.c:mark_window_display_accurate (WINDOW, false): WINDOW and the
/// windows after it among its siblings, with the leaves below them, made
/// inaccurate (mark_window_display_accurate_1: no modification counts
/// known, the star recorded as the buffer has it, the window to be
/// redisplayed), and every overlay arrow drawn afresh.
pub(crate) fn mark_window_display_inaccurate(interp: &mut Interpreter, window: WindowRef) {
    fn walk(interp: &mut Interpreter, mut window: Option<WindowRef>) {
        while let Some(w) = window {
            match w.first_child() {
                Some(child) => walk(interp, Some(child)),
                None => {
                    let star = w
                        .live_buffer_id(interp)
                        .and_then(|buffer_id| interp.get_buffer_by_id(buffer_id))
                        .is_some_and(|buffer| buffer.is_modified());
                    w.set_last_modified(0);
                    w.set_last_overlay_modified(0);
                    w.set_last_had_star(star);
                    w.set_redisplay(true);
                }
            }
            window = w.next_sibling();
        }
    }
    walk(interp, Some(window));
    mark_overlay_arrows_inaccurate(interp);
}

/// xdisp.c:update_overlay_arrows (-1), as mark_window_display_accurate
/// calls it for a window made inaccurate: each symbol in
/// `overlay-arrow-variable-list' gets t as its last arrow position and
/// string, so the next redisplay draws its arrow afresh.
fn mark_overlay_arrows_inaccurate(interp: &mut Interpreter) {
    let mut list = interp
        .symbol_value_key(cached_symbol!("overlay-arrow-variable-list"))
        .unwrap_or(Value::Nil);
    while let Some((variable, rest)) = list.cons_values() {
        if variable.as_symbol().is_ok() {
            for property in [
                cached_symbol!("last-arrow-position"),
                cached_symbol!("last-arrow-string"),
            ] {
                crate::lisp::primitives::symbol_put(interp, variable, property, Value::T);
            }
        }
        list = rest;
    }
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
fn frame_or_window_frame(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<crate::lisp::types::FrameRef, LispError> {
    if let Some(value) = value
        && let Some(window) = WindowRef::from_value(value)
        && window.is_valid()
    {
        return Ok(window
            .frame()
            .expect("valid window stores its owning frame"));
    }
    super::frames::decode_live_frame(interp, value, true)
}

/// The state dispnew.c:Fframe_or_buffer_changed_p records: each frame of
/// Vframe_list with its name, then each live buffer whose name does not
/// begin with a space with its `buffer-read-only' and Fbuffer_modified_p.
/// The second value is the size GNU counts for it, which takes every
/// live buffer in.
fn current_frame_and_buffer_state(interp: &Interpreter) -> (Vec<Value>, usize) {
    let mut state = Vec::new();
    let mut size = 1;
    for frame in interp.frame_states.iter().filter(|frame| frame.is_live()) {
        state.push(Value::Frame(*frame));
        state.push(frame.name.get());
        size += 2;
    }
    for (buffer_id, name) in &interp.buffer_list {
        let Some(buffer) = interp.get_buffer_by_id(*buffer_id) else {
            continue;
        };
        size += 3;
        if crate::lisp::primitives::string_starts_with_byte(name, b' ') {
            continue;
        }
        state.push(interp.buffer_value(*buffer_id).expect("live buffer object"));
        state.push(cached_symbol!("buffer-read-only").with(|symbol| {
            interp
                .buffer_local_value_symbol(*buffer_id, symbol)
                .unwrap_or(Value::Nil)
        }));
        state.push(if buffer.is_autosaved() {
            Value::Symbol(cached_symbol!("autosaved").symbol())
        } else if buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        });
    }
    (state, size)
}

/// dispnew.c:Fframe_or_buffer_changed_p.
fn frame_or_buffer_changed(
    interp: &mut Interpreter,
    variable: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    // CHECK_SYMBOL, then Fsymbol_value.
    let variable = match variable {
        Some(value) if !value.is_nil() => {
            Some((*value, checked_symbol_identity(interp, value, env)?))
        }
        _ => None,
    };
    let state = match &variable {
        Some((value, _)) => super::misc::direct_symbol_value(interp, &[*value], env)?,
        None => interp.frame_and_buffer_state(),
    };
    let (current, size) = current_frame_and_buffer_state(interp);
    let lambda = Value::Symbol(cached_symbol!("lambda").symbol());
    let old_items = is_vector_value(&state)
        .then(|| vector_items(&state))
        .transpose()?;
    if let Some(items) = &old_items
        && items.len() > current.len()
        && current
            .iter()
            .zip(items)
            .all(|(new, old)| crate::lisp::types::eq(*new, *old))
        && crate::lisp::types::eq(items[current.len()], lambda)
    {
        return Ok(Value::Nil);
    }
    // Reallocated when the data has grown past it or it has shrunk a
    // lot, with 20 extra so it grows less often.
    let state = match &old_items {
        Some(items) if size <= items.len() && size + 20 >= items.len() / 2 => state,
        _ => {
            let state = Value::vector(std::iter::repeat_n(lambda, size + 20));
            match &variable {
                Some((_, symbol)) => {
                    super::misc::set_internal_symbol(interp, symbol, state, env)?;
                }
                None => interp.set_frame_and_buffer_state(state),
            }
            state
        }
    };
    let slots = vector_items(&state)?.len();
    for (index, value) in current.iter().enumerate() {
        aset_vector_value(&state, index, *value)?;
    }
    for index in current.len()..slots {
        aset_vector_value(&state, index, lambda)?;
    }
    Ok(Value::T)
}

/// window.c's decode_any_window: WINDOW, live, internal or deleted
/// (CHECK_WINDOW), nil the selected window.
fn decode_any_window(interp: &Interpreter, window: &Value) -> Result<WindowRef, LispError> {
    if window.is_nil() {
        return Ok(interp.selected_window_ref());
    }
    WindowRef::from_value(window)
        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), *window))
}

/// window.c's decode_valid_window: a live or internal window
/// (CHECK_VALID_WINDOW), nil the selected window, unchecked.
fn decode_valid_window(interp: &Interpreter, window: &Value) -> Result<WindowRef, LispError> {
    if window.is_nil() {
        return Ok(interp.selected_window_ref());
    }
    WindowRef::from_value(window)
        .filter(|window| window.is_valid())
        .ok_or_else(|| wrong_type_argument("window-valid-p", *window))
}

/// The link Fwindow_parent, Fwindow_prev_sibling and
/// Fwindow_next_sibling read: w->parent, w->prev, w->next.
#[derive(Clone, Copy)]
enum WindowLink {
    Parent,
    Prev,
    Next,
}

/// window.c's Fwindow_parent, Fwindow_prev_sibling and
/// Fwindow_next_sibling: the window LINK of decode_valid_window (WINDOW)
/// names.
fn window_link_value(
    interp: &Interpreter,
    name: &str,
    args: &[Value],
    link: WindowLink,
) -> Result<Value, LispError> {
    need_arg_range(name, args, 0, 1)?;
    let window = decode_valid_window(interp, args.first().unwrap_or(&Value::Nil))?;
    let linked = match link {
        WindowLink::Parent => window.parent_window(),
        WindowLink::Prev => window.prev_sibling(),
        WindowLink::Next => window.next_sibling(),
    };
    if let Some(linked) = linked {
        return Ok(linked.value());
    }
    // frame.c make_frame links a frame's root and its own minibuffer
    // window as siblings (wset_next (rw, mini_window); wset_prev (mw,
    // root_window)); window.el's frame-windows-min-size reaches the
    // minibuffer through that link.
    let this = window.value();
    Ok(match link {
        WindowLink::Next if this == frame_root_window_value(interp) => {
            interp.minibuffer_window_value()
        }
        WindowLink::Prev if this == interp.minibuffer_window_value() => {
            frame_root_window_value(interp)
        }
        _ => Value::Nil,
    })
}

/// window.c's Fwindow_top_child and Fwindow_left_child: w->contents of
/// WINDOW (nil the selected one) when it is a vertical combination
/// (WINDOW_VERTICAL_COMBINATION_P), or with HORIZONTAL a horizontal one
/// (WINDOW_HORIZONTAL_COMBINATION_P).
fn window_child_value(
    interp: &Interpreter,
    name: &str,
    args: &[Value],
    horizontal: bool,
) -> Result<Value, LispError> {
    need_arg_range(name, args, 0, 1)?;
    let window = decode_valid_window(interp, args.first().unwrap_or(&Value::Nil))?;
    Ok(match window.first_child() {
        Some(child) if window.horizontal() == horizontal => child.value(),
        _ => Value::Nil,
    })
}

/// window.c:Fwindow_parameter: PARAMETER's entry found by `assq'.
fn window_parameter_value(window: WindowRef, parameter: &Value) -> Value {
    window_parameter_entry(window, parameter)
        .and_then(|entry| entry.cdr().ok())
        .unwrap_or(Value::Nil)
}

/// PARAMETER's `assq' entry in WINDOW's own parameters.
fn window_parameter_entry(window: WindowRef, parameter: &Value) -> Option<Value> {
    // Fassq (parameter, w->window_parameters), walking the alist in place.
    let mut params = window.window_parameters();
    while let Some((item, rest)) = params.cons_values() {
        if item
            .car()
            .is_ok_and(|key| crate::lisp::types::eq(key, *parameter))
        {
            return Some(item);
        }
        params = rest;
    }
    None
}

/// window.c:Fset_window_parameter: change PARAMETER's `assq' entry in
/// place, or push a new one on the front (a nil VALUE stays an entry).
fn set_window_parameter_value(
    window: WindowRef,
    parameter: Value,
    value: Value,
) -> Result<Value, LispError> {
    if let Some(entry) = window_parameter_entry(window, &parameter) {
        entry.set_cdr(value)?;
        return Ok(value);
    }
    let existing = window.window_parameters();
    let parameters = Value::cons(Value::cons(parameter, value), existing);
    window.set_window_parameters(parameters);
    Ok(value)
}

/// WINDOW's pixel geometry as (pixel_width, pixel_height, pixel_left,
/// pixel_top).  A window given no size reads the selected frame's, and
/// one given no edge the frame's corner.
fn window_geometry(interp: &Interpreter, window: WindowRef) -> (i64, i64, i64, i64) {
    (
        window.pixel_width().unwrap_or_else(|| interp.frame_width()),
        window
            .pixel_height()
            .unwrap_or_else(|| interp.frame_height()),
        window.pixel_left().unwrap_or(0),
        window.pixel_top().unwrap_or(0),
    )
}

fn window_at_frame_coordinates(
    interp: &Interpreter,
    frame: crate::lisp::types::FrameRef,
    x: i64,
    y: i64,
) -> Option<WindowRef> {
    frame_window_tree_leaves(frame)
        .into_iter()
        .chain(frame.borrow().minibuffer_window)
        .find(|window| {
            let (width, height, left, top) = window_geometry(interp, *window);
            x >= left && x < left + width && y >= top && y < top + height
        })
}

fn position_at_coordinates(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    // keyboard.c:Fposn_at_x_y: CHECK_FIXNUM (x), CHECK_FIXNAT (x) unless it
    // is -1, then CHECK_FIXNAT (y).
    let Kind::Integer(mut x) = args[0].kind() else {
        return Err(wrong_type_argument("fixnump", args[0]));
    };
    if x < -1 {
        return Err(wrong_type_argument("wholenump", args[0]));
    }
    let mut y = match args[1].kind() {
        Kind::Integer(y) if y >= 0 => y,
        _ => return Err(wrong_type_argument("wholenump", args[1])),
    };
    let requested = args.get(2).copied().unwrap_or(Value::Nil);
    let frame = if requested.is_nil() || is_window_value(interp, &requested) {
        let window = decode_live_window(interp, Some(&requested))?;
        let (_, _, left, top) = window_geometry(interp, window);
        x += left;
        y += top;
        if args.get(3).is_none_or(|value| value.is_nil()) {
            x += crate::lisp::primitives::window_margin_cols(window)
                .0
                .unwrap_or(0);
        }
        window.frame().expect("live window frame")
    } else {
        super::frames::decode_live_frame(interp, Some(&requested), false)?
    };
    let Some(w) = window_at_frame_coordinates(interp, frame, x, y) else {
        return Ok(Value::list([
            Value::Frame(frame),
            Value::Nil,
            Value::cons(Value::Integer(x), Value::Integer(y)),
            Value::Integer(0),
        ]));
    };
    let window = w.value();
    let (_, _, left, top) = window_geometry(interp, w);
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
        // make_lispy_position's mode_line_string for the tab, header and
        // mode lines: COL the glyph under X (a cell a glyph, the line
        // filling the window's width), ROW the line's row counted from
        // the first text row, DX and DY from the glyph, its pixel_width
        // 1 and ascent plus descent 0 (term.c).
        let tab_line = window_line_height(interp, buffer, "tab-line-format");
        let header_line = window_line_height(interp, buffer, "header-line-format");
        let row = if crate::lisp::types::eq_symbol_named(area, "tab-line") {
            Some(-(tab_line + header_line))
        } else if crate::lisp::types::eq_symbol_named(area, "header-line") {
            Some(-header_line)
        } else if crate::lisp::types::eq_symbol_named(area, "mode-line") {
            Some(window_geometry(interp, w).1 - window_non_body_height(interp, w, Some(buffer)))
        } else {
            None
        };
        let (col_row, dx_dy, size) = match row {
            Some(row) => ((local_x, row), (0, 0), (1, 0)),
            None => ((local_x, local_y), (-1, -1), (-1, -1)),
        };
        return Ok(Value::list([
            window,
            area,
            Value::cons(Value::Integer(local_x), Value::Integer(local_y)),
            Value::Integer(0),
            Value::Nil,
            Value::Nil,
            Value::cons(Value::Integer(col_row.0), Value::Integer(col_row.1)),
            Value::Nil,
            Value::cons(Value::Integer(dx_dy.0), Value::Integer(dx_dy.1)),
            Value::cons(Value::Integer(size.0), Value::Integer(size.1)),
        ]));
    }
    let text_x = area.car()?.as_integer()?;
    let text_y = local_y
        - window_line_height(interp, buffer, "tab-line-format")
        - window_line_height(interp, buffer, "header-line-format");
    let old_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(buffer)?;
    let result = (|| {
        let start = window_start(interp, Some(&window))?;
        let end = interp.current_buffer().zv();
        let hscroll = window_hscroll_state(w).hscroll.max(0);
        let motion_args = crate::lisp::alloc::RootedVec::from_vec(vec![
            Value::Integer(start as i64),
            Value::cons(Value::Integer(-hscroll), Value::Integer(0)),
            Value::Integer(end as i64),
            Value::cons(Value::Integer(text_x), Value::Integer(text_y)),
            Value::Nil,
            Value::cons(Value::Integer(hscroll), Value::Integer(0)),
            window,
        ]);
        // dispnew.c obtains dimensions from a realized glyph matrix:
        // the glyph's pixel_width and its ascent plus descent, which a
        // terminal's glyphs leave at 0 (term.c's append_glyph sets neither),
        // the space display_line appends at a line's end or the buffer's
        // being one column wide.  Batch has no such matrix, even though
        // position traversal works.
        let realized = interactive_window_metrics().is_some();
        let mut object = Value::Nil;
        let (position, hpos, col, vpos) = if realized {
            // buffer_posn_from_coords: start_display at the window's
            // start, clipped into the accessible text
            // (CLIP_TEXT_POS_FROM_MARKER), move_it_to the row, then to
            // the x, as the window lays its text out.
            let begv = interp.current_buffer().begv();
            let at = posn_at_window_xy(
                interp,
                env,
                w,
                start.clamp(begv, end),
                (text_x.max(0) as usize, text_y.max(0) as usize),
            );
            if let Some((string, index)) = at.object {
                object = Value::cons(string, Value::Integer(index as i64));
            }
            (at.position, at.x, at.col, at.row as i64)
        } else {
            let motion = super::buffer_edit::display_motion(interp, env, &motion_args, true)?;
            (
                motion.position,
                motion.hpos,
                text_x.max(motion.hpos),
                motion.vpos,
            )
        };
        let dx = text_x - hpos;
        let dy = text_y - vpos;
        // The glyph's pixel_width, which term.c's append_glyph sets to 1
        // for each cell glyph (a wide character's included).
        let width = i64::from(realized);
        let position = Value::Integer(position as i64);
        Ok(Value::list([
            window,
            position,
            Value::cons(Value::Integer(text_x), Value::Integer(text_y)),
            Value::Integer(0),
            object,
            position,
            // make_lispy_position's COL is buffer_posn_from_coords's
            // it.hpos (plus the cells past the element's end).
            Value::cons(Value::Integer(col), Value::Integer(vpos)),
            Value::Nil,
            Value::cons(Value::Integer(dx), Value::Integer(dy)),
            Value::cons(Value::Integer(width), Value::Integer(0)),
        ]))
    })();
    interp.with_lisp_stack_roots(&result, |interp| interp.set_current_buffer_id(old_buffer))?;
    result
}

/// pos_visible_p on the terminal layout of WINDOW, which shows the
/// current buffer: start_display at START, then move_it_to POS within
/// TEXT_ROWS rows.  The iterator fetches what it passes: up to POS and
/// POS itself, then the whole of POS's row when nothing was produced on
/// it before POS (line_bottom_y moves a line down for the row's
/// height); when POS is on the last row or does not show, every row to
/// its end (move_it_to MOVE_TO_Y scans each row it leaves, and stops at
/// the last row's start, which move_it_by_lines then walks).  No POS (t,
/// charpos -1) asks for the window's end: past the glyphs of ZV's row
/// when ZV shows above the last row (the next row's start when a
/// truncated row reaches its edge first), else where the last row begins
/// (it2's x, the hscroll taken off).
fn pos_visible_xy(
    interp: &mut Interpreter,
    env: &mut Env,
    window: WindowRef,
    start: usize,
    pos: Option<usize>,
    text_rows: usize,
) -> Option<crate::tty::VisibleAt> {
    let mut passes = 0;
    let mut fetches = crate::tty::IteratorFetches::off();
    loop {
        let geometry = super::buffer_edit::window_text_geometry(interp, env, window);
        let shown = match pos {
            Some(target) => {
                crate::tty::window_position_xy(interp, window, start, target, geometry, text_rows)
            }
            None => crate::tty::window_end_xy(interp, window, start, geometry, text_rows)
                .map(|(x, y)| crate::tty::VisibleAt { x, y, rbot: 0 }),
        };
        let xy = shown.or_else(|| {
            pos.is_none().then(|| crate::tty::VisibleAt {
                x: -(geometry.hscroll as i64),
                y: text_rows.saturating_sub(1),
                rbot: 0,
            })
        });
        passes += 1;
        fetches.begin_pass(interp, env, window);
        if passes > crate::tty::ITERATOR_FONTIFICATION_PASSES || fetches.geometry().is_none() {
            return xy;
        }
        crate::tty::fetch_pos_visible(
            interp,
            window,
            &mut fetches,
            geometry,
            (start, pos),
            shown.map(|at| (at.x, at.y)),
            text_rows,
        );
        if !fetches.fontify(interp, env, window) {
            return xy;
        }
    }
}

/// Fwindow_end's walk on the terminal layout of WINDOW, which shows the
/// current buffer: start_display at START, move_it_vertically down its
/// TEXT_ROWS and past the end of the line where the text ends above the
/// row below the window, the iterator fetching what it passes.
fn window_end_walk(
    interp: &mut Interpreter,
    env: &mut Env,
    window: WindowRef,
    start: usize,
    text_rows: usize,
) -> usize {
    let mut passes = 0;
    let mut fetches = crate::tty::IteratorFetches::off();
    loop {
        let geometry = super::buffer_edit::window_text_geometry(interp, env, window);
        let end = crate::tty::window_end_walk(interp, window, start, geometry, text_rows);
        passes += 1;
        fetches.begin_pass(interp, env, window);
        if passes > crate::tty::ITERATOR_FONTIFICATION_PASSES || fetches.geometry().is_none() {
            return end;
        }
        crate::tty::fetch_window_end(interp, window, &mut fetches, geometry, start, text_rows);
        if !fetches.fontify(interp, env, window) {
            return end;
        }
    }
}

/// buffer_posn_from_coords on the terminal layout of WINDOW, which shows
/// the current buffer: start_display at START, move_it_to row Y, each row
/// above walked to its end, then to X, fetching through the element
/// there (the x reached, the row is known to hold Y).
fn posn_at_window_xy(
    interp: &mut Interpreter,
    env: &mut Env,
    window: WindowRef,
    start: usize,
    xy: (usize, usize),
) -> crate::tty::WindowPositionAt {
    let mut passes = 0;
    let mut fetches = crate::tty::IteratorFetches::off();
    loop {
        let geometry = super::buffer_edit::window_text_geometry(interp, env, window);
        let at = crate::tty::window_position_at(interp, window, start, xy, geometry);
        passes += 1;
        fetches.begin_pass(interp, env, window);
        if passes > crate::tty::ITERATOR_FONTIFICATION_PASSES || fetches.geometry().is_none() {
            return at;
        }
        crate::tty::fetch_start_display(interp, window, &mut fetches, start);
        let mut walk = crate::tty::IteratorWalk::from_window_start(interp, window, geometry, start);
        let row_start = walk.walk_rows(&mut fetches, start, xy.1);
        walk.walk_to(&mut fetches, row_start.min(at.position), at.position);
        if !fetches.fontify(interp, env, window) {
            return at;
        }
    }
}

/// WINDOW's pixel_width, pixel_height, pixel_left and pixel_top, in
/// `window_geometry''s order.
fn set_window_geometry(window: WindowRef, geometry: (i64, i64, i64, i64)) {
    let (width, height, left, top) = geometry;
    window.set_pixel_width(Some(width));
    window.set_pixel_height(Some(height));
    window.set_pixel_left(Some(left));
    window.set_pixel_top(Some(top));
}

/// FRAME_ROOT_WINDOW (f) of a frame that has its windows, as every
/// frame has from make_frame until it is deleted.
pub(super) fn frame_root_window(frame: crate::lisp::types::FrameRef) -> WindowRef {
    frame
        .borrow()
        .root_window
        .expect("a live frame has a root window")
}

/// FRAME_SELECTED_WINDOW (f), as `frame_root_window'.
pub(super) fn frame_selected_window(frame: crate::lisp::types::FrameRef) -> WindowRef {
    frame
        .borrow()
        .selected_window
        .expect("a live frame has a selected window")
}

/// FRAME_MINIBUF_WINDOW (f), as `frame_root_window'.
fn frame_minibuffer_window(frame: crate::lisp::types::FrameRef) -> WindowRef {
    frame
        .borrow()
        .minibuffer_window
        .expect("a live frame has a minibuffer window")
}

fn frame_root_window_value(interp: &Interpreter) -> Value {
    interp.root_window_value()
}

/// A live window (WINDOW_LIVE_P, of a live buffer) that is no minibuffer
/// window.
fn is_live_ordinary_window(interp: &Interpreter, window: WindowRef) -> bool {
    !window_is_minibuffer(window) && window.live_buffer_id(interp).is_some()
}

/// Leaf windows of the frame's window tree in GNU's canonical order — a
/// depth-first walk that descends first children and follows sibling
/// chains.  `next-window' cycles and `window-list' both follow it, and it
/// is the top-to-bottom, left-to-right order the glass paints.
/// The windows a walk of the tree collects.  The callers go through
/// them while hooks and other Lisp run, which can delete a window of
/// the list and collect, so the list holds its windows for the
/// collector (window_loop's `window_list' is a Lisp list, GCPRO'd by
/// being on the C stack).
pub(crate) type WindowList = crate::lisp::alloc::RootedVec<WindowRef>;

fn window_tree_leaves(interp: &Interpreter) -> WindowList {
    frame_window_tree_leaves(interp.selected_frame_id)
}

fn frame_window_tree_leaves(frame: crate::lisp::types::FrameRef) -> WindowList {
    let mut leaves = WindowList::new();
    let Some(root) = frame.borrow().root_window else {
        return leaves;
    };
    // A frame of one window is its root.
    if root.first_child().is_none() {
        leaves.push(root);
        return leaves;
    }
    // The walk's own stack and chain run no Lisp, and name windows the
    // tree holds.
    let mut stack: smallvec::SmallVec<[WindowRef; 8]> = smallvec::smallvec![root];
    while let Some(window) = stack.pop() {
        match window.first_child() {
            Some(child) => {
                let mut chain: smallvec::SmallVec<[WindowRef; 8]> = smallvec::SmallVec::new();
                let mut walk = Some(child);
                while let Some(node) = walk {
                    chain.push(node);
                    walk = node.next_sibling();
                }
                stack.extend(chain.into_iter().rev());
            }
            None => leaves.push(window),
        }
    }
    leaves
}

/// FRAME's own minibuffer window, which window.c links into the frame's
/// tree as the root window's `next': none for a frame whose minibuffer
/// window is another frame's, or whose root is its minibuffer window.
fn frame_own_minibuffer_window(frame: crate::lisp::types::FrameRef) -> Option<WindowRef> {
    let (root, mini) = {
        let state = frame.borrow();
        (state.root_window, state.minibuffer_window)
    };
    mini.filter(|mini| Some(*mini) != root && mini.frame() == Some(frame))
}

/// window.c:window_list's walk without its cache: FOR_EACH_FRAME over the
/// frame list, and foreach_window over each frame, whose
/// foreach_window_1 visits the leaves of the frame's window tree in
/// canonical order and then, as the root window's `next', the frame's own
/// minibuffer window.  Every leaf is listed, whatever its buffer.
pub(crate) fn foreach_frame_window(interp: &Interpreter) -> WindowList {
    let mut windows = WindowList::new();
    for frame in interp
        .frame_states
        .iter()
        .copied()
        .filter(|frame| frame.is_live())
    {
        windows.extend(frame_window_tree_leaves(frame));
        windows.extend(frame_own_minibuffer_window(frame));
    }
    windows
}

/// Every window of FRAME's tree, internal ones too, in the order of
/// window.c:delete_all_child_windows on the frame's root window: for a
/// window, the windows after it (its later siblings, and after the root
/// the frame's own minibuffer window) first, then its children, then the
/// window itself.
pub(crate) fn frame_windows_for_deletion(frame: crate::lisp::types::FrameRef) -> WindowList {
    fn walk(window: WindowRef, next: Option<WindowRef>, windows: &mut WindowList) {
        if let Some(next) = next {
            walk(next, next.next_sibling(), windows);
        }
        if let Some(child) = window.first_child() {
            walk(child, child.next_sibling(), windows);
        }
        windows.push(window);
    }
    let mut windows = WindowList::new();
    let root = frame.borrow().root_window;
    if let Some(root) = root {
        walk(root, frame_own_minibuffer_window(frame), &mut windows);
    }
    windows
}

/// window.c:window_list_1 (selected_window, Qt, Qt), the windows
/// Fnext_window (w, Qt, Qt) visits from the selected window round to it:
/// the live windows of every frame, the minibuffer windows included, in
/// the cyclic order from the selected window.
pub(crate) fn all_frames_window_cycle(interp: &Interpreter) -> WindowList {
    let frames = window_list_frames(interp, interp.selected_frame_id, &Value::T);
    window_list_windows(interp, Some(&Value::T), None, &frames)
}

/// window.c:window_list: the live windows of every live frame in
/// canonical order, each frame's minibuffer window after its tree's.
pub(crate) fn frames_window_list(interp: &Interpreter) -> WindowList {
    let mut windows = WindowList::new();
    for frame in interp
        .frame_states
        .iter()
        .copied()
        .filter(|frame| frame.is_live())
    {
        windows.extend(
            frame_window_tree_leaves(frame)
                .into_iter()
                .filter(|window| is_live_ordinary_window(interp, *window)),
        );
        windows.extend(frame_own_minibuffer_window(frame));
    }
    windows
}

/// The live windows of the selected frame's tree that are no minibuffer
/// window, in canonical order.
pub(crate) fn live_ordinary_windows(interp: &Interpreter) -> WindowList {
    window_tree_leaves(interp)
        .into_iter()
        .filter(|window| is_live_ordinary_window(interp, *window))
        .collect()
}

/// One live window as the terminal frontend renders it: its frame rect,
/// buffer, and display anchors.  Geometry counts total lines and columns
/// — the mode line is the rect's last row, and a window not flush with
/// the frame's right edge spends its last column on the vertical border.
#[derive(Clone, Copy)]
pub(crate) struct WindowRenderInfo {
    pub(crate) window: WindowRef,
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
/// geometry and content anchors redisplay needs.  The list holds the
/// windows across the Lisp that redisplay runs, so the collector scans
/// it.
pub(crate) fn window_render_layout(
    interp: &Interpreter,
) -> crate::lisp::alloc::RootedVec<WindowRenderInfo> {
    let selected_window = interp.selected_window_ref();
    live_ordinary_windows(interp)
        .into_iter()
        .filter_map(|window| {
            let selected = window == selected_window;
            let buffer_id = window.live_buffer_id(interp)?;
            let (width, height, left, top) = window_geometry(interp, window);
            let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
            let clamp = |value: i64| value.clamp(point_min as i64, point_max as i64) as usize;
            let start = window_start_marker(interp, window, buffer_id);
            let live_point = if buffer_id == interp.current_buffer_id() {
                interp.current_buffer().pt()
            } else {
                interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.point())
                    .unwrap_or(point_min)
            };
            let point = if selected {
                live_point
            } else {
                window_marker_position(window, WindowMarker::Pointm)
                    .map(|point| clamp(point as i64))
                    .unwrap_or(live_point)
            };
            Some(WindowRenderInfo {
                window,
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

/// dispextern.h's FACE_TTY_DEFAULT_COLOR, FACE_TTY_DEFAULT_FG_COLOR and
/// FACE_TTY_DEFAULT_BG_COLOR: the pixels of no color, the terminal's own
/// foreground and its own background (face_tty_specified_color is a
/// pixel of none of them).
pub(crate) const FACE_TTY_DEFAULT_COLOR: i64 = -1;
pub(crate) const FACE_TTY_DEFAULT_FG_COLOR: i64 = -2;
pub(crate) const FACE_TTY_DEFAULT_BG_COLOR: i64 = -3;

/// dispextern.h's enum face_underline_type.
pub(crate) const FACE_NO_UNDERLINE: u8 = 0;
pub(crate) const FACE_UNDERLINE_SINGLE: u8 = 1;
const FACE_UNDERLINE_DOUBLE_LINE: u8 = 2;
const FACE_UNDERLINE_WAVE: u8 = 3;
const FACE_UNDERLINE_DOTS: u8 = 4;
const FACE_UNDERLINE_DASHES: u8 = 5;

/// A face's tty attributes, the frontend's paint unit, as
/// xfaces.c:realize_tty_face maps them: the colors' pixels (map_tty_color:
/// the terminal's color index, or one of the FACE_TTY_DEFAULT_ pixels;
/// None where the face leaves the color to the default face),
/// tty_bold_p (a weight above medium), tty_italic_p (a slant other than
/// normal), the underline's type and color pixel (0: none),
/// tty_reverse_p and tty_strike_through_p.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub(crate) struct TtyFaceAttrs {
    pub(crate) foreground: Option<i64>,
    pub(crate) background: Option<i64>,
    pub(crate) bold: bool,
    pub(crate) italic: bool,
    pub(crate) underline: u8,
    pub(crate) underline_color: i64,
    pub(crate) reverse: bool,
    pub(crate) strike_through: bool,
    /// GNU's `:extend': the face keeps painting past the end of line to
    /// the window edge (the region's whole-row highlight).
    pub(crate) extend: bool,
    /// The FACE_OFF_* attributes the face specifies as off (`:weight
    /// normal', `:underline nil'...), which merging over a face that has
    /// them on turns off; the ones neither on nor off are unspecified,
    /// the underlying face's.
    pub(crate) off: u8,
}

pub(crate) const FACE_OFF_BOLD: u8 = 1;
pub(crate) const FACE_OFF_ITALIC: u8 = 2;
pub(crate) const FACE_OFF_UNDERLINE: u8 = 4;
pub(crate) const FACE_OFF_REVERSE: u8 = 8;
pub(crate) const FACE_OFF_STRIKE_THROUGH: u8 = 16;
pub(crate) const FACE_OFF_EXTEND: u8 = 32;

impl TtyFaceAttrs {
    fn from_options(options: TtyFaceAttrOptions) -> Self {
        let (underline, underline_color) = options.underline.unwrap_or((FACE_NO_UNDERLINE, 0));
        let off = |value: Option<bool>, bit: u8| if value == Some(false) { bit } else { 0 };
        Self {
            foreground: options.foreground,
            background: options.background,
            bold: options.bold.unwrap_or(false),
            italic: options.italic.unwrap_or(false),
            underline,
            underline_color,
            reverse: options.reverse.unwrap_or(false),
            strike_through: options.strike_through.unwrap_or(false),
            extend: options.extend.unwrap_or(false),
            off: off(options.bold, FACE_OFF_BOLD)
                | off(options.italic, FACE_OFF_ITALIC)
                | off(
                    options
                        .underline
                        .map(|(style, _)| style != FACE_NO_UNDERLINE),
                    FACE_OFF_UNDERLINE,
                )
                | off(options.reverse, FACE_OFF_REVERSE)
                | off(options.strike_through, FACE_OFF_STRIKE_THROUGH)
                | off(options.extend, FACE_OFF_EXTEND),
        }
    }

    /// merge_face_vectors for tty attributes: OVER's specified ones (on
    /// or off) over BASE's, BASE's where OVER leaves them unspecified.
    pub(crate) fn merged_over(base: &Self, over: &Self) -> Self {
        let flag = |base_on: bool, over_on: bool, bit: u8| -> (bool, u8) {
            if over_on {
                (true, 0)
            } else if over.off & bit != 0 {
                (false, bit)
            } else {
                (base_on, base.off & bit)
            }
        };
        let (bold, bold_off) = flag(base.bold, over.bold, FACE_OFF_BOLD);
        let (italic, italic_off) = flag(base.italic, over.italic, FACE_OFF_ITALIC);
        let (reverse, reverse_off) = flag(base.reverse, over.reverse, FACE_OFF_REVERSE);
        let (strike_through, strike_off) = flag(
            base.strike_through,
            over.strike_through,
            FACE_OFF_STRIKE_THROUGH,
        );
        let (extend, extend_off) = flag(base.extend, over.extend, FACE_OFF_EXTEND);
        let (underline, underline_color, underline_off) = if over.underline != FACE_NO_UNDERLINE {
            (over.underline, over.underline_color, 0)
        } else if over.off & FACE_OFF_UNDERLINE != 0 {
            (FACE_NO_UNDERLINE, 0, FACE_OFF_UNDERLINE)
        } else {
            (
                base.underline,
                base.underline_color,
                base.off & FACE_OFF_UNDERLINE,
            )
        };
        Self {
            foreground: over.foreground.or(base.foreground),
            background: over.background.or(base.background),
            bold,
            italic,
            underline,
            underline_color,
            reverse,
            strike_through,
            extend,
            off: bold_off | italic_off | underline_off | reverse_off | strike_off | extend_off,
        }
    }
}

/// Resolve FACE to its tty attributes through the runtime's own face
/// machinery: `face-attribute' answers the inherit-merged attributes,
/// which realize_tty_face maps to the terminal's (map_tty_color for the
/// colors), in the current buffer's `face-remapping-alist'.
#[cfg(test)]
pub(crate) fn resolve_tty_face_attrs(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
) -> TtyFaceAttrs {
    let remapping = interp
        .symbol_value_key(cached_symbol!("face-remapping-alist"))
        .unwrap_or(Value::Nil);
    resolve_tty_face_attrs_remapped(interp, env, face, remapping)
}

/// `resolve_tty_face_attrs' with REMAPPING the `face-remapping-alist' in
/// effect: the displayed buffer's (redisplay_window makes a window's
/// buffer current while it realizes the window's faces), nil for the
/// frame's own faces.
pub(crate) fn resolve_tty_face_attrs_remapped(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
    remapping: Value,
) -> TtyFaceAttrs {
    // face-remap.el installs buffer-local substitutions such as Magit's
    // `(header-line magit-header-line header-line)'.  Redisplay realizes
    // the cdr as the effective precedence-ordered face list; the trailing
    // base face is deliberately resolved without recursively remapping it.
    if let Kind::Symbol(requested) = face.kind()
        && let Some(remapped) = Some(remapping)
            .filter(|alist| !alist.is_nil())
            .and_then(|alist| alist.to_vec().ok())
            .and_then(|entries| {
                entries.into_iter().find_map(|entry| {
                    let key = entry.car_if_list()?;
                    matches!(key.kind(), Kind::Symbol(name) if name == requested)
                        .then(|| entry.cdr_if_list())?
                })
            })
    {
        return TtyFaceAttrs::from_options(resolve_tty_face_reference_options(
            interp, env, &remapped, 0,
        ));
    }
    // xfaces.c merge_face_ref: a face reference may be a LIST of face
    // references, merged left to right with the entries nearer the
    // front taking precedence (comint's prompt carries
    // `(comint-highlight-prompt comint-highlight-prompt)').  Realize
    // each member and fold, letting an earlier member's set attributes
    // override a later one's.
    if matches!(face.kind(), Kind::Cons(_)) {
        return TtyFaceAttrs::from_options(resolve_tty_face_reference_options(
            interp, env, face, 0,
        ));
    }
    TtyFaceAttrs::from_options(resolve_tty_face_attr_options(interp, env, face))
}

/// One face member's tty attributes with per-attribute specificity: a
/// `None' means the face leaves that attribute unspecified (inherit
/// merged), a `Some' is an explicit value — explicit nil included, so
/// a list merge can turn attributes off.
#[derive(Default)]
pub(crate) struct TtyFaceAttrOptions {
    pub(crate) foreground: Option<i64>,
    pub(crate) background: Option<i64>,
    pub(crate) bold: Option<bool>,
    pub(crate) italic: Option<bool>,
    pub(crate) underline: Option<(u8, i64)>,
    pub(crate) reverse: Option<bool>,
    pub(crate) strike_through: Option<bool>,
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
    if overlay.italic.is_some() {
        base.italic = overlay.italic;
    }
    if overlay.underline.is_some() {
        base.underline = overlay.underline;
    }
    if overlay.reverse.is_some() {
        base.reverse = overlay.reverse;
    }
    if overlay.strike_through.is_some() {
        base.strike_through = overlay.strike_through;
    }
    if overlay.extend.is_some() {
        base.extend = overlay.extend;
    }
}

/// xfaces.c:map_tty_color of a face's color VALUE (a foreground's or an
/// underline's when FOREGROUND_P, else a background's): a string's index
/// in `tty-color-alist' when `tty-defined-color-alist' is a list, else
/// load_color's pixel (tty_defined_color's, the FACE_TTY_DEFAULT_ pixels
/// for "unspecified-fg" and "unspecified-bg"), a color it cannot load
/// logged ("Unable to load color") and given the frame's default pixel.
/// None for a VALUE that is no string.
fn map_tty_color(
    interp: &mut Interpreter,
    env: &mut Env,
    value: &Value,
    foreground_p: bool,
) -> Option<i64> {
    if !value.is_string() {
        return None;
    }
    let default_pixel = if foreground_p {
        FACE_TTY_DEFAULT_FG_COLOR
    } else {
        FACE_TTY_DEFAULT_BG_COLOR
    };
    let mut pixel = default_pixel;
    let frame = interp.selected_frame_value();
    let defined = interp
        .symbol_value_key(cached_symbol!("tty-defined-color-alist"))
        .unwrap_or(Value::Nil);
    if string_text(value).is_ok_and(|text| !text.is_empty())
        && matches!(defined.kind(), Kind::Cons(_))
        && let Ok(alist) = interp.call_function_value(
            Value::Symbol("tty-color-alist".into()),
            Some("tty-color-alist"),
            &[frame],
            env,
        )
        && let Ok(def) = crate::lisp::primitives::call(interp, "assoc", &[*value, alist], env)
        && let Some((_, rest)) = def.cons_values()
        && let Some((index, _)) = rest.cons_values()
        && let Kind::Integer(index) = index.kind()
    {
        pixel = index;
    }
    if pixel == default_pixel {
        let Kind::Frame(frame) = frame.kind() else {
            return Some(pixel);
        };
        match super::faces::tty_defined_color_pixel(interp, env, frame, value) {
            Ok(Some(loaded)) => pixel = loaded,
            Ok(None) => {
                let _ = add_to_log(interp, "Unable to load color \"%s\"", &[*value], env);
            }
            Err(_) => {}
        }
    }
    Some(pixel)
}

/// realize_tty_face's underline of VALUE: (type, color pixel), nil none,
/// t or a string (its color) a single line, and a (:color COLOR :style
/// STYLE) list its style's line (a single one for an unknown style) and
/// COLOR's pixel (none for `foreground-color').
fn tty_underline(interp: &mut Interpreter, env: &mut Env, value: &Value) -> (u8, i64) {
    match value.kind() {
        Kind::Nil => (FACE_NO_UNDERLINE, 0),
        Kind::T => (FACE_UNDERLINE_SINGLE, 0),
        Kind::StringObject(_) => (
            FACE_UNDERLINE_SINGLE,
            map_tty_color(interp, env, value, true).unwrap_or(0),
        ),
        Kind::Cons(_) => {
            let (mut kind, mut color) = (FACE_UNDERLINE_SINGLE, 0);
            let mut tail = *value;
            while let Some((keyword, rest)) = tail.cons_values() {
                let Some((value, rest)) = rest.cons_values() else {
                    break;
                };
                tail = rest;
                if crate::lisp::types::eq_symbol_named(keyword, ":color") {
                    if crate::lisp::types::eq_symbol_named(value, "foreground-color") {
                        color = 0;
                    } else if value.is_string() {
                        color = map_tty_color(interp, env, &value, true).unwrap_or(0);
                    }
                } else if crate::lisp::types::eq_symbol_named(keyword, ":style") {
                    kind = [
                        ("line", FACE_UNDERLINE_SINGLE),
                        ("double-line", FACE_UNDERLINE_DOUBLE_LINE),
                        ("wave", FACE_UNDERLINE_WAVE),
                        ("dots", FACE_UNDERLINE_DOTS),
                        ("dashes", FACE_UNDERLINE_DASHES),
                    ]
                    .into_iter()
                    .find(|(name, _)| crate::lisp::types::eq_symbol_named(value, name))
                    .map_or(FACE_UNDERLINE_SINGLE, |(_, kind)| kind);
                }
            }
            (kind, color)
        }
        _ => (FACE_NO_UNDERLINE, 0),
    }
}

/// realize_tty_face's tty_bold_p and tty_italic_p: FONT_WEIGHT_NAME_NUMERIC
/// above medium's 100, FONT_SLANT_NAME_NUMERIC other than normal's 100.
fn tty_weight_bold(interp: &mut Interpreter, value: &Value) -> bool {
    crate::lisp::primitives::font::style_name_numeric(
        interp,
        crate::lisp::primitives::font::FONT_WEIGHT_INDEX,
        value,
    )
    .is_ok_and(|weight| weight > 100)
}

fn tty_slant_italic(interp: &mut Interpreter, value: &Value) -> bool {
    crate::lisp::primitives::font::style_name_numeric(
        interp,
        crate::lisp::primitives::font::FONT_SLANT_INDEX,
        value,
    )
    .is_ok_and(|slant| slant != 100)
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
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name.as_str().starts_with(':'))
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
        .find(|pair| crate::lisp::types::eq_symbol_named(pair[0], ":inherit"))
        .map(|pair| &pair[1])
        .filter(|value| value.is_truthy());
    let mut merged = inherited
        .map(|value| resolve_tty_face_reference_options(interp, env, value, depth + 1))
        .unwrap_or_default();
    for pair in pairs {
        let Kind::Symbol(name) = pair[0].kind() else {
            continue;
        };
        let value = &pair[1];
        if crate::lisp::types::eq_symbol_named(value, "unspecified") {
            continue;
        }
        match name.as_ref() {
            ":foreground" => merged.foreground = map_tty_color(interp, env, value, true),
            ":background" => merged.background = map_tty_color(interp, env, value, false),
            ":weight" => merged.bold = Some(tty_weight_bold(interp, value)),
            ":slant" => merged.italic = Some(tty_slant_italic(interp, value)),
            ":underline" => merged.underline = Some(tty_underline(interp, env, value)),
            ":inverse-video" => merged.reverse = Some(value.is_truthy()),
            ":strike-through" => merged.strike_through = Some(value.is_truthy()),
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
            .filter(|value| !crate::lisp::types::eq_symbol_named(value, "unspecified"))
    };
    let foreground = attribute(interp, env, ":foreground");
    let background = attribute(interp, env, ":background");
    let weight = attribute(interp, env, ":weight");
    let slant = attribute(interp, env, ":slant");
    let underline = attribute(interp, env, ":underline");
    TtyFaceAttrOptions {
        foreground: foreground.and_then(|value| map_tty_color(interp, env, &value, true)),
        background: background.and_then(|value| map_tty_color(interp, env, &value, false)),
        bold: weight.map(|weight| tty_weight_bold(interp, &weight)),
        italic: slant.map(|slant| tty_slant_italic(interp, &slant)),
        underline: underline.map(|value| tty_underline(interp, env, &value)),
        reverse: attribute(interp, env, ":inverse-video").map(|value| value.is_truthy()),
        strike_through: attribute(interp, env, ":strike-through").map(|value| value.is_truthy()),
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
            .buffer_local_value_key(buffer_id, cached_symbol!("char-property-alias-alist"))
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                let key = entry.car_if_list()?;
                crate::lisp::types::eq_symbol_named(key, "face")
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
                    .symbol_value_key(cached_symbol!("mark-active"))
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
            .symbol_value_key(cached_symbol!("transient-mark-mode"))
            .is_some_and(|mode| mode.is_truthy())
        && interp
            .symbol_value_key(cached_symbol!("mark-active"))
            .is_some_and(|active| active.is_truthy())
        // The buffer's mark marker is native buffer.c state; `mark' itself
        // is GNU simple.el's and must not enter the native dispatcher.
        && let Some(mark) = interp.buffer.borrow().mark()
    {
        let point = interp.current_buffer().pt();
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
    window: WindowRef,
    point: usize,
    metrics: InteractiveWindowMetrics,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(
        interp,
        env,
        window,
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
    window: WindowRef,
    point: usize,
    metrics: InteractiveWindowMetrics,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(
        interp,
        env,
        window,
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
    window: WindowRef,
    point: usize,
    metrics: InteractiveWindowMetrics,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(
        interp,
        env,
        window,
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
    window: WindowRef,
    point: usize,
    metrics: InteractiveWindowMetrics,
    format_variable: &str,
    width: usize,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    let saved_window = interp.selected_window_ref();
    let saved_buffer = interp.current_buffer_id();
    let saved_metrics = interactive_window_metrics();
    let buffer_id = window.live_buffer_id(interp).unwrap_or(saved_buffer);
    interp.set_selected_window(Some(window));
    let switched = buffer_id != saved_buffer && interp.set_current_buffer_id(buffer_id).is_ok();
    let saved_point = interp.current_buffer().pt();
    interp.buffer.borrow_mut().goto_char(point);
    set_interactive_window_metrics(Some(metrics));
    mode_line::record_window_end(interp, window, metrics.window_end);
    let result = {
        // display_mode_lines: the window's parameter, else the buffer's
        // format.
        let parameter = window_parameter_value(window, &Value::symbol(format_variable));
        let format = if parameter.is_nil() {
            interp.symbol_value(format_variable).unwrap_or(Value::Nil)
        } else {
            parameter
        };
        mode_line::display_mode_line(
            interp,
            env,
            window,
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
    interp.set_selected_window(Some(saved_window));
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
                        (Kind::Symbol(key), Some(Kind::Integer(target)))
                            if key.is_named(":align-to") =>
                        {
                            Some((target.max(0) as usize).saturating_sub(x))
                        }
                        (Kind::Symbol(key), Some(Kind::Integer(width)))
                            if key.is_named(":width") =>
                        {
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
    let window = interp.selected_window_ref();
    let format = interp
        .symbol_value_key(cached_symbol!("mode-line-format"))
        .unwrap_or(Value::Nil);
    let (text, _) =
        mode_line::display_mode_line(interp, env, window, format, 200, true, &UnitCells)?;
    Ok(text.trim_end().to_string())
}

/// window.c's decode_live_window: a window showing a buffer
/// (CHECK_LIVE_WINDOW), nil or none the selected window, unchecked.
pub(super) fn decode_live_window(
    interp: &Interpreter,
    window: Option<&Value>,
) -> Result<WindowRef, LispError> {
    let Some(window) = window.filter(|window| !window.is_nil()).cloned() else {
        return Ok(interp.selected_window_ref());
    };
    // WINDOW_LIVE_P of the window: a leaf showing a live buffer.
    WindowRef::from_value(&window)
        .filter(|record| record.live_buffer_id(interp).is_some())
        .ok_or_else(|| LispError::WrongTypeArgument("window-live-p".into(), window))
}

fn select_window_value(
    interp: &mut Interpreter,
    window: &Value,
    norecord: bool,
) -> Result<Value, LispError> {
    select_window_marking(interp, window, norecord, !norecord)
}

/// window.c:select_window, NORECORD's nil or `mark-for-redisplay'
/// (MARK) marking the old and new selected windows for redisplay, where
/// another NORECORD only has redisplay consider the other windows.
fn select_window_marking(
    interp: &mut Interpreter,
    window: &Value,
    norecord: bool,
    mark: bool,
) -> Result<Value, LispError> {
    let w = decode_live_window(interp, Some(window))?;
    let previous_window = interp.selected_window_ref();
    let target_buffer_id = window_buffer_id(interp, window);
    // select_window_1: the point of the old selected window's buffer goes
    // into that window's point marker, and the new window's point marker
    // gives its buffer's point.  The selected window selected again keeps
    // its buffer's point (GNU's `save-selected-window' relies on this).
    let target_point = if w != previous_window {
        // Mark the windows for redisplay since the selected window has a
        // different mode line.
        if mark {
            interp.wset_redisplay(previous_window);
            interp.wset_redisplay(w);
        } else {
            interp.redisplay_other_windows();
        }
        // `if (BUFFERP (ow->contents))': a deleted old selected window
        // keeps its unchained point marker.
        if let Some(previous_buffer_id) = interp.selected_window_contents_buffer_id() {
            let previous_point = interp.buffer_point_by_id(previous_buffer_id);
            set_window_marker(
                interp,
                previous_window,
                WindowMarker::Pointm,
                previous_buffer_id,
                previous_point,
            );
        }
        // set_point_from_marker's marker_position, None for a marker that
        // points nowhere: the error comes once the window is selected.
        window_marker(w, WindowMarker::Pointm).map(|marker| marker.position())
    } else {
        None
    };
    if let Some(frame) = w.frame() {
        interp.note_selected_frame(frame);
    }
    interp.set_selected_window(Some(w));
    if matches!(target_point, Some(None)) {
        // Fset_buffer (w->contents) came first.
        if let Some(buffer_id) = target_buffer_id
            && interp.has_buffer_id(buffer_id)
        {
            interp.set_current_buffer_id(buffer_id)?;
        }
        return Err(LispError::Signal("Marker does not point anywhere".into()));
    }
    let target_point = target_point.flatten();
    // bset_last_selected_window: the window's buffer was last selected in
    // it (unshow_buffer leaves that window's point in the buffer).
    if w != previous_window
        && let Some(buffer_id) = target_buffer_id
    {
        let window_value = w.value();
        if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
            buffer.set_last_selected_window(window_value);
        }
    }
    if !norecord {
        interp.record_window_selection(w);
    }
    if let Some(buffer_id) = target_buffer_id
        && interp.has_buffer_id(buffer_id)
    {
        interp.switch_to_buffer_id_preserving_window_history(buffer_id)?;
        // set_point_from_marker, inside the accessible portion.
        if let Some(target_point) = target_point {
            interp.buffer.borrow_mut().goto_char(target_point);
        }
        if !norecord {
            interp.record_buffer_front(buffer_id);
        }
    }
    Ok(*window)
}

/// The end of buffer.c:record_buffer, after select_window recorded the
/// selected window's buffer (current now): run_buffer_list_update_hook,
/// unless the buffer inhibits its hooks.
fn run_record_buffer_hook(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let buffer_id = interp.current_buffer_id();
    if interp.buffer_hooks_inhibited(buffer_id) {
        return Ok(());
    }
    run_hook(
        interp,
        &cached_symbol!("buffer-list-update-hook").symbol(),
        env,
    )
}

/// MINI_WINDOW_P of a window that is not deleted.  window.h tests w->mini
/// alone, which a deleted minibuffer window keeps; such a window has
/// been no minibuffer window to the callers here, and stays none.
pub(super) fn window_is_minibuffer(window: WindowRef) -> bool {
    window.mini() && window.is_valid()
}

/// WINDOW_HORIZONTAL_COMBINATION_P / WINDOW_VERTICAL_COMBINATION_P: the
/// combination's direction (w->horizontal) for an internal window, None
/// for a leaf or a deleted window.
fn window_combination(window: WindowRef) -> Option<bool> {
    window.is_internal().then(|| window.horizontal())
}

fn window_pixel_size(interp: &Interpreter, window: WindowRef, horflag: bool) -> i64 {
    let (width, height, _, _) = window_geometry(interp, window);
    if horflag { width } else { height }
}

fn window_new_pixel(window: WindowRef) -> i64 {
    window.new_pixel().as_integer().unwrap_or(0)
}

/// `wset_parent (w, TARGET)', `wset_next' or `wset_prev' by SLOT, nil
/// for no TARGET.
/// wset_combination (W, HORFLAG, CHILD): make W an internal window of
/// HORFLAG's direction whose first child is CHILD, or (CHILD nil) leave
/// it without contents, which marks a deleted window ("when an internal
/// window is deleted and VAL is nil, HORFLAG is meaningless": the bit
/// stays as it was).
fn set_window_combination(
    interp: &mut Interpreter,
    window: WindowRef,
    horflag: bool,
    child: Option<WindowRef>,
) {
    let child = child.map_or(Value::Nil, WindowRef::value);
    interp.wset_combination(window, horflag, child);
}

/// A window without contents keeps no links (GNU leaves them dangling
/// and refuses every accessor on a deleted window).
fn clear_deleted_window(interp: &mut Interpreter, window: WindowRef) {
    set_window_combination(interp, window, false, None);
    window.set_parent_window(None);
    window.set_prev_sibling(None);
    window.set_next_sibling(None);
}

/// window.c:window_resize_check: whether the new pixel sizes staged in
/// W's subtree add up (an iso-combination's children sum to W's, an
/// ortho-combination's children each equal W's), and no leaf gets
/// smaller than `window-safe-min-width' (2) or `-height' (1).
fn window_resize_check(window: WindowRef, horflag: bool) -> bool {
    let new_pixel = window_new_pixel(window);
    let Some(horizontal) = window_combination(window) else {
        return new_pixel >= if horflag { 2 } else { 1 };
    };
    let mut child = window.first_child();
    if horizontal == horflag {
        let mut remaining = new_pixel;
        while let Some(w) = child {
            if !window_resize_check(w, horflag) {
                return false;
            }
            remaining -= window_new_pixel(w);
            if remaining < 0 {
                return false;
            }
            child = w.next_sibling();
        }
        remaining == 0
    } else {
        while let Some(w) = child {
            if window_new_pixel(w) != new_pixel || !window_resize_check(w, horflag) {
                return false;
            }
            child = w.next_sibling();
        }
        true
    }
}

/// window.c:window_resize_apply: commit W's staged new pixel size (and
/// a numeric new normal size) in HORFLAG's dimension, its total size
/// from that, then place its children (pixel edge and column or line)
/// from W's edge, advancing the edge across an iso-combination.
fn window_resize_apply(
    interp: &mut Interpreter,
    window: WindowRef,
    horflag: bool,
) -> Result<(), LispError> {
    let new_pixel = window_new_pixel(window);
    let (mut width, mut height, left, top) = window_geometry(interp, window);
    if horflag {
        width = new_pixel;
    } else {
        height = new_pixel;
    }
    set_window_geometry(window, (width, height, left, top));
    if horflag {
        window.set_total_cols(Some(new_pixel));
    } else {
        window.set_total_lines(Some(new_pixel));
    }
    let new_normal = window.new_normal();
    if matches!(new_normal.kind(), Kind::Integer(_) | Kind::Float(_)) {
        window.set_normal_size(horflag, new_normal);
    }
    let Some(horizontal) = window_combination(window) else {
        // Bug#15957.
        window.set_window_end_valid(false);
        return Ok(());
    };
    let mut edge = if horflag { left } else { top };
    let mut child = window.first_child();
    while let Some(w) = child {
        let (child_width, child_height, child_left, child_top) = window_geometry(interp, w);
        let placed = if horflag {
            (child_width, child_height, edge, child_top)
        } else {
            (child_width, child_height, child_left, edge)
        };
        set_window_geometry(w, placed);
        if horflag {
            w.set_left_col(Some(edge));
        } else {
            w.set_top_line(Some(edge));
        }
        window_resize_apply(interp, w, horflag)?;
        if horizontal == horflag {
            edge += window_pixel_size(interp, w, horflag);
        }
        child = w.next_sibling();
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
    let root = frame_root_window(frame);
    if grow == 0 || !window_resize_check(root, false) {
        return Ok(false);
    }
    let mini = frame_minibuffer_window(frame);
    resize_mini_window_apply(interp, frame, mini, -grow)?;
    Ok(true)
}

/// window.c:resize_mini_window_apply: mini window W grows by DELTA
/// pixels, the root window tree takes its staged heights and W stays
/// right below it.
fn resize_mini_window_apply(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    mini: WindowRef,
    delta: i64,
) -> Result<(), LispError> {
    let root = frame_root_window(frame);
    let (width, height, left, top) = window_geometry(interp, mini);
    set_window_geometry(mini, (width, height + delta, left, top));
    mini.set_total_lines(Some(height + delta));
    window_resize_apply(interp, root, false)?;
    let (_, root_height, _, root_top) = window_geometry(interp, root);
    let (_, root_lines, _, root_top_line) = window_char_geometry(root);
    set_window_geometry(mini, (width, height + delta, left, root_top + root_height));
    mini.set_top_line(Some(root_top_line + root_lines));
    // fset_redisplay (f).
    interp.fset_redisplay(frame);
    Ok(())
}

/// window.c:replace_window: NEW takes OLD's place among its siblings and
/// under its parent (and as its frame's root); with SETFLAG, also OLD's
/// edges and normal sizes, OLD's normal sizes reverting to 1.0.
fn replace_window(
    interp: &mut Interpreter,
    old: WindowRef,
    new: WindowRef,
    setflag: bool,
) -> Result<(), LispError> {
    let frame = old.frame().expect("window stores its owning frame");
    if frame.borrow().root_window == Some(old) {
        frame.borrow_mut().root_window = Some(new);
    }
    if setflag {
        set_window_geometry(new, window_geometry(interp, old));
        let (total_cols, total_lines, left_col, top_line) = window_char_geometry(old);
        new.set_left_col(Some(left_col));
        new.set_top_line(Some(top_line));
        new.set_total_cols(Some(total_cols));
        new.set_total_lines(Some(total_lines));
        for horflag in [true, false] {
            let normal = old.normal_size(horflag);
            new.set_normal_size(horflag, normal);
            old.set_normal_size(horflag, Value::float(1.0));
        }
    }
    let next = old.next_sibling();
    new.set_next_sibling(next);
    if let Some(next) = next {
        next.set_prev_sibling(Some(new));
    }
    let prev = old.prev_sibling();
    new.set_prev_sibling(prev);
    if let Some(prev) = prev {
        prev.set_next_sibling(Some(new));
    }
    let parent = old.parent_window();
    new.set_parent_window(parent);
    if let Some(parent) = parent
        && parent.first_child() == Some(old)
    {
        // wset_combination (XWINDOW (tem), XWINDOW (tem)->horizontal, new).
        let horizontal = window_combination(parent) == Some(true);
        set_window_combination(interp, parent, horizontal, Some(new));
    }
    Ok(())
}

/// window.c:make_parent_window: a copy of WINDOW (window.c memcpy's the
/// whole struct) replaces it in the tree as an internal window of
/// HORFLAG's direction whose only child is WINDOW.
fn make_parent_window(
    interp: &mut Interpreter,
    window: WindowRef,
    horflag: bool,
) -> Result<WindowRef, LispError> {
    // p = allocate_window (), and the memcpy of the window into it: its
    // Lisp fields, its frame among them; its use time, pixel edges and
    // sizes, hscroll and the bits of its kind; and the fields only
    // make_window gives values here.  The copy has a sequence number of
    // its own (`p->sequence_number = ++sequence_number'), and its cursor
    // is on, whatever the window's.  Its columns and lines replace_window
    // gives it below; the change stamp and sizes window_change_record
    // recorded, the margins, the base line and what redisplay left in the
    // window it starts without.
    let parent = interp.allocate_window();
    parent.set_lisp_values(window.lisp_values());
    parent.copy_display_fields(window);
    interp.window_sequence_number += 1;
    parent.set_sequence_number(interp.window_sequence_number);
    parent.set_contents(window.contents());
    parent.set_use_time(window.use_time());
    parent.set_pixel_left(window.pixel_left());
    parent.set_pixel_top(window.pixel_top());
    parent.set_pixel_width(window.pixel_width());
    parent.set_pixel_height(window.pixel_height());
    parent.set_hscroll(window.hscroll());
    parent.set_min_hscroll(window.min_hscroll());
    parent.set_mini(window.mini());
    parent.set_horizontal(window.horizontal());
    parent.set_suspend_auto_hscroll(window.suspend_auto_hscroll());
    // make_parent_window's adjust_window_count (p, 1): P's contents,
    // copied from the window's, are a buffer until wset_buffer below.
    interp.adjust_window_count(parent, true);
    replace_window(interp, window, parent, true)?;
    window.set_next_sibling(None);
    window.set_prev_sibling(None);
    window.set_parent_window(Some(parent));
    interp.wset_buffer(parent, Value::Nil);
    for which in [
        WindowMarker::Start,
        WindowMarker::OldPointm,
        WindowMarker::Pointm,
    ] {
        parent.set_marker_field(which, Value::Nil);
    }
    parent.set_combination_limit(Value::Nil);
    parent.set_prev_buffers(Value::Nil);
    parent.set_next_buffers(Value::Nil);
    parent.set_window_parameters(Value::Nil);
    set_window_combination(interp, parent, horflag, Some(window));
    Ok(parent)
}

/// window.c:recombine_windows: an internal WINDOW whose parent combines
/// in the same direction (and whose combination limit is nil) hands its
/// children to that parent, each child's normal size becoming its share
/// of the parent, and WINDOW is deleted.
fn recombine_windows(interp: &mut Interpreter, window: WindowRef) -> Result<(), LispError> {
    let Some(parent) = window.parent_window() else {
        return Ok(());
    };
    if window.combination_limit().is_truthy() {
        return Ok(());
    }
    let (Some(parent_horizontal), Some(horflag)) =
        (window_combination(parent), window_combination(window))
    else {
        return Ok(());
    };
    if parent_horizontal != horflag {
        return Ok(());
    }
    let first = window
        .first_child()
        .expect("an internal window has children");
    let prev = window.prev_sibling();
    let next = window.next_sibling();
    match prev {
        // wset_combination (p, horflag, child).
        None => set_window_combination(interp, parent, horflag, Some(first)),
        Some(prev) => {
            first.set_prev_sibling(Some(prev));
            prev.set_next_sibling(Some(first));
        }
    }
    let parent_size = window_pixel_size(interp, parent, horflag) as f64;
    let mut child = Some(first);
    while let Some(w) = child {
        w.set_parent_window(Some(parent));
        let normal = window_pixel_size(interp, w, horflag) as f64 / parent_size;
        w.set_normal_size(horflag, Value::float(normal));
        child = w.next_sibling();
        if child.is_none()
            && let Some(next) = next
        {
            w.set_next_sibling(Some(next));
            next.set_prev_sibling(Some(w));
        }
    }
    clear_deleted_window(interp, window);
    Ok(())
}

/// window.c:run_funs: call each element of FUNS but t, a list's alone.
fn run_window_funs(interp: &mut Interpreter, funs: Value, env: &mut Env) -> Result<(), LispError> {
    let mut tail = funs;
    while let Kind::Cons(_) = tail.kind() {
        let function = tail.car()?;
        if !matches!(function.kind(), Kind::T) {
            call_function_value(interp, &function, &[], env)?;
        }
        tail = tail.cdr()?;
    }
    Ok(())
}

/// window.c:select_window_norecord, the unwinding of a selection: a
/// window still live is selected as `(select-window WINDOW t)' does.
fn select_window_norecord(interp: &mut Interpreter, window: WindowRef) -> Result<(), LispError> {
    if window.live_buffer_id(interp).is_some() {
        select_window_value(interp, &window.value(), true)?;
    }
    Ok(())
}

/// window.c:run_window_configuration_change_hook for FRAME: the
/// buffer-local values of `window-configuration-change-hook' run with
/// the window showing their buffer selected (Fselect_window with
/// NORECORD, each window of FRAME's list but its minibuffer window),
/// then the default value with FRAME selected; all with the selected
/// window's buffer current, and the selections and that buffer undone
/// after, an error included.
fn run_window_configuration_change_hook(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    env: &mut Env,
) -> Result<(), LispError> {
    let global = interp
        .default_value_key(cached_symbol!("window-configuration-change-hook"))
        .unwrap_or(Value::Nil);
    if interp.hooks_cleared {
        return Ok(());
    }
    // `if (current_buffer != XBUFFER (Fwindow_buffer (Qnil))) Fset_buffer
    // (Fwindow_buffer (Qnil))': a deleted selected window's buffer is nil,
    // and Fset_buffer's Fget_buffer of nil is CHECK_STRING's error.
    let saved_buffer = interp.current_buffer_id();
    let selected_buffer = interp
        .selected_window_contents_buffer_id()
        .ok_or_else(|| wrong_type_argument("stringp", Value::Nil))?;
    let switch_buffer = selected_buffer != saved_buffer;
    if switch_buffer {
        interp.set_current_buffer_id(selected_buffer)?;
    }
    let saved_frame = interp.selected_frame_id;
    let switch_frame = saved_frame != frame;
    let mut result = if switch_frame {
        super::frames::select_frame(interp, frame, true, env)
    } else {
        Ok(())
    };
    if result.is_ok() {
        result = (|| {
            let start = frame.borrow().selected_window.map(WindowRef::value);
            let frames = [frame];
            let windows = window_list_windows(
                interp,
                Some(&Value::symbol("lambda")),
                start.as_ref(),
                &frames,
            );
            for window in windows {
                // Fwindow_buffer: nil for a window a hook deleted after the
                // list was made, and Flocal_variable_p of nil asks the
                // current buffer.
                let buffer_id = window.live_buffer_id(interp);
                let hook = cached_symbol!("window-configuration-change-hook").symbol();
                if !interp.has_buffer_local_binding_symbol(
                    buffer_id.unwrap_or(interp.current_buffer_id()),
                    &hook,
                ) {
                    continue;
                }
                let saved_window = interp.selected_window_ref();
                let mut inner = select_window_norecord(interp, window);
                if inner.is_ok() {
                    // Fbuffer_local_value (..., buffer): CHECK_BUFFER, and a
                    // void local binding's `void-variable'.
                    inner = match buffer_id {
                        Some(id) => match interp.buffer_local_value_symbol(id, &hook) {
                            Some(local) => run_window_funs(interp, local, env),
                            None => Err(LispError::SignalValue(Value::list([
                                Value::symbol("void-variable"),
                                Value::Symbol(hook),
                            ]))),
                        },
                        None => Err(LispError::WrongTypeArgument("bufferp".into(), Value::Nil)),
                    };
                }
                let unwound = select_window_norecord(interp, saved_window);
                inner?;
                unwound?;
            }
            run_window_funs(interp, global, env)
        })();
    }
    if switch_frame && interp.frame_is_live(saved_frame) {
        let unwound = super::frames::select_frame(interp, saved_frame, true, env);
        result = result.and(unwound);
    }
    if switch_buffer && interp.has_buffer_id(saved_buffer) {
        let unwound = interp.set_current_buffer_id(saved_buffer);
        result = result.and(unwound);
    }
    result
}

/// window.c:unshow_buffer, then the unchain_marker calls of the window's
/// start, point and old point that delete_window and
/// delete_all_child_windows make: the buffer remembers the window's
/// start, and takes its point unless the selected window shows it.
fn unshow_buffer(interp: &mut Interpreter, window: WindowRef) -> Result<(), LispError> {
    interp.unshow_window_buffer(window)?;
    unchain_window_markers(window);
    Ok(())
}

/// window.c:delete_all_child_windows: delete WINDOW, the siblings after
/// it and all their descendants, children after their later siblings.  A
/// leaf's buffer stays in its combination limit, where a later
/// Fset_window_configuration that revives the window finds it.
fn delete_all_child_windows(interp: &mut Interpreter, window: WindowRef) -> Result<(), LispError> {
    if let Some(next) = window.next_sibling() {
        delete_all_child_windows(interp, next)?;
    }
    if let Some(child) = window.first_child() {
        delete_all_child_windows(interp, child)?;
    } else {
        unshow_buffer(interp, window)?;
        // wset_combination_limit (w, w->contents).
        let contents = window.contents();
        if matches!(contents.kind(), Kind::Buffer(_)) {
            window.set_combination_limit(contents);
        }
    }
    clear_deleted_window(interp, window);
    Ok(())
}

/// Fselect_window for a window that is its frame's choice: select it when
/// its frame is the selected frame, else only record it as the frame's
/// selected window.
fn select_window_for_frame(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    window: WindowRef,
    norecord: bool,
) -> Result<(), LispError> {
    if frame == interp.selected_frame_id {
        select_window_value(interp, &window.value(), norecord)?;
    } else {
        frame.borrow_mut().selected_window = Some(window);
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
    if old.is_nil() || WindowRef::from_value(old).is_none() {
        return Err(wrong_type_argument("windowp", *old));
    }
    let old_window = decode_any_window(interp, old)?;
    let frame = old_window.frame().expect("window stores its owning frame");
    let pixel_size = match pixel_size.kind() {
        Kind::Integer(size) => size,
        _ => return Err(wrong_type_argument("fixnump", *pixel_size)),
    };
    let total_size = pixel_size;
    let old_parent = old_window.parent_window();
    let limit_is_t = matches!(
        interp
            .symbol_value_key(cached_symbol!("window-combination-limit"))
            .map(|value| value.kind()),
        Some(Kind::T)
    );
    let combination_limit = limit_is_t
        || match old_parent {
            None => true,
            Some(parent) => window_combination(parent) != Some(horflag),
        };
    let old_live = window_buffer_id(interp, old).is_some() && !window_is_minibuffer(old_window);
    let reference = if old_live {
        old_window
    } else {
        frame_selected_window(frame)
    };
    let resize = interp
        .symbol_value_key(cached_symbol!("window-combination-resize"))
        .is_some_and(|value| value.is_truthy());
    if window_is_minibuffer(old_window) {
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
        parent.set_new_pixel(Value::Integer(parent_size - pixel_size));
        let fits = window_resize_check(parent, horflag);
        if !fits {
            return Err(crate::lisp::primitives::values::doprnt_error(
                interp,
                "Window sizes don't fit",
                &[],
            ));
        }
        parent.set_new_pixel(Value::Integer(parent_size));
    } else if !window_resize_check(old_window, horflag) {
        return Err(LispError::Signal("Resizing old window failed".into()));
    } else if pixel_size + window_new_pixel(old_window)
        != window_pixel_size(interp, old_window, horflag)
    {
        return Err(crate::lisp::primitives::values::doprnt_error(
            interp,
            "Sum of sizes of old and new window don't fit",
            &[],
        ));
    }

    let parent = if combination_limit {
        let new_normal = old_window.normal_size(horflag);
        let parent = make_parent_window(interp, old_window, horflag)?;
        if limit_is_t {
            parent.set_combination_limit(Value::T);
        }
        let size = window_pixel_size(interp, old_window, horflag);
        let (old_cols, old_lines, _, _) = window_char_geometry(old_window);
        let total = if horflag { old_cols } else { old_lines };
        parent.set_new_pixel(Value::Integer(size));
        parent.set_new_total(Value::Integer(total));
        parent.set_new_normal(new_normal);
        parent
    } else {
        old_parent.expect("a combined window has a parent")
    };

    let reference_buffer = reference
        .live_buffer_id(interp)
        .ok_or_else(|| LispError::Signal("Attempt to split a non-live window".into()))?;
    let start = window_marker_position(reference, WindowMarker::Start).unwrap_or(1);
    let (old_width, old_height, old_left, old_top) = window_geometry(interp, old_window);
    // Orthogonal coordinates and sizes come from OLD; the iso ones are
    // assigned by window_resize_apply below.
    let geometry = if horflag {
        (0, old_height, 0, old_top)
    } else {
        (old_width, 0, old_left, 0)
    };
    let new = make_leaf_window(interp, reference_buffer, start, false, geometry);
    // Fsplit_window_internal's set_window_buffer of the new window.
    interp.adjust_window_count(new, true);
    new.set_frame(Value::Frame(frame));
    let (old_cols, old_lines, old_left_col, old_top_line) = window_char_geometry(old_window);
    if horflag {
        new.set_top_line(Some(old_top_line));
        new.set_total_lines(Some(old_lines));
    } else {
        new.set_left_col(Some(old_left_col));
        new.set_total_cols(Some(old_cols));
    }
    let old_parent = old_window.parent_window();
    new.set_parent_window(old_parent);
    if before {
        let prev = old_window.prev_sibling();
        new.set_prev_sibling(prev);
        match prev {
            None => set_window_combination(interp, parent, horflag, Some(new)),
            Some(prev) => prev.set_next_sibling(Some(new)),
        }
        new.set_next_sibling(Some(old_window));
        old_window.set_prev_sibling(Some(new));
    } else {
        let next = old_window.next_sibling();
        new.set_next_sibling(next);
        if let Some(next) = next {
            next.set_prev_sibling(Some(new));
        }
        new.set_prev_sibling(Some(old_window));
        old_window.set_next_sibling(Some(new));
    }
    new.set_new_pixel(Value::Integer(pixel_size));
    let mut sum = 0;
    let mut child = parent.first_child();
    while let Some(w) = child {
        if w != new {
            sum += w.new_total().as_integer().unwrap_or(0);
        }
        child = w.next_sibling();
    }
    let (parent_cols, parent_lines, _, _) = window_char_geometry(parent);
    new.set_new_total(Value::Integer(
        if horflag { parent_cols } else { parent_lines } - sum,
    ));
    new.set_new_normal(*normal_size);
    window_resize_apply(interp, parent, horflag)?;
    // NEW takes REFERENCE's margins (fringes and scroll bars have no
    // terminal width); set_window_buffer (new, r->contents, true, true)
    // keeps them, and NEW, having had no buffer, takes the buffer's
    // last_window_start and point.
    let (left_margin, right_margin) = crate::lisp::primitives::window_margin_cols(reference);
    crate::lisp::primitives::set_window_margin_cols(new, left_margin, right_margin);
    interp.show_buffer_in_window(new, reference_buffer, false);
    set_window_buffer_hooks(interp, new, reference_buffer, env)?;
    Ok(new.value())
}

/// window.c:set_window_margins: WINDOW's margins become LEFT_WIDTH and
/// RIGHT_WIDTH columns (nil for none), unless that changes nothing or
/// would leave the window under MIN_SAFE_WINDOW_WIDTH columns; true when
/// the margins changed (the caller's apply_window_adjustment then
/// redisplays it).
fn set_window_margins(
    interp: &mut Interpreter,
    window: WindowRef,
    left_width: Value,
    right_width: Value,
) -> Result<bool, LispError> {
    // extract_dimension: check_integer_range (dimension, 0, INT_MAX).
    let extract = |dimension: Value| -> Result<i64, LispError> {
        if dimension.is_nil() {
            return Ok(0);
        }
        crate::lisp::primitives::values::check_integer_range(dimension, 0, i64::from(i32::MAX))
    };
    let left = extract(left_width)?;
    let right = extract(right_width)?;
    let (old_left, old_right) = crate::lisp::primitives::window_margin_cols(window);
    if old_left.unwrap_or(0) == left && old_right.unwrap_or(0) == right {
        return Ok(false);
    }
    // A text window has no fringes or scroll bars; MIN_SAFE_WINDOW_WIDTH
    // is two columns.
    if window_geometry(interp, window).0 - (left + right) < 2 {
        return Ok(false);
    }
    crate::lisp::primitives::set_window_margin_cols(window, Some(left), Some(right));
    Ok(true)
}

/// window.c:apply_window_adjustment: WINDOW is redisplayed and its
/// frame's glyphs adjusted (adjust_frame_glyphs), which on a terminal
/// garbages the frame whenever a window shows margins
/// (adjust_frame_glyphs_for_frame_redisplay's showing_window_margins_p).
fn apply_window_adjustment(interp: &mut Interpreter, w: WindowRef) {
    w.set_window_end_valid(false);
    interp.wset_redisplay(w);
    let showing_margins = crate::lisp::primitives::frames_window_list(interp)
        .into_iter()
        .any(|window| {
            let (left, right) = crate::lisp::primitives::window_margin_cols(window);
            left.unwrap_or(0) > 0 || right.unwrap_or(0) > 0
        });
    if showing_margins {
        crate::lisp::primitives::set_frame_garbaged();
    }
}

/// window.c:set_window_buffer: WINDOW shows BUFFER_ID.  Its start,
/// points and hscroll are the buffer's unless KEEP_MARGINS_P and the
/// window already shows that buffer; without KEEP_MARGINS_P its margins
/// are the buffer's (a same-buffer refresh included); RUN_HOOKS_P runs
/// `window-scroll-functions'.  It does not unshow the buffer the window
/// showed: Fset_window_buffer does that first, window_loop's
/// REPLACE_BUFFER_IN_WINDOWS_SAFELY does not.
pub(crate) fn set_window_buffer(
    interp: &mut Interpreter,
    window: WindowRef,
    buffer_id: u64,
    run_hooks_p: bool,
    keep_margins_p: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let buffer = interp.buffer_value(buffer_id).unwrap_or(Value::Nil);
    set_window_buffer_object(
        interp,
        window,
        (buffer_id, buffer),
        run_hooks_p,
        keep_margins_p,
        env,
    )
}

/// `set_window_buffer' of BUFFER, the object and its id: Fset_window_buffer
/// passes the buffer it tested for liveness before `record-window-buffer'
/// ran, and the window shows that object even when the hook killed it.
fn set_window_buffer_object(
    interp: &mut Interpreter,
    window: WindowRef,
    (buffer_id, buffer): (u64, Value),
    run_hooks_p: bool,
    keep_margins_p: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    // samebuf = EQ (buffer, w->contents).
    let samebuf = window.contents_buffer_id() == Some(buffer_id);
    interp.show_buffer_object_in_window(window, buffer_id, buffer, keep_margins_p && samebuf);
    if !keep_margins_p {
        // set_window_margins (w, BVAR (b, left_margin_cols), BVAR (b,
        // right_margin_cols)).
        let margin_width = |name: &str| {
            interp
                .buffer_local_value(buffer_id, name)
                .unwrap_or(Value::Nil)
        };
        let (left, right) = (
            margin_width("left-margin-width"),
            margin_width("right-margin-width"),
        );
        set_window_margins(interp, window, left, right)?;
        apply_window_adjustment(interp, window);
    }
    if run_hooks_p {
        set_window_buffer_hooks(interp, window, buffer_id, env)?;
    }
    Ok(())
}

/// window.c:replace_buffer_in_windows_safely: window_loop's
/// REPLACE_BUFFER_IN_WINDOWS_SAFELY over each frame of Vframe_list, from
/// the frame's selected window, minibuffer windows included.  A window
/// still showing BUFFER_ID is undedicated and shows other_buffer_safely's
/// buffer by set_window_buffer, its hooks not run and its margins the new
/// buffer's.  (GNU then makes the selected window's buffer current only
/// when it already is, which changes nothing.)  other_buffer_safely can
/// run Lisp that deletes frames: FOR_EACH_FRAME walks the list as it now
/// is, so each step takes the next live frame of the current list not
/// yet visited, and no frame is held across Lisp.
pub(crate) fn replace_buffer_in_windows_safely(
    interp: &mut Interpreter,
    buffer_id: u64,
) -> Result<(), LispError> {
    let mut visited: Vec<u64> = Vec::new();
    let mut env = Env::new();
    while let Some(frame) = interp
        .frame_states
        .iter()
        .find(|frame| frame.is_live() && !visited.contains(&frame.borrow().id))
        .copied()
    {
        visited.push(frame.borrow().id);
        let start = frame_selected_window(frame).value();
        for window in window_list_windows(interp, Some(&Value::T), Some(&start), &[frame]) {
            let shows = window.contents_buffer_id() == Some(buffer_id);
            if !shows {
                continue;
            }
            window.set_dedicated(Value::Nil);
            let other = interp.other_buffer_safely(buffer_id)?;
            set_window_buffer(interp, window, other, false, false, &mut env)?;
        }
    }
    Ok(())
}

/// set_window_buffer's run_hooks_p: `window-scroll-functions' see the
/// window's new start with BUFFER current.
fn set_window_buffer_hooks(
    interp: &mut Interpreter,
    w: WindowRef,
    buffer_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    let window = w.value();
    let start = Value::Integer(window_start_marker(interp, w, buffer_id) as i64);
    let original_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(buffer_id)?;
    let result = run_window_scroll_functions(interp, window, start, env);
    if interp.has_buffer_id(original_buffer) {
        interp.set_current_buffer_id(original_buffer)?;
    }
    result
}

/// `if (!NILP (Vwindow_scroll_functions)) run_hook_with_args_2
/// (Qwindow_scroll_functions, window, start)' in the current buffer.
fn run_window_scroll_functions(
    interp: &mut Interpreter,
    window: Value,
    start: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let hook = cached_symbol!("window-scroll-functions");
    if !interp
        .symbol_value_key(hook)
        .is_some_and(|value| !value.is_nil())
    {
        return Ok(());
    }
    run_hook_with_args(interp, &hook.symbol(), env, |interp, function, env| {
        call_function_value(interp, &function, &[window, start], env)?;
        Ok(Value::Nil)
    })
    .map(drop)
}

/// window.c:Fdelete_window_internal.  `delete-window' has staged the new
/// sizes of WINDOW's siblings; a parent left with one child is replaced
/// by it, which then merges into its new parent when that combines in
/// the same direction.
fn delete_window_from_tree(interp: &mut Interpreter, window: &Value) -> Result<Value, LispError> {
    let w = decode_any_window(interp, window)?;
    if !w.is_valid() {
        return Ok(Value::Nil);
    }
    let Some(parent) = w.parent_window() else {
        return Err(LispError::Signal(
            "Attempt to delete minibuffer or sole ordinary window".into(),
        ));
    };
    let prev = w.prev_sibling();
    let next = w.next_sibling();
    if prev.is_none() && next.is_none() {
        return Err(LispError::Signal(
            "Attempt to delete sole window of parent".into(),
        ));
    }
    let horflag = window_combination(parent) == Some(true);
    let frame = w.frame().expect("window stores its owning frame");
    let root = frame_root_window(frame);

    let before_sibling = prev.is_none();
    let sibling = if let Some(prev) = prev {
        prev.set_next_sibling(next);
        if let Some(next) = next {
            next.set_prev_sibling(Some(prev));
        }
        prev
    } else {
        let next = next.expect("a window with no previous sibling has a next one");
        next.set_prev_sibling(None);
        set_window_combination(interp, parent, horflag, Some(next));
        next
    };

    if !(window_resize_check(root, horflag)
        && window_new_pixel(root) == window_pixel_size(interp, root, horflag))
    {
        if before_sibling {
            sibling.set_prev_sibling(Some(w));
            set_window_combination(interp, parent, horflag, Some(w));
        } else {
            sibling.set_next_sibling(Some(w));
            if let Some(next) = next {
                next.set_prev_sibling(Some(w));
            }
        }
        return Err(LispError::Signal("Deletion failed".into()));
    }

    window_resize_apply(interp, parent, horflag)?;
    w.set_next_sibling(None);
    if let Some(child) = w.first_child() {
        delete_all_child_windows(interp, child)?;
    } else {
        unshow_buffer(interp, w)?;
    }
    clear_deleted_window(interp, w);

    if sibling.prev_sibling().is_none() && sibling.next_sibling().is_none() {
        replace_window(interp, parent, sibling, false)?;
        for horflag in [true, false] {
            let normal = parent.normal_size(horflag);
            sibling.set_normal_size(horflag, normal);
        }
        clear_deleted_window(interp, parent);
        recombine_windows(interp, sibling)?;
    }

    let selected = frame_selected_window(frame);
    if !is_live_ordinary_window(interp, selected) {
        let first = frame_window_tree_leaves(frame)
            .into_iter()
            .next()
            .expect("a frame keeps a live window");
        if selected == interp.selected_window_ref() {
            select_window_value(interp, &first.value(), true)?;
        } else {
            frame.borrow_mut().selected_window = Some(first);
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
    let mut w = decode_valid_window(interp, window)?;
    let frame = w.frame().expect("window stores its owning frame");
    let root_window = if root.is_nil() {
        frame_root_window(frame)
    } else {
        let root_window = decode_valid_window(interp, root)?;
        let mut ancestor = w.parent_window();
        while let Some(id) = ancestor
            && id != root_window
        {
            ancestor = id.parent_window();
        }
        if ancestor != Some(root_window) {
            return Err(LispError::Signal(
                "Specified root is not an ancestor of specified window".into(),
            ));
        }
        root_window
    };
    if w == root_window {
        return Ok(Value::Nil);
    }
    if window_is_minibuffer(w) {
        return Err(crate::lisp::primitives::values::doprnt_error(
            interp,
            "Can't expand minibuffer to full frame",
            &[],
        ));
    }

    let live = window_combination(w).is_none();
    // A live WINDOW's start and top line, for keeping the text at its
    // start where it was on the frame.
    let frame_top = window_geometry(interp, frame_root_window(frame)).3;
    // marker_position (w->start): a start that points nowhere signals
    // before anything changes.
    let start_and_top = if live {
        let buffer_id = w
            .live_buffer_id(interp)
            .unwrap_or(interp.current_buffer_id());
        let startpos = match window_marker(w, WindowMarker::Start) {
            Some(marker) => marker
                .position()
                .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?,
            None => window_start_marker(interp, w, buffer_id),
        };
        Some((
            buffer_id,
            startpos,
            window_geometry(interp, w).3 - frame_top,
        ))
    } else {
        None
    };
    let mut swindow = w;
    if live {
        if Some(w) != frame.borrow().selected_window {
            select_window_for_frame(interp, frame, w, false)?;
        }
    } else {
        // A selected window in WINDOW's subtree stays selected, else the
        // nearest one before it in the cyclic order.
        swindow = frame_selected_window(frame);
        let leaves = frame_window_tree_leaves(frame);
        loop {
            let mut ancestor = Some(swindow);
            while let Some(id) = ancestor
                && id != w
            {
                ancestor = id.parent_window();
            }
            if ancestor == Some(w) {
                break;
            }
            let position = leaves.iter().position(|id| *id == swindow).unwrap_or(0);
            swindow = leaves[(position + leaves.len() - 1) % leaves.len()];
        }
        if Some(swindow) != frame.borrow().selected_window {
            select_window_for_frame(interp, frame, swindow, false)?;
        }
    }

    let mut resize_failed = false;
    if !live {
        let (root_width, root_height, root_left, root_top) = window_geometry(interp, root_window);
        let (_, _, root_left_col, root_top_line) = window_char_geometry(root_window);
        for horflag in [false, true] {
            let (width, height, left, top) = window_geometry(interp, w);
            let delta = if horflag {
                w.set_left_col(Some(root_left_col));
                set_window_geometry(w, (width, height, root_left, top));
                root_width - width
            } else {
                w.set_top_line(Some(root_top_line));
                set_window_geometry(w, (width, height, left, root_top));
                root_height - height
            };
            let mut resized = false;
            for ignore in [Value::Nil, Value::T] {
                crate::lisp::primitives::hooks_overlays::call_named_function(
                    interp,
                    "window--resize-root-window",
                    &[
                        w.value(),
                        Value::Integer(delta),
                        if horflag { Value::T } else { Value::Nil },
                        ignore,
                        Value::T,
                    ],
                    env,
                )?;
                if window_resize_check(w, horflag) {
                    window_resize_apply(interp, w, horflag)?;
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
            w = swindow;
        }
    }

    // Unlink WINDOW from its siblings.
    let parent = w.parent_window();
    let next = w.next_sibling();
    if let Some(prev) = w.prev_sibling() {
        prev.set_next_sibling(next);
        if let Some(next) = next {
            next.set_prev_sibling(Some(prev));
        }
    } else if let (Some(parent), Some(next)) = (parent, next) {
        next.set_prev_sibling(None);
        let horizontal = window_combination(parent) == Some(true);
        set_window_combination(interp, parent, horizontal, Some(next));
    }
    if let Some(child) = root_window.first_child() {
        delete_all_child_windows(interp, child)?;
    }
    replace_window(interp, root_window, w, true)?;
    clear_deleted_window(interp, root_window);
    // New total sizes for all of FRAME's windows, assigned only once
    // WINDOW has replaced ROOT (Bug#51007).
    frame_size::window_pixel_to_total(interp, frame, false, env)?;
    frame_size::window_pixel_to_total(interp, frame, true, env)?;
    if let Some((buffer_id, startpos, top)) = start_and_top {
        let new_top =
            window_geometry(interp, w).3 - window_geometry(interp, frame_root_window(frame)).3;
        let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
        if new_top != top && startpos >= point_min && startpos <= point_max {
            let saved = interp.current_buffer_id();
            interp.set_current_buffer_id(buffer_id)?;
            let (start, _) = crate::lisp::primitives::window::move_screen_lines_in(
                interp,
                env,
                w,
                startpos,
                -(top as isize),
            );
            let at_line_beg = {
                let buffer = interp.buffer.borrow();
                start <= buffer.point_min() || buffer.char_at(start - 1) == Some('\n')
            };
            interp.set_current_buffer_id(saved)?;
            set_window_start_marker(interp, w, start);
            w.set_start_at_line_beg(at_line_beg);
            // So that redisplay runs the window-scroll-functions.
            w.set_optional_new_start(true);
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

fn window_body_pixel_size(interp: &Interpreter, window: WindowRef) -> (i64, i64) {
    let (_, height, _, _) = window_geometry(interp, window);
    let body_height =
        height - window_non_body_height(interp, window, window.live_buffer_id(interp));
    (window_text_width_columns(interp, window), body_height)
}

/// WINDOW's old_pixel_width, old_pixel_height, old_body_pixel_width and
/// old_body_pixel_height: the sizes window_change_record recorded, 0 for
/// a window made since.
fn window_old_sizes(window: WindowRef) -> (i64, i64, i64, i64) {
    (
        window.old_pixel_width(),
        window.old_pixel_height(),
        window.old_body_pixel_width(),
        window.old_body_pixel_height(),
    )
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
            .buffer_local_value(buffer, symbol)
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default(),
    };
    for function in functions {
        let live = match window_or_frame.kind() {
            Kind::Frame(frame) => interp.frame_is_live(frame),
            _ => WindowRef::from_value(&window_or_frame)
                .is_some_and(|window| is_live_ordinary_window(interp, window)),
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
fn window_change_record(interp: &mut Interpreter, record_frames: bool) {
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
            let windows = frame_window_tree_leaves(frame);
            for &window in &windows {
                let buffer = window
                    .live_buffer_id(interp)
                    .and_then(|buffer| interp.buffer_value(buffer))
                    .unwrap_or(Value::Nil);
                let (width, height, _, _) = window_geometry(interp, window);
                let (body_width, body_height) = window_body_pixel_size(interp, window);
                window.set_old_buffer(buffer);
                window.set_change_stamp(stamp);
                window.set_old_pixel_width(width);
                window.set_old_pixel_height(height);
                window.set_old_body_pixel_width(body_width);
                window.set_old_body_pixel_height(body_height);
            }
            let mut state = frame.borrow_mut();
            state.number_of_windows = windows.len();
            state.window_state_change = false;
        }
    }
    interp.old_selected_window = interp.selected_window();
    interp.old_selected_frame_id = interp.selected_frame_id;
}

/// FRAME_WINDOW_CHANGE: window.c raises it whenever a window of FRAME is
/// split, deleted, resized or shown another buffer, and xdisp.c when a
/// window's body size changed; here the same events are read off the
/// state window_change_record left.
fn frame_window_change(
    interp: &Interpreter,
    windows: &[WindowRef],
    number_of_windows: usize,
    stamp: i64,
) -> bool {
    windows.len() != number_of_windows
        || windows.iter().any(|&window| {
            let buffer = window
                .live_buffer_id(interp)
                .and_then(|buffer| interp.buffer_value(buffer))
                .unwrap_or(Value::Nil);
            let (width, height, _, _) = window_geometry(interp, window);
            let (body_width, body_height) = window_body_pixel_size(interp, window);
            window.change_stamp() != stamp
                || !buffer.eq_value(window.old_buffer())
                || (width, height, body_width, body_height) != window_old_sizes(window)
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
    window_change_record(interp, record_frames);
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
                state.old_selected_window,
                state.selected_window,
                state.window_state_change,
            )
        };
        let windows = frame_window_tree_leaves(frame);
        let window_change = frame_window_change(interp, &windows, number_of_windows, stamp);
        let frame_selected_change =
            selected_frame_change && (frame == old_selected_frame || frame == selected_frame);
        let frame_selected_window_change = old_selected != selected;
        if !(window_change || frame_selected_change || frame_selected_window_change || state_change)
        {
            continue;
        }
        let old_selected_window = interp.old_selected_window;
        let selected_window = interp.selected_window_ref();
        let mut frame_buffer_change = false;
        let mut frame_size_change = false;
        let mut counted = 0usize;
        for &window in &windows {
            counted += 1;
            if !is_live_ordinary_window(interp, window) {
                continue;
            }
            let window_value = window.value();
            let buffer_id = window_buffer_id(interp, &window_value);
            let buffer = buffer_id
                .and_then(|buffer| interp.buffer_value(buffer))
                .unwrap_or(Value::Nil);
            let buffer_change = window_change
                && (!buffer.eq_value(window.old_buffer()) || window.change_stamp() != stamp);
            let (width, height, _, _) = window_geometry(interp, window);
            let (body_width, body_height) = window_body_pixel_size(interp, window);
            let size_change = window_change
                && (buffer_change
                    || (width, height, body_width, body_height) != window_old_sizes(window));
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
                && (Some(window) == old_selected_window || window == selected_window))
                || (frame_selected_window_change
                    && (Some(window) == old_selected || Some(window) == selected));
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
            .symbol_value_key(cached_symbol!("window-state-change-hook"))
            .is_some_and(|value| value.is_truthy())
    {
        safe_run_hooks(
            interp,
            &cached_symbol!("window-state-change-hook").symbol(),
            env,
        )?;
    }
    Ok(())
}

/// The buffer of the window decode_live_window answers for WINDOW: None
/// for nil when the selected window is deleted, whose contents are nil.
fn window_buffer_id_or_selected(
    interp: &Interpreter,
    window: Option<&Value>,
) -> Result<Option<u64>, LispError> {
    match window.map(|v| v.kind()) {
        None | Some(Kind::Nil) => Ok(interp.selected_window_contents_buffer_id()),
        Some(value) => {
            let window = decode_any_window(interp, &value.value())?;
            window
                .live_buffer_id(interp)
                .map(Some)
                .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), value.value()))
        }
    }
}

fn window_line_height(interp: &Interpreter, buffer_id: u64, format_variable: &str) -> i64 {
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
            _ => interp.symbol_value(format_variable),
        }
    } else {
        interp
            .buffer_local_toplevel_value(buffer_id, format_variable)
            .or_else(|| interp.global_binding_value(format_variable))
    };
    i64::from(format.is_some_and(|format| !format.is_nil()))
}

/// The lines of WINDOW showing BUFFER_ID that are not its body:
/// window.c's window_wants_mode_line, window_wants_header_line and
/// window_wants_tab_line, each a line of a leaf (WINDOW_LEAF_P, contents a
/// buffer: none for a deleted window, BUFFER_ID None) that is no
/// minibuffer window, whose window parameter of the format's name is not
/// `none' and either it or the buffer's format is non-nil, and whose
/// height holds it beside the lines wanted before it (the mode line
/// prevails).
fn window_non_body_height(interp: &Interpreter, window: WindowRef, buffer_id: Option<u64>) -> i64 {
    let Some(buffer_id) = buffer_id else {
        return 0;
    };
    if window_combination(window).is_some() || window_is_minibuffer(window) {
        return 0;
    }
    let height = window_geometry(interp, window).1;
    let wants = |parameter: Value, variable: &str| {
        let format = window_parameter_value(window, &parameter);
        !format.is_key(cached_symbol!("none"))
            && (!format.is_nil() || window_line_height(interp, buffer_id, variable) != 0)
    };
    let mode = wants(
        Value::Symbol(cached_symbol!("mode-line-format").symbol()),
        "mode-line-format",
    ) && height > 1;
    let header = wants(
        Value::Symbol(cached_symbol!("header-line-format").symbol()),
        "header-line-format",
    ) && height > if mode { 2 } else { 1 };
    let tab = wants(
        Value::Symbol(cached_symbol!("tab-line-format").symbol()),
        "tab-line-format",
    ) && height > i64::from(mode) + i64::from(header) + 1;
    i64::from(mode) + i64::from(header) + i64::from(tab)
}

/// Screen rows owned by the window selected *now*, its body height in the
/// live window tree, batch or not.  During a minibuffer read, packages can
/// temporarily select the shrunken ordinary window and call `recenter'; the
/// last published glyph metrics then belong to an earlier redisplay and can
/// still describe the pre-minibuffer height.
fn selected_command_text_height(interp: &Interpreter) -> usize {
    let window = interp.selected_window_ref();
    let buffer_id = interp.selected_window_contents_buffer_id();
    (window_geometry(interp, window).1 - window_non_body_height(interp, window, buffer_id)).max(1)
        as usize
}

/// WINDOW_TOTAL_COLS of WINDOW.
pub(crate) fn window_total_columns(interp: &Interpreter, window: WindowRef) -> i64 {
    window_geometry(interp, window).0
}

/// window_box_width of WINDOW's text area in terminal cells.
pub(crate) fn window_text_area_columns(interp: &Interpreter, window: WindowRef) -> usize {
    window_text_width_columns(interp, window).max(0) as usize
}

fn window_text_width_columns(interp: &Interpreter, window: WindowRef) -> i64 {
    let (total, _, left, _) = window_geometry(interp, window);
    let root = WindowRef::from_value(&interp.root_window_value()).unwrap_or(window);
    let (root_width, _, root_left, _) = window_geometry(interp, root);
    // GNU reserves one terminal column for the vertical separator of every
    // live window whose right edge is not the root window's right edge.  The
    // separator is part of WINDOW-TOTAL-WIDTH but not WINDOW-BODY-WIDTH (see
    // window_body_width in window.c).  Emaxx currently exposes terminal
    // frames, so there are no GUI fringes or scroll-bar pixels to subtract.
    let vertical_separator =
        i64::from(left.saturating_add(total) < root_left.saturating_add(root_width));
    let (left, right) = crate::lisp::primitives::window_margin_cols(window);
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

pub(crate) fn window_hscroll_state(window: WindowRef) -> WindowHscrollState {
    WindowHscrollState {
        hscroll: window.hscroll().max(0),
        min_hscroll: window.min_hscroll().max(0),
        suspended: window.suspend_auto_hscroll(),
        old_point: window_marker_position(window, WindowMarker::OldPointm)
            .map(|point| point as i64),
    }
}

/// hscroll_window_tree's write-back: the recomputed hscroll, the
/// (possibly lifted) suspension, and the remembered window point.
pub(crate) fn store_window_hscroll_state(
    interp: &mut Interpreter,
    window: WindowRef,
    hscroll: i64,
    suspended: bool,
    old_point: i64,
) {
    window.set_hscroll(hscroll.max(0));
    window.set_suspend_auto_hscroll(suspended);
    if let Some(buffer_id) = window.live_buffer_id(interp) {
        set_window_marker(
            interp,
            window,
            WindowMarker::OldPointm,
            buffer_id,
            old_point.max(1) as usize,
        );
    }
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
    let buffer_local = |name: crate::lisp::types::symbols::SymbolKey| {
        interp
            .buffer_local_value_key(buffer_id, name)
            .or_else(|| interp.default_value_key(name))
            .unwrap_or(Value::Nil)
    };
    let mode = match buffer_local(cached_symbol!("display-line-numbers")).kind() {
        Kind::Nil => return None,
        Kind::Symbol(ref name) if name.is_named("relative") => LineNumberMode::Relative,
        Kind::Symbol(ref name) if name.is_named("visual") => LineNumberMode::Visual,
        _ => LineNumberMode::Absolute,
    };
    let current_absolute =
        buffer_local(cached_symbol!("display-line-numbers-current-absolute")).is_truthy();
    let offset = match mode {
        LineNumberMode::Absolute => buffer_local(cached_symbol!("display-line-numbers-offset"))
            .as_integer()
            .unwrap_or(0),
        _ => 0,
    };
    // A non-zero offset forces counting from the buffer's beginning, as
    // if display-line-numbers-widen were non-nil.
    let widen =
        buffer_local(cached_symbol!("display-line-numbers-widen")).is_truthy() || offset != 0;
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
    let explicit = buffer_local(cached_symbol!("display-line-numbers-width"))
        .as_integer()
        .ok()
        .filter(|width| *width >= 0)
        .unwrap_or(0) as usize;
    let width = needed.max(explicit);
    let tick = |name: crate::lisp::types::symbols::SymbolKey| {
        interp
            .symbol_value_key(name)
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
        major_tick: tick(cached_symbol!("display-line-numbers-major-tick")),
        minor_tick: tick(cached_symbol!("display-line-numbers-minor-tick")),
    })
}

/// window.c:set_window_hscroll.
fn set_window_hscroll_value(interp: &mut Interpreter, window: WindowRef, requested: i64) -> Value {
    // clip_to_bounds (0, hscroll, min (MOST_POSITIVE_FIXNUM, PTRDIFF_MAX)).
    let hscroll = requested.clamp(0, (1_i64 << 61) - 1);
    if window.hscroll() != hscroll {
        // set_window_hscroll: a changed hscroll redisplays the window,
        // its buffer without the shortcuts.
        if let Some(buffer_id) = window.live_buffer_id(interp) {
            interp.prevent_redisplay_optimizations(buffer_id);
        }
        interp.wset_redisplay(window);
    }
    window.set_hscroll(hscroll);
    window.set_suspend_auto_hscroll(true);
    Value::Integer(hscroll)
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
        window_list_windows(interp, minibuf, start, frames)
            .into_iter()
            .map(|id| id.value()),
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
) -> smallvec::SmallVec<[crate::lisp::types::FrameRef; 4]> {
    interp
        .frame_states
        .iter()
        .filter(|frame| frame.is_live())
        .filter(
            |frame| match crate::lisp::types::eq_dispatch(frame_filter).kind() {
                Kind::T => true,
                Kind::Frame(id) => **frame == id,
                Kind::Integer(0) => {
                    frame.borrow().terminal.expect("live frame terminal").id
                        == interp.selected_terminal_id()
                }
                Kind::Symbol(name) if name.is_named("visible") => {
                    frame.borrow().terminal.expect("live frame terminal").id
                        == interp.selected_terminal_id()
                }
                _ => **frame == own_frame,
            },
        )
        .copied()
        .collect()
}

/// window.c:Fget_buffer_window's window_loop (GET_BUFFER_WINDOW): among
/// ALL-FRAMES's windows in their cyclic order from the selected window of
/// the frame ALL-FRAMES names (else of the selected frame), the selected
/// window if it shows BUFFER_ID, else the first that does on the selected
/// frame, else the first that does at all.  A minibuffer window counts
/// only while a minibuffer is active and only as minibuf_window.  The
/// walk's first window is window_list_1's decode_live_window: a deleted
/// one signals.
pub(crate) fn buffer_window(
    interp: &Interpreter,
    buffer_id: u64,
    all_frames: &Value,
) -> Result<Option<Value>, LispError> {
    let selected_frame = interp.selected_frame_id;
    let frame = match all_frames.kind() {
        Kind::Frame(frame) if frame.is_live() => frame,
        _ => selected_frame,
    };
    decode_live_window(interp, Some(&frame_selected_window(frame).value()))?;
    let selected = interp.selected_window_ref();
    let minibuffer_active = interp.active_minibuffer_buffer_id().is_some();
    // "Don't find any minibuffer window except the one that is currently
    // in use": minibuf_window.
    let minibuffer = interp.minibuf_window();
    let shows = |window: WindowRef| {
        (is_live_ordinary_window(interp, window)
            || (minibuffer_active && Some(window) == minibuffer))
            && window.live_buffer_id(interp) == Some(buffer_id)
    };
    // The selected window is returned wherever the walk meets it, and it
    // is among the windows unless ALL-FRAMES names another frame.
    if frame == selected_frame && shows(selected) {
        return Ok(Some(selected.value()));
    }
    let start = frame_selected_window(frame).value();
    let frames = window_list_frames(interp, frame, all_frames);
    let mut best = None;
    let mut on_selected_frame = false;
    for id in window_list_windows(interp, Some(&Value::T), Some(&start), &frames) {
        if !shows(id) {
            continue;
        }
        if id == selected {
            return Ok(Some(id.value()));
        }
        if !on_selected_frame && id.frame() == Some(selected_frame) {
            best = Some(id);
            on_selected_frame = true;
        } else if best.is_none() {
            best = Some(id);
        }
    }
    Ok(best.map(|id| id.value()))
}

/// The windows `window_list_value' lists.
fn window_list_windows(
    interp: &Interpreter,
    minibuf: Option<&Value>,
    start: Option<&Value>,
    frames: &[crate::lisp::types::FrameRef],
) -> WindowList {
    let mut windows = WindowList::new();
    for frame in frames {
        let mut leaves = frame_window_tree_leaves(*frame);
        leaves.retain(|window| is_live_ordinary_window(interp, *window));
        if windows.is_empty() {
            windows = leaves;
        } else {
            windows.extend(leaves);
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
        if include_minibuffer
            && let Some(minibuffer) = frame.borrow().minibuffer_window
            && !windows.contains(&minibuffer)
        {
            windows.push(minibuffer);
        }
    }
    // GNU lists windows in cyclic order starting from WINDOW (default
    // the selected window), not from the tree's first leaf.
    let start_window = start
        .filter(|window| !window.is_nil())
        .and_then(WindowRef::from_value)
        .unwrap_or_else(|| interp.selected_window_ref());
    if let Some(index) = windows.iter().position(|window| *window == start_window) {
        windows.rotate_left(index);
    }
    windows
}

/// print.c's Vprin1_to_string_buffer, as buffer.c:init_buffer_once makes
/// it: `Fget_buffer_create (" prin1", Qt)' -- a space name, so no undo,
/// and its hooks inhibited -- before any buffer is current (so its
/// directory is nil) and just before `Vbuffer_alist = Qnil', so no buffer
/// list or name lookup ever finds it.  editfns.c:syms_of_editfns makes
/// buffer-access-fontify-functions local (nil) in it.  The image carries
/// it; a session without one makes it at its first print.  Killed, it
/// stays killed, as the C variable keeps the dead buffer: printing into
/// it is Fset_buffer's error.
pub(crate) fn prin1_to_string_buffer(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let id = match interp.prin1_to_string_buffer {
        Some(id) => id,
        None => {
            let (id, _) = interp.create_buffer(" prin1");
            interp.set_buffer_hooks_inhibited(id, true);
            interp.buffer_list.retain(|(listed, _)| *listed != id);
            interp.prin1_to_string_buffer = Some(id);
            let previous = interp.current_buffer_id();
            interp.set_current_buffer_id(id)?;
            let made = (|| -> Result<(), LispError> {
                interp.set_symbol_value_key(cached_symbol!("default-directory"), Value::Nil);
                let fontify = Value::symbol("buffer-access-fontify-functions");
                super::call(interp, "make-local-variable", &[fontify], env)?;
                interp.set_symbol_value_key(
                    cached_symbol!("buffer-access-fontify-functions"),
                    Value::Nil,
                );
                Ok(())
            })();
            interp.set_current_buffer_id(previous)?;
            made?;
            id
        }
    };
    interp
        .buffer_value(id)
        .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))
}

/// The hidden buffer's text as Fprin1_to_string and Ferror_message_string
/// take it once printing has inserted it: `Fbuffer_string' there (made
/// unibyte when its bytes are its characters, for Fprin1_to_string), then
/// `Ferase_buffer', then the previous buffer current again.
fn take_prin1_buffer_text(
    interp: &mut Interpreter,
    env: &mut Env,
    hidden: &Value,
    unibyte_when_ascii: bool,
) -> Result<Value, LispError> {
    let previous = interp.current_buffer_id();
    interp.set_current_buffer_id(interp.resolve_buffer_id(hidden)?)?;
    let taken = (|| -> Result<Value, LispError> {
        let mut text = super::call(interp, "buffer-string", &[], env)?;
        if unibyte_when_ascii && let Kind::StringObject(string) = text.kind() {
            let (characters, ascii) = {
                let contents = string.borrow();
                (contents.len(), contents.storage_bytes() == contents.len())
            };
            // STRING_SET_UNIBYTE (object), a macro: an empty result
            // becomes empty_unibyte_string itself.
            if characters == 0 {
                text = Value::StringObject(crate::lisp::types::StringObjectRef::from_unibyte(
                    Vec::new(),
                ));
            } else if ascii && string.borrow().is_multibyte() {
                string.borrow_mut().set_unibyte();
            }
        }
        super::call(interp, "erase-buffer", &[], env)?;
        Ok(text)
    })();
    if interp.has_buffer_id(previous) {
        interp.set_current_buffer_id(previous)?;
    }
    taken
}

/// print.c Fprin1_to_string: OBJECT as `prin1' prints it, or as `princ'
/// does with NOESCAPE (no quoting), into the hidden prin1 buffer with
/// modification hooks inhibited and OVERRIDES bound (so the printer reads
/// the variables there, after print_prepare selects it), and that
/// buffer's text taken and erased.  `deactivate-mark' is kept as it was.
pub(crate) fn prin1_to_string(
    interp: &mut Interpreter,
    env: &mut Env,
    object: &Value,
    noescape: bool,
    overrides: Option<&Value>,
) -> Result<Value, LispError> {
    let hidden = prin1_to_string_buffer(interp, env)?;
    let restore = cached_symbol!("inhibit-modification-hooks")
        .with(|symbol| interp.bind_special_symbol(symbol, Value::T, env))?;
    let result = with_print_overrides(interp, env, overrides, |interp, env| {
        let save_deactivate_mark = interp
            .symbol_value_key(cached_symbol!("deactivate-mark"))
            .unwrap_or(Value::Nil);
        let empty =
            matches!(hidden.kind(), Kind::Buffer(buffer) if buffer.borrow().size_total() == 0);
        let octets = crate::lisp::primitives::file_io::print_object_to_buffer(
            interp, object, hidden, env, !noescape,
        )?;
        let mut text = take_prin1_buffer_text(interp, env, &hidden, true)?;
        // print_finish inserted the octets a character each, so the
        // buffer's character and byte counts agree and Fbuffer_string's
        // string is made unibyte: alone in the buffer, they are the result.
        if empty && let Some(octets) = octets {
            text = Value::StringObject(crate::lisp::types::StringObjectRef::from_unibyte(octets));
        }
        interp.set_c_variable("deactivate-mark", save_deactivate_mark);
        Ok(text)
    });
    let restored = interp.restore_special_dynamic(restore, env);
    restored.and(result)
}

/// print.c:print_error_message: an error message for the error DATA
/// onto the Lisp output stream STREAM (suitable for the print functions),
/// each piece in a print_prepare and print_finish of its own.  CONTEXT,
/// a C string describing the context of the error, is written first
/// (write_string); CALLER, the Lisp function inside which the error was
/// signaled, is shown in *Messages* when it is a symbol.
pub(crate) fn print_error_message(
    interp: &mut Interpreter,
    env: &mut Env,
    data: Value,
    stream: Value,
    context: Option<&[u8]>,
    caller: Value,
) -> Result<(), LispError> {
    use crate::lisp::primitives::file_io::{write_c_string, write_printer_output};
    if let Some(context) = context {
        write_c_string(interp, context, Some(&stream), env)?;
    }
    // If we know from where the error was signaled, show it in
    // *Messages*.
    match caller.kind() {
        Kind::Symbol(caller) => {
            log_message_partial(interp, &caller.name_text(), env);
            log_message_partial(interp, ": ", env);
        }
        Kind::T => log_message_partial(interp, "t: ", env),
        _ => {}
    }
    let error = Value::symbol("error");
    let obj = data;
    // Fcar (data).
    let errname = match obj.cons_values() {
        Some((head, _)) => head,
        None if obj.is_nil() => Value::Nil,
        None => return Err(LispError::WrongTypeArgument("listp".into(), obj)),
    };
    let (mut errmsg, file_error, data) = if crate::lisp::types::eq(errname, error) {
        let data = obj.cons_values().map_or(Value::Nil, |(_, rest)| rest);
        let data = if data.cons_values().is_some() {
            data
        } else {
            Value::Nil
        };
        (
            data.cons_values().map_or(Value::Nil, |(car, _)| car),
            false,
            data,
        )
    } else {
        let conditions = super::call(
            interp,
            "get",
            &[errname, Value::symbol("error-conditions")],
            env,
        )?;
        let mut errmsg = super::call(
            interp,
            "get",
            &[errname, Value::symbol("error-message")],
            env,
        )?;
        // During loadup `substitute-command-keys' may be missing, and
        // its errors are ignored (safe_calln).
        let substitute = Value::symbol("substitute-command-keys");
        if super::call(interp, "fboundp", &[substitute], env)?.is_truthy()
            && let Ok(substituted) = interp.call_function_value(
                substitute,
                Some("substitute-command-keys"),
                &[errmsg],
                env,
            )
            && !substituted.is_nil()
        {
            errmsg = substituted;
        }
        let file_error = super::call(
            interp,
            "memq",
            &[Value::symbol("file-error"), conditions],
            env,
        )?
        .is_truthy();
        (errmsg, file_error, obj)
    };
    let mut tail = data.cons_values().map_or(Value::Nil, |(_, rest)| rest);
    // A file-error's message is its first datum.
    if file_error && let Some((first, rest)) = tail.cons_values() {
        errmsg = first;
        tail = rest;
    }
    // Each piece goes onto the stream through a print_prepare and
    // print_finish of its own (into the hidden prin1 buffer with
    // modification hooks live: text already inserted stays there when a
    // datum's printing or the tail's cycle escapes, for the next
    // prin1-to-string to take).
    let mut sep = Some(": ");
    match errmsg.kind() {
        Kind::StringObject(message) if message.borrow().len() == 0 => sep = None,
        Kind::StringObject(_) => {
            super::call(interp, "princ", &[errmsg, stream], env)?;
        }
        _ => write_printer_output(interp, "peculiar error", Some(&stream), env)?,
    }
    // princ for file-error, end-of-file and user-error data, prin1 for
    // every other condition's: Magit's multi-part git errors keep their
    // quotes and escapes.
    let printer = if file_error
        || crate::lisp::types::eq(errname, Value::symbol("end-of-file"))
        || crate::lisp::types::eq(errname, Value::symbol("user-error"))
    {
        "princ"
    } else {
        "prin1"
    };
    let mut cycle = crate::lisp::primitives::values::ForEachTail::new(tail);
    // FOR_EACH_TAIL: `XCAR (tail)' is read once the separator is
    // written and `XCDR (tail)' once the datum is printed, so what the
    // stream's function or `print-unreadable-function' stores in the
    // list is what comes next.
    while tail.cons_values().is_some() {
        if let Some(sep) = sep {
            write_printer_output(interp, sep, Some(&stream), env)?;
        }
        sep = Some(", ");
        let datum = tail.cons_values().map_or(Value::Nil, |(datum, _)| datum);
        super::call(interp, printer, &[datum, stream], env)?;
        tail = tail.cons_values().map_or(Value::Nil, |(_, next)| next);
        if cycle.step(tail) {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    Ok(())
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
                // editfns.c:Fmessage: nil or an empty format string is
                // message1 (0), clearing the echo area unlogged; anything
                // else is message3 of `format-message''s string.
                let first = args.first().copied().unwrap_or(Value::Nil);
                if first.is_nil() || with_string_text(&first, str::is_empty).unwrap_or(false) {
                    message3(interp, Value::Nil, None, env)?;
                    return Ok(first);
                }
                let formatted = super::call(interp, "format-message", args, env)?;
                let log = string_text(&formatted)?;
                message3(interp, formatted, Some(&log), env)?;
                Ok(formatted)
            }
            "message-box" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                // editfns.c:Fmessage_box: nil clears the echo area as
                // message1 (0) does; anything else is formatted and shown
                // in a dialog with an "OK" button, which a frame with no
                // menu hook (a batch session's) never draws.
                if args[0].is_nil() {
                    super::call(interp, "message", &[Value::Nil], env)?;
                    return Ok(Value::Nil);
                }
                let formatted = super::call(interp, "format-message", args, env)?;
                let pane = Value::list([Value::cons(Value::string("OK"), Value::T)]);
                super::call(
                    interp,
                    "x-popup-dialog",
                    &[Value::T, Value::cons(formatted, pane), Value::T],
                    env,
                )?;
                Ok(formatted)
            }
            "message-or-box" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                // editfns.c:Fmessage_or_box: a dialog after a mouse event
                // (or before any) while `use-dialog-box' is non-nil.
                let last_nonmenu_event = interp
                    .symbol_value_key(cached_symbol!("last-nonmenu-event"))
                    .unwrap_or(Value::Nil);
                let use_dialog_box = interp
                    .symbol_value_key(cached_symbol!("use-dialog-box"))
                    .is_some_and(|value| value.is_truthy());
                let after_mouse = last_nonmenu_event.is_nil()
                    || matches!(last_nonmenu_event.kind(), Kind::Cons(_));
                let function = if after_mouse && use_dialog_box {
                    "message-box"
                } else {
                    "message"
                };
                super::call(interp, function, args, env)
            }
            "current-message" => {
                need_args(name, args, 0)?;
                // GNU batch mode has no echo area; `current-message' is nil.
                if interp
                    .symbol_value_key(cached_symbol!("noninteractive"))
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
                let obj = args[0];
                let error = Value::symbol("error");
                // print.c:Ferror_message_string returns the STRING of (error
                // STRING) itself, without allocating or losing properties.
                if let Some((head, rest)) = obj.cons_values()
                    && crate::lisp::types::eq(head, error)
                    && let Some((message, end)) = rest.cons_values()
                    && message.is_string()
                    && end.is_nil()
                {
                    return Ok(message);
                }
                // print_error_message (obj, Vprin1_to_string_buffer, 0,
                // Qnil), then that buffer's text.
                let hidden = prin1_to_string_buffer(interp, env)?;
                print_error_message(interp, env, obj, hidden, None, Value::Nil)?;
                take_prin1_buffer_text(interp, env, &hidden, false)
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
                let minibuffer_quit =
                    conditions
                        .to_vec()
                        .unwrap_or_default()
                        .iter()
                        .any(|condition| {
                            crate::lisp::types::eq_symbol_named(condition, "minibuffer-quit")
                        });
                // CHECK_STRING (context), then its SSDATA.
                string_text(&args[1])?;
                let context = match args[1].kind() {
                    Kind::StringObject(state) => state.borrow().bytes().to_vec(),
                    _ => Vec::new(),
                };
                // keyboard.c command-error-default-function, noninteractive
                // branch: print CONTEXT plus the error to stderr and
                // kill-emacs -1.  The previous arm computed the message and
                // discarded it, silently converting a should-fail batch run
                // into a clean exit (2026-08-23 audit finding 80).
                if !minibuffer_quit
                    && interp
                        .symbol_value_key(cached_symbol!("noninteractive"))
                        .is_some_and(|value| value.is_truthy())
                {
                    // print_error_message and Fterpri with
                    // Qexternal_debugging_output as the stream: printchar
                    // hands it a character at a time, each its own write
                    // of the unbuffered stderr.
                    let stream = Value::symbol("external-debugging-output");
                    print_error_message(interp, env, args[0], stream, Some(&context), args[2])?;
                    super::call(interp, "terpri", &[stream], env)?;
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
                    // Fdiscard_input: the typeahead goes with the error.
                    super::call(interp, "discard-input", &[], env)?;
                    super::call(interp, "ding", &[], env)?;
                }
                // print_error_message (data, Qt, context, signal).
                print_error_message(interp, env, args[0], Value::T, Some(&context), args[2])?;
                Ok(Value::Nil)
            }
            "ding" => {
                need_arg_range(name, args, 0, 1)?;
                // dispnew.c:Fding: ARG non-nil rings the bell without
                // ending a keyboard macro (a BEL on stdout in batch).
                if args.first().is_some_and(Value::is_truthy) {
                    if interp
                        .forwarded_c_value("noninteractive")
                        .is_some_and(|value| value.is_truthy())
                    {
                        crate::lisp::primitives::batch_stdout::write(b"\x07");
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
                // dispnew.c:Fsleep_for: SECONDS by extract_float, the
                // deprecated MILLISECONDS a fixnum added in thousandths.
                let mut duration = crate::lisp::primitives::extract_float(interp, &args[0])?;
                if let Some(milliseconds) = args.get(1).filter(|value| !value.is_nil()) {
                    let Kind::Integer(milliseconds) = milliseconds.kind() else {
                        return Err(LispError::WrongTypeArgument(
                            "fixnump".into(),
                            *milliseconds,
                        ));
                    };
                    duration += milliseconds as f64 / 1000.0;
                }
                if duration > 0.0 {
                    // tend = now + dtotimespec (duration), None when past
                    // what the clock can hold.  wait_reading_process_output
                    // returns as soon as it detects output from any
                    // subprocess, so wait in a loop until the time expires.
                    let (mut secs, mut nsecs) =
                        crate::lisp::primitives::processes::dtotimespec(duration);
                    let tend = std::time::Instant::now()
                        .checked_add(std::time::Duration::new(secs as u64, nsecs as u32));
                    loop {
                        crate::lisp::primitives::processes::wait_reading_process_output(
                            interp,
                            env,
                            crate::lisp::primitives::processes::WaitRequest {
                                // min (t.tv_sec, WAIT_READING_MAX): time_t's
                                // maximum, which no tv_sec exceeds.
                                time_limit: secs,
                                nsecs,
                                read_kbd: 0,
                                do_display: false,
                                wait_proc: None,
                                just_wait_proc: 0,
                            },
                        )?;
                        if let Some(tend) = tend {
                            let left = tend.saturating_duration_since(std::time::Instant::now());
                            if left.is_zero() {
                                break;
                            }
                            secs = i64::try_from(left.as_secs())
                                .unwrap_or(crate::lisp::primitives::processes::WAIT_READING_MAX);
                            nsecs = i64::from(left.subsec_nanos());
                        }
                    }
                }
                Ok(Value::Nil)
            }
            "accept-process-output" => {
                need_arg_range(name, args, 0, 4)?;
                // process.c:Faccept_process_output.
                let process = args.first().copied().unwrap_or(Value::Nil);
                let mut seconds = args.get(1).copied().unwrap_or(Value::Nil);
                let millisec = args.get(2).copied().unwrap_or(Value::Nil);
                let mut just_this_one = args.get(3).copied().unwrap_or(Value::Nil);
                let wait_proc = if process.is_nil() {
                    just_this_one = Value::Nil;
                    None
                } else {
                    // CHECK_PROCESS, and a process dedicated to another
                    // thread cannot be waited for.
                    let process_id = interp.resolve_process_id(&process)?;
                    interp.ensure_process_owned_by_current_thread(process_id)?;
                    Some(process_id)
                };
                if !millisec.is_nil() {
                    // Obsolete calling convention using integers rather
                    // than floats.
                    let Kind::Integer(millis) = millisec.kind() else {
                        return Err(LispError::WrongTypeArgument("fixnump".into(), millisec));
                    };
                    seconds = if seconds.is_nil() {
                        Value::float(millis as f64 / 1000.0)
                    } else {
                        let Kind::Integer(whole) = seconds.kind() else {
                            return Err(LispError::WrongTypeArgument("fixnump".into(), seconds));
                        };
                        Value::float(millis as f64 / 1000.0 + whole as f64)
                    };
                }
                let mut secs: i64 = 0;
                let mut nsecs: i64 = -1;
                if !seconds.is_nil() {
                    match seconds.kind() {
                        Kind::Integer(whole) => {
                            if whole > 0 {
                                secs = whole;
                                nsecs = 0;
                            }
                        }
                        Kind::Float(float) => {
                            let float = float.get();
                            if float > 0.0 {
                                let (whole, fraction) =
                                    crate::lisp::primitives::processes::dtotimespec(float);
                                // min (t.tv_sec, WAIT_READING_MAX): time_t's
                                // maximum, which no tv_sec exceeds.
                                secs = whole;
                                nsecs = fraction;
                            }
                        }
                        _ => {
                            return Err(LispError::WrongTypeArgument("numberp".into(), seconds));
                        }
                    }
                } else if wait_proc.is_some() {
                    nsecs = 0;
                }
                let just_wait_proc = if just_this_one.is_nil() {
                    0
                } else if !matches!(just_this_one.kind(), Kind::Integer(_)) {
                    1
                } else {
                    -1
                };
                let got = crate::lisp::primitives::processes::wait_reading_process_output(
                    interp,
                    env,
                    crate::lisp::primitives::processes::WaitRequest {
                        time_limit: secs,
                        nsecs,
                        read_kbd: 0,
                        do_display: false,
                        wait_proc,
                        just_wait_proc,
                    },
                )?;
                Ok(if got <= 0 { Value::Nil } else { Value::T })
            }
            "input-pending-p" => {
                need_arg_range(name, args, 0, 1)?;
                // keyboard.c:Finput_pending_p: requeued_events_pending_p,
                // CONSP (Vunread_command_events) or either input-method
                // queue non-nil, answers t.
                if interp
                    .symbol_value_key(cached_symbol!("unread-command-events"))
                    .is_some_and(|events| events.is_cons())
                    || [
                        cached_symbol!("unread-post-input-method-events"),
                        cached_symbol!("unread-input-method-events"),
                    ]
                    .into_iter()
                    .any(|key| interp.symbol_value_key(key).is_some_and(|v| !v.is_nil()))
                {
                    return Ok(Value::T);
                }
                // process_special_events handles selection events only:
                // buffered file notifications wait for read_char and the
                // kernel notification queue is not read here at all (the
                // oracle answers nil and runs no callback for a watched
                // file written moments earlier).  A non-nil CHECK-TIMERS is
                // READABLE_EVENTS_DO_TIMERS_NOW; `sit-for' deliberately
                // calls `(input-pending-p t)', even for a zero-length
                // no-redisplay wait.
                // get_input_pending: `quit-flag' set, or an event in the
                // keyboard buffer.
                let check_timers = args.first().is_some_and(Value::is_truthy);
                Ok(
                    if crate::lisp::primitives::get_input_pending(interp, env, check_timers)?.0 {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "discard-input" => {
                need_args(name, args, 0)?;
                if interp
                    .symbol_value_key(cached_symbol!("defining-kbd-macro"))
                    .is_some_and(|value| value.is_truthy())
                {
                    // end_kbd_macro.
                    interp.update_mode_lines = 20;
                    {
                        let state = &mut **interp;
                        state
                            .kbd_macro_definition
                            .truncate(state.kbd_macro_committed_len);
                    }
                    let last_macro = Value::vector(interp.kbd_macro_definition.iter().cloned());
                    interp.set_symbol_value_key(cached_symbol!("last-kbd-macro"), last_macro);
                }
                interp.set_symbol_value_key(cached_symbol!("unread-command-events"), Value::Nil);
                interp.set_symbol_value_key(cached_symbol!("defining-kbd-macro"), Value::Nil);
                crate::lisp::primitives::discard_tty_input();
                Ok(Value::Nil)
            }
            "prin1" => {
                need_arg_range(name, args, 1, 3)?;
                with_print_overrides(interp, env, args.get(2), |interp, env| {
                    let stream = printer_stream_value(interp, args.get(1));
                    print_object_to_stream(interp, &args[0], stream, env, true, false)
                })
            }
            "princ" => {
                if args.is_empty() {
                    return Ok(Value::Nil);
                }
                let stream = printer_stream_value(interp, args.get(1));
                // Fprinc's fast path for plain strings: print_string
                // alone, between print_prepare and print_finish; the
                // depth and the numbering of a print under way are left
                // as they are.
                if let Kind::StringObject(string) = args[0].kind()
                    && string.borrow().props.intervals().is_none()
                    && interp
                        .symbol_value_key(cached_symbol!("print-continuous-numbering"))
                        .is_none_or(|value| value.is_nil())
                {
                    super::super::file_io::with_print_stream(
                        interp,
                        stream.as_ref(),
                        env,
                        |interp, env, sink| {
                            super::super::print::print_string(interp, env, string, sink)
                        },
                    )?;
                    return Ok(args[0]);
                }
                print_object_to_stream(interp, &args[0], stream, env, false, false)
            }
            "print" => {
                if args.is_empty() {
                    return Ok(Value::Nil);
                }
                let stream = printer_stream_value(interp, args.get(1));
                print_object_to_stream(interp, &args[0], stream, env, true, true)
            }
            "terpri" => {
                need_arg_range(name, args, 0, 2)?;
                let stream = printer_stream_value(interp, args.first());
                let ensure = args.get(1).is_some_and(Value::is_truthy);
                // print.c:Fterpri, between print_prepare and
                // print_finish.
                let printed = super::super::file_io::with_print_stream(
                    interp,
                    stream.as_ref(),
                    env,
                    |interp, env, sink| {
                        let newline = if !ensure {
                            true
                        } else {
                            // Difficult to check if at line beginning so
                            // abort.
                            if let PrintSink::Function(function) = sink
                                && function_value_p(interp, &function, env)
                            {
                                // signal_error: a function that is a
                                // proper list is the data's tail.
                                return Err(super::super::values::signal_error(
                                    "Unsupported function argument",
                                    function,
                                ));
                            }
                            let batch = matches!(sink, PrintSink::Stdout)
                                || (matches!(sink, PrintSink::Function(_))
                                    && interp
                                        .symbol_value_key(cached_symbol!("noninteractive"))
                                        .is_some_and(|value| value.is_truthy()));
                            if batch {
                                // `printchar_stdout_last == 10'.
                                interp.batch_standard_output_last_char != Some(u32::from(b'\n'))
                            } else if matches!(sink, PrintSink::Echo) {
                                // NILP (Fbolp ()) in the echo area buffer,
                                // which setup_echo_area_for_printing made
                                // current: at the end of what is printed
                                // there.
                                echo_area_message()
                                    .is_some_and(|text| !text.is_empty() && !text.ends_with('\n'))
                            } else {
                                // NILP (Fbolp ()).
                                let buffer = interp.buffer.borrow();
                                !super::super::file_io::buffer_position_at_line_start(
                                    &buffer,
                                    buffer.point(),
                                )
                            }
                        };
                        if newline {
                            super::super::print::printchar(interp, env, sink, u32::from(b'\n'))?;
                        }
                        Ok(newline)
                    },
                )?;
                Ok(if printed { Value::T } else { Value::Nil })
            }
            "prin1-to-string" => {
                need_arg_range(name, args, 1, 3)?;
                let noescape = args.get(1).is_some_and(|value| value.is_truthy());
                prin1_to_string(interp, env, &args[0], noescape, args.get(2))
            }
            "write-char" => {
                need_arg_range(name, args, 1, 2)?;
                // print.c Fwrite_char: CHECK_FIXNUM, then printchar of the
                // code as an unsigned int, between print_prepare and
                // print_finish.  A function stream is called with that
                // number as it is; other output stores its CHAR_STRING
                // form, whose modifier bits char_string resolves and
                // whose code past MAX_CHAR is an error.
                let stream = printer_stream_value(interp, args.get(1));
                let Kind::Integer(code) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                };
                let code = code as u32;
                super::super::file_io::with_print_stream(
                    interp,
                    stream.as_ref(),
                    env,
                    |interp, env, sink| match sink {
                        PrintSink::Function(function) => interp
                            .call_function_value(
                                function,
                                None,
                                &[Value::Integer(i64::from(code))],
                                env,
                            )
                            .map(drop),
                        _ => super::super::print::printchar_unsigned(interp, env, sink, code),
                    },
                )?;
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
                super::super::file_io::printchar_to_stream(interp, code as u32, false, env)?;
                Ok(args[0])
            }
            "print--preprocess" => {
                need_args(name, args, 1)?;
                print_preprocess(interp, &args[0], env)
            }
            "yes-or-no-p" => {
                need_args(name, args, 1)?;
                ensure_interaction_allowed(interp)?;
                // fns.c:3521 Fyes_or_no_p.  The old arm answered t whenever
                // no unread event supplied a `y' or `n' -- silence became
                // consent (finding 11).  GNU never invents an answer: it
                // honors `use-short-answers', appends `yes-or-no-prompt',
                // and loops on `read-from-minibuffer' until the user types
                // yes or no; in batch, EOF on stdin signals end-of-file.
                if interp
                    .symbol_value_key(cached_symbol!("use-short-answers"))
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
                    .symbol_value_key(cached_symbol!("yes-or-no-prompt"))
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
                // editfns.c:Fchar_equal: CHECK_CHARACTER of both; equal
                // codes are equal, else unequal without case-fold-search,
                // else equal when their downcase agrees, a unibyte buffer
                // reading 0x80..0xFF as raw bytes (UNIBYTE_TO_CHAR).
                let character = |value: &Value| match value.kind() {
                    Kind::Integer(code) if (0..=0x3F_FFFF).contains(&code) => Ok(code as u32),
                    _ => Err(LispError::WrongTypeArgument("characterp".into(), *value)),
                };
                let (mut a, mut b) = (character(&args[0])?, character(&args[1])?);
                if a == b {
                    return Ok(Value::T);
                }
                if !interp
                    .symbol_value_key(cached_symbol!("case-fold-search"))
                    .is_some_and(|value| value.is_truthy())
                {
                    return Ok(Value::Nil);
                }
                if !interp.current_buffer().multibyte() {
                    for code in [&mut a, &mut b] {
                        if (0x80..0x100).contains(code) {
                            *code += RAW_BYTE8_BASE;
                        }
                    }
                }
                // buffer.h:downcase, the current downcase table's entry.
                let table = interp.current_case_table_id();
                let downcase = |code: u32| {
                    interp
                        .char_table_get(table, code)
                        .and_then(|value| value.integer_value())
                        .and_then(|mapped| u32::try_from(mapped).ok())
                        .unwrap_or(code)
                };
                Ok(if downcase(a) == downcase(b) {
                    Value::T
                } else {
                    Value::Nil
                })
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
                text_char_description(interp, &args[0])
            }

            // ── More buffer ops ──
            "following-char" => direct_following_char(interp, args, env),
            "preceding-char" => direct_preceding_char(interp, args, env),
            "buffer-last-name" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c: BVAR (decode_buffer (BUFFER), last_name), the
                // name a buffer was made, renamed or killed with.
                Ok(interp.decode_buffer(args.first())?.borrow().last_name())
            }

            // ── Display stubs ──
            // frame.c:Fwindow_system: Fframep of FRAME (nil for the selected
            // frame), nil for a terminal frame's t.
            "window-system" => {
                need_arg_range(name, args, 0, 1)?;
                match args.first().map(|frame| frame.kind()) {
                    None | Some(Kind::Nil | Kind::Frame(_)) => Ok(Value::Nil),
                    Some(frame) => Err(wrong_type_argument("framep", frame.value())),
                }
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
                // term.c: a tty terminal whose name is dev_tty.
                let target = args.first().copied().unwrap_or(Value::Nil);
                let dev_tty = crate::lisp::eval::terminal::dev_tty();
                let controlling = interp.decode_terminal(&target).is_some_and(|terminal| {
                    let terminal = terminal.borrow();
                    terminal.kind.is_some() && terminal.name == dev_tty
                });
                Ok(if controlling { Value::T } else { Value::Nil })
            }
            // decode_tty_terminal: TERMINAL's TN_max_colors where it is a
            // tty, else none.
            "tty-display-color-p" => {
                need_arg_range(name, args, 0, 1)?;
                let terminal = require_live_terminal(interp, args.first())?;
                let state = terminal.borrow();
                Ok(if state.kind.is_some() && state.colors.max_colors > 0 {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "tty-display-color-cells" => {
                need_arg_range(name, args, 0, 1)?;
                let terminal = require_live_terminal(interp, args.first())?;
                let state = terminal.borrow();
                Ok(Value::Integer(if state.kind.is_some() {
                    state.colors.max_colors
                } else {
                    0
                }))
            }
            // term.c Ftty_no_underline: a tty terminal's
            // TS_enter_underline_mode is no more.
            "tty-no-underline" => {
                need_arg_range(name, args, 0, 1)?;
                let terminal = require_live_terminal(interp, args.first())?;
                let mut state = terminal.borrow_mut();
                if state.kind.is_some() {
                    state.appearance.enter_underline_mode = None;
                }
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
            "display-supports-face-attributes-p" => {
                need_arg_range(name, args, 1, 2)?;
                super::faces::display_supports_face_attributes_p(
                    interp,
                    env,
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                )
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
                    if position < interp.current_buffer().begv()
                        || position >= interp.current_buffer().zv()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("args-out-of-range".into()),
                            args[0],
                            Value::Integer(interp.current_buffer().begv() as i64),
                            Value::Integer(interp.current_buffer().zv() as i64),
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
                // keyboard.c: (INTERRUPT FLOW META QUIT); a frame not on a
                // terminal has no flow control and takes 8-bit input as Meta.
                let interrupt = if interp.input_interrupt_mode() {
                    Value::T
                } else {
                    Value::Nil
                };
                let (flow, meta) = match crate::lisp::primitives::tty_input_modes() {
                    Some(modes) => (
                        if modes.flow_control {
                            Value::T
                        } else {
                            Value::Nil
                        },
                        match modes.meta_key {
                            2 => Value::Integer(0),
                            1 => Value::T,
                            3 => Value::symbol("encoded"),
                            _ => Value::Nil,
                        },
                    ),
                    None => (Value::Nil, Value::T),
                };
                Ok(Value::list([
                    interrupt,
                    flow,
                    meta,
                    Value::Integer(crate::lisp::primitives::quit_char()),
                ]))
            }
            "set-input-interrupt-mode" => {
                need_args(name, args, 1)?;
                set_input_interrupt_mode(interp, args[0]);
                Ok(Value::Nil)
            }
            "set-output-flow-control" | "set-input-meta-mode" => {
                need_arg_range(name, args, 1, 2)?;
                let terminal = args.get(1).copied().unwrap_or(Value::Nil);
                set_tty_input_mode(interp, name, args[0], terminal)?;
                Ok(Value::Nil)
            }
            "set-input-mode" => {
                need_arg_range(name, args, 3, 4)?;
                // Fset_input_mode: each mode's own setter, QUIT only when
                // given.
                set_input_interrupt_mode(interp, args[0]);
                set_tty_input_mode(interp, "set-output-flow-control", args[1], Value::Nil)?;
                set_tty_input_mode(interp, "set-input-meta-mode", args[2], Value::Nil)?;
                if let Some(quit) = args.get(3).filter(|quit| !quit.is_nil()) {
                    set_quit_char(interp, *quit)?;
                }
                Ok(Value::Nil)
            }
            "set-quit-char" => {
                need_args(name, args, 1)?;
                set_quit_char(interp, args[0])?;
                Ok(Value::Nil)
            }

            "menu-or-popup-active-p" => {
                need_args(name, args, 0)?;
                // A headless Emaxx process cannot have an active native menu.
                Ok(Value::Nil)
            }
            "imagep" => {
                need_args(name, args, 1)?;
                Ok(if valid_image_p(interp, &args[0]) {
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
                // FILTER nil or a frame names the frame whose cache to
                // clear, which must be a window system's.
                let filter = args.first().cloned().unwrap_or(Value::Nil);
                if filter.is_nil() || matches!(filter.kind(), Kind::Frame(_)) {
                    return Err(super::frames::window_system_frame_error(
                        interp,
                        Some(&filter),
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
                if !valid_image_p(interp, &args[0]) {
                    return Err(LispError::Signal("Invalid image specification".into()));
                }
                // FRAME t flushes the window system frames, none here.
                if matches!(args.get(1).map(|v| v.kind()), Some(Kind::T)) {
                    return Ok(Value::Nil);
                }
                Err(super::frames::window_system_frame_error(
                    interp,
                    args.get(1),
                ))
            }
            // image.c: decode_live_frame; no transforms off a window system.
            "image-transforms-p" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Nil)
            }
            // image.c: a valid spec is looked up on the window system
            // frame FRAME names; any other is invalid but for
            // `image-metadata', which has none to give.
            "image-size" | "image-mask-p" | "image-metadata" => {
                if name == "image-size" {
                    need_arg_range(name, args, 1, 3)?;
                } else {
                    need_arg_range(name, args, 1, 2)?;
                }
                if valid_image_p(interp, &args[0]) {
                    let frame = if name == "image-size" {
                        args.get(2)
                    } else {
                        args.get(1)
                    };
                    Err(super::frames::window_system_frame_error(interp, frame))
                } else if name == "image-metadata" {
                    Ok(Value::Nil)
                } else {
                    Err(LispError::Signal("Invalid image specification".into()))
                }
            }

            "init-image-library" => {
                need_args(name, args, 1)?;
                // lookup_image_type compares TYPE with each type's symbol.
                let Ok(image_type) = args[0].as_symbol() else {
                    return Ok(Value::Nil);
                };
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
                // window.c:Fwindow_start: Fmarker_position of w->start,
                // nil while it points nowhere.
                let window = decode_live_window(interp, args.first())?;
                Ok(match window_marker_position(window, WindowMarker::Start) {
                    Some(start) => Value::Integer(start as i64),
                    None if window_marker(window, WindowMarker::Start).is_some() => Value::Nil,
                    None => Value::Integer(window_start(interp, args.first())? as i64),
                })
            }
            "window-end" => {
                need_arg_range(name, args, 0, 2)?;
                let window = decode_live_window(interp, args.first())?;
                // buf = w->contents; CHECK_BUFFER (buf).
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?
                    .ok_or_else(|| wrong_type_argument("bufferp", Value::Nil))?;
                let z = buffer_z(interp, buffer_id);
                // window.c Fwindow_end: UPDATE on a real frame, with the
                // window's recorded end not up to date, displays the
                // window's body from its start; otherwise the answer is Z
                // less the end the last redisplay recorded (none before
                // one, so a batch session answers Z).
                if args.get(1).is_some_and(Value::is_truthy)
                    && crate::tty::window_end_outdated(interp, window, buffer_id)
                    && interactive_window_metrics().is_some()
                    && !interp
                        .symbol_value_key(cached_symbol!("noninteractive"))
                        .is_some_and(|value| value.is_truthy())
                {
                    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
                    let start =
                        window_start_marker(interp, window, buffer_id).clamp(point_min, point_max);
                    let height = (window_geometry(interp, window).1
                        - window_non_body_height(interp, window, Some(buffer_id)))
                    .max(1) as usize;
                    let saved = interp.current_buffer_id();
                    interp.set_current_buffer_id(buffer_id)?;
                    let end = window_end_walk(interp, env, window, start, height);
                    interp.set_current_buffer_id(saved)?;
                    return Ok(Value::Integer(end as i64));
                }
                let end_pos = window.window_end_pos().unwrap_or(0);
                Ok(Value::Integer(z as i64 - end_pos))
            }
            "window-point" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                let buffer_id = window
                    .live_buffer_id(interp)
                    .ok_or_else(|| LispError::TypeError("window".into(), "deleted".into()))?;
                let point = if window == interp.selected_window_ref() {
                    if buffer_id == interp.current_buffer_id() {
                        interp.current_buffer().pt()
                    } else {
                        interp
                            .get_buffer_by_id(buffer_id)
                            .map(|buffer| buffer.point())
                            .unwrap_or(1)
                    }
                } else {
                    // window.c:window_point: w->pointm's charpos, kept by a
                    // marker set to point nowhere.
                    match window_marker(window, WindowMarker::Pointm) {
                        Some(marker) => marker.last_position(),
                        None => return Ok(Value::Nil),
                    }
                };
                Ok(Value::Integer(point as i64))
            }
            "window-hscroll" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(Value::Integer(window.hscroll()))
            }
            "window-vscroll" => {
                need_arg_range(name, args, 0, 2)?;
                decode_live_window(interp, args.first())?;
                // A terminal frame has no smooth pixel scrolling; GNU returns
                // zero in both canonical-line and pixel modes.
                Ok(Value::Integer(0))
            }
            "set-window-hscroll" => {
                need_args(name, args, 2)?;
                // CHECK_FIXNUM (ncol) before decode_live_window.
                let Kind::Integer(requested) = args[1].kind() else {
                    return Err(wrong_type_argument("fixnump", args[1]));
                };
                let window = decode_live_window(interp, args.first())?;
                Ok(set_window_hscroll_value(interp, window, requested))
            }
            "scroll-left" | "scroll-right" => {
                need_arg_range(name, args, 0, 2)?;
                let window = decode_live_window(interp, None)?;
                let requested = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => window_text_width_columns(interp, window) - 2,
                    Some(value) => prefix_numeric_value(&value.value())?.as_integer()?,
                };
                let current = window.hscroll();
                let requested = if name == "scroll-left" {
                    current.saturating_add(requested)
                } else {
                    current.saturating_sub(requested)
                };
                let result = set_window_hscroll_value(interp, window, requested);
                if args.get(1).is_some_and(Value::is_truthy) {
                    // w->min_hscroll = w->hscroll.
                    window.set_min_hscroll(window.hscroll());
                }
                Ok(result)
            }
            "other-window-for-scrolling" => {
                need_args(name, args, 0)?;
                let selected = decode_live_window(interp, None)?;
                let mut candidate = if window_is_minibuffer(selected) {
                    interp
                        .symbol_value_key(cached_symbol!("minibuffer-scroll-window"))
                        .filter(Value::is_truthy)
                } else {
                    None
                };

                if candidate.is_none()
                    && let Some(buffer @ Kind::Buffer(_)) = interp
                        .symbol_value_key(cached_symbol!("other-window-scroll-buffer"))
                        .map(|v| v.kind())
                    && let Kind::Buffer(buffer_value) = buffer
                    && interp.has_buffer_id(buffer_value.id)
                {
                    candidate = live_ordinary_windows(interp)
                        .into_iter()
                        .find(|window| window.live_buffer_id(interp) == Some(buffer_value.id))
                        .map(WindowRef::value);
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
                        .symbol_value_key(cached_symbol!("other-window-scroll-default"))
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
                    let windows = live_ordinary_windows(interp);
                    let index = windows
                        .iter()
                        .position(|window| *window == selected)
                        .unwrap_or(0);
                    (index + 1)
                        .checked_rem(windows.len())
                        .and_then(|next| windows.get(next))
                        .copied()
                        .map(|id| id.value())
                        .unwrap_or_else(|| interp.selected_window_value())
                });
                let candidate_window = decode_live_window(interp, Some(&candidate))?;
                if candidate_window == selected {
                    return Err(LispError::Signal("There is no other window".into()));
                }
                Ok(candidate_window.value())
            }
            "set-window-margins" => {
                need_arg_range(name, args, 2, 3)?;
                let window = decode_live_window(interp, args.first())?;
                let changed = set_window_margins(
                    interp,
                    window,
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )?;
                if changed {
                    apply_window_adjustment(interp, window);
                }
                Ok(if changed { Value::T } else { Value::Nil })
            }
            "window-margins" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                let (left, right) = crate::lisp::primitives::window_margin_cols(window);
                let to_value =
                    |margin: Option<i64>| margin.map(Value::Integer).unwrap_or(Value::Nil);
                Ok(Value::cons(to_value(left), to_value(right)))
            }
            "coordinates-in-window-p" => {
                need_args(name, args, 2)?;
                let window = decode_live_window(interp, Some(&args[1]))?;
                // CHECK_CONS (coordinates), CHECK_NUMBER of each.
                let (lx, ly) = args[0]
                    .cons_values()
                    .ok_or_else(|| wrong_type_argument("consp", args[0]))?;
                let coordinate = |value: Value| match value.kind() {
                    Kind::Integer(_) | Kind::Float(_) | Kind::BigInteger(_) => {
                        value.as_float().map(|value| value.trunc() as i64)
                    }
                    _ => Err(wrong_type_argument("numberp", value)),
                };
                let x = coordinate(lx)?;
                let y = coordinate(ly)?;
                let (width, height, left, top) = window_geometry(interp, window);
                if x < left || x >= left + width || y < top || y >= top + height {
                    return Ok(Value::Nil);
                }
                let local_x = x - left;
                let local_y = y - top;
                let buffer_id = window
                    .live_buffer_id(interp)
                    .ok_or_else(|| wrong_type_argument("window-live-p", args[1]))?;
                let tab_line = window_line_height(interp, buffer_id, "tab-line-format");
                let header_line = window_line_height(interp, buffer_id, "header-line-format");
                let mode_line = window_line_height(interp, buffer_id, "mode-line-format");
                if tab_line > 0 && local_y < tab_line {
                    return Ok(Value::symbol("tab-line"));
                }
                if header_line > 0 && local_y < tab_line + header_line {
                    return Ok(Value::symbol("header-line"));
                }
                if mode_line > 0 && local_y >= height - mode_line {
                    return Ok(Value::symbol("mode-line"));
                }
                let (left_margin, right_margin) =
                    crate::lisp::primitives::window_margin_cols(window);
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
                let window = args.get(1).filter(|value| !value.is_nil());
                let w = decode_live_window(interp, window)?;
                // xdisp.c: decode_live_window, buf = XBUFFER (w->contents),
                // SET_TEXT_POS_FROM_MARKER (top, w->start), whose
                // marker_position signals for the unchained start of a
                // deleted selected window (its contents nil), then
                // fix_position of a POS that is neither t nor nil, before
                // anything is displayed.
                let Some(buffer_id) = w.contents_buffer_id().filter(|_| {
                    window_marker(w, WindowMarker::Start)
                        .is_none_or(|start| start.position().is_some())
                }) else {
                    return Err(LispError::Signal("Marker does not point anywhere".into()));
                };
                if let Some(pos) = args
                    .first()
                    .filter(|pos| !matches!(pos.kind(), Kind::Nil | Kind::T))
                {
                    position_from_value(interp, pos)?;
                }
                // pos_visible_p: nothing shows on the initial frame, the
                // batch session's (FRAME_INITIAL_P).
                if w.frame().is_none_or(|frame| {
                    frame
                        .borrow()
                        .terminal
                        .as_ref()
                        .is_none_or(|terminal| terminal.borrow().kind.is_none())
                }) {
                    return Ok(Value::Nil);
                }
                let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
                let pos = match args.first().map(|v| v.kind()) {
                    // PT for the selected window, else w->pointm.
                    None | Some(Kind::Nil) if w == interp.selected_window_ref() => {
                        interp.current_buffer().pt()
                    }
                    None | Some(Kind::Nil) => window_marker_position(w, WindowMarker::Pointm)
                        .ok_or_else(|| {
                            LispError::Signal("Marker does not point anywhere".into())
                        })?,
                    Some(Kind::T) => point_max,
                    Some(value) => position_from_value(interp, &value.value())?,
                };
                let start = window_start(interp, window)?;
                // pos_visible_p's start_display, for t or a POS from the
                // window start to the end of the accessible text, the start
                // itself within it: init_iterator frees the frame's realized
                // faces after a face change.
                let pos_is_t = matches!(args.first().map(|v| v.kind()), Some(Kind::T));
                if (pos_is_t || (start..=point_max).contains(&pos))
                    && (point_min..=point_max).contains(&start)
                {
                    interp.init_iterator_faces_window(w);
                }
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
                let limit = (window_geometry(interp, w).1
                    - window_non_body_height(interp, w, Some(buffer_id)))
                .max(1) as usize;
                // pos_visible_p: start_display at the window's start and
                // move_it_to POS, in the window's buffer, as the window
                // lays its text out -- only for a start inside the
                // accessible text and t or a POS from the start to ZV.
                let xy = if !(point_min..=point_max).contains(&start)
                    || (!pos_is_t && !(first_visible..=point_max).contains(&pos))
                {
                    None
                } else {
                    let saved_buffer = interp.current_buffer_id();
                    let switched = saved_buffer != buffer_id
                        && interp.set_current_buffer_id(buffer_id).is_ok();
                    let xy = pos_visible_xy(
                        interp,
                        env,
                        w,
                        first_visible,
                        (!pos_is_t).then_some(pos),
                        limit,
                    );
                    if switched {
                        let _ = interp.set_current_buffer_id(saved_buffer);
                    }
                    xy
                };
                let Some(crate::tty::VisibleAt { x, y, rbot }) = xy else {
                    return Ok(Value::Nil);
                };
                // fully_p: no part of the row above or below the window
                // (a terminal's rows are never cut at the top); a row
                // partly visible counts only with PARTIALLY.
                let partially = args.get(2).is_some_and(Value::is_truthy);
                if !partially && rbot > 0 {
                    return Ok(Value::Nil);
                }
                // With PARTIALLY non-nil GNU answers a list (X Y [RTOP RBOT
                // ROWH VPOS]) — two elements for a fully visible position.
                // Text-terminal frames measure pixels in character cells, so
                // X is the column in the text area (the line-number glyphs
                // counted, the hscroll taken off) and Y the window row.
                if partially {
                    // The iterator's y starts below the tab and header
                    // lines (start_display's current_y).
                    let top = window_line_height(interp, buffer_id, "tab-line-format")
                        + window_line_height(interp, buffer_id, "header-line-format");
                    let mut answer = vec![Value::Integer(x), Value::Integer(y as i64 + top)];
                    if rbot > 0 {
                        // RTOP RBOT ROWH VPOS: the row lies wholly below
                        // the window, none of it shown.
                        answer.extend([
                            Value::Integer(0),
                            Value::Integer(rbot as i64),
                            Value::Integer(0),
                            Value::Integer(y as i64),
                        ]);
                    }
                    return Ok(Value::list(answer));
                }
                Ok(Value::T)
            }
            "window-total-width" | "window-total-height" => {
                // window.c:Fwindow_total_width, Fwindow_total_height: the
                // total size, or with ROUND `floor' or `ceiling' the pixel
                // size in units (a text frame's unit is one pixel).
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let w = decode_valid_window(interp, &window)?;
                let horizontal = name == "window-total-width";
                let pixel_rounded = args.get(1).is_some_and(|round| {
                    matches!(round.kind(), Kind::Symbol(ref round) if round.is_named("floor") || round.is_named("ceiling"))
                });
                Ok(Value::Integer(if pixel_rounded {
                    window_pixel_size(interp, w, horizontal)
                } else {
                    let (cols, lines, _, _) = window_char_geometry(w);
                    if horizontal { cols } else { lines }
                }))
            }
            "window-use-time" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(Value::Integer(window.use_time()))
            }
            "window-bump-use-time" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(interp
                    .bump_window_use_time(window)
                    .map(Value::Integer)
                    .unwrap_or(Value::Nil))
            }
            "window-body-width" => {
                need_arg_range(name, args, 0, 2)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(Value::Integer(window_text_width_columns(interp, window)))
            }
            "window-body-height" => {
                need_arg_range(name, args, 0, 2)?;
                let window = decode_live_window(interp, args.first())?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(
                    window_geometry(interp, window).1
                        - window_non_body_height(interp, window, buffer_id),
                ))
            }
            "window-text-width" => {
                need_arg_range(name, args, 0, 2)?;
                let window = decode_live_window(interp, args.first())?;
                // window_box_width (w, TEXT_AREA): the margins and fringes
                // come off, a text frame's vertical border does not (the
                // display iterator takes that column off its own width).
                let (total, _, _, _) = window_geometry(interp, window);
                let (left, right) = crate::lisp::primitives::window_margin_cols(window);
                Ok(Value::Integer(
                    (total - left.unwrap_or(0) - right.unwrap_or(0)).max(0),
                ))
            }
            "window-text-height" => {
                need_arg_range(name, args, 0, 2)?;
                let window = decode_live_window(interp, args.first())?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(
                    window_geometry(interp, window).1
                        - window_non_body_height(interp, window, buffer_id),
                ))
            }
            "window-line-height" => {
                need_arg_range(name, args, 0, 2)?;
                decode_live_window(interp, args.get(1))?;
                // GNU has no current glyph matrix in batch mode.
                Ok(Value::Nil)
            }
            "window-lines-pixel-dimensions" => {
                need_arg_range(name, args, 0, 6)?;
                decode_live_window(interp, args.first())?;
                // GNU has no current glyph matrix in batch mode.
                Ok(Value::Nil)
            }
            "window-pixel-width" | "window-pixel-height" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let w = decode_valid_window(interp, &window)?;
                let geometry = window_geometry(interp, w);
                Ok(Value::Integer(if name == "window-pixel-width" {
                    geometry.0
                } else {
                    geometry.1
                }))
            }
            "window-pixel-left" | "window-pixel-top" | "window-left-column" | "window-top-line" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let w = decode_valid_window(interp, &window)?;
                let (_, _, left, top) = if matches!(name, "window-pixel-left" | "window-pixel-top")
                {
                    window_geometry(interp, w)
                } else {
                    window_char_geometry(w)
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
                let w = decode_valid_window(interp, &window)?;
                let (width, height, _, _) = window_old_sizes(w);
                Ok(Value::Integer(if name == "window-old-pixel-width" {
                    width
                } else {
                    height
                }))
            }
            "window-old-body-pixel-width" | "window-old-body-pixel-height" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                let (_, _, width, height) = window_old_sizes(window);
                Ok(Value::Integer(if name == "window-old-body-pixel-width" {
                    width
                } else {
                    height
                }))
            }
            "window-new-pixel" | "window-new-total" | "window-new-normal" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let w = decode_valid_window(interp, &window)?;
                Ok(match name {
                    "window-new-pixel" => w.new_pixel(),
                    "window-new-total" => w.new_total(),
                    _ => w.new_normal(),
                })
            }
            "window-normal-size" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let w = decode_valid_window(interp, &window)?;
                Ok(w.normal_size(args.get(1).is_some_and(Value::is_truthy)))
            }
            "set-window-new-pixel" | "set-window-new-total" => {
                need_arg_range(name, args, 2, 3)?;
                let window = decode_valid_window(interp, &args[0])?;
                let pixel = name == "set-window-new-pixel";
                let current = if pixel {
                    window.new_pixel()
                } else {
                    window.new_total()
                }
                .as_integer()
                .unwrap_or(0);
                let add = args.get(2).is_some_and(Value::is_truthy);
                let requested = if name == "set-window-new-pixel" {
                    // check_integer_range (size, size_min, size_max): from
                    // zero, or from minus the current size when ADD.
                    let size_min = if add { -current } else { 0 };
                    let size_max = size_min + i64::from(i32::MAX);
                    crate::lisp::primitives::values::check_integer_range(
                        args[1], size_min, size_max,
                    )?
                } else {
                    // CHECK_FIXNUM (size).
                    match args[1].kind() {
                        Kind::Integer(size) => size,
                        _ => return Err(wrong_type_argument("fixnump", args[1])),
                    }
                };
                let requested = if add {
                    current.saturating_add(requested)
                } else {
                    requested
                };
                let size = Value::Integer(requested);
                if pixel {
                    window.set_new_pixel(size);
                } else {
                    window.set_new_total(size);
                }
                Ok(size)
            }
            "set-window-new-normal" => {
                need_arg_range(name, args, 1, 2)?;
                let window = decode_valid_window(interp, &args[0])?;
                let size = args.get(1).cloned().unwrap_or(Value::Nil);
                window.set_new_normal(size);
                Ok(size)
            }
            "window-resize-apply" => {
                need_arg_range(name, args, 0, 2)?;
                // window.c:Fwindow_resize_apply: commit the sizes staged
                // in FRAME's window tree when they add up to its root's.
                let horflag = args.get(1).is_some_and(Value::is_truthy);
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                let root = frame_root_window(frame);
                if !window_resize_check(root, horflag)
                    || window_new_pixel(root) != window_pixel_size(interp, root, horflag)
                {
                    return Ok(Value::Nil);
                }
                window_resize_apply(interp, root, horflag)?;
                interp.fset_redisplay(frame);
                Ok(Value::T)
            }
            "window-resize-apply-total" => {
                need_arg_range(name, args, 0, 2)?;
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                frame_size::window_resize_apply_total_for_frame(
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
                let window = decode_live_window(interp, args.first())?;
                if window_is_minibuffer(window) {
                    return Ok(Value::Integer(0));
                }
                // window_wants_mode_line and its siblings: WINDOW_LEAF_P
                // first, so a deleted selected window has none.
                let Some(buffer_id) = window_buffer_id_or_selected(interp, args.first())? else {
                    return Ok(Value::Integer(0));
                };
                let format_variable = match name {
                    "window-mode-line-height" => "mode-line-format",
                    "window-header-line-height" => "header-line-format",
                    _ => "tab-line-format",
                };
                Ok(Value::Integer(window_line_height(
                    interp,
                    buffer_id,
                    format_variable,
                )))
            }
            "window-right-divider-width"
            | "window-bottom-divider-width"
            | "window-scroll-bar-width" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_window(interp, args.first())?;
                Ok(Value::Integer(0))
            }
            // WINDOW_SCROLL_BAR_AREA_HEIGHT with WINDOW_HAS_HORIZONTAL_SCROLL_BAR
            // false in this configuration: zero, the argument unread.
            "window-scroll-bar-height" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Integer(0))
            }
            "move-to-window-line" => {
                need_arg_range(name, args, 1, 1)?;
                // window.c Fmove_to_window_line.
                let window = interp.selected_window_ref();
                // `if (!(BUFFERP (w->contents) && XBUFFER (w->contents) ==
                // current_buffer))'.
                if interp.selected_window_contents_buffer_id() != Some(interp.current_buffer_id()) {
                    return Err(LispError::Signal(
                        "move-to-window-line called from unrelated buffer".into(),
                    ));
                }
                let height = selected_command_text_height(interp);
                let (point_min, point_max) = {
                    let buffer = interp.buffer.borrow();
                    (buffer.point_min(), buffer.point_max())
                };
                let start = current_window_start(interp);
                if start < point_min || start > point_max {
                    visual_vertical_motion(interp, env, window, -((height / 2) as i64), None)?;
                    let (start, bolp) = {
                        let buffer = interp.buffer.borrow();
                        let pt = buffer.point();
                        (
                            pt,
                            pt <= buffer.point_min() || buffer.char_at(pt - 1) == Some('\n'),
                        )
                    };
                    interp.set_selected_window_start(start);
                    window.set_start_at_line_beg(bolp);
                    window.set_force_start(true);
                } else {
                    interp.buffer.borrow_mut().goto_char(start);
                }
                // displayed_window_lines: the window's rows, a window the
                // text does not fill counting one row fewer, its walk
                // fontifying what it passes.
                let from = interp.current_buffer().pt();
                crate::tty::fontify_window_rows(interp, env, window, from, height);
                let (_, shortage) = crate::lisp::primitives::window::move_screen_lines_in(
                    interp,
                    env,
                    window,
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
                let moved = visual_vertical_motion(interp, env, window, target, None)?;
                Ok(Value::Integer(moved))
            }
            "recenter" => {
                need_arg_range(name, args, 0, 2)?;
                let height = selected_command_text_height(interp);
                let interactive = !interp
                    .symbol_value_key(cached_symbol!("noninteractive"))
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
                // window.c scroll_command on the selected window: when its
                // buffer is not current, record_unwind_protect_excursion and
                // Fset_buffer (w->contents), whose Fget_buffer of a deleted
                // window's nil is CHECK_STRING's error.
                let direction: i64 = if name == "scroll-up" { 1 } else { -1 };
                let selected = interp.selected_window_contents_buffer_id();
                let count = interp.specpdl_index();
                if selected != Some(interp.current_buffer_id()) {
                    let buffer =
                        selected.ok_or_else(|| wrong_type_argument("stringp", Value::Nil))?;
                    interp.record_unwind_protect_excursion();
                    if let Err(error) = interp.set_current_buffer_id(buffer) {
                        let _ = interp.unbind_to(count, env);
                        return Err(error);
                    }
                }
                let result = (|| -> Result<(), LispError> {
                    let (n, whole) = match args.first().map(|v| v.kind()) {
                        None | Some(Kind::Nil) => (direction, true),
                        Some(Kind::Symbol(minus)) if minus.is_named("-") => (-direction, true),
                        Some(value) => (
                            prefix_numeric_value(&value.value())?.as_integer()? * direction,
                            false,
                        ),
                    };
                    let height = selected_command_text_height(interp);
                    let window = interp.selected_window_ref();
                    let total_lines = window_geometry(interp, window).1;
                    let hscroll = window.hscroll();
                    crate::lisp::primitives::window::window_scroll(
                        interp,
                        env,
                        n,
                        whole,
                        false,
                        height,
                        total_lines,
                        hscroll,
                    )
                })();
                let unbind = interp.unbind_to(count, env);
                result.and(unbind).map(|()| Value::Nil)
            }
            "window-text-pixel-size" => {
                // xdisp.c:Fwindow_text_pixel_size: measured in WINDOW's
                // buffer, MODE-LINES adding the lines it names.
                need_arg_range(name, args, 0, 7)?;
                let arg = |index: usize| args.get(index).copied().unwrap_or(Value::Nil);
                let window = decode_live_window(interp, args.first())?;
                let buffer_id = window
                    .live_buffer_id(interp)
                    .unwrap_or(interp.current_buffer_id());
                let saved_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                // window_text_pixel_size's start_display: init_iterator frees
                // the frame's realized faces after a face change.
                interp.init_iterator_faces_window(window);
                let size = super::buffer_edit::window_text_pixel_size(
                    interp,
                    env,
                    window,
                    [arg(1), arg(2), arg(3), arg(4), arg(5), arg(6)],
                );
                interp.set_current_buffer_id(saved_buffer)?;
                let size = size?;
                let mode_lines = arg(5);
                let mut extra = 0;
                for (line, format_variable) in [
                    ("tab-line", "tab-line-format"),
                    ("header-line", "header-line-format"),
                    ("mode-line", "mode-line-format"),
                ] {
                    if matches!(mode_lines.kind(), Kind::T) || mode_lines.symbol_str() == Some(line)
                    {
                        extra += window_line_height(interp, buffer_id, format_variable);
                    }
                }
                if extra == 0 {
                    return Ok(size);
                }
                // The y is the second element of (X . Y) and of (X Y START).
                let mut parts = Vec::new();
                let mut tail = size;
                while let Some((car, cdr)) = tail.cons_values() {
                    parts.push(car);
                    tail = cdr;
                }
                if parts.len() == 1 {
                    parts.push(tail);
                    let y = parts[1].integer_value().unwrap_or(0) + extra;
                    return Ok(Value::cons(parts[0], Value::Integer(y)));
                }
                let y = parts[1].integer_value().unwrap_or(0) + extra;
                parts[1] = Value::Integer(y);
                Ok(Value::list(parts))
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
                let window = decode_live_window(interp, args.get(1))?;
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
                // window_text_pixel_size over the whole accessible text,
                // the buffer shown in WINDOW meanwhile.
                let saved_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let arg = |index: usize| args.get(index).copied().unwrap_or(Value::Nil);
                let size = super::buffer_edit::window_text_pixel_size(
                    interp,
                    env,
                    window,
                    [
                        Value::Nil,
                        Value::Nil,
                        arg(2),
                        arg(3),
                        Value::Nil,
                        Value::Nil,
                    ],
                );
                interp.set_current_buffer_id(saved_buffer)?;
                let size = size?;
                // unwind_with_echo_area_buffer: WINDOW's points and start
                // come back through set_marker_restricted_both, inside its
                // buffer's accessible region (ERC's fill-wrap measures in a
                // narrowed buffer and so moves the start to BEGV).
                if let Some(window_buffer) = window.live_buffer_id(interp) {
                    let (point_min, point_max) = buffer_point_bounds(interp, window_buffer);
                    let start = window_start_marker(interp, window, window_buffer);
                    set_window_start_marker(interp, window, start.clamp(point_min, point_max));
                    for which in [WindowMarker::Pointm, WindowMarker::OldPointm] {
                        if let Some(point) = window_marker_position(window, which) {
                            set_window_marker(
                                interp,
                                window,
                                which,
                                window_buffer,
                                point.clamp(point_min, point_max),
                            );
                        }
                    }
                }
                Ok(size)
            }
            "get-display-property" => {
                need_arg_range(name, args, 2, 4)?;
                // xdisp.c:Fget_display_property: PROPERTIES, else
                // POSITION's `display' text property in OBJECT.
                let properties = match args.get(3).copied().filter(|value| !value.is_nil()) {
                    Some(properties) => {
                        if !properties.is_cons() {
                            return Err(wrong_type_argument("listp", properties));
                        }
                        properties
                    }
                    None => crate::lisp::primitives::call(
                        interp,
                        "get-text-property",
                        &[
                            args[0],
                            Value::symbol("display"),
                            args.get(2).copied().unwrap_or(Value::Nil),
                        ],
                        env,
                    )?,
                };
                Ok(crate::lisp::primitives::hash_insert::find_display_property(
                    properties, args[1],
                ))
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
                    // WINDOWP: decode_live_window's buffer.
                    _ if WindowRef::from_value(&object).is_some() => {
                        let window = decode_live_window(interp, Some(&object))?;
                        window
                            .live_buffer_id(interp)
                            .expect("a live window's buffer")
                    }
                    _ => return Err(wrong_type_argument("stringp", object)),
                };
                let saved = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let result = (|| {
                    if !interp
                        .symbol_value_key(cached_symbol!("bidi-display-reordering"))
                        .is_some_and(|value| value.is_truthy())
                        || !interp.current_buffer().multibyte()
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
                    return Ok(current_bidi_paragraph_direction_value(interp));
                };
                let Kind::Buffer(buffer_value) = buffer.kind() else {
                    return Err(wrong_type_argument("bufferp", *buffer));
                };
                // CHECK_BUFFER passes a killed buffer, whose freed text has
                // no strong character: the paragraph is left-to-right.
                if !buffer_value.is_live() {
                    return Ok(Value::symbol("left-to-right"));
                }
                let buffer_id = buffer_value.id;
                if !interp.has_buffer_id(buffer_id) {
                    return Err(wrong_type_argument("bufferp", *buffer));
                }
                let saved_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let direction = current_bidi_paragraph_direction_value(interp);
                interp.set_current_buffer_id(saved_buffer)?;
                Ok(direction)
            }
            "bidi-resolved-levels" => {
                need_arg_range(name, args, 0, 1)?;
                // CHECK_FIXNUM (vpos).
                if let Some(vpos) = args.first().filter(|value| !value.is_nil())
                    && vpos.fixnum().is_none()
                {
                    return Err(wrong_type_argument("fixnump", *vpos));
                }
                // Emaxx's headless renderer intentionally retains no glyph
                // matrix between evaluations, exactly the stale-display case
                // for which GNU documents a nil result.
                Ok(Value::Nil)
            }
            "line-pixel-height" => {
                need_args(name, args, 0)?;
                // Fline_pixel_height's start_display on the selected window:
                // init_iterator frees its frame's realized faces after a
                // face change.
                let window = interp.selected_window_ref();
                interp.init_iterator_faces_window(window);
                // The canonical terminal cell is one pixel high in the
                // headless frame used by both compatibility runners.
                Ok(Value::Integer(1))
            }
            "display--line-is-continued-p" => {
                need_args(name, args, 0)?;
                with_selected_window_buffer(interp, |interp| {
                    let original_point = interp.current_buffer().pt();
                    let result = (|| -> Result<Value, LispError> {
                        super::call(interp, "vertical-motion", &[Value::Integer(0)], env)?;
                        let screen_line_start = interp.current_buffer().pt();
                        let moved =
                            super::call(interp, "vertical-motion", &[Value::Integer(1)], env)?
                                .as_integer()?;
                        let next_screen_line = interp.current_buffer().pt();
                        let crosses_logical_line =
                            (screen_line_start..next_screen_line).any(|position| {
                                interp.current_buffer().char_at(position) == Some('\n')
                            });
                        Ok(
                            if moved == 1
                                && next_screen_line < interp.current_buffer().zv()
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
                // xdisp.c:Fmove_point_visually: CHECK_FIXNUM.
                let Kind::Integer(direction) = args[0].kind() else {
                    return Err(wrong_type_argument("fixnump", args[0]));
                };
                // Fmove_point_visually moves the current buffer's point; it
                // reads the selected window's buffer only to test whether
                // its glyph rows can answer, and never makes it current.
                (|| {
                    // `int dir = XFIXNUM (direction)': the fixnum's low 32
                    // bits, so most-positive-fixnum moves left.
                    let right = (direction as i32) > 0;
                    let right_to_left = matches!(
                        current_bidi_paragraph_direction_value(interp).kind(),
                        Kind::Symbol(ref direction) if direction.is_named("right-to-left")
                    );
                    let logical_forward = right != right_to_left;
                    let point = interp.current_buffer().pt();
                    let target = if logical_forward {
                        if point >= interp.current_buffer().zv() {
                            return Err(signal_condition("end-of-buffer"));
                        }
                        point + 1
                    } else {
                        if point <= interp.current_buffer().begv() {
                            return Err(signal_condition("beginning-of-buffer"));
                        }
                        point - 1
                    };
                    interp.buffer.borrow_mut().goto_char(target);
                    Ok(Value::Integer(target as i64))
                })()
            }
            "long-line-optimizations-p" => {
                need_args(name, args, 0)?;
                // xdisp.c: current_buffer->long_line_optimizations_p, which
                // redisplay_window sets.
                Ok(Value::from_bool(
                    interp.buffer.borrow().long_line_optimizations_p(),
                ))
            }
            // xsettings.c:Ftool_bar_get_system_style: a known `tool-bar-style',
            // else the system's (none without a display), else `image'.
            "tool-bar-get-system-style" => {
                need_args(name, args, 0)?;
                let style = interp
                    .symbol_value_key(cached_symbol!("tool-bar-style"))
                    .unwrap_or(Value::Nil);
                let known = ["image", "text", "both", "both-horiz", "text-image-horiz"]
                    .iter()
                    .any(|known| crate::lisp::types::eq_symbol_named(style, known));
                Ok(if known { style } else { Value::symbol("image") })
            }
            "tab-bar-height" | "tool-bar-height" => {
                need_arg_range(name, args, 0, 2)?;
                // xdisp.c: decode_any_frame, a dead frame among them.
                if let Some(frame) = args.first().filter(|frame| !frame.is_nil())
                    && !matches!(frame.kind(), Kind::Frame(_))
                {
                    return Err(wrong_type_argument("framep", *frame));
                }
                // The bootstrap terminal frame has no tab/tool-bar window.
                Ok(Value::Integer(0))
            }
            "lookup-image-map" => {
                need_args(name, args, 3)?;
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                // xdisp.c:Flookup_image_map: CHECK_FIXNUM (x), (y).
                let (Some(x), Some(y)) = (args[1].fixnum(), args[2].fixnum()) else {
                    let bad = if args[1].fixnum().is_none() {
                        args[1]
                    } else {
                        args[2]
                    };
                    return Err(wrong_type_argument("fixnump", bad));
                };
                Ok(lookup_image_map(&args[0], x, y))
            }
            "redisplay" => {
                need_arg_range(name, args, 0, 1)?;
                // dispnew.c:Fredisplay: the events no one sees and the
                // ripe timers first; nil, with no redisplay, when input is
                // pending (unless FORCE or `redisplay-dont-pause') or a
                // keyboard macro executes.
                let force = args.first().copied().unwrap_or(Value::Nil);
                crate::lisp::primitives::swallow_events(interp, env, true)?;
                let dont_pause = interp
                    .symbol_value_key(cached_symbol!("redisplay-dont-pause"))
                    .is_some_and(|value| value.is_truthy());
                if (crate::lisp::primitives::detect_input_pending_run_timers(interp, env, true)?
                    && force.is_nil()
                    && !dont_pause)
                    || interp
                        .symbol_value_key(cached_symbol!("executing-kbd-macro"))
                        .is_some_and(|value| value.is_truthy())
                {
                    return Ok(Value::Nil);
                }
                let count = interp.specpdl_index();
                if !force.is_nil() && !dont_pause {
                    interp.specbind_symbol(
                        &cached_symbol!("redisplay-dont-pause").with(|symbol| *symbol),
                        Value::T,
                        env,
                    )?;
                }
                // redisplay_preserve_echo_area (2): a message wiped by
                // input arrival but still last-displayed shows again
                // instead of clearing the row (menu-bar-open relies on
                // it).
                with_preserved_echo_redisplay(|| {
                    crate::lisp::primitives::run_tty_frame_redraw(interp, env)
                });
                interp.unbind_to(count, env)?;
                Ok(Value::T)
            }
            "redraw-frame" => {
                need_arg_range(name, args, 0, 1)?;
                let frame = decode_live_frame(interp, args.first(), true)?;
                // dispnew.c:redraw_frame: the terminal cleared and the
                // current matrices with it (the frame garbaged, which the
                // next redisplay draws from a cleared screen), every
                // window's display inaccurate.
                crate::lisp::primitives::set_frame_garbaged();
                if let Kind::Frame(frame) = frame.kind() {
                    redraw_frame_flags(interp, frame);
                }
                Ok(Value::Nil)
            }
            "redraw-display" => {
                need_args(name, args, 0)?;
                // redraw_frame on each visible frame; every live frame is.
                crate::lisp::primitives::set_frame_garbaged();
                let frames: Vec<_> = interp
                    .frame_states
                    .iter()
                    .copied()
                    .filter(|frame| frame.is_live())
                    .collect();
                for frame in frames {
                    redraw_frame_flags(interp, frame);
                }
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
                let window = if args[0].is_nil() {
                    interp.selected_window_ref()
                } else {
                    WindowRef::from_value(&args[0])
                        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), args[0]))?
                };
                window.set_cursor_off_p(!args[1].is_truthy());
                Ok(Value::Nil)
            }
            "internal-show-cursor-p" => {
                need_arg_range(name, args, 0, 1)?;
                let w = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => interp.selected_window_ref(),
                    Some(window) => WindowRef::from_value(&window.value()).ok_or_else(|| {
                        LispError::WrongTypeArgument("windowp".into(), window.value())
                    })?,
                };
                Ok(if !w.cursor_off_p() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-buffer-redisplay" => {
                need_args(name, args, 4)?;
                // xdisp.c:Fset_buffer_redisplay: the current buffer's
                // windows and mode lines redisplayed in full.
                let current = interp.current_buffer_id();
                interp.bset_update_mode_line(current);
                interp.prevent_redisplay_optimizations(current);
                Ok(Value::Nil)
            }
            "color-distance" => {
                need_arg_range(name, args, 2, 4)?;
                // FRAME is decoded before either color is read.
                let frame = super::frames::decode_live_frame(interp, args.get(2), true)?;
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
                        // STRINGP (color) && defined_color_hook (f, SSDATA
                        // (color), ...).
                        _ if matches!(value.kind(), Kind::StringObject(_)) => {
                            super::faces::tty_defined_color(interp, env, frame, value)?
                        }
                        _ => None,
                    };
                    rgb.ok_or_else(|| signal_error("Invalid color", *value))
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
                Ok(Value::Integer(super::faces::color_distance(left, right)))
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
                let id = frame_or_window_frame(interp, args.first())?;
                Ok(id
                    .borrow()
                    .selected_window
                    .map(WindowRef::value)
                    .unwrap_or(Value::Nil))
            }
            "frame-old-selected-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = super::frames::decode_live_frame(interp, args.first(), true)?;
                Ok(id
                    .borrow()
                    .old_selected_window
                    .map(WindowRef::value)
                    .unwrap_or(Value::Nil))
            }
            "set-frame-selected-window" => {
                need_arg_range(name, args, 2, 3)?;
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                let window = decode_live_window(interp, args.get(1))?;
                if window.frame() != Some(frame) {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "In `set-frame-selected-window', WINDOW is not on FRAME",
                        &[],
                    ));
                }
                if frame == interp.selected_frame_id {
                    let norecord = args.get(2).is_some_and(Value::is_truthy);
                    let mark = !norecord || norecord_marks_for_redisplay(args.get(2));
                    let selected = select_window_marking(interp, &args[1], norecord, mark)?;
                    if !norecord {
                        run_record_buffer_hook(interp, env)?;
                    }
                    Ok(selected)
                } else {
                    frame.borrow_mut().selected_window = Some(window);
                    Ok(args[1])
                }
            }
            "select-window" => {
                need_arg_range(name, args, 1, 2)?;
                // select_window's CHECK_LIVE_WINDOW: nil is no window here.
                if args[0].is_nil() {
                    return Err(wrong_type_argument("window-live-p", args[0]));
                }
                let norecord = args.get(1).is_some_and(Value::is_truthy);
                let mark = !norecord || norecord_marks_for_redisplay(args.get(1));
                let selected = select_window_marking(interp, &args[0], norecord, mark)?;
                if !norecord {
                    run_record_buffer_hook(interp, env)?;
                }
                Ok(selected)
            }
            "current-window-configuration" => {
                need_arg_range(name, args, 0, 1)?;
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                Ok(interp.window_configuration_value(frame))
            }
            "set-window-configuration" => {
                need_arg_range(name, args, 1, 3)?;
                let selected = interp.selected_frame_id;
                let restored = interp.apply_window_configuration_value(
                    &args[0],
                    args.get(2).is_some_and(Value::is_truthy),
                )?;
                if args.get(1).is_some_and(Value::is_truthy) && interp.frame_is_live(selected) {
                    interp.note_selected_frame(selected);
                    let window = selected.borrow().selected_window;
                    interp.set_selected_window(window);
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
                    interp.windows_or_buffers_changed = 29;
                    interp.update_mode_lines = 28;
                    return Ok(Value::T);
                };
                if let Ok(w) = decode_live_window(interp, Some(object)) {
                    mark_window_display_inaccurate(interp, w);
                    w.set_update_mode_line(true);
                    if let Some(buffer_id) = window_buffer_id(interp, object) {
                        interp.prevent_redisplay_optimizations(buffer_id);
                    }
                    interp.update_mode_lines = 29;
                    return Ok(Value::T);
                }
                let buffer_id = match object.kind() {
                    Kind::Buffer(buffer) => interp.live_buffer_id(buffer),
                    _ if string_like(object).is_some() => interp.find_buffer_named(object),
                    _ => None,
                };
                // A live buffer shown in a window: window_loop
                // (REDISPLAY_BUFFER_WINDOWS, object, false, Qvisible) over
                // the windows showing it, window_list_1's from the
                // selected window over the visible frames, a minibuffer
                // window never among them.
                let Some(buffer_id) =
                    buffer_id.filter(|buffer_id| interp.buffer_window_count(*buffer_id) > 0)
                else {
                    return Ok(Value::Nil);
                };
                let visible = Value::Symbol(cached_symbol!("visible").symbol());
                let selected_frame = interp.selected_frame_id;
                let start = frame_selected_window(selected_frame).value();
                let frames = window_list_frames(interp, selected_frame, &visible);
                let showing: WindowList =
                    window_list_windows(interp, Some(&Value::Nil), Some(&start), &frames)
                        .into_iter()
                        .filter(|window| {
                            is_live_ordinary_window(interp, *window)
                                && window.live_buffer_id(interp) == Some(buffer_id)
                        })
                        .collect();
                for window in showing.iter().copied() {
                    mark_window_display_inaccurate(interp, window);
                    window.set_update_mode_line(true);
                    interp.prevent_redisplay_optimizations(buffer_id);
                    interp.update_mode_lines = 27;
                }
                Ok(if showing.is_empty() {
                    Value::Nil
                } else {
                    Value::T
                })
            }
            "run-window-configuration-change-hook" => {
                need_arg_range(name, args, 0, 1)?;
                let Kind::Frame(frame) = decode_live_frame(interp, args.first(), true)?.kind()
                else {
                    unreachable!("decode_live_frame answers a frame")
                };
                run_window_configuration_change_hook(interp, frame, env)?;
                Ok(Value::Nil)
            }
            "resize-mini-window-internal" => {
                need_args(name, args, 1)?;
                // CHECK_LIVE_WINDOW: nil is no window here.
                if args[0].is_nil() {
                    return Err(wrong_type_argument("window-live-p", args[0]));
                }
                let window = decode_live_window(interp, Some(&args[0]))?;
                let frame = window.frame().expect("window stores its owning frame");
                if frame.borrow().minibuffer_window != Some(window) {
                    return Err(LispError::Signal("Not a valid minibuffer window".into()));
                }
                // window.c:Fresize_mini_window_internal: the staged root
                // and mini heights must add up to their current sum.
                let root = frame_root_window(frame);
                let old_height =
                    window_geometry(interp, root).1 + window_geometry(interp, window).1;
                let new_pixel = window_new_pixel(window);
                let delta = new_pixel - window_geometry(interp, window).1;
                if window_resize_check(root, false)
                    && new_pixel > 0
                    && old_height == window_new_pixel(root) + new_pixel
                {
                    resize_mini_window_apply(interp, frame, window, delta)?;
                    Ok(Value::T)
                } else {
                    Err(LispError::Signal("Cannot resize mini window".into()))
                }
            }
            "window-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                // decode_any_window: nil is the selected window.
                let window = match args.first().filter(|window| !window.is_nil()) {
                    Some(value) => WindowRef::from_value(value)
                        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), *value))?,
                    None => interp.selected_window_ref(),
                };
                // w->contents where it is a live buffer, read from the
                // window itself.
                Ok(window
                    .live_buffer_id(interp)
                    .map(|buffer_id| interp.buffer_value(buffer_id).expect("live buffer object"))
                    .unwrap_or(Value::Nil))
            }
            "window-old-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                let old_buffer = window.old_buffer();
                let frame = window.frame().expect("window stores its owning frame");
                Ok(if old_buffer.is_nil() {
                    Value::Nil
                } else if window.change_stamp() != frame.borrow().change_stamp {
                    // A window restored from a configuration.
                    Value::T
                } else {
                    old_buffer
                })
            }
            "window-old-point" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(window_marker_position(window, WindowMarker::OldPointm)
                    .map_or(Value::Nil, |point| Value::Integer(point as i64)))
            }
            "set-window-buffer" => {
                need_arg_range(name, args, 2, 3)?;
                let keep_margins = args.get(2).is_some_and(Value::is_truthy);
                // window.c:Fset_window_buffer: decode_live_window, then
                // Fget_buffer, CHECK_BUFFER and the buffer's liveness.
                let w = decode_live_window(interp, args.first())?;
                let window = w.value();
                let buffer = interp
                    .get_buffer(&args[1])?
                    .ok_or_else(|| LispError::WrongTypeArgument("bufferp".into(), Value::Nil))?;
                let buffer_id = interp
                    .live_buffer_id(buffer)
                    .ok_or_else(|| LispError::Signal("Attempt to display deleted buffer".into()))?;
                // tem = w->contents: nil only for the selected window nil
                // decoded, deleted.
                if !w.is_valid() {
                    return Err(LispError::Signal("Window is deleted".into()));
                }
                let previous = window_buffer_id(interp, &window);
                let changes_buffer = previous.is_some_and(|previous| previous != buffer_id);
                if changes_buffer {
                    if matches!(w.dedicated().kind(), Kind::T) {
                        // error ("Window is dedicated to `%s'", SDATA (BVAR
                        // (XBUFFER (tem), name))).
                        let name = previous
                            .and_then(|id| interp.get_buffer_by_id(id).map(|b| b.name()))
                            .unwrap_or(Value::Nil);
                        return Err(crate::lisp::primitives::values::doprnt_error_sdata(
                            Some(interp),
                            "Window is dedicated to `{}'",
                            &[name],
                        ));
                    }
                    w.set_dedicated(Value::Nil);
                    if let Ok(record_window_buffer) =
                        interp.lookup_function_key(cached_symbol!("record-window-buffer"), env)
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
                // set_window_buffer (window, buffer, true, !NILP
                // (keep_margins)).
                interp.unshow_window_buffer(w)?;
                set_window_buffer_object(
                    interp,
                    w,
                    (buffer_id, Value::Buffer(buffer)),
                    true,
                    keep_margins,
                    env,
                )?;
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
                                .map(WindowRef::value)
                                .unwrap_or(Value::Nil)
                        } else {
                            interp.selected_window_value()
                        }
                    });
                let window = decode_any_window(interp, &start)?;
                if Some(frame) != window.frame().map(Value::Frame) {
                    return Err(LispError::Signal("Window is on a different frame".into()));
                }
                // window_list_1's decode_next_window_args: decode_live_window.
                decode_live_window(interp, Some(&start))?;
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
                // decode_next_window_args: decode_live_window.
                let window = decode_live_window(interp, args.first())?;
                let current = window.value();
                let own_frame = window.frame().expect("window stores its owning frame");
                let frames =
                    window_list_frames(interp, own_frame, args.get(2).unwrap_or(&Value::Nil));
                let windows = window_list_value(interp, args.get(1), Some(&current), &frames);
                if name == "window-list-1" {
                    return Ok(windows);
                }
                let windows = windows.to_vec()?;
                // window.c:next_window walks Vwindow_list from WINDOW, the
                // minibuffer window last of its frame's: a WINDOW that is
                // no candidate itself (an inactive minibuffer window) is
                // followed by the first candidate and preceded by the last.
                let listed = windows
                    .first()
                    .is_some_and(|first| first.word() == current.word());
                Ok(if name == "next-window" {
                    if listed {
                        windows.get(1).or(windows.first())
                    } else {
                        windows.first()
                    }
                } else {
                    windows.last()
                }
                .cloned()
                .unwrap_or(current))
            }
            "frame-root-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = frame_or_window_frame(interp, args.first())?;
                Ok(id
                    .borrow()
                    .root_window
                    .map(WindowRef::value)
                    .unwrap_or(Value::Nil))
            }
            "frame-first-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = frame_or_window_frame(interp, args.first())?;
                let mut window = id
                    .borrow()
                    .root_window
                    .map(WindowRef::value)
                    .unwrap_or(Value::Nil);
                while let Some(this) = WindowRef::from_value(&window) {
                    let Some(child) = this.first_child() else {
                        return Ok(this.value());
                    };
                    window = child.value();
                }
                Ok(interp.selected_window_value())
            }
            "window-prev-buffers" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(window.prev_buffers())
            }
            "set-window-prev-buffers" => {
                need_args(name, args, 2)?;
                let window = decode_live_window(interp, args.first())?;
                window.set_prev_buffers(args[1]);
                Ok(args[1])
            }
            "window-next-buffers" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(window.next_buffers())
            }
            "set-window-next-buffers" => {
                need_args(name, args, 2)?;
                let window = decode_live_window(interp, args.first())?;
                window.set_next_buffers(args[1]);
                Ok(args[1])
            }
            "window-parameter" => {
                need_args(name, args, 2)?;
                // decode_any_window, then Fassq in the window's own
                // parameters.
                let window = decode_any_window(interp, &args[0])?;
                Ok(window_parameter_value(window, &args[1]))
            }
            "set-window-parameter" => {
                need_args(name, args, 3)?;
                let window = decode_any_window(interp, &args[0])?;
                set_window_parameter_value(window, args[1], args[2])
            }
            "window-parameters" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_valid_window(interp, args.first().unwrap_or(&Value::Nil))?;
                // window.c:Fwindow_parameters: Fcopy_alist of the window's.
                let parameters = window.window_parameters();
                crate::lisp::primitives::values::copy_alist_value(&parameters)
            }
            "window-frame" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_valid_window(interp, args.first().unwrap_or(&Value::Nil))?;
                Ok(window.frame().map_or(Value::Nil, Value::Frame))
            }
            // WINDOWP, WINDOW_VALID_P and WINDOW_LIVE_P of the window, each
            // a subr of its own.
            "windowp" => {
                need_args(name, args, 1)?;
                Ok(Value::from_bool(WindowRef::from_value(&args[0]).is_some()))
            }
            "window-valid-p" => {
                need_args(name, args, 1)?;
                Ok(Value::from_bool(
                    WindowRef::from_value(&args[0]).is_some_and(WindowRef::is_valid),
                ))
            }
            "window-live-p" => {
                need_args(name, args, 1)?;
                Ok(Value::from_bool(
                    WindowRef::from_value(&args[0])
                        .is_some_and(|window| window.live_buffer_id(interp).is_some()),
                ))
            }
            "window-parent" => window_link_value(interp, name, args, WindowLink::Parent),
            "window-prev-sibling" => window_link_value(interp, name, args, WindowLink::Prev),
            "window-next-sibling" => window_link_value(interp, name, args, WindowLink::Next),
            "window-top-child" => window_child_value(interp, name, args, false),
            "window-left-child" => window_child_value(interp, name, args, true),
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
                // CHECK_VALID_WINDOW: nil is no window here.
                let window = WindowRef::from_value(&args[0])
                    .filter(|window| window.is_valid())
                    .ok_or_else(|| wrong_type_argument("window-valid-p", args[0]))?;
                if !window.is_internal() {
                    return Err(LispError::Signal(
                        "Combination limit is meaningful for internal windows only".into(),
                    ));
                }
                if name == "set-window-combination-limit" {
                    window.set_combination_limit(args[1]);
                    Ok(args[1])
                } else {
                    Ok(window.combination_limit())
                }
            }
            "window-minibuffer-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_valid_window(interp, args.first().unwrap_or(&Value::Nil))?;
                Ok(Value::from_bool(window_is_minibuffer(window)))
            }
            "window-at" => {
                need_arg_range(name, args, 2, 3)?;
                // decode_live_frame, then CHECK_NUMBER of each coordinate.
                let frame = super::frames::decode_live_frame(interp, args.get(2), true)?;
                let coordinate = |value: Value| match value.kind() {
                    Kind::Integer(_) | Kind::Float(_) | Kind::BigInteger(_) => {
                        value.as_float().map(|value| value.floor() as i64)
                    }
                    _ => Err(wrong_type_argument("numberp", value)),
                };
                let x = coordinate(args[0])?;
                let y = coordinate(args[1])?;
                Ok(window_at_frame_coordinates(interp, frame, x, y)
                    .map_or(Value::Nil, |id| id.value()))
            }
            "split-window-internal" => {
                need_args(name, args, 4)?;
                let new = split_window_tree(interp, &args[0], &args[1], &args[2], &args[3], env)?;
                // fset_redisplay: the frame's window tree changed.
                if let Ok(w) = decode_live_window(interp, Some(&new)) {
                    interp.fset_redisplay_window_frame(w);
                }
                Ok(new)
            }
            "delete-window-internal" => {
                need_args(name, args, 1)?;
                let frame = decode_any_window(interp, &args[0])
                    .ok()
                    .and_then(|w| w.frame());
                let result = delete_window_from_tree(interp, &args[0])?;
                // fset_redisplay: the window was unlinked.
                match frame {
                    Some(frame) => interp.fset_redisplay(frame),
                    None => interp.redisplay_other_windows(),
                }
                Ok(result)
            }
            "delete-other-windows-internal" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let root = args.get(1).cloned().unwrap_or(Value::Nil);
                let result = delete_other_windows_from_tree(interp, &window, &root, env)?;
                // fset_redisplay: the tree was replaced; the window that
                // stays is to be displayed anew.
                if let Ok(w) = decode_live_window(interp, Some(&window)) {
                    w.set_window_end_valid(false);
                    interp.fset_redisplay_window_frame(w);
                }
                Ok(result)
            }
            "run-window-scroll-functions" => {
                need_arg_range(name, args, 0, 1)?;
                // The hook hears WINDOW as given, nil included; the
                // selected window stands in for nil only to be decoded.
                let given = args.first().cloned().unwrap_or(Value::Nil);
                let window = if given.is_nil() {
                    interp.selected_window_value()
                } else {
                    given
                };
                let buffer_id = window_buffer_id(interp, &window)
                    .ok_or_else(|| wrong_type_argument("window-live-p", window))?;
                let start = Value::Integer(window_start(interp, Some(&window))? as i64);
                let original_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let result = run_window_scroll_functions(interp, given, start, env);
                if interp.has_buffer_id(original_buffer) {
                    interp.set_current_buffer_id(original_buffer)?;
                }
                result.map(|()| Value::Nil)
            }
            "window-dedicated-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(window.dedicated())
            }
            "set-window-dedicated-p" => {
                need_args(name, args, 2)?;
                let window = decode_live_window(interp, args.first())?;
                window.set_dedicated(args[1]);
                Ok(args[1])
            }
            "posn-at-x-y" => {
                need_arg_range(name, args, 2, 4)?;
                position_at_coordinates(interp, args, env)
            }
            "posn-at-point" => {
                need_arg_range(name, args, 0, 2)?;
                // keyboard.c:Fposn_at_point: where pos-visible-in-window-p
                // puts POS in WINDOW (the selected one for nil), as
                // posn-at-x-y describes that place.
                let window = args
                    .get(1)
                    .copied()
                    .filter(|window| !window.is_nil())
                    .unwrap_or_else(|| interp.selected_window_value());
                let pos = args.first().copied().unwrap_or(Value::Nil);
                let visible = super::call(
                    interp,
                    "pos-visible-in-window-p",
                    &[pos, window, Value::T],
                    env,
                )?;
                if visible.is_nil() {
                    return Ok(Value::Nil);
                }
                let x = visible.car()?;
                let rest = visible.cdr()?;
                let mut y = rest.car()?;
                let aux = rest.cdr()?;
                // Point invisible due to hscrolling?
                if x.as_integer()? < -1 {
                    return Ok(Value::Nil);
                }
                let y_coord = y.as_integer()?;
                if !aux.is_nil() && y_coord < 0 {
                    y = Value::Integer(y_coord + aux.car()?.as_integer()?);
                }
                super::call(interp, "posn-at-x-y", &[x, y, window, Value::Nil], env)
            }
            "window-display-table" | "window-cursor-type" => {
                need_arg_range(name, args, 0, 1)?;
                let window = decode_live_window(interp, args.first())?;
                Ok(if name == "window-display-table" {
                    window.display_table()
                } else {
                    window.cursor_type()
                })
            }
            "set-window-display-table" | "set-window-cursor-type" => {
                need_args(name, args, 2)?;
                let window = decode_live_window(interp, args.first())?;
                if name == "set-window-cursor-type" && !valid_window_cursor_type(&args[1]) {
                    return Err(LispError::Signal("Invalid cursor type".into()));
                }
                let value = args[1];
                if name == "set-window-display-table" {
                    window.set_display_table(value);
                } else {
                    window.set_cursor_type(value);
                    interp.wset_redisplay(window);
                }
                Ok(value)
            }
            "window-fringes" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_window(interp, args.first())?;
                Ok(Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Nil,
                    Value::Nil,
                ]))
            }
            "set-window-fringes" => {
                need_arg_range(name, args, 2, 5)?;
                decode_live_window(interp, args.first())?;
                // GNU's TTY implementation leaves fringe settings unchanged.
                Ok(Value::Nil)
            }
            "window-scroll-bars" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_window(interp, args.first())?;
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
                decode_live_window(interp, args.first())?;
                // GNU's TTY implementation leaves scroll-bar settings unchanged.
                Ok(Value::Nil)
            }
            "send-string-to-terminal" => {
                // dispnew.c: decode_live_terminal, CHECK_STRING, and the
                // string's bytes written unaltered to the terminal.
                need_arg_range(name, args, 1, 2)?;
                let terminal = require_live_terminal(interp, args.get(1))?;
                let Kind::StringObject(string) = args[0].kind() else {
                    return Err(wrong_type_argument("stringp", args[0]));
                };
                let bytes = string.borrow().bytes().to_vec();
                crate::lisp::eval::terminal::send_string_to_terminal(terminal, &bytes);
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
                        let name = value.value();
                        if !matches!(name.kind(), Kind::StringObject(_)) {
                            return Err(wrong_type_argument("stringp", name));
                        }
                        interp.find_buffer_named(&name)
                    }
                };
                let Some(buffer_id) = buffer_id else {
                    return Ok(Value::Nil);
                };
                Ok(
                    buffer_window(interp, buffer_id, args.get(1).unwrap_or(&Value::Nil))?
                        .unwrap_or(Value::Nil),
                )
            }
            "minibuffer-window" => {
                need_arg_range(name, args, 0, 1)?;
                let id = super::frames::decode_live_frame(interp, args.first(), true)?;
                Ok(id
                    .borrow()
                    .minibuffer_window
                    .map(WindowRef::value)
                    .unwrap_or(Value::Nil))
            }
            "set-minibuffer-window" => {
                need_args(name, args, 1)?;
                let Some(window) = WindowRef::from_value(&args[0]) else {
                    return Err(wrong_type_argument("windowp", args[0]));
                };
                if !window_is_minibuffer(window) {
                    return Err(LispError::Signal(
                        "Window is not a minibuffer window".into(),
                    ));
                }
                // minibuf.c:Fset_minibuffer_window: `minibuf_window =
                // window', which the next frame switch or read sets again
                // from the selected frame; no frame's own minibuffer window
                // changes.
                interp.set_minibuf_window(Some(window));
                Ok(args[0])
            }
            "minibuffer-selected-window" => {
                need_args(name, args, 0)?;
                // minibuf.c:Fminibuffer_selected_window: the window only
                // while a minibuffer is active, its window selected, and the
                // remembered window live.
                if interp.minibuffer_depth() == 0 {
                    return Ok(Value::Nil);
                }
                let selected = interp.selected_window_value();
                let selected_is_mini =
                    WindowRef::from_value(&selected).is_some_and(window_is_minibuffer);
                let remembered = interp.minibuffer_selected_window().map(WindowRef::value);
                Ok(match remembered {
                    Some(window)
                        if selected_is_mini
                            && decode_live_window(interp, Some(&window)).is_ok() =>
                    {
                        window
                    }
                    _ => Value::Nil,
                })
            }
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
                let window = decode_live_window(interp, args.first().filter(|w| !w.is_nil()))?;
                let noforce = args.get(2).is_some_and(Value::is_truthy);
                set_window_start_value(interp, window, &args[1], noforce)?;
                Ok(args[1])
            }
            "set-window-point" => {
                need_args(name, args, 2)?;
                let window = decode_live_window(interp, args.first().filter(|w| !w.is_nil()))?;
                let buffer_id = window
                    .live_buffer_id(interp)
                    .unwrap_or_else(|| interp.current_buffer_id());
                // window.c Fset_window_point: the selected window moves its
                // buffer's point (Fgoto_char), in that buffer made current
                // for it after CHECK_FIXNUM_COERCE_MARKER when it is not;
                // any other window's point marker is set_marker_restricted,
                // nil or a marker that points nowhere leaving it pointing
                // nowhere.
                if window == interp.selected_window_ref() {
                    if buffer_id == interp.current_buffer_id() {
                        super::buffer_edit::direct_goto_char(interp, &args[1..2], env)?;
                    } else {
                        // CHECK_FIXNUM_COERCE_MARKER: POS becomes the
                        // fixnum fix_position makes it, the value returned.
                        let pos = Value::Integer(crate::lisp::primitives::buffers::fix_position(
                            interp, &args[1],
                        )?);
                        let saved = interp.current_buffer_id();
                        interp.set_current_buffer_id(buffer_id)?;
                        let moved = super::buffer_edit::direct_goto_char(interp, &[pos], env);
                        interp.set_current_buffer_id(saved)?;
                        moved?;
                        return Ok(pos);
                    }
                } else {
                    set_window_marker_restricted_value(
                        interp,
                        window,
                        WindowMarker::Pointm,
                        buffer_id,
                        &args[1],
                    )?;
                    interp.wset_redisplay(window);
                }
                Ok(args[1])
            }
            "set-window-vscroll" => {
                need_arg_range(name, args, 2, 4)?;
                // decode_live_window, CHECK_NUMBER (vscroll), then
                // Fwindow_vscroll: no vertical scrolling on a text frame.
                decode_live_window(interp, args.first())?;
                if !matches!(
                    args[1].kind(),
                    Kind::Integer(_) | Kind::Float(_) | Kind::BigInteger(_)
                ) {
                    return Err(wrong_type_argument("numberp", args[1]));
                }
                Ok(Value::Integer(0))
            }
        }
    }
);

// editfns.c:Ffollowing_char and Fprevious_char as functions of the
// interpreter, read through a shared reference, that the direct entries
// below and native code's entries (native_comp/runtime.rs) both call.
// Each reads the current buffer's point, restriction and one character
// under shared borrows; a read of multibyte text may fill the text's
// own caches of characters and positions, which are host memory, and
// none signals, runs Lisp or makes a Lisp object.  A character code is
// at most MAX_CHAR, so `Value::Integer' makes a fixnum of it.

/// editfns.c:Ffollowing_char.
pub(crate) fn following_char(interp: &Interpreter) -> Value {
    Value::Integer(crate::lisp::primitives::following_char_code(interp))
}

/// editfns.c:Fprevious_char, `preceding-char'.
pub(crate) fn preceding_char(interp: &Interpreter) -> Value {
    Value::Integer(crate::lisp::primitives::preceding_char_code(interp))
}

/// The `preceding-char' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_preceding_char(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(preceding_char(interp))
}

/// The `following-char' primitive, callable directly (a subr's function
/// pointer).
pub(super) fn direct_following_char(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(following_char(interp))
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
        .symbol_value_key(cached_symbol!("meta-prefix-char"))
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
        .map_err(|error| string_storage_error(interp, error))
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
            .map_err(|error| string_storage_error(interp, error));
    }
    // EVENT_HEAD.
    if matches!(key.kind(), Kind::Cons(_)) {
        key = key.car()?;
    }
    // SYMBOLP, then SYMBOL_NAME of XSYMBOL: a positioned symbol's name
    // while enabled.
    let key = lisp_symbol(interp, key).unwrap_or(key);
    match key.kind() {
        // Normal character.
        Kind::Integer(code) => {
            let mut bytes = Vec::new();
            push_key_description(code, &mut bytes);
            make_specified_string(bytes, true).map_err(|error| string_storage_error(interp, error))
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
            make_string(buffer).map_err(|error| string_storage_error(interp, error))
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
fn text_char_description(interp: &mut Interpreter, character: &Value) -> Result<Value, LispError> {
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
    result.map_err(|error| string_storage_error(interp, error))
}

/// Fbuffer_text_pixel_size's measurement of the current buffer's
/// accessible text on a terminal: the widest line's columns by the line
/// count.
/// keyboard.c:Fset_input_interrupt_mode: a change of `interrupt_input'
/// between reset_all_sys_modes and init_all_sys_modes.
fn set_input_interrupt_mode(interp: &mut Interpreter, interrupt: Value) {
    let new = !interrupt.is_nil();
    if new != interp.input_interrupt_mode() {
        crate::lisp::primitives::reset_all_sys_modes();
        interp.set_input_interrupt_mode(new);
        crate::lisp::primitives::init_all_sys_modes(interp, new);
    }
}

/// keyboard.c:Fset_output_flow_control and Fset_input_meta_mode:
/// decode_tty_terminal's terminal, nothing for one that is no tty, and
/// the tty's mode changed between reset_sys_modes and init_sys_modes.
fn set_tty_input_mode(
    interp: &mut Interpreter,
    name: &str,
    value: Value,
    terminal: Value,
) -> Result<(), LispError> {
    let Some(decoded) = interp.decode_terminal(&terminal) else {
        return Err(wrong_type_argument("terminal-live-p", terminal));
    };
    let (is_tty, is_session) = {
        let state = decoded.borrow();
        (state.kind.is_some(), state.device.is_none())
    };
    if !is_tty || !is_session {
        return Ok(());
    }
    let interrupt_input = interp.input_interrupt_mode();
    crate::lisp::primitives::change_tty_input_modes(interp, interrupt_input, |modes| {
        if name == "set-output-flow-control" {
            modes.flow_control = !value.is_nil();
        } else {
            modes.meta_key = if value.is_nil() {
                0
            } else if matches!(value.kind(), Kind::T) {
                1
            } else if crate::lisp::types::eq_symbol_named(value, "encoded") {
                3
            } else {
                2
            };
        }
    });
    Ok(())
}

/// keyboard.c:Fset_quit_char: nothing without the controlling terminal;
/// QUIT an ASCII character, kept in the meta mode's range.
fn set_quit_char(interp: &mut Interpreter, quit: Value) -> Result<(), LispError> {
    if !crate::lisp::primitives::dev_tty_terminal() {
        return Ok(());
    }
    match quit.kind() {
        Kind::Integer(code) if (0..=0o400).contains(&code) => {
            let interrupt_input = interp.input_interrupt_mode();
            crate::lisp::primitives::set_quit_char(interp, code, interrupt_input);
            Ok(())
        }
        _ => Err(LispError::Signal("QUIT must be an ASCII character".into())),
    }
}

/// `EQ (norecord, Qmark_for_redisplay)'.
fn norecord_marks_for_redisplay(norecord: Option<&Value>) -> bool {
    norecord.is_some_and(|value| {
        crate::lisp::types::eq(
            *value,
            cached_symbol!("mark-for-redisplay").with(|symbol| Value::Symbol(*symbol)),
        )
    })
}

/// dispnew.c:redraw_frame's flags: `fset_redisplay (f)' and every window
/// of the frame made inaccurate (mark_window_display_accurate of its root
/// window with false).
fn redraw_frame_flags(interp: &mut Interpreter, frame: crate::lisp::types::FrameRef) {
    interp.fset_redisplay(frame);
    if let Some(root) = frame.borrow().root_window {
        mark_window_display_inaccurate(interp, root);
    }
}

/// The runtime leaves this OS thread (`runtime::release_thread_state'):
/// the echo area's messages, their face spans and its display hook go.
pub(crate) fn release_thread_state() {
    crate::lisp::runtime::release_local(&ECHO_AREA_MESSAGE, |cell| {
        if let Ok(mut value) = cell.try_borrow_mut() {
            *value = None;
        }
    });
    crate::lisp::runtime::release_local(&ECHO_AREA_MESSAGE_VALUE, |cell| {
        if let Ok(mut value) = cell.try_borrow_mut() {
            *value = None;
        }
    });
    crate::lisp::runtime::release_local(&ECHO_AREA_LAST_DISPLAYED, |cell| {
        if let Ok(mut value) = cell.try_borrow_mut() {
            *value = None;
        }
    });
    crate::lisp::runtime::release_local(&ECHO_AREA_DISPLAY, |cell| {
        if let Ok(mut value) = cell.try_borrow_mut() {
            *value = None;
        }
    });
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
        interp.set_symbol_value_key(cached_symbol!("clear-message-function"), callback);
        interp.set_symbol_value_key(cached_symbol!("inhibit-quit"), Value::Nil);
        interp.set_symbol_value_key(cached_symbol!("inhibit-redisplay"), Value::Nil);
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
        assert_eq!(
            interp.symbol_value_key(cached_symbol!("inhibit-quit")),
            Some(Value::Nil)
        );
        assert_eq!(
            interp.symbol_value_key(cached_symbol!("inhibit-redisplay")),
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
        interp.set_symbol_value_key(cached_symbol!("clear-message-function"), callback);
        for gc_inhibited in [true, false] {
            set_echo_area_message(Some("must clear".into()));
            if gc_inhibited {
                interp.inhibit_garbage_collection();
            } else {
                interp.set_symbol_value_key(
                    cached_symbol!("inhibit-eval-during-redisplay"),
                    Value::T,
                );
            }
            let result = clear_message(&mut interp, &mut env, true, true);
            if gc_inhibited {
                interp.allow_garbage_collection();
            }
            result.expect("inhibited callbacks do not preserve the message");
            assert!(echo_area_message().is_none());
        }
        interp.set_symbol_value_key(cached_symbol!("inhibit-eval-during-redisplay"), Value::Nil);
        interp.set_symbol_value_key(cached_symbol!("clear-message-function"), Value::Integer(42));
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
        interp.set_symbol_value_key(
            cached_symbol!("clear-message-function"),
            Value::symbol("ignore"),
        );
        interp.set_symbol_value_key(cached_symbol!("inhibit-quit"), Value::Nil);
        interp.set_symbol_value_key(cached_symbol!("inhibit-redisplay"), Value::Nil);
        set_echo_area_message(Some("old message".into()));
        let tick = echo_area_message_tick();
        clear_message(&mut interp, &mut env, true, true).expect("callback signal is caught");
        assert!(echo_area_message().is_none());
        assert_eq!(echo_area_message_tick(), tick);
        assert_eq!(
            interp.symbol_value_key(cached_symbol!("inhibit-quit")),
            Some(Value::Nil)
        );
        assert_eq!(
            interp.symbol_value_key(cached_symbol!("inhibit-redisplay")),
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
        interp.set_symbol_value_key(cached_symbol!("clear-message-function"), callback);
        interp.set_symbol_value_key(cached_symbol!("inhibit-quit"), Value::Nil);
        interp.set_symbol_value_key(cached_symbol!("inhibit-redisplay"), Value::Nil);
        set_echo_area_message(Some("not reached".into()));
        let catcher = interp.push_active_catch_tag(tag);
        let result = clear_message(&mut interp, &mut env, true, true);
        interp.pop_handler_bindings(catcher);
        assert!(matches!(
            result.as_ref().map_err(LispError::kind),
            Err(LispErrorKind::Throw(actual, value)) if values_eql(actual, &tag) && *value == Value::Integer(7)
        ));
        assert_eq!(
            interp.symbol_value_key(cached_symbol!("inhibit-quit")),
            Some(Value::Nil)
        );
        assert_eq!(
            interp.symbol_value_key(cached_symbol!("inhibit-redisplay")),
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
        interp.set_symbol_value_key(cached_symbol!("clear-message-function"), callback);
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
        crate::lisp::primitives::unwind_active_minibuffer(&mut interp, active, &mut env)
            .expect("unwind the read");
    }

    #[test]
    fn clear_message_nonlocal_exit_restores_minibuffer_entry_state() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let saved_buffer = interp.current_buffer_id();
        let saved_window = interp.selected_window_ref();
        let tag = Value::symbol("exit");
        let callback = bytecode_callback(
            &mut interp,
            &[192, 193, 194, 34, 135],
            vec![Value::symbol("throw"), tag, Value::Integer(7)],
        );
        interp.set_symbol_value_key(cached_symbol!("clear-message-function"), callback);
        let catcher = interp.push_active_catch_tag(tag);
        let result = crate::lisp::primitives::activate_minibuffer(
            &mut interp,
            &Value::String("Prompt: ".into()),
            &Value::String("input".into()),
            Value::Nil,
            &mut env,
        );
        interp.pop_handler_bindings(catcher);
        assert!(matches!(
            result.as_ref().map_err(LispError::kind),
            Err(LispErrorKind::Throw(actual, value)) if values_eql(actual, &tag) && *value == Value::Integer(7)
        ));
        assert_eq!(interp.current_buffer_id(), saved_buffer);
        assert_eq!(interp.selected_window_ref(), saved_window);
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
        interp.set_symbol_value_key(cached_symbol!("clear-message-function"), callback);
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
