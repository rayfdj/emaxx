use super::*;
use crate::lisp::types::Kind;

fn event_vector(events: impl IntoIterator<Item = Value>) -> Value {
    Value::vector(events)
}

/// keyboard.c:make_event_array: a unibyte string when every event is a
/// character in 0...127 once the meta bit and the bits above it are
/// dropped (meta moves to the string byte's 0200 bit), else a vector.
pub(crate) fn event_array(events: &[Value], force_vector: bool) -> Value {
    const CHAR_META: i64 = 0x0800_0000;
    if !force_vector {
        let bytes = events
            .iter()
            .map(|event| match event.kind() {
                Kind::Integer(code) if code & (CHAR_META - 1) < 0o200 => {
                    Some((code & 0o177) as u8 | if code & CHAR_META != 0 { 0o200 } else { 0 })
                }
                _ => None,
            })
            .collect::<Option<Vec<u8>>>();
        if let Some(bytes) = bytes {
            return crate::lisp::primitives::bytes_to_unibyte_value(&bytes);
        }
    }
    event_vector(events.iter().cloned())
}

fn execute_kbd_macro(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("execute-kbd-macro", args, 1, 3)?;
    // macros.c:Fexecute_kbd_macro: the iteration count reset, COUNT
    // decoded, then MACRO's function indirection checked.
    interp.kbd_macro_iterations = 0;
    let mut repeat = args
        .get(1)
        .map(prefix_numeric_value)
        .transpose()?
        .unwrap_or(Value::Integer(1))
        .as_integer()?;
    let final_macro = if matches!(args[0].kind(), Kind::Symbol(_)) {
        super::call(
            interp,
            "indirect-function",
            std::slice::from_ref(&args[0]),
            env,
        )?
    } else {
        args[0]
    };
    if !final_macro.is_string() && !is_vector_value(&final_macro) {
        return Err(LispError::Signal(
            "Keyboard macros must be strings or vectors".into(),
        ));
    }
    let loop_function = args.get(2).cloned().unwrap_or(Value::Nil);
    let previous_macro = interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
        .unwrap_or(Value::Nil);
    let previous_index = interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro-index"), env)
        .unwrap_or(Value::Nil);
    let previous_real_this_command = interp
        .lookup_var_key(cached_symbol!("real-this-command"), env)
        .unwrap_or(Value::Nil);
    // macros.c:Fexecute_kbd_macro saves the public state in two ordinary
    // conses, the argument of its record_unwind_protect (pop_kbd_macro),
    // while a borrowed stack root keeps the final array and loop function
    // alive across replacement, nested commands and collecting callbacks.
    let pdlcount = interp.specpdl_index();
    interp.record_unwind(crate::lisp::eval::UnwindAction::KbdMacro(Value::cons(
        previous_macro,
        Value::cons(previous_index, previous_real_this_command),
    )));
    let roots = [final_macro, loop_function];
    interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
        let mut result = Ok(());
        let mut success_count = 0;
        loop {
            interp.set_variable_key(cached_symbol!("executing-kbd-macro"), roots[0], env);
            interp.executing_kbd_macro = true;
            interp.set_variable_key(
                cached_symbol!("executing-kbd-macro-index"),
                Value::Integer(0),
                env,
            );
            interp.set_variable_key(cached_symbol!("prefix-arg"), Value::Nil, env);

            if !roots[1].is_nil() {
                match call_function_value(interp, &roots[1], &[], env) {
                    Ok(value) if value.is_nil() => break,
                    Ok(_) => {}
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                }
            }

            // command_loop_2 (list1 (Qminibuffer_quit)): the iteration's
            // command loop, which returns at the macro's end; its other
            // errors leave the macro.
            if let Err(error) = crate::lisp::primitives::command_loop_2(
                interp,
                env,
                crate::lisp::primitives::CommandLoopHandlers::MinibufferQuit,
            ) {
                result = Err(error);
                break;
            }
            success_count += 1;
            interp.kbd_macro_iterations = success_count;
            if let Err(error) = interp.maybe_quit(env) {
                result = Err(error);
                break;
            }

            if repeat != 0 {
                repeat = repeat.saturating_sub(1);
                if repeat == 0 {
                    break;
                }
            }
            let still_executing = interp
                .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
                .is_some_and(|value| value.is_string() || is_vector_value(&value));
            if !still_executing {
                break;
            }
        }

        if result.is_ok() {
            interp.executing_kbd_macro = false;
        }
        // unbind_to (pdlcount, Qnil): pop_kbd_macro, once for normal
        // completion, loop-function termination and command errors.
        let result = result.map(|()| Value::Nil);
        let unbind =
            interp.with_lisp_stack_roots(&result, |interp| interp.unbind_to(pdlcount, env));
        result.and_then(|value| unbind.map(|()| value))
    })
}

/// macros.c:pop_kbd_macro of INFO, (MACRO INDEX . REAL-THIS-COMMAND):
/// the outer macro's state back, then `kbd-macro-termination-hook'.
pub(crate) fn pop_kbd_macro(
    interp: &mut Interpreter,
    info: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let (previous_macro, tail) = info.cons_values().expect("saved macro pair");
    let (previous_index, previous_real_this_command) =
        tail.cons_values().expect("saved macro index and command");
    interp.set_variable_key(cached_symbol!("executing-kbd-macro"), previous_macro, env);
    interp.set_variable_key(
        cached_symbol!("executing-kbd-macro-index"),
        previous_index,
        env,
    );
    interp.set_variable_key(
        cached_symbol!("real-this-command"),
        previous_real_this_command,
        env,
    );
    run_hook(
        interp,
        &cached_symbol!("kbd-macro-termination-hook").symbol(),
        env,
    )
}

pub(crate) fn next_kbd_macro_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let array = interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
        .unwrap_or(Value::Nil);
    // macros.c:at_end_of_macro_p treats t as an explicit stop. There is
    // no private event queue or cursor to keep consuming after this store.
    if array.is_nil() || array == Value::T {
        return Ok(None);
    }
    let index = interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro-index"), env)
        .unwrap_or(Value::Integer(0))
        .as_integer()?;
    if index >= sequence_length_value(&array)? {
        return Ok(None);
    }
    // keyboard.c:read_char reads the live array with Faref. Mutation,
    // replacement and Lisp assignments to the index are visible directly.
    let frame = Value::symbol("macro");
    interp.keyboard_input.internal_last_event_frame = Some(frame);
    interp.set_variable_key(cached_symbol!("last-event-frame"), frame, env);
    let mut event = super::call(interp, "aref", &[array, Value::Integer(index)], env)?;
    if array.is_string()
        && let Kind::Integer(code @ 0x80..=0xff) = event.kind()
    {
        event = Value::Integer(KEY_DESCRIPTION_META_BIT | (code & 0x7f));
    }
    interp.set_variable_key(
        cached_symbol!("executing-kbd-macro-index"),
        Value::Integer(index + 1),
        env,
    );
    interp.keyboard_input.num_input_events += 1;
    interp.keyboard_input.last_event_reread = false;
    crate::lisp::primitives::note_event_source(crate::lisp::primitives::EventSource::Macro);
    crate::lisp::primitives::note_macro_event_read();
    Ok(Some(event))
}

/// read_minibuf's arguments as its callers give them: PROMPT, the initial
/// contents, the keymap, HISTVAR (with its position), DEFALT and
/// ALLOW_PROPS.
struct ReadMinibufArgs<'a> {
    prompt: &'a Value,
    initial_value: &'a Value,
    local_map: &'a Value,
    history: Value,
    defalt: Value,
    allow_props: bool,
}

/// minibuf.c:read_minibuf past its noninteractive reader: the minibuffer
/// made ready (minibuf_save_list, the history variable and position,
/// `minibuffer-setup-hook'), its recursive edit, and its contents as a
/// string, with their text properties under ALLOW_PROPS; once the read is
/// unwound the string, or DEFALT for an empty one, goes to the history
/// through `add-to-history' unless `history-add-new-input' is nil.
fn read_minibuf(
    interp: &mut Interpreter,
    env: &mut Env,
    args: ReadMinibufArgs<'_>,
) -> Result<Value, LispError> {
    let ReadMinibufArgs {
        prompt,
        initial_value,
        local_map,
        history,
        defalt,
        allow_props,
    } = args;
    let histvar = match history.cons_values() {
        Some((variable, _)) => variable,
        None if history.is_symbol() => history,
        None => Value::Nil,
    };
    let histvar = if histvar.is_nil() {
        Value::symbol("minibuffer-history")
    } else {
        histvar
    };
    let minibuffer = crate::lisp::primitives::activate_minibuffer_with_history(
        interp,
        prompt,
        initial_value,
        *local_map,
        history,
        env,
    )?;
    let buffer_id = minibuffer.buffer_id;
    let val = run_active_minibuffer(interp, env, minibuffer, |interp, env| {
        crate::lisp::primitives::recursive_edit_1(interp, env)?;
        // Make minibuffer contents into a string: with properties when
        // the caller allowed them or `minibuffer-allow-text-properties' is
        // non-nil in the minibuffer (a setup hook's local value counts).
        if interp.has_buffer_id(buffer_id) {
            interp.set_current_buffer_id(buffer_id)?;
        }
        let allow_props = allow_props
            || interp
                .lookup_var_key(cached_symbol!("minibuffer-allow-text-properties"), env)
                .is_some_and(|value| value.is_truthy());
        let contents = if allow_props {
            "minibuffer-contents"
        } else {
            "minibuffer-contents-no-properties"
        };
        super::call(interp, contents, &[], env)
    })?;
    // Choose the string to add to the history.
    let histstring = if string_like(&val).is_some_and(|text| !text.text.is_empty()) {
        val
    } else if defalt.is_string() {
        defalt
    } else if let Some((head, _)) = defalt.cons_values()
        && head.is_string()
    {
        head
    } else {
        Value::Nil
    };
    let add = interp
        .lookup_var_key(cached_symbol!("history-add-new-input"), env)
        .is_some_and(|value| value.is_truthy());
    if add && !histstring.is_nil() {
        let roots = [val, histvar, histstring];
        interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
            interp.call_function_value(
                Value::symbol("add-to-history"),
                Some("add-to-history"),
                &[histvar, histstring],
                env,
            )
        })?;
    }
    Ok(val)
}

/// The bytes read from standard input and not yet consumed, as stdio's
/// buffer behind getchar keeps them between reads.
static BATCH_STDIN: std::sync::Mutex<std::collections::VecDeque<u8>> =
    std::sync::Mutex::new(std::collections::VecDeque::new());

/// getchar on standard input: the next byte, None at end of file or on an
/// error other than EINTR.  A wait for input runs a pending fatal signal
/// (SIGTERM, SIGHUP; SIGINT in batch) as its handler would, by
/// `kill-emacs'.
#[cfg(unix)]
fn batch_stdin_byte(interp: &mut Interpreter, env: &mut Env) -> Result<Option<u8>, LispError> {
    loop {
        if let Some(byte) = BATCH_STDIN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
        {
            return Ok(Some(byte));
        }
        if crate::lisp::primitives::fatal_signal_pending() {
            crate::lisp::primitives::run_pending_fatal_signal(interp, env)?;
        }
        // poll is never restarted after a handler, so a signal ends the
        // wait; the timeout covers one that lands just before it.
        let mut fds = libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: FDS is one valid pollfd.
        let ready = unsafe { libc::poll(&mut fds, 1, 1000) };
        if ready <= 0 {
            if ready < 0
                && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
            {
                return Ok(None);
            }
            continue;
        }
        let mut chunk = [0u8; 4096];
        // SAFETY: CHUNK is writable for its length.
        let read =
            unsafe { libc::read(libc::STDIN_FILENO, chunk.as_mut_ptr().cast(), chunk.len()) };
        match read {
            0 => return Ok(None),
            n if n < 0 => {
                if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
                    return Ok(None);
                }
            }
            n => BATCH_STDIN
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(&chunk[..n as usize]),
        }
    }
}

#[cfg(not(unix))]
fn batch_stdin_byte(_interp: &mut Interpreter, _env: &mut Env) -> Result<Option<u8>, LispError> {
    use std::io::Read;
    let mut byte = [0u8];
    Ok(match std::io::stdin().read(&mut byte) {
        Ok(1) => Some(byte[0]),
        _ => None,
    })
}

/// minibuf.c:read_minibuf_noninteractive: the prompt written to standard
/// output, then one line read a byte at a time to a newline or carriage
/// return, made a string as make_string makes one; an empty read at end of
/// file signals end-of-file.  With `read-hide-char' a character, the tty
/// (when standard input is one) stops echoing and each byte read shows as
/// that character, a newline after.
fn read_minibuffer_value_from_batch_stdin(
    interp: &mut Interpreter,
    env: &mut Env,
    prompt: &Value,
) -> Result<Value, LispError> {
    let hide_char = interp
        .lookup_var_key(cached_symbol!("read-hide-char"), env)
        .and_then(|value| match value.kind() {
            Kind::Integer(code) if (0..=0x3F_FFFF).contains(&code) => Some(code as u8),
            _ => None,
        });
    #[cfg(unix)]
    let saved_tty = hide_char.and_then(|_| {
        // SAFETY: TERMIOS is written by tcgetattr before it is read.
        unsafe {
            let mut termios: libc::termios = std::mem::zeroed();
            (libc::tcgetattr(libc::STDIN_FILENO, &mut termios) == 0).then(|| {
                let mut quiet = termios;
                quiet.c_lflag &= !(libc::ICANON | libc::ECHO);
                libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &quiet);
                termios
            })
        }
    });
    // fwrite of the prompt's internal bytes.
    let prompt_bytes = match prompt.kind() {
        Kind::StringObject(state) => state.borrow().bytes().to_vec(),
        _ => Vec::new(),
    };
    crate::lisp::primitives::batch_stdout::write(&prompt_bytes)
        .and_then(|()| crate::lisp::primitives::batch_stdout::flush())
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let mut line = Vec::new();
    let mut terminated = false;
    let read = loop {
        match batch_stdin_byte(interp, env) {
            Ok(Some(b'\n' | b'\r')) => {
                terminated = true;
                break Ok(());
            }
            Ok(Some(byte)) => {
                if let Some(hide) = hide_char {
                    let _ = crate::lisp::primitives::batch_stdout::write(&[hide]);
                }
                line.push(byte);
            }
            Ok(None) => break Ok(()),
            Err(error) => break Err(error),
        }
    };
    if hide_char.is_some() {
        let _ = crate::lisp::primitives::batch_stdout::write(b"\n");
        let _ = crate::lisp::primitives::batch_stdout::flush();
        #[cfg(unix)]
        if let Some(termios) = saved_tty {
            // SAFETY: TERMIOS is the state tcgetattr returned.
            unsafe {
                libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &termios);
            }
        }
    }
    read?;
    if line.is_empty() && !terminated {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("end-of-file".into()),
            Value::String("Error reading from stdin".into()),
        ])));
    }
    crate::lisp::primitives::make_string(line)
        .map_err(|error| string_storage_error(interp, env, error))
}

/// minibuf.c:string_to_object: an empty VAL replaced by DEFALT (or its
/// car) when that is a string, then read; anything but whitespace after
/// the expression signals invalid-read-syntax ("Trailing garbage
/// following expression").
fn string_to_object(
    interp: &mut Interpreter,
    env: &mut Env,
    val: Value,
    defalt: Value,
) -> Result<Value, LispError> {
    let mut val = val;
    if string_like(&val).is_some_and(|string| string.text.is_empty()) {
        if string_like(&defalt).is_some() {
            val = defalt;
        } else if let Some((car, _)) = defalt.cons_values()
            && string_like(&car).is_some()
        {
            val = car;
        }
    }
    let expr_and_pos = super::call(interp, "read-from-string", &[val], env)?;
    let (expr, pos) = expr_and_pos
        .cons_values()
        .unwrap_or((expr_and_pos, Value::Nil));
    if let (Kind::Integer(pos), Some(text)) = (pos.kind(), string_like(&val)) {
        let trailing = text.text.chars().skip(pos.max(0) as usize);
        if trailing
            .into_iter()
            .any(|ch| !matches!(ch, ' ' | '\t' | '\n'))
        {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("invalid-read-syntax".into()),
                Value::String("Trailing garbage following expression".into()),
            ])));
        }
    }
    Ok(expr)
}

/// read_char (0, Qnil, Qnil, 0, NULL), as read_char's help-form step
/// reads the event after the help: no end time and no input method; the
/// event recorded, made part of the current key unless reread after it,
/// `last-input-event', and a help character processed again.
fn read_char_after_help(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    let event = match pop_pending_input_event_value(interp, env)? {
        Some(event) => event,
        None => {
            crate::lisp::primitives::run_tty_frame_redraw(interp, env);
            pop_unread_command_event_value(interp, env)?
        }
    };
    crate::lisp::primitives::record_unrecorded_event(interp, &event, env);
    crate::lisp::primitives::add_read_event_to_command_keys(interp, event, env)?;
    interp.set_variable_key(cached_symbol!("last-input-event"), event, env);
    if crate::lisp::primitives::help_form_applies(interp, event, env) {
        return crate::lisp::primitives::run_help_form(interp, env, &mut read_char_after_help);
    }
    Ok(event)
}

// keyboard.c:read_char with END_TIME (None when it is past what the clock
// can hold): the first event consumed (unread, macro or terminal) before
// it, or None.
fn read_event_within(
    interp: &mut Interpreter,
    end_time: Option<std::time::Instant>,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    // read_char consumes unread and live macro events before entering
    // the timed keyboard wait.
    if let Some(event) = pop_pending_input_event_value(interp, env)? {
        return Ok(Some(event));
    }
    // read_char's redisplay when no input is pending (COMMANDFLAG 0),
    // END_TIME or not.
    crate::lisp::primitives::run_tty_frame_redraw(interp, env);
    // kbd_buffer_get_event with END_TIME: until an event comes or the
    // time is up, wait_reading_process_output (the time left, -1, 1, Qnil,
    // NULL, 0) with the keyboard in its select.
    crate::lisp::primitives::read_tty_event_with_timeout(interp, env, end_time)
}

// lread.c:read_filtered_event's conversion of an event to a character
// code: a fixnum is one, and a symbol whose `event-symbol-element-mask'
// base has an `ascii-character' is that code with the symbol's modifier
// bits; anything else is None.
fn ascii_event_code(interp: &Interpreter, event: Value) -> Option<i64> {
    if let Kind::Integer(code) = event.kind() {
        return Some(code);
    }
    let mask = interp
        .get_property_of_symbol_value(event, "event-symbol-element-mask")
        .filter(|mask| !mask.is_nil())?;
    let base = mask.car().ok()?;
    let ascii = interp.get_property_of_symbol_value(base, "ascii-character")?;
    let modifiers = mask.cdr().ok()?.car().ok()?;
    Some(ascii.as_integer().ok()? | modifiers.as_integer().ok()?)
}

// Dispatch commands from the innermost keyboard macro until its events run
// out.  `recursive-edit` re-enters this loop on the same shared cursor, so a
// command that stops in a recursive edit (like Edebug) keeps consuming the
// same macro until `exit-recursive-edit` throws back out.
// data.c Fbare_symbol: the expected-predicate slot of the signal carries
// BOTH accepted predicates -- (wrong-type-argument (symbolp
// symbol-with-pos-p) VALUE).
fn bare_symbol_type_error(value: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::list([
            Value::Symbol("symbolp".into()),
            Value::Symbol("symbol-with-pos-p".into()),
        ]),
        *value,
    ]))
}

/// keyboard.c:recursive_edit_1 on the value thrown to `exit' (by
/// `exit-recursive-edit', `abort-recursive-edit', `exit-minibuffer' and
/// minibuffer.el's `minibuffer-quit-recursive-edit'): t quits, a string is
/// an error's message, and a function is called.
pub(crate) fn recursive_edit_exit_value(
    interp: &mut Interpreter,
    env: &mut Env,
    value: Value,
) -> Result<(), LispError> {
    if matches!(value.kind(), Kind::T) {
        return Err(LispError::SignalValue(Value::list([Value::symbol("quit")])));
    }
    if value.is_string() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("error"),
            value,
        ])));
    }
    if crate::lisp::primitives::function_value_p(interp, &value, env) {
        interp.call_function_value(value, None, &[], env)?;
    }
    Ok(())
}

fn nth_list_element(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    list: &Value,
    count: &Value,
) -> Result<Value, LispError> {
    // GNU fns.c defines `nth' and list `elt' as car(nthcdr(...)).  Keep that
    // one traversal authority so negative counts, bignums, improper tails,
    // and circular lists cannot drift between the three public primitives.
    let tail = nthcdr_value(interp, env, count, list)?;
    match tail.kind() {
        Kind::Nil => Ok(Value::Nil),
        Kind::Cons(ref cell) => Ok(cell.car.get()),
        other => Err(wrong_type_argument("listp", other.value())),
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
            // ── List operations ──
            "cons" => direct_cons(interp, args, env),
            "car" => direct_car(interp, args, env),
            "cdr" => direct_cdr(interp, args, env),
            "car-safe" => direct_car_safe(interp, args, env),
            "cdr-safe" => direct_cdr_safe(interp, args, env),
            "identity" => {
                need_args(name, args, 1)?;
                Ok(args[0])
            }
            // alloc.c:Flist conses the arguments from the last back.
            "list" => Ok(Value::list_of(args)),
            "nconc" => nconc_values(args, || interp.maybe_quit(env)),
            "append" => {
                // fns.c:Fappend's concat_to_list: the elements of every
                // argument but the last in new conses as they are read, and
                // the last argument the tail as it is, even a string or a
                // vector: (append '(2) "b") => (2 . "b").
                let Some((&last_tail, leading)) = args.split_last() else {
                    return Ok(Value::Nil);
                };
                let mut head = Value::Nil;
                let mut last: Option<crate::lisp::types::SharedCons> = None;
                let mut store = |item: Value| {
                    let node = Value::cons(item, Value::Nil);
                    match &last {
                        Some(cell) => cell.cdr.set(node),
                        None => head = node,
                    }
                    last = node.cons_ref();
                };
                for a in leading {
                    if let Some(string) = sequence_string_like(a) {
                        string_sequence_values(&string)
                            .into_iter()
                            .for_each(&mut store);
                        continue;
                    }
                    // VECTORP and BOOL_VECTOR_P; a char-table is no
                    // sequence here.
                    if is_vector_value(a) || is_bool_vector_value(a) {
                        sequence_values(a)?.into_iter().for_each(&mut store);
                        continue;
                    }
                    // CLOSUREP args flatten to their slots (edebug-unwrap*
                    // rebuilds compiled closures with `(nthcdr 3 (append fn
                    // ()))').
                    if let Kind::Closure(lambda) = a.kind() {
                        lambda.slots().for_each(&mut store);
                        continue;
                    }
                    if !a.is_nil() && !a.is_cons() {
                        return Err(wrong_type_argument("sequencep", *a));
                    }
                    // FOR_EACH_TAIL from the cdr and CHECK_LIST_END.
                    a.visit_concat_list_elements(&mut store)?;
                }
                Ok(match last {
                    Some(cell) => {
                        cell.cdr.set(last_tail);
                        head
                    }
                    None => last_tail,
                })
            }
            "nth" => direct_nth(interp, args, env),
            "elt" => direct_elt(interp, args, env),
            "nthcdr" => direct_nthcdr(interp, args, env),
            "length" => direct_length(interp, args, env),
            "safe-length" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(safe_list_length(&args[0])))
            }
            "length<" | "length>" | "length=" => {
                need_args(name, args, 2)?;
                // fns.c:Flength_less, Flength_greater and Flength_equal:
                // CHECK_FIXNUM, then length_internal on a cons and Flength
                // on anything else.
                let Some(len) = args[1].fixnum() else {
                    return Err(wrong_type_argument("fixnump", args[1]));
                };
                if name == "length=" && len < 0 {
                    return Ok(Value::Nil);
                }
                let matches = if args[0].is_cons() {
                    match name {
                        "length<" => length_internal(interp, env, args[0], len)? != -1,
                        "length>" => {
                            length_internal(interp, env, args[0], len.wrapping_add(1))? == -1
                        }
                        _ => length_internal(interp, env, args[0], len.wrapping_add(1))? == 1,
                    }
                } else {
                    let length = sequence_length_value(&args[0])?;
                    match name {
                        "length<" => length < len,
                        "length>" => length > len,
                        _ => length == len,
                    }
                };
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "reverse" => {
                need_args(name, args, 1)?;
                reverse_sequence_value(interp, env, &args[0])
            }
            "copy-alist" => {
                need_args(name, args, 1)?;
                copy_alist_value(&args[0])
            }
            "memq" | "memql" | "member" => direct_member_family(interp, args, env, name),
            "assq" | "rassq" => direct_assq_family(interp, args, env, name == "assq"),
            "rassoc" => {
                need_args(name, args, 2)?;
                direct_assoc_family(interp, &[args[0], args[1], Value::Nil], env, false)
            }
            "assoc" => {
                need_arg_range(name, args, 2, 3)?;
                let testfn = args.get(2).copied().unwrap_or(Value::Nil);
                direct_assoc_family(interp, &[args[0], args[1], testfn], env, true)
            }
            "assoc-string" => {
                need_arg_range(name, args, 2, 3)?;
                assoc_string(
                    interp,
                    args[0],
                    args[1],
                    args.get(2).copied().unwrap_or(Value::Nil),
                    env,
                )
            }

            // Fast native ports of the GNU cl-seq.el sequence functions.  The
            // interpreted Lisp definitions are semantically fine but far too
            // slow for the multi-million-element sequences in
            // cl-seq-test-bug24264, so these arms carry the native-override
            // metadata consumed by function definition.
            "mapcar" => {
                need_args(name, args, 2)?;
                // fns.c's mapcar1 gathers the results in a SAFE_ALLOCA_LISP
                // array the collector reaches while the calls go on.
                let mut results = crate::lisp::alloc::RootedVec::new();
                map_sequence(interp, &args[0], &args[1], env, Some(&mut results))?;
                // mapcar's Flist (leni, args): the results stay rooted in
                // their array while the list is consed.
                Ok(Value::list_of(&results))
            }
            "mapcan" => {
                need_args(name, args, 2)?;
                let mut mapped = crate::lisp::alloc::RootedVec::new();
                map_sequence(interp, &args[0], &args[1], env, Some(&mut mapped))?;
                nconc_values(&mapped, || interp.maybe_quit(env))
            }
            "mapc" => {
                need_args(name, args, 2)?;
                map_sequence(interp, &args[0], &args[1], env, None)?;
                Ok(args[1])
            }
            "eval" => eval_impl(interp, args, env),
            "eval-buffer" => eval_buffer_impl(interp, args, env),
            "eval-region" => eval_region_impl(interp, args, env),

            "mapconcat" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // fns.c:Fmapconcat: every value mapped before Fconcat
                // validates them, as the actual Lisp sequences (projecting
                // strings through Rust text loses extended characters,
                // encoding and shared property values).
                let (function, sequence) = (args[0], args[1]);
                let length = usize::try_from(sequence_length_value(&sequence)?).unwrap_or(0);
                if matches!(sequence.kind(), Kind::CharTable(_)) {
                    return Err(LispError::WrongTypeArgument("listp".into(), sequence));
                }
                let mut mapped = crate::lisp::alloc::RootedVec::new();
                mapped.reserve(length);
                let identity = function
                    .symbol_cell()
                    .is_some_and(|symbol| cached_symbol!("identity").with(|key| symbol == *key));
                match sequence.kind() {
                    // The fast path when no function call is necessary.
                    Kind::Cons(_) if identity => {
                        let mut tail = sequence;
                        while let Some(cell) = tail.cons_ref() {
                            mapped.push(cell.car.get());
                            tail = cell.cdr.get();
                        }
                    }
                    Kind::Vector(vector) if identity => {
                        for index in 0..length {
                            mapped.push(vector.get(index).unwrap_or(Value::Nil));
                        }
                    }
                    _ => map_sequence_of(
                        interp,
                        &function,
                        &sequence,
                        length,
                        env,
                        Some(&mut mapped),
                    )?,
                }
                // A nil or empty SEPARATOR is no argument of Fconcat's.
                let separator = args.get(2).copied().unwrap_or(Value::Nil);
                let separated = !separator.is_nil()
                    && !matches!(separator.kind(), Kind::StringObject(state) if state.borrow().len() == 0);
                let mut parts = Vec::with_capacity(if separated {
                    mapped.len().saturating_mul(2).saturating_sub(1)
                } else {
                    mapped.len()
                });
                for (index, item) in mapped.iter().enumerate() {
                    if separated && index > 0 {
                        parts.push(separator);
                    }
                    parts.push(*item);
                }
                StringObjectRef::concatenate(&parts)
                    .map(Value::StringObject)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "position-symbol" => {
                need_args(name, args, 2)?;
                // data.c Fposition_symbol: SYM goes through Fbare_symbol
                // (any bare symbol passes, nil and t included; a symbol
                // with position yields its symbol), and POS is a fixnum OR
                // a symbol with position whose position is borrowed (cconv
                // repositions `ignore' from the unused variable this way).
                let bare = match args[0].kind() {
                    Kind::Symbol(_) | Kind::Nil | Kind::T => args[0],
                    other => symbol_with_pos_parts(interp, &other.value())
                        .map(|(symbol, _)| symbol)
                        .ok_or_else(|| bare_symbol_type_error(&args[0]))?,
                };
                let position = match args[1].kind() {
                    Kind::Integer(position) => position,
                    other => symbol_with_pos_parts(interp, &other.value())
                        .map(|(_, position)| position)
                        .ok_or_else(|| {
                            LispError::WrongTypeArgument(
                                "fixnum-or-symbol-with-pos-p".into(),
                                args[1],
                            )
                        })?,
                };
                Ok(Value::positioned_symbol(bare, Value::Integer(position)))
            }
            "symbol-with-pos-pos" => {
                need_args(name, args, 1)?;
                let (_, position) = symbol_with_pos_parts(interp, &args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("symbol-with-pos-p".into(), args[0])
                })?;
                Ok(Value::Integer(position))
            }
            "remove-pos-from-symbol" => {
                // data.c Fremove_pos_from_symbol: any non-symbol-with-pos
                // argument comes back unchanged, no type check.
                need_args(name, args, 1)?;
                Ok(symbol_with_pos_parts(interp, &args[0])
                    .map(|(symbol, _)| symbol)
                    .unwrap_or_else(|| args[0]))
            }
            "bare-symbol" => {
                // data.c Fbare_symbol: unlike remove-pos-from-symbol, a
                // non-symbol argument signals wrong-type-argument.
                need_args(name, args, 1)?;
                match args[0].kind() {
                    Kind::Symbol(_) | Kind::Nil | Kind::T => Ok(args[0]),
                    other => symbol_with_pos_parts(interp, &other.value())
                        .map(|(symbol, _)| symbol)
                        .ok_or_else(|| bare_symbol_type_error(&args[0])),
                }
            }
            "apply" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs("apply".into(), args.len()));
                }
                if args.len() == 1 {
                    // eval.c:Fapply treats the sole argument as the complete
                    // funcall vector: its first list element is the function
                    // and the rest are arguments.  A one-element list is a
                    // valid zero-argument call; an empty list therefore tries
                    // to funcall nil and reports void-function.
                    if is_vector_value(&args[0]) {
                        return Err(LispError::WrongTypeArgument("listp".into(), args[0]));
                    }
                    let expanded_args = args[0].to_vec()?;
                    let Some((function, call_args)) = expanded_args.split_first() else {
                        return interp.call_function_value(Value::Nil, None, &[], env);
                    };
                    return interp.call_function_value(*function, None, call_args, env);
                }
                let func = &args[0];
                let last = &args[args.len() - 1];
                // Fapply calls list_length and then walks XCAR/XCDR.  Its
                // spread argument is a proper list, not an arbitrary Emacs
                // sequence.
                if is_vector_value(last) {
                    return Err(LispError::WrongTypeArgument("listp".into(), *last));
                }
                // SAFE_ALLOCA_LISP's array: the function and eight argument
                // slots on the stack (visible to the stack scan, as
                // eval_sub's are), the heap past them.  Slot 0 holds the
                // function, so a respread call's frame vector is a prefix.
                let fixed = &args[1..args.len() - 1];
                let mut stack = [Value::Nil; 9];
                stack[0] = *func;
                let mut count = 1;
                let mut spilled: Vec<Value> = Vec::new();
                if fixed.len() < stack.len() {
                    stack[1..=fixed.len()].copy_from_slice(fixed);
                    count += fixed.len();
                } else {
                    spilled.push(*func);
                    spilled.extend_from_slice(fixed);
                }
                last.visit_list_elements(|value| {
                    if spilled.is_empty() && count < stack.len() {
                        stack[count] = value;
                        count += 1;
                    } else {
                        if spilled.is_empty() {
                            spilled.extend_from_slice(&stack[..count]);
                        }
                        spilled.push(value);
                    }
                    Ok(())
                })?;
                let total = if spilled.is_empty() {
                    count
                } else {
                    spilled.len()
                } - 1;
                let numargs = total - fixed.len();
                if numargs == 1 {
                    // `args[nargs - 1] = XCAR (spread_arg); return Ffuncall
                    // (nargs, args)': the element takes the list's slot in
                    // the vector apply's own frame records.
                    let respread: &[Value] = if spilled.is_empty() {
                        &stack[..count]
                    } else {
                        &spilled[..]
                    };
                    let changed = interp.respread_backtrace_args(args, respread);
                    let result = interp.call_function_value(*func, None, &respread[1..], env);
                    if changed {
                        interp.restore_backtrace_args(respread, args);
                    }
                    return result;
                }
                // A subr that takes more arguments than these, and no fewer
                // than its MINARGS, gets nil for each missing optional one
                // (funcall_args padded to MAXARGS).
                if numargs > 1 {
                    let fun =
                        crate::lisp::primitives::keymap::indirect_function(interp, *func, env);
                    // MAX_ARGS as recorded: a count, or UNEVALLED or MANY
                    // (negative), which no count is below.
                    let (minimum, maximum) = match fun.kind() {
                        Kind::BuiltinFunc(subr) => (
                            i64::from(subr.descriptor().min_args),
                            i64::from(subr.descriptor().max_args_word()),
                        ),
                        Kind::NativeFunction(function) => (
                            i64::from(function.min_args()),
                            i64::from(function.max_args_word()),
                        ),
                        _ => (0, -1),
                    };
                    if maximum > total as i64 && total as i64 >= minimum {
                        let padded = maximum as usize + 1;
                        if spilled.is_empty() && padded <= stack.len() {
                            count = padded;
                        } else {
                            if spilled.is_empty() {
                                spilled.extend_from_slice(&stack[..count]);
                            }
                            spilled.resize(padded, Value::Nil);
                        }
                    }
                }
                let call_args = if spilled.is_empty() {
                    &stack[1..count]
                } else {
                    &spilled[1..]
                };
                interp.call_function_value(*func, None, call_args, env)
            }
            "funcall" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs("funcall".into(), 0));
                }
                // Let the common funcall_general translation resolve the
                // original function object so resolution failures retain the
                // attempted call in the backtrace, as GNU does.
                interp.call_function_value(args[0], None, &args[1..], env)
            }
            "fset" => {
                need_args(name, args, 2)?;
                // GNU 30.2 data.c:Ffset uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                // Ffset: `NILP (symbol) && !NILP (definition)' signals.
                if args[0].is_nil() && !args[1].is_nil() {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("setting-constant"),
                        args[0],
                    ])));
                }
                interp.fset_function_symbol(&symbol, args[0], args[1], env)
            }
            "fmakunbound" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Ffmakunbound uses CHECK_SYMBOL/XSYMBOL and
                // returns its original symbol argument.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                // `NILP (symbol) || EQ (symbol, Qt)': nil as BASE_EQ, t as
                // EQ, which takes a positioned t while enabled.
                if args[0].is_nil() || lisp_symbol(interp, args[0]) == Some(Value::T) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("setting-constant"),
                        args[0],
                    ])));
                }
                // GNU voids the function cell outright; shadowed stale entries
                // (repeated defuns push duplicates) must not resurface.
                interp.set_function_binding_symbol(&symbol, None);
                Ok(args[0])
            }
            "funcall-interactively" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                // callint.c Ffuncall_interactively just funcalls its
                // arguments: the advised symbol's own backtrace frame is
                // recorded by the ordinary call path, which is the exact
                // shape nadvice's called-interactively-p skip walks
                // (lambda, apply, SYMBOL, funcall-interactively).
                interp.call_function_value(args[0], None, &args[1..], env)
            }
            "call-interactively" => call_interactively_impl(interp, args, env),

            "start-kbd-macro" => {
                need_arg_range(name, args, 1, 2)?;
                if interp
                    .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return Err(LispError::Signal("Already defining kbd macro".into()));
                }
                if args[0].is_truthy() {
                    let previous = interp
                        .lookup_var_key(cached_symbol!("last-kbd-macro"), env)
                        .unwrap_or(Value::Nil);
                    interp.kbd_macro_definition = if let Some(string) = string_like(&previous) {
                        string
                            .text
                            .chars()
                            .map(|character| Value::Integer(character as i64))
                            .collect()
                    } else if is_vector_value(&previous) {
                        vector_items(&previous)?
                    } else {
                        // macros.c: CHECK_VECTOR_OR_STRING.
                        return Err(wrong_type_argument("arrayp", previous));
                    };
                    interp.kbd_macro_committed_len = interp.kbd_macro_definition.len();
                    if !args.get(1).is_some_and(Value::is_truthy) {
                        execute_kbd_macro(interp, &[previous], env)?;
                    }
                } else {
                    interp.kbd_macro_definition.clear();
                    interp.kbd_macro_committed_len = 0;
                }
                let status = if args[0].is_truthy() {
                    "Appending to kbd macro..."
                } else {
                    "Defining kbd macro..."
                };
                super::call(interp, "message", &[Value::String(status.into())], env)?;
                interp.set_variable_key(cached_symbol!("defining-kbd-macro"), Value::T, env);
                Ok(Value::Nil)
            }
            "end-kbd-macro" => {
                need_arg_range(name, args, 0, 2)?;
                if interp
                    .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
                    .is_none_or(|value| value.is_nil())
                {
                    return Err(LispError::Signal("Not defining kbd macro".into()));
                }
                let repeat = args
                    .first()
                    .filter(|value| !value.is_nil())
                    .map(Value::as_integer)
                    .transpose()?
                    .unwrap_or(1);
                interp.set_variable_key(cached_symbol!("defining-kbd-macro"), Value::Nil, env);
                {
                    let state = &mut **interp;
                    state
                        .kbd_macro_definition
                        .truncate(state.kbd_macro_committed_len);
                }
                let last_macro = Value::vector(interp.kbd_macro_definition.iter().cloned());
                interp.set_variable_key(cached_symbol!("last-kbd-macro"), last_macro, env);
                super::call(
                    interp,
                    "message",
                    &[Value::String("Keyboard macro defined".into())],
                    env,
                )?;
                if repeat == 0 {
                    let mut execute_args = vec![last_macro, Value::Integer(0)];
                    if let Some(loop_function) = args.get(1) {
                        execute_args.push(*loop_function);
                    }
                    execute_kbd_macro(interp, &execute_args, env)?;
                } else if repeat > 1 {
                    let mut execute_args =
                        vec![last_macro, Value::Integer(repeat.saturating_sub(1))];
                    if let Some(loop_function) = args.get(1) {
                        execute_args.push(*loop_function);
                    }
                    execute_kbd_macro(interp, &execute_args, env)?;
                }
                Ok(Value::Nil)
            }
            "call-last-kbd-macro" => {
                need_arg_range(name, args, 0, 2)?;
                let macro_value = interp
                    .lookup_var_key(cached_symbol!("last-kbd-macro"), env)
                    .unwrap_or(Value::Nil);
                interp.set_variable_key(
                    cached_symbol!("this-command"),
                    interp
                        .lookup_var_key(cached_symbol!("last-command"), env)
                        .unwrap_or(Value::Nil),
                    env,
                );
                interp.set_variable_key(cached_symbol!("real-this-command"), macro_value, env);
                if interp
                    .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Can't execute anonymous macro while defining one",
                        &[],
                    ));
                }
                if macro_value.is_nil() {
                    return Err(LispError::Signal("No kbd macro has been defined".into()));
                }
                let mut execute_args = vec![macro_value];
                execute_args.extend_from_slice(args);
                execute_kbd_macro(interp, &execute_args, env)?;
                interp.set_variable_key(
                    cached_symbol!("this-command"),
                    interp
                        .lookup_var_key(cached_symbol!("last-command"), env)
                        .unwrap_or(Value::Nil),
                    env,
                );
                Ok(Value::Nil)
            }
            "execute-kbd-macro" => execute_kbd_macro(interp, args, env),
            "cancel-kbd-macro-events" => {
                need_args(name, args, 0)?;
                {
                    let state = &mut **interp;
                    state
                        .kbd_macro_definition
                        .truncate(state.kbd_macro_committed_len);
                }
                Ok(Value::Nil)
            }
            "store-kbd-macro-event" => {
                need_args(name, args, 1)?;
                if interp
                    .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
                    .is_some_and(|value| value.is_truthy())
                {
                    interp.kbd_macro_definition.push(args[0]);
                }
                Ok(Value::Nil)
            }
            "event-convert-list" => {
                need_args(name, args, 1)?;
                event_convert_list_value(interp, &args[0])
            }
            "internal-event-symbol-parse-modifiers" => {
                need_args(name, args, 1)?;
                parse_event_symbol_modifiers(interp, &args[0])
            }
            "internal--track-mouse" => {
                need_args(name, args, 1)?;
                let restore =
                    interp.bind_special_key(cached_symbol!("track-mouse"), Value::T, env)?;
                let result = call_function_value(interp, &args[0], &[], env);
                interp.restore_special_binding(restore, env)?;
                result
            }
            "internal-handle-focus-in" => {
                need_args(name, args, 1)?;
                let event = args[0].to_vec().unwrap_or_default();
                let valid = matches!(
                    event.as_slice().iter().map(|v| v.kind()).collect::<Vec<_>>().as_slice(),
                    [Kind::Symbol(kind), Kind::Frame(frame), ..]
                        if kind.is_named("focus-in") && interp.frame_is_live(*frame)
                );
                if !valid {
                    return Err(LispError::Signal("invalid focus-in event".into()));
                }
                interp.keyboard_input.internal_last_event_frame = event.get(1).cloned();
                Ok(Value::Nil)
            }
            "open-dribble-file" => {
                need_args(name, args, 1)?;
                // GNU closes the old stream before it attempts to open the new
                // one, so a failed replacement must not leave the old file live.
                interp.keyboard_input.dribble_file = None;
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                let expanded = super::call(
                    interp,
                    "expand-file-name",
                    std::slice::from_ref(&args[0]),
                    env,
                )?;
                let path = PathBuf::from(
                    string_like(&expanded)
                        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?
                        .text,
                );
                if path.exists() {
                    std::fs::remove_file(&path)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                }
                let mut options = crate::file_system::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                options
                    .open(&path)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                interp.keyboard_input.dribble_file = Some(path);
                Ok(Value::Nil)
            }
            "suspend-emacs" => {
                need_arg_range(name, args, 0, 1)?;
                // Fsuspend_emacs: not with other tty frames open, then
                // CHECK_STRING of STUFFSTRING.
                if interp.secondary_tty_terminals_live() {
                    return Err(LispError::Signal(
                        "There are other tty frames open; close them before suspending Emacs"
                            .into(),
                    ));
                }
                if let Some(stuff_string) = args.first()
                    && !stuff_string.is_nil()
                    && string_like(stuff_string).is_none()
                {
                    return Err(LispError::WrongTypeArgument(
                        "stringp".into(),
                        *stuff_string,
                    ));
                }
                run_hook(interp, &cached_symbol!("suspend-hook").symbol(), env)?;
                // A terminal session's modes are reset and the job stopped
                // (sys_suspend), STUFFSTRING and the input read ahead stuffed
                // for the shell; the modes come back, also after an error.
                // GNU's batch session has no terminal to do this on (its
                // CURTTY is null); there only the hooks run.
                if crate::lisp::primitives::tty_input_modes().is_some() {
                    let stuff = args.first().and_then(string_like).map(|string| {
                        if string.multibyte {
                            string.text.into_bytes()
                        } else {
                            string
                                .character_codes()
                                .into_iter()
                                .map(|code| code as u8)
                                .collect::<Vec<u8>>()
                        }
                    });
                    let old_size = crate::lisp::eval::terminal::controlling_tty_size();
                    crate::lisp::primitives::reset_all_sys_modes();
                    crate::lisp::primitives::stuff_buffered_input(stuff.as_deref());
                    crate::lisp::primitives::sys_suspend();
                    let interrupt_input = interp.input_interrupt_mode();
                    crate::lisp::primitives::init_all_sys_modes(interp, interrupt_input);
                    // A size the terminal took while Emacs was stopped is
                    // the selected frame's before suspend-resume-hook runs
                    // (change_frame_size, not delayed).
                    let size = crate::lisp::eval::terminal::controlling_tty_size();
                    if let Some((width, height)) = size
                        && size != old_size
                    {
                        let frame = interp.selected_frame_id;
                        crate::lisp::primitives::change_frame_size(
                            interp, frame, width, height, false, false, env,
                        )?;
                    }
                }
                run_hook(interp, &cached_symbol!("suspend-resume-hook").symbol(), env)?;
                Ok(Value::Nil)
            }
            "recursive-edit" => {
                need_args(name, args, 0)?;
                crate::lisp::primitives::recursive_edit(interp, env)
            }
            "exit-recursive-edit" | "abort-recursive-edit" => {
                need_args(name, args, 0)?;
                if interp.command_loop_recursion_depth == 0 && interp.minibuffer_depth() == 0 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("user-error"),
                        Value::string("No recursive edit is in progress"),
                    ])));
                }
                Err(LispError::Throw(
                    Value::Symbol("exit".into()),
                    if name == "abort-recursive-edit" {
                        Value::T
                    } else {
                        Value::Nil
                    },
                ))
            }
            "recursion-depth" => {
                need_args(name, args, 0)?;
                Ok(Value::Integer(
                    (interp.command_loop_recursion_depth + interp.minibuffer_depth()) as i64,
                ))
            }
            "top-level" => {
                need_args(name, args, 0)?;
                Err(LispError::Throw(
                    Value::Symbol("top-level".into()),
                    Value::Nil,
                ))
            }
            "barf-if-buffer-read-only" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c: POSITION point for nil, else CHECK_FIXNUM; an
                // `inhibit-read-only' property there lets it pass.
                let position = match args.first().map(|value| value.kind()) {
                    None | Some(Kind::Nil) => Value::Integer(interp.buffer.borrow().point() as i64),
                    Some(Kind::Integer(_)) => args[0],
                    Some(_) => {
                        return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                    }
                };
                let read_only = interp
                    .lookup_var_key(cached_symbol!("buffer-read-only"), env)
                    .is_some_and(|value| value.is_truthy());
                let inhibited = interp
                    .lookup_var_key(cached_symbol!("inhibit-read-only"), env)
                    .is_some_and(|value| value.is_truthy());
                if read_only
                    && !inhibited
                    && super::call(
                        interp,
                        "get-text-property",
                        &[position, Value::symbol("inhibit-read-only")],
                        env,
                    )?
                    .is_nil()
                {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("buffer-read-only".into()),
                        Value::Buffer(interp.buffer),
                    ])));
                }
                Ok(Value::Nil)
            }
            "this-command-keys" => {
                need_args(name, args, 0)?;
                Ok(event_array(&interp.keyboard_input.command_keys, false))
            }
            "this-command-keys-vector" => {
                need_args(name, args, 0)?;
                Ok(event_array(&interp.keyboard_input.command_keys, true))
            }
            "this-single-command-keys" => {
                need_args(name, args, 0)?;
                let start = interp
                    .keyboard_input
                    .single_command_start
                    .min(interp.keyboard_input.command_keys.len());
                Ok(event_array(
                    &interp.keyboard_input.command_keys[start..],
                    true,
                ))
            }
            "this-single-command-raw-keys" => {
                need_args(name, args, 0)?;
                Ok(event_array(&interp.keyboard_input.raw_keys, true))
            }
            "set--this-command-keys" => {
                need_args(name, args, 1)?;
                // keyboard.c:Fset__this_command_keys: KEYS's characters as
                // fetch_string_char_advance reads them (a unibyte string's
                // bytes), an eight-bit one as its byte, the first 248
                // ("\M-x") as M-x for novice.el.
                let Kind::StringObject(string) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let codes = string.borrow().codes();
                let keys = codes
                    .into_iter()
                    .enumerate()
                    .map(|(index, code)| {
                        let code = if (0x3fff80..=0x3fffff).contains(&code) {
                            code - 0x3fff00
                        } else {
                            code
                        };
                        if index == 0 && code == 248 {
                            Value::Integer(i64::from(b'x') | KEY_DESCRIPTION_META_BIT)
                        } else {
                            Value::Integer(i64::from(code))
                        }
                    })
                    .collect::<Vec<_>>();
                // The raw keys stay what read_key_sequence read.
                let raw = interp.keyboard_input.raw_keys.clone();
                set_command_key_state(interp, keys, raw, env);
                Ok(Value::Nil)
            }
            "clear-this-command-keys" => {
                need_arg_range(name, args, 0, 1)?;
                interp.keyboard_input.command_keys.clear();
                interp.keyboard_input.single_command_start = 0;
                if args.first().is_none_or(Value::is_nil) {
                    interp.keyboard_input.recent_keys.clear();
                }
                Ok(Value::Nil)
            }
            "recent-keys" => {
                need_arg_range(name, args, 0, 1)?;
                let include_commands = args.first().is_some_and(Value::is_truthy);
                Ok(event_vector(
                    interp
                        .keyboard_input
                        .recent_keys
                        .iter()
                        .filter(|event| {
                            include_commands
                                || event.cons_values().is_none_or(|(car, _)| !car.is_nil())
                        })
                        .cloned(),
                ))
            }

            "read-key-sequence" | "read-key-sequence-vector" => {
                need_arg_range(name, args, 1, 6)?;
                if !args[0].is_nil() && !args[0].is_string() {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                }
                ensure_interaction_allowed(interp, env)?;
                let events = crate::lisp::primitives::read_key_sequence_vs(
                    interp,
                    args[0],
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(2).is_some_and(Value::is_truthy),
                    args.get(3).is_some_and(Value::is_truthy),
                    args.get(4).is_some_and(Value::is_truthy),
                    env,
                )?;
                Ok(event_array(&events, name == "read-key-sequence-vector"))
            }
            "read-event" | "read-char" | "read-char-exclusive" => {
                ensure_interaction_allowed(interp, env)?;
                // lread.c: a non-nil PROMPT is shown, unlogged, as the
                // read begins.
                if let Some(prompt) = args.first().filter(|prompt| !prompt.is_nil()) {
                    crate::lisp::primitives::cancel_echoing(interp);
                    super::display::message_with_string_nolog(interp, "%s", *prompt, env)?;
                }
                let read_event = name == "read-event";
                // read_filtered_event's read_char with PREV_EVENT t unless
                // INHERIT-INPUT-METHOD: the terminal's bytes undecoded.
                let _raw = crate::lisp::primitives::RawEventRead::enter(
                    args.get(1).is_none_or(Value::is_nil),
                );
                // read_filtered_event: a number SECONDS is an end time
                // (dtotimespec added to now, None past what the clock can
                // hold) bounding the whole read, retries included; any
                // other SECONDS is none.
                let deadline = match args.get(2).map(|seconds| seconds.kind()) {
                    Some(Kind::Integer(_) | Kind::Float(_) | Kind::BigInteger(_)) => {
                        let duration = crate::lisp::primitives::numeric_to_f64(interp, &args[2])?;
                        let now = std::time::Instant::now();
                        Some(if duration > 0.0 {
                            let (secs, nsecs) =
                                crate::lisp::primitives::processes::dtotimespec(duration);
                            now.checked_add(std::time::Duration::new(secs as u64, nsecs as u32))
                        } else {
                            Some(now)
                        })
                    }
                    _ => None,
                };
                loop {
                    let event = match deadline {
                        Some(deadline) => {
                            let event = read_event_within(interp, deadline, env)?;
                            // A read with an end time adds nothing to
                            // this-command-keys.
                            interp.keyboard_input.last_event_reread = false;
                            match event {
                                Some(event) => event,
                                None => return Ok(Value::Nil),
                            }
                        }
                        None => {
                            // read_char takes an unread or keyboard-macro
                            // event at once; else it redisplays before
                            // blocking for input: window-configuration
                            // changes a command made before reading (rmc's
                            // help pop-up, y-or-n-p's prompt context) reach
                            // the glass while the read waits.
                            match pop_pending_input_event_value(interp, env)? {
                                Some(event) => event,
                                None => {
                                    crate::lisp::primitives::run_tty_frame_redraw(interp, env);
                                    pop_unread_command_event_value(interp, env)?
                                }
                            }
                        }
                    };
                    // read_filtered_event's read_char with PREV_EVENT nil
                    // and no keymap when it inherits the input method,
                    // which may give no event: read again (nothing added
                    // to this-command-keys).
                    let event = if args.get(1).is_some_and(Value::is_truthy) {
                        match crate::lisp::primitives::input_method_for_last_event(
                            interp,
                            env,
                            event,
                            crate::lisp::primitives::InputMethodRead::NoKeymap,
                        )? {
                            Some(event) => event,
                            None => continue,
                        }
                    } else {
                        event
                    };
                    // Past the input method: an unread or macro event is
                    // recorded now, the event (as the input method left
                    // it) joins this-command-keys unless the read had an
                    // end time, and it is `last-input-event'.
                    crate::lisp::primitives::record_unrecorded_event(interp, &event, env);
                    if deadline.is_none() {
                        crate::lisp::primitives::add_read_event_to_command_keys(
                            interp, event, env,
                        )?;
                    }
                    interp.set_variable_key(cached_symbol!("last-input-event"), event, env);
                    // read_char's help character: `help-form' shown, the
                    // event read after it the read's.
                    let event = if crate::lisp::primitives::help_form_applies(interp, event, env) {
                        crate::lisp::primitives::run_help_form(
                            interp,
                            env,
                            &mut read_char_after_help,
                        )?
                    } else {
                        event
                    };
                    if read_event {
                        return normalize_input_event_value(event);
                    }
                    // read_filtered_event's ASCII requirement: a symbol
                    // whose base has an `ascii-character' is that
                    // character with the symbol's modifiers; another
                    // non-character is pushed back as the only unread
                    // event and refused by `read-char', skipped by
                    // `read-char-exclusive'.
                    match ascii_event_code(interp, event) {
                        Some(code) => {
                            return Ok(Value::Integer(
                                crate::lisp::primitives::ccl::char_resolve_modifier_mask(code),
                            ));
                        }
                        None if name == "read-char" => {
                            interp.set_variable_key(
                                cached_symbol!("unread-command-events"),
                                Value::list([event]),
                                env,
                            );
                            return Err(LispError::Signal("Non-character input-event".into()));
                        }
                        None => {}
                    }
                }
            }
            "read-string" | "read-from-minibuffer" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                ensure_interaction_allowed(interp, env)?;
                let initial_value = args.get(1).cloned().unwrap_or(Value::Nil);
                let prompt = args[0];
                if string_like(&prompt).is_none() {
                    return Err(wrong_type_argument("stringp", prompt));
                }
                // Fread_from_minibuffer's KEYMAP: `minibuffer-local-map'
                // when nil, else get_keymap (KEYMAP, 1, 0).
                let local_map = match args.get(2).filter(|_| name == "read-from-minibuffer") {
                    Some(map) if !map.is_nil() => {
                        crate::lisp::primitives::keymap::get_keymap(interp, *map, true, false, env)?
                    }
                    _ => interp
                        .lookup_var_key(cached_symbol!("minibuffer-local-map"), env)
                        .unwrap_or(Value::Nil),
                };
                // read_minibuf checks INITIAL, a string or (STRING .
                // POSITION), before the recursive-read guard.
                let (initial_string, _) =
                    crate::lisp::primitives::minibuffer_initial_input(&initial_value)?;
                let initial = string_like(&initial_string)
                    .map(|string| string.text)
                    .unwrap_or_default();
                crate::lisp::primitives::check_recursive_minibuffer(interp, env)?;
                // HIST and DEFAULT sit at different positions per reader:
                // read-from-minibuffer's KEYMAP and READ shift them to
                // args 4 and 5, read-string keeps GNU's 2 and 3.
                let (history, default) = match name {
                    "read-from-minibuffer" => (
                        args.get(4).cloned().unwrap_or(Value::Nil),
                        args.get(5).cloned().unwrap_or(Value::Nil),
                    ),
                    _ => (
                        args.get(2).cloned().unwrap_or(Value::Nil),
                        args.get(3).cloned().unwrap_or(Value::Nil),
                    ),
                };
                // read_minibuf's entry: `minibuffer-default' is DEFAULT and
                // `inhibit-read-only' nil for the read, and
                // `minibuffer-completing-file-name' is bound, a `lambda' a
                // previous recursive minibuffer left (t there, not set
                // again for this read) nil in this one.
                let count = interp.specpdl_index();
                // Fread_string: the previous minibuffer's completion table
                // does not apply to the new one.
                if name == "read-string" {
                    interp.specbind_symbol(
                        &"minibuffer-completion-table".into(),
                        Value::Nil,
                        env,
                    )?;
                }
                interp.specbind_symbol(&"minibuffer-default".into(), default, env)?;
                interp.specbind_symbol(&"inhibit-read-only".into(), Value::Nil, env)?;
                let completing_file_name = interp
                    .lookup_var_key(cached_symbol!("minibuffer-completing-file-name"), env)
                    .unwrap_or(Value::Nil);
                interp.specbind_symbol(
                    &"minibuffer-completing-file-name".into(),
                    completing_file_name,
                    env,
                )?;
                if crate::lisp::types::eq(completing_file_name, Value::symbol("lambda")) {
                    interp.set_variable_key(
                        cached_symbol!("minibuffer-completing-file-name"),
                        Value::Nil,
                        env,
                    );
                }
                let read = (|| {
                    // minibuf.c read_minibuf takes read_minibuf_noninteractive
                    // (stdin, no history) only while `noninteractive' and no
                    // keyboard macro executes; it does not look at
                    // `unread-command-events'.  Every other read goes through
                    // the recursive command loop, whose read_char consumes
                    // unread events first and then the macro.
                    let batch_stdin = interp
                        .lookup_var_key(cached_symbol!("noninteractive"), env)
                        .is_some_and(|value| value.is_truthy())
                        && interp
                            .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
                            .is_none_or(|value| value.is_nil());
                    if batch_stdin {
                        // read_minibuf_noninteractive returns before any
                        // minibuffer is chosen: no buffer, no setup or exit hook,
                        // no history.
                        let value = read_minibuffer_value_from_batch_stdin(interp, env, &prompt)?;
                        if name == "read-from-minibuffer"
                            && args.get(3).is_some_and(Value::is_truthy)
                        {
                            return string_to_object(interp, env, value, default);
                        }
                        if name == "read-string"
                            && string_like(&value).is_some_and(|string| string.text.is_empty())
                            && !default.is_nil()
                        {
                            return Ok(default.cons_values().map_or(default, |(head, _)| head));
                        }
                        return Ok(value);
                    }
                    // INHERIT-INPUT-METHOD: read-from-minibuffer's seventh
                    // argument, read-string's fifth.
                    let inherit = args
                        .get(if name == "read-from-minibuffer" { 6 } else { 4 })
                        .is_some_and(Value::is_truthy);
                    crate::lisp::primitives::inherit_minibuffer_input_method(interp, env, inherit);
                    let allow_props = interp
                        .forwarded_c_value("minibuffer-allow-text-properties", &Env::new())
                        .is_some_and(|value| value.is_truthy());
                    let val = read_minibuf(
                        interp,
                        env,
                        ReadMinibufArgs {
                            prompt: &prompt,
                            initial_value: &initial_value,
                            local_map: &local_map,
                            history,
                            defalt: default,
                            allow_props,
                        },
                    )?;
                    // If Lisp form desired instead of string, parse it.
                    if name == "read-from-minibuffer" && args.get(3).is_some_and(Value::is_truthy) {
                        return string_to_object(interp, env, val, default);
                    }
                    // Fread_string: an empty answer is DEFAULT-VALUE, the first
                    // of a list.
                    if name == "read-string"
                        && string_like(&val).is_some_and(|string| string.text.is_empty())
                        && !default.is_nil()
                    {
                        return Ok(default.cons_values().map_or(default, |(head, _)| head));
                    }
                    Ok(val)
                })();
                let unbound = interp.unbind_to(count, env);
                let value = read?;
                unbound.map(|()| value)
            }
            "completing-read" => completing_read(interp, args, env),
            "read-buffer" => {
                need_arg_range(name, args, 1, 4)?;
                // minibuf.c:Fread_buffer.  A buffer DEF is its name.
                let mut def = args.get(1).copied().unwrap_or(Value::Nil);
                if let Kind::Buffer(buffer) = def.kind() {
                    def = buffer.borrow().name();
                }
                let require_match = args.get(2).copied().unwrap_or(Value::Nil);
                let predicate = args.get(3).copied().unwrap_or(Value::Nil);
                let count = interp.specpdl_index();
                let ignore_case = interp
                    .lookup_var_key(cached_symbol!("read-buffer-completion-ignore-case"), env)
                    .is_some_and(|value| value.is_truthy());
                interp.specbind_symbol(
                    &"completion-ignore-case".into(),
                    if ignore_case { Value::T } else { Value::Nil },
                    env,
                )?;
                let roots = [args[0], def, require_match, predicate];
                let result = interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
                    let function = interp
                        .lookup_var_key(cached_symbol!("read-buffer-function"), env)
                        .unwrap_or(Value::Nil);
                    if !function.is_nil() {
                        // Partial backward compatibility for older
                        // read_buffer_functions which don't expect a
                        // `predicate' argument.
                        let call_args: &[Value] = if predicate.is_nil() {
                            &[args[0], def, require_match]
                        } else {
                            &[args[0], def, require_match, predicate]
                        };
                        return call_function_value(interp, &function, call_args, env);
                    }
                    let mut prompt = args[0];
                    if !def.is_nil() {
                        // The default edited in before the colon: PROMPT
                        // without its terminal ": ", ":" or " ", then
                        // `format-prompt'.
                        // make_specified_string of the bytes kept: the
                        // prompt's multibyteness, none of its properties.
                        if let Some(text) = string_like(&prompt) {
                            let chars = text.text.chars().count() as i64;
                            let cut = if text.text.ends_with(": ") {
                                2
                            } else if text.text.ends_with(':') || text.text.ends_with(' ') {
                                1
                            } else {
                                0
                            };
                            prompt = super::call(
                                interp,
                                "substring-no-properties",
                                &[prompt, Value::Integer(0), Value::Integer(chars - cut)],
                                env,
                            )?;
                        }
                        let shown = def.cons_values().map_or(def, |(head, _)| head);
                        prompt = interp.with_lisp_stack_roots(&prompt, |interp| {
                            call_function_value(
                                interp,
                                &Value::symbol("format-prompt"),
                                &[prompt, shown],
                                env,
                            )
                        })?;
                    }
                    completing_read(
                        interp,
                        &[
                            prompt,
                            Value::symbol("internal-complete-buffer"),
                            predicate,
                            require_match,
                            Value::Nil,
                            Value::symbol("buffer-name-history"),
                            def,
                            Value::Nil,
                        ],
                        env,
                    )
                });
                let unbound = interp.unbind_to(count, env);
                let value = result?;
                unbound.map(|()| value)
            }
            "read-command" | "read-variable" => {
                need_arg_range(name, args, 1, 2)?;
                // minibuf.c:Fread_command, Fread_variable: a symbol
                // DEFAULT-VALUE's SYMBOL_NAME, then Fintern of the name
                // read in `obarray'.
                let default = args.get(1).cloned().unwrap_or(Value::Nil);
                let default = if !default.is_nil() && default.is_symbol() {
                    super::call(interp, "symbol-name", &[default], env)?
                } else {
                    default
                };
                let obarray = interp
                    .lookup_var_key(cached_symbol!("obarray"), env)
                    .unwrap_or(Value::Nil);
                let predicate = if name == "read-command" {
                    Value::Symbol("commandp".into())
                } else {
                    Value::Symbol("custom-variable-p".into())
                };
                let history = if name == "read-variable" {
                    Value::Symbol("custom-variable-history".into())
                } else {
                    Value::Nil
                };
                let value = completing_read(
                    interp,
                    &[
                        args[0],
                        obarray,
                        predicate,
                        Value::T,
                        Value::Nil,
                        history,
                        default,
                        Value::Nil,
                    ],
                    env,
                )?;
                if value.is_nil() {
                    Ok(Value::Nil)
                } else {
                    super::call(interp, "intern", &[value], env)
                }
            } // GNU byte-run.el defines this as an ordinary &rest function.
              // Keeping it on the callable dispatch route makes interpreted
              // and byte-compiled calls share one function-cell contract.
        }
    }
);

/// The `car-safe' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_car_safe(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "car-safe";
    need_args(name, args, 1)?;
    Ok(match args[0].kind() {
        Kind::Cons(cell) => cell.car.get(),
        _ => Value::Nil,
    })
}

/// The `cdr-safe' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_cdr_safe(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "cdr-safe";
    need_args(name, args, 1)?;
    Ok(match args[0].kind() {
        Kind::Cons(cell) => cell.cdr.get(),
        _ => Value::Nil,
    })
}

/// The `nth' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_nth(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "nth";
    need_args(name, args, 2)?;
    nth_list_element(interp, env, &args[1], &args[0])
}

/// The `nthcdr' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_nthcdr(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "nthcdr";
    need_args(name, args, 2)?;
    nthcdr_value(interp, env, &args[0], &args[1])
}

/// The `elt' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_elt(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "elt";
    need_args(name, args, 2)?;
    if matches!(args[0].kind(), Kind::Nil | Kind::Cons(_)) {
        nth_list_element(interp, env, &args[0], &args[1])
    } else if matches!(
        args[0].kind(),
        Kind::Vector(_) | Kind::StringObject(_) | Kind::CharTable(_) | Kind::BoolVector(_)
    ) {
        // fns.c:Felt: CHECK_ARRAY (SEQUENCE, Qsequencep) before Faref.
        super::call(interp, "aref", args, env)
    } else {
        Err(wrong_type_argument("sequencep", args[0]))
    }
}

/// The `length' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_length(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "length";
    need_args(name, args, 1)?;
    Ok(Value::Integer(sequence_length_value(&args[0])?))
}

/// `memq', `memql' and `member' by NAME (fns.c's three walks share this body).
pub(super) fn direct_member_family(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
    name: &str,
) -> Result<Value, LispError> {
    need_args(name, args, 2)?;
    #[derive(Clone, Copy)]
    enum MemTest {
        Equal,
        // An ELT (A . B) of two fixnums or bare symbols, whose `equal'
        // elements are the conses of an A and a B they are EQ to.
        AtomPair(Value, Value),
        Eql,
        Eq,
    }
    let atom = |value: Value| value.is_fixnum() || value.is_symbol();
    let test = match name {
        // Fmember: an eq-comparable ELT is Fmemq's.
        "member" if eq_comparable_value(interp, &args[0], env) => MemTest::Eq,
        "member" => match args[0].cons_ref() {
            Some(pair) if atom(pair.car.get()) && atom(pair.cdr.get()) => {
                MemTest::AtomPair(pair.car.get(), pair.cdr.get())
            }
            _ => MemTest::Equal,
        },
        "memql" => MemTest::Eql,
        _ => MemTest::Eq,
    };
    let mut tail = args[1];
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        let item = cell.car.get();
        let matches = match test {
            MemTest::Equal => values_equal(interp, &item, &args[0])?,
            // internal_equal of a cons and anything else is false; of two
            // conses, the cars' and then the cdrs', each EQ for atoms like
            // these, unless a part of ITEM is vectorlike (a positioned
            // symbol, a bignum), which the full walk compares.
            MemTest::AtomPair(car, cdr) => match item.cons_ref() {
                None => false,
                Some(pair) => {
                    let (left, right) = (pair.car.get(), pair.cdr.get());
                    if left.is_vectorlike() || right.is_vectorlike() {
                        values_equal(interp, &item, &args[0])?
                    } else {
                        left.eq_value(car) && right.eq_value(cdr)
                    }
                }
            },
            MemTest::Eql => values_eql(&item, &args[0]),
            MemTest::Eq => values_eq_in_env(interp, &item, &args[0], env),
        };
        if matches {
            return Ok(tail);
        }
        tail = cell.cdr.get();
        if walk.cycled(tail, || interp.maybe_quit(env))? {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    // CHECK_LIST_END(tail, list) reports the original list. An improper
    // tail is never an element, even when it equals the sought value.
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", args[1]));
    }
    Ok(Value::Nil)
}

/// The `memq' primitive, callable directly (fns.c:Fmemq).
pub(in crate::lisp::primitives) fn direct_memq(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    direct_member_family(interp, args, env, "memq")
}

pub(super) fn direct_memql(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    direct_member_family(interp, args, env, "memql")
}

pub(super) fn direct_member(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    direct_member_family(interp, args, env, "member")
}

/// fns.c:Fassq and Frassq by NAME: FOR_EACH_TAIL over ALIST for an
/// element that is a cons whose car (or cdr) is EQ to KEY, then
/// CHECK_LIST_END, which reports ALIST itself.
pub(super) fn direct_assq_family(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
    want_car: bool,
) -> Result<Value, LispError> {
    need_args(if want_car { "assq" } else { "rassq" }, args, 2)?;
    let key = args[0];
    let mut tail = args[1];
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cons) = tail.cons_ref() {
        // The cell once for its car and its cdr, as XCAR and XCDR read
        // one object.
        let cell: &crate::lisp::types::ConsCell = &cons;
        let item = cell.car.get();
        if let Some(entry) = item.cons_ref() {
            let entry_key = if want_car {
                entry.car.get()
            } else {
                entry.cdr.get()
            };
            if values_eq_in_env(interp, &entry_key, &key, env) {
                return Ok(item);
            }
        }
        tail = cell.cdr.get();
        if walk.cycled(tail, || interp.maybe_quit(env))? {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", args[1]));
    }
    Ok(Value::Nil)
}

pub(super) fn direct_assq(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    direct_assq_family(interp, args, env, true)
}

pub(super) fn direct_rassq(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    direct_assq_family(interp, args, env, false)
}

/// fns.c:eq_comparable_value: SYMBOLP or FIXNUMP, whose `equal' is `eq'.
fn eq_comparable_value(interp: &Interpreter, value: &Value, env: &crate::lisp::types::Env) -> bool {
    value.fixnum().is_some()
        || matches!(value.kind(), Kind::Nil | Kind::T | Kind::Symbol(_))
        || (matches!(value.kind(), Kind::SymbolWithPos(_))
            && crate::lisp::primitives::symbols_with_pos_enabled(interp, env))
}

/// fns.c:Fassoc (CAR) and Frassoc: ARGS are KEY, ALIST and TESTFN
/// (always nil for `rassoc').  An `eq'-comparable KEY with no TESTFN is
/// Fassq's (Frassq's) walk; otherwise FOR_EACH_TAIL tests each cons
/// element's car (cdr) by EQ or Fequal, or calls TESTFN with it and KEY.
fn direct_assoc_family(
    interp: &mut Interpreter,
    args: &[Value; 3],
    env: &mut crate::lisp::types::Env,
    car: bool,
) -> Result<Value, LispError> {
    let [key, alist, testfn] = *args;
    if testfn.is_nil() && eq_comparable_value(interp, &key, env) {
        return direct_assq_family(interp, &[key, alist], env, car);
    }
    let mut tail = alist;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        let item = cell.car.get();
        if let Some(entry) = item.cons_ref() {
            let entry_key = if car {
                entry.car.get()
            } else {
                entry.cdr.get()
            };
            let matched = if testfn.is_nil() {
                values_eq_in_env(interp, &entry_key, &key, env)
                    || values_equal(interp, &entry_key, &key)?
            } else {
                call_function_value(interp, &testfn, &[entry_key, key], env)?.is_truthy()
            };
            if matched {
                return Ok(item);
            }
        }
        tail = cell.cdr.get();
        if walk.cycled(tail, || interp.maybe_quit(env))? {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", alist));
    }
    Ok(Value::Nil)
}

/// fns.c:length_internal: LEN (an `int' there) less the conses of
/// SEQUENCE, or -1 once that reaches zero; a short LEN walks without
/// looking for a cycle, a long one with FOR_EACH_TAIL.
fn length_internal(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    sequence: Value,
    len: i64,
) -> Result<i64, LispError> {
    let mut len = len as i32;
    let mut tail = sequence;
    if len < 0xffff {
        while let Some(cell) = tail.cons_ref() {
            len = len.wrapping_sub(1);
            if len <= 0 {
                return Ok(-1);
            }
            tail = cell.cdr.get();
        }
    } else {
        let mut walk = crate::lisp::types::TailWalk::new(tail);
        while let Some(cell) = tail.cons_ref() {
            len = len.wrapping_sub(1);
            if len <= 0 {
                return Ok(-1);
            }
            tail = cell.cdr.get();
            if walk.cycled(tail, || interp.maybe_quit(env))? {
                return Err(crate::lisp::types::circular_list(tail));
            }
        }
    }
    Ok(i64::from(len))
}

/// The subr behind `cons', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_cons(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "cons";
    need_args(name, args, 2)?;
    Ok(Value::cons(args[0], args[1]))
}

/// The subr behind `car', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_car(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "car";
    need_args(name, args, 1)?;
    args[0]
        .car()
        .map_err(|_| wrong_type_argument("listp", args[0]))
}

/// The subr behind `cdr', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_cdr(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "cdr";
    need_args(name, args, 1)?;
    args[0]
        .cdr()
        .map_err(|_| wrong_type_argument("listp", args[0]))
}

/// fns.c's mapping entry and mapcar1: SEQUENCE's length as Flength counts
/// it (its errors first), `listp' for a char-table, then FUNCTION on each
/// element, read only when FUNCTION is called on it. A function that
/// changes the sequence therefore sees its changes, and a list it makes
/// shorter ends the mapping early. RESULTS, when given, gathers the values.
fn map_sequence(
    interp: &mut Interpreter,
    function: &Value,
    sequence: &Value,
    env: &mut crate::lisp::types::Env,
    results: Option<&mut crate::lisp::alloc::RootedVec<Value>>,
) -> Result<(), LispError> {
    // Fmapcar's `XFIXNAT (Flength (sequence))'.
    let length = usize::try_from(sequence_length_value(sequence)?).unwrap_or(0);
    map_sequence_of(interp, function, sequence, length, env, results)
}

/// fns.c:mapcar1: FUNCTION called on each of SEQUENCE's LENGTH elements,
/// its values pushed onto RESULTS when there are any.
fn map_sequence_of(
    interp: &mut Interpreter,
    function: &Value,
    sequence: &Value,
    length: usize,
    env: &mut crate::lisp::types::Env,
    mut results: Option<&mut crate::lisp::alloc::RootedVec<Value>>,
) -> Result<(), LispError> {
    // mapcar1's SAFE_ALLOCA_LISP array: the results' room is taken once.
    if let Some(results) = results.as_deref_mut() {
        results.reserve(length);
    }
    let mut call = |interp: &mut Interpreter, item: Value, env: &mut _| -> Result<(), LispError> {
        let value = call_function_value(interp, function, &[item], env)?;
        if let Some(results) = results.as_deref_mut() {
            results.push(value);
        }
        Ok(())
    };
    match sequence.kind() {
        Kind::Nil => {}
        Kind::CharTable(_) => {
            return Err(LispError::WrongTypeArgument("listp".into(), *sequence));
        }
        Kind::Cons(_) => {
            let mut tail = *sequence;
            for _ in 0..length {
                let Some(cell) = tail.cons_ref() else {
                    break;
                };
                call(interp, cell.car.get(), env)?;
                tail = cell.cdr.get();
            }
        }
        Kind::Vector(vector) => {
            for index in 0..length {
                call(interp, vector.get(index).unwrap_or(Value::Nil), env)?;
            }
        }
        Kind::Closure(closure) => {
            for index in 0..length {
                call(interp, closure.get(index).unwrap_or(Value::Nil), env)?;
            }
        }
        Kind::StringObject(string) => {
            let mut offset = 0;
            for _ in 0..length {
                let Some(code) = string.borrow().character_advance(&mut offset) else {
                    break;
                };
                call(interp, Value::Integer(code), env)?;
            }
        }
        Kind::BoolVector(vector) => {
            // mapcar1 reads each bit (bool_vector_ref) when its call comes.
            for index in 0..length {
                let item = if vector.get(index) == Some(true) {
                    Value::T
                } else {
                    Value::Nil
                };
                call(interp, item, env)?;
            }
        }
        _ => return Err(LispError::WrongTypeArgument("vectorp".into(), *sequence)),
    }
    Ok(())
}

/// minibuf.c:Fassoc_string: the first element of LIST whose car, or which
/// itself, is a string or a symbol `compare-strings' finds equal to KEY
/// over their whole lengths, upcasing a differing pair of characters when
/// CASE-FOLD is non-nil.  Other elements are passed over, and the walk ends
/// at the first tail that is not a cons, as `CONSP (tail)' ends GNU's.
fn assoc_string(
    interp: &mut Interpreter,
    key: Value,
    list: Value,
    case_fold: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let key = symbol_name_of(interp, &key, env).unwrap_or(key);
    let mut tail = list;
    while let Kind::Cons(cell) = tail.kind() {
        let elt = cell.car.get();
        let thiscar = match elt.kind() {
            Kind::Cons(inner) => inner.car.get(),
            _ => elt,
        };
        let thiscar = match symbol_name_of(interp, &thiscar, env) {
            Some(name) => name,
            None if matches!(thiscar.kind(), Kind::StringObject(_)) => thiscar,
            None => {
                tail = cell.cdr.get();
                continue;
            }
        };
        let tem = crate::lisp::primitives::strings::compare_strings_value(
            interp,
            &[
                thiscar,
                Value::Integer(0),
                Value::Nil,
                key,
                Value::Integer(0),
                Value::Nil,
                case_fold,
            ],
            env,
        )?;
        if matches!(tem.kind(), Kind::T) {
            return Ok(elt);
        }
        interp.maybe_quit(env)?;
        tail = cell.cdr.get();
    }
    Ok(Value::Nil)
}

/// `SYMBOLP (OBJECT) ? Fsymbol_name (OBJECT)': a symbol's name, a
/// symbol-with-position's while `symbols-with-pos-enabled'; None for any
/// other object.
fn symbol_name_of(interp: &Interpreter, object: &Value, env: &Env) -> Option<Value> {
    crate::lisp::primitives::interactive::checked_symbol_identity(interp, object, env)
        .ok()
        .map(|symbol| symbol.lisp_name())
}
