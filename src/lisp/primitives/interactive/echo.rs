//! keyboard.c's echoing of a key sequence in the echo area: the kboard's
//! echo_string (the keys read so far, after its echo_prompt) and
//! immediate_echo, with keyboard.c's echoing, ok_to_echo_at_next_pause,
//! echo_kboard and echo_message_buffer; echo_now shows the string with
//! message3_nolog, so it is the echo area's message (`current-message')
//! like any other.
//!
//! Emaxx has one kboard: ok_to_echo_at_next_pause and echo_kboard are
//! whether they point to it.  echo_message_buffer, which every echo area
//! display sets back to nil, is not kept.

use super::*;

/// keyboard.c:echo_keystrokes_p: `echo-keystrokes' a positive number.
pub(crate) fn echo_keystrokes_p(interp: &Interpreter, env: &Env) -> bool {
    match interp
        .lookup_var_key(cached_symbol!("echo-keystrokes"), env)
        .map(|value| value.kind())
    {
        Some(Kind::Float(seconds)) => seconds.get() > 0.0,
        Some(Kind::Integer(seconds)) => seconds > 0,
        _ => false,
    }
}

fn echo_string(interp: &Interpreter) -> Value {
    interp.keyboard_input.echo_string.unwrap_or(Value::Nil)
}

fn set_echo_string(interp: &mut Interpreter, value: Value) {
    interp.keyboard_input.echo_string = (!value.is_nil()).then_some(value);
}

/// kset_echo_prompt.
pub(crate) fn set_echo_prompt(interp: &mut Interpreter, value: Value) {
    interp.keyboard_input.echo_prompt = (!value.is_nil()).then_some(value);
}

fn echo_prompt(interp: &Interpreter) -> Value {
    interp.keyboard_input.echo_prompt.unwrap_or(Value::Nil)
}

fn schars(value: &Value) -> usize {
    crate::lisp::primitives::string_text(value).map_or(0, |text| text.chars().count())
}

fn concat2(
    interp: &mut Interpreter,
    left: Value,
    right: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    call(interp, "concat", &[left, right], env)
}

/// help_char_p: C is `help-char' or one of `help-event-list'.
pub(super) fn help_char_p(interp: &Interpreter, c: Value, env: &Env) -> bool {
    if interp
        .lookup_var_key(cached_symbol!("help-char"), env)
        .is_some_and(|help| crate::lisp::types::eq(help, c))
    {
        return true;
    }
    let mut tail = interp
        .lookup_var_key(cached_symbol!("help-event-list"), env)
        .unwrap_or(Value::Nil);
    while let Some((event, rest)) = tail.cons_values() {
        if crate::lisp::types::eq(event, c) {
            return true;
        }
        tail = rest;
    }
    false
}

/// keyboard.c:echo_add_key: C (a composite event's head) added to the
/// echo string after a space, a character by its key description, a
/// symbol by its name; the first key, when it is a help character,
/// followed by " (Type ? for further options, C-q for quick help)".
fn echo_add_key(interp: &mut Interpreter, c: Value, env: &mut Env) -> Result<(), LispError> {
    let echo = echo_string(interp);
    let mut bytes = Vec::new();
    if echo.is_string() && schars(&echo) > 0 {
        bytes.push(b' ');
    }
    // EVENT_HEAD.
    let c = c.cons_values().map_or(c, |(head, _)| head);
    let mut multibyte_name = None;
    match c.kind() {
        Kind::Integer(code) => crate::lisp::primitives::push_key_description(code, &mut bytes),
        Kind::Nil | Kind::T | Kind::Symbol(_) => {
            multibyte_name = Some(call(interp, "symbol-name", &[c], env)?);
        }
        _ => {}
    }
    let mut new_string = crate::lisp::primitives::strings::make_string(bytes)
        .map_err(|_| LispError::Signal("Memory exhausted".into()))?;
    if let Some(name) = multibyte_name {
        new_string = concat2(interp, new_string, name, env)?;
    }
    if (echo.is_nil() || schars(&echo) == 0) && help_char_p(interp, c, env) {
        let help = Value::string(" (Type ? for further options, C-q for quick help)");
        let props = Value::list([Value::symbol("face"), Value::symbol("help-key-binding")]);
        call(
            interp,
            "add-text-properties",
            &[Value::Integer(7), Value::Integer(8), props, help],
            env,
        )?;
        call(
            interp,
            "add-text-properties",
            &[Value::Integer(30), Value::Integer(33), props, help],
            env,
        )?;
        new_string = concat2(interp, new_string, help, env)?;
    }
    let combined = concat2(interp, echo, new_string, env)?;
    set_echo_string(interp, combined);
    Ok(())
}

/// keyboard.c:echo_dash: a dash after a non-empty echo string, as a
/// mini-prompt for the next key, unless the string is just the prompt or
/// already ends in a dash (or the keystroke help); `echo-keystrokes-help'
/// adds help.el's hint; then echo_now.
pub(crate) fn echo_dash(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let echo = echo_string(interp);
    if echo.is_nil() {
        return Ok(());
    }
    if !interp.keyboard_input.immediate_echo && schars(&echo) == 0 {
        return Ok(());
    }
    let prompt = echo_prompt(interp);
    if prompt.is_string() && schars(&prompt) == schars(&echo) {
        return Ok(());
    }
    let help = interp
        .lookup_var_key(cached_symbol!("echo-keystrokes-help"), env)
        .is_some_and(|value| value.is_truthy());
    let chars: Vec<char> = crate::lisp::primitives::string_text(&echo)
        .unwrap_or_default()
        .chars()
        .collect();
    if chars.len() > 1 {
        let (prev, last) = (chars[chars.len() - 2], chars[chars.len() - 1]);
        if (last == '-' && prev != ' ') || (help && last == ')' && prev == 'p') {
            return Ok(());
        }
    }
    let dashed = concat2(interp, echo, Value::string("-"), env)?;
    set_echo_string(interp, dashed);
    if help {
        let appended = interp.call_function_value(
            Value::symbol("help--append-keystrokes-help"),
            Some("help--append-keystrokes-help"),
            &[dashed],
            env,
        )?;
        set_echo_string(interp, appended);
    }
    echo_now(interp, env)
}

/// keyboard.c:echo_update: while echoing at once, the echo string made
/// again from the prompt, `internal-echo-keystrokes-prefix' and every key
/// of this-command-keys but mouse movements, then echo_now.
pub(crate) fn echo_update(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if !interp.keyboard_input.immediate_echo {
        return Ok(());
    }
    let prompt = echo_prompt(interp);
    let prefix = interp.call_function_value(
        Value::symbol("internal-echo-keystrokes-prefix"),
        Some("internal-echo-keystrokes-prefix"),
        &[],
        env,
    )?;
    let start = if prompt.is_nil() {
        prefix
    } else if prefix.is_nil() {
        prompt
    } else {
        concat2(interp, prompt, prefix, env)?
    };
    set_echo_string(interp, start);
    let keys = interp.keyboard_input.command_keys.clone();
    for c in keys {
        if !mouse_movement_p(interp, &c) {
            echo_add_key(interp, c, env)?;
        }
    }
    echo_now(interp, env)
}

/// EVENT_HAS_PARAMETERS (C) && EQ (EVENT_HEAD_KIND (EVENT_HEAD (C)),
/// Qmouse_movement).
pub(super) fn mouse_movement_p(interp: &Interpreter, c: &Value) -> bool {
    let Some((head, _)) = c.cons_values() else {
        return false;
    };
    interp
        .get_property_of_symbol_value(head, "event-kind")
        .is_some_and(|kind| crate::lisp::types::eq_symbol_named(kind, "mouse-movement"))
}

/// keyboard.c:echo_now: echoing at once from now (echo_update and
/// echo_dash the first time), the echo string shown by message3_nolog,
/// its message and kboard recorded, and a `quit-flag' set while waiting
/// for input thrown to read_char.
pub(crate) fn echo_now(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if !interp.keyboard_input.immediate_echo {
        interp.keyboard_input.immediate_echo = true;
        echo_update(interp, env)?;
        echo_dash(interp, env)?;
    }
    interp.keyboard_input.echoing = true;
    // message3_nolog: an empty or nil echo string clears the echo area.
    let echo = echo_string(interp);
    let shown = crate::lisp::primitives::dispatch::display::message3_nolog(interp, echo, env);
    interp.keyboard_input.echoing = false;
    shown?;
    interp.keyboard_input.echo_kboard = true;
    if waiting_for_input() && !interp.quit_flag_is_nil() {
        quit_throw_to_read_char_outside_signal(interp, env)?;
    }
    Ok(())
}

/// keyboard.c:cancel_echoing: no echoing, prompt or string, at the start
/// of a new command.
pub(crate) fn cancel_echoing(interp: &mut Interpreter) {
    let input = &mut interp.keyboard_input;
    input.immediate_echo = false;
    input.echo_prompt = None;
    input.echo_string = None;
    input.ok_to_echo_at_next_pause = false;
    input.echo_kboard = false;
}

/// keyboard.c:echo_length.
pub(crate) fn echo_length(interp: &Interpreter) -> usize {
    let echo = echo_string(interp);
    if echo.is_string() { schars(&echo) } else { 0 }
}

/// keyboard.c:echo_truncate: the echo string and the echo area's message
/// cut to their first NCHARS characters.
pub(crate) fn echo_truncate(
    interp: &mut Interpreter,
    nchars: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let echo = echo_string(interp);
    if echo.is_string() && schars(&echo) > nchars {
        let cut = call(
            interp,
            "substring",
            &[echo, Value::Integer(0), Value::Integer(nchars as i64)],
            env,
        )?;
        set_echo_string(interp, cut);
    }
    crate::lisp::primitives::dispatch::display::truncate_echo_area(interp, nchars, env)
}

/// read_char before it waits for an event: "Message turns off echoing
/// unless more keystrokes turn it on again" -- something in the echo area
/// that is not this kboard's echo, or that may not be overwritten
/// (ok_to_echo_at_next_pause NULL), cancels echoing; else echo_dash.
pub(crate) fn echo_before_wait(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let message = crate::lisp::primitives::dispatch::display::echo_area_message_value().is_some();
    let input = &interp.keyboard_input;
    if message && (!input.echo_kboard || !input.ok_to_echo_at_next_pause) {
        cancel_echoing(interp);
        Ok(())
    } else {
        echo_dash(interp, env)
    }
}

/// read_char's echo timer, for a read with no end time: in the middle of a
/// key sequence, or with a prefix command's echo to show
/// (`internal-echo-keystrokes-prefix'), the minibuffer not active, and the
/// echo area free (no message, an empty one, or echoing allowed at this
/// pause), echoing starts once `echo-keystrokes' seconds pass with no
/// input -- at once after an event with parameters (PREV_EVENT), as a
/// menu is probably about to show.  The seconds sit_for waits, None when
/// there is no timer to run.
pub(crate) fn echo_timer(
    interp: &mut Interpreter,
    env: &mut Env,
    prev_event: Value,
) -> Result<Option<std::time::Duration>, LispError> {
    if interp.minibuffer_depth() != 0 || interp.keyboard_input.immediate_echo {
        return Ok(None);
    }
    if interp.keyboard_input.command_keys.is_empty() {
        let prefix = interp.call_function_value(
            Value::symbol("internal-echo-keystrokes-prefix"),
            Some("internal-echo-keystrokes-prefix"),
            &[],
            env,
        )?;
        if prefix.is_nil() {
            return Ok(None);
        }
    }
    if interp
        .forwarded_c_value("noninteractive", &Env::new())
        .is_some_and(|value| value.is_truthy())
        || !echo_keystrokes_p(interp, env)
    {
        return Ok(None);
    }
    // One kboard: `echo_kboard && ok_to_echo_at_next_pause == echo_kboard'
    // or `!echo_kboard && ok_to_echo_at_next_pause' is ok_to_echo alone.
    let free = match crate::lisp::primitives::dispatch::display::echo_area_message_value() {
        None => true,
        Some(message) => schars(&message) == 0,
    } || interp.keyboard_input.ok_to_echo_at_next_pause;
    if !free {
        return Ok(None);
    }
    if prev_event.is_cons() {
        echo_now(interp, env)?;
        return Ok(None);
    }
    let seconds = match interp
        .lookup_var_key(cached_symbol!("echo-keystrokes"), env)
        .map(|value| value.kind())
    {
        Some(Kind::Float(seconds)) => seconds.get(),
        Some(Kind::Integer(seconds)) => seconds as f64,
        _ => 0.0,
    };
    // sit_for's dtotimespec, capped as WAIT_READING_MAX caps it.
    Ok(Some(
        std::time::Duration::try_from_secs_f64(seconds)
            .unwrap_or(std::time::Duration::from_secs(u64::from(u32::MAX))),
    ))
}

/// read_char once its echo timer's sit_for returned t (no input arrived
/// in `echo-keystrokes' seconds): echo_now, unless there are unread
/// command events.
pub(crate) fn echo_timer_expired(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if interp
        .lookup_var_key(cached_symbol!("unread-command-events"), env)
        .is_some_and(|events| events.is_cons())
    {
        return Ok(());
    }
    echo_now(interp, env)
}

/// read_char with its event, unless reread with keys already in
/// this-command-keys or read with an end time: echoing can happen at the
/// next pause (not after a mouse movement).  The caller has added the
/// event to the command keys; echo_update shows them.
pub(crate) fn echo_after_event(
    interp: &mut Interpreter,
    event: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if !mouse_movement_p(interp, &event) {
        interp.keyboard_input.ok_to_echo_at_next_pause = true;
    }
    echo_update(interp, env)
}

/// command_loop_1 after a command: with echoing on and a prefix command's
/// echo to show, the echo shown afresh (the prefix argument it now
/// describes), else no echoing.
pub(crate) fn echo_after_command(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if interp.keyboard_input.immediate_echo {
        let prefix = interp.call_function_value(
            Value::symbol("internal-echo-keystrokes-prefix"),
            Some("internal-echo-keystrokes-prefix"),
            &[],
            env,
        )?;
        if !prefix.is_nil() {
            interp.keyboard_input.immediate_echo = false;
            return echo_now(interp, env);
        }
    }
    cancel_echoing(interp);
    Ok(())
}
