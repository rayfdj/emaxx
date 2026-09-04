use super::*;

#[cfg(unix)]
use std::sync::atomic::{AtomicUsize, Ordering as UserSignalOrdering};

#[cfg(unix)]
static PENDING_SIGUSR1: AtomicUsize = AtomicUsize::new(0);
#[cfg(unix)]
static PENDING_SIGUSR2: AtomicUsize = AtomicUsize::new(0);

/// sysdep.c registers SIGUSR1/SIGUSR2 as user-input signals.  The handler may
/// only touch lock-free state; Lisp dispatch happens later from read_char's
/// ordinary event pump.
pub(crate) fn install_user_signal_handlers() {
    #[cfg(unix)]
    {
        static INSTALLED: OnceLock<Result<(), String>> = OnceLock::new();
        let result = INSTALLED.get_or_init(|| {
            // SAFETY: both callbacks perform only an atomic increment, which
            // is async-signal-safe, and signal-hook owns handler chaining.
            unsafe {
                signal_hook::low_level::register(libc::SIGUSR1, || {
                    PENDING_SIGUSR1.fetch_add(1, UserSignalOrdering::Relaxed);
                })
                .map_err(|error| error.to_string())?;
                signal_hook::low_level::register(libc::SIGUSR2, || {
                    PENDING_SIGUSR2.fetch_add(1, UserSignalOrdering::Relaxed);
                })
                .map_err(|error| error.to_string())?;
            }
            Ok(())
        });
        result
            .as_ref()
            .unwrap_or_else(|error| panic!("install SIGUSR1/SIGUSR2 handlers: {error}"));
    }
}

#[cfg(unix)]
fn take_pending_user_signal() -> Option<&'static str> {
    // GNU prepends each add_user_signal registration.  sysdep.c registers
    // SIGUSR1 and then SIGUSR2, so simultaneous pending events drain in this
    // order, one occurrence at a time.
    for (pending, name) in [(&PENDING_SIGUSR2, "sigusr2"), (&PENDING_SIGUSR1, "sigusr1")] {
        if pending
            .fetch_update(
                UserSignalOrdering::Relaxed,
                UserSignalOrdering::Relaxed,
                |count| count.checked_sub(1),
            )
            .is_ok()
        {
            return Some(name);
        }
    }
    None
}

#[cfg(not(unix))]
fn take_pending_user_signal() -> Option<&'static str> {
    None
}

/// Convert pending host user signals into GNU keyboard events.  A binding in
/// `special-event-map' executes through `command-execute'; an unbound event is
/// appended to `unread-command-events' for the active input reader to return.
pub(crate) fn run_pending_user_signal_events(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut handled = false;
    while let Some(name) = take_pending_user_signal() {
        handled = true;
        let event = Value::Symbol(name.into());
        let keymap = interp
            .lookup_var("special-event-map", env)
            .unwrap_or(Value::Nil);
        let binding = keymap_lookup_binding_exact_parts(interp, &keymap, &[name.into()])?;
        interp.set_variable("last-input-event", event.clone(), env);
        if binding.is_nil() {
            let mut unread = unread_command_events(interp, env)?;
            unread.push(event);
            interp.set_variable("unread-command-events", Value::list(unread), env);
            // Keep later signals behind this unread event, matching the main
            // keyboard queue's ordering.
            break;
        }

        let keys = Value::list([Value::symbol("vector-literal"), event]);
        interp.call_function_value(
            Value::Symbol("command-execute".into()),
            Some("command-execute"),
            &[binding, Value::Nil, keys, Value::T],
            env,
        )?;
    }
    Ok(handled)
}

pub(crate) fn set_command_key_state(
    interp: &mut Interpreter,
    keys: Vec<Value>,
    raw_keys: Vec<Value>,
    env: &mut Env,
) {
    interp.keyboard_input.command_keys = keys.clone();
    interp.keyboard_input.single_command_start = 0;
    interp.keyboard_input.raw_keys = raw_keys;
    // GNU has no `this-single-command-keys' *variable* -- only the
    // keyboard.c function, which reads this native state (finding 66).
    let _ = (keys, env);
}

fn dribble_event_bytes(event: &Value) -> Vec<u8> {
    match event {
        Value::Integer(code) => u32::try_from(*code)
            .ok()
            .and_then(char::from_u32)
            .map(|character| character.to_string().into_bytes())
            .unwrap_or_else(|| format!("<{code}>").into_bytes()),
        Value::String(text) => text.as_bytes().to_vec(),
        Value::StringObject(state) => state.borrow().text.as_bytes().to_vec(),
        Value::Symbol(symbol) => format!("<{symbol}>").into_bytes(),
        other => format!("<{other}>").into_bytes(),
    }
}

fn record_external_input_event(interp: &mut Interpreter, event: &Value, env: &Env) {
    if !interp.kbd_macro_executions.is_empty() {
        return;
    }
    interp.keyboard_input.recent_keys.push(event.clone());
    let limit = interp.lossage_size.max(0) as usize;
    if interp.keyboard_input.recent_keys.len() > limit {
        let excess = interp.keyboard_input.recent_keys.len() - limit;
        interp.keyboard_input.recent_keys.drain(0..excess);
    }
    if let Some(path) = &interp.keyboard_input.dribble_file
        && let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(path)
    {
        let _ = file.write_all(&dribble_event_bytes(event));
        let _ = file.flush();
    }
    // read_char records terminal events at the point where they are
    // consumed, including events read recursively by an interactive
    // command.  The outer command loop records only that command's key
    // sequence, so a register name, query answer, or other nested input
    // must be appended here to make the macro self-contained on replay.
    if interp
        .lookup_var("defining-kbd-macro", env)
        .is_some_and(|value| value.is_truthy())
    {
        interp.kbd_macro_definition.push(event.clone());
    }
}

pub(crate) fn function_documentation(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<Value> {
    if let Value::Symbol(symbol) = value
        && let Some(documentation) = interp.get_symbol_property(symbol, "function-documentation")
    {
        return Some(documentation);
    }
    let value = match value {
        Value::Symbol(symbol) => interp.lookup_function(symbol, env).ok()?,
        other => other.clone(),
    };
    // doc.c Fdocumentation: a macro's documentation lives on the function
    // inside its (macro . FUNCTION) cons.
    let value = if matches!(value.car(), Ok(Value::Symbol(ref name)) if name == "macro") {
        value.cdr().ok()?
    } else {
        value
    };
    // doc.c Fdocumentation: an autoload's documentation is the third
    // element of its (autoload FILE DOC INTERACTIVE TYPE) form, read
    // without resolving the autoload (GNU does not load the file here).
    if matches!(value.car(), Ok(Value::Symbol(ref name)) if name == "autoload") {
        let items = value.to_vec().ok()?;
        return items.get(2).filter(|doc| !doc.is_nil()).cloned();
    }
    if let Value::Record(id) = value
        && let Some(record) = interp.find_record(id)
        && record.kind == crate::lisp::eval::RecordKind::Closure
    {
        return record.slots.get(4).filter(|doc| !doc.is_nil()).cloned();
    }
    let Value::Lambda(lambda) = value else {
        return None;
    };
    lambda.documentation.clone().filter(|documentation| {
        matches!(
            documentation,
            Value::String(_) | Value::StringObject(_) | Value::Integer(_) | Value::Cons(_)
        )
    })
}

pub(crate) fn is_vector_like_value(interp: &Interpreter, value: &Value) -> bool {
    is_vector_value(value)
        || is_bool_vector_value(interp, value)
        || matches!(value, Value::CharTable(_))
}

pub(crate) fn is_vector_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Cons(cell)
            if matches!(&*cell.car.borrow(), Value::Symbol(symbol) if symbol == "vector-literal")
    )
}

/// Whether VALUE has GNU cons identity despite Emaxx's internal facade
/// representations.  Vector literals use cons storage but are not Lisp
/// conses; runtime keymaps use records but project the list identity GNU
/// exposes.  Keep this decision shared by native predicates and VM opcodes.
pub(crate) fn is_cons_value(interp: &Interpreter, value: &Value) -> bool {
    (matches!(value, Value::Cons(_)) && !is_vector_value(value))
        || keymap_record_id(interp, value).is_some()
}

pub(crate) fn fixnum_bounds(interp: &Interpreter) -> Result<(i64, i64), LispError> {
    let max_fixnum = interp
        .default_value("most-positive-fixnum")
        .ok_or_else(|| LispError::Void("most-positive-fixnum".into()))?
        .as_integer()?;
    let min_fixnum = interp
        .default_value("most-negative-fixnum")
        .ok_or_else(|| LispError::Void("most-negative-fixnum".into()))?
        .as_integer()?;
    Ok((min_fixnum, max_fixnum))
}

pub(crate) fn symbol_with_pos_parts(interp: &Interpreter, value: &Value) -> Option<(Value, i64)> {
    let Value::Record(id) = value else {
        return None;
    };
    let record = interp.find_record(*id)?;
    if record.kind != crate::lisp::eval::RecordKind::SymbolWithPos || record.slots.len() < 2 {
        return None;
    }
    Some((record.slots[0].clone(), record.slots[1].as_integer().ok()?))
}

#[cfg(test)]
thread_local! {
    static SYMBOL_WITH_POS_FLAG_READ_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

pub(crate) fn symbols_with_pos_enabled(interp: &Interpreter, env: &Env) -> bool {
    #[cfg(test)]
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(|count| count.set(count.get() + 1));
    interp
        .lookup_var("symbols-with-pos-enabled", env)
        .is_some_and(|value| value.is_truthy())
}

/// GNU's `CHECK_SYMBOL' accepts a symbol-with-position while
/// `symbols-with-pos-enabled' is dynamically non-nil, and `XSYMBOL' then
/// addresses the underlying bare symbol (`src/lisp.h').  Keep that contract
/// in one place so C-owned symbol primitives do not each invent a subtly
/// different positioned-symbol policy.
pub(crate) fn checked_symbol_name(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<String, LispError> {
    if let Ok(symbol) = value.as_symbol() {
        return Ok(symbol.to_string());
    }
    if symbols_with_pos_enabled(interp, env)
        && let Some((symbol, _)) = symbol_with_pos_parts(interp, value)
        && let Ok(symbol) = symbol.as_symbol()
    {
        return Ok(symbol.to_string());
    }
    Err(LispError::WrongTypeArgument(
        "symbolp".into(),
        value.clone(),
    ))
}

#[cfg(test)]
pub(crate) fn reset_symbol_with_pos_flag_read_count() {
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn symbol_with_pos_flag_read_count() -> usize {
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(std::cell::Cell::get)
}

pub(crate) fn symbol_with_pos_equal_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Option<bool> {
    let left_with_pos = symbol_with_pos_parts(interp, left);
    let right_with_pos = symbol_with_pos_parts(interp, right);
    if left_with_pos.is_none() && right_with_pos.is_none() {
        return None;
    }

    let enabled = symbols_with_pos_enabled(interp, env);
    if enabled {
        let left_base = left_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(left);
        let right_base = right_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(right);
        return Some(
            match (plain_symbol_name(left_base), plain_symbol_name(right_base)) {
                (Some(_), Some(_)) => left_base == right_base,
                _ => false,
            },
        );
    }

    match (left_with_pos, right_with_pos) {
        (Some((left_symbol, left_pos)), Some((right_symbol, right_pos))) => {
            Some(left_symbol == right_symbol && left_pos == right_pos)
        }
        _ => None,
    }
}

pub(crate) fn symbol_with_pos_eq_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Option<bool> {
    let left_with_pos = symbol_with_pos_parts(interp, left);
    let right_with_pos = symbol_with_pos_parts(interp, right);
    if left_with_pos.is_none() && right_with_pos.is_none() {
        return None;
    }

    if !symbols_with_pos_enabled(interp, env) {
        return None;
    }

    let left_base = left_with_pos
        .as_ref()
        .map(|(symbol, _)| symbol)
        .unwrap_or(left);
    let right_base = right_with_pos
        .as_ref()
        .map(|(symbol, _)| symbol)
        .unwrap_or(right);
    Some(
        match (plain_symbol_name(left_base), plain_symbol_name(right_base)) {
            (Some(_), Some(_)) => left_base == right_base,
            _ => false,
        },
    )
}

pub(crate) fn eval_callable_metadata_form(
    interp: &mut Interpreter,
    func: &Value,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if let Value::Lambda(lambda) = func {
        interp.eval_with_closure_env(&lambda.env, env, |interp, call_env| {
            interp.eval(form, call_env)
        })
    } else {
        interp.eval(form, env)
    }
}

pub(crate) fn parse_interactive_string(
    spec: &str,
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    // callint.c's leading flag characters, handled once at the start of
    // the whole spec: `*' barfs on a read-only buffer, `^' runs the
    // shift-selection protocol, `@' selects the event's window (a mouse
    // affair the keyboard frontend has no window position for), and `-'
    // is ignored for Lucid compatibility.
    let mut rest = spec;
    loop {
        match rest.chars().next() {
            Some('*') => {
                super::call(interp, "barf-if-buffer-read-only", &[], env)?;
            }
            Some('^') => {
                // callint.c:410 `call0 (Qhandle_shift_selection)': the
                // handler is simple.el's, reached through the ordinary
                // function cell, and GNU has no missing-function
                // tolerance -- a runtime without simple.el loaded
                // signals void-function exactly as GNU would.
                interp.call_function_value(
                    Value::Symbol("handle-shift-selection".into()),
                    Some("handle-shift-selection"),
                    &[],
                    env,
                )?;
            }
            Some('@') | Some('-') => {}
            _ => break,
        }
        rest = &rest[1..];
    }
    let call1 = |interp: &mut Interpreter, env: &mut Env, name: &str, args: &[Value]| {
        interp.call_function_value(Value::Symbol(name.into()), Some(name), args, env)
    };
    // callint.c's check_mark: the mark must exist in this buffer, and
    // under transient-mark-mode an inactive mark signals `mark-inactive'
    // unless mark-even-if-inactive overrides.
    let check_mark =
        |interp: &mut Interpreter, env: &mut Env, for_region: bool| -> Result<usize, LispError> {
            let mark = interp.buffer.mark().ok_or_else(|| {
                LispError::Signal(
                    if for_region {
                        "The mark is not set now, so there is no region"
                    } else {
                        "The mark is not set now"
                    }
                    .into(),
                )
            })?;
            let transient = interp
                .lookup_var("transient-mark-mode", env)
                .is_some_and(|value| value.is_truthy());
            let even_if_inactive = interp
                .lookup_var("mark-even-if-inactive", env)
                .is_some_and(|value| value.is_truthy());
            if transient && !even_if_inactive && !interp.buffer.mark_active() {
                return Err(LispError::SignalValue(Value::list([Value::Symbol(
                    "mark-inactive".into(),
                )])));
            }
            Ok(mark)
        };
    let mut values = Vec::new();
    // GNU formats each prompt with `format-message' against the
    // arguments read so far (callint.c builds callint_message from the
    // visargs); a later prompt's %s shows the earlier answer.
    let mut visible: Vec<Value> = Vec::new();
    for line in rest.split('\n') {
        if line.is_empty() {
            continue;
        }
        let mut chars = line.chars();
        let Some(code) = chars.next() else {
            continue;
        };
        let raw_prompt: String = chars.collect();
        // GNU builds every prompt through Fformat_message against the
        // visible arguments read so far; that also converts quote
        // characters per text-quoting-style, exactly as the glass shows.
        let formatted_prompt = |interp: &mut Interpreter,
                                env: &mut Env,
                                visible: &[Value]|
         -> Result<Value, LispError> {
            let mut args = vec![Value::String(raw_prompt.clone().into())];
            args.extend(visible.iter().cloned());
            call1(interp, env, "format-message", &args)
        };
        // Each answer's visible form feeds later prompts' formats; the
        // no-I/O codes leave nil there exactly as GNU's visargs do.
        let mut seen = Value::Nil;
        match code {
            'a' | 'C' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let obarray = interp.lookup_var("obarray", env).unwrap_or(Value::Nil);
                let predicate =
                    Value::Symbol(if code == 'a' { "fboundp" } else { "commandp" }.into());
                let name = call1(
                    interp,
                    env,
                    "completing-read",
                    &[message, obarray, predicate, Value::T],
                )?;
                let text = crate::lisp::primitives::string_text(&name)?;
                seen = Value::String(text.clone().into());
                values.push(Value::Symbol(text.into()));
            }
            'b' | 'B' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let current = call1(interp, env, "current-buffer", &[])?;
                let in_minibuffer = call1(interp, env, "window-minibuffer-p", &[])
                    .map(|flag| flag.is_truthy())
                    .unwrap_or(false);
                let default = if code == 'B' || in_minibuffer {
                    call1(interp, env, "other-buffer", &[current])?
                } else {
                    current
                };
                let require = if code == 'b' { Value::T } else { Value::Nil };
                let name = call1(interp, env, "read-buffer", &[message, default, require])?;
                seen = name.clone();
                values.push(name);
            }
            'c' => {
                let message = formatted_prompt(interp, env, &visible)?;
                // GNU shows the prompt in minibuffer-prompt face while
                // read-char waits on the echo area.
                let message = call1(
                    interp,
                    env,
                    "propertize",
                    &[
                        message,
                        Value::Symbol("face".into()),
                        Value::Symbol("minibuffer-prompt".into()),
                    ],
                )
                .unwrap_or_else(|_| Value::String(raw_prompt.clone().into()));
                let event = call1(interp, env, "read-char", &[message])?;
                if !matches!(event, Value::Integer(_)) {
                    return Err(LispError::Signal("Non-character input-event".into()));
                }
                seen = call1(interp, env, "char-to-string", std::slice::from_ref(&event))?;
                values.push(event);
            }
            'd' => values.push(Value::Integer(interp.buffer.point() as i64)),
            'D' | 'f' | 'F' | 'G' => {
                let message = formatted_prompt(interp, env, &visible)?;
                // callint.c's read_file_name helper: Fread_file_name
                // (PROMPT, nil, DEFAULT, MUSTMATCH, INITIAL, PREDICATE).
                let (default, mustmatch, initial, predicate) = match code {
                    'D' => (
                        interp
                            .lookup_var("default-directory", env)
                            .unwrap_or(Value::Nil),
                        Value::Symbol("lambda".into()),
                        Value::Nil,
                        Value::Symbol("file-directory-p".into()),
                    ),
                    'f' => (
                        Value::Nil,
                        Value::Symbol("lambda".into()),
                        Value::Nil,
                        Value::Nil,
                    ),
                    'G' => (Value::Nil, Value::Nil, Value::String("".into()), Value::Nil),
                    _ => (Value::Nil, Value::Nil, Value::Nil, Value::Nil),
                };
                let name = call1(
                    interp,
                    env,
                    "read-file-name",
                    &[message, Value::Nil, default, mustmatch, initial, predicate],
                )?;
                seen = name.clone();
                values.push(name);
            }
            'e' => {
                // The invoking event, which must carry parameters (a
                // mouse posn); keyboard keys never do.
                let event = interp
                    .lookup_var("this-command-keys-vector", env)
                    .and_then(|keys| keys.to_vec().ok())
                    .and_then(|events| {
                        events
                            .into_iter()
                            .skip(1)
                            .find(|event| matches!(event, Value::Cons(_)))
                    });
                match event {
                    Some(event) => values.push(event),
                    None => {
                        return Err(LispError::Signal(
                            "command must be bound to an event with parameters".into(),
                        ));
                    }
                }
            }
            'k' | 'K' => {
                let ch = unread_command_event_char(&pop_unread_command_event_value(interp, env)?)?;
                values.push(Value::String(ch.to_string().into()));
            }
            'm' => {
                let mark = check_mark(interp, env, false)?;
                values.push(Value::Integer(mark as i64));
            }
            'p' => {
                let prefix = interp
                    .lookup_var("current-prefix-arg", env)
                    .unwrap_or(Value::Nil);
                values.push(prefix_numeric_value(&prefix)?);
            }
            'P' => {
                values.push(
                    interp
                        .lookup_var("current-prefix-arg", env)
                        .unwrap_or(Value::Nil),
                );
            }
            // "i": an ignored argument — always nil, no I/O (window.el's
            // commands pass their INTERACTIVE params through it).
            'i' => values.push(Value::Nil),
            'n' | 'N' => {
                let prefix = interp
                    .lookup_var("current-prefix-arg", env)
                    .unwrap_or(Value::Nil);
                if code == 'N' && prefix.is_truthy() {
                    values.push(prefix_numeric_value(&prefix)?);
                } else {
                    let message = formatted_prompt(interp, env, &visible)?;
                    let number = call1(interp, env, "read-number", &[message])?;
                    seen = call1(
                        interp,
                        env,
                        "number-to-string",
                        std::slice::from_ref(&number),
                    )
                    .unwrap_or(Value::Nil);
                    values.push(number);
                }
            }
            'r' => {
                let mark = check_mark(interp, env, true)?;
                let point = interp.buffer.point();
                values.push(Value::Integer(point.min(mark) as i64));
                values.push(Value::Integer(point.max(mark) as i64));
            }
            's' | 'M' => {
                let message = formatted_prompt(interp, env, &visible)?;
                // 'M' inherits the input method; the tty session reads
                // plain strings either way.
                let inherit = if code == 'M' { Value::T } else { Value::Nil };
                let text = call1(
                    interp,
                    env,
                    "read-string",
                    &[message, Value::Nil, Value::Nil, Value::Nil, inherit],
                )?;
                seen = text.clone();
                values.push(text);
            }
            'S' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let text = call1(
                    interp,
                    env,
                    "read-string",
                    &[message, Value::Nil, Value::Nil, Value::Nil, Value::Nil],
                )?;
                seen = text.clone();
                values.push(Value::Symbol(
                    crate::lisp::primitives::string_text(&text)?.into(),
                ));
            }
            'U' => {
                // The up-event recorded by a preceding k/K; the keyboard
                // frontend records none, exactly like GNU without one.
                values.push(Value::Nil);
            }
            'v' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let variable = call1(interp, env, "read-variable", &[message])?;
                seen = interp
                    .lookup_var("minibuffer-history", env)
                    .and_then(|history| history.car().ok())
                    .unwrap_or(Value::Nil);
                values.push(variable);
            }
            'x' | 'X' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let reader = if code == 'x' {
                    "read-minibuffer"
                } else {
                    "eval-minibuffer"
                };
                let form = call1(interp, env, reader, &[message])?;
                values.push(form);
            }
            'z' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let coding = call1(interp, env, "read-coding-system", &[message, Value::Nil])?;
                values.push(coding);
            }
            'Z' => {
                let prefix = interp
                    .lookup_var("current-prefix-arg", env)
                    .unwrap_or(Value::Nil);
                if prefix.is_truthy() {
                    let message = formatted_prompt(interp, env, &visible)?;
                    let coding = call1(interp, env, "read-non-nil-coding-system", &[message])?;
                    values.push(coding);
                } else {
                    values.push(Value::Nil);
                }
            }
            _ => return Err(invalid_interactive_control_letter(code)),
        }
        if seen.is_nil()
            && let Some(Value::String(_) | Value::StringObject(_)) = values.last()
        {
            seen = values.last().cloned().unwrap_or(Value::Nil);
        }
        visible.push(seen);
    }
    Ok(values)
}

// Terminal-driven event input, installed by the tty frontend for the
// duration of an interactive session.  The reader blocks on the terminal
// and returns one key event, or `None' for C-g; without a reader the
// queued-events contract below is unchanged.
thread_local! {
    static TTY_EVENT_READER: std::cell::RefCell<Option<TtyEventReader>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyEventReader = Box<dyn FnMut() -> Option<Value>>;

// A non-blocking companion to the reader: wait briefly for one event and
// answer None when the terminal stays quiet.  Blocking reads poll
// through it so ripe timers keep firing while a command waits for input
// (GNU's read_char runs timer_check inside its wait) — the minibuffer's
// own reads included.
thread_local! {
    static TTY_EVENT_POLLER: std::cell::RefCell<Option<TtyEventPoller>> =
        const { std::cell::RefCell::new(None) };
    static TTY_CURSOR_IN_ECHO_AREA: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

pub(crate) type TtyEventPoller = Box<dyn FnMut() -> Option<Option<Value>>>;

pub(crate) fn set_tty_event_reader(reader: Option<TtyEventReader>) {
    TTY_EVENT_READER.with_borrow_mut(|slot| *slot = reader);
}

pub(crate) fn set_tty_event_poller(poller: Option<TtyEventPoller>) {
    TTY_EVENT_POLLER.with_borrow_mut(|slot| *slot = poller);
}

fn read_via_tty_event_reader(cursor_in_echo_area: bool) -> Option<Option<Value>> {
    TTY_CURSOR_IN_ECHO_AREA.set(cursor_in_echo_area);
    let result = TTY_EVENT_READER.with_borrow_mut(|slot| slot.as_mut().map(|reader| reader()));
    TTY_CURSOR_IN_ECHO_AREA.set(false);
    result
}

fn poll_via_tty_event_poller(cursor_in_echo_area: bool) -> Option<Option<Option<Value>>> {
    TTY_CURSOR_IN_ECHO_AREA.set(cursor_in_echo_area);
    let result = TTY_EVENT_POLLER.with_borrow_mut(|slot| slot.as_mut().map(|poller| poller()));
    TTY_CURSOR_IN_ECHO_AREA.set(false);
    result
}

pub(crate) fn tty_cursor_in_echo_area() -> bool {
    TTY_CURSOR_IN_ECHO_AREA.get()
}

/// Whether a terminal frontend is feeding events this session; the
/// minibuffer reads through its own event loop when one is.
pub(crate) fn has_tty_event_reader() -> bool {
    TTY_EVENT_READER.with_borrow(|slot| slot.is_some())
}

/// What a pending key sequence resolves to under the live keymaps.
pub(crate) enum KeyResolution {
    Command(Value),
    Prefix,
    Undefined,
}

fn command_loop_call(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    interp.call_function_value(Value::Symbol(name.into()), None, args, env)
}

// GNU's kboard->echo_string for the frontend: the echo of a key
// sequence that is still pending while its read blocks (the mouse-menu
// popup).  The blocking reader displays it once `echo-keystrokes' idle
// seconds pass, exactly as read_char's echo timer does.
thread_local! {
    static PENDING_KEYSTROKE_ECHO: std::cell::RefCell<
        Option<(String, crate::lisp::primitives::EchoSpans)>,
    > = const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_pending_keystroke_echo(
    echo: Option<(String, crate::lisp::primitives::EchoSpans)>,
) {
    PENDING_KEYSTROKE_ECHO.with_borrow_mut(|slot| *slot = echo);
}

pub(crate) fn take_pending_keystroke_echo() -> Option<(String, crate::lisp::primitives::EchoSpans)>
{
    PENDING_KEYSTROKE_ECHO.with_borrow_mut(|slot| slot.take())
}

/// keyboard.c's echo pipeline for a pending key sequence: echo_add_key
/// renders each event space-separated (a composite event echoes its
/// bare head symbol name, a character its key description), echo_dash
/// appends the trailing dash, and — when `echo-keystrokes-help' is on —
/// help.el's own `help--append-keystrokes-help' decides whether the C-h
/// hint follows, reading `this-single-command-keys' against the active
/// maps and propertizing the key through substitute-command-keys.
pub(crate) fn pending_keystroke_echo(
    interp: &mut Interpreter,
    env: &mut Env,
    pending: &[Value],
) -> (String, crate::lisp::primitives::EchoSpans) {
    let mut text = String::new();
    for event in pending {
        if !text.is_empty() {
            text.push(' ');
        }
        let head = match event {
            Value::Cons(_) => event.car().unwrap_or(Value::Nil),
            other => other.clone(),
        };
        match head {
            Value::Symbol(name) => text.push_str(&name),
            other => {
                let description = super::call(
                    interp,
                    "single-key-description",
                    std::slice::from_ref(&other),
                    env,
                )
                .ok()
                .and_then(|value| crate::lisp::primitives::string_text(&value).ok());
                match description {
                    Some(description) => text.push_str(&description),
                    None => text.push_str(&format!("{other}")),
                }
            }
        }
    }
    // echo_dash: the temporary trailing dash of an unfinished sequence.
    text.push('-');
    let help_wanted = interp
        .lookup_var("echo-keystrokes-help", env)
        .is_none_or(|value| value.is_truthy());
    if help_wanted
        && interp
            .lookup_function("help--append-keystrokes-help", env)
            .is_ok()
    {
        // The Lisp side reads this-single-command-keys, exactly what
        // read_key_sequence has accumulated at this point.
        set_command_key_state(interp, pending.to_vec(), pending.to_vec(), env);
        // help.el's function is interpreted Lisp: route through the
        // full function channel, not the builtin dispatch.
        if let Ok(appended) = interp.call_function_value(
            Value::Symbol("help--append-keystrokes-help".into()),
            Some("help--append-keystrokes-help"),
            &[Value::String(text.clone().into())],
            env,
        ) && let Ok(appended_text) = crate::lisp::primitives::string_text(&appended)
        {
            let spans = crate::lisp::primitives::string_face_spans(&appended);
            return (appended_text, spans);
        }
    }
    (text, Vec::new())
}

/// The event-head symbol of a parameterized mouse click event —
/// ("C-down-mouse-3" POSN) answers the symbol; anything else nil.
fn mouse_event_head(event: &Value) -> Option<String> {
    let items = event.to_vec().ok()?;
    match (items.first(), items.get(1)) {
        (Some(Value::Symbol(head)), Some(Value::Cons(_))) if head.contains("mouse-") => {
            Some(head.to_string())
        }
        _ => None,
    }
}

/// Whether a click event's posn sits on the menu-bar area.
fn mouse_event_on_menu_bar(event: &Value) -> bool {
    event
        .to_vec()
        .ok()
        .and_then(|items| items.get(1)?.to_vec().ok())
        .is_some_and(|posn| matches!(posn.get(1), Some(Value::Symbol(area)) if area == "menu-bar"))
}

/// Resolve a pending key sequence through the runtime's own keymaps
/// (`key-binding'), classifying strict prefixes so a multi-key sequence
/// keeps reading.  This is the single resolution path for every command
/// loop — the frame's and the minibuffer's recursive one.
pub(crate) fn resolve_key_sequence(
    interp: &mut Interpreter,
    env: &mut Env,
    pending: &[Value],
) -> KeyResolution {
    // keyboard.c's read_key_sequence: a click's keymap key is its head
    // symbol, and a click on the menu-bar area inserts the fake
    // `menu-bar' prefix before it ([menu-bar mouse-1] finds
    // menu-bar-open-mouse).
    let mut lookup_events: Vec<Value> = Vec::with_capacity(pending.len() + 1);
    for (index, event) in pending.iter().enumerate() {
        if let Some(head) = mouse_event_head(event) {
            if index == 0 && mouse_event_on_menu_bar(event) {
                lookup_events.push(Value::Symbol("menu-bar".into()));
            }
            lookup_events.push(Value::Symbol(head.into()));
        } else {
            lookup_events.push(event.clone());
        }
    }
    let key_vector =
        Value::list(std::iter::once(Value::Symbol("vector-literal".into())).chain(lookup_events));
    let binding = match command_loop_call(interp, env, "key-binding", &[key_vector, Value::T]) {
        Ok(binding) => binding,
        Err(_) => Value::Nil,
    };
    if binding.is_nil() {
        // An unresolved strict prefix keeps reading (C-x alone answers nil
        // while C-x C-f resolves), so probe whether any longer sequence can
        // still match by asking for the prefix's own keymap.
        if pending_sequence_is_prefix(interp, env, pending) {
            return KeyResolution::Prefix;
        }
        return KeyResolution::Undefined;
    }
    // A prefix can answer as the keymap itself or as a prefix command
    // symbol (`Control-X-prefix') whose function cell holds the keymap;
    // GNU resolves through the indirection before dispatching.
    let resolved = if let Value::Symbol(name) = &binding {
        interp
            .lookup_function(name, env)
            .unwrap_or_else(|_| binding.clone())
    } else {
        binding.clone()
    };
    if crate::lisp::primitives::is_keymap_value(interp, &resolved) {
        // A keymap bound to a parameterized click pops up as a menu
        // (read_key_sequence's mouse-menu path — C-down-mouse-3's
        // menu-item filter yields the menu-bar keymap); the chosen
        // item's key path finishes the sequence.
        if let Some(event) = pending
            .last()
            .filter(|event| mouse_event_head(event).is_some())
            && has_tty_menu_executor()
        {
            // The sequence stays pending while the menu is up: GNU
            // holds its echo in kboard->echo_string and read_char's
            // timer displays it after `echo-keystrokes' idle seconds.
            // Compute it through the real machinery now; the executor's
            // modal read owns the timing.
            let echo = pending_keystroke_echo(interp, env, pending);
            set_pending_keystroke_echo(Some(echo));
            let answer = super::call(
                interp,
                "x-popup-menu",
                &[(*event).clone(), resolved.clone()],
                env,
            )
            .unwrap_or(Value::Nil);
            let path = answer.to_vec().unwrap_or_default();
            if path.is_empty() {
                // Cancelled: the sequence dissolves with no command and
                // no quit (MENU_FOR_CLICK).
                return KeyResolution::Command(Value::Symbol("ignore".into()));
            }
            let vector =
                Value::list(std::iter::once(Value::Symbol("vector-literal".into())).chain(path));
            let chosen = super::call(interp, "lookup-key", &[resolved, vector], env)
                .ok()
                .filter(|value| !value.is_nil())
                .and_then(|value| {
                    crate::lisp::primitives::keymap_get_keyelt(interp, &value, true, env).ok()
                });
            if let Some(command) = chosen.filter(|value| !value.is_nil()) {
                return KeyResolution::Command(command);
            }
            return KeyResolution::Command(Value::Symbol("ignore".into()));
        }
        KeyResolution::Prefix
    } else {
        KeyResolution::Command(binding)
    }
}

fn pending_sequence_is_prefix(interp: &mut Interpreter, env: &mut Env, pending: &[Value]) -> bool {
    // ESC alone is always a live prefix (meta encoding).
    if pending.len() == 1 && matches!(pending.first(), Some(Value::Integer(27))) {
        return true;
    }
    let key_vector = Value::list(
        std::iter::once(Value::Symbol("vector-literal".into())).chain(pending.iter().cloned()),
    );
    // `key-binding' with ACCEPT-DEFAULT nil still answers prefix keymaps.
    command_loop_call(interp, env, "key-binding", &[key_vector])
        .map(|binding| {
            !binding.is_nil()
                && command_loop_call(interp, env, "keymapp", &[binding])
                    .map(|value| value.is_truthy())
                    .unwrap_or(false)
        })
        .unwrap_or(false)
}

/// Execute one resolved command with GNU's full per-command ceremony:
/// undo boundary, echo clearing, key-state publication, prefix handoff,
/// pre/post-command hooks around `call-interactively', and the
/// last-command bookkeeping.  Shared by the frame command loop and the
/// minibuffer's recursive loop.
pub(crate) fn execute_command_binding(
    interp: &mut Interpreter,
    env: &mut Env,
    binding: Value,
    keys: &[Value],
    last_event: Value,
) -> Result<(), LispError> {
    execute_command_binding_inner(interp, env, binding, keys, last_event, true)
}

/// Execute a command whose input events were already recorded by a recursive
/// reader.  `read_char' records those events as they arrive; adding the same
/// resolved key sequence again would duplicate minibuffer input in a keyboard
/// macro definition.
pub(crate) fn execute_recorded_input_command_binding(
    interp: &mut Interpreter,
    env: &mut Env,
    binding: Value,
    keys: &[Value],
    last_event: Value,
) -> Result<(), LispError> {
    execute_command_binding_inner(interp, env, binding, keys, last_event, false)
}

fn execute_command_binding_inner(
    interp: &mut Interpreter,
    env: &mut Env,
    binding: Value,
    keys: &[Value],
    last_event: Value,
    record_keys: bool,
) -> Result<(), LispError> {
    // keyboard.c refreshes point_before_last_command_or_undo at every
    // command boundary, independently of whether simple.el decides that a
    // new undo-list boundary is needed.  Without this, a motion between two
    // edits leaves record_point using the earlier edit's position.
    interp.buffer.note_undo_command_point();
    // keyboard.c:1537: before this command runs, boundaries for the
    // LAST command's changes are ensured through simple.el's own
    // `undo-auto--add-boundary', whose amalgamation policy (fusing runs
    // of self-inserts) native boundary pushes cannot reproduce.  A bare
    // runtime without simple.el keeps the plain native boundary.
    if interp
        .lookup_function("undo-auto--add-boundary", env)
        .is_ok()
    {
        let _ = interp.call_function_value(
            Value::Symbol("undo-auto--add-boundary".into()),
            Some("undo-auto--add-boundary"),
            &[],
            env,
        );
    } else {
        interp.buffer.push_undo_boundary();
    }
    // keyboard.c read_char wipes a lingering message when the next input
    // event arrives: the channel empties before the command runs, but the
    // glass only catches up at the next redisplay — so a command that
    // blocks with redisplay frozen (the F10 menu) keeps the old message
    // visible until an explicit `message' repaints the row.
    crate::lisp::primitives::expire_echo_area_message();
    interp.set_variable("last-command-event", last_event.clone(), env);
    interp.set_variable("last-input-event", last_event.clone(), env);
    if !mouse_event_on_menu_bar(&last_event) {
        interp.set_variable("last-nonmenu-event", last_event, env);
    }
    // The canonical key-state channel: this-command-keys,
    // this-single-command-keys, and their raw variants all read it
    // (isearch's pre-command-hook indexes the vector).
    set_command_key_state(interp, keys.to_vec(), keys.to_vec(), env);
    interp.set_variable(
        "this-command-keys-vector",
        Value::list(
            std::iter::once(Value::Symbol("vector-literal".into())).chain(keys.iter().cloned()),
        ),
        env,
    );
    interp.set_variable("this-command", binding.clone(), env);
    // read_char records terminal events as they arrive while a keyboard
    // macro is being defined.  The command's end keys are deliberately
    // provisional: `end-kbd-macro' truncates back to the last command
    // boundary, while an ordinary completed command commits them below.
    if record_keys
        && interp
            .lookup_var("defining-kbd-macro", env)
            .is_some_and(|value| value.is_truthy())
    {
        interp.kbd_macro_definition.extend_from_slice(keys);
    }
    // Timers may have messed with `deactivate-mark'; the command starts
    // with it reset (keyboard.c command_loop_1).
    interp.set_variable("deactivate-mark", Value::Nil, env);
    let modified_before = (
        interp.current_buffer_id(),
        interp.buffer.chars_modified_tick(),
    );
    // pre-command-hook may rewrite `this-command' (isearch's exit path
    // does); GNU executes whatever the hook left there.
    let buffer_id = interp.current_buffer_id();
    crate::lisp::primitives::safe_run_named_hooks(interp, "pre-command-hook", env, Some(buffer_id))
        .unwrap_or(());
    let dispatched = interp
        .lookup_var("this-command", env)
        .filter(|command| !command.is_nil())
        .unwrap_or_else(|| binding.clone());
    // keyboard.c command_loop_1 executes the command through simple.el's
    // `command-execute' (call1 (Qcommand_execute, Vthis_command)): the
    // prefix-arg handoff (current-prefix-arg takes prefix-arg, which
    // clears, with `prefix-command-update' run), the `disabled' property
    // check routing to `disabled-command-function', and keyboard-macro
    // arrays all live there.  A bare runtime without simple.el keeps the
    // native equivalent of the prefix handoff plus call-interactively.
    let result = if interp.lookup_function("command-execute", env).is_ok() {
        command_loop_call(
            interp,
            env,
            "command-execute",
            std::slice::from_ref(&dispatched),
        )
        .map(|_| ())
    } else {
        let prefix = interp.lookup_var("prefix-arg", env).unwrap_or(Value::Nil);
        interp.set_variable("current-prefix-arg", prefix, env);
        interp.set_variable("prefix-arg", Value::Nil, env);
        command_loop_call(
            interp,
            env,
            "call-interactively",
            std::slice::from_ref(&dispatched),
        )
        .map(|_| ())
    };
    // A command that leaves through `throw' or another nonlocal exit never
    // reaches keyboard.c's command-completion boundary.  In particular,
    // exit-minibuffer must not run the minibuffer's post-command hooks a
    // second time after its initial pre-input boundary.
    if result.is_ok() {
        let buffer_id = interp.current_buffer_id();
        crate::lisp::primitives::safe_run_named_hooks(
            interp,
            "post-command-hook",
            env,
            Some(buffer_id),
        )
        .unwrap_or(());
        // After post-command-hook GNU deactivates the mark when the command
        // asked for it (keyboard.c calls the real `deactivate-mark' so its
        // hook runs).  Buffer-modifying primitives are insdel.c's trigger
        // for the same flag; until every native arm publishes it, a changed
        // modification tick stands in for that side of the protocol.
        let deactivate = interp
            .lookup_var("deactivate-mark", env)
            .is_some_and(|value| value.is_truthy())
            || (interp.current_buffer_id() == modified_before.0
                && interp.buffer.chars_modified_tick() != modified_before.1);
        let mark_active = interp
            .lookup_var("mark-active", env)
            .is_some_and(|value| value.is_truthy());
        if deactivate && mark_active {
            // `deactivate-mark' is GNU simple.el's; keyboard.c reaches it as
            // an ordinary Lisp call (call0), never through native dispatch.
            let _ = interp.call_function_value(
                Value::Symbol("deactivate-mark".into()),
                Some("deactivate-mark"),
                &[],
                env,
            );
        }
    }
    // GNU takes last-command from this-command AFTER the command ran: a
    // prefix command (universal-argument) restores the previous value
    // there via prefix-command-preserve-state, keeping last-command
    // stable across the C-u chain.
    // undo.c's run_undoable_change fires on the first change a command
    // makes in a buffer; at command granularity the same set is "every
    // buffer whose text tick moved" -- report each through simple.el's
    // `undo-auto--undoable-change' so the next `undo-auto--add-boundary'
    // sees it.  (Changes made by idle timers between commands are the
    // disclosed gap of this granularity.)
    if interp
        .lookup_function("undo-auto--undoable-change", env)
        .is_ok()
    {
        let current_changed = interp.current_buffer_id() == modified_before.0
            && interp.buffer.chars_modified_tick() != modified_before.1;
        let switched_and_changed = interp.current_buffer_id() != modified_before.0
            && interp
                .get_buffer_by_id(modified_before.0)
                .is_some_and(|buffer| buffer.chars_modified_tick() != modified_before.1);
        if current_changed {
            let _ = interp.call_function_value(
                Value::Symbol("undo-auto--undoable-change".into()),
                Some("undo-auto--undoable-change"),
                &[],
                env,
            );
        }
        if switched_and_changed {
            let here = interp.current_buffer_id();
            if interp.set_current_buffer_id(modified_before.0).is_ok() {
                let _ = interp.call_function_value(
                    Value::Symbol("undo-auto--undoable-change".into()),
                    Some("undo-auto--undoable-change"),
                    &[],
                    env,
                );
                let _ = interp.set_current_buffer_id(here);
            }
        }
    }
    let last_command = interp
        .lookup_var("this-command", env)
        .filter(|command| !command.is_nil())
        .unwrap_or(dispatched);
    interp.set_variable("last-command", last_command, env);
    // keyboard.c:1585: real-last-command takes real-this-command after
    // the command ran (execute-extended-command sets real-this-command
    // to the invoked function, and its suggestion timer compares
    // against real-last-command).
    let real_last = interp
        .lookup_var("real-this-command", env)
        .unwrap_or(Value::Nil);
    interp.set_variable("real-last-command", real_last, env);
    // keyboard.c takes last-prefix-arg from current-prefix-arg after the
    // command ran (command-execute moved prefix-arg there).
    let last_prefix = interp
        .lookup_var("current-prefix-arg", env)
        .unwrap_or(Value::Nil);
    interp.set_variable("last-prefix-arg", last_prefix, env);
    if result.is_ok()
        && interp
            .lookup_var("defining-kbd-macro", env)
            .is_some_and(|value| value.is_truthy())
    {
        interp.kbd_macro_committed_len = interp.kbd_macro_definition.len();
    }
    // keyboard.c:1421 (command_loop_1): before waiting for the next key
    // sequence, `this-command' and its shadows go nil -- Lisp that runs
    // between commands (idle timers; eldoc's `(not this-command)' guard)
    // must see no command executing.
    interp.set_variable("this-command", Value::Nil, env);
    interp.set_variable("real-this-command", Value::Nil, env);
    interp.set_variable("this-original-command", Value::Nil, env);
    result
}

// keyboard.c's timer_check in miniature: while the command loop waits
// for input, fire the ripe entries of `timer-list' (absolute times) and
// `timer-idle-list' (idle durations, once per idle period) through
// timer.el's own `timer-event-handler'.  The idle clock below backs
// `current-idle-time'; isearch's lazy highlight arrives this way.
thread_local! {
    static TTY_IDLE_START: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

// keyboard.c timer_start_idle: on entering the idle state (and only on
// entering it), every idle timer becomes a candidate again -- GNU calls
// timer.el's `internal-timer-start-idle' to clear the triggered flags,
// and so do we.  The recorded instant backs `current-idle-time'.
pub(crate) fn tty_note_idle_start(interp: &mut Interpreter, env: &mut Env) {
    let already_idle = TTY_IDLE_START.with(|cell| cell.get().is_some());
    if already_idle {
        return;
    }
    TTY_IDLE_START.with(|cell| cell.set(Some(std::time::Instant::now())));
    if interp
        .lookup_function("internal-timer-start-idle", env)
        .is_ok()
    {
        let _ = interp.call_function_value(
            Value::Symbol("internal-timer-start-idle".into()),
            Some("internal-timer-start-idle"),
            &[],
            env,
        );
    }
}

// keyboard.c timer_stop_idle: input ends the idle period.
pub(crate) fn tty_note_idle_end() {
    TTY_IDLE_START.with(|cell| cell.set(None));
}

// Fcurrent_idle_time's backing state: the elapsed idle span, when idle.
pub(crate) fn tty_current_idle_duration() -> Option<std::time::Duration> {
    TTY_IDLE_START
        .with(|cell| cell.get())
        .map(|start| start.elapsed())
}

pub(crate) fn run_due_timers(
    interp: &mut Interpreter,
    env: &mut Env,
    idle_seconds: f64,
) -> Result<bool, LispError> {
    if interp.lookup_function("timer-event-handler", env).is_err() {
        return Ok(false);
    }
    // keyboard.c's decode_timer reads the timer vector and compares its
    // exact timestamp against the C clock.  Going back through the Lisp
    // accessors here is observably wrong: applications are free to advise or
    // redefine `float-time' for presentation, and that must not make every
    // delayed timer immediately ripe.
    let decode_timer = |interp: &Interpreter, timer: &Value| {
        let slots = vector_items(timer).ok()?;
        // keyboard.c decode_timer: exactly ten slots, an untriggered
        // timer (vec[0] nil -- on BOTH timer lists), and a fixnum USECS
        // slot; anything else "is not a proper timer" and is skipped.
        if slots.len() != 10 || slots[0].is_truthy() || !matches!(slots[2], Value::Integer(_)) {
            return None;
        }
        exact_time_from_old_style(
            interp,
            &[
                slots[1].clone(),
                slots[2].clone(),
                slots[3].clone(),
                slots[8].clone(),
            ],
        )
        .ok()
    };
    let wall_now = current_time_value().ok();
    let idle_now = exact_time_from_float(idle_seconds).ok();
    let mut ran = false;
    for (list_name, idle) in [("timer-idle-list", true), ("timer-list", false)] {
        let Some(timers) = interp
            .lookup_var(list_name, env)
            .and_then(|value| value.to_vec().ok())
        else {
            continue;
        };
        for timer in timers {
            let Some(time) = decode_timer(interp, &timer) else {
                continue;
            };
            let due = if idle {
                idle_now
                    .as_ref()
                    .is_some_and(|now| !exact_time_less(now, &time))
            } else {
                wall_now
                    .as_ref()
                    .is_some_and(|now| !exact_time_less(now, &time))
            };
            if due {
                ran = true;
                interp.begin_timer_callback();
                let outcome = interp.call_function_value(
                    Value::Symbol("timer-event-handler".into()),
                    Some("timer-event-handler"),
                    std::slice::from_ref(&timer),
                    env,
                );
                interp.end_timer_callback();
                outcome?;
            }
        }
    }
    Ok(ran)
}

/// The echo-area text for a command's error, GNU's
/// `error-message-string' rendering ("Quit", "Beginning of buffer", a
/// user-error's own message).
pub(crate) fn command_error_echo_text(
    interp: &mut Interpreter,
    env: &mut Env,
    error: &LispError,
) -> String {
    let text = match error {
        LispError::SignalValue(data) => {
            let data = if matches!(data, Value::Symbol(_)) {
                Value::list([data.clone()])
            } else {
                data.clone()
            };
            command_loop_call(
                interp,
                env,
                "error-message-string",
                std::slice::from_ref(&data),
            )
            .ok()
            .and_then(|value| match value {
                Value::String(text) => Some(text.to_string()),
                _ => None,
            })
            .unwrap_or_else(|| format!("{data}"))
        }
        LispError::Signal(text) => text.clone(),
        other => format!("{other:?}"),
    };
    text.replace(['\n', '\r'], " ").chars().take(200).collect()
}

// Frame repaint, installed alongside the event reader.  Command code
// that runs its own event loop (the interactive minibuffer) calls it so
// window-configuration changes made mid-read — a *Completions* pop-up —
// reach the glass before the next key blocks.
thread_local! {
    static TTY_FRAME_REDRAW: std::cell::RefCell<Option<TtyFrameRedraw>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyFrameRedraw = Box<dyn FnMut(&mut Interpreter, &mut Env)>;

pub(crate) fn set_tty_frame_redraw(hook: Option<TtyFrameRedraw>) {
    TTY_FRAME_REDRAW.with_borrow_mut(|slot| *slot = hook);
}

/// Run the frontend's frame repaint if one is installed.  The hook is
/// taken out for the call: redisplay evaluates Lisp (mode lines), which
/// must not re-enter the hook cell.
pub(crate) fn run_tty_frame_redraw(interp: &mut Interpreter, env: &mut Env) {
    let hook = TTY_FRAME_REDRAW.with_borrow_mut(std::mem::take);
    if let Some(mut hook) = hook {
        hook(interp, env);
        TTY_FRAME_REDRAW.with_borrow_mut(|slot| {
            if slot.is_none() {
                *slot = Some(hook);
            }
        });
    }
}

pub(crate) fn pop_unread_command_event_value(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let unread = interp
        .lookup_var("unread-command-events", env)
        .unwrap_or(Value::Nil);
    let mut events = unread.to_vec()?;
    if events.is_empty() {
        run_pending_user_signal_events(interp, env)?;
        events = unread_command_events(interp, env)?;
        if !events.is_empty() {
            let event = events.remove(0);
            interp.set_variable("unread-command-events", Value::list(events), env);
            record_external_input_event(interp, &event, env);
            return Ok(event);
        }
        // GNU's input readers consume the executing keyboard macro's
        // remaining events (viper's `F'/`t' read their target char that way).
        // Lisp hooks may have rewound the public index after a speculative
        // read (kmacro's quoted-insert step editor does exactly this), so the
        // typed cursor must observe that assignment before supplying input.
        crate::lisp::primitives::dispatch::sync_kbd_macro_execution(interp, env)?;
        if let Some(state) = interp.kbd_macro_executions.last_mut()
            && let Some(event) = state.events.get(state.index).cloned()
        {
            state.index += 1;
            let index = state.index;
            interp.set_variable(
                "executing-kbd-macro-index",
                Value::Integer(index as i64),
                env,
            );
            return Ok(event);
        }
        // Poll for the event so ripe timers fire during the wait, as
        // GNU's read_char does; the blocking reader stands in when the
        // frontend installed no poller.
        let mut idle_start: Option<std::time::Instant> = None;
        let cursor_in_echo_area = interp
            .lookup_var("cursor-in-echo-area", env)
            .is_some_and(|value| value.is_truthy());
        while let Some(step) = poll_via_tty_event_poller(cursor_in_echo_area) {
            match step {
                None => return Err(LispError::SignalValue(Value::Symbol("quit".into()))),
                Some(Some(event)) => {
                    if event == Value::Integer(7) {
                        return Err(LispError::SignalValue(Value::Symbol("quit".into())));
                    }
                    record_external_input_event(interp, &event, env);
                    return Ok(event);
                }
                Some(None) => {
                    let idle = idle_start
                        .get_or_insert_with(std::time::Instant::now)
                        .elapsed();
                    let mut process_progress =
                        crate::lisp::primitives::processes::pump_external_process_output(
                            interp, env,
                        )?;
                    process_progress |=
                        crate::lisp::primitives::processes::pump_connection_processes(interp, env)?;
                    if process_progress
                        || interp.service_async_runtime_events(
                            env,
                            true,
                            Some(idle.as_secs_f64()),
                        )?
                    {
                        // A blocking Lisp reader owns the command thread, so
                        // the outer terminal loop cannot observe asynchronous
                        // work.  Redisplay here, as read_char does after
                        // wait_reading_process_output.
                        run_tty_frame_redraw(interp, env);
                    }
                    let mut events = unread_command_events(interp, env)?;
                    if !events.is_empty() {
                        let event = events.remove(0);
                        interp.set_variable("unread-command-events", Value::list(events), env);
                        record_external_input_event(interp, &event, env);
                        return Ok(event);
                    }
                }
            }
        }
        if let Some(read) = read_via_tty_event_reader(cursor_in_echo_area) {
            return match read {
                Some(event) => {
                    record_external_input_event(interp, &event, env);
                    Ok(event)
                }
                None => Err(LispError::SignalValue(Value::Symbol("quit".into()))),
            };
        }
        return Err(LispError::Signal(
            "No unread-command-events available for interactive input".into(),
        ));
    }
    let event = events.remove(0);
    interp.set_variable("unread-command-events", Value::list(events), env);
    record_external_input_event(interp, &event, env);
    Ok(event)
}

pub(crate) fn unread_command_event_char(event: &Value) -> Result<char, LispError> {
    translated_unread_event_char(event)
        .ok_or_else(|| LispError::Signal(format!("Invalid unread command event {}", event)))
}

pub(crate) fn normalize_input_event_value(event: Value) -> Result<Value, LispError> {
    if let Some(ch) = unread_event_char(&event) {
        Ok(Value::Integer(ch as i64))
    } else {
        Ok(event)
    }
}

pub(crate) fn normalize_key_event_value(event: Value) -> Result<Value, LispError> {
    if let Some(ch) = translated_unread_event_char(&event) {
        Ok(Value::Integer(ch as i64))
    } else {
        Ok(event)
    }
}

pub(crate) fn unread_command_events(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    interp
        .lookup_var("unread-command-events", env)
        .unwrap_or(Value::Nil)
        .to_vec()
}

pub(crate) fn unread_event_char(value: &Value) -> Option<char> {
    match value {
        Value::Integer(code) if *code >= 0 => modified_event_code_char(*code),
        Value::Cons(cell) if matches!(cell.car.borrow().clone(), Value::T) => {
            match cell.cdr.borrow().clone() {
                Value::Integer(code) if code >= 0 => modified_event_code_char(code),
                _ => None,
            }
        }
        Value::String(text) => text.chars().next(),
        Value::StringObject(state) => state.borrow().text.chars().next(),
        _ => None,
    }
}

/// GNU's default `local-function-key-map' translations.  Raw event readers
/// preserve these symbols, while key sequences, character readers, and
/// minibuffer command loops consume their translated character events.
pub(crate) fn function_key_default_translation(name: &str) -> Option<i64> {
    Some(match name {
        "escape" => 27,
        "tab" => 9,
        "return" => 13,
        "linefeed" => 10,
        "delete" | "backspace" => 127,
        _ => return None,
    })
}

pub(crate) fn translated_unread_event_char(value: &Value) -> Option<char> {
    match value {
        Value::Symbol(name) => {
            function_key_default_translation(name).and_then(|code| char::from_u32(code as u32))
        }
        _ => unread_event_char(value),
    }
}

fn modified_event_code_char(code: i64) -> Option<char> {
    const ALT: i64 = 1 << 22;
    const SUPER: i64 = 1 << 23;
    const HYPER: i64 = 1 << 24;
    const SHIFT: i64 = 1 << 25;
    const CONTROL: i64 = 1 << 26;
    const META: i64 = 1 << 27;
    const MODIFIERS: i64 = ALT | SUPER | HYPER | SHIFT | CONTROL | META;

    let mut base = code & !MODIFIERS;
    if code & CONTROL != 0 && base != 0 {
        base = match base {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    } else if code & SHIFT != 0
        && let Some(ch) = char::from_u32(base as u32)
    {
        base = ch.to_ascii_uppercase() as i64;
    }
    char::from_u32(base as u32)
}

pub(crate) fn unread_prefix_matches(events: &[Value], prefix: &str) -> Option<usize> {
    let chars: Vec<char> = prefix.chars().collect();
    if events.len() < chars.len() {
        return None;
    }
    for (index, expected) in chars.iter().enumerate() {
        if unread_event_char(&events[index]) != Some(*expected) {
            return None;
        }
    }
    Some(chars.len())
}

pub(crate) fn prepend_unread_command_events(
    interp: &mut Interpreter,
    env: &mut Env,
    mut prefix: Vec<Value>,
) -> Result<(), LispError> {
    let unread = unread_command_events(interp, env)?;
    prefix.extend(unread);
    interp.set_variable("unread-command-events", Value::list(prefix), env);
    Ok(())
}

pub(crate) fn translated_input_events(value: &Value) -> Result<Vec<Value>, LispError> {
    if matches!(value, Value::Nil) {
        return Ok(Vec::new());
    }
    if let Ok(items) = vector_items(value) {
        return Ok(items);
    }
    Ok(vec![value.clone()])
}

pub(crate) fn is_mouse_down_event(value: &Value) -> bool {
    value
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|item| item.as_symbol().ok().map(str::to_string))
        .is_some_and(|name| name.contains("down-mouse"))
}

pub(crate) fn read_decoded_input_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let unread = unread_command_events(interp, env)?;
    for (prefix, function_name) in [
        ("\u{1b}[<", "xterm-mouse-translate-extended"),
        ("\u{1b}[M", "xterm-mouse-translate"),
    ] {
        let Some(prefix_len) = unread_prefix_matches(&unread, prefix) else {
            continue;
        };
        let Ok(function) = interp.lookup_function(function_name, env) else {
            continue;
        };

        let mut remaining = unread.clone();
        remaining.drain(0..prefix_len);
        interp.set_variable("unread-command-events", Value::list(remaining), env);

        let translated =
            interp.call_function_value(function, Some(function_name), &[Value::Nil], env)?;
        let events = translated_input_events(&translated)?;
        if events.is_empty() {
            return Ok(None);
        }
        if events.len() > 1 {
            prepend_unread_command_events(interp, env, events[1..].to_vec())?;
        }
        return Ok(events.into_iter().next());
    }

    let Some(input_decode_map) = interp.lookup_var("input-decode-map", env) else {
        return Ok(None);
    };
    let mut best_match: Option<(usize, Value)> = None;
    let mut prefix = String::new();
    for event in unread.iter().take(8) {
        let Some(ch) = unread_event_char(event) else {
            break;
        };
        prefix.push(ch);
        let binding = keymap_lookup_binding(interp, &input_decode_map, &prefix)?;
        if !binding.is_nil()
            && best_match
                .as_ref()
                .is_none_or(|(best_len, _)| prefix.chars().count() > *best_len)
        {
            best_match = Some((prefix.chars().count(), binding));
        }
    }
    let Some((prefix_len, binding)) = best_match else {
        return Ok(None);
    };

    let mut remaining = unread;
    remaining.drain(0..prefix_len);
    interp.set_variable("unread-command-events", Value::list(remaining), env);

    let function = resolve_callable(interp, &binding, env)?;
    let translated = invoke_function_value(interp, &function, &[Value::Nil], env)?;
    let events = translated_input_events(&translated)?;
    if events.is_empty() {
        return Ok(None);
    }
    if events.len() > 1 {
        prepend_unread_command_events(interp, env, events[1..].to_vec())?;
    }
    Ok(events.into_iter().next())
}

pub(crate) fn input_event_symbol(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(symbol) => Some(symbol.to_string()),
        Value::Cons(_) => value
            .to_vec()
            .ok()
            .and_then(|items| items.first().cloned())
            .and_then(|item| item.as_symbol().ok().map(str::to_string)),
        _ => None,
    }
}

pub(crate) fn update_input_event_symbol(value: &Value, symbol: &str) -> Value {
    match value {
        Value::Symbol(_) => Value::Symbol(symbol.into()),
        Value::Cons(_) => {
            let mut items = value.to_vec().unwrap_or_default();
            if let Some(first) = items.first_mut() {
                *first = Value::Symbol(symbol.into());
            }
            Value::list(items)
        }
        _ => value.clone(),
    }
}

pub(crate) fn first_input_from_link_action(value: &Value) -> Option<Value> {
    if let Some(string) = string_like(value) {
        return string
            .text
            .chars()
            .next()
            .map(|ch| Value::Integer(ch as i64));
    }

    vector_items(value).ok()?.into_iter().next()
}

pub(crate) fn translate_mouse_read_key_sequence_event(
    interp: &mut Interpreter,
    event: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.set_variable("last-input-event", event.clone(), env);

    if input_event_symbol(&event).as_deref() != Some("mouse-1") {
        return Ok(event);
    }
    if !interp
        .lookup_var("mouse-1-click-follows-link", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(event);
    }

    let action = match interp.lookup_function("mouse-on-link-p", env) {
        Ok(function) => {
            interp.call_function_value(function, Some("mouse-on-link-p"), &[Value::Nil], env)?
        }
        Err(_) => Value::Nil,
    };

    if let Some(first) = first_input_from_link_action(&action) {
        interp.set_variable("last-input-event", first.clone(), env);
        return Ok(first);
    }
    if action.is_truthy() {
        let translated = update_input_event_symbol(&event, "mouse-2");
        interp.set_variable("last-input-event", translated.clone(), env);
        return Ok(translated);
    }

    Ok(event)
}

pub(crate) fn read_key_sequence_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    loop {
        let event = if let Some(decoded) = read_decoded_input_event(interp, env)? {
            decoded
        } else {
            normalize_key_event_value(pop_unread_command_event_value(interp, env)?)?
        };
        if is_mouse_down_event(&event) {
            let event_name =
                input_event_symbol(&event).expect("mouse-down events have a symbolic head");
            let key_parts = vec![event_name];
            let mut binding = Value::Nil;
            for map in active_command_keymaps(interp, env)? {
                binding = keymap_lookup_sequence_value_with_default(
                    interp, &map, &key_parts, false, env,
                )?;
                if !binding.is_nil() {
                    break;
                }
            }
            // bindings.el installs this in GNU's dumped global map.  Keep
            // the file-less bootstrap fallback aligned without treating all
            // mouse-down events as bound.
            if binding.is_nil() && key_parts == ["down-mouse-1"] {
                binding = Value::Symbol("mouse-drag-region".into());
            }
            if binding.is_nil() {
                continue;
            }
        }
        return translate_mouse_read_key_sequence_event(interp, event, env);
    }
}

pub(crate) fn record_command_history(
    interp: &mut Interpreter,
    function_name: &str,
    args: Vec<Value>,
    env: &mut Env,
) {
    let mut history = interp
        .lookup_var("command-history", env)
        .unwrap_or(Value::Nil)
        .to_vec()
        .unwrap_or_default();
    let mut entry = vec![Value::Symbol(function_name.to_string().into())];
    entry.extend(args);
    history.insert(0, Value::list(entry));
    if let Some(Value::Integer(length)) = interp.lookup_var("history-length", env) {
        let length = length.max(0) as usize;
        history.truncate(length);
    }
    interp.set_variable("command-history", Value::list(history), env);
}

pub(crate) fn is_declare_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "declare"),
    )
}

pub(crate) fn invalid_interactive_control_letter(ch: char) -> LispError {
    let code = raw_byte_from_regex_char(ch)
        .map(u32::from)
        .unwrap_or(ch as u32);
    let display = char::from_u32(code).unwrap_or(ch);
    LispError::Signal(format!(
        "Invalid control letter `{display}' (#o{code:03o}, #x{code:04x}) in interactive calling string"
    ))
}

pub(crate) fn need_args(name: &str, args: &[Value], n: usize) -> Result<(), LispError> {
    if args.len() < n {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

pub(crate) fn need_arg_range(
    name: &str,
    args: &[Value],
    min: usize,
    max: usize,
) -> Result<(), LispError> {
    if args.len() < min || args.len() > max {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

/// keyboard.c's menu_bar_items: the menu bar's top-level captions in
/// display order.  Every active keymap's `menu-bar' prefix is scanned
/// lowest-precedence first (global map, then local, then minor modes),
/// so global menus lead the row and higher-precedence maps append or
/// merge into existing entries; the keys named by
/// `menu-bar-final-items' move to the end (Help).
pub(crate) fn menu_bar_row_captions(interp: &mut Interpreter, env: &mut Env) -> Vec<String> {
    menu_bar_row_items(interp, env)
        .into_iter()
        .map(|(caption, _, _)| caption)
        .collect()
}

/// The menu bar's items with their display geometry: (CAPTION, KEY,
/// COLUMN), column being where the caption starts on the row — the
/// coordinates menu.c's menu-bar-menu-at-x-y answers from.
pub(crate) fn menu_bar_row_items(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Vec<(String, Value, usize)> {
    let maps = super::call(interp, "current-active-maps", &[Value::T], env)
        .ok()
        .and_then(|maps| maps.to_vec().ok())
        .unwrap_or_default();
    let menu_bar_key = Value::list([
        Value::Symbol("vector-literal".into()),
        Value::Symbol("menu-bar".into()),
    ]);
    let same_key = |a: &Value, b: &Value| match (a, b) {
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        _ => false,
    };
    let mut items: Vec<(Value, String)> = Vec::new();
    for map in maps.iter().rev() {
        // keymap.c access_keymap merges every `menu-bar' submap bound
        // along the map's parent chain into one `(keymap CHILD PARENT)'
        // reference, so a mode map's own menus and its parent's (shell's
        // Complete over comint's In/Out and Signals) all reach the bar.
        // Enumerate the chain child-first; a level without its own
        // binding answers its parent's submap, which the identity dedup
        // below drops.
        let mut chain_menus: Vec<Value> = Vec::new();
        let mut level = map.clone();
        for _ in 0..32 {
            if let Ok(menu) = super::call(
                interp,
                "lookup-key",
                &[level.clone(), menu_bar_key.clone()],
                env,
            ) && super::is_keymap_value(interp, &menu)
            {
                let identity = super::keymap_record_id(interp, &menu);
                let duplicate = chain_menus.iter().any(|earlier| {
                    match (identity, super::keymap_record_id(interp, earlier)) {
                        (Some(a), Some(b)) => a == b,
                        _ => false,
                    }
                });
                if !duplicate {
                    chain_menus.push(menu);
                }
            }
            match super::call(interp, "keymap-parent", &[level.clone()], env) {
                Ok(parent) if parent.is_truthy() => level = parent,
                _ => break,
            }
        }
        // One keymap contributes to a key only once across its chain,
        // even when its entry list carries shadowed duplicates.
        let mut seen: Vec<Value> = Vec::new();
        for menu in chain_menus {
            // A runtime keymap answers as its record identity; walk GNU's
            // public `(keymap ...)' cons projection of it.
            let menu = {
                if let Some(id) = super::keymap_record_id(interp, &menu) {
                    let _ = super::refresh_runtime_keymap_public_view(interp, id);
                }
                super::public_keymap_value(interp, &menu)
            };
            if !matches!(menu.car(), Ok(Value::Symbol(tag)) if tag == "keymap") {
                continue;
            }
            let mut tail = menu.cdr().unwrap_or(Value::Nil);
            while let Value::Cons(_) = tail {
                let Ok(entry) = tail.car() else { break };
                let next = tail.cdr().unwrap_or(Value::Nil);
                match &entry {
                    // A parent keymap's entries follow through the tail.
                    Value::Symbol(tag) if tag == "keymap" => {}
                    Value::Cons(_) => {
                        let key = entry.car().unwrap_or(Value::Nil);
                        let item = entry.cdr().unwrap_or(Value::Nil);
                        if !seen.iter().any(|earlier| same_key(earlier, &key)) {
                            seen.push(key.clone());
                            if matches!(&item, Value::Symbol(def) if def == "undefined") {
                                // An explicit `undefined' discards any
                                // previously made item for this key.
                                items.retain(|(existing, _)| !same_key(existing, &key));
                            } else if let Some(caption) = menu_item_caption(interp, env, &item)
                                && !items.iter().any(|(existing, _)| same_key(existing, &key))
                            {
                                items.push((key, caption));
                            }
                        }
                    }
                    _ => {}
                }
                tail = next;
            }
        }
    }
    if let Some(final_items) = interp
        .lookup_var("menu-bar-final-items", env)
        .and_then(|value| value.to_vec().ok())
    {
        for name in final_items {
            if let Some(position) = items.iter().position(|(key, _)| same_key(key, &name)) {
                let item = items.remove(position);
                items.push(item);
            }
        }
    }
    let mut column = 0usize;
    items
        .into_iter()
        .map(|(key, caption)| {
            let start = column;
            column += caption.chars().count() + 1;
            (caption, key, start)
        })
        .collect()
}

/// parse_menu_item for the menu bar: the caption of a live top-level
/// item, or None when the item is invisible, disabled, undefined, or
/// not a menu item at all.
fn menu_item_caption(interp: &mut Interpreter, env: &mut Env, item: &Value) -> Option<String> {
    let (caption, def, enabled) = menu_item_details(interp, env, item)?;
    // The menu bar drops disabled and definition-less items
    // (parse_menu_item's inmenubar rules); a dropdown keeps them greyed.
    (enabled && !def.is_nil()).then_some(caption)
}

/// parse_menu_item for a dropdown pane: (CAPTION, DEF, ENABLED) of a
/// visible item — disabled and unselectable entries stay, drawn in
/// tty-menu-disabled-face.  None only for invisible or non-items.
pub(crate) fn menu_item_details(
    interp: &mut Interpreter,
    env: &mut Env,
    item: &Value,
) -> Option<(String, Value, bool)> {
    menu_item_details_with_button(interp, env, item).map(|(c, d, e, _)| (c, d, e))
}

/// A parsed pane item: (CAPTION, DEF, ENABLED, BUTTON), button being
/// the :button spec's (TYPE-KEYWORD, SELECTED) for toggle/radio items —
/// menu.c's checkbox prefix source.
pub(crate) type MenuItemDetails = (String, Value, bool, Option<(String, bool)>);

/// menu_item_details plus the :button spec.
pub(crate) fn menu_item_details_with_button(
    interp: &mut Interpreter,
    env: &mut Env,
    item: &Value,
) -> Option<MenuItemDetails> {
    if !matches!(item, Value::Cons(_)) {
        return None;
    }
    let eval_property = |interp: &mut Interpreter, env: &mut Env, form: &Value| match form {
        Value::Symbol(name) if name != "t" && name != "nil" => {
            interp.lookup_var(name, env).unwrap_or(Value::Nil)
        }
        Value::Cons(_) => interp.eval(form, env).unwrap_or(Value::Nil),
        other => other.clone(),
    };
    let car = item.car().ok()?;
    if let Ok(name) = crate::lisp::primitives::string_text(&car) {
        // Old format (NAME [HELP-STRING] [CACHE] . DEF).
        let mut def = item.cdr().ok()?;
        if def.car().is_ok_and(|help| help.is_string()) {
            def = def.cdr().ok()?;
        }
        if def.car().is_ok_and(|cache| {
            matches!(cache.car(), Ok(Value::Nil))
                || matches!(cache.car(), Ok(Value::Symbol(tag)) if tag == "vector-literal")
        }) {
            def = def.cdr().ok()?;
        }
        // Unselectable text (a separator) still draws in the enabled
        // face; only the menu bar drops definition-less items.
        return Some((name, def, true, None));
    }
    if !matches!(&car, Value::Symbol(tag) if tag == "menu-item") {
        return None;
    }
    // New format (menu-item NAME DEF [CACHE] . PROPS).
    let rest = item.cdr().ok()?.to_vec().ok()?;
    let name_form = rest.first()?.clone();
    let mut def = rest.get(1).cloned().unwrap_or(Value::Nil);
    let mut index = 2;
    if matches!(rest.get(2), Some(Value::Cons(_))) {
        index = 3;
    }
    let mut filter = None;
    let mut enabled = true;
    let mut button = None;
    while index + 1 < rest.len() {
        let Value::Symbol(keyword) = &rest[index] else {
            break;
        };
        let value = &rest[index + 1];
        match keyword.as_ref() {
            ":visible" => {
                if eval_property(interp, env, value).is_nil() {
                    return None;
                }
            }
            ":enable" => {
                if eval_property(interp, env, value).is_nil() {
                    enabled = false;
                }
            }
            ":filter" => filter = Some(value.clone()),
            ":button" => {
                if let (Ok(Value::Symbol(kind)), Ok(selected)) = (value.car(), value.cdr()) {
                    let selected = eval_property(interp, env, &selected).is_truthy();
                    button = Some((kind.to_string(), selected));
                }
            }
            _ => {}
        }
        index += 2;
    }
    if let Some(filter) = filter {
        def = interp
            .call_function_value(filter, None, std::slice::from_ref(&def), env)
            .unwrap_or(Value::Nil);
    }
    let name = eval_property(interp, env, &name_form);
    let caption = crate::lisp::primitives::string_text(&name).ok()?;
    Some((caption, def, enabled, button))
}

/// The menu bar's cheap per-redraw change signal: how many minor-mode
/// maps are live plus whether an overriding map is installed.  Entering
/// isearch (or any minor mode with a map) changes it, which triggers
/// the recompute GNU gets from update_mode_lines — without walking the
/// full active-keymap set on every keystroke.
pub(crate) fn active_keymap_count(interp: &mut Interpreter, env: &mut Env) -> usize {
    let minors = interp
        .lookup_var("minor-mode-map-alist", env)
        .and_then(|alist| alist.to_vec().ok())
        .map(|entries| {
            entries
                .iter()
                .filter(|entry| {
                    entry.car().is_ok_and(|mode| {
                        matches!(&mode, Value::Symbol(name)
                            if interp.lookup_var(name, env).is_some_and(|on| on.is_truthy()))
                    })
                })
                .count()
        })
        .unwrap_or(0);
    let overriding = interp
        .lookup_var("overriding-terminal-local-map", env)
        .is_some_and(|map| map.is_truthy());
    minors + usize::from(overriding)
}

/// Pop the first `unread-command-events' entry, unwrapping GNU's
/// `(t . EVENT)' don't-re-record form — read_char's front of the input
/// stream, consulted before the terminal.
pub(crate) fn take_unread_command_event(interp: &mut Interpreter, env: &mut Env) -> Option<Value> {
    let events = interp.lookup_var("unread-command-events", env)?;
    let mut events = events.to_vec().ok()?;
    if events.is_empty() {
        return None;
    }
    let event = events.remove(0);
    interp.set_variable("unread-command-events", Value::list(events), env);
    match &event {
        Value::Cons(_) if matches!(event.car(), Ok(Value::T)) => event.cdr().ok(),
        _ => Some(event),
    }
}

// Whether terminal input is ready without consuming it — keyboard.c's
// input_pending, for `input-pending-p' and sit-for's early exit.
thread_local! {
    static TTY_INPUT_PENDING: std::cell::RefCell<Option<Box<dyn Fn() -> bool>>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_tty_input_pending_check(check: Option<Box<dyn Fn() -> bool>>) {
    TTY_INPUT_PENDING.with_borrow_mut(|slot| *slot = check);
}

pub(crate) fn tty_input_pending() -> bool {
    TTY_INPUT_PENDING.with_borrow(|slot| slot.as_ref().is_some_and(|check| check()))
}

pub(crate) fn has_tty_event_poller() -> bool {
    TTY_EVENT_POLLER.with_borrow(|slot| slot.is_some())
}

/// A timed event read on the live terminal: the first event within
/// TIMEOUT, or None when it elapses quietly.  Ripe timers fire between
/// polls, exactly like the untimed read.
pub(crate) fn read_tty_event_with_timeout(
    interp: &mut Interpreter,
    env: &mut Env,
    timeout: std::time::Duration,
) -> Result<Option<Value>, LispError> {
    let deadline = std::time::Instant::now() + timeout;
    let started = std::time::Instant::now();
    loop {
        if let Some(event) = take_unread_command_event(interp, env) {
            record_external_input_event(interp, &event, env);
            return Ok(Some(event));
        }
        // A timed read is still wait_reading_process_output with keyboard
        // input in the descriptor set.  In particular, subr.el's `sit-for'
        // is the cancel-on-input wait used by jsonrpc/Eglot completion: a
        // subprocess reply and the zero-delay continuation its filter
        // schedules must run even when no key arrives.
        let mut process_progress =
            crate::lisp::primitives::processes::pump_external_process_output(interp, env)?;
        process_progress |=
            crate::lisp::primitives::processes::pump_connection_processes(interp, env)?;
        let async_progress = interp.service_async_runtime_events(
            env,
            true,
            Some(started.elapsed().as_secs_f64()),
        )?;
        if process_progress || async_progress {
            run_tty_frame_redraw(interp, env);
        }
        let cursor_in_echo_area = interp
            .lookup_var("cursor-in-echo-area", env)
            .is_some_and(|value| value.is_truthy());
        let Some(step) = poll_via_tty_event_poller(cursor_in_echo_area) else {
            return Ok(None);
        };
        match step {
            None => return Err(LispError::SignalValue(Value::Symbol("quit".into()))),
            Some(Some(event)) => {
                if event == Value::Integer(7) {
                    return Err(LispError::SignalValue(Value::Symbol("quit".into())));
                }
                record_external_input_event(interp, &event, env);
                return Ok(Some(event));
            }
            Some(None) => {
                if std::time::Instant::now() >= deadline {
                    return Ok(None);
                }
            }
        }
    }
}

/// One dropdown pane ready for the glass: term.c's tty_menu built from
/// a menu keymap, item text already NAME-padded-plus-key-hint as
/// tty_menu_show lays it out.
pub(crate) struct TtyMenuPane {
    pub title: String,
    pub items: Vec<TtyMenuPaneItem>,
    /// Pane text width (the widest NAME+DESCRIP); the drawn box adds
    /// the two padding blanks.
    pub width: usize,
}

pub(crate) struct TtyMenuPaneItem {
    pub text: String,
    pub enabled: bool,
    pub key: Value,
}

pub(crate) enum TtyMenuOutcome {
    Selected(usize),
    NextMenu,
    PrevMenu,
    /// A separator or disabled item was chosen: no selection, no quit
    /// signal (GNU's TTYM_IA_SELECT).
    NoSelect,
    Quit,
}

// The frontend's modal dropdown executor — term.c's tty_menu_activate.
thread_local! {
    static TTY_MENU_EXECUTOR: std::cell::RefCell<Option<TtyMenuExecutor>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyMenuExecutor =
    Box<dyn FnMut(&mut Interpreter, &mut Env, &TtyMenuPane, usize, usize) -> TtyMenuOutcome>;

pub(crate) fn set_tty_menu_executor(executor: Option<TtyMenuExecutor>) {
    TTY_MENU_EXECUTOR.with_borrow_mut(|slot| *slot = executor);
}

pub(crate) fn has_tty_menu_executor() -> bool {
    TTY_MENU_EXECUTOR.with_borrow(|slot| slot.is_some())
}

pub(crate) fn run_tty_menu_executor(
    interp: &mut Interpreter,
    env: &mut Env,
    pane: &TtyMenuPane,
    x: usize,
    y: usize,
) -> Option<TtyMenuOutcome> {
    let executor = TTY_MENU_EXECUTOR.with_borrow_mut(|slot| slot.take());
    let mut executor = executor?;
    let outcome = executor(interp, env, pane, x, y);
    TTY_MENU_EXECUTOR.with_borrow_mut(|slot| {
        if slot.is_none() {
            *slot = Some(executor);
        }
    });
    Some(outcome)
}

/// tty_menu_show's pane construction: walk MENU's entries in keymap
/// order, item text = NAME padded to the widest name + "  KEY-HINT"
/// (parse_menu_item's equivalent-key description).
pub(crate) fn tty_menu_pane_from_keymap(
    interp: &mut Interpreter,
    env: &mut Env,
    menu: &Value,
    title: &str,
) -> TtyMenuPane {
    let menu = {
        if let Some(id) = super::keymap_record_id(interp, menu) {
            let _ = super::refresh_runtime_keymap_public_view(interp, id);
        }
        super::public_keymap_value(interp, menu)
    };
    #[allow(clippy::type_complexity)]
    let mut raw: Vec<(Value, String, Value, bool, Option<(String, bool)>)> = Vec::new();
    let mut tail = menu.cdr().unwrap_or(Value::Nil);
    let mut seen: Vec<Value> = Vec::new();
    let same_key = |a: &Value, b: &Value| match (a, b) {
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        _ => false,
    };
    while let Value::Cons(_) = tail {
        let Ok(entry) = tail.car() else { break };
        let next = tail.cdr().unwrap_or(Value::Nil);
        if let Value::Cons(_) = &entry {
            let key = entry.car().unwrap_or(Value::Nil);
            let item = entry.cdr().unwrap_or(Value::Nil);
            if !seen.iter().any(|earlier| same_key(earlier, &key)) {
                seen.push(key.clone());
                if let Some((caption, def, enabled, button)) =
                    menu_item_details_with_button(interp, env, &item)
                {
                    // A tty submenu item carries GNU's " >" marker,
                    // counted by the pane's width scan.
                    let caption = if super::is_keymap_value(interp, &def)
                        || matches!(&def, Value::Symbol(name)
                            if interp.lookup_function(name, env)
                                .is_ok_and(|f| super::is_keymap_value(interp, &f)))
                    {
                        format!("{caption} >")
                    } else {
                        caption
                    };
                    raw.push((key, caption, def, enabled, button));
                }
            }
        }
        tail = next;
    }
    // menu.c's checkbox column: once any item in the pane is a
    // toggle/radio button, every other item gains a four-blank prefix
    // so captions line up with "[X] " — separators and empty names
    // excepted.  A buttonless pane keeps bare captions (Edit).
    let has_buttons = raw.iter().any(|(_, _, _, _, button)| button.is_some());
    let raw: Vec<(Value, String, Value, bool)> = raw
        .into_iter()
        .map(|(key, caption, def, enabled, button)| {
            let prefixed = match button {
                Some((kind, selected)) => {
                    let mark = match (kind.as_str(), selected) {
                        (":radio", true) => "(*) ",
                        (":radio", false) => "( ) ",
                        (_, true) => "[X] ",
                        (_, false) => "[ ] ",
                    };
                    format!("{mark}{caption}")
                }
                None if has_buttons && !caption.is_empty() && !caption.starts_with('-') => {
                    format!("    {caption}")
                }
                None => caption,
            };
            (key, prefixed, def, enabled)
        })
        .collect();
    // The widest NAME sets the padding column for every key hint.
    let max_name = raw
        .iter()
        .map(|(_, caption, _, _)| caption.chars().count())
        .max()
        .unwrap_or(0);
    let mut items = Vec::new();
    let mut width = 0usize;
    for (key, caption, def, enabled) in raw {
        // parse_menu_item's equivalent-key hint: the first non-menu
        // binding of the command, through the real where-is machinery
        // (a [menu-bar ...] or [open]-style menu path is not a key).
        let hint = if matches!(&def, Value::Symbol(_)) {
            super::call(interp, "where-is-internal", std::slice::from_ref(&def), env)
                .ok()
                .and_then(|keys| keys.to_vec().ok())
                .and_then(|keys| {
                    // GNU prefers a typed key sequence (its where-is
                    // sorts ASCII sequences first) and never shows a
                    // menu path as the equivalent key.
                    let event_kinds = |key: &Value| {
                        key.to_vec()
                            .ok()
                            .map(|events| events.iter().skip(1).cloned().collect::<Vec<_>>())
                            .unwrap_or_default()
                    };
                    let is_menu_path = |key: &Value| {
                        matches!(
                            event_kinds(key).first(),
                            Some(Value::Symbol(head))
                                if head == "menu-bar"
                                    || head == "tool-bar"
                                    || head == "tab-bar"
                                    || head == "mode-line"
                        )
                    };
                    let typed = keys.iter().find(|key| {
                        event_kinds(key)
                            .first()
                            .is_some_and(|event| matches!(event, Value::Integer(_)))
                    });
                    typed
                        .or_else(|| keys.iter().find(|key| !is_menu_path(key)))
                        .cloned()
                })
                .and_then(|key| {
                    super::call(interp, "key-description", &[key], env)
                        .ok()
                        .and_then(|description| {
                            crate::lisp::primitives::string_text(&description).ok()
                        })
                })
        } else {
            None
        };
        let text = match hint {
            Some(hint) => {
                let mut text = caption.clone();
                for _ in caption.chars().count()..max_name {
                    text.push(' ');
                }
                text.push_str("  ");
                text.push_str(&hint);
                text
            }
            None => caption,
        };
        width = width.max(text.chars().count());
        items.push(TtyMenuPaneItem { text, enabled, key });
    }
    TtyMenuPane {
        title: title.to_string(),
        items,
        width,
    }
}
