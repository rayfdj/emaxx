use super::*;

pub(crate) fn set_command_key_state(
    interp: &mut Interpreter,
    keys: Vec<Value>,
    raw_keys: Vec<Value>,
    env: &mut Env,
) {
    interp.keyboard_input.command_keys = keys.clone();
    interp.keyboard_input.single_command_start = 0;
    interp.keyboard_input.raw_keys = raw_keys;
    interp.set_variable(
        "this-single-command-keys",
        Value::list(std::iter::once(Value::symbol("vector-literal")).chain(keys)),
        env,
    );
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

fn record_external_input_event(interp: &mut Interpreter, event: &Value) {
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
    if let Value::Record(id) = value
        && let Some(record) = interp.find_record(id)
        && record.kind == crate::lisp::eval::RecordKind::Closure
    {
        return record.slots.get(4).filter(|doc| !doc.is_nil()).cloned();
    }
    let Value::Lambda(lambda) = value else {
        return None;
    };
    lambda.body.iter().find_map(|form| match form {
        Value::String(text) => Some(Value::String(text.clone())),
        Value::StringObject(state) => Some(Value::String(state.borrow().text.clone().into())),
        _ => None,
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

pub(crate) fn is_lambda_value(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "lambda"),
    )
}

pub(crate) fn validate_lambda_params(params: &Value) -> Result<(), LispError> {
    let items = params.to_vec()?;
    validate_lambda_list_items(params, &items)
}

pub(crate) fn validate_lambda_form(form: &Value) -> Result<(), LispError> {
    let items = form.to_vec()?;
    let Some(params) = items.get(1) else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("invalid-function".into()),
            form.clone(),
        ])));
    };
    validate_lambda_params(params)
}

pub(crate) fn validate_lambda_list_items(spec: &Value, items: &[Value]) -> Result<(), LispError> {
    let invalid = || {
        LispError::SignalValue(Value::list([
            Value::Symbol("invalid-function".into()),
            spec.clone(),
        ]))
    };
    let mut seen_optional = false;
    let mut seen_rest = false;
    let mut needs_rest_arg = false;
    let mut rest_arg_seen = false;

    for item in items {
        let Value::Symbol(symbol) = item else {
            return Err(invalid());
        };
        match symbol.as_str() {
            "&optional" => {
                if seen_optional || seen_rest {
                    return Err(invalid());
                }
                seen_optional = true;
            }
            "&rest" => {
                if seen_rest {
                    return Err(invalid());
                }
                seen_rest = true;
                needs_rest_arg = true;
            }
            _ => {
                if needs_rest_arg {
                    needs_rest_arg = false;
                    rest_arg_seen = true;
                } else if rest_arg_seen {
                    return Err(invalid());
                }
            }
        }
    }

    if needs_rest_arg {
        return Err(invalid());
    }

    Ok(())
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
    let mut values = Vec::new();
    for line in spec.split('\n') {
        if line.is_empty() {
            continue;
        }
        let mut chars = line.chars().skip_while(|ch| matches!(ch, '*' | '@' | '^'));
        let Some(code) = chars.next() else {
            continue;
        };
        match code {
            'k' => {
                let ch = unread_command_event_char(&pop_unread_command_event_value(interp, env)?)?;
                values.push(Value::String(ch.to_string().into()));
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
            'N' => {
                let prefix = interp
                    .lookup_var("current-prefix-arg", env)
                    .unwrap_or(Value::Nil);
                if prefix.is_truthy() {
                    values.push(prefix_numeric_value(&prefix)?);
                } else {
                    let prompt = chars.collect::<String>();
                    values.push(interp.call_function_value(
                        Value::Symbol("read-number".into()),
                        Some("read-number"),
                        &[Value::String(prompt.into())],
                        env,
                    )?);
                }
            }
            _ => return Err(invalid_interactive_control_letter(code)),
        }
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

pub(crate) fn set_tty_event_reader(reader: Option<TtyEventReader>) {
    TTY_EVENT_READER.with_borrow_mut(|slot| *slot = reader);
}

fn read_via_tty_event_reader() -> Option<Option<Value>> {
    TTY_EVENT_READER.with_borrow_mut(|slot| slot.as_mut().map(|reader| reader()))
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

/// Resolve a pending key sequence through the runtime's own keymaps
/// (`key-binding'), classifying strict prefixes so a multi-key sequence
/// keeps reading.  This is the single resolution path for every command
/// loop — the frame's and the minibuffer's recursive one.
pub(crate) fn resolve_key_sequence(
    interp: &mut Interpreter,
    env: &mut Env,
    pending: &[Value],
) -> KeyResolution {
    let key_vector = Value::list(
        std::iter::once(Value::Symbol("vector-literal".into())).chain(pending.iter().cloned()),
    );
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
    // GNU's command loop separates each command into its own undo group
    // (undo-auto--add-boundary after every command); `undo' relies on the
    // boundary to skip before replaying the previous group.
    interp.buffer.push_undo_boundary();
    // A lingering echo-area message belongs to the previous command; GNU
    // clears it when the next command runs (its own `message' then shows).
    crate::lisp::primitives::set_echo_area_message(None);
    interp.set_variable("last-command-event", last_event, env);
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
    // GNU's command loop hands the accumulated prefix to the command:
    // current-prefix-arg takes prefix-arg's value and prefix-arg clears
    // before the call; last-prefix-arg keeps it for the next cycle.
    let prefix = interp.lookup_var("prefix-arg", env).unwrap_or(Value::Nil);
    interp.set_variable("current-prefix-arg", prefix.clone(), env);
    interp.set_variable("prefix-arg", Value::Nil, env);
    // pre-command-hook may rewrite `this-command' (isearch's exit path
    // does); GNU executes whatever the hook left there.
    let buffer_id = interp.current_buffer_id();
    crate::lisp::primitives::safe_run_named_hooks(interp, "pre-command-hook", env, Some(buffer_id))
        .unwrap_or(());
    let dispatched = interp
        .lookup_var("this-command", env)
        .filter(|command| !command.is_nil())
        .unwrap_or_else(|| binding.clone());
    // GNU's command_execute is a thin wrapper over call-interactively
    // (prefix-arg bookkeeping, kbd-macro expansion); the runtime does not
    // define it yet, so drive the interactive call directly.
    let result = command_loop_call(
        interp,
        env,
        "call-interactively",
        std::slice::from_ref(&dispatched),
    )
    .map(|_| ());
    let buffer_id = interp.current_buffer_id();
    crate::lisp::primitives::safe_run_named_hooks(
        interp,
        "post-command-hook",
        env,
        Some(buffer_id),
    )
    .unwrap_or(());
    // GNU takes last-command from this-command AFTER the command ran: a
    // prefix command (universal-argument) restores the previous value
    // there via prefix-command-preserve-state, keeping last-command
    // stable across the C-u chain.
    let last_command = interp
        .lookup_var("this-command", env)
        .filter(|command| !command.is_nil())
        .unwrap_or(dispatched);
    interp.set_variable("last-command", last_command, env);
    interp.set_variable("last-prefix-arg", prefix, env);
    result
}

/// keyboard.c's timer_check in miniature: while the command loop waits
/// for input, fire the ripe entries of `timer-list' (absolute times) and
/// `timer-idle-list' (idle durations, once per idle period) through
/// timer.el's own `timer-event-handler'.  Returns whether any ran —
/// isearch's lazy highlight arrives this way.
pub(crate) fn run_due_timers(interp: &mut Interpreter, env: &mut Env, idle_seconds: f64) -> bool {
    if interp.lookup_function("timer-event-handler", env).is_err() {
        return false;
    }
    let timer_seconds = |interp: &mut Interpreter, env: &mut Env, timer: &Value| {
        let time = interp
            .call_function_value(
                Value::Symbol("timer--time".into()),
                None,
                std::slice::from_ref(timer),
                env,
            )
            .ok()?;
        interp
            .call_function_value(
                Value::Symbol("float-time".into()),
                None,
                std::slice::from_ref(&time),
                env,
            )
            .ok()?
            .as_float()
            .ok()
    };
    let mut ran = false;
    for (list_name, idle) in [("timer-idle-list", true), ("timer-list", false)] {
        let Some(timers) = interp
            .lookup_var(list_name, env)
            .and_then(|value| value.to_vec().ok())
        else {
            continue;
        };
        for timer in timers {
            let Some(time) = timer_seconds(interp, env, &timer) else {
                continue;
            };
            let due = if idle {
                let triggered = interp
                    .call_function_value(
                        Value::Symbol("timer--triggered".into()),
                        None,
                        std::slice::from_ref(&timer),
                        env,
                    )
                    .is_ok_and(|value| value.is_truthy());
                time <= idle_seconds && !triggered
            } else {
                interp
                    .call_function_value(Value::Symbol("float-time".into()), None, &[], env)
                    .ok()
                    .and_then(|now| now.as_float().ok())
                    .is_some_and(|now| time <= now)
            };
            if due {
                ran = true;
                let _ = interp.call_function_value(
                    Value::Symbol("timer-event-handler".into()),
                    None,
                    std::slice::from_ref(&timer),
                    env,
                );
            }
        }
    }
    ran
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
        // GNU's input readers consume the executing keyboard macro's
        // remaining events (viper's `F'/`t' read their target char that way).
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
        if let Some(read) = read_via_tty_event_reader() {
            return match read {
                Some(event) => {
                    record_external_input_event(interp, &event);
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
    record_external_input_event(interp, &event);
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

pub(crate) fn parse_edmacro_key_sequence(source: &str) -> Result<Value, LispError> {
    let mut parser = EdmacroKeyParser::new(source);
    let items = parser.parse()?;
    let mut vector = vec![Value::symbol("vector-literal")];
    vector.extend(items);
    Ok(Value::list(vector))
}

pub(crate) struct EdmacroKeyParser<'a> {
    source: &'a str,
    pos: usize,
}

impl<'a> EdmacroKeyParser<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, pos: 0 }
    }

    fn parse(&mut self) -> Result<Vec<Value>, LispError> {
        let mut items = Vec::new();
        while self.pos < self.source.len() {
            self.skip_whitespace();
            if self.pos >= self.source.len() {
                break;
            }
            if self.starts_comment() {
                self.skip_comment();
                continue;
            }
            let repeat = self.parse_repeat_prefix()?;
            if self.starts_comment() {
                self.skip_comment();
                continue;
            }
            let token = self.read_token();
            if token.is_empty() {
                break;
            }
            let parsed = parse_edmacro_token(token)?;
            for _ in 0..repeat {
                items.extend(parsed.iter().cloned());
            }
        }
        Ok(items)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek_char() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn starts_comment(&self) -> bool {
        let rest = &self.source[self.pos..];
        if rest.starts_with(";;") {
            return true;
        }
        if !rest.starts_with("REM") {
            return false;
        }
        match rest.get(3..).and_then(|tail| tail.chars().next()) {
            None => true,
            Some(ch) => ch.is_whitespace(),
        }
    }

    fn skip_comment(&mut self) {
        while let Some(ch) = self.peek_char() {
            self.pos += ch.len_utf8();
            if ch == '\n' {
                break;
            }
        }
    }

    fn parse_repeat_prefix(&mut self) -> Result<usize, LispError> {
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if !ch.is_ascii_digit() {
                break;
            }
            self.pos += ch.len_utf8();
        }
        if self.pos == start || self.peek_char() != Some('*') {
            self.pos = start;
            return Ok(1);
        }
        let count = self.source[start..self.pos]
            .parse::<usize>()
            .map_err(|error| LispError::Signal(format!("Invalid repetition count: {error}")))?;
        self.pos += 1;
        Ok(count)
    }

    fn read_token(&mut self) -> &'a str {
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
        &self.source[start..self.pos]
    }

    fn peek_char(&self) -> Option<char> {
        self.source[self.pos..].chars().next()
    }
}

pub(crate) fn parse_edmacro_token(token: &str) -> Result<Vec<Value>, LispError> {
    if token.starts_with("<<") && token.ends_with(">>") && token.len() >= 4 {
        let command = &token[2..token.len() - 2];
        let mut items = vec![Value::Integer(apply_edmacro_modifiers(
            'x' as i64, false, true,
        ))];
        items.extend(command.chars().map(|ch| Value::Integer(ch as i64)));
        items.push(Value::Integer('\r' as i64));
        return Ok(items);
    }

    if let Some(key) = parse_modified_edmacro_key(token)? {
        return Ok(vec![Value::Integer(key)]);
    }

    if let Some(key) = parse_named_edmacro_key(token) {
        return Ok(vec![Value::Integer(key)]);
    }

    Ok(token.chars().map(|ch| Value::Integer(ch as i64)).collect())
}

pub(crate) fn parse_modified_edmacro_key(token: &str) -> Result<Option<i64>, LispError> {
    let mut ctrl = false;
    let mut meta = false;
    let mut shift = false;
    let mut super_key = false;
    let mut rest = token;

    while let Some((prefix, tail)) = rest.split_once('-') {
        match prefix {
            "C" => ctrl = true,
            "M" => meta = true,
            "S" => shift = true,
            "s" => super_key = true,
            _ => return Ok(None),
        }
        rest = tail;
    }

    if !(ctrl || meta || shift || super_key) {
        return Ok(None);
    }

    let base = if let Some(key) = parse_named_edmacro_key(rest) {
        key
    } else if rest.chars().count() == 1 {
        rest.chars().next().expect("count checked") as i64
    } else {
        return Ok(None);
    };

    let mut key = apply_edmacro_modifiers(base, ctrl, meta);
    if shift {
        key |= 1 << 25;
    }
    if super_key {
        key |= 1 << 23;
    }
    Ok(Some(key))
}

pub(crate) fn apply_edmacro_modifiers(mut value: i64, ctrl: bool, meta: bool) -> i64 {
    if ctrl && value != 0 {
        value = match value {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    }
    if meta {
        value |= 1 << 27;
    }
    value
}

pub(crate) fn parse_named_edmacro_key(token: &str) -> Option<i64> {
    match token {
        "NUL" => Some(0),
        "TAB" => Some('\t' as i64),
        "LFD" => Some('\n' as i64),
        "RET" => Some('\r' as i64),
        "ESC" => Some(0x1b),
        "SPC" => Some(' ' as i64),
        "DEL" => Some(0x7f),
        _ => None,
    }
}

pub(crate) fn is_composed_accessor_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() >= 3
        && bytes.first() == Some(&b'c')
        && bytes.last() == Some(&b'r')
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| matches!(byte, b'a' | b'd'))
}

/// keyboard.c's menu_bar_items: the menu bar's top-level captions in
/// display order.  Every active keymap's `menu-bar' prefix is scanned
/// lowest-precedence first (global map, then local, then minor modes),
/// so global menus lead the row and higher-precedence maps append or
/// merge into existing entries; the keys named by
/// `menu-bar-final-items' move to the end (Help).
pub(crate) fn menu_bar_row_captions(interp: &mut Interpreter, env: &mut Env) -> Vec<String> {
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
        let Ok(menu) = super::call(
            interp,
            "lookup-key",
            &[map.clone(), menu_bar_key.clone()],
            env,
        ) else {
            continue;
        };
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
        // One keymap contributes to a key only once, even when its
        // entry list carries shadowed duplicates.
        let mut seen: Vec<Value> = Vec::new();
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
    items.into_iter().map(|(_, caption)| caption).collect()
}

/// parse_menu_item for the menu bar: the caption of a live top-level
/// item, or None when the item is invisible, disabled, undefined, or
/// not a menu item at all.
fn menu_item_caption(interp: &mut Interpreter, env: &mut Env, item: &Value) -> Option<String> {
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
        // Old format (NAME [HELP-STRING] [CACHE] . DEF): a menu-bar item
        // needs a live definition after the optional extras.
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
        if def.is_nil() {
            return None;
        }
        return Some(name);
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
    while index + 1 < rest.len() {
        let Value::Symbol(keyword) = &rest[index] else {
            break;
        };
        let value = &rest[index + 1];
        match keyword.as_ref() {
            ":visible" | ":enable" => {
                if eval_property(interp, env, value).is_nil() {
                    return None;
                }
            }
            ":filter" => filter = Some(value.clone()),
            _ => {}
        }
        index += 2;
    }
    if let Some(filter) = filter {
        def = interp
            .call_function_value(filter, None, std::slice::from_ref(&def), env)
            .unwrap_or(Value::Nil);
    }
    if def.is_nil() {
        return None;
    }
    let name = eval_property(interp, env, &name_form);
    crate::lisp::primitives::string_text(&name)
        .ok()
        .map(|name| name.to_string())
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
