use super::*;
use crate::lisp::primitives::processes::wait_pumping_processes;

fn event_vector(events: impl IntoIterator<Item = Value>) -> Value {
    Value::list(std::iter::once(Value::symbol("vector-literal")).chain(events))
}

fn event_array(events: &[Value], force_vector: bool) -> Value {
    if !force_vector {
        let characters = events
            .iter()
            .map(|event| {
                let Value::Integer(code) = event else {
                    return None;
                };
                u32::try_from(*code).ok().and_then(char::from_u32)
            })
            .collect::<Option<String>>();
        if let Some(characters) = characters {
            return Value::String(characters.into());
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
    let final_macro = if matches!(&args[0], Value::Symbol(_)) {
        super::call(
            interp,
            "indirect-function",
            std::slice::from_ref(&args[0]),
            env,
        )?
    } else {
        args[0].clone()
    };
    let events = if let Some(string) = string_like(&final_macro) {
        string
            .text
            .chars()
            .map(|ch| Value::Integer(ch as i64))
            .collect()
    } else if is_vector_value(&final_macro) {
        vector_items(&final_macro)?
    } else {
        return Err(LispError::Signal(
            "Keyboard macros must be strings or vectors".into(),
        ));
    };
    let mut repeat = args
        .get(1)
        .map(prefix_numeric_value)
        .transpose()?
        .unwrap_or(Value::Integer(1))
        .as_integer()?;
    let loop_function = args.get(2).cloned().unwrap_or(Value::Nil);
    let previous_macro = interp
        .lookup_var("executing-kbd-macro", env)
        .unwrap_or(Value::Nil);
    let previous_index = interp
        .lookup_var("executing-kbd-macro-index", env)
        .unwrap_or(Value::Nil);
    let previous_real_this_command = interp
        .lookup_var("real-this-command", env)
        .unwrap_or(Value::Nil);

    // Fexecute_kbd_macro starts each iteration in the selected window's
    // buffer.  This is observable when Lisp deliberately makes another
    // buffer current without changing the selected window.
    interp.set_current_buffer_id(interp.selected_window_buffer_id())?;

    let mut result = Ok(());
    loop {
        interp.set_variable("executing-kbd-macro", final_macro.clone(), env);
        interp.set_variable("executing-kbd-macro-index", Value::Integer(0), env);
        interp.set_variable("prefix-arg", Value::Nil, env);
        interp.set_variable("last-prefix-arg", Value::Nil, env);

        if !loop_function.is_nil() {
            match call_function_value(interp, &loop_function, &[], env) {
                Ok(value) if value.is_nil() => break,
                Ok(_) => {}
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }

        interp
            .kbd_macro_executions
            .push(crate::lisp::eval::KbdMacroExecutionState {
                events: events.clone(),
                index: 0,
            });
        let iteration = match run_kbd_macro_events(interp, env) {
            // GNU's outermost command loop catches `top-level`, terminating
            // the keyboard macro without propagating an error.
            Err(LispError::Throw(tag, _)) if matches!(&tag, Value::Symbol(symbol) if symbol == "top-level") => {
                Ok(())
            }
            other => other,
        };
        interp.kbd_macro_executions.pop();
        if let Err(error) = iteration {
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
            .lookup_var("executing-kbd-macro", env)
            .is_some_and(|value| string_like(&value).is_some() || is_vector_value(&value));
        if !still_executing {
            break;
        }
    }

    interp.set_variable("executing-kbd-macro", previous_macro, env);
    interp.set_variable("executing-kbd-macro-index", previous_index, env);
    interp.set_variable("real-this-command", previous_real_this_command, env);
    interp.set_variable("this-command", Value::Nil, env);
    // This is an unwind cleanup in GNU: it runs once for normal completion,
    // loop-function termination, and command errors.
    if let Err(hook_error) = run_named_hooks(interp, "kbd-macro-termination-hook", env, None) {
        result = Err(hook_error);
    }
    result.map(|()| Value::Nil)
}

fn current_kbd_macro_event(interp: &Interpreter, offset: usize) -> Option<Value> {
    let state = interp.kbd_macro_executions.last()?;
    state.events.get(state.index + offset).cloned()
}

fn increment_num_input_keys(interp: &mut Interpreter, env: &mut Env) {
    let count = interp
        .lookup_var("num-input-keys", env)
        .and_then(|value| value.as_integer().ok())
        .unwrap_or(0);
    interp.set_variable(
        "num-input-keys",
        Value::Integer(count.saturating_add(1)),
        env,
    );
}

fn sync_kbd_macro_execution(interp: &mut Interpreter, env: &Env) -> Result<(), LispError> {
    if interp.kbd_macro_executions.is_empty() {
        return Ok(());
    }
    let events = interp
        .lookup_var("executing-kbd-macro", env)
        .and_then(|value| {
            if let Some(string) = string_like(&value) {
                Some(Ok(string
                    .text
                    .chars()
                    .map(|character| Value::Integer(character as i64))
                    .collect()))
            } else if is_vector_value(&value) {
                Some(vector_items(&value))
            } else {
                None
            }
        })
        .transpose()?;
    let index = interp
        .lookup_var("executing-kbd-macro-index", env)
        .and_then(|value| value.as_integer().ok())
        .and_then(|index| usize::try_from(index).ok());
    if let Some(state) = interp.kbd_macro_executions.last_mut() {
        if let Some(events) = events {
            state.events = events;
        }
        if let Some(index) = index {
            state.index = index;
        }
    }
    Ok(())
}

fn load_autoloaded_prefix_map(
    interp: &mut Interpreter,
    binding: &Value,
    env: &Env,
) -> Result<(), LispError> {
    let Value::Symbol(name) = binding else {
        return Ok(());
    };
    let Ok(function) = interp.lookup_function(name, env) else {
        return Ok(());
    };
    if let Some((file, _, Value::Symbol(kind))) = autoload_parts(&function)
        && kind == "keymap"
    {
        interp.load_target_with_env(&file, env)?;
    }
    Ok(())
}

// GNU's C command loop runs `pre-command-hook', delegates the command body
// (including prefix transfer) to simple.el's `command-execute', records the
// consumed prefix, and only then runs `post-command-hook'.  Batch startup
// loads that Elisp owner.  The small native transfer below exists solely for
// file-less Interpreter users and native command bodies that cannot delegate
// to `command-execute'.
fn prepare_native_kbd_command_body(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let pending_prefix = interp.lookup_var("prefix-arg", env).unwrap_or(Value::Nil);
    interp.set_variable("current-prefix-arg", pending_prefix.clone(), env);
    interp.set_variable("prefix-arg", Value::Nil, env);
    if pending_prefix.is_truthy()
        && let Ok(update) = interp.lookup_function("prefix-command-update", env)
    {
        call_function_value(interp, &update, &[], env)?;
    }
    Ok(())
}

fn execute_kbd_command_body(
    interp: &mut Interpreter,
    command: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Ok(command_execute) = interp.lookup_function("command-execute", env) {
        call_function_value(interp, &command_execute, std::slice::from_ref(command), env)?;
    } else {
        prepare_native_kbd_command_body(interp, env)?;
        call_interactively_impl(interp, std::slice::from_ref(command), env)?;
    }
    Ok(())
}

fn finish_kbd_macro_command_cycle(
    interp: &mut Interpreter,
    real_command: Value,
    dispatched_command: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let current_prefix = interp
        .lookup_var("current-prefix-arg", env)
        .unwrap_or(Value::Nil);
    interp.set_variable("last-prefix-arg", current_prefix, env);
    safe_run_named_hooks(
        interp,
        "post-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let final_this_command = interp
        .lookup_var("this-command", env)
        .filter(|value| !value.is_nil())
        .unwrap_or(dispatched_command);
    finish_kbd_macro_command(interp, real_command, final_this_command, env);
    Ok(())
}

fn pending_prefix_argument(interp: &Interpreter, env: &Env) -> Value {
    interp.lookup_var("prefix-arg", env).unwrap_or(Value::Nil)
}

fn prefix_integer(value: &Value) -> Option<BigInt> {
    match value {
        Value::Integer(integer) => Some(BigInt::from(*integer)),
        Value::BigInteger(integer) => Some(integer.clone().into()),
        _ => None,
    }
}

pub(crate) fn next_universal_prefix(pending: &Value) -> Value {
    match pending {
        Value::Nil => Value::list([Value::Integer(4)]),
        Value::Cons(_) => {
            let factor = prefix_numeric_value(pending)
                .ok()
                .and_then(|value| prefix_integer(&value))
                .unwrap_or_else(|| BigInt::from(4));
            Value::list([normalize_bigint_value(factor * BigInt::from(4))])
        }
        Value::Symbol(minus) if minus == "-" => Value::list([Value::Integer(-4)]),
        other => other.clone(),
    }
}

pub(crate) fn next_negative_prefix(pending: &Value) -> Value {
    if let Some(integer) = prefix_integer(pending) {
        normalize_bigint_value(-integer)
    } else if matches!(pending, Value::Symbol(minus) if minus == "-") {
        Value::Nil
    } else {
        Value::Symbol("-".into())
    }
}

pub(crate) fn next_digit_prefix(pending: &Value, digit: i64) -> Value {
    if let Some(integer) = prefix_integer(pending) {
        let negative = integer.sign() == Sign::Minus;
        let digit = BigInt::from(digit);
        normalize_bigint_value(if negative {
            integer * BigInt::from(10) - digit
        } else {
            integer * BigInt::from(10) + digit
        })
    } else if matches!(pending, Value::Symbol(minus) if minus == "-") {
        if digit == 0 {
            Value::Symbol("-".into())
        } else {
            Value::Integer(-digit)
        }
    } else {
        Value::Integer(digit)
    }
}

fn run_minibuffer_prefix_command(
    interp: &mut Interpreter,
    command_name: &str,
    event: &Value,
    next_prefix: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let original_command = Value::Symbol(command_name.into());
    set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
    interp.set_variable("last-command-event", event.clone(), env);
    interp.set_variable("last-input-event", event.clone(), env);
    interp.set_variable("this-original-command", original_command.clone(), env);
    interp.set_variable("this-command", original_command.clone(), env);
    increment_num_input_keys(interp, env);
    safe_run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let dispatched = interp
        .lookup_var("this-command", env)
        .filter(|command| !command.is_nil())
        .unwrap_or_else(|| original_command.clone());
    let has_command_owner = interp.lookup_function("command-execute", env).is_ok()
        || dispatched != original_command
        || interp.lookup_function(command_name, env).is_ok();
    if has_command_owner {
        execute_kbd_command_body(interp, &dispatched, env)?;
    } else if let Ok(preserve) = interp.lookup_function("prefix-command-preserve-state", env) {
        prepare_native_kbd_command_body(interp, env)?;
        call_function_value(interp, &preserve, &[], env)?;
        interp.set_variable("prefix-arg", next_prefix, env);
    } else {
        prepare_native_kbd_command_body(interp, env)?;
        interp.set_variable("prefix-arg", next_prefix, env);
    }
    finish_kbd_macro_command_cycle(interp, original_command, dispatched, env)?;
    sync_kbd_macro_execution(interp, env)
}

fn active_minibuffer_prompt_end(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<usize, LispError> {
    let position = super::call(interp, "minibuffer-prompt-end", &[], env)?.as_integer()?;
    usize::try_from(position).map_err(|_| LispError::Signal("Invalid minibuffer prompt".into()))
}

fn active_minibuffer_text(interp: &mut Interpreter, env: &mut Env) -> Result<String, LispError> {
    let contents = super::call(interp, "minibuffer-contents-no-properties", &[], env)?;
    string_text(&contents)
}

// Minibuffer reads issued while a keyboard macro executes consume the
// macro's remaining events as minibuffer input, up to the RET (or C-j) that
// runs `exit-minibuffer' in the real command loop.  INITIAL seeds the
// contents with point at the end, and the basic editing keys the Edebug
// tests use to replace a suggested default are honored.
fn read_minibuffer_text_from_kbd_macro(
    interp: &mut Interpreter,
    env: &mut Env,
    prompt: &str,
    initial: &str,
    local_map: &Value,
) -> Result<Option<String>, LispError> {
    if interp.kbd_macro_executions.is_empty() {
        return Ok(None);
    }
    let saved_buffer_id = prepare_kbd_macro_minibuffer_entry(interp, env)?;
    let result = (|| {
        let minibuffer = activate_minibuffer(interp, prompt, initial, local_map.clone(), env)?;
        run_active_minibuffer(interp, env, minibuffer, |interp, env| {
            read_minibuffer_text_from_kbd_macro_inner(interp, env, initial)
        })
    })();
    if interp.has_buffer_id(saved_buffer_id) {
        let _ = interp.set_current_buffer_id(saved_buffer_id);
    }
    result
}

pub(crate) fn prepare_kbd_macro_minibuffer_entry(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<u64, LispError> {
    let prompting_buffer_id = interp.current_buffer_id();
    // Entering the recursive minibuffer command loop completes the outer
    // command's pending cycle before the first minibuffer key is read.
    // Kmacro's step editor uses this boundary to carry the outer macro index
    // into the minibuffer.
    let result = safe_run_named_hooks(interp, "post-command-hook", env, Some(prompting_buffer_id));
    if interp.has_buffer_id(prompting_buffer_id) {
        interp.set_current_buffer_id(prompting_buffer_id)?;
    }
    result.map(|()| prompting_buffer_id)
}

pub(crate) fn read_minibuffer_text_from_kbd_macro_inner(
    interp: &mut Interpreter,
    env: &mut Env,
    _initial: &str,
) -> Result<Option<String>, LispError> {
    while let Some(event) = current_kbd_macro_event(interp, 0) {
        // `read-kbd-macro' can retain GNU's modifier bits on character
        // events, whereas literal macro strings contain resolved control
        // bytes.  Minibuffer editing must see C-a/C-k identically in both
        // representations.
        let event = crate::lisp::primitives::reader_key_event_value(event);
        let code = match &event {
            Value::Integer(code) => *code,
            Value::Symbol(name) => function_key_default_translation(name).unwrap_or(-1),
            _ => -1,
        };
        if code < 0 {
            break;
        }
        advance_kbd_macro_index(interp, 1, env);
        let original_command = Value::Symbol(
            match code {
                13 | 10 => "exit-minibuffer",
                1 => "beginning-of-line",
                5 => "end-of-line",
                11 => "kill-line",
                127 => "delete-backward-char",
                _ => "self-insert-command",
            }
            .into(),
        );
        set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
        interp.set_variable("last-command-event", event.clone(), env);
        interp.set_variable("last-input-event", event, env);
        interp.set_variable("this-original-command", original_command.clone(), env);
        interp.set_variable("this-command", original_command.clone(), env);
        increment_num_input_keys(interp, env);
        safe_run_named_hooks(
            interp,
            "pre-command-hook",
            env,
            Some(interp.current_buffer_id()),
        )?;
        let command = interp
            .lookup_var("this-command", env)
            .filter(|command| !command.is_nil())
            .unwrap_or_else(|| original_command.clone());
        let native_command = matches!(
            command.as_symbol().unwrap_or(""),
            "ignore"
                | "exit-minibuffer"
                | "move-beginning-of-line"
                | "beginning-of-line"
                | "move-end-of-line"
                | "end-of-line"
                | "kill-line"
                | "delete-backward-char"
                | "self-insert-command"
        );
        if native_command {
            prepare_native_kbd_command_body(interp, env)?;
        }
        let mut exit = false;
        match command.as_symbol().unwrap_or("") {
            "ignore" => {}
            "exit-minibuffer" => exit = true,
            "move-beginning-of-line" | "beginning-of-line" => {
                let prompt_end = active_minibuffer_prompt_end(interp, env)?;
                interp.buffer.goto_char(prompt_end);
            }
            "move-end-of-line" | "end-of-line" => {
                interp.buffer.goto_char(interp.buffer.point_max());
            }
            "kill-line" => {
                let point = interp.buffer.point();
                let end = interp.buffer.point_max();
                delete_region_with_hooks(interp, point, end, env)?;
            }
            "delete-backward-char" => {
                let point = interp.buffer.point();
                if point > active_minibuffer_prompt_end(interp, env)? {
                    delete_region_with_hooks(interp, point - 1, point, env)?;
                }
            }
            "self-insert-command" => {
                let event = interp
                    .lookup_var("last-command-event", env)
                    .unwrap_or(Value::Integer(code));
                if let Some(ch) = unread_event_char(&event)
                    && (!ch.is_control() || ch == '\t')
                {
                    insert_text_with_hooks(interp, &ch.to_string(), &[], &[], false, false, env)?;
                }
            }
            _ => {
                execute_kbd_command_body(interp, &command, env)?;
            }
        }
        // `exit-minibuffer' leaves the recursive command loop before its
        // post-command phase.  The prompting command resumes, consumes the
        // submitted text, and only then runs its own post-command hook.  In
        // particular, a keyboard-macro thunk attached to RET must observe
        // the prompting command's completed assignment, not the state from
        // just before `read-from-minibuffer' returns.
        if exit {
            sync_kbd_macro_execution(interp, env)?;
            break;
        }
        finish_kbd_macro_command_cycle(interp, original_command, command, env)?;
        sync_kbd_macro_execution(interp, env)?;
    }
    active_minibuffer_text(interp, env).map(Some)
}

fn read_minibuffer_text_from_batch_stdin(prompt: &str) -> Result<String, LispError> {
    print!("{prompt}");
    std::io::stdout()
        .flush()
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let mut line = String::new();
    if std::io::stdin()
        .read_line(&mut line)
        .map_err(|error| LispError::Signal(error.to_string()))?
        == 0
    {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("end-of-file".into()),
            Value::String("Error reading from stdin".into()),
        ])));
    }
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
    Ok(line)
}

// With no queued events or executing keyboard macro, the native file-less
// fallback still enters the minibuffer before it consults batch stdin (or
// returns the current contents for an interactive embedding).  GNU runs
// `minibuffer-setup-hook' in that state, and a hook may nonlocally exit before
// any input source is touched.
// Terminal-driven minibuffer input, installed by the tty frontend for the
// duration of an interactive session.  The reader returns `Some(text)` for
// a submitted line and `None` when the user quit (C-g); with no reader
// installed the interactive branch keeps its queued-events behavior.
thread_local! {
    static TTY_MINIBUFFER_READER: std::cell::RefCell<Option<TtyMinibufferReader>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyMinibufferReader = Box<dyn FnMut(&str, &str) -> Option<String>>;

pub(crate) fn set_tty_minibuffer_reader(reader: Option<TtyMinibufferReader>) {
    TTY_MINIBUFFER_READER.with_borrow_mut(|slot| *slot = reader);
}

fn read_via_tty_minibuffer(prompt: &str, initial: &str) -> Option<Option<String>> {
    TTY_MINIBUFFER_READER
        .with_borrow_mut(|slot| slot.as_mut().map(|reader| reader(prompt, initial)))
}

fn read_minibuffer_text_without_queued_events(
    interp: &mut Interpreter,
    env: &mut Env,
    prompt: &str,
    initial: &str,
    local_map: &Value,
) -> Result<String, LispError> {
    let minibuffer = activate_minibuffer(interp, prompt, initial, local_map.clone(), env)?;
    run_active_minibuffer(interp, env, minibuffer, |interp, env| {
        if interp
            .lookup_var("noninteractive", env)
            .is_some_and(|value| value.is_truthy())
        {
            read_minibuffer_text_from_batch_stdin(prompt)
        } else if let Some(read) = read_via_tty_minibuffer(prompt, initial) {
            match read {
                Some(text) => Ok(text),
                // C-g during minibuffer input is GNU's `quit' signal.
                None => Err(LispError::SignalValue(Value::Symbol("quit".into()))),
            }
        } else {
            active_minibuffer_text(interp, env)
        }
    })
}

// `ert-simulate-keys' drives a real GNU minibuffer through
// `unread-command-events', not through `execute-kbd-macro'.  Resolve command
// prefixes before treating events as text so global commands such as
// C-x RET c can open a nested prompt and then return to the outer minibuffer.
fn read_minibuffer_text_from_unread_events(
    interp: &mut Interpreter,
    env: &mut Env,
    prompt: &str,
    initial: &str,
    local_map: &Value,
) -> Result<Option<String>, LispError> {
    if crate::lisp::primitives::unread_command_events(interp, env)?.is_empty() {
        return Ok(None);
    }
    // A recursive minibuffer command loop has its own prefix state.  Preserve
    // the caller's prefix (which may control what the prompting command asks)
    // while simulated keys start unprefixed and may build a fresh C-u prefix.
    let saved_buffer_id = interp.current_buffer_id();
    let restore = interp.bind_special_dynamic("current-prefix-arg", Value::Nil, env)?;
    let result = (|| {
        let minibuffer = activate_minibuffer(interp, prompt, initial, local_map.clone(), env)?;
        run_active_minibuffer(interp, env, minibuffer, |interp, env| {
            read_minibuffer_text_from_unread_events_inner(interp, env, initial)
        })
    })();
    if interp.has_buffer_id(saved_buffer_id) {
        let _ = interp.set_current_buffer_id(saved_buffer_id);
    }
    let restore_result = interp.restore_special_dynamic(restore, env);
    match (result, restore_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn read_minibuffer_text_from_unread_events_inner(
    interp: &mut Interpreter,
    env: &mut Env,
    _initial: &str,
) -> Result<Option<String>, LispError> {
    let unread = crate::lisp::primitives::unread_command_events(interp, env)?;
    if unread.is_empty() {
        return Ok(None);
    }
    let mut events = VecDeque::from(unread);
    let mut pending_keys = Vec::new();
    let mut pending_events = Vec::new();

    while let Some(mut event) = events.pop_front() {
        interp.set_variable(
            "unread-command-events",
            Value::list(events.iter().cloned()),
            env,
        );
        let key = Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
        let mut event_key = key_sequence_binding_text(&key)?;
        if matches!(&event, Value::Symbol(_)) && !event_key.starts_with('<') {
            event_key = format!("<{event_key}>");
        }
        if pending_keys.is_empty()
            && let Value::Symbol(name) = &event
            && let Some(translated) = function_key_default_translation(name)
            && key_binding(interp, &event_key, false, false, env)?.is_nil()
            && !key_sequence_is_prefix(interp, &event_key, env)?
        {
            event = Value::Integer(translated);
            let translated = Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
            event_key = key_sequence_binding_text(&translated)?;
        }
        let code = event.as_integer().ok();
        if pending_keys.is_empty() && matches!(code, Some(10 | 13)) {
            break;
        }
        pending_keys.push(event_key);
        pending_events.push(event.clone());
        let binding_key = pending_keys.join(" ");

        if binding_key == "C-u" {
            let pending = pending_prefix_argument(interp, env);
            let next = next_universal_prefix(&pending);
            let command = if pending.is_nil() {
                "universal-argument"
            } else {
                "universal-argument-more"
            };
            run_minibuffer_prefix_command(interp, command, &event, next, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        if pending_keys.len() == 1
            && matches!(code, Some(code) if code == '-' as i64)
            && pending_prefix_argument(interp, env).is_truthy()
        {
            let next = next_negative_prefix(&pending_prefix_argument(interp, env));
            run_minibuffer_prefix_command(interp, "negative-argument", &event, next, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        if pending_keys.len() == 1
            && let Some(code) = code
            && (0x30..=0x39).contains(&code)
        {
            let pending = pending_prefix_argument(interp, env);
            if !pending.is_nil() {
                let digit = code - 0x30;
                let next = next_digit_prefix(&pending, digit);
                run_minibuffer_prefix_command(interp, "digit-argument", &event, next, env)?;
                pending_keys.clear();
                pending_events.clear();
                continue;
            }
        }

        let binding = key_binding(interp, &binding_key, false, false, env)?;
        if is_keymap_value(interp, &binding) || key_sequence_is_prefix(interp, &binding_key, env)? {
            load_autoloaded_prefix_map(interp, &binding, env)?;
            continue;
        }

        if pending_keys.len() == 1
            && let Some(text) = keyboard_macro_self_insert_text(&event)
            && (binding.is_nil()
                || matches!(&binding, Value::Symbol(command) if command == "self-insert-command"))
        {
            let command = Value::Symbol("self-insert-command".into());
            set_command_key_state(interp, pending_events.clone(), pending_events.clone(), env);
            interp.set_variable("last-command-event", event.clone(), env);
            interp.set_variable("last-input-event", event.clone(), env);
            interp.set_variable("this-original-command", command.clone(), env);
            interp.set_variable("this-command", command.clone(), env);
            increment_num_input_keys(interp, env);
            safe_run_named_hooks(
                interp,
                "pre-command-hook",
                env,
                Some(interp.current_buffer_id()),
            )?;
            let dispatched_command = interp
                .lookup_var("this-command", env)
                .filter(|command| !command.is_nil())
                .unwrap_or_else(|| command.clone());
            if dispatched_command == command {
                prepare_native_kbd_command_body(interp, env)?;
                let repeat = interp
                    .lookup_var("current-prefix-arg", env)
                    .and_then(|prefix| prefix_numeric_value(&prefix).ok())
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(1)
                    .max(0) as usize;
                insert_text_with_hooks(interp, &text.repeat(repeat), &[], &[], false, false, env)?;
            } else {
                execute_kbd_command_body(interp, &dispatched_command, env)?;
            }
            finish_kbd_macro_command_cycle(interp, command, dispatched_command, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        if !binding.is_nil() {
            execute_kbd_macro_command(interp, &binding, &pending_events, env)?;
            events = VecDeque::from(crate::lisp::primitives::unread_command_events(interp, env)?);
            pending_keys.clear();
            pending_events.clear();
            continue;
        }

        pending_keys.clear();
        pending_events.clear();
    }

    interp.set_variable("unread-command-events", Value::list(events), env);
    active_minibuffer_text(interp, env).map(Some)
}

fn advance_kbd_macro_index(interp: &mut Interpreter, count: usize, env: &mut Env) {
    if let Some(state) = interp.kbd_macro_executions.last_mut() {
        state.index += count;
        let index = state.index;
        interp.set_variable(
            "executing-kbd-macro-index",
            Value::Integer(index as i64),
            env,
        );
    }
}

// Some compact batch implementations (currently Isearch) perform the command
// body natively but must still expose each input event to the Lisp command
// loop.  Return true when hooks left COMMAND unchanged, so the caller should
// perform that native body; a rewritten command is dispatched here instead.
fn run_internal_kbd_macro_command(
    interp: &mut Interpreter,
    command: &str,
    event: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    let command = Value::Symbol(command.into());
    set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
    interp.set_variable("last-command-event", event.clone(), env);
    interp.set_variable("last-input-event", event.clone(), env);
    interp.set_variable("this-original-command", command.clone(), env);
    interp.set_variable("this-command", command.clone(), env);
    increment_num_input_keys(interp, env);
    safe_run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let dispatched = interp
        .lookup_var("this-command", env)
        .filter(|value| !value.is_nil())
        .unwrap_or_else(|| command.clone());
    let unchanged = dispatched == command;
    if unchanged {
        prepare_native_kbd_command_body(interp, env)?;
    } else {
        execute_kbd_command_body(interp, &dispatched, env)?;
    }
    finish_kbd_macro_command_cycle(interp, command, dispatched, env)?;
    sync_kbd_macro_execution(interp, env)?;
    Ok(unchanged)
}

// Dispatch commands from the innermost keyboard macro until its events run
// out.  `recursive-edit` re-enters this loop on the same shared cursor, so a
// command that stops in a recursive edit (like Edebug) keeps consuming the
// same macro until `exit-recursive-edit` throws back out.
fn run_kbd_macro_events(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let mut pending_keys: Vec<String> = Vec::new();
    let mut pending_events = Vec::new();
    loop {
        let macro_active = interp
            .lookup_var("executing-kbd-macro", env)
            .is_some_and(|value| value.is_truthy());
        if !macro_active {
            return Ok(());
        }
        sync_kbd_macro_execution(interp, env)?;
        let Some(mut event) = current_kbd_macro_event(interp, 0) else {
            // read_key_sequence increments this counter before reporting the
            // end of a keyboard macro to the command loop.
            increment_num_input_keys(interp, env);
            return Ok(());
        };
        let key = Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
        let mut event_key = key_sequence_binding_text(&key)?;
        // GNU describes function-key symbol events in angle brackets
        // ("<escape>"), which is also what the string-parsing lookup path
        // needs to see one named key instead of one key per character.
        if matches!(&event, Value::Symbol(_)) && !event_key.starts_with('<') {
            event_key = format!("<{event_key}>");
        }
        // GNU's local-function-key-map translates unbound function-key
        // symbols to their ASCII equivalents ([escape] a1 ESC dispatches
        // viper's ESC binding, not an `escape' text insertion).
        let default_translation = match &event {
            Value::Symbol(name) => function_key_default_translation(name).map(Value::Integer),
            Value::Integer(code)
                if *code
                    == (crate::lisp::primitives::KEY_DESCRIPTION_SHIFT_BIT | i64::from(b'\t')) =>
            {
                Some(Value::Symbol("backtab".into()))
            }
            _ => None,
        };
        if pending_keys.is_empty()
            && let Some(translated_event) = default_translation
            && key_binding(interp, &event_key, false, false, env)?.is_nil()
            && !key_sequence_is_prefix(interp, &event_key, env)?
        {
            event = translated_event;
            let translated = Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
            event_key = key_sequence_binding_text(&translated)?;
            if matches!(&event, Value::Symbol(_)) && !event_key.starts_with('<') {
                event_key = format!("<{event_key}>");
            }
        }
        if pending_keys.is_empty() && event_key == "C-s" {
            advance_kbd_macro_index(interp, 1, env);
            if !run_internal_kbd_macro_command(interp, "isearch-forward", &event, env)? {
                continue;
            }
            let mut search_text = String::new();
            while let Some(next_event) = current_kbd_macro_event(interp, 0) {
                if let Some(text) = keyboard_macro_self_insert_text(&next_event) {
                    advance_kbd_macro_index(interp, 1, env);
                    if run_internal_kbd_macro_command(
                        interp,
                        "isearch-printing-char",
                        &next_event,
                        env,
                    )? {
                        search_text.push_str(&text);
                    }
                } else {
                    break;
                }
            }
            if !search_text.is_empty() {
                // GNU Isearch starts from `case-fold-search', then typed
                // uppercase disables folding when `search-upper-case' is
                // enabled.  This matters even in our compact keyboard-macro
                // driver: treating `Ind' as `ind' can select "Windows" and
                // leave every following motion command in the wrong region.
                let smart_case = interp.lookup_var("case-fold-search", env) == Some(Value::T)
                    && interp
                        .lookup_var("search-upper-case", env)
                        .is_some_and(|value| value.is_truthy());
                let case_fold = if smart_case
                    && !crate::lisp::primitives::regexp::isearch_no_upper_case_p(
                        &search_text,
                        false,
                    ) {
                    Value::Nil
                } else {
                    interp
                        .lookup_var("case-fold-search", env)
                        .unwrap_or(Value::Nil)
                };
                let restore = interp.bind_special_variable("case-fold-search", case_fold, env)?;
                let search_result = super::call(
                    interp,
                    "search-forward",
                    &[Value::String(search_text.into()), Value::Nil, Value::T],
                    env,
                );
                interp.restore_special_binding(restore, env)?;
                search_result?;
            }
            continue;
        }
        pending_keys.push(event_key);
        pending_events.push(event.clone());
        let binding_key = pending_keys.join(" ");
        if binding_key == "C-u" {
            let pending = pending_prefix_argument(interp, env);
            let next = next_universal_prefix(&pending);
            pending_keys.clear();
            pending_events.clear();
            advance_kbd_macro_index(interp, 1, env);
            let command = if pending.is_nil() {
                "universal-argument"
            } else {
                "universal-argument-more"
            };
            run_minibuffer_prefix_command(interp, command, &event, next, env)?;
            continue;
        }
        if pending_keys.len() == 1
            && matches!(event.as_integer(), Ok(code) if code == '-' as i64)
            && pending_prefix_argument(interp, env).is_truthy()
        {
            let next = next_negative_prefix(&pending_prefix_argument(interp, env));
            pending_keys.clear();
            pending_events.clear();
            advance_kbd_macro_index(interp, 1, env);
            run_minibuffer_prefix_command(interp, "negative-argument", &event, next, env)?;
            continue;
        }
        // While a prefix argument is being entered, digits accumulate into it
        // (`digit-argument') instead of dispatching as ordinary keys.
        if pending_keys.len() == 1
            && let Ok(code) = event.as_integer()
            && (0x30..=0x39).contains(&code)
        {
            let pending = pending_prefix_argument(interp, env);
            if !pending.is_nil() {
                let digit = code - 0x30;
                let next = next_digit_prefix(&pending, digit);
                pending_keys.clear();
                pending_events.clear();
                advance_kbd_macro_index(interp, 1, env);
                run_minibuffer_prefix_command(interp, "digit-argument", &event, next, env)?;
                continue;
            }
        }
        let binding = key_binding(interp, &binding_key, false, false, env)?;
        if is_keymap_value(interp, &binding) || key_sequence_is_prefix(interp, &binding_key, env)? {
            load_autoloaded_prefix_map(interp, &binding, env)?;
            advance_kbd_macro_index(interp, 1, env);
            continue;
        }
        if !binding.is_nil() {
            advance_kbd_macro_index(interp, 1, env);
            execute_kbd_macro_command(interp, &binding, &pending_events, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        if pending_keys.len() == 1
            && let Some(text) = keyboard_macro_self_insert_text(&event)
        {
            advance_kbd_macro_index(interp, 1, env);
            execute_kbd_macro_self_insert(interp, &text, &event, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        pending_keys.clear();
        pending_events.clear();
        advance_kbd_macro_index(interp, 1, env);
        increment_num_input_keys(interp, env);
        set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
        interp.set_variable("last-command-event", event.clone(), env);
        interp.set_variable("last-input-event", event, env);
        interp.set_variable("this-original-command", Value::Nil, env);
        interp.set_variable("this-command", Value::Nil, env);
        safe_run_named_hooks(
            interp,
            "pre-command-hook",
            env,
            Some(interp.current_buffer_id()),
        )?;
        if let Some(command) = interp
            .lookup_var("this-command", env)
            .filter(|command| !command.is_nil())
        {
            execute_kbd_command_body(interp, &command, env)?;
            finish_kbd_macro_command_cycle(interp, Value::Nil, command, env)?;
            continue;
        }
        // The command loop reports an unbound complete sequence and stops
        // the executing macro.  ERC's keymap tests observe this through
        // ert-with-message-capture after removing module bindings.
        call_function_value(
            interp,
            &Value::Symbol("message".into()),
            &[Value::String(format!("{binding_key} is undefined").into())],
            env,
        )?;
        return Ok(());
    }
}

// Batch recursive-edit: consume the remaining events of the innermost
// executing keyboard macro until `exit-recursive-edit` throws `exit` or the
// macro runs dry.
fn recursive_edit(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    interp.command_loop_recursion_depth += 1;
    // GNU's command loop runs post-command-hook at the top of each cycle,
    // including right after entering a recursive edit mid-command; the
    // Edebug tests observe their stop points from that hook run.
    let entry_hooks = if interp.kbd_macro_executions.is_empty() {
        Ok(())
    } else {
        safe_run_named_hooks(
            interp,
            "post-command-hook",
            env,
            Some(interp.current_buffer_id()),
        )
    };
    let result = entry_hooks
        .and_then(|()| run_recursive_kbd_command_loop(interp, env))
        // With no more events to dispatch the command loop goes idle, which
        // processes queued file notifications and fires due timers.  Loaded
        // timer.el owns GNU timer objects in `timer-list'; the native queue
        // remains the bootstrap path, so a real command-loop pump must drain
        // both representations just like the other event-waiting paths.
        .and_then(|()| interp.run_pending_file_notifications(env))
        .and_then(|()| interp.run_pending_timer_events(env));
    interp.command_loop_recursion_depth -= 1;
    match result {
        Err(LispError::Throw(tag, value)) if matches!(&tag, Value::Symbol(symbol) if symbol == "exit") => {
            if value.is_truthy() {
                Err(LispError::SignalValue(Value::list([Value::Symbol(
                    "quit".into(),
                )])))
            } else {
                Ok(Value::Nil)
            }
        }
        Err(error) => Err(error),
        Ok(()) => Ok(Value::Nil),
    }
}

/// Run the command loop used by a recursive edit.
///
/// GNU's top-level `execute-kbd-macro' invokes `command_loop_2' with only
/// `minibuffer-quit' handled, while `recursive-edit' invokes the same loop
/// with the complete `error' condition.  Edebug depends on that distinction:
/// a command error inside its recursive edit is reported, the active macro is
/// stopped, and the next command-loop cycle runs `post-command-hook', where
/// its test driver may deliberately resume the macro.
fn run_recursive_kbd_command_loop(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    loop {
        match run_kbd_macro_events(interp, env) {
            Ok(()) => return Ok(()),
            Err(error @ LispError::Throw(_, _)) => return Err(error),
            Err(error) if error_matches_condition(interp, &error, "error") => {
                report_kbd_command_error(interp, &error, env)?;
                safe_run_named_hooks(
                    interp,
                    "post-command-hook",
                    env,
                    Some(interp.current_buffer_id()),
                )?;
                let resumed = interp
                    .lookup_var("executing-kbd-macro", env)
                    .is_some_and(|value| value.is_truthy());
                if !resumed {
                    return Ok(());
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn execute_kbd_macro_command(
    interp: &mut Interpreter,
    command: &Value,
    events: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let event = events.last().cloned().unwrap_or(Value::Nil);
    // The command loop resolves [remap COMMAND] bindings from the active
    // keymaps before dispatching (erc-fill-wrap remaps erc-bol);
    // `this-original-command' keeps the pre-remap binding.
    let original_command = command.clone();
    let remapped = crate::lisp::primitives::command_remapping(interp, command, None, env)?;
    let command = if remapped.is_nil() {
        original_command.clone()
    } else {
        remapped
    };
    // GNU's command loop separates each command into its own undo group
    // (undo-auto--boundaries); viper's undo tests observe that grouping.
    interp.buffer.push_undo_boundary();
    set_command_key_state(interp, events.to_vec(), events.to_vec(), env);
    interp.set_variable("deactivate-mark", Value::Nil, env);
    interp.set_variable("last-command-event", event.clone(), env);
    interp.set_variable("last-input-event", event, env);
    interp.set_variable("this-original-command", original_command, env);
    interp.set_variable("this-command", command.clone(), env);
    increment_num_input_keys(interp, env);
    safe_run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let dispatched_command = interp
        .lookup_var("this-command", env)
        .filter(|command| !command.is_nil())
        .unwrap_or_else(|| command.clone());
    let command_result = if matches!(&dispatched_command, Value::Symbol(name) if name == "narrow-to-region")
    {
        prepare_native_kbd_command_body(interp, env)?;
        let mark = interp.buffer.mark().unwrap_or(interp.buffer.point());
        let point = interp.buffer.point();
        super::call(
            interp,
            "narrow-to-region",
            &[Value::Integer(mark as i64), Value::Integer(point as i64)],
            env,
        )
        .map(|_| Value::Nil)
    } else {
        execute_kbd_command_body(interp, &dispatched_command, env).map(|()| Value::Nil)
    };
    if let Err(error) = command_result {
        // `Fexecute_kbd_macro' enters GNU's `command_loop_2' with
        // `minibuffer-quit' as its sole condition handler.  A customized
        // reporter changes how that one condition is displayed; it does not
        // turn the command loop into a catch-all for ordinary command errors.
        if matches!(error, LispError::Throw(_, _))
            || !error_matches_condition(interp, &error, "minibuffer-quit")
        {
            return Err(error);
        }
        report_kbd_command_error(interp, &error, env)?;
    }
    // GNU copies `this-command' into `last-command' at the end of the
    // cycle, so a command that rewrites this-command (viper-undo-more sets
    // it to viper-undo) steers the next dispatch.
    finish_kbd_macro_command_cycle(interp, command, dispatched_command, env)?;
    Ok(())
}

fn report_kbd_command_error(
    interp: &mut Interpreter,
    error: &LispError,
    env: &mut Env,
) -> Result<(), LispError> {
    // GNU's cmd_error stops an executing macro for ordinary errors, but a
    // `minibuffer-quit' is allowed to return to the same macro.  Assign the
    // active dynamic binding: Edebug deliberately rebinds this variable
    // around its recursive edit.
    if !error_matches_condition(interp, error, "minibuffer-quit") {
        interp.set_variable("executing-kbd-macro", Value::Nil, env);
    }
    let error_function = interp
        .lookup_var("command-error-function", env)
        .unwrap_or(Value::Nil);
    if !error_function.is_nil() {
        interp.call_function_value(
            error_function,
            None,
            &[
                crate::lisp::eval::error_condition_value(error),
                Value::String(String::new().into()),
                Value::Nil,
            ],
            env,
        )?;
    }
    Ok(())
}

fn error_matches_condition(interp: &Interpreter, error: &LispError, expected: &str) -> bool {
    let condition = error.condition_type();
    condition == expected
        || interp
            .get_symbol_property(&condition, "error-conditions")
            .and_then(|conditions| conditions.to_vec().ok())
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition
                        .as_symbol()
                        .is_ok_and(|condition| condition == expected)
                })
            })
}

fn execute_kbd_macro_self_insert(
    interp: &mut Interpreter,
    text: &str,
    event: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let command = Value::Symbol("self-insert-command".into());
    // GNU amalgamates consecutive self-insertions into one undo group;
    // any other preceding command starts a fresh group.
    if !matches!(
        interp.lookup_var("last-command", env),
        Some(Value::Symbol(last)) if last == "self-insert-command"
    ) {
        interp.buffer.push_undo_boundary();
    }
    set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
    interp.set_variable("deactivate-mark", Value::Nil, env);
    interp.set_variable("last-command-event", event.clone(), env);
    interp.set_variable("last-input-event", event.clone(), env);
    interp.set_variable("this-original-command", command.clone(), env);
    interp.set_variable("this-command", command.clone(), env);
    increment_num_input_keys(interp, env);
    safe_run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let dispatched_command = interp
        .lookup_var("this-command", env)
        .filter(|command| !command.is_nil())
        .unwrap_or_else(|| command.clone());
    if dispatched_command == command {
        prepare_native_kbd_command_body(interp, env)?;
        let repeat = interp
            .lookup_var("current-prefix-arg", env)
            .and_then(|prefix| prefix_numeric_value(&prefix).ok())
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(1)
            .max(0) as usize;
        insert_text_with_hooks(interp, &text.repeat(repeat), &[], &[], false, false, env)?;
    } else {
        execute_kbd_command_body(interp, &dispatched_command, env)?;
    }
    finish_kbd_macro_command_cycle(interp, command, dispatched_command, env)?;
    Ok(())
}

fn finish_kbd_macro_command(
    interp: &mut Interpreter,
    original_command: Value,
    this_command: Value,
    env: &mut Env,
) {
    if interp
        .lookup_var("defining-kbd-macro", env)
        .is_some_and(|value| value.is_truthy())
    {
        interp.kbd_macro_committed_len = interp.kbd_macro_definition.len();
    }
    interp.set_variable("real-last-command", original_command, env);
    interp.set_variable("last-command", this_command, env);
    if interp.lookup_var("last-repeatable-command", env).is_some() {
        let real_last_command = interp
            .lookup_var("real-last-command", env)
            .unwrap_or(Value::Nil);
        interp.set_variable("last-repeatable-command", real_last_command, env);
    }
    // The command loop re-establishes the selected window's buffer after
    // every command.  Lisp commands commonly use `save-current-buffer', so
    // selecting another window inside them can otherwise leave the command
    // loop's current buffer pointing at the window that was just quit.
    let selected_buffer = interp.selected_window_buffer_id();
    if interp.has_buffer_id(selected_buffer) {
        let _ = interp.set_current_buffer_id(selected_buffer);
    }
}

fn keyboard_macro_self_insert_text(event: &Value) -> Option<String> {
    let code = event.as_integer().ok()?;
    if !(0..=char::MAX as i64).contains(&code) {
        return None;
    }
    let ch = char::from_u32(code as u32)?;
    if ch.is_control() && ch != '\n' && ch != '\t' {
        return None;
    }
    Some(ch.to_string())
}

fn nth_list_element(list: &Value, count: &Value) -> Result<Value, LispError> {
    // GNU fns.c defines `nth' and list `elt' as car(nthcdr(...)).  Keep that
    // one traversal authority so negative counts, bignums, improper tails,
    // and circular lists cannot drift between the three public primitives.
    let tail = nthcdr_value(count, list)?;
    match tail {
        Value::Nil => Ok(Value::Nil),
        Value::Cons(ref cell) => Ok(cell.car.borrow().clone()),
        other => Err(wrong_type_argument("listp", other)),
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
            "cons" => {
                need_args(name, args, 2)?;
                Ok(Value::cons(args[0].clone(), args[1].clone()))
            }
            "car" => {
                need_args(name, args, 1)?;
                if let Some(view) = runtime_keymap_public_view(interp, &args[0]) {
                    view.car()
                } else {
                    args[0]
                        .car()
                        .map_err(|_| wrong_type_argument("listp", args[0].clone()))
                }
            }
            "cdr" => {
                need_args(name, args, 1)?;
                if let Some(view) = runtime_keymap_public_view(interp, &args[0]) {
                    view.cdr()
                } else {
                    args[0]
                        .cdr()
                        .map_err(|_| wrong_type_argument("listp", args[0].clone()))
                }
            }
            "car-safe" => {
                need_args(name, args, 1)?;
                Ok(match &args[0] {
                    Value::Cons(cell) => cell.car.borrow().clone(),
                    value => runtime_keymap_public_view(interp, value)
                        .and_then(|view| view.car().ok())
                        .unwrap_or(Value::Nil),
                })
            }
            "cdr-safe" => {
                need_args(name, args, 1)?;
                Ok(match &args[0] {
                    Value::Cons(cell) => cell.cdr.borrow().clone(),
                    value => runtime_keymap_public_view(interp, value)
                        .and_then(|view| view.cdr().ok())
                        .unwrap_or(Value::Nil),
                })
            }
            "identity" => {
                need_args(name, args, 1)?;
                Ok(args[0].clone())
            }
            "list" => Ok(Value::list(args.iter().cloned())),
            "nconc" => {
                let projected = args
                    .iter()
                    .map(|value| {
                        keymap_list_items(interp, value)
                            .map(|items| items.map(Value::list).unwrap_or_else(|| value.clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                nconc_values(&projected)
            }
            "append" => {
                let mut items: Vec<Value> = Vec::new();
                for (i, a) in args.iter().enumerate() {
                    let projected;
                    let a = if let Some(keymap_items) = keymap_list_items(interp, a)? {
                        projected = Value::list(keymap_items);
                        &projected
                    } else {
                        a
                    };
                    let is_last = i == args.len() - 1;
                    if is_last {
                        // `append` copies all preceding args and reuses the
                        // last one verbatim as the tail — even when it is a
                        // string or vector: (append '(2) "b") => (2 . "b").
                        let mut result = a.clone();
                        for item in items.into_iter().rev() {
                            result = Value::cons(item, result);
                        }
                        return Ok(result);
                    }
                    if let Some(string) = sequence_string_like(a) {
                        items.extend(string_sequence_values(&string));
                        continue;
                    }
                    if is_vector_like_value(interp, a) {
                        items.extend(sequence_values(interp, a)?);
                        continue;
                    }
                    items.extend(a.to_vec()?);
                }
                Ok(Value::list(items))
            }
            "nth" => {
                need_args(name, args, 2)?;
                if let Some(items) = keymap_list_items(interp, &args[1])? {
                    nth_list_element(&Value::list(items), &args[0])
                } else {
                    nth_list_element(&args[1], &args[0])
                }
            }
            "elt" => {
                need_args(name, args, 2)?;
                if matches!(args[0], Value::Cons(_))
                    && matches!(
                        args[0].to_vec().ok().and_then(|items| items.first().cloned()),
                        Some(Value::Symbol(symbol)) if symbol == "vector-literal"
                    )
                {
                    super::call(interp, "aref", args, env)
                } else if matches!(args[0], Value::Nil | Value::Cons(_)) {
                    nth_list_element(&args[0], &args[1])
                } else {
                    super::call(interp, "aref", args, env)
                }
            }
            "nthcdr" => {
                need_args(name, args, 2)?;
                if let Some(items) = keymap_list_items(interp, &args[1])? {
                    if matches!(&args[0], Value::Integer(count) if *count <= 0)
                        || matches!(&args[0], Value::BigInteger(count) if **count <= BigInt::from(0))
                    {
                        // Runtime keymaps project to GNU's cons-list surface,
                        // but nthcdr with a nonpositive count returns the
                        // original object, including its identity.
                        return Ok(args[1].clone());
                    }
                    return nthcdr_value(&args[0], &Value::list(items));
                }
                nthcdr_value(&args[0], &args[1])
            }
            "length" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sequence_length_value(interp, &args[0])?))
            }
            "safe-length" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(
                    keymap_list_items(interp, &args[0])?
                        .map(|items| items.len() as i64)
                        .unwrap_or_else(|| safe_list_length(&args[0])),
                ))
            }
            "length<" | "length>" | "length=" => {
                need_args(name, args, 2)?;
                let length = sequence_length_value(interp, &args[0])?;
                let target = args[1].as_integer()?;
                let matches = match name {
                    "length<" => length < target,
                    "length>" => length > target,
                    _ => length == target,
                };
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "reverse" => {
                need_args(name, args, 1)?;
                reverse_sequence_value(interp, &args[0])
            }
            "copy-alist" => {
                need_args(name, args, 1)?;
                copy_alist_value(&args[0])
            }
            "memq" | "memql" | "member" => {
                need_args(name, args, 2)?;
                #[derive(Clone, Copy)]
                enum MemTest {
                    Equal,
                    Eql,
                    Eq,
                }
                let test = match name {
                    "member" => MemTest::Equal,
                    "memql" => MemTest::Eql,
                    _ => MemTest::Eq,
                };
                let mut current = args[1].clone();
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    let next = match &current {
                        Value::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            if seen.step(crate::lisp::types::ConsCell::identity(cons_cell)) {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("circular-list".into()),
                                    Value::String("Circular list".into()),
                                ])));
                            }
                            let matches = {
                                let item = car.borrow();
                                match test {
                                    MemTest::Equal => values_equal(interp, &item, &args[0]),
                                    MemTest::Eql => values_eql(&item, &args[0]),
                                    MemTest::Eq => values_eq_in_env(interp, &item, &args[0], env),
                                }
                            };
                            if matches {
                                return Ok(current.clone());
                            }
                            cdr.borrow().clone()
                        }
                        Value::Nil => return Ok(Value::Nil),
                        other => {
                            let matches = match name {
                                "member" => values_equal(interp, other, &args[0]),
                                "memql" => values_eql(other, &args[0]),
                                _ => values_eq_in_env(interp, other, &args[0], env),
                            };
                            if matches {
                                return Ok(other.clone());
                            }
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("wrong-type-argument".into()),
                                Value::Symbol("listp".into()),
                                other.clone(),
                            ])));
                        }
                    };
                    current = next;
                }
            }
            "assq" | "rassq" => {
                need_args(name, args, 2)?;
                let want_car = name == "assq";
                let key = &args[0];
                let projected;
                let alist = if let Some(items) = keymap_list_items(interp, &args[1])? {
                    projected = Value::list(items);
                    &projected
                } else {
                    &args[1]
                };
                let mut seen = crate::lisp::types::CycleGuard::new();
                // Walk by cons cells rather than by cloned Values: one Rc
                // bump per step and no whole-Value churn.
                let mut cell = match alist {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(cell) => Rc::clone(cell),
                    other => {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("listp".into()),
                            other.clone(),
                        ])));
                    }
                };
                loop {
                    if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("circular-list".into()),
                            Value::String("Circular list".into()),
                        ])));
                    }
                    let matched = {
                        let item = cell.car.borrow();
                        match &*item {
                            Value::Cons(cons_cell) => {
                                let item_car = &cons_cell.car;
                                let item_cdr = &cons_cell.cdr;
                                let slot = if want_car { item_car } else { item_cdr };
                                let entry_key = slot.borrow();
                                match (&*entry_key, key) {
                                    (Value::Integer(a), Value::Integer(b)) => a == b,
                                    (Value::Symbol(a), Value::Symbol(b)) => a == b,
                                    (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
                                    (Value::Nil | Value::T, _)
                                    | (_, Value::Nil | Value::T)
                                    | (Value::Integer(_), Value::Symbol(_))
                                    | (Value::Symbol(_), Value::Integer(_)) => false,
                                    // GNU 30.2 fns.c implements assq/rassq
                                    // with EQ, whose lisp.h contract unwraps
                                    // symbol-with-position objects while the
                                    // dynamic mode is enabled.  Keep ordinary
                                    // scalar keys on the fast path above.
                                    (a @ Value::Record(_), b) | (a, b @ Value::Record(_)) => {
                                        values_eq_in_env(interp, a, b, env)
                                    }
                                    (a, b) => *a == *b,
                                }
                            }
                            _ => false,
                        }
                    };
                    if matched {
                        return Ok(cell.car.borrow().clone());
                    }
                    let tail = cell.cdr.borrow();
                    let next = match &*tail {
                        Value::Nil => return Ok(Value::Nil),
                        Value::Cons(next) => Rc::clone(next),
                        other => {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("wrong-type-argument".into()),
                                Value::Symbol("listp".into()),
                                other.clone(),
                            ])));
                        }
                    };
                    drop(tail);
                    cell = next;
                }
            }
            "rassoc" => {
                need_args(name, args, 2)?;
                let mut current = args[1].clone();
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    match current {
                        Value::Nil => return Ok(Value::Nil),
                        Value::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                            if seen.step(cell_id) {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("circular-list".into()),
                                    Value::String("Circular list".into()),
                                ])));
                            }
                            let item = car.borrow().clone();
                            if matches!(item, Value::Cons(_))
                                && values_equal(interp, &item.cdr()?, &args[0])
                            {
                                return Ok(item);
                            }
                            current = cdr.borrow().clone();
                        }
                        other => {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("wrong-type-argument".into()),
                                Value::Symbol("listp".into()),
                                other,
                            ])));
                        }
                    }
                }
            }
            "assoc" => {
                need_arg_range(name, args, 2, 3)?;
                let mut current = args[1].clone();
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    match current {
                        Value::Nil => return Ok(Value::Nil),
                        Value::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                            if seen.step(cell_id) {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("circular-list".into()),
                                    Value::String("Circular list".into()),
                                ])));
                            }
                            let item = car.borrow().clone();
                            if matches!(item, Value::Cons(_))
                                && if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                                {
                                    call_function_value(
                                        interp,
                                        testfn,
                                        &[args[0].clone(), item.car()?],
                                        env,
                                    )?
                                    .is_truthy()
                                } else {
                                    values_equal(interp, &item.car()?, &args[0])
                                }
                            {
                                return Ok(item);
                            }
                            current = cdr.borrow().clone();
                        }
                        other => {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("wrong-type-argument".into()),
                                Value::Symbol("listp".into()),
                                other,
                            ])));
                        }
                    }
                }
            }
            "assoc-string" => {
                need_arg_range(name, args, 2, 3)?;
                let items = args[1].to_vec()?;
                if items.is_empty() {
                    return Ok(Value::Nil);
                }
                let key = assoc_string_text(&args[0])?;
                let key = if args.get(2).is_some_and(|value| !value.is_nil()) {
                    assoc_string_folded_text(interp, &key)?
                } else {
                    key
                };
                for item in &items {
                    let thiscar = match item {
                        Value::Cons(_) => item.car()?,
                        _ => item.clone(),
                    };
                    let Some(candidate) = assoc_string_candidate_text(&thiscar) else {
                        continue;
                    };
                    let candidate = if args.get(2).is_some_and(|value| !value.is_nil()) {
                        assoc_string_folded_text(interp, &candidate)?
                    } else {
                        candidate
                    };
                    if candidate == key {
                        return Ok(item.clone());
                    }
                }
                Ok(Value::Nil)
            }

            // Fast native ports of the GNU cl-seq.el sequence functions.  The
            // interpreted Lisp definitions are semantically fine but far too
            // slow for the multi-million-element sequences in
            // cl-seq-test-bug24264, so these arms carry the native-override
            // metadata consumed by function definition.
            "mapcar" => {
                need_args(name, args, 2)?;
                let list = sequence_values(interp, &args[1])?;
                let mut results = Vec::new();
                for item in list {
                    results.push(call_function_value(interp, &args[0], &[item], env)?);
                }
                Ok(Value::list(results))
            }
            "mapcan" => {
                need_args(name, args, 2)?;
                let list = sequence_values(interp, &args[1])?;
                let mut mapped = Vec::with_capacity(list.len());
                for item in list {
                    mapped.push(call_function_value(interp, &args[0], &[item], env)?);
                }
                nconc_values(&mapped)
            }
            "mapc" => {
                need_args(name, args, 2)?;
                let list = sequence_values(interp, &args[1])?;
                for item in &list {
                    let _ = call_function_value(interp, &args[0], std::slice::from_ref(item), env)?;
                }
                Ok(args[1].clone())
            }
            "eval" => eval_impl(interp, args, env),
            "eval-buffer" => eval_buffer_impl(interp, args, env),
            "eval-region" => eval_region_impl(interp, args, env),

            "mapconcat" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let list = super::call(interp, "mapcar", &args[..2], env)?.to_vec()?;
                // GNU: a nil SEPARATOR stands for the empty string (subr-x's
                // string-join passes nil when no separator is given).
                let sep = if args.len() == 3 && !args[2].is_nil() {
                    let text = string_text(&args[2])?;
                    let multibyte = text.chars().any(|ch| (ch as u32) > 0x7F);
                    string_like(&args[2]).unwrap_or(StringLike {
                        text,
                        props: Vec::new(),
                        multibyte,
                        extended_chars: Vec::new(),
                    })
                } else {
                    StringLike {
                        text: String::new(),
                        props: Vec::new(),
                        multibyte: false,
                        extended_chars: Vec::new(),
                    }
                };
                let mut result = String::new();
                let mut props = Vec::new();
                for (index, item) in list.iter().enumerate() {
                    if index > 0 {
                        let offset = result.chars().count();
                        result.push_str(&sep.text);
                        props.extend(shift_string_props(&sep.props, offset));
                    }
                    if let Some(string) = string_like(item) {
                        let offset = result.chars().count();
                        result.push_str(&string.text);
                        props.extend(shift_string_props(&string.props, offset));
                    } else if item.is_nil() {
                    } else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("sequencep".into()),
                            item.clone(),
                        ])));
                    }
                }
                Ok(string_like_value(result, merge_string_props(props)))
            }
            "position-symbol" => {
                need_args(name, args, 2)?;
                let position = args[1].as_integer()?;
                Ok(interp.create_pseudovector(
                    crate::lisp::eval::RecordKind::SymbolWithPos,
                    "symbol-with-pos",
                    vec![args[0].clone(), Value::Integer(position)],
                ))
            }
            "symbol-with-pos-pos" => {
                need_args(name, args, 1)?;
                let (_, position) = symbol_with_pos_parts(interp, &args[0]).ok_or_else(|| {
                    LispError::TypeError("symbol-with-pos".into(), args[0].type_name())
                })?;
                Ok(Value::Integer(position))
            }
            "remove-pos-from-symbol" | "bare-symbol" => {
                need_args(name, args, 1)?;
                Ok(symbol_with_pos_parts(interp, &args[0])
                    .map(|(symbol, _)| symbol)
                    .unwrap_or_else(|| args[0].clone()))
            }
            "apply" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs("apply".into(), args.len()));
                }
                if args.len() == 1 {
                    let expanded_args = args[0].to_vec()?;
                    if expanded_args.len() < 2 {
                        return Err(LispError::WrongNumberOfArgs(
                            "apply".into(),
                            expanded_args.len(),
                        ));
                    }
                    let resolved = resolve_callable(interp, &expanded_args[0], env)?;
                    let original_name = expanded_args[0].as_symbol().ok();
                    return interp.call_function_value(
                        resolved,
                        original_name,
                        &expanded_args[1..],
                        env,
                    );
                }
                let func = &args[0];
                let last = &args[args.len() - 1];
                let mut all_args: Vec<Value> = args[1..args.len() - 1].to_vec();
                all_args.extend(sequence_values(interp, last)?);
                let resolved = resolve_callable(interp, func, env)?;
                let original_name = func.as_symbol().ok();
                interp.call_function_value(resolved, original_name, &all_args, env)
            }
            "funcall" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs("funcall".into(), 0));
                }
                let resolved = resolve_callable(interp, &args[0], env)?;
                let original_name = args[0].as_symbol().ok();
                interp.call_function_value(resolved, original_name, &args[1..], env)
            }
            "fset" => {
                need_args(name, args, 2)?;
                // GNU 30.2 data.c:Ffset uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                if args[1].is_nil() {
                    interp.set_function_binding(&symbol, None);
                    Ok(Value::Nil)
                } else {
                    interp.validate_function_binding(&symbol, &args[1])?;
                    interp.set_function_binding(&symbol, Some(args[1].clone()));
                    Ok(args[1].clone())
                }
            }
            "fmakunbound" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Ffmakunbound uses CHECK_SYMBOL/XSYMBOL and
                // returns its original symbol argument.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                // GNU voids the function cell outright; shadowed stale entries
                // (repeated defuns push duplicates) must not resurface.
                interp.remove_all_function_bindings(&symbol);
                Ok(args[0].clone())
            }
            "funcall-interactively" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                let func = resolve_callable(interp, &args[0], env)?;
                invoke_function_value(interp, &func, &args[1..], env)
            }
            "call-interactively" => call_interactively_impl(interp, args, env),

            "start-kbd-macro" => {
                need_arg_range(name, args, 1, 2)?;
                if interp
                    .lookup_var("defining-kbd-macro", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return Err(LispError::Signal("Already defining kbd macro".into()));
                }
                if args[0].is_truthy() {
                    let previous = interp
                        .lookup_var("last-kbd-macro", env)
                        .unwrap_or(Value::Nil);
                    interp.kbd_macro_definition = if let Some(string) = string_like(&previous) {
                        string
                            .text
                            .chars()
                            .map(|character| Value::Integer(character as i64))
                            .collect()
                    } else {
                        vector_items(&previous)?
                    };
                    interp.kbd_macro_committed_len = interp.kbd_macro_definition.len();
                    if !args.get(1).is_some_and(Value::is_truthy) {
                        execute_kbd_macro(interp, &[previous], env)?;
                    }
                } else {
                    interp.kbd_macro_definition.clear();
                    interp.kbd_macro_committed_len = 0;
                }
                interp.set_variable("defining-kbd-macro", Value::T, env);
                Ok(Value::Nil)
            }
            "end-kbd-macro" => {
                need_arg_range(name, args, 0, 2)?;
                if interp
                    .lookup_var("defining-kbd-macro", env)
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
                interp.set_variable("defining-kbd-macro", Value::Nil, env);
                interp
                    .kbd_macro_definition
                    .truncate(interp.kbd_macro_committed_len);
                let last_macro = Value::list(
                    std::iter::once(Value::symbol("vector-literal"))
                        .chain(interp.kbd_macro_definition.iter().cloned()),
                );
                interp.set_variable("last-kbd-macro", last_macro.clone(), env);
                if repeat == 0 {
                    let mut execute_args = vec![last_macro, Value::Integer(0)];
                    if let Some(loop_function) = args.get(1) {
                        execute_args.push(loop_function.clone());
                    }
                    execute_kbd_macro(interp, &execute_args, env)?;
                } else if repeat > 1 {
                    let mut execute_args =
                        vec![last_macro, Value::Integer(repeat.saturating_sub(1))];
                    if let Some(loop_function) = args.get(1) {
                        execute_args.push(loop_function.clone());
                    }
                    execute_kbd_macro(interp, &execute_args, env)?;
                }
                Ok(Value::Nil)
            }
            "call-last-kbd-macro" => {
                need_arg_range(name, args, 0, 2)?;
                let macro_value = interp
                    .lookup_var("last-kbd-macro", env)
                    .unwrap_or(Value::Nil);
                interp.set_variable(
                    "this-command",
                    interp.lookup_var("last-command", env).unwrap_or(Value::Nil),
                    env,
                );
                interp.set_variable("real-this-command", macro_value.clone(), env);
                if interp
                    .lookup_var("defining-kbd-macro", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return Err(LispError::Signal(
                        "Can't execute anonymous macro while defining one".into(),
                    ));
                }
                if macro_value.is_nil() {
                    return Err(LispError::Signal("No kbd macro has been defined".into()));
                }
                let mut execute_args = vec![macro_value];
                execute_args.extend_from_slice(args);
                execute_kbd_macro(interp, &execute_args, env)?;
                interp.set_variable(
                    "this-command",
                    interp.lookup_var("last-command", env).unwrap_or(Value::Nil),
                    env,
                );
                Ok(Value::Nil)
            }
            "execute-kbd-macro" => execute_kbd_macro(interp, args, env),
            "cancel-kbd-macro-events" => {
                need_args(name, args, 0)?;
                interp
                    .kbd_macro_definition
                    .truncate(interp.kbd_macro_committed_len);
                Ok(Value::Nil)
            }
            "store-kbd-macro-event" => {
                need_args(name, args, 1)?;
                if interp
                    .lookup_var("defining-kbd-macro", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    interp.kbd_macro_definition.push(args[0].clone());
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
                let restore = interp.bind_special_variable("track-mouse", Value::T, env)?;
                let result = call_function_value(interp, &args[0], &[], env);
                interp.restore_special_binding(restore, env)?;
                result
            }
            "internal-handle-focus-in" => {
                need_args(name, args, 1)?;
                let event = args[0].to_vec().unwrap_or_default();
                let valid = matches!(
                    event.as_slice(),
                    [Value::Symbol(kind), Value::Frame(frame), ..]
                        if kind == "focus-in" && interp.frame_is_live(*frame)
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
                        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0].clone()))?
                        .text,
                );
                if path.exists() {
                    std::fs::remove_file(&path)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                }
                let mut options = std::fs::OpenOptions::new();
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
                if let Some(stuff_string) = args.first()
                    && !stuff_string.is_nil()
                    && string_like(stuff_string).is_none()
                {
                    return Err(LispError::TypeError(
                        "stringp".into(),
                        stuff_string.type_name(),
                    ));
                }
                run_named_hooks(interp, "suspend-hook", env, None)?;
                // Emaxx currently exposes a headless batch terminal.  There is no
                // foreground terminal process group to stop and later resume;
                // the observable native contract in that environment is the
                // paired hook transition.
                run_named_hooks(interp, "suspend-resume-hook", env, None)?;
                Ok(Value::Nil)
            }
            "recursive-edit" => {
                need_args(name, args, 0)?;
                recursive_edit(interp, env)
            }
            "exit-recursive-edit" | "abort-recursive-edit" => {
                need_args(name, args, 0)?;
                if interp.command_loop_recursion_depth == 0 {
                    return Err(LispError::Signal("No recursive edit is in progress".into()));
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
                Ok(Value::Integer(interp.command_loop_recursion_depth as i64))
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
                let read_only = interp
                    .lookup_var("buffer-read-only", env)
                    .is_some_and(|value| value.is_truthy());
                let inhibited = interp
                    .lookup_var("inhibit-read-only", env)
                    .is_some_and(|value| value.is_truthy());
                if read_only && !inhibited {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("buffer-read-only".into()),
                        Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone()),
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
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0].clone()))?;
                let keys = string
                    .text
                    .chars()
                    .map(|character| {
                        if character as u32 == 248 {
                            Value::Integer(i64::from(b'x') | KEY_DESCRIPTION_META_BIT)
                        } else {
                            Value::Integer(character as i64)
                        }
                    })
                    .collect::<Vec<_>>();
                set_command_key_state(interp, keys, Vec::new(), env);
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
                ensure_interaction_allowed(interp, env)?;
                let event = read_key_sequence_event(interp, env)?;
                let events = vec![event];
                set_command_key_state(interp, events.clone(), events.clone(), env);
                Ok(event_array(&events, name == "read-key-sequence-vector"))
            }
            "read-event" | "read-char" | "read-char-exclusive" => {
                let read_event = name == "read-event";
                let timed_poll = args.len() >= 3 && args[2].is_truthy();
                if timed_poll {
                    let timeout = wait_duration(std::slice::from_ref(&args[2]))?;
                    let previous_wait = interp.set_waiting_for_user_input(true);
                    let wait_result =
                        wait_pumping_processes(interp, env, Some(timeout), false, None);
                    interp.set_waiting_for_user_input(previous_wait);
                    wait_result?;
                    if !interaction_allowed(interp, env) {
                        return Ok(Value::Nil);
                    }
                    return match pop_unread_command_event_value(interp, env) {
                        Ok(event) => {
                            if read_event {
                                normalize_input_event_value(event)
                            } else {
                                Ok(Value::Integer(unread_command_event_char(&event)? as i64))
                            }
                        }
                        Err(_) => Ok(Value::Nil),
                    };
                }
                ensure_interaction_allowed(interp, env)?;
                let event = pop_unread_command_event_value(interp, env)?;
                if read_event {
                    normalize_input_event_value(event)
                } else {
                    Ok(Value::Integer(unread_command_event_char(&event)? as i64))
                }
            }
            "read-string" | "read-from-minibuffer" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                ensure_interaction_allowed(interp, env)?;
                let initial = args
                    .get(1)
                    .and_then(string_like)
                    .map(|string| string.text)
                    .unwrap_or_default();
                let prompt = string_text(&args[0])?;
                let local_map = args
                    .get(2)
                    .filter(|map| !map.is_nil())
                    .cloned()
                    .or_else(|| interp.lookup_var("minibuffer-local-map", env))
                    .unwrap_or(Value::Nil);
                let mut contents = read_minibuffer_text_from_unread_events(
                    interp, env, &prompt, &initial, &local_map,
                )?;
                if contents.is_none() {
                    contents = read_minibuffer_text_from_kbd_macro(
                        interp, env, &prompt, &initial, &local_map,
                    )?;
                }
                if contents.is_none() {
                    contents = Some(read_minibuffer_text_without_queued_events(
                        interp, env, &prompt, &initial, &local_map,
                    )?);
                }
                if let Some(contents) = contents {
                    if name == "read-from-minibuffer" && args.get(3).is_some_and(Value::is_truthy) {
                        let parsed = super::call(
                            interp,
                            "read-from-string",
                            &[Value::String(contents.into())],
                            env,
                        )?;
                        return Ok(parsed.cons_values().map(|(car, _)| car).unwrap_or(parsed));
                    }
                    if contents.is_empty() {
                        // GNU read-string returns DEFAULT-VALUE unchanged on
                        // empty input, even when it is not a string.  A list
                        // default contributes its first element.  In contrast,
                        // read-from-minibuffer's DEFAULT is only history input
                        // and does not replace an empty return value.
                        if name == "read-string"
                            && let Some(default) = args.get(3)
                        {
                            let default = match default.cons_values() {
                                Some((head, _)) => head,
                                None => default.clone(),
                            };
                            return Ok(default);
                        }
                    }
                    return Ok(Value::String(contents.into()));
                }
                Ok(Value::String(String::new().into()))
            }
            "completing-read" => completing_read(interp, args, env),
            "read-buffer" => {
                need_arg_range(name, args, 1, 4)?;
                let buffers = super::call(interp, "buffer-list", &[], env)?
                    .to_vec()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|buffer| match buffer {
                        Value::Buffer(buffer) => Some(Value::String(buffer.name.clone())),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                completing_read(
                    interp,
                    &[
                        args[0].clone(),
                        Value::list(buffers),
                        args.get(3).cloned().unwrap_or(Value::Nil),
                        args.get(2).cloned().unwrap_or(Value::Nil),
                        Value::Nil,
                        Value::Nil,
                        args.get(1).cloned().unwrap_or(Value::Nil),
                    ],
                    env,
                )
            }
            "read-command" | "read-variable" => {
                need_arg_range(name, args, 1, 2)?;
                let default = args.get(1).cloned().unwrap_or(Value::Nil);
                let default = match default {
                    Value::Symbol(symbol) => Value::String(
                        crate::lisp::types::visible_symbol_name(&symbol)
                            .to_string()
                            .into(),
                    ),
                    other => other,
                };
                let obarray = interp.lookup_var("obarray", env).unwrap_or(Value::Nil);
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
                        args[0].clone(),
                        obarray.clone(),
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
                    let symbol = string_text(&value)?;
                    intern_in_obarray(interp, &obarray, &symbol)
                }
            } // GNU byte-run.el defines this as an ordinary &rest function.
              // Keeping it on the callable dispatch route makes interpreted
              // and byte-compiled calls share one function-cell contract.
        }
    }
);
