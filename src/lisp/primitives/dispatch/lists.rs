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
        // Fexecute_kbd_macro's command loop handles only `minibuffer-quit'
        // (see execute_kbd_macro_command); register that frame so outer
        // handler-binds see the same handler landscape GNU's
        // signal_or_quit does.
        let handler_start =
            interp.push_condition_case_handler(vec![Value::Symbol("minibuffer-quit".into())]);
        let iteration = match run_kbd_macro_events(interp, env) {
            // GNU's outermost command loop catches `top-level`, terminating
            // the keyboard macro without propagating an error.
            Err(LispError::Throw(tag, _)) if matches!(&tag, Value::Symbol(symbol) if symbol == "top-level") => {
                Ok(())
            }
            other => other,
        };
        interp.pop_handler_bindings(handler_start);
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

pub(crate) fn sync_kbd_macro_execution(
    interp: &mut Interpreter,
    env: &Env,
) -> Result<(), LispError> {
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
    prompt: &Value,
    initial: &str,
    initial_value: &Value,
    local_map: &Value,
) -> Result<Option<String>, LispError> {
    // keyboard.c read_char reads from `executing-kbd-macro' whenever it is
    // non-nil, however it came to be bound: a Lisp `let' of the variable
    // (ert-simulate-keys binds it to t) drives the recursive minibuffer
    // loop just like `execute-kbd-macro'.  A value that is neither a string
    // nor a vector carries no events, so the loop ends at once and the
    // read returns the minibuffer's contents, as GNU's command loop does
    // at the end of a macro.
    let Some(synthesized) = ensure_kbd_macro_execution_from_variable(interp, env)? else {
        return Ok(None);
    };
    let saved_buffer_id = prepare_kbd_macro_minibuffer_entry(interp, env)?;
    let result = (|| {
        let minibuffer =
            activate_minibuffer(interp, prompt, initial_value, local_map.clone(), env)?;
        run_active_minibuffer(interp, env, minibuffer, |interp, env| {
            read_minibuffer_text_from_kbd_macro_inner(interp, env, initial)
        })
    })();
    if interp.has_buffer_id(saved_buffer_id) {
        let _ = interp.set_current_buffer_id(saved_buffer_id);
    }
    if synthesized {
        finish_synthesized_kbd_macro_execution(interp, env);
    }
    result
}

/// None when no keyboard macro is executing; otherwise whether an execution
/// state had to be synthesized from a bare `executing-kbd-macro' binding
/// (the caller then hands it to `finish_synthesized_kbd_macro_execution').
fn ensure_kbd_macro_execution_from_variable(
    interp: &mut Interpreter,
    env: &Env,
) -> Result<Option<bool>, LispError> {
    if !interp.kbd_macro_executions.is_empty() {
        return Ok(Some(false));
    }
    let macro_value = interp
        .lookup_var("executing-kbd-macro", env)
        .unwrap_or(Value::Nil);
    if macro_value.is_nil() {
        return Ok(None);
    }
    let events = if let Some(string) = string_like(&macro_value) {
        string
            .text
            .chars()
            .map(|character| Value::Integer(character as i64))
            .collect()
    } else if is_vector_value(&macro_value) {
        vector_items(&macro_value)?
    } else {
        Vec::new()
    };
    let index = interp
        .lookup_var("executing-kbd-macro-index", env)
        .and_then(|value| value.as_integer().ok())
        .and_then(|index| usize::try_from(index).ok())
        .unwrap_or(0);
    interp
        .kbd_macro_executions
        .push(crate::lisp::eval::KbdMacroExecutionState { events, index });
    Ok(Some(true))
}

/// read_char advanced executing_kbd_macro_index as it consumed events.
fn finish_synthesized_kbd_macro_execution(interp: &mut Interpreter, env: &mut Env) {
    if let Some(state) = interp.kbd_macro_executions.pop() {
        interp.set_variable(
            "executing-kbd-macro-index",
            Value::Integer(i64::try_from(state.index).unwrap_or(i64::MAX)),
            env,
        );
    }
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
    // The recursive minibuffer command loop, driven by the macro's
    // remaining events.  Keys resolve through the active keymaps -- the
    // minibuffer's own local map included -- and dispatch the real
    // commands; nothing is intercepted by event code.  GNU's read_minibuf
    // wraps this loop in a `catch \='exit`: `exit-minibuffer' and its
    // relatives (minibuffer-complete-and-exit, read--expression-try-read)
    // return the submitted text by throwing to that tag.
    let mut pending_keys: Vec<String> = Vec::new();
    let mut pending_events: Vec<Value> = Vec::new();
    while let Some(event) = current_kbd_macro_event(interp, 0) {
        // `read-kbd-macro' can retain GNU's modifier bits on character
        // events, whereas literal macro strings contain resolved control
        // bytes.  Minibuffer editing must see C-a/C-k identically in both
        // representations.
        let mut event = crate::lisp::primitives::reader_key_event_value(event);
        let code = match &event {
            Value::Integer(code) => *code,
            Value::Symbol(name) => function_key_default_translation(name).unwrap_or(-1),
            _ => -1,
        };
        if code < 0 {
            break;
        }
        let key = Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
        let mut event_key = key_sequence_binding_text(&key)?;
        if matches!(&event, Value::Symbol(_)) && !event_key.starts_with('<') {
            event_key = format!("<{event_key}>");
        }
        // GNU's local-function-key-map translates unbound function-key
        // symbols to their ASCII equivalents before lookup.
        if pending_keys.is_empty()
            && matches!(&event, Value::Symbol(_))
            && key_binding(interp, &event_key, false, false, env)?.is_nil()
            && !key_sequence_is_prefix(interp, &event_key, env)?
        {
            event = Value::Integer(code);
            let translated = Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
            event_key = key_sequence_binding_text(&translated)?;
        }
        advance_kbd_macro_index(interp, 1, env);
        pending_keys.push(event_key);
        pending_events.push(event.clone());
        let binding_key = pending_keys.join(" ");
        let binding = key_binding(interp, &binding_key, false, false, env)?;
        if is_keymap_value(interp, &binding) || key_sequence_is_prefix(interp, &binding_key, env)? {
            load_autoloaded_prefix_map(interp, &binding, env)?;
            continue;
        }
        if !binding.is_nil() {
            // GNU read_minibuf establishes `catch \='exit` around its
            // recursive edit; register the tag so the exiting command's
            // `throw' reaches this boundary instead of failing `no-catch'.
            interp.push_catch_tag(Value::Symbol("exit".into()));
            let dispatch = execute_kbd_macro_command(interp, &binding, &pending_events, env);
            interp.pop_catch_tag();
            match dispatch {
                Ok(()) => {}
                Err(LispError::Throw(tag, _)) if matches!(&tag, Value::Symbol(name) if name == "exit") =>
                {
                    // The exiting command leaves the recursive loop before
                    // its post-command phase.  The prompting command
                    // resumes, consumes the submitted text, and only then
                    // runs its own post-command hook.
                    sync_kbd_macro_execution(interp, env)?;
                    return active_minibuffer_text(interp, env).map(Some);
                }
                Err(error) => return Err(error),
            }
            sync_kbd_macro_execution(interp, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        if pending_keys.len() == 1
            && let Some(text) = keyboard_macro_self_insert_text(&event)
        {
            execute_kbd_macro_self_insert(interp, &text, &event, env)?;
            sync_kbd_macro_execution(interp, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        pending_keys.clear();
        pending_events.clear();
    }
    active_minibuffer_text(interp, env).map(Some)
}

fn read_minibuffer_text_from_batch_stdin(prompt: &Value) -> Result<String, LispError> {
    print!("{}", string_text(prompt)?);
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

#[allow(clippy::too_many_arguments)]
fn read_minibuffer_text_without_queued_events(
    interp: &mut Interpreter,
    env: &mut Env,
    prompt: &Value,
    initial: &str,
    initial_value: &Value,
    local_map: &Value,
    history: Value,
    batch_stdin: bool,
) -> Result<String, LispError> {
    let minibuffer = activate_minibuffer(interp, prompt, initial_value, local_map.clone(), env)?;
    run_active_minibuffer(interp, env, minibuffer, |interp, env| {
        if batch_stdin {
            read_minibuffer_text_from_batch_stdin(prompt)
        } else if crate::lisp::primitives::has_tty_event_reader() {
            // A live terminal reads through the recursive minibuffer
            // command loop over the real Lisp keymaps; a session without
            // that machinery keeps the native editing subset.  C-g inside
            // either signals GNU's `quit'.
            crate::lisp::primitives::interactive_minibuffer_read(interp, env, initial, &history)
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
    prompt: &Value,
    initial: &str,
    initial_value: &Value,
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
        let minibuffer =
            activate_minibuffer(interp, prompt, initial_value, local_map.clone(), env)?;
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
            // GNU dispatches RET through the active completion keymap:
            // minibuffer-local-must-match-map binds it to
            // minibuffer-complete-and-exit, which refuses input that
            // test-completion rejects and keeps reading (ERC's
            // switch-to-buffer journeys clear the input and retry after
            // exactly such a refusal).  Blank input falls through: the
            // default's substitution belongs to completing-read-default.
            if interp
                .lookup_var("minibuffer--require-match", env)
                .is_some_and(|value| value.is_truthy())
            {
                // The prompt occupies the buffer front (minibuffer-prompt-end);
                // the submission validates only the user's input after it.
                let prompt_length = interp
                    .minibuffer_prompt_text()
                    .map(|prompt| prompt.chars().count())
                    .unwrap_or(0);
                let contents: Vec<char> = interp
                    .buffer
                    .buffer_string()
                    .chars()
                    .skip(prompt_length)
                    .collect();
                if !contents.is_empty() {
                    let collection = interp
                        .lookup_var("minibuffer-completion-table", env)
                        .unwrap_or(Value::Nil);
                    let predicate = interp
                        .lookup_var("minibuffer-completion-predicate", env)
                        .filter(|value| !value.is_nil());
                    if crate::lisp::primitives::completion::minibuffer_submission(
                        interp,
                        env,
                        &contents,
                        &collection,
                        predicate.as_ref(),
                        true,
                        None,
                    )?
                    .is_none()
                    {
                        continue;
                    }
                }
            }
            break;
        }
        pending_keys.push(event_key);
        pending_events.push(event.clone());
        let binding_key = pending_keys.join(" ");

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
    // With the unread events gone, read_char turns to `executing-kbd-macro'
    // and the recursive loop continues in this same minibuffer.
    if let Some(synthesized) = ensure_kbd_macro_execution_from_variable(interp, env)? {
        let result = read_minibuffer_text_from_kbd_macro_inner(interp, env, _initial);
        if synthesized {
            finish_synthesized_kbd_macro_execution(interp, env);
        }
        return result;
    }
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
        value.clone(),
    ]))
}

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
        // read_char can push a non-digit terminator back onto
        // `unread-command-events' while rewinding the public macro cursor.
        // GNU's next command-loop read consumes that unread event before it
        // returns to the same event in the keyboard macro.
        let mut unread = crate::lisp::primitives::unread_command_events(interp, env)?;
        let next_event = if unread.is_empty() {
            current_kbd_macro_event(interp, 0).map(|event| (event, true))
        } else {
            let event = unread.remove(0);
            interp.set_variable("unread-command-events", Value::list(unread), env);
            Some((event, false))
        };
        let Some((mut event, from_macro)) = next_event else {
            // read_key_sequence increments this counter before reporting the
            // end of a keyboard macro to the command loop.
            increment_num_input_keys(interp, env);
            // command_loop_1 zeroes this_command_key_count after each command,
            // so the end-of-macro read leaves this-single-command-keys empty.
            // kmacro-call-macro keys its repeat-map offer on that emptiness;
            // a stale multi-key sequence here armed a phantom repeat map that
            // swallowed the first key of the next macro.
            set_command_key_state(interp, Vec::new(), Vec::new(), env);
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
        pending_keys.push(event_key);
        pending_events.push(event.clone());
        let binding_key = pending_keys.join(" ");
        let binding = key_binding(interp, &binding_key, false, false, env)?;
        if is_keymap_value(interp, &binding) || key_sequence_is_prefix(interp, &binding_key, env)? {
            load_autoloaded_prefix_map(interp, &binding, env)?;
            if from_macro {
                advance_kbd_macro_index(interp, 1, env);
            }
            continue;
        }
        if !binding.is_nil() {
            if from_macro {
                advance_kbd_macro_index(interp, 1, env);
            }
            execute_kbd_macro_command(interp, &binding, &pending_events, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        if pending_keys.len() == 1
            && let Some(text) = keyboard_macro_self_insert_text(&event)
        {
            if from_macro {
                advance_kbd_macro_index(interp, 1, env);
            }
            execute_kbd_macro_self_insert(interp, &text, &event, env)?;
            pending_keys.clear();
            pending_events.clear();
            continue;
        }
        pending_keys.clear();
        pending_events.clear();
        if from_macro {
            advance_kbd_macro_index(interp, 1, env);
        }
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
    // GNU's recursive edit runs command_loop_2 under
    // internal_condition_case with `error': signal_or_quit stops its
    // handler-bind scan at that frame, so a handler-bind OUTSIDE the
    // recursive edit (ert's test wrapper) must not fire for a command
    // error this loop is about to report itself.
    let handler_start = interp.push_condition_case_handler(vec![Value::Symbol("error".into())]);
    let result = entry_hooks
        .and_then(|()| run_recursive_kbd_command_loop(interp, env))
        // With no more events to dispatch the command loop goes idle, which
        // processes queued file notifications and fires due timers.  Loaded
        // timer.el owns GNU timer objects in `timer-list'; the native queue
        // remains the bootstrap path, so a real command-loop pump must drain
        // both representations just like the other event-waiting paths.
        .and_then(|()| interp.service_file_notifications(env).map(|_| ()))
        .and_then(|()| interp.run_pending_timer_events(env).map(|_| ()));
    interp.pop_handler_bindings(handler_start);
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
            Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => return Err(error),
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
        if matches!(error, LispError::Throw(_, _) | LispError::Terminate(_))
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
                    // fns.c concat_to_list: CLOSUREP args flatten to their
                    // slots (edebug-unwrap* rebuilds compiled closures with
                    // `(nthcdr 3 (append fn ()))').
                    match a {
                        Value::Lambda(lambda) => {
                            items.extend(interp.interpreted_closure_slots(lambda));
                            continue;
                        }
                        Value::Record(id) => {
                            if let Some(record) = interp.find_record(*id)
                                && record.kind == crate::lisp::eval::RecordKind::Closure
                            {
                                items.extend(record.slots.iter().cloned());
                                continue;
                            }
                        }
                        _ => {}
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
                                    MemTest::Equal => {
                                        values_equal_in_env(interp, &item, &args[0], env)
                                    }
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
                                "member" => values_equal_in_env(interp, other, &args[0], env),
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
                                && values_equal_in_env(interp, &item.cdr()?, &args[0], env)
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
                                    values_equal_in_env(interp, &item.car()?, &args[0], env)
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
                        props.extend(copied_string_props(&sep.props, offset));
                    }
                    if let Some(string) = string_like(item) {
                        let offset = result.chars().count();
                        result.push_str(&string.text);
                        props.extend(copied_string_props(&string.props, offset));
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
                // data.c Fposition_symbol: SYM goes through Fbare_symbol
                // (any bare symbol passes, nil and t included; a symbol
                // with position yields its symbol), and POS is a fixnum OR
                // a symbol with position whose position is borrowed (cconv
                // repositions `ignore' from the unused variable this way).
                let bare = match &args[0] {
                    Value::Symbol(_) | Value::Nil | Value::T => args[0].clone(),
                    other => symbol_with_pos_parts(interp, other)
                        .map(|(symbol, _)| symbol)
                        .ok_or_else(|| bare_symbol_type_error(&args[0]))?,
                };
                let position = match &args[1] {
                    Value::Integer(position) => *position,
                    other => symbol_with_pos_parts(interp, other)
                        .map(|(_, position)| position)
                        .ok_or_else(|| {
                            LispError::WrongTypeArgument(
                                "fixnum-or-symbol-with-pos-p".into(),
                                args[1].clone(),
                            )
                        })?,
                };
                Ok(interp.create_pseudovector(
                    crate::lisp::eval::RecordKind::SymbolWithPos,
                    "symbol-with-pos",
                    vec![bare, Value::Integer(position)],
                ))
            }
            "symbol-with-pos-pos" => {
                need_args(name, args, 1)?;
                let (_, position) = symbol_with_pos_parts(interp, &args[0]).ok_or_else(|| {
                    LispError::TypeError("symbol-with-pos".into(), args[0].type_name())
                })?;
                Ok(Value::Integer(position))
            }
            "remove-pos-from-symbol" => {
                // data.c Fremove_pos_from_symbol: any non-symbol-with-pos
                // argument comes back unchanged, no type check.
                need_args(name, args, 1)?;
                Ok(symbol_with_pos_parts(interp, &args[0])
                    .map(|(symbol, _)| symbol)
                    .unwrap_or_else(|| args[0].clone()))
            }
            "bare-symbol" => {
                // data.c Fbare_symbol: unlike remove-pos-from-symbol, a
                // non-symbol argument signals wrong-type-argument.
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::Symbol(_) | Value::Nil | Value::T => Ok(args[0].clone()),
                    other => symbol_with_pos_parts(interp, other)
                        .map(|(symbol, _)| symbol)
                        .ok_or_else(|| bare_symbol_type_error(&args[0])),
                }
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
                // callint.c Ffuncall_interactively just funcalls its
                // arguments: the advised symbol's own backtrace frame is
                // recorded by the ordinary call path, which is the exact
                // shape nadvice's called-interactively-p skip walks
                // (lambda, apply, SYMBOL, funcall-interactively).
                interp.call_function_value(args[0].clone(), None, &args[1..], env)
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
                let status = if args[0].is_truthy() {
                    "Appending to kbd macro..."
                } else {
                    "Defining kbd macro..."
                };
                super::call(interp, "message", &[Value::String(status.into())], env)?;
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
                super::call(
                    interp,
                    "message",
                    &[Value::String("Keyboard macro defined".into())],
                    env,
                )?;
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
                        .ok_or_else(|| {
                            LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                        })?
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
                    return Err(LispError::WrongTypeArgument(
                        "stringp".into(),
                        stuff_string.clone(),
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
                let string = string_like(&args[0]).ok_or_else(|| {
                    LispError::WrongTypeArgument("stringp".into(), args[0].clone())
                })?;
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
                    // A live terminal answers the first event inside the
                    // window, or nil when it elapses (keyboard.c's timed
                    // read); the process pump below is the batch stand-in.
                    if crate::lisp::primitives::has_tty_event_poller() {
                        return match crate::lisp::primitives::read_tty_event_with_timeout(
                            interp, env, timeout,
                        )? {
                            Some(event) => {
                                if read_event {
                                    normalize_input_event_value(event)
                                } else {
                                    Ok(Value::Integer(unread_command_event_char(&event)? as i64))
                                }
                            }
                            None => Ok(Value::Nil),
                        };
                    }
                    let previous_wait = interp.set_waiting_for_user_input(true);
                    let wait_result =
                        wait_pumping_processes(interp, env, Some(timeout), false, None, None, true);
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
                // GNU's read_char enters redisplay before blocking for
                // input: window-configuration changes a command made
                // before reading (rmc's help pop-up, y-or-n-p's prompt
                // context) reach the glass while the read waits.
                crate::lisp::primitives::run_tty_frame_redraw(interp, env);
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
                let initial_value = args.get(1).cloned().unwrap_or(Value::Nil);
                let initial = string_like(&initial_value)
                    .map(|string| string.text)
                    .unwrap_or_default();
                let prompt = args[0].clone();
                if string_like(&prompt).is_none() {
                    return Err(wrong_type_argument("stringp", prompt));
                }
                let local_map = (name == "read-from-minibuffer")
                    .then(|| args.get(2).filter(|map| !map.is_nil()).cloned())
                    .flatten()
                    .or_else(|| interp.lookup_var("minibuffer-local-map", env))
                    .unwrap_or(Value::Nil);
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
                // minibuf.c read_minibuf takes read_minibuf_noninteractive
                // (stdin, no history) only while `noninteractive' and no
                // keyboard macro executes; it does not look at
                // `unread-command-events'.  Every other read goes through
                // the recursive command loop, whose read_char consumes
                // unread events first and then the macro.
                let batch_stdin = interp
                    .lookup_var("noninteractive", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp
                        .lookup_var("executing-kbd-macro", env)
                        .is_none_or(|value| value.is_nil());
                let mut contents = None;
                if !batch_stdin {
                    contents = read_minibuffer_text_from_unread_events(
                        interp,
                        env,
                        &prompt,
                        &initial,
                        &initial_value,
                        &local_map,
                    )?;
                    if contents.is_none() {
                        contents = read_minibuffer_text_from_kbd_macro(
                            interp,
                            env,
                            &prompt,
                            &initial,
                            &initial_value,
                            &local_map,
                        )?;
                    }
                }
                if contents.is_none() {
                    contents = Some(read_minibuffer_text_without_queued_events(
                        interp,
                        env,
                        &prompt,
                        &initial,
                        &initial_value,
                        &local_map,
                        history.clone(),
                        batch_stdin,
                    )?);
                }
                if let Some(contents) = contents {
                    if !batch_stdin {
                        // read_minibuf adds the value to HIST after the
                        // recursive edit, or DEFAULT (its first element
                        // when a list) for an empty value; the noninteractive
                        // reader returns before that point.
                        let histstring = if contents.is_empty() {
                            match default.cons_values() {
                                Some((head, _)) => head,
                                None => default.clone(),
                            }
                        } else {
                            Value::String(contents.clone().into())
                        };
                        if let Some(text) = string_like(&histstring).map(|text| text.text)
                            && let Some(variable) =
                                crate::lisp::primitives::completion::history_variable_name(&history)
                        {
                            crate::lisp::primitives::completion::push_minibuffer_history(
                                interp, env, &variable, &text,
                            );
                        }
                    }
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
                let default = match args.get(1).cloned().unwrap_or(Value::Nil) {
                    Value::Buffer(buffer) => Value::String(buffer.name.clone()),
                    other => other,
                };
                if let Some(function) = interp
                    .lookup_var("read-buffer-function", env)
                    .filter(|function| !function.is_nil())
                {
                    let mut call_args = vec![
                        args[0].clone(),
                        default,
                        args.get(2).cloned().unwrap_or(Value::Nil),
                    ];
                    if let Some(predicate) = args.get(3) {
                        call_args.push(predicate.clone());
                    }
                    return call_function_value(interp, &function, &call_args, env);
                }
                let prompt = if default.is_nil() {
                    args[0].clone()
                } else {
                    let raw = string_text(&args[0])?;
                    let stem = raw
                        .strip_suffix(": ")
                        .or_else(|| raw.strip_suffix(':'))
                        .or_else(|| raw.strip_suffix(' '))
                        .unwrap_or(&raw);
                    let shown_default = default.car().unwrap_or_else(|_| default.clone());
                    call_function_value(
                        interp,
                        &Value::Symbol("format-prompt".into()),
                        &[Value::String(stem.into()), shown_default.clone()],
                        env,
                    )
                    .or_else(|error| {
                        if matches!(error, LispError::VoidFunction(_)) {
                            Ok(Value::String(
                                format!(
                                    "{stem} (default {}): ",
                                    string_text(&shown_default)
                                        .unwrap_or_else(|_| format!("{shown_default}"))
                                )
                                .into(),
                            ))
                        } else {
                            Err(error)
                        }
                    })?
                };
                // minibuf.c's read_buffer completes over Vbuffer_alist:
                // (NAME . BUFFER) conses, so a PREDICATE receives the pair
                // and can inspect the buffer object (ERC's foreign-buffer
                // filter reads buffer-local variables through the cdr).
                let buffers = super::call(interp, "buffer-list", &[], env)?
                    .to_vec()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|buffer| match buffer {
                        Value::Buffer(handle) => Some(Value::cons(
                            Value::String(handle.name.clone()),
                            Value::Buffer(handle),
                        )),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                completing_read(
                    interp,
                    &[
                        prompt,
                        Value::list(buffers),
                        args.get(3).cloned().unwrap_or(Value::Nil),
                        args.get(2).cloned().unwrap_or(Value::Nil),
                        Value::Nil,
                        Value::Symbol("buffer-name-history".into()),
                        default,
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
