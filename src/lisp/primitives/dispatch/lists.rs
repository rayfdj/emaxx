use super::*;
use crate::lisp::primitives::processes::wait_pumping_processes;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

fn event_vector(events: impl IntoIterator<Item = Value>) -> Value {
    Value::list(std::iter::once(Value::symbol("vector-literal")).chain(events))
}

fn event_array(events: &[Value], force_vector: bool) -> Value {
    if !force_vector {
        let characters = events
            .iter()
            .map(|event| {
                let Kind::Integer(code) = event.kind() else {
                    return None;
                };
                u32::try_from(code).ok().and_then(char::from_u32)
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
    // macros.c:Fexecute_kbd_macro saves the public state in two ordinary
    // conses. Use the same allocations, including their normal GC charge,
    // while a borrowed stack root keeps the final array and loop function
    // alive across replacement, nested commands and collecting callbacks.
    let roots = [
        final_macro,
        loop_function,
        Value::cons(
            previous_macro,
            Value::cons(previous_index, previous_real_this_command),
        ),
    ];
    interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
        // Fexecute_kbd_macro starts each iteration in the selected window's
        // buffer.  This is observable when Lisp deliberately makes another
        // buffer current without changing the selected window.
        interp.set_current_buffer_id(interp.selected_window_buffer_id())?;

        let mut result = Ok(());
        loop {
            interp.set_variable("executing-kbd-macro", roots[0], env);
            interp.set_variable("executing-kbd-macro-index", Value::Integer(0), env);
            interp.set_variable("prefix-arg", Value::Nil, env);
            interp.set_variable("last-prefix-arg", Value::Nil, env);

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

            // Fexecute_kbd_macro's command loop handles only `minibuffer-quit'
            // (see execute_kbd_macro_resolved_command); register that frame so outer
            // handler-binds see the same handler landscape GNU's
            // signal_or_quit does.
            let handler_start =
                interp.push_condition_case_handler(vec![Value::Symbol("minibuffer-quit".into())]);
            let iteration = match run_kbd_macro_events(interp, env).map_err(LispError::into_kind) {
                // GNU's outermost command loop catches `top-level`, terminating
                // the keyboard macro without propagating an error.
                Err(LispErrorKind::Throw(tag, _)) if matches!(tag.kind(), Kind::Symbol(symbol) if symbol == "top-level") => {
                    Ok(())
                }
                other => other,
            };
            interp.pop_handler_bindings(handler_start);
            if let Err(error) = iteration {
                result = Err(LispError::from(error));
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
                .is_some_and(|value| value.is_string() || is_vector_value(&value));
            if !still_executing {
                break;
            }
        }

        let (previous_macro, tail) = roots[2].cons_values().expect("saved macro pair");
        let (previous_index, previous_real_this_command) =
            tail.cons_values().expect("saved macro index and command");
        interp.set_variable("executing-kbd-macro", previous_macro, env);
        interp.set_variable("executing-kbd-macro-index", previous_index, env);
        interp.set_variable("real-this-command", previous_real_this_command, env);
        interp.set_variable("this-command", Value::Nil, env);
        // This is an unwind cleanup in GNU: it runs once for normal completion,
        // loop-function termination, and command errors.
        let mut result = result.map(|()| Value::Nil);
        if let Err(hook_error) = interp.with_lisp_stack_roots(&result, |interp| {
            run_named_hooks(interp, "kbd-macro-termination-hook", env, None)
        }) {
            result = Err(hook_error);
        }
        result
    })
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

pub(crate) fn next_kbd_macro_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let array = interp
        .lookup_var("executing-kbd-macro", env)
        .unwrap_or(Value::Nil);
    // macros.c:at_end_of_macro_p treats t as an explicit stop. There is
    // no private event queue or cursor to keep consuming after this store.
    if array.is_nil() || array == Value::T {
        return Ok(None);
    }
    let index = interp
        .lookup_var("executing-kbd-macro-index", env)
        .unwrap_or(Value::Integer(0))
        .as_integer()?;
    if index >= sequence_length_value(interp, &array)? {
        return Ok(None);
    }
    // keyboard.c:read_char reads the live array with Faref. Mutation,
    // replacement and Lisp assignments to the index are visible directly.
    let frame = Value::symbol("macro");
    interp.keyboard_input.internal_last_event_frame = Some(frame);
    interp.set_variable("last-event-frame", frame, env);
    let mut event = super::call(interp, "aref", &[array, Value::Integer(index)], env)?;
    if array.is_string()
        && let Kind::Integer(code @ 0x80..=0xff) = event.kind()
    {
        event = Value::Integer(KEY_DESCRIPTION_META_BIT | (code & 0x7f));
    }
    interp.set_variable("executing-kbd-macro-index", Value::Integer(index + 1), env);
    Ok(Some(event))
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
    interp.set_variable("current-prefix-arg", pending_prefix, env);
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
    // loop just like `execute-kbd-macro'. The public array and index are
    // the same authority in both cases; t carries no macro events.
    if !executing_kbd_macro_p(interp, env) {
        return Ok(None);
    }
    let saved_buffer_id = prepare_kbd_macro_minibuffer_entry(interp, env)?;
    let result = (|| {
        let minibuffer = activate_minibuffer(interp, prompt, initial_value, *local_map, env)?;
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
    read_minibuffer_queued_commands(interp, env)
}

// minibuf.c:read_minibuf runs the ordinary recursive command loop. Both
// unread input and macro input enter the same incremental key reader;
// callbacks observe the actual queue spine and the already-advanced index.
// RET and completion are commands in the active map, never special event
// codes intercepted by this loop.
fn read_minibuffer_queued_commands(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<String>, LispError> {
    let mut reader: Option<KeySequenceReader> = None;
    while let Some(event) = next_kbd_command_event(interp, env)? {
        if reader.is_none() {
            reader = Some(KeySequenceReader::new(interp, Value::Nil, env)?);
        }
        let active = reader.as_mut().expect("minibuffer key reader");
        let command = match active.read_event(interp, event, env)? {
            KeyResolution::Prefix => continue,
            KeyResolution::Command(command) => command,
            KeyResolution::Undefined => Value::Nil,
        };
        let original = active.command_binding();
        let events = reader
            .take()
            .expect("completed minibuffer key reader")
            .finish(interp, false, env);
        // Register read_minibuf's catch before dispatch so a real
        // exit-minibuffer (including one invoked by completion) can throw.
        interp.push_catch_tag(Value::symbol("exit"));
        let dispatch = execute_kbd_macro_resolved_command(interp, original, command, &events, env);
        interp.pop_catch_tag();
        match dispatch.map_err(LispError::into_kind) {
            Ok(()) => {}
            Err(LispErrorKind::Throw(tag, _)) if tag.eq_value(Value::symbol("exit")) => {
                // The submitting command leaves before its post-command
                // phase; the prompting command resumes that outer cycle.
                return active_minibuffer_text(interp, env).map(Some);
            }
            Err(error) => return Err(LispError::from(error)),
        }
    }
    active_minibuffer_text(interp, env).map(Some)
}

fn read_minibuffer_text_from_batch_stdin(prompt: &Value) -> Result<String, LispError> {
    // minibuf.c read_minibuf_noninteractive: fputs the prompt, fflush stdout.
    crate::lisp::primitives::batch_stdout::write(string_text(prompt)?.as_bytes())
        .and_then(|()| crate::lisp::primitives::batch_stdout::flush())
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
    let minibuffer = activate_minibuffer(interp, prompt, initial_value, *local_map, env)?;
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
    if !interp
        .lookup_var("unread-command-events", env)
        .is_some_and(|events| events.is_cons())
    {
        return Ok(None);
    }
    // A recursive minibuffer command loop has its own prefix state.  Preserve
    // the caller's prefix (which may control what the prompting command asks)
    // while simulated keys start unprefixed and may build a fresh C-u prefix.
    let saved_buffer_id = interp.current_buffer_id();
    let restore = interp.bind_special_dynamic("current-prefix-arg", Value::Nil, env)?;
    let result = (|| {
        let minibuffer = activate_minibuffer(interp, prompt, initial_value, *local_map, env)?;
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
    read_minibuffer_queued_commands(interp, env)
}

fn next_kbd_command_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    if let Some(event) = take_unread_command_event(interp, env) {
        return Ok(Some(event));
    }
    next_kbd_macro_event(interp, env)
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

fn run_kbd_macro_events(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let mut reader: Option<KeySequenceReader> = None;
    loop {
        if !interp
            .lookup_var("executing-kbd-macro", env)
            .is_some_and(|value| value.is_truthy())
        {
            return Ok(());
        }
        let event = next_kbd_command_event(interp, env)?;
        let Some(event) = event else {
            increment_num_input_keys(interp, env);
            set_command_key_state(interp, Vec::new(), Vec::new(), env);
            return Ok(());
        };
        if reader.is_none() {
            reader = Some(KeySequenceReader::new(interp, Value::Nil, env)?);
        }
        let active = reader.as_mut().expect("macro key reader");
        let resolution = active.read_event(interp, event, env)?;
        let command = match resolution {
            KeyResolution::Prefix => continue,
            KeyResolution::Command(command) => command,
            KeyResolution::Undefined => Value::Nil,
        };
        let original = active.command_binding();
        let events = reader
            .take()
            .expect("completed macro key reader")
            .finish(interp, false, env);
        execute_kbd_macro_resolved_command(interp, original, command, &events, env)?;
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
    let entry_hooks = if !executing_kbd_macro_p(interp, env) {
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
    match result.map_err(LispError::into_kind) {
        Err(LispErrorKind::Throw(tag, value)) if matches!(tag.kind(), Kind::Symbol(symbol) if symbol == "exit") => {
            if value.is_truthy() {
                Err(LispError::SignalValue(Value::list([Value::Symbol(
                    "quit".into(),
                )])))
            } else {
                Ok(Value::Nil)
            }
        }
        Err(error) => Err(LispError::from(error)),
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
        match run_kbd_macro_events(interp, env).map_err(LispError::into_kind) {
            Ok(()) => return Ok(()),
            Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
                return Err(LispError::from(error));
            }
            Err(error)
                if error_matches_condition(interp, &LispError::from(error.clone()), "error") =>
            {
                report_kbd_command_error(interp, &LispError::from(error.clone()), env)?;
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
            Err(error) => return Err(LispError::from(error)),
        }
    }
}

// Both command loops supply the binding already found and remapped,
// preserving filter effects and translated/raw command-key state.
fn execute_kbd_macro_resolved_command(
    interp: &mut Interpreter,
    original_command: Value,
    command: Value,
    events: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let event = events.last().copied().unwrap_or(Value::Nil);
    // GNU's command loop separates each command into its own undo group
    // (undo-auto--boundaries); viper's undo tests observe that grouping.
    interp.buffer.borrow_mut().push_undo_boundary();
    interp.set_variable("deactivate-mark", Value::Nil, env);
    interp.set_variable("last-command-event", event, env);
    interp.set_variable("last-input-event", event, env);
    interp.set_variable("this-original-command", original_command, env);
    interp.set_variable("this-command", command, env);
    increment_num_input_keys(interp, env);
    safe_run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let dispatched_command = interp.lookup_var("this-command", env).unwrap_or(Value::Nil);
    // keyboard.c:command_loop_1 calls the unchanged Lisp `undefined' when
    // the map (or pre-command-hook) leaves this-command nil. In particular,
    // an unbound printable event is not an implicit self-insert command.
    let command_result = if dispatched_command.is_nil() {
        call_function_value(interp, &Value::symbol("undefined"), &[], env)
    } else if matches!(dispatched_command.kind(), Kind::Symbol(name) if name == "narrow-to-region")
    {
        prepare_native_kbd_command_body(interp, env)?;
        let mark = interp
            .buffer
            .borrow()
            .mark()
            .unwrap_or(interp.buffer.borrow().point());
        let point = interp.buffer.borrow().point();
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
        if matches!(
            error.kind(),
            LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
        ) || !error_matches_condition(interp, &error, "minibuffer-quit")
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

fn nth_list_element(list: &Value, count: &Value) -> Result<Value, LispError> {
    // GNU fns.c defines `nth' and list `elt' as car(nthcdr(...)).  Keep that
    // one traversal authority so negative counts, bignums, improper tails,
    // and circular lists cannot drift between the three public primitives.
    let tail = nthcdr_value(count, list)?;
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
            "list" => Ok(Value::list(args.iter().cloned())),
            "nconc" => nconc_values(args),
            "append" => {
                let mut items: Vec<Value> = Vec::new();
                for (i, a) in args.iter().enumerate() {
                    let is_last = i == args.len() - 1;
                    if is_last {
                        // `append` copies all preceding args and reuses the
                        // last one verbatim as the tail — even when it is a
                        // string or vector: (append '(2) "b") => (2 . "b").
                        let mut result = *a;
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
                    match a.kind() {
                        Kind::Lambda(lambda) => {
                            items.extend(interp.interpreted_closure_slots(&lambda));
                            continue;
                        }
                        Kind::Record(id) => {
                            if let Some(record) = interp.find_record(id)
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
            "memq" | "memql" | "member" => direct_member_family(interp, args, env, name),
            "assq" | "rassq" => direct_assq_family(interp, args, env, name),
            "rassoc" => {
                need_args(name, args, 2)?;
                let mut current = args[1];
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    match current.kind() {
                        Kind::Nil => return Ok(Value::Nil),
                        Kind::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                            if seen.step(cell_id) {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("circular-list".into()),
                                    Value::String("Circular list".into()),
                                ])));
                            }
                            let item = car.get();
                            if matches!(item.kind(), Kind::Cons(_))
                                && values_equal_in_env(interp, &item.cdr()?, &args[0], env)
                            {
                                return Ok(item);
                            }
                            current = cdr.get();
                        }
                        other => {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("wrong-type-argument".into()),
                                Value::Symbol("listp".into()),
                                other.value(),
                            ])));
                        }
                    }
                }
            }
            "assoc" => {
                need_arg_range(name, args, 2, 3)?;
                let mut current = args[1];
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    match current.kind() {
                        Kind::Nil => return Ok(Value::Nil),
                        Kind::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                            if seen.step(cell_id) {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("circular-list".into()),
                                    Value::String("Circular list".into()),
                                ])));
                            }
                            let item = car.get();
                            if matches!(item.kind(), Kind::Cons(_))
                                && if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                                {
                                    call_function_value(
                                        interp,
                                        testfn,
                                        &[args[0], item.car()?],
                                        env,
                                    )?
                                    .is_truthy()
                                } else {
                                    values_equal_in_env(interp, &item.car()?, &args[0], env)
                                }
                            {
                                return Ok(item);
                            }
                            current = cdr.get();
                        }
                        other => {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("wrong-type-argument".into()),
                                Value::Symbol("listp".into()),
                                other.value(),
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
                    let thiscar = match item.kind() {
                        Kind::Cons(_) => item.car()?,
                        _ => *item,
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
                        return Ok(*item);
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
                // fns.c's mapcar1 gathers the results in a SAFE_ALLOCA_LISP
                // array the collector reaches while the calls go on.
                let mut results = crate::lisp::alloc::RootedVec::with_capacity(list.len());
                for item in list {
                    results.push(call_function_value(interp, &args[0], &[item], env)?);
                }
                Ok(Value::list(results))
            }
            "mapcan" => {
                need_args(name, args, 2)?;
                let list = sequence_values(interp, &args[1])?;
                let mut mapped = crate::lisp::alloc::RootedVec::with_capacity(list.len());
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
                Ok(args[1])
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
                            *item,
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
                let mut all_args: Vec<Value> = args[1..args.len() - 1].to_vec();
                // Fapply calls list_length and then walks XCAR/XCDR.  Its
                // spread argument is a proper list, not an arbitrary Emacs
                // sequence.
                if is_vector_value(last) {
                    return Err(LispError::WrongTypeArgument("listp".into(), *last));
                }
                all_args.extend(last.to_vec()?);
                interp.call_function_value(*func, None, &all_args, env)
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
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                interp.fset_function(&symbol, args[1], env)
            }
            "fmakunbound" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Ffmakunbound uses CHECK_SYMBOL/XSYMBOL and
                // returns its original symbol argument.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                // GNU voids the function cell outright; shadowed stale entries
                // (repeated defuns push duplicates) must not resurface.
                interp.remove_all_function_bindings(&symbol);
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
                {
                    let state = &mut **interp;
                    state
                        .kbd_macro_definition
                        .truncate(state.kbd_macro_committed_len);
                }
                let last_macro = Value::list(
                    std::iter::once(Value::symbol("vector-literal"))
                        .chain(interp.kbd_macro_definition.iter().cloned()),
                );
                interp.set_variable("last-kbd-macro", last_macro, env);
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
                    .lookup_var("last-kbd-macro", env)
                    .unwrap_or(Value::Nil);
                interp.set_variable(
                    "this-command",
                    interp.lookup_var("last-command", env).unwrap_or(Value::Nil),
                    env,
                );
                interp.set_variable("real-this-command", macro_value, env);
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
                    .lookup_var("defining-kbd-macro", env)
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
                let restore = interp.bind_special_variable("track-mouse", Value::T, env)?;
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
                if let Some(stuff_string) = args.first()
                    && !stuff_string.is_nil()
                    && string_like(stuff_string).is_none()
                {
                    return Err(LispError::WrongTypeArgument(
                        "stringp".into(),
                        *stuff_string,
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
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?;
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
                if !args[0].is_nil() && !args[0].is_string() {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                }
                ensure_interaction_allowed(interp, env)?;
                let events = read_key_sequence_events(
                    interp,
                    args[0],
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )?;
                Ok(event_array(&events, name == "read-key-sequence-vector"))
            }
            "read-event" | "read-char" | "read-char-exclusive" => {
                ensure_interaction_allowed(interp, env)?;
                let read_event = name == "read-event";
                let timed_poll = args.len() >= 3 && args[2].is_truthy();
                if timed_poll {
                    let timeout = wait_duration(std::slice::from_ref(&args[2]))?;
                    // keyboard.c:read_char consumes unread and live macro
                    // events before entering the timed keyboard wait.
                    if let Some(event) = pop_pending_input_event_value(interp, env)? {
                        return if read_event {
                            normalize_input_event_value(event)
                        } else {
                            Ok(Value::Integer(unread_command_event_char(&event)? as i64))
                        };
                    }
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
                    return match pop_pending_input_event_value(interp, env)? {
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
                let prompt = args[0];
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
                        history,
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
                                None => default,
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
                            && let Some(default) = args.get(3).filter(|value| !value.is_nil())
                        {
                            let default = match default.cons_values() {
                                Some((head, _)) => head,
                                None => *default,
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
                let default = match args.get(1).cloned().unwrap_or(Value::Nil).kind() {
                    Kind::Buffer(buffer) => Value::string(&buffer.borrow().name),
                    other => other.value(),
                };
                if let Some(function) = interp
                    .lookup_var("read-buffer-function", env)
                    .filter(|function| !function.is_nil())
                {
                    let mut call_args =
                        vec![args[0], default, args.get(2).cloned().unwrap_or(Value::Nil)];
                    if let Some(predicate) = args.get(3) {
                        call_args.push(*predicate);
                    }
                    return call_function_value(interp, &function, &call_args, env);
                }
                let prompt = if default.is_nil() {
                    args[0]
                } else {
                    let raw = string_text(&args[0])?;
                    let stem = raw
                        .strip_suffix(": ")
                        .or_else(|| raw.strip_suffix(':'))
                        .or_else(|| raw.strip_suffix(' '))
                        .unwrap_or(&raw);
                    let shown_default = default.car().unwrap_or(default);
                    call_function_value(
                        interp,
                        &Value::Symbol("format-prompt".into()),
                        &[Value::String(stem.into()), shown_default],
                        env,
                    )
                    .or_else(|error| {
                        if matches!(error.kind(), LispErrorKind::VoidFunction(_)) {
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
                    .filter_map(|buffer| match buffer.kind() {
                        Kind::Buffer(handle) => Some(Value::cons(
                            Value::string(&handle.borrow().name),
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
                let default = match default.kind() {
                    Kind::Symbol(symbol) => Value::String(
                        crate::lisp::types::visible_symbol_name(&symbol)
                            .to_string()
                            .into(),
                    ),
                    other => other.value(),
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
                    let symbol = string_text(&value)?;
                    intern_in_obarray(interp, &obarray, &symbol)
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
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "nth";
    need_args(name, args, 2)?;
    nth_list_element(&args[1], &args[0])
}

/// The `nthcdr' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_nthcdr(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "nthcdr";
    need_args(name, args, 2)?;
    nthcdr_value(&args[0], &args[1])
}

/// The `elt' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_elt(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "elt";
    need_args(name, args, 2)?;
    if matches!(args[0].kind(), Kind::Cons(_))
        && matches!(
            args[0].to_vec().ok().and_then(|items| items.first().cloned()).map(|v| v.kind()),
            Some(Kind::Symbol(symbol)) if symbol == "vector-literal"
        )
    {
        super::call(interp, "aref", args, env)
    } else if matches!(args[0].kind(), Kind::Nil | Kind::Cons(_)) {
        nth_list_element(&args[0], &args[1])
    } else {
        super::call(interp, "aref", args, env)
    }
}

/// The `length' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_length(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "length";
    need_args(name, args, 1)?;
    Ok(Value::Integer(sequence_length_value(interp, &args[0])?))
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
        Eql,
        Eq,
    }
    let test = match name {
        "member" => MemTest::Equal,
        "memql" => MemTest::Eql,
        _ => MemTest::Eq,
    };
    let mut tail = args[1];
    let mut tortoise = tail;
    let mut maximum = 2_isize;
    let mut remaining = 0_isize;
    let mut quit_count = 2_u16;
    while let Kind::Cons(cell) = tail.kind() {
        let item = cell.car.get();
        let matches = match test {
            MemTest::Equal => values_equal_in_env(interp, &item, &args[0], env),
            MemTest::Eql => values_eql(&item, &args[0]),
            MemTest::Eq => values_eq_in_env(interp, &item, &args[0], env),
        };
        if matches {
            return Ok(tail);
        }
        // lisp.h:FOR_EACH_TAIL advances after the comparison, then checks
        // quit and Brent's tortoise at the same points as the native walk.
        tail = cell.cdr.get();
        quit_count = quit_count.wrapping_sub(1);
        let compare = if quit_count != 0 {
            true
        } else {
            interp.maybe_quit(env)?;
            remaining = remaining.wrapping_sub(1);
            remaining > 0
        };
        if compare {
            if tail.word() == tortoise.word() {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("circular-list"),
                    tail,
                ])));
            }
        } else {
            maximum = maximum.wrapping_shl(1);
            quit_count = maximum as u16;
            remaining = maximum >> u16::BITS;
            tortoise = tail;
        }
    }
    // CHECK_LIST_END(tail, list) reports the original list. An improper
    // tail is never an element, even when it equals the sought value.
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", args[1]));
    }
    Ok(Value::Nil)
}

pub(super) fn direct_memq(
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

/// `assq' and `rassq' by NAME.
pub(super) fn direct_assq_family(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
    name: &str,
) -> Result<Value, LispError> {
    need_args(name, args, 2)?;
    let want_car = name == "assq";
    let key = &args[0];
    let alist = &args[1];
    let mut seen = crate::lisp::types::CycleGuard::new();
    // Walk by cons cells rather than by cloned Values: one Rc
    // bump per step and no whole-Value churn.
    let mut cell = match alist.kind() {
        Kind::Nil => return Ok(Value::Nil),
        Kind::Cons(cell) => cell,
        other => {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("wrong-type-argument".into()),
                Value::Symbol("listp".into()),
                other.value(),
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
            let item = cell.car.get();
            match (item).kind() {
                Kind::Cons(cons_cell) => {
                    let item_car = &cons_cell.car;
                    let item_cdr = &cons_cell.cdr;
                    let slot = if want_car { item_car } else { item_cdr };
                    let entry_key = slot.get();
                    match ((entry_key).kind(), key.kind()) {
                        (Kind::Integer(a), Kind::Integer(b)) => a == b,
                        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
                        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
                        (Kind::Nil | Kind::T, _)
                        | (_, Kind::Nil | Kind::T)
                        | (Kind::Integer(_), Kind::Symbol(_))
                        | (Kind::Symbol(_), Kind::Integer(_)) => false,
                        // GNU 30.2 fns.c implements assq/rassq
                        // with EQ, whose lisp.h contract unwraps
                        // symbol-with-position objects while the
                        // dynamic mode is enabled.  Keep ordinary
                        // scalar keys on the fast path above.
                        (a, b) => values_eq_in_env(interp, &a.value(), &b.value(), env),
                    }
                }
                _ => false,
            }
        };
        if matched {
            return Ok(cell.car.get());
        }
        let tail = cell.cdr.get();
        let next = match (tail).kind() {
            Kind::Nil => return Ok(Value::Nil),
            Kind::Cons(next) => next,
            other => {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("wrong-type-argument".into()),
                    Value::Symbol("listp".into()),
                    other.value(),
                ])));
            }
        };
        cell = next;
    }
}

pub(super) fn direct_assq(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    direct_assq_family(interp, args, env, "assq")
}

pub(super) fn direct_rassq(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    direct_assq_family(interp, args, env, "rassq")
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
