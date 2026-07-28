use super::*;
use crate::lisp::primitives::processes::wait_pumping_processes;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "cons"
            | "car"
            | "cl-first"
            | "cl-second"
            | "cl-third"
            | "cl-fourth"
            | "cl-fifth"
            | "cl-sixth"
            | "cl-seventh"
            | "cl-eighth"
            | "cl-ninth"
            | "cl-tenth"
            | "cdr"
            | "car-safe"
            | "cdr-safe"
            | "identity"
            | "list"
            | "cl-values"
            | "nconc"
            | "append"
            | "nth"
            | "cl-nth-value"
            | "elt"
            | "nthcdr"
            | "last"
            | "butlast"
            | "nbutlast"
            | "length"
            | "safe-length"
            | "length<"
            | "length>"
            | "length="
            | "reverse"
            | "copy-tree"
            | "flatten-tree"
            | "flatten-list"
            | "copy-alist"
            | "delete-dups"
            | "remove"
            | "memq"
            | "memql"
            | "member"
            | "cl-member"
            | "member-ignore-case"
            | "assq"
            | "rassq"
            | "rassoc"
            | "rassq-delete-all"
            | "assq-delete-all"
            | "assoc-delete-all"
            | "assoc"
            | "assoc-string"
            | "alist-get"
            | "cl-set-exclusive-or"
            | "cl-remove-if-not"
            | "cl-delete-if"
            | "cl-position"
            | "cl-remove"
            | "cl-substitute"
            | "cl-replace"
            | "cl-fill"
            | "mapcar"
            | "mapcan"
            | "cl-mapcar"
            | "cl-mapcan"
            | "cl-some"
            | "seq-mapcat"
            | "mapc"
            | "cl-reduce"
            | "eval"
            | "eval-buffer"
            | "eval-region"
            | "unload-feature"
            | "mapconcat"
            | "string-join"
            | "ensure-list"
            | "position-symbol"
            | "symbol-with-pos-pos"
            | "remove-pos-from-symbol"
            | "bare-symbol"
            | "seq-find"
            | "seq-contains-p"
            | "seq-take"
            | "seq-position"
            | "cl-coerce"
            | "treesit-language-available-p"
            | "treesit--linecol-cache"
            | "treesit--linecol-cache-set"
            | "treesit--linecol-at"
            | "apply"
            | "apply-partially"
            | "funcall"
            | "fset"
            | "fmakunbound"
            | "funcall-interactively"
            | "call-interactively"
            | "keyboard-quit"
            | "start-kbd-macro"
            | "end-kbd-macro"
            | "call-last-kbd-macro"
            | "execute-kbd-macro"
            | "cancel-kbd-macro-events"
            | "store-kbd-macro-event"
            | "clear-this-command-keys"
            | "event-convert-list"
            | "internal--track-mouse"
            | "internal-event-symbol-parse-modifiers"
            | "internal-handle-focus-in"
            | "open-dribble-file"
            | "read-key-sequence-vector"
            | "set--this-command-keys"
            | "suspend-emacs"
            | "recursive-edit"
            | "exit-recursive-edit"
            | "abort-recursive-edit"
            | "recursion-depth"
            | "top-level"
            | "barf-if-buffer-read-only"
            | "this-command-keys"
            | "this-command-keys-vector"
            | "this-single-command-keys"
            | "this-single-command-raw-keys"
            | "recent-keys"
            | "define-keymap"
            | "define-abbrev-table"
            | "read-key"
            | "read-key-sequence"
            | "read-event"
            | "read-char"
            | "read-char-exclusive"
            | "mouse-double-click-time"
            | "context-menu-map"
            | "read-string"
            | "read-file-name"
            | "read-from-minibuffer"
            | "read-no-blanks-input"
            | "completing-read"
            | "read-buffer"
            | "read-command"
            | "read-variable"
            | "format-prompt"
    )
}

fn apply_cl_key(
    interp: &mut Interpreter,
    keyfn: Option<&Value>,
    item: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match keyfn.filter(|value| !value.is_nil()) {
        Some(keyfn) => {
            let function = resolve_callable(interp, keyfn, env)?;
            invoke_function_value(interp, &function, std::slice::from_ref(item), env)
        }
        None => Ok(item.clone()),
    }
}

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
            return Value::String(characters);
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
        interp.set_variable("current-prefix-arg", Value::Nil, env);

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

// Minibuffer reads issued while a keyboard macro executes consume the
// macro's remaining events as minibuffer input, up to the RET (or C-j) that
// runs `exit-minibuffer' in the real command loop.  INITIAL seeds the
// contents with point at the end, and the basic editing keys the Edebug
// tests use to replace a suggested default are honored.
fn read_minibuffer_text_from_kbd_macro(
    interp: &mut Interpreter,
    env: &mut Env,
    initial: &str,
) -> Option<String> {
    interp.kbd_macro_executions.last()?;
    let mut text: Vec<char> = initial.chars().collect();
    let mut cursor = text.len();
    while let Some(event) = current_kbd_macro_event(interp, 0) {
        let Ok(code) = event.as_integer() else {
            break;
        };
        advance_kbd_macro_index(interp, 1, env);
        match code {
            13 | 10 => break,            // RET / C-j: exit-minibuffer
            1 => cursor = 0,             // C-a: move-beginning-of-line
            5 => cursor = text.len(),    // C-e: move-end-of-line
            11 => text.truncate(cursor), // C-k: kill-line
            127 => {
                // DEL: delete-backward-char
                if cursor > 0 {
                    cursor -= 1;
                    text.remove(cursor);
                }
            }
            _ => {
                if let Some(ch) = u32::try_from(code).ok().and_then(char::from_u32)
                    && (!ch.is_control() || ch == '\t')
                {
                    text.insert(cursor, ch);
                    cursor += 1;
                }
            }
        }
    }
    Some(text.into_iter().collect())
}

// `ert-simulate-keys' drives a real GNU minibuffer through
// `unread-command-events', not through `execute-kbd-macro'.  Resolve command
// prefixes before treating events as text so global commands such as
// C-x RET c can open a nested prompt and then return to the outer minibuffer.
fn read_minibuffer_text_from_unread_events(
    interp: &mut Interpreter,
    env: &mut Env,
    initial: &str,
) -> Result<Option<String>, LispError> {
    let unread = crate::lisp::primitives::unread_command_events(interp, env)?;
    if unread.is_empty() {
        return Ok(None);
    }
    let mut events = VecDeque::from(unread);
    let mut contents = initial.to_string();
    let mut pending_keys = Vec::new();

    while let Some(mut event) = events.pop_front() {
        interp.set_variable(
            "unread-command-events",
            Value::list(events.iter().cloned()),
            env,
        );
        let code = event.as_integer().ok();
        if pending_keys.is_empty() && matches!(code, Some(10 | 13)) {
            break;
        }

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
        pending_keys.push(event_key);
        let binding_key = pending_keys.join(" ");

        if binding_key == "C-u" {
            let current = interp
                .lookup_var("current-prefix-arg", env)
                .unwrap_or(Value::Nil);
            let next = match prefix_numeric_value(&current).and_then(|value| value.as_integer()) {
                Ok(value) if !current.is_nil() => value.saturating_mul(4),
                _ => 4,
            };
            interp.set_variable(
                "current-prefix-arg",
                Value::list([Value::Integer(next)]),
                env,
            );
            pending_keys.clear();
            continue;
        }
        if pending_keys.len() == 1
            && let Some(code) = code
            && (0x30..=0x39).contains(&code)
        {
            let current = interp
                .lookup_var("current-prefix-arg", env)
                .unwrap_or(Value::Nil);
            if !current.is_nil() {
                let digit = code - 0x30;
                let next = match &current {
                    Value::Integer(accumulated) if *accumulated < 0 => {
                        accumulated.saturating_mul(10).saturating_sub(digit)
                    }
                    Value::Integer(accumulated) => {
                        accumulated.saturating_mul(10).saturating_add(digit)
                    }
                    Value::Symbol(minus) if minus == "-" => -digit,
                    _ => digit,
                };
                interp.set_variable("current-prefix-arg", Value::Integer(next), env);
                pending_keys.clear();
                continue;
            }
        }

        let binding = key_binding(interp, &binding_key, false, false, env)?;
        if is_keymap_value(interp, &binding)
            || (binding.is_nil() && key_sequence_is_prefix(interp, &binding_key, env)?)
        {
            continue;
        }

        if pending_keys.len() == 1
            && let Some(text) = keyboard_macro_self_insert_text(&event)
            && (binding.is_nil()
                || matches!(&binding, Value::Symbol(command) if command == "self-insert-command"))
        {
            let repeat = interp
                .lookup_var("current-prefix-arg", env)
                .and_then(|prefix| prefix_numeric_value(&prefix).ok())
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(1)
                .max(0) as usize;
            let command = Value::Symbol("self-insert-command".into());
            set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
            interp.set_variable("last-command-event", event.clone(), env);
            interp.set_variable("this-original-command", command.clone(), env);
            interp.set_variable("this-command", command.clone(), env);
            safe_run_named_hooks(
                interp,
                "pre-command-hook",
                env,
                Some(interp.current_buffer_id()),
            )?;
            contents.push_str(&text.repeat(repeat));
            safe_run_named_hooks(
                interp,
                "post-command-hook",
                env,
                Some(interp.current_buffer_id()),
            )?;
            let final_this_command = interp
                .lookup_var("this-command", env)
                .filter(|value| !value.is_nil())
                .unwrap_or_else(|| command.clone());
            finish_kbd_macro_command(interp, command, final_this_command, env);
            interp.set_variable("current-prefix-arg", Value::Nil, env);
            pending_keys.clear();
            continue;
        }
        if !binding.is_nil() {
            execute_kbd_macro_command(interp, &binding, &event, env)?;
            events = VecDeque::from(crate::lisp::primitives::unread_command_events(interp, env)?);
            interp.set_variable("current-prefix-arg", Value::Nil, env);
            pending_keys.clear();
            continue;
        }

        pending_keys.clear();
    }

    interp.set_variable("unread-command-events", Value::list(events), env);
    Ok(Some(contents))
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
fn run_kbd_macro_events(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let mut pending_keys: Vec<String> = Vec::new();
    loop {
        let macro_active = interp
            .lookup_var("executing-kbd-macro", env)
            .is_some_and(|value| value.is_truthy());
        if !macro_active {
            return Ok(());
        }
        let Some(mut event) = current_kbd_macro_event(interp, 0) else {
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
        if pending_keys.is_empty()
            && let Value::Symbol(name) = &event
            && let Some(code) = function_key_default_translation(name)
            && key_binding(interp, &event_key, false, false, env)?.is_nil()
            && !key_sequence_is_prefix(interp, &event_key, env)?
        {
            event = Value::Integer(code);
            let translated = Value::list([Value::Symbol("vector-literal".into()), event.clone()]);
            event_key = key_sequence_binding_text(&translated)?;
        }
        if pending_keys.is_empty() && event_key == "C-s" {
            advance_kbd_macro_index(interp, 1, env);
            let mut search_text = String::new();
            while let Some(next_event) = current_kbd_macro_event(interp, 0) {
                if let Some(text) = keyboard_macro_self_insert_text(&next_event) {
                    search_text.push_str(&text);
                    advance_kbd_macro_index(interp, 1, env);
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
                    &[Value::String(search_text), Value::Nil, Value::T],
                    env,
                );
                interp.restore_special_binding(restore, env)?;
                search_result?;
            }
            finish_kbd_macro_command(
                interp,
                Value::Symbol("isearch-forward".into()),
                Value::Symbol("isearch-forward".into()),
                env,
            );
            continue;
        }
        pending_keys.push(event_key);
        let binding_key = pending_keys.join(" ");
        if binding_key == "C-u" {
            let current = interp
                .lookup_var("current-prefix-arg", env)
                .unwrap_or(Value::Nil);
            let next = match prefix_numeric_value(&current).and_then(|value| value.as_integer()) {
                Ok(value) if !current.is_nil() => value.saturating_mul(4),
                _ => 4,
            };
            interp.set_variable(
                "current-prefix-arg",
                Value::list([Value::Integer(next)]),
                env,
            );
            pending_keys.clear();
            advance_kbd_macro_index(interp, 1, env);
            continue;
        }
        // While a prefix argument is being entered, digits accumulate into it
        // (`digit-argument') instead of dispatching as ordinary keys.
        if pending_keys.len() == 1
            && let Ok(code) = event.as_integer()
            && (0x30..=0x39).contains(&code)
        {
            let current = interp
                .lookup_var("current-prefix-arg", env)
                .unwrap_or(Value::Nil);
            if !current.is_nil() {
                let digit = code - 0x30;
                let next = match &current {
                    Value::Integer(accumulated) if *accumulated < 0 => {
                        accumulated.saturating_mul(10).saturating_sub(digit)
                    }
                    Value::Integer(accumulated) => {
                        accumulated.saturating_mul(10).saturating_add(digit)
                    }
                    Value::Symbol(minus) if minus == "-" => -digit,
                    _ => digit,
                };
                interp.set_variable("current-prefix-arg", Value::Integer(next), env);
                pending_keys.clear();
                advance_kbd_macro_index(interp, 1, env);
                continue;
            }
        }
        let binding = key_binding(interp, &binding_key, false, false, env)?;
        if is_keymap_value(interp, &binding) {
            advance_kbd_macro_index(interp, 1, env);
            continue;
        }
        if binding.is_nil() && key_sequence_is_prefix(interp, &binding_key, env)? {
            advance_kbd_macro_index(interp, 1, env);
            continue;
        }
        if !binding.is_nil() {
            advance_kbd_macro_index(interp, 1, env);
            execute_kbd_macro_command(interp, &binding, &event, env)?;
            interp.set_variable("current-prefix-arg", Value::Nil, env);
            pending_keys.clear();
            continue;
        }
        if pending_keys.len() == 1
            && let Some(text) = keyboard_macro_self_insert_text(&event)
        {
            set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
            advance_kbd_macro_index(interp, 1, env);
            execute_kbd_macro_self_insert(interp, &text, env)?;
            pending_keys.clear();
            continue;
        }
        pending_keys.clear();
        advance_kbd_macro_index(interp, 1, env);
        // The command loop reports an unbound complete sequence and stops
        // the executing macro.  ERC's keymap tests observe this through
        // ert-with-message-capture after removing module bindings.
        super::call(
            interp,
            "message",
            &[Value::String(format!("{binding_key} is undefined"))],
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
        .and_then(|()| run_kbd_macro_events(interp, env))
        // With no more events to dispatch the command loop goes idle, which
        // processes queued file notifications and fires due timers.
        .and_then(|()| interp.run_pending_file_notifications(env))
        .and_then(|()| interp.run_pending_timers(env));
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

fn execute_kbd_macro_command(
    interp: &mut Interpreter,
    command: &Value,
    event: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    // The command loop resolves [remap COMMAND] bindings from the active
    // keymaps before dispatching (erc-fill-wrap remaps erc-bol);
    // `this-original-command' keeps the pre-remap binding.
    let original_command = command.clone();
    let remapped = crate::lisp::primitives::command_remapping(interp, command, None, env)?;
    let command = &if remapped.is_nil() {
        original_command.clone()
    } else {
        remapped
    };
    // GNU's command loop separates each command into its own undo group
    // (undo-auto--boundaries); viper's undo tests observe that grouping.
    interp.buffer.push_undo_boundary();
    set_command_key_state(interp, vec![event.clone()], vec![event.clone()], env);
    interp.set_variable("deactivate-mark", Value::Nil, env);
    interp.set_variable("last-command-event", event.clone(), env);
    interp.set_variable("this-original-command", original_command, env);
    interp.set_variable("this-command", command.clone(), env);
    safe_run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let command_result = if matches!(command, Value::Symbol(name) if name == "narrow-to-region") {
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
        call_interactively_impl(interp, std::slice::from_ref(command), env)
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
        let error_function = interp
            .lookup_var("command-error-function", env)
            .unwrap_or(Value::Nil);
        let data = crate::lisp::eval::error_condition_value(&error);
        interp.set_global_binding("executing-kbd-macro", Value::Nil);
        if !error_function.is_nil() {
            interp.call_function_value(
                error_function,
                None,
                &[data, Value::String(String::new()), Value::Nil],
                env,
            )?;
        }
    }
    safe_run_named_hooks(
        interp,
        "post-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    // GNU copies `this-command' into `last-command' at the end of the
    // cycle, so a command that rewrites this-command (viper-undo-more sets
    // it to viper-undo) steers the next dispatch.
    let final_this_command = interp
        .lookup_var("this-command", env)
        .filter(|value| !value.is_nil())
        .unwrap_or_else(|| command.clone());
    finish_kbd_macro_command(interp, command.clone(), final_this_command, env);
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
    interp.set_variable("deactivate-mark", Value::Nil, env);
    interp.set_variable("this-original-command", command.clone(), env);
    interp.set_variable("this-command", command.clone(), env);
    safe_run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    insert_text_with_hooks(interp, text, &[], false, false, env)?;
    safe_run_named_hooks(
        interp,
        "post-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    finish_kbd_macro_command(interp, command.clone(), command, env);
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
}

/// GNU's `local-function-key-map' default translations from function-key
/// symbols to ASCII control characters.
fn function_key_default_translation(name: &str) -> Option<i64> {
    Some(match name {
        "escape" => 27,
        "tab" => 9,
        "return" => 13,
        "linefeed" => 10,
        "delete" | "backspace" => 127,
        _ => return None,
    })
}

fn keyboard_macro_self_insert_text(event: &Value) -> Option<String> {
    if let Value::Symbol(text) = event
        && !looks_like_textual_key_spec(text)
    {
        return Some(text.clone());
    }
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

fn nth_list_element(list: &Value, n: usize) -> Result<Value, LispError> {
    let mut tail = list.clone();
    for _ in 0..n {
        match tail {
            Value::Nil => return Ok(Value::Nil),
            Value::Cons(_, ref cdr) => {
                let next = cdr.borrow().clone();
                tail = next;
            }
            other => return Err(wrong_type_argument("listp", other)),
        }
    }
    match tail {
        Value::Nil => Ok(Value::Nil),
        Value::Cons(ref car, _) => Ok(car.borrow().clone()),
        other => Err(wrong_type_argument("listp", other)),
    }
}

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
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                Ok(items.into_iter().next().unwrap_or(Value::Nil))
            } else {
                args[0]
                    .car()
                    .map_err(|_| wrong_type_argument("listp", args[0].clone()))
            }
        }
        "cl-first" | "cl-second" | "cl-third" | "cl-fourth" | "cl-fifth" | "cl-sixth"
        | "cl-seventh" | "cl-eighth" | "cl-ninth" | "cl-tenth" => {
            need_args(name, args, 1)?;
            let index = match name {
                "cl-first" => 0,
                "cl-second" => 1,
                "cl-third" => 2,
                "cl-fourth" => 3,
                "cl-fifth" => 4,
                "cl-sixth" => 5,
                "cl-seventh" => 6,
                "cl-eighth" => 7,
                "cl-ninth" => 8,
                _ => 9,
            };
            let mut tail = args[0].clone();
            for _ in 0..index {
                tail = tail.cdr()?;
            }
            tail.car()
        }
        "cdr" => {
            need_args(name, args, 1)?;
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                Ok(Value::list(items.into_iter().skip(1)))
            } else {
                args[0]
                    .cdr()
                    .map_err(|_| wrong_type_argument("listp", args[0].clone()))
            }
        }
        "car-safe" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(car, _) => car.borrow().clone(),
                value => keymap_list_items(interp, value)?
                    .and_then(|items| items.into_iter().next())
                    .unwrap_or(Value::Nil),
            })
        }
        "cdr-safe" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(_, cdr) => cdr.borrow().clone(),
                value => keymap_list_items(interp, value)?
                    .map(|items| Value::list(items.into_iter().skip(1)))
                    .unwrap_or(Value::Nil),
            })
        }
        "identity" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "list" | "cl-values" => Ok(Value::list(args.iter().cloned())),
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
        "nth" | "cl-nth-value" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()? as usize;
            if let Some(items) = keymap_list_items(interp, &args[1])? {
                Ok(items.get(n).cloned().unwrap_or(Value::Nil))
            } else {
                nth_list_element(&args[1], n)
            }
        }
        "elt" => {
            need_args(name, args, 2)?;
            if matches!(args[0], Value::Cons(_, _))
                && matches!(
                    args[0].to_vec().ok().and_then(|items| items.first().cloned()),
                    Some(Value::Symbol(symbol)) if symbol == "vector-literal"
                )
            {
                super::call(interp, "aref", args, env)
            } else if matches!(args[0], Value::Nil | Value::Cons(_, _)) {
                let n = args[1].as_integer()? as usize;
                nth_list_element(&args[0], n)
            } else {
                super::call(interp, "aref", args, env)
            }
        }
        "nthcdr" => {
            need_args(name, args, 2)?;
            if let Some(items) = keymap_list_items(interp, &args[1])? {
                return nthcdr_value(&args[0], &Value::list(items));
            }
            nthcdr_value(&args[0], &args[1])
        }
        "last" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                let projected = Value::list(items);
                return super::call(
                    interp,
                    "last",
                    &[projected, args.get(1).cloned().unwrap_or(Value::Integer(1))],
                    env,
                );
            }
            let n = args
                .get(1)
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            if n < 0 {
                return Ok(Value::Nil);
            }
            let n = n as usize;
            let mut tails = Vec::new();
            let mut current = args[0].clone();
            loop {
                match current.clone() {
                    Value::Cons(_, cdr) => {
                        tails.push(current.clone());
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => {
                        return if n == 0 {
                            Ok(Value::Nil)
                        } else if let Some(index) = tails.len().checked_sub(n.max(1)) {
                            Ok(tails[index].clone())
                        } else {
                            Ok(args[0].clone())
                        };
                    }
                    other => {
                        return if n == 0 {
                            Ok(other)
                        } else if let Some(index) = tails.len().checked_sub(n.max(1)) {
                            Ok(tails[index].clone())
                        } else {
                            Ok(args[0].clone())
                        };
                    }
                }
            }
        }
        "butlast" | "nbutlast" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let n = args
                .get(1)
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            if n <= 0 {
                return Ok(args[0].clone());
            }
            let items = list_sequence_items(interp, &args[0])?;
            let keep = items.len().saturating_sub(n as usize);
            if name == "butlast" {
                return Ok(Value::list(items.into_iter().take(keep)));
            }
            if keep == 0 {
                return Ok(Value::Nil);
            }
            let mut tail = args[0].clone();
            for _ in 1..keep {
                tail = tail.cdr()?;
            }
            tail.set_cdr(Value::Nil)?;
            Ok(args[0].clone())
        }
        "length" => {
            need_args(name, args, 1)?;
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                return Ok(Value::Integer(items.len() as i64));
            }
            if let Some(items) = record_literal_items(&args[0]) {
                return Ok(Value::Integer((items.len().saturating_sub(1)) as i64));
            }
            match &args[0] {
                value if string_like(value).is_some() => {
                    Ok(Value::Integer(string_text(value)?.chars().count() as i64))
                }
                Value::Nil => Ok(Value::Integer(0)),
                Value::Cons(_, _) if is_vector_value(&args[0]) => {
                    Ok(Value::Integer(vector_items(&args[0])?.len() as i64))
                }
                Value::CharTable(_) => Ok(Value::Integer(0x40_0000)),
                value if is_bool_vector_value(interp, value) => Ok(Value::Integer(
                    bool_vector_values(interp, value)?.len() as i64,
                )),
                Value::Cons(_, _) => Ok(Value::Integer(args[0].to_vec()?.len() as i64)),
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    Ok(Value::Integer((record.slots.len() + 1) as i64))
                }
                _ => Err(LispError::TypeError("sequence".into(), args[0].type_name())),
            }
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
        "copy-tree" => {
            need_arg_range(name, args, 1, 2)?;
            let vectors_and_records = args.get(1).is_some_and(Value::is_truthy);
            copy_tree_value(interp, &args[0], vectors_and_records)
        }
        "flatten-tree" | "flatten-list" => {
            need_args(name, args, 1)?;
            let mut leaves = Vec::new();
            flatten_tree_value(&args[0], &mut leaves);
            Ok(Value::list(leaves))
        }
        "copy-alist" => {
            need_args(name, args, 1)?;
            copy_alist_value(&args[0])
        }
        "delete-dups" => {
            need_args(name, args, 1)?;
            let mut deduped = Vec::new();
            for item in args[0].to_vec()? {
                if !deduped
                    .iter()
                    .any(|existing| values_equal(interp, existing, &item))
                {
                    deduped.push(item);
                }
            }
            Ok(Value::list(deduped))
        }
        "remove" => {
            need_args(name, args, 2)?;
            remove_equal(interp, &args[0], &args[1])
        }
        "memq" | "memql" | "member" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
            let mut seen = HashSet::new();
            loop {
                match current.clone() {
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let item = car.borrow().clone();
                        let matches = match name {
                            "member" => values_equal(interp, &item, &args[0]),
                            "memql" => values_eql(&item, &args[0]),
                            _ => values_eq_in_env(interp, &item, &args[0], env),
                        };
                        if matches {
                            return Ok(current);
                        }
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => return Ok(Value::Nil),
                    other => {
                        let matches = match name {
                            "member" => values_equal(interp, &other, &args[0]),
                            "memql" => values_eql(&other, &args[0]),
                            _ => values_eq_in_env(interp, &other, &args[0], env),
                        };
                        if matches {
                            return Ok(other);
                        }
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("listp".into()),
                            other,
                        ])));
                    }
                }
            }
        }
        "cl-member" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let mut testfn = None;
            let mut test_not = None;
            let mut keyfn = None;
            let mut index = 2usize;
            while index < args.len() {
                let keyword = args[index].as_symbol()?;
                let Some(value) = args.get(index + 1) else {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                };
                match keyword {
                    ":test" => testfn = Some(value.clone()),
                    ":test-not" => test_not = Some(value.clone()),
                    ":key" => keyfn = Some(value.clone()),
                    _ => return Err(LispError::Signal("Unsupported cl-member keyword".into())),
                }
                index += 2;
            }
            let needle = args[0].clone();
            let mut current = args[1].clone();
            let mut seen = HashSet::new();
            loop {
                match current.clone() {
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let item = car.borrow().clone();
                        let keyed_item = apply_cl_key(interp, keyfn.as_ref(), &item, env)?;
                        let matches = if let Some(predicate) = test_not.as_ref() {
                            !value_matches_with_test(
                                interp,
                                &needle,
                                &keyed_item,
                                Some(predicate),
                                env,
                            )?
                        } else if let Some(predicate) = testfn.as_ref() {
                            value_matches_with_test(
                                interp,
                                &needle,
                                &keyed_item,
                                Some(predicate),
                                env,
                            )?
                        } else {
                            values_eql(&needle, &keyed_item)
                        };
                        if matches {
                            return Ok(current);
                        }
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => return Ok(Value::Nil),
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
        "member-ignore-case" => {
            need_args(name, args, 2)?;
            let needle = string_text(&args[0])?.to_ascii_lowercase();
            let items = args[1].to_vec()?;
            for (index, item) in items.iter().enumerate() {
                if string_like(item)
                    .is_some_and(|candidate| candidate.text.to_ascii_lowercase() == needle)
                {
                    return Ok(Value::list(items[index..].iter().cloned()));
                }
            }
            Ok(Value::Nil)
        }
        "assq" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
            let mut seen = HashSet::new();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _)) && item.car()? == args[0] {
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
        "rassq" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
            let mut seen = HashSet::new();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _)) && item.cdr()? == args[0] {
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
        "rassoc" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
            let mut seen = HashSet::new();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _))
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
        "rassq-delete-all" => {
            need_args(name, args, 2)?;
            rassq_delete_all(&args[0], &args[1])
        }
        "assq-delete-all" => {
            need_args(name, args, 2)?;
            assq_delete_all(&args[0], &args[1])
        }
        "assoc-delete-all" => {
            need_args(name, args, 2)?;
            assoc_delete_all(interp, &args[0], &args[1])
        }
        "assoc" => {
            need_arg_range(name, args, 2, 3)?;
            let mut current = args[1].clone();
            let mut seen = HashSet::new();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _))
                            && if let Some(testfn) = args.get(2).filter(|value| !value.is_nil()) {
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
                    Value::Cons(_, _) => item.car()?,
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
        "alist-get" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let default = args.get(2).cloned().unwrap_or(Value::Nil);
            let testfn = args.get(4);
            let items = args[1].to_vec()?;
            for item in items {
                let Some((car, cdr)) = item.cons_values() else {
                    continue;
                };
                if value_matches_with_test(interp, &args[0], &car, testfn, env)? {
                    return Ok(cdr);
                }
            }
            Ok(default)
        }
        "cl-set-exclusive-or" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let left = args[0].to_vec()?;
            let right = args[1].to_vec()?;
            let mut test = Value::BuiltinFunc("equal".into());
            let mut index = 2usize;
            while index + 1 < args.len() {
                if matches!(&args[index], Value::Symbol(keyword) if keyword == ":test") {
                    test = resolve_callable(interp, &args[index + 1], env)?;
                }
                index += 2;
            }
            let mut result = Vec::new();
            for item in &left {
                if !list_contains_with(interp, &right, item, &test, env)? {
                    result.push(item.clone());
                }
            }
            for item in &right {
                if !list_contains_with(interp, &left, item, &test, env)? {
                    result.push(item.clone());
                }
            }
            Ok(Value::list(result))
        }
        "cl-remove-if-not" => {
            need_args(name, args, 2)?;
            let mut kept = Vec::new();
            for item in args[1].to_vec()? {
                if call_function_value(interp, &args[0], std::slice::from_ref(&item), env)?
                    .is_truthy()
                {
                    kept.push(item);
                }
            }
            Ok(Value::list(kept))
        }
        "cl-delete-if" => cl_delete_if_values(interp, args, env),
        // Fast native ports of the GNU cl-seq.el sequence functions.  The
        // interpreted Lisp definitions are semantically fine but far too
        // slow for the multi-million-element sequences in
        // cl-seq-test-bug24264, so these names are also listed in
        // `prefer_builtin_override'.
        "cl-position" => {
            need_args(name, args, 2)?;
            let keys = parse_cl_seq_keys(
                &args[2..],
                &[
                    ":test",
                    ":test-not",
                    ":key",
                    ":if",
                    ":if-not",
                    ":start",
                    ":end",
                    ":from-end",
                ],
            )?;
            let items = cl_seq_elements(interp, &args[1])?;
            let end = keys.end.unwrap_or(items.len()).min(items.len());
            let mut result = Value::Nil;
            for (index, item) in items.iter().enumerate().take(end).skip(keys.start) {
                if cl_seq_match(interp, &keys, &args[0], item, env)? {
                    result = Value::Integer(index as i64);
                    if !keys.from_end {
                        break;
                    }
                }
            }
            Ok(result)
        }
        "cl-remove" => {
            need_args(name, args, 2)?;
            let keys = parse_cl_seq_keys(
                &args[2..],
                &[
                    ":test",
                    ":test-not",
                    ":key",
                    ":if",
                    ":if-not",
                    ":count",
                    ":from-end",
                    ":start",
                    ":end",
                ],
            )?;
            let items = cl_seq_elements(interp, &args[1])?;
            let count = keys.count.unwrap_or(items.len() as i64);
            if count <= 0 {
                return Ok(args[1].clone());
            }
            let end = keys.end.unwrap_or(items.len()).min(items.len());
            let mut matched = Vec::new();
            for (index, item) in items.iter().enumerate().take(end).skip(keys.start) {
                if cl_seq_match(interp, &keys, &args[0], item, env)? {
                    matched.push(index);
                }
            }
            if matched.is_empty() {
                return Ok(args[1].clone());
            }
            let count = count as usize;
            if matched.len() > count {
                if keys.from_end {
                    matched.drain(..matched.len() - count);
                } else {
                    matched.truncate(count);
                }
            }
            let mut kept = Vec::with_capacity(items.len() - matched.len());
            let mut drop = matched.iter().copied().peekable();
            for (index, item) in items.into_iter().enumerate() {
                if drop.peek() == Some(&index) {
                    drop.next();
                } else {
                    kept.push(item);
                }
            }
            cl_seq_rebuild(&args[1], kept)
        }
        "cl-substitute" => {
            need_args(name, args, 3)?;
            let keys = parse_cl_seq_keys(
                &args[3..],
                &[
                    ":test",
                    ":test-not",
                    ":key",
                    ":if",
                    ":if-not",
                    ":count",
                    ":start",
                    ":end",
                    ":from-end",
                ],
            )?;
            let mut items = cl_seq_elements(interp, &args[2])?;
            let count = keys.count.unwrap_or(items.len() as i64);
            if count <= 0 || values_eq_in_env(interp, &args[0], &args[1], env) {
                return Ok(args[2].clone());
            }
            let end = keys.end.unwrap_or(items.len()).min(items.len());
            let mut matched = Vec::new();
            for (index, item) in items.iter().enumerate().take(end).skip(keys.start) {
                if cl_seq_match(interp, &keys, &args[1], item, env)? {
                    matched.push(index);
                }
            }
            if matched.is_empty() {
                return Ok(args[2].clone());
            }
            let count = count as usize;
            if matched.len() > count {
                if keys.from_end {
                    matched.drain(..matched.len() - count);
                } else {
                    matched.truncate(count);
                }
            }
            for index in matched {
                items[index] = args[0].clone();
            }
            cl_seq_rebuild(&args[2], items)
        }
        "cl-replace" => {
            need_args(name, args, 2)?;
            let keys = parse_cl_seq_keys(&args[2..], &[":start1", ":end1", ":start2", ":end2"])?;
            let source = cl_seq_elements(interp, &args[1])?;
            let source_end = keys.end2.unwrap_or(source.len()).min(source.len());
            let source: Vec<Value> = source
                .into_iter()
                .take(source_end)
                .skip(keys.start2)
                .collect();
            let mut budget = match keys.end1 {
                Some(end1) => end1.saturating_sub(keys.start1).min(source.len()),
                None => source.len(),
            };
            if matches!(&args[0], Value::Cons(_, _) | Value::Nil) && !is_vector_value(&args[0]) {
                let mut tail = args[0].clone();
                for _ in 0..keys.start1 {
                    let Value::Cons(_, cdr) = tail else { break };
                    let next = cdr.borrow().clone();
                    tail = next;
                }
                let mut src = source.into_iter();
                while budget > 0
                    && matches!(&tail, Value::Cons(_, _))
                    && let Some(item) = src.next()
                {
                    tail.set_car(item)?;
                    let Value::Cons(_, cdr) = tail else { break };
                    let next = cdr.borrow().clone();
                    tail = next;
                    budget -= 1;
                }
            } else {
                let len = cl_seq_elements(interp, &args[0])?.len();
                for (offset, item) in source.into_iter().take(budget).enumerate() {
                    let index = keys.start1 + offset;
                    if index >= len {
                        break;
                    }
                    cl_seq_set_element(&args[0], index, item)?;
                }
            }
            Ok(args[0].clone())
        }
        "cl-fill" => {
            need_args(name, args, 2)?;
            let keys = parse_cl_seq_keys(&args[2..], &[":start", ":end"])?;
            if matches!(&args[0], Value::Cons(_, _) | Value::Nil) && !is_vector_value(&args[0]) {
                let mut tail = args[0].clone();
                for _ in 0..keys.start {
                    let Value::Cons(_, cdr) = tail else { break };
                    let next = cdr.borrow().clone();
                    tail = next;
                }
                let mut budget = keys.end.map(|end| end.saturating_sub(keys.start));
                while budget != Some(0) && matches!(&tail, Value::Cons(_, _)) {
                    tail.set_car(args[1].clone())?;
                    let Value::Cons(_, cdr) = tail else { break };
                    let next = cdr.borrow().clone();
                    tail = next;
                    budget = budget.map(|n| n - 1);
                }
            } else {
                let len = cl_seq_elements(interp, &args[0])?.len();
                let end = keys.end.unwrap_or(len).min(len);
                for index in keys.start..end {
                    cl_seq_set_element(&args[0], index, args[1].clone())?;
                }
            }
            Ok(args[0].clone())
        }
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
        "cl-mapcar" => {
            need_args(name, args, 2)?;
            let lists = args[1..]
                .iter()
                .map(|value| sequence_values(interp, value))
                .collect::<Result<Vec<_>, _>>()?;
            let len = lists.iter().map(Vec::len).min().unwrap_or(0);
            let mut results = Vec::with_capacity(len);
            for index in 0..len {
                let call_args = lists
                    .iter()
                    .map(|list| list[index].clone())
                    .collect::<Vec<_>>();
                results.push(call_function_value(interp, &args[0], &call_args, env)?);
            }
            Ok(Value::list(results))
        }
        "cl-mapcan" => {
            need_args(name, args, 2)?;
            let mapped = super::call(interp, "cl-mapcar", args, env)?.to_vec()?;
            let mut flattened = Vec::new();
            for item in mapped {
                flattened.extend(item.to_vec()?);
            }
            Ok(Value::list(flattened))
        }
        "cl-some" => {
            need_args(name, args, 2)?;
            let sequences = args[1..]
                .iter()
                .map(|value| sequence_values(interp, value))
                .collect::<Result<Vec<_>, _>>()?;
            let len = sequences.iter().map(Vec::len).min().unwrap_or(0);
            for index in 0..len {
                let call_args = sequences
                    .iter()
                    .map(|sequence| sequence[index].clone())
                    .collect::<Vec<_>>();
                let result = call_function_value(interp, &args[0], &call_args, env)?;
                if result.is_truthy() {
                    return Ok(result);
                }
            }
            Ok(Value::Nil)
        }
        "seq-mapcat" => {
            need_arg_range(name, args, 2, 3)?;
            let sequence = sequence_values(interp, &args[1])?;
            let mut flattened = Vec::new();
            for item in sequence {
                let mapped = call_function_value(interp, &args[0], &[item], env)?;
                flattened.extend(sequence_values(interp, &mapped)?);
            }

            match args
                .get(2)
                .and_then(|value| value.as_symbol().ok())
                .unwrap_or("list")
            {
                "list" => Ok(Value::list(flattened)),
                "vector" => Ok(Value::list(
                    std::iter::once(Value::Symbol("vector-literal".into())).chain(flattened),
                )),
                "string" => super::call(interp, "concat", &flattened, env),
                other => Err(LispError::Signal(format!(
                    "Unsupported seq-mapcat result type: {other}"
                ))),
            }
        }
        "mapc" => {
            need_args(name, args, 2)?;
            let list = sequence_values(interp, &args[1])?;
            for item in &list {
                let _ = call_function_value(interp, &args[0], std::slice::from_ref(item), env)?;
            }
            Ok(args[1].clone())
        }
        "cl-reduce" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            let Some((first, rest)) = items.split_first() else {
                return Ok(Value::Nil);
            };
            let mut acc = first.clone();
            for item in rest {
                acc = call_function_value(interp, &args[0], &[acc.clone(), item.clone()], env)?;
            }
            Ok(acc)
        }
        "eval" => eval_impl(interp, args, env),
        "eval-buffer" => eval_buffer_impl(interp, args, env),
        "eval-region" => eval_region_impl(interp, args, env),
        "unload-feature" => unload_feature_impl(interp, args, env),
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
                })
            } else {
                StringLike {
                    text: String::new(),
                    props: Vec::new(),
                    multibyte: false,
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
        "string-join" => {
            need_arg_range(name, args, 1, 2)?;
            let separator = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| Value::String(String::new()));
            super::call(
                interp,
                "mapconcat",
                &[Value::Symbol("identity".into()), args[0].clone(), separator],
                env,
            )
        }
        "ensure-list" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].is_nil() || matches!(args[0], Value::Cons(_, _)) {
                    args[0].clone()
                } else {
                    Value::list([args[0].clone()])
                },
            )
        }
        "position-symbol" => {
            need_args(name, args, 2)?;
            let position = args[1].as_integer()?;
            Ok(interp.create_record(
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
        "seq-find" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let predicate = resolve_callable(interp, &args[0], env)?;
            if let Ok(items) = vector_items(&args[1]) {
                for item in items {
                    if interp
                        .call_function_value(
                            predicate.clone(),
                            args[0].as_symbol().ok(),
                            std::slice::from_ref(&item),
                            env,
                        )?
                        .is_truthy()
                    {
                        return Ok(item);
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = sequence_string_like(&args[1]) {
                for ch in string.text.chars() {
                    let item = string_sequence_value(&string, ch);
                    if interp
                        .call_function_value(
                            predicate.clone(),
                            args[0].as_symbol().ok(),
                            std::slice::from_ref(&item),
                            env,
                        )?
                        .is_truthy()
                    {
                        return Ok(item);
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[1].type_name()))
            }
        }
        "seq-contains-p" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Ok(items) = vector_items(&args[0]) {
                for item in items {
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &item, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &item, &args[1])
                    };
                    if matches {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = sequence_string_like(&args[0]) {
                for ch in string.text.chars() {
                    let candidate = string_sequence_value(&string, ch);
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &candidate, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &candidate, &args[1])
                    };
                    if matches {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "seq-take" => {
            need_args(name, args, 2)?;
            let count = args[1].as_integer()?.max(0) as usize;
            if let Ok(items) = args[0].to_vec() {
                Ok(Value::list(items.into_iter().take(count)))
            } else if let Some(string) = string_like(&args[0]) {
                let text: String = string.text.chars().take(count).collect();
                let props = slice_string_props(&string.props, 0, text.chars().count());
                Ok(string_like_value(text, props))
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "seq-position" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Ok(items) = args[0].to_vec() {
                for (index, item) in items.into_iter().enumerate() {
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &item, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &item, &args[1])
                    };
                    if matches {
                        return Ok(Value::Integer(index as i64));
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = string_like(&args[0]) {
                for (index, ch) in string.text.chars().enumerate() {
                    let candidate = string_sequence_value(&string, ch);
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &candidate, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &candidate, &args[1])
                    };
                    if matches {
                        return Ok(Value::Integer(index as i64));
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "cl-coerce" => {
            need_args(name, args, 2)?;
            let items = if is_bool_vector_value(interp, &args[0]) {
                bool_vector_values(interp, &args[0])?
            } else {
                sequence_values(interp, &args[0])?
            };
            match args[1].as_symbol()? {
                "list" => Ok(Value::list(items)),
                "vector" => {
                    let mut vector = vec![Value::symbol("vector-literal")];
                    vector.extend(items);
                    Ok(Value::list(vector))
                }
                "string" => {
                    let mut text = String::new();
                    for item in items {
                        let code = item.as_integer()?;
                        let ch = char::from_u32(code as u32).ok_or_else(|| {
                            LispError::Signal(format!("Invalid character: {code}"))
                        })?;
                        text.push(ch);
                    }
                    Ok(Value::String(text))
                }
                kind => Err(LispError::Signal(format!(
                    "cl-coerce unsupported type: {kind}"
                ))),
            }
        }
        "treesit-language-available-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "treesit--linecol-cache" => {
            need_args(name, args, 0)?;
            Ok(interp
                .buffer_local_value(interp.current_buffer_id(), TREESIT_LINECOL_CACHE_VAR)
                .unwrap_or_else(treesit_default_linecol_cache))
        }
        "treesit--linecol-cache-set" => {
            need_args(name, args, 3)?;
            let cache = treesit_linecol_cache_value(
                args[0].as_integer()?,
                args[1].as_integer()?,
                args[2].as_integer()?,
            );
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                TREESIT_LINECOL_CACHE_VAR,
                cache,
            );
            Ok(Value::Nil)
        }
        "treesit--linecol-at" => {
            if args.is_empty() || args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .map(|value| value.max(1) as usize)
                .unwrap_or_else(|| interp.current_buffer().point());
            treesit_linecol_at(interp, pos)
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
        "apply-partially" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let rest_name = "__emaxx-apply-partially-rest".to_string();
            let mut body = vec![Value::Symbol("apply".into()), literal_form(&args[0])];
            body.extend(args[1..].iter().map(literal_form));
            body.push(Value::Symbol(rest_name.clone()));
            Ok(Value::Lambda(
                vec!["&rest".into(), rest_name],
                vec![Value::list(body)].into(),
                shared_env(env.clone()),
            ))
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
            let symbol = args[0].as_symbol()?;
            if args[1].is_nil() {
                interp.shadow_macro_binding(symbol);
                interp.set_function_binding(symbol, None);
                Ok(Value::Nil)
            } else {
                interp.validate_function_binding(symbol, &args[1])?;
                // GNU macro-ness lives in the function cell: only a
                // (macro . EXPANDER) cell or a symbol alias keeps it; any
                // other definition erases the macro.
                let keeps_macro = matches!(&args[1], Value::Symbol(_))
                    || args[1]
                        .cons_values()
                        .is_some_and(|(car, _)| matches!(&car, Value::Symbol(s) if s == "macro"));
                if !keeps_macro {
                    interp.shadow_macro_binding(symbol);
                }
                interp.set_function_binding(symbol, Some(args[1].clone()));
                Ok(args[1].clone())
            }
        }
        "fmakunbound" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            // GNU voids the function cell outright; shadowed stale entries
            // (repeated defuns push duplicates) must not resurface.
            interp.remove_all_function_bindings(symbol);
            interp.shadow_macro_binding(symbol);
            // The dispatch-chain metadata describes the (now removed)
            // function binding; a fresh generic must not rank its methods
            // against specializers of the destroyed chain.
            interp.put_symbol_property(symbol, "emaxx-cl-defmethod-specializers", Value::Nil);
            Ok(Value::Symbol(symbol.to_string()))
        }
        "funcall-interactively" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let func = resolve_callable(interp, &args[0], env)?;
            invoke_function_value(interp, &func, &args[1..], env)
        }
        "call-interactively" => call_interactively_impl(interp, args, env),
        "keyboard-quit" => Err(LispError::SignalValue(Value::list([
            Value::Symbol("quit".into()),
            Value::Nil,
        ]))),
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
                let mut execute_args = vec![last_macro, Value::Integer(repeat.saturating_sub(1))];
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
                    .ok_or_else(|| LispError::TypeError("stringp".into(), args[0].type_name()))?
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
                    Value::Buffer(interp.current_buffer_id(), interp.buffer.name.clone()),
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
                .ok_or_else(|| LispError::TypeError("stringp".into(), args[0].type_name()))?;
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
            interp.set_variable(
                "this-single-command-keys",
                event_vector(std::iter::empty()),
                env,
            );
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
                        include_commands || event.cons_values().is_none_or(|(car, _)| !car.is_nil())
                    })
                    .cloned(),
            ))
        }
        "define-keymap" => Ok(keymap_placeholder(None)),
        "define-abbrev-table" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol = args[0].as_symbol()?.to_string();
            let table = match interp.lookup_var(&symbol, &Vec::new()) {
                Some(existing) if is_abbrev_table_value(interp, &existing) => existing,
                _ => {
                    let created = make_runtime_abbrev_table(interp, Some(&symbol), Value::Nil);
                    interp.set_global_binding(&symbol, created.clone());
                    register_abbrev_table_symbol(interp, &symbol);
                    created
                }
            };
            if let Some(docstring) = args.get(2)
                && matches!(docstring, Value::String(_) | Value::StringObject(_))
            {
                interp.put_symbol_property(&symbol, "variable-documentation", docstring.clone());
            }
            let mut prop_index = 2usize;
            if matches!(args.get(2), Some(Value::String(_) | Value::StringObject(_))) {
                prop_index = 3;
            }
            if !(args.len() - prop_index).is_multiple_of(2) {
                return Err(LispError::Signal(
                    "Invalid abbrev table property list".into(),
                ));
            }
            while prop_index + 1 < args.len() {
                set_abbrev_table_property(
                    interp,
                    &table,
                    &args[prop_index],
                    args[prop_index + 1].clone(),
                )?;
                prop_index += 2;
            }
            set_abbrev_table_entries_from_definitions(interp, &table, &args[1])?;
            Ok(table)
        }
        "read-key" => {
            need_arg_range(name, args, 0, 2)?;
            ensure_interaction_allowed(interp, env)?;
            let disable_fallbacks = args.get(1).is_some_and(Value::is_truthy);
            loop {
                let event = if let Some(decoded) = read_decoded_input_event(interp, env)? {
                    decoded
                } else {
                    normalize_input_event_value(pop_unread_command_event_value(interp, env)?)?
                };
                if !disable_fallbacks && is_mouse_down_event(&event) {
                    continue;
                }
                return Ok(event);
            }
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
                let wait_result = wait_pumping_processes(interp, env, Some(timeout), false, None);
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
        "mouse-double-click-time" => {
            need_arg_range(name, args, 0, 0)?;
            let value = interp
                .lookup_var("double-click-time", env)
                .unwrap_or(Value::Nil);
            match value {
                Value::T => Ok(Value::Integer(10_000)),
                Value::Integer(value) if value > 0 => Ok(Value::Integer(value)),
                Value::Float(value) if value > 0.0 => Ok(Value::Float(value)),
                _ => Ok(Value::Integer(0)),
            }
        }
        "context-menu-map" => {
            need_arg_range(name, args, 0, 1)?;
            let click = args
                .first()
                .cloned()
                .or_else(|| interp.lookup_var("last-input-event", env))
                .unwrap_or(Value::Nil);
            let mut menu = make_runtime_keymap(interp, Some("Context Menu"));

            for function in interp
                .lookup_var("context-menu-functions", env)
                .unwrap_or(Value::Nil)
                .to_vec()?
            {
                let result =
                    call_function_value(interp, &function, &[menu.clone(), click.clone()], env)?;
                if is_keymap_value(interp, &result) {
                    menu = result;
                }
            }

            if let Some(filter) = interp.lookup_var("context-menu-filter-function", env)
                && !filter.is_nil()
            {
                let result =
                    call_function_value(interp, &filter, &[menu.clone(), click.clone()], env)?;
                if is_keymap_value(interp, &result) {
                    menu = result;
                }
            }

            context_menu_keymap_items(interp, &menu)
        }
        "read-string" | "read-from-minibuffer" | "read-no-blanks-input" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            ensure_interaction_allowed(interp, env)?;
            let initial = args
                .get(1)
                .and_then(string_like)
                .map(|string| string.text)
                .unwrap_or_default();
            if let Some(contents) = read_minibuffer_text_from_unread_events(interp, env, &initial)?
                .or_else(|| read_minibuffer_text_from_kbd_macro(interp, env, &initial))
            {
                if name == "read-from-minibuffer" && args.get(3).is_some_and(Value::is_truthy) {
                    let parsed =
                        super::call(interp, "read-from-string", &[Value::String(contents)], env)?;
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
                return Ok(Value::String(contents));
            }
            Ok(Value::String(String::new()))
        }
        "completing-read" => completing_read(interp, args, env),
        "read-buffer" => {
            need_arg_range(name, args, 1, 4)?;
            let buffers = super::call(interp, "buffer-list", &[], env)?
                .to_vec()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|buffer| match buffer {
                    Value::Buffer(_, buffer_name) => Some(Value::String(buffer_name)),
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
                Value::Symbol(symbol) => {
                    Value::String(crate::lisp::types::visible_symbol_name(&symbol).to_string())
                }
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
        }
        "read-file-name" => {
            need_arg_range(name, args, 1, 6)?;
            if let Some(function) = interp.lookup_var("read-file-name-function", env)
                && function.is_truthy()
            {
                return interp.call_function_value(function, None, args, env);
            }
            let prompt = args[0].clone();
            let dir = args.get(1).cloned().unwrap_or(Value::Nil);
            let default = args.get(2).cloned().unwrap_or(Value::Nil);
            let mustmatch = args.get(3).cloned().unwrap_or(Value::Nil);
            let initial = args.get(4).cloned().unwrap_or(Value::Nil);
            let completion = interp.call_function_value(
                Value::Symbol("completing-read".into()),
                Some("completing-read"),
                &[
                    prompt,
                    Value::Nil,
                    Value::Nil,
                    mustmatch,
                    initial,
                    Value::Nil,
                    default.clone(),
                ],
                env,
            )?;
            if let Some(text) = string_like(&completion)
                && !text.text.is_empty()
            {
                return Ok(Value::String(text.text));
            }
            if let Some(text) = string_like(&default)
                && !text.text.is_empty()
            {
                return Ok(Value::String(text.text));
            }
            if let Some(text) = string_like(&dir) {
                return Ok(Value::String(text.text));
            }
            Ok(Value::String(String::new()))
        }
        "format-prompt" => format_prompt(interp, args, env),

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

/// Parsed keyword arguments for the native cl-seq functions, mirroring the
/// bindings established by GNU cl-seq.el's `cl--parsing-keywords'.
struct ClSeqKeys {
    test: Option<Value>,
    test_not: bool,
    if_fn: Option<Value>,
    if_not: bool,
    key: Option<Value>,
    count: Option<i64>,
    start: usize,
    end: Option<usize>,
    from_end: bool,
    start1: usize,
    end1: Option<usize>,
    start2: usize,
    end2: Option<usize>,
}

fn cl_seq_plist_value(keys: &[Value], keyword: &str) -> Option<Value> {
    let mut index = 0;
    while index < keys.len() {
        if matches!(&keys[index], Value::Symbol(name) if name == keyword) {
            return Some(keys.get(index + 1).cloned().unwrap_or(Value::Nil));
        }
        index += 2;
    }
    None
}

fn parse_cl_seq_keys(keys: &[Value], allowed: &[&str]) -> Result<ClSeqKeys, LispError> {
    let allow_other = cl_seq_plist_value(keys, ":allow-other-keys")
        .map(|value| value.is_truthy())
        .unwrap_or(false);
    if !allow_other {
        let mut index = 0;
        while index < keys.len() {
            let known = matches!(&keys[index], Value::Symbol(name)
                if allowed.iter().any(|kw| kw == name));
            if !known {
                return Err(LispError::Signal(format!(
                    "Bad keyword argument {}",
                    keys[index]
                )));
            }
            index += 2;
        }
    }
    let truthy = |keyword: &str| cl_seq_plist_value(keys, keyword).filter(Value::is_truthy);
    let index_arg = |keyword: &str| -> Result<Option<usize>, LispError> {
        truthy(keyword)
            .map(|value| value.as_integer().map(|n| n.max(0) as usize))
            .transpose()
    };
    let mut test = truthy(":test");
    let mut test_not = false;
    if let Some(value) = truthy(":test-not") {
        test = Some(value);
        test_not = true;
    }
    let mut if_fn = truthy(":if");
    let mut if_not = false;
    if let Some(value) = truthy(":if-not") {
        if_fn = Some(value);
        if_not = true;
    }
    Ok(ClSeqKeys {
        test,
        test_not,
        if_fn,
        if_not,
        key: truthy(":key"),
        count: truthy(":count")
            .map(|value| value.as_integer())
            .transpose()?,
        start: index_arg(":start")?.unwrap_or(0),
        end: index_arg(":end")?,
        from_end: truthy(":from-end").is_some(),
        start1: index_arg(":start1")?.unwrap_or(0),
        end1: index_arg(":end1")?,
        start2: index_arg(":start2")?.unwrap_or(0),
        end2: index_arg(":end2")?,
    })
}

/// GNU cl-seq.el's `cl--check-test': apply :key, then match via :test /
/// :test-not / :if / :if-not, defaulting to `eql'.
fn cl_seq_match(
    interp: &mut Interpreter,
    keys: &ClSeqKeys,
    item: &Value,
    element: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<bool, LispError> {
    let keyed = match &keys.key {
        Some(key) => call_function_value(interp, key, std::slice::from_ref(element), env)?,
        None => element.clone(),
    };
    if let Some(test) = &keys.test {
        let result = call_function_value(interp, test, &[item.clone(), keyed], env)?;
        Ok(result.is_truthy() != keys.test_not)
    } else if let Some(predicate) = &keys.if_fn {
        let result = call_function_value(interp, predicate, &[keyed], env)?;
        Ok(result.is_truthy() != keys.if_not)
    } else {
        Ok(values_eql(item, &keyed))
    }
}

fn cl_seq_elements(interp: &Interpreter, sequence: &Value) -> Result<Vec<Value>, LispError> {
    sequence_values(interp, sequence)
}

/// Rebuild a fresh sequence of the same kind as ORIGINAL, mirroring the
/// list / `concat' / `vconcat' result types of GNU cl-remove.
fn cl_seq_rebuild(original: &Value, items: Vec<Value>) -> Result<Value, LispError> {
    match original {
        Value::String(_) | Value::StringObject(_) => {
            let mut text = String::new();
            for item in &items {
                let (ch, _multibyte) = concat_character_value(item)?;
                text.push(ch);
            }
            Ok(Value::String(text))
        }
        value if is_vector_value(value) => {
            let mut vector = vec![Value::symbol("vector-literal")];
            vector.extend(items);
            Ok(Value::list(vector))
        }
        Value::Cons(_, _) | Value::Nil => Ok(Value::list(items)),
        _ => {
            let mut vector = vec![Value::symbol("vector-literal")];
            vector.extend(items);
            Ok(Value::list(vector))
        }
    }
}

fn cl_seq_set_element(target: &Value, index: usize, value: Value) -> Result<(), LispError> {
    match target {
        Value::String(_) | Value::StringObject(_) => {
            aset_string_value(target, index, &value)?;
            Ok(())
        }
        _ => aset_vector_value(target, index, value),
    }
}
