//! keyboard.c's command loop: Frecursive_edit, recursive_edit_1,
//! command_loop, command_loop_2 and command_loop_1.  The terminal's
//! top level, every minibuffer read, each keyboard macro iteration and
//! `recursive-edit' run this one loop; read_char reads its events from
//! the unread lists, the executing macro and the keyboard in that order.

use super::*;

/// The handlers command_loop_2 runs command_loop_1 under: `error' for the
/// command loops (every condition, the debugger let in), and
/// `(minibuffer-quit)' for a keyboard macro's iterations
/// (Fexecute_kbd_macro), whose other errors leave the macro.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandLoopHandlers {
    Error,
    MinibufferQuit,
}

/// The terminal frontend's xdisp.c:resize_echo_area_exactly.
pub(crate) type ResizeEchoAreaExactly = Box<dyn Fn(&mut Interpreter, &mut Env)>;

thread_local! {
    static RESIZE_ECHO_AREA_EXACTLY: std::cell::RefCell<Option<std::rc::Rc<ResizeEchoAreaExactly>>> =
        const { std::cell::RefCell::new(None) };
}

/// The frontend's resize_echo_area_exactly, none without a terminal.
pub(crate) fn set_resize_echo_area_exactly(hook: Option<ResizeEchoAreaExactly>) {
    RESIZE_ECHO_AREA_EXACTLY.with_borrow_mut(|slot| *slot = hook.map(std::rc::Rc::new));
}

/// xdisp.c:resize_echo_area_exactly as command_loop_1 calls it: the mini
/// window showing a message resized to it (exactly, with no minibuffer
/// active) and, when its height changed, redisplayed with
/// windows_or_buffers_changed 42 and update_mode_lines 30.
pub(super) fn resize_echo_area_exactly(interp: &mut Interpreter, env: &mut Env) {
    let hook = RESIZE_ECHO_AREA_EXACTLY.with_borrow(|slot| slot.clone());
    if let Some(hook) = hook {
        hook(interp, env);
    }
}

/// keyboard.c:Frecursive_edit: one more command loop level, the current
/// buffer given back when it was not the selected window's
/// (recursive_edit_unwind), around recursive_edit_1.
pub(crate) fn recursive_edit(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    let buffer = interp
        .selected_window_contents_buffer_id()
        .filter(|selected| *selected != interp.current_buffer_id())
        .and_then(|_| interp.buffer_value(interp.current_buffer_id()))
        .unwrap_or(Value::Nil);
    interp.command_loop_recursion_depth += 1;
    interp.update_mode_lines = 17;
    let result = interp.with_lisp_stack_roots(&buffer, |interp| recursive_edit_1(interp, env));
    // recursive_edit_unwind: Fset_buffer of BUFFER, which signals when it
    // was killed meanwhile (the level then stays counted, as GNU's does),
    // and the level given back.
    if !buffer.is_nil() {
        super::call(interp, "set-buffer", &[buffer], env)?;
    }
    interp.command_loop_recursion_depth -= 1;
    interp.update_mode_lines = 18;
    result.map(|()| Value::Nil)
}

/// keyboard.c:recursive_edit_1: inside a recursive edit the standard
/// streams are t again and symbols with position off; redisplay is let
/// in and the buffers changed before go unrecorded for undo boundaries;
/// then the command loop, whose value thrown to `exit' is acted on: t
/// quits, a string is an error's message, a function is called.
pub(crate) fn recursive_edit_1(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let count = interp.specpdl_index();
    let result = (|| {
        if interp.command_loop_recursion_depth > 0 {
            interp.specbind_symbol(&"standard-output".into(), Value::T, env)?;
            interp.specbind_symbol(&"standard-input".into(), Value::T, env)?;
            interp.specbind_symbol(&"symbols-with-pos-enabled".into(), Value::Nil, env)?;
            interp.specbind_symbol(&"print-symbols-bare".into(), Value::Nil, env)?;
        }
        interp.specbind_symbol(&"inhibit-redisplay".into(), Value::Nil, env)?;
        interp.specbind_symbol(
            &"undo-auto--undoably-changed-buffers".into(),
            Value::Nil,
            env,
        )?;
        let value = command_loop(interp, env)?;
        crate::lisp::primitives::dispatch::recursive_edit_exit_value(interp, env, value)
    })();
    let unbound = interp.unbind_to(count, env);
    result?;
    unbound
}

/// keyboard.c:command_loop.  Within a recursive edit or a minibuffer it
/// is command_loop_2 under a catch for `exit', whose thrown value it
/// returns; at the top level it runs the startup (top_level_1) and then
/// command_loop_2, each under a catch for `top-level', for ever.
///
/// HAVE_STACK_OVERFLOW_HANDLING's entry is where a C stack overflow
/// returns to (sysdep.c:handle_sigsegv's siglongjmp to
/// return_to_command_loop, here the exit `Interpreter::c_stack_overflow'
/// starts, which the innermost running command loop takes): init_eval's
/// resets, and the loop entered again with `internal--top-level-message'
/// saying so, which `normal-top-level' shows at the top level.
pub(crate) fn command_loop(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    interp.command_loops_entered += 1;
    let mut message = "Back to top level";
    let result = loop {
        interp.set_variable_key(
            cached_symbol!("internal--top-level-message"),
            Value::string(message),
            env,
        );
        match command_loop_body(interp, env) {
            Err(error) if is_c_stack_overflow_exit(&error) => {
                init_eval_after_overflow(interp, env);
                message = "Re-entering top level after C stack overflow";
            }
            result => break result,
        }
    };
    interp.command_loops_entered -= 1;
    result
}

/// Whether ERROR is the exit after a C stack overflow.
fn is_c_stack_overflow_exit(error: &LispError) -> bool {
    matches!(error.kind(), LispErrorKind::Terminate(termination) if termination.c_stack_overflow)
}

/// eval.c:init_eval as command_loop runs it after a C stack overflow:
/// `quit-flag' nil, debug_on_next_call cleared and when_entered_debugger
/// -1 (less than any `num-nonmacro-input-events', so the next error may
/// enter the debugger), here with the exit's pending termination taken
/// back.  Its `specpdl_ptr = specpdl' and
/// `lisp_eval_depth = 0' were done by the frames the exit left, which
/// popped their specpdl entries undone and gave their depth back; the
/// entries and depth of the frames outside this command loop stay.  (GNU's
/// drop the outer recursive edits' entries too, so that their unbind_to
/// later walks below the specpdl's start; there are none at the top
/// level.)
fn init_eval_after_overflow(interp: &mut Interpreter, env: &mut Env) {
    interp.c_stack_overflow_exit = false;
    interp.take_pending_termination();
    interp.clear_quit_flag_in_place();
    interp.clear_debug_on_next_call();
    interp.set_variable_key(
        cached_symbol!("internal-when-entered-debugger"),
        Value::Integer(-1),
        env,
    );
}

/// command_loop past its HAVE_STACK_OVERFLOW_HANDLING entry.
fn command_loop_body(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    if interp.command_loop_recursion_depth > 0 || interp.minibuffer_depth() > 0 {
        let result = internal_catch(interp, env, "exit", |interp, env| {
            command_loop_2(interp, env, CommandLoopHandlers::Error).map(|()| Value::Nil)
        });
        interp.executing_kbd_macro = false;
        return result;
    }
    loop {
        top_level_catch(interp, env, |interp, env| {
            top_level_1(interp, env).map(|()| Value::Nil)
        })?;
        top_level_catch(interp, env, |interp, env| {
            command_loop_2(interp, env, CommandLoopHandlers::Error).map(|()| Value::Nil)
        })?;
        interp.executing_kbd_macro = false;
        // End of file in -batch run causes exit here.
        if interp
            .forwarded_c_value("noninteractive", &Env::new())
            .is_some_and(|value| value.is_truthy())
        {
            super::call(interp, "kill-emacs", &[Value::T, Value::Nil], env)?;
        }
    }
}

/// command_loop's internal_catch (Qtop_level, ...): an error no handler
/// takes is eval.c:signal_or_quit's Fthrow (Qtop_level, Qt), which this
/// catch, the outermost, receives (an error escaping cmd_error, for one).
fn top_level_catch(
    interp: &mut Interpreter,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    let count = interp.specpdl_index();
    match internal_catch(interp, env, "top-level", body).map_err(LispError::into_kind) {
        Ok(value) => Ok(value),
        Err(error @ (LispErrorKind::Throw(..) | LispErrorKind::Terminate(_))) => {
            Err(LispError::from(error))
        }
        Err(_) => {
            interp.unbind_to(count, env)?;
            Ok(Value::T)
        }
    }
}

/// eval.c:internal_catch: BODY with TAG caught; a throw to it is BODY's
/// value.
fn internal_catch(
    interp: &mut Interpreter,
    env: &mut Env,
    tag: &str,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    let tag = Value::symbol(tag);
    let count = interp.specpdl_index();
    let catcher = interp.push_active_catch_tag(tag);
    let result = body(interp, env);
    interp.pop_handler_bindings(catcher);
    let error = match result {
        Ok(value) => return Ok(value),
        Err(error) => error,
    };
    if let LispErrorKind::Throw(thrown, value) = error.kind()
        && crate::lisp::types::eq(*thrown, tag)
    {
        let value = *value;
        interp.unbind_to(count, env)?;
        return Ok(value);
    }
    Err(error)
}

/// keyboard.c:top_level_1: the startup, `top-level''s form (normal
/// top level, which says it is back there when run again), its error
/// reported by cmd_error; without one, that standard Lisp is missing.
pub(crate) fn top_level_1(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let form = interp
        .forwarded_c_value("top-level", &Env::new())
        .unwrap_or(Value::Nil);
    if form.is_nil() {
        let purify = interp
            .forwarded_c_value("purify-flag", &Env::new())
            .unwrap_or(Value::Nil);
        let message = if purify.is_nil() {
            "Bare Emacs (standard Lisp code not loaded)"
        } else {
            "Bare impure Emacs (standard Lisp code not loaded)"
        };
        return crate::lisp::primitives::dispatch::display::message3(
            interp,
            Value::string(message),
            Some(message),
            env,
        );
    }
    let start = interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::Error);
    let result = crate::batch::top_level_2(interp, form, env);
    interp.pop_handler_bindings(start);
    match result {
        Ok(_) => Ok(()),
        Err(error)
            if !matches!(
                error.kind(),
                LispErrorKind::Throw(..) | LispErrorKind::Terminate(_)
            ) =>
        {
            cmd_error(interp, env, &error)
        }
        Err(error) => Err(error),
    }
}

/// keyboard.c:command_loop_2: command_loop_1 under HANDLERS with
/// cmd_error as their handler, entered again after each error it reports,
/// until it returns at the end of a keyboard macro or of input.
pub(crate) fn command_loop_2(
    interp: &mut Interpreter,
    env: &mut Env,
    handlers: CommandLoopHandlers,
) -> Result<(), LispError> {
    loop {
        let start = match handlers {
            CommandLoopHandlers::Error => {
                interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::Error)
            }
            CommandLoopHandlers::MinibufferQuit => interp.push_condition_case_handler(
                crate::lisp::eval::CaseHeads::One(Value::symbol("minibuffer-quit")),
            ),
        };
        let count = interp.specpdl_index();
        let result = command_loop_1(interp, env);
        interp.pop_handler_bindings(start);
        let error = match result {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let caught = match error.kind() {
            LispErrorKind::Throw(..) | LispErrorKind::Terminate(_) => false,
            _ => match handlers {
                CommandLoopHandlers::Error => true,
                // Not a signal the search passed this handler for.
                CommandLoopHandlers::MinibufferQuit if error.skipped_by_search(start) => false,
                CommandLoopHandlers::MinibufferQuit => error_data_has_condition(
                    interp,
                    &crate::lisp::eval::error_condition_value(&error),
                    "minibuffer-quit",
                ),
            },
        };
        if !caught {
            return Err(error);
        }
        // internal_condition_case unwinds to its own frame before the
        // handler runs.
        interp.unbind_to(count, env)?;
        cmd_error(interp, env, &error)?;
    }
}

/// keyboard.c:command_loop_1: read a key sequence and execute its
/// command, for ever; nil only at the end of a keyboard macro or of
/// input (read_key_sequence's 0).
pub(crate) fn command_loop_1(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    // kset_prefix_arg (current_kboard, Qnil) ... this_command_key_count
    // = 0, then the post-command hooks, so they run after commands that
    // left with an error or a throw to top level, and `last-command'.
    command_loop_entry(interp, env)?;
    // command_loop_1's prev_buffer and prev_modiff and the static
    // last_point_position, as the last command left them (a rejected
    // menu's finalize reads them).
    let mut carried: Option<(u64, crate::buffer::ModCount, usize)> = None;
    loop {
        // Make sure the current window's buffer is selected.
        select_window_buffer(interp)?;
        interp.set_c_variable("deactivate-mark", Value::Nil);
        // If minibuffer on and echo area in use, wait a short time and
        // redraw minibuffer.
        minibuffer_message_timeout(interp, env)?;
        clear_this_command(interp, env);
        // Read next key sequence; i gets its length.
        init_raw_keybuf_count(interp);
        let read =
            super::key_sequence::read_key_sequence(interp, Value::Nil, false, true, false, env)?;
        // read_key_sequence_cmd, read_key_sequence_remapped and the last
        // key, held across what runs before the command does.
        let end_of_input = read.keys.is_empty() && !read.menu_rejected;
        let menu_rejected = read.menu_rejected;
        let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![
            read.cmd,
            read.remapped,
            read.keys.last().copied().unwrap_or(Value::Nil),
        ]);
        drop(read);
        // A filter may have run while we were reading the input.
        select_window_buffer(interp)?;
        increment_num_input_keys(interp, env);
        // End of file -- happens only in a kbd macro, at the end.
        if end_of_input {
            return Ok(());
        }
        // -1 means read_key_sequence got a menu that was rejected.  Just
        // loop around and read another command.
        if menu_rejected {
            super::echo::cancel_echoing(interp);
            clear_command_keys(interp);
            if let Some((prev_buffer, prev_modiff, last_point_position)) = carried {
                finalize_command(interp, env, prev_buffer, prev_modiff, last_point_position)?;
            }
            continue;
        }
        interp.set_variable_key(cached_symbol!("last-command-event"), roots[2], env);
        if interp
            .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
            .is_some_and(|value| value.is_truthy())
            && !interp.quit_flag_is_nil()
        {
            // Will return since macro now empty.
            interp.set_variable_key(cached_symbol!("executing-kbd-macro"), Value::T, env);
            interp.maybe_quit(env)?;
        }
        // Do redisplay processing after this command except in special
        // cases identified below.
        let prev_buffer = interp.current_buffer_id();
        let prev_modiff = interp.buffer.borrow().modified_tick();
        let last_point_position = interp.buffer.borrow().point();
        carried = Some((prev_buffer, prev_modiff, last_point_position));
        interp.set_variable_key(cached_symbol!("disable-point-adjustment"), Value::Nil, env);
        // Process filters and timers may have messed with
        // deactivate-mark; reset it before we execute the command.
        interp.set_c_variable("deactivate-mark", Value::Nil);
        // Remap command through active keymaps.
        interp.set_variable_key(cached_symbol!("this-original-command"), roots[0], env);
        if !roots[1].is_nil() {
            roots[0] = roots[1];
        }
        let cmd = roots[0];
        record_command_lossage(interp, cmd);
        interp.set_variable_key(cached_symbol!("this-command"), cmd, env);
        interp.set_variable_key(cached_symbol!("real-this-command"), cmd, env);
        drop(roots);
        crate::lisp::primitives::safe_run_hooks_maybe_narrowed(
            interp,
            &cached_symbol!("pre-command-hook").symbol(),
            env,
        )?;
        let this_command = interp
            .lookup_var_key(cached_symbol!("this-command"), env)
            .unwrap_or(Value::Nil);
        if this_command.is_nil() {
            // nil means key is undefined.
            interp.call_function_value(Value::symbol("undefined"), Some("undefined"), &[], env)?;
        } else {
            // Ensure that we have added appropriate undo-boundaries as a
            // result of changes from the last command, and record point
            // and buffer for the undo information.
            add_command_undo_boundary(interp, env)?;
            interp.call_function_value(
                Value::symbol("command-execute"),
                Some("command-execute"),
                &[this_command],
                env,
            )?;
        }
        let current_prefix_arg = interp
            .lookup_var_key(cached_symbol!("current-prefix-arg"), env)
            .unwrap_or(Value::Nil);
        interp.set_variable_key(cached_symbol!("last-prefix-arg"), current_prefix_arg, env);
        crate::lisp::primitives::safe_run_hooks_maybe_narrowed(
            interp,
            &cached_symbol!("post-command-hook").symbol(),
            env,
        )?;
        // If displaying a message, resize the echo area window to fit
        // that message's size exactly.
        if crate::lisp::primitives::dispatch::display::echo_area_message_value().is_some() {
            resize_echo_area_exactly(interp, env);
        }
        // If there are warnings queued up, issue them.
        if interp
            .lookup_var_key(cached_symbol!("delayed-warnings-list"), env)
            .is_some_and(|value| value.is_truthy())
        {
            crate::lisp::primitives::safe_run_hooks(
                interp,
                &cached_symbol!("delayed-warnings-hook").symbol(),
                env,
            )?;
        }
        let this_command = interp
            .lookup_var_key(cached_symbol!("this-command"), env)
            .unwrap_or(Value::Nil);
        let real_this_command = interp
            .lookup_var_key(cached_symbol!("real-this-command"), env)
            .unwrap_or(Value::Nil);
        interp.set_variable_key(cached_symbol!("last-command"), this_command, env);
        interp.set_variable_key(cached_symbol!("real-last-command"), real_this_command, env);
        let last_command_event = interp
            .lookup_var_key(cached_symbol!("last-command-event"), env)
            .unwrap_or(Value::Nil);
        if !last_command_event.is_cons() {
            interp.set_variable_key(
                cached_symbol!("last-repeatable-command"),
                real_this_command,
                env,
            );
        }
        clear_command_keys(interp);
        echo_after_command(interp, env)?;
        if interp.buffer.borrow().mark_active() && run_hooks_bound(interp) {
            update_mark_after_command(interp, env, prev_buffer, prev_modiff)?;
        }
        // finalize: point out of an intangible place the command left it
        // in, then the keys of a macro being defined.
        finalize_command(interp, env, prev_buffer, prev_modiff, last_point_position)?;
    }
}

/// command_loop_1's `set_buffer_internal (XBUFFER (XWINDOW
/// (selected_window)->contents))'.  A deleted selected window's nil
/// contents have no XBUFFER (GNU's checking build aborts there); the
/// current buffer stays.
pub(crate) fn select_window_buffer(interp: &mut Interpreter) -> Result<(), LispError> {
    if let Some(selected_buffer) = interp.selected_window_contents_buffer_id()
        && selected_buffer != interp.current_buffer_id()
        && interp.has_buffer_id(selected_buffer)
    {
        interp.set_current_buffer_id(selected_buffer)?;
    }
    Ok(())
}

/// keyboard.c's ++num_input_keys after each key sequence read.
fn increment_num_input_keys(interp: &mut Interpreter, env: &mut Env) {
    let count = interp
        .lookup_var_key(cached_symbol!("num-input-keys"), env)
        .and_then(|value| value.integer_value())
        .unwrap_or(0);
    interp.set_variable_key(
        cached_symbol!("num-input-keys"),
        Value::Integer(count.saturating_add(1)),
        env,
    );
}

/// `!NILP (Vrun_hooks)': the hooks can run.
fn run_hooks_bound(interp: &Interpreter) -> bool {
    // eval.c's Vrun_hooks, the symbol `run-hooks' until shut_down_emacs
    // clears it (not a Lisp variable of that name).
    !interp.hooks_cleared
}

/// command_loop_1 with the mark active after a command: the obsolete
/// one-command `transient-mark-mode' (`only', then `identity') steps on,
/// and `deactivate-mark' set deactivates the mark; otherwise a command
/// that changed the buffer, or another made current, runs
/// `activate-mark-hook'.  The region's text goes to no window system's
/// selection on a text terminal.
fn update_mark_after_command(
    interp: &mut Interpreter,
    env: &mut Env,
    prev_buffer: u64,
    prev_modiff: crate::buffer::ModCount,
) -> Result<(), LispError> {
    let transient = interp
        .lookup_var_key(cached_symbol!("transient-mark-mode"), env)
        .unwrap_or(Value::Nil);
    if crate::lisp::types::eq_symbol_named(transient, "identity") {
        interp.set_variable_key(cached_symbol!("transient-mark-mode"), Value::Nil, env);
    } else if crate::lisp::types::eq_symbol_named(transient, "only") {
        interp.set_variable_key(
            cached_symbol!("transient-mark-mode"),
            Value::symbol("identity"),
            env,
        );
    }
    let deactivate = interp
        .lookup_var_key(cached_symbol!("deactivate-mark"), env)
        .is_some_and(|value| value.is_truthy());
    if deactivate {
        // With `select-active-regions', `deactivate-mark' also sets the
        // PRIMARY selection.
        interp.call_function_value(
            Value::symbol("deactivate-mark"),
            Some("deactivate-mark"),
            &[],
            env,
        )?;
    } else {
        set_primary_from_region(interp, env)?;
        if interp.current_buffer_id() != prev_buffer
            || interp.buffer.borrow().modified_tick() != prev_modiff
        {
            crate::lisp::primitives::run_hook(
                interp,
                &cached_symbol!("activate-mark-hook").symbol(),
                env,
            )?;
        }
    }
    interp.set_variable_key(cached_symbol!("saved-region-selection"), Value::Nil, env);
    Ok(())
}

/// command_loop_1 with the mark active and kept: PRIMARY set from the
/// region under `select-active-regions' (`only' for a temporary
/// transient mark), on a window system's frame or a terminal that can set
/// the selection (`tty-select-active-regions' with xterm.el's
/// `xterm--set-selection'), unless the command is one of
/// `selection-inhibit-update-commands'; `post-select-region-hook' sees
/// the text.
fn set_primary_from_region(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let truthy = |interp: &Interpreter, env: &Env, name: &str| {
        interp
            .lookup_var(name, env)
            .is_some_and(|value| value.is_truthy())
    };
    let window_system = super::call(interp, "window-system", &[Value::Nil], env)?;
    let terminal_can_select = truthy(interp, env, "tty-select-active-regions")
        && super::call(
            interp,
            "terminal-parameter",
            &[Value::Nil, Value::symbol("xterm--set-selection")],
            env,
        )?
        .is_truthy();
    if window_system.is_nil() && !terminal_can_select {
        return Ok(());
    }
    // Even if mark_active is non-nil, the actual buffer marker may not
    // have been set yet (Bug#7044).
    let mark = super::call(interp, "mark-marker", &[], env)?;
    if super::call(interp, "marker-buffer", &[mark], env)?.is_nil() {
        return Ok(());
    }
    let select = interp
        .lookup_var("select-active-regions", env)
        .unwrap_or(Value::Nil);
    let transient = interp
        .lookup_var_key(cached_symbol!("transient-mark-mode"), env)
        .unwrap_or(Value::Nil);
    let wanted = if crate::lisp::types::eq_symbol_named(select, "only") {
        transient
            .cons_values()
            .is_some_and(|(head, _)| crate::lisp::types::eq_symbol_named(head, "only"))
    } else {
        select.is_truthy() && transient.is_truthy()
    };
    let this_command = interp
        .lookup_var_key(cached_symbol!("this-command"), env)
        .unwrap_or(Value::Nil);
    let inhibit = interp
        .lookup_var("selection-inhibit-update-commands", env)
        .unwrap_or(Value::Nil);
    if !wanted || super::call(interp, "memq", &[this_command, inhibit], env)?.is_truthy() {
        return Ok(());
    }
    let extract = interp
        .lookup_var("region-extract-function", env)
        .unwrap_or(Value::Nil);
    let txt = interp.call_function_value(extract, None, &[Value::Nil], env)?;
    let roots = [txt];
    interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
        // Don't set empty selections.
        if super::call(interp, "length", &[txt], env)?
            .integer_value()
            .unwrap_or(0)
            > 0
        {
            interp.call_function_value(
                Value::symbol("gui-set-selection"),
                Some("gui-set-selection"),
                &[Value::symbol("PRIMARY"), txt],
                env,
            )?;
        }
        super::call(
            interp,
            "run-hook-with-args",
            &[Value::symbol("post-select-region-hook"), txt],
            env,
        )
        .map(|_| ())
    })
}

/// command_loop_1's finalize: with point moved in the selected window's
/// buffer and point adjustment not disabled, point leaves the inside of
/// a composition, a display string's text or invisible text
/// (adjust_point_for_property); then a keyboard macro being defined,
/// with no prefix argument pending, takes the command's keys
/// (finalize_kbd_macro_chars).
fn finalize_command(
    interp: &mut Interpreter,
    env: &mut Env,
    prev_buffer: u64,
    prev_modiff: crate::buffer::ModCount,
    last_point_position: usize,
) -> Result<(), LispError> {
    if interp.current_buffer_id() == prev_buffer
        && interp.selected_window_contents_buffer_id() == Some(prev_buffer)
        && last_point_position != interp.buffer.borrow().point()
    {
        let (begv, zv) = interp.buffer.borrow().accessible_bounds();
        if !interp
            .lookup_var_key(cached_symbol!("disable-point-adjustment"), env)
            .is_some_and(|value| value.is_truthy())
            && !interp
                .lookup_var_key(cached_symbol!("global-disable-point-adjustment"), env)
                .is_some_and(|value| value.is_truthy())
            && !interp
                .lookup_var("composition-break-at-point", env)
                .is_some_and(|value| value.is_truthy())
        {
            // The last point was set within a grapheme cluster to prevent
            // automatic composition; the display updated recovers it.
            if last_point_position > begv
                && last_point_position < zv
                && crate::lisp::primitives::dispatch::composition_adjust_point(
                    interp,
                    env,
                    last_point_position,
                    last_point_position,
                )? != last_point_position
            {
                interp.windows_or_buffers_changed = 21;
            }
            let modified = interp.buffer.borrow().modified_tick() != prev_modiff;
            adjust_point_for_property(interp, env, last_point_position, modified)?;
        } else {
            // Point within a grapheme cluster: the display updated
            // de-composes the cluster on the screen and places the cursor.
            let point = interp.buffer.borrow().point();
            if point > begv
                && point < zv
                && crate::lisp::primitives::dispatch::composition_adjust_point(
                    interp,
                    env,
                    last_point_position,
                    point,
                )? != point
            {
                interp.windows_or_buffers_changed = 39;
            }
        }
    }
    let defining = interp
        .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
        .is_some_and(|value| value.is_truthy());
    let prefix_arg = interp
        .lookup_var_key(cached_symbol!("prefix-arg"), env)
        .unwrap_or(Value::Nil);
    if defining && prefix_arg.is_nil() {
        interp.kbd_macro_committed_len = interp.kbd_macro_definition.len();
    }
    Ok(())
}

/// keyboard.c:adjust_point_for_property: point, moved from LAST_PT, out
/// of what is to be treated as intangible: the inside of a composition
/// (not checked after a buffer change, so that inserting or deleting
/// around point keeps working), of text a `display' property replaces,
/// and of invisible text, cycling until none moves it.
fn adjust_point_for_property(
    interp: &mut Interpreter,
    env: &mut Env,
    last_pt: usize,
    modified: bool,
) -> Result<(), LispError> {
    let point = |interp: &Interpreter| interp.buffer.borrow().point();
    let bounds = |interp: &Interpreter| {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max())
    };
    let set_pt = |interp: &mut Interpreter, env: &mut Env, pos: usize| {
        super::call(interp, "goto-char", &[Value::Integer(pos as i64)], env).map(|_| ())
    };
    let display = Value::symbol("display");
    let invisible = Value::symbol("invisible");
    let mut check_composition = !modified;
    let mut check_display = true;
    let mut check_invisible = true;
    let mut orig_pt = Some(point(interp));
    while check_composition || check_display || check_invisible {
        let (begv, zv) = bounds(interp);
        let pt = point(interp);
        if check_composition && pt > begv && pt < zv {
            let beg = crate::lisp::primitives::dispatch::composition_adjust_point(
                interp, env, last_pt, pt,
            )?;
            if beg != pt {
                set_pt(interp, env, beg)?;
                check_display = true;
                check_invisible = true;
            }
        }
        check_composition = false;
        let (begv, zv) = bounds(interp);
        let pt = point(interp);
        if check_display && pt > begv && pt < zv {
            let window = interp.selected_window_value();
            let found = super::call(
                interp,
                "get-char-property-and-overlay",
                &[Value::Integer(pt as i64), display, window],
                env,
            )?;
            let (val, overlay) = found.cons_values().unwrap_or((Value::Nil, Value::Nil));
            let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![val, overlay]);
            if !roots[0].is_nil()
                && crate::lisp::primitives::dispatch::display::display_prop_intangible_p(
                    interp, env, roots[0], pt,
                )?
            {
                let range = if roots[1].is_nil() {
                    crate::lisp::primitives::textprop::get_property_and_range(
                        interp,
                        pt,
                        crate::lisp::types::PropertyName::new(display),
                        crate::lisp::primitives::textprop::SearchObject::Buffer(
                            interp.current_buffer_id(),
                        ),
                    )?
                    .map(|(value, beg, end)| {
                        roots[0] = value;
                        (beg, end)
                    })
                } else {
                    let start = super::call(interp, "overlay-start", &[roots[1]], env)?;
                    let end = super::call(interp, "overlay-end", &[roots[1]], env)?;
                    match (start.integer_value(), end.integer_value()) {
                        (Some(start), Some(end)) => Some((start as usize, end as usize)),
                        _ => None,
                    }
                };
                let empty_string = string_like(&roots[0]).is_some_and(|text| text.text.is_empty());
                if let Some((beg, end)) = range
                    && (beg < pt || (beg <= pt && empty_string))
                {
                    let target = if pt < last_pt {
                        if empty_string {
                            beg.saturating_sub(1).max(begv)
                        } else {
                            beg
                        }
                    } else {
                        end
                    };
                    set_pt(interp, env, target)?;
                    check_composition = true;
                    check_invisible = true;
                }
            }
            roots.clear();
        }
        check_display = false;
        let (begv, zv) = bounds(interp);
        let pt = point(interp);
        if check_invisible && pt > begv && pt < zv {
            let spec = interp
                .lookup_var_key(cached_symbol!("buffer-invisibility-spec"), env)
                .unwrap_or(Value::Nil);
            let mut ellipsis = false;
            let mut beg = pt;
            let mut end = pt;
            // The invisible run's own ends, the overlays' strings counting
            // as an ellipsis.
            let invisible_at = |interp: &mut Interpreter,
                                env: &mut Env,
                                pos: usize,
                                ellipsis: &mut bool|
             -> Result<bool, LispError> {
                let found = super::call(
                    interp,
                    "get-char-property-and-overlay",
                    &[Value::Integer(pos as i64), invisible, Value::Nil],
                    env,
                )?;
                let (val, overlay) = found.cons_values().unwrap_or((Value::Nil, Value::Nil));
                if val.is_nil() {
                    return Ok(false);
                }
                let inv =
                    crate::lisp::primitives::invisibility::text_prop_means_invisible(val, spec);
                if inv == 0 {
                    return Ok(false);
                }
                *ellipsis = *ellipsis || inv > 1;
                if !overlay.is_nil() && !*ellipsis {
                    let after = super::call(
                        interp,
                        "overlay-get",
                        &[overlay, Value::symbol("after-string")],
                        env,
                    )?;
                    let before = super::call(
                        interp,
                        "overlay-get",
                        &[overlay, Value::symbol("before-string")],
                        env,
                    )?;
                    *ellipsis = !after.is_nil() || !before.is_nil();
                }
                Ok(true)
            };
            while end < zv && invisible_at(interp, env, end, &mut ellipsis)? {
                let next = super::call(
                    interp,
                    "next-single-char-property-change",
                    &[
                        Value::Integer(end as i64),
                        invisible,
                        Value::Nil,
                        Value::Nil,
                    ],
                    env,
                )?;
                end = next.integer_value().map_or(zv, |next| next as usize);
            }
            while beg > begv && invisible_at(interp, env, beg - 1, &mut ellipsis)? {
                let previous = super::call(
                    interp,
                    "previous-single-char-property-change",
                    &[
                        Value::Integer(beg as i64),
                        invisible,
                        Value::Nil,
                        Value::Nil,
                    ],
                    env,
                )?;
                beg = previous
                    .integer_value()
                    .map_or(begv, |previous| previous as usize);
            }
            // Move away from the inside area.
            let pt = point(interp);
            if beg < pt && end > pt {
                let target = if orig_pt == Some(pt) && (last_pt < beg || last_pt > end) {
                    // We haven't moved yet and we were outside the range
                    // before: pretend we moved less than we actually did.
                    orig_pt = None;
                    if pt < last_pt { end } else { beg }
                } else if pt < last_pt {
                    beg
                } else {
                    end
                };
                set_pt(interp, env, target)?;
                check_composition = true;
                check_display = true;
            }
            // Pretend the area doesn't exist if the buffer is not modified.
            let pt = point(interp);
            if !modified && !ellipsis && beg < end {
                let invisible_pos = |interp: &mut Interpreter,
                                     env: &mut Env,
                                     pos: usize|
                 -> Result<bool, LispError> {
                    let value = super::call(
                        interp,
                        "get-pos-property",
                        &[Value::Integer(pos as i64), invisible, Value::Nil],
                        env,
                    )?;
                    Ok(
                        crate::lisp::primitives::invisibility::text_prop_means_invisible(
                            value, spec,
                        ) != 0,
                    )
                };
                if last_pt == beg && pt == end && end < zv {
                    check_composition = true;
                    check_display = true;
                    set_pt(interp, env, end + 1)?;
                } else if last_pt == end && pt == beg && beg > begv {
                    check_composition = true;
                    check_display = true;
                    set_pt(interp, env, beg - 1)?;
                } else if pt == if pt < last_pt { beg } else { end } {
                    // We've already moved as far as we can.
                } else if invisible_pos(interp, env, pt)?
                    && !invisible_pos(interp, env, if pt == beg { end } else { beg })?
                {
                    check_composition = true;
                    check_display = true;
                    set_pt(interp, env, if pt == beg { end } else { beg })?;
                }
            }
        }
        check_invisible = false;
    }
    Ok(())
}
