use super::*;
use crate::lisp::types::EnvFrame;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

mod file;
mod search;
pub(crate) use file::load_file;

#[cfg(test)]
mod tests;

pub(crate) fn autoload_parts(value: &Value) -> Option<(String, Value, Value)> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name == "autoload") {
        return None;
    }
    let file = string_like(items.get(1)?)
        .map(|string| string.text)
        .filter(|text| !text.is_empty())?;
    let interactive = items.get(3).cloned().unwrap_or(Value::Nil);
    let kind = items.get(4).cloned().unwrap_or(Value::Nil);
    Some((file, interactive, kind))
}

pub(crate) fn autoload_is_macro(interp: &Interpreter, symbol: Option<&str>, value: &Value) -> bool {
    autoload_parts(value).is_some_and(|(_, _, kind)| {
        matches!(kind.kind(), Kind::T)
            || matches!(kind.kind(), Kind::Symbol(name) if name == "t" || name == "macro")
    }) || symbol.is_some_and(|name| {
        interp
            .get_symbol_property(name, "autoload-macro")
            .is_some_and(|value| !value.is_nil())
    })
}

pub(crate) fn autoload_command_p(value: &Value) -> bool {
    autoload_parts(value).is_some_and(|(_, interactive, kind)| {
        interactive.is_truthy() || matches!(kind.kind(), Kind::Symbol(symbol) if symbol == "keymap")
    })
}

// The arguments an interactive spec produced, with the command-history
// entry a string spec's call records once the reading bindings are gone.
pub(crate) struct InteractiveArgs {
    pub(crate) values: Vec<Value>,
    pub(crate) history: Option<Value>,
    pub(crate) record_then_fail: bool,
}

// callint.c's quotify_arg: what the history entry evaluates back to the
// same argument.
fn quotify_arg(value: Value) -> Value {
    if value.is_cons() || (value.is_symbol() && !value.is_nil() && value != Value::T) {
        Value::list([Value::Symbol("quote".into()), value])
    } else {
        value
    }
}

// callint.c's fix_command: a symbol command's `interactive-args'
// replacements, then its trailing nil optional arguments dropped.
fn fix_command(
    interp: &mut Interpreter,
    function: &Value,
    values: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(name) = function.symbol_str() else {
        return Ok(());
    };
    if !values.is_cons() {
        return Ok(());
    }
    let reps = interp
        .get_symbol_property(name, "interactive-args")
        .unwrap_or(Value::Nil);
    if reps.is_cons() {
        let mut vals = values;
        let mut i = 0;
        while !vals.is_nil() {
            let rep = call(interp, "assq", &[Value::Integer(i), reps], env)?;
            if !rep.is_nil() {
                vals.set_car(rep.cdr()?)?;
            }
            vals = vals.cdr()?;
            i += 1;
        }
    }
    let arity = call(interp, "func-arity", std::slice::from_ref(function), env)?;
    if let (Kind::Integer(min), Kind::Integer(_)) = (arity.car()?.kind(), arity.cdr()?.kind()) {
        let mut last = Value::Nil;
        let mut last_index = 0;
        let mut tail = values;
        let mut i = 0;
        while let Some((head, rest)) = tail.cons_values() {
            if !head.is_nil() {
                last = tail;
                last_index = i;
            }
            tail = rest;
            i += 1;
        }
        if last_index > 0 && last_index >= min - 1 {
            last.set_cdr(Value::Nil)?;
        }
    }
    Ok(())
}

// callint.c's `add-to-history' call: subr.el keeps `command-history'
// to `history-length', its duplicates kept.
fn add_command_history(
    interp: &mut Interpreter,
    entry: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.call_function_value(
        Value::Symbol("add-to-history".into()),
        Some("add-to-history"),
        &[
            Value::Symbol("command-history".into()),
            entry,
            Value::Nil,
            Value::T,
        ],
        env,
    )
}

pub(crate) fn collect_interactive_args(
    interp: &mut Interpreter,
    function: &Value,
    func: &Value,
    spec: Value,
    record_flag: bool,
    env: &mut Env,
) -> Result<InteractiveArgs, LispError> {
    if let Kind::StringObject(state) = spec.kind() {
        let text = state.borrow().text();
        let parsed = parse_interactive_string(function, &text, record_flag, interp, env)?;
        // Recorded when an argument came from the terminal, or on request:
        // point, mark and the region as the calls that find them again.
        let history = (record_flag || parsed.varies.contains(&0)).then(|| {
            let mut entry = Vec::with_capacity(parsed.values.len() + 1);
            entry.push(*function);
            for (value, vary) in parsed.values.iter().zip(&parsed.varies) {
                entry.push(match vary {
                    1 => Value::list([Value::Symbol("point".into())]),
                    2 => Value::list([Value::Symbol("mark".into())]),
                    3 => Value::list([Value::Symbol("region-beginning".into())]),
                    4 => Value::list([Value::Symbol("region-end".into())]),
                    _ => quotify_arg(*value),
                });
            }
            Value::list(entry)
        });
        return Ok(InteractiveArgs {
            values: parsed.values,
            history,
            record_then_fail: parsed.record_then_fail,
        });
    }
    // A Lisp form's values are recorded when computing them read input,
    // or on request -- before the call, which may still refuse them.
    let events = interp.keyboard_input.num_input_events;
    let specs = eval_callable_metadata_form(interp, func, &spec, env)?;
    if events != interp.keyboard_input.num_input_events || record_flag {
        let values = call(interp, "copy-sequence", &[specs], env)?;
        let mut tail = values;
        while let Some((head, rest)) = tail.cons_values() {
            tail.set_car(quotify_arg(head))?;
            tail = rest;
        }
        fix_command(interp, function, values, env)?;
        add_command_history(interp, Value::cons(*function, values), env)?;
    }
    Ok(InteractiveArgs {
        values: specs.to_vec()?,
        history: None,
        record_then_fail: false,
    })
}

pub(crate) fn call_interactively_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs("call-interactively".into(), 0));
    }
    // callint.c requires an interactive form, including forms supplied by
    // the Lisp generic for closures and advice; `interactive-form' loads
    // an autoloaded command.
    let form = call(
        interp,
        "interactive-form",
        std::slice::from_ref(&args[0]),
        env,
    )?;
    if !matches!(form.kind(), Kind::Cons(_)) {
        return Err(wrong_type_argument("commandp", args[0]));
    }
    let mut func = resolve_callable(interp, &args[0], env)?;
    if autoload_parts(&func).is_some() {
        func = interp.autoload_do_load(func, args[0], Value::Nil, env)?;
    }
    let spec = form.cdr()?.car()?;
    // callint.c saves the command identities that reading the arguments
    // through the minibuffer clobbers, restoring them once the arguments
    // are read, and binds `current-minibuffer-command' so that minibuffer
    // hooks can see which command asked: until the arguments are read for
    // a string spec, through the call for a Lisp form's.
    let saved_command_state = [
        "this-command",
        "this-original-command",
        "real-this-command",
        "last-command",
    ]
    .map(|name| (name, interp.lookup_var(name, env).unwrap_or(Value::Nil)));
    let mut minibuffer_command = Some(interp.bind_special_variable(
        "current-minibuffer-command",
        saved_command_state[0].1,
        env,
    )?);
    let result = call_interactively_bound(
        interp,
        args,
        func,
        spec,
        &saved_command_state,
        &mut minibuffer_command,
        env,
    );
    if let Some(restore) = minibuffer_command {
        interp.restore_special_binding(restore, env)?;
    }
    result
}

fn call_interactively_bound(
    interp: &mut Interpreter,
    args: &[Value],
    func: Value,
    spec: Value,
    saved_command_state: &[(&str, Value)],
    minibuffer_command: &mut Option<crate::lisp::eval::SpecialBindingRestore>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let string_spec = spec.is_string();
    let record_flag = args.get(1).is_some_and(Value::is_truthy);
    // callint.c: a non-nil KEYS is the key sequence the spec codes (`e',
    // `k'...) read instead of the current command's keys -- how
    // `command-execute' hands a special event to its `special-event-map'
    // binding.
    let keys_binding = match args.get(2).filter(|keys| !keys.is_nil()) {
        Some(keys) => Some(interp.bind_special_variable("this-command-keys-vector", *keys, env)?),
        None => None,
    };
    // A command whose `enable-recursive-minibuffers' property is set reads
    // its string spec's arguments with recursive minibuffers enabled.
    let recursive_binding = match args[0].symbol_str() {
        Some(symbol)
            if string_spec
                && interp
                    .get_symbol_property(symbol, "enable-recursive-minibuffers")
                    .is_some_and(|value| value.is_truthy()) =>
        {
            Some(interp.bind_special_variable("enable-recursive-minibuffers", Value::T, env)?)
        }
        _ => None,
    };
    let interactive_args =
        collect_interactive_args(interp, &args[0], &func, spec, record_flag, env);
    if let Some(restore) = recursive_binding {
        interp.restore_special_binding(restore, env)?;
    }
    if let Some(restore) = keys_binding {
        interp.restore_special_binding(restore, env)?;
    }
    if string_spec && let Some(restore) = minibuffer_command.take() {
        interp.restore_special_binding(restore, env)?;
    }
    let InteractiveArgs {
        values: interactive_args,
        history,
        record_then_fail,
    } = interactive_args?;
    if let Some(entry) = history {
        add_command_history(interp, entry, env)?;
    }
    if record_then_fail {
        call(interp, "barf-if-buffer-read-only", &[], env)?;
    }
    for (name, value) in saved_command_state {
        interp.set_variable(name, *value, env);
    }
    // A string spec's command runs with `command-debug-status' bound to
    // nil.
    let debug_status = if string_spec {
        Some(interp.bind_special_variable("command-debug-status", Value::Nil, env)?)
    } else {
        None
    };
    interp.push_interactive_call();
    // The interactive dispatch frame is what `called-interactively-p's
    // backtrace walk stops at (GNU stops at funcall-interactively); the
    // native dispatch paths (special form, command loop) don't otherwise
    // leave one.
    interp.push_backtrace_frame(
        Value::Symbol("funcall-interactively".into()),
        &interactive_args,
    );
    // Call through the SYMBOL when one was given, as GNU's
    // Ffuncall_interactively does: the advised command's own frame is
    // then recorded between funcall-interactively and the advice
    // machinery — the exact shape nadvice's called-interactively-p
    // skip function walks.
    let call_target = if args[0].as_symbol().is_ok() {
        args[0]
    } else {
        func
    };
    let result = interp.call_function_value(call_target, None, &interactive_args, env);
    interp.pop_backtrace_frame();
    interp.pop_interactive_call();
    if let Some(restore) = debug_status {
        interp.restore_special_binding(restore, env)?;
    }
    result
}

pub(crate) fn eval_impl(
    interp: &mut Interpreter,
    args: &[Value],
    caller_env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs("eval".into(), args.len()));
    }
    if let Some(lexical) = args.get(1) {
        // Feval: `specbind (Qinternal_interpreter_environment, CONSP
        // (lexical) || NILP (lexical) ? lexical : list_of_t)' -- an alist
        // is the environment itself, its conses the bindings' storage.
        let _ = caller_env;
        let (capture_lexical, mut eval_env) = match lexical.kind() {
            Kind::Nil => (false, crate::lisp::types::Env::new()),
            Kind::Cons(_) => (
                true,
                crate::lisp::types::Env::from_vec(vec![EnvFrame::from_alist(*lexical)]),
            ),
            _ => (
                true,
                crate::lisp::types::Env::from_vec(vec![EnvFrame::lexical()]),
            ),
        };
        interp.push_lambda_eval_context(capture_lexical);
        let installed = interp
            .install_frame_environment(crate::lisp::types::current_environment_value(&eval_env));
        let result = interp.eval(&args[0], &mut eval_env);
        interp.uninstall_frame_environments(installed);
        interp.pop_lambda_capture_override();
        result
    } else {
        // GNU (eval FORM) without LEXICAL evaluates with a nil lexical
        // environment: every variable reference is dynamic (solar/diary
        // run `mapconcat #'eval' over display forms bound by dlet).  Mark the
        // directly evaluated forms as dynamic; lexical function call
        // boundaries mask this context so their internal lambdas and lets
        // retain the function's definition-time semantics.
        interp.push_lambda_eval_context(false);
        let result = interp.eval(&args[0], &mut crate::lisp::types::Env::new());
        interp.pop_lambda_capture_override();
        result
    }
}

pub(crate) fn eval_buffer_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() > 5 {
        return Err(LispError::WrongNumberOfArgs(
            "eval-buffer".into(),
            args.len(),
        ));
    }
    // lread.c:Feval_buffer: Fget_buffer, then its own two errors.
    let buffer_id = if let Some(buffer) = args.first().filter(|value| !value.is_nil()) {
        let buffer = interp
            .get_buffer(buffer)?
            .ok_or_else(|| LispError::Signal("No such buffer".into()))?;
        interp
            .live_buffer_id(buffer)
            .ok_or_else(|| LispError::Signal("Reading from killed buffer".into()))?
    } else {
        interp.current_buffer_id()
    };
    // Feval_buffer uses FILENAME unless nil; the source buffer need not
    // visit that file. readevalloop accepts nil, otherwise CHECK_STRING.
    let source_file = args
        .get(2)
        .filter(|filename| !filename.is_nil())
        .cloned()
        .unwrap_or_else(|| {
            interp
                .get_buffer_by_id(buffer_id)
                .map_or(Value::Nil, |buffer| {
                    buffer.file.as_deref().map_or(Value::Nil, Value::string)
                })
        });
    if !source_file.is_nil() {
        string_text(&source_file)?;
    }
    // GNU freezes this decision at readevalloop entry. A later definition
    // of the owner does not retroactively enable eager expansion.
    let eager_macroexpand = super::call(
        interp,
        "fboundp",
        &[Value::symbol("internal-macroexpand-for-load")],
        env,
    )?
    .is_truthy()
        && !string_like(&source_file).is_some_and(|file| file.text.ends_with(".elc"));
    // lread.c:Feval_buffer binds `eval-buffer-list' and `standard-output'
    // (`symbolp' unless values are printed or printing is allowed), saves
    // the current buffer's point, and reads the buffer from BEGV.
    let readcharfun = interp.buffer_value(buffer_id).expect("live buffer object");
    let printflag = args.get(1).copied().unwrap_or(Value::Nil);
    let output = if printflag.is_nil() && args.get(4).is_none_or(Value::is_nil) {
        Value::symbol("symbolp")
    } else {
        printflag
    };
    let eval_buffer_list = interp
        .lookup_var("eval-buffer-list", env)
        .unwrap_or(Value::Nil);
    let previous_eval_buffer_list = interp.bind_special_variable(
        "eval-buffer-list",
        Value::cons(readcharfun, eval_buffer_list),
        env,
    )?;
    let previous_output = interp.bind_special_variable("standard-output", output, env)?;
    let excursion = interp.save_excursion_state();
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let begv = buffer.point_min();
        buffer.goto_char(begv);
    }
    // Feval_buffer specbinds the Lisp variable `lexical-binding' to the
    // buffer's file-variable cookie (lisp_file_lexical_cookie) for the
    // whole readevalloop, so macros that consult the variable while the
    // buffer's forms expand (`named-let' signals without it) see the
    // buffer's own setting, not the caller's.  The fresh interpreter
    // environment below decides how the forms are evaluated; this is the
    // variable the forms themselves can read.
    let cookie_lexical = interp
        .get_buffer_by_id(buffer_id)
        .map(|buffer| buffer.buffer_string())
        .and_then(|text| lisp_file_lexical_cookie(&text))
        .unwrap_or(false);
    let previous_lexical_binding = interp.bind_special_variable(
        "lexical-binding",
        if cookie_lexical { Value::T } else { Value::Nil },
        env,
    )?;
    // readevalloop reads from `standard-input', bound to the buffer, binds
    // `macroexp--dynvars' to its value, so that the forms' declarations go
    // with the loop, and then `current-load-list' (loadhist_initialize).
    let previous_input = interp.bind_special_variable("standard-input", readcharfun, env)?;
    let dynvars = interp
        .lookup_var("macroexp--dynvars", env)
        .unwrap_or(Value::Nil);
    let previous_dynvars = interp.bind_special_variable("macroexp--dynvars", dynvars, env)?;
    let previous_load_list =
        interp.bind_special_variable("current-load-list", Value::list([source_file]), env)?;
    // Feval_buffer returns nil whatever the last form evaluated to.
    let mut result = eval_buffer_forms(
        interp,
        buffer_id,
        eager_macroexpand,
        !printflag.is_nil(),
        env,
    )
    .map(|_| Value::Nil);
    if result.is_ok() {
        let current = interp
            .lookup_var("current-load-list", env)
            .unwrap_or(Value::Nil);
        interp.commit_entire_load_history(&source_file, current);
    }
    for restore in [
        previous_load_list,
        previous_dynvars,
        previous_input,
        previous_lexical_binding,
    ] {
        if let Err(error) = interp.restore_special_binding(restore, env)
            && result.is_ok()
        {
            result = Err(error);
        }
    }
    interp.restore_excursion_state(excursion);
    for restore in [previous_output, previous_eval_buffer_list] {
        if let Err(error) = interp.restore_special_binding(restore, env)
            && result.is_ok()
        {
            result = Err(error);
        }
    }
    result
}

pub(crate) fn eval_region_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "eval-region".into(),
            args.len(),
        ));
    }
    let start = position_from_value(interp, &args[0])?;
    let end = position_from_value(interp, &args[1])?;
    if start > end {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("args-out-of-range".into()),
            args[0],
            args[1],
        ])));
    }
    let print_flag = args.get(2).cloned().unwrap_or(Value::Nil);
    // lread.c readevalloop: READ-FUNCTION reads each form, else
    // `load-read-function'; only the built-in `read' takes the native
    // reading path.
    let explicit_reader = args.get(3).filter(|value| !value.is_nil()).copied();
    let read_function = explicit_reader.or_else(|| {
        interp
            .lookup_var("load-read-function", env)
            .filter(|value| !matches!(value.kind(), Kind::Symbol(symbol) if symbol == "read"))
    });
    let buffer_id = interp.current_buffer_id();
    let source_file = interp.buffer.borrow().file.clone();
    let readcharfun = interp.buffer_value(buffer_id).expect("live buffer object");

    // lread.c dynamically binds these around `readevalloop'.  In
    // particular, the file load context lets macros expanded by eval-defun
    // resolve resources relative to the buffer's defining file.
    let mut restores = Vec::new();
    let standard_output = if print_flag.is_nil() {
        Value::Symbol("symbolp".into())
    } else {
        print_flag
    };
    restores.push(interp.bind_special_variable("standard-output", standard_output, env)?);
    let eval_buffer_list = interp
        .lookup_var("eval-buffer-list", env)
        .unwrap_or(Value::Nil);
    restores.push(interp.bind_special_variable(
        "eval-buffer-list",
        Value::cons(readcharfun, eval_buffer_list),
        env,
    )?);
    // readevalloop reads from `standard-input', bound to the buffer, binds
    // `macroexp--dynvars' to its value and `current-load-list' to a list
    // of the buffer's file name (loadhist_initialize), nil if it has none.
    restores.push(interp.bind_special_variable("standard-input", readcharfun, env)?);
    let dynvars = interp
        .lookup_var("macroexp--dynvars", env)
        .unwrap_or(Value::Nil);
    restores.push(interp.bind_special_variable("macroexp--dynvars", dynvars, env)?);
    restores.push(interp.bind_special_variable(
        "current-load-list",
        Value::list([source_file.map_or(Value::Nil, |file| Value::String(file.into()))]),
        env,
    )?);

    // readevalloop reads the buffer-local `lexical-binding' and runs the
    // forms in a fresh interpreter environment — the caller's lexical
    // frames never leak into the evaluated region's top level.
    let lexical = interp
        .lookup_var("lexical-binding", env)
        .is_some_and(|value| value.is_truthy());
    let print = !print_flag.is_nil();
    let mut result = with_fresh_eval_environment(interp, lexical, |interp, eval_env| {
        if let Some(read_function) = read_function {
            return eval_region_via_read_function(
                interp,
                buffer_id,
                (start, end),
                &read_function,
                explicit_reader.is_some(),
                print,
                eval_env,
            );
        }
        read_eval_buffer_forms(
            interp,
            buffer_id,
            Some((start, end)),
            eval_env,
            |interp, form, eval_env| {
                let result = eager_expand_eval(interp, &form, eval_env)?;
                if print {
                    readevalloop_print(interp, result, eval_env)?;
                }
                Ok(result)
            },
        )
    });

    for restore in restores.into_iter().rev() {
        if let Err(error) = interp.restore_special_binding(restore, env)
            && result.is_ok()
        {
            result = Err(error);
        }
    }
    result.map(|_| Value::Nil)
}

/// readevalloop's printing of a form's VALUE: onto `values', then to
/// `standard-output', prin1'd when that is t and printed otherwise.
fn readevalloop_print(
    interp: &mut Interpreter,
    value: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let values = interp.lookup_var("values", env).unwrap_or(Value::Nil);
    interp.set_variable("values", Value::cons(value, values), env);
    let printer = match interp.lookup_var("standard-output", env).map(|v| v.kind()) {
        Some(Kind::T) => "prin1",
        _ => "print",
    };
    crate::lisp::primitives::call(interp, printer, &[value], env)?;
    Ok(())
}

/// A marker at POSITION in the buffer BUFFER_ID, as Fpoint_marker makes.
fn readevalloop_marker(
    interp: &mut Interpreter,
    buffer_id: u64,
    position: usize,
) -> crate::lisp::types::MarkerRef {
    let Kind::Marker(id) = interp.make_marker().kind() else {
        unreachable!("make_marker returns a marker");
    };
    interp
        .set_marker(id, Some(position), Some(buffer_id))
        .expect("a fresh marker accepts a live buffer");
    id
}

/// readevalloop's skip before each form, from POS to END of the buffer:
/// whitespace (space, tab, newline, formfeed, return and no-break space)
/// and `;' comments.  Where the next form starts, or END.
fn skip_readevalloop_separators(
    interp: &Interpreter,
    buffer_id: u64,
    mut pos: usize,
    end: usize,
) -> usize {
    let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
        return end;
    };
    while pos < end {
        match buffer.char_at(pos) {
            Some(';') => {
                while pos < end && buffer.char_at(pos) != Some('\n') {
                    pos += 1;
                }
            }
            Some(' ' | '\t' | '\n' | '\u{c}' | '\r' | '\u{a0}') => pos += 1,
            _ => break,
        }
    }
    pos
}

/// readevalloop over the current buffer from START to END with
/// READ-FUNCTION (EXPLICIT) or `load-read-function': each form is read
/// with the buffer current, narrowed to END and with point at the next
/// form, and the excursion is restored before it is evaluated.  A
/// READ-FUNCTION that leaves point at the end stops the reading after
/// its form.
fn eval_region_via_read_function(
    interp: &mut Interpreter,
    buffer_id: u64,
    (start, end): (usize, usize),
    read_function: &Value,
    explicit: bool,
    print: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let start = readevalloop_marker(interp, buffer_id, start);
    let end = readevalloop_marker(interp, buffer_id, end);
    let mut result = Ok(Value::Nil);
    loop {
        let read = read_region_form(
            interp,
            buffer_id,
            (start, end),
            read_function,
            explicit,
            env,
        );
        let (form, stop) = match read {
            Ok(Some(read)) => read,
            Ok(None) => break,
            Err(error) => {
                result = Err(error);
                break;
            }
        };
        let value = match eager_expand_eval(interp, &form, env) {
            Ok(value) => value,
            Err(error) => {
                result = Err(error);
                break;
            }
        };
        if print && let Err(error) = readevalloop_print(interp, value, env) {
            result = Err(error);
            break;
        }
        result = Ok(value);
        if stop {
            break;
        }
    }
    let _ = interp.set_marker(start, None, None);
    let _ = interp.set_marker(end, None, None);
    result
}

/// One form of eval-region's loop with a Lisp reader: the excursion,
/// the buffer made current, point at START narrowed to END, the
/// separators skipped and READ-FUNCTION called, START then where it
/// stopped; None at the end.  The second value says to stop reading.
fn read_region_form(
    interp: &mut Interpreter,
    buffer_id: u64,
    (start, end): (crate::lisp::types::MarkerRef, crate::lisp::types::MarkerRef),
    read_function: &Value,
    explicit: bool,
    env: &mut Env,
) -> Result<Option<(Value, bool)>, LispError> {
    if !interp.has_buffer_id(buffer_id) {
        return Err(LispError::Signal("Reading from killed buffer".into()));
    }
    let outer = interp.save_excursion_state();
    if let Err(error) = interp.set_current_buffer_id(buffer_id) {
        interp.restore_excursion_state(outer);
        return Err(error);
    }
    let inner = interp.save_excursion_state();
    let restriction = interp.save_restriction_state();
    interp.restore_labeled_restrictions(buffer_id, Vec::new());
    let read = (|| {
        let stream = interp.buffer_value(buffer_id).expect("live buffer object");
        let (begv, zv) = {
            let buffer = interp.buffer.borrow();
            (buffer.point_min(), buffer.point_max())
        };
        let from = interp
            .marker_position(start)
            .unwrap_or(begv)
            .clamp(begv, zv);
        interp.buffer.borrow_mut().goto_char(from);
        let limit = interp.marker_position(end).unwrap_or(zv);
        crate::lisp::primitives::call(
            interp,
            "narrow-to-region",
            &[Value::Integer(begv as i64), Value::Integer(limit as i64)],
            env,
        )?;
        let (point, zv) = {
            let buffer = interp.buffer.borrow();
            (buffer.point(), buffer.point_max())
        };
        let position = skip_readevalloop_separators(interp, buffer_id, point, zv);
        interp.buffer.borrow_mut().goto_char(position);
        if position >= zv {
            return Ok(None);
        }
        let form = interp.call_function_value(*read_function, None, &[stream], env)?;
        let (point, zv) = {
            let buffer = interp.buffer.borrow();
            (buffer.point(), buffer.point_max())
        };
        let stop = explicit && point == zv;
        if !stop {
            let _ = interp.set_marker(start, Some(point), Some(buffer_id));
        }
        Ok(Some((form, stop)))
    })();
    interp.restore_restriction_state(restriction);
    interp.restore_excursion_state(inner);
    interp.restore_excursion_state(outer);
    read
}

/// lread.c lisp_file_lexical_cookie: the `lexical-binding' file variable
/// of a Lisp source, read the way readevalloop's callers read it.  Only
/// the first line counts (the second when the first is a `#!' line), and
/// only when that line starts with `;': the `-*- ... -*-' fields are
/// scanned for `lexical-binding', whose value is lexical unless it is
/// `nil'.  A cookie-looking string on a later line is just text.
pub(crate) fn lisp_file_lexical_cookie(text: &str) -> Option<bool> {
    let mut lines = text.split('\n');
    let mut line = lines.next()?;
    if line.starts_with("#!") {
        line = lines.next()?;
    }
    if !line.starts_with(';') {
        return None;
    }
    let start = line.find("-*-")? + 3;
    let rest = &line[start..];
    let end = rest.find("-*-").unwrap_or(rest.len());
    for field in rest[..end].split(';') {
        let Some((name, value)) = field.split_once(':') else {
            continue;
        };
        if name.trim() == "lexical-binding" {
            return Some(value.trim() != "nil");
        }
    }
    None
}

fn eval_buffer_forms(
    interp: &mut Interpreter,
    buffer_id: u64,
    eager_macroexpand: bool,
    print: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let load_read = interp
        .lookup_var("load-read-function", env)
        .unwrap_or_else(|| Value::Symbol("read".into()));
    let text = interp
        .get_buffer_by_id(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
        .buffer_string();
    // Feval_buffer binds `lexical-binding' from the buffer's own file
    // cookie, and readevalloop then evaluates every form in a FRESH
    // interpreter environment (nil, or the empty lexical `(t)').  The
    // caller's lexical frames must never leak into a buffer's top level:
    // a cookie-less buffer's defuns are dynamic even when `eval-buffer'
    // is called from inside a lexical closure (testcover's
    // instrumentation runner is exactly that caller).
    let lexical = lisp_file_lexical_cookie(&text).unwrap_or(false);
    let evaluate = move |interp: &mut Interpreter, form: Value, eval_env: &mut Env| {
        let value = if eager_macroexpand {
            eager_expand_eval(interp, &form, eval_env)?
        } else {
            interp.eval(&form, eval_env)?
        };
        if print {
            readevalloop_print(interp, value, eval_env)?;
        }
        Ok(value)
    };
    with_fresh_eval_environment(interp, lexical, |interp, eval_env| {
        if matches!(load_read.kind(), Kind::Symbol(symbol) if symbol == "read") {
            read_eval_buffer_forms(interp, buffer_id, None, eval_env, evaluate)
        } else {
            // A customized reader (like `edebug--read') reads from the
            // buffer itself, form by form, moving point.
            eval_buffer_via_load_read_function(interp, buffer_id, &load_read, eval_env, evaluate)
        }
    })
}

/// The text a readevalloop's forms are read from, and what the buffer
/// was when it was taken: it serves while the buffer's characters, its
/// accessible region, the end of the reading and where the next form
/// starts are as the last read left them.
struct ReadevalloopSource {
    text: BufferReadSource,
    state: (crate::buffer::ModCount, bool, usize, usize),
    end: usize,
    offset: usize,
    position: usize,
}

/// What a buffer's text was for a readevalloop source.
fn readevalloop_text_state(
    interp: &Interpreter,
    buffer_id: u64,
) -> (crate::buffer::ModCount, bool, usize, usize) {
    interp
        .get_buffer_by_id(buffer_id)
        .map_or((0, false, 0, 0), |buffer| {
            (
                buffer.chars_modified_tick(),
                buffer.is_multibyte(),
                buffer.point_min(),
                buffer.size_total(),
            )
        })
}

/// readevalloop with the built-in `read' over a buffer, each form read
/// by `read' on it (its frame on the backtrace while the reader runs)
/// and handed to EVAL, the last value returned.  For eval-buffer (REGION
/// nil) a form is read at the buffer's point, which the reader leaves
/// after it and the forms may move; for eval-region from START, a marker
/// the reader's end becomes, to END, a marker too, the buffer's point
/// untouched.  The reader takes the buffer's characters as lread.c's
/// readchar does; an `invalid-read-syntax' carries the line and column
/// where reading stopped.
fn read_eval_buffer_forms(
    interp: &mut Interpreter,
    buffer_id: u64,
    region: Option<(usize, usize)>,
    env: &mut Env,
    mut eval: impl FnMut(&mut Interpreter, Value, &mut Env) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    let markers = region.map(|(start, end)| {
        (
            readevalloop_marker(interp, buffer_id, start),
            readevalloop_marker(interp, buffer_id, end),
        )
    });
    let result = read_eval_buffer_loop(interp, buffer_id, markers, env, &mut eval);
    if let Some((start, end)) = markers {
        let _ = interp.set_marker(start, None, None);
        let _ = interp.set_marker(end, None, None);
    }
    result
}

fn read_eval_buffer_loop(
    interp: &mut Interpreter,
    buffer_id: u64,
    markers: Option<(crate::lisp::types::MarkerRef, crate::lisp::types::MarkerRef)>,
    env: &mut Env,
    eval: &mut impl FnMut(&mut Interpreter, Value, &mut Env) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    let readcharfun = interp.buffer_value(buffer_id).expect("live buffer object");
    let mut source: Option<ReadevalloopSource> = None;
    let mut result = Value::Nil;
    loop {
        if !interp.has_buffer_id(buffer_id) {
            return Err(LispError::Signal("Reading from killed buffer".into()));
        }
        let (begv, point, zv) = {
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .expect("a live buffer is registered");
            (buffer.point_min(), buffer.point(), buffer.point_max())
        };
        // eval-region's Fgoto_char (start) and Fnarrow_to_region (BEGV,
        // end) keep both in the accessible text.
        let (position, end) = match markers {
            None => (point, zv),
            Some((start, end)) => {
                let end = interp.marker_position(end).unwrap_or(zv).clamp(begv, zv);
                let start = interp
                    .marker_position(start)
                    .unwrap_or(begv)
                    .clamp(begv, end);
                (start, end)
            }
        };
        let state = readevalloop_text_state(interp, buffer_id);
        let current = source.as_ref().is_some_and(|source| {
            source.state == state && source.position == position && source.end == end
        });
        if !current {
            source = Some(ReadevalloopSource {
                text: BufferReadSource::new(interp, buffer_id, position, end)?,
                state,
                end,
                offset: 0,
                position,
            });
        }
        let source = source.as_mut().expect("a source was just taken");
        interp.push_backtrace_frame(Value::symbol("read"), &[readcharfun]);
        let before = source.offset;
        let read = read_next_eval_form(interp, &source.text, &mut source.offset, env);
        let reached = source.position + source.text.characters_between(before, source.offset);
        source.position = reached;
        let form = match read {
            Ok(form) => form,
            Err(error) => {
                let error = reader_end_of_file(interp, error, env);
                // The reader moved point to where it stopped; eval-region's
                // excursion then restores it.
                let error = if matches!(error.kind(), LispErrorKind::ReadError(_)) {
                    let saved = interp
                        .get_buffer_by_id(buffer_id)
                        .map(|buffer| buffer.point());
                    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                        buffer.goto_char(reached.min(end));
                    }
                    let error = invalid_syntax_in_buffer(interp, buffer_id, error, env);
                    if markers.is_some()
                        && let Some(saved) = saved
                        && let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id)
                    {
                        buffer.goto_char(saved);
                    }
                    error
                } else {
                    error
                };
                let settled = interp.settle_frame_error(error, env);
                interp.pop_backtrace_frame();
                return settled;
            }
        };
        interp.pop_backtrace_frame();
        match markers {
            // readchar moved the buffer's point past what it read.
            None => {
                if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                    buffer.goto_char(reached);
                }
            }
            Some((start, _)) => {
                if form.is_some() {
                    let _ = interp.set_marker(start, Some(reached), Some(buffer_id));
                    source.position = reached;
                }
            }
        }
        let Some(form) = form else {
            return Ok(result);
        };
        result = eval(interp, form, env)?;
    }
}

/// lread.c:readevalloop reads one form in the currently selected obarray,
/// then evaluates it before reading the next. Parsing the whole file in the
/// standard namespace first can leak private names and misses obarray changes
/// made by earlier forms. The text slice shares the same input allocation.
/// Read the next form of SOURCE from byte OFFSET, advancing OFFSET past
/// it; on an error OFFSET is where the reader stopped.
fn read_next_eval_form(
    interp: &mut Interpreter,
    source: &BufferReadSource,
    offset: &mut usize,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let (value, consumed, needs_materialization, unescaped) = {
        let mut context = crate::lisp::eval::LispReadContext::current(interp, env);
        let reader = match source.input_from(*offset) {
            LispReaderInput::Encoded(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded(bytes, multibyte, shorthands)
            }
            LispReaderInput::Buffer(parts, multibyte) => {
                crate::lisp::reader::Reader::from_buffer_parts(parts, multibyte, shorthands)
            }
            LispReaderInput::BufferBytes(bytes, multibyte) => {
                crate::lisp::reader::Reader::from_encoded(bytes, true, shorthands)
                    .reporting_multibyte(multibyte)
            }
        };
        let mut reader = reader.with_read_context(&mut context);
        let value = match reader.read() {
            Ok(value) => value,
            Err(error) => {
                *offset += reader.position();
                return Err(error);
            }
        };
        (
            value,
            reader.position(),
            reader.emitted_reader_forms(),
            reader.unescaped_character_literals().collect::<Vec<_>>(),
        )
    };
    *offset += consumed;
    if !unescaped.is_empty() {
        record_unescaped_character_literals(interp, unescaped, env);
    }
    value
        .map(|value| {
            if needs_materialization {
                interp.materialize_read_object_literals(value, env)
            } else {
                Ok(value)
            }
        })
        .transpose()
}

/// readevalloop's `internal-interpreter-environment' specbind: run BODY
/// with a fresh top-level environment — the empty lexical `(t)' frame
/// when LEXICAL, a plain dynamic scope otherwise.
fn with_fresh_eval_environment<T>(
    interp: &mut Interpreter,
    lexical: bool,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> T,
) -> T {
    let mut eval_env = if lexical {
        crate::lisp::types::Env::from_vec(vec![EnvFrame::lexical()])
    } else {
        crate::lisp::types::Env::new()
    };
    interp.push_lambda_eval_context(lexical);
    let result = body(interp, &mut eval_env);
    interp.pop_lambda_capture_override();
    result
}

// GNU readevalloop eagerly macroexpands each top-level form read from
// source (`internal-macroexpand-for-load'), so macros in function bodies
// expand while `current-load-list' still names the file being evaluated.
// Expansion failures fall back to the unexpanded form like GNU.
pub(crate) fn eager_expand_eval(
    interp: &mut Interpreter,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // GNU does not suppress load-history attachment while a top-level
    // `eval-when-compile' body runs during a source load: a `require'
    // evaluated there still records under the loading file.
    eager_expand_eval_inner(interp, form, env)
}

fn eager_expand_eval_inner(
    interp: &mut Interpreter,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let expanded = call_internal_macroexpand_for_load(interp, form, Value::Nil, env)?;
    if let Ok(items) = expanded.to_vec()
        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head == "progn")
    {
        // A top-level progn is expanded form by form so a macro defined by
        // one subform is live while expanding the rest.
        // Expansion can return a fresh list whose only remaining references
        // are this heap-allocated snapshot. GNU keeps its pending forms on the
        // scanned C stack; register ours while earlier forms can collect.
        return interp.with_lisp_stack_roots(&items, |interp| {
            let mut result = Value::Nil;
            for subform in &items[1..] {
                result = eager_expand_eval(interp, subform, env)?;
            }
            Ok(result)
        });
    }
    let full = call_internal_macroexpand_for_load(interp, &expanded, Value::T, env)?;
    interp.eval(&full, env)
}

fn call_internal_macroexpand_for_load(
    interp: &mut Interpreter,
    form: &Value,
    full: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let owner = interp.lookup_function("internal-macroexpand-for-load", env)?;
    if std::env::var_os("EMAXX_DEBUG_EAGER_MACROEXPAND").is_some() {
        let head = match form.kind() {
            Kind::Cons(cell) => format!("{}", cell.car.get().clone()),
            other => format!("{other}"),
        };
        let second = if let Kind::Cons(cell) = form.kind() {
            if let Kind::Cons(inner) = cell.cdr.get().kind() {
                format!(" {}", inner.car.get().clone())
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        eprintln!("EAGER-EXPAND: ({head}{second} ...)");
    }
    interp.call_function_value(
        owner,
        Some("internal-macroexpand-for-load"),
        &[*form, full],
        env,
    )
}

fn eval_buffer_via_load_read_function(
    interp: &mut Interpreter,
    buffer_id: u64,
    load_read: &Value,
    env: &mut Env,
    mut eval: impl FnMut(&mut Interpreter, Value, &mut Env) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    // readevalloop with `load-read-function' over a buffer it does not
    // make current: each form read at the buffer's point, past the
    // separators, and evaluated.  Deliberately NO `intern_symbols_in_value'
    // here: GNU interns nothing beyond what that reader interned, so a
    // deliberately `unintern'-ed symbol it returns stays dead.
    let stream = interp.buffer_value(buffer_id).expect("live buffer object");
    let mut result = Value::Nil;
    loop {
        if !interp.has_buffer_id(buffer_id) {
            return Err(LispError::Signal("Reading from killed buffer".into()));
        }
        let (point, zv) = {
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .expect("a live buffer is registered");
            (buffer.point(), buffer.point_max())
        };
        let position = skip_readevalloop_separators(interp, buffer_id, point, zv);
        if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
            buffer.goto_char(position);
        }
        if position >= zv {
            return Ok(result);
        }
        let form =
            interp.call_function_value(*load_read, None, std::slice::from_ref(&stream), env)?;
        result = eval(interp, form, env)?;
    }
}

pub(crate) fn resolve_load_target_in_env(
    interp: &mut Interpreter,
    target: &str,
    env: &Env,
) -> Result<Option<PathBuf>, LispError> {
    Ok(resolve_load_file_in_env(interp, target, env, false, false)?.0)
}

/// lread.c:Fload's suffix policy. The shared openp search never invents
/// filename aliases, process-cwd probes, or a private source/bytecode order.
pub(crate) fn resolve_load_file_in_env(
    interp: &mut Interpreter,
    target: &str,
    env: &Env,
    nosuffix: bool,
    must_suffix: bool,
) -> Result<(Option<PathBuf>, i32), LispError> {
    let (found, errno) = find_load_file(interp, target, env, nosuffix, must_suffix)?;
    let path = found
        .map(|found| string_text(&found.into_name()).map(PathBuf::from))
        .transpose()?;
    Ok((path, errno))
}

fn find_load_file(
    interp: &mut Interpreter,
    target: &str,
    env: &Env,
    nosuffix: bool,
    mut must_suffix: bool,
) -> Result<(Option<search::SearchMatch>, i32), LispError> {
    if target.is_empty() {
        return Ok((None, libc::ENOENT));
    }
    let mut load_env = env.clone();
    if must_suffix {
        let has_suffix = [".el", ".elc", ".eln"]
            .iter()
            .any(|suffix| target.ends_with(suffix))
            || crate::lisp::eval::module_suffix_values()
                .iter()
                .any(|suffix| {
                    string_like(suffix).is_some_and(|suffix| target.ends_with(&suffix.text))
                });
        if has_suffix
            || super::call(
                interp,
                "file-name-directory",
                &[Value::string(target)],
                &mut load_env,
            )?
            .is_truthy()
        {
            must_suffix = false;
        }
    }
    let suffixes = if nosuffix {
        Value::Nil
    } else {
        let suffixes = get_load_suffixes_value(interp, &mut load_env)?;
        if must_suffix {
            suffixes
        } else {
            let reps = interp
                .forwarded_c_value("load-file-rep-suffixes", env)
                .unwrap_or(Value::Nil);
            super::call(interp, "append", &[suffixes, reps], &mut load_env)?
        }
    };
    let path = interp
        .forwarded_c_value("load-path", env)
        .unwrap_or(Value::Nil);
    let newer = interp
        .forwarded_c_value("load-prefer-newer", env)
        .is_some_and(|value| value.is_truthy());
    search::openp_search(
        interp,
        &Value::string(target),
        &path,
        &suffixes,
        &Value::Nil,
        newer,
        &mut load_env,
    )
}

/// GNU lread.c:maybe_swap_for_eln.  A bare `load' first resolves its normal
/// `.elc' target, then substitutes a matching, at-least-as-new `.eln' from
/// `native-comp-eln-load-path'.  Explicit `.elc' loads deliberately bypass
/// this substitution.
pub(crate) fn maybe_swap_for_native(
    interp: &mut Interpreter,
    requested: &str,
    resolved: &Path,
    env: &Env,
) -> Result<PathBuf, LispError> {
    if requested.ends_with(".elc")
        || resolved
            .extension()
            .is_none_or(|extension| extension != "elc")
        || interp
            .lookup_var("load-no-native", env)
            .is_some_and(|value| value.is_truthy())
    {
        return Ok(resolved.to_path_buf());
    }

    // The names here are Lisp file names' text; each reaches the host
    // ENCODE_FILE'd, as lread.c's maybe_swap_for_eln passes them.
    let host = |path: &Path| host_file_name(interp, env, &path.to_string_lossy());
    let mut source = resolved.to_path_buf();
    source.set_extension("el");
    if !fs::is_regular_file(host(&source)?) {
        let compressed = PathBuf::from(format!("{}.gz", source.display()));
        if !fs::is_regular_file(host(&compressed)?) {
            return Ok(resolved.to_path_buf());
        }
        source = compressed;
    }

    let mut filename_env = env.clone();
    let relative = super::dispatch::comp_el_to_eln_rel_filename(
        interp,
        &Value::String(source.display().to_string().into()),
        &mut filename_env,
    )?;
    let version = interp
        .lookup_var("comp-native-version-dir", env)
        .and_then(|value| string_like(&value).map(|string| string.text));
    let Some(version) = version else {
        return Ok(resolved.to_path_buf());
    };
    let load_paths = interp
        .lookup_var("native-comp-eln-load-path", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let invocation_directory = interp
        .lookup_var("invocation-directory", env)
        .and_then(|value| string_like(&value).map(|string| PathBuf::from(string.text)));
    let resolved_mtime = fs::metadata(host_file_name(interp, env, &resolved.to_string_lossy())?)
        .and_then(|metadata| metadata.modified())
        .ok();

    let mut system_directory = None;
    for entry in load_paths {
        let Some(text) = string_like(&entry).map(|string| string.text) else {
            continue;
        };
        let directory = PathBuf::from(text);
        let directory = if directory.is_absolute() {
            directory
        } else if let Some(invocation_directory) = &invocation_directory {
            invocation_directory.join(directory)
        } else {
            directory
        };
        system_directory = Some(directory.clone());
        let candidate = directory.join(&version).join(&relative);
        let host = host_file_name(interp, env, &candidate.to_string_lossy())?;
        if native_candidate_is_current(&host, resolved_mtime) {
            record_native_source(interp, &candidate, &source, env)?;
            return Ok(candidate);
        }
    }

    // GNU also searches the `preloaded' subdirectory of the final (system)
    // native directory after all ordinary cache directories.
    if let Some(directory) = system_directory {
        let candidate = directory.join(version).join("preloaded").join(relative);
        let host = host_file_name(interp, env, &candidate.to_string_lossy())?;
        if native_candidate_is_current(&host, resolved_mtime) {
            record_native_source(interp, &candidate, &source, env)?;
            return Ok(candidate);
        }
    }

    Ok(resolved.to_path_buf())
}

fn native_candidate_is_current(
    candidate: &Path,
    resolved_mtime: Option<std::time::SystemTime>,
) -> bool {
    let Ok(metadata) = fs::metadata(candidate) else {
        return false;
    };
    if metadata.is_dir() {
        return false;
    }
    match (metadata.modified().ok(), resolved_mtime) {
        (Some(native), Some(resolved)) => native >= resolved,
        _ => true,
    }
}

fn record_native_source(
    interp: &mut Interpreter,
    native: &Path,
    source: &Path,
    env: &Env,
) -> Result<(), LispError> {
    let Some(table) = interp.lookup_var("comp-eln-to-el-h", env) else {
        return Ok(());
    };
    let Some(basename) = native.file_name() else {
        return Ok(());
    };
    let mut put_env = env.clone();
    super::call(
        interp,
        "puthash",
        &[
            Value::String(basename.to_string_lossy().into_owned().into()),
            Value::String(source.display().to_string().into()),
            table,
        ],
        &mut put_env,
    )?;
    Ok(())
}

/// lread.c read_char_literal conses every unescaped `?)'-style literal
/// onto `lread--unescaped-character-literals', which `load' binds to nil
/// around a file and warns about as it unwinds
/// (load_warn_unescaped_character_literals).  The eval-buffer path
/// `load-with-code-conversion' takes reads with its own reader, so its
/// literals are added here.
pub(crate) fn record_unescaped_character_literals(
    interp: &mut Interpreter,
    literals: impl IntoIterator<Item = i64>,
    env: &mut Env,
) {
    let mut items = interp
        .lookup_var("lread--unescaped-character-literals", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let mut changed = false;
    for literal in literals {
        let literal = Value::Integer(literal);
        if !items.contains(&literal) {
            items.insert(0, literal);
            changed = true;
        }
    }
    if changed {
        interp.set_variable(
            "lread--unescaped-character-literals",
            Value::list(items),
            env,
        );
    }
}

pub(crate) fn read_symbol_shorthands_in_env(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<(String, String)>, LispError> {
    let Some(raw) = interp.lookup_var("read-symbol-shorthands", env) else {
        return Ok(Vec::new());
    };
    let mut shorthands = Vec::new();
    for entry in raw.to_vec()? {
        let Some((from, to)) = entry.cons_values() else {
            continue;
        };
        let Some(from) = string_like(&from).map(|string| string.text) else {
            continue;
        };
        let Some(to) = string_like(&to).map(|string| string.text) else {
            continue;
        };
        shorthands.push((from, to));
    }
    Ok(shorthands)
}

pub(crate) fn apply_symbol_shorthands_in_env(
    interp: &Interpreter,
    symbol_name: &str,
    env: &Env,
) -> Result<String, LispError> {
    for (short, long) in read_symbol_shorthands_in_env(interp, env)? {
        if let Some(rest) = symbol_name.strip_prefix(&short) {
            return Ok(format!("{long}{rest}"));
        }
    }
    Ok(symbol_name.to_string())
}

pub(crate) fn get_load_suffixes_value(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    // Fget_load_suffixes reads the C slots, including after makunbound
    // detaches their public symbols. concat2 accepts sequences, not just strings.
    let mut suffixes = search::SearchTail::new(
        interp
            .forwarded_c_value("load-suffixes", env)
            .unwrap_or(Value::Nil),
    );
    let mut values = Vec::new();
    while let Some((suffix, _)) = suffixes.current.cons_values() {
        let mut reps = search::SearchTail::new(
            interp
                .forwarded_c_value("load-file-rep-suffixes", env)
                .unwrap_or(Value::Nil),
        );
        while let Some((rep, _)) = reps.current.cons_values() {
            values.push(super::call(interp, "concat", &[suffix, rep], env)?);
            reps.advance(interp, env, false)?;
        }
        suffixes.advance(interp, env, false)?;
    }
    Ok(Value::list(values))
}

pub(crate) fn locate_file_internal(
    interp: &mut Interpreter,
    file: &Value,
    path: &Value,
    suffixes: &Value,
    predicate: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    Ok(locate_file_search(interp, file, path, suffixes, predicate, env)?.0)
}

/// lread.c:Flocate_file_internal and process.c callers share openp's search.
/// Return the actual found name, never a rewritten source-provenance path.
pub(crate) fn locate_file_search(
    interp: &mut Interpreter,
    file: &Value,
    path: &Value,
    suffixes: &Value,
    predicate: &Value,
    env: &mut Env,
) -> Result<(Value, i32), LispError> {
    let (found, errno) = search::openp_search(interp, file, path, suffixes, predicate, false, env)?;
    Ok((
        found
            .map(search::SearchMatch::into_name)
            .unwrap_or(Value::Nil),
        errno,
    ))
}

/// openp's access check for a fixnum predicate: `faccessat' with the mask
/// and AT_EACCESS, then an accessible directory counts as EISDIR rather
/// than a match.  The error is the errno openp records.
pub(crate) fn locate_file_access_probe(mask: i64, candidate: &Path) -> Result<(), i32> {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(candidate.as_os_str().as_bytes()) else {
        return Err(libc::ENOENT);
    };
    let Ok(mode) = libc::c_int::try_from(mask) else {
        return Err(libc::EINVAL);
    };
    // SAFETY: `path' is a valid NUL-terminated string for the call's duration.
    let accessible =
        unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), mode, libc::AT_EACCESS) } == 0;
    if !accessible {
        return Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::ENOENT));
    }
    // fileio.c:file_directory_p's generic POSIX branch. A failed directory
    // probe is a match only for ENOENT/ENOTDIR, not every metadata error.
    let directory_error = match accessible_directory(candidate.as_os_str().as_bytes()) {
        Ok(()) => return Err(libc::EISDIR),
        Err(error) => error.raw_os_error().unwrap_or(libc::EIO),
    };
    let directory_error = if directory_error == libc::EACCES {
        match fs::metadata(candidate) {
            Ok(metadata) if metadata.is_dir() => return Err(libc::EISDIR),
            Ok(_) => libc::ENOTDIR,
            Err(error) if error.raw_os_error() == Some(libc::EOVERFLOW) => {
                return Err(libc::EISDIR);
            }
            Err(error) => error.raw_os_error().unwrap_or(libc::EIO),
        }
    } else {
        directory_error
    };
    if directory_error != libc::ENOENT && directory_error != libc::ENOTDIR {
        return Err(directory_error);
    }
    Ok(())
}
