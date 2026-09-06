use super::*;
use crate::lisp::types::EnvFrame;

mod file;
mod search;
pub(crate) use file::load_file;

#[cfg(test)]
mod tests;

pub(crate) fn autoload_parts(value: &Value) -> Option<(String, Value, Value)> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "autoload") {
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
        matches!(kind, Value::T)
            || matches!(&kind, Value::Symbol(name) if name == "t" || name == "macro")
    }) || symbol.is_some_and(|name| {
        interp
            .get_symbol_property(name, "autoload-macro")
            .is_some_and(|value| !value.is_nil())
    })
}

pub(crate) fn autoload_command_p(value: &Value) -> bool {
    autoload_parts(value).is_some_and(|(_, interactive, kind)| {
        interactive.is_truthy() || matches!(kind, Value::Symbol(symbol) if symbol == "keymap")
    })
}

pub(crate) fn resolve_callable_aliases(
    interp: &Interpreter,
    func: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let mut current = func.clone();
    let mut seen = HashSet::new();
    while let Value::Symbol(name) = current.clone() {
        if !seen.insert(name.as_str().to_owned()) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("cyclic-function-indirection".into()),
                Value::Symbol(name),
            ])));
        }
        current = interp.lookup_function(&name, env)?;
    }
    Ok(current)
}

pub(crate) fn collect_interactive_args(
    interp: &mut Interpreter,
    func: &Value,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let func = resolve_callable_aliases(interp, func, env)?;
    // GNU's C interactive_form consults `oclosure-interactive-form' for
    // OClosures: nadvice's advice objects have no (interactive ...) in their
    // body and instead COMPOSE the advised function's spec.
    // Compiled OClosures (like nadvice's advice objects) are closure
    // records, not interpreted lambdas, so ask the real `oclosure-type'
    // owner when the native lambda-shape probe misses.
    let is_oclosure = crate::lisp::primitives::dispatch::oclosure_type_of(&func).is_some()
        || (matches!(&func, Value::Record(_) | Value::Lambda(_))
            && interp.has_lisp_function("oclosure-type")
            && interp
                .call_function_value(
                    Value::Symbol("oclosure-type".into()),
                    Some("oclosure-type"),
                    std::slice::from_ref(&func),
                    env,
                )
                .map(|value| value.is_truthy())
                .unwrap_or(false));
    let oclosure_spec = if is_oclosure
        && interp.has_lisp_function("oclosure-interactive-form")
        && interactive_spec_form(interp, &func).is_none()
    {
        interp
            .call_function_value(
                Value::Symbol("oclosure-interactive-form".into()),
                Some("oclosure-interactive-form"),
                std::slice::from_ref(&func),
                env,
            )?
            .to_vec()
            .ok()
            .and_then(|items| items.get(1).cloned())
    } else {
        None
    };
    let Some(spec) = oclosure_spec.or_else(|| interactive_spec_form(interp, &func)) else {
        return Ok(Vec::new());
    };
    match spec {
        Value::String(spec) => parse_interactive_string(&spec, interp, env),
        Value::StringObject(state) => parse_interactive_string(&state.borrow().text, interp, env),
        _ => {
            if let Some(items) = interactive_list_form_items(&spec) {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(eval_callable_metadata_form(interp, &func, &item, env)?);
                }
                Ok(values)
            } else {
                eval_callable_metadata_form(interp, &func, &spec, env)?.to_vec()
            }
        }
    }
}

pub(crate) fn call_interactively_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs("call-interactively".into(), 0));
    }
    let mut func = resolve_callable(interp, &args[0], env)?;
    if let (Some(symbol), Some((file, _, _))) = (args[0].as_symbol().ok(), autoload_parts(&func)) {
        interp.load_autoload_target(&file, env)?;
        func = interp.lookup_function(symbol, env)?;
    }
    // callint.c: a non-nil KEYS is the key sequence the spec codes (`e',
    // `k'...) read instead of the current command's keys -- how
    // `command-execute' hands a special event to its `special-event-map'
    // binding.
    let keys_binding = match args.get(2).filter(|keys| !keys.is_nil()) {
        Some(keys) => {
            Some(interp.bind_special_variable("this-command-keys-vector", keys.clone(), env)?)
        }
        None => None,
    };
    let interactive_args = collect_interactive_args(interp, &func, env);
    if let Some(restore) = keys_binding {
        interp.restore_special_binding(restore, env)?;
    }
    let interactive_args = interactive_args?;
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
        args[0].clone()
    } else {
        func.clone()
    };
    let result = interp.call_function_value(call_target, None, &interactive_args, env);
    interp.pop_backtrace_frame();
    interp.pop_interactive_call();
    let result = result?;
    if args.get(1).is_some_and(Value::is_truthy)
        && let Some(function_name) = callable_name(&args[0], &func)
    {
        let history_args = history_args_for_call(interp, &args[0], &interactive_args);
        record_command_history(interp, &function_name, history_args, env);
    }
    Ok(result)
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
        let (capture_lexical, mut eval_env) = match lexical {
            Value::Nil => (false, Vec::new()),
            Value::T => (
                true,
                vec![EnvFrame::with_lisp_environment_and_identity(
                    Vec::new(),
                    Value::list([Value::T]),
                    Interpreter::fresh_frame_identity(),
                )],
            ),
            Value::Cons(_) => {
                let frame = lexical_alist_frame(interp, lexical, caller_env)?;
                (true, vec![frame.into()])
            }
            _ => (
                true,
                vec![EnvFrame::with_lisp_environment_and_identity(
                    Vec::new(),
                    Value::list([Value::T]),
                    Interpreter::fresh_frame_identity(),
                )],
            ),
        };
        interp.push_lambda_eval_context(capture_lexical);
        // A fresh `eval' is a fresh activation: closures it creates must not
        // share captured-environment cells with content-identical captures
        // from the caller's activation (bug#51695's interpreted lambda).
        let previous_activation = interp.enter_activation();
        let result = interp.eval(&args[0], &mut eval_env);
        interp.leave_activation(previous_activation);
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
        let previous_activation = interp.enter_activation();
        let result = interp.eval(&args[0], &mut Vec::new());
        interp.leave_activation(previous_activation);
        interp.pop_lambda_capture_override();
        result
    }
}

fn lexical_alist_frame(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Vec<(crate::lisp::types::SymbolName, Value)>, LispError> {
    let mut frame = Vec::new();
    for entry in value.to_vec()? {
        let Some((key, val)) = entry.cons_values() else {
            continue;
        };
        if let Ok(name) = checked_symbol_identity(interp, &key, env) {
            frame.push((name, val));
        }
    }
    Ok(frame)
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
    let buffer_id = if let Some(buffer) = args.first().filter(|value| !value.is_nil()) {
        interp.resolve_buffer_id(buffer)?
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
                .and_then(|buffer| buffer.file.as_ref())
                .map_or(Value::Nil, |file| Value::string(file))
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
    let previous_load_list = interp.bind_special_variable(
        "current-load-list",
        Value::list([source_file.clone()]),
        env,
    )?;
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
    // Feval_buffer returns nil whatever the last form evaluated to.
    let result = eval_buffer_forms(interp, buffer_id, eager_macroexpand, env).map(|_| Value::Nil);
    if result.is_ok() {
        let current = interp
            .lookup_var("current-load-list", env)
            .unwrap_or(Value::Nil);
        interp.commit_entire_load_history(&source_file, current);
    }
    interp.restore_special_binding(previous_lexical_binding, env)?;
    interp.restore_special_binding(previous_load_list, env)?;
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
            args[0].clone(),
            args[1].clone(),
        ])));
    }
    let print_flag = args.get(2).cloned().unwrap_or(Value::Nil);
    // lread.c readevalloop: a nil READ-FUNCTION means `load-read-function',
    // and only the built-in `read' takes the native reading path.
    let read_function = args
        .get(3)
        .filter(|value| !value.is_nil())
        .cloned()
        .or_else(|| {
            interp
                .lookup_var("load-read-function", env)
                .filter(|value| !matches!(value, Value::Symbol(symbol) if symbol == "read"))
        });
    let buffer_id = interp.current_buffer_id();
    let buffer_name = interp.buffer.name.clone();
    let source_file = interp.buffer.file.clone();
    let saved_point = interp.buffer.point();

    // lread.c dynamically binds these around `readevalloop'.  In
    // particular, the file load context lets macros expanded by eval-defun
    // resolve resources relative to the buffer's defining file.
    let mut restores = Vec::new();
    let standard_output = if print_flag.is_nil() {
        Value::Symbol("symbolp".into())
    } else {
        print_flag.clone()
    };
    restores.push(interp.bind_special_variable("standard-output", standard_output, env)?);
    let eval_buffer_list = interp
        .lookup_var("eval-buffer-list", env)
        .unwrap_or(Value::Nil);
    restores.push(interp.bind_special_variable(
        "eval-buffer-list",
        Value::cons(Value::buffer(buffer_id, buffer_name), eval_buffer_list),
        env,
    )?);
    if let Some(file) = source_file {
        restores.push(interp.bind_special_variable(
            "current-load-list",
            Value::list([Value::String(file.into())]),
            env,
        )?);
    }

    // readevalloop reads the buffer-local `lexical-binding' and runs the
    // forms in a fresh interpreter environment — the caller's lexical
    // frames never leak into the evaluated region's top level.
    let lexical = interp
        .lookup_var("lexical-binding", env)
        .is_some_and(|value| value.is_truthy());
    let mut result = with_fresh_eval_environment(interp, lexical, |interp, eval_env| {
        if let Some(read_function) = read_function {
            return eval_region_via_read_function(
                interp,
                buffer_id,
                start,
                end,
                &read_function,
                &print_flag,
                eval_env,
            );
        }
        let text = interp
            .buffer
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        // lread.c reads through `read-symbol-shorthands' (oblookup_
        // considering_shorthand); load-with-code-conversion binds it from
        // the file's local variables around the evaluation.
        let shorthands = read_symbol_shorthands_in_env(interp, env)?;
        let mut reader = crate::lisp::reader::Reader::with_symbol_shorthands(&text, shorthands);
        let forms = reader.read_all()?;
        record_unescaped_character_literals(interp, &reader, env);
        let mut result = Value::Nil;
        for form in forms {
            // lread.c reads through the dynamically active `obarray', so a
            // let-bound private obarray owns the parsed symbols; evaluation
            // still happens in the fresh readevalloop environment.
            let form = interp.intern_read_symbols_in_value(form, eval_env)?;
            result = eager_expand_eval(interp, &form, eval_env)?;
            if !print_flag.is_nil() {
                let _ = crate::lisp::primitives::call(
                    interp,
                    "print",
                    &[result.clone(), print_flag.clone()],
                    eval_env,
                )?;
            }
        }
        Ok(result)
    });

    if interp.current_buffer_id() == buffer_id {
        interp
            .buffer
            .goto_char(saved_point.min(interp.buffer.point_max()));
    }
    for restore in restores.into_iter().rev() {
        if let Err(error) = interp.restore_special_binding(restore, env)
            && result.is_ok()
        {
            result = Err(error);
        }
    }
    result.map(|_| Value::Nil)
}

fn eval_region_via_read_function(
    interp: &mut Interpreter,
    buffer_id: u64,
    start: usize,
    end: usize,
    read_function: &Value,
    print_flag: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.buffer.goto_char(start);
    let stream = Value::buffer(buffer_id, interp.buffer.name.clone());
    let mut result = Value::Nil;
    while interp.buffer.point() < end {
        let _ = crate::lisp::primitives::call(
            interp,
            "forward-comment",
            &[Value::Integer(i64::MAX / 2)],
            env,
        );
        if interp.buffer.point() >= end {
            break;
        }
        // readevalloop calls the Lisp reader without a handler: its
        // `end-of-file' propagates; the loop itself stops at END.
        let form = interp.call_function_value(
            read_function.clone(),
            None,
            std::slice::from_ref(&stream),
            env,
        )?;
        // No `intern_symbols_in_value' here: when reading is delegated to a
        // Lisp reader GNU interns nothing beyond what that reader interned,
        // so a deliberately `unintern'-ed symbol it returns stays dead.
        result = eager_expand_eval(interp, &form, env)?;
        if !print_flag.is_nil() {
            let _ = crate::lisp::primitives::call(
                interp,
                "print",
                &[result.clone(), print_flag.clone()],
                env,
            )?;
        }
    }
    Ok(result)
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
    if !matches!(&load_read, Value::Symbol(symbol) if symbol == "read") {
        // A customized reader (like `edebug--read') reads from the buffer
        // itself, form by form, moving point like `readevalloop' does.
        return with_fresh_eval_environment(interp, lexical, |interp, eval_env| {
            eval_buffer_via_load_read_function(
                interp,
                buffer_id,
                &load_read,
                eager_macroexpand,
                eval_env,
            )
        });
    }
    // lread.c reads through `read-symbol-shorthands' (oblookup_considering_
    // shorthand); load-with-code-conversion binds it from the file's local
    // variables around `eval-buffer', so `(defun f-test3 ...)' under
    // `(("f-" . "elisp--foo-"))' defines `elisp--foo-test3'.
    let shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let mut reader = crate::lisp::reader::Reader::with_symbol_shorthands(&text, shorthands);
    let forms = reader.read_all()?;
    record_unescaped_character_literals(interp, &reader, env);
    with_fresh_eval_environment(interp, lexical, |interp, eval_env| {
        let mut result = Value::Nil;
        for form in forms {
            // lread.c reads through the dynamically active `obarray', so a
            // let-bound private obarray owns the parsed symbols.
            let form = interp.intern_read_symbols_in_value(form, eval_env)?;
            // GNU constructs identity-bearing reader literals before the
            // form reaches eval-buffer's evaluator.  In particular, native
            // compilation contexts contain #s records, hash tables, and
            // circular #[...] constants; leaving ReaderForm markers here
            // makes the later compiler read fail with invalid-read-syntax.
            let form = interp.materialize_read_object_literals(form, eval_env)?;
            result = if eager_macroexpand {
                eager_expand_eval(interp, &form, eval_env)?
            } else {
                interp.eval(&form, eval_env)?
            };
        }
        Ok(result)
    })
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
        vec![EnvFrame::with_lisp_environment_and_identity(
            Vec::new(),
            Value::list([Value::T]),
            Interpreter::fresh_frame_identity(),
        )]
    } else {
        Vec::new()
    };
    interp.push_lambda_eval_context(lexical);
    let previous_activation = interp.enter_activation();
    let result = body(interp, &mut eval_env);
    interp.leave_activation(previous_activation);
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
        && matches!(items.first(), Some(Value::Symbol(head)) if head == "progn")
    {
        // A top-level progn is expanded form by form so a macro defined by
        // one subform is live while expanding the rest.
        let mut result = Value::Nil;
        for subform in &items[1..] {
            result = eager_expand_eval(interp, subform, env)?;
        }
        return Ok(result);
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
        let head = match form {
            Value::Cons(cell) => format!("{}", cell.car.borrow().clone()),
            other => format!("{other}"),
        };
        let second = if let Value::Cons(cell) = form {
            if let Value::Cons(inner) = &cell.cdr.borrow().clone() {
                format!(" {}", inner.car.borrow().clone())
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
        &[form.clone(), full],
        env,
    )
}

fn eval_buffer_via_load_read_function(
    interp: &mut Interpreter,
    buffer_id: u64,
    load_read: &Value,
    eager_macroexpand: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let previous_buffer = interp.current_buffer_id();
    interp.switch_to_buffer_id(buffer_id)?;
    let saved_point = interp.buffer.point();
    let minimum = interp.buffer.point_min();
    interp.buffer.goto_char(minimum);
    let stream = crate::lisp::primitives::call(interp, "current-buffer", &[], env)?;
    let mut result = Ok(Value::Nil);
    loop {
        let _ = crate::lisp::primitives::call(
            interp,
            "forward-comment",
            &[Value::Integer(i64::MAX / 2)],
            env,
        );
        if interp.buffer.point() >= interp.buffer.point_max() {
            break;
        }
        let form = match interp.call_function_value(
            load_read.clone(),
            None,
            std::slice::from_ref(&stream),
            env,
        ) {
            Ok(form) => form,
            Err(error) => {
                if error.condition_type() == "end-of-file" {
                    break;
                }
                result = Err(error);
                break;
            }
        };
        // Deliberately NO `intern_symbols_in_value' here.  When reading is
        // delegated to a Lisp `load-read-function', GNU interns nothing extra
        // -- the form's symbols are whatever that function produced.  Walking
        // it re-interns symbols GNU leaves alone: a reader returning a
        // deliberately `unintern'-ed symbol made Emaxx resurrect it while GNU
        // still answered nil.  An earlier revision added the walk here for
        // "symmetry" with `eval-region' below, which achieved symmetry by
        // copying that path's defect (finding 120) rather than fixing it.
        match if eager_macroexpand {
            eager_expand_eval(interp, &form, env)
        } else {
            interp.eval(&form, env)
        } {
            Ok(value) => result = Ok(value),
            Err(error) => {
                result = Err(error);
                break;
            }
        }
    }
    let target = saved_point.min(interp.buffer.point_max());
    interp.buffer.goto_char(target);
    interp.switch_to_buffer_id(previous_buffer)?;
    result
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
            || crate::lisp::eval::dynamic_library_suffix_values()
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

    let mut source = resolved.to_path_buf();
    source.set_extension("el");
    if !source.is_file() {
        let compressed = PathBuf::from(format!("{}.gz", source.display()));
        if !compressed.is_file() {
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
    let resolved_mtime = fs::metadata(resolved)
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
        if native_candidate_is_current(&candidate, resolved_mtime) {
            record_native_source(interp, &candidate, &source, env)?;
            return Ok(candidate);
        }
    }

    // GNU also searches the `preloaded' subdirectory of the final (system)
    // native directory after all ordinary cache directories.
    if let Some(directory) = system_directory {
        let candidate = directory.join(version).join("preloaded").join(relative);
        if native_candidate_is_current(&candidate, resolved_mtime) {
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
fn record_unescaped_character_literals(
    interp: &mut Interpreter,
    reader: &crate::lisp::reader::Reader<'_>,
    env: &mut Env,
) {
    let mut items = interp
        .lookup_var("lread--unescaped-character-literals", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let mut changed = false;
    for literal in reader.unescaped_character_literals() {
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
            values.push(super::call(interp, "concat", &[suffix.clone(), rep], env)?);
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
pub(crate) fn locate_file_access_probe(mask: i64, candidate: &str) -> Result<(), i32> {
    let Ok(path) = std::ffi::CString::new(candidate) else {
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
    let directory_error = match accessible_directory(candidate.as_bytes()) {
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

pub(crate) fn history_args_for_call(
    interp: &Interpreter,
    command: &Value,
    actual_args: &[Value],
) -> Vec<Value> {
    let mut recorded = actual_args.to_vec();
    let Value::Symbol(command) = command else {
        return recorded;
    };
    let Some(replacements) = interp.get_symbol_property(command, "interactive-args") else {
        return recorded;
    };
    let Ok(replacements) = replacements.to_vec() else {
        return recorded;
    };
    for replacement in replacements {
        let Some((index, value)) = replacement.cons_values() else {
            continue;
        };
        let Value::Integer(index) = index else {
            continue;
        };
        let Ok(index) = usize::try_from(index) else {
            continue;
        };
        if index < recorded.len() {
            recorded[index] = value;
        }
    }
    recorded
}
