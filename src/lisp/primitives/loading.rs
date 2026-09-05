use super::*;
use crate::lisp::types::EnvFrame;

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
        interp.load_target_with_env(&file, env)?;
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
    // Like `readevalloop', evaluating a file-visiting buffer records its
    // definitions in `load-history' under the buffer's file name.
    let source_file = interp
        .get_buffer_by_id(buffer_id)
        .and_then(|buffer| buffer.file.clone());
    let previous_load_list = source_file.as_ref().map(|file| {
        let previous = interp
            .lookup_var("current-load-list", env)
            .unwrap_or(Value::Nil);
        interp.set_global_binding(
            "current-load-list",
            Value::list([Value::String(file.clone().into())]),
        );
        previous
    });
    // Feval_buffer returns nil whatever the last form evaluated to.
    let result = eval_buffer_forms(interp, buffer_id, env).map(|_| Value::Nil);
    if let Some(previous) = previous_load_list {
        let current = interp
            .lookup_var("current-load-list", env)
            .unwrap_or(Value::Nil);
        if result.is_ok()
            && let Some(source_file) = source_file
        {
            interp.commit_entire_load_history(&source_file, current);
        }
        interp.set_global_binding("current-load-list", previous);
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
        let forms = crate::lisp::reader::Reader::new(&text).read_all()?;
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
        let buffer = &mut interp.buffer;
        buffer.goto_char(saved_point.min(buffer.point_max()));
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

fn eval_buffer_forms(
    interp: &mut Interpreter,
    buffer_id: u64,
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
    let lexical = crate::lisp::extract_mode_line_variable(&text, "lexical-binding")
        .is_some_and(|value| value != "nil");
    if !matches!(&load_read, Value::Symbol(symbol) if symbol == "read") {
        // A customized reader (like `edebug--read') reads from the buffer
        // itself, form by form, moving point like `readevalloop' does.
        return with_fresh_eval_environment(interp, lexical, |interp, eval_env| {
            eval_buffer_via_load_read_function(interp, buffer_id, &load_read, eval_env)
        });
    }
    let forms = crate::lisp::reader::Reader::new(&text).read_all()?;
    with_fresh_eval_environment(interp, lexical, |interp, eval_env| {
        let mut result = Value::Nil;
        for form in forms {
            // lread.c reads through the dynamically active `obarray', so a
            // let-bound private obarray owns the parsed symbols.
            let form = interp.intern_read_symbols_in_value(form, eval_env)?;
            result = eager_expand_eval(interp, &form, eval_env)?;
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
        match eager_expand_eval(interp, &form, env) {
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
    interp: &Interpreter,
    target: &str,
    env: &Env,
) -> Option<PathBuf> {
    let direct = PathBuf::from(target);
    if direct.is_file() {
        return Some(direct);
    }
    let bare_target = !target.ends_with(".el") && !target.ends_with(".elc");
    let with_el = bare_target.then(|| format!("{target}.el"));
    let with_elc = bare_target.then(|| format!("{target}.elc"));
    let Some(load_path) = interp.lookup_var("load-path", env) else {
        return interp.resolve_load_target(target);
    };
    let Ok(entries) = load_path.to_vec() else {
        return interp.resolve_load_target(target);
    };
    for entry in entries {
        let Some(root) = string_like(&entry).map(|string| PathBuf::from(string.text)) else {
            continue;
        };
        let candidate = root.join(target);
        if candidate.is_file() {
            return Some(candidate);
        }
        if let Some(with_el) = &with_el {
            let candidate = root.join(with_el);
            if candidate.is_file() {
                if interp.load_source_prefers_elc(&candidate)
                    && let Some(with_elc) = &with_elc
                {
                    let elc = root.join(with_elc);
                    if elc.is_file() {
                        return Some(elc);
                    }
                }
                return Some(candidate);
            }
        }
        // GNU load-suffixes include .elc; the .el may be gone (gzipped
        // sources with compiled artifacts left in place).
        if let Some(with_elc) = &with_elc {
            let candidate = root.join(with_elc);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    interp.resolve_load_target(target)
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
    let no_native = requested.ends_with(".elc")
        || interp
            .forwarded_c_value("load-no-native", env)
            .is_some_and(|value| value.is_truthy());
    // lread.c updates this table for every selected file, before testing
    // its suffix. comp.c uses the entry to suppress deferred compilation
    // when an explicit .elc load or load-no-native requested bytecode.
    let table = interp
        .forwarded_c_value("comp-no-native-file-h", env)
        .ok_or_else(|| LispError::Void("comp-no-native-file-h".into()))?;
    let filename = Value::string(resolved.to_string_lossy().as_ref());
    let mut native_env = env.clone();
    if no_native {
        super::call(
            interp,
            "puthash",
            &[filename, Value::T, table],
            &mut native_env,
        )?;
    } else {
        super::call(interp, "remhash", &[filename, table], &mut native_env)?;
    }
    if no_native
        || resolved
            .extension()
            .is_none_or(|extension| extension != "elc")
    {
        return Ok(resolved.to_path_buf());
    }

    // GNU takes this list snapshot before file-exists-p and source hashing:
    // a file-name handler can rebind the C slot during either call.
    let mut load_paths = interp
        .forwarded_c_value("native-comp-eln-load-path", env)
        .unwrap_or(Value::Nil);
    let mut source = resolved.to_path_buf();
    source.set_extension("el");
    let exists = |interp: &mut Interpreter, path: &Path, env: &mut Env| {
        super::call(
            interp,
            "file-exists-p",
            &[Value::string(path.to_string_lossy().as_ref())],
            env,
        )
        .map(|value| value.is_truthy())
    };
    if !exists(interp, &source, &mut native_env)? {
        source = PathBuf::from(format!("{}.gz", source.display()));
        if !exists(interp, &source, &mut native_env)?
            && interp
                .lookup_var("native-comp-warning-on-missing-source", env)
                .unwrap_or(Value::Unbound)
                .is_truthy()
        {
            let load_path = interp
                .forwarded_c_value("load-path", env)
                .unwrap_or(Value::Nil);
            // An installation with no central .el sources must not produce
            // a warning cascade. This is GNU's own sanity check, not a test
            // or filename-specific execution substitute.
            if !locate_file_internal(
                interp,
                &Value::string("simple.el"),
                &load_path,
                &Value::Nil,
                &Value::Nil,
                &mut native_env,
            )?
            .is_nil()
            {
                let warning = Value::list([
                    Value::symbol("native-compiler"),
                    Value::string(&format!(
                        "Cannot look up .eln file for {} because no source file was found for it",
                        resolved.display()
                    )),
                ]);
                let pending = interp
                    .forwarded_c_value("delayed-warnings-list", env)
                    .unwrap_or(Value::Nil);
                interp.set_forwarded_lisp_value(
                    "delayed-warnings-list",
                    Value::cons(warning, pending),
                );
            }
            return Ok(resolved.to_path_buf());
        }
    }

    let mut filename_env = env.clone();
    let relative = super::dispatch::comp_el_to_eln_rel_filename(
        interp,
        &Value::String(source.display().to_string().into()),
        &mut filename_env,
    )?;
    let resolved_mtime = fs::metadata(resolved)
        .and_then(|metadata| metadata.modified())
        .ok();

    let expand = |interp: &mut Interpreter, name: Value, base: Value, env: &mut Env| {
        super::call(interp, "expand-file-name", &[name, base], env)
    };
    let mut system_directory = Value::Nil;
    let mut seen = crate::lisp::types::CycleGuard::new();
    // FOR_EACH_TAIL_SAFE in lread.c stops at a dotted tail or cycle, while
    // preserving every directory already visited.
    while let Some((car, cdr)) = load_paths.cons_cells() {
        if seen.step(car.cell_id()) {
            break;
        }
        system_directory = car.borrow().clone();
        // Unlike the path-list snapshot, the version slot is read afresh
        // for each candidate's expansion in lread.c.
        let version = interp
            .forwarded_c_value("comp-native-version-dir", env)
            .unwrap_or(Value::Nil);
        let directory = expand(interp, version, system_directory.clone(), &mut native_env)?;
        let candidate = PathBuf::from(string_text(&expand(
            interp,
            Value::string(&relative),
            directory,
            &mut native_env,
        )?)?);
        if native_candidate_is_current(&candidate, resolved_mtime) {
            record_native_source(interp, &candidate, &source, env)?;
            return Ok(candidate);
        }
        load_paths = cdr.borrow().clone();
    }

    // GNU also searches the `preloaded' subdirectory of the final (system)
    // native directory after all ordinary cache directories.
    let version = interp
        .forwarded_c_value("comp-native-version-dir", env)
        .unwrap_or(Value::Nil);
    let directory = expand(interp, version, system_directory, &mut native_env)?;
    let preloaded = expand(
        interp,
        Value::string("preloaded"),
        directory,
        &mut native_env,
    )?;
    let candidate = PathBuf::from(string_text(&expand(
        interp,
        Value::string(&relative),
        preloaded,
        &mut native_env,
    )?)?);
    if native_candidate_is_current(&candidate, resolved_mtime) {
        record_native_source(interp, &candidate, &source, env)?;
        return Ok(candidate);
    }

    Ok(resolved.to_path_buf())
}

fn native_candidate_is_current(
    candidate: &Path,
    resolved_mtime: Option<std::time::SystemTime>,
) -> bool {
    let Ok(metadata) = fs::File::open(candidate).and_then(|file| file.metadata()) else {
        return false;
    };
    if metadata.is_dir() {
        return false;
    }
    match (metadata.modified().ok(), resolved_mtime) {
        (Some(native), Some(resolved)) => native >= resolved,
        _ => false,
    }
}

fn record_native_source(
    interp: &mut Interpreter,
    native: &Path,
    source: &Path,
    env: &Env,
) -> Result<(), LispError> {
    let table = interp
        .forwarded_c_value("comp-eln-to-el-h", env)
        .ok_or_else(|| LispError::Void("comp-eln-to-el-h".into()))?;
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

pub(crate) fn get_load_suffixes_value(interp: &Interpreter, env: &Env) -> Result<Value, LispError> {
    let suffixes = interp
        .lookup_var("load-suffixes", env)
        .unwrap_or(Value::list([Value::String(".el".into())]))
        .to_vec()?;
    let rep_suffixes = interp
        .lookup_var("load-file-rep-suffixes", env)
        .unwrap_or(Value::list([Value::String(String::new().into())]))
        .to_vec()?;
    let mut values = Vec::new();
    for suffix in suffixes {
        let suffix = string_text(&suffix)?;
        for rep in &rep_suffixes {
            values.push(Value::String(
                format!("{suffix}{}", string_text(rep)?).into(),
            ));
        }
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

/// lread.c openp: search PATH for FILE with SUFFIXES, returning the found
/// name (or nil) together with the errno openp leaves for its caller's
/// `report_file_error'.  Only an access-mask predicate tracks errno: the
/// value starts at ENOENT, an accessible directory records EISDIR, and any
/// failure other than ENOENT/ENOTDIR replaces it.
pub(crate) fn locate_file_search(
    interp: &mut Interpreter,
    file: &Value,
    path: &Value,
    suffixes: &Value,
    predicate: &Value,
    env: &mut Env,
) -> Result<(Value, i32), LispError> {
    let file = string_text(file)?;
    let mut last_errno = libc::ENOENT;
    let mut path_entries = path.to_vec()?;
    // openp treats an empty search path as one empty element, and an empty
    // element means the dynamically current `default-directory'.
    if path_entries.is_empty() {
        path_entries.push(Value::Nil);
    }
    let suffixes = match suffixes {
        Value::Nil => vec![String::new()],
        Value::String(_) | Value::StringObject(_) => vec![string_text(suffixes)?],
        _ => suffixes
            .to_vec()?
            .into_iter()
            .map(|value| string_text(&value))
            .collect::<Result<Vec<_>, _>>()?,
    };
    let default_directory = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .unwrap_or_else(default_directory);
    let default_directory =
        unquote_local_file_name(&default_directory).unwrap_or(default_directory);

    for directory in path_entries {
        let directory = if directory.is_nil() {
            default_directory.clone()
        } else {
            let directory = string_text(&directory)?;
            let directory = unquote_local_file_name(&directory).unwrap_or(directory);
            expand_file_name_in_env(interp, env, &directory, Some(&default_directory))
        };
        for suffix in &suffixes {
            let candidate =
                expand_file_name_in_env(interp, env, &format!("{file}{suffix}"), Some(&directory));
            let candidate = unquote_local_file_name(&candidate).unwrap_or(candidate);
            let predicate = (!predicate.is_nil()).then_some(predicate);
            let found = match predicate.and_then(locate_file_access_mask) {
                Some(mask) => match locate_file_access_probe(mask, &candidate) {
                    Ok(()) => true,
                    Err(errno) => {
                        if errno != libc::ENOENT && errno != libc::ENOTDIR {
                            last_errno = errno;
                        }
                        false
                    }
                },
                None => locate_file_candidate_matches(interp, predicate, &candidate, env)?,
            };
            if found {
                // Search the isolated physical tree, but report the same
                // standard-Lisp source provenance exposed by GNU's build-tree
                // load path.  Test-owned paths are outside the configured
                // prefix and remain untouched.
                let provenance = interp.load_source_provenance_path(Path::new(&candidate));
                return Ok((
                    Value::String(provenance.display().to_string().into()),
                    last_errno,
                ));
            }
        }
    }

    Ok((Value::Nil, last_errno))
}

pub(crate) fn locate_file_candidate_matches(
    interp: &mut Interpreter,
    predicate: Option<&Value>,
    candidate: &str,
    env: &mut Env,
) -> Result<bool, LispError> {
    let Some(predicate) = predicate else {
        return Ok(fs::metadata(candidate)
            .map(|metadata| metadata.is_file() && file_readable_p(candidate))
            .unwrap_or(false));
    };
    if let Some(mask) = locate_file_access_mask(predicate) {
        return Ok(locate_file_access_matches(mask, candidate));
    }
    Ok(interp
        .call_function_value(
            resolve_callable(interp, predicate, env)?,
            predicate.as_symbol().ok(),
            &[Value::String(candidate.to_string().into())],
            env,
        )?
        .is_truthy())
}

pub(crate) fn locate_file_access_mask(value: &Value) -> Option<i64> {
    if let Ok(mask) = value.as_integer() {
        return Some(mask);
    }
    if let Ok(symbol) = value.as_symbol() {
        return locate_file_access_symbol_mask(symbol);
    }
    let items = value.to_vec().ok()?;
    if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "lambda") {
        return None;
    }
    let mut mask = 0;
    for item in items {
        mask |= locate_file_access_symbol_mask(item.as_symbol().ok()?)?;
    }
    Some(mask)
}

pub(crate) fn locate_file_access_symbol_mask(symbol: &str) -> Option<i64> {
    match symbol {
        "executable" => Some(1),
        "writable" => Some(2),
        "readable" => Some(4),
        "exists" => Some(0),
        _ => None,
    }
}

pub(crate) fn locate_file_access_matches(mask: i64, candidate: &str) -> bool {
    locate_file_access_probe(mask, candidate).is_ok()
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
    if fs::metadata(candidate).is_ok_and(|metadata| metadata.is_dir()) {
        return Err(libc::EISDIR);
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

#[cfg(test)]
mod native_load_tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn directory() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("emaxx-native-load-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).expect("create unique native loader fixture directory");
        path
    }

    fn path_value(path: &Path) -> Value {
        Value::string(path.to_string_lossy().as_ref())
    }

    fn table_entry(interp: &mut Interpreter, name: &str, key: Value, env: &mut Env) -> Value {
        let table = interp.lookup_var(name, env).expect("C-initialized table");
        super::super::call(interp, "gethash", &[key, table, Value::Nil], env).expect("gethash")
    }

    fn detach_and_replace(
        interp: &mut Interpreter,
        name: &str,
        replacement: Value,
        env: &mut Env,
    ) -> Value {
        let original = interp.forwarded_c_value(name, env).expect("C slot exists");
        super::super::call(interp, "makunbound", &[Value::symbol(name)], env)
            .expect("detach forwarded symbol");
        assert!(interp.lookup_var(name, env).is_none());
        interp.set_variable(name, replacement.clone(), env);
        assert_eq!(interp.lookup_var(name, env), Some(replacement));
        assert_eq!(interp.forwarded_c_value(name, env), Some(original.clone()));
        original
    }

    #[test]
    fn native_load_keeps_detached_c_slots_separate_from_lisp_bindings() {
        let work = directory();
        let selected = work.join("absent.elc");
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let suppression =
            detach_and_replace(&mut interp, "comp-no-native-file-h", Value::Nil, &mut env);
        interp.set_variable("load-no-native", Value::T, &mut env);
        detach_and_replace(&mut interp, "load-no-native", Value::Nil, &mut env);
        assert_eq!(
            maybe_swap_for_native(&mut interp, "absent", &selected, &env)
                .expect("detached C bool still suppresses native load"),
            selected
        );
        assert_eq!(
            super::super::call(
                &mut interp,
                "gethash",
                &[path_value(&selected), suppression, Value::Nil],
                &mut env
            )
            .expect("read retained C suppression table"),
            Value::T
        );
        assert_eq!(
            interp.lookup_var("comp-no-native-file-h", &env),
            Some(Value::Nil)
        );

        let mut interp = Interpreter::new();
        let mut env = Env::new();
        interp.set_variable(
            "load-path",
            Value::list([path_value(
                &Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/lisp"),
            )]),
            &mut env,
        );
        detach_and_replace(&mut interp, "load-path", Value::Nil, &mut env);
        let user_value = Value::symbol("independent-warning-binding");
        detach_and_replace(
            &mut interp,
            "delayed-warnings-list",
            user_value.clone(),
            &mut env,
        );
        let warning = Value::list([
            Value::symbol("native-compiler"),
            Value::string(&format!(
                "Cannot look up .eln file for {} because no source file was found for it",
                selected.display()
            )),
        ]);
        for count in 1..=2 {
            maybe_swap_for_native(&mut interp, "absent", &selected, &env)
                .expect("warning appended to retained C slot");
            assert_eq!(
                interp.forwarded_c_value("delayed-warnings-list", &env),
                Some(Value::list(std::iter::repeat_n(warning.clone(), count)))
            );
            assert_eq!(
                interp.lookup_var("delayed-warnings-list", &env),
                Some(user_value.clone())
            );
        }
        fs::remove_dir_all(&work).expect("remove successful detached loader fixture");
    }

    #[test]
    fn native_load_suppression_and_missing_source_follow_lread_c() {
        let work = directory();
        let selected = work.join("missing-source.elc");
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        interp.set_variable("default-directory", path_value(&work), &mut env);
        interp.set_variable("load-path", Value::Nil, &mut env);

        // Exercise the selected-file boundary without creating or evaluating
        // diagnostic Elisp. lread.c records suppression before inspecting the
        // source, so an explicit .elc request needs no source file.
        assert_eq!(
            maybe_swap_for_native(&mut interp, "missing-source.elc", &selected, &env)
                .expect("explicit bytecode load"),
            selected
        );
        assert_eq!(
            table_entry(
                &mut interp,
                "comp-no-native-file-h",
                path_value(&selected),
                &mut env
            ),
            Value::T
        );
        interp.set_variable("load-no-native", Value::T, &mut env);
        assert_eq!(
            maybe_swap_for_native(&mut interp, "missing-source", &selected, &env)
                .expect("dynamic bytecode suppression"),
            selected
        );
        assert_eq!(
            table_entry(
                &mut interp,
                "comp-no-native-file-h",
                path_value(&selected),
                &mut env
            ),
            Value::T
        );

        interp.set_variable("load-no-native", Value::Nil, &mut env);
        interp.set_variable("native-comp-warning-on-missing-source", Value::T, &mut env);
        assert_eq!(
            maybe_swap_for_native(&mut interp, "missing-source", &selected, &env)
                .expect("installation with no central sources"),
            selected
        );
        assert_eq!(
            table_entry(
                &mut interp,
                "comp-no-native-file-h",
                path_value(&selected),
                &mut env
            ),
            Value::Nil
        );
        assert_eq!(
            interp.lookup_var("delayed-warnings-list", &env),
            Some(Value::Nil)
        );

        // GNU warns only when ordinary central sources are available. Use
        // the unchanged pinned installation, not a generated simple.el.
        let upstream_lisp = Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/lisp");
        assert!(upstream_lisp.join("simple.el").is_file());
        interp.set_variable(
            "load-path",
            Value::list([path_value(&upstream_lisp)]),
            &mut env,
        );
        assert_eq!(
            maybe_swap_for_native(&mut interp, "missing-source", &selected, &env)
                .expect("missing source warning"),
            selected
        );
        assert_eq!(
            interp.lookup_var("delayed-warnings-list", &env),
            Some(Value::list([Value::list([
                Value::symbol("native-compiler"),
                Value::string(&format!(
                    "Cannot look up .eln file for {} because no source file was found for it",
                    selected.display()
                ))
            ])]))
        );
        // GNU still attempts the source hash when this warning is disabled;
        // comp.c then signals file-missing. Do not invent a quiet fallback.
        interp.set_variable(
            "native-comp-warning-on-missing-source",
            Value::Nil,
            &mut env,
        );
        let error = maybe_swap_for_native(&mut interp, "missing-source", &selected, &env)
            .expect_err("GNU source hashing rejects the absent .el.gz");
        assert!(matches!(error, LispError::SignalValue(ref signal)
            if *signal == Value::list([Value::symbol("file-missing"), path_value(&work.join("missing-source.el.gz"))])));

        let source = work.join("ordinary.el");
        interp.set_variable("load-no-native", Value::T, &mut env);
        maybe_swap_for_native(&mut interp, "ordinary.el", &source, &env)
            .expect("source suppression entry");
        assert_eq!(
            table_entry(
                &mut interp,
                "comp-no-native-file-h",
                path_value(&source),
                &mut env
            ),
            Value::T
        );
        interp.set_variable("load-no-native", Value::Nil, &mut env);
        maybe_swap_for_native(&mut interp, "ordinary.el", &source, &env)
            .expect("clear source suppression entry");
        assert_eq!(
            table_entry(
                &mut interp,
                "comp-no-native-file-h",
                path_value(&source),
                &mut env
            ),
            Value::Nil
        );
        fs::remove_dir_all(&work).expect("remove successful loader fixture");
    }

    #[test]
    fn native_load_cache_resolution_uses_default_directory_and_file_times() {
        let work = directory();
        let upstream = Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs");
        let source = work.join("sample.el");
        let bytecode = work.join("sample.elc");
        // These copies remain byte-for-byte GNU files. This contract tests
        // path selection only; execution and .eln identity have separate gates.
        fs::copy(upstream.join("lisp/emacs-lisp/seq.el"), &source).expect("copy GNU source");
        fs::copy(upstream.join("lisp/emacs-lisp/seq.elc"), &bytecode).expect("copy GNU bytecode");
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        interp.set_variable("default-directory", path_value(&work), &mut env);
        interp.set_variable(
            "invocation-directory",
            path_value(&work.join("not-the-cache-base")),
            &mut env,
        );
        interp.set_variable(
            "native-comp-eln-load-path",
            Value::list([Value::string("cache")]),
            &mut env,
        );
        let version = string_text(
            &interp
                .lookup_var("comp-native-version-dir", &env)
                .expect("ABI version"),
        )
        .expect("version string");
        let relative = super::super::dispatch::comp_el_to_eln_rel_filename(
            &mut interp,
            &path_value(&source),
            &mut env,
        )
        .expect("GNU relative native name");
        let native_source = fs::read_dir(
            upstream
                .join("native-lisp")
                .join(&version)
                .join("preloaded"),
        )
        .expect("pinned native preload directory")
        .map(|entry| entry.expect("native preload entry").path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("seq-"))
                && path.extension().is_some_and(|extension| extension == "eln")
        })
        .expect("unchanged GNU seq native artifact");
        let native = work.join("cache").join(&version).join(&relative);
        fs::create_dir_all(native.parent().expect("cache directory")).expect("create cache");
        fs::copy(&native_source, &native).expect("copy GNU native artifact");
        let set_time = |path: &Path, seconds| {
            fs::File::open(path)
                .expect("open fixture")
                .set_times(
                    fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)),
                )
                .expect("set deterministic fixture timestamp");
        };
        set_time(&bytecode, 100);
        set_time(&native, 99);
        assert_eq!(
            maybe_swap_for_native(&mut interp, "sample", &bytecode, &env)
                .expect("old native artifact"),
            bytecode
        );
        set_time(&native, 100);
        assert_eq!(
            maybe_swap_for_native(&mut interp, "sample", &bytecode, &env)
                .expect("equal timestamps permit native selection"),
            native
        );
        assert_eq!(
            table_entry(
                &mut interp,
                "comp-eln-to-el-h",
                Value::string(&relative),
                &mut env
            ),
            path_value(&source)
        );
        assert_eq!(
            maybe_swap_for_native(&mut interp, "sample.elc", &bytecode, &env)
                .expect("explicit bytecode wins over native cache"),
            bytecode
        );

        let preloaded = work.join(&version).join("preloaded").join(&relative);
        fs::create_dir_all(preloaded.parent().expect("preloaded directory"))
            .expect("create fallback cache");
        fs::copy(&native_source, &preloaded).expect("copy GNU fallback artifact");
        set_time(&preloaded, 100);
        for path in [Value::Nil, Value::list([Value::Nil])] {
            interp.set_variable("native-comp-eln-load-path", path, &mut env);
            assert_eq!(
                maybe_swap_for_native(&mut interp, "sample", &bytecode, &env)
                    .expect("nil directory uses Lisp default-directory"),
                preloaded
            );
        }
        assert!(!native_candidate_is_current(&native, None));
        assert!(!native_candidate_is_current(&work, Some(UNIX_EPOCH)));

        interp.set_variable(
            "native-comp-eln-load-path",
            Value::list([Value::string("cache")]),
            &mut env,
        );
        // A real C primitive can act as the file handler here: `set'
        // receives (OPERATION FILENAME), and the variable alias redirects
        // that write to the native path slot. No diagnostic Elisp or test
        // callback is needed to exercise reentrant C-slot mutation.
        interp
            .set_variable_alias("file-exists-p", "native-comp-eln-load-path")
            .expect("redirect the handler's C set operation");
        interp.put_symbol_property(
            "set",
            "operations",
            Value::list([Value::symbol("file-exists-p")]),
        );
        interp.set_variable(
            "file-name-handler-alist",
            Value::list([Value::cons(
                Value::string("sample\\.el\\'"),
                Value::symbol("set"),
            )]),
            &mut env,
        );
        assert_eq!(
            maybe_swap_for_native(&mut interp, "sample", &bytecode, &env)
                .expect("path list is captured before the file handler runs"),
            native
        );
        assert_eq!(
            interp.lookup_var("native-comp-eln-load-path", &env),
            Some(path_value(
                &fs::canonicalize(&source).expect("real source path")
            ))
        );
        interp.set_variable("file-name-handler-alist", Value::Nil, &mut env);
        interp.set_variable(
            "native-comp-eln-load-path",
            Value::list([Value::string("cache")]),
            &mut env,
        );

        for (name, replacement) in [
            ("comp-native-version-dir", Value::Nil),
            ("native-comp-eln-load-path", Value::Integer(17)),
            ("load-no-native", Value::T),
            ("comp-no-native-file-h", Value::Nil),
        ] {
            detach_and_replace(&mut interp, name, replacement, &mut env);
        }
        let origins = detach_and_replace(&mut interp, "comp-eln-to-el-h", Value::Nil, &mut env);
        super::super::call(
            &mut interp,
            "remhash",
            &[Value::string(&relative), origins.clone()],
            &mut env,
        )
        .expect("clear old origin record");
        assert_eq!(
            maybe_swap_for_native(&mut interp, "sample", &bytecode, &env)
                .expect("native selection reads retained C slots"),
            native
        );
        assert_eq!(
            super::super::call(
                &mut interp,
                "gethash",
                &[Value::string(&relative), origins, Value::Nil],
                &mut env,
            )
            .expect("fresh mapping in retained C table"),
            path_value(&source)
        );
        assert_eq!(
            interp.lookup_var("comp-eln-to-el-h", &env),
            Some(Value::Nil)
        );
        fs::remove_dir_all(&work).expect("remove successful native path fixture");
    }
}
