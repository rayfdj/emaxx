use super::*;

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
        if !seen.insert(name.clone()) {
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
    let oclosure_spec = if crate::lisp::primitives::dispatch::oclosure_type_of(&func).is_some()
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
    let interactive_args = collect_interactive_args(interp, &func, env)?;
    interp.push_interactive_call();
    // The interactive dispatch frame is what `called-interactively-p's
    // backtrace walk stops at (GNU stops at funcall-interactively); the
    // native dispatch paths (special form, command loop) don't otherwise
    // leave one.
    interp.push_backtrace_frame(
        Value::Symbol("funcall-interactively".into()),
        &interactive_args,
    );
    let result = invoke_function_value(interp, &func, &interactive_args, env);
    interp.pop_backtrace_frame();
    interp.pop_interactive_call();
    let result = result?;
    if args.get(1).is_some_and(Value::is_truthy)
        && let Some(function_name) = callable_name(&args[0], &func)
    {
        let history_args = history_args_for_call(interp, &func, &interactive_args, env)?;
        record_command_history(interp, &function_name, history_args, env);
    }
    Ok(result)
}

pub(crate) fn eval_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs("eval".into(), args.len()));
    }
    if let Some(lexical) = args.get(1) {
        let (capture_lexical, trim_context, mut eval_env) = match lexical {
            Value::Nil => (false, false, Vec::new()),
            Value::T => (true, false, env.clone()),
            Value::Cons(_, _) => {
                let frame = lexical_alist_frame(lexical)?;
                (true, true, vec![frame])
            }
            _ => (true, false, env.clone()),
        };
        interp.push_lambda_eval_context(capture_lexical, trim_context);
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
        interp.push_lambda_eval_context(false, false);
        let previous_activation = interp.enter_activation();
        let result = interp.eval(&args[0], &mut Vec::new());
        interp.leave_activation(previous_activation);
        interp.pop_lambda_capture_override();
        result
    }
}

fn lexical_alist_frame(value: &Value) -> Result<Vec<(String, Value)>, LispError> {
    let mut frame = Vec::new();
    for entry in value.to_vec()? {
        let Some((key, val)) = entry.cons_values() else {
            continue;
        };
        if let Value::Symbol(name) = key {
            frame.push((name, val));
        }
    }
    Ok(frame)
}

pub(crate) fn unload_feature_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(
            "unload-feature".into(),
            args.len(),
        ));
    }
    let feature = args[0].as_symbol()?.to_string();
    if !interp.has_feature(&feature) {
        return Err(LispError::Signal(format!(
            "{feature} is not a currently loaded feature"
        )));
    }
    let provide_entry = Value::cons(
        Value::Symbol("provide".into()),
        Value::Symbol(feature.clone()),
    );
    let load_history = interp.lookup_var("load-history", env).unwrap_or(Value::Nil);
    let mut entries = load_history.to_vec().unwrap_or_default();
    let feature_entry = entries.iter().position(|entry| {
        let Value::Cons(_, defs) = entry else {
            return false;
        };
        let defs = defs.borrow().clone();
        defs.to_vec()
            .is_ok_and(|defs| defs.iter().any(|def| def == &provide_entry))
    });
    if let Some(index) = feature_entry {
        // Purge every entry recorded for the feature's file: repeated
        // evaluation of the same file stacks entries, and a stale entry
        // keeps pointing at a temp file after it is deleted.
        let feature_file = match &entries[index] {
            Value::Cons(file, _) => Some(file.borrow().clone()),
            _ => None,
        };
        let mut removed = vec![entries.remove(index)];
        if let Some(feature_file) = &feature_file {
            let mut kept = Vec::with_capacity(entries.len());
            for entry in entries {
                let same_file = matches!(
                    &entry,
                    Value::Cons(file, _) if &*file.borrow() == feature_file
                );
                if same_file {
                    removed.push(entry);
                } else {
                    kept.push(entry);
                }
            }
            entries = kept;
        }
        for entry in &removed {
            let Value::Cons(_, defs) = entry else {
                continue;
            };
            let defs = defs.borrow().clone();
            for def in defs.to_vec().unwrap_or_default() {
                let Value::Cons(kind, target) = &def else {
                    continue;
                };
                let kind = kind.borrow().clone();
                let target = target.borrow().clone();
                match (&kind, &target) {
                    (Value::Symbol(kind), Value::Symbol(name)) if kind == "provide" => {
                        interp.unprovide_feature(name);
                    }
                    (Value::Symbol(kind), Value::Symbol(name)) if kind == "defun" => {
                        interp.remove_all_function_bindings(name);
                    }
                    (Value::Symbol(kind), rest) if kind == "cl-defmethod" => {
                        if let Ok(parts) = rest.to_vec()
                            && let Some(Value::Symbol(name)) = parts.first()
                        {
                            interp.remove_all_function_bindings(name);
                            interp.put_symbol_property(
                                name,
                                "emaxx-cl-defmethod-specializers",
                                Value::Nil,
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
        interp.set_global_binding("load-history", Value::list(entries));
    }
    interp.unprovide_feature(&feature);
    Ok(Value::Nil)
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
            Value::list([Value::String(file.clone())]),
        );
        previous
    });
    let result = eval_buffer_forms(interp, buffer_id, env);
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
    let read_function = args.get(3).filter(|value| !value.is_nil()).cloned();
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
        Value::cons(Value::Buffer(buffer_id, buffer_name), eval_buffer_list),
        env,
    )?);
    if let Some(file) = source_file {
        restores.push(interp.bind_special_variable(
            "current-load-list",
            Value::list([Value::String(file)]),
            env,
        )?);
    }

    let mut result = (|| -> Result<Value, LispError> {
        if let Some(read_function) = read_function {
            return eval_region_via_read_function(
                interp,
                buffer_id,
                start,
                end,
                &read_function,
                &print_flag,
                env,
            );
        }
        let text = interp
            .buffer
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let forms = crate::lisp::reader::Reader::new(&text).read_all()?;
        let mut result = Value::Nil;
        for form in forms {
            interp.intern_symbols_in_value(&form);
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
    })();

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
    let stream = Value::Buffer(buffer_id, interp.buffer.name.clone());
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
        let form = match interp.call_function_value(
            read_function.clone(),
            None,
            std::slice::from_ref(&stream),
            env,
        ) {
            Ok(form) => form,
            Err(error) if error.condition_type() == "end-of-file" => break,
            Err(error) => return Err(error),
        };
        interp.intern_symbols_in_value(&form);
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
    if !matches!(&load_read, Value::Symbol(symbol) if symbol == "read") {
        // A customized reader (like `edebug--read') reads from the buffer
        // itself, form by form, moving point like `readevalloop' does.
        return eval_buffer_via_load_read_function(interp, buffer_id, &load_read, env);
    }
    let text = interp
        .get_buffer_by_id(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
        .buffer_string();
    let forms = crate::lisp::reader::Reader::new(&text).read_all()?;
    let mut result = Value::Nil;
    for form in forms {
        interp.intern_symbols_in_value(&form);
        result = eager_expand_eval(interp, &form, env)?;
    }
    Ok(result)
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
    let compile_time_only = form.to_vec().is_ok_and(
        |items| matches!(items.first(), Some(Value::Symbol(head)) if head == "eval-when-compile"),
    );
    if compile_time_only {
        return interp.with_current_load_history_suppressed(|interp| {
            eager_expand_eval_inner(interp, form, env)
        });
    }
    eager_expand_eval_inner(interp, form, env)
}

fn eager_expand_eval_inner(
    interp: &mut Interpreter,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let expanded =
        crate::lisp::primitives::call(interp, "macroexpand", std::slice::from_ref(form), env)
            .unwrap_or_else(|_| form.clone());
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
    let full = interp
        .macroexpand_all_form_with_environment(&expanded, None, env)
        .unwrap_or_else(|_| expanded.clone());
    interp.eval(&full, env)
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
        .unwrap_or(Value::list([Value::String(String::new())]))
        .to_vec()?;
    let mut values = Vec::new();
    for suffix in suffixes {
        let suffix = string_text(&suffix)?;
        for rep in &rep_suffixes {
            values.push(Value::String(format!("{suffix}{}", string_text(rep)?)));
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
    let file = string_text(file)?;
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
            if locate_file_candidate_matches(interp, predicate, &candidate, env)? {
                return Ok(Value::String(candidate));
            }
        }
    }

    Ok(Value::Nil)
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
            &[Value::String(candidate.to_string())],
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
    fs::metadata(candidate).is_ok()
        && (mask & 1 == 0 || file_executable_p(candidate))
        && (mask & 2 == 0 || file_writable_p(candidate))
        && (mask & 4 == 0 || file_readable_p(candidate))
}

pub(crate) fn history_args_for_call(
    interp: &mut Interpreter,
    func: &Value,
    actual_args: &[Value],
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut recorded = actual_args.to_vec();
    let Value::Lambda(params, _, _) = func else {
        return Ok(recorded);
    };
    let positional_params = params
        .iter()
        .filter(|param| *param != "&optional" && *param != "&rest")
        .cloned()
        .collect::<Vec<_>>();
    for (name, form) in interactive_args_overrides(func) {
        if let Some(index) = positional_params.iter().position(|param| param == &name) {
            let value = eval_callable_metadata_form(interp, func, &form, env)?;
            if index >= recorded.len() {
                recorded.resize(index + 1, Value::Nil);
            }
            recorded[index] = value;
        }
    }
    Ok(recorded)
}
