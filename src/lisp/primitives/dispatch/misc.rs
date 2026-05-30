use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "read"
            | "read-from-string"
            | "md5"
            | "sha1"
            | "secure-hash"
            | "secure-hash-algorithms"
            | "buffer-hash"
            | "error"
            | "user-error"
            | "signal"
            | "throw"
            | "define-error"
            | "define-fringe-bitmap"
            | "intern"
            | "intern-soft"
            | "unintern"
            | "make-symbol"
            | "gensym"
            | "autoload"
            | "autoload-do-load"
            | "set"
            | "set-default"
            | "customize-set-variable"
            | "symbol-value"
            | "default-value"
            | "default-toplevel-value"
            | "set-default-toplevel-value"
            | "symbol-plist"
            | "setplist"
            | "interactive-form"
            | "autoloadp"
            | "macrop"
            | "apropos-internal"
            | "custom-autoload"
            | "custom-set-variables"
            | "custom-add-to-group"
            | "custom-current-group"
            | "daemonp"
            | "documentation"
            | "documentation-property"
            | "get"
            | "function-get"
            | "makunbound"
            | "lread--substitute-object-in-subtree"
            | "defvaralias"
            | "define-obsolete-variable-alias"
            | "indirect-variable"
            | "internal-delete-indirect-variable"
            | "internal--define-uninitialized-variable"
            | "defvar-1"
            | "defconst-1"
            | "internal-make-var-non-special"
            | "make-interpreted-closure"
            | "getenv"
            | "getenv-internal"
            | "set-language-environment"
            | "setenv"
            | "ignore"
            | "purecopy"
            | "help--docstring-quote"
            | "help-add-fundoc-usage"
            | "pcase--mutually-exclusive-p"
            | "make-obsolete"
            | "make-obsolete-variable"
            | "define-obsolete-face-alias"
            | "define-obsolete-function-alias"
            | "macroexp-warn-and-return"
            | "describe-function"
            | "macroexp-quote"
            | "macroexp-progn"
            | "macroexp-compiling-p"
            | "macroexp--dynamic-variable-p"
            | "macroexpand"
            | "macroexpand-1"
            | "macroexpand-all"
            | "run-at-time"
            | "run-with-timer"
            | "run-with-idle-timer"
            | "cancel-timer"
            | "timerp"
            | "lossage-size"
            | "executable-find"
            | "add-hook"
            | "run-hooks"
            | "run-mode-hooks"
            | "run-hook-with-args"
            | "run-hook-with-args-until-success"
            | "run-hook-with-args-until-failure"
            | "eval-after-load"
            | "run-hook-wrapped"
            | "ert-simulate-command"
            | "mapatoms"
            | "remove-hook"
    )
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        // ── Reader ──
        "read" => {
            need_args(name, args, 1)?;
            read_from_lisp_source(interp, &args[0], env)
        }
        "read-from-string" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let s = string_text(&args[0])?;
            let chars: Vec<char> = s.chars().collect();
            let start = normalize_string_index(args.get(1), 0, chars.len() as i64)? as usize;
            let end = normalize_string_index(args.get(2), chars.len() as i64, chars.len() as i64)?
                as usize;
            let slice: String = chars[start..end].iter().collect();
            let mut reader = crate::lisp::reader::Reader::new(&slice);
            match reader.read()? {
                Some(val) => {
                    let consumed = slice[..reader.position()].chars().count();
                    Ok(Value::cons(
                        crate::lisp::reader::resolve_circular_read_syntax(val)?,
                        Value::Integer((start + consumed) as i64),
                    ))
                }
                None => Err(LispError::EndOfInput),
            }
        }
        "md5" => {
            need_arg_range(name, args, 1, 4)?;
            let text = md5_source_text(interp, &args[0], args.get(1), args.get(2))?;
            let bytes = match args.get(3) {
                Some(coding) if !coding.is_nil() => {
                    encode_text_bytes(interp, &text, &checked_coding_symbol(interp, coding)?)?
                }
                _ => text.into_bytes(),
            };
            Ok(Value::String(format!("{:x}", md5::compute(bytes))))
        }
        "sha1" => {
            need_arg_range(name, args, 1, 4)?;
            secure_hash_value(
                interp,
                "sha1",
                &args[0],
                args.get(1),
                args.get(2),
                args.get(3),
            )
        }
        "secure-hash" => {
            need_arg_range(name, args, 2, 5)?;
            let algorithm = args[0].as_symbol()?;
            secure_hash_value(
                interp,
                algorithm,
                &args[1],
                args.get(2),
                args.get(3),
                args.get(4),
            )
        }
        "secure-hash-algorithms" => {
            need_args(name, args, 0)?;
            Ok(Value::list([
                Value::Symbol("md5".into()),
                Value::Symbol("sha1".into()),
                Value::Symbol("sha224".into()),
                Value::Symbol("sha256".into()),
                Value::Symbol("sha384".into()),
                Value::Symbol("sha512".into()),
                Value::Symbol("sha3-224".into()),
                Value::Symbol("sha3-256".into()),
                Value::Symbol("sha3-384".into()),
                Value::Symbol("sha3-512".into()),
            ]))
        }
        "buffer-hash" => {
            need_arg_range(name, args, 0, 1)?;
            buffer_hash_value(interp, args.first())
        }

        // ── Misc ──
        "error" => {
            let msg = if args.is_empty() {
                "error".to_string()
            } else if matches!(args[0], Value::String(_) | Value::StringObject(_)) {
                if args.len() > 1 {
                    string_text(&super::call(interp, "format", args, env)?)?
                } else {
                    string_text(&args[0])?
                }
            } else {
                args[0].to_string()
            };
            Err(LispError::Signal(msg))
        }
        "user-error" => {
            let msg = if args.is_empty() {
                "user-error".to_string()
            } else if let Ok(fmt) = string_text(&args[0]) {
                if args.len() > 1 {
                    string_text(&super::call(interp, "format", args, env)?)?
                } else {
                    fmt
                }
            } else {
                args[0].to_string()
            };
            Err(LispError::SignalValue(Value::list([
                Value::Symbol("user-error".into()),
                Value::String(msg),
            ])))
        }
        "signal" => {
            if args.is_empty() {
                return Err(LispError::Signal("signal".into()));
            }
            let condition = args[0].clone();
            let data = args.get(1).cloned().unwrap_or(Value::Nil);
            let value = if let Ok(items) = data.to_vec() {
                Value::cons(condition, Value::list(items))
            } else {
                Value::list([condition, data])
            };
            Err(LispError::SignalValue(value))
        }
        "throw" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("throw".into(), args.len()));
            }
            Err(LispError::Throw(args[0].clone(), args[1].clone()))
        }
        "define-error" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "define-fringe-bitmap" => Ok(Value::Nil),
        "intern" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol_name = match &args[0] {
                Value::Symbol(symbol) => symbol.clone(),
                _ => string_text(&args[0])?,
            };
            let symbol_name = apply_symbol_shorthands_in_env(interp, &symbol_name, env)?;
            match args.get(1) {
                Some(obarray) if !obarray.is_nil() => {
                    intern_in_obarray(interp, obarray, &symbol_name)
                }
                _ => {
                    interp.intern_symbol_name(&symbol_name);
                    Ok(Value::Symbol(symbol_name))
                }
            }
        }
        "intern-soft" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol_name = match &args[0] {
                Value::Symbol(symbol) if args.get(1).is_none_or(Value::is_nil) => {
                    return Ok(Value::Symbol(symbol.clone()));
                }
                Value::Symbol(symbol) => symbol.clone(),
                _ => string_text(&args[0])?,
            };
            let symbol_name = apply_symbol_shorthands_in_env(interp, &symbol_name, env)?;
            match args.get(1) {
                Some(obarray) if !obarray.is_nil() => {
                    intern_soft_in_obarray(interp, obarray, &symbol_name)
                }
                _ => Ok(default_intern_soft_result(interp, &symbol_name, env)),
            }
        }
        "unintern" => {
            need_arg_range(name, args, 1, 2)?;
            match args.get(1) {
                Some(obarray) if !obarray.is_nil() => {
                    Ok(if unintern_from_obarray(interp, obarray, &args[0], env)? {
                        Value::T
                    } else {
                        Value::Nil
                    })
                }
                _ => Ok(Value::Nil),
            }
        }
        "make-symbol" => {
            need_args(name, args, 1)?;
            let base = string_text(&args[0])?;
            let id = MAKE_SYMBOL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(Value::Symbol(
                crate::lisp::types::make_uninterned_symbol_name(&base, id),
            ))
        }
        "gensym" => {
            need_arg_range(name, args, 0, 1)?;
            let prefix = gensym_prefix(args.first())?;
            let id = GENSYM_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let visible = format!("{prefix}{id}");
            Ok(Value::Symbol(
                crate::lisp::types::make_uninterned_symbol_name(&visible, id),
            ))
        }
        "autoload" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let function = args[0].as_symbol()?.to_string();
            let file = string_text(&args[1])?;
            let docstring = args.get(2).cloned().unwrap_or(Value::Nil);
            let interactive = args.get(3).cloned().unwrap_or(Value::Nil);
            let kind = args.get(4).cloned().unwrap_or(Value::Nil);
            interp.push_function_binding(
                &function,
                Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String(file),
                    docstring,
                    interactive,
                    kind,
                ]),
            );
            Ok(Value::Symbol(function))
        }
        "autoload-do-load" => {
            need_arg_range(name, args, 1, 3)?;
            let fundef = args[0].clone();
            let Some((file, _, kind)) = autoload_parts(&fundef) else {
                return Ok(fundef);
            };
            let funname = args.get(1).cloned().unwrap_or(Value::Nil);
            let macro_only = args.get(2).cloned().unwrap_or(Value::Nil);
            let loads_macro = matches!(kind, Value::T)
                || matches!(&kind, Value::Symbol(symbol) if symbol == "t" || symbol == "macro");
            if matches!(&macro_only, Value::Symbol(symbol) if symbol == "macro") && !loads_macro {
                return Ok(fundef);
            }
            let ignore_errors = !loads_macro && macro_only.is_truthy();
            match interp.load_target(&file) {
                Ok(_) => {}
                Err(_) if ignore_errors => return Ok(Value::Nil),
                Err(error) => return Err(error),
            }
            if funname.is_nil() || ignore_errors {
                return Ok(Value::Nil);
            }
            if loads_macro {
                let symbol = funname.as_symbol()?;
                if let Some(function) = interp.macro_function_value(symbol) {
                    interp.push_function_binding(symbol, function.clone());
                    return Ok(function);
                }
            }
            let function = super::call(
                interp,
                "indirect-function",
                std::slice::from_ref(&funname),
                env,
            )?;
            if values_equal(interp, &function, &fundef) {
                let symbol = funname.as_symbol()?;
                return Err(LispError::Signal(format!(
                    "Autoloading file {file} failed to define function {symbol}"
                )));
            }
            Ok(function)
        }
        "set" => {
            need_args(name, args, 2)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            let value = interp.prepare_variable_assignment(&symbol, args[1].clone())?;
            let buffer_id = interp.assignment_buffer_id(&symbol);
            interp.notify_variable_watchers(&symbol, value.clone(), "set", buffer_id, env)?;
            interp.set_symbol_value_cell(&symbol, value.clone());
            Ok(value)
        }
        "set-default" => {
            need_args(name, args, 2)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            let value = interp.prepare_variable_assignment(&symbol, args[1].clone())?;
            interp.notify_variable_watchers(&symbol, value.clone(), "set", None, env)?;
            interp.set_global_binding(&symbol, value.clone());
            Ok(value)
        }
        "customize-set-variable" => {
            need_arg_range(name, args, 2, 3)?;
            let symbol = args[0].as_symbol()?;
            interp.set_custom_option(symbol, args[1].clone(), env)
        }
        "symbol-value" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            if let Some(value) = interp.lookup_var(symbol, env) {
                Ok(value)
            } else {
                let resolved = interp.resolve_variable_name(symbol)?;
                Err(LispError::Void(resolved))
            }
        }
        "default-value" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            interp
                .default_value(symbol)
                .ok_or_else(|| LispError::Void(symbol.to_string()))
        }
        "default-toplevel-value" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            interp
                .default_toplevel_value(symbol)
                .ok_or_else(|| LispError::Void(symbol.to_string()))
        }
        "set-default-toplevel-value" => {
            need_args(name, args, 2)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            let value = interp.prepare_variable_assignment(&symbol, args[1].clone())?;
            interp.notify_variable_watchers(&symbol, value.clone(), "set", None, env)?;
            interp.set_default_toplevel_value(&symbol, value.clone());
            Ok(value)
        }
        "symbol-plist" => {
            need_args(name, args, 1)?;
            Ok(interp.symbol_plist(args[0].as_symbol()?))
        }
        "setplist" => {
            need_args(name, args, 2)?;
            interp.set_symbol_plist(args[0].as_symbol()?, args[1].clone())
        }
        "interactive-form" => {
            need_args(name, args, 1)?;
            let mut value = resolve_callable(interp, &args[0], env)?;
            if let (Some(symbol), Some((file, _, _))) =
                (args[0].as_symbol().ok(), autoload_parts(&value))
            {
                interp.load_target(&file)?;
                value = interp.lookup_function(symbol, env)?;
            }
            Ok(interactive_form_items(&value)
                .map(Value::list)
                .unwrap_or(Value::Nil))
        }
        "autoloadp" => {
            need_args(name, args, 1)?;
            let autoload = autoload_parts(&args[0]).is_some();
            Ok(if autoload { Value::T } else { Value::Nil })
        }
        "macrop" => {
            need_args(name, args, 1)?;
            if let Ok(symbol) = args[0].as_symbol()
                && interp.has_macro_binding(symbol)
            {
                return Ok(Value::T);
            }
            let definition = super::call(interp, "indirect-function", &[args[0].clone()], env)?;
            let is_macro = if let Ok(items) = definition.to_vec() {
                matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "macro")
                    || autoload_is_macro(interp, args[0].as_symbol().ok(), &definition)
            } else {
                false
            };
            Ok(if is_macro { Value::T } else { Value::Nil })
        }
        "apropos-internal" => {
            need_arg_range(name, args, 1, 2)?;
            let pattern = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            regexp::validate_elisp_regex(&pattern.text)?;
            let regex = regexp::compile_elisp_regex(interp, &pattern, env, "", true)?;
            let predicate = args.get(1).cloned().filter(|value| !value.is_nil());
            let mut found = Vec::new();
            for symbol_name in interp.known_symbol_names() {
                if !regex
                    .is_match(&symbol_name)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                {
                    continue;
                }
                let symbol = Value::Symbol(symbol_name);
                if let Some(predicate) = &predicate {
                    let keep = interp.call_function_value(
                        predicate.clone(),
                        None,
                        std::slice::from_ref(&symbol),
                        env,
                    )?;
                    if !keep.is_truthy() {
                        continue;
                    }
                }
                found.push(symbol);
            }
            found.sort_by(|left, right| {
                left.as_symbol()
                    .unwrap_or("")
                    .cmp(right.as_symbol().unwrap_or(""))
            });
            Ok(Value::list(found))
        }
        "custom-autoload" => {
            need_arg_range(name, args, 2, 3)?;
            let symbol = args[0].as_symbol()?;
            let load = args[1].clone();
            let autoload_flag = if args.get(2).is_some_and(Value::is_truthy) {
                Value::Symbol("noset".into())
            } else {
                Value::T
            };
            interp.put_symbol_property(symbol, "custom-autoload", autoload_flag);

            let existing = interp
                .get_symbol_property(symbol, "custom-loads")
                .unwrap_or(Value::Nil);
            let already_present = existing
                .to_vec()
                .map(|items| items.iter().any(|item| item == &load))
                .unwrap_or(existing == load);
            if !already_present {
                interp.put_symbol_property(symbol, "custom-loads", Value::cons(load, existing));
            }
            Ok(Value::Nil)
        }
        "custom-set-variables" => {
            let mut result = Value::Nil;
            for entry in args {
                let items = entry.to_vec()?;
                if items.len() < 2 {
                    return Err(LispError::Signal("Incompatible Custom theme spec".into()));
                }
                let symbol = items[0].as_symbol()?.to_string();
                interp.put_symbol_property(&symbol, "saved-value", Value::list([items[1].clone()]));
                if items.get(2).is_some_and(Value::is_truthy) {
                    let value = interp.eval(&items[1], env)?;
                    result = interp.set_custom_option(&symbol, value, env)?;
                }
            }
            Ok(result)
        }
        "custom-add-to-group" => {
            need_args(name, args, 3)?;
            custom_add_to_group(
                interp,
                args[0].as_symbol()?,
                args[1].clone(),
                args[2].clone(),
            );
            Ok(Value::Nil)
        }
        "custom-current-group" => {
            need_args(name, args, 0)?;
            Ok(custom_current_group(interp).unwrap_or(Value::Nil))
        }
        "daemonp" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "documentation" => {
            need_args(name, args, 1)?;
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            Ok(function_documentation(interp, &value, env).unwrap_or(Value::Nil))
        }
        "documentation-property" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            Ok(interp
                .get_symbol_property(symbol, property)
                .unwrap_or(Value::Nil))
        }
        "get" | "function-get" => {
            need_arg_range(name, args, 2, 3)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            Ok(interp
                .get_symbol_property(symbol, property)
                .unwrap_or(Value::Nil))
        }
        "makunbound" => {
            need_args(name, args, 1)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            if symbol == "initial-window-system"
                || matches!(
                    symbol.as_str(),
                    "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum"
                )
                || symbol.starts_with(':')
            {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("setting-constant".into()),
                    Value::Symbol(symbol),
                ])));
            }
            if interp
                .buffer_local_value(interp.current_buffer_id(), &symbol)
                .is_some()
            {
                interp.notify_variable_watchers(
                    &symbol,
                    Value::Nil,
                    "makunbound",
                    if interp.is_auto_buffer_local(&symbol) {
                        None
                    } else {
                        Some(interp.current_buffer_id())
                    },
                    env,
                )?;
                interp.remove_buffer_local_value(interp.current_buffer_id(), &symbol);
            } else {
                interp.notify_variable_watchers(&symbol, Value::Nil, "makunbound", None, env)?;
                interp.remove_global_binding(&symbol);
            }
            Ok(Value::Symbol(symbol))
        }
        "lread--substitute-object-in-subtree" => {
            need_args(name, args, 3)?;
            substitute_object_in_subtree(interp, &args[0], &args[1], &args[2])?;
            Ok(Value::Nil)
        }
        "defvaralias" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let alias = args[0].as_symbol()?.to_string();
            let target = args[1].as_symbol()?.to_string();
            let alias_value = interp.lookup_var(&alias, env);
            let target_value = interp.lookup_var(&target, env);
            if !interp.variable_watchers(&alias).is_empty() {
                interp.notify_variable_watchers(
                    &alias,
                    Value::Symbol(target.clone()),
                    "defvaralias",
                    None,
                    env,
                )?;
                interp.clear_variable_watchers(&alias);
            }
            interp.set_variable_alias(&alias, &target)?;
            interp.remove_global_binding(&alias);
            interp.remove_buffer_local_value(interp.current_buffer_id(), &alias);
            if let Some(doc) = args.get(2).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(&alias, "variable-documentation", doc.clone());
            }
            if alias_value
                .as_ref()
                .zip(target_value.as_ref())
                .is_some_and(|(left, right)| left != right)
            {
                let warning = Value::list([
                    Value::Symbol("defvaralias".into()),
                    Value::Symbol("losing-value".into()),
                    Value::Symbol(alias.clone()),
                ]);
                call_named_function(interp, "display-warning", &[warning], env)?;
            }
            Ok(Value::Symbol(alias))
        }
        "define-obsolete-variable-alias" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let alias = args[0].as_symbol()?.to_string();
            let target = args[1].as_symbol()?.to_string();
            let alias_value = interp.lookup_var(&alias, env);
            let target_value = interp.lookup_var(&target, env);
            if !interp.variable_watchers(&alias).is_empty() {
                interp.notify_variable_watchers(
                    &alias,
                    Value::Symbol(target.clone()),
                    "defvaralias",
                    None,
                    env,
                )?;
                interp.clear_variable_watchers(&alias);
            }
            interp.set_variable_alias(&alias, &target)?;
            interp.remove_global_binding(&alias);
            interp.remove_buffer_local_value(interp.current_buffer_id(), &alias);
            if let Some(doc) = args.get(3).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(&alias, "variable-documentation", doc.clone());
            }
            interp.put_symbol_property(
                &alias,
                "byte-obsolete-variable",
                Value::list([Value::Symbol(target.clone()), Value::Nil, args[2].clone()]),
            );
            if alias_value
                .as_ref()
                .zip(target_value.as_ref())
                .is_some_and(|(left, right)| left != right)
            {
                let warning = Value::list([
                    Value::Symbol("defvaralias".into()),
                    Value::Symbol("losing-value".into()),
                    Value::Symbol(alias.clone()),
                ]);
                call_named_function(interp, "display-warning", &[warning], env)?;
            }
            Ok(Value::Symbol(alias))
        }
        "indirect-variable" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(Value::Symbol(interp.indirect_variable_name(symbol)?))
        }
        "internal-delete-indirect-variable" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            if !interp.remove_variable_alias(symbol) {
                return Err(LispError::Signal("Variable is not indirect".into()));
            }
            interp.remove_global_binding(symbol);
            interp.remove_buffer_local_value(interp.current_buffer_id(), symbol);
            interp.remove_symbol_property(symbol, "variable-documentation");
            Ok(Value::Symbol(symbol.to_string()))
        }
        "internal--define-uninitialized-variable" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            interp.mark_special_variable(symbol);
            if !args[1].is_nil() {
                interp.put_symbol_property(symbol, "variable-documentation", args[1].clone());
            }
            Ok(Value::Symbol(symbol.to_string()))
        }
        "defvar-1" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol = args[0].as_symbol()?;
            interp.mark_special_variable(symbol);
            if interp.lookup_var(symbol, env).is_none() {
                interp.set_variable(symbol, args[1].clone(), &mut Vec::new());
            }
            if let Some(doc) = args.get(2).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(symbol, "variable-documentation", doc.clone());
            }
            Ok(Value::Symbol(symbol.to_string()))
        }
        "defconst-1" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol = args[0].as_symbol()?;
            interp.mark_special_variable(symbol);
            interp.set_variable(symbol, args[1].clone(), &mut Vec::new());
            if let Some(doc) = args.get(2).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(symbol, "variable-documentation", doc.clone());
            }
            interp.put_symbol_property(symbol, "risky-local-variable", Value::T);
            Ok(Value::Symbol(symbol.to_string()))
        }
        "internal-make-var-non-special" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            interp.unmark_special_variable(symbol);
            Ok(Value::Symbol(symbol.to_string()))
        }
        "make-interpreted-closure" => {
            need_arg_range(name, args, 3, 5)?;
            let params = parse_lambda_params_value(&args[0])?;
            let body = args[1].to_vec()?;
            let captured_env = closure_env_from_alist(&args[2])?;
            let mut lambda_body = Vec::new();
            if let Some(doc) = args.get(3).filter(|value| !value.is_nil()) {
                lambda_body.push(doc.clone());
            }
            if let Some(spec) = args.get(4).filter(|value| !value.is_nil()) {
                if spec
                    .to_vec()
                    .ok()
                    .is_some_and(|items| matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "interactive"))
                {
                    lambda_body.push(spec.clone());
                } else {
                    lambda_body.push(Value::list([
                        Value::Symbol("interactive".into()),
                        spec.clone(),
                    ]));
                }
            }
            lambda_body.extend(body);
            Ok(Value::Lambda(params, lambda_body, shared_env(captured_env)))
        }
        "getenv" | "getenv-internal" => {
            need_args(name, args, 1)?;
            let variable = string_text(&args[0])?;
            let from_explicit_env = args.get(1).is_some_and(|value| !value.is_nil());
            let mut process_environment = args
                .get(1)
                .filter(|value| !value.is_nil())
                .cloned()
                .unwrap_or_else(|| {
                    interp
                        .lookup_var("process-environment", env)
                        .unwrap_or(Value::Nil)
                });
            if let Some((Value::Symbol(symbol), environment)) = process_environment.cons_values()
                && symbol == "environment"
            {
                process_environment = environment;
            }
            Ok(
                getenv_in_environment(&variable, &process_environment, from_explicit_env)?
                    .unwrap_or(Value::Nil),
            )
        }
        "set-language-environment" => {
            need_args(name, args, 1)?;
            let language = if args[0].is_nil() {
                "English".to_string()
            } else if let Ok(symbol) = args[0].as_symbol() {
                symbol.to_string()
            } else {
                string_text(&args[0])?
            };
            let value = Value::String(language);
            interp.set_global_binding("current-language-environment", value.clone());
            Ok(value)
        }
        "setenv" => {
            need_arg_range(name, args, 1, 3)?;
            let variable = string_text(&args[0])?;
            if variable.contains('=') {
                return Err(LispError::Signal(format!(
                    "Environment variable name `{variable}` contains `='"
                )));
            }

            let mut value = args
                .get(1)
                .filter(|value| !value.is_nil())
                .map(string_text)
                .transpose()?;
            if let Some(text) = value.as_mut()
                && args.get(2).is_some_and(Value::is_truthy)
            {
                *text = substitute_in_file_name(text);
            }

            let mut process_environment = interp
                .lookup_var("process-environment", env)
                .unwrap_or(Value::Nil);
            let wrapped_environment = matches!(
                process_environment.cons_values(),
                Some((Value::Symbol(ref symbol), _)) if symbol == "environment"
            );
            if wrapped_environment && let Some((_, environment)) = process_environment.cons_values()
            {
                process_environment = environment;
            }

            let mut entries = process_environment_entries(&process_environment)?;
            setenv_in_environment_entries(&mut entries, &variable, value.as_deref(), true);
            let updated = process_environment_from_entries(&entries);
            interp.set_variable(
                "process-environment",
                if wrapped_environment {
                    Value::cons(Value::Symbol("environment".into()), updated)
                } else {
                    updated
                },
                env,
            );

            unsafe {
                if let Some(value) = value.as_deref() {
                    std::env::set_var(&variable, value);
                } else {
                    std::env::remove_var(&variable);
                }
            }
            Ok(value.map(Value::String).unwrap_or(Value::Nil))
        }
        "ignore" => Ok(Value::Nil),
        // Load-time compatibility shims for upstream Lisp helpers whose exact
        // side effects are not needed by the currently exercised batch paths.
        "purecopy" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "help--docstring-quote" => {
            need_args(name, args, 1)?;
            let text = string_text(&args[0])?;
            Ok(Value::String(
                text.chars()
                    .flat_map(|ch| match ch {
                        '\'' | '`' | '\u{2018}' | '\u{2019}' => vec!['\\', '=', ch],
                        _ => vec![ch],
                    })
                    .collect(),
            ))
        }
        "help-add-fundoc-usage" => {
            need_args(name, args, 2)?;
            Ok(args[0].clone())
        }
        "pcase--mutually-exclusive-p" => {
            need_args(name, args, 2)?;
            Ok(Value::Nil)
        }
        "make-obsolete" => {
            need_arg_range(name, args, 3, 4)?;
            let obsolete_name = obsolete_definition_symbol(&args[0])?;
            Ok(Value::Symbol(obsolete_name.to_string()))
        }
        "define-obsolete-face-alias" | "define-obsolete-function-alias" => Ok(Value::Nil),
        "make-obsolete-variable" => {
            need_arg_range(name, args, 3, 4)?;
            let obsolete_name = obsolete_definition_symbol(&args[0])?;
            let access_type = args.get(3).cloned().unwrap_or(Value::Nil);
            interp.put_symbol_property(
                obsolete_name,
                "byte-obsolete-variable",
                Value::list([args[1].clone(), access_type, args[2].clone()]),
            );
            Ok(Value::Symbol(obsolete_name.to_string()))
        }
        "macroexp-warn-and-return" => Ok(args.get(1).cloned().unwrap_or(Value::Nil)),
        "describe-function" => {
            let _ = get_or_create_buffer(interp, "*Help*");
            Ok(Value::Nil)
        }
        "macroexp-quote" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(_, _) | Value::Symbol(_) => {
                    Value::list([Value::Symbol("quote".into()), args[0].clone()])
                }
                other => other.clone(),
            })
        }
        "macroexp-progn" => {
            need_args(name, args, 1)?;
            let forms = args[0].to_vec().unwrap_or_default();
            Ok(match forms.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                many => Value::list(
                    std::iter::once(Value::Symbol("progn".into())).chain(many.iter().cloned()),
                ),
            })
        }
        "macroexp-compiling-p" | "macroexp--dynamic-variable-p" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::Nil)
        }
        "macroexpand" | "macroexpand-1" | "macroexpand-all" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let environment = args.get(1).filter(|value| value.is_truthy());
            if name == "macroexpand-all" {
                interp.macroexpand_all_form_with_environment(&args[0], environment, env)
            } else {
                interp.macroexpand_1_form_with_environment(&args[0], environment, env)
            }
        }
        "run-at-time" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            interp.schedule_timer(args[2].clone(), args[3..].to_vec());
            Ok(Value::String("#<timer>".into()))
        }
        "run-with-timer" | "run-with-idle-timer" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            interp.schedule_timer(args[2].clone(), args[3..].to_vec());
            Ok(Value::String("#<timer>".into()))
        }
        "cancel-timer" => Ok(Value::Nil),
        "timerp" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::String(text) if text == "#<timer>")
                    || matches!(&args[0], Value::StringObject(state) if state.borrow().text == "#<timer>")
                    || matches!(&args[0], Value::Record(id) if interp.find_record(*id).is_some_and(|record| record.type_name == "timer"))
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "lossage-size" => {
            if args.is_empty() {
                return Ok(Value::Integer(interp.lossage_size));
            }
            let new_size = args[0].as_integer()?;
            if new_size < 100 {
                return Err(LispError::Signal("lossage-size must be >= 100".into()));
            }
            interp.lossage_size = new_size;
            Ok(Value::Integer(new_size))
        }
        "executable-find" => {
            need_args(name, args, 1)?;
            let executable = string_text(&args[0])?;
            Ok(find_executable(&executable)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "add-hook" => {
            need_args(name, args, 2)?;
            let hook_name = args[0].as_symbol()?.to_string();
            let function = args[1].clone();
            let append = args.get(2).is_some_and(|value| {
                value.is_truthy() && !matches!(value, Value::Symbol(symbol) if symbol == ":local")
            });
            let local = args
                .get(2)
                .is_some_and(|value| matches!(value, Value::Symbol(symbol) if symbol == ":local"))
                || args.get(3).is_some_and(|value| value.is_truthy());
            let mut hooks = if local {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .unwrap_or_default()
            } else {
                interp
                    .lookup_var(&hook_name, env)
                    .map(|value| value.to_vec().unwrap_or_default())
                    .unwrap_or_default()
            };
            if !hooks.contains(&function) {
                if append {
                    hooks.push(function);
                } else {
                    hooks.insert(0, function);
                }
            }
            if hook_name == "post-self-insert-hook" {
                hooks.sort_by_key(post_self_insert_hook_depth);
            }
            if local {
                interp.set_buffer_local_hook(interp.current_buffer_id(), &hook_name, hooks);
            } else {
                interp.set_variable(&hook_name, Value::list(hooks), &mut Vec::new());
            }
            Ok(Value::Nil)
        }
        "run-hooks" | "run-mode-hooks" => {
            for hook in args {
                if let Ok(hook_name) = hook.as_symbol() {
                    run_named_hooks(interp, hook_name, env, Some(interp.current_buffer_id()))?;
                }
            }
            Ok(Value::Nil)
        }
        "run-hook-with-args" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let hook_name = args[0].as_symbol()?;
            for hook in hook_values(interp, hook_name, env, Some(interp.current_buffer_id())) {
                call_function_value(interp, &hook, &args[1..], env)?;
            }
            Ok(Value::Nil)
        }
        "run-hook-with-args-until-success" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let hook_name = args[0].as_symbol()?;
            for hook in hook_values(interp, hook_name, env, Some(interp.current_buffer_id())) {
                let result = call_function_value(interp, &hook, &args[1..], env)?;
                if result.is_truthy() {
                    return Ok(result);
                }
            }
            Ok(Value::Nil)
        }
        "run-hook-with-args-until-failure" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let hook_name = args[0].as_symbol()?;
            for hook in hook_values(interp, hook_name, env, Some(interp.current_buffer_id())) {
                let result = call_function_value(interp, &hook, &args[1..], env)?;
                if result.is_nil() {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }
        "eval-after-load" => Ok(Value::Nil),
        "run-hook-wrapped" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let hook_name = args[0].as_symbol()?;
            let wrapper = resolve_callable(interp, &args[1], env)?;
            let hook_values = interp
                .lookup_var(hook_name, env)
                .map(|value| value.to_vec().unwrap_or_default())
                .unwrap_or_default();
            for hook in hook_values {
                let mut wrapper_args = vec![hook];
                wrapper_args.extend_from_slice(&args[2..]);
                let value =
                    interp.call_function_value(wrapper.clone(), None, &wrapper_args, env)?;
                if value.is_truthy() {
                    return Ok(value);
                }
            }
            Ok(Value::Nil)
        }
        "ert-simulate-command" => ert_simulate_command(interp, args, env),
        "mapatoms" => {
            need_arg_range(name, args, 1, 2)?;
            let callback = resolve_callable(interp, &args[0], env)?;
            let obarray = args.get(1).cloned().unwrap_or(Value::Nil);
            let symbols = if obarray.is_nil() {
                interp
                    .known_symbol_names()
                    .into_iter()
                    .map(Value::Symbol)
                    .collect()
            } else {
                obarray_symbols(interp, &obarray)?
            };
            for symbol in symbols {
                interp.call_function_value(
                    callback.clone(),
                    args[0].as_symbol().ok(),
                    &[symbol],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "remove-hook" => {
            need_args(name, args, 2)?;
            let hook_name = args[0].as_symbol()?.to_string();
            let function = args[1].clone();
            let local = args
                .get(2)
                .is_some_and(|value| matches!(value, Value::Symbol(symbol) if symbol == ":local"))
                || args.get(3).is_some_and(|value| value.is_truthy());
            let mut hooks = if local {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .unwrap_or_default()
            } else {
                interp
                    .lookup_var(&hook_name, env)
                    .map(|value| value.to_vec().unwrap_or_default())
                    .unwrap_or_default()
            };
            hooks.retain(|hook| hook != &function);
            if local {
                interp.set_buffer_local_hook(interp.current_buffer_id(), &hook_name, hooks);
            } else {
                interp.set_variable(&hook_name, Value::list(hooks), &mut Vec::new());
            }
            Ok(Value::Nil)
        }
        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

fn post_self_insert_hook_depth(hook: &Value) -> i32 {
    match hook {
        Value::Symbol(name) if name == "electric-layout-post-self-insert-function" => 40,
        Value::Symbol(name)
            if name == "electric-pair-post-self-insert-function"
                || name == "electric-pair-open-newline-between-pairs-psif" =>
        {
            50
        }
        Value::Symbol(name) if name == "electric-indent-post-self-insert-function" => 60,
        _ => 50,
    }
}

fn obsolete_definition_symbol(value: &Value) -> Result<&str, LispError> {
    match value {
        Value::Symbol(name) => Ok(name),
        _ => Err(LispError::TypeError("symbol".into(), value.type_name())),
    }
}
