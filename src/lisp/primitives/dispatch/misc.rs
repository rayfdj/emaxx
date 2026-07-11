use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "read"
            | "read-positioning-symbols"
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
            | "define-mail-user-agent"
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
            | "byte-run--unescaped-character-literals-warning"
            | "purecopy"
            | "help--docstring-quote"
            | "help-add-fundoc-usage"
            | "pcase--mutually-exclusive-p"
            | "make-obsolete"
            | "make-obsolete-variable"
            | "define-obsolete-face-alias"
            | "define-obsolete-function-alias"
            | "macroexp-warn-and-return"
            | "cl--generic-method-files"
            | "cl--generic-describe"
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
        "read-positioning-symbols" => {
            need_arg_range(name, args, 0, 1)?;
            let source = args
                .first()
                .cloned()
                .or_else(|| interp.lookup_var("standard-input", env))
                .unwrap_or(Value::Nil);
            read_positioning_symbols_from_lisp_source(interp, &source, env)
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
            let mut reader = crate::lisp::reader::Reader::with_raw_quote_symbols(&slice);
            match reader.read()? {
                Some(val) => {
                    let consumed = slice[..reader.position()].chars().count();
                    let resolved = crate::lisp::reader::resolve_circular_read_syntax(val)?;
                    let materialized = materialize_read_hash_table_literals(interp, &resolved)?;
                    Ok(Value::cons(
                        materialized,
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
                // GNU keeps non-list DATA as the cdr: (signal 'foo 4)
                // is caught as the dotted pair (foo . 4).
                Value::cons(condition, data)
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
        "define-mail-user-agent" => {
            need_arg_range(name, args, 3, 5)?;
            let symbol = args[0].as_symbol()?;
            interp.put_symbol_property(symbol, "composefunc", args[1].clone());
            interp.put_symbol_property(symbol, "sendfunc", args[2].clone());
            interp.put_symbol_property(
                symbol,
                "abortfunc",
                args.get(3)
                    .filter(|value| value.is_truthy())
                    .cloned()
                    .unwrap_or_else(|| Value::Symbol("kill-buffer".into())),
            );
            interp.put_symbol_property(
                symbol,
                "hookvar",
                args.get(4)
                    .filter(|value| value.is_truthy())
                    .cloned()
                    .unwrap_or_else(|| Value::Symbol("mail-send-hook".into())),
            );
            Ok(args[0].clone())
        }
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
                    Ok(crate::lisp::types::interned_symbol_value(symbol_name))
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
            // The visible number comes from the `gensym-counter' variable so
            // callers can rebind it; the uninterned identity stays unique.
            let counter = interp
                .lookup_var("gensym-counter", env)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or_else(|| GENSYM_COUNTER.load(AtomicOrdering::Relaxed) as i64);
            interp.set_variable("gensym-counter", Value::Integer(counter + 1), env);
            let id = GENSYM_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let visible = format!("{prefix}{counter}");
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
            if crate::lisp::primitives::prefer_builtin_override(&function) {
                interp.push_function_binding(&function, Value::BuiltinFunc(function.clone()));
                return Ok(Value::Symbol(function));
            }
            // GNU `autoload' does nothing when FUNCTION already has a real
            // (non-autoload) definition; subrs count as definitions, so the
            // cl-loaddefs autoloads must not shadow emaxx's native cl-*
            // primitives either.
            if let Ok(existing) = interp.lookup_function(&function, env)
                && crate::lisp::primitives::autoload_parts(&existing).is_none()
            {
                return Ok(Value::Symbol(function));
            }
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
            interp.symbol_value_cell(symbol)
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
            // GNU returns nil for an unbound symbol (advice.el probes the
            // interactive form of not-yet-defined advice functions).
            let Ok(mut value) = resolve_callable(interp, &args[0], env) else {
                return Ok(Value::Nil);
            };
            // GNU's C interactive_form consults `oclosure-interactive-form'
            // for OClosures (nadvice's advice objects compose their spec);
            // it outranks the defun-recorded property for advised symbols,
            // but an (interactive ...) form IN THE BODY outranks the
            // generic (oclosure-lambda bodies may carry their own spec).
            if super::misc_keymaps::oclosure_type_of(&value).is_some()
                && interp.has_lisp_function("oclosure-interactive-form")
            {
                if let Some(items) = interactive_form_items(&value) {
                    return Ok(Value::list(items));
                }
                return interp.call_function_value(
                    Value::Symbol("oclosure-interactive-form".into()),
                    Some("oclosure-interactive-form"),
                    std::slice::from_ref(&value),
                    env,
                );
            }
            if let Ok(symbol) = args[0].as_symbol()
                && let Some(form) = interp.get_symbol_property(symbol, "interactive-form")
            {
                return Ok(form);
            }
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
            // An unbound symbol is simply not a macro.
            let Ok(definition) = super::call(interp, "indirect-function", &[args[0].clone()], env)
            else {
                return Ok(Value::Nil);
            };
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
            if let Some(documentation) = function_documentation(interp, &args[0], env) {
                return Ok(documentation);
            }
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            if let Some(documentation) = function_documentation(interp, &value, env) {
                return Ok(documentation);
            }
            // Fall back to the version's DOC file for builtins that carry no
            // native docstring (e.g. C primitives documented in etc/DOC), then
            // to the docstrings inline in the GNU lisp sources on the load path
            // (many subr.el/files.el functions are implemented natively here and
            // have no lambda body to read the docstring from).
            if let Value::Symbol(sym) = &args[0] {
                if let Some(doc) = builtin_doc_from_doc_file(sym) {
                    return Ok(Value::String(doc));
                }
                if let Some(doc) = builtin_doc_from_lisp_sources(sym) {
                    return Ok(Value::String(doc));
                }
            }
            Ok(Value::Nil)
        }
        "documentation-property" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            Ok(interp
                .get_symbol_property(symbol, property)
                .unwrap_or(Value::Nil))
        }
        "get" => {
            need_arg_range(name, args, 2, 3)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            Ok(interp
                .get_symbol_property(symbol, property)
                .unwrap_or(Value::Nil))
        }
        "function-get" => {
            need_arg_range(name, args, 2, 3)?;
            let mut symbol = args[0].as_symbol()?.to_string();
            let property = args[1].as_symbol()?;
            // GNU follows defalias chains until a non-nil property is found
            // ((function-get 'not 'side-effect-free) reads null's — unsafep).
            let mut hops = 0;
            loop {
                if let Some(value) = interp.get_symbol_property(&symbol, property)
                    && !value.is_nil()
                {
                    return Ok(value);
                }
                hops += 1;
                if hops > 10 {
                    return Ok(Value::Nil);
                }
                match interp.raw_function_binding(&symbol, env) {
                    Some(Value::Symbol(next)) if next != symbol => symbol = next,
                    _ => return Ok(Value::Nil),
                }
            }
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
            // GNU: (SYMBOL &optional DOC) — cus-start.el passes one arg.
            need_arg_range(name, args, 1, 2)?;
            let symbol = args[0].as_symbol()?;
            interp.mark_special_variable(symbol);
            if let Some(doc) = args.get(1).filter(|value| !value.is_nil()) {
                interp.put_symbol_property(symbol, "variable-documentation", doc.clone());
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
        "byte-run--unescaped-character-literals-warning" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
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
        "define-obsolete-face-alias" => Ok(Value::Nil),
        "define-obsolete-function-alias" => {
            // GNU byte-run.el: (defalias OBSOLETE CURRENT DOC) +
            // (make-obsolete ...); the alias must actually be installed
            // (rx.el aliases rx-submatch-n to rx-to-string).
            need_arg_range(name, args, 2, 4)?;
            let obsolete = obsolete_definition_symbol(&args[0])?.to_string();
            // defalias is a special form: eval a quoted (defalias 'OLD
            // 'NEW DOC) form rather than dispatching it as a primitive.
            let doc = args.get(3).cloned().unwrap_or(Value::Nil);
            let defalias_form = Value::list([
                Value::Symbol("defalias".into()),
                Value::list([Value::Symbol("quote".into()), args[0].clone()]),
                Value::list([Value::Symbol("quote".into()), args[1].clone()]),
                Value::list([Value::Symbol("quote".into()), doc]),
            ]);
            interp.eval(&defalias_form, env)?;
            let mut make_obsolete = vec![args[0].clone(), args[1].clone()];
            if let Some(when) = args.get(2) {
                make_obsolete.push(when.clone());
            }
            let _ = super::call(interp, "make-obsolete", &make_obsolete, env);
            Ok(Value::Symbol(obsolete))
        }
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
        "cl--generic-method-files" => {
            need_args(name, args, 1)?;
            let method_name = args[0].as_symbol()?;
            Ok(Value::list(cl_generic_method_file_entries(
                interp,
                env,
                method_name,
            )))
        }
        "cl--generic-describe" => {
            need_args(name, args, 1)?;
            let method_name = args[0].as_symbol()?;
            for entry in cl_generic_method_file_entries(interp, env, method_name) {
                let Some((_file, method)) = entry.cons_values() else {
                    continue;
                };
                let rendered = render_prin1(interp, &method, env)?;
                interp.insert_current_buffer(&rendered);
                interp.insert_current_buffer("\n");
            }
            Ok(Value::Nil)
        }
        "describe-function" => {
            need_args(name, args, 1)?;
            let _ = get_or_create_buffer(interp, "*Help*");
            let mut docs = Vec::new();
            let target = args[0].as_symbol().ok();
            if let Some(symbol) = target {
                if let Some(doc) =
                    interp.get_symbol_property(symbol, "emaxx-cl-defgeneric-documentation")
                {
                    docs.push(string_text(&doc)?);
                } else if let Some(doc) = function_documentation(interp, &args[0], env) {
                    docs.push(string_text(&doc)?);
                }
                if let Some(method_docs) =
                    interp.get_symbol_property(symbol, "emaxx-cl-defmethod-documentation")
                    && let Ok(items) = method_docs.to_vec()
                {
                    for doc in items {
                        docs.push(string_text(&doc)?);
                    }
                }
            } else if let Some(doc) = function_documentation(interp, &args[0], env) {
                docs.push(string_text(&doc)?);
            }
            Ok(Value::String(docs.join("\n")))
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
                // GNU macroexpand-all dynamically binds
                // `macroexpand-all-environment' around the expansion so env
                // expanders like cl--labels-convert can read it back.  Only
                // bind it for environments carrying a `function' expander
                // (cl-flet/cl-labels): binding it unconditionally makes
                // expander sets like bindat's re-read the variable from their
                // helpers and re-expand already-processed type specs forever.
                let has_function_expander = environment
                    .and_then(|value| value.to_vec().ok())
                    .is_some_and(|entries| {
                        entries.iter().any(|entry| {
                            matches!(
                                entry.car(),
                                // cl-flet/cl-labels use a `function'
                                // expander; rx-let/rx-let-eval carry
                                // `:rx-locals' that the rx macro reads back.
                                Ok(Value::Symbol(head)) if head == "function" || head == ":rx-locals"
                            )
                        })
                    });
                let previous = if has_function_expander {
                    let previous = interp.global_binding_value("macroexpand-all-environment");
                    interp.set_global_binding(
                        "macroexpand-all-environment",
                        environment.cloned().unwrap_or(Value::Nil),
                    );
                    Some(previous)
                } else {
                    None
                };
                let result =
                    interp.macroexpand_all_form_with_environment(&args[0], environment, env);
                if let Some(previous) = previous {
                    match previous {
                        Some(value) => {
                            interp.set_global_binding("macroexpand-all-environment", value)
                        }
                        None => interp.remove_global_binding("macroexpand-all-environment"),
                    }
                }
                result
            } else if name == "macroexpand-1" {
                interp.macroexpand_1_form_with_environment(&args[0], environment, env)
            } else {
                // `macroexpand' repeats until the head is no longer a macro.
                let mut form = args[0].clone();
                loop {
                    let expanded =
                        interp.macroexpand_1_form_with_environment(&form, environment, env)?;
                    if expanded == form {
                        break;
                    }
                    form = expanded;
                }
                Ok(form)
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
            // GNU timer.el: timers are plain 10-slot vectors.
            let vector_timer = is_vector_value(&args[0])
                && vector_items(&args[0]).is_ok_and(|items| items.len() == 10);
            Ok(
                if vector_timer
                    || matches!(&args[0], Value::String(text) if text == "#<timer>")
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
            if name == "run-mode-hooks"
                && interp
                    .lookup_var("delay-mode-hooks", env)
                    .is_some_and(|value| value.is_truthy())
            {
                let mut delayed = interp
                    .lookup_var("delayed-mode-hooks", env)
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                delayed.extend(args.iter().cloned());
                interp.set_variable("delayed-mode-hooks", Value::list(delayed), env);
                return Ok(Value::Nil);
            }
            for hook in args {
                if let Ok(hook_name) = hook.as_symbol() {
                    run_named_hooks(interp, hook_name, env, Some(interp.current_buffer_id()))?;
                }
            }
            if name == "run-mode-hooks" {
                let delayed = interp
                    .lookup_var("delayed-mode-hooks", env)
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                if !delayed.is_empty() {
                    interp.set_variable("delayed-mode-hooks", Value::Nil, env);
                    for hook in delayed {
                        if let Ok(hook_name) = hook.as_symbol() {
                            run_named_hooks(
                                interp,
                                hook_name,
                                env,
                                Some(interp.current_buffer_id()),
                            )?;
                        }
                    }
                }
                let mut after_hooks = interp
                    .lookup_var("delayed-after-hook-functions", env)
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                if !after_hooks.is_empty() {
                    interp.set_variable("delayed-after-hook-functions", Value::Nil, env);
                    after_hooks.reverse();
                    for hook in after_hooks {
                        call_function_value(interp, &hook, &[], env)?;
                    }
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

fn cl_generic_method_file_entries(
    interp: &Interpreter,
    env: &Env,
    method_name: &str,
) -> Vec<Value> {
    let load_history = interp.lookup_var("load-history", env).unwrap_or(Value::Nil);
    let mut result = Vec::new();
    for load_entry in load_history.to_vec().unwrap_or_default() {
        let Some((file, defs)) = load_entry.cons_values() else {
            continue;
        };
        for def in defs.to_vec().unwrap_or_default() {
            let Ok(parts) = def.to_vec() else {
                continue;
            };
            if matches!(parts.first(), Some(Value::Symbol(kind)) if kind == "cl-defmethod")
                && matches!(parts.get(1), Some(Value::Symbol(method)) if method == method_name)
            {
                result.push(Value::cons(file.clone(), Value::list(parts[1..].to_vec())));
            }
        }
    }
    result
}

fn obsolete_definition_symbol(value: &Value) -> Result<&str, LispError> {
    match value {
        Value::Symbol(name) => Ok(name),
        _ => Err(LispError::TypeError("symbol".into(), value.type_name())),
    }
}

/// A lazily-populated docstring cache: the source path it was built from and a
/// shared name → docstring map.
type DocSourceCache = Option<(
    String,
    std::rc::Rc<std::collections::HashMap<String, String>>,
)>;

thread_local! {
    // Cache of (DOC-file-path -> {function-name -> docstring}) parsed lazily.
    static DOC_FILE_CACHE: std::cell::RefCell<DocSourceCache> =
        const { std::cell::RefCell::new(None) };
}

/// Look up FUNCTION's docstring in the version's `DOC` file (the same file GNU
/// Emacs distributes in its data directory).  Returns `None` when the DOC file
/// cannot be located or has no entry for the function.
fn builtin_doc_from_doc_file(function: &str) -> Option<String> {
    let etc_dir = crate::lisp::primitives::compat_data_directory()?;
    let path = std::path::Path::new(&etc_dir).join("DOC");
    let path_str = path.to_string_lossy().to_string();

    let map = DOC_FILE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((cached_path, cached_map)) = cache.as_ref()
            && *cached_path == path_str
        {
            return Some(cached_map.clone());
        }
        let bytes = std::fs::read(&path).ok()?;
        let parsed = std::rc::Rc::new(parse_doc_file(&bytes));
        *cache = Some((path_str.clone(), parsed.clone()));
        Some(parsed)
    })?;

    map.get(function).cloned()
}

thread_local! {
    // Cache of (lisp-root-path -> {function-name -> docstring}) scanned lazily.
    static LISP_SOURCE_DOC_CACHE: std::cell::RefCell<DocSourceCache> =
        const { std::cell::RefCell::new(None) };
}

/// Look up FUNCTION's docstring in the GNU lisp sources on the load path.
///
/// The version's `lisp/` tree sits next to the data directory.  Many functions
/// (subr.el, files.el, simple.el, …) are implemented natively in emaxx and so
/// have no lambda body to read a docstring from, yet their docstrings are not in
/// the `DOC` file either — they live inline in the byte-compiled sources.  We
/// scan the `.el` sources once and cache a name → first-docstring map.
fn builtin_doc_from_lisp_sources(function: &str) -> Option<String> {
    let etc_dir = crate::lisp::primitives::compat_data_directory()?;
    let lisp_root = std::path::Path::new(&etc_dir).parent()?.join("lisp");
    let root_str = lisp_root.to_string_lossy().to_string();

    let map = LISP_SOURCE_DOC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((cached_root, cached_map)) = cache.as_ref()
            && *cached_root == root_str
        {
            return Some(cached_map.clone());
        }
        if !lisp_root.is_dir() {
            return None;
        }
        let mut map = std::collections::HashMap::new();
        scan_lisp_dir_for_docstrings(&lisp_root, &mut map);
        let parsed = std::rc::Rc::new(map);
        *cache = Some((root_str.clone(), parsed.clone()));
        Some(parsed)
    })?;

    map.get(function).cloned()
}

/// Recursively walk DIR collecting the first docstring of every top-level
/// `defun`/`defmacro`/`defsubst`/`define-inline`/`cl-defun`/`cl-defmacro` form
/// in each `.el` file into MAP (first definition wins).
fn scan_lisp_dir_for_docstrings(
    dir: &std::path::Path,
    map: &mut std::collections::HashMap<String, String>,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_lisp_dir_for_docstrings(&path, map);
        } else if path.extension().is_some_and(|ext| ext == "el")
            && let Ok(text) = std::fs::read_to_string(&path)
        {
            parse_el_source_docstrings(&text, map);
        }
    }
}

/// Extract `(NAME . docstring)` pairs from a single `.el` source's top-level
/// definition forms.  A top-level form begins at column 0 with `(`.
fn parse_el_source_docstrings(text: &str, map: &mut std::collections::HashMap<String, String>) {
    const HEADS: [&str; 6] = [
        "defun",
        "defmacro",
        "defsubst",
        "define-inline",
        "cl-defun",
        "cl-defmacro",
    ];
    let bytes = text.as_bytes();
    for line_start in line_starts(text) {
        let rest = &text[line_start..];
        // Only column-0 open-paren forms are top-level definitions.
        if !rest.starts_with('(') {
            continue;
        }
        let after_paren = &rest[1..];
        let Some(head) = HEADS.iter().find(|head| {
            after_paren.starts_with(**head) && is_symbol_boundary(after_paren, head.len())
        }) else {
            continue;
        };
        let mut idx = line_start + 1 + head.len();
        idx = skip_ws(bytes, idx);
        let name_start = idx;
        while idx < bytes.len() && is_lisp_symbol_byte(bytes[idx]) {
            idx += 1;
        }
        if idx == name_start {
            continue;
        }
        let name = &text[name_start..idx];
        idx = skip_ws(bytes, idx);
        // Skip the arglist `(...)`.
        if idx >= bytes.len() || bytes[idx] != b'(' {
            continue;
        }
        idx = match skip_balanced_parens(bytes, idx) {
            Some(next) => next,
            None => continue,
        };
        idx = skip_ws(bytes, idx);
        // The docstring, if present, is the next form and starts with `"`.
        if idx >= bytes.len() || bytes[idx] != b'"' {
            continue;
        }
        if let Some(doc) = read_lisp_string(bytes, idx) {
            map.entry(name.to_string()).or_insert(doc);
        }
    }
}

/// Byte offsets of the start of each line in TEXT.
fn line_starts(text: &str) -> impl Iterator<Item = usize> + '_ {
    std::iter::once(0).chain(
        text.match_indices('\n')
            .map(|(index, _)| index + 1)
            .filter(move |index| *index < text.len()),
    )
}

fn is_symbol_boundary(text: &str, offset: usize) -> bool {
    text.as_bytes()
        .get(offset)
        .is_none_or(|b| !is_lisp_symbol_byte(*b))
}

fn is_lisp_symbol_byte(b: u8) -> bool {
    !b.is_ascii_whitespace() && !matches!(b, b'(' | b')' | b'"' | b';' | b'\'' | b'`' | b',')
}

fn skip_ws(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

/// Given IDX at an opening `(`, return the offset just past the matching `)`,
/// honoring string literals and character/escape syntax.
fn skip_balanced_parens(bytes: &[u8], mut idx: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_string = false;
    while idx < bytes.len() {
        let b = bytes[idx];
        if in_string {
            match b {
                b'\\' => idx += 1,
                b'"' => in_string = false,
                _ => {}
            }
        } else {
            match b {
                b'"' => in_string = true,
                b'?' => idx += 1, // character literal: skip the next byte
                b';' => {
                    while idx < bytes.len() && bytes[idx] != b'\n' {
                        idx += 1;
                    }
                    continue;
                }
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(idx + 1);
                    }
                }
                _ => {}
            }
        }
        idx += 1;
    }
    None
}

/// Read a Lisp string literal that starts at IDX (a `"`), returning the decoded
/// contents (with `\"` and `\\` unescaped; other escapes kept verbatim).
fn read_lisp_string(bytes: &[u8], mut idx: usize) -> Option<String> {
    idx += 1; // skip opening quote
    let mut out = Vec::new();
    while idx < bytes.len() {
        match bytes[idx] {
            b'\\' if idx + 1 < bytes.len() => {
                let next = bytes[idx + 1];
                match next {
                    b'"' | b'\\' => out.push(next),
                    _ => {
                        out.push(b'\\');
                        out.push(next);
                    }
                }
                idx += 2;
            }
            b'"' => return Some(String::from_utf8_lossy(&out).to_string()),
            b => {
                out.push(b);
                idx += 1;
            }
        }
    }
    None
}

/// Parse a GNU Emacs `DOC` file into a map from function name to docstring.
///
/// Entries are separated by the `\x1f` (unit-separator) byte and prefixed by a
/// type tag: `F` for functions, `V` for variables, `S` for source-file markers.
/// Only function entries are collected here.  The stored docstring keeps the
/// trailing `(fn ...)` usage line exactly as GNU stores it.
fn parse_doc_file(bytes: &[u8]) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for chunk in bytes.split(|&b| b == 0x1f) {
        // Each chunk is `<tag><name>\n<doc>`; skip empties and non-function tags.
        let Some((&tag, rest)) = chunk.split_first() else {
            continue;
        };
        if tag != b'F' {
            continue;
        }
        let Some(newline) = rest.iter().position(|&b| b == b'\n') else {
            continue;
        };
        let name = String::from_utf8_lossy(&rest[..newline]).to_string();
        let doc = String::from_utf8_lossy(&rest[newline + 1..]).to_string();
        map.entry(name).or_insert(doc);
    }
    map
}
