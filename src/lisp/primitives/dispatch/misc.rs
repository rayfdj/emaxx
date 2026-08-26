use super::*;

fn fringe_bitmap_id(interp: &Interpreter, name: &str) -> Option<i64> {
    interp
        .fringe_bitmap_states
        .iter()
        .find(|bitmap| bitmap.name == name)
        .map(|bitmap| bitmap.id)
}

fn fringe_bits_length(value: &Value) -> Result<usize, LispError> {
    if is_vector_value(value) {
        return Ok(vector_items(value)?.len());
    }
    if let Some(string) = string_like(value) {
        return Ok(string.text.chars().count());
    }
    Err(wrong_type_argument("arrayp", value.clone()))
}

fn fringe_fixnum(value: &Value) -> Result<i64, LispError> {
    match value {
        Value::Integer(value) => Ok(*value),
        _ => Err(wrong_type_argument("fixnump", value.clone())),
    }
}

fn fringe_symbol_name(value: &Value) -> Result<String, LispError> {
    value
        .as_symbol()
        .map(str::to_string)
        .map_err(|_| wrong_type_argument("symbolp", value.clone()))
}

fn define_fringe_bitmap(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
) -> Result<Value, LispError> {
    let name = fringe_symbol_name(&args[0])?;
    let bits_length = fringe_bits_length(&args[1])?;
    let height = match args.get(2).filter(|value| !value.is_nil()) {
        Some(value) => fringe_fixnum(value)?.clamp(0, 255),
        None => bits_length.min(255) as i64,
    };
    let width = match args.get(3).filter(|value| !value.is_nil()) {
        Some(value) => {
            let width = fringe_fixnum(value)?;
            if !(1..=16).contains(&width) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("args-out-of-range"),
                    value.clone(),
                    Value::String("Width must be from 1 to 16".into()),
                ])));
            }
            width
        }
        None => 8,
    };
    let mut align = args.get(4).cloned().unwrap_or(Value::Nil);
    let mut periodic = false;
    if let Some((head, tail)) = align.cons_values() {
        if let Some((period, _)) = tail.cons_values() {
            periodic = period.is_truthy();
        }
        align = head;
    }
    if !align.is_nil()
        && !matches!(
            align,
            Value::Symbol(ref name) if matches!(name.as_str(), "top" | "center" | "bottom")
        )
    {
        return Err(LispError::Signal("Bad align argument".into()));
    }

    let existing_id = fringe_bitmap_id(interp, &name);
    let bitmap_id = existing_id.unwrap_or_else(|| {
        interp
            .fringe_bitmap_states
            .iter()
            .map(|bitmap| bitmap.id)
            .max()
            .unwrap_or(STANDARD_FRINGE_BITMAPS.len() as i64)
            + 1
    });
    if existing_id.is_none() {
        let mut bitmaps = vec![args[0].clone()];
        bitmaps.extend(
            interp
                .lookup_var("fringe-bitmaps", env)
                .and_then(|value| value.to_vec().ok())
                .unwrap_or_default(),
        );
        interp.set_global_binding("fringe-bitmaps", Value::list(bitmaps));
        interp.put_symbol_property(&name, "fringe", Value::Integer(bitmap_id));
    }
    let definition = Value::list([
        args[1].clone(),
        Value::Integer(height),
        Value::Integer(width),
        align,
        if periodic { Value::T } else { Value::Nil },
    ]);
    if let Some(bitmap) = interp
        .fringe_bitmap_states
        .iter_mut()
        .find(|bitmap| bitmap.id == bitmap_id)
    {
        bitmap.definition = Some(definition);
        // GNU destroys the old platform bitmap before replacing it, which
        // also resets any face override.
        bitmap.face = Value::Nil;
    } else {
        interp
            .fringe_bitmap_states
            .push(crate::lisp::eval::FringeBitmapState {
                name,
                id: bitmap_id,
                standard: false,
                definition: Some(definition),
                face: Value::Nil,
            });
    }
    Ok(args[0].clone())
}

fn destroy_fringe_bitmap(
    interp: &mut Interpreter,
    bitmap: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let name = fringe_symbol_name(bitmap)?;
    let Some(id) = fringe_bitmap_id(interp, &name) else {
        return Ok(Value::Nil);
    };
    let Some(index) = interp
        .fringe_bitmap_states
        .iter()
        .position(|bitmap| bitmap.id == id)
    else {
        return Ok(Value::Nil);
    };
    if interp.fringe_bitmap_states[index].standard {
        drop(interp.fringe_bitmap_states[index].definition.take());
        interp.fringe_bitmap_states[index].face = Value::Nil;
    } else {
        interp.fringe_bitmap_states.remove(index);
        interp.put_symbol_property(&name, "fringe", Value::Nil);
        let bitmaps = interp
            .lookup_var("fringe-bitmaps", env)
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter(|candidate| !values_eq_in_env(interp, candidate, bitmap, env))
            .collect::<Vec<_>>();
        interp.set_global_binding("fringe-bitmaps", Value::list(bitmaps));
    }
    Ok(Value::Nil)
}

/// Bare name of VALUE when it is a symbol, or a positioned symbol while
/// `symbols-with-pos-enabled' is non-nil — the same view GNU's `eq' takes
/// inside `assq'/`plist-get' during byte compilation.
fn bare_symbol_name(interp: &Interpreter, env: &Env, value: &Value) -> Option<String> {
    if let Ok(symbol) = value.as_symbol() {
        return Some(symbol.to_string());
    }
    if symbols_with_pos_enabled(interp, env)
        && let Some((bare, _)) = symbol_with_pos_parts(interp, value)
        && let Ok(symbol) = bare.as_symbol()
    {
        return Some(symbol.to_string());
    }
    None
}

/// The non-nil value of PROPERTY for SYMBOL from the first matching
/// `overriding-plist-environment' entry, GNU fns.c:Fget's pre-plist source.
fn overriding_plist_property(
    interp: &Interpreter,
    env: &Env,
    symbol: &str,
    property: &str,
) -> Option<Value> {
    let mut entries = interp
        .lookup_var("overriding-plist-environment", env)
        .filter(|value| !value.is_nil())?;
    let mut entry_guard = crate::lisp::types::CycleGuard::new();
    while let Value::Cons(ref entries_cell) = entries {
        if entry_guard.step(crate::lisp::types::ConsCell::identity(entries_cell)) {
            break;
        }
        let entry = entries.car().ok()?;
        let entry_key = entry
            .car()
            .ok()
            .and_then(|key| bare_symbol_name(interp, env, &key));
        if entry_key.as_deref() == Some(symbol) {
            let mut plist = entry.cdr().ok()?;
            let mut plist_guard = crate::lisp::types::CycleGuard::new();
            while let Value::Cons(ref plist_cell) = plist {
                if plist_guard.step(crate::lisp::types::ConsCell::identity(plist_cell)) {
                    break;
                }
                let key = plist.car().ok()?;
                let rest = plist.cdr().ok()?;
                if !matches!(rest, Value::Cons(_)) {
                    break;
                }
                if bare_symbol_name(interp, env, &key).as_deref() == Some(property) {
                    let value = rest.car().ok()?;
                    if !value.is_nil() {
                        return Some(value);
                    }
                }
                plist = rest.cdr().ok()?;
            }
            // GNU reads only the first assq hit before the real plist.
            return None;
        }
        entries = entries.cdr().ok()?;
    }
    None
}

fn macroexpand_dispatch(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    let environment = args.get(1).filter(|value| value.is_truthy());
    let mut form = args[0].clone();
    loop {
        let expanded = interp.macroexpand_1_form_with_environment(&form, environment, env)?;
        if expanded == form {
            return Ok(form);
        }
        form = expanded;
    }
}

fn run_hooks_dispatch(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
    mode_hooks: bool,
) -> Result<Value, LispError> {
    if mode_hooks
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
    if mode_hooks {
        let delayed = interp
            .lookup_var("delayed-mode-hooks", env)
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default();
        if !delayed.is_empty() {
            interp.set_variable("delayed-mode-hooks", Value::Nil, env);
            for hook in delayed {
                if let Ok(hook_name) = hook.as_symbol() {
                    run_named_hooks(interp, hook_name, env, Some(interp.current_buffer_id()))?;
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

define_dispatch!(
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
                let end =
                    normalize_string_index(args.get(2), chars.len() as i64, chars.len() as i64)?
                        as usize;
                let slice: String = chars[start..end].iter().collect();
                match read_one_form_in_env(interp, &slice, env) {
                    Ok((val, consumed)) => {
                        let materialized = materialize_read_hash_table_literals(interp, &val)?;
                        let materialized =
                            materialize_read_char_table_literals(interp, &materialized)?;
                        interp.intern_symbols_in_value(&materialized);
                        Ok(Value::cons(
                            materialized,
                            Value::Integer((start + consumed) as i64),
                        ))
                    }
                    Err(error) => Err(error),
                }
            }
            "md5" => {
                need_arg_range(name, args, 1, 4)?;
                let text = md5_source_text(interp, &args[0], args.get(1), args.get(2))?;
                let bytes = match args.get(3) {
                    Some(coding) if !coding.is_nil() => {
                        let inhibit_eol_conversion = interp
                            .lookup_var("inhibit-eol-conversion", env)
                            .is_some_and(|value| value.is_truthy());
                        encode_text_bytes(
                            interp,
                            &text,
                            &checked_coding_symbol(interp, coding)?,
                            inhibit_eol_conversion,
                        )?
                    }
                    _ => text.into_bytes(),
                };
                Ok(Value::String(format!("{:x}", md5::compute(bytes)).into()))
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
            // hex-util.el (native for speed; the feature is compat-preloaded).
            "secure-hash-algorithms" => {
                need_args(name, args, 0)?;
                Ok(Value::list([
                    Value::Symbol("md5".into()),
                    Value::Symbol("sha1".into()),
                    Value::Symbol("sha224".into()),
                    Value::Symbol("sha256".into()),
                    Value::Symbol("sha384".into()),
                    Value::Symbol("sha512".into()),
                ]))
            }
            "buffer-hash" => {
                need_arg_range(name, args, 0, 1)?;
                buffer_hash_value(interp, args.first())
            }

            // ── Misc ──
            "kill-emacs" => {
                need_arg_range(name, args, 0, 2)?;

                // In supported Emaxx use the Lisp runtime is owned by the batch
                // process.  GNU uses safe_run_hooks in this mode: an ordinary
                // hook error is reported but cannot cancel orderly shutdown.
                safe_run_named_hooks(
                    interp,
                    "kill-emacs-hook",
                    env,
                    Some(interp.current_buffer_id()),
                )?;

                // emacs.c accepts only a fixnum here.  Preserve its explicit
                // INT_MIN/INT_MAX masking before the CLI boundary narrows the
                // platform status to what the parent process can observe.
                let exit_code = match args.first() {
                    Some(Value::Integer(value)) if *value < 0 => {
                        ((*value as u32) | (i32::MIN as u32)) as i32
                    }
                    Some(Value::Integer(value)) => ((*value as u32) & (i32::MAX as u32)) as i32,
                    _ => 0,
                };
                let termination = EmacsTermination {
                    exit_code,
                    restart: args.get(1).is_some_and(Value::is_truthy),
                };
                interp.request_termination(termination.clone());
                Err(LispError::Terminate(termination))
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
                if args.len() != 2 {
                    return Err(LispError::WrongNumberOfArgs("throw".into(), args.len()));
                }
                interp.throw_value(args[0].clone(), args[1].clone(), env)
            }
            "defalias" => {
                need_arg_range(name, args, 2, 3)?;
                interp.defalias_value(args, env)
            }
            "provide" => {
                need_arg_range(name, args, 1, 2)?;
                // GNU 30.2 fns.c:Fprovide uses CHECK_SYMBOL/XSYMBOL, so
                // positioned reader symbols denote their underlying symbol
                // while `symbols-with-pos-enabled' is dynamically non-nil.
                let feature = checked_symbol_name(interp, &args[0], env)?;
                let subfeatures = args.get(1).cloned().unwrap_or(Value::Nil);
                // GNU rejects improper/non-list subfeature values even when the
                // feature was already present.
                subfeatures.to_vec()?;
                if subfeatures.is_truthy() {
                    interp.put_symbol_property(&feature, "subfeatures", subfeatures);
                }
                interp
                    .provide_feature_with_after_load(&feature, env)
                    .map(|_| args[0].clone())
            }
            "require" => {
                need_arg_range(name, args, 1, 3)?;
                // Like `provide', GNU's Frequire accepts a positioned symbol
                // through CHECK_SYMBOL and returns the original object.
                let feature = checked_symbol_name(interp, &args[0], env)?;
                let target = match args.get(1) {
                    Some(value) if value.is_truthy() => Some(string_text(value)?),
                    _ => None,
                };
                let noerror = args.get(2).is_some_and(Value::is_truthy);
                let result = interp.require_feature_with_target(&feature, target.as_deref(), env);
                // GNU's NOERROR only suppresses failure to locate/open the file;
                // errors raised after a file was found still propagate.
                if noerror
                    && let Err(LispError::SignalValue(condition)) = &result
                    && matches!(condition.car(), Ok(Value::Symbol(kind))
                    if kind == "file-missing" || kind == "file-error")
                {
                    return Ok(Value::Nil);
                }
                result.map(|_| args[0].clone())
            }
            "define-fringe-bitmap" => {
                need_arg_range(name, args, 2, 5)?;
                define_fringe_bitmap(interp, args, env)
            }
            "destroy-fringe-bitmap" => {
                need_args(name, args, 1)?;
                destroy_fringe_bitmap(interp, &args[0], env)
            }
            "set-fringe-bitmap-face" => {
                need_arg_range(name, args, 1, 2)?;
                let bitmap = fringe_symbol_name(&args[0])?;
                let Some(bitmap) = interp
                    .fringe_bitmap_states
                    .iter_mut()
                    .find(|candidate| candidate.name == bitmap)
                else {
                    return Err(LispError::Signal("Undefined fringe bitmap".into()));
                };
                bitmap.face = args.get(1).cloned().unwrap_or(Value::Nil);
                Ok(Value::Nil)
            }
            "fringe-bitmaps-at-pos" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                if window_record_id_from_value(interp, &window).is_none() {
                    return Err(wrong_type_argument("windowp", window));
                }
                if let Some(position) = args.first().filter(|value| !value.is_nil()) {
                    let position_value = position_from_value(interp, position)?;
                    if position_value < interp.buffer.point_min()
                        || position_value > interp.buffer.point_max()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("args-out-of-range"),
                            window,
                            position.clone(),
                        ])));
                    }
                }
                // The headless renderer retains no glyph matrix, so no display
                // row can own fringe bitmaps.  GNU returns nil in this state.
                Ok(Value::Nil)
            }
            "intern" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let symbol_name = match &args[0] {
                    Value::Symbol(symbol) => symbol.to_string(),
                    _ => string_text(&args[0])?,
                };
                let symbol_name = apply_symbol_shorthands_in_env(interp, &symbol_name, env)?;
                let obarray = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| {
                        interp
                            .lookup_var("obarray", env)
                            .filter(|value| !value.is_nil())
                    });
                if let Some(obarray) = obarray {
                    intern_in_obarray(interp, &obarray, &symbol_name)
                } else {
                    interp.intern_symbol_name(&symbol_name);
                    Ok(crate::lisp::types::interned_symbol_value(symbol_name))
                }
            }
            "intern-soft" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let obarray = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| {
                        interp
                            .lookup_var("obarray", env)
                            .filter(|value| !value.is_nil())
                    });
                let symbol_name = match &args[0] {
                    // `nil' and `t' are symbols in Elisp even though Emaxx
                    // gives their canonical values dedicated representations.
                    // Like every other symbol argument, GNU's `intern-soft'
                    // returns the exact object only when it belongs to the
                    // requested obarray.  `nil' is also the miss result, so it
                    // can be returned immediately for every obarray.
                    Value::Nil => return Ok(Value::Nil),
                    Value::T if obarray.is_none() => return Ok(Value::T),
                    Value::T if matches!(&obarray, Some(Value::Record(id)) if interp.is_standard_obarray_id(*id)) =>
                    {
                        return Ok(if interp.standard_obarray_contains_symbol("t") {
                            Value::T
                        } else {
                            Value::Nil
                        });
                    }
                    Value::T => return Ok(Value::Nil),
                    Value::Symbol(symbol) if obarray.is_none() => {
                        return Ok(if interp.standard_obarray_contains_symbol(symbol) {
                            Value::Symbol(symbol.clone())
                        } else {
                            Value::Nil
                        });
                    }
                    Value::Symbol(symbol)
                        if matches!(&obarray, Some(Value::Record(id)) if interp.is_standard_obarray_id(*id))
                            && crate::lisp::types::visible_symbol_name(symbol) == symbol =>
                    {
                        // An ordinary symbol object read by Lisp is already a
                        // member of the standard obarray.  Synthetic `make-symbol'
                        // and private-obarray names carry identity markers and
                        // must still miss here.
                        return Ok(if interp.standard_obarray_contains_symbol(symbol) {
                            Value::Symbol(symbol.clone())
                        } else {
                            Value::Nil
                        });
                    }
                    Value::Symbol(symbol) => {
                        let Some(obarray) = &obarray else {
                            return Ok(Value::Symbol(symbol.clone()));
                        };
                        let interned = intern_soft_in_obarray(
                            interp,
                            obarray,
                            crate::lisp::types::visible_symbol_name(symbol),
                        )?;
                        return Ok(if interned == args[0] {
                            args[0].clone()
                        } else {
                            Value::Nil
                        });
                    }
                    positioned
                        if symbols_with_pos_enabled(interp, env)
                            && symbol_with_pos_parts(interp, positioned).is_some() =>
                    {
                        // GNU 30.2 lread.c:Fintern_soft treats a positioned
                        // symbol as a symbol while the dynamic switch is on,
                        // searches for its bare symbol, and returns the exact
                        // positioned object on a hit.
                        let (bare, _) = symbol_with_pos_parts(interp, positioned)
                            .expect("guard established symbol-with-position");
                        let bare_name = bare.as_symbol()?;
                        if obarray.is_none() {
                            return Ok(if interp.standard_obarray_contains_symbol(bare_name) {
                                positioned.clone()
                            } else {
                                Value::Nil
                            });
                        }
                        let obarray = obarray.as_ref().expect("checked above");
                        let interned = intern_soft_in_obarray(
                            interp,
                            obarray,
                            crate::lisp::types::visible_symbol_name(bare_name),
                        )?;
                        return Ok(if interned == bare {
                            positioned.clone()
                        } else {
                            Value::Nil
                        });
                    }
                    _ => string_text(&args[0])?,
                };
                let symbol_name = apply_symbol_shorthands_in_env(interp, &symbol_name, env)?;
                if let Some(obarray) = obarray {
                    let interned = intern_soft_in_obarray(interp, &obarray, &symbol_name)?;
                    if interned.is_nil()
                        && matches!(&obarray, Value::Record(id) if interp.is_standard_obarray_id(*id))
                    {
                        // Built-in loaddefs entries are part of the standard
                        // obarray even before their libraries are loaded.  The
                        // compact preload index does not duplicate those names
                        // in `known_symbol_names', so consult function/value
                        // cells after an ordinary standard-obarray miss.
                        Ok(default_intern_soft_result(interp, &symbol_name, env))
                    } else {
                        Ok(interned)
                    }
                } else {
                    Ok(default_intern_soft_result(interp, &symbol_name, env))
                }
            }
            "unintern" => {
                need_arg_range(name, args, 1, 2)?;
                let obarray = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| {
                        interp
                            .lookup_var("obarray", env)
                            .filter(|value| !value.is_nil())
                    });
                match obarray {
                    Some(obarray) => {
                        Ok(if unintern_from_obarray(interp, &obarray, &args[0], env)? {
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
                    crate::lisp::types::make_uninterned_symbol_name(&base, id).into(),
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
                // GNU `autoload' does nothing when FUNCTION already has a real
                // (non-autoload) definition; subrs count as definitions, so the
                // cl-loaddefs autoloads must not shadow emaxx's native cl-*
                // primitives either.
                let old_definition = interp.logical_function_binding(&function, &Env::new());
                if old_definition
                    .as_ref()
                    .is_some_and(|existing| autoload_parts(existing).is_none())
                {
                    return Ok(Value::Nil);
                }
                let autoload = Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String(file.into()),
                    docstring,
                    interactive,
                    kind,
                ]);
                interp.record_definition_in_load_history("defun", &function);
                if let Some(old_definition) = old_definition {
                    interp.record_function_redefinition(&function, old_definition);
                }
                interp.set_function_binding(&function, Some(autoload));
                Ok(Value::Symbol(function.into()))
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
                if matches!(&macro_only, Value::Symbol(symbol) if symbol == "macro") && !loads_macro
                {
                    return Ok(fundef);
                }
                let ignore_errors = !loads_macro && macro_only.is_truthy();
                match interp.load_target_with_env(&file, env) {
                    Ok(_) => {}
                    Err(_) if ignore_errors => return Ok(Value::Nil),
                    Err(error) => return Err(error),
                }
                if funname.is_nil() || ignore_errors {
                    return Ok(Value::Nil);
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
                // GNU 30.2 data.c:Fset reaches set_internal's CHECK_SYMBOL.
                let checked = checked_symbol_name(interp, &args[0], env)?;
                let symbol = interp.resolve_variable_name(&checked)?;
                let value = interp.prepare_variable_assignment(&symbol, args[1].clone())?;
                let buffer_id = interp.assignment_buffer_id(&symbol);
                interp.notify_variable_watchers(&symbol, value.clone(), "set", buffer_id, env)?;
                interp.set_symbol_value_cell(&symbol, value.clone());
                Ok(value)
            }
            "set-default" => {
                need_args(name, args, 2)?;
                // GNU 30.2 data.c:Fset_default reaches
                // set_default_internal's CHECK_SYMBOL.
                let checked = checked_symbol_name(interp, &args[0], env)?;
                let symbol = interp.resolve_variable_name(&checked)?;
                let value = interp.prepare_variable_assignment(&symbol, args[1].clone())?;
                interp.notify_variable_watchers(&symbol, value.clone(), "set", None, env)?;
                interp.set_global_binding(&symbol, value.clone());
                Ok(value)
            }
            "symbol-value" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fsymbol_value reaches
                // find_symbol_value's CHECK_SYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                interp.symbol_value_cell(&symbol)
            }
            "default-value" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fdefault_value delegates to
                // default_value, which uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                interp.default_value(&symbol).ok_or(LispError::Void(symbol))
            }
            "default-toplevel-value" => {
                need_args(name, args, 1)?;
                // GNU 30.2 eval.c:Fdefault_toplevel_value ultimately uses
                // the same CHECK_SYMBOL default-value path.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                interp
                    .default_toplevel_value(&symbol)
                    .ok_or(LispError::Void(symbol))
            }
            "set-default-toplevel-value" => {
                need_args(name, args, 2)?;
                // GNU 30.2 eval.c:Fset_default_toplevel_value uses the
                // CHECK_SYMBOL default binding path.
                let checked = checked_symbol_name(interp, &args[0], env)?;
                let symbol = interp.resolve_variable_name(&checked)?;
                let value = interp.prepare_variable_assignment(&symbol, args[1].clone())?;
                interp.notify_variable_watchers(&symbol, value.clone(), "set", None, env)?;
                interp.set_default_toplevel_value(&symbol, value.clone());
                Ok(value)
            }
            "symbol-plist" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fsymbol_plist uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                Ok(interp.symbol_plist(&symbol))
            }
            "setplist" => {
                need_args(name, args, 2)?;
                // GNU 30.2 data.c:Fsetplist uses CHECK_SYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                interp.set_symbol_plist(&symbol, args[1].clone())
            }
            "interactive-form" => {
                need_args(name, args, 1)?;
                // GNU returns nil for an unbound symbol (advice.el probes the
                // interactive form of not-yet-defined advice functions).
                let Ok(mut value) = resolve_callable(interp, &args[0], env) else {
                    return Ok(Value::Nil);
                };
                // data.c:1141-1151 consults an `interactive-form' property
                // FIRST -- unconditionally, and walking the symbol-function
                // alias chain -- before any subr/closure/oclosure inspection.
                // The `indirect_function' nil check above already ran, which is
                // why an unbound symbol never reaches this walk.
                // Unbounded, exactly as data.c:1144 is: `defalias' signals
                // `cyclic-function-indirection' in both binaries, so no cyclic
                // chain can reach here.  A defensive cap was measured to
                // diverge from GNU at the 64th link, and the sibling
                // `command-modes' walk is likewise uncapped.
                let mut probe = args[0].clone();
                loop {
                    let Ok(symbol) = probe.as_symbol() else { break };
                    let symbol = symbol.to_string();
                    if let Some(form) = interp.get_symbol_property(&symbol, "interactive-form")
                        && !matches!(form, Value::Nil)
                    {
                        return Ok(form);
                    }
                    match interp.logical_function_binding(&symbol, env) {
                        Some(next) => probe = next,
                        None => break,
                    }
                }
                // GNU's C interactive_form then consults
                // `oclosure-interactive-form' for OClosures (nadvice's advice
                // objects compose their spec), but an (interactive ...) form
                // IN THE BODY outranks the generic (oclosure-lambda bodies may
                // carry their own spec).
                // Cheap guard first: GNU compares a function pointer here,
                // so do not pay an `oclosure-type' Lisp call when the generic
                // it feeds is not even loaded.
                if interp.has_lisp_function("oclosure-interactive-form")
                    && super::misc_keymaps::value_is_oclosure(interp, &value, env)
                {
                    if let Some(items) = callable_interactive_form_items(interp, &value) {
                        return Ok(Value::list(items));
                    }
                    return interp.call_function_value(
                        Value::Symbol("oclosure-interactive-form".into()),
                        Some("oclosure-interactive-form"),
                        std::slice::from_ref(&value),
                        env,
                    );
                }
                if let (Some(symbol), Some((file, _, _))) =
                    (args[0].as_symbol().ok(), autoload_parts(&value))
                {
                    interp.load_target_with_env(&file, env)?;
                    value = interp.lookup_function(symbol, env)?;
                }
                Ok(callable_interactive_form_items(interp, &value)
                    .map(Value::list)
                    .unwrap_or(Value::Nil))
            }
            "daemonp" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "daemon-initialized" => {
                need_args(name, args, 0)?;
                Err(LispError::Signal(
                    "This function can only be called if emacs is run as a daemon".into(),
                ))
            }
            "invocation-name" | "invocation-directory" => {
                need_args(name, args, 0)?;
                let value = interp.lookup_var(name, env).unwrap_or(Value::Nil);
                Ok(string_like(&value)
                    .map(|string| Value::String(string.text.into()))
                    .unwrap_or(value))
            }
            "Snarf-documentation" => {
                need_args(name, args, 1)?;
                snarf_documentation(interp, &args[0], env)
            }
            "documentation" => {
                need_arg_range(name, args, 1, 2)?;
                documentation(interp, args, env)
            }
            "documentation-property" => {
                need_arg_range(name, args, 2, 3)?;
                documentation_property(interp, args, env)
            }
            "internal-subr-documentation" => {
                need_args(name, args, 1)?;
                internal_subr_documentation(interp, &args[0], env)
            }
            "get" => {
                need_arg_range(name, args, 2, 3)?;
                // GNU 30.2 fns.c:Fget applies CHECK_SYMBOL to SYMBOL and
                // XSYMBOL to the same underlying bare symbol.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                let property = args[1].as_symbol()?;
                // GNU fns.c:Fget consults `overriding-plist-environment'
                // (populated by bytecomp's compile-time handler for top-level
                // `function-put'/`define-symbol-prop') before the symbol's
                // own plist, returning the first non-nil hit.
                if let Some(overriding) = overriding_plist_property(interp, env, &symbol, property)
                {
                    return Ok(overriding);
                }
                Ok(interp
                    .get_symbol_property(&symbol, property)
                    .unwrap_or(Value::Nil))
            }
            "makunbound" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fmakunbound uses CHECK_SYMBOL/XSYMBOL and
                // returns the original symbol argument.
                let checked = checked_symbol_name(interp, &args[0], env)?;
                let symbol = interp.resolve_variable_name(&checked)?;
                if symbol == "initial-window-system"
                    || matches!(
                        symbol.as_str(),
                        "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum"
                    )
                    || symbol.starts_with(':')
                {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("setting-constant".into()),
                        args[0].clone(),
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
                    interp.notify_variable_watchers(
                        &symbol,
                        Value::Nil,
                        "makunbound",
                        None,
                        env,
                    )?;
                    interp.remove_global_binding(&symbol);
                }
                Ok(args[0].clone())
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
                        Value::Symbol(target.clone().into()),
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
                        Value::Symbol(alias.clone().into()),
                    ]);
                    call_named_function(interp, "display-warning", &[warning], env)?;
                }
                Ok(Value::Symbol(alias.into()))
            }
            "indirect-variable" => {
                need_args(name, args, 1)?;
                let symbol = args[0].as_symbol()?;
                Ok(Value::Symbol(interp.indirect_variable_name(symbol)?.into()))
            }
            "internal--define-uninitialized-variable" => {
                // GNU: (SYMBOL &optional DOC) — cus-start.el passes one arg.
                need_arg_range(name, args, 1, 2)?;
                let symbol = args[0].as_symbol()?;
                interp.mark_special_variable(symbol);
                if let Some(doc) = args.get(1).filter(|value| !value.is_nil()) {
                    interp.put_symbol_property(symbol, "variable-documentation", doc.clone());
                }
                Ok(Value::Symbol(symbol.to_string().into()))
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
                Ok(Value::Symbol(symbol.to_string().into()))
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
                Ok(Value::Symbol(symbol.to_string().into()))
            }
            "internal-make-var-non-special" => {
                need_args(name, args, 1)?;
                let symbol = args[0].as_symbol()?;
                interp.unmark_special_variable(symbol);
                Ok(Value::Symbol(symbol.to_string().into()))
            }
            "make-interpreted-closure" => {
                need_arg_range(name, args, 3, 5)?;
                let mut slots = vec![args[0].clone(), args[1].clone(), args[2].clone()];
                let documentation = args.get(3).filter(|value| !value.is_nil()).cloned();
                let interactive = match args.get(4).filter(|value| !value.is_nil()) {
                    Some(iform) => {
                        let items = iform.to_vec()?;
                        Some(
                            crate::lisp::types::LambdaValue::interactive_slot_from_iform_items(
                                &items,
                            ),
                        )
                    }
                    None => None,
                };
                // Slot 3 is the bytecode stack-depth position and is unused
                // by interpreted closures.  GNU chooses the public size from
                // the values, not from whether optional arguments were
                // syntactically supplied.
                if interactive.is_some() || documentation.is_some() {
                    slots.push(Value::Nil);
                    slots.push(documentation.unwrap_or(Value::Nil));
                }
                if let Some(interactive) = interactive {
                    slots.push(interactive);
                }
                interp.make_interpreted_closure_value(&slots)
            }
            "getenv-internal" => {
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
                if let Some((Value::Symbol(symbol), environment)) =
                    process_environment.cons_values()
                    && symbol == "environment"
                {
                    process_environment = environment;
                }
                Ok(
                    getenv_in_environment(&variable, &process_environment, from_explicit_env)?
                        .unwrap_or(Value::Nil),
                )
            }

            // Load-time compatibility shims for upstream Lisp helpers whose exact
            // side effects are not needed by the currently exercised batch paths.
            "purecopy" => {
                need_args(name, args, 1)?;
                Ok(args[0].clone())
            }

            "macroexpand" => macroexpand_dispatch(interp, name, args, env),

            "current-idle-time" => {
                need_args(name, args, 0)?;
                // keyboard.c Fcurrent_idle_time: the span since idleness
                // began, as (HIGH LOW USEC PSEC), or nil while input is
                // being processed (and always nil in batch, which has no
                // input loop).
                match crate::lisp::primitives::tty_current_idle_duration() {
                    Some(elapsed) => {
                        let secs = elapsed.as_secs();
                        let usec = elapsed.subsec_micros();
                        let psec = (elapsed.subsec_nanos() % 1_000) * 1_000_000;
                        Ok(Value::list([
                            Value::Integer((secs >> 16) as i64),
                            Value::Integer((secs & 0xffff) as i64),
                            Value::Integer(i64::from(usec)),
                            Value::Integer(i64::from(psec)),
                        ]))
                    }
                    None => Ok(Value::Nil),
                }
            }
            "subr-type" => {
                need_args(name, args, 1)?;
                if !matches!(args[0], Value::BuiltinFunc(_)) {
                    return Err(wrong_type_argument("subrp", args[0].clone()));
                }
                // Emaxx currently has no native-compiled Lisp subrs.  GNU C
                // primitives report nil here even in native-comp-enabled builds.
                Ok(Value::Nil)
            }
            "function-equal" => {
                need_args(name, args, 2)?;
                let same = match (&args[0], &args[1]) {
                    (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
                    (Value::Integer(left), Value::Integer(right)) => left == right,
                    (Value::Symbol(left), Value::Symbol(right))
                    | (Value::BuiltinFunc(left), Value::BuiltinFunc(right)) => left == right,
                    (Value::StringObject(left), Value::StringObject(right)) => {
                        Rc::ptr_eq(left, right)
                    }
                    (Value::Cons(left), Value::Cons(right)) => Rc::ptr_eq(left, right),
                    (Value::Lambda(left), Value::Lambda(right)) => {
                        Rc::ptr_eq(&left.body, &right.body)
                    }
                    (Value::Buffer(left), Value::Buffer(right)) => left.id == right.id,
                    (Value::Marker(left), Value::Marker(right))
                    | (Value::Overlay(left), Value::Overlay(right))
                    | (Value::CharTable(left), Value::CharTable(right))
                    | (Value::Frame(left), Value::Frame(right))
                    | (Value::Terminal(left), Value::Terminal(right))
                    | (Value::Record(left), Value::Record(right))
                    | (Value::Finalizer(left), Value::Finalizer(right)) => left == right,
                    _ => false,
                };
                Ok(if same { Value::T } else { Value::Nil })
            }
            "get-internal-run-time" => {
                need_args(name, args, 0)?;
                process_cpu_time_value()
            }
            "flush-standard-output" => {
                need_args(name, args, 0)?;
                std::io::stdout()
                    .flush()
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                Ok(Value::Nil)
            }
            "lossage-size" => {
                need_arg_range(name, args, 0, 1)?;
                if args.first().is_none_or(Value::is_nil) {
                    return Ok(Value::Integer(interp.lossage_size));
                }
                let Value::Integer(new_size) = &args[0] else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("user-error"),
                        Value::String("Value must be a positive integer".into()),
                    ])));
                };
                let new_size = *new_size;
                if new_size < 0 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("user-error"),
                        Value::String("Value must be a positive integer".into()),
                    ])));
                }
                if new_size < 100 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("user-error"),
                        Value::String("Value must be >= 100".into()),
                    ])));
                }
                interp.lossage_size = new_size;
                let new_size = new_size as usize;
                if interp.keyboard_input.recent_keys.len() > new_size {
                    let excess = interp.keyboard_input.recent_keys.len() - new_size;
                    interp.keyboard_input.recent_keys.drain(0..excess);
                }
                Ok(Value::Integer(interp.lossage_size))
            }
            "run-hooks" => run_hooks_dispatch(interp, args, env, false),

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

            "run-hook-wrapped" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let hook_name = args[0].as_symbol()?;
                let wrapper = resolve_callable(interp, &args[1], env)?;
                // Merge global and buffer-local members like `run-hooks'
                // (also strips the local-hook `t' sentinel).
                let hook_values =
                    hook_values(interp, hook_name, env, Some(interp.current_buffer_id()));
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

            "mapatoms" => {
                need_arg_range(name, args, 1, 2)?;
                let callback = resolve_callable(interp, &args[0], env)?;
                let obarray = args.get(1).cloned().unwrap_or(Value::Nil);
                let symbols = if obarray.is_nil() {
                    interp
                        .known_symbol_names()
                        .into_iter()
                        .map(|value| Value::Symbol(value.into()))
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
        }
    }
);

#[cfg(unix)]
fn process_cpu_time_value() -> Result<Value, LispError> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the pointed-to rusage structure and does
    // not retain the pointer.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: the successful getrusage call initialized every field.
    let usage = unsafe { usage.assume_init() };
    fn libc_time_to_i64<T: Into<i64>>(value: T) -> i64 {
        value.into()
    }

    let mut seconds =
        libc_time_to_i64(usage.ru_utime.tv_sec) + libc_time_to_i64(usage.ru_stime.tv_sec);
    let mut micros =
        libc_time_to_i64(usage.ru_utime.tv_usec) + libc_time_to_i64(usage.ru_stime.tv_usec);
    if micros >= 1_000_000 {
        seconds += micros / 1_000_000;
        micros %= 1_000_000;
    }
    Ok(Value::list([
        Value::Integer(seconds >> 16),
        Value::Integer(seconds & 0xffff),
        Value::Integer(micros),
        Value::Integer(0),
    ]))
}

#[cfg(not(unix))]
fn process_cpu_time_value() -> Result<Value, LispError> {
    system_time_list_value(SystemTime::now())
}

#[derive(Clone, Debug)]
struct DocFileEntry {
    kind: u8,
    name: String,
    offset: i64,
    user_variable: bool,
    skip: bool,
}

fn parse_doc_file_entries(bytes: &[u8]) -> Result<Vec<DocFileEntry>, LispError> {
    let mut entries = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = bytes[cursor..].iter().position(|byte| *byte == 0x1f) {
        let marker = cursor + relative;
        if marker + 2 > bytes.len() {
            return Err(LispError::Signal(format!(
                "DOC file invalid at position {marker}"
            )));
        }
        let kind = bytes[marker + 1];
        let name_start = marker + 2;
        let Some(relative_newline) = bytes[name_start..].iter().position(|byte| *byte == b'\n')
        else {
            return Err(LispError::Signal(format!(
                "DOC file invalid at position {marker}"
            )));
        };
        let newline = name_start + relative_newline;
        let content_start = newline + 1;
        let next_marker = bytes[content_start..]
            .iter()
            .position(|byte| *byte == 0x1f)
            .map(|relative| content_start + relative)
            .unwrap_or(bytes.len());
        if !matches!(kind, b'F' | b'V' | b'S') {
            return Err(LispError::Signal(format!(
                "DOC file invalid at position {marker}"
            )));
        }
        entries.push(DocFileEntry {
            kind,
            name: String::from_utf8_lossy(&bytes[name_start..newline]).into_owned(),
            offset: content_start as i64,
            user_variable: kind == b'V' && bytes.get(content_start) == Some(&b'*'),
            skip: bytes[content_start..next_marker].starts_with(b"SKIP"),
        });
        cursor = next_marker;
        if cursor == bytes.len() {
            break;
        }
    }
    Ok(entries)
}

fn doc_path(directory: &Value, filename: &Value) -> Result<PathBuf, LispError> {
    let directory = string_like(directory)
        .ok_or_else(|| wrong_type_argument("stringp", directory.clone()))?
        .text;
    let filename = string_like(filename)
        .ok_or_else(|| wrong_type_argument("stringp", filename.clone()))?
        .text;
    let filename = PathBuf::from(filename);
    Ok(if filename.is_absolute() {
        filename
    } else {
        Path::new(&directory).join(filename)
    })
}

fn standard_doc_path(interp: &Interpreter, env: &Env) -> Result<PathBuf, LispError> {
    let directory = interp
        .lookup_var("doc-directory", env)
        .unwrap_or(Value::Nil);
    let filename = interp
        .lookup_var("internal-doc-file-name", env)
        .unwrap_or(Value::Nil);
    doc_path(&directory, &filename)
}

fn decode_doc_string(bytes: &[u8], position: i64) -> Result<Option<String>, LispError> {
    let Some(position) = position
        .checked_abs()
        .and_then(|value| usize::try_from(value).ok())
    else {
        return Ok(None);
    };
    if position == 0
        || position > bytes.len()
        || bytes.get(position.wrapping_sub(1)) != Some(&b'\n')
    {
        return Ok(None);
    }
    let marker = bytes[..position - 1].iter().rposition(|byte| *byte == 0x1f);
    if marker.is_none() {
        return Ok(None);
    }
    let end = bytes[position..]
        .iter()
        .position(|byte| *byte == 0x1f)
        .map(|relative| position + relative)
        .unwrap_or(bytes.len());
    let mut decoded = Vec::with_capacity(end - position);
    let mut cursor = position;
    while cursor < end {
        if bytes[cursor] != 1 {
            decoded.push(bytes[cursor]);
            cursor += 1;
            continue;
        }
        let Some(escaped) = bytes.get(cursor + 1).copied() else {
            return Err(LispError::Signal(
                "Invalid data in documentation file".into(),
            ));
        };
        decoded.push(match escaped {
            1 => 1,
            b'0' => 0,
            b'_' => 0x1f,
            _ => {
                return Err(LispError::Signal(format!(
                    "Invalid data in documentation file -- {} followed by code {escaped:03o}",
                    1
                )));
            }
        });
        cursor += 2;
    }
    Ok(Some(String::from_utf8_lossy(&decoded).into_owned()))
}

fn doc_reference_parts(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Option<(PathBuf, i64)>, LispError> {
    if let Value::Integer(position) = value {
        return Ok(Some((standard_doc_path(interp, env)?, *position)));
    }
    let Some((filename, position)) = value.cons_values() else {
        return Ok(None);
    };
    let Value::Integer(position) = position else {
        return Ok(None);
    };
    let directory = interp
        .lookup_var("lisp-directory", env)
        .unwrap_or(Value::Nil);
    Ok(Some((doc_path(&directory, &filename)?, position)))
}

fn resolve_doc_reference(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Option<Value>, LispError> {
    let Some((path, position)) = doc_reference_parts(interp, value, env)? else {
        return Ok(None);
    };
    match std::fs::read(&path) {
        Ok(bytes) => {
            Ok(decode_doc_string(&bytes, position)?.map(|value| Value::String(value.into())))
        }
        Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound) => {
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_else(|| path.to_str().unwrap_or(""));
            Ok(Some(Value::String(
                format!("Cannot open doc string file \"{filename}\"\n").into(),
            )))
        }
        Err(error) => Err(LispError::SignalValue(file_error_value(
            &error.to_string(),
            &path.to_string_lossy(),
        ))),
    }
}

fn is_doc_reference(value: &Value) -> bool {
    matches!(value, Value::Integer(_))
        || value.cons_values().is_some_and(|(filename, position)| {
            string_like(&filename).is_some() && matches!(position, Value::Integer(_))
        })
}

fn substitute_doc_keys(
    interp: &mut Interpreter,
    doc: Value,
    raw: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    if raw || string_like(&doc).is_none() {
        return Ok(doc);
    }
    interp.call_function_value(
        Value::symbol("substitute-command-keys"),
        Some("substitute-command-keys"),
        &[doc],
        env,
    )
}

fn snarf_documentation(
    interp: &mut Interpreter,
    filename: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if string_like(filename).is_none() {
        return Err(wrong_type_argument("stringp", filename.clone()));
    }
    let directory = interp
        .lookup_var("doc-directory", env)
        .unwrap_or(Value::Nil);
    let path = doc_path(&directory, filename)?;
    let bytes = std::fs::read(&path).map_err(|error| {
        LispError::SignalValue(file_error_value(
            &format!("Opening doc string file: {error}"),
            &path.to_string_lossy(),
        ))
    })?;
    let entries = parse_doc_file_entries(&bytes)?;
    interp.set_variable("internal-doc-file-name", filename.clone(), env);
    // GNU's help-fns.el validates C source markers against the native object
    // inventory in `build-files'.  DOC already carries that inventory as S
    // records, so derive it from this parse instead of maintaining a second
    // hard-coded list which can drift with the selected GNU tree.
    interp.set_variable(
        "build-files",
        Value::list(
            entries
                .iter()
                .filter(|entry| entry.kind == b'S')
                .map(|entry| Value::String(entry.name.clone().into())),
        ),
        env,
    );
    let delayed = interp
        .lookup_var("custom-delayed-init-variables", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    for entry in entries {
        if entry.skip {
            continue;
        }
        match entry.kind {
            b'F' => {
                if let Ok(Value::BuiltinFunc(native_name)) =
                    interp.lookup_function(&entry.name, env)
                {
                    interp
                        .builtin_doc_offsets
                        .insert(native_name.to_string(), entry.offset);
                }
            }
            b'V' if interp.lookup_var(&entry.name, env).is_some()
                || delayed
                    .iter()
                    .any(|value| matches!(value, Value::Symbol(name) if name == &entry.name)) =>
            {
                interp.put_symbol_property(
                    &entry.name,
                    "variable-documentation",
                    Value::Integer(if entry.user_variable {
                        -entry.offset
                    } else {
                        entry.offset
                    }),
                );
            }
            b'V' | b'S' => {}
            _ => unreachable!("DOC entry kinds were validated while parsing"),
        }
    }
    Ok(Value::Nil)
}

fn ensure_builtin_doc_offset(
    interp: &mut Interpreter,
    name: &str,
    env: &Env,
) -> Result<i64, LispError> {
    if let Some(offset) = interp.builtin_doc_offsets.get(name) {
        return Ok(*offset);
    }
    let path = standard_doc_path(interp, env)?;
    let Ok(bytes) = std::fs::read(path) else {
        return Ok(0);
    };
    let offset = parse_doc_file_entries(&bytes)?
        .into_iter()
        .find(|entry| entry.kind == b'F' && entry.name == name && !entry.skip)
        .map(|entry| entry.offset)
        .unwrap_or(0);
    interp.builtin_doc_offsets.insert(name.to_string(), offset);
    Ok(offset)
}

fn internal_subr_documentation(
    interp: &mut Interpreter,
    function: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let Value::BuiltinFunc(name) = function else {
        return Ok(Value::T);
    };
    Ok(Value::Integer(ensure_builtin_doc_offset(
        interp, name, env,
    )?))
}

fn documentation_property(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let symbol = args[0]
        .as_symbol()
        .map_err(|_| wrong_type_argument("symbolp", args[0].clone()))?;
    let property = args[1]
        .as_symbol()
        .map_err(|_| wrong_type_argument("symbolp", args[1].clone()))?;
    let mut doc = interp
        .get_symbol_property(symbol, property)
        .unwrap_or(Value::Nil);
    if doc.is_nil()
        && property == "variable-documentation"
        && let Ok(target) = interp.resolve_variable_name(symbol)
        && target != symbol
    {
        doc = interp
            .get_symbol_property(&target, property)
            .unwrap_or(Value::Nil);
    }
    if doc == Value::Integer(0) {
        doc = Value::Nil;
    } else if is_doc_reference(&doc) {
        doc = resolve_doc_reference(interp, &doc, env)?.unwrap_or(Value::Nil);
    } else if string_like(&doc).is_none() && !doc.is_nil() {
        doc = interp.eval(&doc, env)?;
    }
    substitute_doc_keys(interp, doc, args.get(2).is_some_and(Value::is_truthy), env)
}

fn documentation(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let raw = args.get(1).is_some_and(Value::is_truthy);
    if let Value::Symbol(symbol) = &args[0]
        && interp
            .get_symbol_property(symbol, "function-documentation")
            .is_some_and(|value| !value.is_nil())
    {
        return documentation_property(
            interp,
            &[
                args[0].clone(),
                Value::symbol("function-documentation"),
                if raw { Value::T } else { Value::Nil },
            ],
            env,
        );
    }

    let function = resolve_callable(interp, &args[0], env)?;
    let mut doc = match &function {
        Value::BuiltinFunc(name) => {
            let offset = ensure_builtin_doc_offset(interp, name, env)?;
            if offset == 0 {
                fallback_function_documentation(interp, name)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil)
            } else {
                resolve_doc_reference(interp, &Value::Integer(offset), env)?.unwrap_or(Value::Nil)
            }
        }
        _ => function_documentation(interp, &function, env).unwrap_or(Value::Nil),
    };
    if doc.is_nil()
        && let Value::Symbol(symbol) = &args[0]
    {
        doc = fallback_function_documentation(interp, symbol)
            .map(|value| Value::String(value.into()))
            .unwrap_or(Value::Nil);
    }
    if doc == Value::Integer(0) {
        doc = Value::Nil;
    } else if is_doc_reference(&doc) {
        doc = resolve_doc_reference(interp, &doc, env)?.unwrap_or(Value::Nil);
    }
    substitute_doc_keys(interp, doc, raw, env)
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
fn lisp_source_root(interp: &Interpreter) -> Option<PathBuf> {
    crate::lisp::primitives::compat_data_directory()
        .map(PathBuf::from)
        .and_then(|etc| etc.parent().map(|root| root.join("lisp")))
        .filter(|root| root.is_dir())
        .or_else(|| {
            interp
                .configured_load_path()
                .iter()
                .find(|path| path.file_name().is_some_and(|name| name == "lisp"))
                .cloned()
        })
}

fn builtin_doc_from_doc_file(interp: &Interpreter, function: &str) -> Option<String> {
    let path = crate::lisp::primitives::compat_data_directory()
        .map(PathBuf::from)
        .map(|etc| etc.join("DOC"))
        .filter(|path| path.is_file())
        .or_else(|| {
            lisp_source_root(interp)?
                .parent()
                .map(|root| root.join("etc/DOC"))
        })?;
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

pub(crate) fn fallback_function_documentation(
    interp: &Interpreter,
    function: &str,
) -> Option<String> {
    // The etc/DOC database is the sole honest source for native
    // documentation; scraping GNU .el sources forged Lisp provenance for
    // natively implemented names.
    builtin_doc_from_doc_file(interp, function)
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
