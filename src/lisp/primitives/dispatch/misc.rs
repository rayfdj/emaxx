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
            | "decode-hex-string"
            | "encode-hex-string"
            | "rfc2104-hash"
            | "error"
            | "user-error"
            | "signal"
            | "throw"
            | "defalias"
            | "provide"
            | "require"
            | "define-error"
            | "define-fringe-bitmap"
            | "destroy-fringe-bitmap"
            | "set-fringe-bitmap-face"
            | "fringe-bitmaps-at-pos"
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
            | "daemon-initialized"
            | "kill-emacs"
            | "invocation-name"
            | "invocation-directory"
            | "Snarf-documentation"
            | "documentation"
            | "documentation-property"
            | "internal-subr-documentation"
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
            | "timer-event-handler"
            | "timerp"
            | "current-idle-time"
            | "subr-type"
            | "function-equal"
            | "get-internal-run-time"
            | "flush-standard-output"
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
                    let materialized = materialize_read_char_table_literals(interp, &materialized)?;
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
        // hex-util.el (native for speed; the feature is compat-preloaded).
        "decode-hex-string" => {
            need_args(name, args, 1)?;
            let text = string_text(&args[0])?;
            let chars: Vec<char> = text.chars().collect();
            if !chars.len().is_multiple_of(2) {
                return Err(LispError::Signal(format!(
                    "Args out of range: {:?}, {}",
                    text,
                    chars.len()
                )));
            }
            let mut out = Vec::with_capacity(chars.len() / 2);
            for pair in chars.chunks(2) {
                let hi = hex_util_digit(pair[0])?;
                let lo = hex_util_digit(pair[1])?;
                out.push((hi * 16 + lo) as u8);
            }
            Ok(bytes_to_shared_unibyte_value(&out))
        }
        "encode-hex-string" => {
            need_args(name, args, 1)?;
            let text = string_text(&args[0])?;
            let bytes = internal_text_bytes(&text, false)?;
            let mut dst = String::with_capacity(bytes.len() * 2);
            for byte in bytes {
                dst.push(HEX_UTIL_DIGITS[(byte / 16) as usize] as char);
                dst.push(HEX_UTIL_DIGITS[(byte % 16) as usize] as char);
            }
            Ok(Value::String(dst))
        }
        // rfc2104.el HMAC (native for speed; the feature is compat-preloaded).
        // HASH is funcalled for wrapper functions (sasl-scram-sha256 etc.);
        // known algorithm symbols hash natively.
        "rfc2104-hash" => {
            need_args(name, args, 5)?;
            let block_length = usize::try_from(args[1].as_integer()?)
                .map_err(|_| LispError::TypeError("natnum".into(), args[1].type_name()))?;
            let hash_length = usize::try_from(args[2].as_integer()?)
                .map_err(|_| LispError::TypeError("natnum".into(), args[2].type_name()))?;
            let key_text = string_text(&args[3])?;
            let text = string_text(&args[4])?;
            let mut key = internal_text_bytes(&key_text, false)?;
            let text_bytes = internal_text_bytes(&text, false)?;
            // GNU: a key longer than the block is replaced by the HASH's
            // return value -- the hex STRING itself, not its octets.
            if key.len() > block_length {
                key = rfc2104_hash_hex(interp, &args[0], &key, env)?.into_bytes();
            }
            if key.len() > block_length {
                return Err(LispError::Signal(format!(
                    "Args out of range: {}, {}",
                    key.len(),
                    block_length
                )));
            }
            let mut ipad = vec![0x36u8; block_length];
            let mut opad = vec![0x5cu8; block_length];
            for (index, byte) in key.iter().enumerate() {
                ipad[index] ^= byte;
                opad[index] ^= byte;
            }
            ipad.extend_from_slice(&text_bytes);
            let inner = rfc2104_hash_digest(interp, &args[0], &ipad, env)?;
            if inner.len() < hash_length {
                return Err(LispError::Signal(format!(
                    "Args out of range: {}, {}",
                    inner.len(),
                    hash_length
                )));
            }
            opad.extend_from_slice(&inner[..hash_length]);
            Ok(Value::String(rfc2104_hash_hex(
                interp, &args[0], &opad, env,
            )?))
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
            let feature = args[0].as_symbol()?.to_string();
            let subfeatures = args.get(1).cloned().unwrap_or(Value::Nil);
            // GNU rejects improper/non-list subfeature values even when the
            // feature was already present.
            subfeatures.to_vec()?;
            if subfeatures.is_truthy() {
                interp.put_symbol_property(&feature, "subfeatures", subfeatures);
            }
            interp.provide_feature_with_after_load(&feature)
        }
        "require" => {
            need_arg_range(name, args, 1, 3)?;
            let feature = args[0].as_symbol()?.to_string();
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
            result
        }
        "define-error" => {
            need_arg_range(name, args, 2, 3)?;
            let condition_name = args[0].as_symbol()?.to_string();
            let parent = args.get(2).filter(|value| value.is_truthy()).cloned();
            let parent = parent.unwrap_or_else(|| Value::Symbol("error".into()));
            let parent_is_list = matches!(parent, Value::Cons(_, _));
            let parents = if parent_is_list {
                parent.to_vec()?
            } else {
                vec![parent]
            };

            let mut conditions = vec![Value::Symbol(condition_name.clone())];
            for parent in parents {
                let parent_name = parent.as_symbol()?.to_string();
                if !conditions.contains(&parent) {
                    conditions.push(parent.clone());
                }
                let inherited = interp
                    .get_symbol_property(&parent_name, "error-conditions")
                    .and_then(|value| value.to_vec().ok());
                if parent_is_list && inherited.is_none() {
                    return Err(LispError::Signal(format!("Unknown signal `{parent_name}'")));
                }
                for ancestor in inherited.unwrap_or_default() {
                    if !conditions.contains(&ancestor) {
                        conditions.push(ancestor);
                    }
                }
            }
            interp.put_symbol_property(
                &condition_name,
                "error-conditions",
                Value::list(conditions),
            );
            if args[1].is_truthy() {
                interp.put_symbol_property(&condition_name, "error-message", args[1].clone());
            }
            Ok(args[0].clone())
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
                Value::Symbol(symbol) if obarray.is_none() => {
                    return Ok(Value::Symbol(symbol.clone()));
                }
                Value::Symbol(symbol)
                    if matches!(&obarray, Some(Value::Record(id)) if interp.is_standard_obarray_id(*id))
                        && crate::lisp::types::visible_symbol_name(symbol) == symbol =>
                {
                    // An ordinary symbol object read by Lisp is already a
                    // member of the standard obarray.  Synthetic `make-symbol'
                    // and private-obarray names carry identity markers and
                    // must still miss here.
                    return Ok(Value::Symbol(symbol.clone()));
                }
                Value::Symbol(symbol) => symbol.clone(),
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
                Some(obarray) => Ok(if unintern_from_obarray(interp, &obarray, &args[0], env)? {
                    Value::T
                } else {
                    Value::Nil
                }),
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
                Value::String(file),
                docstring,
                interactive,
                kind,
            ]);
            interp.record_definition_in_load_history("defun", &function);
            if let Some(old_definition) = old_definition {
                interp.record_function_redefinition(&function, old_definition);
            }
            interp.set_function_binding(&function, Some(autoload));
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
                    interp.set_function_binding(symbol, Some(function.clone()));
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
                // GNU sets an already-defined option immediately; NOW only
                // forces a binding for options not yet defined.
                if items.get(2).is_some_and(Value::is_truthy)
                    || interp.default_toplevel_value(&symbol).is_some()
                {
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
                .map(|string| Value::String(string.text))
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
            let autoload = args.get(2).cloned().unwrap_or(Value::Nil);
            // GNU follows aliases until a non-nil property is found.  With
            // AUTOLOAD, a lazy definition is loaded before retrying the same
            // symbol: declaration side effects such as `gv-expander' are
            // intentionally installed by the owning file, not loaddefs.
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
                let Some(function) = interp.raw_function_binding(&symbol, env) else {
                    return Ok(Value::Nil);
                };
                if autoload.is_truthy() && autoload_parts(&function).is_some() {
                    let macro_only = if matches!(
                        &autoload,
                        Value::Symbol(name) if name == "macro"
                    ) {
                        Value::Symbol("macro".into())
                    } else {
                        Value::Nil
                    };
                    let loaded = super::call(
                        interp,
                        "autoload-do-load",
                        &[function.clone(), Value::Symbol(symbol.clone()), macro_only],
                        env,
                    )?;
                    if !values_equal(interp, &function, &loaded) {
                        continue;
                    }
                }
                match function {
                    Value::Symbol(next) if next != symbol => symbol = next,
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
            Ok(Value::Lambda(
                params,
                lambda_body.into(),
                shared_env(captured_env),
            ))
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
                *text = substitute_in_file_name_in_env(interp, env, text);
            }
            let process_environment = interp
                .lookup_var("process-environment", env)
                .unwrap_or(Value::Nil);
            let updated = updated_process_environment(
                &process_environment,
                &variable,
                value.as_deref(),
                true,
            )?;
            interp.set_variable("process-environment", updated, env);
            if variable == "TZ" {
                interp.local_time_zone_rule = value
                    .as_ref()
                    .map(|rule| Value::String(rule.clone()))
                    .unwrap_or_else(|| Value::Symbol("wall".into()));
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
            let (help_id, _) = get_or_create_buffer(interp, "*Help*");
            let mut docs = Vec::new();
            let target = args[0].as_symbol().ok().map(str::to_string);
            if let Some(symbol) = target.as_deref() {
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
                if docs.is_empty() {
                    // Same fallbacks as `documentation': the version's DOC
                    // file, then the lisp sources on the load path.
                    if let Some(doc) = fallback_function_documentation(interp, symbol) {
                        docs.push(doc);
                    }
                }
            } else if let Some(doc) = function_documentation(interp, &args[0], env) {
                docs.push(string_text(&doc)?);
            }
            // GNU renders the description into *Help*: a "NAME is a
            // function" header line followed by the docstring.
            let mut help_text = String::new();
            if let Some(symbol) = target.as_deref() {
                help_text.push_str(&format!("{symbol} is a function.\n\n"));
            }
            help_text.push_str(&docs.join("\n"));
            help_text.push('\n');
            let previous_buffer = interp.current_buffer_id();
            interp.switch_to_buffer_id(help_id)?;
            let start = interp.buffer.point_min();
            let end = interp.buffer.point_max();
            let _ = interp.delete_region_current_buffer(start, end);
            interp.insert_current_buffer(&help_text);
            interp.switch_to_buffer_id(previous_buffer)?;
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
        "macroexp-compiling-p" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::Nil)
        }
        "macroexp--dynamic-variable-p" => {
            need_args(name, args, 1)?;
            let var = args[0].as_symbol()?;
            // GNU: (or (not lexical-binding) (special-variable-p var)
            //          (memq var macroexp--dynvars) ...)
            let lexical = interp
                .lookup_var("lexical-binding", env)
                .is_some_and(|value| value.is_truthy());
            if !lexical || interp.is_dynamic_binding_name(var) || interp.local_special_declared(var)
            {
                return Ok(Value::T);
            }
            let dynvars = interp
                .lookup_var("macroexp--dynvars", env)
                .unwrap_or(Value::Nil);
            let found = dynvars.to_vec().is_ok_and(|items| {
                items
                    .iter()
                    .any(|item| matches!(item, Value::Symbol(name) if name == var))
            });
            Ok(if found { Value::T } else { Value::Nil })
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
                    interp.macroexpand_all_scoped_with_environment(&args[0], environment, env);
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
        "run-at-time" | "run-with-timer" | "run-with-idle-timer" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let repeat_secs = match args.get(1) {
                Some(Value::Integer(n)) => Some(*n as f64),
                Some(Value::Float(f)) => Some(*f),
                _ => None,
            };
            // GNU run-at-time TIME: nil/0 means fire at the next idle
            // opportunity; a number is seconds from now; t means the next
            // integral multiple of REPEAT.  Other forms (relative-time
            // strings, absolute timestamps) fall back to firing promptly.
            let delay_secs = match &args[0] {
                Value::Integer(n) => *n as f64,
                Value::Float(f) => *f,
                Value::T => repeat_secs.unwrap_or(0.0),
                _ => 0.0,
            };
            interp.schedule_timer_after(
                args[2].clone(),
                args[3..].to_vec(),
                delay_secs,
                repeat_secs,
            );
            // GNU returns a 10-slot timer vector (timer.el's cl-defstruct
            // with :type vector): [triggered high low usecs repeat-delay
            // function args idle-delay psecs integral-multiple].
            Ok(Value::list([
                Value::symbol("vector-literal"),
                Value::Nil,
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                args.get(1).cloned().unwrap_or(Value::Nil),
                args[2].clone(),
                Value::list(args[3..].to_vec()),
                if name == "run-with-idle-timer" {
                    Value::T
                } else {
                    Value::Nil
                },
                Value::Integer(0),
                Value::Nil,
            ]))
        }
        "cancel-timer" => {
            need_arg_range(name, args, 1, 1)?;
            if let Ok(items) = vector_items(&args[0])
                && items.len() == 10
            {
                let timer_args = items[6].to_vec().unwrap_or_default();
                interp.unschedule_timer_by_function_and_args(&items[5], &timer_args);
            }
            Ok(Value::Nil)
        }
        "timer-event-handler" => {
            need_args(name, args, 1)?;
            let items = vector_items(&args[0])?;
            if items.len() != 10 {
                return Err(LispError::TypeError("timerp".into(), args[0].type_name()));
            }
            // Fire the timer once, removing it from the native queue so a
            // later drain doesn't run it twice.  GNU reschedules repeating
            // timers; the native queue models one-shot firing.
            let timer_args = items[6].to_vec().unwrap_or_default();
            interp.unschedule_timer_by_function_and_args(&items[5], &timer_args);
            call_function_value(interp, &items[5], &timer_args, env)
        }
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
        "current-idle-time" => {
            need_args(name, args, 0)?;
            // Batch Emaxx has no input loop in which idle time accumulates.
            Ok(Value::Nil)
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
                (Value::StringObject(left), Value::StringObject(right)) => Rc::ptr_eq(left, right),
                (Value::Cons(left_car, left_cdr), Value::Cons(right_car, right_cdr)) => {
                    Rc::ptr_eq(left_car, right_car) && Rc::ptr_eq(left_cdr, right_cdr)
                }
                (Value::Lambda(_, left_body, _), Value::Lambda(_, right_body, _)) => {
                    Rc::ptr_eq(left_body, right_body)
                }
                (Value::Buffer(left, _), Value::Buffer(right, _))
                | (Value::Marker(left), Value::Marker(right))
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
        "executable-find" => {
            need_arg_range(name, args, 1, 2)?;
            let path = interp.lookup_var("exec-path", env).unwrap_or(Value::Nil);
            let suffixes = interp
                .lookup_var("exec-suffixes", env)
                .unwrap_or_else(|| Value::list([Value::String(String::new())]));
            // Keep the search semantics in the same producer as `locate-file':
            // in particular, an empty `exec-path' is one empty entry denoting
            // the dynamically bound `default-directory'.
            locate_file_internal(interp, &args[0], &path, &suffixes, &Value::Integer(1), env)
        }
        "add-hook" => {
            need_args(name, args, 2)?;
            let hook_name = args[0].as_symbol()?.to_string();
            let function = args[1].clone();
            // Since Emacs 29 the third argument is a numeric DEPTH.  The
            // historical non-nil APPEND values retain their old meaning by
            // mapping to depth 90.
            let depth = match args.get(2) {
                Some(Value::Integer(depth)) => *depth,
                Some(value) if value.is_truthy() => 90,
                _ => 0,
            };
            let local = args.get(3).is_some_and(|value| value.is_truthy());
            // GNU add-hook: a hook whose current value is a single function
            // (not a list) is first wrapped in a one-element list, so the
            // existing handler survives (erc's `422' hook holds the bare
            // symbol `erc-server-376' before the networks module adds to it).
            let mut hooks = if local {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .unwrap_or_else(|| vec![Value::T])
            } else if interp
                .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                .is_some()
            {
                // The buffer-local mirror (including its `t' splice
                // sentinel) shadows the global value here; GNU's global
                // add-hook reads the DEFAULT.
                interp
                    .default_value(&hook_name)
                    .map(hook_value_to_vec)
                    .unwrap_or_default()
            } else {
                interp
                    .lookup_var(&hook_name, env)
                    .map(hook_value_to_vec)
                    .unwrap_or_default()
            };
            if !hooks.contains(&function) {
                if depth > 0 {
                    hooks.push(function);
                } else {
                    hooks.insert(0, function);
                }
                if depth != 0 {
                    set_hook_function_depth(interp, &hook_name, &args[1], depth, local);
                }
                if let Some(depths) = hook_function_depths(interp, &hook_name, local) {
                    hooks.sort_by_key(|hook| hook_function_depth(&depths, hook));
                }
            }
            if local {
                let buffer_id = interp.current_buffer_id();
                interp.set_buffer_local_hook(buffer_id, &hook_name, hooks.clone());
                // Keep GNU's `t' sentinel in its depth-sorted position.  It
                // splices the default hook at depth zero, so positive local
                // functions run after the default while negative ones run
                // before it.
                interp.set_buffer_local_value(buffer_id, &hook_name, Value::list(hooks));
            } else if interp
                .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                .is_some()
            {
                // A plain set would hit the buffer-local mirror; GNU's
                // global add-hook writes the default (setq-default).
                super::call(
                    interp,
                    "set-default",
                    &[Value::Symbol(hook_name.clone()), Value::list(hooks)],
                    env,
                )?;
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
            // Merge global and buffer-local members like `run-hooks'
            // (also strips the local-hook `t' sentinel).
            let hook_values = hook_values(interp, hook_name, env, Some(interp.current_buffer_id()));
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
            // (remove-hook HOOK FUNCTION &optional LOCAL) — no DEPTH slot.
            let local = args.get(2).is_some_and(|value| value.is_truthy());
            if local
                && interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .is_none()
            {
                return Ok(Value::Nil);
            }
            let mut hooks = if local {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .unwrap_or_default()
            } else if interp
                .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                .is_some()
            {
                // The buffer-local mirror (including its `t' splice
                // sentinel) shadows the global value here; GNU's global
                // remove-hook reads the DEFAULT.
                interp
                    .default_value(&hook_name)
                    .map(hook_value_to_vec)
                    .unwrap_or_default()
            } else {
                interp
                    .lookup_var(&hook_name, env)
                    .map(hook_value_to_vec)
                    .unwrap_or_default()
            };
            let removed = hooks.iter().find(|hook| *hook == &function).cloned();
            hooks.retain(|hook| hook != &function);
            if let Some(removed) = removed {
                remove_hook_function_depth(interp, &hook_name, &removed, local);
            }
            if local {
                let buffer_id = interp.current_buffer_id();
                if hooks == [Value::T] {
                    // GNU kills the local binding when only the default-hook
                    // sentinel remains.
                    interp.remove_buffer_local_hook(buffer_id, &hook_name);
                    super::call(
                        interp,
                        "kill-local-variable",
                        &[Value::Symbol(hook_name.clone())],
                        env,
                    )?;
                } else {
                    interp.set_buffer_local_hook(buffer_id, &hook_name, hooks.clone());
                    interp.set_buffer_local_value(buffer_id, &hook_name, Value::list(hooks));
                }
            } else if interp
                .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                .is_some()
            {
                super::call(
                    interp,
                    "set-default",
                    &[Value::Symbol(hook_name.clone()), Value::list(hooks)],
                    env,
                )?;
            } else {
                interp.set_variable(&hook_name, Value::list(hooks), &mut Vec::new());
            }
            Ok(Value::Nil)
        }
        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

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
    let mut seconds = usage.ru_utime.tv_sec + usage.ru_stime.tv_sec;
    let mut micros = i64::from(usage.ru_utime.tv_usec) + i64::from(usage.ru_stime.tv_usec);
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

fn hook_value_to_vec(value: Value) -> Vec<Value> {
    match value.to_vec() {
        Ok(items) => items,
        Err(_) if value.is_nil() => Vec::new(),
        Err(_) => vec![value],
    }
}

fn hook_depth_symbol_name(interp: &Interpreter, hook_name: &str) -> Option<String> {
    interp
        .get_symbol_property(hook_name, "hook--depth-alist")
        .and_then(|value| value.as_symbol().ok().map(str::to_string))
}

fn ensure_hook_depth_symbol(interp: &mut Interpreter, hook_name: &str) -> String {
    if let Some(name) = hook_depth_symbol_name(interp, hook_name) {
        return name;
    }
    let id = MAKE_SYMBOL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let name = crate::lisp::types::make_uninterned_symbol_name("depth-alist", id);
    interp.put_symbol_property(hook_name, "hook--depth-alist", Value::Symbol(name.clone()));
    interp.set_global_binding(&name, Value::Nil);
    name
}

fn hook_function_depths(
    interp: &Interpreter,
    hook_name: &str,
    local: bool,
) -> Option<Vec<(Value, i64)>> {
    let depth_name = hook_depth_symbol_name(interp, hook_name)?;
    let value = if local {
        interp
            .buffer_local_value(interp.current_buffer_id(), &depth_name)
            .or_else(|| interp.default_value(&depth_name))
    } else {
        interp.default_value(&depth_name)
    }
    .unwrap_or(Value::Nil);
    Some(
        value
            .to_vec()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                let (function, depth) = entry.cons_values()?;
                Some((function, depth.as_integer().ok()?))
            })
            .collect(),
    )
}

fn store_hook_function_depths(
    interp: &mut Interpreter,
    depth_name: &str,
    depths: Vec<(Value, i64)>,
    local: bool,
) {
    let value = Value::list(
        depths
            .into_iter()
            .map(|(function, depth)| Value::cons(function, Value::Integer(depth))),
    );
    if local {
        interp.set_buffer_local_value(interp.current_buffer_id(), depth_name, value);
    } else {
        interp.set_global_binding(depth_name, value);
    }
}

fn set_hook_function_depth(
    interp: &mut Interpreter,
    hook_name: &str,
    function: &Value,
    depth: i64,
    local: bool,
) {
    let depth_name = ensure_hook_depth_symbol(interp, hook_name);
    if local
        && interp
            .buffer_local_value(interp.current_buffer_id(), &depth_name)
            .is_none()
    {
        let inherited = interp.default_value(&depth_name).unwrap_or(Value::Nil);
        interp.set_buffer_local_value(interp.current_buffer_id(), &depth_name, inherited);
    }
    let mut depths = hook_function_depths(interp, hook_name, local).unwrap_or_default();
    depths.retain(|(existing, _)| existing != function);
    depths.push((function.clone(), depth));
    store_hook_function_depths(interp, &depth_name, depths, local);
}

fn remove_hook_function_depth(
    interp: &mut Interpreter,
    hook_name: &str,
    function: &Value,
    local: bool,
) {
    let Some(depth_name) = hook_depth_symbol_name(interp, hook_name) else {
        return;
    };
    let Some(mut depths) = hook_function_depths(interp, hook_name, local) else {
        return;
    };
    let before = depths.len();
    depths.retain(|(existing, _)| existing != function);
    if depths.len() != before {
        store_hook_function_depths(interp, &depth_name, depths, local);
    }
}

fn hook_function_depth(depths: &[(Value, i64)], hook: &Value) -> i64 {
    depths
        .iter()
        .find_map(|(function, depth)| (function == hook).then_some(*depth))
        .unwrap_or(0)
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
        Ok(bytes) => Ok(decode_doc_string(&bytes, position)?.map(Value::String)),
        Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound) => {
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_else(|| path.to_str().unwrap_or(""));
            Ok(Some(Value::String(format!(
                "Cannot open doc string file \"{filename}\"\n"
            ))))
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
                    interp.builtin_doc_offsets.insert(native_name, entry.offset);
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
                    .map(Value::String)
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
            .map(Value::String)
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
fn builtin_doc_from_lisp_sources(interp: &Interpreter, function: &str) -> Option<String> {
    let lisp_root = lisp_source_root(interp)?;
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

pub(crate) fn fallback_function_documentation(
    interp: &Interpreter,
    function: &str,
) -> Option<String> {
    builtin_doc_from_doc_file(interp, function)
        .or_else(|| builtin_doc_from_lisp_sources(interp, function))
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
        let Ok(Some(arglist)) = crate::lisp::reader::Reader::new(&text[idx..]).read() else {
            continue;
        };
        idx = match skip_balanced_parens(bytes, idx) {
            Some(next) => next,
            None => continue,
        };
        idx = skip_ws(bytes, idx);
        // The docstring, if present, is the next form and starts with `"`.
        if idx >= bytes.len() || bytes[idx] != b'"' {
            continue;
        }
        if let Ok(Some(doc)) = crate::lisp::reader::Reader::new(&text[idx..]).read() {
            let mut doc = match doc {
                Value::String(text) => text,
                Value::StringObject(state) => state.borrow().text.clone(),
                _ => continue,
            };
            if !doc.contains("(fn") {
                let parameters = arglist
                    .to_vec()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|parameter| parameter.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                doc.push_str("\n\n(fn");
                if !parameters.is_empty() {
                    doc.push(' ');
                    doc.push_str(&parameters);
                }
                doc.push(')');
            }
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

const HEX_UTIL_DIGITS: &[u8; 16] = b"0123456789abcdef";

// hex-util.el hex-char-to-num, including its error for non-hex digits.
fn hex_util_digit(ch: char) -> Result<u32, LispError> {
    match ch {
        'a'..='f' => Ok(ch as u32 - 'a' as u32 + 10),
        'A'..='F' => Ok(ch as u32 - 'A' as u32 + 10),
        '0'..='9' => Ok(ch as u32 - '0' as u32),
        other => Err(LispError::Signal(format!(
            "Invalid hexadecimal digit `{other}'"
        ))),
    }
}

// Run rfc2104's HASH over INPUT, returning the hex string HASH yields.
fn rfc2104_hash_hex(
    interp: &mut Interpreter,
    hash: &Value,
    input: &[u8],
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    if let Value::Symbol(algorithm) = hash
        && matches!(
            algorithm.as_str(),
            "md5" | "sha1" | "sha224" | "sha256" | "sha384" | "sha512"
        )
    {
        return Ok(digest_hex(&secure_hash_digest(algorithm, input)?));
    }
    let arg = bytes_to_shared_unibyte_value(input);
    let result = interp.call_function_value(hash.clone(), None, &[arg], env)?;
    string_text(&result)
}

// As above but decoded to octets (the inner-hash packing step).
fn rfc2104_hash_digest(
    interp: &mut Interpreter,
    hash: &Value,
    input: &[u8],
    env: &mut crate::lisp::types::Env,
) -> Result<Vec<u8>, LispError> {
    let hex = rfc2104_hash_hex(interp, hash, input, env)?;
    let chars: Vec<char> = hex.chars().collect();
    let mut out = Vec::with_capacity(chars.len() / 2);
    for pair in chars.chunks(2) {
        if pair.len() == 2 {
            out.push((hex_util_digit(pair[0])? * 16 + hex_util_digit(pair[1])?) as u8);
        }
    }
    Ok(out)
}
