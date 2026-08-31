use super::*;

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Type predicates ──
            "null" => {
                need_args(name, args, 1)?;
                Ok(if args[0].is_nil() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "integerp" => {
                need_args(name, args, 1)?;
                Ok(if args[0].is_integer() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "numberp" => {
                need_args(name, args, 1)?;
                Ok(
                    if args[0].is_integer() || matches!(args[0], Value::Float(_)) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "number-or-marker-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if args[0].is_integer() || matches!(args[0], Value::Float(_) | Value::Marker(_))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "char-or-string-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0], Value::Integer(code) if (0..=0x10_FFFF).contains(&code))
                        || string_like(&args[0]).is_some()
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "arrayp" => {
                need_args(name, args, 1)?;
                Ok(
                    if string_like(&args[0]).is_some()
                        || is_vector_value(&args[0])
                        || is_bool_vector_value(interp, &args[0])
                        || matches!(args[0], Value::CharTable(_))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "sequencep" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0], Value::Nil | Value::Cons(_))
                        || string_like(&args[0]).is_some()
                        || is_vector_value(&args[0])
                        || is_bool_vector_value(interp, &args[0])
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "vectorp" => {
                need_args(name, args, 1)?;
                Ok(if is_vector_value(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "integer-or-marker-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(
                        args[0],
                        Value::Integer(_) | Value::BigInteger(_) | Value::Marker(_)
                    ) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "vector-or-char-table-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if is_vector_value(&args[0]) || matches!(args[0], Value::CharTable(_)) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "bool-vector-p" => {
                need_args(name, args, 1)?;
                Ok(if is_bool_vector_value(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "bool-vector-subsetp" => {
                need_args(name, args, 2)?;
                let left = bool_vector_bits(interp, &args[0])?;
                let right = bool_vector_bits(interp, &args[1])?;
                if left.len() != right.len() {
                    return Err(LispError::Signal("Args out of range".into()));
                }
                Ok(
                    if left
                        .iter()
                        .zip(&right)
                        .all(|(left_bit, right_bit)| !*left_bit || *right_bit)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "floatp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0], Value::Float(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "stringp" => {
                need_args(name, args, 1)?;
                Ok(if string_like(&args[0]).is_some() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "documentation-stringp" => {
                need_args(name, args, 1)?;
                let valid = matches!(&args[0], Value::Integer(_))
                    || string_like(&args[0]).is_some()
                    || args[0].cons_values().is_some_and(|(file, position)| {
                        string_like(&file).is_some() && matches!(position, Value::Integer(_))
                    });
                Ok(if valid { Value::T } else { Value::Nil })
            }
            "symbolp" => {
                need_args(name, args, 1)?;
                Ok(
                    if args[0].is_symbol()
                        || (symbols_with_pos_enabled(interp, env)
                            && symbol_with_pos_parts(interp, &args[0]).is_some())
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "keywordp" => {
                need_args(name, args, 1)?;
                let positioned_keyword = symbols_with_pos_enabled(interp, env)
                    .then(|| symbol_with_pos_parts(interp, &args[0]))
                    .flatten()
                    .is_some_and(|(symbol, _)| {
                        matches!(symbol, Value::Symbol(name) if name.starts_with(':'))
                    });
                Ok(
                    if matches!(&args[0], Value::Symbol(symbol) if symbol.starts_with(':'))
                        || positioned_keyword
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "functionp" => {
                need_args(name, args, 1)?;
                let symbol_value = if symbols_with_pos_enabled(interp, env) {
                    symbol_with_pos_parts(interp, &args[0])
                        .map(|(symbol, _)| symbol)
                        .unwrap_or_else(|| args[0].clone())
                } else {
                    args[0].clone()
                };
                let symbol = symbol_value.as_symbol().ok();
                if symbol.is_some_and(|symbol| {
                    crate::lisp::primitives::name_facts(symbol).special_form
                        || interp.has_macro_binding(symbol)
                }) {
                    return Ok(Value::Nil);
                }
                let value =
                    resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
                let autoloaded_function = symbol.is_some()
                    && autoload_parts(&value).is_some_and(|(_, _, kind)| kind.is_nil());
                Ok(
                    if callable_value_p(interp, &value, env) || autoloaded_function {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "byte-code-function-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if record_type_name(interp, &args[0]) == Some("byte-code-function")
                        || matches!(
                            &args[0],
                            Value::Lambda(lambda)
                                if lambda.params.as_slice() == ["vals", "start", "end"]
                        )
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "module-function-p" => {
                need_args(name, args, 1)?;
                Ok(Value::Nil)
            }
            "make-closure" => {
                // GNU Fmake_closure (alloc.c): copy PROTOTYPE, replacing the
                // first CLOSURE-VARS elements of its constants vector.
                need_args(name, args, 1)?;
                let Value::Record(id) = &args[0] else {
                    return Err(wrong_type_argument("byte-code-function-p", args[0].clone()));
                };
                let Some(record) = interp.find_record(*id) else {
                    return Err(wrong_type_argument("byte-code-function-p", args[0].clone()));
                };
                if record.kind != crate::lisp::eval::RecordKind::Closure {
                    return Err(wrong_type_argument("byte-code-function-p", args[0].clone()));
                }
                let mut slots = record.slots.clone();
                let mut constants = slots
                .get(2)
                .and_then(|slot| slot.to_vec().ok())
                .filter(|items| {
                    matches!(items.first(), Some(Value::Symbol(marker)) if marker == "vector-literal")
                })
                .map(|items| items[1..].to_vec())
                .ok_or_else(|| {
                    LispError::Signal("make-closure prototype has no constants vector".into())
                })?;
                let vars = &args[1..];
                if vars.len() > constants.len() {
                    return Err(LispError::Signal(
                        "Closure vars do not fit in constvec".into(),
                    ));
                }
                constants[..vars.len()].clone_from_slice(vars);
                slots[2] =
                    Value::list(std::iter::once(Value::symbol("vector-literal")).chain(constants));
                Ok(interp.create_pseudovector(
                    crate::lisp::eval::RecordKind::Closure,
                    "byte-code-function",
                    slots,
                ))
            }
            "make-byte-code" => {
                // GNU Fmake_byte_code (alloc.c): the arguments become the
                // closure's elements verbatim (arglist, code, constants,
                // depth, docstring, interactive-spec, extras).
                need_args(name, args, 4)?;
                Ok(interp.create_pseudovector(
                    crate::lisp::eval::RecordKind::Closure,
                    "byte-code-function",
                    args.to_vec(),
                ))
            }
            "byte-code" => {
                // GNU Fbyte_code (bytecode.c): execute BYTESTR against VECTOR
                // with MAXDEPTH as an argumentless program.
                need_args(name, args, 3)?;
                let slots = [
                    Value::Integer(0),
                    args[0].clone(),
                    args[1].clone(),
                    args[2].clone(),
                ];
                let object = crate::lisp::bytecode::ByteCodeObject::from_slots(&slots)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                    .ok_or_else(|| {
                        LispError::SignalValue(Value::list([
                            Value::Symbol("error".into()),
                            Value::String("Invalid byte-code".into()),
                        ]))
                    })?;
                crate::lisp::bytecode::vm::execute(interp, &object, &[], env)
            }
            "internal-stack-stats" => {
                // GNU logs bytecode-stack telemetry to stderr and returns nil;
                // the Emaxx VM keeps per-frame stacks, so there is no shared
                // stack to report.
                Ok(Value::Nil)
            }
            "user-ptrp" => {
                need_args(name, args, 1)?;
                // GNU's true case is the PVEC_USER_PTR variant created by a
                // native module.  Emaxx deliberately has no such Value variant
                // while module loading is absent.  Keep this match exhaustive:
                // adding module user pointers later must force this predicate to
                // be revisited instead of silently preserving a blanket nil.
                let is_user_ptr = match &args[0] {
                    Value::Nil
                    | Value::T
                    | Value::Integer(_)
                    | Value::BigInteger(_)
                    | Value::Float(_)
                    | Value::String(_)
                    | Value::StringObject(_)
                    | Value::Symbol(_)
                    | Value::Cons(_)
                    | Value::BuiltinFunc(_)
                    | Value::Lambda(_)
                    | Value::Buffer(_)
                    | Value::Marker(_)
                    | Value::Overlay(_)
                    | Value::CharTable(_)
                    | Value::Frame(_)
                    | Value::Terminal(_)
                    | Value::Record(_)
                    | Value::Finalizer(_)
                    | Value::ReaderForm(_)
                    | Value::Unbound => false,
                };
                Ok(if is_user_ptr { Value::T } else { Value::Nil })
            }
            "closurep" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0], Value::Lambda(_))
                        || record_type_name(interp, &args[0]) == Some("byte-code-function")
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "interpreted-function-p" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0], Value::Lambda(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "subrp" => {
                need_args(name, args, 1)?;
                // GNU data.c: t exactly for subr objects.  A native builtin
                // is a subr; disguising any as Lisp forged provenance.
                Ok(match &args[0] {
                    Value::BuiltinFunc(_) => Value::T,
                    _ => Value::Nil,
                })
            }
            "keymapp" => {
                need_args(name, args, 1)?;
                Ok(if is_keymap_value(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "current-active-maps" => {
                need_arg_range(name, args, 0, 2)?;
                let olp = args.first().is_some_and(Value::is_truthy);
                Ok(Value::list(current_active_maps(
                    interp,
                    env,
                    olp,
                    args.get(1),
                )?))
            }
            "commandp" => {
                need_args(name, args, 1)?;
                if let Ok(symbol) = args[0].as_symbol()
                    && interp
                        .get_symbol_property(symbol, "interactive-form")
                        .is_some()
                {
                    return Ok(Value::T);
                }
                let value =
                    resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
                if let Value::BuiltinFunc(name) = &value
                    && generated_builtin_arities::generated_builtin_command_p(name)
                {
                    return Ok(Value::T);
                }
                if autoload_command_p(&value)
                    || callable_interactive_form_items(interp, &value).is_some()
                {
                    return Ok(Value::T);
                }
                // OClosures may get their interactive form from the
                // `oclosure-interactive-form' generic (like GNU's commandp).
                if interp.has_lisp_function("oclosure-interactive-form")
                    && super::misc_keymaps::value_is_oclosure(interp, &value, env)
                    && interp
                        .call_function_value(
                            Value::Symbol("oclosure-interactive-form".into()),
                            Some("oclosure-interactive-form"),
                            std::slice::from_ref(&value),
                            env,
                        )?
                        .is_truthy()
                {
                    return Ok(Value::T);
                }
                Ok(Value::Nil)
            }
            "boundp" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fboundp uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                Ok(
                    if interp.symbol_value_cell(&symbol).is_ok()
                        || matches!(
                            symbol.as_str(),
                            "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum"
                        )
                        || symbol == "buffer-undo-list"
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "where-is-internal" => {
                need_arg_range(name, args, 1, 5)?;
                let first_only = args.get(2).is_some_and(Value::is_truthy);
                let nomenus = first_only
                    && !matches!(args.get(2), Some(Value::Symbol(mode)) if mode == "non-ascii");
                let keymaps = where_is_internal_maps(interp, args.get(1), env)?;
                let matches =
                    where_is_internal(interp, &args[0], &keymaps, first_only, nomenus, env)?;
                if first_only {
                    Ok(matches.into_iter().next().unwrap_or(Value::Nil))
                } else {
                    Ok(Value::list(matches))
                }
            }
            "default-boundp" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fdefault_boundp delegates to
                // default_value, whose first operation is CHECK_SYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                Ok(if interp.is_default_bound(&symbol) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "special-variable-p" => {
                need_args(name, args, 1)?;
                // GNU 30.2 eval.c:Fspecial_variable_p uses
                // CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                // GNU: the self-evaluating constants t/nil/keywords are declared
                // special (erc-button-setup's alist FORM check relies on
                // (special-variable-p t) being non-nil).
                Ok(
                    if matches!(symbol.as_str(), "t" | "nil")
                        || symbol.starts_with(':')
                        || interp.is_special_variable(&symbol)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "make-variable-buffer-local" => {
                need_args(name, args, 1)?;
                let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
                interp.mark_auto_buffer_local(&symbol);
                Ok(Value::Symbol(symbol.into()))
            }
            "local-variable-p" => {
                need_arg_range(name, args, 1, 2)?;
                let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
                let buffer_id = if let Some(buffer) = args.get(1) {
                    interp.resolve_buffer_id(buffer)?
                } else {
                    interp.current_buffer_id()
                };
                Ok(
                    if interp.buffer_local_value(buffer_id, &symbol).is_some()
                        || interp.is_always_buffer_local_special(&symbol)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "local-variable-if-set-p" => {
                need_arg_range(name, args, 1, 2)?;
                let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
                let buffer_id = if let Some(buffer) = args.get(1) {
                    interp.resolve_buffer_id(buffer)?
                } else {
                    interp.current_buffer_id()
                };
                Ok(
                    if interp.buffer_local_value(buffer_id, &symbol).is_some()
                        || interp.is_auto_buffer_local(&symbol)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "variable-binding-locus" => {
                need_args(name, args, 1)?;
                let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
                Ok(interp
                    .buffer_local_value(interp.current_buffer_id(), &symbol)
                    .and_then(|_| interp.buffer_identity_value(interp.current_buffer_id()))
                    .unwrap_or(Value::Nil))
            }
            "bare-symbol-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0], Value::Symbol(_) | Value::Nil | Value::T) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "symbol-with-pos-p" => {
                need_args(name, args, 1)?;
                Ok(if symbol_with_pos_parts(interp, &args[0]).is_some() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "fboundp" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Ffboundp uses CHECK_SYMBOL/XSYMBOL, so a
                // positioned symbol denotes its bare symbol exactly while
                // the dynamic reader switch is enabled.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                Ok(
                    if is_builtin(&symbol)
                        || interp.lookup_function(&symbol, env).is_ok()
                        || is_special_form_name(&symbol)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "featurep" => {
                need_args(name, args, 1)?;
                // GNU 30.2 fns.c:Ffeaturep uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                if !interp.has_feature(&symbol) {
                    return Ok(Value::Nil);
                }
                // GNU: with SUBFEATURE, check (memq SUBFEATURE (get FEATURE
                // 'subfeatures)) — `member', since subfeatures are lists like
                // (:family local).
                if let Some(subfeature) = args.get(1).filter(|value| value.is_truthy()) {
                    let subfeatures = interp
                        .get_symbol_property(&symbol, "subfeatures")
                        .unwrap_or(Value::Nil);
                    let found = subfeatures.to_vec().unwrap_or_default().iter().any(|item| {
                        crate::lisp::primitives::values_equal(interp, item, subfeature)
                    });
                    return Ok(if found { Value::T } else { Value::Nil });
                }
                Ok(Value::T)
            }
            "zlib-available-p" => Ok(Value::T),
            "libxml-available-p" => Ok(Value::T),
            "consp" => {
                need_args(name, args, 1)?;
                Ok(if is_cons_value(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "listp" => {
                need_args(name, args, 1)?;
                Ok(if args[0].is_nil() || is_cons_value(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "proper-list-p" => {
                need_args(name, args, 1)?;
                Ok(match keymap_list_items(interp, &args[0])? {
                    Some(items) => Value::Integer(items.len() as i64),
                    None => match proper_list_length(&args[0]) {
                        Some(length) => Value::Integer(length as i64),
                        None => Value::Nil,
                    },
                })
            }
            "bufferp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0], Value::Buffer(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "buffer-live-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(&args[0], Value::Buffer(buffer) if interp.has_buffer_id(buffer.id))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "processp" => {
                need_args(name, args, 1)?;
                Ok(if interp.resolve_process_id(&args[0]).is_ok() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "threadp" => {
                need_args(name, args, 1)?;
                Ok(if interp.resolve_thread_id(&args[0]).is_ok() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "mutexp" => {
                need_args(name, args, 1)?;
                Ok(if interp.resolve_mutex_id(&args[0]).is_ok() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "condition-variable-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.resolve_condition_variable_id(&args[0]).is_ok() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "minibufferp" => {
                if args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let buffer_id = match args.first() {
                    None | Some(Value::Nil) => Some(interp.current_buffer_id()),
                    Some(value) if string_like(value).is_some() => string_like(value)
                        .and_then(|name| interp.find_buffer(&name.text).map(|(id, _)| id)),
                    Some(value) => Some(interp.resolve_buffer_id(value)?),
                };
                let is_minibuffer = buffer_id
                    .and_then(|buffer_id| interp.get_buffer_by_id(buffer_id))
                    .map(|buffer| buffer.name.starts_with(" *Minibuf"))
                    .unwrap_or(false);
                let is_live = buffer_id == interp.active_minibuffer_buffer_id();
                let matches =
                    is_minibuffer && (!args.get(1).is_some_and(Value::is_truthy) || is_live);
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "minibuffer-depth" => {
                need_args(name, args, 0)?;
                Ok(Value::Integer(interp.minibuffer_depth() as i64))
            }
            "minibuffer-prompt" => {
                need_args(name, args, 0)?;
                Ok(interp
                    .minibuffer_prompt_text()
                    .map(|prompt| Value::String(prompt.into()))
                    .unwrap_or(Value::Nil))
            }
            "innermost-minibuffer-p" | "minibuffer-innermost-command-loop-p" => {
                need_arg_range(name, args, 0, 1)?;
                let target = match args.first() {
                    None | Some(Value::Nil) => interp
                        .buffer_identity_value(interp.current_buffer_id())
                        .unwrap_or(Value::Nil),
                    Some(value) => value.clone(),
                };
                let active = interp
                    .active_minibuffer_buffer_id()
                    .and_then(|buffer_id| interp.buffer_identity_value(buffer_id))
                    .unwrap_or(Value::Nil);
                let matches = if name == "minibuffer-innermost-command-loop-p" {
                    !active.is_nil() && values_equal(interp, &target, &active)
                } else if active.is_nil() {
                    interp
                        .find_buffer(" *Minibuf-0*")
                        .and_then(|(id, _)| interp.buffer_identity_value(id))
                        .is_some_and(|buffer| values_equal(interp, &target, &buffer))
                } else {
                    values_equal(interp, &target, &active)
                };
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "abort-minibuffers" => {
                need_args(name, args, 0)?;
                if interp.active_minibuffer_buffer_id().is_none() {
                    return Err(LispError::Signal("Not in a minibuffer".into()));
                }
                Err(LispError::SignalValue(Value::list([Value::symbol(
                    "minibuffer-quit",
                )])))
            }

            "natnump" => {
                need_args(name, args, 1)?;
                Ok(match &args[0] {
                    Value::Integer(n) if *n >= 0 => Value::T,
                    Value::BigInteger(n) if n.sign() != Sign::Minus => Value::T,
                    _ => Value::Nil,
                })
            }

            "atom" => {
                need_args(name, args, 1)?;
                Ok(
                    if (args[0].is_cons() && !is_vector_like_value(interp, &args[0]))
                        || keymap_list_items(interp, &args[0])?.is_some()
                    {
                        Value::Nil
                    } else {
                        Value::T
                    },
                )
            }

            "nlistp" => {
                need_args(name, args, 1)?;
                Ok(
                    if (args[0].is_list() && !is_vector_like_value(interp, &args[0]))
                        || keymap_list_items(interp, &args[0])?.is_some()
                    {
                        Value::Nil
                    } else {
                        Value::T
                    },
                )
            }

            "characterp" => {
                need_args(name, args, 1)?;
                // In Emacs, characters are integers 0..#x3FFFFF
                Ok(match &args[0] {
                    Value::Integer(n) if *n >= 0 && *n <= 0x3F_FFFF => Value::T,
                    Value::BigInteger(n)
                        if n.sign() != Sign::Minus && **n <= BigInt::from(0x3F_FFFFu32) =>
                    {
                        Value::T
                    }
                    _ => Value::Nil,
                })
            }
            "markerp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0], Value::Marker(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "recordp" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0], Value::Record(id)
                        if interp.find_record(id).is_some_and(|record|
                            record.kind == crate::lisp::eval::RecordKind::Record))
                        || record_literal_items(&args[0]).is_some()
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
        }
    }
);
