use super::*;
use crate::lisp::types::Kind;

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Type predicates ──
            "null" => direct_null(interp, args, env),
            "integerp" => direct_integerp(interp, args, env),
            "numberp" => direct_numberp(interp, args, env),
            "number-or-marker-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if args[0].is_integer()
                        || matches!(args[0].kind(), Kind::Float(_) | Kind::Marker(_))
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
                    // data.c:Fchar_or_string_p uses character.h:MAX_CHAR,
                    // not Unicode's scalar limit, and STRINGP reads a tag.
                    if matches!(args[0].kind(), Kind::Integer(code) if (0..=0x3F_FFFF).contains(&code))
                        || args[0].is_string()
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "arrayp" => {
                need_args(name, args, 1)?;
                Ok(if args[0].is_string() || is_vector_like_value(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "sequencep" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0].kind(), Kind::Nil | Kind::Cons(_))
                        || args[0].is_string()
                        || is_vector_like_value(&args[0])
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
                        args[0].kind(),
                        Kind::Integer(_) | Kind::BigInteger(_) | Kind::Marker(_)
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
                    if is_vector_value(&args[0]) || matches!(args[0].kind(), Kind::CharTable(_)) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "bool-vector-p" => {
                need_args(name, args, 1)?;
                Ok(if is_bool_vector_value(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "bool-vector-subsetp" => {
                need_args(name, args, 2)?;
                super::collections::bool_vector_subsetp(&args[0], &args[1])
            }
            "floatp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::Float(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "stringp" => direct_stringp(interp, args, env),
            "documentation-stringp" => {
                need_args(name, args, 1)?;
                let valid = matches!(args[0].kind(), Kind::Integer(_))
                    || args[0].is_string()
                    || args[0].cons_values().is_some_and(|(file, position)| {
                        file.is_string() && matches!(position.kind(), Kind::Integer(_))
                    });
                Ok(if valid { Value::T } else { Value::Nil })
            }
            "symbolp" => direct_symbolp(interp, args, env),
            "keywordp" => {
                need_args(name, args, 1)?;
                let positioned_keyword = symbols_with_pos_enabled(interp, env)
                    .then(|| symbol_with_pos_parts(interp, &args[0]))
                    .flatten()
                    .is_some_and(|(symbol, _)| {
                        matches!(symbol.kind(), Kind::Symbol(name) if name.is_keyword())
                    });
                Ok(
                    if matches!(args[0].kind(), Kind::Symbol(symbol) if symbol.is_keyword())
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
                Ok(if function_value_p(interp, &args[0], env) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "byte-code-function-p" => {
                need_args(name, args, 1)?;
                // data.c:Fbyte_code_function_p checks CLOSUREP and the
                // code slot's STRINGP tag, never names or parameter lists.
                Ok(
                    if matches!(args[0].kind(), Kind::Closure(closure) if closure.is_bytecode()) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "module-function-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0].kind(), Kind::Record(id) if interp.find_record(id).is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::ModuleFunction))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "make-closure" => {
                need_args(name, args, 1)?;
                interp.make_closure(&args[0], &args[1..])
            }
            "make-byte-code" => {
                // GNU Fmake_byte_code (alloc.c): the arguments become the
                // closure's elements verbatim (arglist, code, constants,
                // depth, docstring, interactive-spec, extras).
                need_args(name, args, 4)?;
                // alloc.c checks these four field types before allocating.
                // It does not validate the arglist spine, packed descriptor,
                // opcodes, or optional/extra slots until their later uses.
                if !(matches!(args[0].kind(), Kind::Integer(_) | Kind::Cons(_) | Kind::Nil)
                    && args[1].is_string()
                    && !string_argument_multibyte(&args[1])
                    && matches!(args[2].kind(), Kind::Vector(_))
                    && matches!(args[3].kind(), Kind::Integer(depth) if depth >= 0))
                {
                    return Err(LispError::Signal("Invalid byte-code object".into()));
                }
                Ok(Value::allocated_closure(args))
            }
            "byte-code" => {
                // GNU Fbyte_code (bytecode.c): execute BYTESTR against VECTOR
                // with MAXDEPTH as an argumentless program.
                need_args(name, args, 3)?;
                if !(args[0].is_string()
                    && matches!(args[1].kind(), Kind::Vector(_))
                    && matches!(args[2].kind(), Kind::Integer(depth) if depth >= 0))
                {
                    return Err(LispError::Signal("Invalid byte-code".into()));
                }
                // bytecode.c:Fbyte_code accepts old multibyte code strings,
                // converts them with string-as-unibyte, then constructs an
                // ordinary closure which roots both code and constants.
                let code = if string_argument_multibyte(&args[0]) {
                    super::super::call(interp, "string-as-unibyte", &args[..1], env)?
                } else {
                    args[0]
                };
                let function = Value::allocated_closure(&[Value::Nil, code, args[1], args[2]]);
                let Kind::Closure(closure) = function.kind() else {
                    unreachable!("allocated bytecode closure")
                };
                crate::lisp::bytecode::vm::execute_closure(interp, closure, &[], env)
            }
            "internal-stack-stats" => {
                // GNU logs bytecode-stack telemetry to stderr and returns nil;
                // the Emaxx VM keeps per-frame stacks, so there is no shared
                // stack to report.
                Ok(Value::Nil)
            }
            "user-ptrp" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0].kind(), Kind::Record(id) if interp.find_record(id).is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::UserPointer))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "closurep" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::Closure(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "interpreted-function-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0].kind(), Kind::Closure(closure) if !closure.is_bytecode()) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "subrp" => {
                need_args(name, args, 1)?;
                // GNU data.c: t exactly for subr objects.  A native builtin
                // is a subr; disguising any as Lisp forged provenance.
                Ok(match args[0].kind() {
                    Kind::BuiltinFunc(_) => Value::T,
                    Kind::NativeFunction(_) => Value::T,
                    _ => Value::Nil,
                })
            }
            "keymapp" => {
                need_args(name, args, 1)?;
                Ok(if keymapp(interp, args[0], env) {
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
                need_arg_range(name, args, 1, 2)?;
                command_p(
                    interp,
                    args[0],
                    args.get(1).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "boundp" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fboundp uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                Ok(
                    if matches!(interp.dynamic_value_symbol(&symbol), Ok(Some(_))) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "where-is-internal" => {
                need_arg_range(name, args, 1, 5)?;
                let arg = |index: usize| args.get(index).copied().unwrap_or(Value::Nil);
                where_is_internal_command(
                    interp,
                    args[0],
                    arg(1),
                    arg(2),
                    arg(3).is_truthy(),
                    arg(4).is_truthy(),
                    env,
                )
            }
            "default-boundp" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fdefault_boundp delegates to
                // default_value, whose first operation is CHECK_SYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                Ok(if interp.default_value_symbol(&symbol)?.is_some() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "special-variable-p" => {
                need_args(name, args, 1)?;
                // GNU 30.2 eval.c:Fspecial_variable_p uses
                // CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                Ok(if interp.is_special_variable_symbol(&symbol) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "make-variable-buffer-local" => {
                need_args(name, args, 1)?;
                // data.c:Fmake_variable_buffer_local: CHECK_SYMBOL, follow the
                // alias, a void plain value becomes nil, SYMBOL_CONSTANT_P on
                // VARIABLE signals, then `blv->local_if_set = 1'; VARIABLE is
                // returned as given (no XSETSYMBOL here).
                let checked = checked_symbol_identity(interp, &args[0], env)?;
                let symbol = interp.resolve_variable_symbol(&checked)?;
                super::buffer_meta::check_may_be_buffer_local(interp, &checked, &symbol)?;
                if interp.is_constant_symbol(&checked) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("setting-constant"),
                        args[0],
                    ])));
                }
                if !interp.is_per_buffer_symbol(&symbol)
                    && interp
                        .default_value_symbol(&symbol)
                        .ok()
                        .flatten()
                        .is_none()
                {
                    interp.set_global_binding_resolved(&symbol, Value::Nil);
                }
                interp.mark_auto_buffer_local_symbol(&symbol);
                Ok(args[0])
            }
            "local-variable-p" => {
                need_arg_range(name, args, 1, 2)?;
                // data.c:Flocal_variable_p: decode_buffer, then CHECK_SYMBOL.
                // A killed buffer keeps no alist.
                let buffer_id = interp.decode_buffer(args.get(1))?.id;
                let symbol = interp
                    .resolve_variable_symbol(&checked_symbol_identity(interp, &args[0], env)?)?;
                // data.c:Flocal_variable_p: an alist cell, bound or void,
                // or a per-buffer slot that is local in every buffer.
                Ok(
                    if interp.has_buffer_local_binding_symbol(buffer_id, &symbol)
                        || interp.is_always_buffer_local_symbol(&symbol)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "local-variable-if-set-p" => {
                need_arg_range(name, args, 1, 2)?;
                let symbol = interp
                    .resolve_variable_symbol(&checked_symbol_identity(interp, &args[0], env)?)?;
                // data.c:Flocal_variable_if_set_p: a variable local when
                // set, or a buffer's slot, is t and a plain one nil,
                // whatever BUFFER is; only a localized one asks
                // Flocal_variable_p, of BUFFER decoded.
                let buffer_id = if interp.is_auto_buffer_local_symbol(&symbol)
                    || interp.symbol_redirect(&symbol) != crate::lisp::eval::Redirect::Localized
                {
                    interp.current_buffer_id()
                } else {
                    interp.decode_buffer(args.get(1))?.id
                };
                Ok(if interp.local_variable_if_set_p(&symbol, buffer_id) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "variable-binding-locus" => {
                need_args(name, args, 1)?;
                let symbol = interp
                    .resolve_variable_symbol(&checked_symbol_identity(interp, &args[0], env)?)?;
                // data.c:Fvariable_binding_locus asks Flocal_variable_p: a
                // binding cell, bound or void, names the buffer.
                Ok(interp
                    .has_buffer_local_binding_symbol(interp.current_buffer_id(), &symbol)
                    .then(|| interp.buffer_identity_value(interp.current_buffer_id()))
                    .flatten()
                    .unwrap_or(Value::Nil))
            }
            "bare-symbol-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0].kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "symbol-with-pos-p" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::SymbolWithPos(_)) {
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
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                Ok(
                    if interp
                        .raw_function_binding_symbol(&symbol, env)
                        .is_some_and(|function| !function.is_nil())
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
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                if !interp.has_feature_symbol(symbol) {
                    return Ok(Value::Nil);
                }
                // GNU: with SUBFEATURE, check (memq SUBFEATURE (get FEATURE
                // 'subfeatures)) — `member', since subfeatures are lists like
                // (:family local).
                if let Some(subfeature) = args.get(1).filter(|value| value.is_truthy()) {
                    let subfeatures = interp
                        .get_property_of_symbol_value(Value::Symbol(symbol), "subfeatures")
                        .unwrap_or(Value::Nil);
                    let mut found = false;
                    for item in subfeatures.to_vec().unwrap_or_default() {
                        if crate::lisp::primitives::values_equal(interp, &item, subfeature)? {
                            found = true;
                            break;
                        }
                    }
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
            "listp" => direct_listp(interp, args, env),
            "proper-list-p" => {
                need_args(name, args, 1)?;
                Ok(match proper_list_length(&args[0]) {
                    Some(length) => Value::Integer(length as i64),
                    None => Value::Nil,
                })
            }
            "bufferp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::Buffer(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "buffer-live-p" => {
                need_args(name, args, 1)?;
                Ok(
                    // BUFFER_LIVE_P of the object itself.
                    if matches!(args[0].kind(), Kind::Buffer(buffer) if buffer.is_live()) {
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
                // minibuf.c:Fminibufferp: a string through Fget_buffer (nil
                // for no such buffer), else CHECK_BUFFER; then Fmemq of
                // Vminibuffer_list, or live_minibuffer_p with LIVE.
                let buffer = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => Value::Buffer(interp.buffer),
                    Some(Kind::Buffer(_)) => args[0],
                    Some(Kind::StringObject(_)) => interp
                        .get_buffer(&args[0])?
                        .map_or(Value::Nil, Value::Buffer),
                    Some(_) => {
                        return Err(LispError::WrongTypeArgument("bufferp".into(), args[0]));
                    }
                };
                let matches = if args.get(1).is_some_and(Value::is_truthy) {
                    crate::lisp::primitives::live_minibuffer_p(interp, &buffer)
                } else {
                    crate::lisp::primitives::minibuffer_list_has(interp, &buffer)
                };
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
            "minibuffer-innermost-command-loop-p" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => interp
                        .buffer_identity_value(interp.current_buffer_id())
                        .unwrap_or(Value::Nil),
                    Some(value) => value.value(),
                };
                Ok(
                    if crate::lisp::primitives::minibuffer_innermost_command_loop_p(interp, &buffer)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "innermost-minibuffer-p" => {
                need_arg_range(name, args, 0, 1)?;
                let target = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => interp
                        .buffer_identity_value(interp.current_buffer_id())
                        .unwrap_or(Value::Nil),
                    Some(value) => value.value(),
                };
                // EQ (buffer, Fcar (Fnthcdr (make_fixnum (minibuf_level),
                // Vminibuffer_list))).
                let innermost =
                    crate::lisp::primitives::nth_minibuffer(interp, interp.minibuffer_depth());
                Ok(if innermost.eq_value(target) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "abort-minibuffers" => {
                // minibuf.c:Fabort_minibuffers.
                need_args(name, args, 0)?;
                let buffer = interp
                    .buffer_identity_value(interp.current_buffer_id())
                    .unwrap_or(Value::Nil);
                let depth = crate::lisp::primitives::this_minibuffer_depth(interp, &buffer);
                if depth == 0 {
                    return Err(LispError::Signal("Not in a minibuffer".into()));
                }
                if !crate::lisp::primitives::minibuffer_innermost_command_loop_p(interp, &buffer) {
                    return Err(LispError::Signal("Not in most nested command loop".into()));
                }
                let level = interp.minibuffer_depth();
                if depth < level {
                    let levels = Value::Integer((level - depth + 1) as i64);
                    let prompt = super::super::call(
                        interp,
                        "format",
                        &[Value::string("Abort %s minibuffer levels? "), levels],
                        env,
                    )?;
                    if super::super::call(interp, "yes-or-no-p", &[prompt], env)?.is_truthy() {
                        // The current minibuffer is in the most nested
                        // command loop, so no non-minibuffer recursive edits
                        // are aborted with it.
                        interp.call_function_value(
                            Value::symbol("minibuffer-quit-recursive-edit"),
                            Some("minibuffer-quit-recursive-edit"),
                            &[levels],
                            env,
                        )?;
                    }
                } else {
                    interp.call_function_value(
                        Value::symbol("minibuffer-quit-recursive-edit"),
                        Some("minibuffer-quit-recursive-edit"),
                        &[],
                        env,
                    )?;
                }
                Ok(Value::Nil)
            }

            "natnump" => {
                need_args(name, args, 1)?;
                Ok(match args[0].kind() {
                    Kind::Integer(n) if n >= 0 => Value::T,
                    Kind::BigInteger(n) if n.sign() != Sign::Minus => Value::T,
                    _ => Value::Nil,
                })
            }

            "atom" => {
                need_args(name, args, 1)?;
                Ok(if args[0].is_cons() && !is_vector_like_value(&args[0]) {
                    Value::Nil
                } else {
                    Value::T
                })
            }

            "nlistp" => {
                need_args(name, args, 1)?;
                Ok(if args[0].is_list() && !is_vector_like_value(&args[0]) {
                    Value::Nil
                } else {
                    Value::T
                })
            }

            "characterp" => {
                need_args(name, args, 1)?;
                // In Emacs, characters are integers 0..#x3FFFFF
                Ok(match args[0].kind() {
                    Kind::Integer(n) if (0..=0x3F_FFFF).contains(&n) => Value::T,
                    Kind::BigInteger(n)
                        if n.sign() != Sign::Minus && *n <= BigInt::from(0x3F_FFFFu32) =>
                    {
                        Value::T
                    }
                    _ => Value::Nil,
                })
            }
            "markerp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::Marker(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "recordp" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0].kind(), Kind::LispRecord(_))
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

/// The `null' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_null(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "null";
    need_args(name, args, 1)?;
    Ok(if args[0].is_nil() {
        Value::T
    } else {
        Value::Nil
    })
}

/// The `integerp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_integerp(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "integerp";
    need_args(name, args, 1)?;
    Ok(if args[0].is_integer() {
        Value::T
    } else {
        Value::Nil
    })
}

/// The `numberp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_numberp(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "numberp";
    need_args(name, args, 1)?;
    Ok(
        if args[0].is_integer() || matches!(args[0].kind(), Kind::Float(_)) {
            Value::T
        } else {
            Value::Nil
        },
    )
}

/// The `stringp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_stringp(
    _interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "stringp";
    need_args(name, args, 1)?;
    Ok(if args[0].is_string() {
        Value::T
    } else {
        Value::Nil
    })
}

/// The `symbolp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_symbolp(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "symbolp";
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

/// The `listp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_listp(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "listp";
    need_args(name, args, 1)?;
    Ok(if args[0].is_nil() || is_cons_value(interp, &args[0]) {
        Value::T
    } else {
        Value::Nil
    })
}

/// lisp.h:VALID_DOCSTRING_P: a docstring slot holding a string, a file
/// offset, or a (FILE . OFFSET) pair.
fn valid_docstring_p(doc: &Value) -> bool {
    doc.fixnum().is_some()
        || doc.is_string()
        || doc
            .cons_ref()
            .is_some_and(|cell| cell.car.get().is_string() && cell.cdr.get().fixnum().is_some())
}

/// eval.c:Fcommandp.  FUNCTION's indirect function answers by its kind: a
/// subr, a closure long enough for an interactive slot, or a module
/// function with an interactive form is a command; strings and vectors
/// are keyboard macros unless FOR-CALL-INTERACTIVELY; an autoload with a
/// non-nil fourth element is one; any other list is a command when it is
/// a `lambda' whose body has an `interactive' entry (Fassq), and nothing
/// else is.  A subr, closure, module function or autoload that is no
/// command then walks FUNCTION's symbol chain, where an
/// `interactive-form' property is an error, and a closure whose docstring
/// slot holds no docstring (an OClosure) asks `interactive-form'.
pub(crate) fn command_p(
    interp: &mut Interpreter,
    function: Value,
    for_call_interactively: bool,
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    // SYMBOLP takes a symbol with position as its bare symbol while
    // `symbols-with-pos-enabled'.
    let function = if matches!(function.kind(), Kind::SymbolWithPos(_))
        && symbols_with_pos_enabled(interp, env)
    {
        symbol_with_pos_parts(interp, &function).map_or(function, |(symbol, _)| symbol)
    } else {
        function
    };
    // `t' is a symbol whose function cell `fset' may fill.
    let chain_symbol = |value: Value| match value.kind() {
        Kind::Symbol(symbol) => Some(symbol),
        Kind::T => Some(crate::lisp::types::symbols::builtin_boolean(true)),
        _ => None,
    };
    // data.c:indirect_function: a void cell ends the chain at nil.
    let fun = match chain_symbol(function) {
        Some(symbol) => interp
            .lookup_function_symbol(&symbol, env)
            .unwrap_or(Value::Nil),
        None => function,
    };
    let mut genfun = false;
    match fun.kind() {
        Kind::Nil => return Ok(Value::Nil),
        Kind::BuiltinFunc(builtin) => {
            if generated_builtin_arities::generated_builtin_command_p(&builtin) {
                return Ok(Value::T);
            }
        }
        Kind::NativeFunction(subr) => {
            if subr.interactive().is_truthy() {
                return Ok(Value::T);
            }
        }
        Kind::Closure(closure) => {
            // PVSIZE > CLOSURE_INTERACTIVE, else an invalid docstring at
            // CLOSURE_DOC_STRING marks an OClosure.
            if closure.public_len() > 5 {
                return Ok(Value::T);
            }
            if let Some(doc) = closure.documentation() {
                genfun = !(doc.is_nil() || valid_docstring_p(&doc));
            }
        }
        Kind::Record(id) => {
            let Some(record) = interp
                .find_record(id)
                .filter(|record| record.kind == crate::lisp::eval::RecordKind::ModuleFunction)
            else {
                return Ok(Value::Nil);
            };
            if record.slots.get(1).is_some_and(|form| !form.is_nil()) {
                return Ok(Value::T);
            }
        }
        Kind::StringObject(_) | Kind::Vector(_) => {
            return Ok(if for_call_interactively {
                Value::Nil
            } else {
                Value::T
            });
        }
        Kind::Cons(cell) => {
            let funcar = cell.car.get();
            let is_head = |head: &str| {
                matches!(funcar.kind(), Kind::Symbol(symbol) if symbol.is_named(head))
                    || (matches!(funcar.kind(), Kind::SymbolWithPos(_))
                        && matches!(
                            symbol_with_pos_parts(interp, &funcar).map(|(symbol, _)| symbol.kind()),
                            Some(Kind::Symbol(symbol)) if symbol.is_named(head)
                        )
                        && symbols_with_pos_enabled(interp, env))
            };
            if is_head("autoload") {
                // Fcar (Fcdr (Fcdr (XCDR (fun)))).
                if cell.cdr.get().cdr()?.cdr()?.car()?.is_truthy() {
                    return Ok(Value::T);
                }
            } else {
                if !is_head("lambda") {
                    return Ok(Value::Nil);
                }
                let body = cell
                    .cdr
                    .get()
                    .cons_ref()
                    .map_or(Value::Nil, |rest| rest.cdr.get());
                let interactive = Value::Symbol(cached_symbol!("interactive").with(Clone::clone));
                return Ok(
                    if super::lists::direct_assq(interp, &[interactive, body], env)?.is_nil() {
                        Value::Nil
                    } else {
                        Value::T
                    },
                );
            }
        }
        _ => return Ok(Value::Nil),
    }
    // An `interactive-form' property anywhere on the symbol chain is an
    // error, as the function-documentation property analogue.
    let mut link = function;
    while let Some(symbol) = chain_symbol(link) {
        if cached_symbol!("interactive-form")
            .with(|property| interp.get_symbol_property_of(&symbol, property))
            .is_some_and(|form| !form.is_nil())
        {
            return Err(crate::lisp::primitives::values::doprnt_error(
                interp,
                "Found an 'interactive-form' property!",
                &[],
            ));
        }
        link = interp
            .raw_function_binding_symbol(&symbol, env)
            .unwrap_or(Value::Nil);
    }
    if !genfun {
        return Ok(Value::Nil);
    }
    let iform = interp.call_function_value(
        Value::Symbol(cached_symbol!("interactive-form").with(Clone::clone)),
        Some("interactive-form"),
        std::slice::from_ref(&fun),
        env,
    )?;
    Ok(if iform.is_nil() { Value::Nil } else { Value::T })
}
