use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "null"
            | "not"
            | "xor"
            | "integerp"
            | "cl-evenp"
            | "cl-oddp"
            | "fixnump"
            | "bignump"
            | "booleanp"
            | "numberp"
            | "number-or-marker-p"
            | "char-or-string-p"
            | "eventp"
            | "arrayp"
            | "sequencep"
            | "vectorp"
            | "integer-or-marker-p"
            | "vector-or-char-table-p"
            | "bool-vector-p"
            | "bool-vector-subsetp"
            | "floatp"
            | "stringp"
            | "documentation-stringp"
            | "symbolp"
            | "keywordp"
            | "functionp"
            | "cl-struct-p"
            | "compiled-function-p"
            | "byte-code-function-p"
            | "closurep"
            | "interpreted-function-p"
            | "module-function-p"
            | "user-ptrp"
            | "subrp"
            | "special-form-p"
            | "oddp"
            | "keymapp"
            | "current-active-maps"
            | "commandp"
            | "boundp"
            | "where-is-internal"
            | "default-boundp"
            | "special-variable-p"
            | "make-variable-buffer-local"
            | "local-variable-p"
            | "local-variable-if-set-p"
            | "variable-binding-locus"
            | "bare-symbol-p"
            | "symbol-with-pos-p"
            | "fboundp"
            | "facep"
            | "face-equal"
            | "face-differs-from-default-p"
            | "face-list"
            | "face-valid-attribute-values"
            | "seq-some"
            | "any"
            | "featurep"
            | "zlib-available-p"
            | "gnutls-available-p"
            | "libxml-available-p"
            | "consp"
            | "listp"
            | "cl-endp"
            | "proper-list-p"
            | "bufferp"
            | "buffer-live-p"
            | "processp"
            | "threadp"
            | "mutexp"
            | "condition-variable-p"
            | "minibufferp"
            | "innermost-minibuffer-p"
            | "minibuffer-innermost-command-loop-p"
            | "minibuffer-depth"
            | "minibuffer-prompt"
            | "abort-minibuffers"
            | "zerop"
            | "natnump"
            | "atom"
            | "nlistp"
            | "characterp"
            | "markerp"
            | "recordp"
    )
}

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
        "not" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_nil() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "xor" => {
            need_args(name, args, 2)?;
            Ok(if args[0].is_truthy() ^ args[1].is_truthy() {
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
        "cl-evenp" | "cl-oddp" => {
            need_args(name, args, 1)?;
            let value = integer_like_bigint(interp, &args[0])?;
            let is_odd = !(&value % BigInt::from(2)).is_zero();
            Ok(
                if (name == "cl-oddp" && is_odd) || (name == "cl-evenp" && !is_odd) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "fixnump" => {
            need_args(name, args, 1)?;
            let (min_fixnum, max_fixnum) = fixnum_bounds(interp)?;
            Ok(
                if integer_like_bigint(interp, &args[0])
                    .ok()
                    .is_some_and(|value| {
                        value >= BigInt::from(min_fixnum) && value <= BigInt::from(max_fixnum)
                    })
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "bignump" => {
            need_args(name, args, 1)?;
            let (min_fixnum, max_fixnum) = fixnum_bounds(interp)?;
            Ok(
                if integer_like_bigint(interp, &args[0])
                    .ok()
                    .is_some_and(|value| {
                        value < BigInt::from(min_fixnum) || value > BigInt::from(max_fixnum)
                    })
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "booleanp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Nil | Value::T) {
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
                if args[0].is_integer() || matches!(args[0], Value::Float(_) | Value::Marker(_)) {
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
        "eventp" => {
            need_args(name, args, 1)?;
            let truthy = match &args[0] {
                Value::Integer(_) => true,
                Value::Symbol(symbol_name) => symbol_name != "nil" && !symbol_name.starts_with(':'),
                Value::T => true,
                Value::Cons(car, _) => matches!(&*car.borrow(), Value::Symbol(_) | Value::T),
                _ => false,
            };
            Ok(if truthy { Value::T } else { Value::Nil })
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
                if matches!(args[0], Value::Nil | Value::Cons(_, _))
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
            Ok(if args[0].is_symbol() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "keywordp" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Symbol(symbol) if symbol.starts_with(':')) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "functionp" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol().ok();
            if symbol.is_some_and(|symbol| {
                crate::lisp::primitives::name_facts(symbol).special_form
                    || interp.has_macro_binding(symbol)
            }) {
                return Ok(Value::Nil);
            }
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            let autoloaded_function = symbol.is_some()
                && autoload_parts(&value).is_some_and(|(_, _, kind)| kind.is_nil());
            Ok(
                if matches!(value, Value::BuiltinFunc(_) | Value::Lambda(_, _, _))
                    || is_lambda_expression(&value)
                    || record_type_name(interp, &value) == Some("byte-code-function")
                    || autoloaded_function
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "cl-struct-p" => {
            need_args(name, args, 1)?;
            super::call(
                interp,
                "cl-typep",
                &[args[0].clone(), Value::Symbol("cl-structure-object".into())],
                env,
            )
        }
        "compiled-function-p" | "byte-code-function-p" => {
            need_args(name, args, 1)?;
            Ok(
                if record_type_name(interp, &args[0]) == Some("byte-code-function")
                    || matches!(
                        &args[0],
                        Value::Lambda(params, _, _)
                            if params == &["vals", "start", "end"]
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
                | Value::Cons(_, _)
                | Value::BuiltinFunc(_)
                | Value::Lambda(_, _, _)
                | Value::Buffer(_, _)
                | Value::Marker(_)
                | Value::Overlay(_)
                | Value::CharTable(_)
                | Value::Record(_)
                | Value::Finalizer(_)
                | Value::Unbound => false,
            };
            Ok(if is_user_ptr { Value::T } else { Value::Nil })
        }
        "closurep" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(args[0], Value::Lambda(_, _, _))
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
            Ok(if matches!(args[0], Value::Lambda(_, _, _)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "subrp" => {
            need_args(name, args, 1)?;
            // Native emaxx builtins that GNU defines in preloaded LISP
            // (mark-sexp, zap-to-char...) are NOT subrs in GNU; find-func
            // relies on subr-primitive-p being nil to consult symbol-file.
            Ok(match &args[0] {
                Value::BuiltinFunc(builtin)
                    if !super::misc_keymaps::builtin_is_gnu_preloaded_lisp(interp, builtin) =>
                {
                    Value::T
                }
                _ => Value::Nil,
            })
        }
        "special-form-p" => {
            need_args(name, args, 1)?;
            // GNU's fixed C set (enumerated from the oracle).  emaxx's
            // native-form list is far broader (native macros and commands)
            // and must not leak here: nadvice refuses to advise special
            // forms, and GNU's `when'/`call-interactively' are not ones.
            let gnu_special = |name: &str| {
                matches!(
                    name,
                    "and"
                        | "catch"
                        | "cond"
                        | "condition-case"
                        | "defconst"
                        | "defvar"
                        | "function"
                        | "if"
                        | "inline"
                        | "interactive"
                        | "let"
                        | "let*"
                        | "or"
                        | "prog1"
                        | "progn"
                        | "quote"
                        | "save-current-buffer"
                        | "save-excursion"
                        | "save-restriction"
                        | "setq"
                        | "unwind-protect"
                        | "while"
                )
            };
            Ok(match &args[0] {
                Value::BuiltinFunc(name) if gnu_special(name) => Value::T,
                Value::Symbol(name) if gnu_special(name) => Value::T,
                _ => Value::Nil,
            })
        }
        "oddp" => {
            need_args(name, args, 1)?;
            Ok(
                if (&integer_like_bigint(interp, &args[0])? & BigInt::from(1u8)).is_zero() {
                    Value::Nil
                } else {
                    Value::T
                },
            )
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
            Ok(Value::list(current_active_maps(interp, env, args.get(1))?))
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
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            if let Value::BuiltinFunc(name) = &value
                && generated_builtin_arities::generated_builtin_command_p(name)
            {
                return Ok(Value::T);
            }
            if autoload_command_p(&value) || interactive_form_items(&value).is_some() {
                return Ok(Value::T);
            }
            // OClosures may get their interactive form from the
            // `oclosure-interactive-form' generic (like GNU's commandp).
            if super::misc_keymaps::oclosure_type_of(&value).is_some()
                && interp.has_lisp_function("oclosure-interactive-form")
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
            let symbol = args[0].as_symbol()?;
            Ok(
                if interp.symbol_value_cell(symbol).is_ok()
                    || matches!(
                        symbol,
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
            need_arg_range(name, args, 1, 4)?;
            let command = args[0].as_symbol()?;
            let first_only = args.get(2).is_some_and(Value::is_truthy);
            let keymaps = where_is_internal_maps(interp, args.get(1), env)?;
            let matches = where_is_internal(interp, command, &keymaps, env)?;
            if first_only {
                Ok(matches.into_iter().next().unwrap_or(Value::Nil))
            } else {
                Ok(Value::list(matches))
            }
        }
        "default-boundp" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if interp.is_default_bound(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "special-variable-p" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            // GNU: the self-evaluating constants t/nil/keywords are declared
            // special (erc-button-setup's alist FORM check relies on
            // (special-variable-p t) being non-nil).
            Ok(
                if matches!(symbol, "t" | "nil")
                    || symbol.starts_with(':')
                    || interp.is_special_variable(symbol)
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
            Ok(Value::Symbol(symbol))
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
                    || is_always_buffer_local_builtin(&symbol)
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
            let symbol = args[0].as_symbol()?;
            Ok(
                if is_builtin(symbol)
                    || interp.lookup_function(symbol, env).is_ok()
                    || is_special_form_name(symbol)
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "facep" => {
            need_args(name, args, 1)?;
            let face = match &args[0] {
                Value::Symbol(symbol) => symbol.clone(),
                Value::String(_) | Value::StringObject(_) => string_text(&args[0])?,
                _ => return Ok(Value::Nil),
            };
            Ok(if face_exists(interp, &face) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "face-equal" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let left = match &args[0] {
                Value::Symbol(symbol) => symbol.clone(),
                Value::String(_) | Value::StringObject(_) => string_text(&args[0])?,
                _ => return Ok(Value::Nil),
            };
            let right = match &args[1] {
                Value::Symbol(symbol) => symbol.clone(),
                Value::String(_) | Value::StringObject(_) => string_text(&args[1])?,
                _ => return Ok(Value::Nil),
            };
            Ok(if left == right { Value::T } else { Value::Nil })
        }
        "face-differs-from-default-p" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = match &args[0] {
                Value::Symbol(symbol) => symbol.clone(),
                Value::String(_) | Value::StringObject(_) => string_text(&args[0])?,
                _ => return Ok(Value::Nil),
            };
            Ok(if face != "default" && face_exists(interp, &face) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "face-list" => {
            need_args(name, args, 0)?;
            let mut faces = interp
                .known_symbol_names()
                .into_iter()
                .filter(|symbol| face_exists(interp, symbol))
                .map(Value::Symbol)
                .collect::<Vec<_>>();
            if !faces
                .iter()
                .any(|face| face == &Value::Symbol("default".into()))
            {
                faces.push(Value::Symbol("default".into()));
            }
            faces.sort_by_key(|value| value.to_string());
            Ok(Value::list(faces))
        }
        "face-valid-attribute-values" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(face_valid_attribute_values(&args[0]))
        }
        "seq-some" => {
            need_args(name, args, 2)?;
            let predicate = args[0].clone();
            for element in args[1].to_vec()? {
                let result = call_function_value(interp, &predicate, &[element], env)?;
                if result.is_truthy() {
                    return Ok(result);
                }
            }
            Ok(Value::Nil)
        }
        "any" => {
            need_args(name, args, 2)?;
            let predicate = args[0].clone();
            let items = args[1].to_vec()?;
            for (index, element) in items.iter().enumerate() {
                let result =
                    call_function_value(interp, &predicate, std::slice::from_ref(element), env)?;
                if result.is_truthy() {
                    return Ok(Value::list(items[index..].to_vec()));
                }
            }
            Ok(Value::Nil)
        }
        "featurep" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            if !interp.has_feature(symbol) {
                return Ok(Value::Nil);
            }
            // GNU: with SUBFEATURE, check (memq SUBFEATURE (get FEATURE
            // 'subfeatures)) — `member', since subfeatures are lists like
            // (:family local).
            if let Some(subfeature) = args.get(1).filter(|value| value.is_truthy()) {
                let subfeatures = interp
                    .get_symbol_property(symbol, "subfeatures")
                    .unwrap_or(Value::Nil);
                let found =
                    subfeatures.to_vec().unwrap_or_default().iter().any(|item| {
                        crate::lisp::primitives::values_equal(interp, item, subfeature)
                    });
                return Ok(if found { Value::T } else { Value::Nil });
            }
            Ok(Value::T)
        }
        "zlib-available-p" => Ok(Value::T),
        // Built without GnuTLS, like a GNU build configured --without-gnutls.
        "gnutls-available-p" => Ok(Value::Nil),
        "libxml-available-p" => Ok(Value::T),
        "consp" => {
            need_args(name, args, 1)?;
            // Vector and bool-vector literals ride on conses internally but
            // are not conses to Lisp.
            Ok(
                if args[0].is_cons() && !is_vector_like_value(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "listp" => {
            need_args(name, args, 1)?;
            Ok(
                if (args[0].is_list() && !is_vector_like_value(interp, &args[0]))
                    || keymap_list_items(interp, &args[0])?.is_some()
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "cl-endp" => {
            need_args(name, args, 1)?;
            if is_vector_like_value(interp, &args[0]) {
                return Err(wrong_type_argument("listp", args[0].clone()));
            }
            match &args[0] {
                Value::Nil => Ok(Value::T),
                Value::Cons(_, _) => Ok(Value::Nil),
                other => Err(wrong_type_argument("listp", other.clone())),
            }
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
            Ok(if matches!(args[0], Value::Buffer(_, _)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "buffer-live-p" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Buffer(id, _) if interp.has_buffer_id(*id)) {
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
            let is_live = buffer_id.is_some_and(|buffer_id| {
                interp
                    .lookup_var("emaxx--active-minibuffer", env)
                    .and_then(|value| interp.resolve_buffer_id(&value).ok())
                    == Some(buffer_id)
            });
            let matches = is_minibuffer && (!args.get(1).is_some_and(Value::is_truthy) || is_live);
            Ok(if matches { Value::T } else { Value::Nil })
        }
        "minibuffer-depth" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("emaxx--minibuffer-depth", env)
                .unwrap_or(Value::Integer(0)))
        }
        "minibuffer-prompt" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("emaxx--minibuffer-prompt", env)
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
                .lookup_var("emaxx--active-minibuffer", env)
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
            if interp
                .lookup_var("emaxx--active-minibuffer", env)
                .is_none_or(|value| value.is_nil())
            {
                return Err(LispError::Signal("Not in a minibuffer".into()));
            }
            Err(LispError::SignalValue(Value::list([Value::symbol(
                "minibuffer-quit",
            )])))
        }

        "zerop" => {
            need_args(name, args, 1)?;
            let is_zero = match &args[0] {
                Value::Integer(value) => *value == 0,
                Value::BigInteger(value) => value.is_zero(),
                Value::Float(value) => *value == 0.0,
                Value::Marker(id) => {
                    interp
                        .marker_position(*id)
                        .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?
                        == 0
                }
                other => return Err(wrong_type_argument("number-or-marker-p", other.clone())),
            };
            Ok(if is_zero { Value::T } else { Value::Nil })
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
                    if n.sign() != Sign::Minus && n <= &BigInt::from(0x3F_FFFFu32) =>
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
                if matches!(args[0], Value::Record(_)) || record_literal_items(&args[0]).is_some() {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

/// GNU's DEFVAR_PER_BUFFER variables whose buffer_local_flags slot is -1:
/// they are buffer-local in every buffer, so `local-variable-p' is always t
/// (unsafep counts a setq of these as safe).
fn is_always_buffer_local_builtin(name: &str) -> bool {
    matches!(
        name,
        "buffer-file-name"
            | "default-directory"
            | "buffer-backed-up"
            | "buffer-saved-size"
            | "buffer-auto-save-file-name"
            | "buffer-read-only"
            | "major-mode"
            | "local-minor-modes"
            | "mode-name"
            | "buffer-undo-list"
            | "mark-active"
            | "point-before-scroll"
            | "buffer-file-truename"
            | "buffer-invisibility-spec"
            | "buffer-file-format"
            | "buffer-auto-save-file-format"
            | "buffer-display-count"
            | "buffer-display-time"
            | "enable-multibyte-characters"
    )
}

fn is_special_form_name(name: &str) -> bool {
    matches!(
        name,
        "quote"
            | "if"
            | "static-if"
            | "if-let"
            | "if-let*"
            | "when"
            | "static-when"
            | "when-let"
            | "when-let*"
            | "unless"
            | "static-unless"
            | "bound-and-true-p"
            | "cond"
            | "pcase"
            | "pcase-defmacro"
            | "pcase-exhaustive"
            | "and-let*"
            | "and"
            | "or"
            | "not"
            | "progn"
            | "atomic-change-group"
            | "prog1"
            | "prog2"
            | "let"
            | "dlet"
            | "let*"
            | "cl-progv"
            | "pcase-let"
            | "pcase-let*"
            | "let-alist"
            | "setq"
            | "setq-default"
            | "setq-local"
            | "setopt"
            | "setf"
            | "incf"
            | "cl-incf"
            | "decf"
            | "cl-decf"
            | "cl-callf"
            | "defvar"
            | "defconst"
            | "defcustom"
            | "defvar-local"
            | "defgroup"
            | "defface"
            | "defvar-keymap"
            | "define-short-documentation-group"
            | "insert"
            | "insert-and-inherit"
            | "insert-char"
            | "insert-before-markers"
            | "insert-before-markers-and-inherit"
            | "define-minor-mode"
            | "define-globalized-minor-mode"
            | "define-derived-mode"
            | "defclass"
            | "defun"
            | "defsubst"
            | "cl-defun"
            | "cl-defmacro"
            | "cl-generic-define-generalizer"
            | "cl-defgeneric"
            | "cl-defmethod"
            | "cl-generic-define-context-rewriter"
            | "oclosure-define"
            | "oclosure-lambda"
            | "define-inline"
            | "defmacro"
            | "with-memoization"
            | "easy-menu-define"
            | "cl-defstruct"
            | "backquote"
            | "comma"
            | "lambda"
            | "interactive"
            | "function"
            | "function-quote"
            | "while"
            | "dolist"
            | "dolist-with-progress-reporter"
            | "pcase-dolist"
            | "dotimes"
            | "cl-loop"
            | "unwind-protect"
            | "ignore-error"
            | "ignore-errors"
            | "condition-case"
            | "condition-case-unless-debug"
            | "handler-bind"
            | "cl-assert"
            | "with-temp-buffer"
            | "ert-with-test-buffer"
            | "ert-with-temp-directory"
            | "ert-with-message-capture"
            | "with-environment-variables"
            | "with-output-to-string"
            | "with-mutex"
            | "with-temp-file"
            | "ert-with-temp-file"
            | "with-current-buffer"
            | "with-current-buffer-window"
            | "with-restriction"
            | "without-restriction"
            | "add-function"
            | "with-selected-window"
            | "with-syntax-table"
            | "save-match-data"
            | "save-excursion"
            | "save-window-excursion"
            | "save-current-buffer"
            | "save-restriction"
            | "with-suppressed-warnings"
            | "with-demoted-errors"
            | "with-coding-priority"
            | "with-silent-modifications"
            | "combine-change-calls"
            | "cl-destructuring-bind"
            | "cl-letf"
            | "cl-flet"
            | "cl-labels"
            | "cl-macrolet"
            | "cl-symbol-macrolet"
            | "push"
            | "cl-pushnew"
            | "pop"
            | "catch"
            | "add-to-list"
            | "ert-deftest"
            | "should"
            | "should-not"
            | "should-error"
            | "skip-unless"
            | "ert--skip-unless"
            | "skip-when"
            | "ert--skip-when"
            | "rx"
            | "rx-define"
            | "rx-let"
            | "with-eval-after-load"
            | "with-no-warnings"
            | "declare"
            | "declare-function"
            | "cl-declaim"
            | "declaim"
            | "cl-deftype"
            | "def-edebug-elem-spec"
            | "def-edebug-spec"
            | "eval-and-compile"
            | "eval-when-compile"
            | "while-no-input"
            | "ert-info"
            | "minibuffer-with-setup-hook"
    )
}

fn face_valid_attribute_values(attribute: &Value) -> Value {
    let Ok(attribute) = attribute.as_symbol() else {
        return Value::Nil;
    };
    match attribute {
        ":height" => Value::Symbol("integerp".into()),
        ":inherit" => Value::cons(
            Value::cons(Value::String("none".into()), Value::Nil),
            Value::Nil,
        ),
        ":family" => Value::list([Value::cons(
            Value::String("default".into()),
            Value::String("default".into()),
        )]),
        ":foundry" => Value::list([Value::Nil]),
        ":width" | ":weight" | ":slant" | ":inverse-video" | ":extend" | ":underline"
        | ":overline" | ":strike-through" | ":box" | ":foreground" | ":background" => {
            Value::list([
                Value::cons(
                    Value::String("unspecified".into()),
                    Value::Symbol("unspecified".into()),
                ),
                Value::cons(Value::String("nil".into()), Value::Nil),
                Value::cons(Value::String("t".into()), Value::T),
            ])
        }
        ":stipple" => Value::Nil,
        _ => Value::Nil,
    }
}
