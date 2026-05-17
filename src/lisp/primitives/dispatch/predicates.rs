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
            | "numberp"
            | "number-or-marker-p"
            | "char-or-string-p"
            | "arrayp"
            | "sequencep"
            | "vectorp"
            | "integer-or-marker-p"
            | "vector-or-char-table-p"
            | "bool-vector-p"
            | "bool-vector-subsetp"
            | "floatp"
            | "stringp"
            | "symbolp"
            | "keywordp"
            | "functionp"
            | "compiled-function-p"
            | "byte-code-function-p"
            | "closurep"
            | "interpreted-function-p"
            | "subrp"
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
            | "seq-some"
            | "any"
            | "featurep"
            | "zlib-available-p"
            | "libxml-available-p"
            | "consp"
            | "listp"
            | "proper-list-p"
            | "bufferp"
            | "buffer-live-p"
            | "processp"
            | "threadp"
            | "mutexp"
            | "condition-variable-p"
            | "minibufferp"
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
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            Ok(
                if matches!(value, Value::BuiltinFunc(_) | Value::Lambda(_, _, _))
                    || is_lambda_expression(&value)
                    || record_type_name(interp, &value) == Some("byte-code-function")
                {
                    Value::T
                } else {
                    Value::Nil
                },
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
            Ok(if matches!(args[0], Value::BuiltinFunc(_)) {
                Value::T
            } else {
                Value::Nil
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
            let value = resolve_callable(interp, &args[0], env).unwrap_or_else(|_| args[0].clone());
            Ok(
                if autoload_command_p(&value) || interactive_form_items(&value).is_some() {
                    Value::T
                } else {
                    Value::Nil
                },
            )
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
            Ok(if interp.is_special_variable(symbol) {
                Value::T
            } else {
                Value::Nil
            })
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
            Ok(if interp.buffer_local_value(buffer_id, &symbol).is_some() {
                Value::T
            } else {
                Value::Nil
            })
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
            Ok(if interp.has_feature(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "zlib-available-p" => Ok(Value::T),
        "libxml-available-p" => Ok(Value::T),
        "consp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_cons() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "listp" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].is_list() || keymap_list_items(interp, &args[0])?.is_some() {
                    Value::T
                } else {
                    Value::Nil
                },
            )
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
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let buffer_id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            let is_minibuffer = interp
                .get_buffer_by_id(buffer_id)
                .map(|buffer| buffer.name.starts_with(" *Minibuf"))
                .unwrap_or(false);
            Ok(if is_minibuffer { Value::T } else { Value::Nil })
        }

        "zerop" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Integer(0) => Value::T,
                Value::BigInteger(n) if n.is_zero() => Value::T,
                Value::Float(f) if *f == 0.0 => Value::T,
                _ => Value::Nil,
            })
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
                if args[0].is_cons() || keymap_list_items(interp, &args[0])?.is_some() {
                    Value::Nil
                } else {
                    Value::T
                },
            )
        }

        "nlistp" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].is_list() || keymap_list_items(interp, &args[0])?.is_some() {
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
