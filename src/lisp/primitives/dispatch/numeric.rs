use super::*;
use crate::lisp::types::Kind;

fn has_float(args: &[Value]) -> bool {
    args.iter()
        .any(|value| matches!(value.kind(), Kind::Float(_)))
}

fn has_big_integer(args: &[Value]) -> bool {
    args.iter()
        .any(|value| matches!(value.kind(), Kind::BigInteger(_)))
}

/// data.c's arithmetic operations that `arith_driver' runs over floats.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ArithOp {
    Add,
    Sub,
    Mult,
    Div,
}

/// floatop_arith_driver's `accum OP= next' as the x86-64 build computes
/// it: a NaN operand gives its NaN, and when both are NaNs the later
/// operand's wins for `+' and `*' (the compiled additions and products
/// keep NEXT in the destination register) and the accumulator's for `-'
/// and `/'.  The hardware rule for one NaN, made explicit here rather
/// than left to the code generator.
#[inline]
fn float_operation(code: ArithOp, accum: f64, next: f64) -> f64 {
    let commutative = matches!(code, ArithOp::Add | ArithOp::Mult);
    if commutative && next.is_nan() {
        return next;
    }
    if accum.is_nan() {
        return accum;
    }
    if next.is_nan() {
        return next;
    }
    match code {
        ArithOp::Add => accum + next,
        ArithOp::Sub => accum - next,
        ArithOp::Mult => accum * next,
        ArithOp::Div => accum / next,
    }
}

/// data.c:floatop_arith_driver: the arguments from ARGNUM on folded into
/// ACCUM in double precision, NEXT the value of ARGS[ARGNUM] (taken as the
/// accumulator when ARGNUM is 0), each later one coerced as it is reached.
fn floatop_arith_driver(
    interp: &Interpreter,
    code: ArithOp,
    args: &[Value],
    mut argnum: usize,
    accum: f64,
    next: f64,
) -> Result<Value, LispError> {
    let mut accum = if argnum == 0 {
        next
    } else {
        float_operation(code, accum, next)
    };
    loop {
        argnum += 1;
        if argnum == args.len() {
            return Ok(Value::float(accum));
        }
        let next = numeric_to_f64(interp, &check_number_coerce_marker(interp, &args[argnum])?)?;
        accum = float_operation(code, accum, next);
    }
}

/// data.c:bignum_arith_driver: the integers from ARGNUM on folded exactly
/// into an accumulator holding IACCUM (VAL itself when ARGNUM is 0), VAL
/// the integer at ARGNUM; at a float the accumulator, rounded to the
/// nearest double, goes on in floatop_arith_driver.
fn bignum_arith_driver(
    interp: &Interpreter,
    code: ArithOp,
    args: &[Value],
    mut argnum: usize,
    iaccum: i64,
    val: Value,
) -> Result<Value, LispError> {
    let operate = |accum: BigInt, next: BigInt| -> Result<BigInt, LispError> {
        Ok(match code {
            ArithOp::Add => accum + next,
            ArithOp::Sub => accum - next,
            // emacs_mpz_mul refuses a product past GMP's limb limit.
            ArithOp::Mult => emacs_mpz_mul(&accum, &next)?,
            ArithOp::Div => {
                if next.is_zero() {
                    return Err(arith_error());
                }
                // mpz_tdiv_q: the quotient truncated toward zero.
                accum / next
            }
        })
    };
    let mut accum = if argnum == 0 {
        integer_like_bigint(interp, &val)?
    } else {
        operate(BigInt::from(iaccum), integer_like_bigint(interp, &val)?)?
    };
    loop {
        argnum += 1;
        if argnum == args.len() {
            return make_integer(interp, accum);
        }
        let val = check_number_coerce_marker(interp, &args[argnum])?;
        if let Kind::Float(next) = val.kind() {
            let rounded = accum.to_f64().unwrap_or(f64::NAN);
            return floatop_arith_driver(interp, code, args, argnum, rounded, next.get());
        }
        accum = operate(accum, integer_like_bigint(interp, &val)?)?;
    }
}

/// data.c:arith_driver: ARGS (at least two) folded with CODE from VAL, the
/// first coerced: in a machine integer while each value and result fits,
/// the integers past that in a bignum, and from the first float on in
/// double precision, the accumulator converted once, where it stands, as
/// GNU converts it.
fn arith_driver(
    interp: &Interpreter,
    code: ArithOp,
    args: &[Value],
    val: Value,
) -> Result<Value, LispError> {
    let mut argnum = 0;
    let mut accum = 0_i64;
    let mut val = val;
    if let Some(first) = val.fixnum() {
        accum = first;
        loop {
            argnum += 1;
            if argnum == args.len() {
                return Ok(normalize_integer_value(accum));
            }
            val = check_number_coerce_marker(interp, &args[argnum])?;
            // integer_to_intmax: a fixnum, or a bignum within intmax_t.
            let next = match val.kind() {
                Kind::Integer(next) => next,
                Kind::BigInteger(next) => match BigInt::from(next).to_i64() {
                    Some(next) => next,
                    None => break,
                },
                _ => break,
            };
            let result = match code {
                ArithOp::Add => accum.checked_add(next),
                ArithOp::Sub => accum.checked_sub(next),
                ArithOp::Mult => accum.checked_mul(next),
                ArithOp::Div => {
                    if next == 0 {
                        return Err(arith_error());
                    }
                    // No overflow: ACCUM is never INTMAX_MIN.
                    accum /= next;
                    continue;
                }
            };
            match result {
                Some(result) => accum = result,
                None => break,
            }
        }
    }
    match val.kind() {
        Kind::Float(next) => {
            floatop_arith_driver(interp, code, args, argnum, accum as f64, next.get())
        }
        _ => bignum_arith_driver(interp, code, args, argnum, accum, val),
    }
}

/// data.c:Frem/Fmod coerce operands in order before calculating a remainder.
/// `%` accepts integers/markers; only `mod` also accepts floating point.
fn remainder_argument(
    interp: &Interpreter,
    value: Value,
    modulo: bool,
) -> Result<Value, LispError> {
    match value.kind() {
        Kind::Integer(_) | Kind::BigInteger(_) => Ok(value),
        Kind::Float(_) if modulo => Ok(value),
        Kind::Marker(marker) => interp
            .marker_position(marker)
            .map(|position| Value::Integer(position as i64))
            .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into())),
        _ => Err(LispError::WrongTypeArgument(
            if modulo {
                "number-or-marker-p"
            } else {
                "integer-or-marker-p"
            }
            .into(),
            value,
        )),
    }
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Arithmetic ──
            "+" => direct_plus(interp, args, env),
            "-" => direct_minus(interp, args, env),
            "*" => direct_times(interp, args, env),
            "/" => {
                // data.c:Fquo.
                let Some(first) = args.first() else {
                    return Err(LispError::WrongNumberOfArgs("/".into(), args.len()));
                };
                let first = check_number_coerce_marker(interp, first)?;
                if args.len() == 1 {
                    return match first.kind() {
                        Kind::Integer(0) => Err(arith_error()),
                        Kind::Integer(value) => Ok(Value::Integer(1 / value)),
                        Kind::Float(value) => Ok(Value::float(1.0 / value.get())),
                        // Dividing 1 by any bignum yields 0.
                        _ => Ok(Value::Integer(0)),
                    };
                }
                // All of it in floating point if a divisor past the first is
                // a float.
                if args[2..]
                    .iter()
                    .any(|value| matches!(value.kind(), Kind::Float(_)))
                {
                    let first = numeric_to_f64(interp, &first)?;
                    return floatop_arith_driver(interp, ArithOp::Div, args, 0, 0.0, first);
                }
                arith_driver(interp, ArithOp::Div, args, first)
            }
            "%" | "mod" => {
                need_args(name, args, 2)?;
                let numbers = [
                    remainder_argument(interp, args[0], name == "mod")?,
                    remainder_argument(interp, args[1], name == "mod")?,
                ];
                let args = &numbers;
                if has_float(args) {
                    let a = numeric_to_f64(interp, &args[0])?;
                    let b = numeric_to_f64(interp, &args[1])?;
                    let mut remainder = a % b;
                    if name == "mod"
                        && remainder != 0.0
                        && !remainder.is_nan()
                        && (remainder.is_sign_negative() != b.is_sign_negative())
                    {
                        remainder += b;
                    }
                    return Ok(Value::float(remainder));
                }
                if has_big_integer(args) {
                    let a = integer_like_bigint(interp, &args[0])?;
                    let b = integer_like_bigint(interp, &args[1])?;
                    if b.is_zero() {
                        return Err(arith_error());
                    }
                    let mut r = &a % &b;
                    if name == "mod" && !r.is_zero() && (r.sign() != b.sign()) {
                        r += &b;
                    }
                    return make_integer(interp, r);
                }
                let a = integer_like_i64(interp, &args[0])?;
                let b = integer_like_i64(interp, &args[1])?;
                if b == 0 {
                    return Err(arith_error());
                }
                Ok(normalize_integer_value(if name == "mod" {
                    let mut remainder = a % b;
                    if remainder != 0 && (remainder.is_negative() != b.is_negative()) {
                        remainder += b;
                    }
                    remainder
                } else {
                    a % b
                }))
            }
            "1+" => direct_add1(interp, args, env),
            "1-" => direct_sub1(interp, args, env),
            "max" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs("max".into(), 0));
                }
                extremum_numeric_value(interp, args, true)
            }
            "min" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs("min".into(), 0));
                }
                extremum_numeric_value(interp, args, false)
            }
            "abs" => {
                need_args(name, args, 1)?;
                // floatfns.c:Fabs: CHECK_NUMBER.
                check_number(&args[0])?;
                if let Kind::Float(value) = args[0].kind() {
                    Ok(Value::float(value.abs()))
                } else if matches!(args[0].kind(), Kind::BigInteger(_)) {
                    make_integer(interp, integer_like_bigint(interp, &args[0])?.abs())
                } else {
                    let value = integer_like_i64(interp, &args[0])?;
                    match value.checked_abs() {
                        Some(abs) => Ok(normalize_integer_value(abs)),
                        None => make_integer(interp, BigInt::from(value).abs()),
                    }
                }
            }
            "sin" => {
                need_args(name, args, 1)?;
                Ok(Value::float(extract_float(interp, &args[0])?.sin()))
            }
            "cos" => {
                need_args(name, args, 1)?;
                Ok(Value::float(extract_float(interp, &args[0])?.cos()))
            }
            "tan" => {
                need_args(name, args, 1)?;
                Ok(Value::float(extract_float(interp, &args[0])?.tan()))
            }
            "asin" => {
                need_args(name, args, 1)?;
                Ok(Value::float(extract_float(interp, &args[0])?.asin()))
            }
            "acos" => {
                need_args(name, args, 1)?;
                Ok(Value::float(extract_float(interp, &args[0])?.acos()))
            }
            "atan" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let y = extract_float(interp, &args[0])?;
                Ok(Value::float(
                    if let Some(x) = args.get(1).filter(|x| !x.is_nil()) {
                        y.atan2(extract_float(interp, x)?)
                    } else {
                        y.atan()
                    },
                ))
            }
            "copysign" => {
                need_args(name, args, 2)?;
                // floatfns.c:Fcopysign: CHECK_FLOAT of both.
                let magnitude = check_float(&args[0])?;
                Ok(Value::float(magnitude.copysign(check_float(&args[1])?)))
            }
            "isnan" => {
                need_args(name, args, 1)?;
                // floatfns.c:Fisnan: CHECK_FLOAT.
                Ok(if check_float(&args[0])?.is_nan() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "exp" => {
                need_args(name, args, 1)?;
                Ok(Value::float(extract_float(interp, &args[0])?.exp()))
            }
            "expt" => {
                need_args(name, args, 2)?;
                Ok(expt_value(interp, &args[0], &args[1])?)
            }
            "log" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let value = extract_float(interp, &args[0])?;
                // floatfns.c:Flog treats a nil BASE as the natural log.
                // Native fixed-arity calls include this optional nil slot.
                let result = if let Some(base) = args.get(1).filter(|base| !base.is_nil()) {
                    let base = extract_float(interp, base)?;
                    if base == 10.0 {
                        value.log10()
                    } else if base == 2.0 {
                        value.log2()
                    } else {
                        value.log(base)
                    }
                } else {
                    value.ln()
                };
                Ok(Value::float(result))
            }
            "sqrt" => {
                need_args(name, args, 1)?;
                Ok(Value::float(extract_float(interp, &args[0])?.sqrt()))
            }
            "float" => {
                need_args(name, args, 1)?;
                // floatfns.c:Ffloat: "If ARG is a float, give 'em the same
                // float back" -- the same object, with no allocation.
                if matches!(args[0].kind(), Kind::Float(_)) {
                    return Ok(args[0]);
                }
                Ok(Value::float(extract_float(interp, &args[0])?))
            }

            "frexp" => {
                need_args(name, args, 1)?;
                let (sig, exp) = frexp_parts(extract_float(interp, &args[0])?);
                Ok(Value::cons(Value::float(sig), Value::Integer(exp)))
            }
            "ldexp" => {
                need_args(name, args, 2)?;
                // floatfns.c:Fldexp: CHECK_FIXNUM (exponent), then the
                // significand by extract_float.
                let exponent = args[1].as_fixnum()?;
                let significand = extract_float(interp, &args[0])?;
                Ok(Value::float(ldexp_value(significand, exponent)))
            }
            "logb" => {
                need_args(name, args, 1)?;
                Ok(logb_value(interp, &args[0])?)
            }
            "ceiling" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Ceiling,
                args,
                false,
            )?),
            "floor" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Floor,
                args,
                false,
            )?),
            "round" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Round,
                args,
                false,
            )?),
            "truncate" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Truncate,
                args,
                false,
            )?),
            "fceiling" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Ceiling,
                args,
                true,
            )?),
            "ffloor" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Floor,
                args,
                true,
            )?),
            "fround" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Round,
                args,
                true,
            )?),
            "ftruncate" => Ok(integer_rounding_value(
                interp,
                RoundingKind::Truncate,
                args,
                true,
            )?),
            "ash" => {
                need_args(name, args, 2)?;
                if let (Some(value), Some(shift)) = (args[0].fixnum(), args[1].fixnum())
                    && let Some(shifted) = fixnum_ash(value, shift)
                {
                    return Ok(normalize_integer_value(shifted));
                }
                let value = integer_operand(&args[0])?;
                integer_operand(&args[1])?;
                // data.c Fash with a bignum COUNT: 0 stays 0, a negative
                // count shifts everything else to -1 or 0 by its sign, and a
                // positive one overflows.  (A fixnum count follows below;
                // `most-positive-fixnum' is 2**61-1, so `(* 2
                // most-positive-fixnum)' is a bignum GNU never converts.)
                if let Kind::BigInteger(count) = args[1].kind() {
                    if value.sign() == Sign::NoSign {
                        return Ok(Value::Integer(0));
                    }
                    if count.sign() == Sign::Minus {
                        return Ok(Value::Integer(if value.sign() == Sign::Minus {
                            -1
                        } else {
                            0
                        }));
                    }
                    return Err(LispError::SignalValue(Value::list([Value::symbol(
                        "overflow-error",
                    )])));
                }
                let shift = args[1].fixnum().expect("a fixnum count, checked above");
                // mpz_fdiv_q_2exp for a negative count, emacs_mpz_mul_2exp
                // (which refuses a result past GMP's limb limit) otherwise.
                let shifted = if shift >= 0 {
                    emacs_mpz_mul_2exp(&value, shift)?
                } else {
                    value >> shift.unsigned_abs()
                };
                make_integer(interp, shifted)
            }
            "logcount" => {
                need_args(name, args, 1)?;
                let mut value = integer_operand(&args[0])?;
                if value.sign() == Sign::Minus {
                    value = !value;
                }
                let count = value
                    .to_str_radix(2)
                    .chars()
                    .filter(|bit| *bit == '1')
                    .count() as i64;
                Ok(Value::Integer(count))
            }
            "logand" => {
                if let Some(result) = fold_fixnums(args, -1, |a, b| a & b) {
                    return Ok(normalize_integer_value(result));
                }
                let mut result = BigInt::from(-1);
                for (index, arg) in args.iter().enumerate() {
                    result &= logical_operand(interp, arg, index == 0)?;
                }
                make_integer(interp, result)
            }
            "logior" => {
                if let Some(result) = fold_fixnums(args, 0, |a, b| a | b) {
                    return Ok(normalize_integer_value(result));
                }
                let mut result = BigInt::from(0);
                for (index, arg) in args.iter().enumerate() {
                    result |= logical_operand(interp, arg, index == 0)?;
                }
                make_integer(interp, result)
            }
            "logxor" => {
                if let Some(result) = fold_fixnums(args, 0, |a, b| a ^ b) {
                    return Ok(normalize_integer_value(result));
                }
                let mut result = BigInt::from(0);
                for (index, arg) in args.iter().enumerate() {
                    result ^= logical_operand(interp, arg, index == 0)?;
                }
                make_integer(interp, result)
            }
            "lognot" => {
                need_args(name, args, 1)?;
                if let Some(value) = args[0].fixnum() {
                    return Ok(normalize_integer_value(!value));
                }
                make_integer(interp, !integer_operand(&args[0])?)
            }
            "prefix-numeric-value" => {
                need_args(name, args, 1)?;
                prefix_numeric_value(&args[0])
            }

            // ── Comparison ──
            "=" => direct_num_eq(interp, args, env),
            "<" => direct_lt(interp, args, env),
            ">" => direct_gt(interp, args, env),
            "<=" => direct_le(interp, args, env),
            ">=" => direct_ge(interp, args, env),
            "/=" => {
                need_args(name, args, 2)?;
                for index in 0..args.len() {
                    for other in index + 1..args.len() {
                        if numeric_eq(interp, &args[index], &args[other])? {
                            return Ok(Value::Nil);
                        }
                    }
                }
                Ok(Value::T)
            }
            "value<" => {
                need_arg_range(name, args, 2, 2)?;
                Ok(if value_less(interp, &args[0], &args[1], env)? {
                    Value::T
                } else {
                    Value::Nil
                })
            }

            // ── Equality ──
            "eq" => direct_eq(interp, args, env),
            "eql" => {
                need_args(name, args, 2)?;
                Ok(if values_eql_in_env(interp, &args[0], &args[1], env) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "equal" => direct_equal(interp, args, env),
            "equal-including-properties" => {
                need_args(name, args, 2)?;
                Ok(
                    if values_equal_including_properties(interp, &args[0], &args[1])? {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "sxhash-equal" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value_in_env(
                    interp,
                    &args[0],
                    HashMode::Equal,
                    env,
                )))
            }
            "sxhash-eq" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value_in_env(
                    interp,
                    &args[0],
                    HashMode::Eq,
                    env,
                )))
            }
            "sxhash-eql" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value_in_env(
                    interp,
                    &args[0],
                    HashMode::Eql,
                    env,
                )))
            }
            "sxhash-equal-including-properties" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value_in_env(
                    interp,
                    &args[0],
                    HashMode::EqualIncludingProperties,
                    env,
                )))
            }
            "string-equal" => {
                need_args(name, args, 2)?;
                let a = string_comparison_object(interp, &args[0], env)?;
                let b = string_comparison_object(interp, &args[1], env)?;
                Ok(if a == b { Value::T } else { Value::Nil })
            }
            "string-lessp" => {
                need_args(name, args, 2)?;
                let order = crate::lisp::primitives::string_order(interp, &args[0], &args[1], env)?;
                let matches = if name == "string>" {
                    order == Ordering::Greater
                } else {
                    order == Ordering::Less
                };
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "string-version-lessp" => {
                need_args(name, args, 2)?;
                let a = string_comparison_object(interp, &args[0], env)?;
                let b = string_comparison_object(interp, &args[1], env)?;
                Ok(
                    if string_version_compare(a.borrow().bytes(), b.borrow().bytes())
                        == Ordering::Less
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "compare-strings" => {
                need_arg_range(name, args, 6, 7)?;
                compare_strings_value(interp, args, env)
            }
            "string-distance" => {
                need_arg_range(name, args, 2, 3)?;
                string_distance_value(
                    &args[0],
                    &args[1],
                    args.get(2).is_some_and(Value::is_truthy),
                )
            }
            "string-collate-equalp" => {
                need_arg_range(name, args, 2, 4)?;
                // GNU/Linux defines __STDC_ISO_10646__, so fns.c collates
                // through sysdep.c str_collate; Darwin does not, and GNU
                // itself falls back to the lexicographic comparison there.
                #[cfg(target_os = "linux")]
                {
                    let ordering = crate::lisp::primitives::strings::str_collate(
                        &args[0],
                        &args[1],
                        args.get(2),
                        args.get(3).is_some_and(Value::is_truthy),
                    )?;
                    Ok(if ordering == Ordering::Equal {
                        Value::T
                    } else {
                        Value::Nil
                    })
                }
                #[cfg(not(target_os = "linux"))]
                {
                    validate_collation_locale(args.get(2))?;
                    Ok(
                        if string_compare_ordering(
                            &args[0],
                            &args[1],
                            args.get(3).is_some_and(Value::is_truthy),
                        )? == Ordering::Equal
                        {
                            Value::T
                        } else {
                            Value::Nil
                        },
                    )
                }
            }
            "string-collate-lessp" => {
                need_arg_range(name, args, 2, 4)?;
                #[cfg(target_os = "linux")]
                {
                    let ordering = crate::lisp::primitives::strings::str_collate(
                        &args[0],
                        &args[1],
                        args.get(2),
                        args.get(3).is_some_and(Value::is_truthy),
                    )?;
                    Ok(if ordering == Ordering::Less {
                        Value::T
                    } else {
                        Value::Nil
                    })
                }
                #[cfg(not(target_os = "linux"))]
                {
                    validate_collation_locale(args.get(2))?;
                    Ok(
                        if string_compare_ordering(
                            &args[0],
                            &args[1],
                            args.get(3).is_some_and(Value::is_truthy),
                        )? == Ordering::Less
                        {
                            Value::T
                        } else {
                            Value::Nil
                        },
                    )
                }
            }
            "string-search" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                string_search(args[0], args[1], args.get(2).copied().unwrap_or(Value::Nil))
            }
        }
    }
);

/// fns.c:Fstring_search: memmem over the strings' bytes from START-POS's
/// byte (string_char_to_byte), the needle first converted to the
/// haystack's representation when only its ASCII would match as it is.
fn string_search(needle: Value, haystack: Value, start_pos: Value) -> Result<Value, LispError> {
    let Kind::StringObject(needle_state) = needle.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), needle));
    };
    let Kind::StringObject(haystack_state) = haystack.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), haystack));
    };
    let needle_text = needle_state.borrow();
    let hay = haystack_state.borrow();
    let mut start = 0;
    let mut start_byte = 0;
    if !start_pos.is_nil() {
        let Kind::Integer(index) = start_pos.kind() else {
            return Err(LispError::WrongTypeArgument("fixnump".into(), start_pos));
        };
        if index < 0 || index as usize > hay.len() {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                start_pos,
            ])));
        }
        start = index as usize;
        start_byte = hay.char_to_byte(start);
    }
    if needle_text.len() > hay.len() - start {
        return Ok(Value::Nil);
    }
    let haystart = &hay.bytes()[start_byte..];
    let needle_bytes = needle_text.bytes();
    let needle_ascii = || needle_bytes.is_ascii();
    let found = if if hay.is_multibyte() {
        needle_text.is_multibyte() || hay.len() == hay.bytes().len() || needle_ascii()
    } else {
        !needle_text.is_multibyte() || needle_text.len() == needle_bytes.len()
    } {
        if hay.is_multibyte()
            && needle_text.is_multibyte()
            && hay.len() == hay.bytes().len()
            && needle_text.len() != needle_bytes.len()
        {
            // A multibyte non-ASCII needle in a multibyte ASCII haystack.
            return Ok(Value::Nil);
        }
        memchr::memmem::find(haystart, needle_bytes)
    } else if hay.is_multibyte() {
        // A unibyte non-ASCII needle: string_to_multibyte makes each byte
        // above 0x7F its eight-bit character.
        let mut multi = Vec::with_capacity(needle_bytes.len() * 2);
        for &byte in needle_bytes {
            if byte < 0x80 {
                multi.push(byte);
            } else {
                multi.extend_from_slice(&[0xC0 | ((byte >> 6) & 1), 0x80 | (byte & 0x3F)]);
            }
        }
        memchr::memmem::find(haystart, &multi)
    } else {
        // A multibyte non-ASCII needle in a unibyte haystack can match only
        // when it is raw bytes (and ASCII): Fstring_to_unibyte's bytes.
        let mut uni = Vec::with_capacity(needle_bytes.len());
        let mut i = 0;
        while i < needle_bytes.len() {
            let c = needle_bytes[i];
            if c & 0xFE == 0xC0 {
                // CHAR_BYTE8_HEAD_P: a raw byte's two-byte form.
                uni.push(((c & 1) << 6) | (needle_bytes[i + 1] & 0x3F) | 0x80);
                i += 2;
            } else if c < 0x80 {
                uni.push(c);
                i += 1;
            } else {
                return Ok(Value::Nil);
            }
        }
        memchr::memmem::find(haystart, &uni)
    };
    Ok(found.map_or(Value::Nil, |offset| {
        Value::Integer(hay.byte_to_char(start_byte + offset) as i64)
    }))
}

/// The `equal' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_equal(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "equal";
    need_args(name, args, 2)?;
    let equal = match symbol_with_pos_equal_in_env(interp, &args[0], &args[1], env) {
        Some(equal) => equal,
        None => super::super::values::values_equal(interp, &args[0], &args[1])?,
    };
    Ok(if equal { Value::T } else { Value::Nil })
}

/// Both arguments of a two-argument call, when both are fixnums: data.c's
/// FIXNUMP fast paths in arith_driver and arithcompare.
#[inline(always)]
fn two_fixnums(args: &[Value]) -> Option<(i64, i64)> {
    match args {
        [left, right] => Some((left.fixnum()?, right.fixnum()?)),
        _ => None,
    }
}

/// The argument of a one-argument call, when it is a fixnum.
#[inline(always)]
fn one_fixnum(args: &[Value]) -> Option<i64> {
    match args {
        [value] => value.fixnum(),
        _ => None,
    }
}

/// The subr behind `+', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_plus(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args) {
        return Ok(Value::Integer(x + y));
    }
    let Some(first) = args.first() else {
        return Ok(Value::Integer(0));
    };
    let first = check_number_coerce_marker(interp, first)?;
    if args.len() == 1 {
        return Ok(first);
    }
    arith_driver(interp, ArithOp::Add, args, first)
}

/// The subr behind `-', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_minus(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args) {
        return Ok(Value::Integer(x - y));
    }
    let Some(first) = args.first() else {
        return Ok(Value::Integer(0));
    };
    let first = check_number_coerce_marker(interp, first)?;
    if args.len() == 1 {
        return match first.kind() {
            Kind::Integer(value) => Ok(normalize_integer_value(-value)),
            Kind::Float(value) => Ok(Value::float(-value.get())),
            _ => make_integer(interp, -integer_like_bigint(interp, &first)?),
        };
    }
    arith_driver(interp, ArithOp::Sub, args, first)
}

/// The subr behind `*', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_times(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args)
        && let Some(product) = x.checked_mul(y)
    {
        return Ok(normalize_integer_value(product));
    }
    let Some(first) = args.first() else {
        return Ok(Value::Integer(1));
    };
    let first = check_number_coerce_marker(interp, first)?;
    if args.len() == 1 {
        return Ok(first);
    }
    arith_driver(interp, ArithOp::Mult, args, first)
}

/// The subr behind `=', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_num_eq(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args) {
        return Ok(if x == y { Value::T } else { Value::Nil });
    }
    let name = "=";
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs(name.into(), 0));
    }
    for pair in args.windows(2) {
        if !numeric_eq(interp, &pair[0], &pair[1])? {
            return Ok(Value::Nil);
        }
    }
    Ok(Value::T)
}

/// The subr behind `<', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_lt(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args) {
        return Ok(if x < y { Value::T } else { Value::Nil });
    }
    let name = "<";
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs(name.into(), 0));
    }
    for pair in args.windows(2) {
        if !numeric_lt(interp, &pair[0], &pair[1])? {
            return Ok(Value::Nil);
        }
    }
    Ok(Value::T)
}

/// The subr behind `>', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_gt(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args) {
        return Ok(if x > y { Value::T } else { Value::Nil });
    }
    let name = ">";
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs(name.into(), 0));
    }
    for pair in args.windows(2) {
        if !numeric_gt(interp, &pair[0], &pair[1])? {
            return Ok(Value::Nil);
        }
    }
    Ok(Value::T)
}

/// The subr behind `<=', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_le(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args) {
        return Ok(if x <= y { Value::T } else { Value::Nil });
    }
    let name = "<=";
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs(name.into(), 0));
    }
    for pair in args.windows(2) {
        if !numeric_lte(interp, &pair[0], &pair[1])? {
            return Ok(Value::Nil);
        }
    }
    Ok(Value::T)
}

/// The subr behind `>=', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_ge(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some((x, y)) = two_fixnums(args) {
        return Ok(if x >= y { Value::T } else { Value::Nil });
    }
    let name = ">=";
    if args.is_empty() {
        return Err(LispError::WrongNumberOfArgs(name.into(), 0));
    }
    for pair in args.windows(2) {
        if !numeric_gte(interp, &pair[0], &pair[1])? {
            return Ok(Value::Nil);
        }
    }
    Ok(Value::T)
}

/// The subr behind `1+', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_add1(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some(x) = one_fixnum(args) {
        return Ok(Value::Integer(x + 1));
    }
    let name = "1+";
    need_args(name, args, 1)?;
    if matches!(args[0].kind(), Kind::Float(_)) {
        Ok(Value::float(numeric_to_f64(interp, &args[0])? + 1.0))
    } else if !matches!(args[0].kind(), Kind::BigInteger(_))
        && let Some(value) = integer_like_i64(interp, &args[0])?.checked_add(1)
    {
        Ok(normalize_integer_value(value))
    } else {
        make_integer(interp, integer_like_bigint(interp, &args[0])? + 1)
    }
}

/// The subr behind `1-', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_sub1(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some(x) = one_fixnum(args) {
        return Ok(Value::Integer(x - 1));
    }
    let name = "1-";
    need_args(name, args, 1)?;
    if matches!(args[0].kind(), Kind::Float(_)) {
        Ok(Value::float(numeric_to_f64(interp, &args[0])? - 1.0))
    } else if !matches!(args[0].kind(), Kind::BigInteger(_))
        && let Some(value) = integer_like_i64(interp, &args[0])?.checked_sub(1)
    {
        Ok(normalize_integer_value(value))
    } else {
        make_integer(interp, integer_like_bigint(interp, &args[0])? - 1)
    }
}

/// The subr behind `eq', by pointer (data.c/fns.c: called through
/// the function cell).
pub(super) fn direct_eq(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "eq";
    need_args(name, args, 2)?;
    let equal = values_eq_in_env(interp, &args[0], &args[1], env);
    Ok(if equal { Value::T } else { Value::Nil })
}

/// ARGS folded from INIT by OP when every one is a fixnum, as data.c's
/// arith_driver combines fixnums without bignums; None when any is not
/// (a bignum or a marker takes the bignum path, which also signals).
/// The bitwise operations keep a fixnum's three top bits equal, so the
/// result is a fixnum again.
fn fold_fixnums(args: &[Value], init: i64, op: impl Fn(i64, i64) -> i64) -> Option<i64> {
    args.iter().try_fold(init, |result, arg| {
        arg.fixnum().map(|value| op(result, value))
    })
}

/// data.c:Fash of fixnum VALUE by fixnum SHIFT when the result is an
/// i64: a right shift always is (arithmetic, to 0 or -1 past the width);
/// a left one when it does not overflow. None sends a left shift that
/// overflows to the bignum path.
fn fixnum_ash(value: i64, shift: i64) -> Option<i64> {
    if shift <= 0 {
        return Some(value >> shift.unsigned_abs().min(63));
    }
    // A zero shifted far still goes through emacs_mpz_mul_2exp, whose
    // limb limit refuses a count past it whatever the value.
    if shift >= 63 {
        return None;
    }
    let shifted = value.checked_shl(shift as u32)?;
    (shifted >> shift == value).then_some(shifted)
}

/// data.c's CHECK_INTEGER for `lognot', `ash' and `logcount': a fixnum
/// or a bignum, never a marker.
fn integer_operand(value: &Value) -> Result<BigInt, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(BigInt::from(n)),
        Kind::BigInteger(n) => Ok(n.into()),
        _ => Err(LispError::WrongTypeArgument("integerp".into(), *value)),
    }
}

/// An argument of `logand', `logior' or `logxor': data.c takes the first
/// with check_integer_coerce_marker, and arith_driver the others with
/// check_number_coerce_marker before it refuses a float, so a later one
/// that is no number names `number-or-marker-p'.
fn logical_operand(interp: &Interpreter, value: &Value, first: bool) -> Result<BigInt, LispError> {
    match value.kind() {
        Kind::Integer(_) | Kind::BigInteger(_) | Kind::Marker(_) => {
            integer_like_bigint(interp, value)
        }
        Kind::Float(_) => Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            *value,
        )),
        _ if first => Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            *value,
        )),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            *value,
        )),
    }
}
