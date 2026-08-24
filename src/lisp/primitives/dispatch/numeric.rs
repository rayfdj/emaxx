use super::*;

fn has_float(args: &[Value]) -> bool {
    args.iter().any(|value| matches!(value, Value::Float(_)))
}

fn has_big_integer(args: &[Value]) -> bool {
    args.iter()
        .any(|value| matches!(value, Value::BigInteger(_)))
}

/// Fold ordinary integers and markers without allocating a `BigInt`.
///
/// `None` means an operand is already a bignum or the checked operation
/// overflowed, in which case the caller restarts through the exact bignum
/// path. Type errors are reported immediately and identically on both paths.
#[inline]
fn checked_integer_fold(
    interp: &Interpreter,
    args: &[Value],
    mut accumulator: i64,
    mut operation: impl FnMut(i64, i64) -> Option<i64>,
) -> Result<Option<i64>, LispError> {
    for arg in args {
        if matches!(arg, Value::BigInteger(_)) {
            return Ok(None);
        }
        let operand = integer_like_i64(interp, arg)?;
        let Some(result) = operation(accumulator, operand) else {
            return Ok(None);
        };
        accumulator = result;
    }
    Ok(Some(accumulator))
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
            "+" => {
                if has_float(args) {
                    // data.c arith_driver: the accumulator starts from the
                    // FIRST argument, not from 0.0 -- (+ -0.0) is -0.0,
                    // while seeding with 0.0 turns it into +0.0 (IEEE
                    // 0.0 + -0.0 = +0.0).  Exposed by the bytecomp
                    // signed-zero cases once `equal' stopped conflating
                    // the zero signs.
                    let mut sum = numeric_to_f64(interp, &args[0])?;
                    for a in &args[1..] {
                        sum += numeric_to_f64(interp, a)?;
                    }
                    Ok(Value::Float(sum))
                } else if let Some(sum) = checked_integer_fold(interp, args, 0, i64::checked_add)? {
                    Ok(Value::Integer(sum))
                } else {
                    let mut sum = BigInt::zero();
                    for a in args {
                        sum += integer_like_bigint(interp, a)?;
                    }
                    Ok(normalize_bigint_value(sum))
                }
            }
            "-" => {
                if args.is_empty() {
                    return Ok(Value::Integer(0));
                }
                if has_float(args) {
                    if args.len() == 1 {
                        return Ok(Value::Float(-numeric_to_f64(interp, &args[0])?));
                    }
                    let mut result = numeric_to_f64(interp, &args[0])?;
                    for a in &args[1..] {
                        result -= numeric_to_f64(interp, a)?;
                    }
                    Ok(Value::Float(result))
                } else {
                    if args.len() == 1 {
                        if !matches!(args[0], Value::BigInteger(_)) {
                            let value = integer_like_i64(interp, &args[0])?;
                            if let Some(result) = value.checked_neg() {
                                return Ok(Value::Integer(result));
                            }
                        }
                        return Ok(normalize_bigint_value(-integer_like_bigint(
                            interp, &args[0],
                        )?));
                    }
                    if !matches!(args[0], Value::BigInteger(_)) {
                        let first = integer_like_i64(interp, &args[0])?;
                        if let Some(result) =
                            checked_integer_fold(interp, &args[1..], first, i64::checked_sub)?
                        {
                            return Ok(Value::Integer(result));
                        }
                    }
                    let mut result = integer_like_bigint(interp, &args[0])?;
                    for a in &args[1..] {
                        result -= integer_like_bigint(interp, a)?;
                    }
                    Ok(normalize_bigint_value(result))
                }
            }
            "*" => {
                if has_float(args) {
                    let mut product = 1.0;
                    for a in args {
                        product *= numeric_to_f64(interp, a)?;
                    }
                    Ok(Value::Float(product))
                } else if let Some(product) =
                    checked_integer_fold(interp, args, 1, i64::checked_mul)?
                {
                    Ok(Value::Integer(product))
                } else {
                    let mut product = BigInt::from(1u8);
                    for a in args {
                        product *= integer_like_bigint(interp, a)?;
                    }
                    Ok(normalize_bigint_value(product))
                }
            }
            "/" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs("/".into(), args.len()));
                }
                if has_float(args) {
                    let mut result = if args.len() == 1 {
                        1.0 / numeric_to_f64(interp, &args[0])?
                    } else {
                        numeric_to_f64(interp, &args[0])?
                    };
                    for a in &args[1..] {
                        result /= numeric_to_f64(interp, a)?;
                    }
                    Ok(Value::Float(result))
                } else if has_big_integer(args) {
                    let mut result = if args.len() == 1 {
                        let divisor = integer_like_bigint(interp, &args[0])?;
                        if divisor.is_zero() {
                            return Err(arith_error());
                        }
                        BigInt::from(1u8) / divisor
                    } else {
                        integer_like_bigint(interp, &args[0])?
                    };
                    for a in &args[1..] {
                        let divisor = integer_like_bigint(interp, a)?;
                        if divisor.is_zero() {
                            return Err(arith_error());
                        }
                        result /= divisor;
                    }
                    Ok(normalize_bigint_value(result))
                } else {
                    let mut result = if args.len() == 1 {
                        let divisor = integer_like_i64(interp, &args[0])?;
                        if divisor == 0 {
                            return Err(arith_error());
                        }
                        1 / divisor
                    } else {
                        integer_like_i64(interp, &args[0])?
                    };
                    for a in &args[1..] {
                        let divisor = integer_like_i64(interp, a)?;
                        if divisor == 0 {
                            return Err(arith_error());
                        }
                        result /= divisor;
                    }
                    Ok(Value::Integer(result))
                }
            }
            "%" | "mod" => {
                need_args(name, args, 2)?;
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
                    return Ok(Value::Float(remainder));
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
                    return Ok(normalize_bigint_value(r));
                }
                let a = integer_like_i64(interp, &args[0])?;
                let b = integer_like_i64(interp, &args[1])?;
                if b == 0 {
                    return Err(arith_error());
                }
                Ok(Value::Integer(if name == "mod" {
                    let mut remainder = a % b;
                    if remainder != 0 && (remainder.is_negative() != b.is_negative()) {
                        remainder += b;
                    }
                    remainder
                } else {
                    a % b
                }))
            }
            "1+" => {
                need_args(name, args, 1)?;
                if matches!(args[0], Value::Float(_)) {
                    Ok(Value::Float(numeric_to_f64(interp, &args[0])? + 1.0))
                } else if !matches!(args[0], Value::BigInteger(_))
                    && let Some(value) = integer_like_i64(interp, &args[0])?.checked_add(1)
                {
                    Ok(Value::Integer(value))
                } else {
                    Ok(normalize_bigint_value(
                        integer_like_bigint(interp, &args[0])? + 1,
                    ))
                }
            }
            "1-" => {
                need_args(name, args, 1)?;
                if matches!(args[0], Value::Float(_)) {
                    Ok(Value::Float(numeric_to_f64(interp, &args[0])? - 1.0))
                } else if !matches!(args[0], Value::BigInteger(_))
                    && let Some(value) = integer_like_i64(interp, &args[0])?.checked_sub(1)
                {
                    Ok(Value::Integer(value))
                } else {
                    Ok(normalize_bigint_value(
                        integer_like_bigint(interp, &args[0])? - 1,
                    ))
                }
            }
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
                if let Value::Float(value) = args[0] {
                    Ok(Value::Float(value.abs()))
                } else if matches!(args[0], Value::BigInteger(_)) {
                    Ok(normalize_bigint_value(
                        integer_like_bigint(interp, &args[0])?.abs(),
                    ))
                } else {
                    let value = integer_like_i64(interp, &args[0])?;
                    match value.checked_abs() {
                        Some(abs) => Ok(Value::Integer(abs)),
                        None => Ok(normalize_bigint_value(BigInt::from(value).abs())),
                    }
                }
            }
            "sin" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?.sin()))
            }
            "cos" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?.cos()))
            }
            "tan" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?.tan()))
            }
            "asin" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?.asin()))
            }
            "acos" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?.acos()))
            }
            "atan" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let y = numeric_to_f64(interp, &args[0])?;
                Ok(Value::Float(if let Some(x) = args.get(1) {
                    y.atan2(numeric_to_f64(interp, x)?)
                } else {
                    y.atan()
                }))
            }
            "copysign" => {
                need_args(name, args, 2)?;
                Ok(Value::Float(
                    numeric_to_f64(interp, &args[0])?.copysign(numeric_to_f64(interp, &args[1])?),
                ))
            }
            "isnan" => {
                need_args(name, args, 1)?;
                let value = match &args[0] {
                    Value::Float(value) => *value,
                    Value::Integer(_) | Value::BigInteger(_) => {
                        return Ok(Value::Nil);
                    }
                    _ => return Err(LispError::WrongTypeArgument("number-or-marker-p".into(), args[0].clone())),
                };
                Ok(if value.is_nan() { Value::T } else { Value::Nil })
            }
            "exp" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?.exp()))
            }
            "expt" => {
                need_args(name, args, 2)?;
                Ok(expt_value(interp, &args[0], &args[1])?)
            }
            "log" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let value = numeric_to_f64(interp, &args[0])?;
                let result = if let Some(base) = args.get(1) {
                    let base = numeric_to_f64(interp, base)?;
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
                Ok(Value::Float(result))
            }
            "sqrt" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?.sqrt()))
            }
            "float" => {
                need_args(name, args, 1)?;
                Ok(Value::Float(numeric_to_f64(interp, &args[0])?))
            }

            "frexp" => {
                need_args(name, args, 1)?;
                let (sig, exp) = frexp_parts(numeric_to_f64(interp, &args[0])?);
                Ok(Value::cons(Value::Float(sig), Value::Integer(exp)))
            }
            "ldexp" => {
                need_args(name, args, 2)?;
                let significand = numeric_to_f64(interp, &args[0])?;
                let exponent = integer_like_i64(interp, &args[1])?;
                Ok(Value::Float(ldexp_value(significand, exponent)))
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
                let value = integer_like_bigint(interp, &args[0])?;
                let shift = integer_like_i64(interp, &args[1])?;
                let shifted = if shift >= 0 {
                    value << shift as usize
                } else {
                    value >> (-shift) as usize
                };
                Ok(normalize_bigint_value(shifted))
            }
            "logcount" => {
                need_args(name, args, 1)?;
                let mut value = integer_like_bigint(interp, &args[0])?;
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
                let mut result = BigInt::from(-1);
                for arg in args {
                    result &= integer_like_bigint(interp, arg)?;
                }
                Ok(normalize_bigint_value(result))
            }
            "logior" => {
                let mut result = BigInt::from(0);
                for arg in args {
                    result |= integer_like_bigint(interp, arg)?;
                }
                Ok(normalize_bigint_value(result))
            }
            "logxor" => {
                let mut result = BigInt::from(0);
                for arg in args {
                    result ^= integer_like_bigint(interp, arg)?;
                }
                Ok(normalize_bigint_value(result))
            }
            "lognot" => {
                need_args(name, args, 1)?;
                Ok(normalize_bigint_value(!integer_like_bigint(
                    interp, &args[0],
                )?))
            }
            "prefix-numeric-value" => {
                need_args(name, args, 1)?;
                prefix_numeric_value(&args[0])
            }

            // ── Comparison ──
            "=" => {
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
            "<" => {
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
            ">" => {
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
            "<=" => {
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
            ">=" => {
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
            "eq" => {
                need_args(name, args, 2)?;
                let equal = values_eq_in_env(interp, &args[0], &args[1], env);
                Ok(if equal { Value::T } else { Value::Nil })
            }
            "eql" => {
                need_args(name, args, 2)?;
                Ok(if values_eql(&args[0], &args[1]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "equal" => {
                need_args(name, args, 2)?;
                let equal = match symbol_with_pos_equal_in_env(interp, &args[0], &args[1], env) {
                    Some(equal) => equal,
                    None => values_equal(interp, &args[0], &args[1]),
                };
                Ok(if equal { Value::T } else { Value::Nil })
            }
            "equal-including-properties" => {
                need_args(name, args, 2)?;
                Ok(if values_equal_including_properties(&args[0], &args[1]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "sxhash-equal" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value(
                    interp,
                    &args[0],
                    HashMode::Equal,
                )))
            }
            "sxhash-eq" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value(interp, &args[0], HashMode::Eq)))
            }
            "sxhash-eql" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value(
                    interp,
                    &args[0],
                    HashMode::Eql,
                )))
            }
            "sxhash-equal-including-properties" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(sxhash_value(
                    interp,
                    &args[0],
                    HashMode::EqualIncludingProperties,
                )))
            }
            "string-equal" => {
                need_args(name, args, 2)?;
                let a = string_comparison_text(&args[0])?;
                let b = string_comparison_text(&args[1])?;
                Ok(if a == b { Value::T } else { Value::Nil })
            }
            "string-lessp" => {
                need_args(name, args, 2)?;
                let a = string_comparison_text(&args[0])?;
                let b = string_comparison_text(&args[1])?;
                let matches = if name == "string>" { a > b } else { a < b };
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "string-version-lessp" => {
                need_args(name, args, 2)?;
                let a = string_comparison_text(&args[0])?;
                let b = string_comparison_text(&args[1])?;
                Ok(if string_version_compare(&a, &b) == Ordering::Less {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "compare-strings" => {
                need_arg_range(name, args, 6, 7)?;
                compare_strings_value(
                    &args[0],
                    args.get(1),
                    args.get(2),
                    &args[3],
                    args.get(4),
                    args.get(5),
                    args.get(6).is_some_and(Value::is_truthy),
                )
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
            "string-collate-lessp" => {
                need_arg_range(name, args, 2, 4)?;
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
            "string-search" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let needle = string_text(&args[0])?;
                let haystack = string_text(&args[1])?;
                let hay_chars: Vec<char> = haystack.chars().collect();
                let start = if args.len() == 3 {
                    let start = args[2].as_integer()?;
                    if start < 0 || start as usize > hay_chars.len() {
                        return Err(LispError::Signal("Args out of range".into()));
                    }
                    start as usize
                } else {
                    0
                };
                if needle.is_empty() {
                    return Ok(Value::Integer(start as i64));
                }
                let suffix: String = hay_chars[start..].iter().collect();
                match suffix.find(&needle) {
                    Some(byte_offset) => {
                        let char_offset = suffix[..byte_offset].chars().count();
                        Ok(Value::Integer((start + char_offset) as i64))
                    }
                    None => Ok(Value::Nil),
                }
            }
        }
    }
);
