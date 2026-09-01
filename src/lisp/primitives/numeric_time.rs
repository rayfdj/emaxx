use super::*;

pub(crate) fn wait_duration(args: &[Value]) -> Result<Duration, LispError> {
    let seconds = args
        .first()
        .map(Value::as_float)
        .transpose()?
        .unwrap_or(0.0);
    let millis = match args.get(1) {
        None | Some(Value::Nil) => 0.0,
        Some(value) => value.as_float()?,
    };
    let total = seconds + millis / 1000.0;
    if !total.is_finite() || total <= 0.0 {
        return Ok(Duration::ZERO);
    }
    Ok(Duration::from_secs_f64(total))
}

#[derive(Clone, Copy)]
pub(crate) enum RoundingKind {
    Ceiling,
    Floor,
    Round,
    Truncate,
}

pub(crate) fn normalize_bigint_value(value: BigInt) -> Value {
    value
        .to_i64()
        .map(Value::Integer)
        .unwrap_or(Value::BigInteger(value.into()))
}

pub(crate) fn string_version_compare(left: &str, right: &str) -> Ordering {
    let left_bytes = left.as_bytes();
    let right_bytes = right.as_bytes();
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left_bytes.len() && right_index < right_bytes.len() {
        let left_byte = left_bytes[left_index];
        let right_byte = right_bytes[right_index];

        if left_byte.is_ascii_digit() && right_byte.is_ascii_digit() {
            let left_start = left_index;
            while left_index < left_bytes.len() && left_bytes[left_index].is_ascii_digit() {
                left_index += 1;
            }
            let right_start = right_index;
            while right_index < right_bytes.len() && right_bytes[right_index].is_ascii_digit() {
                right_index += 1;
            }

            let left_digits = &left[left_start..left_index];
            let right_digits = &right[right_start..right_index];
            let left_trimmed = left_digits.trim_start_matches('0');
            let right_trimmed = right_digits.trim_start_matches('0');
            let left_normalized = if left_trimmed.is_empty() {
                "0"
            } else {
                left_trimmed
            };
            let right_normalized = if right_trimmed.is_empty() {
                "0"
            } else {
                right_trimmed
            };

            match left_normalized.len().cmp(&right_normalized.len()) {
                Ordering::Equal => match left_normalized.cmp(right_normalized) {
                    Ordering::Equal => {}
                    ordering => return ordering,
                },
                ordering => return ordering,
            }
            continue;
        }

        match left_byte.cmp(&right_byte) {
            Ordering::Equal => {
                left_index += 1;
                right_index += 1;
            }
            ordering => return ordering,
        }
    }

    left_bytes.len().cmp(&right_bytes.len())
}

fn arity_value((minimum, maximum): (i64, i64)) -> Value {
    let maximum = match maximum {
        -1 => Value::Symbol("many".into()),
        -2 => Value::Symbol("unevalled".into()),
        maximum => Value::Integer(maximum),
    };
    Value::cons(Value::Integer(minimum), maximum)
}

pub(crate) fn builtin_arity_value(name: &str) -> Option<Value> {
    generated_builtin_arities::generated_builtin_arity(name).map(arity_value)
}

pub(crate) fn special_form_arity_value(name: &str) -> Option<Value> {
    is_special_form_name(name)
        .then(|| generated_builtin_arities::generated_builtin_arity(name))
        .flatten()
        .map(arity_value)
}

pub(crate) fn lambda_arity_value(params: &[String]) -> Value {
    let mut required = 0i64;
    let mut optional = 0i64;
    let mut in_optional = false;
    let mut has_rest = false;

    for param in params {
        match param.as_str() {
            "&optional" => in_optional = true,
            "&rest" | "&body" => {
                has_rest = true;
                break;
            }
            _ if in_optional => optional += 1,
            _ => required += 1,
        }
    }

    Value::cons(
        Value::Integer(required),
        if has_rest {
            Value::Symbol("many".into())
        } else {
            Value::Integer(required + optional)
        },
    )
}

fn invalid_function_arity(function: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("invalid-function".into()),
        function.clone(),
    ]))
}

/// Decode GNU's CLOSURE_ARGLIST using eval.c:lambda_arity and
/// bytecode.c:get_byte_code_arity.
fn closure_arity_value(argument_spec: &Value, function: &Value) -> Result<Value, LispError> {
    if let Value::Integer(packed) = argument_spec {
        if *packed < 0 {
            return Err(invalid_function_arity(function));
        }
        let mandatory = packed & 127;
        let nonrest = packed >> 8;
        return Ok(Value::cons(
            Value::Integer(mandatory),
            if packed & 128 != 0 {
                Value::Symbol("many".into())
            } else {
                Value::Integer(nonrest)
            },
        ));
    }

    let mut required = 0i64;
    let mut maximum = 0i64;
    let mut optional = false;
    let mut cursor = argument_spec.clone();
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        let Value::Cons(cell) = cursor else {
            return if cursor.is_nil() {
                Ok(Value::cons(
                    Value::Integer(required),
                    Value::Integer(maximum),
                ))
            } else {
                Err(invalid_function_arity(function))
            };
        };
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(invalid_function_arity(function));
        }
        let parameter = cell.car.borrow().clone();
        cursor = cell.cdr.borrow().clone();
        let Value::Symbol(parameter) = parameter else {
            return Err(invalid_function_arity(function));
        };
        match parameter.as_ref() {
            "&rest" => {
                return Ok(Value::cons(
                    Value::Integer(required),
                    Value::Symbol("many".into()),
                ));
            }
            "&optional" => optional = true,
            _ => {
                if !optional {
                    required += 1;
                }
                maximum += 1;
            }
        }
    }
}

pub(crate) fn function_arity_value(
    interp: &Interpreter,
    function: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    match function {
        // Every genuine subr has its arity in the GNU-generated table; a
        // miss means an emaxx coverage gap, never a value to invent.
        Value::BuiltinFunc(name) => builtin_arity_value(name)
            .or_else(|| special_form_arity_value(name))
            .ok_or_else(|| {
                LispError::Signal(format!("emaxx: no GNU-derived arity for subr {name}"))
            }),
        Value::Lambda(lambda) => Ok(lambda_arity_value(&lambda.params)),
        Value::Symbol(symbol) => {
            if let Some(arity) = special_form_arity_value(symbol) {
                Ok(arity)
            } else {
                let resolved = interp.lookup_function(symbol, env)?;
                function_arity_value(interp, &resolved, env)
            }
        }
        Value::Record(id)
            if interp
                .find_record(*id)
                .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Closure) =>
        {
            let argument_spec = interp
                .find_record(*id)
                .and_then(|record| record.slots.first())
                .ok_or_else(|| invalid_function_arity(function))?;
            closure_arity_value(argument_spec, function)
        }
        value if is_lambda_expression(interp, value, env) => {
            let items = value.to_vec()?;
            let parameters = items[1]
                .to_vec()?
                .into_iter()
                .map(|parameter| {
                    parameter.as_symbol().map(str::to_string).or_else(|_| {
                        symbols_with_pos_enabled(interp, env)
                            .then(|| symbol_with_pos_parts(interp, &parameter))
                            .flatten()
                            .and_then(|(symbol, _)| symbol.as_symbol().ok().map(str::to_string))
                            .ok_or_else(|| invalid_function_arity(function))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(lambda_arity_value(&parameters))
        }
        Value::Cons(_) => Err(invalid_function_arity(function)),
        _ => Err(LispError::WrongTypeArgument(
            "functionp".into(),
            function.clone(),
        )),
    }
}

pub(crate) fn integer_like_i64(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value {
        Value::Integer(n) => Ok(*n),
        Value::Marker(id) => interp
            .marker_position(*id)
            .map(|pos| pos as i64)
            .ok_or_else(|| {
                LispError::WrongTypeArgument("number-or-marker-p".into(), value.clone())
            }),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            value.clone(),
        )),
    }
}

pub(crate) fn integer_like_bigint(
    interp: &Interpreter,
    value: &Value,
) -> Result<BigInt, LispError> {
    match value {
        Value::Integer(n) => Ok(BigInt::from(*n)),
        Value::BigInteger(n) => Ok(n.clone().into()),
        Value::Marker(id) => interp
            .marker_position(*id)
            .map(BigInt::from)
            .ok_or_else(|| {
                LispError::WrongTypeArgument("number-or-marker-p".into(), value.clone())
            }),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            value.clone(),
        )),
    }
}

pub(crate) fn numeric_to_f64(interp: &Interpreter, value: &Value) -> Result<f64, LispError> {
    match value {
        Value::Float(f) => Ok(*f),
        Value::BigInteger(n) => n.to_f64().ok_or_else(|| {
            LispError::WrongTypeArgument("number-or-marker-p".into(), value.clone())
        }),
        _ => Ok(integer_like_i64(interp, value)? as f64),
    }
}

pub(crate) fn bigint_from_integral_float(value: f64) -> Option<BigInt> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    bigint_from_truncated_float(value).ok()
}

pub(crate) fn apply_rounding_kind(kind: RoundingKind, value: f64) -> Result<f64, LispError> {
    if !value.is_finite() {
        return Err(LispError::Signal("Floating-point overflow".into()));
    }
    Ok(match kind {
        RoundingKind::Ceiling => value.ceil(),
        RoundingKind::Floor => value.floor(),
        RoundingKind::Round => value.round_ties_even(),
        RoundingKind::Truncate => value.trunc(),
    })
}

pub(crate) fn integer_rounding_value(
    interp: &Interpreter,
    kind: RoundingKind,
    args: &[Value],
    float_result: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(
            match (kind, float_result) {
                (RoundingKind::Ceiling, false) => "ceiling",
                (RoundingKind::Floor, false) => "floor",
                (RoundingKind::Round, false) => "round",
                (RoundingKind::Truncate, false) => "truncate",
                (RoundingKind::Ceiling, true) => "fceiling",
                (RoundingKind::Floor, true) => "ffloor",
                (RoundingKind::Round, true) => "fround",
                (RoundingKind::Truncate, true) => "ftruncate",
            }
            .into(),
            args.len(),
        ));
    }
    if float_result && !matches!(args[0], Value::Float(_)) {
        return Err(LispError::WrongTypeArgument(
            "floatp".into(),
            args[0].clone(),
        ));
    }

    if args.len() == 1 {
        if float_result {
            return Ok(Value::Float(apply_rounding_kind(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )?));
        }
        return match &args[0] {
            Value::Integer(_) | Value::BigInteger(_) => Ok(args[0].clone()),
            _ => rounded_f64_to_number_value(apply_rounding_kind(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )?),
        };
    }

    if let Some(rounded) = exact_numeric_division_round(interp, kind, &args[0], &args[1])? {
        return if float_result {
            Ok(Value::Float(numeric_to_f64(interp, &rounded)?))
        } else {
            Ok(rounded)
        };
    }

    if let (Some(numerator), Some(divisor)) = (
        integer_like_bigint_for_rounding(interp, &args[0]),
        integer_like_bigint_for_rounding(interp, &args[1]),
    ) {
        if divisor.is_zero() {
            return Err(arith_error());
        }
        let rounded = exact_integer_division_round(kind, numerator, divisor);
        return if float_result {
            Ok(Value::Float(
                numeric_to_f64(interp, &rounded).unwrap_or(f64::NAN),
            ))
        } else {
            Ok(rounded)
        };
    }

    let divisor = numeric_to_f64(interp, &args[1])?;
    if divisor == 0.0 || divisor.is_nan() {
        return Err(arith_error());
    }
    let quotient = numeric_to_f64(interp, &args[0])? / divisor;
    let rounded = apply_rounding_kind(kind, quotient)?;
    if float_result {
        Ok(Value::Float(rounded))
    } else {
        rounded_f64_to_number_value(rounded)
    }
}

pub(crate) fn rounded_f64_to_number_value(value: f64) -> Result<Value, LispError> {
    Ok(normalize_bigint_value(bigint_from_truncated_float(value)?))
}

pub(crate) fn integer_like_bigint_for_rounding(
    interp: &Interpreter,
    value: &Value,
) -> Option<BigInt> {
    match value {
        Value::Float(value) => bigint_from_integral_float(*value),
        _ => integer_like_bigint(interp, value).ok(),
    }
}

pub(crate) fn exact_numeric_division_round(
    interp: &Interpreter,
    kind: RoundingKind,
    numerator: &Value,
    divisor: &Value,
) -> Result<Option<Value>, LispError> {
    let Some((num_sig, num_exp)) = exact_binary_rational(interp, numerator)? else {
        return Ok(None);
    };
    let Some((div_sig, div_exp)) = exact_binary_rational(interp, divisor)? else {
        return Ok(None);
    };
    if div_sig.is_zero() {
        return Err(arith_error());
    }
    let mut scaled_num = num_sig;
    let mut scaled_div = div_sig;
    let exponent_delta = num_exp - div_exp;
    if exponent_delta >= 0 {
        scaled_num <<= exponent_delta as usize;
    } else {
        scaled_div <<= (-exponent_delta) as usize;
    }
    Ok(Some(exact_integer_division_round(
        kind, scaled_num, scaled_div,
    )))
}

pub(crate) fn exact_integer_division_round(
    kind: RoundingKind,
    numerator: BigInt,
    divisor: BigInt,
) -> Value {
    let quotient = &numerator / &divisor;
    let remainder = &numerator % &divisor;
    if remainder.is_zero() {
        return normalize_bigint_value(quotient);
    }
    let same_sign = numerator.sign() == divisor.sign();
    let adjusted = match kind {
        RoundingKind::Truncate => quotient,
        RoundingKind::Floor => {
            if same_sign {
                quotient
            } else {
                quotient - 1
            }
        }
        RoundingKind::Ceiling => {
            if same_sign {
                quotient + 1
            } else {
                quotient
            }
        }
        RoundingKind::Round => {
            let twice_remainder = remainder.abs() * 2;
            let divisor_abs = divisor.abs();
            if twice_remainder < divisor_abs {
                quotient
            } else if twice_remainder > divisor_abs {
                if same_sign {
                    quotient + 1
                } else {
                    quotient - 1
                }
            } else if (&quotient & BigInt::from(1u8)).is_zero() {
                quotient
            } else if same_sign {
                quotient + 1
            } else {
                quotient - 1
            }
        }
    };
    normalize_bigint_value(adjusted)
}

pub(crate) fn frexp_parts(value: f64) -> (f64, i64) {
    if value == 0.0 {
        return (value, 0);
    }
    let exponent = value.abs().log2().floor() as i64 + 1;
    let significand = value / ldexp_value(1.0, exponent);
    (significand, exponent)
}

pub(crate) fn ldexp_value(significand: f64, exponent: i64) -> f64 {
    if exponent > i32::MAX as i64 {
        return if significand == 0.0 {
            0.0
        } else if significand.is_sign_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if exponent < i32::MIN as i64 {
        return 0.0_f64.copysign(significand);
    }
    significand * 2.0_f64.powi(exponent as i32)
}

pub(crate) fn logb_value(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Integer(number) => {
            if *number == 0 {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            Ok(Value::Integer(
                i64::BITS as i64 - 1 - number.unsigned_abs().leading_zeros() as i64,
            ))
        }
        Value::BigInteger(number) => {
            if number.is_zero() {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            Ok(Value::Integer(
                number.abs().to_str_radix(2).len() as i64 - 1,
            ))
        }
        _ => {
            let value = numeric_to_f64(interp, value)?;
            if !value.is_finite() || value == 0.0 {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            let (_sig, exponent) = frexp_parts(value.abs());
            Ok(Value::Integer(exponent - 1))
        }
    }
}

pub(crate) fn expt_value(
    interp: &Interpreter,
    base: &Value,
    exponent: &Value,
) -> Result<Value, LispError> {
    let exponent_bigint = integer_like_bigint(interp, exponent);
    if matches!(base, Value::Float(_)) || matches!(exponent, Value::Float(_)) {
        return Ok(Value::Float(
            numeric_to_f64(interp, base)?.powf(numeric_to_f64(interp, exponent)?),
        ));
    }
    let exponent_bigint = exponent_bigint?;
    if exponent_bigint.is_negative() {
        let base_value = integer_like_i64(interp, base)? as f64;
        let exponent_value = exponent_bigint.to_f64().ok_or_else(|| {
            LispError::WrongTypeArgument("number-or-marker-p".into(), exponent.clone())
        })?;
        return Ok(Value::Float(base_value.powf(exponent_value)));
    }

    let base_bigint = integer_like_bigint(interp, base)?;
    if exponent_bigint.is_zero() {
        return Ok(Value::Integer(1));
    }
    if base_bigint.is_zero() {
        return Ok(Value::Integer(0));
    }
    if base_bigint == BigInt::from(1) {
        return Ok(Value::Integer(1));
    }
    if base_bigint == BigInt::from(-1) {
        let even = (&exponent_bigint & BigInt::from(1u8)).is_zero();
        return Ok(Value::Integer(if even { 1 } else { -1 }));
    }
    let exponent_u32 = exponent_bigint
        .to_u32()
        .ok_or_else(|| LispError::Signal("Exponent too large".into()))?;
    Ok(normalize_bigint_value(base_bigint.pow(exponent_u32)))
}

pub(crate) fn exact_binary_rational(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<(BigInt, i32)>, LispError> {
    match value {
        Value::Float(value) => Ok(exact_float_binary_rational(*value)),
        Value::Integer(value) => Ok(Some((BigInt::from(*value), 0))),
        Value::BigInteger(value) => Ok(Some((value.clone().into(), 0))),
        Value::Marker(_) => Ok(Some((BigInt::from(integer_like_i64(interp, value)?), 0))),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            value.clone(),
        )),
    }
}

pub(crate) fn exact_float_binary_rational(value: f64) -> Option<(BigInt, i32)> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some((BigInt::zero(), 0));
    }
    let bits = value.to_bits();
    let negative = bits >> 63 != 0;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = bits & ((1u64 << 52) - 1);
    let (significand, exponent) = if exponent_bits == 0 {
        (mantissa, 1 - 1023 - 52)
    } else {
        ((1u64 << 52) | mantissa, exponent_bits - 1023 - 52)
    };
    let mut bigint = BigInt::from(significand);
    if negative {
        bigint = -bigint;
    }
    Some((bigint, exponent))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExactTimeValue {
    ticks: BigInt,
    hz: BigInt,
}

#[derive(Clone, Debug)]
pub(crate) struct ZoneSpec {
    offset_seconds: i32,
    abbreviation: String,
    is_dst: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PosixTransitionRule {
    month: u32,
    week: u32,
    weekday: u32,
    seconds: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct PosixTimeZone {
    std_abbr: String,
    std_offset: i32,
    dst_abbr: String,
    dst_offset: i32,
    start: PosixTransitionRule,
    end: PosixTransitionRule,
}

pub(crate) fn exact_time_value(ticks: BigInt, hz: BigInt) -> Result<ExactTimeValue, LispError> {
    if hz <= BigInt::zero() {
        return Err(LispError::Signal("Invalid time resolution".into()));
    }
    if ticks.is_zero() {
        return Ok(ExactTimeValue {
            ticks: BigInt::zero(),
            hz: BigInt::from(1u8),
        });
    }
    let divisor = bigint_gcd(ticks.abs(), hz.clone());
    Ok(ExactTimeValue {
        ticks: ticks / &divisor,
        hz: hz / divisor,
    })
}

pub(crate) fn bigint_gcd(mut left: BigInt, mut right: BigInt) -> BigInt {
    while !right.is_zero() {
        let remainder = left % &right;
        left = right;
        right = remainder;
    }
    left.abs()
}

pub(crate) fn current_time_value() -> Result<ExactTimeValue, LispError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let ticks = BigInt::from(now.as_secs()) * BigInt::from(1_000_000_000u64)
        + BigInt::from(now.subsec_nanos());
    exact_time_value(ticks, BigInt::from(1_000_000_000u64))
}

pub(crate) fn exact_time_from_float(value: f64) -> Result<ExactTimeValue, LispError> {
    let Some((significand, exponent)) = exact_float_binary_rational(value) else {
        return Err(LispError::TypeError("number".into(), "float".into()));
    };
    if exponent >= 0 {
        exact_time_value(significand << exponent as usize, BigInt::from(1u8))
    } else {
        exact_time_value(significand, BigInt::from(1u8) << (-exponent) as usize)
    }
}

pub(crate) fn exact_time_from_old_style(
    interp: &Interpreter,
    items: &[Value],
) -> Result<ExactTimeValue, LispError> {
    if !(2..=4).contains(&items.len()) {
        return Err(LispError::TypeError(
            "time-value".into(),
            Value::list(items.to_vec()).type_name(),
        ));
    }
    let high = integer_like_bigint(interp, &items[0])?;
    let low = integer_like_bigint(interp, &items[1])?;
    let micros = if items.len() >= 3 {
        integer_like_bigint(interp, &items[2])?
    } else {
        BigInt::zero()
    };
    let picos = if items.len() >= 4 {
        integer_like_bigint(interp, &items[3])?
    } else {
        BigInt::zero()
    };
    let ticks = (high * BigInt::from(65_536u32) + low) * BigInt::from(1_000_000_000_000u64)
        + micros * BigInt::from(1_000_000u32)
        + picos;
    exact_time_value(ticks, BigInt::from(1_000_000_000_000u64))
}

pub(crate) fn exact_time_from_value(
    interp: &Interpreter,
    value: &Value,
    now: &ExactTimeValue,
) -> Result<ExactTimeValue, LispError> {
    match value {
        Value::Nil => Ok(now.clone()),
        Value::Integer(value) => exact_time_value(BigInt::from(*value), BigInt::from(1u8)),
        Value::BigInteger(value) => exact_time_value(value.clone().into(), BigInt::from(1u8)),
        Value::Float(value) => exact_time_from_float(*value),
        Value::Cons(cons_cell) => {
            let car = &cons_cell.car;
            let cdr = &cons_cell.cdr;
            if let Ok(items) = value.to_vec()
                && (2..=4).contains(&items.len())
            {
                return exact_time_from_old_style(interp, &items);
            }
            exact_time_value(
                integer_like_bigint(interp, &car.borrow())?,
                integer_like_bigint(interp, &cdr.borrow())?,
            )
        }
        _ => Err(LispError::TypeError("time-value".into(), value.type_name())),
    }
}

pub(crate) fn floor_div_mod(value: &BigInt, divisor: &BigInt) -> (BigInt, BigInt) {
    let mut quotient = value / divisor;
    let mut remainder = value % divisor;
    if remainder.sign() == Sign::Minus {
        quotient -= 1;
        remainder += divisor;
    }
    (quotient, remainder)
}

pub(crate) fn time_floor_parts(time: &ExactTimeValue) -> (BigInt, BigInt) {
    floor_div_mod(&time.ticks, &time.hz)
}

pub(crate) fn exact_time_to_value(time: &ExactTimeValue) -> Value {
    if time.hz == BigInt::from(1u8) {
        normalize_bigint_value(time.ticks.clone())
    } else {
        Value::cons(
            normalize_bigint_value(time.ticks.clone()),
            normalize_bigint_value(time.hz.clone()),
        )
    }
}

pub(crate) fn exact_time_to_tick_pair(time: &ExactTimeValue) -> Value {
    Value::cons(
        normalize_bigint_value(time.ticks.clone()),
        normalize_bigint_value(time.hz.clone()),
    )
}

pub(crate) fn exact_time_floor_integer_value(time: &ExactTimeValue) -> Value {
    let (whole, _) = time_floor_parts(time);
    normalize_bigint_value(whole)
}

pub(crate) fn exact_time_to_scaled_pair(
    time: &ExactTimeValue,
    hz: &BigInt,
) -> Result<Value, LispError> {
    if hz <= &BigInt::zero() {
        return Err(LispError::Signal("Invalid time resolution".into()));
    }
    // GNU rounds toward negative infinity when the target clock cannot
    // represent the time exactly ((time-convert '(1 . 3) 2) => (0 . 2)).
    let scaled = &time.ticks * hz;
    let (ticks, _) = floor_div_mod(&scaled, &time.hz);
    Ok(Value::cons(
        normalize_bigint_value(ticks),
        normalize_bigint_value(hz.clone()),
    ))
}

pub(crate) fn exact_time_to_old_style(time: &ExactTimeValue) -> Result<Value, LispError> {
    let scaled = &time.ticks * BigInt::from(1_000_000_000_000u64);
    let (picoseconds, _) = floor_div_mod(&scaled, &time.hz);
    let (whole_seconds, fractional_picoseconds) =
        floor_div_mod(&picoseconds, &BigInt::from(1_000_000_000_000u64));
    let (high, low) = floor_div_mod(&whole_seconds, &BigInt::from(65_536u32));
    let (micros, picos) = floor_div_mod(&fractional_picoseconds, &BigInt::from(1_000_000u32));
    Ok(Value::list([
        normalize_bigint_value(high),
        normalize_bigint_value(low),
        normalize_bigint_value(micros),
        normalize_bigint_value(picos),
    ]))
}

pub(crate) fn power_of_two_exponent(value: &BigInt) -> Option<i32> {
    if value <= &BigInt::zero() {
        return None;
    }
    let mut exponent = 0i32;
    let mut current = value.clone();
    let two = BigInt::from(2u8);
    while (&current % &two).is_zero() {
        current /= &two;
        exponent += 1;
    }
    (current == BigInt::from(1u8)).then_some(exponent)
}

pub(crate) fn exact_time_to_f64(time: &ExactTimeValue) -> f64 {
    if let Some(exponent) = power_of_two_exponent(&time.hz)
        && let Some(ticks) = time.ticks.to_f64()
    {
        let mut value = ticks;
        let mut remaining = exponent;
        while remaining > 0 {
            let chunk = remaining.min(1022);
            value *= 2f64.powi(-chunk);
            remaining -= chunk;
        }
        return value;
    }
    let ticks = time.ticks.to_f64().unwrap_or_else(|| {
        if time.ticks.sign() == Sign::Minus {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    });
    let hz = time.hz.to_f64().unwrap_or(f64::INFINITY);
    ticks / hz
}

pub(crate) fn exact_time_equal(left: &ExactTimeValue, right: &ExactTimeValue) -> bool {
    left.ticks.clone() * &right.hz == right.ticks.clone() * &left.hz
}

pub(crate) fn exact_time_less(left: &ExactTimeValue, right: &ExactTimeValue) -> bool {
    left.ticks.clone() * &right.hz < right.ticks.clone() * &left.hz
}

fn posix_time_zone_from_value(value: &Value) -> Option<PosixTimeZone> {
    string_text(value)
        .ok()
        .and_then(|text| parse_posix_tz(&text))
}

pub(crate) fn local_zone_spec(interp: &Interpreter, time: Option<&ExactTimeValue>) -> ZoneSpec {
    if let Some(posix) = posix_time_zone_from_value(&interp.local_time_zone_rule)
        && let Some(time) = time
    {
        return posix.zone_for_instant(time);
    }
    if !local_time_zone_rule_is_wall(&interp.local_time_zone_rule)
        && let Ok(zone) = explicit_zone_spec_from_value(&interp.local_time_zone_rule, time)
    {
        return zone;
    }
    system_wall_zone_spec(time)
}

fn system_wall_zone_spec(time: Option<&ExactTimeValue>) -> ZoneSpec {
    let offset_seconds = time
        .and_then(|value| {
            let (whole_seconds, _) = time_floor_parts(value);
            whole_seconds.to_i64()
        })
        .and_then(|seconds| Local.timestamp_opt(seconds, 0).single())
        .map(|datetime| datetime.offset().local_minus_utc())
        .unwrap_or_else(|| Local::now().offset().local_minus_utc());
    ZoneSpec {
        offset_seconds,
        abbreviation: format_numeric_zone_name(offset_seconds),
        is_dst: false,
    }
}

pub(crate) fn local_zone_spec_for_civil(
    interp: &Interpreter,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> ZoneSpec {
    if let Some(posix) = posix_time_zone_from_value(&interp.local_time_zone_rule) {
        return posix.zone_for_civil(year, month, day, hour, minute, second);
    }
    if !local_time_zone_rule_is_wall(&interp.local_time_zone_rule)
        && let Ok(zone) = explicit_zone_spec_from_value(&interp.local_time_zone_rule, None)
    {
        return zone;
    }
    // A local civil time must be resolved with the offset that applied on
    // that date, not today's offset.  Historical timezone changes can
    // otherwise move midnight into the previous day when encode-time is
    // followed by decode-time (icalendar's anniversary import does exactly
    // that).  For an ambiguous or skipped wall time, use Chrono's earliest
    // representable side; GNU likewise normalizes rather than substituting
    // the current offset.
    let local = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .and_then(|civil| {
            let resolved = Local.from_local_datetime(&civil);
            resolved
                .single()
                .or_else(|| resolved.earliest())
                .or_else(|| resolved.latest())
        });
    local.map_or_else(
        || system_wall_zone_spec(None),
        |datetime| ZoneSpec {
            offset_seconds: datetime.offset().local_minus_utc(),
            abbreviation: datetime.format("%Z").to_string(),
            is_dst: false,
        },
    )
}

pub(crate) fn normalize_decoded_civil_time(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i64,
) -> Result<chrono::NaiveDateTime, LispError> {
    let total_months = i64::from(year)
        .checked_mul(12)
        .and_then(|value| value.checked_add(i64::from(month) - 1))
        .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
    let normalized_year = total_months.div_euclid(12);
    let normalized_month = total_months.rem_euclid(12) + 1;
    let normalized_year = i32::try_from(normalized_year)
        .map_err(|_| LispError::Signal("Invalid decoded time".into()))?;
    let base_date = chrono::NaiveDate::from_ymd_opt(normalized_year, normalized_month as u32, 1)
        .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
    let base_time = base_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
    let day_offset = chrono::Duration::days(i64::from(day) - 1);
    let second_offset = chrono::Duration::seconds(
        i64::from(hour)
            .checked_mul(3600)
            .and_then(|value| value.checked_add(i64::from(minute) * 60))
            .and_then(|value| value.checked_add(second))
            .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?,
    );
    base_time
        .checked_add_signed(day_offset)
        .and_then(|value| value.checked_add_signed(second_offset))
        .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))
}

pub(crate) fn format_numeric_zone_name(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.abs();
    let hours = abs / 3600;
    let minutes = (abs % 3600) / 60;
    let seconds = abs % 60;
    if seconds != 0 {
        format!("{sign}{hours:02}{minutes:02}{seconds:02}")
    } else if minutes != 0 {
        format!("{sign}{hours:02}{minutes:02}")
    } else {
        format!("{sign}{hours:02}")
    }
}

pub(crate) fn parse_posix_zone_string(value: &str) -> Option<ZoneSpec> {
    let abbr_end = value
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(value.len());
    let abbreviation = value[..abbr_end].to_string();
    if abbreviation.is_empty() {
        return None;
    }
    let rest = &value[abbr_end..];
    if rest.is_empty() {
        return Some(ZoneSpec {
            offset_seconds: 0,
            abbreviation,
            is_dst: false,
        });
    }
    let (sign, digits) = match rest.chars().next() {
        Some('+') | Some('-') => (rest.chars().next()?, &rest[1..]),
        Some(_) => ('+', rest),
        None => ('+', ""),
    };
    let mut parts = digits.split(':');
    let hours = parts.next()?.parse::<i32>().ok()?;
    let minutes = parts
        .next()
        .map_or(Some(0), |part| part.parse::<i32>().ok())?;
    let seconds = parts
        .next()
        .map_or(Some(0), |part| part.parse::<i32>().ok())?;
    if parts.next().is_some() {
        return None;
    }
    let magnitude = hours * 3600 + minutes * 60 + seconds;
    let offset_seconds = if sign == '-' { magnitude } else { -magnitude };
    Some(ZoneSpec {
        offset_seconds,
        abbreviation,
        is_dst: false,
    })
}

pub(crate) fn parse_posix_abbr(input: &str) -> Option<(&str, &str)> {
    let end = input
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(input.len());
    (end > 0).then_some((&input[..end], &input[end..]))
}

pub(crate) fn parse_posix_offset(input: &str) -> Option<(i32, &str)> {
    let sign_len = usize::from(matches!(input.as_bytes().first(), Some(b'+') | Some(b'-')));
    let digit_end = input[sign_len..]
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_digit() && ch != ':').then_some(sign_len + index))
        .unwrap_or(input.len());
    if digit_end == sign_len {
        return None;
    }
    let raw = &input[..digit_end];
    let (sign, digits) = match raw.chars().next() {
        Some('+') => (1, &raw[1..]),
        Some('-') => (-1, &raw[1..]),
        _ => (1, raw),
    };
    let mut parts = digits.split(':');
    let hours = parts.next()?.parse::<i32>().ok()?;
    let minutes = parts
        .next()
        .map_or(Some(0), |part| part.parse::<i32>().ok())?;
    let seconds = parts
        .next()
        .map_or(Some(0), |part| part.parse::<i32>().ok())?;
    if parts.next().is_some() {
        return None;
    }
    let magnitude = hours * 3600 + minutes * 60 + seconds;
    Some((-sign * magnitude, &input[digit_end..]))
}

pub(crate) fn parse_posix_rule(input: &str) -> Option<PosixTransitionRule> {
    let input = input.strip_prefix('M')?;
    let mut parts = input.splitn(2, '/');
    let date = parts.next()?;
    let seconds = parts
        .next()
        .and_then(parse_posix_offset)
        .and_then(|(offset, rest)| rest.is_empty().then_some(offset.unsigned_abs()))
        .unwrap_or(2 * 3600);
    let mut date_parts = date.split('.');
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let week = date_parts.next()?.parse::<u32>().ok()?;
    let weekday = date_parts.next()?.parse::<u32>().ok()?;
    if date_parts.next().is_some()
        || !(1..=12).contains(&month)
        || !(1..=5).contains(&week)
        || weekday > 6
        || seconds >= 24 * 3600
    {
        return None;
    }
    Some(PosixTransitionRule {
        month,
        week,
        weekday,
        seconds,
    })
}

pub(crate) fn parse_posix_tz(value: &str) -> Option<PosixTimeZone> {
    let (std_abbr, rest) = parse_posix_abbr(value)?;
    let (std_offset, rest) = parse_posix_offset(rest)?;
    let (dst_abbr, rest) = parse_posix_abbr(rest)?;
    let (dst_offset, rest) = if let Some((offset, rest)) = parse_posix_offset(rest) {
        (offset, rest)
    } else {
        (std_offset + 3600, rest)
    };
    let rest = rest.strip_prefix(',')?;
    let (start, end) = rest.split_once(',')?;
    Some(PosixTimeZone {
        std_abbr: std_abbr.to_string(),
        std_offset,
        dst_abbr: dst_abbr.to_string(),
        dst_offset,
        start: parse_posix_rule(start)?,
        end: parse_posix_rule(end)?,
    })
}

pub(crate) fn transition_date(year: i32, rule: &PosixTransitionRule) -> Option<chrono::NaiveDate> {
    let first = chrono::NaiveDate::from_ymd_opt(year, rule.month, 1)?;
    let first_weekday = first.weekday().num_days_from_sunday();
    let delta = (rule.weekday + 7 - first_weekday) % 7;
    let day = if rule.week < 5 {
        1 + delta + 7 * (rule.week - 1)
    } else {
        let (next_year, next_month) = if rule.month == 12 {
            (year + 1, 1)
        } else {
            (year, rule.month + 1)
        };
        let next_month_first = chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1)?;
        let last = next_month_first.pred_opt()?;
        let last_weekday = last.weekday().num_days_from_sunday();
        last.day() - ((last_weekday + 7 - rule.weekday) % 7)
    };
    chrono::NaiveDate::from_ymd_opt(year, rule.month, day)
}

pub(crate) fn transition_utc_timestamp(
    year: i32,
    rule: &PosixTransitionRule,
    offset_before: i32,
) -> Option<i64> {
    let date = transition_date(year, rule)?;
    let local = date.and_hms_opt(
        rule.seconds / 3600,
        (rule.seconds % 3600) / 60,
        rule.seconds % 60,
    )?;
    Some(local.and_utc().timestamp() - i64::from(offset_before))
}

impl PosixTimeZone {
    fn zone_for_instant(&self, time: &ExactTimeValue) -> ZoneSpec {
        let (whole_seconds, _) = time_floor_parts(time);
        let seconds = whole_seconds.to_i64().unwrap_or(0);
        let utc = Utc
            .timestamp_opt(seconds, 0)
            .single()
            .unwrap_or_else(Utc::now);
        let year = utc.year();
        let start = transition_utc_timestamp(year, &self.start, self.std_offset);
        let end = transition_utc_timestamp(year, &self.end, self.dst_offset);
        let is_dst = match (start, end) {
            (Some(start), Some(end)) if start <= end => seconds >= start && seconds < end,
            (Some(start), Some(end)) => seconds >= start || seconds < end,
            _ => false,
        };
        self.zone(is_dst)
    }

    fn zone_for_civil(
        &self,
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> ZoneSpec {
        let Some(local) = chrono::NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_opt(hour, minute, second))
        else {
            return self.zone(false);
        };
        let std_seconds = local.and_utc().timestamp() - i64::from(self.std_offset);
        let time = ExactTimeValue {
            ticks: BigInt::from(std_seconds),
            hz: BigInt::from(1u8),
        };
        self.zone_for_instant(&time)
    }

    fn zone(&self, is_dst: bool) -> ZoneSpec {
        if is_dst {
            ZoneSpec {
                offset_seconds: self.dst_offset,
                abbreviation: self.dst_abbr.clone(),
                is_dst: true,
            }
        } else {
            ZoneSpec {
                offset_seconds: self.std_offset,
                abbreviation: self.std_abbr.clone(),
                is_dst: false,
            }
        }
    }
}

fn local_time_zone_rule_is_wall(rule: &Value) -> bool {
    rule.is_nil() || matches!(rule, Value::Symbol(symbol) if symbol == "wall")
}

pub(crate) fn zone_spec_from_value(
    interp: &Interpreter,
    zone: &Value,
    time: Option<&ExactTimeValue>,
) -> Result<ZoneSpec, LispError> {
    match zone {
        Value::Nil => Ok(local_zone_spec(interp, time)),
        Value::Symbol(symbol) if symbol == "-" => Ok(local_zone_spec(interp, time)),
        _ => explicit_zone_spec_from_value(zone, time),
    }
}

fn explicit_zone_spec_from_value(
    zone: &Value,
    time: Option<&ExactTimeValue>,
) -> Result<ZoneSpec, LispError> {
    match zone {
        Value::T => Ok(ZoneSpec {
            offset_seconds: 0,
            abbreviation: "UTC".into(),
            is_dst: false,
        }),
        Value::Integer(value) => Ok(ZoneSpec {
            offset_seconds: *value as i32,
            abbreviation: format_numeric_zone_name(*value as i32),
            is_dst: false,
        }),
        Value::BigInteger(value) => {
            let offset = value
                .to_i32()
                .ok_or_else(|| LispError::WrongTypeArgument("integerp".into(), zone.clone()))?;
            Ok(ZoneSpec {
                offset_seconds: offset,
                abbreviation: format_numeric_zone_name(offset),
                is_dst: false,
            })
        }
        _ if zone.is_string() => {
            let text = string_text(zone)?;
            if let Some(posix) = parse_posix_tz(&text)
                && let Some(time) = time
            {
                return Ok(posix.zone_for_instant(time));
            }
            Ok(parse_posix_zone_string(&text).unwrap_or(ZoneSpec {
                offset_seconds: 0,
                abbreviation: "UTC".into(),
                is_dst: false,
            }))
        }
        Value::Cons(_) => {
            let items = zone.to_vec()?;
            if items.is_empty() {
                return Err(LispError::TypeError("time-zone".into(), zone.type_name()));
            }
            let offset = match &items[0] {
                Value::Integer(value) => *value as i32,
                Value::BigInteger(value) => value.to_i32().ok_or_else(|| {
                    LispError::WrongTypeArgument("integerp".into(), items[0].clone())
                })?,
                _ => {
                    return Err(LispError::WrongTypeArgument(
                        "integerp".into(),
                        items[0].clone(),
                    ));
                }
            };
            let abbreviation = items
                .get(1)
                .and_then(|value| string_text(value).ok())
                .unwrap_or_else(|| format_numeric_zone_name(offset));
            Ok(ZoneSpec {
                offset_seconds: offset,
                abbreviation,
                is_dst: false,
            })
        }
        _ => Err(LispError::TypeError("time-zone".into(), zone.type_name())),
    }
}

pub(crate) fn zone_offset(zone: &ZoneSpec) -> Result<FixedOffset, LispError> {
    FixedOffset::east_opt(zone.offset_seconds)
        .ok_or_else(|| LispError::Signal("Invalid time zone".into()))
}

pub(crate) fn time_local_datetime(
    time: &ExactTimeValue,
    zone: &ZoneSpec,
) -> Result<(chrono::DateTime<FixedOffset>, BigInt), LispError> {
    let (whole_seconds, fractional_ticks) = time_floor_parts(time);
    let whole_seconds = whole_seconds
        .to_i64()
        .ok_or_else(|| LispError::Signal("Time out of range".into()))?;
    let offset = zone_offset(zone)?;
    let utc = Utc
        .timestamp_opt(whole_seconds, 0)
        .single()
        .ok_or_else(|| LispError::Signal("Time out of range".into()))?;
    Ok((utc.with_timezone(&offset), fractional_ticks))
}

pub(crate) fn fraction_picoseconds(time: &ExactTimeValue) -> BigInt {
    let (_, fractional_ticks) = time_floor_parts(time);
    (&fractional_ticks * BigInt::from(1_000_000_000_000u64)) / &time.hz
}

pub(crate) fn format_fraction_digits(picoseconds: &BigInt, width: usize) -> String {
    let base = format!("{:012}", picoseconds.to_u64().unwrap_or(0));
    if width <= 12 {
        base[..width].to_string()
    } else {
        format!("{base}{}", "0".repeat(width - 12))
    }
}

pub(crate) fn trim_trailing_zeros(text: &str) -> String {
    let trimmed = text.trim_end_matches('0');
    if trimmed.is_empty() {
        "0".into()
    } else {
        trimmed.into()
    }
}

pub(crate) fn strip_leading_zeros(text: &str) -> String {
    let trimmed = text.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".into()
    } else {
        trimmed.into()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimeFormatSpec {
    minimal: bool,
    pad: Option<char>,
    colons: usize,
    width: Option<usize>,
    uppercase: bool,
    opposite_case: bool,
    force_sign: bool,
    modifier: Option<char>,
    conversion: char,
}

pub(crate) fn parse_time_format_spec(spec: &[char], index: &mut usize) -> TimeFormatSpec {
    let mut minimal = false;
    let mut pad = None;
    let mut colons = 0usize;
    let mut uppercase = false;
    let mut opposite_case = false;
    let mut force_sign = false;
    while *index < spec.len() {
        match spec[*index] {
            '-' => {
                minimal = true;
                *index += 1;
            }
            '_' => {
                pad = Some(' ');
                *index += 1;
            }
            '0' => {
                pad = Some('0');
                *index += 1;
            }
            '+' => {
                force_sign = true;
                *index += 1;
            }
            '^' => {
                uppercase = true;
                *index += 1;
            }
            '#' => {
                opposite_case = true;
                *index += 1;
            }
            ':' => {
                colons += 1;
                *index += 1;
            }
            _ => break,
        }
    }
    let width_start = *index;
    while *index < spec.len() && spec[*index].is_ascii_digit() {
        *index += 1;
    }
    let width = if *index > width_start {
        spec[width_start..*index]
            .iter()
            .collect::<String>()
            .parse::<usize>()
            .ok()
    } else {
        None
    };
    let modifier = spec
        .get(*index)
        .copied()
        .filter(|ch| matches!(ch, 'E' | 'O'));
    if modifier.is_some() {
        *index += 1;
    }
    let conversion = spec.get(*index).copied().unwrap_or('%');
    TimeFormatSpec {
        minimal,
        pad,
        colons,
        width,
        uppercase,
        opposite_case,
        force_sign,
        modifier,
        conversion,
    }
}

fn pad_time_field(mut field: String, width: usize, pad: char) -> String {
    if field.len() >= width {
        return field;
    }
    let padding = width - field.len();
    if pad == '0' && matches!(field.as_bytes().first(), Some(b'+' | b'-')) {
        let sign = field.remove(0);
        format!("{sign}{}{field}", "0".repeat(padding))
    } else {
        format!("{}{field}", pad.to_string().repeat(padding))
    }
}

fn numeric_time_field(
    value: i64,
    default_width: usize,
    default_pad: char,
    spec: TimeFormatSpec,
) -> String {
    let field = value.to_string();
    if spec.minimal {
        field
    } else {
        pad_time_field(
            field,
            spec.width.unwrap_or(default_width),
            spec.pad.unwrap_or(default_pad),
        )
    }
}

fn year_time_field(year: i32, spec: TimeFormatSpec) -> String {
    let field = if spec.force_sign && year > 9_999 {
        format!("+{year}")
    } else {
        year.to_string()
    };
    if spec.minimal {
        field
    } else {
        pad_time_field(field, spec.width.unwrap_or(4), spec.pad.unwrap_or('0'))
    }
}

fn present_time_field(mut field: String, spec: TimeFormatSpec) -> String {
    if spec.uppercase {
        field = field.to_uppercase();
    } else if spec.opposite_case {
        field = if field.chars().any(char::is_lowercase) {
            field.to_uppercase()
        } else {
            field.to_lowercase()
        };
    }
    if !spec.minimal
        && let Some(width) = spec.width
    {
        field = pad_time_field(field, width, spec.pad.unwrap_or(' '));
    }
    field
}

pub(crate) fn format_zone_offset(
    offset_seconds: i32,
    colons: usize,
    minimal: bool,
    pad: char,
    width: Option<usize>,
) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.abs();
    let hours = abs / 3600;
    let minutes = (abs % 3600) / 60;
    let seconds = abs % 60;
    let canonical_digits = if seconds != 0 {
        format!("{hours:02}{minutes:02}{seconds:02}")
    } else {
        format!("{hours:02}{minutes:02}")
    };
    let minimal_digits = strip_leading_zeros(&canonical_digits);
    let mut rendered = if colons == 1 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else if colons == 2 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else if colons == 3 {
        let body = if seconds != 0 {
            format!("{hours:02}:{minutes:02}:{seconds:02}")
        } else if minutes != 0 {
            format!("{hours:02}:{minutes:02}")
        } else {
            format!("{hours:02}")
        };
        format!("{sign}{body}")
    } else {
        let use_minimal =
            minimal || width.is_some_and(|target| target < canonical_digits.len() + 1);
        format!(
            "{sign}{}",
            if use_minimal {
                minimal_digits
            } else {
                canonical_digits
            }
        )
    };
    if let Some(target_width) = width
        && rendered.len() < target_width
    {
        let padding = target_width - rendered.len();
        rendered = if pad == ' ' {
            format!("{}{}", " ".repeat(padding), rendered)
        } else if colons == 0 {
            format!("{sign}{}{}", "0".repeat(padding), &rendered[1..])
        } else {
            format!("{}{}", "0".repeat(padding), rendered)
        };
    }
    rendered
}

pub(crate) fn weekday_name(datetime: &chrono::DateTime<FixedOffset>, abbrev: bool) -> String {
    let names = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];
    let n = chrono::Datelike::weekday(datetime).num_days_from_sunday() as usize;
    if abbrev {
        names[n][..3].to_string()
    } else {
        names[n].to_string()
    }
}

pub(crate) fn month_name(datetime: &chrono::DateTime<FixedOffset>, abbrev: bool) -> String {
    let names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let n = (chrono::Datelike::month(datetime) as usize) - 1;
    if abbrev {
        names[n][..3].to_string()
    } else {
        names[n].to_string()
    }
}

pub(crate) fn twelve_hour(hour: u32) -> u32 {
    let h = hour % 12;
    if h == 0 { 12 } else { h }
}

pub(crate) fn format_time_string_value(
    interp: &Interpreter,
    format: &str,
    time: &ExactTimeValue,
    zone: &ZoneSpec,
) -> Result<String, LispError> {
    let _ = interp;
    let (datetime, _) = time_local_datetime(time, zone)?;
    let picoseconds = fraction_picoseconds(time);
    let chars = format.chars().collect::<Vec<_>>();
    let mut result = String::new();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] != '%' {
            result.push(chars[index]);
            index += 1;
            continue;
        }
        let format_start = index;
        index += 1;
        if index >= chars.len() {
            break;
        }
        if chars[index] == '%' {
            result.push('%');
            index += 1;
            continue;
        }
        let spec = parse_time_format_spec(&chars, &mut index);
        if index >= chars.len() {
            result.extend(chars[format_start..].iter());
            break;
        }
        // Alternative locale representations collapse to the ordinary C
        // locale representation used by the compatibility harness.  Parsing
        // the modifier is nevertheless essential so E/O are not mistaken
        // for conversion characters.
        let _modifier = spec.modifier;
        let field = match spec.conversion {
            'Y' => year_time_field(datetime.year(), spec),
            'G' => year_time_field(datetime.iso_week().year(), spec),
            'g' => numeric_time_field(
                datetime.iso_week().year().rem_euclid(100) as i64,
                2,
                '0',
                spec,
            ),
            'F' => format!(
                "{:04}-{:02}-{:02}",
                datetime.year(),
                datetime.month(),
                datetime.day()
            ),
            'm' => numeric_time_field(datetime.month() as i64, 2, '0', spec),
            'd' => numeric_time_field(datetime.day() as i64, 2, '0', spec),
            'e' => numeric_time_field(datetime.day() as i64, 2, ' ', spec),
            'a' => weekday_name(&datetime, true),
            'A' => weekday_name(&datetime, false),
            'b' | 'h' => month_name(&datetime, true),
            'B' => month_name(&datetime, false),
            'y' => numeric_time_field(datetime.year().rem_euclid(100) as i64, 2, '0', spec),
            'C' => numeric_time_field(datetime.year().div_euclid(100) as i64, 2, '0', spec),
            'j' => numeric_time_field(chrono::Datelike::ordinal(&datetime) as i64, 3, '0', spec),
            'u' => {
                let n = chrono::Datelike::weekday(&datetime).num_days_from_sunday();
                numeric_time_field(if n == 0 { 7 } else { n } as i64, 1, '0', spec)
            }
            'w' => numeric_time_field(
                chrono::Datelike::weekday(&datetime).num_days_from_sunday() as i64,
                1,
                '0',
                spec,
            ),
            'U' => {
                let week =
                    (datetime.ordinal0() + 7 - datetime.weekday().num_days_from_sunday()) / 7;
                numeric_time_field(week as i64, 2, '0', spec)
            }
            'W' => {
                let week =
                    (datetime.ordinal0() + 7 - datetime.weekday().num_days_from_monday()) / 7;
                numeric_time_field(week as i64, 2, '0', spec)
            }
            'V' => numeric_time_field(datetime.iso_week().week() as i64, 2, '0', spec),
            'q' => numeric_time_field(((datetime.month() - 1) / 3 + 1) as i64, 1, '0', spec),
            'D' => format!(
                "{:02}/{:02}/{:02}",
                datetime.month(),
                datetime.day(),
                datetime.year().rem_euclid(100)
            ),
            'x' => format!(
                "{:02}/{:02}/{:02}",
                datetime.month(),
                datetime.day(),
                datetime.year().rem_euclid(100)
            ),
            'X' => format!(
                "{:02}:{:02}:{:02}",
                datetime.hour(),
                datetime.minute(),
                datetime.second()
            ),
            'R' => format!("{:02}:{:02}", datetime.hour(), datetime.minute()),
            'I' => numeric_time_field(twelve_hour(datetime.hour()) as i64, 2, '0', spec),
            'l' => numeric_time_field(twelve_hour(datetime.hour()) as i64, 2, ' ', spec),
            'p' => if datetime.hour() < 12 { "AM" } else { "PM" }.to_string(),
            'P' => if datetime.hour() < 12 { "am" } else { "pm" }.to_string(),
            'r' => format!(
                "{:02}:{:02}:{:02} {}",
                twelve_hour(datetime.hour()),
                datetime.minute(),
                datetime.second(),
                if datetime.hour() < 12 { "AM" } else { "PM" }
            ),
            'c' => format!(
                "{} {} {:2} {:02}:{:02}:{:02} {}",
                weekday_name(&datetime, true),
                month_name(&datetime, true),
                datetime.day(),
                datetime.hour(),
                datetime.minute(),
                datetime.second(),
                datetime.year()
            ),
            's' => {
                let (whole_seconds, _) = time_floor_parts(time);
                whole_seconds.to_string()
            }
            'n' => "\n".to_string(),
            't' => "\t".to_string(),
            'H' => numeric_time_field(datetime.hour() as i64, 2, '0', spec),
            'k' => numeric_time_field(datetime.hour() as i64, 2, ' ', spec),
            'M' => numeric_time_field(datetime.minute() as i64, 2, '0', spec),
            'S' => numeric_time_field(datetime.second() as i64, 2, '0', spec),
            'T' => format!(
                "{:02}:{:02}:{:02}",
                datetime.hour(),
                datetime.minute(),
                datetime.second()
            ),
            'Z' => zone.abbreviation.clone(),
            'z' => format_zone_offset(
                zone.offset_seconds,
                spec.colons,
                spec.minimal,
                spec.pad.unwrap_or('0'),
                spec.width,
            ),
            'N' => {
                let width = spec.width.unwrap_or(9);
                let digits = format_fraction_digits(&picoseconds, width);
                if spec.minimal {
                    trim_trailing_zeros(&digits)
                } else if spec.pad == Some(' ') {
                    let trimmed = trim_trailing_zeros(&digits);
                    format!(
                        "{}{}",
                        trimmed,
                        " ".repeat(width.saturating_sub(trimmed.len()))
                    )
                } else {
                    digits
                }
            }
            _ => {
                result.extend(chars[format_start..=index].iter());
                index += 1;
                continue;
            }
        };
        result.push_str(&present_time_field(field, spec));
        index += 1;
    }
    Ok(result)
}

pub(crate) fn decode_time_value(
    time: &ExactTimeValue,
    zone: &ZoneSpec,
    form: &Value,
) -> Result<Value, LispError> {
    let (datetime, fractional_ticks) = time_local_datetime(time, zone)?;
    let seconds = exact_time_value(
        BigInt::from(datetime.second()) * time.hz.clone() + fractional_ticks,
        time.hz.clone(),
    )?;
    let second_field = match form {
        Value::Symbol(symbol) if symbol == "integer" => exact_time_floor_integer_value(&seconds),
        Value::T => exact_time_to_tick_pair(&seconds),
        _ => exact_time_to_value(&seconds),
    };
    Ok(Value::list([
        second_field,
        Value::Integer(datetime.minute() as i64),
        Value::Integer(datetime.hour() as i64),
        Value::Integer(datetime.day() as i64),
        Value::Integer(datetime.month() as i64),
        Value::Integer(datetime.year() as i64),
        Value::Integer(datetime.weekday().num_days_from_sunday() as i64),
        if zone.is_dst { Value::T } else { Value::Nil },
        Value::Integer(zone.offset_seconds as i64),
    ]))
}

pub(crate) fn decoded_seconds_value(
    interp: &Interpreter,
    value: &Value,
) -> Result<ExactTimeValue, LispError> {
    exact_time_from_value(
        interp,
        value,
        &ExactTimeValue {
            ticks: BigInt::zero(),
            hz: BigInt::from(1u8),
        },
    )
}

pub(crate) fn integer_field(interp: &Interpreter, value: &Value) -> Result<i32, LispError> {
    integer_like_bigint(interp, value)?
        .to_i32()
        .ok_or_else(|| LispError::WrongTypeArgument("integerp".into(), value.clone()))
}

pub(crate) fn value_is_unspecified(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Nil) => true,
        Some(Value::Symbol(symbol)) if symbol == "-" => true,
        _ => false,
    }
}

pub(crate) fn time_convert_value(time: &ExactTimeValue, form: &Value) -> Result<Value, LispError> {
    match form {
        Value::Nil => Ok(exact_time_to_value(time)),
        Value::T => Ok(exact_time_to_tick_pair(time)),
        Value::Symbol(symbol) if symbol == "integer" => Ok(exact_time_floor_integer_value(time)),
        Value::Symbol(symbol) if symbol == "list" => exact_time_to_old_style(time),
        Value::Integer(value) if *value == 4 => exact_time_to_old_style(time),
        Value::BigInteger(value) if value == &BigInt::from(4u8) => exact_time_to_old_style(time),
        Value::Integer(value) => exact_time_to_scaled_pair(time, &BigInt::from(*value)),
        Value::BigInteger(value) => exact_time_to_scaled_pair(time, value),
        _ => Err(LispError::TypeError(
            "time-convert form".into(),
            form.type_name(),
        )),
    }
}

pub(crate) fn call_time_builtin(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    call_time_builtin_at(interp, name, args, env, current_time_value()?)
}

define_dispatch!(
    pub(crate) fn call_time_builtin_at(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        _env: &mut Env,
        now: ExactTimeValue,
    ) -> Result<Value, LispError> {
        match name {
            "set-time-zone-rule" => {
                need_args(name, args, 1)?;
                let rule = if args[0].is_nil() {
                    Value::Symbol("wall".into())
                } else {
                    args[0].clone()
                };
                if !local_time_zone_rule_is_wall(&rule) {
                    let _ = explicit_zone_spec_from_value(&rule, Some(&now))?;
                }
                interp.local_time_zone_rule = rule;
                Ok(Value::Nil)
            }
            "current-time" => {
                need_arg_range(name, args, 0, 0)?;
                Ok(exact_time_to_value(&now))
            }
            "current-time-string" => {
                need_arg_range(name, args, 0, 2)?;
                let time =
                    exact_time_from_value(interp, args.first().unwrap_or(&Value::Nil), &now)?;
                let zone =
                    zone_spec_from_value(interp, args.get(1).unwrap_or(&Value::Nil), Some(&time))?;
                let (datetime, _) = time_local_datetime(&time, &zone)?;
                Ok(string_like_value_with_multibyte(
                    datetime.format("%a %b %e %H:%M:%S %Y").to_string(),
                    Vec::new(),
                    false,
                ))
            }
            "time-add" | "time-subtract" => {
                need_args(name, args, 2)?;
                let left = exact_time_from_value(interp, &args[0], &now)?;
                let right = exact_time_from_value(interp, &args[1], &now)?;
                let subtract = name == "time-subtract";
                // GNU time_arith: with equal clocks the ticks combine directly
                // and HZ is preserved (no fraction reduction); with different
                // clocks the result is LO/HI with LO = db2*na OP da2*nb and
                // HI = da2*db2*g (g = gcd of the clocks), reduced only by
                // gcd(LO, da2*db2) — timer-tests compares the cons with `equal'.
                let result = if left.hz == right.hz {
                    let ticks = if subtract {
                        left.ticks.clone() - &right.ticks
                    } else {
                        left.ticks.clone() + &right.ticks
                    };
                    ExactTimeValue {
                        ticks,
                        hz: left.hz.clone(),
                    }
                } else {
                    let g = bigint_gcd(left.hz.clone(), right.hz.clone());
                    let da2 = left.hz.clone() / &g;
                    let db2 = right.hz.clone() / &g;
                    let lo = if subtract {
                        db2.clone() * &left.ticks - da2.clone() * &right.ticks
                    } else {
                        db2.clone() * &left.ticks + da2.clone() * &right.ticks
                    };
                    let hi = da2.clone() * &db2 * &g;
                    let g2 = bigint_gcd(lo.clone(), da2 * db2);
                    ExactTimeValue {
                        ticks: lo / &g2,
                        hz: hi / g2,
                    }
                };
                if result.hz <= BigInt::zero() {
                    return Err(LispError::Signal("Invalid time resolution".into()));
                }
                Ok(exact_time_to_value(&result))
            }
            "time-equal-p" => {
                need_args(name, args, 2)?;
                let left = exact_time_from_value(interp, &args[0], &now)?;
                let right = exact_time_from_value(interp, &args[1], &now)?;
                Ok(if exact_time_equal(&left, &right) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "time-less-p" => {
                need_args(name, args, 2)?;
                let left = exact_time_from_value(interp, &args[0], &now)?;
                let right = exact_time_from_value(interp, &args[1], &now)?;
                Ok(if exact_time_less(&left, &right) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "float-time" => {
                need_arg_range(name, args, 0, 1)?;
                let value = args.first().unwrap_or(&Value::Nil);
                Ok(Value::Float(exact_time_to_f64(&exact_time_from_value(
                    interp, value, &now,
                )?)))
            }
            "time-convert" => {
                need_arg_range(name, args, 1, 2)?;
                let time = exact_time_from_value(interp, &args[0], &now)?;
                let form = args.get(1).unwrap_or(&Value::Nil);
                time_convert_value(&time, form)
            }
            "decode-time" => {
                need_arg_range(name, args, 0, 3)?;
                let time =
                    exact_time_from_value(interp, args.first().unwrap_or(&Value::Nil), &now)?;
                let zone =
                    zone_spec_from_value(interp, args.get(1).unwrap_or(&Value::Nil), Some(&time))?;
                let form = args.get(2).unwrap_or(&Value::Nil);
                decode_time_value(&time, &zone, form)
            }
            "encode-time" => {
                need_arg_range(name, args, 1, 9)?;
                let list_form = args.len() == 1;
                let fields = if list_form {
                    args[0].to_vec()?
                } else {
                    args.to_vec()
                };
                if fields.len() < 6 || fields.len() > 9 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), fields.len()));
                }
                let seconds = decoded_seconds_value(interp, &fields[0])?;
                let (whole_seconds, fractional_ticks) = time_floor_parts(&seconds);
                let second = whole_seconds
                    .to_i64()
                    .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
                if !(0..=59).contains(&second) {
                    return Err(LispError::Signal("Invalid decoded time".into()));
                }
                let minute = integer_field(interp, &fields[1])?;
                let hour = integer_field(interp, &fields[2])?;
                let day = integer_field(interp, &fields[3])?;
                let month = integer_field(interp, &fields[4])?;
                let year = integer_field(interp, &fields[5])?;
                let time = normalize_decoded_civil_time(year, month, day, hour, minute, second)?;
                let zone_value = if list_form {
                    fields.get(8)
                } else {
                    fields.get(6..).and_then(|extra| extra.last())
                };
                let zone = if value_is_unspecified(zone_value) {
                    local_zone_spec_for_civil(
                        interp,
                        time.year(),
                        time.month(),
                        time.day(),
                        time.hour(),
                        time.minute(),
                        time.second(),
                    )
                } else if let Some(zone_text) = zone_value.filter(|value| value.is_string()) {
                    let text = string_text(zone_text)?;
                    if let Some(posix) = parse_posix_tz(&text) {
                        posix.zone_for_civil(
                            time.year(),
                            time.month(),
                            time.day(),
                            time.hour(),
                            time.minute(),
                            time.second(),
                        )
                    } else {
                        zone_spec_from_value(interp, zone_text, None)?
                    }
                } else {
                    zone_spec_from_value(interp, zone_value.unwrap_or(&Value::Nil), None)?
                };
                let offset = zone_offset(&zone)?;
                let local = offset
                    .from_local_datetime(&time)
                    .single()
                    .ok_or_else(|| LispError::Signal("Invalid decoded time".into()))?;
                Ok(exact_time_to_value(&exact_time_value(
                    BigInt::from(local.timestamp()) * seconds.hz.clone() + fractional_ticks,
                    seconds.hz,
                )?))
            }
            "format-time-string" => {
                need_arg_range(name, args, 1, 3)?;
                let format = string_text(&args[0])?;
                let time = exact_time_from_value(interp, args.get(1).unwrap_or(&Value::Nil), &now)?;
                let zone =
                    zone_spec_from_value(interp, args.get(2).unwrap_or(&Value::Nil), Some(&time))?;
                Ok(string_like_value_with_multibyte(
                    format_time_string_value(interp, &format, &time, &zone)?,
                    Vec::new(),
                    true,
                ))
            }
            "current-time-zone" => {
                need_arg_range(name, args, 0, 2)?;
                let time = if let Some(value) = args.first().filter(|value| !value.is_nil()) {
                    Some(exact_time_from_value(interp, value, &now)?)
                } else {
                    None
                };
                let zone = if let Some(zone_value) = args.get(1).filter(|value| !value.is_nil()) {
                    zone_spec_from_value(interp, zone_value, time.as_ref())?
                } else {
                    local_zone_spec(interp, time.as_ref())
                };
                Ok(Value::list([
                    Value::Integer(zone.offset_seconds as i64),
                    Value::String(zone.abbreviation.into()),
                ]))
            }
        }
    }
);

pub(crate) fn numeric_lt(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    Ok(matches!(
        numeric_ordering(interp, left, right)?,
        Some(Ordering::Less)
    ))
}

pub(crate) fn numeric_eq(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    Ok(matches!(
        numeric_ordering(interp, left, right)?,
        Some(Ordering::Equal)
    ))
}

pub(crate) fn numeric_gt(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    Ok(matches!(
        numeric_ordering(interp, left, right)?,
        Some(Ordering::Greater)
    ))
}

pub(crate) fn numeric_lte(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    Ok(matches!(
        numeric_ordering(interp, left, right)?,
        Some(Ordering::Less | Ordering::Equal)
    ))
}

pub(crate) fn numeric_gte(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    Ok(matches!(
        numeric_ordering(interp, left, right)?,
        Some(Ordering::Greater | Ordering::Equal)
    ))
}

pub(crate) fn numeric_ordering(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<Option<Ordering>, LispError> {
    // GNU's arithcompare family (<, <=, =, >=, >) accepts numbers and
    // markers and reports that public union type for every rejected operand.
    // The lower-level numeric converters are also used by number-only
    // arithmetic, so keep the comparison contract here.
    for value in [left, right] {
        if !matches!(
            value,
            Value::Integer(_) | Value::BigInteger(_) | Value::Float(_) | Value::Marker(_)
        ) {
            return Err(LispError::WrongTypeArgument(
                "number-or-marker-p".into(),
                value.clone(),
            ));
        }
    }
    if matches!(left, Value::Float(value) if value.is_nan())
        || matches!(right, Value::Float(value) if value.is_nan())
    {
        return Ok(None);
    }

    // GNU's arithcompare handles two fixnums directly. Markers coerce to
    // fixnums at the same boundary, so keep this overwhelmingly common path
    // allocation-free and reserve exact-rational construction for mixed or
    // genuinely large representations.
    if matches!(left, Value::Integer(_) | Value::Marker(_))
        && matches!(right, Value::Integer(_) | Value::Marker(_))
    {
        return Ok(Some(
            integer_like_i64(interp, left)?.cmp(&integer_like_i64(interp, right)?),
        ));
    }

    if let (Some(left_exact), Some(right_exact)) = (
        exact_binary_rational(interp, left)?,
        exact_binary_rational(interp, right)?,
    ) {
        return Ok(Some(compare_exact_binary_rationals(
            left_exact,
            right_exact,
        )));
    }

    match (left, right) {
        (Value::Float(left), Value::Float(right)) => Ok(left.partial_cmp(right)),
        (Value::Float(left), _) if left.is_infinite() => Ok(Some(if left.is_sign_positive() {
            Ordering::Greater
        } else {
            Ordering::Less
        })),
        (_, Value::Float(right)) if right.is_infinite() => Ok(Some(if right.is_sign_positive() {
            Ordering::Less
        } else {
            Ordering::Greater
        })),
        _ => Ok(numeric_to_f64(interp, left)?.partial_cmp(&numeric_to_f64(interp, right)?)),
    }
}

pub(crate) fn compare_exact_binary_rationals(
    left: (BigInt, i32),
    right: (BigInt, i32),
) -> Ordering {
    let (left_sig, left_exp) = left;
    let (right_sig, right_exp) = right;
    let exponent_delta = left_exp - right_exp;
    if exponent_delta >= 0 {
        (left_sig << exponent_delta as usize).cmp(&right_sig)
    } else {
        left_sig.cmp(&(right_sig << (-exponent_delta) as usize))
    }
}

pub(crate) fn extremum_numeric_value(
    interp: &Interpreter,
    args: &[Value],
    choose_max: bool,
) -> Result<Value, LispError> {
    let mut best = numeric_result_value(interp, &args[0])?;
    if matches!(best, Value::Float(value) if value.is_nan()) {
        return Ok(best);
    }

    for arg in &args[1..] {
        let candidate = numeric_result_value(interp, arg)?;
        if matches!(candidate, Value::Float(value) if value.is_nan()) {
            return Ok(candidate);
        }
        let ordering = numeric_ordering(interp, &best, &candidate)?;
        if (choose_max && matches!(ordering, Some(Ordering::Less)))
            || (!choose_max && matches!(ordering, Some(Ordering::Greater)))
        {
            best = candidate;
        }
    }

    Ok(best)
}

pub(crate) fn numeric_result_value(
    interp: &Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    match value {
        Value::Integer(number) => Ok(Value::Integer(*number)),
        Value::BigInteger(number) => Ok(normalize_bigint_value(number.clone().into())),
        Value::Float(number) => Ok(Value::Float(*number)),
        Value::Marker(id) => Ok(Value::Integer(interp.marker_position(*id).ok_or_else(|| {
            LispError::WrongTypeArgument("number-or-marker-p".into(), value.clone())
        })? as i64)),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            value.clone(),
        )),
    }
}

pub(crate) fn parse_string_to_number_value(
    text: &str,
    base: Option<i64>,
) -> Result<Value, LispError> {
    match base.unwrap_or(10) {
        10 => Ok(parse_decimal_string_to_number(text)),
        base if (2..=16).contains(&base) => Ok(parse_integer_string_with_base(text, base as u32)),
        _ => Err(LispError::Signal("Args out of range".into())),
    }
}

pub(crate) fn parse_decimal_string_to_number(text: &str) -> Value {
    let text = text.trim_start_matches([' ', '\t']);
    let Some(prefix) = decimal_number_prefix(text) else {
        return Value::Integer(0);
    };
    if prefix.contains(['.', 'e', 'E']) {
        if let Some(value) = crate::lisp::reader::parse_special_float_token(prefix) {
            return Value::Float(value);
        }
        if prefix.ends_with('.')
            && !prefix.contains(['e', 'E'])
            && prefix
                .trim_end_matches('.')
                .chars()
                .filter(|ch| !matches!(ch, '+' | '-'))
                .all(|ch| ch.is_ascii_digit())
        {
            let integer = prefix.trim_end_matches('.');
            if let Ok(value) = integer.parse::<i64>() {
                return Value::Integer(value);
            }
            if let Ok(value) = integer.parse::<BigInt>() {
                return normalize_bigint_value(value);
            }
            return Value::Integer(0);
        }
        prefix
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or(Value::Integer(0))
    } else if let Ok(value) = prefix.parse::<i64>() {
        Value::Integer(value)
    } else if let Ok(value) = prefix.parse::<BigInt>() {
        normalize_bigint_value(value)
    } else {
        Value::Integer(0)
    }
}

pub(crate) fn decimal_number_prefix(text: &str) -> Option<&str> {
    let mut index = 0usize;
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    if let Some((_, sign)) = chars.get(index)
        && matches!(sign, '+' | '-')
    {
        index += 1;
    }

    let integer_start = index;
    while let Some((_, ch)) = chars.get(index)
        && ch.is_ascii_digit()
    {
        index += 1;
    }
    let integer_digits = index - integer_start;

    let mut fractional_digits = 0usize;
    if let Some((_, '.')) = chars.get(index) {
        index += 1;
        let fraction_start = index;
        while let Some((_, ch)) = chars.get(index)
            && ch.is_ascii_digit()
        {
            index += 1;
        }
        fractional_digits = index - fraction_start;
    }

    if integer_digits == 0 && fractional_digits == 0 {
        return None;
    }

    let mut end = index;
    if matches!(chars.get(index), Some((_, 'e' | 'E'))) {
        let exponent_marker = index;
        index += 1;
        let mut positive_special = false;
        if let Some((_, sign)) = chars.get(index)
            && matches!(sign, '+' | '-')
        {
            positive_special = *sign == '+';
            index += 1;
        }
        let exponent_start = index;
        let special_exponent = positive_special
            && chars
                .get(index..index + 3)
                .map(|suffix| {
                    suffix
                        .iter()
                        .map(|(_, ch)| ch.to_ascii_uppercase())
                        .collect::<String>()
                })
                .is_some_and(|suffix| matches!(suffix.as_str(), "INF" | "NAN"));
        if special_exponent {
            index += 3;
            end = index;
        } else {
            while let Some((_, ch)) = chars.get(index)
                && ch.is_ascii_digit()
            {
                index += 1;
            }
            if index > exponent_start {
                end = index;
            } else {
                end = exponent_marker;
            }
        }
    }

    Some(&text[..chars.get(end).map_or(text.len(), |(offset, _)| *offset)])
}

pub(crate) fn parse_integer_string_with_base(text: &str, base: u32) -> Value {
    let text = text.trim_start_matches([' ', '\t']);
    let mut chars = text.chars().peekable();
    let negative = match chars.peek() {
        Some('+') => {
            chars.next();
            false
        }
        Some('-') => {
            chars.next();
            true
        }
        _ => false,
    };

    let mut value = BigInt::zero();
    let mut saw_digit = false;
    while let Some(&ch) = chars.peek() {
        let Some(digit) = digit_value_for_base(ch, base) else {
            break;
        };
        saw_digit = true;
        value = value * base + BigInt::from(digit);
        chars.next();
    }

    if !saw_digit {
        return Value::Integer(0);
    }
    if negative {
        value = -value;
    }
    normalize_bigint_value(value)
}

pub(crate) fn digit_value_for_base(ch: char, base: u32) -> Option<u32> {
    let digit = match ch {
        '0'..='9' => ch as u32 - '0' as u32,
        'a'..='z' => 10 + (ch as u32 - 'a' as u32),
        'A'..='Z' => 10 + (ch as u32 - 'A' as u32),
        _ => return None,
    };
    (digit < base).then_some(digit)
}

pub(crate) fn number_to_string(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Integer(n) => Ok(n.to_string()),
        Value::BigInteger(n) => Ok(n.to_string()),
        Value::Float(f) => Ok(crate::lisp::types::format_float(*f)),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            value.clone(),
        )),
    }
}
