use super::tz::{TimeZone, Tm, localtime_rz, mktime_z};
use super::*;
use crate::lisp::types::Kind;
use std::rc::Rc;

pub(crate) fn wait_duration(args: &[Value]) -> Result<Duration, LispError> {
    let seconds = args
        .first()
        .map(Value::as_float)
        .transpose()?
        .unwrap_or(0.0);
    let millis = match args.get(1).map(|v| v.kind()) {
        None | Some(Kind::Nil) => 0.0,
        Some(value) => value.value().as_float()?,
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

pub(crate) const MOST_POSITIVE_FIXNUM: i64 = (1_i64 << 61) - 1;
pub(crate) const MOST_NEGATIVE_FIXNUM: i64 = -(1_i64 << 61);

/// GNU lisp.h's `make_int`: keep values in the immediate fixnum range and
/// represent every other mathematical integer as a bignum.
#[inline]
pub(crate) fn normalize_integer_value(value: i64) -> Value {
    if (MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM).contains(&value) {
        Value::Integer(value)
    } else {
        Value::BigInteger(BigInt::from(value).into())
    }
}

pub(crate) fn normalize_bigint_value(value: BigInt) -> Value {
    match value.to_i64() {
        Some(value) if (MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM).contains(&value) => {
            Value::Integer(value)
        }
        _ => Value::BigInteger(value.into()),
    }
}

/// fns.c:Fstring_version_lessp passes the actual bytes and lengths to
/// GNU lib/filevercmp.c:filenvercmp (LGPL-3.0-or-later). Follow its empty
/// and dot-name ordering, suffix passes and Debian digit-run comparison.
/// Leading zeros are ignored; an equal version has no length tie-break.
pub(crate) fn string_version_compare(left: &[u8], right: &[u8]) -> Ordering {
    fn prefix_len(bytes: &[u8]) -> usize {
        let mut index = 0;
        let mut prefix = 0;
        while index < bytes.len() {
            index += 1;
            prefix = index;
            while index + 1 < bytes.len()
                && bytes[index] == b'.'
                && (bytes[index + 1].is_ascii_alphabetic() || bytes[index + 1] == b'~')
            {
                index += 2;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'~')
                {
                    index += 1;
                }
            }
        }
        prefix
    }

    fn order(byte: Option<u8>) -> i16 {
        match byte {
            None => -1,
            Some(b'~') => -2,
            Some(byte) if byte.is_ascii_digit() => 0,
            Some(byte) if byte.is_ascii_alphabetic() => i16::from(byte),
            Some(byte) => i16::from(byte) + 256,
        }
    }

    fn revision_compare(left: &[u8], right: &[u8]) -> Ordering {
        let (mut a, mut b) = (0, 0);
        while a < left.len() || b < right.len() {
            while left.get(a).is_some_and(|byte| !byte.is_ascii_digit())
                || right.get(b).is_some_and(|byte| !byte.is_ascii_digit())
            {
                let result = order(left.get(a).copied()).cmp(&order(right.get(b).copied()));
                if result != Ordering::Equal {
                    return result;
                }
                a += 1;
                b += 1;
            }
            while left.get(a) == Some(&b'0') {
                a += 1;
            }
            while right.get(b) == Some(&b'0') {
                b += 1;
            }
            let mut first_difference = Ordering::Equal;
            while left.get(a).is_some_and(u8::is_ascii_digit)
                && right.get(b).is_some_and(u8::is_ascii_digit)
            {
                if first_difference == Ordering::Equal {
                    first_difference = left[a].cmp(&right[b]);
                }
                a += 1;
                b += 1;
            }
            if left.get(a).is_some_and(u8::is_ascii_digit) {
                return Ordering::Greater;
            }
            if right.get(b).is_some_and(u8::is_ascii_digit) {
                return Ordering::Less;
            }
            if first_difference != Ordering::Equal {
                return first_difference;
            }
        }
        Ordering::Equal
    }

    if left.is_empty() || right.is_empty() {
        return left.len().cmp(&right.len());
    }
    if left[0] == b'.' || right[0] == b'.' {
        let dotted = (right[0] == b'.').cmp(&(left[0] == b'.'));
        if dotted != Ordering::Equal {
            return dotted;
        }
        for special in [b".".as_slice(), b"..".as_slice()] {
            if left == special || right == special {
                return (right == special).cmp(&(left == special));
            }
        }
    }
    let a = prefix_len(left);
    let b = prefix_len(right);
    let result = revision_compare(&left[..a], &right[..b]);
    if result != Ordering::Equal || (a == left.len() && b == right.len()) {
        result
    } else {
        revision_compare(left, right)
    }
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
    // The host's contracted C inventory owns dispatch (dispatch.rs), so it
    // also answers `subr-arity' for primitives the source-tree table lacks:
    // that table is regenerated from the Darwin oracle and has no inotify
    // entries, which made every macroexpansion touching `inotify-valid-p'
    // fail on Linux before any test in inotify-tests.el could run.
    host_c_primitive_arity(name)
        .or_else(|| generated_builtin_arities::generated_builtin_arity(name))
        .map(arity_value)
}

fn host_c_primitive_arity(name: &str) -> Option<(i64, i64)> {
    let contracts = crate::lisp::primitives::GNU_C_PRIMITIVES;
    contracts
        .binary_search_by_key(&name, |contract| contract.name)
        .ok()
        .and_then(|index| contracts[index].arity)
}

pub(crate) fn special_form_arity_value(name: &str) -> Option<Value> {
    is_special_form_name(name)
        .then(|| generated_builtin_arities::generated_builtin_arity(name))
        .flatten()
        .map(arity_value)
}

pub(crate) fn lambda_arity_value(params: &[impl AsRef<str>]) -> Value {
    let mut required = 0i64;
    let mut optional = 0i64;
    let mut in_optional = false;
    let mut has_rest = false;

    for param in params {
        match param.as_ref() {
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
        *function,
    ]))
}

/// Return the bare symbol GNU's `XSYMBOL` addresses after `SYMBOLP`.
/// `SYMBOLP` includes nil and t, and includes symbol-with-position objects
/// only while `symbols_with_pos_enabled` is active (`lisp.h`).
fn arity_bare_symbol(
    interp: &Interpreter,
    value: &Value,
    positions_enabled: bool,
) -> Option<Value> {
    if value.is_symbol() {
        return Some(*value);
    }
    positions_enabled
        .then(|| symbol_with_pos_parts(interp, value))
        .flatten()
        .map(|(symbol, _)| symbol)
}

/// Decode GNU's CLOSURE_ARGLIST using eval.c:lambda_arity and
/// bytecode.c:get_byte_code_arity.
fn closure_arity_value(
    interp: &Interpreter,
    argument_spec: &Value,
    function: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    if let Kind::Integer(packed) = argument_spec.kind() {
        if packed < 0 {
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
    let positions_enabled = symbols_with_pos_enabled(interp, env);
    let mut cursor = *argument_spec;
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        let Kind::Cons(cell) = cursor.kind() else {
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
        let parameter = cell.car.get();
        cursor = cell.cdr.get();
        let Some(parameter) = arity_bare_symbol(interp, &parameter, positions_enabled) else {
            return Err(invalid_function_arity(function));
        };
        match parameter.as_symbol().expect("SYMBOLP established a symbol") {
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

/// eval.c:Ffunc_arity of FUNCTION, reached from ORIGINAL, the object
/// an `invalid-function' error names.
pub(crate) fn function_arity_value(
    interp: &Interpreter,
    function: &Value,
    original: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let positions_enabled = symbols_with_pos_enabled(interp, env);
    if let Some(symbol) = arity_bare_symbol(interp, function, positions_enabled) {
        if symbol.is_nil() {
            return Err(LispError::VoidFunction("nil".into()));
        }
        let name = symbol.as_symbol().expect("SYMBOLP established a symbol");
        if let Some(arity) = special_form_arity_value(name) {
            return Ok(arity);
        }
        let resolved = interp.lookup_function(name, env)?;
        return function_arity_value(interp, &resolved, original, env);
    }

    match function.kind() {
        Kind::BuiltinFunc(subr) => Ok(subr.arity_value()),
        Kind::Closure(lambda) => closure_arity_value(interp, &lambda.parameters(), function, env),
        Kind::NativeFunction(function) => Ok(function.arity_value()),
        Kind::Record(id) if interp.modules.functions.contains_key(&id.id) => {
            let function = &interp.modules.functions[&id.id];
            Ok(Value::cons(
                Value::Integer(function.min as i64),
                if function.max == -2 {
                    Value::symbol("many")
                } else {
                    Value::Integer(function.max as i64)
                },
            ))
        }
        value if is_lambda_expression(interp, &value.value(), env) => {
            let items = value.value().to_vec()?;
            let parameters = items[1]
                .to_vec()?
                .into_iter()
                .map(|parameter| {
                    arity_bare_symbol(interp, &parameter, positions_enabled)
                        .and_then(|symbol| symbol.symbol_str().map(str::to_string))
                        .ok_or_else(|| invalid_function_arity(function))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(lambda_arity_value(&parameters))
        }
        _ => Err(invalid_function_arity(original)),
    }
}

/// marker.c:marker_position's error for a marker that points nowhere,
/// which every coercion of a marker to its position (CHECK_*_COERCE_MARKER,
/// fix_position) signals.
pub(crate) fn marker_points_nowhere() -> LispError {
    LispError::Signal("Marker does not point anywhere".into())
}

pub(crate) fn integer_like_i64(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(n),
        Kind::Marker(id) => interp
            .marker_position(id)
            .map(|pos| pos as i64)
            .ok_or_else(marker_points_nowhere),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            *value,
        )),
    }
}

pub(crate) fn integer_like_bigint(
    interp: &Interpreter,
    value: &Value,
) -> Result<BigInt, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(BigInt::from(n)),
        Kind::BigInteger(n) => Ok((n).into()),
        Kind::Marker(id) => interp
            .marker_position(id)
            .map(BigInt::from)
            .ok_or_else(marker_points_nowhere),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            *value,
        )),
    }
}

/// lisp.h:CHECK_NUMBER: an integer or a float, no marker.
pub(crate) fn check_number(value: &Value) -> Result<(), LispError> {
    match value.kind() {
        Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_) => Ok(()),
        _ => Err(LispError::WrongTypeArgument("numberp".into(), *value)),
    }
}

/// lisp.h:CHECK_CHARACTER: a fixnum naming a character (0..MAX_CHAR).
pub(crate) fn check_character(value: &Value) -> Result<u32, LispError> {
    match value.kind() {
        Kind::Integer(code) if (0..=0x3F_FFFF).contains(&code) => Ok(code as u32),
        _ => Err(LispError::WrongTypeArgument("characterp".into(), *value)),
    }
}

/// lisp.h:CHECK_FIXNAT: a non-negative fixnum, any other object
/// `wholenump'.
pub(crate) fn check_fixnat(value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) if n >= 0 => Ok(n),
        _ => Err(LispError::WrongTypeArgument("wholenump".into(), *value)),
    }
}

/// floatfns.c:extract_float: CHECK_NUMBER, then the number as a double.
pub(crate) fn extract_float(interp: &Interpreter, value: &Value) -> Result<f64, LispError> {
    check_number(value)?;
    numeric_to_f64(interp, value)
}

/// floatfns.c:CHECK_FLOAT: a float's value, any other object `floatp'.
pub(crate) fn check_float(value: &Value) -> Result<f64, LispError> {
    match value.kind() {
        Kind::Float(value) => Ok(value.get()),
        _ => Err(LispError::WrongTypeArgument("floatp".into(), *value)),
    }
}

pub(crate) fn numeric_to_f64(interp: &Interpreter, value: &Value) -> Result<f64, LispError> {
    match value.kind() {
        Kind::Float(f) => Ok(f.get()),
        Kind::BigInteger(n) => n
            .to_f64()
            .ok_or_else(|| LispError::WrongTypeArgument("number-or-marker-p".into(), *value)),
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
        return Err(LispError::SignalValue(Value::list([Value::symbol(
            "overflow-error",
        )])));
    }
    Ok(round_float(kind, value))
}

fn round_float(kind: RoundingKind, value: f64) -> f64 {
    match kind {
        RoundingKind::Ceiling => value.ceil(),
        RoundingKind::Floor => value.floor(),
        RoundingKind::Round => value.round_ties_even(),
        RoundingKind::Truncate => value.trunc(),
    }
}

pub(crate) fn integer_rounding_value(
    interp: &Interpreter,
    kind: RoundingKind,
    args: &[Value],
    float_result: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 || (float_result && args.len() != 1) {
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
    if float_result && !matches!(args[0].kind(), Kind::Float(_)) {
        return Err(LispError::WrongTypeArgument("floatp".into(), args[0]));
    }
    // floatfns.c:rounding_driver: CHECK_NUMBER (n), and CHECK_NUMBER (d)
    // for a divisor that is not nil.
    check_number(&args[0])?;
    if let Some(divisor) = args.get(1).filter(|divisor| !divisor.is_nil()) {
        check_number(divisor)?;
    }
    // rounding_driver divides a fixnum by a nonzero fixnum specially, for
    // speed.
    if let [numerator, divisor] = args
        && let (Some(numerator), Some(divisor)) = (numerator.fixnum(), divisor.fixnum())
        && divisor != 0
    {
        return Ok(Value::Integer(fixnum_divide(kind, numerator, divisor)));
    }

    // floatfns.c:rounding_driver receives a fixed second C argument and uses
    // NILP (d), so funcall_subr's padded Qnil and an explicit nil have the
    // same no-divisor behavior.
    if args.len() == 1 || (!float_result && args[1].is_nil()) {
        if float_result {
            // GNU's f* family returns an integral float and deliberately
            // preserves infinities and NaNs.  Only the integer-returning
            // family signals `overflow-error' for non-finite input.
            return Ok(Value::float(round_float(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )));
        }
        return match args[0].kind() {
            Kind::Integer(_) | Kind::BigInteger(_) => Ok(args[0]),
            _ => rounded_f64_to_number_value(apply_rounding_kind(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )?),
        };
    }

    if let Some(rounded) = exact_numeric_division_round(interp, kind, &args[0], &args[1])? {
        return if float_result {
            Ok(Value::float(numeric_to_f64(interp, &rounded)?))
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
            Ok(Value::float(
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
        Ok(Value::float(rounded))
    } else {
        rounded_f64_to_number_value(rounded)
    }
}

/// floatfns.c's ceiling2, floor2, truncate2 and round2: N divided by D,
/// fixnums and D nonzero, rounded as KIND says (round to even on a tie).
/// A fixnum's quotient is within an i64.
fn fixnum_divide(kind: RoundingKind, n: i64, d: i64) -> i64 {
    let (q, r) = (n / d, n % d);
    match kind {
        RoundingKind::Ceiling => q + i64::from(r != 0 && (n < 0) == (d < 0)),
        RoundingKind::Floor => q - i64::from(r != 0 && (n < 0) != (d < 0)),
        RoundingKind::Truncate => q,
        RoundingKind::Round => {
            // The remainder on the other side of zero, if it is nearer
            // zero, or as near and the truncated quotient odd.
            let abs_r = r.abs();
            let abs_r1 = d.abs() - abs_r;
            if abs_r1 < abs_r + (q & 1) {
                q + if (d < 0) == (r < 0) { 1 } else { -1 }
            } else {
                q
            }
        }
    }
}

pub(crate) fn rounded_f64_to_number_value(value: f64) -> Result<Value, LispError> {
    Ok(normalize_bigint_value(bigint_from_truncated_float(value)?))
}

pub(crate) fn integer_like_bigint_for_rounding(
    interp: &Interpreter,
    value: &Value,
) -> Option<BigInt> {
    match value.kind() {
        Kind::Float(value) => bigint_from_integral_float(value.get()),
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
    check_number(value)?;
    match value.kind() {
        Kind::Integer(number) => {
            if number == 0 {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            Ok(Value::Integer(
                i64::BITS as i64 - 1 - number.unsigned_abs().leading_zeros() as i64,
            ))
        }
        Kind::BigInteger(number) => {
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
    // floatfns.c:Fexpt: CHECK_NUMBER (arg1), CHECK_NUMBER (arg2).
    check_number(base)?;
    check_number(exponent)?;
    let exponent_bigint = integer_like_bigint(interp, exponent);
    if matches!(base.kind(), Kind::Float(_)) || matches!(exponent.kind(), Kind::Float(_)) {
        return Ok(Value::float(
            numeric_to_f64(interp, base)?.powf(numeric_to_f64(interp, exponent)?),
        ));
    }
    let exponent_bigint = exponent_bigint?;
    if exponent_bigint.is_negative() {
        let base_value = integer_like_i64(interp, base)? as f64;
        let exponent_value = exponent_bigint
            .to_f64()
            .ok_or_else(|| LispError::WrongTypeArgument("number-or-marker-p".into(), *exponent))?;
        return Ok(Value::float(base_value.powf(exponent_value)));
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
    match value.kind() {
        Kind::Float(value) => Ok(exact_float_binary_rational(value.get())),
        Kind::Integer(value) => Ok(Some((BigInt::from(value), 0))),
        Kind::BigInteger(value) => Ok(Some(((value).into(), 0))),
        Kind::Marker(_) => Ok(Some((BigInt::from(integer_like_i64(interp, value)?), 0))),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            *value,
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

impl ExactTimeValue {
    /// A timespec's time on the nanosecond clock.
    pub(crate) fn from_timespec(seconds: i64, nanoseconds: u32) -> Self {
        Self {
            ticks: BigInt::from(seconds) * BigInt::from(1_000_000_000u32)
                + BigInt::from(nanoseconds),
            hz: BigInt::from(1_000_000_000u32),
        }
    }
}

/// timefns.c's lisp_time: TICKS over HZ as given.  decode_ticks_hz only
/// checks that HZ is positive; nothing reduces the fraction, so
/// `(time-convert '(4 . 8) t)' is (4 . 8), and `time-add' keeps a common
/// HZ (time_arith reduces only when the clocks differ).  Reducing here
/// changed those answers and cost a bignum gcd on every time value.
pub(crate) fn exact_time_value(ticks: BigInt, hz: BigInt) -> Result<ExactTimeValue, LispError> {
    if hz <= BigInt::zero() {
        return Err(LispError::Signal("Invalid time resolution".into()));
    }
    Ok(ExactTimeValue { ticks, hz })
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

/// timefns.c's enum timeform: the shape decode_lisp_time found a
/// timestamp in, which time_arith and `time-convert' look at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimeForm {
    Invalid,
    /// Seconds as (HI << 16) + LO, or an integer.
    HiLo,
    /// (HI LO US).
    HiLoUs,
    /// nil, the clock.
    Nil,
    /// (HI LO US PS).
    HiLoUsPs,
    Float,
    /// (TICKS . HZ).
    TicksHz,
}

/// timefns.c:time_error for EINVAL.
pub(crate) fn invalid_time_specification() -> LispError {
    LispError::Signal("Invalid time specification".into())
}

/// timefns.c:time_overflow, and time_error for EOVERFLOW.
pub(crate) fn time_overflow() -> LispError {
    LispError::Signal("Specified time is not representable".into())
}

/// timefns.c:invalid_hz.
fn invalid_hz(hz: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("Invalid time frequency"),
        hz,
    ]))
}

fn lisp_integer(value: &Value) -> Option<BigInt> {
    match value.kind() {
        Kind::Integer(n) => Some(BigInt::from(n)),
        Kind::BigInteger(n) => Some(n.into()),
        _ => None,
    }
}

/// timefns.c:decode_float_time: TICKS / HZ exactly equal to the finite
/// T, HZ being T's precision (trailing zeros count) or 1 if greater.
pub(crate) fn decode_float_time(value: f64) -> ExactTimeValue {
    match exact_float_binary_rational(value) {
        Some((significand, exponent)) if exponent < 0 => ExactTimeValue {
            ticks: significand,
            hz: BigInt::from(1u8) << (-exponent) as usize,
        },
        Some((significand, exponent)) => ExactTimeValue {
            ticks: significand << exponent as usize,
            hz: BigInt::from(1u8),
        },
        None => ExactTimeValue {
            ticks: BigInt::zero(),
            hz: BigInt::from(1u8),
        },
    }
}

/// decode_lisp_time's reading of SPECIFIED_TIME: its form and its HIGH,
/// LOW, USEC and PSEC components (LOW is the whole of a non-list).
/// DECODE_SECS_ONLY leaves a list's subsecond parts unread.
fn lisp_time_components(
    specified_time: &Value,
    decode_secs_only: bool,
) -> (TimeForm, Value, Value, Value, Value) {
    let zero = Value::Integer(0);
    let (mut high, mut low, mut usec, mut psec) = (zero, *specified_time, zero, zero);
    let mut form = TimeForm::HiLo;
    if specified_time.is_nil() {
        form = TimeForm::Nil;
    } else if let Some(cell) = specified_time.cons_ref() {
        high = cell.car.get();
        low = cell.cdr.get();
        if let Some(low_cell) = low.cons_ref() {
            let mut low_tail = low_cell.cdr.get();
            low = low_cell.car.get();
            if !decode_secs_only {
                if let Some(usec_cell) = low_tail.cons_ref() {
                    usec = usec_cell.car.get();
                    low_tail = usec_cell.cdr.get();
                    if let Some(psec_cell) = low_tail.cons_ref() {
                        psec = psec_cell.car.get();
                        form = TimeForm::HiLoUsPs;
                    } else {
                        form = TimeForm::HiLoUs;
                    }
                } else if !low_tail.is_nil() {
                    usec = low_tail;
                    form = TimeForm::HiLoUs;
                }
            }
        } else {
            form = TimeForm::TicksHz;
        }
        // "Require LOW to be an integer, as otherwise the computation
        // would be considerably trickier."
        if !low.is_integer() {
            form = TimeForm::Invalid;
        }
    }
    (form, high, low, usec, psec)
}

/// timefns.c:decode_time_components for a non-float FORM; None is its
/// EINVAL.  The list forms carry each out-of-range lower component into
/// the next, which leaves the value TICKS / HZ: HZ is the list's own
/// clock, 1 for (HI LO), 10^6 for (HI LO US) and 10^12 for (HI LO US PS).
pub(crate) fn decode_time_components(
    form: TimeForm,
    high: &Value,
    low: &Value,
    usec: &Value,
    psec: &Value,
) -> Option<ExactTimeValue> {
    match form {
        TimeForm::Invalid | TimeForm::Float => None,
        TimeForm::TicksHz => {
            let ticks = lisp_integer(high)?;
            let hz = lisp_integer(low).filter(|hz| hz.sign() == Sign::Plus)?;
            Some(ExactTimeValue { ticks, hz })
        }
        TimeForm::Nil => current_time_value().ok(),
        TimeForm::HiLo | TimeForm::HiLoUs | TimeForm::HiLoUsPs => {
            let high = lisp_integer(high)?;
            let low = lisp_integer(low)?;
            let us = BigInt::from(usec.fixnum()?);
            let ps = BigInt::from(psec.fixnum()?);
            let seconds = high * BigInt::from(65_536u32) + low;
            Some(match form {
                TimeForm::HiLo => ExactTimeValue {
                    ticks: seconds,
                    hz: BigInt::from(1u8),
                },
                TimeForm::HiLoUs => ExactTimeValue {
                    ticks: seconds * BigInt::from(1_000_000u32) + us,
                    hz: BigInt::from(1_000_000u32),
                },
                _ => ExactTimeValue {
                    ticks: seconds * BigInt::from(1_000_000_000_000u64)
                        + us * BigInt::from(1_000_000u32)
                        + ps,
                    hz: BigInt::from(1_000_000_000_000u64),
                },
            })
        }
    }
}

/// timefns.c:decode_lisp_time: SPECIFIED_TIME's form and value, nil
/// reading the clock.  An integer is (TICKS . 1) in the HI-LO form; a
/// float must be finite.  DECODE_SECS_ONLY ignores, and does not check,
/// an old-style list's subsecond parts.
pub(crate) fn decode_lisp_time(
    specified_time: &Value,
    decode_secs_only: bool,
) -> Result<(TimeForm, ExactTimeValue), LispError> {
    match specified_time.kind() {
        Kind::Integer(ticks) => {
            return Ok((
                TimeForm::HiLo,
                ExactTimeValue {
                    ticks: BigInt::from(ticks),
                    hz: BigInt::from(1u8),
                },
            ));
        }
        Kind::BigInteger(ticks) => {
            return Ok((
                TimeForm::HiLo,
                ExactTimeValue {
                    ticks: ticks.into(),
                    hz: BigInt::from(1u8),
                },
            ));
        }
        Kind::Float(float) => {
            let value = float.get();
            if value.is_nan() {
                return Err(invalid_time_specification());
            }
            if value.is_infinite() {
                return Err(time_overflow());
            }
            return Ok((TimeForm::Float, decode_float_time(value)));
        }
        _ => {}
    }
    let (form, high, low, usec, psec) = lisp_time_components(specified_time, decode_secs_only);
    decode_time_components(form, &high, &low, &usec, &psec)
        .map(|time| (form, time))
        .ok_or_else(invalid_time_specification)
}

/// timefns.c:lisp_time_struct.
pub(crate) fn lisp_time_struct(specified_time: &Value) -> Result<ExactTimeValue, LispError> {
    decode_lisp_time(specified_time, false).map(|(_, time)| time)
}

/// timefns.c:lisp_to_timespec: TIME floored to whole nanoseconds, as
/// seconds and the nanoseconds within them; None when the seconds do not
/// fit a time_t.
pub(crate) fn lisp_to_timespec(time: &ExactTimeValue) -> Option<(i64, u32)> {
    let nanoseconds = exact_time_floor_nanoseconds(time);
    let (seconds, subsecond) = floor_div_mod(&nanoseconds, &BigInt::from(1_000_000_000u32));
    Some((seconds.to_i64()?, subsecond.to_u32()?))
}

/// timefns.c:lisp_time_argument: SPECIFIED_TIME as a valid timespec.
pub(crate) fn lisp_time_argument(specified_time: &Value) -> Result<(i64, u32), LispError> {
    lisp_to_timespec(&lisp_time_struct(specified_time)?).ok_or_else(time_overflow)
}

/// timefns.c:lisp_seconds_argument: SPECIFIED_TIME's whole seconds,
/// without decoding (or checking) an old-style list's subsecond parts.
pub(crate) fn lisp_seconds_argument(specified_time: &Value) -> Result<i64, LispError> {
    let (_, time) = decode_lisp_time(specified_time, true)?;
    lisp_to_timespec(&time)
        .map(|(seconds, _)| seconds)
        .ok_or_else(time_overflow)
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

/// timefns.c lisp_to_timespec's first step: TIME as whole nanoseconds,
/// floored (TICKS * 10^9 floor-divided by HZ).
pub(crate) fn exact_time_floor_nanoseconds(time: &ExactTimeValue) -> BigInt {
    floor_div_mod(&(&time.ticks * BigInt::from(1_000_000_000u32)), &time.hz).0
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

/// C's scalbn: X times 2 to the N, rounded once (musl's stepwise
/// scaling keeps a subnormal result from rounding twice).
pub(crate) fn scalbn(mut x: f64, mut n: i32) -> f64 {
    const TWO_1023: f64 = 8.98846567431158e307;
    const TWO_M1022_53: f64 = 2.2250738585072014e-308 * 9007199254740992.0;
    if n > 1023 {
        x *= TWO_1023;
        n -= 1023;
        if n > 1023 {
            x *= TWO_1023;
            n -= 1023;
            if n > 1023 {
                n = 1023;
            }
        }
    } else if n < -1022 {
        x *= TWO_M1022_53;
        n += 1022 - 53;
        if n < -1022 {
            x *= TWO_M1022_53;
            n += 1022 - 53;
            if n < -1022 {
                n = -1022;
            }
        }
    }
    x * f64::from_bits(((0x3ff + n) as u64) << 52)
}

/// timefns.c frac_to_double: NUMERATOR / DENOMINATOR rounded to the
/// nearest double, ties to even.  DENOMINATOR is positive.
pub(crate) fn frac_to_double(numerator: &BigInt, denominator: &BigInt) -> f64 {
    const DBL_MANT_DIG: i64 = 53;
    // flt_radix_power_size - 1: DBL_MANT_DIG - DBL_MIN_EXP.
    const MAX_SCALE: i64 = 53 + 1021;
    if let (Some(n), Some(d)) = (numerator.to_i64(), denominator.to_i64())
        && n % d == 0
    {
        return (n / d) as f64;
    }
    // mpz_sizeinbase (x, 2): the bit count, 1 for zero.
    let ndig = numerator.bits().max(1) as i64;
    let ddig = denominator.bits().max(1) as i64;
    let mut scale = ddig - ndig + DBL_MANT_DIG;
    let (n, d) = if scale < 0 {
        (numerator.clone(), denominator << (-scale) as usize)
    } else {
        // min so tiny numbers are not scaled as if they were normalized.
        scale = scale.min(MAX_SCALE);
        (numerator << scale as usize, denominator.clone())
    };
    // Truncating division, as mpz_tdiv_qr.
    let (mut q, r) = (&n / &d, &n % &d);
    let q_magnitude = q.magnitude().clone();
    // The amount to add to the absolute value of Q so that truncating
    // it to double rounds correctly.
    let incr: u32 = if q_magnitude.bits().max(1) as i64 <= DBL_MANT_DIG {
        // Converting to double uses the whole quotient: add 1 as per
        // round-to-even when the doubled remainder exceeds the
        // denominator, or equals it and the quotient is odd.
        let doubled = r.magnitude() << 1usize;
        match doubled.cmp(d.magnitude()) {
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Equal => u32::from(q_magnitude.bit(0)),
            std::cmp::Ordering::Less => 0,
        }
    } else {
        // Converting to double discards the quotient's low-order bit:
        // add 2 as per round-to-even.
        let lo_2digits = (&q_magnitude % 4u32).to_u32().unwrap_or(0);
        let lo_digit = lo_2digits % 2;
        if lo_digit == 1 && ((lo_2digits / 2) & 1 == 1 || !r.is_zero()) {
            2
        } else {
            0
        }
    };
    if incr != 0 {
        if n.sign() == Sign::Minus {
            q -= incr;
        } else {
            q += incr;
        }
    }
    // mpz_get_d truncates: a 54-bit quotient loses its low-order bit
    // (the increment above already rounded it).  Then rescale Q back to
    // double; this step does not round.
    let mut magnitude = q.magnitude().clone();
    if magnitude.bits() > DBL_MANT_DIG as u64 {
        magnitude = (magnitude >> 1usize) << 1usize;
    }
    let truncated = magnitude.to_f64().unwrap_or(0.0);
    let truncated = if q.sign() == Sign::Minus {
        -truncated
    } else {
        truncated
    };
    scalbn(truncated, -(scale as i32))
}

pub(crate) fn exact_time_to_f64(time: &ExactTimeValue) -> f64 {
    frac_to_double(&time.ticks, &time.hz)
}

pub(crate) fn exact_time_equal(left: &ExactTimeValue, right: &ExactTimeValue) -> bool {
    left.ticks.clone() * &right.hz == right.ticks.clone() * &left.hz
}

pub(crate) fn exact_time_less(left: &ExactTimeValue, right: &ExactTimeValue) -> bool {
    left.ticks.clone() * &right.hz < right.ticks.clone() * &left.hz
}

/// timefns.c:invalid_time_zone_specification.
fn invalid_time_zone_specification(zone: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("Invalid time zone specification"),
        zone,
    ]))
}

/// timefns.c:tzlookup's zone string for an integer offset ZONE (and the
/// abbreviation ABBR of a `(OFFSET ABBR)' list): "<+HH[MM[SS]]>-H:MM:SS",
/// a POSIX TZ string for the fixed offset.
fn offset_zone_string(offset: i64, abbr: Option<&str>) -> String {
    let abszone = offset.unsigned_abs();
    let hour = abszone / (60 * 60);
    let hour_remainder = abszone % (60 * 60);
    let (min, sec) = (hour_remainder / 60, hour_remainder % 60);
    // The TZ string's offset is west of Greenwich, so its sign is
    // opposite.
    let trailing = format!(
        "{}{hour}:{min:02}:{sec:02}",
        if offset < 0 { "" } else { "-" }
    );
    match abbr {
        Some(abbr) => format!("<{abbr}>{trailing}"),
        None => {
            let (mut prec, mut numzone) = (2usize, hour);
            if hour_remainder != 0 {
                prec += 2;
                numzone = 100 * numzone + min;
                if sec != 0 {
                    prec += 2;
                    numzone = 100 * numzone + sec;
                }
            }
            // "%+.*d": the sign, and at least PREC digits.
            let sign = if offset < 0 { '-' } else { '+' };
            format!("<{sign}{numzone:0prec$}>{trailing}")
        }
    }
}

/// The local time zone rule as `set-time-zone-rule' and init_timefns
/// record it: t for utc_tz, `wall' for TZ unset, else the TZ string.
fn local_tz(interp: &Interpreter) -> TimeZone {
    let rule = interp.local_time_zone_rule;
    match rule.kind() {
        Kind::T => TimeZone::Utc,
        _ if rule.is_string() => TimeZone::Tz(Some(Rc::from(
            string_text(&rule).unwrap_or_default().as_str(),
        ))),
        _ => TimeZone::Tz(None),
    }
}

/// timefns.c:tzlookup (ZONE, false): ZONE as a timezone_t, nil being
/// Emacs local time, t and 0 Universal Time (the null zone that gmtime
/// serves), `wall' the system wall clock, a string a TZ setting, and an
/// integer or `(OFFSET ABBR)' a fixed offset.
pub(crate) fn tzlookup(interp: &Interpreter, zone: &Value) -> Result<TimeZone, LispError> {
    if zone.is_nil() {
        return Ok(local_tz(interp));
    }
    let zone = super::values::bare_symbol(interp, *zone);
    match zone.kind() {
        Kind::T | Kind::Integer(0) => Ok(TimeZone::Utc),
        Kind::Symbol(symbol) if symbol == "wall" => Ok(TimeZone::Tz(None)),
        _ if zone.is_string() => Ok(TimeZone::Tz(Some(Rc::from(string_text(&zone)?.as_str())))),
        Kind::Integer(offset) => Ok(TimeZone::Tz(Some(Rc::from(
            offset_zone_string(offset, None).as_str(),
        )))),
        Kind::Cons(cell)
            if cell.car.get().fixnum().is_some() && cell.cdr.get().cons_ref().is_some() =>
        {
            let offset = cell.car.get().fixnum().unwrap_or_default();
            let abbr = cell.cdr.get().cons_ref().map(|abbr| abbr.car.get());
            let abbr = zone_abbreviation_text(interp, abbr.unwrap_or(Value::Nil))?;
            Ok(TimeZone::Tz(Some(Rc::from(
                offset_zone_string(offset, Some(&abbr)).as_str(),
            ))))
        }
        _ => Err(invalid_time_zone_specification(zone)),
    }
}

/// The text concat3 makes of a `(OFFSET ABBR)' zone's ABBR after
/// ENCODE_SYSTEM: a string's own (encoding needs a string unless
/// `locale-coding-system' is nil), else a sequence of characters.
fn zone_abbreviation_text(interp: &Interpreter, abbr: Value) -> Result<String, LispError> {
    if abbr.is_string() {
        return string_text(&abbr);
    }
    let coding = interp
        .lookup_var("locale-coding-system", &crate::lisp::types::Env::new())
        .unwrap_or(Value::Nil);
    if !coding.is_nil() {
        return Err(LispError::WrongTypeArgument("stringp".into(), abbr));
    }
    let elements = match abbr.kind() {
        Kind::Nil => Vec::new(),
        Kind::Cons(_) => abbr.to_vec()?,
        Kind::Vector(_) => vector_items(&abbr)?,
        _ => return Err(LispError::WrongTypeArgument("sequencep".into(), abbr)),
    };
    elements
        .iter()
        .map(|element| {
            element
                .fixnum()
                .and_then(|code| u32::try_from(code).ok())
                .and_then(char::from_u32)
                .ok_or_else(|| LispError::WrongTypeArgument("characterp".into(), *element))
        })
        .collect()
}

/// timefns.c:emacs_localtime_rz, failing as time_error does.
fn emacs_localtime_rz(tz: &TimeZone, seconds: i64) -> Result<Tm, LispError> {
    localtime_rz(tz, seconds).map_err(|_| time_overflow())
}

/// timefns.c:check_tm_member: OBJ less OFFSET as an int.
fn check_tm_member(obj: &Value, offset: i32) -> Result<i32, LispError> {
    let Some(n) = obj.fixnum() else {
        return Err(LispError::WrongTypeArgument("fixnump".into(), *obj));
    };
    n.checked_sub(i64::from(offset))
        .and_then(|i| i32::try_from(i).ok())
        .ok_or_else(time_overflow)
}

/// A Lisp string's bytes as code_convert_string_norecord encodes it with
/// `locale-coding-system' (nil: the string's own bytes).
fn locale_encoded_bytes(
    interp: &mut Interpreter,
    string: &Value,
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    let coding = interp
        .lookup_var("locale-coding-system", env)
        .unwrap_or(Value::Nil);
    let source = if coding.is_nil() {
        *string
    } else {
        call(
            interp,
            "encode-coding-string",
            &[*string, coding, Value::T],
            env,
        )?
    };
    Ok(match source.kind() {
        Kind::StringObject(state) => state.borrow().bytes().to_vec(),
        _ => string_text(&source)?.into_bytes(),
    })
}

/// format_time_string's result: BYTES decoded with `locale-coding-system'
/// (nil: no-conversion, whose ASCII text is a multibyte string and whose
/// other text is the bytes themselves).
fn locale_decoded_string(
    interp: &mut Interpreter,
    bytes: &[u8],
    env: &mut Env,
) -> Result<Value, LispError> {
    let coding = interp
        .lookup_var("locale-coding-system", env)
        .unwrap_or(Value::Nil);
    if bytes.is_ascii() {
        return Ok(string_like_value_with_multibyte(
            String::from_utf8_lossy(bytes).into_owned(),
            None,
            true,
        ));
    }
    let raw = super::coding::bytes_to_unibyte_value(bytes);
    if coding.is_nil() {
        return Ok(raw);
    }
    call(
        interp,
        "decode-coding-string",
        &[raw, coding, Value::T],
        env,
    )
}

/// timefns.c:format_time_string: FORMAT's bytes formatted for the
/// timespec SECONDS, NANOSECONDS in ZONE, and the broken-down time.
fn format_time_string(
    interp: &mut Interpreter,
    format: &[u8],
    seconds: i64,
    nanoseconds: u32,
    zone: &Value,
    env: &mut Env,
) -> Result<(Value, Tm), LispError> {
    let tz = tzlookup(interp, zone)?;
    let tm = emacs_localtime_rz(&tz, seconds)?;
    let bytes = super::strftime::nmemftime(format, &tm, &tz, nanoseconds as i32);
    Ok((locale_decoded_string(interp, &bytes, env)?, tm))
}

/// Fdecode_time's list for TIME (whose timespec's seconds are SECONDS)
/// in ZONE.  SEC is an integer unless FORM is t and TIME's clock is not
/// 1, when it is (TICKS . HZ) with TICKS = HZ * SEC + mod (TIME's ticks,
/// HZ).
pub(crate) fn decode_time_value(
    interp: &Interpreter,
    time: &ExactTimeValue,
    seconds: i64,
    zone: &Value,
    form: &Value,
) -> Result<Value, LispError> {
    let tz = tzlookup(interp, zone)?;
    let tm = emacs_localtime_rz(&tz, seconds)?;
    let second_field = if time.hz == BigInt::from(1u8) || !matches!(form.kind(), Kind::T) {
        Value::Integer(i64::from(tm.sec))
    } else {
        let (_, fractional_ticks) = time_floor_parts(time);
        Value::cons(
            normalize_bigint_value(BigInt::from(tm.sec) * &time.hz + fractional_ticks),
            normalize_bigint_value(time.hz.clone()),
        )
    };
    Ok(Value::list([
        second_field,
        Value::Integer(i64::from(tm.min)),
        Value::Integer(i64::from(tm.hour)),
        Value::Integer(i64::from(tm.mday)),
        Value::Integer(i64::from(tm.mon) + 1),
        Value::Integer(i64::from(tm.year) + 1900),
        Value::Integer(i64::from(tm.wday)),
        match tm.isdst {
            isdst if isdst < 0 => Value::Integer(-1),
            0 => Value::Nil,
            _ => Value::T,
        },
        Value::Integer(tm.gmtoff),
    ]))
}

/// timefns.c:Fencode_time: (encode-time TIME) with TIME a list (SEC MINUTE
/// HOUR DAY MONTH YEAR [IGNORED DST ZONE]), or the obsolescent (encode-time
/// SEC MINUTE HOUR DAY MONTH YEAR [... ZONE]).  SEC is any time value;
/// mktime_z normalizes out-of-range fields.
fn encode_time(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    let mut zone = Value::Nil;
    let mut isdst = -1;
    let consp = |tail: Value| LispError::WrongTypeArgument("consp".into(), tail);
    let fields: [Value; 6] = if let [time] = args {
        let mut tail = *time;
        let mut fields = [Value::Nil; 6];
        for field in &mut fields {
            let cell = tail.cons_ref().ok_or_else(|| consp(tail))?;
            *field = cell.car.get();
            tail = cell.cdr.get();
        }
        if !tail.is_nil() {
            // The ignored day of the week, then DST and ZONE.
            tail = tail.cons_ref().ok_or_else(|| consp(tail))?.cdr.get();
            let cell = tail.cons_ref().ok_or_else(|| consp(tail))?;
            let dstflag = cell.car.get();
            tail = cell.cdr.get();
            zone = tail.cons_ref().ok_or_else(|| consp(tail))?.car.get();
            if dstflag.is_symbol() && zone.fixnum().is_none() && zone.cons_ref().is_none() {
                isdst = i32::from(!dstflag.is_nil());
            }
        }
        fields
    } else if args.len() < 6 {
        return Err(LispError::WrongNumberOfArgs(
            "encode-time".into(),
            args.len(),
        ));
    } else {
        if args.len() > 6 {
            zone = args[args.len() - 1];
        }
        [args[0], args[1], args[2], args[3], args[4], args[5]]
    };
    // SEC = floor (TICKS / HZ), with SUBSECTICKS the remainder.
    let seconds = lisp_time_struct(&fields[0])?;
    let (sec, subsecticks) = floor_div_mod(&seconds.ticks, &seconds.hz);
    let mut tm = Tm {
        sec: check_tm_member(&normalize_bigint_value(sec), 0)?,
        min: check_tm_member(&fields[1], 0)?,
        hour: check_tm_member(&fields[2], 0)?,
        mday: check_tm_member(&fields[3], 0)?,
        mon: check_tm_member(&fields[4], 1)?,
        year: check_tm_member(&fields[5], 1900)?,
        isdst,
        wday: -1,
        ..Tm::default()
    };
    let tz = tzlookup(interp, &zone)?;
    let value = mktime_z(&tz, &mut tm).map_err(|_| time_overflow())?;
    if seconds.hz == BigInt::from(1u8) {
        return Ok(if current_time_list_is_set(interp) {
            Value::list([Value::Integer(value >> 16), Value::Integer(value & 0xffff)])
        } else {
            Value::Integer(value)
        });
    }
    Ok(exact_time_to_tick_pair(&ExactTimeValue {
        ticks: BigInt::from(value) * &seconds.hz + subsecticks,
        hz: seconds.hz,
    }))
}

/// timefns.c's `current_time_list': the DEFVAR_BOOL that chooses the
/// old-style list for `current-time' and a nil `time-convert' FORM.
fn current_time_list_is_set(interp: &Interpreter) -> bool {
    interp
        .lookup_var("current-time-list", &crate::lisp::types::Env::new())
        .is_none_or(|value| value.is_truthy())
}

/// timefns.c:Ftime_convert.  A (TICKS . HZ) TIME already on FORM's
/// clock is returned itself, as is an integer TIME for `integer'.
fn time_convert(interp: &Interpreter, time: &Value, form: &Value) -> Result<Value, LispError> {
    let (input_form, decoded) = decode_lisp_time(time, false)?;
    let form = if !form.is_nil() {
        super::values::bare_symbol(interp, *form)
    } else if current_time_list_is_set(interp) {
        Value::symbol("list")
    } else {
        Value::T
    };
    if matches!(form.kind(), Kind::Symbol(symbol) if symbol == "list") {
        return exact_time_to_old_style(&decoded);
    }
    if matches!(form.kind(), Kind::Symbol(symbol) if symbol == "integer") {
        return Ok(if time.is_integer() {
            *time
        } else {
            exact_time_floor_integer_value(&decoded)
        });
    }
    if matches!(form.kind(), Kind::T) {
        // FORM is TIME's own clock, which a (TICKS . HZ) TIME is on.
        if input_form == TimeForm::TicksHz {
            return Ok(*time);
        }
        return Ok(exact_time_to_tick_pair(&decoded));
    }
    if input_form == TimeForm::TicksHz
        && time
            .cons_ref()
            .is_some_and(|cell| cell.cdr.get().word() == form.word())
    {
        return Ok(*time);
    }
    Ok(Value::cons(lisp_time_hz_ticks(&decoded, &form)?, form))
}

/// timefns.c:lisp_time_hz_ticks: TIME floored to a count of HZ ticks,
/// HZ being checked.
fn lisp_time_hz_ticks(time: &ExactTimeValue, hz: &Value) -> Result<Value, LispError> {
    let Some(target) = lisp_integer(hz).filter(|target| target.sign() == Sign::Plus) else {
        return Err(invalid_hz(*hz));
    };
    if target == time.hz {
        return Ok(normalize_bigint_value(time.ticks.clone()));
    }
    let (ticks, _) = floor_div_mod(&(&time.ticks * &target), &time.hz);
    Ok(normalize_bigint_value(ticks))
}

/// timefns.c:Fcurrent_time's make_lisp_time of the clock, as the general
/// path renders it: (HI LO US PS) under `current-time-list', else
/// (TICKS . 1000000000), while the ticks are a fixnum.
fn current_time_without_bignums(interp: &Interpreter) -> Option<Value> {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    let seconds = i64::try_from(since.as_secs()).ok()?;
    let nanos = i64::from(since.subsec_nanos());
    if current_time_list_is_set(interp) {
        return Some(Value::list([
            Value::Integer(seconds >> 16),
            Value::Integer(seconds & 0xffff),
            Value::Integer(nanos / 1000),
            Value::Integer(nanos % 1000 * 1000),
        ]));
    }
    let ticks = seconds.checked_mul(1_000_000_000)?.checked_add(nanos)?;
    (ticks <= MOST_POSITIVE_FIXNUM)
        .then(|| Value::cons(Value::Integer(ticks), Value::Integer(1_000_000_000)))
}

/// timefns.c:time_cmp for the operands it can order without a bignum:
/// `eq' operands (valid or not), fixnums, (X . Z) and (Y . Z) with one Z
/// and fixnum X and Y (Z is not inspected), and otherwise the ticks and
/// clocks of two time values that fit machine integers, cross-multiplied.
/// None leaves the comparison, and any error, to the general decoding.
fn time_cmp_without_bignums(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if a.word() == b.word() {
        return Some(std::cmp::Ordering::Equal);
    }
    let (mut x, mut y) = (*a, *b);
    if let (Some(left), Some(right)) = (a.cons_ref(), b.cons_ref())
        && left.cdr.get().word() == right.cdr.get().word()
    {
        x = left.car.get();
        y = right.car.get();
    }
    if let (Some(x), Some(y)) = (x.fixnum(), y.fixnum()) {
        return Some(x.cmp(&y));
    }
    let mut now = None;
    let (a_ticks, a_hz) = small_time(a, &mut now)?;
    let (b_ticks, b_hz) = small_time(b, &mut now)?;
    Some(a_ticks.checked_mul(b_hz)?.cmp(&b_ticks.checked_mul(a_hz)?))
}

/// A time value's TICKS and HZ (as decode_lisp_time decodes it) when
/// every part is a fixnum and they fit an i128: nil (the clock, read
/// once into NOW), a fixnum, a float, (HI LO [US [PS]]) or (TICKS . HZ).
fn small_time(value: &Value, now: &mut Option<(i128, i128)>) -> Option<(i128, i128)> {
    match value.kind() {
        Kind::Nil => {
            if now.is_none() {
                let since = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()?;
                *now = Some((
                    i128::from(since.as_secs()) * 1_000_000_000 + i128::from(since.subsec_nanos()),
                    1_000_000_000,
                ));
            }
            *now
        }
        Kind::Integer(ticks) => Some((i128::from(ticks), 1)),
        Kind::Float(float) => {
            let value = float.get();
            if !value.is_finite() {
                return None;
            }
            if value == 0.0 {
                return Some((0, 1));
            }
            // exact_float_binary_rational's significand and exponent.
            let bits = value.to_bits();
            let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
            let mantissa = bits & ((1u64 << 52) - 1);
            let (significand, exponent) = if exponent_bits == 0 {
                (mantissa, 1 - 1023 - 52)
            } else {
                ((1u64 << 52) | mantissa, exponent_bits - 1023 - 52)
            };
            let significand = if bits >> 63 != 0 {
                -i128::from(significand)
            } else {
                i128::from(significand)
            };
            if exponent >= 0 {
                (exponent <= 64).then(|| (significand << exponent, 1))
            } else {
                (exponent >= -120).then(|| (significand, 1i128 << -exponent))
            }
        }
        Kind::Cons(_) => {
            let (form, high, low, usec, psec) = lisp_time_components(value, false);
            let (high, low) = (i128::from(high.fixnum()?), i128::from(low.fixnum()?));
            if form == TimeForm::TicksHz {
                return (low > 0).then_some((high, low));
            }
            if !matches!(form, TimeForm::HiLo | TimeForm::HiLoUs | TimeForm::HiLoUsPs) {
                return None;
            }
            // decode_time_components: the list's own clock.
            let (us, ps) = (i128::from(usec.fixnum()?), i128::from(psec.fixnum()?));
            let seconds = high.checked_mul(65_536)?.checked_add(low)?;
            match form {
                TimeForm::HiLo => Some((seconds, 1)),
                TimeForm::HiLoUs => {
                    Some((seconds.checked_mul(1_000_000)?.checked_add(us)?, 1_000_000))
                }
                _ => Some((
                    seconds
                        .checked_mul(1_000_000_000_000)?
                        .checked_add(us.checked_mul(1_000_000)?)?
                        .checked_add(ps)?,
                    1_000_000_000_000,
                )),
            }
        }
        _ => None,
    }
}

define_dispatch!(
    pub(crate) fn call_time_builtin(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "set-time-zone-rule" => {
                need_args(name, args, 1)?;
                // Fset_time_zone_rule: tzlookup (nil means `wall', SETTZ).
                let zone = if args[0].is_nil() {
                    Value::symbol("wall")
                } else {
                    args[0]
                };
                interp.local_time_zone_rule = match tzlookup(interp, &zone)? {
                    TimeZone::Utc => Value::T,
                    TimeZone::Tz(None) => Value::symbol("wall"),
                    TimeZone::Tz(Some(tz)) => Value::String((*tz).into()),
                };
                Ok(Value::Nil)
            }
            "current-time" => {
                need_arg_range(name, args, 0, 0)?;
                if let Some(value) = current_time_without_bignums(interp) {
                    return Ok(value);
                }
                // timefns.c make_lisp_time: the old-style list under
                // `current-time-list', else the nanosecond
                // (TICKS . 1000000000) pair, never reduced.
                let now = current_time_value()?;
                if current_time_list_is_set(interp) {
                    exact_time_to_old_style(&now)
                } else {
                    exact_time_to_scaled_pair(&now, &BigInt::from(1_000_000_000u64))
                }
            }
            "current-time-string" => {
                need_arg_range(name, args, 0, 2)?;
                // Fcurrent_time_string: the whole seconds alone, in ctime's
                // layout without its newline or its four-digit year limit.
                let seconds = lisp_seconds_argument(args.first().unwrap_or(&Value::Nil))?;
                let tz = tzlookup(interp, args.get(1).unwrap_or(&Value::Nil))?;
                let tm = emacs_localtime_rz(&tz, seconds)?;
                const WDAY_NAME: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
                const MON_NAME: [&str; 12] = [
                    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
                    "Dec",
                ];
                let text = format!(
                    "{} {}{:3} {:02}:{:02}:{:02} {}",
                    WDAY_NAME[tm.wday as usize],
                    MON_NAME[tm.mon as usize],
                    tm.mday,
                    tm.hour,
                    tm.min,
                    tm.sec,
                    i64::from(tm.year) + 1900
                );
                Ok(super::coding::bytes_to_unibyte_value(text.as_bytes()))
            }
            "time-add" | "time-subtract" => {
                need_args(name, args, 2)?;
                let subtract = name == "time-subtract";
                // Ftime_subtract: `eq' operands, nil and nil (the clock
                // read once) among them, give make_lisp_time of a zero
                // timespec without being decoded, so even an invalid X
                // gives zero.
                if subtract && args[0].word() == args[1].word() {
                    return Ok(if current_time_list_is_set(interp) {
                        Value::list([Value::Integer(0); 4])
                    } else {
                        Value::cons(Value::Integer(0), Value::Integer(1_000_000_000))
                    });
                }
                let (left_form, left) = decode_lisp_time(&args[0], false)?;
                let (right_form, right) = decode_lisp_time(&args[1], false)?;
                // timefns.c time_arith: with equal clocks the ticks combine
                // directly and HZ is kept; with different clocks the sum is
                // taken over lcm (da, db) = fa * db (g = gcd (da, db), fa =
                // da / g, fb = db / g), then normalized by gcd (ticks, hz),
                // and if that leaves HZ below the smaller clock, rescaled up
                // by ceil (hzmin / hz) so the result is no less precise than
                // either operand.
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
                    let hzmin = left.hz.clone().min(right.hz.clone());
                    let g = bigint_gcd(left.hz.clone(), right.hz.clone());
                    let fa = left.hz.clone() / &g;
                    let fb = right.hz.clone() / &g;
                    let mut hz = fa.clone() * &right.hz;
                    let mut ticks = if subtract {
                        fb * &left.ticks - fa * &right.ticks
                    } else {
                        fb * &left.ticks + fa * &right.ticks
                    };
                    let ig = bigint_gcd(ticks.abs(), hz.clone());
                    if ig > BigInt::from(1u8) {
                        ticks /= &ig;
                        hz /= &ig;
                        if hz < hzmin {
                            let rescale = (&hzmin + &hz - BigInt::from(1u8)) / &hz;
                            ticks *= &rescale;
                            hz *= &rescale;
                        }
                    }
                    ExactTimeValue { ticks, hz }
                };
                // time_arith's result form: an integer when HZ is 1; else
                // (TICKS . HZ) when `current-time-list' is nil, either
                // operand was (TICKS . HZ), or the value has no exact
                // (HI LO US PS) form; else the list form.
                if result.hz == BigInt::from(1u8) {
                    return Ok(normalize_bigint_value(result.ticks));
                }
                let ticks_hz_input =
                    left_form == TimeForm::TicksHz || right_form == TimeForm::TicksHz;
                let exact_in_list_form =
                    (BigInt::from(1_000_000_000_000u64) % &result.hz).is_zero();
                if !current_time_list_is_set(interp) || ticks_hz_input || !exact_in_list_form {
                    return Ok(exact_time_to_tick_pair(&result));
                }
                exact_time_to_old_style(&result)
            }
            "time-equal-p" => {
                need_args(name, args, 2)?;
                // Ftime_equal_p: a nil argument equals only another.
                if args[0].is_nil() != args[1].is_nil() {
                    return Ok(Value::Nil);
                }
                if let Some(order) = time_cmp_without_bignums(&args[0], &args[1]) {
                    return Ok(if order.is_eq() { Value::T } else { Value::Nil });
                }
                let left = lisp_time_struct(&args[0])?;
                let right = lisp_time_struct(&args[1])?;
                Ok(if exact_time_equal(&left, &right) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "time-less-p" => {
                need_args(name, args, 2)?;
                if let Some(order) = time_cmp_without_bignums(&args[0], &args[1]) {
                    return Ok(if order.is_lt() { Value::T } else { Value::Nil });
                }
                let left = lisp_time_struct(&args[0])?;
                let right = lisp_time_struct(&args[1])?;
                Ok(if exact_time_less(&left, &right) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "float-time" => {
                need_arg_range(name, args, 0, 1)?;
                // Ffloat_time returns a float argument itself, even an
                // infinity or a NaN.
                let value = args.first().unwrap_or(&Value::Nil);
                if value.is_float() {
                    return Ok(*value);
                }
                Ok(Value::float(exact_time_to_f64(&lisp_time_struct(value)?)))
            }
            "time-convert" => {
                need_arg_range(name, args, 1, 2)?;
                time_convert(interp, &args[0], args.get(1).unwrap_or(&Value::Nil))
            }
            "decode-time" => {
                need_arg_range(name, args, 0, 3)?;
                let time = lisp_time_struct(args.first().unwrap_or(&Value::Nil))?;
                let (seconds, _) = lisp_to_timespec(&time).ok_or_else(time_overflow)?;
                let form = args.get(2).unwrap_or(&Value::Nil);
                decode_time_value(
                    interp,
                    &time,
                    seconds,
                    args.get(1).unwrap_or(&Value::Nil),
                    form,
                )
            }
            "encode-time" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                encode_time(interp, args)
            }
            "format-time-string" => {
                need_arg_range(name, args, 1, 3)?;
                // Fformat_time_string decodes TIME before checking FORMAT.
                let (seconds, nanoseconds) =
                    lisp_time_argument(args.get(1).unwrap_or(&Value::Nil))?;
                if !args[0].is_string() {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                }
                let format = locale_encoded_bytes(interp, &args[0], _env)?;
                let (string, _) = format_time_string(
                    interp,
                    &format,
                    seconds,
                    nanoseconds,
                    args.get(2).unwrap_or(&Value::Nil),
                    _env,
                )?;
                Ok(string)
            }
            "current-time-zone" => {
                need_arg_range(name, args, 0, 2)?;
                // Fcurrent_time_zone: the offset and "%Z" name at the whole
                // seconds of TIME (the clock for nil).
                let seconds = lisp_seconds_argument(args.first().unwrap_or(&Value::Nil))?;
                let (zone_name, tm) = format_time_string(
                    interp,
                    b"%Z",
                    seconds,
                    0,
                    args.get(1).unwrap_or(&Value::Nil),
                    _env,
                )?;
                let offset = tm.gmtoff;
                let zone_name = if string_text(&zone_name)?.is_empty() {
                    // No local time zone name: the numeric zone instead,
                    // "%c%.2ld%.*d%.*d".
                    let hour = offset / 3600;
                    let min_sec = offset % 3600;
                    let amin_sec = min_sec.abs();
                    let (min, sec) = (amin_sec / 60, amin_sec % 60);
                    let precision = |prec: bool, value: i64| {
                        if prec {
                            format!("{value:02}")
                        } else if value == 0 {
                            String::new()
                        } else {
                            value.to_string()
                        }
                    };
                    let hour = if hour < 0 {
                        format!("-{:02}", -hour)
                    } else {
                        format!("{hour:02}")
                    };
                    let text = format!(
                        "{}{hour}{}{}",
                        if offset < 0 { '-' } else { '+' },
                        precision(min_sec != 0, min),
                        precision(sec != 0, sec)
                    );
                    super::coding::bytes_to_unibyte_value(text.as_bytes())
                } else {
                    zone_name
                };
                Ok(Value::list([Value::Integer(offset), zone_name]))
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
            value.kind(),
            Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_) | Kind::Marker(_)
        ) {
            return Err(LispError::WrongTypeArgument(
                "number-or-marker-p".into(),
                *value,
            ));
        }
    }
    if matches!(left.kind(), Kind::Float(value) if value.is_nan())
        || matches!(right.kind(), Kind::Float(value) if value.is_nan())
    {
        return Ok(None);
    }

    // GNU's arithcompare handles two fixnums directly. Markers coerce to
    // fixnums at the same boundary, so keep this overwhelmingly common path
    // allocation-free and reserve exact-rational construction for mixed or
    // genuinely large representations.
    if matches!(left.kind(), Kind::Integer(_) | Kind::Marker(_))
        && matches!(right.kind(), Kind::Integer(_) | Kind::Marker(_))
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

    match (left.kind(), right.kind()) {
        (Kind::Float(left), Kind::Float(right)) => Ok(left.get().partial_cmp(&right.get())),
        (Kind::Float(left), _) if left.is_infinite() => Ok(Some(if left.is_sign_positive() {
            Ordering::Greater
        } else {
            Ordering::Less
        })),
        (_, Kind::Float(right)) if right.is_infinite() => Ok(Some(if right.is_sign_positive() {
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
    if matches!(best.kind(), Kind::Float(value) if value.is_nan()) {
        return Ok(best);
    }

    for arg in &args[1..] {
        let candidate = numeric_result_value(interp, arg)?;
        if matches!(candidate.kind(), Kind::Float(value) if value.is_nan()) {
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
    match value.kind() {
        Kind::Integer(number) => Ok(Value::Integer(number)),
        Kind::BigInteger(number) => Ok(normalize_bigint_value((number).into())),
        Kind::Float(number) => Ok(Value::Float(number)),
        Kind::Marker(id) => Ok(Value::Integer(
            interp
                .marker_position(id)
                .ok_or_else(marker_points_nowhere)? as i64,
        )),
        _ => Err(LispError::WrongTypeArgument(
            "number-or-marker-p".into(),
            *value,
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
            return Value::float(value);
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
            .map(Value::float)
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

    // lread.c:string_to_number accumulates in an unsigned machine word
    // and makes a bignum only when the digits overflow it.
    let mut small: u64 = 0;
    let mut big: Option<BigInt> = None;
    let mut saw_digit = false;
    while let Some(&ch) = chars.peek() {
        let Some(digit) = digit_value_for_base(ch, base) else {
            break;
        };
        saw_digit = true;
        match &mut big {
            Some(value) => *value = &*value * base + digit,
            None => {
                match small
                    .checked_mul(u64::from(base))
                    .and_then(|value| value.checked_add(u64::from(digit)))
                {
                    Some(value) => small = value,
                    None => big = Some(BigInt::from(small) * base + digit),
                }
            }
        }
        chars.next();
    }

    if !saw_digit {
        return Value::Integer(0);
    }
    match big {
        None if small <= MOST_POSITIVE_FIXNUM as u64 => {
            let value = small as i64;
            Value::Integer(if negative { -value } else { value })
        }
        None => normalize_bigint_value(if negative {
            -BigInt::from(small)
        } else {
            BigInt::from(small)
        }),
        Some(value) => normalize_bigint_value(if negative { -value } else { value }),
    }
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

/// data.c Fnumber_to_string: CHECK_NUMBER, then an integer's digits or
/// print.c float_to_string's text of a float.
pub(crate) fn number_to_string(interp: &Interpreter, value: &Value) -> Result<String, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(n.to_string()),
        Kind::BigInteger(n) => Ok(n.to_string()),
        Kind::Float(f) => Ok(float_to_string(interp, f.get())),
        _ => Err(LispError::WrongTypeArgument("numberp".into(), *value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_int_boundary_matches_gnu_fixnum_representation() {
        assert!(matches!(
            normalize_integer_value(MOST_NEGATIVE_FIXNUM).kind(),
            Kind::Integer(MOST_NEGATIVE_FIXNUM)
        ));
        assert!(matches!(
            normalize_integer_value(MOST_POSITIVE_FIXNUM).kind(),
            Kind::Integer(MOST_POSITIVE_FIXNUM)
        ));
        assert!(matches!(
            normalize_integer_value(MOST_NEGATIVE_FIXNUM - 1).kind(),
            Kind::BigInteger(_)
        ));
        assert!(matches!(
            normalize_integer_value(MOST_POSITIVE_FIXNUM + 1).kind(),
            Kind::BigInteger(_)
        ));
        assert!(matches!(
            normalize_bigint_value(BigInt::from(MOST_POSITIVE_FIXNUM) + 1).kind(),
            Kind::BigInteger(_)
        ));
    }
}
