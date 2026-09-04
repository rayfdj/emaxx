use super::*;

pub(crate) fn copy_sequence_value(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    if let Some(string) = string_like(value) {
        return Ok(make_shared_string_value_with_multibyte(
            string.text,
            string.props,
            string.multibyte,
        ));
    }

    if is_vector_value(value) {
        return Ok(Value::list(value.to_vec()?));
    }

    match value {
        Value::Nil => Ok(Value::Nil),
        Value::Cons(_) => {
            let Some((car, cdr)) = value.cons_values() else {
                return Ok(value.clone());
            };
            Ok(Value::cons(car, copy_sequence_value(interp, &cdr)?))
        }
        Value::CharTable(id) => interp.clone_char_table(*id),
        Value::Record(id) => interp.copy_record(*id),
        _ => Ok(value.clone()),
    }
}

pub(crate) fn exit_status_code(status: &std::process::ExitStatus) -> i64 {
    status
        .code()
        .unwrap_or(if status.success() { 0 } else { 1 }) as i64
}

pub(crate) fn default_sort_lt(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    let left_marker = if let Value::Marker(id) = left {
        interp
            .marker_position(*id)
            .or_else(|| interp.marker_last_position(*id))
    } else {
        None
    };
    let right_marker = if let Value::Marker(id) = right {
        interp
            .marker_position(*id)
            .or_else(|| interp.marker_last_position(*id))
    } else {
        None
    };
    if let (Some(left), Some(right)) = (left_marker, right_marker) {
        return Ok(left < right);
    }
    if let (Ok(left), Ok(right)) = (left.as_integer(), right.as_integer()) {
        return Ok(left < right);
    }
    if let (Some(left), Some(right)) = (string_like(left), string_like(right)) {
        return Ok(left.text < right.text);
    }
    Ok(left.to_string() < right.to_string())
}

pub(crate) enum SortSequenceKind {
    List,
    Vector(String),
}

pub(crate) fn list_or_vector_type_error(value: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::Symbol("list-or-vector-p".into()),
        value.clone(),
    ]))
}

pub(crate) fn sort_sequence_kind_and_items(
    value: &Value,
) -> Result<(SortSequenceKind, Vec<Value>), LispError> {
    if is_vector_value(value) {
        let items = value.to_vec()?;
        let tag = items
            .first()
            .and_then(|value| value.as_symbol().ok())
            .unwrap_or("vector")
            .to_string();
        return Ok((
            SortSequenceKind::Vector(tag),
            items.into_iter().skip(1).collect(),
        ));
    }
    if matches!(value, Value::Nil | Value::Cons(_)) {
        return Ok((SortSequenceKind::List, value.to_vec()?));
    }
    Err(list_or_vector_type_error(value))
}

fn sort_compare_ordering_resolved(
    interp: &mut Interpreter,
    lessp: Option<&Value>,
    direct: Option<&DirectSortComparator>,
    left: &Value,
    right: &Value,
    env: &mut Env,
) -> Result<Ordering, LispError> {
    if let Some(comparator) = direct {
        return apply_direct_sort_comparator(interp, comparator, left, right, env);
    }

    let left_lt_right = if let Some(function) = lessp {
        call_function_value(interp, function, &[left.clone(), right.clone()], env)?.is_truthy()
    } else {
        default_sort_lt(interp, left, right)?
    };
    if left_lt_right {
        return Ok(Ordering::Less);
    }

    let right_lt_left = if let Some(function) = lessp {
        call_function_value(interp, function, &[right.clone(), left.clone()], env)?.is_truthy()
    } else {
        default_sort_lt(interp, right, left)?
    };
    Ok(if right_lt_left {
        Ordering::Greater
    } else {
        Ordering::Equal
    })
}

pub(crate) enum DirectSortKind {
    Less,
    Greater,
    ValueLess,
}

pub(crate) enum DirectSortKeyFn {
    Abs,
}

pub(crate) enum DirectSortOperand {
    Left,
    Right,
    LeftCar,
    RightCar,
    LeftAbs,
    RightAbs,
}

pub(crate) struct DirectSortComparator {
    prelude: Vec<Value>,
    kind: DirectSortKind,
    left: DirectSortOperand,
    right: DirectSortOperand,
}

pub(crate) fn resolve_direct_sort_kind(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<DirectSortKind> {
    match value {
        Value::Symbol(name) | Value::BuiltinFunc(name) => match name.as_str() {
            "<" => Some(DirectSortKind::Less),
            ">" => Some(DirectSortKind::Greater),
            "value<" => Some(DirectSortKind::ValueLess),
            _ => interp
                .lookup_var(name, env)
                .and_then(|resolved| resolve_direct_sort_kind(interp, &resolved, env)),
        },
        _ => None,
    }
}

pub(crate) fn resolve_direct_sort_key_fn(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<DirectSortKeyFn> {
    match value {
        Value::Symbol(name) | Value::BuiltinFunc(name) => match name.as_str() {
            "abs" => Some(DirectSortKeyFn::Abs),
            _ => interp
                .lookup_var(name, env)
                .and_then(|resolved| resolve_direct_sort_key_fn(interp, &resolved, env)),
        },
        _ => None,
    }
}

pub(crate) fn parse_direct_sort_operand(
    interp: &Interpreter,
    value: &Value,
    params: &[String],
    env: &Env,
) -> Option<DirectSortOperand> {
    match value {
        Value::Symbol(symbol) if symbol == &params[0] => Some(DirectSortOperand::Left),
        Value::Symbol(symbol) if symbol == &params[1] => Some(DirectSortOperand::Right),
        Value::Cons(_) => {
            let items = value.to_vec().ok()?;
            match items.as_slice() {
                [Value::Symbol(name), Value::Symbol(symbol)]
                    if name == "car" && symbol == &params[0] =>
                {
                    Some(DirectSortOperand::LeftCar)
                }
                [Value::Symbol(name), Value::Symbol(symbol)]
                    if name == "car" && symbol == &params[1] =>
                {
                    Some(DirectSortOperand::RightCar)
                }
                [Value::Symbol(name), key, Value::Symbol(symbol)]
                    if name == "funcall" && symbol == &params[0] =>
                {
                    match resolve_direct_sort_key_fn(interp, key, env)? {
                        DirectSortKeyFn::Abs => Some(DirectSortOperand::LeftAbs),
                    }
                }
                [Value::Symbol(name), key, Value::Symbol(symbol)]
                    if name == "funcall" && symbol == &params[1] =>
                {
                    match resolve_direct_sort_key_fn(interp, key, env)? {
                        DirectSortKeyFn::Abs => Some(DirectSortOperand::RightAbs),
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

pub(crate) fn direct_sort_comparator(
    interp: &Interpreter,
    function: &Value,
    env: &Env,
) -> Option<DirectSortComparator> {
    match function {
        Value::Symbol(name) | Value::BuiltinFunc(name) if name == "car-less-than-car" => {
            Some(DirectSortComparator {
                prelude: Vec::new(),
                kind: DirectSortKind::Less,
                left: DirectSortOperand::LeftCar,
                right: DirectSortOperand::RightCar,
            })
        }
        Value::Symbol(_) | Value::BuiltinFunc(_) => {
            let kind = resolve_direct_sort_kind(interp, function, env)?;
            Some(DirectSortComparator {
                prelude: Vec::new(),
                kind,
                left: DirectSortOperand::Left,
                right: DirectSortOperand::Right,
            })
        }
        Value::Lambda(lambda) if lambda.params.len() == 2 && !lambda.body.is_empty() => {
            let closure_env = lambda.env.borrow().clone();
            let compare_form = lambda.body.last()?;
            let items = compare_form.to_vec().ok()?;
            let (kind, left, right) = match items.as_slice() {
                [Value::Symbol(op), left, right] => {
                    let kind =
                        resolve_direct_sort_kind(interp, &Value::Symbol(op.clone()), &closure_env)?;
                    (
                        kind,
                        parse_direct_sort_operand(interp, left, &lambda.params, &closure_env)?,
                        parse_direct_sort_operand(interp, right, &lambda.params, &closure_env)?,
                    )
                }
                [Value::Symbol(name), function, left, right] if name == "funcall" => {
                    let kind = resolve_direct_sort_kind(interp, function, &closure_env)?;
                    (
                        kind,
                        parse_direct_sort_operand(interp, left, &lambda.params, &closure_env)?,
                        parse_direct_sort_operand(interp, right, &lambda.params, &closure_env)?,
                    )
                }
                _ => return None,
            };
            Some(DirectSortComparator {
                prelude: lambda.body[..lambda.body.len() - 1].to_vec(),
                kind,
                left,
                right,
            })
        }
        _ => None,
    }
}

pub(crate) fn resolve_direct_sort_operand(
    operand: &DirectSortOperand,
    left: &Value,
    right: &Value,
) -> Result<Value, LispError> {
    match operand {
        DirectSortOperand::Left => Ok(left.clone()),
        DirectSortOperand::Right => Ok(right.clone()),
        DirectSortOperand::LeftCar => left.car(),
        DirectSortOperand::RightCar => right.car(),
        DirectSortOperand::LeftAbs => direct_sort_abs_value(left),
        DirectSortOperand::RightAbs => direct_sort_abs_value(right),
    }
}

pub(crate) fn direct_sort_abs_value(value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Integer(number) => match number.checked_abs() {
            Some(abs) => Ok(Value::Integer(abs)),
            None => Ok(normalize_bigint_value(BigInt::from(*number).abs())),
        },
        Value::BigInteger(number) => Ok(normalize_bigint_value(number.abs())),
        Value::Float(number) => Ok(Value::float(number.abs())),
        _ => Err(LispError::WrongTypeArgument(
            "numberp".into(),
            value.clone(),
        )),
    }
}

/// Apply an already-recognized comparator; `sort' resolves it once for
/// the whole sequence instead of re-parsing the predicate per
/// comparison (n log n times).
pub(crate) fn apply_direct_sort_comparator(
    interp: &mut Interpreter,
    comparator: &DirectSortComparator,
    left: &Value,
    right: &Value,
    env: &mut Env,
) -> Result<Ordering, LispError> {
    for form in &comparator.prelude {
        let _ = interp.eval(form, env)?;
    }

    let resolved_left = resolve_direct_sort_operand(&comparator.left, left, right)?;
    let resolved_right = resolve_direct_sort_operand(&comparator.right, left, right)?;

    let ordering = match comparator.kind {
        DirectSortKind::Less => {
            if default_sort_lt(interp, &resolved_left, &resolved_right)? {
                Ordering::Less
            } else if default_sort_lt(interp, &resolved_right, &resolved_left)? {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        DirectSortKind::Greater => {
            if default_sort_lt(interp, &resolved_right, &resolved_left)? {
                Ordering::Less
            } else if default_sort_lt(interp, &resolved_left, &resolved_right)? {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        DirectSortKind::ValueLess => {
            if value_less(interp, &resolved_left, &resolved_right, env)? {
                Ordering::Less
            } else if value_less(interp, &resolved_right, &resolved_left, env)? {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
    };
    Ok(ordering)
}

pub(crate) fn sort_sequence_items(
    interp: &mut Interpreter,
    items: Vec<Value>,
    key: Option<&Value>,
    lessp: Option<&Value>,
    reverse: bool,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut keyed = Vec::with_capacity(items.len());
    for item in items {
        let sort_key = if let Some(function) = key {
            call_function_value(interp, function, std::slice::from_ref(&item), env)?
        } else {
            item.clone()
        };
        keyed.push((item, sort_key));
    }

    if reverse {
        keyed.reverse();
    }

    let direct = lessp.and_then(|function| direct_sort_comparator(interp, function, env));
    let mut error = None;
    keyed.sort_by(|(_, left_key), (_, right_key)| {
        if let Some(existing) = &error {
            let _ = existing;
            return Ordering::Equal;
        }
        match sort_compare_ordering_resolved(
            interp,
            lessp,
            direct.as_ref(),
            left_key,
            right_key,
            env,
        ) {
            Ok(ordering) => ordering,
            Err(err) => {
                error = Some(err);
                Ordering::Equal
            }
        }
    });
    if let Some(err) = error {
        return Err(err);
    }

    if reverse {
        keyed.reverse();
    }

    Ok(keyed.into_iter().map(|(item, _)| item).collect())
}

pub(crate) fn write_sorted_sequence(
    target: &Value,
    kind: &SortSequenceKind,
    items: &[Value],
) -> Result<(), LispError> {
    match kind {
        SortSequenceKind::List => {
            let mut current = target.clone();
            for item in items {
                match current {
                    Value::Cons(cons_cell) => {
                        let car = &cons_cell.car;
                        let cdr = &cons_cell.cdr;
                        *car.borrow_mut() = item.clone();
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => break,
                    _ => return Err(list_or_vector_type_error(target)),
                }
            }
            Ok(())
        }
        SortSequenceKind::Vector(_) => write_vector_items_in_place(target, items),
    }
}

pub(crate) fn build_sorted_sequence(kind: &SortSequenceKind, items: Vec<Value>) -> Value {
    match kind {
        SortSequenceKind::List => Value::list(items),
        SortSequenceKind::Vector(tag) => {
            Value::list(std::iter::once(Value::Symbol(tag.clone().into())).chain(items))
        }
    }
}

pub(crate) fn write_vector_items_in_place(
    target: &Value,
    items: &[Value],
) -> Result<(), LispError> {
    if !is_vector_value(target) {
        return Err(list_or_vector_type_error(target));
    }

    if vector_items(target)?.len() != items.len() {
        return Err(LispError::Signal("Args out of range".into()));
    }

    for (index, item) in items.iter().enumerate() {
        aset_vector_value(target, index, item.clone())?;
    }

    Ok(())
}
