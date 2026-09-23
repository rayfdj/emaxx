use super::*;
use crate::lisp::eval::RecordKind;
use crate::lisp::types::Kind;

pub(crate) fn copy_sequence_value(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    if matches!(value.kind(), Kind::Record(_))
        && let Some(public) = runtime_keymap_public_view(interp, value)
    {
        return copy_sequence_value(interp, &public);
    }
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

    match value.kind() {
        Kind::Nil => Ok(Value::Nil),
        Kind::Cons(_) => Ok(Value::list(value.to_vec()?)),
        Kind::CharTable(id) => interp.clone_char_table(id),
        Kind::Record(id)
            if interp.find_record(id).is_some_and(|record| {
                matches!(record.kind, RecordKind::Record | RecordKind::BoolVector)
            }) =>
        {
            interp.copy_record(id.id)
        }
        _ => Err(LispError::WrongTypeArgument("sequencep".into(), *value)),
    }
}

pub(crate) enum SortSequenceKind {
    List,
    Vector(String),
}

pub(crate) fn list_or_vector_type_error(value: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::Symbol("list-or-vector-p".into()),
        *value,
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
    if matches!(value.kind(), Kind::Nil | Kind::Cons(_)) {
        return Ok((SortSequenceKind::List, value.to_vec()?));
    }
    Err(list_or_vector_type_error(value))
}

/// sort.c:resolve_fun resolves a function cell once, but preserves an
/// autoload's symbol so the first ordinary call can load its definition.
fn resolve_sort_function(
    interp: &mut Interpreter,
    function: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if !function.is_symbol() {
        return Ok(function);
    }
    let resolved = call(interp, "indirect-function", &[function], env)?;
    Ok(
        if resolved.is_nil() || autoload_parts(&resolved).is_some() {
            function
        } else {
            resolved
        },
    )
}

fn sort_compare_ordering_resolved(
    interp: &mut Interpreter,
    predicate: Value,
    left: &Value,
    right: &Value,
    env: &mut Env,
) -> Result<Ordering, LispError> {
    let less = if predicate.is_nil() {
        value_less(interp, left, right, env)?
    } else {
        call_function_value(interp, &predicate, &[*left, *right], env)?.is_truthy()
    };
    if less {
        return Ok(Ordering::Less);
    }
    let greater = if predicate.is_nil() {
        value_less(interp, right, left, env)?
    } else {
        call_function_value(interp, &predicate, &[*right, *left], env)?.is_truthy()
    };
    Ok(if greater {
        Ordering::Greater
    } else {
        Ordering::Equal
    })
}

pub(crate) fn sort_sequence_items(
    interp: &mut Interpreter,
    mut items: Vec<Value>,
    key: Option<&Value>,
    lessp: Option<&Value>,
    reverse: bool,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    // sort_list/sort_vector do not invoke callbacks for fewer than two elements.
    if items.len() < 2 {
        return Ok(items);
    }
    let predicate = match lessp.copied().unwrap_or(Value::Nil) {
        value
            if value.is_nil() || matches!(value.kind(), Kind::Symbol(name) if name == "value<") =>
        {
            Value::Nil
        }
        value => resolve_sort_function(interp, value, env)?,
    };
    // GNU reverses before key computation, then reverses the result to keep
    // equal keys stable. Callback order is observable even for pure keys.
    if reverse {
        items.reverse();
    }
    let key_function = match key.copied().unwrap_or(Value::Nil) {
        value
            if value.is_nil()
                || matches!(value.kind(), Kind::Symbol(name) if name == "identity") =>
        {
            Value::Nil
        }
        value => resolve_sort_function(interp, value, env)?,
    };
    let functions = (predicate, key_function);
    interp.with_lisp_stack_roots(&functions, |interp| {
        // The original values and each computed key remain live while a
        // callback runs GC. Never substitute a parsed comparator body.
        let mut keyed: crate::lisp::alloc::RootedVec<_> =
            items.into_iter().map(|item| (item, Value::Nil)).collect();
        for index in 0..keyed.len() {
            let item = keyed[index].0;
            let sort_key = if key_function.is_nil() {
                item
            } else {
                call_function_value(interp, &key_function, &[item], env)?
            };
            keyed[index].1 = sort_key;
        }
        let mut error = None;
        // Keep Lisp values in the rooted storage while Rust sorts indices.
        // This still uses Rust's comparison schedule; matching GNU's Timsort
        // schedule is a separate outstanding compatibility requirement.
        let mut order: Vec<usize> = (0..keyed.len()).collect();
        order.sort_by(|&left, &right| {
            if error.is_some() {
                return Ordering::Equal;
            }
            match sort_compare_ordering_resolved(
                interp,
                predicate,
                &keyed[left].1,
                &keyed[right].1,
                env,
            ) {
                Ok(ordering) => ordering,
                Err(err) => {
                    error = Some(err);
                    Ordering::Equal
                }
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        let mut sorted: Vec<Value> = order.iter().map(|&index| keyed[index].0).collect();
        if reverse {
            sorted.reverse();
        }
        Ok(sorted)
    })
}

pub(crate) fn write_sorted_sequence(
    target: &Value,
    kind: &SortSequenceKind,
    items: &[Value],
) -> Result<(), LispError> {
    match kind {
        SortSequenceKind::List => {
            let mut current = *target;
            for item in items {
                match current.kind() {
                    Kind::Cons(cons_cell) => {
                        let car = &cons_cell.car;
                        let cdr = &cons_cell.cdr;
                        car.set(*item);
                        current = cdr.get();
                    }
                    Kind::Nil => break,
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
        aset_vector_value(target, index, *item)?;
    }

    Ok(())
}
