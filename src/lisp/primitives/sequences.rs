use super::*;
use crate::lisp::types::{BoolVectorRef, Kind};

mod timsort;

pub(crate) fn copy_sequence_value(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    if matches!(value.kind(), Kind::StringObject(_)) {
        return shared_string_copy(value);
    }

    if is_vector_value(value) {
        return Ok(Value::list(value.to_vec()?));
    }

    match value.kind() {
        Kind::Nil => Ok(Value::Nil),
        Kind::Cons(_) => Ok(Value::list(value.to_vec()?)),
        Kind::CharTable(id) => interp.clone_char_table(id),
        Kind::LispRecord(record) => Ok(Value::LispRecord(record.shallow_copy())),
        // fns.c:Fcopy_sequence: a new bool vector of the same bits.
        Kind::BoolVector(vector) => Ok(Value::BoolVector(BoolVectorRef::copy(vector))),
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
            .and_then(|value| value.symbol_str())
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

/// sort.c's `inorder': whether LEFT < RIGHT by PREDICATE (`value<' for
/// nil).
fn sort_inorder(
    interp: &mut Interpreter,
    predicate: Value,
    left: Value,
    right: Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    if predicate.is_nil() {
        value_less(interp, &left, &right, env)
    } else {
        Ok(call_function_value(interp, &predicate, &[left, right], env)?.is_truthy())
    }
}

/// A signal from a sort's resolution, key function or predicate, with the
/// items in the order the sort had left them once it began to move them:
/// sort.c works in a vector's own slots, so a vector sorted in place keeps
/// that order, while a list is sorted in a copy (fns.c:sort_list).
pub(crate) struct SortFailure {
    pub(crate) error: LispError,
    pub(crate) items: Option<Vec<Value>>,
}

impl From<LispError> for SortFailure {
    fn from(error: LispError) -> Self {
        Self { error, items: None }
    }
}

pub(crate) fn sort_sequence_items(
    interp: &mut Interpreter,
    mut items: Vec<Value>,
    key: Option<&Value>,
    lessp: Option<&Value>,
    reverse: bool,
    env: &mut Env,
) -> Result<Vec<Value>, SortFailure> {
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
        value => match resolve_sort_function(interp, value, env) {
            Ok(function) => function,
            Err(error) => {
                return Err(SortFailure {
                    error,
                    items: Some(items),
                });
            }
        },
    };
    let functions = (predicate, key_function);
    interp.with_lisp_stack_roots(&functions, |interp| {
        // The original values and each computed key remain live while a
        // callback runs GC. Never substitute a parsed comparator body.
        let mut keyed: crate::lisp::alloc::RootedVec<_> =
            items.into_iter().map(|item| (item, Value::Nil)).collect();
        let mut result = Ok(());
        for index in 0..keyed.len() {
            if !key_function.is_nil() {
                let item = keyed[index].0;
                match call_function_value(interp, &key_function, &[item], env) {
                    Ok(sort_key) => keyed[index].1 = sort_key,
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                }
            } else {
                keyed[index].1 = keyed[index].0;
            }
        }
        // GNU's Timsort, so the predicate sees GNU's comparisons in GNU's
        // order; the entries stay in the rooted storage as they move.
        if result.is_ok() {
            result = timsort::tim_sort(&mut keyed, !key_function.is_nil(), |left, right| {
                sort_inorder(interp, predicate, left, right, env)
            });
        }
        let mut sorted: Vec<Value> = keyed.iter().map(|&(item, _)| item).collect();
        match result {
            Ok(()) => {
                if reverse {
                    sorted.reverse();
                }
                Ok(sorted)
            }
            Err(error) => Err(SortFailure {
                error,
                items: Some(sorted),
            }),
        }
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
