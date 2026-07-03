use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

// Nested advice wrappers execute with their caller's frames visible, so each
// wrapper needs unique capture names or an inner wrapper resolves the outer
// wrapper's captured original/advice values.
static ADVICE_WRAPPER_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn make_advice_wrapper_after(original: Value, advice: Value) -> Value {
    let unique = ADVICE_WRAPPER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let args_name = format!("__emaxx-advice-after-args-{unique}");
    let original_name = format!("__emaxx-advice-after-original-{unique}");
    let advice_name = format!("__emaxx-advice-after-function-{unique}");
    Value::Lambda(
        vec!["&rest".into(), args_name.clone()],
        vec![
            Value::Symbol(":closure-transparent-env".into()),
            Value::list([
                Value::Symbol("emaxx-apply-after-advice".into()),
                Value::Symbol(original_name.clone()),
                Value::Symbol(advice_name.clone()),
                Value::Symbol(args_name),
            ]),
        ],
        shared_env(vec![vec![(original_name, original), (advice_name, advice)]]),
    )
}

pub(crate) fn make_advice_wrapper_around(original: Value, advice: Value) -> Value {
    let unique = ADVICE_WRAPPER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let args_name = format!("__emaxx-advice-around-args-{unique}");
    let original_name = format!("__emaxx-advice-around-original-{unique}");
    let advice_name = format!("__emaxx-advice-around-function-{unique}");
    Value::Lambda(
        vec!["&rest".into(), args_name.clone()],
        vec![
            Value::Symbol(":closure-transparent-env".into()),
            Value::list([
                Value::Symbol("emaxx-apply-around-advice".into()),
                Value::Symbol(original_name.clone()),
                Value::Symbol(advice_name.clone()),
                Value::Symbol(args_name),
            ]),
        ],
        shared_env(vec![vec![(original_name, original), (advice_name, advice)]]),
    )
}

pub(crate) fn wrap_advice(where_sym: &str, original: Value, advice: Value) -> Option<Value> {
    match where_sym {
        ":override" => Some(advice),
        ":after" => Some(make_advice_wrapper_after(original, advice)),
        ":around" => Some(make_advice_wrapper_around(original, advice)),
        _ => None,
    }
}

pub(crate) fn wait_duration(args: &[Value]) -> Result<Duration, LispError> {
    let seconds = args
        .first()
        .map(Value::as_float)
        .transpose()?
        .unwrap_or(0.0);
    let millis = args.get(1).map(Value::as_float).transpose()?.unwrap_or(0.0);
    let total = seconds + millis / 1000.0;
    if !total.is_finite() || total <= 0.0 {
        return Ok(Duration::ZERO);
    }
    Ok(Duration::from_secs_f64(total.min(1.0)))
}

#[derive(Clone)]
pub(crate) struct EieioSlotSpec {
    pub(crate) name: String,
    pub(crate) initargs: Vec<String>,
    pub(crate) initform: Option<Value>,
    pub(crate) class_allocated: bool,
}

pub(crate) fn eieio_slot_specs(
    interp: &Interpreter,
    class_name: &str,
) -> Result<Vec<EieioSlotSpec>, LispError> {
    let mut slots = Vec::new();
    if let Some(parents) = interp.get_symbol_property(class_name, "emaxx-class-parents")
        && let Ok(parent_values) = parents.to_vec()
    {
        for parent in parent_values {
            if let Ok(parent_name) = parent.as_symbol() {
                slots.extend(eieio_slot_specs(interp, parent_name)?);
            }
        }
    }

    let Some(raw_slots) = interp.get_symbol_property(class_name, "emaxx-class-slots") else {
        return Ok(slots);
    };
    for raw_slot in raw_slots.to_vec()? {
        let parts = raw_slot.to_vec()?;
        let Some(slot_name) = parts
            .first()
            .and_then(|value| value.as_symbol().ok())
            .map(str::to_string)
        else {
            continue;
        };
        let mut initargs = Vec::new();
        let mut initform = None;
        let mut class_allocated = false;
        let mut index = 1usize;
        while index + 1 < parts.len() {
            let Some(keyword) = parts[index].as_symbol().ok() else {
                index += 1;
                continue;
            };
            match keyword {
                ":initarg" => {
                    if let Ok(initarg) = parts[index + 1].as_symbol() {
                        initargs.push(initarg.to_string());
                    }
                }
                ":initform" => initform = Some(parts[index + 1].clone()),
                ":allocation" => {
                    class_allocated = matches!(&parts[index + 1], Value::Symbol(value) if value == ":class" || value == "class");
                }
                _ => {}
            }
            index += 2;
        }
        slots.push(EieioSlotSpec {
            name: slot_name,
            initargs,
            initform,
            class_allocated,
        });
    }
    Ok(slots)
}

pub(crate) fn eieio_slot_index(slots: &[EieioSlotSpec], slot_name: &str) -> Option<usize> {
    slots.iter().rposition(|slot| {
        slot.name == slot_name || slot.initargs.iter().any(|initarg| initarg == slot_name)
    })
}

fn eieio_class_default_property(slot_name: &str) -> String {
    format!("emaxx-class-default:{slot_name}")
}

pub(crate) fn make_eieio_instance(
    interp: &mut Interpreter,
    class_name: &str,
    initargs: &[Value],
    skip_obsolete_name: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let slots = eieio_slot_specs(interp, class_name)?;
    let mut values = Vec::with_capacity(slots.len());
    for slot in &slots {
        values.push(match &slot.initform {
            Some(initform) => interp.eval(initform, env)?,
            None => Value::Unbound,
        });
    }

    let mut index = 0usize;
    if skip_obsolete_name
        && matches!(
            initargs.first(),
            Some(Value::Nil | Value::String(_) | Value::StringObject(_))
        )
    {
        if let Some(name) = initargs.first()
            && let Some(slot_index) = slots.iter().rposition(|slot| slot.name == "object-name")
        {
            values[slot_index] = name.clone();
        }
        index = 1;
    }

    while index + 1 < initargs.len() {
        let initarg = initargs[index].as_symbol()?;
        if !initarg.starts_with(':') {
            return Err(LispError::Signal(format!("Invalid initarg {initarg}")));
        }
        if let Some(slot_index) = slots
            .iter()
            .rposition(|slot| slot.initargs.iter().any(|candidate| candidate == initarg))
        {
            values[slot_index] = initargs[index + 1].clone();
        }
        index += 2;
    }

    let record = interp.create_record(class_name, values.clone());
    if let Some(index) = slots
        .iter()
        .rposition(|slot| slot.name == "tracking-symbol")
        && let Some(Value::Symbol(symbol)) = values.get(index)
    {
        let current = interp.lookup_var(symbol, env).unwrap_or(Value::Nil);
        let already_tracked = current
            .to_vec()
            .is_ok_and(|items| items.iter().any(|item| item == &record));
        if !already_tracked {
            interp.set_global_binding(symbol, Value::cons(record.clone(), current));
        }
    }
    Ok(record)
}

pub(crate) fn eieio_slot_value(
    interp: &Interpreter,
    object: &Value,
    slot_name: &str,
) -> Result<Value, LispError> {
    let Value::Record(record_id) = object else {
        return Err(LispError::TypeError(
            "eieio-object".into(),
            object.type_name(),
        ));
    };
    let record = interp
        .find_record(*record_id)
        .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{record_id}>")))?;
    let slots = eieio_slot_specs(interp, &record.type_name)?;
    let Some(slot_index) = eieio_slot_index(&slots, slot_name) else {
        return Err(LispError::Signal(format!(
            "Invalid slot name: {slot_name} (class {})",
            record.type_name
        )));
    };
    if slots[slot_index].class_allocated
        && let Some(value) =
            interp.get_symbol_property(&record.type_name, &eieio_class_default_property(slot_name))
    {
        return Ok(value);
    }
    match record.slots.get(slot_index) {
        Some(Value::Unbound) | None => Err(LispError::SignalValue(Value::list([
            Value::Symbol("unbound-slot".into()),
            Value::String(format!("Unbound slot: {slot_name}")),
            object.clone(),
            Value::Symbol(slot_name.into()),
        ]))),
        Some(value) => Ok(value.clone()),
    }
}

pub(crate) fn clone_eieio_instance(
    interp: &mut Interpreter,
    object: &Value,
    params: &[Value],
) -> Result<Value, LispError> {
    let Value::Record(record_id) = object else {
        return Err(LispError::TypeError(
            "eieio-object".into(),
            object.type_name(),
        ));
    };
    let clone = interp.copy_record(*record_id)?;
    let mut index = 0usize;
    if matches!(
        params.first(),
        Some(Value::Nil | Value::String(_) | Value::StringObject(_))
    ) {
        index = 1;
    }
    while index + 1 < params.len() {
        let initarg = params[index].as_symbol()?;
        if !initarg.starts_with(':') {
            return Err(LispError::Signal(format!("Invalid initarg {initarg}")));
        }
        set_eieio_slot_value(interp, &clone, initarg, params[index + 1].clone())?;
        index += 2;
    }
    Ok(clone)
}

pub(crate) fn set_eieio_slot_value(
    interp: &mut Interpreter,
    object: &Value,
    slot_name: &str,
    value: Value,
) -> Result<Value, LispError> {
    let Value::Record(record_id) = object else {
        return Err(LispError::TypeError(
            "eieio-object".into(),
            object.type_name(),
        ));
    };
    let type_name = interp
        .find_record(*record_id)
        .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{record_id}>")))?
        .type_name
        .clone();
    let slots = eieio_slot_specs(interp, &type_name)?;
    let Some(slot_index) = eieio_slot_index(&slots, slot_name) else {
        return Err(LispError::Signal(format!(
            "Invalid slot name: {slot_name} (class {type_name})"
        )));
    };
    if slots[slot_index].class_allocated {
        interp.put_symbol_property(
            &type_name,
            &eieio_class_default_property(slot_name),
            value.clone(),
        );
        return Ok(value);
    }
    let record = interp
        .find_record_mut(*record_id)
        .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{record_id}>")))?;
    if record.slots.len() <= slot_index {
        record.slots.resize(slot_index + 1, Value::Unbound);
    }
    record.slots[slot_index] = value.clone();
    Ok(value)
}
