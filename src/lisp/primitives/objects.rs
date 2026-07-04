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

// GNU eieio-core.el represents each class slot as a `cl-slot-descriptor'
// record carrying the slot name, raw initform, type and an alist of the
// remaining slot options.  Rebuild that view from the plain slot specs
// stored on the class so `eieio--class-slots' consumers (object-write,
// eieio-persistent) observe GNU's descriptor protocol.
#[derive(Clone)]
pub(crate) struct EieioSlotDescriptor {
    pub(crate) name: String,
    pub(crate) initform: Option<Value>,
    pub(crate) slot_type: Value,
    pub(crate) props: Vec<(String, Value)>,
    pub(crate) initargs: Vec<String>,
    pub(crate) class_allocated: bool,
}

pub(crate) fn eieio_slot_descriptors(
    interp: &Interpreter,
    class_name: &str,
) -> Result<Vec<EieioSlotDescriptor>, LispError> {
    let mut descriptors = Vec::new();
    collect_eieio_slot_descriptors(interp, class_name, &mut descriptors)?;
    Ok(descriptors)
}

fn collect_eieio_slot_descriptors(
    interp: &Interpreter,
    class_name: &str,
    descriptors: &mut Vec<EieioSlotDescriptor>,
) -> Result<(), LispError> {
    if let Some(parents) = interp.get_symbol_property(class_name, "emaxx-class-parents")
        && let Ok(parent_values) = parents.to_vec()
    {
        for parent in parent_values {
            if let Ok(parent_name) = parent.as_symbol() {
                collect_eieio_slot_descriptors(interp, parent_name, descriptors)?;
            }
        }
    }
    let Some(raw_slots) = interp.get_symbol_property(class_name, "emaxx-class-slots") else {
        return Ok(());
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
        let mut descriptor = EieioSlotDescriptor {
            name: slot_name,
            initform: None,
            slot_type: Value::T,
            props: Vec::new(),
            initargs: Vec::new(),
            class_allocated: false,
        };
        let mut documentation = None;
        let mut custom = None;
        let mut label = None;
        let mut group = None;
        let mut printer = None;
        let mut protection = None;
        let mut index = 1usize;
        while index + 1 < parts.len() {
            let Ok(keyword) = parts[index].as_symbol() else {
                index += 1;
                continue;
            };
            let option_value = parts[index + 1].clone();
            match keyword {
                ":initarg" => {
                    if let Ok(initarg) = option_value.as_symbol() {
                        descriptor.initargs.push(initarg.to_string());
                    }
                }
                ":initform" => descriptor.initform = Some(option_value),
                ":type" => descriptor.slot_type = option_value,
                ":allocation" => {
                    descriptor.class_allocated = matches!(&option_value, Value::Symbol(value) if value == ":class" || value == "class");
                }
                ":documentation" => documentation = Some(option_value),
                ":custom" => custom = Some(option_value),
                ":label" => label = Some(option_value),
                ":group" => group = Some(option_value),
                ":printer" => printer = Some(option_value),
                ":protection" => protection = Some(option_value),
                _ => {}
            }
            index += 2;
        }
        // GNU defaults the custom group to `(default)' and normalizes the
        // protection symbol; public protection is dropped from the props.
        if custom.is_some() && group.is_none() {
            group = Some(Value::list([Value::Symbol("default".into())]));
        }
        if let Some(value) = &group
            && !matches!(value, Value::Cons(..) | Value::Nil)
        {
            group = Some(Value::list([value.clone()]));
        }
        let protection = protection.and_then(|value| match value.as_symbol().ok() {
            Some("protected" | ":protected") => Some(Value::Symbol("protected".into())),
            Some("private" | ":private") => Some(Value::Symbol("private".into())),
            _ => None,
        });
        for (key, value) in [
            (":documentation", documentation),
            (":custom", custom),
            (":label", label),
            (":group", group),
            (":printer", printer),
            (":protection", protection),
        ] {
            if let Some(value) = value {
                descriptor.props.push((key.to_string(), value));
            }
        }
        if let Some(existing) = descriptors
            .iter_mut()
            .find(|existing| existing.name == descriptor.name)
        {
            // A subclass re-declaring an inherited slot overrides its
            // defaults in place (GNU's `defaultoverride') while any new
            // initarg is added alongside the inherited ones.
            existing.initform = descriptor.initform;
            existing.slot_type = descriptor.slot_type;
            existing.props = descriptor.props;
            existing.class_allocated = descriptor.class_allocated;
            for initarg in descriptor.initargs {
                if !existing.initargs.contains(&initarg) {
                    existing.initargs.push(initarg);
                }
            }
        } else {
            descriptors.push(descriptor);
        }
    }
    Ok(())
}

pub(crate) fn eieio_slot_descriptor_record(
    interp: &mut Interpreter,
    env: &Env,
    descriptor: &EieioSlotDescriptor,
) -> Value {
    // GNU stores the raw initform, quoting it unless it is constant or a
    // function call evaluated at instantiation time (`eieio--eval-default-p');
    // a slot without an initform carries the quoted unbound marker.
    let initform = match &descriptor.initform {
        None => Value::list([
            Value::Symbol("quote".into()),
            Value::Symbol("eieio--unbound".into()),
        ]),
        Some(form) => {
            let head = match form {
                Value::Cons(car, _) => match &*car.borrow() {
                    Value::Symbol(symbol) => Some(symbol.clone()),
                    _ => None,
                },
                _ => None,
            };
            let constant = match form {
                Value::Cons(..) => matches!(
                    head.as_deref(),
                    Some("quote" | "function" | "vector-literal")
                ),
                Value::Symbol(symbol) => symbol.starts_with(':'),
                _ => true,
            };
            let evaluated = !constant
                && head.as_deref().is_some_and(|symbol| {
                    is_builtin(symbol)
                        || interp.lookup_function(symbol, env).is_ok()
                        || is_special_form_name(symbol)
                });
            if constant || evaluated {
                form.clone()
            } else {
                Value::list([Value::Symbol("quote".into()), form.clone()])
            }
        }
    };
    interp.create_record(
        "cl-slot-descriptor",
        vec![
            Value::Symbol(descriptor.name.clone()),
            initform,
            descriptor.slot_type.clone(),
            Value::list(
                descriptor
                    .props
                    .iter()
                    .map(|(key, value)| Value::cons(Value::Symbol(key.clone()), value.clone())),
            ),
        ],
    )
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
    // GNU's `make-instance' only downgrades the record tag from the class
    // object to the class symbol when `eieio-backward-compatibility' is
    // non-nil; otherwise the object keeps the class object as its tag and
    // raw-prints with the class expanded (and its circular default-object
    // cache marked).
    if interp
        .lookup_var("eieio-backward-compatibility", env)
        .is_some_and(|value| value.is_nil())
        && let Value::Record(record_id) = &record
    {
        interp.mark_class_object_tagged_record(*record_id);
    }
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
    // GNU `clone' copies the record, so the class-object tag travels with it.
    if interp.is_class_object_tagged_record(*record_id)
        && let Value::Record(clone_id) = &clone
    {
        interp.mark_class_object_tagged_record(*clone_id);
    }
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
