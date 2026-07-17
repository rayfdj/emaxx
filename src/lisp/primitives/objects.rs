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
    Ok(Duration::from_secs_f64(total))
}

#[derive(Clone)]
pub(crate) struct EieioSlotSpec {
    pub(crate) name: String,
    pub(crate) initargs: Vec<String>,
    pub(crate) initform: Option<Value>,
    pub(crate) slot_type: Value,
    pub(crate) class_allocated: bool,
}

pub(crate) fn eieio_slot_specs(
    interp: &Interpreter,
    class_name: &str,
) -> Result<Vec<EieioSlotSpec>, LispError> {
    Ok(eieio_slot_descriptors(interp, class_name)?
        .into_iter()
        .map(|descriptor| EieioSlotSpec {
            name: descriptor.name,
            initargs: descriptor.initargs,
            initform: descriptor.initform,
            slot_type: descriptor.slot_type,
            class_allocated: descriptor.class_allocated,
        })
        .collect())
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
    let mut seen = Vec::new();
    collect_merged_eieio_slot_descriptors(interp, class_name, &mut descriptors, &mut seen)?;
    Ok(descriptors)
}

// GNU stores each class's slots already merged: a class's own
// redeclarations override slots inherited from its OWN ancestors, and a
// subclass copies each parent's merged view first-parent-wins (only
// initargs accumulate across same-named slots from later parents).
fn collect_merged_eieio_slot_descriptors(
    interp: &Interpreter,
    class_name: &str,
    descriptors: &mut Vec<EieioSlotDescriptor>,
    seen: &mut Vec<String>,
) -> Result<(), LispError> {
    if seen.iter().any(|name| name == class_name) {
        return Ok(());
    }
    seen.push(class_name.to_string());
    if let Some(parents) = interp.get_symbol_property(class_name, "emaxx-class-parents")
        && let Ok(parent_values) = parents.to_vec()
    {
        for parent in parent_values {
            if let Ok(parent_name) = parent.as_symbol() {
                let mut parent_descriptors = Vec::new();
                let mut parent_seen = seen.clone();
                collect_merged_eieio_slot_descriptors(
                    interp,
                    parent_name,
                    &mut parent_descriptors,
                    &mut parent_seen,
                )?;
                for descriptor in parent_descriptors {
                    merge_eieio_slot_descriptor(descriptors, descriptor, false);
                }
            }
        }
    }
    collect_eieio_slot_descriptors(interp, class_name, descriptors, true)
}

fn collect_eieio_slot_descriptors(
    interp: &Interpreter,
    class_name: &str,
    descriptors: &mut Vec<EieioSlotDescriptor>,
    own_class: bool,
) -> Result<(), LispError> {
    let Some(raw_slots) = interp.get_symbol_property(class_name, "emaxx-class-slots") else {
        return Ok(());
    };
    for raw_slot in raw_slots.to_vec()? {
        // cl-defstruct classes store bare slot-name symbols.
        let parts = match &raw_slot {
            Value::Symbol(slot_name) => vec![Value::Symbol(slot_name.clone())],
            _ => raw_slot.to_vec()?,
        };
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
        merge_eieio_slot_descriptor(descriptors, descriptor, own_class);
    }
    Ok(())
}

fn merge_eieio_slot_descriptor(
    descriptors: &mut Vec<EieioSlotDescriptor>,
    descriptor: EieioSlotDescriptor,
    with_override: bool,
) {
    let Some(existing) = descriptors
        .iter_mut()
        .find(|existing| existing.name == descriptor.name)
    else {
        descriptors.push(descriptor);
        return;
    };
    // GNU pushes the redeclaration's initarg alongside the inherited ones
    // in both phases, but only the class's OWN redeclarations override the
    // inherited attributes (`eieio--slot-override'); a later parent's
    // same-named slot leaves the first parent's definition in place.
    for initarg in descriptor.initargs {
        if !existing.initargs.contains(&initarg) {
            existing.initargs.push(initarg);
        }
    }
    if with_override {
        // A redeclared slot keeps its inherited allocation and, when the
        // redeclaration has no initform, its inherited default; a `t' type
        // keeps the inherited type.
        if descriptor.initform.is_some() {
            existing.initform = descriptor.initform;
        }
        if !matches!(descriptor.slot_type, Value::T) {
            existing.slot_type = descriptor.slot_type;
        }
        for (key, value) in descriptor.props {
            if key == ":group" {
                // Custom groups combine across the hierarchy.
                let mut combined = value.to_vec().unwrap_or_default();
                if let Some((_, existing_group)) = existing
                    .props
                    .iter()
                    .find(|(existing_key, _)| existing_key == ":group")
                {
                    for member in existing_group.to_vec().unwrap_or_default() {
                        if !combined.iter().any(|candidate| candidate == &member) {
                            combined.push(member);
                        }
                    }
                }
                let combined = Value::list(combined);
                match existing
                    .props
                    .iter_mut()
                    .find(|(existing_key, _)| existing_key == ":group")
                {
                    Some(slot) => slot.1 = combined,
                    None => existing.props.push((key, combined)),
                }
            } else {
                match existing
                    .props
                    .iter_mut()
                    .find(|(existing_key, _)| existing_key == &key)
                {
                    Some(slot) => slot.1 = value,
                    None => existing.props.push((key, value)),
                }
            }
        }
    }
}

// GNU signals `invalid-slot-type' when a constant initform does not match
// the slot's :type, and plain errors when a subclass redeclares an
// inherited slot with a different type or protection
// (`eieio--perform-slot-validation-for-default' / `eieio--slot-override').
pub(crate) fn eieio_validate_class_slots(
    interp: &mut Interpreter,
    class_name: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp
        .lookup_var("eieio-skip-typecheck", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    let mut inherited: Vec<EieioSlotDescriptor> = Vec::new();
    if let Some(parents) = interp.get_symbol_property(class_name, "emaxx-class-parents")
        && let Ok(parent_values) = parents.to_vec()
    {
        for parent in parent_values {
            if let Ok(parent_name) = parent.as_symbol() {
                let mut parent_descriptors = Vec::new();
                let mut parent_seen = vec![class_name.to_string()];
                collect_merged_eieio_slot_descriptors(
                    interp,
                    parent_name,
                    &mut parent_descriptors,
                    &mut parent_seen,
                )?;
                for descriptor in parent_descriptors {
                    merge_eieio_slot_descriptor(&mut inherited, descriptor, false);
                }
            }
        }
    }
    let mut own: Vec<EieioSlotDescriptor> = Vec::new();
    let mut merged = inherited.clone();
    collect_eieio_slot_descriptors(interp, class_name, &mut own, true)?;
    collect_eieio_slot_descriptors(interp, class_name, &mut merged, true)?;
    for descriptor in &own {
        if inherited
            .iter()
            .any(|parent_slot| parent_slot.name == descriptor.name)
        {
            continue;
        }
        eieio_validate_constant_initform(interp, descriptor, env)?;
    }
    for descriptor in &own {
        let Some(old) = inherited
            .iter()
            .find(|parent_slot| parent_slot.name == descriptor.name)
        else {
            continue;
        };
        if !matches!(descriptor.slot_type, Value::T)
            && !crate::lisp::primitives::values_equal(interp, &descriptor.slot_type, &old.slot_type)
        {
            return Err(LispError::Signal(format!(
                "Child slot type `{}' does not match inherited type `{}' for `{}'",
                descriptor.slot_type, old.slot_type, descriptor.name
            )));
        }
        let old_protection = old.props.iter().find(|(key, _)| key == ":protection");
        let new_protection = descriptor
            .props
            .iter()
            .find(|(key, _)| key == ":protection");
        if old_protection.map(|(_, value)| value) != new_protection.map(|(_, value)| value) {
            return Err(LispError::Signal(format!(
                "Child slot protection does not match inherited protection for `{}'",
                descriptor.name
            )));
        }
        if descriptor.initform.is_some() {
            // Validate the override against the inherited (merged) type.
            let merged_slot = merged
                .iter()
                .find(|candidate| candidate.name == descriptor.name);
            let mut check = descriptor.clone();
            if let Some(merged_slot) = merged_slot {
                check.slot_type = merged_slot.slot_type.clone();
            }
            eieio_validate_constant_initform(interp, &check, env)?;
        }
    }
    Ok(())
}

fn eieio_validate_constant_initform(
    interp: &mut Interpreter,
    descriptor: &EieioSlotDescriptor,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(initform) = &descriptor.initform else {
        return Ok(());
    };
    if matches!(descriptor.slot_type, Value::T) {
        return Ok(());
    }
    let constant = match initform {
        Value::Cons(car, _) => {
            matches!(&*car.borrow(), Value::Symbol(head) if head == "quote" || head == "function" || head == "vector-literal")
        }
        Value::Symbol(symbol) => symbol.starts_with(':'),
        _ => true,
    };
    if !constant {
        return Ok(());
    }
    let value = interp.eval(initform, env)?;
    if !eieio_value_matches_type(interp, &value, &descriptor.slot_type, env)? {
        if std::env::var("EMAXX_DEBUG_EIEIO").is_ok() {
            eprintln!(
                "EIEIO defclass initform type fail: slot={} type={} value={}",
                descriptor.name, descriptor.slot_type, value
            );
        }
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("invalid-slot-type".into()),
            Value::Symbol(descriptor.name.clone()),
            descriptor.slot_type.clone(),
            value,
        ])));
    }
    Ok(())
}

pub(crate) fn eieio_value_matches_type(
    interp: &mut Interpreter,
    value: &Value,
    slot_type: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    if matches!(slot_type, Value::T) || matches!(value, Value::Unbound) {
        return Ok(true);
    }
    // GNU `functionp' accepts fbound symbols.
    if matches!(slot_type, Value::Symbol(type_name) if type_name == "function")
        && matches!(value, Value::Symbol(fn_name)
            if is_builtin(fn_name) || interp.lookup_function(fn_name, env).is_ok())
    {
        return Ok(true);
    }
    Ok(
        crate::lisp::primitives::call(interp, "cl-typep", &[value.clone(), slot_type.clone()], env)
            .map(|result| result.is_truthy())
            .unwrap_or(false),
    )
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

// GNU stores class-allocated slot values once per class
// (`eieio--class-class-allocation-values'); emaxx keeps them in the same
// per-class property used by `oset-default'.
pub(crate) fn eieio_class_allocation_property(slot_name: &str) -> String {
    format!("emaxx-class-default:{slot_name}")
}

fn eieio_slot_missing_dispatch(
    interp: &mut Interpreter,
    env: &mut Env,
    object: &Value,
    slot_name: &str,
    operation: &str,
    new_value: Option<&Value>,
) -> Result<Value, LispError> {
    // GNU funnels unknown slots through the `slot-missing' generic; its
    // default method signals `invalid-slot-name'.
    if let Ok(function) = interp.lookup_function("slot-missing", env) {
        let mut args = vec![
            object.clone(),
            Value::Symbol(slot_name.into()),
            Value::Symbol(operation.into()),
        ];
        if let Some(new_value) = new_value {
            args.push(new_value.clone());
        }
        return invoke_function_value(interp, &function, &args, env);
    }
    Err(LispError::SignalValue(Value::list([
        Value::Symbol("invalid-slot-name".into()),
        object.clone(),
        Value::Symbol(slot_name.into()),
    ])))
}

fn eieio_slot_unbound_dispatch(
    interp: &mut Interpreter,
    env: &mut Env,
    object: &Value,
    class_name: &str,
    slot_name: &str,
    function_name: &str,
) -> Result<Value, LispError> {
    // GNU funnels unbound reads through the `slot-unbound' generic; its
    // default method signals `unbound-slot'.
    if let Ok(function) = interp.lookup_function("slot-unbound", env) {
        let class = interp
            .class_value(class_name)
            .unwrap_or_else(|| Value::Symbol(class_name.into()));
        return invoke_function_value(
            interp,
            &function,
            &[
                object.clone(),
                class,
                Value::Symbol(slot_name.into()),
                Value::Symbol(function_name.into()),
            ],
            env,
        );
    }
    Err(LispError::SignalValue(Value::list([
        Value::Symbol("unbound-slot".into()),
        Value::String(format!("Unbound slot: {slot_name}")),
        object.clone(),
        Value::Symbol(slot_name.into()),
    ])))
}

fn eieio_class_allocated_value(
    interp: &mut Interpreter,
    class_name: &str,
    slot: &EieioSlotSpec,
    env: &mut Env,
) -> Result<Value, LispError> {
    if let Some(value) =
        interp.get_symbol_property(class_name, &eieio_class_allocation_property(&slot.name))
    {
        return Ok(value);
    }
    match &slot.initform {
        Some(initform) => interp.eval(initform, env),
        None => Ok(Value::Unbound),
    }
}

pub(crate) fn eieio_oref_dispatch(
    interp: &mut Interpreter,
    env: &mut Env,
    object: &Value,
    slot_name: &str,
) -> Result<Value, LispError> {
    // A class symbol (or class record) reads its class-allocated storage.
    if let Value::Symbol(_) = object {
        let Some(class_name) = interp.class_name_from_value(object) else {
            return Err(LispError::TypeError(
                "eieio-object".into(),
                object.type_name(),
            ));
        };
        if interp.class_value(&class_name).is_none() {
            return Err(LispError::TypeError(
                "eieio-object".into(),
                object.type_name(),
            ));
        }
        let slots = eieio_slot_specs(interp, &class_name)?;
        if let Some(index) = eieio_slot_index(&slots, slot_name)
            && slots[index].class_allocated
        {
            let value = eieio_class_allocated_value(interp, &class_name, &slots[index], env)?;
            if matches!(value, Value::Unbound) {
                return eieio_slot_unbound_dispatch(
                    interp,
                    env,
                    object,
                    &class_name,
                    slot_name,
                    "oref",
                );
            }
            return Ok(value);
        }
        return eieio_slot_missing_dispatch(interp, env, object, slot_name, "oref", None);
    }
    let Value::Record(record_id) = object else {
        return Err(LispError::TypeError(
            "eieio-object".into(),
            object.type_name(),
        ));
    };
    let (type_name, stored) = {
        let record = interp
            .find_record(*record_id)
            .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{record_id}>")))?;
        (record.type_name.clone(), record.slots.clone())
    };
    let slots = eieio_slot_specs(interp, &type_name)?;
    let Some(slot_index) = eieio_slot_index(&slots, slot_name) else {
        return eieio_slot_missing_dispatch(interp, env, object, slot_name, "oref", None);
    };
    let slot_name = slots[slot_index].name.clone();
    if slots[slot_index].class_allocated {
        let value = eieio_class_allocated_value(interp, &type_name, &slots[slot_index], env)?;
        if matches!(value, Value::Unbound) {
            return eieio_slot_unbound_dispatch(
                interp, env, object, &type_name, &slot_name, "oref",
            );
        }
        return Ok(value);
    }
    match stored.get(slot_index) {
        Some(Value::Unbound) | None => {
            eieio_slot_unbound_dispatch(interp, env, object, &type_name, &slot_name, "oref")
        }
        Some(value) => Ok(value.clone()),
    }
}

pub(crate) fn eieio_oset_dispatch(
    interp: &mut Interpreter,
    env: &mut Env,
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
        return eieio_slot_missing_dispatch(interp, env, object, slot_name, "oset", Some(&value));
    };
    let skip_typecheck = interp
        .lookup_var("eieio-skip-typecheck", env)
        .is_some_and(|setting| setting.is_truthy());
    if !skip_typecheck
        && !eieio_value_matches_type(interp, &value, &slots[slot_index].slot_type.clone(), env)?
    {
        if std::env::var("EMAXX_DEBUG_EIEIO").is_ok() {
            eprintln!(
                "EIEIO oset type fail: class={type_name} slot={} type={} value={}",
                slots[slot_index].name, slots[slot_index].slot_type, value
            );
        }
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("invalid-slot-type".into()),
            Value::Symbol(type_name.clone()),
            Value::Symbol(slots[slot_index].name.clone()),
            slots[slot_index].slot_type.clone(),
            value,
        ])));
    }
    // cl-defstruct slots can be :read-only.
    if let Some(descs) = interp.get_symbol_property(&type_name, "emaxx-struct-slot-descs")
        && let Ok(descs) = descs.to_vec()
    {
        for desc in descs {
            let Ok(parts) = desc.to_vec() else { continue };
            if parts
                .first()
                .and_then(|part| part.as_symbol().ok())
                .is_some_and(|name| name == slots[slot_index].name)
            {
                let mut cursor = 1usize;
                while cursor + 1 < parts.len() {
                    if matches!(&parts[cursor], Value::Symbol(key) if key == ":read-only")
                        && parts[cursor + 1].is_truthy()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("eieio-read-only".into()),
                            Value::Symbol(type_name.clone()),
                            Value::Symbol(slots[slot_index].name.clone()),
                        ])));
                    }
                    cursor += 1;
                }
                break;
            }
        }
    }
    if slots[slot_index].class_allocated {
        interp.put_symbol_property(
            &type_name,
            &eieio_class_allocation_property(&slots[slot_index].name),
            value.clone(),
        );
        return Ok(value);
    }
    set_eieio_slot_value(interp, object, slot_name, value)
}

pub(crate) fn eieio_slot_makeunbound(
    interp: &mut Interpreter,
    object: &Value,
    slot_name: &str,
) -> Result<Value, LispError> {
    let (class_name, record_id) = match object {
        Value::Record(id) => (
            interp
                .find_record(*id)
                .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?
                .type_name
                .clone(),
            Some(*id),
        ),
        other => match interp.class_name_from_value(other) {
            Some(class_name) if interp.class_value(&class_name).is_some() => (class_name, None),
            _ => {
                return Err(LispError::TypeError(
                    "eieio-object".into(),
                    object.type_name(),
                ));
            }
        },
    };
    let slots = eieio_slot_specs(interp, &class_name)?;
    let Some(slot_index) = eieio_slot_index(&slots, slot_name) else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("invalid-slot-name".into()),
            object.clone(),
            Value::Symbol(slot_name.into()),
        ])));
    };
    if slots[slot_index].class_allocated {
        interp.put_symbol_property(
            &class_name,
            &eieio_class_allocation_property(&slots[slot_index].name),
            Value::Unbound,
        );
    } else if let Some(record_id) = record_id
        && let Some(record) = interp.find_record_mut(record_id)
        && let Some(slot) = record.slots.get_mut(slot_index)
    {
        *slot = Value::Unbound;
    }
    Ok(Value::Nil)
}

pub(crate) fn eieio_slot_boundp(
    interp: &Interpreter,
    object: &Value,
    slot_name: &str,
) -> Result<Value, LispError> {
    let (class_name, record_id) = match object {
        Value::Record(id) => (
            interp
                .find_record(*id)
                .ok_or_else(|| LispError::TypeError("eieio-object-p".into(), object.type_name()))?
                .type_name
                .clone(),
            Some(*id),
        ),
        other => match interp.class_name_from_value(other) {
            Some(class_name) if interp.class_value(&class_name).is_some() => (class_name, None),
            _ => {
                return Err(LispError::TypeError(
                    "eieio-object-p".into(),
                    object.type_name(),
                ));
            }
        },
    };
    let slots = eieio_slot_specs(interp, &class_name)?;
    let Some(slot_index) = eieio_slot_index(&slots, slot_name) else {
        return Ok(Value::Nil);
    };
    if slots[slot_index].class_allocated {
        let bound = match interp.get_symbol_property(
            &class_name,
            &eieio_class_allocation_property(&slots[slot_index].name),
        ) {
            Some(Value::Unbound) => false,
            Some(_) => true,
            None => slots[slot_index].initform.is_some(),
        };
        return Ok(if bound { Value::T } else { Value::Nil });
    }
    let bound = record_id
        .and_then(|record_id| interp.find_record(record_id))
        .and_then(|record| record.slots.get(slot_index))
        .is_some_and(|value| !matches!(value, Value::Unbound));
    Ok(if bound { Value::T } else { Value::Nil })
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
