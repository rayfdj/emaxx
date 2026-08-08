use super::*;

// GNU `defclass' generates slot accessors as cl-generic methods: an
// :accessor gets a reader method plus a `(setf ACC)' method (and, under
// `eieio-backward-compatibility', a `(subclass CLASS)' reader for
// class-allocated slots); a :reader gets a reader method; a :writer gets a
// setter method.  Evaluate the same method definitions here.
pub(super) fn install_eieio_slot_accessors(
    interp: &mut Interpreter,
    class_name: &str,
) -> Result<(), LispError> {
    let Some(raw_slots) = interp.get_symbol_property(class_name, "emaxx-class-slots") else {
        return Ok(());
    };
    let mut method_forms = Vec::new();
    for raw_slot in raw_slots.to_vec()? {
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
        let quoted_slot = Value::list([
            Value::Symbol("quote".into()),
            Value::Symbol(slot_name.clone().into()),
        ]);
        let this_specializer = Value::list([
            Value::Symbol("this".into()),
            Value::Symbol(class_name.to_string().into()),
        ]);
        let mut class_allocated = false;
        let mut accessor = None;
        let mut reader = None;
        let mut writer = None;
        let mut index = 1usize;
        while index + 1 < parts.len() {
            let Some(keyword) = parts[index].as_symbol().ok() else {
                index += 1;
                continue;
            };
            match keyword {
                ":accessor" => accessor = parts[index + 1].as_symbol().ok().map(str::to_string),
                ":reader" => reader = parts[index + 1].as_symbol().ok().map(str::to_string),
                ":writer" => writer = parts[index + 1].as_symbol().ok().map(str::to_string),
                ":allocation" => {
                    class_allocated = matches!(&parts[index + 1], Value::Symbol(value) if value == ":class" || value == "class");
                }
                _ => {}
            }
            index += 2;
        }
        if let Some(accessor) = accessor {
            method_forms.push(Value::list([
                Value::Symbol("cl-defmethod".into()),
                Value::list([
                    Value::Symbol("setf".into()),
                    Value::Symbol(accessor.clone().into()),
                ]),
                Value::list([Value::Symbol("value".into()), this_specializer.clone()]),
                Value::list([
                    Value::Symbol("eieio-oset".into()),
                    Value::Symbol("this".into()),
                    quoted_slot.clone(),
                    Value::Symbol("value".into()),
                ]),
            ]));
            method_forms.push(Value::list([
                Value::Symbol("cl-defmethod".into()),
                Value::Symbol(accessor.clone().into()),
                Value::list([this_specializer.clone()]),
                Value::list([
                    Value::Symbol("slot-value".into()),
                    Value::Symbol("this".into()),
                    quoted_slot.clone(),
                ]),
            ]));
            if class_allocated {
                method_forms.push(Value::list([
                    Value::Symbol("cl-defmethod".into()),
                    Value::Symbol(accessor.into()),
                    Value::list([Value::list([
                        Value::Symbol("this".into()),
                        Value::list([
                            Value::Symbol("subclass".into()),
                            Value::Symbol(class_name.to_string().into()),
                        ]),
                    ])]),
                    Value::list([
                        Value::Symbol("if".into()),
                        Value::list([
                            Value::Symbol("slot-boundp".into()),
                            Value::Symbol("this".into()),
                            quoted_slot.clone(),
                        ]),
                        Value::list([
                            Value::Symbol("eieio-oref-default".into()),
                            Value::Symbol("this".into()),
                            quoted_slot.clone(),
                        ]),
                    ]),
                ]));
            }
        }
        if let Some(reader) = reader {
            method_forms.push(Value::list([
                Value::Symbol("cl-defmethod".into()),
                Value::Symbol(reader.into()),
                Value::list([this_specializer.clone()]),
                Value::list([
                    Value::Symbol("slot-value".into()),
                    Value::Symbol("this".into()),
                    quoted_slot.clone(),
                ]),
            ]));
        }
        if let Some(writer) = writer {
            method_forms.push(Value::list([
                Value::Symbol("cl-defmethod".into()),
                Value::Symbol(writer.into()),
                Value::list([this_specializer.clone(), Value::Symbol("value".into())]),
                Value::list([
                    Value::Symbol("eieio-oset".into()),
                    Value::Symbol("this".into()),
                    quoted_slot.clone(),
                    Value::Symbol("value".into()),
                ]),
            ]));
        }
    }
    for form in method_forms {
        interp.eval(&form, &mut Env::new())?;
    }
    Ok(())
}
