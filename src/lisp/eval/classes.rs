use super::*;

pub(super) fn install_eieio_slot_accessors(
    interp: &mut Interpreter,
    class_name: &str,
) -> Result<(), LispError> {
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
        let mut index = 1usize;
        while index + 1 < parts.len() {
            let Some(keyword) = parts[index].as_symbol().ok() else {
                index += 1;
                continue;
            };
            if keyword == ":accessor" {
                let accessor = parts[index + 1].as_symbol()?.to_string();
                interp.set_function_binding(
                    &accessor,
                    Some(Value::Lambda(
                        vec!["object".into()],
                        vec![Value::list([
                            Value::Symbol("slot-value".into()),
                            Value::Symbol("object".into()),
                            Value::list([
                                Value::Symbol("quote".into()),
                                Value::Symbol(slot_name.clone()),
                            ]),
                        ])],
                        shared_env(Vec::new()),
                    )),
                );
            }
            index += 2;
        }
    }
    Ok(())
}
