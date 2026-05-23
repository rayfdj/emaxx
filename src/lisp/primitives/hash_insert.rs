use super::*;

pub(crate) fn hash_table_user_test_functions(
    interp: &Interpreter,
    test: &str,
) -> Option<(Value, Value)> {
    let spec = interp.get_symbol_property(test, "hash-table-test")?;
    let items = spec.to_vec().ok()?;
    if items.len() != 2 {
        return None;
    }
    Some((items[0].clone(), items[1].clone()))
}

pub(crate) fn call_hash_table_test_function(
    interp: &mut Interpreter,
    table: &Value,
    function: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some((_, before_entries)) = json::hash_table_entries(interp, table) else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    let result = call_function_value(interp, function, args, env);
    let Some((_, after_entries)) = json::hash_table_entries(interp, table) else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    if before_entries != after_entries {
        set_hash_table_entries(interp, table, before_entries)?;
        return Err(LispError::Signal("hash table test modifies table".into()));
    }
    result
}

pub(crate) fn touch_hash_table_key(
    interp: &mut Interpreter,
    table: &Value,
    test: &str,
    key: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some((_, hash_fn)) = hash_table_user_test_functions(interp, test) else {
        return Ok(());
    };
    let _ = call_hash_table_test_function(interp, table, &hash_fn, std::slice::from_ref(key), env)?;
    Ok(())
}

pub(crate) fn hash_table_key_matches(
    interp: &mut Interpreter,
    table: &Value,
    test: &str,
    left: &Value,
    right: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    match test {
        "equal" => Ok(values_equal(interp, left, right)),
        "eq" => Ok(values_eq_in_env(interp, left, right, env)),
        "eql" => Ok(values_eql(left, right)),
        _ => {
            let Some((compare_fn, _)) = hash_table_user_test_functions(interp, test) else {
                return Err(LispError::Signal("Invalid hash table test".into()));
            };
            Ok(call_hash_table_test_function(
                interp,
                table,
                &compare_fn,
                &[left.clone(), right.clone()],
                env,
            )?
            .is_truthy())
        }
    }
}

pub(crate) fn weak_hash_component_is_dead(value: &Value) -> bool {
    string_like(value)
        .map(|string| string.text.ends_with("-dead"))
        .unwrap_or(false)
}

pub(crate) fn collect_weak_hash_tables(interp: &mut Interpreter) -> Result<(), LispError> {
    let table_ids = interp.record_ids_by_type("hash-table");
    for id in table_ids {
        let table = Value::Record(id);
        let weakness = hash_table_metadata_slot(interp, &table, 5, Value::Nil)?;
        let Some(weakness_name) = weakness.as_symbol().ok() else {
            continue;
        };
        let Some((_, entries)) = json::hash_table_entries(interp, &table) else {
            continue;
        };
        let retained = entries
            .into_iter()
            .filter(|(key, value)| {
                let key_live = !weak_hash_component_is_dead(key);
                let value_live = !weak_hash_component_is_dead(value);
                match weakness_name {
                    "key" => key_live,
                    "value" => value_live,
                    "key-and-value" => key_live && value_live,
                    "key-or-value" => key_live || value_live,
                    _ => true,
                }
            })
            .collect();
        set_hash_table_entries(interp, &table, retained)?;
    }
    Ok(())
}

pub(crate) fn hash_table_metadata_slot(
    interp: &Interpreter,
    table: &Value,
    slot: usize,
    default: Value,
) -> Result<Value, LispError> {
    let Value::Record(id) = table else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    if record.type_name != "hash-table" {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    }
    Ok(record.slots.get(slot).cloned().unwrap_or(default))
}

pub(crate) fn hash_table_entries_to_value(entries: Vec<(Value, Value)>) -> Value {
    Value::list(
        entries
            .into_iter()
            .map(|(key, value)| Value::cons(key, value)),
    )
}

pub(crate) fn list_sequence_items(
    interp: &Interpreter,
    value: &Value,
) -> Result<Vec<Value>, LispError> {
    if let Some(items) = keymap_list_items(interp, value)? {
        Ok(items)
    } else {
        value.to_vec()
    }
}

pub(crate) fn keymap_list_items(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<Vec<Value>>, LispError> {
    let Some(id) = keymap_record_id(interp, value) else {
        return Ok(None);
    };
    let Some(record) = interp.find_record(id) else {
        return Ok(None);
    };
    let mut items = vec![Value::Symbol("keymap".into())];
    if let Some(char_table) = keymap_char_table(record) {
        items.push(char_table);
    }
    let bindings = keymap_bindings(record)?;
    if let Some(name) = record.slots.first()
        && !name.is_nil()
    {
        items.push(name.clone());
    }
    items.extend(
        bindings
            .iter()
            .filter(|binding| !binding.after_prompt)
            .rev()
            .map(keymap_binding_entry),
    );
    items.extend(
        bindings
            .iter()
            .filter(|binding| binding.after_prompt)
            .map(keymap_binding_entry),
    );
    Ok(Some(items))
}

pub(crate) fn context_menu_keymap_items(
    interp: &Interpreter,
    keymap: &Value,
) -> Result<Value, LispError> {
    let Some(id) = keymap_record_id(interp, keymap) else {
        return Ok(keymap.clone());
    };
    let Some(record) = interp.find_record(id) else {
        return Ok(keymap.clone());
    };
    let bindings = keymap_bindings(record)?;
    let mut items = vec![Value::Symbol("keymap".into())];
    items.extend(
        bindings
            .iter()
            .filter(|binding| !binding.after_prompt)
            .map(keymap_binding_entry),
    );
    if let Some(name) = record.slots.first()
        && !name.is_nil()
    {
        items.push(name.clone());
    }
    items.extend(
        bindings
            .iter()
            .filter(|binding| binding.after_prompt)
            .map(keymap_binding_entry),
    );
    Ok(Value::list(trim_redundant_separator_items(interp, items)))
}

pub(crate) fn trim_redundant_separator_items(
    interp: &Interpreter,
    items: Vec<Value>,
) -> Vec<Value> {
    if !matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "keymap") {
        return items;
    }

    let mut cleaned = Vec::with_capacity(items.len());
    cleaned.push(Value::Symbol("keymap".into()));

    let mut pending_separator_index = Some(0usize);
    for item in items.into_iter().skip(1) {
        if is_separator_keymap_entry(interp, &item) {
            if pending_separator_index.is_some() {
                continue;
            }
            pending_separator_index = Some(cleaned.len());
            cleaned.push(item);
            continue;
        }

        if matches!(item, Value::Cons(_, _)) {
            pending_separator_index = None;
        }
        cleaned.push(item);
    }

    if let Some(index) = pending_separator_index
        && index > 0
        && index < cleaned.len()
    {
        cleaned.remove(index);
    }

    cleaned
}

pub(crate) fn is_separator_keymap_entry(interp: &Interpreter, entry: &Value) -> bool {
    let Some((_, binding)) = entry.cons_values() else {
        return false;
    };
    matches!(binding, Value::Symbol(ref symbol) if symbol == "menu-bar-separator")
        || interp
            .lookup_var("menu-bar-separator", &Vec::new())
            .is_some_and(|separator| values_equal(interp, &binding, &separator))
}

#[derive(Clone)]
pub(crate) struct RuntimeKeymapBinding {
    pub(crate) key: String,
    pub(crate) parts: Option<Vec<String>>,
    pub(crate) value: Value,
    pub(crate) after_prompt: bool,
}

pub(crate) fn keymap_binding_entry(binding: &RuntimeKeymapBinding) -> Value {
    Value::cons(
        keymap_entry_key_value(&binding_key_parts(binding), &binding.key),
        binding.value.clone(),
    )
}

pub(crate) fn keymap_entry_key_value(parts: &[String], key: &str) -> Value {
    if let [part] = parts {
        let (_, _, saw_prefix) = parse_kbd_prefixes(part);
        if !part.starts_with('<')
            && !part.ends_with('>')
            && !saw_prefix
            && named_kbd_key_code(part).is_none()
            && part.chars().count() > 1
        {
            return Value::Symbol(part.clone());
        }

        let mut events = parse_kbd_token(part)
            .into_iter()
            .map(reader_key_event_value)
            .collect::<Vec<_>>();
        if events.len() == 1 {
            return events.remove(0);
        }
    }

    Value::String(key.into())
}

pub(crate) fn set_hash_table_entries(
    interp: &mut Interpreter,
    table: &Value,
    entries: Vec<(Value, Value)>,
) -> Result<(), LispError> {
    let Value::Record(id) = table else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    if record.slots.len() < 2 {
        record.slots.resize(2, Value::Nil);
    }
    record.slots[1] = hash_table_entries_to_value(entries);
    Ok(())
}

pub(crate) fn parse_xml_region(xml: &str) -> Result<Value, roxmltree::Error> {
    let doc = Document::parse(xml)?;
    let children = xml_child_values(doc.root())?;
    if children.len() == 1 && matches!(children.first(), Some(Value::Cons(_, _))) {
        Ok(children.into_iter().next().unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            [Value::Symbol("top".into()), Value::Nil]
                .into_iter()
                .chain(children),
        ))
    }
}

pub(crate) fn xml_child_values(node: Node<'_, '_>) -> Result<Vec<Value>, roxmltree::Error> {
    let mut children = Vec::new();
    for child in node.children() {
        match child.node_type() {
            NodeType::Element => children.push(xml_element_value(child)?),
            NodeType::Comment => {
                children.push(Value::list([
                    Value::Symbol("comment".into()),
                    Value::Nil,
                    Value::String(child.text().unwrap_or_default().to_string()),
                ]));
            }
            NodeType::Text => {
                let text = child.text().unwrap_or_default();
                if !text.trim().is_empty() {
                    children.push(Value::String(text.to_string()));
                }
            }
            _ => {}
        }
    }
    Ok(children)
}

pub(crate) fn xml_element_value(node: Node<'_, '_>) -> Result<Value, roxmltree::Error> {
    let attrs = if node.attributes().len() == 0 {
        Value::Nil
    } else {
        Value::list(node.attributes().map(|attr| {
            Value::cons(
                Value::Symbol(attr.name().to_string()),
                Value::String(attr.value().to_string()),
            )
        }))
    };
    let children = xml_child_values(node)?;
    Ok(Value::list(
        [Value::Symbol(node.tag_name().name().to_string()), attrs]
            .into_iter()
            .chain(children),
    ))
}

pub(crate) fn display_property_value(value: &Value, property: &str) -> Option<Value> {
    if let Ok(items) = value.to_vec() {
        if let Some(Value::Symbol(name)) = items.first()
            && name == property
        {
            return items.get(1).cloned();
        }
        if matches!(items.first(), Some(Value::Symbol(name)) if name == "vector-literal")
            || matches!(items.first(), Some(Value::Symbol(name)) if name == "vector")
        {
            for item in items.iter().skip(1) {
                if let Some(found) = display_property_value(item, property) {
                    return Some(found);
                }
            }
            return None;
        }
        for item in items {
            if let Some(found) = display_property_value(&item, property) {
                return Some(found);
            }
        }
    }
    None
}

pub(crate) fn find_bidi_override(interp: &Interpreter, start: usize, end: usize) -> Option<usize> {
    let text = interp.buffer.buffer_substring(start, end).ok()?;
    let chars = text.chars().collect::<Vec<_>>();
    let control_index = chars
        .iter()
        .position(|ch| matches!(*ch as u32, 0x202A..=0x202E | 0x2066..=0x2069))?;
    chars[control_index..]
        .iter()
        .position(|ch| {
            !matches!((*ch) as u32, 0x202A..=0x202E | 0x2066..=0x2069)
                && !ch.is_whitespace()
                && !matches!(*ch, '{' | '}')
        })
        .map(|offset| start + control_index + offset)
}

pub(crate) fn insert_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
    inherit: bool,
    before_markers: bool,
) -> Result<Value, LispError> {
    let combined = combine_insert_args(args)?;
    let insert_at = interp.buffer.point();
    let nchars = combined.text.chars().count();
    insert_text_with_hooks(
        interp,
        &combined.text,
        &combined.props,
        inherit,
        before_markers,
        env,
    )?;
    if before_markers {
        for overlay in &mut interp.buffer.overlays {
            if overlay.is_dead() {
                continue;
            }
            if overlay.beg == insert_at {
                overlay.beg += nchars;
            }
            if overlay.end == insert_at {
                overlay.end += nchars;
            }
        }
    }
    Ok(Value::Nil)
}

pub(crate) fn skeleton_insert_value(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    point: &mut Option<usize>,
) -> Result<(), LispError> {
    match value {
        Value::Nil => Ok(()),
        Value::String(_) | Value::StringObject(_) => {
            insert_impl(interp, std::slice::from_ref(value), env, false, false)?;
            Ok(())
        }
        Value::Integer(code) => {
            insert_char_impl(interp, std::slice::from_ref(value), env)?;
            if char::from_u32(*code as u32).is_none() {
                return Err(LispError::Signal(format!("Invalid character: {code}")));
            }
            Ok(())
        }
        Value::Symbol(symbol) if symbol == "_" => {
            point.get_or_insert_with(|| interp.buffer.point());
            Ok(())
        }
        Value::Symbol(_) => Ok(()),
        Value::Cons(_, _) => {
            let items = value.to_vec()?;
            for item in items.iter().skip(1) {
                skeleton_insert_value(interp, item, env, point)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn insert_char_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_args("insert-char", args, 1)?;
    let ch = args[0].as_integer()?;
    let count = if args.len() > 1 {
        args[1].as_integer()?.max(0) as usize
    } else {
        1
    };
    if let Some(c) = char::from_u32(ch as u32) {
        let text: String = std::iter::repeat_n(c, count).collect();
        insert_text_with_hooks(interp, &text, &[], false, false, env)?;
    } else if (0..=0x3F_FFFF).contains(&ch) {
        let text: String = std::iter::repeat_n(RAW_CHAR_SENTINEL, count).collect();
        let props = vec![TextPropertySpan {
            start: 0,
            end: count,
            props: vec![("emaxx-raw-char".into(), Value::Integer(ch))],
        }];
        insert_text_with_hooks(interp, &text, &props, false, false, env)?;
    } else {
        return Err(LispError::Signal(format!("Invalid character: {}", ch)));
    }
    Ok(Value::Nil)
}

pub(crate) fn insert_text_with_hooks(
    interp: &mut Interpreter,
    text: &str,
    props: &[TextPropertySpan],
    inherit: bool,
    before_markers: bool,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    ensure_insert_modifiable(interp, env)?;
    ensure_no_supersession_threat(interp, env)?;
    let start = interp.buffer.point();
    let overlay_calls = overlay_insert_hook_calls(&interp.buffer, start, text.chars().count());
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
    run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(start as i64), Value::Integer(start as i64)],
        env,
    )?;
    if before_markers {
        if inherit {
            interp.insert_current_buffer_before_markers_and_inherit(text);
        } else {
            interp.insert_current_buffer_before_markers(text);
        }
    } else if inherit {
        interp.insert_current_buffer_and_inherit(text);
    } else {
        interp.insert_current_buffer(text);
    }
    for span in props {
        interp
            .buffer
            .add_text_properties(start + span.start, start + span.end, &span.props);
    }
    let end = start + text.chars().count();
    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(start as i64),
            Value::Integer(end as i64),
            Value::Integer(0),
        ],
        env,
    )?;
    let _ = maybe_lock_current_buffer(interp, env);
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
    Ok(())
}

pub(crate) fn combine_insert_args(args: &[Value]) -> Result<StringLike, LispError> {
    let mut text = String::new();
    let mut props = Vec::new();
    for arg in args {
        if let Some(string) = string_like(arg) {
            let offset = text.chars().count();
            text.push_str(&string.text);
            props.extend(shift_string_props(&string.props, offset));
        } else {
            let fragment = match arg {
                Value::Integer(n) => {
                    let offset = text.chars().count();
                    if let Some(c) = char::from_u32(*n as u32) {
                        c.to_string()
                    } else if (0..=0x3F_FFFF).contains(n) {
                        props.push(TextPropertySpan {
                            start: offset,
                            end: offset + 1,
                            props: vec![("emaxx-raw-char".into(), Value::Integer(*n))],
                        });
                        RAW_CHAR_SENTINEL.to_string()
                    } else {
                        String::new()
                    }
                }
                Value::Nil => String::new(),
                _ => arg.to_string(),
            };
            text.push_str(&fragment);
        }
    }
    Ok(StringLike {
        multibyte: text.chars().any(|ch| (ch as u32) > 0x7F),
        text,
        props: merge_string_props(props),
    })
}
