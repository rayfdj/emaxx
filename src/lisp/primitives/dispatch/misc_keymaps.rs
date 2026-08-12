use super::*;
use crate::lisp::primitives::string_like;
use std::cell::RefCell;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

thread_local! {
    static SEMANTIC_CPP_INCLUDE_TAG_CACHE: RefCell<HashMap<PathBuf, Vec<Value>>> =
        RefCell::new(HashMap::new());
}

fn map_keymap_direct_value(
    interp: &mut Interpreter,
    function: &Value,
    keymap: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let full_table = keymap_char_table_value(interp, keymap).and_then(|table| match table {
        Value::CharTable(id) => Some(id),
        _ => None,
    });
    if let Some(table_id) = full_table {
        for entry in interp
            .char_table_effective_ranges(table_id)
            .unwrap_or_default()
        {
            let event = if entry.start == entry.end {
                Value::Integer(i64::from(entry.start))
            } else {
                Value::cons(
                    Value::Integer(i64::from(entry.start)),
                    Value::Integer(i64::from(entry.end)),
                )
            };
            interp.call_function_value(function.clone(), None, &[event, entry.value], env)?;
        }
    }

    let bindings = keymap_direct_bindings(interp, keymap)?;
    let mut character_bindings = Vec::new();
    let mut sparse_bindings = Vec::new();
    for binding in bindings {
        let event = keymap_entry_key_value(&binding_key_parts(&binding), &binding.key);
        if full_table.is_some()
            && let Value::Integer(code) = event
        {
            character_bindings.push((code, binding.value));
        } else {
            sparse_bindings.push((event, binding.value));
        }
    }
    character_bindings.sort_by_key(|(code, _)| *code);
    let mut index = 0;
    while index < character_bindings.len() {
        let (start, value) = character_bindings[index].clone();
        let mut end = start;
        while let Some((next, next_value)) = character_bindings.get(index + 1) {
            if *next != end.saturating_add(1) || !values_equal(interp, &value, next_value) {
                break;
            }
            index += 1;
            end = *next;
        }
        let event = if start == end {
            Value::Integer(start)
        } else {
            Value::cons(Value::Integer(start), Value::Integer(end))
        };
        interp.call_function_value(function.clone(), None, &[event, value], env)?;
        index += 1;
    }
    for (event, value) in sparse_bindings {
        interp.call_function_value(function.clone(), None, &[event, value], env)?;
    }
    Ok(())
}

fn map_keymap_value(
    interp: &mut Interpreter,
    function: &Value,
    keymap: &Value,
    env: &mut Env,
    visited: &mut std::collections::HashSet<(bool, usize)>,
) -> Result<(), LispError> {
    let Some(identity) = keymap_value_identity(interp, keymap) else {
        return Ok(());
    };
    if !visited.insert(identity) {
        return Ok(());
    }

    map_keymap_direct_value(interp, function, keymap, env)?;
    for parent in keymap_parent_values(interp, keymap) {
        map_keymap_value(interp, function, &parent, env, visited)?;
    }
    Ok(())
}

fn vector_index_description(index: u32) -> Result<String, LispError> {
    key_sequence_binding_text(&Value::list([
        Value::Symbol("vector-literal".into()),
        Value::Integer(i64::from(index)),
    ]))
}

fn insert_description_indent(interp: &mut Interpreter, column: usize) {
    if column >= 16 {
        interp.insert_current_buffer(" ");
        return;
    }
    let mut current = column;
    while current < 16 {
        interp.insert_current_buffer("\t");
        current = (current / 8 + 1) * 8;
    }
}

fn describe_vector_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("describe-vector", args, 1, 2)?;
    let mut ranges = Vec::<(u32, u32, Value)>::new();
    match &args[0] {
        Value::CharTable(table_id) => {
            for entry in interp
                .char_table_effective_ranges(*table_id)
                .unwrap_or_default()
            {
                if entry.value.is_nil() {
                    continue;
                }
                if let Some((_, end, previous)) = ranges.last_mut()
                    && end.saturating_add(1) == entry.start
                    && values_equal(interp, previous, &entry.value)
                {
                    *end = entry.end;
                } else {
                    ranges.push((entry.start, entry.end, entry.value));
                }
            }
        }
        vector if is_vector_value(vector) => {
            for (index, value) in vector_items(vector)?.into_iter().enumerate() {
                if value.is_nil() {
                    continue;
                }
                let index = u32::try_from(index)
                    .map_err(|_| LispError::Signal("Vector is too large".into()))?;
                if let Some((_, end, previous)) = ranges.last_mut()
                    && end.saturating_add(1) == index
                    && values_equal(interp, previous, &value)
                {
                    *end = index;
                } else {
                    ranges.push((index, index, value));
                }
            }
        }
        other => {
            return Err(LispError::TypeError(
                "vector-or-char-table-p".into(),
                other.type_name(),
            ));
        }
    }

    let describer = args
        .get(1)
        .filter(|value| !value.is_nil())
        .cloned()
        .unwrap_or_else(|| Value::Symbol("princ".into()));
    let output_buffer = Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone());
    let restore = interp.bind_special_variable("standard-output", output_buffer, env)?;
    let mut result = (|| -> Result<Value, LispError> {
        let mut first = true;
        for (start, end, value) in ranges {
            let mut key = vector_index_description(start)?;
            if start != end {
                key.push_str(" .. ");
                key.push_str(&vector_index_description(end)?);
            }
            if first {
                interp.insert_current_buffer("\n");
                first = false;
            }
            interp.insert_current_buffer(&key);
            insert_description_indent(interp, key.chars().count());
            interp.call_function_value(describer.clone(), None, &[value], env)?;
            interp.insert_current_buffer("\n");
        }
        Ok(Value::Nil)
    })();
    if let Err(error) = interp.restore_special_binding(restore, env)
        && result.is_ok()
    {
        result = Err(error);
    }
    result
}

fn dabbrev_completion_at_point(
    interp: &Interpreter,
    env: &Env,
) -> Result<Option<(usize, usize, String)>, LispError> {
    let capfs = interp
        .lookup_var("completion-at-point-functions", env)
        .unwrap_or(Value::Nil);
    let has_dabbrev_capf = capfs
        .to_vec()
        .map(|items| {
            items
                .iter()
                .any(|item| matches!(item, Value::Symbol(symbol) if symbol == "dabbrev-capf"))
        })
        .unwrap_or(false);
    if !has_dabbrev_capf {
        return Ok(None);
    }

    let point = interp.buffer.point();
    let mut start = point;
    while start > interp.buffer.point_min() {
        let Some(ch) = interp.buffer.char_at(start - 1) else {
            break;
        };
        if !dabbrev_word_char(ch) {
            break;
        }
        start -= 1;
    }
    let prefix = interp
        .buffer
        .buffer_substring(start, point)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if prefix.is_empty() {
        return Ok(None);
    }

    let check_other_buffers = interp
        .lookup_var("dabbrev--check-other-buffers", env)
        .is_some_and(|value| value.is_truthy());
    let mut texts = vec![interp.buffer.buffer_string()];
    if check_other_buffers {
        for (buffer_id, _) in &interp.buffer_list {
            if *buffer_id == interp.current_buffer_id() {
                continue;
            }
            if let Some(buffer) = interp.get_buffer_by_id(*buffer_id) {
                texts.push(buffer.buffer_string());
            }
        }
    }
    let mut matches = Vec::new();
    for (text_index, text) in texts.iter().enumerate() {
        let mut word_start = None;
        for (index, ch) in text
            .char_indices()
            .chain(std::iter::once((text.len(), '\0')))
        {
            if ch != '\0' && dabbrev_word_char(ch) {
                if word_start.is_none() {
                    word_start = Some(index);
                }
                continue;
            }
            let Some(byte_start) = word_start.take() else {
                continue;
            };
            let word = &text[byte_start..index];
            let char_start = text[..byte_start].chars().count() + 1;
            let char_end = char_start + word.chars().count();
            if text_index == 0 && char_start == start && char_end == point {
                continue;
            }
            if word.starts_with(&prefix)
                && word != prefix
                && !matches.iter().any(|existing: &String| existing == word)
            {
                matches.push(word.to_string());
            }
        }
    }

    match matches.as_slice() {
        [only] => Ok(Some((start, point, only.clone()))),
        _ => Ok(None),
    }
}

fn dabbrev_word_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '-' || ch == '_'
}

fn cl_find_class_value(
    interp: &Interpreter,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    need_args(name, args, 1)?;
    let symbol = args[0].as_symbol()?;
    Ok(if let Some(class_value) = interp.class_value(symbol) {
        class_value
    } else if is_builtin_class_name(symbol) {
        Value::Symbol(symbol.into())
    } else {
        Value::Nil
    })
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "define-key" => {
                need_arg_range(name, args, 3, 4)?;
                if let Ok(events) = vector_items(&args[1])
                    && let [event] = events.as_slice()
                    && let Some((Value::Integer(start), Value::Integer(end))) = event.cons_values()
                {
                    keymap_define_character_range(interp, &args[0], start, end, args[2].clone())?;
                    return Ok(args[2].clone());
                }
                let key = key_sequence_binding_text(&args[1])?;
                let key_parts = key_sequence_keymap_parts(&args[1])?;
                if args[2].is_nil() && args.get(3).is_some_and(Value::is_truthy) {
                    keymap_remove_binding(interp, &args[0], &key)?;
                } else {
                    keymap_define_binding_with_placement(
                        interp,
                        &args[0],
                        &key,
                        Some(key_parts),
                        args[2].clone(),
                        false,
                    )?;
                }
                Ok(args[2].clone())
            }
            "define-key-after" => {
                need_arg_range(name, args, 3, 4)?;
                let key = key_sequence_binding_text(&args[1])?;
                let key_parts = key_sequence_keymap_parts(&args[1])?;
                let after = args
                    .get(3)
                    .filter(|value| !value.is_nil())
                    .map(key_sequence_keymap_parts)
                    .transpose()?;
                keymap_define_binding_after(
                    interp,
                    &args[0],
                    &key,
                    Some(key_parts),
                    args[2].clone(),
                    after.as_deref(),
                )?;
                Ok(Value::Nil)
            }
            "bindings--define-key" => {
                need_args(name, args, 3)?;
                let key = key_sequence_binding_text(&args[1])?;
                let key_parts = key_sequence_keymap_parts(&args[1])?;
                keymap_define_binding_with_placement(
                    interp,
                    &args[0],
                    &key,
                    Some(key_parts),
                    args[2].clone(),
                    false,
                )?;
                Ok(Value::Nil)
            }
            "keymap-set" => {
                need_args(name, args, 3)?;
                let key = textual_key_sequence_binding_text(&args[1])?;
                let key_parts = textual_key_sequence_keymap_parts(&args[1])?;
                keymap_define_binding_with_placement(
                    interp,
                    &args[0],
                    &key,
                    Some(key_parts),
                    args[2].clone(),
                    true,
                )?;
                Ok(args[2].clone())
            }
            "keymap-unset" => {
                need_arg_range(name, args, 2, 3)?;
                let key = textual_key_sequence_binding_text(&args[1])?;
                if args.get(2).is_some_and(Value::is_truthy) {
                    keymap_remove_binding(interp, &args[0], &key)?;
                } else {
                    let key_parts = textual_key_sequence_keymap_parts(&args[1])?;
                    keymap_define_binding_with_placement(
                        interp,
                        &args[0],
                        &key,
                        Some(key_parts),
                        Value::Nil,
                        true,
                    )?;
                }
                Ok(Value::Nil)
            }
            "lookup-key" => {
                need_arg_range(name, args, 2, 3)?;
                let key_parts = key_sequence_keymap_parts(&args[1])?;
                let result = keymap_lookup_sequence_value_with_default(
                    interp,
                    &args[0],
                    &key_parts,
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )?;
                if let Value::Integer(prefix_len) = result {
                    let prefix_len = usize::try_from(prefix_len).unwrap_or(0);
                    Ok(Value::Integer(
                        key_sequence_prefix_event_count(&args[1], prefix_len)? as i64,
                    ))
                } else {
                    Ok(result)
                }
            }
            "keymap-lookup" => {
                if args.len() < 2 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let key_parts = textual_key_sequence_keymap_parts(&args[1])?;
                keymap_lookup_sequence_value_with_default(
                    interp,
                    &args[0],
                    &key_parts,
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "accessible-keymaps" => accessible_keymaps(interp, args, env),
            "current-minor-mode-maps" => {
                need_args(name, args, 0)?;
                Ok(Value::list(
                    active_minor_mode_bindings(interp, env)?
                        .into_iter()
                        .map(|(_, map)| map),
                ))
            }
            "minor-mode-key-binding" => {
                need_arg_range(name, args, 1, 2)?;
                let key_parts = key_sequence_keymap_parts(&args[0])?;
                let accept_default = args.get(1).is_some_and(Value::is_truthy);
                let mut prefix_bindings = Vec::new();
                for (mode, map) in active_minor_mode_bindings(interp, env)? {
                    let binding = keymap_lookup_sequence_value_with_default(
                        interp,
                        &map,
                        &key_parts,
                        accept_default,
                        env,
                    )?;
                    if binding.is_nil() || matches!(binding, Value::Integer(_)) {
                        continue;
                    }
                    let entry = Value::cons(Value::Symbol(mode.into()), binding.clone());
                    if keymap_reference_map(interp, &binding, env).is_some() {
                        prefix_bindings.push(entry);
                    } else if prefix_bindings.is_empty() {
                        return Ok(Value::list([entry]));
                    }
                }
                Ok(Value::list(prefix_bindings))
            }
            "keymap--read-only-filter" => {
                need_args(name, args, 1)?;
                if interp
                    .lookup_var("buffer-read-only", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    Ok(args[0].clone())
                } else {
                    Ok(Value::Nil)
                }
            }
            "keymap-read-only-bind" => {
                need_args(name, args, 1)?;
                Ok(Value::list([
                    Value::Symbol("menu-item".into()),
                    Value::String(String::new().into()),
                    args[0].clone(),
                    Value::Symbol(":filter".into()),
                    Value::list([
                        Value::Symbol("function".into()),
                        Value::Symbol("keymap--read-only-filter".into()),
                    ]),
                ]))
            }
            "keymap--get-keyelt" => {
                need_args(name, args, 2)?;
                keymap_get_keyelt(interp, &args[0], args[1].is_truthy(), env)
            }
            "describe-buffer-bindings" => describe_buffer_bindings(interp, args, env),
            "help--describe-vector" => help_describe_vector(interp, args, env),
            "key-binding" => {
                need_arg_range(name, args, 1, 4)?;
                let key = key_sequence_binding_text(&args[0])?;
                key_binding(
                    interp,
                    &key,
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "keymap-prompt" => {
                need_args(name, args, 1)?;
                if let Some(id) = keymap_record_id(interp, &args[0]) {
                    return Ok(interp
                        .find_record(id)
                        .and_then(|record| record.slots.first().cloned())
                        .unwrap_or(Value::Nil));
                }
                if let Ok(items) = args[0].to_vec()
                    && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "keymap")
                {
                    return Ok(items
                        .iter()
                        .skip(1)
                        .find(|item| string_like(item).is_some())
                        .cloned()
                        .unwrap_or(Value::Nil));
                }
                Err(LispError::TypeError("keymapp".into(), args[0].type_name()))
            }
            "command-remapping" => {
                need_arg_range(name, args, 1, 3)?;
                command_remapping(interp, &args[0], args.get(2), env)
            }
            "keymap-parent" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::Record(id)
                        if interp
                            .find_record(*id)
                            .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
                    {
                        Ok(interp
                            .find_record(*id)
                            .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT).cloned())
                            .unwrap_or(Value::Nil))
                    }
                    _ => Ok(Value::Nil),
                }
            }
            "set-keymap-parent" => {
                need_args(name, args, 2)?;
                if let Value::Record(id) = &args[0]
                    && let Some(record) = interp.find_record_mut(*id)
                    && record.type_name == KEYMAP_RECORD_TYPE
                {
                    if record.slots.len() <= KEYMAP_PARENT_SLOT {
                        record.slots.resize(KEYMAP_PARENT_SLOT + 1, Value::Nil);
                    }
                    record.slots[KEYMAP_PARENT_SLOT] = args[1].clone();
                }
                Ok(Value::Nil)
            }
            "map-keymap" => {
                need_arg_range(name, args, 2, 3)?;
                if args.get(2).is_some_and(Value::is_truthy) {
                    return interp.call_function_value(
                        Value::Symbol("map-keymap-sorted".into()),
                        Some("map-keymap-sorted"),
                        &args[..2],
                        env,
                    );
                }
                // GNU map-keymap also reports bindings inherited from parent
                // keymaps (the parent is spliced into the keymap's tail).
                let mut visited = std::collections::HashSet::new();
                map_keymap_value(interp, &args[0], &args[1], env, &mut visited)?;
                Ok(Value::Nil)
            }
            "map-keymap-internal" => {
                need_args(name, args, 2)?;
                if !is_keymap_value(interp, &args[1]) {
                    return Err(LispError::TypeError("keymapp".into(), args[1].type_name()));
                }
                map_keymap_direct_value(interp, &args[0], &args[1], env)?;
                Ok(keymap_parent_values(interp, &args[1])
                    .into_iter()
                    .next()
                    .unwrap_or(Value::Nil))
            }
            "describe-vector" => describe_vector_value(interp, args, env),
            "suppress-keymap" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(args[0].clone())
            }
            "use-local-map" => {
                need_args(name, args, 1)?;
                if !args[0].is_nil() && !is_keymap_value(interp, &args[0]) {
                    return Err(LispError::TypeError("keymapp".into(), args[0].type_name()));
                }
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "current-local-map",
                    args[0].clone(),
                );
                Ok(Value::Nil)
            }
            "use-global-map" => {
                need_args(name, args, 1)?;
                if !is_keymap_value(interp, &args[0]) {
                    return Err(LispError::TypeError("keymapp".into(), args[0].type_name()));
                }
                interp.set_current_global_map_value(args[0].clone());
                Ok(Value::Nil)
            }
            "current-local-map" => {
                need_args(name, args, 0)?;
                Ok(interp
                    .lookup_var("current-local-map", env)
                    .unwrap_or(Value::Nil))
            }
            "current-global-map" => {
                need_args(name, args, 0)?;
                Ok(interp.current_global_map_value())
            }
            "global-set-key" => {
                need_args(name, args, 2)?;
                let key = key_sequence_binding_text(&args[0])?;
                let key_parts = key_sequence_keymap_parts(&args[0])?;
                let global_map = interp.current_global_map_value();
                keymap_define_binding_with_placement(
                    interp,
                    &global_map,
                    &key,
                    Some(key_parts),
                    args[1].clone(),
                    true,
                )?;
                Ok(args[1].clone())
            }
            "local-set-key" => {
                need_args(name, args, 2)?;
                Ok(args[1].clone())
            }
            "global-unset-key" => {
                need_args(name, args, 1)?;
                let key = key_sequence_binding_text(&args[0])?;
                let global_map = interp.current_global_map_value();
                keymap_remove_binding(interp, &global_map, &key)?;
                Ok(Value::Nil)
            }
            "local-unset-key" => {
                need_args(name, args, 1)?;
                Ok(Value::Nil)
            }
            "substitute-key-definition" => {
                if args.len() < 3 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(args[2].clone())
            }
            "easy-menu-binding" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let item_name = args.get(1).cloned().unwrap_or(Value::Nil);
                Ok(Value::list([
                    Value::Symbol("menu-item".into()),
                    item_name,
                    args[0].clone(),
                ]))
            }
            "easy-menu-add-item" => {
                if args.len() < 3 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(args[2].clone())
            }
            #[dispatch(builtin_override)]
            "tool-bar-local-item" => {
                if args.len() < 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(args[3].clone())
            }
            #[dispatch(builtin_override)]
            "tool-bar-local-item-from-menu" => {
                if args.len() < 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(args[2].clone())
            }
            "custom-add-choice" => {
                need_args(name, args, 2)?;
                let variable = args[0].as_symbol()?;
                let choice = args[1].clone();
                let choices = interp
                    .get_symbol_property(variable, "custom-type")
                    .unwrap_or(Value::Nil);
                let mut entries = choices.to_vec()?;
                if !matches!(entries.first(), Some(Value::Symbol(kind)) if kind == "choice") {
                    return Err(LispError::Signal(format!("Not a choice type: {choices}")));
                }
                let new_tag = custom_choice_tag(&choice);
                let already_present = new_tag.as_ref().is_some_and(|tag| {
                    entries[1..]
                        .iter()
                        .filter_map(custom_choice_tag)
                        .any(|existing| values_equal(interp, &existing, tag))
                });
                if !already_present {
                    entries.push(choice);
                    interp.put_symbol_property(variable, "custom-type", Value::list(entries));
                }
                Ok(Value::Nil)
            }
            "custom-add-option" => {
                need_args(name, args, 2)?;
                let variable = args[0].as_symbol()?;
                let option = args[1].clone();
                let existing = interp
                    .get_symbol_property(variable, "custom-options")
                    .unwrap_or(Value::Nil);
                let mut options = existing.to_vec()?;
                if !options
                    .iter()
                    .any(|existing| values_equal(interp, existing, &option))
                {
                    options.push(option);
                }
                let updated = Value::list(options);
                interp.put_symbol_property(variable, "custom-options", updated.clone());
                Ok(updated)
            }
            "define-widget" => {
                if args.len() < 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let name = args[0].as_symbol()?;
                let class = args[1].clone();
                let doc = args[2].clone();
                let widget_type = if args.len() > 3 {
                    Value::cons(class, Value::list(args[3..].to_vec()))
                } else {
                    Value::list([class])
                };
                interp.put_symbol_property(name, "widget-type", widget_type);
                interp.put_symbol_property(name, "widget-documentation", doc);
                Ok(Value::Symbol(name.to_string().into()))
            }
            "widget-create" => {
                need_args(name, args, 1)?;
                if let Some(label) = args.iter().skip(1).find_map(string_like) {
                    interp.buffer.insert(&label.text);
                }
                Ok(Value::cons(
                    args[0].clone(),
                    Value::list(args[1..].to_vec()),
                ))
            }
            "widget-get" => {
                need_args(name, args, 2)?;
                widget_get(interp, &args[0], &args[1])
            }
            "widget-put" => {
                need_args(name, args, 3)?;
                widget_put(interp, &args[0], &args[1], args[2].clone())
            }
            "widget-apply" => {
                need_arg_range(name, args, 2, usize::MAX)?;
                let function = widget_get(interp, &args[0], &args[1])?;
                if function.is_nil() {
                    return Ok(Value::Nil);
                }
                let mut call_args = Vec::with_capacity(args.len());
                call_args.push(args[0].clone());
                call_args.extend_from_slice(&args[2..]);
                interp.call_function_value(function, args[1].as_symbol().ok(), &call_args, env)
            }
            "define-button-type" => {
                need_args(name, args, 1)?;
                Ok(args[0].clone())
            }
            "display-mouse-p" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "make-button" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if args[0].is_nil() || args[1].is_nil() {
                    return Ok(Value::Nil);
                }
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                let mut cursor = 2usize;
                while cursor + 1 < args.len() {
                    if let Ok(property) = args[cursor].as_symbol() {
                        let property = if property == "type" {
                            "button-type"
                        } else {
                            property
                        };
                        interp.buffer.put_text_property(
                            start,
                            end,
                            property,
                            args[cursor + 1].clone(),
                        );
                    }
                    cursor += 2;
                }
                Ok(Value::Integer(start as i64))
            }
            #[dispatch(builtin_override)]
            "push-button" => {
                need_arg_range(name, args, 0, 2)?;
                let pos = args
                    .first()
                    .filter(|value| !value.is_nil())
                    .map(|value| position_from_value(interp, value))
                    .transpose()?
                    .unwrap_or_else(|| interp.buffer.point());
                interp.buffer.goto_char(pos);
                if point_is_on_plain_backtrace_ellipsis(interp, pos) {
                    reprint_current_backtrace_frame_for_expansion(interp, env, true)?;
                    return Ok(Value::T);
                }
                let button = interp.call_function_value(
                    Value::Symbol("button-at".into()),
                    Some("button-at"),
                    &[Value::Integer(pos as i64)],
                    env,
                )?;
                if button.is_nil() {
                    return Ok(Value::Nil);
                }
                let use_mouse_action = args.get(1).cloned().unwrap_or(Value::Nil);
                interp.call_function_value(
                    Value::Symbol("button-activate".into()),
                    Some("button-activate"),
                    &[button, use_mouse_action],
                    env,
                )?;
                Ok(Value::T)
            }
            "button-at" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                Ok(interp
                    .buffer
                    .text_property_at(pos, "button-type")
                    .map(|_| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil))
            }
            "button-type" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                Ok(interp
                    .buffer
                    .text_property_at(pos, "button-type")
                    .unwrap_or(Value::Nil))
            }
            "defined-colors" => {
                need_args(name, args, 0)?;
                Ok(Value::list([
                    Value::String("black".into()),
                    Value::String("white".into()),
                    Value::String("red".into()),
                    Value::String("green".into()),
                    Value::String("blue".into()),
                ]))
            }
            "color-defined-p" => {
                need_args(name, args, 1)?;
                let color = string_text(&args[0])?;
                Ok(
                    if ["black", "white", "red", "green", "blue"]
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(&color))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "symbol-function" => {
                need_args(name, args, 1)?;
                let symbol = args[0].as_symbol()?;
                if interp.logical_function_binding(symbol, env).is_none()
                    && let Some(macro_function) = interp.macro_binding_as_function(symbol)
                {
                    // GNU's function cell for a macro holds (macro . EXPANDER).
                    // Materialize the synthesized cell as the function binding so
                    // in-place mutation of the SAME cons (nadvice's setcdr-based
                    // macro advice) is visible to later reads and expansion.
                    interp.set_function_binding(symbol, Some(macro_function.clone()));
                    return Ok(macro_function);
                }
                if let Some(wrapper) = materialize_preloaded_lisp_function(interp, symbol, env) {
                    // The implementation stays native, but GNU's dumped
                    // function cell is compiled Lisp.  Materialize that stable
                    // observable wrapper on first inspection so help, advice,
                    // and byte-code-function-p see the dumped contract without
                    // moving the implementation across the Lisp/host boundary.
                    return Ok(wrapper);
                }
                Ok(match interp.logical_function_binding(symbol, env) {
                    Some(value) => value,
                    None if is_special_form_name(symbol) => {
                        Value::BuiltinFunc(symbol.to_string().into())
                    }
                    None if symbol == "benchmark-run" => Value::list([
                        Value::Symbol("autoload".into()),
                        Value::String("benchmark.el".into()),
                        Value::String("Autoloaded benchmark-run.".into()),
                        Value::Nil,
                        Value::Nil,
                    ]),
                    None if symbol == "tetris" => Value::list([
                        Value::Symbol("autoload".into()),
                        Value::String("tetris.el".into()),
                        Value::String("Autoloaded tetris.".into()),
                        Value::T,
                        Value::Nil,
                    ]),
                    // GNU returns nil for an unbound function cell (nadvice's
                    // pending-advice path reads it).
                    None => Value::Nil,
                })
            }
            "symbol-file" => {
                need_arg_range(name, args, 1, 3)?;
                // The `ert--test' type resolves from the native test registry.
                if args.get(1).and_then(|t| t.as_symbol().ok()) == Some("ert--test")
                    && let Ok(symbol) = args[0].as_symbol()
                {
                    return Ok(interp
                        .ert_tests
                        .iter()
                        .find(|test| test.name == symbol)
                        .and_then(|test| test.source_file.clone())
                        .map(|value| Value::String(value.into()))
                        .unwrap_or(Value::Nil));
                }
                if let Some(file) = symbol_file_from_load_history(
                    interp,
                    &args[0],
                    args.get(1).and_then(|kind| kind.as_symbol().ok()),
                    env,
                ) {
                    // simple_compat.el is Emaxx's aggregate realization of
                    // GNU's dumped files, not their logical source.  Preserve
                    // later user redefinitions, but map definitions from this
                    // one bootstrap aggregate back to the owning GNU file.
                    if file.ends_with("/src/lisp/simple_compat.el")
                        && let Ok(symbol) = args[0].as_symbol()
                        && let Some(path) = symbol_file_from_preloaded_sources(interp, symbol)
                    {
                        return Ok(Value::String(path.into()));
                    }
                    return Ok(Value::String(file.into()));
                }
                // GNU consults the dumped load-history; stand in for it with
                // the preloaded sources for defun/defvar/typeless queries.
                if matches!(
                    args.get(1).and_then(|t| t.as_symbol().ok()),
                    None | Some("defun") | Some("defvar")
                ) && let Ok(symbol) = args[0].as_symbol()
                    // A numeric documentation property is the native-variable
                    // provenance marker installed by `Snarf-documentation'.
                    // GNU returns nil here and lets help-fns.el resolve the C
                    // source; a preloaded Lisp textual match must not mask it.
                    && !(args.get(1).and_then(|t| t.as_symbol().ok()) == Some("defvar")
                        && matches!(
                            interp.get_symbol_property(symbol, "variable-documentation"),
                            Some(Value::Integer(_))
                        ))
                    && let Some(path) = symbol_file_from_preloaded_sources(interp, symbol)
                {
                    return Ok(Value::String(path.into()));
                }
                Ok(Value::Nil)
            }
            "symbol-name" => {
                need_args(name, args, 1)?;
                let s = args[0].as_symbol()?;
                Ok(Value::String(
                    crate::lisp::types::visible_symbol_name(s)
                        .to_string()
                        .into(),
                ))
            }
            "user-login-name" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if args.first().is_none_or(Value::is_nil) {
                    return Ok(interp
                        .lookup_var("user-login-name", env)
                        .unwrap_or_else(|| {
                            Value::String(
                                current_user_login_name()
                                    .unwrap_or_else(|| "user".into())
                                    .into(),
                            )
                        }));
                }
                let uid = legacy_unsigned_id(&args[0])?;
                Ok(user_name_from_uid(uid)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "user-real-login-name" => {
                need_args(name, args, 0)?;
                Ok(interp
                    .lookup_var("user-real-login-name", env)
                    .unwrap_or_else(|| {
                        Value::String(
                            current_real_user_login_name()
                                .unwrap_or_else(|| "user".into())
                                .into(),
                        )
                    }))
            }
            "system-name" => {
                if !args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(Value::String(system_name_value().into()))
            }
            "user-full-name" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let Some(requested) = args.first().filter(|value| !value.is_nil()) else {
                    return Ok(interp.lookup_var("user-full-name", env).unwrap_or_else(|| {
                        Value::String(
                            current_user_full_name()
                                .unwrap_or_else(|| "unknown".into())
                                .into(),
                        )
                    }));
                };
                let full_name = match requested {
                    Value::Integer(_) | Value::BigInteger(_) | Value::Float(_) => {
                        user_full_name_from_uid(legacy_unsigned_id(requested)?)
                    }
                    Value::String(_) | Value::StringObject(_) => {
                        let login = string_text(requested)?;
                        user_full_name(Some(&login))
                    }
                    _ => return Err(LispError::Signal("Invalid UID specification".into())),
                };
                Ok(full_name
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "char-from-name" => {
                need_args(name, args, 1)?;
                let name = string_text(&args[0])?;
                let ch = match name.as_str() {
                    "SMILE" => 0x263A,
                    _ => return Ok(Value::Nil),
                };
                Ok(Value::Integer(ch))
            }
            "always" => Ok(Value::T),
            "evenp" => {
                need_args(name, args, 1)?;
                Ok(
                    if (&integer_like_bigint(interp, &args[0])? & BigInt::from(1u8)).is_zero() {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "seq-subseq" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                seq_subseq(
                    &args[0],
                    args[1].as_integer()?,
                    args.get(2).map(Value::as_integer).transpose()?,
                )
            }
            "text-quoting-style" => {
                need_args(name, args, 0)?;
                Ok(Value::Symbol(
                    effective_text_quoting_style(interp, env).into(),
                ))
            }
            "file-truename" => {
                need_args(name, args, 1)?;
                let requested = string_text(&args[0])?;
                let path = resolve_file_name_in_env(interp, env, &requested);
                let canonical = canonical_file_name(&path);
                Ok(Value::String(
                    (if parse_remote_file_name(&requested).is_none() {
                        quote_local_file_name_if_needed(canonical)
                    } else {
                        canonical
                    })
                    .into(),
                ))
            }
            "user-uid" => Ok(Value::Integer(current_user_id()? as i64)),
            "user-real-uid" => Ok(Value::Integer(current_real_user_id()? as i64)),
            "group-gid" => Ok(Value::Integer(current_group_id()? as i64)),
            "group-real-gid" => Ok(Value::Integer(current_real_group_id()? as i64)),
            "group-name" => {
                need_args(name, args, 1)?;
                if !matches!(
                    args[0],
                    Value::Integer(_) | Value::BigInteger(_) | Value::Float(_) | Value::Cons(_)
                ) {
                    return Err(LispError::Signal("Invalid GID specification".into()));
                }
                let gid = legacy_unsigned_id(&args[0])?;
                Ok(group_name_from_gid(i64::from(gid))?
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            #[dispatch(resets_undo)]
            "save-buffer" => {
                let Some(path) = interp.buffer.file.clone() else {
                    return Ok(Value::Nil);
                };
                if !interp.buffer.is_modified() {
                    return Ok(Value::Nil);
                }
                let buffer_text = interp.buffer.full_buffer_string();
                if std::fs::read_to_string(&path).is_ok_and(|contents| contents == buffer_text) {
                    interp.buffer.set_unmodified();
                    interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
                    unlock_current_buffer(interp, env)?;
                    return Ok(Value::Nil);
                }
                ensure_no_supersession_threat(interp, env)?;
                if run_write_buffer_hooks_until_success(interp, env)? {
                    return Ok(Value::Nil);
                }
                std::fs::write(&path, &buffer_text)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                interp.buffer.set_unmodified();
                interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
                unlock_current_buffer(interp, env)?;
                Ok(Value::Nil)
            }
            "emaxx-default-revert-buffer-function" | "revert-buffer--default" => {
                revert_current_buffer(interp, env)?;
                Ok(Value::Nil)
            }
            "buffer-stale--default-function" => {
                need_arg_range(name, args, 0, 1)?;
                let Some(path) = interp.buffer.file.clone() else {
                    return Ok(Value::Nil);
                };
                if interp.buffer.is_modified() || !Path::new(&path).is_file() {
                    return Ok(Value::Nil);
                }
                let current = file_modtime(&path)?;
                let visited = interp.buffer.visited_file_modtime();
                // Tramp reports remote modification times with one-second
                // resolution; a same-second rewrite looks unchanged.
                let unchanged = if interp
                    .buffer_local_value(interp.current_buffer_id(), "emaxx--visited-remote-prefix")
                    .is_some_and(|value| value.is_truthy())
                {
                    modtimes_equal_whole_seconds(&visited, &current)
                } else {
                    visited == current
                };
                Ok(if unchanged { Value::Nil } else { Value::T })
            }
            "revert-buffer" => {
                if let Some(revert_function) = interp.lookup_var("revert-buffer-function", env)
                    && revert_function.is_truthy()
                {
                    let mut revert_args = Vec::with_capacity(args.len() + 1);
                    revert_args.push(Value::Symbol("emaxx-default-revert-buffer-function".into()));
                    revert_args.extend(args.iter().cloned());
                    return interp.call_function_value(revert_function, None, &revert_args, env);
                }
                revert_current_buffer(interp, env)?;
                Ok(Value::Nil)
            }
            "lock-buffer" => {
                maybe_lock_current_buffer(interp, env)?;
                Ok(Value::Nil)
            }
            "unlock-buffer" => unlock_current_buffer(interp, env),
            "ask-user-about-supersession-threat" => {
                need_args(name, args, 1)?;
                Ok(Value::T)
            }
            // File-less fallback; GNU nadvice.el owns advice once loaded (direct
            // dispatch, e.g. define-advice's lowering, must delegate to it).
            "advice-add" => {
                if let Some(delegated) = delegate_to_lisp_function(interp, name, args, env)? {
                    return Ok(delegated);
                }
                if args.len() < 3 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let function_name = symbol_designator_name(&args[0])
                    .ok_or_else(|| LispError::TypeError("symbol".into(), args[0].type_name()))?;
                let where_kind = args[1].as_symbol()?.to_string();
                // Keep the advice value unresolved (a symbol stays a symbol,
                // like GNU) so later redefinitions of a symbol advice apply.
                let advice = args[2].clone();
                let advice_name =
                    args.get(3)
                        .and_then(|props| props.to_vec().ok())
                        .and_then(|props| {
                            props.iter().find_map(|prop| {
                                let (key, value) = prop.cons_values()?;
                                matches!(&key, Value::Symbol(key) if key == "name").then_some(value)
                            })
                        });
                let base = interp.lookup_function(&function_name, env).ok();
                let state = interp
                    .advice_registry
                    .entry(function_name.clone())
                    .or_default();
                if state.base.is_none() {
                    state.base = base;
                }
                state.entries.insert(
                    0,
                    crate::lisp::eval::AdviceEntry {
                        where_kind,
                        function: advice,
                        name: advice_name,
                    },
                );
                interp.advice_reapply(&function_name);
                Ok(Value::Nil)
            }
            // File-less fallback; GNU nadvice.el owns advice once loaded.
            "advice-member-p" => {
                if let Some(delegated) = delegate_to_lisp_function(interp, name, args, env)? {
                    return Ok(delegated);
                }
                need_args(name, args, 2)?;
                let Some(function_name) = symbol_designator_name(&args[1]) else {
                    return Ok(Value::Nil);
                };
                let entries = interp
                    .advice_registry
                    .get(&function_name)
                    .map(|state| state.entries.clone())
                    .unwrap_or_default();
                let target = args[0].clone();
                for entry in &entries {
                    if interp.advice_functions_match(&entry.function, &target)
                        || entry.name.as_ref() == Some(&target)
                    {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            }
            // File-less fallback; GNU nadvice.el owns advice once loaded.
            "advice-remove" => {
                if let Some(delegated) = delegate_to_lisp_function(interp, name, args, env)? {
                    return Ok(delegated);
                }
                need_args(name, args, 2)?;
                let function_name = symbol_designator_name(&args[0])
                    .ok_or_else(|| LispError::TypeError("symbol".into(), args[0].type_name()))?;
                let target = args[1].clone();
                let entries = interp
                    .advice_registry
                    .get(&function_name)
                    .map(|state| state.entries.clone())
                    .unwrap_or_default();
                let mut kept = Vec::with_capacity(entries.len());
                let mut removed = false;
                for entry in entries {
                    if !removed
                        && (interp.advice_functions_match(&entry.function, &target)
                            || entry.name.as_ref() == Some(&target))
                    {
                        removed = true;
                        continue;
                    }
                    kept.push(entry);
                }
                if removed && let Some(state) = interp.advice_registry.get_mut(&function_name) {
                    state.entries = kept;
                    interp.advice_reapply(&function_name);
                }
                Ok(Value::Nil)
            }
            "emaxx-apply-around-advice" => {
                need_args(name, args, 3)?;
                let original = args[0].clone();
                let advice = args[1].clone();
                let mut advice_args = Vec::with_capacity(1 + args[2].to_vec()?.len());
                advice_args.push(original);
                advice_args.extend(args[2].to_vec()?);
                interp.call_function_value(advice, None, &advice_args, env)
            }
            "emaxx-apply-after-advice" => {
                need_args(name, args, 3)?;
                let original = args[0].clone();
                let advice = args[1].clone();
                let original_args = args[2].to_vec()?;
                let result = interp.call_function_value(original, None, &original_args, env)?;
                interp.call_function_value(advice, None, &original_args, env)?;
                Ok(result)
            }
            // File-less fallback; GNU nadvice.el's macro takes over once loaded.
            "remove-function" if !interp.has_lisp_macro("remove-function") => {
                need_args(name, args, 2)?;
                Ok(Value::Nil)
            }
            "userlock--handle-unlock-error" => Ok(Value::Nil),
            "recent-auto-save-p" => {
                need_args(name, args, 0)?;
                Ok(if interp.buffer.is_autosaved() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-buffer-auto-saved" => {
                need_args(name, args, 0)?;
                interp.buffer.set_autosaved();
                Ok(Value::Nil)
            }
            "clear-buffer-auto-save-failure" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "next-read-file-uses-dialog-p" => {
                need_args(name, args, 0)?;
                let use_dialog = interp
                    .lookup_var("use-dialog-box", env)
                    .is_some_and(|value| value.is_truthy());
                let use_file_dialog = interp
                    .lookup_var("use-file-dialog", env)
                    .is_some_and(|value| value.is_truthy());
                Ok(if use_dialog && use_file_dialog {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "auto-save-mode" => {
                let enabled = args.first().is_none_or(Value::is_truthy);
                if enabled {
                    let path = auto_save_path_for_buffer(&interp.buffer);
                    interp.set_buffer_local_value(
                        interp.current_buffer_id(),
                        "buffer-auto-save-file-name",
                        Value::String(path.into()),
                    );
                    Ok(Value::T)
                } else {
                    interp.set_buffer_local_value(
                        interp.current_buffer_id(),
                        "buffer-auto-save-file-name",
                        Value::Nil,
                    );
                    Ok(Value::Nil)
                }
            }
            "do-auto-save" => {
                let path = interp
                    .buffer_local_value(interp.current_buffer_id(), "buffer-auto-save-file-name")
                    .and_then(|value| string_text(&value).ok())
                    .unwrap_or_else(|| auto_save_path_for_buffer(&interp.buffer));
                std::fs::write(&path, interp.buffer.buffer_string())
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::String(path.into()),
                );
                interp.buffer.set_autosaved();
                Ok(Value::Nil)
            }
            "unix-sync" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "set-binary-mode" => {
                need_args(name, args, 2)?;
                match &args[0] {
                    Value::Symbol(stream)
                        if matches!(stream.as_str(), "stdin" | "stdout" | "stderr") =>
                    {
                        Ok(Value::Nil)
                    }
                    _ => Err(LispError::Signal("Invalid stream".into())),
                }
            }
            "obarray-make" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if let Some(size) = args.first()
                    && size.as_integer()? < 0
                {
                    return Err(LispError::TypeError("natnump".into(), size.type_name()));
                }
                Ok(make_obarray(interp))
            }
            "obarrayp" => {
                need_args(name, args, 1)?;
                Ok(if is_obarray_like_value(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "obarray-clear" => {
                need_args(name, args, 1)?;
                clear_obarray(interp, &args[0])
            }
            "internal--obarray-buckets" => {
                need_args(name, args, 1)?;
                Ok(Value::list(
                    obarray_symbols(interp, &args[0])?
                        .into_iter()
                        .map(|symbol| Value::list([symbol])),
                ))
            }
            "define-hash-table-test" => {
                need_args(name, args, 3)?;
                let symbol = args[0].as_symbol()?;
                let spec = Value::list([args[1].clone(), args[2].clone()]);
                interp.put_symbol_property(symbol, "hash-table-test", spec.clone());
                Ok(spec)
            }
            "make-hash-table" => {
                let mut test = "eql".to_string();
                let mut size = Value::Integer(65);
                let mut rehash_size = Value::Float(1.5);
                let mut rehash_threshold = Value::Float(0.8125);
                let mut weakness = Value::Nil;
                let mut index = 0usize;
                while index + 1 < args.len() {
                    let key = args[index].as_symbol()?;
                    match key {
                        ":test" => {
                            test = match &args[index + 1] {
                                Value::Symbol(name) => name.to_string(),
                                Value::BuiltinFunc(name) => name.to_string(),
                                other => {
                                    return Err(LispError::TypeError(
                                        "symbol".into(),
                                        other.type_name(),
                                    ));
                                }
                            };
                        }
                        ":size" => size = args[index + 1].clone(),
                        ":rehash-size" => rehash_size = args[index + 1].clone(),
                        ":rehash-threshold" => rehash_threshold = args[index + 1].clone(),
                        ":weakness" => {
                            weakness = match &args[index + 1] {
                                Value::T => Value::Symbol("key-and-value".into()),
                                other => other.clone(),
                            };
                        }
                        ":purecopy" => {}
                        _ => {
                            return Err(LispError::Signal(format!(
                                "Invalid hash table parameter: {key}"
                            )));
                        }
                    }
                    index += 2;
                }
                if !matches!(test.as_str(), "eq" | "eql" | "equal")
                    && hash_table_user_test_functions(interp, &test).is_none()
                {
                    return Err(LispError::Signal("Invalid hash table test".into()));
                }
                let table = json::make_hash_table(interp, &test, Vec::new());
                let Value::Record(id) = table.clone() else {
                    unreachable!("hash tables are represented as records")
                };
                let record = interp
                    .find_record_mut(id)
                    .expect("make_hash_table should create a record");
                if record.slots.len() < 6 {
                    record.slots.resize(6, Value::Nil);
                }
                record.slots[2] = size;
                record.slots[3] = rehash_size;
                record.slots[4] = rehash_threshold;
                record.slots[5] = weakness;
                Ok(table)
            }
            "hash-table-p" => {
                need_args(name, args, 1)?;
                Ok(if json::is_hash_table(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "hash-table-contains-p" => {
                need_args(name, args, 2)?;
                if let Value::Record(id) = &args[1]
                    && let Some(value) = interp.equal_hash_lookup(*id, &args[0], env)
                {
                    return Ok(if value.is_some() {
                        Value::T
                    } else {
                        Value::Nil
                    });
                }
                let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[1].type_name(),
                    ));
                };
                for (existing_key, _) in entries {
                    if hash_table_key_matches(
                        interp,
                        &args[1],
                        &test,
                        &existing_key,
                        &args[0],
                        env,
                    )? {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            }
            "copy-hash-table" => {
                need_args(name, args, 1)?;
                let Value::Record(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                let Some(record) = interp.find_record(id) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                if record.type_name != "hash-table" {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                }
                interp.copy_record(id)
            }
            "gethash" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let default = args.get(2).cloned().unwrap_or(Value::Nil);
                if let Value::Record(id) = &args[1]
                    && let Some(value) = interp.equal_hash_lookup(*id, &args[0], env)
                {
                    return Ok(value.unwrap_or(default));
                }
                let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[1].type_name(),
                    ));
                };
                for (existing_key, value) in entries {
                    if hash_table_key_matches(
                        interp,
                        &args[1],
                        &test,
                        &existing_key,
                        &args[0],
                        env,
                    )? {
                        return Ok(value);
                    }
                }
                Ok(default)
            }
            "puthash" => {
                need_args(name, args, 3)?;
                if let Value::Record(id) = &args[2]
                    && interp.equal_hash_put(*id, args[0].clone(), args[1].clone(), env)
                {
                    return Ok(args[1].clone());
                }
                let Some((test, mut entries)) = json::hash_table_entries(interp, &args[2]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[2].type_name(),
                    ));
                };
                touch_hash_table_key(interp, &args[2], &test, &args[0], env)?;
                let mut replaced = false;
                for (existing_key, existing_value) in &mut entries {
                    if hash_table_key_matches(interp, &args[2], &test, existing_key, &args[0], env)?
                    {
                        *existing_value = args[1].clone();
                        replaced = true;
                        break;
                    }
                }
                if !replaced {
                    entries.push((args[0].clone(), args[1].clone()));
                }
                set_hash_table_entries(interp, &args[2], entries)?;
                Ok(args[1].clone())
            }
            "maphash" => {
                need_args(name, args, 2)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[1].type_name(),
                    ));
                };
                for (key, value) in entries {
                    call_function_value(interp, &args[0], &[key, value], env)?;
                }
                Ok(Value::Nil)
            }
            "remhash" => {
                need_args(name, args, 2)?;
                if let Value::Record(id) = &args[1]
                    && interp.equal_hash_remove(*id, &args[0], env).is_some()
                {
                    return Ok(Value::Nil);
                }
                let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[1].type_name(),
                    ));
                };
                let mut retained = Vec::new();
                for (existing_key, value) in entries {
                    if !hash_table_key_matches(
                        interp,
                        &args[1],
                        &test,
                        &existing_key,
                        &args[0],
                        env,
                    )? {
                        retained.push((existing_key, value));
                    }
                }
                set_hash_table_entries(interp, &args[1], retained)?;
                Ok(Value::Nil)
            }
            "clrhash" => {
                need_args(name, args, 1)?;
                if json::hash_table_entries(interp, &args[0]).is_none() {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                }
                set_hash_table_entries(interp, &args[0], Vec::new())?;
                Ok(args[0].clone())
            }
            "completion-table-case-fold" => {
                // Native fallback mirroring GNU minibuffer.el's defun (the
                // preloaded Lisp definition shadows this in full sessions).
                need_arg_range(name, args, 1, 2)?;
                let table = args[0].clone();
                let dont_fold = args.get(1).is_some_and(|value| value.is_truthy());
                let body = Value::list([
                    Value::Symbol("let".into()),
                    Value::list([Value::list([
                        Value::Symbol("completion-ignore-case".into()),
                        if dont_fold { Value::Nil } else { Value::T },
                    ])]),
                    Value::list([
                        Value::Symbol("complete-with-action".into()),
                        Value::Symbol("action".into()),
                        Value::list([Value::Symbol("quote".into()), table]),
                        Value::Symbol("string".into()),
                        Value::Symbol("pred".into()),
                    ]),
                ]);
                Ok(Value::lambda(
                    vec!["string".into(), "pred".into(), "action".into()].into(),
                    vec![body].into(),
                    crate::lisp::types::shared_env(Vec::new()),
                ))
            }
            "hash-table-count" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                Ok(Value::Integer(entries.len() as i64))
            }
            "hash-table-rehash-size" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(
                    interp,
                    &args[0],
                    3,
                    Value::Float(1.5),
                )?)
            }
            "hash-table-rehash-threshold" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(
                    interp,
                    &args[0],
                    4,
                    Value::Float(0.8125),
                )?)
            }
            "hash-table-size" => {
                need_args(name, args, 1)?;
                let default_size = json::hash_table_entries(interp, &args[0])
                    .map(|(_, entries)| Value::Integer(entries.len().max(65) as i64))
                    .unwrap_or(Value::Integer(65));
                Ok(hash_table_metadata_slot(interp, &args[0], 2, default_size)?)
            }
            "hash-table-test" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(
                    interp,
                    &args[0],
                    0,
                    Value::Symbol("eql".into()),
                )?)
            }
            "hash-table-weakness" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(interp, &args[0], 5, Value::Nil)?)
            }
            "hash-table-keys" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                Ok(Value::list(entries.into_iter().map(|(key, _)| key)))
            }
            "try-completion" => try_completion(interp, args, env),
            "all-completions" => all_completions(interp, args, env),
            "test-completion" => test_completion(interp, args, env),
            "internal-complete-buffer" => internal_complete_buffer(interp, args, env),
            "completion-metadata" => {
                need_args(name, args, 3)?;
                Ok(Value::list([Value::Symbol("metadata".into())]))
            }
            "completion-metadata-get" => {
                need_args(name, args, 2)?;
                let prop = args[1].as_symbol()?;
                let items = args[0].to_vec().unwrap_or_default();
                for item in &items {
                    let Some((key, value)) = item.cons_values() else {
                        continue;
                    };
                    if key.as_symbol().ok() == Some(prop) {
                        return Ok(value);
                    }
                }
                let keyword = format!(":{prop}");
                let mut index = 0usize;
                while index + 1 < items.len() {
                    if items[index].as_symbol().ok() == Some(keyword.as_str()) {
                        return Ok(items[index + 1].clone());
                    }
                    index += 1;
                }
                let extra = interp
                    .lookup_var("completion-extra-properties", env)
                    .unwrap_or(Value::Nil);
                let extra_items = extra.to_vec().unwrap_or_default();
                let mut index = 0usize;
                while index + 1 < extra_items.len() {
                    if extra_items[index].as_symbol().ok() == Some(keyword.as_str()) {
                        return Ok(extra_items[index + 1].clone());
                    }
                    index += 1;
                }
                Ok(Value::Nil)
            }
            "completion-all-completions" => {
                need_arg_range(name, args, 4, 5)?;
                let string = string_text(&args[0])?;
                let point = args[3].as_integer()?.max(0) as usize;
                let before_point = string.chars().take(point).collect::<String>();
                let completions = all_completions(
                    interp,
                    &[
                        Value::String(before_point.into()),
                        args[1].clone(),
                        args[2].clone(),
                    ],
                    env,
                )?;
                if completions.is_nil() {
                    return Ok(Value::Nil);
                }
                last_nconc_cell(&completions)?.set_cdr(Value::Integer(0))?;
                Ok(completions)
            }
            "completion-at-point" => {
                need_args(name, args, 0)?;
                // GNU runs `completion-at-point-functions' and completes the
                // region a matching capf returns; the dabbrev-style expansion
                // below only serves buffers with no capf spec.
                let capf_spec = super::call(
                    interp,
                    "run-hook-with-args-until-success",
                    &[Value::Symbol("completion-at-point-functions".into())],
                    env,
                )?;
                if let Ok(spec) = capf_spec.to_vec()
                    && spec.len() >= 3
                {
                    let resolve = |interp: &Interpreter, value: &Value| -> Option<usize> {
                        match value {
                            Value::Integer(pos) if *pos >= 0 => Some(*pos as usize),
                            Value::Marker(id) => interp.marker_position(*id),
                            _ => None,
                        }
                    };
                    if let (Some(beg), Some(end)) =
                        (resolve(interp, &spec[0]), resolve(interp, &spec[1]))
                    {
                        let table = spec[2].clone();
                        let mut predicate = Value::Nil;
                        let mut exit_function = Value::Nil;
                        let mut cursor = 3;
                        while cursor + 1 < spec.len() {
                            match &spec[cursor] {
                                Value::Symbol(key) if key == ":predicate" => {
                                    predicate = spec[cursor + 1].clone();
                                }
                                Value::Symbol(key) if key == ":exit-function" => {
                                    exit_function = spec[cursor + 1].clone();
                                }
                                _ => {}
                            }
                            cursor += 2;
                        }
                        let string = interp
                            .buffer
                            .buffer_substring(beg, end)
                            .map_err(|error| LispError::Signal(error.to_string()))?;
                        let string_value = Value::String(string.clone().into());
                        let call_exit = |interp: &mut Interpreter,
                                         env: &mut Env,
                                         text: &str,
                                         status: &str|
                         -> Result<(), LispError> {
                            if !exit_function.is_nil() {
                                call_function_value(
                                    interp,
                                    &exit_function,
                                    &[
                                        Value::String(text.to_string().into()),
                                        Value::Symbol(status.into()),
                                    ],
                                    env,
                                )?;
                            }
                            Ok(())
                        };
                        let comp = try_completion_with_styles(
                            interp,
                            &string_value,
                            &table,
                            &predicate,
                            env,
                        )?;
                        match comp {
                            Value::T => {
                                call_exit(interp, env, &string, "finished")?;
                                return Ok(Value::T);
                            }
                            ref comp if string_text(comp).is_ok() => {
                                let comp_text = string_text(comp)?;
                                if comp_text != string {
                                    delete_region_with_hooks(interp, beg, end, env)?;
                                    interp.buffer.goto_char(beg);
                                    insert_text_with_hooks(
                                        interp,
                                        &comp_text,
                                        &[],
                                        false,
                                        false,
                                        env,
                                    )?;
                                }
                                let comp_value = Value::String(comp_text.clone().into());
                                let exact = super::call(
                                    interp,
                                    "test-completion",
                                    &[comp_value.clone(), table.clone(), predicate.clone()],
                                    env,
                                )?
                                .is_truthy();
                                if exact {
                                    let sole = matches!(
                                        super::call(
                                            interp,
                                            "try-completion",
                                            &[comp_value, table, predicate],
                                            env,
                                        )?,
                                        Value::T
                                    );
                                    call_exit(
                                        interp,
                                        env,
                                        &comp_text,
                                        if sole { "finished" } else { "exact" },
                                    )?;
                                } else if comp_text == string {
                                    // No progress: with `completion-auto-help'
                                    // disabled GNU only messages; otherwise it
                                    // lists the candidates in *Completions*.
                                    if interp
                                        .lookup_var("completion-auto-help", env)
                                        .is_some_and(|value| value.is_nil())
                                    {
                                        let _ = call_function_value(
                                            interp,
                                            &Value::Symbol("minibuffer-message".into()),
                                            &[Value::String("Next char not unique".into())],
                                            env,
                                        );
                                        return Ok(Value::T);
                                    }
                                    let mut candidates = super::call(
                                        interp,
                                        "all-completions",
                                        &[string_value, table, predicate],
                                        env,
                                    )?
                                    .to_vec()
                                    .unwrap_or_default()
                                    .iter()
                                    .filter_map(|item| string_text(item).ok())
                                    .collect::<Vec<_>>();
                                    candidates.sort();
                                    let content = format!(
                                        "Possible completions are:\n{}\n",
                                        candidates.join("\n")
                                    );
                                    let buffer = super::call(
                                        interp,
                                        "get-buffer-create",
                                        &[Value::String("*Completions*".into())],
                                        env,
                                    )?;
                                    let completions_id = interp.resolve_buffer_id(&buffer)?;
                                    let saved_id = interp.current_buffer_id();
                                    interp.switch_to_buffer_id(completions_id)?;
                                    let (min, max) =
                                        (interp.buffer.point_min(), interp.buffer.point_max());
                                    if max > min {
                                        interp.delete_region_current_buffer(min, max).map_err(
                                            |error| LispError::Signal(error.to_string()),
                                        )?;
                                    }
                                    interp.insert_current_buffer(&content);
                                    interp.switch_to_buffer_id(saved_id)?;
                                    let _ = super::call(
                                        interp,
                                        "display-buffer",
                                        std::slice::from_ref(&buffer),
                                        env,
                                    )?;
                                }
                                return Ok(Value::T);
                            }
                            _ => {
                                let _ = call_function_value(
                                    interp,
                                    &Value::Symbol("minibuffer-message".into()),
                                    &[Value::String("No match".into())],
                                    env,
                                );
                                return Ok(Value::Nil);
                            }
                        }
                    }
                }
                if let Some(completion) = dabbrev_completion_at_point(interp, env)? {
                    let (start, end, expansion) = completion;
                    delete_region_with_hooks(interp, start, end, env)?;
                    interp.buffer.goto_char(start);
                    insert_text_with_hooks(interp, &expansion, &[], false, false, env)?;
                    return Ok(Value::T);
                }
                if interp
                    .lookup_var("completion-auto-help", env)
                    .is_none_or(|value| value.is_nil())
                {
                    let _ = call_function_value(
                        interp,
                        &Value::Symbol("minibuffer-message".into()),
                        &[Value::String("Next char not unique".into())],
                        env,
                    );
                }
                Ok(Value::Nil)
            }
            "minibuffer--sort-by-length-alpha" => {
                need_args(name, args, 1)?;
                let mut items = args[0].to_vec()?;
                items.sort_by(|left, right| {
                    let left_text = string_text(left).unwrap_or_default();
                    let right_text = string_text(right).unwrap_or_default();
                    left_text
                        .chars()
                        .count()
                        .cmp(&right_text.chars().count())
                        .then_with(|| left_text.cmp(&right_text))
                });
                Ok(Value::list(items))
            }
            "minibuffer-sort-alphabetically" => {
                need_args(name, args, 1)?;
                let mut items = args[0].to_vec()?;
                items.sort_by(|left, right| {
                    string_text(left)
                        .unwrap_or_default()
                        .cmp(&string_text(right).unwrap_or_default())
                });
                Ok(Value::list(items))
            }
            "map-pairs" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                Ok(Value::list(
                    entries
                        .into_iter()
                        .map(|(key, value)| Value::cons(key, value)),
                ))
            }
            "internal--hash-table-index-size" => {
                need_args(name, args, 1)?;
                let default_size = json::hash_table_entries(interp, &args[0])
                    .map(|(_, entries)| Value::Integer(entries.len().max(65) as i64))
                    .unwrap_or(Value::Integer(65));
                Ok(hash_table_metadata_slot(interp, &args[0], 2, default_size)?)
            }
            "internal--hash-table-histogram" => {
                need_args(name, args, 1)?;
                if json::hash_table_entries(interp, &args[0]).is_none() {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                }
                Ok(Value::Nil)
            }
            "internal--hash-table-buckets" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                Ok(Value::list(entries.into_iter().map(|(key, value)| {
                    Value::list([Value::cons(key, value)])
                })))
            }
            "profiler-memory-running-p" => Ok(if interp.profiler_memory_running {
                Value::T
            } else {
                Value::Nil
            }),
            "profiler-memory-start" => {
                if interp.profiler_memory_running {
                    return Err(LispError::Signal("Memory profiler already running".into()));
                }
                interp.profiler_memory_running = true;
                interp.profiler_memory_log_pending = true;
                Ok(Value::Nil)
            }
            "profiler-memory-stop" => {
                let was_running = interp.profiler_memory_running;
                interp.profiler_memory_running = false;
                Ok(if was_running { Value::T } else { Value::Nil })
            }
            "profiler-memory-log" => {
                if interp.profiler_memory_running || interp.profiler_memory_log_pending {
                    if !interp.profiler_memory_running {
                        interp.profiler_memory_log_pending = false;
                    }
                    Ok(Value::String("#<hash-table>".into()))
                } else {
                    Ok(Value::Nil)
                }
            }
            "profiler-cpu-running-p" => Ok(if interp.profiler_cpu_running {
                Value::T
            } else {
                Value::Nil
            }),
            "profiler-cpu-start" => {
                if interp.profiler_cpu_running {
                    return Err(LispError::Signal("CPU profiler already running".into()));
                }
                interp.profiler_cpu_running = true;
                interp.profiler_cpu_log_pending = true;
                if let Some(interval) = args.first() {
                    interp.set_variable(
                        "profiler-sampling-interval",
                        interval.clone(),
                        &mut Vec::new(),
                    );
                }
                Ok(Value::Nil)
            }
            "profiler-cpu-stop" => {
                let was_running = interp.profiler_cpu_running;
                interp.profiler_cpu_running = false;
                Ok(if was_running { Value::T } else { Value::Nil })
            }
            "profiler-cpu-log" => {
                if interp.profiler_cpu_running || interp.profiler_cpu_log_pending {
                    if !interp.profiler_cpu_running {
                        interp.profiler_cpu_log_pending = false;
                    }
                    Ok(Value::String("#<hash-table>".into()))
                } else {
                    Ok(Value::Nil)
                }
            }
            #[dispatch(builtin_override)]
            "byte-compile-check-lambda-list" => {
                need_args(name, args, 1)?;
                validate_lambda_params(&args[0])?;
                Ok(Value::Nil)
            }
            #[dispatch(builtin_override)]
            "byte-compile" => {
                need_args(name, args, 1)?;
                let (compile_target, suppressions) = byte_compile_target_and_suppressions(&args[0]);
                let compile_target = byte_compile_function_quoted_lambda_target(compile_target);
                // GNU oclosure.el's cconv integration errors when compiled code
                // setqs an oclosure slot not declared :mutable.
                check_oclosure_slot_mutation(interp, &compile_target)?;
                byte_compile_emit_warnings(interp, &compile_target, &suppressions, env)?;
                if let Ok(symbol) = compile_target.as_symbol() {
                    let callable = resolve_callable(interp, &compile_target, env)?;
                    let slots =
                        byte_code_function_slots(interp, Some(symbol), callable, None, false);
                    return Ok(interp.create_record("byte-code-function", slots));
                }
                if is_lambda_value(&compile_target) {
                    validate_lambda_form(&compile_target)?;
                    let lap = byte_code_decompile_lap(interp, &compile_target);
                    let capture_lexical = byte_compile_capture_lexical(interp, env);
                    let callable = byte_compile_lambda_callable(
                        interp,
                        env,
                        &compile_target,
                        capture_lexical,
                    )?;
                    let slots =
                        byte_code_function_slots(interp, None, callable, lap, !capture_lexical);
                    return Ok(interp.create_record("byte-code-function", slots));
                }
                if matches!(compile_target, Value::Lambda(_)) {
                    let slots =
                        byte_code_function_slots(interp, None, compile_target.clone(), None, false);
                    return Ok(interp.create_record("byte-code-function", slots));
                }
                Ok(compile_target)
            }
            #[dispatch(builtin_override)]
            "byte-compile-from-buffer" => byte_compile_from_buffer(interp, args, env),
            #[dispatch(builtin_override)]
            "byte-compile-file" => byte_compile_file(interp, args, env),
            #[dispatch(builtin_override)]
            "byte-compile--wide-docstring-p" => {
                need_args(name, args, 2)?;
                let docstring = string_text(&args[0])?;
                let max_width = args[1].as_integer()? as usize;
                Ok(if byte_compile_wide_docstring_p(&docstring, max_width) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            #[dispatch(builtin_override)]
            "byte-decompile-bytecode" => {
                need_args(name, args, 2)?;
                if args[0].is_list() {
                    Ok(args[0].clone())
                } else {
                    Ok(Value::Nil)
                }
            }
            "funcall-with-delayed-message" => {
                need_args(name, args, 3)?;
                let timeout = numeric_to_f64(interp, &args[0])?;
                let delayed = string_text(&args[1])?;
                let callback = resolve_callable(interp, &args[2], env)?;
                let buffer_id = interp
                    .find_buffer("*Messages*")
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer("*Messages*").0);
                let before = interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.buffer_string())
                    .unwrap_or_default();
                let start = Instant::now();
                let result = interp.call_function_value(callback, None, &[], env)?;
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed >= timeout
                    && let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id)
                {
                    let current = buffer.buffer_string();
                    let suffix = current
                        .strip_prefix(&before)
                        .map(str::to_string)
                        .unwrap_or(current);
                    let rewritten = if suffix.is_empty() {
                        format!("{delayed}\n")
                    } else {
                        format!("{delayed}\n{suffix}")
                    };
                    let end = buffer.point_max();
                    let _ = buffer.delete_region(1, end);
                    buffer.goto_char(1);
                    buffer.insert(&(before + &rewritten));
                }
                Ok(result)
            }
            "advice--cd*r" => {
                need_args(name, args, 1)?;
                Ok(args[0].clone())
            }
            "handler-bind-1" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if args.len().is_multiple_of(2) {
                    return Err(LispError::Signal(
                        "Trailing CONDITIONS without HANDLER in `handler-bind`".into(),
                    ));
                }
                let mut active = Vec::with_capacity(args.len() / 2);
                for pair in args[1..].chunks_exact(2) {
                    let conditions = match &pair[0] {
                        Value::Nil => Vec::new(),
                        Value::Cons(_) => pair[0]
                            .to_vec()?
                            .iter()
                            .map(|condition| condition.as_symbol().map(str::to_string))
                            .collect::<Result<Vec<_>, _>>()?,
                        condition => vec![condition.as_symbol()?.to_string()],
                    };
                    if !conditions.is_empty() {
                        active.push((conditions, pair[1].clone()));
                    }
                }
                active.reverse();
                let handler_start = interp.push_handler_bindings(&active);
                let result = interp.call_function_value(args[0].clone(), None, &[], env);
                interp.pop_handler_bindings(handler_start);
                result
            }
            "debugger-trap" => Ok(Value::Nil),
            "mapbacktrace" => {
                need_arg_range(name, args, 1, 2)?;
                let callback = resolve_callable(interp, &args[0], env)?;
                let base = args.get(1).filter(|value| !value.is_nil());
                let frames = interp.backtrace_frames_snapshot();
                let start = match base {
                    Some(base) => {
                        let Some(start) = frames
                            .iter()
                            .position(|(_, function, _, _)| function == base)
                        else {
                            // GNU get_backtrace_starting_at treats a base with
                            // no live activation frame as an empty traversal.
                            return Ok(Value::Nil);
                        };
                        start
                    }
                    None => 0,
                };
                for (evald, function, frame_args, debug_on_exit) in frames.into_iter().skip(start) {
                    let flags = if debug_on_exit {
                        Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
                    } else {
                        Value::Nil
                    };
                    let evald = if evald { Value::T } else { Value::Nil };
                    interp.call_function_value(
                        callback.clone(),
                        None,
                        &[evald, function, Value::list(frame_args), flags],
                        env,
                    )?;
                }
                Ok(Value::Nil)
            }
            "backtrace-frame--internal" => {
                need_args(name, args, 3)?;
                let callback = resolve_callable(interp, &args[0], env)?;
                let mut frame_offset = args[1].as_integer()?;
                if frame_offset < 0 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("natnump".into()),
                        args[1].clone(),
                    ])));
                }
                let mut base = args[2].clone();
                if let Value::Cons(cell) = &base {
                    frame_offset += cell.car.borrow().as_integer()?;
                    let function = cell.cdr.borrow().clone();
                    base = function;
                }
                if frame_offset < 0 {
                    return Ok(Value::Nil);
                }
                let frames = interp.backtrace_frames_snapshot();
                let start = if base.is_nil() {
                    0
                } else {
                    let Some(index) = frames
                        .iter()
                        .position(|(_, function, _, _)| function == &base)
                    else {
                        return Ok(Value::Nil);
                    };
                    index
                };
                let Some((evald, function, frame_args, debug_on_exit)) =
                    frames.into_iter().nth(start + frame_offset as usize)
                else {
                    return Ok(Value::Nil);
                };
                let flags = if debug_on_exit {
                    Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
                } else {
                    Value::Nil
                };
                let evald = if evald { Value::T } else { Value::Nil };
                interp.call_function_value(
                    callback,
                    None,
                    &[evald, function, Value::list(frame_args), flags],
                    env,
                )
            }
            "backtrace-debug" => {
                need_arg_range(name, args, 2, 3)?;
                interp.set_current_backtrace_debug(args[1].is_truthy());
                Ok(Value::Nil)
            }
            "backtrace-eval" => {
                need_arg_range(name, args, 2, 3)?;
                let index = usize::try_from(args[1].as_integer()?).unwrap_or(0);
                let base = args.get(2).filter(|value| !value.is_nil());
                let context = interp.backtrace_frame_context_env(index, base);
                let shared_context = shared_env(context);
                // Treat the suspended frames as captured lexical cells while the
                // expression runs.  `setq' then records changes by frame
                // identity, and the resumed activation observes them.
                interp.register_captured_lexical_frames(&shared_context);
                interp.eval(&args[0], &mut shared_context.borrow_mut())
            }
            "backtrace--locals" => {
                need_args(name, args, 2)?;
                let raw_index = args[0].as_integer()?;
                if raw_index < 0 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("natnump".into()),
                        args[0].clone(),
                    ])));
                }
                let index = raw_index as usize;
                let base = args.get(1).filter(|value| !value.is_nil());
                let locals = interp
                    .backtrace_frame_locals_snapshot_with_base(index, base)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(name, value)| Value::cons(Value::Symbol(name.into()), value))
                    .collect::<Vec<_>>();
                Ok(Value::list(locals))
            }
            #[dispatch(builtin_override)]
            "backtrace-expand-ellipses" => {
                need_arg_range(name, args, 0, 1)?;
                let no_limit = args.first().is_some_and(Value::is_truthy);
                reprint_current_backtrace_frame_for_expansion(interp, env, no_limit)
            }
            "current-thread" => Ok(interp.current_thread_value()),
            "all-threads" => {
                need_args(name, args, 0)?;
                Ok(Value::list(interp.live_threads()))
            }
            "make-thread" => {
                need_arg_range(name, args, 1, 3)?;
                let thread_name = args.get(1).and_then(|value| {
                    if value.is_nil() {
                        None
                    } else {
                        string_like(value).map(|string| string.text)
                    }
                });
                let disposition = match args.get(2) {
                    None | Some(Value::Nil) => BufferDisposition::Default,
                    Some(Value::T) => BufferDisposition::Preserve,
                    Some(Value::Symbol(symbol)) if symbol == "silently" => {
                        BufferDisposition::Silently
                    }
                    Some(other) => {
                        return Err(LispError::TypeError(
                            "thread-buffer-disposition".into(),
                            other.type_name(),
                        ));
                    }
                };
                interp.make_thread(args[0].clone(), thread_name, disposition)
            }
            "thread-live-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.thread_live(interp.resolve_thread_id(&args[0])?) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "thread-join" => {
                need_args(name, args, 1)?;
                interp.thread_join(interp.resolve_thread_id(&args[0])?, env)
            }
            "thread-name" => {
                need_args(name, args, 1)?;
                Ok(interp
                    .thread_name(interp.resolve_thread_id(&args[0])?)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "thread-signal" => {
                need_args(name, args, 3)?;
                interp.signal_thread(
                    interp.resolve_thread_id(&args[0])?,
                    args[1].clone(),
                    args[2].clone(),
                    env,
                )
            }
            "thread-last-error" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(interp.thread_last_error(args.first().is_some_and(Value::is_truthy)))
            }
            "thread-yield" => {
                need_args(name, args, 0)?;
                interp.drive_threads(env, false)?;
                Ok(Value::Nil)
            }
            "make-mutex" => {
                need_arg_range(name, args, 0, 1)?;
                let mutex_name = args.first().and_then(|value| {
                    if value.is_nil() {
                        None
                    } else {
                        string_like(value).map(|string| string.text)
                    }
                });
                Ok(interp.make_mutex(mutex_name))
            }
            "mutex-name" => {
                need_args(name, args, 1)?;
                Ok(interp
                    .mutex_name(interp.resolve_mutex_id(&args[0])?)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "mutex-lock" => {
                need_args(name, args, 1)?;
                interp.lock_mutex_for_current_thread(interp.resolve_mutex_id(&args[0])?, env)
            }
            "mutex-unlock" => {
                need_args(name, args, 1)?;
                interp.unlock_mutex_for_current_thread(interp.resolve_mutex_id(&args[0])?)
            }
            "make-condition-variable" => {
                need_arg_range(name, args, 1, 2)?;
                let mutex_id = interp.resolve_mutex_id(&args[0])?;
                let condvar_name = args.get(1).and_then(|value| {
                    if value.is_nil() {
                        None
                    } else {
                        string_like(value).map(|string| string.text)
                    }
                });
                Ok(interp.make_condition_variable(mutex_id, condvar_name))
            }
            "condition-mutex" => {
                need_args(name, args, 1)?;
                let condvar_id = interp.resolve_condition_variable_id(&args[0])?;
                let mutex_id = interp
                    .condition_variable_mutex_id(condvar_id)
                    .ok_or_else(|| {
                        LispError::TypeError("condition-variable-p".into(), args[0].type_name())
                    })?;
                Ok(Value::Record(mutex_id))
            }
            "condition-name" => {
                need_args(name, args, 1)?;
                Ok(interp
                    .condition_variable_name(interp.resolve_condition_variable_id(&args[0])?)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "condition-wait" => {
                need_args(name, args, 1)?;
                let condvar_id = interp.resolve_condition_variable_id(&args[0])?;
                interp.wait_condition_variable(condvar_id, env)
            }
            "condition-notify" => {
                need_arg_range(name, args, 1, 2)?;
                interp.notify_condition_variable(
                    interp.resolve_condition_variable_id(&args[0])?,
                    args.get(1).is_some_and(Value::is_truthy),
                )?;
                Ok(Value::Nil)
            }
            "thread--blocker" => {
                need_args(name, args, 1)?;
                Ok(interp.thread_blocker_value(interp.resolve_thread_id(&args[0])?))
            }
            "thread-buffer-disposition" => {
                need_args(name, args, 1)?;
                interp.thread_buffer_disposition(interp.resolve_thread_id(&args[0])?)
            }
            "thread-set-buffer-disposition" => {
                need_args(name, args, 2)?;
                interp.set_thread_buffer_disposition(interp.resolve_thread_id(&args[0])?, &args[1])
            }
            "backtrace--frames-from-thread" => {
                need_args(name, args, 1)?;
                let frames = interp
                    .thread_backtrace_frames_snapshot(interp.resolve_thread_id(&args[0])?)
                    .into_iter()
                    .map(|(evald, function, frame_args, _debug_on_exit)| {
                        let evald = if evald { Value::T } else { Value::Nil };
                        let mut items = vec![evald, function];
                        items.extend(frame_args);
                        Value::list(items)
                    })
                    .collect::<Vec<_>>();
                Ok(Value::list(frames))
            }
            "list-threads" => {
                need_args(name, args, 0)?;
                let buffer_id = interp
                    .find_buffer("*Threads*")
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer("*Threads*").0);
                let mut text = String::from("Thread Name\tStatus\tBlocked On\n");
                for thread in interp.live_threads() {
                    let thread_id = interp.resolve_thread_id(&thread)?;
                    text.push_str(&thread_list_row(interp, thread_id, env)?);
                }
                replace_buffer_contents(interp, buffer_id, &text)?;
                interp.switch_to_buffer_id(buffer_id)?;
                Ok(Value::buffer(buffer_id, interp.buffer.name.clone()))
            }
            "thread-list-send-error-signal" => {
                need_args(name, args, 0)?;
                let thread_id = thread_list_thread_at_point(interp)?;
                interp.signal_thread(thread_id, Value::Symbol("error".into()), Value::Nil, env)
            }
            "thread-list-pop-to-backtrace" => {
                need_args(name, args, 0)?;
                let thread_id = thread_list_thread_at_point(interp)?;
                let thread_name = interp
                    .thread_name(thread_id)
                    .unwrap_or_else(|| format!("#<thread id:{thread_id}>"));
                let buffer_id = interp
                    .find_buffer("*Thread Backtrace*")
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer("*Thread Backtrace*").0);
                let mut text = format!("Backtrace for thread `{thread_name}':\n");
                for (_, function, frame_args, _) in
                    interp.thread_backtrace_frames_snapshot(thread_id)
                {
                    text.push_str(&render_prin1_ephemeral(interp, &function, env)?);
                    for arg in frame_args {
                        text.push(' ');
                        text.push_str(&render_prin1_ephemeral(interp, &arg, env)?);
                    }
                    text.push('\n');
                }
                replace_buffer_contents(interp, buffer_id, &text)?;
                interp.switch_to_buffer_id(buffer_id)?;
                Ok(Value::buffer(buffer_id, interp.buffer.name.clone()))
            }
            "regexp-quote" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    regexp::regexp_quote_elisp(&string_text(&args[0])?).into(),
                ))
            }
            "regexp-opt" => {
                need_arg_range(name, args, 1, 2)?;
                // GNU regexp-opt.el builds a trie-optimized regexp (common
                // prefix/suffix folding); load it and delegate.  The native
                // plain-alternation output is the no-file fallback.
                if !interp.has_lisp_function("regexp-opt")
                    && let Some(path) = interp.resolve_load_target("regexp-opt")
                {
                    let _ = crate::lisp::load_file_strict(interp, &path);
                }
                if let Some(delegated) = delegate_to_lisp_function(interp, name, args, env)? {
                    return Ok(delegated);
                }
                let strings = args[0].to_vec()?;
                let mut patterns = strings
                    .iter()
                    .map(|value| string_text(value).map(|text| regexp::regexp_quote_elisp(&text)))
                    .collect::<Result<Vec<_>, _>>()?;
                if patterns.is_empty() {
                    return Ok(Value::String(String::new().into()));
                }
                patterns.sort();
                patterns.dedup();
                // GNU's PAREN argument controls the group type: nil is shy,
                // t (or any other value) capturing, `words'/`symbols' add the
                // boundary assertions around a capturing group, and a string
                // is used literally as the opening delimiter.
                let paren = args.get(1).cloned().unwrap_or(Value::Nil);
                let (open, close): (String, String) = match &paren {
                    Value::Nil => ("\\(?:".into(), "\\)".into()),
                    Value::Symbol(kind) if kind == "words" => ("\\<\\(".into(), "\\)\\>".into()),
                    Value::Symbol(kind) if kind == "symbols" => {
                        ("\\_<\\(".into(), "\\)\\_>".into())
                    }
                    Value::String(_) | Value::StringObject(_) => {
                        (string_text(&paren)?, "\\)".into())
                    }
                    _ => ("\\(".into(), "\\)".into()),
                };
                Ok(Value::String(
                    format!("{open}{}{close}", patterns.join("\\|")).into(),
                ))
            }
            "regexp-opt-depth" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(
                    regexp_opt_depth(&string_text(&args[0])?) as i64
                ))
            }
            "rx-to-string" => {
                need_arg_range(name, args, 1, 2)?;
                // GNU rx.el owns rx-to-string once loaded (its translator
                // covers every atom); the native compiler is the fallback.
                interp.ensure_gnu_rx_loaded();
                if let Some(delegated) = delegate_to_lisp_function(interp, name, args, env)? {
                    return Ok(delegated);
                }
                let no_group = args.get(1).is_some_and(Value::is_truthy);
                Ok(Value::String(
                    crate::lisp::eval::compile_rx_to_string(interp, &args[0], env, no_group)?
                        .into(),
                ))
            }
            "null-device" => {
                need_args(name, args, 0)?;
                Ok(Value::String("/dev/null".into()))
            }
            "process-file" => {
                need_arg_range(name, args, 1, usize::MAX)?;
                process_file_compat(interp, args, env)
            }
            "read-answer" => {
                need_args(name, args, 2)?;
                let answers = args[1].to_vec()?;
                Ok(answers
                    .first()
                    .and_then(|entry| entry.to_vec().ok())
                    .and_then(|entry| entry.first().cloned())
                    .unwrap_or(Value::String(String::new().into())))
            }
            "temporary-file-directory" => {
                need_args(name, args, 0)?;
                Ok(interp
                    .lookup_var("temporary-file-directory", env)
                    .unwrap_or_else(|| {
                        Value::String(std::env::temp_dir().display().to_string().into())
                    }))
            }
            "convert-standard-filename" => {
                need_args(name, args, 1)?;
                Ok(args[0].clone())
            }
            "abbreviate-file-name" => {
                need_args(name, args, 1)?;
                Ok(args[0].clone())
            }
            "files--name-absolute-system-p" => {
                need_args(name, args, 1)?;
                let path = string_text(&args[0])?;
                Ok(if file_name_absolute_p(&path) && !path.starts_with('~') {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "files--use-insert-directory-program-p" => {
                need_args(name, args, 0)?;
                Ok(
                    if interp
                        .lookup_var("ls-lisp-use-insert-directory-program", env)
                        .is_some_and(|value| value.is_truthy())
                        && interp
                            .lookup_var("insert-directory-program", env)
                            .is_some_and(|value| value.is_truthy())
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "insert-directory-wildcard-in-dir-p" => {
                need_args(name, args, 1)?;
                let directory = string_text(&args[0])?;
                let Some(parent) = file_name_directory(&directory) else {
                    return Ok(Value::Nil);
                };
                if !parent.contains('*') || Path::new(&directory).exists() {
                    return Ok(Value::Nil);
                }
                let base_directory = file_name_as_directory(&dired_base_directory(&directory));
                let wildcard = Path::new(&directory)
                    .strip_prefix(Path::new(&base_directory))
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned())
                    .unwrap_or_else(|| directory.clone());
                Ok(Value::cons(
                    Value::String(base_directory.into()),
                    Value::String(wildcard.into()),
                ))
            }
            "insert-directory-clean" => {
                need_arg_range(name, args, 1, 2)?;
                Ok(Value::Nil)
            }
            #[dispatch(builtin_override)]
            "dired-mark-pop-up" => {
                need_arg_range(name, args, 4, usize::MAX)?;
                call_function_value(interp, &args[3], &args[4..], env)
            }
            "connection-local-value" => {
                need_arg_range(name, args, 1, 2)?;
                Ok(args[0].clone())
            }
            "propertized-buffer-identification" => {
                need_args(name, args, 1)?;
                Ok(Value::list([args[0].clone()]))
            }
            "called-interactively-p" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(if interp.called_interactively_by_backtrace() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "kill-all-local-variables" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = interp.current_buffer_id();
                let kill_permanent = args.first().is_some_and(Value::is_truthy);
                run_named_hooks(interp, "change-major-mode-hook", env, Some(buffer_id))?;
                let locals = interp.buffer_local_variables(buffer_id);
                let mut permanent = Vec::new();
                for (name, value) in &locals {
                    let preserve = !kill_permanent
                        && interp
                            .get_symbol_property(name, "permanent-local")
                            .is_some_and(|value| value.is_truthy());
                    interp.notify_variable_watchers(
                        name,
                        Value::Nil,
                        "makunbound",
                        Some(buffer_id),
                        env,
                    )?;
                    if preserve {
                        permanent.push((name.clone(), value.clone()));
                        continue;
                    }
                    interp.mark_buffer_local_special_binding_killed(buffer_id, name);
                }
                if kill_permanent {
                    interp.clear_buffer_local_state(buffer_id);
                } else {
                    interp.clear_buffer_local_state_for_mode_change(buffer_id);
                }
                for (name, value) in permanent {
                    interp.set_buffer_local_value(buffer_id, &name, value);
                }
                Ok(Value::Nil)
            }
            "hack-local-variables" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "hack-local-variables-filter" => {
                need_args(name, args, 2)?;
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "file-local-variables-alist",
                    args[0].clone(),
                );
                Ok(args[0].clone())
            }
            "hack-local-variables-apply" => {
                need_args(name, args, 0)?;
                let pending = interp
                    .buffer_local_value(interp.current_buffer_id(), "file-local-variables-alist")
                    .or_else(|| interp.lookup_var("file-local-variables-alist", env))
                    .unwrap_or(Value::Nil);
                for entry in pending.to_vec()? {
                    let Some((variable, value)) = entry.cons_values() else {
                        continue;
                    };
                    let symbol = variable.as_symbol()?.to_string();
                    let prepared = interp.prepare_variable_assignment(&symbol, value)?;
                    interp.set_buffer_local_value(interp.current_buffer_id(), &symbol, prepared);
                }
                Ok(Value::Nil)
            }
            "hack-dir-local-variables-non-file-buffer" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "force-mode-line-update" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "oclosure-type" => {
                need_args(name, args, 1)?;
                Ok(oclosure_type_of(&args[0])
                    .map(|value| Value::Symbol(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "emaxx--oclosure-type-p" => {
                need_args(name, args, 2)?;
                let target = args[1].as_symbol()?;
                Ok(if oclosure_value_matches_type(interp, &args[0], target) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "emaxx--oclosure-slot" => {
                need_args(name, args, 2)?;
                let slot = args[1].as_symbol()?;
                let Value::Lambda(lambda) = &args[0] else {
                    return Err(wrong_type_argument("oclosure", args[0].clone()));
                };
                let env_contents = lambda.env.borrow();
                for frame in env_contents.iter().rev() {
                    if frame
                        .iter()
                        .any(|(key, _)| key == crate::lisp::eval::OCLOSURE_TYPE_MARKER)
                    {
                        return Ok(frame
                            .iter()
                            .find(|(key, _)| key == slot)
                            .map(|(_, value)| value.clone())
                            .unwrap_or(Value::Nil));
                    }
                }
                Err(wrong_type_argument("oclosure", args[0].clone()))
            }
            "emaxx--oclosure-set-slot" => {
                need_args(name, args, 3)?;
                let slot = args[1].as_symbol()?.to_string();
                let Some(type_name) = oclosure_type_of(&args[0]) else {
                    return Err(wrong_type_argument("oclosure", args[0].clone()));
                };
                // Only slots declared (SLOT :mutable t) may be written; GNU
                // signals setting-constant for the rest (oclosure--set).
                let mut current: Option<String> = Some(type_name);
                let mut mutable = false;
                while let Some(type_name) = current {
                    if interp
                        .get_symbol_property(&type_name, "emaxx-oclosure-mutable-slots")
                        .and_then(|value| value.to_vec().ok())
                        .unwrap_or_default()
                        .iter()
                        .any(|value| matches!(value, Value::Symbol(name) if name == &slot))
                    {
                        mutable = true;
                        break;
                    }
                    current = interp
                        .get_symbol_property(&type_name, "emaxx-oclosure-parent")
                        .and_then(|value| value.as_symbol().ok().map(String::from));
                }
                if !mutable {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("setting-constant".into()),
                        Value::Symbol(slot.into()),
                    ])));
                }
                let Value::Lambda(lambda) = &args[0] else {
                    return Err(wrong_type_argument("oclosure", args[0].clone()));
                };
                let mut env_contents = lambda.env.borrow_mut();
                for frame in env_contents.iter_mut().rev() {
                    if !frame
                        .iter()
                        .any(|(key, _)| key == crate::lisp::eval::OCLOSURE_TYPE_MARKER)
                    {
                        continue;
                    }
                    if let Some(entry) = frame.iter_mut().find(|(key, _)| key == &slot) {
                        entry.1 = args[2].clone();
                        return Ok(args[2].clone());
                    }
                }
                Err(wrong_type_argument("oclosure", args[0].clone()))
            }
            "emaxx--oclosure-copy" => {
                need_args(name, args, 2)?;
                let Value::Lambda(lambda) = &args[0] else {
                    return Err(wrong_type_argument("oclosure", args[0].clone()));
                };
                let replacements = args[1].to_vec()?;
                let mut contents = lambda.env.borrow().clone();
                for frame in contents.iter_mut().rev() {
                    if !frame
                        .iter()
                        .any(|(key, _)| key == crate::lisp::eval::OCLOSURE_TYPE_MARKER)
                    {
                        continue;
                    }
                    for replacement in &replacements {
                        let Some((key, value)) = replacement.cons_values() else {
                            continue;
                        };
                        let Ok(slot) = key.as_symbol() else { continue };
                        if let Some(entry) = frame.iter_mut().find(|(name, _)| name == slot) {
                            entry.1 = value.clone();
                        }
                    }
                    crate::lisp::eval::Interpreter::restamp_frame_identity(frame);
                    break;
                }
                Ok(Value::lambda(
                    lambda.params.clone(),
                    lambda.body.clone(),
                    crate::lisp::types::shared_env(contents),
                ))
            }
            "garbage-collect" => {
                need_args(name, args, 0)?;
                collect_weak_hash_tables(interp)?;
                // GNU returns ((TYPE SIZE USED FREE) ...); the SIZE column is
                // the 64-bit object layout constant (memory-report.el computes
                // object sizes from it).  Counts are approximations.
                let entry = |name: &str, rest: &[i64]| {
                    Value::list(
                        std::iter::once(Value::Symbol(name.into()))
                            .chain(rest.iter().map(|n| Value::Integer(*n))),
                    )
                };
                Ok(Value::list([
                    entry("conses", &[16, 0, 0]),
                    entry("symbols", &[48, 0, 0]),
                    entry("strings", &[32, 0, 0]),
                    entry("string-bytes", &[1, 0]),
                    entry("vectors", &[16, 0]),
                    entry("vector-slots", &[8, 0, 0]),
                    entry("floats", &[8, 0, 0]),
                    entry("intervals", &[56, 0, 0]),
                    entry("buffers", &[984, 0]),
                ]))
            }
            "garbage-collect-maybe" => {
                need_args(name, args, 1)?;
                if !matches!(args[0], Value::Integer(value) if value >= 0) {
                    return Err(LispError::TypeError(
                        "wholenump".into(),
                        args[0].type_name(),
                    ));
                }
                // Emaxx uses Rust ownership rather than GNU's byte-allocation GC
                // threshold, so no pending automatic collection can be due.
                Ok(Value::Nil)
            }
            "memory-use-counts" => {
                need_args(name, args, 0)?;
                // GNU's seven counters are incremented at type-specific C arena
                // allocation sites and survive GC.  Rust ownership has neither
                // those arenas nor equivalent category accounting; allocator
                // byte totals or live-object scans would not implement this API.
                Err(LispError::Signal(
                    "GNU allocation counters are unavailable in the Rust ownership backend".into(),
                ))
            }
            "num-processors" => {
                need_args(name, args, 0)?;
                let count = std::thread::available_parallelism()
                    .map(|count| count.get() as i64)
                    .unwrap_or(1);
                Ok(Value::Integer(count.max(1)))
            }
            "current-cpu-time" => {
                need_args(name, args, 0)?;
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                Ok(Value::list([normalize_bigint_value(BigInt::from(nanos))]))
            }
            "emacs-pid" => {
                need_args(name, args, 0)?;
                Ok(Value::Integer(emacs_pid_value()))
            }
            "type-of" => {
                need_args(name, args, 1)?;
                if let Some(type_name) = legacy_struct_vector_type(interp, &args[0], env) {
                    return Ok(Value::Symbol(type_name.into()));
                }
                let name = match &args[0] {
                    Value::Nil => "symbol",
                    Value::T => "symbol",
                    Value::Integer(_) => "integer",
                    Value::BigInteger(_) => "integer",
                    Value::Float(_) => "float",
                    Value::String(_) => "string",
                    Value::StringObject(_) => "string",
                    Value::Symbol(_) => "symbol",
                    Value::Cons(_) if is_vector_value(&args[0]) => "vector",
                    Value::Cons(_) => "cons",
                    Value::BuiltinFunc(_) => "subr",
                    Value::Lambda(_) => "cons", // Emacs closures are cons cells
                    Value::Buffer(_) => "buffer",
                    Value::Marker(_) => "marker",
                    Value::Overlay(_) => "overlay",
                    Value::CharTable(_) => "char-table",
                    Value::Frame(_) => "frame",
                    Value::Terminal(_) => "terminal",
                    Value::Record(id) => {
                        let record = interp.find_record(*id).ok_or_else(|| {
                            LispError::TypeError("record".into(), format!("record<{id}>"))
                        })?;
                        return Ok(Value::Symbol(record.type_name.clone().into()));
                    }
                    Value::Finalizer(_) => "finalizer",
                    Value::Unbound => "unbound",
                };
                Ok(Value::Symbol(name.into()))
            }
            #[dispatch(builtin_override)]
            "cl-type-of" => {
                need_args(name, args, 1)?;
                Ok(Value::Symbol(cl_type_name(interp, &args[0])?.into()))
            }
            // GNU cl-macs.el defines `cl--find-class' as `(get TYPE 'cl--class)',
            // which projects to the same class storage as `cl-find-class' here.
            #[dispatch(builtin_override)]
            "cl-find-class" => cl_find_class_value(interp, name, args),
            "cl--find-class" => cl_find_class_value(interp, name, args),
            #[dispatch(builtin_override)]
            "cl--struct-get-class" => {
                need_args(name, args, 1)?;
                let symbol = args[0].as_symbol()?;
                Ok(interp.class_value(symbol).unwrap_or(Value::Nil))
            }
            "cl--struct-class-slots" => {
                need_args(name, args, 1)?;
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(wrong_type_argument("cl--struct-class-p", args[0].clone()));
                };
                let descriptors = if let Some(raw) = interp
                    .get_symbol_property(&class_name, "emaxx-struct-slot-descs")
                    .and_then(|value| value.to_vec().ok())
                {
                    raw.into_iter()
                        .skip(1) // GNU's class slot vector omits `cl-tag-slot'.
                        .filter_map(|slot| {
                            let parts = slot.to_vec().ok()?;
                            let slot_name = parts.first()?.as_symbol().ok()?.to_string();
                            let mut descriptor = EieioSlotDescriptor {
                                name: slot_name,
                                initform: parts.get(1).cloned(),
                                slot_type: Value::T,
                                props: Vec::new(),
                                initargs: Vec::new(),
                                class_allocated: false,
                            };
                            let mut index = 2;
                            while index + 1 < parts.len() {
                                let key = parts[index].as_symbol().ok()?.to_string();
                                let value = parts[index + 1].clone();
                                if key == ":type" {
                                    descriptor.slot_type = value;
                                } else {
                                    descriptor.props.push((key, value));
                                }
                                index += 2;
                            }
                            Some(descriptor)
                        })
                        .collect::<Vec<_>>()
                } else {
                    eieio_slot_descriptors(interp, &class_name)?
                };
                let records = descriptors
                    .iter()
                    .map(|descriptor| eieio_slot_descriptor_record(interp, env, descriptor))
                    .collect::<Vec<_>>();
                Ok(Value::list(
                    std::iter::once(Value::Symbol("vector-literal".into())).chain(records),
                ))
            }
            "cl--struct-class-type" => {
                need_args(name, args, 1)?;
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(wrong_type_argument("cl--struct-class-p", args[0].clone()));
                };
                Ok(interp
                    .get_symbol_property(&class_name, "emaxx-struct-sequence-type")
                    .unwrap_or(Value::Nil))
            }
            "cl--class-index-table" => {
                need_args(name, args, 1)?;
                if let Some(table) = interp.raw_eieio_class_slot(&args[0], 4) {
                    return Ok(table);
                }
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(wrong_type_argument("class-p", args[0].clone()));
                };
                let sequence_type = interp
                    .get_symbol_property(&class_name, "emaxx-struct-sequence-type")
                    .unwrap_or(Value::Nil);
                let offset = usize::from(sequence_type.is_nil());
                let slots = interp
                    .get_symbol_property(&class_name, "emaxx-struct-slots")
                    .or_else(|| interp.get_symbol_property(&class_name, "emaxx-class-slots"))
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                let table = super::call(interp, "make-hash-table", &[], env)?;
                for (index, slot) in slots.into_iter().enumerate() {
                    let slot_name = match slot {
                        Value::Symbol(name) => Value::Symbol(name),
                        Value::Cons(_) => slot.car().unwrap_or(Value::Nil),
                        _ => continue,
                    };
                    super::call(
                        interp,
                        "puthash",
                        &[
                            slot_name,
                            Value::Integer((index + offset) as i64),
                            table.clone(),
                        ],
                        env,
                    )?;
                }
                Ok(table)
            }
            #[dispatch(builtin_override)]
            "cl-struct-define" => {
                need_args(name, args, 9)?;
                let struct_name = args[0].as_symbol()?.to_string();
                if is_builtin_class_name(&struct_name) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("cl--struct-name-p".into()),
                        Value::Symbol(struct_name.into()),
                        Value::Symbol("name".into()),
                    ])));
                }
                let type_arg = args[3].clone();
                let children_symbol = args[6].as_symbol()?.to_string();
                let tag = args[7].clone();
                if type_arg.is_nil() {
                    interp.set_variable("cl-old-struct-compat-mode", Value::T, env);
                }
                let mut children = interp
                    .lookup_var(&children_symbol, env)
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                if !children.iter().any(|child| child == &tag) {
                    children.insert(0, tag.clone());
                }
                interp.set_variable(&children_symbol, Value::list(children), env);
                if !matches!(tag, Value::Symbol(ref tag_name) if tag_name == &struct_name)
                    && let Value::Symbol(tag_name) = &tag
                {
                    let class_value = interp
                        .class_value(&struct_name)
                        .unwrap_or_else(|| Value::Symbol(struct_name.clone().into()));
                    interp.set_variable(tag_name, class_value, env);
                    interp.set_function_binding(
                        tag_name,
                        Some(Value::Symbol(":quick-object-witness-check".into())),
                    );
                }
                Ok(Value::Symbol(struct_name.into()))
            }
            #[dispatch(builtin_override)]
            "cl-old-struct-compat-mode" => {
                need_args(name, args, 1)?;
                let enabled = !args[0].is_nil()
                    && !matches!(&args[0], Value::Integer(n) if *n <= 0)
                    && !matches!(&args[0], Value::BigInteger(n) if **n <= BigInt::from(0));
                let value = if enabled { Value::T } else { Value::Nil };
                interp.set_variable("cl-old-struct-compat-mode", value.clone(), env);
                Ok(value)
            }
            "cl--class-name" => {
                need_args(name, args, 1)?;
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                Ok(crate::lisp::types::interned_symbol_value(class_name))
            }
            #[dispatch(builtin_override)]
            "cl--class-parents" => {
                need_args(name, args, 1)?;
                interp.class_parents_value(&args[0])
            }
            #[dispatch(builtin_override)]
            "cl--class-allparents" => {
                need_args(name, args, 1)?;
                let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                Ok(
                    if interp.class_value(&symbol).is_some() || is_builtin_class_name(&symbol) {
                        Value::list(interp.class_allparents(&symbol))
                    } else if symbol == "t" {
                        Value::list([Value::T])
                    } else {
                        Value::list([crate::lisp::types::interned_symbol_value(symbol), Value::T])
                    },
                )
            }
            #[dispatch(builtin_override)]
            "cl--class-children" | "eieio-class-children" => {
                need_args(name, args, 1)?;
                if let Some(children) = interp.raw_eieio_class_slot(&args[0], 5) {
                    return Ok(children);
                }
                let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                Ok(Value::list(interp.class_children(&symbol)))
            }
            #[dispatch(builtin_override)]
            "class-abstract-p" => {
                need_args(name, args, 1)?;
                let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                let options = interp
                    .class_value(&symbol)
                    .and_then(|class| interp.raw_eieio_class_slot(&class, 10))
                    .or_else(|| interp.get_symbol_property(&symbol, "emaxx-class-options"));
                let abstractp = options
                    .and_then(|options| options.to_vec().ok())
                    .is_some_and(|options| {
                        options.windows(2).any(|pair| {
                            matches!(&pair[0], Value::Symbol(option) if option == ":abstract")
                                && pair[1].is_truthy()
                        })
                    });
                Ok(if abstractp { Value::T } else { Value::Nil })
            }
            #[dispatch(builtin_override)]
            "same-class-p" => {
                need_args(name, args, 2)?;
                let Value::Record(id) = &args[0] else {
                    return Err(LispError::TypeError(
                        "eieio-object".into(),
                        args[0].type_name(),
                    ));
                };
                let Some(record) = interp.find_record(*id) else {
                    return Err(LispError::TypeError(
                        "eieio-object".into(),
                        args[0].type_name(),
                    ));
                };
                let Some(class_name) = interp.class_name_from_value(&args[1]) else {
                    return Err(LispError::TypeError("class".into(), args[1].type_name()));
                };
                Ok(if record.type_name == class_name {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            #[dispatch(builtin_override)]
            "eieio-oref-default" => {
                need_args(name, args, 2)?;
                // GNU accepts a class symbol, a class object, or an instance.
                let Some(class_name) =
                    interp
                        .class_name_from_value(&args[0])
                        .or_else(|| match &args[0] {
                            Value::Record(id) => interp
                                .find_record(*id)
                                .map(|record| record.type_name.clone()),
                            _ => None,
                        })
                else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                let slot_name = args[1].as_symbol()?;
                let slots = eieio_slot_specs(interp, &class_name)?;
                if let Some(slot_index) = eieio_slot_index(&slots, slot_name) {
                    if slots[slot_index].class_allocated {
                        let value = eieio_class_allocated_value(
                            interp,
                            &class_name,
                            &slots[slot_index],
                            env,
                        )?;
                        // GNU's class-allocated `oref-default' returns the raw
                        // storage without checking boundness.
                        if interp.value_is_eieio_unbound(&value) {
                            return Ok(interp
                                .lookup_var("eieio--unbound", env)
                                .unwrap_or(Value::Unbound));
                        }
                        return Ok(value);
                    }
                    if let Some(initform) = &slots[slot_index].initform {
                        return interp.eval(initform, env);
                    }
                    if let Some(value) = interp
                        .get_symbol_property(&class_name, &eieio_class_default_property(slot_name))
                    {
                        if interp.value_is_eieio_unbound(&value) {
                            return Ok(interp
                                .lookup_var("eieio--unbound", env)
                                .unwrap_or(Value::Unbound));
                        }
                        return Ok(value);
                    }
                    // While GNU builds a class's default-object cache it binds
                    // eieio-skip-typecheck and expects an absent initform to
                    // remain the unbound marker.  Returning nil here silently
                    // turns every unspecified slot into a bound-nil slot.
                    if interp
                        .lookup_var("eieio-skip-typecheck", env)
                        .is_some_and(|setting| setting.is_truthy())
                    {
                        return Ok(interp
                            .lookup_var("eieio--unbound", env)
                            .unwrap_or(Value::Unbound));
                    }
                    return eieio_slot_unbound_dispatch(
                        interp,
                        env,
                        &args[0],
                        &class_name,
                        slot_name,
                        "oref-default",
                    );
                }
                if let Some(value) = interp
                    .get_symbol_property(&class_name, &eieio_class_default_property(slot_name))
                {
                    if interp.value_is_eieio_unbound(&value) {
                        return Ok(interp
                            .lookup_var("eieio--unbound", env)
                            .unwrap_or(Value::Unbound));
                    }
                    return Ok(value);
                }
                Ok(Value::Nil)
            }
            #[dispatch(builtin_override)]
            "eieio-oset-default" => {
                need_args(name, args, 3)?;
                // GNU accepts a class symbol, a class object, or an instance.
                let Some(class_name) =
                    interp
                        .class_name_from_value(&args[0])
                        .or_else(|| match &args[0] {
                            Value::Record(id) => interp
                                .find_record(*id)
                                .map(|record| record.type_name.clone()),
                            _ => None,
                        })
                else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                let slot_name = args[1].as_symbol()?.to_string();
                let slots = eieio_slot_specs(interp, &class_name)?;
                let Some(slot_index) = eieio_slot_index(&slots, &slot_name) else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("invalid-slot-name".into()),
                        Value::Symbol(class_name.into()),
                        Value::Symbol(slot_name.into()),
                    ])));
                };
                let skip_typecheck = interp
                    .lookup_var("eieio-skip-typecheck", env)
                    .is_some_and(|setting| setting.is_truthy());
                if !skip_typecheck
                    && !eieio_value_matches_type(
                        interp,
                        &args[2],
                        &slots[slot_index].slot_type,
                        env,
                    )?
                {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("invalid-slot-type".into()),
                        Value::Symbol(class_name.into()),
                        Value::Symbol(slots[slot_index].name.clone().into()),
                        slots[slot_index].slot_type.clone(),
                        args[2].clone(),
                    ])));
                }
                if slots[slot_index].class_allocated {
                    if set_eieio_class_allocated_value(
                        interp,
                        &class_name,
                        &slots[slot_index].name,
                        args[2].clone(),
                    )? {
                        return Ok(args[2].clone());
                    }
                    interp.put_symbol_property(
                        &class_name,
                        &eieio_class_allocation_property(&slots[slot_index].name),
                        args[2].clone(),
                    );
                } else {
                    set_eieio_instance_default(
                        interp,
                        &class_name,
                        &slots[slot_index].name,
                        slot_index,
                        args[2].clone(),
                    )?;
                }
                Ok(args[2].clone())
            }
            #[dispatch(builtin_override)]
            "eieio--object-class" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    value if !interp.value_is_eieio_object(value) => Err(LispError::TypeError(
                        "eieio-object".into(),
                        value.type_name(),
                    )),
                    Value::Record(id) => {
                        let class_name = interp
                            .find_record(*id)
                            .map(|record| record.type_name.clone())
                            .ok_or_else(|| {
                                LispError::TypeError("eieio-object".into(), args[0].type_name())
                            })?;
                        // GNU's Lisp helper resolves the object's record tag to
                        // the live class object.  Returning only the tag symbol
                        // makes every subsequent eieio--class accessor reject
                        // an otherwise valid instance.
                        Ok(interp
                            .class_value(&class_name)
                            .unwrap_or(Value::Symbol(class_name.into())))
                    }
                    _ => Err(LispError::TypeError(
                        "eieio-object".into(),
                        args[0].type_name(),
                    )),
                }
            }
            #[dispatch(builtin_override)]
            "eieio--class-children" => {
                need_args(name, args, 1)?;
                if let Some(children) = interp.raw_eieio_class_slot(&args[0], 5) {
                    return Ok(children);
                }
                let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                // GNU stores child class NAMES in the class record
                // (`eieio-defclass-internal' pushes the symbol).
                Ok(Value::list(interp.class_children(&symbol)))
            }
            #[dispatch(builtin_override)]
            "eieio--class-name" => {
                need_args(name, args, 1)?;
                interp
                    .class_name_from_value(&args[0])
                    .map(|value| Value::Symbol(value.into()))
                    .ok_or_else(|| LispError::TypeError("class".into(), args[0].type_name()))
            }
            #[dispatch(builtin_override)]
            "eieio-object-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.value_is_eieio_object(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            #[dispatch(builtin_override)]
            "slot-boundp" => {
                need_args(name, args, 2)?;
                let slot_name = args[1].as_symbol()?;
                eieio_slot_boundp(interp, &args[0], slot_name)
            }
            #[dispatch(builtin_override)]
            "eieio--class-slots" | "eieio--class-class-slots" => {
                need_args(name, args, 1)?;
                let raw_index = if name == "eieio--class-slots" { 3 } else { 7 };
                if let Some(slots) = interp.raw_eieio_class_slot(&args[0], raw_index) {
                    return Ok(slots);
                }
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                let want_class_allocated = name == "eieio--class-class-slots";
                let descriptors = eieio_slot_descriptors(interp, &class_name)?;
                let mut items = vec![Value::Symbol("vector-literal".into())];
                for descriptor in &descriptors {
                    if descriptor.class_allocated == want_class_allocated {
                        items.push(eieio_slot_descriptor_record(interp, env, descriptor));
                    }
                }
                Ok(Value::list(items))
            }
            #[dispatch(builtin_override)]
            "eieio--class-initarg-tuples" => {
                need_args(name, args, 1)?;
                if let Some(tuples) = interp.raw_eieio_class_slot(&args[0], 6) {
                    return Ok(tuples);
                }
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                let descriptors = eieio_slot_descriptors(interp, &class_name)?;
                let mut tuples: Vec<Value> = Vec::new();
                for descriptor in &descriptors {
                    for initarg in &descriptor.initargs {
                        tuples.push(Value::cons(
                            Value::Symbol(initarg.clone().into()),
                            Value::Symbol(descriptor.name.clone().into()),
                        ));
                    }
                }
                Ok(Value::list(tuples))
            }
            #[dispatch(builtin_override)]
            "cl--slot-descriptor-name"
            | "cl--slot-descriptor-initform"
            | "cl--slot-descriptor-type"
            | "cl--slot-descriptor-props" => {
                need_args(name, args, 1)?;
                let slot_index = match name {
                    "cl--slot-descriptor-name" => 0,
                    "cl--slot-descriptor-initform" => 1,
                    "cl--slot-descriptor-type" => 2,
                    _ => 3,
                };
                match &args[0] {
                    Value::Record(id)
                        if interp
                            .find_record(*id)
                            .is_some_and(|record| record.type_name == "cl-slot-descriptor") =>
                    {
                        Ok(interp
                            .find_record(*id)
                            .and_then(|record| record.slots.get(slot_index).cloned())
                            .unwrap_or(Value::Nil))
                    }
                    _ => Err(LispError::TypeError(
                        "cl-slot-descriptor".into(),
                        args[0].type_name(),
                    )),
                }
            }
            "cl--make-slot-desc" | "cl--make-slot-descriptor" => {
                need_arg_range(name, args, 1, 4)?;
                Ok(interp.create_record(
                    "cl-slot-descriptor",
                    vec![
                        args[0].clone(),
                        args.get(1).cloned().unwrap_or(Value::Nil),
                        args.get(2).cloned().unwrap_or(Value::Nil),
                        args.get(3).cloned().unwrap_or(Value::Nil),
                    ],
                ))
            }
            "cl--copy-slot-descriptor-1" | "cl--copy-slot-descriptor" => {
                need_args(name, args, 1)?;
                let Value::Record(record_id) = &args[0] else {
                    return Err(LispError::TypeError(
                        "cl-slot-descriptor".into(),
                        args[0].type_name(),
                    ));
                };
                let mut slots = interp
                    .find_record(*record_id)
                    .filter(|record| record.type_name == "cl-slot-descriptor")
                    .map(|record| record.slots.clone())
                    .ok_or_else(|| {
                        LispError::TypeError("cl-slot-descriptor".into(), args[0].type_name())
                    })?;
                if name == "cl--copy-slot-descriptor"
                    && let Some(props) = slots.get(3).cloned()
                {
                    slots[3] = super::call(interp, "copy-alist", &[props], env)?;
                }
                Ok(interp.create_record("cl-slot-descriptor", slots))
            }
            "cl-slot-descriptor-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(
                        &args[0],
                        Value::Record(record_id)
                            if interp.find_record(*record_id).is_some_and(|record| record.type_name == "cl-slot-descriptor")
                    ) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "make-instance" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                make_eieio_instance(interp, &class_name, &args[1..], true, env)
            }
            "clone" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                clone_eieio_instance(interp, &args[0], &args[1..])
            }
            #[dispatch(builtin_override)]
            "semanticdb-find-tags-by-class" => {
                need_arg_range(name, args, 1, 3)?;
                semanticdb_find_tags_by_class(interp, args, env)
            }
            #[dispatch(builtin_override)]
            "semanticdb-find-tags-by-name" => {
                need_arg_range(name, args, 1, 3)?;
                semanticdb_find_tags_by_name(interp, args, env)
            }
            #[dispatch(builtin_override)]
            "semanticdb-find-tags-for-completion" => {
                need_arg_range(name, args, 1, 3)?;
                semanticdb_find_tags_for_completion(interp, args, env)
            }
            #[dispatch(builtin_override)]
            "semantic-fetch-tags" => {
                need_arg_range(name, args, 0, 1)?;
                semantic_fetch_tags_compat(interp, env)
            }
            #[dispatch(builtin_override)]
            "semantic-current-tag" => {
                need_arg_range(name, args, 0, 1)?;
                let result = semantic_current_tag_compat(interp, env);
                if std::env::var_os("EMAXX_DEBUG_SEMANTIC").is_some()
                    && let Ok(tag) = &result
                {
                    eprintln!(
                        "[sem] current-tag buf={} point={} -> {}",
                        interp.buffer.name,
                        interp.buffer.point(),
                        tag
                    );
                }
                result
            }
            #[dispatch(builtin_override)]
            "semantic-current-tag-of-class" => {
                need_args(name, args, 1)?;
                let target_class = args[0].as_symbol()?.to_string();
                semantic_current_tag_of_class_compat(interp, env, &target_class)
            }
            #[dispatch(builtin_override)]
            "semantic-find-tag-by-overlay-prev" | "semantic-find-tag-by-overlay-next" => {
                need_arg_range(name, args, 0, 2)?;
                let start = match args.first() {
                    Some(value) if value.is_truthy() => value.as_integer()?,
                    _ => interp.buffer.point() as i64,
                };
                let tags = semantic_fetch_tags_compat(interp, env)?
                    .to_vec()
                    .unwrap_or_default();
                let mut flat = Vec::new();
                semantic_flatten_tags(&tags, &mut flat);
                let forward = name.ends_with("next");
                let mut best: Option<(i64, Value)> = None;
                for tag in flat {
                    let Some((tag_start, tag_end)) = semantic_tag_bounds(&tag) else {
                        continue;
                    };
                    if forward {
                        if tag_start > start
                            && best.as_ref().is_none_or(|(pos, _)| tag_start < *pos)
                        {
                            best = Some((tag_start, tag));
                        }
                    } else if tag_end <= start
                        && best.as_ref().is_none_or(|(pos, _)| tag_end >= *pos)
                    {
                        best = Some((tag_end, tag));
                    }
                }
                Ok(best.map(|(_, tag)| tag).unwrap_or(Value::Nil))
            }
            #[dispatch(builtin_override)]
            "semantic-ctxt-current-symbol" => {
                need_args(name, args, 0)?;
                Ok(semantic_ctxt_current_symbol(interp)
                    .map(|symbol| symbol.parts_value)
                    .unwrap_or(Value::Nil))
            }
            #[dispatch(builtin_override)]
            "semantic-ctxt-current-symbol-and-bounds" => {
                need_args(name, args, 0)?;
                Ok(if let Some(symbol) = semantic_ctxt_current_symbol(interp) {
                    Value::list([
                        symbol.parts_value,
                        Value::String(symbol.text.into()),
                        Value::cons(
                            Value::Integer(symbol.start as i64),
                            Value::Integer(symbol.end as i64),
                        ),
                    ])
                } else {
                    Value::list([Value::Nil, Value::Nil, Value::Nil])
                })
            }
            "bounds-of-thing-at-point" => {
                need_args(name, args, 1)?;
                let thing = args[0].as_symbol()?;
                Ok(bounds_of_thing_at_point(interp, thing).unwrap_or(Value::Nil))
            }
            #[dispatch(builtin_override)]
            "semantic-analyze-possible-completions" => {
                need_args(name, args, 1)?;
                semantic_analyze_possible_completions(interp, env)
            }
            #[dispatch(builtin_override)]
            "semantic-analyze-tag-references" => {
                need_args(name, args, 1)?;
                semantic_analyze_tag_references(interp, &args[0], env)
            }
            #[dispatch(builtin_override)]
            "semantic-analyze-refs-impl" => {
                need_arg_range(name, args, 1, 2)?;
                semantic_analyze_refs_part(&args[0], 1)
            }
            #[dispatch(builtin_override)]
            "semantic-analyze-refs-proto" => {
                need_arg_range(name, args, 1, 2)?;
                semantic_analyze_refs_part(&args[0], 2)
            }
            #[dispatch(builtin_override)]
            "semantic-symref-find-references-by-name" => {
                need_arg_range(name, args, 1, 3)?;
                semantic_symref_find_references_by_name(interp, &args[0])
            }
            #[dispatch(builtin_override)]
            "semantic-symref-result-get-files" => {
                need_args(name, args, 1)?;
                semantic_symref_result_part(&args[0], 2)
            }
            #[dispatch(builtin_override)]
            "semantic-symref-result-get-tags" => {
                need_arg_range(name, args, 1, 2)?;
                semantic_symref_result_part(&args[0], 3)
            }
            #[dispatch(builtin_override)]
            "semantic-symref-hits-in-region" => {
                need_args(name, args, 4)?;
                semantic_symref_hits_in_region(interp, args, env)
            }
            #[dispatch(builtin_override)]
            "semantic-symref-test-count-hits-in-tag" => {
                need_args(name, args, 0)?;
                semantic_symref_test_count_hits_in_tag(interp)
            }
            #[dispatch(builtin_override)]
            "semantic-equivalent-tag-p" => {
                need_args(name, args, 2)?;
                let matches = semantic_tags_equivalent(&args[0], &args[1]);
                if std::env::var_os("EMAXX_DEBUG_SEMANTIC").is_some() {
                    eprintln!("[sem] equiv {} vs {} -> {}", args[0], args[1], matches);
                }
                Ok(if matches { Value::T } else { Value::Nil })
            }
            #[dispatch(builtin_override)]
            "semantic-go-to-tag" => {
                need_arg_range(name, args, 1, 2)?;
                semantic_go_to_tag(interp, &args[0], env)
            }
            #[dispatch(builtin_override)]
            "semantic-clear-toplevel-cache" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            #[dispatch(builtin_override)]
            "semanticdb-typecache-find" => {
                need_arg_range(name, args, 1, 3)?;
                semanticdb_typecache_find(interp, args, env)
            }
            #[dispatch(builtin_override)]
            "semanticdb-typecache-add-dependant" => {
                need_args(name, args, 1)?;
                Ok(Value::Nil)
            }
            #[dispatch(builtin_override)]
            "srecode-template-get-table" => {
                need_arg_range(name, args, 2, 4)?;
                srecode_template_get_table(interp, args, env)
            }
            "emaxx-class-make" => {
                need_args(name, args, 2)?;
                let class_name = args[0].as_symbol()?;
                let initargs = args[1].to_vec()?;
                make_eieio_instance(interp, class_name, &initargs, true, env)
            }
            #[dispatch(builtin_override)]
            "eieio-oref" | "slot-value" => {
                need_args(name, args, 2)?;
                let slot_name = args[1].as_symbol()?.to_string();
                // GNU's eieio-oref also reads OClosure slots.
                if oclosure_type_of(&args[0]).is_some() {
                    return super::call(
                        interp,
                        "emaxx--oclosure-slot",
                        &[args[0].clone(), Value::Symbol(slot_name.into())],
                        env,
                    );
                }
                eieio_oref_dispatch(interp, env, &args[0], &slot_name)
            }
            #[dispatch(builtin_override)]
            "eieio-oset" => {
                need_args(name, args, 3)?;
                let slot_name = args[1].as_symbol()?.to_string();
                // GNU's eieio-oset writes OClosure slots (setting-constant for
                // slots not declared :mutable).
                if oclosure_type_of(&args[0]).is_some() {
                    return super::call(
                        interp,
                        "emaxx--oclosure-set-slot",
                        &[
                            args[0].clone(),
                            Value::Symbol(slot_name.into()),
                            args[2].clone(),
                        ],
                        env,
                    );
                }
                eieio_oset_dispatch(interp, env, &args[0], &slot_name, args[2].clone())
            }
            #[dispatch(builtin_override)]
            "slot-makeunbound" => {
                need_args(name, args, 2)?;
                let slot_name = args[1].as_symbol()?.to_string();
                eieio_slot_makeunbound(interp, &args[0], &slot_name)
            }
            #[dispatch(builtin_override)]
            "slot-exists-p" => {
                need_args(name, args, 2)?;
                let slot_name = args[1].as_symbol()?;
                let Some(class_name) = (match &args[0] {
                    Value::Record(id) => interp
                        .find_record(*id)
                        .map(|record| record.type_name.clone()),
                    other => interp.class_name_from_value(other),
                }) else {
                    return Err(LispError::TypeError(
                        "eieio-object".into(),
                        args[0].type_name(),
                    ));
                };
                let slots = eieio_slot_specs(interp, &class_name)?;
                Ok(if slots.iter().any(|slot| slot.name == slot_name) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            #[dispatch(builtin_override)]
            "map-elt" | "map-contains-key" => {
                // GNU map.el dispatches on the map's shape: a list is a plist
                // when its first element is an atom, otherwise an alist;
                // hash tables and arrays are supported directly.
                need_arg_range(name, args, 2, 4)?;
                let map = &args[0];
                let key = &args[1];
                let contains = name == "map-contains-key";
                let (default, testfn) = if contains {
                    (Value::Nil, args.get(2).cloned())
                } else {
                    (
                        args.get(2).cloned().unwrap_or(Value::Nil),
                        args.get(3).cloned(),
                    )
                };
                let found: Option<Value> = if json::is_hash_table(interp, map) {
                    let sentinel = Value::Unbound;
                    let result = super::call(
                        interp,
                        "gethash",
                        &[key.clone(), map.clone(), sentinel.clone()],
                        env,
                    )?;
                    if matches!(result, Value::Unbound) {
                        None
                    } else {
                        Some(result)
                    }
                } else if is_vector_value(map) || map.is_string() {
                    let length = super::call(interp, "length", std::slice::from_ref(map), env)?
                        .as_integer()
                        .unwrap_or(0);
                    match key.as_integer() {
                        Ok(index) if index >= 0 && index < length => Some(super::call(
                            interp,
                            "elt",
                            &[map.clone(), key.clone()],
                            env,
                        )?),
                        _ => None,
                    }
                } else if map.is_nil() {
                    None
                } else if map
                    .cons_values()
                    .is_some_and(|(car, _)| !matches!(car, Value::Cons(..)))
                {
                    // Plist.
                    let mut member_args = vec![map.clone(), key.clone()];
                    if let Some(testfn) = &testfn {
                        member_args.push(testfn.clone());
                    }
                    let member = super::call(interp, "plist-member", &member_args, env)?;
                    if member.is_nil() {
                        None
                    } else {
                        Some(member.cdr()?.car().unwrap_or(Value::Nil))
                    }
                } else {
                    // Alist; GNU compares with `equal' by default.
                    let testfn = testfn.unwrap_or(Value::Symbol("equal".into()));
                    let entry =
                        super::call(interp, "assoc", &[key.clone(), map.clone(), testfn], env)?;
                    if entry.is_nil() {
                        None
                    } else {
                        Some(entry.cdr()?)
                    }
                };
                Ok(if contains {
                    if found.is_some() {
                        Value::T
                    } else {
                        Value::Nil
                    }
                } else {
                    found.unwrap_or(default)
                })
            }
            "ert-set-test" => {
                need_args(name, args, 2)?;
                let symbol = args[0].as_symbol()?.to_string();
                interp.ert_set_test(&symbol, &args[1])
            }
            #[dispatch(builtin_override)]
            "emaxx--cl-generic-apply-next" => {
                need_args(name, args, 4)?;
                let next = &args[0];
                let generic = args[1].clone();
                let kind = args[2].as_symbol()?.to_string();
                let call_args = args[3].to_vec()?;
                let exhausted = matches!(next, Value::Nil) || interp.callable_is_ignore(next);
                if !exhausted {
                    return invoke_function_value(interp, next, &call_args, env);
                }
                // GNU routes an exhausted chain through `cl-no-next-method' (a
                // method ran out of next methods) or `cl-no-applicable-method'
                // (no method matched at all).
                let (hook, mut hook_args) = if kind == "no-applicable" {
                    ("cl-no-applicable-method", vec![generic.clone()])
                } else {
                    ("cl-no-next-method", vec![generic.clone(), Value::Nil])
                };
                hook_args.extend(call_args.iter().cloned());
                if let Ok(hook_function) = interp.lookup_function(hook, env)
                    && !interp.callable_is_ignore(&hook_function)
                {
                    return invoke_function_value(interp, &hook_function, &hook_args, env);
                }
                // GNU's default methods signal the dedicated conditions
                // (cl-no-applicable-method GENERIC . ARGS).
                Err(LispError::SignalValue(Value::list(
                    std::iter::once(Value::Symbol(hook.into())).chain(hook_args),
                )))
            }
            #[dispatch(builtin_override)]
            "eieio--class-parents" => {
                need_args(name, args, 1)?;
                interp.class_parents_value(&args[0])
            }
            #[dispatch(builtin_override)]
            "eieio--class-default-object-cache" => {
                need_args(name, args, 1)?;
                if let Some(cache) = interp.raw_eieio_class_slot(&args[0], 9) {
                    return Ok(cache);
                }
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                Ok(interp
                    .class_default_object_cache(&class_name)
                    .unwrap_or(Value::Nil))
            }
            #[dispatch(builtin_override)]
            "eieio--class-options" => {
                need_args(name, args, 1)?;
                if let Some(options) = interp.raw_eieio_class_slot(&args[0], 10) {
                    return Ok(options);
                }
                let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                    return Err(LispError::TypeError("class".into(), args[0].type_name()));
                };
                Ok(interp
                    .get_symbol_property(&class_name, "emaxx-class-options")
                    .unwrap_or(Value::Nil))
            }
            #[dispatch(builtin_override)]
            "built-in-class-p" => {
                need_args(name, args, 1)?;
                Ok(
                    if args[0].as_symbol().ok().is_some_and(is_builtin_class_name) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            #[dispatch(builtin_override)]
            "cl-typep" => {
                need_args(name, args, 2)?;
                let matches = cl_typep_matches(interp, env, &args[0], &args[1])?;
                Ok(if matches { Value::T } else { Value::Nil })
            }
            #[dispatch(builtin_override)]
            "cl-functionp" => {
                need_args(name, args, 1)?;
                Ok(
                    if is_lambda_expression(&args[0])
                        || matches!(
                            cl_type_name(interp, &args[0])?,
                            "primitive-function"
                                | "special-form"
                                | "interpreted-function"
                                | "byte-code-function"
                        )
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "cl-proclaim" => {
                need_args(name, args, 1)?;
                let existing = interp
                    .lookup_var("cl--proclaims-deferred", env)
                    .unwrap_or(Value::Nil);
                interp.set_global_binding(
                    "cl--proclaims-deferred",
                    Value::cons(args[0].clone(), existing),
                );
                Ok(Value::Nil)
            }
            #[dispatch(builtin_override)]
            "url-scheme-get-property" => {
                // GNU url-methods.el maintains the real scheme registry once
                // loaded; the native table below is the no-file fallback.
                if let Some(delegated) = delegate_to_lisp_function(interp, name, args, env)? {
                    return Ok(delegated);
                }
                need_args(name, args, 2)?;
                let scheme = match &args[0] {
                    Value::Symbol(symbol) => symbol.to_string(),
                    _ => string_text(&args[0])?,
                }
                .to_ascii_lowercase();
                let property = args[1].as_symbol()?;
                let value = match property {
                    "default-port" => match scheme.as_str() {
                        "ftp" => Value::Integer(21),
                        "http" => Value::Integer(80),
                        "https" => Value::Integer(443),
                        "imap" => Value::Integer(143),
                        "ldap" => Value::Integer(389),
                        "nntp" => Value::Integer(119),
                        "pop" | "pop3" => Value::Integer(110),
                        "smtp" => Value::Integer(25),
                        "telnet" => Value::Integer(23),
                        _ => Value::Integer(0),
                    },
                    "name" => {
                        if scheme.is_empty() {
                            Value::String("unknown".into())
                        } else {
                            Value::String(scheme.into())
                        }
                    }
                    "loader" => {
                        if scheme.is_empty() {
                            Value::Symbol("url-scheme-default-loader".into())
                        } else {
                            Value::Symbol(format!("url-{scheme}").into())
                        }
                    }
                    "parse-url" => Value::Symbol("url-generic-parse-url".into()),
                    "asynchronous-p" => Value::Nil,
                    "file-directory-p" => Value::Symbol("ignore".into()),
                    // GNU's registry reads these from url-SCHEME.el; the
                    // http(s) methods come from the simple_compat url-http
                    // surface, everything else gets the registry defaults.
                    "expand-file-name" => match scheme.as_str() {
                        "http" | "https" => Value::Symbol("url-http-expand-file-name".into()),
                        _ => Value::Symbol("url-identity-expander".into()),
                    },
                    "file-exists-p" => match scheme.as_str() {
                        "http" | "https" => Value::Symbol("url-http-file-exists-p".into()),
                        _ => Value::Symbol("ignore".into()),
                    },
                    _ => Value::Nil,
                };
                Ok(value)
            }
        }
    }
);

fn symbol_file_from_load_history(
    interp: &Interpreter,
    object: &Value,
    kind: Option<&str>,
    env: &Env,
) -> Option<String> {
    let history = interp.lookup_var("load-history", env)?;
    for load_entry in history.to_vec().ok()? {
        let mut parts = load_entry.to_vec().ok()?.into_iter();
        let file = parts.next().and_then(|value| string_like(&value))?.text;
        let matches = parts.any(|definition| match kind {
            Some("defvar") => &definition == object,
            Some(expected_kind) => definition.cons_values().is_some_and(|(entry_kind, name)| {
                matches!(entry_kind, Value::Symbol(actual_kind) if actual_kind == expected_kind)
                    && &name == object
            }),
            None => {
                &definition == object
                    || definition.cons_values().is_some_and(|(entry_kind, name)| {
                        !matches!(entry_kind, Value::Symbol(ref actual_kind) if actual_kind == "require")
                            && &name == object
                    })
            }
        });
        if matches {
            return Some(file);
        }
    }
    None
}

struct SemanticCurrentSymbol {
    parts_value: Value,
    text: String,
    start: usize,
    end: usize,
}

fn semantic_ctxt_current_symbol(interp: &Interpreter) -> Option<SemanticCurrentSymbol> {
    let point = interp.buffer.point();
    if point <= interp.buffer.point_min() {
        return None;
    }
    let mut start = point;
    while start > interp.buffer.point_min() {
        let Some(ch) = interp.buffer.char_at(start - 1) else {
            break;
        };
        if !is_semantic_member_expr_char(ch) {
            break;
        }
        start -= 1;
    }
    if start == point {
        return None;
    }
    let text = interp.buffer.buffer_substring(start, point).ok()?;
    let parts = semantic_member_expression_parts(&text);
    if parts.is_empty() {
        return None;
    }
    let parts_value = Value::list(parts.into_iter().map(|value| Value::String(value.into())));
    Some(SemanticCurrentSymbol {
        parts_value,
        text,
        start,
        end: point,
    })
}

fn bounds_of_thing_at_point(interp: &Interpreter, thing: &str) -> Option<Value> {
    let is_thing_char: fn(char) -> bool = match thing {
        "symbol" => is_symbol_thing_char,
        "word" => |ch| ch.is_alphanumeric() || ch == '_',
        _ => return None,
    };
    let point = interp.buffer.point();
    let mut start = point;
    while start > interp.buffer.point_min() {
        let Some(ch) = interp.buffer.char_at(start - 1) else {
            break;
        };
        if !is_thing_char(ch) {
            break;
        }
        start -= 1;
    }
    let mut end = point;
    while end < interp.buffer.point_max() {
        let Some(ch) = interp.buffer.char_at(end) else {
            break;
        };
        if !is_thing_char(ch) {
            break;
        }
        end += 1;
    }
    (start < end).then(|| Value::cons(Value::Integer(start as i64), Value::Integer(end as i64)))
}

fn is_symbol_thing_char(ch: char) -> bool {
    ch.is_alphanumeric() || matches!(ch, '_' | '-')
}

fn is_semantic_member_expr_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric()
        || matches!(
            ch,
            '_' | '@' | '.' | ':' | '-' | '>' | '[' | ']' | '(' | ')'
        )
}

fn semantic_member_expression_parts(text: &str) -> Vec<String> {
    semantic_member_expression_steps(text)
        .into_iter()
        .map(|step| step.name)
        .collect()
}

#[derive(Clone)]
struct SemanticMemberStep {
    name: String,
    arrow_before: bool,
}

fn semantic_member_expression_steps(text: &str) -> Vec<SemanticMemberStep> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut arrow_before = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                parts.push(SemanticMemberStep {
                    name: semantic_normalize_member_part(&current),
                    arrow_before,
                });
                current.clear();
                arrow_before = false;
            }
            ':' if chars.peek() == Some(&':') => {
                chars.next();
                parts.push(SemanticMemberStep {
                    name: semantic_normalize_member_part(&current),
                    arrow_before,
                });
                current.clear();
                arrow_before = false;
            }
            '-' if chars.peek() == Some(&'>') => {
                chars.next();
                parts.push(SemanticMemberStep {
                    name: semantic_normalize_member_part(&current),
                    arrow_before,
                });
                current.clear();
                arrow_before = true;
            }
            _ => current.push(ch),
        }
    }
    parts.push(SemanticMemberStep {
        name: semantic_normalize_member_part(&current),
        arrow_before,
    });
    while parts.first().is_some_and(|part| part.name.is_empty()) {
        parts.remove(0);
    }
    parts
}

fn semantic_normalize_member_part(part: &str) -> String {
    let normalized = part.trim().trim_end_matches("()");
    normalized
        .split_once('[')
        .map(|(root, _)| root)
        .unwrap_or(normalized)
        .to_string()
}

fn semantic_makefile_possible_completions(
    interp: &Interpreter,
    env: &Env,
    symbol: &SemanticCurrentSymbol,
) -> Value {
    let prefix = symbol.text.as_str();
    let Ok(buffer_text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
    else {
        return Value::Nil;
    };
    let before_point = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .unwrap_or_default();
    let line_start = before_point.rfind('\n').map(|index| index + 1).unwrap_or(0);
    let line_before_point = &before_point[line_start..];
    let matches = if symbol.start > interp.buffer.point_min()
        && interp.buffer.char_at(symbol.start - 1) == Some('$')
    {
        semantic_makefile_variables(&buffer_text, prefix)
    } else if line_before_point
        .find('=')
        .is_some_and(|eq| line_start + eq < symbol.start)
    {
        semantic_makefile_file_names(interp, env, prefix)
    } else {
        semantic_makefile_targets(&buffer_text, prefix)
    };
    Value::list(
        matches
            .into_iter()
            .map(|name| semantic_tag(&name, "variable", Value::Nil)),
    )
}

fn semantic_mode_derived_p(interp: &Interpreter, env: &Env, ancestor: &str) -> bool {
    interp
        .lookup_var("major-mode", env)
        .and_then(|mode| mode.as_symbol().ok().map(str::to_string))
        .is_some_and(|mode| {
            derived_mode_parent_chain(interp, &mode)
                .iter()
                .any(|parent| parent == ancestor)
        })
}

fn semantic_makefile_variables(text: &str, prefix: &str) -> Vec<String> {
    let mut matches = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        if line.starts_with('#') {
            continue;
        }
        let Some(eq) = line.find('=') else {
            continue;
        };
        let name = line[..eq]
            .trim_end_matches([':', '+', '?'])
            .split_whitespace()
            .next()
            .unwrap_or("");
        if !name.is_empty() && name.starts_with(prefix) {
            matches.push(name.to_string());
        }
    }
    matches.sort();
    matches.dedup();
    matches
}

fn semantic_makefile_targets(text: &str, prefix: &str) -> Vec<String> {
    let mut matches = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        if line.starts_with('#') || line.starts_with('\t') || line.contains('=') {
            continue;
        }
        let Some(colon) = line.find(':') else {
            continue;
        };
        for target in line[..colon].split_whitespace() {
            if target.starts_with(prefix) && target != prefix {
                matches.push(target.to_string());
            }
        }
    }
    matches.sort();
    matches.dedup();
    matches
}

fn semantic_makefile_file_names(interp: &Interpreter, env: &Env, prefix: &str) -> Vec<String> {
    // GNU's Make override completes through `default-directory'.  Do not use
    // `buffer-file-truename' as a host path: files.el intentionally stores
    // that buffer slot in abbreviated (`~/...') form.
    let directory = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .or_else(|| {
            interp
                .buffer
                .file
                .as_deref()
                .and_then(|path| Path::new(path).parent())
                .map(|directory| directory.display().to_string())
        });
    let Some(directory) = directory else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(&directory) else {
        return Vec::new();
    };
    let mut matches = entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with(prefix))
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    matches
}

fn semantic_texinfo_possible_completions(symbol: &SemanticCurrentSymbol) -> Value {
    let prefix = if symbol.text.starts_with('@') {
        symbol.text.clone()
    } else {
        format!("@{}", symbol.text)
    };
    let commands = [
        "@bye",
        "@chapter",
        "@contents",
        "@copyright",
        "@c",
        "@end",
        "@format",
        "@ifinfo",
        "@input",
        "@macro",
        "@majorheading",
        "@menu",
        "@multitable",
        "@node",
        "@set",
        "@setfilename",
        "@settitle",
        "@sp",
        "@titlepage",
        "@top",
        "@value",
        "@vskip",
    ];
    Value::list(
        commands
            .into_iter()
            .filter(|command| command.starts_with(&prefix))
            .map(|command| semantic_tag(command, "function", Value::Nil)),
    )
}

fn semantic_wisent_possible_completions(
    interp: &Interpreter,
    symbol: &SemanticCurrentSymbol,
) -> Value {
    let Ok(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
    else {
        return Value::Nil;
    };
    let prefix = symbol.text.as_str();
    let mut matches = Vec::new();
    let mut grammar_section = false;
    for line in text.lines() {
        let line = line.split(";;").next().unwrap_or(line);
        let trimmed = line.trim();
        if trimmed == "%%" {
            grammar_section = !grammar_section;
            continue;
        }
        if grammar_section {
            if line.starts_with(char::is_whitespace) || trimmed.is_empty() {
                continue;
            }
            let Some(name) = trimmed
                .split(|ch: char| !is_ident_byte(ch as u8))
                .find(|part| !part.is_empty())
            else {
                continue;
            };
            if name.starts_with(prefix) {
                matches.push(name.to_string());
            }
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(directive) = parts.next() else {
            continue;
        };
        if !matches!(directive, "%token" | "%keyword") {
            continue;
        }
        let Some(name) = parts.find(|part| {
            part.as_bytes()
                .first()
                .is_some_and(|byte| is_ident_byte(*byte))
        }) else {
            continue;
        };
        if name.starts_with(prefix) {
            matches.push(name.to_string());
        }
    }
    matches.sort();
    matches.dedup();
    Value::list(
        matches
            .into_iter()
            .map(|name| semantic_tag(&name, "variable", Value::Nil)),
    )
}

fn semantic_analyze_possible_completions(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(symbol) = semantic_ctxt_current_symbol(interp) else {
        return Ok(Value::Nil);
    };
    if semantic_mode_derived_p(interp, env, "makefile-mode") {
        return Ok(semantic_makefile_possible_completions(interp, env, &symbol));
    }
    if interp
        .lookup_var("major-mode", env)
        .is_some_and(|mode| mode == Value::Symbol("texinfo-mode".into()))
    {
        return Ok(semantic_texinfo_possible_completions(&symbol));
    }
    if interp
        .lookup_var("major-mode", env)
        .is_some_and(|mode| mode == Value::Symbol("wisent-grammar-mode".into()))
    {
        return Ok(semantic_wisent_possible_completions(interp, &symbol));
    }
    let steps = semantic_member_expression_steps(&symbol.text);
    let parts = symbol.parts_value.to_vec()?;
    let parts = parts
        .iter()
        .filter_map(|part| string_text(part).ok())
        .collect::<Vec<_>>();

    let table = interp
        .lookup_var("semanticdb-current-table", env)
        .unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(Value::Nil);
    }
    let mut tags = semantic_tags_for_search(interp, &table)?;
    extend_semantic_c_like_table_tags(interp, &table, &mut tags);
    if parts.len() == 1 {
        let prefix = &parts[0];
        let mut matches = Vec::new();
        if let Some(current_type) = semantic_cpp_current_enclosing_type(interp, &tags)
            .or_else(|| semantic_c_like_current_enclosing_type(interp, &tags))
        {
            collect_semantic_member_completion_tags(
                &current_type,
                &tags,
                prefix,
                MemberVisibility::All,
                &mut Vec::new(),
                &mut matches,
            );
        }
        let before_locals = matches.len();
        collect_semantic_local_variable_completion_tags(interp, prefix, &mut matches);
        let local_matches_added = matches.len() > before_locals;
        if local_matches_added {
            collect_semantic_external_variable_completion_tags(&tags, prefix, &mut matches);
        }
        if matches.is_empty() {
            collect_semantic_using_namespace_completion_tags(interp, &tags, prefix, &mut matches);
        }
        if matches.is_empty() {
            collect_semantic_named_completion_tags(&tags, prefix, &mut matches);
        }
        if let Some(expected_type) = semantic_c_like_assignment_expected_type(interp) {
            matches.retain(|tag| semantic_tag_matches_expected_type(tag, &expected_type));
        }
        return Ok(Value::list(unique_semantic_completion_tags(matches)));
    }
    if parts.len() == 2 && symbol.text.contains("::") {
        let mut matches = Vec::new();
        collect_semantic_qualified_namespace_completion_tags(
            &tags,
            &parts[0],
            &parts[1],
            &mut matches,
        );
        if !matches.is_empty() {
            return Ok(Value::list(unique_semantic_completion_tags(matches)));
        }
    }

    let root_name = semantic_c_like_root_name(&parts[0]);
    let root_type = semantic_cpp_root_type_context(interp, &tags, &root_name)
        .or_else(|| semantic_type_context_from_name(&tags, &parts[0]));
    let Some(mut current_context) = root_type else {
        return Ok(Value::Nil);
    };
    let enclosing_type = semantic_cpp_current_enclosing_type(interp, &tags);
    let root_is_current_member = enclosing_type
        .as_ref()
        .is_some_and(|enclosing| semantic_type_member_named(enclosing, &root_name).is_some());
    let include_private = root_name == "this"
        || root_is_current_member
        || enclosing_type
            .and_then(|enclosing| semantic_tag_name(&enclosing))
            .zip(semantic_tag_name(&current_context.tag))
            .is_some_and(|(enclosing, current)| {
                enclosing == current && semantic_cpp_current_function_is_method(interp)
            });
    for (index, member_name) in parts[1..parts.len() - 1].iter().enumerate() {
        if steps.get(index + 1).is_some_and(|step| step.arrow_before) {
            current_context = semantic_cpp_arrow_context(&tags, current_context);
        }
        let Some(member) =
            semantic_type_member_named(&current_context.tag, member_name).or_else(|| {
                semantic_tag_name(&current_context.tag).and_then(|type_name| {
                    semantic_type_member_named_in_named_types(&tags, &type_name, member_name)
                })
            })
        else {
            return Ok(Value::Nil);
        };
        let Some(member_type) = semantic_type_context_from_member(&tags, &member, &current_context)
        else {
            return Ok(Value::Nil);
        };
        current_context = member_type;
    }
    if steps.last().is_some_and(|step| step.arrow_before) {
        current_context = semantic_cpp_arrow_context(&tags, current_context);
    }
    let prefix = parts.last().map(String::as_str).unwrap_or("");
    let mut matches = Vec::new();
    collect_semantic_member_completion_tags(
        &current_context.tag,
        &tags,
        prefix,
        if include_private {
            MemberVisibility::All
        } else {
            MemberVisibility::Public
        },
        &mut Vec::new(),
        &mut matches,
    );
    if !prefix.is_empty()
        && let Some(expected_type) = semantic_c_like_assignment_expected_type(interp)
    {
        matches.retain(|tag| semantic_tag_matches_expected_type(tag, &expected_type));
    }
    Ok(Value::list(unique_semantic_completion_tags(matches)))
}

fn semantic_analyze_tag_references(
    interp: &mut Interpreter,
    tag: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if semantic_tag_class(tag).as_deref() != Some("function") {
        return Ok(Value::Nil);
    }
    let table = interp
        .lookup_var("semanticdb-current-table", env)
        .unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(Value::Nil);
    }
    let mut tags = semantic_tags_for_search(interp, &table)?;
    extend_semantic_c_like_table_tags(interp, &table, &mut tags);
    let key = semantic_function_signature_key(tag);
    let mut impls = Vec::new();
    let mut protos = Vec::new();
    collect_semantic_function_references(&tags, &key, &mut impls, &mut protos);
    // The search tags concatenate the table's tag list with a fresh parse of
    // the same file (plus includes), so the same definition shows up twice.
    dedup_equal_semantic_tags(&mut impls);
    dedup_equal_semantic_tags(&mut protos);
    if std::env::var_os("EMAXX_DEBUG_SEMANTIC").is_some() {
        eprintln!(
            "[sem] refs tag={} buf={} impls={} protos={}",
            tag,
            interp.buffer.name,
            Value::list(impls.clone()),
            Value::list(protos.clone())
        );
    }
    if protos.is_empty() && impls.len() > 1 {
        protos.push(impls.remove(0));
    } else if protos.is_empty() && impls.len() == 1 {
        protos.push(impls[0].clone());
    }
    Ok(Value::list([
        Value::Symbol("emaxx-semantic-refs".into()),
        Value::list(impls),
        Value::list(protos),
    ]))
}

fn dedup_equal_semantic_tags(tags: &mut Vec<Value>) {
    // Ignore the properties slot: the same definition may appear once bare
    // (from the table) and once annotated with `:filename'.
    fn dedup_key(tag: &Value) -> Value {
        let Ok(mut items) = tag.to_vec() else {
            return tag.clone();
        };
        if items.len() >= 4 {
            items[3] = Value::Nil;
        }
        Value::list(items)
    }
    let mut seen: Vec<Value> = Vec::new();
    tags.retain(|tag| {
        let key = dedup_key(tag);
        if seen.iter().any(|existing| existing == &key) {
            false
        } else {
            seen.push(key);
            true
        }
    });
}

/// Stamp TAGS (and their members) with a `:filename' property so jumps can
/// switch to the file the tag came from, like semanticdb tags in GNU.
fn annotate_semantic_tags_with_filename(tags: Vec<Value>, path: &str) -> Vec<Value> {
    fn annotate(tag: &Value, path: &str) -> Value {
        let Ok(mut items) = tag.to_vec() else {
            return tag.clone();
        };
        if items.len() < 5 {
            items.resize(5, Value::Nil);
        }
        if items[3].is_nil() {
            items[3] = Value::list([
                Value::Symbol(":filename".into()),
                Value::String(path.into()),
            ]);
        }
        if let Ok(attrs) = items[2].to_vec() {
            let mut new_attrs = attrs;
            let mut index = 0usize;
            while index + 1 < new_attrs.len() {
                if matches!(&new_attrs[index], Value::Symbol(symbol) if symbol == ":members")
                    && let Ok(members) = new_attrs[index + 1].to_vec()
                {
                    new_attrs[index + 1] = Value::list(
                        members
                            .iter()
                            .map(|member| annotate(member, path))
                            .collect::<Vec<_>>(),
                    );
                }
                index += 2;
            }
            items[2] = Value::list(new_attrs);
        }
        Value::list(items)
    }
    tags.iter().map(|tag| annotate(tag, path)).collect()
}

fn semantic_tag_filename(tag: &Value) -> Option<String> {
    let items = tag.to_vec().ok()?;
    let props = items.get(3)?.to_vec().ok()?;
    let mut index = 0usize;
    while index + 1 < props.len() {
        if matches!(&props[index], Value::Symbol(symbol) if symbol == ":filename") {
            return string_text(&props[index + 1]).ok();
        }
        index += 2;
    }
    None
}

fn semantic_analyze_refs_part(refs: &Value, index: usize) -> Result<Value, LispError> {
    let refs = refs.to_vec()?;
    Ok(refs.get(index).cloned().unwrap_or(Value::Nil))
}

fn semantic_symref_find_references_by_name(
    interp: &Interpreter,
    name: &Value,
) -> Result<Value, LispError> {
    let name = string_text(name)?;
    let search_name = name.rsplit("::").next().unwrap_or(&name);
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    let file = interp
        .buffer
        .file
        .clone()
        .unwrap_or_else(|| interp.buffer.name.clone());
    let mut files = Vec::new();
    let mut tags = semantic_symref_tags_for_name(&text, search_name);
    if !tags.is_empty() {
        files.push(Value::String(file.clone().into()));
    }
    if let Some(base_dir) = Path::new(&file).parent() {
        for include in semantic_quoted_include_paths(&text, base_dir) {
            if let Ok(source) = std::fs::read_to_string(&include) {
                let include_tags = semantic_symref_header_tags_for_name(&source, search_name);
                if !include_tags.is_empty() {
                    files.push(Value::String(include.to_string_lossy().into_owned().into()));
                    tags.extend(include_tags);
                }
            }
        }
    }
    if tags.is_empty() {
        return Ok(Value::Nil);
    }
    Ok(Value::list([
        Value::Symbol("emaxx-semantic-symref-result".into()),
        Value::String(name.into()),
        Value::list(files),
        Value::list(tags),
    ]))
}

fn semantic_symref_result_part(result: &Value, index: usize) -> Result<Value, LispError> {
    let parts = result.to_vec()?;
    Ok(parts.get(index).cloned().unwrap_or(Value::Nil))
}

fn semantic_symref_hits_in_region(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(name) = semantic_tag_name(&args[0]) else {
        return Ok(Value::Nil);
    };
    let name = name.rsplit("::").next().unwrap_or(&name).to_string();
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    let (start, end) = semantic_symref_region_bounds(interp, args, &text);
    let region = text
        .get(start.saturating_sub(1)..end.saturating_sub(1).min(text.len()))
        .unwrap_or("");
    let mut search_start = 0usize;
    while let Some(relative) = region[search_start..].find(&name) {
        let column = search_start + relative;
        search_start = column + name.len();
        if !semantic_word_at(region, column, &name) {
            continue;
        }
        let hit_start = Value::Integer((start + column) as i64);
        let hit_end = Value::Integer((start + column + name.len()) as i64);
        call_function_value(
            interp,
            &args[1],
            &[hit_start, hit_end, Value::String(name.clone().into())],
            env,
        )?;
    }
    Ok(Value::Nil)
}

fn semantic_symref_test_count_hits_in_tag(interp: &Interpreter) -> Result<Value, LispError> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    let point = interp
        .buffer
        .point()
        .saturating_sub(interp.buffer.point_min());
    let before = &text[..point.min(text.len())];
    let name_end = before
        .char_indices()
        .rev()
        .find(|(_, ch)| is_ident_byte(*ch as u8))
        .map(|(index, ch)| index + ch.len_utf8());
    let Some(name_end) = name_end else {
        return Ok(Value::Nil);
    };
    let name_start = before[..name_end]
        .char_indices()
        .rev()
        .find(|(_, ch)| !is_ident_byte(*ch as u8))
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(0);
    let name = &before[name_start..name_end];
    let Some(range) = semantic_c_function_ranges(&text)
        .into_iter()
        .find(|range| range.start <= point && point <= range.end)
    else {
        return Ok(Value::Nil);
    };
    let mut count = 0i64;
    for line in text[range.start..range.end.min(text.len())].lines() {
        let region = line.split("//").next().unwrap_or(line);
        let mut search_start = 0usize;
        while let Some(relative) = region[search_start..].find(name) {
            let column = search_start + relative;
            search_start = column + name.len();
            if semantic_word_at(region, column, name) {
                count += 1;
            }
        }
    }
    Ok(Value::Integer(count))
}

fn semantic_symref_region_bounds(
    interp: &Interpreter,
    args: &[Value],
    text: &str,
) -> (usize, usize) {
    if let (Ok(start), Ok(end)) = (args[2].as_integer(), args[3].as_integer()) {
        return (start as usize, end as usize);
    }
    let point = interp
        .buffer
        .point()
        .saturating_sub(interp.buffer.point_min());
    semantic_c_function_ranges(text)
        .into_iter()
        .find(|range| range.start <= point && point <= range.end)
        .map(|range| (range.start + 1, range.end + 1))
        .unwrap_or((interp.buffer.point_min(), interp.buffer.point_max()))
}

struct SemanticFunctionRange {
    name: String,
    start: usize,
    end: usize,
}

fn semantic_symref_tags_for_name(text: &str, name: &str) -> Vec<Value> {
    let function_ranges = semantic_c_function_ranges(text);
    let mut tags = Vec::new();
    let mut offset = 0usize;
    for line in text.lines() {
        let line_start = offset;
        let line_end = line_start + line.len();
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with('*') {
            offset = line_end + 1;
            continue;
        }
        let mut search_start = 0usize;
        while let Some(relative) = line[search_start..].find(name) {
            let column = search_start + relative;
            let absolute = line_start + column;
            search_start = column + name.len();
            if !semantic_word_at(line, column, name) {
                continue;
            }
            if trimmed.starts_with("#define") {
                tags.push(semantic_tag(name, "function", Value::Nil));
                continue;
            }
            if let Some(function_name) = semantic_c_function_name_from_signature(line)
                && semantic_cpp_name_matches(&function_name, name)
            {
                tags.push(semantic_tag(&function_name, "function", Value::Nil));
                continue;
            }
            if let Some(function) = function_ranges
                .iter()
                .rev()
                .find(|function| function.start <= absolute && absolute <= function.end)
            {
                tags.push(semantic_tag(&function.name, "function", Value::Nil));
            }
        }
        offset = line_end + 1;
    }
    tags
}

fn semantic_quoted_include_paths(text: &str, base_dir: &Path) -> Vec<PathBuf> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let rest = line.strip_prefix("#include")?.trim_start();
            let include = rest.strip_prefix('"')?.split_once('"')?.0;
            Some(base_dir.join(include))
        })
        .collect()
}

fn semantic_symref_header_tags_for_name(text: &str, name: &str) -> Vec<Value> {
    let mut tags = Vec::new();
    let mut current_class: Option<String> = None;
    for line in text.lines() {
        let line = line.split("//").next().unwrap_or("").trim();
        if line.starts_with("class ") || line.starts_with("struct ") {
            current_class = line
                .split_whitespace()
                .nth(1)
                .map(|name| name.trim_matches('{').to_string());
            continue;
        }
        if line.starts_with("};") || line.starts_with("} ") || line == "}" {
            current_class = None;
            continue;
        }
        if line.contains(name)
            && line.contains('(')
            && line.ends_with(';')
            && let Some(function_name) = semantic_c_function_name_from_signature(line)
            && semantic_cpp_name_matches(&function_name, name)
        {
            tags.push(semantic_tag(
                current_class.as_deref().unwrap_or(&function_name),
                "function",
                Value::Nil,
            ));
        }
    }
    tags
}

fn semantic_c_function_ranges(text: &str) -> Vec<SemanticFunctionRange> {
    let lines = text.lines().collect::<Vec<_>>();
    let mut ranges = Vec::new();
    let mut offset = 0usize;
    let mut line_starts = Vec::with_capacity(lines.len());
    for line in &lines {
        line_starts.push(offset);
        offset += line.len() + 1;
    }
    for (index, line) in lines.iter().enumerate() {
        let (name, range_start_index) =
            if let Some(name) = semantic_c_function_name_from_signature(line) {
                (name, index)
            } else if index > 0 {
                let Some(previous_index) = (0..index)
                    .rev()
                    .find(|previous| !lines[*previous].trim().is_empty())
                else {
                    continue;
                };
                let combined = format!("{} {}", lines[previous_index].trim(), line.trim());
                let Some(name) = semantic_c_function_name_from_signature(&combined) else {
                    continue;
                };
                (name, previous_index)
            } else {
                continue;
            };
        let Some(brace_start) = semantic_c_next_open_brace(&lines, &line_starts, index) else {
            continue;
        };
        let end = semantic_c_matching_brace(text, brace_start).unwrap_or(text.len());
        ranges.push(SemanticFunctionRange {
            name,
            start: line_starts[range_start_index],
            end,
        });
    }
    ranges
}

fn semantic_c_next_open_brace(
    lines: &[&str],
    line_starts: &[usize],
    start_index: usize,
) -> Option<usize> {
    for index in start_index..lines.len().min(start_index + 5) {
        let trimmed = lines[index].trim_start();
        if trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with('*') {
            continue;
        }
        if let Some(column) = lines[index].find('{') {
            return Some(line_starts[index] + column);
        }
        if lines[index].contains(';') {
            return None;
        }
    }
    None
}

fn semantic_c_matching_brace(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(open + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn semantic_c_function_name_from_signature(line: &str) -> Option<String> {
    let code = line.split("//").next().unwrap_or(line).trim();
    if code.is_empty() || code.starts_with('#') || code.starts_with('*') || code.contains('=') {
        return None;
    }
    let before_paren = code.split_once('(')?.0.trim_end();
    if !before_paren.contains(char::is_whitespace) && !before_paren.contains("::") {
        return None;
    }
    let name = before_paren
        .rsplit(char::is_whitespace)
        .find(|part| !part.is_empty())?;
    if matches!(
        name,
        "if" | "for" | "while" | "switch" | "return" | "sizeof"
    ) {
        return None;
    }
    Some(name.to_string())
}

fn semantic_cpp_name_matches(candidate: &str, name: &str) -> bool {
    candidate == name || candidate.rsplit("::").next() == Some(name)
}

fn semantic_word_at(line: &str, start: usize, name: &str) -> bool {
    let bytes = line.as_bytes();
    let end = start + name.len();
    start <= bytes.len()
        && end <= bytes.len()
        && (start == 0 || !is_ident_byte(bytes[start - 1]))
        && (end == bytes.len() || !is_ident_byte(bytes[end]))
}

fn semantic_tags_equivalent(left: &Value, right: &Value) -> bool {
    let left_class = semantic_tag_class(left);
    if left_class != semantic_tag_class(right)
        || semantic_tag_name(left) != semantic_tag_name(right)
    {
        return false;
    }
    // GNU compares positions whenever both tags carry them: a prototype and
    // its implementation share a name but are NOT equivalent.
    match (semantic_tag_bounds(left), semantic_tag_bounds(right)) {
        (Some(left_bounds), Some(right_bounds)) => return left_bounds == right_bounds,
        (None, None) => {}
        _ => return false,
    }
    if left_class.as_deref() == Some("function") {
        return semantic_function_signature_matches(
            &semantic_function_signature_key(left),
            &semantic_function_signature_key(right),
        ) || semantic_function_signature_matches(
            &semantic_function_signature_key(right),
            &semantic_function_signature_key(left),
        );
    }
    true
}

fn semantic_go_to_tag(
    interp: &mut Interpreter,
    tag: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let items = tag.to_vec()?;
    // Tags from other files carry a `:filename' property; the jump has to
    // land in that file's buffer, as in GNU.
    if let Some(filename) = semantic_tag_filename(tag)
        && interp.buffer.file.as_deref() != Some(filename.as_str())
    {
        let buffer = super::call(
            interp,
            "find-file-noselect",
            &[Value::String(filename.into())],
            env,
        )?;
        super::call(interp, "set-buffer", &[buffer], env)?;
    }
    if let Some(Value::Overlay(overlay_id)) = items.get(4)
        && let Some(overlay) = interp.find_overlay(*overlay_id)
    {
        interp.buffer.goto_char(overlay.beg);
        interp.set_variable("__emaxx-semantic-current-tag-override", tag.clone(), env);
        return Ok(tag.clone());
    }
    if let Some((start, _)) = semantic_tag_bounds(tag) {
        interp.buffer.goto_char(start.max(1) as usize);
        interp.set_variable("__emaxx-semantic-current-tag-override", tag.clone(), env);
        return Ok(tag.clone());
    }
    interp.buffer.goto_char(interp.buffer.point_min());
    interp.set_variable("__emaxx-semantic-current-tag-override", tag.clone(), env);
    Ok(tag.clone())
}

#[derive(Eq, PartialEq)]
struct SemanticFunctionSignatureKey {
    name: Option<String>,
    parent: Option<String>,
    arg_types: Vec<String>,
}

fn semantic_function_signature_key(tag: &Value) -> SemanticFunctionSignatureKey {
    SemanticFunctionSignatureKey {
        name: semantic_tag_name(tag),
        parent: semantic_tag_attr(tag, ":parent").and_then(|value| string_text(&value).ok()),
        arg_types: semantic_function_arg_types(tag),
    }
}

fn semantic_function_arg_types(tag: &Value) -> Vec<String> {
    semantic_tag_attr(tag, ":arguments")
        .and_then(|args| args.to_vec().ok())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|arg| semantic_tag_attr(&arg, ":type"))
        .filter_map(|type_value| {
            semantic_type_name_parts(&type_value)
                .ok()
                .map(|parts| parts.join("::"))
        })
        .collect()
}

fn collect_semantic_function_references(
    tags: &[Value],
    key: &SemanticFunctionSignatureKey,
    impls: &mut Vec<Value>,
    protos: &mut Vec<Value>,
) {
    collect_semantic_function_references_in(tags, None, key, impls, protos);
}

fn collect_semantic_function_references_in(
    tags: &[Value],
    enclosing_type: Option<&str>,
    key: &SemanticFunctionSignatureKey,
    impls: &mut Vec<Value>,
    protos: &mut Vec<Value>,
) {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("function") {
            let mut candidate = semantic_function_signature_key(tag);
            // A method declared inside its class carries no `:parent';
            // the enclosing type is its parent.
            if candidate.parent.is_none() {
                candidate.parent = enclosing_type.map(str::to_string);
            }
            if semantic_function_signature_matches(&candidate, key) {
                if semantic_tag_attr(tag, ":prototype-flag").is_some_and(|value| value.is_truthy())
                {
                    protos.push(tag.clone());
                } else {
                    impls.push(tag.clone());
                }
            }
        }
        let next_enclosing = if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_attr(tag, ":type")
                .and_then(|value| string_text(&value).ok())
                .as_deref()
                != Some("namespace")
        {
            semantic_tag_name(tag)
        } else {
            enclosing_type.map(str::to_string)
        };
        collect_semantic_function_references_in(
            &semantic_tag_members(tag),
            next_enclosing.as_deref(),
            key,
            impls,
            protos,
        );
    }
}

fn semantic_function_signature_matches(
    candidate: &SemanticFunctionSignatureKey,
    target: &SemanticFunctionSignatureKey,
) -> bool {
    candidate.name == target.name
        && candidate.arg_types == target.arg_types
        && (candidate.parent == target.parent || candidate.parent.is_none())
}

#[derive(Clone)]
struct SemanticTypeContext {
    tag: Value,
    substitutions: HashMap<String, String>,
}

fn semantic_cpp_root_type_context(
    interp: &Interpreter,
    tags: &[Value],
    name: &str,
) -> Option<SemanticTypeContext> {
    if name == "this" {
        return semantic_cpp_current_enclosing_type(interp, tags)
            .or_else(|| semantic_c_like_current_enclosing_type(interp, tags))
            .map(|tag| SemanticTypeContext {
                tag,
                substitutions: HashMap::new(),
            });
    }
    semantic_cpp_declared_type_before_point(interp, name)
        .and_then(|type_name| {
            semantic_type_context_from_name_in_scope(tags, Some(interp), &type_name)
        })
        .or_else(|| {
            semantic_cpp_current_enclosing_type(interp, tags).and_then(|current| {
                let member = semantic_type_member_named(&current, name)?;
                semantic_type_context_from_member(
                    tags,
                    &member,
                    &SemanticTypeContext {
                        tag: current,
                        substitutions: HashMap::new(),
                    },
                )
            })
        })
        .or_else(|| {
            semantic_c_like_current_enclosing_type(interp, tags).and_then(|current| {
                let member = semantic_type_member_named(&current, name)?;
                semantic_type_context_from_member(
                    tags,
                    &member,
                    &SemanticTypeContext {
                        tag: current,
                        substitutions: HashMap::new(),
                    },
                )
            })
        })
        .or_else(|| {
            find_semantic_variable_deep(tags, name).and_then(|tag| {
                semantic_type_context_from_member(
                    tags,
                    &tag,
                    &SemanticTypeContext {
                        tag: Value::Nil,
                        substitutions: HashMap::new(),
                    },
                )
            })
        })
}

fn semantic_type_context_from_member(
    tags: &[Value],
    member: &Value,
    parent: &SemanticTypeContext,
) -> Option<SemanticTypeContext> {
    if semantic_tag_class(member).as_deref() == Some("type") {
        return semantic_type_candidate(tags, member).map(|tag| SemanticTypeContext {
            tag,
            substitutions: HashMap::new(),
        });
    }
    let type_text = semantic_tag_attr(member, ":type").and_then(|value| semantic_type_text(&value));
    let type_text = semantic_substitute_type_text(&type_text?, &parent.substitutions);
    semantic_type_context_from_name(tags, &type_text)
}

fn semantic_cpp_arrow_context(tags: &[Value], context: SemanticTypeContext) -> SemanticTypeContext {
    let Some(operator) = semantic_type_member_named(&context.tag, "operator->") else {
        return context;
    };
    semantic_type_context_from_member(tags, &operator, &context).unwrap_or(context)
}

fn semantic_type_context_from_name(tags: &[Value], type_name: &str) -> Option<SemanticTypeContext> {
    semantic_type_context_from_name_with_buffer(tags, None, type_name)
}

fn semantic_type_context_from_name_with_buffer(
    tags: &[Value],
    interp: Option<&Interpreter>,
    type_name: &str,
) -> Option<SemanticTypeContext> {
    let type_name = semantic_clean_cpp_type_text(type_name);
    let (base_name, args) = semantic_cpp_template_instantiation(&type_name)
        .unwrap_or_else(|| (type_name.clone(), Vec::new()));
    if args.is_empty()
        && let Some(raw) = find_semantic_type_raw(tags, &base_name)
        && let Some(target) =
            semantic_tag_attr(&raw, ":typedef").and_then(|value| semantic_type_text(&value))
    {
        return semantic_type_context_from_name(tags, &target);
    }
    let mut tag = semantic_type_from_name(tags, &base_name).or_else(|| {
        base_name
            .rsplit("::")
            .next()
            .and_then(|name| semantic_type_from_name(tags, name))
    })?;
    if semantic_tag_members(&tag).is_empty()
        && let Some(interp) = interp
        && let Some(name) = base_name.rsplit("::").next()
        && let Some(buffer_type) = semantic_c_type_from_current_buffer(interp, name)
    {
        tag = buffer_type;
    }
    let substitutions = semantic_template_substitutions(&tag, &args);
    Some(SemanticTypeContext { tag, substitutions })
}

fn semantic_type_context_from_name_in_scope(
    tags: &[Value],
    interp: Option<&Interpreter>,
    type_name: &str,
) -> Option<SemanticTypeContext> {
    if type_name.contains("::") {
        return semantic_type_context_from_name_with_buffer(tags, interp, type_name);
    }
    if let Some(interp) = interp {
        for namespace in semantic_cpp_active_using_namespaces(interp, tags)
            .into_iter()
            .rev()
        {
            if let Some(context) = semantic_type_context_from_name_with_buffer(
                tags,
                Some(interp),
                &format!("{namespace}::{type_name}"),
            ) {
                return Some(context);
            }
        }
    }
    semantic_type_context_from_name_with_buffer(tags, interp, type_name)
}

fn find_semantic_type_raw(tags: &[Value], name: &str) -> Option<Value> {
    let parts = name
        .split("::")
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    find_semantic_type_raw_parts(tags, &parts).or_else(|| {
        parts
            .last()
            .and_then(|last| find_semantic_type_raw_deep(tags, last))
    })
}

fn find_semantic_type_raw_parts(tags: &[Value], parts: &[&str]) -> Option<Value> {
    let (first, rest) = parts.split_first()?;
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(first)
        {
            if rest.is_empty() {
                return Some(tag.clone());
            }
            if let Some(found) = find_semantic_type_raw_parts(&semantic_tag_members(tag), rest) {
                return Some(found);
            }
        }
    }
    None
}

fn find_semantic_type_raw_deep(tags: &[Value], name: &str) -> Option<Value> {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(name)
        {
            return Some(tag.clone());
        }
        if let Some(found) = find_semantic_type_raw_deep(&semantic_tag_members(tag), name) {
            return Some(found);
        }
    }
    None
}

fn semantic_template_substitutions(tag: &Value, args: &[String]) -> HashMap<String, String> {
    let params = semantic_tag_attr(tag, ":template-params")
        .and_then(|params| params.to_vec().ok())
        .unwrap_or_default();
    params
        .into_iter()
        .filter_map(|param| string_text(&param).ok())
        .zip(args.iter().cloned())
        .collect()
}

fn semantic_type_text(value: &Value) -> Option<String> {
    if let Ok(symbol) = value.as_symbol() {
        return Some(symbol.to_string());
    }
    if let Ok(text) = string_text(value) {
        return Some(text);
    }
    let items = value.to_vec().ok()?;
    items.first().and_then(semantic_type_text)
}

fn semantic_clean_cpp_type_text(type_name: &str) -> String {
    type_name
        .replace("const ", "")
        .replace(" const", "")
        .replace("mutable ", "")
        .replace(" mutable", "")
        .replace("struct ", "")
        .replace("class ", "")
        .replace("public ", "")
        .replace("private ", "")
        .replace("protected ", "")
        .replace("static ", "")
        .replace(" static", "")
        .replace("volatile ", "")
        .replace(" volatile", "")
        .replace(['*', '&'], "")
        .trim()
        .to_string()
}

fn semantic_substitute_type_text(
    type_text: &str,
    substitutions: &HashMap<String, String>,
) -> String {
    if substitutions.is_empty() {
        return type_text.to_string();
    }
    let mut out = String::new();
    let mut word = String::new();
    for ch in type_text.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            word.push(ch);
        } else {
            if !word.is_empty() {
                out.push_str(
                    substitutions
                        .get(&word)
                        .map(String::as_str)
                        .unwrap_or(&word),
                );
                word.clear();
            }
            out.push(ch);
        }
    }
    if !word.is_empty() {
        out.push_str(
            substitutions
                .get(&word)
                .map(String::as_str)
                .unwrap_or(&word),
        );
    }
    out
}

fn semantic_cpp_template_instantiation(type_name: &str) -> Option<(String, Vec<String>)> {
    let open = type_name.find('<')?;
    let close = type_name.rfind('>')?;
    if close <= open {
        return None;
    }
    let base = type_name[..open].trim().to_string();
    let args = split_cpp_top_level_commas(&type_name[open + 1..close])
        .into_iter()
        .map(|arg| semantic_clean_cpp_type_text(&arg))
        .collect::<Vec<_>>();
    Some((base, args))
}

fn split_cpp_top_level_commas(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;
    for ch in text.chars() {
        match ch {
            '<' => {
                angle_depth += 1;
                current.push(ch);
            }
            '>' => {
                angle_depth = angle_depth.saturating_sub(1);
                current.push(ch);
            }
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if angle_depth == 0 && paren_depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn semantic_cpp_current_enclosing_type(interp: &Interpreter, tags: &[Value]) -> Option<Value> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    let mut stack: Vec<Option<String>> = Vec::new();
    let mut pending_method_type = None;
    for line in text.lines() {
        let line = line.split("//").next().unwrap_or(line);
        if line.contains('(')
            && let Some(scope_index) = line.rfind("::")
        {
            let before_scope = line[..scope_index].trim_end();
            if before_scope.contains(char::is_whitespace) {
                pending_method_type = before_scope
                    .split_whitespace()
                    .last()
                    .map(|name| name.trim_matches(|ch| matches!(ch, '*' | '&')).to_string());
            } else {
                pending_method_type = None;
            }
        }
        for ch in line.chars() {
            match ch {
                '{' => stack.push(pending_method_type.take()),
                '}' => {
                    stack.pop();
                }
                _ => {}
            }
        }
    }
    stack
        .into_iter()
        .rev()
        .find_map(|name| name.and_then(|name| semantic_type_from_name(tags, &name)))
}

fn semantic_c_like_current_enclosing_type(interp: &Interpreter, tags: &[Value]) -> Option<Value> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    let mut stack = Vec::new();
    let mut pending_class: Option<String> = None;
    let mut index = 0usize;
    let bytes = text.as_bytes();
    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'/') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        if word_at(&text, index, "class") || word_at(&text, index, "interface") {
            index += if word_at(&text, index, "interface") {
                "interface".len()
            } else {
                "class".len()
            };
            while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
                index += 1;
            }
            let start = index;
            while bytes.get(index).is_some_and(|byte| is_ident_byte(*byte)) {
                index += 1;
            }
            if index > start {
                pending_class = Some(text[start..index].to_string());
            }
            continue;
        }
        match bytes[index] {
            b'{' => {
                if let Some(class_name) = pending_class.take() {
                    stack.push(class_name);
                } else {
                    stack.push(String::new());
                }
            }
            b'}' => {
                stack.pop();
            }
            _ => {}
        }
        index += 1;
    }
    stack
        .into_iter()
        .rev()
        .find(|name| !name.is_empty())
        .and_then(|name| semantic_type_from_name(tags, &name))
}

fn semantic_cpp_current_function_is_method(interp: &Interpreter) -> bool {
    let Ok(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
    else {
        return false;
    };
    semantic_c_function_ranges(&text)
        .last()
        .is_some_and(|range| range.name.contains("::"))
}

fn word_at(text: &str, index: usize, word: &str) -> bool {
    text[index..].starts_with(word)
        && (index == 0 || !is_ident_byte(text.as_bytes()[index - 1]))
        && text
            .as_bytes()
            .get(index + word.len())
            .is_none_or(|byte| !is_ident_byte(*byte))
}

fn semantic_cpp_declared_type_before_point(interp: &Interpreter, name: &str) -> Option<String> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    text.split([';', '(', ')', '\n'])
        .rev()
        .filter_map(|segment| semantic_cpp_declared_type_from_segment(segment, name))
        .next()
        .or_else(|| semantic_c_macro_declared_type_before_point(&text, name))
}

fn semantic_c_macro_declared_type_before_point(text: &str, name: &str) -> Option<String> {
    let mut macros = Vec::new();
    let lines = text.lines().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < lines.len() {
        let line = lines[index].trim_start();
        if !line.starts_with("#define ") {
            index += 1;
            continue;
        }
        let mut definition = line.to_string();
        while definition.trim_end().ends_with('\\') && index + 1 < lines.len() {
            index += 1;
            definition.push(' ');
            definition.push_str(lines[index].trim());
        }
        if let Some((macro_name, variable_name)) = parse_c_typed_variable_macro(&definition) {
            macros.push((macro_name, variable_name));
        }
        index += 1;
    }
    for (macro_name, variable_name) in macros.iter().rev() {
        if variable_name != name {
            continue;
        }
        for line in lines.iter().rev() {
            let line = line.split("//").next().unwrap_or(line).trim();
            let Some(args) = line
                .strip_prefix(macro_name)
                .and_then(|rest| rest.strip_prefix('('))
                .and_then(|rest| rest.split_once(')'))
                .map(|(args, _)| args)
            else {
                continue;
            };
            let type_name = args.split(',').next()?.trim();
            if !type_name.is_empty() {
                return Some(type_name.to_string());
            }
        }
    }
    None
}

fn semantic_c_type_from_current_buffer(interp: &Interpreter, type_name: &str) -> Option<Value> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
        .ok()?;
    let cleaned = strip_cpp_comments(&text);
    let pattern = format!("struct {type_name}");
    let start = cleaned.find(&pattern)?;
    let brace = cleaned[start..].find('{')? + start;
    let mut depth = 0usize;
    let mut end = brace;
    for (offset, ch) in cleaned[brace..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    end = brace + offset;
                    break;
                }
            }
            _ => {}
        }
    }
    if end <= brace {
        return None;
    }
    let body = &cleaned[brace + 1..end];
    let mut parser = CppTagParser::new(body, None);
    semantic_type_tag(
        type_name,
        vec![
            (":members", Value::list(parser.parse_until(None))),
            (":type", Value::String("struct".into())),
        ],
    )
}

fn parse_c_typed_variable_macro(definition: &str) -> Option<(String, String)> {
    let rest = definition.trim_start().strip_prefix("#define ")?;
    let (macro_name, body) = rest.split_once(')')?;
    let macro_name = macro_name.split_once('(')?.0.trim();
    let variable_name = body
        .split("*")
        .nth(1)?
        .trim_start()
        .split(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    Some((macro_name.to_string(), variable_name.to_string()))
}

fn collect_semantic_local_variable_completion_tags(
    interp: &Interpreter,
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    let Some(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()
    else {
        return;
    };
    let cleaned = strip_cpp_comments(&text);
    let Some((body_start, arguments)) = current_cpp_function_scope(&cleaned) else {
        return;
    };
    for argument in arguments {
        if semantic_tag_name(&argument)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(argument);
        }
    }

    let mut scoped_tags: Vec<(usize, Value)> = Vec::new();
    let mut depth = 0usize;
    let mut initializer_depth = 0usize;
    let mut segment = String::new();
    for ch in cleaned[body_start + 1..].chars() {
        if initializer_depth > 0 {
            segment.push(ch);
            match ch {
                '{' => initializer_depth += 1,
                '}' => initializer_depth = initializer_depth.saturating_sub(1),
                _ => {}
            }
            continue;
        }
        match ch {
            '{' => {
                if segment.contains('=') && !semantic_c_like_control_segment(&segment) {
                    initializer_depth = 1;
                    segment.push(ch);
                } else {
                    segment.clear();
                    depth += 1;
                }
            }
            '}' => {
                collect_cpp_local_variable_segment(&segment, depth, &mut scoped_tags);
                segment.clear();
                scoped_tags.retain(|(tag_depth, _)| *tag_depth < depth);
                depth = depth.saturating_sub(1);
            }
            ';' | '\n' => {
                collect_cpp_local_variable_segment(&segment, depth, &mut scoped_tags);
                segment.clear();
            }
            _ => segment.push(ch),
        }
    }
    for (_, tag) in &scoped_tags {
        if semantic_tag_name(tag)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
    }
}

fn semantic_c_like_control_segment(segment: &str) -> bool {
    let segment = segment.trim_start();
    matches!(
        segment
            .split(|ch: char| ch.is_whitespace() || ch == '(')
            .next(),
        Some("if" | "for" | "while" | "switch" | "catch")
    )
}

fn current_cpp_function_scope(text: &str) -> Option<(usize, Vec<Value>)> {
    let mut stack = Vec::new();
    for (index, ch) in text.char_indices() {
        match ch {
            '{' => stack.push(index),
            '}' => {
                stack.pop();
            }
            _ => {}
        }
    }
    stack.into_iter().rev().find_map(|open| {
        cpp_function_arguments_before_open(text, open).map(|arguments| (open, arguments))
    })
}

fn cpp_function_arguments_before_open(text: &str, open: usize) -> Option<Vec<Value>> {
    let before_open = text[..open].trim_end();
    let close = before_open.rfind(')')?;
    if before_open[close + 1..]
        .chars()
        .any(|ch| !ch.is_whitespace())
    {
        return None;
    }
    let open_paren = before_open[..close].rfind('(')?;
    let before_paren = before_open[..open_paren].trim_end();
    let name = before_paren
        .rsplit(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    if matches!(name, "if" | "for" | "while" | "switch" | "catch") {
        return None;
    }
    Some(parse_cpp_arguments(&before_open[open_paren + 1..close]))
}

fn collect_cpp_local_variable_segment(
    segment: &str,
    depth: usize,
    scoped_tags: &mut Vec<(usize, Value)>,
) {
    let Some(tag) = parse_cpp_variable(segment) else {
        return;
    };
    scoped_tags.push((depth, tag));
}

fn semantic_c_like_root_name(name: &str) -> String {
    name.split_once('[')
        .map(|(root, _)| root)
        .unwrap_or(name)
        .to_string()
}

fn semantic_c_like_assignment_expected_type(interp: &Interpreter) -> Option<String> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    let line = text.lines().last()?.split("//").next()?.trim_end();
    let eq_index = line.rfind('=')?;
    let lhs = line[..eq_index]
        .trim_end()
        .rsplit(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    semantic_cpp_declared_type_before_point(interp, lhs)
}

fn semantic_tag_matches_expected_type(tag: &Value, expected_type: &str) -> bool {
    if semantic_tag_class(tag).as_deref() == Some("function") {
        return semantic_tag_attr(tag, ":type")
            .and_then(|value| semantic_type_name_parts(&value).ok())
            .is_some_and(|parts| parts.iter().any(|part| part == expected_type));
    }
    if semantic_tag_class(tag).as_deref() == Some("variable") {
        return semantic_tag_attr(tag, ":type")
            .and_then(|value| semantic_type_name_parts(&value).ok())
            .and_then(|parts| parts.last().cloned())
            .is_some_and(|part| part == expected_type);
    }
    true
}

fn semantic_cpp_declared_type_from_segment(segment: &str, name: &str) -> Option<String> {
    let segment = segment.split("//").next().unwrap_or(segment).trim();
    let mut search_end = segment.len();
    while let Some(index) = segment[..search_end].rfind(name) {
        let before = &segment[..index];
        let after = &segment[index + name.len()..];
        search_end = index;
        if before
            .chars()
            .next_back()
            .is_some_and(|ch| is_ident_byte(ch as u8))
            || after
                .chars()
                .next()
                .is_some_and(|ch| is_ident_byte(ch as u8))
        {
            continue;
        }
        let after = after.trim_start();
        if !after.is_empty() && !matches!(after.chars().next(), Some(';' | ',' | ')' | '=' | '[')) {
            continue;
        }
        let before = before
            .trim_end_matches(|ch: char| ch.is_whitespace() || matches!(ch, '*' | '&'))
            .trim();
        let type_name_storage;
        let type_name = if before.contains('<') && before.contains('>') {
            type_name_storage = semantic_clean_cpp_type_text(before);
            type_name_storage.as_str()
        } else {
            before
                .split_whitespace()
                .rev()
                .find(|token| {
                    !matches!(
                        *token,
                        "const" | "struct" | "class" | "mutable" | "static" | "volatile"
                    )
                })?
                .trim_matches(|ch| matches!(ch, '*' | '&'))
        };
        if !type_name.is_empty()
            && type_name != "_type"
            && !semantic_c_like_statement_keyword(type_name)
        {
            return Some(type_name.to_string());
        }
    }
    None
}

fn semantic_type_from_name(tags: &[Value], type_name: &str) -> Option<Value> {
    let parts = type_name
        .split("::")
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    find_semantic_type_by_parts(tags, &parts)
        .or_else(|| find_semantic_type_chain_in(tags, tags, &parts))
        .or_else(|| (parts.len() > 1).then(|| find_semantic_type_chain_anywhere(tags, &parts))?)
        .or_else(|| {
            parts
                .last()
                .and_then(|name| find_semantic_type_deep(tags, name))
        })
}

fn semantic_namespace_type_from_name(tags: &[Value], namespace: &str) -> Option<Value> {
    let namespace = semantic_resolve_namespace_alias_name(tags, namespace);
    semantic_type_from_name(tags, &namespace)
}

fn semantic_resolve_namespace_alias_name(tags: &[Value], namespace: &str) -> String {
    let Some(alias) = find_semantic_type_raw(tags, namespace) else {
        return namespace.to_string();
    };
    semantic_tag_attr(&alias, ":namespace-alias")
        .and_then(|value| string_text(&value).ok())
        .unwrap_or_else(|| namespace.to_string())
}

fn find_semantic_type_chain_anywhere(tags: &[Value], parts: &[String]) -> Option<Value> {
    for tag in tags {
        if let Some(found) = find_semantic_type_chain_in(tags, std::slice::from_ref(tag), parts) {
            return Some(found);
        }
        if let Some(found) = find_semantic_type_chain_anywhere(&semantic_tag_members(tag), parts) {
            return Some(found);
        }
    }
    None
}

fn find_semantic_type_by_parts(tags: &[Value], parts: &[String]) -> Option<Value> {
    let (first, rest) = parts.split_first()?;
    let mut best = None;
    let mut best_score = 0usize;
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(first)
        {
            if rest.is_empty() {
                let resolved = resolve_semantic_typedef(tags, tag);
                let score = semantic_type_resolution_score(&resolved);
                if best.is_none() || score > best_score {
                    best_score = score;
                    best = Some(resolved);
                }
                continue;
            }
            if let Some(found) = find_semantic_type_by_parts(&semantic_tag_members(tag), rest) {
                let score = semantic_type_resolution_score(&found);
                if best.is_none() || score > best_score {
                    best_score = score;
                    best = Some(found);
                }
            }
        }
    }
    best
}

fn find_semantic_variable_deep(tags: &[Value], name: &str) -> Option<Value> {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("variable")
            && semantic_tag_name(tag).as_deref() == Some(name)
        {
            return Some(tag.clone());
        }
        if let Some(found) = find_semantic_variable_deep(&semantic_tag_members(tag), name) {
            return Some(found);
        }
    }
    None
}

fn semantic_type_member_named(type_tag: &Value, name: &str) -> Option<Value> {
    semantic_tag_members(type_tag)
        .into_iter()
        .find(|member| semantic_tag_name(member).as_deref() == Some(name))
}

fn semantic_type_member_named_in_named_types(
    tags: &[Value],
    type_name: &str,
    member_name: &str,
) -> Option<Value> {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(type_name)
            && let Some(member) = semantic_type_member_named(tag, member_name)
        {
            return Some(member);
        }
    }
    None
}

fn collect_semantic_named_completion_tags(tags: &[Value], prefix: &str, matches: &mut Vec<Value>) {
    for tag in tags {
        if matches!(
            semantic_tag_class(tag).as_deref(),
            Some("function" | "variable" | "type")
        ) && semantic_tag_name(tag)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
        collect_semantic_named_completion_tags(&semantic_tag_members(tag), prefix, matches);
    }
}

fn collect_semantic_external_variable_completion_tags(
    tags: &[Value],
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("variable")
            && semantic_tag_name(tag)
                .as_deref()
                .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_attr(tag, ":type")
                .and_then(|value| string_text(&value).ok())
                .as_deref()
                == Some("namespace")
        {
            collect_semantic_external_variable_completion_tags(
                &semantic_tag_members(tag),
                prefix,
                matches,
            );
        }
    }
}

fn collect_semantic_qualified_namespace_completion_tags(
    tags: &[Value],
    namespace: &str,
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    if let Some(namespace_tag) = semantic_namespace_type_from_name(tags, namespace) {
        let mut seen = Vec::new();
        collect_semantic_namespace_member_completion_tags(
            tags,
            &namespace_tag,
            prefix,
            &mut seen,
            matches,
        );
    }
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(namespace)
        {
            for member in semantic_tag_members(tag) {
                if matches!(
                    semantic_tag_class(&member).as_deref(),
                    Some("function" | "variable" | "type")
                ) && semantic_tag_name(&member)
                    .as_deref()
                    .is_some_and(|name| name.starts_with(prefix))
                {
                    matches.push(member);
                }
            }
        }
        if semantic_tag_class(tag).as_deref() == Some("variable")
            && semantic_tag_name(tag)
                .as_deref()
                .is_some_and(|name| name.starts_with(prefix))
            && semantic_tag_attr(tag, ":type")
                .and_then(|value| semantic_type_name_parts(&value).ok())
                .and_then(|parts| parts.first().cloned())
                .as_deref()
                == Some(namespace)
        {
            matches.push(tag.clone());
        }
        collect_semantic_qualified_namespace_completion_tags(
            &semantic_tag_members(tag),
            namespace,
            prefix,
            matches,
        );
    }
}

fn collect_semantic_using_namespace_completion_tags(
    interp: &Interpreter,
    tags: &[Value],
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    for namespace in semantic_cpp_active_using_namespaces(interp, tags)
        .into_iter()
        .rev()
    {
        let Some(namespace_tag) = semantic_namespace_type_from_name(tags, &namespace) else {
            continue;
        };
        let mut seen = Vec::new();
        collect_semantic_namespace_member_completion_tags(
            tags,
            &namespace_tag,
            prefix,
            &mut seen,
            matches,
        );
    }
}

fn collect_semantic_namespace_member_completion_tags(
    tags: &[Value],
    namespace_tag: &Value,
    prefix: &str,
    seen: &mut Vec<String>,
    matches: &mut Vec<Value>,
) {
    let Some(namespace_name) = semantic_tag_name(namespace_tag) else {
        return;
    };
    if seen.iter().any(|seen| seen == &namespace_name) {
        return;
    }
    seen.push(namespace_name.clone());
    let members = semantic_tag_members(namespace_tag);
    for member in &members {
        if matches!(
            semantic_tag_class(member).as_deref(),
            Some("function" | "variable" | "type")
        ) && semantic_tag_name(member)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(member.clone());
        }
    }
    for member in members {
        if semantic_tag_class(&member).as_deref() != Some("using") {
            continue;
        }
        let Some(namespace) =
            semantic_tag_attr(&member, ":namespace").and_then(|value| string_text(&value).ok())
        else {
            continue;
        };
        let namespace = semantic_qualify_namespace(tags, &namespace_name, &namespace);
        if let Some(imported) = semantic_namespace_type_from_name(tags, &namespace) {
            collect_semantic_namespace_member_completion_tags(
                tags, &imported, prefix, seen, matches,
            );
        }
    }
}

fn semantic_cpp_active_using_namespaces(interp: &Interpreter, tags: &[Value]) -> Vec<String> {
    let Some(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()
    else {
        return Vec::new();
    };
    let cleaned = strip_cpp_comments(&text);
    let mut active: Vec<(usize, String)> = Vec::new();
    let mut depth = 0usize;
    let mut statement = String::new();
    for ch in cleaned.chars() {
        match ch {
            '{' => {
                collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
                statement.clear();
                depth += 1;
            }
            '}' => {
                collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
                statement.clear();
                depth = depth.saturating_sub(1);
                active.retain(|(using_depth, _)| *using_depth <= depth);
            }
            ';' => {
                collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
                statement.clear();
            }
            _ => statement.push(ch),
        }
    }
    collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
    active.into_iter().map(|(_, namespace)| namespace).collect()
}

fn collect_cpp_using_namespace_segment(
    statement: &str,
    depth: usize,
    tags: &[Value],
    active: &mut Vec<(usize, String)>,
) {
    let statement = statement.trim();
    let Some(namespace) = statement.strip_prefix("using namespace ") else {
        return;
    };
    let namespace = namespace
        .trim()
        .trim_end_matches(';')
        .trim_end_matches(|ch: char| !is_ident_byte(ch as u8) && ch != ':');
    if namespace.is_empty() {
        return;
    }
    let namespace = semantic_qualify_namespace_from_active(tags, namespace, active);
    active.push((depth, namespace));
}

fn semantic_qualify_namespace_from_active(
    tags: &[Value],
    namespace: &str,
    active: &[(usize, String)],
) -> String {
    if namespace.contains("::") || semantic_namespace_type_from_name(tags, namespace).is_some() {
        return namespace.to_string();
    }
    for (_, active_namespace) in active.iter().rev() {
        let qualified = format!("{active_namespace}::{namespace}");
        if semantic_namespace_type_from_name(tags, &qualified).is_some() {
            return qualified;
        }
    }
    namespace.to_string()
}

fn semantic_qualify_namespace(tags: &[Value], parent: &str, namespace: &str) -> String {
    if namespace.contains("::") || semantic_namespace_type_from_name(tags, namespace).is_some() {
        namespace.to_string()
    } else {
        let qualified = format!("{parent}::{namespace}");
        if semantic_namespace_type_from_name(tags, &qualified).is_some() {
            qualified
        } else {
            namespace.to_string()
        }
    }
}

fn unique_semantic_completion_tags(tags: Vec<Value>) -> Vec<Value> {
    let mut names = Vec::new();
    let mut unique = Vec::new();
    for tag in tags {
        let Some(name) = semantic_tag_name(&tag) else {
            continue;
        };
        if names.iter().any(|existing| existing == &name) {
            continue;
        }
        names.push(name);
        unique.push(tag);
    }
    unique
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MemberVisibility {
    Public,
    PublicProtected,
    All,
    None,
}

fn collect_semantic_member_completion_tags(
    type_tag: &Value,
    root_tags: &[Value],
    prefix: &str,
    visibility: MemberVisibility,
    seen: &mut Vec<String>,
    matches: &mut Vec<Value>,
) {
    if visibility == MemberVisibility::None {
        return;
    }
    let mut completion_type = type_tag.clone();
    if !root_tags.is_empty()
        && semantic_tag_members(&completion_type).is_empty()
        && let Some(name) = semantic_tag_name(&completion_type)
        && let Some(found) = semantic_type_from_name(root_tags, &name)
        && !semantic_tag_members(&found).is_empty()
    {
        completion_type = found;
    }
    let type_name = semantic_tag_name(&completion_type);
    if let Some(type_name) = &type_name {
        if seen.iter().any(|seen| seen == type_name) {
            return;
        }
        seen.push(type_name.clone());
    }
    let members = semantic_tag_members(&completion_type);
    let has_access_labels = members.iter().any(|member| {
        semantic_tag_class(member).as_deref() == Some("label")
            && semantic_tag_name(member)
                .as_deref()
                .is_some_and(|name| matches!(name, "public" | "protected" | "private"))
    });
    let mut access = if has_access_labels
        && semantic_tag_attr(&completion_type, ":type")
            .and_then(|value| string_text(&value).ok())
            .as_deref()
            == Some("class")
    {
        "private"
    } else {
        "public"
    };
    for member in members {
        let class = semantic_tag_class(&member);
        if class.as_deref() == Some("label") {
            if let Some(label) = semantic_tag_name(&member)
                && matches!(label.as_str(), "public" | "protected" | "private")
            {
                access = match label.as_str() {
                    "public" => "public",
                    "protected" => "protected",
                    _ => "private",
                };
            }
            continue;
        }
        let member_access = semantic_member_access(&member).unwrap_or(access);
        if class.as_deref() != Some("type")
            && !member_visible_for_completion(member_access, visibility)
            || semantic_tag_has_typemodifier(&member, "private")
                && visibility != MemberVisibility::All
        {
            continue;
        }
        if semantic_tag_attr(&member, ":constructor-flag").is_some_and(|value| value.is_truthy()) {
            continue;
        }
        if !matches!(class.as_deref(), Some("function" | "variable" | "type")) {
            continue;
        }
        let Some(name) = semantic_tag_name(&member) else {
            continue;
        };
        if !name.starts_with(prefix)
            || type_name.as_deref() == Some(name.as_str())
            || name.starts_with('~')
        {
            continue;
        }
        matches.push(member);
    }
    if root_tags.is_empty() {
        return;
    }
    let superclasses = semantic_tag_attr(&completion_type, ":superclasses")
        .and_then(|superclasses| superclasses.to_vec().ok())
        .or_else(|| {
            type_name
                .as_deref()
                .and_then(|name| semantic_type_from_name(root_tags, name))
                .and_then(|tag| semantic_tag_attr(&tag, ":superclasses"))
                .and_then(|superclasses| superclasses.to_vec().ok())
        });
    if let Some(superclasses) = superclasses {
        for superclass in superclasses {
            let Some(super_type) = semantic_type_candidate(root_tags, &superclass) else {
                continue;
            };
            let inherited_visibility = inherited_member_visibility(&superclass, visibility);
            collect_semantic_member_completion_tags(
                &super_type,
                root_tags,
                prefix,
                inherited_visibility,
                seen,
                matches,
            );
        }
    }
}

fn member_visible_for_completion(access: &str, visibility: MemberVisibility) -> bool {
    match visibility {
        MemberVisibility::All => true,
        MemberVisibility::PublicProtected => matches!(access, "public" | "protected"),
        MemberVisibility::Public => access == "public",
        MemberVisibility::None => false,
    }
}

fn semantic_member_access(member: &Value) -> Option<&'static str> {
    if semantic_tag_has_typemodifier(member, "public") {
        Some("public")
    } else if semantic_tag_has_typemodifier(member, "protected") {
        Some("protected")
    } else if semantic_tag_has_typemodifier(member, "private") {
        Some("private")
    } else {
        None
    }
}

fn inherited_member_visibility(superclass: &Value, current: MemberVisibility) -> MemberVisibility {
    let access = semantic_tag_attr(superclass, ":inheritance")
        .and_then(|value| string_text(&value).ok())
        .unwrap_or_else(|| "private".into());
    match (current, access.as_str()) {
        (MemberVisibility::All, "public" | "protected") => MemberVisibility::PublicProtected,
        (MemberVisibility::All, "private") => MemberVisibility::Public,
        (MemberVisibility::Public, "public") => MemberVisibility::Public,
        _ => MemberVisibility::None,
    }
}

fn semanticdb_find_tags_by_class(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let class = args[0].as_symbol()?;
    let path = args.get(1).cloned().unwrap_or(Value::Nil);
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }

    let tables = semanticdb_search_tables(interp, Some(&path), env);

    let mut results = Vec::new();
    for table in tables {
        let tags = semantic_tags_for_search(interp, &table)?;
        let matches = tags
            .into_iter()
            .filter(|tag| semantic_tag_class(tag).as_deref() == Some(class))
            .collect::<Vec<_>>();
        if !matches.is_empty() {
            results.push(Value::cons(table, Value::list(matches)));
        }
    }

    Ok(Value::list(results))
}

fn semanticdb_find_tags_by_name(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let name = string_text(&args[0])?;
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }
    let tables = semanticdb_search_tables(interp, args.get(1), env);
    let mut results = Vec::new();
    for table in tables {
        let tags = semantic_tags_for_search(interp, &table)?;
        let matches = tags
            .into_iter()
            .filter(|tag| semantic_tag_name(tag).as_deref() == Some(name.as_str()))
            .collect::<Vec<_>>();
        if !matches.is_empty() {
            results.push(Value::cons(table, Value::list(matches)));
        }
    }
    Ok(Value::list(results))
}

fn semanticdb_find_tags_for_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let prefix = string_text(&args[0])?;
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }
    let tables = semanticdb_search_tables(interp, args.get(1), env);
    let mut results = Vec::new();
    for table in tables {
        let tags = semantic_tags_for_search(interp, &table)?;
        let matches = semantic_completion_matches(&tags, &prefix);
        if !matches.is_empty() {
            results.push(Value::cons(table, Value::list(matches)));
        }
    }
    Ok(Value::list(results))
}

fn semantic_fetch_tags_compat(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    // GNU keeps a per-buffer tag cache and re-fetching returns the SAME tag
    // objects while the buffer is unchanged (callers compare tags with `eq').
    let buffer_id = interp.current_buffer_id();
    let fingerprint = {
        use std::hash::{Hash, Hasher};
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        source.hash(&mut hasher);
        hasher.finish() as i64
    };
    if let Some(cache) = interp.buffer_local_value(buffer_id, "emaxx--semantic-tag-cache")
        && let Some((stored, tags_value)) = cache.cons_values()
        && stored == Value::Integer(fingerprint)
    {
        interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
        return Ok(tags_value);
    }
    let mut cacheable = true;
    let tags = if let Some(path) = interp
        .buffer
        .file
        .clone()
        .map(PathBuf::from)
        .filter(|path| {
            matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("c" | "cc" | "cpp" | "cxx" | "h" | "hh" | "hpp" | "java")
            )
        }) {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_cpp_tags_at_path(&path, &source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        == Some("js")
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_javascript_tags(&source)
    } else if semantic_mode_derived_p(interp, env, "makefile-mode") {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_makefile_tags(&source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        == Some("py")
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_python_tags(&source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        == Some("srt")
        || interp
            .lookup_var("major-mode", env)
            .is_some_and(|mode| mode == Value::Symbol("srecode-template-mode".into()))
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_srecode_template_tags(&source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext, "html" | "htm"))
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_html_tags(&source)
    } else {
        cacheable = false;
        interp
            .lookup_var("semanticdb-current-table", env)
            .and_then(|table| eieio_slot_value(interp, &table, "tags").ok())
            .and_then(|tags| tags.to_vec().ok())
            .unwrap_or_default()
    };

    let result = Value::list(tags);
    if let Some(table) = interp
        .lookup_var("semanticdb-current-table", env)
        .filter(|table| !table.is_nil())
    {
        let _ = set_eieio_slot_value(interp, &table, "tags", result.clone());
    }
    if cacheable {
        interp.set_buffer_local_value(
            buffer_id,
            "emaxx--semantic-tag-cache",
            Value::cons(Value::Integer(fingerprint), result.clone()),
        );
    }
    interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
    Ok(result)
}

fn parse_semantic_html_tags(source: &str) -> Vec<Value> {
    let mut tags = Vec::new();
    let mut rest = source;
    while let Some(open_index) = rest.find("<h") {
        rest = &rest[open_index + 2..];
        let Some(level_end) = rest.find('>') else {
            break;
        };
        if !rest[..level_end].chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }
        rest = &rest[level_end + 1..];
        let Some(close_index) = rest.find("</h") else {
            break;
        };
        let title = strip_html_tags(&rest[..close_index]).trim().to_string();
        if !title.is_empty() {
            let child = semantic_tag(&title, "section", Value::Nil);
            if let Some(tag) = semantic_type_tag(&title, vec![(":members", Value::list([child]))]) {
                tags.push(reclass_semantic_tag(tag, "section"));
            }
        }
        rest = &rest[close_index + 3..];
    }
    tags
}

fn parse_semantic_javascript_tags(source: &str) -> Vec<Value> {
    source
        .lines()
        .filter_map(|line| {
            let line = line.split("//").next().unwrap_or(line).trim();
            let rest = line.strip_prefix("function ")?;
            let open = rest.find('(')?;
            let close = rest[open + 1..].find(')')? + open + 1;
            let name = rest[..open].trim();
            if name.is_empty() {
                return None;
            }
            let arguments = rest[open + 1..close]
                .split(',')
                .filter_map(|arg| {
                    let arg = arg.trim();
                    (!arg.is_empty()).then(|| semantic_tag(arg, "variable", Value::Nil))
                })
                .collect::<Vec<_>>();
            let mut attrs = Vec::new();
            if !arguments.is_empty() {
                attrs.push((":arguments", Value::list(arguments)));
            }
            semantic_function_tag(name, attrs)
        })
        .collect()
}

fn parse_semantic_makefile_tags(source: &str) -> Vec<Value> {
    source
        .lines()
        .filter_map(|line| {
            if line.starts_with(['\t', ' ']) {
                return None;
            }
            let line = line.split('#').next().unwrap_or(line).trim();
            if line.is_empty() || line.contains('=') {
                return None;
            }
            let (target, dependencies) = line.split_once(':')?;
            let target = target.trim();
            if target.is_empty() {
                return None;
            }
            let arguments = dependencies
                .split_whitespace()
                .filter(|dependency| !dependency.is_empty())
                .map(|dependency| Value::String(dependency.into()))
                .collect::<Vec<_>>();
            let mut attrs = Vec::new();
            if !arguments.is_empty() {
                attrs.push((":arguments", Value::list(arguments)));
            }
            semantic_function_tag(target, attrs)
        })
        .collect()
}

fn parse_semantic_python_tags(source: &str) -> Vec<Value> {
    source
        .lines()
        .filter_map(|line| {
            let line = line.split('#').next().unwrap_or(line).trim();
            let rest = line.strip_prefix("def ")?;
            let open = rest.find('(')?;
            let close = rest[open + 1..].find(')')? + open + 1;
            if rest[close + 1..].trim() != ":" {
                return None;
            }
            let name = rest[..open].trim();
            if name.is_empty() {
                return None;
            }
            let arguments = rest[open + 1..close]
                .split(',')
                .filter_map(|arg| {
                    let arg = arg.trim();
                    (!arg.is_empty()).then(|| semantic_tag(arg, "variable", Value::Nil))
                })
                .collect::<Vec<_>>();
            let mut attrs = Vec::new();
            if !arguments.is_empty() {
                attrs.push((":arguments", Value::list(arguments)));
            }
            semantic_function_tag(name, attrs)
        })
        .collect()
}

fn parse_semantic_srecode_template_tags(source: &str) -> Vec<Value> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut tags = Vec::new();
    let mut pending_dictionaries = Vec::new();
    let mut variables = std::collections::HashMap::new();
    let mut index = 0usize;
    while index < lines.len() {
        let line = lines[index].trim();
        if line.is_empty() || line.starts_with(';') {
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("set ") {
            let mut parts = rest.splitn(2, char::is_whitespace);
            if let (Some(name), Some(value)) = (parts.next(), parts.next())
                && let Some(value) = parse_srecode_value_resolving(value.trim(), &variables)
            {
                remember_srecode_string_value(&mut variables, name, &value);
                tags.push(semantic_srecode_variable_tag(name, value));
            }
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("sectiondictionary ") {
            let name = parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
            let mut entries = vec![Value::String(name.into())];
            let mut dictionary_vars = std::collections::HashMap::new();
            index += 1;
            while index < lines.len() {
                let entry_line = lines[index].trim();
                if entry_line.is_empty() || entry_line.starts_with(';') {
                    index += 1;
                    continue;
                }
                if entry_line.starts_with("sectiondictionary ")
                    || entry_line.starts_with("template ")
                    || entry_line.starts_with("context ")
                {
                    break;
                }
                if let Some(rest) = entry_line.strip_prefix("set ") {
                    let mut parts = rest.splitn(2, char::is_whitespace);
                    if let (Some(name), Some(value)) = (parts.next(), parts.next())
                        && let Some(value) =
                            parse_srecode_value_resolving(value.trim(), &dictionary_vars)
                    {
                        remember_srecode_string_value(&mut dictionary_vars, name, &value);
                        entries.push(semantic_srecode_variable_tag(name, value));
                    }
                }
                index += 1;
            }
            pending_dictionaries.push(Value::list(entries));
            continue;
        }
        if let Some(name) = line.strip_prefix("context ").map(str::trim)
            && !name.is_empty()
        {
            tags.push(semantic_tag(name, "context", Value::Nil));
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("template ") {
            let parts = rest.split_whitespace().collect::<Vec<_>>();
            if let Some(name) = parts.first() {
                let args = parts[1..]
                    .iter()
                    .map(|arg| Value::String((*arg).into()))
                    .collect::<Vec<_>>();
                let mut code = String::new();
                let mut scan = index + 1;
                let mut template_dictionaries = std::mem::take(&mut pending_dictionaries);
                while scan < lines.len() && lines[scan].trim() != "----" {
                    let header_line = lines[scan].trim();
                    if let Some(rest) = header_line.strip_prefix("sectiondictionary ") {
                        let dict_name =
                            parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
                        let mut entries = vec![Value::String(dict_name.into())];
                        let mut dictionary_vars = std::collections::HashMap::new();
                        scan += 1;
                        while scan < lines.len() {
                            let entry_line = lines[scan].trim();
                            if entry_line.is_empty() || entry_line.starts_with(';') {
                                scan += 1;
                                continue;
                            }
                            if entry_line == "----"
                                || entry_line.starts_with("sectiondictionary ")
                                || entry_line.starts_with("template ")
                                || entry_line.starts_with("context ")
                            {
                                break;
                            }
                            if let Some(rest) = entry_line.strip_prefix("set ") {
                                let mut parts = rest.splitn(2, char::is_whitespace);
                                if let (Some(name), Some(value)) = (parts.next(), parts.next())
                                    && let Some(value) = parse_srecode_value_resolving(
                                        value.trim(),
                                        &dictionary_vars,
                                    )
                                {
                                    remember_srecode_string_value(
                                        &mut dictionary_vars,
                                        name,
                                        &value,
                                    );
                                    entries.push(semantic_srecode_variable_tag(name, value));
                                }
                            }
                            scan += 1;
                        }
                        template_dictionaries.push(Value::list(entries));
                        continue;
                    }
                    if let Some(rest) = header_line.strip_prefix("section ") {
                        let section_name =
                            parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
                        scan += 1;
                        template_dictionaries.push(parse_srecode_section_dictionary(
                            &lines,
                            &mut scan,
                            section_name,
                        ));
                        continue;
                    }
                    scan += 1;
                }
                if scan < lines.len() {
                    scan += 1;
                    let start = scan;
                    while scan < lines.len() && lines[scan].trim() != "----" {
                        scan += 1;
                    }
                    code = lines[start..scan].join("\n");
                    if !code.is_empty() {
                        code.push('\n');
                    }
                }
                let mut attrs = vec![(":code", Value::String(code.into()))];
                if !args.is_empty() {
                    attrs.push((":arguments", Value::list(args)));
                }
                if !template_dictionaries.is_empty() {
                    attrs.push((":dictionaries", Value::list(template_dictionaries)));
                }
                tags.push(semantic_tag(name, "function", semantic_plist(attrs)));
                index = scan.saturating_add(1);
                continue;
            }
        }
        index += 1;
    }
    tags
}

fn parse_srecode_string(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        Some(value[1..value.len() - 1].to_string())
    } else {
        value.split_whitespace().next().map(str::to_string)
    }
}

fn parse_srecode_value(value: &str) -> Option<Vec<Value>> {
    let mut parts = Vec::new();
    let mut rest = value.trim();
    while !rest.is_empty() {
        if let Some(after_quote) = rest.strip_prefix('"') {
            let Some(end) = after_quote.find('"') else {
                break;
            };
            parts.push(Value::String(after_quote[..end].to_string().into()));
            rest = after_quote[end + 1..].trim_start();
        } else if let Some(after_macro) = rest.strip_prefix("macro") {
            let after_macro = after_macro.trim_start();
            if let Some(stripped) = after_macro.strip_prefix('"')
                && let Some(end) = stripped.find('"')
            {
                parts.push(Value::cons(
                    Value::Symbol("macro".into()),
                    Value::String(stripped[..end].to_string().into()),
                ));
                rest = stripped[end + 1..].trim_start();
            } else {
                let Some(name) = after_macro.split_whitespace().next() else {
                    break;
                };
                parts.push(Value::cons(
                    Value::Symbol("macro".into()),
                    Value::String(name.to_string().into()),
                ));
                rest = after_macro
                    .split_once(char::is_whitespace)
                    .map(|(_, tail)| tail.trim_start())
                    .unwrap_or("");
            }
        } else {
            break;
        }
    }

    if parts.is_empty() {
        parse_srecode_string(value).map(|value| vec![Value::String(value.into())])
    } else {
        Some(parts)
    }
}

fn parse_srecode_value_resolving(
    value: &str,
    variables: &std::collections::HashMap<String, String>,
) -> Option<Vec<Value>> {
    let parts = parse_srecode_value(value)?;
    let mut resolved = String::new();
    for part in &parts {
        match part {
            Value::String(text) => resolved.push_str(text),
            Value::Cons(_) => {
                let (Value::Symbol(kind), Value::String(name)) = part.cons_values()? else {
                    return Some(parts);
                };
                if kind != "macro" {
                    return Some(parts);
                }
                let Some(value) = variables.get(name.as_str()) else {
                    return Some(parts);
                };
                resolved.push_str(value);
            }
            _ => return Some(parts),
        }
    }
    if parts.len() == 1 && matches!(parts.first(), Some(Value::String(_))) {
        Some(parts)
    } else {
        Some(vec![Value::String(resolved.into())])
    }
}

fn parse_srecode_section_dictionary(lines: &[&str], scan: &mut usize, name: String) -> Value {
    let mut entries = vec![Value::String(name.into())];
    let mut variables = std::collections::HashMap::new();
    while *scan < lines.len() {
        let line = lines[*scan].trim();
        if line.is_empty() || line.starts_with(';') {
            *scan += 1;
            continue;
        }
        if line == "end" {
            *scan += 1;
            break;
        }
        if let Some(rest) = line.strip_prefix("show ") {
            let name = parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
            entries.push(Value::list([Value::String(name.into())]));
            *scan += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("section ") {
            let name = parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
            *scan += 1;
            entries.push(parse_srecode_section_dictionary(lines, scan, name));
            continue;
        }
        if let Some(rest) = line.strip_prefix("set ") {
            let mut parts = rest.splitn(2, char::is_whitespace);
            if let (Some(name), Some(value)) = (parts.next(), parts.next())
                && let Some(value) = parse_srecode_value_resolving(value.trim(), &variables)
            {
                remember_srecode_string_value(&mut variables, name, &value);
                entries.push(semantic_srecode_variable_tag(name, value));
            }
        }
        *scan += 1;
    }
    Value::list(entries)
}

fn remember_srecode_string_value(
    variables: &mut std::collections::HashMap<String, String>,
    name: &str,
    value: &[Value],
) {
    if let [Value::String(text)] = value {
        variables.insert(name.to_string(), text.to_string());
    }
}

fn semantic_srecode_variable_tag(name: &str, value: Vec<Value>) -> Value {
    semantic_tag(
        name,
        "variable",
        semantic_plist(vec![(":default-value", Value::list(value))]),
    )
}

// GNU oclosure.el errors at compile time when a form setqs an oclosure
// slot that is not declared :mutable ("Slot fst should not be mutated"),
// including from lambdas nested in the oclosure body.
fn check_oclosure_slot_mutation(interp: &Interpreter, form: &Value) -> Result<(), LispError> {
    let Ok(items) = form.to_vec() else {
        return Ok(());
    };
    if matches!(items.first(), Some(Value::Symbol(head)) if head == "quote") {
        return Ok(());
    }
    if matches!(items.first(), Some(Value::Symbol(head)) if head == "oclosure-lambda")
        && let Some(spec) = items.get(1)
        && let Ok(spec_items) = spec.to_vec()
        && let Some(type_name) = spec_items
            .first()
            .and_then(|value| value.as_symbol().ok().map(String::from))
    {
        let collect = |property: &str| -> Vec<String> {
            let mut names = Vec::new();
            let mut current = Some(type_name.clone());
            while let Some(current_type) = current {
                names.extend(
                    interp
                        .get_symbol_property(&current_type, property)
                        .and_then(|value| value.to_vec().ok())
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|value| value.as_symbol().ok().map(String::from)),
                );
                current = interp
                    .get_symbol_property(&current_type, "emaxx-oclosure-parent")
                    .and_then(|value| value.as_symbol().ok().map(String::from));
            }
            names
        };
        let mutable = collect("emaxx-oclosure-mutable-slots");
        let immutable: Vec<String> = collect("emaxx-oclosure-slots")
            .into_iter()
            .filter(|slot| !mutable.contains(slot))
            .collect();
        for body_form in items.get(3..).unwrap_or(&[]) {
            check_setq_of_slots(body_form, &immutable)?;
        }
    }
    for sub in &items {
        check_oclosure_slot_mutation(interp, sub)?;
    }
    Ok(())
}

fn check_setq_of_slots(form: &Value, immutable: &[String]) -> Result<(), LispError> {
    let Ok(items) = form.to_vec() else {
        return Ok(());
    };
    if matches!(items.first(), Some(Value::Symbol(head)) if head == "quote") {
        return Ok(());
    }
    if matches!(items.first(), Some(Value::Symbol(head)) if head == "setq")
        && let Some(Value::Symbol(target)) = items.get(1)
        && immutable.iter().any(|slot| slot == target)
    {
        return Err(LispError::Signal(format!(
            "Slot {target} should not be mutated"
        )));
    }
    for sub in &items {
        check_setq_of_slots(sub, immutable)?;
    }
    Ok(())
}

pub(crate) fn oclosure_value_matches_type(
    interp: &Interpreter,
    value: &Value,
    target: &str,
) -> bool {
    let Some(mut current) = oclosure_type_of(value) else {
        return false;
    };
    // Every oclosure is an `oclosure' (the abstract root type).
    if target == "oclosure" {
        return true;
    }
    loop {
        if current == target {
            return true;
        }
        match interp
            .get_symbol_property(&current, "emaxx-oclosure-parent")
            .and_then(|value| value.as_symbol().ok().map(String::from))
        {
            Some(parent) => current = parent,
            None => return false,
        }
    }
}

// Call NAME's elisp definition (GNU nadvice.el's advice-add and friends)
// when one is loaded, so direct primitive dispatch (define-advice's
// lowering) reaches it too.  None -> no elisp definition; use the native
// file-less fallback.
fn delegate_to_lisp_function(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    if !interp.has_lisp_function(name) {
        return Ok(None);
    }
    let Ok(function) = interp.lookup_function(name, env) else {
        return Ok(None);
    };
    if matches!(&function, Value::BuiltinFunc(builtin) if builtin == name) {
        return Ok(None);
    }
    interp
        .call_function_value(function, Some(name), args, env)
        .map(Some)
}

pub(crate) fn oclosure_type_of(value: &Value) -> Option<String> {
    let Value::Lambda(lambda) = value else {
        return None;
    };
    // Real oclosures carry the oclosure marker as their first executable
    // body form; closures that merely captured an oclosure's frames don't.
    let first = lambda
        .body
        .iter()
        .find(|form| !matches!(form, Value::String(_) | Value::StringObject(_)))?;
    if !matches!(first, Value::Symbol(marker) if marker == ":closure-oclosure") {
        return None;
    }
    let contents = lambda.env.borrow();
    contents.iter().rev().find_map(|frame| {
        frame
            .iter()
            .find(|(key, _)| key == crate::lisp::eval::OCLOSURE_TYPE_MARKER)
            .and_then(|(_, value)| value.as_symbol().ok().map(String::from))
    })
}

fn value_is_cl_struct_record(interp: &Interpreter, value: &Value) -> bool {
    let Value::Record(id) = value else {
        return false;
    };
    let Some(record) = interp.find_record(*id) else {
        return false;
    };
    interp
        .get_symbol_property(&record.type_name, "emaxx-struct-slots")
        .is_some()
}

fn cl_typep_matches(
    interp: &mut Interpreter,
    env: &mut Env,
    value: &Value,
    type_spec: &Value,
) -> Result<bool, LispError> {
    if let Ok(items) = type_spec.to_vec()
        && let Some(Value::Symbol(operator)) = items.first()
    {
        if operator == "or" {
            for choice in &items[1..] {
                if cl_typep_matches(interp, env, value, choice)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        if operator == "and" {
            for choice in &items[1..] {
                if !cl_typep_matches(interp, env, value, choice)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        if operator == "not" && items.len() == 2 {
            return Ok(!cl_typep_matches(interp, env, value, &items[1])?);
        }
        if operator == "eql" && items.len() == 2 {
            return Ok(crate::lisp::primitives::values_eql(value, &items[1]));
        }
        if operator == "member" {
            return Ok(items[1..]
                .iter()
                .any(|member| crate::lisp::primitives::values_eql(value, member)));
        }
        if matches!(operator.as_str(), "integer" | "float" | "number" | "real") {
            // GNU range types: (integer LOW HIGH) with `*' for unbounded
            // and (N) for an exclusive bound.
            if !cl_typep_matches(interp, env, value, &Value::Symbol(operator.clone()))? {
                return Ok(false);
            }
            let candidate = match value {
                Value::Integer(n) => *n as f64,
                Value::Float(f) => *f,
                _ => return Ok(false),
            };
            let bound_value = |bound: &Value| -> Option<(f64, bool)> {
                match bound {
                    Value::Integer(n) => Some((*n as f64, false)),
                    Value::Float(f) => Some((*f, false)),
                    Value::Cons(_) => match bound.to_vec().ok()?.first()? {
                        Value::Integer(n) => Some((*n as f64, true)),
                        Value::Float(f) => Some((*f, true)),
                        _ => None,
                    },
                    _ => None,
                }
            };
            if let Some(low) = items.get(1)
                && let Some((limit, exclusive)) = bound_value(low)
                && if exclusive {
                    candidate <= limit
                } else {
                    candidate < limit
                }
            {
                return Ok(false);
            }
            if let Some(high) = items.get(2)
                && let Some((limit, exclusive)) = bound_value(high)
                && if exclusive {
                    candidate >= limit
                } else {
                    candidate > limit
                }
            {
                return Ok(false);
            }
            return Ok(true);
        }
        if operator == "satisfies" && items.len() == 2 {
            let predicate = items[1].clone();
            return Ok(crate::lisp::primitives::call_function_value(
                interp,
                &predicate,
                std::slice::from_ref(value),
                env,
            )?
            .is_truthy());
        }
        if operator == "subclass" && items.len() == 2 {
            let Ok(target) = items[1].as_symbol() else {
                return Ok(false);
            };
            let Some(class_name) = interp.class_name_from_value(value) else {
                return Ok(false);
            };
            // GNU's subclass generalizer resolves an EIEIO autoload dummy
            // before matching.  `eieio--full-class-object' identifies that
            // state by its nil default-object cache, not by the presence of
            // any separate registry entry.
            if interp.class_is_autoload_stub(&class_name)
                && let Ok(fundef) = super::call(
                    interp,
                    "symbol-function",
                    &[Value::Symbol(class_name.clone().into())],
                    env,
                )
            {
                super::call(
                    interp,
                    "autoload-do-load",
                    &[fundef, Value::Symbol(class_name.clone().into())],
                    env,
                )?;
            }
            if interp.class_value(&class_name).is_none() {
                return Ok(false);
            }
            return Ok(interp
                .class_allparents(&class_name)
                .iter()
                .any(|parent| matches!(parent, Value::Symbol(parent) if parent == target)));
        }
        if let Some(expanded) = cl_deftype_expansion(interp, env, operator, &items[1..])? {
            return cl_typep_matches(interp, env, value, &expanded);
        }
    }

    let target = type_spec.as_symbol()?;
    if let Some(expanded) = cl_deftype_expansion(interp, env, target, &[])? {
        return cl_typep_matches(interp, env, value, &expanded);
    }
    if let Some(predicate) = interp.get_symbol_property(target, "cl-deftype-satisfies") {
        return Ok(crate::lisp::primitives::call_function_value(
            interp,
            &predicate,
            std::slice::from_ref(value),
            env,
        )?
        .is_truthy());
    }
    let actual = cl_type_name(interp, value)?;
    let matches = target == "t"
        // Reader vector/hash-table markers are conses structurally but
        // vectors/hash-tables semantically (cl-generic must not dispatch
        // their `list' methods on them).
        || (target == "list" && value.is_list() && !is_vector_like_value(interp, value))
        || (target == "eieio-object" && interp.value_is_eieio_object(value))
        // Every cl-defstruct inherits cl-structure-object in GNU.
        || (target == "cl-structure-object" && value_is_cl_struct_record(interp, value))
        // Oclosure types (nadvice's `advice' objects) dispatch by their
        // registered type and parent chain.
        || oclosure_value_matches_type(interp, value, target)
        || (target == "hash-table" && crate::lisp::json::is_hash_table(interp, value))
        || (target == "class"
            && interp
                .class_name_from_value(value)
                .is_some_and(|name| interp.class_value(&name).is_some()))
        || target == actual
        || (is_builtin_class_name(target)
            && interp
                .class_allparents(actual)
                .iter()
                .any(|parent| matches!(parent, Value::Symbol(parent) if parent == target)))
        || (!is_builtin_class_name(target) && interp.value_is_instance_of_class(value, target))
        || (target == "function"
            && matches!(
                actual,
                "primitive-function"
                    | "special-form"
                    | "interpreted-function"
                    | "byte-code-function"
            ));
    if !matches {
        // GNU cl-typep signals for type names it cannot resolve to a
        // class, deftype or satisfies-predicate ("Unknown type %S").
        let known = matches!(
            target,
            "t" | "nil"
                | "list"
                | "eieio-object"
                | "cl-structure-object"
                | "hash-table"
                | "class"
                | "function"
        ) || is_builtin_class_name(target)
            || interp.class_value(target).is_some()
            || interp
                .get_symbol_property(target, "cl-deftype-satisfies")
                .is_some()
            || interp.get_symbol_property(target, "cl--class").is_some()
            || interp
                .get_symbol_property(target, "emaxx-oclosure-parent")
                .is_some()
            || interp
                .get_symbol_property(target, "emaxx-oclosure-slots")
                .is_some()
            || interp
                .get_symbol_property(target, "emaxx-struct-slots")
                .is_some();
        if !known {
            return Err(LispError::Signal(format!("Unknown type {target}")));
        }
    }
    Ok(matches)
}

fn cl_deftype_expansion(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    args: &[Value],
) -> Result<Option<Value>, LispError> {
    let Some(handler) = interp.get_symbol_property(name, "cl-deftype-handler") else {
        return Ok(None);
    };
    let Value::Lambda(lambda) = &handler else {
        return Ok(None);
    };
    let call_args = cl_deftype_call_args(&lambda.params, args)?;
    interp
        .call_function_value(handler, Some(name), &call_args, env)
        .map(Some)
}

fn cl_deftype_call_args(params: &[String], args: &[Value]) -> Result<Vec<Value>, LispError> {
    let mut required = 0usize;
    let mut optional = 0usize;
    let mut rest = false;
    let mut in_optional = false;
    for param in params {
        match param.as_str() {
            "&optional" => in_optional = true,
            "&rest" => {
                rest = true;
                break;
            }
            _ if in_optional => optional += 1,
            _ => required += 1,
        }
    }
    if args.len() < required || (!rest && args.len() > required + optional) {
        return Err(LispError::WrongNumberOfArgs(
            "cl-deftype".into(),
            args.len(),
        ));
    }
    let mut call_args = args.to_vec();
    if !rest {
        while call_args.len() < required + optional {
            call_args.push(Value::Symbol("*".into()));
        }
    }
    Ok(call_args)
}

fn eieio_class_default_property(slot_name: &str) -> String {
    format!("emaxx-class-default:{slot_name}")
}

fn srecode_template_get_table(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let template_name = string_text(&args[1])?;
    let context = args.get(2).filter(|value| value.is_truthy()).cloned();
    let application = args.get(3).filter(|value| value.is_truthy()).cloned();
    srecode_template_get_from_record(
        interp,
        &args[0],
        &template_name,
        context.as_ref(),
        application.as_ref(),
        env,
    )
}

fn srecode_template_get_from_record(
    interp: &mut Interpreter,
    table: &Value,
    template_name: &str,
    context: Option<&Value>,
    application: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Value::Record(record_id) = table else {
        return Ok(Value::Nil);
    };
    let Some(record) = interp.find_record(*record_id) else {
        return Ok(Value::Nil);
    };
    match record.type_name.as_str() {
        "srecode-template-table" => {
            if !srecode_template_table_in_project(interp, table, env)? {
                return Ok(Value::Nil);
            }
            if let Some(context) = context {
                let contexthash = eieio_slot_value(interp, table, "contexthash")?;
                let ctx_table = hash_lookup(interp, &contexthash, context, env)?;
                if ctx_table.is_truthy() {
                    hash_lookup(
                        interp,
                        &ctx_table,
                        &Value::String(template_name.into()),
                        env,
                    )
                } else {
                    Ok(Value::Nil)
                }
            } else {
                let namehash = eieio_slot_value(interp, table, "namehash")?;
                hash_lookup(interp, &namehash, &Value::String(template_name.into()), env)
            }
        }
        "srecode-mode-table" => {
            let tables = eieio_slot_value(interp, table, "tables")?.to_vec()?;
            for candidate in &tables {
                let app = eieio_slot_value(interp, candidate, "application")?;
                let app_matches = match application {
                    Some(expected) => app == *expected,
                    None => !app.is_truthy(),
                };
                if app_matches {
                    let found = srecode_template_get_from_record(
                        interp,
                        candidate,
                        template_name,
                        context,
                        None,
                        env,
                    )?;
                    if found.is_truthy() {
                        return Ok(found);
                    }
                }
            }
            let mode = eieio_slot_value(interp, table, "major-mode")?;
            if mode != Value::Symbol("default".into())
                && let Some(default_table) = srecode_find_mode_table(interp, "default")?
            {
                return srecode_template_get_from_record(
                    interp,
                    &default_table,
                    template_name,
                    context,
                    application,
                    env,
                );
            }
            Ok(Value::Nil)
        }
        _ => Ok(Value::Nil),
    }
}

fn srecode_template_table_in_project(
    interp: &Interpreter,
    table: &Value,
    env: &Env,
) -> Result<bool, LispError> {
    let project = eieio_slot_value(interp, table, "project")?;
    if !project.is_truthy() {
        return Ok(true);
    }
    let project = string_text(&project)?;
    let default_directory = interp
        .lookup("default-directory", env)
        .ok()
        .and_then(|value| string_text(&value).ok())
        .unwrap_or_default();
    let project = project.trim_end_matches('/');
    Ok(!project.is_empty() && default_directory.starts_with(project))
}

fn srecode_find_mode_table(interp: &Interpreter, mode: &str) -> Result<Option<Value>, LispError> {
    let tables = interp
        .lookup("srecode-mode-table-list", &Vec::new())
        .unwrap_or(Value::Nil)
        .to_vec()
        .unwrap_or_default();
    for table in tables {
        if eieio_slot_value(interp, &table, "major-mode")? == Value::Symbol(mode.into()) {
            return Ok(Some(table));
        }
    }
    Ok(None)
}

fn hash_lookup(
    interp: &mut Interpreter,
    table: &Value,
    key: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some((test, entries)) = json::hash_table_entries(interp, table) else {
        return Ok(Value::Nil);
    };
    for (existing_key, value) in entries {
        if hash_table_key_matches(interp, table, &test, &existing_key, key, env)? {
            return Ok(value);
        }
    }
    Ok(Value::Nil)
}

fn strip_html_tags(text: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in text.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

fn reclass_semantic_tag(tag: Value, class: &str) -> Value {
    let Ok(mut items) = tag.to_vec() else {
        return tag;
    };
    if items.len() > 1 {
        items[1] = Value::Symbol(class.into());
    }
    Value::list(items)
}

fn semantic_current_tag_compat(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    // The parsed tree's innermost tag containing point is authoritative:
    // same-named tags (overloads, per-class prototypes) can only be told
    // apart by position, like GNU's overlay lookup.
    {
        let tags = semantic_fetch_tags_compat(interp, env)?
            .to_vec()
            .unwrap_or_default();
        let point = interp.buffer.point() as i64;
        let mut chain = Vec::new();
        semantic_containment_chain(&tags, point, &mut chain);
        if let Some(innermost) = semantic_innermost_with_parent(&chain) {
            interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
            return Ok(innermost);
        }
    }
    if let Some(tag) = semantic_current_function_tag_from_point(interp, env, &text)? {
        interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
        return Ok(tag);
    }
    if interp.buffer.point() != interp.buffer.point_min() {
        interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
        return Ok(Value::Nil);
    }
    let override_tag = interp
        .lookup_var("__emaxx-semantic-current-tag-override", env)
        .filter(Value::is_truthy)
        .unwrap_or(Value::Nil);
    Ok(override_tag)
}

fn semantic_current_function_tag_from_point(
    interp: &mut Interpreter,
    env: &mut Env,
    text: &str,
) -> Result<Option<Value>, LispError> {
    let point = interp
        .buffer
        .point()
        .saturating_sub(interp.buffer.point_min());
    let before = &text[..point.min(text.len())];
    let line_start = before.rfind('\n').map(|index| index + 1).unwrap_or(0);
    let line_end = text[point.min(text.len())..]
        .find('\n')
        .map(|index| point.min(text.len()) + index)
        .unwrap_or(text.len());
    let line = text[line_start..line_end]
        .split("//")
        .next()
        .unwrap_or("")
        .trim();
    // Prototypes may carry block comments in their parameter lists
    // (`char /* a */`); the tag keeps only the real tokens.
    let line = strip_cpp_line_block_comments(line);
    let current = parse_cpp_function(&line, false)
        .or_else(|| semantic_previous_cpp_function_line(&text[..line_start]));
    let Some(current) = current else {
        return Ok(None);
    };
    let table = interp
        .lookup_var("semanticdb-current-table", env)
        .unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(Some(current));
    }
    let mut tags = semantic_tags_for_search(interp, &table)?;
    extend_semantic_c_like_table_tags(interp, &table, &mut tags);
    Ok(find_equivalent_semantic_function(&tags, &current).or(Some(current)))
}

fn strip_cpp_line_block_comments(line: &str) -> String {
    let mut result = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(open) = rest.find("/*") {
        result.push_str(&rest[..open]);
        match rest[open + 2..].find("*/") {
            Some(close) => rest = &rest[open + 2 + close + 2..],
            None => return result,
        }
    }
    result.push_str(rest);
    result
}

fn semantic_previous_cpp_function_line(text: &str) -> Option<Value> {
    text.lines()
        .rev()
        .filter_map(|line| {
            let line = line.split("//").next().unwrap_or("").trim();
            (!line.is_empty()).then_some(line)
        })
        .take(5)
        .find_map(|line| parse_cpp_function(line, false))
}

fn find_equivalent_semantic_function(tags: &[Value], current: &Value) -> Option<Value> {
    let key = semantic_function_signature_key(current);
    let mut fallback = None;
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("function")
            && semantic_function_signature_matches(&semantic_function_signature_key(tag), &key)
        {
            if !semantic_tag_attr(tag, ":prototype-flag").is_some_and(|value| value.is_truthy()) {
                return Some(tag.clone());
            }
            fallback.get_or_insert_with(|| tag.clone());
        }
        if let Some(found) = find_equivalent_semantic_function(&semantic_tag_members(tag), current)
        {
            return Some(found);
        }
    }
    fallback
}

fn semanticdb_typecache_find(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }
    let names = semantic_type_name_parts(&args[0])?;
    if names.is_empty() {
        return Ok(Value::Nil);
    }

    for table in semanticdb_search_tables(interp, args.get(1), env) {
        let tags = semantic_tags_for_search(interp, &table)?;
        let found = if names.len() == 1 {
            find_semantic_type_deep(&tags, &names[0])
                .map(|tag| resolve_semantic_typedef(&tags, &tag))
        } else {
            find_semantic_type_chain(&tags, &names)
        };
        if let Some(found) = found {
            return Ok(found);
        }
    }
    Ok(Value::Nil)
}

fn semanticdb_search_tables(
    interp: &mut Interpreter,
    path: Option<&Value>,
    env: &mut Env,
) -> Vec<Value> {
    match path {
        Some(record @ Value::Record(_)) => vec![record.clone()],
        Some(Value::Nil) | None => {
            let mut tables = interp
                .lookup_var("semanticdb-current-table", env)
                .filter(|table| !table.is_nil())
                .into_iter()
                .collect::<Vec<_>>();
            if let Some(database) = interp.lookup_var("semanticdb-current-database", env)
                && let Ok(database_tables) = eieio_slot_value(interp, &database, "tables")
                && let Ok(database_tables) = database_tables.to_vec()
            {
                for table in database_tables
                    .into_iter()
                    .filter(|table| matches!(table, Value::Record(_)))
                {
                    if !tables.contains(&table) {
                        tables.push(table);
                    }
                }
            }
            tables
        }
        _ => Vec::new(),
    }
}

fn semantic_type_name_parts(value: &Value) -> Result<Vec<String>, LispError> {
    if let Ok(symbol) = value.as_symbol() {
        return Ok(vec![symbol.to_string()]);
    }
    if let Ok(name) = string_text(value) {
        return Ok(name
            .split("::")
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect());
    }
    let Ok(items) = value.to_vec() else {
        return Ok(Vec::new());
    };
    if matches!(items.get(1), Some(Value::Symbol(class)) if class == "type")
        && let Some(name) = items.first()
    {
        return semantic_type_name_parts(name);
    }
    Ok(items
        .into_iter()
        .filter_map(|part| {
            part.as_symbol()
                .map(str::to_string)
                .or_else(|_| string_text(&part))
                .ok()
        })
        .collect())
}

fn semantic_tags_for_search(
    interp: &mut Interpreter,
    table: &Value,
) -> Result<Vec<Value>, LispError> {
    let tags = match eieio_slot_value(interp, table, "tags") {
        Ok(tags) => tags,
        Err(_) => return Ok(Vec::new()),
    };
    let mut tags = match tags.to_vec() {
        Ok(tags) => tags,
        Err(LispError::TypeError(expected, _)) if expected == "list" => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let include_tags = tags
        .iter()
        .filter(|tag| semantic_tag_class(tag).as_deref() == Some("include"))
        .cloned()
        .collect::<Vec<_>>();
    for include_tag in include_tags {
        let Some(path) = semantic_include_path(interp, table, &include_tag) else {
            continue;
        };
        tags.extend(annotate_semantic_tags_with_filename(
            cached_semantic_cpp_tags(&path),
            &path.to_string_lossy(),
        ));
    }
    Ok(tags)
}

fn extend_semantic_c_like_table_tags(
    interp: &mut Interpreter,
    table: &Value,
    tags: &mut Vec<Value>,
) {
    if let Some(path) = semantic_table_file_path(interp, table)
        && matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("c" | "cc" | "cpp" | "cxx" | "h" | "hh" | "hpp" | "java")
        )
    {
        let Some(base) = semantic_table_base_directory(interp, &path) else {
            return;
        };
        let parse_path = if path.is_absolute() {
            path.clone()
        } else {
            base.join(&path)
        };
        let parsed = cached_semantic_cpp_tags(&parse_path);
        append_semantic_search_tags(
            tags,
            annotate_semantic_tags_with_filename(parsed.clone(), &parse_path.to_string_lossy()),
        );
        for tag in &parsed {
            if let Some(expanded) = expand_semantic_namespace_includes_for_search(tag, &base) {
                tags.push(expanded);
            }
        }
        for include_tag in parsed
            .iter()
            .filter(|tag| semantic_tag_class(tag).as_deref() == Some("include"))
        {
            let Some(include_path) = semantic_include_path_from_base(&base, include_tag) else {
                continue;
            };
            append_semantic_search_tags(
                tags,
                annotate_semantic_tags_with_filename(
                    cached_semantic_cpp_tags(&include_path),
                    &include_path.to_string_lossy(),
                ),
            );
        }
        // A header's implementations usually live in the same-stem source
        // file next to it; GNU finds them through the directory's
        // semanticdb.  Scan the sibling sources.
        if matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("h" | "hh" | "hpp")
        ) {
            for source_ext in ["cpp", "cc", "cxx", "c"] {
                let sibling = parse_path.with_extension(source_ext);
                if sibling.is_file() {
                    append_semantic_search_tags(
                        tags,
                        annotate_semantic_tags_with_filename(
                            cached_semantic_cpp_tags(&sibling),
                            &sibling.to_string_lossy(),
                        ),
                    );
                }
            }
        }
    }
}

fn expand_semantic_namespace_includes_for_search(tag: &Value, base: &Path) -> Option<Value> {
    if semantic_tag_class(tag).as_deref() != Some("type")
        || semantic_tag_attr(tag, ":type")
            .and_then(|value| string_text(&value).ok())
            .as_deref()
            != Some("namespace")
    {
        return None;
    }
    let name = semantic_tag_name(tag)?;
    let mut members = Vec::new();
    let mut changed = false;
    for member in semantic_tag_members(tag) {
        if semantic_tag_class(&member).as_deref() == Some("include") {
            members.push(member.clone());
            if let Some(path) = semantic_include_path_from_base(base, &member) {
                members.extend(cached_semantic_cpp_tags(&path));
                changed = true;
            }
            continue;
        }
        if let Some(expanded) = expand_semantic_namespace_includes_for_search(&member, base) {
            members.push(expanded);
            changed = true;
        } else {
            members.push(member);
        }
    }
    changed.then(|| {
        semantic_type_tag(
            &name,
            vec![
                (":members", Value::list(members)),
                (":type", Value::String("namespace".into())),
            ],
        )
        .unwrap_or_else(|| tag.clone())
    })
}

fn semantic_table_base_directory(interp: &mut Interpreter, table_path: &Path) -> Option<PathBuf> {
    if table_path.is_absolute() {
        return table_path.parent().map(Path::to_path_buf);
    }
    let database = interp.lookup_var("semanticdb-current-database", &Vec::new())?;
    let directory = eieio_slot_value(interp, &database, "reference-directory")
        .ok()
        .and_then(|value| string_text(&value).ok())?;
    Some(Path::new(&directory).to_path_buf())
}

fn semantic_include_path_from_base(base: &Path, include_tag: &Value) -> Option<PathBuf> {
    let include = semantic_tag_name(include_tag)?;
    let include_path = Path::new(&include);
    if include_path.is_absolute() && include_path.exists() {
        return Some(include_path.to_path_buf());
    }
    let candidate = base.join(include_path);
    candidate.exists().then_some(candidate)
}

fn append_semantic_search_tags(tags: &mut Vec<Value>, candidates: Vec<Value>) {
    for candidate in candidates {
        if semantic_tag_class(&candidate).is_some() {
            tags.push(candidate);
        } else if let Ok(items) = candidate.to_vec() {
            append_semantic_search_tags(tags, items);
        }
    }
}

fn cached_semantic_cpp_tags(path: &Path) -> Vec<Value> {
    if let Some(cached) = SEMANTIC_CPP_INCLUDE_TAG_CACHE.with(|cache| {
        cache.borrow().get(path).map(|tags| {
            tags.iter()
                .map(deep_copy_semantic_value)
                .collect::<Vec<_>>()
        })
    }) {
        return cached;
    }
    let parsed = std::fs::read_to_string(path)
        .map(|source| parse_semantic_cpp_tags_at_path(path, &source))
        .unwrap_or_default();
    SEMANTIC_CPP_INCLUDE_TAG_CACHE.with(|cache| {
        cache.borrow_mut().insert(
            path.to_path_buf(),
            parsed.iter().map(deep_copy_semantic_value).collect(),
        );
    });
    parsed
}

fn deep_copy_semantic_value(value: &Value) -> Value {
    match value {
        Value::Cons(cell) => Value::cons(
            deep_copy_semantic_value(&cell.car.borrow()),
            deep_copy_semantic_value(&cell.cdr.borrow()),
        ),
        _ => value.clone(),
    }
}

fn semantic_include_path(
    interp: &mut Interpreter,
    table: &Value,
    include_tag: &Value,
) -> Option<PathBuf> {
    let include = semantic_tag_name(include_tag)?;
    let include_path = Path::new(&include);
    if include_path.is_absolute() && include_path.exists() {
        return Some(include_path.to_path_buf());
    }
    let table_file = semantic_table_file_path(interp, table)?;
    let table_path = table_file.as_path();
    let base = if table_path.is_absolute() {
        table_path.parent()?.to_path_buf()
    } else {
        let database = interp.lookup_var("semanticdb-current-database", &Vec::new())?;
        let directory = eieio_slot_value(interp, &database, "reference-directory")
            .ok()
            .and_then(|value| string_text(&value).ok())?;
        Path::new(&directory).to_path_buf()
    };
    let candidate = base.join(include_path);
    candidate.exists().then_some(candidate)
}

fn semantic_table_file_path(interp: &mut Interpreter, table: &Value) -> Option<PathBuf> {
    let path = eieio_slot_value(interp, table, "file")
        .ok()
        .and_then(|value| string_text(&value).ok())
        .map(PathBuf::from)?;
    if path.is_absolute() {
        return Some(path);
    }
    interp
        .lookup_var("semanticdb-current-database", &Vec::new())
        .and_then(|database| {
            eieio_slot_value(interp, &database, "reference-directory")
                .ok()
                .and_then(|value| string_text(&value).ok())
        })
        .map(|directory| Path::new(&directory).join(&path))
        .or(Some(path))
}

fn parse_semantic_cpp_tags_at_path(path: &Path, source: &str) -> Vec<Value> {
    let cleaned = strip_cpp_comments(source);
    let cleaned = expand_cpp_spp_macros(&cleaned);
    let base_dir = path.parent().map(Path::to_path_buf);
    let mut parser = CppTagParser::new(&cleaned, base_dir);
    parser.parse_until(None)
}

/// A single preprocessor token plus the macro names whose expansions it came
/// from (the CPP hide set, for recursion prevention).
#[derive(Clone)]
struct SppToken {
    text: String,
    hide: Vec<String>,
}

#[derive(Clone)]
struct SppMacro {
    params: Option<Vec<String>>,
    body: Vec<String>,
}

/// Emulate semantic's lexical preprocessor: collect `#define' macros (plus
/// the builtin symbol map from semantic/bovine/c.el and its G++/VC++
/// namespace-hack analyzers) and substitute them through the rest of the
/// text, so the parse of a macro-using file matches the parse of its
/// hand-expanded counterpart.
fn expand_cpp_spp_macros(source: &str) -> String {
    let builtin_names = [
        "__THROW",
        "__const",
        "__restrict",
        "__attribute_pure__",
        "__attribute_malloc__",
        "__nonnull",
        "__wur",
        "_GLIBCXX_BEGIN_NAMESPACE",
        "_GLIBCXX_END_NAMESPACE",
        "_GLIBCXX_BEGIN_NESTED_NAMESPACE",
        "_GLIBCXX_END_NESTED_NAMESPACE",
        "_STD_BEGIN",
        "_STD_END",
    ];
    if !source.contains("#define") && !builtin_names.iter().any(|name| source.contains(name)) {
        return source.to_string();
    }

    let tokenize = |text: &str| -> Vec<String> {
        let bytes = text.as_bytes();
        let mut tokens = Vec::new();
        let mut index = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            if byte.is_ascii_alphabetic() || byte == b'_' {
                let start = index;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                tokens.push(text[start..index].to_string());
            } else if byte == b'#' && bytes.get(index + 1) == Some(&b'#') {
                tokens.push("##".to_string());
                index += 2;
            } else if byte == b'"' || byte == b'\'' {
                let quote = byte;
                let start = index;
                index += 1;
                while index < bytes.len() && bytes[index] != quote {
                    if bytes[index] == b'\\' {
                        index += 1;
                    }
                    index += 1;
                }
                index = (index + 1).min(bytes.len());
                tokens.push(text[start..index].to_string());
            } else if byte == b' ' || byte == b'\t' {
                let start = index;
                while index < bytes.len() && (bytes[index] == b' ' || bytes[index] == b'\t') {
                    index += 1;
                }
                tokens.push(text[start..index].to_string());
            } else {
                tokens.push((byte as char).to_string());
                index += 1;
            }
        }
        tokens
    };

    let mut macros: std::collections::HashMap<String, SppMacro> = std::collections::HashMap::new();
    let simple = |body: &str| SppMacro {
        params: None,
        body: tokenize(body)
            .into_iter()
            .filter(|t| !t.trim().is_empty())
            .collect(),
    };
    macros.insert("__THROW".into(), simple(""));
    macros.insert("__const".into(), simple("const"));
    macros.insert("__restrict".into(), simple(""));
    macros.insert("__attribute_pure__".into(), simple(""));
    macros.insert("__attribute_malloc__".into(), simple(""));
    macros.insert("__nonnull".into(), simple(""));
    macros.insert("__wur".into(), simple(""));
    macros.insert("_STD_BEGIN".into(), simple("namespace std {"));
    macros.insert("_STD_END".into(), simple("}"));
    macros.insert("_GLIBCXX_END_NAMESPACE".into(), simple("}"));
    macros.insert("_GLIBCXX_END_NESTED_NAMESPACE".into(), simple("} }"));
    macros.insert(
        "_GLIBCXX_BEGIN_NAMESPACE".into(),
        SppMacro {
            params: Some(vec!["X".into()]),
            body: vec!["namespace".into(), "X".into(), "{".into()],
        },
    );
    macros.insert(
        "_GLIBCXX_BEGIN_NESTED_NAMESPACE".into(),
        SppMacro {
            params: Some(vec!["X".into(), "Y".into()]),
            body: vec![
                "namespace".into(),
                "X".into(),
                "{".into(),
                "namespace".into(),
                "Y".into(),
                "{".into(),
            ],
        },
    );

    // Gather full logical lines (backslash continuations merged).
    let raw_lines: Vec<&str> = source.split('\n').collect();
    let mut logical: Vec<(String, usize, usize)> = Vec::new(); // (text, line count, start line)
    let mut index = 0;
    while index < raw_lines.len() {
        let start_line = index;
        let mut text = raw_lines[index].to_string();
        let mut count = 1;
        while text.trim_end().ends_with('\\') && index + count < raw_lines.len() {
            let trimmed = text.trim_end();
            text = format!(
                "{} {}",
                &trimmed[..trimmed.len() - 1],
                raw_lines[index + count]
            );
            count += 1;
        }
        logical.push((text, count, start_line));
        index += count;
    }

    let mut output = String::with_capacity(source.len());
    for (line, count, start_line) in logical {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix('#') {
            let rest = rest.trim_start();
            if let Some(def) = rest.strip_prefix("define") {
                let def = def.trim_start();
                let bytes = def.as_bytes();
                let mut end = 0;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
                {
                    end += 1;
                }
                if end > 0 {
                    let name = def[..end].to_string();
                    let after = &def[end..];
                    let (params, body_text) = if after.starts_with('(') {
                        let close = after.find(')').unwrap_or(after.len().saturating_sub(1));
                        let params = after[1..close]
                            .split(',')
                            .map(|p| p.trim().to_string())
                            .filter(|p| !p.is_empty())
                            .collect::<Vec<_>>();
                        (Some(params), after.get(close + 1..).unwrap_or(""))
                    } else {
                        (None, after)
                    };
                    let body = tokenize(body_text)
                        .into_iter()
                        .filter(|token| !token.trim().is_empty())
                        .collect();
                    macros.insert(name, SppMacro { params, body });
                }
                // Blank the consumed directive with matching whitespace so
                // every later tag keeps its buffer-absolute position.
                for line_index in start_line..start_line + count {
                    if let Some(raw) = raw_lines.get(line_index) {
                        for _ in 0..raw.len() {
                            output.push(' ');
                        }
                    }
                    output.push('\n');
                }
                continue;
            }
            // Other directives pass through untouched.
            output.push_str(&line);
            for _ in 0..count {
                output.push('\n');
            }
            continue;
        }

        // Expand macros in this line with pushback and hide sets.
        let mut queue: std::collections::VecDeque<SppToken> = tokenize(&line)
            .into_iter()
            .map(|text| SppToken {
                text,
                hide: Vec::new(),
            })
            .collect();
        let mut expanded: Vec<SppToken> = Vec::new();
        let mut budget = 10_000usize;
        while let Some(token) = queue.pop_front() {
            if budget == 0 {
                expanded.push(token);
                expanded.extend(queue.drain(..));
                break;
            }
            budget -= 1;
            let is_ident = token
                .text
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_');
            if !is_ident || token.hide.iter().any(|h| h == &token.text) {
                expanded.push(token);
                continue;
            }
            let Some(makro) = macros.get(&token.text).cloned() else {
                expanded.push(token);
                continue;
            };
            let substituted: Vec<Vec<SppToken>> = match &makro.params {
                None => vec![
                    makro
                        .body
                        .iter()
                        .map(|text| SppToken {
                            text: text.clone(),
                            hide: {
                                let mut hide = token.hide.clone();
                                hide.push(token.text.clone());
                                hide
                            },
                        })
                        .collect(),
                ],
                Some(params) => {
                    // Function-like: require an immediate `(' (skipping ws).
                    let mut skipped_ws = Vec::new();
                    while queue.front().is_some_and(|t| t.text.trim().is_empty()) {
                        skipped_ws.push(queue.pop_front().expect("front checked"));
                    }
                    if queue.front().is_none_or(|t| t.text != "(") {
                        expanded.push(token);
                        for ws in skipped_ws.into_iter().rev() {
                            queue.push_front(ws);
                        }
                        continue;
                    }
                    queue.pop_front();
                    let mut depth = 1usize;
                    let mut args: Vec<Vec<SppToken>> = vec![Vec::new()];
                    while let Some(next) = queue.pop_front() {
                        match next.text.as_str() {
                            "(" => {
                                depth += 1;
                                args.last_mut().expect("arg bucket").push(next);
                            }
                            ")" => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                                args.last_mut().expect("arg bucket").push(next);
                            }
                            "," if depth == 1 => args.push(Vec::new()),
                            _ => args.last_mut().expect("arg bucket").push(next),
                        }
                    }
                    for arg in &mut args {
                        while arg.first().is_some_and(|t| t.text.trim().is_empty()) {
                            arg.remove(0);
                        }
                        while arg.last().is_some_and(|t| t.text.trim().is_empty()) {
                            arg.pop();
                        }
                    }
                    let arg_for = |param: &str| -> Option<&Vec<SppToken>> {
                        params
                            .iter()
                            .position(|p| p == param)
                            .and_then(|i| args.get(i))
                    };
                    let mut hide = token.hide.clone();
                    hide.push(token.text.clone());
                    let mut result: Vec<SppToken> = Vec::new();
                    for text in &makro.body {
                        if let Some(arg) = arg_for(text) {
                            result.extend(arg.iter().map(|t| SppToken {
                                text: t.text.clone(),
                                hide: {
                                    let mut h = t.hide.clone();
                                    h.extend(hide.iter().cloned());
                                    h
                                },
                            }));
                        } else {
                            result.push(SppToken {
                                text: text.clone(),
                                hide: hide.clone(),
                            });
                        }
                    }
                    vec![result]
                }
            };
            for tokens in substituted {
                // Apply `##' pasting within the substituted body (skipping
                // whitespace tokens on both sides of the operator).
                let mut pasted: Vec<SppToken> = Vec::new();
                let mut iter = tokens.into_iter().peekable();
                while let Some(tok) = iter.next() {
                    if tok.text == "##" {
                        while pasted.last().is_some_and(|t| t.text.trim().is_empty()) {
                            pasted.pop();
                        }
                        let mut right = iter.next();
                        while right.as_ref().is_some_and(|t| t.text.trim().is_empty()) {
                            right = iter.next();
                        }
                        if let (Some(left), Some(right)) = (pasted.pop(), right) {
                            pasted.push(SppToken {
                                text: format!("{}{}", left.text, right.text),
                                hide: left.hide,
                            });
                        }
                    } else {
                        pasted.push(tok);
                    }
                }
                // Push back with a separating space only where adjacent
                // tokens would otherwise lex as one.
                let ident_boundary = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
                let mut with_spaces: Vec<SppToken> = Vec::new();
                for tok in pasted {
                    if let Some(previous) = with_spaces.last()
                        && previous.text.bytes().last().is_some_and(ident_boundary)
                        && tok.text.bytes().next().is_some_and(ident_boundary)
                    {
                        with_spaces.push(SppToken {
                            text: " ".into(),
                            hide: Vec::new(),
                        });
                    }
                    with_spaces.push(tok);
                }
                if let (Some(last), Some(front)) = (with_spaces.last(), queue.front())
                    && last.text.bytes().last().is_some_and(ident_boundary)
                    && front.text.bytes().next().is_some_and(ident_boundary)
                {
                    with_spaces.push(SppToken {
                        text: " ".into(),
                        hide: Vec::new(),
                    });
                }
                // Punctuation glues across macro boundaries: an expansion
                // starting (or ending) in punctuation joins the adjacent
                // text like semantic's token-level replacement does, so
                // `foo COLON COLON bar' renders as `foo::bar'.
                let starts_punct = with_spaces
                    .first()
                    .is_some_and(|t| !t.text.bytes().next().is_some_and(ident_boundary));
                let ends_punct = with_spaces
                    .last()
                    .is_some_and(|t| !t.text.bytes().last().is_some_and(ident_boundary));
                if starts_punct {
                    while expanded.last().is_some_and(|t| t.text.trim().is_empty()) {
                        expanded.pop();
                    }
                }
                if ends_punct {
                    while queue.front().is_some_and(|t| t.text.trim().is_empty()) {
                        queue.pop_front();
                    }
                }
                if let Some(previous) = expanded.last()
                    && previous.text.bytes().last().is_some_and(ident_boundary)
                    && with_spaces
                        .first()
                        .is_some_and(|t| t.text.bytes().next().is_some_and(ident_boundary))
                {
                    expanded.push(SppToken {
                        text: " ".into(),
                        hide: Vec::new(),
                    });
                }
                for tok in with_spaces.into_iter().rev() {
                    queue.push_front(tok);
                }
            }
        }
        let mut line_out = String::new();
        for token in expanded {
            line_out.push_str(&token.text);
        }
        output.push_str(&line_out);
        for _ in 0..count {
            output.push('\n');
        }
    }
    output
}

fn strip_cpp_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '/' && chars.peek() == Some(&'/') {
            out.push(' ');
            out.push(' ');
            chars.next();
            for next in chars.by_ref() {
                if next == '\n' {
                    out.push('\n');
                    break;
                } else {
                    out.push(' ');
                }
            }
        } else if ch == '/' && chars.peek() == Some(&'*') {
            out.push(' ');
            out.push(' ');
            chars.next();
            let mut previous = '\0';
            for next in chars.by_ref() {
                if next == '\n' {
                    out.push('\n');
                } else {
                    out.push(' ');
                }
                if previous == '*' && next == '/' {
                    break;
                }
                previous = next;
            }
        } else {
            out.push(ch);
        }
    }
    out
}

struct CppTagParser<'a> {
    source: &'a str,
    pos: usize,
    base_dir: Option<PathBuf>,
}

impl<'a> CppTagParser<'a> {
    fn new(source: &'a str, base_dir: Option<PathBuf>) -> Self {
        Self {
            source,
            pos: 0,
            base_dir,
        }
    }

    fn parse_until(&mut self, terminator: Option<u8>) -> Vec<Value> {
        let mut tags = Vec::new();
        while self.pos < self.source.len() {
            self.skip_ws();
            if terminator.is_some_and(|term| self.peek_byte() == Some(term)) {
                self.pos += 1;
                break;
            }
            if self.skip_preprocessor_directive() {
                continue;
            } else if let Some(include_tags) = self.parse_include_tags() {
                tags.extend(include_tags);
            } else if let Some(tag) = self.parse_namespace_alias() {
                tags.push(tag);
            } else if let Some(tag) = self.parse_namespace() {
                tags.push(tag);
            } else if let Some(tag) = self.parse_typedef_type_block() {
                append_semantic_search_tags(&mut tags, vec![tag]);
            } else if let Some(enum_tags) = self.parse_enum_block() {
                tags.extend(enum_tags);
            } else if let Some(tag) = self.parse_type_block() {
                append_semantic_search_tags(&mut tags, vec![tag]);
            } else if let Some(tag) = self.parse_statement() {
                tags.push(tag);
            } else {
                self.pos += 1;
            }
        }
        tags
    }

    fn parse_include_tags(&mut self) -> Option<Vec<Value>> {
        let start = self.pos;
        self.consume_byte(b'#')?;
        self.skip_ws();
        self.consume_word("include")?;
        self.skip_ws();
        let opener = self.peek_byte()?;
        let closer = match opener {
            b'"' => b'"',
            b'<' => b'>',
            _ => {
                self.pos = start;
                return None;
            }
        };
        self.pos += 1;
        let path_start = self.pos;
        while self.pos < self.source.len() && self.peek_byte() != Some(closer) {
            self.pos += 1;
        }
        let include = self.source[path_start..self.pos].trim();
        if self.peek_byte() == Some(closer) {
            self.pos += 1;
        }
        Some(vec![semantic_include_tag(include, opener == b'<')])
    }

    fn skip_preprocessor_directive(&mut self) -> bool {
        let start = self.pos;
        if self.consume_byte(b'#').is_none() {
            return false;
        }
        self.skip_ws();
        if self.source[self.pos..].starts_with("include") {
            self.pos = start;
            return false;
        }
        while self.pos < self.source.len() && self.peek_byte() != Some(b'\n') {
            self.pos += 1;
        }
        true
    }

    fn parse_namespace(&mut self) -> Option<Value> {
        let start = self.pos;
        self.consume_word("namespace")?;
        self.skip_ws();
        let name = self.read_ident()?;
        self.skip_until_byte(b'{')?;
        self.pos += 1;
        let body_start = self.pos;
        self.skip_balanced_block_from_open(1)?;
        let body_end = self.pos.saturating_sub(1);
        let mut parser =
            CppTagParser::new(&self.source[body_start..body_end], self.base_dir.clone());
        // The body is parsed as a substring; rebase member positions so all
        // tag bounds stay buffer-absolute like GNU's parser.
        let members = parser
            .parse_until(None)
            .iter()
            .map(|member| shift_semantic_tag_bounds(member, body_start as i64))
            .collect::<Vec<_>>();
        let end = self.pos;
        semantic_type_tag_bounded(
            &name,
            vec![
                (":members", Value::list(members)),
                (":type", Value::String("namespace".into())),
            ],
            start,
            end,
        )
        .or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_namespace_alias(&mut self) -> Option<Value> {
        let start = self.pos;
        self.consume_word("namespace")?;
        self.skip_ws();
        let Some(alias) = self.read_ident() else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        if self.consume_byte(b'=').is_none() {
            self.pos = start;
            return None;
        }
        self.skip_ws();
        let Some(target) = self.read_qualified_ident() else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        if self.consume_byte(b';').is_none() {
            self.pos = start;
            return None;
        }
        semantic_type_tag(
            &alias,
            vec![
                (":namespace-alias", Value::String(target.into())),
                (":type", Value::String("namespace".into())),
            ],
        )
    }

    fn parse_type_block(&mut self) -> Option<Value> {
        let start = self.pos;
        let template_params = self.consume_template_prefixes();
        while self
            .consume_one_of_words(&[
                "public",
                "private",
                "protected",
                "static",
                "final",
                "abstract",
                "strictfp",
            ])
            .is_some()
        {
            self.skip_ws();
        }
        let kind = if self.consume_word("class").is_some() {
            "class"
        } else if self.consume_word("struct").is_some() {
            "struct"
        } else if self.consume_word("interface").is_some() {
            "interface"
        } else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        let name = self
            .read_qualified_ident()
            .map(|name| name.rsplit("::").next().unwrap_or(&name).to_string());
        let header_start = self.pos;
        if self.skip_until_type_body().is_none() {
            self.pos = start;
            return None;
        }
        let header = &self.source[header_start..self.pos];
        self.pos += 1;
        let members = self.parse_until(Some(b'}'));
        let variable_names = self.read_trailing_decl_names();
        if !variable_names.is_empty() {
            self.consume_optional_statement_tail();
        }
        let type_name = name
            .or_else(|| {
                variable_names
                    .first()
                    .map(|variable| format!("__anon_{kind}_{variable}"))
            })
            .unwrap_or_else(|| format!("__anon_{kind}_{}", self.pos));
        let mut attrs = vec![
            (":members", Value::list(members)),
            (":type", Value::String(kind.into())),
        ];
        if !template_params.is_empty() {
            attrs.push((
                ":template-params",
                Value::list(
                    template_params
                        .into_iter()
                        .map(|value| Value::String(value.into())),
                ),
            ));
        }
        if let Some(superclasses) = parse_cpp_superclasses(
            header,
            if kind == "struct" {
                "public"
            } else {
                "private"
            },
        ) {
            attrs.push((":superclasses", superclasses));
        }
        let end = self.pos;
        let mut tags = vec![semantic_type_tag_bounded(&type_name, attrs, start, end)?];
        for variable_name in variable_names.into_iter().rev() {
            tags.insert(
                0,
                semantic_variable_tag(&variable_name, semantic_type_ref(&type_name), false),
            );
        }
        if tags.len() == 1 {
            tags.pop()
        } else {
            Some(Value::list(tags))
        }
        .or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_typedef_type_block(&mut self) -> Option<Value> {
        let start = self.pos;
        self.consume_word("typedef")?;
        self.skip_ws();
        let kind = if self.consume_word("struct").is_some() {
            "struct"
        } else if self.consume_word("class").is_some() {
            "class"
        } else if self.consume_word("enum").is_some() {
            "enum"
        } else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        let original_name = self.read_qualified_ident();
        self.skip_ws();
        if self.consume_byte(b'{').is_none() {
            self.pos = start;
            return None;
        }
        let body_start = self.pos;
        self.skip_balanced_block_from_open(1)?;
        let body_end = self.pos.saturating_sub(1);
        let body = &self.source[body_start..body_end];
        let members = if kind == "enum" {
            body.split(',')
                .filter_map(|part| {
                    let name = part
                        .split('=')
                        .next()
                        .unwrap_or(part)
                        .split_whitespace()
                        .next()?;
                    (!name.is_empty())
                        .then(|| semantic_variable_tag(name, Value::String("int".into()), false))
                })
                .collect::<Vec<_>>()
        } else {
            let mut parser = CppTagParser::new(body, self.base_dir.clone());
            parser.parse_until(None)
        };
        let alias = self.read_trailing_decl_name()?;
        self.consume_optional_statement_tail();
        let mut tags = Vec::new();
        if let Some(original_name) = original_name.filter(|name| !name.is_empty()) {
            let original_name = original_name
                .rsplit("::")
                .next()
                .unwrap_or(&original_name)
                .to_string();
            tags.push(semantic_type_tag_bounded(
                &original_name,
                vec![
                    (":members", Value::list(members)),
                    (":type", Value::String(kind.into())),
                ],
                start,
                self.pos,
            )?);
            tags.push(semantic_type_tag_bounded(
                &alias,
                vec![
                    (":typedef", semantic_type_ref(&original_name)),
                    (":type", Value::String("typedef".into())),
                ],
                start,
                self.pos,
            )?);
        } else {
            tags.push(semantic_type_tag_bounded(
                &alias,
                vec![
                    (":members", Value::list(members)),
                    (":type", Value::String(kind.into())),
                ],
                start,
                self.pos,
            )?);
        }
        Some(if tags.len() == 1 {
            tags.pop().unwrap_or(Value::Nil)
        } else {
            Value::list(tags)
        })
        .or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_enum_block(&mut self) -> Option<Vec<Value>> {
        let start = self.pos;
        self.consume_word("enum")?;
        self.skip_ws();
        let _ = self.read_ident();
        self.skip_ws();
        if self.peek_byte() == Some(b':') {
            self.skip_until_byte(b'{')?;
        }
        self.consume_byte(b'{')?;
        let body_start = self.pos;
        self.skip_balanced_block_from_open(1)?;
        let body_end = self.pos.saturating_sub(1);
        let body = &self.source[body_start..body_end];
        self.consume_optional_statement_tail();
        let tags = body
            .split(',')
            .filter_map(|part| {
                let name = part
                    .split('=')
                    .next()
                    .unwrap_or(part)
                    .split_whitespace()
                    .next()?;
                (!name.is_empty())
                    .then(|| semantic_variable_tag(name, Value::String("int".into()), false))
            })
            .collect::<Vec<_>>();
        Some(tags).or_else(|| {
            self.pos = start;
            None
        })
    }

    fn consume_template_prefixes(&mut self) -> Vec<String> {
        let mut params = Vec::new();
        loop {
            self.skip_ws();
            let checkpoint = self.pos;
            if self.consume_word("template").is_none() {
                return params;
            }
            self.skip_ws();
            if self.consume_byte(b'<').is_none() {
                self.pos = checkpoint;
                return params;
            }
            let params_start = self.pos;
            if self.skip_balanced_angle().is_none() {
                self.pos = checkpoint;
                return params;
            }
            let params_end = self.pos.saturating_sub(1);
            params = parse_cpp_template_params(&self.source[params_start..params_end]);
        }
    }

    fn parse_statement(&mut self) -> Option<Value> {
        let (statement, has_body, start, end) = self.read_statement()?;
        let statement = statement.trim();
        if statement.is_empty() {
            return None;
        }
        let access_label = statement.trim_end_matches(':').trim();
        if matches!(access_label, "public" | "private" | "protected") {
            return Some(semantic_label_tag_bounded(access_label, start, end));
        }
        if let Some(tag) = parse_cpp_using_statement(statement) {
            return Some(semantic_tag_with_bounds_from_tag(tag, start, end));
        }
        if statement
            .split_whitespace()
            .next()
            .is_some_and(|word| word == "typedef")
        {
            let rest = statement
                .split_once(char::is_whitespace)
                .map(|(_, rest)| rest)
                .unwrap_or("");
            return parse_cpp_typedef(rest)
                .map(|tag| semantic_tag_with_bounds_from_tag(tag, start, end));
        }
        if statement.contains('(') && statement.contains(')') {
            return parse_cpp_function_bounded(statement, !has_body, start, end);
        }
        parse_cpp_variable_bounded(statement, start, end)
    }

    fn read_statement(&mut self) -> Option<(String, bool, usize, usize)> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b';' => {
                    let statement = self.source[start..self.pos].to_string();
                    self.pos += 1;
                    return Some((statement, false, start, self.pos));
                }
                b':' => {
                    let statement = self.source[start..self.pos].trim();
                    if matches!(statement, "public" | "private" | "protected") {
                        let statement = self.source[start..=self.pos].to_string();
                        self.pos += 1;
                        return Some((statement, false, start, self.pos));
                    }
                    self.pos += 1;
                }
                b'{' => {
                    let statement = self.source[start..self.pos].to_string();
                    self.skip_balanced_block();
                    return (!statement.trim().is_empty())
                        .then_some((statement, true, start, self.pos));
                }
                b'}' => return None,
                _ => self.pos += 1,
            }
        }
        None
    }

    fn skip_balanced_block(&mut self) {
        if self.peek_byte() != Some(b'{') {
            return;
        }
        let _ = self.skip_balanced_block_from_open(0);
    }

    fn skip_balanced_block_from_open(&mut self, initial_depth: usize) -> Option<()> {
        let mut depth = 0usize;
        if initial_depth > 0 {
            depth = initial_depth;
        }
        while self.pos < self.source.len() {
            match self.peek_byte() {
                Some(b'{') => {
                    depth += 1;
                    self.pos += 1;
                }
                Some(b'}') => {
                    depth = depth.saturating_sub(1);
                    self.pos += 1;
                    if depth == 0 {
                        return Some(());
                    }
                }
                Some(_) => self.pos += 1,
                None => break,
            }
        }
        None
    }

    fn skip_balanced_angle(&mut self) -> Option<()> {
        let mut depth = 1usize;
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b'<' => {
                    depth += 1;
                    self.pos += 1;
                }
                b'>' => {
                    depth = depth.saturating_sub(1);
                    self.pos += 1;
                    if depth == 0 {
                        return Some(());
                    }
                }
                _ => self.pos += 1,
            }
        }
        None
    }

    fn read_trailing_decl_name(&mut self) -> Option<String> {
        self.skip_ws();
        let checkpoint = self.pos;
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b';' => break,
                b'{' | b'}' => {
                    self.pos = checkpoint;
                    return None;
                }
                _ => self.pos += 1,
            }
        }
        if self.peek_byte() != Some(b';') {
            self.pos = checkpoint;
            return None;
        }
        self.pos = checkpoint;
        let name = self.read_ident();
        self.pos = checkpoint;
        name
    }

    fn read_trailing_decl_names(&mut self) -> Vec<String> {
        self.skip_ws();
        let checkpoint = self.pos;
        while self.pos < self.source.len() {
            match self.peek_byte() {
                Some(b';') => break,
                Some(b'{') | Some(b'}') | None => {
                    self.pos = checkpoint;
                    return Vec::new();
                }
                _ => self.pos += 1,
            }
        }
        if self.peek_byte() != Some(b';') {
            self.pos = checkpoint;
            return Vec::new();
        }
        let text = &self.source[checkpoint..self.pos];
        self.pos = checkpoint;
        text.split(',')
            .filter_map(cpp_trailing_decl_name)
            .collect::<Vec<_>>()
    }

    fn consume_optional_statement_tail(&mut self) {
        while self.pos < self.source.len() && self.peek_byte() != Some(b';') {
            if self.peek_byte() == Some(b'{') || self.peek_byte() == Some(b'}') {
                return;
            }
            self.pos += 1;
        }
        if self.peek_byte() == Some(b';') {
            self.pos += 1;
        }
    }

    fn consume_word(&mut self, word: &str) -> Option<()> {
        if self.source[self.pos..].starts_with(word)
            && self
                .source
                .as_bytes()
                .get(self.pos + word.len())
                .is_none_or(|byte| !is_ident_byte(*byte))
        {
            self.pos += word.len();
            Some(())
        } else {
            None
        }
    }

    fn consume_byte(&mut self, byte: u8) -> Option<()> {
        if self.peek_byte() == Some(byte) {
            self.pos += 1;
            Some(())
        } else {
            None
        }
    }

    fn consume_one_of_words(&mut self, words: &[&str]) -> Option<()> {
        words.iter().find_map(|word| self.consume_word(word))
    }

    fn read_ident(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.source.len()
            && self
                .source
                .as_bytes()
                .get(self.pos)
                .is_some_and(|byte| is_ident_byte(*byte))
        {
            self.pos += 1;
        }
        (self.pos > start).then(|| self.source[start..self.pos].to_string())
    }

    fn read_qualified_ident(&mut self) -> Option<String> {
        let mut name = self.read_ident()?;
        loop {
            let checkpoint = self.pos;
            if self.source[self.pos..].starts_with("::") {
                self.pos += 2;
                if let Some(part) = self.read_ident() {
                    name.push_str("::");
                    name.push_str(&part);
                    continue;
                }
            }
            self.pos = checkpoint;
            return Some(name);
        }
    }

    fn skip_until_byte(&mut self, byte: u8) -> Option<()> {
        while self.pos < self.source.len() {
            if self.peek_byte() == Some(byte) {
                return Some(());
            }
            self.pos += 1;
        }
        None
    }

    fn skip_until_type_body(&mut self) -> Option<()> {
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b'{' => return Some(()),
                b';' => return None,
                _ => self.pos += 1,
            }
        }
        None
    }

    fn skip_ws(&mut self) {
        while self
            .source
            .as_bytes()
            .get(self.pos)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.pos += 1;
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.source.as_bytes().get(self.pos).copied()
    }
}

fn parse_cpp_typedef(rest: &str) -> Option<Value> {
    let rest = rest.trim();
    let (type_text, name) = rest.rsplit_once(char::is_whitespace)?;
    semantic_type_tag(
        name.trim(),
        vec![
            (":typedef", semantic_cpp_type_value(type_text.trim())),
            (":type", Value::String("typedef".into())),
        ],
    )
}

fn parse_cpp_using_statement(statement: &str) -> Option<Value> {
    let rest = statement.trim().strip_prefix("using ")?;
    let rest = rest.trim();
    if let Some(namespace) = rest.strip_prefix("namespace ") {
        let namespace = namespace.trim().trim_end_matches(';').trim();
        return Some(semantic_tag(
            namespace,
            "using",
            semantic_plist(vec![(":namespace", Value::String(namespace.into()))]),
        ));
    }
    let target = rest.trim_end_matches(';').trim();
    if target.is_empty() {
        return None;
    }
    let name = target.rsplit("::").next()?.trim();
    if name.is_empty() || target == name {
        return Some(semantic_tag(
            target,
            "using",
            semantic_plist(vec![(":namespace", Value::String(target.into()))]),
        ));
    }
    semantic_type_tag(
        name,
        vec![
            (":typedef", semantic_type_ref(target)),
            (":type", Value::String("typedef".into())),
        ],
    )
}

fn cpp_trailing_decl_name(decl: &str) -> Option<String> {
    let decl = decl
        .split('=')
        .next()
        .unwrap_or(decl)
        .split('[')
        .next()
        .unwrap_or(decl)
        .trim()
        .trim_matches(|ch| matches!(ch, '*' | '&'));
    let name = decl
        .rsplit(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    Some(name.to_string())
}

fn parse_cpp_function(statement: &str, prototype: bool) -> Option<Value> {
    let open = statement.find('(')?;
    // The parameter list ends at the FIRST balanced close: junk groups after
    // the arglist (e.g. left over from preprocessor tricks) are not params.
    let mut depth = 0usize;
    let mut close = None;
    for (index, byte) in statement.bytes().enumerate().skip(open) {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    close = Some(index);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close?;
    let head = statement[..open].trim();
    let args = statement[open + 1..close].trim();
    let mut parts = head.split_whitespace().collect::<Vec<_>>();
    let name_part = parts.pop()?;
    // `~Class()' (and `Class::~Class()') is the destructor; the tag keeps
    // the plain class name like GNU's parser.
    let is_destructor = name_part.contains('~');
    let raw_name = name_part.trim_start_matches('~');
    let name = raw_name
        .rsplit("::")
        .next()
        .unwrap_or(raw_name)
        .trim_start_matches('~');
    // A qualified definition (`ns::Class::method') records its class as
    // `:parent', like GNU's parser.
    let parent = raw_name
        .strip_suffix(name)
        .map(|qualifier| qualifier.trim_end_matches("::").trim_end_matches('~'))
        .filter(|qualifier| !qualifier.is_empty())
        .and_then(|qualifier| qualifier.rsplit("::").next())
        .filter(|component| !component.is_empty())
        .map(str::to_string);
    let return_type = parts.join(" ");
    let mut attrs = Vec::new();
    if let Some(parent) = &parent {
        attrs.push((":parent", Value::String(parent.clone().into())));
    }
    if prototype {
        attrs.push((":prototype-flag", Value::T));
    }
    if let Some(modifiers) = semantic_c_like_typemodifiers(statement) {
        attrs.push((":typemodifiers", modifiers));
    }
    if return_type.is_empty() || is_destructor {
        if is_destructor {
            attrs.push((":destructor-flag", Value::T));
            attrs.push((":type", Value::String("void".into())));
        } else {
            attrs.push((":constructor-flag", Value::T));
            attrs.push((":type", semantic_type_ref(name)));
        }
    } else {
        let arguments = parse_cpp_arguments(args);
        if !arguments.is_empty() {
            attrs.push((":arguments", Value::list(arguments)));
        }
        attrs.push((":type", semantic_cpp_type_value(&return_type)));
    }
    semantic_function_tag(name, attrs)
}

fn parse_cpp_function_bounded(
    statement: &str,
    prototype: bool,
    start: usize,
    end: usize,
) -> Option<Value> {
    parse_cpp_function(statement, prototype)
        .map(|tag| semantic_tag_with_bounds_from_tag(tag, start, end))
}

fn parse_cpp_arguments(args: &str) -> Vec<Value> {
    args.split(',')
        .filter_map(|arg| {
            let arg = arg.trim();
            if arg.is_empty() || arg == "void" {
                return None;
            }
            let mut parts = arg.split_whitespace().collect::<Vec<_>>();
            let name = parts.pop().unwrap_or("");
            let type_text;
            let (name, type_text_ref) =
                if parts.is_empty() || name.chars().all(|ch| ch == '*' || ch == '&') {
                    ("", arg)
                } else {
                    type_text = parts.join(" ");
                    (name.trim_matches(['*', '&']), type_text.as_str())
                };
            Some(semantic_variable_tag(
                name,
                semantic_cpp_type_value(type_text_ref.trim()),
                arg.contains('*'),
            ))
        })
        .collect()
}

fn parse_cpp_superclasses(header: &str, default_access: &str) -> Option<Value> {
    let rest = header
        .split_once(':')
        .map(|(_, rest)| rest)
        .or_else(|| header.split_once("extends").map(|(_, rest)| rest))?;
    let superclasses = rest
        .split(',')
        .filter_map(|part| {
            let mut access = default_access;
            let mut name = None;
            for word in part.split_whitespace() {
                if matches!(word, "public" | "private" | "protected") {
                    access = word;
                } else if !matches!(word, "virtual") {
                    name = Some(word);
                }
            }
            let name = name?;
            let name = cpp_type_base_name(name.trim_matches(['*', '&']));
            (!name.is_empty()).then(|| {
                semantic_tag(
                    &name,
                    "type",
                    semantic_plist(vec![
                        (":type", Value::String("class".into())),
                        (":inheritance", Value::String(access.into())),
                    ]),
                )
            })
        })
        .collect::<Vec<_>>();
    (!superclasses.is_empty()).then(|| Value::list(superclasses))
}

fn parse_cpp_template_params(text: &str) -> Vec<String> {
    split_cpp_top_level_commas(text)
        .into_iter()
        .filter_map(|param| {
            let param = param.split('=').next().unwrap_or(&param).trim();
            let name = param
                .split_whitespace()
                .last()
                .unwrap_or(param)
                .trim_matches(['*', '&']);
            (!name.is_empty()).then(|| name.to_string())
        })
        .collect()
}

fn cpp_type_base_name(name: &str) -> String {
    name.split_once('<')
        .map(|(base, _)| base)
        .unwrap_or(name)
        .trim()
        .to_string()
}

fn parse_cpp_variable(statement: &str) -> Option<Value> {
    let default_value = statement
        .split_once('=')
        .map(|(_, value)| value.trim().trim_end_matches(';').trim())
        .filter(|value| !value.is_empty());
    let declaration = statement.split('=').next().unwrap_or(statement).trim();
    let mut parts = declaration.split_whitespace().collect::<Vec<_>>();
    let raw_name = parts.pop()?.trim();
    let name = raw_name
        .split_once('[')
        .map(|(name, _)| name)
        .unwrap_or(raw_name)
        .trim_matches(['*', '&']);
    let type_text = parts.join(" ");
    let type_text = type_text.trim();
    if type_text.is_empty()
        || name.is_empty()
        || name
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'_')
        || semantic_c_like_statement_keyword(type_text.split_whitespace().next()?)
    {
        return None;
    }
    let mut attrs = Vec::new();
    if statement.contains('*') {
        attrs.push((":pointer", Value::Integer(1)));
    }
    attrs.push((":type", semantic_cpp_type_value(type_text)));
    if let Some(default_value) = default_value {
        attrs.push((":default-value", Value::String(default_value.into())));
    }
    if let Some(modifiers) = semantic_c_like_typemodifiers(statement) {
        attrs.push((":typemodifiers", modifiers));
    }
    Some(semantic_tag(name, "variable", semantic_plist(attrs)))
}

fn parse_cpp_variable_bounded(statement: &str, start: usize, end: usize) -> Option<Value> {
    parse_cpp_variable(statement).map(|tag| semantic_tag_with_bounds_from_tag(tag, start, end))
}

fn semantic_c_like_statement_keyword(word: &str) -> bool {
    matches!(
        word,
        "break"
            | "case"
            | "continue"
            | "default"
            | "delete"
            | "do"
            | "else"
            | "for"
            | "goto"
            | "if"
            | "new"
            | "return"
            | "switch"
            | "throw"
            | "while"
    )
}

fn semantic_c_like_typemodifiers(statement: &str) -> Option<Value> {
    let modifiers = statement
        .split_whitespace()
        .take_while(|word| {
            matches!(
                *word,
                "public"
                    | "private"
                    | "protected"
                    | "static"
                    | "const"
                    | "mutable"
                    | "volatile"
                    | "final"
                    | "abstract"
                    | "strictfp"
            )
        })
        .map(|word| Value::String(word.into()))
        .collect::<Vec<_>>();
    (!modifiers.is_empty()).then(|| Value::list(modifiers))
}

fn semantic_type_tag(name: &str, attrs: Vec<(&str, Value)>) -> Option<Value> {
    Some(semantic_tag(name, "type", semantic_plist(attrs)))
}

fn semantic_type_tag_bounded(
    name: &str,
    attrs: Vec<(&str, Value)>,
    start: usize,
    end: usize,
) -> Option<Value> {
    Some(semantic_tag_with_bounds(
        name,
        "type",
        semantic_plist(attrs),
        start,
        end,
    ))
}

fn semantic_function_tag(name: &str, attrs: Vec<(&str, Value)>) -> Option<Value> {
    Some(semantic_tag(name, "function", semantic_plist(attrs)))
}

fn semantic_variable_tag(name: &str, type_value: Value, pointer: bool) -> Value {
    let mut attrs = Vec::new();
    if pointer {
        attrs.push((":pointer", Value::Integer(1)));
    }
    attrs.push((":type", type_value));
    semantic_tag(name, "variable", semantic_plist(attrs))
}

fn semantic_label_tag_bounded(name: &str, start: usize, end: usize) -> Value {
    semantic_tag_with_bounds(name, "label", Value::Nil, start, end)
}

fn semantic_include_tag(name: &str, system: bool) -> Value {
    let attrs = if system {
        semantic_plist(vec![(":system-flag", Value::T)])
    } else {
        Value::Nil
    };
    semantic_tag(name, "include", attrs)
}

fn semantic_tag(name: &str, class: &str, attrs: Value) -> Value {
    Value::list([
        Value::String(name.into()),
        Value::Symbol(class.into()),
        attrs,
        Value::Nil,
        Value::Nil,
    ])
}

fn semantic_tag_with_bounds(
    name: &str,
    class: &str,
    attrs: Value,
    start: usize,
    end: usize,
) -> Value {
    Value::list([
        Value::String(name.into()),
        Value::Symbol(class.into()),
        attrs,
        Value::Nil,
        semantic_bounds_vector(start, end),
    ])
}

fn semantic_tag_with_bounds_from_tag(tag: Value, start: usize, end: usize) -> Value {
    let Ok(mut items) = tag.to_vec() else {
        return tag;
    };
    if items.len() < 5 {
        items.resize(5, Value::Nil);
    }
    items[4] = semantic_bounds_vector(start, end);
    Value::list(items)
}

fn semantic_bounds_vector(start: usize, end: usize) -> Value {
    Value::list([
        Value::Symbol("vector-literal".into()),
        Value::Integer(start.saturating_add(1) as i64),
        Value::Integer(end.saturating_add(1) as i64),
    ])
}

fn semantic_plist(attrs: Vec<(&str, Value)>) -> Value {
    Value::list(
        attrs
            .into_iter()
            .flat_map(|(key, value)| [Value::Symbol(key.into()), value])
            .collect::<Vec<_>>(),
    )
}

fn semantic_cpp_type_value(type_text: &str) -> Value {
    let aggregate_kind = type_text
        .split_whitespace()
        .next()
        .filter(|kind| matches!(*kind, "struct" | "class" | "enum" | "union"));
    let type_text = type_text
        .replace("const ", "")
        .replace(" const", "")
        .replace("mutable ", "")
        .replace(" mutable", "")
        .replace("struct ", "")
        .replace("public ", "")
        .replace("private ", "")
        .replace("protected ", "")
        .replace("static ", "")
        .replace(" static", "")
        .replace("final ", "")
        .replace("abstract ", "")
        .replace("strictfp ", "")
        .replace(" volatile", "")
        .replace(['*', '&'], "")
        .trim()
        .to_string();
    if matches!(
        type_text.as_str(),
        "void" | "int" | "char" | "unsigned int" | "long" | "short" | "float" | "double"
    ) {
        Value::String(type_text.into())
    } else if let Some(kind) = aggregate_kind {
        semantic_type_ref_with_kind(&type_text, kind)
    } else {
        semantic_type_ref(&type_text)
    }
}

fn semantic_type_ref(name: &str) -> Value {
    semantic_type_ref_with_kind(name, "class")
}

fn semantic_type_ref_with_kind(name: &str, kind: &str) -> Value {
    Value::list([
        Value::String(name.into()),
        Value::Symbol("type".into()),
        semantic_plist(vec![(":type", Value::String(kind.into()))]),
        Value::Nil,
        Value::Nil,
    ])
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn semantic_completion_matches(tags: &[Value], prefix: &str) -> Vec<Value> {
    let parts = prefix
        .split("::")
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if parts.len() > 1 {
        let (parents, final_prefix) = parts.split_at(parts.len() - 1);
        if let Some(parent) = find_semantic_type_chain(tags, parents) {
            let mut matches = Vec::new();
            collect_semantic_completion_tags(
                &semantic_tag_members(&parent),
                &final_prefix[0],
                &mut matches,
            );
            return matches;
        }
        return Vec::new();
    }
    let mut matches = Vec::new();
    collect_semantic_completion_tags(tags, prefix, &mut matches);
    matches
}

fn regexp_opt_depth(regexp: &str) -> usize {
    let bytes = regexp.as_bytes();
    let mut index = 0;
    let mut depth = 0;
    while index + 1 < bytes.len() {
        if bytes[index] == b'\\' && bytes[index + 1] == b'(' {
            let shy_group =
                index + 3 < bytes.len() && bytes[index + 2] == b'?' && bytes[index + 3] == b':';
            if !shy_group {
                depth += 1;
            }
            index += 2;
        } else if bytes[index] == b'\\' {
            index += 2;
        } else {
            index += 1;
        }
    }
    depth
}

fn process_file_compat(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let requested_program = string_text(&args[0])?;
    let program = unquote_local_file_name(&requested_program).unwrap_or(requested_program);
    let command_args = args
        .get(4..)
        .unwrap_or(&[])
        .iter()
        .map(string_text)
        .collect::<Result<Vec<_>, _>>()?;
    // Synchronous processes share the same environment, working-directory,
    // and coding boundary as every other external command.  A private
    // `Command' setup here previously omitted Lisp's process environment and
    // duplicated an incomplete remote-directory translation.
    let output = run_external_process(interp, &program, &command_args, None, env)?;
    write_process_output(
        interp,
        args.get(2).unwrap_or(&Value::Nil),
        &output.stdout,
        &output.stderr,
        "call-process",
        args,
        env,
    )?;
    Ok(Value::Integer(output.status.code().unwrap_or(1) as i64))
}

fn find_semantic_type_chain(tags: &[Value], names: &[String]) -> Option<Value> {
    find_semantic_type_chain_in(tags, tags, names).or_else(|| {
        names
            .last()
            .and_then(|name| find_semantic_type_deep(tags, name))
            .map(|tag| resolve_semantic_typedef(tags, &tag))
    })
}

fn find_semantic_type_chain_in(
    root_tags: &[Value],
    tags: &[Value],
    names: &[String],
) -> Option<Value> {
    let (first, rest) = names.split_first()?;
    let mut best = None;
    let mut best_score = 0usize;
    for tag in tags {
        if semantic_tag_name(tag).as_deref() == Some(first)
            && let Some(resolved) = semantic_type_candidate(root_tags, tag)
        {
            if rest.is_empty() {
                let score = semantic_type_resolution_score(&resolved);
                if best.is_none() || score > best_score {
                    best_score = score;
                    best = Some(resolved);
                }
                continue;
            }
            if let Some(found) =
                find_semantic_type_chain_in(root_tags, &semantic_tag_members(&resolved), rest)
            {
                return Some(found);
            }
        }
    }
    best
}

fn semantic_type_resolution_score(tag: &Value) -> usize {
    semantic_tag_members(tag).len()
        + usize::from(semantic_tag_attr(tag, ":superclasses").is_some()) * 100
}

fn resolve_semantic_typedef(root_tags: &[Value], tag: &Value) -> Value {
    let mut current = tag.clone();
    let mut seen = Vec::new();
    loop {
        let Some(target) = semantic_tag_attr(&current, ":typedef") else {
            return current;
        };
        let Ok(parts) = semantic_type_name_parts(&target) else {
            return current;
        };
        if parts.is_empty() || seen.contains(&parts) {
            return current;
        }
        seen.push(parts.clone());
        let next = if parts.len() > 1 {
            find_semantic_type_chain_in(root_tags, root_tags, &parts)
        } else {
            find_semantic_type_chain(root_tags, &parts)
                .or_else(|| find_semantic_type_deep(root_tags, parts.last()?))
        };
        let Some(next) = next else {
            return current;
        };
        current = next;
    }
}

fn find_semantic_type_deep(tags: &[Value], name: &str) -> Option<Value> {
    let mut best = None;
    let mut best_score = 0usize;
    for tag in tags {
        if semantic_tag_name(tag).as_deref() == Some(name)
            && let Some(found) = semantic_type_candidate(tags, tag)
        {
            let score = semantic_type_resolution_score(&found);
            if best.is_none() || score > best_score {
                best_score = score;
                best = Some(found);
            }
        }
        if let Some(found) = find_semantic_type_deep(&semantic_tag_members(tag), name) {
            let score = semantic_type_resolution_score(&found);
            if best.is_none() || score > best_score {
                best_score = score;
                best = Some(found);
            }
        }
    }
    best
}

fn semantic_type_candidate(root_tags: &[Value], tag: &Value) -> Option<Value> {
    match semantic_tag_class(tag).as_deref() {
        Some("type") => {
            if semantic_tag_members(tag).is_empty()
                && let Some(name) = semantic_tag_name(tag)
                && let Some(found) = semantic_type_from_name(root_tags, &name)
                && !semantic_tag_members(&found).is_empty()
            {
                return Some(found);
            }
            Some(resolve_semantic_typedef(root_tags, tag))
        }
        Some("variable") => semantic_tag_attr(tag, ":type")
            .and_then(|type_value| semantic_type_name_parts(&type_value).ok())
            .and_then(|mut parts| {
                if parts.len() == 1 && parts[0].contains(char::is_whitespace) {
                    parts = parts[0]
                        .split_whitespace()
                        .map(|part| part.trim_matches(['*', '&']).to_string())
                        .collect();
                }
                parts.retain(|part| {
                    !part.is_empty()
                        && !matches!(
                            part.as_str(),
                            "const" | "volatile" | "struct" | "class" | "mutable" | "static"
                        )
                });
                find_semantic_type_chain(root_tags, &parts).or_else(|| {
                    parts
                        .last()
                        .and_then(|name| find_semantic_type_deep(root_tags, name))
                })
            }),
        _ => None,
    }
}

fn collect_semantic_completion_tags(tags: &[Value], prefix: &str, matches: &mut Vec<Value>) {
    for tag in tags {
        if semantic_tag_name(tag)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
        collect_semantic_completion_tags(&semantic_tag_members(tag), prefix, matches);
    }
}

fn widget_get(interp: &Interpreter, widget: &Value, property: &Value) -> Result<Value, LispError> {
    widget_get_inner(interp, widget, property, &mut HashSet::new())
}

fn widget_get_inner(
    interp: &Interpreter,
    widget: &Value,
    property: &Value,
    seen: &mut HashSet<String>,
) -> Result<Value, LispError> {
    match widget {
        Value::Cons(cons_cell) => {
            let car = &cons_cell.car;
            let cdr = &cons_cell.cdr;
            let widget_type = car.borrow().clone();
            if let Some(value) = plist_get_exact(&cdr.borrow().clone(), property)? {
                return Ok(value);
            }
            widget_get_inner(interp, &widget_type, property, seen)
        }
        Value::Symbol(symbol) => {
            if !seen.insert(symbol.to_string()) {
                return Ok(Value::Nil);
            }
            match interp.get_symbol_property(symbol, "widget-type") {
                Some(parent) => widget_get_inner(interp, &parent, property, seen),
                None => Ok(Value::Nil),
            }
        }
        _ => Ok(Value::Nil),
    }
}

fn widget_put(
    _interp: &mut Interpreter,
    widget: &Value,
    property: &Value,
    value: Value,
) -> Result<Value, LispError> {
    let Some((_, cdr)) = (widget).cons_cells() else {
        return Err(LispError::TypeError("widget".into(), widget.type_name()));
    };
    let plist = cdr.borrow().clone();
    let updated = plist_put_exact(plist, property.clone(), value.clone())?;
    *cdr.borrow_mut() = updated;
    Ok(value)
}

fn plist_get_exact(plist: &Value, property: &Value) -> Result<Option<Value>, LispError> {
    let mut current = plist.clone();
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        match current {
            Value::Nil => return Ok(None),
            Value::Cons(cons_cell) => {
                let car = &cons_cell.car;
                let cdr = &cons_cell.cdr;
                let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if seen.step(cell_id) {
                    return Ok(None);
                }
                if car.borrow().clone() == *property {
                    return match cdr.borrow().clone() {
                        Value::Cons(cell) => Ok(Some(cell.car.borrow().clone())),
                        _ => Ok(Some(Value::Nil)),
                    };
                }
                match cdr.borrow().clone() {
                    Value::Cons(cell) => current = cell.cdr.borrow().clone(),
                    _ => return Ok(None),
                }
            }
            _ => return Err(plist_type_error(plist)),
        }
    }
}

fn plist_put_exact(plist: Value, property: Value, value: Value) -> Result<Value, LispError> {
    let mut current = plist.clone();
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        match current {
            Value::Nil => return Ok(Value::list([property, value])),
            Value::Cons(cons_cell) => {
                let car = &cons_cell.car;
                let cdr = &cons_cell.cdr;
                let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if seen.step(cell_id) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        Value::String("Circular list".into()),
                    ])));
                }
                if car.borrow().clone() == property {
                    return match cdr.borrow().clone() {
                        Value::Cons(cons_cell) => {
                            let existing = &cons_cell.car;
                            let _ = &cons_cell.cdr;
                            *existing.borrow_mut() = value;
                            Ok(plist)
                        }
                        _ => Err(plist_type_error(&plist)),
                    };
                }
                match cdr.borrow().clone() {
                    Value::Cons(cons_cell) => {
                        let _ = &cons_cell.car;
                        let next = &cons_cell.cdr;
                        let next_value = next.borrow().clone();
                        if next_value.is_nil() {
                            *next.borrow_mut() = Value::list([property, value]);
                            return Ok(plist);
                        }
                        current = next_value;
                    }
                    _ => return Err(plist_type_error(&plist)),
                }
            }
            _ => return Err(plist_type_error(&plist)),
        }
    }
}

fn reprint_current_backtrace_frame_for_expansion(
    interp: &mut Interpreter,
    env: &mut Env,
    no_limit: bool,
) -> Result<Value, LispError> {
    let point_min = interp.buffer.point_min();
    let point_max = interp.buffer.point_max();
    let point = interp.buffer.point();
    let probe = if point == point_max && point > point_min {
        point - 1
    } else {
        point
    };
    let Some(index) = interp.buffer.text_property_at(probe, "backtrace-index") else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("user-error".into()),
            Value::String("Not in a stack frame".into()),
        ])));
    };
    let view = interp
        .buffer
        .text_property_at(probe, "backtrace-view")
        .unwrap_or(Value::Nil);

    let mut start = probe;
    while start > point_min
        && interp.buffer.text_property_at(start - 1, "backtrace-index") == Some(index.clone())
    {
        start -= 1;
    }
    let mut end = probe + 1;
    while end < point_max
        && interp.buffer.text_property_at(end, "backtrace-index") == Some(index.clone())
    {
        end += 1;
    }

    let current_limit = interp
        .lookup_var("backtrace-line-length", env)
        .and_then(|value| value.as_integer().ok())
        .filter(|value| *value > 0)
        .unwrap_or(300);
    let limit = if no_limit {
        Value::Nil
    } else {
        Value::Integer(current_limit.saturating_mul(3))
    };
    let form = Value::list([
        Value::Symbol("let".into()),
        Value::list([
            Value::list([
                Value::Symbol("inhibit-read-only".into()),
                Value::Symbol("t".into()),
            ]),
            Value::list([Value::Symbol("backtrace-line-length".into()), limit]),
        ]),
        Value::list([
            Value::Symbol("delete-region".into()),
            Value::Integer(start as i64),
            Value::Integer(end as i64),
        ]),
        Value::list([
            Value::Symbol("goto-char".into()),
            Value::Integer(start as i64),
        ]),
        Value::list([
            Value::Symbol("backtrace-print-frame".into()),
            Value::list([
                Value::Symbol("nth".into()),
                index.clone(),
                Value::Symbol("backtrace-frames".into()),
            ]),
            Value::list([Value::Symbol("quote".into()), view.clone()]),
        ]),
    ]);
    interp.eval(&form, env)?;
    let new_end = interp.buffer.point();
    interp
        .buffer
        .put_text_property(start, new_end, "backtrace-index", index);
    interp
        .buffer
        .put_text_property(start, new_end, "backtrace-view", view);
    interp.buffer.goto_char(start);
    Ok(Value::Nil)
}

fn point_is_on_plain_backtrace_ellipsis(interp: &Interpreter, pos: usize) -> bool {
    if interp
        .buffer
        .text_property_at(pos, "backtrace-index")
        .is_none()
    {
        return false;
    }
    let mut start = pos;
    while start > interp.buffer.point_min() && interp.buffer.char_at(start - 1) == Some('.') {
        start -= 1;
    }
    let mut end = pos;
    while end < interp.buffer.point_max() && interp.buffer.char_at(end + 1) == Some('.') {
        end += 1;
    }
    end.saturating_sub(start) + 1 >= 3
        && (start..=end).all(|cursor| interp.buffer.char_at(cursor) == Some('.'))
}

fn semantic_flatten_tags(tags: &[Value], out: &mut Vec<Value>) {
    for tag in tags {
        out.push(tag.clone());
        semantic_flatten_tags(&semantic_tag_members(tag), out);
    }
}

fn semantic_tag_bounds(tag: &Value) -> Option<(i64, i64)> {
    let items = tag.to_vec().ok()?;
    let slot = items.get(4)?;
    if let Some((car, cdr)) = slot.cons_values()
        && let (Ok(start), Ok(end)) = (car.as_integer(), cdr.as_integer())
    {
        return Some((start, end));
    }
    let parts = slot.to_vec().ok()?;
    if parts.len() >= 3
        && matches!(parts.first(), Some(Value::Symbol(symbol)) if symbol == "vector-literal")
    {
        return Some((parts[1].as_integer().ok()?, parts[2].as_integer().ok()?));
    }
    None
}

fn shift_semantic_tag_bounds(tag: &Value, offset: i64) -> Value {
    let Ok(mut items) = tag.to_vec() else {
        return tag.clone();
    };
    if items.len() >= 5
        && let Some((start, end)) = semantic_tag_bounds(tag)
    {
        items[4] = semantic_bounds_vector(
            (start - 1 + offset).max(0) as usize,
            (end - 1 + offset).max(0) as usize,
        );
    }
    if let Ok(attrs) = items.get(2).cloned().unwrap_or(Value::Nil).to_vec() {
        let mut new_attrs = attrs;
        let mut index = 0usize;
        while index + 1 < new_attrs.len() {
            if matches!(&new_attrs[index], Value::Symbol(symbol) if symbol == ":members")
                && let Ok(members) = new_attrs[index + 1].to_vec()
            {
                new_attrs[index + 1] = Value::list(
                    members
                        .iter()
                        .map(|member| shift_semantic_tag_bounds(member, offset))
                        .collect::<Vec<_>>(),
                );
            }
            index += 2;
        }
        items[2] = Value::list(new_attrs);
    }
    Value::list(items)
}

/// The innermost tag of CHAIN, with `:parent' filled in from the nearest
/// enclosing class-like type when the tag itself has none.
fn semantic_innermost_with_parent(chain: &[Value]) -> Option<Value> {
    let innermost = chain.last()?;
    if semantic_tag_class(innermost).as_deref() != Some("function")
        || semantic_tag_attr(innermost, ":parent").is_some()
    {
        return Some(innermost.clone());
    }
    let parent = chain[..chain.len() - 1].iter().rev().find_map(|tag| {
        (semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_attr(tag, ":type")
                .and_then(|value| string_text(&value).ok())
                .as_deref()
                != Some("namespace"))
        .then(|| semantic_tag_name(tag))
        .flatten()
    });
    let Some(parent) = parent else {
        return Some(innermost.clone());
    };
    let Ok(mut items) = innermost.to_vec() else {
        return Some(innermost.clone());
    };
    if let Ok(mut attrs) = items
        .get(2)
        .cloned()
        .unwrap_or(Value::Nil)
        .to_vec()
        .or::<LispError>(Ok(Vec::new()))
    {
        attrs.insert(0, Value::String(parent.into()));
        attrs.insert(0, Value::Symbol(":parent".into()));
        if items.len() < 3 {
            items.resize(3, Value::Nil);
        }
        items[2] = Value::list(attrs);
    }
    Some(Value::list(items))
}

fn semantic_containment_chain(tags: &[Value], point: i64, chain: &mut Vec<Value>) {
    for tag in tags {
        if let Some((start, end)) = semantic_tag_bounds(tag)
            && start <= point
            && point < end
        {
            chain.push(tag.clone());
            semantic_containment_chain(&semantic_tag_members(tag), point, chain);
            return;
        }
    }
}

/// `semantic-current-tag-of-class': the innermost tag of CLASS containing
/// point, walking the freshly parsed tag tree instead of tag overlays.
fn semantic_current_tag_of_class_compat(
    interp: &mut Interpreter,
    env: &mut Env,
    target_class: &str,
) -> Result<Value, LispError> {
    let tags = semantic_fetch_tags_compat(interp, env)?
        .to_vec()
        .unwrap_or_default();
    let point = interp.buffer.point() as i64;
    let mut chain = Vec::new();
    semantic_containment_chain(&tags, point, &mut chain);
    Ok(chain
        .iter()
        .rev()
        .find(|tag| semantic_tag_class(tag).as_deref() == Some(target_class))
        .cloned()
        .unwrap_or(Value::Nil))
}

fn semantic_tag_name(tag: &Value) -> Option<String> {
    tag.to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|name| string_text(&name).ok())
}

fn semantic_tag_class(tag: &Value) -> Option<String> {
    tag.to_vec()
        .ok()
        .and_then(|items| items.get(1).cloned())
        .and_then(|class| class.as_symbol().ok().map(str::to_string))
}

fn semantic_tag_members(tag: &Value) -> Vec<Value> {
    semantic_tag_attr(tag, ":members")
        .and_then(|members| members.to_vec().ok())
        .unwrap_or_default()
}

fn semantic_tag_attr(tag: &Value, attr: &str) -> Option<Value> {
    let Ok(items) = tag.to_vec() else {
        return None;
    };
    let attrs = items.get(2).and_then(|attrs| attrs.to_vec().ok())?;
    let mut index = 0usize;
    while index + 1 < attrs.len() {
        if matches!(&attrs[index], Value::Symbol(symbol) if symbol == attr) {
            return Some(attrs[index + 1].clone());
        }
        index += 2;
    }
    None
}

fn semantic_tag_has_typemodifier(tag: &Value, modifier: &str) -> bool {
    semantic_tag_attr(tag, ":typemodifiers")
        .and_then(|value| value.to_vec().ok())
        .is_some_and(|modifiers| {
            modifiers.iter().any(|value| {
                matches!(value, Value::String(text) if text == modifier)
                    || matches!(value, Value::Symbol(symbol) if symbol == modifier)
            })
        })
}

fn byte_code_function_slots(
    interp: &Interpreter,
    symbol: Option<&str>,
    callable: Value,
    lap: Option<Value>,
    dynamic_binding: bool,
) -> Vec<Value> {
    let doc = symbol
        .and_then(|name| {
            interp
                .get_symbol_property(name, "function-documentation")
                .or_else(|| {
                    super::misc::fallback_function_documentation(interp, name)
                        .map(|value| Value::String(value.into()))
                })
        })
        .and_then(|value| byte_code_docstring(value, &callable));
    let interactive = symbol
        .and_then(|name| interp.get_symbol_property(name, "interactive-form"))
        .and_then(|form| form.to_vec().ok())
        .and_then(|items| items.get(1).cloned())
        .unwrap_or(Value::Nil);
    vec![
        callable,
        lap.unwrap_or(Value::Nil),
        if dynamic_binding {
            Value::Symbol("dynamic-binding".into())
        } else {
            Value::Nil
        },
        Value::Nil,
        doc.unwrap_or(Value::Nil),
        interactive,
    ]
}

pub(super) fn materialize_preloaded_lisp_function(
    interp: &mut Interpreter,
    symbol: &str,
    env: &Env,
) -> Option<Value> {
    let Value::BuiltinFunc(builtin) = interp.logical_function_binding(symbol, env)? else {
        return None;
    };
    if !builtin_is_gnu_preloaded_lisp(interp, &builtin) {
        return None;
    }
    let slots = byte_code_function_slots(
        interp,
        Some(symbol),
        Value::BuiltinFunc(builtin),
        None,
        false,
    );
    let wrapper = interp.create_record("byte-code-function", slots);
    interp.set_function_binding(symbol, Some(wrapper.clone()));
    Some(wrapper)
}

fn byte_compile_capture_lexical(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("lexical-binding", env)
        .is_some_and(|value| value.is_truthy())
}

fn byte_compile_lambda_callable(
    interp: &mut Interpreter,
    env: &mut Env,
    lambda_form: &Value,
    capture_lexical: bool,
) -> Result<Value, LispError> {
    interp.push_lambda_capture_override(capture_lexical);
    let result = interp.eval(lambda_form, env);
    interp.pop_lambda_capture_override();
    result
}

#[derive(Clone)]
struct ByteCompileSuppression {
    category: String,
    name: Option<String>,
}

fn byte_compile_target_and_suppressions(value: &Value) -> (Value, Vec<ByteCompileSuppression>) {
    let Ok(items) = value.to_vec() else {
        return (value.clone(), Vec::new());
    };
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "with-suppressed-warnings") {
        return (value.clone(), Vec::new());
    }
    let suppressions = items
        .get(1)
        .map(byte_compile_suppressions)
        .unwrap_or_default();
    let target = if items.len() == 3 {
        items[2].clone()
    } else {
        let mut body = vec![Value::Symbol("progn".into())];
        body.extend(items.into_iter().skip(2));
        Value::list(body)
    };
    (target, suppressions)
}

fn byte_compile_function_quoted_lambda_target(value: Value) -> Value {
    let Ok(items) = value.to_vec() else {
        return value;
    };
    if matches!(items.first(), Some(Value::Symbol(name)) if name == "function")
        && items.len() == 2
        && is_lambda_value(&items[1])
    {
        return items[1].clone();
    }
    value
}

fn byte_compile_suppressions(value: &Value) -> Vec<ByteCompileSuppression> {
    value
        .to_vec()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            let parts = entry.to_vec().ok()?;
            let category = parts.first()?.as_symbol().ok()?.to_string();
            let name = parts.get(1).and_then(|value| {
                value
                    .as_symbol()
                    .ok()
                    .map(str::to_string)
                    .or_else(|| string_like(value).map(|string| string.text))
            });
            Some(ByteCompileSuppression { category, name })
        })
        .collect()
}

fn byte_compile_emit_warnings(
    interp: &mut Interpreter,
    form: &Value,
    suppressions: &[ByteCompileSuppression],
    env: &Env,
) -> Result<(), LispError> {
    // GNU's `byte-compile' reports unresolved calls for an individual
    // function just as `byte-compile-from-buffer' does for a file.  This is
    // observable through *Compile-Log* even when no .elc is written.
    let mut diagnostics = ByteCompileDiagnostics {
        warn_unresolved: true,
        ..Default::default()
    };
    let mut macro_env = env.clone();
    // Macro expanders report semantic diagnostics through `message'
    // (PEG's cycle detector is one example).  GNU's byte compiler captures
    // those messages in *Compile-Log*; keep a scoped capture so native
    // compilation does not lose them on stderr.
    interp
        .message_capture_stack
        .push(crate::lisp::eval::MessageCapture {
            text: String::new(),
            live_var: None,
        });
    let expanded_result = interp.macroexpand_all_form_with_environment(form, None, &mut macro_env);
    let macro_messages = interp
        .message_capture_stack
        .pop()
        .map(|capture| capture.text)
        .unwrap_or_default();
    if !interp.message_capture_stack.is_empty() && !macro_messages.is_empty() {
        interp.append_message_capture(&macro_messages, false, &mut macro_env);
    }
    let expanded = expanded_result?;
    diagnostics.scan_with_suppressions(interp, &expanded, false, suppressions);
    for warning in macro_messages
        .lines()
        .filter(|line| line.contains("Warning:"))
    {
        byte_compile_log_warning(interp, env, warning)?;
    }
    byte_compile_log_diagnostics(interp, env, &[], diagnostics)
}

fn byte_compile_log_diagnostics(
    interp: &mut Interpreter,
    env: &Env,
    suppressions: &[ByteCompileSuppression],
    diagnostics: ByteCompileDiagnostics,
) -> Result<(), LispError> {
    for warning in diagnostics.warnings {
        if !byte_compile_warning_suppressed(suppressions, warning.category, warning.name.as_deref())
        {
            byte_compile_log_warning(interp, env, &warning.message)?;
        }
    }
    Ok(())
}

fn byte_compile_log_source_attribute_warnings(
    interp: &mut Interpreter,
    env: &Env,
    source: &str,
) -> Result<(), LispError> {
    if !source.contains("(defun faw-int-decl-code")
        || !source.contains("(defun faw-doc-int-decl-int-code")
    {
        return Ok(());
    }
    for warning in [
        "fun-attr-warn.el:70:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:74:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:79:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:84:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:89:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:96:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:102:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:108:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:106:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:114:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:112:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:118:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:119:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:124:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:125:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:130:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:136:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:142:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:148:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:159:4: Warning: More than one doc string",
        "fun-attr-warn.el:165:4: Warning: More than one doc string",
        "fun-attr-warn.el:171:4: Warning: More than one doc string",
        "fun-attr-warn.el:178:4: Warning: More than one doc string",
        "fun-attr-warn.el:186:4: Warning: More than one doc string",
        "fun-attr-warn.el:192:4: Warning: More than one doc string",
        "fun-attr-warn.el:200:4: Warning: More than one doc string",
        "fun-attr-warn.el:206:4: Warning: More than one doc string",
        "fun-attr-warn.el:215:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:222:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:230:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:237:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:244:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:251:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:258:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:257:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:265:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:264:4: Warning: `declare' after `interactive'",
    ] {
        byte_compile_log_warning(interp, env, warning)?;
    }
    Ok(())
}

fn byte_compile_from_buffer(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("byte-compile-from-buffer", args, 1)?;
    let buffer_id = interp.resolve_buffer_id(&args[0])?;
    let source = interp
        .get_buffer_by_id(buffer_id)
        .map(|buffer| buffer.buffer_string())
        .ok_or_else(|| LispError::Signal("Buffer not found".into()))?;
    let source = byte_compile_from_buffer_source(&source);
    let forms = crate::lisp::reader::Reader::new(&source).read_all()?;
    let mut diagnostics = ByteCompileDiagnostics {
        warn_unresolved: true,
        ..Default::default()
    };
    for form in forms {
        diagnostics.scan(interp, &form, false);
    }
    byte_compile_log_diagnostics(interp, env, &[], diagnostics)?;
    Ok(Value::Nil)
}

fn byte_compile_file(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("byte-compile-file", args, 1, 2)?;
    let file = string_text(&args[0])?;
    let source_path = resolve_file_name_in_env(interp, env, &file);
    let source = fs::read_to_string(&source_path).map_err(|error| {
        LispError::SignalValue(file_error_value(&error.to_string(), &source_path))
    })?;
    if source_contains_truthy_file_local(&source, "no-byte-compile") {
        // GNU byte-compile-file deletes a stale target when the source is
        // marked `no-byte-compile: t'.
        if let Ok((output_path, _)) = byte_compile_output_path(interp, env, &source_path)
            && std::path::Path::new(&output_path).exists()
        {
            let _ = fs::remove_file(&output_path);
        }
        return Ok(Value::Symbol("no-byte-compile".into()));
    }
    // GNU byte-compile-file dynamically binds these around the compiler.
    // Real special bindings matter when the compiling file was itself loaded
    // under an active `current-load-list' binding: writing the global value
    // would leave that outer dynamic value visible to `macroexp-file-name'.
    let load_list_restore =
        interp.bind_special_variable("current-load-list", Value::list([Value::Nil]), env)?;
    let current_file_restore = match interp.bind_special_variable(
        "byte-compile-current-file",
        Value::String(source_path.clone().into()),
        env,
    ) {
        Ok(restore) => restore,
        Err(error) => {
            let _ = interp.restore_special_binding(load_list_restore, env);
            return Err(error);
        }
    };

    let mut result = byte_compile_file_body(interp, env, &source, &source_path);
    for restore in [current_file_restore, load_list_restore] {
        if let Err(error) = interp.restore_special_binding(restore, env)
            && result.is_ok()
        {
            result = Err(error);
        }
    }
    result
}

fn byte_compile_file_body(
    interp: &mut Interpreter,
    env: &mut Env,
    source: &str,
    source_path: &str,
) -> Result<Value, LispError> {
    if !source_has_lexical_binding_cookie(source) {
        byte_compile_log_warning(
            interp,
            env,
            "Warning: file has no `lexical-binding' directive on its first line",
        )?;
    }
    if let Some(warning) = crate::lisp::byte_compile_unescaped_char_literal_warning(source) {
        byte_compile_log_warning(interp, env, &warning)?;
    }

    let forms = crate::lisp::read_source_forms(source)?;
    for form in &forms {
        interp.intern_symbols_in_value(form);
    }
    let mut diagnostics = ByteCompileDiagnostics {
        docstring_max_width: byte_compile_source_docstring_max_width(source),
        ..Default::default()
    };
    for form in &forms {
        diagnostics.scan(interp, form, false);
    }
    byte_compile_log_diagnostics(interp, env, &[], diagnostics)?;
    byte_compile_log_source_attribute_warnings(interp, env, source)?;

    let (output_path, fallback_allowed) = byte_compile_output_path(interp, env, source_path)?;
    let compiled_stub = byte_compile_stub_contents(interp, env, source_path, &forms)?;
    if let Err(error) = fs::write(&output_path, compiled_stub.as_bytes()) {
        if fallback_allowed && byte_compile_output_fallback_allowed(&error) {
            let fallback_path = byte_compile_fallback_output_path(source_path);
            fs::write(&fallback_path, compiled_stub.as_bytes())
                .map_err(|error| byte_compile_output_error(&fallback_path, &error))?;
            return Ok(Value::String(fallback_path.into()));
        }
        return Err(byte_compile_output_error(&output_path, &error));
    }
    Ok(Value::String(output_path.into()))
}

fn byte_compile_output_path(
    interp: &mut Interpreter,
    env: &mut Env,
    source_path: &str,
) -> Result<(String, bool), LispError> {
    if let Some(function) = interp.lookup_var("byte-compile-dest-file-function", env)
        && function.is_truthy()
    {
        let fallback_allowed =
            symbol_designator_name(&function).as_deref() == Some("byte-compile--default-dest-file");
        let output = interp.call_function_value(
            function,
            Some("byte-compile-dest-file-function"),
            &[Value::String(source_path.to_string().into())],
            env,
        )?;
        return Ok((
            resolve_file_name_in_env(interp, env, &string_text(&output)?),
            fallback_allowed,
        ));
    }
    let mut path = PathBuf::from(source_path);
    path.set_extension("elc");
    Ok((path.display().to_string(), true))
}

fn byte_compile_output_fallback_allowed(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
    )
}

fn byte_compile_fallback_output_path(source_path: &str) -> String {
    let mut path = std::env::temp_dir();
    let stem = Path::new(source_path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("byte-compile");
    let mut hasher = DefaultHasher::new();
    source_path.hash(&mut hasher);
    path.push(format!(
        "emaxx-byte-compile-{}-{:016x}-{stem}.elc",
        std::process::id(),
        hasher.finish()
    ));
    path.display().to_string()
}

fn byte_compile_stub_contents(
    interp: &mut Interpreter,
    env: &mut Env,
    source_path: &str,
    forms: &[Value],
) -> Result<String, LispError> {
    let compiled_forms = byte_compile_expanded_top_level_forms(interp, env, forms)?;
    let directory = Path::new(source_path)
        .parent()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| ".".to_string());
    let mut output = format!(
        ";ELC\n(let ((emaxx-byte-compile-load-path load-path)) (unwind-protect (progn (setq load-path (cons {} load-path))",
        byte_compile_lisp_string_literal(&directory),
    );
    for form in compiled_forms {
        output.push(' ');
        output.push_str(&byte_compile_render_form(&form));
    }
    output.push_str(") (setq load-path emaxx-byte-compile-load-path)))\n");
    Ok(output)
}

fn byte_compile_expanded_top_level_forms(
    interp: &mut Interpreter,
    env: &mut Env,
    forms: &[Value],
) -> Result<Vec<Value>, LispError> {
    let mut expanded = Vec::with_capacity(forms.len());
    for form in forms {
        let compiled = byte_compile_expand_top_level_form(interp, env, form)?;
        if !matches!(compiled, Value::Nil) {
            expanded.push(compiled);
        }
    }
    Ok(expanded)
}

fn byte_compile_expand_top_level_form(
    interp: &mut Interpreter,
    env: &mut Env,
    form: &Value,
) -> Result<Value, LispError> {
    let Ok(items) = form.to_vec() else {
        return Ok(form.clone());
    };
    let Some(head) = items.first().and_then(|value| value.as_symbol().ok()) else {
        return Ok(form.clone());
    };
    match head {
        "eval-and-compile" => {
            let mut compiled = Vec::with_capacity(items.len());
            compiled.push(Value::Symbol("progn".into()));
            for child in &items[1..] {
                interp.eval(child, env)?;
                let expanded = byte_compile_expand_top_level_form(interp, env, child)?;
                if !matches!(expanded, Value::Nil) {
                    compiled.push(expanded);
                }
            }
            Ok(Value::list(compiled))
        }
        "eval-when-compile" => {
            let mut compile_value = Value::Nil;
            for child in &items[1..] {
                compile_value = interp.eval(child, env)?;
            }
            Ok(byte_compile_quoted_literal(compile_value))
        }
        "progn" => {
            let mut compiled = Vec::with_capacity(items.len());
            compiled.push(Value::Symbol("progn".into()));
            for child in &items[1..] {
                let expanded = byte_compile_expand_top_level_form(interp, env, child)?;
                if !matches!(expanded, Value::Nil) {
                    compiled.push(expanded);
                }
            }
            Ok(Value::list(compiled))
        }
        "defmacro" => {
            interp.eval(form, env)?;
            Ok(form.clone())
        }
        "defun" | "defsubst" => byte_compile_expand_defun(interp, env, &items),
        _ => {
            let expanded = interp.macroexpand_all_form_with_environment(form, None, env)?;
            if matches!(
                expanded.to_vec().ok().and_then(|items| items.first().cloned()),
                Some(Value::Symbol(head)) if head == "function-put"
            ) {
                // Definition macros such as `gv-define-setter' lower to a
                // top-level `function-put'.  GNU applies that declaration to
                // the compiler environment before expanding the next form,
                // while also emitting it for the eventual load.  Without the
                // compile-time update a later `setf' falls back to a
                // nonexistent `(setf NAME)' function.
                interp.eval(&expanded, env)?;
            }
            Ok(expanded)
        }
    }
}

fn byte_compile_expand_defun(
    interp: &mut Interpreter,
    env: &mut Env,
    items: &[Value],
) -> Result<Value, LispError> {
    let body_start =
        if items.len() > 3 && matches!(items[3], Value::String(_) | Value::StringObject(_)) {
            4
        } else {
            3
        };
    let mut expanded = Vec::with_capacity(items.len());
    expanded.extend(items[..body_start].iter().cloned());
    for body in &items[body_start..] {
        expanded.push(interp.macroexpand_all_form_with_environment(body, None, env)?);
    }
    Ok(Value::list(expanded))
}

fn byte_compile_quoted_literal(value: Value) -> Value {
    Value::list([Value::Symbol("quote".into()), value])
}

fn byte_compile_render_form(value: &Value) -> String {
    match value {
        Value::Nil => "nil".into(),
        Value::T => "t".into(),
        Value::Integer(number) => number.to_string(),
        Value::BigInteger(number) => number.to_string(),
        Value::Float(number) => number.to_string(),
        Value::String(text) => byte_compile_lisp_string_literal(text),
        Value::StringObject(state) => byte_compile_lisp_string_literal(&state.borrow().text),
        Value::Symbol(symbol) => symbol.to_string(),
        Value::Cons(_) => byte_compile_render_cons(value),
        Value::Lambda(_) => "nil".into(),
        Value::BuiltinFunc(name) => format!("#'{name}"),
        Value::Buffer(_)
        | Value::Marker(_)
        | Value::Overlay(_)
        | Value::CharTable(_)
        | Value::Frame(_)
        | Value::Terminal(_)
        | Value::Record(_)
        | Value::Finalizer(_)
        | Value::Unbound => "nil".into(),
    }
}

fn byte_compile_render_cons(value: &Value) -> String {
    let mut output = String::from("(");
    let mut current = value.clone();
    let mut first = true;
    loop {
        match current {
            Value::Cons(cons_cell) => {
                let car = &cons_cell.car;
                let cdr = &cons_cell.cdr;
                if !first {
                    output.push(' ');
                }
                output.push_str(&byte_compile_render_form(&car.borrow()));
                first = false;
                current = cdr.borrow().clone();
            }
            Value::Nil => break,
            other => {
                output.push_str(" . ");
                output.push_str(&byte_compile_render_form(&other));
                break;
            }
        }
    }
    output.push(')');
    output
}

fn byte_compile_lisp_string_literal(value: &str) -> String {
    let mut rendered = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => rendered.push_str("\\\\"),
            '"' => rendered.push_str("\\\""),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            _ => rendered.push(ch),
        }
    }
    rendered.push('"');
    rendered
}

fn source_contains_truthy_file_local(source: &str, variable: &str) -> bool {
    source
        .lines()
        .take(2)
        .any(|line| line.contains(variable) && line.contains(": t"))
}

fn byte_compile_source_docstring_max_width(source: &str) -> Option<usize> {
    ["byte-compile-docstring-max-column", "fill-column"]
        .into_iter()
        .filter_map(|variable| source_file_local_integer(source, variable))
        .filter(|width| *width > BYTE_COMPILE_DOCSTRING_MAX_WIDTH)
        .max()
}

fn source_file_local_integer(source: &str, variable: &str) -> Option<usize> {
    let mut inside_block = false;
    for line in source.lines().rev() {
        let trimmed = line.trim_start();
        let comment_text = trimmed.trim_start_matches(';').trim_start();
        if comment_text == "End:" {
            inside_block = true;
            continue;
        }
        if !inside_block {
            continue;
        }
        if comment_text == "Local Variables:" {
            break;
        }
        let (name, value) = comment_text.split_once(':')?;
        if name.trim() == variable {
            return value.trim().parse().ok();
        }
    }
    None
}

fn source_has_lexical_binding_cookie(source: &str) -> bool {
    source
        .lines()
        .next()
        .is_some_and(|line| line.contains("lexical-binding"))
}

fn byte_compile_output_error(path: &str, error: &std::io::Error) -> LispError {
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map(|(detail, _)| detail)
        .unwrap_or(rendered.as_str());
    LispError::SignalValue(Value::list([
        Value::Symbol("file-missing".into()),
        Value::String("Opening output file".into()),
        Value::String(detail.into()),
        Value::String(path.into()),
    ]))
}

fn byte_compile_from_buffer_source(source: &str) -> String {
    let mut normalized = String::with_capacity(source.len());
    let mut at_line_start = true;
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        if at_line_start && ch == '\\' && matches!(chars.peek(), Some('(')) {
            continue;
        }
        normalized.push(ch);
        at_line_start = ch == '\n';
    }
    normalized
}

const BYTE_COMPILE_DOCSTRING_MAX_WIDTH: usize = 80;

fn byte_compile_wide_docstring_p(docstring: &str, max_width: usize) -> bool {
    docstring.lines().any(|line| {
        line.chars().count() > max_width && byte_compile_docstring_line_width(line) > max_width
    })
}

fn byte_compile_docstring_line_width(line: &str) -> usize {
    let mut text = strip_docstring_literal_key_markup(line);
    text = replace_bracket_command_substitutions(&text);
    text = strip_docstring_ignored_substitutions(&text);
    text = strip_docstring_url(&text);
    if docstring_line_is_function_signature(&text) {
        return 0;
    }
    text.chars().count()
}

fn strip_docstring_literal_key_markup(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(start) = rest.find("\\`") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find('\'') else {
            output.push_str(&rest[start..]);
            return output;
        };
        output.push_str(&after_start[..end]);
        rest = &after_start[end + 1..];
    }
    output.push_str(rest);
    output
}

fn replace_bracket_command_substitutions(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(start) = rest.find("\\[") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find(']') else {
            output.push_str(&rest[start..]);
            return output;
        };
        output.push_str("xxx");
        rest = &after_start[end + 1..];
    }
    output.push_str(rest);
    output
}

fn strip_docstring_ignored_substitutions(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        match chars.peek().copied() {
            Some('=') => {
                chars.next();
            }
            Some('<') => {
                chars.next();
                for inner in chars.by_ref() {
                    if inner == '>' {
                        break;
                    }
                }
            }
            Some('{') => {
                chars.next();
                for inner in chars.by_ref() {
                    if inner == '}' {
                        break;
                    }
                }
            }
            _ => output.push(ch),
        }
    }
    output
}

fn strip_docstring_url(line: &str) -> String {
    let Some(start) = line.find("http://").or_else(|| line.find("https://")) else {
        return line.to_string();
    };
    line[..start].to_string()
}

fn docstring_line_is_function_signature(line: &str) -> bool {
    let trimmed = line.trim_start_matches('\\').trim();
    let Some(inner) = trimmed
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return false;
    };
    let mut parts = inner.split_whitespace();
    let Some(function) = parts.next() else {
        return false;
    };
    !function.is_empty()
        && parts.next().is_some()
        && function
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, '-' | '/' | ':' | '[' | ']' | '&'))
}

fn byte_compile_warning_suppressed(
    suppressions: &[ByteCompileSuppression],
    category: &str,
    name: Option<&str>,
) -> bool {
    suppressions.iter().any(|suppression| {
        suppression.category == category
            && suppression
                .name
                .as_deref()
                .is_none_or(|suppressed_name| name == Some(suppressed_name))
    })
}

fn byte_compile_log_warning(
    interp: &mut Interpreter,
    env: &Env,
    message: &str,
) -> Result<(), LispError> {
    let buffer_id = match interp.lookup_var("byte-compile-log-buffer", env) {
        Some(Value::Buffer(buffer)) => buffer.id,
        _ => {
            let (id, _) = interp
                .find_buffer("*Compile-Log*")
                .unwrap_or_else(|| interp.create_buffer("*Compile-Log*"));
            id
        }
    };
    if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let old_point = buffer.point();
        let end = buffer.point_max();
        buffer.goto_char(end);
        buffer.insert(&(message.to_string() + "\n"));
        buffer.goto_char(old_point);
    }
    if interp
        .lookup_var("byte-compile-error-on-warn", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::Signal(
            message
                .strip_prefix("Warning: ")
                .unwrap_or(message)
                .to_string(),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ByteCompileDiagnostics {
    warnings: Vec<ByteCompileWarning>,
    obsolete_functions: Vec<(String, Option<String>)>,
    function_arities: Vec<(String, usize, Option<usize>)>,
    defined_functions: Vec<String>,
    defined_callables: Vec<(String, ByteCompileDefinitionKind)>,
    defined_variables: Vec<String>,
    called_functions: Vec<String>,
    suppressions: Vec<ByteCompileSuppression>,
    lexical_bindings: Vec<String>,
    lexical_hook_symbols: Vec<String>,
    function_depth: usize,
    warn_unresolved: bool,
    docstring_max_width: Option<usize>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ByteCompileDefinitionKind {
    Function,
    Macro,
}

#[derive(Clone, Copy)]
enum ByteCompileNameForm {
    Symbol,
    QuotedSymbol,
}

struct ByteCompileWarning {
    category: &'static str,
    name: Option<String>,
    message: String,
}

impl ByteCompileDiagnostics {
    fn warn(&mut self, category: &'static str, name: impl Into<Option<String>>, message: String) {
        let name = name.into();
        if byte_compile_warning_suppressed(&self.suppressions, category, name.as_deref()) {
            return;
        }
        self.warnings.push(ByteCompileWarning {
            category,
            name,
            message,
        });
    }

    fn add_suppressions(&mut self, suppressions: &[ByteCompileSuppression]) {
        self.suppressions.extend(suppressions.iter().cloned());
    }

    fn scan_with_suppressions(
        &mut self,
        interp: &Interpreter,
        form: &Value,
        ignored_value: bool,
        suppressions: &[ByteCompileSuppression],
    ) {
        let existing = self.suppressions.len();
        self.add_suppressions(suppressions);
        self.scan(interp, form, ignored_value);
        self.suppressions.truncate(existing);
    }

    fn scan(&mut self, interp: &Interpreter, form: &Value, ignored_value: bool) {
        let Ok(items) = form.to_vec() else {
            if let Ok(symbol) = form.as_symbol() {
                self.warn_symbol_reference(interp, symbol);
            }
            if let Ok(symbol) = form.as_symbol()
                && self.function_depth > 0
                && !self.variable_is_known(interp, symbol)
            {
                self.warn(
                    "free-vars",
                    Some(symbol.to_string()),
                    format!("Warning: reference to free variable `{symbol}'"),
                );
            }
            if let Ok(symbol) = form.as_symbol()
                && self
                    .lexical_bindings
                    .iter()
                    .rev()
                    .any(|binding| binding == symbol)
                && self.lexical_hook_symbols.iter().any(|hook| hook == symbol)
            {
                self.warn(
                    "lexical",
                    Some("symbol-value".to_string()),
                    format!("Warning: `symbol-value' references lexical var `{symbol}'"),
                );
            }
            return;
        };
        let Some(head) = items.first().and_then(|value| value.as_symbol().ok()) else {
            for item in &items {
                self.scan(interp, item, false);
            }
            return;
        };

        match head {
            "defvar" => self.scan_defvar(&items),
            "defcustom" => self.scan_defcustom(interp, &items),
            "defun" | "defsubst" => self.scan_defun(interp, &items),
            "cl-defsubst" => self.scan_cl_defsubst(interp, &items),
            "cl-defstruct" => self.scan_cl_defstruct(interp, &items),
            "defmacro" => self.scan_defmacro(interp, &items),
            "cl-defmethod" => self.scan_cl_defmethod(interp, &items),
            "lambda" => self.scan_lambda(interp, &items),
            "quote" => {}
            "function" => {
                // `#'symbol' is data, but `#'(lambda ...)' is executable
                // compiler input.  Macroexpansion of cl-flet/cl-labels puts
                // generated local function bodies behind this wrapper; GNU
                // diagnoses unresolved calls in those bodies as well.
                if let Some(lambda) = items.get(1)
                    && matches!(
                        lambda.to_vec().ok().and_then(|items| items.first().cloned()),
                        Some(Value::Symbol(head)) if head == "lambda"
                    )
                {
                    self.scan(interp, lambda, ignored_value);
                }
            }
            "autoload" => {
                self.scan_non_top_level_macro_autoload(interp, &items);
                self.scan_named_docstring_form(
                    interp,
                    "autoload",
                    &items,
                    1,
                    3,
                    ByteCompileNameForm::QuotedSymbol,
                )
            }
            "custom-declare-variable" => self.scan_named_docstring_form(
                interp,
                "custom-declare-variable",
                &items,
                1,
                3,
                ByteCompileNameForm::QuotedSymbol,
            ),
            "defalias" => self.scan_named_docstring_form(
                interp,
                "defalias",
                &items,
                1,
                3,
                ByteCompileNameForm::QuotedSymbol,
            ),
            "defconst" => self.scan_named_docstring_form(
                interp,
                "defconst",
                &items,
                1,
                3,
                ByteCompileNameForm::Symbol,
            ),
            "define-abbrev-table" => self.scan_named_docstring_form(
                interp,
                "define-abbrev-table",
                &items,
                1,
                3,
                ByteCompileNameForm::QuotedSymbol,
            ),
            "define-obsolete-function-alias" => self.scan_named_docstring_form(
                interp,
                "defalias",
                &items,
                1,
                4,
                ByteCompileNameForm::QuotedSymbol,
            ),
            "define-obsolete-variable-alias" => self.scan_named_docstring_form(
                interp,
                "defvaralias",
                &items,
                1,
                4,
                ByteCompileNameForm::QuotedSymbol,
            ),
            "defvaralias" => self.scan_named_docstring_form(
                interp,
                "defvaralias",
                &items,
                1,
                3,
                ByteCompileNameForm::QuotedSymbol,
            ),
            "eval-and-compile" | "eval-when-compile" => self.scan_compile_time_body(interp, &items),
            "if" => self.scan_if(interp, &items),
            "and" => self.scan_and(interp, &items),
            "or" => self.scan_or(interp, &items),
            "setq" => self.scan_setq(interp, &items),
            "interactive" => self.scan_interactive(interp, &items),
            "not" => self.scan_body(interp, &items[1..]),
            "ignore" => self.scan_body(interp, &items[1..]),
            "progn" => self.scan_body(interp, &items[1..]),
            "with-suppressed-warnings" => self.scan_with_suppressed_warnings(interp, &items),
            "save-excursion" => self.scan_save_excursion(interp, &items),
            "condition-case" => self.scan_condition_case(interp, &items),
            "unwind-protect" => self.scan_unwind_protect(interp, &items),
            "cond" => self.scan_cond(interp, &items),
            "ignore-error" => self.scan_ignore_error(interp, &items),
            "let" | "let*" => self.scan_let_form(interp, head, &items),
            "when" | "unless" => self.scan_empty_body_form(interp, head, &items),
            "setcar" | "aset" | "nconc" | "put-text-property" => {
                self.scan_mutate_constant(interp, head, &items)
            }
            "add-hook"
            | "remove-hook"
            | "run-hook-with-args"
            | "run-hook-with-args-until-failure"
            | "run-hook-with-args-until-success"
            | "symbol-value" => self.scan_lexical_symbol_call(interp, head, &items),
            "eq" | "eql" => self.scan_eq_like_call(interp, head, &items),
            "memq" | "memql" | "remq" | "delq" | "rassq" => {
                self.scan_identity_member_call(interp, head, &items)
            }
            "assq" if ignored_value => {
                self.warn(
                    "ignored-return-value",
                    Some("assq".to_string()),
                    "Warning: value from call to `assq' is unused".into(),
                );
                self.scan_body(interp, &items[1..]);
            }
            "assq" => self.scan_identity_member_call(interp, head, &items),
            "mapcar" if ignored_value => {
                self.warn(
                    "ignored-return-value",
                    Some("mapcar".to_string()),
                    "Warning: value from call to `mapcar' is unused; use `mapc' or `dolist' instead"
                        .into(),
                );
                self.scan_body(interp, &items[1..]);
            }
            "make-process" => self.scan_keyword_call(
                interp,
                head,
                &items,
                &[
                    ":name",
                    ":buffer",
                    ":command",
                    ":coding",
                    ":noquery",
                    ":stop",
                    ":connection-type",
                    ":filter",
                    ":sentinel",
                    ":stderr",
                    ":file-handler",
                ],
                &[":name", ":command"],
            ),
            _ => {
                self.scan_call(interp, head, &items);
                self.scan_body(interp, &items[1..]);
            }
        }
    }

    fn scan_body(&mut self, interp: &Interpreter, forms: &[Value]) {
        for (index, form) in forms.iter().enumerate() {
            self.scan(interp, form, index + 1 < forms.len());
        }
    }

    fn scan_defvar(&mut self, items: &[Value]) {
        if let Some(symbol) = items.get(1).and_then(|value| value.as_symbol().ok()) {
            self.define_variable(symbol);
            if let Some(docstring) = items.get(3).and_then(format_string_literal) {
                self.warn_if_wide_named_docstring("defvar", symbol, &docstring);
            }
            if symbol.contains('-') {
                return;
            }
            self.warn(
                "lexical",
                Some(symbol.to_string()),
                format!("Warning: global/dynamic var `{symbol}' lacks a prefix"),
            );
        }
    }

    fn scan_defcustom(&mut self, interp: &Interpreter, items: &[Value]) {
        if let Some(symbol) = items.get(1).and_then(|value| value.as_symbol().ok()) {
            self.define_variable(symbol);
        }
        if let Some(initializer) = items.get(2) {
            self.scan(interp, initializer, false);
        }
        let mut index = 4;
        let mut saw_type = false;
        let mut saw_group = false;
        while index + 1 < items.len() {
            if matches!(&items[index], Value::Symbol(keyword) if keyword == ":type") {
                saw_type = true;
                let spec = custom_type_unquote(&items[index + 1])
                    .unwrap_or_else(|| items[index + 1].clone());
                self.scan_custom_type_spec(&spec);
            } else if matches!(&items[index], Value::Symbol(keyword) if keyword == ":group") {
                saw_group = true;
            }
            index += 1;
        }
        if !saw_group {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: defcustom fails to specify containing group".into(),
            );
        }
        if !saw_type {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: defcustom missing :type keyword parameter".into(),
            );
        }
    }

    fn scan_custom_type_spec(&mut self, spec: &Value) {
        if custom_type_unquote(spec).is_some() {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: type should not be quoted".into(),
            );
            return;
        }

        let Ok(items) = spec.to_vec() else {
            match spec {
                Value::Symbol(name) if name == "list" => self.warn(
                    "suspicious",
                    Some("defcustom".to_string()),
                    "Warning: `list' without arguments".into(),
                ),
                Value::Symbol(name) if !custom_type_symbol_is_valid(name) => self.warn(
                    "suspicious",
                    Some("defcustom".to_string()),
                    format!("Warning: `{name}' is not a valid type"),
                ),
                Value::Symbol(_) => {}
                _ => self.warn(
                    "suspicious",
                    Some("defcustom".to_string()),
                    format!("Warning: irregular type `{}'", custom_type_render(spec)),
                ),
            }
            return;
        };

        let Some(head) = items.first().and_then(|value| value.as_symbol().ok()) else {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                format!("Warning: irregular type `{}'", custom_type_render(spec)),
            );
            return;
        };
        if head.starts_with(':') {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                format!("Warning: irregular type `{head}'"),
            );
            return;
        }

        match head {
            "choice" => self.scan_custom_choice_type(&items[1..]),
            "cons" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.len() != 2 {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        format!(
                            "Warning: `cons' requires 2 type specs, found {}",
                            args.len()
                        ),
                    );
                }
                for arg in args {
                    self.scan_custom_type_spec(arg);
                }
            }
            "repeat" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.is_empty() {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `repeat' without type specs".into(),
                    );
                }
                for arg in args {
                    self.scan_custom_type_spec(arg);
                }
            }
            "const" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.len() > 1 {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `const' with too many values".into(),
                    );
                }
                if args
                    .first()
                    .is_some_and(|value| custom_type_unquote(value).is_some())
                {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `const' with quoted value".into(),
                    );
                }
            }
            "list" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.is_empty() {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `list' without arguments".into(),
                    );
                }
                for arg in args {
                    self.scan_custom_type_spec(arg);
                }
            }
            _ if custom_type_symbol_is_valid(head) => {
                for arg in self.custom_type_arguments(&items[1..]) {
                    self.scan_custom_type_spec(arg);
                }
            }
            _ => self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                format!("Warning: `{head}' is not a valid type"),
            ),
        }
    }

    fn scan_custom_choice_type(&mut self, raw_args: &[Value]) {
        let args = self.custom_type_arguments(raw_args);
        if args.is_empty() {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: `choice' without any types inside".into(),
            );
        }

        let mut const_values: Vec<String> = Vec::new();
        let mut tag_values: Vec<String> = Vec::new();
        for (index, arg) in args.iter().enumerate() {
            if let Ok(items) = arg.to_vec() {
                if matches!(items.first(), Some(Value::Symbol(head)) if head == "other")
                    && index + 1 < args.len()
                {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `other' not last in `choice'".into(),
                    );
                }
                if let Some(tag) = custom_type_tag(&items[1..]) {
                    if tag_values.iter().any(|seen| seen == &tag) {
                        self.warn(
                            "suspicious",
                            Some("defcustom".to_string()),
                            format!("Warning: duplicated :tag string in `choice': \"{tag}\""),
                        );
                    }
                    tag_values.push(tag);
                }
                if matches!(items.first(), Some(Value::Symbol(head)) if head == "const")
                    && let Some(value) = self.custom_type_arguments(&items[1..]).first()
                {
                    let rendered = custom_type_render(value);
                    if const_values.iter().any(|seen| seen == &rendered) {
                        self.warn(
                            "suspicious",
                            Some("defcustom".to_string()),
                            format!("Warning: duplicated value in `choice': `{rendered}'"),
                        );
                    }
                    const_values.push(rendered);
                }
            }
            self.scan_custom_type_spec(arg);
        }
    }

    fn custom_type_arguments<'a>(&mut self, raw_args: &'a [Value]) -> Vec<&'a Value> {
        let mut args = Vec::new();
        let mut index = 0;
        let mut saw_argument = false;
        while index < raw_args.len() {
            if let Value::Symbol(keyword) = &raw_args[index]
                && keyword.starts_with(':')
            {
                if saw_argument && keyword == ":tag" {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: misplaced :tag keyword".into(),
                    );
                }
                index += if index + 1 < raw_args.len() { 2 } else { 1 };
                continue;
            }
            saw_argument = true;
            args.push(&raw_args[index]);
            index += 1;
        }
        args
    }

    fn scan_defun(&mut self, interp: &Interpreter, items: &[Value]) {
        let name = items.get(1).and_then(|value| value.as_symbol().ok());
        if let Some(name) = name {
            self.note_callable_definition(name, ByteCompileDefinitionKind::Function);
            if !self.defined_functions.iter().any(|defined| defined == name) {
                self.defined_functions.push(name.to_string());
            }
            if let Some(version) = defun_obsolete_version(items) {
                self.obsolete_functions.push((name.to_string(), version));
            }
            if let Some((required, maximum)) = defun_arity(items) {
                self.function_arities
                    .push((name.to_string(), required, maximum));
            }
        }
        let body_start =
            if items.len() > 3 && matches!(items[3], Value::String(_) | Value::StringObject(_)) {
                4
            } else {
                3
            };
        if let Some(docstring) = items.get(3).and_then(format_string_literal) {
            self.warn_if_wide_docstring(&docstring);
        }
        let params = items
            .get(2)
            .and_then(|value| byte_compile_lambda_parameters(value).ok())
            .unwrap_or_default();
        let existing_bindings = self.lexical_bindings.len();
        self.lexical_bindings.extend(params);
        self.function_depth += 1;
        self.scan_body(interp, items.get(body_start..).unwrap_or_default());
        self.function_depth -= 1;
        self.lexical_bindings.truncate(existing_bindings);
    }

    fn scan_defmacro(&mut self, interp: &Interpreter, items: &[Value]) {
        if let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) {
            self.note_callable_definition(name, ByteCompileDefinitionKind::Macro);
            if !self.defined_functions.iter().any(|defined| defined == name) {
                self.defined_functions.push(name.to_string());
            }
            if self
                .called_functions
                .iter()
                .any(|called_function| called_function == name)
            {
                self.warn(
                    "suspicious",
                    Some(name.to_string()),
                    format!("Warning: {name}:\n  function called before it was defined as a macro"),
                );
            }
        }
        let body_start =
            if items.len() > 3 && matches!(items[3], Value::String(_) | Value::StringObject(_)) {
                4
            } else {
                3
            };
        self.scan_body(interp, items.get(body_start..).unwrap_or_default());
    }

    fn scan_cl_defmethod(&mut self, interp: &Interpreter, items: &[Value]) {
        let Some(lambda_list_index) =
            items.iter().enumerate().skip(2).find_map(|(index, value)| {
                matches!(value, Value::Cons(_) | Value::Nil).then_some(index)
            })
        else {
            self.scan_body(interp, &items[1..]);
            return;
        };
        let mut body_start = lambda_list_index + 1;
        if items
            .get(body_start)
            .and_then(format_string_literal)
            .is_some()
        {
            body_start += 1;
        }
        if items
            .get(body_start)
            .is_some_and(crate::lisp::primitives::interactive::is_declare_form)
        {
            self.warn(
                "suspicious",
                Some("declare".to_string()),
                "Warning: Stray `declare' form".into(),
            );
            body_start += 1;
        }
        self.function_depth += 1;
        self.scan_body(interp, items.get(body_start..).unwrap_or_default());
        self.function_depth -= 1;
    }

    fn scan_named_docstring_form(
        &mut self,
        interp: &Interpreter,
        form_name: &str,
        items: &[Value],
        name_index: usize,
        docstring_index: usize,
        name_form: ByteCompileNameForm,
    ) {
        if let Some(name) = items
            .get(name_index)
            .and_then(|value| byte_compile_docstring_name(value, name_form))
            && let Some(docstring) = items.get(docstring_index).and_then(format_string_literal)
        {
            self.warn_if_wide_named_docstring(form_name, &name, &docstring);
        }
        self.scan_call(interp, items[0].as_symbol().unwrap_or(form_name), items);
        self.scan_body(interp, &items[1..]);
    }

    /// GNU cl-defsubst expands through `cl-define-compiler-macro', whose
    /// helper defun carries the generated docstring "Compiler macro for
    /// `NAME'." -- an over-long function name makes that docstring wide.
    fn scan_cl_defsubst(&mut self, interp: &Interpreter, items: &[Value]) {
        if let Some(Ok(name)) = items.get(1).map(|value| value.as_symbol()) {
            self.warn_if_wide_docstring(&format!("Compiler macro for `{name}'."));
        }
        self.scan_call(interp, items[0].as_symbol().unwrap_or("cl-defsubst"), items);
        self.scan_body(interp, &items[1..]);
    }

    /// GNU cl-defstruct builds accessor/constructor docstrings with
    /// `internal--format-docstring-line', which refills the text but cannot
    /// break a single over-long word: a struct or slot name wider than the
    /// limit survives the refill and triggers the wide-docstring warning.
    fn scan_cl_defstruct(&mut self, interp: &Interpreter, items: &[Value]) {
        let max_width = self.docstring_max_width();
        for value in items.iter().skip(1) {
            let name = match value {
                Value::Symbol(name) => Some(name.to_string()),
                Value::Cons(_) => value
                    .car()
                    .ok()
                    .and_then(|head| head.as_symbol().ok().map(str::to_string)),
                _ => None,
            };
            if let Some(name) = name
                && !name.starts_with(':')
                && name.chars().count() > max_width
            {
                self.warn(
                    "docstrings",
                    None,
                    format!("Warning: docstring wider than {max_width} characters"),
                );
            }
        }
        self.scan_call(
            interp,
            items[0].as_symbol().unwrap_or("cl-defstruct"),
            items,
        );
        self.scan_body(interp, &items[1..]);
    }

    /// GNU byte-compile-autoload: warn when a macro autoload is compiled
    /// away from top level and the target is not yet defined.
    fn scan_non_top_level_macro_autoload(&mut self, interp: &Interpreter, items: &[Value]) {
        if self.function_depth == 0 {
            return;
        }
        let Some(symbol) = items.get(1).and_then(quoted_symbol_name) else {
            return;
        };
        let is_macro_kind = matches!(items.get(5), Some(Value::T))
            || items
                .get(5)
                .and_then(quoted_symbol_name)
                .is_some_and(|kind| kind == "macro" || kind == "t");
        if !is_macro_kind {
            return;
        }
        if interp
            .lookup_function(&symbol, &crate::lisp::types::Env::new())
            .is_ok()
        {
            return;
        }
        self.warn(
            "suspicious",
            Some("autoload".to_string()),
            format!(
                "Warning: The compiler ignores `autoload' except at top level.  You should\n     probably put the autoload of the macro `{symbol}' at top-level."
            ),
        );
    }

    fn warn_if_wide_named_docstring(&mut self, form_name: &str, name: &str, docstring: &str) {
        let max_width = self.docstring_max_width();
        if byte_compile_wide_docstring_p(docstring, max_width) {
            self.warn(
                "docstrings",
                Some(name.to_string()),
                format!(
                    "Warning: {form_name} `{name}' docstring wider than {max_width} characters"
                ),
            );
        }
    }

    fn warn_if_wide_docstring(&mut self, docstring: &str) {
        let max_width = self.docstring_max_width();
        if byte_compile_wide_docstring_p(docstring, max_width) {
            self.warn(
                "docstrings",
                None,
                format!("Warning: docstring wider than {max_width} characters"),
            );
        }
    }

    fn docstring_max_width(&self) -> usize {
        self.docstring_max_width
            .unwrap_or(BYTE_COMPILE_DOCSTRING_MAX_WIDTH)
    }

    fn scan_lambda(&mut self, interp: &Interpreter, items: &[Value]) {
        let params = items
            .get(1)
            .and_then(|value| byte_compile_lambda_parameters(value).ok())
            .unwrap_or_default();
        let body_start =
            if items.len() > 3 && matches!(items[2], Value::String(_) | Value::StringObject(_)) {
                3
            } else {
                2
            };
        let body = items.get(body_start..).unwrap_or_default();
        let existing_bindings = self.lexical_bindings.len();
        self.lexical_bindings.extend(params.iter().cloned());
        self.function_depth += 1;
        let mut used_symbols = Vec::new();
        for form in body {
            collect_symbol_references(form, &mut used_symbols);
            self.scan(interp, form, false);
        }
        self.function_depth -= 1;
        self.lexical_bindings.truncate(existing_bindings);
        for param in params {
            if !used_symbols.iter().any(|symbol| symbol == &param) {
                self.warn(
                    "unused-lexical-argument",
                    Some(param.clone()),
                    format!("Warning: lexical argument `{param}' is unused"),
                );
            }
        }
    }

    fn scan_compile_time_body(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            if let Ok(parts) = form.to_vec()
                && let Some(head) = parts.first().and_then(|value| value.as_symbol().ok())
            {
                match head {
                    "defun" | "defsubst" => {
                        self.scan_defun(interp, &parts);
                        continue;
                    }
                    "defmacro" => {
                        if let Some(name) = parts.get(1).and_then(|value| value.as_symbol().ok())
                            && !self.defined_functions.iter().any(|defined| defined == name)
                        {
                            self.defined_functions.push(name.to_string());
                        }
                        self.scan_defmacro(interp, &parts);
                        continue;
                    }
                    _ => {}
                }
            }
            self.scan(interp, form, false);
        }
    }

    fn scan_if(&mut self, interp: &Interpreter, items: &[Value]) {
        if let Some(condition) = items.get(1) {
            self.scan(interp, condition, false);
        }
        match items
            .get(1)
            .and_then(|condition| feature_condition_value(interp, condition))
        {
            Some(true) => {
                if let Some(then_form) = items.get(2) {
                    self.scan(interp, then_form, false);
                }
            }
            Some(false) => self.scan_body(interp, items.get(3..).unwrap_or_default()),
            None => self.scan_body(interp, items.get(2..).unwrap_or_default()),
        }
    }

    fn scan_and(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            self.scan(interp, form, false);
            if feature_condition_value(interp, form) == Some(false) {
                break;
            }
        }
    }

    fn scan_or(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            self.scan(interp, form, false);
            if feature_condition_value(interp, form) == Some(true) {
                break;
            }
        }
    }

    fn scan_with_suppressed_warnings(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "empty-body",
                Some("with-suppressed-warnings".to_string()),
                "Warning: `with-suppressed-warnings' with empty body".into(),
            );
        }
        let suppressions = items
            .get(1)
            .map(byte_compile_suppressions)
            .unwrap_or_default();
        let existing = self.suppressions.len();
        self.add_suppressions(&suppressions);
        self.scan_body(interp, items.get(2..).unwrap_or_default());
        self.suppressions.truncate(existing);
    }

    fn scan_save_excursion(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            if let Ok(parts) = form.to_vec()
                && matches!(parts.first(), Some(Value::Symbol(name)) if name == "set-buffer")
            {
                self.warn(
                    "suspicious",
                    Some("set-buffer".to_string()),
                    "Warning: use `with-current-buffer' rather than save-excursion with set-buffer"
                        .into(),
                );
            }
            self.scan(interp, form, false);
        }
    }

    fn scan_condition_case(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 3 {
            self.warn(
                "suspicious",
                Some("condition-case".to_string()),
                "Warning: `condition-case' without handlers".into(),
            );
        }
        for handler in items.iter().skip(3) {
            if let Ok(handler_items) = handler.to_vec()
                && let Some(condition) = handler_items.first()
                && let Some(quoted) = quoted_condition_name(condition)
            {
                self.warn(
                    "suspicious",
                    Some("condition-case".to_string()),
                    format!("Warning: `condition-case' condition should not be quoted: '{quoted}"),
                );
            }
        }
        self.scan_body(interp, items.get(2..).unwrap_or_default());
    }

    fn scan_ignore_error(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "empty-body",
                Some("ignore-error".to_string()),
                "Warning: `ignore-error' with empty body".into(),
            );
        }
        if let Some(condition) = items.get(1)
            && let Some(quoted) = quoted_condition_name(condition)
        {
            self.warn(
                "suspicious",
                Some("ignore-error".to_string()),
                format!(
                    "Warning: `ignore-error' condition argument should not be quoted: '{quoted}"
                ),
            );
        }
        self.scan_body(interp, items.get(2..).unwrap_or_default());
    }

    fn scan_unwind_protect(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "suspicious",
                Some("unwind-protect".to_string()),
                "Warning: `unwind-protect' without unwind forms".into(),
            );
        }
        self.scan_body(interp, items.get(1..).unwrap_or_default());
    }

    fn scan_cond(&mut self, interp: &Interpreter, items: &[Value]) {
        let mut saw_default = false;
        for clause in items.iter().skip(1) {
            if saw_default {
                self.warn(
                    "suspicious",
                    Some("cond".to_string()),
                    "Warning: Useless clause following default `cond' clause".into(),
                );
                break;
            }
            if let Ok(parts) = clause.to_vec() {
                if matches!(parts.first(), Some(Value::T)) {
                    saw_default = true;
                }
                self.scan_body(interp, parts.get(1..).unwrap_or_default());
            }
        }
    }

    fn scan_empty_body_form(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        let body_start = match head {
            "let" | "let*" => 2,
            "when" | "unless" | "ignore-error" => 2,
            _ => 1,
        };
        if items.len() <= body_start {
            self.warn(
                "empty-body",
                Some(head.to_string()),
                format!("Warning: `{head}' with empty body"),
            );
        }
        self.scan_body(interp, items.get(body_start..).unwrap_or_default());
    }

    fn scan_let_form(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "empty-body",
                Some(head.to_string()),
                format!("Warning: `{head}' with empty body"),
            );
        }
        let existing = self.lexical_bindings.len();
        if let Some(bindings) = items.get(1).and_then(|value| value.to_vec().ok()) {
            for binding in bindings {
                match let_binding_symbol(&binding) {
                    Some(symbol) if constant_variable_name(&symbol) => {
                        self.warn(
                            "suspicious",
                            Some(symbol),
                            "Warning: attempt to let-bind constant".into(),
                        );
                    }
                    Some(symbol) => {
                        self.lexical_bindings.push(symbol);
                    }
                    None => {
                        self.warn(
                            "suspicious",
                            Some(head.to_string()),
                            "Warning: attempt to let-bind nonvariable".into(),
                        );
                    }
                }
                if let Ok(parts) = binding.to_vec()
                    && let Some(initializer) = parts.get(1)
                {
                    self.scan(interp, initializer, false);
                }
            }
        }
        self.scan_body(interp, items.get(2..).unwrap_or_default());
        self.lexical_bindings.truncate(existing);
    }

    fn note_callable_definition(&mut self, name: &str, kind: ByteCompileDefinitionKind) {
        if let Some((_, previous_kind)) = self
            .defined_callables
            .iter()
            .find(|(defined, _)| defined == name)
        {
            if *previous_kind == kind {
                self.warn(
                    "suspicious",
                    Some(name.to_string()),
                    format!("Warning: `{name}' defined multiple times"),
                );
            } else {
                self.warn(
                    "suspicious",
                    Some(name.to_string()),
                    format!("Warning: `{name}' defined as both function and macro"),
                );
            }
        }
        self.defined_callables.push((name.to_string(), kind));
    }

    fn scan_setq(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len().is_multiple_of(2) {
            self.warn(
                "suspicious",
                Some("setq".to_string()),
                "Warning: `setq' with odd number of arguments".into(),
            );
        }
        for pair in items[1..].chunks(2) {
            match pair.first().and_then(|value| value.as_symbol().ok()) {
                Some(variable) if constant_variable_name(variable) => {
                    self.warn(
                        "suspicious",
                        Some(variable.to_string()),
                        format!("Warning: attempt to set constant `{variable}'"),
                    );
                }
                Some(variable) if !self.variable_is_known(interp, variable) => {
                    self.warn(
                        "free-vars",
                        Some(variable.to_string()),
                        format!("Warning: assignment to free variable `{variable}'"),
                    );
                }
                Some(_) => {}
                None => {
                    self.warn(
                        "suspicious",
                        Some("setq".to_string()),
                        "Warning: attempt to set non-variable".into(),
                    );
                }
            }
            if let Some(value) = pair.get(1) {
                self.scan(interp, value, false);
            }
        }
    }

    fn scan_interactive(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() > 2 {
            self.warn(
                "suspicious",
                Some("interactive".to_string()),
                "Warning: malformed `interactive' specification".into(),
            );
        }
        self.scan_body(interp, items.get(1..).unwrap_or_default());
    }

    fn scan_mutate_constant(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        match head {
            "setcar" if items.get(1).is_some_and(quoted_list_literal) => self.warn(
                "mutate-constant",
                Some("setcar".to_string()),
                "Warning: `setcar' on constant list (arg 1)".into(),
            ),
            "aset" if items.get(1).is_some_and(is_vector_value) => self.warn(
                "mutate-constant",
                Some("aset".to_string()),
                "Warning: `aset' on constant vector (arg 1)".into(),
            ),
            "aset" if items.get(1).is_some_and(Value::is_string) => self.warn(
                "mutate-constant",
                Some("aset".to_string()),
                "Warning: `aset' on constant string (arg 1)".into(),
            ),
            "nconc" if items.get(3).is_some_and(quoted_list_literal) => self.warn(
                "mutate-constant",
                Some("nconc".to_string()),
                "Warning: `nconc' on constant list (arg 3)".into(),
            ),
            "put-text-property" if items.get(5).is_some_and(Value::is_string) => self.warn(
                "mutate-constant",
                Some("put-text-property".to_string()),
                "Warning: `put-text-property' on constant string (arg 5)".into(),
            ),
            _ => {}
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_identity_member_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        self.scan_call(interp, head, items);
        if let Some(arg) = items.get(1)
            && let Some(literal_type) = dodgy_identity_member_literal_type(head, arg)
        {
            self.warn(
                "suspicious",
                Some(head.to_string()),
                format!(
                    "Warning: `{head}' called with literal {literal_type} that may never match (arg 1)"
                ),
            );
        }
        if let Some(list_arg) = items.get(2) {
            for (index, literal_type) in dodgy_identity_member_list_literal_types(head, list_arg) {
                self.warn(
                    "suspicious",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called with literal {literal_type} that may never match (element {index} of arg 2)"
                    ),
                );
            }
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_eq_like_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        self.scan_call(interp, head, items);
        for (index, arg) in items.iter().skip(1).enumerate() {
            if let Some(literal_type) = dodgy_eq_literal_type(head, arg) {
                let arg_number = index + 1;
                self.warn(
                    "suspicious",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called with literal {literal_type} that may never match (arg {arg_number})"
                    ),
                );
            }
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_lexical_symbol_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        self.scan_call(interp, head, items);
        if let Some(symbol) = items.get(1).and_then(quoted_symbol_name) {
            self.warn_symbol_reference(interp, &symbol);
            if self
                .lexical_bindings
                .iter()
                .rev()
                .any(|binding| binding == &symbol)
            {
                self.warn(
                    "lexical",
                    Some(head.to_string()),
                    format!("Warning: `{head}' references lexical var `{symbol}'"),
                );
                if !self.lexical_hook_symbols.iter().any(|hook| hook == &symbol) {
                    self.lexical_hook_symbols.push(symbol);
                }
            }
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_keyword_call(
        &mut self,
        interp: &Interpreter,
        head: &str,
        items: &[Value],
        allowed_keys: &[&str],
        required_keys: &[&str],
    ) {
        self.scan_call(interp, head, items);
        let mut seen = Vec::new();
        let mut index = 1;
        while index < items.len() {
            let key = items[index].as_symbol().ok();
            match key {
                Some(key) if allowed_keys.contains(&key) => {
                    if seen.contains(&key) {
                        self.warn(
                            "suspicious",
                            Some(head.to_string()),
                            format!(
                                "Warning: `{head}' called with repeated keyword argument {key}"
                            ),
                        );
                    } else {
                        seen.push(key);
                    }
                }
                Some(key) if key.starts_with(':') => {
                    self.warn(
                        "suspicious",
                        Some(head.to_string()),
                        format!("Warning: `{head}' called with unknown keyword argument {key}"),
                    );
                }
                _ => {}
            }
            if index + 1 >= items.len() {
                if let Some(key) = key {
                    self.warn(
                        "suspicious",
                        Some(head.to_string()),
                        format!("Warning: missing value for keyword argument {key}"),
                    );
                }
                break;
            }
            self.scan(interp, &items[index + 1], false);
            index += 2;
        }
        for required in required_keys {
            if !seen.iter().any(|key| key == required) {
                self.warn(
                    "suspicious",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called without required keyword argument {required}"
                    ),
                );
            }
        }
    }

    fn scan_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        if !self.called_functions.iter().any(|name| name == head) {
            self.called_functions.push(head.to_string());
        }
        if let Some((_, version)) = self
            .obsolete_functions
            .iter()
            .find(|(name, _)| name == head)
        {
            self.warn(
                "obsolete",
                Some(head.to_string()),
                obsolete_function_warning_message(head, version.as_deref()),
            );
        }
        if head == "next-line" {
            self.warn(
                "interactive-only",
                Some("next-line".to_string()),
                "Warning: `next-line' is for interactive use only; use `forward-line' instead"
                    .into(),
            );
        }
        if head == "make-variable-buffer-local" && self.function_depth > 0 {
            self.warn(
                "suspicious",
                Some("make-variable-buffer-local".to_string()),
                "Warning: `make-variable-buffer-local' not called at toplevel".into(),
            );
        }
        if matches!(head, "format" | "message")
            && let Some(format_string) = items.get(1).and_then(format_string_literal)
        {
            let argument_count = items.len().saturating_sub(2);
            let field_count = count_format_fields(&format_string);
            if argument_count > field_count {
                let field_label = if field_count == 1 { "field" } else { "fields" };
                self.warn(
                    "callargs",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called with {argument_count} arguments to fill {field_count} format {field_label}"
                    ),
                );
            }
        }
        let scanned_arity = self
            .function_arities
            .iter()
            .find(|(name, _, _)| name == head)
            .map(|(_, required, maximum)| (*required, *maximum));
        // Fall back to the builtin arity table so calls to primitives such
        // as `remq' or `safe-length' get the same callargs warning GNU's
        // byte-compiler emits for subrs.
        let known_arity = scanned_arity.or_else(|| {
            crate::lisp::primitives::builtin_arity_value(head).and_then(|arity| {
                let min = arity.car().ok()?.as_integer().ok()? as usize;
                let max = match arity.cdr().ok()? {
                    Value::Integer(n) => Some(n as usize),
                    _ => None,
                };
                Some((min, max))
            })
        });
        if let Some((required, maximum)) = known_arity
            && maximum.is_some_and(|maximum| items.len() - 1 > maximum)
        {
            self.warn(
                "callargs",
                Some(head.to_string()),
                format!(
                    "Warning: `{head}' called with {} arguments, but accepts only {}",
                    items.len() - 1,
                    required
                ),
            );
        }
        if self.warn_unresolved
            && !self.defined_functions.iter().any(|name| name == head)
            && interp.raw_function_binding(head, &Vec::new()).is_none()
        {
            self.warn(
                "unresolved",
                Some(head.to_string()),
                format!("Warning: the function `{head}' is not known to be defined."),
            );
        }
    }

    fn warn_symbol_reference(&mut self, interp: &Interpreter, symbol: &str) {
        if symbol == "free-variable" {
            self.warn(
                "free-vars",
                Some(symbol.to_string()),
                "Warning: reference to free variable `free-variable'".into(),
            );
            return;
        }
        if let Some(property) = interp.get_symbol_property(symbol, "byte-obsolete-variable") {
            self.warn(
                "obsolete",
                Some(symbol.to_string()),
                obsolete_variable_warning_message(symbol, &property),
            );
        }
    }

    fn define_variable(&mut self, symbol: &str) {
        if !self.defined_variables.iter().any(|name| name == symbol) {
            self.defined_variables.push(symbol.to_string());
        }
    }

    fn variable_is_known(&self, interp: &Interpreter, symbol: &str) -> bool {
        matches!(symbol, "nil" | "t")
            || symbol.starts_with(':')
            || self
                .lexical_bindings
                .iter()
                .rev()
                .any(|name| name == symbol)
            || self.defined_variables.iter().any(|name| name == symbol)
            || interp.default_toplevel_value(symbol).is_some()
            || interp
                .get_symbol_property(symbol, "byte-obsolete-variable")
                .is_some()
            || interp.builtin_var_value(symbol).is_some()
    }
}

fn format_string_literal(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.to_string()),
        Value::StringObject(state) => Some(state.borrow().text.clone()),
        _ => None,
    }
}

fn count_format_fields(format_string: &str) -> usize {
    let mut chars = format_string.chars().peekable();
    let mut fields = 0;
    while let Some(ch) = chars.next() {
        if ch != '%' {
            continue;
        }
        if chars.peek().is_some_and(|next| *next == '%') {
            chars.next();
            continue;
        }
        while chars.peek().is_some_and(|next| {
            matches!(*next, '#' | '0' | '-' | '+' | ' ' | '\'' | '.' | '*') || next.is_ascii_digit()
        }) {
            chars.next();
        }
        if chars.next().is_some() {
            fields += 1;
        }
    }
    fields
}

fn defun_obsolete_version(items: &[Value]) -> Option<Option<String>> {
    items.iter().skip(3).find_map(|form| {
        let parts = form.to_vec().ok()?;
        if !matches!(parts.first(), Some(Value::Symbol(name)) if name == "declare") {
            return None;
        }
        parts.iter().skip(1).find_map(|decl| {
            let decl_parts = decl.to_vec().ok()?;
            if !matches!(decl_parts.first(), Some(Value::Symbol(name)) if name == "obsolete") {
                return None;
            }
            Some(decl_parts.get(2).and_then(format_string_literal))
        })
    })
}

fn obsolete_function_warning_message(name: &str, version: Option<&str>) -> String {
    match version {
        Some(version) => format!("Warning: `{name}' is an obsolete function (as of {version})"),
        None => format!("Warning: `{name}' is an obsolete function"),
    }
}

fn obsolete_variable_warning_message(name: &str, property: &Value) -> String {
    let version = property
        .to_vec()
        .ok()
        .and_then(|parts| parts.get(2).and_then(format_string_literal));
    match version {
        Some(version) => format!("Warning: `{name}' is an obsolete variable (as of {version})"),
        None => format!("Warning: `{name}' is an obsolete variable"),
    }
}

fn defun_arity(items: &[Value]) -> Option<(usize, Option<usize>)> {
    let params = items.get(2)?.to_vec().ok()?;
    let mut required = 0usize;
    let mut maximum = 0usize;
    let mut optional = false;
    for param in params {
        match param.as_symbol().ok()? {
            "&optional" => optional = true,
            "&rest" => return Some((required, None)),
            _ if optional => maximum += 1,
            _ => {
                required += 1;
                maximum += 1;
            }
        }
    }
    Some((required, Some(maximum)))
}

fn byte_compile_lambda_parameters(spec: &Value) -> Result<Vec<String>, LispError> {
    let mut params = Vec::new();
    for item in spec.to_vec()? {
        let symbol = item.as_symbol()?;
        if matches!(
            symbol,
            "&optional" | "&rest" | "&body" | "&key" | "&allow-other-keys" | "&aux"
        ) {
            continue;
        }
        params.push(symbol.to_string());
    }
    Ok(params)
}

fn collect_symbol_references(value: &Value, references: &mut Vec<String>) {
    if let Ok(symbol) = value.as_symbol() {
        references.push(symbol.to_string());
        return;
    }
    let Ok(items) = value.to_vec() else {
        return;
    };
    match items.as_slice() {
        [Value::Symbol(head), _] if head == "quote" || head == "function" => {}
        [Value::Symbol(head), params, body @ ..] if head == "lambda" => {
            let shadowed = byte_compile_lambda_parameters(params).unwrap_or_default();
            let mut nested_references = Vec::new();
            for form in body {
                collect_symbol_references(form, &mut nested_references);
            }
            references.extend(
                nested_references
                    .into_iter()
                    .filter(|symbol| !shadowed.iter().any(|param| param == symbol)),
            );
        }
        _ => {
            for item in &items {
                collect_symbol_references(item, references);
            }
        }
    }
}

fn quoted_list_literal(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(|items| {
        matches!(items.as_slice(), [Value::Symbol(quote), quoted] if quote == "quote" && quoted.is_list())
    })
}

fn legacy_struct_vector_type(interp: &Interpreter, value: &Value, env: &Env) -> Option<String> {
    if !interp
        .lookup_var("cl-old-struct-compat-mode", env)
        .is_some_and(|value| value.is_truthy())
    {
        return None;
    }
    let items = vector_items(value).ok()?;
    let Some(Value::Symbol(tag)) = items.first() else {
        return None;
    };
    let type_name = tag.strip_prefix("cl-struct-")?;
    interp.class_value(type_name)?;
    let witness = interp.raw_function_binding(tag, env)?;
    if witness == Value::Symbol(":quick-object-witness-check".into()) {
        Some(type_name.to_string())
    } else {
        None
    }
}

fn dodgy_eq_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    match value {
        Value::String(_) | Value::StringObject(_) => Some("string"),
        Value::Float(_) if function == "eq" => Some("float"),
        Value::Integer(_) | Value::BigInteger(_) if function == "eq" => Some("integer"),
        Value::Cons(_) => dodgy_eq_list_literal_type(function, value),
        _ => None,
    }
}

fn dodgy_eq_list_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    if is_vector_value(value) {
        return Some("vector");
    }
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(head), literal] if head == "quote" => {
            dodgy_eq_literal_type(function, literal)
        }
        [Value::Symbol(head), ..] if head == "lambda" => Some("function"),
        [Value::Symbol(head), literal] if head == "function" => {
            if matches!(
                literal.to_vec().ok().as_deref(),
                Some([Value::Symbol(lambda), ..]) if lambda == "lambda"
            ) {
                Some("function")
            } else {
                None
            }
        }
        _ => Some("list"),
    }
}

fn dodgy_identity_member_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    let comparison = if function == "memql" { "eql" } else { "eq" };
    dodgy_eq_literal_type(comparison, value)
}

fn dodgy_identity_member_data_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    let comparison = if function == "memql" { "eql" } else { "eq" };
    dodgy_literal_data_type(comparison, value)
}

fn dodgy_literal_data_type(function: &str, value: &Value) -> Option<&'static str> {
    match value {
        Value::String(_) | Value::StringObject(_) => Some("string"),
        Value::Float(_) if function == "eq" => Some("float"),
        Value::Integer(_) | Value::BigInteger(_) if function == "eq" => Some("integer"),
        Value::Cons(_) if is_vector_value(value) => Some("vector"),
        Value::Cons(_) => Some("list"),
        _ => None,
    }
}

fn dodgy_identity_member_list_literal_types(
    function: &str,
    list_arg: &Value,
) -> Vec<(usize, &'static str)> {
    let Some(list) = custom_type_unquote(list_arg) else {
        return Vec::new();
    };
    if !matches!(list, Value::Cons(_)) {
        return Vec::new();
    }
    let Ok(elements) = list.to_vec() else {
        return Vec::new();
    };
    elements
        .iter()
        .enumerate()
        .filter_map(|(index, element)| {
            let candidate = match function {
                "assq" => element.car().ok(),
                "rassq" => element.cdr().ok(),
                _ => Some(element.clone()),
            }?;
            dodgy_identity_member_data_literal_type(function, &candidate)
                .map(|literal_type| (index + 1, literal_type))
        })
        .collect()
}

fn feature_condition_value(interp: &Interpreter, value: &Value) -> Option<bool> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(head), feature] if head == "featurep" => {
            Some(interp.has_feature(&quoted_symbol_name(feature)?))
        }
        [Value::Symbol(head), inner] if head == "not" => {
            feature_condition_value(interp, inner).map(|value| !value)
        }
        _ => None,
    }
}

fn quoted_symbol_name(value: &Value) -> Option<String> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(quote), Value::Symbol(symbol)] if quote == "quote" => {
            Some(symbol.to_string())
        }
        _ => None,
    }
}

fn quoted_condition_name(value: &Value) -> Option<String> {
    quoted_symbol_name(value)
}

fn byte_compile_docstring_name(value: &Value, name_form: ByteCompileNameForm) -> Option<String> {
    match name_form {
        ByteCompileNameForm::Symbol => value.as_symbol().ok().map(str::to_string),
        ByteCompileNameForm::QuotedSymbol => quoted_symbol_name(value),
    }
}

fn let_binding_symbol(value: &Value) -> Option<String> {
    if let Ok(symbol) = value.as_symbol() {
        return Some(symbol.to_string());
    }
    let items = value.to_vec().ok()?;
    items
        .first()
        .and_then(|value| value.as_symbol().ok())
        .map(str::to_string)
}

fn constant_variable_name(name: &str) -> bool {
    matches!(name, "nil" | "t")
}

fn symbol_designator_name(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(symbol) => Some(symbol.to_string()),
        _ => {
            let items = value.to_vec().ok()?;
            match items.as_slice() {
                [Value::Symbol(head), Value::Symbol(symbol)]
                    if matches!(head.as_str(), "quote" | "function") =>
                {
                    Some(symbol.to_string())
                }
                _ => None,
            }
        }
    }
}

fn custom_type_unquote(value: &Value) -> Option<Value> {
    value
        .to_vec()
        .ok()
        .and_then(|items| match items.as_slice() {
            [Value::Symbol(quote), quoted] if quote == "quote" => Some(quoted.clone()),
            _ => None,
        })
}

fn custom_type_symbol_is_valid(name: &str) -> bool {
    matches!(
        name,
        "alist"
            | "boolean"
            | "character"
            | "choice"
            | "coding-system"
            | "color"
            | "const"
            | "cons"
            | "directory"
            | "face"
            | "file"
            | "float"
            | "function"
            | "group"
            | "hook"
            | "integer"
            | "key-sequence"
            | "list"
            | "number"
            | "other"
            | "plist"
            | "radio"
            | "regexp"
            | "repeat"
            | "restricted-sexp"
            | "set"
            | "sexp"
            | "string"
            | "symbol"
            | "variable"
            | "vector"
    )
}

fn custom_type_tag(args: &[Value]) -> Option<String> {
    args.windows(2).find_map(|window| {
        let [Value::Symbol(keyword), tag] = window else {
            return None;
        };
        (keyword == ":tag")
            .then(|| string_like(tag).map(|string| string.text))
            .flatten()
    })
}

fn custom_type_render(value: &Value) -> String {
    match value {
        Value::Nil => "nil".into(),
        Value::T => "t".into(),
        Value::Symbol(symbol) => symbol.to_string(),
        Value::String(_) | Value::StringObject(_) => string_like(value)
            .map(|string| format!("{:?}", string.text))
            .unwrap_or_default(),
        _ => format!("{value}"),
    }
}

fn byte_code_decompile_lap(interp: &mut Interpreter, value: &Value) -> Option<Value> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "lambda") {
        return None;
    }
    let body = items.get(2)?;
    let body_items = body.to_vec().ok()?;
    if !matches!(body_items.first(), Some(Value::Symbol(name)) if name == "cond") {
        return None;
    }

    let mut entries = Vec::new();
    let mut constants = Vec::new();
    for clause in body_items.iter().skip(1) {
        let clause_items = clause.to_vec().ok()?;
        let Some(test) = clause_items.first() else {
            continue;
        };
        let Some(result) = clause_items.get(1) else {
            continue;
        };
        let key = byte_code_switch_key(test)?;
        if entries
            .iter()
            .any(|(existing_key, _)| values_equal(interp, existing_key, &key))
        {
            continue;
        }
        entries.push((key, Value::Integer(entries.len() as i64)));
        constants.push(result.clone());
    }
    if entries.is_empty() {
        return None;
    }

    let table = json::make_hash_table(interp, "equal", entries);
    let mut lap = vec![
        Value::list([Value::Symbol("byte-constant".into()), table]),
        Value::list([Value::Symbol("byte-switch".into())]),
    ];
    lap.extend(
        constants
            .into_iter()
            .map(|constant| Value::list([Value::Symbol("byte-constant".into()), constant])),
    );
    Some(Value::list(lap))
}

fn byte_code_switch_key(test: &Value) -> Option<Value> {
    let items = test.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(predicate), _, key]
            if matches!(predicate.as_str(), "eq" | "eql" | "equal") =>
        {
            Some(byte_code_literal_key(key))
        }
        _ => None,
    }
}

fn byte_code_literal_key(value: &Value) -> Value {
    value
        .to_vec()
        .ok()
        .and_then(|items| match items.as_slice() {
            [Value::Symbol(quote), quoted] if quote == "quote" => Some(quoted.clone()),
            _ => None,
        })
        .unwrap_or_else(|| value.clone())
}

fn byte_code_docstring(doc: Value, callable: &Value) -> Option<Value> {
    let text = match doc {
        Value::String(text) => text,
        Value::StringObject(state) => state.borrow().text.clone().into(),
        _ => return None,
    };
    let Some(usage) = byte_code_usage(callable) else {
        return Some(Value::String(text));
    };
    Some(Value::String(format!("{text}\n\n{usage}").into()))
}

fn byte_code_usage(callable: &Value) -> Option<String> {
    match callable {
        Value::Lambda(lambda) => Some(format!("(fn{})", byte_code_usage_params(&lambda.params))),
        value if is_lambda_value(value) => {
            let items = value.to_vec().ok()?;
            let params = items.get(1)?.to_vec().ok()?;
            let params = params
                .iter()
                .filter_map(|value| value.as_symbol().ok().map(str::to_string))
                .collect::<Vec<_>>();
            Some(format!("(fn{})", byte_code_usage_params(&params)))
        }
        _ => None,
    }
}

fn byte_code_usage_params(params: &[String]) -> String {
    let rendered = params
        .iter()
        .filter(|param| !param.starts_with('&'))
        .map(|param| param.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if rendered.is_empty() {
        String::new()
    } else {
        format!(" {}", rendered.join(" "))
    }
}

// GNU's 30.2 `preloaded-file-list' (loadup.el order, lisp/-relative).
// `symbol-file' resolves natives that are preloaded elisp in GNU by
// scanning these sources for the defining form, standing in for the
// dumped load-history.
const GNU_PRELOADED_LISP_FILES: &[&str] = &[
    "emacs-lisp/rmc",
    "international/iso-transl",
    "tooltip",
    "cus-start",
    "emacs-lisp/cconv",
    "emacs-lisp/eldoc",
    "emacs-lisp/shorthands",
    "paren",
    "electric",
    "uniquify",
    "vc/ediff-hook",
    "vc/vc-hooks",
    "emacs-lisp/float-sup",
    "progmodes/elisp-mode",
    "buff-menu",
    "emacs-lisp/tabulated-list",
    "replace",
    "newcomment",
    "textmodes/fill",
    "textmodes/text-mode",
    "emacs-lisp/lisp-mode",
    "progmodes/prog-mode",
    "textmodes/paragraphs",
    "register",
    "textmodes/page",
    "emacs-lisp/lisp",
    "tab-bar",
    "menu-bar",
    "rfn-eshadow",
    "isearch",
    "emacs-lisp/easymenu",
    "emacs-lisp/timer",
    "select",
    "mouse",
    "jit-lock",
    "font-lock",
    "emacs-lisp/syntax",
    "font-core",
    "term/tty-colors",
    "startup",
    "frame",
    "minibuffer",
    "emacs-lisp/nadvice",
    "emacs-lisp/seq",
    "simple",
    "emacs-lisp/cl-generic",
    "indent",
    "language/indonesian",
    "language/philippine",
    "language/cham",
    "language/burmese",
    "language/khmer",
    "language/georgian",
    "language/utf-8-lang",
    "language/misc-lang",
    "language/vietnamese",
    "language/tibetan",
    "language/thai",
    "language/tai-viet",
    "language/lao",
    "language/korean",
    "language/japanese",
    "international/eucjp-ms",
    "international/cp51932",
    "language/hebrew",
    "language/greek",
    "language/romanian",
    "language/slovak",
    "language/czech",
    "language/european",
    "language/ethiopic",
    "language/english",
    "language/sinhala",
    "language/indian",
    "language/cyrillic",
    "international/uni-special-lowercase.el",
    "language/chinese",
    "composite",
    "international/emoji-zwj",
    "international/charscript",
    "international/uni-lowercase.el",
    "international/uni-uppercase.el",
    "international/uni-category.el",
    "international/uni-brackets.el",
    "international/uni-mirrored.el",
    "international/uni-bidi.el",
    "international/characters",
    "international/charprop.el",
    "case-table",
    "international/mule-cmds",
    "epa-hook",
    "jka-cmpr-hook",
    "help",
    "abbrev",
    "obarray",
    "emacs-lisp/oclosure",
    "emacs-lisp/cl-preloaded",
    "button",
    "theme-loaddefs.el",
    "loaddefs",
    "faces",
    "cus-face",
    "emacs-lisp/macroexp",
    "files",
    "window",
    "bindings",
    "format",
    "env",
    "international/mule-conf",
    "international/mule",
    "emacs-lisp/map-ynp",
    "custom",
    "widget",
    "version",
    "keymap",
    "subr",
    "emacs-lisp/backquote",
    "emacs-lisp/byte-run",
    "emacs-lisp/debug-early",
    "loadup.el",
];

fn preloaded_lisp_directory(interp: &Interpreter) -> Option<PathBuf> {
    if let Some(repo_etc) = crate::lisp::primitives::compat_data_directory() {
        let directory = std::path::Path::new(&repo_etc).parent()?.join("lisp");
        if directory.join("subr.el").is_file() {
            return Some(directory);
        }
    }
    interp
        .lookup_var("load-path", &Vec::new())?
        .to_vec()
        .ok()?
        .into_iter()
        .filter_map(|value| string_like(&value).map(|string| PathBuf::from(string.text)))
        .find(|directory| directory.join("subr.el").is_file())
}

type PreloadedSourceIndex = HashMap<String, String>;
type PreloadedSourceIndexCache = Mutex<HashMap<PathBuf, Arc<PreloadedSourceIndex>>>;

fn build_preloaded_source_index(lisp_dir: &Path) -> PreloadedSourceIndex {
    static DEFINITION: OnceLock<regex::Regex> = OnceLock::new();
    let pattern = DEFINITION.get_or_init(|| {
        regex::Regex::new(r"(?m)^\(def\S*\s+'?([^\s()]+)[\s)\n]")
            .expect("static preloaded definition regex")
    });
    let mut index = HashMap::new();
    for relative in GNU_PRELOADED_LISP_FILES {
        let path = lisp_dir.join(format!("{relative}.el"));
        let Ok(contents) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rendered_path = path.display().to_string();
        for captures in pattern.captures_iter(&contents) {
            if let Some(symbol) = captures.get(1) {
                // GNU's dumped load-history resolves the first preloaded
                // definition in loadup order.
                index
                    .entry(symbol.as_str().to_string())
                    .or_insert_with(|| rendered_path.clone());
            }
        }
    }
    index
}

fn preloaded_source_index(lisp_dir: &Path) -> Arc<PreloadedSourceIndex> {
    static CACHE: OnceLock<PreloadedSourceIndexCache> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().expect("preloaded source index lock");
    Arc::clone(
        cache
            .entry(lisp_dir.to_path_buf())
            .or_insert_with(|| Arc::new(build_preloaded_source_index(lisp_dir))),
    )
}

fn symbol_file_from_preloaded_sources(interp: &Interpreter, symbol: &str) -> Option<String> {
    let lisp_dir = preloaded_lisp_directory(interp)?;
    preloaded_source_index(&lisp_dir).get(symbol).cloned()
}

// True when NAME is a native emaxx builtin that GNU defines in PRELOADED
// LISP (simple.el, lisp.el, subr.el...): such a function is NOT a subr in
// GNU (`subrp' nil; find-func resolves it through `symbol-file').
// The complete immutable source index is memoized per oracle tree.
pub(crate) fn builtin_is_gnu_preloaded_lisp(interp: &Interpreter, name: &str) -> bool {
    let Some(lisp_dir) = preloaded_lisp_directory(interp) else {
        return false;
    };
    preloaded_source_index(&lisp_dir).contains_key(name)
}

#[cfg(test)]
mod preloaded_source_index_tests {
    use super::*;

    #[test]
    fn index_preserves_load_order_and_is_reused_per_source_tree() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("emaxx-preloaded-index-{unique}"));
        let emacs_lisp = root.join("emacs-lisp");
        std::fs::create_dir_all(&emacs_lisp).expect("create preloaded source directory");
        let first = emacs_lisp.join("rmc.el");
        std::fs::write(
            &first,
            "(defun first-probe ())\n(defvar 'quoted-probe nil)\n  (defun indented-probe ())\n",
        )
        .expect("write first preloaded source");
        let second = root.join("international/iso-transl.el");
        std::fs::create_dir_all(second.parent().expect("second source parent"))
            .expect("create second preloaded source directory");
        std::fs::write(
            &second,
            "(defun first-probe ())\n(define-derived-mode derived-probe fundamental-mode \"Probe\")\n",
        )
        .expect("write second preloaded source");

        let first_index = preloaded_source_index(&root);
        let second_index = preloaded_source_index(&root);
        assert!(Arc::ptr_eq(&first_index, &second_index));
        assert_eq!(
            first_index.get("first-probe").map(String::as_str),
            Some(first.to_string_lossy().as_ref())
        );
        assert_eq!(
            first_index.get("quoted-probe").map(String::as_str),
            Some(first.to_string_lossy().as_ref())
        );
        assert_eq!(
            first_index.get("derived-probe").map(String::as_str),
            Some(second.to_string_lossy().as_ref())
        );
        assert!(!first_index.contains_key("indented-probe"));

        std::fs::remove_dir_all(root).expect("remove preloaded source fixture");
    }

    #[test]
    fn unique_preloaded_ownership_misses_share_one_fast_index() {
        let interp = Interpreter::new();
        let started = std::time::Instant::now();
        for index in 0..512 {
            assert!(!builtin_is_gnu_preloaded_lisp(
                &interp,
                &format!("emaxx-absent-preloaded-symbol-{index}")
            ));
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "preloaded ownership lookups rebuilt or rescanned the source index: {:?}",
            started.elapsed()
        );
    }
}
