use super::*;

pub(crate) fn set_command_key_state(
    interp: &mut Interpreter,
    keys: Vec<Value>,
    raw_keys: Vec<Value>,
    env: &mut Env,
) {
    interp.keyboard_input.command_keys = keys.clone();
    interp.keyboard_input.single_command_start = 0;
    interp.keyboard_input.raw_keys = raw_keys;
    interp.set_variable(
        "this-single-command-keys",
        Value::list(std::iter::once(Value::symbol("vector-literal")).chain(keys)),
        env,
    );
}

fn dribble_event_bytes(event: &Value) -> Vec<u8> {
    match event {
        Value::Integer(code) => u32::try_from(*code)
            .ok()
            .and_then(char::from_u32)
            .map(|character| character.to_string().into_bytes())
            .unwrap_or_else(|| format!("<{code}>").into_bytes()),
        Value::String(text) => text.as_bytes().to_vec(),
        Value::StringObject(state) => state.borrow().text.as_bytes().to_vec(),
        Value::Symbol(symbol) => format!("<{symbol}>").into_bytes(),
        other => format!("<{other}>").into_bytes(),
    }
}

fn record_external_input_event(interp: &mut Interpreter, event: &Value) {
    if !interp.kbd_macro_executions.is_empty() {
        return;
    }
    interp.keyboard_input.recent_keys.push(event.clone());
    let limit = interp.lossage_size.max(0) as usize;
    if interp.keyboard_input.recent_keys.len() > limit {
        let excess = interp.keyboard_input.recent_keys.len() - limit;
        interp.keyboard_input.recent_keys.drain(0..excess);
    }
    if let Some(path) = &interp.keyboard_input.dribble_file
        && let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(path)
    {
        let _ = file.write_all(&dribble_event_bytes(event));
        let _ = file.flush();
    }
}

pub(crate) fn function_documentation(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<Value> {
    if let Value::Symbol(symbol) = value
        && let Some(documentation) = interp.get_symbol_property(symbol, "function-documentation")
    {
        return Some(documentation);
    }
    let value = match value {
        Value::Symbol(symbol) => interp.lookup_function(symbol, env).ok()?,
        other => other.clone(),
    };
    if let Value::Record(id) = value
        && let Some(record) = interp.find_record(id)
        && record.type_name == "byte-code-function"
    {
        return record.slots.get(4).filter(|doc| !doc.is_nil()).cloned();
    }
    let Value::Lambda(lambda) = value else {
        return None;
    };
    lambda.body.iter().find_map(|form| match form {
        Value::String(text) => Some(Value::String(text.clone())),
        Value::StringObject(state) => Some(Value::String(state.borrow().text.clone().into())),
        _ => None,
    })
}

pub(crate) fn is_vector_like_value(interp: &Interpreter, value: &Value) -> bool {
    is_vector_value(value)
        || is_bool_vector_value(interp, value)
        || matches!(value, Value::CharTable(_))
}

pub(crate) fn is_vector_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Cons(cell)
            if matches!(&*cell.car.borrow(), Value::Symbol(symbol) if symbol == "vector-literal")
    )
}

pub(crate) fn fixnum_bounds(interp: &Interpreter) -> Result<(i64, i64), LispError> {
    let max_fixnum = interp
        .default_value("most-positive-fixnum")
        .ok_or_else(|| LispError::Void("most-positive-fixnum".into()))?
        .as_integer()?;
    let min_fixnum = interp
        .default_value("most-negative-fixnum")
        .ok_or_else(|| LispError::Void("most-negative-fixnum".into()))?
        .as_integer()?;
    Ok((min_fixnum, max_fixnum))
}

pub(crate) fn symbol_with_pos_parts(interp: &Interpreter, value: &Value) -> Option<(Value, i64)> {
    let Value::Record(id) = value else {
        return None;
    };
    let record = interp.find_record(*id)?;
    if record.type_name != "symbol-with-pos" || record.slots.len() < 2 {
        return None;
    }
    Some((record.slots[0].clone(), record.slots[1].as_integer().ok()?))
}

#[cfg(test)]
thread_local! {
    static SYMBOL_WITH_POS_FLAG_READ_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

fn symbols_with_pos_enabled(interp: &Interpreter, env: &Env) -> bool {
    #[cfg(test)]
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(|count| count.set(count.get() + 1));
    interp
        .lookup_var("symbols-with-pos-enabled", env)
        .is_some_and(|value| value.is_truthy())
}

#[cfg(test)]
pub(crate) fn reset_symbol_with_pos_flag_read_count() {
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn symbol_with_pos_flag_read_count() -> usize {
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(std::cell::Cell::get)
}

pub(crate) fn symbol_with_pos_equal_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Option<bool> {
    let left_with_pos = symbol_with_pos_parts(interp, left);
    let right_with_pos = symbol_with_pos_parts(interp, right);
    if left_with_pos.is_none() && right_with_pos.is_none() {
        return None;
    }

    let enabled = symbols_with_pos_enabled(interp, env);
    if enabled {
        let left_base = left_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(left);
        let right_base = right_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(right);
        return Some(
            match (plain_symbol_name(left_base), plain_symbol_name(right_base)) {
                (Some(_), Some(_)) => left_base == right_base,
                _ => false,
            },
        );
    }

    match (left_with_pos, right_with_pos) {
        (Some((left_symbol, left_pos)), Some((right_symbol, right_pos))) => {
            Some(left_symbol == right_symbol && left_pos == right_pos)
        }
        _ => None,
    }
}

pub(crate) fn symbol_with_pos_eq_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Option<bool> {
    let left_with_pos = symbol_with_pos_parts(interp, left);
    let right_with_pos = symbol_with_pos_parts(interp, right);
    if left_with_pos.is_none() && right_with_pos.is_none() {
        return None;
    }

    if !symbols_with_pos_enabled(interp, env) {
        return None;
    }

    let left_base = left_with_pos
        .as_ref()
        .map(|(symbol, _)| symbol)
        .unwrap_or(left);
    let right_base = right_with_pos
        .as_ref()
        .map(|(symbol, _)| symbol)
        .unwrap_or(right);
    Some(
        match (plain_symbol_name(left_base), plain_symbol_name(right_base)) {
            (Some(_), Some(_)) => left_base == right_base,
            _ => false,
        },
    )
}

pub(crate) fn is_lambda_value(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "lambda"),
    )
}

pub(crate) fn validate_lambda_params(params: &Value) -> Result<(), LispError> {
    let items = params.to_vec()?;
    validate_lambda_list_items(params, &items)
}

pub(crate) fn validate_lambda_form(form: &Value) -> Result<(), LispError> {
    let items = form.to_vec()?;
    let Some(params) = items.get(1) else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("invalid-function".into()),
            form.clone(),
        ])));
    };
    validate_lambda_params(params)
}

pub(crate) fn validate_lambda_list_items(spec: &Value, items: &[Value]) -> Result<(), LispError> {
    let invalid = || {
        LispError::SignalValue(Value::list([
            Value::Symbol("invalid-function".into()),
            spec.clone(),
        ]))
    };
    let mut seen_optional = false;
    let mut seen_rest = false;
    let mut needs_rest_arg = false;
    let mut rest_arg_seen = false;

    for item in items {
        let Value::Symbol(symbol) = item else {
            return Err(invalid());
        };
        match symbol.as_str() {
            "&optional" => {
                if seen_optional || seen_rest {
                    return Err(invalid());
                }
                seen_optional = true;
            }
            "&rest" => {
                if seen_rest {
                    return Err(invalid());
                }
                seen_rest = true;
                needs_rest_arg = true;
            }
            _ => {
                if needs_rest_arg {
                    needs_rest_arg = false;
                    rest_arg_seen = true;
                } else if rest_arg_seen {
                    return Err(invalid());
                }
            }
        }
    }

    if needs_rest_arg {
        return Err(invalid());
    }

    Ok(())
}

pub(crate) fn eval_callable_metadata_form(
    interp: &mut Interpreter,
    func: &Value,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if let Value::Lambda(lambda) = func {
        interp.eval_with_closure_env(&lambda.env, env, |interp, call_env| {
            interp.eval(form, call_env)
        })
    } else {
        interp.eval(form, env)
    }
}

pub(crate) fn parse_interactive_string(
    spec: &str,
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut values = Vec::new();
    for line in spec.split('\n') {
        if line.is_empty() {
            continue;
        }
        let mut chars = line.chars().skip_while(|ch| matches!(ch, '*' | '@' | '^'));
        let Some(code) = chars.next() else {
            continue;
        };
        match code {
            'k' => {
                let ch = unread_command_event_char(&pop_unread_command_event_value(interp, env)?)?;
                values.push(Value::String(ch.to_string().into()));
            }
            'p' => {
                let prefix = interp
                    .lookup_var("current-prefix-arg", env)
                    .unwrap_or(Value::Nil);
                values.push(prefix_numeric_value(&prefix)?);
            }
            'P' => {
                values.push(
                    interp
                        .lookup_var("current-prefix-arg", env)
                        .unwrap_or(Value::Nil),
                );
            }
            'N' => {
                let prefix = interp
                    .lookup_var("current-prefix-arg", env)
                    .unwrap_or(Value::Nil);
                if prefix.is_truthy() {
                    values.push(prefix_numeric_value(&prefix)?);
                } else {
                    let prompt = chars.collect::<String>();
                    values.push(interp.call_function_value(
                        Value::Symbol("read-number".into()),
                        Some("read-number"),
                        &[Value::String(prompt.into())],
                        env,
                    )?);
                }
            }
            _ => return Err(invalid_interactive_control_letter(code)),
        }
    }
    Ok(values)
}

// Terminal-driven event input, installed by the tty frontend for the
// duration of an interactive session.  The reader blocks on the terminal
// and returns one key event, or `None' for C-g; without a reader the
// queued-events contract below is unchanged.
thread_local! {
    static TTY_EVENT_READER: std::cell::RefCell<Option<TtyEventReader>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyEventReader = Box<dyn FnMut() -> Option<Value>>;

pub(crate) fn set_tty_event_reader(reader: Option<TtyEventReader>) {
    TTY_EVENT_READER.with_borrow_mut(|slot| *slot = reader);
}

fn read_via_tty_event_reader() -> Option<Option<Value>> {
    TTY_EVENT_READER.with_borrow_mut(|slot| slot.as_mut().map(|reader| reader()))
}

pub(crate) fn pop_unread_command_event_value(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let unread = interp
        .lookup_var("unread-command-events", env)
        .unwrap_or(Value::Nil);
    let mut events = unread.to_vec()?;
    if events.is_empty() {
        // GNU's input readers consume the executing keyboard macro's
        // remaining events (viper's `F'/`t' read their target char that way).
        if let Some(state) = interp.kbd_macro_executions.last_mut()
            && let Some(event) = state.events.get(state.index).cloned()
        {
            state.index += 1;
            let index = state.index;
            interp.set_variable(
                "executing-kbd-macro-index",
                Value::Integer(index as i64),
                env,
            );
            return Ok(event);
        }
        if let Some(read) = read_via_tty_event_reader() {
            return match read {
                Some(event) => {
                    record_external_input_event(interp, &event);
                    Ok(event)
                }
                None => Err(LispError::SignalValue(Value::Symbol("quit".into()))),
            };
        }
        return Err(LispError::Signal(
            "No unread-command-events available for interactive input".into(),
        ));
    }
    let event = events.remove(0);
    interp.set_variable("unread-command-events", Value::list(events), env);
    record_external_input_event(interp, &event);
    Ok(event)
}

pub(crate) fn unread_command_event_char(event: &Value) -> Result<char, LispError> {
    translated_unread_event_char(event)
        .ok_or_else(|| LispError::Signal(format!("Invalid unread command event {}", event)))
}

pub(crate) fn normalize_input_event_value(event: Value) -> Result<Value, LispError> {
    if let Some(ch) = unread_event_char(&event) {
        Ok(Value::Integer(ch as i64))
    } else {
        Ok(event)
    }
}

pub(crate) fn normalize_key_event_value(event: Value) -> Result<Value, LispError> {
    if let Some(ch) = translated_unread_event_char(&event) {
        Ok(Value::Integer(ch as i64))
    } else {
        Ok(event)
    }
}

pub(crate) fn unread_command_events(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    interp
        .lookup_var("unread-command-events", env)
        .unwrap_or(Value::Nil)
        .to_vec()
}

pub(crate) fn unread_event_char(value: &Value) -> Option<char> {
    match value {
        Value::Integer(code) if *code >= 0 => modified_event_code_char(*code),
        Value::Cons(cell) if matches!(cell.car.borrow().clone(), Value::T) => {
            match cell.cdr.borrow().clone() {
                Value::Integer(code) if code >= 0 => modified_event_code_char(code),
                _ => None,
            }
        }
        Value::String(text) => text.chars().next(),
        Value::StringObject(state) => state.borrow().text.chars().next(),
        _ => None,
    }
}

/// GNU's default `local-function-key-map' translations.  Raw event readers
/// preserve these symbols, while key sequences, character readers, and
/// minibuffer command loops consume their translated character events.
pub(crate) fn function_key_default_translation(name: &str) -> Option<i64> {
    Some(match name {
        "escape" => 27,
        "tab" => 9,
        "return" => 13,
        "linefeed" => 10,
        "delete" | "backspace" => 127,
        _ => return None,
    })
}

pub(crate) fn translated_unread_event_char(value: &Value) -> Option<char> {
    match value {
        Value::Symbol(name) => {
            function_key_default_translation(name).and_then(|code| char::from_u32(code as u32))
        }
        _ => unread_event_char(value),
    }
}

fn modified_event_code_char(code: i64) -> Option<char> {
    const ALT: i64 = 1 << 22;
    const SUPER: i64 = 1 << 23;
    const HYPER: i64 = 1 << 24;
    const SHIFT: i64 = 1 << 25;
    const CONTROL: i64 = 1 << 26;
    const META: i64 = 1 << 27;
    const MODIFIERS: i64 = ALT | SUPER | HYPER | SHIFT | CONTROL | META;

    let mut base = code & !MODIFIERS;
    if code & CONTROL != 0 && base != 0 {
        base = match base {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    } else if code & SHIFT != 0
        && let Some(ch) = char::from_u32(base as u32)
    {
        base = ch.to_ascii_uppercase() as i64;
    }
    char::from_u32(base as u32)
}

pub(crate) fn unread_prefix_matches(events: &[Value], prefix: &str) -> Option<usize> {
    let chars: Vec<char> = prefix.chars().collect();
    if events.len() < chars.len() {
        return None;
    }
    for (index, expected) in chars.iter().enumerate() {
        if unread_event_char(&events[index]) != Some(*expected) {
            return None;
        }
    }
    Some(chars.len())
}

pub(crate) fn prepend_unread_command_events(
    interp: &mut Interpreter,
    env: &mut Env,
    mut prefix: Vec<Value>,
) -> Result<(), LispError> {
    let unread = unread_command_events(interp, env)?;
    prefix.extend(unread);
    interp.set_variable("unread-command-events", Value::list(prefix), env);
    Ok(())
}

pub(crate) fn translated_input_events(value: &Value) -> Result<Vec<Value>, LispError> {
    if matches!(value, Value::Nil) {
        return Ok(Vec::new());
    }
    if let Ok(items) = vector_items(value) {
        return Ok(items);
    }
    Ok(vec![value.clone()])
}

pub(crate) fn is_mouse_down_event(value: &Value) -> bool {
    value
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|item| item.as_symbol().ok().map(str::to_string))
        .is_some_and(|name| name.contains("down-mouse"))
}

pub(crate) fn read_decoded_input_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let unread = unread_command_events(interp, env)?;
    for (prefix, function_name) in [
        ("\u{1b}[<", "xterm-mouse-translate-extended"),
        ("\u{1b}[M", "xterm-mouse-translate"),
    ] {
        let Some(prefix_len) = unread_prefix_matches(&unread, prefix) else {
            continue;
        };
        let Ok(function) = interp.lookup_function(function_name, env) else {
            continue;
        };

        let mut remaining = unread.clone();
        remaining.drain(0..prefix_len);
        interp.set_variable("unread-command-events", Value::list(remaining), env);

        let translated =
            interp.call_function_value(function, Some(function_name), &[Value::Nil], env)?;
        let events = translated_input_events(&translated)?;
        if events.is_empty() {
            return Ok(None);
        }
        if events.len() > 1 {
            prepend_unread_command_events(interp, env, events[1..].to_vec())?;
        }
        return Ok(events.into_iter().next());
    }

    let Some(input_decode_map) = interp.lookup_var("input-decode-map", env) else {
        return Ok(None);
    };
    let mut best_match: Option<(usize, Value)> = None;
    let mut prefix = String::new();
    for event in unread.iter().take(8) {
        let Some(ch) = unread_event_char(event) else {
            break;
        };
        prefix.push(ch);
        let binding = keymap_lookup_binding(interp, &input_decode_map, &prefix)?;
        if !binding.is_nil()
            && best_match
                .as_ref()
                .is_none_or(|(best_len, _)| prefix.chars().count() > *best_len)
        {
            best_match = Some((prefix.chars().count(), binding));
        }
    }
    let Some((prefix_len, binding)) = best_match else {
        return Ok(None);
    };

    let mut remaining = unread;
    remaining.drain(0..prefix_len);
    interp.set_variable("unread-command-events", Value::list(remaining), env);

    let function = resolve_callable(interp, &binding, env)?;
    let translated = invoke_function_value(interp, &function, &[Value::Nil], env)?;
    let events = translated_input_events(&translated)?;
    if events.is_empty() {
        return Ok(None);
    }
    if events.len() > 1 {
        prepend_unread_command_events(interp, env, events[1..].to_vec())?;
    }
    Ok(events.into_iter().next())
}

pub(crate) fn input_event_symbol(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(symbol) => Some(symbol.to_string()),
        Value::Cons(_) => value
            .to_vec()
            .ok()
            .and_then(|items| items.first().cloned())
            .and_then(|item| item.as_symbol().ok().map(str::to_string)),
        _ => None,
    }
}

pub(crate) fn update_input_event_symbol(value: &Value, symbol: &str) -> Value {
    match value {
        Value::Symbol(_) => Value::Symbol(symbol.into()),
        Value::Cons(_) => {
            let mut items = value.to_vec().unwrap_or_default();
            if let Some(first) = items.first_mut() {
                *first = Value::Symbol(symbol.into());
            }
            Value::list(items)
        }
        _ => value.clone(),
    }
}

pub(crate) fn first_input_from_link_action(value: &Value) -> Option<Value> {
    if let Some(string) = string_like(value) {
        return string
            .text
            .chars()
            .next()
            .map(|ch| Value::Integer(ch as i64));
    }

    vector_items(value).ok()?.into_iter().next()
}

pub(crate) fn translate_mouse_read_key_sequence_event(
    interp: &mut Interpreter,
    event: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.set_variable("last-input-event", event.clone(), env);

    if input_event_symbol(&event).as_deref() != Some("mouse-1") {
        return Ok(event);
    }
    if !interp
        .lookup_var("mouse-1-click-follows-link", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(event);
    }

    let action = match interp.lookup_function("mouse-on-link-p", env) {
        Ok(function) => {
            interp.call_function_value(function, Some("mouse-on-link-p"), &[Value::Nil], env)?
        }
        Err(_) => Value::Nil,
    };

    if let Some(first) = first_input_from_link_action(&action) {
        interp.set_variable("last-input-event", first.clone(), env);
        return Ok(first);
    }
    if action.is_truthy() {
        let translated = update_input_event_symbol(&event, "mouse-2");
        interp.set_variable("last-input-event", translated.clone(), env);
        return Ok(translated);
    }

    Ok(event)
}

pub(crate) fn read_key_sequence_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    loop {
        let event = if let Some(decoded) = read_decoded_input_event(interp, env)? {
            decoded
        } else {
            normalize_key_event_value(pop_unread_command_event_value(interp, env)?)?
        };
        if is_mouse_down_event(&event) {
            let event_name =
                input_event_symbol(&event).expect("mouse-down events have a symbolic head");
            let key_parts = vec![event_name];
            let mut binding = Value::Nil;
            for map in active_command_keymaps(interp, env)? {
                binding = keymap_lookup_sequence_value_with_default(
                    interp, &map, &key_parts, false, env,
                )?;
                if !binding.is_nil() {
                    break;
                }
            }
            // bindings.el installs this in GNU's dumped global map.  Keep
            // the file-less bootstrap fallback aligned without treating all
            // mouse-down events as bound.
            if binding.is_nil() && key_parts == ["down-mouse-1"] {
                binding = Value::Symbol("mouse-drag-region".into());
            }
            if binding.is_nil() {
                continue;
            }
        }
        return translate_mouse_read_key_sequence_event(interp, event, env);
    }
}

pub(crate) fn record_command_history(
    interp: &mut Interpreter,
    function_name: &str,
    args: Vec<Value>,
    env: &mut Env,
) {
    let mut history = interp
        .lookup_var("command-history", env)
        .unwrap_or(Value::Nil)
        .to_vec()
        .unwrap_or_default();
    let mut entry = vec![Value::Symbol(function_name.to_string().into())];
    entry.extend(args);
    history.insert(0, Value::list(entry));
    if let Some(Value::Integer(length)) = interp.lookup_var("history-length", env) {
        let length = length.max(0) as usize;
        history.truncate(length);
    }
    interp.set_variable("command-history", Value::list(history), env);
}

pub(crate) fn is_declare_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "declare"),
    )
}

pub(crate) fn invalid_interactive_control_letter(ch: char) -> LispError {
    let code = raw_byte_from_regex_char(ch)
        .map(u32::from)
        .unwrap_or(ch as u32);
    let display = char::from_u32(code).unwrap_or(ch);
    LispError::Signal(format!(
        "Invalid control letter `{display}' (#o{code:03o}, #x{code:04x}) in interactive calling string"
    ))
}

pub(crate) fn need_args(name: &str, args: &[Value], n: usize) -> Result<(), LispError> {
    if args.len() < n {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

pub(crate) fn need_arg_range(
    name: &str,
    args: &[Value],
    min: usize,
    max: usize,
) -> Result<(), LispError> {
    if args.len() < min || args.len() > max {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

pub(crate) fn parse_edmacro_key_sequence(source: &str) -> Result<Value, LispError> {
    let mut parser = EdmacroKeyParser::new(source);
    let items = parser.parse()?;
    let mut vector = vec![Value::symbol("vector-literal")];
    vector.extend(items);
    Ok(Value::list(vector))
}

pub(crate) struct EdmacroKeyParser<'a> {
    source: &'a str,
    pos: usize,
}

impl<'a> EdmacroKeyParser<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, pos: 0 }
    }

    fn parse(&mut self) -> Result<Vec<Value>, LispError> {
        let mut items = Vec::new();
        while self.pos < self.source.len() {
            self.skip_whitespace();
            if self.pos >= self.source.len() {
                break;
            }
            if self.starts_comment() {
                self.skip_comment();
                continue;
            }
            let repeat = self.parse_repeat_prefix()?;
            if self.starts_comment() {
                self.skip_comment();
                continue;
            }
            let token = self.read_token();
            if token.is_empty() {
                break;
            }
            let parsed = parse_edmacro_token(token)?;
            for _ in 0..repeat {
                items.extend(parsed.iter().cloned());
            }
        }
        Ok(items)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek_char() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn starts_comment(&self) -> bool {
        let rest = &self.source[self.pos..];
        if rest.starts_with(";;") {
            return true;
        }
        if !rest.starts_with("REM") {
            return false;
        }
        match rest.get(3..).and_then(|tail| tail.chars().next()) {
            None => true,
            Some(ch) => ch.is_whitespace(),
        }
    }

    fn skip_comment(&mut self) {
        while let Some(ch) = self.peek_char() {
            self.pos += ch.len_utf8();
            if ch == '\n' {
                break;
            }
        }
    }

    fn parse_repeat_prefix(&mut self) -> Result<usize, LispError> {
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if !ch.is_ascii_digit() {
                break;
            }
            self.pos += ch.len_utf8();
        }
        if self.pos == start || self.peek_char() != Some('*') {
            self.pos = start;
            return Ok(1);
        }
        let count = self.source[start..self.pos]
            .parse::<usize>()
            .map_err(|error| LispError::Signal(format!("Invalid repetition count: {error}")))?;
        self.pos += 1;
        Ok(count)
    }

    fn read_token(&mut self) -> &'a str {
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
        &self.source[start..self.pos]
    }

    fn peek_char(&self) -> Option<char> {
        self.source[self.pos..].chars().next()
    }
}

pub(crate) fn parse_edmacro_token(token: &str) -> Result<Vec<Value>, LispError> {
    if token.starts_with("<<") && token.ends_with(">>") && token.len() >= 4 {
        let command = &token[2..token.len() - 2];
        let mut items = vec![Value::Integer(apply_edmacro_modifiers(
            'x' as i64, false, true,
        ))];
        items.extend(command.chars().map(|ch| Value::Integer(ch as i64)));
        items.push(Value::Integer('\r' as i64));
        return Ok(items);
    }

    if let Some(key) = parse_modified_edmacro_key(token)? {
        return Ok(vec![Value::Integer(key)]);
    }

    if let Some(key) = parse_named_edmacro_key(token) {
        return Ok(vec![Value::Integer(key)]);
    }

    Ok(token.chars().map(|ch| Value::Integer(ch as i64)).collect())
}

pub(crate) fn parse_modified_edmacro_key(token: &str) -> Result<Option<i64>, LispError> {
    let mut ctrl = false;
    let mut meta = false;
    let mut shift = false;
    let mut super_key = false;
    let mut rest = token;

    while let Some((prefix, tail)) = rest.split_once('-') {
        match prefix {
            "C" => ctrl = true,
            "M" => meta = true,
            "S" => shift = true,
            "s" => super_key = true,
            _ => return Ok(None),
        }
        rest = tail;
    }

    if !(ctrl || meta || shift || super_key) {
        return Ok(None);
    }

    let base = if let Some(key) = parse_named_edmacro_key(rest) {
        key
    } else if rest.chars().count() == 1 {
        rest.chars().next().expect("count checked") as i64
    } else {
        return Ok(None);
    };

    let mut key = apply_edmacro_modifiers(base, ctrl, meta);
    if shift {
        key |= 1 << 25;
    }
    if super_key {
        key |= 1 << 23;
    }
    Ok(Some(key))
}

pub(crate) fn apply_edmacro_modifiers(mut value: i64, ctrl: bool, meta: bool) -> i64 {
    if ctrl && value != 0 {
        value = match value {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    }
    if meta {
        value |= 1 << 27;
    }
    value
}

pub(crate) fn parse_named_edmacro_key(token: &str) -> Option<i64> {
    match token {
        "NUL" => Some(0),
        "TAB" => Some('\t' as i64),
        "LFD" => Some('\n' as i64),
        "RET" => Some('\r' as i64),
        "ESC" => Some(0x1b),
        "SPC" => Some(' ' as i64),
        "DEL" => Some(0x7f),
        _ => None,
    }
}

pub(crate) fn is_composed_accessor_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() >= 3
        && bytes.first() == Some(&b'c')
        && bytes.last() == Some(&b'r')
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| matches!(byte, b'a' | b'd'))
}
