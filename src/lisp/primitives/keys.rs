use super::*;

pub(crate) const KEY_DESCRIPTION_ALT_BIT: i64 = 0x0400000;
pub(crate) const KEY_DESCRIPTION_SUPER_BIT: i64 = 0x0800000;
pub(crate) const KEY_DESCRIPTION_HYPER_BIT: i64 = 0x1000000;
pub(crate) const KEY_DESCRIPTION_SHIFT_BIT: i64 = 0x2000000;
pub(crate) const KEY_DESCRIPTION_CTRL_BIT: i64 = 0x4000000;
pub(crate) const KEY_DESCRIPTION_META_BIT: i64 = 0x8000000;
pub(crate) const KEY_DESCRIPTION_MODIFIER_MASK: i64 = KEY_DESCRIPTION_ALT_BIT
    | KEY_DESCRIPTION_SUPER_BIT
    | KEY_DESCRIPTION_HYPER_BIT
    | KEY_DESCRIPTION_SHIFT_BIT
    | KEY_DESCRIPTION_CTRL_BIT
    | KEY_DESCRIPTION_META_BIT;
pub(crate) const KEY_DESCRIPTION_META_PREFIX: i64 = 0x1B;

const EVENT_UP_BIT: i64 = 1;
const EVENT_DOWN_BIT: i64 = 1 << 1;
const EVENT_DRAG_BIT: i64 = 1 << 2;
const EVENT_CLICK_BIT: i64 = 1 << 3;
const EVENT_DOUBLE_BIT: i64 = 1 << 4;
const EVENT_TRIPLE_BIT: i64 = 1 << 5;

pub(crate) fn solitary_event_modifier(name: &str) -> i64 {
    match name {
        "A" | "alt" => KEY_DESCRIPTION_ALT_BIT,
        "C" | "ctrl" | "control" => KEY_DESCRIPTION_CTRL_BIT,
        "H" | "hyper" => KEY_DESCRIPTION_HYPER_BIT,
        "M" | "meta" => KEY_DESCRIPTION_META_BIT,
        "S" | "shift" => KEY_DESCRIPTION_SHIFT_BIT,
        "s" | "super" => KEY_DESCRIPTION_SUPER_BIT,
        "up" => EVENT_UP_BIT,
        "down" => EVENT_DOWN_BIT,
        "drag" => EVENT_DRAG_BIT,
        "click" => EVENT_CLICK_BIT,
        "double" => EVENT_DOUBLE_BIT,
        "triple" => EVENT_TRIPLE_BIT,
        _ => 0,
    }
}

fn modified_event_symbol_name(modifiers: i64, base: &str) -> String {
    let mut name = String::new();
    for (bit, prefix) in [
        (KEY_DESCRIPTION_ALT_BIT, "A-"),
        (KEY_DESCRIPTION_CTRL_BIT, "C-"),
        (KEY_DESCRIPTION_HYPER_BIT, "H-"),
        (KEY_DESCRIPTION_META_BIT, "M-"),
        (KEY_DESCRIPTION_SHIFT_BIT, "S-"),
        (KEY_DESCRIPTION_SUPER_BIT, "s-"),
        (EVENT_DOUBLE_BIT, "double-"),
        (EVENT_TRIPLE_BIT, "triple-"),
        (EVENT_UP_BIT, "up-"),
        (EVENT_DOWN_BIT, "down-"),
        (EVENT_DRAG_BIT, "drag-"),
    ] {
        if modifiers & bit != 0 {
            name.push_str(prefix);
        }
    }
    // click is represented by the absence of the other mouse prefixes.
    name.push_str(base);
    name
}

/// keyboard.c:make_ctrl_char.  The upper-case column folds to a control
/// char (a shifted LETTER also gains the shift modifier), lower-case
/// letters fold, and every other printable code keeps the control
/// modifier bit because "the basic ASCII code can't indicate" it.
fn make_ctrl_event_code(code: i64) -> i64 {
    let upper = code & !0o177;
    if !(0..128).contains(&code) {
        return code | KEY_DESCRIPTION_CTRL_BIT;
    }
    let mut c = code & 0o177;
    if (0o100..0o140).contains(&c) {
        let oc = c;
        c ^= 0o100;
        if (b'A' as i64..=b'Z' as i64).contains(&oc) {
            c |= KEY_DESCRIPTION_SHIFT_BIT;
        }
    } else if (b'a' as i64..=b'z' as i64).contains(&c) {
        c &= !0o140;
    } else if c >= b' ' as i64 {
        c |= KEY_DESCRIPTION_CTRL_BIT;
    }
    c | upper
}

pub(crate) fn event_convert_list_value(
    interp: &mut Interpreter,
    event_description: &Value,
) -> Result<Value, LispError> {
    let items = event_description.to_vec()?;
    let mut base = None;
    let mut modifiers = 0;
    for (index, item) in items.iter().enumerate() {
        let modifier = if index + 1 < items.len() {
            item.as_symbol()
                .ok()
                .map(solitary_event_modifier)
                .unwrap_or(0)
        } else {
            0
        };
        if modifier != 0 {
            modifiers |= modifier;
        } else if base.is_some() {
            return Err(LispError::Signal("Two bases given in one event".into()));
        } else {
            base = Some(item.clone());
        }
    }

    let mut base = base.unwrap_or(Value::Nil);
    if let Value::Symbol(symbol) = &base
        && symbol.chars().count() == 1
    {
        base = Value::Integer(symbol.chars().next().expect("one character") as i64);
    }
    match base {
        Value::Integer(mut code) => {
            if modifiers & KEY_DESCRIPTION_SHIFT_BIT != 0
                && (b'a' as i64..=b'z' as i64).contains(&code)
            {
                code -= i64::from(b'a' - b'A');
                modifiers &= !KEY_DESCRIPTION_SHIFT_BIT;
            }
            if modifiers & KEY_DESCRIPTION_CTRL_BIT != 0 {
                // keyboard.c:Fevent_convert_list: the control fold goes
                // through make_ctrl_char, which may keep the control bit
                // (C-9) or add a shift bit (control of a shifted letter).
                code = make_ctrl_event_code(code);
                modifiers &= !KEY_DESCRIPTION_CTRL_BIT;
            }
            Ok(Value::Integer(code | modifiers))
        }
        Value::Symbol(base) => {
            let modified = modified_event_symbol_name(modifiers, &base);
            if let Some(kind) = interp.get_symbol_property(&base, "event-kind") {
                interp.put_symbol_property(&modified, "event-kind", kind);
            }
            // keyboard.c reaches this through apply_modifiers, which interns
            // the modified symbol and stamps its parse cache
            // (`event-symbol-element-mask'/`event-symbol-elements') --
            // observable on any batch GNU after bindings.el's keypad
            // `define-key' loop runs.
            let modified = Value::Symbol(modified.into());
            interp.intern_symbols_in_value(&modified);
            parse_event_symbol_modifiers(interp, &modified)?;
            Ok(modified)
        }
        _ => Err(LispError::Signal("Invalid base event".into())),
    }
}

fn event_modifier_elements(modifiers: i64) -> Vec<Value> {
    [
        (KEY_DESCRIPTION_META_BIT, "meta"),
        (KEY_DESCRIPTION_CTRL_BIT, "control"),
        (KEY_DESCRIPTION_SHIFT_BIT, "shift"),
        (KEY_DESCRIPTION_HYPER_BIT, "hyper"),
        (KEY_DESCRIPTION_SUPER_BIT, "super"),
        (KEY_DESCRIPTION_ALT_BIT, "alt"),
        (EVENT_TRIPLE_BIT, "triple"),
        (EVENT_DOUBLE_BIT, "double"),
        (EVENT_CLICK_BIT, "click"),
        (EVENT_DRAG_BIT, "drag"),
        (EVENT_DOWN_BIT, "down"),
        (EVENT_UP_BIT, "up"),
    ]
    .into_iter()
    .filter(|(bit, _)| modifiers & bit != 0)
    .map(|(_, name)| Value::symbol(name))
    .collect()
}

pub(crate) fn parse_event_symbol_modifiers(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    let symbol = value.as_symbol()?;
    if interp
        .get_symbol_property(symbol, "event-symbol-element-mask")
        .is_some_and(|value| value.cons_values().is_some())
    {
        return Ok(interp
            .get_symbol_property(symbol, "event-symbol-elements")
            .unwrap_or(Value::Nil));
    }

    let mut modifiers = 0;
    let mut offset = 0;
    while offset + 1 < symbol.len() {
        let rest = &symbol[offset..];
        let parsed = [
            ("A-", KEY_DESCRIPTION_ALT_BIT),
            ("C-", KEY_DESCRIPTION_CTRL_BIT),
            ("H-", KEY_DESCRIPTION_HYPER_BIT),
            ("M-", KEY_DESCRIPTION_META_BIT),
            ("S-", KEY_DESCRIPTION_SHIFT_BIT),
            ("s-", KEY_DESCRIPTION_SUPER_BIT),
            ("drag-", EVENT_DRAG_BIT),
            ("down-", EVENT_DOWN_BIT),
            ("double-", EVENT_DOUBLE_BIT),
            ("triple-", EVENT_TRIPLE_BIT),
            ("up-", EVENT_UP_BIT),
        ]
        .into_iter()
        .find(|(prefix, _)| rest.starts_with(prefix));
        let Some((prefix, bit)) = parsed else {
            break;
        };
        modifiers |= bit;
        offset += prefix.len();
    }
    let base = &symbol[offset..];
    let mouse_click =
        modifiers & (EVENT_DOWN_BIT | EVENT_DRAG_BIT | EVENT_DOUBLE_BIT | EVENT_TRIPLE_BIT) == 0
            && ((base
                .strip_prefix("mouse-")
                .is_some_and(|button| button.len() == 1 && button.as_bytes()[0].is_ascii_digit()))
                || base.starts_with("wheel-"));
    if mouse_click {
        modifiers |= EVENT_CLICK_BIT;
    }

    let base = crate::lisp::types::interned_symbol_value(base.to_string());
    let element_mask = Value::list([base.clone(), Value::Integer(modifiers)]);
    let elements =
        Value::list(std::iter::once(base.clone()).chain(event_modifier_elements(modifiers)));
    interp.put_symbol_property(symbol, "event-symbol-element-mask", element_mask);
    interp.put_symbol_property(symbol, "event-symbol-elements", elements.clone());
    Ok(elements)
}

pub(crate) fn parse_kbd_sequence(text: &str) -> Result<Value, LispError> {
    let mut items = vec![Value::Symbol("vector-literal".into())];
    for token in text.split_whitespace() {
        items.extend(parse_kbd_token(token));
    }
    Ok(Value::list(items))
}

pub(crate) fn parse_kbd_token(token: &str) -> Vec<Value> {
    if token.chars().count() == 1 {
        return token.chars().map(|ch| Value::Integer(ch as i64)).collect();
    }
    let (modifiers, rest, saw_prefix) = parse_kbd_prefixes(token);
    if saw_prefix {
        if rest.starts_with('<') && rest.ends_with('>') && rest.len() >= 2 {
            return vec![Value::Symbol(
                symbolic_kbd_event(modifiers, &rest[1..rest.len() - 1]).into(),
            )];
        }
        if rest == "ESC" {
            return vec![Value::Symbol(
                symbolic_kbd_event(modifiers, "escape").into(),
            )];
        }
        if let Some(code) = named_kbd_key_code(rest) {
            return vec![Value::Integer(code | modifiers)];
        }
        if rest.chars().count() == 1 {
            return rest
                .chars()
                .map(|ch| Value::Integer(ch as i64 | modifiers))
                .collect();
        }
        return vec![Value::Symbol(symbolic_kbd_event(modifiers, rest).into())];
    }
    if token.starts_with('<') && token.ends_with('>') && token.len() >= 2 {
        return vec![Value::Symbol(token[1..token.len() - 1].to_string().into())];
    }
    if token == "ESC" {
        return vec![Value::Integer(KEY_DESCRIPTION_META_PREFIX)];
    }
    if let Some(code) = named_kbd_key_code(token) {
        return vec![Value::Integer(code)];
    }
    token.chars().map(|ch| Value::Integer(ch as i64)).collect()
}

pub(crate) fn parse_kbd_prefixes(token: &str) -> (i64, &str, bool) {
    let mut modifiers = 0;
    let mut rest = token;
    let mut saw_prefix = false;
    while rest.len() >= 3 && rest.as_bytes()[1] == b'-' {
        let prefix = rest.as_bytes()[0] as char;
        let bit = match prefix {
            'A' => KEY_DESCRIPTION_ALT_BIT,
            'C' => KEY_DESCRIPTION_CTRL_BIT,
            'H' => KEY_DESCRIPTION_HYPER_BIT,
            'M' => KEY_DESCRIPTION_META_BIT,
            'S' => KEY_DESCRIPTION_SHIFT_BIT,
            's' => KEY_DESCRIPTION_SUPER_BIT,
            _ => break,
        };
        modifiers |= bit;
        rest = &rest[2..];
        saw_prefix = true;
    }
    (modifiers, rest, saw_prefix)
}

pub(crate) fn named_kbd_key_code(token: &str) -> Option<i64> {
    match token {
        "RET" => Some('\r' as i64),
        "LFD" => Some('\n' as i64),
        "TAB" => Some('\t' as i64),
        "DEL" => Some(0x7F),
        "ESC" => Some(KEY_DESCRIPTION_META_PREFIX),
        "SPC" => Some(0x20),
        _ => None,
    }
}

pub(crate) fn symbolic_kbd_event(modifiers: i64, name: &str) -> String {
    let mut symbol = String::new();
    if modifiers & KEY_DESCRIPTION_ALT_BIT != 0 {
        symbol.push_str("A-");
    }
    if modifiers & KEY_DESCRIPTION_CTRL_BIT != 0 {
        symbol.push_str("C-");
    }
    if modifiers & KEY_DESCRIPTION_HYPER_BIT != 0 {
        symbol.push_str("H-");
    }
    if modifiers & KEY_DESCRIPTION_META_BIT != 0 {
        symbol.push_str("M-");
    }
    if modifiers & KEY_DESCRIPTION_SHIFT_BIT != 0 {
        symbol.push_str("S-");
    }
    if modifiers & KEY_DESCRIPTION_SUPER_BIT != 0 {
        symbol.push_str("s-");
    }
    symbol.push_str(name);
    symbol
}

pub(crate) fn key_sequence_binding_text(value: &Value) -> Result<String, LispError> {
    Ok(key_sequence_binding_parts(value)?.join(" "))
}

/// Parse the descriptive key spelling accepted by the `keymap-*' Lisp API.
///
/// This is intentionally separate from `key_sequence_binding_parts': GNU's
/// older primitives (`define-key', `lookup-key', `key-binding', ...) treat a
/// string as the raw sequence of characters it contains, while `keymap-set'
/// and friends explicitly pass their strings through `key-parse'.  Guessing
/// from spaces is observably wrong for raw bindings such as `"\C-c, "'.
pub(crate) fn textual_key_sequence_binding_parts(value: &Value) -> Result<Vec<String>, LispError> {
    if let Some(string) = string_like(value) {
        return key_sequence_binding_parts(&parse_kbd_sequence(&string.text)?);
    }
    key_sequence_binding_parts(value)
}

pub(crate) fn key_sequence_binding_parts(value: &Value) -> Result<Vec<String>, LispError> {
    if let Ok(events) = vector_items(value)
        && let [event] = events.as_slice()
    {
        match event {
            Value::Symbol(symbol) => return Ok(vec![symbol.to_string()]),
            // GNU renders the [t] default binding as "<t>", which also keeps
            // it distinct from a binding on the letter t.
            Value::T => return Ok(vec!["<t>".into()]),
            _ => {}
        }
    }
    let mut parts = Vec::new();
    append_key_description_parts(value, &mut parts)?;
    Ok(parts)
}

/// keymap.c's Fdefine_key/lookup_key_1 convert each Lucid-style event
/// description in a key vector through Fevent_convert_list before storing
/// or traversing.  When `symbols-with-pos-enabled' is non-nil, GNU's SYMBOLP
/// and XSYMBOL also make a positioned event name the same key as its bare
/// symbol.  Returns KEY unchanged when no element needs conversion.
pub(crate) fn normalize_lucid_key_events(
    interp: &mut Interpreter,
    key: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let Ok(events) = vector_items(key) else {
        return Ok(key.clone());
    };
    let positions_enabled = symbols_with_pos_enabled(interp, env);
    if !events.iter().any(|event| {
        lucid_event_type_list_p(event)
            || (positions_enabled && symbol_with_pos_parts(interp, event).is_some())
    }) {
        return Ok(key.clone());
    }
    let mut converted = vec![Value::Symbol("vector-literal".into())];
    for event in events {
        let event = if lucid_event_type_list_p(&event) {
            event_convert_list_value(interp, &event)?
        } else {
            event
        };
        converted.push(if positions_enabled {
            symbol_with_pos_parts(interp, &event)
                .map(|(symbol, _)| symbol)
                .unwrap_or(event)
        } else {
            event
        });
    }
    Ok(Value::list(converted))
}

/// keymap.c's Fdefine_key: a DEF vector whose first element is a cons "is
/// apparently an XEmacs-style keyboard macro" -- every Lucid event
/// description in it is converted through Fevent_convert_list.
pub(crate) fn normalize_xemacs_macro_definition(
    interp: &mut Interpreter,
    def: &Value,
) -> Result<Value, LispError> {
    let Ok(events) = vector_items(def) else {
        return Ok(def.clone());
    };
    if !matches!(events.first(), Some(Value::Cons(_))) {
        return Ok(def.clone());
    }
    let mut converted = vec![Value::Symbol("vector-literal".into())];
    for event in events {
        converted.push(if lucid_event_type_list_p(&event) {
            event_convert_list_value(interp, &event)?
        } else {
            event
        });
    }
    Ok(Value::list(converted))
}

/// keyboard.c's lucid_event_type_list_p: a proper list of fixnums and
/// symbols whose head is not one of the posn-bearing pseudo-event kinds.
fn lucid_event_type_list_p(event: &Value) -> bool {
    if !matches!(event, Value::Cons(_)) {
        return false;
    }
    // GNU's CONSP is false for a real vector; Emaxx's vector-literal
    // facade uses cons storage, so exclude it explicitly.
    if crate::lisp::primitives::interactive::is_vector_value(event) {
        return false;
    }
    if matches!(
        event.car(),
        Ok(Value::Symbol(head)) if head == "help-echo"
            || head == "vertical-line"
            || head == "mode-line"
            || head == "tab-line"
            || head == "header-line"
    ) {
        return false;
    }
    let Ok(items) = event.to_vec() else {
        return false;
    };
    items
        .iter()
        .all(|item| matches!(item, Value::Integer(_) | Value::Symbol(_)))
}

/// Return the event path used to store or traverse a keymap binding.
///
/// GNU represents Meta-modified character keys as an ESC prefix followed by
/// the character event.  Keeping the display spelling (`M-v') as a synthetic
/// root event changes full-keymap ordering and prevents the ESC prefix map
/// from being discoverable.  Symbolic events such as `M-<up>' remain single
/// events, just as they do in GNU.
pub(crate) fn key_sequence_keymap_parts(value: &Value) -> Result<Vec<String>, LispError> {
    // A string stored inside a vector is GNU's legacy spelling for one
    // already-described key sequence: ["C-x C-f"] names the same events as
    // (kbd "C-x C-f"), it is not a single opaque string event.  Plain string
    // KEY arguments remain raw character sequences in the legacy keymap API.
    if let Ok(events) = vector_items(value)
        && let [event] = events.as_slice()
        && let Some(string) = string_like(event)
    {
        return keymap_parts_from_display_parts(key_sequence_binding_parts(&parse_kbd_sequence(
            &string.text,
        )?)?);
    }
    // keymap.c's access_keymap traverses a parameterized event by its
    // EVENT_HEAD: (C-down-mouse-3 POSN) inside a key vector looks up as
    // the bare C-down-mouse-3 symbol.  Lucid-style event descriptions such
    // as (control ?c) are not events; lookup converts those through
    // event-convert-list instead of taking their car.
    if let Ok(events) = vector_items(value)
        && events.iter().any(|event| {
            event.cons_values().is_some()
                && !lucid_event_type_list_p(event)
                && !crate::lisp::primitives::interactive::is_vector_value(event)
        })
    {
        let heads = events.into_iter().map(|event| {
            if lucid_event_type_list_p(&event)
                || crate::lisp::primitives::interactive::is_vector_value(&event)
            {
                return event;
            }
            // keymap.c:lookup_key_1 converts proper Lucid event lists first;
            // access_keymap_1 then applies EVENT_HEAD to every remaining
            // cons event.  That includes both parameterized events and the
            // dotted (FROM . TO) character ranges emitted by map-keymap.
            event.car().unwrap_or(event)
        });
        let vector =
            Value::list(std::iter::once(Value::Symbol("vector-literal".into())).chain(heads));
        return keymap_parts_from_display_parts(key_sequence_binding_parts(&vector)?);
    }
    keymap_parts_from_display_parts(key_sequence_binding_parts(value)?)
}

pub(crate) fn key_sequence_prefix_event_count(
    value: &Value,
    keymap_prefix_len: usize,
) -> Result<usize, LispError> {
    let mut consumed_parts = 0;
    let mut consumed_events = 0;
    for event in key_description_events(value)? {
        let sequence = Value::list([Value::Symbol("vector-literal".into()), event]);
        consumed_parts += key_sequence_keymap_parts(&sequence)?.len();
        if consumed_parts > keymap_prefix_len {
            break;
        }
        consumed_events += 1;
    }
    Ok(consumed_events)
}

pub(crate) fn textual_key_sequence_keymap_parts(value: &Value) -> Result<Vec<String>, LispError> {
    keymap_parts_from_display_parts(textual_key_sequence_binding_parts(value)?)
}

fn keymap_parts_from_display_parts(display_parts: Vec<String>) -> Result<Vec<String>, LispError> {
    let mut parts = Vec::with_capacity(display_parts.len());
    for part in display_parts {
        let events = parse_kbd_token(&part);
        if let [Value::Integer(code)] = events.as_slice()
            && code & KEY_DESCRIPTION_META_BIT != 0
        {
            parts.push("ESC".into());
            parts.push(describe_key_code(code & !KEY_DESCRIPTION_META_BIT));
        } else {
            parts.push(part);
        }
    }
    Ok(parts)
}

pub(crate) fn append_key_description_parts(
    sequence: &Value,
    output: &mut Vec<String>,
) -> Result<(), LispError> {
    let events = key_description_events(sequence)?;
    let mut add_meta = false;
    for event in events {
        if add_meta {
            match event {
                Value::Integer(code) if code == KEY_DESCRIPTION_META_PREFIX => {
                    output.push(describe_key_code(KEY_DESCRIPTION_META_PREFIX));
                    add_meta = true;
                }
                Value::Integer(code)
                    if code != KEY_DESCRIPTION_META_PREFIX
                        && code & KEY_DESCRIPTION_META_BIT == 0 =>
                {
                    output.push(describe_key_code(code | KEY_DESCRIPTION_META_BIT));
                    add_meta = false;
                }
                other => {
                    output.push(describe_key_code(KEY_DESCRIPTION_META_PREFIX));
                    output.push(single_key_description_text(&other, false)?);
                    add_meta = false;
                }
            }
            continue;
        }

        if matches!(&event, Value::Integer(code) if *code == KEY_DESCRIPTION_META_PREFIX) {
            add_meta = true;
            continue;
        }

        output.push(single_key_description_text(&event, false)?);
    }

    if add_meta {
        output.push(describe_key_code(KEY_DESCRIPTION_META_PREFIX));
    }

    Ok(())
}

pub(crate) fn key_description_events(sequence: &Value) -> Result<Vec<Value>, LispError> {
    if let Some(string) = string_like(sequence) {
        let mut events = Vec::new();
        for ch in string.text.chars() {
            if !string.multibyte {
                if let Some(byte) = raw_byte_from_regex_char(ch) {
                    let code = if byte & 0x80 != 0 {
                        ((byte ^ 0x80) as i64) | KEY_DESCRIPTION_META_BIT
                    } else {
                        byte as i64
                    };
                    events.push(Value::Integer(code));
                    continue;
                }
                let code = ch as u32;
                if code <= 0xFF {
                    let byte = code as u8;
                    let normalized = if byte & 0x80 != 0 {
                        ((byte ^ 0x80) as i64) | KEY_DESCRIPTION_META_BIT
                    } else {
                        byte as i64
                    };
                    events.push(Value::Integer(normalized));
                    continue;
                }
            }
            events.push(Value::Integer(ch as i64));
        }
        return Ok(events);
    }

    match sequence {
        Value::Nil => Ok(Vec::new()),
        value if is_vector_value(value) => Ok(vector_items(sequence)?
            .into_iter()
            .map(normalize_key_description_event)
            .collect()),
        Value::Integer(_) | Value::Symbol(_) => {
            Ok(vec![normalize_key_description_event(sequence.clone())])
        }
        _ => Err(LispError::WrongTypeArgument(
            "arrayp".into(),
            sequence.clone(),
        )),
    }
}

pub(crate) fn normalize_key_description_event(event: Value) -> Value {
    let Some((start, end)) = event.cons_values() else {
        return event;
    };
    match (start, end) {
        (Value::Integer(start), Value::Integer(end)) if start == end => Value::Integer(start),
        _ => event,
    }
}

pub(crate) fn sequence_values(
    interp: &Interpreter,
    sequence: &Value,
) -> Result<Vec<Value>, LispError> {
    if let Some(string) = sequence_string_like(sequence) {
        Ok(string_sequence_values(&string))
    } else if let Some(items) = keymap_list_items(interp, sequence)? {
        Ok(items)
    } else if matches!(sequence, Value::Nil | Value::Cons(_)) {
        sequence.to_vec()
    } else if is_bool_vector_value(interp, sequence) {
        bool_vector_values(interp, sequence)
    } else {
        vector_items(sequence)
    }
}

pub(crate) fn string_sequence_values(string: &StringLike) -> Vec<Value> {
    string
        .character_codes()
        .into_iter()
        .map(Value::Integer)
        .collect()
}

pub(crate) fn string_sequence_value(string: &StringLike, ch: char) -> Value {
    let code = if let Some(byte) = raw_byte_from_regex_char(ch) {
        if string.multibyte {
            RAW_BYTE8_BASE as i64 + byte as i64
        } else {
            i64::from(byte)
        }
    } else {
        ch as i64
    };
    Value::Integer(code)
}

pub(crate) fn concat_character_value(value: &Value) -> Result<(char, bool), LispError> {
    let Value::Integer(code) = value else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol("characterp".into()),
            value.clone(),
        ])));
    };
    if *code < 0 {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol("characterp".into()),
            value.clone(),
        ])));
    }
    if (RAW_BYTE8_BASE as i64..=RAW_BYTE8_BASE as i64 + 0xFF).contains(code) {
        let byte = (*code - RAW_BYTE8_BASE as i64) as u8;
        return Ok((raw_byte_regex_char(byte), false));
    }
    let Some(ch) = char::from_u32(*code as u32) else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol("characterp".into()),
            value.clone(),
        ])));
    };
    Ok((ch, !is_raw_byte_regex_char(ch) && (*code as u32) > 0x7F))
}

pub(crate) fn concat_sequence_string(
    interp: &Interpreter,
    value: &Value,
) -> Result<(String, bool), LispError> {
    let items = sequence_values(interp, value)?;
    let mut text = String::new();
    let mut multibyte = false;
    for item in items {
        let (ch, char_multibyte) = concat_character_value(&item)?;
        text.push(ch);
        multibyte |= char_multibyte;
    }
    Ok((text, multibyte))
}

pub(crate) fn sequence_string_like(value: &Value) -> Option<StringLike> {
    match value {
        Value::String(_) | Value::StringObject(_) => string_like(value),
        Value::Cons(_) => {
            let items = value.to_vec().ok()?;
            if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector-literal")
                && matches!(items.get(1), Some(Value::String(_)))
            {
                string_like(value)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn single_key_description_text(
    key: &Value,
    no_angles: bool,
) -> Result<String, LispError> {
    match key {
        Value::Nil => Ok(describe_symbolic_key("nil", no_angles)),
        Value::Integer(code) => Ok(describe_key_code(*code)),
        Value::Symbol(symbol) => Ok(describe_symbolic_key(symbol, no_angles)),
        Value::T => Ok(describe_symbolic_key("t", no_angles)),
        Value::String(text) => Ok(text.to_string()),
        Value::StringObject(state) => Ok(state.borrow().text.clone()),
        Value::Cons(_) => list_event_key_description_text(key, no_angles),
        _ => Err(LispError::TypeError(
            "integer, symbol, or string".into(),
            key.type_name(),
        )),
    }
}

pub(crate) fn list_event_key_description_text(
    key: &Value,
    no_angles: bool,
) -> Result<String, LispError> {
    let items = key.to_vec()?;
    let Some((base, modifiers)) = items.split_last() else {
        return Err(LispError::TypeError(
            "integer, symbol, or string".into(),
            key.type_name(),
        ));
    };

    let mut bits = 0;
    for modifier in modifiers {
        bits |= match modifier.as_symbol()? {
            "alt" => KEY_DESCRIPTION_ALT_BIT,
            "control" => KEY_DESCRIPTION_CTRL_BIT,
            "hyper" => KEY_DESCRIPTION_HYPER_BIT,
            "meta" => KEY_DESCRIPTION_META_BIT,
            "shift" => KEY_DESCRIPTION_SHIFT_BIT,
            "super" => KEY_DESCRIPTION_SUPER_BIT,
            _ => {
                return Err(LispError::TypeError(
                    "event modifier".into(),
                    modifier.type_name(),
                ));
            }
        };
    }

    match base {
        Value::Integer(code) => Ok(describe_key_code(*code | bits)),
        Value::Symbol(symbol) => {
            if let Some(ch) = event_name_character(symbol) {
                Ok(describe_key_code(ch as i64 | bits))
            } else {
                Ok(describe_symbolic_key(
                    &symbolic_kbd_event(bits, symbol),
                    no_angles,
                ))
            }
        }
        Value::String(text) => {
            if let Some(ch) = event_name_character(text) {
                Ok(describe_key_code(ch as i64 | bits))
            } else if bits == 0 {
                Ok(text.to_string())
            } else {
                Ok(describe_symbolic_key(
                    &symbolic_kbd_event(bits, text),
                    no_angles,
                ))
            }
        }
        Value::StringObject(state) => {
            let text = state.borrow().text.clone();
            if let Some(ch) = event_name_character(&text) {
                Ok(describe_key_code(ch as i64 | bits))
            } else if bits == 0 {
                Ok(text)
            } else {
                Ok(describe_symbolic_key(
                    &symbolic_kbd_event(bits, &text),
                    no_angles,
                ))
            }
        }
        _ => Err(LispError::TypeError(
            "integer, symbol, or string".into(),
            base.type_name(),
        )),
    }
}

pub(crate) fn event_name_character(text: &str) -> Option<char> {
    let mut chars = text.chars();
    let ch = chars.next()?;
    chars.next().is_none().then_some(ch)
}

pub(crate) fn describe_symbolic_key(symbol: &str, no_angles: bool) -> String {
    if no_angles {
        return symbol.to_string();
    }

    let bytes = symbol.as_bytes();
    let mut prefix_len = 0usize;
    while prefix_len + 3 <= bytes.len()
        && bytes[prefix_len + 1] == b'-'
        && matches!(bytes[prefix_len], b'C' | b'M' | b'S' | b's' | b'H' | b'A')
    {
        prefix_len += 2;
    }

    format!("{}<{}>", &symbol[..prefix_len], &symbol[prefix_len..])
}

pub(crate) fn describe_key_code(code: i64) -> String {
    let mut text = String::new();
    let mut code = code & (KEY_DESCRIPTION_META_BIT | !-KEY_DESCRIPTION_META_BIT);
    let base = code & !KEY_DESCRIPTION_MODIFIER_MASK;
    let Some(_) = char::from_u32(base as u32) else {
        return format!("[{code}]");
    };

    let tab_as_ci = base == '\t' as i64 && code & KEY_DESCRIPTION_META_BIT != 0;

    if code & KEY_DESCRIPTION_ALT_BIT != 0 {
        text.push_str("A-");
        code &= !KEY_DESCRIPTION_ALT_BIT;
    }
    if code & KEY_DESCRIPTION_CTRL_BIT != 0
        || (base < ' ' as i64
            && base != KEY_DESCRIPTION_META_PREFIX
            && base != '\t' as i64
            && base != '\r' as i64)
        || tab_as_ci
    {
        text.push_str("C-");
        code &= !KEY_DESCRIPTION_CTRL_BIT;
    }
    if code & KEY_DESCRIPTION_HYPER_BIT != 0 {
        text.push_str("H-");
        code &= !KEY_DESCRIPTION_HYPER_BIT;
    }
    if code & KEY_DESCRIPTION_META_BIT != 0 {
        text.push_str("M-");
        code &= !KEY_DESCRIPTION_META_BIT;
    }
    if code & KEY_DESCRIPTION_SHIFT_BIT != 0 {
        text.push_str("S-");
        code &= !KEY_DESCRIPTION_SHIFT_BIT;
    }
    if code & KEY_DESCRIPTION_SUPER_BIT != 0 {
        text.push_str("s-");
        code &= !KEY_DESCRIPTION_SUPER_BIT;
    }

    match code {
        0x00..=0x1F => {
            if code == KEY_DESCRIPTION_META_PREFIX {
                text.push_str("ESC");
            } else if tab_as_ci {
                text.push('i');
            } else if code == '\t' as i64 {
                text.push_str("TAB");
            } else if code == '\r' as i64 {
                text.push_str("RET");
            } else if (1..=26).contains(&code) {
                text.push((code as u8 + b'`') as char);
            } else {
                text.push((code as u8 + b'@') as char);
            }
        }
        0x20 => text.push_str("SPC"),
        0x7F => text.push_str("DEL"),
        0x21..=0x7E => text.push(char::from_u32(code as u32).expect("ascii codepoint is valid")),
        _ => {
            if let Some(ch) = char::from_u32(code as u32) {
                text.push(ch);
            } else {
                return format!("[{code}]");
            }
        }
    }

    text
}

pub(crate) fn text_char_description_text(code: i64) -> Result<String, LispError> {
    if !(0..=0x3F_FFFF).contains(&code) || code & KEY_DESCRIPTION_MODIFIER_MASK != 0 {
        return Err(LispError::Signal("Invalid character".into()));
    }

    if code > 0x7F {
        return char::from_u32(code as u32)
            .map(|ch| ch.to_string())
            .ok_or_else(|| LispError::Signal("Invalid character".into()));
    }

    Ok(match code {
        0x00..=0x1F => format!("^{}", char::from_u32((code + 64) as u32).unwrap_or('@')),
        0x7F => "^?".into(),
        _ => char::from_u32(code as u32)
            .ok_or_else(|| LispError::Signal("Invalid character".into()))?
            .to_string(),
    })
}

pub(crate) fn auto_save_path_for_buffer(buffer: &crate::buffer::Buffer) -> String {
    if let Some(path) = &buffer.file {
        format!("{path}#")
    } else {
        std::env::temp_dir()
            .join(format!("{}.autosave", buffer.name.replace('/', "_")))
            .display()
            .to_string()
    }
}

pub(crate) fn forward_line_bigint(buffer: &mut crate::buffer::Buffer, n: BigInt) -> BigInt {
    if n.is_zero() {
        let _ = buffer.forward_line(0);
        return BigInt::zero();
    }

    let max_isize = BigInt::from(isize::MAX);
    let min_isize = BigInt::from(isize::MIN);
    if n >= min_isize && n <= max_isize {
        let step = match n.to_isize() {
            Some(value) => value,
            None => return BigInt::zero(),
        };
        return BigInt::from(buffer.forward_line(step) as i64);
    }

    if n.sign() == Sign::Minus {
        let available = count_backward_line_moves(buffer);
        move_line_steps(buffer, available, false);
        n + BigInt::from(available)
    } else {
        let available = count_forward_line_moves(buffer);
        move_line_steps(buffer, available, true);
        n - BigInt::from(available)
    }
}

pub(crate) fn move_line_steps(buffer: &mut crate::buffer::Buffer, mut steps: usize, forward: bool) {
    while steps > 0 {
        let chunk = steps.min(isize::MAX as usize);
        let _ = buffer.forward_line(if forward {
            chunk as isize
        } else {
            -(chunk as isize)
        });
        steps -= chunk;
    }
}

pub(crate) fn count_forward_line_moves(buffer: &crate::buffer::Buffer) -> usize {
    let mut count = 0;
    let mut pos = buffer.point();
    while pos < buffer.point_max() {
        if buffer.char_at(pos) == Some('\n') {
            count += 1;
        }
        pos += 1;
    }
    count
}

pub(crate) fn count_backward_line_moves(buffer: &crate::buffer::Buffer) -> usize {
    let mut line_start = buffer.point();
    while line_start > buffer.point_min() {
        if buffer.char_at(line_start - 1) == Some('\n') {
            break;
        }
        line_start -= 1;
    }

    let mut count = 0;
    let mut pos = buffer.point_min();
    while pos < line_start {
        if buffer.char_at(pos) == Some('\n') {
            count += 1;
        }
        pos += 1;
    }
    count
}

/// keymap.c get_keymap (OBJECT, error_if_not_keymap = true, autoload =
/// false): a `(keymap ...)' list is itself, a symbol whose function
/// indirection is one yields that keymap, and anything else is
/// `(wrong-type-argument keymapp OBJECT)'.
fn keymap_list_for_copy(
    interp: &mut Interpreter,
    object: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let is_keymap_list =
        |value: &Value| matches!(value.car(), Ok(Value::Symbol(head)) if head == "keymap");
    if is_keymap_list(object) {
        return Ok(object.clone());
    }
    if object.as_symbol().is_ok() {
        let function = call(
            interp,
            "indirect-function",
            std::slice::from_ref(object),
            env,
        )?;
        if is_keymap_list(&function) {
            return Ok(function);
        }
    }
    Err(LispError::WrongTypeArgument(
        "keymapp".into(),
        object.clone(),
    ))
}

/// keymap.c copy_keymap_1: a fresh `(keymap ...)' spine whose char-table
/// and vector elements are copied with their items sent through
/// `copy_keymap_item', whose nested `(keymap ...)' elements are copied,
/// and whose `(EVENT . DEFINITION)' cells are fresh with the definition
/// sent through `copy_keymap_item'; the tail from the parent keymap on
/// is shared.
pub(crate) fn copy_keymap_value(
    interp: &mut Interpreter,
    keymap: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    copy_keymap_1(interp, keymap, 0, env)
}

/// The copy of a keymap Emaxx owns through a runtime record (its public
/// `(keymap ...)' cons is a view of the record): a new record with the
/// same name, the parent shared, every binding's definition and the
/// char-table sent through `copy_keymap_item', and a fresh public view.
fn copy_runtime_keymap(
    interp: &mut Interpreter,
    id: u64,
    depth: usize,
    env: &mut Env,
) -> Result<Value, LispError> {
    let (name, parent, bindings, char_table) = {
        let record = interp
            .find_record(id)
            .ok_or_else(|| LispError::WrongTypeArgument("keymapp".into(), Value::Record(id)))?;
        (
            record.slots.first().cloned().unwrap_or(Value::Nil),
            record
                .slots
                .get(KEYMAP_PARENT_SLOT)
                .cloned()
                .unwrap_or(Value::Nil),
            record
                .slots
                .get(KEYMAP_BINDINGS_SLOT)
                .cloned()
                .unwrap_or(Value::Nil),
            keymap_char_table(record),
        )
    };
    let name = string_like(&name).map(|string| string.text);
    let copy = make_runtime_keymap(interp, name.as_deref());
    let copy_id = keymap_record_id(interp, &copy)
        .ok_or_else(|| LispError::WrongTypeArgument("keymapp".into(), copy.clone()))?;
    let mut copied_bindings = Vec::new();
    for entry in bindings.to_vec()? {
        let mut items = entry.to_vec()?;
        if items.len() >= 2 {
            items[1] = copy_keymap_item(interp, &items[1], depth + 1, env)?;
        }
        copied_bindings.push(Value::list(items));
    }
    let char_table = match char_table {
        Some(table) => Some(copy_keymap_char_table(interp, &table, depth + 1, env)?),
        None => None,
    };
    if let Some(record) = interp.find_record_mut(copy_id) {
        if record.slots.len() <= KEYMAP_CHAR_TABLE_SLOT {
            record.slots.resize(KEYMAP_CHAR_TABLE_SLOT + 1, Value::Nil);
        }
        record.slots[KEYMAP_PARENT_SLOT] = parent;
        record.slots[KEYMAP_BINDINGS_SLOT] = Value::list(copied_bindings);
        if let Some(table) = char_table {
            record.slots[KEYMAP_CHAR_TABLE_SLOT] = table;
        }
    }
    refresh_runtime_keymap_public_view(interp, copy_id)?;
    Ok(runtime_keymap_public_view(interp, &copy).unwrap_or(copy))
}

fn copy_keymap_1(
    interp: &mut Interpreter,
    keymap: &Value,
    depth: usize,
    env: &mut Env,
) -> Result<Value, LispError> {
    if depth > 100 {
        return Err(LispError::Signal(
            "Possible infinite recursion when copying keymap".into(),
        ));
    }
    if let Some(id) = keymap_record_id(interp, keymap) {
        return copy_runtime_keymap(interp, id, depth, env);
    }
    let keymap = keymap_list_for_copy(interp, keymap, env)?;
    if let Some(id) = keymap_record_id(interp, &keymap) {
        return copy_runtime_keymap(interp, id, depth, env);
    }
    let mut items = Vec::new();
    let mut tail = keymap.cdr()?;
    while let Some((elt, rest)) = tail.cons_values() {
        if matches!(&elt, Value::Symbol(head) if head == "keymap") {
            break;
        }
        let copied = if matches!(elt, Value::CharTable(_)) {
            copy_keymap_char_table(interp, &elt, depth + 1, env)?
        } else if is_vector_value(&elt) {
            let mut copied = Vec::new();
            for item in vector_items(&elt)? {
                copied.push(copy_keymap_item(interp, &item, depth + 1, env)?);
            }
            Value::vector(copied)
        } else if let Some((car, cdr)) = elt.cons_values() {
            if matches!(&car, Value::Symbol(head) if head == "keymap") {
                copy_keymap_1(interp, &elt, depth + 1, env)?
            } else {
                Value::cons(car, copy_keymap_item(interp, &cdr, depth + 1, env)?)
            }
        } else {
            elt
        };
        items.push(copied);
        tail = rest;
    }
    let mut result = tail;
    for item in items.into_iter().rev() {
        result = Value::cons(item, result);
    }
    Ok(Value::cons(Value::symbol("keymap"), result))
}

/// `Fcopy_sequence' of the char-table, then every entry's definition sent
/// through `copy_keymap_item' (keymap.c copy_keymap_set_char_table through
/// map_char_table).  The values are rewritten in place: `equal' compares
/// two tables entry by entry, so appending would leave the copy unequal to
/// the original even with the same mappings.
fn copy_keymap_char_table(
    interp: &mut Interpreter,
    table: &Value,
    depth: usize,
    env: &mut Env,
) -> Result<Value, LispError> {
    let copy = call(interp, "copy-sequence", std::slice::from_ref(table), env)?;
    let Value::CharTable(copy_id) = copy else {
        return Ok(copy);
    };
    let entries = interp.char_table_entries(copy_id).unwrap_or_default();
    let mut replaced = Vec::with_capacity(entries.len());
    let mut changed = false;
    for entry in entries {
        let item = syntax::char_table_public_value(interp, copy_id, entry.value.clone());
        let copied = copy_keymap_item(interp, &item, depth, env)?;
        if crate::lisp::primitives::values_eq_in_env(interp, &copied, &item, env) {
            replaced.push(entry);
        } else {
            changed = true;
            replaced.push(crate::lisp::eval::CharTableEntry {
                start: entry.start,
                end: entry.end,
                value: copied,
            });
        }
    }
    if changed {
        interp.char_table_replace_entries(copy_id, replaced)?;
    }
    Ok(copy)
}

/// keymap.c copy_keymap_item: a `(menu-item NAME BINDING . REST)' gets
/// fresh cells for the marker, the name and the binding (a keymap binding
/// copied) and shares REST; an old-style `(STRING [HELP] . DEFINITION)'
/// gets fresh cells for the strings and a copied keymap definition; a
/// `(keymap ...)' is copied; anything else is shared.
fn copy_keymap_item(
    interp: &mut Interpreter,
    elt: &Value,
    depth: usize,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some((car, cdr)) = elt.cons_values() else {
        return Ok(elt.clone());
    };
    let is_keymap_list =
        |value: &Value| matches!(value.car(), Ok(Value::Symbol(head)) if head == "keymap");
    let rebuild = |cells: Vec<Value>, rest: Value| {
        let mut result = rest;
        for cell in cells.into_iter().rev() {
            result = Value::cons(cell, result);
        }
        result
    };
    if matches!(&car, Value::Symbol(head) if head == "menu-item") {
        let mut cells = vec![car];
        let mut rest = cdr;
        if let Some((name, after_name)) = rest.cons_values() {
            cells.push(name);
            rest = after_name;
            if let Some((binding, after_binding)) = rest.cons_values() {
                let binding = if is_keymap_list(&binding) {
                    copy_keymap_1(interp, &binding, depth, env)?
                } else {
                    binding
                };
                cells.push(binding);
                rest = after_binding;
            }
        }
        return Ok(rebuild(cells, rest));
    }
    if string_like(&car).is_some() {
        let mut cells = vec![car];
        let mut rest = cdr;
        if let Some((help, after_help)) = rest.cons_values()
            && string_like(&help).is_some()
        {
            cells.push(help);
            rest = after_help;
        }
        let rest = if is_keymap_list(&rest) {
            copy_keymap_1(interp, &rest, depth, env)?
        } else {
            rest
        };
        return Ok(rebuild(cells, rest));
    }
    if matches!(&car, Value::Symbol(head) if head == "keymap") {
        return copy_keymap_1(interp, elt, depth, env);
    }
    Ok(elt.clone())
}
