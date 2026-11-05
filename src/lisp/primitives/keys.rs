use super::*;
use crate::lisp::types::Kind;

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

/// keyboard.c:parse_solitary_modifier: the modifier SYMBOL's name
/// (SYMBOL_NAME, whatever obarray it is in) spells, 0 for none or for
/// what is no symbol.
pub(crate) fn solitary_modifier_of(symbol: &Value) -> i64 {
    match symbol.kind() {
        Kind::Nil | Kind::T => 0,
        Kind::Symbol(symbol) => solitary_event_modifier(&symbol.name_text()),
        _ => 0,
    }
}

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

/// keyboard.c:apply_modifiers_uncached's Fintern of the prefixed NAME,
/// which it builds as a multibyte string: the symbol of that name, or a
/// new one named by that string.
pub(crate) fn intern_modified_event_symbol(name: &str) -> Value {
    Value::Symbol(crate::lisp::types::SymbolName::intern_initial_with(
        name,
        || {
            crate::lisp::types::string_data::encode_text(name, true, &[])
                .ok()
                .and_then(|bytes| super::strings::make_specified_string(bytes, true).ok())
                .unwrap_or_else(|| Value::string(name))
        },
    ))
}

pub(crate) fn modified_event_symbol_name(modifiers: i64, base: &str) -> String {
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

/// keyboard.c:Fevent_convert_list: EVENT-DESC walked as FOR_EACH_TAIL_SAFE
/// walks it, each symbol with a cons after it a modifier if it names one;
/// the base event (nil when there is none) takes them through
/// apply_modifiers, a one-character symbol first becoming its name's first
/// byte.
pub(crate) fn event_convert_list_value(
    interp: &mut Interpreter,
    event_description: &Value,
) -> Result<Value, LispError> {
    let mut base = Value::Nil;
    let mut found_base = false;
    let mut modifiers = 0;
    let mut tail = *event_description;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        let element = cell.car.get();
        let rest = cell.cdr.get();
        let modifier = if rest.is_cons() {
            solitary_modifier_of(&element)
        } else {
            0
        };
        if modifier != 0 {
            modifiers |= modifier;
        } else if found_base {
            return Err(LispError::Signal("Two bases given in one event".into()));
        } else {
            base = element;
            found_base = true;
        }
        tail = rest;
        if walk.cycled_no_quit(tail) {
            break;
        }
    }

    // Let the symbol A refer to the character A.
    let name = match base.kind() {
        Kind::Nil => Some(crate::lisp::types::symbols::builtin_boolean(false).lisp_name()),
        Kind::T => Some(crate::lisp::types::symbols::builtin_boolean(true).lisp_name()),
        Kind::Symbol(symbol) => Some(symbol.lisp_name()),
        _ => None,
    };
    if let Some(name) = name
        && let Kind::StringObject(state) = name.kind()
    {
        let state = state.borrow();
        if state.len() == 1 {
            base = Value::Integer(i64::from(state.bytes()[0]));
        }
    }
    match base.kind() {
        Kind::Integer(mut code) => {
            // Turn (shift a) into A.
            if modifiers & KEY_DESCRIPTION_SHIFT_BIT != 0
                && (b'a' as i64..=b'z' as i64).contains(&code)
            {
                code -= i64::from(b'a' - b'A');
                modifiers &= !KEY_DESCRIPTION_SHIFT_BIT;
            }
            // Turn (control a) into C-a, through make_ctrl_char, which may
            // keep the control bit (C-9) or add a shift bit (control of a
            // shifted letter).
            if modifiers & KEY_DESCRIPTION_CTRL_BIT != 0 {
                code = make_ctrl_event_code(code);
                modifiers &= !KEY_DESCRIPTION_CTRL_BIT;
            }
            Ok(Value::Integer(code | modifiers))
        }
        Kind::Nil | Kind::T | Kind::Symbol(_) => {
            crate::lisp::primitives::keymap::apply_modifiers(interp, modifiers, base)
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

pub(crate) fn event_symbol_name_modifiers(symbol: &str) -> (i64, &str) {
    let symbol = crate::lisp::types::visible_symbol_name(symbol);
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

    (modifiers, base)
}

pub(crate) fn parse_event_symbol_modifiers(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    use crate::lisp::primitives::keymap::{symbol_get, symbol_put};
    // parse_modifiers reads SYMBOL_NAME, whatever obarray SYMBOL is in.
    value.as_symbol()?;
    let name = match value.kind() {
        Kind::Symbol(symbol) => symbol.name_text(),
        _ => value.as_symbol()?.to_owned(),
    };
    let symbol = name.as_str();
    let element_mask_key = cached_symbol!("event-symbol-element-mask");
    let elements_key = cached_symbol!("event-symbol-elements");
    if symbol_get(interp, *value, element_mask_key)
        .cons_values()
        .is_some()
    {
        return Ok(symbol_get(interp, *value, elements_key));
    }

    let (modifiers, base) = event_symbol_name_modifiers(symbol);

    let base = crate::lisp::types::interned_symbol_value(base.to_string());
    let element_mask = Value::list([base, Value::Integer(modifiers)]);
    let elements = Value::list(std::iter::once(base).chain(event_modifier_elements(modifiers)));
    symbol_put(interp, *value, element_mask_key, element_mask);
    symbol_put(interp, *value, elements_key, elements);
    Ok(elements)
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

pub(crate) fn key_sequence_binding_parts(value: &Value) -> Result<Vec<String>, LispError> {
    if let Ok(events) = vector_items(value)
        && let [event] = events.as_slice()
    {
        match event.kind() {
            Kind::Symbol(symbol) => return Ok(vec![symbol.to_string()]),
            // GNU renders the [t] default binding as "<t>", which also keeps
            // it distinct from a binding on the letter t.
            Kind::T => return Ok(vec!["<t>".into()]),
            _ => {}
        }
    }
    let mut parts = Vec::new();
    append_key_description_parts(value, &mut parts)?;
    Ok(parts)
}

/// keyboard.c's lucid_event_type_list_p: a proper list of fixnums and
/// symbols whose head is not one of the posn-bearing pseudo-event kinds.
pub(crate) fn lucid_event_type_list_p(event: &Value) -> bool {
    if !matches!(event.kind(), Kind::Cons(_)) {
        return false;
    }
    if matches!(
        event.car().map(|v| v.kind()),
        Ok(Kind::Symbol(head)) if head.is_named("help-echo")
            || head.is_named("vertical-line")
            || head.is_named("mode-line")
            || head.is_named("tab-line")
            || head.is_named("header-line")
    ) {
        return false;
    }
    let Ok(items) = event.to_vec() else {
        return false;
    };
    items
        .iter()
        .all(|item| matches!(item.kind(), Kind::Integer(_) | Kind::Symbol(_)))
}

pub(crate) fn append_key_description_parts(
    sequence: &Value,
    output: &mut Vec<String>,
) -> Result<(), LispError> {
    let events = key_description_events(sequence)?;
    let mut add_meta = false;
    for event in events {
        if add_meta {
            match event.kind() {
                Kind::Integer(code) if code == KEY_DESCRIPTION_META_PREFIX => {
                    output.push(describe_key_code(KEY_DESCRIPTION_META_PREFIX));
                    add_meta = true;
                }
                Kind::Integer(code)
                    if code != KEY_DESCRIPTION_META_PREFIX
                        && code & KEY_DESCRIPTION_META_BIT == 0 =>
                {
                    output.push(describe_key_code(code | KEY_DESCRIPTION_META_BIT));
                    add_meta = false;
                }
                other => {
                    output.push(describe_key_code(KEY_DESCRIPTION_META_PREFIX));
                    output.push(single_key_description_text(&other.value(), false)?);
                    add_meta = false;
                }
            }
            continue;
        }

        if matches!(event.kind(), Kind::Integer(code) if code == KEY_DESCRIPTION_META_PREFIX) {
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

    match sequence.kind() {
        Kind::Nil => Ok(Vec::new()),
        value if is_vector_value(&value.value()) => Ok(vector_items(sequence)?
            .into_iter()
            .map(normalize_key_description_event)
            .collect()),
        Kind::Integer(_) | Kind::Symbol(_) => Ok(vec![normalize_key_description_event(*sequence)]),
        _ => Err(LispError::WrongTypeArgument("arrayp".into(), *sequence)),
    }
}

pub(crate) fn normalize_key_description_event(event: Value) -> Value {
    let Some((start, end)) = event.cons_values() else {
        return event;
    };
    match (start.kind(), end.kind()) {
        (Kind::Integer(start), Kind::Integer(end)) if start == end => Value::Integer(start),
        _ => event,
    }
}

pub(crate) fn sequence_values(sequence: &Value) -> Result<Vec<Value>, LispError> {
    if let Some(string) = sequence_string_like(sequence) {
        Ok(string_sequence_values(&string))
    } else if matches!(sequence.kind(), Kind::Nil | Kind::Cons(_)) {
        sequence.to_vec()
    } else if is_bool_vector_value(sequence) {
        bool_vector_values(sequence)
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

pub(crate) fn sequence_string_like(value: &Value) -> Option<StringLike> {
    string_like(value)
}

pub(crate) fn single_key_description_text(
    key: &Value,
    no_angles: bool,
) -> Result<String, LispError> {
    match key.kind() {
        Kind::Nil => Ok(describe_symbolic_key("nil", no_angles)),
        Kind::Integer(code) => Ok(describe_key_code(code)),
        Kind::Symbol(symbol) => Ok(describe_symbolic_key(symbol.as_str(), no_angles)),
        Kind::T => Ok(describe_symbolic_key("t", no_angles)),
        Kind::StringObject(state) => Ok(state.borrow().text()),
        Kind::Cons(_) => list_event_key_description_text(key, no_angles),
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

    match base.kind() {
        Kind::Integer(code) => Ok(describe_key_code(code | bits)),
        Kind::Symbol(symbol) => {
            if let Some(ch) = event_name_character(symbol.as_str()) {
                Ok(describe_key_code(ch as i64 | bits))
            } else {
                Ok(describe_symbolic_key(
                    &symbolic_kbd_event(bits, symbol.as_str()),
                    no_angles,
                ))
            }
        }
        Kind::StringObject(state) => {
            let text = state.borrow().text();
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

/// keymap.c push_key_description: append the description of character
/// event CH to P as internal multibyte bytes.
pub(crate) fn push_key_description(ch: i64, p: &mut Vec<u8>) {
    // Clear all the meaningless bits above the meta bit.
    let mut c = ch & (KEY_DESCRIPTION_META_BIT | !-KEY_DESCRIPTION_META_BIT);
    let c2 = c & !KEY_DESCRIPTION_MODIFIER_MASK;
    if !(0..=i64::from(super::coding::MAX_CHAR)).contains(&c2) {
        p.extend_from_slice(format!("[{c}]").as_bytes());
        return;
    }
    let tab_as_ci = c2 == '\t' as i64 && c & KEY_DESCRIPTION_META_BIT != 0;
    if c & KEY_DESCRIPTION_ALT_BIT != 0 {
        p.extend_from_slice(b"A-");
        c -= KEY_DESCRIPTION_ALT_BIT;
    }
    if c & KEY_DESCRIPTION_CTRL_BIT != 0
        || (c2 < ' ' as i64 && c2 != 27 && c2 != '\t' as i64 && c2 != '\r' as i64)
        || tab_as_ci
    {
        p.extend_from_slice(b"C-");
        c &= !KEY_DESCRIPTION_CTRL_BIT;
    }
    if c & KEY_DESCRIPTION_HYPER_BIT != 0 {
        p.extend_from_slice(b"H-");
        c -= KEY_DESCRIPTION_HYPER_BIT;
    }
    if c & KEY_DESCRIPTION_META_BIT != 0 {
        p.extend_from_slice(b"M-");
        c -= KEY_DESCRIPTION_META_BIT;
    }
    if c & KEY_DESCRIPTION_SHIFT_BIT != 0 {
        p.extend_from_slice(b"S-");
        c -= KEY_DESCRIPTION_SHIFT_BIT;
    }
    if c & KEY_DESCRIPTION_SUPER_BIT != 0 {
        p.extend_from_slice(b"s-");
        c -= KEY_DESCRIPTION_SUPER_BIT;
    }
    if c < 0o40 {
        if c == 0o33 {
            p.extend_from_slice(b"ESC");
        } else if tab_as_ci {
            p.push(b'i');
        } else if c == '\t' as i64 {
            p.extend_from_slice(b"TAB");
        } else if c == '\r' as i64 {
            p.extend_from_slice(b"RET");
        } else if c > 0 && c <= 26 {
            // `C-' already added above.
            p.push(c as u8 + 0o140);
        } else {
            p.push(c as u8 + 0o100);
        }
    } else if c == 0o177 {
        p.extend_from_slice(b"DEL");
    } else if c == ' ' as i64 {
        p.extend_from_slice(b"SPC");
    } else if c < 128 {
        p.push(c as u8);
    } else {
        // Now we are sure that C is a valid character code.
        let (bytes, length) = crate::lisp::types::string_data::encode_character(c as u32)
            .expect("a character code below MAX_CHAR encodes");
        p.extend_from_slice(&bytes[..length]);
    }
}

/// The description of character event CODE in Emaxx's text view, for the
/// keymap listings that build Rust text.
pub(crate) fn describe_key_code(code: i64) -> String {
    let mut bytes = Vec::new();
    push_key_description(code, &mut bytes);
    crate::lisp::types::string_data::decode_bytes(&bytes, true)
        .map(|(text, _)| text)
        .expect("push_key_description writes valid multibyte text")
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
    let keymap = get_keymap(interp, *keymap, true, false, env)?;
    let mut items = Vec::new();
    let mut tail = keymap.cdr()?;
    while let Some((elt, rest)) = tail.cons_values() {
        if matches!(elt.kind(), Kind::Symbol(head) if head.is_named("keymap")) {
            break;
        }
        let copied = if matches!(elt.kind(), Kind::CharTable(_)) {
            copy_keymap_char_table(interp, &elt, depth + 1, env)?
        } else if is_vector_value(&elt) {
            let mut copied = Vec::new();
            for item in vector_items(&elt)? {
                copied.push(copy_keymap_item(interp, &item, depth + 1, env)?);
            }
            Value::vector(copied)
        } else if let Some((car, cdr)) = elt.cons_values() {
            if matches!(car.kind(), Kind::Symbol(head) if head.is_named("keymap")) {
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
/// map_char_table). Rewrite each copied range in its existing radix tree.
fn copy_keymap_char_table(
    interp: &mut Interpreter,
    table: &Value,
    depth: usize,
    env: &mut Env,
) -> Result<Value, LispError> {
    let copy = call(interp, "copy-sequence", std::slice::from_ref(table), env)?;
    let Kind::CharTable(copy_id) = copy.kind() else {
        return Ok(copy);
    };
    interp.with_lisp_stack_roots(&copy, |interp| {
        for entry in copy_id.effective_ranges() {
            let copied = copy_keymap_item(interp, &entry.value, depth, env)?;
            copy_id.set_range(entry.start, entry.end, copied);
        }
        Ok(copy)
    })
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
        return Ok(*elt);
    };
    let is_keymap_list = |value: &Value| matches!(value.car().map(|v| v.kind()), Ok(Kind::Symbol(head)) if head.is_named("keymap"));
    let rebuild = |cells: Vec<Value>, rest: Value| {
        let mut result = rest;
        for cell in cells.into_iter().rev() {
            result = Value::cons(cell, result);
        }
        result
    };
    if matches!(car.kind(), Kind::Symbol(head) if head.is_named("menu-item")) {
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
    if matches!(car.kind(), Kind::Symbol(head) if head.is_named("keymap")) {
        return copy_keymap_1(interp, elt, depth, env);
    }
    Ok(*elt)
}
