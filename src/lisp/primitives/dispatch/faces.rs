use super::*;
use crate::lisp::eval::LFACE_VECTOR_SIZE;
use crate::lisp::reader::Reader;
use crate::lisp::types::Kind;

/// xfaces.c:resolve_face_name (FACE, true): a string names the symbol it
/// interns; the `face-alias' chain is followed to its last symbol, the hare
/// two links to the tortoise's one, and a chain that comes round again
/// signals `circular-list' with the face.
fn resolve_face_name(interp: &mut Interpreter, value: &Value) -> Result<Value, LispError> {
    let face = match value.kind() {
        Kind::Symbol(symbol) => Value::Symbol(symbol),
        Kind::StringObject(_) => Value::Symbol(crate::lisp::types::SymbolName::intern_str(
            &string_text(value)?,
        )),
        _ => return Err(wrong_type_argument("symbolp", *value)),
    };
    let property = cached_symbol!("face-alias").with(|symbol| Value::Symbol(*symbol));
    // Fget (SYMBOL, Qface_alias), and whether it is a symbol other than nil.
    let alias = |interp: &mut Interpreter, symbol: Value| -> Result<Option<Value>, LispError> {
        let alias = super::misc::direct_get(interp, &[symbol, property], &mut Env::new())?;
        Ok(matches!(alias.kind(), Kind::Symbol(_) | Kind::T).then_some(alias))
    };
    let (mut hare, mut tortoise) = (face, face);
    let mut face_name;
    loop {
        face_name = hare;
        let Some(next) = alias(interp, hare)? else {
            break;
        };
        hare = next;
        face_name = hare;
        let Some(next) = alias(interp, hare)? else {
            break;
        };
        hare = next;
        tortoise = super::misc::direct_get(interp, &[tortoise, property], &mut Env::new())?;
        if hare.eq_value(tortoise) {
            return Err(LispError::SignalValue(Value::list([
                cached_symbol!("circular-list").with(|symbol| Value::Symbol(*symbol)),
                face,
            ])));
        }
    }
    Ok(face_name)
}

/// CHECK_SYMBOL (FACE): the symbol, a symbol with position's bare one.
fn face_symbol(value: &Value) -> Result<Value, LispError> {
    match value.kind() {
        Kind::Symbol(_) | Kind::Nil | Kind::T => Ok(*value),
        Kind::SymbolWithPos(object) => Ok(object.symbol()),
        _ => Err(wrong_type_argument("symbolp", *value)),
    }
}

fn face_vector(items: impl IntoIterator<Item = Value>) -> Value {
    Value::vector(items)
}

fn unspecified_face_vector() -> Value {
    face_vector(std::iter::repeat_n(
        Value::symbol("unspecified"),
        LFACE_VECTOR_SIZE,
    ))
}

fn face_target_is_global(frame: Option<&Value>) -> bool {
    matches!(frame.map(|v| v.kind()), Some(Kind::T))
}

fn face_attribute_error(attribute: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("Invalid face attribute name"),
        *attribute,
    ]))
}

use crate::lisp::eval::invalid_face_error;

/// UNSPECIFIEDP, IGNORE_DEFFACE_P or RESET_P.
fn special_face_value(value: &Value) -> bool {
    matches!(
        value.kind(),
        Kind::Symbol(symbol)
            if matches!(
                symbol.as_str(),
                "unspecified" | ":ignore-defface" | "reset"
            )
    )
}

/// UNSPECIFIEDP or IGNORE_DEFFACE_P.
fn unspecified_face_value(value: &Value) -> bool {
    matches!(
        value.kind(),
        Kind::Symbol(symbol) if matches!(symbol.as_str(), "unspecified" | ":ignore-defface")
    )
}

/// eval.c:signal_error (S, ARG): `error' with S and ARG's elements when
/// ARG is a proper list, else with S and ARG.
fn face_signal_error(message: &str, arg: Value) -> LispError {
    let data = if arg.to_vec().is_ok() {
        Value::cons(Value::string(message), arg)
    } else {
        Value::list([Value::string(message), arg])
    };
    LispError::SignalValue(Value::cons(Value::symbol("error"), data))
}

/// xfaces.c:merge_face_heights: FROM, an absolute height, a scale of TO
/// or a function of it, merged with TO; INVALID where FROM is none of
/// them, or a function that makes TO's absolute height something else.
fn merge_face_heights(
    interp: &mut Interpreter,
    from: Value,
    to: Value,
    invalid: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    Ok(match from.kind() {
        Kind::Integer(_) => from,
        Kind::Float(scale) => match to.kind() {
            Kind::Integer(height) => Value::Integer((scale.get() * height as f64) as i64),
            Kind::Float(height) => Value::float(scale.get() * height.get()),
            _ if matches!(to.kind(), Kind::Symbol(symbol) if symbol == "unspecified") => from,
            _ => invalid,
        },
        _ if function_value_p(interp, &from, env) => {
            let result = interp.safe_funcall(from, &[to], env)?;
            if matches!(to.kind(), Kind::Integer(_)) && !matches!(result.kind(), Kind::Integer(_)) {
                invalid
            } else {
                result
            }
        }
        _ => invalid,
    })
}

/// FONTP: a font-spec, font-entity or font-object.
fn font_value_p(interp: &Interpreter, value: &Value) -> bool {
    matches!(value.kind(), Kind::Record(record)
        if interp.find_record(record).is_some_and(|state| state.kind == crate::lisp::eval::RecordKind::Font))
}

/// Finternal_set_lisp_face_attribute's checks of VALUE for ATTRIBUTE of
/// FACE, on all frames (GLOBAL) or one tty frame: the lface slot and the
/// value it takes, or None where nothing is stored (a font or fontset on
/// a tty frame).
fn checked_face_attribute(
    interp: &mut Interpreter,
    face: Value,
    attribute: &str,
    value: Value,
    global: bool,
    env: &mut Env,
) -> Result<Option<(usize, Value)>, LispError> {
    let index = |name: &str| face_attribute_index(name).expect("an attribute of the lface vector");
    let special = special_face_value(&value);
    let is = |name: &str| matches!(value.kind(), Kind::Symbol(symbol) if symbol == name);
    let check_string = |value: Value| -> Result<String, LispError> {
        if value.is_string() {
            string_text(&value)
        } else {
            Err(wrong_type_argument("stringp", value))
        }
    };
    let check_symbol = |value: Value| -> Result<(), LispError> {
        if matches!(value.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) {
            Ok(())
        } else {
            Err(wrong_type_argument("symbolp", value))
        }
    };
    let mut value = value;
    match attribute {
        ":family" | ":foundry" => {
            if !special && check_string(value)?.is_empty() {
                return Err(face_signal_error(
                    if attribute == ":family" {
                        "Invalid face family"
                    } else {
                        "Invalid face foundry"
                    },
                    value,
                ));
            }
        }
        ":height" => {
            if !special {
                if face.eq_value(Value::symbol("default")) {
                    // The default face must have an absolute size.
                    if !matches!(value.kind(), Kind::Integer(height) if height > 0) {
                        return Err(face_signal_error(
                            "Default face height not absolute and positive",
                            value,
                        ));
                    }
                } else {
                    // A test merge with a random height.
                    let test =
                        merge_face_heights(interp, value, Value::Integer(10), Value::Nil, env)?;
                    if !matches!(test.kind(), Kind::Integer(height) if height > 0) {
                        return Err(face_signal_error(
                            "Face height does not produce a positive integer",
                            value,
                        ));
                    }
                }
            }
        }
        ":weight" | ":slant" | ":width" => {
            if !special {
                check_symbol(value)?;
                let (style, message) = match attribute {
                    ":weight" => (super::fonts::FONT_WEIGHT_INDEX, "Invalid face weight"),
                    ":slant" => (super::fonts::FONT_SLANT_INDEX, "Invalid face slant"),
                    _ => (super::fonts::FONT_WIDTH_INDEX, "Invalid face width"),
                };
                let name = value.symbol_str().unwrap_or(match value.kind() {
                    Kind::T => "t",
                    _ => "nil",
                });
                if !super::fonts::valid_style_name(style, name) {
                    return Err(face_signal_error(message, value));
                }
            }
        }
        ":underline" => {
            let valid = special
                || matches!(value.kind(), Kind::Nil | Kind::T)
                || (value.is_string() && !string_text(&value)?.is_empty())
                || (matches!(value.kind(), Kind::Cons(_)) && underline_plist_valid(value)?);
            if !valid {
                return Err(face_signal_error("Invalid face underline", value));
            }
        }
        ":overline" | ":strike-through" => {
            if !special
                && ((matches!(value.kind(), Kind::Symbol(_)))
                    || (value.is_string() && string_text(&value)?.is_empty()))
            {
                return Err(face_signal_error(
                    if attribute == ":overline" {
                        "Invalid face overline"
                    } else {
                        "Invalid face strike-through"
                    },
                    value,
                ));
            }
        }
        ":box" => {
            // t is a simple box of width 1 in the face's foreground color.
            if matches!(value.kind(), Kind::T) {
                value = Value::Integer(1);
            }
            let valid = special_face_value(&value)
                || match value.kind() {
                    Kind::Nil => true,
                    Kind::Integer(width) => width != 0,
                    Kind::Cons(cell) => {
                        matches!(
                            (cell.car.get().kind(), cell.cdr.get().kind()),
                            (Kind::Integer(_), Kind::Integer(_))
                        ) || box_plist_valid(value)?
                    }
                    _ if value.is_string() => !string_text(&value)?.is_empty(),
                    _ => false,
                };
            if !valid {
                return Err(face_signal_error("Invalid face box", value));
            }
        }
        ":inverse-video" | ":reverse-video" | ":extend" => {
            if !special {
                check_symbol(value)?;
                if !matches!(value.kind(), Kind::Nil | Kind::T) {
                    return Err(face_signal_error(
                        if attribute == ":extend" {
                            "Invalid extend face attribute value"
                        } else {
                            "Invalid inverse-video face attribute value"
                        },
                        value,
                    ));
                }
            }
        }
        ":foreground" | ":distant-foreground" | ":background" => {
            // HANDLE_INVALID_NIL_VALUE: compatibility with 20.x.
            if value.is_nil() {
                let message = format!(
                    "Warning: setting attribute `{attribute}' of face `{}': nil value is invalid, use `unspecified' instead.",
                    face.symbol_str().unwrap_or("nil")
                );
                log_message_text(interp, &message, env);
                value = Value::symbol("unspecified");
            }
            if !special_face_value(&value) && check_string(value)?.is_empty() {
                return Err(face_signal_error(
                    match attribute {
                        ":foreground" => "Empty foreground color value",
                        ":background" => "Empty background color value",
                        _ => "Empty distant-foreground color value",
                    },
                    value,
                ));
            }
        }
        ":stipple" => {
            if !special && !value.is_nil() && !bitmap_spec_p(&value) {
                return Err(face_signal_error("Invalid stipple attribute", value));
            }
        }
        ":font" => {
            // A tty frame realizes no font: only an unspecified value
            // set for all frames is stored.
            if !global {
                return Ok(None);
            }
            if !special {
                if !value.is_string() && !font_value_p(interp, &value) {
                    return Err(face_signal_error("Invalid font or font-spec", value));
                }
                return Ok(None);
            }
        }
        ":fontset" => {
            if !global {
                return Ok(None);
            }
            if !is("reset") {
                let fontset = super::call(interp, "query-fontset", &[value, Value::Nil], env)?;
                if fontset.is_nil() {
                    return Err(face_signal_error("Invalid fontset name", value));
                }
                value = fontset;
            }
        }
        ":inherit" => {
            let mut tail = value;
            if !matches!(value.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) {
                while let Some((head, rest)) = tail.cons_values() {
                    if !matches!(head.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) {
                        break;
                    }
                    tail = rest;
                }
            } else {
                tail = Value::Nil;
            }
            if !tail.is_nil() {
                return Err(face_signal_error("Invalid face inheritance", value));
            }
        }
        ":bold" => {
            if !is("reset") {
                value = Value::symbol(if value.is_nil() { "normal" } else { "bold" });
            }
        }
        ":italic" => {
            if !is("reset") {
                value = Value::symbol(if value.is_nil() { "normal" } else { "italic" });
            }
        }
        _ => {
            return Err(face_signal_error(
                "Invalid face attribute name",
                Value::symbol(attribute),
            ));
        }
    }
    Ok(Some((index(attribute), value)))
}

/// The plist walk Finternal_set_lisp_face_attribute makes of an
/// underline: each key and value until a key is nil, a key nil, a value
/// nil but for `:position', a `:color' neither `foreground-color' nor a
/// nonempty string, or a `:style' not one of the line styles invalid.
fn underline_plist_valid(mut list: Value) -> Result<bool, LispError> {
    let car = |value: Value| value.cons_values().map_or(Value::Nil, |(car, _)| car);
    let cdr = |value: Value| value.cons_values().map_or(Value::Nil, |(_, cdr)| cdr);
    while !car(list).is_nil() {
        let key = car(list);
        list = cdr(list);
        let val = car(list);
        list = cdr(list);
        let key_is = |name: &str| matches!(key.kind(), Kind::Symbol(symbol) if symbol == name);
        let val_is = |names: &[&str]| matches!(val.kind(), Kind::Symbol(symbol) if names.contains(&symbol.as_str()));
        if key.is_nil() || (val.is_nil() && !key_is(":position")) {
            return Ok(false);
        }
        if key_is(":color")
            && !(val_is(&["foreground-color"])
                || (val.is_string() && !string_text(&val)?.is_empty()))
        {
            return Ok(false);
        }
        if key_is(":style") && !val_is(&["line", "double-line", "wave", "dots", "dashes"]) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The plist walk Finternal_set_lisp_face_attribute makes of a box: each
/// `:line-width' a nonzero width or (WIDTH . HEIGHT) of them, `:color'
/// nil or a nonempty string, `:style' nil or a button style; anything
/// else, or a key without a value, ends the walk short of nil.
fn box_plist_valid(value: Value) -> Result<bool, LispError> {
    let nonzero = |value: Value| matches!(value.kind(), Kind::Integer(width) if width != 0);
    let mut tail = value;
    while let Some((key, rest)) = tail.cons_values() {
        tail = rest;
        let Some((val, rest)) = tail.cons_values() else {
            break;
        };
        let key_is = |name: &str| matches!(key.kind(), Kind::Symbol(symbol) if symbol == name);
        if key_is(":line-width") {
            let pair = val
                .cons_values()
                .is_some_and(|(width, height)| nonzero(width) && nonzero(height));
            if !pair && !nonzero(val) {
                break;
            }
        } else if key_is(":color") {
            if !val.is_nil() && (!val.is_string() || string_text(&val)?.is_empty()) {
                break;
            }
        } else if key_is(":style") {
            if !val.is_nil()
                && !matches!(val.kind(), Kind::Symbol(symbol)
                    if matches!(symbol.as_str(), "pressed-button" | "released-button" | "flat-button"))
            {
                break;
            }
        } else {
            break;
        }
        tail = rest;
    }
    Ok(tail.is_nil())
}

fn valid_named_value(value: &Value, choices: &[&str]) -> bool {
    special_face_value(value)
        || matches!(value.kind(), Kind::Symbol(symbol) if choices.contains(&symbol.as_str()))
}

fn valid_inherit_value(value: &Value) -> bool {
    matches!(value.kind(), Kind::Nil | Kind::Symbol(_))
        || value.to_vec().is_ok_and(|items| {
            items
                .iter()
                .all(|item| matches!(item.kind(), Kind::Symbol(_)))
        })
}

fn normalize_face_attribute_value(
    attribute: &str,
    value: &Value,
) -> Result<(usize, Value), LispError> {
    let index = face_attribute_index(attribute)
        .ok_or_else(|| face_attribute_error(&Value::symbol(attribute)))?;
    let normalized = match attribute {
        ":bold" => {
            if matches!(value.kind(), Kind::Symbol(symbol) if symbol == "reset") {
                *value
            } else if value.is_nil() {
                Value::symbol("normal")
            } else {
                Value::symbol("bold")
            }
        }
        ":italic" => {
            if matches!(value.kind(), Kind::Symbol(symbol) if symbol == "reset") {
                *value
            } else if value.is_nil() {
                Value::symbol("normal")
            } else {
                Value::symbol("italic")
            }
        }
        _ if special_face_value(value) => *value,
        ":family" | ":foundry" => {
            let text = string_text(value)?;
            if text.is_empty() {
                return Err(LispError::Signal("Invalid face family".into()));
            }
            *value
        }
        ":height" => match value.kind() {
            Kind::Integer(height) if height > 0 => *value,
            Kind::Float(scale) if scale.is_finite() && scale.get() > 0.0 => *value,
            Kind::Closure(_) | Kind::BuiltinFunc(_) | Kind::Symbol(_) => *value,
            _ => return Err(LispError::Signal("Invalid face height".into())),
        },
        ":weight" => {
            if !valid_named_value(
                value,
                &[
                    "ultra-light",
                    "extra-light",
                    "light",
                    "semi-light",
                    "normal",
                    "regular",
                    "medium",
                    "semi-bold",
                    "bold",
                    "extra-bold",
                    "ultra-bold",
                ],
            ) {
                return Err(LispError::Signal("Invalid face weight".into()));
            }
            *value
        }
        ":slant" => {
            if !valid_named_value(
                value,
                &[
                    "normal",
                    "italic",
                    "oblique",
                    "reverse-italic",
                    "reverse-oblique",
                ],
            ) {
                return Err(LispError::Signal("Invalid face slant".into()));
            }
            *value
        }
        ":width" => {
            if !valid_named_value(
                value,
                &[
                    "ultra-condensed",
                    "extra-condensed",
                    "condensed",
                    "semi-condensed",
                    "normal",
                    "semi-expanded",
                    "expanded",
                    "extra-expanded",
                    "ultra-expanded",
                ],
            ) {
                return Err(LispError::Signal("Invalid face width".into()));
            }
            *value
        }
        ":underline" => {
            if !(matches!(value.kind(), Kind::Nil | Kind::T | Kind::Cons(..)) || value.is_string())
            {
                return Err(LispError::Signal("Invalid face underline".into()));
            }
            *value
        }
        ":overline" | ":strike-through" => {
            if !(matches!(value.kind(), Kind::Nil | Kind::T) || value.is_string()) {
                return Err(LispError::Signal("Invalid face line attribute".into()));
            }
            *value
        }
        ":inverse-video" | ":reverse-video" | ":extend" => {
            if !matches!(value.kind(), Kind::Nil | Kind::T) {
                return Err(LispError::Signal("Invalid boolean face attribute".into()));
            }
            *value
        }
        ":foreground" | ":distant-foreground" | ":background" => {
            if value.is_nil() {
                Value::symbol("unspecified")
            } else {
                let text = string_text(value)?;
                if text.is_empty() {
                    return Err(LispError::Signal("Empty face color value".into()));
                }
                *value
            }
        }
        ":stipple" => *value,
        ":box" => {
            if !(matches!(
                value.kind(),
                Kind::Nil | Kind::T | Kind::Integer(_) | Kind::Cons(..)
            ) || value.is_string())
            {
                return Err(LispError::Signal("Invalid face box".into()));
            }
            if matches!(value.kind(), Kind::T) {
                Value::Integer(1)
            } else {
                *value
            }
        }
        ":font" | ":fontset" => *value,
        ":inherit" => {
            if !valid_inherit_value(value) {
                return Err(LispError::Signal("Invalid face inheritance".into()));
            }
            *value
        }
        _ => return Err(face_attribute_error(&Value::symbol(attribute))),
    };
    Ok((index, normalized))
}

pub(super) fn set_face_attribute(
    interp: &mut Interpreter,
    face: Value,
    attribute: &str,
    value: &Value,
    global: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    set_face_attribute_on(
        interp,
        face,
        attribute,
        value,
        (!global).then_some(interp.selected_frame_id),
        env,
    )
}

/// Finternal_set_lisp_face_attribute of FACE's definition on FRAME (the
/// one for new frames when FRAME is None, where an unspecified VALUE is
/// stored as `:ignore-defface' so frame defaults ignore the defface).
fn set_face_attribute_on(
    interp: &mut Interpreter,
    face: Value,
    attribute: &str,
    value: &Value,
    frame: Option<crate::lisp::types::FrameRef>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut value = *value;
    if frame.is_none() && matches!(value.kind(), Kind::Symbol(symbol) if symbol == "unspecified") {
        value = Value::symbol(":ignore-defface");
    }
    if let Some((index, value)) =
        checked_face_attribute(interp, face, attribute, value, frame.is_none(), env)?
    {
        interp.set_lisp_face_attribute_on(face, index, value, frame)?;
    }
    Ok(face)
}

fn face_vectors_equal(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    for index in 1..LFACE_VECTOR_SIZE {
        if !values_equal(
            interp,
            &vector_slot_value(left, index)?,
            &vector_slot_value(right, index)?,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn bitmap_spec_p(value: &Value) -> bool {
    if value.is_string() {
        return true;
    }
    let Ok(items) = value.to_vec() else {
        return false;
    };
    let kinds = items.iter().map(|v| v.kind()).collect::<Vec<_>>();
    let [Kind::Integer(width), Kind::Integer(height), data, ..] = kinds.as_slice() else {
        return false;
    };
    if *width <= 0 || *height <= 0 {
        return false;
    }
    let Some(data) = string_like(&data.value()) else {
        return false;
    };
    let bytes_per_row = (*width as usize).div_ceil(8);
    (*height as usize) <= data.text.len() / bytes_per_row
}

fn transform_font_alist(value: &Value, registry: bool) -> Result<Value, LispError> {
    let entries = value.to_vec()?;
    let mut result = Vec::with_capacity(entries.len());
    for entry in entries {
        let values = entry.to_vec()?;
        let mut transformed = Vec::with_capacity(values.len());
        for value in values {
            let text = string_text(&value)?;
            transformed.push(if registry {
                Value::String(text.to_lowercase().into())
            } else {
                Value::Symbol(text.into())
            });
        }
        result.push(Value::list(transformed));
    }
    Ok(Value::list(result))
}

fn parse_resource_boolean(value: &str, signal: bool) -> Result<Option<Value>, LispError> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" => Ok(Some(Value::T)),
        "off" | "false" => Ok(Some(Value::Nil)),
        "unspecified" => Ok(Some(Value::symbol("unspecified"))),
        _ if signal => Err(LispError::Signal(
            "Invalid face attribute value from X resource".into(),
        )),
        _ => Ok(None),
    }
}

fn resource_face_value(attribute: &str, text: &str) -> Result<Value, LispError> {
    if text.eq_ignore_ascii_case("unspecified") {
        return Ok(Value::symbol("unspecified"));
    }
    match attribute {
        ":height" => text
            .parse::<i64>()
            .ok()
            .filter(|value| *value > 0)
            .map(Value::Integer)
            .ok_or_else(|| LispError::Signal("Invalid face height from X resource".into())),
        ":bold" | ":italic" | ":inverse-video" | ":reverse-video" | ":extend" => {
            parse_resource_boolean(text, true)?
                .ok_or_else(|| LispError::Signal("Invalid face resource value".into()))
        }
        ":weight" | ":slant" | ":width" => Ok(Value::symbol(text)),
        ":underline" | ":overline" | ":strike-through" => {
            Ok(parse_resource_boolean(text, false)?.unwrap_or_else(|| Value::string(text)))
        }
        ":box" | ":inherit" => Reader::new(text)
            .read()?
            .ok_or_else(|| LispError::Signal("Invalid face resource value".into())),
        _ => Ok(Value::string(text)),
    }
}

fn load_color_file(filename: &str) -> Value {
    let Ok(contents) = fs::read_to_string(filename) else {
        return Value::Nil;
    };
    let mut colors = Vec::new();
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        let (Some(red), Some(green), Some(blue)) = (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let (Ok(red), Ok(green), Ok(blue)) = (
            red.parse::<i64>(),
            green.parse::<i64>(),
            blue.parse::<i64>(),
        ) else {
            continue;
        };
        let name = fields.collect::<Vec<_>>().join(" ");
        if name.is_empty() {
            continue;
        }
        colors.push(Value::cons(
            Value::string(&name),
            Value::Integer((red << 16) | (green << 8) | blue),
        ));
    }
    colors.reverse();
    Value::list(colors)
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "frame--face-hash-table" => {
                need_arg_range(name, args, 0, 1)?;
                let frame = frames::decode_live_frame(interp, args.first(), true)?;
                Ok(interp.frame_face_hash_table(frame))
            }
            "internal-face-x-get-resource" => {
                need_arg_range(name, args, 2, 3)?;
                string_text(&args[0])?;
                string_text(&args[1])?;
                frames::decode_live_frame(interp, args.get(2), true)?;
                Err(frames::window_system_unavailable())
            }
            "clear-face-cache" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "bitmap-spec-p" => {
                need_args(name, args, 1)?;
                Ok(if bitmap_spec_p(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "color-gray-p" => {
                need_arg_range(name, args, 1, 2)?;
                // xfaces.c face_color_gray_p on a tty frame: the terminal's
                // defined_color_hook, then GNU's near-black or
                // channels-within-a-twentieth test.
                let color = string_text(&args[0])?;
                let gray = tty_defined_color(interp, env, &color)?.is_some_and(|[r, g, b]| {
                    let (r, g, b) = (i64::from(r), i64::from(g), i64::from(b));
                    (r < 5000 && g < 5000 && b < 5000)
                        || ((r - g).abs() < r.max(g) / 20
                            && (g - b).abs() < g.max(b) / 20
                            && (b - r).abs() < b.max(r) / 20)
                });
                Ok(if gray { Value::T } else { Value::Nil })
            }
            "color-supported-p" => {
                need_arg_range(name, args, 1, 3)?;
                // xfaces.c face_color_supported_p on a tty frame is
                // tty_defined_color's verdict; BACKGROUND-P only matters on
                // window-system frames.
                let color = string_text(&args[0])?;
                Ok(if tty_defined_color(interp, env, &color)?.is_some() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "internal-make-lisp-face" => {
                need_arg_range(name, args, 1, 2)?;
                let face = face_symbol(&args[0])?;
                let target = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => None,
                    Some(frame) => Some(frames::decode_live_frame(
                        interp,
                        Some(&frame.value()),
                        true,
                    )?),
                };
                interp.make_lisp_face_on(face, target, true)
            }
            "internal-lisp-face-p" => {
                need_arg_range(name, args, 1, 2)?;
                if !matches!(args[0].kind(), Kind::Symbol(_) | Kind::StringObject(_)) {
                    return Ok(Value::Nil);
                }
                let face = resolve_face_name(interp, &args[0])?;
                let target = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => None,
                    Some(frame) => Some(frames::decode_live_frame(
                        interp,
                        Some(&frame.value()),
                        true,
                    )?),
                };
                Ok(interp
                    .lisp_face_vector_on(face, target)
                    .unwrap_or(Value::Nil))
            }
            "internal-copy-lisp-face" => {
                need_args(name, args, 4)?;
                let from = face_symbol(&args[0])?;
                let to = face_symbol(&args[1])?;
                let (source, target) = if matches!(args[2].kind(), Kind::T) {
                    (None, None)
                } else {
                    let source = frames::decode_live_frame(interp, Some(&args[2]), false)?;
                    let target = if args[3].is_nil() {
                        source
                    } else {
                        frames::decode_live_frame(interp, Some(&args[3]), false)?
                    };
                    (Some(source), Some(target))
                };
                interp.copy_lisp_face_attributes(from, to, source, target)?;
                Ok(to)
            }
            "internal-set-lisp-face-attribute" => {
                need_arg_range(name, args, 3, 4)?;
                let face = resolve_face_name(interp, &face_symbol(&args[0])?)?;
                let attribute = args[1].as_symbol()?;
                let frame = args.get(3).unwrap_or(&Value::Nil);
                if matches!(frame.kind(), Kind::Integer(0)) {
                    if interp.lisp_face_vector(face, true).is_none() {
                        return Err(invalid_face_error(face));
                    }
                    set_face_attribute(interp, face, attribute, &args[2], true, env)?;
                    let frames: Vec<_> = interp
                        .frame_states
                        .iter()
                        .filter(|frame| frame.is_live())
                        .copied()
                        .collect();
                    for frame in frames {
                        set_face_attribute_on(interp, face, attribute, &args[2], Some(frame), env)?;
                    }
                    Ok(face)
                } else if matches!(frame.kind(), Kind::T) {
                    if interp.lisp_face_vector(face, true).is_none() {
                        return Err(invalid_face_error(face));
                    }
                    set_face_attribute(interp, face, attribute, &args[2], true, env)
                } else {
                    let frame = frames::decode_live_frame(interp, Some(frame), true)?;
                    // GNU creates a missing frame-local face before it validates
                    // ATTR, so preserve that observable ordering here.
                    interp.make_lisp_face_on(face, Some(frame), false)?;
                    set_face_attribute_on(interp, face, attribute, &args[2], Some(frame), env)
                }
            }
            "internal-set-lisp-face-attribute-from-resource" => {
                need_arg_range(name, args, 3, 4)?;
                let face = resolve_face_name(interp, &args[0])?;
                let attribute = args[1].as_symbol()?;
                let text = string_text(&args[2])?;
                let value = resource_face_value(attribute, &text)?;
                set_face_attribute(
                    interp,
                    face,
                    attribute,
                    &value,
                    face_target_is_global(args.get(3)),
                    env,
                )
            }
            "face-attribute-relative-p" => {
                need_args(name, args, 2)?;
                let attribute = args[0].as_symbol()?;
                Ok(
                    if unspecified_face_value(&args[1])
                        || (attribute == ":height" && !matches!(args[1].kind(), Kind::Integer(_)))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "merge-face-attribute" => {
                need_args(name, args, 3)?;
                let attribute = args[0].as_symbol()?;
                if unspecified_face_value(&args[1]) {
                    Ok(args[2])
                } else if attribute == ":height" {
                    merge_face_heights(interp, args[1], args[2], args[1], env)
                } else {
                    Ok(args[1])
                }
            }
            "internal-get-lisp-face-attribute" => {
                need_arg_range(name, args, 2, 3)?;
                let face = resolve_face_name(interp, &args[0])?;
                let attribute = args[1].as_symbol()?;
                let index = face_attribute_index(attribute)
                    .ok_or_else(|| face_attribute_error(&args[1]))?;
                let target = if face_target_is_global(args.get(2)) {
                    None
                } else {
                    Some(frames::decode_live_frame(interp, args.get(2), true)?)
                };
                let value = interp
                    .lisp_face_vector_on(face, target)
                    .and_then(|vector| vector_slot_value(&vector, index).ok())
                    .ok_or_else(|| invalid_face_error(face))?;
                Ok(
                    if matches!(value.kind(), Kind::Symbol(symbol) if symbol == ":ignore-defface") {
                        Value::symbol("unspecified")
                    } else {
                        value
                    },
                )
            }
            "internal-lisp-face-attribute-values" => {
                need_args(name, args, 1)?;
                let attribute = args[0].as_symbol()?;
                Ok(
                    if matches!(
                        attribute,
                        ":underline"
                            | ":overline"
                            | ":strike-through"
                            | ":inverse-video"
                            | ":reverse-video"
                            | ":extend"
                    ) {
                        Value::list([Value::T, Value::Nil])
                    } else {
                        Value::Nil
                    },
                )
            }
            "internal-merge-in-global-face" => {
                need_args(name, args, 2)?;
                let frame = frames::decode_live_frame(interp, Some(&args[1]), false)?;
                let face = resolve_face_name(interp, &args[0])?;
                let global = interp
                    .lisp_face_vector(face, true)
                    .ok_or_else(|| invalid_face_error(face))?;
                let local = interp.make_lisp_face_on(face, Some(frame), false)?;
                for index in 1..LFACE_VECTOR_SIZE {
                    let value = vector_slot_value(&global, index)?;
                    if matches!(value.kind(), Kind::Symbol(symbol) if symbol == ":ignore-defface") {
                        aset_vector_value(&local, index, Value::symbol("unspecified"))?;
                    } else if !matches!(value.kind(), Kind::Symbol(symbol) if symbol == "unspecified")
                    {
                        aset_vector_value(&local, index, value)?;
                    }
                }
                Ok(Value::Nil)
            }
            "face-font" => {
                need_arg_range(name, args, 1, 3)?;
                let face = resolve_face_name(interp, &args[0])?;
                if matches!(args.get(1).map(|v| v.kind()), Some(Kind::T)) {
                    let vector = interp
                        .lisp_face_vector(face, true)
                        .ok_or_else(|| invalid_face_error(face))?;
                    let weight = vector_slot_value(&vector, 5)?;
                    let slant = vector_slot_value(&vector, 6)?;
                    let mut result = Vec::new();
                    if !matches!(weight.kind(), Kind::Symbol(symbol) if matches!(symbol.as_str(), "unspecified" | "normal"))
                    {
                        result.push(Value::symbol("bold"));
                    }
                    if !matches!(slant.kind(), Kind::Symbol(symbol) if matches!(symbol.as_str(), "unspecified" | "normal"))
                    {
                        result.insert(0, Value::symbol("italic"));
                    }
                    Ok(Value::list(result))
                } else {
                    Ok(Value::Nil)
                }
            }
            "internal-lisp-face-equal-p" => {
                need_arg_range(name, args, 2, 3)?;
                let target = if face_target_is_global(args.get(2)) {
                    None
                } else {
                    Some(frames::decode_live_frame(interp, args.get(2), true)?)
                };
                let left_name = resolve_face_name(interp, &args[0])?;
                let right_name = resolve_face_name(interp, &args[1])?;
                let left = interp
                    .lisp_face_vector_on(left_name, target)
                    .ok_or_else(|| invalid_face_error(left_name))?;
                let right = interp
                    .lisp_face_vector_on(right_name, target)
                    .ok_or_else(|| invalid_face_error(right_name))?;
                Ok(if face_vectors_equal(interp, &left, &right)? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "internal-lisp-face-empty-p" => {
                need_arg_range(name, args, 1, 2)?;
                let target = if face_target_is_global(args.get(1)) {
                    None
                } else {
                    Some(frames::decode_live_frame(interp, args.get(1), true)?)
                };
                let face = resolve_face_name(interp, &args[0])?;
                let vector = interp
                    .lisp_face_vector_on(face, target)
                    .ok_or_else(|| invalid_face_error(face))?;
                for index in 1..LFACE_VECTOR_SIZE {
                    if !matches!(
                        (vector_slot_value(&vector, index)?).kind(),
                        Kind::Symbol(symbol) if symbol == "unspecified"
                    ) {
                        return Ok(Value::Nil);
                    }
                }
                Ok(Value::T)
            }
            "face-attributes-as-vector" => {
                need_args(name, args, 1)?;
                let vector = unspecified_face_vector();
                let values = args[0].to_vec()?;
                if !values.len().is_multiple_of(2) {
                    return Err(LispError::Signal("Invalid face attribute list".into()));
                }
                for pair in values.as_chunks::<2>().0 {
                    let attribute = pair[0].as_symbol()?;
                    let (index, value) = normalize_face_attribute_value(attribute, &pair[1])?;
                    aset_vector_value(&vector, index, value)?;
                }
                Ok(vector)
            }
            "internal-set-font-selection-order" => {
                need_args(name, args, 1)?;
                let values = args[0].to_vec()?;
                let expected = [":width", ":height", ":weight", ":slant"];
                if values.len() != expected.len()
                || values.iter().any(|value| {
                    !matches!(value.kind(), Kind::Symbol(symbol) if expected.contains(&symbol.as_str()))
                })
            {
                return Err(LispError::Signal("Invalid font sort order".into()));
            }
                interp.font_selection_order = std::array::from_fn(|index| {
                    values[index]
                        .as_symbol()
                        .expect("validated font selection order item")
                        .to_string()
                });
                Ok(Value::Nil)
            }
            "internal-set-alternative-font-family-alist" => {
                need_args(name, args, 1)?;
                let transformed = transform_font_alist(&args[0], false)?;
                interp.alternative_font_family_alist = transformed;
                Ok(transformed)
            }
            "internal-set-alternative-font-registry-alist" => {
                need_args(name, args, 1)?;
                let transformed = transform_font_alist(&args[0], true)?;
                interp.alternative_font_registry_alist = transformed;
                Ok(transformed)
            }
            "tty-suppress-bold-inverse-default-colors" => {
                need_args(name, args, 1)?;
                interp.tty_suppress_bold_inverse_default_colors = args[0].is_truthy();
                Ok(args[0])
            }
            "x-family-fonts" => {
                need_arg_range(name, args, 0, 2)?;
                if let Some(family) = args.first().filter(|value| !value.is_nil()) {
                    string_text(family)?;
                }
                Ok(Value::Nil)
            }
            "x-list-fonts" => {
                need_arg_range(name, args, 1, 5)?;
                string_text(&args[0])?;
                Err(LispError::Signal(
                    "Window system is not in use or not initialized".into(),
                ))
            }
            "x-load-color-file" => {
                need_args(name, args, 1)?;
                Ok(load_color_file(&string_text(&args[0])?))
            }
        }
    }
);

/// term.c tty_defined_color, the tty frame's defined_color_hook: an empty
/// name is the default color (black, status true); otherwise xfaces.c
/// tty_lookup_color asks Lisp `tty-color-desc' and takes the (NAME INDEX R G
/// B) it returns, treats a nil `tty-defined-color-alist' as "not set up yet"
/// (true, black), and rejects anything else -- except that
/// "unspecified-fg"/"unspecified-bg" still resolve to the default pixels.
/// Some(rgb) is a successful lookup, None the failure the callers report as
/// "Invalid color".
pub(crate) fn tty_defined_color(
    interp: &mut Interpreter,
    env: &mut Env,
    color: &str,
) -> Result<Option<[u16; 3]>, LispError> {
    if color.is_empty() {
        return Ok(Some([0, 0, 0]));
    }
    let looked_up = if interp.lookup_function("tty-color-desc", env).is_ok() {
        let desc = interp.call_function_value(
            Value::Symbol("tty-color-desc".into()),
            Some("tty-color-desc"),
            &[Value::String(color.into()), Value::Nil],
            env,
        )?;
        let items = desc.to_vec().unwrap_or_default();
        if items.len() >= 2 {
            match items
                .as_slice()
                .iter()
                .map(|v| v.kind())
                .collect::<Vec<_>>()
                .as_slice()
            {
                [
                    _,
                    Kind::Integer(_),
                    Kind::Integer(r),
                    Kind::Integer(g),
                    Kind::Integer(b),
                    ..,
                ] => Some(Some([*r as u16, *g as u16, *b as u16])),
                _ => Some(None),
            }
        } else if interp
            .lookup_var("tty-defined-color-alist", env)
            .is_none_or(|alist| alist.is_nil())
        {
            Some(Some([0, 0, 0]))
        } else {
            Some(None)
        }
    } else {
        None
    };
    Ok(match looked_up {
        Some(Some(rgb)) => Some(rgb),
        _ if matches!(color, "unspecified-fg" | "unspecified-bg") => Some([0, 0, 0]),
        _ => None,
    })
}
