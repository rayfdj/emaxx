use super::*;
use crate::lisp::eval::LFACE_VECTOR_SIZE;
use crate::lisp::reader::Reader;

const XFACE_BUILTINS: &[&str] = &[
    "bitmap-spec-p",
    "clear-face-cache",
    "color-gray-p",
    "color-supported-p",
    "face-attribute-relative-p",
    "face-attributes-as-vector",
    "face-font",
    "internal-copy-lisp-face",
    "internal-get-lisp-face-attribute",
    "internal-lisp-face-attribute-values",
    "internal-lisp-face-empty-p",
    "internal-lisp-face-equal-p",
    "internal-lisp-face-p",
    "internal-make-lisp-face",
    "internal-merge-in-global-face",
    "internal-set-alternative-font-family-alist",
    "internal-set-alternative-font-registry-alist",
    "internal-set-font-selection-order",
    "internal-set-lisp-face-attribute",
    "internal-set-lisp-face-attribute-from-resource",
    "merge-face-attribute",
    "tty-suppress-bold-inverse-default-colors",
    "x-family-fonts",
    "x-list-fonts",
    "x-load-color-file",
];

pub(super) fn handles(name: &str) -> bool {
    XFACE_BUILTINS.contains(&name)
}

fn resolve_face_name(interp: &Interpreter, value: &Value) -> Result<String, LispError> {
    let mut name = match value {
        Value::Symbol(name) => name.clone(),
        Value::String(_) | Value::StringObject(_) => string_text(value)?,
        _ => return Err(wrong_type_argument("symbolp", value.clone())),
    };
    let mut seen = HashSet::new();
    while seen.insert(name.clone()) {
        let Some(alias) = interp
            .get_symbol_property(&name, "face-alias")
            .and_then(|value| value.as_symbol().ok().map(str::to_string))
        else {
            break;
        };
        name = alias;
    }
    Ok(name)
}

fn face_vector(items: impl IntoIterator<Item = Value>) -> Value {
    Value::list(std::iter::once(Value::symbol("vector-literal")).chain(items))
}

fn unspecified_face_vector() -> Value {
    face_vector(std::iter::repeat_n(
        Value::symbol("unspecified"),
        LFACE_VECTOR_SIZE,
    ))
}

fn face_target_is_global(frame: Option<&Value>) -> bool {
    matches!(frame, Some(Value::T))
}

fn face_attribute_error(attribute: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("Invalid face attribute name"),
        attribute.clone(),
    ]))
}

fn special_face_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Symbol(symbol)
            if matches!(
                symbol.as_str(),
                "unspecified" | "ignore-defface" | "reset"
            )
    )
}

fn valid_named_value(value: &Value, choices: &[&str]) -> bool {
    special_face_value(value)
        || matches!(value, Value::Symbol(symbol) if choices.contains(&symbol.as_str()))
}

fn valid_inherit_value(value: &Value) -> bool {
    matches!(value, Value::Nil | Value::Symbol(_))
        || value
            .to_vec()
            .is_ok_and(|items| items.iter().all(|item| matches!(item, Value::Symbol(_))))
}

fn normalize_face_attribute_value(
    attribute: &str,
    value: &Value,
) -> Result<(usize, Value), LispError> {
    let index = face_attribute_index(attribute)
        .ok_or_else(|| face_attribute_error(&Value::symbol(attribute)))?;
    let normalized = match attribute {
        ":bold" => {
            if matches!(value, Value::Symbol(symbol) if symbol == "reset") {
                value.clone()
            } else if value.is_nil() {
                Value::symbol("normal")
            } else {
                Value::symbol("bold")
            }
        }
        ":italic" => {
            if matches!(value, Value::Symbol(symbol) if symbol == "reset") {
                value.clone()
            } else if value.is_nil() {
                Value::symbol("normal")
            } else {
                Value::symbol("italic")
            }
        }
        _ if special_face_value(value) => value.clone(),
        ":family" | ":foundry" => {
            let text = string_text(value)?;
            if text.is_empty() {
                return Err(LispError::Signal("Invalid face family".into()));
            }
            value.clone()
        }
        ":height" => match value {
            Value::Integer(height) if *height > 0 => value.clone(),
            Value::Float(scale) if scale.is_finite() && *scale > 0.0 => value.clone(),
            Value::Lambda(..) | Value::BuiltinFunc(_) | Value::Symbol(_) => value.clone(),
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
            value.clone()
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
            value.clone()
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
            value.clone()
        }
        ":underline" => {
            if !(matches!(value, Value::Nil | Value::T | Value::Cons(..)) || value.is_string()) {
                return Err(LispError::Signal("Invalid face underline".into()));
            }
            value.clone()
        }
        ":overline" | ":strike-through" => {
            if !(matches!(value, Value::Nil | Value::T) || value.is_string()) {
                return Err(LispError::Signal("Invalid face line attribute".into()));
            }
            value.clone()
        }
        ":inverse-video" | ":reverse-video" | ":extend" => {
            if !matches!(value, Value::Nil | Value::T) {
                return Err(LispError::Signal("Invalid boolean face attribute".into()));
            }
            value.clone()
        }
        ":foreground" | ":distant-foreground" | ":background" => {
            if value.is_nil() {
                Value::symbol("unspecified")
            } else {
                let text = string_text(value)?;
                if text.is_empty() {
                    return Err(LispError::Signal("Empty face color value".into()));
                }
                value.clone()
            }
        }
        ":stipple" => value.clone(),
        ":box" => {
            if !(matches!(
                value,
                Value::Nil | Value::T | Value::Integer(_) | Value::Cons(..)
            ) || value.is_string())
            {
                return Err(LispError::Signal("Invalid face box".into()));
            }
            if matches!(value, Value::T) {
                Value::Integer(1)
            } else {
                value.clone()
            }
        }
        ":font" | ":fontset" => value.clone(),
        ":inherit" => {
            if !valid_inherit_value(value) {
                return Err(LispError::Signal("Invalid face inheritance".into()));
            }
            value.clone()
        }
        _ => return Err(face_attribute_error(&Value::symbol(attribute))),
    };
    Ok((index, normalized))
}

pub(super) fn set_face_attribute(
    interp: &mut Interpreter,
    face: &str,
    attribute: &str,
    value: &Value,
    global: bool,
) -> Result<Value, LispError> {
    let (index, normalized) = normalize_face_attribute_value(attribute, value)?;
    interp.set_lisp_face_attribute(face, index, normalized, global)?;
    Ok(Value::symbol(face))
}

fn merge_face_height(
    interp: &mut Interpreter,
    from: &Value,
    to: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match from {
        Value::Integer(_) => Ok(from.clone()),
        Value::Float(scale) => match to {
            Value::Integer(height) => Ok(Value::Integer((*scale * *height as f64) as i64)),
            Value::Float(height) => Ok(Value::Float(*scale * *height)),
            Value::Symbol(symbol) if symbol == "unspecified" => Ok(from.clone()),
            _ => Ok(from.clone()),
        },
        Value::Lambda(..) | Value::BuiltinFunc(_) | Value::Symbol(_) => {
            interp.call_function_value(from.clone(), None, std::slice::from_ref(to), env)
        }
        _ => Ok(from.clone()),
    }
}

fn values_are_equal(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    values_equal(interp, left, right)
}

fn face_vectors_equal(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
) -> Result<bool, LispError> {
    for index in 1..LFACE_VECTOR_SIZE {
        if !values_are_equal(
            interp,
            &vector_slot_value(left, index)?,
            &vector_slot_value(right, index)?,
        ) {
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
    let [Value::Integer(width), Value::Integer(height), data, ..] = items.as_slice() else {
        return false;
    };
    if *width <= 0 || *height <= 0 {
        return false;
    }
    let Some(data) = string_like(data) else {
        return false;
    };
    let bytes_per_row = (*width as usize).div_ceil(8);
    (*height as usize) <= data.text.len() / bytes_per_row
}

fn color_gray_p(value: [u16; 3]) -> bool {
    let [red, green, blue] = value.map(i64::from);
    (red < 5_000 && green < 5_000 && blue < 5_000)
        || ((red - green).abs() < red.max(green) / 20
            && (green - blue).abs() < green.max(blue) / 20
            && (blue - red).abs() < blue.max(red) / 20)
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
                Value::String(text.to_lowercase())
            } else {
                Value::Symbol(text)
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

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    match name {
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
            let color = string_text(&args[0])?;
            Ok(if parse_color_spec(&color).is_some_and(color_gray_p) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "color-supported-p" => {
            need_arg_range(name, args, 1, 3)?;
            let color = string_text(&args[0])?;
            Ok(if parse_color_spec(&color).is_some() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "internal-make-lisp-face" => {
            need_arg_range(name, args, 1, 2)?;
            let face = args[0]
                .as_symbol()
                .map_err(|_| wrong_type_argument("symbolp", args[0].clone()))?;
            let vector =
                interp.ensure_lisp_face(face, args.get(1).is_some_and(Value::is_truthy), true)?;
            interp.register_lisp_face_id(face);
            Ok(vector)
        }
        "internal-lisp-face-p" => {
            need_arg_range(name, args, 1, 2)?;
            let face = resolve_face_name(interp, &args[0])?;
            Ok(interp
                .lisp_face_vector(&face, args.get(1).is_none_or(Value::is_nil))
                .unwrap_or(Value::Nil))
        }
        "internal-copy-lisp-face" => {
            need_args(name, args, 4)?;
            let from = args[0]
                .as_symbol()
                .map_err(|_| wrong_type_argument("symbolp", args[0].clone()))?;
            let to = args[1]
                .as_symbol()
                .map_err(|_| wrong_type_argument("symbolp", args[1].clone()))?;
            let global = matches!(args[2], Value::T);
            interp.ensure_lisp_face(to, !global, false)?;
            interp.register_lisp_face_id(to);
            interp.copy_lisp_face_attributes(from, to, global)?;
            Ok(Value::symbol(to))
        }
        "internal-set-lisp-face-attribute" => {
            need_arg_range(name, args, 3, 4)?;
            let face = resolve_face_name(interp, &args[0])?;
            let attribute = args[1].as_symbol()?;
            let global = face_target_is_global(args.get(3));
            if matches!(args.get(3), Some(Value::Integer(0))) {
                set_face_attribute(interp, &face, attribute, &args[2], true)?;
                set_face_attribute(interp, &face, attribute, &args[2], false)
            } else {
                set_face_attribute(interp, &face, attribute, &args[2], global)
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
                &face,
                attribute,
                &value,
                face_target_is_global(args.get(3)),
            )
        }
        "face-attribute-relative-p" => {
            need_args(name, args, 2)?;
            let attribute = args[0].as_symbol()?;
            Ok(
                if matches!(&args[1], Value::Symbol(symbol) if matches!(symbol.as_str(), "unspecified" | "ignore-defface"))
                    || (attribute == ":height" && !matches!(args[1], Value::Integer(_)))
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
            if matches!(&args[1], Value::Symbol(symbol) if matches!(symbol.as_str(), "unspecified" | "ignore-defface"))
            {
                Ok(args[2].clone())
            } else if attribute == ":height" {
                merge_face_height(interp, &args[1], &args[2], env)
            } else {
                Ok(args[1].clone())
            }
        }
        "internal-get-lisp-face-attribute" => {
            need_arg_range(name, args, 2, 3)?;
            let face = resolve_face_name(interp, &args[0])?;
            let attribute = args[1].as_symbol()?;
            let index =
                face_attribute_index(attribute).ok_or_else(|| face_attribute_error(&args[1]))?;
            let value = interp
                .lisp_face_attribute(&face, index, face_target_is_global(args.get(2)))
                .ok_or_else(|| LispError::Signal(format!("Invalid face: {face}")))?;
            Ok(
                if matches!(&value, Value::Symbol(symbol) if symbol == "ignore-defface") {
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
            let face = resolve_face_name(interp, &args[0])?;
            let global = interp
                .lisp_face_vector(&face, true)
                .ok_or_else(|| LispError::Signal(format!("Invalid face: {face}")))?;
            let local = interp.ensure_lisp_face(&face, true, false)?;
            for index in 1..LFACE_VECTOR_SIZE {
                let value = vector_slot_value(&global, index)?;
                if matches!(&value, Value::Symbol(symbol) if symbol == "ignore-defface") {
                    aset_vector_value(&local, index, Value::symbol("unspecified"))?;
                } else if !matches!(&value, Value::Symbol(symbol) if symbol == "unspecified") {
                    aset_vector_value(&local, index, value)?;
                }
            }
            Ok(Value::Nil)
        }
        "face-font" => {
            need_arg_range(name, args, 1, 3)?;
            let face = resolve_face_name(interp, &args[0])?;
            if matches!(args.get(1), Some(Value::T)) {
                let vector = interp
                    .lisp_face_vector(&face, true)
                    .ok_or_else(|| LispError::Signal(format!("Invalid face: {face}")))?;
                let weight = vector_slot_value(&vector, 5)?;
                let slant = vector_slot_value(&vector, 6)?;
                let mut result = Vec::new();
                if !matches!(&weight, Value::Symbol(symbol) if matches!(symbol.as_str(), "unspecified" | "normal"))
                {
                    result.push(Value::symbol("bold"));
                }
                if !matches!(&slant, Value::Symbol(symbol) if matches!(symbol.as_str(), "unspecified" | "normal"))
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
            let global = face_target_is_global(args.get(2));
            let left_name = resolve_face_name(interp, &args[0])?;
            let right_name = resolve_face_name(interp, &args[1])?;
            let left = interp
                .lisp_face_vector(&left_name, global)
                .ok_or_else(|| LispError::Signal(format!("Invalid face: {left_name}")))?;
            let right = interp
                .lisp_face_vector(&right_name, global)
                .ok_or_else(|| LispError::Signal(format!("Invalid face: {right_name}")))?;
            Ok(if face_vectors_equal(interp, &left, &right)? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "internal-lisp-face-empty-p" => {
            need_arg_range(name, args, 1, 2)?;
            let global = face_target_is_global(args.get(1));
            let face = resolve_face_name(interp, &args[0])?;
            let vector = interp
                .lisp_face_vector(&face, global)
                .ok_or_else(|| LispError::Signal(format!("Invalid face: {face}")))?;
            for index in 1..LFACE_VECTOR_SIZE {
                if !matches!(
                    vector_slot_value(&vector, index)?,
                    Value::Symbol(symbol) if symbol == "unspecified"
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
            for pair in values.chunks_exact(2) {
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
                    !matches!(value, Value::Symbol(symbol) if expected.contains(&symbol.as_str()))
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
            interp.alternative_font_family_alist = transformed.clone();
            Ok(transformed)
        }
        "internal-set-alternative-font-registry-alist" => {
            need_args(name, args, 1)?;
            let transformed = transform_font_alist(&args[0], true)?;
            interp.alternative_font_registry_alist = transformed.clone();
            Ok(transformed)
        }
        "tty-suppress-bold-inverse-default-colors" => {
            need_args(name, args, 1)?;
            interp.tty_suppress_bold_inverse_default_colors = args[0].is_truthy();
            Ok(args[0].clone())
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
        _ => unreachable!("xfaces dispatcher called for unsupported primitive"),
    }
}
