use super::*;
use crate::lisp::eval::LFACE_VECTOR_SIZE;
use crate::lisp::primitives::font;
use crate::lisp::types::Kind;

/// xfaces.c:resolve_face_name (FACE, true): a string names the symbol it
/// interns, and nil or any other object that is no symbol comes back
/// as it is; a symbol's `face-alias' chain is followed to its last
/// symbol, the hare two links to the tortoise's one, and a chain that
/// comes round again signals `circular-list' with the face.
fn resolve_face_name(interp: &mut Interpreter, value: &Value) -> Result<Value, LispError> {
    resolve_face_name_1(interp, value, true)
}

/// resolve_face_name (FACE, SIGNAL): a circular alias chain `default'
/// when not SIGNAL.
fn resolve_face_name_1(
    interp: &mut Interpreter,
    value: &Value,
    signal: bool,
) -> Result<Value, LispError> {
    let face = match value.kind() {
        Kind::Symbol(symbol) => Value::Symbol(symbol),
        Kind::T => Value::T,
        Kind::StringObject(_) => Value::Symbol(crate::lisp::types::SymbolName::intern_str(
            &string_text(value)?,
        )),
        _ => return Ok(*value),
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
            if !signal {
                return Ok(Value::symbol("default"));
            }
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

fn face_target_is_global(frame: Option<&Value>) -> bool {
    matches!(frame.map(|v| v.kind()), Some(Kind::T))
}

fn face_attribute_error(attribute: &Value) -> LispError {
    signal_error("Invalid face attribute name", *attribute)
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

/// xfaces.c:named_merge_point_kind.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NamedMergePointKind {
    Normal,
    Remap,
}

/// The named merge points a merge has passed, innermost last.
type NamedMergePoints = Vec<(Value, NamedMergePointKind)>;

/// A face's Lisp attribute vector being merged.
type LfaceAttrs = [Value; LFACE_VECTOR_SIZE];

const LFACE_FAMILY_INDEX: usize = 1;
const LFACE_FOUNDRY_INDEX: usize = 2;
const LFACE_SWIDTH_INDEX: usize = 3;
const LFACE_HEIGHT_INDEX: usize = 4;
const LFACE_WEIGHT_INDEX: usize = 5;
const LFACE_SLANT_INDEX: usize = 6;
const LFACE_UNDERLINE_INDEX: usize = 7;
const LFACE_INVERSE_INDEX: usize = 8;
const LFACE_FOREGROUND_INDEX: usize = 9;
const LFACE_BACKGROUND_INDEX: usize = 10;
const LFACE_STIPPLE_INDEX: usize = 11;
const LFACE_OVERLINE_INDEX: usize = 12;
const LFACE_STRIKE_THROUGH_INDEX: usize = 13;
const LFACE_BOX_INDEX: usize = 14;
const LFACE_FONT_INDEX: usize = 15;
const LFACE_INHERIT_INDEX: usize = 16;
const LFACE_DISTANT_FOREGROUND_INDEX: usize = 18;
const LFACE_EXTEND_INDEX: usize = 19;

/// UNSPECIFIEDP.
fn unspecifiedp(value: &Value) -> bool {
    matches!(value.kind(), Kind::Symbol(symbol) if symbol == "unspecified")
}

/// xfaces.c:push_named_merge_point: whether FACE_NAME may be merged as
/// KIND below POINTS, pushed on them when it may.  The same kind of
/// merge of the same face again is circular; a remap of it hides any
/// normal merge further out.
fn push_named_merge_point(
    points: &mut NamedMergePoints,
    face_name: Value,
    kind: NamedMergePointKind,
) -> bool {
    for &(prev, prev_kind) in points.iter().rev() {
        if prev.eq_value(face_name) {
            if prev_kind == kind {
                return false;
            } else if prev_kind == NamedMergePointKind::Remap {
                break;
            }
        }
    }
    points.push((face_name, kind));
    true
}

/// xfaces.c:get_lface_attributes_no_remap: FACE_NAME's definition on
/// FRAME copied to ATTRS; none is "Invalid face" when SIGNAL.
fn get_lface_attributes_no_remap(
    interp: &Interpreter,
    frame: crate::lisp::types::FrameRef,
    face_name: Value,
    attrs: &mut LfaceAttrs,
    signal: bool,
) -> Result<bool, LispError> {
    let Some(lface) = interp.lisp_face_vector_on(face_name, Some(frame)) else {
        return if signal {
            Err(invalid_face_error(face_name))
        } else {
            Ok(false)
        };
    };
    for (index, attr) in attrs.iter_mut().enumerate() {
        *attr = vector_slot_value(&lface, index)?;
    }
    Ok(true)
}

/// xfaces.c:get_lface_attributes: FACE_NAME resolved, then its
/// `face-remapping-alist' remapping merged into ATTRS made unspecified,
/// or its own definition on FRAME.
fn get_lface_attributes(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    face_name: Value,
    attrs: &mut LfaceAttrs,
    signal: bool,
    points: &mut NamedMergePoints,
    env: &mut Env,
) -> Result<bool, LispError> {
    let face_name = resolve_face_name_1(interp, &face_name, signal)?;
    // assq_no_quit (FACE_NAME, Vface_remapping_alist).
    let mut tail = interp
        .lookup_var("face-remapping-alist", env)
        .unwrap_or(Value::Nil);
    let mut remapping = None;
    while let Some((element, rest)) = tail.cons_values() {
        if let Some((key, remap)) = element.cons_values()
            && key.eq_value(face_name)
        {
            remapping = Some(remap);
            break;
        }
        tail = rest;
    }
    if let Some(remap) = remapping
        && push_named_merge_point(points, face_name, NamedMergePointKind::Remap)
    {
        attrs[1..].fill(Value::symbol("unspecified"));
        let merged = merge_face_ref(interp, frame, remap, attrs, signal, points, env);
        points.pop();
        return merged;
    }
    get_lface_attributes_no_remap(interp, frame, face_name, attrs, signal)
}

/// xfaces.c:merge_face_vectors: what FROM inherits merged into TO, then
/// FROM's own specified attributes (a relative height merged with TO's),
/// each clearing the font property it overrides; TO inherits nothing.
/// Only a window-system frame's definitions hold a font spec, so a text
/// frame's merge never meets one in FROM.
fn merge_face_vectors(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    from: &LfaceAttrs,
    to: &mut LfaceAttrs,
    points: &mut NamedMergePoints,
    env: &mut Env,
) -> Result<(), LispError> {
    let inherit = from[LFACE_INHERIT_INDEX];
    if !unspecifiedp(&inherit) && !inherit.is_nil() {
        merge_face_ref(interp, frame, inherit, to, false, points, env)?;
    }
    for index in 1..LFACE_VECTOR_SIZE {
        if unspecifiedp(&from[index]) {
            continue;
        }
        if index == LFACE_HEIGHT_INDEX && !matches!(from[index].kind(), Kind::Integer(_)) {
            to[index] = merge_face_heights(interp, from[index], to[index], to[index], env)?;
            font::font_clear_prop(interp, to, font::FONT_SIZE_INDEX, env)?;
        } else if index != LFACE_FONT_INDEX && !to[index].eq_value(from[index]) {
            to[index] = from[index];
            if (LFACE_FAMILY_INDEX..=LFACE_SLANT_INDEX).contains(&index) {
                let prop = match index {
                    LFACE_FAMILY_INDEX => font::FONT_FAMILY_INDEX,
                    LFACE_FOUNDRY_INDEX => font::FONT_FOUNDRY_INDEX,
                    LFACE_SWIDTH_INDEX => font::FONT_WIDTH_INDEX,
                    LFACE_HEIGHT_INDEX => font::FONT_SIZE_INDEX,
                    LFACE_WEIGHT_INDEX => font::FONT_WEIGHT_INDEX,
                    _ => font::FONT_SLANT_INDEX,
                };
                font::font_clear_prop(interp, to, prop, env)?;
            }
        }
    }
    to[LFACE_INHERIT_INDEX] = Value::Nil;
    Ok(())
}

/// xfaces.c:merge_named_face: FACE_NAME's attributes on FRAME, its
/// `reset' ones the default face's, merged into TO unless the merge is
/// circular.
fn merge_named_face(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    face_name: Value,
    to: &mut LfaceAttrs,
    points: &mut NamedMergePoints,
    env: &mut Env,
) -> Result<bool, LispError> {
    if !push_named_merge_point(points, face_name, NamedMergePointKind::Normal) {
        return Ok(false);
    }
    let merged = (|| {
        let mut from: LfaceAttrs = std::array::from_fn(|_| Value::symbol("unspecified"));
        let ok = get_lface_attributes(interp, frame, face_name, &mut from, false, points, env)?;
        if ok && !face_name.eq_value(Value::symbol("default")) {
            // FACE_FROM_ID (F, DEFAULT_FACE_ID)->lface.
            let default = Value::symbol("default");
            let deflt = match interp.lisp_face_vector_on(default, Some(frame)) {
                Some(lface) => lface,
                None => {
                    super::frames::realize_basic_faces(interp, frame)?;
                    interp
                        .lisp_face_vector_on(default, Some(frame))
                        .ok_or_else(|| invalid_face_error(default))?
                }
            };
            for (index, attr) in from.iter_mut().enumerate().skip(1) {
                if matches!(attr.kind(), Kind::Symbol(symbol) if symbol == "reset") {
                    *attr = vector_slot_value(&deflt, index)?;
                }
            }
        }
        if ok {
            merge_face_vectors(interp, frame, &from, to, points, env)?;
        }
        Ok(ok)
    })();
    points.pop();
    merged
}

/// xfaces.c:evaluate_face_filter with no window: nil matches, as every
/// filter does under `face-filters-always-match'; a (:window PARAMETER
/// VALUE) filter has no window's parameter to match, and anything else
/// is invalid.
fn evaluate_face_filter(
    interp: &mut Interpreter,
    filter: Value,
    ok: &mut bool,
    err_msgs: bool,
    env: &mut Env,
) -> Result<bool, LispError> {
    if filter.is_nil() {
        return Ok(true);
    }
    if interp
        .lookup_var("face-filters-always-match", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(true);
    }
    let well_formed = filter.cons_values().and_then(|(head, rest)| {
        matches!(head.kind(), Kind::Symbol(symbol) if symbol == ":window").then_some(())?;
        let (_parameter, rest) = rest.cons_values()?;
        let (_value, rest) = rest.cons_values()?;
        rest.is_nil().then_some(())
    });
    if well_formed.is_some() {
        return Ok(false);
    }
    if err_msgs {
        super::display::add_to_log(interp, "Invalid face filter %S", &[filter], env)?;
    }
    *ok = false;
    Ok(false)
}

/// xfaces.c:filter_face_ref: a (:filtered FILTER FACE-REF) reference
/// is FACE-REF where FILTER matches, else nil; any other is itself.
fn filter_face_ref(
    interp: &mut Interpreter,
    face_ref: Value,
    ok: &mut bool,
    err_msgs: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some((head, rest)) = face_ref.cons_values() else {
        return Ok(face_ref);
    };
    if !matches!(head.kind(), Kind::Symbol(symbol) if symbol == ":filtered") {
        return Ok(face_ref);
    }
    let parts = rest.cons_values().and_then(|(filter, rest)| {
        let (filtered, rest) = rest.cons_values()?;
        rest.is_nil().then_some((filter, filtered))
    });
    if let Some((filter, filtered)) = parts {
        return Ok(
            if evaluate_face_filter(interp, filter, ok, err_msgs, env)? {
                filtered
            } else {
                Value::Nil
            },
        );
    }
    if err_msgs {
        super::display::add_to_log(interp, "Invalid face ref %S", &[face_ref], env)?;
    }
    *ok = false;
    Ok(Value::Nil)
}

/// xfaces.c:merge_face_ref with no window and no attribute filter:
/// FACE_REF, a face name, a (foreground-color . COLOR) or
/// (background-color . COLOR) pair, a face attribute plist or a list of
/// face references (the earlier ones first in precedence), merged into
/// TO.  A bad reference is logged, when ERR_MSGS, and the merge goes on;
/// the result is whether all was well.
fn merge_face_ref(
    interp: &mut Interpreter,
    frame: crate::lisp::types::FrameRef,
    face_ref: Value,
    to: &mut LfaceAttrs,
    err_msgs: bool,
    points: &mut NamedMergePoints,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut ok = true;
    let mut face_ref = face_ref;
    let mut filtered = face_ref;
    loop {
        face_ref = filtered;
        filtered = filter_face_ref(interp, face_ref, &mut ok, err_msgs, env)?;
        if !(ok && !face_ref.eq_value(filtered)) {
            break;
        }
    }
    if !ok {
        return Ok(false);
    }
    if face_ref.is_nil() {
        return Ok(true);
    }
    let Some((first, rest)) = face_ref.cons_values() else {
        // FACE_REF ought to be a face name.
        let ok = merge_named_face(interp, frame, face_ref, to, points, env)?;
        if !ok && err_msgs {
            super::display::add_to_log(interp, "Invalid face reference: %s", &[face_ref], env)?;
        }
        return Ok(ok);
    };
    let symbol_is =
        |value: &Value, name: &str| matches!(value.kind(), Kind::Symbol(symbol) if symbol == name);
    if symbol_is(&first, "foreground-color") || symbol_is(&first, "background-color") {
        if rest.is_string() {
            let index = if symbol_is(&first, "foreground-color") {
                LFACE_FOREGROUND_INDEX
            } else {
                LFACE_BACKGROUND_INDEX
            };
            to[index] = rest;
        } else {
            if err_msgs {
                super::display::add_to_log(interp, "Invalid face color %S", &[rest], env)?;
            }
            ok = false;
        }
    } else if matches!(first.kind(), Kind::Symbol(symbol) if symbol.as_str().starts_with(':')) {
        // The property list form.
        let mut plist = face_ref;
        while let Some((keyword, tail)) = plist.cons_values()
            && let Some((value, next)) = tail.cons_values()
        {
            let is = |name: &str| symbol_is(&keyword, name);
            let t_or_nil = matches!(value.kind(), Kind::T | Kind::Nil);
            let mut err = false;
            let mut set = |to: &mut LfaceAttrs, index: usize, valid: bool| {
                if valid {
                    to[index] = value;
                } else {
                    err = true;
                }
                valid
            };
            // Specifying `unspecified' is a no-op.
            if unspecifiedp(&value) {
            } else if is(":family") || is(":foundry") {
                let (index, prop) = if is(":family") {
                    (LFACE_FAMILY_INDEX, font::FONT_FAMILY_INDEX)
                } else {
                    (LFACE_FOUNDRY_INDEX, font::FONT_FOUNDRY_INDEX)
                };
                if set(to, index, value.is_string()) {
                    font::font_clear_prop(interp, to, prop, env)?;
                }
            } else if is(":height") {
                let height =
                    merge_face_heights(interp, value, to[LFACE_HEIGHT_INDEX], Value::Nil, env)?;
                if height.is_nil() {
                    err = true;
                } else {
                    to[LFACE_HEIGHT_INDEX] = height;
                    font::font_clear_prop(interp, to, font::FONT_SIZE_INDEX, env)?;
                }
            } else if is(":weight") || is(":slant") || is(":width") {
                let (index, prop) = if is(":weight") {
                    (LFACE_WEIGHT_INDEX, font::FONT_WEIGHT_INDEX)
                } else if is(":slant") {
                    (LFACE_SLANT_INDEX, font::FONT_SLANT_INDEX)
                } else {
                    (LFACE_SWIDTH_INDEX, font::FONT_WIDTH_INDEX)
                };
                // SYMBOLP and FONT_*_NAME_NUMERIC (VALUE) >= 0.
                let valid = matches!(value.kind(), Kind::Nil | Kind::T | Kind::Symbol(_))
                    && font::style_name_numeric(interp, prop, &value)? >= 0;
                if set(to, index, valid) {
                    font::font_clear_prop(interp, to, prop, env)?;
                }
            } else if is(":underline") {
                set(
                    to,
                    LFACE_UNDERLINE_INDEX,
                    t_or_nil || value.is_string() || matches!(value.kind(), Kind::Cons(_)),
                );
            } else if is(":overline") || is(":strike-through") {
                let index = if is(":overline") {
                    LFACE_OVERLINE_INDEX
                } else {
                    LFACE_STRIKE_THROUGH_INDEX
                };
                set(to, index, t_or_nil || value.is_string());
            } else if is(":box") {
                let value = if matches!(value.kind(), Kind::T) {
                    Value::Integer(1)
                } else {
                    value
                };
                if matches!(value.kind(), Kind::Integer(width) if width != 0)
                    || value.is_string()
                    || matches!(value.kind(), Kind::Cons(_) | Kind::Nil)
                {
                    to[LFACE_BOX_INDEX] = value;
                } else {
                    err = true;
                }
            } else if is(":inverse-video") || is(":reverse-video") {
                set(to, LFACE_INVERSE_INDEX, t_or_nil);
            } else if is(":foreground") {
                set(to, LFACE_FOREGROUND_INDEX, value.is_string());
            } else if is(":distant-foreground") {
                set(to, LFACE_DISTANT_FOREGROUND_INDEX, value.is_string());
            } else if is(":background") {
                set(to, LFACE_BACKGROUND_INDEX, value.is_string());
            } else if is(":stipple") {
                set(
                    to,
                    LFACE_STIPPLE_INDEX,
                    value.is_nil() || bitmap_spec_p(&value),
                );
            } else if is(":font") {
                let valid = font_value_p(interp, &value);
                set(to, LFACE_FONT_INDEX, valid);
            } else if is(":inherit") {
                // Just like a face reference.
                if !merge_face_ref(interp, frame, value, to, err_msgs, points, env)? {
                    err = true;
                }
            } else if is(":extend") {
                set(to, LFACE_EXTEND_INDEX, t_or_nil);
            } else {
                err = true;
            }
            if err {
                super::display::add_to_log(
                    interp,
                    "Invalid face attribute %S %S",
                    &[keyword, value],
                    env,
                )?;
                ok = false;
            }
            plist = next;
        }
    } else {
        // A list of face references: those at the front take
        // precedence, so the merge goes from the end backwards.
        if !rest.is_nil() {
            ok = merge_face_ref(interp, frame, rest, to, err_msgs, points, env)?;
        }
        if !merge_face_ref(interp, frame, first, to, err_msgs, points, env)? {
            ok = false;
        }
    }
    Ok(ok)
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
                return Err(signal_error(
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
                        return Err(signal_error(
                            "Default face height not absolute and positive",
                            value,
                        ));
                    }
                } else {
                    // A test merge with a random height.
                    let test =
                        merge_face_heights(interp, value, Value::Integer(10), Value::Nil, env)?;
                    if !matches!(test.kind(), Kind::Integer(height) if height > 0) {
                        return Err(signal_error(
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
                    ":weight" => (font::FONT_WEIGHT_INDEX, "Invalid face weight"),
                    ":slant" => (font::FONT_SLANT_INDEX, "Invalid face slant"),
                    _ => (font::FONT_WIDTH_INDEX, "Invalid face width"),
                };
                if font::style_name_numeric(interp, style, &value)? < 0 {
                    return Err(signal_error(message, value));
                }
            }
        }
        ":underline" => {
            let valid = special
                || matches!(value.kind(), Kind::Nil | Kind::T)
                || (value.is_string() && !string_text(&value)?.is_empty())
                || (matches!(value.kind(), Kind::Cons(_)) && underline_plist_valid(value)?);
            if !valid {
                return Err(signal_error("Invalid face underline", value));
            }
        }
        ":overline" | ":strike-through" => {
            if !special
                && ((matches!(value.kind(), Kind::Symbol(_)))
                    || (value.is_string() && string_text(&value)?.is_empty()))
            {
                return Err(signal_error(
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
                return Err(signal_error("Invalid face box", value));
            }
        }
        ":inverse-video" | ":reverse-video" | ":extend" => {
            if !special {
                check_symbol(value)?;
                if !matches!(value.kind(), Kind::Nil | Kind::T) {
                    return Err(signal_error(
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
                super::display::add_to_log(
                    interp,
                    "Warning: setting attribute `%s' of face `%s': nil value is invalid, use `unspecified' instead.",
                    &[Value::symbol(attribute), face],
                    env,
                )?;
                value = Value::symbol("unspecified");
            }
            if !special_face_value(&value) && check_string(value)?.is_empty() {
                return Err(signal_error(
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
                return Err(signal_error("Invalid stipple attribute", value));
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
                    return Err(signal_error("Invalid font or font-spec", value));
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
                    return Err(signal_error("Invalid fontset name", value));
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
                return Err(signal_error("Invalid face inheritance", value));
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
            return Err(signal_error(
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
        let old_value = match interp.lisp_face_vector_on(face, frame) {
            Some(lface) => vector_slot_value(&lface, index)?,
            None => Value::symbol("unspecified"),
        };
        interp.set_lisp_face_attribute_on(face, index, value, frame)?;
        frame_parameter_of_face_change(interp, face, attribute, value, old_value, frame, env)?;
    }
    Ok(face)
}

/// Finternal_set_lisp_face_attribute's tail: a changed color of the
/// default face is the frame's `foreground-color' or `background-color'
/// parameter, and in a build with a window system the `scroll-bar'
/// face's colors, the `border', `cursor' and `mouse' faces' backgrounds
/// the scroll-bar, border, cursor and mouse colors; for frame t the
/// parameter goes into `default-frame-alist', else through
/// Fmodify_frame_parameters.
fn frame_parameter_of_face_change(
    interp: &mut Interpreter,
    face: Value,
    attribute: &str,
    value: Value,
    old_value: Value,
    frame: Option<crate::lisp::types::FrameRef>,
    env: &mut Env,
) -> Result<(), LispError> {
    if matches!(value.kind(), Kind::Symbol(symbol) if symbol == "unspecified" || symbol == ":ignore-defface")
        || values_equal(interp, &old_value, &value)?
    {
        return Ok(());
    }
    let param = match (face.symbol_str(), attribute) {
        (Some("default"), ":foreground") => "foreground-color",
        (Some("default"), ":background") => "background-color",
        (Some("scroll-bar"), ":foreground") => "scroll-bar-foreground",
        (Some("scroll-bar"), ":background") => "scroll-bar-background",
        (Some("border"), ":background") => "border-color",
        (Some("cursor"), ":background") => "cursor-color",
        (Some("mouse"), ":background") => "mouse-color",
        _ => return Ok(()),
    };
    let param = Value::symbol(param);
    match frame {
        None => {
            // store_in_alist (&Vdefault_frame_alist, param, value).
            let mut alist = interp
                .lookup_var("default-frame-alist", env)
                .unwrap_or(Value::Nil);
            let before = alist;
            super::frames::store_in_alist(&mut alist, param, value)?;
            if !alist.eq_value(before) {
                interp.set_c_variable("default-frame-alist", alist);
            }
            Ok(())
        }
        Some(frame) => super::frames::modify_frame_parameters(
            interp,
            frame,
            Value::list([Value::cons(param, value)]),
            env,
        ),
    }
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

/// xfaces.c:Fbitmap_spec_p: a string (a file name), or (WIDTH HEIGHT
/// DATA ...) with WIDTH and HEIGHT positive ints and the string DATA
/// holding HEIGHT rows of WIDTH bits.
fn bitmap_spec_p(value: &Value) -> bool {
    if value.is_string() {
        return true;
    }
    let Some((width, rest)) = value.cons_values() else {
        return false;
    };
    let (height, data) = match rest.cons_values() {
        Some((height, rest)) => (
            height,
            rest.cons_values().map_or(Value::Nil, |(data, _)| data),
        ),
        None => (Value::Nil, Value::Nil),
    };
    let positive_int = |value: &Value| matches!(value.kind(), Kind::Integer(n) if (1..=i64::from(i32::MAX)).contains(&n));
    if !data.is_string() || !positive_int(&width) || !positive_int(&height) {
        return false;
    }
    let (Kind::Integer(width), Kind::Integer(height)) = (width.kind(), height.kind()) else {
        return false;
    };
    let bytes_per_row = (width + 7) / 8;
    let sbytes = string_like(&data)
        .and_then(|text| text.byte_len().ok())
        .unwrap_or(0) as i64;
    height <= sbytes / bytes_per_row
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

/// xfaces.c:face_boolean_x_resource_value: VALUE's "on"/"true" t,
/// "off"/"false" nil, "unspecified" `unspecified', compared without
/// case; anything else 0, or an error when SIGNAL.
fn face_boolean_x_resource_value(value: &Value, signal: bool) -> Result<Value, LispError> {
    let text = string_text(value)?;
    Ok(
        if text.eq_ignore_ascii_case("on") || text.eq_ignore_ascii_case("true") {
            Value::T
        } else if text.eq_ignore_ascii_case("off") || text.eq_ignore_ascii_case("false") {
            Value::Nil
        } else if text.eq_ignore_ascii_case("unspecified") {
            Value::symbol("unspecified")
        } else if signal {
            return Err(signal_error(
                "Invalid face attribute value from X resource",
                *value,
            ));
        } else {
            Value::Integer(0)
        },
    )
}

/// Finternal_set_lisp_face_attribute_from_resource's conversion of the
/// resource string VALUE for ATTRIBUTE.
fn resource_face_value(
    interp: &mut Interpreter,
    attribute: &str,
    value: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if string_text(&value)?.eq_ignore_ascii_case("unspecified") {
        return Ok(Value::symbol("unspecified"));
    }
    Ok(match attribute {
        ":height" => {
            let height = super::call(interp, "string-to-number", &[value], env)?;
            if !matches!(height.kind(), Kind::Integer(height) if height > 0) {
                return Err(signal_error("Invalid face height from X resource", height));
            }
            height
        }
        ":bold" | ":italic" | ":reverse-video" | ":inverse-video" | ":extend" => {
            face_boolean_x_resource_value(&value, true)?
        }
        ":weight" | ":slant" | ":width" => Value::symbol(&string_text(&value)?),
        ":underline" | ":overline" | ":strike-through" => {
            // A boolean result says VALUE names no color.
            let boolean = face_boolean_x_resource_value(&value, false)?;
            if matches!(boolean.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) {
                boolean
            } else {
                value
            }
        }
        ":box" | ":inherit" => super::call(interp, "read-from-string", &[value], env)?
            .car_if_list()
            .ok_or_else(|| wrong_type_argument("listp", value))?,
        _ => value,
    })
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
            // xfaces.c: resolve_face_name, then CHECK_LIVE_FRAME of a
            // non-nil FRAME, then the face's Lisp vector (none for a name
            // that is no symbol).
            "internal-lisp-face-p" => {
                need_arg_range(name, args, 1, 2)?;
                let named = matches!(args[0].kind(), Kind::Symbol(_) | Kind::StringObject(_));
                let face = if named {
                    Some(resolve_face_name(interp, &args[0])?)
                } else {
                    None
                };
                let target = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => None,
                    Some(frame) => Some(frames::decode_live_frame(
                        interp,
                        Some(&frame.value()),
                        true,
                    )?),
                };
                let Some(face) = face else {
                    return Ok(Value::Nil);
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
            // xfaces.c: CHECK_SYMBOL FACE and ATTR, CHECK_STRING VALUE,
            // the string converted for ATTR, then
            // Finternal_set_lisp_face_attribute.
            "internal-set-lisp-face-attribute-from-resource" => {
                need_arg_range(name, args, 3, 4)?;
                let face = face_symbol(&args[0])?;
                let attribute = face_symbol(&args[1])?;
                if !matches!(args[2].kind(), Kind::StringObject(_)) {
                    return Err(wrong_type_argument("stringp", args[2]));
                }
                let value = resource_face_value(interp, attribute.as_symbol()?, args[2], env)?;
                let frame = args.get(3).copied().unwrap_or(Value::Nil);
                super::call(
                    interp,
                    "internal-set-lisp-face-attribute",
                    &[face, attribute, value, frame],
                    env,
                )
            }
            // xfaces.c: ATTRIBUTE is only compared with :height.
            "face-attribute-relative-p" => {
                need_args(name, args, 2)?;
                let height = args[0] == Value::symbol(":height");
                Ok(
                    if unspecified_face_value(&args[1])
                        || (height && !matches!(args[1].kind(), Kind::Integer(_)))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "merge-face-attribute" => {
                need_args(name, args, 3)?;
                if unspecified_face_value(&args[1]) {
                    Ok(args[2])
                } else if args[0] == Value::symbol(":height") {
                    merge_face_heights(interp, args[1], args[2], args[1], env)
                } else {
                    Ok(args[1])
                }
            }
            // xfaces.c: FRAME decoded unless t, the face's vector found
            // ("Invalid face"), then CHECK_SYMBOL of SYMBOL and KEYWORD.
            "internal-get-lisp-face-attribute" => {
                need_arg_range(name, args, 2, 3)?;
                let target = if face_target_is_global(args.get(2)) {
                    None
                } else {
                    Some(frames::decode_live_frame(interp, args.get(2), true)?)
                };
                let face = resolve_face_name(interp, &args[0])?;
                let vector = interp
                    .lisp_face_vector_on(face, target)
                    .ok_or_else(|| invalid_face_error(face))?;
                face_symbol(&args[0])?;
                let attribute = face_symbol(&args[1])?;
                // The attribute names its keyword ladder knows: neither
                // :bold nor :italic.
                let index = match attribute.as_symbol()? {
                    ":bold" | ":italic" => None,
                    keyword => face_attribute_index(keyword),
                }
                .ok_or_else(|| face_attribute_error(&attribute))?;
                let value = vector_slot_value(&vector, index)?;
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
            // xfaces.c: FRAME t asks the global definition, any other
            // the live frame's realized face, whose font a frame with no
            // window system lacks.
            "face-font" => {
                need_arg_range(name, args, 1, 3)?;
                if !matches!(args.get(1).map(|v| v.kind()), Some(Kind::T)) {
                    let frame = frames::decode_live_frame(interp, args.get(1), true)?;
                    // lookup_named_face: get_lface_attributes, a face that
                    // `face-remapping-alist' names merged from its remapping
                    // (whose bad references only reach the log), any other
                    // the frame's own definition.
                    let face = resolve_face_name(interp, &args[0])?;
                    let mut tail = interp
                        .lookup_var("face-remapping-alist", env)
                        .unwrap_or(Value::Nil);
                    while let Some((element, rest)) = tail.cons_values() {
                        if element
                            .cons_values()
                            .is_some_and(|(key, _)| key.eq_value(face))
                        {
                            return Ok(Value::Nil);
                        }
                        tail = rest;
                    }
                    interp
                        .lisp_face_vector_on(face, Some(frame))
                        .ok_or_else(|| invalid_face_error(face))?;
                    return Ok(Value::Nil);
                }
                let face = resolve_face_name(interp, &args[0])?;
                {
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
                let left = interp
                    .lisp_face_vector_on(left_name, target)
                    .ok_or_else(|| invalid_face_error(left_name))?;
                let right_name = resolve_face_name(interp, &args[1])?;
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
            // xfaces.c: PLIST merged as a face reference on the selected
            // frame into a vector of unspecified attributes.
            "face-attributes-as-vector" => {
                need_args(name, args, 1)?;
                let mut lface: LfaceAttrs = std::array::from_fn(|_| Value::symbol("unspecified"));
                let frame = interp.selected_frame_id;
                merge_face_ref(
                    interp,
                    frame,
                    args[0],
                    &mut lface,
                    true,
                    &mut Vec::new(),
                    env,
                )?;
                Ok(face_vector(lface))
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
            // xfaces.c:Fx_family_fonts decodes FRAME before FAMILY; a
            // terminal frame's font drivers list no entities.
            "x-family-fonts" => {
                need_arg_range(name, args, 0, 2)?;
                super::frames::decode_live_frame(interp, args.get(1), true)?;
                if let Some(family) = args.first().filter(|value| !value.is_nil()) {
                    string_text(family)?;
                }
                Ok(Value::Nil)
            }
            // xfaces.c:Fx_list_fonts: check_window_system (NULL) first.
            "x-list-fonts" => {
                need_arg_range(name, args, 1, 5)?;
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
