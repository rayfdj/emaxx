use super::*;
use crate::lisp::eval::{FontPatternState, FontsetMappingState, FontsetState, FontsetTargetState};
use crate::lisp::primitives::buffers::position_from_value;
use crate::lisp::primitives::color_lcms::parse_font_name;
use crate::lisp::primitives::window::{window_buffer_id, window_record_id_from_value};

const FONT_TYPE_INDEX: usize = 0;
const FONT_FOUNDRY_INDEX: usize = 1;
const FONT_FAMILY_INDEX: usize = 2;
const FONT_ADSTYLE_INDEX: usize = 3;
const FONT_REGISTRY_INDEX: usize = 4;
const FONT_WEIGHT_INDEX: usize = 5;
const FONT_SLANT_INDEX: usize = 6;
const FONT_WIDTH_INDEX: usize = 7;
const FONT_SIZE_INDEX: usize = 8;
const FONT_DPI_INDEX: usize = 9;
const FONT_SPACING_INDEX: usize = 10;
const FONT_AVGWIDTH_INDEX: usize = 11;
const FONT_EXTRA_INDEX: usize = 12;
const FONT_SPEC_SIZE: usize = FONT_EXTRA_INDEX + 1;

fn font_property_index(key: &str) -> Option<usize> {
    Some(match key {
        ":type" => FONT_TYPE_INDEX,
        ":foundry" => FONT_FOUNDRY_INDEX,
        ":family" => FONT_FAMILY_INDEX,
        ":adstyle" => FONT_ADSTYLE_INDEX,
        ":registry" => FONT_REGISTRY_INDEX,
        ":weight" => FONT_WEIGHT_INDEX,
        ":slant" => FONT_SLANT_INDEX,
        ":width" => FONT_WIDTH_INDEX,
        ":size" => FONT_SIZE_INDEX,
        ":dpi" => FONT_DPI_INDEX,
        ":spacing" => FONT_SPACING_INDEX,
        ":avgwidth" => FONT_AVGWIDTH_INDEX,
        _ => return None,
    })
}

fn font_record<'a>(
    interp: &'a Interpreter,
    value: &Value,
) -> Result<&'a crate::lisp::eval::RecordState, LispError> {
    let Value::Record(id) = value else {
        return Err(wrong_type_argument("fontp", value.clone()));
    };
    interp
        .find_record(*id)
        .filter(|record| record.kind == crate::lisp::eval::RecordKind::Font)
        .ok_or_else(|| wrong_type_argument("fontp", value.clone()))
}

fn font_spec_id(interp: &Interpreter, value: &Value) -> Result<u64, LispError> {
    let Value::Record(id) = value else {
        return Err(wrong_type_argument("font-spec-p", value.clone()));
    };
    interp
        .find_record(*id)
        .filter(|record| {
            record.kind == crate::lisp::eval::RecordKind::Font
                && record.has_symbol_type("font-spec")
        })
        .map(|_| *id)
        .ok_or_else(|| wrong_type_argument("font-spec-p", value.clone()))
}

fn require_font_type(
    interp: &Interpreter,
    value: &Value,
    record_type: &str,
    predicate: &str,
) -> Result<(), LispError> {
    if font_record(interp, value).is_ok_and(|font| font.has_symbol_type(record_type)) {
        Ok(())
    } else {
        Err(wrong_type_argument(predicate, value.clone()))
    }
}

fn require_font(interp: &Interpreter, value: &Value) -> Result<(), LispError> {
    font_record(interp, value)
        .map(|_| ())
        .map_err(|_| wrong_type_argument("font", value.clone()))
}

fn require_character(value: &Value) -> Result<(), LispError> {
    match value {
        Value::Integer(character) if (0..=0x3f_ffff).contains(character) => Ok(()),
        _ => Err(wrong_type_argument("characterp", value.clone())),
    }
}

fn require_live_frame(interp: &Interpreter, frame: Option<&Value>) -> Result<(), LispError> {
    match frame {
        None | Some(Value::Nil) => Ok(()),
        Some(Value::Frame(id)) if interp.frame_is_live(*id) => Ok(()),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.clone())),
    }
}

fn require_frame(frame: Option<&Value>) -> Result<(), LispError> {
    match frame {
        None | Some(Value::Nil | Value::Frame(_)) => Ok(()),
        Some(frame) => Err(wrong_type_argument("framep", frame.clone())),
    }
}

fn window_system_frame_required() -> LispError {
    LispError::Signal("Window system frame should be used".into())
}

fn invalid_glyph_string(gstring: &Value) -> LispError {
    let mut data = vec![
        Value::symbol("error"),
        Value::string("Invalid glyph-string: "),
    ];
    if !gstring.is_nil() {
        data.push(gstring.clone());
    }
    LispError::SignalValue(Value::list(data))
}

fn composition_gstring_parts(interp: &Interpreter, gstring: &Value) -> Option<(Value, Value)> {
    if !is_vector_value(gstring) {
        return None;
    }
    let body = vector_items(gstring).ok()?;
    if body.len() < 2 || !is_vector_value(body.first()?) {
        return None;
    }
    let header = vector_items(&body[0]).ok()?;
    if header.len() < 2 {
        return None;
    }
    let font = header[0].clone();
    if !font.is_nil()
        && !matches!(&font, Value::Symbol(name) if interp.has_coding_system(name))
        && !font_record(interp, &font).is_ok_and(|record| record.has_symbol_type("font-object"))
    {
        return None;
    }
    if header
        .iter()
        .skip(1)
        .any(|value| !matches!(value, Value::Integer(number) if *number >= 0))
        || (!body[1].is_nil() && !matches!(&body[1], Value::Integer(number) if *number >= 0))
    {
        return None;
    }
    for glyph in body.iter().skip(2).take_while(|glyph| !glyph.is_nil()) {
        if !is_vector_value(glyph) || !vector_items(glyph).is_ok_and(|items| items.len() == 10) {
            return None;
        }
    }
    Some((font, body[1].clone()))
}

fn invalid_font_property() -> LispError {
    LispError::Signal("invalid font property".into())
}

fn args_out_of_range(object: Value, position: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("args-out-of-range"),
        object,
        position,
    ]))
}

fn symbol_property(value: &Value, lowercase: bool) -> Result<Value, LispError> {
    let name = match value {
        Value::String(_) | Value::StringObject(_) => string_text(value)?,
        Value::Nil => return Ok(Value::Nil),
        Value::T => "t".into(),
        Value::Symbol(name) => name.to_string(),
        _ => return Err(invalid_font_property()),
    };
    let name = if lowercase { name.to_lowercase() } else { name };
    Ok(Value::symbol(&name))
}

fn valid_style_name(index: usize, name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    match index {
        FONT_WEIGHT_INDEX => matches!(
            name.as_str(),
            "thin"
                | "ultra-light"
                | "ultralight"
                | "extra-light"
                | "extralight"
                | "light"
                | "semi-light"
                | "semilight"
                | "demilight"
                | "regular"
                | "normal"
                | "unspecified"
                | "book"
                | "medium"
                | "semi-bold"
                | "semibold"
                | "demibold"
                | "demi-bold"
                | "demi"
                | "bold"
                | "extra-bold"
                | "extrabold"
                | "ultra-bold"
                | "ultrabold"
                | "black"
                | "heavy"
                | "ultra-heavy"
                | "ultraheavy"
        ),
        FONT_SLANT_INDEX => matches!(
            name.as_str(),
            "reverse-oblique"
                | "ro"
                | "reverse-italic"
                | "ri"
                | "normal"
                | "r"
                | "unspecified"
                | "italic"
                | "i"
                | "ot"
                | "oblique"
                | "o"
        ),
        FONT_WIDTH_INDEX => matches!(
            name.as_str(),
            "ultra-condensed"
                | "ultracondensed"
                | "extra-condensed"
                | "extracondensed"
                | "condensed"
                | "compressed"
                | "narrow"
                | "semi-condensed"
                | "semicondensed"
                | "demicondensed"
                | "normal"
                | "medium"
                | "regular"
                | "unspecified"
                | "semi-expanded"
                | "semiexpanded"
                | "demiexpanded"
                | "expanded"
                | "extra-expanded"
                | "extraexpanded"
                | "ultra-expanded"
                | "ultraexpanded"
                | "wide"
        ),
        _ => false,
    }
}

fn style_property(index: usize, value: &Value) -> Result<Value, LispError> {
    let name = value.as_symbol().map_err(|_| invalid_font_property())?;
    if valid_style_name(index, name) {
        Ok(Value::symbol(name))
    } else {
        Err(invalid_font_property())
    }
}

fn nonnegative_property(value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Integer(number) if *number >= 0 => Ok(value.clone()),
        Value::Float(number) if *number >= 0.0 => Ok(value.clone()),
        _ => Err(invalid_font_property()),
    }
}

fn spacing_property(value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Nil => Ok(Value::Nil),
        Value::Integer(number) if (0..=110).contains(number) => Ok(value.clone()),
        Value::Symbol(symbol) if symbol.len() == 1 => match symbol.as_bytes()[0] {
            b'p' | b'P' => Ok(Value::Integer(0)),
            b'd' | b'D' => Ok(Value::Integer(90)),
            b'm' | b'M' => Ok(Value::Integer(100)),
            b'c' | b'C' => Ok(Value::Integer(110)),
            _ => Err(invalid_font_property()),
        },
        _ => Err(invalid_font_property()),
    }
}

fn otf_property(value: &Value) -> Result<Value, LispError> {
    let values = value.to_vec().map_err(|_| invalid_font_property())?;
    if values.is_empty() || values.len() > 4 || values[0].as_symbol().is_err() {
        return Err(invalid_font_property());
    }
    if let Some(language) = values.get(1)
        && language.as_symbol().is_err()
    {
        return Err(invalid_font_property());
    }
    for features in values.iter().skip(2) {
        let features = features.to_vec().map_err(|_| invalid_font_property())?;
        if features.iter().any(|feature| feature.as_symbol().is_err()) {
            return Err(invalid_font_property());
        }
    }
    Ok(value.clone())
}

fn validated_font_property(index: usize, value: &Value) -> Result<Value, LispError> {
    match index {
        FONT_TYPE_INDEX | FONT_FOUNDRY_INDEX | FONT_FAMILY_INDEX | FONT_ADSTYLE_INDEX => {
            symbol_property(value, false)
        }
        FONT_REGISTRY_INDEX => symbol_property(value, true),
        FONT_WEIGHT_INDEX | FONT_SLANT_INDEX | FONT_WIDTH_INDEX => style_property(index, value),
        FONT_SIZE_INDEX | FONT_DPI_INDEX | FONT_AVGWIDTH_INDEX => nonnegative_property(value),
        FONT_SPACING_INDEX => spacing_property(value),
        _ => unreachable!("all fixed font properties have validators"),
    }
}

fn extra_entries(value: &Value) -> Vec<(Value, Value)> {
    value
        .to_vec()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| entry.cons_values())
        .collect()
}

fn extra_value(extra: &Value, key: &str) -> Option<Value> {
    extra_entries(extra)
        .into_iter()
        .find_map(|(entry_key, value)| {
            matches!(entry_key, Value::Symbol(symbol) if symbol == key).then_some(value)
        })
}

fn put_extra(extra: &mut Value, key: Value, value: Value) {
    let key_name = key.as_symbol().ok().map(str::to_string);
    let mut entries = extra_entries(extra);
    if let Some((_, existing)) = entries.iter_mut().find(|(entry_key, _)| {
        key_name
            .as_ref()
            .is_some_and(|key| matches!(entry_key, Value::Symbol(symbol) if symbol == key))
    }) {
        *existing = value;
    } else {
        entries.insert(0, (key, value));
    }
    *extra = Value::list(
        entries
            .into_iter()
            .map(|(entry_key, entry_value)| Value::cons(entry_key, entry_value)),
    );
}

fn set_name_properties(slots: &mut [Value], name: &str) -> Result<(), LispError> {
    if name.is_empty() {
        return Err(LispError::Signal("Invalid font name: ".into()));
    }
    let info = parse_font_name(name);
    if let Some(foundry) = info.foundry {
        slots[FONT_FOUNDRY_INDEX] = Value::symbol(&foundry);
    }
    if let Some(family) = info.family {
        slots[FONT_FAMILY_INDEX] = Value::symbol(&family);
    }
    if let Some(weight) = info.weight {
        slots[FONT_WEIGHT_INDEX] = Value::symbol(&weight);
    }
    if let Some(slant) = info.slant {
        slots[FONT_SLANT_INDEX] = Value::symbol(&slant);
    }
    if let Some(size) = info.size {
        slots[FONT_SIZE_INDEX] = Value::Float(size);
    }
    if let Some(spacing) = info.spacing {
        slots[FONT_SPACING_INDEX] = Value::Integer(spacing);
    }
    Ok(())
}

fn make_font_spec(args: &[Value]) -> Result<Vec<Value>, LispError> {
    let mut slots = vec![Value::Nil; FONT_SPEC_SIZE];
    let mut index = 0;
    while index < args.len() {
        let key = args[index]
            .as_symbol()
            .map_err(|_| wrong_type_argument("symbolp", args[index].clone()))?;
        let Some(value) = args.get(index + 1) else {
            return Err(LispError::Signal(format!("No value for key `{key}'")));
        };
        if key == ":name" {
            let name = string_text(value)?;
            set_name_properties(&mut slots, &name)?;
            put_extra(
                &mut slots[FONT_EXTRA_INDEX],
                args[index].clone(),
                value.clone(),
            );
        } else if let Some(property_index) = font_property_index(key) {
            slots[property_index] = validated_font_property(property_index, value)?;
        } else {
            let value = match key {
                ":lang" | ":script" => symbol_property(value, false)?,
                ":otf" => otf_property(value)?,
                _ => value.clone(),
            };
            put_extra(&mut slots[FONT_EXTRA_INDEX], args[index].clone(), value);
        }
        index += 2;
    }
    Ok(slots)
}

fn font_property(interp: &Interpreter, font: &Value, key: &str) -> Result<Value, LispError> {
    let record = font_record(interp, font)?;
    if let Some(index) = font_property_index(key) {
        return Ok(record.slots.get(index).cloned().unwrap_or(Value::Nil));
    }
    Ok(record
        .slots
        .get(FONT_EXTRA_INDEX)
        .and_then(|extra| extra_value(extra, key))
        .unwrap_or(Value::Nil))
}

fn put_font_property(
    interp: &mut Interpreter,
    font: &Value,
    key: &str,
    key_value: Value,
    value: &Value,
) -> Result<Value, LispError> {
    let id = if font_property_index(key).is_some()
        || matches!(key, ":name" | ":script" | ":lang" | ":otf")
    {
        font_spec_id(interp, font)?
    } else {
        let Value::Record(id) = font else {
            return Err(wrong_type_argument("fontp", font.clone()));
        };
        font_record(interp, font)?;
        *id
    };

    if key == ":name" {
        let name = string_text(value)?;
        let mut replacement = interp
            .find_record(id)
            .map(|record| record.slots.clone())
            .ok_or_else(|| wrong_type_argument("font-spec-p", font.clone()))?;
        set_name_properties(&mut replacement, &name)?;
        put_extra(&mut replacement[FONT_EXTRA_INDEX], key_value, value.clone());
        interp
            .find_record_mut(id)
            .expect("validated font-spec record must stay live")
            .slots = replacement;
        return Ok(value.clone());
    }

    if let Some(index) = font_property_index(key) {
        let normalized = validated_font_property(index, value)?;
        let record = interp
            .find_record_mut(id)
            .expect("validated font-spec record must stay live");
        if record.slots.len() < FONT_SPEC_SIZE {
            record.slots.resize(FONT_SPEC_SIZE, Value::Nil);
        }
        record.slots[index] = normalized;
        return Ok(value.clone());
    }

    let normalized = match key {
        ":lang" | ":script" => symbol_property(value, false)?,
        ":otf" => otf_property(value)?,
        _ => value.clone(),
    };
    let record = interp
        .find_record_mut(id)
        .expect("validated font record must stay live");
    if record.slots.len() < FONT_SPEC_SIZE {
        record.slots.resize(FONT_SPEC_SIZE, Value::Nil);
    }
    put_extra(&mut record.slots[FONT_EXTRA_INDEX], key_value, normalized);
    Ok(value.clone())
}

fn xlfd_field(value: Option<&Value>) -> String {
    match value {
        Some(Value::Symbol(symbol)) if !symbol.is_empty() => symbol.to_string(),
        Some(Value::T) => "t".into(),
        _ => "*".into(),
    }
}

fn number_field(value: Option<&Value>) -> String {
    match value {
        Some(Value::Integer(number)) => number.to_string(),
        Some(Value::Float(number)) if number.fract() == 0.0 => {
            format!("{number:.0}")
        }
        Some(Value::Float(number)) => number.to_string(),
        _ => "*".into(),
    }
}

fn font_xlfd_name(
    interp: &Interpreter,
    font: &Value,
    fold_wildcards: bool,
    allow_long: bool,
) -> Result<Value, LispError> {
    let record = font_record(interp, font)?;
    let slot = |index| record.slots.get(index);
    let (pixel_size, point_size) = match slot(FONT_SIZE_INDEX) {
        Some(Value::Integer(size)) => (size.to_string(), "*".into()),
        Some(Value::Float(size)) => {
            let tenths = size * 10.0;
            let point = if tenths.fract() == 0.0 {
                format!("{tenths:.0}")
            } else {
                tenths.to_string()
            };
            ("*".into(), point)
        }
        _ => ("*".into(), "*".into()),
    };
    let dpi = match slot(FONT_DPI_INDEX) {
        Some(Value::Integer(_)) => number_field(slot(FONT_DPI_INDEX)),
        _ => "*".into(),
    };
    let spacing = match slot(FONT_SPACING_INDEX) {
        Some(Value::Integer(0)) => "p",
        Some(Value::Integer(90)) => "d",
        Some(Value::Integer(100)) => "m",
        Some(Value::Integer(110)) => "c",
        _ => "*",
    };
    let average_width = match slot(FONT_AVGWIDTH_INDEX) {
        Some(Value::Integer(_)) => number_field(slot(FONT_AVGWIDTH_INDEX)),
        _ => "*".into(),
    };
    let registry = xlfd_field(slot(FONT_REGISTRY_INDEX));
    let (registry, encoding) = registry
        .rsplit_once('-')
        .map(|(registry, encoding)| (registry.to_string(), encoding.to_string()))
        .unwrap_or((registry, "*".into()));
    let mut name = format!(
        "-{}-{}-{}-{}-{}-{}-{pixel_size}-{point_size}-{dpi}-{dpi}-{spacing}-{average_width}-{registry}-{encoding}",
        xlfd_field(slot(FONT_FOUNDRY_INDEX)),
        xlfd_field(slot(FONT_FAMILY_INDEX)),
        xlfd_field(slot(FONT_WEIGHT_INDEX)),
        xlfd_field(slot(FONT_SLANT_INDEX)),
        xlfd_field(slot(FONT_WIDTH_INDEX)),
        xlfd_field(slot(FONT_ADSTYLE_INDEX)),
    );
    if fold_wildcards {
        while let Some(index) = name.find("-*-*") {
            name.replace_range(index..index + 4, "-*");
        }
    }
    if !allow_long && name.len() > 255 {
        Ok(Value::Nil)
    } else {
        Ok(Value::string(&name))
    }
}

fn font_match_p(interp: &Interpreter, spec: &Value, font: &Value) -> Result<bool, LispError> {
    let spec_id = font_spec_id(interp, spec)?;
    let font = font_record(interp, font)?;
    let spec = interp
        .find_record(spec_id)
        .expect("validated font-spec record must stay live");
    for index in FONT_FOUNDRY_INDEX..=FONT_AVGWIDTH_INDEX {
        let expected = spec.slots.get(index).cloned().unwrap_or(Value::Nil);
        if expected.is_nil() {
            continue;
        }
        let actual = font.slots.get(index).cloned().unwrap_or(Value::Nil);
        if !actual.is_nil() && !values_equal(interp, &expected, &actual) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn fontset_index(interp: &Interpreter, name: &str) -> Option<usize> {
    interp
        .fontset_states
        .iter()
        .position(|fontset| fontset.name.eq_ignore_ascii_case(name))
}

fn resolve_fontset(interp: &Interpreter, value: &Value) -> Result<usize, LispError> {
    match value {
        Value::Nil | Value::T => Ok(0),
        Value::String(_) | Value::StringObject(_) => {
            let name = string_text(value)?;
            fontset_index(interp, &name)
                .ok_or_else(|| LispError::Signal(format!("Fontset {name} does not exist")))
        }
        _ => Err(wrong_type_argument("stringp", value.clone())),
    }
}

fn font_pattern_component(value: &Value) -> Result<Option<String>, LispError> {
    match value {
        Value::Nil => Ok(None),
        Value::Symbol(symbol) => Ok(Some(symbol.to_string())),
        Value::String(_) | Value::StringObject(_) => Ok(Some(string_text(value)?)),
        _ => Err(invalid_font_property()),
    }
}

fn font_pattern_from_slots(slots: &[Value]) -> FontPatternState {
    let component = |index| match slots.get(index) {
        Some(Value::Symbol(symbol)) => Some(symbol.to_string()),
        _ => None,
    };
    FontPatternState {
        family: component(FONT_FAMILY_INDEX),
        registry: component(FONT_REGISTRY_INDEX),
    }
}

fn font_pattern(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<FontPatternState>, LispError> {
    match value {
        Value::Nil => Ok(None),
        Value::Record(_) => {
            font_spec_id(interp, value)?;
            let record = font_record(interp, value)?;
            Ok(Some(font_pattern_from_slots(&record.slots)))
        }
        Value::String(_) | Value::StringObject(_) => {
            let slots = make_font_spec(&[Value::symbol(":name"), value.clone()])?;
            Ok(Some(font_pattern_from_slots(&slots)))
        }
        Value::Cons(_) => {
            let (family, registry) = value
                .cons_values()
                .expect("a cons font pattern must have two cells");
            Ok(Some(FontPatternState {
                family: font_pattern_component(&family)?,
                registry: font_pattern_component(&registry)?
                    .map(|registry| registry.to_lowercase()),
            }))
        }
        _ => Err(LispError::SignalValue(Value::list([
            Value::symbol("font"),
            Value::string("Invalid font-spec"),
            value.clone(),
        ]))),
    }
}

fn parse_fontset_target(value: &Value) -> Result<FontsetTargetState, LispError> {
    match value {
        Value::Nil => Ok(FontsetTargetState::Fallback),
        Value::Integer(character) if (0..=0x3f_ffff).contains(character) => {
            Ok(FontsetTargetState::Character(*character))
        }
        Value::Symbol(script) => Ok(FontsetTargetState::Script(script.to_string())),
        Value::Cons(_) => {
            let (from, to) = value
                .cons_values()
                .expect("a range cons must have endpoints");
            let (Value::Integer(from), Value::Integer(to)) = (from, to) else {
                return Err(wrong_type_argument("characterp", value.clone()));
            };
            if from < 0 || to < from || to > 0x3f_ffff {
                return Err(args_out_of_range(value.clone(), Value::Integer(to)));
            }
            Ok(FontsetTargetState::Range(from, to))
        }
        _ => Err(wrong_type_argument("characterp", value.clone())),
    }
}

fn target_changes_partial_ascii(target: &FontsetTargetState) -> bool {
    match target {
        FontsetTargetState::Character(character) => *character < 0x80,
        FontsetTargetState::Range(from, to) => *from < 0x80 && !(*from == 0 && *to >= 0x7f),
        FontsetTargetState::Script(_) | FontsetTargetState::Fallback => false,
    }
}

fn set_fontset_mapping(
    interp: &mut Interpreter,
    fontset_index: usize,
    target: FontsetTargetState,
    pattern: Option<FontPatternState>,
    add: Option<&str>,
) {
    let mappings = &mut interp.fontset_states[fontset_index].mappings;
    if let Some(mapping) = mappings.iter_mut().find(|mapping| mapping.target == target) {
        match add {
            Some("prepend") => mapping.patterns.insert(0, pattern),
            Some("append") => mapping.patterns.push(pattern),
            _ => mapping.patterns = vec![pattern],
        }
    } else {
        mappings.push(FontsetMappingState {
            target,
            patterns: vec![pattern],
        });
    }
}

fn script_contains(script: &str, character: i64) -> bool {
    match script {
        "ascii" => (0..=0x7f).contains(&character),
        "latin" => (0..=0x024f).contains(&character) || (0x1e00..=0x1eff).contains(&character),
        "greek" => (0x0370..=0x03ff).contains(&character),
        "cyrillic" => (0x0400..=0x052f).contains(&character),
        "hebrew" => (0x0590..=0x05ff).contains(&character),
        "arabic" => (0x0600..=0x06ff).contains(&character),
        "han" => (0x3400..=0x4dbf).contains(&character) || (0x4e00..=0x9fff).contains(&character),
        _ => false,
    }
}

fn target_matches(target: &FontsetTargetState, character: i64) -> bool {
    match target {
        FontsetTargetState::Character(target) => *target == character,
        FontsetTargetState::Range(from, to) => (*from..=*to).contains(&character),
        FontsetTargetState::Script(script) => script_contains(script, character),
        FontsetTargetState::Fallback => false,
    }
}

fn font_pattern_value(pattern: &FontPatternState) -> Value {
    Value::cons(
        pattern
            .family
            .as_deref()
            .map(Value::string)
            .unwrap_or(Value::Nil),
        pattern
            .registry
            .as_deref()
            .map(Value::string)
            .unwrap_or(Value::Nil),
    )
}

enum FontsetLookup {
    Continue,
    Return(Value),
}

fn collect_fontset_patterns(
    patterns: &[Option<FontPatternState>],
    all: bool,
    result: &mut Vec<Value>,
) -> FontsetLookup {
    for pattern in patterns {
        let Some(pattern) = pattern else {
            return FontsetLookup::Return(Value::Nil);
        };
        let value = font_pattern_value(pattern);
        if !all {
            return FontsetLookup::Return(value);
        }
        result.push(value);
    }
    FontsetLookup::Continue
}

fn lookup_fontset_patterns(
    fontset: &FontsetState,
    character: i64,
    all: bool,
    result: &mut Vec<Value>,
) -> FontsetLookup {
    if let Some(mapping) = fontset
        .mappings
        .iter()
        .rev()
        .find(|mapping| target_matches(&mapping.target, character))
        && let FontsetLookup::Return(value) =
            collect_fontset_patterns(&mapping.patterns, all, result)
    {
        return FontsetLookup::Return(value);
    }
    if let Some(mapping) = fontset
        .mappings
        .iter()
        .rev()
        .find(|mapping| mapping.target == FontsetTargetState::Fallback)
        && let FontsetLookup::Return(value) =
            collect_fontset_patterns(&mapping.patterns, all, result)
    {
        return FontsetLookup::Return(value);
    }
    FontsetLookup::Continue
}

fn fontset_font(interp: &Interpreter, fontset_index: usize, character: i64, all: bool) -> Value {
    let mut result = Vec::new();
    if let FontsetLookup::Return(value) = lookup_fontset_patterns(
        &interp.fontset_states[fontset_index],
        character,
        all,
        &mut result,
    ) {
        return value;
    }
    if fontset_index != 0
        && let FontsetLookup::Return(value) =
            lookup_fontset_patterns(&interp.fontset_states[0], character, all, &mut result)
    {
        return value;
    }
    if all { Value::list(result) } else { Value::Nil }
}

fn valid_fontset_name(name: &str) -> bool {
    name.starts_with('-') && name.to_ascii_lowercase().contains("-fontset-")
}

fn add_fontlist(
    interp: &mut Interpreter,
    fontset_index: usize,
    fontlist: &Value,
) -> Result<(), LispError> {
    for entry in fontlist.to_vec()? {
        let (script, definitions) = entry
            .cons_values()
            .ok_or_else(|| wrong_type_argument("listp", entry.clone()))?;
        let script = script
            .as_symbol()
            .map_err(|_| wrong_type_argument("symbolp", script.clone()))?
            .to_string();
        let definitions = definitions
            .to_vec()
            .unwrap_or_else(|_| vec![definitions.clone()]);
        for definition in definitions {
            let pattern = font_pattern(interp, &definition)?;
            set_fontset_mapping(
                interp,
                fontset_index,
                FontsetTargetState::Script(script.clone()),
                pattern,
                Some("append"),
            );
        }
    }
    Ok(())
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "font-spec" => Ok(interp.create_pseudovector(
                crate::lisp::eval::RecordKind::Font,
                "font-spec",
                make_font_spec(args)?,
            )),
            "fontp" => {
                need_arg_range(name, args, 1, 2)?;
                let font_type = match &args[0] {
                    Value::Record(id) => interp.find_record(*id).and_then(|record| {
                        (record.kind == crate::lisp::eval::RecordKind::Font)
                            .then(|| record.symbol_type_name().map(str::to_owned))
                            .flatten()
                    }),
                    _ => None,
                };
                let matches = match args.get(1) {
                    None | Some(Value::Nil) => font_type.is_some(),
                    Some(Value::Symbol(expected))
                        if matches!(
                            expected.as_str(),
                            "font-spec" | "font-entity" | "font-object"
                        ) =>
                    {
                        font_type.as_deref() == Some(expected)
                    }
                    Some(extra_type) => {
                        return Err(wrong_type_argument("font-extra-type", extra_type.clone()));
                    }
                };
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "font-get" => {
                need_args(name, args, 2)?;
                let key = args[1]
                    .as_symbol()
                    .map_err(|_| wrong_type_argument("symbolp", args[1].clone()))?;
                font_property(interp, &args[0], key)
            }
            "font-put" => {
                need_args(name, args, 3)?;
                let key = args[1]
                    .as_symbol()
                    .map_err(|_| wrong_type_argument("symbolp", args[1].clone()))?
                    .to_string();
                put_font_property(interp, &args[0], &key, args[1].clone(), &args[2])
            }
            "font-match-p" => {
                need_args(name, args, 2)?;
                Ok(if font_match_p(interp, &args[0], &args[1])? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "font-xlfd-name" => {
                need_arg_range(name, args, 1, 3)?;
                font_xlfd_name(
                    interp,
                    &args[0],
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(2).is_some_and(Value::is_truthy),
                )
            }
            "list-fonts" => {
                need_arg_range(name, args, 1, 4)?;
                font_spec_id(interp, &args[0])?;
                if let Some(limit) = args.get(2).filter(|value| !value.is_nil()) {
                    let Value::Integer(limit) = limit else {
                        return Err(wrong_type_argument("fixnump", limit.clone()));
                    };
                    if *limit <= 0 {
                        return Ok(Value::Nil);
                    }
                }
                if let Some(prefer) = args.get(3).filter(|value| !value.is_nil()) {
                    font_spec_id(interp, prefer)?;
                }
                Ok(Value::Nil)
            }
            "find-font" => {
                need_arg_range(name, args, 1, 2)?;
                font_spec_id(interp, &args[0])?;
                Ok(Value::Nil)
            }
            "font-at" => {
                need_arg_range(name, args, 1, 3)?;
                let window = args.get(1).filter(|value| !value.is_nil());
                let window = match window {
                    Some(window) => {
                        window_record_id_from_value(interp, window)
                            .ok_or_else(|| wrong_type_argument("window-live-p", window.clone()))?;
                        window.clone()
                    }
                    None => Value::Record(interp.selected_window_id()),
                };
                if let Some(string) = args.get(2).filter(|value| !value.is_nil()) {
                    let Value::Integer(position) = args[0] else {
                        return Err(wrong_type_argument("fixnump", args[0].clone()));
                    };
                    let string_value = string.clone();
                    let string = string_like(&string_value)
                        .ok_or_else(|| wrong_type_argument("stringp", string.clone()))?;
                    let length = string.text.chars().count() as i64;
                    if !(0..length).contains(&position) {
                        return Err(args_out_of_range(string_value.clone(), args[0].clone()));
                    }
                } else {
                    if window_buffer_id(interp, &window) != Some(interp.current_buffer_id()) {
                        return Err(LispError::Signal(
                            "Specified window is not displaying the current buffer".into(),
                        ));
                    }
                    let position = position_from_value(interp, &args[0])?;
                    let (begin, end) = interp.buffer.restriction();
                    if !(begin..end).contains(&position) {
                        return Err(args_out_of_range(args[0].clone(), args[0].clone()));
                    }
                }
                Ok(Value::Nil)
            }
            "font-face-attributes" => {
                need_arg_range(name, args, 1, 2)?;
                require_live_frame(interp, args.get(1))?;
                Err(window_system_frame_required())
            }
            "open-font" => {
                need_arg_range(name, args, 1, 3)?;
                require_live_frame(interp, args.get(2))?;
                Err(window_system_frame_required())
            }
            "close-font" => {
                need_arg_range(name, args, 1, 2)?;
                require_font_type(interp, &args[0], "font-object", "font-object")?;
                require_live_frame(interp, args.get(1))?;
                Err(window_system_frame_required())
            }
            "query-font" => {
                need_args(name, args, 1)?;
                require_font_type(interp, &args[0], "font-object", "font-object")?;
                Err(window_system_frame_required())
            }
            "font-has-char-p" => {
                need_arg_range(name, args, 2, 3)?;
                require_font(interp, &args[0])?;
                require_character(&args[1])?;
                require_frame(args.get(2))?;
                Err(window_system_frame_required())
            }
            "font-get-glyphs" => {
                need_arg_range(name, args, 3, 4)?;
                require_font_type(interp, &args[0], "font-object", "font-object")?;
                Err(window_system_frame_required())
            }
            "font-variation-glyphs" => {
                need_args(name, args, 2)?;
                require_font_type(interp, &args[0], "font-object", "font-object")?;
                require_character(&args[1])?;
                Err(window_system_frame_required())
            }
            "font-shape-gstring" => {
                need_args(name, args, 2)?;
                let (font, id) = composition_gstring_parts(interp, &args[0])
                    .ok_or_else(|| invalid_glyph_string(&args[0]))?;
                if !id.is_nil() {
                    return Ok(args[0].clone());
                }
                require_font_type(interp, &font, "font-object", "font-object")?;
                Err(window_system_frame_required())
            }
            "font-info" => {
                need_arg_range(name, args, 1, 2)?;
                Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ))
            }
            "font-family-list" | "frame-font-cache" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "clear-font-cache" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "query-fontset" | "fontset-info" => {
                need_arg_range(name, args, 1, 2)?;
                Err(LispError::Signal(
                    "Window system is not in use or not initialized".into(),
                ))
            }
            "fontset-list" => {
                need_args(name, args, 0)?;
                Ok(Value::list(
                    interp
                        .fontset_states
                        .iter()
                        .rev()
                        .map(|fontset| Value::string(&fontset.name)),
                ))
            }
            "new-fontset" => {
                need_args(name, args, 2)?;
                let fontset_name = string_text(&args[0])?.to_lowercase();
                if !valid_fontset_name(&fontset_name) {
                    return Err(LispError::Signal(
                        "Fontset name must be in XLFD format".into(),
                    ));
                }
                let fontset_index = match fontset_index(interp, &fontset_name) {
                    Some(index) => {
                        interp.fontset_states[index]
                            .mappings
                            .retain(|mapping| mapping.target == FontsetTargetState::Fallback);
                        index
                    }
                    None => {
                        interp.fontset_states.push(FontsetState {
                            name: fontset_name.clone(),
                            mappings: Vec::new(),
                        });
                        interp.fontset_states.len() - 1
                    }
                };
                add_fontlist(interp, fontset_index, &args[1])?;
                Ok(Value::string(&fontset_name))
            }
            "set-fontset-font" => {
                need_arg_range(name, args, 3, 5)?;
                let fontset_index = resolve_fontset(interp, &args[0])?;
                let target = parse_fontset_target(&args[1])?;
                if target_changes_partial_ascii(&target) {
                    return Err(LispError::Signal(
                        "Can't set a font for partial ASCII range".into(),
                    ));
                }
                let pattern = font_pattern(interp, &args[2])?;
                let add = match args.get(4).filter(|value| !value.is_nil()) {
                    Some(Value::Symbol(add)) if matches!(add.as_str(), "prepend" | "append") => {
                        Some(add.as_str())
                    }
                    Some(value) => {
                        return Err(LispError::Signal(format!("Invalid ADD argument: {value}")));
                    }
                    None => None,
                };
                set_fontset_mapping(interp, fontset_index, target, pattern, add);
                Ok(Value::Nil)
            }
            "fontset-font" => {
                need_arg_range(name, args, 2, 3)?;
                let fontset_index = resolve_fontset(interp, &args[0])?;
                let Value::Integer(character) = args[1] else {
                    return Err(wrong_type_argument("characterp", args[1].clone()));
                };
                if !(0..=0x3f_ffff).contains(&character) {
                    return Err(wrong_type_argument("characterp", args[1].clone()));
                }
                Ok(fontset_font(
                    interp,
                    fontset_index,
                    character,
                    args.get(2).is_some_and(Value::is_truthy),
                ))
            }
        }
    }
);
