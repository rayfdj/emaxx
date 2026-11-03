use super::*;
use crate::lisp::eval::{FontPatternState, FontsetMappingState, FontsetState, FontsetTargetState};
use crate::lisp::primitives::buffers::position_from_value;
use crate::lisp::primitives::font;
use crate::lisp::primitives::window::{window_buffer_id, window_record_id_from_value};
use crate::lisp::types::Kind;

fn require_character(value: &Value) -> Result<(), LispError> {
    match value.kind() {
        Kind::Integer(character) if (0..=0x3f_ffff).contains(&character) => Ok(()),
        _ => Err(wrong_type_argument("characterp", *value)),
    }
}

fn require_frame(frame: Option<&Value>) -> Result<(), LispError> {
    match frame.map(|v| v.kind()) {
        None | Some(Kind::Nil | Kind::Frame(_)) => Ok(()),
        Some(frame) => Err(wrong_type_argument("framep", frame.value())),
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
        data.push(*gstring);
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
    let font = header[0];
    if !font.is_nil()
        && !matches!(font.kind(), Kind::Symbol(name) if interp.has_coding_system(&name))
        && font::font_kind(interp, &font) != Some(font::FontKind::Object)
    {
        return None;
    }
    if header
        .iter()
        .skip(1)
        .any(|value| !matches!(value.kind(), Kind::Integer(number) if number >= 0))
        || (!body[1].is_nil() && !matches!(body[1].kind(), Kind::Integer(number) if number >= 0))
    {
        return None;
    }
    for glyph in body.iter().skip(2).take_while(|glyph| !glyph.is_nil()) {
        if !is_vector_value(glyph) || !vector_items(glyph).is_ok_and(|items| items.len() == 10) {
            return None;
        }
    }
    Some((font, body[1]))
}

fn args_out_of_range(object: Value, position: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("args-out-of-range"),
        object,
        position,
    ]))
}

fn fontset_index(interp: &Interpreter, name: &str) -> Option<usize> {
    interp
        .fontset_states
        .iter()
        .position(|fontset| fontset.name.eq_ignore_ascii_case(name))
}

/// fontset.c's fs_query_fontset: the base fontset NAME names, its
/// fontset-alias-alist alias resolved; with NAME_PATTERN 0 a name with
/// wildcards is a pattern (fontset_pattern_regexp), with 1 NAME is one.
fn fs_query_fontset(
    interp: &mut Interpreter,
    name: &Value,
    mut name_pattern: i32,
    env: &mut Env,
) -> Result<Option<usize>, LispError> {
    let mut name = super::call(interp, "downcase", std::slice::from_ref(name), env)?;
    if name_pattern != 1 {
        let aliases = interp
            .lookup_var("fontset-alias-alist", env)
            .unwrap_or(Value::Nil);
        let mut tem = super::call(interp, "rassoc", &[name, aliases], env)?;
        if tem.is_nil() {
            tem = super::call(interp, "assoc", &[name, aliases], env)?;
        }
        if let Some((alias, _)) = tem.cons_values()
            && alias.is_string()
        {
            name = alias;
        } else if name_pattern == 0
            && let Some(regexp) = fontset_pattern_regexp(&name)?
        {
            name = regexp;
            name_pattern = 1;
        }
    }
    let text = string_text(&name)?;
    for index in 0..interp.fontset_states.len() {
        let this_name = interp.fontset_states[index].name.clone();
        let found = if name_pattern == 1 {
            let canon = interp.standard_case_tables().canon;
            crate::lisp::primitives::fast_string_match(
                interp,
                env,
                &name,
                &Value::string(&this_name),
                Some(canon),
                false,
            )?
            .is_some()
        } else {
            text.eq_ignore_ascii_case(&this_name)
        };
        if found {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

/// fontset.c's fontset_pattern_regexp: the regexp of the fontset name
/// PATTERN with `*' and `?' wildcards, or None when it has neither.
fn fontset_pattern_regexp(pattern: &Value) -> Result<Option<Value>, LispError> {
    let bytes = font::string_sdata(pattern)?;
    let bytes = &bytes[..bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len())];
    if !bytes.contains(&b'*') && !bytes.contains(&b'?') {
        // PATTERN does not contain any wild cards.
        return Ok(None);
    }
    let ndashes = bytes.iter().filter(|&&b| b == b'-').count();
    let mut regex = vec![b'^'];
    for &c in bytes {
        match c {
            // If PATTERN is not full XLFD we convert "*" to ".*".
            // Otherwise we convert "*" to "[^-]*" which is much faster in
            // regular expression matching.
            b'*' if ndashes < 14 => regex.extend_from_slice(b".*"),
            b'*' => regex.extend_from_slice(b"[^-]*"),
            b'?' => regex.push(b'.'),
            b'[' | b'.' | b'\\' | b'+' | b'^' | b'$' => {
                regex.push(b'\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push(b'$');
    crate::lisp::primitives::strings::make_string(regex)
        .map(Some)
        .map_err(|error| LispError::Signal(error.to_string()))
}

/// fontset.c's check_fontset_name: the fontset NAME names after FRAME is
/// decoded as a live frame: t the default one, nil the frame's own (none
/// on a terminal frame), a string the base fontset of that name, or else
/// as a pattern.
fn check_fontset_name(
    interp: &mut Interpreter,
    name: &Value,
    frame: Option<&Value>,
    env: &mut Env,
) -> Result<usize, LispError> {
    super::frames::decode_live_frame(interp, frame, true)?;
    match name.kind() {
        Kind::T => return Ok(0),
        Kind::Nil => {
            return Err(LispError::Signal(
                "Can't use fontsets in non-GUI frames".into(),
            ));
        }
        _ => {}
    }
    if !name.is_string() {
        return Err(wrong_type_argument("stringp", *name));
    }
    // First try NAME as literal.
    if let Some(id) = fs_query_fontset(interp, name, 2, env)? {
        return Ok(id);
    }
    // For backward compatibility, try again NAME as pattern.
    if let Some(id) = fs_query_fontset(interp, name, 0, env)? {
        return Ok(id);
    }
    Err(LispError::Signal(format!(
        "Fontset `{}' does not exist",
        string_text(name)?
    )))
}

/// The family and registry names of the font-spec SPEC, as fontset.c's
/// font_def keeps them for Ffontset_font.
fn font_pattern_of_spec(interp: &Interpreter, spec: &Value) -> FontPatternState {
    let component = |index: usize| {
        let value = font::aref(interp, spec, index);
        match value.kind() {
            Kind::Symbol(symbol) => Some(symbol.to_string()),
            Kind::T => Some("t".into()),
            _ => None,
        }
    };
    FontPatternState {
        family: component(font::FONT_FAMILY_INDEX),
        registry: component(font::FONT_REGISTRY_INDEX),
    }
}

/// Fset_fontset_font's FONT-SPEC: a (FAMILY . REGISTRY) cons through
/// font_parse_family_registry, a font name through Ffont_spec's :name, a
/// font-spec as it is, or nil.
fn font_pattern(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Option<FontPatternState>, LispError> {
    let spec = if let Some((family, registry)) = value.cons_values() {
        let spec = font::font_make_spec(interp);
        font::font_parse_family_registry(interp, &family, &registry, &spec, env)?;
        spec
    } else if value.is_string() {
        font::font_spec(interp, &[Value::symbol(":name"), *value], env)?
    } else if font::font_kind(interp, value) == Some(font::FontKind::Spec) {
        *value
    } else if !value.is_nil() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("font"),
            Value::string("Invalid font-spec"),
            *value,
        ])));
    } else {
        return Ok(None);
    };
    Ok(Some(font_pattern_of_spec(interp, &spec)))
}

/// Fset_fontset_font's CHARACTERS: a character, a (FROM . TO) range, a
/// script of char-script-table or a charset, or nil for the fallback;
/// and whether it changes the ASCII font.
fn parse_fontset_target(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<(FontsetTargetState, bool), LispError> {
    let is_character =
        |value: &Value| matches!(value.kind(), Kind::Integer(c) if (0..=0x3f_ffff).contains(&c));
    if let Kind::Integer(character) = value.kind()
        && is_character(value)
    {
        if character < 0x80 {
            return Err(LispError::Signal(
                "Can't set a font for partial ASCII range".into(),
            ));
        }
        return Ok((FontsetTargetState::Character(character), false));
    }
    if let Some((from, to)) = value.cons_values() {
        if !is_character(&from) {
            return Err(wrong_type_argument("characterp", from));
        }
        if !is_character(&to) {
            return Err(wrong_type_argument("characterp", to));
        }
        let (Kind::Integer(from), Kind::Integer(to)) = (from.kind(), to.kind()) else {
            unreachable!("characters are fixnums");
        };
        let mut ascii_changed = false;
        if from < 0x80 {
            if from != 0 || to < 0x7f {
                return Err(LispError::Signal(
                    "Can't set a font for partial ASCII range".into(),
                ));
            }
            ascii_changed = true;
        }
        return Ok((FontsetTargetState::Range(from, to), ascii_changed));
    }
    match value.kind() {
        Kind::Symbol(script) => {
            let table = interp
                .lookup_var("char-script-table", env)
                .unwrap_or(Value::Nil);
            let script_list = super::call(
                interp,
                "char-table-extra-slot",
                &[table, Value::Integer(0)],
                env,
            )?;
            let is_script = !super::call(interp, "memq", &[*value, script_list], env)?.is_nil();
            let is_charset = !super::call(interp, "charsetp", &[*value], env)?.is_nil();
            if !is_script && !is_charset {
                return Err(LispError::Signal(format!(
                    "Invalid script or charset name: {script}"
                )));
            }
            let ascii_changed = is_script && script.as_str() == "latin";
            Ok((
                FontsetTargetState::Script(script.to_string()),
                ascii_changed,
            ))
        }
        Kind::Nil => Ok((FontsetTargetState::Fallback, false)),
        _ => Err(LispError::Signal(
            "Invalid second argument for setting a font in a fontset".into(),
        )),
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
    env: &mut Env,
) -> Result<(), LispError> {
    for entry in fontlist.to_vec()? {
        let (script, definitions) = entry
            .cons_values()
            .ok_or_else(|| wrong_type_argument("listp", entry))?;
        let script = script
            .as_symbol()
            .map_err(|_| wrong_type_argument("symbolp", script))?
            .to_string();
        let definitions = definitions.to_vec().unwrap_or_else(|_| vec![definitions]);
        for definition in definitions {
            let pattern = font_pattern(interp, &definition, env)?;
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

/// Flist_fonts on a terminal frame, which has no font driver: the frame,
/// FONT-SPEC, NUM and PREFER checked, then no entity.
fn list_fonts(
    interp: &mut Interpreter,
    font_spec: &Value,
    frame: Option<&Value>,
    num: Option<&Value>,
    prefer: Option<&Value>,
) -> Result<Value, LispError> {
    super::frames::decode_live_frame(interp, frame, true)?;
    font::check_font_spec(interp, font_spec)?;
    if let Some(num) = num.filter(|value| !value.is_nil()) {
        let Kind::Integer(n) = num.kind() else {
            return Err(wrong_type_argument("fixnump", *num));
        };
        if n <= 0 {
            return Ok(Value::Nil);
        }
    }
    if let Some(prefer) = prefer.filter(|value| !value.is_nil()) {
        font::check_font_spec(interp, prefer)?;
    }
    Ok(Value::Nil)
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "font-spec" => font::font_spec(interp, args, env),
            "fontp" => {
                need_arg_range(name, args, 1, 2)?;
                let font_type = match args[0].kind() {
                    Kind::Record(id) => interp.find_record(id).and_then(|record| {
                        (record.kind == crate::lisp::eval::RecordKind::Font)
                            .then(|| record.symbol_type_name().map(str::to_owned))
                            .flatten()
                    }),
                    _ => None,
                };
                let matches = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => font_type.is_some(),
                    Some(Kind::Symbol(expected))
                        if matches!(
                            expected.as_str(),
                            "font-spec" | "font-entity" | "font-object"
                        ) =>
                    {
                        font_type.as_deref() == Some(&expected)
                    }
                    Some(extra_type) => {
                        return Err(wrong_type_argument("font-extra-type", extra_type.value()));
                    }
                };
                Ok(if matches { Value::T } else { Value::Nil })
            }
            "font-get" => {
                need_args(name, args, 2)?;
                font::font_get(interp, &args[0], &args[1], env)
            }
            "font-put" => {
                need_args(name, args, 3)?;
                font::font_put(interp, &args[0], args[1], args[2], env)
            }
            "font-match-p" => {
                need_args(name, args, 2)?;
                font::check_font_spec(interp, &args[0])?;
                font::check_font(interp, &args[1])?;
                Ok(if font::font_match_p(interp, &args[0], &args[1], env)? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "font-xlfd-name" => {
                need_arg_range(name, args, 1, 3)?;
                font::font_xlfd_name(
                    interp,
                    &args[0],
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(2).is_some_and(Value::is_truthy),
                )
            }
            // Flist_fonts: no frame has a font driver to list entities.
            "list-fonts" => {
                need_arg_range(name, args, 1, 4)?;
                list_fonts(interp, &args[0], args.get(1), args.get(2), args.get(3))
            }
            // Ffind_font: Flist_fonts with NUM 1.
            "find-font" => {
                need_arg_range(name, args, 1, 2)?;
                list_fonts(
                    interp,
                    &args[0],
                    args.get(1),
                    Some(&Value::Integer(1)),
                    None,
                )
            }
            "font-at" => {
                need_arg_range(name, args, 1, 3)?;
                let window = args.get(1).filter(|value| !value.is_nil());
                let window = match window {
                    Some(window) => {
                        window_record_id_from_value(interp, window)
                            .ok_or_else(|| wrong_type_argument("window-live-p", *window))?;
                        *window
                    }
                    None => interp.record_value(interp.selected_window_id()),
                };
                if let Some(string) = args.get(2).filter(|value| !value.is_nil()) {
                    let Kind::Integer(position) = args[0].kind() else {
                        return Err(wrong_type_argument("fixnump", args[0]));
                    };
                    let string_value = *string;
                    let string = string_like(&string_value)
                        .ok_or_else(|| wrong_type_argument("stringp", *string))?;
                    let length = string.text.chars().count() as i64;
                    if !(0..length).contains(&position) {
                        return Err(args_out_of_range(string_value, args[0]));
                    }
                } else {
                    if window_buffer_id(interp, &window) != Some(interp.current_buffer_id()) {
                        return Err(LispError::Signal(
                            "Specified window is not displaying the current buffer".into(),
                        ));
                    }
                    let position = position_from_value(interp, &args[0])?;
                    let (begin, end) = interp.buffer.borrow().restriction();
                    if !(begin..end).contains(&position) {
                        // args_out_of_range_3 (POSITION, BEGV, ZV).
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("args-out-of-range"),
                            args[0],
                            Value::Integer(begin as i64),
                            Value::Integer(end as i64),
                        ])));
                    }
                }
                Ok(Value::Nil)
            }
            "font-face-attributes" => {
                need_arg_range(name, args, 1, 2)?;
                super::frames::decode_live_frame(interp, args.get(1), true)?;
                font::font_face_attributes(interp, &args[0], env)
            }
            // Fopen_font: the frame decoded before the entity is checked;
            // no frame can open one.
            "open-font" => {
                need_arg_range(name, args, 1, 3)?;
                super::frames::decode_live_frame(interp, args.get(2), true)?;
                font::check_font_entity(interp, &args[0])?;
                Err(window_system_frame_required())
            }
            "close-font" => {
                need_arg_range(name, args, 1, 2)?;
                font::check_font_object(interp, &args[0])?;
                super::frames::decode_live_frame(interp, args.get(1), true)?;
                Err(window_system_frame_required())
            }
            "query-font" => {
                need_args(name, args, 1)?;
                font::check_font_object(interp, &args[0])?;
                Err(window_system_frame_required())
            }
            "font-has-char-p" => {
                need_arg_range(name, args, 2, 3)?;
                font::check_font(interp, &args[0])?;
                require_character(&args[1])?;
                require_frame(args.get(2))?;
                Err(window_system_frame_required())
            }
            "font-get-glyphs" => {
                need_arg_range(name, args, 3, 4)?;
                font::check_font_object(interp, &args[0])?;
                Err(window_system_frame_required())
            }
            "font-variation-glyphs" => {
                need_args(name, args, 2)?;
                font::check_font_object(interp, &args[0])?;
                require_character(&args[1])?;
                Err(window_system_frame_required())
            }
            "font-shape-gstring" => {
                need_args(name, args, 2)?;
                let (font, id) = composition_gstring_parts(interp, &args[0])
                    .ok_or_else(|| invalid_glyph_string(&args[0]))?;
                if !id.is_nil() {
                    return Ok(args[0]);
                }
                font::check_font_object(interp, &font)?;
                Err(window_system_frame_required())
            }
            // Ffont_info: NAME a font or a string, then
            // decode_window_system_frame.
            "font-info" => {
                need_arg_range(name, args, 1, 2)?;
                if font::font_kind(interp, &args[0]).is_none() && !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0]));
                }
                Err(super::frames::window_system_frame_error(
                    interp,
                    args.get(1),
                ))
            }
            // xsettings.c: the fonts the XSETTINGS manager or GSettings
            // gave, read when an X display opens; Emaxx opens none.
            "font-get-system-font" | "font-get-system-normal-font" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            // font.c: decode_live_frame; a terminal frame's drivers list no
            // families and it has no display name list.
            "font-family-list" | "frame-font-cache" => {
                need_arg_range(name, args, 0, 1)?;
                super::frames::decode_live_frame(interp, args.first(), true)?;
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
                add_fontlist(interp, fontset_index, &args[1], env)?;
                Ok(Value::string(&fontset_name))
            }
            // Fset_fontset_font: the fontset and frame first, then the
            // font spec, then the characters.
            "set-fontset-font" => {
                need_arg_range(name, args, 3, 5)?;
                let fontset_index = check_fontset_name(interp, &args[0], args.get(3), env)?;
                let pattern = font_pattern(interp, &args[2], env)?;
                let (target, ascii_changed) = parse_fontset_target(interp, &args[1], env)?;
                if ascii_changed && pattern.is_none() {
                    return Err(LispError::Signal("Can't set ASCII font to nil".into()));
                }
                // FONTSET_ADD: nil replaces, `append' appends and any other
                // value prepends.
                let add = match args.get(4).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => None,
                    Some(Kind::Symbol(add)) if add.as_str() == "append" => Some("append"),
                    Some(_) => Some("prepend"),
                };
                set_fontset_mapping(interp, fontset_index, target, pattern, add);
                Ok(Value::Nil)
            }
            "fontset-font" => {
                need_arg_range(name, args, 2, 3)?;
                let fontset_index = check_fontset_name(interp, &args[0], None, env)?;
                let Kind::Integer(character) = args[1].kind() else {
                    return Err(wrong_type_argument("characterp", args[1]));
                };
                if !(0..=0x3f_ffff).contains(&character) {
                    return Err(wrong_type_argument("characterp", args[1]));
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
