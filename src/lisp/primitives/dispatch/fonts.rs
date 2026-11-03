use super::*;
use crate::lisp::primitives::buffers::position_from_value;
use crate::lisp::primitives::font;
use crate::lisp::primitives::fontset;
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
                Ok(fontset::fontset_list(interp))
            }
            "new-fontset" => {
                need_args(name, args, 2)?;
                fontset::new_fontset(interp, &args[0], &args[1], env)
            }
            "set-fontset-font" => {
                need_arg_range(name, args, 3, 5)?;
                fontset::set_fontset_font(interp, args, env)
            }
            "fontset-font" => {
                need_arg_range(name, args, 2, 3)?;
                fontset::fontset_font(
                    interp,
                    &args[0],
                    &args[1],
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )
            }
        }
    }
);
