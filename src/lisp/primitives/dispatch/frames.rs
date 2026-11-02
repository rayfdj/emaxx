use super::*;
use crate::lisp::types::Kind;
use regex::Regex;
use std::sync::OnceLock;

fn frame_value(id: crate::lisp::types::FrameRef) -> Value {
    Value::Frame(id)
}

fn decode_frame(
    interp: &Interpreter,
    value: Option<&Value>,
    nil_defaults_to_selected: bool,
    require_live: bool,
) -> Result<crate::lisp::types::FrameRef, LispError> {
    let value = value.unwrap_or(&Value::Nil);
    let id = match value.kind() {
        Kind::Nil if nil_defaults_to_selected => interp.selected_frame_id,
        Kind::Frame(id) => id,
        _ => {
            return Err(wrong_type_argument(
                if require_live {
                    "frame-live-p"
                } else {
                    "framep"
                },
                *value,
            ));
        }
    };
    if require_live && !interp.frame_is_live(id) {
        return Err(wrong_type_argument("frame-live-p", frame_value(id)));
    }
    Ok(id)
}

pub(super) fn decode_live_frame(
    interp: &Interpreter,
    value: Option<&Value>,
    nil_defaults_to_selected: bool,
) -> Result<crate::lisp::types::FrameRef, LispError> {
    decode_frame(interp, value, nil_defaults_to_selected, true)
}

type FrameRef = crate::lisp::types::FrameRef;

/// frame.c:store_in_alist: PROP's entry in *ALIST given VAL with
/// Fsetcdr, or a new entry consed onto its front.
pub(crate) fn store_in_alist(alist: &mut Value, prop: Value, val: Value) -> Result<(), LispError> {
    match assq_entry(*alist, prop)? {
        Some(entry) => entry.set_cdr(val),
        None => {
            *alist = Value::cons(Value::cons(prop, val), *alist);
            Ok(())
        }
    }
}

/// Fassq (PROP, ALIST): the entry whose car is PROP, an entry that is
/// no cons passed over; an ALIST that is no list signals `listp'.
fn assq_entry(alist: Value, prop: Value) -> Result<Option<Value>, LispError> {
    let mut tail = alist;
    while let Some((entry, rest)) = tail.cons_values() {
        if entry
            .cons_values()
            .is_some_and(|(key, _)| key.eq_value(prop))
        {
            return Ok(Some(entry));
        }
        tail = rest;
    }
    if tail.is_nil() {
        Ok(None)
    } else {
        Err(wrong_type_argument("listp", alist))
    }
}

/// Fcdr (Fassq (PROP, ALIST)).
fn alist_get(alist: Value, prop: Value) -> Value {
    assq_entry(alist, prop)
        .ok()
        .flatten()
        .and_then(|entry| entry.cons_values())
        .map_or(Value::Nil, |(_, value)| value)
}

/// frame.c:get_frame_param: PROP's value in FRAME's param_alist.
pub(crate) fn get_frame_param(frame: FrameRef, prop: Value) -> Value {
    let alist = frame.borrow().param_alist;
    alist_get(alist, prop)
}

/// frame.c:frame_unspecified_color on a text frame: tty_color_name of
/// the frame's default background or foreground pixel (which a text
/// frame never changes) when UNSPEC begins `unspecified-bg' or
/// `unspecified-fg' (strncmp over UNSPEC's bytes), else None.
fn frame_unspecified_color(unspec: &Value) -> Option<Value> {
    let text = string_like(unspec)?;
    let bytes = text.text.as_bytes();
    if b"unspecified-bg".starts_with(bytes) {
        Some(Value::string("unspecified-bg"))
    } else if b"unspecified-fg".starts_with(bytes) {
        Some(Value::string("unspecified-fg"))
    } else {
        None
    }
}

/// frame.c:Fframe_parameters of a live text frame: a copy of its
/// param_alist, with the colors it reports for unspecified ones, its
/// font, name and size, the mode line and split flags, its buffer lists
/// and its bar lines stored in.
fn frame_parameters_value(interp: &Interpreter, id: FrameRef) -> Result<Value, LispError> {
    let frame = id.borrow();
    let mut alist = crate::lisp::primitives::values::copy_alist_value(&frame.param_alist)?;
    for (prop, unspecified) in [
        ("foreground-color", "unspecified-fg"),
        ("background-color", "unspecified-bg"),
    ] {
        let prop = Value::symbol(prop);
        let elt = assq_entry(alist, prop)?.and_then(|entry| entry.cons_values());
        match elt {
            Some((_, value)) if value.is_string() => {
                if let Some(color) = frame_unspecified_color(&value) {
                    store_in_alist(&mut alist, prop, color)?;
                }
            }
            _ => store_in_alist(&mut alist, prop, Value::string(unspecified))?,
        }
    }
    store_in_alist(&mut alist, Value::symbol("font"), Value::string("tty"))?;
    store_in_alist(&mut alist, Value::symbol("name"), id.name.get())?;
    // f->new_height and f->new_width when a new size is queued.
    let height = if frame.new_size_p && frame.new_height >= 0 {
        frame.new_height
    } else {
        frame.text_height
    };
    store_in_alist(&mut alist, Value::symbol("height"), Value::Integer(height))?;
    let width = if frame.new_size_p && frame.new_width >= 0 {
        frame.new_width
    } else {
        frame.width
    };
    store_in_alist(&mut alist, Value::symbol("width"), Value::Integer(width))?;
    // FRAME_WANTS_MODELINE_P and FRAME_NO_SPLIT_P, which a text frame
    // keeps at make_frame's true and false.
    store_in_alist(&mut alist, Value::symbol("modeline"), Value::T)?;
    store_in_alist(&mut alist, Value::symbol("unsplittable"), Value::Nil)?;
    let buffers =
        |ids: &[u64]| Value::list(ids.iter().filter_map(|buffer| interp.buffer_value(*buffer)));
    store_in_alist(
        &mut alist,
        Value::symbol("buffer-list"),
        buffers(&frame.buffer_list),
    )?;
    store_in_alist(
        &mut alist,
        Value::symbol("buried-buffer-list"),
        buffers(&frame.buried_buffer_list),
    )?;
    store_in_alist(
        &mut alist,
        Value::symbol("menu-bar-lines"),
        Value::Integer(frame.menu_bar_lines),
    )?;
    store_in_alist(
        &mut alist,
        Value::symbol("tab-bar-lines"),
        Value::Integer(frame.tab_bar_lines),
    )?;
    Ok(alist)
}

/// frame.c:Fframe_parameter of a live text frame.
pub(super) fn frame_parameter_of(
    interp: &Interpreter,
    id: FrameRef,
    parameter: Value,
) -> Result<Value, LispError> {
    Ok(match parameter.symbol_str() {
        Some("name") => id.name.get(),
        // A text frame has no scroll bars (vertical_scroll_bar_none, no
        // horizontal ones) and its extra_line_spacing stays 0.
        Some("vertical-scroll-bars" | "horizontal-scroll-bars") => Value::Nil,
        Some("line-spacing") => Value::Integer(0),
        Some("background-color" | "foreground-color") => {
            let alist = id.borrow().param_alist;
            match assq_entry(alist, parameter)?.and_then(|entry| entry.cons_values()) {
                Some((_, value)) => {
                    // Fframe_parameters puts the actual color names,
                    // even when the alist says "unspecified".
                    if value.is_string() {
                        frame_unspecified_color(&value).unwrap_or(value)
                    } else {
                        value
                    }
                }
                None => alist_get(frame_parameters_value(interp, id)?, parameter),
            }
        }
        Some("display-type" | "background-mode") => get_frame_param(id, parameter),
        _ => alist_get(frame_parameters_value(interp, id)?, parameter),
    })
}

/// `frame_parameter_of' of the parameter named NAME, nil when the frame
/// cannot report it.
pub(super) fn frame_parameter_value(interp: &Interpreter, id: FrameRef, name: &str) -> Value {
    frame_parameter_of(interp, id, Value::symbol(name)).unwrap_or(Value::Nil)
}

/// Fframe_parameter (FRAME, Qdisplay) as callproc.c asks it for the
/// child's DISPLAY: FRAME nil is the selected frame.
pub(crate) fn frame_display_parameter(
    interp: &Interpreter,
    frame: &Value,
) -> Result<Value, LispError> {
    let id = decode_frame(interp, Some(frame), true, false)?;
    Ok(if interp.frame_is_live(id) {
        frame_parameter_value(interp, id, "display")
    } else {
        Value::Nil
    })
}

fn is_window_record(interp: &Interpreter, value: Value) -> bool {
    matches!(value.kind(), Kind::Record(id) if interp.find_record(id).is_some_and(|record|
        record.kind == crate::lisp::eval::RecordKind::Window))
}

/// frame.c:frame_name_fnn_p: whether NAME is `F' and decimal digits.
fn frame_name_fnn_p(name: &[u8]) -> bool {
    name.len() > 1 && name[0] == b'F' && name[1..].iter().all(u8::is_ascii_digit)
}

/// frame.c:set_term_frame_name: nil names the frame F<num> unless it
/// already is, a string must not be of that form.
fn set_term_frame_name(
    interp: &mut Interpreter,
    id: FrameRef,
    name: Value,
) -> Result<(), LispError> {
    let current = id.name.get();
    let name = if name.is_nil() {
        if string_like(&current).is_some_and(|text| frame_name_fnn_p(text.text.as_bytes())) {
            return Ok(());
        }
        let number = interp.next_frame_id;
        interp.next_frame_id += 1;
        Value::string(&format!("F{number}"))
    } else {
        let Some(text) = string_like(&name) else {
            return Err(wrong_type_argument("stringp", name));
        };
        if string_like(&current).is_some_and(|current| current.text == text.text) {
            return Ok(());
        }
        if frame_name_fnn_p(text.text.as_bytes()) {
            return Err(LispError::Signal(
                "Frame names of the form F<num> are usurped by Emacs".into(),
            ));
        }
        name
    };
    id.name.set(name);
    Ok(())
}

/// frame.c:store_frame_param: PROP's checks, the buffer lists kept
/// apart, the alist entry set, then what a text frame keeps elsewhere
/// (the bar lines and the name).
fn store_frame_parameter(
    interp: &mut Interpreter,
    id: FrameRef,
    prop: Value,
    mut val: Value,
) -> Result<(), LispError> {
    match prop.symbol_str() {
        Some("minibuffer") => {
            if is_window_record(interp, val) {
                let mini = id.borrow().minibuffer_window.map(Value::Record);
                let Kind::Record(window) = val.kind() else {
                    unreachable!()
                };
                if !super::display::window_is_minibuffer(interp, window.id) {
                    return Err(LispError::Signal(
                        "The `minibuffer' parameter does not specify a valid minibuffer window"
                            .into(),
                    ));
                }
                // A text frame has its own minibuffer window.
                if mini.is_some_and(|mini| mini.eq_value(val)) {
                    val = Value::T;
                } else {
                    return Err(LispError::Signal(
                        "Can't change the minibuffer window of a frame with its own minibuffer"
                            .into(),
                    ));
                }
            } else {
                let old = get_frame_param(id, prop);
                if !old.is_nil() {
                    if is_window_record(interp, old) && val.is_nil() {
                        val = old;
                    } else if !old.eq_value(val) {
                        return Err(LispError::Signal(
                            "Can't change the `minibuffer' parameter of this frame".into(),
                        ));
                    }
                }
            }
        }
        Some(name @ ("parent-frame" | "delete-before")) => {
            let old = get_frame_param(id, prop);
            if !old.eq_value(val) && !val.is_nil() {
                let live = |value: Value| matches!(value.kind(), Kind::Frame(frame) if interp.frame_is_live(frame));
                if !live(val) {
                    return Err(LispError::Signal(format!(
                        "Invalid `{name}' frame parameter"
                    )));
                }
                let mut frame = val;
                while let Kind::Frame(next) = frame.kind()
                    && interp.frame_is_live(next)
                {
                    if next == id {
                        return Err(LispError::Signal(format!(
                            "Circular specification of `{name}' frame parameter"
                        )));
                    }
                    frame = get_frame_param(next, prop);
                }
            }
        }
        Some(name @ ("buffer-list" | "buried-buffer-list")) => {
            // The buffer lists are kept apart from the alist, each of the
            // live buffers in VAL, in its order.
            let mut ids = Vec::new();
            let mut tail = val;
            while let Some((buffer, rest)) = tail.cons_values() {
                if let Kind::Buffer(buffer) = buffer.kind()
                    && interp.has_buffer_id(buffer.id)
                {
                    ids.push(buffer.id);
                }
                tail = rest;
            }
            let mut frame = id.borrow_mut();
            if name == "buffer-list" {
                frame.buffer_list = ids;
            } else {
                frame.buried_buffer_list = ids;
            }
            return Ok(());
        }
        Some("scroll-bar-width" | "scroll-bar-height")
            if !val.is_nil()
                && !matches!(val.kind(), Kind::Integer(width) if (1..=i64::from(i32::MAX)).contains(&width)) =>
        {
            val = get_frame_param(id, prop);
        }
        Some("tty-color-mode") => {
            // GNU's channel for changing a tty frame's color support
            // (term.c's set_tty_color_mode): tty_setup_colors publishes
            // the count, then safe_calln (Qtty_set_up_initial_frame_faces)
            // has faces.el recompute every face against the new display.
            let cells = match val.kind() {
                Kind::Integer(mode) if mode > 1 => mode,
                Kind::Integer(_) | Kind::Nil => 0,
                _ => 8,
            };
            interp.set_tty_display_colors(cells);
            if let Ok(forms) =
                crate::lisp::reader::Reader::new("(tty-set-up-initial-frame-faces)").read_all()
                && let Some(form) = forms.first()
            {
                let _ = interp.eval(form, &mut crate::lisp::types::Env::new());
            }
        }
        _ => {}
    }
    let mut alist = id.borrow().param_alist;
    match assq_entry(alist, prop)? {
        Some(entry) => entry.set_cdr(val)?,
        None => {
            alist = Value::cons(Value::cons(prop, val), alist);
            id.borrow_mut().param_alist = alist;
        }
    }
    // A text frame's bar lines through set_menu_bar_lines and
    // set_tab_bar_lines, and its name through set_term_frame_name.
    match prop.symbol_str() {
        Some("menu-bar-lines") => super::display::set_menu_bar_lines(interp, id, val),
        Some("tab-bar-lines") => super::display::set_tab_bar_lines(interp, id, val),
        Some("name") => set_term_frame_name(interp, id, val)?,
        _ => {}
    }
    Ok(())
}

/// frame.c:Fmodify_frame_parameters on a text frame: ALIST's names and
/// values taken apart first, then stored last to first so the first
/// occurrence wins, the colors also reaching the default face
/// (update_face_from_frame_parameter).
pub(crate) fn modify_frame_parameters(
    interp: &mut Interpreter,
    id: FrameRef,
    alist: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    // list_length's check, then Fcar and Fcdr of each element.
    check_list(alist)?;
    let mut entries = Vec::new();
    let mut tail = alist;
    while let Some((elt, rest)) = tail.cons_values() {
        let (prop, val) = match elt.kind() {
            Kind::Nil => (Value::Nil, Value::Nil),
            Kind::Cons(_) => elt.cons_values().expect("a cons"),
            _ => return Err(wrong_type_argument("listp", elt)),
        };
        entries.push((prop, val));
        tail = rest;
    }
    for (prop, val) in entries.into_iter().rev() {
        store_frame_parameter(interp, id, prop, val)?;
        if matches!(
            prop.symbol_str(),
            Some("foreground-color" | "background-color")
        ) {
            update_face_from_frame_parameter(interp, id, prop, val, env)?;
        }
    }
    Ok(())
}

/// xfaces.c:update_face_from_frame_parameter for the colors: once the
/// frame has face definitions, its `default' face takes the color (a
/// string, else unspecified), the background first reconsidering
/// `frame-set-background-mode', and the basic faces are realized.
fn update_face_from_frame_parameter(
    interp: &mut Interpreter,
    id: FrameRef,
    param: Value,
    new_value: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let table = interp.frame_face_hash_table(id);
    let Kind::HashTable(table) = table.kind() else {
        return Ok(());
    };
    if table.count() == 0 {
        return Ok(());
    }
    let index = if param.symbol_str() == Some("foreground-color") {
        9
    } else {
        let function =
            cached_symbol!("frame-set-background-mode").with(|symbol| Value::Symbol(*symbol));
        interp.call_function_value(function, None, &[Value::Frame(id)], env)?;
        10
    };
    let default = Value::symbol("default");
    let lface = interp
        .lisp_face_vector_on(default, Some(id))
        .ok_or_else(|| crate::lisp::eval::invalid_face_error(default))?;
    let color = if new_value.is_string() {
        new_value
    } else {
        Value::symbol("unspecified")
    };
    crate::lisp::primitives::aset_vector_value(&lface, index, color)?;
    realize_basic_faces(interp, id)
}

/// The faces realize_basic_faces realizes beside the default, in its
/// order.
const BASIC_FACES: [&str; 18] = [
    "mode-line-active",
    "mode-line-inactive",
    "tool-bar",
    "fringe",
    "header-line",
    "scroll-bar",
    "border",
    "cursor",
    "mouse",
    "menu",
    "vertical-border",
    "window-divider",
    "window-divider-first-pixel",
    "window-divider-last-pixel",
    "internal-border",
    "child-frame-border",
    "tab-bar",
    "tab-line",
];

/// xfaces.c:realize_basic_faces on a text frame: realize_default_face
/// completes the frame's `default' definition (made when missing) as a
/// text frame's must be, then realize_named_face makes the definition
/// of each basic face the frame lacks.
pub(crate) fn realize_basic_faces(interp: &mut Interpreter, id: FrameRef) -> Result<(), LispError> {
    use crate::lisp::primitives::{aset_vector_value, vector_slot_value};
    let default = Value::symbol("default");
    let lface = match interp.lisp_face_vector_on(default, Some(id)) {
        Some(lface) => lface,
        None => interp.make_lisp_face_on(default, Some(id), false)?,
    };
    let unspecified = |index: usize| -> Result<bool, LispError> {
        Ok(matches!(vector_slot_value(&lface, index)?.kind(),
            Kind::Symbol(symbol) if symbol == "unspecified"))
    };
    let normal = Value::symbol("normal");
    // !FRAME_WINDOW_P: one family string for the family and foundry,
    // a normal width and a height of 1.
    let family = Value::string("default");
    aset_vector_value(&lface, 1, family)?;
    aset_vector_value(&lface, 2, family)?;
    aset_vector_value(&lface, 3, normal)?;
    aset_vector_value(&lface, 4, Value::Integer(1))?;
    for (index, value) in [(5, normal), (6, normal), (17, Value::Nil)] {
        if unspecified(index)? {
            aset_vector_value(&lface, index, value)?;
        }
    }
    // The extend, underline, overline, strike-through, box and inverse
    // attributes.
    for index in [19, 7, 12, 13, 14, 8] {
        if unspecified(index)? {
            aset_vector_value(&lface, index, Value::Nil)?;
        }
    }
    // A color the frame's parameters do not give is a text frame's
    // unspecified one.
    for (index, param, color) in [
        (9, "foreground-color", "unspecified-fg"),
        (10, "background-color", "unspecified-bg"),
    ] {
        if unspecified(index)? {
            let given = get_frame_param(id, Value::symbol(param));
            let value = if given.is_string() {
                given
            } else {
                Value::string(color)
            };
            aset_vector_value(&lface, index, value)?;
        }
    }
    if unspecified(11)? {
        aset_vector_value(&lface, 11, Value::Nil)?;
    }
    for face in BASIC_FACES {
        let face = Value::symbol(face);
        if interp.lisp_face_vector_on(face, Some(id)).is_none() {
            interp.make_lisp_face_on(face, Some(id), false)?;
        }
    }
    Ok(())
}

/// list_length's checks: a circular list signals `circular-list', one
/// that does not end in nil `listp' with its tail.
fn check_list(list: Value) -> Result<(), LispError> {
    let mut tail = list;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Some(cell) = tail.cons_ref() {
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("circular-list"),
                list,
            ])));
        }
        tail = cell.cdr.get();
    }
    if tail.is_nil() {
        Ok(())
    } else {
        Err(wrong_type_argument("listp", tail))
    }
}

fn check_frame_size(value: &Value) -> Result<i64, LispError> {
    value.as_integer()
}

fn parse_geometry(string: &str) -> Value {
    static GEOMETRY: OnceLock<Regex> = OnceLock::new();
    let regex = GEOMETRY.get_or_init(|| {
        Regex::new(
            r"^=?(?:(?P<width>[0-9]+)?[xX](?P<height>[0-9]+)?)?(?P<x>[+-][0-9]+)?(?P<y>[+-][0-9]+)?$",
        )
        .expect("static X geometry regex")
    });
    let Some(captures) = regex.captures(string) else {
        return Value::Nil;
    };
    if captures.name("width").is_none()
        && captures.name("height").is_none()
        && captures.name("x").is_none()
        && captures.name("y").is_none()
    {
        return Value::Nil;
    }
    let integer = |name: &str| {
        captures
            .name(name)
            .and_then(|value| value.as_str().parse::<i64>().ok())
    };
    let mut result = Vec::new();
    if let Some(height) = integer("height") {
        result.push(Value::cons(Value::symbol("height"), Value::Integer(height)));
    }
    if let Some(width) = integer("width") {
        result.push(Value::cons(Value::symbol("width"), Value::Integer(width)));
    }
    if let Some(top) = integer("y") {
        result.push(Value::cons(Value::symbol("top"), Value::Integer(top)));
    }
    if let Some(left) = integer("x") {
        result.push(Value::cons(Value::symbol("left"), Value::Integer(left)));
    }
    Value::list(result)
}

fn selected_frame_list(interp: &Interpreter) -> Value {
    Value::list(
        interp
            .frame_states
            .iter()
            .filter(|frame| frame.is_live())
            .map(|frame| frame_value(*frame)),
    )
}

pub(super) fn window_system_unavailable() -> LispError {
    LispError::Signal("Window system is not in use or not initialized".into())
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "selected-frame" | "last-nonminibuffer-frame" => {
                need_args(name, args, 0)?;
                Ok(interp.selected_frame_value())
            }
            "old-selected-frame" => {
                need_args(name, args, 0)?;
                Ok(interp.old_selected_frame_value())
            }
            "framep" => {
                need_args(name, args, 1)?;
                Ok(match args[0].kind() {
                    Kind::Frame(_) => Value::T,
                    _ => Value::Nil,
                })
            }
            "frame-live-p" => {
                need_args(name, args, 1)?;
                Ok(match args[0].kind() {
                    Kind::Frame(id) if interp.frame_is_live(id) => Value::T,
                    _ => Value::Nil,
                })
            }
            "frame-list" | "visible-frame-list" => {
                need_args(name, args, 0)?;
                Ok(selected_frame_list(interp))
            }
            "frame-visible-p" => {
                need_args(name, args, 1)?;
                decode_live_frame(interp, args.first(), false)?;
                Ok(Value::T)
            }
            "make-frame-visible" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                Ok(frame_value(id))
            }
            "make-frame-invisible" => {
                need_arg_range(name, args, 0, 2)?;
                decode_live_frame(interp, args.first(), true)?;
                if !args.get(1).is_some_and(Value::is_truthy) {
                    return Err(LispError::Signal(
                        "Attempt to make invisible the sole visible or iconified frame".into(),
                    ));
                }
                Ok(Value::Nil)
            }
            "iconify-frame" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Nil)
            }
            "frame-parameter" => {
                need_args(name, args, 2)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                if !matches!(args[1].kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) {
                    return Err(wrong_type_argument("symbolp", args[1]));
                }
                if interp.frame_is_live(id) {
                    frame_parameter_of(interp, id, args[1])
                } else {
                    Ok(Value::Nil)
                }
            }
            "frame-parameters" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                if interp.frame_is_live(id) {
                    frame_parameters_value(interp, id)
                } else {
                    Ok(Value::Nil)
                }
            }
            "modify-frame-parameters" => {
                need_args(name, args, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                modify_frame_parameters(interp, id, args[1], env)?;
                Ok(Value::Nil)
            }
            "frame-char-width" | "frame-char-height" => {
                need_arg_range(name, args, 0, 1)?;
                decode_frame(interp, args.first(), true, false)?;
                Ok(Value::Integer(1))
            }
            "frame-native-width" | "frame-text-width" | "frame-text-cols" | "frame-total-cols" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let frame = id.borrow();
                Ok(Value::Integer(frame.width.max(1)))
            }
            "frame-native-height" | "frame-total-lines" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let frame = id.borrow();
                Ok(Value::Integer(frame.height.max(1)))
            }
            "frame-text-height" | "frame-text-lines" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let frame = id.borrow();
                Ok(Value::Integer(frame.text_height.max(1)))
            }
            "frame-internal-border-width"
            | "frame-fringe-width"
            | "frame-scroll-bar-width"
            | "frame-scroll-bar-height"
            | "frame-right-divider-width"
            | "frame-bottom-divider-width"
            | "frame-child-frame-border-width"
            | "tool-bar-pixel-width" => {
                need_arg_range(name, args, 0, 1)?;
                decode_frame(interp, args.first(), true, false)?;
                Ok(Value::Integer(0))
            }
            "set-frame-width" | "set-frame-height" => {
                // frame.c:Fset_frame_width, Fset_frame_height: the text
                // size changes in one dimension at once (INHIBIT 1).
                need_arg_range(name, args, 2, 4)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let size = check_frame_size(&args[1])?;
                let pretend = args.get(2).is_some_and(Value::is_truthy);
                let (width, height) = {
                    let frame = id.borrow();
                    if name == "set-frame-width" {
                        (size, frame.text_height)
                    } else {
                        (frame.width, size)
                    }
                };
                super::display::adjust_frame_size(interp, id, width, height, 1, pretend, env)?;
                Ok(Value::Nil)
            }
            "set-frame-size" => {
                need_arg_range(name, args, 3, 4)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let width = check_frame_size(&args[1])?;
                let height = check_frame_size(&args[2])?;
                super::display::adjust_frame_size(interp, id, width, height, 1, false, env)?;
                Ok(Value::Nil)
            }
            "frame-position" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let frame = id.borrow();
                Ok(Value::cons(
                    Value::Integer(frame.left),
                    Value::Integer(frame.top),
                ))
            }
            "set-frame-position" => {
                need_args(name, args, 3)?;
                decode_live_frame(interp, args.first(), true)?;
                check_frame_size(&args[1])?;
                check_frame_size(&args[2])?;
                // TTY frames have no window-system offset hook.
                Ok(Value::T)
            }
            "frame-scale-factor" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::float(1.0))
            }
            "frame-parent" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Nil)
            }
            "frame-ancestor-p" => {
                need_args(name, args, 2)?;
                decode_live_frame(interp, args.first(), true)?;
                decode_live_frame(interp, args.get(1), true)?;
                Ok(Value::Nil)
            }
            "next-frame" | "previous-frame" => {
                need_arg_range(name, args, 0, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let terminal = id.borrow().terminal.expect("live frame terminal").id;
                let mut ids: Vec<_> = interp
                    .frame_states
                    .iter()
                    .filter(|frame| {
                        frame.is_live()
                            && frame.borrow().terminal.expect("live frame terminal").id == terminal
                    })
                    .copied()
                    .collect();
                if name == "previous-frame" {
                    ids.reverse();
                }
                if let Some(index) = ids.iter().position(|frame| *frame == id) {
                    ids.rotate_left(index);
                }
                let next = ids
                    .iter()
                    .cycle()
                    .skip(1)
                    .take(ids.len())
                    .copied()
                    .find(|candidate| {
                        frame_parameter_value(interp, *candidate, "no-other-frame").is_nil()
                            && match args.get(1).map(|v| v.kind()) {
                                Some(Kind::Record(window)) => {
                                    candidate.borrow().minibuffer_window_id() == window.id
                                        || interp.window_frame_id(window.id) == Some(*candidate)
                                }
                                _ => true,
                            }
                    })
                    .unwrap_or(id);
                Ok(frame_value(next))
            }
            "select-frame" => {
                need_arg_range(name, args, 1, 2)?;
                let id = decode_live_frame(interp, args.first(), false)?;
                select_frame(interp, id, args.get(1).is_some_and(Value::is_truthy), env)?;
                Ok(frame_value(id))
            }
            "handle-switch-frame" => {
                need_args(name, args, 1)?;
                let frame = args[0]
                .to_vec()
                .ok()
                .filter(|items| {
                    matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(event)) if event == "switch-frame")
                })
                .and_then(|items| items.get(1).cloned())
                .unwrap_or_else(|| args[0]);
                let id = decode_frame(interp, Some(&frame), false, false)?;
                if !interp.frame_is_live(id) {
                    return Ok(Value::Nil);
                }
                select_frame(interp, id, false, env)?;
                Ok(frame_value(id))
            }
            "make-terminal-frame" => {
                need_args(name, args, 1)?;
                make_terminal_frame(interp, &args[0], env)
            }
            "delete-frame" => {
                need_arg_range(name, args, 0, 2)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                delete_frame(
                    interp,
                    id,
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(1)
                        .is_some_and(|value| value.symbol_str() == Some("noelisp")),
                    env,
                )?;
                Ok(Value::Nil)
            }
            "mouse-position" | "mouse-pixel-position" => {
                need_args(name, args, 0)?;
                Ok(Value::list([interp.selected_frame_value(), Value::Nil]))
            }
            "set-mouse-position" | "set-mouse-pixel-position" => {
                need_args(name, args, 3)?;
                let id = decode_live_frame(interp, args.first(), false)?;
                check_frame_size(&args[1])?;
                check_frame_size(&args[2])?;
                select_frame(interp, id, false, env)?;
                Ok(Value::Nil)
            }
            "raise-frame" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                select_frame(interp, id, false, env)?;
                Ok(Value::Nil)
            }
            "lower-frame" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Nil)
            }
            "redirect-frame-focus" => {
                need_arg_range(name, args, 1, 2)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let focus = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => None,
                    Some(value) => Some(decode_live_frame(interp, Some(&value.value()), false)?),
                };
                id.borrow_mut().focus_frame_id = focus;
                Ok(Value::Nil)
            }
            "frame-focus" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                Ok(id
                    .borrow()
                    .focus_frame_id
                    .map(frame_value)
                    .unwrap_or(Value::Nil))
            }
            "x-focus-frame" => {
                need_arg_range(name, args, 1, 2)?;
                decode_live_frame(interp, args.first(), false)?;
                Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ))
            }
            // Fx_uses_old_gtk_dialog: t only for a GTK file dialog on a
            // window-system frame.
            "x-display-list" | "x-hide-tip" | "x-uses-old-gtk-dialog" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "x-display-backing-store"
            | "x-display-color-cells"
            | "x-display-grayscale-p"
            | "x-display-mm-height"
            | "x-display-mm-width"
            | "x-display-pixel-height"
            | "x-display-pixel-width"
            | "x-display-planes"
            | "x-display-save-under"
            | "x-display-screens"
            | "x-display-visual-class"
            | "x-server-max-request-size"
            | "x-server-vendor"
            | "x-server-version"
            | "xw-display-color-p" => {
                need_arg_range(name, args, 0, 1)?;
                Err(window_system_unavailable())
            }
            "xw-color-defined-p" | "xw-color-values" => {
                need_arg_range(name, args, 1, 2)?;
                Err(window_system_unavailable())
            }
            "frame-after-make-frame" => {
                need_args(name, args, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                id.borrow_mut().after_make_frame = args[1].is_truthy();
                Ok(args[1])
            }
            "frame-window-state-change" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                Ok(if id.borrow().window_state_change {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-frame-window-state-change" => {
                need_arg_range(name, args, 0, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let state = args.get(1).is_some_and(Value::is_truthy);
                id.borrow_mut().window_state_change = state;
                Ok(if state { Value::T } else { Value::Nil })
            }
            "frame-pointer-visible-p" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                Ok(if id.borrow().pointer_invisible {
                    Value::Nil
                } else {
                    Value::T
                })
            }
            "frame--set-was-invisible" => {
                need_args(name, args, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let state = args[1].is_truthy();
                id.borrow_mut().was_invisible = state;
                Ok(if state { Value::T } else { Value::Nil })
            }
            "reconsider-frame-fonts" => {
                need_args(name, args, 1)?;
                decode_live_frame(interp, args.first(), false)?;
                Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ))
            }
            "x-get-resource" => {
                need_arg_range(name, args, 2, 4)?;
                Err(window_system_unavailable())
            }
            "x-parse-geometry" => {
                need_args(name, args, 1)?;
                Ok(parse_geometry(&string_text(&args[0])?))
            }
        }
    }
);

fn make_terminal_frame(
    interp: &mut Interpreter,
    parameters: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let entries = parameters.to_vec()?;
    let parameter = |name: &str| {
        entries.iter().find_map(|entry| {
            let (key, value) = entry.cons_values()?;
            (key.symbol_str() == Some(name)).then_some(value)
        })
    };
    let terminal = if let Some(value) = parameter("terminal") {
        interp
            .decode_terminal(&value)
            .ok_or_else(|| wrong_type_argument("terminal-live-p", value))?
    } else {
        // frame.c:get_future_frame_param: supplied alist, selected frame
        // parameter alist, then that frame's terminal. Non-strings mean nil.
        let future = |name: &str| {
            parameter(name)
                .or_else(|| interp.frame_parameter_override(name))
                .map(|value| string_like(&value).map(|s| s.text))
                .unwrap_or_else(|| {
                    let object = interp.decode_terminal(&Value::Nil)?;
                    let terminal = object.borrow();
                    if name == "tty" {
                        terminal.kind.as_ref().map(|_| terminal.name.clone())
                    } else {
                        terminal.kind.clone()
                    }
                })
        };
        let tty = future("tty").unwrap_or_else(|| "/dev/tty".into());
        let kind =
            future("tty-type").ok_or_else(|| LispError::Signal("Unknown terminal type".into()))?;
        interp.open_tty_terminal(&tty, &kind)?
    };
    let id = interp.new_terminal_frame(terminal);
    let terminal = terminal.borrow();
    let tty = Value::string(&terminal.name);
    let kind = terminal
        .kind
        .as_deref()
        .map(Value::string)
        .unwrap_or(Value::Nil);
    drop(terminal);
    // Fmake_terminal_frame's store_in_alist of the terminal's type and
    // name into PARMS (in place where PARMS has them), `minibuffer'
    // virtually t on a terminal frame, then Fmodify_frame_parameters.
    let mut parms = *parameters;
    store_in_alist(&mut parms, Value::symbol("tty-type"), kind)?;
    store_in_alist(&mut parms, Value::symbol("tty"), tty)?;
    store_in_alist(&mut parms, Value::symbol("minibuffer"), Value::T)?;
    modify_frame_parameters(interp, id, parms, env)?;
    Ok(Value::Frame(id))
}

pub(super) fn select_frame(
    interp: &mut Interpreter,
    id: crate::lisp::types::FrameRef,
    norecord: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    if id == interp.selected_frame_id {
        return Ok(());
    }
    let window = id.borrow().selected_window_id();
    super::call(
        interp,
        "select-window",
        &[
            interp.record_value(window),
            if norecord { Value::T } else { Value::Nil },
        ],
        env,
    )?;
    Ok(())
}

pub(super) fn deletion_hook(
    interp: &mut Interpreter,
    name: &str,
    target: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    interp
        .safe_funcall(
            Value::symbol("run-hook-with-args"),
            &[Value::symbol(name), target],
            env,
        )
        .map(|_| ())
}

pub(super) fn delete_frame(
    interp: &mut Interpreter,
    id: crate::lisp::types::FrameRef,
    force: bool,
    noelisp: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    if !id.is_live() {
        return Ok(());
    }
    let check = |interp: &Interpreter| {
        if !noelisp
            && !interp
                .frame_states
                .iter()
                .any(|frame| *frame != id && frame.is_live())
        {
            Err(LispError::Signal(
                if force {
                    "Attempt to delete the only frame"
                } else {
                    "Attempt to delete the sole visible or iconified frame"
                }
                .into(),
            ))
        } else {
            Ok(())
        }
    };
    check(interp)?;
    if noelisp {
        interp.pending_funcalls.push(Value::list([
            Value::symbol("run-hook-with-args"),
            Value::symbol("delete-frame-functions"),
            Value::Frame(id),
        ]));
    } else {
        deletion_hook(interp, "delete-frame-functions", Value::Frame(id), env)?;
    }
    if !id.is_live() {
        return Ok(());
    }
    check(interp)?;
    let terminal = id.borrow().terminal.expect("live frame terminal");
    if id == interp.selected_frame_id {
        let replacement = interp
            .frame_states
            .iter()
            .find(|frame| {
                **frame != id
                    && frame.is_live()
                    && frame
                        .borrow()
                        .terminal
                        .is_some_and(|object| object.ptr_eq(&terminal))
            })
            .or_else(|| {
                interp
                    .frame_states
                    .iter()
                    .find(|frame| **frame != id && frame.is_live())
            })
            .copied();
        if let Some(replacement) = replacement {
            select_frame(interp, replacement, false, env)?;
        }
    }
    interp.retire_frame(id);
    if !noelisp
        && !interp.frame_states.iter().any(|frame| {
            frame.is_live()
                && frame
                    .borrow()
                    .terminal
                    .is_some_and(|object| object.ptr_eq(&terminal))
        })
    {
        super::call(
            interp,
            "delete-terminal",
            &[Value::Terminal(terminal), Value::T],
            env,
        )?;
    }
    if noelisp {
        interp.pending_funcalls.push(Value::list([
            Value::symbol("run-hook-with-args"),
            Value::symbol("after-delete-frame-functions"),
            Value::Frame(id),
        ]));
    } else {
        deletion_hook(
            interp,
            "after-delete-frame-functions",
            Value::Frame(id),
            env,
        )?;
    }
    Ok(())
}
