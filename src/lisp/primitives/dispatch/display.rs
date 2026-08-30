use super::*;
use crate::lisp::primitives::processes::wait_pumping_processes;

// The live echo-area line of an interactive session.  GNU keeps this in
// the echo buffer that redisplay paints; the terminal frontend reads it
// after every command and inside blocking event reads so prompts appear
// before input is consumed.  Batch sessions never write it — their
// `message' contract (stderr + *Messages*) is unchanged.
/// (START, END, FACE) face runs in char offsets, the currency between
/// renderers and the frontend's paint layer.
pub(crate) type FaceSpans = Vec<(usize, usize, Value)>;
pub(crate) type EchoSpans = FaceSpans;

thread_local! {
    static ECHO_AREA_MESSAGE: std::cell::RefCell<Option<(String, EchoSpans)>> =
        const { std::cell::RefCell::new(None) };
    /// GNU's echo_area_buffer[1]: the last message shown on the glass.
    /// `redisplay' from Lisp is redisplay_preserve_echo_area — when the
    /// current message was wiped by input arrival, it re-displays this
    /// one instead of clearing the row (menu-bar-open relies on it).
    static ECHO_AREA_LAST_DISPLAYED: std::cell::RefCell<Option<(String, EchoSpans)>> =
        const { std::cell::RefCell::new(None) };
    /// The frontend is redrawing under Fredisplay's preserve semantics.
    static ECHO_PRESERVE_REDISPLAY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The current echo text came from printing to the `t' stream;
    /// further prints append (GNU's echo buffer accumulates a print
    /// sequence) while any `message' replaces it and resets this.
    static ECHO_FROM_PRINT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Monotonic count of message emissions.  GNU's message3 repaints
    /// the echo area directly (display_echo_area) even while a tty menu
    /// holds redisplay frozen, whereas read_char's input-arrival
    /// clear_message only takes effect at the next redisplay; the
    /// frontend's menu loop tells the two apart by whether this moved.
    static ECHO_MESSAGE_TICK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn bump_echo_message_tick() {
    ECHO_MESSAGE_TICK.with(|tick| tick.set(tick.get().wrapping_add(1)));
}

pub(crate) fn echo_area_message_tick() -> u64 {
    ECHO_MESSAGE_TICK.with(std::cell::Cell::get)
}

pub(crate) fn set_echo_area_message(text: Option<String>) {
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    bump_echo_message_tick();
    let message = text.map(|text| (text, Vec::new()));
    // message3 displays right away: the new message (or, for a clear,
    // nothing) becomes the last-displayed one too.
    ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| slot.clone_from(&message));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = message);
}

/// A message carrying face spans (a propertized `message' — isearch's
/// prompt); the frontend paints the spans on the echo row.
pub(crate) fn set_echo_area_message_with_spans(text: String, spans: EchoSpans) {
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    bump_echo_message_tick();
    let message = Some((text, spans));
    ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| slot.clone_from(&message));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = message);
}

/// keyboard.c read_char's wipe when the next input event arrives: the
/// message channel empties without counting as an emission, so nothing
/// repaints until the next redisplay — a menu freezing redisplay keeps
/// the old pixels on the glass exactly as GNU does.
pub(crate) fn expire_echo_area_message() {
    ECHO_FROM_PRINT.with(|flag| flag.set(false));
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| *slot = None);
}

/// Printing to the `t' stream in an interactive session displays in the
/// echo area (GNU's print_string to Qt); consecutive prints of one
/// sequence — eval-expression's value then its print format — append.
pub(crate) fn echo_area_print(text: &str) {
    let appending = ECHO_FROM_PRINT.with(std::cell::Cell::get);
    bump_echo_message_tick();
    ECHO_AREA_MESSAGE.with_borrow_mut(|slot| match slot {
        Some((existing, _)) if appending => existing.push_str(text),
        _ => *slot = Some((text.to_string(), Vec::new())),
    });
    ECHO_AREA_LAST_DISPLAYED
        .with_borrow_mut(|slot| *slot = ECHO_AREA_MESSAGE.with_borrow(|current| current.clone()));
    ECHO_FROM_PRINT.with(|flag| flag.set(true));
}

pub(crate) fn echo_area_message() -> Option<String> {
    ECHO_AREA_MESSAGE.with_borrow(|slot| slot.as_ref().map(|(text, _)| text.clone()))
}

#[cfg(test)]
pub(crate) fn echo_area_message_with_spans() -> Option<(String, EchoSpans)> {
    ECHO_AREA_MESSAGE.with_borrow(|slot| slot.clone())
}

/// What redisplay shows on the echo row — xdisp.c's echo_area_display.
/// Normally the current message, which then becomes the last-displayed
/// one (emptiness after an input-arrival wipe clears it).  Under
/// Fredisplay's preserve semantics a wiped-but-last-displayed message
/// shows again instead, leaving the wipe pending for the next normal
/// redisplay (redisplay_preserve_echo_area).
pub(crate) fn echo_display_message() -> Option<(String, EchoSpans)> {
    let current = ECHO_AREA_MESSAGE.with_borrow(|slot| slot.clone());
    if current.is_none() && ECHO_PRESERVE_REDISPLAY.with(std::cell::Cell::get) {
        return ECHO_AREA_LAST_DISPLAYED.with_borrow(|slot| slot.clone());
    }
    ECHO_AREA_LAST_DISPLAYED.with_borrow_mut(|slot| slot.clone_from(&current));
    current
}

/// Run F with `redisplay's preserve-echo-area semantics in force
/// (dispnew.c's Fredisplay calls redisplay_preserve_echo_area).
pub(crate) fn with_preserved_echo_redisplay<T>(f: impl FnOnce() -> T) -> T {
    let previous = ECHO_PRESERVE_REDISPLAY.with(|flag| flag.replace(true));
    let result = f();
    ECHO_PRESERVE_REDISPLAY.with(|flag| flag.set(previous));
    result
}

/// The face runs of a propertized string value, in char offsets.
pub(crate) fn string_face_spans(value: &Value) -> EchoSpans {
    let Value::StringObject(state) = value else {
        return Vec::new();
    };
    let state = state.borrow();
    state
        .props
        .iter()
        .filter_map(|span| {
            let face = span
                .props
                .iter()
                .find(|(name, _)| name == "face")
                .or_else(|| span.props.iter().find(|(name, _)| name == "font-lock-face"));
            face.map(|(_, face)| (span.start, span.end, face.clone()))
                .filter(|(_, _, face)| !face.is_nil())
        })
        .collect()
}

fn valid_image_spec(interp: &Interpreter, spec: &Value, env: &Env) -> bool {
    let Ok(items) = spec.to_vec() else {
        return false;
    };
    if !matches!(items.first(), Some(Value::Symbol(head)) if head == "image")
        || items.len() < 5
        || (items.len() - 1) % 2 != 0
    {
        return false;
    }
    let mut properties = std::collections::HashMap::new();
    for pair in items[1..].chunks_exact(2) {
        let Value::Symbol(key) = &pair[0] else {
            return false;
        };
        if !key.starts_with(':') || properties.insert(key.as_str(), &pair[1]).is_some() {
            return false;
        }
    }
    let Some(Value::Symbol(image_type)) = properties.get(":type").copied() else {
        return false;
    };
    let type_supported = interp
        .lookup_var("image-types", env)
        .and_then(|types| types.to_vec().ok())
        .is_some_and(|types| {
            types
                .iter()
                .any(|value| matches!(value, Value::Symbol(name) if name == image_type))
        });
    let file = properties.get(":file").copied();
    let data = properties.get(":data").copied();
    type_supported
        && matches!(
            (file, data),
            (Some(Value::String(_) | Value::StringObject(_)), None)
                | (None, Some(Value::String(_) | Value::StringObject(_)))
        )
}

fn current_bidi_paragraph_direction_value(interp: &Interpreter, env: &Env) -> Value {
    if !interp.buffer.is_multibyte()
        || interp
            .lookup_var("bidi-display-reordering", env)
            .is_some_and(|value| value.is_nil())
    {
        return Value::symbol("left-to-right");
    }
    if let Some(direction) = interp
        .lookup_var("bidi-paragraph-direction", env)
        .filter(|value| !value.is_nil())
    {
        return direction;
    }

    let point_min = interp.buffer.point_min();
    let point_max = interp.buffer.point_max();
    let position = interp.buffer.point().clamp(point_min, point_max);
    // GNU display paragraphs span ordinary newlines.  A blank line starts a
    // new paragraph only once point has reached nonblank text after it; the
    // separator itself and trailing blank lines retain the preceding
    // paragraph's direction.
    let mut start = point_min;
    let mut scan = point_min;
    while scan < point_max {
        if interp.buffer.char_at(scan) != Some('\n') {
            scan += 1;
            continue;
        }
        let mut separator_end = scan + 1;
        while separator_end < point_max
            && matches!(
                interp.buffer.char_at(separator_end),
                Some(' ' | '\t' | '\u{c}')
            )
        {
            separator_end += 1;
        }
        if interp.buffer.char_at(separator_end) != Some('\n') {
            scan += 1;
            continue;
        }
        let mut next_text = separator_end + 1;
        while next_text < point_max
            && matches!(
                interp.buffer.char_at(next_text),
                Some('\n' | ' ' | '\t' | '\u{c}')
            )
        {
            next_text += 1;
        }
        if next_text < point_max && next_text <= position {
            start = next_text;
            scan = next_text;
        } else {
            break;
        }
    }
    let text = interp
        .buffer
        .buffer_substring(start, point_max)
        .unwrap_or_default();
    let bidi = unicode_bidi::BidiInfo::new(&text, None);
    if bidi
        .paragraphs
        .first()
        .is_some_and(|paragraph| paragraph.level.is_rtl())
    {
        Value::symbol("right-to-left")
    } else {
        Value::symbol("left-to-right")
    }
}

fn with_selected_window_buffer<T>(
    interp: &mut Interpreter,
    operation: impl FnOnce(&mut Interpreter) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let selected_buffer = interp.selected_window_buffer_id();
    let saved_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(selected_buffer)?;
    let result = operation(interp);
    let restore = interp.set_current_buffer_id(saved_buffer);
    match (result, restore) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn image_hot_spot_contains(area: &Value, x: i64, y: i64) -> bool {
    let Ok(kind) = area
        .car()
        .and_then(|value| value.as_symbol().map(str::to_string))
    else {
        return false;
    };
    let Ok(shape) = area.cdr() else {
        return false;
    };
    match kind.as_str() {
        "rect" => {
            let Ok((top_left, bottom_right)) = shape
                .cons_values()
                .ok_or_else(|| wrong_type_argument("consp", shape.clone()))
            else {
                return false;
            };
            let Ok((x0, y0)) = top_left
                .cons_values()
                .ok_or_else(|| wrong_type_argument("consp", top_left.clone()))
                .and_then(|(x, y)| Ok((x.as_integer()?, y.as_integer()?)))
            else {
                return false;
            };
            let Ok((x1, y1)) = bottom_right
                .cons_values()
                .ok_or_else(|| wrong_type_argument("consp", bottom_right.clone()))
                .and_then(|(x, y)| Ok((x.as_integer()?, y.as_integer()?)))
            else {
                return false;
            };
            x0 <= x && x <= x1 && y0 <= y && y <= y1
        }
        "circle" => {
            let Some((center, radius)) = shape.cons_values() else {
                return false;
            };
            let Some((x0, y0)) = center.cons_values() else {
                return false;
            };
            let (Ok(x0), Ok(y0), Ok(radius)) =
                (x0.as_integer(), y0.as_integer(), radius.as_float())
            else {
                return false;
            };
            let dx = x0 as f64 - x as f64;
            let dy = y0 as f64 - y as f64;
            dx * dx + dy * dy <= radius * radius
        }
        "poly" => {
            let Ok(coordinates) = vector_items(&shape) else {
                return false;
            };
            if coordinates.len() < 6 || coordinates.len() % 2 != 0 {
                return false;
            }
            let Ok(mut x0) = coordinates[coordinates.len() - 2].as_integer() else {
                return false;
            };
            let Ok(mut y0) = coordinates[coordinates.len() - 1].as_integer() else {
                return false;
            };
            let mut inside = false;
            for pair in coordinates.chunks_exact(2) {
                let (x1, y1) = (x0, y0);
                let (Ok(next_x), Ok(next_y)) = (pair[0].as_integer(), pair[1].as_integer()) else {
                    return false;
                };
                x0 = next_x;
                y0 = next_y;
                if (x0 >= x && x1 >= x) || (x0 < x && x1 < x) || (y > y0 && y > y1) {
                    continue;
                }
                if y < y0 + ((y1 - y0) * (x - x0)) / (x1 - x0) {
                    inside = !inside;
                }
            }
            inside
        }
        _ => false,
    }
}

fn lookup_image_map(map: &Value, x: i64, y: i64) -> Value {
    let Ok(entries) = map.to_vec() else {
        return Value::Nil;
    };
    entries
        .into_iter()
        .find(|entry| {
            entry
                .car()
                .is_ok_and(|area| image_hot_spot_contains(&area, x, y))
        })
        .unwrap_or(Value::Nil)
}

fn decode_live_frame(
    interp: &Interpreter,
    frame: Option<&Value>,
    nil_defaults_to_selected: bool,
) -> Result<Value, LispError> {
    match frame {
        None => Ok(interp.selected_frame_value()),
        Some(Value::Nil) if nil_defaults_to_selected => Ok(interp.selected_frame_value()),
        Some(Value::Frame(id)) if interp.frame_is_live(*id) => Ok(Value::Frame(*id)),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.clone())),
    }
}

fn require_live_terminal(interp: &Interpreter, value: Option<&Value>) -> Result<(), LispError> {
    if !interp.terminal_live() {
        return Err(wrong_type_argument(
            "terminal-live-p",
            value.cloned().unwrap_or(Value::Nil),
        ));
    }
    match value.unwrap_or(&Value::Nil) {
        Value::Nil => Ok(()),
        Value::Terminal(0) => Ok(()),
        Value::Frame(id) if interp.frame_is_live(*id) => Ok(()),
        value => Err(wrong_type_argument("terminal-live-p", value.clone())),
    }
}

fn current_frame_and_buffer_state(interp: &Interpreter) -> Vec<Value> {
    let mut state = Vec::new();
    if interp.terminal_live() {
        state.push(interp.selected_frame_value());
        state.push(
            interp
                .frame_parameter_override("name")
                .unwrap_or_else(|| interp.frame_name_value()),
        );
    }
    for (buffer_id, name) in &interp.buffer_list {
        if name.starts_with(' ') {
            continue;
        }
        let Some(buffer) = interp.get_buffer_by_id(*buffer_id) else {
            continue;
        };
        state.push(Value::buffer(*buffer_id, name.clone()));
        state.push(
            interp
                .buffer_local_value(*buffer_id, "buffer-read-only")
                .or_else(|| interp.default_value("buffer-read-only"))
                .unwrap_or(Value::Nil),
        );
        state.push(if buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        });
    }
    state
}

fn frame_or_buffer_changed(
    interp: &mut Interpreter,
    variable: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let variable_name = match variable {
        None | Some(Value::Nil) => None,
        Some(value) => Some(value.as_symbol()?.to_string()),
    };
    let old_state = if let Some(name) = variable_name.as_deref() {
        interp.lookup(name, env)?
    } else {
        interp.frame_and_buffer_state()
    };
    let current = current_frame_and_buffer_state(interp);
    let old_items = is_vector_value(&old_state)
        .then(|| vector_items(&old_state))
        .transpose()?;
    let unchanged = old_items.as_ref().is_some_and(|items| {
        items.len() > current.len()
            && current
                .iter()
                .zip(items)
                .all(|(new, old)| values_eq_in_env(interp, new, old, env))
            && matches!(
                items.get(current.len()),
                Some(Value::Symbol(symbol)) if symbol == "lambda"
            )
    });
    if unchanged {
        return Ok(Value::Nil);
    }

    let required = current.len() + 1;
    let reusable = old_items.as_ref().is_some_and(|items| {
        required <= items.len() && required.saturating_add(20) >= items.len() / 2
    });
    let state = if reusable {
        old_state
    } else {
        Value::list(
            std::iter::once(Value::symbol("vector-literal"))
                .chain(std::iter::repeat_n(Value::symbol("lambda"), required + 20)),
        )
    };
    for (index, value) in current.into_iter().enumerate() {
        aset_vector_value(&state, index, value)?;
    }
    let slots = vector_items(&state)?.len();
    for index in required - 1..slots {
        aset_vector_value(&state, index, Value::symbol("lambda"))?;
    }

    if !reusable {
        if let Some(name) = variable_name {
            interp.set_variable(&name, state, env);
        } else {
            interp.set_frame_and_buffer_state(state);
        }
    }
    Ok(Value::T)
}

fn window_id_or_selected(interp: &Interpreter, value: &Value) -> Result<u64, LispError> {
    if value.is_nil() {
        return Ok(interp.selected_window_id());
    }
    window_record_id_from_value(interp, value)
        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), value.clone()))
}

fn window_parameter_value(interp: &Interpreter, window_id: u64, parameter: &Value) -> Value {
    let Some(params) = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
    else {
        return Value::Nil;
    };
    let Ok(items) = params.to_vec() else {
        return Value::Nil;
    };
    for item in items {
        if let Ok(key) = item.car()
            && values_equal(interp, &key, parameter)
            && let Ok(value) = item.cdr()
        {
            return value;
        }
    }
    Value::Nil
}

fn set_window_parameter_value(
    interp: &mut Interpreter,
    window_id: u64,
    parameter: Value,
    value: Value,
) -> Result<Value, LispError> {
    let existing = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
        .cloned()
        .unwrap_or(Value::Nil);
    let mut items: Vec<Value> = existing.to_vec().unwrap_or_default();
    items.retain(|item| match item.car() {
        Ok(key) => !values_equal(interp, &key, &parameter),
        Err(_) => true,
    });
    if value.is_truthy() {
        items.push(Value::cons(parameter, value.clone()));
    }
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= WINDOW_PARAMETERS_SLOT {
        record.slots.resize(WINDOW_PARAMETERS_SLOT + 1, Value::Nil);
    }
    record.slots[WINDOW_PARAMETERS_SLOT] = Value::list(items);
    Ok(value)
}

fn window_slot_value(interp: &Interpreter, window_id: u64, slot: usize) -> Value {
    interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(slot))
        .cloned()
        .unwrap_or(Value::Nil)
}

fn set_window_slot_value(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
    value: Value,
) -> Result<Value, LispError> {
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= slot {
        record.slots.resize(slot + 1, Value::Nil);
    }
    record.slots[slot] = value.clone();
    Ok(value)
}

fn window_link(interp: &Interpreter, window_id: u64, slot: usize) -> Option<u64> {
    match window_slot_value(interp, window_id, slot) {
        Value::Record(id) => Some(id),
        _ => None,
    }
}

fn window_geometry(interp: &Interpreter, window_id: u64) -> (i64, i64, i64, i64) {
    let integer_slot = |slot, fallback| {
        window_slot_value(interp, window_id, slot)
            .as_integer()
            .unwrap_or(fallback)
    };
    (
        integer_slot(WINDOW_PIXEL_WIDTH_SLOT, interp.frame_width()),
        integer_slot(WINDOW_PIXEL_HEIGHT_SLOT, interp.frame_height()),
        integer_slot(WINDOW_PIXEL_LEFT_SLOT, 0),
        integer_slot(WINDOW_PIXEL_TOP_SLOT, 0),
    )
}

/// window.c's window_resize_apply: place WINDOW at (X, Y) and commit the
/// size staged in its new-pixel slot for the resized dimension (the
/// other dimension keeps its current size), then lay the children back
/// out — a vertical combination stacks them by their applied heights, a
/// horizontal one by their applied widths.  `window--resize' (window.el)
/// stages every affected window before calling this.
fn apply_staged_window_sizes(
    interp: &mut Interpreter,
    window_id: u64,
    horizontal: bool,
    x: i64,
    y: i64,
) -> Result<(i64, i64), LispError> {
    let (current_width, current_height, _, _) = window_geometry(interp, window_id);
    let staged = window_slot_value(interp, window_id, WINDOW_NEW_PIXEL_SLOT)
        .as_integer()
        .ok()
        .filter(|size| *size > 0)
        .unwrap_or(if horizontal {
            current_width
        } else {
            current_height
        });
    let (new_width, new_height) = if horizontal {
        (staged, current_height)
    } else {
        (current_width, staged)
    };
    set_window_geometry(interp, window_id, (new_width, new_height, x, y))?;
    let staged_normal = window_slot_value(interp, window_id, WINDOW_NEW_NORMAL_SLOT);
    if matches!(staged_normal, Value::Float(_) | Value::Integer(_)) {
        let slot = if horizontal {
            WINDOW_NORMAL_WIDTH_SLOT
        } else {
            WINDOW_NORMAL_HEIGHT_SLOT
        };
        set_window_slot_value(interp, window_id, slot, staged_normal)?;
    }
    let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
    let vertical_combination = matches!(
        kind,
        Value::Symbol(ref kind) if kind == INTERNAL_VERTICAL_WINDOW_KIND
    );
    let mut child = window_link(interp, window_id, WINDOW_FIRST_CHILD_SLOT);
    let (mut child_x, mut child_y) = (x, y);
    while let Some(child_id) = child {
        let (applied_width, applied_height) =
            apply_staged_window_sizes(interp, child_id, horizontal, child_x, child_y)?;
        if vertical_combination {
            child_y += applied_height;
        } else {
            child_x += applied_width;
        }
        child = window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT);
    }
    Ok((new_width, new_height))
}

fn set_window_geometry(
    interp: &mut Interpreter,
    window_id: u64,
    geometry: (i64, i64, i64, i64),
) -> Result<(), LispError> {
    for (slot, value) in [
        (WINDOW_PIXEL_WIDTH_SLOT, geometry.0),
        (WINDOW_PIXEL_HEIGHT_SLOT, geometry.1),
        (WINDOW_PIXEL_LEFT_SLOT, geometry.2),
        (WINDOW_PIXEL_TOP_SLOT, geometry.3),
    ] {
        set_window_slot_value(interp, window_id, slot, Value::Integer(value))?;
    }
    Ok(())
}

fn frame_root_window_value(interp: &Interpreter) -> Value {
    interp.root_window_value()
}

fn is_live_ordinary_window(interp: &Interpreter, id: u64) -> bool {
    let kind = window_slot_value(interp, id, WINDOW_KIND_SLOT);
    !matches!(
        kind,
        Value::Symbol(ref kind)
            if matches!(
                kind.as_str(),
                MINIBUFFER_WINDOW_KIND
                    | INTERNAL_HORIZONTAL_WINDOW_KIND
                    | INTERNAL_VERTICAL_WINDOW_KIND
                    | DELETED_WINDOW_KIND
            )
    ) && window_buffer_id(interp, &Value::Record(id)).is_some()
}

/// Leaf windows of the frame's window tree in GNU's canonical order — a
/// depth-first walk that descends first children and follows sibling
/// chains.  `next-window' cycles and `window-list' both follow it, and it
/// is the top-to-bottom, left-to-right order the glass paints.
fn window_tree_leaf_ids(interp: &Interpreter) -> Vec<u64> {
    let Value::Record(root_id) = frame_root_window_value(interp) else {
        return Vec::new();
    };
    // A malformed tree (stale sibling links) must not loop forever; no
    // healthy tree revisits a window record.
    let budget = interp.record_ids_by_type("window").len().saturating_add(1);
    let mut leaves = Vec::new();
    let mut stack = vec![root_id];
    let mut visited = 0usize;
    while let Some(id) = stack.pop() {
        visited += 1;
        if visited > budget {
            return Vec::new();
        }
        match window_link(interp, id, WINDOW_FIRST_CHILD_SLOT) {
            Some(child) => {
                let mut chain = Vec::new();
                let mut walk = Some(child);
                while let Some(node) = walk {
                    chain.push(node);
                    if chain.len() > budget {
                        return Vec::new();
                    }
                    walk = window_link(interp, node, WINDOW_NEXT_SIBLING_SLOT);
                }
                stack.extend(chain.into_iter().rev());
            }
            None => leaves.push(id),
        }
    }
    leaves
}

fn live_ordinary_window_ids(interp: &Interpreter) -> Vec<u64> {
    let from_tree: Vec<u64> = window_tree_leaf_ids(interp)
        .into_iter()
        .filter(|id| is_live_ordinary_window(interp, *id))
        .collect();
    if !from_tree.is_empty() {
        return from_tree;
    }
    // No usable tree (no root binding yet, or a degenerate walk): fall
    // back to record order, which equals tree order for a lone window.
    interp
        .record_ids_by_type("window")
        .into_iter()
        .filter(|id| is_live_ordinary_window(interp, *id))
        .collect()
}

/// One live window as the terminal frontend renders it: its frame rect,
/// buffer, and display anchors.  Geometry counts total lines and columns
/// — the mode line is the rect's last row, and a window not flush with
/// the frame's right edge spends its last column on the vertical border.
pub(crate) struct WindowRenderInfo {
    pub(crate) window_id: u64,
    pub(crate) buffer_id: u64,
    pub(crate) left: usize,
    pub(crate) top: usize,
    pub(crate) width: usize,
    pub(crate) height: usize,
    /// The window's display start (the window-start slot, clamped).
    pub(crate) start: usize,
    /// The window's own point: the live buffer point for the selected
    /// window, the saved window-point slot otherwise.
    pub(crate) point: usize,
    pub(crate) selected: bool,
}

/// The frame's live windows in paint order (the tree walk), with the
/// geometry and content anchors redisplay needs.
pub(crate) fn window_render_layout(interp: &Interpreter) -> Vec<WindowRenderInfo> {
    let selected_id = interp.selected_window_id();
    live_ordinary_window_ids(interp)
        .into_iter()
        .filter_map(|window_id| {
            let selected = window_id == selected_id;
            let buffer_id = window_buffer_id(interp, &Value::Record(window_id))?;
            let (width, height, left, top) = window_geometry(interp, window_id);
            let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
            let clamp = |value: i64| value.clamp(point_min as i64, point_max as i64) as usize;
            let start = if selected {
                interp.selected_window_start()
            } else {
                clamp(
                    window_slot_value(interp, window_id, WINDOW_START_SLOT)
                        .as_integer()
                        .unwrap_or(point_min as i64),
                )
            };
            let live_point = if buffer_id == interp.current_buffer_id() {
                interp.buffer.point()
            } else {
                interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.point())
                    .unwrap_or(point_min)
            };
            let point = if selected {
                live_point
            } else {
                window_slot_value(interp, window_id, WINDOW_POINT_SLOT)
                    .as_integer()
                    .map(clamp)
                    .unwrap_or(live_point)
            };
            Some(WindowRenderInfo {
                window_id,
                buffer_id,
                left: left.max(0) as usize,
                top: top.max(0) as usize,
                width: width.max(0) as usize,
                height: height.max(0) as usize,
                start,
                point,
                selected,
            })
        })
        .collect()
}

/// A face's realized tty attributes, the frontend's paint unit: ANSI
/// color indexes plus the boolean attributes a terminal can show.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub(crate) struct TtyFaceAttrs {
    pub(crate) foreground: Option<u8>,
    pub(crate) background: Option<u8>,
    pub(crate) bold: bool,
    pub(crate) underline: bool,
    pub(crate) reverse: bool,
    /// GNU's `:extend': the face keeps painting past the end of line to
    /// the window edge (the region's whole-row highlight).
    pub(crate) extend: bool,
}

/// xfaces.c tty_supports_face_attributes_p: whether the requested
/// attributes both render differently from the default face and lie
/// within the terminal's capabilities.  The capability set mirrors GNU
/// on the frontend's terminal class (TERM=xterm family): bold, italic,
/// inverse video, and strike-through pass; dim and underline fail the
/// capability test there; family, foundry, stipple, height, width,
/// overline, and box never exist on a tty.  Color names must resolve
/// on the terminal's palette and differ from the default face's.
fn tty_supports_face_attributes(
    interp: &mut Interpreter,
    env: &mut Env,
    attributes: &Value,
) -> Result<Value, LispError> {
    let mut pairs: Vec<(String, Value)> = Vec::new();
    // merge_face_ref accepts both a bare plist and a precedence-ordered
    // list of face references.  `supports' display conditions pass their
    // cdr verbatim, so the widespread `(supports (:box t))' spelling
    // arrives here as the one-element face-reference list `((:box t))'.
    // Walk those lists just as merge_face_ref does instead of mistaking an
    // empty top-level plist for "every requested attribute is supported".
    let mut pending = vec![attributes.clone()];
    while let Some(reference) = pending.pop() {
        let Ok(items) = reference.to_vec() else {
            continue;
        };
        if !matches!(items.first(), Some(Value::Symbol(name)) if name.starts_with(':')) {
            pending.extend(items.into_iter().rev());
            continue;
        }
        for pair in items.chunks_exact(2) {
            if let Value::Symbol(key) = &pair[0] {
                pairs.push((key.to_string(), pair[1].clone()));
            }
        }
    }
    let lookup = |wanted: &str| {
        pairs
            .iter()
            .find(|(key, _)| key == wanted)
            .map(|(_, value)| value.clone())
    };
    for (key, _) in &pairs {
        if matches!(
            key.as_str(),
            ":family" | ":foundry" | ":stipple" | ":height" | ":width" | ":overline" | ":box"
        ) {
            return Ok(Value::Nil);
        }
    }
    if let Some(weight) = lookup(":weight") {
        // FONT_WEIGHT_NAME_NUMERIC: heavier than normal needs bold
        // (supported), lighter needs dim (not on this terminal), and
        // normal weights are the default's own.
        match weight.as_symbol().unwrap_or("") {
            "semi-bold" | "bold" | "extra-bold" | "ultra-bold" | "heavy" | "ultra-heavy"
            | "black" => {}
            "thin" | "ultra-light" | "extra-light" | "light" | "semi-light" => {
                return Ok(Value::Nil);
            }
            "normal" | "medium" | "regular" | "book" => return Ok(Value::Nil),
            _ => {}
        }
    }
    if let Some(slant) = lookup(":slant") {
        // Italic and oblique differ from the default's normal slant and
        // the terminal renders italics.
        if matches!(slant.as_symbol().unwrap_or(""), "normal") {
            return Ok(Value::Nil);
        }
    }
    if lookup(":underline").is_some() {
        // The capability test fails for underline on this terminal
        // class, styled variants included.
        return Ok(Value::Nil);
    }
    for color_key in [":foreground", ":background"] {
        if let Some(color) = lookup(color_key) {
            if !color.is_string() {
                return Ok(Value::Nil);
            }
            let default_color = interp
                .call_function_value(
                    Value::Symbol("face-attribute".into()),
                    Some("face-attribute"),
                    &[
                        Value::Symbol("default".into()),
                        Value::Symbol(color_key.into()),
                    ],
                    env,
                )
                .ok();
            // face_attr_equal_p compares color strings case-insensitively.
            let text_of = |value: &Value| -> Option<String> {
                match value {
                    Value::String(text) => Some(text.to_string()),
                    Value::StringObject(state) => {
                        Some(std::cell::RefCell::borrow(state).text.clone())
                    }
                    _ => None,
                }
            };
            if let (Some(default_text), Some(color_text)) =
                (default_color.as_ref().and_then(text_of), text_of(&color))
                && default_text.eq_ignore_ascii_case(&color_text)
            {
                return Ok(Value::Nil);
            }
            let index = interp
                .call_function_value(
                    Value::Symbol("tty-color-translate".into()),
                    None,
                    std::slice::from_ref(&color),
                    env,
                )?
                .as_integer()
                .unwrap_or(-1);
            if index < 0 {
                return Ok(Value::Nil);
            }
        }
    }
    Ok(Value::T)
}

/// Resolve FACE to its tty attributes through the runtime's own face
/// machinery: `face-attribute' answers the realized (inherit-merged)
/// attributes, and `tty-color-translate' maps color names onto the
/// terminal's ANSI palette exactly as GNU's tty color support does.
pub(crate) fn resolve_tty_face_attrs(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
) -> TtyFaceAttrs {
    // face-remap.el installs buffer-local substitutions such as Magit's
    // `(header-line magit-header-line header-line)'.  Redisplay realizes
    // the cdr as the effective precedence-ordered face list; the trailing
    // base face is deliberately resolved without recursively remapping it.
    if let Value::Symbol(requested) = face
        && let Some(remapped) = interp
            .lookup_var("face-remapping-alist", env)
            .and_then(|alist| alist.to_vec().ok())
            .and_then(|entries| {
                entries.into_iter().find_map(|entry| {
                    let key = entry.car().ok()?;
                    matches!(&key, Value::Symbol(name) if name == requested)
                        .then(|| entry.cdr().ok())?
                })
            })
    {
        let options = resolve_tty_face_reference_options(interp, env, &remapped, 0);
        return TtyFaceAttrs {
            foreground: options.foreground,
            background: options.background,
            bold: options.bold.unwrap_or(false),
            underline: options.underline.unwrap_or(false),
            reverse: options.reverse.unwrap_or(false),
            extend: options.extend.unwrap_or(false),
        };
    }
    // xfaces.c merge_face_ref: a face reference may be a LIST of face
    // references, merged left to right with the entries nearer the
    // front taking precedence (comint's prompt carries
    // `(comint-highlight-prompt comint-highlight-prompt)').  Realize
    // each member and fold, letting an earlier member's set attributes
    // override a later one's.
    if matches!(face, Value::Cons(_)) {
        let options = resolve_tty_face_reference_options(interp, env, face, 0);
        return TtyFaceAttrs {
            foreground: options.foreground,
            background: options.background,
            bold: options.bold.unwrap_or(false),
            underline: options.underline.unwrap_or(false),
            reverse: options.reverse.unwrap_or(false),
            extend: options.extend.unwrap_or(false),
        };
    }
    // `face-attribute' is GNU faces.el's; reach it through the ordinary
    // function cell, never the native dispatcher.
    let attribute = |interp: &mut Interpreter, env: &mut Env, name: &str| {
        interp
            .call_function_value(
                Value::Symbol("face-attribute".into()),
                Some("face-attribute"),
                &[
                    face.clone(),
                    Value::Symbol(name.into()),
                    Value::Nil,
                    Value::T,
                ],
                env,
            )
            .ok()
            .filter(|value| !matches!(value, Value::Symbol(s) if s == "unspecified"))
            .filter(|value| !value.is_nil())
    };
    let color_index = |interp: &mut Interpreter, env: &mut Env, value: Option<Value>| {
        tty_face_color_index(interp, env, value.as_ref()?)
    };
    let foreground = attribute(interp, env, ":foreground");
    let background = attribute(interp, env, ":background");
    TtyFaceAttrs {
        foreground: color_index(interp, env, foreground),
        background: color_index(interp, env, background),
        bold: attribute(interp, env, ":weight")
            .is_some_and(|weight| matches!(weight, Value::Symbol(s) if s == "bold" || s == "semi-bold" || s == "extra-bold" || s == "ultra-bold")),
        underline: attribute(interp, env, ":underline").is_some_and(|value| value.is_truthy()),
        reverse: attribute(interp, env, ":inverse-video").is_some_and(|value| value.is_truthy()),
        extend: attribute(interp, env, ":extend").is_some_and(|value| value.is_truthy()),
    }
}

/// One face member's tty attributes with per-attribute specificity: a
/// `None' means the face leaves that attribute unspecified (inherit
/// merged), a `Some' is an explicit value — explicit nil included, so
/// a list merge can turn attributes off.
#[derive(Default)]
pub(crate) struct TtyFaceAttrOptions {
    pub(crate) foreground: Option<u8>,
    pub(crate) background: Option<u8>,
    pub(crate) bold: Option<bool>,
    pub(crate) underline: Option<bool>,
    pub(crate) reverse: Option<bool>,
    pub(crate) extend: Option<bool>,
}

fn merge_tty_face_options(base: &mut TtyFaceAttrOptions, overlay: TtyFaceAttrOptions) {
    if overlay.foreground.is_some() {
        base.foreground = overlay.foreground;
    }
    if overlay.background.is_some() {
        base.background = overlay.background;
    }
    if overlay.bold.is_some() {
        base.bold = overlay.bold;
    }
    if overlay.underline.is_some() {
        base.underline = overlay.underline;
    }
    if overlay.reverse.is_some() {
        base.reverse = overlay.reverse;
    }
    if overlay.extend.is_some() {
        base.extend = overlay.extend;
    }
}

/// Translate one realized tty face color through tty-colors.el's registered
/// palette and approximation machinery.
fn tty_face_color_index(interp: &mut Interpreter, env: &mut Env, value: &Value) -> Option<u8> {
    string_text(value).ok()?;
    interp
        .call_function_value(
            Value::Symbol("tty-color-translate".into()),
            None,
            std::slice::from_ref(value),
            env,
        )
        .ok()
        .and_then(|index| index.as_integer().ok())
        .and_then(|index| u8::try_from(index).ok())
}

/// Resolve the named, precedence-ordered list, and anonymous attribute
/// plist face-reference forms used by the TTY renderer.  Propertized
/// strings use the last form heavily (Flymake wraps its compilation face
/// in `(:inherit ((FACE) default))').
fn resolve_tty_face_reference_options(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
    depth: usize,
) -> TtyFaceAttrOptions {
    if depth >= 32 {
        return TtyFaceAttrOptions::default();
    }
    let Value::Cons(_) = face else {
        return resolve_tty_face_attr_options(interp, env, face);
    };
    let Ok(items) = face.to_vec() else {
        return TtyFaceAttrOptions::default();
    };
    if !matches!(items.first(), Some(Value::Symbol(name)) if name.starts_with(':')) {
        let mut merged = TtyFaceAttrOptions::default();
        for item in items.iter().rev() {
            let options = resolve_tty_face_reference_options(interp, env, item, depth + 1);
            merge_tty_face_options(&mut merged, options);
        }
        return merged;
    }

    let mut pairs = items.chunks_exact(2);
    let inherited = pairs
        .clone()
        .find(|pair| matches!(&pair[0], Value::Symbol(name) if name == ":inherit"))
        .map(|pair| &pair[1])
        .filter(|value| value.is_truthy());
    let mut merged = inherited
        .map(|value| resolve_tty_face_reference_options(interp, env, value, depth + 1))
        .unwrap_or_default();
    let color_index = tty_face_color_index;
    for pair in &mut pairs {
        let Value::Symbol(name) = &pair[0] else {
            continue;
        };
        let value = &pair[1];
        if matches!(value, Value::Symbol(name) if name == "unspecified") {
            continue;
        }
        match name.as_ref() {
            ":foreground" => merged.foreground = color_index(interp, env, value),
            ":background" => merged.background = color_index(interp, env, value),
            ":weight" => {
                merged.bold = Some(matches!(value, Value::Symbol(weight)
                    if weight == "bold"
                        || weight == "semi-bold"
                        || weight == "extra-bold"
                        || weight == "ultra-bold"));
            }
            ":underline" => merged.underline = Some(value.is_truthy()),
            ":inverse-video" => merged.reverse = Some(value.is_truthy()),
            ":extend" => merged.extend = Some(value.is_truthy()),
            _ => {}
        }
    }
    merged
}

fn resolve_tty_face_attr_options(
    interp: &mut Interpreter,
    env: &mut Env,
    face: &Value,
) -> TtyFaceAttrOptions {
    let attribute = |interp: &mut Interpreter, env: &mut Env, name: &str| {
        interp
            .call_function_value(
                Value::Symbol("face-attribute".into()),
                Some("face-attribute"),
                &[
                    face.clone(),
                    Value::Symbol(name.into()),
                    Value::Nil,
                    Value::T,
                ],
                env,
            )
            .ok()
            .filter(|value| !matches!(value, Value::Symbol(s) if s == "unspecified"))
    };
    let color_index = |interp: &mut Interpreter, env: &mut Env, value: Option<Value>| {
        tty_face_color_index(interp, env, value.as_ref()?)
    };
    let foreground = attribute(interp, env, ":foreground").filter(|value| !value.is_nil());
    let background = attribute(interp, env, ":background").filter(|value| !value.is_nil());
    TtyFaceAttrOptions {
        foreground: color_index(interp, env, foreground),
        background: color_index(interp, env, background),
        bold: attribute(interp, env, ":weight").map(|weight| {
            matches!(weight, Value::Symbol(ref s)
                if s == "bold" || s == "semi-bold" || s == "extra-bold" || s == "ultra-bold")
        }),
        underline: attribute(interp, env, ":underline").map(|value| value.is_truthy()),
        reverse: attribute(interp, env, ":inverse-video").map(|value| value.is_truthy()),
        extend: attribute(interp, env, ":extend").map(|value| value.is_truthy()),
    }
}

/// The face spans redisplay paints over a window's text in [FROM, TO):
/// buffer `face' text properties first, the selected window's active
/// region above them, and overlay faces last in ascending priority —
/// the stacking GNU's face merging produces (isearch's priority-1001
/// overlay wins over the region, which wins over font-lock properties).
pub(crate) fn window_face_spans(
    interp: &mut Interpreter,
    env: &mut Env,
    buffer_id: u64,
    from: usize,
    to: usize,
    region_for_selected: bool,
) -> Vec<(usize, usize, Value)> {
    let mut spans = Vec::new();
    let is_current = buffer_id == interp.current_buffer_id();
    let mut overlay_spans: Vec<(i64, u64, usize, usize, Value)> = Vec::new();
    {
        let buffer = if is_current {
            &interp.buffer
        } else {
            match interp.get_buffer_by_id(buffer_id) {
                Some(buffer) => buffer,
                None => return spans,
            }
        };
        // font-core.el:165: font-lock-mode installs `font-lock-face' as
        // an alias of `face' in the buffer-local
        // `char-property-alias-alist'; the display engine resolves the
        // face for each position through the same alias chain textget
        // uses, which is how comint's font-lock-face-only output reaches
        // the glass.
        let face_alias_names: Vec<String> = interp
            .buffer_local_value(buffer_id, "char-property-alias-alist")
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                let key = entry.car().ok()?;
                matches!(&key, Value::Symbol(name) if name == "face")
                    .then(|| entry.cdr().ok()?.to_vec().ok())?
            })
            .flatten()
            .filter_map(|alias| alias.as_symbol().ok().map(str::to_string))
            .collect();
        // The common frame — a plain buffer, no overlays, no active
        // region — must not pay a per-character property walk on every
        // redisplay.
        if !buffer.has_text_property_named("face")
            && !buffer.has_text_property_named("category")
            && face_alias_names
                .iter()
                .all(|name| !buffer.has_text_property_named(name))
            && buffer.overlays.iter().all(|overlay| overlay.is_dead())
            && !(region_for_selected
                && is_current
                && interp
                    .lookup_var("mark-active", env)
                    .is_some_and(|active| active.is_truthy()))
        {
            return spans;
        }
        let face_at = |pos: usize| {
            crate::lisp::primitives::strings::buffer_property_at_with_category(
                interp, buffer, pos, "face",
            )
            .filter(|face| !face.is_nil())
            .or_else(|| {
                face_alias_names.iter().find_map(|name| {
                    crate::lisp::primitives::strings::buffer_property_at_with_category(
                        interp, buffer, pos, name,
                    )
                    .filter(|face| !face.is_nil())
                })
            })
        };
        let mut pos = from;
        while pos < to {
            let face = face_at(pos);
            let mut end = pos + 1;
            while end < to && face_at(end) == face {
                end += 1;
            }
            if let Some(face) = face {
                spans.push((pos, end, face));
            }
            pos = end;
        }
        for overlay in &buffer.overlays {
            if overlay.is_dead() || overlay.beg >= to || overlay.end <= from {
                continue;
            }
            let Some(face) = overlay
                .get_prop(&Value::Symbol("face".into()))
                .filter(|face| !face.is_nil())
                .or_else(|| {
                    face_alias_names.iter().find_map(|name| {
                        overlay
                            .get_prop(&Value::Symbol(name.clone().into()))
                            .filter(|face| !face.is_nil())
                    })
                })
                .cloned()
            else {
                continue;
            };
            let priority = overlay
                .get_prop(&Value::Symbol("priority".into()))
                .and_then(|priority| priority.as_integer().ok())
                .unwrap_or(0);
            overlay_spans.push((
                priority,
                overlay.id,
                overlay.beg.max(from),
                overlay.end.min(to),
                face,
            ));
        }
    }
    // The active region, redisplay's own quasi-overlay under real ones.
    if region_for_selected
        && is_current
        && interp
            .lookup_var("transient-mark-mode", env)
            .is_some_and(|mode| mode.is_truthy())
        && interp
            .lookup_var("mark-active", env)
            .is_some_and(|active| active.is_truthy())
        // The buffer's mark marker is native buffer.c state; `mark' itself
        // is GNU simple.el's and must not enter the native dispatcher.
        && let Some(mark) = interp.buffer.mark()
    {
        let point = interp.buffer.point();
        let mark = mark.max(1);
        let (beg, end) = if mark <= point {
            (mark, point)
        } else {
            (point, mark)
        };
        let beg = beg.max(from);
        let end = end.min(to);
        if beg < end {
            spans.push((beg, end, Value::Symbol("region".into())));
        }
    }
    overlay_spans.sort_by_key(|(priority, id, ..)| (*priority, *id));
    spans.extend(
        overlay_spans
            .into_iter()
            .map(|(_, _, beg, end, face)| (beg, end, face)),
    );
    spans
}

/// A window's mode line as the display engine paints it.  GNU renders
/// each mode line with that window selected and its buffer current
/// (display_mode_line's context); this swaps both in — plus the window's
/// point and live metrics — and restores every piece afterwards.
pub(crate) fn render_window_mode_line(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    point: usize,
    metrics: InteractiveWindowMetrics,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(interp, env, window_id, point, metrics, "mode-line-format")
}

/// The window's header line, rendered by the same machinery as its mode
/// line (xdisp.c's display_mode_lines drives both through
/// display_mode_line with the window's `header-line-format').
pub(crate) fn render_window_header_line(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    point: usize,
    metrics: InteractiveWindowMetrics,
) -> Result<(String, FaceSpans), LispError> {
    render_window_line_with_format(interp, env, window_id, point, metrics, "header-line-format")
}

fn render_window_line_with_format(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    point: usize,
    metrics: InteractiveWindowMetrics,
    format_variable: &str,
) -> Result<(String, FaceSpans), LispError> {
    let saved_window = interp.selected_window_id();
    let saved_buffer = interp.current_buffer_id();
    let saved_metrics = interactive_window_metrics();
    let buffer_id = window_buffer_id(interp, &Value::Record(window_id)).unwrap_or(saved_buffer);
    interp.set_selected_window_id(window_id);
    let switched = buffer_id != saved_buffer && interp.set_current_buffer_id(buffer_id).is_ok();
    let saved_point = interp.buffer.point();
    interp.buffer.goto_char(point);
    set_interactive_window_metrics(Some(metrics));
    let result = (|| {
        let format = interp
            .lookup_var(format_variable, env)
            .unwrap_or(Value::Nil);
        let mut spans = Vec::new();
        let text = render_mode_line_element(interp, env, &format, false, true, 0, 0, &mut spans)?;
        Ok((text, spans))
    })();
    interp.buffer.goto_char(saved_point);
    if switched {
        let _ = interp.set_current_buffer_id(saved_buffer);
    }
    interp.set_selected_window_id(saved_window);
    set_interactive_window_metrics(saved_metrics);
    result
}

fn live_window_id_or_selected(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<u64, LispError> {
    let window = value
        .filter(|window| !window.is_nil())
        .cloned()
        .unwrap_or_else(|| interp.selected_window_value());
    let window_id = window_record_id_from_value(interp, &window)
        .ok_or_else(|| LispError::TypeError("window-live-p".into(), window.type_name()))?;
    let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
    if matches!(
        kind,
        Value::Symbol(ref kind)
            if matches!(
                kind.as_str(),
                INTERNAL_HORIZONTAL_WINDOW_KIND
                    | INTERNAL_VERTICAL_WINDOW_KIND
                    | DELETED_WINDOW_KIND
            )
    ) || window_buffer_id(interp, &window).is_none()
    {
        return Err(LispError::TypeError(
            "window-live-p".into(),
            window.type_name(),
        ));
    }
    Ok(window_id)
}

fn select_window_value(
    interp: &mut Interpreter,
    window: &Value,
    norecord: bool,
) -> Result<Value, LispError> {
    let window_id = live_window_id_or_selected(interp, Some(window))?;
    let previous_window_id = interp.selected_window_id();
    if window_id != previous_window_id {
        let previous_buffer_id = interp.selected_window_buffer_id();
        let previous_point = if previous_buffer_id == interp.current_buffer_id() {
            interp.buffer.point()
        } else {
            interp
                .get_buffer_by_id(previous_buffer_id)
                .map(|buffer| buffer.point())
                .unwrap_or(1)
        };
        set_window_slot_value(
            interp,
            previous_window_id,
            WINDOW_POINT_SLOT,
            Value::Integer(previous_point as i64),
        )?;
    }
    // The selected window's live point is the point of its current buffer;
    // its saved window-point slot is only authoritative while unselected.
    // In particular, reselecting the already-selected window must not rewind
    // point to a stale slot (GNU's `save-selected-window' relies on this).
    let target_buffer_id = window_buffer_id(interp, window);
    let target_point = if window_id == previous_window_id
        && target_buffer_id == Some(interp.current_buffer_id())
    {
        interp.buffer.point()
    } else {
        window_slot_value(interp, window_id, WINDOW_POINT_SLOT)
            .as_integer()
            .unwrap_or(1)
            .max(1) as usize
    };
    interp.set_selected_window_id(window_id);
    if !norecord {
        interp.record_window_selection(window_id);
    }
    if let Some(buffer_id) = target_buffer_id
        && interp.has_buffer_id(buffer_id)
    {
        interp.switch_to_buffer_id_preserving_window_history(buffer_id)?;
        interp.buffer.goto_char(target_point);
        set_window_slot_value(
            interp,
            window_id,
            WINDOW_POINT_SLOT,
            Value::Integer(interp.buffer.point() as i64),
        )?;
        if !norecord {
            interp.record_buffer_front(buffer_id);
        }
    }
    Ok(window.clone())
}

fn split_window_tree(
    interp: &mut Interpreter,
    old: &Value,
    pixel_size: &Value,
    side: &Value,
    normal_size: &Value,
) -> Result<Value, LispError> {
    let old_id = window_id_or_selected(interp, old)?;
    let kind = window_slot_value(interp, old_id, WINDOW_KIND_SLOT);
    if matches!(kind, Value::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND)
        || window_buffer_id(interp, &Value::Record(old_id)).is_none()
    {
        return Err(LispError::Signal(
            "Attempt to split a non-live window".into(),
        ));
    }
    let horizontal = matches!(side, Value::T)
        || matches!(side, Value::Symbol(side) if matches!(side.as_str(), "left" | "right"));
    let before = matches!(side, Value::Symbol(side) if matches!(side.as_str(), "above" | "left"));
    let requested = pixel_size.as_integer()?;
    let (width, height, left, top) = window_geometry(interp, old_id);
    let available = if horizontal { width } else { height };
    if requested <= 0 || requested >= available {
        return Err(LispError::Signal(
            "Size of new window too small (after split)".into(),
        ));
    }
    let old_parent = window_link(interp, old_id, WINDOW_PARENT_SLOT);
    let outside_prev = window_link(interp, old_id, WINDOW_PREV_SIBLING_SLOT);
    let outside_next = window_link(interp, old_id, WINDOW_NEXT_SIBLING_SLOT);
    let buffer_id = window_buffer_id(interp, &Value::Record(old_id))
        .ok_or_else(|| LispError::Signal("Attempt to split a non-live window".into()))?;
    let start = window_slot_value(interp, old_id, WINDOW_START_SLOT)
        .as_integer()
        .unwrap_or(1)
        .max(1) as usize;

    let (old_geometry, new_geometry) = if horizontal {
        let old_width = available - requested;
        if before {
            (
                (old_width, height, left + requested, top),
                (requested, height, left, top),
            )
        } else {
            (
                (old_width, height, left, top),
                (requested, height, left + old_width, top),
            )
        }
    } else {
        let old_height = available - requested;
        if before {
            (
                (width, old_height, left, top + requested),
                (width, requested, left, top),
            )
        } else {
            (
                (width, old_height, left, top),
                (width, requested, left, top + old_height),
            )
        }
    };

    let parent_kind = if horizontal {
        INTERNAL_HORIZONTAL_WINDOW_KIND
    } else {
        INTERNAL_VERTICAL_WINDOW_KIND
    };
    let parent = interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Window,
        "window",
        window_record_slots(
            None,
            1,
            Value::Symbol(parent_kind.into()),
            (width, height, left, top),
        ),
    );
    let Value::Record(parent_id) = parent else {
        unreachable!("window records use Value::Record");
    };
    let new = interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Window,
        "window",
        window_record_slots(Some(buffer_id), start, Value::Nil, new_geometry),
    );
    let Value::Record(new_id) = new else {
        unreachable!("window records use Value::Record");
    };

    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_PARENT_SLOT,
        old_parent.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_PREV_SIBLING_SLOT,
        outside_prev.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_NEXT_SIBLING_SLOT,
        outside_next.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    if let Some(parent) = old_parent
        && window_link(interp, parent, WINDOW_FIRST_CHILD_SLOT) == Some(old_id)
    {
        set_window_slot_value(
            interp,
            parent,
            WINDOW_FIRST_CHILD_SLOT,
            Value::Record(parent_id),
        )?;
    }
    if let Some(previous) = outside_prev {
        set_window_slot_value(
            interp,
            previous,
            WINDOW_NEXT_SIBLING_SLOT,
            Value::Record(parent_id),
        )?;
    }
    if let Some(next) = outside_next {
        set_window_slot_value(
            interp,
            next,
            WINDOW_PREV_SIBLING_SLOT,
            Value::Record(parent_id),
        )?;
    }
    for window_id in [old_id, new_id] {
        set_window_slot_value(
            interp,
            window_id,
            WINDOW_PARENT_SLOT,
            Value::Record(parent_id),
        )?;
    }
    let (first, second) = if before {
        (new_id, old_id)
    } else {
        (old_id, new_id)
    };
    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_FIRST_CHILD_SLOT,
        Value::Record(first),
    )?;
    set_window_slot_value(interp, first, WINDOW_PREV_SIBLING_SLOT, Value::Nil)?;
    set_window_slot_value(
        interp,
        first,
        WINDOW_NEXT_SIBLING_SLOT,
        Value::Record(second),
    )?;
    set_window_slot_value(
        interp,
        second,
        WINDOW_PREV_SIBLING_SLOT,
        Value::Record(first),
    )?;
    set_window_slot_value(interp, second, WINDOW_NEXT_SIBLING_SLOT, Value::Nil)?;
    set_window_slot_value(interp, new_id, WINDOW_NEW_NORMAL_SLOT, normal_size.clone())?;
    for (window_id, pixel_size) in [
        (parent_id, available),
        (old_id, available - requested),
        (new_id, requested),
    ] {
        set_window_slot_value(
            interp,
            window_id,
            WINDOW_NEW_PIXEL_SLOT,
            Value::Integer(pixel_size),
        )?;
        set_window_slot_value(
            interp,
            window_id,
            WINDOW_NEW_TOTAL_SLOT,
            Value::Integer(pixel_size),
        )?;
    }
    set_window_geometry(interp, old_id, old_geometry)?;
    if old_parent.is_none() {
        interp.set_root_window_id(parent_id);
    }
    Ok(Value::Record(new_id))
}

fn delete_window_from_tree(interp: &mut Interpreter, window_id: u64) -> Result<(), LispError> {
    let parent_id = window_link(interp, window_id, WINDOW_PARENT_SLOT)
        .ok_or_else(|| LispError::Signal("Attempt to delete sole ordinary window".into()))?;
    let sibling_id = window_link(interp, window_id, WINDOW_PREV_SIBLING_SLOT)
        .or_else(|| window_link(interp, window_id, WINDOW_NEXT_SIBLING_SLOT))
        .ok_or_else(|| LispError::Signal("Attempt to delete sole window of parent".into()))?;
    let grandparent = window_link(interp, parent_id, WINDOW_PARENT_SLOT);
    let outside_prev = window_link(interp, parent_id, WINDOW_PREV_SIBLING_SLOT);
    let outside_next = window_link(interp, parent_id, WINDOW_NEXT_SIBLING_SLOT);
    let parent_geometry = window_geometry(interp, parent_id);
    let replacement = (interp.selected_window_id() == window_id).then(|| {
        let point = window_slot_value(interp, sibling_id, WINDOW_POINT_SLOT)
            .as_integer()
            .unwrap_or(1)
            .max(1) as usize;
        (sibling_id, point)
    });

    if let Some(grandparent_id) = grandparent
        && window_link(interp, grandparent_id, WINDOW_FIRST_CHILD_SLOT) == Some(parent_id)
    {
        set_window_slot_value(
            interp,
            grandparent_id,
            WINDOW_FIRST_CHILD_SLOT,
            Value::Record(sibling_id),
        )?;
    }
    if let Some(previous) = outside_prev {
        set_window_slot_value(
            interp,
            previous,
            WINDOW_NEXT_SIBLING_SLOT,
            Value::Record(sibling_id),
        )?;
    }
    if let Some(next) = outside_next {
        set_window_slot_value(
            interp,
            next,
            WINDOW_PREV_SIBLING_SLOT,
            Value::Record(sibling_id),
        )?;
    }
    set_window_slot_value(
        interp,
        sibling_id,
        WINDOW_PARENT_SLOT,
        grandparent.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        sibling_id,
        WINDOW_PREV_SIBLING_SLOT,
        outside_prev.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        sibling_id,
        WINDOW_NEXT_SIBLING_SLOT,
        outside_next.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_geometry(interp, sibling_id, parent_geometry)?;
    for deleted_id in [window_id, parent_id] {
        set_window_slot_value(
            interp,
            deleted_id,
            WINDOW_KIND_SLOT,
            Value::Symbol(DELETED_WINDOW_KIND.into()),
        )?;
        set_window_slot_value(interp, deleted_id, WINDOW_BUFFER_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_PARENT_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_PREV_SIBLING_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_NEXT_SIBLING_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_FIRST_CHILD_SLOT, Value::Nil)?;
    }
    if grandparent.is_none() {
        interp.set_root_window_id(sibling_id);
    }
    if let Some((replacement_id, point)) = replacement {
        interp.set_selected_window_id(replacement_id);
        if let Some(buffer_id) = window_buffer_id(interp, &Value::Record(replacement_id)) {
            interp.switch_to_buffer_id_preserving_window_history(buffer_id)?;
            interp.buffer.goto_char(point);
        }
    }
    Ok(())
}

fn delete_other_windows_from_tree(
    interp: &mut Interpreter,
    window_id: u64,
) -> Result<(), LispError> {
    let root = frame_root_window_value(interp);
    let root_id = window_id_or_selected(interp, &root)?;
    let root_geometry = window_geometry(interp, root_id);
    for id in interp.record_ids_by_type("window") {
        if id == window_id {
            continue;
        }
        let kind = window_slot_value(interp, id, WINDOW_KIND_SLOT);
        if !matches!(kind, Value::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND) {
            set_window_slot_value(
                interp,
                id,
                WINDOW_KIND_SLOT,
                Value::Symbol(DELETED_WINDOW_KIND.into()),
            )?;
            set_window_slot_value(interp, id, WINDOW_BUFFER_SLOT, Value::Nil)?;
        }
    }
    set_window_slot_value(interp, window_id, WINDOW_PARENT_SLOT, Value::Nil)?;
    set_window_slot_value(interp, window_id, WINDOW_PREV_SIBLING_SLOT, Value::Nil)?;
    set_window_slot_value(interp, window_id, WINDOW_NEXT_SIBLING_SLOT, Value::Nil)?;
    set_window_geometry(interp, window_id, root_geometry)?;
    interp.set_root_window_id(window_id);
    Ok(())
}

fn window_buffer_id_or_selected(
    interp: &Interpreter,
    window: Option<&Value>,
) -> Result<u64, LispError> {
    match window {
        None | Some(Value::Nil) => Ok(interp.selected_window_buffer_id()),
        Some(window) => {
            window_id_or_selected(interp, window)?;
            window_buffer_id(interp, window)
                .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), window.clone()))
        }
    }
}

fn window_line_height(
    interp: &Interpreter,
    buffer_id: u64,
    format_variable: &str,
    env: &Env,
) -> i64 {
    let format = if buffer_id == interp.current_buffer_id() {
        interp.lookup_var(format_variable, env)
    } else {
        interp
            .buffer_local_toplevel_value(buffer_id, format_variable)
            .or_else(|| interp.global_binding_value(format_variable))
    };
    i64::from(format.is_some_and(|format| !format.is_nil()))
}

fn window_non_body_height(interp: &Interpreter, buffer_id: u64, env: &Env) -> i64 {
    ["mode-line-format", "header-line-format", "tab-line-format"]
        .into_iter()
        .map(|variable| window_line_height(interp, buffer_id, variable, env))
        .sum()
}

fn window_text_width_columns(interp: &Interpreter, window_id: u64) -> i64 {
    let (total, _, left, _) = window_geometry(interp, window_id);
    let root_id = match interp.root_window_value() {
        Value::Record(id) => id,
        _ => window_id,
    };
    let (root_width, _, root_left, _) = window_geometry(interp, root_id);
    // GNU reserves one terminal column for the vertical separator of every
    // live window whose right edge is not the root window's right edge.  The
    // separator is part of WINDOW-TOTAL-WIDTH but not WINDOW-BODY-WIDTH (see
    // window_body_width in window.c).  Emaxx currently exposes terminal
    // frames, so there are no GUI fringes or scroll-bar pixels to subtract.
    let vertical_separator =
        i64::from(left.saturating_add(total) < root_left.saturating_add(root_width));
    let (left, right) = interp.window_margins(window_id);
    (total - vertical_separator - left.unwrap_or(0) - right.unwrap_or(0)).max(0)
}

/// The per-window horizontal-scroll state xdisp.c's hscroll_window_tree
/// reads and writes: w->hscroll, w->min_hscroll, w->suspend_auto_hscroll,
/// and w->old_pointm (the point the last redisplay saw, which decides
/// when an explicit scroll's suspension lifts).
pub(crate) struct WindowHscrollState {
    pub(crate) hscroll: i64,
    pub(crate) min_hscroll: i64,
    pub(crate) suspended: bool,
    pub(crate) old_point: Option<i64>,
}

pub(crate) fn window_hscroll_state(interp: &Interpreter, window_id: u64) -> WindowHscrollState {
    WindowHscrollState {
        hscroll: window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
            .as_integer()
            .unwrap_or(0)
            .max(0),
        min_hscroll: window_slot_value(interp, window_id, WINDOW_MIN_HSCROLL_SLOT)
            .as_integer()
            .unwrap_or(0)
            .max(0),
        suspended: window_slot_value(interp, window_id, WINDOW_SUSPEND_AUTO_HSCROLL_SLOT)
            .is_truthy(),
        old_point: window_slot_value(interp, window_id, WINDOW_OLD_POINT_SLOT)
            .as_integer()
            .ok(),
    }
}

/// hscroll_window_tree's write-back: the recomputed hscroll, the
/// (possibly lifted) suspension, and the remembered window point.
pub(crate) fn store_window_hscroll_state(
    interp: &mut Interpreter,
    window_id: u64,
    hscroll: i64,
    suspended: bool,
    old_point: i64,
) {
    let _ = set_window_slot_value(
        interp,
        window_id,
        WINDOW_HSCROLL_SLOT,
        Value::Integer(hscroll.max(0)),
    );
    let _ = set_window_slot_value(
        interp,
        window_id,
        WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
        if suspended { Value::T } else { Value::Nil },
    );
    let _ = set_window_slot_value(
        interp,
        window_id,
        WINDOW_OLD_POINT_SLOT,
        Value::Integer(old_point),
    );
}

/// Which numbers `display-line-numbers' asks for (xdisp.c's
/// Vdisplay_line_numbers): absolute buffer lines, distances from the
/// line showing point, or distances in screen lines.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineNumberMode {
    Absolute,
    Relative,
    Visual,
}

/// The line-number column a window reserves this redisplay, resolved
/// from the buffer's `display-line-numbers' family of variables the way
/// maybe_produce_line_number does.  `width' is the digit field
/// (it->lnum_width); the column costs `width + 2' screen columns — the
/// number right-justified in a width+1 field plus a blank separator
/// (pint2str with lnum_width + 1, then the appended space).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct LineNumberLayout {
    pub(crate) mode: LineNumberMode,
    pub(crate) width: usize,
    pub(crate) cols: usize,
    pub(crate) current_absolute: bool,
    pub(crate) offset: i64,
    pub(crate) widen: bool,
    pub(crate) major_tick: i64,
    pub(crate) minor_tick: i64,
}

/// maybe_produce_line_number's width computation for one window.
/// TOP_LINE and POINT_LINE are 1-based absolute buffer lines,
/// BEGV_LINE the absolute line where the accessible region begins:
/// numbers count from there unless the layout widens.  TEXT_ROWS is
/// the window's body height.  The maximal number a window row can show
/// is the one belonging to its last row — a 0-based this_line at vpos
/// 0 plus the matrix rows: this_line + nrows - 1 with nrows =
/// text_rows + 1.
pub(crate) fn window_line_number_layout(
    interp: &Interpreter,
    buffer_id: u64,
    top_line: usize,
    point_line: usize,
    begv_line: usize,
    text_rows: usize,
) -> Option<LineNumberLayout> {
    let buffer_local = |name: &str| {
        interp
            .buffer_local_value(buffer_id, name)
            .or_else(|| interp.default_value(name))
            .unwrap_or(Value::Nil)
    };
    let mode = match buffer_local("display-line-numbers") {
        Value::Nil => return None,
        Value::Symbol(ref name) if name == "relative" => LineNumberMode::Relative,
        Value::Symbol(ref name) if name == "visual" => LineNumberMode::Visual,
        _ => LineNumberMode::Absolute,
    };
    let current_absolute = buffer_local("display-line-numbers-current-absolute").is_truthy();
    let offset = match mode {
        LineNumberMode::Absolute => buffer_local("display-line-numbers-offset")
            .as_integer()
            .unwrap_or(0),
        _ => 0,
    };
    // A non-zero offset forces counting from the buffer's beginning, as
    // if display-line-numbers-widen were non-nil.
    let widen = buffer_local("display-line-numbers-widen").is_truthy() || offset != 0;
    let displayed = |line: usize| {
        if widen {
            line
        } else {
            line.saturating_sub(begv_line) + 1
        }
    };
    let max_lnum = match mode {
        LineNumberMode::Relative | LineNumberMode::Visual if !current_absolute => {
            text_rows.saturating_sub(1)
        }
        LineNumberMode::Visual => displayed(point_line).saturating_add(text_rows) - 1,
        _ => displayed(top_line).saturating_add(text_rows) - 1,
    };
    let needed = max_lnum.max(1).to_string().len();
    let explicit = buffer_local("display-line-numbers-width")
        .as_integer()
        .ok()
        .filter(|width| *width >= 0)
        .unwrap_or(0) as usize;
    let width = needed.max(explicit);
    let tick = |name: &str| {
        interp
            .lookup_var(name, &Vec::new())
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(0)
    };
    Some(LineNumberLayout {
        mode,
        width,
        cols: width + 2,
        current_absolute,
        offset,
        widen,
        major_tick: tick("display-line-numbers-major-tick"),
        minor_tick: tick("display-line-numbers-minor-tick"),
    })
}

fn set_window_hscroll_value(
    interp: &mut Interpreter,
    window_id: u64,
    requested: i64,
) -> Result<Value, LispError> {
    let hscroll = requested.max(0);
    set_window_slot_value(
        interp,
        window_id,
        WINDOW_HSCROLL_SLOT,
        Value::Integer(hscroll),
    )?;
    set_window_slot_value(
        interp,
        window_id,
        WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
        Value::T,
    )?;
    Ok(Value::Integer(hscroll))
}

fn valid_window_cursor_type(value: &Value) -> bool {
    match value {
        Value::Nil | Value::T => true,
        Value::Symbol(symbol) => matches!(symbol.as_str(), "box" | "hollow" | "bar" | "hbar"),
        Value::Cons(_) => {
            let Ok(kind) = value
                .car()
                .and_then(|kind| kind.as_symbol().map(str::to_owned))
            else {
                return false;
            };
            matches!(kind.as_str(), "box" | "bar" | "hbar")
                && value.cdr().is_ok_and(|width| width.as_integer().is_ok())
        }
        _ => false,
    }
}

fn window_list_value(
    interp: &Interpreter,
    minibuf: Option<&Value>,
    start: Option<&Value>,
) -> Value {
    let mut ids = live_ordinary_window_ids(interp);
    // GNU lists windows in cyclic order starting from WINDOW (default
    // the selected window), not from the tree's first leaf.
    let start_id = start
        .filter(|window| !window.is_nil())
        .and_then(|window| window_record_id_from_value(interp, window))
        .unwrap_or_else(|| interp.selected_window_id());
    if let Some(index) = ids.iter().position(|id| *id == start_id) {
        ids.rotate_left(index);
    }
    let mut windows = ids.into_iter().map(Value::Record).collect::<Vec<_>>();
    let include_minibuffer = matches!(minibuf, Some(Value::T));
    if !include_minibuffer {
        return Value::list(windows);
    }
    let minibuffer = interp.minibuffer_window_value();
    if !windows
        .iter()
        .any(|window| values_equal(interp, window, &minibuffer))
    {
        windows.push(minibuffer);
    }
    Value::list(windows)
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "format-mode-line" => {
                need_arg_range(name, args, 1, 4)?;
                // GNU's C primitive intentionally makes formatting a no-op in
                // noninteractive mode, before it inspects FORMAT or frame state.
                // An interactive session (a frontend published live window
                // geometry) renders the construct for real.
                if interactive_window_metrics().is_none() {
                    return Ok(Value::String(String::new().into()));
                }
                let format = if args[0].is_nil() {
                    interp
                        .lookup_var("mode-line-format", env)
                        .unwrap_or(Value::Nil)
                } else {
                    args[0].clone()
                };
                let text = render_mode_line_construct(interp, env, &format, 0)?;
                Ok(Value::String(text.into()))
            }
            // ── Output ──
            "message" => {
                let (text, text_spans, formatted) =
                    if args.is_empty() || args.first().is_some_and(Value::is_nil) {
                        (String::new(), Vec::new(), None)
                    } else {
                        let formatted = super::call(interp, "format", args, env)?;
                        (
                            string_text(&formatted)?,
                            string_face_spans(&formatted),
                            Some(formatted),
                        )
                    };
                let buffer_name = interp
                    .lookup_var("messages-buffer-name", env)
                    .and_then(|value| string_like(&value).map(|string| string.text))
                    .unwrap_or_else(|| "*Messages*".into());
                // GNU message_dolog: nothing is logged for an empty message or
                // with `message-log-max' nil; a fixnum keeps that many lines.
                let log_max = interp
                    .lookup_var("message-log-max", env)
                    .unwrap_or(Value::T);
                if !text.is_empty() && !log_max.is_nil() {
                    let buffer_id = interp
                        .find_buffer(&buffer_name)
                        .map(|(id, _)| id)
                        .unwrap_or_else(|| interp.create_buffer(&buffer_name).0);
                    if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                        let end = buffer.point_max();
                        buffer.goto_char(end);
                        buffer.insert(&(text.clone() + "\n"));
                        if let Ok(max_lines) = log_max.as_integer()
                            && max_lines >= 0
                        {
                            let contents = buffer.full_buffer_string();
                            let lines = contents.matches('\n').count();
                            if lines > max_lines as usize {
                                let drop = lines - max_lines as usize;
                                let mut offset = 0usize;
                                for _ in 0..drop {
                                    if let Some(next) = contents[offset..].find('\n') {
                                        offset += next + 1;
                                    }
                                }
                                let char_end = contents[..offset].chars().count();
                                let _ = buffer.delete_region(1, char_end + 1);
                            }
                        }
                    }
                }
                // The upstream capture advice ignores `(message nil)' and
                // `(message "")', which edebug uses to clear the echo area.
                let capturable = !args.is_empty()
                    && !args.first().is_some_and(Value::is_nil)
                    && args
                        .first()
                        .and_then(string_like)
                        .is_none_or(|string| !string.text.is_empty());
                if capturable {
                    interp.append_message_capture(&text, true, env);
                }
                // An interactive session shows the message in the echo area
                // (an empty or nil MESSAGE clears it, GNU's documented way
                // to wipe the echo line).
                if interp
                    .lookup_var("noninteractive", env)
                    .is_none_or(|value| value.is_nil())
                {
                    if text.is_empty() {
                        // xdisp.c clear_message: `clear-message-function'
                        // clears its own display first (the minibuffer's
                        // transient overlay); `dont-clear-message' keeps
                        // the echo area untouched.
                        let kept = interp
                            .lookup_var("clear-message-function", env)
                            .filter(|function| !function.is_nil())
                            .and_then(|function| {
                                interp.call_function_value(function, None, &[], env).ok()
                            })
                            .is_some_and(|result| {
                                matches!(&result, Value::Symbol(answer)
                                    if answer == "dont-clear-message")
                            });
                        if !kept {
                            set_echo_area_message(None);
                        }
                    } else {
                        // xdisp.c set_message: `set-message-function' may
                        // replace the string or consume it entirely
                        // (set-minibuffer-message displays it inside the
                        // active minibuffer instead of the echo area).
                        let mut display = Some((text.clone(), text_spans));
                        if let Some(function) = interp
                            .lookup_var("set-message-function", env)
                            .filter(|function| !function.is_nil())
                        {
                            let string = formatted
                                .clone()
                                .unwrap_or_else(|| Value::String(text.clone().into()));
                            if let Ok(result) =
                                interp.call_function_value(function, None, &[string], env)
                            {
                                if result.is_string() {
                                    display =
                                        Some((string_text(&result)?, string_face_spans(&result)));
                                } else if result.is_truthy() {
                                    display = None;
                                }
                            }
                        }
                        if let Some((text, spans)) = display {
                            set_echo_area_message_with_spans(text, spans);
                        }
                    }
                }
                // There is no echo area in batch mode.  GNU writes messages to
                // the process stderr instead, including the newline used to
                // clear an empty message.  Message capture remains independent
                // of `inhibit-message', matching the advice used by ERT.
                if interp
                    .lookup_var("noninteractive", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp
                        .lookup_var("inhibit-message", env)
                        .is_none_or(|value| value.is_nil())
                {
                    std::io::stderr()
                        .write_all(format!("{text}\n").as_bytes())
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                }
                if args.first().is_some_and(Value::is_nil) {
                    Ok(Value::Nil)
                } else {
                    Ok(Value::String(text.into()))
                }
            }
            "message-box" | "message-or-box" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                // Emaxx currently has no graphical frame, so GNU's documented
                // dialog fallback is the ordinary echo-area message path.
                super::call(interp, "message", args, env)
            }
            "current-message" => {
                need_args(name, args, 0)?;
                // GNU batch mode has no echo area; `current-message' is nil.
                if interp
                    .lookup_var("noninteractive", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return Ok(Value::Nil);
                }
                // The interactive echo area is authoritative: unlike the
                // *Messages* tail it reflects `(message nil)' clears and
                // messages suppressed from the log by `message-log-max'.
                Ok(echo_area_message()
                    .map(|text| Value::String(text.into()))
                    .unwrap_or(Value::Nil))
            }
            "error-message-string" => {
                need_args(name, args, 1)?;
                if let Err(LispError::SignalValue(signal)) = args[0].to_vec()
                    && circular_list_signal_p(&signal)
                {
                    return Err(LispError::SignalValue(signal));
                }
                // print.c's print_error_message: plain `error' promotes its
                // first datum to the message; other conditions lead with
                // their error-message property.  Remaining data is printed
                // under the condition-specific princ/prin1 rule below.
                let Ok(items) = args[0].to_vec() else {
                    return Ok(Value::String(args[0].to_string().into()));
                };
                let Some(Value::Symbol(condition)) = items.first() else {
                    return Ok(Value::String(args[0].to_string().into()));
                };
                // A file-error condition promotes its first datum to the
                // message ("Opening input file: ...").
                let file_error = interp
                    .get_symbol_property(condition, "error-conditions")
                    .and_then(|conditions| conditions.to_vec().ok())
                    .is_some_and(|conditions| {
                        conditions.iter().any(
                            |entry| matches!(entry, Value::Symbol(name) if name == "file-error"),
                        )
                    });
                let mut data = &items[1..];
                let mut text = if condition == "error" {
                    let message = data
                        .first()
                        .and_then(string_like)
                        .map(|message| message.text)
                        .unwrap_or_else(|| "peculiar error".to_string());
                    data = data.get(1..).unwrap_or_default();
                    message
                } else {
                    interp
                        .get_symbol_property(condition, "error-message")
                        .as_ref()
                        .and_then(string_like)
                        .map(|message| message.text)
                        .unwrap_or_else(|| "peculiar error".to_string())
                };
                if file_error && !data.is_empty() {
                    text =
                        crate::lisp::primitives::print::render_princ_object(interp, &data[0], env)?;
                    data = &data[1..];
                }
                if !data.is_empty() {
                    if !text.is_empty() {
                        text.push_str(": ");
                    }
                    // print.c princ's file/end-of-file/user-error data and
                    // prin1's every other condition's data.  The distinction
                    // matters for custom conditions carrying strings: Magit's
                    // multi-part git errors retain quotes and escaping.
                    let use_princ =
                        file_error || condition == "end-of-file" || condition == "user-error";
                    let mut rendered = Vec::with_capacity(data.len());
                    for datum in data {
                        rendered.push(if use_princ {
                            crate::lisp::primitives::print::render_princ_object(interp, datum, env)?
                        } else {
                            crate::lisp::primitives::print::render_prin1_ephemeral(
                                interp, datum, env,
                            )?
                        });
                    }
                    text.push_str(&rendered.join(", "));
                }
                Ok(Value::String(text.into()))
            }
            "command-error-default-function" => {
                need_args(name, args, 3)?;
                let context = string_text(&args[1])?;
                // keyboard.c command-error-default-function, noninteractive
                // branch: print CONTEXT plus the error to stderr and
                // kill-emacs -1.  The previous arm computed the message and
                // discarded it, silently converting a should-fail batch run
                // into a clean exit (2026-08-23 audit finding 80).
                let message = super::call(interp, "error-message-string", &[args[0].clone()], env)?;
                let rendered = string_text(&message).unwrap_or_default();
                if interp
                    .lookup_var("noninteractive", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    use std::io::Write;
                    let _ = writeln!(std::io::stderr(), "{context}{rendered}");
                    return super::call(interp, "kill-emacs", &[Value::Integer(-1)], env);
                }
                // Interactive frames echo instead; emaxx's tty loop reads the
                // echo area, so route the text there via `message'.
                super::call(
                    interp,
                    "message",
                    &[Value::String(format!("{context}{rendered}").into())],
                    env,
                )?;
                Ok(Value::Nil)
            }
            "ding" => Ok(Value::Nil),
            "sleep-for" => {
                need_arg_range(name, args, 1, 2)?;
                // GNU processes subprocess output whenever it waits; epg relies
                // on the trailing (sleep-for 0.1) in epg-wait-for-completion to
                // flush gpg's final status lines through the process filter.
                wait_pumping_processes(interp, env, Some(wait_duration(args)?), false, None)?;
                Ok(Value::Nil)
            }
            "accept-process-output" => {
                need_arg_range(name, args, 0, 4)?;
                // GNU: (accept-process-output &optional PROCESS SECONDS MILLISEC
                // JUST-THIS-ONE) - the wait always comes from args 2 and 3.  GNU
                // may service unrelated processes during the wait, but when
                // PROCESS is non-nil their output does not satisfy this call.
                let duration_args = if args.len() > 1 {
                    &args[1..args.len().min(3)]
                } else {
                    &[]
                };
                let target_process_id = args
                    .first()
                    .filter(|process| !process.is_nil())
                    .map(|process| interp.resolve_process_id(process))
                    .transpose()?;
                if let Some(process_id) = target_process_id {
                    interp.ensure_process_owned_by_current_thread(process_id)?;
                }
                let timeout = if !duration_args.is_empty() {
                    Some(wait_duration(duration_args)?)
                } else if target_process_id.is_none() {
                    // With neither PROCESS nor a timeout GNU performs one event
                    // pump (including due timers) and returns.  Only a specified
                    // process makes the timeout-less form wait indefinitely.
                    Some(std::time::Duration::ZERO)
                } else {
                    None
                };
                let delivered =
                    wait_pumping_processes(interp, env, timeout, true, target_process_id)?;
                Ok(if delivered { Value::T } else { Value::Nil })
            }
            "input-pending-p" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(
                    if unread_command_events(interp, env)?.is_empty()
                        && !crate::lisp::primitives::tty_input_pending()
                    {
                        Value::Nil
                    } else {
                        Value::T
                    },
                )
            }
            "discard-input" => {
                need_args(name, args, 0)?;
                if interp
                    .lookup_var("defining-kbd-macro", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    interp
                        .kbd_macro_definition
                        .truncate(interp.kbd_macro_committed_len);
                    let last_macro = Value::list(
                        std::iter::once(Value::symbol("vector-literal"))
                            .chain(interp.kbd_macro_definition.iter().cloned()),
                    );
                    interp.set_variable("last-kbd-macro", last_macro, env);
                }
                interp.set_variable("unread-command-events", Value::Nil, env);
                interp.set_variable("defining-kbd-macro", Value::Nil, env);
                Ok(Value::Nil)
            }
            "prin1" => {
                need_arg_range(name, args, 1, 3)?;
                let rendered = if matches!(args.get(2), None | Some(Value::Nil)) {
                    render_prin1(interp, &args[0], env)?
                } else {
                    let mut print_env = printer_env_with_overrides(env, args.get(2))?;
                    let rendered = render_prin1(interp, &args[0], &mut print_env)?;
                    sync_print_number_table(env, args.get(2), &print_env);
                    let stream = printer_stream_value(interp, &print_env, args.get(1));
                    write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                    return Ok(args[0].clone());
                };
                let stream = printer_stream_value(interp, env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                if native_print_updates_batch_last_char(interp, &args[0], env, true)
                    && let Some(last) = rendered.chars().last()
                {
                    record_batch_standard_output_char(interp, stream.as_ref(), env, last);
                }
                Ok(args[0].clone())
            }
            "princ" => {
                if args.is_empty() {
                    return Ok(Value::Nil);
                }
                let rendered =
                    crate::lisp::primitives::print::render_princ_object(interp, &args[0], env)?;
                let stream = printer_stream_value(interp, env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                if native_print_updates_batch_last_char(interp, &args[0], env, false)
                    && let Some(last) = rendered.chars().last()
                {
                    record_batch_standard_output_char(interp, stream.as_ref(), env, last);
                }
                Ok(args[0].clone())
            }
            "print" => {
                if args.is_empty() {
                    return Ok(Value::Nil);
                }
                let rendered = format!("\n{}\n", render_prin1(interp, &args[0], env)?);
                let stream = printer_stream_value(interp, env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                record_batch_standard_output_char(interp, stream.as_ref(), env, '\n');
                Ok(args[0].clone())
            }
            "terpri" => {
                need_arg_range(name, args, 0, 2)?;
                let stream = printer_stream_value(interp, env, args.first());
                if args.get(1).is_some_and(Value::is_truthy) {
                    if let Some(function) = stream.as_ref()
                        && super::call(interp, "functionp", std::slice::from_ref(function), env)?
                            .is_truthy()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("error".into()),
                            Value::String("Unsupported function argument".into()),
                            function.clone(),
                        ])));
                    }
                    let noninteractive_stdout = interp
                        .lookup_var("noninteractive", env)
                        .is_some_and(|value| value.is_truthy())
                        && stream
                            .as_ref()
                            .is_some_and(|value| matches!(value, Value::T));
                    let at_line_start = if noninteractive_stdout {
                        interp.batch_standard_output_last_char == Some('\n')
                    } else {
                        printer_stream_at_line_start(interp, stream.as_ref())?
                    };
                    if at_line_start {
                        return Ok(Value::Nil);
                    }
                }
                write_printer_output(interp, "\n", stream.as_ref(), env)?;
                record_batch_standard_output_char(interp, stream.as_ref(), env, '\n');
                Ok(Value::T)
            }
            "prin1-to-string" => {
                need_arg_range(name, args, 1, 3)?;
                // NOESCAPE non-nil prints like `princ' (no quoting).
                if args.get(1).is_some_and(|value| value.is_truthy()) {
                    return Ok(Value::String(
                        crate::lisp::primitives::print::render_princ_object(interp, &args[0], env)?
                            .into(),
                    ));
                }
                if matches!(args.get(2), None | Some(Value::Nil)) {
                    return Ok(Value::String(render_prin1(interp, &args[0], env)?.into()));
                }
                let mut print_env = printer_env_with_overrides(env, args.get(2))?;
                let rendered = render_prin1(interp, &args[0], &mut print_env)?;
                sync_print_number_table(env, args.get(2), &print_env);
                Ok(Value::String(rendered.into()))
            }
            "write-char" => {
                need_arg_range(name, args, 1, 2)?;
                let rendered = format_char_conversion(&args[0])?;
                let stream = printer_stream_value(interp, env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                if let Some(last) = rendered.chars().last() {
                    record_batch_standard_output_char(interp, stream.as_ref(), env, last);
                }
                Ok(args[0].clone())
            }
            "redirect-debugging-output" => {
                need_arg_range(name, args, 0, 2)?;
                let target = match args.first() {
                    None | Some(Value::Nil) => Value::Nil,
                    Some(value) => Value::String(string_text(value)?.into()),
                };
                interp.external_debugging_output_target = match &target {
                    Value::String(path) => Some(path.to_string()),
                    _ => None,
                };
                Ok(target)
            }
            "external-debugging-output" => {
                need_args(name, args, 1)?;
                let rendered = string_like(&args[0])
                    .map(|value| value.text)
                    .unwrap_or(format_char_conversion(&args[0])?);
                append_external_debugging_output(interp, &rendered)?;
                Ok(args[0].clone())
            }
            "print--preprocess" => {
                need_args(name, args, 1)?;
                print_preprocess(interp, &args[0], env)
            }
            "yes-or-no-p" => {
                need_args(name, args, 1)?;
                ensure_interaction_allowed(interp, env)?;
                // fns.c:3521 Fyes_or_no_p.  The old arm answered t whenever
                // no unread event supplied a `y' or `n' -- silence became
                // consent (finding 11).  GNU never invents an answer: it
                // honors `use-short-answers', appends `yes-or-no-prompt',
                // and loops on `read-from-minibuffer' until the user types
                // yes or no; in batch, EOF on stdin signals end-of-file.
                if interp
                    .lookup_var("use-short-answers", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    return crate::lisp::primitives::call_named_function(
                        interp, "y-or-n-p", args, env,
                    );
                }
                let mut prompt = string_text(&args[0])?;
                if !prompt.is_empty() && !prompt.ends_with(char::is_whitespace) {
                    prompt.push(' ');
                }
                let suffix = interp
                    .lookup_var("yes-or-no-prompt", env)
                    .and_then(|value| string_like(&value).map(|text| text.text))
                    .unwrap_or_else(|| "(yes or no) ".into());
                prompt.push_str(&suffix);
                loop {
                    let answer = super::call(
                        interp,
                        "read-from-minibuffer",
                        &[
                            Value::String(prompt.clone().into()),
                            Value::Nil,
                            Value::Nil,
                            Value::Nil,
                            Value::symbol("yes-or-no-p-history"),
                        ],
                        env,
                    )?;
                    let answer = string_like(&answer)
                        .map(|text| text.text.to_lowercase())
                        .unwrap_or_default();
                    match answer.as_str() {
                        "yes" => return Ok(Value::T),
                        "no" => return Ok(Value::Nil),
                        _ => {}
                    }
                    let _ = super::call(interp, "ding", &[Value::Nil], env);
                    let _ = super::call(interp, "discard-input", &[], env);
                    let _ = crate::lisp::primitives::call_named_function(
                        interp,
                        "message",
                        &[Value::String("Please answer yes or no.".into())],
                        env,
                    );
                    let _ = super::call(interp, "sleep-for", &[Value::Integer(2)], env);
                }
            }

            // ── More string/char ops ──
            "char-equal" => {
                need_args(name, args, 2)?;
                let a = args[0].as_integer()?;
                let b = args[1].as_integer()?;
                let case_fold = interp
                    .lookup_var("case-fold-search", env)
                    .map(|v| v.is_truthy())
                    .unwrap_or(false);
                let eq = if case_fold {
                    a == b || (a as u8 as char).eq_ignore_ascii_case(&(b as u8 as char))
                } else {
                    a == b
                };
                Ok(if eq { Value::T } else { Value::Nil })
            }
            "key-description" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(
                        "key-description".into(),
                        args.len(),
                    ));
                }
                let mut parts = Vec::new();
                if let Some(prefix) = args.get(1) {
                    // PREFIX and KEYS are one event sequence for display.  In
                    // particular a trailing ESC in PREFIX combines with the
                    // first character in KEYS (`ESC' + `v' -> `M-v').
                    let mut events = key_description_events(prefix)?;
                    events.extend(key_description_events(&args[0])?);
                    let sequence = Value::list(
                        std::iter::once(Value::Symbol("vector-literal".into())).chain(events),
                    );
                    append_key_description_parts(&sequence, &mut parts)?;
                } else {
                    append_key_description_parts(&args[0], &mut parts)?;
                }
                Ok(Value::String(parts.join(" ").into()))
            }
            "single-key-description" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(
                        "single-key-description".into(),
                        args.len(),
                    ));
                }
                let no_angles = args.get(1).is_some_and(Value::is_truthy);
                Ok(Value::String(
                    single_key_description_text(&args[0], no_angles)?.into(),
                ))
            }
            "text-char-description" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    text_char_description_text(args[0].as_integer()?)?.into(),
                ))
            }

            // ── More buffer ops ──
            "following-char" => match public_buffer_char_code_at(interp, interp.buffer.point()) {
                Some(code) => Ok(Value::Integer(code)),
                None => Ok(Value::Integer(0)),
            },
            "preceding-char" => {
                let pt = interp.buffer.point();
                if pt <= interp.buffer.point_min() {
                    Ok(Value::Integer(0))
                } else {
                    match public_buffer_char_code_at(interp, pt - 1) {
                        Some(code) => Ok(Value::Integer(code)),
                        None => Ok(Value::Integer(0)),
                    }
                }
            }
            "buffer-last-name" => Ok(Value::String(
                interp
                    .buffer
                    .last_name
                    .clone()
                    .unwrap_or_else(|| interp.buffer.name.clone())
                    .into(),
            )),

            // ── Display stubs ──
            "window-system" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "tty-type" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(interp
                    .tty_terminal_type()
                    .map(|terminal_type| Value::String(terminal_type.to_string().into()))
                    .unwrap_or(Value::Nil))
            }
            "controlling-tty-p" | "tty-top-frame" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(Value::Nil)
            }
            "tty-display-color-p" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(if interp.tty_display_color_cells() > 0 {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "tty-display-color-cells" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(Value::Integer(interp.tty_display_color_cells()))
            }
            "tty-no-underline" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Ok(Value::Nil)
            }
            "suspend-tty" | "resume-tty" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Err(LispError::Signal(format!(
                    "Attempt to {} a non-text terminal device",
                    if name == "suspend-tty" {
                        "suspend"
                    } else {
                        "resume"
                    }
                )))
            }
            "tty--output-buffer-size" => {
                need_arg_range(name, args, 0, 1)?;
                require_live_terminal(interp, args.first())?;
                Err(LispError::Signal("Not a tty terminal".into()))
            }
            "tty--set-output-buffer-size" => {
                need_arg_range(name, args, 1, 2)?;
                if !matches!(args[0], Value::Integer(size) if size >= 0) {
                    return Err(LispError::Signal("Invalid output buffer size".into()));
                }
                require_live_terminal(interp, args.get(1))?;
                Err(LispError::Signal(
                    "Attempt to suspend a non-text terminal device".into(),
                ))
            }
            // Batch sessions have no color support (GNU: nil / 0).

            // xfaces.c Fdisplay_supports_face_attributes_p: batch gives
            // up before frames exist (noninteractive -> nil); a live tty
            // frame dispatches to tty_supports_face_attributes_p.
            "display-supports-face-attributes-p" => {
                need_arg_range(name, args, 1, 2)?;
                if interp.tty_display_color_cells() <= 0 {
                    return Ok(Value::Nil);
                }
                tty_supports_face_attributes(interp, env, &args[0])
            }
            "internal-char-font" => {
                need_arg_range(name, args, 1, 2)?;
                if args[0].is_nil() {
                    let Some(character) = args.get(1) else {
                        return Err(LispError::TypeError("characterp".into(), "nil".into()));
                    };
                    let valid = matches!(character, Value::Integer(codepoint)
                    if u32::try_from(*codepoint).ok().and_then(char::from_u32).is_some());
                    if !valid {
                        return Err(LispError::WrongTypeArgument(
                            "characterp".into(),
                            character.clone(),
                        ));
                    }
                } else {
                    let position = position_from_value(interp, &args[0])?;
                    if position < interp.buffer.point_min() || position >= interp.buffer.point_max()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("args-out-of-range".into()),
                            args[0].clone(),
                            Value::Integer(interp.buffer.point_min() as i64),
                            Value::Integer(interp.buffer.point_max() as i64),
                        ])));
                    }
                    if let Some(character) = args.get(1)
                        && !character.is_nil()
                    {
                        let valid = matches!(character, Value::Integer(codepoint)
                        if u32::try_from(*codepoint).ok().and_then(char::from_u32).is_some());
                        if !valid {
                            return Err(LispError::WrongTypeArgument(
                                "characterp".into(),
                                character.clone(),
                            ));
                        }
                    }
                }
                // Emaxx currently models a headless batch terminal and has no
                // redisplay font object or terminal glyph-code service.  GNU's
                // corresponding batch frame reports nil for both cases.
                Ok(Value::Nil)
            }
            "current-input-mode" => {
                need_args(name, args, 0)?;
                Ok(Value::list([
                    if interp.input_interrupt_mode() {
                        Value::T
                    } else {
                        Value::Nil
                    },
                    Value::Nil,
                    Value::T,
                    Value::Integer(7),
                ]))
            }
            "set-input-interrupt-mode" => {
                need_args(name, args, 1)?;
                interp.set_input_interrupt_mode(args[0].is_truthy());
                Ok(Value::Nil)
            }
            "set-output-flow-control" | "set-input-meta-mode" => {
                need_arg_range(name, args, 1, 2)?;
                // GNU's batch frame has no decoded tty terminal, so these
                // terminal-local setters are successful no-ops.
                Ok(Value::Nil)
            }
            "set-input-mode" => {
                need_arg_range(name, args, 3, 4)?;
                interp.set_input_interrupt_mode(args[0].is_truthy());
                Ok(Value::Nil)
            }
            "set-quit-char" => {
                need_args(name, args, 1)?;
                // No controlling tty in batch mode; GNU returns before
                // validating or changing its tty-local quit character.
                Ok(Value::Nil)
            }

            "menu-or-popup-active-p" => {
                need_args(name, args, 0)?;
                // A headless Emaxx process cannot have an active native menu.
                Ok(Value::Nil)
            }
            "imagep" => {
                need_args(name, args, 1)?;
                Ok(if valid_image_spec(interp, &args[0], env) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "clear-image-cache" => {
                need_arg_range(name, args, 0, 2)?;
                if let Some(animation_cache) = args.get(1).filter(|value| !value.is_nil()) {
                    if animation_cache.cons_values().is_none() {
                        return Err(wrong_type_argument("consp", animation_cache.clone()));
                    }
                    return Ok(Value::Nil);
                }
                let filter = args.first().cloned().unwrap_or(Value::Nil);
                if filter.is_nil() || matches!(filter, Value::Frame(id) if interp.frame_is_live(id))
                {
                    return Err(LispError::Signal(
                        "Window system frame should be used".into(),
                    ));
                }
                Ok(Value::Nil)
            }
            "image-cache-size" => {
                need_args(name, args, 0)?;
                // The single batch/TTY frame has no native image cache.
                Ok(Value::Integer(0))
            }
            "image-flush" => {
                need_arg_range(name, args, 1, 2)?;
                if !valid_image_spec(interp, &args[0], env) {
                    return Err(LispError::Signal("Invalid image specification".into()));
                }
                if matches!(args.get(1), Some(Value::T)) {
                    return Ok(Value::Nil);
                }
                Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ))
            }
            "image-transforms-p" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "image-size" | "image-mask-p" | "image-metadata" => Err(LispError::Signal(
                "Images are unavailable on a nongraphical display".into(),
            )),

            "init-image-library" => {
                need_args(name, args, 1)?;
                let image_type = args[0].as_symbol()?;
                Ok(
                    if matches!(
                        image_type,
                        "pbm" | "png" | "jpeg" | "gif" | "svg" | "xbm" | "xpm" | "webp" | "tiff"
                    ) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "window-start" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Integer(window_start(interp, args.first())? as i64))
            }
            "window-end" => {
                need_arg_range(name, args, 0, 2)?;
                // GNU accepts an UPDATE flag here.  Emaxx computes the current
                // headless extent eagerly, so accepting it requires no extra
                // invalidation step.
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
                // A batch session's dumb frame shows the whole buffer (GNU
                // answers ZV); an interactive frontend publishes the real
                // displayed extent.
                let end = interactive_window_metrics()
                    .map(|metrics| metrics.window_end.clamp(point_min, point_max))
                    .unwrap_or(point_max);
                Ok(Value::Integer(end as i64))
            }
            "window-point" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let buffer_id = window_buffer_id(interp, &Value::Record(window_id))
                    .ok_or_else(|| LispError::TypeError("window".into(), "deleted".into()))?;
                let point = if window_id == interp.selected_window_id() {
                    if buffer_id == interp.current_buffer_id() {
                        interp.buffer.point()
                    } else {
                        interp
                            .get_buffer_by_id(buffer_id)
                            .map(|buffer| buffer.point())
                            .unwrap_or(1)
                    }
                } else {
                    window_slot_value(interp, window_id, WINDOW_POINT_SLOT)
                        .as_integer()
                        .unwrap_or(1)
                        .max(1) as usize
                };
                Ok(Value::Integer(point as i64))
            }
            "window-hscroll" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
                    .as_integer()
                    .map(Value::Integer)
                    .unwrap_or(Value::Integer(0)))
            }
            "window-vscroll" => {
                need_arg_range(name, args, 0, 2)?;
                live_window_id_or_selected(interp, args.first())?;
                // A terminal frame has no smooth pixel scrolling; GNU returns
                // zero in both canonical-line and pixel modes.
                Ok(Value::Integer(0))
            }
            "set-window-hscroll" => {
                need_args(name, args, 2)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let Value::Integer(requested) = args[1] else {
                    return Err(wrong_type_argument("fixnump", args[1].clone()));
                };
                set_window_hscroll_value(interp, window_id, requested)
            }
            "scroll-left" | "scroll-right" => {
                need_arg_range(name, args, 0, 2)?;
                let window_id = live_window_id_or_selected(interp, None)?;
                let requested = match args.first() {
                    None | Some(Value::Nil) => window_text_width_columns(interp, window_id) - 2,
                    Some(value) => prefix_numeric_value(value)?.as_integer()?,
                };
                let current = window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
                    .as_integer()
                    .unwrap_or(0);
                let requested = if name == "scroll-left" {
                    current.saturating_add(requested)
                } else {
                    current.saturating_sub(requested)
                };
                let result = set_window_hscroll_value(interp, window_id, requested)?;
                if args.get(1).is_some_and(Value::is_truthy) {
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_MIN_HSCROLL_SLOT,
                        result.clone(),
                    )?;
                }
                Ok(result)
            }
            "other-window-for-scrolling" => {
                need_args(name, args, 0)?;
                let selected_id = live_window_id_or_selected(interp, None)?;
                let selected_kind = window_slot_value(interp, selected_id, WINDOW_KIND_SLOT);
                let mut candidate = if matches!(
                    selected_kind,
                    Value::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND
                ) {
                    interp
                        .lookup_var("minibuffer-scroll-window", env)
                        .filter(Value::is_truthy)
                } else {
                    None
                };

                if candidate.is_none()
                    && let Some(buffer @ Value::Buffer(_)) =
                        interp.lookup_var("other-window-scroll-buffer", env)
                    && let Value::Buffer(buffer_value) = &buffer
                    && interp.has_buffer_id(buffer_value.id)
                {
                    candidate = live_ordinary_window_ids(interp)
                        .into_iter()
                        .find(|window_id| {
                            window_buffer_id(interp, &Value::Record(*window_id))
                                == Some(buffer_value.id)
                        })
                        .map(Value::Record);
                    if candidate.is_none() {
                        candidate = Some(call_function_value(
                            interp,
                            &Value::symbol("display-buffer"),
                            &[buffer, Value::T],
                            env,
                        )?);
                    }
                }

                if candidate.is_none()
                    && let Some(default) = interp
                        .lookup_var("other-window-scroll-default", env)
                        .filter(Value::is_truthy)
                    && crate::lisp::primitives::call(
                        interp,
                        "functionp",
                        std::slice::from_ref(&default),
                        env,
                    )?
                    .is_truthy()
                {
                    candidate = Some(call_function_value(interp, &default, &[], env)?);
                }

                let candidate = candidate.unwrap_or_else(|| {
                    let ids = live_ordinary_window_ids(interp);
                    let index = ids.iter().position(|id| *id == selected_id).unwrap_or(0);
                    ids.get((index + 1) % ids.len())
                        .copied()
                        .map(Value::Record)
                        .unwrap_or_else(|| interp.selected_window_value())
                });
                let candidate_id = live_window_id_or_selected(interp, Some(&candidate))?;
                if candidate_id == selected_id {
                    return Err(LispError::Signal("There is no other window".into()));
                }
                Ok(Value::Record(candidate_id))
            }
            "set-window-margins" => {
                need_arg_range(name, args, 1, 3)?;
                let window_id = if args.first().is_none_or(|value| value.is_nil()) {
                    interp.selected_window_id()
                } else {
                    crate::lisp::primitives::window::window_record_id_from_value(interp, &args[0])
                        .ok_or_else(|| {
                        LispError::WrongTypeArgument("windowp".into(), args[0].clone())
                    })?
                };
                let margin = |value: Option<&Value>| -> Result<Option<i64>, LispError> {
                    match value {
                        None | Some(Value::Nil) => Ok(None),
                        Some(value) => Ok(Some(value.as_integer()?.max(0))),
                    }
                };
                let left = margin(args.get(1))?;
                let right = margin(args.get(2))?;
                interp.set_window_margins(window_id, left, right);
                Ok(Value::T)
            }
            "window-margins" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = if args.first().is_none_or(|value| value.is_nil()) {
                    interp.selected_window_id()
                } else {
                    crate::lisp::primitives::window::window_record_id_from_value(interp, &args[0])
                        .ok_or_else(|| {
                        LispError::WrongTypeArgument("windowp".into(), args[0].clone())
                    })?
                };
                let (left, right) = interp.window_margins(window_id);
                let to_value =
                    |margin: Option<i64>| margin.map(Value::Integer).unwrap_or(Value::Nil);
                Ok(Value::cons(to_value(left), to_value(right)))
            }
            "coordinates-in-window-p" => {
                need_args(name, args, 2)?;
                let window_id = live_window_id_or_selected(interp, Some(&args[1]))?;
                let x = args[0].car()?.as_float()?.trunc() as i64;
                let y = args[0].cdr()?.as_float()?.trunc() as i64;
                let (width, height, left, top) = window_geometry(interp, window_id);
                if x < left || x >= left + width || y < top || y >= top + height {
                    return Ok(Value::Nil);
                }
                let local_x = x - left;
                let local_y = y - top;
                let buffer_id = window_buffer_id(interp, &Value::Record(window_id))
                    .ok_or_else(|| wrong_type_argument("window-live-p", args[1].clone()))?;
                let tab_line = window_line_height(interp, buffer_id, "tab-line-format", env);
                let header_line = window_line_height(interp, buffer_id, "header-line-format", env);
                let mode_line = window_line_height(interp, buffer_id, "mode-line-format", env);
                if tab_line > 0 && local_y < tab_line {
                    return Ok(Value::symbol("tab-line"));
                }
                if header_line > 0 && local_y < tab_line + header_line {
                    return Ok(Value::symbol("header-line"));
                }
                if mode_line > 0 && local_y >= height - mode_line {
                    return Ok(Value::symbol("mode-line"));
                }
                let (left_margin, right_margin) = interp.window_margins(window_id);
                let left_margin = left_margin.unwrap_or(0);
                let right_margin = right_margin.unwrap_or(0);
                if local_x < left_margin {
                    return Ok(Value::symbol("left-margin"));
                }
                if local_x >= width - right_margin {
                    return Ok(Value::symbol("right-margin"));
                }
                Ok(Value::cons(
                    Value::Integer(local_x - left_margin),
                    Value::Integer(local_y),
                ))
            }
            "pos-visible-in-window-p" => {
                need_arg_range(name, args, 0, 3)?;
                if interp
                    .lookup_var("noninteractive", env)
                    .is_some_and(|value| !value.is_nil())
                {
                    return Ok(Value::Nil);
                }
                let window = args.get(1).filter(|value| !value.is_nil());
                let window_id = live_window_id_or_selected(interp, window)?;
                let buffer_id = if let Some(window) = window {
                    window_buffer_id(interp, window).ok_or_else(|| {
                        LispError::WrongTypeArgument("windowp".into(), window.clone())
                    })?
                } else {
                    interp.selected_window_buffer_id()
                };
                let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
                let pos = match args.first() {
                    None | Some(Value::Nil) => interp.buffer.point(),
                    Some(Value::T) => point_max,
                    Some(value) => position_from_value(interp, value)?,
                };
                let start = window_start(interp, window)?;
                let first_visible = start.max(point_min);
                // GNU walks DISPLAY lines from the window start (its
                // start_display iterator): invisible text collapses, so
                // the end of a folded org subtree sits a couple of rows
                // down, not the raw line count away —
                // org-subtree-end-visible-p decides whether org-cycle
                // recenters on exactly this answer.
                // The global interactive metrics describe only the last
                // redrawn selected window.  Help asks about its newly split,
                // non-selected window before the next redisplay; reusing the
                // old selected height falsely declared Help's point-max
                // visible and changed help-window-display-message.  The live
                // window tree already owns the queried window's exact body
                // height.
                let limit = (window_geometry(interp, window_id).1
                    - window_non_body_height(interp, buffer_id, env))
                .max(1) as usize;
                let rows = {
                    let saved_buffer = interp.current_buffer_id();
                    let switched = saved_buffer != buffer_id
                        && interp.set_current_buffer_id(buffer_id).is_ok();
                    let mut rows = 0usize;
                    let mut cursor = first_visible;
                    while rows < limit && pos >= first_visible {
                        let (next, remaining) = crate::lisp::primitives::window::move_screen_lines(
                            interp, env, cursor, 1,
                        );
                        if remaining != 0 || next <= cursor || pos < next {
                            break;
                        }
                        cursor = next;
                        rows += 1;
                    }
                    if switched {
                        let _ = interp.set_current_buffer_id(saved_buffer);
                    }
                    rows
                };
                let visible = pos >= first_visible && pos <= point_max && rows < limit;
                if !visible {
                    return Ok(Value::Nil);
                }
                // With PARTIALLY non-nil GNU answers a list (X Y [RTOP RBOT
                // ROWH VPOS]) — two elements for a fully visible position.
                // Text-terminal frames measure pixels in character cells, so
                // X is the display column and Y the window row; simple.el's
                // line-move-partial consumes exactly this shape.
                if args.get(2).is_some_and(Value::is_truthy) {
                    let saved = interp.buffer.point();
                    interp.buffer.goto_char(pos);
                    let x = super::call(interp, "current-column", &[], env)
                        .ok()
                        .and_then(|value| value.as_integer().ok())
                        .unwrap_or(0);
                    interp.buffer.goto_char(saved);
                    return Ok(Value::list([
                        Value::Integer(x),
                        Value::Integer(rows as i64),
                    ]));
                }
                Ok(Value::T)
            }
            "window-total-width" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(Value::Integer(window_geometry(interp, window_id).0))
            }
            "window-total-height" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(Value::Integer(window_geometry(interp, window_id).1))
            }
            "window-use-time" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(interp.window_use_time(window_id)))
            }
            "window-bump-use-time" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(interp
                    .bump_window_use_time(window_id)
                    .map(Value::Integer)
                    .unwrap_or(Value::Nil))
            }
            "window-body-width" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(window_text_width_columns(interp, window_id)))
            }
            "window-body-height" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(
                    window_geometry(interp, window_id).1
                        - window_non_body_height(interp, buffer_id, env),
                ))
            }
            "window-text-width" => {
                need_arg_range(name, args, 0, 2)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(window_text_width_columns(interp, window_id)))
            }
            "window-text-height" => {
                need_arg_range(name, args, 0, 2)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::Integer(
                    window_geometry(interp, window_id).1
                        - window_non_body_height(interp, buffer_id, env),
                ))
            }
            "window-line-height" => {
                need_arg_range(name, args, 0, 2)?;
                live_window_id_or_selected(interp, args.get(1))?;
                // GNU has no current glyph matrix in batch mode.
                Ok(Value::Nil)
            }
            "window-lines-pixel-dimensions" => {
                need_arg_range(name, args, 0, 6)?;
                live_window_id_or_selected(interp, args.first())?;
                // GNU has no current glyph matrix in batch mode.
                Ok(Value::Nil)
            }
            "window-pixel-width" | "window-pixel-height" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let geometry = window_geometry(interp, window_id);
                Ok(Value::Integer(if name == "window-pixel-width" {
                    geometry.0
                } else {
                    geometry.1
                }))
            }
            "window-pixel-left" | "window-pixel-top" | "window-left-column" | "window-top-line" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let geometry = window_geometry(interp, window_id);
                Ok(Value::Integer(
                    if matches!(name, "window-pixel-left" | "window-left-column") {
                        geometry.2
                    } else {
                        geometry.3
                    },
                ))
            }
            "window-old-pixel-width"
            | "window-old-pixel-height"
            | "window-old-body-pixel-width"
            | "window-old-body-pixel-height" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(window) = args.first() {
                    window_id_or_selected(interp, window)?;
                }
                Ok(Value::Integer(0))
            }
            "window-new-pixel" | "window-new-total" | "window-new-normal" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = match name {
                    "window-new-pixel" => WINDOW_NEW_PIXEL_SLOT,
                    "window-new-total" => WINDOW_NEW_TOTAL_SLOT,
                    _ => WINDOW_NEW_NORMAL_SLOT,
                };
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(slot))
                    .cloned()
                    .unwrap_or(Value::Integer(0)))
            }
            "window-normal-size" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = if args.get(1).is_some_and(Value::is_truthy) {
                    WINDOW_NORMAL_WIDTH_SLOT
                } else {
                    WINDOW_NORMAL_HEIGHT_SLOT
                };
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(slot))
                    .cloned()
                    .unwrap_or(Value::Float(1.0)))
            }
            "set-window-new-pixel" | "set-window-new-total" => {
                need_arg_range(name, args, 2, 3)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                let requested = args[1].as_integer()?;
                let slot = if name == "set-window-new-pixel" {
                    WINDOW_NEW_PIXEL_SLOT
                } else {
                    WINDOW_NEW_TOTAL_SLOT
                };
                let current = window_slot_value(interp, window_id, slot)
                    .as_integer()
                    .unwrap_or(0);
                let requested = if args.get(2).is_some_and(Value::is_truthy) {
                    current.saturating_add(requested)
                } else {
                    requested
                };
                if name == "set-window-new-pixel" && requested < 0 {
                    return Err(LispError::Signal(format!("Args out of range: {requested}")));
                }
                set_window_slot_value(interp, window_id, slot, Value::Integer(requested))
            }
            "set-window-new-normal" => {
                need_arg_range(name, args, 1, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(
                    interp,
                    window_id,
                    WINDOW_NEW_NORMAL_SLOT,
                    args.get(1).cloned().unwrap_or(Value::Nil),
                )
            }
            "window-resize-apply" => {
                need_arg_range(name, args, 0, 2)?;
                if let Some(frame) = args.first().filter(|frame| !frame.is_nil())
                    && !matches!(frame, Value::Frame(id) if interp.frame_is_live(*id))
                {
                    return Err(LispError::WrongTypeArgument("framep".into(), frame.clone()));
                }
                let horizontal = args.get(1).is_some_and(Value::is_truthy);
                let root = frame_root_window_value(interp);
                let root_id = window_id_or_selected(interp, &root)?;
                let (_, _, left, top) = window_geometry(interp, root_id);
                apply_staged_window_sizes(interp, root_id, horizontal, left, top)?;
                Ok(Value::T)
            }
            "window-resize-apply-total" => {
                need_arg_range(name, args, 0, 2)?;
                if let Some(frame) = args.first().filter(|frame| !frame.is_nil())
                    && !matches!(frame, Value::Frame(id) if interp.frame_is_live(*id))
                {
                    return Err(LispError::WrongTypeArgument("framep".into(), frame.clone()));
                }
                Ok(Value::T)
            }
            "window-mode-line-height" | "window-header-line-height" | "window-tab-line-height" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
                let format_variable = match name {
                    "window-mode-line-height" => "mode-line-format",
                    "window-header-line-height" => "header-line-format",
                    _ => "tab-line-format",
                };
                Ok(Value::Integer(window_line_height(
                    interp,
                    buffer_id,
                    format_variable,
                    env,
                )))
            }
            "window-right-divider-width"
            | "window-bottom-divider-width"
            | "window-scroll-bar-width"
            | "window-scroll-bar-height" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(window) = args.first() {
                    window_id_or_selected(interp, window)?;
                }
                Ok(Value::Integer(0))
            }
            "move-to-window-line" => {
                need_arg_range(name, args, 0, 1)?;
                let line = resolve_window_line(args.first(), selected_window_text_height() / 2)?;
                let window_start = current_window_start(interp);
                let (target, shortage) = move_lines_from(interp, window_start, line);
                interp.buffer.goto_char(target);
                let actual = if shortage > 0 {
                    line - shortage
                } else if shortage < 0 {
                    line + shortage.abs()
                } else {
                    line
                };
                Ok(Value::Integer(actual as i64))
            }
            "recenter" => {
                need_arg_range(name, args, 0, 2)?;
                // A raw C-u (cons) centers, GNU's Frecenter convention.
                let arg = args
                    .first()
                    .filter(|value| !matches!(value, Value::Cons(_)));
                let line = resolve_window_line(arg, selected_window_text_height() / 2)?;
                // Walk back whole screen lines: wrapped lines occupy one
                // row per continuation, exactly as the display counts.
                let point = interp.buffer.point();
                let (new_start, _) = move_screen_lines(interp, env, point, -line);
                set_current_window_start(interp, new_start);
                Ok(Value::Nil)
            }
            "scroll-up" | "scroll-down" => {
                need_arg_range(name, args, 0, 1)?;
                let sign: isize = if name == "scroll-up" { 1 } else { -1 };
                match args.first() {
                    // nil scrolls a near-full screen.
                    None | Some(Value::Nil) => scroll_selected_window(interp, env, None, sign)?,
                    // `-' scrolls a near-full screen the other way.
                    Some(Value::Symbol(minus)) if minus == "-" => {
                        scroll_selected_window(interp, env, None, -sign)?
                    }
                    Some(value) => {
                        let lines = prefix_numeric_value(value)?.as_integer()? as isize;
                        scroll_selected_window(interp, env, Some(sign * lines), sign)?
                    }
                }
                Ok(Value::Nil)
            }
            "window-text-pixel-size" => {
                // (window-text-pixel-size &optional WINDOW FROM TO X-LIMIT
                //  Y-LIMIT MODE-LINES).  Without a graphical frame a cell
                // is the pixel unit: the widest line's character count and
                // the line count stand for the pixel size of WINDOW's
                // buffer text.  MODE-LINES non-nil adds the window's mode,
                // header, and tab lines — `fit-window-to-buffer' sizes the
                // whole window from that total.
                need_arg_range(name, args, 0, 6)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let buffer_id = window_buffer_id(interp, &Value::Record(window_id))
                    .unwrap_or(interp.current_buffer_id());
                let text = if buffer_id == interp.current_buffer_id() {
                    interp.buffer.buffer_string()
                } else {
                    interp
                        .get_buffer_by_id(buffer_id)
                        .map(|buffer| buffer.buffer_string())
                        .unwrap_or_default()
                };
                let mut width = text
                    .lines()
                    .map(|line| line.chars().count())
                    .max()
                    .unwrap_or(0) as i64;
                if let Some(limit) = args.get(3).and_then(|limit| limit.as_integer().ok()) {
                    width = width.min(limit);
                }
                let mut height = text.lines().count().max(1) as i64;
                if args.get(5).is_some_and(Value::is_truthy) {
                    for format_variable in
                        ["mode-line-format", "header-line-format", "tab-line-format"]
                    {
                        height += window_line_height(interp, buffer_id, format_variable, env);
                    }
                }
                if let Some(limit) = args.get(4).and_then(|limit| limit.as_integer().ok()) {
                    height = height.min(limit);
                }
                Ok(Value::cons(Value::Integer(width), Value::Integer(height)))
            }
            "buffer-text-pixel-size" => {
                // (buffer-text-pixel-size &optional WINDOW FROM TO X-LIMIT).
                // Without a graphical frame there is no font, so report the
                // widest line's character count as the pixel width and the line
                // count as the pixel height (one nominal unit per character).
                // Like GNU, honor `display' replacements: a string spec
                // substitutes for the covered text and a margin spec removes it
                // from the line flow entirely.
                let point_min = interp.buffer.point_min();
                let point_max = interp.buffer.point_max();
                let mut effective = String::new();
                let mut pos = point_min;
                while pos < point_max {
                    let display = interp.buffer.text_property_at(pos, "display");
                    if let Some(display_value) = display.clone().filter(|value| !value.is_nil()) {
                        let mut end = pos;
                        while end < point_max
                            && interp.buffer.text_property_at(end, "display") == display
                        {
                            end += 1;
                        }
                        if let Some(replacement) = string_like(&display_value) {
                            effective.push_str(&replacement.text);
                            pos = end;
                            continue;
                        }
                        let in_margin = display_value.to_vec().is_ok_and(|items| {
                            items.first().is_some_and(|head| {
                                head.to_vec().is_ok_and(|spec| {
                                    matches!(spec.first(),
                                    Some(Value::Symbol(kind)) if kind == "margin")
                                })
                            })
                        });
                        if in_margin {
                            pos = end;
                            continue;
                        }
                    }
                    match interp.buffer.char_at(pos) {
                        Some(ch) => effective.push(ch),
                        None => break,
                    }
                    pos += 1;
                }
                let width = effective
                    .lines()
                    .map(|line| line.chars().count())
                    .max()
                    .unwrap_or(0);
                let height = effective.lines().count().max(1);
                Ok(Value::cons(
                    Value::Integer(width as i64),
                    Value::Integer(height as i64),
                ))
            }
            "get-display-property" => {
                need_args(name, args, 2)?;
                let pos = args[0].as_integer()?.max(0) as usize;
                let property = args[1].as_symbol()?;
                let display = interp
                    .buffer
                    .text_property_at(pos, "display")
                    .unwrap_or(Value::Nil);
                Ok(display_property_value(&display, property).unwrap_or(Value::Nil))
            }
            "bidi-find-overridden-directionality" => {
                need_args(name, args, 2)?;
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                Ok(find_bidi_override(interp, start, end)
                    .map(|pos| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil))
            }
            "current-bidi-paragraph-direction" => {
                need_arg_range(name, args, 0, 1)?;
                let Some(buffer) = args.first().filter(|value| !value.is_nil()) else {
                    return Ok(current_bidi_paragraph_direction_value(interp, env));
                };
                let Value::Buffer(buffer_value) = buffer else {
                    return Err(wrong_type_argument("bufferp", buffer.clone()));
                };
                let buffer_id = buffer_value.id;
                if !interp.has_buffer_id(buffer_id) {
                    return Err(wrong_type_argument("bufferp", buffer.clone()));
                }
                let saved_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let direction = current_bidi_paragraph_direction_value(interp, env);
                interp.set_current_buffer_id(saved_buffer)?;
                Ok(direction)
            }
            "bidi-resolved-levels" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(vpos) = args.first().filter(|value| !value.is_nil()) {
                    vpos.as_integer()?;
                }
                // Emaxx's headless renderer intentionally retains no glyph
                // matrix between evaluations, exactly the stale-display case
                // for which GNU documents a nil result.
                Ok(Value::Nil)
            }
            "line-pixel-height" => {
                need_args(name, args, 0)?;
                // The canonical terminal cell is one pixel high in the
                // headless frame used by both compatibility runners.
                Ok(Value::Integer(1))
            }
            "display--line-is-continued-p" => {
                need_args(name, args, 0)?;
                with_selected_window_buffer(interp, |interp| {
                    let original_point = interp.buffer.point();
                    let result = (|| -> Result<Value, LispError> {
                        super::call(interp, "vertical-motion", &[Value::Integer(0)], env)?;
                        let screen_line_start = interp.buffer.point();
                        let moved =
                            super::call(interp, "vertical-motion", &[Value::Integer(1)], env)?
                                .as_integer()?;
                        let next_screen_line = interp.buffer.point();
                        let crosses_logical_line = (screen_line_start..next_screen_line)
                            .any(|position| interp.buffer.char_at(position) == Some('\n'));
                        Ok(
                            if moved == 1
                                && next_screen_line < interp.buffer.point_max()
                                && !crosses_logical_line
                            {
                                Value::T
                            } else {
                                Value::Nil
                            },
                        )
                    })();
                    interp.buffer.goto_char(original_point);
                    result
                })
            }
            "move-point-visually" => {
                need_args(name, args, 1)?;
                let direction = args[0].as_integer()?;
                with_selected_window_buffer(interp, |interp| {
                    let right = direction > 0;
                    let right_to_left = matches!(
                        current_bidi_paragraph_direction_value(interp, env),
                        Value::Symbol(ref direction) if direction == "right-to-left"
                    );
                    let logical_forward = right != right_to_left;
                    let point = interp.buffer.point();
                    let target = if logical_forward {
                        if point >= interp.buffer.point_max() {
                            return Err(signal_condition("end-of-buffer"));
                        }
                        point + 1
                    } else {
                        if point <= interp.buffer.point_min() {
                            return Err(signal_condition("beginning-of-buffer"));
                        }
                        point - 1
                    };
                    interp.buffer.goto_char(target);
                    Ok(Value::Integer(target as i64))
                })
            }
            "long-line-optimizations-p" => {
                need_args(name, args, 0)?;
                // GNU only raises this redisplay-owned flag while maintaining a
                // glyph matrix.  The cache-free headless renderer never does.
                Ok(Value::Nil)
            }
            "tab-bar-height" | "tool-bar-height" => {
                need_arg_range(name, args, 0, 2)?;
                decode_live_frame(interp, args.first(), true)?;
                // The bootstrap terminal frame has no tab/tool-bar window.
                Ok(Value::Integer(0))
            }
            "lookup-image-map" => {
                need_args(name, args, 3)?;
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                let x = args[1].as_integer()?;
                let y = args[2].as_integer()?;
                Ok(lookup_image_map(&args[0], x, y))
            }
            "redisplay" => {
                need_arg_range(name, args, 0, 1)?;
                // A live frontend repaints through its redraw hook
                // (sit-for redisplays before waiting); the cache-free
                // batch renderer cannot be preempted by pending terminal
                // input, so redisplay always completes.  Fredisplay is
                // redisplay_preserve_echo_area: a message wiped by input
                // arrival but still last-displayed shows again instead
                // of clearing the row (menu-bar-open relies on it).
                with_preserved_echo_redisplay(|| {
                    crate::lisp::primitives::run_tty_frame_redraw(interp, env)
                });
                Ok(Value::T)
            }
            "redraw-frame" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                // GNU clears glyph matrices and immediately recomputes them.
                // Emaxx retains no glyph matrix between batch evaluations.
                Ok(Value::Nil)
            }
            "redraw-display" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "display--update-for-mouse-movement" => {
                need_args(name, args, 2)?;
                for coordinate in args {
                    if !matches!(coordinate, Value::Integer(_)) {
                        return Err(wrong_type_argument("fixnump", coordinate.clone()));
                    }
                }
                // Mouse-face/help-echo redisplay is absent from the headless
                // renderer, but the native fixnum surface remains observable.
                Ok(Value::Nil)
            }
            "open-termscript" => {
                need_args(name, args, 1)?;
                // GNU's batch bootstrap frame is output_initial, not termcap;
                // it rejects both a filename and nil before inspecting FILE.
                Err(LispError::Signal(
                    "Current frame is not on a tty device".into(),
                ))
            }
            "frame-or-buffer-changed-p" => {
                need_arg_range(name, args, 0, 1)?;
                frame_or_buffer_changed(interp, args.first(), env)
            }
            "internal-show-cursor" => {
                need_args(name, args, 2)?;
                let window_id = if args[0].is_nil() {
                    interp.selected_window_id()
                } else {
                    window_record_id_from_value(interp, &args[0]).ok_or_else(|| {
                        LispError::WrongTypeArgument("windowp".into(), args[0].clone())
                    })?
                };
                interp.set_window_cursor_visible(window_id, args[1].is_truthy());
                Ok(Value::Nil)
            }
            "internal-show-cursor-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = match args.first() {
                    None | Some(Value::Nil) => interp.selected_window_id(),
                    Some(window) => {
                        window_record_id_from_value(interp, window).ok_or_else(|| {
                            LispError::WrongTypeArgument("windowp".into(), window.clone())
                        })?
                    }
                };
                Ok(if interp.window_cursor_visible(window_id) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-buffer-redisplay" => {
                need_args(name, args, 4)?;
                // GNU's xdisp.c primitive invalidates redisplay caches.  Emaxx's
                // batch renderer has no retained glyph cache, so the observable
                // watcher contract is the arity and nil return value.
                Ok(Value::Nil)
            }
            "color-distance" => {
                need_args(name, args, 2)?;
                let left = parse_color_spec(&string_text(&args[0])?)
                    .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
                let right = parse_color_spec(&string_text(&args[1])?)
                    .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
                let distance = left
                    .into_iter()
                    .zip(right)
                    .map(|(a, b)| {
                        let diff = i64::from(a) - i64::from(b);
                        diff * diff
                    })
                    .sum::<i64>();
                Ok(Value::Integer(distance))
            }
            "color-values-from-color-spec" => {
                need_args(name, args, 1)?;
                Ok(parse_color_spec(&string_text(&args[0])?)
                    .map(|[r, g, b]| {
                        Value::list([
                            Value::Integer(i64::from(r)),
                            Value::Integer(i64::from(g)),
                            Value::Integer(i64::from(b)),
                        ])
                    })
                    .unwrap_or(Value::Nil))
            }
            "selected-window" => Ok(interp.selected_window_value()),
            "old-selected-window" => Ok(interp.old_selected_window_value()),
            "frame-selected-window" => Ok(interp.selected_window_value()),
            "frame-old-selected-window" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(interp.frame_old_selected_window_value())
            }
            "set-frame-selected-window" => {
                need_arg_range(name, args, 2, 3)?;
                if !args[0].is_nil()
                    && !matches!(&args[0], Value::Frame(id) if interp.frame_is_live(*id))
                {
                    return Err(wrong_type_argument("frame-live-p", args[0].clone()));
                }
                select_window_value(interp, &args[1], args.get(2).is_some_and(Value::is_truthy))
            }
            "select-window" => {
                need_arg_range(name, args, 1, 2)?;
                select_window_value(interp, &args[0], args.get(1).is_some_and(Value::is_truthy))
            }
            "current-window-configuration" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(interp.window_configuration_value())
            }
            "set-window-configuration" => {
                need_arg_range(name, args, 1, 3)?;
                let restored = interp.apply_window_configuration_value(&args[0])?;
                Ok(if restored { Value::T } else { Value::Nil })
            }
            "window-configuration-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.is_window_configuration_value(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "window-configuration-equal-p" => {
                need_args(name, args, 2)?;
                Ok(if interp.window_configurations_equal(&args[0], &args[1])? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "window-configuration-frame" => {
                need_args(name, args, 1)?;
                if !interp.is_window_configuration_value(&args[0]) {
                    return Err(wrong_type_argument(
                        "window-configuration-p",
                        args[0].clone(),
                    ));
                }
                Ok(interp.selected_frame_value())
            }
            "force-window-update" => {
                need_arg_range(name, args, 0, 1)?;
                let Some(object) = args.first().filter(|value| !value.is_nil()) else {
                    return Ok(Value::T);
                };
                if live_window_id_or_selected(interp, Some(object)).is_ok() {
                    return Ok(Value::T);
                }
                let buffer_id = match object {
                    Value::Buffer(buffer) if interp.has_buffer_id(buffer.id) => Some(buffer.id),
                    _ => string_like(object)
                        .and_then(|string| interp.find_buffer(&string.text).map(|(id, _)| id)),
                };
                Ok(
                    if buffer_id.is_some_and(|buffer_id| {
                        live_ordinary_window_ids(interp)
                            .into_iter()
                            .any(|window_id| {
                                window_buffer_id(interp, &Value::Record(window_id))
                                    == Some(buffer_id)
                            })
                    }) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "run-window-configuration-change-hook" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(frame) = args.first()
                    && !frame.is_nil()
                    && !matches!(frame, Value::Frame(id) if interp.frame_is_live(*id))
                {
                    return Err(wrong_type_argument("frame-live-p", frame.clone()));
                }
                let original_window = interp.selected_window_id();
                let original_buffer = interp.current_buffer_id();
                let mut result = Ok(());
                for window_id in live_ordinary_window_ids(interp) {
                    let Some(buffer_id) = window_buffer_id(interp, &Value::Record(window_id))
                    else {
                        continue;
                    };
                    // GNU Elisp's `add-hook' LOCAL registration lives in
                    // the buffer-local value cell; the native depth mirror
                    // only sees hooks installed through the Rust bootstrap.
                    let local_hooks = interp
                        .buffer_local_hook(buffer_id, "window-configuration-change-hook")
                        .or_else(|| {
                            interp
                                .buffer_local_value(buffer_id, "window-configuration-change-hook")
                                .map(|value| value.to_vec().unwrap_or_else(|_| vec![value]))
                        });
                    let Some(local_hooks) = local_hooks else {
                        continue;
                    };
                    interp.set_selected_window_id(window_id);
                    if let Err(error) = interp.set_current_buffer_id(buffer_id) {
                        result = Err(error);
                        break;
                    }
                    for hook in local_hooks
                        .into_iter()
                        .filter(|hook| !matches!(hook, Value::T))
                    {
                        if let Err(error) = call_function_value(interp, &hook, &[], env) {
                            result = Err(error);
                            break;
                        }
                    }
                    if result.is_err() {
                        break;
                    }
                }

                interp.set_selected_window_id(original_window);
                if result.is_ok()
                    && let Some(selected_buffer) =
                        window_buffer_id(interp, &Value::Record(original_window))
                {
                    result = interp.set_current_buffer_id(selected_buffer);
                }
                if result.is_ok() {
                    let default_hooks = interp
                        .default_value("window-configuration-change-hook")
                        .map(|value| value.to_vec().unwrap_or_else(|_| vec![value]))
                        .unwrap_or_default();
                    for hook in default_hooks
                        .into_iter()
                        .filter(|hook| !matches!(hook, Value::T))
                    {
                        if let Err(error) = call_function_value(interp, &hook, &[], env) {
                            result = Err(error);
                            break;
                        }
                    }
                }
                interp.set_selected_window_id(original_window);
                if interp.has_buffer_id(original_buffer) {
                    let restore = interp.set_current_buffer_id(original_buffer);
                    if result.is_ok() {
                        result = restore;
                    }
                }
                result.map(|()| Value::Nil)
            }
            "resize-mini-window-internal" => {
                need_args(name, args, 1)?;
                let window_id = live_window_id_or_selected(interp, Some(&args[0]))?;
                if interp.minibuffer_window_id() != window_id {
                    return Err(LispError::Signal("Not a valid minibuffer window".into()));
                }
                let root_id = window_record_id_from_value(interp, &frame_root_window_value(interp))
                    .ok_or_else(|| LispError::Signal("Cannot resize mini window".into()))?;
                let root_geometry = window_geometry(interp, root_id);
                let mini_geometry = window_geometry(interp, window_id);
                let root_requested = window_slot_value(interp, root_id, WINDOW_NEW_PIXEL_SLOT)
                    .as_integer()
                    .unwrap_or(0);
                let mini_requested = window_slot_value(interp, window_id, WINDOW_NEW_PIXEL_SLOT)
                    .as_integer()
                    .unwrap_or(0);
                let old_height = root_geometry.1 + mini_geometry.1;
                if root_requested <= 0
                    || mini_requested <= 0
                    || root_requested.saturating_add(mini_requested) != old_height
                {
                    return Err(LispError::Signal("Cannot resize mini window".into()));
                }
                set_window_geometry(
                    interp,
                    root_id,
                    (
                        root_geometry.0,
                        root_requested,
                        root_geometry.2,
                        root_geometry.3,
                    ),
                )?;
                set_window_geometry(
                    interp,
                    window_id,
                    (
                        mini_geometry.0,
                        mini_requested,
                        mini_geometry.2,
                        root_geometry.3 + root_requested,
                    ),
                )?;
                Ok(Value::T)
            }
            "window-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                if window_record_id_from_value(interp, &window).is_none() {
                    return Err(LispError::WrongTypeArgument(
                        "windowp".into(),
                        window.clone(),
                    ));
                }
                let Some(buffer_id) = window_buffer_id(interp, &window) else {
                    return Ok(Value::Nil);
                };
                if buffer_id == interp.current_buffer_id() {
                    Ok(Value::buffer(buffer_id, interp.buffer.name.clone()))
                } else if let Some((_, name)) = interp
                    .buffer_list
                    .iter()
                    .find(|(id, _)| *id == buffer_id)
                    .cloned()
                {
                    Ok(Value::buffer(buffer_id, name))
                } else {
                    Ok(Value::Nil)
                }
            }
            "window-old-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(window_slot_value(interp, window_id, WINDOW_OLD_BUFFER_SLOT))
            }
            "window-old-point" => {
                need_arg_range(name, args, 0, 1)?;
                let window_id = live_window_id_or_selected(interp, args.first())?;
                Ok(window_slot_value(interp, window_id, WINDOW_OLD_POINT_SLOT))
            }
            "set-window-buffer" => {
                need_arg_range(name, args, 2, 3)?;
                let keep_margins = args.get(2).is_some_and(Value::is_truthy);
                let window = if args[0].is_nil() {
                    interp.selected_window_value()
                } else {
                    args[0].clone()
                };
                let Some(window_id) = window_record_id_from_value(interp, &window) else {
                    return Err(LispError::WrongTypeArgument(
                        "windowp".into(),
                        window.clone(),
                    ));
                };
                let buffer_id = interp.resolve_buffer_id(&args[1])?;
                let changes_buffer =
                    window_buffer_id(interp, &window).is_some_and(|previous| previous != buffer_id);
                if changes_buffer {
                    if matches!(
                        window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT),
                        Value::T
                    ) {
                        return Err(LispError::Signal(
                            "Window is strongly dedicated to its buffer".into(),
                        ));
                    }
                    set_window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT, Value::Nil)?;
                    if let Ok(record_window_buffer) =
                        interp.lookup_function("record-window-buffer", env)
                    {
                        interp.call_function_value(
                            record_window_buffer,
                            Some("record-window-buffer"),
                            std::slice::from_ref(&window),
                            env,
                        )?;
                    }
                }
                if window_id == interp.selected_window_id() {
                    interp.set_selected_window_buffer_id(buffer_id);
                } else {
                    let Some(record) = interp.find_record_mut(window_id) else {
                        return Err(LispError::WrongTypeArgument(
                            "windowp".into(),
                            window.clone(),
                        ));
                    };
                    if record.slots.len() == WINDOW_BUFFER_SLOT {
                        record.slots.resize(WINDOW_BUFFER_SLOT + 1, Value::Nil);
                    }
                    record.slots[WINDOW_BUFFER_SLOT] = Value::Integer(buffer_id as i64);
                }
                if changes_buffer {
                    let point = interp
                        .get_buffer_by_id(buffer_id)
                        .map(|buffer| buffer.point())
                        .unwrap_or(1);
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_POINT_SLOT,
                        Value::Integer(point as i64),
                    )?;
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_OLD_POINT_SLOT,
                        Value::Integer(point as i64),
                    )?;
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_HSCROLL_SLOT,
                        Value::Integer(0),
                    )?;
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_MIN_HSCROLL_SLOT,
                        Value::Integer(0),
                    )?;
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
                        Value::Nil,
                    )?;
                }
                if !keep_margins {
                    // GNU resets margins from the displayed buffer by
                    // default, including on a same-buffer refresh.  A true
                    // KEEP-MARGINS third argument preserves the window's
                    // explicit widths (Magit relies on that during refresh).
                    let margin_width = |name: &str| {
                        interp
                            .buffer_local_value(buffer_id, name)
                            .and_then(|value| value.as_integer().ok())
                            .filter(|width| *width > 0)
                    };
                    interp.set_window_margins(
                        window_id,
                        margin_width("left-margin-width"),
                        margin_width("right-margin-width"),
                    );
                }
                Ok(Value::Nil)
            }
            "window-list" | "window-list-1" => {
                need_arg_range(name, args, 0, 3)?;
                let start = if name == "window-list" {
                    args.get(2)
                } else {
                    args.first()
                };
                Ok(window_list_value(interp, args.get(1), start))
            }
            "next-window" | "previous-window" => {
                need_arg_range(name, args, 0, 3)?;
                let current = args
                    .first()
                    .filter(|window| !window.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let current_id = window_id_or_selected(interp, &current)?;
                let mut ids = live_ordinary_window_ids(interp);
                let include_minibuffer = matches!(args.get(1), Some(Value::T));
                if include_minibuffer {
                    ids.push(interp.minibuffer_window_id());
                }
                if ids.is_empty() {
                    return Ok(interp.selected_window_value());
                }
                let index = ids.iter().position(|id| *id == current_id).unwrap_or(0);
                let next = if name == "next-window" {
                    (index + 1) % ids.len()
                } else {
                    (index + ids.len() - 1) % ids.len()
                };
                Ok(Value::Record(ids[next]))
            }
            "frame-root-window" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(frame_root_window_value(interp))
            }
            "frame-first-window" => {
                need_arg_range(name, args, 0, 1)?;
                let mut window = frame_root_window_value(interp);
                while let Value::Record(id) = window {
                    let Some(child) = window_link(interp, id, WINDOW_FIRST_CHILD_SLOT) else {
                        return Ok(Value::Record(id));
                    };
                    window = Value::Record(child);
                }
                Ok(interp.selected_window_value())
            }
            "window-prev-buffers" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(window_slot_value(
                    interp,
                    window_id,
                    WINDOW_PREV_BUFFERS_SLOT,
                ))
            }
            "set-window-prev-buffers" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(interp, window_id, WINDOW_PREV_BUFFERS_SLOT, args[1].clone())
            }
            "window-next-buffers" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(window_slot_value(
                    interp,
                    window_id,
                    WINDOW_NEXT_BUFFERS_SLOT,
                ))
            }
            "set-window-next-buffers" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(interp, window_id, WINDOW_NEXT_BUFFERS_SLOT, args[1].clone())
            }
            "window-parameter" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                Ok(window_parameter_value(interp, window_id, &args[1]))
            }
            "set-window-parameter" => {
                need_args(name, args, 3)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_parameter_value(interp, window_id, args[1].clone(), args[2].clone())
            }
            "window-parameters" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
                    .cloned()
                    .unwrap_or(Value::Nil))
            }
            "window-frame" => {
                // emaxx has a single frame; any live window belongs to it.
                need_arg_range(name, args, 0, 1)?;
                Ok(interp.selected_frame_value())
            }
            "windowp" | "window-live-p" | "window-valid-p" => {
                need_args(name, args, 1)?;
                let Some(window_id) = window_record_id_from_value(interp, &args[0]) else {
                    return Ok(Value::Nil);
                };
                let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
                let valid = !matches!(
                    kind,
                    Value::Symbol(ref kind) if kind == DELETED_WINDOW_KIND
                );
                let live = valid && window_buffer_id(interp, &args[0]).is_some();
                Ok(
                    if name == "windowp" || (name == "window-valid-p" && valid) || live {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "window-parent" | "window-prev-sibling" | "window-next-sibling" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = match name {
                    "window-parent" => WINDOW_PARENT_SLOT,
                    "window-prev-sibling" => WINDOW_PREV_SIBLING_SLOT,
                    _ => WINDOW_NEXT_SIBLING_SLOT,
                };
                Ok(window_link(interp, window_id, slot)
                    .map(Value::Record)
                    .unwrap_or(Value::Nil))
            }
            "window-top-child" | "window-left-child" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
                let matching_orientation = matches!(
                    (name, kind),
                    ("window-top-child", Value::Symbol(kind))
                        if kind == INTERNAL_VERTICAL_WINDOW_KIND
                ) || matches!(
                    (name, window_slot_value(interp, window_id, WINDOW_KIND_SLOT)),
                    ("window-left-child", Value::Symbol(kind))
                        if kind == INTERNAL_HORIZONTAL_WINDOW_KIND
                );
                Ok(if matching_orientation {
                    window_link(interp, window_id, WINDOW_FIRST_CHILD_SLOT)
                        .map(Value::Record)
                        .unwrap_or(Value::Nil)
                } else {
                    Value::Nil
                })
            }
            "window-combination-limit" | "set-window-combination-limit" => {
                need_args(
                    name,
                    args,
                    if name == "window-combination-limit" {
                        1
                    } else {
                        2
                    },
                )?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
                if !matches!(
                    kind,
                    Value::Symbol(ref kind)
                        if matches!(
                            kind.as_str(),
                            INTERNAL_HORIZONTAL_WINDOW_KIND | INTERNAL_VERTICAL_WINDOW_KIND
                        )
                ) {
                    return Err(LispError::Signal(
                        "Combination limit is meaningful for internal windows only".into(),
                    ));
                }
                if name == "set-window-combination-limit" {
                    set_window_slot_value(
                        interp,
                        window_id,
                        WINDOW_COMBINATION_LIMIT_SLOT,
                        args[1].clone(),
                    )
                } else {
                    Ok(window_slot_value(
                        interp,
                        window_id,
                        WINDOW_COMBINATION_LIMIT_SLOT,
                    ))
                }
            }
            "window-minibuffer-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let Some(window_id) = window_record_id_from_value(interp, &window) else {
                    return Err(LispError::WrongTypeArgument(
                        "windowp".into(),
                        window.clone(),
                    ));
                };
                let is_minibuffer = interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot, Value::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
                Ok(if is_minibuffer { Value::T } else { Value::Nil })
            }
            "window-at" => {
                need_arg_range(name, args, 2, 3)?;
                Ok(interp.selected_window_value())
            }
            "split-window-internal" => {
                need_args(name, args, 4)?;
                split_window_tree(interp, &args[0], &args[1], &args[2], &args[3])
            }
            "delete-window-internal" => {
                need_args(name, args, 1)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                delete_window_from_tree(interp, window_id)?;
                Ok(Value::Nil)
            }
            "delete-other-windows-internal" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                delete_other_windows_from_tree(interp, window_id)?;
                Ok(Value::Nil)
            }
            "run-window-scroll-functions" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args
                    .first()
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let buffer_id = window_buffer_id(interp, &window).ok_or_else(|| {
                    LispError::TypeError("window-live-p".into(), window.type_name())
                })?;
                let start = Value::Integer(window_start(interp, Some(&window))? as i64);
                let original_buffer = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let hooks = hook_values(interp, "window-scroll-functions", env, Some(buffer_id));
                let mut result = Ok(Value::Nil);
                for hook in hooks {
                    if let Err(error) =
                        call_function_value(interp, &hook, &[window.clone(), start.clone()], env)
                    {
                        result = Err(error);
                        break;
                    }
                }
                interp.set_current_buffer_id(original_buffer)?;
                result
            }
            "window-dedicated-p" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                Ok(window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT))
            }
            "set-window-dedicated-p" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                set_window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT, args[1].clone())
            }
            "posn-at-x-y" => {
                need_arg_range(name, args, 2, 4)?;
                let x = args[0].as_integer()?;
                let y = args[1].as_integer()?;
                let window = args
                    .get(2)
                    .filter(|value| is_window_value(interp, value))
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                Ok(Value::list([
                    window,
                    Value::Nil,
                    Value::cons(Value::Integer(x), Value::Integer(y)),
                    Value::Integer(0),
                ]))
            }
            "posn-at-point" => {
                need_arg_range(name, args, 0, 2)?;
                // Like GNU --batch, Emaxx has no realized glyph matrix from
                // which to derive a screen position.  An interactive
                // session publishes live window geometry, from which the
                // posn's screen coordinates follow (tty pixels are cells).
                let Some(metrics) = interactive_window_metrics() else {
                    return Ok(Value::Nil);
                };
                let pos = match args.first() {
                    None | Some(Value::Nil) => interp.buffer.point(),
                    Some(value) => position_from_value(interp, value)?,
                };
                let point_min = interp.buffer.point_min();
                let point_max = interp.buffer.point_max();
                let start = crate::lisp::primitives::current_window_start(interp)
                    .clamp(point_min, point_max);
                let visible = pos >= start
                    && (pos < metrics.window_end
                        || (pos <= point_max && metrics.window_end >= point_max));
                if !visible {
                    // GNU answers nil for positions outside the window.
                    return Ok(Value::Nil);
                }
                let (bol, eol) = super::buffer_edit::visual_line_bounds(interp, pos);
                let starts = super::buffer_edit::visual_segment_starts(interp, env, bol, eol);
                let seg_index = starts.iter().rposition(|&s| s <= pos).unwrap_or(0);
                let widths = super::buffer_edit::visual_char_widths(interp, env, bol, eol);
                let mut x = 0usize;
                for p in starts[seg_index]..pos {
                    let (raw, _) = widths[p - bol];
                    x += if raw == usize::MAX { 8 - (x % 8) } else { raw };
                }
                // Row within the window: visual rows between window-start
                // and POS's screen line, capped at the window height.
                let mut y = 0usize;
                let (mut walk_bol, mut walk_eol) =
                    super::buffer_edit::visual_line_bounds(interp, start);
                let walk_starts =
                    super::buffer_edit::visual_segment_starts(interp, env, walk_bol, walk_eol);
                let mut walk_seg = walk_starts.iter().rposition(|&s| s <= start).unwrap_or(0);
                while y < metrics.text_height {
                    if walk_bol == bol {
                        y += seg_index.saturating_sub(walk_seg);
                        break;
                    }
                    let segs =
                        super::buffer_edit::visual_segment_starts(interp, env, walk_bol, walk_eol)
                            .len();
                    y += segs.saturating_sub(walk_seg);
                    walk_seg = 0;
                    if walk_eol >= point_max {
                        break;
                    }
                    let bounds = super::buffer_edit::visual_line_bounds(interp, walk_eol + 1);
                    walk_bol = bounds.0;
                    walk_eol = bounds.1;
                }
                let y = y.min(metrics.text_height.saturating_sub(1));
                // keyboard.c's make_lispy_position for a text-area point:
                // (WINDOW POS (X . Y) TIME OBJECT POS (COL . ROW) IMAGE
                //  (DX . DY) (WIDTH . HEIGHT)).
                let xy = Value::cons(Value::Integer(x as i64), Value::Integer(y as i64));
                Ok(Value::list([
                    interp.selected_window_value(),
                    Value::Integer(pos as i64),
                    xy.clone(),
                    Value::Integer(0),
                    Value::Nil,
                    Value::Integer(pos as i64),
                    xy,
                    Value::Nil,
                    Value::cons(Value::Integer(0), Value::Integer(0)),
                    Value::cons(Value::Integer(1), Value::Integer(1)),
                ]))
            }
            "window-display-table" | "window-cursor-type" => {
                need_arg_range(name, args, 0, 1)?;
                let window = args.first().cloned().unwrap_or(Value::Nil);
                let window_id = window_id_or_selected(interp, &window)?;
                let slot = if name == "window-display-table" {
                    WINDOW_DISPLAY_TABLE_SLOT
                } else {
                    WINDOW_CURSOR_TYPE_SLOT
                };
                Ok(interp
                    .find_record(window_id)
                    .and_then(|record| record.slots.get(slot))
                    .cloned()
                    .unwrap_or(if name == "window-cursor-type" {
                        Value::T
                    } else {
                        Value::Nil
                    }))
            }
            "set-window-display-table" | "set-window-cursor-type" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                if name == "set-window-cursor-type" && !valid_window_cursor_type(&args[1]) {
                    return Err(LispError::Signal("Invalid cursor type".into()));
                }
                let slot = if name == "set-window-display-table" {
                    WINDOW_DISPLAY_TABLE_SLOT
                } else {
                    WINDOW_CURSOR_TYPE_SLOT
                };
                set_window_slot_value(interp, window_id, slot, args[1].clone())
            }
            "window-fringes" => {
                need_arg_range(name, args, 0, 1)?;
                window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Nil,
                    Value::Nil,
                ]))
            }
            "set-window-fringes" => {
                need_arg_range(name, args, 2, 5)?;
                window_id_or_selected(interp, &args[0])?;
                // GNU's TTY implementation leaves fringe settings unchanged.
                Ok(Value::Nil)
            }
            "window-scroll-bars" => {
                need_arg_range(name, args, 0, 1)?;
                window_buffer_id_or_selected(interp, args.first())?;
                Ok(Value::list([
                    Value::Nil,
                    Value::Integer(0),
                    Value::T,
                    Value::Nil,
                    Value::Integer(0),
                    Value::T,
                    Value::Nil,
                ]))
            }
            "set-window-scroll-bars" => {
                need_arg_range(name, args, 1, 6)?;
                window_id_or_selected(interp, &args[0])?;
                // GNU's TTY implementation leaves scroll-bar settings unchanged.
                Ok(Value::Nil)
            }
            "send-string-to-terminal" => {
                need_arg_range(name, args, 1, 2)?;
                let _ = string_text(&args[0])?;
                Ok(Value::Nil)
            }
            "get-buffer-window" => {
                need_arg_range(name, args, 0, 3)?;
                let buffer_id = if let Some(buffer) = args.first() {
                    if buffer.is_nil() {
                        Some(interp.current_buffer_id())
                    } else if let Some(string) = string_like(buffer) {
                        interp.find_buffer(&string.text).map(|(id, _)| id)
                    } else {
                        Some(interp.resolve_buffer_id(buffer)?)
                    }
                } else {
                    Some(interp.current_buffer_id())
                };
                Ok(buffer_id
                    .and_then(|buffer_id| {
                        live_ordinary_window_ids(interp)
                            .into_iter()
                            .find(|id| {
                                window_buffer_id(interp, &Value::Record(*id)) == Some(buffer_id)
                            })
                            .or_else(|| {
                                // ALL-FRAMES t considers the minibuffer
                                // window when it is active (GNU's
                                // choose-completion-string finds the
                                // minibuffer through this).
                                if !matches!(args.get(1), Some(Value::T)) {
                                    return None;
                                }
                                interp.active_minibuffer_buffer_id()?;
                                match interp.minibuffer_window_value() {
                                    Value::Record(id)
                                        if window_buffer_id(interp, &Value::Record(id))
                                            == Some(buffer_id) =>
                                    {
                                        Some(id)
                                    }
                                    _ => None,
                                }
                            })
                    })
                    .map(Value::Record)
                    .unwrap_or(Value::Nil))
            }
            "minibuffer-window" => Ok(interp.minibuffer_window_value()),
            "set-minibuffer-window" => {
                need_args(name, args, 1)?;
                let Some(window_id) = window_record_id_from_value(interp, &args[0]) else {
                    return Err(wrong_type_argument("windowp", args[0].clone()));
                };
                let is_minibuffer = interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot, Value::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
                if !is_minibuffer {
                    return Err(LispError::Signal(
                        "Window is not a minibuffer window".into(),
                    ));
                }
                interp.set_minibuffer_window_id(window_id);
                Ok(args[0].clone())
            }
            "minibuffer-selected-window" => Ok(interp
                .minibuffer_selected_window_id()
                .map(Value::Record)
                .unwrap_or_else(|| interp.selected_window_value())),
            "active-minibuffer-window" => {
                // Non-nil while a minibuffer-with-setup-hook hook runs (the
                // approximation of GNU's activated minibuffer).
                if interp.active_minibuffer_buffer_id().is_some() {
                    Ok(interp
                        .active_minibuffer_window_value()
                        .unwrap_or_else(|| interp.selected_window_value()))
                } else {
                    Ok(Value::Nil)
                }
            }
            "set-window-start" => {
                need_arg_range(name, args, 2, 4)?;
                let start = position_from_value(interp, &args[1])?;
                set_window_start_value(interp, &args[0], start)?;
                Ok(Value::T)
            }
            "set-window-point" => {
                need_args(name, args, 2)?;
                let window_id = window_id_or_selected(interp, &args[0])?;
                let pos = position_from_value(interp, &args[1])?;
                // GNU: setting the selected window's point moves point in the
                // window's buffer (emaxx has no separate window points).
                if window_id == interp.selected_window_id() {
                    let buffer_id = window_buffer_id(interp, &Value::Record(window_id))
                        .unwrap_or_else(|| interp.current_buffer_id());
                    if buffer_id == interp.current_buffer_id() {
                        interp.buffer.goto_char(pos);
                    } else if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                        buffer.goto_char(pos);
                    }
                }
                set_window_slot_value(
                    interp,
                    window_id,
                    WINDOW_POINT_SLOT,
                    Value::Integer(pos as i64),
                )?;
                Ok(args[1].clone())
            }
            "set-window-vscroll" => {
                need_arg_range(name, args, 2, 4)?;
                let _ = args[1].as_integer()?;
                Ok(Value::Integer(0))
            }
        }
    }
);

// ── Mode-line rendering (xdisp.c's display_mode_element) ───────────────
//
// The engine renders GNU's dumped mode-line specification to plain text:
// faces, mouse maps, and help-echo do not change the characters a tty
// draws.  %-constructs follow decode_mode_spec; the geometry-dependent
// ones (%p, %P) read the live window metrics the frontend publishes.

/// Render a mode-line construct to its display text.  LITERAL marks
/// strings reached through a symbol's value, which GNU shows verbatim
/// (%-constructs are only recognized in strings written in the spec).
#[allow(clippy::too_many_arguments)]
fn render_mode_line_element(
    interp: &mut Interpreter,
    env: &mut Env,
    element: &Value,
    literal: bool,
    glass: bool,
    depth: usize,
    offset: usize,
    spans: &mut Vec<(usize, usize, Value)>,
) -> Result<String, LispError> {
    if depth > 32 {
        return Ok(String::new());
    }
    match element {
        value if value.is_string() => {
            let string =
                string_like(value).ok_or_else(|| wrong_type_argument("stringp", value.clone()))?;
            let (text, display_offsets) = if glass && !string.props.is_empty() {
                render_mode_line_string_display_properties(
                    interp,
                    env,
                    &string.text,
                    &string.props,
                    offset,
                )?
            } else {
                let length = string.text.chars().count();
                (string.text, (0..=length).collect())
            };
            let expands_percent_constructs = !literal && text.contains('%');
            let rendered = if literal {
                text
            } else {
                expand_mode_line_percent_constructs(interp, env, &text)?
            };
            // GNU maps a template's text properties onto its expansion;
            // the shipped formats propertize whole templates (%12b's
            // buffer-id face), so a face covering the template covers
            // the expansion.
            if let Value::StringObject(state) = value {
                let state = state.borrow();
                let source_length = state.text.chars().count();
                for property_span in &state.props {
                    let Some(face) = property_span
                        .props
                        .iter()
                        .find(|(name, _)| name == "face")
                        .or_else(|| {
                            property_span
                                .props
                                .iter()
                                .find(|(name, _)| name == "font-lock-face")
                        })
                        .map(|(_, face)| face.clone())
                    else {
                        continue;
                    };
                    if expands_percent_constructs {
                        if property_span.start == 0 && property_span.end >= source_length {
                            spans.push((offset, offset + rendered.chars().count(), face));
                        }
                    } else {
                        let from = display_offsets
                            .get(property_span.start.min(source_length))
                            .copied()
                            .unwrap_or(0);
                        let to = display_offsets
                            .get(property_span.end.min(source_length))
                            .copied()
                            .unwrap_or_else(|| rendered.chars().count());
                        spans.push((offset + from, offset + to, face));
                    }
                }
            }
            Ok(rendered)
        }
        Value::Symbol(name) => {
            if name == "t" || name == "nil" {
                return Ok(String::new());
            }
            // Only a direct string value is shown verbatim; list values
            // are full constructs whose strings carry %-specs (xdisp's
            // display_mode_element symbol case).
            let value = interp.lookup_var(name, env).unwrap_or(Value::Nil);
            render_mode_line_element(
                interp,
                env,
                &value,
                value.is_string(),
                glass,
                depth + 1,
                offset,
                spans,
            )
        }
        Value::Cons(_) => {
            let items = element.to_vec().unwrap_or_default();
            let Some(head) = items.first() else {
                return Ok(String::new());
            };
            match head {
                Value::Symbol(keyword) if keyword == ":eval" => {
                    let Some(form) = items.get(1) else {
                        return Ok(String::new());
                    };
                    // Mode-line evaluation never signals in GNU
                    // (safe_eval); a broken construct renders empty.
                    let result =
                        crate::lisp::primitives::eval_impl(interp, std::slice::from_ref(form), env)
                            .unwrap_or(Value::Nil);
                    render_mode_line_element(
                        interp,
                        env,
                        &result,
                        false,
                        glass,
                        depth + 1,
                        offset,
                        spans,
                    )
                }
                Value::Symbol(keyword) if keyword == ":propertize" => {
                    let Some(inner) = items.get(1) else {
                        return Ok(String::new());
                    };
                    // The outer span goes in before the recursion so any
                    // nested :propertize face lands after it and wins on
                    // overlap when the spans apply in order.
                    let face = items[2..].chunks(2).find_map(|pair| match pair {
                        [Value::Symbol(key), value] if key == "face" => Some(value.clone()),
                        _ => None,
                    });
                    let span_index = face.map(|face| {
                        spans.push((offset, offset, face));
                        spans.len() - 1
                    });
                    let mut text = render_mode_line_element(
                        interp,
                        env,
                        inner,
                        false,
                        glass,
                        depth + 1,
                        offset,
                        spans,
                    )?;
                    // display (min-width (N.0)) only affects the glass:
                    // format-mode-line's string ignores it, while the
                    // display engine emits a stretch glyph — one column
                    // wide even when the span already exceeds the
                    // minimum (produce_stretch_glyph floors width at 1).
                    if glass {
                        let mut properties = items[2..].chunks(2);
                        if let Some(min_width) = properties.find_map(|pair| match pair {
                            [Value::Symbol(key), value] if key == "display" => {
                                mode_line_min_width(value)
                            }
                            _ => None,
                        }) {
                            let actual = text.chars().count();
                            let pad = if actual < min_width {
                                min_width - actual
                            } else if actual == min_width {
                                0
                            } else {
                                1
                            };
                            text.extend(std::iter::repeat_n(' ', pad));
                        }
                    }
                    if let Some(index) = span_index {
                        spans[index].1 = offset + text.chars().count();
                    }
                    Ok(text)
                }
                Value::Symbol(condition) => {
                    // (SYMBOL THEN [ELSE]): a variable-conditioned construct.
                    let value = interp.lookup_var(condition, env).unwrap_or(Value::Nil);
                    let branch = if value.is_truthy() {
                        items.get(1)
                    } else {
                        items.get(2)
                    };
                    match branch {
                        Some(branch) => render_mode_line_element(
                            interp,
                            env,
                            branch,
                            false,
                            glass,
                            depth + 1,
                            offset,
                            spans,
                        ),
                        None => Ok(String::new()),
                    }
                }
                Value::Integer(width) => {
                    // (WIDTH REST...): pad to WIDTH, or truncate to -WIDTH.
                    let width = *width;
                    let mut text = String::new();
                    for item in &items[1..] {
                        text.push_str(&render_mode_line_element(
                            interp,
                            env,
                            item,
                            false,
                            glass,
                            depth + 1,
                            offset + text.chars().count(),
                            spans,
                        )?);
                    }
                    if width >= 0 {
                        let width = width as usize;
                        while text.chars().count() < width {
                            text.push(' ');
                        }
                    } else {
                        let limit = (-width) as usize;
                        if text.chars().count() > limit {
                            text = text.chars().take(limit).collect();
                        }
                    }
                    Ok(text)
                }
                _ => {
                    let mut text = String::new();
                    for item in &items {
                        text.push_str(&render_mode_line_element(
                            interp,
                            env,
                            item,
                            false,
                            glass,
                            depth + 1,
                            offset + text.chars().count(),
                            spans,
                        )?);
                    }
                    Ok(text)
                }
            }
        }
        _ => Ok(String::new()),
    }
}

fn render_mode_line_string_display_properties(
    interp: &mut Interpreter,
    env: &mut Env,
    text: &str,
    properties: &[crate::buffer::TextPropertySpan],
    offset: usize,
) -> Result<(String, Vec<usize>), LispError> {
    let mut rendered = String::new();
    let mut display_offsets = Vec::with_capacity(text.chars().count() + 1);
    display_offsets.push(0);
    let mut column = offset;
    for (index, character) in text.chars().enumerate() {
        let display = properties
            .iter()
            .find(|span| span.start <= index && index < span.end)
            .and_then(|span| {
                span.props
                    .iter()
                    .find(|(name, _)| name == "display")
                    .map(|(_, value)| value)
            });
        let align_to = display.and_then(|value| {
            let items = value.to_vec().ok()?;
            if !matches!(items.first(), Some(Value::Symbol(head)) if head == "space") {
                return None;
            }
            let index = items
                .iter()
                .position(|item| matches!(item, Value::Symbol(name) if name == ":align-to"))?;
            let target = items.get(index + 1)?;
            match target {
                Value::Integer(target) => usize::try_from(*target).ok(),
                expression => crate::lisp::primitives::eval_impl(
                    interp,
                    std::slice::from_ref(expression),
                    env,
                )
                .ok()
                .and_then(|value| value.as_integer().ok())
                .and_then(|target| usize::try_from(target).ok()),
            }
        });
        if let Some(target) = align_to {
            let padding = target.saturating_sub(column);
            rendered.extend(std::iter::repeat_n(' ', padding));
            column += padding;
        } else {
            rendered.push(character);
            column += if character == '\t' {
                8 - (column % 8)
            } else {
                1
            };
        }
        display_offsets.push(rendered.chars().count());
    }
    Ok((rendered, display_offsets))
}

pub(super) fn render_mode_line_construct(
    interp: &mut Interpreter,
    env: &mut Env,
    format: &Value,
    depth: usize,
) -> Result<String, LispError> {
    render_mode_line_element(interp, env, format, false, false, depth, 0, &mut Vec::new())
}

/// The selected window's mode line as the display engine paints it:
/// format-mode-line's text plus the glass-only stretch behavior of
/// `min-width' display properties.
#[cfg(test)]
pub(crate) fn render_mode_line_glass(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<String, LispError> {
    let format = interp
        .lookup_var("mode-line-format", env)
        .unwrap_or(Value::Nil);
    render_mode_line_element(interp, env, &format, false, true, 0, 0, &mut Vec::new())
}

/// The glass mode line together with its `:propertize' face spans, in
/// char offsets — the frontend paints the spans over the mode-line face
/// (the buffer name's `mode-line-buffer-id' bold).
/// The `(min-width (N.0))' display specification's width, if VALUE is one.
fn mode_line_min_width(value: &Value) -> Option<usize> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(key), width] if key == "min-width" => {
            let widths = width.to_vec().ok()?;
            match widths.first()? {
                Value::Float(width) => Some(*width as usize),
                Value::Integer(width) => Some(*width as usize),
                _ => None,
            }
        }
        _ => None,
    }
}

/// decode_mode_spec: expand the %-constructs of a spec string.
fn expand_mode_line_percent_constructs(
    interp: &mut Interpreter,
    env: &mut Env,
    text: &str,
) -> Result<String, LispError> {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let mut field = 0usize;
        while let Some(digit) = chars.peek().and_then(|c| c.to_digit(10)) {
            field = field * 10 + digit as usize;
            chars.next();
        }
        let Some(spec) = chars.next() else {
            break;
        };
        let expanded = decode_mode_line_spec(interp, env, spec)?;
        let mut expanded = expanded;
        while expanded.chars().count() < field {
            expanded.push(' ');
        }
        out.push_str(&expanded);
    }
    Ok(out)
}

fn decode_mode_line_spec(
    interp: &mut Interpreter,
    env: &mut Env,
    spec: char,
) -> Result<String, LispError> {
    let var = |interp: &Interpreter, name: &str| -> Value {
        interp.lookup_var(name, &Vec::new()).unwrap_or(Value::Nil)
    };
    Ok(match spec {
        '%' => "%".to_string(),
        'b' => interp.buffer.name.clone(),
        'f' => match super::call(interp, "buffer-file-name", &[], env) {
            Ok(path) if path.is_string() => string_text(&path)?,
            _ => String::new(),
        },
        'F' => match super::call(
            interp,
            "frame-parameter",
            &[Value::Nil, Value::Symbol("name".into())],
            env,
        ) {
            Ok(name) if name.is_string() => string_text(&name)?,
            _ => "F1".to_string(),
        },
        'l' => {
            let point_line = interp.buffer.line_number_at_pos(interp.buffer.point());
            let first_accessible_line = interp.buffer.line_number_at_pos(interp.buffer.point_min());
            point_line
                .saturating_sub(first_accessible_line)
                .saturating_add(1)
                .to_string()
        }
        'c' | 'C' => {
            let column = super::call(interp, "current-column", &[], env)?
                .as_integer()
                .unwrap_or(0);
            (column + i64::from(spec == 'C')).to_string()
        }
        'i' => (interp.buffer.point_max() - interp.buffer.point_min()).to_string(),
        'I' => human_readable_size(interp.buffer.point_max() - interp.buffer.point_min()),
        'p' | 'P' => window_percent_spec(interp, spec == 'P'),
        'n' => {
            // xdisp.c:28812 decode_mode_spec case 'n': pure C accessibility
            // checks (BUF_BEGV > BUF_BEG || BUF_ZV < BUF_Z) -- GNU never
            // consults the Lisp-owned `buffer-narrowed-p' here, and the old
            // native dispatch of that name was both a gate escape (it went
            // through `super::call', which the anti-cheat regex missed) and
            // half wrong: it ignored narrowing at the buffer's end.
            let narrowed = interp.buffer.point_min() > 1
                || interp.buffer.point_max() < interp.buffer.size_total() + 1;
            if narrowed {
                " Narrow".to_string()
            } else {
                String::new()
            }
        }
        '*' => {
            if var(interp, "buffer-read-only").is_truthy() {
                "%".to_string()
            } else if interp.buffer.is_modified() {
                "*".to_string()
            } else {
                "-".to_string()
            }
        }
        '+' => {
            if interp.buffer.is_modified() {
                "*".to_string()
            } else if var(interp, "buffer-read-only").is_truthy() {
                "%".to_string()
            } else {
                "-".to_string()
            }
        }
        '&' => {
            if interp.buffer.is_modified() {
                "*".to_string()
            } else {
                "-".to_string()
            }
        }
        '@' => {
            // xdisp.c:28909 case '@': `dsafe_call1 (Qfile_remote_p, curdir)'
            // -- GNU's C reaches the Lisp-owned `file-remote-p' through the
            // function cell and treats any error as nil; so must Emaxx
            // (finding 67's second catch).
            let remote = crate::lisp::primitives::call_named_function(
                interp,
                "file-remote-p",
                &[var(interp, "default-directory")],
                env,
            )
            .map(|value| value.is_truthy())
            .unwrap_or(false);
            if remote { "@" } else { "-" }.to_string()
        }
        'z' | 'Z' => {
            // Keyboard and terminal coding mnemonics (tty frames only in
            // GNU; this renderer only runs interactively), then the
            // buffer's; %Z appends the end-of-line mnemonic.
            let mut text = String::new();
            for source in ["keyboard-coding-system", "terminal-coding-system"] {
                let coding = super::call(interp, source, &[], env).unwrap_or(Value::Nil);
                text.push(coding_mnemonic_char(interp, env, &coding));
            }
            let buffer_coding = var(interp, "buffer-file-coding-system");
            text.push(coding_mnemonic_char(interp, env, &buffer_coding));
            if spec == 'Z' {
                // xdisp.c:28455: the eol mnemonic comes from the
                // `eol-mnemonic-*' variables keyed by the coding system's
                // eol type -- GNU never calls the Lisp-owned
                // `coding-system-eol-type-mnemonic' here (finding 67's
                // third catch; the old dispatch was a gate escape).
                let eol_type = super::call(interp, "coding-system-eol-type", &[buffer_coding], env)
                    .unwrap_or(Value::Nil);
                let mnemonic_variable = match &eol_type {
                    Value::Integer(0) => "eol-mnemonic-unix",
                    Value::Integer(1) => "eol-mnemonic-dos",
                    Value::Integer(2) => "eol-mnemonic-mac",
                    _ => "eol-mnemonic-undecided",
                };
                let mnemonic = var(interp, mnemonic_variable);
                if let Some(mnemonic) = string_like(&mnemonic) {
                    text.push_str(&mnemonic.text);
                }
            }
            text
        }
        'm' => {
            let mode_name = var(interp, "mode-name");
            render_mode_line_element(interp, env, &mode_name, true, false, 0, 0, &mut Vec::new())?
        }
        'M' => {
            let global = var(interp, "global-mode-string");
            render_mode_line_element(interp, env, &global, true, false, 0, 0, &mut Vec::new())?
        }
        's' => {
            // xdisp.c:28899 case 's': the status of the current buffer's
            // process -- Fget_buffer_process, "no process" when there is
            // none, else the `process-status' symbol's name.
            match super::call(interp, "get-buffer-process", &[Value::Nil], env) {
                Ok(process) if process.is_truthy() => {
                    match super::call(interp, "process-status", &[process], env) {
                        Ok(Value::Symbol(status)) => status.to_string(),
                        _ => String::new(),
                    }
                }
                _ => "no process".to_string(),
            }
        }
        // Recursive-editing depth brackets: the frontend runs at level 0.
        '[' | ']' => String::new(),
        'e' => String::new(),
        '-' => "-".repeat(256),
        other => other.to_string(),
    })
}

// `coding-system-mnemonic' is GNU mule.el's; reach it through the
// ordinary function cell, never the native dispatcher.
fn coding_mnemonic_char(interp: &mut Interpreter, env: &mut Env, coding: &Value) -> char {
    // xdisp.c:28432: an undefined spec ("Not yet decided") renders '-' in
    // a multibyte buffer and ' ' in a unibyte one WITHOUT consulting any
    // mnemonic.  (terminal-coding-system) is nil on a fresh tty and GNU
    // shows '-' there, while (coding-system-mnemonic nil) would answer
    // '=' by resolving nil to no-conversion.
    let defined = !coding.is_nil()
        && super::call(interp, "coding-system-p", std::slice::from_ref(coding), env)
            .is_ok_and(|value| value.is_truthy());
    if !defined {
        return if interp.buffer.is_multibyte() {
            '-'
        } else {
            ' '
        };
    }
    // xdisp.c:28421 `decode_mode_spec_coding' reads CODING_ATTR_MNEMONIC
    // out of the coding system's attribute vector; the same datum reaches
    // Lisp as the `:mnemonic' entry of the C-owned `coding-system-plist'.
    // The Lisp-owned `coding-system-mnemonic' wrapper is exactly that
    // plist-get, and dispatching it natively was a gate escape
    // (finding 67).
    let plist = super::call(
        interp,
        "coding-system-plist",
        std::slice::from_ref(coding),
        env,
    )
    .unwrap_or(Value::Nil);
    match super::call(
        interp,
        "plist-get",
        &[plist, Value::symbol(":mnemonic")],
        env,
    ) {
        Ok(Value::Integer(code)) => char::from_u32(code as u32).unwrap_or('-'),
        _ => '-',
    }
}

/// xdisp.c's percent99: a ceiling percentage capped at 99.
fn percent99(n: usize, d: usize) -> usize {
    let d = d.max(1);
    (100 * n).div_ceil(d).min(99)
}

/// %p / %P: window position within the buffer, from the live metrics
/// (decode_mode_spec's 'p' and 'P', "%2d%%" formatting included; the
/// spec's own -3 width truncates "Bottom" to "Bot" downstream).
fn window_percent_spec(interp: &Interpreter, of_bottom: bool) -> String {
    let Some(metrics) = crate::lisp::primitives::interactive_window_metrics() else {
        return String::new();
    };
    let point_min = interp.buffer.point_min();
    let point_max = interp.buffer.point_max();
    let start = crate::lisp::primitives::current_window_start(interp).clamp(point_min, point_max);
    let window_end = metrics.window_end.clamp(point_min, point_max);
    if of_bottom {
        // %P: percent of buffer above the window bottom.
        if window_end >= point_max {
            return if start <= point_min { "All" } else { "Bottom" }.to_string();
        }
        let above_top = start - point_min;
        let percent = percent99(above_top, above_top + (point_max - window_end));
        return format!("{percent:2}%");
    }
    // %p: percent of buffer above the window top.
    if window_end >= point_max {
        return if start <= point_min { "All" } else { "Bottom" }.to_string();
    }
    if start <= point_min {
        return "Top".to_string();
    }
    format!("{:2}%", percent99(start - point_min, point_max - point_min))
}

/// pint2hrstr: a size with k/M/G units, at most four characters.
fn human_readable_size(size: usize) -> String {
    if size < 10_000 {
        return size.to_string();
    }
    let mut quotient = size as f64;
    for unit in ['k', 'M', 'G'] {
        quotient /= 1000.0;
        if quotient < 10.0 {
            return format!("{quotient:.1}{unit}");
        }
        if quotient < 1000.0 {
            return format!("{}{unit}", quotient.round() as usize);
        }
    }
    format!("{}T", (quotient / 1000.0).round() as usize)
}
