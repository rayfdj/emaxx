use super::{
    beginning_of_line_at, line_distance, move_lines_from, prefix_numeric_value, signal_condition,
};
use crate::lisp::eval::Interpreter;
use crate::lisp::types::{Env, LispError, Value};

pub(crate) const DEFAULT_SELECTED_WINDOW_HEIGHT: usize = 24;
pub(crate) const WINDOW_BUFFER_SLOT: usize = 0;
pub(crate) const WINDOW_START_SLOT: usize = 1;
pub(crate) const WINDOW_OLD_POINT_SLOT: usize = 2;
pub(crate) const WINDOW_KIND_SLOT: usize = 3;
pub(crate) const WINDOW_PARAMETERS_SLOT: usize = 4;
pub(crate) const WINDOW_PREV_BUFFERS_SLOT: usize = 5;
pub(crate) const WINDOW_NEXT_BUFFERS_SLOT: usize = 6;
pub(crate) const WINDOW_DEDICATED_SLOT: usize = 7;
pub(crate) const WINDOW_NEW_PIXEL_SLOT: usize = 8;
pub(crate) const WINDOW_NEW_TOTAL_SLOT: usize = 9;
pub(crate) const WINDOW_NEW_NORMAL_SLOT: usize = 10;
pub(crate) const WINDOW_DISPLAY_TABLE_SLOT: usize = 11;
pub(crate) const WINDOW_CURSOR_TYPE_SLOT: usize = 12;
pub(crate) const WINDOW_PARENT_SLOT: usize = 13;
pub(crate) const WINDOW_PREV_SIBLING_SLOT: usize = 14;
pub(crate) const WINDOW_NEXT_SIBLING_SLOT: usize = 15;
pub(crate) const WINDOW_FIRST_CHILD_SLOT: usize = 16;
pub(crate) const WINDOW_PIXEL_WIDTH_SLOT: usize = 17;
pub(crate) const WINDOW_PIXEL_HEIGHT_SLOT: usize = 18;
pub(crate) const WINDOW_PIXEL_LEFT_SLOT: usize = 19;
pub(crate) const WINDOW_PIXEL_TOP_SLOT: usize = 20;
pub(crate) const WINDOW_NORMAL_WIDTH_SLOT: usize = 21;
pub(crate) const WINDOW_NORMAL_HEIGHT_SLOT: usize = 22;
pub(crate) const WINDOW_COMBINATION_LIMIT_SLOT: usize = 23;
pub(crate) const WINDOW_USE_TIME_SLOT: usize = 24;
pub(crate) const WINDOW_HSCROLL_SLOT: usize = 25;
pub(crate) const WINDOW_MIN_HSCROLL_SLOT: usize = 26;
pub(crate) const WINDOW_SUSPEND_AUTO_HSCROLL_SLOT: usize = 27;
pub(crate) const WINDOW_OLD_BUFFER_SLOT: usize = 28;
pub(crate) const WINDOW_POINT_SLOT: usize = 29;
pub(crate) const MINIBUFFER_WINDOW_KIND: &str = "minibuffer";
pub(crate) const INTERNAL_HORIZONTAL_WINDOW_KIND: &str = "internal-horizontal";
pub(crate) const INTERNAL_VERTICAL_WINDOW_KIND: &str = "internal-vertical";
pub(crate) const DELETED_WINDOW_KIND: &str = "deleted";

pub(crate) fn window_record_slots(
    buffer_id: Option<u64>,
    start: usize,
    kind: Value,
    geometry: (i64, i64, i64, i64),
) -> Vec<Value> {
    let (width, height, left, top) = geometry;
    vec![
        buffer_id
            .map(|id| Value::Integer(id as i64))
            .unwrap_or(Value::Nil),
        Value::Integer(start as i64),
        Value::Integer(start as i64),
        kind,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(0),
        Value::Nil,
        Value::T,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Integer(width),
        Value::Integer(height),
        Value::Integer(left),
        Value::Integer(top),
        Value::Float(1.0),
        Value::Float(1.0),
        Value::Nil,
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(0),
        Value::Nil,
        Value::Nil,
        Value::Integer(start as i64),
    ]
}

pub(crate) fn current_window_start(interp: &Interpreter) -> usize {
    interp.selected_window_start()
}

pub(crate) fn buffer_point_bounds(interp: &Interpreter, buffer_id: u64) -> (usize, usize) {
    interp
        .buffer_bounds_by_id(buffer_id)
        .unwrap_or((interp.buffer.point_min(), interp.buffer.point_max()))
}

fn buffer_line_start_at(interp: &Interpreter, buffer_id: u64, pos: usize) -> usize {
    if buffer_id == interp.current_buffer_id() {
        interp.buffer.line_start_at(pos)
    } else {
        interp
            .get_buffer_by_id(buffer_id)
            .map(|buffer| buffer.line_start_at(pos))
            .unwrap_or_else(|| interp.buffer.line_start_at(pos))
    }
}

pub(crate) fn set_current_window_start(interp: &mut Interpreter, start: usize) {
    let buffer_id = interp.selected_window_buffer_id();
    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
    let start = start.clamp(point_min, point_max);
    let start = buffer_line_start_at(interp, buffer_id, start);
    interp.set_selected_window_start(start);
}

pub(crate) fn window_record_id_from_value(interp: &Interpreter, value: &Value) -> Option<u64> {
    match value {
        Value::Record(id)
            if interp
                .find_record(*id)
                .is_some_and(|record| record.type_name == "window") =>
        {
            Some(*id)
        }
        Value::Symbol(symbol) if symbol == "window" => Some(interp.selected_window_id()),
        _ => None,
    }
}

pub(crate) fn window_buffer_id(interp: &Interpreter, value: &Value) -> Option<u64> {
    match window_record_id_from_value(interp, value) {
        Some(id) if id == interp.selected_window_id() => {
            let buffer_id = interp.selected_window_buffer_id();
            interp.has_buffer_id(buffer_id).then_some(buffer_id)
        }
        Some(id) => interp
            .find_record(id)
            .and_then(|record| record.slots.get(WINDOW_BUFFER_SLOT))
            .and_then(|slot| slot.as_integer().ok())
            .map(|buffer_id| buffer_id.max(0) as u64)
            .filter(|buffer_id| interp.has_buffer_id(*buffer_id)),
        None => None,
    }
}

pub(crate) fn window_start(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<usize, LispError> {
    match value {
        None | Some(Value::Nil) => Ok(current_window_start(interp)),
        Some(value) => {
            let Some(id) = window_record_id_from_value(interp, value) else {
                return Err(LispError::TypeError("window".into(), value.type_name()));
            };
            if id == interp.selected_window_id() {
                return Ok(current_window_start(interp));
            }
            let buffer_id = window_buffer_id(interp, value).unwrap_or(interp.current_buffer_id());
            let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
            Ok(interp
                .find_record(id)
                .and_then(|record| record.slots.get(WINDOW_START_SLOT))
                .and_then(|slot| slot.as_integer().ok())
                .map(|start| start.clamp(point_min as i64, point_max as i64) as usize)
                .unwrap_or(point_min))
        }
    }
}

pub(crate) fn set_window_start_value(
    interp: &mut Interpreter,
    window: &Value,
    start: usize,
) -> Result<(), LispError> {
    let Some(id) = window_record_id_from_value(interp, window) else {
        return Err(LispError::TypeError("window".into(), window.type_name()));
    };
    let buffer_id = window_buffer_id(interp, window).unwrap_or(interp.current_buffer_id());
    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
    let start = start.clamp(point_min, point_max);
    let start = buffer_line_start_at(interp, buffer_id, start);
    if id == interp.selected_window_id() {
        interp.set_selected_window_start(start);
        return Ok(());
    }
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::TypeError("window".into(), window.type_name()));
    };
    if record.slots.len() <= WINDOW_START_SLOT {
        record.slots.resize(WINDOW_START_SLOT + 1, Value::Nil);
    }
    record.slots[WINDOW_START_SLOT] = Value::Integer(start as i64);
    Ok(())
}

fn scroll_preserve_screen_position(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("scroll-preserve-screen-position", env)
        .is_some_and(|value| !value.is_nil())
}

pub(crate) fn resolve_window_line(
    value: Option<&Value>,
    default_line: usize,
) -> Result<isize, LispError> {
    let line = match value {
        None | Some(Value::Nil) => default_line as i64,
        Some(value) => prefix_numeric_value(value)?.as_integer()?,
    };
    Ok(if line >= 0 {
        line as isize
    } else {
        (DEFAULT_SELECTED_WINDOW_HEIGHT as isize + line as isize).max(0)
    })
}

pub(crate) fn scroll_selected_window(
    interp: &mut Interpreter,
    count: isize,
    env: &Env,
) -> Result<(), LispError> {
    let window_start = current_window_start(interp);
    let point_line = beginning_of_line_at(interp, interp.buffer.point());
    let point_offset = line_distance(interp, window_start, point_line);
    let (new_start, shortage) = move_lines_from(interp, window_start, count);
    if shortage != 0 {
        return Err(if count >= 0 {
            signal_condition("end-of-buffer")
        } else {
            signal_condition("beginning-of-buffer")
        });
    }

    set_current_window_start(interp, new_start);

    if scroll_preserve_screen_position(interp, env) || point_line < new_start {
        let (target, target_shortage) = move_lines_from(interp, new_start, point_offset as isize);
        if target_shortage > 0 {
            interp.buffer.goto_char(interp.buffer.point_max());
        } else {
            interp.buffer.goto_char(target);
        }
    }

    Ok(())
}
