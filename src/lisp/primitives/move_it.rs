//! What xdisp.c's display iterator does on its way between screen lines
//! that the terminal's row model (tty.rs's screen line layouts) does not
//! show by itself: where reseat's handle_stop leaves the iterator at a
//! position (invisible text and an empty `display' string skipped), where
//! move_it_to stops short of a position, back_to_previous_visible_line_start,
//! and move_it_by_lines' search backward, which reseats at a line start.
//! Fvertical_motion and Frecenter walk these on a live display.

use super::{InvisibilitySpec, invisible_run_at, resolve_buffer_invisibility};
use crate::buffer::Buffer;
use crate::lisp::eval::Interpreter;
use crate::lisp::types::{Env, Kind, Value};

/// A `display' string the iterator delivers in place of buffer text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DisplayString {
    /// In the text area, not a margin (it->area == TEXT_AREA).
    pub(crate) text_area: bool,
    /// The newlines it holds.
    pub(crate) newlines: usize,
}

/// Where handle_stop leaves an iterator reseated at a position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Reseated {
    /// IT_CHARPOS.
    pub(crate) pos: usize,
    /// The iterator delivers a `display' string there (GET_FROM_STRING
    /// with string_from_display_prop_p, over buffer text).
    pub(crate) string: Option<DisplayString>,
}

/// What handle_display_spec makes of a `display' value on a terminal.
enum DisplaySpec {
    /// Not a replacing spec: the text displays.
    Text,
    /// An empty string (popped at once), or a fringe bitmap on a
    /// terminal: the covered text disappears.
    Nothing,
    String(DisplayString),
    /// A stretch or an image.
    Other,
}

fn string_spec(value: &Value, text_area: bool) -> Option<DisplaySpec> {
    let text = super::string_text(value).ok()?;
    Some(if text.is_empty() {
        DisplaySpec::Nothing
    } else {
        DisplaySpec::String(DisplayString {
            text_area,
            newlines: text.matches('\n').count(),
        })
    })
}

/// handle_single_display_spec: Some for a spec that replaces the text.
fn single_display_spec(spec: &Value) -> Option<DisplaySpec> {
    if let Some(string) = string_spec(spec, true) {
        return Some(string);
    }
    let (head, tail) = spec.cons_values()?;
    if let Some((margin, _)) = head.cons_values()
        && margin.symbol_str() == Some("margin")
    {
        let value = tail.cons_values().map_or(Value::Nil, |(value, _)| value);
        return Some(string_spec(&value, false).unwrap_or(DisplaySpec::Other));
    }
    match head.symbol_str()? {
        "image" | "space" => Some(DisplaySpec::Other),
        // A terminal has no fringes: POSITION has been advanced across
        // the text with this property.
        "left-fringe" | "right-fringe" if tail.cons_values().is_some() => {
            Some(DisplaySpec::Nothing)
        }
        _ => None,
    }
}

/// handle_display_spec: a list or vector of specs, or one spec; the
/// first that replaces the text decides.
fn display_spec(value: Value) -> DisplaySpec {
    let mut spec = value;
    if let Some((head, tail)) = spec.cons_values()
        && head.symbol_str() == Some("disable-eval")
    {
        spec = tail.cons_values().map_or(Value::Nil, |(inner, _)| inner);
    }
    if spec.is_nil() {
        return DisplaySpec::Text;
    }
    let list_of_specs = spec.cons_values().is_some_and(|(head, _)| {
        !head.is_nil()
            && !matches!(
                head.symbol_str(),
                Some(
                    "image" | "slice" | "space" | "when" | "raise" | "left-fringe" | "right-fringe"
                )
            )
            && !head
                .cons_values()
                .is_some_and(|(margin, _)| margin.symbol_str() == Some("margin"))
    });
    let specs = if list_of_specs {
        let mut items = Vec::new();
        let mut tail = spec;
        while let Some((item, next)) = tail.cons_values() {
            items.push(item);
            tail = next;
        }
        items
    } else if matches!(spec.kind(), Kind::Vector(_)) {
        super::vector_items(&spec).unwrap_or_default()
    } else {
        vec![spec]
    };
    specs
        .iter()
        .find_map(single_display_spec)
        .unwrap_or(DisplaySpec::Text)
}

/// get_char_property_and_overlay (POS, Qdisplay, BUFFER): the
/// highest-priority overlay's `display', else the text property's.
fn display_value_at(buffer: &Buffer, pos: usize) -> Option<Value> {
    let name = Value::Symbol("display".into());
    let mut best: Option<((i64, usize), Value)> = None;
    for overlay in &buffer.overlays {
        if overlay.is_dead() || !(overlay.beg() <= pos && pos < overlay.end()) {
            continue;
        }
        let Some(value) = overlay.get_prop(&name).filter(|value| !value.is_nil()) else {
            continue;
        };
        let priority = overlay
            .get_prop(&Value::Symbol("priority".into()))
            .and_then(|priority| priority.integer_value())
            .unwrap_or(0);
        let key = (priority, overlay.identity());
        if best.as_ref().is_none_or(|(existing, _)| key >= *existing) {
            best = Some((key, value));
        }
    }
    best.map(|(_, value)| value)
        .or_else(|| buffer.text_property_at(pos, "display"))
}

/// display_prop_end: Fnext_single_char_property_change (POS, Qdisplay).
fn display_prop_end(buffer: &Buffer, pos: usize) -> usize {
    let zv = buffer.point_max();
    let initial = display_value_at(buffer, pos).map(|value| value.word());
    let mut at = pos;
    loop {
        let (_, property_end) = buffer.text_property_interval_around(at);
        let next = property_end
            .min(super::next_overlay_change_position(buffer, at))
            .max(at + 1);
        if next >= zv || display_value_at(buffer, next).map(|value| value.word()) != initial {
            return next.min(zv);
        }
        at = next;
    }
}

/// reseat with FORCE_P: handle_stop at POS.  A `display' spec replacing
/// the text is handled first: an empty string is popped at once, leaving
/// the iterator past the text it covers, while any other stops there.
/// Invisible text is skipped (handle_invisible_prop), and the properties
/// after it handled anew unless an ellipsis shows.
pub(crate) fn reseat(buffer: &Buffer, spec: &InvisibilitySpec, pos: usize) -> Reseated {
    let zv = buffer.point_max();
    let mut pos = pos.clamp(buffer.point_min(), zv);
    loop {
        if pos >= zv {
            return Reseated { pos, string: None };
        }
        match display_value_at(buffer, pos).map_or(DisplaySpec::Text, display_spec) {
            DisplaySpec::Nothing => {
                return Reseated {
                    pos: display_prop_end(buffer, pos),
                    string: None,
                };
            }
            DisplaySpec::String(string) => {
                return Reseated {
                    pos,
                    string: Some(string),
                };
            }
            DisplaySpec::Other => return Reseated { pos, string: None },
            DisplaySpec::Text => {}
        }
        match invisible_run_at(buffer, spec, pos) {
            Some((end, ellipsis)) => {
                pos = end;
                if ellipsis {
                    return Reseated { pos, string: None };
                }
            }
            None => return Reseated { pos, string: None },
        }
    }
}

/// `reseat' in the current buffer.
pub(crate) fn reseat_current(interp: &Interpreter, pos: usize) -> Reseated {
    let buffer = interp.buffer.borrow();
    let spec = resolve_buffer_invisibility(interp, &buffer, interp.current_buffer_id());
    reseat(&buffer, &spec, pos)
}

/// Where move_it_to (MOVE_TO_POS) from FROM stops for TARGET: at FROM
/// when that is already past it; at TARGET when a screen line begins
/// there in the buffer (move_it_to checks each line it starts before
/// handle_stop runs there); else at the first position at or after
/// TARGET that the iterator delivers from the buffer -- after any
/// `display' replacement covering TARGET or starting there, and after
/// invisible text (BUFFER_POS_REACHED_P wants GET_FROM_BUFFER).
pub(crate) fn pos_reached(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    target: usize,
) -> usize {
    use super::dispatch::{screen_row_starts, visual_line_bounds};
    if from >= target {
        return from;
    }
    let (bol, eol) = visual_line_bounds(interp, target);
    if bol == target {
        return target;
    }
    let starts = screen_row_starts(interp, env, window_id, bol, eol);
    if starts.contains(&target) {
        return target;
    }
    let buffer = interp.buffer.borrow();
    let spec = resolve_buffer_invisibility(interp, &buffer, interp.current_buffer_id());
    let zv = buffer.point_max();
    let mut pos = target.min(zv);
    while pos < zv {
        if !matches!(
            display_value_at(&buffer, pos).map_or(DisplaySpec::Text, display_spec),
            DisplaySpec::Text
        ) {
            pos = display_prop_end(&buffer, pos);
            continue;
        }
        match invisible_run_at(&buffer, &spec, pos) {
            Some((end, _)) => pos = end,
            None => break,
        }
    }
    pos
}

/// move_it_vertically_backward (IT, 0): the start of FROM's screen line
/// -- for a line that begins inside a display string begun a line above,
/// the buffer text after the string, where move_it_by_lines leaves an
/// iterator it moved there.
fn screen_line_start(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
) -> usize {
    use super::dispatch::{row_index_of, screen_row_starts, visual_line_bounds};
    let (bol, eol) = visual_line_bounds(interp, from);
    let starts = screen_row_starts(interp, env, window_id, bol, eol);
    let index = row_index_of(&starts, from);
    let start = starts[index];
    if index > 0 && starts[index - 1] == start && reseat_current(interp, start).string.is_some() {
        return pos_reached(interp, env, window_id, start, start + 1);
    }
    start
}

/// back_to_previous_visible_line_start: the start of the line holding
/// POS - 1, earlier while the newline before it is invisible.
pub(crate) fn back_to_previous_visible_line_start(interp: &Interpreter, pos: usize) -> usize {
    let begv = interp.buffer.borrow().point_min();
    if pos <= begv {
        return begv;
    }
    super::dispatch::visual_line_bounds(interp, pos - 1).0
}

/// it2.vpos after move_it_to (&it2, TO, MOVE_TO_POS) from FROM: the
/// screen lines from FROM's down to the one where the iterator reaches
/// TO (past a display string starting there).
pub(crate) fn rows_between(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    to: usize,
) -> i64 {
    use super::dispatch::{row_index_of, screen_row_starts, visual_line_bounds};
    if to <= from {
        return 0;
    }
    let to = pos_reached(interp, env, window_id, from, to);
    let zv = interp.buffer.borrow().point_max();
    let (mut bol, mut eol) = visual_line_bounds(interp, from);
    let mut starts = screen_row_starts(interp, env, window_id, bol, eol);
    let mut index = row_index_of(&starts, from);
    let mut rows = 0;
    loop {
        if index + 1 < starts.len() {
            index += 1;
        } else if eol < zv {
            (bol, eol) = visual_line_bounds(interp, eol + 1);
            starts = screen_row_starts(interp, env, window_id, bol, eol);
            index = 0;
        } else {
            break;
        }
        if starts[index] > to {
            break;
        }
        rows += 1;
        if starts[index] == to {
            break;
        }
    }
    rows
}

/// move_it_to (MOVE_TO_VPOS) COUNT screen lines down from FROM: the
/// landing line's start, the lines moved, and whether the line begins
/// inside a string that began a line above (its start repeats the one
/// before).
fn forward_rows(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    count: i64,
) -> (usize, i64, bool) {
    use super::dispatch::{row_index_of, screen_row_starts, visual_line_bounds};
    let zv = interp.buffer.borrow().point_max();
    let (mut bol, mut eol) = visual_line_bounds(interp, from);
    let mut starts = screen_row_starts(interp, env, window_id, bol, eol);
    let mut index = row_index_of(&starts, from);
    let mut moved = 0;
    while moved < count {
        if index + 1 < starts.len() {
            index += 1;
        } else if eol < zv {
            (bol, eol) = visual_line_bounds(interp, eol + 1);
            starts = screen_row_starts(interp, env, window_id, bol, eol);
            index = 0;
        } else {
            break;
        }
        moved += 1;
    }
    let in_string = index > 0 && starts[index] == starts[index - 1];
    (starts[index], moved, in_string)
}

/// move_it_by_lines from an iterator at FROM on vpos 0: where it lands
/// and its vpos there.  DVPOS 0 is the start of FROM's screen line
/// (move_it_vertically_backward, no reseat); forward, the start of the
/// line DVPOS down.  Backward, -DVPOS buffer lines back (no farther than
/// -DVPOS full screen lines), reseated -- skipping what handle_stop skips
/// -- then forward again if continuation lines took it too far.
pub(crate) fn move_it_by_lines(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    dvpos: i64,
) -> (usize, i64) {
    if dvpos == 0 {
        return (screen_line_start(interp, env, window_id, from), 0);
    }
    if dvpos > 0 {
        let (pos, moved, in_string) = forward_rows(interp, env, window_id, from, dvpos);
        if moved < dvpos {
            // move_it_to runs out at ZV.
            return (interp.buffer.borrow().point_max(), moved);
        }
        // !IT_POS_VALID_AFTER_MOVE_P: inside a display string, on to the
        // buffer text after it (an overlay string keeps its position).
        if in_string && reseat_current(interp, pos).string.is_some() {
            return (pos_reached(interp, env, window_id, pos, pos + 1), moved);
        }
        return (pos, moved);
    }
    let begv = interp.buffer.borrow().point_min();
    let orig_charpos = from;
    let start_charpos = screen_line_start(interp, env, window_id, from);
    let truncate = crate::tty::window_truncates(interp, window_id, interp.current_buffer_id());
    let nchars_per_row = super::window_text_area_columns(interp, window_id) as i64;
    let pos_limit = if truncate || nchars_per_row == 0 {
        begv
    } else {
        (start_charpos as i64)
            .saturating_add(dvpos.saturating_mul(nchars_per_row))
            .max(begv as i64) as usize
    };
    let mut it = start_charpos;
    let mut i = -dvpos;
    while i > 0 && it > pos_limit {
        it = back_to_previous_visible_line_start(interp, it);
        i -= 1;
    }
    let hit_pos_limit = i > 0 && it <= pos_limit;
    let it = reseat_current(interp, it).pos;
    // Above may have moved too far if continuation lines are involved.
    let it2_vpos = rows_between(interp, env, window_id, it, start_charpos);
    let vpos = -it2_vpos;
    if it2_vpos > -dvpos {
        let delta = it2_vpos + dvpos;
        let (forward, moved, _) = forward_rows(interp, env, window_id, it, delta);
        // Back again if it did not move at all.
        if forward == orig_charpos {
            (it, vpos)
        } else {
            (forward, vpos + moved)
        }
    } else if hit_pos_limit && pos_limit > begv && it2_vpos < -dvpos {
        // A display string with a newline covering a large chunk of
        // text: back until DVPOS is reached, the limit lifted.
        let mut it = it;
        let mut vpos = vpos;
        for _ in 0..(-(dvpos + it2_vpos)) {
            it = back_to_previous_visible_line_start(interp, it);
            vpos -= 1;
        }
        (it, vpos)
    } else {
        (it, vpos)
    }
}

/// indent.c Fvertical_motion on a live display, from point: where the
/// iterator lands LINES screen lines away and the screen lines moved.
pub(crate) fn vertical_motion(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    lines: i64,
) -> (usize, i64) {
    let (begv, zv, pt) = {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max(), buffer.point())
    };
    let char_at = |interp: &Interpreter, pos: usize| interp.buffer.borrow().char_at(pos);
    // start_display: init_iterator reseats at PT.
    let started = reseat_current(interp, pt);
    let it_start = started.pos;
    let disp_string_at_start_p = started.string.is_some_and(|string| string.text_area);
    let mut it_overshoot_count: i64 = match started.string {
        Some(string) if string.newlines > 0 => string.newlines as i64,
        Some(_) => -1,
        None => 1,
    };
    // Scan from the start of the line containing PT.
    let line_start = reseat_current(
        interp,
        back_to_previous_visible_line_start(interp, it_start),
    )
    .pos;
    let mut it = line_start;
    if it != pt {
        // When PT is covered by a display string, move_it_to would
        // overshoot it: stop before the string unless a newline is
        // there.
        let target = if !disp_string_at_start_p || char_at(interp, it) == Some('\n') {
            pt
        } else {
            pt - 1
        };
        it = pos_reached(interp, env, window_id, it, target);
    }
    let mut nlines = lines;
    let mut vpos_init = 0i64;
    let mut overshoot_handled = false;
    if it > it_start {
        // IT overshot: back up to PT's screen line.
        let it_vpos = rows_between(interp, env, window_id, line_start, it);
        if it_overshoot_count < 0 && char_at(interp, it) == Some('\n') {
            it_overshoot_count = 1;
        } else if it_overshoot_count == 1 && it_vpos == 0 {
            it_overshoot_count = 0;
        } else if disp_string_at_start_p && it_vpos > 0 {
            it_overshoot_count = it_vpos;
        }
        if it_overshoot_count > 0 {
            it = move_it_by_lines(interp, env, window_id, it, -it_overshoot_count).0;
        }
        overshoot_handled = true;
    } else if pt > begv && it == pt - 1 && char_at(interp, pt - 1) == Some('\n') && nlines <= 0 {
        // Moved before the display string at PT and up a line: one line
        // less to go up, still counted in the value.
        nlines += 1;
        vpos_init = -1;
        overshoot_handled = true;
    }
    if nlines <= 0 {
        let mut vpos = vpos_init;
        if (nlines < 0 && it > begv) || nlines == 0 {
            let (landed, moved) = move_it_by_lines(interp, env, window_id, it, nlines);
            it = landed;
            vpos += moved;
        }
        return (it, vpos);
    }
    if overshoot_handled {
        let (landed, moved) = move_it_by_lines(interp, env, window_id, it, nlines);
        return (landed, vpos_init + moved);
    }
    if it_start < zv {
        // From the last screen line PT occupies.
        let mut vpos = 0;
        if it < it_start {
            (it, vpos) = move_it_by_lines(interp, env, window_id, it, 1);
        }
        while it == it_start {
            let (landed, moved) = move_it_by_lines(interp, env, window_id, it, 1);
            vpos = moved;
            if landed == it {
                break;
            }
            it = landed;
        }
        if nlines > 1 {
            let (landed, moved) = move_it_by_lines(interp, env, window_id, it, nlines - 1);
            it = landed;
            vpos += moved;
        }
        return (it, vpos);
    }
    let (landed, mut vpos) = move_it_by_lines(interp, env, window_id, it, nlines);
    if landed == pt && pt == it_start {
        vpos = 0;
    }
    (landed, vpos)
}
