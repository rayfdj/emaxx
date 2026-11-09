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
use crate::tty::{IteratorFetches, IteratorWalk};

/// Record move_it_to's walk from FROM to TO (MOVE_TO_POS) in WINDOW.
fn fetch_walk_to(
    interp: &Interpreter,
    window_id: u64,
    fetches: &mut IteratorFetches,
    from: usize,
    to: usize,
) {
    if let Some(geometry) = fetches.geometry() {
        IteratorWalk::at(interp, window_id, geometry, from).walk_to(fetches, from, to);
    }
}

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
        && margin.is_key(cached_symbol!("margin"))
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
        && head.is_key(cached_symbol!("disable-eval"))
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
                .is_some_and(|(margin, _)| margin.is_key(cached_symbol!("margin")))
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
/// invisible text (BUFFER_POS_REACHED_P wants GET_FROM_BUFFER).  What
/// the walk fetches is recorded in FETCHES.
pub(crate) fn pos_reached(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    target: usize,
    fetches: &mut IteratorFetches,
) -> usize {
    let reached = pos_reached_by(interp, env, window_id, from, target);
    if reached > from {
        fetch_walk_to(interp, window_id, fetches, from, reached);
    }
    reached
}

fn pos_reached_by(
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
    text_reached(interp, target)
}

/// move_it_vertically_backward (IT, 0): where the iterator at FROM goes
/// back to, and the row of FROM's display line it is on there.  It walks
/// from the line start back_to_previous_visible_line_start reached to
/// FROM and moves that many rows down from it: for FROM inside its line,
/// or at BEGV, where that is FROM's own line, the walk loads the strings
/// at FROM and goes through them, and through a `display' string
/// covering FROM, to FROM's text, so the row is the one showing that
/// text; for FROM at a later line's start the walk comes from the line
/// before and stops at FROM before handle_stop loads the strings there
/// (move_it_to's check at each line it begins), on FROM's first row.  A
/// row beginning inside a `display' string (!IT_POS_VALID_AFTER_MOVE_P)
/// leaves the iterator past it, at the text after it (move_it_to
/// IT_CHARPOS + 1); inside an overlay string, which conceals no buffer
/// position, at the string's position on that row.
fn screen_line_start_row(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    fetches: &mut IteratorFetches,
) -> (usize, usize) {
    use super::dispatch::{
        interactive_screen_line, row_index_of, screen_row_starts, visual_line_bounds,
    };
    crate::tty::fetch_screen_line_start(interp, window_id, fetches, from);
    let (bol, eol) = visual_line_bounds(interp, from);
    let starts = screen_row_starts(interp, env, window_id, bol, eol);
    if let Some(index) = zv_strings_row(interp, env, window_id, bol, from)
        && let Some(&start) = starts.get(index)
    {
        return (start, index);
    }
    let begv = interp.buffer.borrow().point_min();
    let index = if from > bol || from <= begv {
        let text = text_reached(interp, from);
        starts
            .iter()
            .rposition(|&row_start| row_start <= text)
            .unwrap_or(0)
    } else {
        row_index_of(&starts, from)
    };
    let start = starts[index];
    // Whether the row begins inside a `display' string; without the
    // display iterator, a second row beginning where the string does.
    let inside_display = index > 0
        && if super::dispatch::interactive_motion(interp, env) {
            interactive_screen_line(interp, env, window_id, bol)
                .row_first_string(index)
                .is_some_and(|(display, character)| display && character > 0)
        } else {
            starts[index - 1] == start && reseat_current(interp, start).string.is_some()
        };
    if !inside_display {
        return (start, index);
    }
    let pos = pos_reached(interp, env, window_id, start, start + 1, fetches);
    let row = starts
        .iter()
        .rposition(|&row_start| row_start <= pos)
        .unwrap_or(index);
    (pos, row)
}

/// Where an iterator going to FROM through the buffer from before it
/// reaches FROM's own text (BUFFER_POS_REACHED_P wants GET_FROM_BUFFER):
/// past a `display' replacement covering FROM, and past invisible text.
fn text_reached(interp: &Interpreter, from: usize) -> usize {
    let buffer = interp.buffer.borrow();
    let spec = resolve_buffer_invisibility(interp, &buffer, interp.current_buffer_id());
    let zv = buffer.point_max();
    let mut pos = from.min(zv);
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

/// back_to_previous_visible_line_start: the start of the line holding
/// POS - 1, earlier while the newline before it is invisible.
pub(crate) fn back_to_previous_visible_line_start(interp: &Interpreter, pos: usize) -> usize {
    let begv = interp.buffer.borrow().point_min();
    if pos <= begv {
        return begv;
    }
    super::dispatch::visual_line_bounds(interp, pos - 1).0
}

/// At ZV part-way along the display line beginning at BOL, under strings
/// at ZV that go on to rows of their own: the row before the first row
/// beginning at ZV, where an iterator moved to ZV stays, before the
/// strings (GNU's at ZV) -- whether that row begins inside a string or
/// at a string's first character.
fn zv_strings_row(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    bol: usize,
    pos: usize,
) -> Option<usize> {
    if pos != interp.buffer.borrow().point_max()
        || pos <= bol
        || !super::dispatch::interactive_motion(interp, env)
    {
        return None;
    }
    let layout = super::dispatch::interactive_screen_line(interp, env, window_id, bol);
    let first = layout.row_starts.iter().position(|&start| start == pos)?;
    (first > 0 && layout.row_first_string(first).is_some()).then(|| first - 1)
}

/// it2.vpos after move_it_to (&it2, TO, MOVE_TO_POS) from FROM: the
/// screen lines from FROM's down to the one where the iterator reaches
/// TO in the buffer's text -- past a display string starting there, and
/// past the rows of overlay strings at TO, which begin at TO too, unless
/// TO begins a line, where move_it_to checks each line it starts before
/// handle_stop loads the strings there, or is ZV with strings there
/// (zv_strings_row).
pub(crate) fn rows_between(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    to: usize,
    fetches: &mut IteratorFetches,
) -> i64 {
    use super::dispatch::{row_index_of, screen_row_starts, visual_line_bounds};
    if to <= from {
        return 0;
    }
    let to = pos_reached(interp, env, window_id, from, to, fetches);
    let zv = interp.buffer.borrow().point_max();
    let zv_row = zv_strings_row(interp, env, window_id, visual_line_bounds(interp, to).0, to);
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
        if starts[index] > to || (starts[index] == to && zv_row.is_some()) {
            break;
        }
        rows += 1;
        if starts[index] == to && (index == 0 || starts.get(index + 1) != Some(&to)) {
            break;
        }
    }
    rows
}

/// Where move_it_to (MOVE_TO_VPOS) COUNT screen lines down from FROM
/// lands.
struct RowsDown {
    /// The landing line's start.
    start: usize,
    /// The lines moved.
    moved: i64,
    /// The line begins inside a string, past its first character.
    in_string: bool,
    /// The landing line among the rows of its display line, and those
    /// rows' starts.
    index: usize,
    starts: Vec<usize>,
}

/// move_it_to (MOVE_TO_VPOS) COUNT screen lines down from FROM, on the
/// row FROM_ROW of FROM's display line when the iterator is known to be
/// there (a string's rows share their position), else the first row
/// showing FROM.
fn forward_rows(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    from_row: Option<usize>,
    count: i64,
    fetches: &mut IteratorFetches,
) -> RowsDown {
    use super::dispatch::{
        interactive_motion, interactive_screen_line, row_index_of, visual_line_bounds,
        visual_segment_starts,
    };
    if let Some(geometry) = fetches.geometry()
        && count > 0
    {
        IteratorWalk::at(interp, window_id, geometry, from).walk_rows(
            fetches,
            from,
            count as usize,
        );
    }
    let zv = interp.buffer.borrow().point_max();
    // screen_row_starts, keeping a terminal's layout of the line, whose
    // glyphs say what each row begins with.
    let interactive = interactive_motion(interp, env);
    let lay_out = |interp: &mut Interpreter, env: &mut Env, bol: usize, eol: usize| {
        if interactive {
            let layout = interactive_screen_line(interp, env, window_id, bol);
            (layout.row_starts.clone(), Some(layout))
        } else {
            (visual_segment_starts(interp, env, bol, eol), None)
        }
    };
    let (mut bol, mut eol) = visual_line_bounds(interp, from);
    let (mut starts, mut layout) = lay_out(interp, env, bol, eol);
    let mut index = from_row
        .filter(|&row| starts.get(row).is_some_and(|&start| start <= from))
        .unwrap_or_else(|| row_index_of(&starts, from));
    let mut moved = 0;
    while moved < count {
        if index + 1 < starts.len() {
            index += 1;
        } else if eol < zv {
            (bol, eol) = visual_line_bounds(interp, eol + 1);
            (starts, layout) = lay_out(interp, env, bol, eol);
            index = 0;
        } else {
            break;
        }
        moved += 1;
    }
    RowsDown {
        start: starts[index],
        moved,
        in_string: layout
            .as_ref()
            .is_some_and(|layout| layout.row_begins_inside_string(index)),
        index,
        starts,
    }
}

/// (it->last_visible_x - it->first_visible_x) / FRAME_COLUMN_WIDTH, the
/// columns of WINDOW's text area less the one init_iterator leaves for
/// the continuation or truncation glyph.
pub(crate) fn iterator_row_chars(interp: &Interpreter, window_id: u64) -> usize {
    super::window_text_area_columns(interp, window_id).saturating_sub(1)
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
    fetches: &mut IteratorFetches,
) -> (usize, i64) {
    let landing = move_it_by_lines_from(interp, env, window_id, from, None, dvpos, fetches);
    (landing.pos, landing.moved)
}

/// move_it_by_lines back, DVPOS negative, from FROM: the row reached is
/// known when continuation lines took the walk back too far and IT went
/// forward again.
fn move_it_by_lines_back(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    dvpos: i64,
    fetches: &mut IteratorFetches,
) -> Landing {
    let unrowed = |pos: usize, moved: i64| Landing {
        pos,
        moved,
        row: None,
    };
    let begv = interp.buffer.borrow().point_min();
    let orig_charpos = from;
    let (start_charpos, start_row) = screen_line_start_row(interp, env, window_id, from, fetches);
    let truncate = crate::tty::window_truncates(interp, window_id, interp.current_buffer_id());
    let nchars_per_row = iterator_row_chars(interp, window_id) as i64;
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
    // reseat (IT, ..., true): it2, a copy, walks from the line start.
    fetches.begin_walk();
    fetches.push(it, it + 1);
    let it = reseat_current(interp, it).pos;
    // Above may have moved too far if continuation lines are involved.
    let it2_vpos = rows_between(interp, env, window_id, it, start_charpos, fetches);
    let vpos = -it2_vpos;
    if it2_vpos > -dvpos {
        let delta = it2_vpos + dvpos;
        // IT itself, which stops again at the line start.
        fetches.begin_walk();
        let down = forward_rows(interp, env, window_id, it, None, delta, fetches);
        let (forward, moved) = (down.start, down.moved);
        // Back again if it did not move at all.
        if forward == orig_charpos {
            unrowed(it, vpos)
        } else {
            Landing {
                pos: forward,
                moved: vpos + moved,
                row: Some(down.index),
            }
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
        unrowed(it, vpos)
    } else if it == start_charpos {
        // No line start to go back to (START_CHARPOS is BEGV).  The
        // overlay strings there, ended at the row move_it_by_lines
        // reached, set ignore_overlay_strings_at_pos_p, and reseat at
        // the same position loads none: the iterator stays on its row.
        // When that row began inside a string, the move on to the text
        // after it fetched a buffer element, which cleared the flag:
        // reseat loads the strings again, and it2, walking from them to
        // the text, comes back as far as -DVPOS allows.
        let reloaded = start_row > 0
            && super::dispatch::interactive_motion(interp, env)
            && super::dispatch::interactive_screen_line(interp, env, window_id, it)
                .row_first_string(start_row)
                .is_some_and(|(display, character)| !display && character > 0);
        if !reloaded {
            return Landing {
                pos: it,
                moved: vpos,
                row: Some(start_row),
            };
        }
        let it2_vpos = start_row as i64;
        if it2_vpos > -dvpos {
            fetches.begin_walk();
            let down = forward_rows(
                interp,
                env,
                window_id,
                it,
                Some(0),
                it2_vpos + dvpos,
                fetches,
            );
            if down.start == orig_charpos {
                Landing {
                    pos: it,
                    moved: -it2_vpos,
                    row: Some(0),
                }
            } else {
                Landing {
                    pos: down.start,
                    moved: down.moved - it2_vpos,
                    row: Some(down.index),
                }
            }
        } else {
            Landing {
                pos: it,
                moved: -it2_vpos,
                row: Some(0),
            }
        }
    } else {
        unrowed(it, vpos)
    }
}

/// Where move_it_by_lines leaves the iterator: its position, the lines
/// moved and, after a move down, the row it is on among its display
/// line's rows, which a further move goes on from.
pub(crate) struct Landing {
    pub(crate) pos: usize,
    pub(crate) moved: i64,
    pub(crate) row: Option<usize>,
}

/// move_it_by_lines from an iterator at FROM on row FROM_ROW of FROM's
/// display line (see forward_rows).
pub(crate) fn move_it_by_lines_from(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    from_row: Option<usize>,
    dvpos: i64,
    fetches: &mut IteratorFetches,
) -> Landing {
    if dvpos == 0 {
        let (pos, row) = screen_line_start_row(interp, env, window_id, from, fetches);
        return Landing {
            pos,
            moved: 0,
            row: Some(row),
        };
    }
    if dvpos < 0 {
        return move_it_by_lines_back(interp, env, window_id, from, dvpos, fetches);
    }
    let down = forward_rows(interp, env, window_id, from, from_row, dvpos, fetches);
    let (pos, moved) = (down.start, down.moved);
    if moved < dvpos {
        // move_it_to runs out at ZV.
        return Landing {
            pos: interp.buffer.borrow().point_max(),
            moved,
            row: Some(down.index),
        };
    }
    // !IT_POS_VALID_AFTER_MOVE_P: inside a string, move_it_to its
    // position -- one past it for a display string, whose glyphs
    // conceal the text -- goes on to the buffer text after it, the
    // rows it passes counted in its vpos.
    if down.in_string {
        let reached = if reseat_current(interp, pos).string.is_some() {
            pos_reached(interp, env, window_id, pos, pos + 1, fetches)
        } else {
            pos
        };
        // A `display' string replacing the line's newline: the text after
        // it begins the next line's first row, past this line's rows.
        if reached > super::dispatch::visual_line_bounds(interp, pos).1
            && down.starts.iter().all(|&start| start < reached)
        {
            return Landing {
                pos: reached,
                moved: moved + (down.starts.len() - down.index) as i64,
                row: Some(0),
            };
        }
        let last = down
            .starts
            .iter()
            .rposition(|&start| start <= reached)
            .unwrap_or(down.index)
            .max(down.index);
        return Landing {
            pos: reached,
            moved: moved + (last - down.index) as i64,
            row: Some(last),
        };
    }
    Landing {
        pos,
        moved,
        row: Some(down.index),
    }
}

/// indent.c Fvertical_motion on a live display, from point: where the
/// iterator lands LINES screen lines away, the screen lines moved and,
/// after a move down, the row it is on, what its walks fetch recorded in
/// FETCHES; and the lines asked as Fvertical_motion leaves its NLINES
/// (one fewer up after a move to before a display string at PT).
pub(crate) fn vertical_motion(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    lines: i64,
    fetches: &mut IteratorFetches,
) -> (Landing, i64) {
    let unrowed = |pos: usize, moved: i64| Landing {
        pos,
        moved,
        row: None,
    };
    let (begv, zv, pt) = {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max(), buffer.point())
    };
    let char_at = |interp: &Interpreter, pos: usize| interp.buffer.borrow().char_at(pos);
    // start_display: init_iterator reseats at PT.
    crate::tty::fetch_start_display(interp, window_id, fetches, pt);
    let started = reseat_current(interp, pt);
    let it_start = started.pos;
    let disp_string_at_start_p = started.string.is_some_and(|string| string.text_area);
    let mut it_overshoot_count: i64 = match started.string {
        Some(string) if string.newlines > 0 => string.newlines as i64,
        Some(_) => -1,
        None => 1,
    };
    // Scan from the start of the line containing PT
    // (reseat_at_previous_visible_line_start).
    let previous_line_start = back_to_previous_visible_line_start(interp, it_start);
    fetches.begin_walk();
    fetches.push(previous_line_start, previous_line_start + 1);
    let line_start = reseat_current(interp, previous_line_start).pos;
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
        it = pos_reached(interp, env, window_id, it, target, fetches);
    }
    let mut nlines = lines;
    let mut vpos_init = 0i64;
    let mut overshoot_handled = false;
    if it > it_start {
        // IT overshot: back up to PT's screen line.
        let it_vpos = rows_between(interp, env, window_id, line_start, it, fetches);
        if it_overshoot_count < 0 && char_at(interp, it) == Some('\n') {
            it_overshoot_count = 1;
        } else if it_overshoot_count == 1 && it_vpos == 0 {
            it_overshoot_count = 0;
        } else if disp_string_at_start_p && it_vpos > 0 {
            it_overshoot_count = it_vpos;
        }
        if it_overshoot_count > 0 {
            it = move_it_by_lines(interp, env, window_id, it, -it_overshoot_count, fetches).0;
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
        let mut landing = unrowed(it, vpos_init);
        if (nlines < 0 && it > begv) || nlines == 0 {
            let moved = move_it_by_lines_from(interp, env, window_id, it, None, nlines, fetches);
            landing = Landing {
                moved: vpos_init + moved.moved,
                ..moved
            };
        }
        return (landing, nlines);
    }
    if overshoot_handled {
        let landing = move_it_by_lines_from(interp, env, window_id, it, None, nlines, fetches);
        return (
            Landing {
                moved: vpos_init + landing.moved,
                ..landing
            },
            nlines,
        );
    }
    if it_start < zv {
        // From the last screen line PT occupies.
        let mut vpos = 0;
        let mut row = None;
        if it < it_start {
            let landing = move_it_by_lines_from(interp, env, window_id, it, None, 1, fetches);
            (it, vpos, row) = (landing.pos, landing.moved, landing.row);
        }
        // A line at a time, it.vpos 0 before each, while the iterator
        // is at IT_START: the rows of a string there all begin at it, so
        // each move goes on from the row the last one reached, and the
        // value is the last move's (a move out of a string can pass
        // more than one row).
        while it == it_start {
            let landing = move_it_by_lines_from(interp, env, window_id, it, row, 1, fetches);
            vpos = landing.moved;
            let stuck = landing.moved == 0 || (landing.pos == it && landing.row == row);
            (it, row) = (landing.pos, landing.row);
            if stuck {
                break;
            }
        }
        if nlines > 1 {
            let landing =
                move_it_by_lines_from(interp, env, window_id, it, row, nlines - 1, fetches);
            it = landing.pos;
            row = landing.row;
            vpos += landing.moved;
        }
        return (
            Landing {
                pos: it,
                moved: vpos,
                row,
            },
            nlines,
        );
    }
    let mut landing = move_it_by_lines_from(interp, env, window_id, it, None, nlines, fetches);
    if landing.pos == pt && pt == it_start {
        landing.moved = 0;
    }
    (landing, nlines)
}
