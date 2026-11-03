//! indent.c compute_motion and vmotion: where a scan of the current
//! buffer arrives, and at what screen position, as a window lays the text
//! out in columns -- Fcompute_motion, and Fvertical_motion when Emacs runs
//! noninteractively (window.c's line-based scrolling goes through it).

use super::dispatch::CompositionIterator;
use super::{buffer_display_table, multibyte_char_width, position_indentation, vector_items};
use crate::lisp::eval::Interpreter;
use crate::lisp::types::{CharTableRef, Env, Kind, PropertyName, Value};

/// indent.c struct position: the buffer position a scan stopped at, its
/// screen column and line, the column of the character before it, and
/// whether the line it is on continues the previous one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) bufpos: usize,
    pub(crate) hpos: i64,
    pub(crate) vpos: i64,
    pub(crate) prevhpos: i64,
    pub(crate) contin: bool,
}

/// The window a motion is laid out in: ID lends its display table and
/// frame, and CONTENTS is the buffer it shows meanwhile (Fvertical_motion
/// shows the current buffer in it while it moves).
#[derive(Clone, Copy, Debug)]
pub(crate) struct MotionWindow {
    pub(crate) id: u64,
    pub(crate) contents: u64,
}

/// dispextern.h GLYPH_CODE_P and GLYPH_CODE_CHAR: the character of a
/// display vector's ENTRY, a space for an entry that is no glyph code.
fn glyph_code_char(entry: &Value) -> i64 {
    // MAX_FACE_ID is (1 << FACE_ID_BITS) - 1, and a character takes
    // CHARACTERBITS (22) bits under it.
    const MAX_FACE_ID: i64 = (1 << 20) - 1;
    const MAX_CHAR: i64 = 0x3F_FFFF;
    match entry.kind() {
        Kind::Integer(code) if (0..=((MAX_FACE_ID << 22) | MAX_CHAR)).contains(&code) => {
            code & MAX_CHAR
        }
        Kind::Cons(cell) => match (cell.car.get().kind(), cell.cdr.get().kind()) {
            (Kind::Integer(c), Kind::Integer(face))
                if (0..=MAX_CHAR).contains(&c) && (0..=MAX_FACE_ID).contains(&face) =>
            {
                c
            }
            _ => i64::from(b' '),
        },
        _ => i64::from(b' '),
    }
}

/// DISP_TABLE_P: a char-table of purpose `display-table' with its six
/// extra slots.
fn display_table_of(value: Value) -> Option<CharTableRef> {
    match value.kind() {
        Kind::CharTable(table)
            if table.has_purpose("display-table") && table.extra_count() == 6 =>
        {
            Some(table)
        }
        _ => None,
    }
}

/// window.c window_display_table: WIN's own display table, else its
/// buffer's `buffer-display-table', else `standard-display-table'.
fn window_display_table(
    interp: &Interpreter,
    env: &Env,
    win: MotionWindow,
) -> Option<CharTableRef> {
    let own = interp
        .find_record(win.id)
        .and_then(|window| window.slots.get(super::WINDOW_DISPLAY_TABLE_SLOT).copied())
        .and_then(display_table_of);
    if own.is_some() {
        return own;
    }
    if win.contents == interp.current_buffer_id() {
        return buffer_display_table(interp, env);
    }
    interp
        .buffer_local_toplevel_value(win.contents, "buffer-display-table")
        .or_else(|| interp.default_toplevel_value("buffer-display-table"))
        .and_then(display_table_of)
        .or_else(|| {
            interp
                .lookup_var_key(cached_symbol!("standard-display-table"), env)
                .and_then(display_table_of)
        })
}

/// DISP_CHAR_VECTOR of DP for C when it is a vector: its entries.
fn display_vector(interp: &Interpreter, dp: Option<CharTableRef>, c: i64) -> Option<Vec<Value>> {
    let entry = interp.char_table_get(dp?, u32::try_from(c).ok()?)?;
    if !super::interactive::is_vector_value(&entry) {
        return None;
    }
    vector_items(&entry).ok()
}

/// BVAR (current_buffer, selective_display) as compute_motion reads it:
/// a fixnum's value, -1 for another non-nil value, else 0.
fn selective_display(interp: &Interpreter, env: &Env) -> i64 {
    match interp.lookup_var_key(cached_symbol!("selective-display"), env) {
        Some(value) => match value.kind() {
            Kind::Integer(n) => n,
            Kind::Nil => 0,
            _ => -1,
        },
        None => 0,
    }
}

/// Fget_char_property (POS, PROP, OBJECT) where OBJECT is the current
/// buffer, or WINDOW showing it so that only the overlays for that window
/// count.
fn char_property(interp: &Interpreter, pos: usize, prop: &str, window: Option<u64>) -> Value {
    let name = PropertyName::symbol(prop);
    {
        let buffer = interp.buffer.borrow();
        if let Ok(Some((value, _))) = super::highest_priority_overlay_property_with_id(
            interp, &buffer, pos, name, false, window,
        ) {
            return value;
        }
    }
    super::textprop::get_text_property(interp, &Value::Integer(pos as i64), name, Value::Nil)
        .unwrap_or(Value::Nil)
}

/// The object compute_motion's skip_invisible and vmotion read text
/// properties through: WIN when it shows the current buffer, else the
/// buffer.
fn text_prop_window(interp: &Interpreter, win: MotionWindow) -> Option<u64> {
    (win.contents == interp.current_buffer_id()).then_some(win.id)
}

/// TEXT_PROP_MEANS_INVISIBLE of the `invisible' property at POS.
fn invisible_at(interp: &Interpreter, env: &Env, pos: usize, window: Option<u64>) -> u8 {
    let prop = char_property(interp, pos, "invisible", window);
    super::invisibility_level(interp, &prop, env)
}

/// indent.c skip_invisible, for a window: past the invisible text at POS,
/// where the `invisible' property next changes (within a hundred
/// characters, and not past the next overlay change) stored in
/// NEXT_BOUNDARY.
fn skip_invisible(
    interp: &Interpreter,
    env: &Env,
    pos: usize,
    next_boundary: &mut usize,
    to: usize,
    window: Option<u64>,
) -> usize {
    let position = Value::Integer(pos as i64);
    let overlay_limit = super::next_overlay_change_position(&interp.buffer.borrow(), pos);
    let mut proplimit =
        super::textprop::next_property_change(interp, &position, Value::Nil, Value::T)
            .ok()
            .and_then(|value| value.integer_value())
            .map_or(overlay_limit, |value| value as usize);
    if overlay_limit < proplimit {
        proplimit = overlay_limit;
    }
    if proplimit > pos + 100 || proplimit >= to {
        *next_boundary = proplimit;
    } else {
        let limit = (pos + 100).min(to).min(overlay_limit);
        *next_boundary = super::textprop::next_single_property_change(
            interp,
            &[
                position,
                Value::symbol("invisible"),
                Value::Nil,
                Value::Integer(limit as i64),
            ],
        )
        .ok()
        .and_then(|value| value.integer_value())
        .map_or(limit, |value| value as usize);
    }
    if invisible_at(interp, env, pos, window) != 0 {
        return *next_boundary;
    }
    pos
}

/// buffer.c overlay_strings' total for compute_motion: the columns of the
/// after-strings of the overlays ending at POS and the before-strings of
/// those starting there (an empty overlay's after-string too), counting
/// only the overlays for WIN.  In a multibyte buffer that is strwidth of
/// the strings as copy_text makes them multibyte; in a unibyte one, their
/// characters.
fn overlay_strings_width(interp: &Interpreter, env: &Env, pos: usize, win: MotionWindow) -> i64 {
    let mut strings = Vec::new();
    let multibyte = {
        let buffer = interp.buffer.borrow();
        if buffer.overlays.is_empty() {
            return 0;
        }
        for overlay in buffer.overlays.iter() {
            if overlay.is_dead() {
                continue;
            }
            let (start, end) = (overlay.beg(), overlay.end());
            if start != pos && end != pos {
                continue;
            }
            let get = |name: &str| {
                super::overlay_property_with_category(interp, &overlay, PropertyName::symbol(name))
                    .ok()
                    .flatten()
                    .unwrap_or(Value::Nil)
            };
            let window = get("window");
            if super::window_record_id_from_value(interp, &window).is_some_and(|id| id != win.id) {
                continue;
            }
            let before = get("before-string");
            if start == pos && before.is_string() {
                strings.push(before);
                if start == end {
                    let after = get("after-string");
                    if after.is_string() {
                        strings.push(after);
                    }
                }
            } else if end == pos {
                let after = get("after-string");
                if after.is_string() {
                    strings.push(after);
                }
            }
        }
        buffer.is_multibyte()
    };
    // c_string_width reads the buffer's display table.
    let dp = multibyte
        .then(|| buffer_display_table(interp, env))
        .flatten();
    let mut width = 0i64;
    for string in strings {
        let Kind::StringObject(state) = string.kind() else {
            continue;
        };
        let state = state.borrow();
        if !multibyte {
            width += state.len() as i64;
            continue;
        }
        // copy_text makes a unibyte string's bytes above ASCII raw bytes.
        let mut offset = 0;
        while let Some(code) = state.character_as_multibyte_advance(&mut offset) {
            width += match display_vector(interp, dp, code) {
                Some(glyphs) => glyphs.len().min(1000) as i64,
                None => super::character_width(interp, code as u32, env) as i64,
            };
        }
    }
    width
}

/// indent.c indented_beyond_p: whether the line at POS, or the last
/// nonempty line before an empty one there, is indented COLUMN columns
/// or more.
fn indented_beyond_p(interp: &mut Interpreter, env: &mut Env, mut pos: usize, column: i64) -> bool {
    let begv = interp.buffer.borrow().point_min();
    while pos > begv && interp.buffer.borrow().char_at(pos) == Some('\n') {
        pos = interp.buffer.borrow().line_start_at(pos - 1);
    }
    position_indentation(interp, env, pos) as i64 >= column
}

/// search.c find_before_next_newline (FROM, TO, 1).
fn find_before_next_newline(interp: &Interpreter, from: usize, to: usize) -> usize {
    interp.buffer.borrow().find_before_next_newline(from, to)
}

/// The character code compute_motion's FETCH_BYTE starts the character at
/// POS with: the character itself when it is ASCII or the buffer is
/// unibyte (a byte), and its code with LEADING set for a multibyte
/// character's leading byte.
fn fetch_char(interp: &Interpreter, pos: usize) -> i64 {
    super::public_buffer_char_code_at(interp, pos).unwrap_or(0)
}

/// indent.c compute_motion: scan the current buffer from FROM, taken to
/// be at line FROMVPOS, column FROMHPOS, to TO or to line TOVPOS, column
/// TOHPOS, whichever comes first, as window WIN lays the text out
/// WIDTH columns wide (negative for its text area less the continuation
/// glyph's column) with HSCROLL columns scrolled off and TAB_OFFSET
/// columns of the first tab not shown.  DID_MOTION is that FROMHPOS
/// already counts the overlay strings at FROM.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_motion(
    interp: &mut Interpreter,
    env: &mut Env,
    from: usize,
    fromvpos: i64,
    fromhpos: i64,
    mut did_motion: bool,
    to: usize,
    tovpos: i64,
    tohpos: i64,
    mut width: i64,
    hscroll: i64,
    mut tab_offset: i64,
    win: MotionWindow,
) -> Position {
    let mut hpos = fromhpos;
    let mut vpos = fromvpos;
    // SANE_TAB_WIDTH.
    let tab_width = interp
        .lookup_var_key(cached_symbol!("tab-width"), env)
        .and_then(|value| value.integer_value())
        .filter(|width| (1..=1000).contains(width))
        .unwrap_or(8);
    let ctl_arrow = interp
        .lookup_var_key(cached_symbol!("ctl-arrow"), env)
        .is_some_and(|value| !value.is_nil());
    let dp = window_display_table(interp, env, win);
    let selective = selective_display(interp, env);
    let selective_rlen = if selective != 0 {
        dp.and_then(|dp| interp.char_table_extra_slot(dp, 4))
            .filter(super::interactive::is_vector_value)
            .and_then(|vector| vector_items(&vector).ok())
            .map_or(0, |items| items.len() as i64)
    } else {
        0
    };
    let truncate_lines = interp
        .lookup_var("truncate-lines", env)
        .is_some_and(|value| !value.is_nil());
    let truncate_partial = interp
        .lookup_var("truncate-partial-width-windows", env)
        .unwrap_or(Value::Nil);
    let multibyte = interp.buffer.borrow().is_multibyte();
    let zv = interp.buffer.borrow().point_max();
    let window = text_prop_window(interp, win);

    // Negative width means use all available text columns, less one for
    // the continuation glyph on a character terminal.
    if width < 0 {
        let window_value = interp.record_value(win.id);
        width = super::dispatch::call(interp, "window-body-width", &[window_value], env)
            .ok()
            .and_then(|value| value.integer_value())
            .unwrap_or(0)
            - 1;
    }
    let continuation_glyph_width = 1i64;

    let mut pos = from;
    let mut prev_pos = from;
    let mut next_boundary = from;
    let mut wide_column_end_hpos = 0i64;
    let mut prev_hpos = 0i64;
    let mut prev_vpos = 0i64;
    let mut contin_hpos = 0i64;
    let mut prev_tab_offset = tab_offset;
    let mut cmp_it = CompositionIterator::new();
    cmp_it.compute_stop_pos(interp, env, pos, to);

    'scan: loop {
        while pos == next_boundary {
            let pos_here = pos;
            // Don't skip invisible text when already at the margin.
            if vpos > tovpos || (vpos == tovpos && hpos >= tohpos) {
                if contin_hpos != 0
                    && prev_hpos == 0
                    && hpos > tohpos
                    && (contin_hpos == width || wide_column_end_hpos > width)
                {
                    // The line broke because the character could not fit
                    // the previous one: back to where it begins.
                    pos = prev_pos;
                    hpos = prev_hpos;
                    vpos = prev_vpos;
                    tab_offset = prev_tab_offset;
                }
                break;
            }
            // The first time through, unless the caller's FROMHPOS counts
            // them, the overlay strings at POS take their columns.
            if !did_motion {
                hpos += overlay_strings_width(interp, env, pos, win);
            }
            did_motion = false;
            if pos >= to {
                break;
            }
            let newpos = skip_invisible(interp, env, pos, &mut next_boundary, to, window);
            if newpos >= to {
                pos = to.min(newpos);
                break 'scan;
            }
            if newpos != pos_here {
                pos = newpos;
            }
            if newpos > cmp_it.stop_pos && !cmp_it.composing() {
                cmp_it.reseat(interp, env, pos, to);
            }
        }

        // Handle the right margin.  A multi-column character (a tab, ^A,
        // \033) may be divided at the margin; a wide-column one cannot,
        // so the line may be shorter than WIDTH and its continuation is
        // only found after it happens.
        if hpos > width {
            let total_width = width + continuation_glyph_width;
            let mut truncate = false;
            if !truncate_partial.is_nil() && total_width < interp.frame_width() {
                truncate = match truncate_partial.kind() {
                    Kind::Integer(limit) => total_width < limit,
                    _ => true,
                };
            }
            if hscroll != 0 || truncate || truncate_lines {
                // Truncating: skip to the newline, unless already past TO.
                if pos <= to {
                    pos = find_before_next_newline(interp, pos, to);
                    hpos = width;
                    // A skipped NEXT_BOUNDARY is handled in the main loop.
                    if pos >= next_boundary {
                        next_boundary = pos + 1;
                    }
                    prev_hpos = width;
                    prev_vpos = vpos;
                    prev_tab_offset = tab_offset;
                }
            } else {
                // Continuing.
                prev_tab_offset = tab_offset;
                if wide_column_end_hpos > width {
                    hpos -= prev_hpos;
                    tab_offset += prev_hpos;
                } else {
                    tab_offset += width;
                    hpos -= width;
                }
                vpos += 1;
                contin_hpos = prev_hpos;
                prev_hpos = 0;
                prev_vpos = vpos;
            }
        }

        // Stop if past the target buffer position or screen position.
        if pos > to {
            pos = prev_pos;
            hpos = prev_hpos;
            vpos = prev_vpos;
            if contin_hpos != 0
                && prev_hpos == 0
                && contin_hpos < width
                && wide_column_end_hpos == 0
            {
                // The line broke in the middle of a multi-column
                // character: back to the previous line.
                hpos = contin_hpos;
                vpos -= 1;
            }
            break;
        }
        if vpos > tovpos || (vpos == tovpos && hpos >= tohpos) {
            if contin_hpos != 0
                && prev_hpos == 0
                && hpos > tohpos
                && (contin_hpos == width || wide_column_end_hpos > width)
            {
                pos = prev_pos;
                hpos = prev_hpos;
                vpos = prev_vpos;
            }
            break;
        }
        if pos == zv {
            break;
        }

        prev_hpos = hpos;
        prev_vpos = vpos;
        prev_pos = pos;
        wide_column_end_hpos = 0;

        // A composition sequence: a static composition takes its width,
        // an automatic one a glyph cluster's characters' at a time.
        if cmp_it.composing() || (pos == cmp_it.stop_pos && cmp_it.reseat(interp, env, pos, to)) {
            cmp_it.update(interp, env);
        }
        if cmp_it.composing() {
            pos += cmp_it.nchars;
            hpos += cmp_it.width;
            cmp_it.advance(interp, env, pos, to);
            continue;
        }

        let code = fetch_char(interp, pos);
        let leading = multibyte && code >= 0x80;
        pos += 1;
        let charvec = if leading {
            None
        } else {
            display_vector(interp, dp, code)
        };
        // Each glyph's character, and whether compute_motion takes it for
        // a multibyte form's leading byte: the character's own when it is
        // not ASCII, a glyph's when its code has the leading bits.
        let glyphs: Vec<(i64, bool)> = match &charvec {
            Some(entries) => entries
                .iter()
                .map(glyph_code_char)
                .map(|c| (c, multibyte && (c & 0xC0) == 0xC0))
                .collect(),
            None => vec![(code, leading)],
        };
        for (c, leading_code) in glyphs {
            if (0o40..0o177).contains(&c) {
                hpos += 1;
            } else if c == i64::from(b'\t') {
                let mut tem = (hpos + tab_offset + hscroll - i64::from(hscroll > 0)) % tab_width;
                if tem < 0 {
                    tem += tab_width;
                }
                hpos += tab_width - tem;
            } else if c == i64::from(b'\n') {
                if selective > 0 && indented_beyond_p(interp, env, pos, selective) {
                    // At TO there is no selective display to take care of.
                    if pos < to {
                        // Skip any number of invisible lines all at once.
                        loop {
                            pos = find_before_next_newline(interp, pos, to);
                            if pos < to {
                                pos += 1;
                            }
                            if !(pos < to && indented_beyond_p(interp, env, pos, selective)) {
                                break;
                            }
                        }
                        // Allow for the " ..." shown for them.
                        if selective_rlen != 0 {
                            hpos += selective_rlen;
                            if hpos >= width {
                                hpos = width;
                            }
                        }
                        // The invisible text is skipped, not the newline
                        // after it.
                        pos -= 1;
                    }
                } else {
                    // A visible line.
                    vpos += 1;
                    hpos = -hscroll;
                    // Count the truncation glyph on column 0.
                    if hscroll > 0 {
                        hpos += continuation_glyph_width;
                    }
                    tab_offset = 0;
                }
                contin_hpos = 0;
            } else if c == i64::from(b'\r') && selective < 0 {
                // Under selective display everything from a ^M to the end
                // of the line is invisible: stop before the real newline.
                if pos < to {
                    pos = find_before_next_newline(interp, pos, to);
                }
                if pos > next_boundary {
                    next_boundary = pos;
                }
                if selective_rlen != 0 {
                    hpos += selective_rlen;
                    if hpos >= width {
                        hpos = width;
                    }
                }
            } else if leading_code {
                // MULTIBYTE_BYTES_WIDTH of the character just stepped
                // over, whose leading byte this is -- or, for a display
                // vector's glyph, of the ASCII character the vector is
                // for, rewound to.
                let mb_width = multibyte_char_width(interp, env, code as u32, dp).min(1000) as i64;
                if mb_width > 1 {
                    wide_column_end_hpos = hpos + mb_width;
                }
                hpos += mb_width;
            } else if charvec.is_some() {
                hpos += 1;
            } else {
                hpos += if ctl_arrow && c < 0o200 { 2 } else { 4 };
            }
        }
    }

    let contin = contin_hpos != 0 && prev_hpos == 0;
    Position {
        bufpos: pos,
        hpos,
        vpos,
        prevhpos: if contin { contin_hpos } else { prev_hpos },
        contin,
    }
}

/// indent.c vmotion: the start of the screen line VTARGET lines from
/// FROM as WIN lays the current buffer out (moving up a line at a time
/// past lines hidden by selective display or an invisible newline, then
/// down from the start of FROM's line).
pub(crate) fn vmotion(
    interp: &mut Interpreter,
    env: &mut Env,
    mut from: usize,
    vtarget: i64,
    win: MotionWindow,
) -> Position {
    let hscroll = super::dispatch::display::window_hscroll_state(interp, win.id).hscroll;
    let lmargin = if hscroll > 0 { 1 - hscroll } else { 0 };
    let selective = selective_display(interp, env).max(-1);
    let window = text_prop_window(interp, win);
    let begv = interp.buffer.borrow().point_min();
    let mut vpos = 0i64;
    // A line start PREVLINE that moving by screen lines skips: one hidden
    // by selective display, or whose newline (NEWLINE) is invisible.
    let hidden = |interp: &mut Interpreter, env: &mut Env, prevline: usize, newline: usize| {
        (selective > 0 && indented_beyond_p(interp, env, prevline, selective))
            || invisible_at(interp, env, newline, window) != 0
    };

    if vpos >= vtarget {
        // To move upward, go a line at a time until far enough.
        let mut first = true;
        while (vpos > vtarget || first) && from > begv {
            let mut prevline = interp.buffer.borrow().line_start_at(from - 1);
            // When moving upward, check the newline before.
            while prevline > begv && hidden(interp, env, prevline, prevline - 1) {
                prevline = interp.buffer.borrow().line_start_at(prevline - 1);
            }
            let pos = compute_motion(
                interp,
                env,
                prevline,
                0,
                lmargin,
                false,
                from,
                1 << 15,
                1 << 15,
                -1,
                hscroll,
                0,
                win,
            );
            vpos -= pos.vpos;
            first = false;
            from = prevline;
        }
        // Exactly the distance wanted, or the beginning of the buffer.
        if vpos >= vtarget {
            return Position {
                bufpos: from,
                hpos: lmargin,
                vpos,
                prevhpos: 0,
                contin: false,
            };
        }
        // Otherwise find the spot moving down.
    }
    // Moving down: the column of FROM, counted from its line's start.
    let (hpos, did_motion) =
        if from > begv && interp.buffer.borrow().char_at(from - 1) != Some('\n') {
            let mut prevline = interp.buffer.borrow().line_start_at(from);
            // When moving downward, check the newline after.
            while prevline > begv && hidden(interp, env, prevline, prevline) {
                prevline = interp.buffer.borrow().line_start_at(prevline - 1);
            }
            let pos = compute_motion(
                interp,
                env,
                prevline,
                0,
                lmargin,
                false,
                from,
                1 << 15,
                1 << 15,
                -1,
                hscroll,
                0,
                win,
            );
            (pos.hpos, true)
        } else {
            (lmargin, false)
        };
    let zv = interp.buffer.borrow().point_max();
    compute_motion(
        interp,
        env,
        from,
        vpos,
        hpos,
        did_motion,
        zv,
        vtarget,
        -(1 << 15),
        -1,
        hscroll,
        0,
        win,
    )
}
