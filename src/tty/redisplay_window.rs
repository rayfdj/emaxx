//! xdisp.c redisplay_window's choice of where a window's display starts:
//! a forced start (set-window-start, the scroll commands) kept with point
//! moved into view, an optional one (recenter) kept when point is visible
//! from it, the same start kept while point stays visible outside the
//! scroll margins, try_scrolling's scroll-conservatively, scroll-step and
//! scroll-*-aggressively, and recentering.  Every window gets the same
//! treatment; a terminal row is one line high, so GNU's pixel arithmetic
//! is row arithmetic here.

use super::{
    CharacterDisplayContext, VisualLine, glyphless_visual_line_at, layout,
    wrap_glyphless_visual_line, wrapped_position_of_column,
};
use crate::buffer::Buffer;
use crate::lisp::primitives::{InvisibilitySpec, invisible_class_at, visual_line_first_line};
use crate::lisp::types::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// One glyph row: segment SEG of the display line that begins at raw
/// line LINE, laid out from the line's start, or with ORIGIN non-zero
/// from that position inside it, where a window's display starts
/// (start_display).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Row {
    pub(super) line: usize,
    pub(super) seg: usize,
    pub(super) origin: usize,
}

/// A per-plan cache keyed by a display line's raw line and the origin it
/// is laid out from.
type LineCache<T> = RefCell<HashMap<(usize, usize), T>>;

/// A display line's rows, the raw lines it spans, and whether it is a
/// wholly invisible tail that produces no row.
type LineInfo = (usize, usize, bool);

/// A window's rows over its buffer under its wrap geometry, walked the
/// way move_it_by_lines walks them.
pub(super) struct Rows<'a, 'c> {
    pub(super) buffer: &'a Buffer,
    spec: &'a InvisibilitySpec,
    glyphless: Option<&'a CharacterDisplayContext<'c>>,
    body_width: usize,
    truncate: bool,
    first_line: usize,
    last_line: usize,
    /// Per display line and origin: its rows, the raw lines it spans, and
    /// whether it is a wholly invisible tail that produces no row.
    lines: LineCache<LineInfo>,
    /// Per display line: the line as the window shows it, laid out once
    /// for every question the plan asks of it (the buffer cannot change
    /// while the plan borrows it).
    visuals: RefCell<HashMap<usize, Rc<VisualLine>>>,
    /// Per display line and origin: where each of its rows starts.
    row_starts: LineCache<Rc<Vec<usize>>>,
    /// Per display line and origin: its layout continued at the window's
    /// width.
    layouts: LineCache<Rc<layout::LineLayout>>,
}

impl<'a, 'c> Rows<'a, 'c> {
    pub(super) fn new(
        buffer: &'a Buffer,
        spec: &'a InvisibilitySpec,
        glyphless: Option<&'a CharacterDisplayContext<'c>>,
        body_width: usize,
        truncate: bool,
    ) -> Self {
        Rows {
            buffer,
            spec,
            glyphless,
            body_width,
            truncate,
            first_line: buffer.line_number_at_pos(buffer.point_min()),
            last_line: buffer.line_number_at_pos(buffer.point_max()),
            lines: RefCell::new(HashMap::new()),
            visuals: RefCell::new(HashMap::new()),
            row_starts: RefCell::new(HashMap::new()),
            layouts: RefCell::new(HashMap::new()),
        }
    }

    /// handle_line_prefix on a row past ZV, where display_line still
    /// starts each row of the window: the `line-prefix' there.
    pub(super) fn prefix_past_zv(&self) -> Option<String> {
        let context = self.glyphless?;
        context.prefix_glyphs(&context.prefix_value_at(self.buffer, self.buffer.point_max(), false))
    }

    /// The display line beginning at raw line LINE laid out at the
    /// window's width from ORIGIN (0: from its start), continued or, in a
    /// truncating window, broken only where a string's newline ends a
    /// row; made once.
    pub(super) fn laid_at(&self, line: usize, origin: usize) -> Rc<layout::LineLayout> {
        if let Some(laid) = self.layouts.borrow().get(&(line, origin)) {
            return Rc::clone(laid);
        }
        let visual = self.visual(line);
        let walk = !self.at_line_beg(origin) && !self.truncate;
        let laid = Rc::new(if origin == 0 {
            layout::lay_out_visual_line(&visual, self.body_width, self.truncate)
        } else {
            layout::lay_out_visual_line_from(
                &visual,
                self.body_width,
                self.truncate,
                visual.start_index(
                    origin.saturating_sub(self.buffer.display_line_start_of(line)),
                    walk,
                ),
                walk,
            )
        });
        self.layouts
            .borrow_mut()
            .insert((line, origin), Rc::clone(&laid));
        laid
    }

    /// The display index of buffer position POS in VISUAL, the display
    /// line beginning at raw line LINE.
    fn index_of(&self, line: usize, visual: &VisualLine, pos: usize) -> usize {
        let offset = pos.saturating_sub(self.buffer.display_line_start_of(line));
        visual
            .map
            .get(offset)
            .copied()
            .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0))
    }

    /// The display line beginning at raw line LINE as the window shows it.
    pub(super) fn visual(&self, line: usize) -> Rc<VisualLine> {
        if let Some(visual) = self.visuals.borrow().get(&line) {
            return Rc::clone(visual);
        }
        let visual = Rc::new(glyphless_visual_line_at(
            self.buffer,
            self.spec,
            line,
            self.glyphless,
        ));
        self.visuals.borrow_mut().insert(line, Rc::clone(&visual));
        visual
    }

    /// Where each row of the display line beginning at raw line LINE
    /// starts: the line's start, then the position each continuation
    /// row's first glyph comes from, the line wrapped USABLE + 1 wide.
    fn row_starts(&self, line: usize, origin: usize) -> Rc<Vec<usize>> {
        if let Some(starts) = self.row_starts.borrow().get(&(line, origin)) {
            return Rc::clone(starts);
        }
        let start = self.buffer.display_line_start_of(line);
        let visual = self.visual(line);
        let usable = self.body_width.saturating_sub(1).max(1);
        // At any width but one column that is the plan's own layout.
        let laid =
            (usable + 1 == self.body_width || origin != 0).then(|| self.laid_at(line, origin));
        let other;
        let segments = match &laid {
            Some(laid) => &laid.rows,
            None => {
                other = wrap_glyphless_visual_line(&visual, usable + 1);
                &other
            }
        };
        let first = if origin == 0 { start } else { origin };
        let starts: Vec<usize> = segments
            .iter()
            .enumerate()
            .map(|(seg, segment)| {
                if seg == 0 {
                    first
                } else {
                    start + visual.row_start_offset(segment.start_index)
                }
            })
            .collect();
        let starts = Rc::new(starts);
        self.row_starts
            .borrow_mut()
            .insert((line, origin), Rc::clone(&starts));
        starts
    }

    fn line_info(&self, line: usize, origin: usize) -> LineInfo {
        if let Some(info) = self.lines.borrow().get(&(line, origin)) {
            return *info;
        }
        let visual = self.visual(line);
        let line_start = self.buffer.display_line_start_of(line);
        let point_max = self.buffer.point_max();
        let invisible_tail = visual.text.is_empty()
            && line_start < point_max
            && invisible_class_at(self.buffer, self.spec, line_start) != 0
            && visual.raw_of_display.last().copied() == Some(point_max.saturating_sub(line_start));
        let segs = self.laid_at(line, origin).rows.len().max(1);
        let info = (segs, visual.lines_spanned.max(1), invisible_tail);
        self.lines.borrow_mut().insert((line, origin), info);
        info
    }

    fn produces_row(&self, line: usize) -> bool {
        line <= self.last_line && !self.line_info(line, 0).2
    }

    pub(super) fn next(&self, row: Row) -> Option<Row> {
        let (segs, spanned, _) = self.line_info(row.line, row.origin);
        if row.seg + 1 < segs {
            return Some(Row {
                seg: row.seg + 1,
                ..row
            });
        }
        let line = row.line + spanned;
        self.produces_row(line).then_some(Row {
            line,
            ..Row::default()
        })
    }

    /// The row above ROW.  Moving back from a display starting inside a
    /// line walks that line's own rows (move_it_vertically_backward
    /// measures from the line's start to the position): the row above a
    /// start's is the one above the line's own row holding the start.
    pub(super) fn prev(&self, row: Row) -> Option<Row> {
        if row.seg > 0 {
            return Some(Row {
                seg: row.seg - 1,
                ..row
            });
        }
        if row.origin != 0 {
            return self.prev(self.row_of(row.origin));
        }
        if row.line <= self.first_line {
            return None;
        }
        let line =
            visual_line_first_line(self.buffer, self.spec, row.line - 1).max(self.first_line);
        let (segs, _, _) = self.line_info(line, 0);
        Some(Row {
            line,
            seg: segs - 1,
            origin: 0,
        })
    }

    /// Buffer position where ROW begins under the window's wrap geometry:
    /// where its first glyph comes from.
    pub(super) fn row_start(&self, row: Row) -> usize {
        let start = self.buffer.display_line_start_of(row.line);
        if row.seg == 0 {
            return if row.origin == 0 { start } else { row.origin };
        }
        self.row_starts(row.line, row.origin)
            .get(row.seg)
            .copied()
            .unwrap_or_else(|| {
                start
                    + self
                        .visual(row.line)
                        .raw_of_display
                        .last()
                        .copied()
                        .unwrap_or(0)
            })
    }

    /// Whether display_line ends ROW with IT_CHARPOS past the row's start
    /// position, the condition, with first_visible_x past 0, for the left
    /// truncation glyph (insert_left_trunc_glyphs).  A row of a truncated
    /// line ends at a string's newline -- the iterator then delivers the
    /// row below's first element, at the string's position while more of
    /// it follows or the position an overlay string returns to, at the
    /// end of the text a `display' string covers -- or at the line's end,
    /// past its newline: an empty line's row ends past its start, a row
    /// at ZV with no newline does not.
    pub(super) fn ends_past_start(&self, row: Row) -> bool {
        self.end_pos(row) != self.row_start(row)
    }

    /// MATRIX_ROW_END_CHARPOS of ROW, a row of a truncated line: where
    /// the row below starts when a string's newline ended it, else past
    /// the line's newline, or ZV.
    pub(super) fn end_pos(&self, row: Row) -> usize {
        let laid = self.laid_at(row.line, row.origin);
        if row.seg + 1 < laid.rows.len() {
            self.row_start(Row {
                seg: row.seg + 1,
                ..row
            })
        } else {
            let line_end = self.buffer.display_line_start_of(row.line)
                + self
                    .visual(row.line)
                    .raw_of_display
                    .last()
                    .copied()
                    .unwrap_or(0);
            if line_end < self.buffer.point_max() {
                line_end + 1
            } else {
                line_end
            }
        }
    }

    /// MATRIX_ROW_START_CHARPOS of ROW.
    pub(super) fn start_pos(&self, row: Row) -> usize {
        self.row_start(row).max(self.buffer.point_min())
    }

    /// The row showing POS, its line laid out from its start.
    pub(super) fn row_of(&self, pos: usize) -> Row {
        let buffer = self.buffer;
        let pos = pos.clamp(buffer.point_min(), buffer.point_max());
        let line = visual_line_first_line(buffer, self.spec, buffer.line_number_at_pos(pos))
            .max(self.first_line);
        let row = Row {
            line,
            ..Row::default()
        };
        self.row_in_layout(row, pos)
    }

    /// The row of the display line ROW is on (laid out from ROW's origin)
    /// showing POS.
    fn row_in_layout(&self, row: Row, pos: usize) -> Row {
        let visual = self.visual(row.line);
        let index = self.index_of(row.line, &visual, pos);
        let laid = self.laid_at(row.line, row.origin);
        let column = laid.columns[index.min(laid.columns.len() - 1)];
        let (seg, _) = wrapped_position_of_column(
            &laid.rows,
            column,
            visual
                .glyphless_spans
                .iter()
                .any(|(from, _)| *from == index),
        );
        Row { seg, ..row }
    }

    /// start_display: the first row of a display starting at START,
    /// which begins at START wherever it falls in its line, with what
    /// init_iterator's handle_stop shows there first (the overlay
    /// strings at START before its text, a display string or ellipsis
    /// covering it from its beginning).  A row of the line's own layout
    /// beginning with that display element is that row, when the walk
    /// to START's text stops on it too (it takes that row's
    /// continuation_lines_width).
    pub(super) fn top_row(&self, start: usize) -> Row {
        let start = start.clamp(self.buffer.point_min(), self.buffer.point_max());
        let row = self.row_of(start);
        let origin = Row {
            line: row.line,
            seg: 0,
            origin: start,
        };
        // Truncating, start_display does not walk from the line's start:
        // init_iterator at START shows the strings there first.
        if self.truncate {
            return if start == self.buffer.display_line_start_of(row.line) {
                Row { seg: 0, ..row }
            } else {
                origin
            };
        }
        let line_start = self.buffer.display_line_start_of(row.line);
        let visual = self.visual(row.line);
        let walked = !self.at_line_beg(start);
        let first = visual.start_index(start.saturating_sub(line_start), walked);
        // No walk from a line's beginning: its first row is the line's.
        let walk_to = if walked {
            visual.walk_stop_index(first)
        } else {
            first
        };
        let laid = self.laid_at(row.line, 0);
        match laid
            .rows
            .iter()
            .position(|segment| segment.start_index == first)
        {
            Some(seg)
                if laid
                    .rows
                    .iter()
                    .rposition(|segment| segment.start_index <= walk_to)
                    == Some(seg) =>
            {
                Row { seg, ..row }
            }
            _ => origin,
        }
    }

    /// The display indexes ROW shows: from its first through the one
    /// before the next row's, a newline ending it included.
    fn row_indexes(&self, row: Row) -> std::ops::Range<usize> {
        let laid = self.laid_at(row.line, row.origin);
        let count = self.visual(row.line).text.chars().count();
        let start = laid
            .rows
            .get(row.seg)
            .map_or(count, |segment| segment.start_index);
        let end = laid
            .rows
            .get(row.seg + 1)
            .map_or(count, |segment| segment.start_index);
        start..end.max(start)
    }

    /// The newline ending ROW, when a string produced it
    /// (ends_in_newline_from_string_p): its display index and the
    /// string (an index into the visual line's objects).
    fn string_newline_ending(&self, row: Row) -> Option<(usize, usize)> {
        let visual = self.visual(row.line);
        let last = self.row_indexes(row).end.checked_sub(1)?;
        (visual.text.chars().nth(last) == Some('\n'))
            .then(|| visual_object(&visual, last))
            .flatten()
            .map(|object| (last, object))
    }

    /// find_row_edges' MATRIX_ROW_END_CHARPOS of ROW, the window's row
    /// below PREV (none for its first row), where it is not where the
    /// next row starts: one past the buffer position of the string whose
    /// newline ends ROW.  Either the string ends there and a `display'
    /// property gave it, so the iterator pops it to the end of the text
    /// it replaces, past max_pos (an overlay string pops to its own
    /// position, where the next row starts); or it goes on past the
    /// newline and PREV ended in a newline from a string too having seen
    /// this string, so the row ends at eol_pos + 1.
    pub(super) fn string_row_end(&self, row: Row, prev: Option<Row>) -> Option<usize> {
        let (newline, object) = self.string_newline_ending(row)?;
        let visual = self.visual(row.line);
        let raw = visual.raw_of_display.get(newline).copied()?;
        let past = self.buffer.display_line_start_of(row.line) + raw + 1;
        // it->object past the newline: the same string still.
        if visual_object(&visual, newline + 1) != Some(object) {
            return visual.object_from_display(newline).then_some(past);
        }
        let (string, _) = visual.object_at(newline)?;
        self.seen_this_string(prev?, string).then_some(past)
    }

    /// Whether ROW shows no glyph at all: the row display_line starts at
    /// ZV, which displays no text (displays_text_p false).
    pub(super) fn empty_row(&self, row: Row) -> bool {
        self.row_indexes(row).is_empty()
    }

    /// find_row_edges' seen_this_string for STRING: PREV ends in a
    /// newline from a string, and the last of its glyphs that came from
    /// buffer or string -- past the trailing ones redisplay inserted,
    /// whose object is nil and charpos not positive -- came from STRING;
    /// or no glyph did.  The newline's own glyph is such a one unless it
    /// is the row's first (used_before == 0), which takes the newline's
    /// string index as its charpos; line-number glyphs before it always
    /// are, a prefix's glyphs never (their object is the prefix).
    fn seen_this_string(&self, prev: Row, string: Value) -> bool {
        let Some((newline, _)) = self.string_newline_ending(prev) else {
            return false;
        };
        let visual = self.visual(prev.line);
        if newline > self.row_indexes(prev).start {
            return visual
                .object_at(newline - 1)
                .is_some_and(|(object, _)| crate::lisp::types::eq(object, string));
        }
        let prefix_cols = self
            .laid_at(prev.line, prev.origin)
            .rows
            .get(prev.seg)
            .map_or(0, |segment| segment.prefix_cols);
        if prefix_cols > 0 {
            return false;
        }
        visual.lnum_cols > 0
            || visual
                .object_at(newline)
                .is_some_and(|(_, index)| index == 0)
    }

    /// set_cursor_from_row's x on ROW, every glyph of which comes from a
    /// string, for POINT, whose glyph it does not show (the row the
    /// force_start block moves point to): step 2 finds no glyph for
    /// point and looks through the row's strings in turn.  A `display'
    /// string at point or after it (string_buffer_position_lim's TEM)
    /// puts the cursor on its first glyph whose character has a non-nil
    /// `cursor' property, else on its first glyph, and the search ends;
    /// an overlay string's (TEM 0) `cursor' glyph is a candidate, the
    /// last one found taken.  With none the cursor goes past the row's
    /// glyphs onto its newline's.  A row the string continues past puts
    /// it on the row's first glyph, a line number's included (the FALSE
    /// answer: before the gutter).
    pub(super) fn string_row_cursor_x(&self, row: Row, point: usize) -> Option<(usize, bool)> {
        let visual = self.visual(row.line);
        let indexes = self.row_indexes(row);
        if indexes.is_empty()
            || indexes
                .clone()
                .any(|index| visual_object(&visual, index).is_none())
        {
            return None;
        }
        let laid = self.laid_at(row.line, row.origin);
        let column = |index: usize| {
            let start = laid
                .rows
                .get(row.seg)
                .map_or(0, |segment| segment.start_column);
            laid.columns
                .get(index)
                .copied()
                .unwrap_or(start)
                .saturating_sub(start)
        };
        let has_cursor = |index: usize| {
            visual.object_at(index).is_some_and(|(value, character)| {
                crate::lisp::primitives::string_property_at(&value, character, "cursor")
                    .is_some_and(|cursor| cursor.is_truthy())
            })
        };
        let line_start = self.buffer.display_line_start_of(row.line);
        let mut cursor = None;
        let mut index = indexes.start;
        while index < indexes.end {
            let object = visual_object(&visual, index);
            let run_end = (index..indexes.end)
                .find(|&next| visual_object(&visual, next) != object)
                .unwrap_or(indexes.end);
            let found = (index..run_end).find(|&glyph| has_cursor(glyph));
            if visual.object_from_display(index) {
                let tem = visual
                    .raw_of_display
                    .get(index)
                    .map(|&raw| line_start + raw);
                if tem.is_some_and(|tem| tem >= point) {
                    cursor = Some(found.unwrap_or(index));
                    break;
                }
            } else if found.is_some() {
                cursor = found;
            }
            index = run_end;
        }
        match (cursor, self.string_newline_ending(row)) {
            (Some(index), _) | (None, Some((index, _))) => Some((column(index), true)),
            (None, None) => Some((0, false)),
        }
    }

    /// The row showing POS in a display whose first row is TOP.
    pub(super) fn row_in(&self, top: Row, pos: usize) -> Row {
        let row = self.row_of(pos);
        if top.origin == 0 || row.line != top.line || pos < top.origin {
            return row;
        }
        self.row_in_layout(top, pos)
    }

    /// Rows from FROM down to TO, when TO lies within LIMIT rows.
    pub(super) fn distance(&self, from: Row, to: Row, limit: usize) -> Option<usize> {
        if to < from {
            return None;
        }
        if to.line == from.line && to.origin == from.origin {
            return Some(to.seg - from.seg);
        }
        if !self.spec.active && to.line - from.line > limit {
            // Every line fills at least one row.
            return None;
        }
        let mut row = from;
        let mut count = 0usize;
        while row != to {
            if count >= limit {
                return None;
            }
            row = self.next(row)?;
            count += 1;
        }
        Some(count)
    }

    /// Up to COUNT rows down from ROW, stopping at the last row.
    pub(super) fn forward(&self, mut row: Row, count: usize) -> (Row, usize) {
        let mut moved = 0;
        while moved < count {
            match self.next(row) {
                Some(next) => row = next,
                None => break,
            }
            moved += 1;
        }
        (row, moved)
    }

    /// overlay_touches_p: an overlay begins or ends at POS.
    pub(super) fn overlay_touches(&self, pos: usize) -> bool {
        self.buffer
            .overlays
            .iter()
            .any(|overlay| !overlay.is_dead() && (overlay.beg() == pos || overlay.end() == pos))
    }

    /// Up to COUNT rows up from ROW, stopping at BEGV's row.
    pub(super) fn backward(&self, mut row: Row, count: usize) -> Row {
        for _ in 0..count {
            match self.prev(row) {
                Some(prev) => row = prev,
                None => break,
            }
        }
        row
    }

    /// FETCH_BYTE (BYTEPOS (pos) - 1) == '\n', which reads the text
    /// before POS wherever the restriction lies.
    fn char_before_is_newline(&self, pos: usize) -> bool {
        pos > 1 && self.buffer.fetch_char(pos - 1) == Some('\n')
    }

    pub(super) fn at_line_beg(&self, pos: usize) -> bool {
        pos <= self.buffer.point_min() || self.char_before_is_newline(pos)
    }
}

/// The scroll options redisplay consults, read in the window's buffer.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ScrollParams {
    /// window_scroll_margin in lines.
    pub(super) margin: usize,
    pub(super) conservatively: i64,
    pub(super) step: i64,
    pub(super) up_aggressively: Option<f64>,
    pub(super) down_aggressively: Option<f64>,
}

/// Where a window's redisplay resumes once `window-scroll-functions' ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    ForceStart,
    Scrolled,
    /// The recenter label after the scroll functions, its iterator at
    /// ITERATOR, the start it computed.
    Recentered {
        iterator: usize,
    },
}

/// What redisplay_window reads of a window and its buffer.
#[derive(Clone, Copy, Debug)]
pub(super) struct StartState {
    /// The window's start marker.
    pub(super) start: usize,
    /// The window's point (the buffer's for the selected window).
    pub(super) point: usize,
    /// The buffer's own point, which compute_window_start_on_continuation_line
    /// reads before redisplay switches to the window's point.
    pub(super) buffer_point: usize,
    pub(super) force_start: bool,
    pub(super) optional_new_start: bool,
    pub(super) start_at_line_beg: bool,
    pub(super) windows_or_buffers_changed: bool,
    /// The buffer changed since this window was last displayed.
    pub(super) window_outdated: bool,
    /// try_cursor_movement's success without motion: nothing about the
    /// window, its buffer or point changed since its last display.
    pub(super) cursor_unmoved: bool,
    /// w->last_point: the window's point when its display was last
    /// complete.
    pub(super) last_point: Option<usize>,
    /// current_matrix_up_to_date_p with try_cursor_movement's other
    /// conditions: the window's text, start and restriction as its last
    /// display left them, point aside.
    pub(super) matrix_current: bool,
    /// `w->cursor.y' as a text row, from the window's last display that
    /// showed point: try_window's margin test reads it when point does
    /// not show.
    pub(super) cursor_y: usize,
    /// `window-scroll-functions' is non-nil in the window's buffer.
    pub(super) scroll_functions: bool,
    pub(super) make_window_start_visible: bool,
    /// WINDOW_TOTAL_LINES times WINDOW_TOTAL_COLS.
    pub(super) total_cells: usize,
    /// Tab and header rows above the text (their y precedes the text's).
    pub(super) top_rows: usize,
    pub(super) scroll: ScrollParams,
    /// redisplay_window's beg_unchanged and end_unchanged as positions:
    /// BEG + BEG_UNCHANGED and Z - END_UNCHANGED.
    pub(super) changed_region: (i64, i64),
    /// `track-mouse' is non-nil (a down-mouse drag in progress).
    pub(super) track_mouse: bool,
    /// Resume at a stage with the start the scroll functions left, and
    /// whether the way there already had the mode line redisplayed.
    pub(super) resume: Option<(Stage, usize, bool)>,
}

/// The start chosen, and what redisplay changed on the way.
#[derive(Clone, Copy, Debug)]
pub(super) struct Chosen {
    /// The new start marker position.
    pub(super) start: usize,
    /// The row displayed first.
    pub(super) top: Row,
    /// Point's row from the top, if point shows.
    pub(super) cursor_row: Option<usize>,
    /// Point moved into view (the force_start case).
    pub(super) point: Option<usize>,
    pub(super) start_at_line_beg: bool,
    /// redisplay_window's local update_mode_line as its start decision
    /// leaves it: set where it forces the start or tries to scroll.
    pub(super) update_mode_line: bool,
}

/// A walk of the display iterator redisplay_window makes on its way to
/// a window's display, recorded so the text it fetches is fontified
/// (handle_fontified_prop) in the order GNU's walks fetch it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FetchStep {
    /// try_window at START: start_display, then the window's rows, each
    /// to its end, whether or not they then show point.
    Display { start: usize },
    /// init_iterator at POS: reseat's handle_stop there.
    Init { pos: usize },
    /// start_display at START.
    StartDisplay { start: usize },
    /// move_it_to POS (MOVE_TO_POS | MOVE_TO_Y) from the iterator
    /// start_display left at FROM, no lower than ROWS rows below FROM's.
    MoveTo {
        from: usize,
        pos: usize,
        rows: usize,
    },
    /// move_it_by_lines 1, ROWS times, from the iterator start_display
    /// left at FROM (try_scrolling under `scroll-conservatively' past
    /// SCROLL_LIMIT).
    Lines { from: usize, rows: usize },
    /// partial_line_height of the iterator start_display left at FROM:
    /// a copy moved to the window's last visible y, then a line on
    /// unless it reached ZV (none for very long truncated lines).
    Partial { from: usize },
    /// move_it_vertically_backward ROWS rows from an iterator at POS.
    Back { pos: usize, rows: usize },
    /// The recenter label's move back ROWS rows from its iterator at
    /// POS: move_it_vertically_backward, or for very long truncated
    /// lines back_to_previous_visible_line_start and reseat_1, which
    /// fetch nothing.
    RecenterBack { pos: usize, rows: usize },
    /// move_it_by_lines DVPOS from an iterator at FROM: move_it_vertically's
    /// move_it_by_lines 0 when it stops at a ZV no newline comes before,
    /// and the recenter label's retries when point does not show.
    ByLines { from: usize, dvpos: i64 },
    /// The recenter label's iterator again, after the scroll functions and
    /// try_window: it goes on from AT, where it set the window's start.
    Continue { at: usize },
    /// pos_visible_p of POS from the window's start START: its
    /// start_display, and the walk to POS or over the window's rows.
    PosVisible { start: usize, pos: usize },
    /// run_window_scroll_functions' SET_MARKER_FROM_TEXT_POS: the
    /// window's start is START from here on (try_scrolling's and the
    /// recenter label's new start, which try_window then displays).
    SetStart { start: usize },
    /// compute_window_start_on_continuation_line: init_iterator at
    /// START, reseat_at_previous_visible_line_start to LINE_START, then
    /// move_it_by_lines 1 ROWS times.
    ContinuationLine {
        start: usize,
        line_start: usize,
        rows: usize,
    },
}

pub(super) enum Decision {
    /// Run `window-scroll-functions' with START as the window's start,
    /// then resume at STAGE with the start they leave.
    Hook {
        stage: Stage,
        start: usize,
        update_mode_line: bool,
    },
    Display(Chosen),
}

/// The flags redisplay consumed before deciding.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Consumed {
    pub(super) force_start: bool,
    pub(super) optional_new_start: bool,
}

struct Planner<'r, 'a, 'c> {
    rows: &'r Rows<'a, 'c>,
    state: &'r StartState,
    height: usize,
    point: usize,
    point_row: Row,
    begv: usize,
    zv: usize,
    margin: usize,
    /// The decision's update_mode_line so far.
    update_mode_line: std::cell::Cell<bool>,
    /// The iterator's walks so far.
    steps: std::cell::RefCell<Vec<FetchStep>>,
}

const SCROLL_LIMIT: i64 = 100;

/// redisplay_window's decision for one window of TEXT_ROWS rows.
pub(super) fn decide(
    rows: &Rows,
    state: &StartState,
    text_rows: usize,
) -> (Decision, Consumed, Vec<FetchStep>) {
    let begv = rows.buffer.point_min();
    let zv = rows.buffer.point_max();
    let point = state.point.clamp(begv, zv);
    let planner = Planner {
        rows,
        state,
        height: text_rows.max(1),
        point,
        point_row: rows.row_of(point),
        begv,
        zv,
        margin: state.scroll.margin,
        update_mode_line: std::cell::Cell::new(false),
        steps: std::cell::RefCell::new(Vec::new()),
    };
    let finish = |decision: Decision, consumed: Consumed| {
        let steps = planner.steps.take();
        (decision, consumed, steps)
    };
    let mut consumed = Consumed::default();
    if let Some((stage, start, update_mode_line)) = state.resume {
        planner.update_mode_line.set(update_mode_line);
        let decision = match stage {
            Stage::ForceStart => planner.forced(start),
            Stage::Scrolled => planner.scrolled(start, state.start),
            Stage::Recentered { iterator } => planner.recentered(start, iterator),
        };
        return finish(decision, consumed);
    }
    let mut start = state.start;
    if state.windows_or_buffers_changed {
        start = planner
            .continuation_line_start(start, state.buffer_point)
            .unwrap_or(start);
    }
    let mut force = state.force_start;
    if state.optional_new_start && planner.in_range(start) {
        consumed.optional_new_start = true;
        if !force {
            // start_display at the start, then move_it_to PT no lower
            // than the window's last visible y.
            planner.walk(FetchStep::StartDisplay { start });
            planner.walk(FetchStep::MoveTo {
                from: start,
                pos: point,
                rows: planner.height,
            });
            if planner.visible_row(start).is_some() {
                force = true;
            }
        }
    }
    if force {
        consumed.force_start = true;
        if state.scroll_functions {
            // The force_start block redisplays the mode line before it
            // runs the scroll functions.
            return finish(
                Decision::Hook {
                    stage: Stage::ForceStart,
                    start,
                    update_mode_line: true,
                },
                consumed,
            );
        }
        let decision = planner.forced(start);
        return finish(decision, consumed);
    }
    let decision = planner.ignore_start(start);
    finish(decision, consumed)
}

impl Planner<'_, '_, '_> {
    fn in_range(&self, pos: usize) -> bool {
        self.begv <= pos && pos <= self.zv
    }

    /// Point's row counted from the row START begins, if it shows there.
    fn visible_row(&self, start: usize) -> Option<usize> {
        if self.point < start || !self.in_range(start) {
            return None;
        }
        let top = self.rows.top_row(start);
        self.rows
            .distance(top, self.rows.row_in(top, self.point), self.height)
            .filter(|row| *row < self.height)
    }

    /// try_cursor_movement with point where the window's last display
    /// left it: the row the cursor was on then (w->last_cursor_vpos),
    /// which an unmoved point does not leave, while point is within that
    /// row's start and end positions -- a row of a string before point's
    /// glyph or of a `display' string taller than the window, where the
    /// force_start block put the cursor, rather than point's glyph's.
    fn unmoved_cursor_row(&self, start: usize) -> Option<usize> {
        let vpos = self.state.cursor_y;
        if self.point < start || !self.in_range(start) || vpos >= self.height {
            return None;
        }
        let top = self.rows.top_row(start);
        let (row, moved) = self.rows.forward(top, vpos);
        if moved != vpos {
            return None;
        }
        let prev = (vpos > 0).then(|| self.rows.forward(top, vpos - 1).0);
        let row_end = self
            .rows
            .string_row_end(row, prev)
            .or_else(|| self.rows.next(row).map(|next| self.rows.start_pos(next)))
            .unwrap_or(self.zv);
        (self.rows.start_pos(row) <= self.point && self.point <= row_end).then_some(vpos)
    }

    /// The iterator walks STEP.
    fn walk(&self, step: FetchStep) {
        self.steps.borrow_mut().push(step);
    }

    /// try_window at START: with CHECK_MARGINS, start_display at START
    /// again and partial_line_height, which the bottom margin test
    /// evaluates unless the cursor is in the top margin: point's row ROW,
    /// or where the window's last display left it when point does not
    /// show.
    fn try_window(&self, start: usize, check_margins: bool, row: Option<usize>) {
        self.walk(FetchStep::Display { start });
        if check_margins {
            self.walk(FetchStep::StartDisplay { start });
            let cursor_y = row.unwrap_or(self.state.cursor_y);
            if !(cursor_y < self.margin && start > self.begv) {
                self.walk(FetchStep::Partial { from: start });
            }
        }
    }

    fn done(&self, start: usize, cursor_row: Option<usize>, point: Option<usize>) -> Decision {
        Decision::Display(Chosen {
            start,
            top: self.rows.top_row(start),
            cursor_row,
            point,
            start_at_line_beg: self.rows.at_line_beg(start),
            update_mode_line: self.update_mode_line.get(),
        })
    }

    /// window_start_acceptable_p.
    fn acceptable(&self, start: usize) -> bool {
        if !self.state.make_window_start_visible || start >= self.zv {
            return true;
        }
        if invisible_class_at(self.rows.buffer, self.rows.spec, start) != 0 {
            return false;
        }
        // A replacing `display' property.
        !self
            .rows
            .buffer
            .text_property_at(start, "display")
            .is_some_and(|spec| {
                crate::lisp::primitives::string_text(&spec).is_ok()
                    || super::margin_display_spec(&spec).is_some()
                    || spec.cons_values().is_some_and(|(head, _)| {
                        head.is_key(crate::lisp::cached_symbol!("space"))
                            || head.is_key(crate::lisp::cached_symbol!("image"))
                    })
            })
    }

    /// The force_start block, after the scroll functions ran.
    fn forced(&self, start: usize) -> Decision {
        // `update_mode_line = true; w->update_mode_line = true'.
        self.update_mode_line.set(true);
        let start = start.clamp(self.begv, self.zv);
        if !self.acceptable(start) {
            return self.ignore_start(start);
        }
        // try_window (window, startp, 0).
        self.try_window(start, false, None);
        match self.visible_row(start) {
            None => {
                // Point does not appear: move it to the row GNU's
                // new_vpos (half the window) reaches, row bottoms
                // counting the tab and header rows above the text.
                let target = (self.height / 2).saturating_sub(self.state.top_rows + 1);
                let (row, moved) = self.rows.forward(self.rows.top_row(start), target);
                self.done(start, Some(moved), Some(self.rows.start_pos(row)))
            }
            Some(row) => {
                // Some people insist on not letting point enter the scroll
                // margin, even in a window that did not scroll.
                if row < self.margin || row + self.margin >= self.height {
                    self.try_to_scroll(start)
                } else {
                    self.done(start, Some(row), None)
                }
            }
        }
    }

    /// From ignore_start on: the window's own start, if it still serves.
    fn ignore_start(&self, start: usize) -> Decision {
        let state = self.state;
        if state.cursor_unmoved
            && self.point >= start
            && let Some(row) = self
                .unmoved_cursor_row(start)
                .or_else(|| self.visible_row(start))
        {
            // try_cursor_movement: the current matrix, no iterator.
            return self.done(start, Some(row), None);
        }
        if state.matrix_current && self.point >= start {
            // try_cursor_movement over the current matrix: point's row,
            // or, past the rows or in a scroll margin, the scroll it needs
            // (CURSOR_MOVEMENT_MUST_SCROLL), with no iterator walking the
            // window.
            // Point moved forward scrolls in the bottom margin, back in
            // either margin (the top one not from BEGV); unmoved it stays
            // where it is.
            let last_point = state.last_point.unwrap_or(self.point);
            return match self.visible_row(start) {
                Some(row)
                    if self.point == last_point
                        || (row + self.margin < self.height
                            && !(self.point < last_point
                                && row < self.margin
                                && start > self.begv)) =>
                {
                    self.done(start, Some(row), None)
                }
                _ => self.try_to_scroll(start),
            };
        }
        if state.start_at_line_beg
            && ((start > self.begv && !self.rows.char_before_is_newline(start))
                || (self.in_range(start) && !self.acceptable(start)))
        {
            return self.recenter(start);
        }
        if self.in_range(start)
            && self.point >= start
            && (start < self.zv || start == self.begv || !state.window_outdated)
        {
            // A first window line continuing a line the first change
            // comes before, the start inside the changed text: a new
            // start in that line, forced when point shows from it.
            let (first_changed, last_changed) = state.changed_region;
            if !state.start_at_line_beg
                && !state.track_mouse
                && start > self.begv
                && start as i64 > first_changed
                && start as i64 <= last_changed
                && let Some(new_start) = self.continuation_line_start(start, self.point)
                && {
                    // pos_visible_p of PT from the start just set.
                    self.walk(FetchStep::PosVisible {
                        start: new_start,
                        pos: self.point,
                    });
                    self.visible_row(new_start).is_some()
                }
            {
                if state.scroll_functions {
                    return Decision::Hook {
                        stage: Stage::ForceStart,
                        start: new_start,
                        update_mode_line: true,
                    };
                }
                return self.forced(new_start);
            }
            if !self.acceptable(start) {
                return self.recenter(start);
            }
            // try_window with CHECK_MARGINS: it displays the window's
            // rows before finding point outside them or in a margin.
            let row = self.visible_row(start);
            self.try_window(start, true, row);
            if let Some(row) = row
                && !(row < self.margin && start > self.begv)
                && row + self.margin < self.height
            {
                return self.done(start, Some(row), None);
            }
        }
        self.try_to_scroll(start)
    }

    fn try_to_scroll(&self, start: usize) -> Decision {
        // try_to_scroll: redisplay the mode line.
        self.update_mode_line.set(true);
        let scroll = &self.state.scroll;
        if (scroll.conservatively > 0
            || scroll.step > 0
            || scroll.up_aggressively.is_some()
            || scroll.down_aggressively.is_some())
            && self.in_range(start)
            && let Some(new_start) = self.try_scrolling(start)
        {
            if self.state.scroll_functions {
                return Decision::Hook {
                    stage: Stage::Scrolled,
                    start: new_start,
                    update_mode_line: true,
                };
            }
            self.walk(FetchStep::SetStart { start: new_start });
            return self.scrolled(new_start, start);
        }
        self.recenter(start)
    }

    /// try_scrolling's display at its new start; failing that, recenter
    /// from the window's original start.
    fn scrolled(&self, new_start: usize, start: usize) -> Decision {
        self.try_window(new_start, false, None);
        match self.visible_row(new_start) {
            Some(row) => self.done(new_start, Some(row), None),
            None => self.recenter(start),
        }
    }

    /// try_scrolling's new start, or None when scrolling cannot bring
    /// point into view within the allowed distance.
    fn try_scrolling(&self, start: usize) -> Option<usize> {
        let scroll = &self.state.scroll;
        let height = self.height as i64;
        let margin = self.margin as i64;
        let mut conservatively = scroll.conservatively;
        let scroll_max: i64 = if conservatively > SCROLL_LIMIT {
            conservatively = SCROLL_LIMIT + 1;
            SCROLL_LIMIT
        } else if scroll.step > 0 || conservatively > 0 {
            scroll.step.max(conservatively).clamp(0, 1_000_000)
        } else if scroll.up_aggressively.is_some() || scroll.down_aggressively.is_some() {
            10
        } else {
            0
        };
        let aggressive = |fraction: Option<f64>, dy: i64| -> i64 {
            let Some(fraction) = fraction else {
                return 0;
            };
            let float_amount = fraction * height as f64;
            let mut amount = float_amount as i64;
            if amount == 0 && float_amount > 0.0 {
                amount = 1;
            }
            if amount + 2 * margin > height {
                amount = height - 2 * margin;
            }
            dy + amount
        };
        let start_row = self.rows.top_row(start);
        let mut dy = 0i64;
        let mut scroll_down = false;
        if self.point > start {
            // start_display at the start, partial_line_height, then
            // move_it_to the row holding the bottom margin's top or PT,
            // whichever comes first.
            self.walk(FetchStep::StartDisplay { start });
            self.walk(FetchStep::Partial { from: start });
            let stop = (height - margin - 1).max(0) as usize;
            let (stop_row, reached) = self.rows.forward(start_row, stop);
            let walk_to = |rows: usize| {
                self.walk(FetchStep::MoveTo {
                    from: start,
                    pos: self.point,
                    rows,
                })
            };
            if reached == stop && self.point > self.rows.start_pos(stop_row) {
                let slack = scroll_max.max(10) as usize;
                let limit = self.height + slack;
                // On to PT, no lower than the slack below the window.
                walk_to(limit);
                let point_row = self.rows.row_in(start_row, self.point);
                let to_point = match self.rows.distance(start_row, point_row, limit) {
                    Some(rows) => rows,
                    None => limit,
                };
                dy = to_point as i64 - stop as i64;
                if dy > scroll_max {
                    return None;
                }
                if dy > 0 {
                    scroll_down = true;
                }
            } else {
                walk_to(stop);
            }
        }
        let new_start = if scroll_down {
            let amount = if conservatively != 0 {
                dy.max(1).min(conservatively)
            } else if scroll.step > 0 {
                scroll_max
            } else {
                aggressive(scroll.up_aggressively, dy)
            };
            if amount <= 0 {
                return None;
            }
            // start_display at the start, move_it_vertically AMOUNT, or
            // past SCROLL_LIMIT move_it_by_lines until AMOUNT is reached.
            self.walk(FetchStep::StartDisplay { start });
            if scroll.conservatively <= SCROLL_LIMIT {
                self.move_vertically(start, amount as usize);
            } else {
                self.walk(FetchStep::Lines {
                    from: start,
                    rows: amount as usize,
                });
            }
            let (mut row, _) = self.rows.forward(start_row, amount as usize);
            if self.rows.start_pos(row) == start
                && let Some(next) = self.rows.next(row)
            {
                row = next;
            }
            self.rows.start_pos(row)
        } else {
            // Is point inside the top scroll margin, or above the window?
            let (scroll_margin_pos, y_offset) = if self.margin > 0 {
                // start_display at the start, move_it_vertically the
                // margin; stopped at ZV short of it, the margin goes on
                // past ZV.
                self.walk(FetchStep::StartDisplay { start });
                let (pos, moved) = self.move_vertically(start, self.margin);
                let offset = if pos == self.zv {
                    (self.margin - moved) as i64
                } else {
                    0
                };
                (pos, offset)
            } else {
                (start, 0)
            };
            if self.point >= scroll_margin_pos {
                return Some(start);
            }
            let y_to_move = (self.state.top_rows + self.height).max(scroll_max.max(10) as usize);
            let allowed = y_to_move.saturating_sub(self.state.top_rows);
            // start_display at PT, then move_it_to the scroll margin's
            // position: the rows of a display starting at point.
            let from = self.rows.top_row(self.point);
            let target = self.rows.row_in(from, scroll_margin_pos);
            self.walk(FetchStep::StartDisplay { start: self.point });
            self.walk(FetchStep::MoveTo {
                from: self.point,
                pos: scroll_margin_pos,
                rows: allowed,
            });
            let rows_up = self.rows.distance(from, target, allowed)? as i64;
            if rows_up > scroll_max {
                return None;
            }
            let dy = rows_up + y_offset;
            // start_display at the start before the amount is known.
            self.walk(FetchStep::StartDisplay { start });
            let amount = if conservatively != 0 {
                dy.max(1).min(conservatively)
            } else if scroll.step > 0 {
                scroll_max
            } else {
                aggressive(scroll.down_aggressively, dy)
            };
            if amount <= 0 {
                return None;
            }
            // move_it_vertically_backward from it.
            self.walk(FetchStep::Back {
                pos: start,
                rows: amount as usize,
            });
            self.rows
                .start_pos(self.rows.backward(start_row, amount as usize))
        };
        Some(new_start)
    }

    /// move_it_vertically ROWS rows down from start_display at FROM, its
    /// walks recorded: where the iterator stops and the rows it moved.
    /// Run out at ZV short of the rows, move_it_by_lines 0 takes it back
    /// to its screen line's start when no newline comes before ZV.
    fn move_vertically(&self, from: usize, rows: usize) -> (usize, usize) {
        self.walk(FetchStep::MoveTo {
            from,
            pos: self.zv,
            rows,
        });
        let (row, moved) = self.rows.forward(self.rows.top_row(from), rows);
        if moved == rows {
            return (self.rows.start_pos(row), moved);
        }
        if self.zv > self.begv && !self.rows.char_before_is_newline(self.zv) {
            self.walk(FetchStep::ByLines {
                from: self.zv,
                dvpos: 0,
            });
            let zv_row = self.rows.row_of(self.zv);
            return (self.rows.start_pos(zv_row), moved);
        }
        (self.zv, moved)
    }

    /// The recenter label: a start placing point where the user's
    /// scrolling options want it.
    fn recenter(&self, start: usize) -> Decision {
        let scroll = &self.state.scroll;
        let height = self.height as i64;
        let mut margin = self.margin as i64;
        // init_iterator at PT.
        self.walk(FetchStep::Init { pos: self.point });
        let margin_pos = if margin > 0 && self.in_range(start) {
            // A copy start_display'ed at the start moves the margin down.
            self.walk(FetchStep::StartDisplay { start });
            self.move_vertically(start, self.margin).0
        } else {
            start
        };
        let scrolling_up = self.point > margin_pos;
        let aggressive = if scrolling_up {
            scroll.up_aggressively
        } else {
            scroll.down_aggressively
        };
        let centering = if scroll.conservatively > SCROLL_LIMIT || aggressive.is_some() {
            let mut pt_offset = 0i64;
            if scroll.conservatively == 0
                && let Some(fraction) = aggressive
            {
                pt_offset = (fraction * height as f64) as i64;
                if pt_offset == 0 && fraction > 0.0 {
                    pt_offset = 1;
                }
                if pt_offset != 0 && margin > 0 {
                    margin -= 1;
                }
            }
            if scrolling_up {
                (height - pt_offset - (1 + margin)).max(margin)
            } else {
                margin + pt_offset
            }
        } else {
            height / 2
        };
        // move_it_vertically_backward from PT, its iterator's y at the
        // window's last visible y.
        let centering = centering.max(0) as usize;
        self.walk(FetchStep::RecenterBack {
            pos: self.point,
            rows: centering,
        });
        let mut row = self.point_row;
        let mut moved = 0;
        while moved < centering
            && let Some(prev) = self.rows.prev(row)
        {
            row = prev;
            moved += 1;
        }
        let mut new_start = self.rows.start_pos(row);
        // Very long truncated lines go back by lines, leaving the
        // iterator's y where init_iterator put it.
        let long_truncated = self.rows.truncate && self.rows.buffer.long_line_optimizations_p();
        if !long_truncated && moved >= self.height + self.state.top_rows {
            // Back over more than the window: display from the start of
            // point's screen line (init_iterator at PT,
            // move_it_vertically_backward 0).
            self.walk(FetchStep::Init { pos: self.point });
            self.walk(FetchStep::Back {
                pos: self.point,
                rows: 0,
            });
            new_start = self.rows.start_pos(self.point_row);
        }
        if self.state.scroll_functions {
            return Decision::Hook {
                stage: Stage::Recentered {
                    iterator: new_start,
                },
                start: new_start,
                update_mode_line: self.update_mode_line.get(),
            };
        }
        self.walk(FetchStep::SetStart { start: new_start });
        self.recentered(new_start, new_start)
    }

    /// The recenter label's try_window at START, the start the scroll
    /// functions left; its iterator stays at ITERATOR.  When point does
    /// not show it tries once more: from the screen line before the
    /// iterator's when point is above it, or under `scroll-conservatively'
    /// past SCROLL_LIMIT from the next screen line that moves past a
    /// string or overlay the iterator stands at.
    fn recentered(&self, start: usize, iterator: usize) -> Decision {
        let start = start.clamp(self.begv, self.zv);
        self.try_window(start, false, None);
        if let Some(row) = self.visible_row(start) {
            return self.done(start, Some(row), None);
        }
        let iterator = iterator.clamp(self.begv, self.zv);
        self.walk(FetchStep::Continue { at: iterator });
        let retry = if self.point < iterator {
            self.walk(FetchStep::ByLines {
                from: iterator,
                dvpos: -1,
            });
            let row = self.rows.row_of(iterator);
            Some(self.rows.start_pos(self.rows.backward(row, 1)))
        } else if self.state.scroll.conservatively > SCROLL_LIMIT
            && iterator < self.zv
            && (crate::lisp::primitives::move_it::reseat(
                self.rows.buffer,
                self.rows.spec,
                iterator,
            )
            .string
            .is_some()
                || self.rows.overlay_touches(iterator))
        {
            let mut row = self.rows.row_of(iterator);
            let mut lines = 0;
            loop {
                lines += 1;
                match self.rows.next(row) {
                    Some(next) => row = next,
                    None => {
                        row = self.rows.row_of(self.zv);
                        break;
                    }
                }
                if self.rows.start_pos(row) != iterator {
                    break;
                }
            }
            self.walk(FetchStep::ByLines {
                from: iterator,
                dvpos: lines,
            });
            Some(self.rows.start_pos(row))
        } else {
            None
        };
        match retry {
            Some(retry) => {
                let retry = retry.clamp(self.begv, self.zv);
                self.try_window(retry, false, None);
                let row = self.visible_row(retry);
                self.done(retry, row, None)
            }
            // Point stays out of the window: the cursor goes to the first
            // row.
            None => self.done(start, None, None),
        }
    }

    /// compute_window_start_on_continuation_line: a start inside a
    /// continued line moves to the row start nearest it, not after the
    /// buffer's point.
    fn continuation_line_start(&self, start: usize, pt: usize) -> Option<usize> {
        let rows = self.rows;
        if start <= self.begv || rows.char_before_is_newline(start) {
            return None;
        }
        let start = start.clamp(self.begv, self.zv);
        let mut row = Row {
            line: rows.row_of(start).line,
            ..Row::default()
        };
        // reseat_at_previous_visible_line_start: past what handle_stop
        // skips there, so a start move_it_by_lines reseated stays.
        let visible_line_start = rows.start_pos(row);
        let line_start =
            crate::lisp::primitives::move_it::reseat(rows.buffer, rows.spec, visible_line_start)
                .pos;
        let mut walked = 0;
        let walk = |walked: usize| {
            self.walk(FetchStep::ContinuationLine {
                start,
                line_start: visible_line_start,
                rows: walked,
            })
        };
        if line_start > pt || start.saturating_sub(line_start) >= self.state.total_cells {
            walk(walked);
            return None;
        }
        let mut it = line_start;
        let mut pos = line_start;
        let mut pos_before_pt = line_start;
        let mut min_distance = usize::MAX;
        loop {
            let distance = start.abs_diff(it);
            if distance >= min_distance {
                break;
            }
            min_distance = distance;
            if pos <= pt {
                pos_before_pt = pos;
            }
            pos = it;
            // move_it_by_lines (&it, 1).
            walked += 1;
            match rows.next(row) {
                Some(next) => {
                    row = next;
                    it = rows.start_pos(next);
                }
                None => it = self.zv,
            }
        }
        walk(walked);
        let new_start = if pos > pt { pos_before_pt } else { pos };
        // SET_MARKER_FROM_TEXT_POS (w->start, pos).
        self.walk(FetchStep::SetStart { start: new_start });
        Some(new_start)
    }
}

/// The string display INDEX of VISUAL comes from, as an index into its
/// objects.
fn visual_object(visual: &VisualLine, index: usize) -> Option<usize> {
    visual
        .object_of
        .get(index)
        .copied()
        .flatten()
        .map(|(object, _)| object)
}
