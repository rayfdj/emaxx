//! xdisp.c redisplay_window's choice of where a window's display starts:
//! a forced start (set-window-start, the scroll commands) kept with point
//! moved into view, an optional one (recenter) kept when point is visible
//! from it, the same start kept while point stays visible outside the
//! scroll margins, try_scrolling's scroll-conservatively, scroll-step and
//! scroll-*-aggressively, and recentering.  Every window gets the same
//! treatment; a terminal row is one line high, so GNU's pixel arithmetic
//! is row arithmetic here.

use super::{
    CharacterDisplayContext, glyphless_visual_line_at, layout, position_of_visual_row,
    wrap_glyphless_visual_line, wrapped_position_of_column,
};
use crate::buffer::Buffer;
use crate::lisp::primitives::{InvisibilitySpec, invisible_class_at, visual_line_first_line};
use std::cell::RefCell;
use std::collections::HashMap;

/// One glyph row: segment SEG of the display line that begins at raw
/// line LINE.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Row {
    pub(super) line: usize,
    pub(super) seg: usize,
}

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
    /// Per display line: its rows, the raw lines it spans, and whether it
    /// is a wholly invisible tail that produces no row.
    lines: RefCell<HashMap<usize, (usize, usize, bool)>>,
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
        }
    }

    fn line_info(&self, line: usize) -> (usize, usize, bool) {
        if let Some(info) = self.lines.borrow().get(&line) {
            return *info;
        }
        let visual = glyphless_visual_line_at(self.buffer, self.spec, line, self.glyphless);
        let line_start = self.buffer.display_line_start_of(line);
        let point_max = self.buffer.point_max();
        let invisible_tail = visual.text.is_empty()
            && line_start < point_max
            && invisible_class_at(self.buffer, self.spec, line_start) != 0
            && visual.raw_of_display.last().copied() == Some(point_max.saturating_sub(line_start));
        let segs = if self.truncate {
            1
        } else {
            wrap_glyphless_visual_line(&visual, self.body_width)
                .len()
                .max(1)
        };
        let info = (segs, visual.lines_spanned.max(1), invisible_tail);
        self.lines.borrow_mut().insert(line, info);
        info
    }

    fn produces_row(&self, line: usize) -> bool {
        line <= self.last_line && !self.line_info(line).2
    }

    pub(super) fn next(&self, row: Row) -> Option<Row> {
        let (segs, spanned, _) = self.line_info(row.line);
        if row.seg + 1 < segs {
            return Some(Row {
                line: row.line,
                seg: row.seg + 1,
            });
        }
        let line = row.line + spanned;
        self.produces_row(line).then_some(Row { line, seg: 0 })
    }

    pub(super) fn prev(&self, row: Row) -> Option<Row> {
        if row.seg > 0 {
            return Some(Row {
                line: row.line,
                seg: row.seg - 1,
            });
        }
        if row.line <= self.first_line {
            return None;
        }
        let line =
            visual_line_first_line(self.buffer, self.spec, row.line - 1).max(self.first_line);
        let (segs, _, _) = self.line_info(line);
        Some(Row {
            line,
            seg: segs - 1,
        })
    }

    /// MATRIX_ROW_START_CHARPOS of ROW.
    pub(super) fn start_pos(&self, row: Row) -> usize {
        position_of_visual_row(
            self.buffer,
            self.spec,
            self.glyphless,
            row.line,
            row.seg,
            self.body_width.saturating_sub(1).max(1),
        )
        .max(self.buffer.point_min())
    }

    /// The row showing POS.
    pub(super) fn row_of(&self, pos: usize) -> Row {
        let buffer = self.buffer;
        let pos = pos.clamp(buffer.point_min(), buffer.point_max());
        let line = visual_line_first_line(buffer, self.spec, buffer.line_number_at_pos(pos))
            .max(self.first_line);
        if self.truncate {
            return Row { line, seg: 0 };
        }
        let visual = glyphless_visual_line_at(buffer, self.spec, line, self.glyphless);
        let offset = pos.saturating_sub(buffer.display_line_start_of(line));
        let index = visual
            .map
            .get(offset)
            .copied()
            .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0));
        let laid = layout::lay_out_visual_line(&visual, self.body_width, false);
        let column = laid.columns[index.min(laid.columns.len() - 1)];
        let (seg, _) = wrapped_position_of_column(
            &laid.rows,
            column,
            visual
                .glyphless_spans
                .iter()
                .any(|(from, _)| *from == index),
        );
        Row { line, seg }
    }

    /// Rows from FROM down to TO, when TO lies within LIMIT rows.
    pub(super) fn distance(&self, from: Row, to: Row, limit: usize) -> Option<usize> {
        if to < from {
            return None;
        }
        if to.line == from.line {
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

    fn char_before_is_newline(&self, pos: usize) -> bool {
        pos > 1 && self.buffer.char_at(pos - 1) == Some('\n')
    }

    fn at_line_beg(&self, pos: usize) -> bool {
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
    Recentered,
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
    /// `window-scroll-functions' is non-nil in the window's buffer.
    pub(super) scroll_functions: bool,
    pub(super) make_window_start_visible: bool,
    /// WINDOW_TOTAL_LINES times WINDOW_TOTAL_COLS.
    pub(super) total_cells: usize,
    /// Tab and header rows above the text (their y precedes the text's).
    pub(super) top_rows: usize,
    pub(super) scroll: ScrollParams,
    /// Resume at a stage with the start the scroll functions left.
    pub(super) resume: Option<(Stage, usize)>,
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
}

pub(super) enum Decision {
    /// Run `window-scroll-functions' with START as the window's start,
    /// then resume at STAGE with the start they leave.
    Hook {
        stage: Stage,
        start: usize,
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
}

const SCROLL_LIMIT: i64 = 100;

/// redisplay_window's decision for one window of TEXT_ROWS rows.
pub(super) fn decide(rows: &Rows, state: &StartState, text_rows: usize) -> (Decision, Consumed) {
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
    };
    let mut consumed = Consumed::default();
    if let Some((stage, start)) = state.resume {
        let decision = match stage {
            Stage::ForceStart => planner.forced(start),
            Stage::Scrolled => planner.scrolled(start, state.start),
            Stage::Recentered => planner.recentered(start),
        };
        return (decision, consumed);
    }
    let mut start = state.start;
    if state.windows_or_buffers_changed {
        start = planner.continuation_line_start(start);
    }
    let mut force = state.force_start;
    if state.optional_new_start && planner.in_range(start) {
        consumed.optional_new_start = true;
        if !force && planner.visible_row(start).is_some() {
            force = true;
        }
    }
    if force {
        consumed.force_start = true;
        if state.scroll_functions {
            return (
                Decision::Hook {
                    stage: Stage::ForceStart,
                    start,
                },
                consumed,
            );
        }
        return (planner.forced(start), consumed);
    }
    (planner.ignore_start(start), consumed)
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
        let top = self.rows.row_of(start);
        self.rows
            .distance(top, self.point_row, self.height)
            .filter(|row| *row < self.height)
    }

    fn done(&self, start: usize, cursor_row: Option<usize>, point: Option<usize>) -> Decision {
        Decision::Display(Chosen {
            start,
            top: self.rows.row_of(start),
            cursor_row,
            point,
            start_at_line_beg: self.rows.at_line_beg(start),
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
                        matches!(head.symbol_str(), Some("space" | "image"))
                    })
            })
    }

    /// The force_start block, after the scroll functions ran.
    fn forced(&self, start: usize) -> Decision {
        let start = start.clamp(self.begv, self.zv);
        if !self.acceptable(start) {
            return self.ignore_start(start);
        }
        match self.visible_row(start) {
            None => {
                // Point does not appear: move it to the row GNU's
                // new_vpos (half the window) reaches, row bottoms
                // counting the tab and header rows above the text.
                let target = (self.height / 2).saturating_sub(self.state.top_rows + 1);
                let (row, moved) = self.rows.forward(self.rows.row_of(start), target);
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
            && let Some(row) = self.visible_row(start)
        {
            return self.done(start, Some(row), None);
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
            if !self.acceptable(start) {
                return self.recenter(start);
            }
            // try_window with CHECK_MARGINS.
            if let Some(row) = self.visible_row(start)
                && !(row < self.margin && start > self.begv)
                && row + self.margin < self.height
            {
                return self.done(start, Some(row), None);
            }
        }
        self.try_to_scroll(start)
    }

    fn try_to_scroll(&self, start: usize) -> Decision {
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
                };
            }
            return self.scrolled(new_start, start);
        }
        self.recenter(start)
    }

    /// try_scrolling's display at its new start; failing that, recenter
    /// from the window's original start.
    fn scrolled(&self, new_start: usize, start: usize) -> Decision {
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
        let start_row = self.rows.row_of(start);
        let mut dy = 0i64;
        let mut scroll_down = false;
        if self.point > start {
            // move_it_to the row holding the bottom margin's top or PT,
            // whichever comes first.
            let stop = (height - margin - 1).max(0) as usize;
            let (stop_row, reached) = self.rows.forward(start_row, stop);
            if reached == stop && self.point > self.rows.start_pos(stop_row) {
                let slack = scroll_max.max(10) as usize;
                let limit = self.height + slack;
                let to_point = match self.rows.distance(start_row, self.point_row, limit) {
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
            let (mut row, _) = self.rows.forward(start_row, amount as usize);
            if self.rows.start_pos(row) == start
                && let Some(next) = self.rows.next(row)
            {
                row = next;
            }
            self.rows.start_pos(row)
        } else {
            // Is point inside the top scroll margin, or above the window?
            let (margin_row, moved) = self.rows.forward(start_row, self.margin);
            let (scroll_margin_pos, y_offset) = if self.margin > 0 {
                let pos = if moved < self.margin && self.rows.next(margin_row).is_none() {
                    self.zv
                } else {
                    self.rows.start_pos(margin_row)
                };
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
            let target = if scroll_margin_pos == self.zv {
                self.rows.row_of(self.zv)
            } else {
                margin_row
            };
            let rows_up = self.rows.distance(self.point_row, target, allowed)? as i64;
            if rows_up > scroll_max {
                return None;
            }
            let dy = rows_up + y_offset;
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
            self.rows
                .start_pos(self.rows.backward(start_row, amount as usize))
        };
        Some(new_start)
    }

    /// The recenter label: a start placing point where the user's
    /// scrolling options want it.
    fn recenter(&self, start: usize) -> Decision {
        let scroll = &self.state.scroll;
        let height = self.height as i64;
        let mut margin = self.margin as i64;
        let margin_pos = if margin > 0 && self.in_range(start) {
            let (row, moved) = self.rows.forward(self.rows.row_of(start), self.margin);
            if moved < self.margin && self.rows.next(row).is_none() {
                self.zv
            } else {
                self.rows.start_pos(row)
            }
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
        let row = self
            .rows
            .backward(self.point_row, centering.max(0) as usize);
        let new_start = self.rows.start_pos(row);
        if self.state.scroll_functions {
            return Decision::Hook {
                stage: Stage::Recentered,
                start: new_start,
            };
        }
        self.recentered(new_start)
    }

    fn recentered(&self, start: usize) -> Decision {
        let start = start.clamp(self.begv, self.zv);
        match self.visible_row(start) {
            Some(row) => self.done(start, Some(row), None),
            // Point stays out of the window the scroll functions chose: the
            // cursor goes to the first row.
            None => self.done(start, None, None),
        }
    }

    /// compute_window_start_on_continuation_line: a start inside a
    /// continued line moves to the row start nearest it, not after the
    /// buffer's point.
    fn continuation_line_start(&self, start: usize) -> usize {
        let rows = self.rows;
        if start <= self.begv || rows.char_before_is_newline(start) {
            return start;
        }
        let start = start.clamp(self.begv, self.zv);
        let pt = self.state.buffer_point;
        let mut row = Row {
            line: rows.row_of(start).line,
            seg: 0,
        };
        let line_start = rows.start_pos(row);
        if line_start > pt || start - line_start >= self.state.total_cells {
            return start;
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
            match rows.next(row) {
                Some(next) => {
                    row = next;
                    it = rows.start_pos(next);
                }
                None => it = self.zv,
            }
        }
        if pos > pt { pos_before_pt } else { pos }
    }
}
