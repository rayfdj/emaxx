//! xdisp.c display_line's row breaking on a terminal frame: where each
//! glyph row of a display line begins and ends under continuation,
//! `word-wrap' (and `word-wrap-by-category'), `line-prefix' and
//! `wrap-prefix', with TAB stops measured as term.c measures them.
//!
//! Columns live in GNU's `x + continuation_lines_width' space: a row's
//! cells start at the column where the previous row of the same line
//! ended, prefix cells included, so a TAB after a prefix stops where
//! GNU's does and a continued row's start column is the previous row's
//! end.

use super::{VisualLine, WrappedVisualSegment};

/// Category bits a display character carries for
/// `word-wrap-by-category' (kinsoku.el and characters.el).
pub(super) const LINE_BREAKABLE: u8 = 1;
pub(super) const NOT_AT_EOL: u8 = 2;
pub(super) const NOT_AT_BOL: u8 = 4;

/// How a display line continues past the window edge (it->line_wrap).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum LineWrap {
    /// Continue mid-word at the edge with the `\' glyph.
    #[default]
    Window,
    /// `word-wrap': back up to the last wrap point; BY_CATEGORY is
    /// `word-wrap-by-category'.
    Word { by_category: bool },
}

/// The `line-prefix' and `wrap-prefix' in effect from a raw offset of
/// the display line on (handle_line_prefix reads them at each row's
/// first position).  `None' means no prefix; `Some("")' is a pushed
/// zero-width prefix, which still makes the row's first glyph obligatory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct PrefixRun {
    pub(super) raw_start: usize,
    pub(super) line: Option<String>,
    pub(super) wrap: Option<String>,
}

/// A display line laid out into glyph rows.
pub(super) struct LineLayout {
    pub(super) rows: Vec<WrappedVisualSegment>,
    /// The column of each display index's first glyph (one past the end
    /// included), in the space the rows' columns are measured in.
    pub(super) columns: Vec<usize>,
}

/// One display element: display indexes [START, END) produce their
/// glyphs together, and a row edge inside them restarts the whole
/// element on the next row (glyphless acronyms, hex codes).
#[derive(Clone, Copy)]
struct Element {
    start: usize,
    end: usize,
}

fn elements_of(visual: &VisualLine, count: usize) -> Vec<Element> {
    let mut elements = Vec::with_capacity(count);
    let mut spans = visual.glyphless_spans.iter().peekable();
    let mut index = 0usize;
    while index < count {
        while spans.peek().is_some_and(|(_, to)| *to <= index) {
            spans.next();
        }
        let end = match spans.peek() {
            Some(&&(from, to)) if from == index && to > from => to.min(count),
            _ => index + 1,
        };
        elements.push(Element { start: index, end });
        index = end;
    }
    elements
}

/// Give the display indexes of ELEMENT the columns of the glyphs it
/// produces from COLUMN on, unless an earlier row already showed them
/// (an element restarted after a row edge keeps its first position).
fn record_element_columns(
    columns: &mut [usize],
    assigned: &mut [bool],
    element: Element,
    column: usize,
    nglyphs: usize,
) {
    for (offset, index) in (element.start..element.end).enumerate() {
        if !assigned[index] {
            columns[index] = column + offset.min(nglyphs.saturating_sub(1));
            assigned[index] = true;
        }
    }
}

fn prefix_at(visual: &VisualLine, index: usize, continuation: bool) -> Option<String> {
    let raw = visual
        .raw_of_display
        .get(index)
        .copied()
        .unwrap_or_else(|| visual.raw_of_display.last().copied().unwrap_or(0));
    let run = visual
        .prefixes
        .iter()
        .rev()
        .find(|run| run.raw_start <= raw)?;
    if continuation {
        run.wrap.clone()
    } else {
        run.line.clone()
    }
}

/// Lay out VISUAL in a window COLS cells wide (the terminal's last
/// column holds the continuation or truncation glyph).  TRUNCATE lays
/// the line out as one row running past the edge; the caller clips it.
pub(super) fn lay_out_visual_line(visual: &VisualLine, cols: usize, truncate: bool) -> LineLayout {
    let chars: Vec<char> = visual.text.chars().collect();
    let count = chars.len();
    let elements = elements_of(visual, count);
    let last_visible_x = cols.saturating_sub(1).max(1);
    let tab_width = visual.tab_width.max(1);
    let (word_wrap, by_category) = match visual.line_wrap {
        LineWrap::Word { by_category } if !truncate => (true, by_category),
        _ => (false, false),
    };
    let category = |index: usize, bit: u8| {
        visual
            .wrap_categories
            .get(index)
            .is_some_and(|bits| bits & bit != 0)
    };
    let whitespace = |element: &Element| {
        element.end == element.start + 1 && matches!(chars[element.start], ' ' | '\t')
    };
    // char_can_wrap_before / char_can_wrap_after (xdisp.c).
    let can_wrap_before = |element: &Element| {
        !whitespace(element) && !(by_category && category(element.start, NOT_AT_BOL))
    };
    let can_wrap_after = |element: &Element| {
        whitespace(element)
            || (by_category
                && category(element.start, LINE_BREAKABLE)
                && !category(element.start, NOT_AT_EOL))
    };

    let mut columns = vec![0usize; count + 1];
    let mut assigned = vec![false; count + 1];
    let mut rows = Vec::new();
    let mut element = 0usize;
    // Column where the current hard line (a newline in an overlay or
    // display string starts another) began, and continuation_lines_width
    // relative to it.
    let mut line_base = 0usize;
    let mut continuation_width = 0usize;
    // GNU's own continuation_lines_width: each continued row adds its
    // full x, the line-number gutter included.
    let lnum = visual.lnum_cols;
    let mut gnu_continuation = 0usize;
    loop {
        let row_start_column = line_base + continuation_width;
        let start_index = elements.get(element).map_or(count, |e| e.start);
        let prefix = prefix_at(visual, start_index, continuation_width > 0);
        let mut text = prefix.clone().unwrap_or_default();
        let prefix_cols = text.chars().count();
        // it->wrap_prefix_width: current_x past a continuation row's
        // pushed prefix, gutter included.
        let wrap_prefix_width = if continuation_width > 0 && prefix.is_some() {
            lnum + prefix_cols
        } else {
            0
        };
        // this_line_subject_to_line_prefix: the first real element after
        // a pushed prefix is produced even past the edge.
        let mut subject_to_prefix = prefix.is_some();
        let mut x = prefix_cols;
        let mut hpos = prefix_cols;
        let mut may_wrap = false;
        // (element, x, glyphs in the row) of the last wrap point.
        let mut wrap_point: Option<(usize, usize, usize)> = None;
        let mut glyph_count = prefix_cols;
        let mut continued_to: Option<usize> = None;
        let mut continued_glyph = false;
        let mut hard_newline = false;
        while let Some(current) = elements.get(element).copied() {
            if chars[current.start] == '\n' {
                if !assigned[current.start] {
                    columns[current.start] = row_start_column + x;
                    assigned[current.start] = true;
                }
                hard_newline = true;
                element += 1;
                break;
            }
            if word_wrap {
                let next_may_wrap = can_wrap_after(&current);
                if may_wrap && can_wrap_before(&current) {
                    wrap_point = Some((element, x, glyph_count));
                }
                may_wrap = next_may_wrap;
            }
            let glyphs: Vec<char> =
                if chars[current.start] == '\t' && current.end == current.start + 1 {
                    // term.c produce_glyphs: current_x plus
                    // continuation_lines_width less the wrap-prefix's
                    // width, the gutter left out, to the next multiple
                    // of tab-width.
                    let tab_x = if continuation_width > 0 {
                        (lnum + x + gnu_continuation).saturating_sub(wrap_prefix_width + lnum)
                    } else {
                        x
                    };
                    let next_tab_x = (tab_x + tab_width) / tab_width * tab_width;
                    vec![' '; next_tab_x - tab_x]
                } else {
                    chars[current.start..current.end].to_vec()
                };
            let nglyphs = glyphs.len();
            let column = row_start_column + x;
            let record_columns = |columns: &mut [usize], assigned: &mut [bool]| {
                record_element_columns(columns, assigned, current, column, nglyphs);
            };
            let new_x = x + nglyphs;
            if nglyphs == 0 {
                record_columns(&mut columns, &mut assigned);
                element += 1;
                continue;
            }
            if truncate || new_x < last_visible_x || (nglyphs == 1 && subject_to_prefix) {
                record_columns(&mut columns, &mut assigned);
                text.extend(glyphs);
                glyph_count += nglyphs;
                hpos += nglyphs;
                x = new_x;
                element += 1;
                subject_to_prefix = false;
                continue;
            }
            // Some glyph of this element may not fit: GNU's per-glyph loop.
            let mut ended = false;
            for (i, glyph) in glyphs.iter().enumerate() {
                let glyph_x = x + i;
                let glyph_new_x = glyph_x + 1;
                if glyph_new_x > last_visible_x
                    && (!subject_to_prefix || (i != 0 && glyph_x > last_visible_x))
                {
                    if hpos == 0 {
                        // The only glyph on the row: it stays, overflowing.
                        if i == 0 {
                            record_columns(&mut columns, &mut assigned);
                        }
                        text.push(*glyph);
                        hpos += 1;
                        if i + 1 == nglyphs {
                            element += 1;
                        }
                        continued_to = Some(continuation_width + glyph_new_x);
                    } else if let Some((wrap_element, wrap_x, wrap_glyphs)) = wrap_point {
                        // back_to_wrap: the row ends before the wrap point,
                        // with no continuation glyph.
                        assigned[elements[wrap_element].start..count].fill(false);
                        element = wrap_element;
                        text = text.chars().take(wrap_glyphs).collect();
                        continued_to = Some(continuation_width + wrap_x);
                    } else {
                        // The glyphs that fit stay; `\' fills the last column
                        // and the element is produced again on the next row.
                        if i > 0 {
                            record_columns(&mut columns, &mut assigned);
                        }
                        text.push('\\');
                        continued_glyph = true;
                        continued_to = Some(continuation_width + glyph_x);
                    }
                    ended = true;
                    break;
                }
                if i == 0 {
                    record_columns(&mut columns, &mut assigned);
                }
                text.push(*glyph);
                hpos += 1;
            }
            if ended {
                break;
            }
            glyph_count += nglyphs;
            x = new_x;
            element += 1;
            subject_to_prefix = false;
        }
        let end_column = match continued_to {
            Some(width) => line_base + width,
            None if hard_newline => row_start_column + x + 1,
            None => row_start_column + x,
        };
        rows.push(WrappedVisualSegment {
            text,
            start_column: row_start_column,
            end_column,
            start_index,
            prefix_cols,
            continuation: continuation_width > 0,
            continued_glyph,
        });
        match continued_to {
            Some(width) => {
                gnu_continuation += lnum + (width - continuation_width);
                continuation_width = width;
            }
            None if hard_newline && element < elements.len() => {
                line_base = end_column;
                continuation_width = 0;
                gnu_continuation = 0;
            }
            None => break,
        }
    }
    let end = rows.last().map_or(0, |row| row.end_column);
    for index in 0..=count {
        if !assigned[index] {
            columns[index] = if index == count {
                end
            } else {
                columns[index.saturating_sub(1)]
            };
        }
    }
    LineLayout { rows, columns }
}
