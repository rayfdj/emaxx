//! xdisp.c's restrictions for buffers with long lines (its "How Emacs
//! deals with long lines"): the large narrowing around point that the
//! command hooks and `fontification-functions' run in, set with
//! editfns.c's labeled_narrow_to_region.

use crate::lisp::eval::{Interpreter, UnwindAction};
use crate::lisp::types::{Env, Kind, LispError, Value};

/// A C integer variable's value in the current buffer, 0 when it holds
/// no fixnum.
fn int_variable(interp: &Interpreter, key: crate::lisp::types::symbols::SymbolKey) -> i64 {
    match interp
        .lookup_var_key(key, &Env::new())
        .map(|value| value.kind())
    {
        Some(Kind::Integer(value)) => value,
        _ => 0,
    }
}

/// long_line_optimizations_region_size.
pub(crate) fn region_size(interp: &Interpreter) -> i64 {
    int_variable(
        interp,
        cached_symbol!("long-line-optimizations-region-size"),
    )
}

/// xdisp.c:get_large_narrowing_begv: half the region size before POS,
/// moved back at most `long-line-optimizations-bol-search-limit'
/// characters to a line start.
pub(crate) fn large_narrowing_begv(interp: &Interpreter, pos: usize) -> usize {
    let buffer = interp.buffer.borrow();
    let region = region_size(interp);
    if region <= 0 {
        return buffer.point_min();
    }
    let len = (region / 2) as usize;
    let begv = buffer.point_min();
    let mut start = pos.saturating_sub(len).max(begv);
    let mut limit = int_variable(
        interp,
        cached_symbol!("long-line-optimizations-bol-search-limit"),
    );
    while limit > 0 {
        if start == begv || buffer.char_at(start - 1) == Some('\n') {
            return start;
        }
        start -= 1;
        limit -= 1;
    }
    start
}

/// xdisp.c:get_large_narrowing_zv: half the region size after POS.
pub(crate) fn large_narrowing_zv(interp: &Interpreter, pos: usize) -> usize {
    let buffer = interp.buffer.borrow();
    let region = region_size(interp);
    if region <= 0 {
        return buffer.point_max();
    }
    (pos + (region / 2) as usize).min(buffer.point_max())
}

/// xdisp.c:get_narrowed_width: twice WINDOW's body width in columns
/// (a character-only terminal uses one font size) less the column a
/// window without fringes gives its line-wrapping glyph.
fn narrowed_width(interp: &mut Interpreter, env: &mut Env, window: Value) -> i64 {
    let width = super::dispatch::call(interp, "window-body-width", &[window], env)
        .ok()
        .and_then(|value| value.integer_value())
        .unwrap_or(1);
    2 * (width - 1).max(1)
}

/// xdisp.c:get_nearby_bol_pos: the line start before POS, unless it is
/// too far away; the search widens [POS-500..POS], [POS-5500..POS-500],
/// [POS-55500..POS-5500] and [POS-555500..POS-55500], and finds BEGV-1
/// when no line starts within it.
fn nearby_bol_pos(interp: &Interpreter, pos: usize) -> i64 {
    let buffer = interp.buffer.borrow();
    let begv = buffer.point_min();
    let limit = pos.saturating_sub(555_500).max(begv);
    match buffer.line_start_within(pos, limit) {
        Some(bol) => bol as i64,
        None if limit == begv => begv as i64,
        None => begv as i64 - 1,
    }
}

/// xdisp.c:get_small_narrowing_begv: the start of the small restriction
/// around POS in WINDOW, whole multiples of its narrowed width from a
/// nearby line start.
pub(crate) fn small_narrowing_begv(
    interp: &mut Interpreter,
    env: &mut Env,
    window: Value,
    pos: usize,
) -> usize {
    let len = narrowed_width(interp, env, window);
    let begv = interp.buffer.borrow().point_min() as i64;
    let bol_pos = nearby_bol_pos(interp, pos).max(begv);
    (bol_pos + ((pos as i64 - bol_pos) / len - 1) * len).max(begv) as usize
}

/// editfns.c:labeled_narrow_to_region: point put back and the labeled
/// restriction removed at the unbind_to that ends the caller's binding,
/// then the current buffer narrowed to BEGV..ZV under LABEL.
pub(crate) fn labeled_narrow_to_region(
    interp: &mut Interpreter,
    begv: usize,
    zv: usize,
    label: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let point = crate::lisp::primitives::call(interp, "point-marker", &[], env)?;
    interp.record_unwind(UnwindAction::RestorePoint(point));
    interp.record_unwind(UnwindAction::LabeledWiden(label));
    crate::lisp::primitives::call(
        interp,
        "internal--labeled-narrow-to-region",
        &[
            Value::Integer(begv as i64),
            Value::Integer(zv as i64),
            label,
        ],
        env,
    )
    .map(drop)
}
