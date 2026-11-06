//! editfns.c's fields: stretches of text with the same `field'
//! char-property, found from a position by the char-property scans.

use super::*;
use crate::lisp::types::PropertyName;

fn field_name() -> PropertyName {
    PropertyName::new(Value::Symbol(cached_symbol!("field").with(Clone::clone)))
}

fn eq(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    values_eq_in_env(interp, left, right, &Env::new())
}

/// editfns.c:find_field: the field around POS (nil point) -- its
/// beginning when WANT_BEG, its end when WANT_END, each by the
/// char-property scans bounded by BEG_LIMIT and END_LIMIT.  With
/// MERGE_AT_BOUNDARY nil, POS at the edge of a field belongs to the field
/// a character inserted there would join (Fget_pos_property), unless that
/// would be nil between two non-nil fields; with it non-nil, a `boundary'
/// field next to POS is skipped over.
pub(crate) fn find_field(
    interp: &mut Interpreter,
    pos: &Value,
    merge_at_boundary: bool,
    beg_limit: Value,
    want_beg: bool,
    end_limit: Value,
    want_end: bool,
) -> Result<(i64, i64), LispError> {
    let pos = if pos.is_nil() {
        interp.buffer.borrow().point() as i64
    } else {
        fixnum_coerce_marker(interp, pos)?
    };
    let field = field_name();
    let after_field = get_char_property(interp, &Value::Integer(pos), field, Value::Nil)?;
    let begv = interp.buffer.borrow().point_min() as i64;
    // Using nil before the start would fail when the buffer starts with
    // a non-sticky field.
    let before_field = if pos > begv {
        get_char_property(interp, &Value::Integer(pos - 1), field, Value::Nil)?
    } else {
        after_field
    };
    let (mut at_field_start, mut at_field_end) = (false, false);
    if !merge_at_boundary {
        let inherited = get_pos_property(interp, &Value::Integer(pos), field, Value::Nil)?;
        at_field_end = !eq(interp, &inherited, &after_field);
        at_field_start = !eq(interp, &inherited, &before_field);
        // A nil field between non-nil ones is no zero-length field but
        // text not meant for editing, such as comint's prompts.
        if inherited.is_nil() && at_field_start && at_field_end {
            at_field_start = false;
            at_field_end = false;
        }
    }
    let boundary = Value::symbol("boundary");
    let field_symbol = field.value();
    let mut beg = 0;
    if want_beg {
        beg = if at_field_start {
            pos
        } else {
            let mut p = Value::Integer(pos);
            if merge_at_boundary && eq(interp, &before_field, &boundary) {
                p = previous_single_char_property_change(
                    interp,
                    &p,
                    field_symbol,
                    Value::Nil,
                    beg_limit,
                )?;
            }
            p = previous_single_char_property_change(
                interp,
                &p,
                field_symbol,
                Value::Nil,
                beg_limit,
            )?;
            if p.is_nil() {
                interp.buffer.borrow().point_min() as i64
            } else {
                p.as_integer()?
            }
        };
    }
    let mut end = 0;
    if want_end {
        end = if at_field_end {
            pos
        } else {
            let mut p = Value::Integer(pos);
            if merge_at_boundary && eq(interp, &after_field, &boundary) {
                p = next_single_char_property_change(
                    interp,
                    &p,
                    field_symbol,
                    Value::Nil,
                    end_limit,
                )?;
            }
            p = next_single_char_property_change(interp, &p, field_symbol, Value::Nil, end_limit)?;
            if p.is_nil() {
                interp.buffer.borrow().point_max() as i64
            } else {
                p.as_integer()?
            }
        };
    }
    Ok((beg, end))
}

/// editfns.c:Ffield_beginning.
pub(crate) fn field_beginning(
    interp: &mut Interpreter,
    pos: &Value,
    escape_from_edge: bool,
    limit: Value,
) -> Result<i64, LispError> {
    find_field(
        interp,
        pos,
        escape_from_edge,
        limit,
        true,
        Value::Nil,
        false,
    )
    .map(|(beg, _)| beg)
}

/// editfns.c:Ffield_end.
pub(crate) fn field_end(
    interp: &mut Interpreter,
    pos: &Value,
    escape_from_edge: bool,
    limit: Value,
) -> Result<i64, LispError> {
    find_field(
        interp,
        pos,
        escape_from_edge,
        Value::Nil,
        false,
        limit,
        true,
    )
    .map(|(_, end)| end)
}

/// editfns.c:Fconstrain_to_field: the position closest to NEW_POS (nil
/// point, then moved there) in OLD_POS's field, when either position or
/// the character before it has a `field' char-property, unless
/// `inhibit-field-text-motion' or OLD_POS's INHIBIT_CAPTURE_PROPERTY; a
/// bound past NEW_POS (ESCAPE_FROM_EDGE's other side) leaves it, and with
/// ONLY_IN_LINE so does a bound across a newline.
pub(crate) fn constrain_to_field(
    interp: &mut Interpreter,
    new_pos: &Value,
    old_pos: &Value,
    escape_from_edge: bool,
    only_in_line: bool,
    inhibit_capture_property: Value,
) -> Result<i64, LispError> {
    let mut orig_point = None;
    let mut new_pos = if new_pos.is_nil() {
        let point = interp.buffer.borrow().point() as i64;
        orig_point = Some(point);
        point
    } else {
        fixnum_coerce_marker(interp, new_pos)?
    };
    let old_pos = fixnum_coerce_marker(interp, old_pos)?;
    let fwd = new_pos > old_pos;
    let field = field_name();
    let begv = interp.buffer.borrow().point_min() as i64;
    let has = |interp: &Interpreter, pos: i64, prop: PropertyName| {
        get_char_property(interp, &Value::Integer(pos), prop, Value::Nil).map(|v| !v.is_nil())
    };
    let inhibit_motion = interp
        .symbol_value_key(cached_symbol!("inhibit-field-text-motion"))
        .is_some_and(|value| !value.is_nil());
    let near = !inhibit_motion
        && new_pos != old_pos
        && (has(interp, new_pos, field)?
            || has(interp, old_pos, field)?
            || (new_pos > begv && has(interp, new_pos - 1, field)?)
            || (old_pos > begv && has(interp, old_pos - 1, field)?));
    let captured = || -> Result<bool, LispError> {
        if inhibit_capture_property.is_nil() {
            return Ok(false);
        }
        let prop = PropertyName::new(inhibit_capture_property);
        Ok(
            !(get_pos_property(interp, &Value::Integer(old_pos), prop, Value::Nil)?.is_nil()
                && (old_pos <= begv
                    || !has(interp, old_pos, prop)?
                    || !has(interp, old_pos - 1, prop)?)),
        )
    };
    if near && !captured()? {
        let field_bound = if fwd {
            field_end(
                interp,
                &Value::Integer(old_pos),
                escape_from_edge,
                Value::Integer(new_pos),
            )?
        } else {
            field_beginning(
                interp,
                &Value::Integer(old_pos),
                escape_from_edge,
                Value::Integer(new_pos),
            )?
        };
        let other_side = if field_bound < new_pos { fwd } else { !fwd };
        let same_line = || {
            let (low, high) = (field_bound.min(new_pos), field_bound.max(new_pos));
            let buffer = interp.buffer.borrow();
            !(low.max(1)..high).any(|pos| buffer.char_at(pos as usize) == Some('\n'))
        };
        if other_side && (!only_in_line || same_line()) {
            new_pos = field_bound;
        }
        if let Some(point) = orig_point
            && new_pos != point
        {
            interp.buffer.borrow_mut().goto_char(new_pos as usize);
        }
    }
    Ok(new_pos)
}
