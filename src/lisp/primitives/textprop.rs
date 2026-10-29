//! textprop.c's searches over an object's intervals: `text-property-any',
//! `text-property-not-all' and the single-property change scans, each a
//! walk from find_interval with `textget' values compared by EQ.

use super::*;
use crate::buffer::{PropertyInterval, PropertyIntervals, PropertySpan};
use crate::lisp::types::{Kind, PropertyKey, PropertyName};

/// The object a text-property search names: a buffer (nil the current
/// one) or a string.
#[derive(Clone, Copy)]
pub(crate) enum SearchObject {
    Buffer(u64),
    String(Value),
}

/// validate_interval_range's CHECK_STRING_OR_BUFFER, nil the current
/// buffer.
pub(crate) fn search_object(
    interp: &Interpreter,
    object: Value,
) -> Result<SearchObject, LispError> {
    match object.kind() {
        Kind::Nil => Ok(SearchObject::Buffer(interp.current_buffer_id())),
        Kind::StringObject(_) => Ok(SearchObject::String(object)),
        Kind::Buffer(_) => Ok(SearchObject::Buffer(interp.resolve_buffer_id(&object)?)),
        _ => Err(LispError::WrongTypeArgument(
            "buffer-or-string-p".into(),
            object,
        )),
    }
}

/// buffer.h:CHECK_FIXNUM_COERCE_MARKER: fix_position's fixnum.
pub(crate) fn fixnum_coerce_marker(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    fix_position(interp, value)
}

fn args_out_of_range(begin: Value, end: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("args-out-of-range"),
        begin,
        end,
    ]))
}

/// What validate_interval_range leaves a search of OBJECT: the bounds a
/// result is measured against (BEGV and ZV of a buffer, 0 and the length
/// of a string) and its intervals, None where find_interval would get no
/// tree (none, or no accessible text).
struct SearchText<'a, S> {
    min: usize,
    max: usize,
    intervals: Option<PropertyIntervals<'a, S>>,
}

/// F over OBJECT's text, its buffer or string borrowed meanwhile.
fn with_search_text<R>(
    interp: &Interpreter,
    object: SearchObject,
    f: impl FnOnce(&dyn SearchWalk) -> Result<R, LispError>,
) -> Result<R, LispError> {
    match object {
        SearchObject::Buffer(id) => {
            let buffer = interp
                .get_buffer_by_id(id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {id}")))?;
            let (min, max) = (buffer.point_min(), buffer.point_max());
            f(&SearchText {
                min,
                max,
                intervals: buffer.property_intervals().filter(|_| min < max),
            })
        }
        SearchObject::String(value) => {
            let Kind::StringObject(state) = value.kind() else {
                unreachable!("a string search names a string");
            };
            let state = state.borrow();
            let len = state.len();
            f(&SearchText {
                min: 0,
                max: len,
                intervals: (state.props.has_intervals() && len > 0)
                    .then(|| PropertyIntervals::new(&state.props[..], 0, len)),
            })
        }
    }
}

/// The searches over one object's text, whatever its span type.
trait SearchWalk {
    fn min(&self) -> usize;
    fn max(&self) -> usize;
    fn has_intervals(&self) -> bool;
    /// find_interval: the interval holding POS, the last one for POS at
    /// the end of the text; None without intervals.
    fn find(&self, pos: usize) -> Option<PropertyInterval<'_>>;
    /// next_interval.
    fn next(&self, interval: &PropertyInterval<'_>) -> Option<PropertyInterval<'_>>;
    /// previous_interval.
    fn previous(&self, interval: &PropertyInterval<'_>) -> Option<PropertyInterval<'_>>;
    /// The first interval from FROM (find_interval) on whose start is
    /// below END and whose `textget' of PROP is EQ to VALUE (MATCH) or not:
    /// its start, at least FROM.
    fn find_property(
        &self,
        interp: &Interpreter,
        from: usize,
        end: usize,
        prop: PropertyName,
        value: Value,
        matching: bool,
    ) -> Option<usize>;
    /// The start of the first interval after the one holding POS whose
    /// PROP differs (by EQ) from that interval's, with the start of each
    /// interval passed over below LIMIT.
    fn next_change(
        &self,
        interp: &Interpreter,
        pos: usize,
        prop: PropertyName,
        limit: Option<i64>,
    ) -> Option<usize>;
    /// Backward: the end of the first interval before the one holding the
    /// character before POS whose PROP differs, each end passed over above
    /// LIMIT.
    fn previous_change(
        &self,
        interp: &Interpreter,
        pos: usize,
        prop: PropertyName,
        limit: Option<i64>,
    ) -> Option<usize>;
}

fn textget_eq(
    interp: &Interpreter,
    interval: &PropertyInterval,
    prop: PropertyName,
    value: Value,
) -> bool {
    values_eq_in_env(
        interp,
        &textget(interp, interval.plist, prop),
        &value,
        &Env::new(),
    )
}

impl<S: PropertySpan> SearchWalk for SearchText<'_, S> {
    fn min(&self) -> usize {
        self.min
    }

    fn max(&self) -> usize {
        self.max
    }

    fn has_intervals(&self) -> bool {
        self.intervals.is_some()
    }

    fn find(&self, pos: usize) -> Option<PropertyInterval<'_>> {
        self.intervals.as_ref().map(|intervals| intervals.find(pos))
    }

    fn next(&self, interval: &PropertyInterval<'_>) -> Option<PropertyInterval<'_>> {
        self.intervals.as_ref()?.next(interval)
    }

    fn previous(&self, interval: &PropertyInterval<'_>) -> Option<PropertyInterval<'_>> {
        self.intervals.as_ref()?.previous(interval)
    }

    fn find_property(
        &self,
        interp: &Interpreter,
        from: usize,
        end: usize,
        prop: PropertyName,
        value: Value,
        matching: bool,
    ) -> Option<usize> {
        let intervals = self.intervals.as_ref()?;
        let mut interval = Some(intervals.find(from));
        while let Some(current) = interval {
            if current.start >= end {
                break;
            }
            if textget_eq(interp, &current, prop, value) == matching {
                return Some(current.start.max(from));
            }
            interval = intervals.next(&current);
        }
        None
    }

    fn next_change(
        &self,
        interp: &Interpreter,
        pos: usize,
        prop: PropertyName,
        limit: Option<i64>,
    ) -> Option<usize> {
        let intervals = self.intervals.as_ref()?;
        let here = intervals.find(pos);
        let here_value = textget(interp, here.plist, prop);
        let mut next = intervals.next(&here);
        while let Some(current) = next
            && textget_eq(interp, &current, prop, here_value)
            && limit.is_none_or(|limit| (current.start as i64) < limit)
        {
            next = intervals.next(&current);
        }
        let bound = limit.unwrap_or(self.max as i64);
        next.map(|interval| interval.start)
            .filter(|&start| (start as i64) < bound)
    }

    fn previous_change(
        &self,
        interp: &Interpreter,
        pos: usize,
        prop: PropertyName,
        limit: Option<i64>,
    ) -> Option<usize> {
        let intervals = self.intervals.as_ref()?;
        // Start with the interval holding the character before POS.
        let mut here = intervals.find(pos);
        if here.start == pos {
            here = intervals.previous(&here)?;
        }
        let here_value = textget(interp, here.plist, prop);
        let mut previous = intervals.previous(&here);
        while let Some(current) = previous
            && textget_eq(interp, &current, prop, here_value)
            && limit.is_none_or(|limit| (current.end as i64) > limit)
        {
            previous = intervals.previous(&current);
        }
        let bound = limit.unwrap_or(self.min as i64);
        previous
            .map(|interval| interval.end)
            .filter(|&end| (end as i64) > bound)
    }
}

/// validate_interval_range's bounds check of [BEGIN, END] (BEGIN0 and END0
/// the arguments as given).
fn check_range(
    text: &dyn SearchWalk,
    begin: i64,
    end: i64,
    begin0: Value,
    end0: Value,
) -> Result<(), LispError> {
    if text.min() as i64 <= begin && begin <= end && end <= text.max() as i64 {
        Ok(())
    } else {
        Err(args_out_of_range(begin0, end0))
    }
}

/// textprop.c:Ftext_property_any (MATCHING) and Ftext_property_not_all:
/// (START END PROPERTY VALUE &optional OBJECT).
pub(crate) fn text_property_any(
    interp: &Interpreter,
    args: &[Value],
    matching: bool,
) -> Result<Value, LispError> {
    let object = search_object(interp, args.get(4).copied().unwrap_or(Value::Nil))?;
    let start = fixnum_coerce_marker(interp, &args[0])?;
    let end = fixnum_coerce_marker(interp, &args[1])?;
    let (prop, value) = (PropertyName::new(args[2]), args[3]);
    let found = with_search_text(interp, object, |text| {
        let (start, end) = (start.min(end), start.max(end));
        // A range of one point has no interval to search, nor bounds to
        // check.
        let interval_found = if start == end {
            None
        } else {
            check_range(text, start, end, args[0], args[1])?;
            text.has_intervals().then(|| {
                text.find_property(interp, start as usize, end as usize, prop, value, matching)
            })
        };
        Ok(match interval_found {
            Some(found) => found.map(|pos| pos as i64),
            // No intervals: every character's PROPERTY is nil.
            None if start != end && value.is_nil() == matching => Some(start),
            None => None,
        })
    })?;
    Ok(found.map_or(Value::Nil, Value::Integer))
}

/// The coerced LIMIT of a change scan, nil when not given.
fn scan_limit(interp: &Interpreter, limit: Option<&Value>) -> Result<Option<i64>, LispError> {
    match limit {
        Some(limit) if !limit.is_nil() => fixnum_coerce_marker(interp, limit).map(Some),
        _ => Ok(None),
    }
}

/// POSITION of a change scan, validated as a range of one point.
fn scan_position(
    interp: &Interpreter,
    text: &dyn SearchWalk,
    position: &Value,
) -> Result<usize, LispError> {
    let pos = fixnum_coerce_marker(interp, position)?;
    check_range(text, pos, pos, *position, *position)?;
    Ok(pos as usize)
}

fn limit_value(limit: Option<i64>) -> Value {
    limit.map_or(Value::Nil, Value::Integer)
}

/// textprop.c:Fnext_single_property_change: (POSITION PROP &optional
/// OBJECT LIMIT).
pub(crate) fn next_single_property_change(
    interp: &Interpreter,
    args: &[Value],
) -> Result<Value, LispError> {
    let limit = scan_limit(interp, args.get(3))?;
    let object = search_object(interp, args.get(2).copied().unwrap_or(Value::Nil))?;
    let prop = PropertyName::new(args[1]);
    with_search_text(interp, object, |text| {
        let pos = scan_position(interp, text, &args[0])?;
        Ok(text
            .next_change(interp, pos, prop, limit)
            .map_or_else(|| limit_value(limit), |pos| Value::Integer(pos as i64)))
    })
}

/// textprop.c:Fprevious_single_property_change: (POSITION PROP &optional
/// OBJECT LIMIT).
pub(crate) fn previous_single_property_change(
    interp: &Interpreter,
    args: &[Value],
) -> Result<Value, LispError> {
    let limit = scan_limit(interp, args.get(3))?;
    let object = search_object(interp, args.get(2).copied().unwrap_or(Value::Nil))?;
    let prop = PropertyName::new(args[1]);
    with_search_text(interp, object, |text| {
        let pos = scan_position(interp, text, &args[0])?;
        Ok(text
            .previous_change(interp, pos, prop, limit)
            .map_or_else(|| limit_value(limit), |pos| Value::Integer(pos as i64)))
    })
}

/// The string case of Fnext_single_char_property_change (FORWARD) and
/// Fprevious_single_char_property_change: the single-property scan, else
/// LIMIT, else the string's end (start).
pub(crate) fn single_char_property_change_in_string(
    interp: &Interpreter,
    args: &[Value],
    forward: bool,
) -> Result<Value, LispError> {
    let change = if forward {
        next_single_property_change(interp, args)?
    } else {
        previous_single_property_change(interp, args)?
    };
    if !change.is_nil() {
        return Ok(change);
    }
    match args.get(3).filter(|limit| !limit.is_nil()) {
        Some(limit) => match limit.kind() {
            Kind::Integer(_) => Ok(*limit),
            _ => Err(LispError::WrongTypeArgument("fixnump".into(), *limit)),
        },
        None if forward => {
            let Kind::StringObject(state) = args[2].kind() else {
                unreachable!("a string scan names a string");
            };
            Ok(Value::Integer(state.borrow().len() as i64))
        }
        None => Ok(Value::Integer(0)),
    }
}

/// validate_interval_range of [BEGIN, END) in OBJECT for a primitive
/// over a range: each bound CHECK_FIXNUM_COERCE_MARKER'd, the same
/// position twice nothing (GNU's NULL before any check), else the bounds
/// in order within the buffer's BEGV..ZV or the string's 0..length, or
/// args-out-of-range with BEGIN and END as given; an object without text
/// is nothing too.  The bounds, and whether the object has intervals (a
/// soft validation's NULL without them).
pub(crate) fn validate_interval_range(
    interp: &Interpreter,
    object: SearchObject,
    begin: &Value,
    end: &Value,
) -> Result<Option<(usize, usize, bool)>, LispError> {
    let (b, e) = (
        fixnum_coerce_marker(interp, begin)?,
        fixnum_coerce_marker(interp, end)?,
    );
    if b == e {
        return Ok(None);
    }
    let (b, e) = (b.min(e), b.max(e));
    with_search_text(interp, object, |text| {
        check_range(text, b, e, *begin, *end)?;
        Ok((text.min() < text.max()).then(|| (b as usize, e as usize, text.has_intervals())))
    })
}

/// The plist of the character at POS in TEXT (a position
/// validate_interval_range accepted), the empty one without intervals or
/// at the very end of the object, where no character follows.
fn plist_at(text: &dyn SearchWalk, pos: usize) -> &[(PropertyName, Value)] {
    match text.find(pos) {
        Some(interval) if pos != interval.end => interval.plist,
        _ => &[],
    }
}

/// textprop.c:Ftext_properties_at: (POSITION &optional OBJECT).
pub(crate) fn text_properties_at(
    interp: &Interpreter,
    position: &Value,
    object: Value,
) -> Result<Value, LispError> {
    let object = search_object(interp, object)?;
    with_search_text(interp, object, |text| {
        let pos = scan_position(interp, text, position)?;
        Ok(plist_value(plist_at(text, pos)))
    })
}

/// textprop.c:Fget_text_property: textget of Ftext_properties_at.
pub(crate) fn get_text_property(
    interp: &Interpreter,
    position: &Value,
    prop: PropertyName,
    object: Value,
) -> Result<Value, LispError> {
    let object = search_object(interp, object)?;
    with_search_text(interp, object, |text| {
        let pos = scan_position(interp, text, position)?;
        Ok(textget(interp, plist_at(text, pos), prop))
    })
}

/// textprop.c:Fnext_property_change: (POSITION &optional OBJECT LIMIT),
/// a change being an interval whose plist is not intervals_equal to the
/// one at POSITION; LIMIT t the start of the next interval whatever it
/// holds.
pub(crate) fn next_property_change(
    interp: &Interpreter,
    position: &Value,
    object: Value,
    limit: Value,
) -> Result<Value, LispError> {
    let to_next_interval = matches!(limit.kind(), Kind::T);
    let limit = if to_next_interval {
        None
    } else {
        scan_limit(interp, Some(&limit))?
    };
    let object = search_object(interp, object)?;
    with_search_text(interp, object, |text| {
        let pos = scan_position(interp, text, position)?;
        let here = text.find(pos);
        if to_next_interval {
            let next = here.and_then(|here| text.next(&here));
            return Ok(Value::Integer(
                next.map_or(text.max(), |next| next.start) as i64
            ));
        }
        let Some(here) = here else {
            return Ok(limit_value(limit));
        };
        let mut next = text.next(&here);
        while let Some(current) = next
            && crate::buffer::text_property_plists_eq(here.plist, current.plist)
            && limit.is_none_or(|limit| (current.start as i64) < limit)
        {
            next = text.next(&current);
        }
        let bound = limit.unwrap_or(text.max() as i64);
        Ok(next.filter(|next| (next.start as i64) < bound).map_or_else(
            || limit_value(limit),
            |next| Value::Integer(next.start as i64),
        ))
    })
}

/// textprop.c:Fprevious_property_change: (POSITION &optional OBJECT
/// LIMIT), from the interval holding the character before POSITION.
pub(crate) fn previous_property_change(
    interp: &Interpreter,
    position: &Value,
    object: Value,
    limit: Value,
) -> Result<Value, LispError> {
    let limit = scan_limit(interp, Some(&limit))?;
    let object = search_object(interp, object)?;
    with_search_text(interp, object, |text| {
        let pos = scan_position(interp, text, position)?;
        let Some(mut here) = text.find(pos) else {
            return Ok(limit_value(limit));
        };
        if here.start == pos {
            match text.previous(&here) {
                Some(previous) => here = previous,
                None => return Ok(limit_value(limit)),
            }
        }
        let mut previous = text.previous(&here);
        while let Some(current) = previous
            && crate::buffer::text_property_plists_eq(current.plist, here.plist)
            && limit.is_none_or(|limit| (current.end as i64) > limit)
        {
            previous = text.previous(&current);
        }
        let bound = limit.unwrap_or(text.min() as i64);
        Ok(previous
            .filter(|previous| (previous.end as i64) > bound)
            .map_or_else(
                || limit_value(limit),
                |previous| Value::Integer(previous.end as i64),
            ))
    })
}

/// A window object (WINDOWP), live or not: its record id.
fn window_object(interp: &Interpreter, object: &Value) -> Option<u64> {
    match object.kind() {
        Kind::Record(_) => window_record_id_from_value(interp, object),
        _ => None,
    }
}

/// textprop.c:get_char_property_and_overlay: PROP of the character at
/// POSITION (fix_position'd) in OBJECT -- nil the current buffer, a live
/// window its buffer with only the overlays for that window -- from the
/// highest-priority overlay covering it with a non-nil PROP, that overlay
/// returned too, else from its text properties.  A buffer position
/// outside BEGV..ZV is (args-out-of-range POSITION).
pub(crate) fn get_char_property_and_overlay(
    interp: &Interpreter,
    position: &Value,
    prop: PropertyName,
    object: Value,
) -> Result<(Value, Option<crate::overlay::OverlayRef>), LispError> {
    let pos = fix_position(interp, position)?;
    let mut window = None;
    let object = if object.is_nil() {
        Value::Buffer(interp.buffer)
    } else if let Some(id) = window_object(interp, &object) {
        let buffer = window_buffer_id(interp, &object)
            .ok_or_else(|| wrong_type_argument("window-live-p", object))?;
        window = Some(id);
        interp
            .buffer_value(buffer)
            .ok_or_else(|| wrong_type_argument("window-live-p", object))?
    } else {
        object
    };
    if let Kind::Buffer(_) = object.kind() {
        let buffer_id = interp.resolve_buffer_id(&object)?;
        let buffer = interp
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
        if !((buffer.point_min() as i64) <= pos && pos <= buffer.point_max() as i64) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                *position,
            ])));
        }
        if let Some((value, overlay)) = highest_priority_overlay_property_with_id(
            interp,
            &buffer,
            pos as usize,
            prop,
            false,
            window,
        ) {
            return Ok((value, Some(overlay)));
        }
    }
    Ok((
        get_text_property(interp, &Value::Integer(pos), prop, object)?,
        None,
    ))
}

/// textprop.c:Fget_char_property.
pub(crate) fn get_char_property(
    interp: &Interpreter,
    position: &Value,
    prop: PropertyName,
    object: Value,
) -> Result<Value, LispError> {
    get_char_property_and_overlay(interp, position, prop, object).map(|(value, _)| value)
}

/// Fassq of KEY in LIST, its improper tail ignored.
fn assq_entry(key: PropertyName, list: Value) -> Value {
    let mut tail = list;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Kind::Cons(cell) = tail.kind() {
        let entry = cell.car.get();
        if let Kind::Cons(pair) = entry.kind()
            && key.is(pair.car.get())
        {
            return entry;
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        tail = cell.cdr.get();
    }
    Value::Nil
}

/// textprop.c:text_property_stickiness: whether a character inserted at
/// POS in BUFFER inherits PROP from the character after (1), before (-1)
/// or neither (0), by `text-property-default-nonsticky', the character
/// before's `rear-nonsticky' and the one after's `front-sticky'; when
/// both would give it, the one before wins unless its PROP is nil.
pub(crate) fn text_property_stickiness(
    interp: &Interpreter,
    prop: PropertyName,
    pos: i64,
    buffer: Value,
) -> Result<i32, LispError> {
    let prev_pos = Value::Integer(pos - 1);
    let defaults = interp
        .symbol_value_key(cached_symbol!("text-property-default-nonsticky"))
        .unwrap_or(Value::Nil);
    let default = assq_entry(prop, defaults);
    let begv = match buffer.kind() {
        Kind::Nil => interp.buffer.borrow().point_min(),
        _ => {
            let id = interp.resolve_buffer_id(&buffer)?;
            interp
                .get_buffer_by_id(id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {id}")))?
                .point_min()
        }
    } as i64;
    let ignore_previous_character = pos <= begv;
    let mut is_rear_sticky = true;
    if ignore_previous_character
        || matches!(default.kind(), Kind::Cons(cell) if !cell.cdr.get().is_nil())
    {
        is_rear_sticky = false;
    } else {
        let rear_non_sticky = get_text_property(
            interp,
            &prev_pos,
            PropertyName::symbol("rear-nonsticky"),
            buffer,
        )?;
        if tmem(prop, rear_non_sticky) {
            is_rear_sticky = false;
        }
    }
    // An args-out-of-range error for POS outside the accessible text.
    let front_sticky = get_text_property(
        interp,
        &Value::Integer(pos),
        PropertyName::symbol("front-sticky"),
        buffer,
    )?;
    let is_front_sticky = matches!(front_sticky.kind(), Kind::T)
        || (is_cons(&front_sticky) && memq(prop, front_sticky));
    Ok(match (is_rear_sticky, is_front_sticky) {
        (true, false) => -1,
        (false, true) => 1,
        (false, false) => 0,
        (true, true) => {
            if ignore_previous_character
                || get_text_property(interp, &prev_pos, prop, buffer)?.is_nil()
            {
                1
            } else {
                -1
            }
        }
    })
}

/// editfns.c:Fget_pos_property: PROP that a character inserted at
/// POSITION in OBJECT would have -- from the highest-priority overlay
/// whose ends' advance types would take it in, else the text property by
/// stickiness.  A string (or a window's missing buffer) is
/// Fget_text_property's.
pub(crate) fn get_pos_property(
    interp: &Interpreter,
    position: &Value,
    prop: PropertyName,
    object: Value,
) -> Result<Value, LispError> {
    let pos = fixnum_coerce_marker(interp, position)?;
    let position = Value::Integer(pos);
    let object = if object.is_nil() {
        Value::Buffer(interp.buffer)
    } else if window_object(interp, &object).is_some() {
        window_buffer_id(interp, &object)
            .and_then(|buffer| interp.buffer_value(buffer))
            .unwrap_or(Value::Nil)
    } else {
        object
    };
    if !matches!(object.kind(), Kind::Buffer(_)) {
        return get_text_property(interp, &position, prop, object);
    }
    let begv = {
        let buffer_id = interp.resolve_buffer_id(&object)?;
        let buffer = interp
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
        if pos >= 0
            && let Some(value) =
                highest_priority_overlay_property(interp, &buffer, pos as usize, prop, true, None)
        {
            return Ok(value);
        }
        buffer.point_min() as i64
    };
    match text_property_stickiness(interp, prop, pos, object)? {
        stickiness if stickiness > 0 => get_text_property(interp, &position, prop, object),
        stickiness if stickiness < 0 && pos > begv => {
            get_text_property(interp, &Value::Integer(pos - 1), prop, object)
        }
        _ => Ok(Value::Nil),
    }
}

/// textprop.c:Fnext_char_property_change: the next text property change
/// from POSITION in the current buffer, before the next overlay boundary
/// (Fnext_overlay_change) and LIMIT.
pub(crate) fn next_char_property_change(
    interp: &Interpreter,
    position: &Value,
    limit: &Value,
) -> Result<Value, LispError> {
    let pos = fixnum_coerce_marker(interp, position)?;
    let mut temp =
        next_overlay_change_position(&interp.buffer.borrow(), pos.max(0) as usize) as i64;
    if !limit.is_nil() {
        let limit = fixnum_coerce_marker(interp, limit)?;
        if limit < temp {
            temp = limit;
        }
    }
    next_property_change(interp, position, Value::Nil, Value::Integer(temp))
}

/// textprop.c:Fprevious_char_property_change.
pub(crate) fn previous_char_property_change(
    interp: &Interpreter,
    position: &Value,
    limit: &Value,
) -> Result<Value, LispError> {
    let pos = fixnum_coerce_marker(interp, position)?;
    let mut temp =
        previous_overlay_change_position(&interp.buffer.borrow(), pos.max(0) as usize) as i64;
    if !limit.is_nil() {
        let limit = fixnum_coerce_marker(interp, limit)?;
        if limit > temp {
            temp = limit;
        }
    }
    previous_property_change(interp, position, Value::Nil, Value::Integer(temp))
}

/// F with OBJECT's buffer current when OBJECT is a buffer other than the
/// current one (record_unwind_current_buffer and set_buffer_internal),
/// the current buffer restored after, an error too.
pub(crate) fn with_object_buffer<R>(
    interp: &mut Interpreter,
    object: &Value,
    f: impl FnOnce(&mut Interpreter) -> Result<R, LispError>,
) -> Result<R, LispError> {
    let Kind::Buffer(_) = object.kind() else {
        return f(interp);
    };
    let id = interp.resolve_buffer_id(object)?;
    let original = interp.current_buffer_id();
    if id == original {
        return f(interp);
    }
    interp.set_current_buffer_id(id)?;
    let result = f(interp);
    if interp.has_buffer_id(original) {
        interp.set_current_buffer_id(original)?;
    }
    result
}

/// The string case of Fnext_single_char_property_change (FORWARD) and
/// Fprevious_single_char_property_change: the single-property scan, else
/// LIMIT (CHECK_FIXNUM), else the string's end (start).
fn single_char_property_change_string(
    interp: &Interpreter,
    position: &Value,
    prop: Value,
    string: Value,
    limit: Value,
    forward: bool,
) -> Result<Value, LispError> {
    single_char_property_change_in_string(interp, &[*position, prop, string, limit], forward)
}

/// textprop.c:Fnext_single_char_property_change: (POSITION PROP
/// &optional OBJECT LIMIT), in a buffer from char-property change to
/// change (Fnext_char_property_change) until PROP's Fget_char_property
/// is not EQ to POSITION's, with OBJECT's buffer current.
pub(crate) fn next_single_char_property_change(
    interp: &mut Interpreter,
    position: &Value,
    prop: Value,
    object: Value,
    limit: Value,
) -> Result<Value, LispError> {
    if let Kind::StringObject(_) = object.kind() {
        return single_char_property_change_string(interp, position, prop, object, limit, true);
    }
    if !object.is_nil() && !matches!(object.kind(), Kind::Buffer(_)) {
        return Err(wrong_type_argument("bufferp", object));
    }
    let name = PropertyName::new(prop);
    with_object_buffer(interp, &object, |interp| {
        let mut position = fixnum_coerce_marker(interp, position)?;
        let initial = get_char_property(interp, &Value::Integer(position), name, object)?;
        let zv = interp.buffer.borrow().point_max() as i64;
        let limit = if limit.is_nil() {
            zv
        } else {
            fixnum_coerce_marker(interp, &limit)?
        };
        if position >= limit {
            position = limit.min(zv);
        } else {
            loop {
                position = next_char_property_change(
                    interp,
                    &Value::Integer(position),
                    &Value::Integer(limit),
                )?
                .as_integer()?;
                if position >= limit {
                    position = limit;
                    break;
                }
                let value = get_char_property(interp, &Value::Integer(position), name, object)?;
                if !values_eq_in_env(interp, &value, &initial, &Env::new()) || position >= zv {
                    break;
                }
            }
        }
        Ok(Value::Integer(position))
    })
}

/// textprop.c:Fprevious_single_char_property_change: backward, PROP read
/// from the character before each position.
pub(crate) fn previous_single_char_property_change(
    interp: &mut Interpreter,
    position: &Value,
    prop: Value,
    object: Value,
    limit: Value,
) -> Result<Value, LispError> {
    if let Kind::StringObject(_) = object.kind() {
        return single_char_property_change_string(interp, position, prop, object, limit, false);
    }
    if !object.is_nil() && !matches!(object.kind(), Kind::Buffer(_)) {
        return Err(wrong_type_argument("bufferp", object));
    }
    let name = PropertyName::new(prop);
    with_object_buffer(interp, &object, |interp| {
        let mut position = fixnum_coerce_marker(interp, position)?;
        let begv = interp.buffer.borrow().point_min() as i64;
        let limit = if limit.is_nil() {
            begv
        } else {
            fixnum_coerce_marker(interp, &limit)?
        };
        if position <= limit {
            position = limit.max(begv);
        } else {
            let initial = get_char_property(
                interp,
                &Value::Integer(position - i64::from(0 <= position)),
                name,
                object,
            )?;
            loop {
                position = previous_char_property_change(
                    interp,
                    &Value::Integer(position),
                    &Value::Integer(limit),
                )?
                .as_integer()?;
                if position <= limit {
                    position = limit;
                    break;
                }
                let value = get_char_property(interp, &Value::Integer(position - 1), name, object)?;
                if !values_eq_in_env(interp, &value, &initial, &Env::new()) {
                    break;
                }
            }
        }
        Ok(Value::Integer(position))
    })
}

/// textprop.c:validate_plist: nil no properties, a list its pairs (one
/// of an odd length "Odd length text property list"), any other object
/// that property with nil.
fn validate_plist(list: Value) -> Result<Vec<(PropertyName, Value)>, LispError> {
    match list.kind() {
        Kind::Nil => Ok(Vec::new()),
        Kind::Cons(_) => {
            let mut pairs = Vec::new();
            let mut seen = crate::lisp::types::CycleGuard::new();
            let mut tail = list;
            while let Kind::Cons(cell) = tail.kind() {
                let Kind::Cons(rest) = cell.cdr.get().kind() else {
                    return Err(LispError::Signal("Odd length text property list".into()));
                };
                pairs.push((PropertyName::new(cell.car.get()), rest.car.get()));
                if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("circular-list"),
                        list,
                    ])));
                }
                tail = rest.cdr.get();
            }
            Ok(pairs)
        }
        _ => Ok(vec![(PropertyName::new(list), Value::Nil)]),
    }
}

/// The names of the plist PROPERTIES as remove_properties walks it, the
/// car of every other cons (Fcdr of an improper tail a `listp' error).
fn plist_names(properties: Value) -> Result<Vec<PropertyName>, LispError> {
    let mut names = Vec::new();
    let mut seen = crate::lisp::types::CycleGuard::new();
    let mut tail = properties;
    while let Kind::Cons(cell) = tail.kind() {
        names.push(PropertyName::new(cell.car.get()));
        tail = match cell.cdr.get().kind() {
            Kind::Cons(rest) => rest.cdr.get(),
            Kind::Nil => Value::Nil,
            _ => return Err(wrong_type_argument("listp", cell.cdr.get())),
        };
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
    }
    Ok(names)
}

/// The elements of LIST up to its first non-cons tail.
fn list_names(list: Value) -> Vec<PropertyName> {
    let mut names = Vec::new();
    let mut seen = crate::lisp::types::CycleGuard::new();
    let mut tail = list;
    while let Kind::Cons(cell) = tail.kind() {
        names.push(PropertyName::new(cell.car.get()));
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        tail = cell.cdr.get();
    }
    names
}

/// textprop.c:add_text_properties_1 with TEXT_PROPERTY_REPLACE
/// (Fadd_text_properties, Fput_text_property): PROPERTIES over
/// [START, END) of OBJECT, a buffer one with that buffer current and
/// through modify_text_properties and signal_after_change when a value
/// changes.  Whether one did.
pub(crate) fn add_text_properties(
    interp: &mut Interpreter,
    start: &Value,
    end: &Value,
    properties: Value,
    object: Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    with_object_buffer(interp, &object, |interp| {
        let props = validate_plist(properties)?;
        if props.is_empty() {
            return Ok(false);
        }
        let target = search_object(interp, object)?;
        let Some((start, end, _)) = validate_interval_range(interp, target, start, end)? else {
            return Ok(false);
        };
        match target {
            SearchObject::String(string) => add_string_properties(&string, start, end, &props),
            SearchObject::Buffer(id) => modify_buffer_text_properties(
                interp,
                id,
                start,
                end,
                &|buffer| properties_would_add(buffer, start, end, &props),
                &|buffer| buffer.add_text_properties(start, end, &props),
                env,
            ),
        }
    })
}

/// textprop.c:add_properties with TEXT_PROPERTY_REPLACE over [START, END)
/// of the string STRING: each of PROPS replaces its value in place, or is
/// consed onto the head of a plist without it.  Whether one changed.
pub(crate) fn add_string_properties(
    string: &Value,
    start: usize,
    end: usize,
    props: &[(PropertyName, Value)],
) -> Result<bool, LispError> {
    modify_shared_string_properties(string, start, end, |mut current| {
        for (name, value) in props {
            if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == name) {
                *existing = *value;
            } else {
                current.insert(0, (*name, *value));
            }
        }
        current
    })
}

/// textprop.c:set_text_properties with a coherent change
/// (Fset_text_properties): PROPERTIES the whole plist of [START, END).  t
/// unless there is nothing to set: no text in the range, or neither
/// properties nor intervals.
pub(crate) fn set_text_properties(
    interp: &mut Interpreter,
    start: &Value,
    end: &Value,
    properties: Value,
    object: Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    with_object_buffer(interp, &object, |interp| {
        let props = validate_plist(properties)?;
        let target = search_object(interp, object)?;
        // No properties for a whole string: no intervals at all.
        if props.is_empty()
            && let SearchObject::String(string) = target
            && let Kind::StringObject(state) = string.kind()
            && matches!(start.kind(), Kind::Integer(0))
            && matches!(end.kind(), Kind::Integer(len) if len == state.borrow().len() as i64)
        {
            if !state.borrow().props.has_intervals() {
                return Ok(false);
            }
            state.check_impure()?;
            state.borrow_mut().props = Default::default();
            return Ok(true);
        }
        let Some((start, end, intervals)) = validate_interval_range(interp, target, start, end)?
        else {
            return Ok(false);
        };
        if !intervals && props.is_empty() {
            return Ok(false);
        }
        match target {
            SearchObject::String(string) => {
                modify_shared_string_properties(&string, start, end, |_| props.clone())?;
                Ok(true)
            }
            SearchObject::Buffer(id) => modify_buffer_text_properties(
                interp,
                id,
                start,
                end,
                &|_| true,
                &|buffer| buffer.set_text_properties(start, end, &props),
                env,
            ),
        }
    })
}

/// textprop.c:Fremove_text_properties (LIST nil, PROPERTIES a plist whose
/// names go) and Fremove_list_of_text_properties (LIST t, a list of
/// names): whether a property went.
pub(crate) fn remove_text_properties(
    interp: &mut Interpreter,
    start: &Value,
    end: &Value,
    properties: Value,
    object: Value,
    list: bool,
    env: &mut Env,
) -> Result<bool, LispError> {
    with_object_buffer(interp, &object, |interp| {
        let target = search_object(interp, object)?;
        let Some((start, end, true)) = validate_interval_range(interp, target, start, end)? else {
            return Ok(false);
        };
        let names = if list {
            list_names(properties)
        } else {
            plist_names(properties)?
        };
        match target {
            SearchObject::String(string) => {
                modify_shared_string_properties(&string, start, end, |current| {
                    current
                        .into_iter()
                        .filter(|(key, _)| !names.contains(key))
                        .collect()
                })
            }
            SearchObject::Buffer(id) => modify_buffer_text_properties(
                interp,
                id,
                start,
                end,
                &|buffer| properties_would_remove(buffer, start, end, &names),
                &|buffer| buffer.remove_list_of_text_properties(start, end, &names),
                env,
            ),
        }
    })
}

/// add_properties' TEXT_PROPERTY_PREPEND or _APPEND of FACE to an
/// interval's face OLD: a list (but an anonymous face, a plist of
/// attributes) takes FACE at its head or, copied, its end; a single value
/// becomes a list of the two.
fn combine_face(old: Value, face: Value, append: bool) -> Result<Value, LispError> {
    let is_list = matches!(old.kind(), Kind::Cons(cell)
        if !matches!(cell.car.get().kind(), Kind::Symbol(name) if name.is_keyword()));
    Ok(match (is_list, append) {
        (true, false) => Value::cons(face, old),
        (true, true) => {
            let mut items = old.to_vec()?;
            items.push(face);
            Value::list(items)
        }
        (false, false) => Value::list([face, old]),
        (false, true) => Value::list([old, face]),
    })
}

/// add_properties' new face for an interval whose plist is PLIST: None
/// where its face is FACE already, else FACE consed on for none, else
/// combine_face's.
fn face_update(
    plist: &[(PropertyName, Value)],
    face_name: PropertyName,
    face: Value,
    append: bool,
) -> Result<Option<Value>, LispError> {
    match plist.iter().find(|(name, _)| *name == face_name) {
        None => Ok(Some(face)),
        Some((_, old)) if old.word() == face.word() => Ok(None),
        Some((_, old)) => combine_face(*old, face, append).map(Some),
    }
}

/// PLIST with FACE_NAME's value NEW, replaced in place or consed on.
fn with_face(
    mut plist: Vec<(PropertyName, Value)>,
    face_name: PropertyName,
    new: Value,
) -> Vec<(PropertyName, Value)> {
    if let Some((_, existing)) = plist.iter_mut().find(|(key, _)| *key == face_name) {
        *existing = new;
    } else {
        plist.insert(0, (face_name, new));
    }
    plist
}

/// textprop.c:Fadd_face_text_property: add_text_properties_1 of (face
/// FACE) prepended (APPEND nil) or appended to each interval's face but
/// the ones whose face is FACE already, not destructively, in one pass
/// over the intervals after one that finds whether any changes (and
/// signals what Fappend would of an improper face list).
pub(crate) fn add_face_text_property(
    interp: &mut Interpreter,
    start: &Value,
    end: &Value,
    face: Value,
    append: bool,
    object: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let face_name = PropertyName::symbol("face");
    let update = move |current: Vec<(PropertyName, Value)>| match face_update(
        &current, face_name, face, append,
    ) {
        Ok(Some(new)) => with_face(current, face_name, new),
        _ => current,
    };
    with_object_buffer(interp, &object, |interp| {
        let target = search_object(interp, object)?;
        let Some((start, end, _)) = validate_interval_range(interp, target, start, end)? else {
            return Ok(());
        };
        let mut changes = false;
        match target {
            SearchObject::String(string) => {
                let Kind::StringObject(state) = string.kind() else {
                    unreachable!("a string target is a string");
                };
                {
                    let state = state.borrow();
                    let intervals = PropertyIntervals::new(&state.props[..], 0, state.len());
                    let mut interval = Some(intervals.find(start));
                    while let Some(current) = interval
                        && current.start < end
                    {
                        changes |= face_update(current.plist, face_name, face, append)?.is_some();
                        interval = intervals.next(&current);
                    }
                }
                if changes {
                    modify_shared_string_properties(&string, start, end, update)?;
                }
                Ok(())
            }
            SearchObject::Buffer(id) => {
                {
                    let buffer = interp
                        .get_buffer_by_id(id)
                        .ok_or_else(|| LispError::Signal(format!("No buffer with id {id}")))?;
                    buffer.try_for_each_property_interval(start, end, |_, _, plist| {
                        changes |= face_update(plist, face_name, face, append)?.is_some();
                        Ok::<(), LispError>(())
                    })?;
                }
                if changes {
                    modify_buffer_text_properties(
                        interp,
                        id,
                        start,
                        end,
                        &|_| true,
                        &|buffer| buffer.update_text_properties(start, end, update),
                        env,
                    )?;
                }
                Ok(())
            }
        }
    })
}

/// editfns.c:update_buffer_properties: `buffer-access-fontify-functions'
/// run with START and END before text of the current buffer is copied
/// with its properties, unless `buffer-access-fontified-property' is set
/// and no character of the range lacks it.
pub(crate) fn update_buffer_properties(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let functions = interp
        .symbol_value_key(cached_symbol!("buffer-access-fontify-functions"))
        .unwrap_or(Value::Nil);
    if functions.is_nil() {
        return Ok(());
    }
    let fontified = interp
        .symbol_value_key(cached_symbol!("buffer-access-fontified-property"))
        .unwrap_or(Value::Nil);
    let (start, end) = (Value::Integer(start as i64), Value::Integer(end as i64));
    if !fontified.is_nil()
        && text_property_any(
            interp,
            &[start, end, fontified, Value::Nil, Value::Nil],
            true,
        )?
        .is_nil()
    {
        return Ok(());
    }
    call_function_value(
        interp,
        &Value::symbol("run-hook-with-args"),
        &[Value::symbol("buffer-access-fontify-functions"), start, end],
        env,
    )?;
    Ok(())
}
