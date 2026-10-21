//! textprop.c's searches over an object's intervals: `text-property-any',
//! `text-property-not-all' and the single-property change scans, each a
//! walk from find_interval with `textget' values compared by EQ.

use super::*;
use crate::buffer::{PropertyInterval, PropertyIntervals, PropertySpan};
use crate::lisp::types::{Kind, PropertyName};

/// The object a text-property search names: a buffer (nil the current
/// one) or a string.
enum SearchObject {
    Buffer(u64),
    String(Value),
}

fn search_object(interp: &Interpreter, object: Value) -> Result<SearchObject, LispError> {
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

/// lisp.h:CHECK_FIXNUM_COERCE_MARKER.
fn fixnum_coerce_marker(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(n),
        Kind::Marker(marker) => interp
            .marker_position(marker)
            .map(|position| position as i64)
            .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into())),
        _ => Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            *value,
        )),
    }
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
