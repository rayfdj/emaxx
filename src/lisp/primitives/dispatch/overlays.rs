use super::*;
use crate::lisp::types::{BufferRef, Kind, OverlayRef};
use crate::overlay::Traversal;

fn overlay(value: Value) -> Result<OverlayRef, LispError> {
    match value.kind() {
        Kind::Overlay(object) => Ok(object),
        _ => Err(LispError::WrongTypeArgument("overlayp".into(), value)),
    }
}

fn overlay_buffer(
    interp: &Interpreter,
    value: Option<&Value>,
    default: BufferRef,
    moving: bool,
) -> Result<BufferRef, LispError> {
    let object = match value.map(|v| v.kind()) {
        None | Some(Kind::Nil) => default,
        Some(Kind::Buffer(buffer)) => buffer,
        Some(_) => {
            return Err(LispError::WrongTypeArgument(
                "bufferp".into(),
                *value.expect("present"),
            ));
        }
    };
    if !interp
        .buffer_object(object.id)
        .is_some_and(|live| live.ptr_eq(&object))
    {
        return Err(LispError::Signal(if moving {
            "Attempt to move overlay to a dead buffer".into()
        } else {
            "Attempt to create overlay in a dead buffer".into()
        }));
    }
    Ok(object)
}

fn overlay_range(
    interp: &Interpreter,
    buffer: BufferRef,
    beg: Value,
    end: Value,
) -> Result<(usize, usize), LispError> {
    // buffer.c checks both marker owners before coercing either endpoint.
    for value in [beg, end] {
        if let Kind::Marker(marker) = value.kind()
            && !marker.buffer().is_some_and(|owner| owner.ptr_eq(&buffer))
        {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::String("Marker points into wrong buffer".into()),
                value,
            ])));
        }
    }
    let beg = position_from_value(interp, &beg)? as i64;
    let end = position_from_value(interp, &end)? as i64;
    Ok(clamp_overlay_range(&buffer.borrow(), beg, end))
}

pub(super) fn next_overlay_change_position(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> usize {
    let mut next = buffer.point_max();
    let mut iter =
        buffer
            .overlays
            .intersecting(position as isize, next as isize, Traversal::Ascending);
    while let Some(overlay) = iter.next() {
        let (beg, end) = overlay.bounds();
        if beg > position as isize {
            next = beg as usize;
            break;
        }
        if beg < end && end < next as isize {
            next = end as usize;
            iter.narrow(position as isize, next as isize);
        }
    }
    next
}

pub(super) fn previous_overlay_change_position(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> usize {
    let mut previous = buffer.point_min();
    let mut iter =
        buffer
            .overlays
            .intersecting(previous as isize, position as isize, Traversal::Descending);
    while let Some(overlay) = iter.next() {
        let (beg, end) = overlay.bounds();
        previous = if end < position as isize {
            end as usize
        } else {
            previous.max(beg as usize)
        };
        iter.narrow(previous as isize, position as isize);
    }
    previous
}

/// buffer.c:overlays_in supplies both public overlap queries. Enumeration
/// comes from the tree; there is no creation-ID ordering or separate cache.
fn overlays_in_range(
    buffer: &crate::buffer::Buffer,
    beg: usize,
    end: usize,
    empty: bool,
    trailing: bool,
) -> Vec<OverlayRef> {
    let mut result = Vec::new();
    let zv = buffer.point_max();
    let search_end = zv + usize::from(end >= zv && (empty || trailing));
    for overlay in
        buffer
            .overlays
            .intersecting(beg as isize, search_end as isize, Traversal::Ascending)
    {
        let (start, stop) = overlay.bounds();
        if start > end as isize {
            break;
        }
        if start == end as isize {
            if (!empty || end < zv) && beg < end {
                break;
            }
            if empty && start != stop {
                continue;
            }
        }
        if !empty && start == stop {
            continue;
        }
        result.push(overlay);
    }
    result
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        _env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "make-overlay" => {
                if !(2..=5).contains(&args.len()) {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let buffer = overlay_buffer(interp, args.get(2), interp.buffer, false)?;
                let (beg, end) = overlay_range(interp, buffer, args[0], args[1])?;
                let object = OverlayRef::new(
                    args.get(3).is_some_and(Value::is_truthy),
                    args.get(4).is_some_and(Value::is_truthy),
                );
                object.move_to(buffer, beg, end);
                Ok(Value::Overlay(object))
            }
            "overlayp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::Overlay(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "overlay-buffer" => {
                need_args(name, args, 1)?;
                Ok(overlay(args[0])?
                    .buffer()
                    .map(Value::Buffer)
                    .unwrap_or(Value::Nil))
            }
            "overlay-start" | "overlay-end" => {
                need_args(name, args, 1)?;
                let object = overlay(args[0])?;
                let Some(buffer) = object.buffer() else {
                    return Ok(Value::Nil);
                };
                let (beg, end) = object.bounds();
                let pos = if name == "overlay-start" { beg } else { end } as usize;
                let buffer = buffer.borrow();
                let pos = if buffer.is_multibyte() {
                    pos
                } else {
                    buffer_position_to_byte(&buffer, pos).unwrap_or(pos)
                };
                Ok(Value::Integer(pos as i64))
            }
            "move-overlay" => {
                if !(3..=4).contains(&args.len()) {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let object = overlay(args[0])?;
                let buffer = overlay_buffer(
                    interp,
                    args.get(3),
                    object.buffer().unwrap_or(interp.buffer),
                    true,
                )?;
                let (beg, end) = overlay_range(interp, buffer, args[1], args[2])?;
                object.move_to(buffer, beg, end);
                if beg == end
                    && overlay_property_with_category(interp, &object, "evaporate")
                        .is_some_and(|v| v.is_truthy())
                {
                    object.detach();
                }
                Ok(args[0])
            }
            "delete-overlay" => {
                need_args(name, args, 1)?;
                overlay(args[0])?.detach();
                Ok(Value::Nil)
            }
            "delete-all-overlays" => {
                let buffer_id = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => interp.current_buffer_id(),
                    Some(buffer) => interp.resolve_buffer_id(&buffer.value())?,
                };
                interp.delete_buffer_overlays(buffer_id);
                Ok(Value::Nil)
            }
            "overlay-put" => {
                need_args(name, args, 3)?;
                let object = overlay(args[0])?;
                object.put_prop(args[1], args[2]);
                if matches!(args[1].kind(), Kind::Symbol(prop) if prop == "evaporate")
                    && args[2].is_truthy()
                    && object.beg() == object.end()
                {
                    object.detach();
                }
                Ok(args[2])
            }
            "overlay-get" => {
                need_args(name, args, 2)?;
                let object = overlay(args[0])?;
                Ok(if let Kind::Symbol(prop) = args[1].kind() {
                    overlay_property_with_category(interp, &object, &prop)
                } else {
                    object.get_prop(&args[1])
                }
                .unwrap_or(Value::Nil))
            }
            "overlay-properties" => {
                need_args(name, args, 1)?;
                Ok(Value::list(overlay(args[0])?.plist().to_vec()?))
            }
            "overlays-at" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                let mut objects =
                    overlays_in_range(&interp.buffer.borrow(), pos, pos + 1, false, true);
                if let Some(sorted) = args.get(1).filter(|value| value.is_truthy()) {
                    let window = window_record_id_from_value(interp, sorted);
                    sort_overlays(interp, &mut objects, window);
                    objects.reverse();
                }
                Ok(Value::list(objects.into_iter().map(Value::Overlay)))
            }
            "overlays-in" => {
                need_args(name, args, 2)?;
                let beg = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                let objects = overlays_in_range(&interp.buffer.borrow(), beg, end, true, false);
                Ok(Value::list(objects.into_iter().map(Value::Overlay)))
            }
            "next-overlay-change" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                Ok(Value::Integer(
                    next_overlay_change_position(&interp.buffer.borrow(), pos) as i64,
                ))
            }
            "previous-overlay-change" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                Ok(Value::Integer(
                    previous_overlay_change_position(&interp.buffer.borrow(), pos) as i64,
                ))
            }
            "overlay-lists" => {
                let buffer = interp.buffer.borrow();
                let mut objects: Vec<_> = buffer
                    .overlays
                    .intersecting(1, (buffer.size_total() + 1) as isize, Traversal::Descending)
                    .map(Value::Overlay)
                    .collect();
                objects.reverse();
                Ok(Value::list([Value::list(objects)]))
            }
            "overlay-recenter" => {
                need_args(name, args, 1)?;
                position_from_value(interp, &args[0])?;
                Ok(Value::Nil)
            }
        }
    }
);
