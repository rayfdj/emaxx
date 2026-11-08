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
                    buffer.storage_byte_position(pos).unwrap_or(pos)
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
                let old = object.buffer().map(|old| (old, object.beg(), object.end()));
                object.move_to(buffer, beg, end);
                let buffer_id = interp.live_buffer_id(buffer);
                match old {
                    // The overlay was and is in the same buffer: the area
                    // it has just left, or just enclosed, shown anew.
                    Some((old, o_beg, o_end)) if old.ptr_eq(&buffer) => {
                        if let Some(id) = buffer_id {
                            if o_beg == beg {
                                interp.modify_overlay(id, o_end, end);
                            } else if o_end == end {
                                interp.modify_overlay(id, o_beg, beg);
                            } else {
                                interp.modify_overlay(id, o_beg.min(beg), o_end.max(end));
                            }
                        }
                    }
                    // A change of buffers: where the overlay was, and
                    // where it is going to be.
                    old => {
                        if let Some((old, o_beg, o_end)) = old
                            && let Some(old_id) = interp.live_buffer_id(old)
                        {
                            interp.modify_overlay(old_id, o_beg, o_end);
                        }
                        if let Some(id) = buffer_id {
                            interp.modify_overlay(id, beg, end);
                        }
                    }
                }
                if beg == end
                    && overlay_property_with_category(interp, &object, "evaporate")?
                        .is_some_and(|v| v.is_truthy())
                {
                    interp.drop_overlay(object);
                }
                Ok(args[0])
            }
            "delete-overlay" => {
                need_args(name, args, 1)?;
                delete_overlay(interp, overlay(args[0])?)?;
                Ok(Value::Nil)
            }
            "delete-all-overlays" => {
                // buffer.c:decode_buffer: a killed buffer has no overlays.
                let buffer = interp.decode_buffer(args.first())?;
                if let Some(buffer_id) = interp.live_buffer_id(buffer) {
                    interp.delete_buffer_overlays(buffer_id);
                }
                Ok(Value::Nil)
            }
            "overlay-put" => {
                need_args(name, args, 3)?;
                let object = overlay(args[0])?;
                let changed = object.put_prop(args[1], args[2]);
                if let Some(buffer) = object.buffer() {
                    if changed && let Some(buffer_id) = interp.live_buffer_id(buffer) {
                        interp.modify_overlay(buffer_id, object.beg(), object.end());
                    }
                    // EQ (prop, Qevaporate).
                    if crate::lisp::types::eq(
                        args[1],
                        cached_symbol!("evaporate").with(|symbol| Value::Symbol(*symbol)),
                    ) && args[2].is_truthy()
                        && object.beg() == object.end()
                    {
                        delete_overlay(interp, object)?;
                    }
                }
                Ok(args[2])
            }
            "overlay-get" => {
                need_args(name, args, 2)?;
                let object = overlay(args[0])?;
                Ok(overlay_property_with_category(
                    interp,
                    &object,
                    crate::lisp::types::PropertyName::new(args[1]),
                )?
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
                    sort_overlays(interp, &mut objects, window)?;
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

/// buffer.c:Fdelete_overlay: OVERLAY dropped from its buffer, whose
/// redisplay takes no shortcut when the overlay had a before- or
/// after-string (they may hold newlines).
fn delete_overlay(interp: &mut Interpreter, object: OverlayRef) -> Result<(), LispError> {
    let Some(buffer) = object.buffer() else {
        return Ok(());
    };
    interp.drop_overlay(object);
    if interp.windows_or_buffers_changed == 0
        && (overlay_property_with_category(interp, &object, "before-string")?
            .is_some_and(|value| !value.is_nil())
            || overlay_property_with_category(interp, &object, "after-string")?
                .is_some_and(|value| !value.is_nil()))
        && let Some(buffer_id) = interp.live_buffer_id(buffer)
    {
        interp.prevent_redisplay_optimizations(buffer_id);
    }
    Ok(())
}
