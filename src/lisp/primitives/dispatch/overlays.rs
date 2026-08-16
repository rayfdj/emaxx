use super::*;

pub(super) fn next_overlay_change_position(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> usize {
    let mut next = buffer.point_max();
    for overlay in &buffer.overlays {
        if overlay.is_dead() {
            continue;
        }
        if overlay.beg > position && overlay.beg < next {
            next = overlay.beg;
        }
        if overlay.end > position && overlay.end < next {
            next = overlay.end;
        }
    }
    next
}

pub(super) fn previous_overlay_change_position(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> usize {
    let mut previous = buffer.point_min();
    for overlay in &buffer.overlays {
        if overlay.is_dead() {
            continue;
        }
        if overlay.beg < position && overlay.beg > previous {
            previous = overlay.beg;
        }
        if overlay.end < position && overlay.end > previous {
            previous = overlay.end;
        }
    }
    previous
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        _env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Overlay operations ──
            "make-overlay" => {
                // (make-overlay BEG END &optional BUFFER FRONT-ADVANCE REAR-ADVANCE)
                if !(2..=5).contains(&args.len()) {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let beg = position_from_value(interp, &args[0])? as i64;
                let end = position_from_value(interp, &args[1])? as i64;
                let buffer_id = if let Some(buffer_arg) = args.get(2) {
                    if buffer_arg.is_nil() {
                        interp.current_buffer_id()
                    } else if matches!(buffer_arg, Value::Buffer(_)) {
                        interp.resolve_buffer_id(buffer_arg)?
                    } else {
                        return Err(LispError::TypeError(
                            "buffer".into(),
                            buffer_arg.type_name(),
                        ));
                    }
                } else {
                    interp.current_buffer_id()
                };
                let front_advance = args.get(3).is_some_and(|v| v.is_truthy());
                let rear_advance = args.get(4).is_some_and(|v| v.is_truthy());
                let ov_id = interp.alloc_overlay_id();
                let (beg, end) = {
                    let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", buffer_id))
                    })?;
                    clamp_overlay_range(buffer, beg, end)
                };
                let ov = crate::overlay::Overlay::new(
                    ov_id,
                    beg,
                    end,
                    buffer_id,
                    front_advance,
                    rear_advance,
                );
                interp
                    .get_buffer_by_id_mut(buffer_id)
                    .expect("resolved live buffer id")
                    .overlays
                    .push(ov);
                Ok(Value::Overlay(ov_id))
            }

            "overlayp" => {
                need_args(name, args, 1)?;
                Ok(if matches!(&args[0], Value::Overlay(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }

            "copy-overlay" => {
                need_args(name, args, 1)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                let mut copy = interp
                    .find_overlay(ov_id)
                    .cloned()
                    .ok_or_else(|| LispError::Signal("No such overlay".into()))?;
                copy.id = interp.alloc_overlay_id();
                let copy_id = copy.id;
                let target_buffer_id = copy.buffer_id.unwrap_or_else(|| interp.current_buffer_id());
                interp
                    .get_buffer_by_id_mut(target_buffer_id)
                    .ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", target_buffer_id))
                    })?
                    .overlays
                    .push(copy);
                Ok(Value::Overlay(copy_id))
            }

            "overlay-buffer" => {
                need_args(name, args, 1)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                match interp.find_overlay(ov_id) {
                    Some(ov) if !ov.is_dead() => {
                        let buf_id = ov.buffer_id.unwrap_or(0);
                        let buf_name = interp
                            .buffer_list
                            .iter()
                            .find(|(id, _)| *id == buf_id)
                            .map_or("*unknown*".to_string(), |(_, n)| n.clone());
                        Ok(Value::buffer(buf_id, buf_name))
                    }
                    _ => Ok(Value::Nil),
                }
            }

            "overlay-start" => {
                need_args(name, args, 1)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                match interp.find_overlay(ov_id) {
                    Some(ov) if !ov.is_dead() => {
                        let pos = if let Some(buffer_id) = ov.buffer_id {
                            let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                                LispError::Signal(format!("No buffer with id {}", buffer_id))
                            })?;
                            if buffer.is_multibyte() {
                                ov.beg
                            } else {
                                buffer_position_to_byte(buffer, ov.beg).unwrap_or(ov.beg)
                            }
                        } else {
                            ov.beg
                        };
                        Ok(Value::Integer(pos as i64))
                    }
                    _ => Ok(Value::Nil),
                }
            }

            "overlay-end" => {
                need_args(name, args, 1)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                match interp.find_overlay(ov_id) {
                    Some(ov) if !ov.is_dead() => {
                        let pos = if let Some(buffer_id) = ov.buffer_id {
                            let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                                LispError::Signal(format!("No buffer with id {}", buffer_id))
                            })?;
                            if buffer.is_multibyte() {
                                ov.end
                            } else {
                                buffer_position_to_byte(buffer, ov.end).unwrap_or(ov.end)
                            }
                        } else {
                            ov.end
                        };
                        Ok(Value::Integer(pos as i64))
                    }
                    _ => Ok(Value::Nil),
                }
            }

            "move-overlay" => {
                // (move-overlay OVERLAY BEG END &optional BUFFER)
                if !(3..=4).contains(&args.len()) {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                let target_buffer_id = if let Some(buffer_arg) = args.get(3) {
                    if buffer_arg.is_nil() {
                        interp.current_buffer_id()
                    } else if matches!(buffer_arg, Value::Buffer(_)) {
                        interp.resolve_buffer_id(buffer_arg)?
                    } else {
                        return Err(LispError::TypeError(
                            "buffer".into(),
                            buffer_arg.type_name(),
                        ));
                    }
                } else {
                    interp.current_buffer_id()
                };
                let beg = position_from_value(interp, &args[1])? as i64;
                let end = position_from_value(interp, &args[2])? as i64;
                let (beg, end) = {
                    let buffer = interp.get_buffer_by_id(target_buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", target_buffer_id))
                    })?;
                    clamp_overlay_range(buffer, beg, end)
                };
                let mut overlay = take_overlay(interp, ov_id).unwrap_or_else(|| {
                    crate::overlay::Overlay::new(ov_id, beg, end, target_buffer_id, false, false)
                });
                overlay.beg = beg;
                overlay.end = end;
                overlay.buffer_id = Some(target_buffer_id);
                // buffer.c Fmove_overlay: an overlay left empty by the
                // move evaporates on the spot (rfn-eshadow's shadow
                // overlay parks empty between shadowed states).
                if overlay.beg == overlay.end
                    && overlay
                        .get_prop(&Value::Symbol("evaporate".into()))
                        .is_some_and(|value| value.is_truthy())
                {
                    overlay.buffer_id = None;
                }
                interp
                    .get_buffer_by_id_mut(target_buffer_id)
                    .expect("resolved live buffer id")
                    .overlays
                    .push(overlay);
                Ok(Value::Overlay(ov_id))
            }

            "delete-overlay" => {
                need_args(name, args, 1)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                if let Some(ov) = interp.find_overlay_mut(ov_id) {
                    ov.buffer_id = None;
                }
                Ok(Value::Nil)
            }

            "delete-all-overlays" => {
                // Remove all overlays (or mark them dead)
                interp.buffer.overlays.clear();
                Ok(Value::Nil)
            }

            "overlay-put" => {
                need_args(name, args, 3)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                let key = args[1].clone();
                let value = args[2].clone();
                if let Some(ov) = interp.find_overlay_mut(ov_id) {
                    ov.put_prop(key, value.clone());
                    // buffer.c Foverlay_put: giving an already-empty
                    // overlay the evaporate property deletes it on the
                    // spot (rfn-eshadow's shadow overlay starts life
                    // this way; move-overlay later revives it).
                    if matches!(&args[1], Value::Symbol(prop) if prop == "evaporate")
                        && value.is_truthy()
                        && ov.beg == ov.end
                    {
                        ov.buffer_id = None;
                    }
                }
                Ok(value)
            }

            "overlay-get" => {
                need_args(name, args, 2)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                let key = args[1].clone();
                match interp.find_overlay(ov_id) {
                    Some(ov) => {
                        if let Value::Symbol(name) = &key {
                            Ok(overlay_property_with_category(interp, ov, name)
                                .unwrap_or(Value::Nil))
                        } else {
                            Ok(ov.get_prop(&key).cloned().unwrap_or(Value::Nil))
                        }
                    }
                    None => Ok(Value::Nil),
                }
            }

            "overlay-properties" => {
                need_args(name, args, 1)?;
                let ov_id = match &args[0] {
                    Value::Overlay(id) => *id,
                    _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
                };
                match interp.find_overlay(ov_id) {
                    Some(ov) => {
                        let mut items = Vec::new();
                        for (k, v) in &ov.plist {
                            items.push(k.clone());
                            items.push(v.clone());
                        }
                        Ok(Value::list(items))
                    }
                    None => Ok(Value::Nil),
                }
            }

            "overlays-at" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                let mut overlays = interp
                    .buffer
                    .overlays
                    .iter()
                    .filter(|ov| !ov.is_dead() && ov.beg <= pos && pos < ov.end)
                    .collect::<Vec<_>>();
                overlays.sort_by_key(|overlay| std::cmp::Reverse(overlay.id));
                let result: Vec<Value> = overlays
                    .into_iter()
                    .map(|overlay| Value::Overlay(overlay.id))
                    .collect();
                Ok(Value::list(result))
            }

            "overlays-in" => {
                need_args(name, args, 2)?;
                let beg = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                // GNU treats the accessible end as the endpoint for empty
                // overlays.  After narrowing, an overlay at ZV is visible to
                // `overlays-in ZV ZV' even when it is not at the buffer's Z.
                let zv = interp.buffer.point_max();
                let mut overlays = interp
                    .buffer
                    .overlays
                    .iter()
                    .filter(|ov| {
                        if ov.is_dead() {
                            return false;
                        }
                        if ov.beg == ov.end {
                            // Zero-length overlay at pos P:
                            // Include if P is in [beg, end), or if beg==end and P==beg,
                            // or if P==end and end >= ZV (at the accessible end).
                            return ov.beg >= interp.buffer.point_min()
                                && ov.beg <= zv
                                && ((ov.beg >= beg && ov.beg < end)
                                    || (beg == end && ov.beg == beg)
                                    || (ov.beg == end && end >= zv));
                        }
                        // Non-empty overlay: include if it overlaps [beg, end)
                        ov.beg < end && ov.end > beg
                    })
                    .collect::<Vec<_>>();
                // GNU's interval tree enumerates ascending start positions
                // and, for equal starts, newest overlays first.  The public
                // order is documented as arbitrary but remains observable to
                // stable Lisp sorts such as diff-mode's overlay report.
                overlays.sort_by_key(|overlay| (overlay.beg, std::cmp::Reverse(overlay.id)));
                let result: Vec<Value> = overlays
                    .into_iter()
                    .map(|overlay| Value::Overlay(overlay.id))
                    .collect();
                Ok(Value::list(result))
            }

            "next-overlay-change" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                Ok(Value::Integer(
                    next_overlay_change_position(&interp.buffer, pos) as i64,
                ))
            }

            "previous-overlay-change" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                Ok(Value::Integer(
                    previous_overlay_change_position(&interp.buffer, pos) as i64,
                ))
            }

            "overlay-lists" => {
                // Returns (BEFORE-LIST . AFTER-LIST) relative to point.
                let pt = interp.buffer.point();
                let mut before = Vec::new();
                let mut after = Vec::new();
                for ov in &interp.buffer.overlays {
                    if ov.is_dead() {
                        continue;
                    }
                    if ov.end <= pt {
                        before.push(Value::Overlay(ov.id));
                    } else {
                        after.push(Value::Overlay(ov.id));
                    }
                }
                Ok(Value::cons(Value::list(before), Value::list(after)))
            }

            "overlay-recenter" => {
                // In real Emacs this recenters the overlay cache. We're a no-op.
                Ok(Value::Nil)
            }

            // GNU subr.el owns this high-level splitting policy.  Retain a
            // file-less bootstrap route, but never pin initialized batch
            // execution to this fallback (a loaded defun wins function-cell
            // resolution).
            "remove-overlays" => {
                // (remove-overlays &optional BEG END NAME VAL)
                let beg = if args.is_empty() || args[0].is_nil() {
                    interp.buffer.point_min()
                } else {
                    args[0].as_integer()? as usize
                };
                let end = if args.len() < 2 || args[1].is_nil() {
                    interp.buffer.point_max()
                } else {
                    args[1].as_integer()? as usize
                };
                let filter_name = args.get(2).cloned();
                let filter_val = args.get(3).cloned();
                let zv = interp.buffer.point_max();

                // Collect IDs to delete (fully contained or matching)
                let ids_to_delete: Vec<u64> = interp
                    .buffer
                    .overlays
                    .iter()
                    .filter(|ov| {
                        if ov.is_dead() {
                            return false;
                        }
                        // Check property filter
                        if let Some(ref fname) = filter_name {
                            let val = ov.get_prop(fname).cloned().unwrap_or(Value::Nil);
                            if let Some(ref fval) = filter_val
                                && !values_eql(&val, fval)
                            {
                                return false;
                            }
                        }
                        // Check containment
                        if ov.beg == ov.end {
                            // Zero-length: include if within range
                            ov.beg >= beg && (ov.beg < end || (ov.beg == end && end == zv))
                        } else {
                            ov.beg >= beg && ov.end <= end
                        }
                    })
                    .map(|ov| ov.id)
                    .collect();

                for id in &ids_to_delete {
                    if let Some(ov) = interp.find_overlay_mut(*id) {
                        ov.buffer_id = None;
                    }
                }
                interp
                    .buffer
                    .overlays
                    .retain(|ov| !ids_to_delete.contains(&ov.id));
                Ok(Value::Nil)
            }
        }
    }
);
