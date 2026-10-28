use super::*;
use crate::lisp::types::Kind;

pub(crate) fn clamp_overlay_range(
    buffer: &crate::buffer::Buffer,
    beg: i64,
    end: i64,
) -> (usize, usize) {
    let beg = if buffer.is_multibyte() {
        beg as usize
    } else {
        buffer_byte_to_position_boundary(buffer, beg.max(0) as usize).unwrap_or(1)
    } as i64;
    let end = if buffer.is_multibyte() {
        end as usize
    } else {
        buffer_byte_to_position_boundary(buffer, end.max(0) as usize).unwrap_or(1)
    } as i64;
    let min = 1;
    let max = (buffer.size_total() + 1) as i64;
    let clamp = |pos: i64| pos.clamp(min, max) as usize;
    let beg = clamp(beg);
    let end = clamp(end);
    if beg > end { (end, beg) } else { (beg, end) }
}

pub(crate) fn highest_priority_overlay_property(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: impl crate::lisp::types::PropertyKey,
    at_insertion_position: bool,
    window_id: Option<u64>,
) -> Option<Value> {
    highest_priority_overlay_property_with_id(
        interp,
        buffer,
        pos,
        prop,
        at_insertion_position,
        window_id,
    )
    .map(|(value, _)| value)
}

pub(crate) fn highest_priority_overlay_property_with_id(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: impl crate::lisp::types::PropertyKey,
    at_insertion_position: bool,
    window_id: Option<u64>,
) -> Option<(Value, crate::overlay::OverlayRef)> {
    let mut overlays: Vec<crate::overlay::OverlayRef> = buffer
        .overlays
        .intersecting(
            pos as isize - isize::from(at_insertion_position),
            pos as isize + 1,
            crate::overlay::Traversal::Ascending,
        )
        .filter(|overlay| overlay_covers_position(overlay, pos, at_insertion_position))
        .collect();
    sort_overlays(interp, &mut overlays, window_id);
    overlays.into_iter().rev().find_map(|overlay| {
        overlay_property_with_category(interp, &overlay, prop).map(|value| (value, overlay))
    })
}

/// buffer.c:next_overlay_change: the first overlay boundary after
/// POSITION, else ZV.
pub(crate) fn next_overlay_change_position(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> usize {
    let mut next = buffer.point_max();
    let mut iter = buffer.overlays.intersecting(
        position as isize,
        next as isize,
        crate::overlay::Traversal::Ascending,
    );
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

/// buffer.c:previous_overlay_change: the last overlay boundary before
/// POSITION, else BEGV.
pub(crate) fn previous_overlay_change_position(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> usize {
    let mut previous = buffer.point_min();
    let mut iter = buffer.overlays.intersecting(
        previous as isize,
        position as isize,
        crate::overlay::Traversal::Descending,
    );
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

pub(crate) fn sort_overlays(
    interp: &Interpreter,
    overlays: &mut Vec<crate::overlay::OverlayRef>,
    window_id: Option<u64>,
) {
    // buffer.c:compare_overlays combines interval nesting with secondary
    // priority; crossing intervals can make this relation non-transitive.
    // Rust's sort_by requires a total order and can panic here. Use the same
    // platform qsort as GNU, with a callback that cannot call Lisp or unwind.
    #[repr(C)]
    struct SortItem {
        overlay: crate::overlay::OverlayRef,
        beg: isize,
        end: isize,
        priority: i64,
        secondary: i64,
    }
    unsafe extern "C" fn compare(a: *const libc::c_void, b: *const libc::c_void) -> libc::c_int {
        // SAFETY: qsort supplies aligned elements of the live SortItem array.
        let (a, b) = unsafe { (&*a.cast::<SortItem>(), &*b.cast::<SortItem>()) };
        use std::cmp::Ordering::{Equal, Greater, Less};
        let order = if a.priority != b.priority {
            a.priority.cmp(&b.priority)
        } else if a.beg < b.beg {
            if a.end < b.end && a.secondary > b.secondary {
                Greater
            } else {
                Less
            }
        } else if a.beg > b.beg {
            if a.end > b.end && a.secondary < b.secondary {
                Less
            } else {
                Greater
            }
        } else if a.end != b.end {
            b.end.cmp(&a.end)
        } else if a.secondary != b.secondary {
            a.secondary.cmp(&b.secondary)
        } else {
            a.overlay.identity().cmp(&b.overlay.identity())
        };
        match order {
            Less => -1,
            Equal => 0,
            Greater => 1,
        }
    }
    let mut items: Vec<_> = overlays
        .iter()
        .copied()
        .filter(|overlay| {
            window_id.is_none_or(|window| {
                overlay_property_with_category(interp, overlay, "window")
                    .and_then(|value| window_record_id_from_value(interp, &value))
                    .is_none_or(|owner| owner == window)
            })
        })
        .map(|overlay| {
            let (priority, secondary) = match overlay_property_with_category(
                interp, &overlay, "priority",
            )
            .map(|v| v.kind())
            {
                Some(Kind::Integer(value)) => (value, 0),
                Some(Kind::Cons(cell)) => (
                    match cell.car.get().kind() {
                        Kind::Integer(v) => v,
                        _ => 0,
                    },
                    match cell.cdr.get().kind() {
                        Kind::Integer(v) => v,
                        _ => 0,
                    },
                ),
                _ => (0, 0),
            };
            let (beg, end) = overlay.bounds();
            SortItem {
                overlay,
                beg,
                end,
                priority,
                secondary,
            }
        })
        .collect();
    if items.len() > 1 {
        // SAFETY: the exclusive Vec borrow keeps this initialized, trivially
        // movable array alive. The comparator reads only its two elements;
        // no references survive a callback or any subsequent bytewise move.
        unsafe {
            libc::qsort(
                items.as_mut_ptr().cast(),
                items.len(),
                std::mem::size_of::<SortItem>(),
                Some(compare),
            );
        }
    }
    overlays.clear();
    overlays.extend(items.into_iter().map(|item| item.overlay));
}

pub(crate) fn overlay_covers_position(
    overlay: &crate::overlay::OverlayRef,
    pos: usize,
    at_insertion_position: bool,
) -> bool {
    let (beg, end) = overlay.bounds();
    let pos = pos as isize;
    if !at_insertion_position {
        return beg < end && beg <= pos && pos < end;
    }

    // `get-pos-property' asks whether a character inserted at POS would
    // belong to the overlay. Endpoint advancement controls that question.
    (beg < pos && pos < end)
        || (beg == pos && !overlay.front_advance() && pos < end)
        || (beg < pos && end == pos && overlay.rear_advance())
        || (beg == end && beg == pos && !overlay.front_advance() && overlay.rear_advance())
}

/// character.c:translate_char: C through TABLE, a char-table whose entry
/// for C replaces it when a character, or a list of tables applied in
/// turn; any other TABLE leaves C as it is.
pub(crate) fn translate_char(interp: &Interpreter, table: &Value, c: u32) -> u32 {
    match table.kind() {
        Kind::CharTable(id) => match interp.char_table_get(id, c).map(|value| value.kind()) {
            Some(Kind::Integer(ch)) if (0..=0x3F_FFFF).contains(&ch) => ch as u32,
            _ => c,
        },
        Kind::Cons(_) => {
            let mut c = c;
            let mut tail = *table;
            while let Kind::Cons(cell) = tail.kind() {
                c = translate_char(interp, &cell.car.get(), c);
                tail = cell.cdr.get();
            }
            c
        }
        _ => c,
    }
}

/// buffer.c:fix_position: a fixnum as itself, a marker as its position
/// (one pointing nowhere an error), a bignum as the most positive or
/// negative fixnum, anything else not an integer-or-marker.
pub(crate) fn fix_position(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(n),
        Kind::Marker(id) => interp
            .marker_position(id)
            .map(|pos| pos as i64)
            .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into())),
        Kind::BigInteger(n) => Ok(if n.sign() == num_bigint::Sign::Minus {
            -(1_i64 << 61)
        } else {
            (1_i64 << 61) - 1
        }),
        _ => Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            *value,
        )),
    }
}

/// buffer.c:validate_region: B and E as positions in order, within the
/// current buffer's accessible portion, or args-out-of-range with the
/// buffer and B and E as given.
pub(crate) fn validate_region(
    interp: &Interpreter,
    b: &Value,
    e: &Value,
) -> Result<(usize, usize), LispError> {
    let (beg, end) = (fix_position(interp, b)?, fix_position(interp, e)?);
    let (beg, end) = if end < beg { (end, beg) } else { (beg, end) };
    let (begv, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point_min() as i64, buffer.point_max() as i64)
    };
    if !(begv <= beg && end <= zv) {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            Value::Buffer(interp.buffer),
            *b,
            *e,
        ])));
    }
    Ok((beg as usize, end as usize))
}

pub(crate) fn position_from_value(interp: &Interpreter, value: &Value) -> Result<usize, LispError> {
    match value.kind() {
        // GNU clamps an ordinary fixnum position below point-min at the
        // consuming buffer operation (for example, `goto-char -10' reaches
        // point-min).  Preserve that signed boundary here rather than
        // misclassifying a negative integer as a non-position type.
        Kind::Integer(pos) => Ok((pos).max(0) as usize),
        // A small BigInteger can arise transiently inside Emaxx even though
        // GNU would represent the same numeric value as a fixnum.  Accept it
        // only when it fits the ordinary signed position domain; a genuine
        // out-of-range bignum retains GNU's integer-or-marker-p error.
        Kind::BigInteger(pos) => pos
            .to_i64()
            .map(|pos| pos.max(0) as usize)
            .ok_or_else(|| LispError::WrongTypeArgument("integer-or-marker-p".into(), *value)),
        Kind::Marker(id) => interp
            .marker_position(id)
            .ok_or_else(|| LispError::WrongTypeArgument("integer-or-marker-p".into(), *value)),
        _ => Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            *value,
        )),
    }
}

/// What `translate-region-internal' translates by: a translation
/// char-table, or a string whose Nth character maps the character with
/// code N.
pub(crate) enum TranslationTable {
    Chars(crate::lisp::types::CharTableRef),
    String(Value),
}

/// The characters character.c's CHAR_STRING writes for C in a multibyte
/// buffer: a raw byte two, others as their code's size.
fn multibyte_char_bytes(c: u32) -> usize {
    match c {
        0..0x80 => 1,
        0x80..0x800 => 2,
        0x800..0x10000 => 3,
        0x10000..0x200000 => 4,
        0x200000..0x3FFF80 => 5,
        _ => 2,
    }
}

/// editfns.c:Ftranslate_region_internal: modify_text over [FROM, TO),
/// then each character TABLE maps to another replaced, in place and
/// signalled as a change of its own when its encoding keeps its size,
/// otherwise by replace_range with its own hooks (as are the char-table's
/// sequence translations).  The characters changed.
pub(crate) fn translate_region_internal(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    table: TranslationTable,
    env: &mut Env,
) -> Result<Value, LispError> {
    let (from, to) = if from <= to { (from, to) } else { (to, from) };
    let multibyte = interp.buffer.borrow().is_multibyte();
    // A string table: its bytes bound the translatable codes, and a unibyte
    // buffer reads it as unibyte (string_make_unibyte).
    let string_table = match &table {
        TranslationTable::String(string) => {
            let Kind::StringObject(state) = string.kind() else {
                return Err(LispError::WrongTypeArgument("stringp".into(), *string));
            };
            let state = state.borrow();
            let codes: Vec<u32> = if state.is_multibyte() && multibyte {
                (0..state.len())
                    .map(|index| state.character_at(index).unwrap_or(0) as u32)
                    .collect()
            } else if state.is_multibyte() {
                (0..state.len())
                    .map(|index| {
                        let code = state.character_at(index).unwrap_or(0) as u32;
                        if code >= RAW_BYTE8_BASE {
                            code - RAW_BYTE8_BASE
                        } else {
                            code & 0xFF
                        }
                    })
                    .collect()
            } else {
                state.bytes().iter().map(|byte| u32::from(*byte)).collect()
            };
            let translatable = if state.is_multibyte() && multibyte {
                state.bytes().len()
            } else {
                codes.len()
            };
            Some((codes, state.is_multibyte() && multibyte, translatable))
        }
        TranslationTable::Chars(_) => None,
    };
    let prepared = prepare_to_modify_buffer(interp, from, to, false, env)?;
    with_overlay_hook_roots(interp, &prepared.overlay_calls, |interp| {
        let mut pos = from;
        let mut end_pos = to;
        let mut changed = 0_i64;
        while pos < end_pos.min(interp.buffer.borrow().point_max()) {
            let oc = public_buffer_char_code_at(interp, pos)
                .and_then(|code| u32::try_from(code).ok())
                .unwrap_or_default();
            let old_bytes = if multibyte {
                multibyte_char_bytes(oc)
            } else {
                1
            };
            // The simple translation's character and its encoding's size,
            // or a sequence: the characters it replaces and those it puts.
            let mut simple = None;
            let mut sequence = None;
            match (&table, &string_table) {
                (_, Some((codes, table_multibyte, translatable))) => {
                    if (oc as usize) < *translatable
                        && let Some(&code) = codes.get(oc as usize)
                    {
                        let (nc, bytes) = if *table_multibyte {
                            (code, multibyte_char_bytes(code))
                        } else if code >= 0x80 && multibyte {
                            (RAW_BYTE8_BASE + code, 2)
                        } else {
                            (code, 1)
                        };
                        simple = Some((nc, bytes));
                    }
                }
                (TranslationTable::Chars(table_id), None) => {
                    let mapping = interp.char_table_get(*table_id, oc);
                    match mapping.as_ref().map(|value| value.kind()) {
                        Some(Kind::Integer(code)) if code >= 0 => {
                            let nc = code as u32;
                            let bytes = if multibyte {
                                multibyte_char_bytes(nc)
                            } else {
                                1
                            };
                            simple = Some((nc, bytes));
                        }
                        Some(Kind::Cons(_)) => {
                            let source: Vec<u32> = (pos..end_pos)
                                .map(|position| {
                                    public_buffer_char_code_at(interp, position)
                                        .and_then(|code| u32::try_from(code).ok())
                                        .unwrap_or_default()
                                })
                                .collect();
                            sequence = mapping
                                .as_ref()
                                .and_then(|value| translation_sequence_match(value, &source));
                            if sequence.is_none() {
                                pos += 1;
                                continue;
                            }
                        }
                        Some(_) if mapping.as_ref().is_some_and(is_vector_value) => {
                            sequence = mapping
                                .as_ref()
                                .and_then(translation_characters)
                                .map(|characters| (1, characters));
                        }
                        _ => {}
                    }
                }
                (TranslationTable::String(_), None) => unreachable!("string table read above"),
            }
            if let Some((nc, new_bytes)) = simple
                && nc != oc
            {
                if new_bytes != old_bytes {
                    replace_range_with_hooks(interp, pos, pos + 1, &[nc], env)?;
                } else {
                    replace_character_in_place(interp, pos, nc)?;
                    signal_after_change(
                        interp,
                        AfterChange::new(pos, 1, 1),
                        &prepared.overlay_calls,
                        env,
                    )?;
                }
                changed += 1;
            } else if let Some((length, replacement)) = sequence {
                let inserted = replacement.len();
                replace_range_with_hooks(interp, pos, pos + length, &replacement, env)?;
                pos += inserted;
                changed += inserted as i64;
                end_pos = (end_pos + inserted).saturating_sub(length);
                continue;
            }
            pos += 1;
        }
        Ok(Value::Integer(changed))
    })
}

/// One character at POS replaced by NC where the text stands, its
/// properties and markers kept, the change recorded for undo.
fn replace_character_in_place(
    interp: &mut Interpreter,
    pos: usize,
    nc: u32,
) -> Result<(), LispError> {
    if let Some(character) = char::from_u32(nc)
        && raw_byte_from_regex_char(character).is_none()
        && interp.buffer.borrow().extended_char_at(pos).is_none()
    {
        let mut text = [0u8; 4];
        interp.buffer.borrow_mut().replace_region_in_place(
            pos,
            pos + 1,
            character.encode_utf8(&mut text),
            &[],
            false,
        );
        return Ok(());
    }
    // A raw byte, or a character the text view keeps in the extended
    // sidecar: replaced as deleted and inserted text.
    replace_characters_unsignalled(interp, pos, pos + 1, &[nc])
}

/// insdel.c:replace_range over [FROM, TO) by the characters REPLACEMENT,
/// with its own prepare_to_modify_buffer and signal_after_change; point
/// moves as a marker would.
pub(crate) fn replace_range_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    replacement: &[u32],
    env: &mut Env,
) -> Result<(), LispError> {
    let length = to - from;
    modify_current_buffer(interp, from, to, true, env, |interp, from, _| {
        let to = (from + length)
            .min(interp.buffer.borrow().point_max())
            .max(from);
        replace_characters_unsignalled(interp, from, to, replacement)?;
        Ok((
            Some(AfterChange::new(from, to - from, replacement.len())),
            (),
        ))
    })
}

fn replace_characters_unsignalled(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    replacement: &[u32],
) -> Result<(), LispError> {
    let multibyte = interp.buffer.borrow().is_multibyte();
    let mut text = String::new();
    let mut extended_chars = Vec::new();
    for (index, &character) in replacement.iter().enumerate() {
        push_buffer_character(&mut text, &mut extended_chars, index, character, multibyte);
    }
    let point = interp.buffer.borrow().point();
    interp
        .delete_region_current_buffer(from, to)
        .map_err(|e| LispError::Signal(e.to_string()))?;
    interp.buffer.borrow_mut().goto_char(from);
    interp.insert_current_buffer(&text);
    interp.set_inserted_extended_chars(from, &extended_chars);
    let inserted = replacement.len();
    let point = if point >= to {
        point + inserted - (to - from)
    } else if point > from {
        from
    } else {
        point
    };
    interp.buffer.borrow_mut().goto_char(point);
    Ok(())
}

fn translation_characters(value: &Value) -> Option<Vec<u32>> {
    if let Ok(character) = value.as_integer() {
        return u32::try_from(character)
            .ok()
            .filter(|character| *character <= 0x3F_FFFF)
            .map(|character| vec![character]);
    }
    let Kind::Vector(characters) = value.kind() else {
        return None;
    };
    characters
        .slots()
        .map(|character| {
            character
                .as_integer()
                .ok()
                .and_then(|character| u32::try_from(character).ok())
                .filter(|character| *character <= 0x3F_FFFF)
        })
        .collect()
}

fn translation_sequence_match(value: &Value, source: &[u32]) -> Option<(usize, Vec<u32>)> {
    for candidate in value.to_vec().ok()? {
        let Some((from, to)) = candidate.cons_values() else {
            continue;
        };
        let Some(from) = translation_characters(&from).filter(|from| !from.is_empty()) else {
            continue;
        };
        if source.starts_with(&from) {
            return translation_characters(&to).map(|to| (from.len(), to));
        }
    }
    None
}

pub(crate) fn marker_id_from_value(
    value: &Value,
) -> Result<crate::lisp::types::MarkerRef, LispError> {
    match value.kind() {
        Kind::Marker(id) => Ok(id),
        _ => Err(LispError::WrongTypeArgument("markerp".into(), *value)),
    }
}

/// marker.c:set_marker_internal decodes BUFFER before checking MARKER, then
/// detaches for a nil position, an unattached source marker, or a dead buffer.
pub(crate) fn set_marker_value(
    interp: &Interpreter,
    value: &Value,
    position: &Value,
    buffer: Option<&Value>,
) -> Result<Value, LispError> {
    let buffer = match buffer.map(|value| value.kind()) {
        None | Some(Kind::Nil) => interp.buffer,
        Some(Kind::Buffer(buffer)) => buffer,
        _ => {
            return Err(LispError::WrongTypeArgument(
                "bufferp".into(),
                *buffer.expect("the non-nil buffer argument was matched"),
            ));
        }
    };
    let marker = marker_id_from_value(value)?;
    if !interp
        .buffer_object(buffer.id)
        .is_some_and(|live| live.ptr_eq(&buffer))
        || position.is_nil()
        || matches!(position.kind(), Kind::Marker(source) if source.buffer().is_none())
    {
        marker.detach();
        return Ok(*value);
    }
    let state = buffer.borrow();
    let charpos = match position.kind() {
        Kind::Integer(position) => position.clamp(1, (state.size_total() + 1) as i64) as usize,
        Kind::Marker(source) => source.last_position().clamp(1, state.size_total() + 1),
        _ => {
            return Err(LispError::WrongTypeArgument(
                "integer-or-marker-p".into(),
                *position,
            ));
        }
    };
    let bytepos = match position.kind() {
        Kind::Marker(source) if source.buffer().is_some_and(|source| source.ptr_eq(&buffer)) => {
            source.bytepos()
        }
        _ => state.marker_byte_position(charpos),
    };
    marker.attach(buffer, charpos, bytepos);
    Ok(*value)
}

pub(crate) fn vector_items(value: &Value) -> Result<Vec<Value>, LispError> {
    if let Kind::Vector(vector) = value.kind() {
        Ok(vector.slots().collect::<Vec<_>>())
    } else {
        Err(LispError::WrongTypeArgument("vectorp".into(), *value))
    }
}

/// The elements of VALUE, a vector or a proper list.
pub(crate) fn vector_or_list_items(value: &Value) -> Result<Vec<Value>, LispError> {
    match value.kind() {
        Kind::Vector(vector) => Ok(vector.slots().collect()),
        _ => value.to_vec(),
    }
}

/// BOOL_VECTOR_P.
pub(crate) fn is_bool_vector_value(value: &Value) -> bool {
    matches!(value.kind(), Kind::BoolVector(_))
}

/// CHECK_BOOL_VECTOR.
pub(crate) fn bool_vector_arg(
    value: &Value,
) -> Result<crate::lisp::types::BoolVectorRef, LispError> {
    match value.kind() {
        Kind::BoolVector(vector) => Ok(vector),
        _ => Err(LispError::WrongTypeArgument("bool-vector-p".into(), *value)),
    }
}

/// Element read for the VM's Baref: bytecode.c's fast case, a vector or a
/// record (its type in slot zero, as PVSIZE counts it), and the string case
/// of the Faref it calls otherwise, which in range runs no Lisp and cannot
/// signal.  Char-tables, closures, and out-of-range accesses return None so
/// the caller takes the full `aref' path and preserves GNU's exact error
/// behavior.
pub(crate) fn vector_aref_fast(value: &Value, index: usize) -> Option<Value> {
    match value.kind() {
        Kind::Vector(vector) => vector.get(index),
        Kind::LispRecord(record) => record.get(index),
        Kind::StringObject(state) => state.borrow().character_at(index).map(Value::Integer),
        _ => None,
    }
}

/// Element write for the VM's Baset, same contract as [`vector_aref_fast`]:
/// a string takes Faset's stores that keep its size, into impure storage.
pub(crate) fn vector_aset_fast(value: &Value, index: usize, new_value: &Value) -> Option<()> {
    match value.kind() {
        Kind::StringObject(state) => {
            if state.is_pure() {
                return None;
            }
            let code = new_value
                .fixnum()
                .filter(|code| (0..=0x3f_ffff).contains(code))? as u32;
            state
                .borrow_mut()
                .store_character_in_place(index, code)
                .then_some(())
        }
        Kind::Vector(vector) => {
            if index >= vector.len() {
                return None;
            }
            vector.set(index, *new_value);
            Some(())
        }
        Kind::LispRecord(record) => record.set(index, *new_value).then_some(()),
        _ => None,
    }
}

pub(crate) fn vector_slot_value(value: &Value, index: usize) -> Result<Value, LispError> {
    let Kind::Vector(vector) = value.kind() else {
        return Err(LispError::WrongTypeArgument("vectorp".into(), *value));
    };
    vector.get(index).ok_or_else(|| {
        LispError::SignalValue(Value::list([
            Value::Symbol("args-out-of-range".into()),
            *value,
            Value::Integer(index as i64),
        ]))
    })
}

/// The elements of a bool vector, `bool_vector_ref' of each bit.
pub(crate) fn bool_vector_values(value: &Value) -> Result<Vec<Value>, LispError> {
    Ok(bool_vector_arg(value)?
        .bits()
        .map(|bit| if bit { Value::T } else { Value::Nil })
        .collect())
}

pub(crate) fn bool_vector_bits(value: &Value) -> Result<Vec<bool>, LispError> {
    Ok(bool_vector_arg(value)?.bits().collect())
}

/// A new bool vector of BITS.
pub(crate) fn make_bool_vector_value(bits: impl IntoIterator<Item = bool>) -> Value {
    let bits = bits.into_iter().collect::<Vec<_>>();
    Value::BoolVector(crate::lisp::types::BoolVectorRef::from_bits(
        bits.into_iter(),
    ))
}

pub(crate) const ABBREV_TABLE_RECORD_TYPE: &str = "abbrev-table";
pub(crate) const ABBREV_TABLE_ENTRIES_SLOT: usize = 2;

pub(crate) fn abbrev_table_record_id(interp: &Interpreter, value: &Value) -> Option<u64> {
    let Kind::Record(id) = value.kind() else {
        return None;
    };
    interp
        .find_record(id)
        .filter(|record| record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE))
        .map(|_| id.id)
}

pub(crate) fn is_abbrev_table_value(interp: &Interpreter, value: &Value) -> bool {
    if abbrev_table_record_id(interp, value).is_some() {
        return true;
    }

    // GNU abbrev.el implements tables as ordinary obarrays tagged by the
    // empty symbol's :abbrev-table-modiff property.  Emaxx's compact native
    // table record is the file-less fallback; once the GNU owner is loaded,
    // mode construction must recognize and preserve its real representation.
    obarray_symbols(interp, value).is_ok_and(|symbols| {
        symbols.into_iter().any(|symbol| {
            let Kind::Symbol(name) = symbol.kind() else {
                return false;
            };
            crate::lisp::types::visible_symbol_name(&name).is_empty()
                && matches!(
                    interp
                        .get_symbol_property(&name, ":abbrev-table-modiff")
                        .map(|v| v.kind()),
                    Some(Kind::Integer(_) | Kind::BigInteger(_))
                )
        })
    })
}

pub(crate) fn make_runtime_abbrev_table(
    interp: &mut Interpreter,
    name: Option<&str>,
    props: Value,
) -> Value {
    let props = abbrev_table_props_with_modiff(props);
    let table = interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Obarray,
        ABBREV_TABLE_RECORD_TYPE,
        vec![
            name.map(Value::symbol).unwrap_or(Value::Nil),
            props,
            Value::Nil,
        ],
    );
    if let Kind::Record(id) = table.kind() {
        let symbol = abbrev_symbol_name(id.id, "");
        interp.set_global_binding(&symbol, Value::Nil);
        let _ = interp.set_symbol_plist(&symbol, props);
    }
    table
}

pub(crate) fn abbrev_table_props_with_modiff(props: Value) -> Value {
    let mut pairs = plist_pairs(&props).unwrap_or_default();
    if !pairs.iter().any(|(key, _)| key == ":abbrev-table-modiff") {
        pairs.push((
            crate::lisp::types::PropertyName::symbol(":abbrev-table-modiff"),
            Value::Integer(0),
        ));
    }
    plist_value(&pairs)
}

pub(crate) fn abbrev_table_entries(
    interp: &Interpreter,
    table: &Value,
) -> Result<Vec<(String, Value, Value)>, LispError> {
    let Some(id) = abbrev_table_record_id(interp, table) else {
        return Err(LispError::TypeError(
            "abbrev-table".into(),
            table.type_name(),
        ));
    };
    let Some(record) = interp.find_record(id) else {
        return Err(LispError::TypeError(
            "abbrev-table".into(),
            table.type_name(),
        ));
    };
    let entries = record
        .slots
        .get(ABBREV_TABLE_ENTRIES_SLOT)
        .cloned()
        .unwrap_or(Value::Nil);
    let mut result = Vec::new();
    for entry in entries.to_vec()? {
        let parts = entry.to_vec()?;
        if parts.len() < 2 {
            continue;
        }
        let name = string_text(&parts[0])?;
        let expansion = parts[1];
        let props = parts.get(2).cloned().unwrap_or(Value::Nil);
        result.push((name, expansion, props));
    }
    Ok(result)
}

pub(crate) fn set_abbrev_table_entries(
    interp: &mut Interpreter,
    table: &Value,
    entries: Vec<(String, Value, Value)>,
) -> Result<(), LispError> {
    let Some(id) = abbrev_table_record_id(interp, table) else {
        return Err(LispError::TypeError(
            "abbrev-table".into(),
            table.type_name(),
        ));
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::TypeError(
            "abbrev-table".into(),
            table.type_name(),
        ));
    };
    if record.slots.len() <= ABBREV_TABLE_ENTRIES_SLOT {
        record
            .slots
            .resize(ABBREV_TABLE_ENTRIES_SLOT + 1, Value::Nil);
    }
    record.slots[ABBREV_TABLE_ENTRIES_SLOT] =
        Value::list(entries.iter().cloned().map(|(name, expansion, props)| {
            Value::list([Value::String(name.into()), expansion, props])
        }));
    for (entry_name, expansion, props) in entries {
        set_abbrev_symbol_state(interp, id, &entry_name, expansion, props)?;
    }
    Ok(())
}

pub(crate) fn abbrev_symbol_name(table_id: u64, name: &str) -> String {
    crate::lisp::types::make_obarray_symbol_name(name, table_id)
}

pub(crate) fn set_abbrev_symbol_state(
    interp: &mut Interpreter,
    table_id: u64,
    name: &str,
    expansion: Value,
    props: Value,
) -> Result<(), LispError> {
    let symbol = abbrev_symbol_name(table_id, name);
    let hook = abbrev_prop(&props, ":hook").unwrap_or(Value::Nil);
    interp.set_global_binding(&symbol, expansion);
    if hook.is_nil() {
        interp.set_function_binding(&symbol, None);
    } else {
        interp.set_function_binding(&symbol, Some(hook));
    }
    interp.set_symbol_plist(&symbol, props)?;
    Ok(())
}

pub(crate) fn abbrev_prop(props: &Value, key: &str) -> Option<Value> {
    plist_pairs(props)
        .ok()?
        .into_iter()
        .find_map(|(existing, value)| (existing == key).then_some(value))
}

pub(crate) fn define_abbrev_entry(
    interp: &mut Interpreter,
    table: &Value,
    name: &str,
    expansion: Value,
    props: Value,
) -> Result<(), LispError> {
    let mut entries = abbrev_table_entries(interp, table)?;
    entries.retain(|(existing, _, _)| existing != name);
    entries.insert(0, (name.to_string(), expansion, props));
    set_abbrev_table_entries(interp, table, entries)
}

pub(crate) fn ensure_standard_abbrev_tables(interp: &mut Interpreter) {
    for symbol in [
        "fundamental-mode-abbrev-table",
        "global-abbrev-table",
        "text-mode-abbrev-table",
    ] {
        if !interp
            .lookup_var(symbol, &crate::lisp::types::Env::new())
            .is_some_and(|value| is_abbrev_table_value(interp, &value))
        {
            let table = make_runtime_abbrev_table(interp, Some(symbol), Value::Nil);
            interp.set_global_binding(symbol, table);
        }
    }

    let existing = interp
        .lookup_var("abbrev-table-name-list", &crate::lisp::types::Env::new())
        .unwrap_or(Value::Nil);
    let mut items = existing.to_vec().unwrap_or_default();
    for symbol in [
        "fundamental-mode-abbrev-table",
        "global-abbrev-table",
        "text-mode-abbrev-table",
    ] {
        if !items.iter().any(|value| value.symbol_str() == Some(symbol)) {
            items.push(Value::Symbol(symbol.to_string().into()));
        }
    }
    interp.set_global_binding("abbrev-table-name-list", Value::list(items));
    if let Some(table) = interp.lookup_var(
        "fundamental-mode-abbrev-table",
        &crate::lisp::types::Env::new(),
    ) {
        interp.set_global_binding("local-abbrev-table", table);
    }
}

/// data.c:Fcl_type_of's PVEC_RECORD case: the type name field of the
/// class in slot 0 when that is a record of more than one slot, slot 0
/// itself otherwise.
#[inline]
pub(crate) fn record_type_of(record: crate::lisp::types::LispRecordRef) -> Value {
    let type_tag = record.type_tag();
    if let Kind::LispRecord(descriptor) = type_tag.kind()
        && let Some(name) = descriptor.get(1)
    {
        return name;
    }
    type_tag
}

pub(crate) fn cl_type_value(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    // data.c:Fcl_type_of uses XTYPE/PSEUDOVECTOR_TYPE, not Lisp variable
    // values. Wide Integer values cross the native ABI as bignums; an
    // explicitly allocated BigInteger already has that object subtype.
    let name = match value.kind() {
        Kind::Nil => "null",
        Kind::T => "boolean",
        Kind::Integer(number) => {
            if (MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM).contains(&number) {
                "fixnum"
            } else {
                "bignum"
            }
        }
        Kind::BigInteger(_) => "bignum",
        Kind::Float(_) => "float",
        Kind::StringObject(_) => "string",
        Kind::Symbol(_) => "symbol",
        Kind::Vector(_) => "vector",
        Kind::Cons(_) if is_vector_value(value) => "vector",
        Kind::Cons(_) => "cons",
        Kind::BuiltinFunc(name) if is_special_form_name(&name) => "special-form",
        Kind::BuiltinFunc(_) => "primitive-function",
        Kind::Closure(closure) => {
            if closure.is_bytecode() {
                "byte-code-function"
            } else {
                "interpreted-function"
            }
        }
        Kind::Buffer(_) => "buffer",
        Kind::Marker(_) => "marker",
        Kind::Overlay(_) => "overlay",
        Kind::CharTable(_) => "char-table",
        Kind::HashTable(_) => "hash-table",
        Kind::SubCharTable(_) => "sub-char-table",
        Kind::Frame(_) => "frame",
        Kind::Terminal(_) => "terminal",
        Kind::SymbolWithPos(_) => "symbol-with-pos",
        Kind::BoolVector(_) => "bool-vector",
        Kind::LispRecord(record) => return Ok(record_type_of(record)),
        Kind::Record(id) => {
            let Some(record) = interp.find_record(id) else {
                return Ok(Value::symbol("record"));
            };
            let type_name = match record.kind {
                crate::lisp::eval::RecordKind::Font => match record.symbol_type_name() {
                    Some("font-entity") => "font-entity",
                    Some("font-object") => "font-object",
                    _ => "font-spec",
                },
                crate::lisp::eval::RecordKind::Process => "process",
                crate::lisp::eval::RecordKind::Obarray => "obarray",
                crate::lisp::eval::RecordKind::Window => "window",
                crate::lisp::eval::RecordKind::WindowConfiguration => "window-configuration",
                crate::lisp::eval::RecordKind::Thread => "thread",
                crate::lisp::eval::RecordKind::Mutex => "mutex",
                crate::lisp::eval::RecordKind::ConditionVariable => "condition-variable",
                crate::lisp::eval::RecordKind::ModuleFunction => "module-function",
                crate::lisp::eval::RecordKind::UserPointer => "user-ptr",
                crate::lisp::eval::RecordKind::TreeSitterParser => "treesit-parser",
                crate::lisp::eval::RecordKind::TreeSitterNode => "treesit-node",
                crate::lisp::eval::RecordKind::TreeSitterCompiledQuery => "treesit-compiled-query",
                crate::lisp::eval::RecordKind::Sqlite => "sqlite",
            };
            return Ok(Value::symbol(type_name));
        }
        Kind::NativeFunction(_) => "native-comp-function",
        Kind::NativeCompUnit(_) => "native-comp-unit",
        Kind::Finalizer(_) => "finalizer",
        Kind::ReaderForm(_) => {
            return Err(LispError::Signal(
                "reader form escaped object materialization".into(),
            ));
        }
        Kind::Unbound => "unbound",
    };
    Ok(Value::symbol(name))
}

pub(crate) fn buffer_position_to_byte(buffer: &crate::buffer::Buffer, pos: usize) -> Option<usize> {
    buffer.position_bytes(pos)
}

pub(crate) fn buffer_byte_to_position(
    buffer: &crate::buffer::Buffer,
    byte: usize,
) -> Option<usize> {
    buffer.byte_to_position(byte)
}

pub(crate) fn buffer_byte_to_position_boundary(
    buffer: &crate::buffer::Buffer,
    byte: usize,
) -> Option<usize> {
    if byte == 0 {
        return None;
    }
    let text = buffer.buffer_string();
    let total_bytes = text.len();
    if byte > total_bytes + 1 {
        return None;
    }
    if byte == total_bytes + 1 {
        return Some(text.chars().count() + 1);
    }
    let mut current_byte = 1usize;
    for (index, ch) in text.chars().enumerate() {
        let next = current_byte + ch.len_utf8();
        if byte == current_byte {
            return Some(index + 1);
        }
        if byte < next {
            return Some(index + 2);
        }
        current_byte = next;
    }
    Some(text.chars().count() + 1)
}

pub(crate) fn char_table_range_spec(
    value: &Value,
    name: &str,
) -> Result<Option<(u32, u32)>, LispError> {
    let character = |value: Value| match value.kind() {
        Kind::Integer(code) if (0..=0x3f_ffff).contains(&code) => Ok(code as u32),
        _ => Err(LispError::WrongTypeArgument("characterp".into(), value)),
    };
    match value.kind() {
        Kind::Nil => Ok(None),
        Kind::Integer(code) if (0..=0x3f_ffff).contains(&code) => {
            Ok(Some((code as u32, code as u32)))
        }
        Kind::Cons(cell) => Ok(Some((
            character(cell.car.get())?,
            character(cell.cdr.get())?,
        ))),
        _ => Err(LispError::Signal(format!(
            "Invalid RANGE argument to `{name}'"
        ))),
    }
}

pub(crate) fn normalize_string_index(
    arg: Option<&Value>,
    default: i64,
    len: i64,
) -> Result<i64, LispError> {
    let Some(value) = arg else {
        return Ok(default);
    };
    if value.is_nil() {
        return Ok(default);
    }
    let raw = value.as_integer()?;
    let index = if raw < 0 { len + raw } else { raw };
    if !(0..=len).contains(&index) {
        return Err(LispError::Signal("Args out of range".into()));
    }
    Ok(index)
}

pub(crate) fn resolve_char_modifiers(value: i64) -> i64 {
    const SHIFT_BIT: i64 = 1 << 25;
    const CTRL_BIT: i64 = 1 << 26;
    const META_BIT: i64 = 1 << 27;
    const CHAR_MASK: i64 = 0x3F_FFFF;

    let mut base = value & CHAR_MASK;
    let meta = value & META_BIT;
    let shift = value & SHIFT_BIT != 0;
    let ctrl = value & CTRL_BIT != 0;

    if shift
        && let Some(ch) = char::from_u32(base as u32)
        && ch.is_ascii_lowercase()
    {
        base = ch.to_ascii_uppercase() as i64;
    }

    if ctrl {
        base = match base {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    }

    base | meta
}

pub(crate) fn position_bytes(interp: &Interpreter, pos: usize) -> Option<usize> {
    buffer_position_to_byte(&interp.buffer.borrow(), pos)
}

pub(crate) fn byte_to_position(interp: &Interpreter, byte: usize) -> Option<usize> {
    buffer_byte_to_position(&interp.buffer.borrow(), byte)
}

/// find_newline backward from POS to BEGV: the beginning of POS's line.
pub(crate) fn line_beginning_at(interp: &Interpreter, pos: usize) -> usize {
    let buffer = interp.buffer.borrow();
    let begv = buffer.point_min();
    let mut at = pos.clamp(begv, buffer.point_max());
    while at > begv && buffer.char_at(at - 1) != Some('\n') {
        at -= 1;
    }
    at
}

/// The columns from LINE_START, the beginning of a line, to POS
/// (indent.c current_column_1).
pub(crate) fn column_at(
    interp: &mut Interpreter,
    env: &mut Env,
    line_start: usize,
    pos: usize,
) -> usize {
    scan_for_column(interp, env, line_start, pos, None).col
}

/// What indent.c scan_for_column leaves.
pub(crate) struct ColumnScan {
    /// The column reached.
    pub(crate) col: usize,
    /// Where the scan stopped: END, the line's end, or the first position
    /// at GOAL's column.
    pub(crate) pos: usize,
    /// The position and column before the last character or display
    /// replacement scanned.
    pub(crate) prev_pos: usize,
    pub(crate) prev_col: usize,
}

/// indent.c scan_for_column from LINE_START, the beginning of a line:
/// forward to END or the line's end, or until GOAL columns are reached
/// once invisible text there is skipped (text whose invisibility shows
/// an ellipsis still counts).  A `display' string counts its
/// `string-width' and a (space ...) spec its width for the text it
/// covers (check_display_width); a display table's glyph vector counts
/// its glyphs.
pub(crate) fn scan_for_column(
    interp: &mut Interpreter,
    env: &mut Env,
    line_start: usize,
    end: usize,
    goal: Option<usize>,
) -> ColumnScan {
    let goal = goal.unwrap_or(usize::MAX);
    // SANE_TAB_WIDTH.
    let tab_width = interp
        .lookup_var_key(cached_symbol!("tab-width"), env)
        .and_then(|value| value.integer_value())
        .filter(|width| (1..=1000).contains(width))
        .unwrap_or(8) as usize;
    let ctl_arrow = interp
        .lookup_var_key(cached_symbol!("ctl-arrow"), env)
        .is_some_and(|value| value.is_truthy());
    let selective_display = interp
        .lookup_var_key(cached_symbol!("selective-display"), env)
        .is_some_and(|value| matches!(value.kind(), Kind::T));
    let multibyte = interp.buffer.borrow().is_multibyte();
    let display_table = buffer_display_table(interp, env);
    let end = end.min(interp.buffer.borrow().point_max());
    let spec = {
        let buffer = interp.buffer.borrow();
        crate::lisp::primitives::resolve_buffer_invisibility_in(
            interp,
            &buffer,
            interp.current_buffer_id(),
            line_start,
            end,
        )
    };
    let tab = |col: usize| (col + tab_width) / tab_width * tab_width;
    let mut col = 0usize;
    let mut prev_col = 0usize;
    let mut scan = line_start;
    let mut prev_pos = scan;
    'scan: while scan < end {
        // skip_invisible with no window: invisible text, not the text an
        // ellipsis stands for.
        while scan < end && invisible_class_at(&interp.buffer.borrow(), &spec, scan) == 1 {
            scan += 1;
        }
        if scan >= end {
            break;
        }
        // The goal, tested after the invisible text, so point goes before
        // the character the cursor will appear on.
        if col >= goal {
            break;
        }
        prev_col = col;
        prev_pos = scan;
        if let Some((width, display_end)) = check_display_width(interp, env, scan, col) {
            col += width;
            if display_end > scan {
                scan = display_end;
                continue;
            }
        }
        let Some(ch) = interp.buffer.borrow().char_at(scan) else {
            break;
        };
        let glyphs = display_table
            .filter(|_| !(multibyte && ch > '\u{7f}'))
            .and_then(|table| interp.char_table_get(table, ch as u32))
            .filter(|entry| matches!(entry.kind(), Kind::Vector(_)));
        if let Some(glyphs) = glyphs {
            for glyph in vector_items(&glyphs).unwrap_or_default() {
                let code = match glyph.kind() {
                    Kind::Integer(code) => Some(code & 0x3F_FFFF),
                    _ => glyph
                        .cons_values()
                        .and_then(|(code, _)| code.integer_value()),
                };
                match code
                    .and_then(|code| char::from_u32(code as u32))
                    .unwrap_or(' ')
                {
                    '\n' => break 'scan,
                    '\r' if selective_display => break 'scan,
                    '\t' => col = tab(col),
                    _ => col += 1,
                }
            }
        } else {
            match ch {
                '\n' => break,
                '\r' if selective_display => break,
                '\t' => col = tab(col),
                _ if multibyte && ch > '\u{7f}' => {
                    col += multibyte_char_width(interp, env, ch, display_table);
                }
                _ if ctl_arrow && (ch < ' ' || ch == '\u{7f}') => col += 2,
                _ if !(' '..'\u{7f}').contains(&ch) => col += 4,
                _ => col += 1,
            }
        }
        scan += 1;
    }
    ColumnScan {
        col,
        pos: scan,
        prev_pos,
        prev_col,
    }
}

/// MULTIBYTE_BYTES_WIDTH: a display table's glyph count for CH, else
/// its `char-width-table' width (buffer.h sanitize_char_width).
fn multibyte_char_width(
    interp: &Interpreter,
    env: &Env,
    ch: char,
    display_table: Option<crate::lisp::types::CharTableRef>,
) -> usize {
    if let Some(entry) = display_table
        .and_then(|table| interp.char_table_get(table, ch as u32))
        .filter(|entry| matches!(entry.kind(), Kind::Vector(_)))
    {
        return vector_items(&entry)
            .map_or(0, |items| items.len())
            .min(1000);
    }
    let width = interp
        .lookup_var("char-width-table", env)
        .and_then(|value| match value.kind() {
            Kind::CharTable(table) => interp.char_table_get(table, ch as u32),
            _ => None,
        })
        .and_then(|value| value.integer_value())
        .unwrap_or(0);
    if (0..=1000).contains(&width) {
        width as usize
    } else {
        1000
    }
}

/// indent.c check_display_width: the columns a `display' property at POS
/// takes -- a (space ...) spec's :width, :relative-width or :align-to, a
/// string's `string-width' -- and the end of the text it covers (its
/// overlay's end, or its text property's range).
fn check_display_width(
    interp: &mut Interpreter,
    env: &mut Env,
    pos: usize,
    col: usize,
) -> Option<(usize, usize)> {
    let (value, overlay) = get_char_property_and_overlay(
        interp,
        &Value::Integer(pos as i64),
        crate::lisp::types::PropertyName::new(Value::symbol("display")),
        Value::Nil,
    )
    .ok()?;
    if value.is_nil() {
        return None;
    }
    const INT_MAX: i64 = i32::MAX as i64;
    let plist_get = |plist: Value, key: &str| -> Value {
        let mut tail = plist;
        while let Some((name, rest)) = tail.cons_values() {
            let Some((item, next)) = rest.cons_values() else {
                break;
            };
            if name.symbol_str() == Some(key) {
                return item;
            }
            tail = next;
        }
        Value::Nil
    };
    let ranged = |value: Value, low: i64, high: i64| {
        value.integer_value().filter(|n| (low..=high).contains(n))
    };
    let float = |value: Value| match value.kind() {
        Kind::Float(f) => Some(f.get()),
        _ => None,
    };
    let mut plist = Value::Nil;
    let width = match value.cons_values() {
        Some((head, tail)) if head.symbol_str() == Some("space") => {
            plist = tail;
            let col = col as i64;
            let align_to_max = col.saturating_add(INT_MAX);
            let relative = plist_get(plist, ":relative-width");
            if let Some(width) = ranged(plist_get(plist, ":width"), 0, INT_MAX)
                .or_else(|| ranged(relative, 0, INT_MAX))
            {
                width
            } else if let Some(f) = float(relative).filter(|f| (0.0..=INT_MAX as f64).contains(f)) {
                (f + 0.5) as i64
            } else {
                let align = plist_get(plist, ":align-to");
                if let Some(to) = ranged(align, col, align_to_max) {
                    to - col
                } else if let Some(f) =
                    float(align).filter(|f| col as f64 <= *f && *f <= align_to_max as f64)
                {
                    (f + 0.5) as i64 - col
                } else {
                    -1
                }
            }
        }
        _ if value.is_string() => super::dispatch::call(interp, "string-width", &[value], env)
            .ok()
            .and_then(|width| width.integer_value())
            .unwrap_or(-1),
        _ => -1,
    };
    if width < 0 {
        return None;
    }
    let end = match overlay {
        Some(overlay) => overlay.end(),
        None => text_property_range_end(&interp.buffer.borrow(), pos, "display"),
    };
    let mut width = width as usize;
    if !plist_get(plist, ":relative-width").is_nil() && interp.buffer.borrow().is_multibyte() {
        // The column width of the character at POS, when above one.
        if let Some(ch) = interp.buffer.borrow().char_at(pos) {
            let char_width = if ch > '\u{7f}' {
                let table = buffer_display_table(interp, env);
                multibyte_char_width(interp, env, ch, table)
            } else {
                1
            };
            width *= char_width;
        }
    }
    Some((width, end))
}

/// get_property_and_range's end: past the text property intervals from
/// POS on whose PROP is EQ to POS's.
fn text_property_range_end(buffer: &crate::buffer::Buffer, pos: usize, prop: &str) -> usize {
    let zv = buffer.point_max();
    let initial = buffer.text_property_at(pos, prop).map(|value| value.word());
    let mut at = pos;
    loop {
        let (_, interval_end) = buffer.text_property_interval_around(at);
        let next = interval_end.max(at + 1);
        if next >= zv
            || buffer
                .text_property_at(next, prop)
                .map(|value| value.word())
                != initial
        {
            return next.min(zv);
        }
        at = next;
    }
}

pub(crate) fn char_is_invisible(interp: &Interpreter, pos: usize, env: &Env) -> bool {
    let value = buffer_char_property_at(interp, &interp.buffer.borrow(), pos, "invisible");
    invisibility_value_is_hidden(interp, &value, env)
}

pub(crate) fn invisibility_value_is_hidden(interp: &Interpreter, value: &Value, env: &Env) -> bool {
    let spec = interp
        .lookup_var("buffer-invisibility-spec", env)
        .unwrap_or(Value::T);
    invisibility_spec_matches(&spec, value)
}

pub(crate) fn invisibility_spec_matches(spec: &Value, value: &Value) -> bool {
    if value.is_nil() {
        return false;
    }
    match spec.kind() {
        Kind::Nil => false,
        Kind::T => value.is_truthy(),
        _ => {
            if spec == value {
                return true;
            }
            if let Some((car, _)) = spec.cons_values()
                && car == *value
            {
                return true;
            }
            if let Ok(items) = spec.to_vec() {
                if items.iter().any(|entry| entry == value) {
                    return true;
                }
                if items
                    .iter()
                    .any(|entry| entry.cons_values().is_some_and(|(car, _)| car == *value))
                {
                    return true;
                }
            }
            if let Ok(items) = value.to_vec() {
                return items
                    .into_iter()
                    .any(|item| invisibility_spec_matches(spec, &item));
            }
            false
        }
    }
}

pub(crate) fn compare_buffer_substrings(
    mut left_chars: impl Iterator<Item = char>,
    mut right_chars: impl Iterator<Item = char>,
    mut canonicalize: impl FnMut(char) -> u32,
) -> i64 {
    let mut matched = 0i64;

    loop {
        match (left_chars.next(), right_chars.next()) {
            (Some(left), Some(right)) => {
                let left = canonicalize(left);
                let right = canonicalize(right);
                if left != right {
                    return if left < right {
                        -matched - 1
                    } else {
                        matched + 1
                    };
                }
                matched += 1;
            }
            (Some(_), None) => return matched + 1,
            (None, Some(_)) => return -matched - 1,
            (None, None) => return 0,
        }
    }
}

pub(crate) fn prefix_numeric_value(value: &Value) -> Result<Value, LispError> {
    Ok(match value.kind() {
        Kind::Nil => Value::Integer(1),
        Kind::Symbol(symbol) if symbol == "-" => Value::Integer(-1),
        Kind::Integer(_) => *value,
        Kind::Cons(_) => value
            .cons_values()
            .and_then(|(head, _)| matches!(head.kind(), Kind::Integer(_)).then_some(head))
            .unwrap_or(Value::Integer(1)),
        // GNU accepts any Lisp object here.  Values outside the raw-prefix
        // representation, including floats and bignums, have numeric meaning
        // 1 rather than signaling a type error.
        _ => Value::Integer(1),
    })
}
