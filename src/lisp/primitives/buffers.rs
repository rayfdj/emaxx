use super::*;

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
    let min = buffer.point_min() as i64;
    let max = buffer.point_max() as i64;
    let clamp = |pos: i64| pos.clamp(min, max) as usize;
    let beg = clamp(beg);
    let end = clamp(end);
    if beg > end { (end, beg) } else { (beg, end) }
}

pub(crate) fn take_overlay(
    interp: &mut Interpreter,
    overlay_id: u64,
) -> Option<crate::overlay::Overlay> {
    interp.take_overlay(overlay_id)
}

pub(crate) fn highest_priority_overlay_property(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
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
    prop: &str,
    at_insertion_position: bool,
    window_id: Option<u64>,
) -> Option<(Value, u64)> {
    let mut overlays: Vec<&crate::overlay::Overlay> = buffer
        .overlays
        .iter()
        .filter(|overlay| {
            !overlay.is_dead()
                && overlay_covers_position(overlay, pos, at_insertion_position)
                && window_id.is_none_or(|window_id| {
                    // GNU's `overlay_matches_window' treats a `window'
                    // property as restrictive only when its value is a
                    // window.  Other values leave the overlay visible in
                    // every window.
                    overlay_property_with_category(interp, overlay, "window")
                        .and_then(|window| window_record_id_from_value(interp, &window))
                        .is_none_or(|overlay_window_id| overlay_window_id == window_id)
                })
        })
        .collect();
    overlays.sort_by(|a, b| {
        a.priority()
            .cmp(&b.priority())
            .then_with(|| a.id.cmp(&b.id))
    });
    overlays.into_iter().rev().find_map(|overlay| {
        overlay_property_with_category(interp, overlay, prop).map(|value| (value, overlay.id))
    })
}

pub(crate) fn overlay_covers_position(
    overlay: &crate::overlay::Overlay,
    pos: usize,
    at_insertion_position: bool,
) -> bool {
    if !at_insertion_position {
        return overlay.beg < overlay.end && overlay.beg <= pos && pos < overlay.end;
    }

    // `get-pos-property' asks whether a character inserted at POS would
    // belong to the overlay.  Endpoint advancement controls that question:
    // front-advancing excludes the beginning, rear-advancing includes the
    // end.  An empty overlay covers an insertion only when both endpoint
    // rules agree.
    (overlay.beg < pos && pos < overlay.end)
        || (overlay.beg == pos && !overlay.front_advance && pos < overlay.end)
        || (overlay.beg < pos && overlay.end == pos && overlay.rear_advance)
        || (overlay.beg == overlay.end
            && overlay.beg == pos
            && !overlay.front_advance
            && overlay.rear_advance)
}

pub(crate) fn position_from_value(interp: &Interpreter, value: &Value) -> Result<usize, LispError> {
    match value {
        // GNU clamps an ordinary fixnum position below point-min at the
        // consuming buffer operation (for example, `goto-char -10' reaches
        // point-min).  Preserve that signed boundary here rather than
        // misclassifying a negative integer as a non-position type.
        Value::Integer(pos) => Ok((*pos).max(0) as usize),
        // A small BigInteger can arise transiently inside Emaxx even though
        // GNU would represent the same numeric value as a fixnum.  Accept it
        // only when it fits the ordinary signed position domain; a genuine
        // out-of-range bignum retains GNU's integer-or-marker-p error.
        Value::BigInteger(pos) => pos.to_i64().map(|pos| pos.max(0) as usize).ok_or_else(|| {
            LispError::WrongTypeArgument("integer-or-marker-p".into(), value.clone())
        }),
        Value::Marker(id) => interp.marker_position(*id).ok_or_else(|| {
            LispError::WrongTypeArgument("integer-or-marker-p".into(), value.clone())
        }),
        _ => Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            value.clone(),
        )),
    }
}

pub(crate) fn translate_region_with_table(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    table_id: u64,
) -> Result<Value, LispError> {
    let source = (from..to)
        .map(|position| {
            public_buffer_char_code_at(interp, position)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let mut translated = String::new();
    let mut translated_props = Vec::new();
    let mut translated_extended_chars = Vec::new();
    let mut changed = 0i64;
    let mut index = 0usize;
    while index < source.len() {
        let source_char = source[index];
        let mapping = interp.char_table_get(table_id, source_char);
        let mut consumed = 1usize;
        let replacement = mapping.as_ref().and_then(|value| {
            if let Ok(character) = value.as_integer() {
                let character = u32::try_from(character).ok()?;
                return (character != source_char).then_some(vec![character]);
            }
            translation_characters(value).or_else(|| {
                translation_sequence_match(value, &source[index..]).map(
                    |(matched_length, replacement)| {
                        consumed = matched_length;
                        replacement
                    },
                )
            })
        });
        match replacement {
            Some(replacement) => {
                changed += replacement.len() as i64;
                for character in replacement {
                    append_public_buffer_character(
                        &mut translated,
                        &mut translated_props,
                        &mut translated_extended_chars,
                        character,
                    );
                }
            }
            None => {
                append_public_buffer_character(
                    &mut translated,
                    &mut translated_props,
                    &mut translated_extended_chars,
                    source_char,
                );
            }
        }
        index += consumed;
    }
    interp
        .delete_region_current_buffer(from, to)
        .map_err(|e| LispError::Signal(e.to_string()))?;
    interp.buffer.goto_char(from);
    interp.insert_current_buffer(&translated);
    interp.set_inserted_extended_chars(from, &translated_extended_chars);
    for span in translated_props {
        interp
            .buffer
            .set_text_properties(from + span.start, from + span.end, &span.props);
    }
    Ok(Value::Integer(changed))
}

fn append_public_buffer_character(
    text: &mut String,
    _props: &mut Vec<TextPropertySpan>,
    extended_chars: &mut Vec<(usize, u32)>,
    character: u32,
) {
    if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xff).contains(&character) {
        text.push(raw_byte_regex_char((character - RAW_BYTE8_BASE) as u8));
    } else if let Some(character) = char::from_u32(character) {
        text.push(character);
    } else if character <= 0x3f_ffff {
        let offset = text.chars().count();
        text.push(RAW_CHAR_SENTINEL);
        extended_chars.push((offset, character));
    }
}

fn translation_characters(value: &Value) -> Option<Vec<u32>> {
    if let Ok(character) = value.as_integer() {
        return u32::try_from(character)
            .ok()
            .filter(|character| char::from_u32(*character).is_some())
            .map(|character| vec![character]);
    }
    let characters = vector_items(value).ok()?;
    characters
        .iter()
        .map(|character| {
            character
                .as_integer()
                .ok()
                .and_then(|character| u32::try_from(character).ok())
                .filter(|character| char::from_u32(*character).is_some())
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

pub(crate) fn marker_id_from_value(value: &Value) -> Result<u64, LispError> {
    match value {
        Value::Marker(id) => Ok(*id),
        _ => Err(LispError::WrongTypeArgument(
            "markerp".into(),
            value.clone(),
        )),
    }
}

pub(crate) fn marker_target(
    interp: &Interpreter,
    value: &Value,
    buffer: Option<&Value>,
) -> Result<(Option<usize>, Option<u64>), LispError> {
    match value {
        Value::Nil => Ok((None, None)),
        Value::Marker(marker_id) => Ok((
            interp.marker_position(*marker_id),
            interp.marker_buffer_id(*marker_id),
        )),
        _ => {
            let position = position_from_value(interp, value)?;
            let buffer_id = if let Some(buffer) = buffer {
                if buffer.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(buffer)?
                }
            } else {
                interp.current_buffer_id()
            };
            Ok((Some(position), Some(buffer_id)))
        }
    }
}

pub(crate) fn vector_items(value: &Value) -> Result<Vec<Value>, LispError> {
    if let Value::Vector(vector) = value {
        Ok(vector.slots().clone())
    } else {
        Err(LispError::WrongTypeArgument(
            "vectorp".into(),
            value.clone(),
        ))
    }
}

pub(crate) fn record_type_name<'a>(interp: &'a Interpreter, value: &Value) -> Option<&'a str> {
    let Value::Record(id) = value else {
        return None;
    };
    interp
        .find_record(*id)
        .and_then(|record| record.symbol_type_name())
}

pub(crate) fn is_bool_vector_value(interp: &Interpreter, value: &Value) -> bool {
    let Value::Record(id) = value else {
        return false;
    };
    interp
        .find_record(*id)
        .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::BoolVector)
}

/// O(1) element read for the VM's Baref.  Strings, char-tables, records,
/// closures, and out-of-range accesses return None so the caller takes the
/// full `aref' path and preserves GNU's exact error behavior.
pub(crate) fn vector_aref_fast(value: &Value, index: usize) -> Option<Value> {
    let Value::Vector(vector) = value else {
        return None;
    };
    vector.slots().get(index).cloned()
}

/// O(1) element write for the VM's Baset, same contract as
/// [`vector_aref_fast`].
pub(crate) fn vector_aset_fast(value: &Value, index: usize, new_value: &Value) -> Option<()> {
    let Value::Vector(vector) = value else {
        return None;
    };
    let mut slots = vector.slots_mut();
    let slot = slots.get_mut(index)?;
    *slot = new_value.clone();
    Some(())
}

pub(crate) fn vector_slot_value(value: &Value, index: usize) -> Result<Value, LispError> {
    let Value::Vector(vector) = value else {
        return Err(LispError::WrongTypeArgument(
            "vectorp".into(),
            value.clone(),
        ));
    };
    vector.slots().get(index).cloned().ok_or_else(|| {
        LispError::SignalValue(Value::list([
            Value::Symbol("args-out-of-range".into()),
            value.clone(),
            Value::Integer(index as i64),
        ]))
    })
}

pub(crate) fn bool_vector_values(
    interp: &Interpreter,
    value: &Value,
) -> Result<Vec<Value>, LispError> {
    let Value::Record(id) = value else {
        return Err(LispError::WrongTypeArgument(
            "bool-vector-p".into(),
            value.clone(),
        ));
    };
    let record = interp
        .find_record(*id)
        .ok_or_else(|| LispError::WrongTypeArgument("bool-vector-p".into(), value.clone()))?;
    if record.kind != crate::lisp::eval::RecordKind::BoolVector {
        return Err(LispError::WrongTypeArgument(
            "bool-vector-p".into(),
            value.clone(),
        ));
    }
    Ok(record.slots.clone())
}

pub(crate) fn bool_vector_bits(
    interp: &Interpreter,
    value: &Value,
) -> Result<Vec<bool>, LispError> {
    Ok(bool_vector_values(interp, value)?
        .into_iter()
        .map(|slot| slot.is_truthy())
        .collect())
}

pub(crate) fn make_bool_vector_value(
    interp: &mut Interpreter,
    bits: impl IntoIterator<Item = bool>,
) -> Value {
    interp.create_pseudovector(
        crate::lisp::eval::RecordKind::BoolVector,
        "bool-vector",
        bits.into_iter()
            .map(|bit| if bit { Value::T } else { Value::Nil })
            .collect(),
    )
}

pub(crate) const ABBREV_TABLE_RECORD_TYPE: &str = "abbrev-table";
pub(crate) const ABBREV_TABLE_ENTRIES_SLOT: usize = 2;

pub(crate) fn set_bool_vector_bit(
    interp: &mut Interpreter,
    value: &Value,
    index: usize,
    bit: bool,
) -> Result<(), LispError> {
    let Value::Record(id) = value else {
        return Err(LispError::WrongTypeArgument(
            "bool-vector-p".into(),
            value.clone(),
        ));
    };
    let record = interp
        .find_record_mut(*id)
        .ok_or_else(|| LispError::WrongTypeArgument("bool-vector-p".into(), value.clone()))?;
    if record.kind != crate::lisp::eval::RecordKind::BoolVector {
        return Err(LispError::WrongTypeArgument(
            "bool-vector-p".into(),
            value.clone(),
        ));
    }
    if index >= record.slots.len() {
        return Err(LispError::Signal("Args out of range".into()));
    }
    record.slots[index] = if bit { Value::T } else { Value::Nil };
    Ok(())
}

pub(crate) fn abbrev_table_record_id(interp: &Interpreter, value: &Value) -> Option<u64> {
    let Value::Record(id) = value else {
        return None;
    };
    interp
        .find_record(*id)
        .filter(|record| record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE))
        .map(|_| *id)
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
            let Value::Symbol(name) = symbol else {
                return false;
            };
            crate::lisp::types::visible_symbol_name(&name).is_empty()
                && matches!(
                    interp.get_symbol_property(&name, ":abbrev-table-modiff"),
                    Some(Value::Integer(_) | Value::BigInteger(_))
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
            props.clone(),
            Value::Nil,
        ],
    );
    if let Value::Record(id) = table {
        let symbol = abbrev_symbol_name(id, "");
        interp.set_global_binding(&symbol, Value::Nil);
        let _ = interp.set_symbol_plist(&symbol, props);
    }
    table
}

pub(crate) fn abbrev_table_props_with_modiff(props: Value) -> Value {
    let mut pairs = plist_pairs(&props).unwrap_or_default();
    if !pairs.iter().any(|(key, _)| key == ":abbrev-table-modiff") {
        pairs.push((":abbrev-table-modiff".into(), Value::Integer(0)));
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
        let expansion = parts[1].clone();
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
            .lookup_var(symbol, &Vec::new())
            .is_some_and(|value| is_abbrev_table_value(interp, &value))
        {
            let table = make_runtime_abbrev_table(interp, Some(symbol), Value::Nil);
            interp.set_global_binding(symbol, table);
        }
    }

    let existing = interp
        .lookup_var("abbrev-table-name-list", &Vec::new())
        .unwrap_or(Value::Nil);
    let mut items = existing.to_vec().unwrap_or_default();
    for symbol in [
        "fundamental-mode-abbrev-table",
        "global-abbrev-table",
        "text-mode-abbrev-table",
    ] {
        if !items
            .iter()
            .any(|value| value.as_symbol().ok() == Some(symbol))
        {
            items.push(Value::Symbol(symbol.to_string().into()));
        }
    }
    interp.set_global_binding("abbrev-table-name-list", Value::list(items));
    if let Some(table) = interp.lookup_var("fundamental-mode-abbrev-table", &Vec::new()) {
        interp.set_global_binding("local-abbrev-table", table);
    }
}

pub(crate) fn cl_type_value(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    let (min_fixnum, max_fixnum) = fixnum_bounds(interp)?;
    let name = match value {
        Value::Nil => "null",
        Value::T => "boolean",
        Value::Integer(number) => {
            if *number >= min_fixnum && *number <= max_fixnum {
                "fixnum"
            } else {
                "bignum"
            }
        }
        Value::BigInteger(number) => {
            if **number >= BigInt::from(min_fixnum) && **number <= BigInt::from(max_fixnum) {
                "fixnum"
            } else {
                "bignum"
            }
        }
        Value::Float(_) => "float",
        Value::String(_) | Value::StringObject(_) => "string",
        Value::Symbol(_) => "symbol",
        Value::Vector(_) => "vector",
        Value::Cons(_) if is_vector_value(value) => "vector",
        Value::Cons(_) => "cons",
        Value::BuiltinFunc(name) if is_special_form_name(name) => "special-form",
        Value::BuiltinFunc(_) => "primitive-function",
        Value::Lambda(_) => "interpreted-function",
        Value::Buffer(_) => "buffer",
        Value::Marker(_) => "marker",
        Value::Overlay(_) => "overlay",
        Value::CharTable(_) => "char-table",
        Value::Frame(_) => "frame",
        Value::Terminal(_) => "terminal",
        Value::Record(id) => {
            let Some(record) = interp.find_record(*id) else {
                return Ok(Value::symbol("record"));
            };
            let type_name = match record.kind {
                // GNU data.c's PVEC_RECORD branch returns the exact type tag.
                // When that tag is itself a record with at least one public
                // slot, it is a type descriptor and slot one names the type.
                crate::lisp::eval::RecordKind::Record => {
                    let type_tag = record.type_tag.clone();
                    if let Value::Record(type_id) = type_tag
                        && let Some(type_record) = interp.find_record(type_id)
                        && type_record.kind == crate::lisp::eval::RecordKind::Record
                        && let Some(type_name) = type_record.slots.first()
                    {
                        return Ok(type_name.clone());
                    }
                    return Ok(type_tag);
                }
                crate::lisp::eval::RecordKind::BoolVector => "bool-vector",
                crate::lisp::eval::RecordKind::Closure => "byte-code-function",
                crate::lisp::eval::RecordKind::Font => match record.symbol_type_name() {
                    Some("font-entity") => "font-entity",
                    Some("font-object") => "font-object",
                    _ => "font-spec",
                },
                crate::lisp::eval::RecordKind::SymbolWithPos => "symbol-with-pos",
                crate::lisp::eval::RecordKind::Process => "process",
                crate::lisp::eval::RecordKind::HashTable => "hash-table",
                crate::lisp::eval::RecordKind::Obarray => "obarray",
                crate::lisp::eval::RecordKind::Window => "window",
                crate::lisp::eval::RecordKind::WindowConfiguration => "window-configuration",
                crate::lisp::eval::RecordKind::Thread => "thread",
                crate::lisp::eval::RecordKind::Mutex => "mutex",
                crate::lisp::eval::RecordKind::ConditionVariable => "condition-variable",
                crate::lisp::eval::RecordKind::NativeCompUnit => "native-comp-unit",
                crate::lisp::eval::RecordKind::NativeCompiledFunction => "native-comp-function",
                crate::lisp::eval::RecordKind::TreeSitterParser => "treesit-parser",
                crate::lisp::eval::RecordKind::TreeSitterNode => "treesit-node",
                crate::lisp::eval::RecordKind::TreeSitterCompiledQuery => "treesit-compiled-query",
                crate::lisp::eval::RecordKind::Sqlite => "sqlite",
                crate::lisp::eval::RecordKind::Keymap => "cons",
            };
            return Ok(Value::symbol(type_name));
        }
        Value::Finalizer(_) => "finalizer",
        Value::ReaderForm(_) => {
            return Err(LispError::Signal(
                "reader form escaped object materialization".into(),
            ));
        }
        Value::Unbound => "unbound",
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

pub(crate) fn char_table_range_spec(value: &Value) -> Result<Option<(u32, u32)>, LispError> {
    match value {
        Value::Nil => Ok(None),
        Value::T => Ok(Some((0, char::MAX as u32))),
        Value::Integer(codepoint) if *codepoint >= 0 => {
            Ok(Some((*codepoint as u32, *codepoint as u32)))
        }
        Value::Cons(cons_cell) => {
            let car = &cons_cell.car;
            let cdr = &cons_cell.cdr;
            let start = car.borrow().as_integer()?;
            let end = cdr.borrow().as_integer()?;
            if start < 0 || end < 0 {
                return Err(LispError::Signal("Args out of range".into()));
            }
            Ok(Some((start as u32, end as u32)))
        }
        other => Err(LispError::TypeError(
            "character-or-cons-or-nil".into(),
            other.type_name(),
        )),
    }
}

pub(crate) fn normalize_category_set(text: &str) -> String {
    let mut chars: Vec<char> = text.chars().collect();
    chars.sort_unstable();
    chars.dedup();
    chars.into_iter().collect()
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
    buffer_position_to_byte(&interp.buffer, pos)
}

pub(crate) fn byte_to_position(interp: &Interpreter, byte: usize) -> Option<usize> {
    buffer_byte_to_position(&interp.buffer, byte)
}

pub(crate) fn column_at(interp: &Interpreter, env: &Env, line_start: usize, pos: usize) -> usize {
    let mut col = 0usize;
    for p in line_start..pos {
        match interp.buffer.char_at(p) {
            Some(ch) => col = column_after(interp, env, col, p, ch),
            None => break,
        }
    }
    col
}

pub(crate) fn column_after(
    interp: &Interpreter,
    env: &Env,
    current_col: usize,
    pos: usize,
    ch: char,
) -> usize {
    if char_is_invisible(interp, pos, env) {
        current_col
    } else if ch == '\t' {
        let tab_width = interp
            .lookup_var("tab-width", env)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(8)
            .max(1) as usize;
        (current_col / tab_width + 1) * tab_width
    } else {
        current_col + 1
    }
}

pub(crate) fn char_is_invisible(interp: &Interpreter, pos: usize, env: &Env) -> bool {
    let value = buffer_char_property_at(interp, &interp.buffer, pos, "invisible");
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
    match spec {
        Value::Nil => false,
        Value::T => value.is_truthy(),
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
    left: &str,
    right: &str,
    mut canonicalize: impl FnMut(char) -> u32,
) -> i64 {
    let mut left_chars = left.chars();
    let mut right_chars = right.chars();
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
    Ok(match value {
        Value::Nil => Value::Integer(1),
        Value::Symbol(symbol) if symbol == "-" => Value::Integer(-1),
        Value::Integer(_) => value.clone(),
        Value::Cons(_) => value
            .cons_values()
            .and_then(|(head, _)| matches!(head, Value::Integer(_)).then_some(head))
            .unwrap_or(Value::Integer(1)),
        // GNU accepts any Lisp object here.  Values outside the raw-prefix
        // representation, including floats and bignums, have numeric meaning
        // 1 rather than signaling a type error.
        _ => Value::Integer(1),
    })
}
