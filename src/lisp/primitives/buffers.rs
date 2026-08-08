use super::*;

pub(crate) fn get_or_create_buffer(interp: &mut Interpreter, name: &str) -> (u64, String) {
    interp
        .find_buffer(name)
        .unwrap_or_else(|| interp.create_buffer(name))
}

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
) -> Option<Value> {
    highest_priority_overlay_property_with_id(interp, buffer, pos, prop, at_insertion_position)
        .map(|(value, _)| value)
}

pub(crate) fn highest_priority_overlay_property_with_id(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
    at_insertion_position: bool,
) -> Option<(Value, u64)> {
    let mut overlays: Vec<&crate::overlay::Overlay> = buffer
        .overlays
        .iter()
        .filter(|overlay| {
            !overlay.is_dead() && overlay_covers_position(overlay, pos, at_insertion_position)
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
        Value::Integer(pos) if *pos >= 0 => Ok(*pos as usize),
        Value::Marker(id) => interp
            .marker_position(*id)
            .ok_or_else(|| LispError::TypeError("integer-or-marker-p".into(), value.type_name())),
        _ => Err(LispError::TypeError(
            "integer-or-marker-p".into(),
            value.type_name(),
        )),
    }
}

pub(crate) fn count_lines_in_buffer(
    buffer: &crate::buffer::Buffer,
    start: usize,
    end: usize,
) -> Result<i64, LispError> {
    let (from, to) = if start <= end {
        (start, end)
    } else {
        (end, start)
    };
    if from == to {
        return Ok(0);
    }

    let text = buffer
        .buffer_substring(from, to)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let mut lines = text.chars().filter(|ch| *ch == '\n').count() as i64;
    if to > buffer.point_min() && buffer.char_at(to - 1) != Some('\n') {
        lines += 1;
    }
    Ok(lines)
}

pub(crate) fn file_modes_number_to_symbolic(mode: i64, filetype: Option<char>) -> String {
    fn mode_bit(mode: i64, bit: i64) -> bool {
        mode & bit != 0
    }

    let leading = filetype.unwrap_or(match (mode >> 12) & 0o17 {
        0o14 => 's',
        0o12 => 'l',
        0o06 => 'b',
        0o04 => 'd',
        0o02 => 'c',
        0o01 => 'p',
        _ => '-',
    });

    let mut result = String::with_capacity(10);
    result.push(leading);
    result.push(if mode_bit(mode, 0o400) { 'r' } else { '-' });
    result.push(if mode_bit(mode, 0o200) { 'w' } else { '-' });
    result.push(if mode_bit(mode, 0o100) {
        if mode_bit(mode, 0o4000) { 's' } else { 'x' }
    } else if mode_bit(mode, 0o4000) {
        'S'
    } else {
        '-'
    });
    result.push(if mode_bit(mode, 0o040) { 'r' } else { '-' });
    result.push(if mode_bit(mode, 0o020) { 'w' } else { '-' });
    result.push(if mode_bit(mode, 0o010) {
        if mode_bit(mode, 0o2000) { 's' } else { 'x' }
    } else if mode_bit(mode, 0o2000) {
        'S'
    } else {
        '-'
    });
    result.push(if mode_bit(mode, 0o004) { 'r' } else { '-' });
    result.push(if mode_bit(mode, 0o002) { 'w' } else { '-' });
    result.push(if mode_bit(mode, 0o1000) {
        if mode_bit(mode, 0o001) { 't' } else { 'T' }
    } else if mode_bit(mode, 0o001) {
        'x'
    } else {
        '-'
    });
    result
}

pub(crate) fn path_is_gzip_encoded(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".gz") || lower.ends_with(".tgz")
}

pub(crate) fn decompress_gzip_bytes(bytes: &[u8]) -> Result<Vec<u8>, LispError> {
    let mut decoder = GzDecoder::new(bytes);
    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    Ok(output)
}

pub(crate) fn maybe_decompress_file_bytes(
    path: &str,
    bytes: Vec<u8>,
) -> Result<Vec<u8>, LispError> {
    if path_is_gzip_encoded(path) {
        decompress_gzip_bytes(&bytes)
    } else {
        Ok(bytes)
    }
}

#[cfg(test)]
pub(crate) fn mode_function_for_file_name(path: &str) -> Option<&'static str> {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".tgz") {
        return Some("tar-mode");
    }
    if lower.ends_with(".gz") {
        return mode_function_for_file_name(&path[..path.len() - 3]);
    }
    if lower.ends_with(".tar") {
        Some("tar-mode")
    } else if lower.ends_with(".zip") {
        Some("archive-mode")
    } else {
        None
    }
}

pub(crate) fn compressed_payload_path(path: &str) -> Option<String> {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".tgz") {
        return Some(format!("{}.tar", &path[..path.len() - 4]));
    }
    if lower.ends_with(".gz") {
        return Some(path[..path.len() - 3].to_string());
    }
    None
}

pub(crate) fn auto_compression_enabled(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("auto-compression-mode", env)
        .is_some_and(|value| value.is_truthy())
}

pub(crate) fn should_auto_decompress(interp: &Interpreter, env: &Env, path: &str) -> bool {
    auto_compression_enabled(interp, env) && path_is_gzip_encoded(path)
}

pub(crate) fn auto_mode_candidates(interp: &Interpreter, env: &Env, path: &str) -> Vec<String> {
    let mut candidates = vec![path.to_string()];
    if should_auto_decompress(interp, env, path)
        && let Some(payload) = compressed_payload_path(path)
        && !candidates.iter().any(|candidate| candidate == &payload)
    {
        candidates.push(payload);
    }
    candidates
}

pub(crate) enum TranslationTable {
    CharTable(u64),
    String(String),
}

pub(crate) fn translation_table_from_value(
    interp: &Interpreter,
    value: &Value,
) -> Result<TranslationTable, LispError> {
    match value {
        Value::CharTable(id) => Ok(TranslationTable::CharTable(*id)),
        Value::Symbol(symbol) => match interp.get_symbol_property(symbol, "translation-table") {
            Some(Value::CharTable(id)) => Ok(TranslationTable::CharTable(id)),
            _ => Err(LispError::Signal(format!(
                "Invalid translation table name: {symbol}"
            ))),
        },
        _ => string_like(value)
            .map(|string| TranslationTable::String(string.text))
            .ok_or_else(|| LispError::TypeError("string-or-char-table".into(), value.type_name())),
    }
}

pub(crate) fn translate_region_with_table(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    table: &TranslationTable,
) -> Result<Value, LispError> {
    let source = (from..to)
        .map(|position| {
            interp
                .buffer
                .text_property_at(position, "emaxx-raw-char")
                .and_then(|value| value.as_integer().ok())
                .and_then(|value| u32::try_from(value).ok())
                .or_else(|| interp.buffer.char_at(position).map(u32::from))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let string_table = match table {
        TranslationTable::String(text) => Some(text.chars().map(u32::from).collect::<Vec<_>>()),
        TranslationTable::CharTable(_) => None,
    };
    let mut translated = String::new();
    let mut changed = 0i64;
    let mut index = 0usize;
    while index < source.len() {
        let source_char = source[index];
        let mapping = match table {
            TranslationTable::CharTable(id) => interp.char_table_get(*id, source_char),
            TranslationTable::String(_) => string_table
                .as_ref()
                .and_then(|chars| chars.get(source_char as usize))
                .copied()
                .map(|character| Value::Integer(i64::from(character))),
        };
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
                    if let Some(character) = char::from_u32(character) {
                        translated.push(character);
                    }
                }
            }
            None => {
                if let Some(character) = char::from_u32(source_char) {
                    translated.push(character);
                }
            }
        }
        index += consumed;
    }
    interp
        .delete_region_current_buffer(from, to)
        .map_err(|e| LispError::Signal(e.to_string()))?;
    interp.buffer.goto_char(from);
    interp.insert_current_buffer(&translated);
    Ok(Value::Integer(changed))
}

fn translation_characters(value: &Value) -> Option<Vec<u32>> {
    if let Ok(character) = value.as_integer() {
        return u32::try_from(character)
            .ok()
            .filter(|character| char::from_u32(*character).is_some())
            .map(|character| vec![character]);
    }
    let items = value.to_vec().ok()?;
    let (Value::Symbol(marker), characters) = items.split_first()? else {
        return None;
    };
    if marker != "vector-literal" {
        return None;
    }
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
        _ => Err(LispError::TypeError("marker".into(), value.type_name())),
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
    if is_vector_value(value) {
        Ok(vector_slot_refs(value)?
            .iter()
            .map(|slot| slot.borrow().clone())
            .collect())
    } else {
        Ok(value.to_vec()?)
    }
}

pub(crate) fn record_type_name<'a>(interp: &'a Interpreter, value: &Value) -> Option<&'a str> {
    let Value::Record(id) = value else {
        return None;
    };
    interp
        .find_record(*id)
        .map(|record| record.type_name.as_str())
}

pub(crate) fn is_bool_vector_value(interp: &Interpreter, value: &Value) -> bool {
    record_type_name(interp, value) == Some("bool-vector")
}

pub(crate) fn vector_root_slot(value: &Value) -> Option<ConsSlot> {
    match value {
        Value::Cons(cell) if matches!(&*cell.car.borrow(), Value::Symbol(symbol) if symbol == "vector-literal") => {
            Some(ConsSlot::car(cell))
        }
        _ => None,
    }
}

/// Run OP against the cached slot vector of VALUE without cloning the
/// `Rc<Vec>` per access; a cold cache falls back to the filling path.
fn with_vector_slots<T>(value: &Value, op: impl Fn(&[ConsSlot]) -> Option<T>) -> Option<T> {
    let root = vector_root_slot(value)?;
    let key = root.cell_id();
    let mut hit = false;
    let cached = VECTOR_SLOT_CACHE.with_borrow(|cache| {
        let (cached_root, slots) = cache.get(&key)?;
        let live = cached_root.upgrade()?;
        if !live.ptr_eq(&root) {
            return None;
        }
        hit = true;
        op(slots)
    });
    if hit {
        return cached;
    }
    let slots = vector_slot_refs(value).ok()?;
    op(&slots)
}

/// O(1) element read for the VM's Baref: Some only when VALUE is a plain
/// list-vector and INDEX is in range; strings, char-tables, records,
/// closures, and out-of-range all return None so the caller takes the
/// full `aref' path (and its exact errors).
pub(crate) fn vector_aref_fast(value: &Value, index: usize) -> Option<Value> {
    with_vector_slots(value, |slots| {
        slots.get(index).map(|slot| slot.borrow().clone())
    })
}

/// O(1) element write for the VM's Baset, same contract as
/// [`vector_aref_fast`].
pub(crate) fn vector_aset_fast(value: &Value, index: usize, new_value: &Value) -> Option<()> {
    with_vector_slots(value, |slots| {
        let slot = slots.get(index)?;
        *slot.borrow_mut() = new_value.clone();
        Some(())
    })
}

pub(crate) fn vector_slot_refs(value: &Value) -> Result<Rc<Vec<ConsSlot>>, LispError> {
    let Some(root) = vector_root_slot(value) else {
        return Err(LispError::TypeError("vector".into(), value.type_name()));
    };
    let key = root.cell_id();
    if let Some(slots) = VECTOR_SLOT_CACHE.with_borrow_mut(|cache| match cache.get(&key) {
        Some((cached_root, slots)) => match cached_root.upgrade() {
            Some(cached_root) if cached_root.ptr_eq(&root) => Some(slots.clone()),
            _ => {
                cache.remove(&key);
                None
            }
        },
        None => None,
    }) {
        return Ok(slots);
    }

    let Some((_, cdr)) = (value).cons_cells() else {
        return Err(LispError::TypeError("vector".into(), value.type_name()));
    };
    let mut current = cdr.borrow().clone();
    let mut slots = Vec::new();
    loop {
        match current {
            Value::Cons(cell) => {
                slots.push(ConsSlot::car(&cell));
                current = cell.cdr.borrow().clone();
            }
            Value::Nil => break,
            _ => return Err(LispError::TypeError("vector".into(), value.type_name())),
        }
    }

    let slots = Rc::new(slots);
    VECTOR_SLOT_CACHE.with_borrow_mut(|cache| {
        cache.insert(key, (root.downgrade(), slots.clone()));
    });
    Ok(slots)
}

pub(crate) fn vector_slot_value(value: &Value, index: usize) -> Result<Value, LispError> {
    vector_slot_refs(value)?
        .get(index)
        .map(|slot| slot.borrow().clone())
        .ok_or_else(|| {
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
        return Err(LispError::TypeError(
            "bool-vector".into(),
            value.type_name(),
        ));
    };
    let record = interp
        .find_record(*id)
        .ok_or_else(|| LispError::TypeError("bool-vector".into(), value.type_name()))?;
    if record.type_name != "bool-vector" {
        return Err(LispError::TypeError(
            "bool-vector".into(),
            value.type_name(),
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
    interp.create_record(
        "bool-vector",
        bits.into_iter()
            .map(|bit| if bit { Value::T } else { Value::Nil })
            .collect(),
    )
}

pub(crate) const ABBREV_TABLE_RECORD_TYPE: &str = "abbrev-table";
pub(crate) const ABBREV_TABLE_NAME_SLOT: usize = 0;
pub(crate) const ABBREV_TABLE_PROPS_SLOT: usize = 1;
pub(crate) const ABBREV_TABLE_ENTRIES_SLOT: usize = 2;

pub(crate) fn set_bool_vector_bit(
    interp: &mut Interpreter,
    value: &Value,
    index: usize,
    bit: bool,
) -> Result<(), LispError> {
    let Value::Record(id) = value else {
        return Err(LispError::TypeError(
            "bool-vector".into(),
            value.type_name(),
        ));
    };
    let record = interp
        .find_record_mut(*id)
        .ok_or_else(|| LispError::TypeError("bool-vector".into(), value.type_name()))?;
    if record.type_name != "bool-vector" {
        return Err(LispError::TypeError(
            "bool-vector".into(),
            value.type_name(),
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
        .filter(|record| record.type_name == ABBREV_TABLE_RECORD_TYPE)
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
    let table = interp.create_record(
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

pub(crate) fn abbrev_table_name_value(interp: &Interpreter, table: &Value) -> Option<Value> {
    let id = abbrev_table_record_id(interp, table)?;
    let record = interp.find_record(id)?;
    match record.slots.get(ABBREV_TABLE_NAME_SLOT) {
        Some(Value::Nil) | None => None,
        Some(value) => Some(value.clone()),
    }
}

pub(crate) fn abbrev_table_props_value(
    interp: &Interpreter,
    table: &Value,
) -> Result<Value, LispError> {
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
    Ok(record
        .slots
        .get(ABBREV_TABLE_PROPS_SLOT)
        .cloned()
        .unwrap_or(Value::Nil))
}

pub(crate) fn set_abbrev_table_props_value(
    interp: &mut Interpreter,
    table: &Value,
    props: Value,
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
    if record.slots.len() <= ABBREV_TABLE_PROPS_SLOT {
        record.slots.resize(ABBREV_TABLE_PROPS_SLOT + 1, Value::Nil);
    }
    record.slots[ABBREV_TABLE_PROPS_SLOT] = props;
    let symbol = abbrev_symbol_name(id, "");
    interp.set_global_binding(&symbol, Value::Nil);
    interp.set_symbol_plist(&symbol, abbrev_table_props_value(interp, table)?)?;
    Ok(())
}

pub(crate) fn abbrev_table_property(
    interp: &Interpreter,
    table: &Value,
    property: &Value,
) -> Option<Value> {
    let key = property.as_symbol().ok()?;
    let items = abbrev_table_props_value(interp, table)
        .ok()?
        .to_vec()
        .ok()?;
    let mut index = 0usize;
    while index + 1 < items.len() {
        if items[index].as_symbol().ok() == Some(key) {
            return Some(items[index + 1].clone());
        }
        index += 2;
    }
    None
}

pub(crate) fn set_abbrev_table_property(
    interp: &mut Interpreter,
    table: &Value,
    property: &Value,
    value: Value,
) -> Result<(), LispError> {
    let key = property.as_symbol()?.to_string();
    let mut items = abbrev_table_props_value(interp, table)?
        .to_vec()
        .unwrap_or_default();
    let mut index = 0usize;
    let mut updated = false;
    while index + 1 < items.len() {
        if items[index].as_symbol().ok() == Some(key.as_str()) {
            items[index + 1] = value.clone();
            updated = true;
            break;
        }
        index += 2;
    }
    if !updated {
        items.push(Value::Symbol(key.into()));
        items.push(value);
    }
    set_abbrev_table_props_value(interp, table, Value::list(items))
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

pub(crate) fn abbrev_props_from_parts(
    hook: Option<Value>,
    props: &[Value],
) -> Result<Value, LispError> {
    let mut prop_items = props.to_vec();
    if !prop_items.is_empty()
        && !matches!(prop_items.first(), Some(Value::Symbol(symbol)) if symbol.starts_with(':'))
    {
        let count = prop_items.first().cloned().unwrap_or(Value::Nil);
        let system = prop_items.get(1).cloned().unwrap_or(Value::Nil);
        prop_items = vec![Value::symbol(":count"), count];
        if !system.is_nil() {
            prop_items.push(Value::symbol(":system"));
            prop_items.push(system);
        }
    }
    if !prop_items.len().is_multiple_of(2) {
        return Err(LispError::Signal("Invalid abbrev property list".into()));
    }
    let mut props = plist_pairs(&Value::list(prop_items))?;
    if let Some(hook) = hook.filter(|value| !value.is_nil()) {
        if let Some((_, existing)) = props.iter_mut().find(|(key, _)| key == ":hook") {
            *existing = hook;
        } else {
            props.push((":hook".into(), hook));
        }
    }
    if !props.iter().any(|(key, _)| key == ":count") {
        props.push((":count".into(), Value::Integer(0)));
    }
    Ok(plist_value(&props))
}

pub(crate) fn abbrev_prop(props: &Value, key: &str) -> Option<Value> {
    plist_pairs(props)
        .ok()?
        .into_iter()
        .find_map(|(existing, value)| (existing == key).then_some(value))
}

pub(crate) fn abbrev_matches_name(
    interp: &Interpreter,
    table: &Value,
    existing: &str,
    props: &Value,
    name: &str,
) -> bool {
    if existing == name {
        return true;
    }
    let table_case_fixed = abbrev_table_property(interp, table, &Value::symbol(":case-fixed"))
        .is_some_and(|value| value.is_truthy());
    if table_case_fixed || abbrev_prop(props, ":case-fixed").is_some_and(|value| value.is_truthy())
    {
        return false;
    }
    existing == name.to_lowercase()
}

pub(crate) fn abbrev_parent_tables(
    interp: &Interpreter,
    table: &Value,
) -> Result<Vec<Value>, LispError> {
    let Some(value) = abbrev_table_property(interp, table, &Value::symbol(":parents")) else {
        return Ok(Vec::new());
    };
    if value.is_nil() {
        return Ok(Vec::new());
    }
    if is_abbrev_table_value(interp, &value) {
        return Ok(vec![value]);
    }
    value.to_vec()
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

pub(crate) fn abbrev_expansion(
    interp: &Interpreter,
    table: &Value,
    name: &str,
) -> Result<Option<Value>, LispError> {
    fn lookup(
        interp: &Interpreter,
        table: &Value,
        name: &str,
        seen: &mut HashSet<u64>,
    ) -> Result<Option<Value>, LispError> {
        let Some(id) = abbrev_table_record_id(interp, table) else {
            return Err(LispError::TypeError(
                "abbrev-table".into(),
                table.type_name(),
            ));
        };
        if !seen.insert(id) {
            return Ok(None);
        }
        for (existing, expansion, props) in abbrev_table_entries(interp, table)? {
            if abbrev_matches_name(interp, table, &existing, &props, name) {
                return Ok(Some(expansion));
            }
        }
        for parent in abbrev_parent_tables(interp, table)? {
            if let Some(expansion) = lookup(interp, &parent, name, seen)? {
                return Ok(Some(expansion));
            }
        }
        Ok(None)
    }

    lookup(interp, table, name, &mut HashSet::new())
}

pub(crate) fn copy_abbrev_table(
    interp: &mut Interpreter,
    table: &Value,
) -> Result<Value, LispError> {
    let Some(id) = abbrev_table_record_id(interp, table) else {
        return Err(LispError::TypeError(
            "abbrev-table".into(),
            table.type_name(),
        ));
    };
    interp.copy_record(id)
}

pub(crate) fn parse_abbrev_definition(entry: &Value) -> Result<(String, Value, Value), LispError> {
    let parts = entry.to_vec()?;
    if parts.len() < 2 {
        return Err(LispError::Signal("Invalid abbrev definition".into()));
    }
    let name = string_text(&parts[0])?;
    let expansion = parts[1].clone();
    let hook = parts.get(2).cloned().unwrap_or(Value::Nil);
    let props = if parts.len() > 3 {
        abbrev_props_from_parts(Some(hook), &parts[3..])?
    } else {
        abbrev_props_from_parts(Some(hook), &[])?
    };
    Ok((name, expansion, props))
}

pub(crate) fn set_abbrev_table_entries_from_definitions(
    interp: &mut Interpreter,
    table: &Value,
    definitions: &Value,
) -> Result<(), LispError> {
    let mut entries = Vec::new();
    for entry in definitions.to_vec()? {
        entries.push(parse_abbrev_definition(&entry)?);
    }
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

pub(crate) fn register_abbrev_table_symbol(interp: &mut Interpreter, symbol: &str) {
    let existing = interp
        .lookup_var("abbrev-table-name-list", &Vec::new())
        .unwrap_or(Value::Nil);
    let mut items = existing.to_vec().unwrap_or_default();
    if !items
        .iter()
        .any(|value| value.as_symbol().ok() == Some(symbol))
    {
        items.insert(0, Value::Symbol(symbol.to_string().into()));
        interp.set_global_binding("abbrev-table-name-list", Value::list(items));
    }
}

pub(crate) fn derived_mode_set_parent(interp: &mut Interpreter, mode: &str, parent: Option<&str>) {
    match parent {
        Some(parent) => {
            interp.put_symbol_property(mode, "derived-mode-parent", Value::Symbol(parent.into()))
        }
        None => interp.remove_symbol_property(mode, "derived-mode-parent"),
    }
    derived_mode_flush(interp, mode);
}

pub(crate) fn derived_mode_add_parents(
    interp: &mut Interpreter,
    mode: &str,
    extra_parents: &Value,
) -> Result<(), LispError> {
    let extras = extra_parents.to_vec()?;
    interp.put_symbol_property(mode, "derived-mode-extra-parents", Value::list(extras));
    derived_mode_flush(interp, mode);
    Ok(())
}

pub(crate) fn derived_mode_flush(interp: &mut Interpreter, mode: &str) {
    interp.remove_symbol_property(mode, "derived-mode--all-parents");
    let followers = interp
        .get_symbol_property(mode, "derived-mode--followers")
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    interp.remove_symbol_property(mode, "derived-mode--followers");
    for follower in followers {
        if let Ok(symbol) = follower.as_symbol() {
            derived_mode_flush(interp, symbol);
        }
    }
}

pub(crate) fn derived_mode_parent_chain(interp: &Interpreter, mode: &str) -> Vec<String> {
    fn visit(
        interp: &Interpreter,
        mode: &str,
        seen: &mut std::collections::HashSet<String>,
        out: &mut Vec<String>,
    ) {
        if !seen.insert(mode.to_string()) {
            return;
        }
        out.push(mode.to_string());
        if let Some(Value::Symbol(parent)) = interp.get_symbol_property(mode, "derived-mode-parent")
        {
            visit(interp, &parent, seen, out);
        }
        if let Some(extra) = interp.get_symbol_property(mode, "derived-mode-extra-parents")
            && let Ok(items) = extra.to_vec()
        {
            for parent in items {
                if let Ok(parent) = parent.as_symbol() {
                    visit(interp, parent, seen, out);
                }
            }
        }
        if let Ok(alias) = interp.lookup_function(mode, &Vec::new())
            && let Value::Symbol(alias_name) = alias
        {
            visit(interp, &alias_name, seen, out);
        }
    }

    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    visit(interp, mode, &mut seen, &mut result);
    result
}

pub(crate) fn is_builtin_class_name(name: &str) -> bool {
    builtin_class(name).is_some()
}

#[derive(Clone, Copy)]
pub(crate) struct BuiltinClass {
    pub(crate) name: &'static str,
    parents: &'static [&'static str],
    pub(crate) predicate: Option<&'static str>,
}

impl BuiltinClass {
    const fn new(
        name: &'static str,
        parents: &'static [&'static str],
        predicate: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            parents,
            predicate,
        }
    }
}

const BUILTIN_CLASSES: &[BuiltinClass] = &[
    BuiltinClass::new("t", &[], None),
    BuiltinClass::new("array", &["sequence", "atom"], Some("arrayp")),
    BuiltinClass::new("atom", &["t"], Some("atom")),
    BuiltinClass::new("bignum", &["integer"], Some("bignump")),
    BuiltinClass::new("bool-vector", &["array"], Some("bool-vector-p")),
    BuiltinClass::new("boolean", &["symbol"], Some("booleanp")),
    BuiltinClass::new("buffer", &["atom"], Some("bufferp")),
    BuiltinClass::new(
        "byte-code-function",
        &["compiled-function", "closure"],
        Some("byte-code-function-p"),
    ),
    BuiltinClass::new("char-table", &["array"], Some("char-table-p")),
    BuiltinClass::new("closure", &["function"], Some("closurep")),
    BuiltinClass::new(
        "compiled-function",
        &["function"],
        Some("compiled-function-p"),
    ),
    BuiltinClass::new("condvar", &["atom"], None),
    BuiltinClass::new("cons", &["list"], Some("consp")),
    BuiltinClass::new("finalizer", &["atom"], None),
    BuiltinClass::new("fixnum", &["integer"], Some("fixnump")),
    BuiltinClass::new("float", &["number"], Some("floatp")),
    BuiltinClass::new("font-entity", &["atom"], None),
    BuiltinClass::new("font-object", &["atom"], None),
    BuiltinClass::new("font-spec", &["atom"], None),
    BuiltinClass::new("frame", &["atom"], Some("framep")),
    BuiltinClass::new("function", &["atom"], Some("functionp")),
    BuiltinClass::new("hash-table", &["atom"], Some("hash-table-p")),
    BuiltinClass::new(
        "integer",
        &["number", "integer-or-marker"],
        Some("integerp"),
    ),
    BuiltinClass::new(
        "integer-or-marker",
        &["number-or-marker"],
        Some("integer-or-marker-p"),
    ),
    BuiltinClass::new(
        "interpreted-function",
        &["closure"],
        Some("interpreted-function-p"),
    ),
    BuiltinClass::new("list", &["sequence"], Some("listp")),
    BuiltinClass::new("marker", &["integer-or-marker"], Some("markerp")),
    BuiltinClass::new("module-function", &["function"], Some("module-function-p")),
    BuiltinClass::new("mutex", &["atom"], Some("mutexp")),
    BuiltinClass::new(
        "native-comp-function",
        &["subr", "compiled-function"],
        Some("native-comp-function-p"),
    ),
    BuiltinClass::new("native-comp-unit", &["atom"], None),
    BuiltinClass::new("null", &["boolean", "list"], Some("null")),
    BuiltinClass::new("number", &["number-or-marker"], Some("numberp")),
    BuiltinClass::new("number-or-marker", &["atom"], Some("number-or-marker-p")),
    BuiltinClass::new("obarray", &["atom"], Some("obarrayp")),
    BuiltinClass::new("overlay", &["atom"], Some("overlayp")),
    BuiltinClass::new(
        "primitive-function",
        &["subr", "compiled-function"],
        Some("primitive-function-p"),
    ),
    BuiltinClass::new("process", &["atom"], Some("processp")),
    BuiltinClass::new("record", &["atom"], Some("recordp")),
    BuiltinClass::new("sequence", &["t"], Some("sequencep")),
    BuiltinClass::new("special-form", &["subr"], Some("special-form-p")),
    BuiltinClass::new("string", &["array"], Some("stringp")),
    BuiltinClass::new("subr", &["atom"], Some("subrp")),
    BuiltinClass::new("symbol", &["atom"], Some("symbolp")),
    BuiltinClass::new("symbol-with-pos", &["symbol"], Some("symbol-with-pos-p")),
    BuiltinClass::new("terminal", &["atom"], None),
    BuiltinClass::new("thread", &["atom"], Some("threadp")),
    BuiltinClass::new("tree-sitter-compiled-query", &["atom"], None),
    BuiltinClass::new("tree-sitter-node", &["atom"], None),
    BuiltinClass::new("tree-sitter-parser", &["atom"], None),
    BuiltinClass::new("user-ptr", &["atom"], Some("user-ptrp")),
    BuiltinClass::new("vector", &["array"], Some("vectorp")),
    BuiltinClass::new("window", &["atom"], Some("windowp")),
    BuiltinClass::new(
        "window-configuration",
        &["atom"],
        Some("window-configuration-p"),
    ),
];

fn builtin_class(name: &str) -> Option<&'static BuiltinClass> {
    BUILTIN_CLASSES.iter().find(|class| class.name == name)
}

pub(crate) fn builtin_classes() -> &'static [BuiltinClass] {
    BUILTIN_CLASSES
}

pub(crate) fn builtin_class_parents(name: &str) -> &'static [&'static str] {
    builtin_class(name).map_or(&[], |class| class.parents)
}

fn merge_class_precedence(mut lists: Vec<Vec<&'static str>>) -> Vec<&'static str> {
    lists.retain(|list| !list.is_empty());
    let mut merged = Vec::new();
    while lists.len() > 1 {
        let candidate = lists.iter().find_map(|list| {
            let head = *list.first()?;
            lists
                .iter()
                .all(|other| !other.iter().skip(1).any(|item| item == &head))
                .then_some(head)
        });
        let candidate = candidate.unwrap_or(lists[0][0]);
        merged.push(candidate);
        for list in &mut lists {
            if list.first() == Some(&candidate) {
                list.remove(0);
            }
        }
        lists.retain(|list| !list.is_empty());
    }
    if let Some(last) = lists.pop() {
        merged.extend(last);
    }
    merged
}

fn compute_builtin_class_precedence(
    name: &'static str,
    active: &mut std::collections::HashSet<&'static str>,
) -> Vec<&'static str> {
    let Some(class) = builtin_class(name) else {
        return Vec::new();
    };
    if !active.insert(name) {
        return vec![name];
    }
    let mut result = vec![name];
    if class.parents.is_empty() {
        if name != "t" {
            result.extend(compute_builtin_class_precedence("t", active));
        }
    } else {
        result.extend(merge_class_precedence(
            class
                .parents
                .iter()
                .map(|parent| compute_builtin_class_precedence(parent, active))
                .collect(),
        ));
    }
    active.remove(name);
    result
}

pub(crate) fn builtin_class_allparents(name: &str) -> Option<&'static [&'static str]> {
    static PRECEDENCE: std::sync::OnceLock<
        std::collections::HashMap<&'static str, Vec<&'static str>>,
    > = std::sync::OnceLock::new();
    PRECEDENCE
        .get_or_init(|| {
            BUILTIN_CLASSES
                .iter()
                .map(|class| {
                    (
                        class.name,
                        compute_builtin_class_precedence(
                            class.name,
                            &mut std::collections::HashSet::new(),
                        ),
                    )
                })
                .collect()
        })
        .get(name)
        .map(Vec::as_slice)
}

/// CL types that GNU defines directly through `cl-deftype-satisfies' rather
/// than through the built-in class hierarchy.
pub(crate) fn builtin_cl_satisfies_types() -> &'static [(&'static str, &'static str)] {
    &[
        ("base-char", "characterp"),
        ("character", "natnump"),
        ("command", "commandp"),
        ("keyword", "keywordp"),
        ("natnum", "natnump"),
        ("real", "numberp"),
    ]
}

pub(crate) fn cl_type_name(interp: &Interpreter, value: &Value) -> Result<&'static str, LispError> {
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
        Value::Record(id) => match interp
            .find_record(*id)
            .map(|record| record.type_name.as_str())
            .unwrap_or("record")
        {
            "bool-vector" => "bool-vector",
            "condition-variable" => "condvar",
            "font-entity" => "font-entity",
            "font-object" => "font-object",
            "font-spec" => "font-spec",
            "frame" => "frame",
            "module-function" => "module-function",
            "mutex" => "mutex",
            "native-comp-function" => "native-comp-function",
            "native-comp-unit" => "native-comp-unit",
            "obarray" => "obarray",
            "process" => "process",
            "terminal" => "terminal",
            "thread" => "thread",
            "tree-sitter-compiled-query" => "tree-sitter-compiled-query",
            "tree-sitter-node" => "tree-sitter-node",
            "tree-sitter-parser" => "tree-sitter-parser",
            "user-ptr" => "user-ptr",
            "window" => "window",
            "window-configuration" => "window-configuration",
            "byte-code-function" => "byte-code-function",
            _ => "record",
        },
        Value::Finalizer(_) => "finalizer",
        Value::Unbound => "unbound",
    };
    Ok(name)
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

pub(crate) fn compare_buffer_substrings(left: &str, right: &str) -> i64 {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();
    let min_len = left_chars.len().min(right_chars.len());
    for index in 0..min_len {
        if left_chars[index] != right_chars[index] {
            let offset = (index + 1) as i64;
            return if left_chars[index] < right_chars[index] {
                -offset
            } else {
                offset
            };
        }
    }
    if left_chars.len() == right_chars.len() {
        0
    } else {
        let offset = (min_len + 1) as i64;
        if left_chars.len() < right_chars.len() {
            -offset
        } else {
            offset
        }
    }
}

pub(crate) fn prefix_numeric_value(value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Nil => Ok(Value::Integer(1)),
        Value::Integer(_) | Value::BigInteger(_) => Ok(value.clone()),
        Value::Symbol(symbol) if symbol == "-" => Ok(Value::Integer(-1)),
        Value::Cons(_) => {
            let items = value.to_vec()?;
            if items.len() == 1 {
                prefix_numeric_value(&items[0])
            } else {
                Err(LispError::TypeError("number".into(), value.type_name()))
            }
        }
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}
