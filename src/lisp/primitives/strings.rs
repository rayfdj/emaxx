use super::*;

#[derive(Clone, Debug)]
pub(crate) struct StringLike {
    pub(crate) text: String,
    pub(crate) props: Vec<TextPropertySpan>,
    pub(crate) multibyte: bool,
    pub(crate) extended_chars: Vec<(usize, u32)>,
}

impl StringLike {
    pub(crate) fn char_code_at(&self, index: usize) -> Option<i64> {
        if let Ok(position) = self
            .extended_chars
            .binary_search_by_key(&index, |(position, _)| *position)
        {
            return Some(i64::from(self.extended_chars[position].1));
        }
        self.text
            .chars()
            .nth(index)
            .map(|ch| string_character_code(self.multibyte, ch))
    }

    pub(crate) fn character_codes(&self) -> Vec<i64> {
        self.text
            .chars()
            .enumerate()
            .map(|(index, ch)| {
                self.extended_chars
                    .binary_search_by_key(&index, |(position, _)| *position)
                    .ok()
                    .map(|position| i64::from(self.extended_chars[position].1))
                    .unwrap_or_else(|| string_character_code(self.multibyte, ch))
            })
            .collect()
    }

    pub(crate) fn byte_len(&self) -> Result<usize, LispError> {
        if !self.multibyte {
            return Ok(encode_raw_text_bytes(&self.text)?.len());
        }
        self.character_codes()
            .into_iter()
            .try_fold(0usize, |len, code| {
                let code = u32::try_from(code)
                    .map_err(|_| LispError::Signal("Invalid character".into()))?;
                Ok(len + emacs_multibyte_char_len(code)?)
            })
    }
}

pub(crate) fn emacs_multibyte_char_len(code: u32) -> Result<usize, LispError> {
    Ok(match code {
        0x000000..=0x00007F => 1,
        0x000080..=0x0007FF => 2,
        0x000800..=0x00FFFF => 3,
        0x010000..=0x1FFFFF => 4,
        0x200000..=0x3FFF7F => 5,
        0x3FFF80..=0x3FFFFF => 2,
        _ => return Err(LispError::Signal("Invalid character".into())),
    })
}

pub(crate) fn push_emacs_multibyte_char(output: &mut Vec<u8>, code: u32) -> Result<(), LispError> {
    match emacs_multibyte_char_len(code)? {
        1 => output.push(code as u8),
        2 if code <= 0x7FF => {
            output.push(0xC0 | (code >> 6) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        2 => {
            let payload = code - 0x3F_FF80;
            output.push(0xC0 | (payload >> 6) as u8);
            output.push(0x80 | (payload & 0x3F) as u8);
        }
        3 => {
            output.push(0xE0 | (code >> 12) as u8);
            output.push(0x80 | ((code >> 6) & 0x3F) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        4 => {
            output.push(0xF0 | (code >> 18) as u8);
            output.push(0x80 | ((code >> 12) & 0x3F) as u8);
            output.push(0x80 | ((code >> 6) & 0x3F) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        5 => {
            output.push(0xF8);
            output.push(0x80 | ((code >> 18) & 0x0F) as u8);
            output.push(0x80 | ((code >> 12) & 0x3F) as u8);
            output.push(0x80 | ((code >> 6) & 0x3F) as u8);
            output.push(0x80 | (code & 0x3F) as u8);
        }
        _ => unreachable!("validated Emacs multibyte length"),
    }
    Ok(())
}

fn string_character_code(multibyte: bool, ch: char) -> i64 {
    if let Some(byte) = raw_byte_from_regex_char(ch) {
        if multibyte {
            RAW_BYTE8_BASE as i64 + i64::from(byte)
        } else {
            i64::from(byte)
        }
    } else {
        ch as i64
    }
}

pub(crate) fn string_like(value: &Value) -> Option<StringLike> {
    match value {
        Value::String(text) => Some(StringLike {
            text: text.to_string(),
            props: Vec::new(),
            extended_chars: Vec::new(),
            multibyte: text
                .chars()
                .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F),
        }),
        Value::StringObject(state) => {
            let state = state.borrow();
            Some(StringLike {
                text: state.text.clone(),
                props: state
                    .props
                    .iter()
                    .map(|span| TextPropertySpan {
                        start: span.start,
                        end: span.end,
                        props: span.props.clone(),
                    })
                    .collect(),
                multibyte: state.multibyte,
                extended_chars: state.extended_chars.clone(),
            })
        }
        Value::Cons(cell) if matches!(&*cell.car.borrow(), Value::Symbol(symbol) if symbol == "vector-literal") =>
        {
            let items = vector_items(value).ok()?;
            if items.len() < 4 {
                return None;
            }
            let Value::String(text) = items.first()?.clone() else {
                return None;
            };
            let mut props = Vec::new();
            let mut i = 1;
            while i + 2 < items.len() {
                let start = items[i].as_integer().ok()? as usize;
                let end = items[i + 1].as_integer().ok()? as usize;
                let plist = plist_pairs(&items[i + 2]).ok()?;
                props.push(TextPropertySpan {
                    start,
                    end,
                    props: plist,
                });
                i += 3;
            }
            let multibyte = text
                .chars()
                .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
            Some(StringLike {
                text: text.to_string(),
                props,
                multibyte,
                extended_chars: Vec::new(),
            })
        }
        Value::Cons(_) => None,
        _ => None,
    }
}

pub(crate) fn string_text(value: &Value) -> Result<String, LispError> {
    string_like(value)
        .map(|string| string.text)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))
}

pub(crate) fn char_from_integer(code: i64) -> Result<char, LispError> {
    if code < 0 {
        return Err(LispError::Signal("Invalid character".into()));
    }
    if (RAW_BYTE8_BASE as i64..=RAW_BYTE8_BASE as i64 + 0xFF).contains(&code) {
        return Ok(raw_byte_regex_char((code - RAW_BYTE8_BASE as i64) as u8));
    }
    char::from_u32(code as u32).ok_or_else(|| LispError::Signal("Invalid character".into()))
}

pub(crate) fn string_comparison_text(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Nil => Ok("nil".into()),
        Value::T => Ok("t".into()),
        Value::Symbol(name) => Ok(crate::lisp::types::visible_symbol_name(name).to_string()),
        _ => string_text(value),
    }
}

pub(crate) fn fold_string_compare_code(code: i64, ignore_case: bool) -> i64 {
    if !ignore_case {
        return code;
    }
    let Some(codepoint) = u32::try_from(code).ok() else {
        return code;
    };
    simple_upcase_char(codepoint) as i64
}

pub(crate) fn normalize_compare_strings_end(
    arg: Option<&Value>,
    len: i64,
) -> Result<i64, LispError> {
    let Some(value) = arg else {
        return Ok(len);
    };
    if value.is_nil() {
        return Ok(len);
    }
    let raw = value.as_integer()?;
    let index = if raw < 0 { len + raw } else { raw };
    Ok(index.clamp(0, len))
}

pub(crate) fn string_compare_codes(
    value: &Value,
    start: Option<&Value>,
    end: Option<&Value>,
    ignore_case: bool,
    clamp_end: bool,
) -> Result<Vec<i64>, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    let codes = string_sequence_values(&string)
        .into_iter()
        .map(|value| value.as_integer())
        .collect::<Result<Vec<_>, _>>()?;
    let len = codes.len() as i64;
    let start = normalize_string_index(start, 0, len)? as usize;
    let end = if clamp_end {
        normalize_compare_strings_end(end, len)?
    } else {
        normalize_string_index(end, len, len)?
    } as usize;
    if start > end {
        return Err(LispError::Signal("Args out of range".into()));
    }
    Ok(codes[start..end]
        .iter()
        .copied()
        .map(|code| fold_string_compare_code(code, ignore_case))
        .collect())
}

pub(crate) fn string_compare_ordering(
    left: &Value,
    right: &Value,
    ignore_case: bool,
) -> Result<Ordering, LispError> {
    Ok(
        string_compare_codes(left, None, None, ignore_case, false)?.cmp(&string_compare_codes(
            right,
            None,
            None,
            ignore_case,
            false,
        )?),
    )
}

pub(crate) fn compare_strings_value(
    left: &Value,
    left_start: Option<&Value>,
    left_end: Option<&Value>,
    right: &Value,
    right_start: Option<&Value>,
    right_end: Option<&Value>,
    ignore_case: bool,
) -> Result<Value, LispError> {
    let left = string_compare_codes(left, left_start, left_end, ignore_case, true)?;
    let right = string_compare_codes(right, right_start, right_end, ignore_case, true)?;
    let common_len = left.len().min(right.len());

    for index in 0..common_len {
        match left[index].cmp(&right[index]) {
            Ordering::Less => return Ok(Value::Integer(-((index + 1) as i64))),
            Ordering::Greater => return Ok(Value::Integer((index + 1) as i64)),
            Ordering::Equal => {}
        }
    }

    match left.len().cmp(&right.len()) {
        Ordering::Less => Ok(Value::Integer(-((common_len + 1) as i64))),
        Ordering::Greater => Ok(Value::Integer((common_len + 1) as i64)),
        Ordering::Equal => Ok(Value::T),
    }
}

pub(crate) fn validate_collation_locale(locale: Option<&Value>) -> Result<(), LispError> {
    if locale.is_some_and(|value| {
        !(value.is_nil() || matches!(value, Value::T) || string_like(value).is_some())
    }) {
        return Err(LispError::TypeError(
            "string".into(),
            locale.expect("checked above").type_name(),
        ));
    }
    Ok(())
}

pub(crate) fn assoc_string_text(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Nil => Ok("nil".into()),
        Value::T => Ok("t".into()),
        Value::Symbol(name) => Ok(name.to_string()),
        _ => string_text(value),
    }
}

pub(crate) fn assoc_string_candidate_text(value: &Value) -> Option<String> {
    match value {
        Value::Nil => Some("nil".into()),
        Value::T => Some("t".into()),
        Value::Symbol(name) => Some(name.to_string()),
        _ => string_like(value).map(|string| string.text),
    }
}

pub(crate) fn assoc_string_folded_text(
    interp: &mut Interpreter,
    text: &str,
) -> Result<String, LispError> {
    let (down_table, _) = current_case_table_ids(interp)?;
    let mut folded = String::new();
    let chars: Vec<char> = text.chars().collect();
    for (index, ch) in chars.iter().copied().enumerate() {
        let next_is_word = chars
            .get(index + 1)
            .copied()
            .is_some_and(|next| interp.is_syntax_word_char(normalize_case_key(next as u32)));
        folded.push_str(&full_downcase_string(
            interp,
            down_table,
            ch,
            interp.is_syntax_word_char(normalize_case_key(ch as u32)) && !next_is_word,
        ));
    }
    Ok(folded)
}

pub(crate) fn aset_string_value(
    target: &Value,
    index: usize,
    new_value: &Value,
) -> Result<Value, LispError> {
    let mut string = string_like(target)
        .ok_or_else(|| LispError::TypeError("string".into(), target.type_name()))?;
    let code = new_value.as_integer()?;
    let mut chars: Vec<char> = string.text.chars().collect();
    if index >= chars.len() {
        return Err(LispError::Signal("Args out of range".into()));
    }
    let ch = if string.multibyte {
        char_from_integer(code)?
    } else if (0..=255).contains(&code) {
        let byte = code as u8;
        if byte <= 0x7F {
            byte as char
        } else {
            raw_byte_regex_char(byte)
        }
    } else {
        // GNU can promote an all-ASCII unibyte string in place when the new
        // character needs multibyte storage.  Raw non-ASCII bytes cannot be
        // reinterpreted during that promotion, so they keep the documented
        // args-out-of-range failure instead.
        if chars.iter().any(|ch| !ch.is_ascii()) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("args-out-of-range".into()),
                target.clone(),
                Value::Integer(code),
            ])));
        }
        string.multibyte = true;
        char_from_integer(code)?
    };
    chars[index] = ch;
    string.text = chars.into_iter().collect();
    if let Value::StringObject(state) = target {
        let mut state = state.borrow_mut();
        state.text = string.text;
        state.props = shared_string_props(&string.props);
        state.multibyte = string.multibyte;
        return Ok(target.clone());
    }
    Ok(make_shared_string_value_with_multibyte(
        string.text,
        string.props,
        string.multibyte,
    ))
}

pub(crate) fn shared_string_props(props: &[TextPropertySpan]) -> Vec<StringPropertySpan> {
    props
        .iter()
        .map(|span| StringPropertySpan {
            start: span.start,
            end: span.end,
            props: span.props.clone(),
        })
        .collect()
}

pub(crate) fn make_shared_string_value_with_multibyte(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
) -> Value {
    make_shared_string_value_with_extended_chars(text, props, multibyte, Vec::new())
}

pub(crate) fn make_shared_string_value_with_extended_chars(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
    extended_chars: Vec<(usize, u32)>,
) -> Value {
    Value::StringObject(Rc::new(RefCell::new(SharedStringState {
        text,
        props: shared_string_props(&props),
        multibyte,
        extended_chars,
    })))
}

pub(crate) fn string_like_value_with_extended_chars(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
    extended_chars: Vec<(usize, u32)>,
) -> Value {
    if extended_chars.is_empty() {
        string_like_value_with_multibyte(text, props, multibyte)
    } else {
        make_shared_string_value_with_extended_chars(text, props, multibyte, extended_chars)
    }
}

pub(crate) fn string_like_value_with_multibyte(
    text: String,
    props: Vec<TextPropertySpan>,
    multibyte: bool,
) -> Value {
    // GNU string-producing primitives allocate mutable Lisp string objects
    // even when the result is ASCII and has no properties yet.  Collapsing
    // that case into Emaxx's immutable host-text representation breaks
    // subsequent `aset' and text-property mutation through aliases.
    make_shared_string_value_with_multibyte(text, props, multibyte)
}

pub(crate) fn string_like_value(text: String, props: Vec<TextPropertySpan>) -> Value {
    let multibyte = text
        .chars()
        .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
    string_like_value_with_multibyte(text, merge_string_props(props), multibyte)
}

pub(crate) fn reverse_string_like_value(value: &Value) -> Result<Value, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    let len = string.text.chars().count();
    let text = string.text.chars().rev().collect::<String>();
    let props = string
        .props
        .into_iter()
        .map(|span| TextPropertySpan {
            start: len - span.end,
            end: len - span.start,
            props: span.props,
        })
        .collect();
    Ok(string_like_value(text, merge_string_props(props)))
}

pub(crate) fn reverse_sequence_value(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    if string_like(value).is_some() {
        return reverse_string_like_value(value);
    }
    if is_bool_vector_value(interp, value) {
        let mut bits = bool_vector_bits(interp, value)?;
        bits.reverse();
        return Ok(make_bool_vector_value(interp, bits));
    }
    match value {
        Value::Cons(_) if is_vector_value(value) => {
            let mut items = value.to_vec()?;
            items[1..].reverse();
            Ok(Value::list(items))
        }
        Value::Nil | Value::Cons(_) => {
            let mut items = value.to_vec()?;
            items.reverse();
            Ok(Value::list(items))
        }
        _ => Err(LispError::TypeError("sequence".into(), value.type_name())),
    }
}

pub(crate) fn nreverse_sequence_value(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    if string_like(value).is_some() {
        return reverse_string_like_value(value);
    }
    if let Value::Record(id) = value
        && is_bool_vector_value(interp, value)
    {
        let record = interp
            .find_record_mut(*id)
            .ok_or_else(|| LispError::TypeError("bool-vector".into(), value.type_name()))?;
        record.slots.reverse();
        return Ok(value.clone());
    }
    match value {
        Value::Cons(_) if is_vector_value(value) => {
            let mut items = vector_items(value)?;
            items.reverse();
            for (index, item) in items.into_iter().enumerate() {
                aset_vector_value(value, index, item)?;
            }
            Ok(value.clone())
        }
        Value::Nil | Value::Cons(_) => nreverse_list_cells(value),
        _ => Err(LispError::TypeError("sequence".into(), value.type_name())),
    }
}

fn nreverse_list_cells(value: &Value) -> Result<Value, LispError> {
    let mut current = value.clone();
    let mut reversed = Value::Nil;
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        let cell = match &current {
            Value::Nil => return Ok(reversed),
            Value::Cons(cell) => Rc::clone(cell),
            other => return Err(LispError::TypeError("list".into(), other.type_name())),
        };
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("circular-list".into()),
                Value::String("Circular list".into()),
            ])));
        }
        let next = cell.cdr.borrow().clone();
        *cell.cdr.borrow_mut() = reversed;
        reversed = Value::Cons(cell);
        current = next;
    }
}

pub(crate) fn plist_pairs(value: &Value) -> Result<Vec<(String, Value)>, LispError> {
    let items = value.to_vec()?;
    let mut props = Vec::new();
    let mut i = 0;
    while i + 1 < items.len() {
        let key = items[i].as_symbol()?.to_string();
        props.push((key, items[i + 1].clone()));
        i += 2;
    }
    Ok(props)
}

pub(crate) fn plist_value(props: &[(String, Value)]) -> Value {
    let mut items = Vec::new();
    for (key, value) in props {
        items.push(Value::Symbol(key.clone().into()));
        items.push(value.clone());
    }
    Value::list(items)
}

pub(crate) fn object_intervals_value(
    interp: &mut Interpreter,
    object: &Value,
) -> Result<Value, LispError> {
    let (len, spans) = if let Some(string) = string_like(object) {
        (
            string.text.chars().count(),
            merge_string_props(string.props),
        )
    } else {
        let buffer_id = interp.resolve_buffer_id(object)?;
        let buffer = interp
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
        (
            buffer.point_max() - buffer.point_min(),
            buffer.substring_property_spans(buffer.point_min(), buffer.point_max()),
        )
    };

    if spans.is_empty() {
        return Ok(Value::Nil);
    }

    let mut boundaries = vec![0usize, len];
    for span in &spans {
        boundaries.push(span.start);
        boundaries.push(span.end);
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut intervals = Vec::new();
    for window in boundaries.windows(2) {
        let start = window[0];
        let end = window[1];
        if start >= end {
            continue;
        }
        let props = spans
            .iter()
            .find(|span| span.start <= start && start < span.end)
            .map(|span| plist_value(&span.props))
            .unwrap_or(Value::Nil);
        intervals.push(Value::list([
            Value::Integer(start as i64),
            Value::Integer(end as i64),
            props,
        ]));
    }

    Ok(Value::list(intervals))
}

pub(crate) fn shift_string_props(
    props: &[TextPropertySpan],
    offset: usize,
) -> Vec<TextPropertySpan> {
    props
        .iter()
        .map(|span| TextPropertySpan {
            start: span.start + offset,
            end: span.end + offset,
            props: span.props.clone(),
        })
        .collect()
}

pub(crate) fn slice_string_props(
    props: &[TextPropertySpan],
    from: usize,
    to: usize,
) -> Vec<TextPropertySpan> {
    let mut sliced = Vec::new();
    for span in props {
        let start = span.start.max(from);
        let end = span.end.min(to);
        if start < end {
            sliced.push(TextPropertySpan {
                start: start - from,
                end: end - from,
                props: span.props.clone(),
            });
        }
    }
    merge_string_props(sliced)
}

pub(crate) fn merge_string_props(mut props: Vec<TextPropertySpan>) -> Vec<TextPropertySpan> {
    props.retain(|span| span.start < span.end && !span.props.is_empty());
    props.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<TextPropertySpan> = Vec::new();
    for span in props {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && crate::buffer::text_property_plists_eq(&last.props, &span.props)
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
    }
    merged
}

pub(crate) fn property_from_category_symbol(
    interp: &Interpreter,
    props: &[(String, Value)],
    prop: &str,
) -> Option<Value> {
    if prop == "category" {
        return None;
    }
    let category = props
        .iter()
        .find(|(name, _)| name == "category")
        .and_then(|(_, value)| value.as_symbol().ok())?;
    interp.get_symbol_property(category, prop)
}

pub(crate) fn property_from_props_with_category(
    interp: &Interpreter,
    props: &[(String, Value)],
    prop: &str,
) -> Option<Value> {
    // Categories and aliases can only redirect a property already present in
    // this interval.  Most buffer positions have no properties at all, so do
    // not perform a buffer-local alias lookup for an empty interval.
    if props.is_empty() {
        return None;
    }
    let direct = props
        .iter()
        .find(|(name, _)| name == prop)
        .map(|(_, value)| value.clone())
        .or_else(|| property_from_category_symbol(interp, props, prop));
    if direct.is_some() {
        return direct;
    }
    let aliases = interp
        .buffer_local_value(interp.current_buffer_id(), "char-property-alias-alist")
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let aliases = aliases.into_iter().find_map(|entry| {
        let key = entry.car().ok()?;
        matches!(&key, Value::Symbol(name) if name == prop)
            .then(|| entry.cdr().ok()?.to_vec().ok())?
    })?;
    aliases.into_iter().find_map(|alias| {
        let alias = alias.as_symbol().ok()?;
        props
            .iter()
            .find(|(name, _)| name == alias)
            .map(|(_, value)| value.clone())
            .or_else(|| property_from_category_symbol(interp, props, alias))
    })
}

pub(crate) fn buffer_property_at_with_category(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
) -> Option<Value> {
    if pos < buffer.point_min() || pos >= buffer.point_max() {
        return None;
    }
    property_from_props_with_category(interp, buffer.text_properties_at_ref(pos), prop)
}

pub(crate) fn buffer_char_property_at(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
) -> Value {
    buffer_char_property_at_with_overlay_id(interp, buffer, pos, prop).0
}

pub(crate) fn buffer_char_property_at_with_overlay_id(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
) -> (Value, Option<u64>) {
    if let Some((value, overlay_id)) =
        highest_priority_overlay_property_with_id(interp, buffer, pos, prop, false, None)
    {
        return (value, Some(overlay_id));
    }
    (
        buffer_property_at_with_category(interp, buffer, pos, prop).unwrap_or(Value::Nil),
        None,
    )
}

pub(crate) fn overlay_property_with_category(
    interp: &Interpreter,
    overlay: &crate::overlay::Overlay,
    prop: &str,
) -> Option<Value> {
    let direct = overlay
        .plist
        .iter()
        .find(|(name, _)| matches!(name, Value::Symbol(name) if name == prop))
        .map(|(_, value)| value.clone());
    if direct.is_some() || prop == "category" {
        return direct;
    }
    let category = overlay
        .plist
        .iter()
        .find(|(name, _)| matches!(name, Value::Symbol(name) if name == "category"))
        .and_then(|(_, value)| value.as_symbol().ok())?;
    interp.get_symbol_property(category, prop)
}

pub(crate) fn string_property_at(value: &Value, pos: usize, prop: &str) -> Option<Value> {
    let string = string_like(value)?;
    string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .and_then(|span| {
            span.props
                .iter()
                .find(|(name, _)| name == prop)
                .map(|(_, value)| value.clone())
        })
}

pub(crate) fn string_property_at_with_category(
    interp: &Interpreter,
    value: &Value,
    pos: usize,
    prop: &str,
) -> Option<Value> {
    let string = string_like(value)?;
    let span = string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)?;
    property_from_props_with_category(interp, &span.props, prop)
}

pub(crate) fn text_property_search_buffer(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    start: usize,
    end: usize,
    prop: &str,
    wanted: &Value,
    want_match: bool,
) -> Option<usize> {
    let start = start.max(buffer.point_min());
    let end = end.min(buffer.point_max());
    if start >= end {
        return None;
    }
    for pos in start..end {
        let matches = values_equal(
            interp,
            &buffer.text_property_at(pos, prop).unwrap_or(Value::Nil),
            wanted,
        );
        if matches == want_match {
            return Some(pos);
        }
    }
    None
}

pub(crate) fn text_property_search_string(
    interp: &Interpreter,
    value: &Value,
    start: usize,
    end: usize,
    prop: &str,
    wanted: &Value,
    want_match: bool,
) -> Option<usize> {
    let len = string_text(value).ok()?.chars().count();
    let start = start.min(len);
    let end = end.min(len);
    if start >= end {
        return None;
    }
    for pos in start..end {
        let matches = values_equal(
            interp,
            &string_property_at(value, pos, prop).unwrap_or(Value::Nil),
            wanted,
        );
        if matches == want_match {
            return Some(pos);
        }
    }
    None
}

pub(crate) fn string_properties_at(value: &Value, pos: usize) -> Vec<(String, Value)> {
    string_like(value)
        .and_then(|string| {
            string
                .props
                .iter()
                .find(|span| span.start <= pos && pos < span.end)
                .map(|span| span.props.clone())
        })
        .unwrap_or_default()
}

pub(crate) fn merge_string_object_props(
    mut spans: Vec<StringPropertySpan>,
) -> Vec<StringPropertySpan> {
    spans.retain(|span| span.start < span.end && !span.props.is_empty());
    spans.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<StringPropertySpan> = Vec::new();
    for span in spans {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && crate::buffer::text_property_plists_eq(&last.props, &span.props)
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
    }
    merged
}

pub(crate) fn string_object_properties_at(
    spans: &[StringPropertySpan],
    pos: usize,
) -> Vec<(String, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

pub(crate) fn modify_shared_string_properties<F>(
    value: &Value,
    start: usize,
    end: usize,
    mut f: F,
) -> Result<(), LispError>
where
    F: FnMut(Vec<(String, Value)>) -> Vec<(String, Value)>,
{
    let Value::StringObject(state) = value else {
        return Err(LispError::TypeError("string".into(), value.type_name()));
    };
    let mut state = state.borrow_mut();
    let len = state.text.chars().count();
    let start = start.min(len);
    let end = end.min(len);
    if start >= end {
        return Ok(());
    }

    let original = state.props.clone();
    let mut updated = Vec::new();
    for span in &original {
        if span.end <= start || span.start >= end {
            updated.push(span.clone());
        } else {
            if span.start < start {
                updated.push(StringPropertySpan {
                    start: span.start,
                    end: start,
                    props: span.props.clone(),
                });
            }
            if span.end > end {
                updated.push(StringPropertySpan {
                    start: end,
                    end: span.end,
                    props: span.props.clone(),
                });
            }
        }
    }

    let mut boundaries = vec![start, end];
    for span in &original {
        if span.end <= start || span.start >= end {
            continue;
        }
        boundaries.push(span.start.max(start));
        boundaries.push(span.end.min(end));
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    for window in boundaries.windows(2) {
        let seg_start = window[0];
        let seg_end = window[1];
        if seg_start >= seg_end {
            continue;
        }
        let current = string_object_properties_at(&original, seg_start);
        let next = f(current);
        if !next.is_empty() {
            updated.push(StringPropertySpan {
                start: seg_start,
                end: seg_end,
                props: next,
            });
        }
    }

    state.props = merge_string_object_props(updated);
    Ok(())
}
