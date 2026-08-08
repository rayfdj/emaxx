use super::*;

fn vector_value(items: impl IntoIterator<Item = Value>) -> Value {
    Value::list(std::iter::once(Value::symbol("vector-literal")).chain(items))
}

fn args_out_of_range(values: impl IntoIterator<Item = Value>) -> LispError {
    LispError::SignalValue(Value::list(
        std::iter::once(Value::symbol("args-out-of-range")).chain(values),
    ))
}

fn current_buffer_value(interp: &Interpreter) -> Value {
    Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone())
}

fn checked_buffer_region(
    interp: &Interpreter,
    start: &Value,
    end: &Value,
) -> Result<(usize, usize), LispError> {
    let from = position_from_value(interp, start)?;
    let to = position_from_value(interp, end)?;
    if from < interp.buffer.point_min()
        || from > interp.buffer.point_max()
        || to < interp.buffer.point_min()
        || to > interp.buffer.point_max()
    {
        return Err(args_out_of_range([
            current_buffer_value(interp),
            start.clone(),
            end.clone(),
        ]));
    }
    Ok(if from <= to { (from, to) } else { (to, from) })
}

fn checked_string_region(
    string: &Value,
    start: &Value,
    end: &Value,
) -> Result<(usize, usize), LispError> {
    let text = string_like(string).ok_or_else(|| wrong_type_argument("stringp", string.clone()))?;
    let len = text.text.chars().count() as i64;
    let normalize = |value: &Value| -> Result<i64, LispError> {
        let raw = value.as_integer()?;
        Ok(if raw < 0 { len + raw } else { raw })
    };
    let from = normalize(start)?;
    let to = normalize(end)?;
    if from < 0 || to < 0 || from > to || to > len {
        return Err(args_out_of_range([
            string.clone(),
            start.clone(),
            end.clone(),
        ]));
    }
    Ok((from as usize, to as usize))
}

fn composition_property(length: usize, components: Value, modification: Value) -> Value {
    Value::cons(
        Value::cons(Value::Integer(length as i64), components),
        modification,
    )
}

fn compose_region(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    let (start, end) = checked_buffer_region(interp, &args[0], &args[1])?;
    let components = args.get(2).cloned().unwrap_or(Value::Nil);
    if !matches!(
        components,
        Value::Nil | Value::Integer(_) | Value::Cons(_) | Value::String(_) | Value::StringObject(_)
    ) && !is_vector_value(&components)
    {
        return Err(wrong_type_argument("vectorp", components));
    }
    let modification = args.get(3).cloned().unwrap_or(Value::Nil);
    let property = composition_property(end - start, components, modification);
    interp
        .buffer
        .put_text_property(start, end, "composition", property);
    Ok(Value::Nil)
}

fn compose_string(args: &[Value]) -> Result<Value, LispError> {
    if string_like(&args[0]).is_none() {
        return Err(wrong_type_argument("stringp", args[0].clone()));
    }
    let (start, end) = checked_string_region(&args[0], &args[1], &args[2])?;
    let components = args.get(3).cloned().unwrap_or(Value::Nil);
    let modification = args.get(4).cloned().unwrap_or(Value::Nil);
    let property = composition_property(end - start, components, modification);
    modify_shared_string_properties(&args[0], start, end, |mut properties| {
        if let Some((_, value)) = properties
            .iter_mut()
            .find(|(name, _)| name == "composition")
        {
            *value = property.clone();
        } else {
            properties.insert(0, ("composition".into(), property.clone()));
        }
        properties
    })?;
    Ok(args[0].clone())
}

#[derive(Clone)]
struct CompositionRange {
    start: usize,
    end: usize,
    property: Value,
}

fn composition_ranges(
    interp: &Interpreter,
    string: &Value,
) -> Result<Vec<CompositionRange>, LispError> {
    let spans = if string.is_nil() {
        interp
            .buffer
            .full_property_spans()
            .into_iter()
            .map(|span| (span.start, span.end, span.props))
            .collect::<Vec<_>>()
    } else {
        let string =
            string_like(string).ok_or_else(|| wrong_type_argument("stringp", string.clone()))?;
        string
            .props
            .into_iter()
            .map(|span| (span.start, span.end, span.props))
            .collect::<Vec<_>>()
    };
    Ok(spans
        .into_iter()
        .filter_map(|(start, end, properties)| {
            properties
                .into_iter()
                .find(|(name, _)| name == "composition")
                .map(|(_, property)| CompositionRange {
                    start,
                    end,
                    property,
                })
        })
        .collect())
}

fn find_static_composition(
    ranges: &[CompositionRange],
    position: usize,
    limit: Option<usize>,
) -> Option<CompositionRange> {
    if let Some(range) = ranges
        .iter()
        .find(|range| range.start <= position && position < range.end)
    {
        return Some(range.clone());
    }
    let limit = limit?;
    if limit > position {
        ranges
            .iter()
            .filter(|range| position <= range.start && range.start < limit)
            .min_by_key(|range| range.start)
            .cloned()
    } else if limit < position {
        ranges
            .iter()
            .filter(|range| limit < range.end && range.end <= position)
            .max_by_key(|range| range.end)
            .cloned()
    } else {
        None
    }
}

fn composition_chars(
    interp: &Interpreter,
    string: &Value,
    start: usize,
    end: usize,
) -> Result<Vec<char>, LispError> {
    let text = if string.is_nil() {
        interp
            .buffer
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?
    } else {
        string_like(string)
            .ok_or_else(|| wrong_type_argument("stringp", string.clone()))?
            .text
            .chars()
            .skip(start)
            .take(end - start)
            .collect()
    };
    Ok(text.chars().collect())
}

fn components_vector(components: &Value, chars: &[char]) -> Result<Value, LispError> {
    let items = match components {
        Value::Nil => chars
            .iter()
            .map(|character| Value::Integer(*character as i64))
            .collect(),
        Value::Integer(_) => vec![components.clone()],
        Value::String(_) | Value::StringObject(_) => string_like(components)
            .expect("string variant must be string-like")
            .text
            .chars()
            .map(|character| Value::Integer(character as i64))
            .collect(),
        value if is_vector_value(value) => vector_items(value)?,
        Value::Cons(_) => components.to_vec()?,
        _ => return Err(LispError::Signal("Invalid composition".into())),
    };
    Ok(vector_value(items))
}

fn character_width(value: &Value) -> i64 {
    let Ok(codepoint) = value.as_integer() else {
        return 0;
    };
    if codepoint == '\t' as i64 {
        return 1;
    }
    u32::try_from(codepoint)
        .ok()
        .and_then(char::from_u32)
        .and_then(|character| character.width())
        .unwrap_or(0) as i64
}

fn rule_composition_width(items: &[Value]) -> i64 {
    let mut leftmost = 0.0_f64;
    let mut rightmost = items.first().map(character_width).unwrap_or(0) as f64;
    let mut index = 1;
    while index + 1 < items.len() {
        let rule = items[index].as_integer().unwrap_or(0) & 0xff;
        let global_reference = (rule / 12).min(11);
        let new_reference = rule % 12;
        let width = character_width(&items[index + 1]) as f64;
        let left = leftmost + (global_reference % 3) as f64 * (rightmost - leftmost) / 2.0
            - (new_reference % 3) as f64 * width / 2.0;
        leftmost = leftmost.min(left);
        rightmost = rightmost.max(left + width);
        index += 2;
    }
    (rightmost - leftmost).ceil() as i64
}

fn register_composition(
    interp: &mut Interpreter,
    range: &CompositionRange,
    string: &Value,
) -> Result<Option<(Value, bool, Value, i64)>, LispError> {
    let Some((head, tail)) = range.property.cons_values() else {
        return Ok(None);
    };
    if let Value::Integer(id) = head {
        let Ok(index) = usize::try_from(id) else {
            return Ok(None);
        };
        let Some((length, rest)) = tail.cons_values() else {
            return Ok(None);
        };
        if length != Value::Integer((range.end - range.start) as i64) {
            return Ok(None);
        }
        let Some((_, modification)) = rest.cons_values() else {
            return Ok(None);
        };
        let Some(state) = interp.composition_states.get(index) else {
            return Ok(None);
        };
        return Ok(Some((
            state.components.clone(),
            state.relative,
            modification,
            state.width,
        )));
    }

    let Some((length, components)) = head.cons_values() else {
        return Ok(None);
    };
    if length != Value::Integer((range.end - range.start) as i64) {
        return Ok(None);
    }
    if !matches!(
        components,
        Value::Nil | Value::Integer(_) | Value::String(_) | Value::StringObject(_) | Value::Cons(_)
    ) && !is_vector_value(&components)
    {
        return Ok(None);
    }
    let chars = composition_chars(interp, string, range.start, range.end)?;
    let key = components_vector(&components, &chars)?;
    let key_items = vector_items(&key)?;
    let rule_based = matches!(components, Value::Cons(_)) || is_vector_value(&components);
    if rule_based {
        let glyph_string = key_items.first().is_some_and(is_vector_value);
        let valid = if glyph_string {
            key_items.iter().all(is_vector_value)
        } else {
            key_items.len() % 2 == 1
                && key_items
                    .iter()
                    .all(|value| matches!(value, Value::Integer(_)))
        };
        if !valid {
            return Ok(None);
        }
    }
    let relative = !rule_based;
    let width = if rule_based {
        rule_composition_width(&key_items)
    } else {
        key_items.iter().map(character_width).max().unwrap_or(0)
    };
    let id = interp
        .composition_states
        .iter()
        .position(|state| values_equal(interp, &state.components, &key))
        .unwrap_or_else(|| {
            let id = interp.composition_states.len();
            interp
                .composition_states
                .push(crate::lisp::eval::CompositionState {
                    components: key.clone(),
                    relative,
                    width,
                });
            id
        });
    range.property.set_car(Value::Integer(id as i64))?;
    range.property.set_cdr(Value::cons(
        Value::Integer((range.end - range.start) as i64),
        Value::cons(key.clone(), tail.clone()),
    ))?;
    Ok(Some((key, relative, tail, width)))
}

fn terminal_font(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    if value.is_nil()
        || matches!(value, Value::Terminal(0))
        || matches!(value, Value::Frame(id) if interp.frame_is_live(*id))
    {
        Ok(Value::symbol("utf-8-unix"))
    } else {
        Err(wrong_type_argument("terminal-live-p", value.clone()))
    }
}

fn glyph_string(chars: &[char], font: Value, compose_cluster: bool) -> Value {
    let header = vector_value(
        std::iter::once(font).chain(
            chars
                .iter()
                .map(|character| Value::Integer(*character as i64)),
        ),
    );
    let cluster_end = chars.len().saturating_sub(1) as i64;
    let glyphs = chars.iter().enumerate().map(|(index, character)| {
        let width = character.width().unwrap_or(0) as i64;
        let from = if compose_cluster { 0 } else { index as i64 };
        let to = if compose_cluster {
            cluster_end
        } else {
            index as i64
        };
        vector_value([
            Value::Integer(from),
            Value::Integer(to),
            Value::Integer(*character as i64),
            Value::Integer(*character as i64),
            Value::Integer(width),
            Value::Integer(0),
            Value::Integer(width),
            Value::Integer(1),
            Value::Integer(0),
            Value::Nil,
        ])
    });
    let mut body = vec![header, Value::Nil];
    body.extend(glyphs);
    body.resize(body.len().max(10), Value::Nil);
    vector_value(body)
}

fn automatic_composition(
    interp: &Interpreter,
    string: &Value,
    position: usize,
) -> Result<Option<(usize, usize, Value)>, LispError> {
    let (text, base) = if string.is_nil() {
        (interp.buffer.full_buffer_string(), 1)
    } else {
        (
            string_like(string)
                .ok_or_else(|| wrong_type_argument("stringp", string.clone()))?
                .text,
            0,
        )
    };
    let index = position.saturating_sub(base);
    if index >= text.chars().count() {
        return Ok(None);
    }
    use unicode_segmentation::UnicodeSegmentation;
    let mut offset = 0;
    for cluster in text.graphemes(true) {
        let length = cluster.chars().count();
        if offset <= index && index < offset + length {
            if length <= 1 {
                return Ok(None);
            }
            let chars = cluster.chars().collect::<Vec<_>>();
            return Ok(Some((
                offset + base,
                offset + length + base,
                glyph_string(&chars, Value::symbol("utf-8-unix"), true),
            )));
        }
        offset += length;
    }
    Ok(None)
}

fn find_composition(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    let string = &args[2];
    let position = position_from_value(interp, &args[0])?;
    let (minimum, maximum) = if string.is_nil() {
        (interp.buffer.point_min(), interp.buffer.point_max())
    } else {
        let string =
            string_like(string).ok_or_else(|| wrong_type_argument("stringp", string.clone()))?;
        (0, string.text.chars().count())
    };
    if !(minimum..=maximum).contains(&position) {
        let object = if string.is_nil() {
            current_buffer_value(interp)
        } else {
            string.clone()
        };
        return Err(args_out_of_range([object, args[0].clone()]));
    }
    let limit = if args[1].is_nil() {
        None
    } else {
        let raw = match &args[1] {
            Value::Integer(value) => *value,
            Value::Marker(id) => interp
                .marker_position(*id)
                .map(|position| position as i64)
                .ok_or_else(|| wrong_type_argument("integer-or-marker-p", args[1].clone()))?,
            value => {
                return Err(wrong_type_argument("integer-or-marker-p", value.clone()));
            }
        };
        Some(raw.clamp(minimum as i64, maximum as i64) as usize)
    };
    let ranges = composition_ranges(interp, string)?;
    if let Some(range) = find_static_composition(&ranges, position, limit) {
        let start = Value::Integer(range.start as i64);
        let end = Value::Integer(range.end as i64);
        let Some((components, relative, modification, width)) =
            register_composition(interp, &range, string)?
        else {
            return Ok(Value::list([start, end, Value::Nil]));
        };
        if !args[3].is_truthy() {
            return Ok(Value::list([start, end, Value::T]));
        }
        let copied_components = vector_value(vector_items(&components)?);
        return Ok(Value::list([
            start,
            end,
            copied_components,
            if relative { Value::T } else { Value::Nil },
            modification,
            Value::Integer(width),
        ]));
    }
    if let Some((start, end, gstring)) = automatic_composition(interp, string, position)? {
        return Ok(Value::list([
            Value::Integer(start as i64),
            Value::Integer(end as i64),
            gstring,
        ]));
    }
    Ok(Value::Nil)
}

fn get_glyph_string(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    let font = terminal_font(interp, &args[2])?;
    let chars = if args[3].is_nil() {
        let (start, end) = checked_buffer_region(interp, &args[0], &args[1])?;
        composition_chars(interp, &Value::Nil, start, end)?
    } else {
        let (start, end) = checked_string_region(&args[3], &args[0], &args[1])?;
        composition_chars(interp, &args[3], start, end)?
    };
    if chars.is_empty() {
        return Err(LispError::Signal(
            "Attempt to shape zero-length text".into(),
        ));
    }
    Ok(glyph_string(&chars, font, false))
}

fn sort_rules(rules: &Value) -> Result<Value, LispError> {
    let original = rules;
    let mut rules = original
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", original.clone()))?;
    if rules.len() <= 1 {
        return Ok(original.clone());
    }
    let mut keyed = Vec::with_capacity(rules.len());
    for rule in rules.drain(..) {
        let valid = vector_items(&rule).ok().filter(|items| {
            items.len() == 3 && matches!(items.get(1), Some(Value::Integer(value)) if *value >= 0)
        });
        let Some(items) = valid else {
            return Err(LispError::Signal(
                "Invalid composition rule in RULES argument".into(),
            ));
        };
        keyed.push((items[1].as_integer().unwrap_or(0), rule));
    }
    keyed.sort_by_key(|(lookback, _)| std::cmp::Reverse(*lookback));
    Ok(Value::list(keyed.into_iter().map(|(_, rule)| rule)))
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "clear-composition-cache" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "compose-region-internal" => {
                need_arg_range(name, args, 2, 4)?;
                compose_region(interp, args)
            }
            "compose-string-internal" => {
                need_arg_range(name, args, 3, 5)?;
                compose_string(args)
            }
            "composition-get-gstring" => {
                need_args(name, args, 4)?;
                get_glyph_string(interp, args)
            }
            "composition-sort-rules" => {
                need_args(name, args, 1)?;
                sort_rules(&args[0])
            }
            "find-composition-internal" => {
                need_args(name, args, 4)?;
                find_composition(interp, args)
            }
        }
    }
);
