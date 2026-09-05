use super::*;

fn vector_value(items: impl IntoIterator<Item = Value>) -> Value {
    Value::vector(items)
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

fn char_table_id_from_var(interp: &Interpreter, env: &Env, name: &str) -> Option<u64> {
    match interp.lookup_var(name, env)? {
        Value::CharTable(id) => Some(id),
        _ => None,
    }
}

/// composite.c `char_composable_p': anything from space upward composes
/// unless its general category is Z* or C*, with ZWNJ/ZWJ, the TR51 tag
/// range, and Zs carved back in.  `unicode-category-table' stores the raw
/// fixnum encoding of `enum unicode_category' (character.h); every
/// composable category sorts at or before Zs, index 23.
fn char_composable_p(interp: &Interpreter, env: &Env, c: char) -> bool {
    const ZERO_WIDTH_NON_JOINER: u32 = 0x200C;
    const ZERO_WIDTH_JOINER: u32 = 0x200D;
    const TAG_SPACE: u32 = 0xE0020;
    const CANCEL_TAG: u32 = 0xE007F;
    const UNICODE_CATEGORY_ZS: i64 = 23;
    let c = c as u32;
    c >= ' ' as u32
        && (c == ZERO_WIDTH_NON_JOINER
            || c == ZERO_WIDTH_JOINER
            || (TAG_SPACE..=CANCEL_TAG).contains(&c)
            || matches!(
                char_table_id_from_var(interp, env, "unicode-category-table")
                    .and_then(|id| interp.char_table_get(id, c)),
                Some(Value::Integer(category)) if category <= UNICODE_CATEGORY_ZS
            ))
}

/// composite.c `inhibit_auto_composition': `auto-composition-mode' nil
/// turns automatic composition off entirely; a string value turns it off
/// only on the terminal type it names.
fn inhibit_auto_composition(interp: &Interpreter, env: &Env) -> bool {
    let Some(mode) = interp.lookup_var("auto-composition-mode", env) else {
        return true;
    };
    if mode.is_nil() {
        return true;
    }
    if let Some(mode) = string_like(&mode) {
        return interp
            .tty_terminal_type()
            .is_some_and(|terminal_type| terminal_type == mode.text);
    }
    false
}

fn target_min(interp: &Interpreter, string: &Value) -> usize {
    if string.is_nil() {
        interp.buffer.point_min()
    } else {
        0
    }
}

fn target_char_at(interp: &Interpreter, string: &Value, pos: usize) -> Option<char> {
    if string.is_nil() {
        interp.buffer.char_at(pos)
    } else {
        string_like(string)?.text.chars().nth(pos)
    }
}

fn target_text(
    interp: &Interpreter,
    string: &Value,
    from: usize,
    to: usize,
) -> Result<String, LispError> {
    if string.is_nil() {
        interp
            .buffer
            .buffer_substring(from, to)
            .map_err(|error| LispError::Signal(error.to_string()))
    } else {
        Ok(string_like(string)
            .map(|text| text.text.chars().skip(from).take(to - from).collect())
            .unwrap_or_default())
    }
}

/// `composition-function-table' holds, per character, a list of
/// [PATTERN LOOKBACK FUNC] rules.  The C walks only a proper cons chain
/// (`for (...; CONSP (val); val = XCDR (val))'); a bare vector is ignored.
fn composition_rules_for_char(interp: &Interpreter, env: &Env, c: char) -> Vec<Value> {
    let Some(table_id) = char_table_id_from_var(interp, env, "composition-function-table") else {
        return Vec::new();
    };
    let Some(value) = interp.char_table_get(table_id, c as u32) else {
        return Vec::new();
    };
    let mut rules = Vec::new();
    let mut tail = value;
    while let Some((head, rest)) = tail.cons_values() {
        rules.push(head);
        tail = rest;
    }
    rules
}

/// LGSTRING_CHAR_LEN: the number of characters a glyph-string covers is
/// the length of its header vector minus the leading font slot.
fn lgstring_char_len(gstring: &Value) -> Option<usize> {
    let items = vector_items(gstring).ok()?;
    let header = items.first()?;
    Some(vector_items(header).ok()?.len().saturating_sub(1))
}

/// composite.c `autocmp_chars': try to compose the characters at CHARPOS
/// according to RULE ([PATTERN LOOKBACK FUNC]), bounded by LIMIT.  Match
/// data is saved and restored around the attempt
/// (`record_unwind_save_match_data'), and point is restored after calling
/// out to Lisp on the buffer path (`restore_point_unwind').
fn autocmp_chars(
    interp: &mut Interpreter,
    env: &mut Env,
    rule_items: &[Value],
    charpos: usize,
    limit: usize,
    string: &Value,
) -> Result<Value, LispError> {
    let saved_match_data = interp.last_match_data.clone();
    let saved_match_buffer = interp.last_match_data_buffer_id;
    let saved_point = string.is_nil().then(|| interp.buffer.point());
    let result = autocmp_chars_inner(interp, env, rule_items, charpos, limit, string);
    if let Some(point) = saved_point {
        let clamped = point.clamp(interp.buffer.point_min(), interp.buffer.point_max());
        interp.buffer.goto_char(clamped);
    }
    interp.last_match_data = saved_match_data;
    interp.last_match_data_buffer_id = saved_match_buffer;
    result
}

fn autocmp_chars_inner(
    interp: &mut Interpreter,
    env: &mut Env,
    rule_items: &[Value],
    charpos: usize,
    limit: usize,
    string: &Value,
) -> Result<Value, LispError> {
    let pattern_value = &rule_items[0];
    let len = if pattern_value.is_nil() {
        1
    } else {
        let Some(pattern) = string_like(pattern_value) else {
            // Non-string, non-nil PATTERN: the rule matches nothing.
            return Ok(Value::Nil);
        };
        let haystack = target_text(interp, string, charpos, limit)?;
        let at_absolute_start = charpos == target_min(interp, string);
        match regexp::fast_looking_at_chars(interp, &pattern, &haystack, at_absolute_start)? {
            Some(len) if len > 0 => len,
            _ => return Ok(Value::Nil),
        }
    };
    let to = charpos + len;
    // On a character terminal the font object handed down the shaping
    // pipeline is the frame itself (autocmp_chars: font_object =
    // win->frame; the window-system font_range branch is compiled out).
    let font_object = interp.selected_frame_value();
    // GNU fetches the glyph-string here to check for a cached shaped ID.
    // Emaxx never registers shaped glyph-strings in a cache, so the ID is
    // always nil and control continues to `auto-composition-function'
    // exactly as GNU does on a cache miss; only the fetch's own validation
    // (unibyte text, invalid region) remains observable.
    get_glyph_string(
        interp,
        env,
        &[
            Value::Integer(charpos as i64),
            Value::Integer(to as i64),
            font_object.clone(),
            string.clone(),
        ],
    )?;
    let function = interp
        .lookup_var("auto-composition-function", env)
        .unwrap_or(Value::Nil);
    let call = interp.call_function_value(
        function,
        None,
        &[
            rule_items[2].clone(),
            Value::Integer(charpos as i64),
            Value::Integer(to as i64),
            font_object,
            string.clone(),
            Value::Nil,
        ],
        env,
    );
    match call {
        Ok(value) => Ok(value),
        Err(error @ LispError::Throw(..)) => Err(error),
        // safe_calln: a signaled condition absorbs into a failed
        // composition attempt.
        Err(_) => Ok(Value::Nil),
    }
}

struct AutoCompositionMatch {
    start: usize,
    end: usize,
    gstring: Value,
}

/// composite.c `find_automatic_composition' on the BACKLIM = -1 path used
/// by `find-composition-internal': search for an automatic composition at
/// or near POS, walking back across composable characters to a safe start
/// and matching `composition-function-table' rules forward.
fn find_automatic_composition(
    interp: &mut Interpreter,
    env: &mut Env,
    pos: usize,
    limit: Option<usize>,
    string: &Value,
) -> Result<Option<AutoCompositionMatch>, LispError> {
    const MAX_AUTO_COMPOSITION_LOOKBACK: usize = 3;
    // The C first locates a window showing the current buffer; with none
    // (an undisplayed temp buffer -- even when the target is a string)
    // there is no automatic composition.
    let window = super::display::call(interp, "get-buffer-window", &[Value::Nil, Value::Nil], env)?;
    if window.is_nil() {
        return Ok(None);
    }

    let (head, tail) = if string.is_nil() {
        // BACKLIM is -1 here, so the backward search stops at the first
        // newline before POS: a newline can never be composed.  (GNU's
        // long-line-optimizations narrowing is not modeled.)
        let min = interp.buffer.point_min();
        let before = interp
            .buffer
            .buffer_substring(min, pos)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let head = before
            .rfind('\n')
            .map(|byte| min + before[..byte].chars().count() + 1)
            .unwrap_or(min);
        (head, interp.buffer.point_max())
    } else {
        (
            0,
            string_like(string)
                .map(|text| text.text.chars().count())
                .unwrap_or(0),
        )
    };
    // A negative LIMIT means: find a composition covering the character
    // after POS.
    let limit_value = limit.unwrap_or(pos);
    let mut fore_check_limit = if limit_value <= pos {
        tail.min(pos + 1 + MAX_AUTO_COMPOSITION_LOOKBACK)
    } else {
        tail.min(limit_value + MAX_AUTO_COMPOSITION_LOOKBACK)
    };

    let mut cur = pos;
    loop {
        let composable =
            target_char_at(interp, string, cur).is_some_and(|c| char_composable_p(interp, env, c));
        if !composable && limit_value <= pos {
            // Case (1): rewind to the previous composable character.
            loop {
                if cur <= limit_value {
                    return Ok(None);
                }
                cur -= 1;
                if target_char_at(interp, string, cur)
                    .is_some_and(|c| char_composable_p(interp, env, c))
                {
                    break;
                }
            }
            fore_check_limit = cur + 1;
        }
        if composable || limit_value <= pos {
            // Rewind to a position where forward search is safe: just
            // after the nearest non-composable character (or HEAD).
            while head < cur {
                let prev = cur;
                cur -= 1;
                if !target_char_at(interp, string, cur)
                    .is_some_and(|c| char_composable_p(interp, env, c))
                {
                    cur = prev;
                    break;
                }
            }
        }

        // search_forward:
        let mut last: Option<AutoCompositionMatch> = None;
        let scan_start = cur;
        while cur < fore_check_limit {
            let Some(c) = target_char_at(interp, string, cur) else {
                break;
            };
            let mut advanced = false;
            for rule in composition_rules_for_char(interp, env, c) {
                let Ok(items) = vector_items(&rule) else {
                    continue;
                };
                if items.len() != 3 {
                    continue;
                }
                let Value::Integer(lookback) = items[1] else {
                    continue;
                };
                if lookback < 0 || lookback as usize > cur {
                    continue;
                }
                let check_pos = cur - lookback as usize;
                if check_pos < head {
                    continue;
                }
                if limit_value <= pos {
                    if pos < check_pos {
                        continue;
                    }
                } else if limit_value <= check_pos {
                    continue;
                }
                let gstring = autocmp_chars(interp, env, &items, check_pos, tail, string)?;
                // The C stores every attempt into *gstring: a later failed
                // attempt clears an earlier non-target success.
                let char_len = if gstring.is_nil() {
                    None
                } else {
                    lgstring_char_len(&gstring).filter(|len| *len > 0)
                };
                let Some(char_len) = char_len else {
                    last = None;
                    continue;
                };
                let start = check_pos;
                let end = check_pos + char_len;
                let found = AutoCompositionMatch {
                    start,
                    end,
                    gstring,
                };
                let is_target = if pos < limit_value {
                    pos < end
                } else {
                    start <= pos && pos < end
                };
                if is_target {
                    return Ok(Some(found));
                }
                last = Some(found);
                cur = end;
                advanced = true;
                break;
            }
            if !advanced {
                cur += 1;
            }
        }

        if pos < limit_value {
            // Cases (2) and (4): a single forward pass decides.
            return Ok(None);
        }
        if last.is_some() {
            // A composition was found past POS; the caller checks whether
            // it actually covers POS.
            return Ok(last);
        }
        if scan_start == head {
            return Ok(None);
        }
        cur = scan_start - 1;
    }
}

fn terminal_font(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    if value.is_nil()
        || matches!(value, Value::Terminal(0))
        || matches!(value, Value::Frame(id) if interp.frame_is_live(*id))
    {
        Ok(Value::symbol(&interp.effective_terminal_coding_system()))
    } else {
        Err(wrong_type_argument("terminal-live-p", value.clone()))
    }
}

/// fill_gstring_body reads the fontless glyph width from
/// `char-width-table' (XFIXNAT (CHAR_TABLE_REF (Vchar_width_table, c))),
/// not from any host notion of display width.
fn char_width_table_width(interp: &Interpreter, env: &Env, character: char) -> i64 {
    char_table_id_from_var(interp, env, "char-width-table")
        .and_then(|id| interp.char_table_get(id, character as u32))
        .and_then(|value| value.as_integer().ok())
        .unwrap_or(0)
        .max(0)
}

fn glyph_string(
    interp: &Interpreter,
    env: &Env,
    chars: &[char],
    font: Value,
    compose_cluster: bool,
) -> Value {
    let header = vector_value(
        std::iter::once(font).chain(
            chars
                .iter()
                .map(|character| Value::Integer(*character as i64)),
        ),
    );
    let cluster_end = chars.len().saturating_sub(1) as i64;
    let glyphs = chars.iter().enumerate().map(|(index, character)| {
        let width = char_width_table_width(interp, env, *character);
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

fn find_composition(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
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
        if !(range.start <= position && position < range.end) {
            // A static composition was found through the LIMIT search but
            // does not cover POS; an automatic composition closer to POS
            // wins (Ffind_composition_internal's second
            // find_automatic_composition call carries no multibyte or
            // inhibition guard).
            let string = string.clone();
            if let Some(found) = find_automatic_composition(interp, env, position, limit, &string)?
            {
                let better = if found.end <= position {
                    found.end > range.end
                } else {
                    found.start < range.start
                };
                if better {
                    return Ok(Value::list([
                        Value::Integer(found.start as i64),
                        Value::Integer(found.end as i64),
                        found.gstring,
                    ]));
                }
            }
        }
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
    // No static composition: try the automatic one, gated exactly as
    // Ffind_composition_internal gates it -- a multibyte target and
    // auto-composition not inhibited.  The shaping itself runs through the
    // GNU Lisp owners (`auto-compose-chars',
    // `compose-gstring-for-terminal') over the ported C substrate.
    let multibyte = if string.is_nil() {
        interp.buffer.is_multibyte()
    } else {
        string_like(string).is_some_and(|text| text.multibyte)
    };
    if multibyte && !inhibit_auto_composition(interp, env) {
        let string = string.clone();
        if let Some(found) = find_automatic_composition(interp, env, position, limit, &string)? {
            return Ok(Value::list([
                Value::Integer(found.start as i64),
                Value::Integer(found.end as i64),
                found.gstring,
            ]));
        }
    }
    Ok(Value::Nil)
}

fn get_glyph_string(interp: &Interpreter, env: &Env, args: &[Value]) -> Result<Value, LispError> {
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
    Ok(glyph_string(interp, env, &chars, font, false))
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
        env: &mut Env,
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
                get_glyph_string(interp, env, args)
            }
            "composition-sort-rules" => {
                need_args(name, args, 1)?;
                sort_rules(&args[0])
            }
            "find-composition-internal" => {
                need_args(name, args, 4)?;
                find_composition(interp, env, args)
            }
        }
    }
);
