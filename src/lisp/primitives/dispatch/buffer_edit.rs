use super::*;

// cmds.c:236/288: `self-insert-command' and `delete-char' amalgamate
// their undo with a preceding run of the same command when the count's
// magnitude is below 2 -- through simple.el's own `undo-auto-amalgamate',
// reached as an ordinary Lisp call.
fn call_undo_auto_amalgamate(interp: &mut Interpreter, env: &mut Env) {
    if interp.lookup_function("undo-auto-amalgamate", env).is_ok() {
        let _ = interp.call_function_value(
            Value::Symbol("undo-auto-amalgamate".into()),
            Some("undo-auto-amalgamate"),
            &[],
            env,
        );
    }
}

fn word_syntax_at(interp: &Interpreter, env: &Env, position: usize) -> bool {
    super::super::syntax::syntax_class_at_buffer_position_matches(interp, env, position, 'w')
}

fn word_boundary_function(
    interp: &Interpreter,
    env: &Env,
    character: char,
) -> Option<(Value, String)> {
    let Value::CharTable(table_id) = interp.lookup_var("find-word-boundary-function-table", env)?
    else {
        return None;
    };
    let function_name = interp
        .char_table_get(table_id, character as u32)?
        .as_symbol()
        .ok()?
        .to_owned();
    let function = interp.lookup_function(&function_name, env).ok()?;
    Some((function, function_name))
}

fn call_word_boundary_function(
    interp: &mut Interpreter,
    env: &mut Env,
    function: Value,
    function_name: &str,
    position: usize,
    limit: usize,
) -> Result<Option<usize>, LispError> {
    let result = interp.call_function_value(
        function,
        Some(function_name),
        &[
            Value::Integer(position as i64),
            Value::Integer(limit as i64),
        ],
        env,
    )?;
    Ok(match result {
        Value::Integer(position) => usize::try_from(position).ok(),
        _ => None,
    })
}

fn move_forward_one_word(interp: &mut Interpreter, env: &mut Env) -> Result<bool, LispError> {
    let end = interp.buffer.point_max();
    while interp.buffer.point() < end && !word_syntax_at(interp, env, interp.buffer.point()) {
        let _ = interp.buffer.forward_char(1);
    }
    if interp.buffer.point() == end {
        return Ok(false);
    }

    let first = interp.buffer.point();
    let character = interp
        .buffer
        .char_at(first)
        .expect("point before point-max has a character");
    let _ = interp.buffer.forward_char(1);
    if let Some((function, function_name)) = word_boundary_function(interp, env, character) {
        let after_first = interp.buffer.point();
        let boundary =
            call_word_boundary_function(interp, env, function, &function_name, first, end)?
                .filter(|boundary| after_first < *boundary && *boundary <= end)
                .unwrap_or(after_first);
        interp.buffer.goto_char(boundary);
    } else {
        while interp.buffer.point() < end && word_syntax_at(interp, env, interp.buffer.point()) {
            let _ = interp.buffer.forward_char(1);
        }
    }
    Ok(true)
}

fn move_backward_one_word(interp: &mut Interpreter, env: &mut Env) -> Result<bool, LispError> {
    let beginning = interp.buffer.point_min();
    while interp.buffer.point() > beginning {
        let previous = interp.buffer.point() - 1;
        if word_syntax_at(interp, env, previous) {
            break;
        }
        let _ = interp.buffer.forward_char(-1);
    }
    if interp.buffer.point() == beginning {
        return Ok(false);
    }

    let _ = interp.buffer.forward_char(-1);
    let last = interp.buffer.point();
    let character = interp
        .buffer
        .char_at(last)
        .expect("point at a word character has a character");
    if let Some((function, function_name)) = word_boundary_function(interp, env, character) {
        let boundary =
            call_word_boundary_function(interp, env, function, &function_name, last, beginning)?
                .filter(|boundary| beginning <= *boundary && *boundary < last)
                .unwrap_or(last);
        interp.buffer.goto_char(boundary);
    } else {
        while interp.buffer.point() > beginning {
            let previous = interp.buffer.point() - 1;
            if !word_syntax_at(interp, env, previous) {
                break;
            }
            let _ = interp.buffer.forward_char(-1);
        }
    }
    Ok(true)
}

fn forward_word(interp: &mut Interpreter, count: i64, env: &mut Env) -> Result<Value, LispError> {
    let forward = count >= 0;
    let mut remaining = count.unsigned_abs();
    while remaining > 0 {
        let moved = if forward {
            move_forward_one_word(interp, env)?
        } else {
            move_backward_one_word(interp, env)?
        };
        if !moved {
            return Ok(Value::Nil);
        }
        remaining -= 1;
    }
    Ok(Value::T)
}

fn stickiness_names_property(setting: &Value, prop: &str) -> bool {
    match setting {
        Value::T => true,
        Value::Cons(_) => setting.to_vec().is_ok_and(|items| {
            items
                .iter()
                .any(|item| matches!(item, Value::Symbol(name) if name == prop))
        }),
        _ => false,
    }
}

fn property_is_default_nonsticky(defaults: &Value, prop: &str) -> bool {
    defaults.to_vec().unwrap_or_default().iter().any(|entry| {
        let Some((name, nonsticky)) = (entry).cons_cells() else {
            return false;
        };
        matches!(&*name.borrow(), Value::Symbol(candidate) if candidate == prop)
            && nonsticky.borrow().is_truthy()
    })
}

fn span_properties_at(
    spans: &[crate::buffer::TextPropertySpan],
    position: usize,
) -> &[(String, Value)] {
    spans
        .iter()
        .find(|span| span.start <= position && position < span.end)
        .map(|span| span.props.as_slice())
        .unwrap_or(&[])
}

fn property_span_boundaries(spans: &[crate::buffer::TextPropertySpan]) -> Vec<usize> {
    let mut boundaries = spans
        .iter()
        .flat_map(|span| [span.start, span.end])
        .collect::<Vec<_>>();
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
}

fn next_property_span_boundary(
    spans: &[crate::buffer::TextPropertySpan],
    position: usize,
    limit: usize,
    next_interval_only: bool,
) -> Option<usize> {
    let initial = span_properties_at(spans, position);
    property_span_boundaries(spans)
        .into_iter()
        .filter(|boundary| position < *boundary && *boundary < limit)
        .find(|boundary| next_interval_only || span_properties_at(spans, *boundary) != initial)
}

fn previous_property_span_boundary(
    spans: &[crate::buffer::TextPropertySpan],
    position: usize,
    limit: usize,
) -> Option<usize> {
    let initial = position
        .checked_sub(1)
        .map(|position| span_properties_at(spans, position))
        .unwrap_or(&[]);
    property_span_boundaries(spans)
        .into_iter()
        .rev()
        .filter(|boundary| limit < *boundary && *boundary < position)
        .find(|boundary| {
            boundary
                .checked_sub(1)
                .map(|position| span_properties_at(spans, position))
                .unwrap_or(&[])
                != initial
        })
}

fn buffer_text_property_at_insertion(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
    default_nonsticky: &Value,
) -> Option<Value> {
    let previous = pos
        .checked_sub(1)
        .filter(|previous| *previous >= buffer.point_min())
        .and_then(|previous| buffer_property_at_with_category(interp, buffer, previous, prop))
        .unwrap_or(Value::Nil);
    let following =
        buffer_property_at_with_category(interp, buffer, pos, prop).unwrap_or(Value::Nil);
    let rear_nonsticky = pos
        .checked_sub(1)
        .filter(|previous| *previous >= buffer.point_min())
        .and_then(|previous| {
            buffer_property_at_with_category(interp, buffer, previous, "rear-nonsticky")
        })
        .unwrap_or(Value::Nil);
    let front_sticky =
        buffer_property_at_with_category(interp, buffer, pos, "front-sticky").unwrap_or(Value::Nil);

    let rear_sticky = pos > buffer.point_min()
        && !property_is_default_nonsticky(default_nonsticky, prop)
        && !stickiness_names_property(&rear_nonsticky, prop);
    let front_sticky = stickiness_names_property(&front_sticky, prop);
    let inherited = match (rear_sticky, front_sticky) {
        (true, false) => previous,
        (false, true) => following,
        (false, false) => Value::Nil,
        // Rear stickiness wins a conflict unless it would inherit nil.
        (true, true) if previous.is_nil() => following,
        (true, true) => previous,
    };
    (!inherited.is_nil()).then_some(inherited)
}

fn search_noerror_moves(noerror: Option<&Value>) -> bool {
    noerror.is_some_and(|value| value.is_truthy() && !matches!(value, Value::T))
}

fn character_byte_value(character: Option<char>, multibyte: bool) -> Result<Value, LispError> {
    let Some(character) = character else {
        // GNU exposes the terminating NUL at point-max and at the end of an
        // empty string when no explicit position was supplied.
        return Ok(Value::Integer(0));
    };
    let raw_byte = raw_byte_from_regex_char(character);
    let code = raw_byte.map_or(character as u32, u32::from);
    if multibyte && raw_byte.is_none() && code > 0x7f {
        return Err(LispError::Signal(format!(
            "Not an ASCII nor an 8-bit character: {code}"
        )));
    }
    Ok(Value::Integer(i64::from(code & 0xff)))
}

fn utf8_sequence_width(first: u8) -> usize {
    match first {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => 0,
    }
}

fn unibyte_text_bytes(text: &str) -> Vec<u8> {
    text.chars()
        .flat_map(|character| {
            raw_byte_from_regex_char(character)
                .map(|byte| vec![byte])
                .or_else(|| u8::try_from(character as u32).ok().map(|byte| vec![byte]))
                .unwrap_or_else(|| character.to_string().into_bytes())
        })
        .collect()
}

fn multibyte_buffer_text(text: &str, preserve_utf8_sequences: bool) -> (String, Vec<usize>) {
    let bytes = unibyte_text_bytes(text);
    let mut converted = String::new();
    let mut position_map = vec![1; bytes.len() + 1];
    let mut byte_index = 0;
    let mut new_position = 1;

    while byte_index < bytes.len() {
        position_map[byte_index] = new_position;
        let width = if preserve_utf8_sequences {
            utf8_sequence_width(bytes[byte_index])
        } else {
            0
        };
        let character = (width > 0 && byte_index + width <= bytes.len())
            .then(|| std::str::from_utf8(&bytes[byte_index..byte_index + width]).ok())
            .flatten()
            .and_then(|valid| valid.chars().next());
        let consumed = if let Some(character) = character {
            converted.push(character);
            width
        } else {
            converted.push(raw_byte_regex_char(bytes[byte_index]));
            1
        };
        new_position += 1;
        position_map[byte_index + 1..=byte_index + consumed].fill(new_position);
        byte_index += consumed;
    }
    (converted, position_map)
}

fn unibyte_buffer_text(text: &str) -> (String, Vec<usize>) {
    let mut converted = String::new();
    let mut position_map = Vec::with_capacity(text.chars().count() + 1);
    position_map.push(1);
    for character in text.chars() {
        let bytes = raw_byte_from_regex_char(character)
            .map(|byte| vec![byte])
            .unwrap_or_else(|| character.to_string().into_bytes());
        converted.extend(bytes.into_iter().map(char::from));
        position_map.push(converted.chars().count() + 1);
    }
    (converted, position_map)
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Buffer operations ──
            "insert" => insert_impl(interp, args, env, false, false),
            "insert-and-inherit" => insert_impl(interp, args, env, true, false),
            "insert-char" => insert_char_impl(interp, args, env),
            "self-insert-command" => {
                need_arg_range(name, args, 0, 2)?;
                let amalgamating = args
                    .first()
                    .filter(|value| !value.is_nil())
                    .map_or(1, |value| value.as_integer().unwrap_or(1))
                    .abs()
                    < 2;
                if amalgamating {
                    call_undo_auto_amalgamate(interp, env);
                }
                let event = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| interp.lookup_var("last-command-event", env))
                    .unwrap_or(Value::Nil);
                interp.set_variable("last-command-event", event.clone(), env);
                let ch = match event {
                    Value::Integer(code) => char::from_u32(code as u32),
                    Value::Symbol(symbol) if symbol.chars().count() == 1 => symbol.chars().next(),
                    Value::String(text) if text.chars().count() == 1 => text.chars().next(),
                    _ => None,
                }
                .ok_or_else(|| LispError::Signal("No self-insert character".into()))?;
                let count = args
                    .first()
                    .filter(|value| !value.is_nil())
                    .map(Value::as_integer)
                    .transpose()?
                    .unwrap_or(1);
                if count < 0 {
                    return Err(LispError::Signal(format!(
                        "Negative repetition argument {count}"
                    )));
                }
                let count = count as usize;

                // GNU expands an active word abbrev before inserting a
                // non-word character.  Keep the expansion policy in abbrev.el;
                // this primitive owns only the same syntax-trigger boundary as
                // cmds.c's `internal_self_insert'.
                let expands_abbrev = count > 0
                    && interp
                        .lookup_var("abbrev-mode", env)
                        .is_some_and(|value| value.is_truthy())
                    && syntax::syntax_entry_for_code(
                        interp,
                        interp.current_syntax_table_id(),
                        ch as u32,
                    )
                    .class
                        != syntax::SyntaxClass::Word
                    && interp.buffer.char_before().is_some_and(|previous| {
                        syntax::syntax_entry_for_code(
                            interp,
                            interp.current_syntax_table_id(),
                            previous as u32,
                        )
                        .class
                            == syntax::SyntaxClass::Word
                    });
                if expands_abbrev && let Ok(function) = interp.lookup_function("expand-abbrev", env)
                {
                    let expanded =
                        interp.call_function_value(function, Some("expand-abbrev"), &[], env)?;
                    if let Value::Symbol(abbrev) = expanded {
                        let hook =
                            super::call(interp, "symbol-function", &[Value::Symbol(abbrev)], env)?;
                        if matches!(hook, Value::Symbol(_))
                            && super::call(
                                interp,
                                "get",
                                &[hook, Value::Symbol("no-self-insert".into())],
                                env,
                            )?
                            .is_truthy()
                        {
                            return Ok(Value::Nil);
                        }
                    }
                }
                let text: String = std::iter::repeat_n(ch, count).collect();
                insert_text_with_hooks(interp, &text, &[], &[], true, false, env)?;

                let auto_fill_character = match interp.lookup_var("auto-fill-chars", env) {
                    Some(Value::CharTable(table_id)) => interp
                        .char_table_get(table_id, ch as u32)
                        .is_some_and(|value| value.is_truthy()),
                    _ => matches!(ch, ' ' | '\n'),
                };
                if auto_fill_character
                    && interp
                        .lookup_var("auto-fill-function", env)
                        .is_some_and(|value| value.is_truthy())
                {
                    // cmds.c calls the dumped Elisp orchestration function,
                    // not the mode-specific callback directly.  For a newly
                    // inserted newline it temporarily visits the preceding
                    // line so filling sees the completed line boundary.
                    if ch == '\n' {
                        let point = interp.buffer.point();
                        interp.buffer.goto_char(point.saturating_sub(1));
                    }
                    let function = interp.lookup_function("internal-auto-fill", env)?;
                    interp.call_function_value(function, Some("internal-auto-fill"), &[], env)?;
                    if ch == '\n' && interp.buffer.point() < interp.buffer.point_max() {
                        let point = interp.buffer.point();
                        interp.buffer.goto_char(point + 1);
                    }
                }
                run_named_hooks(
                    interp,
                    "post-self-insert-hook",
                    env,
                    Some(interp.current_buffer_id()),
                )?;
                Ok(Value::Nil)
            }
            "insert-byte" => {
                need_args(name, args, 2)?;
                let byte = args[0].as_integer()?;
                if !(0..=255).contains(&byte) {
                    return Err(LispError::Signal("Byte value out of range".into()));
                }
                let count = args[1].as_integer()?.max(0) as usize;
                let c = char::from_u32(byte as u32)
                    .ok_or_else(|| LispError::Signal(format!("Invalid byte: {}", byte)))?;
                let text: String = std::iter::repeat_n(c, count).collect();
                insert_text_with_hooks(interp, &text, &[], &[], false, false, env)?;
                Ok(Value::Nil)
            }
            "insert-buffer-substring" => {
                if args.is_empty() || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let buffer_id = interp.resolve_buffer_id(&args[0])?;
                let source = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                // GNU: nil START/END mean the accessible portion's bounds.
                let start = match args.get(1) {
                    Some(value) if !value.is_nil() => position_from_value(interp, value)?,
                    _ => source.point_min(),
                };
                let end = match args.get(2) {
                    Some(value) if !value.is_nil() => position_from_value(interp, value)?,
                    _ => source.point_max(),
                };
                let text = source
                    .buffer_substring(start, end)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                let props = source.substring_property_spans(start, end);
                let extended_chars = source.substring_extended_chars(start, end);
                insert_text_with_hooks(interp, &text, &props, &extended_chars, false, false, env)?;
                Ok(Value::Nil)
            }
            "point" => Ok(Value::Integer(interp.buffer.point() as i64)),
            "point-min" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
            "point-max" => Ok(Value::Integer(interp.buffer.point_max() as i64)),
            "minibuffer-prompt-end" => {
                let prompt_length = interp
                    .minibuffer_prompt_text()
                    .map(|prompt| prompt.chars().count())
                    .unwrap_or(0);
                Ok(Value::Integer(
                    interp.buffer.point_min().saturating_add(prompt_length) as i64,
                ))
            }
            "combine-after-change-execute" => {
                need_args(name, args, 0)?;
                flush_combined_after_change(interp, env)
            }
            "goto-char" => {
                need_args(name, args, 1)?;
                let pos = position_from_value(interp, &args[0])?;
                interp.buffer.goto_char(pos);
                // GNU Fgoto_char returns its POSITION argument unchanged (a
                // marker stays a marker), not the clamped integer point —
                // erc-display-msg does (marker-position (goto-char MARKER)).
                Ok(args[0].clone())
            }
            "forward-char" => {
                let n = if args.is_empty() || args[0].is_nil() {
                    1
                } else {
                    args[0].as_integer()?
                };
                match interp.buffer.forward_char(n as isize) {
                    Ok(_) => Ok(Value::Nil),
                    Err(e) => Err(e.into()),
                }
            }
            "forward-word" => {
                need_arg_range(name, args, 0, 1)?;
                let n = if args.is_empty() || args[0].is_nil() {
                    1
                } else {
                    args[0].as_integer()?
                };
                forward_word(interp, n, env)
            }
            "skip-chars-forward" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                regexp::skip_chars_forward_impl(interp, &args[0], args.get(1))
            }
            "skip-chars-backward" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                regexp::skip_chars_backward_impl(interp, &args[0], args.get(1))
            }
            "skip-syntax-forward" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                syntax::skip_syntax_impl(interp, &args[0], args.get(1), true, env)
            }
            "skip-syntax-backward" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                syntax::skip_syntax_impl(interp, &args[0], args.get(1), false, env)
            }
            "backward-char" => {
                let n = if args.is_empty() || args[0].is_nil() {
                    1
                } else {
                    args[0].as_integer()?
                };
                match interp.buffer.forward_char(-(n as isize)) {
                    Ok(_) => Ok(Value::Nil),
                    Err(e) => Err(e.into()),
                }
            }
            "beginning-of-line" => {
                // GNU constrains bol motion to the current field (fields are
                // rare; skip the work when the buffer has none).
                let old_pos = interp.buffer.point();
                let n = args
                    .first()
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(1);
                if n != 1 {
                    interp.buffer.forward_line((n - 1) as isize);
                }
                // After crossing an unterminated final line, GNU's
                // line-beginning-position leaves point at ZV.  Calling the
                // ordinary current-line BOL operation there would incorrectly
                // rewind to that same final line and can make region walkers
                // loop forever.
                let crossed_to_unterminated_eob = n > 1
                    && interp.buffer.point() == interp.buffer.point_max()
                    && interp.buffer.char_before().is_some_and(|ch| ch != '\n');
                if !crossed_to_unterminated_eob {
                    interp.buffer.beginning_of_line();
                }
                if buffer_has_field_property(interp) {
                    let new_pos = interp.buffer.point();
                    let constrained = super::call(
                        interp,
                        "constrain-to-field",
                        &[
                            Value::Integer(new_pos as i64),
                            Value::Integer(old_pos as i64),
                        ],
                        env,
                    )?
                    .as_integer()? as usize;
                    interp.buffer.goto_char(constrained);
                }
                Ok(Value::Nil)
            }
            "end-of-line" => {
                // (end-of-line N): end of the Nth line counting from the
                // current one (0 = previous line's end).
                let n = args
                    .first()
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(1);
                if n != 1 {
                    interp.buffer.forward_line((n - 1) as isize);
                }
                interp.buffer.end_of_line();
                Ok(Value::Nil)
            }
            "forward-line" => {
                let n = if args.is_empty() || args[0].is_nil() {
                    BigInt::from(1u8)
                } else {
                    integer_like_bigint(interp, &args[0])?
                };
                Ok(normalize_bigint_value(forward_line_bigint(
                    &mut interp.buffer,
                    n,
                )))
            }
            "compute-motion" => {
                need_args(name, args, 7)?;
                compute_motion_value(interp, env, args)
            }
            "vertical-motion" => {
                need_arg_range(name, args, 1, 3)?;
                let (goal_col, n) = match &args[0] {
                    cons @ Value::Cons(_) => {
                        let (car, cdr) = cons.cons_values().ok_or_else(|| {
                            LispError::WrongTypeArgument("consp".into(), args[0].clone())
                        })?;
                        // COLS may be a float (line-move-visual divides
                        // pixels by the frame char width); GNU truncates
                        // it to a pixel count.
                        let goal = match &car {
                            Value::Float(float) => float.max(0.0) as usize,
                            other => other.as_integer()?.max(0) as usize,
                        };
                        (Some(goal), cdr.as_integer()?)
                    }
                    other => {
                        let big = integer_like_bigint(interp, other)?;
                        (None, big.to_i64().unwrap_or(i64::MAX / 2))
                    }
                };
                let moved = visual_vertical_motion(interp, env, n, goal_col)?;
                Ok(Value::Integer(moved))
            }
            "search-forward" | "search-backward" => {
                if args.is_empty() || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let needle = string_text(&args[0])?;
                // GNU folds case whenever `case-fold-search' is non-nil; fold
                // per character so char counts stay aligned with the buffer.
                let case_fold = interp
                    .lookup_var("case-fold-search", env)
                    .is_some_and(|value| value.is_truthy());
                let fold = |text: &str| -> String {
                    text.chars()
                        .map(|ch| ch.to_lowercase().next().unwrap_or(ch))
                        .collect()
                };
                let needle_key = if case_fold {
                    fold(&needle)
                } else {
                    needle.clone()
                };
                let noerror = args.get(2).is_some_and(Value::is_truthy);
                let move_on_failure = search_noerror_moves(args.get(2));
                let original_point = interp.buffer.point();
                // GNU repeats the search COUNT times; a negative COUNT searches
                // in the opposite direction (viper's `F' calls search-forward
                // with -1).
                let count = match args.get(3) {
                    Some(value) if !value.is_nil() => value.as_integer()?,
                    _ => 1,
                };
                let forward = (name == "search-forward") == (count >= 0);
                let limit = match args.get(1) {
                    // GNU clamps a BOUND outside the accessible region
                    // (loaddefs-gen searches backward with (- (point-max) 1000)).
                    Some(Value::Integer(pos)) if *pos < interp.buffer.point_min() as i64 => {
                        interp.buffer.point_min()
                    }
                    Some(value) if !value.is_nil() => position_from_value(interp, value)?,
                    _ if forward => interp.buffer.point_max(),
                    _ => interp.buffer.point_min(),
                };
                let mut result = None;
                for _ in 0..count.unsigned_abs().max(1) {
                    let point = interp.buffer.point();
                    result = if forward {
                        let limit = limit.min(interp.buffer.point_max());
                        if limit < point {
                            None
                        } else {
                            let haystack = interp
                                .buffer
                                .buffer_substring(point, limit)
                                .map_err(|error| LispError::Signal(error.to_string()))?;
                            let haystack = if case_fold { fold(&haystack) } else { haystack };
                            haystack.find(&needle_key).map(|found| {
                                let match_start_chars = haystack[..found].chars().count();
                                (
                                    point + match_start_chars,
                                    point + match_start_chars + needle.chars().count(),
                                )
                            })
                        }
                    } else {
                        let limit = limit.max(interp.buffer.point_min());
                        if limit > point {
                            None
                        } else {
                            let haystack = interp
                                .buffer
                                .buffer_substring(limit, point)
                                .map_err(|error| LispError::Signal(error.to_string()))?;
                            let haystack = if case_fold { fold(&haystack) } else { haystack };
                            haystack.rfind(&needle_key).map(|found| {
                                let start = limit + haystack[..found].chars().count();
                                let end = start + needle.chars().count();
                                (start, end)
                            })
                        }
                    };
                    match result {
                        Some((start, end)) => {
                            interp.buffer.goto_char(if forward { end } else { start });
                        }
                        None => break,
                    }
                }
                match result {
                    Some((start, end)) => {
                        interp.last_match_data = Some(vec![Some((start, end))]);
                        interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                        let point = if forward { end } else { start };
                        interp.buffer.goto_char(point);
                        Ok(Value::Integer(point as i64))
                    }
                    None if noerror => {
                        interp.buffer.goto_char(if move_on_failure {
                            limit
                        } else {
                            original_point
                        });
                        Ok(Value::Nil)
                    }
                    None => {
                        interp.buffer.goto_char(original_point);
                        Err(LispError::SignalValue(Value::list([
                            Value::Symbol("search-failed".into()),
                            Value::String(needle.into()),
                        ])))
                    }
                }
            }
            "re-search-forward" => regexp::buffer_regex_search(interp, args, env, true, false),
            "re-search-backward" => regexp::buffer_regex_search(interp, args, env, false, false),
            "posix-search-forward" => regexp::buffer_regex_search(interp, args, env, true, true),
            "posix-search-backward" => regexp::buffer_regex_search(interp, args, env, false, true),
            "forward-comment" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                syntax::forward_comment_impl(interp, args.first(), env)
            }
            "scan-lists" => syntax::scan_lists_impl(interp, args, env),
            "scan-sexps" => {
                need_args(name, args, 2)?;
                let from = position_from_value(interp, &args[0])?;
                let count = args[1].as_integer()?;
                Ok(
                    syntax::scan_sexps_position_for_scan_sexps(interp, env, from, count)?
                        .map(|position| Value::Integer(position as i64))
                        .unwrap_or(Value::Nil),
                )
            }
            "backward-prefix-chars" => {
                need_args(name, args, 0)?;
                syntax::backward_prefix_chars(interp)
            }
            "parse-partial-sexp" => {
                if args.len() < 2 || args.len() > 6 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let from = position_from_value(interp, &args[0])?;
                let to = position_from_value(interp, &args[1])?;
                let target_depth = match args.get(2) {
                    Some(Value::Nil) | None => None,
                    Some(value) => Some(value.as_integer()?),
                };
                let stopbefore = args.get(3).is_some_and(Value::is_truthy);
                let oldstate = args.get(4).filter(|value| !value.is_nil());
                let commentstop = syntax::CommentStop::from_value(args.get(5));
                syntax::parse_forward(
                    interp,
                    from,
                    to,
                    target_depth,
                    stopbefore,
                    oldstate,
                    commentstop,
                    env,
                )
            }
            "buffer-string" => Ok(string_like_value_with_extended_chars(
                interp.buffer.buffer_string(),
                interp
                    .buffer
                    .substring_property_spans(interp.buffer.point_min(), interp.buffer.point_max()),
                interp.buffer.is_multibyte(),
                interp
                    .buffer
                    .substring_extended_chars(interp.buffer.point_min(), interp.buffer.point_max()),
            )),
            "minibuffer-contents" | "minibuffer-contents-no-properties" => {
                need_arg_range(name, args, 0, 0)?;
                let active_minibuffer =
                    interp.active_minibuffer_buffer_id() == Some(interp.current_buffer_id());
                let prompt_length = if active_minibuffer {
                    interp
                        .minibuffer_prompt_text()
                        .map(|prompt| prompt.chars().count())
                        .unwrap_or(0)
                } else {
                    0
                };
                let start = interp
                    .buffer
                    .point_min()
                    .saturating_add(prompt_length)
                    .min(interp.buffer.point_max());
                let end = interp.buffer.point_max();
                let text = interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let props = if name == "minibuffer-contents" {
                    interp.buffer.substring_property_spans(start, end)
                } else {
                    Vec::new()
                };
                Ok(string_like_value_with_extended_chars(
                    text,
                    props,
                    interp.buffer.is_multibyte(),
                    interp.buffer.substring_extended_chars(start, end),
                ))
            }
            "buffer-substring" | "buffer-substring-no-properties" => {
                need_args(name, args, 2)?;
                let from = position_from_value(interp, &args[0])?;
                let to = position_from_value(interp, &args[1])?;
                let (start, end) = if from <= to { (from, to) } else { (to, from) };
                match interp.buffer.buffer_substring(start, end) {
                    Ok(s) => {
                        if name == "buffer-substring" {
                            Ok(string_like_value_with_extended_chars(
                                s,
                                interp.buffer.substring_property_spans(start, end),
                                interp.buffer.is_multibyte(),
                                interp.buffer.substring_extended_chars(start, end),
                            ))
                        } else {
                            Ok(string_like_value_with_extended_chars(
                                s,
                                Vec::new(),
                                interp.buffer.is_multibyte(),
                                interp.buffer.substring_extended_chars(start, end),
                            ))
                        }
                    }
                    Err(e) => Err(LispError::Signal(e.to_string())),
                }
            }
            "invisible-p" => {
                need_args(name, args, 1)?;
                let invisible = match position_from_value(interp, &args[0]) {
                    Ok(position) if position >= 1 => char_is_invisible(interp, position, env),
                    _ => invisibility_value_is_hidden(interp, &args[0], env),
                };
                Ok(if invisible { Value::T } else { Value::Nil })
            }
            "buffer-size" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = match args.first() {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?;
                Ok(Value::Integer(buffer.size_total() as i64))
            }
            "buffer-enable-undo" => {
                interp.buffer.enable_undo();
                Ok(Value::Nil)
            }
            "gap-position" => Ok(Value::Integer(interp.buffer.point() as i64)),
            "gap-size" => Ok(Value::Integer(0)),
            "buffer-line-statistics" => {
                need_arg_range(name, args, 0, 1)?;
                buffer_line_statistics_value(interp, args.first())
            }
            "max-char" => {
                need_arg_range(name, args, 0, 1)?;
                // GNU keeps a wider internal character space for raw-byte and
                // legacy character representations, but a non-nil UNICODE
                // argument asks for the Unicode scalar ceiling specifically.
                // Unicode-wide Lisp scans rely on this distinction to stop at
                // U+10FFFF rather than traversing the whole internal space.
                Ok(Value::Integer(
                    if args.first().is_some_and(Value::is_truthy) {
                        0x10_FFFF
                    } else {
                        0x3F_FFFF
                    },
                ))
            }
            "position-bytes" => {
                let pos = if args.is_empty() {
                    interp.buffer.point()
                } else {
                    position_from_value(interp, &args[0])?
                };
                Ok(position_bytes(interp, pos)
                    .map(|byte_pos| Value::Integer(byte_pos as i64))
                    .unwrap_or(Value::Nil))
            }
            "byte-to-position" => {
                need_args(name, args, 1)?;
                let byte = args[0].as_integer()?;
                if byte <= 0 {
                    return Ok(Value::Nil);
                }
                Ok(byte_to_position(interp, byte as usize)
                    .map(|pos| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil))
            }

            "buffer-name" => {
                if !args.is_empty()
                    && let Value::Buffer(buffer) = &args[0]
                {
                    return Ok(interp
                        .get_buffer_by_id(buffer.id)
                        .map(|buffer| Value::String(buffer.name.clone().into()))
                        .unwrap_or(Value::Nil));
                }
                Ok(Value::String(interp.buffer.name.clone().into()))
            }
            "set-buffer-multibyte" => {
                need_args(name, args, 1)?;
                let enabled = args[0].is_truthy();
                if enabled == interp.buffer.is_multibyte() {
                    return Ok(args[0].clone());
                }
                if interp.buffer.restriction()
                    != (1, interp.buffer.full_buffer_string().chars().count() + 1)
                {
                    return Err(LispError::Signal(
                        "Changing multibyteness in a narrowed buffer".into(),
                    ));
                }

                let original = interp.buffer.full_buffer_string();
                let saved = interp.buffer.saved_text().to_string();
                let preserve_utf8_sequences = matches!(args[0], Value::T);
                let (converted, positions) = if enabled {
                    multibyte_buffer_text(&original, preserve_utf8_sequences)
                } else {
                    unibyte_buffer_text(&original)
                };
                let converted_saved = if enabled {
                    multibyte_buffer_text(&saved, preserve_utf8_sequences).0
                } else {
                    unibyte_buffer_text(&saved).0
                };
                let buffer_id = interp.current_buffer_id();
                let markers = interp.live_marker_positions_for_buffer(buffer_id);
                interp.buffer.set_multibyte_representation(
                    enabled,
                    converted,
                    converted_saved,
                    &positions,
                );
                for (marker_id, position) in markers {
                    let position = position
                        .and_then(|position| positions.get(position.saturating_sub(1)).copied());
                    interp.set_marker(marker_id, position, Some(buffer_id))?;
                }
                if interp.buffer.undo_enabled() {
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Opaque(Value::list([
                            Value::Symbol("apply".into()),
                            Value::Symbol("set-buffer-multibyte".into()),
                            if enabled { Value::Nil } else { Value::T },
                        ])));
                }
                Ok(args[0].clone())
            }
            "char-after" => {
                let pos = match args.first() {
                    None | Some(Value::Nil) => Some(interp.buffer.point()),
                    Some(Value::Integer(position)) if *position >= 0 => {
                        usize::try_from(*position).ok()
                    }
                    Some(Value::Integer(_)) => None,
                    Some(Value::Marker(id)) => interp.marker_position(*id),
                    Some(value) => {
                        return Err(LispError::WrongTypeArgument(
                            "integer-or-marker-p".into(),
                            value.clone(),
                        ));
                    }
                };
                match pos.and_then(|position| public_buffer_char_code_at(interp, position)) {
                    Some(code) => Ok(Value::Integer(code)),
                    None => Ok(Value::Nil),
                }
            }
            "char-before" => {
                let pos = match args.first() {
                    None | Some(Value::Nil) => Some(interp.buffer.point()),
                    Some(Value::Integer(position)) if *position >= 0 => {
                        usize::try_from(*position).ok()
                    }
                    Some(Value::Integer(_)) => None,
                    Some(Value::Marker(id)) => interp.marker_position(*id),
                    Some(value) => {
                        return Err(LispError::WrongTypeArgument(
                            "integer-or-marker-p".into(),
                            value.clone(),
                        ));
                    }
                };
                let Some(pos) = pos else {
                    return Ok(Value::Nil);
                };
                if pos <= interp.buffer.point_min() {
                    Ok(Value::Nil)
                } else {
                    match public_buffer_char_code_at(interp, pos - 1) {
                        Some(code) => Ok(Value::Integer(code)),
                        None => Ok(Value::Nil),
                    }
                }
            }
            "matching-paren" => {
                need_args(name, args, 1)?;
                let ch = args[0].as_integer()? as u32;
                let matching = match char::from_u32(ch) {
                    Some('(') => Some(')'),
                    Some(')') => Some('('),
                    Some('[') => Some(']'),
                    Some(']') => Some('['),
                    Some('{') => Some('}'),
                    Some('}') => Some('{'),
                    Some('<') => Some('>'),
                    Some('>') => Some('<'),
                    _ => None,
                };
                Ok(matching
                    .map(|ch| Value::Integer(ch as i64))
                    .unwrap_or(Value::Nil))
            }
            "get-byte" => {
                if let Some(string_value) = args.get(1).filter(|value| !value.is_nil()) {
                    let string = string_like(string_value).ok_or_else(|| {
                        LispError::WrongTypeArgument("stringp".into(), string_value.clone())
                    })?;
                    let position = match args.first().filter(|value| !value.is_nil()) {
                        Some(Value::Integer(position)) if *position >= 0 => *position as usize,
                        Some(value) => {
                            return Err(wrong_type_argument("wholenump", value.clone()));
                        }
                        None => 0,
                    };
                    let mut characters = string.text.chars();
                    let character = characters.nth(position);
                    if character.is_none() && (position != 0 || !string.text.is_empty()) {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("args-out-of-range".into()),
                            string_value.clone(),
                            Value::Integer(position as i64),
                        ])));
                    }
                    character_byte_value(character, string.multibyte)
                } else {
                    let position = match args.first().filter(|value| !value.is_nil()) {
                        Some(value) => {
                            let position = position_from_value(interp, value)?;
                            if position < interp.buffer.point_min()
                                || position >= interp.buffer.point_max()
                            {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("args-out-of-range".into()),
                                    value.clone(),
                                    Value::Integer(interp.buffer.point_min() as i64),
                                    Value::Integer(interp.buffer.point_max() as i64),
                                ])));
                            }
                            position
                        }
                        None => interp.buffer.point(),
                    };
                    character_byte_value(
                        interp.buffer.char_at(position),
                        interp.buffer.is_multibyte(),
                    )
                }
            }
            "bobp" => Ok(if interp.buffer.bobp() {
                Value::T
            } else {
                Value::Nil
            }),
            "eobp" => Ok(if interp.buffer.eobp() {
                Value::T
            } else {
                Value::Nil
            }),
            "bolp" => Ok(if interp.buffer.bolp() {
                Value::T
            } else {
                Value::Nil
            }),
            "eolp" => Ok(if interp.buffer.eolp() {
                Value::T
            } else {
                Value::Nil
            }),
            "delete-region" => {
                need_args(name, args, 2)?;
                let from = position_from_value(interp, &args[0])?;
                let to = position_from_value(interp, &args[1])?;
                ensure_region_modifiable(interp, from, to, env)?;
                delete_region_with_hooks(interp, from, to, env)?;
                Ok(Value::Nil)
            }
            "delete-and-extract-region" => {
                need_args(name, args, 2)?;
                let from = position_from_value(interp, &args[0])?;
                let to = position_from_value(interp, &args[1])?;
                ensure_region_modifiable(interp, from, to, env)?;
                let (start, end) = if from <= to { (from, to) } else { (to, from) };
                let props = interp.buffer.substring_property_spans(start, end);
                let multibyte = interp.buffer.is_multibyte();
                Ok(string_like_value_with_multibyte(
                    delete_region_with_hooks(interp, from, to, env)?,
                    props,
                    multibyte,
                ))
            }
            "delete-char" => {
                let n = if args.is_empty() {
                    1
                } else {
                    args[0].as_integer()?
                };
                if n.abs() < 2 {
                    call_undo_auto_amalgamate(interp, env);
                }
                let point = interp.buffer.point();
                if n >= 0 {
                    let to = point + n as usize;
                    if to > interp.buffer.point_max() {
                        Err(crate::buffer::BufferError::EndOfBuffer.into())
                    } else {
                        delete_region_with_hooks(interp, point, to, env)?;
                        Ok(Value::Nil)
                    }
                } else {
                    let count = (-n) as usize;
                    if point < interp.buffer.point_min() + count {
                        Err(crate::buffer::BufferError::BeginningOfBuffer.into())
                    } else {
                        delete_region_with_hooks(interp, point - count, point, env)?;
                        Ok(Value::Nil)
                    }
                }
            }
            "erase-buffer" => {
                let size = interp.buffer.buffer_size();
                if size > 0 {
                    let min = interp.buffer.point_min();
                    let max = interp.buffer.point_max();
                    delete_region_with_hooks(interp, min, max, env)?;
                }
                Ok(Value::Nil)
            }
            "upcase-region"
            | "downcase-region"
            | "capitalize-region"
            | "upcase-initials-region" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let action = match name {
                    "upcase-region" => CaseAction::Up,
                    "downcase-region" => CaseAction::Down,
                    "capitalize-region" => CaseAction::Capitalize,
                    _ => CaseAction::UpcaseInitials,
                };
                if args.get(2).is_some_and(Value::is_truthy) {
                    let extractor = interp
                        .lookup_var("region-extract-function", env)
                        .ok_or_else(|| LispError::Void("region-extract-function".into()))?;
                    let bounds = call_function_value(
                        interp,
                        &extractor,
                        &[Value::Symbol("bounds".into())],
                        env,
                    )?;
                    for (start, end) in parse_region_bounds(&bounds)? {
                        casify_buffer_region(interp, start, end, action, env)?;
                    }
                    Ok(Value::Nil)
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    casify_buffer_region(interp, start, end, action, env)?;
                    Ok(Value::Nil)
                }
            }
            "upcase-word" | "downcase-word" | "capitalize-word" => {
                need_args(name, args, 1)?;
                let action = match name {
                    "upcase-word" => CaseAction::Up,
                    "downcase-word" => CaseAction::Down,
                    _ => CaseAction::Capitalize,
                };
                let count = args[0].as_integer()?;
                let point = interp.buffer.point();
                let (start, end) = case_word_region(interp, point, count, env);
                let new_end = casify_buffer_region(interp, start, end, action, env)?;
                if count >= 0 {
                    interp.buffer.goto_char(new_end);
                } else {
                    interp.buffer.goto_char(point);
                }
                Ok(Value::Nil)
            }
            "current-column" => {
                let pt = interp.buffer.point();
                let bol = {
                    let saved = interp.buffer.point();
                    interp.buffer.beginning_of_line();
                    let bol = interp.buffer.point();
                    interp.buffer.goto_char(saved);
                    bol
                };
                Ok(Value::Integer(column_at(interp, env, bol, pt) as i64))
            }
            "current-indentation" => {
                let saved = interp.buffer.point();
                interp.buffer.beginning_of_line();
                while matches!(
                    interp.buffer.char_at(interp.buffer.point()),
                    Some(' ' | '\t')
                ) {
                    let _ = interp.buffer.forward_char(1);
                }
                let pt = interp.buffer.point();
                let bol = {
                    let saved = interp.buffer.point();
                    interp.buffer.beginning_of_line();
                    let bol = interp.buffer.point();
                    interp.buffer.goto_char(saved);
                    bol
                };
                let indentation = column_at(interp, env, bol, pt) as i64;
                interp.buffer.goto_char(saved);
                Ok(Value::Integer(indentation))
            }
            "indent-to" => {
                need_arg_range(name, args, 1, 2)?;
                let target = args[0].as_integer()?;
                let minimum = match args.get(1) {
                    Some(value) if !value.is_nil() => value.as_integer()?,
                    _ => 0,
                };
                let saved = interp.buffer.point();
                interp.buffer.beginning_of_line();
                let bol = interp.buffer.point();
                interp.buffer.goto_char(saved);
                let from_col = column_at(interp, env, bol, saved) as i64;
                let min_col = (from_col + minimum).max(target).max(from_col);
                if min_col == from_col {
                    return Ok(Value::Integer(min_col));
                }

                let tab_width = interp
                    .lookup_var("tab-width", env)
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(8)
                    .max(1);
                let use_tabs = interp
                    .lookup_var("indent-tabs-mode", env)
                    .is_some_and(|value| value.is_truthy());

                let mut current_col = from_col;
                let mut text = String::new();
                if use_tabs {
                    let tab_count = min_col / tab_width - from_col / tab_width;
                    if tab_count > 0 {
                        text.push_str(&"\t".repeat(tab_count as usize));
                        current_col = (min_col / tab_width) * tab_width;
                    }
                }
                let space_count = (min_col - current_col).max(0) as usize;
                if space_count > 0 {
                    text.push_str(&" ".repeat(space_count));
                }
                insert_text_with_hooks(interp, &text, &[], &[], true, false, env)?;
                Ok(Value::Integer(min_col))
            }
            "move-to-column" => {
                need_args(name, args, 1)?;
                let target = args[0].as_integer()?.max(0) as usize;
                let force = args.get(1).is_some_and(Value::is_truthy);
                let saved = interp.buffer.point();
                interp.buffer.beginning_of_line();
                let start = interp.buffer.point();
                interp.buffer.goto_char(saved);
                let mut pos = start;
                let mut current_col = 0;
                while pos < interp.buffer.point_max() {
                    if current_col >= target {
                        break;
                    }
                    let Some(ch) = interp.buffer.char_at(pos) else {
                        break;
                    };
                    if ch == '\n' {
                        break;
                    }
                    let next_col = column_after(interp, env, current_col, pos, ch);
                    if next_col > target && force && ch == '\t' {
                        interp.buffer.goto_char(pos);
                        interp.insert_current_buffer(&" ".repeat(target - current_col));
                        pos = interp.buffer.point();
                        current_col = target;
                        break;
                    }
                    current_col = next_col;
                    pos += 1;
                }
                if force && current_col < target {
                    interp.buffer.goto_char(pos);
                    interp.insert_current_buffer(&" ".repeat(target - current_col));
                    pos = interp.buffer.point();
                    current_col = target;
                }
                interp.buffer.goto_char(pos);
                Ok(Value::Integer(current_col as i64))
            }
            "line-number-at-pos" => {
                need_arg_range(name, args, 0, 2)?;
                let pos = if args.is_empty() || args[0].is_nil() {
                    interp.buffer.point()
                } else {
                    match &args[0] {
                        Value::Integer(pos) => {
                            if *pos < 0 {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("args-out-of-range".into()),
                                    Value::Integer(*pos),
                                    Value::Integer(1),
                                    Value::Integer((interp.buffer.size_total() + 1) as i64),
                                ])));
                            }
                            *pos as usize
                        }
                        Value::Marker(id) => interp.marker_position(*id).ok_or_else(|| {
                            LispError::WrongTypeArgument(
                                "integer-or-marker-p".into(),
                                args[0].clone(),
                            )
                        })?,
                        _ => {
                            return Err(LispError::WrongTypeArgument(
                                "integer-or-marker-p".into(),
                                args[0].clone(),
                            ));
                        }
                    }
                };
                let absolute_max = interp.buffer.size_total() + 1;
                if pos < 1 || pos > absolute_max {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("args-out-of-range".into()),
                        Value::Integer(pos as i64),
                        Value::Integer(1),
                        Value::Integer(absolute_max as i64),
                    ])));
                }
                let absolute = args.get(1).is_some_and(Value::is_truthy);
                let start = if absolute {
                    1
                } else {
                    interp.buffer.point_min()
                };
                let pos = if absolute {
                    pos
                } else {
                    pos.clamp(interp.buffer.point_min(), interp.buffer.point_max())
                };
                let line = interp
                    .buffer
                    .line_number_at_pos(pos)
                    .saturating_sub(interp.buffer.line_number_at_pos(start))
                    + 1;
                Ok(Value::Integer(line as i64))
            }
            "line-number-display-width" => {
                need_arg_range(name, args, 0, 1)?;
                line_number_display_width_value(interp, env, args.first())
            }
            "line-beginning-position" | "pos-bol" => {
                // GNU treats an explicit nil N as 1 (lisp-mnt passes
                // (if after 2) straight through).
                let n = match args.first() {
                    None | Some(Value::Nil) => 1,
                    Some(value) => value.as_integer()?,
                };
                let saved = interp.buffer.point();
                let count = (n - 1) as isize;
                let shortage = if count != 0 {
                    interp.buffer.forward_line(count)
                } else {
                    0
                };
                // Crossing an unterminated final line lands at ZV, which is
                // the requested next-line position even though moving to the
                // beginning of its containing line would jump backward.
                let at_unterminated_eob = count > 0
                    && interp.buffer.point() == interp.buffer.point_max()
                    && interp.buffer.char_before() != Some('\n');
                // If forward_line otherwise overshot, point is already at
                // point-max/point-min; preserve that shortage position.
                if !at_unterminated_eob
                    && (shortage == 0
                        || (count > 0 && interp.buffer.point() < interp.buffer.point_max()))
                {
                    interp.buffer.beginning_of_line();
                }
                let mut result = interp.buffer.point();
                interp.buffer.goto_char(saved);
                // GNU's `pos-bol' ignores fields; only `line-beginning-position'
                // constrains (with ESCAPE-FROM-EDGE only after actual line
                // motion and ONLY-IN-LINE set; see Fline_beginning_position).
                if name == "line-beginning-position" && buffer_has_field_property(interp) {
                    result = super::call(
                        interp,
                        "constrain-to-field",
                        &[
                            Value::Integer(result as i64),
                            Value::Integer(saved as i64),
                            if count != 0 { Value::T } else { Value::Nil },
                            Value::T,
                        ],
                        env,
                    )?
                    .as_integer()? as usize;
                }
                Ok(Value::Integer(result as i64))
            }
            "line-end-position" | "pos-eol" => {
                // GNU treats an explicit nil N as 1.
                let n = match args.first() {
                    None | Some(Value::Nil) => 1,
                    Some(value) => value.as_integer()?,
                };
                let saved = interp.buffer.point();
                let count = (n - 1) as isize;
                let shortage = if count != 0 {
                    interp.buffer.forward_line(count)
                } else {
                    0
                };
                // search.c's find_before_next_newline: a backward scan that
                // runs out of newlines yields BEGV itself, not the end of
                // the first accessible line.
                let mut result = if count < 0 && shortage != 0 {
                    interp.buffer.point_min()
                } else {
                    interp.buffer.end_of_line();
                    interp.buffer.point()
                };
                interp.buffer.goto_char(saved);
                // GNU's `pos-eol' ignores fields; `line-end-position'
                // constrains with ONLY-IN-LINE set (Fline_end_position).
                if name == "line-end-position" && buffer_has_field_property(interp) {
                    result = super::call(
                        interp,
                        "constrain-to-field",
                        &[
                            Value::Integer(result as i64),
                            Value::Integer(saved as i64),
                            Value::Nil,
                            Value::T,
                        ],
                        env,
                    )?
                    .as_integer()? as usize;
                }
                Ok(Value::Integer(result as i64))
            }
            "narrow-to-region" => {
                need_args(name, args, 2)?;
                let mut start = position_from_value(interp, &args[0])?;
                let mut end = position_from_value(interp, &args[1])?;
                if let Some((clamp_start, clamp_end)) =
                    interp.effective_labeled_restriction(interp.current_buffer_id(), None)
                {
                    start = start.max(clamp_start);
                    end = end.min(clamp_end);
                }
                interp.buffer.narrow_to_region(start, end);
                Ok(Value::Nil)
            }
            "widen" => {
                if let Some((start, end)) =
                    interp.effective_labeled_restriction(interp.current_buffer_id(), None)
                {
                    interp.buffer.narrow_to_region(start, end);
                } else {
                    interp.buffer.widen();
                }
                Ok(Value::Nil)
            }
            "buffer-modified-p" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = match args.first() {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?;
                Ok(if buffer.is_autosaved() {
                    Value::Symbol("autosaved".into())
                } else if buffer.is_modified() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "buffer-chars-modified-tick" | "buffer-modified-tick" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = match args.first() {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?;
                let tick = if name == "buffer-chars-modified-tick" {
                    buffer.chars_modified_tick()
                } else {
                    buffer.modified_tick()
                };
                Ok(Value::Integer(tick))
            }
            "internal--set-buffer-modified-tick" => {
                need_arg_range(name, args, 1, 2)?;
                let Value::Integer(tick) = &args[0] else {
                    return Err(LispError::WrongTypeArgument(
                        "fixnump".into(),
                        args[0].clone(),
                    ));
                };
                let buffer_id = match args.get(1) {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                interp
                    .get_buffer_by_id_mut(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?
                    .set_modified_tick(*tick);
                Ok(Value::Nil)
            }
            "set-buffer-modified-p" => {
                need_args(name, args, 1)?;
                let modified = !args[0].is_nil();
                let was_modified = interp.buffer.is_modified();
                let update_lock = !interp
                    .lookup_var("inhibit-modification-hooks", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp.buffer.file.is_some()
                    && interp.buffer.file_truename.is_some();
                if was_modified && !modified && update_lock {
                    unlock_current_buffer(interp, env)?;
                } else if !was_modified && modified && update_lock {
                    maybe_lock_current_buffer_file(interp, env)?;
                }
                if modified {
                    interp.buffer.set_modified();
                } else {
                    interp.buffer.set_unmodified();
                }
                Ok(Value::Nil)
            }
            "restore-buffer-modified-p" => {
                need_args(name, args, 1)?;
                let flag = args[0].clone();
                let modified = !flag.is_nil();
                let was_modified = interp.buffer.is_modified();
                let update_lock = !interp
                    .lookup_var("inhibit-modification-hooks", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp.buffer.file.is_some()
                    && interp.buffer.file_truename.is_some();
                if was_modified && !modified && update_lock {
                    unlock_current_buffer(interp, env)?;
                } else if !was_modified && modified && update_lock {
                    maybe_lock_current_buffer_file(interp, env)?;
                }
                if flag.is_nil() {
                    interp.buffer.set_unmodified();
                } else if matches!(&flag, Value::Symbol(symbol) if symbol == "autosaved") {
                    interp.buffer.set_modified();
                    interp.buffer.set_autosaved();
                } else {
                    interp.buffer.set_modified();
                }
                Ok(flag)
            }
            "get-pos-property" | "get-char-property" => {
                need_args(name, args, 2)?;
                let prop = args[1].as_symbol()?.to_string();
                if let Some(object) = args.get(2)
                    && string_like(object).is_some()
                {
                    let pos = args[0].as_integer()?.max(0) as usize;
                    return Ok(string_property_at_with_category(interp, object, pos, &prop)
                        .unwrap_or(Value::Nil));
                }
                let pos = position_from_value(interp, &args[0])?;
                let window_id = args
                    .get(2)
                    .and_then(|object| window_record_id_from_value(interp, object));
                let buffer_id = match args.get(2) {
                    Some(object) if window_id.is_some() => window_buffer_id(interp, object)
                        .ok_or_else(|| wrong_type_argument("window-live-p", object.clone()))?,
                    Some(object) if !object.is_nil() => interp.resolve_buffer_id(object)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                let default_nonsticky = if name == "get-pos-property" {
                    interp
                        .lookup_var("text-property-default-nonsticky", env)
                        .unwrap_or(Value::Nil)
                } else {
                    Value::Nil
                };
                Ok(highest_priority_overlay_property(
                    interp,
                    buffer,
                    pos,
                    &prop,
                    name == "get-pos-property",
                    window_id,
                )
                .or_else(|| {
                    if name == "get-pos-property" {
                        buffer_text_property_at_insertion(
                            interp,
                            buffer,
                            pos,
                            &prop,
                            &default_nonsticky,
                        )
                    } else {
                        buffer_property_at_with_category(interp, buffer, pos, &prop)
                    }
                })
                .unwrap_or(Value::Nil))
            }
            "get-char-property-and-overlay" => {
                need_arg_range(name, args, 2, 3)?;
                let prop = args[1].as_symbol()?.to_string();
                if let Some(object) = args.get(2)
                    && string_like(object).is_some()
                {
                    let pos = args[0].as_integer()?.max(0) as usize;
                    let value = string_property_at_with_category(interp, object, pos, &prop)
                        .unwrap_or(Value::Nil);
                    return Ok(Value::cons(value, Value::Nil));
                }
                let pos = position_from_value(interp, &args[0])?;
                let window_id = args
                    .get(2)
                    .and_then(|object| window_record_id_from_value(interp, object));
                let buffer_id = match args.get(2) {
                    Some(object) if window_id.is_some() => window_buffer_id(interp, object)
                        .ok_or_else(|| wrong_type_argument("window-live-p", object.clone()))?,
                    Some(object) if !object.is_nil() => interp.resolve_buffer_id(object)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                if let Some((value, overlay_id)) = highest_priority_overlay_property_with_id(
                    interp, buffer, pos, &prop, false, window_id,
                ) {
                    return Ok(Value::cons(value, Value::Overlay(overlay_id)));
                }
                let value = buffer_property_at_with_category(interp, buffer, pos, &prop)
                    .unwrap_or(Value::Nil);
                Ok(Value::cons(value, Value::Nil))
            }
            "get-text-property" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let prop = args[1].as_symbol()?.to_string();
                if let Some(object) = args.get(2) {
                    if string_like(object).is_some() {
                        let pos = args[0].as_integer()?.max(0) as usize;
                        Ok(string_property_at_with_category(interp, object, pos, &prop)
                            .unwrap_or(Value::Nil))
                    } else {
                        let pos = position_from_value(interp, &args[0])?;
                        let buffer_id = if object.is_nil() {
                            interp.current_buffer_id()
                        } else {
                            interp.resolve_buffer_id(object)?
                        };
                        let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?;
                        Ok(buffer_property_at_with_category(interp, buffer, pos, &prop)
                            .unwrap_or(Value::Nil))
                    }
                } else {
                    let pos = position_from_value(interp, &args[0])?;
                    Ok(
                        buffer_property_at_with_category(interp, &interp.buffer, pos, &prop)
                            .unwrap_or(Value::Nil),
                    )
                }
            }
            "text-property-any" | "text-property-not-all" => {
                if args.len() < 4 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let prop = args[2].as_symbol()?.to_string();
                let want_match = name == "text-property-any";
                if let Some(object) = args.get(4) {
                    if string_like(object).is_some() {
                        let start = args[0].as_integer()?.max(0) as usize;
                        let end = args[1].as_integer()?.max(0) as usize;
                        return Ok(text_property_search_string(
                            interp, object, start, end, &prop, &args[3], want_match,
                        )
                        .map(|pos| Value::Integer(pos as i64))
                        .unwrap_or(Value::Nil));
                    }

                    let buffer_id = if object.is_nil() {
                        interp.current_buffer_id()
                    } else {
                        interp.resolve_buffer_id(object)?
                    };
                    let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", buffer_id))
                    })?;
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    return Ok(text_property_search_buffer(
                        interp, buffer, start, end, &prop, &args[3], want_match,
                    )
                    .map(|pos| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil));
                }

                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                Ok(text_property_search_buffer(
                    interp,
                    &interp.buffer,
                    start,
                    end,
                    &prop,
                    &args[3],
                    want_match,
                )
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
            }
            "next-single-property-change" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let prop = args[1].as_symbol()?.to_string();
                let object = args.get(2).unwrap_or(&Value::Nil);
                let limit = match args.get(3).filter(|value| !value.is_nil()) {
                    Some(value) => Some(position_from_value(interp, value)?),
                    None => None,
                };
                if string_like(object).is_some() {
                    let pos = args[0].as_integer()?.max(0) as usize;
                    let text = string_text(object)?;
                    let max_pos = limit.unwrap_or(text.chars().count());
                    let initial = string_property_at_with_category(interp, object, pos, &prop)
                        .unwrap_or(Value::Nil);
                    for cursor in pos.saturating_add(1)..max_pos {
                        let current =
                            string_property_at_with_category(interp, object, cursor, &prop)
                                .unwrap_or(Value::Nil);
                        if !crate::buffer::text_property_values_eq(&current, &initial) {
                            return Ok(Value::Integer(cursor as i64));
                        }
                    }
                    return Ok(limit
                        .map(|value| Value::Integer(value as i64))
                        .unwrap_or(Value::Nil));
                }

                let pos = position_from_value(interp, &args[0])?.max(1);
                let (initial, max_pos) = {
                    let buffer_id = if object.is_nil() {
                        interp.current_buffer_id()
                    } else {
                        interp.resolve_buffer_id(object)?
                    };
                    let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", buffer_id))
                    })?;
                    (
                        buffer.text_property_at(pos, &prop).unwrap_or(Value::Nil),
                        limit.unwrap_or(buffer.point_max()),
                    )
                };
                for cursor in pos.saturating_add(1)..max_pos {
                    let current = if object.is_nil() {
                        interp
                            .buffer
                            .text_property_at(cursor, &prop)
                            .unwrap_or(Value::Nil)
                    } else {
                        let buffer_id = interp.resolve_buffer_id(object)?;
                        let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?;
                        buffer.text_property_at(cursor, &prop).unwrap_or(Value::Nil)
                    };
                    if !crate::buffer::text_property_values_eq(&current, &initial) {
                        return Ok(Value::Integer(cursor as i64));
                    }
                }
                Ok(limit
                    .map(|value| Value::Integer(value as i64))
                    .unwrap_or(Value::Nil))
            }
            "next-property-change" => {
                need_arg_range(name, args, 1, 3)?;
                let object = args.get(1).unwrap_or(&Value::Nil);
                let next_interval_only = matches!(args.get(2), Some(Value::T));
                let explicit_limit = args
                    .get(2)
                    .filter(|value| !value.is_nil() && !matches!(value, Value::T));
                if let Some(string) = string_like(object) {
                    let pos = args[0].as_integer()?.max(0) as usize;
                    let end = string.text.chars().count();
                    let limit = explicit_limit
                        .map(Value::as_integer)
                        .transpose()?
                        .map(|value| value.max(0) as usize)
                        .unwrap_or(end)
                        .min(end);
                    let change =
                        next_property_span_boundary(&string.props, pos, limit, next_interval_only);
                    return Ok(change
                        .map(|position| Value::Integer(position as i64))
                        .or_else(|| {
                            (explicit_limit.is_some() || next_interval_only)
                                .then_some(Value::Integer(limit as i64))
                        })
                        .unwrap_or(Value::Nil));
                }
                let pos = position_from_value(interp, &args[0])?;
                let buffer_id = if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                let end = buffer.point_max();
                let limit = explicit_limit
                    .map(|value| position_from_value(interp, value))
                    .transpose()?
                    .unwrap_or(end)
                    .min(end);
                let spans = buffer.full_property_spans();
                let change = next_property_span_boundary(&spans, pos, limit, next_interval_only);
                Ok(change
                    .map(|position| Value::Integer(position as i64))
                    .or_else(|| {
                        (explicit_limit.is_some() || next_interval_only)
                            .then_some(Value::Integer(limit as i64))
                    })
                    .unwrap_or(Value::Nil))
            }
            "next-single-char-property-change" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let prop = args[1].as_symbol()?.to_string();
                let object = args.get(2).unwrap_or(&Value::Nil);
                let limit = match args.get(3).filter(|value| !value.is_nil()) {
                    Some(value) => Some(position_from_value(interp, value)?),
                    None => None,
                };
                if string_like(object).is_some() {
                    let pos = args[0].as_integer()?.max(0) as usize;
                    let text = string_text(object)?;
                    let max_pos = limit.unwrap_or(text.chars().count());
                    let initial = string_property_at_with_category(interp, object, pos, &prop)
                        .unwrap_or(Value::Nil);
                    for cursor in pos.saturating_add(1)..max_pos {
                        let current =
                            string_property_at_with_category(interp, object, cursor, &prop)
                                .unwrap_or(Value::Nil);
                        if !crate::buffer::text_property_values_eq(&current, &initial) {
                            return Ok(Value::Integer(cursor as i64));
                        }
                    }
                    return Ok(limit
                        .map(|value| Value::Integer(value as i64))
                        .unwrap_or(Value::Nil));
                }

                let pos = position_from_value(interp, &args[0])?.max(1);
                let buffer_id = if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                let max_pos = limit.unwrap_or(buffer.point_max());
                let initial = buffer_char_property_at_with_overlay_id(interp, buffer, pos, &prop);
                for cursor in pos.saturating_add(1)..max_pos {
                    let current =
                        buffer_char_property_at_with_overlay_id(interp, buffer, cursor, &prop);
                    let same_overlay = initial.1.is_some() && initial.1 == current.1;
                    if !same_overlay
                        && !crate::buffer::text_property_values_eq(&current.0, &initial.0)
                    {
                        return Ok(Value::Integer(cursor as i64));
                    }
                }
                Ok(Value::Integer(max_pos as i64))
            }
            "previous-single-char-property-change" => {
                // (previous-single-char-property-change POSITION PROP &optional
                // OBJECT LIMIT): scan back for a change in PROP (text property or
                // overlay), returning the position AFTER the change.
                if args.len() < 2 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let prop = args[1].as_symbol()?.to_string();
                let object = args.get(2).unwrap_or(&Value::Nil);
                let limit = match args.get(3).filter(|value| !value.is_nil()) {
                    Some(value) => Some(position_from_value(interp, value)?),
                    None => None,
                };
                let pos = position_from_value(interp, &args[0])?;
                let buffer_id = if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                let min_pos = limit.unwrap_or(buffer.point_min());
                if pos <= min_pos {
                    return Ok(Value::Integer(min_pos as i64));
                }
                // The property "at" POS for backward scans is that of the char
                // before POS.
                let initial = buffer_char_property_at_with_overlay_id(
                    interp,
                    buffer,
                    pos.saturating_sub(1),
                    &prop,
                );
                let mut cursor = pos;
                while cursor > min_pos + 1 {
                    let current =
                        buffer_char_property_at_with_overlay_id(interp, buffer, cursor - 2, &prop);
                    let same_overlay = initial.1.is_some() && initial.1 == current.1;
                    if !same_overlay
                        && !crate::buffer::text_property_values_eq(&current.0, &initial.0)
                    {
                        return Ok(Value::Integer((cursor - 1) as i64));
                    }
                    cursor -= 1;
                }
                Ok(Value::Integer(min_pos as i64))
            }
            "previous-single-property-change" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let prop = args[1].as_symbol()?.to_string();
                let object = args.get(2).unwrap_or(&Value::Nil);
                let limit = match args.get(3).filter(|value| !value.is_nil()) {
                    Some(value) => Some(position_from_value(interp, value)?),
                    None => None,
                };
                if string_like(object).is_some() {
                    let pos = args[0].as_integer()?.max(0) as usize;
                    let min_pos = limit.unwrap_or(0);
                    if pos <= min_pos {
                        return Ok(limit
                            .map(|value| Value::Integer(value as i64))
                            .unwrap_or(Value::Nil));
                    }
                    let initial = string_property_at(object, pos.saturating_sub(1), &prop)
                        .unwrap_or(Value::Nil);
                    for cursor in (min_pos..pos).rev() {
                        let previous = cursor
                            .checked_sub(1)
                            .and_then(|index| string_property_at(object, index, &prop))
                            .unwrap_or(Value::Nil);
                        if !crate::buffer::text_property_values_eq(&previous, &initial) {
                            return Ok(Value::Integer(cursor as i64));
                        }
                    }
                    return Ok(limit
                        .map(|value| Value::Integer(value as i64))
                        .unwrap_or(Value::Nil));
                }

                let pos = position_from_value(interp, &args[0])?.max(1);
                let buffer_id = if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                let min_pos = limit.unwrap_or(buffer.point_min());
                if pos <= min_pos {
                    return Ok(limit
                        .map(|value| Value::Integer(value as i64))
                        .unwrap_or(Value::Nil));
                }
                let initial = buffer
                    .text_property_at(pos.saturating_sub(1), &prop)
                    .unwrap_or(Value::Nil);
                for cursor in (min_pos..pos).rev() {
                    let previous = cursor
                        .checked_sub(1)
                        .and_then(|index| buffer.text_property_at(index, &prop))
                        .unwrap_or(Value::Nil);
                    if !crate::buffer::text_property_values_eq(&previous, &initial) {
                        return Ok(Value::Integer(cursor as i64));
                    }
                }
                Ok(limit
                    .map(|value| Value::Integer(value as i64))
                    .unwrap_or(Value::Nil))
            }
            "previous-property-change" => {
                need_arg_range(name, args, 1, 3)?;
                let object = args.get(1).unwrap_or(&Value::Nil);
                let explicit_limit = args.get(2).filter(|value| !value.is_nil());
                if let Some(string) = string_like(object) {
                    let pos = args[0].as_integer()?.max(0) as usize;
                    let limit = explicit_limit
                        .map(Value::as_integer)
                        .transpose()?
                        .map(|value| value.max(0) as usize)
                        .unwrap_or(0);
                    let change = previous_property_span_boundary(&string.props, pos, limit);
                    return Ok(change
                        .map(|position| Value::Integer(position as i64))
                        .or_else(|| {
                            explicit_limit
                                .is_some()
                                .then_some(Value::Integer(limit as i64))
                        })
                        .unwrap_or(Value::Nil));
                }
                let pos = position_from_value(interp, &args[0])?;
                let buffer_id = if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                let limit = explicit_limit
                    .map(|value| position_from_value(interp, value))
                    .transpose()?
                    .unwrap_or(buffer.point_min())
                    .max(buffer.point_min());
                let spans = buffer.full_property_spans();
                let change = previous_property_span_boundary(&spans, pos, limit);
                Ok(change
                    .map(|position| Value::Integer(position as i64))
                    .or_else(|| {
                        explicit_limit
                            .is_some()
                            .then_some(Value::Integer(limit as i64))
                    })
                    .unwrap_or(Value::Nil))
            }
            "next-char-property-change" => {
                need_arg_range(name, args, 1, 2)?;
                let position = position_from_value(interp, &args[0])?;
                let mut limit =
                    super::overlays::next_overlay_change_position(&interp.buffer, position) as i64;
                if let Some(explicit_limit) = args.get(1).filter(|value| !value.is_nil()) {
                    limit = limit.min(explicit_limit.as_integer()?);
                }
                let change = usize::try_from(limit).ok().and_then(|limit| {
                    next_property_span_boundary(
                        &interp.buffer.full_property_spans(),
                        position,
                        limit,
                        false,
                    )
                });
                Ok(change
                    .map(|position| Value::Integer(position as i64))
                    .unwrap_or(Value::Integer(limit)))
            }
            "previous-char-property-change" => {
                need_arg_range(name, args, 1, 2)?;
                let position = position_from_value(interp, &args[0])?;
                let mut limit =
                    super::overlays::previous_overlay_change_position(&interp.buffer, position)
                        as i64;
                if let Some(explicit_limit) = args.get(1).filter(|value| !value.is_nil()) {
                    limit = limit.max(explicit_limit.as_integer()?);
                }
                let change = usize::try_from(limit).ok().and_then(|limit| {
                    previous_property_span_boundary(
                        &interp.buffer.full_property_spans(),
                        position,
                        limit,
                    )
                });
                Ok(change
                    .map(|position| Value::Integer(position as i64))
                    .unwrap_or(Value::Integer(limit)))
            }
            "text-properties-at" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let props = if let Some(object) = args.get(1) {
                    if string_like(object).is_some() {
                        let pos = args[0].as_integer()?.max(0) as usize;
                        string_properties_at(object, pos)
                    } else {
                        let pos = position_from_value(interp, &args[0])?;
                        let buffer_id = if object.is_nil() {
                            interp.current_buffer_id()
                        } else {
                            interp.resolve_buffer_id(object)?
                        };
                        interp
                            .get_buffer_by_id(buffer_id)
                            .ok_or_else(|| {
                                LispError::Signal(format!("No buffer with id {}", buffer_id))
                            })?
                            .text_properties_at(pos)
                    }
                } else {
                    let pos = position_from_value(interp, &args[0])?;
                    interp.buffer.text_properties_at(pos)
                };
                Ok(plist_value(&props))
            }
            "object-intervals" => {
                need_args(name, args, 1)?;
                object_intervals_value(interp, &args[0])
            }
            "put-text-property" => {
                if args.len() < 4 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let prop = args[2].as_symbol()?.to_string();
                let prop_value = match &args[3] {
                    Value::StringObject(state) if state.borrow().props.is_empty() => {
                        Value::String(state.borrow().text.clone().into())
                    }
                    _ => args[3].clone(),
                };
                if let Some(object) = args.get(4) {
                    if string_like(object).is_some() {
                        let start = args[0].as_integer()?.max(0) as usize;
                        let end = args[1].as_integer()?.max(0) as usize;
                        modify_shared_string_properties(object, start, end, |mut current| {
                            // GNU replaces an existing property in place and
                            // conses a new one onto the plist head.
                            if let Some((_, existing)) =
                                current.iter_mut().find(|(key, _)| key == &prop)
                            {
                                *existing = prop_value.clone();
                            } else {
                                current.insert(0, (prop.clone(), prop_value.clone()));
                            }
                            current
                        })?;
                    } else {
                        let start = position_from_value(interp, &args[0])?;
                        let end = position_from_value(interp, &args[1])?;
                        interp.apply_text_property_change_shared(&|buffer| {
                            buffer.put_text_property(start, end, &prop, prop_value.clone())
                        });
                    }
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.apply_text_property_change_shared(&|buffer| {
                        buffer.put_text_property(start, end, &prop, prop_value.clone())
                    });
                }
                Ok(Value::T)
            }
            "add-text-properties" => {
                if args.len() < 3 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let props = plist_pairs(&args[2])?;
                if let Some(object) = args.get(3) {
                    if string_like(object).is_some() {
                        let start = args[0].as_integer()?.max(0) as usize;
                        let end = args[1].as_integer()?.max(0) as usize;
                        modify_shared_string_properties(object, start, end, |mut current| {
                            // GNU replaces existing properties in place and
                            // conses new ones onto the plist head.
                            for (name, value) in &props {
                                if let Some((_, existing)) =
                                    current.iter_mut().find(|(key, _)| key == name)
                                {
                                    *existing = value.clone();
                                } else {
                                    current.insert(0, (name.clone(), value.clone()));
                                }
                            }
                            current
                        })?;
                    } else {
                        let start = position_from_value(interp, &args[0])?;
                        let end = position_from_value(interp, &args[1])?;
                        interp.apply_text_property_change_shared(&|buffer| {
                            buffer.add_text_properties(start, end, &props)
                        });
                    }
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.apply_text_property_change_shared(&|buffer| {
                        buffer.add_text_properties(start, end, &props)
                    });
                }
                Ok(Value::T)
            }
            "set-text-properties" => {
                if args.len() < 3 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let props = plist_pairs(&args[2])?;
                if let Some(object) = args.get(3) {
                    if matches!(object, Value::String(_)) {
                        return Ok(Value::T);
                    }
                    if string_like(object).is_some() {
                        let start = args[0].as_integer()?.max(0) as usize;
                        let end = args[1].as_integer()?.max(0) as usize;
                        modify_shared_string_properties(object, start, end, |_| props.clone())?;
                    } else {
                        let start = position_from_value(interp, &args[0])?;
                        let end = position_from_value(interp, &args[1])?;
                        interp.apply_text_property_change_shared(&|buffer| {
                            buffer.set_text_properties(start, end, &props)
                        });
                    }
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.apply_text_property_change_shared(&|buffer| {
                        buffer.set_text_properties(start, end, &props)
                    });
                }
                Ok(Value::T)
            }
            "remove-list-of-text-properties" => {
                if args.len() < 3 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let names = args[2]
                    .to_vec()?
                    .into_iter()
                    .map(|value| value.as_symbol().map(|s| s.to_string()))
                    .collect::<Result<Vec<_>, _>>()?;
                if let Some(object) = args.get(3) {
                    if string_like(object).is_some() {
                        let start = args[0].as_integer()?.max(0) as usize;
                        let end = args[1].as_integer()?.max(0) as usize;
                        modify_shared_string_properties(object, start, end, |current| {
                            current
                                .into_iter()
                                .filter(|(key, _)| !names.iter().any(|name| name == key))
                                .collect()
                        })?;
                    } else {
                        let start = position_from_value(interp, &args[0])?;
                        let end = position_from_value(interp, &args[1])?;
                        interp.apply_text_property_change_shared(&|buffer| {
                            buffer.remove_list_of_text_properties(start, end, &names)
                        });
                    }
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.apply_text_property_change_shared(&|buffer| {
                        buffer.remove_list_of_text_properties(start, end, &names)
                    });
                }
                Ok(Value::T)
            }
            "remove-text-properties" => {
                if args.len() < 3 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let names = plist_pairs(&args[2])?
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>();
                if let Some(object) = args.get(3) {
                    if string_like(object).is_some() {
                        let start = args[0].as_integer()?.max(0) as usize;
                        let end = args[1].as_integer()?.max(0) as usize;
                        modify_shared_string_properties(object, start, end, |current| {
                            current
                                .into_iter()
                                .filter(|(key, _)| !names.iter().any(|name| name == key))
                                .collect()
                        })?;
                    } else {
                        let start = position_from_value(interp, &args[0])?;
                        let end = position_from_value(interp, &args[1])?;
                        interp.apply_text_property_change_shared(&|buffer| {
                            buffer.remove_list_of_text_properties(start, end, &names)
                        });
                    }
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.apply_text_property_change_shared(&|buffer| {
                        buffer.remove_list_of_text_properties(start, end, &names)
                    });
                }
                Ok(Value::T)
            }
            "add-face-text-property" => add_face_text_property(interp, name, args),
            "put" => {
                need_args(name, args, 3)?;
                // GNU 30.2 fns.c:Fput shares `get's CHECK_SYMBOL/XSYMBOL
                // treatment of source-positioned symbols.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                let property_name = super::misc::bare_symbol_name(interp, env, &args[1]);
                // GNU Elisp owns `ert-set-test' and `define-symbol-prop',
                // including duplicate-definition policy and load-history
                // provenance.  Mirror the final public property write into
                // Emaxx's native runner index only when it is an actual ERT
                // record; ordinary user properties remain ordinary `put'.
                if property_name.as_deref() == Some("ert--test")
                    && matches!(&args[2], Value::Record(_))
                {
                    return interp.ert_set_test(&symbol, &args[2]);
                }
                let mut plist = interp.symbol_plist(&symbol).to_vec()?;
                let mut replaced = false;
                for pair in plist.as_chunks_mut::<2>().0 {
                    if values_eq_in_env(interp, &pair[0], &args[1], env) {
                        pair[1] = args[2].clone();
                        replaced = true;
                        break;
                    }
                }
                if !replaced {
                    plist.push(args[1].clone());
                    plist.push(args[2].clone());
                }
                interp.set_symbol_plist(&symbol, Value::list(plist))?;
                Ok(args[2].clone())
            }
        }
    }
);

fn buffer_has_field_property(interp: &Interpreter) -> bool {
    interp.buffer.has_text_property_named("field")
}

/// Column width of a display spec form: integers are columns, one-element
/// lists are pixels (one per column on the batch frame), symbols are
/// variables, and (+ ...)/(- ...) combine recursively.
fn display_spec_width(interp: &mut Interpreter, env: &mut Env, spec: &Value) -> i64 {
    match spec {
        Value::Integer(value) => *value,
        Value::Float(value) => *value as i64,
        Value::Symbol(name) => interp
            .lookup_var(name, env)
            .map(|value| display_spec_width(interp, env, &value))
            .unwrap_or(0),
        Value::Cons(_) => {
            let Ok(items) = spec.to_vec() else { return 0 };
            match items.first() {
                Some(Value::Symbol(op)) if op == "-" || op == "+" => {
                    let mut acc = items
                        .get(1)
                        .map(|item| display_spec_width(interp, env, item))
                        .unwrap_or(0);
                    for item in items.iter().skip(2) {
                        let value = display_spec_width(interp, env, item);
                        if op == "-" {
                            acc -= value;
                        } else {
                            acc += value;
                        }
                    }
                    acc
                }
                Some(inner) if items.len() == 1 => display_spec_width(interp, env, inner),
                _ => 0,
            }
        }
        _ => 0,
    }
}

/// Column width contributed by a `line-prefix'/`wrap-prefix' property.
fn prefix_property_width(interp: &mut Interpreter, env: &mut Env, prop: Option<Value>) -> usize {
    let Some(prop) = prop else { return 0 };
    if let Some(text) = string_like(&prop) {
        return text.text.chars().count();
    }
    if let Ok(items) = prop.to_vec()
        && matches!(items.first(), Some(Value::Symbol(head)) if head == "space")
    {
        let mut cursor = 1;
        while cursor + 1 < items.len() {
            if matches!(&items[cursor], Value::Symbol(key) if key == ":width") {
                return display_spec_width(interp, env, &items[cursor + 1]).max(0) as usize;
            }
            cursor += 2;
        }
    }
    0
}

/// Per-position display widths for [bol, eol): `usize::MAX` marks a TAB
/// (resolved against the running column), 0 marks invisible or
/// display-replaced text, and display strings count once per run.
pub(crate) fn visual_char_widths(
    interp: &mut Interpreter,
    env: &mut Env,
    bol: usize,
    eol: usize,
) -> Vec<(usize, bool)> {
    let _ = env;
    let mut widths = Vec::with_capacity(eol.saturating_sub(bol));
    let mut pos = bol;
    while pos < eol {
        let display = interp.buffer.text_property_at(pos, "display");
        if let Some(display_value) = display.clone().filter(|value| !value.is_nil()) {
            let mut end = pos;
            while end < eol && interp.buffer.text_property_at(end, "display") == display {
                end += 1;
            }
            let run_width = string_like(&display_value)
                .map(|text| text.text.chars().count())
                .unwrap_or(0);
            widths.push((run_width, false));
            for _ in pos + 1..end {
                widths.push((0, false));
            }
            pos = end;
            continue;
        }
        if interp
            .buffer
            .text_property_at(pos, "invisible")
            .is_some_and(|value| value.is_truthy())
        {
            widths.push((0, false));
            pos += 1;
            continue;
        }
        match interp.buffer.char_at(pos) {
            Some('\t') => widths.push((usize::MAX, true)),
            Some(' ') => widths.push((1, true)),
            Some(_) => widths.push((1, false)),
            None => widths.push((0, false)),
        }
        pos += 1;
    }
    widths
}

/// Screen-line segment start positions of the logical line [bol, eol),
/// modeling GNU's batch display: continuation at frame-width (reserving the
/// continuation column unless word-wrapping) minus prefix widths.
/// The columns a `display-line-numbers' column occupies during
/// screen-line motion: Fvertical_motion primes its iterator's
/// lnum_width from line_number_display_width — the width redisplay
/// computes at the window's start — so wrapped lines break that many
/// columns earlier.
fn motion_line_number_columns(interp: &mut Interpreter, env: &mut Env) -> usize {
    let buffer_id = interp.current_buffer_id();
    let enabled = interp
        .buffer_local_value(buffer_id, "display-line-numbers")
        .or_else(|| interp.lookup_var("display-line-numbers", &Vec::new()))
        .unwrap_or(Value::Nil);
    if enabled.is_nil() {
        return 0;
    }
    let text_rows = super::call(interp, "window-body-height", &[], env)
        .ok()
        .and_then(|value| value.as_integer().ok())
        .unwrap_or(0)
        .max(1) as usize;
    let start = interp
        .selected_window_start()
        .clamp(interp.buffer.point_min(), interp.buffer.point_max());
    let top_line = interp.buffer.line_number_at_pos(start);
    let point_line = interp.buffer.line_number_at_pos(interp.buffer.point());
    let begv_line = interp.buffer.line_number_at_pos(interp.buffer.point_min());
    crate::lisp::primitives::window_line_number_layout(
        interp, buffer_id, top_line, point_line, begv_line, text_rows,
    )
    .map_or(0, |layout| layout.cols)
}

pub(crate) fn visual_segment_starts(
    interp: &mut Interpreter,
    env: &mut Env,
    bol: usize,
    eol: usize,
) -> Vec<usize> {
    let mut starts = vec![bol];
    if interp
        .lookup_var("truncate-lines", env)
        .is_some_and(|value| value.is_truthy())
    {
        return starts;
    }
    // GNU's batch display wraps mid-word at frame-width minus the
    // continuation column, even under `word-wrap' (vmotion in indent.c).
    // A line-number column narrows the text area on every row.
    let frame_width = interp
        .frame_width()
        .max(2)
        .saturating_sub(motion_line_number_columns(interp, env) as i64)
        .max(2) as usize;
    let reserve = 1;
    let line_prefix_width = {
        let prop = interp.buffer.text_property_at(bol, "line-prefix");
        prefix_property_width(interp, env, prop)
    };
    let wrap_prefix_width = {
        let prop = interp.buffer.text_property_at(bol, "wrap-prefix");
        prefix_property_width(interp, env, prop)
    };
    let widths = visual_char_widths(interp, env, bol, eol);
    let mut seg_start = bol;
    let mut first = true;
    loop {
        let usable = frame_width
            .saturating_sub(reserve)
            .saturating_sub(if first {
                line_prefix_width
            } else {
                wrap_prefix_width
            })
            .max(1);
        let mut pos = seg_start;
        let mut col = 0usize;
        let mut wrap_at: Option<usize> = None;
        while pos < eol {
            let (raw, _) = widths[pos - bol];
            let width = if raw == usize::MAX {
                8 - (col % 8)
            } else {
                raw
            };
            if col + width > usable && pos > seg_start {
                wrap_at = Some(pos);
                break;
            }
            col += width;
            pos += 1;
        }
        let Some(next_start) = wrap_at else { break };
        if next_start <= seg_start {
            break;
        }
        starts.push(next_start);
        seg_start = next_start;
        first = false;
    }
    starts
}

pub(crate) fn visual_line_bounds(interp: &Interpreter, pos: usize) -> (usize, usize) {
    let point_min = interp.buffer.point_min();
    let point_max = interp.buffer.point_max();
    let raw_bol = |mut from: usize| {
        while from > point_min && interp.buffer.char_at(from - 1) != Some('\n') {
            from -= 1;
        }
        from
    };
    let raw_eol = |mut from: usize| {
        while from < point_max && interp.buffer.char_at(from) != Some('\n') {
            from += 1;
        }
        from
    };
    let mut bol = raw_bol(pos.max(point_min));
    let mut eol = raw_eol(pos.max(point_min));
    // GNU's vertical-motion walks display lines through the iterator:
    // a newline hidden by the `invisible' property does not end the
    // line, so a display line spans every raw line an invisible run
    // joins (org's folded subtrees collapse onto their headline).
    let spec = crate::lisp::primitives::resolve_buffer_invisibility(
        interp,
        &interp.buffer,
        interp.current_buffer_id(),
    );
    if spec.active {
        while bol > point_min
            && crate::lisp::primitives::invisible_class_at(&interp.buffer, &spec, bol - 1) != 0
        {
            bol = raw_bol(bol - 1);
        }
        while eol < point_max {
            let Some((run_end, _)) =
                crate::lisp::primitives::invisible_run_at(&interp.buffer, &spec, eol)
            else {
                break;
            };
            eol = raw_eol(run_end);
        }
    }
    (bol, eol)
}

fn live_motion_window(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    let window = if value.is_nil() {
        interp.selected_window_value()
    } else {
        value.clone()
    };
    let Some(window_id) = window_record_id_from_value(interp, &window) else {
        return Err(wrong_type_argument("window-live-p", window));
    };
    let kind = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
        .cloned()
        .unwrap_or(Value::Nil);
    if matches!(
        kind,
        Value::Symbol(ref kind)
            if matches!(
                kind.as_str(),
                INTERNAL_HORIZONTAL_WINDOW_KIND
                    | INTERNAL_VERTICAL_WINDOW_KIND
                    | DELETED_WINDOW_KIND
            )
    ) || window_buffer_id(interp, &window).is_none()
    {
        return Err(wrong_type_argument("window-live-p", window));
    }
    Ok(window)
}

fn motion_pair(value: &Value) -> Result<(i64, i64), LispError> {
    let (horizontal, vertical) = value
        .cons_values()
        .ok_or_else(|| wrong_type_argument("consp", value.clone()))?;
    Ok((horizontal.as_integer()?, vertical.as_integer()?))
}

fn checked_motion_position(interp: &Interpreter, value: &Value) -> Result<usize, LispError> {
    let position = position_from_value(interp, value)?;
    if position < interp.buffer.point_min() || position > interp.buffer.point_max() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            value.clone(),
            Value::Integer(interp.buffer.point_min() as i64),
            Value::Integer(interp.buffer.point_max() as i64),
        ])));
    }
    Ok(position)
}

fn display_motion_width(
    interp: &Interpreter,
    env: &Env,
    position: usize,
    character: char,
    hpos: i64,
    hscroll: i64,
    tab_offset: i64,
) -> i64 {
    if char_is_invisible(interp, position, env) {
        return 0;
    }
    if let Some(display) = interp
        .buffer
        .text_property_at(position, "display")
        .filter(|value| !value.is_nil())
    {
        let begins_run = position == interp.buffer.point_min()
            || interp.buffer.text_property_at(position - 1, "display") != Some(display.clone());
        if !begins_run {
            return 0;
        }
        return string_like(&display)
            .map(|string| {
                string
                    .text
                    .chars()
                    .map(|character| character.width().unwrap_or(0) as i64)
                    .sum()
            })
            .unwrap_or(0);
    }
    if character == '\t' {
        let tab_width = interp
            .lookup_var("tab-width", env)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(8)
            .max(1);
        let origin = hpos + tab_offset + hscroll - i64::from(hscroll > 0);
        return tab_width - origin.rem_euclid(tab_width);
    }
    if character.is_control() {
        return if interp
            .lookup_var("ctl-arrow", env)
            .is_some_and(|value| value.is_truthy())
        {
            2
        } else {
            4
        };
    }
    character.width().unwrap_or(0) as i64
}

fn compute_motion_value(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    let from = checked_motion_position(interp, &args[0])?;
    let (mut hpos, mut vpos) = motion_pair(&args[1])?;
    let to = checked_motion_position(interp, &args[2])?;
    let window = live_motion_window(interp, &args[6])?;

    let body_width = super::call(
        interp,
        "window-body-width",
        std::slice::from_ref(&window),
        env,
    )?
    .as_integer()?;
    let body_height = super::call(
        interp,
        "window-body-height",
        std::slice::from_ref(&window),
        env,
    )?
    .as_integer()?;
    let (target_hpos, target_vpos) = if args[3].is_nil() {
        ((body_width - 1).max(0), body_height)
    } else {
        motion_pair(&args[3])?
    };
    let mut width = if args[4].is_nil() {
        body_width - 1
    } else {
        args[4].as_integer()?
    };
    if width < 0 {
        width = body_width - 1;
    }
    // A zero-width window has no useful headless rendering model; GNU's
    // redisplay engine still guarantees forward progress through one
    // canonical display cell.
    width = width.max(1);

    let (hscroll, mut tab_offset) = if args[5].is_nil() {
        (0, 0)
    } else {
        let (hscroll, tab_offset) = motion_pair(&args[5])?;
        if hscroll < 0 || tab_offset < 0 {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::Integer(hscroll),
                Value::Integer(tab_offset),
            ])));
        }
        (hscroll, tab_offset)
    };
    let truncates_partial_window = match interp.lookup_var("truncate-partial-width-windows", env) {
        Some(Value::Integer(threshold)) => {
            width + 1 < interp.frame_width() && width + 1 < threshold
        }
        Some(value) => value.is_truthy() && width + 1 < interp.frame_width(),
        None => false,
    };
    let truncates = hscroll > 0
        || truncates_partial_window
        || interp
            .lookup_var("truncate-lines", env)
            .is_some_and(|value| value.is_truthy());
    let left_margin = if hscroll > 0 { 1 - hscroll } else { 0 };

    let mut position = from;
    let mut previous_hpos = 0;
    let mut continuation_hpos = None;
    let mut consumed_character_at_limit = false;
    loop {
        let next_character = interp.buffer.char_at(position);

        // A position at the right edge belongs to the beginning of the
        // continuation line whenever more text follows on the logical line.
        if hpos >= width && next_character.is_some_and(|character| character != '\n') {
            if truncates {
                while position < to
                    && interp
                        .buffer
                        .char_at(position)
                        .is_some_and(|character| character != '\n')
                {
                    position += 1;
                }
                hpos = width;
                previous_hpos = width;
                consumed_character_at_limit = false;
            } else {
                let origin = hpos;
                vpos += 1;
                hpos = left_margin;
                tab_offset += width;
                previous_hpos = 0;
                continuation_hpos = Some(origin);
                consumed_character_at_limit = false;
            }
        }

        if vpos > target_vpos || (vpos == target_vpos && hpos >= target_hpos) {
            break;
        }
        if position >= to || position >= interp.buffer.point_max() {
            break;
        }
        let Some(character) = interp.buffer.char_at(position) else {
            break;
        };
        previous_hpos = hpos;
        if character == '\n' {
            position += 1;
            vpos += 1;
            hpos = left_margin;
            tab_offset = 0;
            continuation_hpos = None;
            consumed_character_at_limit = true;
            continue;
        }

        let character_width =
            display_motion_width(interp, env, position, character, hpos, hscroll, tab_offset);
        if !truncates
            && character != '\t'
            && character_width > 1
            && hpos > left_margin
            && hpos + character_width > width
        {
            let origin = hpos;
            vpos += 1;
            hpos = left_margin;
            tab_offset += width;
            continuation_hpos = Some(origin);
        }

        previous_hpos = hpos;
        hpos += character_width;
        position += 1;
        consumed_character_at_limit = true;
        if hpos > width {
            if truncates {
                while position < to
                    && interp
                        .buffer
                        .char_at(position)
                        .is_some_and(|character| character != '\n')
                {
                    position += 1;
                }
                hpos = width;
                previous_hpos = width;
                consumed_character_at_limit = false;
            } else {
                let origin = previous_hpos;
                hpos -= width;
                vpos += 1;
                tab_offset += width;
                previous_hpos = 0;
                continuation_hpos = Some(origin);
            }
        }
    }

    if position == to && to < interp.buffer.point_max() && consumed_character_at_limit {
        previous_hpos = hpos;
        continuation_hpos = None;
    }
    let continued = continuation_hpos.is_some() && previous_hpos == 0;
    let previous_hpos = if continued {
        continuation_hpos.unwrap_or(previous_hpos)
    } else {
        previous_hpos
    };
    Ok(Value::list([
        Value::Integer(position as i64),
        Value::Integer(hpos),
        Value::Integer(vpos),
        Value::Integer(previous_hpos),
        if continued { Value::T } else { Value::Nil },
    ]))
}

fn line_number_display_width_value(
    interp: &mut Interpreter,
    env: &mut Env,
    pixelwise: Option<&Value>,
) -> Result<Value, LispError> {
    let body_height = super::call(interp, "window-body-height", &[], env)?
        .as_integer()?
        .max(1) as usize;
    let selected_buffer = interp.selected_window_buffer_id();
    let saved_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(selected_buffer)?;
    let result = (|| -> Result<Value, LispError> {
        let mode = interp
            .lookup_var("display-line-numbers", env)
            .unwrap_or(Value::Nil);
        if mode.is_nil() {
            return Ok(
                if matches!(pixelwise, Some(Value::Symbol(name)) if name == "columns") {
                    Value::Float(0.0)
                } else {
                    Value::Integer(0)
                },
            );
        }

        let start = interp
            .selected_window_start()
            .clamp(interp.buffer.point_min(), interp.buffer.point_max());
        let start_line = interp.buffer.line_number_at_pos(start);
        let last_line = interp.buffer.line_number_at_pos(interp.buffer.point_max());
        let visible_end_line = start_line
            .saturating_add(body_height.saturating_sub(1))
            .min(last_line);
        let displayed_maximum = if matches!(
            mode,
            Value::Symbol(ref name) if matches!(name.as_str(), "relative" | "visual")
        ) {
            let point_line = interp.buffer.line_number_at_pos(interp.buffer.point());
            point_line
                .abs_diff(start_line)
                .max(point_line.abs_diff(visible_end_line))
        } else {
            visible_end_line
        };
        let computed_width = displayed_maximum.max(1).to_string().len().max(2) as i64;
        let requested_width = interp
            .lookup_var("display-line-numbers-width", env)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(0)
            .max(0);
        let columns = computed_width.max(requested_width);
        // The line-number face contributes two canonical columns of
        // left/right padding in GNU's headless terminal display.
        let pixels = columns + 2;
        Ok(match pixelwise {
            Some(Value::Symbol(name)) if name == "columns" => Value::Float(pixels as f64),
            Some(value) if value.is_truthy() => Value::Integer(pixels),
            _ => Value::Integer(columns),
        })
    })();
    let restore = interp.set_current_buffer_id(saved_buffer);
    match (result, restore) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

/// GNU `vertical-motion': move by screen lines, honoring continuation.
fn visual_vertical_motion(
    interp: &mut Interpreter,
    env: &mut Env,
    n: i64,
    goal_col: Option<usize>,
) -> Result<i64, LispError> {
    let point_min = interp.buffer.point_min();
    let point_max = interp.buffer.point_max();
    let point = interp.buffer.point();
    let (mut bol, mut eol) = visual_line_bounds(interp, point);
    let mut starts = visual_segment_starts(interp, env, bol, eol);
    let mut index = starts
        .iter()
        .rposition(|&start| start <= point)
        .unwrap_or(0);
    let mut moved = 0i64;
    let mut exhausted_forward = false;
    while moved < n {
        if index + 1 < starts.len() {
            index += 1;
            moved += 1;
        } else if eol < point_max {
            let (next_bol, next_eol) = visual_line_bounds(interp, eol + 1);
            bol = next_bol;
            eol = next_eol;
            starts = visual_segment_starts(interp, env, bol, eol);
            index = 0;
            moved += 1;
        } else {
            exhausted_forward = true;
            break;
        }
    }
    while moved > n {
        if index > 0 {
            index -= 1;
            moved -= 1;
        } else if bol > point_min {
            let (prev_bol, prev_eol) = visual_line_bounds(interp, bol - 1);
            bol = prev_bol;
            eol = prev_eol;
            starts = visual_segment_starts(interp, env, bol, eol);
            index = starts.len().saturating_sub(1);
            moved -= 1;
        } else {
            break;
        }
    }
    // GNU's batch path ignores a cons LINES's goal column: point lands at
    // the start of the target screen line (or buffer end on overshoot).
    // An interactive session has a real window, where GNU moves to the
    // goal column within the target screen line (line-move-visual's
    // temporary-goal-column contract).
    let target = if exhausted_forward {
        point_max
    } else {
        let mut target = starts[index];
        let interactive = interp
            .lookup_var("noninteractive", env)
            .is_some_and(|value| value.is_nil());
        if let Some(goal) = goal_col.filter(|&goal| goal > 0 && interactive) {
            let seg_end = starts.get(index + 1).copied().unwrap_or(eol);
            let widths = visual_char_widths(interp, env, bol, eol);
            let mut col = 0usize;
            while target < seg_end && col < goal {
                let (raw, _) = widths[target - bol];
                col += if raw == usize::MAX {
                    8 - (col % 8)
                } else {
                    raw
                };
                target += 1;
            }
        }
        target
    };
    interp.buffer.goto_char(target);
    Ok(moved)
}
