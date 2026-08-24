use super::*;

fn replacement_case_action(
    interp: &Interpreter,
    matched: &str,
    env: &crate::lisp::types::Env,
) -> Option<CaseAction> {
    let case_symbols_as_words = case_symbols_as_words_enabled(interp, env);
    let mut previous_is_word = false;
    let mut some_multiletter_word = false;
    let mut some_lowercase = false;
    let mut some_uppercase = false;
    let mut some_nonuppercase_initial = false;

    for character in matched.chars() {
        let code = character as u32;
        let lowercase = simple_upcase_char(code) != code;
        let uppercase = simple_downcase_char(code, false) != code;
        if lowercase {
            some_lowercase = true;
            if previous_is_word {
                some_multiletter_word = true;
            } else {
                some_nonuppercase_initial = true;
            }
        } else if uppercase {
            some_uppercase = true;
            if previous_is_word {
                some_multiletter_word = true;
            }
        } else if !previous_is_word {
            // This mirrors search.c's treatment of a caseless character at a
            // word boundary: it cannot establish a capitalized word.
            some_nonuppercase_initial = true;
        }
        previous_is_word = case_word_char(interp, character, case_symbols_as_words);
    }

    if !some_lowercase && some_multiletter_word {
        Some(CaseAction::Up)
    } else if !some_nonuppercase_initial && some_multiletter_word {
        Some(CaseAction::UpcaseInitials)
    } else if !some_nonuppercase_initial && some_uppercase {
        Some(CaseAction::Up)
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BufferReplacementHunk {
    old_start: usize,
    old_end: usize,
    new_start: usize,
    new_end: usize,
}

fn replacement_hunks_from_matches(
    old_len: usize,
    new_len: usize,
    matches: &[(usize, usize)],
) -> Vec<BufferReplacementHunk> {
    let mut hunks = Vec::new();
    let (mut old_cursor, mut new_cursor) = (0, 0);
    for &(old_index, new_index) in matches {
        if old_cursor != old_index || new_cursor != new_index {
            hunks.push(BufferReplacementHunk {
                old_start: old_cursor,
                old_end: old_index,
                new_start: new_cursor,
                new_end: new_index,
            });
        }
        old_cursor = old_index + 1;
        new_cursor = new_index + 1;
    }
    if old_cursor != old_len || new_cursor != new_len {
        hunks.push(BufferReplacementHunk {
            old_start: old_cursor,
            old_end: old_len,
            new_start: new_cursor,
            new_end: new_len,
        });
    }
    hunks
}

fn heuristic_replacement_hunks(old: &[char], new: &[char]) -> Vec<BufferReplacementHunk> {
    let mut prefix = 0;
    while prefix < old.len() && prefix < new.len() && old[prefix] == new[prefix] {
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < old.len().saturating_sub(prefix)
        && suffix < new.len().saturating_sub(prefix)
        && old[old.len() - 1 - suffix] == new[new.len() - 1 - suffix]
    {
        suffix += 1;
    }
    if prefix == old.len() && prefix == new.len() {
        Vec::new()
    } else {
        vec![BufferReplacementHunk {
            old_start: prefix,
            old_end: old.len() - suffix,
            new_start: prefix,
            new_end: new.len() - suffix,
        }]
    }
}

/// Compute a bounded longest-common-subsequence edit plan.  The matrix is
/// deliberately capped by MAX-COSTS: beyond that limit we retain the common
/// head and tail and use one coarse middle replacement, matching GNU's
/// contract that MAX-COSTS may reduce diff quality without making the call
/// fail.  A time-limit breach is distinct and makes the caller report nil.
fn non_destructive_replacement_hunks(
    old: &[char],
    new: &[char],
    max_costs: usize,
    deadline: Option<Instant>,
) -> Option<Vec<BufferReplacementHunk>> {
    let timed_out = || deadline.is_some_and(|limit| Instant::now() >= limit);
    if timed_out() {
        return None;
    }
    let cells = old.len().checked_add(1).and_then(|rows| {
        new.len()
            .checked_add(1)
            .and_then(|columns| rows.checked_mul(columns))
    });
    if cells.is_none_or(|cells| cells > max_costs) {
        return Some(heuristic_replacement_hunks(old, new));
    }

    let columns = new.len() + 1;
    let mut lengths = vec![0_u32; cells.unwrap_or(0)];
    for old_index in (0..old.len()).rev() {
        if timed_out() {
            return None;
        }
        for new_index in (0..new.len()).rev() {
            let slot = old_index * columns + new_index;
            lengths[slot] = if old[old_index] == new[new_index] {
                lengths[(old_index + 1) * columns + new_index + 1] + 1
            } else {
                lengths[(old_index + 1) * columns + new_index]
                    .max(lengths[old_index * columns + new_index + 1])
            };
        }
    }

    let mut matches = Vec::new();
    let (mut old_index, mut new_index) = (0, 0);
    while old_index < old.len() && new_index < new.len() {
        if old[old_index] == new[new_index]
            && lengths[old_index * columns + new_index]
                == lengths[(old_index + 1) * columns + new_index + 1] + 1
        {
            matches.push((old_index, new_index));
            old_index += 1;
            new_index += 1;
        } else if lengths[(old_index + 1) * columns + new_index]
            >= lengths[old_index * columns + new_index + 1]
        {
            // Prefer deleting from the target on ties.  Besides making the
            // result deterministic, this retains the same earlier source
            // match GNU's diff does for ambiguous one-character matches.
            old_index += 1;
        } else {
            new_index += 1;
        }
    }
    Some(replacement_hunks_from_matches(
        old.len(),
        new.len(),
        &matches,
    ))
}

fn clipped_property_spans(
    spans: &[TextPropertySpan],
    from: usize,
    to: usize,
) -> Vec<TextPropertySpan> {
    spans
        .iter()
        .filter_map(|span| {
            let start = span.start.max(from);
            let end = span.end.min(to);
            (start < end).then(|| TextPropertySpan {
                start: start - from,
                end: end - from,
                props: span.props.clone(),
            })
        })
        .collect()
}

fn apply_buffer_replacement_hunks(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    target_start: usize,
    target_end: usize,
    source_chars: &[char],
    source_props: &[TextPropertySpan],
    hunks: &[BufferReplacementHunk],
) -> Result<(), LispError> {
    if hunks.is_empty() {
        return Ok(());
    }
    ensure_region_modifiable(interp, target_start, target_end, env)?;
    ensure_no_supersession_threat(interp, env)?;

    let old_len = target_end - target_start;
    let new_len = source_chars.len();
    let overlay_calls = overlay_change_hook_calls(
        &interp.buffer,
        target_start,
        target_end,
        target_start + new_len,
    );
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
    run_change_hooks(
        interp,
        "before-change-functions",
        &[
            Value::Integer(target_start as i64),
            Value::Integer(target_end as i64),
        ],
        env,
    )?;

    // GNU records the excursion before applying its diff.  A marker, rather
    // than a numeric point, is essential here: if a matching character near
    // point survives, point must continue to follow that character.
    let saved_point = interp.buffer.point();
    let saved_point_marker = match interp.make_marker() {
        Value::Marker(id) => id,
        _ => unreachable!("make_marker returns a marker"),
    };
    interp.set_marker(
        saved_point_marker,
        Some(saved_point),
        Some(interp.current_buffer_id()),
    )?;

    let restore_hooks = interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
    let edit_result: Result<(), LispError> = (|| {
        for hunk in hunks.iter().rev() {
            let from = target_start + hunk.old_start;
            let to = target_start + hunk.old_end;
            if from < to {
                interp
                    .delete_region_current_buffer(from, to)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
            if hunk.new_start < hunk.new_end {
                let inserted: String = source_chars[hunk.new_start..hunk.new_end].iter().collect();
                let inserted_len = hunk.new_end - hunk.new_start;
                interp.buffer.goto_char(from);
                interp.insert_current_buffer(&inserted);
                // `insert' can inherit edge properties.  `replace-buffer-contents'
                // grafts the source intervals instead, including property-free gaps.
                interp
                    .buffer
                    .set_text_properties(from, from + inserted_len, &[]);
                for span in clipped_property_spans(source_props, hunk.new_start, hunk.new_end) {
                    interp.buffer.set_text_properties(
                        from + span.start,
                        from + span.end,
                        &span.props,
                    );
                }
            }
        }
        Ok(())
    })();
    let restore_result = interp.restore_special_dynamic(restore_hooks, env);
    let restored_point = interp
        .marker_position(saved_point_marker)
        .unwrap_or(saved_point)
        .clamp(interp.buffer.point_min(), interp.buffer.point_max());
    interp.buffer.goto_char(restored_point);
    let _ = interp.set_marker(saved_point_marker, None, None);
    edit_result?;
    restore_result?;

    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(target_start as i64),
            Value::Integer((target_start + new_len) as i64),
            Value::Integer(old_len as i64),
        ],
        env,
    )?;
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
    let _ = maybe_lock_current_buffer_on_change(interp, env);
    Ok(())
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "match-beginning" | "match-end" => {
                need_args(name, args, 1)?;
                let index = args[0].as_integer()?;
                if index < 0 {
                    return Err(LispError::Signal("Args out of range".into()));
                }
                let match_data = interp.last_match_data.as_ref().ok_or_else(|| {
                    LispError::Signal("No match data, because no search succeeded".into())
                })?;
                let result = match_data
                    .get(index as usize)
                    .and_then(|entry| *entry)
                    .map(|(start, end)| {
                        if name == "match-beginning" {
                            Value::Integer(start as i64)
                        } else {
                            Value::Integer(end as i64)
                        }
                    })
                    .unwrap_or(Value::Nil);
                Ok(result)
            }
            "match-data" => {
                if args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if args.get(2).is_some_and(Value::is_truthy)
                    && let Some(reuse) = args.get(1)
                {
                    let mut tail = reuse.clone();
                    while let Value::Cons(cell) = tail {
                        let marker_id = match &*cell.car.borrow() {
                            Value::Marker(marker_id) => Some(*marker_id),
                            _ => None,
                        };
                        if let Some(marker_id) = marker_id {
                            interp.set_marker(marker_id, None, None)?;
                            *cell.car.borrow_mut() = Value::Nil;
                        }
                        tail = cell.cdr.borrow().clone();
                    }
                }
                let use_integers = args.first().is_some_and(Value::is_truthy);
                let source_buffer_id = if use_integers {
                    None
                } else {
                    interp.last_match_data_buffer_id
                };
                let mut items = Vec::new();
                let match_data = interp.last_match_data.clone().unwrap_or_default();
                let live_register_count = match_data
                    .iter()
                    .rposition(Option::is_some)
                    .map_or(0, |index| index + 1);
                for entry in match_data.into_iter().take(live_register_count) {
                    match entry {
                        Some((start, end)) => {
                            if let Some(buffer_id) = source_buffer_id {
                                let start_marker = interp.make_marker();
                                let Value::Marker(start_id) = start_marker else {
                                    unreachable!("make_marker always returns a marker")
                                };
                                let end_marker = interp.make_marker();
                                let Value::Marker(end_id) = end_marker else {
                                    unreachable!("make_marker always returns a marker")
                                };
                                interp.set_marker(start_id, Some(start), Some(buffer_id))?;
                                interp.set_marker(end_id, Some(end), Some(buffer_id))?;
                                items.push(start_marker);
                                items.push(end_marker);
                            } else {
                                items.push(Value::Integer(start as i64));
                                items.push(Value::Integer(end as i64));
                            }
                        }
                        None => {
                            items.push(Value::Nil);
                            items.push(Value::Nil);
                        }
                    }
                }
                if use_integers
                    && let Some(buffer_id) = interp.last_match_data_buffer_id
                    && let Some(buffer) = interp.buffer_identity_value(buffer_id)
                {
                    items.push(buffer);
                }
                let Some(reuse) = args.get(1).filter(|value| matches!(value, Value::Cons(_)))
                else {
                    return Ok(Value::list(items));
                };
                let mut tail = reuse.clone();
                let mut previous = None;
                let mut item_index = 0usize;
                while let Value::Cons(cell) = tail {
                    *cell.car.borrow_mut() = items.get(item_index).cloned().unwrap_or(Value::Nil);
                    item_index += 1;
                    previous = Some(Value::Cons(cell.clone()));
                    tail = cell.cdr.borrow().clone();
                }
                if item_index < items.len()
                    && let Some(previous) = previous
                {
                    previous.set_cdr(Value::list(items.into_iter().skip(item_index)))?;
                }
                Ok(reuse.clone())
            }
            "set-match-data" => {
                need_arg_range(name, args, 1, 2)?;
                if args[0].is_nil() {
                    // GNU clears every allocated search register but keeps
                    // the register state established.  `match-data' still
                    // returns nil, while `match-beginning'/'match-end' return
                    // nil rather than reporting that no search ever ran.
                    interp.last_match_data = Some(vec![None]);
                    interp.last_match_data_buffer_id = None;
                    return Ok(Value::Nil);
                }
                let items = args[0].to_vec()?;
                let mut restored = Vec::new();
                let mut restored_buffer_id = None;
                let mut index = 0usize;
                while index + 1 < items.len() {
                    if let Value::Buffer(buffer) = &items[index] {
                        restored_buffer_id = Some(buffer.id);
                        break;
                    }
                    for item in [&items[index], &items[index + 1]] {
                        if let Value::Marker(marker_id) = item
                            && restored_buffer_id.is_none()
                        {
                            restored_buffer_id = interp.marker_buffer_id(*marker_id);
                        }
                    }
                    let start = if items[index].is_nil() {
                        None
                    } else {
                        Some(position_from_value(interp, &items[index])?)
                    };
                    let end = if items[index + 1].is_nil() {
                        None
                    } else {
                        Some(position_from_value(interp, &items[index + 1])?)
                    };
                    restored.push(match (start, end) {
                        (Some(start), Some(end)) => Some((start, end)),
                        _ => None,
                    });
                    index += 2;
                }
                if let Some(Value::Buffer(buffer)) = items.get(index) {
                    restored_buffer_id = Some(buffer.id);
                }
                if args.get(1).is_some_and(Value::is_truthy) {
                    let mut tail = args[0].clone();
                    while let Value::Cons(cell) = tail {
                        let marker_id = match &*cell.car.borrow() {
                            Value::Marker(marker_id) => Some(*marker_id),
                            _ => None,
                        };
                        if let Some(marker_id) = marker_id {
                            interp.set_marker(marker_id, None, None)?;
                            *cell.car.borrow_mut() = Value::Nil;
                        }
                        tail = cell.cdr.borrow().clone();
                    }
                }
                interp.last_match_data = Some(restored);
                interp.last_match_data_buffer_id = restored_buffer_id;
                Ok(Value::Nil)
            }
            "match-data--translate" => {
                need_args(name, args, 1)?;
                let Value::Integer(delta) = args[0] else {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[0].clone()));
                };
                if let Some(match_data) = &mut interp.last_match_data {
                    for entry in match_data.iter_mut().flatten() {
                        entry.0 = (entry.0 as i64).saturating_add(delta).max(0) as usize;
                        entry.1 = (entry.1 as i64).saturating_add(delta).max(0) as usize;
                    }
                }
                Ok(Value::Nil)
            }
            "looking-at" | "posix-looking-at" => {
                need_arg_range(name, args, 1, 2)?;
                let pattern = string_text(&args[0])?;
                interp.set_variable(
                    "last-looking-at-pattern",
                    Value::String(pattern.clone().into()),
                    &mut env.clone(),
                );
                regexp::looking_at_impl(
                    interp,
                    &args[0],
                    name == "posix-looking-at",
                    !args.get(1).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "newline-cache-check" => {
                need_arg_range(name, args, 0, 1)?;
                if let Some(buffer) = args.first().filter(|value| !value.is_nil()) {
                    interp.resolve_buffer_id(buffer)?;
                }
                // Emaxx does not maintain GNU's optional long-line newline
                // cache.  GNU's documented result when no cache exists is nil.
                Ok(Value::Nil)
            }
            "re--describe-compiled" => {
                need_arg_range(name, args, 1, 2)?;
                let pattern = string_like(&args[0]).ok_or_else(|| {
                    LispError::SignalValue(Value::list([
                        Value::symbol("wrong-type-argument"),
                        Value::symbol("stringp"),
                        args[0].clone(),
                    ]))
                })?;
                regexp::compile_elisp_regex(interp, &pattern, env, "", true)?;
                // GNU exposes private bytecode from its own regexp engine.
                // `fancy-regex` deliberately keeps both its VM program and
                // delegated regex-automata state behind a private stable API.
                Err(LispError::Signal(
                    "Compiled regexp introspection is unavailable from the fancy-regex backend"
                        .into(),
                ))
            }

            "replace-match" => {
                need_args(name, args, 1)?;
                let replacement = string_text(&args[0])?;
                let fixedcase = args.get(1).is_some_and(Value::is_truthy);
                let literal = args.get(2).is_some_and(Value::is_truthy);
                let replace_index = args
                    .get(4)
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(0)
                    .max(0) as usize;
                let match_data = interp
                    .last_match_data
                    .clone()
                    .ok_or_else(|| LispError::Signal("No previous search".into()))?;
                let (start, end) = match_data
                    .get(replace_index)
                    .and_then(|entry| *entry)
                    .or_else(|| match_data.first().and_then(|entry| *entry))
                    .ok_or_else(|| LispError::Signal("No previous search".into()))?;
                if let Some(source) = args.get(3).filter(|value| !value.is_nil()) {
                    let source = string_like(source)
                        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), source.clone()))?;
                    let matched = regexp::slice_string_chars(&source.text, start, end);
                    let mut replacement = regexp::expand_replace_match_text(
                        &replacement,
                        &match_data,
                        literal,
                        &source.text,
                    )?;
                    if !fixedcase
                        && let Some(action) = replacement_case_action(interp, &matched, env)
                    {
                        replacement = casify_string(interp, &replacement, action, env)?;
                    }
                    let source_len = source.text.chars().count();
                    let updated = format!(
                        "{}{}{}",
                        regexp::slice_string_chars(&source.text, 0, start),
                        replacement,
                        regexp::slice_string_chars(&source.text, end, source_len)
                    );
                    // The STRING form is non-destructive and GNU returns
                    // before changing search_regs.  Later replacements must
                    // continue to see the original match and subexpressions.
                    return Ok(make_shared_string_value_with_multibyte(
                        updated,
                        Vec::new(),
                        source.multibyte,
                    ));
                }
                let matched = interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                // replace_range grafts NEWSTRING's text properties into
                // the buffer.  A backslash substitution rebuilds the text
                // through build_string and loses them, so the graft holds
                // exactly when the inserted text keeps NEWSTRING's length
                // (literal replacements, case conversion included).
                let source_spans = string_like(&args[0])
                    .map(|source| (source.text.chars().count(), source.props))
                    .unwrap_or((0, Vec::new()));
                let mut replacement =
                    regexp::expand_replace_match(interp, &replacement, &match_data, literal)?;
                if !fixedcase && let Some(action) = replacement_case_action(interp, &matched, env) {
                    replacement = casify_string(interp, &replacement, action, env)?;
                }
                let replacement_len = replacement.chars().count();
                let saved_markers =
                    interp.live_marker_positions_for_buffer(interp.current_buffer_id());
                let overlay_calls =
                    overlay_change_hook_calls(&interp.buffer, start, end, start + replacement_len);
                run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
                run_change_hooks(
                    interp,
                    "before-change-functions",
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                )?;
                interp
                    .delete_region_current_buffer(start, end)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                interp.buffer.goto_char(start);
                interp.insert_current_buffer(&replacement);
                let (source_len, spans) = source_spans;
                if !spans.is_empty() && source_len == replacement_len {
                    interp.apply_text_property_change_shared(&|buffer| {
                        for span in &spans {
                            buffer.set_text_properties(
                                start + span.start,
                                start + span.end,
                                &span.props,
                            );
                        }
                    });
                }
                let removed_len = end.saturating_sub(start);
                for (marker_id, original) in saved_markers {
                    let Some(original_pos) = original else {
                        continue;
                    };
                    let insertion_type = interp.marker_insertion_type(marker_id).unwrap_or(false);
                    let new_pos = if original_pos < start {
                        original_pos
                    } else if original_pos == start {
                        start
                    } else if original_pos < end {
                        if insertion_type {
                            start + replacement_len
                        } else {
                            start
                        }
                    } else {
                        ((original_pos as isize) + replacement_len as isize - removed_len as isize)
                            .max(start as isize) as usize
                    };
                    let _ = interp.set_marker(
                        marker_id,
                        Some(new_pos),
                        Some(interp.current_buffer_id()),
                    );
                }
                run_change_hooks(
                    interp,
                    "after-change-functions",
                    &[
                        Value::Integer(start as i64),
                        Value::Integer((start + replacement_len) as i64),
                        Value::Integer((end - start) as i64),
                    ],
                    env,
                )?;
                run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
                interp.last_match_data = Some(regexp::update_match_data_after_replace(
                    &match_data,
                    replace_index,
                    start,
                    end,
                    replacement_len,
                ));
                interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                Ok(Value::Nil)
            }

            "replace-buffer-contents" => {
                need_arg_range(name, args, 1, 3)?;
                let source_id = interp.resolve_buffer_id(&args[0])?;
                if source_id == interp.current_buffer_id() {
                    return Err(LispError::Signal(
                        "Cannot replace a buffer with itself".into(),
                    ));
                }
                let (source_text, source_props) = {
                    let source = interp.get_buffer_by_id(source_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {source_id}"))
                    })?;
                    (
                        source
                            .buffer_substring(source.point_min(), source.point_max())
                            .map_err(|error| LispError::Signal(error.to_string()))?,
                        source.substring_property_spans(source.point_min(), source.point_max()),
                    )
                };
                let target_start = interp.buffer.point_min();
                let target_end = interp.buffer.point_max();
                let target_text = interp
                    .buffer
                    .buffer_substring(target_start, target_end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let old_chars: Vec<char> = target_text.chars().collect();
                let new_chars: Vec<char> = source_text.chars().collect();

                let max_costs = match args.get(2) {
                    None | Some(Value::Nil) => 1_000_000,
                    Some(Value::Integer(value)) => (*value).max(0) as usize,
                    Some(Value::BigInteger(value)) => value.to_usize().unwrap_or_else(|| {
                        if value.sign() == Sign::Minus {
                            0
                        } else {
                            usize::MAX
                        }
                    }),
                    Some(value) => {
                        return Err(LispError::WrongTypeArgument("integerp".into(), value.clone()));
                    }
                };
                let deadline = match args.get(1) {
                    None | Some(Value::Nil) => None,
                    Some(value) => {
                        let seconds = numeric_to_f64(interp, value)?;
                        if seconds <= 0.0 {
                            Some(Instant::now())
                        } else if seconds.is_finite() {
                            Some(Instant::now() + Duration::from_secs_f64(seconds))
                        } else {
                            None
                        }
                    }
                };

                let (hunks, non_destructive) = match non_destructive_replacement_hunks(
                    &old_chars, &new_chars, max_costs, deadline,
                ) {
                    Some(hunks) => (hunks, true),
                    None => (
                        vec![BufferReplacementHunk {
                            old_start: 0,
                            old_end: old_chars.len(),
                            new_start: 0,
                            new_end: new_chars.len(),
                        }],
                        false,
                    ),
                };
                apply_buffer_replacement_hunks(
                    interp,
                    env,
                    target_start,
                    target_end,
                    &new_chars,
                    &source_props,
                    &hunks,
                )?;
                Ok(if non_destructive {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "subst-char-in-region" => {
                // (subst-char-in-region START END FROMCHAR TOCHAR &optional NOUNDO)
                need_arg_range(name, args, 4, 5)?;
                let from = position_from_value(interp, &args[0])?;
                let to = position_from_value(interp, &args[1])?;
                let old = args[2].as_integer()? as u32;
                let new = args[3].as_integer()? as u32;
                let old = char::from_u32(old)
                    .ok_or_else(|| LispError::Signal("Invalid character".into()))?;
                let new = char::from_u32(new)
                    .ok_or_else(|| LispError::Signal("Invalid character".into()))?;
                let text = interp
                    .buffer
                    .buffer_substring(from, to)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                let replaced: String = text
                    .chars()
                    .map(|ch| if ch == old { new } else { ch })
                    .collect();
                // GNU substitutes characters IN PLACE: text properties and
                // markers in the region are untouched (fill-region's
                // newline→space pass must keep erc-button's props intact).
                if replaced != text {
                    run_change_hooks(
                        interp,
                        "before-change-functions",
                        &[Value::Integer(from as i64), Value::Integer(to as i64)],
                        env,
                    )?;
                    let noundo = args.get(4).is_some_and(Value::is_truthy);
                    interp
                        .buffer
                        .replace_region_in_place(from, to, &replaced, noundo);
                    run_change_hooks(
                        interp,
                        "after-change-functions",
                        &[
                            Value::Integer(from as i64),
                            Value::Integer(to as i64),
                            Value::Integer((to - from) as i64),
                        ],
                        env,
                    )?;
                }
                Ok(Value::Nil)
            }

            "internal--labeled-narrow-to-region" => {
                need_args(name, args, 3)?;
                let mut start = position_from_value(interp, &args[0])?;
                let mut end = position_from_value(interp, &args[1])?;
                let label = args[2].clone();
                if start > end {
                    std::mem::swap(&mut start, &mut end);
                }
                let outermost = (interp.buffer.point_min(), interp.buffer.point_max());
                if let Some((clamp_start, clamp_end)) =
                    interp.effective_labeled_restriction(interp.current_buffer_id(), None)
                {
                    start = start.clamp(clamp_start, clamp_end);
                    end = end.clamp(clamp_start, clamp_end);
                }
                interp.push_labeled_restriction(
                    interp.current_buffer_id(),
                    label,
                    start,
                    end,
                    outermost,
                )?;
                interp.buffer.narrow_to_region(start, end);
                Ok(Value::Nil)
            }

            "internal--labeled-widen" => {
                need_args(name, args, 1)?;
                let label = &args[0];
                if let Some((start, end)) =
                    interp.pop_labeled_restriction(interp.current_buffer_id(), label)
                {
                    interp.buffer.narrow_to_region(start, end);
                } else {
                    interp.buffer.widen();
                }
                Ok(Value::Nil)
            }

            "transpose-regions" => {
                if args.len() < 4 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let mut start1 = position_from_value(interp, &args[0])?;
                let mut end1 = position_from_value(interp, &args[1])?;
                let mut start2 = position_from_value(interp, &args[2])?;
                let mut end2 = position_from_value(interp, &args[3])?;
                if start1 > end1 {
                    std::mem::swap(&mut start1, &mut end1);
                }
                if start2 > end2 {
                    std::mem::swap(&mut start2, &mut end2);
                }
                if start2 < end1 {
                    std::mem::swap(&mut start1, &mut start2);
                    std::mem::swap(&mut end1, &mut end2);
                }
                if start2 < end1 {
                    return Err(LispError::Signal("Transposed regions overlap".into()));
                }
                if (start1 == end1 || start2 == end2) && end1 == start2 {
                    return Ok(Value::Nil);
                }
                let leave_markers = args.get(4).is_some_and(|value| value.is_truthy());
                let saved_markers =
                    interp.live_marker_positions_for_buffer(interp.current_buffer_id());
                let region1_text = interp
                    .buffer
                    .buffer_substring(start1, end1)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                let region1_props = interp.buffer.substring_property_spans(start1, end1);
                let region2_text = interp
                    .buffer
                    .buffer_substring(start2, end2)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                let region2_props = interp.buffer.substring_property_spans(start2, end2);
                let gap = interp
                    .buffer
                    .buffer_substring(end1, start2)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                let gap_len = gap.chars().count();
                interp
                    .delete_region_current_buffer(start2, end2)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                interp
                    .delete_region_current_buffer(start1, end1)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                interp.buffer.goto_char(start1);
                interp.insert_current_buffer(&region2_text);
                for span in &region2_props {
                    interp.buffer.add_text_properties(
                        start1 + span.start,
                        start1 + span.end,
                        &span.props,
                    );
                }
                let insert_region1_at = start1 + region2_text.chars().count() + gap_len;
                interp.buffer.goto_char(insert_region1_at);
                interp.insert_current_buffer(&region1_text);
                for span in &region1_props {
                    interp.buffer.add_text_properties(
                        insert_region1_at + span.start,
                        insert_region1_at + span.end,
                        &span.props,
                    );
                }
                let len1 = end1 - start1;
                let len2 = end2 - start2;
                let diff = len2 as isize - len1 as isize;
                let amt1 = len2 + (start2 - end1);
                let amt2 = len1 + (start2 - end1);
                for (marker_id, original) in saved_markers {
                    let Some(original_pos) = original else {
                        continue;
                    };
                    let new_pos = if leave_markers || original_pos < start1 || original_pos >= end2
                    {
                        original_pos
                    } else if original_pos < end1 {
                        original_pos + amt1
                    } else if original_pos < start2 {
                        ((original_pos as isize) + diff) as usize
                    } else {
                        original_pos - amt2
                    };
                    let _ = interp.set_marker(
                        marker_id,
                        Some(new_pos),
                        Some(interp.current_buffer_id()),
                    );
                }
                Ok(Value::Nil)
            }

            "encode-coding-region" | "decode-coding-region" => {
                if args.len() < 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                let coding = checked_coding_name(interp, &args[2])?;
                let region = interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let region = make_shared_string_value_with_multibyte(
                    region,
                    Vec::new(),
                    interp.buffer.is_multibyte(),
                );
                enum Destination {
                    Replace,
                    Return,
                    Buffer(u64),
                }
                let destination = match args.get(3) {
                    None | Some(Value::Nil) => Destination::Replace,
                    Some(Value::T) => Destination::Return,
                    Some(buffer) => Destination::Buffer(interp.resolve_buffer_id(buffer)?),
                };
                let return_string = matches!(destination, Destination::Return);
                let transformed = if name == "encode-coding-region" {
                    encode_coding_value(interp, &region, coding.as_deref(), return_string, env)?
                } else {
                    decode_coding_text(interp, &region, coding.as_deref(), return_string, env)?
                };
                let transformed_text = string_text(&transformed)?;
                let transformed_length = transformed_text.chars().count();
                let text_for_buffer = |multibyte: bool| -> Result<String, LispError> {
                    if name == "decode-coding-region" && !multibyte {
                        Ok(decode_raw_text_bytes(&encode_utf8_bytes(
                            &transformed_text,
                            false,
                        )?))
                    } else {
                        Ok(transformed_text.clone())
                    }
                };
                match destination {
                    Destination::Return => Ok(transformed),
                    Destination::Replace => {
                        let text = text_for_buffer(interp.buffer.is_multibyte())?;
                        replace_buffer_region_with_text(interp, start, end, &text)?;
                        Ok(Value::Integer(transformed_length as i64))
                    }
                    Destination::Buffer(buffer_id) => {
                        let saved_buffer_id = interp.current_buffer_id();
                        interp.switch_to_buffer_id(buffer_id)?;
                        let insert_at = interp.buffer.point();
                        let text = text_for_buffer(interp.buffer.is_multibyte())?;
                        let insertion =
                            insert_text_with_hooks(interp, &text, &[], &[], false, false, env);
                        interp.buffer.goto_char(insert_at);
                        let restore = interp.switch_to_buffer_id(saved_buffer_id);
                        insertion?;
                        restore?;
                        Ok(Value::Integer(transformed_length as i64))
                    }
                }
            }

            "encode-coding-string" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let coding = checked_coding_name(interp, &args[1])?;
                let nocopy = args.get(2).is_some_and(Value::is_truthy);
                encode_coding_value(interp, &args[0], coding.as_deref(), nocopy, env)
            }

            "decode-coding-string" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let coding = checked_coding_name(interp, &args[1])?;
                let nocopy = args.get(2).is_some_and(Value::is_truthy);
                let decoded = decode_coding_text(interp, &args[0], coding.as_deref(), nocopy, env)?;
                if let Some(buffer) = args.get(3)
                    && !buffer.is_nil()
                {
                    let buffer_id = interp.resolve_buffer_id(buffer)?;
                    let saved_buffer_id = interp.current_buffer_id();
                    interp.switch_to_buffer_id(buffer_id)?;
                    let insert_at = interp.buffer.point();
                    let decoded_text = string_text(&decoded)?;
                    insert_text_with_hooks(interp, &decoded_text, &[], &[], false, false, env)?;
                    interp.buffer.goto_char(insert_at);
                    let _ = interp.switch_to_buffer_id(saved_buffer_id);
                }
                Ok(decoded)
            }
            "json-parse-string" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let options = json_parse_options(&args[1..])?;
                Ok(json::parse_value_source(interp, &args[0], &options, true)?.value)
            }
            "json-parse-buffer" => {
                let options = json_parse_options(args)?;
                let start = interp.buffer.point();
                let text = interp
                    .buffer
                    .buffer_substring(start, interp.buffer.point_max())
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let parsed = json::parse_text_source(
                    interp,
                    &text,
                    interp.buffer.is_multibyte(),
                    &options,
                    false,
                )?;
                interp
                    .buffer
                    .goto_char(start + parsed.consumed_source_pos.saturating_sub(1));
                Ok(parsed.value)
            }
            "json-serialize" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let (null_object, false_object) = json_serialize_options(&args[1..])?;
                Ok(json::serialize(interp, &args[0], &null_object, &false_object)?.bytes_value)
            }
            "json-insert" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let (null_object, false_object) = json_serialize_options(&args[1..])?;
                let serialized = json::serialize(interp, &args[0], &null_object, &false_object)?;
                let text = if interp.buffer.is_multibyte() {
                    &serialized.text
                } else {
                    &serialized.bytes_text
                };
                insert_text_with_hooks(interp, text, &[], &[], false, false, env)?;
                Ok(Value::Nil)
            }

            "insert-before-markers" => insert_impl(interp, args, env, false, true),
            "insert-before-markers-and-inherit" => insert_impl(interp, args, env, true, true),
        }
    }
);
