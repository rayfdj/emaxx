use super::*;
use crate::lisp::types::Kind;

fn replacement_case_action(interp: &Interpreter, matched: &str) -> Option<CaseAction> {
    let case_symbols_as_words = case_symbols_as_words_enabled(interp);
    let mut previous_is_word = false;
    let mut some_multiletter_word = false;
    let mut some_lowercase = false;
    let mut some_uppercase = false;
    let mut some_nonuppercase_initial = false;

    for character in matched.chars() {
        let code = character as u32;
        let (lowercase, uppercase) = current_case_classes(interp, code);
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

/// GNU's RE_NREGS: the registers a search allocates at least, so a
/// `\\N' or SUBEXP below it names a group whether or not the regexp had it.
const RE_NREGS: usize = 30;

fn invalid_backslash() -> LispError {
    LispError::Signal("Invalid use of `\\' in replacement text".into())
}

/// search.c Freplace_match.
fn replace_match(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let newtext = args[0];
    let Kind::StringObject(newtext_ref) = newtext.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), newtext));
    };
    let fixedcase = args.get(1).is_some_and(Value::is_truthy);
    let string = args.get(3).copied().filter(|value| !value.is_nil());
    let subexp = args.get(4).copied().filter(|value| !value.is_nil());
    let source = match string {
        Some(string) => match string.kind() {
            Kind::StringObject(source) => Some(source),
            _ => return Err(LispError::WrongTypeArgument("stringp".into(), string)),
        },
        None => None,
    };
    // Most replacement texts have no backslash directive: the fast path.
    let literal =
        args.get(2).is_some_and(Value::is_truthy) || !newtext_ref.borrow().bytes().contains(&b'\\');
    let Some(regs) = interp.last_match_data.clone() else {
        return Err(LispError::Signal(
            "`replace-match' called before any match found".into(),
        ));
    };
    let num_regs = regs.len().max(RE_NREGS);
    let sub = match subexp {
        None => 0,
        Some(value) => {
            let n = match value.kind() {
                Kind::Integer(n) => Some(n),
                Kind::BigInteger(_) => None,
                _ => return Err(LispError::WrongTypeArgument("integerp".into(), value)),
            };
            match n {
                Some(n) if (0..num_regs as i64).contains(&n) => n as usize,
                _ => {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        value,
                        Value::Integer(0),
                        Value::Integer(num_regs as i64 - 1),
                    ])));
                }
            }
        }
    };
    let group = |index: usize| regs.get(index).copied().flatten();
    let in_bounds = |(start, end): (usize, usize)| match source {
        None => {
            let buffer = interp.buffer.borrow();
            buffer.point_min() <= start && end <= buffer.point_max()
        }
        Some(source) => end <= source.borrow().len(),
    };
    let (sub_start, sub_end) = match group(sub) {
        Some(range) if in_bounds(range) => range,
        Some((start, end)) => {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::Integer(start as i64),
                Value::Integer(end as i64),
            ])));
        }
        None => {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::string("replace-match subexpression does not exist"),
                subexp.unwrap_or(Value::Nil),
            ])));
        }
    };
    // How to casify, from the matched text.
    let case_action = if fixedcase {
        None
    } else {
        let matched = match source {
            Some(source) => regexp::slice_string_chars(&source.borrow().text(), sub_start, sub_end),
            None => interp
                .buffer
                .borrow()
                .buffer_substring(sub_start, sub_end)
                .map_err(|error| LispError::Signal(error.to_string()))?,
        };
        replacement_case_action(interp, &matched)
    };

    if let Some(source) = source {
        let substring = |object: &crate::lisp::types::StringObjectRef, from: usize, to: usize| {
            Value::StringObject(object.borrow().substring(from, to, true))
        };
        let source_len = source.borrow().len();
        let before = substring(&source, 0, sub_start);
        let after = substring(&source, sub_end, source_len);
        let mut newtext = newtext;
        if !literal {
            // Substitute parts of the match into NEWTEXT, piece by piece
            // with their properties, as concat3 accumulates them.
            let codes = string_like(&newtext)
                .expect("newtext is a string")
                .character_codes();
            let mut pieces = Vec::new();
            let mut lastpos = 0;
            let mut pos = 0;
            while pos < codes.len() {
                let c = codes[pos];
                pos += 1;
                let mut substitution = None;
                let mut delbackslash = false;
                if c == i64::from(b'\\') {
                    let Some(&c) = codes.get(pos) else {
                        return Err(invalid_backslash());
                    };
                    pos += 1;
                    match u8::try_from(c).map(char::from) {
                        Ok('&') => substitution = Some((sub_start, sub_end)),
                        Ok(digit @ '1'..='9') => {
                            let index = digit as usize - '0' as usize;
                            substitution = Some(if index < num_regs {
                                group(index).unwrap_or((0, 0))
                            } else {
                                (0, 0)
                            });
                        }
                        Ok('\\') => delbackslash = true,
                        Ok('?') => {}
                        _ => return Err(invalid_backslash()),
                    }
                }
                if let Some((start, end)) = substitution {
                    if pos - 2 != lastpos {
                        pieces.push(substring(&newtext_ref, lastpos, pos - 2));
                    }
                    pieces.push(substring(&source, start, end));
                    lastpos = pos;
                } else if delbackslash {
                    pieces.push(substring(&newtext_ref, lastpos, pos - 1));
                    lastpos = pos;
                }
            }
            if pos != lastpos {
                pieces.push(substring(&newtext_ref, lastpos, pos));
            }
            newtext = super::call(interp, "concat", &pieces, env)?;
        }
        if let Some(action) = case_action {
            newtext = casify_value(interp, &newtext, action, env)?;
        }
        return super::call(interp, "concat", &[before, newtext, after], env);
    }

    // The replacement text, in the buffer's multibyteness when NEWTEXT's
    // directives rebuild it (and then without properties).
    let buf_multibyte = interp.buffer.borrow().is_multibyte();
    let newtext_parts = string_like(&newtext).expect("newtext is a string");
    let (text, extended, props) = if literal {
        (
            newtext_parts.text,
            newtext_parts.extended_chars,
            newtext_parts.props,
        )
    } else {
        let str_multibyte = newtext_parts.multibyte;
        let codes = newtext_parts.character_codes();
        let to_buffer = |c: u32, after_backslash: bool| -> u32 {
            if str_multibyte {
                if !buf_multibyte && !(after_backslash && c < 0x80) {
                    // CHAR_TO_BYTE8.
                    if (RAW_BYTE8_BASE + 0x80..=RAW_BYTE8_BASE + 0xFF).contains(&c) {
                        c - RAW_BYTE8_BASE
                    } else {
                        c & 0xFF
                    }
                } else {
                    c
                }
            } else if buf_multibyte && c >= 0x80 {
                // make_char_multibyte.
                RAW_BYTE8_BASE + c
            } else {
                c
            }
        };
        let mut substed = Vec::with_capacity(codes.len());
        let mut really_changed = false;
        let mut pos = 0;
        while pos < codes.len() {
            let c = to_buffer(codes[pos] as u32, false);
            pos += 1;
            if c != u32::from(b'\\') {
                // CHAR_STRING's bytes: in a unibyte buffer a non-ASCII
                // character adds its multibyte form's bytes.
                if buf_multibyte || c < 0x80 {
                    substed.push(c);
                } else {
                    let (bytes, len) = crate::lisp::types::string_data::encode_character(c)?;
                    substed.extend(bytes[..len].iter().map(|&byte| u32::from(byte)));
                }
                continue;
            }
            really_changed = true;
            let Some(&next) = codes.get(pos) else {
                return Err(invalid_backslash());
            };
            pos += 1;
            let c = to_buffer(next as u32, true);
            let index = match char::from_u32(c) {
                Some('&') => Some(sub),
                Some(digit @ '1'..='9') if (digit as usize - '0' as usize) < num_regs => {
                    let index = digit as usize - '0' as usize;
                    group(index).is_some().then_some(index)
                }
                Some('\\') => {
                    substed.push(c);
                    None
                }
                _ => return Err(invalid_backslash()),
            };
            if let Some((start, end)) = index.and_then(group) {
                substed.extend(buffer_region_codes(interp, start, end));
            }
        }
        if really_changed {
            let (text, extended) = text_from_codes(&substed, buf_multibyte);
            (text, extended, Vec::new())
        } else {
            (
                newtext_parts.text,
                newtext_parts.extended_chars,
                newtext_parts.props,
            )
        }
    };
    let inschars = text.chars().count();
    let newpoint = sub_start + inschars;
    let num_regs_before = regs.len();
    // replace_range (prepare, no inherit, markers, adjusting the match
    // data, its own change hooks inhibited); the case conversion follows,
    // a change of its own, then the replacement's signal_after_change.
    let length = sub_end - sub_start;
    modify_current_buffer(
        interp,
        sub_start,
        sub_end,
        true,
        env,
        |interp, from, env| {
            let to = (from + length)
                .min(interp.buffer.borrow().point_max())
                .max(from);
            let from = interp.replace_range_current_buffer(from, to, &text, &extended);
            if !props.is_empty() {
                interp.apply_text_property_change_shared(&|buffer| {
                    for span in &props {
                        buffer.graft_text_properties(
                            from + span.start,
                            from + span.end,
                            &span.props,
                        );
                    }
                });
            }
            if let Some(match_data) = interp.last_match_data.as_mut() {
                regexp::update_search_regs(match_data, from, to, from + inschars);
            }
            if let Some(action) = case_action {
                let start = interp
                    .last_match_data
                    .as_ref()
                    .and_then(|match_data| match_data.get(sub).copied().flatten())
                    .map_or(from, |(start, _)| start);
                casify_buffer_region(interp, start, newpoint, action, env)?;
            }
            if interp
                .last_match_data
                .as_ref()
                .is_none_or(|match_data| match_data.len() != num_regs_before)
            {
                return Err(LispError::Signal(
                    "Match data clobbered by buffer modification hooks".into(),
                ));
            }
            let point = {
                let buffer = interp.buffer.borrow();
                newpoint.clamp(buffer.point_min(), buffer.point_max())
            };
            interp.buffer.borrow_mut().goto_char(point);
            Ok((Some(AfterChange::new(sub_start, length, inschars)), ()))
        },
    )?;
    Ok(Value::Nil)
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
    let old_len = target_end - target_start;
    let new_len = source_chars.len();
    // editfns.c:Freplace_buffer_contents announces one change of the whole
    // region and applies the differences with the hooks inhibited.
    modify_current_buffer(
        interp,
        target_start,
        target_end,
        false,
        env,
        |interp, _, env| {
            // GNU records the excursion before applying its diff.  A marker, rather
            // than a numeric point, is essential here: if a matching character near
            // point survives, point must continue to follow that character.
            let saved_point = interp.buffer.borrow().point();
            let saved_point_marker = match interp.make_marker().kind() {
                Kind::Marker(id) => id,
                _ => unreachable!("make_marker returns a marker"),
            };
            interp.set_marker(
                saved_point_marker,
                Some(saved_point),
                Some(interp.current_buffer_id()),
            )?;

            let restore_hooks =
                interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
            // The saved point is a marker held here, in no Lisp object, across
            // the edits: a root while they run.
            let held: &[Value] = &[Value::Marker(saved_point_marker)];
            let edit_result: Result<(), LispError> =
                interp.with_lisp_stack_roots(&held, |interp| {
                    for hunk in hunks.iter().rev() {
                        let from = target_start + hunk.old_start;
                        let to = target_start + hunk.old_end;
                        if from < to {
                            interp
                                .delete_region_current_buffer_unreturned(from, to)
                                .map_err(|error| LispError::Signal(error.to_string()))?;
                        }
                        if hunk.new_start < hunk.new_end {
                            let inserted: String =
                                source_chars[hunk.new_start..hunk.new_end].iter().collect();
                            let inserted_len = hunk.new_end - hunk.new_start;
                            interp.buffer.borrow_mut().goto_char(from);
                            interp.insert_current_buffer(&inserted);
                            // `insert' can inherit edge properties.  `replace-buffer-contents'
                            // grafts the source intervals instead, including property-free gaps.
                            interp.buffer.borrow_mut().graft_text_properties(
                                from,
                                from + inserted_len,
                                &[],
                            );
                            for span in
                                clipped_property_spans(source_props, hunk.new_start, hunk.new_end)
                            {
                                interp.buffer.borrow_mut().graft_text_properties(
                                    from + span.start,
                                    from + span.end,
                                    &span.props,
                                );
                            }
                        }
                    }
                    Ok(())
                });
            let restore_result = interp.restore_special_dynamic(restore_hooks, env);
            let restored_point = interp
                .marker_position(saved_point_marker)
                .unwrap_or(saved_point)
                .clamp(
                    interp.buffer.borrow().point_min(),
                    interp.buffer.borrow().point_max(),
                );
            interp.buffer.borrow_mut().goto_char(restored_point);
            let _ = interp.set_marker(saved_point_marker, None, None);
            edit_result?;
            restore_result?;
            Ok((Some(AfterChange::new(target_start, old_len, new_len)), ()))
        },
    )
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "match-beginning" | "match-end" => match_bound_named(interp, name, args, env),
            "match-data" => {
                if args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if args.get(2).is_some_and(Value::is_truthy)
                    && let Some(reuse) = args.get(1)
                {
                    let mut tail = *reuse;
                    while let Kind::Cons(cell) = tail.kind() {
                        let marker_id = match cell.car.get().kind() {
                            Kind::Marker(marker_id) => Some(marker_id),
                            _ => None,
                        };
                        if let Some(marker_id) = marker_id {
                            interp.set_marker(marker_id, None, None)?;
                            cell.car.set(Value::Nil);
                        }
                        tail = cell.cdr.get();
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
                                let Kind::Marker(start_id) = start_marker.kind() else {
                                    unreachable!("make_marker always returns a marker")
                                };
                                let end_marker = interp.make_marker();
                                let Kind::Marker(end_id) = end_marker.kind() else {
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
                let Some(reuse) = args
                    .get(1)
                    .filter(|value| matches!(value.kind(), Kind::Cons(_)))
                else {
                    return Ok(Value::list(items));
                };
                let mut tail = *reuse;
                let mut previous = None;
                let mut item_index = 0usize;
                while let Kind::Cons(cell) = tail.kind() {
                    cell.car
                        .set(items.get(item_index).cloned().unwrap_or(Value::Nil));
                    item_index += 1;
                    previous = Some(Value::Cons(cell));
                    tail = cell.cdr.get();
                }
                if item_index < items.len()
                    && let Some(previous) = previous
                {
                    previous.set_cdr(Value::list(items.into_iter().skip(item_index)))?;
                }
                Ok(*reuse)
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
                // search.c:Fset_match_data coerces detached markers to zero;
                // ordinary buffer-position coercion correctly rejects them.
                let match_position = |value: &Value| match value.kind() {
                    Kind::Marker(marker) => Ok(marker.position().unwrap_or(0)),
                    _ => position_from_value(interp, value),
                };
                let mut index = 0usize;
                while index + 1 < items.len() {
                    if let Kind::Buffer(buffer) = items[index].kind() {
                        restored_buffer_id = Some(buffer.id);
                        break;
                    }
                    for item in [&items[index], &items[index + 1]] {
                        if let Kind::Marker(marker_id) = item.kind()
                            && restored_buffer_id.is_none()
                        {
                            restored_buffer_id = interp.marker_buffer_id(marker_id);
                        }
                    }
                    let start = if items[index].is_nil() {
                        None
                    } else {
                        Some(match_position(&items[index])?)
                    };
                    let end = if items[index + 1].is_nil() {
                        None
                    } else {
                        Some(match_position(&items[index + 1])?)
                    };
                    restored.push(match (start, end) {
                        (Some(start), Some(end)) => Some((start, end)),
                        _ => None,
                    });
                    index += 2;
                }
                if let Some(Kind::Buffer(buffer)) = items.get(index).map(|v| v.kind()) {
                    restored_buffer_id = Some(buffer.id);
                }
                if args.get(1).is_some_and(Value::is_truthy) {
                    let mut tail = args[0];
                    while let Kind::Cons(cell) = tail.kind() {
                        let marker_id = match cell.car.get().kind() {
                            Kind::Marker(marker_id) => Some(marker_id),
                            _ => None,
                        };
                        if let Some(marker_id) = marker_id {
                            interp.set_marker(marker_id, None, None)?;
                            cell.car.set(Value::Nil);
                        }
                        tail = cell.cdr.get();
                    }
                }
                interp.last_match_data = Some(restored);
                interp.last_match_data_buffer_id = restored_buffer_id;
                Ok(Value::Nil)
            }
            "match-data--translate" => {
                need_args(name, args, 1)?;
                let Kind::Integer(delta) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                };
                if let Some(match_data) = &mut interp.last_match_data {
                    for entry in match_data.iter_mut().flatten() {
                        entry.0 = (entry.0 as i64).saturating_add(delta).max(0) as usize;
                        entry.1 = (entry.1 as i64).saturating_add(delta).max(0) as usize;
                    }
                }
                Ok(Value::Nil)
            }
            "looking-at" | "posix-looking-at" => looking_at_named(interp, name, args, env),
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
                let raw = args.get(1).is_some_and(Value::is_truthy);
                search::describe_compiled(interp, env, &args[0], raw)
            }

            "replace-match" => {
                need_arg_range(name, args, 1, 5)?;
                replace_match(interp, args, env)
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
                let target_start = interp.buffer.borrow().point_min();
                let target_end = interp.buffer.borrow().point_max();
                let target_text = interp
                    .buffer
                    .borrow()
                    .buffer_substring(target_start, target_end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let old_chars: Vec<char> = target_text.chars().collect();
                let new_chars: Vec<char> = source_text.chars().collect();

                let max_costs = match args.get(2).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => 1_000_000,
                    Some(Kind::Integer(value)) => (value).max(0) as usize,
                    Some(Kind::BigInteger(value)) => value.to_usize().unwrap_or_else(|| {
                        if value.sign() == Sign::Minus {
                            0
                        } else {
                            usize::MAX
                        }
                    }),
                    Some(value) => {
                        return Err(LispError::WrongTypeArgument(
                            "integerp".into(),
                            value.value(),
                        ));
                    }
                };
                let deadline = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => None,
                    Some(value) => {
                        let seconds = numeric_to_f64(interp, &value.value())?;
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
                let (from, to) = validate_region(interp, &args[0], &args[1])?;
                let character = |value: Value| match value.kind() {
                    Kind::Integer(code) if (0..=MAX_CHAR as i64).contains(&code) => Ok(code as u32),
                    _ => Err(LispError::WrongTypeArgument("characterp".into(), value)),
                };
                let fromc = character(args[2])?;
                let toc = character(args[3])?;
                // editfns.c: in a multibyte buffer the two characters must
                // be as long; in a unibyte one each stands for its byte.
                let multibyte = interp.buffer.borrow().is_multibyte();
                let (fromc, toc) = if multibyte {
                    if emacs_multibyte_char_len(fromc)? != emacs_multibyte_char_len(toc)? {
                        return Err(LispError::Signal(
                            "Characters in `subst-char-in-region' have different byte-lengths"
                                .into(),
                        ));
                    }
                    (fromc, toc)
                } else {
                    (fromc & 0xFF, toc & 0xFF)
                };
                // modify_text from the first FROMCHAR to END, then a
                // signal of the first to the last substituted.
                let Some(first) = buffer_region_codes(interp, from, to)
                    .iter()
                    .position(|&code| code == fromc)
                else {
                    return Ok(Value::Nil);
                };
                let changed = from + first;
                let noundo = args.get(4).is_some_and(Value::is_truthy);
                modify_current_buffer(interp, changed, to, false, env, |interp, _, _| {
                    let to = to.min(interp.buffer.borrow().point_max()).max(changed);
                    let mut codes = buffer_region_codes(interp, changed, to);
                    let Some(last) = codes.iter().rposition(|&code| code == fromc) else {
                        return Ok((None, ()));
                    };
                    codes.truncate(last + 1);
                    let mut buffer = interp.buffer.borrow_mut();
                    // modify_text at the first change, from it to the
                    // region's end, then record_change for each character
                    // replaced (none under NOUNDO).
                    if noundo {
                        buffer.modify_text_unrecorded(changed, to);
                    } else {
                        buffer.modify_text(changed, to);
                    }
                    for (offset, code) in codes.iter_mut().enumerate() {
                        if *code == fromc {
                            if !noundo {
                                buffer.record_change(changed + offset, 1);
                            }
                            *code = toc;
                        }
                    }
                    let last_changed = changed + last + 1;
                    let (replaced, extended) = text_from_codes(&codes, multibyte);
                    // GNU substitutes characters IN PLACE: text properties
                    // and markers in the region are untouched (fill-region's
                    // newline→space pass must keep erc-button's props intact).
                    buffer.replace_text_in_place(changed, last_changed, &replaced, &extended);
                    drop(buffer);
                    let length = last_changed - changed;
                    Ok((Some(AfterChange::new(changed, length, length)), ()))
                })?;
                Ok(Value::Nil)
            }

            "internal--labeled-narrow-to-region" => {
                need_args(name, args, 3)?;
                let mut start = position_from_value(interp, &args[0])?;
                let mut end = position_from_value(interp, &args[1])?;
                let label = args[2];
                if start > end {
                    std::mem::swap(&mut start, &mut end);
                }
                let outermost = (
                    interp.buffer.borrow().point_min(),
                    interp.buffer.borrow().point_max(),
                );
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
                interp.buffer.borrow_mut().narrow_to_region(start, end);
                interp.invalidate_current_column();
                Ok(Value::Nil)
            }

            "internal--labeled-widen" => {
                need_args(name, args, 1)?;
                let label = &args[0];
                if let Some((start, end)) =
                    interp.pop_labeled_restriction(interp.current_buffer_id(), label)
                {
                    interp.buffer.borrow_mut().narrow_to_region(start, end);
                } else {
                    interp.buffer.borrow_mut().widen();
                }
                interp.invalidate_current_column();
                Ok(Value::Nil)
            }

            "transpose-regions" => {
                if args.len() < 4 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // editfns.c:Ftranspose_regions validates each region.
                let (mut start1, mut end1) = validate_region(interp, &args[0], &args[1])?;
                let (mut start2, mut end2) = validate_region(interp, &args[2], &args[3])?;
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
                // editfns.c: modify_text over both regions and what lies
                // between, then one change of it all.
                modify_current_buffer(interp, start1, end2, false, env, |interp, _, _| {
                    let leave_markers = args.get(4).is_some_and(|value| value.is_truthy());
                    let saved_markers =
                        interp.live_marker_positions_for_buffer(interp.current_buffer_id());
                    let region1_text = interp
                        .buffer
                        .borrow()
                        .buffer_substring(start1, end1)
                        .map_err(|e| LispError::Signal(e.to_string()))?;
                    let region1_props = interp
                        .buffer
                        .borrow()
                        .substring_property_spans(start1, end1);
                    let region1_extended = interp
                        .buffer
                        .borrow()
                        .substring_extended_chars(start1, end1);
                    let region2_text = interp
                        .buffer
                        .borrow()
                        .buffer_substring(start2, end2)
                        .map_err(|e| LispError::Signal(e.to_string()))?;
                    let region2_props = interp
                        .buffer
                        .borrow()
                        .substring_property_spans(start2, end2);
                    let region2_extended = interp
                        .buffer
                        .borrow()
                        .substring_extended_chars(start2, end2);
                    let gap = interp
                        .buffer
                        .borrow()
                        .buffer_substring(end1, start2)
                        .map_err(|e| LispError::Signal(e.to_string()))?;
                    let gap_len = gap.chars().count();
                    interp
                        .delete_region_current_buffer_unreturned(start2, end2)
                        .map_err(|e| LispError::Signal(e.to_string()))?;
                    interp
                        .delete_region_current_buffer_unreturned(start1, end1)
                        .map_err(|e| LispError::Signal(e.to_string()))?;
                    interp.buffer.borrow_mut().goto_char(start1);
                    interp.insert_current_buffer(&region2_text);
                    interp.set_inserted_extended_chars(start1, &region2_extended);
                    for span in &region2_props {
                        interp.buffer.borrow_mut().graft_text_properties(
                            start1 + span.start,
                            start1 + span.end,
                            &span.props,
                        );
                    }
                    let insert_region1_at = start1 + region2_text.chars().count() + gap_len;
                    interp.buffer.borrow_mut().goto_char(insert_region1_at);
                    interp.insert_current_buffer(&region1_text);
                    interp.set_inserted_extended_chars(insert_region1_at, &region1_extended);
                    for span in &region1_props {
                        interp.buffer.borrow_mut().graft_text_properties(
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
                        let new_pos =
                            if leave_markers || original_pos < start1 || original_pos >= end2 {
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
                    Ok((
                        Some(AfterChange::new(start1, end2 - start1, end2 - start1)),
                        (),
                    ))
                })?;
                Ok(Value::Nil)
            }

            "encode-coding-region" | "decode-coding-region" => {
                if args.len() < 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // GNU records the coding under the symbol the caller gave
                // (the euc-jp alias stays `euc-jp'); canonicalization is the
                // codec's business, not last-coding-system-used's.
                let coding = checked_coding_name(interp, &args[2])?
                    .map(|_| args[2].as_symbol().expect("validated symbol").to_string());
                enum Destination {
                    Replace,
                    Return,
                    Buffer(u64),
                }
                // coding.c:code_convert_region: CHECK_BUFFER of a
                // destination other than nil and t, then validate_region.
                let destination = match args.get(3).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => Destination::Replace,
                    Some(Kind::T) => Destination::Return,
                    Some(Kind::Buffer(_)) => {
                        Destination::Buffer(interp.resolve_buffer_id(&args[3])?)
                    }
                    Some(_) => return Err(LispError::WrongTypeArgument("bufferp".into(), args[3])),
                };
                let (start, end) = validate_region(interp, &args[0], &args[1])?;
                // The region as encode_coding_object and decode_coding_object
                // read it: its characters (those beyond Unicode included)
                // and, for encoding, the `charset' properties the ISO-2022
                // encoder takes as annotations.
                let region = {
                    let buffer = interp.buffer.borrow();
                    let text = buffer
                        .buffer_substring(start, end)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                    let props = if name == "encode-coding-region" {
                        buffer.substring_property_spans(start, end)
                    } else {
                        Vec::new()
                    };
                    make_shared_string_value_with_extended_chars(
                        text,
                        props,
                        buffer.is_multibyte(),
                        buffer.substring_extended_chars(start, end),
                    )
                };
                let return_string = matches!(destination, Destination::Return);
                let transformed = if name == "encode-coding-region" {
                    encode_coding_value(interp, &region, coding.as_deref(), return_string, env)?
                } else {
                    // decode_coding_object decodes into the destination
                    // buffer as it is, multibyte or not; a string result
                    // follows the coding.
                    let dst_multibyte = match destination {
                        Destination::Replace => Some(interp.buffer.borrow().is_multibyte()),
                        Destination::Return => None,
                        Destination::Buffer(id) => interp
                            .get_buffer_by_id(id)
                            .map(|buffer| buffer.is_multibyte()),
                    };
                    decode_coding_text(
                        interp,
                        &region,
                        coding.as_deref(),
                        return_string,
                        true,
                        dst_multibyte,
                        env,
                    )?
                };
                let transformed_string = string_like(&transformed)
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), transformed))?;
                let transformed_length = transformed_string.text.chars().count();
                // produce_charset's `charset' properties land in the
                // destination buffer along with the decoded text.
                let transformed_props = transformed_string.props.clone();
                // The text (with its characters beyond Unicode) the
                // destination stores.
                let text_for_buffer =
                    |multibyte: bool| -> Result<(String, Vec<(usize, u32)>), LispError> {
                        let form = if name == "decode-coding-region"
                            && !multibyte
                            && transformed_string.multibyte
                        {
                            unibyte_destination_form(&transformed_string)?
                        } else {
                            transformed_string.clone()
                        };
                        Ok((form.text, form.extended_chars))
                    };
                match destination {
                    Destination::Return => Ok(transformed),
                    Destination::Replace => {
                        let (text, extended) =
                            text_for_buffer(interp.buffer.borrow().is_multibyte())?;
                        let old_length = end - start;
                        let new_end = start + text.chars().count();
                        modify_current_buffer(interp, start, end, false, env, |interp, _, _| {
                            replace_buffer_region_with_text(interp, start, end, &text, &extended)?;
                            // decode_coding counts produced characters even
                            // into a unibyte destination, so the spans keep
                            // their character offsets there.
                            for span in &transformed_props {
                                interp.buffer.borrow_mut().add_text_properties(
                                    start + span.start,
                                    start + span.end,
                                    &span.props,
                                );
                            }
                            Ok((
                                Some(AfterChange::new(start, old_length, new_end - start)),
                                Value::Integer(transformed_length as i64),
                            ))
                        })
                    }
                    Destination::Buffer(buffer_id) => {
                        let saved_buffer_id = interp.current_buffer_id();
                        interp.switch_to_buffer_id(buffer_id)?;
                        let insert_at = interp.buffer.borrow().point();
                        let (text, extended) =
                            text_for_buffer(interp.buffer.borrow().is_multibyte())?;
                        let insertion = insert_text_with_hooks(
                            interp,
                            &text,
                            &transformed_props,
                            &extended,
                            false,
                            false,
                            env,
                        );
                        interp.buffer.borrow_mut().goto_char(insert_at);
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
                // coding.c:code_convert_string: CHECK_STRING, then the
                // coding system, then a destination that is no t a buffer.
                if !args[0].is_string() {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                }
                // Verbatim for the same reason as the region ops above.
                let coding = checked_coding_name(interp, &args[1])?
                    .map(|_| args[1].as_symbol().expect("validated symbol").to_string());
                let nocopy = args.get(2).is_some_and(Value::is_truthy);
                let Some(buffer_id) = code_convert_destination(interp, args.get(3))? else {
                    return encode_coding_value(interp, &args[0], coding.as_deref(), nocopy, env);
                };
                // A nil coding system with a buffer destination encodes
                // with no-conversion.
                let encoded = encode_coding_value(
                    interp,
                    &args[0],
                    Some(coding.as_deref().unwrap_or("no-conversion")),
                    false,
                    env,
                )?;
                insert_converted_text(interp, buffer_id, &encoded, env)
            }

            "decode-coding-string" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if !args[0].is_string() {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                }
                let coding = checked_coding_name(interp, &args[1])?
                    .map(|_| args[1].as_symbol().expect("validated symbol").to_string());
                let nocopy = args.get(2).is_some_and(Value::is_truthy);
                let destination = code_convert_destination(interp, args.get(3))?;
                // code_convert_string's ASCII shortcut applies only when
                // returning a string, never when inserting into a buffer.
                let dst_multibyte = destination.and_then(|id| {
                    interp
                        .get_buffer_by_id(id)
                        .map(|buffer| buffer.is_multibyte())
                });
                let decoded = decode_coding_text(
                    interp,
                    &args[0],
                    coding.as_deref(),
                    nocopy,
                    destination.is_some(),
                    dst_multibyte,
                    env,
                )?;
                match destination {
                    Some(buffer_id) => insert_converted_text(interp, buffer_id, &decoded, env),
                    None => Ok(decoded),
                }
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
                let start = interp.buffer.borrow().point();
                let text = interp
                    .buffer
                    .borrow()
                    .buffer_substring(start, interp.buffer.borrow().point_max())
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let multibyte = interp.buffer.borrow().is_multibyte();
                let parsed = json::parse_text_source(interp, &text, multibyte, &options, false)?;
                interp
                    .buffer
                    .borrow_mut()
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
                // json.c Fjson_insert copies the UTF-8 bytes into the gap: a
                // multibyte buffer reads them as its own characters, a real
                // U+E000..U+E0FF included.
                let (text, extended) = if interp.buffer.borrow().is_multibyte() {
                    let extended: Vec<(usize, u32)> = serialized
                        .text
                        .chars()
                        .enumerate()
                        .filter(|(_, ch)| raw_byte_from_regex_char(*ch).is_some())
                        .map(|(index, ch)| (index, ch as u32))
                        .collect();
                    (&serialized.text, extended)
                } else {
                    (&serialized.bytes_text, Vec::new())
                };
                insert_text_with_hooks(interp, text, &[], &extended, false, false, env)?;
                Ok(Value::Nil)
            }

            "insert-before-markers" => insert_impl(interp, args, env, false, true),
            "insert-before-markers-and-inherit" => insert_impl(interp, args, env, true, true),
        }
    }
);

/// The `looking-at' family under NAME: one body, each name a subr's
/// function pointer below.
fn looking_at_named(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_arg_range(name, args, 1, 2)?;
    regexp::looking_at_impl(
        interp,
        &args[0],
        name == "posix-looking-at",
        !args.get(1).is_some_and(Value::is_truthy),
        env,
    )
}

/// The `looking-at' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_looking_at(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    looking_at_named(interp, "looking-at", args, env)
}

/// The `posix-looking-at' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_posix_looking_at(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    looking_at_named(interp, "posix-looking-at", args, env)
}

/// The `match-beginning' family under NAME: one body, each name a subr's
/// function pointer below.
fn match_bound_named(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_args(name, args, 1)?;
    let index = args[0].as_integer()?;
    if index < 0 {
        return Err(LispError::Signal("Args out of range".into()));
    }
    let match_data = interp
        .last_match_data
        .as_ref()
        .ok_or_else(|| LispError::Signal("No match data, because no search succeeded".into()))?;
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

/// The `match-beginning' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_match_beginning(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match_bound_named(interp, "match-beginning", args, env)
}

/// The `match-end' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_match_end(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match_bound_named(interp, "match-end", args, env)
}

/// code_convert_string's DST_OBJECT: nil or t returns a string, anything
/// else must be a buffer to insert into.
fn code_convert_destination(
    interp: &Interpreter,
    destination: Option<&Value>,
) -> Result<Option<u64>, LispError> {
    match destination {
        None => Ok(None),
        Some(value) if value.is_nil() || value.eq_value(Value::T) => Ok(None),
        Some(value) if !matches!(value.kind(), Kind::Buffer(_)) => {
            Err(LispError::WrongTypeArgument("bufferp".into(), *value))
        }
        Some(value) => interp.resolve_buffer_id(value).map(Some),
    }
}

/// code_convert_string with a buffer destination: CONVERTED inserted at
/// the buffer's point, which stays before it, and the number of
/// characters produced as the value.
/// produce_chars into a unibyte destination writes an eight-bit character
/// as its byte and any other in its multibyte form; each still counts as
/// one character produced.
fn unibyte_destination_form(converted: &StringLike) -> Result<StringLike, LispError> {
    let mut bytes = Vec::with_capacity(converted.text.len());
    for code in converted.character_codes_iter() {
        let code = code as u32;
        if (0x3F_FF80..=0x3F_FFFF).contains(&code) {
            bytes.push((code - 0x3F_FF00) as u8);
        } else {
            let (encoded, width) = crate::lisp::types::string_data::encode_character(code)?;
            bytes.extend_from_slice(&encoded[..width]);
        }
    }
    Ok(string_like(&bytes_to_shared_unibyte_value(&bytes)).expect("a unibyte string"))
}

fn insert_converted_text(
    interp: &mut Interpreter,
    buffer_id: u64,
    converted: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let converted = string_like(converted)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *converted))?;
    let produced = converted.text.chars().count();
    let saved_buffer_id = interp.current_buffer_id();
    interp.switch_to_buffer_id(buffer_id)?;
    let converted = if converted.multibyte && !interp.buffer.borrow().is_multibyte() {
        match unibyte_destination_form(&converted) {
            Ok(converted) => converted,
            Err(error) => {
                let _ = interp.switch_to_buffer_id(saved_buffer_id);
                return Err(error);
            }
        }
    } else {
        converted
    };
    let inserted = insert_text_before_point(
        interp,
        &converted.text,
        &converted.props,
        &converted.extended_chars,
        produced,
        env,
    );
    let _ = interp.switch_to_buffer_id(saved_buffer_id);
    inserted?;
    Ok(Value::Integer(produced as i64))
}
