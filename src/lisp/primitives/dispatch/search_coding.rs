use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "match-beginning"
            | "match-end"
            | "match-data"
            | "set-match-data"
            | "match-string"
            | "match-string-no-properties"
            | "looking-at"
            | "looking-at-p"
            | "looking-back"
            | "replace-match"
            | "replace-region-contents"
            | "flush-lines"
            | "subst-char-in-region"
            | "internal--labeled-narrow-to-region"
            | "internal--labeled-widen"
            | "transpose-regions"
            | "dabbrev-expand"
            | "encode-coding-region"
            | "decode-coding-region"
            | "encode-coding-string"
            | "decode-coding-string"
            | "json-parse-string"
            | "json-parse-buffer"
            | "json-serialize"
            | "json-insert"
            | "insert-before-markers"
            | "insert-before-markers-and-inherit"
    )
}

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
            if args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let use_integers = args.first().is_some_and(Value::is_truthy);
            let source_buffer_id = if use_integers {
                None
            } else {
                interp.last_match_data_buffer_id
            };
            let mut items = Vec::new();
            for entry in interp.last_match_data.clone().unwrap_or_default() {
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
            Ok(Value::list(items))
        }
        "set-match-data" => {
            need_arg_range(name, args, 1, 2)?;
            if args[0].is_nil() {
                interp.last_match_data = None;
                interp.last_match_data_buffer_id = None;
                return Ok(Value::Nil);
            }
            let items = args[0].to_vec()?;
            let mut restored = Vec::new();
            let mut restored_buffer_id = None;
            let mut index = 0usize;
            while index + 1 < items.len() {
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
            interp.last_match_data = Some(restored);
            interp.last_match_data_buffer_id = restored_buffer_id;
            Ok(args[0].clone())
        }
        "match-string" | "match-string-no-properties" => regexp::match_string_impl(interp, args),

        "looking-at" => {
            need_args(name, args, 1)?;
            let pattern = string_text(&args[0])?;
            interp.set_variable(
                "last-looking-at-pattern",
                Value::String(pattern.clone()),
                &mut env.clone(),
            );
            regexp::looking_at_impl(interp, &args[0], env)
        }
        "looking-at-p" => {
            need_args(name, args, 1)?;
            let saved_match_data = interp.last_match_data.clone();
            let saved_match_data_buffer_id = interp.last_match_data_buffer_id;
            let result = regexp::looking_at_impl(interp, &args[0], env);
            interp.last_match_data = saved_match_data;
            interp.last_match_data_buffer_id = saved_match_data_buffer_id;
            result
        }
        "looking-back" => regexp::looking_back_impl(interp, args, env),

        "replace-match" => {
            need_args(name, args, 1)?;
            let replacement = string_text(&args[0])?;
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
                    .ok_or_else(|| LispError::TypeError("string".into(), source.type_name()))?;
                let replacement = regexp::expand_replace_match_text(
                    &replacement,
                    &match_data,
                    literal,
                    &source.text,
                )?;
                let source_len = source.text.chars().count();
                let updated = format!(
                    "{}{}{}",
                    regexp::slice_string_chars(&source.text, 0, start),
                    replacement,
                    regexp::slice_string_chars(&source.text, end, source_len)
                );
                interp.last_match_data = Some(regexp::update_match_data_after_replace(
                    &match_data,
                    replace_index,
                    start,
                    end,
                    replacement.chars().count(),
                ));
                interp.last_match_data_buffer_id = None;
                return Ok(make_shared_string_value_with_multibyte(
                    updated,
                    Vec::new(),
                    source.multibyte,
                ));
            }
            let replacement =
                regexp::expand_replace_match(interp, &replacement, &match_data, literal)?;
            let replacement_len = replacement.chars().count();
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

        "replace-region-contents" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let replacement = replacement_content(interp, &args[2])?;
            let saved_point = interp.buffer.point();
            let saved_markers = interp.live_marker_positions_for_buffer(interp.current_buffer_id());
            let removed_len = to.saturating_sub(from);
            let inserted_len = replacement.text.chars().count();
            delete_region_with_hooks(interp, from, to, env)?;
            interp.buffer.goto_char(from);
            insert_text_with_hooks(
                interp,
                &replacement.text,
                &replacement.props,
                false,
                false,
                env,
            )?;
            for (marker_id, original) in saved_markers {
                let Some(original_pos) = original else {
                    continue;
                };
                let insertion_type = interp.marker_insertion_type(marker_id).unwrap_or(false);
                let new_pos = if original_pos < from {
                    original_pos
                } else if original_pos == from {
                    from
                } else if original_pos < to {
                    if insertion_type {
                        from + inserted_len
                    } else {
                        from
                    }
                } else {
                    ((original_pos as isize) + inserted_len as isize - removed_len as isize)
                        .max(from as isize) as usize
                };
                let _ =
                    interp.set_marker(marker_id, Some(new_pos), Some(interp.current_buffer_id()));
            }
            if saved_point > to {
                let target = ((saved_point as isize) + inserted_len as isize - removed_len as isize)
                    .max(from as isize) as usize;
                interp.buffer.goto_char(target);
            } else if (from..=to).contains(&saved_point) {
                let trailing = to.saturating_sub(saved_point);
                let target = from + inserted_len.saturating_sub(trailing);
                interp.buffer.goto_char(target);
            }
            Ok(Value::Nil)
        }
        "flush-lines" => {
            need_args(name, args, 3)?;
            let pattern = string_text(&args[0])?;
            let start = position_from_value(interp, &args[1])?;
            let end = position_from_value(interp, &args[2])?;
            let regex = Regex::new(&regexp::translate_elisp_regex(&pattern))
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let text = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let filtered = text
                .split_inclusive('\n')
                .filter(|line| !regex.is_match(&line.to_lowercase()))
                .collect::<String>();
            delete_region_with_hooks(interp, start, end, env)?;
            insert_text_with_hooks(interp, &filtered, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "subst-char-in-region" => {
            need_args(name, args, 4)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let old = args[2].as_integer()? as u32;
            let new = args[3].as_integer()? as u32;
            let old =
                char::from_u32(old).ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let new =
                char::from_u32(new).ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let text = interp
                .buffer
                .buffer_substring(from, to)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let replaced: String = text
                .chars()
                .map(|ch| if ch == old { new } else { ch })
                .collect();
            delete_region_with_hooks(interp, from, to, env)?;
            insert_text_with_hooks(interp, &replaced, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "internal--labeled-narrow-to-region" => {
            need_args(name, args, 3)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let label = args[2].as_symbol()?.to_string();
            let state = Value::list([
                Value::Integer(interp.buffer.point_min() as i64),
                Value::Integer(interp.buffer.point_max() as i64),
            ]);
            interp.set_variable(
                &format!("__emaxx-labeled-restriction-{label}"),
                state,
                &mut env.clone(),
            );
            interp.set_variable(
                "__emaxx-active-labeled-restriction",
                Value::list([Value::Integer(start as i64), Value::Integer(end as i64)]),
                &mut env.clone(),
            );
            interp.buffer.narrow_to_region(start, end);
            Ok(Value::Nil)
        }

        "internal--labeled-widen" => {
            need_args(name, args, 1)?;
            let label = args[0].as_symbol()?.to_string();
            interp.set_variable(
                "__emaxx-active-labeled-restriction",
                Value::Nil,
                &mut env.clone(),
            );
            if let Some(state) =
                interp.lookup_var(&format!("__emaxx-labeled-restriction-{label}"), env)
            {
                let values = state.to_vec()?;
                let start = values
                    .first()
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or(1) as usize;
                let end = values
                    .get(1)
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or((interp.buffer.size_total() + 1) as i64)
                    as usize;
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
            let saved_markers = interp.live_marker_positions_for_buffer(interp.current_buffer_id());
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
                let new_pos = if leave_markers || original_pos < start1 || original_pos >= end2 {
                    original_pos
                } else if original_pos < end1 {
                    original_pos + amt1
                } else if original_pos < start2 {
                    ((original_pos as isize) + diff) as usize
                } else {
                    original_pos - amt2
                };
                let _ =
                    interp.set_marker(marker_id, Some(new_pos), Some(interp.current_buffer_id()));
            }
            Ok(Value::Nil)
        }

        "dabbrev-expand" => {
            let point = interp.buffer.point();
            let mut start = point;
            while start > interp.buffer.point_min() {
                let Some(ch) = interp.buffer.char_at(start - 1) else {
                    break;
                };
                if !(ch.is_alphanumeric() || ch == '-' || ch == '_') {
                    break;
                }
                start -= 1;
            }
            let prefix = interp
                .buffer
                .buffer_substring(start, point)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            if prefix.is_empty() {
                return Ok(Value::Nil);
            }
            let haystack = interp.buffer.buffer_string();
            let prefix_start = haystack
                .chars()
                .take(start.saturating_sub(1))
                .map(char::len_utf8)
                .sum::<usize>();
            if let Some(found) = haystack[..prefix_start].rfind(&prefix)
                && let Some(expansion) = regexp::expand_symbol_at(&haystack, found, &prefix)
                && expansion != prefix
            {
                delete_region_with_hooks(interp, start, point, env)?;
                interp.buffer.goto_char(start);
                insert_text_with_hooks(interp, &expansion, &[], false, false, env)?;
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
            let destination_buffer = args
                .get(3)
                .filter(|value| !value.is_nil())
                .and_then(|value| interp.resolve_buffer_id(value).ok());
            let destination = args.get(3).is_some_and(Value::is_truthy);
            let transformed = if name == "encode-coding-region" {
                encode_coding_value(interp, &region, coding.as_deref(), destination, env)?
            } else {
                decode_coding_text(interp, &region, coding.as_deref(), destination, env)?
            };
            let transformed_text = string_text(&transformed)?;
            if let Some(buffer_id) = destination_buffer {
                let saved_buffer_id = interp.current_buffer_id();
                interp.switch_to_buffer_id(buffer_id)?;
                let insert_at = interp.buffer.point();
                insert_text_with_hooks(interp, &transformed_text, &[], false, false, env)?;
                interp.buffer.goto_char(insert_at);
                let _ = interp.switch_to_buffer_id(saved_buffer_id);
            } else if !destination {
                replace_buffer_region_with_text(interp, start, end, &transformed_text)?;
            }
            if destination {
                Ok(transformed)
            } else {
                Ok(Value::Nil)
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
                insert_text_with_hooks(interp, &decoded_text, &[], false, false, env)?;
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
            insert_text_with_hooks(interp, text, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "insert-before-markers" => insert_impl(interp, args, env, false, true),
        "insert-before-markers-and-inherit" => insert_impl(interp, args, env, true, true),

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}
