use super::*;

fn search_noerror_moves(noerror: Option<&Value>) -> bool {
    noerror.is_some_and(|value| value.is_truthy() && !matches!(value, Value::T))
}

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "insert"
            | "insert-and-inherit"
            | "insert-char"
            | "insert-byte"
            | "skeleton-insert"
            | "insert-buffer-substring"
            | "point"
            | "point-min"
            | "point-max"
            | "minibuffer-prompt-end"
            | "goto-char"
            | "forward-char"
            | "forward-word"
            | "backward-word"
            | "indent-next-tab-stop"
            | "tab-to-tab-stop"
            | "skip-chars-forward"
            | "skip-chars-backward"
            | "skip-syntax-forward"
            | "skip-syntax-backward"
            | "backward-char"
            | "beginning-of-line"
            | "back-to-indentation"
            | "end-of-line"
            | "beginning-of-defun"
            | "end-of-defun"
            | "backward-sentence"
            | "forward-paragraph"
            | "backward-page"
            | "forward-page"
            | "forward-line"
            | "vertical-motion"
            | "search-forward"
            | "search-backward"
            | "re-search-forward"
            | "search-forward-regexp"
            | "re-search-backward"
            | "search-backward-regexp"
            | "forward-list"
            | "forward-sexp"
            | "backward-sexp"
            | "forward-comment"
            | "scan-lists"
            | "scan-sexps"
            | "syntax-ppss"
            | "parse-partial-sexp"
            | "buffer-string"
            | "minibuffer-contents"
            | "minibuffer-contents-no-properties"
            | "buffer-substring"
            | "buffer-substring-no-properties"
            | "add-to-invisibility-spec"
            | "invisible-p"
            | "derived-mode-p"
            | "provided-mode-derived-p"
            | "derived-mode-all-parents"
            | "derived-mode-add-parents"
            | "emaxx-struct-make"
            | "emaxx-struct-p"
            | "emaxx-class-p"
            | "emaxx-struct-ref"
            | "buffer-size"
            | "buffer-enable-undo"
            | "buffer-disable-undo"
            | "gap-position"
            | "gap-size"
            | "buffer-line-statistics"
            | "max-char"
            | "position-bytes"
            | "byte-to-position"
            | "treesit-available-p"
            | "treesit-ready-p"
            | "buffer-name"
            | "set-buffer-multibyte"
            | "toggle-enable-multibyte-characters"
            | "char-after"
            | "char-before"
            | "get-byte"
            | "bobp"
            | "eobp"
            | "bolp"
            | "eolp"
            | "delete-region"
            | "delete-and-extract-region"
            | "kill-region"
            | "delete-line"
            | "kill-whole-line"
            | "delete-horizontal-space"
            | "delete-char"
            | "delete-forward-char"
            | "kill-word"
            | "erase-buffer"
            | "delete-minibuffer-contents"
            | "newline"
            | "upcase-region"
            | "downcase-region"
            | "capitalize-region"
            | "upcase-initials-region"
            | "upcase-word"
            | "downcase-word"
            | "capitalize-word"
            | "current-column"
            | "current-indentation"
            | "indent-according-to-mode"
            | "indent-to-left-margin"
            | "indent-relative"
            | "indent-to"
            | "move-to-column"
            | "indent-rigidly"
            | "line-number-at-pos"
            | "line-beginning-position"
            | "pos-bol"
            | "count-lines"
            | "line-end-position"
            | "pos-eol"
            | "narrow-to-region"
            | "widen"
            | "buffer-modified-p"
            | "buffer-chars-modified-tick"
            | "buffer-modified-tick"
            | "set-buffer-modified-p"
            | "restore-buffer-modified-p"
            | "get-pos-property"
            | "get-char-property"
            | "get-text-property"
            | "text-property-any"
            | "text-property-not-all"
            | "next-single-property-change"
            | "next-single-char-property-change"
            | "previous-single-property-change"
            | "text-properties-at"
            | "object-intervals"
            | "put-text-property"
            | "add-text-properties"
            | "set-text-properties"
            | "dired-move-to-filename"
            | "dired-restore-positions"
            | "remove-list-of-text-properties"
            | "remove-text-properties"
            | "add-face-text-property"
            | "font-lock-append-text-property"
            | "font-lock-prepend-text-property"
            | "font-lock--remove-face-from-text-property"
            | "put"
            | "define-symbol-prop"
            | "function-put"
    ) || modes::is_major_mode_builtin(name)
}

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
        "newline" => {
            need_arg_range(name, args, 0, 1)?;
            let count = match args.first() {
                Some(value) if !value.is_nil() => value.as_integer()?.max(0),
                _ => 1,
            };
            let text = "\n".repeat(count as usize);
            insert_text_with_hooks(interp, &text, &[], true, false, env)?;
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
            insert_text_with_hooks(interp, &text, &[], false, false, env)?;
            Ok(Value::Nil)
        }
        "skeleton-insert" => {
            need_arg_range(name, args, 1, 3)?;
            let mut point = None;
            skeleton_insert_value(interp, &args[0], env, &mut point)?;
            if let Some(point) = point {
                interp.buffer.goto_char(point);
            }
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
            let start = if args.len() > 1 {
                position_from_value(interp, &args[1])?
            } else {
                source.point_min()
            };
            let end = if args.len() > 2 {
                position_from_value(interp, &args[2])?
            } else {
                source.point_max()
            };
            let text = source
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let props = source.substring_property_spans(start, end);
            insert_text_with_hooks(interp, &text, &props, false, false, env)?;
            Ok(Value::Nil)
        }
        "point" => Ok(Value::Integer(interp.buffer.point() as i64)),
        "point-min" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
        "point-max" => Ok(Value::Integer(interp.buffer.point_max() as i64)),
        "minibuffer-prompt-end" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
        "goto-char" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            interp.buffer.goto_char(pos);
            Ok(Value::Integer(interp.buffer.point() as i64))
        }
        "forward-char" => {
            let n = if args.is_empty() || args[0].is_nil() {
                1
            } else {
                args[0].as_integer()?
            };
            match interp.buffer.forward_char(n as isize) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "forward-word" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let case_symbols_as_words = case_symbols_as_words_enabled(interp, env);
            let syntax_word_chars = interp.syntax_word_chars();
            let is_word = |ch: char| {
                ch.is_alphanumeric()
                    || (case_symbols_as_words && ch == '_')
                    || syntax_word_chars
                        .iter()
                        .any(|code| *code == normalize_case_key(ch as u32))
            };
            let forward = n >= 0;
            let mut remaining = n.unsigned_abs();
            while remaining > 0 {
                if forward {
                    while let Some(ch) = interp.buffer.char_at(interp.buffer.point()) {
                        if is_word(ch) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(1);
                    }
                    while let Some(ch) = interp.buffer.char_at(interp.buffer.point()) {
                        if !is_word(ch) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(1);
                    }
                } else {
                    while interp.buffer.point() > interp.buffer.point_min() {
                        if matches!(interp.buffer.char_before(), Some(ch) if is_word(ch)) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(-1);
                    }
                    while interp.buffer.point() > interp.buffer.point_min() {
                        if !matches!(interp.buffer.char_before(), Some(ch) if is_word(ch)) {
                            break;
                        }
                        let _ = interp.buffer.forward_char(-1);
                    }
                }
                remaining -= 1;
            }
            Ok(Value::Nil)
        }
        "backward-word" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            super::call(interp, "forward-word", &[Value::Integer(-n)], env)
        }
        "indent-next-tab-stop" => {
            need_arg_range(name, args, 1, 2)?;
            let column = args[0].as_integer()?.max(0);
            let prev = args.get(1).is_some_and(Value::is_truthy);
            let tab_width = interp
                .lookup_var("tab-width", env)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(8)
                .max(1);
            let tabs = interp
                .lookup_var("tab-stop-list", env)
                .and_then(|value| value.to_vec().ok())
                .unwrap_or_default()
                .into_iter()
                .filter_map(|value| value.as_integer().ok())
                .collect::<Vec<_>>();
            let next = if let Some(first_after) = tabs.iter().copied().find(|tab| column < *tab) {
                if prev {
                    let previous = tabs
                        .iter()
                        .copied()
                        .take_while(|tab| *tab < first_after)
                        .last()
                        .unwrap_or(0);
                    if column == previous {
                        tabs.iter()
                            .copied()
                            .take_while(|tab| *tab < previous)
                            .last()
                            .unwrap_or(0)
                    } else {
                        previous
                    }
                } else {
                    first_after
                }
            } else {
                let last = tabs.last().copied().unwrap_or(0);
                let step = if tabs.len() >= 2 {
                    (tabs[tabs.len() - 1] - tabs[tabs.len() - 2]).max(1)
                } else {
                    tab_width
                };
                if prev {
                    if column <= last {
                        last - step
                    } else {
                        last + step * (((column - last - 1) / step) - 1).max(0)
                    }
                } else {
                    last + step * (((column - last) / step) + 1)
                }
            };
            Ok(Value::Integer(next.max(0)))
        }
        "tab-to-tab-stop" => {
            need_args(name, args, 0)?;
            let column = super::call(interp, "current-column", &[], env)?.as_integer()?;
            let next = super::call(
                interp,
                "indent-next-tab-stop",
                &[Value::Integer(column)],
                env,
            )?
            .as_integer()?;
            super::call(
                interp,
                "indent-to",
                &[Value::Integer(next), Value::Integer(1)],
                env,
            )
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
            syntax::skip_syntax_impl(interp, &args[0], args.get(1), true)
        }
        "skip-syntax-backward" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            syntax::skip_syntax_impl(interp, &args[0], args.get(1), false)
        }
        "backward-char" => {
            let n = if args.is_empty() || args[0].is_nil() {
                1
            } else {
                args[0].as_integer()?
            };
            match interp.buffer.forward_char(-(n as isize)) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "beginning-of-line" => {
            interp.buffer.beginning_of_line();
            Ok(Value::Nil)
        }
        "back-to-indentation" => {
            interp.buffer.beginning_of_line();
            let limit = interp.buffer.end_of_line();
            interp.buffer.beginning_of_line();
            let _ = syntax::skip_syntax_impl(
                interp,
                &Value::String(" ".into()),
                Some(&Value::Integer(limit as i64)),
                true,
            )?;
            Ok(Value::Nil)
        }
        "end-of-line" => {
            interp.buffer.end_of_line();
            Ok(Value::Nil)
        }
        "beginning-of-defun" => {
            let arg = args.first().cloned().unwrap_or(Value::Integer(1));
            if let Some(function) = interp
                .lookup_var("beginning-of-defun-function", env)
                .filter(Value::is_truthy)
            {
                return call_function_value(interp, &function, &[arg], env);
            }
            interp.buffer.goto_char(interp.buffer.point_min());
            Ok(Value::T)
        }
        "end-of-defun" => {
            let arg = args.first().cloned().unwrap_or(Value::Integer(1));
            if let Some(function) = interp
                .lookup_var("end-of-defun-function", env)
                .filter(Value::is_truthy)
            {
                return call_function_value(interp, &function, &[arg], env);
            }
            interp.buffer.goto_char(interp.buffer.point_max());
            Ok(Value::T)
        }
        "backward-sentence" => {
            need_arg_range(name, args, 0, 1)?;
            let point = interp.buffer.point();
            let prefix = interp
                .buffer
                .buffer_substring(interp.buffer.point_min(), point)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let pos = prefix
                .rfind("\n\n")
                .map(|byte| interp.buffer.point_min() + prefix[..byte].chars().count() + 2)
                .unwrap_or_else(|| interp.buffer.point_min());
            interp.buffer.goto_char(pos);
            Ok(Value::Nil)
        }
        "forward-paragraph" => {
            need_arg_range(name, args, 0, 1)?;
            let point = interp.buffer.point();
            let suffix = interp
                .buffer
                .buffer_substring(point, interp.buffer.point_max())
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let pos = suffix
                .find("\n\n")
                .map(|byte| point + suffix[..byte].chars().count())
                .unwrap_or_else(|| interp.buffer.point_max());
            interp.buffer.goto_char(pos);
            Ok(Value::Nil)
        }
        "backward-page" => {
            need_arg_range(name, args, 0, 1)?;
            let count = args
                .first()
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(1);
            if count < 0 {
                return call(interp, "forward-page", &[Value::Integer(-count)], env);
            }
            for _ in 0..count.max(1) {
                let point = interp.buffer.point();
                let prefix = interp
                    .buffer
                    .buffer_substring(interp.buffer.point_min(), point.saturating_sub(1))
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let pos = prefix
                    .rfind('\x0c')
                    .map(|byte| interp.buffer.point_min() + prefix[..byte].chars().count() + 1)
                    .unwrap_or_else(|| interp.buffer.point_min());
                interp.buffer.goto_char(pos);
            }
            Ok(Value::Nil)
        }
        "forward-page" => {
            need_arg_range(name, args, 0, 1)?;
            let count = args
                .first()
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(1);
            if count < 0 {
                return call(interp, "backward-page", &[Value::Integer(-count)], env);
            }
            for _ in 0..count.max(1) {
                let point = interp.buffer.point();
                let suffix = interp
                    .buffer
                    .buffer_substring(point, interp.buffer.point_max())
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let pos = suffix
                    .find('\x0c')
                    .map(|byte| point + suffix[..byte].chars().count() + 1)
                    .unwrap_or_else(|| interp.buffer.point_max());
                interp.buffer.goto_char(pos);
            }
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
        "vertical-motion" => {
            need_arg_range(name, args, 1, 3)?;
            let n = integer_like_bigint(interp, &args[0])?;
            let remaining = forward_line_bigint(&mut interp.buffer, n.clone());
            Ok(normalize_bigint_value(n - remaining))
        }
        "search-forward" | "search-backward" => {
            if args.is_empty() || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let needle = string_text(&args[0])?;
            let point = interp.buffer.point();
            let noerror = args.get(2).is_some_and(Value::is_truthy);
            let move_on_failure = search_noerror_moves(args.get(2));
            let limit = match args.get(1) {
                Some(value) if !value.is_nil() => position_from_value(interp, value)?,
                _ if name == "search-forward" => interp.buffer.point_max(),
                _ => interp.buffer.point_min(),
            };
            let result = if name == "search-forward" {
                let limit = limit.min(interp.buffer.point_max());
                if limit < point {
                    None
                } else {
                    let haystack = interp
                        .buffer
                        .buffer_substring(point, limit)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                    haystack.find(&needle).map(|found| {
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
                    haystack.rfind(&needle).map(|found| {
                        let start = limit + haystack[..found].chars().count();
                        let end = start + needle.chars().count();
                        (start, end)
                    })
                }
            };
            match result {
                Some((start, end)) => {
                    interp.last_match_data = Some(vec![Some((start, end))]);
                    interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                    let point = if name == "search-backward" {
                        start
                    } else {
                        end
                    };
                    interp.buffer.goto_char(point);
                    Ok(Value::Integer(point as i64))
                }
                None if noerror => {
                    if move_on_failure {
                        interp.buffer.goto_char(limit);
                    }
                    Ok(Value::Nil)
                }
                None => Err(LispError::SignalValue(Value::list([
                    Value::Symbol("search-failed".into()),
                    Value::String(needle),
                ]))),
            }
        }
        "re-search-forward" | "search-forward-regexp" => {
            regexp::buffer_regex_search(interp, args, env, true)
        }
        "re-search-backward" | "search-backward-regexp" => {
            regexp::buffer_regex_search(interp, args, env, false)
        }
        "forward-list" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let count = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            let step = count.signum();
            for _ in 0..count.unsigned_abs() {
                let position = syntax::scan_lists_impl(
                    interp,
                    &[
                        Value::Integer(interp.buffer.point() as i64),
                        Value::Integer(step),
                        Value::Integer(0),
                    ],
                    env,
                )?
                .as_integer()? as usize;
                interp.buffer.goto_char(position);
            }
            Ok(Value::Nil)
        }
        "forward-sexp" | "backward-sexp" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let mut count = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            if name == "backward-sexp" {
                count = -count;
            }
            match syntax::scan_sexps_position(interp, interp.buffer.point(), count) {
                Some(position) => {
                    interp.buffer.goto_char(position);
                    Ok(Value::Nil)
                }
                None => Err(scan_error()),
            }
        }
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
            Ok(syntax::scan_sexps_position(interp, from, count)
                .map(|position| Value::Integer(position as i64))
                .unwrap_or(Value::Nil))
        }
        "syntax-ppss" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let to = match args.first() {
                Some(value) if !value.is_nil() => position_from_value(interp, value)?,
                _ => interp.buffer.point(),
            };
            let saved = interp.buffer.point();
            let state = syntax::parse_forward(
                interp,
                interp.buffer.point_min(),
                to,
                None,
                None,
                false,
                env,
            );
            interp.buffer.goto_char(saved);
            state
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
            let oldstate = args.get(4).filter(|value| !value.is_nil());
            let commentstop = args.get(5).is_some_and(Value::is_truthy);
            syntax::parse_forward(interp, from, to, target_depth, oldstate, commentstop, env)
        }
        "buffer-string" => Ok(string_like_value(
            interp.buffer.buffer_string(),
            interp
                .buffer
                .substring_property_spans(interp.buffer.point_min(), interp.buffer.point_max()),
        )),
        "minibuffer-contents" | "minibuffer-contents-no-properties" => {
            need_arg_range(name, args, 0, 0)?;
            let start = interp.buffer.point_min();
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
            Ok(string_like_value(text, props))
        }
        "buffer-substring" | "buffer-substring-no-properties" => {
            need_args(name, args, 2)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let (start, end) = if from <= to { (from, to) } else { (to, from) };
            match interp.buffer.buffer_substring(start, end) {
                Ok(s) => {
                    if name == "buffer-substring" {
                        Ok(string_like_value(
                            s,
                            interp.buffer.substring_property_spans(start, end),
                        ))
                    } else {
                        Ok(Value::String(s))
                    }
                }
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "add-to-invisibility-spec" => {
            need_args(name, args, 1)?;
            let current = interp
                .lookup_var("buffer-invisibility-spec", env)
                .unwrap_or(Value::T);
            let updated = match current {
                Value::Nil => Value::list([args[0].clone()]),
                Value::T => Value::list([Value::T, args[0].clone()]),
                other => {
                    let mut items = other.to_vec()?;
                    if !items.iter().any(|item| item == &args[0]) {
                        items.push(args[0].clone());
                    }
                    Value::list(items)
                }
            };
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-invisibility-spec",
                updated,
            );
            Ok(Value::Nil)
        }
        "invisible-p" => {
            need_args(name, args, 1)?;
            let invisible = match position_from_value(interp, &args[0]) {
                Ok(position) if position >= 1 => char_is_invisible(interp, position, env),
                _ => invisibility_value_is_hidden(interp, &args[0], env),
            };
            Ok(if invisible { Value::T } else { Value::Nil })
        }
        "derived-mode-p" => {
            if args.is_empty() {
                return Ok(Value::Nil);
            }
            let current_mode = interp
                .lookup_var("major-mode", env)
                .and_then(|value| value.as_symbol().ok().map(str::to_string));
            let mut candidates = Vec::new();
            for value in args {
                if let Ok(symbol) = value.as_symbol() {
                    candidates.push(symbol.to_string());
                    continue;
                }
                if let Ok(items) = value.to_vec() {
                    for item in items {
                        if let Ok(symbol) = item.as_symbol() {
                            candidates.push(symbol.to_string());
                        }
                    }
                }
            }
            Ok(
                if current_mode.is_some_and(|current| {
                    let parents = derived_mode_parent_chain(interp, &current);
                    candidates
                        .iter()
                        .any(|candidate| parents.iter().any(|parent| parent == candidate))
                }) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "provided-mode-derived-p" => {
            need_arg_range(name, args, 2, usize::MAX)?;
            let mode = symbol_name_or_string(&args[0])?;
            let mut candidates = Vec::new();
            for value in &args[1..] {
                if let Ok(symbol) = symbol_name_or_string(value) {
                    candidates.push(symbol);
                    continue;
                }
                if let Ok(items) = value.to_vec() {
                    for item in items {
                        if let Ok(symbol) = symbol_name_or_string(&item) {
                            candidates.push(symbol);
                        }
                    }
                }
            }
            let parents = derived_mode_parent_chain(interp, &mode);
            Ok(candidates
                .into_iter()
                .find(|candidate| parents.iter().any(|parent| parent == candidate))
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }
        "derived-mode-all-parents" => {
            need_arg_range(name, args, 1, 2)?;
            let mode = symbol_name_or_string(&args[0])?;
            Ok(Value::list(
                derived_mode_parent_chain(interp, &mode)
                    .into_iter()
                    .map(Value::Symbol),
            ))
        }
        "derived-mode-add-parents" => {
            need_args(name, args, 2)?;
            let mode = args[0].as_symbol()?;
            derived_mode_add_parents(interp, mode, &args[1])?;
            Ok(args[0].clone())
        }
        "emaxx-struct-make" => {
            need_args(name, args, 5)?;
            let struct_name = args[0].as_symbol()?.to_string();
            let slot_names = args[1]
                .to_vec()?
                .into_iter()
                .map(|value| value.as_symbol().map(str::to_string))
                .collect::<Result<Vec<_>, _>>()?;
            let slot_defaults = args[2].to_vec()?;
            let constructor_spec = args[3]
                .to_vec()?
                .into_iter()
                .map(|value| value.as_symbol().map(str::to_string))
                .collect::<Result<Vec<_>, _>>()?;
            let call_args = args[4].to_vec()?;
            let mut slots = vec![Value::Nil; slot_names.len()];
            let mut provided = vec![false; slot_names.len()];
            let mut arg_index = 0usize;
            let mut spec_index = 0usize;
            while spec_index < constructor_spec.len() && constructor_spec[spec_index] != "&key" {
                if constructor_spec[spec_index] == "&optional" {
                    spec_index += 1;
                    continue;
                }
                if arg_index >= call_args.len() {
                    break;
                }
                if let Some(slot_index) = slot_names
                    .iter()
                    .position(|slot_name| slot_name == &constructor_spec[spec_index])
                {
                    slots[slot_index] = call_args[arg_index].clone();
                    provided[slot_index] = true;
                }
                spec_index += 1;
                arg_index += 1;
            }
            if constructor_spec
                .get(spec_index)
                .is_some_and(|keyword| keyword == "&key")
            {
                while arg_index + 1 < call_args.len() {
                    let keyword = call_args[arg_index].as_symbol()?;
                    let keyword = keyword.strip_prefix(':').unwrap_or(keyword);
                    if let Some(slot_index) =
                        slot_names.iter().position(|slot_name| slot_name == keyword)
                    {
                        slots[slot_index] = call_args[arg_index + 1].clone();
                        provided[slot_index] = true;
                    }
                    arg_index += 2;
                }
            }
            for (index, default_form) in slot_defaults.into_iter().enumerate() {
                if !provided.get(index).copied().unwrap_or(false) {
                    slots[index] = interp.eval(&default_form, env)?;
                }
            }
            Ok(interp.create_record(&struct_name, slots))
        }
        "emaxx-struct-p" => {
            need_args(name, args, 2)?;
            let struct_name = args[0].as_symbol()?;
            Ok(match &args[1] {
                Value::Record(id)
                    if interp
                        .find_record(*id)
                        .is_some_and(|record| record.type_name == struct_name) =>
                {
                    Value::T
                }
                _ => Value::Nil,
            })
        }
        "emaxx-class-p" => {
            need_args(name, args, 2)?;
            let class_name = args[0].as_symbol()?;
            Ok(if interp.value_is_instance_of_class(&args[1], class_name) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "emaxx-struct-ref" => {
            need_args(name, args, 3)?;
            let struct_name = args[0].as_symbol()?;
            let slot_index = args[1].as_integer()?.max(0) as usize;
            match &args[2] {
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    if record.type_name != struct_name {
                        return Err(LispError::TypeError(
                            format!("{struct_name}-p"),
                            args[2].type_name(),
                        ));
                    }
                    Ok(record.slots.get(slot_index).cloned().unwrap_or(Value::Nil))
                }
                other => Err(LispError::TypeError(
                    format!("{struct_name}-p"),
                    other.type_name(),
                )),
            }
        }
        "buffer-size" => Ok(Value::Integer(interp.buffer.size_total() as i64)),
        "buffer-enable-undo" => {
            interp.buffer.enable_undo();
            Ok(Value::Nil)
        }
        "buffer-disable-undo" => {
            interp.buffer.disable_undo();
            Ok(Value::Nil)
        }
        "gap-position" => Ok(Value::Integer(interp.buffer.point() as i64)),
        "gap-size" => Ok(Value::Integer(0)),
        "buffer-line-statistics" => {
            need_arg_range(name, args, 0, 1)?;
            buffer_line_statistics_value(interp, args.first())
        }
        "max-char" => Ok(Value::Integer(0x3F_FFFF)),
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
        _ if modes::is_major_mode_builtin(name) => modes::call_major_mode(interp, name),
        "treesit-available-p" | "treesit-ready-p" => Ok(Value::Nil),
        "buffer-name" => {
            if !args.is_empty()
                && let Value::Buffer(_, name) = &args[0]
            {
                return Ok(interp
                    .resolve_buffer_id(&args[0])
                    .ok()
                    .map(|_| Value::String(name.clone()))
                    .unwrap_or(Value::Nil));
            }
            Ok(Value::String(interp.buffer.name.clone()))
        }
        "set-buffer-multibyte" => {
            let enabled = args.first().is_none_or(Value::is_truthy);
            interp.buffer.set_multibyte(enabled);
            interp
                .buffer
                .push_undo_entry(crate::buffer::UndoEntry::Combined {
                    display: Value::Nil,
                    entries: Vec::new(),
                });
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "toggle-enable-multibyte-characters" => {
            let enabled = !interp.buffer.is_multibyte();
            interp.buffer.set_multibyte(enabled);
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "char-after" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            match interp.buffer.char_at(pos) {
                Some(c) => Ok(Value::Integer(c as i64)),
                None => Ok(Value::Nil),
            }
        }
        "char-before" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            if pos <= interp.buffer.point_min() {
                Ok(Value::Nil)
            } else {
                match interp.buffer.char_at(pos - 1) {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Nil),
                }
            }
        }
        "get-byte" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            Ok(interp
                .buffer
                .char_at(pos)
                .map(|ch| Value::Integer((ch as u32 & 0xFF) as i64))
                .unwrap_or(Value::Nil))
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
            Ok(string_like_value(
                delete_region_with_hooks(interp, from, to, env)?,
                Vec::new(),
            ))
        }
        "kill-region" => super::call(interp, "delete-region", args, env),
        "delete-line" | "kill-whole-line" => {
            need_arg_range(name, args, 0, 0)?;
            let start = interp.buffer.beginning_of_line();
            let end = move_lines_from(interp, start, 1).0;
            ensure_region_modifiable(interp, start, end, env)?;
            delete_region_with_hooks(interp, start, end, env)?;
            Ok(Value::Nil)
        }
        "delete-horizontal-space" => {
            need_arg_range(name, args, 0, 1)?;
            let backward_only = args.first().is_some_and(Value::is_truthy);
            let origin = interp.buffer.point();
            let start = {
                let _ = super::call(
                    interp,
                    "skip-chars-backward",
                    &[Value::String(" \t".into())],
                    env,
                )?;
                interp.buffer.point()
            };
            interp.buffer.goto_char(origin);
            let end = if backward_only {
                origin
            } else {
                let _ = super::call(
                    interp,
                    "skip-chars-forward",
                    &[Value::String(" \t".into())],
                    env,
                )?;
                interp.buffer.point()
            };
            interp.buffer.goto_char(origin);
            if start != end {
                ensure_region_modifiable(interp, start, end, env)?;
                delete_region_with_hooks(interp, start, end, env)?;
            }
            Ok(Value::Nil)
        }
        "delete-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let point = interp.buffer.point();
            if n >= 0 {
                let to = point + n as usize;
                if to > interp.buffer.point_max() {
                    Err(LispError::Signal("End of buffer".into()))
                } else {
                    delete_region_with_hooks(interp, point, to, env)?;
                    Ok(Value::Nil)
                }
            } else {
                let count = (-n) as usize;
                if point < interp.buffer.point_min() + count {
                    Err(LispError::Signal("Beginning of buffer".into()))
                } else {
                    delete_region_with_hooks(interp, point - count, point, env)?;
                    Ok(Value::Nil)
                }
            }
        }
        "delete-forward-char" => {
            if interp.buffer.mark_active()
                && interp
                    .lookup_var("transient-mark-mode", env)
                    .is_some_and(|value| value.is_truthy())
                && let Some((start, end)) = interp.buffer.region()
            {
                interp.buffer.deactivate_mark();
                return super::call(
                    interp,
                    "delete-region",
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                );
            }
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            super::call(interp, "delete-char", &[Value::Integer(n)], env)
        }
        "kill-word" => {
            let count = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let start = interp.buffer.point();
            super::call(interp, "forward-word", &[Value::Integer(count)], env)?;
            let end = interp.buffer.point();
            interp.buffer.goto_char(start);
            super::call(
                interp,
                "delete-region",
                &[Value::Integer(start as i64), Value::Integer(end as i64)],
                env,
            )
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
        "delete-minibuffer-contents" => {
            need_arg_range(name, args, 0, 0)?;
            delete_region_with_hooks(
                interp,
                interp.buffer.point_min(),
                interp.buffer.point_max(),
                env,
            )?;
            Ok(Value::Nil)
        }
        "upcase-region" | "downcase-region" | "capitalize-region" | "upcase-initials-region" => {
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
        "indent-according-to-mode" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
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
        "indent-to-left-margin" => {
            need_args(name, args, 0)?;
            let left_margin = interp
                .lookup_var("left-margin", env)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(0)
                .max(0);
            super::call(interp, "indent-to", &[Value::Integer(left_margin)], env)?;
            Ok(Value::Nil)
        }
        "indent-relative" => {
            need_arg_range(name, args, 0, 2)?;
            let unindented_ok = args.get(1).is_some_and(Value::is_truthy);
            let start_column = super::call(interp, "current-column", &[], env)?.as_integer()?;
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            let current_line_start = interp.buffer.point();
            let mut scan = current_line_start.saturating_sub(1);
            let mut indent = None;
            while scan >= interp.buffer.point_min() {
                let line_start = beginning_of_line_at(interp, scan);
                let line_end = {
                    let original = interp.buffer.point();
                    interp.buffer.goto_char(line_start);
                    let end = interp.buffer.end_of_line();
                    interp.buffer.goto_char(original);
                    end
                };
                let line_text = interp
                    .buffer
                    .buffer_substring(line_start, line_end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                if line_text.chars().any(|ch| ch != ' ' && ch != '\t') {
                    let tab_width = interp
                        .lookup_var("tab-width", env)
                        .and_then(|value| value.as_integer().ok())
                        .unwrap_or(8)
                        .max(1);
                    let mut column = 0i64;
                    let mut after_whitespace = true;
                    for ch in line_text.chars() {
                        if ch != ' ' && ch != '\t' && after_whitespace && column > start_column {
                            indent = Some(column);
                            break;
                        }
                        after_whitespace = ch == ' ' || ch == '\t';
                        column = if ch == '\t' {
                            ((column / tab_width) + 1) * tab_width
                        } else {
                            column + 1
                        };
                    }
                    break;
                }
                if line_start <= interp.buffer.point_min() {
                    break;
                }
                scan = line_start.saturating_sub(1);
            }
            interp.buffer.goto_char(saved);
            match indent {
                Some(column) => {
                    super::call(interp, "indent-to", &[Value::Integer(column)], env)?;
                    Ok(Value::Nil)
                }
                None if unindented_ok => Ok(Value::Nil),
                None => super::call(interp, "tab-to-tab-stop", &[], env).map(|_| Value::Nil),
            }
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
            insert_text_with_hooks(interp, &text, &[], true, false, env)?;
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
            while pos < interp.buffer.point_max() {
                let current_col = column_at(interp, env, start, pos);
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
                if next_col > target && force && ch == '\t' && !char_is_invisible(interp, pos, env)
                {
                    interp.buffer.goto_char(pos);
                    interp.insert_current_buffer(&" ".repeat(target - current_col));
                    pos = interp.buffer.point();
                    break;
                }
                pos += 1;
            }
            if force {
                let current_col = column_at(interp, env, start, pos);
                if current_col < target {
                    interp.buffer.goto_char(pos);
                    interp.insert_current_buffer(&" ".repeat(target - current_col));
                    pos = interp.buffer.point();
                }
            }
            interp.buffer.goto_char(pos);
            Ok(Value::Integer(column_at(interp, env, start, pos) as i64))
        }
        "indent-rigidly" => {
            need_arg_range(name, args, 3, 4)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let count = args[2].as_integer()?;
            let text = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let adjusted = if count > 0 {
                let prefix = " ".repeat(count as usize);
                text.split_inclusive('\n')
                    .map(|line| format!("{prefix}{line}"))
                    .collect::<String>()
            } else if count < 0 {
                let mut adjusted = String::new();
                for line in text.split_inclusive('\n') {
                    let mut remove = (-count) as usize;
                    let mut start_idx = 0usize;
                    for (index, ch) in line.char_indices() {
                        if remove == 0 || !matches!(ch, ' ' | '\t') {
                            start_idx = index;
                            break;
                        }
                        remove -= 1;
                        start_idx = index + ch.len_utf8();
                    }
                    adjusted.push_str(&line[start_idx..]);
                }
                adjusted
            } else {
                text
            };
            interp
                .delete_region_current_buffer(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            interp.buffer.goto_char(start);
            interp.insert_current_buffer(&adjusted);
            Ok(Value::Nil)
        }
        "line-number-at-pos" => {
            let pos = if args.is_empty() || args[0].is_nil() {
                interp.buffer.point()
            } else {
                match &args[0] {
                    Value::Integer(pos) => {
                        if *pos < 0 {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("args-out-of-range".into()),
                                Value::Integer(*pos),
                                Value::Integer(interp.buffer.point_min() as i64),
                                Value::Integer(interp.buffer.point_max() as i64),
                            ])));
                        }
                        *pos as usize
                    }
                    Value::Marker(id) => interp.marker_position(*id).ok_or_else(|| {
                        LispError::TypeError("integer-or-marker-p".into(), args[0].type_name())
                    })?,
                    _ => {
                        return Err(LispError::TypeError(
                            "integer-or-marker-p".into(),
                            args[0].type_name(),
                        ));
                    }
                }
            };
            if pos < interp.buffer.point_min() || pos > interp.buffer.point_max() {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("args-out-of-range".into()),
                    Value::Integer(pos as i64),
                    Value::Integer(interp.buffer.point_min() as i64),
                    Value::Integer(interp.buffer.point_max() as i64),
                ])));
            }
            Ok(Value::Integer(interp.buffer.line_number_at_pos(pos) as i64))
        }
        "line-beginning-position" | "pos-bol" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let saved = interp.buffer.point();
            let count = (n - 1) as isize;
            let shortage = if count != 0 {
                interp.buffer.forward_line(count)
            } else {
                0
            };
            // If forward_line overshot (couldn't find enough lines),
            // point is already at point-max/point-min — don't move it back.
            if shortage == 0 || (count > 0 && interp.buffer.point() < interp.buffer.point_max()) {
                interp.buffer.beginning_of_line();
            }
            let result = interp.buffer.point();
            interp.buffer.goto_char(saved);
            Ok(Value::Integer(result as i64))
        }
        "count-lines" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            Ok(Value::Integer(count_lines_in_buffer(
                &interp.buffer,
                start,
                end,
            )?))
        }
        "line-end-position" | "pos-eol" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let saved = interp.buffer.point();
            let count = (n - 1) as isize;
            if count != 0 {
                interp.buffer.forward_line(count);
            }
            interp.buffer.end_of_line();
            let result = interp.buffer.point();
            interp.buffer.goto_char(saved);
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
            } else if let Some(active) =
                interp.lookup_var("__emaxx-active-labeled-restriction", env)
            {
                let values = active.to_vec()?;
                let clamp_start = values
                    .first()
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or(1) as usize;
                let clamp_end = values
                    .get(1)
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or((interp.buffer.size_total() + 1) as i64)
                    as usize;
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
        "buffer-modified-p" => Ok(if interp.buffer.is_autosaved() {
            Value::Symbol("autosaved".into())
        } else if interp.buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        }),
        "buffer-chars-modified-tick" | "buffer-modified-tick" => {
            Ok(Value::Integer(interp.buffer.modified_tick() as i64))
        }
        "set-buffer-modified-p" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                interp.buffer.set_unmodified();
                if let Some(path) = current_buffer_file(interp).map(str::to_string) {
                    interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
                }
            } else {
                interp.buffer.set_modified();
                let _ = maybe_lock_current_buffer(interp, env);
            }
            Ok(Value::Nil)
        }
        "restore-buffer-modified-p" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                interp.buffer.set_unmodified();
                if let Some(path) = current_buffer_file(interp).map(str::to_string) {
                    interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
                }
            } else if matches!(&args[0], Value::Symbol(symbol) if symbol == "autosaved") {
                interp.buffer.set_modified();
                interp.buffer.set_autosaved();
            } else {
                interp.buffer.set_modified();
            }
            Ok(Value::Nil)
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
            let buffer_id = match args.get(2) {
                Some(object) if !object.is_nil() => interp.resolve_buffer_id(object)?,
                _ => interp.current_buffer_id(),
            };
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
            Ok(highest_priority_overlay_property(
                interp,
                buffer,
                pos,
                &prop,
                name == "get-pos-property",
            )
            .or_else(|| buffer_property_at_with_category(interp, buffer, pos, &prop))
            .unwrap_or(Value::Nil))
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
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
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
            let limit = args
                .get(3)
                .filter(|value| !value.is_nil())
                .map(|value| value.as_integer().map(|value| value.max(0) as usize))
                .transpose()?;
            if string_like(object).is_some() {
                let pos = args[0].as_integer()?.max(0) as usize;
                let text = string_text(object)?;
                let max_pos = limit.unwrap_or(text.chars().count());
                let initial = string_property_at(object, pos, &prop).unwrap_or(Value::Nil);
                for cursor in pos.saturating_add(1)..max_pos {
                    let current = string_property_at(object, cursor, &prop).unwrap_or(Value::Nil);
                    if current != initial {
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
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
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
                if current != initial {
                    return Ok(Value::Integer(cursor as i64));
                }
            }
            Ok(limit
                .map(|value| Value::Integer(value as i64))
                .unwrap_or(Value::Nil))
        }
        "next-single-char-property-change" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let prop = args[1].as_symbol()?.to_string();
            let object = args.get(2).unwrap_or(&Value::Nil);
            let limit = args
                .get(3)
                .filter(|value| !value.is_nil())
                .map(|value| value.as_integer().map(|value| value.max(0) as usize))
                .transpose()?;
            if string_like(object).is_some() {
                let pos = args[0].as_integer()?.max(0) as usize;
                let text = string_text(object)?;
                let max_pos = limit.unwrap_or(text.chars().count());
                let initial = string_property_at(object, pos, &prop).unwrap_or(Value::Nil);
                for cursor in pos.saturating_add(1)..max_pos {
                    let current = string_property_at(object, cursor, &prop).unwrap_or(Value::Nil);
                    if current != initial {
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
            let initial = buffer_char_property_at(interp, buffer, pos, &prop);
            for cursor in pos.saturating_add(1)..max_pos {
                let current = buffer_char_property_at(interp, buffer, cursor, &prop);
                if current != initial {
                    return Ok(Value::Integer(cursor as i64));
                }
            }
            Ok(limit
                .map(|value| Value::Integer(value as i64))
                .unwrap_or(Value::Nil))
        }
        "previous-single-property-change" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let prop = args[1].as_symbol()?.to_string();
            let object = args.get(2).unwrap_or(&Value::Nil);
            let limit = args
                .get(3)
                .filter(|value| !value.is_nil())
                .map(|value| value.as_integer().map(|value| value.max(0) as usize))
                .transpose()?;
            if string_like(object).is_some() {
                let pos = args[0].as_integer()?.max(0) as usize;
                let min_pos = limit.unwrap_or(0);
                if pos <= min_pos {
                    return Ok(limit
                        .map(|value| Value::Integer(value as i64))
                        .unwrap_or(Value::Nil));
                }
                let initial =
                    string_property_at(object, pos.saturating_sub(1), &prop).unwrap_or(Value::Nil);
                for cursor in (min_pos..pos).rev() {
                    let previous = cursor
                        .checked_sub(1)
                        .and_then(|index| string_property_at(object, index, &prop))
                        .unwrap_or(Value::Nil);
                    if previous != initial {
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
                if previous != initial {
                    return Ok(Value::Integer(cursor as i64));
                }
            }
            Ok(limit
                .map(|value| Value::Integer(value as i64))
                .unwrap_or(Value::Nil))
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
                    Value::String(state.borrow().text.clone())
                }
                _ => args[3].clone(),
            };
            if let Some(object) = args.get(4) {
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |mut current| {
                        current.retain(|(key, _)| key != &prop);
                        current.insert(0, (prop.clone(), prop_value.clone()));
                        current
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp
                        .buffer
                        .put_text_property(start, end, &prop, prop_value);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .put_text_property(start, end, &prop, prop_value);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
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
                        for (name, value) in &props {
                            if let Some((_, existing)) =
                                current.iter_mut().find(|(key, _)| key == name)
                            {
                                *existing = value.clone();
                            } else {
                                current.push((name.clone(), value.clone()));
                            }
                        }
                        current
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.buffer.add_text_properties(start, end, &props);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp.buffer.add_text_properties(start, end, &props);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
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
                    interp.buffer.set_text_properties(start, end, &props);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp.buffer.set_text_properties(start, end, &props);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "dired-move-to-filename" => {
            need_arg_range(name, args, 0, 2)?;
            let raise_error = args.first().is_some_and(Value::is_truthy);
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            let start = interp.buffer.point();
            let end = args
                .get(1)
                .filter(|value| !value.is_nil())
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| {
                    interp.buffer.goto_char(saved);
                    interp.buffer.end_of_line();
                    interp.buffer.point()
                });
            interp.buffer.goto_char(start);
            for pos in start..end {
                if interp
                    .buffer
                    .text_property_at(pos, "dired-filename")
                    .is_some_and(|value| value.is_truthy())
                {
                    interp.buffer.goto_char(pos);
                    return Ok(Value::Integer(pos as i64));
                }
            }
            let line = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let leading = line.chars().take_while(|ch| ch.is_whitespace()).count();
            let listing = line.trim_start();
            if matches!(listing.chars().next(), Some('d' | '-' | 'l')) {
                let offset = listing
                    .find(" 00:00 ")
                    .map(|index| index + " 00:00 ".len())
                    .or_else(|| listing.rfind(' ').map(|index| index + 1));
                if let Some(offset) = offset {
                    let target = start + leading + listing[..offset].chars().count();
                    if target < end {
                        interp.buffer.goto_char(target);
                        return Ok(Value::Integer(target as i64));
                    }
                }
            }
            interp.buffer.goto_char(start);
            if raise_error {
                Err(LispError::Signal("No file on this line".into()))
            } else {
                Ok(Value::Nil)
            }
        }
        "dired-restore-positions" => {
            need_args(name, args, 1)?;
            let positions = args[0].to_vec()?;
            let buffer_position = positions
                .first()
                .map(Value::to_vec)
                .transpose()?
                .unwrap_or_default();
            let saved_file = buffer_position.get(1).cloned().unwrap_or(Value::Nil);
            let saved_line = buffer_position
                .get(2)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(1)
                .max(1) as isize;
            let moved_to_file = if saved_file.is_truthy() {
                call_named_function(interp, "dired-goto-file", &[saved_file], env)?.is_truthy()
            } else {
                false
            };
            if !moved_to_file {
                interp.buffer.goto_char(interp.buffer.point_min());
                interp.buffer.forward_line(saved_line - 1);
                let _ = call_named_function(interp, "dired-move-to-filename", &[], env)?;
            }
            Ok(Value::Nil)
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
                    interp
                        .buffer
                        .remove_list_of_text_properties(start, end, &names);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .remove_list_of_text_properties(start, end, &names);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
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
                    interp
                        .buffer
                        .remove_list_of_text_properties(start, end, &names);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .remove_list_of_text_properties(start, end, &names);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "add-face-text-property" => add_face_text_property(interp, name, args),
        "font-lock-append-text-property" => font_lock_add_text_property(interp, name, args, true),
        "font-lock-prepend-text-property" => font_lock_add_text_property(interp, name, args, false),
        "font-lock--remove-face-from-text-property" => {
            need_arg_range(name, args, 4, 5)?;
            let prop = args[2].as_symbol()?.to_string();
            let face = args[3].clone();
            if let Some(object) = args.get(4)
                && string_like(object).is_some()
            {
                let start = args[0].as_integer()?.max(0) as usize;
                let end = args[1].as_integer()?.max(0) as usize;
                modify_shared_string_properties(object, start, end, |mut current| {
                    if let Some(index) = current.iter().position(|(key, _)| key == &prop) {
                        let updated = remove_face_value(current[index].1.clone(), &face);
                        if updated.is_nil() {
                            current.remove(index);
                        } else {
                            current[index].1 = updated;
                        }
                    }
                    current
                })?;
                return Ok(Value::Nil);
            }

            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let buffer_id = font_lock_target_buffer_id(interp, args.get(4))?;
            let mut cursor = start;
            while cursor < end {
                let (previous, next) =
                    font_lock_buffer_segment(interp, buffer_id, cursor, end, &prop)?;
                let updated = remove_face_value(previous, &face);
                font_lock_put_buffer_property(interp, buffer_id, cursor, next, &prop, updated)?;
                cursor = next;
            }
            font_lock_push_buffer_undo_entry(interp, buffer_id)?;
            Ok(Value::Nil)
        }
        "put" | "define-symbol-prop" | "function-put" => {
            need_args(name, args, 3)?;
            let symbol = args[0].as_symbol()?;
            let property = args[1].as_symbol()?;
            interp.put_symbol_property(symbol, property, args[2].clone());
            Ok(args[2].clone())
        }
        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

fn symbol_name_or_string(value: &Value) -> Result<String, LispError> {
    if let Ok(symbol) = value.as_symbol() {
        Ok(symbol.to_string())
    } else {
        string_text(value)
    }
}
