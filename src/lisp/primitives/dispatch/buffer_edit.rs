use super::*;

fn search_noerror_moves(noerror: Option<&Value>) -> bool {
    noerror.is_some_and(|value| value.is_truthy() && !matches!(value, Value::T))
}

fn column_zero_list_starts(interp: &Interpreter) -> Result<Vec<usize>, LispError> {
    let start = interp.buffer.point_min();
    let text = interp
        .buffer
        .buffer_substring(start, interp.buffer.point_max())
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let mut starts = Vec::new();
    let mut at_line_start = true;
    for (pos, ch) in (start..).zip(text.chars()) {
        if at_line_start && ch == '(' {
            starts.push(pos);
        }
        at_line_start = ch == '\n';
    }
    Ok(starts)
}

fn beginning_of_defun_raw_fallback(interp: &mut Interpreter, arg: i64) -> Result<Value, LispError> {
    if arg == 0 {
        return Ok(Value::Nil);
    }
    let starts = column_zero_list_starts(interp)?;

    let point = interp.buffer.point();
    let target = if arg > 0 {
        starts
            .iter()
            .rev()
            .filter(|pos| **pos < point)
            .nth((arg - 1) as usize)
            .copied()
    } else {
        starts
            .iter()
            .filter(|pos| **pos > point)
            .nth((-arg - 1) as usize)
            .copied()
    };

    if let Some(pos) = target {
        interp.buffer.goto_char(pos);
        Ok(Value::T)
    } else {
        // The search runs with the `move' flag: on failure point lands at
        // the buffer boundary in the search direction.
        if arg > 0 {
            let min = interp.buffer.point_min();
            interp.buffer.goto_char(min);
        } else {
            let max = interp.buffer.point_max();
            interp.buffer.goto_char(max);
        }
        Ok(Value::Nil)
    }
}

fn end_of_defun_call_end_function(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some(function) = interp
        .lookup_var("end-of-defun-function", env)
        .filter(Value::is_truthy)
    {
        call_function_value(interp, &function, &[], env)?;
    } else {
        super::call(interp, "forward-sexp", &[Value::Integer(1)], env)?;
    }
    Ok(())
}

fn end_of_defun_skip_trailing(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    // GNU lisp.el treats point right after a close paren as still inside
    // that defun: skip horizontal space and a trailing comment or newline.
    if super::call(interp, "bolp", &[], env)?.is_truthy() {
        return Ok(());
    }
    super::call(
        interp,
        "skip-chars-forward",
        &[Value::String(" \t".into())],
        env,
    )?;
    if super::call(
        interp,
        "looking-at",
        &[Value::String("\\s<\\|\n".into())],
        env,
    )?
    .is_truthy()
    {
        super::call(interp, "forward-line", &[Value::Integer(1)], env)?;
    }
    Ok(())
}

fn end_of_defun_impl(
    interp: &mut Interpreter,
    arg: i64,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut arg = if arg == 0 { 1 } else { arg };
    let pos = interp.buffer.point();
    let moves_to_eol = interp
        .lookup_var("end-of-defun-moves-to-eol", env)
        .map(|value| value.is_truthy())
        .unwrap_or(true);
    if moves_to_eol {
        interp.buffer.end_of_line();
    }
    super::call(interp, "beginning-of-defun-raw", &[Value::Integer(1)], env)?;
    let mut beg = interp.buffer.point();
    end_of_defun_call_end_function(interp, env)?;
    if arg <= 1 {
        end_of_defun_skip_trailing(interp, env)?;
    }
    let mut success = false;
    if arg > 0 {
        if interp.buffer.point() > pos {
            arg -= 1;
        } else {
            interp.buffer.goto_char(pos);
        }
        if arg != 0 {
            success = super::call(
                interp,
                "beginning-of-defun-raw",
                &[Value::Integer(-arg)],
                env,
            )?
            .is_truthy();
            if success {
                end_of_defun_call_end_function(interp, env)?;
            }
        }
    } else {
        if interp.buffer.point() < pos {
            arg += 1;
        } else {
            interp.buffer.goto_char(beg);
        }
        if arg != 0 {
            success = super::call(
                interp,
                "beginning-of-defun-raw",
                &[Value::Integer(-arg)],
                env,
            )?
            .is_truthy();
            if success {
                beg = interp.buffer.point();
                end_of_defun_call_end_function(interp, env)?;
            }
        }
    }
    end_of_defun_skip_trailing(interp, env)?;
    while arg < 0 && interp.buffer.point() >= pos && success {
        interp.buffer.goto_char(beg);
        success = super::call(
            interp,
            "beginning-of-defun-raw",
            &[Value::Integer(-arg)],
            env,
        )?
        .is_truthy();
        if interp.buffer.point() >= beg || !success {
            arg = 0;
        } else {
            beg = interp.buffer.point();
            end_of_defun_call_end_function(interp, env)?;
            end_of_defun_skip_trailing(interp, env)?;
        }
    }
    Ok(Value::Nil)
}

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "insert"
            | "insert-and-inherit"
            | "insert-char"
            | "insert-byte"
            | "self-insert-command"
            | "tex-insert-quote"
            | "skeleton-insert"
            | "insert-buffer-substring"
            | "comment-region"
            | "point"
            | "point-min"
            | "point-max"
            | "minibuffer-prompt-end"
            | "goto-char"
            | "forward-char"
            | "forward-word"
            | "backward-word"
            | "mark-sexp"
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
            | "beginning-of-defun-raw"
            | "end-of-defun"
            | "backward-sentence"
            | "forward-paragraph"
            | "backward-page"
            | "forward-page"
            | "forward-line"
            | "line-move"
            | "next-line"
            | "previous-line"
            | "move-end-of-line"
            | "move-beginning-of-line"
            | "vertical-motion"
            | "search-forward"
            | "search-backward"
            | "re-search-forward"
            | "search-forward-regexp"
            | "re-search-backward"
            | "search-backward-regexp"
            | "forward-list"
            | "down-list"
            | "up-list"
            | "forward-sexp"
            | "backward-sexp"
            | "forward-comment"
            | "scan-lists"
            | "scan-sexps"
            | "syntax-ppss"
            | "ppss-depth"
            | "syntax-ppss-flush-cache"
            | "parse-partial-sexp"
            | "backward-prefix-chars"
            | "buffer-string"
            | "minibuffer-contents"
            | "minibuffer-contents-no-properties"
            | "buffer-substring"
            | "buffer-substring-no-properties"
            | "filter-buffer-substring"
            | "buffer-substring--filter"
            | "add-to-invisibility-spec"
            | "invisible-p"
            | "derived-mode-p"
            | "provided-mode-derived-p"
            | "derived-mode-all-parents"
            | "derived-mode-add-parents"
            | "c-toggle-electric-state"
            | "c-toggle-comment-style"
            | "c-point-syntax"
            | "c-brace-newlines"
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
            | "matching-paren"
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
            | "backward-delete-char-untabify"
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
            | "indent-line-to"
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
        "self-insert-command" => {
            need_arg_range(name, args, 0, 1)?;
            let event = interp
                .lookup_var("last-command-event", env)
                .unwrap_or(Value::Nil);
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
                .unwrap_or(1)
                .max(0) as usize;
            let text: String = std::iter::repeat_n(ch, count).collect();
            insert_text_with_hooks(interp, &text, &[], true, false, env)?;
            run_named_hooks(
                interp,
                "post-self-insert-hook",
                env,
                Some(interp.current_buffer_id()),
            )?;
            Ok(Value::Nil)
        }
        "tex-insert-quote" => {
            need_arg_range(name, args, 0, 1)?;
            let open = interp
                .lookup_var("tex-open-quote", env)
                .map(|value| string_text(&value))
                .transpose()?
                .unwrap_or_else(|| "``".into());
            let close = interp
                .lookup_var("tex-close-quote", env)
                .map(|value| string_text(&value))
                .transpose()?
                .unwrap_or_else(|| "''".into());
            if interp
                .lookup_var("electric-pair-mode", env)
                .is_some_and(|value| value.is_truthy())
                && interp.buffer.mark_active()
                && let Some(mark) = interp.buffer.mark()
            {
                let point = interp.buffer.point();
                if point >= mark {
                    interp.buffer.goto_char(mark);
                    insert_text_with_hooks(interp, &open, &[], false, false, env)?;
                    interp.buffer.goto_char(point + open.chars().count());
                    insert_text_with_hooks(interp, &close, &[], false, false, env)?;
                } else {
                    interp.buffer.goto_char(mark);
                    insert_text_with_hooks(interp, &close, &[], false, false, env)?;
                    interp.buffer.goto_char(point);
                    insert_text_with_hooks(interp, &open, &[], false, false, env)?;
                }
            } else {
                insert_text_with_hooks(interp, &open, &[], false, false, env)?;
            }
            Ok(Value::Nil)
        }
        "newline" => {
            need_arg_range(name, args, 0, 2)?;
            let count = match args.first() {
                Some(value) if !value.is_nil() => value.as_integer()?.max(0),
                _ => 1,
            };
            let text = "\n".repeat(count as usize);
            insert_text_with_hooks(interp, &text, &[], true, false, env)?;
            if count > 0 {
                env.push(vec![(
                    "last-command-event".into(),
                    Value::Integer('\n' as i64),
                )]);
                let buffer_id = interp.current_buffer_id();
                let hook_result =
                    run_named_hooks(interp, "post-self-insert-hook", env, Some(buffer_id));
                env.pop();
                hook_result?;
            }
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
        "comment-region" => {
            need_arg_range(name, args, 2, 3)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let (start, end) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            ensure_region_modifiable(interp, start, end, env)?;
            let text = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let comment_start = interp
                .lookup_var("comment-start", env)
                .and_then(|value| string_text(&value).ok())
                .unwrap_or_else(|| "# ".into());
            let comment_start = if comment_start.chars().count() == 1 {
                let comment_add = interp
                    .lookup_var("comment-add", env)
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(0)
                    .max(0) as usize;
                if comment_add > 0 {
                    format!("{} ", comment_start.repeat(comment_add.saturating_add(1)))
                } else if interp.lookup_var("major-mode", env).is_some_and(
                    |value| matches!(value, Value::Symbol(mode) if mode == "emacs-lisp-mode"),
                ) && comment_start == ";"
                {
                    ";; ".into()
                } else {
                    comment_start
                }
            } else {
                comment_start
            };
            let comment_end = interp
                .lookup_var("comment-end", env)
                .and_then(|value| string_text(&value).ok())
                .unwrap_or_default();
            let commented = if comment_end.is_empty() {
                text.split_inclusive('\n')
                    .map(|line| format!("{comment_start}{line}"))
                    .collect::<String>()
            } else {
                format!("{comment_start}{text}{comment_end}")
            };
            replace_buffer_region_with_text(interp, start, end, &commented)?;
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
            // GNU constrains bol motion to the current field (fields are
            // rare; skip the work when the buffer has none).
            let old_pos = interp.buffer.point();
            interp.buffer.beginning_of_line();
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
        "back-to-indentation" => {
            let old_pos = interp.buffer.point();
            interp.buffer.beginning_of_line();
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
            let bol = interp.buffer.point();
            let limit = interp.buffer.end_of_line();
            interp.buffer.goto_char(bol);
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
            let raw = super::call(
                interp,
                "beginning-of-defun-raw",
                &[args.first().cloned().unwrap_or(Value::Nil)],
                env,
            )?;
            if raw.is_truthy() {
                interp.buffer.beginning_of_line();
                Ok(Value::T)
            } else {
                Ok(Value::Nil)
            }
        }
        "beginning-of-defun-raw" => {
            need_arg_range(name, args, 0, 1)?;
            let arg = args
                .first()
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            if let Some(function) = interp
                .lookup_var("beginning-of-defun-function", env)
                .filter(Value::is_truthy)
            {
                return call_function_value(interp, &function, &[Value::Integer(arg)], env);
            }
            beginning_of_defun_raw_fallback(interp, arg)
        }
        "end-of-defun" => {
            need_arg_range(name, args, 0, 2)?;
            let arg = args
                .first()
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            end_of_defun_impl(interp, arg, env)
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
        "next-line" | "previous-line" => {
            need_arg_range(name, args, 0, 2)?;
            let mut count = args
                .first()
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            if name == "previous-line" {
                count = -count;
            }
            // GNU line motion keeps the goal column across consecutive
            // vertical motion commands via temporary-goal-column.
            let last_command = interp.lookup_var("last-command", env).unwrap_or(Value::Nil);
            let continuing = matches!(
                &last_command,
                Value::Symbol(symbol) if symbol == "next-line" || symbol == "previous-line"
            );
            let goal = if continuing {
                interp
                    .lookup_var("temporary-goal-column", env)
                    .and_then(|value| value.as_integer().ok())
            } else {
                None
            }
            .unwrap_or_else(|| {
                super::call(interp, "current-column", &[], env)
                    .ok()
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(0)
            });
            interp.set_global_binding("temporary-goal-column", Value::Integer(goal));
            super::call(interp, "line-move", &[Value::Integer(count), Value::T], env)?;
            super::call(interp, "move-to-column", &[Value::Integer(goal)], env)?;
            Ok(Value::Nil)
        }
        "move-end-of-line" | "move-beginning-of-line" => {
            need_arg_range(name, args, 0, 1)?;
            let count = args
                .first()
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            if count != 1 {
                super::call(interp, "forward-line", &[Value::Integer(count - 1)], env)?;
            }
            if name == "move-end-of-line" {
                interp.buffer.end_of_line();
            } else {
                interp.buffer.beginning_of_line();
            }
            Ok(Value::Nil)
        }
        "line-move" => {
            need_arg_range(name, args, 1, 4)?;
            let n = integer_like_bigint(interp, &args[0])?;
            let noerror = args.get(1).is_some_and(Value::is_truthy);
            let remaining = forward_line_bigint(&mut interp.buffer, n);
            if remaining == BigInt::from(0u8) {
                Ok(Value::T)
            } else if noerror {
                Ok(Value::Nil)
            } else if remaining > BigInt::from(0u8) {
                Err(LispError::Signal("End of buffer".into()))
            } else {
                Err(LispError::Signal("Beginning of buffer".into()))
            }
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
        "down-list" => {
            need_arg_range(name, args, 0, 1)?;
            syntax::down_list_impl(interp, args.first(), env)
        }
        "up-list" => {
            need_arg_range(name, args, 0, 1)?;
            syntax::up_list_impl(interp, args.first(), env)
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
                // GNU: (goto-char (or (scan-sexps ...) (buffer-end arg)))
                // — with nothing but ignorable text left, move to the
                // buffer end instead of signaling.
                None if count > 0
                    && syntax::rest_of_buffer_is_ignorable(interp, interp.buffer.point()) =>
                {
                    let max = interp.buffer.point_max();
                    interp.buffer.goto_char(max);
                    Ok(Value::Nil)
                }
                None if count < 0
                    && syntax::buffer_before_is_ignorable(interp, interp.buffer.point()) =>
                {
                    let min = interp.buffer.point_min();
                    interp.buffer.goto_char(min);
                    Ok(Value::Nil)
                }
                None => Err(scan_error()),
            }
        }
        "mark-sexp" => {
            need_arg_range(name, args, 0, 1)?;
            let count = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            let Some(position) = syntax::scan_sexps_position(interp, interp.buffer.point(), count)
            else {
                return Err(scan_error());
            };
            interp.buffer.set_mark(position);
            Ok(Value::Nil)
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
            Ok(
                syntax::scan_sexps_position_for_scan_sexps(interp, from, count)?
                    .map(|position| Value::Integer(position as i64))
                    .unwrap_or(Value::Nil),
            )
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
                false,
                None,
                syntax::CommentStop::No,
                env,
            );
            interp.buffer.goto_char(saved);
            state
        }
        "ppss-depth" => {
            need_args(name, args, 1)?;
            Ok(args[0].to_vec()?.first().cloned().unwrap_or(Value::Nil))
        }
        "syntax-ppss-flush-cache" => {
            need_arg_range(name, args, 1, usize::MAX)?;
            Ok(Value::Nil)
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
        "filter-buffer-substring" => {
            need_arg_range(name, args, 2, 3)?;
            let filter = interp
                .lookup_var("filter-buffer-substring-function", env)
                .unwrap_or_else(|| Value::Symbol("buffer-substring--filter".into()));
            interp.call_function_value(filter, None, args, env)
        }
        "buffer-substring--filter" => {
            need_arg_range(name, args, 2, 3)?;
            let text = super::call(
                interp,
                "buffer-substring",
                &[args[0].clone(), args[1].clone()],
                env,
            )?;
            if args.get(2).is_some_and(Value::is_truthy) {
                let _ = super::call(
                    interp,
                    "delete-region",
                    &[args[0].clone(), args[1].clone()],
                    env,
                )?;
            }
            Ok(text)
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
        "c-toggle-electric-state" => {
            need_arg_range(name, args, 0, 1)?;
            let enabled = match args.first() {
                Some(Value::Nil) | None => !interp
                    .lookup_var("c-electric-flag", env)
                    .is_some_and(|value| value.is_truthy()),
                Some(value) => value.as_integer()? > 0,
            };
            interp.set_variable(
                "c-electric-flag",
                if enabled { Value::T } else { Value::Nil },
                env,
            );
            Ok(Value::Nil)
        }
        "c-toggle-comment-style" => {
            need_arg_range(name, args, 0, 1)?;
            let has_line = interp
                .lookup_var("c-line-comment-starter", env)
                .is_some_and(|value| value.is_truthy());
            let has_block = interp
                .lookup_var("c-block-comment-starter", env)
                .is_some_and(|value| value.is_truthy());
            if !(has_line || has_block) {
                return Ok(Value::Nil);
            }
            let use_block = match args.first() {
                Some(Value::Nil) | None => !interp
                    .lookup_var("c-block-comment-flag", env)
                    .is_some_and(|value| value.is_truthy()),
                Some(value) => value.as_integer()? > 0,
            };
            let use_block = if has_line && has_block {
                use_block
            } else {
                has_block
            };
            if use_block {
                let starter = interp
                    .lookup_var("c-block-comment-starter", env)
                    .and_then(|value| value.as_string().ok().map(str::to_string))
                    .unwrap_or_else(|| "/*".into());
                let ender = interp
                    .lookup_var("c-block-comment-ender", env)
                    .and_then(|value| value.as_string().ok().map(str::to_string))
                    .unwrap_or_else(|| "*/".into());
                interp.set_variable("c-block-comment-flag", Value::T, env);
                interp.set_variable("comment-start", Value::String(format!("{starter} ")), env);
                interp.set_variable("comment-end", Value::String(format!(" {ender}")), env);
            } else {
                let starter = interp
                    .lookup_var("c-line-comment-starter", env)
                    .and_then(|value| value.as_string().ok().map(str::to_string))
                    .unwrap_or_else(|| "//".into());
                interp.set_variable("c-block-comment-flag", Value::Nil, env);
                interp.set_variable("comment-start", Value::String(format!("{starter} ")), env);
                interp.set_variable("comment-end", Value::String(String::new()), env);
            }
            Ok(Value::Nil)
        }
        "c-point-syntax" => {
            need_arg_range(name, args, 0, 1)?;
            let syntax = match (interp.buffer.char_after(), interp.buffer.char_before()) {
                (Some('{'), _) | (_, Some('{')) => "brace-list-open",
                (Some('}'), _) | (_, Some('}')) => "brace-list-close",
                _ => "statement",
            };
            Ok(Value::Symbol(syntax.into()))
        }
        "c-brace-newlines" => {
            need_arg_range(name, args, 1, 1)?;
            let syntax = symbol_name_or_string(&args[0]).unwrap_or_default();
            if matches!(syntax.as_str(), "brace-list-open" | "brace-list-close") {
                Ok(Value::list([
                    Value::Symbol("before".into()),
                    Value::Symbol("after".into()),
                ]))
            } else {
                Ok(Value::Nil)
            }
        }
        "emaxx-struct-make" => {
            need_arg_range(name, args, 5, 6)?;
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
            // Unnamed `:type vector' structs are stored as plain vectors.
            if args.get(5).and_then(|mode| mode.as_symbol().ok()) == Some("vector") {
                let mut items = vec![Value::symbol("vector-literal")];
                items.extend(slots);
                return Ok(Value::list(items));
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
                        .is_some_and(|record| record.type_name == struct_name)
                        || interp.value_is_instance_of_class(&args[1], struct_name) =>
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
            need_arg_range(name, args, 3, 4)?;
            let struct_name = args[0].as_symbol()?;
            let slot_index = args[1].as_integer()?.max(0) as usize;
            if args.get(3).and_then(|mode| mode.as_symbol().ok()) == Some("vector") {
                // Unnamed vector struct: plain positional access, no tag.
                return Ok(crate::lisp::primitives::vector_items(&args[2])
                    .map_err(|_| {
                        LispError::TypeError(format!("{struct_name}-p"), args[2].type_name())
                    })?
                    .get(slot_index)
                    .cloned()
                    .unwrap_or(Value::Nil));
            }
            let list_backed = args.get(3).is_some_and(|value| !value.is_nil());
            if list_backed {
                return match &args[2] {
                    Value::Nil => Ok(Value::Nil),
                    Value::Cons(_, _) => Ok(args[2]
                        .to_vec()?
                        .get(slot_index)
                        .cloned()
                        .unwrap_or(Value::Nil)),
                    Value::Record(_) => {
                        // Some locally generated list-backed constructors still
                        // produce records; keep those working while runtime-read
                        // forms use plain list values.
                        let Value::Record(id) = &args[2] else {
                            unreachable!();
                        };
                        let record = interp.find_record(*id).ok_or_else(|| {
                            LispError::TypeError("record".into(), format!("record<{id}>"))
                        })?;
                        if record.type_name != struct_name
                            && !interp.value_is_instance_of_class(&args[2], struct_name)
                        {
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
                };
            }
            match &args[2] {
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    if record.type_name != struct_name
                        && !interp.value_is_instance_of_class(&args[2], struct_name)
                    {
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
            let pos = match args.first() {
                None | Some(Value::Nil) => interp.buffer.point(),
                Some(value) => position_from_value(interp, value)?,
            };
            match interp.buffer.char_at(pos) {
                Some(c) => Ok(Value::Integer(c as i64)),
                None => Ok(Value::Nil),
            }
        }
        "char-before" => {
            let pos = match args.first() {
                None | Some(Value::Nil) => interp.buffer.point(),
                Some(value) => position_from_value(interp, value)?,
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
        "kill-region" => {
            let result = super::call(interp, "delete-region", args, env)?;
            // GNU kill-region records itself for kill-append chaining.
            interp.set_variable("this-command", Value::Symbol("kill-region".into()), env);
            Ok(result)
        }
        "delete-line" | "kill-whole-line" => {
            need_arg_range(name, args, 0, 0)?;
            let start = interp.buffer.beginning_of_line();
            let end = move_lines_from(interp, start, 1).0;
            ensure_region_modifiable(interp, start, end, env)?;
            delete_region_with_hooks(interp, start, end, env)?;
            if name == "kill-whole-line" {
                // GNU kill-whole-line primes `last-command' so consecutive
                // kills append, and kills through `kill-region' which sets
                // `this-command'.
                interp.set_variable("last-command", Value::Symbol("kill-region".into()), env);
                interp.set_variable("this-command", Value::Symbol("kill-region".into()), env);
            }
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
        "backward-delete-char-untabify" => {
            need_arg_range(name, args, 0, 2)?;
            let count = args
                .first()
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1);
            super::call(interp, "delete-char", &[Value::Integer(-count)], env)
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
            if simple_c_family_indent_line(interp, env)? {
                return Ok(Value::Nil);
            }
            // GNU funcalls the buffer's `indent-line-function' (tabbing-only
            // functions excepted; newcomment relies on this for comment-only
            // lines).
            let indent_function = interp
                .lookup_var("indent-line-function", env)
                .unwrap_or(Value::Nil);
            let function_name = indent_function.as_symbol().ok().map(str::to_string);
            if !matches!(
                function_name.as_deref(),
                None | Some(
                    "indent-relative"
                        | "indent-relative-maybe"
                        | "indent-relative-first-indent-point"
                        | "indent-according-to-mode"
                )
            ) {
                interp.call_function_value(
                    indent_function.clone(),
                    function_name.as_deref(),
                    &[],
                    env,
                )?;
            }
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
        "indent-line-to" => {
            need_args(name, args, 1)?;
            let column = args[0].as_integer()?.max(0);
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            // GNU's backward-to-indentation constrains to the current
            // field: a read-only prompt prefix is not indentation.
            if buffer_has_field_property(interp) {
                let new_pos = interp.buffer.point();
                let constrained = super::call(
                    interp,
                    "constrain-to-field",
                    &[Value::Integer(new_pos as i64), Value::Integer(saved as i64)],
                    env,
                )?
                .as_integer()? as usize;
                interp.buffer.goto_char(constrained);
            }
            let bol = interp.buffer.point();
            while matches!(
                interp.buffer.char_at(interp.buffer.point()),
                Some(' ' | '\t')
            ) {
                let _ = interp.buffer.forward_char(1);
            }
            let indentation_end = interp.buffer.point();
            // GNU indent-line-to leaves the existing whitespace untouched
            // when the indentation is already at the requested column.
            let current = super::call(interp, "current-column", &[], env)?.as_integer()?;
            if current == column {
                interp.buffer.goto_char(saved.max(indentation_end));
                return Ok(Value::Integer(column));
            }
            if indentation_end > bol {
                ensure_region_modifiable(interp, bol, indentation_end, env)?;
                delete_region_with_hooks(interp, bol, indentation_end, env)?;
            }
            interp.buffer.goto_char(bol);
            super::call(interp, "indent-to", &[Value::Integer(column)], env)?;
            if saved > indentation_end {
                let removed = indentation_end - bol;
                let inserted = interp.buffer.point().saturating_sub(bol);
                interp
                    .buffer
                    .goto_char(saved.saturating_sub(removed).saturating_add(inserted));
            }
            Ok(Value::Integer(column))
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
            // GNU only touches lines that BEGIN inside the region: a
            // partial first line keeps its indentation, and empty lines
            // are never padded.
            let saved_point = interp.buffer.point();
            interp.buffer.goto_char(start);
            interp.buffer.beginning_of_line();
            let first_line_partial = interp.buffer.point() < start;
            interp.buffer.goto_char(saved_point);
            let adjusted = if count > 0 {
                let prefix = " ".repeat(count as usize);
                text.split_inclusive('\n')
                    .enumerate()
                    .map(|(index, line)| {
                        if (index == 0 && first_line_partial) || line == "\n" || line.is_empty() {
                            line.to_string()
                        } else {
                            format!("{prefix}{line}")
                        }
                    })
                    .collect::<String>()
            } else if count < 0 {
                let mut adjusted = String::new();
                for (index, line) in text.split_inclusive('\n').enumerate() {
                    if index == 0 && first_line_partial {
                        adjusted.push_str(line);
                        continue;
                    }
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
            let mut result = interp.buffer.point();
            interp.buffer.goto_char(saved);
            if buffer_has_field_property(interp) {
                result = super::call(
                    interp,
                    "constrain-to-field",
                    &[Value::Integer(result as i64), Value::Integer(saved as i64)],
                    env,
                )?
                .as_integer()? as usize;
            }
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
            Ok(Value::Integer(max_pos as i64))
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

fn simple_c_family_indent_line(interp: &mut Interpreter, env: &mut Env) -> Result<bool, LispError> {
    let major_mode = interp.lookup_var("major-mode", env).unwrap_or(Value::Nil);
    let mode = major_mode.as_symbol().unwrap_or("");
    if !matches!(
        mode,
        "c-mode" | "c++-mode" | "java-mode" | "js-mode" | "javascript-mode" | "plainer-c-mode"
    ) {
        return Ok(false);
    }

    let saved = interp.buffer.point();
    interp.buffer.beginning_of_line();
    let line_start = interp.buffer.point();
    let mut content_start = line_start;
    while matches!(interp.buffer.char_at(content_start), Some(' ' | '\t')) {
        content_start += 1;
    }
    interp.buffer.goto_char(line_start);
    let line_end = interp.buffer.end_of_line();
    interp.buffer.goto_char(saved);

    let prefix = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), line_start)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let line = interp
        .buffer
        .buffer_substring(content_start.min(line_end), line_end)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let offset = interp
        .lookup_var("c-basic-offset", env)
        .and_then(|value| value.as_integer().ok())
        .unwrap_or(2)
        .max(0) as usize;
    let mut depth = 0usize;
    for ch in prefix.chars() {
        match ch {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    let target_depth = if line.trim_start().starts_with('}') {
        depth.saturating_sub(1)
    } else {
        depth
    };
    let indent = " ".repeat(target_depth * offset);
    // Adjust the leading whitespace through the ordinary edit primitives so
    // markers around the line stay valid (srecode's inserters keep point
    // markers across `indent-according-to-mode').
    if content_start > line_start {
        ensure_region_modifiable(interp, line_start, content_start, env)?;
        delete_region_with_hooks(interp, line_start, content_start, env)?;
    }
    interp.buffer.goto_char(line_start);
    insert_text_with_hooks(interp, &indent, &[], false, false, env)?;
    let restored = if saved <= content_start {
        line_start + indent.len()
    } else {
        saved + indent.len().saturating_sub(content_start - line_start)
    };
    interp
        .buffer
        .goto_char(restored.clamp(interp.buffer.point_min(), interp.buffer.point_max()));
    Ok(true)
}

fn buffer_has_field_property(interp: &Interpreter) -> bool {
    interp
        .buffer
        .full_property_spans()
        .iter()
        .any(|span| span.props.iter().any(|(name, _)| name == "field"))
}
