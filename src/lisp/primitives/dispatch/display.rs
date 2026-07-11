use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "substitute-command-keys"
            | "message"
            | "warn"
            | "display-warning"
            | "current-message"
            | "error-message-string"
            | "ding"
            | "make-progress-reporter"
            | "progress-reporter-update"
            | "progress-reporter-done"
            | "vc-refresh-state"
            | "sleep-for"
            | "sit-for"
            | "accept-process-output"
            | "input-pending-p"
            | "prin1"
            | "cl-prin1"
            | "princ"
            | "print"
            | "terpri"
            | "prin1-to-string"
            | "cl-prin1-to-string"
            | "cl-print--expand-ellipsis"
            | "write-char"
            | "redirect-debugging-output"
            | "external-debugging-output"
            | "print--preprocess"
            | "read-char-choice"
            | "y-or-n-p"
            | "yes-or-no-p"
            | "char-equal"
            | "number-sequence"
            | "kbd"
            | "key-description"
            | "single-key-description"
            | "text-char-description"
            | "following-char"
            | "preceding-char"
            | "buffer-last-name"
            | "display-graphic-p"
            | "display-supports-face-attributes-p"
            | "display-images-p"
            | "display-color-p"
            | "display-grayscale-p"
            | "display-color-cells"
            | "window-system"
            | "frame-parameter"
            | "frame-parameters"
            | "set-frame-parameter"
            | "char-displayable-p"
            | "frame-width"
            | "frame-height"
            | "set-frame-width"
            | "set-frame-height"
            | "frame-char-width"
            | "display-popup-menus-p"
            | "transient-mark-mode"
            | "font-lock-mode"
            | "visual-line-mode"
            | "header-line-indent-mode"
            | "font-lock-specified-p"
            | "font-lock-add-keywords"
            | "font-lock-remove-keywords"
            | "font-lock-flush"
            | "font-lock-ensure"
            | "font-lock-fontify-region"
            | "find-image"
            | "image-size"
            | "image-mask-p"
            | "image-metadata"
            | "imagemagick-types"
            | "init-image-library"
            | "window-start"
            | "window-end"
            | "window-point"
            | "window-hscroll"
            | "window-vscroll"
            | "set-window-hscroll"
            | "pos-visible-in-window-p"
            | "window-width"
            | "window-height"
            | "move-to-window-line"
            | "recenter"
            | "scroll-up"
            | "scroll-down"
            | "window-text-pixel-size"
            | "buffer-text-pixel-size"
            | "get-display-property"
            | "bidi-find-overridden-directionality"
            | "redisplay"
            | "font-spec"
            | "font-get"
            | "face-attribute"
            | "face-name"
            | "face-foreground"
            | "face-background"
            | "set-face-attribute"
            | "color-distance"
            | "color-values"
            | "color-values-from-color-spec"
            | "selected-window"
            | "frame-selected-window"
            | "select-window"
            | "current-window-configuration"
            | "set-window-configuration"
            | "window-configuration-p"
            | "window-buffer"
            | "set-window-buffer"
            | "window-list"
            | "window-list-1"
            | "next-window"
            | "previous-window"
            | "delete-other-windows"
            | "frame-first-window"
            | "window-prev-buffers"
            | "set-window-prev-buffers"
            | "window-next-buffers"
            | "set-window-next-buffers"
            | "window-parameter"
            | "set-window-parameter"
            | "window-parameters"
            | "walk-windows"
            | "selected-frame"
            | "window-frame"
            | "framep"
            | "frame-terminal"
            | "frame-list"
            | "face-set-after-frame-default"
            | "windowp"
            | "window-live-p"
            | "window-minibuffer-p"
            | "window-at"
            | "split-window"
            | "split-window-below"
            | "split-window-vertically"
            | "split-window-right"
            | "split-window-horizontally"
            | "window-combined-p"
            | "window-dedicated-p"
            | "window-splittable-p"
            | "window-edges"
            | "window-body-edges"
            | "window-inside-edges"
            | "window-pixel-edges"
            | "window-body-pixel-edges"
            | "window-inside-pixel-edges"
            | "posn-at-x-y"
            | "window-display-table"
            | "terminal-live-p"
            | "terminal-list"
            | "terminal-name"
            | "terminal-parameter"
            | "set-terminal-parameter"
            | "send-string-to-terminal"
            | "get-buffer-window"
            | "minibuffer-window"
            | "minibuffer-selected-window"
            | "minibuffer-window-active-p"
            | "get-mru-window"
            | "get-buffer-window-list"
            | "display-buffer"
            | "quit-window"
            | "active-minibuffer-window"
            | "set-window-start"
            | "set-window-point"
            | "set-window-vscroll"
            | "facemenu-add-face"
    )
}

fn window_id_or_selected(interp: &Interpreter, value: &Value) -> Result<u64, LispError> {
    if value.is_nil() {
        return Ok(interp.selected_window_id());
    }
    window_record_id_from_value(interp, value)
        .ok_or_else(|| LispError::TypeError("window".into(), value.type_name()))
}

fn window_parameter_value(interp: &Interpreter, window_id: u64, parameter: &Value) -> Value {
    let Some(params) = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
    else {
        return Value::Nil;
    };
    let Ok(items) = params.to_vec() else {
        return Value::Nil;
    };
    for item in items {
        if let Ok(key) = item.car()
            && values_equal(interp, &key, parameter)
            && let Ok(value) = item.cdr()
        {
            return value;
        }
    }
    Value::Nil
}

fn set_window_parameter_value(
    interp: &mut Interpreter,
    window_id: u64,
    parameter: Value,
    value: Value,
) -> Result<Value, LispError> {
    let existing = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
        .cloned()
        .unwrap_or(Value::Nil);
    let mut items: Vec<Value> = existing.to_vec().unwrap_or_default();
    items.retain(|item| match item.car() {
        Ok(key) => !values_equal(interp, &key, &parameter),
        Err(_) => true,
    });
    if value.is_truthy() {
        items.push(Value::cons(parameter, value.clone()));
    }
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= WINDOW_PARAMETERS_SLOT {
        record.slots.resize(WINDOW_PARAMETERS_SLOT + 1, Value::Nil);
    }
    record.slots[WINDOW_PARAMETERS_SLOT] = Value::list(items);
    Ok(value)
}

fn window_slot_value(interp: &Interpreter, window_id: u64, slot: usize) -> Value {
    interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(slot))
        .cloned()
        .unwrap_or(Value::Nil)
}

fn set_window_slot_value(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
    value: Value,
) -> Result<Value, LispError> {
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= slot {
        record.slots.resize(slot + 1, Value::Nil);
    }
    record.slots[slot] = value.clone();
    Ok(value)
}

fn window_list_value(interp: &Interpreter, env: &Env, minibuf: Option<&Value>) -> Value {
    let selected = interp.selected_window_value();
    let include_minibuffer = matches!(minibuf, Some(Value::T));
    if !include_minibuffer {
        return Value::list([selected]);
    }
    let minibuffer = interp
        .lookup_var("emaxx-minibuffer-window", env)
        .unwrap_or_else(|| selected.clone());
    if values_equal(interp, &selected, &minibuffer) {
        Value::list([selected])
    } else {
        Value::list([selected, minibuffer])
    }
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        // ── Output ──
        "substitute-command-keys" => {
            need_arg_range(name, args, 1, 3)?;
            Ok(Value::String(substitute_command_keys(
                interp,
                &string_text(&args[0])?,
                env,
            )))
        }
        "message" => {
            let text = if args.is_empty() || args.first().is_some_and(Value::is_nil) {
                String::new()
            } else {
                string_text(&super::call(interp, "format", args, env)?)?
            };
            let buffer_name = interp
                .lookup_var("messages-buffer-name", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(|| "*Messages*".into());
            // GNU message_dolog: nothing is logged for an empty message or
            // with `message-log-max' nil; a fixnum keeps that many lines.
            let log_max = interp
                .lookup_var("message-log-max", env)
                .unwrap_or(Value::T);
            if !text.is_empty() && !log_max.is_nil() {
                let buffer_id = interp
                    .find_buffer(&buffer_name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&buffer_name).0);
                if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                    let end = buffer.point_max();
                    buffer.goto_char(end);
                    buffer.insert(&(text.clone() + "\n"));
                    if let Ok(max_lines) = log_max.as_integer()
                        && max_lines >= 0
                    {
                        let contents = buffer.full_buffer_string();
                        let lines = contents.matches('\n').count();
                        if lines > max_lines as usize {
                            let drop = lines - max_lines as usize;
                            let mut offset = 0usize;
                            for _ in 0..drop {
                                if let Some(next) = contents[offset..].find('\n') {
                                    offset += next + 1;
                                }
                            }
                            let char_end = contents[..offset].chars().count();
                            let _ = buffer.delete_region(1, char_end + 1);
                        }
                    }
                }
            }
            // The upstream capture advice ignores `(message nil)' and
            // `(message "")', which edebug uses to clear the echo area.
            let capturable = !args.is_empty()
                && !args.first().is_some_and(Value::is_nil)
                && args
                    .first()
                    .and_then(string_like)
                    .is_none_or(|string| !string.text.is_empty());
            if capturable {
                interp.append_message_capture(&text, true);
            }
            if args.first().is_some_and(Value::is_nil) {
                Ok(Value::Nil)
            } else {
                Ok(Value::String(text))
            }
        }
        "warn" => {
            let text = if args.is_empty() {
                String::new()
            } else {
                string_text(&super::call(interp, "format", args, env)?)?
            };
            let warning = if text.is_empty() {
                "Warning".to_string()
            } else {
                format!("Warning: {text}")
            };
            let _ = super::call(interp, "message", &[Value::String(warning.clone())], env)?;
            append_to_warnings_buffer(interp, &warning);
            Ok(Value::Nil)
        }
        "display-warning" => {
            need_arg_range(name, args, 2, 4)?;
            let warning_type = args[0].to_string();
            let message = string_text(&args[1])?;
            let warning = if warning_type == "nil" {
                format!("Warning: {message}")
            } else {
                format!("Warning ({warning_type}): {message}")
            };
            let _ = super::call(interp, "message", &[Value::String(warning.clone())], env)?;
            let buffer_name = args
                .get(3)
                .and_then(string_like)
                .map(|string| string.text)
                .unwrap_or_else(|| "*Warnings*".into());
            let warning = if let Some(prefix_function) =
                interp.lookup_var("warning-prefix-function", env)
                && prefix_function.is_truthy()
            {
                let prefix = interp.call_function_value(
                    prefix_function,
                    None,
                    &[
                        args[2].clone(),
                        Value::list([args[0].clone(), args[1].clone()]),
                    ],
                    env,
                )?;
                string_like(&prefix)
                    .map(|prefix| format!("{}{}", prefix.text, warning))
                    .unwrap_or(warning)
            } else {
                warning
            };
            append_to_named_warnings_buffer(interp, &buffer_name, &warning);
            Ok(Value::Nil)
        }
        "current-message" => {
            need_args(name, args, 0)?;
            // GNU batch mode has no echo area; `current-message' is nil.
            if interp
                .lookup_var("noninteractive", env)
                .is_some_and(|value| value.is_truthy())
            {
                return Ok(Value::Nil);
            }
            let buffer_name = interp
                .lookup_var("messages-buffer-name", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(|| "*Messages*".into());
            let Some((buffer_id, _)) = interp.find_buffer(&buffer_name) else {
                return Ok(Value::Nil);
            };
            let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
                return Ok(Value::Nil);
            };
            Ok(buffer
                .buffer_string()
                .lines()
                .next_back()
                .map(|line| Value::String(line.to_string()))
                .unwrap_or(Value::Nil))
        }
        "error-message-string" => {
            need_args(name, args, 1)?;
            if let Err(LispError::SignalValue(signal)) = args[0].to_vec()
                && circular_list_signal_p(&signal)
            {
                return Err(LispError::SignalValue(signal));
            }
            let items = args[0].to_vec().ok();
            if let Some(items) = items
                && let Some(message) = items.get(1).and_then(string_like)
            {
                Ok(Value::String(message.text))
            } else {
                Ok(Value::String(args[0].to_string()))
            }
        }
        "ding" => Ok(Value::Nil),
        "make-progress-reporter" => {
            need_arg_range(name, args, 1, 6)?;
            Ok(Value::list([
                Value::Symbol("progress-reporter".into()),
                args[0].clone(),
            ]))
        }
        "progress-reporter-update" | "progress-reporter-done" => {
            need_arg_range(name, args, 1, 3)?;
            Ok(Value::Nil)
        }
        "vc-refresh-state" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "sleep-for" => {
            need_arg_range(name, args, 1, 2)?;
            std::thread::sleep(wait_duration(args)?);
            interp.drive_threads(env, true)?;
            Ok(Value::Nil)
        }
        "sit-for" => {
            need_arg_range(name, args, 0, 3)?;
            // GNU: (sit-for SECONDS &optional NODISP), with the obsolete
            // (sit-for SECONDS MILLISEC NODISP) form still accepted when
            // MILLISEC is a number — a non-numeric second arg is NODISP.
            let duration_args = match args.get(1) {
                Some(Value::Integer(_) | Value::Float(_)) => args.get(0..2).unwrap_or(args),
                _ => args.get(0..1).unwrap_or(args),
            };
            std::thread::sleep(wait_duration(duration_args)?);
            interp.drive_threads(env, true)?;
            Ok(Value::T)
        }
        "accept-process-output" => {
            need_arg_range(name, args, 0, 4)?;
            // GNU: (accept-process-output &optional PROCESS SECONDS MILLISEC
            // JUST-THIS-ONE) - the wait always comes from args 2 and 3.
            let duration_args = args.get(1..3).unwrap_or(&[]);
            let deadline = std::time::Instant::now() + wait_duration(duration_args)?;
            let mut delivered = false;
            loop {
                delivered |=
                    crate::lisp::primitives::processes::pump_external_process_output(interp, env)?;
                delivered |=
                    crate::lisp::primitives::processes::run_pending_url_retrievals(interp, env)?;
                if delivered || std::time::Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            interp.drive_threads(env, true)?;
            Ok(if delivered { Value::T } else { Value::Nil })
        }
        "input-pending-p" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(if unread_command_events(interp, env)?.is_empty() {
                Value::Nil
            } else {
                Value::T
            })
        }
        "prin1" => {
            need_arg_range(name, args, 1, 3)?;
            let rendered = if matches!(args.get(2), None | Some(Value::Nil)) {
                render_prin1(interp, &args[0], env)?
            } else {
                let mut print_env = printer_env_with_overrides(env, args.get(2))?;
                let rendered = render_prin1(interp, &args[0], &mut print_env)?;
                sync_print_number_table(env, args.get(2), &print_env);
                let stream = printer_stream_value(interp, &print_env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                return Ok(args[0].clone());
            };
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "cl-prin1" => {
            need_arg_range(name, args, 1, 3)?;
            let rendered = if matches!(args.get(2), None | Some(Value::Nil)) {
                render_cl_prin1(interp, &args[0], env)?
            } else {
                let mut print_env = printer_env_with_overrides(env, args.get(2))?;
                let rendered = render_cl_prin1(interp, &args[0], &mut print_env)?;
                sync_print_number_table(env, args.get(2), &print_env);
                let stream = printer_stream_value(interp, &print_env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                return Ok(args[0].clone());
            };
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "princ" => {
            if args.is_empty() {
                return Ok(Value::Nil);
            }
            let rendered = render_princ(&args[0]);
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "print" => {
            if args.is_empty() {
                return Ok(Value::Nil);
            }
            let rendered = format!("\n{}\n", render_prin1(interp, &args[0], env)?);
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "terpri" => {
            need_arg_range(name, args, 0, 2)?;
            let stream = printer_stream_value(interp, env, args.first());
            if args.get(1).is_some_and(Value::is_truthy)
                && printer_stream_at_line_start(interp, stream.as_ref())?
            {
                return Ok(Value::Nil);
            }
            write_printer_output(interp, "\n", stream.as_ref(), env)?;
            Ok(Value::T)
        }
        "prin1-to-string" => {
            need_arg_range(name, args, 1, 3)?;
            if matches!(args.get(2), None | Some(Value::Nil)) {
                return Ok(Value::String(render_prin1(interp, &args[0], env)?));
            }
            let mut print_env = printer_env_with_overrides(env, args.get(2))?;
            let rendered = render_prin1(interp, &args[0], &mut print_env)?;
            sync_print_number_table(env, args.get(2), &print_env);
            Ok(Value::String(rendered))
        }
        "cl-prin1-to-string" => {
            need_arg_range(name, args, 1, 3)?;
            if matches!(args.get(2), None | Some(Value::Nil)) {
                return render_cl_prin1_value(interp, &args[0], env);
            }
            let mut print_env = printer_env_with_overrides(env, args.get(2))?;
            let rendered = render_cl_prin1_value(interp, &args[0], &mut print_env)?;
            sync_print_number_table(env, args.get(2), &print_env);
            Ok(rendered)
        }
        "cl-print--expand-ellipsis" => {
            need_args(name, args, 2)?;
            let parts = args[0].to_vec()?;
            let [Value::Symbol(tag), expansion] = parts.as_slice() else {
                return Err(LispError::TypeError(
                    "cl-print-ellipsis".into(),
                    args[0].type_name(),
                ));
            };
            if tag != "emaxx-cl-print-ellipsis" {
                return Err(LispError::TypeError(
                    "cl-print-ellipsis".into(),
                    args[0].type_name(),
                ));
            }
            let expansion = string_text(expansion)?;
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &expansion, stream.as_ref(), env)?;
            Ok(Value::Nil)
        }
        "write-char" => {
            need_arg_range(name, args, 1, 2)?;
            let rendered = format_char_conversion(&args[0])?;
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "redirect-debugging-output" => {
            need_arg_range(name, args, 0, 2)?;
            let target = match args.first() {
                None | Some(Value::Nil) => Value::Nil,
                Some(value) => Value::String(string_text(value)?),
            };
            interp.set_global_binding("emaxx-external-debugging-output-target", target.clone());
            Ok(target)
        }
        "external-debugging-output" => {
            need_args(name, args, 1)?;
            let rendered = string_like(&args[0])
                .map(|value| value.text)
                .unwrap_or(format_char_conversion(&args[0])?);
            append_external_debugging_output(interp, &rendered)?;
            Ok(args[0].clone())
        }
        "print--preprocess" => {
            need_args(name, args, 1)?;
            print_preprocess(interp, &args[0], env)
        }
        "read-char-choice" => {
            need_arg_range(name, args, 2, 3)?;
            ensure_interaction_allowed(interp, env)?;
            if interp
                .lookup_var("read-char-choice-use-read-key", env)
                .is_none_or(|value| value.is_nil())
                && let Ok(function) = interp.lookup_function("read-char-from-minibuffer", env)
            {
                return interp.call_function_value(
                    function,
                    Some("read-char-from-minibuffer"),
                    &args[..2],
                    env,
                );
            }
            Ok(first_choice_value(&args[1]).unwrap_or(Value::Integer('y' as i64)))
        }
        "y-or-n-p" | "yes-or-no-p" => {
            need_args(name, args, 1)?;
            ensure_interaction_allowed(interp, env)?;
            let _ = super::call(interp, "message", args, env)?;
            match pop_unread_command_event_value(interp, env)
                .ok()
                .and_then(|event| unread_event_char(&event))
                .map(|ch| ch.to_ascii_lowercase())
            {
                Some('n') => Ok(Value::Nil),
                Some('y') => Ok(Value::T),
                _ => Ok(Value::T),
            }
        }

        // ── More string/char ops ──
        "char-equal" => {
            need_args(name, args, 2)?;
            let a = args[0].as_integer()?;
            let b = args[1].as_integer()?;
            let case_fold = interp
                .lookup_var("case-fold-search", env)
                .map(|v| v.is_truthy())
                .unwrap_or(false);
            let eq = if case_fold {
                a == b || (a as u8 as char).eq_ignore_ascii_case(&(b as u8 as char))
            } else {
                a == b
            };
            Ok(if eq { Value::T } else { Value::Nil })
        }
        "number-sequence" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "number-sequence".into(),
                    args.len(),
                ));
            }
            let integer_sequence = args.iter().all(Value::is_integer);
            if integer_sequence {
                let from = integer_like_bigint(interp, &args[0])?;
                let to = if args.len() > 1 {
                    integer_like_bigint(interp, &args[1])?
                } else {
                    from.clone()
                };
                let step = if args.len() > 2 {
                    integer_like_bigint(interp, &args[2])?
                } else {
                    BigInt::from(1)
                };
                if step.is_zero() {
                    return Err(LispError::Signal(
                        "number-sequence: step must not be 0".into(),
                    ));
                }
                let mut result = Vec::new();
                let mut i = from;
                if step.sign() != Sign::Minus {
                    while i <= to {
                        result.push(normalize_bigint_value(i.clone()));
                        i += &step;
                    }
                } else {
                    while i >= to {
                        result.push(normalize_bigint_value(i.clone()));
                        i += &step;
                    }
                }
                return Ok(Value::list(result));
            }

            let from = numeric_to_f64(interp, &args[0])?;
            let to = if args.len() > 1 {
                numeric_to_f64(interp, &args[1])?
            } else {
                from
            };
            let step_value = args.get(2).cloned().unwrap_or(Value::Integer(1));
            let step = numeric_to_f64(interp, &step_value)?;
            if step == 0.0 {
                return Err(LispError::Signal(
                    "number-sequence: step must not be 0".into(),
                ));
            }
            let mut result = Vec::new();
            let mut current_float = from;
            let mut current_value = args[0].clone();
            let integer_step = step_value.is_integer();
            if step > 0.0 {
                while current_float <= to {
                    result.push(current_value.clone());
                    current_float += step;
                    current_value = if current_value.is_integer() && integer_step {
                        normalize_bigint_value(
                            integer_like_bigint(interp, &current_value)?
                                + integer_like_bigint(interp, &step_value)?,
                        )
                    } else {
                        Value::Float(current_float)
                    };
                }
            } else {
                while current_float >= to {
                    result.push(current_value.clone());
                    current_float += step;
                    current_value = if current_value.is_integer() && integer_step {
                        normalize_bigint_value(
                            integer_like_bigint(interp, &current_value)?
                                + integer_like_bigint(interp, &step_value)?,
                        )
                    } else {
                        Value::Float(current_float)
                    };
                }
            }
            Ok(Value::list(result))
        }
        "kbd" => {
            need_args(name, args, 1)?;
            parse_kbd_sequence(&string_text(&args[0])?)
        }
        "key-description" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(
                    "key-description".into(),
                    args.len(),
                ));
            }
            let mut parts = Vec::new();
            if let Some(prefix) = args.get(1) {
                append_key_description_parts(prefix, &mut parts)?;
            }
            append_key_description_parts(&args[0], &mut parts)?;
            Ok(Value::String(parts.join(" ")))
        }
        "single-key-description" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(
                    "single-key-description".into(),
                    args.len(),
                ));
            }
            let no_angles = args.get(1).is_some_and(Value::is_truthy);
            Ok(Value::String(single_key_description_text(
                &args[0], no_angles,
            )?))
        }
        "text-char-description" => {
            need_args(name, args, 1)?;
            Ok(Value::String(text_char_description_text(
                args[0].as_integer()?,
            )?))
        }

        // ── More buffer ops ──
        "following-char" => match interp.buffer.char_at(interp.buffer.point()) {
            Some(c) => Ok(Value::Integer(c as i64)),
            None => Ok(Value::Integer(0)),
        },
        "preceding-char" => {
            let pt = interp.buffer.point();
            if pt <= interp.buffer.point_min() {
                Ok(Value::Integer(0))
            } else {
                match interp.buffer.char_at(pt - 1) {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Integer(0)),
                }
            }
        }
        "buffer-last-name" => Ok(Value::String(
            interp
                .buffer
                .last_name
                .clone()
                .unwrap_or_else(|| interp.buffer.name.clone()),
        )),

        // ── Display stubs ──
        "display-graphic-p" | "display-images-p" | "window-system" => Ok(Value::Nil),
        // Batch sessions have no color support (GNU: nil / 0).
        "display-color-p" | "display-grayscale-p" => Ok(Value::Nil),
        "display-color-cells" => Ok(Value::Integer(0)),
        // emaxx is a batch/TTY display: no face-attribute display support
        // (rmc.el underlines the shortcut key only on graphical terminals).
        "display-supports-face-attributes-p" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(Value::Nil)
        }
        "frame-parameter" => {
            need_arg_range(name, args, 1, 2)?;
            let parameter = args
                .get(1)
                .ok_or_else(|| LispError::WrongNumberOfArgs(name.into(), args.len()))?
                .as_symbol()?;
            Ok(match parameter {
                "width" => Value::Integer(interp.frame_width()),
                "height" => Value::Integer(interp.frame_height()),
                "menu-bar-lines" | "tab-bar-lines" => Value::Integer(0),
                _ => Value::Nil,
            })
        }
        "set-frame-parameter" => {
            need_args(name, args, 3)?;
            Ok(args[2].clone())
        }
        "frame-parameters" => {
            // Terminal frame parameters, mirroring GNU's --batch alist for
            // the entries emaxx models; unspecified entries are omitted.
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::list([
                Value::cons(Value::Symbol("tab-bar-lines".into()), Value::Integer(0)),
                Value::cons(Value::Symbol("menu-bar-lines".into()), Value::Integer(1)),
                Value::cons(Value::Symbol("modeline".into()), Value::T),
                Value::cons(
                    Value::Symbol("width".into()),
                    Value::Integer(interp.frame_width()),
                ),
                Value::cons(
                    Value::Symbol("height".into()),
                    Value::Integer(interp.frame_height()),
                ),
                Value::cons(Value::Symbol("name".into()), Value::String("F1".into())),
                Value::cons(Value::Symbol("font".into()), Value::String("tty".into())),
                Value::cons(
                    Value::Symbol("background-color".into()),
                    Value::String("unspecified-bg".into()),
                ),
                Value::cons(
                    Value::Symbol("foreground-color".into()),
                    Value::String("unspecified-fg".into()),
                ),
                Value::cons(
                    Value::Symbol("background-mode".into()),
                    Value::Symbol("dark".into()),
                ),
                Value::cons(
                    Value::Symbol("display-type".into()),
                    Value::Symbol("mono".into()),
                ),
                Value::cons(Value::Symbol("minibuffer".into()), Value::T),
            ]))
        }
        "char-displayable-p" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Integer(codepoint) if char::from_u32(*codepoint as u32).is_some() => {
                    Ok(Value::T)
                }
                Value::String(text) if text.chars().count() == 1 => Ok(Value::T),
                Value::StringObject(state) if state.borrow().text.chars().count() == 1 => {
                    Ok(Value::T)
                }
                _ => Ok(Value::Nil),
            }
        }
        "frame-width" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(interp.frame_width()))
        }
        "frame-height" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(interp.frame_height()))
        }
        "set-frame-width" => {
            need_arg_range(name, args, 2, 4)?;
            interp.set_frame_width(args[1].as_integer()?);
            Ok(Value::Nil)
        }
        "set-frame-height" => {
            need_arg_range(name, args, 2, 4)?;
            interp.set_frame_height(args[1].as_integer()?);
            Ok(Value::Nil)
        }
        "frame-char-width" => Ok(Value::Integer(1)),
        "display-popup-menus-p" => Ok(Value::Nil),
        "transient-mark-mode" => {
            let enabled = args.first().is_some_and(Value::is_truthy);
            interp.set_variable(
                "transient-mark-mode",
                if enabled { Value::T } else { Value::Nil },
                env,
            );
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "font-lock-mode" => {
            let enabled = args
                .first()
                .map(|arg| !arg.is_nil() && !matches!(arg, Value::Integer(number) if *number <= 0))
                .unwrap_or(true);
            let buffer_id = interp.current_buffer_id();
            if enabled {
                interp.set_buffer_local_value(buffer_id, "font-lock-mode", Value::T);
                interp.set_buffer_local_value(buffer_id, "jit-lock-mode", Value::T);
                if interp
                    .buffer_local_value(buffer_id, "jit-lock-functions")
                    .is_none()
                {
                    interp.set_buffer_local_value(
                        buffer_id,
                        "jit-lock-functions",
                        Value::list([Value::Symbol("ignore".into())]),
                    );
                }
                interp.set_buffer_local_value(buffer_id, "font-lock-fontified", Value::T);
                font_lock_mode_run_mode_function(interp, buffer_id, Value::T, env)?;
                Ok(Value::T)
            } else {
                interp.set_buffer_local_value(buffer_id, "font-lock-mode", Value::Nil);
                interp.set_buffer_local_value(buffer_id, "jit-lock-mode", Value::Nil);
                interp.set_buffer_local_value(buffer_id, "jit-lock-functions", Value::Nil);
                interp.set_buffer_local_value(buffer_id, "font-lock-fontified", Value::Nil);
                font_lock_mode_run_mode_function(interp, buffer_id, Value::Nil, env)?;
                Ok(Value::Nil)
            }
        }
        "visual-line-mode" => {
            let enabled = args
                .first()
                .map(|arg| !arg.is_nil() && !matches!(arg, Value::Integer(number) if *number <= 0))
                .unwrap_or(true);
            let buffer_id = interp.current_buffer_id();
            interp.set_buffer_local_value(
                buffer_id,
                "visual-line-mode",
                if enabled { Value::T } else { Value::Nil },
            );
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "header-line-indent-mode" => {
            let enabled = args
                .first()
                .map(|arg| !arg.is_nil() && !matches!(arg, Value::Integer(number) if *number <= 0))
                .unwrap_or(true);
            let buffer_id = interp.current_buffer_id();
            interp.set_buffer_local_value(
                buffer_id,
                "header-line-indent-mode",
                if enabled { Value::T } else { Value::Nil },
            );
            interp.set_buffer_local_value(
                buffer_id,
                "header-line-indent",
                Value::String(String::new()),
            );
            interp.set_buffer_local_value(buffer_id, "header-line-indent-width", Value::Integer(0));
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "font-lock-specified-p" => {
            need_arg_range(name, args, 0, 1)?;
            let mode = args.first().is_some_and(Value::is_truthy);
            let defaults = interp
                .lookup_var("font-lock-defaults", env)
                .unwrap_or(Value::Nil);
            let keywords = interp
                .lookup_var("font-lock-keywords", env)
                .unwrap_or(Value::Nil);
            let major_mode = interp.lookup_var("major-mode", env).unwrap_or(Value::Nil);
            let font_lock_major_mode = interp
                .lookup_var("font-lock-major-mode", env)
                .unwrap_or(Value::Nil);
            let set_defaults = interp
                .lookup_var("font-lock-set-defaults", env)
                .unwrap_or(Value::Nil);
            Ok(
                if defaults.is_truthy()
                    || keywords.is_truthy()
                    || (mode
                        && set_defaults.is_truthy()
                        && font_lock_major_mode.is_truthy()
                        && font_lock_major_mode != major_mode)
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "font-lock-add-keywords" => {
            need_arg_range(name, args, 2, 3)?;
            let buffer_id = interp.current_buffer_id();
            let mut current = font_lock_raw_keyword_specs(
                interp.buffer_local_value(buffer_id, "font-lock-keywords"),
            );
            let additions = args[1].to_vec()?;
            if args.get(2).is_some_and(|value| !value.is_nil()) {
                current.extend(additions);
            } else {
                let mut updated = additions;
                updated.extend(current);
                current = updated;
            }
            interp.set_buffer_local_value(
                buffer_id,
                "font-lock-keywords",
                font_lock_keywords_value(&current),
            );
            Ok(Value::Nil)
        }
        "font-lock-remove-keywords" => {
            need_args(name, args, 2)?;
            let buffer_id = interp.current_buffer_id();
            let mut current = interp
                .buffer_local_value(buffer_id, "font-lock-keywords")
                .unwrap_or(Value::Nil)
                .to_vec()
                .unwrap_or_default();
            let removals = args[1].to_vec()?;
            current.retain(|existing| {
                !removals
                    .iter()
                    .any(|keyword| values_equal(interp, existing, keyword))
            });
            interp.set_buffer_local_value(buffer_id, "font-lock-keywords", Value::list(current));
            Ok(Value::Nil)
        }
        "font-lock-flush" => {
            need_arg_range(name, args, 0, 2)?;
            if !interp
                .lookup_var("font-lock-mode", env)
                .unwrap_or(Value::Nil)
                .is_truthy()
                || !interp
                    .lookup_var("font-lock-fontified", env)
                    .unwrap_or(Value::Nil)
                    .is_truthy()
            {
                return Ok(Value::Nil);
            }
            let start = args
                .first()
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_min());
            let end = args
                .get(1)
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_max());
            font_lock_ensure_region(interp, start, end, env)?;
            Ok(Value::Nil)
        }
        "font-lock-ensure" | "font-lock-fontify-region" => {
            // font-lock-fontify-region also takes GNU's optional LOUDLY.
            need_arg_range(name, args, 0, 3)?;
            // GNU fontifies whenever fontification is specified for the
            // buffer (font-lock-specified-p), even with font-lock-mode off
            // in batch.
            if std::env::var("EMAXX_DEBUG_FONTLOCK").is_ok() {
                eprintln!(
                    "FONTLOCK ensure called buffer={}",
                    interp.current_buffer_id()
                );
            }
            font_lock_install_mode_defaults(interp, env)?;
            let specified = interp
                .lookup_var("font-lock-defaults", env)
                .is_some_and(|value| value.is_truthy());
            if !specified
                && !interp
                    .lookup_var("font-lock-mode", env)
                    .unwrap_or(Value::Nil)
                    .is_truthy()
            {
                return Ok(Value::Nil);
            }
            let start = args
                .first()
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_min());
            let end = args
                .get(1)
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_max());
            font_lock_ensure_region(interp, start, end, env)?;
            if name == "font-lock-fontify-region"
                && super::call(
                    interp,
                    "fboundp",
                    &[Value::Symbol(
                        "emaxx--font-lock-fontify-region-extras".into(),
                    )],
                    env,
                )?
                .is_truthy()
            {
                interp.call_function_value(
                    Value::Symbol("emaxx--font-lock-fontify-region-extras".into()),
                    Some("emaxx--font-lock-fontify-region-extras"),
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "find-image" => {
            need_args(name, args, 1)?;
            let specs = args[0].to_vec()?;
            Ok(specs.into_iter().next().unwrap_or(Value::Nil))
        }
        "image-size" | "image-mask-p" | "image-metadata" => Err(LispError::Signal(
            "Images are unavailable on a nongraphical display".into(),
        )),
        "imagemagick-types" => Ok(Value::list([
            Value::Symbol("png".into()),
            Value::Symbol("jpeg".into()),
            Value::Symbol("gif".into()),
        ])),
        "init-image-library" => {
            need_args(name, args, 1)?;
            let image_type = args[0].as_symbol()?;
            Ok(
                if matches!(
                    image_type,
                    "pbm" | "png" | "jpeg" | "gif" | "svg" | "xbm" | "xpm" | "webp" | "tiff"
                ) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-start" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(window_start(interp, args.first())? as i64))
        }
        "window-end" => {
            need_arg_range(name, args, 0, 1)?;
            let buffer_id = if let Some(window) = args.first() {
                window_buffer_id(interp, window)
                    .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?
            } else {
                interp.selected_window_buffer_id()
            };
            let (_, point_max) = buffer_point_bounds(interp, buffer_id);
            Ok(Value::Integer(point_max as i64))
        }
        "window-point" => {
            need_arg_range(name, args, 0, 1)?;
            let buffer_id = if let Some(window) = args.first() {
                window_buffer_id(interp, window)
                    .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?
            } else {
                interp.selected_window_buffer_id()
            };
            let point = if buffer_id == interp.current_buffer_id() {
                interp.buffer.point()
            } else {
                interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.point())
                    .unwrap_or_else(|| interp.buffer.point())
            };
            Ok(Value::Integer(point as i64))
        }
        "window-hscroll" | "window-vscroll" => {
            need_arg_range(name, args, 0, 2)?;
            Ok(Value::Integer(0))
        }
        "set-window-hscroll" => {
            need_arg_range(name, args, 2, 3)?;
            Ok(args.get(1).cloned().unwrap_or(Value::Integer(0)))
        }
        "pos-visible-in-window-p" => {
            need_arg_range(name, args, 0, 3)?;
            if interp
                .lookup_var("noninteractive", env)
                .is_some_and(|value| !value.is_nil())
            {
                return Ok(Value::Nil);
            }
            let window = args.get(1).filter(|value| !value.is_nil());
            let buffer_id = if let Some(window) = window {
                window_buffer_id(interp, window)
                    .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?
            } else {
                interp.selected_window_buffer_id()
            };
            let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
            let pos = match args.first() {
                None | Some(Value::Nil) => interp.buffer.point(),
                Some(Value::T) => point_max,
                Some(value) => position_from_value(interp, value)?,
            };
            let start = window_start(interp, window)?;
            let first_visible = start.max(point_min);
            let visible_line = line_distance_in_buffer(interp, buffer_id, first_visible, pos);
            Ok(
                if pos >= first_visible
                    && pos <= point_max
                    && visible_line < DEFAULT_SELECTED_WINDOW_HEIGHT
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-width" => {
            need_arg_range(name, args, 0, 2)?;
            Ok(Value::Integer(interp.frame_width()))
        }
        "window-height" => {
            need_arg_range(name, args, 0, 2)?;
            Ok(Value::Integer(interp.frame_height()))
        }
        "move-to-window-line" => {
            need_arg_range(name, args, 0, 1)?;
            let line = resolve_window_line(args.first(), DEFAULT_SELECTED_WINDOW_HEIGHT / 2)?;
            let window_start = current_window_start(interp);
            let (target, shortage) = move_lines_from(interp, window_start, line);
            interp.buffer.goto_char(target);
            let actual = if shortage > 0 {
                line - shortage
            } else if shortage < 0 {
                line + shortage.abs()
            } else {
                line
            };
            Ok(Value::Integer(actual as i64))
        }
        "recenter" => {
            need_arg_range(name, args, 0, 2)?;
            let line = resolve_window_line(args.first(), DEFAULT_SELECTED_WINDOW_HEIGHT / 2)?;
            let point_line = beginning_of_line_at(interp, interp.buffer.point());
            let (new_start, _) = move_lines_from(interp, point_line, -line);
            set_current_window_start(interp, new_start);
            Ok(Value::Nil)
        }
        "scroll-up" => {
            need_arg_range(name, args, 0, 1)?;
            let count = if let Some(value) = args.first() {
                prefix_numeric_value(value)?.as_integer()?
            } else {
                1
            };
            scroll_selected_window(interp, count as isize, env)?;
            Ok(Value::Nil)
        }
        "scroll-down" => {
            need_arg_range(name, args, 0, 1)?;
            let count = if let Some(value) = args.first() {
                prefix_numeric_value(value)?.as_integer()?
            } else {
                1
            };
            scroll_selected_window(interp, -(count as isize), env)?;
            Ok(Value::Nil)
        }
        "window-text-pixel-size" => {
            let width = interp
                .buffer
                .buffer_string()
                .lines()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0);
            let height = interp.buffer.buffer_string().lines().count().max(1);
            Ok(Value::cons(
                Value::Integer(width as i64),
                Value::Integer(height as i64),
            ))
        }
        "buffer-text-pixel-size" => {
            // (buffer-text-pixel-size &optional WINDOW FROM TO X-LIMIT).
            // Without a graphical frame there is no font, so report the
            // widest line's character count as the pixel width and the line
            // count as the pixel height (one nominal unit per character).
            let text = interp.buffer.buffer_string();
            let width = text
                .lines()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0);
            let height = text.lines().count().max(1);
            Ok(Value::cons(
                Value::Integer(width as i64),
                Value::Integer(height as i64),
            ))
        }
        "get-display-property" => {
            need_args(name, args, 2)?;
            let pos = args[0].as_integer()?.max(0) as usize;
            let property = args[1].as_symbol()?;
            let display = interp
                .buffer
                .text_property_at(pos, "display")
                .unwrap_or(Value::Nil);
            Ok(display_property_value(&display, property).unwrap_or(Value::Nil))
        }
        "bidi-find-overridden-directionality" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            Ok(find_bidi_override(interp, start, end)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "redisplay" => Ok(Value::Nil),
        "font-spec" => {
            let mut name_spec = None;
            let mut index = 0;
            while index + 1 < args.len() {
                if let Value::Symbol(keyword) = &args[index]
                    && keyword == ":name"
                {
                    name_spec = Some(string_text(&args[index + 1])?);
                }
                index += 2;
            }
            Ok(interp.create_record(
                "font-spec",
                vec![Value::String(name_spec.unwrap_or_default())],
            ))
        }
        "font-get" => {
            need_args(name, args, 2)?;
            let property = args[1].as_symbol()?;
            let info = font_spec_info(interp, &args[0])?;
            Ok(match property {
                ":family" => info.family.map(Value::Symbol).unwrap_or(Value::Nil),
                ":size" => info.size.map(Value::Float).unwrap_or(Value::Nil),
                ":weight" => info.weight.map(Value::Symbol).unwrap_or(Value::Nil),
                ":slant" => info.slant.map(Value::Symbol).unwrap_or(Value::Nil),
                ":spacing" => info.spacing.map(Value::Integer).unwrap_or(Value::Nil),
                ":foundry" => info.foundry.map(Value::Symbol).unwrap_or(Value::Nil),
                _ => Value::Nil,
            })
        }
        "face-attribute" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?;
            let attribute = args[1].as_symbol()?;
            Ok(face_attribute_value(interp, face, attribute, args.get(3)))
        }
        "face-name" => {
            need_args(name, args, 1)?;
            let face = args[0].as_symbol()?;
            if !face_exists(interp, face) {
                return Err(LispError::Signal(format!("Not a face: {face}")));
            }
            Ok(Value::String(face.to_string()))
        }
        "face-foreground" | "face-background" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?;
            let attribute = if name == "face-foreground" {
                ":foreground"
            } else {
                ":background"
            };
            let value = face_attribute_value(interp, face, attribute, args.get(2));
            Ok(if is_unspecified_face_attribute(&value) {
                Value::Nil
            } else {
                value
            })
        }
        "set-face-attribute" => {
            if args.len() < 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?.to_string();
            if !face_exists(interp, &face) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("error".into()),
                    Value::String("Invalid face".into()),
                    Value::Symbol(face),
                ])));
            }
            let mut index = 2;
            while index + 1 < args.len() {
                let attribute = args[index].as_symbol()?;
                let value = &args[index + 1];
                if attribute == ":inherit" {
                    let inherit = match value {
                        Value::Nil => None,
                        Value::Symbol(symbol) => Some(symbol.clone()),
                        _ => return Err(LispError::TypeError("symbol".into(), value.type_name())),
                    };
                    interp.set_face_inherit_target(&face, inherit)?;
                }
                interp.put_symbol_property(
                    &face,
                    &face_attribute_property_name(attribute),
                    value.clone(),
                );
                index += 2;
            }
            Ok(Value::Nil)
        }
        "color-distance" => {
            need_args(name, args, 2)?;
            let left = parse_color_spec(&string_text(&args[0])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let right = parse_color_spec(&string_text(&args[1])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let distance = left
                .into_iter()
                .zip(right)
                .map(|(a, b)| {
                    let diff = i64::from(a) - i64::from(b);
                    diff * diff
                })
                .sum::<i64>();
            Ok(Value::Integer(distance))
        }
        "color-values-from-color-spec" => {
            need_args(name, args, 1)?;
            Ok(parse_color_spec(&string_text(&args[0])?)
                .map(|[r, g, b]| {
                    Value::list([
                        Value::Integer(i64::from(r)),
                        Value::Integer(i64::from(g)),
                        Value::Integer(i64::from(b)),
                    ])
                })
                .unwrap_or(Value::Nil))
        }
        "color-values" => {
            need_arg_range(name, args, 1, 2)?;
            if matches!(&args[0], Value::Symbol(symbol) if symbol == "unspecified")
                || matches!(&args[0], Value::String(text) if matches!(text.as_str(), "unspecified-fg" | "unspecified-bg"))
            {
                return Ok(Value::Nil);
            }
            Ok(parse_color_spec(&string_text(&args[0])?)
                .map(|[r, g, b]| {
                    Value::list([
                        Value::Integer(i64::from(r)),
                        Value::Integer(i64::from(g)),
                        Value::Integer(i64::from(b)),
                    ])
                })
                .unwrap_or(Value::Nil))
        }
        "selected-window" => Ok(interp.selected_window_value()),
        // Batch has a single frame whose selected window is THE window.
        "frame-selected-window" => Ok(interp.selected_window_value()),
        "select-window" => {
            need_arg_range(name, args, 1, 2)?;
            let Some(window_id) = window_record_id_from_value(interp, &args[0]) else {
                return Err(LispError::TypeError("window".into(), args[0].type_name()));
            };
            interp.set_selected_window_id(window_id);
            if let Some(buffer_id) = window_buffer_id(interp, &args[0])
                && interp.has_buffer_id(buffer_id)
            {
                interp.switch_to_buffer_id_preserving_window_history(buffer_id)?;
            }
            Ok(args[0].clone())
        }
        "current-window-configuration" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(interp.window_configuration_value())
        }
        "set-window-configuration" => {
            need_arg_range(name, args, 1, 3)?;
            let restored = interp.apply_window_configuration_value(&args[0])?;
            Ok(if restored { Value::T } else { Value::Nil })
        }
        "window-configuration-p" => {
            need_args(name, args, 1)?;
            Ok(if interp.is_window_configuration_value(&args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "window-buffer" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            if let Some(buffer_id) = window_buffer_id(interp, &window) {
                if buffer_id == interp.current_buffer_id() {
                    Ok(Value::Buffer(buffer_id, interp.buffer.name.clone()))
                } else if let Some((_, name)) = interp
                    .buffer_list
                    .iter()
                    .find(|(id, _)| *id == buffer_id)
                    .cloned()
                {
                    Ok(Value::Buffer(buffer_id, name))
                } else {
                    Ok(Value::Nil)
                }
            } else {
                Err(LispError::TypeError("window".into(), window.type_name()))
            }
        }
        "set-window-buffer" => {
            need_arg_range(name, args, 2, 3)?;
            let window = if args[0].is_nil() {
                interp.selected_window_value()
            } else {
                args[0].clone()
            };
            let Some(window_id) = window_record_id_from_value(interp, &window) else {
                return Err(LispError::TypeError("window".into(), window.type_name()));
            };
            let buffer_id = interp.resolve_buffer_id(&args[1])?;
            if window_id == interp.selected_window_id() {
                interp.set_selected_window_buffer_id(buffer_id);
            } else {
                let Some(record) = interp.find_record_mut(window_id) else {
                    return Err(LispError::TypeError("window".into(), window.type_name()));
                };
                if record.slots.len() == WINDOW_BUFFER_SLOT {
                    record.slots.resize(WINDOW_BUFFER_SLOT + 1, Value::Nil);
                }
                record.slots[WINDOW_BUFFER_SLOT] = Value::Integer(buffer_id as i64);
            }
            Ok(Value::Nil)
        }
        "window-list" | "window-list-1" => {
            need_arg_range(name, args, 0, 3)?;
            Ok(window_list_value(interp, env, args.get(1)))
        }
        "next-window" | "previous-window" => {
            need_arg_range(name, args, 0, 3)?;
            let selected = interp.selected_window_value();
            let include_minibuffer = matches!(args.get(1), Some(Value::T));
            if !include_minibuffer {
                return Ok(selected);
            }
            let current = args.first().cloned().unwrap_or_else(|| selected.clone());
            let minibuffer = interp
                .lookup_var("emaxx-minibuffer-window", env)
                .unwrap_or_else(|| selected.clone());
            Ok(if values_equal(interp, &current, &minibuffer) {
                selected
            } else {
                minibuffer
            })
        }
        "delete-other-windows" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let window_id = window_id_or_selected(interp, &window)?;
            set_window_slot_value(interp, window_id, WINDOW_PREV_BUFFERS_SLOT, Value::Nil)?;
            set_window_slot_value(interp, window_id, WINDOW_NEXT_BUFFERS_SLOT, Value::Nil)?;
            Ok(window)
        }
        "frame-first-window" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(interp.selected_window_value())
        }
        "window-prev-buffers" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(window_slot_value(
                interp,
                window_id,
                WINDOW_PREV_BUFFERS_SLOT,
            ))
        }
        "set-window-prev-buffers" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_slot_value(interp, window_id, WINDOW_PREV_BUFFERS_SLOT, args[1].clone())
        }
        "window-next-buffers" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(window_slot_value(
                interp,
                window_id,
                WINDOW_NEXT_BUFFERS_SLOT,
            ))
        }
        "set-window-next-buffers" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_slot_value(interp, window_id, WINDOW_NEXT_BUFFERS_SLOT, args[1].clone())
        }
        "window-parameter" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            Ok(window_parameter_value(interp, window_id, &args[1]))
        }
        "set-window-parameter" => {
            need_args(name, args, 3)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_parameter_value(interp, window_id, args[1].clone(), args[2].clone())
        }
        "window-parameters" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
                .cloned()
                .unwrap_or(Value::Nil))
        }
        "walk-windows" => {
            need_arg_range(name, args, 1, 3)?;
            call_function_value(
                interp,
                &args[0],
                std::slice::from_ref(&interp.selected_window_value()),
                env,
            )?;
            Ok(Value::Nil)
        }
        "selected-frame" => Ok(Value::Symbol("frame".into())),
        "window-frame" => {
            // emaxx has a single frame; any live window belongs to it.
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Symbol("frame".into()))
        }
        "framep" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Symbol(symbol) if symbol == "frame") {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "frame-terminal" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Symbol("terminal".into()))
        }
        "frame-list" => Ok(Value::list([Value::Symbol("frame".into())])),
        "face-set-after-frame-default" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(Value::Nil)
        }
        "windowp" | "window-live-p" => {
            need_args(name, args, 1)?;
            Ok(if is_window_value(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "window-minibuffer-p" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let Some(window_id) = window_record_id_from_value(interp, &window) else {
                return Err(LispError::TypeError("window".into(), window.type_name()));
            };
            let is_minibuffer = interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot, Value::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
            Ok(if is_minibuffer { Value::T } else { Value::Nil })
        }
        "window-at" => {
            need_arg_range(name, args, 2, 3)?;
            Ok(interp.selected_window_value())
        }
        "split-window"
        | "split-window-below"
        | "split-window-vertically"
        | "split-window-right"
        | "split-window-horizontally" => {
            need_arg_range(name, args, 0, 4)?;
            Ok(interp.selected_window_value())
        }
        "window-combined-p" => {
            need_arg_range(name, args, 0, 2)?;
            Ok(Value::Nil)
        }
        "window-dedicated-p" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "window-splittable-p" => {
            need_arg_range(name, args, 0, 2)?;
            Ok(Value::Nil)
        }
        "window-edges" | "window-body-edges" | "window-inside-edges" => {
            need_arg_range(name, args, 0, 4)?;
            Ok(Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(interp.frame_width()),
                Value::Integer(interp.frame_height()),
            ]))
        }
        "window-pixel-edges" | "window-body-pixel-edges" | "window-inside-pixel-edges" => {
            need_arg_range(name, args, 0, 4)?;
            Ok(Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(interp.frame_width() * 8),
                Value::Integer(interp.frame_height() * 16),
            ]))
        }
        "posn-at-x-y" => {
            need_arg_range(name, args, 2, 4)?;
            let x = args[0].as_integer()?;
            let y = args[1].as_integer()?;
            let pos_y = if y > 0 { y - 1 } else { y };
            let window = args
                .get(2)
                .filter(|value| is_window_value(interp, value))
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            Ok(Value::list([
                window,
                Value::Nil,
                Value::cons(Value::Integer(x), Value::Integer(pos_y)),
                Value::Integer(0),
            ]))
        }
        "window-display-table" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::Nil)
        }
        "terminal-live-p" => {
            need_args(name, args, 1)?;
            Ok(if matches!(&args[0], Value::Nil) {
                Value::T
            } else if let Value::Symbol(symbol) = &args[0] {
                if symbol == "terminal" || symbol == "frame" {
                    Value::T
                } else {
                    Value::Nil
                }
            } else {
                Value::Nil
            })
        }
        "terminal-list" => {
            need_arg_range(name, args, 0, 0)?;
            Ok(Value::list([Value::Symbol("terminal".into())]))
        }
        "terminal-name" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::String("initial_terminal".into()))
        }
        "terminal-parameter" => {
            need_args(name, args, 2)?;
            let parameter = args[1].as_symbol()?;
            Ok(interp.terminal_parameter(parameter).unwrap_or(Value::Nil))
        }
        "set-terminal-parameter" => {
            need_args(name, args, 3)?;
            let parameter = args[1].as_symbol()?;
            interp.set_terminal_parameter(parameter, args[2].clone());
            Ok(args[2].clone())
        }
        "send-string-to-terminal" => {
            need_arg_range(name, args, 1, 2)?;
            let _ = string_text(&args[0])?;
            Ok(Value::Nil)
        }
        "get-buffer-window" => {
            need_arg_range(name, args, 0, 3)?;
            let buffer_id = if let Some(buffer) = args.first() {
                if buffer.is_nil() {
                    Some(interp.current_buffer_id())
                } else if let Some(string) = string_like(buffer) {
                    interp.find_buffer(&string.text).map(|(id, _)| id)
                } else {
                    Some(interp.resolve_buffer_id(buffer)?)
                }
            } else {
                Some(interp.current_buffer_id())
            };
            Ok(if buffer_id == Some(interp.selected_window_buffer_id()) {
                interp.selected_window_value()
            } else {
                Value::Nil
            })
        }
        "minibuffer-window" => Ok(interp
            .lookup_var("emaxx-minibuffer-window", env)
            .unwrap_or_else(|| interp.selected_window_value())),
        "minibuffer-selected-window" | "get-mru-window" => Ok(interp
            .lookup_var("emaxx-minibuffer-selected-window", env)
            .filter(|value| !value.is_nil())
            .unwrap_or_else(|| interp.selected_window_value())),
        "minibuffer-window-active-p" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let is_minibuffer = window_record_id_from_value(interp, &window)
                .and_then(|id| interp.find_record(id))
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot, Value::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
            Ok(if is_minibuffer { Value::T } else { Value::Nil })
        }
        "get-buffer-window-list" => {
            need_arg_range(name, args, 0, 4)?;
            let buffer_id = if let Some(buffer) = args.first() {
                if buffer.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(buffer)?
                }
            } else {
                interp.current_buffer_id()
            };
            Ok(if buffer_id == interp.selected_window_buffer_id() {
                Value::list([interp.selected_window_value()])
            } else {
                Value::Nil
            })
        }
        "display-buffer" => {
            need_arg_range(name, args, 1, 2)?;
            let buffer_id = if let Some(name) = string_like(&args[0]).map(|string| string.text) {
                interp
                    .find_buffer(&name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&name).0)
            } else {
                interp.resolve_buffer_id(&args[0])?
            };
            let buffer_name = if buffer_id == interp.current_buffer_id() {
                interp.buffer.name.clone()
            } else {
                interp
                    .buffer_list
                    .iter()
                    .find(|(id, _)| *id == buffer_id)
                    .map(|(_, name)| name.clone())
                    .unwrap_or_else(|| interp.buffer.name.clone())
            };
            let buffer = Value::Buffer(buffer_id, buffer_name);
            let (action_function, action_alist) =
                split_display_buffer_action(interp, args.get(1), env);
            if let Some(function) = action_function {
                let result = interp.call_function_value(
                    function.clone(),
                    function.as_symbol().ok(),
                    &[buffer.clone(), action_alist.clone()],
                    env,
                )?;
                if is_window_value(interp, &result) {
                    return Ok(result);
                }
            }
            if display_action_inhibits_same_window(&action_alist) {
                return Ok(Value::Nil);
            }
            interp.set_selected_window_buffer_id(buffer_id);
            Ok(interp.selected_window_value())
        }
        "quit-window" => {
            need_arg_range(name, args, 0, 2)?;
            let kill = args.first().is_some_and(Value::is_truthy);
            let window = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let buffer_id = window_buffer_id(interp, &window)
                .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?;
            run_named_hooks(interp, "quit-window-hook", env, Some(buffer_id))?;
            if kill {
                interp.kill_buffer_id(buffer_id);
                return Ok(Value::Nil);
            }
            if buffer_id == interp.current_buffer_id() {
                if let Some(index) = interp
                    .buffer_list
                    .iter()
                    .position(|(id, _)| *id == buffer_id)
                {
                    let entry = interp.buffer_list.remove(index);
                    interp.buffer_list.push(entry);
                }
                let next = interp
                    .selected_window_previous_buffer_id()
                    .filter(|id| *id != buffer_id)
                    .or_else(|| {
                        interp
                            .buffer_list
                            .iter()
                            .find(|(id, _)| *id != buffer_id)
                            .map(|(id, _)| *id)
                    });
                if let Some(next_id) = next {
                    interp.switch_to_buffer_id_preserving_window_history(next_id)?;
                }
            }
            Ok(Value::Nil)
        }
        "active-minibuffer-window" => Ok(Value::Nil),
        "set-window-start" => {
            need_arg_range(name, args, 2, 4)?;
            let start = position_from_value(interp, &args[1])?;
            set_window_start_value(interp, &args[0], start)?;
            Ok(Value::T)
        }
        "set-window-point" => Ok(Value::T),
        "set-window-vscroll" => {
            need_arg_range(name, args, 2, 4)?;
            let _ = args[1].as_integer()?;
            Ok(Value::Integer(0))
        }
        "facemenu-add-face" => {
            need_args(name, args, 3)?;
            let face = args[0].clone();
            let start = position_from_value(interp, &args[1])?;
            let end = position_from_value(interp, &args[2])?;
            interp.buffer.put_text_property(start, end, "face", face);
            Ok(Value::Nil)
        }

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

fn font_lock_raw_keyword_specs(current: Option<Value>) -> Vec<Value> {
    let items = current.unwrap_or(Value::Nil).to_vec().unwrap_or_default();
    if items.first() != Some(&Value::T) {
        return items;
    }
    items
        .get(1)
        .and_then(|specs| specs.to_vec().ok())
        .unwrap_or_default()
}

fn font_lock_keywords_value(raw_specs: &[Value]) -> Value {
    let mut items = vec![Value::T, Value::list(raw_specs.iter().cloned())];
    items.extend(raw_specs.iter().filter_map(font_lock_compiled_keyword_spec));
    Value::list(items)
}

fn font_lock_compiled_keyword_spec(spec: &Value) -> Option<Value> {
    let parts = spec.to_vec().ok()?;
    if parts.len() < 3 {
        return None;
    }
    Some(Value::list([
        parts[0].clone(),
        Value::list([parts[1].clone(), parts[2].clone()]),
    ]))
}

fn append_to_warnings_buffer(interp: &mut Interpreter, warning: &str) {
    append_to_named_warnings_buffer(interp, "*Warnings*", warning);
}

fn append_to_named_warnings_buffer(interp: &mut Interpreter, buffer_name: &str, warning: &str) {
    let buffer_id = interp
        .find_buffer(buffer_name)
        .map(|(id, _)| id)
        .unwrap_or_else(|| interp.create_buffer(buffer_name).0);
    if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let end = buffer.point_max();
        buffer.goto_char(end);
        buffer.insert(&(warning.to_string() + "\n"));
    }
}

// GNU font-core's `font-lock-mode' body runs the buffer's
// `font-lock-function' with the new mode value; modes like ERT's results
// buffer install a redraw hook there.
fn font_lock_mode_run_mode_function(
    interp: &mut Interpreter,
    buffer_id: u64,
    mode: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(function) = interp.buffer_local_value(buffer_id, "font-lock-function") else {
        return Ok(());
    };
    if matches!(&function, Value::Symbol(name) if name == "font-lock-default-function")
        || function.is_nil()
    {
        return Ok(());
    }
    crate::lisp::primitives::call_function_value(interp, &function, &[mode], env)?;
    Ok(())
}
