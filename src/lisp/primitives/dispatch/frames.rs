use super::*;
use regex::Regex;
use std::sync::OnceLock;

fn frame_value(id: u64) -> Value {
    Value::Frame(id)
}

fn decode_frame(
    interp: &Interpreter,
    value: Option<&Value>,
    nil_defaults_to_selected: bool,
    require_live: bool,
) -> Result<u64, LispError> {
    let value = value.unwrap_or(&Value::Nil);
    let id = match value {
        Value::Nil if nil_defaults_to_selected => interp.selected_frame_id,
        Value::Frame(id) if interp.frame_state(*id).is_some() => *id,
        _ => {
            return Err(wrong_type_argument(
                if require_live {
                    "frame-live-p"
                } else {
                    "framep"
                },
                value.clone(),
            ));
        }
    };
    if require_live && !interp.frame_is_live(id) {
        return Err(wrong_type_argument("frame-live-p", frame_value(id)));
    }
    Ok(id)
}

pub(super) fn decode_live_frame(
    interp: &Interpreter,
    value: Option<&Value>,
    nil_defaults_to_selected: bool,
) -> Result<u64, LispError> {
    decode_frame(interp, value, nil_defaults_to_selected, true)
}

fn default_frame_parameters(interp: &Interpreter, id: u64) -> Vec<(String, Value)> {
    let frame = interp
        .frame_state(id)
        .expect("a decoded frame identity must have state");
    let buffers = Value::list(
        interp
            .buffer_list
            .iter()
            .map(|(id, name)| Value::buffer(*id, name.clone())),
    );
    vec![
        ("tab-bar-lines".into(), Value::Integer(0)),
        ("menu-bar-lines".into(), Value::Integer(1)),
        ("buried-buffer-list".into(), Value::Nil),
        ("buffer-list".into(), buffers),
        ("unsplittable".into(), Value::Nil),
        ("modeline".into(), Value::T),
        ("width".into(), Value::Integer(frame.parameter_width.max(1))),
        (
            "height".into(),
            Value::Integer(frame.parameter_height.max(1)),
        ),
        ("name".into(), frame.name.clone()),
        ("font".into(), Value::string("tty")),
        ("background-color".into(), Value::string("unspecified-bg")),
        ("foreground-color".into(), Value::string("unspecified-fg")),
        ("cursor-color".into(), Value::string("white")),
        ("scroll-bar-background".into(), Value::Nil),
        ("scroll-bar-foreground".into(), Value::Nil),
        ("minibuffer".into(), Value::T),
    ]
}

fn frame_parameter_value(interp: &Interpreter, id: u64, parameter: &str) -> Value {
    let frame = interp
        .frame_state(id)
        .expect("a decoded frame identity must have state");
    frame
        .parameter_overrides
        .iter()
        .find(|(name, _)| name == parameter)
        .map(|(_, value)| value.clone())
        .or_else(|| {
            default_frame_parameters(interp, id)
                .into_iter()
                .find(|(name, _)| name == parameter)
                .map(|(_, value)| value)
        })
        .unwrap_or(Value::Nil)
}

fn store_frame_parameter(interp: &mut Interpreter, id: u64, parameter: String, value: Value) {
    if matches!(parameter.as_str(), "width" | "height") {
        // GNU's live TTY frame ignores width/height frame-parameter changes;
        // the native set-frame-{width,height,size} operations own geometry.
        return;
    }
    if parameter == "tty-color-mode" {
        // GNU's channel for changing a tty frame's color support
        // (term.c's set_tty_color_mode): tty_setup_colors publishes the
        // count, then safe_calln (Qtty_set_up_initial_frame_faces) has
        // faces.el recompute every face against the new display.
        let cells = match &value {
            Value::Integer(mode) if *mode > 1 => *mode,
            Value::Integer(_) | Value::Nil => 0,
            _ => 8,
        };
        interp.set_tty_display_colors(cells);
        if let Ok(forms) =
            crate::lisp::reader::Reader::new("(tty-set-up-initial-frame-faces)").read_all()
            && let Some(form) = forms.first()
        {
            let _ = interp.eval(form, &mut Vec::new());
        }
    }
    let menu_bar_lines_changed = matches!(parameter.as_str(), "menu-bar-lines" | "tab-bar-lines");
    let Some(frame) = interp.frame_state_mut(id) else {
        return;
    };
    if parameter == "name" {
        frame.name = value.clone();
    }
    if let Some((_, current)) = frame
        .parameter_overrides
        .iter_mut()
        .find(|(name, _)| name == &parameter)
    {
        *current = value;
    } else {
        frame.parameter_overrides.insert(0, (parameter, value));
    }
    if menu_bar_lines_changed && frame.tty_sized {
        let menu = frame
            .parameter_overrides
            .iter()
            .find(|(key, _)| key == "menu-bar-lines")
            .and_then(|(_, value)| value.as_integer().ok())
            .unwrap_or(0)
            .clamp(0, 1);
        let tab = frame
            .parameter_overrides
            .iter()
            .find(|(key, _)| key == "tab-bar-lines")
            .and_then(|(_, value)| value.as_integer().ok())
            .unwrap_or(0)
            .clamp(0, 1);
        frame.text_height = frame.height - menu - tab;
        interp.resize_frame_window_records_for(id);
    }
}

fn frame_parameters_value(interp: &Interpreter, id: u64) -> Value {
    let frame = interp
        .frame_state(id)
        .expect("a decoded frame identity must have state");
    let mut parameters = frame.parameter_overrides.clone();
    parameters.extend(
        default_frame_parameters(interp, id)
            .into_iter()
            .filter(|(name, _)| {
                !frame
                    .parameter_overrides
                    .iter()
                    .any(|(overridden, _)| overridden == name)
            }),
    );
    Value::list(
        parameters
            .into_iter()
            .map(|(name, value)| Value::cons(Value::symbol(&name), value)),
    )
}

fn check_frame_size(value: &Value) -> Result<i64, LispError> {
    value.as_integer()
}

fn parse_geometry(string: &str) -> Value {
    static GEOMETRY: OnceLock<Regex> = OnceLock::new();
    let regex = GEOMETRY.get_or_init(|| {
        Regex::new(
            r"^=?(?:(?P<width>[0-9]+)?[xX](?P<height>[0-9]+)?)?(?P<x>[+-][0-9]+)?(?P<y>[+-][0-9]+)?$",
        )
        .expect("static X geometry regex")
    });
    let Some(captures) = regex.captures(string) else {
        return Value::Nil;
    };
    if captures.name("width").is_none()
        && captures.name("height").is_none()
        && captures.name("x").is_none()
        && captures.name("y").is_none()
    {
        return Value::Nil;
    }
    let integer = |name: &str| {
        captures
            .name(name)
            .and_then(|value| value.as_str().parse::<i64>().ok())
    };
    let mut result = Vec::new();
    if let Some(height) = integer("height") {
        result.push(Value::cons(Value::symbol("height"), Value::Integer(height)));
    }
    if let Some(width) = integer("width") {
        result.push(Value::cons(Value::symbol("width"), Value::Integer(width)));
    }
    if let Some(top) = integer("y") {
        result.push(Value::cons(Value::symbol("top"), Value::Integer(top)));
    }
    if let Some(left) = integer("x") {
        result.push(Value::cons(Value::symbol("left"), Value::Integer(left)));
    }
    Value::list(result)
}

fn selected_frame_list(interp: &Interpreter) -> Value {
    Value::list(
        interp
            .frame_states
            .iter()
            .filter(|frame| interp.frame_is_live(frame.id))
            .map(|frame| frame_value(frame.id)),
    )
}

pub(super) fn window_system_unavailable() -> LispError {
    LispError::Signal("Window system is not in use or not initialized".into())
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "selected-frame" | "last-nonminibuffer-frame" => {
                need_args(name, args, 0)?;
                Ok(interp.selected_frame_value())
            }
            "old-selected-frame" => {
                need_args(name, args, 0)?;
                Ok(interp.old_selected_frame_value())
            }
            "framep" => {
                need_args(name, args, 1)?;
                Ok(match args[0] {
                    Value::Frame(id) if interp.frame_state(id).is_some() => Value::T,
                    _ => Value::Nil,
                })
            }
            "frame-live-p" => {
                need_args(name, args, 1)?;
                Ok(match args[0] {
                    Value::Frame(id) if interp.frame_is_live(id) => Value::T,
                    _ => Value::Nil,
                })
            }
            "frame-list" | "visible-frame-list" => {
                need_args(name, args, 0)?;
                Ok(selected_frame_list(interp))
            }
            "frame-visible-p" => {
                need_args(name, args, 1)?;
                decode_live_frame(interp, args.first(), false)?;
                Ok(Value::T)
            }
            "make-frame-visible" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                Ok(frame_value(id))
            }
            "make-frame-invisible" => {
                need_arg_range(name, args, 0, 2)?;
                decode_live_frame(interp, args.first(), true)?;
                if !args.get(1).is_some_and(Value::is_truthy) {
                    return Err(LispError::Signal(
                        "Attempt to make invisible the sole visible or iconified frame".into(),
                    ));
                }
                Ok(Value::Nil)
            }
            "iconify-frame" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Nil)
            }
            "frame-parameter" => {
                need_args(name, args, 2)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let parameter = args[1]
                    .as_symbol()
                    .map_err(|_| wrong_type_argument("symbolp", args[1].clone()))?;
                Ok(if interp.frame_is_live(id) {
                    frame_parameter_value(interp, id, parameter)
                } else {
                    Value::Nil
                })
            }
            "frame-parameters" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                Ok(if interp.frame_is_live(id) {
                    frame_parameters_value(interp, id)
                } else {
                    Value::Nil
                })
            }
            "modify-frame-parameters" => {
                need_args(name, args, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let parameters = args[1].to_vec()?;
                for entry in parameters.into_iter().rev() {
                    let (parameter, value) = entry
                        .cons_values()
                        .ok_or_else(|| wrong_type_argument("consp", entry.clone()))?;
                    let parameter = parameter
                        .as_symbol()
                        .map_err(|_| wrong_type_argument("symbolp", parameter.clone()))?
                        .to_string();
                    store_frame_parameter(interp, id, parameter, value);
                }
                Ok(Value::Nil)
            }
            "frame-char-width" | "frame-char-height" => {
                need_arg_range(name, args, 0, 1)?;
                decode_frame(interp, args.first(), true, false)?;
                Ok(Value::Integer(1))
            }
            "frame-native-width" | "frame-text-width" | "frame-text-cols" | "frame-total-cols" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let frame = interp
                    .frame_state(id)
                    .expect("a decoded frame identity must have state");
                Ok(Value::Integer(frame.width.max(1)))
            }
            "frame-native-height" | "frame-total-lines" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let frame = interp
                    .frame_state(id)
                    .expect("a decoded frame identity must have state");
                Ok(Value::Integer(frame.height.max(1)))
            }
            "frame-text-height" | "frame-text-lines" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let frame = interp
                    .frame_state(id)
                    .expect("a decoded frame identity must have state");
                Ok(Value::Integer(frame.text_height.max(1)))
            }
            "frame-internal-border-width"
            | "frame-fringe-width"
            | "frame-scroll-bar-width"
            | "frame-scroll-bar-height"
            | "frame-right-divider-width"
            | "frame-bottom-divider-width"
            | "frame-child-frame-border-width"
            | "tool-bar-pixel-width" => {
                need_arg_range(name, args, 0, 1)?;
                decode_frame(interp, args.first(), true, false)?;
                Ok(Value::Integer(0))
            }
            "set-frame-width" | "set-frame-height" => {
                need_arg_range(name, args, 2, 4)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let size = check_frame_size(&args[1])?;
                if interp
                    .frame_state(id)
                    .expect("decoded frame has state")
                    .tty_sized
                {
                    let frame = interp.frame_state(id).expect("decoded frame has state");
                    let (width, height) = if name == "set-frame-width" {
                        (size, frame.height)
                    } else {
                        (frame.width, size)
                    };
                    interp.resize_terminal_frames(id, width, height);
                } else if id == interp.selected_frame_id {
                    if name == "set-frame-width" {
                        interp.set_frame_width(size);
                    } else {
                        interp.set_frame_height(size);
                    }
                }
                Ok(Value::Nil)
            }
            "set-frame-size" => {
                need_arg_range(name, args, 3, 4)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let width = check_frame_size(&args[1])?;
                let height = check_frame_size(&args[2])?;
                if interp
                    .frame_state(id)
                    .expect("decoded frame has state")
                    .tty_sized
                {
                    interp.resize_terminal_frames(id, width, height);
                } else if id == interp.selected_frame_id {
                    interp.set_frame_width(width);
                    interp.set_frame_height(height);
                }
                Ok(Value::Nil)
            }
            "frame-position" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let frame = interp
                    .frame_state(id)
                    .expect("a decoded frame identity must have state");
                Ok(Value::cons(
                    Value::Integer(frame.left),
                    Value::Integer(frame.top),
                ))
            }
            "set-frame-position" => {
                need_args(name, args, 3)?;
                decode_live_frame(interp, args.first(), true)?;
                check_frame_size(&args[1])?;
                check_frame_size(&args[2])?;
                // TTY frames have no window-system offset hook.
                Ok(Value::T)
            }
            "frame-scale-factor" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::float(1.0))
            }
            "frame-parent" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Nil)
            }
            "frame-ancestor-p" => {
                need_args(name, args, 2)?;
                decode_live_frame(interp, args.first(), true)?;
                decode_live_frame(interp, args.get(1), true)?;
                Ok(Value::Nil)
            }
            "next-frame" | "previous-frame" => {
                need_arg_range(name, args, 0, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let terminal = interp
                    .frame_state(id)
                    .expect("decoded frame has state")
                    .terminal_id;
                let mut ids: Vec<_> = interp
                    .frame_states
                    .iter()
                    .filter(|frame| interp.frame_is_live(frame.id) && frame.terminal_id == terminal)
                    .map(|frame| frame.id)
                    .collect();
                if name == "previous-frame" {
                    ids.reverse();
                }
                if let Some(index) = ids.iter().position(|frame| *frame == id) {
                    ids.rotate_left(index);
                }
                let next = ids
                    .iter()
                    .cycle()
                    .skip(1)
                    .take(ids.len())
                    .copied()
                    .find(|candidate| {
                        frame_parameter_value(interp, *candidate, "no-other-frame").is_nil()
                            && match args.get(1) {
                                Some(Value::Record(window)) => {
                                    interp
                                        .frame_state(*candidate)
                                        .expect("decoded frame has state")
                                        .minibuffer_window_id
                                        == *window
                                        || interp.window_frame_id(*window) == Some(*candidate)
                                }
                                _ => true,
                            }
                    })
                    .unwrap_or(id);
                Ok(frame_value(next))
            }
            "select-frame" => {
                need_arg_range(name, args, 1, 2)?;
                let id = decode_live_frame(interp, args.first(), false)?;
                select_frame(interp, id, args.get(1).is_some_and(Value::is_truthy), env)?;
                Ok(frame_value(id))
            }
            "handle-switch-frame" => {
                need_args(name, args, 1)?;
                let frame = args[0]
                .to_vec()
                .ok()
                .filter(|items| {
                    matches!(items.first(), Some(Value::Symbol(event)) if event == "switch-frame")
                })
                .and_then(|items| items.get(1).cloned())
                .unwrap_or_else(|| args[0].clone());
                let id = decode_frame(interp, Some(&frame), false, false)?;
                if !interp.frame_is_live(id) {
                    return Ok(Value::Nil);
                }
                select_frame(interp, id, false, env)?;
                Ok(frame_value(id))
            }
            "make-terminal-frame" => {
                need_args(name, args, 1)?;
                make_terminal_frame(interp, &args[0], env)
            }
            "delete-frame" => {
                need_arg_range(name, args, 0, 2)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                delete_frame(
                    interp,
                    id,
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(1)
                        .is_some_and(|value| value.as_symbol().ok() == Some("noelisp")),
                    env,
                )?;
                Ok(Value::Nil)
            }
            "mouse-position" | "mouse-pixel-position" => {
                need_args(name, args, 0)?;
                Ok(Value::list([interp.selected_frame_value(), Value::Nil]))
            }
            "set-mouse-position" | "set-mouse-pixel-position" => {
                need_args(name, args, 3)?;
                let id = decode_live_frame(interp, args.first(), false)?;
                check_frame_size(&args[1])?;
                check_frame_size(&args[2])?;
                select_frame(interp, id, false, env)?;
                Ok(Value::Nil)
            }
            "raise-frame" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                select_frame(interp, id, false, env)?;
                Ok(Value::Nil)
            }
            "lower-frame" => {
                need_arg_range(name, args, 0, 1)?;
                decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Nil)
            }
            "redirect-frame-focus" => {
                need_arg_range(name, args, 1, 2)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                let focus = match args.get(1) {
                    None | Some(Value::Nil) => None,
                    Some(value) => Some(decode_live_frame(interp, Some(value), false)?),
                };
                interp
                    .frame_state_mut(id)
                    .expect("a decoded frame identity must have state")
                    .focus_frame_id = focus;
                Ok(Value::Nil)
            }
            "frame-focus" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                Ok(interp
                    .frame_state(id)
                    .and_then(|frame| frame.focus_frame_id)
                    .map(frame_value)
                    .unwrap_or(Value::Nil))
            }
            "x-focus-frame" => {
                need_arg_range(name, args, 1, 2)?;
                decode_live_frame(interp, args.first(), false)?;
                Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ))
            }
            "x-display-list" | "x-hide-tip" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "x-display-backing-store"
            | "x-display-color-cells"
            | "x-display-grayscale-p"
            | "x-display-mm-height"
            | "x-display-mm-width"
            | "x-display-pixel-height"
            | "x-display-pixel-width"
            | "x-display-planes"
            | "x-display-save-under"
            | "x-display-screens"
            | "x-display-visual-class"
            | "x-server-max-request-size"
            | "x-server-vendor"
            | "x-server-version"
            | "xw-display-color-p" => {
                need_arg_range(name, args, 0, 1)?;
                Err(window_system_unavailable())
            }
            "xw-color-defined-p" | "xw-color-values" => {
                need_arg_range(name, args, 1, 2)?;
                Err(window_system_unavailable())
            }
            "frame-after-make-frame" => {
                need_args(name, args, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                interp
                    .frame_state_mut(id)
                    .expect("a decoded frame identity must have state")
                    .after_make_frame = args[1].is_truthy();
                Ok(args[1].clone())
            }
            "frame-window-state-change" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                Ok(
                    if interp
                        .frame_state(id)
                        .is_some_and(|frame| frame.window_state_change)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "set-frame-window-state-change" => {
                need_arg_range(name, args, 0, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let state = args.get(1).is_some_and(Value::is_truthy);
                interp
                    .frame_state_mut(id)
                    .expect("a decoded frame identity must have state")
                    .window_state_change = state;
                Ok(if state { Value::T } else { Value::Nil })
            }
            "frame-pointer-visible-p" => {
                need_arg_range(name, args, 0, 1)?;
                let id = decode_frame(interp, args.first(), true, false)?;
                Ok(
                    if interp
                        .frame_state(id)
                        .is_some_and(|frame| frame.pointer_invisible)
                    {
                        Value::Nil
                    } else {
                        Value::T
                    },
                )
            }
            "frame--set-was-invisible" => {
                need_args(name, args, 2)?;
                let id = decode_live_frame(interp, args.first(), true)?;
                let state = args[1].is_truthy();
                interp
                    .frame_state_mut(id)
                    .expect("a decoded frame identity must have state")
                    .was_invisible = state;
                Ok(if state { Value::T } else { Value::Nil })
            }
            "reconsider-frame-fonts" => {
                need_args(name, args, 1)?;
                decode_live_frame(interp, args.first(), false)?;
                Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ))
            }
            "x-get-resource" => {
                need_arg_range(name, args, 2, 4)?;
                Err(window_system_unavailable())
            }
            "x-parse-geometry" => {
                need_args(name, args, 1)?;
                Ok(parse_geometry(&string_text(&args[0])?))
            }
        }
    }
);

fn make_terminal_frame(
    interp: &mut Interpreter,
    parameters: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let entries = parameters.to_vec()?;
    let parameter = |name: &str| {
        entries.iter().find_map(|entry| {
            let (key, value) = entry.cons_values()?;
            (key.as_symbol().ok() == Some(name)).then_some(value)
        })
    };
    let terminal_id = if let Some(value) = parameter("terminal") {
        interp
            .decode_terminal_id(&value)
            .ok_or_else(|| wrong_type_argument("terminal-live-p", value))?
    } else {
        // frame.c:get_future_frame_param: supplied alist, selected frame
        // parameter alist, then that frame's terminal. Non-strings mean nil.
        let future = |name: &str| {
            parameter(name)
                .or_else(|| interp.frame_parameter_override(name))
                .map(|value| string_like(&value).map(|s| s.text))
                .unwrap_or_else(|| {
                    let terminal = interp.terminal_state(interp.selected_terminal_id())?;
                    if name == "tty" {
                        terminal.kind.as_ref().map(|_| terminal.name.clone())
                    } else {
                        terminal.kind.clone()
                    }
                })
        };
        let tty = future("tty").unwrap_or_else(|| "/dev/tty".into());
        let kind =
            future("tty-type").ok_or_else(|| LispError::Signal("Unknown terminal type".into()))?;
        interp.open_tty_terminal(&tty, &kind)?
    };
    let id = interp.new_terminal_frame(terminal_id);
    let terminal = interp
        .terminal_state(terminal_id)
        .expect("decoded terminal has state");
    let tty = Value::string(&terminal.name);
    let kind = terminal
        .kind
        .as_deref()
        .map(Value::string)
        .unwrap_or(Value::Nil);
    // Fmodify_frame_parameters processes the alist in reverse so its first
    // occurrence wins. Terminal and minibuffer identity come from creation.
    for entry in entries.into_iter().rev() {
        let (key, value) = entry
            .cons_values()
            .ok_or_else(|| wrong_type_argument("consp", entry.clone()))?;
        let name = key
            .as_symbol()
            .map_err(|_| wrong_type_argument("symbolp", key.clone()))?;
        if !matches!(name, "minibuffer" | "tty" | "tty-type") {
            store_frame_parameter(interp, id, name.to_owned(), value);
        }
    }
    for (key, value) in [("minibuffer", Value::T), ("tty", tty), ("tty-type", kind)] {
        store_frame_parameter(interp, id, key.into(), value);
    }
    let _ = env;
    Ok(Value::Frame(id))
}

pub(super) fn select_frame(
    interp: &mut Interpreter,
    id: u64,
    norecord: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    if id == interp.selected_frame_id {
        return Ok(());
    }
    let window = interp
        .frame_state(id)
        .expect("decoded frame has state")
        .selected_window_id;
    super::call(
        interp,
        "select-window",
        &[
            Value::Record(window),
            if norecord { Value::T } else { Value::Nil },
        ],
        env,
    )?;
    Ok(())
}

pub(super) fn deletion_hook(
    interp: &mut Interpreter,
    name: &str,
    target: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    interp
        .safe_funcall(
            Value::symbol("run-hook-with-args"),
            &[Value::symbol(name), target],
            env,
        )
        .map(|_| ())
}

pub(super) fn delete_frame(
    interp: &mut Interpreter,
    id: u64,
    force: bool,
    noelisp: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    if !interp.frame_state(id).is_some_and(|frame| frame.live) {
        return Ok(());
    }
    let check = |interp: &Interpreter| {
        if !noelisp
            && !interp
                .frame_states
                .iter()
                .any(|frame| frame.id != id && interp.frame_is_live(frame.id))
        {
            Err(LispError::Signal(
                if force {
                    "Attempt to delete the only frame"
                } else {
                    "Attempt to delete the sole visible or iconified frame"
                }
                .into(),
            ))
        } else {
            Ok(())
        }
    };
    check(interp)?;
    if noelisp {
        interp.pending_funcalls.push(Value::list([
            Value::symbol("run-hook-with-args"),
            Value::symbol("delete-frame-functions"),
            Value::Frame(id),
        ]));
    } else {
        deletion_hook(interp, "delete-frame-functions", Value::Frame(id), env)?;
    }
    if !interp.frame_state(id).is_some_and(|frame| frame.live) {
        return Ok(());
    }
    check(interp)?;
    let terminal_id = interp
        .frame_state(id)
        .expect("decoded frame has state")
        .terminal_id;
    if id == interp.selected_frame_id {
        let replacement = interp
            .frame_states
            .iter()
            .find(|frame| {
                frame.id != id && interp.frame_is_live(frame.id) && frame.terminal_id == terminal_id
            })
            .or_else(|| {
                interp
                    .frame_states
                    .iter()
                    .find(|frame| frame.id != id && interp.frame_is_live(frame.id))
            })
            .map(|frame| frame.id);
        if let Some(replacement) = replacement {
            select_frame(interp, replacement, false, env)?;
        }
    }
    interp.retire_frame(id);
    if !noelisp
        && !interp
            .frame_states
            .iter()
            .any(|frame| frame.live && frame.terminal_id == terminal_id)
    {
        super::call(
            interp,
            "delete-terminal",
            &[Value::Terminal(terminal_id), Value::T],
            env,
        )?;
    }
    if noelisp {
        interp.pending_funcalls.push(Value::list([
            Value::symbol("run-hook-with-args"),
            Value::symbol("after-delete-frame-functions"),
            Value::Frame(id),
        ]));
    } else {
        deletion_hook(
            interp,
            "after-delete-frame-functions",
            Value::Frame(id),
            env,
        )?;
    }
    Ok(())
}
