use super::*;

fn require_live_frame(interp: &Interpreter, frame: Option<&Value>) -> Result<(), LispError> {
    match frame {
        None | Some(Value::Nil) => Ok(()),
        Some(Value::Frame(id)) if interp.frame_is_live(*id) => Ok(()),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.clone())),
    }
}

fn require_any_frame(frame: Option<&Value>) -> Result<(), LispError> {
    match frame {
        None | Some(Value::Nil | Value::Frame(_)) => Ok(()),
        Some(frame) => Err(wrong_type_argument("framep", frame.clone())),
    }
}

fn window_system_unavailable() -> LispError {
    LispError::Signal("Window system is not in use or not initialized".into())
}

fn window_system_frame_required() -> LispError {
    LispError::Signal("Window system frame should be used".into())
}

fn require_fixnum(value: &Value) -> Result<(), LispError> {
    if matches!(value, Value::Integer(_)) {
        Ok(())
    } else {
        Err(wrong_type_argument("fixnump", value.clone()))
    }
}

fn validate_popup_position(interp: &Interpreter, position: &Value) -> Result<(), LispError> {
    if position == &Value::T {
        return Ok(());
    }
    let items = position
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", position.clone()))?;
    if let Some(Value::Cons(_)) = items.first() {
        let coordinates = items[0]
            .to_vec()
            .map_err(|_| wrong_type_argument("listp", items[0].clone()))?;
        if coordinates.len() >= 2 {
            require_fixnum(&coordinates[0])?;
            require_fixnum(&coordinates[1])?;
        }
        if let Some(window) = items.get(1) {
            let valid_window = matches!(window, Value::Frame(id) if interp.frame_is_live(*id))
                || matches!(window, Value::Record(id)
                        if interp.find_record(*id).is_some_and(|record|
                            record.kind == crate::lisp::eval::RecordKind::Window));
            if !valid_window {
                return Err(wrong_type_argument("windowp", window.clone()));
            }
        }
    }
    Ok(())
}

fn validate_popup_menu(interp: &Interpreter, menu: &Value) -> Result<(), LispError> {
    if is_keymap_value(interp, menu) {
        return Ok(());
    }
    let items = menu
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", menu.clone()))?;
    if items.iter().all(|item| is_keymap_value(interp, item)) && !items.is_empty() {
        return Ok(());
    }
    let title = items.first().cloned().unwrap_or(Value::Nil);
    if !title.is_string() {
        return Err(wrong_type_argument("stringp", title));
    }
    for pane in items.iter().skip(1) {
        let pane_items = pane
            .to_vec()
            .map_err(|_| wrong_type_argument("listp", pane.clone()))?;
        let pane_title = pane_items.first().cloned().unwrap_or(Value::Nil);
        if !pane_title.is_string() {
            return Err(wrong_type_argument("stringp", pane_title));
        }
    }
    Ok(())
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
    ) -> Result<Value, LispError> {
        match name {
            "menu-bar-menu-at-x-y" => {
                need_arg_range(name, args, 2, 3)?;
                require_any_frame(args.get(2))?;
                require_fixnum(&args[0])?;
                require_fixnum(&args[1])?;
                // Emaxx has no native-toolkit menu bar.  Its terminal frame does
                // not retain GNU's redisplay-time menu item geometry, so there is
                // no menu symbol to resolve at these coordinates.
                Ok(Value::Nil)
            }
            "x-begin-drag" => {
                need_arg_range(name, args, 1, 6)?;
                require_live_frame(interp, args.get(2))?;
                Err(window_system_frame_required())
            }
            "x-close-connection" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::Nil | Value::String(_) | Value::StringObject(_) => {}
                    Value::Frame(id) if interp.frame_is_live(*id) => {}
                    Value::Terminal(id) if *id == 0 && interp.terminal_live() => {}
                    terminal => {
                        return Err(wrong_type_argument("frame-live-p", terminal.clone()));
                    }
                }
                Err(window_system_unavailable())
            }
            "x-create-frame" => {
                need_args(name, args, 1)?;
                args[0]
                    .to_vec()
                    .map_err(|_| wrong_type_argument("listp", args[0].clone()))?;
                Err(window_system_unavailable())
            }
            "x-show-tip" => {
                need_arg_range(name, args, 1, 6)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0].clone()));
                }
                string_text(&args[0])?;
                require_live_frame(interp, args.get(1))?;
                Err(window_system_frame_required())
            }
            "x-file-dialog" => {
                need_arg_range(name, args, 2, 5)?;
                Err(window_system_unavailable())
            }
            "x-open-connection" => {
                need_arg_range(name, args, 1, 3)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0].clone()));
                }
                string_text(&args[0])?;
                // The Nextstep implementation ignores the optional resource and
                // must-succeed arguments.  Emaxx deliberately does not launch a
                // platform GUI application from its terminal runtime.
                Err(window_system_unavailable())
            }
            "x-popup-dialog" => {
                need_arg_range(name, args, 2, 3)?;
                match &args[0] {
                    Value::T => {}
                    Value::Frame(id) if interp.frame_is_live(*id) => {}
                    Value::Record(id)
                        if interp.find_record(*id).is_some_and(|record| {
                            record.kind == crate::lisp::eval::RecordKind::Window
                        }) => {}
                    Value::Cons(_) => {}
                    _ => return Err(wrong_type_argument("windowp", Value::Nil)),
                }
                let contents = args[1]
                    .to_vec()
                    .map_err(|_| wrong_type_argument("listp", args[1].clone()))?;
                let title = contents.first().cloned().unwrap_or(Value::Nil);
                if !title.is_string() {
                    return Err(wrong_type_argument("stringp", title));
                }
                // GNU's initial batch frame has no menu hook, so a well-formed
                // dialog is parsed but cannot produce a selection.
                Ok(Value::Nil)
            }
            "x-popup-menu" => {
                need_args(name, args, 2)?;
                // GNU preserves this obsolete cache-warming form as an immediate
                // no-op and deliberately does not inspect MENU.
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                validate_popup_position(interp, &args[0])?;
                validate_popup_menu(interp, &args[1])?;
                // Like GNU's initial batch frame, Emaxx has no native menu hook.
                Ok(Value::Nil)
            }
            "x-select-font" => {
                need_arg_range(name, args, 0, 2)?;
                require_live_frame(interp, args.first())?;
                Err(window_system_frame_required())
            }
        }
    }
);
