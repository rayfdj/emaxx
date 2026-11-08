use super::*;
use crate::lisp::types::Kind;

fn require_live_frame(interp: &Interpreter, frame: Option<&Value>) -> Result<(), LispError> {
    match frame.map(|v| v.kind()) {
        None | Some(Kind::Nil) => Ok(()),
        Some(Kind::Frame(id)) if interp.frame_is_live(id) => Ok(()),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.value())),
    }
}

fn require_any_frame(frame: Option<&Value>) -> Result<(), LispError> {
    match frame.map(|v| v.kind()) {
        None | Some(Kind::Nil | Kind::Frame(_)) => Ok(()),
        Some(frame) => Err(wrong_type_argument("framep", frame.value())),
    }
}

/// The non-X hosts' window-system boundary.
#[cfg(not(target_os = "linux"))]
fn window_system_unavailable() -> LispError {
    LispError::Signal("Window system is not in use or not initialized".into())
}

fn window_system_frame_required() -> LispError {
    LispError::Signal("Window system frame should be used".into())
}

/// CHECK_SYMBOL of an optional argument (nil when absent).
fn check_symbol(value: Option<&Value>) -> Result<(), LispError> {
    match value {
        Some(value) if !value.is_symbol() => Err(wrong_type_argument("symbolp", *value)),
        _ => Ok(()),
    }
}

/// xselect.c's frame_for_x_selection: the X frame a selection call uses,
/// of which Emaxx has none; only a terminal designator is decoded first.
fn frame_for_x_selection(interp: &Interpreter, object: Option<&Value>) -> Result<(), LispError> {
    match object {
        Some(object) if matches!(object.kind(), Kind::Terminal(_)) => interp
            .decode_terminal(object)
            .map(|_| ())
            .ok_or_else(|| wrong_type_argument("terminal-live-p", *object)),
        _ => Ok(()),
    }
}

fn require_fixnum(value: &Value) -> Result<(), LispError> {
    if matches!(value.kind(), Kind::Integer(_)) {
        Ok(())
    } else {
        Err(wrong_type_argument("fixnump", *value))
    }
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "menu-bar-menu-at-x-y" => {
                need_arg_range(name, args, 2, 3)?;
                require_any_frame(args.get(2))?;
                // menu.c reads X and Y with XFIXNUM unchecked, which crashes
                // GNU on what is no fixnum; they are checked here instead.
                require_fixnum(&args[0])?;
                require_fixnum(&args[1])?;
                crate::lisp::primitives::menu::menu_bar_menu_at_x_y(
                    interp,
                    args[0].as_integer()?,
                    args[1].as_integer()?,
                    args.get(2).copied().unwrap_or(Value::Nil),
                )
            }
            "x-begin-drag" => {
                need_arg_range(name, args, 1, 6)?;
                require_live_frame(interp, args.get(2))?;
                Err(window_system_frame_required())
            }
            "x-change-window-property" => {
                need_arg_range(name, args, 2, 7)?;
                // xfns.c decodes a live window-system frame before reading
                // PROP, VALUE or FORMAT. The current frontend owns terminal
                // frames, which GNU rejects at this same boundary.
                super::frames::decode_live_frame(interp, args.get(2), true)?;
                Err(window_system_frame_required())
            }
            "x-window-property" | "x-window-property-attributes" | "x-delete-window-property" => {
                need_arg_range(
                    name,
                    args,
                    1,
                    if name == "x-window-property" { 6 } else { 3 },
                )?;
                super::frames::decode_live_frame(interp, args.get(1), true)?;
                Err(window_system_frame_required())
            }
            "x-close-connection" => {
                need_args(name, args, 1)?;
                Err(super::frames::check_x_display_info(
                    interp,
                    args.first(),
                    env,
                ))
            }
            "x-send-client-message" => {
                need_args(name, args, 6)?;
                Err(super::frames::check_x_display_info(
                    interp,
                    args.first(),
                    env,
                ))
            }
            "x-get-atom-name" | "x-register-dnd-atom" => {
                need_arg_range(name, args, 1, 2)?;
                Err(super::frames::window_system_frame_error(
                    interp,
                    args.get(1),
                ))
            }
            // xselect.c: no frame is an X frame, so frame_for_x_selection
            // finds none (after decoding a terminal designator).
            "x-selection-exists-p" | "x-selection-owner-p" => {
                need_arg_range(name, args, 0, 2)?;
                frame_for_x_selection(interp, args.get(1))?;
                check_symbol(args.first())?;
                Ok(Value::Nil)
            }
            "x-disown-selection-internal" => {
                need_arg_range(name, args, 1, 3)?;
                frame_for_x_selection(interp, args.get(2))?;
                Ok(Value::Nil)
            }
            "x-get-selection-internal" => {
                need_arg_range(name, args, 2, 4)?;
                frame_for_x_selection(interp, args.get(3))?;
                check_symbol(args.first())?;
                check_symbol(args.get(1))?;
                if args[1] == Value::symbol("MULTIPLE") {
                    return Err(LispError::Signal(
                        "Retrieving MULTIPLE selections is currently unimplemented".into(),
                    ));
                }
                Err(LispError::Signal(
                    "X selection unavailable for this frame".into(),
                ))
            }
            "x-own-selection-internal" => {
                need_arg_range(name, args, 2, 3)?;
                Err(LispError::Signal(
                    "X selection unavailable for this frame".into(),
                ))
            }
            // VALUE is (NAME SELECTION-VALUE TIMESTAMP FRAME . _).
            "x-get-local-selection" => {
                need_arg_range(name, args, 0, 2)?;
                check_symbol(args.get(1))?;
                let value = args.first().copied().unwrap_or(Value::Nil);
                let check_cons = |value: Value| {
                    value
                        .cons_values()
                        .ok_or_else(|| wrong_type_argument("consp", value))
                };
                let (selection_name, rest) = check_cons(value)?;
                let (_, rest) = check_cons(rest)?;
                let (timestamp, rest) = check_cons(rest)?;
                let (frame, _) = check_cons(rest)?;
                check_symbol(Some(&selection_name))?;
                crate::lisp::primitives::system::cons_to_unsigned(&timestamp, u64::MAX)?;
                Err(super::frames::window_system_frame_error(
                    interp,
                    Some(&frame),
                ))
            }
            "x-create-frame" => {
                need_args(name, args, 1)?;
                let parms = super::call(interp, "copy-alist", &[args[0]], env)?;
                #[cfg(target_os = "linux")]
                {
                    // xfns.c: the resource name starts as the invocation
                    // name, and the display is the `terminal', else the
                    // `display' parameter (gui_display_get_arg: PARMS, then
                    // `default-frame-alist'), checked as an X display.
                    let invocation_name = interp
                        .lookup_var_key(cached_symbol!("invocation-name"), env)
                        .unwrap_or(Value::Nil);
                    interp.set_c_variable("x-resource-name", invocation_name);
                    let defaults = interp
                        .lookup_var_key(cached_symbol!("default-frame-alist"), env)
                        .unwrap_or(Value::Nil);
                    let mut display = None;
                    for parameter in ["terminal", "display"] {
                        for alist in [parms, defaults] {
                            let found = super::call(
                                interp,
                                "assq",
                                &[Value::symbol(parameter), alist],
                                env,
                            )?;
                            if let Some((_, value)) = found.cons_values() {
                                display = Some(value);
                                break;
                            }
                        }
                        if display.is_some() {
                            break;
                        }
                    }
                    let display = display.unwrap_or(Value::Nil);
                    Err(super::frames::check_x_display_info(
                        interp,
                        Some(&display),
                        env,
                    ))
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let _ = parms;
                    Err(window_system_unavailable())
                }
            }
            "x-show-tip" => {
                need_arg_range(name, args, 1, 6)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0]));
                }
                string_text(&args[0])?;
                require_live_frame(interp, args.get(1))?;
                Err(window_system_frame_required())
            }
            "x-open-connection" => {
                need_arg_range(name, args, 1, 3)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0]));
                }
                string_text(&args[0])?;
                #[cfg(target_os = "linux")]
                {
                    // xfns.c: the resource string is checked, the resource
                    // name validated, and x_term_init cannot open the
                    // display (Emaxx opens none).
                    if let Some(xrm) = args.get(1).filter(|xrm| !xrm.is_nil())
                        && !xrm.is_string()
                    {
                        return Err(wrong_type_argument("stringp", *xrm));
                    }
                    super::frames::validate_x_resource_name(interp, env)?;
                    Err(super::frames::display_cannot_be_opened(interp, &args[0]))
                }
                // The Nextstep implementation ignores the optional resource and
                // must-succeed arguments.  Emaxx deliberately does not launch a
                // platform GUI application from its terminal runtime.
                #[cfg(not(target_os = "linux"))]
                Err(window_system_unavailable())
            }
            "x-popup-dialog" => {
                need_arg_range(name, args, 2, 3)?;
                crate::lisp::primitives::menu::x_popup_dialog(interp, args[0], args[1], env)
            }
            "x-popup-menu" => {
                need_args(name, args, 2)?;
                super::init_raw_keybuf_count(interp);
                crate::lisp::primitives::menu::x_popup_menu_1(interp, args[0], args[1], env)
            }
            "x-select-font" => {
                need_arg_range(name, args, 0, 2)?;
                require_live_frame(interp, args.first())?;
                Err(window_system_frame_required())
            }
        }
    }
);
