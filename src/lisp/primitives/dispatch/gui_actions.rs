use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "x-create-frame" | "x-file-dialog" | "x-select-font" | "x-show-tip"
    )
}

fn require_live_frame(interp: &Interpreter, frame: Option<&Value>) -> Result<(), LispError> {
    match frame {
        None | Some(Value::Nil) => Ok(()),
        Some(Value::Frame(id)) if interp.frame_is_live(*id) => Ok(()),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.clone())),
    }
}

fn window_system_unavailable() -> LispError {
    LispError::Signal("Window system is not in use or not initialized".into())
}

fn window_system_frame_required() -> LispError {
    LispError::Signal("Window system frame should be used".into())
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    match name {
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
        "x-select-font" => {
            need_arg_range(name, args, 0, 2)?;
            require_live_frame(interp, args.first())?;
            Err(window_system_frame_required())
        }
        _ => unreachable!("unhandled graphical action builtin {name}"),
    }
}
