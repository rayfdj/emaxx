use super::*;

const TERMINAL_BUILTINS: &[&str] = &[
    "delete-terminal",
    "frame-terminal",
    "set-terminal-parameter",
    "terminal-list",
    "terminal-live-p",
    "terminal-name",
    "terminal-parameter",
    "terminal-parameters",
];

pub(super) fn handles(name: &str) -> bool {
    TERMINAL_BUILTINS.contains(&name)
}

fn live_terminal_designator(interp: &Interpreter, value: &Value) -> bool {
    interp.terminal_live()
        && match value {
            Value::Nil => true,
            Value::Terminal(id) => *id == 0,
            Value::Frame(id) => interp.frame_is_live(*id),
            _ => false,
        }
}

fn require_live_terminal(interp: &Interpreter, value: Option<&Value>) -> Result<(), LispError> {
    let value = value.unwrap_or(&Value::Nil);
    if live_terminal_designator(interp, value) {
        Ok(())
    } else {
        Err(wrong_type_argument("terminal-live-p", value.clone()))
    }
}

fn require_live_frame(interp: &Interpreter, value: Option<&Value>) -> Result<(), LispError> {
    let value = value.unwrap_or(&Value::Nil);
    if matches!(value, Value::Nil) || matches!(value, Value::Frame(id) if interp.frame_is_live(*id))
    {
        Ok(())
    } else {
        Err(wrong_type_argument("frame-live-p", value.clone()))
    }
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    match name {
        "frame-terminal" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_frame(interp, args.first())?;
            Ok(if interp.terminal_live() {
                interp.terminal_value()
            } else {
                Value::Nil
            })
        }
        "terminal-live-p" => {
            need_args(name, args, 1)?;
            Ok(if live_terminal_designator(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "terminal-list" => {
            need_args(name, args, 0)?;
            Ok(if interp.terminal_live() {
                Value::list([interp.terminal_value()])
            } else {
                Value::Nil
            })
        }
        "terminal-name" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Ok(Value::string("initial_terminal"))
        }
        "terminal-parameters" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Ok(interp.terminal_parameters())
        }
        "terminal-parameter" => {
            need_args(name, args, 2)?;
            if !args[1].is_symbol() {
                return Err(wrong_type_argument("symbolp", args[1].clone()));
            }
            require_live_terminal(interp, args.first())?;
            Ok(interp.terminal_parameter(&args[1]).unwrap_or(Value::Nil))
        }
        "set-terminal-parameter" => {
            need_args(name, args, 3)?;
            require_live_terminal(interp, args.first())?;
            Ok(interp.set_terminal_parameter(args[1].clone(), args[2].clone()))
        }
        "delete-terminal" => {
            need_arg_range(name, args, 0, 2)?;
            let target = args.first().cloned().unwrap_or(Value::Nil);
            if !live_terminal_designator(interp, &target) {
                return Ok(Value::Nil);
            }
            if !args.get(1).is_some_and(Value::is_truthy) {
                return Err(LispError::Signal(
                    "Attempt to delete the sole active display terminal".into(),
                ));
            }

            // GNU demotes ordinary hook errors here, but process-control
            // non-local exits remain authoritative.
            match super::call(
                interp,
                "run-hook-with-args",
                &[Value::symbol("delete-terminal-functions"), target],
                env,
            ) {
                Ok(_) => {}
                Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => {
                    return Err(error);
                }
                Err(_) => {}
            }
            interp.delete_terminal_state();
            Ok(Value::Nil)
        }
        _ => unreachable!("terminal dispatcher called for unsupported primitive"),
    }
}
