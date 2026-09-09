use super::*;

fn require_live_terminal(interp: &Interpreter, value: Option<&Value>) -> Result<u64, LispError> {
    let value = value.unwrap_or(&Value::Nil);
    interp
        .decode_terminal_id(value)
        .ok_or_else(|| wrong_type_argument("terminal-live-p", value.clone()))
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "frame-terminal" => {
                need_arg_range(name, args, 0, 1)?;
                let frame = super::frames::decode_live_frame(interp, args.first(), true)?;
                Ok(Value::Terminal(
                    interp
                        .frame_state(frame)
                        .expect("decoded frame has state")
                        .terminal_id,
                ))
            }
            "terminal-live-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.decode_terminal_id(&args[0]).is_some() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "terminal-list" => {
                need_args(name, args, 0)?;
                Ok(Value::list(
                    interp
                        .terminals
                        .iter()
                        .rev()
                        .filter(|terminal| terminal.live)
                        .map(|terminal| Value::Terminal(terminal.id)),
                ))
            }
            "terminal-name" => {
                need_arg_range(name, args, 0, 1)?;
                let id = require_live_terminal(interp, args.first())?;
                Ok(Value::string(
                    &interp
                        .terminal_state(id)
                        .expect("decoded terminal has state")
                        .name,
                ))
            }
            "terminal-parameters" => {
                need_arg_range(name, args, 0, 1)?;
                let id = require_live_terminal(interp, args.first())?;
                Ok(interp.terminal_parameters_on(id))
            }
            "terminal-parameter" => {
                need_args(name, args, 2)?;
                if !args[1].is_symbol() {
                    return Err(wrong_type_argument("symbolp", args[1].clone()));
                }
                let id = require_live_terminal(interp, args.first())?;
                Ok(interp
                    .terminal_state(id)
                    .expect("decoded terminal has state")
                    .parameters
                    .iter()
                    .rfind(|(key, _)| key == &args[1])
                    .map(|(_, value)| value.clone())
                    .unwrap_or(Value::Nil))
            }
            "set-terminal-parameter" => {
                need_args(name, args, 3)?;
                let id = require_live_terminal(interp, args.first())?;
                Ok(interp.set_terminal_parameter_on(id, args[1].clone(), args[2].clone()))
            }
            "delete-terminal" => {
                need_arg_range(name, args, 0, 2)?;
                let target = args.first().cloned().unwrap_or(Value::Nil);
                let Some(id) = interp.decode_terminal_id(&target) else {
                    return Ok(Value::Nil);
                };
                if !args.get(1).is_some_and(Value::is_truthy)
                    && !interp
                        .terminals
                        .iter()
                        .any(|terminal| terminal.live && terminal.id != id)
                {
                    return Err(LispError::Signal(
                        "Attempt to delete the sole active display terminal".into(),
                    ));
                }
                if args
                    .get(1)
                    .is_some_and(|value| value.as_symbol().ok() == Some("noelisp"))
                {
                    interp.pending_funcalls.push(Value::list([
                        Value::symbol("run-hook-with-args"),
                        Value::symbol("delete-terminal-functions"),
                        target,
                    ]));
                } else {
                    super::frames::deletion_hook(interp, "delete-terminal-functions", target, env)?;
                }
                if !interp
                    .terminal_state(id)
                    .is_some_and(|terminal| terminal.live)
                {
                    return Ok(Value::Nil);
                }
                // terminal.c:delete_terminal clears the terminal name before
                // deleting its frames, preventing recursive terminal deletion.
                interp.retire_terminal(id);
                let frames: Vec<_> = interp
                    .frame_states
                    .iter()
                    .filter(|frame| frame.live && frame.terminal_id == id)
                    .map(|frame| frame.id)
                    .collect();
                for frame in frames {
                    super::frames::delete_frame(interp, frame, true, true, env)?;
                }
                Ok(Value::Nil)
            }
        }
    }
);
