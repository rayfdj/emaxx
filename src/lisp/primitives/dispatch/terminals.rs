use super::*;
use crate::lisp::types::TerminalRef;

fn require_live_terminal(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<TerminalRef, LispError> {
    let value = value.unwrap_or(&Value::Nil);
    interp
        .decode_terminal(value)
        .ok_or_else(|| wrong_type_argument("terminal-live-p", *value))
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
                        .terminal
                        .expect("live frame terminal"),
                ))
            }
            "terminal-live-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.decode_terminal(&args[0]).is_some() {
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
                        .filter(|terminal| terminal.borrow().live)
                        .map(|terminal| Value::Terminal(*terminal)),
                ))
            }
            "terminal-name" => {
                need_arg_range(name, args, 0, 1)?;
                let terminal = require_live_terminal(interp, args.first())?;
                Ok(Value::string(&terminal.borrow().name))
            }
            "terminal-parameters" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(require_live_terminal(interp, args.first())?.parameters())
            }
            "terminal-parameter" => {
                need_args(name, args, 2)?;
                if !args[1].is_symbol() {
                    return Err(wrong_type_argument("symbolp", args[1]));
                }
                Ok(require_live_terminal(interp, args.first())?.parameter(args[1]))
            }
            "set-terminal-parameter" => {
                need_args(name, args, 3)?;
                Ok(require_live_terminal(interp, args.first())?.set_parameter(args[1], args[2]))
            }
            "delete-terminal" => {
                need_arg_range(name, args, 0, 2)?;
                let target = args.first().cloned().unwrap_or(Value::Nil);
                let Some(terminal) = interp.decode_terminal(&target) else {
                    return Ok(Value::Nil);
                };
                if !args.get(1).is_some_and(Value::is_truthy)
                    && !interp
                        .terminals
                        .iter()
                        .any(|candidate| candidate.borrow().live && !candidate.ptr_eq(&terminal))
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
                if !terminal.borrow().live {
                    return Ok(Value::Nil);
                }
                // terminal.c:delete_terminal clears the terminal name before
                // deleting its frames, preventing recursive terminal deletion.
                interp.retire_terminal(terminal);
                let frames: Vec<_> = interp
                    .frame_states
                    .iter()
                    .filter(|frame| {
                        frame.live
                            && frame
                                .terminal
                                .is_some_and(|object| object.ptr_eq(&terminal))
                    })
                    .map(|frame| frame.id)
                    .collect();
                let result = frames.into_iter().try_for_each(|frame| {
                    super::frames::delete_frame(interp, frame, true, true, env).map(|_| ())
                });
                interp
                    .terminals
                    .retain(|candidate| !candidate.ptr_eq(&terminal));
                result.map(|_| Value::Nil)
            }
        }
    }
);
