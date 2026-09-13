use super::*;

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "module-load" => {
                need_args(name, args, 1)?;
                crate::lisp::modules::load(interp, &args[0], env)
            }
        }
    }
);
