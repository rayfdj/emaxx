use super::*;

impl Interpreter {
    pub(super) fn sf_while(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        // GNU eval.c's Fwhile takes an unevalled `args' whose car is TEST;
        // `(while)' therefore signals wrong-number-of-arguments rather than
        // reading past the form.
        let Some((test, body)) = super::core::list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("while".into(), 0));
        };
        loop {
            let cond = self.eval(&test, env)?;
            if cond.is_nil() {
                break;
            }
            self.progn_list(&body, env)?;
        }
        Ok(Value::Nil)
    }

    /// Flet's specbind of `internal-interpreter-environment': BINDINGS
    /// consed onto the current environment, pushed as a frame.
    pub(crate) fn push_bindings(env: &mut Env, bindings: Vec<(SymbolName, Value)>) {
        let outer = crate::lisp::types::current_environment_value(env);
        env.push(EnvFrame::bindings(bindings, &outer));
    }
}
