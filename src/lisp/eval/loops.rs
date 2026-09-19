use super::*;

impl Interpreter {
    pub(super) fn parse_params(&self, spec: &Value) -> Result<Vec<SymbolName>, LispError> {
        match spec {
            Value::Nil => Ok(Vec::new()),
            Value::Cons(_) => {
                let items = spec.to_vec()?;
                // eval.c:Fmake_interpreted_closure stores ARGS verbatim and
                // does not consult `symbols_with_pos_enabled`.  Normalize
                // its typed symbol-with-position storage only for Emaxx's
                // fast internal binding vector; the original Lisp list is
                // retained on LambdaValue as the public closure slot.
                let normalized = items
                    .into_iter()
                    .map(|item| match item {
                        Value::Symbol(_) => Ok(item),
                        _ => crate::lisp::primitives::symbol_with_pos_parts(self, &item)
                            .map(|(symbol, _)| symbol)
                            .ok_or_else(|| invalid_function(*spec)),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                validate_lambda_list(spec, &normalized)?;
                normalized
                    .into_iter()
                    .map(|item| match item {
                        Value::Symbol(name) => Ok(name),
                        Value::Nil => Ok("nil".into()),
                        Value::T => Ok("t".into()),
                        _ => Err(invalid_function(*spec)),
                    })
                    .collect()
            }
            _ => Err(invalid_function(*spec)),
        }
    }

    pub(super) fn parse_source_params(
        &self,
        spec: &Value,
        env: &Env,
    ) -> Result<Vec<SymbolName>, LispError> {
        let items = spec.to_vec()?;
        let positioned = crate::lisp::primitives::symbols_with_pos_enabled(self, env);
        let normalized = items
            .into_iter()
            .map(|item| match item {
                Value::Symbol(_) => Ok(item),
                _ if positioned => crate::lisp::primitives::symbol_with_pos_parts(self, &item)
                    .map(|(symbol, _)| symbol)
                    .ok_or_else(|| invalid_function(*spec)),
                _ => Err(invalid_function(*spec)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_lambda_list(spec, &normalized)?;
        normalized
            .into_iter()
            .map(|item| match item {
                Value::Symbol(name) => Ok(name),
                Value::Nil => Ok("nil".into()),
                Value::T => Ok("t".into()),
                _ => Err(invalid_function(*spec)),
            })
            .collect()
    }

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
