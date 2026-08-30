use super::*;

impl Interpreter {
    pub(super) fn parse_params(&self, spec: &Value) -> Result<Vec<String>, LispError> {
        match spec {
            Value::Nil => Ok(Vec::new()),
            Value::Cons(_) => {
                let items = spec.to_vec()?;
                validate_lambda_list(spec, &items)?;
                items
                    .into_iter()
                    .map(|v| match v {
                        Value::Symbol(s) => Ok(s.to_string()),
                        _ => Err(invalid_function(spec.clone())),
                    })
                    .collect()
            }
            _ => Err(invalid_function(spec.clone())),
        }
    }

    pub(super) fn parse_source_params(
        &self,
        spec: &Value,
        env: &Env,
    ) -> Result<Vec<String>, LispError> {
        let items = spec.to_vec()?;
        let positioned = crate::lisp::primitives::symbols_with_pos_enabled(self, env);
        let normalized = items
            .into_iter()
            .map(|item| match item {
                Value::Symbol(_) => Ok(item),
                _ if positioned => crate::lisp::primitives::symbol_with_pos_parts(self, &item)
                    .map(|(symbol, _)| symbol)
                    .ok_or_else(|| invalid_function(spec.clone())),
                _ => Err(invalid_function(spec.clone())),
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_lambda_list(spec, &normalized)?;
        normalized
            .into_iter()
            .map(|item| {
                item.as_symbol()
                    .map(str::to_string)
                    .map_err(|_| invalid_function(spec.clone()))
            })
            .collect()
    }

    pub(super) fn sf_while(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        // GNU eval.c's Fwhile takes an unevalled `args' whose car is TEST;
        // `(while)' therefore signals wrong-number-of-arguments rather than
        // reading past the form.
        let Some(test) = items.get(1) else {
            return Err(LispError::WrongNumberOfArgs("while".into(), 0));
        };
        loop {
            let cond = self.eval(test, env)?;
            if cond.is_nil() {
                break;
            }
            self.sf_progn(&items[2..], env)?;
        }
        Ok(Value::Nil)
    }

    pub(super) fn same_frame_shape(left: &EnvFrame, right: &EnvFrame) -> bool {
        // Identified frames are the same frame exactly when their typed IDs
        // agree; a name-shape match between two unrelated `let's binding the
        // same variable must not alias them.
        match (Self::frame_identity(left), Self::frame_identity(right)) {
            (Some(left_id), Some(right_id)) => return left_id == right_id,
            (None, None) => {}
            _ => return false,
        }
        left.len() <= right.len()
            && left
                .iter()
                .zip(right.iter())
                .all(|((left_name, _), (right_name, _))| left_name == right_name)
    }

    pub(crate) fn frame_identity(frame: &EnvFrame) -> Option<i64> {
        frame.identity()
    }

    /// Push FRAME onto ENV with a fresh typed identity, so closure alignment
    /// can tell it apart from other frames that happen to bind the same names.
    pub(crate) fn push_marked_frame(env: &mut Env, frame: Vec<(String, Value)>) {
        env.push(EnvFrame::with_identity(frame, Self::fresh_frame_identity()));
    }

    pub(crate) fn fresh_frame_identity() -> i64 {
        use std::sync::atomic::{AtomicI64, Ordering};
        static NEXT_FRAME_IDENTITY: AtomicI64 = AtomicI64::new(1);
        NEXT_FRAME_IDENTITY.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn align_captured_frames(captured: &Env, current: &Env) -> Vec<Option<usize>> {
        let mut mapping = vec![None; captured.len()];
        let mut search_start = 0;
        for captured_index in 0..captured.len() {
            for (current_index, current_frame) in current.iter().enumerate().skip(search_start) {
                if Self::same_frame_shape(&captured[captured_index], current_frame) {
                    mapping[captured_index] = Some(current_index);
                    search_start = current_index + 1;
                    break;
                }
            }
        }
        mapping
    }

    pub(super) fn merge_lexical_lambda_env(
        current: &Env,
        captured: &Env,
        mapping: &[Option<usize>],
    ) -> Env {
        let mut merged = captured.clone();
        for (captured_index, current_index) in mapping.iter().enumerate() {
            if let Some(current_index) = current_index
                && captured_index < merged.len()
                && *current_index < current.len()
            {
                merged[captured_index] = current[*current_index].clone();
            }
        }
        merged
    }
}
