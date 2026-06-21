use super::*;
use crate::lisp::reader;

impl Interpreter {
    pub(super) fn sf_quote(&self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        reader::resolve_circular_read_syntax(items[1].clone())
    }

    pub(super) fn sf_if(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let Some(test_form) = items.get(1) else {
            return Ok(Value::Nil);
        };
        let tail_aliases =
            setcdr_tail_aliases(self, test_form, &Value::list(items[1..].to_vec()), env);
        let saved_aliases = snapshot_tail_alias_values(self, &tail_aliases, env);
        let cond_result = self.eval(test_form, env);
        let tail_became_improper = tail_aliases_became_improper(self, &tail_aliases, env);
        restore_tail_alias_values(self, &saved_aliases, env);
        let cond = cond_result?;
        if tail_became_improper {
            return Err(LispError::Void("if".into()));
        }
        if cond.is_truthy() {
            items
                .get(2)
                .map_or(Ok(Value::Nil), |then_form| self.eval(then_form, env))
        } else {
            // else branches
            self.sf_progn(items.get(3..).unwrap_or(&[]), env)
        }
    }

    pub(super) fn sf_when(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_truthy() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_if_let_star(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "if-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());
        for binding in bindings {
            let value = match binding {
                Value::Symbol(name) => self.lookup(&name, env)?,
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    match parts.as_slice() {
                        [expr] => self.eval(expr, env)?,
                        [Value::Symbol(name), expr] => {
                            let value = self.eval(expr, env)?;
                            if name != "_"
                                && let Some(frame) = env.last_mut()
                            {
                                frame.push((name.clone(), Self::stored_value(value.clone())));
                            }
                            value
                        }
                        _ => {
                            env.pop();
                            return Err(LispError::Signal("Invalid if-let* binding".into()));
                        }
                    }
                }
                _ => {
                    env.pop();
                    return Err(LispError::Signal("Invalid if-let* binding".into()));
                }
            };

            if !value.is_truthy() {
                env.pop();
                return self.sf_progn(items.get(3..).unwrap_or(&[]), env);
            }
        }

        let result = self.eval(&items[2], env);
        env.pop();
        result
    }

    pub(super) fn sf_if_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "if-let".into(),
                items.len().saturating_sub(1),
            ));
        }
        let spec = normalize_if_let_spec(&items[1])?;
        let rewritten = Value::list(
            std::iter::once(Value::symbol("if-let*"))
                .chain(std::iter::once(spec))
                .chain(std::iter::once(items[2].clone()))
                .chain(std::iter::once(forms_to_progn(
                    items.get(3..).unwrap_or(&[]),
                ))),
        );
        self.eval(&rewritten, env)
    }

    pub(super) fn sf_and_let_star(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "and-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());
        let mut last_value = Value::T;
        for binding in bindings {
            let value = match binding {
                Value::Symbol(name) => self.lookup(&name, env)?,
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    match parts.as_slice() {
                        [expr] => self.eval(expr, env)?,
                        [Value::Symbol(name), expr] => {
                            let value = self.eval(expr, env)?;
                            if name != "_"
                                && let Some(frame) = env.last_mut()
                            {
                                frame.push((name.clone(), Self::stored_value(value.clone())));
                            }
                            value
                        }
                        _ => {
                            env.pop();
                            return Err(LispError::Signal("Invalid and-let* binding".into()));
                        }
                    }
                }
                _ => {
                    env.pop();
                    return Err(LispError::Signal("Invalid and-let* binding".into()));
                }
            };

            if !value.is_truthy() {
                env.pop();
                return Ok(Value::Nil);
            }
            last_value = value;
        }

        let result = if items.len() > 2 {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(last_value)
        };
        env.pop();
        result
    }

    pub(super) fn sf_when_let(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "when-let".into(),
                items.len().saturating_sub(1),
            ));
        }
        let rewritten = Value::list([
            Value::symbol("if-let"),
            items[1].clone(),
            forms_to_progn(items.get(2..).unwrap_or(&[])),
        ]);
        self.eval(&rewritten, env)
    }

    pub(super) fn sf_when_let_star(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "when-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let rewritten = Value::list(
            std::iter::once(Value::symbol("if-let*"))
                .chain(std::iter::once(items[1].clone()))
                .chain(std::iter::once(Value::list(
                    std::iter::once(Value::symbol("progn")).chain(items[2..].iter().cloned()),
                )))
                .chain(std::iter::once(Value::Nil)),
        );
        self.eval(&rewritten, env)
    }

    pub(super) fn sf_unless(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_nil() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_bound_and_true_p(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() != 2 {
            return Err(LispError::WrongNumberOfArgs(
                "bound-and-true-p".into(),
                items.len().saturating_sub(1),
            ));
        }
        let symbol = quoted_symbol_name(&items[1])
            .or_else(|| items[1].as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::TypeError("symbol".into(), items[1].type_name()))?;
        Ok(self
            .lookup_var(&symbol, env)
            .filter(|value| value.is_truthy())
            .unwrap_or(Value::Nil))
    }

    pub(super) fn sf_cond(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for (clause_index, clause) in items[1..].iter().enumerate() {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let tail_aliases = setcdr_tail_aliases(
                self,
                &clause_items[0],
                &Value::list(items[clause_index + 1..].to_vec()),
                env,
            );
            let saved_aliases = snapshot_tail_alias_values(self, &tail_aliases, env);
            let test_result = self.eval(&clause_items[0], env);
            let tail_became_improper = tail_aliases_became_improper(self, &tail_aliases, env);
            restore_tail_alias_values(self, &saved_aliases, env);
            let test = test_result?;
            if tail_became_improper {
                return Err(LispError::Void("cond".into()));
            }
            if test.is_truthy() {
                if clause_items.len() == 1 {
                    return Ok(test);
                }
                return self.sf_progn(&clause_items[1..], env);
            }
        }
        Ok(Value::Nil)
    }

    pub(super) fn sf_pcase(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_pcase_like(items, env, false)
    }

    pub(super) fn sf_pcase_defmacro(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::WrongNumberOfArgs(
                "pcase-defmacro".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let params = self.parse_params(&items[2])?;
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };
        let body_start = if body_start < items.len() && is_function_declare_form(&items[body_start])
        {
            body_start + 1
        } else {
            body_start
        };
        let body = items[body_start..].to_vec();
        let expander_name = format!("{name}--pcase-macroexpander");
        let expander = Value::Lambda(params, body, shared_env(env.clone()));
        self.validate_function_binding(&expander_name, &expander)?;
        self.set_function_binding(&expander_name, Some(expander));
        self.put_symbol_property(&name, "pcase-macroexpander", Value::Symbol(expander_name));
        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_pcase_exhaustive(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.sf_pcase_like(items, env, true)
    }

    pub(super) fn sf_pcase_like(
        &mut self,
        items: &[Value],
        env: &mut Env,
        exhaustive: bool,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let value = self.eval(&items[1], env)?;
        for clause in &items[2..] {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let mut bindings = Vec::new();
            if pcase_pattern_bindings(self, env, &clause_items[0], &value, &mut bindings)? {
                env.push(bindings);
                let result = self.sf_progn(&clause_items[1..], env);
                env.pop();
                return result;
            }
        }
        if exhaustive {
            Err(LispError::Signal(
                "pcase-exhaustive: no matching clause".into(),
            ))
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_and(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::T;
        for item in &items[1..] {
            result = self.eval(item, env)?;
            if result.is_nil() {
                return Ok(Value::Nil);
            }
        }
        Ok(result)
    }

    pub(super) fn sf_or(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for item in &items[1..] {
            let val = self.eval(item, env)?;
            if val.is_truthy() {
                return Ok(val);
            }
        }
        Ok(Value::Nil)
    }

    pub(super) fn sf_not(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("not".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        Ok(if val.is_nil() { Value::T } else { Value::Nil })
    }

    pub(super) fn sf_progn(&mut self, body: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        for expr in body {
            result = self.eval(expr, env)?;
        }
        Ok(result)
    }

    pub(super) fn sf_atomic_change_group(
        &mut self,
        body: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer = self.buffer.clone();
        match self.sf_progn(body, env) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.buffer = saved_buffer;
                Err(error)
            }
        }
    }

    pub(super) fn sf_catch(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("catch".into(), 0));
        }
        let tag = self.eval(&items[1], env)?;
        match self.sf_progn(&items[2..], env) {
            Ok(value) => Ok(value),
            Err(LispError::Throw(thrown_tag, value)) if thrown_tag == tag => Ok(value),
            Err(error) => Err(error),
        }
    }

    pub(super) fn sf_throw(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 3 {
            return Err(LispError::WrongNumberOfArgs(
                "throw".into(),
                items.len().saturating_sub(1),
            ));
        }
        let tag = self.eval(&items[1], env)?;
        let value = self.eval(&items[2], env)?;
        Err(LispError::Throw(tag, value))
    }

    pub(super) fn sf_cl_return(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() > 2 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-return".into(),
                items.len().saturating_sub(1),
            ));
        }
        let value = if let Some(value) = items.get(1) {
            self.eval(value, env)?
        } else {
            Value::Nil
        };
        Err(LispError::Throw(
            Value::Symbol("--cl-block-nil--".into()),
            value,
        ))
    }

    pub(super) fn sf_cl_return_from(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if !(2..=3).contains(&items.len()) {
            return Err(LispError::WrongNumberOfArgs(
                "cl-return-from".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?;
        let value = if let Some(value) = items.get(2) {
            self.eval(value, env)?
        } else {
            Value::Nil
        };
        Err(LispError::Throw(
            Value::Symbol(format!("--cl-block-{name}--")),
            value,
        ))
    }

    pub(super) fn sf_prog1(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let result = self.eval(&items[1], env)?;
        let tracked_symbol = items[1].as_symbol().ok().map(str::to_string);
        for expr in &items[2..] {
            self.eval(expr, env)?;
        }
        if let Some(symbol) = tracked_symbol
            && crate::lisp::primitives::is_vector_like_value(self, &result)
            && let Ok(current) = self.lookup(&symbol, env)
            && crate::lisp::primitives::is_vector_like_value(self, &current)
        {
            return Ok(current);
        }
        Ok(result)
    }

    pub(super) fn sf_prog2(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "prog2".into(),
                items.len().saturating_sub(1),
            ));
        }
        self.eval(&items[1], env)?;
        let result = self.eval(&items[2], env)?;
        let tracked_symbol = items[2].as_symbol().ok().map(str::to_string);
        for expr in &items[3..] {
            self.eval(expr, env)?;
        }
        if let Some(symbol) = tracked_symbol
            && crate::lisp::primitives::is_vector_like_value(self, &result)
            && let Ok(current) = self.lookup(&symbol, env)
            && crate::lisp::primitives::is_vector_like_value(self, &current)
        {
            return Ok(current);
        }
        Ok(result)
    }

    pub(super) fn sf_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let mut frame = Vec::new();
        let mut special_bindings = Vec::new();

        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    if self.is_special_variable(name) {
                        special_bindings.push((name.clone(), Value::Nil));
                    } else {
                        frame.push((name.clone(), Value::Nil));
                    }
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad let binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    if self.is_special_variable(&name) {
                        special_bindings.push((name, val));
                    } else {
                        frame.push((name, Self::stored_value(val)));
                    }
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }

        let mut restores = Vec::new();
        for (name, value) in special_bindings {
            restores.push(self.bind_special_variable(&name, value, env)?);
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    pub(super) fn sf_letrec(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("letrec".into(), 0));
        }
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let mut names = Vec::with_capacity(bindings.len());
        let mut initializers = Vec::with_capacity(bindings.len());
        let mut frame = Vec::with_capacity(bindings.len());

        for binding in bindings {
            match binding {
                Value::Symbol(name) => {
                    frame.push((name.clone(), Value::Nil));
                    names.push(name);
                    initializers.push(None);
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad letrec binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    frame.push((name.clone(), Value::Nil));
                    names.push(name);
                    initializers.push(parts.get(1).cloned());
                }
                _ => return Err(wrong_type_argument("listp", binding)),
            }
        }

        env.push(frame);
        for (name, initializer) in names.iter().zip(initializers.iter()) {
            let value = if let Some(initializer) = initializer {
                self.eval(initializer, env)?
            } else {
                Value::Nil
            };
            let frame = env.last_mut().expect("letrec frame just pushed");
            if let Some((_, existing)) = frame.iter_mut().rev().find(|(key, _)| key == name) {
                *existing = Self::stored_value(value);
            }
        }
        {
            let frame = env.last().expect("letrec frame just pushed");
            patch_letrec_lambda_captures(frame, &names);
        }
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    pub(super) fn sf_letstar(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());
        let mut restores = Vec::new();

        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    if self.is_special_variable(name) {
                        restores.push(self.bind_special_variable(name, Value::Nil, env)?);
                    } else {
                        let frame = env.last_mut().expect("env frame just pushed");
                        frame.push((name.clone(), Value::Nil));
                    }
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad let* binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    if self.is_special_variable(&name) {
                        restores.push(self.bind_special_variable(&name, val, env)?);
                    } else {
                        let frame = env.last_mut().expect("env frame just pushed");
                        frame.push((name, Self::stored_value(val)));
                    }
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }

        let result = self.sf_progn(&items[2..], env);
        env.pop();
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    pub(super) fn sf_cl_progv(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-progv".into(),
                items.len().saturating_sub(1),
            ));
        }
        let symbols = self.eval(&items[1], env)?.to_vec()?;
        let values = self.eval(&items[2], env)?.to_vec()?;
        let mut restores = Vec::new();
        for (index, symbol) in symbols.iter().enumerate() {
            let name = symbol.as_symbol()?;
            let value = values.get(index).cloned().unwrap_or(Value::Nil);
            restores.push(self.bind_special_variable(name, value, env)?);
        }
        let result = self.sf_progn(&items[3..], env);
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    pub(super) fn sf_pcase_let(
        &mut self,
        items: &[Value],
        env: &mut Env,
        sequential: bool,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let bindings = items[1].to_vec()?;
        if sequential {
            env.push(Vec::new());
            for binding in &bindings {
                let parts = binding.to_vec()?;
                if parts.len() < 2 {
                    return Err(LispError::ReadError("bad pcase-let* binding".into()));
                }
                let value = self.eval(&parts[1], env)?;
                let mut frame_bindings = Vec::new();
                if !pcase_pattern_bindings_lenient_list(
                    self,
                    env,
                    &parts[0],
                    &value,
                    &mut frame_bindings,
                )? {
                    env.pop();
                    return Err(LispError::Signal("pcase-let*: no matching clause".into()));
                }
                let frame = env.last_mut().expect("env frame just pushed");
                frame.extend(
                    frame_bindings
                        .into_iter()
                        .map(|(name, value)| (name, Self::stored_value(value))),
                );
            }
            let result = self.sf_progn(&items[2..], env);
            env.pop();
            return result;
        }

        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::ReadError("bad pcase-let binding".into()));
            }
            let value = self.eval(&parts[1], env)?;
            if !pcase_pattern_bindings_lenient_list(self, env, &parts[0], &value, &mut frame)? {
                return Err(LispError::Signal("pcase-let: no matching clause".into()));
            }
        }
        env.push(
            frame
                .into_iter()
                .map(|(name, value)| (name, Self::stored_value(value)))
                .collect(),
        );
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    pub(super) fn sf_let_alist(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let alist = self.eval(&items[1], env)?;
        let mut frame = Vec::new();
        for entry in alist.to_vec().unwrap_or_default() {
            let Some((car, cdr)) = entry.cons_values() else {
                continue;
            };
            let Ok(symbol) = car.as_symbol() else {
                continue;
            };
            let value = match cdr {
                Value::Cons(value, tail) if matches!(*tail.borrow(), Value::Nil) => {
                    value.borrow().clone()
                }
                other => other,
            };
            frame.push((format!(".{symbol}"), Self::stored_value(value)));
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }
}

fn patch_letrec_lambda_captures(frame: &[(String, Value)], names: &[String]) {
    for (_, value) in frame {
        if let Value::Lambda(_, _, closure_env) = value {
            let mut captured_env = closure_env.borrow_mut();
            for captured_frame in captured_env.iter_mut() {
                for name in names {
                    let Some((_, replacement)) = frame.iter().find(|(key, _)| key == name) else {
                        continue;
                    };
                    if let Some((_, captured_value)) =
                        captured_frame.iter_mut().rev().find(|(key, _)| key == name)
                    {
                        *captured_value = replacement.clone();
                    }
                }
            }
        }
    }
}
