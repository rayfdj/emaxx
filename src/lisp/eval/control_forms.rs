use super::*;
use crate::lisp::reader;
impl Interpreter {
    pub(super) fn sf_quote(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        // GNU's quote returns its argument as-is, sharing structure.  The
        // emaxx reader leaves marker forms (circular labels, `#s(hash-table
        // ...)' literals) that must be resolved first, but marker-free
        // templates — the common case — are returned directly.  The verdict
        // is cached per template so hot code doesn't rescan large constants.
        if let Value::Cons(cell) = &items[1] {
            let key = crate::lisp::types::ConsCell::identity(cell);
            if self
                .plain_quote_templates
                .get(&key)
                .and_then(ConsMutationStamped::current)
                .is_some()
            {
                return Ok(items[1].clone());
            }
            if !reader::quote_template_needs_resolution(&items[1]) {
                if self.plain_quote_templates.len() >= (1 << 20) {
                    self.plain_quote_templates.clear();
                }
                self.plain_quote_templates.insert(
                    key,
                    ConsMutationStamped::new(
                        crate::lisp::types::ConsMutationSnapshot::tree(&items[1]),
                        items[1].clone(),
                    ),
                );
                return Ok(items[1].clone());
            }
        } else if !reader::quote_template_needs_resolution(&items[1]) {
            return Ok(items[1].clone());
        }
        self.materialize_read_object_literals(items[1].clone(), env)
    }

    pub(super) fn sf_if(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let Some(test_form) = items.get(1) else {
            return Ok(Value::Nil);
        };
        let cond = self.eval(test_form, env)?;
        if cond.is_truthy() {
            items
                .get(2)
                .map_or(Ok(Value::Nil), |then_form| self.eval(then_form, env))
        } else {
            // else branches
            self.sf_progn(items.get(3..).unwrap_or(&[]), env)
        }
    }

    pub(super) fn sf_cond(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for clause in items[1..].iter() {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let test = self.eval(&clause_items[0], env)?;
            if test.is_truthy() {
                if clause_items.len() == 1 {
                    return Ok(test);
                }
                return self.sf_progn(&clause_items[1..], env);
            }
        }
        Ok(Value::Nil)
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

    pub(super) fn sf_progn(&mut self, body: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        for expr in body {
            result = self.eval(expr, env)?;
        }
        Ok(result)
    }

    pub(super) fn sf_catch(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("catch".into(), 0));
        }
        let tag = self.eval(&items[1], env)?;
        let depth = env.len();
        self.active_catch_tags.push(tag.clone());
        let result = self.sf_progn(&items[2..], env);
        self.active_catch_tags.pop();
        // A non-local exit unwinds any binding frames pushed between the
        // catch and the throw, like GNU's unbind_to at the catch point.
        if env.len() > depth {
            env.truncate(depth);
        }
        match result {
            Ok(value) => Ok(value),
            Err(LispError::Throw(thrown_tag, value))
                if crate::lisp::primitives::values_eq_in_env(self, &thrown_tag, &tag, env) =>
            {
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Register/unregister a VM `Bpushcatch' tag so `throw' sees it as an
    /// active catch exactly like `sf_catch' frames.
    pub(crate) fn push_active_catch_tag(&mut self, tag: Value) {
        self.active_catch_tags.push(tag);
    }

    pub(crate) fn pop_active_catch_tag(&mut self) {
        self.active_catch_tags.pop();
    }

    pub(crate) fn throw_value(
        &mut self,
        tag: Value,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if self
            .active_catch_tags
            .iter()
            .rev()
            .any(|candidate| crate::lisp::primitives::values_eq_in_env(self, candidate, &tag, env))
        {
            Err(LispError::Throw(tag, value))
        } else {
            self.dispatch_handler_bindings(
                LispError::SignalValue(Value::list([Value::Symbol("no-catch".into()), tag, value])),
                env,
            )
        }
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

    /// GNU signals `setting-constant' when a let/let* binding variable is
    /// nil, t, or a keyword (subr-x's and-let* expands to such a let*).
    fn check_let_binding_name(name: &str) -> Result<(), LispError> {
        if matches!(name, "nil" | "t") || name.starts_with(':') {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("setting-constant".into()),
                Value::Symbol(name.to_string().into()),
            ])));
        }
        Ok(())
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
                    Self::check_let_binding_name(name)?;
                    if self.binding_is_dynamic(name, env) {
                        special_bindings.push((name.to_string(), Value::Nil));
                    } else {
                        frame.push((name.clone(), Value::Nil));
                    }
                }
                Value::Record(_)
                    if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                        && crate::lisp::primitives::symbol_with_pos_parts(self, binding)
                            .is_some() =>
                {
                    let name =
                        crate::lisp::primitives::checked_symbol_identity(self, binding, env)?;
                    Self::check_let_binding_name(&name)?;
                    if self.binding_is_dynamic(&name, env) {
                        special_bindings.push((name.to_string(), Value::Nil));
                    } else {
                        frame.push((name, Value::Nil));
                    }
                }
                Value::Cons(_) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad let binding".into()));
                    };
                    let name =
                        crate::lisp::primitives::checked_symbol_identity(self, name_value, env)?;
                    Self::check_let_binding_name(&name)?;
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    if self.binding_is_dynamic(&name, env) {
                        special_bindings.push((name.to_string(), val));
                    } else {
                        frame.push((name.clone(), Self::stored_value(val)));
                    }
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }

        let mut restores = Vec::new();
        for (name, value) in special_bindings {
            restores.push(self.bind_special_variable(&name, value, env)?);
        }
        // GNU evaluates all parallel initializers before saving the lexical
        // environment for the `let'.  Bare defvars in those initializers
        // therefore remain in the enclosing scope, while declarations made
        // after a real lexical binding are unwound with that binding.
        let lexical_scope_depth = env.len();
        let has_lexical_scope = !frame.is_empty();
        if has_lexical_scope {
            Self::push_marked_frame(env, frame);
        }
        let result = self.sf_progn(&items[2..], env);
        if has_lexical_scope {
            env.truncate(lexical_scope_depth);
        }
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
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
        let original_depth = env.len();
        let original_frame_identities = env.iter().map(Self::frame_identity).collect::<Vec<_>>();
        let mut lexical_binding_seen = false;
        let mut lexical_restore_depth = None;
        let mut restores = Vec::new();
        let setup = (|| -> Result<(), LispError> {
            for binding in &bindings {
                let (name, value) = match binding {
                    Value::Symbol(name) => {
                        Self::check_let_binding_name(name)?;
                        (name.clone(), Value::Nil)
                    }
                    Value::Record(_)
                        if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                            && crate::lisp::primitives::symbol_with_pos_parts(self, binding)
                                .is_some() =>
                    {
                        let name =
                            crate::lisp::primitives::checked_symbol_identity(self, binding, env)?;
                        Self::check_let_binding_name(&name)?;
                        (name, Value::Nil)
                    }
                    Value::Cons(_) => {
                        let parts = binding.to_vec()?;
                        let Some(name_value) = parts.first() else {
                            return Err(LispError::ReadError("bad let* binding".into()));
                        };
                        let name = crate::lisp::primitives::checked_symbol_identity(
                            self, name_value, env,
                        )?;
                        Self::check_let_binding_name(&name)?;
                        let value = if parts.len() > 1 {
                            self.eval(&parts[1], env)?
                        } else {
                            Value::Nil
                        };
                        (name, value)
                    }
                    _ => return Err(wrong_type_argument("listp", binding.clone())),
                };
                if self.binding_is_dynamic(&name, env) {
                    restores.push(self.bind_special_variable(&name, value, env)?);
                } else {
                    // FletX saves the original interpreter environment only
                    // when its first lexical binding is installed before an
                    // initializer has replaced that environment.  Preserve
                    // this unusual but observable GNU decision: a bare
                    // defvar in the first initializer can make the following
                    // lexical binding live for the rest of the enclosing
                    // interpreter scope.
                    if !lexical_binding_seen {
                        let original_environment_is_current = env.len() == original_depth
                            && env
                                .iter()
                                .map(Self::frame_identity)
                                .eq(original_frame_identities.iter().copied());
                        if original_environment_is_current {
                            lexical_restore_depth = Some(original_depth);
                        }
                        lexical_binding_seen = true;
                    }
                    Self::push_marked_frame(env, vec![(name, Self::stored_value(value))]);
                }
            }
            Ok(())
        })();

        let result = match setup {
            Ok(()) => self.sf_progn(&items[2..], env),
            Err(error) => Err(error),
        };
        if let Some(depth) = lexical_restore_depth {
            env.truncate(depth);
        }
        let mut restore_error = None;
        for restore in restores.into_iter().rev() {
            if let Err(error) = self.restore_special_binding(restore, env)
                && restore_error.is_none()
            {
                restore_error = Some(error);
            }
        }
        match result {
            Ok(value) => restore_error.map_or(Ok(value), Err),
            Err(error) => Err(error),
        }
    }
}
