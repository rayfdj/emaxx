use super::core::{list_cdr, list_forms, list_next};
use super::*;
use crate::lisp::reader;
impl Interpreter {
    pub(super) fn sf_quote(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let Some((template, _)) = list_next(args) else {
            return Ok(Value::Nil);
        };
        // GNU's quote returns its argument as-is, sharing structure.  The
        // emaxx reader leaves marker forms (circular labels, `#s(hash-table
        // ...)' literals) that must be resolved first, but marker-free
        // templates — the common case — are returned directly.  The verdict
        // is cached per template so hot code doesn't rescan large constants.
        if let Value::Cons(cell) = &template {
            let key = crate::lisp::types::ConsCell::identity(cell);
            if self
                .plain_quote_templates
                .get(&key)
                .and_then(ConsMutationStamped::current)
                .is_some()
            {
                return Ok(template);
            }
            if !reader::quote_template_needs_resolution(&template) {
                if self.plain_quote_templates.len() >= (1 << 20) {
                    self.plain_quote_templates.clear();
                }
                self.plain_quote_templates.insert(
                    key,
                    ConsMutationStamped::new(
                        crate::lisp::types::ConsMutationSnapshot::tree(&template),
                        template.clone(),
                    ),
                );
                return Ok(template);
            }
        } else if !reader::quote_template_needs_resolution(&template) {
            return Ok(template);
        }
        self.materialize_read_object_literals(template, env)
    }

    pub(super) fn sf_if(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        // Fif: eval_sub (XCAR (args)); then Fcar (XCDR (args)) or Fprogn
        // (Fcdr (XCDR (args))).
        let Some((test_form, rest)) = list_next(args) else {
            return Ok(Value::Nil);
        };
        let cond = self.eval(&test_form, env)?;
        if cond.is_truthy() {
            match list_next(&rest) {
                Some((then_form, _)) => self.eval(&then_form, env),
                None => Ok(Value::Nil),
            }
        } else {
            self.progn_list(&list_cdr(&rest), env)
        }
    }

    pub(super) fn sf_cond(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        for clause in list_forms(args) {
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

    pub(super) fn sf_and(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::T;
        for form in list_forms(args) {
            result = self.eval(&form, env)?;
            if result.is_nil() {
                return Ok(Value::Nil);
            }
        }
        Ok(result)
    }

    pub(super) fn sf_or(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        for form in list_forms(args) {
            let val = self.eval(&form, env)?;
            if val.is_truthy() {
                return Ok(val);
            }
        }
        Ok(Value::Nil)
    }

    /// Fprogn over a body list: each form evaluated in place, the last
    /// one's value returned.
    pub(super) fn progn_list(&mut self, body: &Value, env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        for form in list_forms(body) {
            result = self.eval(&form, env)?;
        }
        Ok(result)
    }

    pub(super) fn sf_progn(&mut self, body: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        for expr in body {
            result = self.eval(expr, env)?;
        }
        Ok(result)
    }

    pub(super) fn sf_catch(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let Some((tag_form, body)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("catch".into(), 0));
        };
        let tag = self.eval(&tag_form, env)?;
        let depth = env.len();
        self.active_catch_tags.push(tag.clone());
        let result = self.progn_list(&body, env);
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
            .active_handlers
            .iter()
            .any(|handler| matches!(handler, ActiveHandler::Module))
            || self.active_catch_tags.iter().rev().any(|candidate| {
                crate::lisp::primitives::values_eq_in_env(self, candidate, &tag, env)
            })
        {
            Err(LispError::Throw(tag, value))
        } else {
            self.dispatch_handler_bindings(
                LispError::SignalValue(Value::list([Value::Symbol("no-catch".into()), tag, value])),
                env,
            )
        }
    }

    pub(super) fn sf_prog1(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let Some((first, rest)) = list_next(args) else {
            return Ok(Value::Nil);
        };
        let result = self.eval(&first, env)?;
        let tracked_symbol = first.as_symbol().ok().map(str::to_string);
        for form in list_forms(&rest) {
            self.eval(&form, env)?;
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

    /// eval.c's reading of one list element of a `let' varlist: the
    /// symbol and its value form.  A second value form signals as
    /// signal_error does (`error', the message, the element's own
    /// elements); a dotted element signals listp on its tail, as Fcar of
    /// Fcdr does.
    fn let_binding_parts(binding: &Value) -> Result<(Value, Option<Value>), LispError> {
        let Value::Cons(cell) = binding else {
            return Err(wrong_type_argument("listp", binding.clone()));
        };
        let name = cell.car.borrow().clone();
        let rest = cell.cdr.borrow().clone();
        match &rest {
            Value::Nil => Ok((name, None)),
            Value::Cons(second) => {
                if !second.cdr.borrow().is_nil() {
                    return Err(LispError::SignalValue(Value::cons(
                        Value::symbol("error"),
                        Value::cons(
                            Value::String(
                                String::from("`let' bindings can have only one value-form").into(),
                            ),
                            binding.clone(),
                        ),
                    )));
                }
                Ok((name, Some(second.car.borrow().clone())))
            }
            other => Err(wrong_type_argument("listp", other.clone())),
        }
    }

    /// The next element of a varlist walked in place (FOR_EACH_TAIL):
    /// the element and the rest, None at the end; a dotted tail signals
    /// listp with the whole varlist, as list_length and CHECK_LIST_END do.
    fn next_let_binding(
        tail: &Value,
        varlist: &Value,
    ) -> Result<Option<(Value, Value)>, LispError> {
        match tail {
            Value::Nil => Ok(None),
            Value::Cons(cell) => Ok(Some((cell.car.borrow().clone(), cell.cdr.borrow().clone()))),
            _ => Err(wrong_type_argument("listp", varlist.clone())),
        }
    }

    pub(super) fn sf_let(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        // eval.c Flet: list_length (varlist) -- a vector or any other
        // non-list signals wrong-type-argument listp (a vector read as a
        // sequence bound its elements to nil before).  The varlist and
        // each element are read in place; a vector of copies of every
        // binding per evaluation was a share of every interpreted `let'.
        let Some((varlist, body)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("let".into(), 0));
        };
        if is_vector_literal(&varlist) || !matches!(varlist, Value::Nil | Value::Cons(_)) {
            return Err(wrong_type_argument("listp", varlist.clone()));
        }
        let mut frame = Vec::new();
        let mut special_bindings = Vec::new();

        let mut tail = varlist.clone();
        while let Some((binding, next)) = Self::next_let_binding(&tail, &varlist)? {
            tail = next;
            match &binding {
                Value::Symbol(name) => {
                    Self::check_let_binding_name(name)?;
                    if self.binding_is_dynamic_symbol(name, env) {
                        special_bindings.push((name.clone(), Value::Nil));
                    } else {
                        frame.push((name.clone(), Value::Nil));
                    }
                }
                Value::Record(_)
                    if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                        && crate::lisp::primitives::symbol_with_pos_parts(self, &binding)
                            .is_some() =>
                {
                    let name =
                        crate::lisp::primitives::checked_symbol_identity(self, &binding, env)?;
                    Self::check_let_binding_name(&name)?;
                    if self.binding_is_dynamic_symbol(&name, env) {
                        special_bindings.push((name, Value::Nil));
                    } else {
                        frame.push((name, Value::Nil));
                    }
                }
                Value::Cons(_) => {
                    let (name_value, init) = Self::let_binding_parts(&binding)?;
                    let name =
                        crate::lisp::primitives::checked_symbol_identity(self, &name_value, env)?;
                    Self::check_let_binding_name(&name)?;
                    let val = match init {
                        Some(form) => self.eval(&form, env)?,
                        None => Value::Nil,
                    };
                    if self.binding_is_dynamic_symbol(&name, env) {
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
            restores.push(self.bind_special_symbol(&name, value, env)?);
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
        let result = self.progn_list(&body, env);
        if has_lexical_scope {
            env.truncate(lexical_scope_depth);
        }
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    pub(super) fn sf_letstar(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        // eval.c FletX: FOR_EACH_TAIL (varlist) -- a non-list signals
        // wrong-type-argument listp.
        let Some((varlist, body)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("let*".into(), 0));
        };
        if is_vector_literal(&varlist) || !matches!(varlist, Value::Nil | Value::Cons(_)) {
            return Err(wrong_type_argument("listp", varlist.clone()));
        }
        let original_depth = env.len();
        // The environment at entry, by its depth and its innermost frame's
        // identity (a marked frame's identity is unique; only an unmarked
        // innermost frame needs every identity compared): a vector of
        // every frame's identity was collected per `let*' before.
        let original_innermost = env.last().map(Self::frame_identity);
        let original_frame_identities = if matches!(original_innermost, Some(None)) {
            Some(env.iter().map(Self::frame_identity).collect::<Vec<_>>())
        } else {
            None
        };
        let mut lexical_binding_seen = false;
        let mut lexical_restore_depth = None;
        let mut restores = Vec::new();
        let setup = (|| -> Result<(), LispError> {
            // The varlist and each element read in place (FletX's
            // FOR_EACH_TAIL).
            let mut tail = varlist.clone();
            while let Some((binding, next)) = Self::next_let_binding(&tail, &varlist)? {
                tail = next;
                let (name, value) = match &binding {
                    Value::Symbol(name) => {
                        Self::check_let_binding_name(name)?;
                        (name.clone(), Value::Nil)
                    }
                    Value::Record(_)
                        if crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                            && crate::lisp::primitives::symbol_with_pos_parts(self, &binding)
                                .is_some() =>
                    {
                        let name =
                            crate::lisp::primitives::checked_symbol_identity(self, &binding, env)?;
                        Self::check_let_binding_name(&name)?;
                        (name, Value::Nil)
                    }
                    Value::Cons(_) => {
                        let (name_value, init) = Self::let_binding_parts(&binding)?;
                        let name = crate::lisp::primitives::checked_symbol_identity(
                            self,
                            &name_value,
                            env,
                        )?;
                        Self::check_let_binding_name(&name)?;
                        let value = match init {
                            Some(form) => self.eval(&form, env)?,
                            None => Value::Nil,
                        };
                        (name, value)
                    }
                    _ => return Err(wrong_type_argument("listp", binding.clone())),
                };
                if self.binding_is_dynamic_symbol(&name, env) {
                    restores.push(self.bind_special_symbol(&name, value, env)?);
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
                            && match &original_frame_identities {
                                Some(identities) => env
                                    .iter()
                                    .map(Self::frame_identity)
                                    .eq(identities.iter().copied()),
                                None => env.last().map(Self::frame_identity) == original_innermost,
                            };
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
            Ok(()) => self.progn_list(&body, env),
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
