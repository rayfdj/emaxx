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
        /// fns.c's list_length over a proper list; 0 past its end or for
        /// anything else (the binding walk reports the shape itself).
        fn list_length_or_zero(list: &Value) -> usize {
            let mut count = 0;
            let mut tail = list.clone();
            while let Some((_, next)) = list_next(&tail) {
                count += 1;
                tail = next;
            }
            count
        }

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
        // Flet: the values first (`temps'), then each variable bound --
        // lexically by consing onto `lexenv', dynamically by specbind --
        // and the new environment installed once, after the varlist.
        let mut lexenv = crate::lisp::types::current_environment_value(env);
        let mut lexical_bindings = false;
        // Flet's `temps': SAFE_ALLOCA_LISP, an array the collector scans.
        // A lexical value is consed onto LEXENV at once (a local the
        // stack scan sees); a special's waits here until the varlist is
        // read, across the evaluation of the initializers after it, so
        // the array is on the stack for up to eight bindings and rooted
        // past that (a heap vector the scan cannot see lost a fresh
        // value to the collection a later initializer ran).
        let varlist_len = list_length_or_zero(&varlist);
        // The inline array is initialized before use (see eval_call's
        // argvals): a stale word in a live frame is a root to the scan.
        let mut inline_specials: [Option<(SymbolName, Value)>; 8] = [const { None }; 8];
        let mut inline_count = 0usize;
        let mut rooted_specials =
            (varlist_len > 8).then(|| crate::lisp::alloc::RootedVec::with_capacity(varlist_len));
        let mut push_special = |name: SymbolName, value: Value| match rooted_specials.as_mut() {
            Some(rooted) => rooted.push((name, value)),
            None => {
                inline_specials[inline_count] = Some((name, value));
                inline_count += 1;
            }
        };

        let mut tail = varlist.clone();
        while let Some((binding, next)) = Self::next_let_binding(&tail, &varlist)? {
            tail = next;
            match &binding {
                Value::Symbol(name) => {
                    Self::check_let_binding_name(name)?;
                    if self.binding_is_dynamic_symbol(name, env) {
                        push_special(*name, Value::Nil);
                    } else {
                        lexenv = Self::cons_binding(*name, Value::Nil, lexenv);
                        lexical_bindings = true;
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
                        push_special(name, Value::Nil);
                    } else {
                        lexenv = Self::cons_binding(name, Value::Nil, lexenv);
                        lexical_bindings = true;
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
                        push_special(name, val);
                    } else {
                        lexenv = Self::cons_binding(name, Self::stored_value(val), lexenv);
                        lexical_bindings = true;
                    }
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }

        // Flet: specbind each special, the count taken first; a failed
        // bind unwinds the ones made (its error was returned over them
        // before).
        let count = self.specpdl_index();
        let mut bind = |this: &mut Self, name: SymbolName, value: Value| -> Result<(), LispError> {
            if let Err(error) = this.specbind_symbol(&name, value, env) {
                let _ = this.unbind_to(count, env);
                return Err(error);
            }
            Ok(())
        };
        match rooted_specials.take() {
            Some(rooted) => {
                for (name, value) in rooted {
                    bind(self, name, value)?;
                }
            }
            None => {
                for slot in &mut inline_specials[..inline_count] {
                    let (name, value) = slot.take().expect("a pushed special binding");
                    bind(self, name, value)?;
                }
            }
        }
        // `specbind (Qinternal_interpreter_environment, lexenv)' once the
        // varlist is bound: the values were computed under the enclosing
        // environment, so a bare defvar in an initializer stays in the
        // enclosing scope.
        let lexical_scope_depth = env.len();
        if lexical_bindings {
            env.push(EnvFrame::from_alist(lexenv));
        }
        let result = self.progn_list(&body, env);
        if lexical_bindings {
            env.truncate(lexical_scope_depth);
        }
        let unbind = self.unbind_to(count, env);
        match result {
            Ok(value) => unbind.map(|()| value),
            Err(error) => Err(error),
        }
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
        // FletX: `lexenv' is the environment at entry; the first lexical
        // binding saves it on the specpdl (a frame), the later ones store
        // the extended alist into the variable.
        let original_depth = env.len();
        let lexenv = crate::lisp::types::current_environment_value(env);
        let mut lexical_restore_depth = None;
        let count = self.specpdl_index();
        let setup = (|| -> Result<(), LispError> {
            // The varlist and each element read in place (FletX's
            // FOR_EACH_TAIL).
            let mut tail = varlist.clone();
            while let Some((binding, next)) = Self::next_let_binding(&tail, &varlist)? {
                tail = next;
                let (name, value) = match &binding {
                    Value::Symbol(name) => {
                        Self::check_let_binding_name(name)?;
                        (*name, Value::Nil)
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
                    self.specbind_symbol(&name, value, env)?;
                } else {
                    let newenv = Self::cons_binding(
                        name,
                        Self::stored_value(value),
                        crate::lisp::types::current_environment_value(env),
                    );
                    // `EQ (Vinternal_interpreter_environment, lexenv)': the
                    // environment is still the one at entry (no lexical
                    // binding yet, no bare defvar in an initializer), so
                    // this binding saves it on the specpdl; otherwise the
                    // variable is stored into.  A bare defvar in the first
                    // initializer thus makes the following bindings live
                    // for the rest of the enclosing scope, as in GNU.
                    if lexical_restore_depth.is_none()
                        && env.len() == original_depth
                        && Self::same_environment(
                            &crate::lisp::types::current_environment_value(env),
                            &lexenv,
                        )
                    {
                        lexical_restore_depth = Some(original_depth);
                        env.push(EnvFrame::from_alist(newenv));
                    } else if let Some(frame) = env.last_mut() {
                        frame.set_environment(newenv);
                    } else {
                        env.push(EnvFrame::from_alist(newenv));
                    }
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
        let unbind = self.unbind_to(count, env);
        match result {
            Ok(value) => unbind.map(|()| value),
            Err(error) => Err(error),
        }
    }
    /// Flet's `Fcons (Fcons (var, tem), lexenv)'.
    #[inline]
    pub(crate) fn cons_binding(symbol: SymbolName, value: Value, lexenv: Value) -> Value {
        Value::cons(Value::cons(Value::Symbol(symbol), value), lexenv)
    }

    /// `EQ' of two environment heads: the same cons, or both nil.
    pub(crate) fn same_environment(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Cons(a), Value::Cons(b)) => crate::lisp::types::SharedCons::ptr_eq(a, b),
            (Value::Nil, Value::Nil) => true,
            _ => false,
        }
    }
}
