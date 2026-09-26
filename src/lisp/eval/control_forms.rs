use super::core::{list_cdr, list_forms, list_next};
use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;
impl Interpreter {
    pub(super) fn sf_quote(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        // eval.c:Fquote returns the object already constructed by the
        // reader. Evaluation neither walks it nor retains a cached copy.
        let Some((value, rest)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("quote".into(), 0));
        };
        if !rest.is_nil() {
            let length = self.eval_list_length(*args, env)?;
            return Err(LispError::WrongNumberOfArgs("quote".into(), length));
        }
        Ok(value)
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
        self.active_catch_tags.push(tag);
        let result = self.progn_list(&body, env);
        self.active_catch_tags.pop();
        // A non-local exit unwinds any binding frames pushed between the
        // catch and the throw, like GNU's unbind_to at the catch point.
        if env.len() > depth {
            env.truncate(depth);
        }
        match result.map_err(LispError::into_kind) {
            Ok(value) => Ok(value),
            Err(LispErrorKind::Throw(thrown_tag, value))
                if crate::lisp::primitives::values_eq_in_env(self, &thrown_tag, &tag, env) =>
            {
                Ok(value)
            }
            Err(error) => Err(LispError::from(error)),
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
        let Kind::Cons(cell) = binding.kind() else {
            return Err(wrong_type_argument("listp", *binding));
        };
        let name = cell.car.get();
        let rest = cell.cdr.get();
        match rest.kind() {
            Kind::Nil => Ok((name, None)),
            Kind::Cons(second) => {
                if !second.cdr.get().is_nil() {
                    return Err(LispError::SignalValue(Value::cons(
                        Value::symbol("error"),
                        Value::cons(
                            Value::String(
                                String::from("`let' bindings can have only one value-form").into(),
                            ),
                            *binding,
                        ),
                    )));
                }
                Ok((name, Some(second.car.get())))
            }
            other => Err(wrong_type_argument("listp", other.value())),
        }
    }

    pub(super) fn sf_let(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        // eval.c:Flet uses a Lisp-word array for values, then rereads the
        // binding list. Every inline word is initialized, including slots
        // outside the active slice that a conservative stack scan can see.
        let mut inline_values = [Value::Nil; 8];
        let Some((varlist, _)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("let".into(), 0));
        };
        let length = self.eval_list_length(varlist, env)?;
        if length > inline_values.len() {
            let mut values = crate::lisp::alloc::RootedVec::from(vec![Value::Nil; length]);
            self.let_with_values(*args, &mut values, env)
        } else {
            self.let_with_values(*args, &mut inline_values[..length], env)
        }
    }

    fn let_with_values(
        &mut self,
        args: Value,
        values: &mut [Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let count = self.specpdl_index();
        let mut lexical_scope_depth = None;
        let result = (|| {
            // Flet computes every initializer before validating or binding
            // variable names. Initializers may mutate the original varlist;
            // its initial length bounds this walk, as it bounds GNU's temps.
            let mut tail = args.car()?;
            let mut initialized = 0;
            for slot in values.iter_mut() {
                let Kind::Cons(cell) = tail.kind() else {
                    break;
                };
                self.maybe_quit(env)?;
                let binding = cell.car.get();
                tail = cell.cdr.get();
                let symbol = binding.is_symbol()
                    || (matches!(binding.kind(), Kind::Record(_))
                        && crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                        && crate::lisp::primitives::symbol_with_pos_parts(self, &binding)
                            .is_some());
                *slot = if symbol {
                    Value::Nil
                } else {
                    match Self::let_binding_parts(&binding)?.1 {
                        Some(form) => self.eval(&form, env)?,
                        None => Value::Nil,
                    }
                };
                initialized += 1;
            }

            // GNU takes lexenv after the initializers and reads each variable
            // name again. A name or special declaration changed by an
            // initializer must affect binding, while initializer lookup stays
            // in the enclosing environment throughout the first walk.
            let mut lexenv = crate::lisp::types::current_environment_value(env);
            let mut lexical_bindings = false;
            tail = args.car()?;
            for value in &values[..initialized] {
                let Kind::Cons(cell) = tail.kind() else {
                    break;
                };
                let binding = cell.car.get();
                tail = cell.cdr.get();
                let symbol = binding.is_symbol()
                    || (matches!(binding.kind(), Kind::Record(_))
                        && crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                        && crate::lisp::primitives::symbol_with_pos_parts(self, &binding)
                            .is_some());
                let name_value = if symbol { binding } else { binding.car()? };
                let name =
                    crate::lisp::primitives::checked_symbol_identity(self, &name_value, env)?;
                Self::check_let_binding_name(&name)?;
                if self.binding_is_dynamic_symbol(&name, env) {
                    self.specbind_symbol(&name, *value, env)?;
                } else {
                    lexenv = Self::cons_binding(name, Self::stored_value(*value), lexenv);
                    lexical_bindings = true;
                }
            }
            if lexical_bindings {
                lexical_scope_depth = Some(env.len());
                env.push(EnvFrame::from_alist(lexenv));
            }
            self.progn_list(&args.cdr()?, env)
        })();
        if let Some(depth) = lexical_scope_depth {
            env.truncate(depth);
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
        let Some((varlist, _)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("let*".into(), 0));
        };
        if is_vector_literal(&varlist) || !matches!(varlist.kind(), Kind::Nil | Kind::Cons(_)) {
            return Err(wrong_type_argument("listp", varlist));
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
            let mut tail = varlist;
            let mut tortoise = tail;
            let mut maximum = 2usize;
            let mut remaining = 0isize;
            let mut quit_count = 2u16;
            while let Kind::Cons(cell) = tail.kind() {
                let binding = cell.car.get();
                let symbol = binding.is_symbol()
                    || (matches!(binding.kind(), Kind::Record(_))
                        && crate::lisp::primitives::symbols_with_pos_enabled(self, env)
                        && crate::lisp::primitives::symbol_with_pos_parts(self, &binding)
                            .is_some());
                let (name_value, init) = if symbol {
                    (binding, None)
                } else {
                    Self::let_binding_parts(&binding)?
                };
                // FletX reads the name before evaluating its initializer,
                // but validates that name only after evaluation succeeds.
                let value = match init {
                    Some(form) => self.eval(&form, env)?,
                    None => Value::Nil,
                };
                let name =
                    crate::lisp::primitives::checked_symbol_identity(self, &name_value, env)?;
                Self::check_let_binding_name(&name)?;
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
                // FOR_EACH_TAIL reads the cdr after the body, then applies
                // Brent's cycle and quit checks. An initializer may shorten
                // this list; counting it before evaluation would differ.
                tail = cell.cdr.get();
                quit_count = quit_count.wrapping_sub(1);
                let compare = if quit_count != 0 {
                    true
                } else {
                    self.maybe_quit(env)?;
                    remaining -= 1;
                    remaining > 0
                };
                if compare {
                    if tail.word() == tortoise.word() {
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("circular-list"),
                            tail,
                        ])));
                    }
                } else {
                    maximum <<= 1;
                    quit_count = maximum as u16;
                    remaining = (maximum >> u16::BITS) as isize;
                    tortoise = tail;
                }
            }
            if !tail.is_nil() {
                return Err(wrong_type_argument("listp", args.car()?));
            }
            Ok(())
        })();

        let result = match setup {
            Ok(()) => args.cdr().and_then(|body| self.progn_list(&body, env)),
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
        match (a.kind(), b.kind()) {
            (Kind::Cons(a), Kind::Cons(b)) => crate::lisp::types::SharedCons::ptr_eq(&a, &b),
            (Kind::Nil, Kind::Nil) => true,
            _ => false,
        }
    }
}
