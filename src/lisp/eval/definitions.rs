use super::core::{list_car, list_cons_count, list_next, list_nth, next_cons};
use super::*;
use crate::lisp::types::Kind;

impl Interpreter {
    pub(super) fn sf_setq(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(args, env, false)
    }

    pub(super) fn sf_setq_internal(
        &mut self,
        args: &Value,
        env: &mut Env,
        local_only: bool,
    ) -> Result<Value, LispError> {
        // Fsetq: the pairs read off the list in place; a symbol without
        // its value form signals wrong-number-of-arguments with the count
        // read so far.
        let mut result = Value::Nil;
        let mut cur = match args.kind() {
            Kind::Cons(cell) => Some(cell),
            _ => None,
        };
        let mut nargs = 0usize;
        while let Some(symbol_cell) = cur {
            let sym = symbol_cell.car.get();
            let Some(value_cell) = next_cons(&symbol_cell) else {
                return Err(LispError::WrongNumberOfArgs("setq".into(), nargs + 1));
            };
            let value_form = value_cell.car.get();
            cur = next_cons(&value_cell);
            nargs += 2;
            // The symbol itself, resolved and assigned by its id.
            let symbol = match sym.kind() {
                Kind::Symbol(symbol) => symbol,
                Kind::Nil => SymbolName::intern_str("nil"),
                Kind::T => SymbolName::intern_str("t"),
                other => {
                    return Err(LispError::WrongTypeArgument(
                        "symbolp".into(),
                        other.value(),
                    ));
                }
            };
            // Fsetq: the value, then the lexical alist, then Fset -- for
            // a plain untrapped symbol (no alias, no constant: a constant
            // never learns the plain store) set_internal's store is the
            // whole of Fset.
            if !local_only && self.globals.plain_store(&symbol) {
                let val = self.eval(&value_form, env)?;
                result = val;
                if self.set_lexical_variable_checked_symbol(&symbol, val, env)? {
                    continue;
                }
                if let Some(existing) = self.globals.value_mut(&symbol) {
                    *existing = Self::stored_value(val);
                    continue;
                }
                self.setq_variable_symbol(&symbol, val, env)?;
                continue;
            }
            let evaluated = self.eval(&value_form, env)?;
            if !local_only && self.set_lexical_variable_checked_symbol(&symbol, evaluated, env)? {
                result = evaluated;
                continue;
            }
            let resolved = self.resolve_variable_symbol(&symbol)?;
            let val = self.prepare_variable_assignment_symbol(&resolved, evaluated)?;
            result = val;
            if local_only {
                self.notify_variable_watchers(
                    resolved.as_str(),
                    val,
                    "set",
                    Some(self.current_buffer_id()),
                    env,
                )?;
                self.set_buffer_local_value(self.current_buffer_id(), resolved.as_str(), val);
            } else {
                self.setq_variable_symbol(&resolved, val, env)?;
            }
        }
        Ok(result)
    }

    pub(super) fn sf_defvar(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let nargs = list_cons_count(args);
        if nargs < 1 {
            return Err(LispError::WrongNumberOfArgs("defvar".into(), 0));
        }
        // eval.c:Fdefvar uses CHECK_SYMBOL/XSYMBOL.  While source-position
        // symbols are enabled, that means the definition is installed on
        // the underlying bare symbol while the original object is returned.
        let name_value = list_car(args);
        let name = crate::lisp::primitives::checked_symbol_name(self, &name_value, env)?;
        let resolved = self.resolve_variable_name(&name)?;
        if nargs > 3 {
            return Err(LispError::Signal("Too many arguments".into()));
        }
        // GNU: a bare one-arg `defvar' NOT at top level only makes the
        // variable special within the enclosing lexical scope — the global
        // flag stays off (`special-variable-p' returns nil), so other
        // functions' same-named arguments and `let's remain lexical
        // (erc-send-input relies on this for its obsolete dynamic `str').
        // The local specialness is recorded as a frame marker scoped to the
        // current activation so `let's in the SAME scope bind dynamically.
        if nargs > 1 {
            self.mark_special_variable(&resolved);
            let doc = list_nth(args, 2);
            if !doc.is_nil() {
                let doc = crate::lisp::primitives::purecopy_value(self, &doc, env)?;
                self.put_symbol_property(&resolved, "variable-documentation", doc);
            }
            self.record_definition_in_load_history("defvar", &resolved);
        } else if self.interpreter_environment_is_lexical(env) {
            // GNU eval.c prepends the bare symbol to the current interpreter
            // environment.  At file top level that environment is the
            // per-load `(t)' scope installed by lread.c; it is never a
            // process-wide special-variable declaration.
            self.push_local_special_declaration(&resolved, env);
        }
        // Bare `defvar` declarations mark a variable special without binding it.
        // GNU skips the init form only when a REAL default binding exists.
        // The lazily synthesized builtin fallback table must not count:
        // treating it as a binding silently discarded the init forms of
        // genuinely loaded GNU defvars (mode-line-modes, user-emacs-directory).
        if !self.global_default_binding_exists(&resolved) && nargs > 1 {
            let val = self.eval(&list_nth(args, 1), env)?;
            self.set_default_toplevel_value(&resolved, val);
        }
        Ok(name_value)
    }

    pub(super) fn sf_defconst(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let nargs = list_cons_count(args);
        if nargs < 2 {
            return Err(LispError::WrongNumberOfArgs("defconst".into(), nargs));
        }
        // eval.c:Fdefconst has the same CHECK_SYMBOL/XSYMBOL contract as
        // defvar for source-position symbols.
        let name_value = list_car(args);
        let name = crate::lisp::primitives::checked_symbol_name(self, &name_value, env)?;
        let resolved = self.resolve_variable_name(&name)?;
        if nargs > 3 {
            return Err(LispError::Signal("Too many arguments".into()));
        }
        let value = self.eval(&list_nth(args, 1), env)?;
        self.mark_special_variable(&resolved);
        let doc = list_nth(args, 2);
        if !doc.is_nil() {
            let doc = crate::lisp::primitives::purecopy_value(self, &doc, env)?;
            self.put_symbol_property(&resolved, "variable-documentation", doc);
        }
        let value = crate::lisp::primitives::purecopy_value(self, &value, env)?;
        self.set_default_toplevel_value(&resolved, value);
        self.put_symbol_property(&resolved, "risky-local-variable", Value::T);
        self.record_definition_in_load_history("defvar", &resolved);
        Ok(name_value)
    }

    /// eval.c:Ffunction removes only leading documentation/interactive
    /// metadata. Its body is the remaining source tail, not a copied AST.
    pub(super) fn sf_function(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let Some((quoted, rest)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("function".into(), 0));
        };
        if !rest.is_nil() {
            let length = self.eval_list_length(*args, env)?;
            return Err(LispError::WrongNumberOfArgs("function".into(), length));
        }
        if !matches!(quoted.car().map(|value| value.kind()), Ok(Kind::Symbol(head)) if head == "lambda")
        {
            return Ok(quoted);
        }
        let tail = quoted.cdr()?;
        let parameters = tail.car()?;
        let mut body = tail.cdr()?;
        let mut documentation = Value::Nil;
        if let Kind::Cons(cell) = body.kind() {
            let first = cell.car.get();
            if first.is_string() {
                let rest = cell.cdr.get();
                if !rest.is_nil() {
                    documentation = first;
                    body = rest;
                }
            } else if matches!(first.car().map(|value| value.kind()), Ok(Kind::Symbol(head)) if head == ":documentation")
            {
                documentation = self.eval(&first.cdr()?.car()?, env)?;
                // Re-read after evaluation: dynamic documentation can mutate
                // the source cell whose cdr GNU reads here.
                body = cell.cdr.get();
            }
        }
        let mut iform = Value::Nil;
        if let Kind::Cons(cell) = body.kind() {
            let first = cell.car.get();
            if matches!(first.car().map(|value| value.kind()), Ok(Kind::Symbol(head)) if head == "interactive")
            {
                iform = first;
                body = cell.cdr.get();
            }
        }
        if body.is_nil() {
            body = Value::list([Value::Nil]);
        }
        let capture_override = self.lambda_capture_override();
        let closure_env = if capture_override == Some(false) {
            Value::Nil
        } else if let Some(environment) = crate::lisp::types::current_environment(env) {
            *environment
        } else if capture_override == Some(true)
            || self
                .lookup_var("lexical-binding", env)
                .is_some_and(|value| value.is_truthy())
        {
            Value::list([Value::T])
        } else {
            Value::Nil
        };
        // Environment filtering is owned by the unchanged GNU cconv.el.
        if !closure_env.is_nil()
            && let Some(filter) = self
                .lookup_var("internal-make-interpreted-closure-function", env)
                .filter(Value::is_truthy)
        {
            return self.call_function_value(
                filter,
                None,
                &[parameters, body, closure_env, documentation, iform],
                env,
            );
        }
        self.make_interpreted_closure(parameters, body, closure_env, documentation, iform)
    }
}
