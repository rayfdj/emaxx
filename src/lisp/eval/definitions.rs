use super::core::{list_car, list_cons_count, list_next, list_nth, next_cons};
use super::*;
use crate::lisp::types::Kind;

impl Interpreter {
    pub(super) fn sf_setq(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        // Fsetq: the pairs read off the list in place; a symbol without
        // its value form signals wrong-number-of-arguments with the count
        // read so far.
        let mut result = Value::Nil;
        let mut cur = args.cons_ref();
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
            let symbol = match sym.symbol_cell() {
                Some(symbol) => symbol,
                None if sym.is_nil() => crate::lisp::types::symbols::builtin_boolean(false),
                None if sym.word() == Value::T.word() => {
                    crate::lisp::types::symbols::builtin_boolean(true)
                }
                None => {
                    return Err(LispError::WrongTypeArgument("symbolp".into(), sym));
                }
            };
            // Fsetq: the value, then the lexical alist, then Fset -- for
            // a plain untrapped symbol (no alias, no constant: a constant
            // never learns the plain store) set_internal's store is the
            // whole of Fset.
            if self.globals.plain_store(&symbol) {
                let val = self.eval(&value_form, env)?;
                result = val;
                if self.set_lexical_variable_checked_symbol(&symbol, val, env)? {
                    continue;
                }
                if self.globals.replace_bound_value(&symbol, val) {
                    continue;
                }
                self.setq_variable_symbol(&symbol, val, env)?;
                continue;
            }
            let evaluated = self.eval(&value_form, env)?;
            if self.set_lexical_variable_checked_symbol(&symbol, evaluated, env)? {
                result = evaluated;
                continue;
            }
            result = evaluated;
            if self.check_constant_assignment_symbol(&symbol, evaluated)? {
                continue;
            }
            let resolved = self.resolve_variable_symbol(&symbol)?;
            self.setq_variable_symbol(&resolved, evaluated, env)?;
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
            // eval.c:defvar (SYM, EXP, DOC, true), EXP evaluated only if
            // it is wanted.
            let init = list_nth(args, 1);
            self.defvar_symbol(
                &SymbolName::intern_str(&name),
                list_nth(args, 2),
                |interp, env| interp.eval(&init, env),
                env,
            )?;
            return Ok(name_value);
        } else if self.interpreter_environment_is_lexical(env) {
            // GNU eval.c prepends the bare symbol to the current interpreter
            // environment.  At file top level that environment is the
            // per-load `(t)' scope installed by lread.c; it is never a
            // process-wide special-variable declaration.
            self.push_local_special_declaration(&resolved, env);
        }
        Ok(name_value)
    }

    /// eval.c:defvar of SYMBOL as given: whether its variable's default is
    /// bound, asked first (Fdefault_boundp follows an alias); SYMBOL
    /// defined as a dynamic variable with DOC
    /// (`define_uninitialized_variable'); then INIT's value, got only when
    /// wanted, made the default of a variable that had none, or, for one
    /// bound only by lets of SYMBOL itself (a binding records the variable
    /// an alias names, so an alias finds none), the value the outermost of
    /// them saved when it saved none.
    pub(crate) fn defvar_symbol(
        &mut self,
        symbol: &SymbolName,
        doc: Value,
        init: impl FnOnce(&mut Self, &mut Env) -> Result<Value, LispError>,
        env: &mut Env,
    ) -> Result<(), LispError> {
        // Fdefault_boundp: the default cell as it is now, a let's binding
        // of it included; the lazily synthesized builtin fallback table
        // does not count, or the init forms of genuinely loaded GNU
        // defvars (mode-line-modes, user-emacs-directory) were discarded.
        let resolved = self.resolve_variable_symbol(symbol)?;
        let bound = self.global_binding_value_symbol(&resolved).is_some();
        self.define_uninitialized_variable(symbol, doc, env)?;
        if !bound {
            let value = init(self, env)?;
            crate::lisp::primitives::call(
                self,
                "set-default",
                &[Value::Symbol(*symbol), value],
                env,
            )?;
        } else if self
            .active_global_toplevel_value(symbol.as_str())
            .is_some_and(|previous| previous.is_none())
        {
            // default_toplevel_binding's saved Qunbound: a let shadows the
            // global unboundness.
            let value = init(self, env)?;
            self.set_active_global_toplevel_value(symbol.as_str(), Some(value));
        }
        Ok(())
    }

    /// eval.c:Finternal__define_uninitialized_variable: SYMBOL refused
    /// when an interpreter environment an inner binding saved binds it
    /// lexically (`lexbound_p'), then declared special, DOC put as its
    /// documentation, and the definition attached to the load history.
    pub(crate) fn define_uninitialized_variable(
        &mut self,
        symbol: &SymbolName,
        doc: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        if !self.is_special_variable_symbol(symbol) && self.lexbound_p(symbol, env) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::string("Defining as dynamic an already lexical var"),
                Value::Symbol(*symbol),
            ])));
        }
        self.mark_special_variable_symbol(symbol);
        if !doc.is_nil() {
            let doc = crate::lisp::primitives::purecopy_value(self, &doc, env)?;
            self.put_symbol_property(symbol.as_str(), "variable-documentation", doc);
        }
        self.record_definition_in_load_history("defvar", symbol.as_str());
        Ok(())
    }

    /// eval.c:lexbound_p: whether an interpreter environment that an inner
    /// let, call or load replaced, and so saved, binds SYMBOL.  Each such
    /// replacement is a frame of ENV above the one it replaced; the frame
    /// on top is the environment in effect.
    fn lexbound_p(&self, symbol: &SymbolName, env: &Env) -> bool {
        let saved = env.len().saturating_sub(1);
        env.iter().take(saved).any(|frame| {
            crate::lisp::types::assq_binding(frame.environment(), symbol)
                .is_ok_and(|binding| binding.is_some())
        })
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
        if !form_head_is(quoted, "lambda") {
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
            } else if form_head_is(first, ":documentation") {
                documentation = self.eval(&first.cdr()?.car()?, env)?;
                // Re-read after evaluation: dynamic documentation can mutate
                // the source cell whose cdr GNU reads here.
                body = cell.cdr.get();
            }
        }
        let mut iform = Value::Nil;
        if let Kind::Cons(cell) = body.kind() {
            let first = cell.car.get();
            if form_head_is(first, "interactive") {
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
                .lookup_var_key(cached_symbol!("lexical-binding"), env)
                .is_some_and(|value| value.is_truthy())
        {
            Value::list([Value::T])
        } else {
            Value::Nil
        };
        // Environment filtering is owned by the unchanged GNU cconv.el.
        if !closure_env.is_nil()
            && let Some(filter) = self
                .lookup_var_key(
                    cached_symbol!("internal-make-interpreted-closure-function"),
                    env,
                )
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

/// Whether FORM is a list whose car is the symbol NAME. `Value::car'
/// would build a wrong-type-argument error for any other object, only to
/// drop it.
fn form_head_is(form: Value, name: &str) -> bool {
    form.cons_ref().is_some_and(
        |cell| matches!(cell.car.get().kind(), Kind::Symbol(head) if head.is_named(name)),
    )
}
