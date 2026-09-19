use super::core::{list_car, list_cons_count, list_next, list_nth, next_cons};
use super::*;
use crate::lisp::types::Kind;

type NormalizedClosureBody = (Option<Value>, Option<Value>, Vec<Value>);

impl Interpreter {
    fn normalize_function_body_documentation(
        &mut self,
        forms: &[Value],
        env: &mut Env,
    ) -> Result<(Option<Value>, Vec<Value>), LispError> {
        let Some(first) = forms.first() else {
            return Ok((None, Vec::new()));
        };
        let documentation = match first.kind() {
            Kind::String(text) if forms.len() > 1 => Some(Value::String(text)),
            Kind::StringObject(state) if forms.len() > 1 => {
                Some(Value::String(state.borrow().text.clone().into()))
            }
            Kind::Cons(_) => {
                let items = first.to_vec()?;
                match items
                    .as_slice()
                    .iter()
                    .map(|v| v.kind())
                    .collect::<Vec<_>>()
                    .as_slice()
                {
                    [Kind::Symbol(head), expression] if head == ":documentation" => {
                        Some(self.eval(&expression.value(), env)?)
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        let Some(documentation) = documentation else {
            return Ok((None, forms.to_vec()));
        };
        // GNU removes documentation from the executable closure body and
        // stores it in closure slot four.  Keeping a second copy in `body'
        // made non-string dynamic documentation execute as code.
        Ok((Some(documentation), forms[1..].to_vec()))
    }

    /// Extract the metadata GNU's `function' special form stores in closure
    /// slots four and five, leaving only executable body forms.  Keeping this
    /// as the single parser prevents source closures, defuns, and serialized
    /// closures from inventing subtly different slot layouts.
    fn normalize_interpreted_closure_body(
        &mut self,
        forms: &[Value],
        env: &mut Env,
    ) -> Result<NormalizedClosureBody, LispError> {
        let (documentation, mut body) = self.normalize_function_body_documentation(forms, env)?;
        let interactive_form = body
            .first()
            .filter(|form| {
                crate::lisp::types::LambdaValue::interactive_slot_from_form(form).is_some()
            })
            .cloned();
        if interactive_form.is_some() {
            body.remove(0);
        }
        if body.is_empty() {
            body.push(Value::Nil);
        }
        Ok((documentation, interactive_form, body))
    }

    pub(super) fn sf_setq(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(args, env, false)
    }

    pub fn set_custom_option(
        &mut self,
        symbol: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(symbol)?;
        if let Some(setter) = self.get_symbol_property(&resolved, "custom-set") {
            self.call_function_value(
                setter,
                None,
                &[Value::Symbol(resolved.clone().into()), value],
                env,
            )?;
        } else {
            self.call_function_value(
                Value::BuiltinFunc("set-default".into()),
                Some("set-default"),
                &[Value::Symbol(resolved.into()), value],
                env,
            )?;
        }
        Ok(value)
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
            let sym = *symbol_cell.car.borrow();
            let Some(value_cell) = next_cons(&symbol_cell) else {
                return Err(LispError::WrongNumberOfArgs("setq".into(), nargs + 1));
            };
            let value_form = *value_cell.car.borrow();
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

    /// Ffunction: a symbol names itself; a `(setf NAME)' form its
    /// function name; a lambda form becomes the interpreted closure; any
    /// other object is returned as it is.
    pub(super) fn sf_function(&mut self, args: &Value, env: &mut Env) -> Result<Value, LispError> {
        let Some((quoted, _)) = list_next(args) else {
            return Ok(Value::Nil);
        };
        if let Kind::Symbol(name) = quoted.kind() {
            return Ok(Value::Symbol(name));
        }
        if let Ok(name) = super::function_name_from_binding_form(&quoted) {
            return Ok(Value::Symbol(name.into()));
        }
        if matches!(quoted.car().map(|v| v.kind()), Ok(Kind::Symbol(ref head)) if head == "lambda")
        {
            let lambda_items = quoted.to_vec()?;
            return self.sf_lambda_from_source(&quoted, &lambda_items, env);
        }
        Ok(quoted)
    }

    // Expand registered `cl-generic-define-context-rewriter' heads inside a
    // cl-defmethod lambda list's &context section: (erc-obsolete-var VAR
    // SPEC) becomes the rewriter's ((EXPR) SPEC) output.

    pub(super) fn sf_lambda_from_source(
        &mut self,
        source: &Value,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.sf_lambda_with_source(items, Some(source), env)
    }

    fn sf_lambda_with_source(
        &mut self,
        items: &[Value],
        source: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::Signal("lambda needs params".into()));
        }
        let params = self.parse_source_params(&items[1], env)?;
        let (documentation, interactive_form, body) =
            self.normalize_interpreted_closure_body(&items[2..], env)?;
        // Ffunction: under a non-nil interpreter environment the lambda
        // is a closure over that environment (the head itself, sharing
        // its binding conses); under nil it stays a dynamic lambda.  The
        // context stack stands in for the environment being nil or `(t)'
        // where the evaluator was entered without one.
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

        // eval.c:Ffunction delegates lexical-environment filtering to the
        // preloaded `internal-make-interpreted-closure-function'.  In GNU 30
        // that function is the unchanged Elisp `cconv-make-interpreted-closure';
        // C neither scans free variables nor rewrites the body itself.
        if !closure_env.is_nil()
            && let Some(filter) = self
                .lookup_var("internal-make-interpreted-closure-function", env)
                .filter(Value::is_truthy)
        {
            return self.call_function_value(
                filter,
                None,
                &[
                    items[1],
                    Value::list(body.iter().cloned()),
                    closure_env,
                    documentation.unwrap_or(Value::Nil),
                    interactive_form.unwrap_or(Value::Nil),
                ],
                env,
            );
        }

        let body = match source.and_then(|source| source.cons_cells().map(|(car, _)| car)) {
            Some(source_anchor) => {
                let source_id = source_anchor.cell_id();
                if let Some(cached) = self
                    .lambda_source_bodies
                    .get(&source_id)
                    .and_then(ConsMutationStamped::current)
                    && cached
                        .source
                        .upgrade()
                        .is_some_and(|cached| cached.ptr_eq(&source_anchor))
                    && let Some(body) = cached.body.upgrade()
                {
                    body
                } else {
                    let body = Rc::new(body);
                    self.lambda_source_bodies.insert(
                        source_id,
                        ConsMutationStamped::new(
                            crate::lisp::types::ConsMutationSnapshot::list_spine(
                                source.expect("a source anchor came from a source form"),
                            ),
                            LambdaSourceBodyCacheEntry {
                                source: source_anchor.downgrade(),
                                body: Rc::downgrade(&body),
                            },
                        ),
                    );
                    body
                }
            }
            None => Rc::new(body),
        };
        let interactive = interactive_form
            .as_ref()
            .and_then(crate::lisp::types::LambdaValue::interactive_slot_from_form);
        Ok(Value::lambda_with_public_parameters(
            params.into(),
            items[1],
            body,
            closure_env,
            documentation,
            interactive,
        ))
    }
}
