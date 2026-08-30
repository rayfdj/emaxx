use super::*;

fn body_closure_dont_trim_context(body: &[Value]) -> bool {
    let mut start = 0usize;
    if body.len() > 1
        && matches!(
            body.first(),
            Some(Value::String(_) | Value::StringObject(_))
        )
    {
        start = 1;
    }
    matches!(
        body.get(start),
        Some(Value::Symbol(marker)) if marker == ":closure-dont-trim-context"
    ) && body.len().saturating_sub(start) > 1
}

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
        let documentation = match first {
            Value::String(text) if forms.len() > 1 => Some(Value::String(text.clone())),
            Value::StringObject(state) if forms.len() > 1 => {
                Some(Value::String(state.borrow().text.clone().into()))
            }
            Value::Cons(_) => {
                let items = first.to_vec()?;
                match items.as_slice() {
                    [Value::Symbol(head), expression] if head == ":documentation" => {
                        Some(self.eval(expression, env)?)
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
        let interactive = body
            .first()
            .and_then(crate::lisp::types::LambdaValue::interactive_slot_from_form);
        if interactive.is_some() {
            body.remove(0);
        }
        if body.is_empty() {
            body.push(Value::Nil);
        }
        Ok((documentation, interactive, body))
    }

    pub(super) fn sf_setq(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(items, env, false)
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
                &[Value::Symbol(resolved.clone().into()), value.clone()],
                env,
            )?;
        } else {
            self.call_function_value(
                Value::BuiltinFunc("set-default".into()),
                Some("set-default"),
                &[Value::Symbol(resolved.into()), value.clone()],
                env,
            )?;
        }
        Ok(value)
    }

    pub(super) fn sf_setq_internal(
        &mut self,
        items: &[Value],
        env: &mut Env,
        local_only: bool,
    ) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut i = 1;
        while i + 1 < items.len() {
            let name = assignment_target_name(&items[i])?;
            let resolved = self.resolve_variable_name(&name)?;
            let evaluated = self.eval(&items[i + 1], env)?;
            let val = self.prepare_variable_assignment(&resolved, evaluated)?;
            result = val.clone();
            if local_only {
                self.notify_variable_watchers(
                    &resolved,
                    val.clone(),
                    "set",
                    Some(self.current_buffer_id()),
                    env,
                )?;
                self.set_buffer_local_value(self.current_buffer_id(), &resolved, val);
            } else {
                self.setq_variable(&resolved, val, env)?;
            }
            i += 2;
        }
        Ok(result)
    }

    pub(super) fn sf_defvar(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("defvar".into(), 0));
        }
        let name = items[1].as_symbol()?.to_string();
        let resolved = self.resolve_variable_name(&name)?;
        if items.len() > 4 {
            return Err(LispError::Signal("Too many arguments".into()));
        }
        // GNU: a bare one-arg `defvar' NOT at top level only makes the
        // variable special within the enclosing lexical scope — the global
        // flag stays off (`special-variable-p' returns nil), so other
        // functions' same-named arguments and `let's remain lexical
        // (erc-send-input relies on this for its obsolete dynamic `str').
        // The local specialness is recorded as a frame marker scoped to the
        // current activation so `let's in the SAME scope bind dynamically.
        if items.len() > 2 {
            self.mark_special_variable(&resolved);
            if let Some(doc) = items.get(3).filter(|value| !value.is_nil()) {
                self.put_symbol_property(&resolved, "variable-documentation", doc.clone());
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
        if !self.global_default_binding_exists(&resolved) && items.len() > 2 {
            let val = self.eval(&items[2], env)?;
            self.set_default_toplevel_value(&resolved, val);
        }
        Ok(Value::Symbol(resolved.into()))
    }

    pub(super) fn sf_defconst(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "defconst".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let resolved = self.resolve_variable_name(&name)?;
        if items.len() > 4 {
            return Err(LispError::Signal("Too many arguments".into()));
        }
        self.mark_special_variable(&resolved);
        if let Some(doc) = items.get(3).filter(|value| !value.is_nil()) {
            self.put_symbol_property(&resolved, "variable-documentation", doc.clone());
        }
        let value = self.eval(&items[2], env)?;
        self.set_default_toplevel_value(&resolved, value);
        self.put_symbol_property(&resolved, "risky-local-variable", Value::T);
        self.record_definition_in_load_history("defvar", &resolved);
        Ok(Value::Symbol(resolved.into()))
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
        let (documentation, interactive, body) =
            self.normalize_interpreted_closure_body(&items[2..], env)?;
        let keep_full_context = body_closure_dont_trim_context(&body);
        let mut capture_forms = body.clone();
        if let Some(interactive) = &interactive {
            capture_forms.push(interactive.clone());
        }
        let capture_override = self.lambda_capture_override();
        let closure_env = if capture_override.unwrap_or(true) {
            let captured = if !keep_full_context && self.lambda_trim_override() {
                trim_lambda_closure_env(env, &capture_forms)
            } else {
                env.clone()
            };
            // A lexical lambda carries an explicit context marker even when
            // it has no free variables.  Besides forming the scope boundary,
            // invocation uses this marker to give delayed macro expansion
            // the lexical-binding context in which the lambda was created.
            let lexical_source = capture_override == Some(true)
                || self
                    .lookup_var("lexical-binding", env)
                    .is_some_and(|value| value.is_truthy());
            let closure_env = self.capture_closure_env(captured);
            if lexical_source {
                self.mark_lexical_closure_env(&closure_env);
            }
            closure_env
        } else {
            let closure_env = shared_env(Vec::new());
            self.mark_closure_eval_context(&closure_env, false);
            closure_env
        };
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
        let public_environment = self.materialize_public_interpreted_environment(&closure_env);
        Ok(Value::lambda_with_public_environment(
            params.into(),
            body,
            closure_env,
            documentation,
            interactive,
            public_environment,
        ))
    }
}

fn trim_lambda_closure_env(env: &Env, body: &[Value]) -> Env {
    let mut referenced = HashSet::new();
    for form in body {
        collect_referenced_symbols(form, &mut referenced);
    }

    env.iter()
        .filter_map(|frame| {
            // Events at each position are locally-special declarations first,
            // then the real binding at that position.  Preserve that order so
            // trimming retains exactly the prefix GNU's environment alist did.
            let mut cutoff = None;
            for position in 0..=frame.len() {
                for (declaration_index, (_, name)) in frame
                    .local_special_declarations()
                    .iter()
                    .filter(|(declared_at, _)| *declared_at == position)
                    .enumerate()
                {
                    if referenced.contains(name.as_str()) {
                        cutoff = Some((position, Some(declaration_index)));
                    }
                }
                if position < frame.len() && referenced.contains(frame[position].0.as_str()) {
                    cutoff = Some((position, None));
                }
            }
            let (cutoff_position, cutoff_declaration) = cutoff?;
            let binding_count = cutoff_position + usize::from(cutoff_declaration.is_none());
            let mut declarations_seen_at_cutoff = 0;
            let local_special_declarations = frame
                .local_special_declarations()
                .iter()
                .filter(|(position, _)| {
                    if *position < cutoff_position {
                        true
                    } else if *position > cutoff_position {
                        false
                    } else if let Some(last_declaration) = cutoff_declaration {
                        let keep = declarations_seen_at_cutoff <= last_declaration;
                        declarations_seen_at_cutoff += 1;
                        keep
                    } else {
                        true
                    }
                })
                .cloned()
                .collect();
            Some(EnvFrame::from_parts(
                frame[..binding_count].to_vec(),
                frame.identity(),
                frame.has_function_bindings(),
                local_special_declarations,
            ))
        })
        .collect()
}

fn collect_referenced_symbols(value: &Value, referenced: &mut HashSet<String>) {
    match value {
        Value::Symbol(symbol) => {
            referenced.insert(symbol.to_string());
        }
        Value::Cons(_) => {
            let Ok(items) = value.to_vec() else {
                collect_dotted_list_symbols(value, referenced);
                return;
            };
            if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "quote") {
                return;
            }
            for item in items {
                collect_referenced_symbols(&item, referenced);
            }
        }
        _ => {}
    }
}

fn collect_dotted_list_symbols(value: &Value, referenced: &mut HashSet<String>) {
    let Some((car, cdr)) = value.cons_values() else {
        collect_referenced_symbols(value, referenced);
        return;
    };
    collect_referenced_symbols(&car, referenced);
    collect_referenced_symbols(&cdr, referenced);
}
