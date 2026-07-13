use super::*;

fn backquote_splice_elements(value: Value) -> Result<Vec<Value>, LispError> {
    let mut items = value.to_vec()?;
    if matches!(
        items.first(),
        Some(Value::Symbol(symbol)) if symbol == "vector-literal"
    ) {
        items.remove(0);
    }
    Ok(items)
}

impl Interpreter {
    pub(super) fn eval_backquote(
        &mut self,
        expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.eval_backquote_with_depth(expr, env, 0)
    }

    pub(super) fn eval_record_literal_form(
        &mut self,
        slots: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut values = Vec::with_capacity(slots.len());
        for slot in slots {
            values.push(self.eval(slot, env)?);
        }
        if let Some(first) = values.first()
            && let Ok(type_name) = first.as_symbol()
        {
            return Ok(self.create_record(type_name, values[1..].to_vec()));
        }
        Ok(self.create_record("literal-record", values))
    }

    pub(super) fn eval_backquote_with_depth(
        &mut self,
        expr: &Value,
        env: &mut Env,
        depth: usize,
    ) -> Result<Value, LispError> {
        if let Some((_kind, value)) = backquote_unquote_form(expr) {
            if depth == 0 {
                return self.eval(&value, env);
            }
            // Preserve the original head symbol (`\,'/`\,@' vs the
            // canonical names): pcase patterns rebuilt through nested
            // templates must keep the reader's raw symbols.
            let head = expr.to_vec()?[0].clone();
            return Ok(Value::list([
                head,
                self.eval_backquote_with_depth(&value, env, depth - 1)?,
            ]));
        }

        if let Some(body) = nested_backquote_body(expr) {
            let head = expr.to_vec()?[0].clone();
            return Ok(Value::list([
                head,
                self.eval_backquote_with_depth(&body, env, depth + 1)?,
            ]));
        }

        match expr {
            Value::Cons(_, _) => {
                let mut result: Vec<Value> = Vec::new();
                let mut current = expr.clone();
                loop {
                    if backquote_unquote_form(&current).is_some() {
                        let tail = self.eval_backquote_with_depth(&current, env, depth)?;
                        return Ok(cons_list_with_tail(result, tail));
                    }
                    if !result.is_empty() && is_backquote_atomic_cons_tail(&current) {
                        let tail = self.eval_backquote_with_depth(&current, env, depth)?;
                        return Ok(cons_list_with_tail(result, tail));
                    }
                    match current {
                        Value::Cons(car, cdr) => {
                            let car_value = car.borrow().clone();
                            let cdr_value = cdr.borrow().clone();

                            if depth == 0
                                && let Some(("comma-at", value)) =
                                    backquote_unquote_form(&car_value)
                            {
                                let evaled = self.eval(&value, env)?;
                                result.extend(backquote_splice_elements(evaled)?);
                                current = cdr_value;
                                continue;
                            }

                            result.push(self.eval_backquote_with_depth(&car_value, env, depth)?);
                            current = cdr_value;
                        }
                        Value::Nil => break,
                        other => {
                            let tail = self.eval_backquote_with_depth(&other, env, depth)?;
                            return Ok(cons_list_with_tail(result, tail));
                        }
                    }
                }
                let result = Value::list(result);
                if depth == 0 && is_record_literal_reader_form(expr) {
                    return self.eval(&result, env);
                }
                Ok(result)
            }
            _ => Ok(expr.clone()),
        }
    }

    // ── Macros ──

    pub(super) fn sf_defmacro(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs("defmacro".into(), items.len()));
        }
        let name = items[1].as_symbol()?.to_string();
        let params_list = items[2].to_vec()?;
        let mut params = Vec::new();
        for p in &params_list {
            params.push(p.as_symbol()?.to_string());
        }
        // Body starts at index 3, skip docstrings
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };
        // Process and skip (declare ...) forms.
        let body_start = if body_start < items.len() {
            if let Value::Cons(_, _) = &items[body_start] {
                if let Ok(decl) = items[body_start].to_vec() {
                    if let Some(Value::Symbol(s)) = decl.first() {
                        if s == "declare" {
                            self.record_defmacro_declarations(&name, &decl[1..]);
                            body_start + 1
                        } else {
                            body_start
                        }
                    } else {
                        body_start
                    }
                } else {
                    body_start
                }
            } else {
                body_start
            }
        } else {
            body_start
        };
        let body: Vec<Value> = items[body_start..].to_vec();
        self.note_macro_added(&name);
        self.macros
            .push((name.clone(), params.clone(), body.clone()));
        // Pending advice on a macro: GNU defalias hands the fresh
        // (macro . EXPANDER) cell to `defalias-fset-function', and nadvice
        // fsets the advised cell back (the cell wins over the macro table).
        if self
            .get_symbol_property(&name, "defalias-fset-function")
            .is_some_and(|value| value.is_truthy())
        {
            let cell = Value::cons(
                Value::Symbol("macro".into()),
                Value::Lambda(params, body, shared_env(Vec::new())),
            );
            let mut env = Env::new();
            self.defalias_fset_function_handles(&name, &cell, &mut env);
        }
        Ok(Value::Symbol(name))
    }

    fn record_defmacro_declarations(&mut self, name: &str, declarations: &[Value]) {
        for declaration in declarations {
            let Ok(parts) = declaration.to_vec() else {
                continue;
            };
            match (parts.first(), parts.get(1)) {
                (Some(Value::Symbol(head)), Some(spec)) if head == "debug" => {
                    self.put_symbol_property(name, "edebug-form-spec", spec.clone());
                }
                // GNU macro-declarations-alist: (obsolete NEW WHEN) runs
                // `make-obsolete', which stores (NEW nil WHEN).
                (Some(Value::Symbol(head)), Some(new)) if head == "obsolete" => {
                    let when = parts.get(2).cloned().unwrap_or(Value::Nil);
                    self.put_symbol_property(
                        name,
                        "byte-obsolete-info",
                        Value::list([new.clone(), Value::Nil, when]),
                    );
                }
                _ => {}
            }
        }
    }

    pub(super) fn sf_easy_menu_define(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() != 5 {
            return Err(LispError::WrongNumberOfArgs(
                "easy-menu-define".into(),
                items.len().saturating_sub(1),
            ));
        }
        let Some(symbol_name) = (match &items[1] {
            Value::Nil => None,
            Value::Symbol(name) => Some(name.clone()),
            other => {
                return Err(LispError::TypeError("symbol".into(), other.type_name()));
            }
        }) else {
            return Ok(Value::Nil);
        };

        if self.lookup_var(&symbol_name, env).is_none() {
            self.set_variable(
                &symbol_name,
                crate::lisp::primitives::keymap_placeholder(Some(&symbol_name)),
                env,
            );
        }
        if self.lookup_function(&symbol_name, env).is_err() {
            self.set_function_binding(&symbol_name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(symbol_name))
    }

    pub(super) fn sf_defalias(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "defalias".into(),
                items.len().saturating_sub(1),
            ));
        }
        // GNU defalias is a FUNCTION: a bare-symbol first argument is an
        // expression evaluating to the symbol to define (uninterned symbols
        // held in variables — Bug#61179's `(defalias sym ...)').
        let name = match &items[1] {
            Value::Symbol(_) => {
                let value = self.eval(&items[1], env)?;
                value.as_symbol()?.to_string()
            }
            other => quoted_symbol_name(other)
                .ok_or_else(|| LispError::TypeError("symbol".into(), other.type_name()))?,
        };
        let function = self.eval(&items[2], env)?;
        self.validate_function_binding(&name, &function)?;
        if crate::lisp::primitives::prefer_builtin_override(&name) {
            self.set_function_binding(&name, Some(Value::BuiltinFunc(name.clone())));
            return Ok(Value::Symbol(name));
        }
        if self.defalias_fset_function_handles(&name, &function, env) {
            self.advice_note_new_definition(&name);
            return Ok(Value::Symbol(name));
        }
        // Like fset: only a (macro . EXPANDER) cell or a symbol alias keeps
        // macro-ness; any other definition erases the macro.
        let keeps_macro = matches!(&function, Value::Symbol(_))
            || function
                .cons_values()
                .is_some_and(|(car, _)| matches!(&car, Value::Symbol(s) if s == "macro"));
        if !keeps_macro {
            self.shadow_macro_binding(&name);
        }
        self.set_function_binding(&name, Some(function));
        self.advice_note_new_definition(&name);
        Ok(Value::Symbol(name))
    }

    pub(super) fn try_macroexpand(
        &mut self,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        self.try_macroexpand_with_environment(name, args, None, env)
    }

    /// Invoke a macro-environment expander (from cl-flet/cl-labels/
    /// cl-macrolet and friends) in a fresh environment.  Expanders are
    /// closures over their own captured bindings; running them inside the
    /// caller's frames would let same-named caller locals (e.g. the `var'
    /// bound by cl-labels' pcase-let*) shadow the captured ones.
    fn call_macro_environment_expander(
        &mut self,
        expander: Value,
        name: &str,
        args: &[Value],
    ) -> Result<Value, LispError> {
        let mut expander_env = Env::new();
        self.call_function_value(expander, Some(name), args, &mut expander_env)
    }

    pub(super) fn try_macroexpand_with_environment(
        &mut self,
        name: &str,
        args: &[Value],
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        if let Some(expander) = macro_environment_expander(macro_environment, name) {
            return self
                .call_macro_environment_expander(expander, name, args)
                .map(Some);
        }

        // Backquote evaluation is native (the reader encodes unquotes as
        // `comma'/`comma-at' markers that GNU backquote.el's `\`' macro does
        // not recognize, so expanding through it would drop the unquotes).
        // Treat backquote as a special form here; `eval' and
        // `macroexpand-all' both handle it natively.
        if is_backquote_head(name) {
            // GNU's `\`' macro expands templates into list/append
            // constructor code (generator.el's CPS transformer requires
            // that shape).  Nested backquotes stay opaque.
            if let Some(template) = args.first() {
                return Ok(Some(backquote_template_code(template)));
            }
            return Ok(None);
        }

        // The pcase family is evaluated natively UNLESS GNU pcase.el has
        // been loaded (its macros then own the family; the reader encodes
        // patterns with the same `\`'/`\,' symbols pcase.el registers).
        if matches!(
            name,
            "pcase" | "pcase-exhaustive" | "pcase-let" | "pcase-let*" | "pcase-dolist"
        ) {
            self.ensure_gnu_pcase_loaded();
            if !self.has_macro_binding(name) {
                return Ok(None);
            }
        }

        // GNU oclosure.el signals duplicate-slot errors at macroexpansion
        // time (oclosure-tests macroexpands invalid forms and expects the
        // error); the forms themselves stay native special forms.
        if name == "oclosure-define" {
            self.validate_oclosure_define_slots(args)?;
            return Ok(None);
        }
        if name == "oclosure-lambda" {
            validate_oclosure_lambda_slots(args)?;
            return Ok(None);
        }

        // A cached (and still current) not-a-macro verdict skips the whole
        // probe.  cl-flet frame shadowing can only make a name LESS of a
        // macro, so a global "not a macro" verdict stays correct under any
        // frames; verdicts influenced by frames are never cached.
        if self.known_not_macro(name) {
            if let Some(expanded) = self.try_builtin_macroexpand(name, args, env)? {
                return Ok(Some(expanded));
            }
            return Ok(None);
        }

        let mut attempted_autoload = false;
        let (params, body) = loop {
            if let Some(expanded) = self.try_builtin_macroexpand(name, args, env)? {
                return Ok(Some(expanded));
            }

            // GNU keeps macros in the function cell as (macro . EXPANDER);
            // nadvice fsets advised macros (and advised macro ALIASES) that
            // way, so the cell wins over the native macro table.
            if let Some(expander) = self.function_cell_macro_expander(name, env) {
                let expanded = self.call_function_value(expander, Some(name), args, env)?;
                return Ok(Some(expanded));
            }

            if let Some(binding) = self.resolve_macro_binding(name) {
                break binding;
            }

            if attempted_autoload {
                self.note_not_macro(name);
                return Ok(None);
            }
            // Only global state can hold an autoload stub (env frames
            // never resolve to autoload conses), so probe the macro
            // position without scanning ordinary frames.
            let Some((mut function, from_frame)) = self.macro_position_function(name, env) else {
                self.note_not_macro(name);
                return Ok(None);
            };
            // A native fallback arm can shadow a preloaded macro autoload
            // (add-function before nadvice.el loads): honor the autoload.
            if crate::lisp::primitives::autoload_parts(&function).is_none()
                && let Some(stub) = preload::builtin_autoload_function(name)
                && crate::lisp::primitives::autoload_parts(&stub).is_some()
            {
                function = stub;
            }
            let Some((file, _, _kind)) = crate::lisp::primitives::autoload_parts(&function) else {
                if !from_frame {
                    self.note_not_macro(name);
                }
                return Ok(None);
            };
            let loads_macro =
                crate::lisp::primitives::autoload_is_macro(self, Some(name), &function);
            if !loads_macro {
                self.note_not_macro(name);
                return Ok(None);
            }
            // A file-less environment (unit tests) falls back to whatever
            // native arm handles the name.
            if self.load_target(&file).is_err() {
                return Ok(None);
            }
            attempted_autoload = true;
        };

        // Advised macros expand through the advice chain: the expander
        // (resolved now, so redefinitions are seen) is the innermost
        // function and receives the unevaluated argument forms.
        if self
            .advice_registry
            .get(name)
            .is_some_and(|state| !state.entries.is_empty())
        {
            let expander = Value::Lambda(params.clone(), body.clone(), shared_env(Vec::new()));
            let composed = self.compose_advice_chain(name, expander);
            let expanded = self.call_function_value(composed, Some(name), args, env)?;
            return Ok(Some(expanded));
        }

        // Bind params to unevaluated args
        let mut frame = Vec::new();
        let mut arg_idx = 0;
        let mut rest = false;
        let mut optional = false;

        for param in &params {
            if param == "&optional" {
                optional = true;
                continue;
            }
            if param == "&rest" || param == "&body" {
                rest = true;
                continue;
            }
            if rest {
                let rest_args = Value::list(args.get(arg_idx..).unwrap_or(&[]).iter().cloned());
                frame.push((param.clone(), rest_args));
                break;
            }
            let val = if arg_idx < args.len() {
                args[arg_idx].clone()
            } else if optional {
                Value::Nil
            } else {
                // GNU signals wrong-number-of-arguments when a macro call
                // omits required parameters ((pcase-setq a) must error).
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            };
            frame.push((param.clone(), val));
            arg_idx += 1;
        }

        Self::push_marked_frame(env, frame);
        let expanded = if body.len() == 1 {
            self.eval(&body[0], env)?
        } else {
            let progn =
                Value::list(std::iter::once(Value::symbol("progn")).chain(body.iter().cloned()));
            self.eval(&progn, env)?
        };
        env.pop();
        Ok(Some(expanded))
    }

    pub(crate) fn macroexpand_1_form_with_environment(
        &mut self,
        form: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(Value::Symbol(name)) = items.first() else {
            return Ok(form.clone());
        };
        Ok(self
            .try_macroexpand_with_environment(name, &items[1..], macro_environment, env)?
            .unwrap_or_else(|| form.clone()))
    }

    /// Walk a backquote template, macro-expanding only the expressions under
    /// `comma'/`comma-at' markers at the current backquote depth.  Nested
    /// backquotes raise the depth; their unquotes stay untouched until the
    /// matching level is reached, mirroring GNU backquote nesting.
    fn macroexpand_all_backquote_template(
        &mut self,
        value: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
        depth: usize,
    ) -> Result<Value, LispError> {
        if let Some((_, inner)) = backquote_unquote_form(value) {
            let marker = value.car()?;
            let inner = if depth == 0 {
                self.macroexpand_all_form_with_environment(&inner, macro_environment, env)?
            } else {
                self.macroexpand_all_backquote_template(&inner, macro_environment, env, depth - 1)?
            };
            return Ok(Value::list([marker, inner]));
        }
        if let Some(body) = nested_backquote_body(value) {
            let head = value.car()?;
            let body =
                self.macroexpand_all_backquote_template(&body, macro_environment, env, depth + 1)?;
            return Ok(Value::list([head, body]));
        }
        if value.cons_values().is_some() {
            // Walk the list spine iteratively: templates can be arbitrarily
            // long and per-cons recursion would exhaust the stack.
            let mut fronts = Vec::new();
            let mut tail = value.clone();
            loop {
                if backquote_unquote_form(&tail).is_some() || nested_backquote_body(&tail).is_some()
                {
                    tail = self.macroexpand_all_backquote_template(
                        &tail,
                        macro_environment,
                        env,
                        depth,
                    )?;
                    break;
                }
                match tail.cons_values() {
                    Some((car, cdr)) => {
                        fronts.push(self.macroexpand_all_backquote_template(
                            &car,
                            macro_environment,
                            env,
                            depth,
                        )?);
                        tail = cdr;
                    }
                    None => break,
                }
            }
            let mut rebuilt = tail;
            for front in fronts.into_iter().rev() {
                rebuilt = Value::cons(front, rebuilt);
            }
            return Ok(rebuilt);
        }
        Ok(value.clone())
    }

    // GNU macroexp-macroexpand warns when macroexpand-all expands a macro
    // carrying `byte-obsolete-info' (macroexp-warn-and-return sends the
    // warning through `message', so cl-letf interception reaches it).
    fn warn_when_expanding_obsolete_macro(
        &mut self,
        name: &str,
        env: &mut Env,
    ) -> Result<(), LispError> {
        if self
            .get_symbol_property(name, "byte-obsolete-info")
            .is_none_or(|info| !info.is_truthy())
            || !self.has_lisp_function("macroexp-warn-and-return")
            || !self.has_lisp_function("macroexp--obsolete-warning")
        {
            return Ok(());
        }
        let quoted_name = Value::list([
            Value::Symbol("quote".into()),
            Value::Symbol(name.to_string()),
        ]);
        let warn = Value::list([
            Value::Symbol("macroexp-warn-and-return".into()),
            Value::list([
                Value::Symbol("macroexp--obsolete-warning".into()),
                quoted_name.clone(),
                Value::list([
                    Value::Symbol("get".into()),
                    quoted_name.clone(),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol("byte-obsolete-info".into()),
                    ]),
                ]),
                Value::String("macro".into()),
            ]),
            Value::Nil,
            Value::list([
                Value::Symbol("list".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("obsolete".into()),
                ]),
                quoted_name.clone(),
            ]),
            Value::Nil,
            quoted_name,
        ]);
        self.eval(&warn, env)?;
        Ok(())
    }

    pub(crate) fn macroexpand_all_form_with_environment(
        &mut self,
        form: &Value,
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(head) = items.first() else {
            return Ok(Value::Nil);
        };
        if let Value::Symbol(name) = head {
            // The macro environment takes precedence even over `function' and
            // `quote': cl-flet/cl-labels bind `function' to cl--labels-convert
            // so `#'local-fn' references get rewritten (GNU macroexpand-1
            // consults ENVIRONMENT before anything else).
            if matches!(name.as_str(), "quote" | "function")
                && let Some(expander) = macro_environment_expander(macro_environment, name)
            {
                let expanded = self.call_macro_environment_expander(expander, name, &items[1..])?;
                if &expanded != form {
                    return self.macroexpand_all_form_with_environment(
                        &expanded,
                        macro_environment,
                        env,
                    );
                }
                // Unchanged (e.g. cl--labels-convert on a non-local
                // function): fall through so `#'(lambda ...)' bodies still
                // get descended into below.
            }
            match name.as_str() {
                "quote" => return Ok(form.clone()),
                "function" => {
                    // GNU macroexp--expand-all descends into `#'(lambda ...)'
                    // bodies; other function forms stay opaque.
                    if let Some(func) = items.get(1)
                        && let Ok(func_items) = func.to_vec()
                        && matches!(
                            func_items.first(),
                            Some(Value::Symbol(symbol)) if symbol == "lambda"
                        )
                    {
                        let mut expanded_lambda = Vec::with_capacity(func_items.len());
                        expanded_lambda.push(func_items[0].clone());
                        if let Some(params) = func_items.get(1) {
                            expanded_lambda.push(params.clone());
                        }
                        for item in func_items.iter().skip(2) {
                            expanded_lambda.push(self.macroexpand_all_form_with_environment(
                                item,
                                macro_environment,
                                env,
                            )?);
                        }
                        return Ok(Value::list([
                            items[0].clone(),
                            Value::list(expanded_lambda),
                        ]));
                    }
                    return Ok(form.clone());
                }
                "eval-when-compile" => {
                    let value = if items.len() <= 1 {
                        Value::Nil
                    } else if items.len() == 2 {
                        self.eval(&items[1], env)?
                    } else {
                        let progn = Value::list(
                            std::iter::once(Value::Symbol("progn".into()))
                                .chain(items[1..].iter().cloned()),
                        );
                        self.eval(&progn, env)?
                    };
                    return Ok(quoted_literal(&value));
                }
                "eval-and-compile" => {
                    // GNU macroexpand-all evaluates eval-and-compile bodies
                    // at expansion time (compile-time side effects, e.g.
                    // rx-define's `(put ... 'rx-definition ...)') AND keeps
                    // the forms so they also run at load/runtime.
                    for item in &items[1..] {
                        self.eval(item, env)?;
                    }
                    let mut expanded = vec![items[0].clone()];
                    for item in &items[1..] {
                        expanded.push(self.macroexpand_all_form_with_environment(
                            item,
                            macro_environment,
                            env,
                        )?);
                    }
                    return Ok(Value::list(expanded));
                }
                "let" | "let*" | "letrec" => {
                    return self.macroexpand_all_let_form_with_environment(
                        &items,
                        macro_environment,
                        env,
                    );
                }
                // Backquote templates stay in place; only the unquoted
                // expressions inside them are macro-expanded (this is how the
                // env expanders from cl-flet/cl-labels reach `,(local-fn ...)'
                // calls while the template text survives verbatim).
                // The pcase family is evaluated natively and its patterns
                // use backquote SYNTAX; expand only the subject and clause
                // bodies so patterns survive while cl-labels-style env
                // expanders still reach the code inside.
                "pcase" | "pcase-exhaustive"
                    if items.len() >= 2 && !self.has_macro_binding("pcase") =>
                {
                    let mut rebuilt = vec![items[0].clone()];
                    rebuilt.push(self.macroexpand_all_form_with_environment(
                        &items[1],
                        macro_environment,
                        env,
                    )?);
                    for clause in &items[2..] {
                        let Ok(parts) = clause.to_vec() else {
                            rebuilt.push(clause.clone());
                            continue;
                        };
                        if parts.is_empty() {
                            rebuilt.push(clause.clone());
                            continue;
                        }
                        let mut new_clause = vec![parts[0].clone()];
                        for body in &parts[1..] {
                            new_clause.push(self.macroexpand_all_form_with_environment(
                                body,
                                macro_environment,
                                env,
                            )?);
                        }
                        rebuilt.push(Value::list(new_clause));
                    }
                    return Ok(Value::list(rebuilt));
                }
                // GNU's defun/defmacro are macros expanding to a lambda whose
                // body gets macro-expanded while the name, arglist, docstring
                // and declare/interactive forms stay untouched.  emaxx keeps
                // them as special forms, so descend the same way.
                "defun" | "defmacro" | "defsubst" if items.len() >= 3 => {
                    let mut rebuilt = vec![items[0].clone(), items[1].clone(), items[2].clone()];
                    for form in &items[3..] {
                        let head = form
                            .to_vec()
                            .ok()
                            .and_then(|parts| parts.first().cloned())
                            .and_then(|head| head.as_symbol().ok().map(str::to_string));
                        let verbatim = matches!(form, Value::String(_) | Value::StringObject(_))
                            || head
                                .as_deref()
                                .is_some_and(|head| head == "declare" || head == "interactive");
                        if verbatim {
                            rebuilt.push(form.clone());
                        } else {
                            rebuilt.push(self.macroexpand_all_form_with_environment(
                                form,
                                macro_environment,
                                env,
                            )?);
                        }
                    }
                    return Ok(Value::list(rebuilt));
                }
                "pcase-let" | "pcase-let*" | "pcase-dolist"
                    if items.len() >= 2 && !self.has_macro_binding("pcase-let") =>
                {
                    let mut rebuilt = vec![items[0].clone(), items[1].clone()];
                    for body in &items[2..] {
                        rebuilt.push(self.macroexpand_all_form_with_environment(
                            body,
                            macro_environment,
                            env,
                        )?);
                    }
                    return Ok(Value::list(rebuilt));
                }
                other if is_backquote_head(other) && items.len() == 2 => {
                    let template = self.macroexpand_all_backquote_template(
                        &items[1],
                        macro_environment,
                        env,
                        0,
                    )?;
                    // GNU's `\`' is a macro: macroexpand-all yields the
                    // constructor code.
                    return Ok(backquote_template_code(&template));
                }
                _ => {}
            }
            if let Some(expander) = macro_environment_expander(macro_environment, name) {
                let expanded = self.call_macro_environment_expander(expander, name, &items[1..])?;
                return self.macroexpand_all_form_with_environment(
                    &expanded,
                    macro_environment,
                    env,
                );
            } else if let Some(expanded) =
                self.try_macroexpand_with_environment(name, &items[1..], None, env)?
            {
                self.warn_when_expanding_obsolete_macro(name, env)?;
                return self.macroexpand_all_form_with_environment(
                    &expanded,
                    macro_environment,
                    env,
                );
            }
        }

        if matches!(head, Value::Symbol(name) if name == "lambda") {
            let mut expanded = Vec::with_capacity(items.len());
            expanded.push(items[0].clone());
            if let Some(params) = items.get(1) {
                expanded.push(params.clone());
            }
            for item in &items[2..] {
                expanded.push(self.macroexpand_all_form_with_environment(
                    item,
                    macro_environment,
                    env,
                )?);
            }
            return Ok(Value::list(expanded));
        }

        let mut expanded = Vec::with_capacity(items.len());
        if matches!(head, Value::Symbol(_)) {
            expanded.push(items[0].clone());
            for item in &items[1..] {
                expanded.push(self.macroexpand_all_form_with_environment(
                    item,
                    macro_environment,
                    env,
                )?);
            }
        } else {
            for item in &items {
                expanded.push(self.macroexpand_all_form_with_environment(
                    item,
                    macro_environment,
                    env,
                )?);
            }
        }
        Ok(Value::list(expanded))
    }

    fn macroexpand_all_let_form_with_environment(
        &mut self,
        items: &[Value],
        macro_environment: Option<&Value>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(bindings_value) = items.get(1) else {
            return Ok(Value::list(items.iter().cloned()));
        };
        let bindings = bindings_value.to_vec()?;
        let mut expanded_bindings = Vec::with_capacity(bindings.len());
        for binding in bindings {
            match binding {
                Value::Symbol(_) => expanded_bindings.push(binding),
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    if parts.is_empty() {
                        expanded_bindings.push(Value::Nil);
                        continue;
                    }
                    let mut expanded = Vec::with_capacity(parts.len());
                    expanded.push(parts[0].clone());
                    for initializer in &parts[1..] {
                        expanded.push(self.macroexpand_all_form_with_environment(
                            initializer,
                            macro_environment,
                            env,
                        )?);
                    }
                    expanded_bindings.push(Value::list(expanded));
                }
                _ => expanded_bindings.push(binding),
            }
        }

        let mut expanded = Vec::with_capacity(items.len());
        expanded.push(items[0].clone());
        expanded.push(Value::list(expanded_bindings));
        for body in &items[2..] {
            expanded.push(self.macroexpand_all_form_with_environment(
                body,
                macro_environment,
                env,
            )?);
        }
        Ok(Value::list(expanded))
    }

    pub(super) fn try_builtin_macroexpand(
        &mut self,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        match name {
            "cl-case" if !self.has_lisp_macro("cl-case") => {
                self.expand_cl_case(args, env).map(Some)
            }
            "cl-with-gensyms" => self.expand_cl_with_gensyms(args, env).map(Some),
            // GNU push/pop/cl-incf/cl-decf are macros; expand them for
            // macroexpand-all consumers (generator.el's CPS transformer)
            // while normal evaluation keeps hitting the native forms.
            "push" if args.len() == 2 => {
                let value = args[0].clone();
                let place = args[1].clone();
                let setter = if matches!(place, Value::Symbol(_)) {
                    "setq"
                } else {
                    "setf"
                };
                Ok(Some(Value::list([
                    Value::Symbol(setter.into()),
                    place.clone(),
                    Value::list([Value::Symbol("cons".into()), value, place]),
                ])))
            }
            "pop" if args.len() == 1 => {
                let place = args[0].clone();
                let setter = if matches!(place, Value::Symbol(_)) {
                    "setq"
                } else {
                    "setf"
                };
                Ok(Some(Value::list([
                    Value::Symbol("prog1".into()),
                    Value::list([Value::Symbol("car".into()), place.clone()]),
                    Value::list([
                        Value::Symbol(setter.into()),
                        place.clone(),
                        Value::list([Value::Symbol("cdr".into()), place]),
                    ]),
                ])))
            }
            "cl-incf" | "incf" | "cl-decf" | "decf" if !args.is_empty() && args.len() <= 2 => {
                let place = args[0].clone();
                let delta = args.get(1).cloned().unwrap_or(Value::Integer(1));
                let operator = if name.ends_with("incf") { "+" } else { "-" };
                let setter = if matches!(place, Value::Symbol(_)) {
                    "setq"
                } else {
                    "setf"
                };
                Ok(Some(Value::list([
                    Value::Symbol(setter.into()),
                    place.clone(),
                    Value::list([Value::Symbol(operator.into()), place, delta]),
                ])))
            }
            // GNU setf is a macro; with plain symbol places it expands to
            // setq (the CPS transformer only understands the expansion).
            "setf"
                if !args.is_empty()
                    && args.len().is_multiple_of(2)
                    && args
                        .iter()
                        .step_by(2)
                        .all(|place| matches!(place, Value::Symbol(_))) =>
            {
                let mut rebuilt = vec![Value::Symbol("setq".into())];
                rebuilt.extend(args.iter().cloned());
                Ok(Some(Value::list(rebuilt)))
            }
            // prog2 reaches generator.el's CPS transformer, which only
            // understands progn/prog1; give it the equivalent expansion.
            "prog2" if args.len() >= 2 => {
                let mut prog1_form = vec![Value::Symbol("prog1".into())];
                prog1_form.extend(args[1..].iter().cloned());
                Ok(Some(Value::list([
                    Value::Symbol("progn".into()),
                    args[0].clone(),
                    Value::list(prog1_form),
                ])))
            }
            // GNU cl-symbol-macrolet substitutes variable references in
            // the body during macroexpansion (generator.el's variable
            // renaming builds on it).
            "cl-symbol-macrolet" if args.len() >= 2 => {
                let mut substitutions = Vec::new();
                for binding in args[0].to_vec().unwrap_or_default() {
                    let parts = binding.to_vec().unwrap_or_default();
                    if let (Some(Value::Symbol(name)), Some(expansion)) =
                        (parts.first(), parts.get(1))
                    {
                        substitutions.push((name.clone(), expansion.clone()));
                    }
                }
                let mut forms = vec![Value::Symbol("progn".into())];
                let mut failure = None;
                for body_form in &args[1..] {
                    let substituted = substitute_symbol_macros(body_form, &substitutions);
                    match self.macroexpand_all_form_with_environment(&substituted, None, env) {
                        Ok(expanded) => forms.push(expanded),
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                if let Some(error) = failure {
                    return Err(error);
                }
                Ok(Some(Value::list(forms)))
            }
            // GNU cl-macrolet is a macro: its expansion is the body,
            // macroexpanded with the local macros in effect (generator.el's
            // CPS transformer relies on `macroexpand' doing this).
            "cl-macrolet" if args.len() >= 2 => {
                let local_macros = self.parse_cl_macrolet_bindings(&args[0])?;
                let (local_start, local_count) = self.push_local_macros(&local_macros);
                let mut forms = vec![Value::Symbol("progn".into())];
                let mut failure = None;
                for body_form in &args[1..] {
                    match self.macroexpand_all_form_with_environment(body_form, None, env) {
                        Ok(expanded) => forms.push(expanded),
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                self.drain_local_macros(local_start, local_count);
                if let Some(error) = failure {
                    return Err(error);
                }
                Ok(Some(Value::list(forms)))
            }
            "ert-simulate-keys" => self.expand_ert_simulate_keys(args).map(Some),
            "c-lang-const" => self.expand_c_lang_const(args, env).map(Some),
            "c-lang-defconst-eval-immediately" => self
                .expand_c_lang_defconst_eval_immediately(args, env)
                .map(Some),
            "letrec" if !self.has_lisp_macro("letrec") => self.expand_letrec(args).map(Some),
            "cl-defstruct" => {
                // GNU cl-defstruct signals at expansion time when the name
                // fails cl--struct-name-p (nil, keyword, or built-in type).
                let struct_name = args.first().and_then(|spec| match spec {
                    Value::Symbol(name) => Some(name.clone()),
                    Value::Cons(_, _) => spec
                        .car()
                        .ok()
                        .and_then(|head| head.as_symbol().ok().map(str::to_string)),
                    _ => None,
                });
                if let Some(name) = struct_name
                    && (name == "nil"
                        || name.starts_with(':')
                        || crate::lisp::primitives::is_builtin_class_name(&name))
                {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("cl-struct-name-p".into()),
                        Value::Symbol(name),
                        Value::Symbol("name".into()),
                    ])));
                }
                // GNU's expansion ultimately defines every generated
                // function through `defalias'; find-func's
                // macro-expanding search looks for those subforms.  Emit
                // GNU-shaped stubs ahead of the native definer.
                Ok(Some(Self::cl_defstruct_expansion_with_stubs(args)))
            }
            "define-derived-mode" => {
                // Same shape rationale as cl-defstruct: GNU's expansion
                // carries the mode function `defalias' and the
                // MODE-hook/-map/-syntax-table `defvar's that find-func's
                // macro-expanding search looks for.
                let Some(mode) = args.first().and_then(|m| m.as_symbol().ok()) else {
                    return Ok(None);
                };
                let mut forms = vec![
                    Value::Symbol("progn".into()),
                    Value::list([
                        Value::Symbol("defalias".into()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol(mode.to_string()),
                        ]),
                        Value::list([
                            Value::Symbol("function".into()),
                            Value::Symbol("ignore".into()),
                        ]),
                    ]),
                ];
                for suffix in ["hook", "map", "syntax-table", "abbrev-table"] {
                    forms.push(Value::list([
                        Value::Symbol("defvar".into()),
                        Value::Symbol(format!("{mode}-{suffix}")),
                        Value::Nil,
                    ]));
                }
                forms.push(Value::list(
                    std::iter::once(Value::Symbol("emaxx--define-derived-mode".into()))
                        .chain(args.iter().cloned())
                        .collect::<Vec<_>>(),
                ));
                Ok(Some(Value::list(forms)))
            }
            "named-let" => self.expand_named_let(args).map(Some),
            "with-wrapper-hook" => self.expand_with_wrapper_hook(args).map(Some),
            "subr--with-wrapper-hook-no-warnings" => {
                self.expand_subr_with_wrapper_hook(args).map(Some)
            }
            "with-selected-frame" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(
                        "with-selected-frame".into(),
                        0,
                    ));
                }
                let body = &args[1..];
                Ok(Some(match body {
                    [] => Value::Nil,
                    [single] => single.clone(),
                    _ => Value::list(
                        std::iter::once(Value::Symbol("progn".into())).chain(body.iter().cloned()),
                    ),
                }))
            }
            _ => Ok(None),
        }
    }

    fn expand_c_lang_defconst_eval_immediately(
        &mut self,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if args.len() != 1 {
            return Err(LispError::WrongNumberOfArgs(
                "c-lang-defconst-eval-immediately".into(),
                args.len(),
            ));
        }
        self.eval(&args[0], env)
    }

    fn expand_c_lang_const(&mut self, args: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if args.is_empty() || args.len() > 2 {
            return Err(LispError::WrongNumberOfArgs(
                "c-lang-const".into(),
                args.len(),
            ));
        }
        let name = args[0].as_symbol()?;
        let mode = match args.get(1) {
            Some(Value::Nil) | None => None,
            Some(value) => Some(format!("{}-mode", value.as_symbol()?)),
        };
        if self
            .lookup_var("c-lang-const-expansion", env)
            .is_some_and(|value| matches!(value, Value::Symbol(symbol) if symbol == "immediate"))
        {
            let mut call = vec![
                Value::Symbol("c-get-lang-constant".into()),
                quoted_literal(&Value::Symbol(name.into())),
                Value::Nil,
            ];
            if let Some(mode) = mode {
                call.push(quoted_literal(&Value::Symbol(mode)));
            }
            let value = self.eval(&Value::list(call), env)?;
            return Ok(quoted_literal(&value));
        }

        let mut call = vec![
            Value::Symbol("c-get-lang-constant".into()),
            quoted_literal(&Value::Symbol(name.into())),
        ];
        if let Some(mode) = mode {
            call.push(Value::Nil);
            call.push(quoted_literal(&Value::Symbol(mode)));
        }
        Ok(Value::list(call))
    }

    pub(super) fn expand_with_wrapper_hook(&mut self, args: &[Value]) -> Result<Value, LispError> {
        if args.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-wrapper-hook".into(),
                args.len(),
            ));
        }
        Ok(Value::list(
            std::iter::once(Value::Symbol("subr--with-wrapper-hook-no-warnings".into()))
                .chain(args.iter().cloned()),
        ))
    }

    pub(super) fn expand_subr_with_wrapper_hook(
        &mut self,
        args: &[Value],
    ) -> Result<Value, LispError> {
        if args.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "subr--with-wrapper-hook-no-warnings".into(),
                args.len(),
            ));
        }

        let hook = args[0].clone();
        let arg_list = args[1].clone();
        let body = &args[2..];
        let funs = self.make_generated_symbol("funs");
        let global = self.make_generated_symbol("global");
        let argssym = self.make_generated_symbol("args");
        let runrestofhook = self.make_generated_symbol("runrestofhook");

        let lambda_body = Value::list([
            Value::Symbol("if".into()),
            Value::list([Value::Symbol("consp".into()), funs.clone()]),
            Value::list([
                Value::Symbol("if".into()),
                Value::list([
                    Value::Symbol("eq".into()),
                    Value::T,
                    Value::list([Value::Symbol("car".into()), funs.clone()]),
                ]),
                Value::list([
                    Value::Symbol("funcall".into()),
                    runrestofhook.clone(),
                    Value::list([
                        Value::Symbol("append".into()),
                        global.clone(),
                        Value::list([Value::Symbol("cdr".into()), funs.clone()]),
                    ]),
                    Value::Nil,
                    argssym.clone(),
                ]),
                Value::list([
                    Value::Symbol("apply".into()),
                    Value::list([Value::Symbol("car".into()), funs.clone()]),
                    Value::list([
                        Value::Symbol("apply-partially".into()),
                        Value::list([
                            Value::Symbol("lambda".into()),
                            Value::list([
                                funs.clone(),
                                global.clone(),
                                Value::Symbol("&rest".into()),
                                argssym.clone(),
                            ]),
                            Value::list([
                                Value::Symbol("funcall".into()),
                                runrestofhook.clone(),
                                funs.clone(),
                                global.clone(),
                                argssym.clone(),
                            ]),
                        ]),
                        Value::list([Value::Symbol("cdr".into()), funs.clone()]),
                        global.clone(),
                    ]),
                    argssym.clone(),
                ]),
            ]),
            Value::list([
                Value::Symbol("apply".into()),
                Value::list(
                    std::iter::once(Value::Symbol("lambda".into()))
                        .chain(std::iter::once(arg_list))
                        .chain(body.iter().cloned()),
                ),
                argssym.clone(),
            ]),
        ]);

        let global_form = match &hook {
            Value::Symbol(_) => Value::list([
                Value::Symbol("if".into()),
                Value::list([
                    Value::Symbol("local-variable-p".into()),
                    quoted_literal(&hook),
                ]),
                Value::list([Value::Symbol("default-value".into()), quoted_literal(&hook)]),
            ]),
            _ => Value::Nil,
        };

        let wrapper_args = args[1].to_vec()?;

        Ok(Value::list([
            Value::Symbol("letrec".into()),
            Value::list([Value::list([
                runrestofhook.clone(),
                Value::list([
                    Value::Symbol("lambda".into()),
                    Value::list([funs, global, argssym.clone()]),
                    lambda_body,
                ]),
            ])]),
            Value::list([
                Value::Symbol("funcall".into()),
                runrestofhook,
                hook,
                global_form,
                Value::list(std::iter::once(Value::Symbol("list".into())).chain(wrapper_args)),
            ]),
        ]))
    }

    pub(super) fn expand_ert_simulate_keys(&mut self, args: &[Value]) -> Result<Value, LispError> {
        if args.is_empty() {
            return Err(LispError::WrongNumberOfArgs("ert-simulate-keys".into(), 0));
        }
        let bindings = Value::list([
            Value::list([
                Value::Symbol("unread-command-events".into()),
                Value::list([
                    Value::Symbol("append".into()),
                    args[0].clone(),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::list([Value::Integer(7), Value::Integer(7), Value::Integer(7)]),
                    ]),
                ]),
            ]),
            Value::list([Value::Symbol("executing-kbd-macro".into()), Value::T]),
        ]);
        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(bindings))
                .chain(args[1..].iter().cloned()),
        ))
    }

    pub(super) fn expand_cl_with_gensyms(
        &mut self,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        let names = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("cl-with-gensyms".into(), 0))?
            .to_vec()?;
        let mut bindings = Vec::with_capacity(names.len());
        for name in names {
            let name = name.as_symbol()?.to_string();
            bindings.push(Value::list([
                Value::Symbol(name.clone()),
                Value::list([
                    Value::Symbol("quote".into()),
                    self.make_generated_symbol(&name),
                ]),
            ]));
        }
        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(Value::list(bindings)))
                .chain(args[1..].iter().cloned()),
        ))
    }

    pub(super) fn expand_letrec(&mut self, args: &[Value]) -> Result<Value, LispError> {
        let bindings = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("letrec".into(), 0))?
            .to_vec()?;
        let mut lowered_bindings = Vec::with_capacity(bindings.len());
        let mut initializers = Vec::new();

        for binding in bindings {
            match binding {
                Value::Symbol(name) => lowered_bindings.push(Value::Symbol(name)),
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad letrec binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    lowered_bindings.push(Value::Symbol(name.clone()));
                    if parts.len() > 1 {
                        initializers.push(Value::list([
                            Value::Symbol("setq".into()),
                            Value::Symbol(name),
                            parts[1].clone(),
                        ]));
                    }
                }
                other => return Err(wrong_type_argument("listp", other)),
            }
        }

        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(Value::list(lowered_bindings)))
                .chain(initializers)
                .chain(args[1..].iter().cloned()),
        ))
    }

    pub(super) fn expand_named_let(&mut self, args: &[Value]) -> Result<Value, LispError> {
        let name = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("named-let".into(), 0))?
            .as_symbol()?
            .to_string();
        let bindings = args
            .get(1)
            .ok_or_else(|| LispError::WrongNumberOfArgs("named-let".into(), 1))?
            .to_vec()?;
        let mut params = Vec::with_capacity(bindings.len());
        let mut inits = Vec::with_capacity(bindings.len());
        for binding in bindings {
            match binding {
                Value::Symbol(symbol) => {
                    params.push(Value::Symbol(symbol));
                    inits.push(Value::Nil);
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(param) = parts.first() else {
                        return Err(LispError::ReadError("bad named-let binding".into()));
                    };
                    params.push(Value::Symbol(param.as_symbol()?.to_string()));
                    inits.push(parts.get(1).cloned().unwrap_or(Value::Nil));
                }
                other => return Err(wrong_type_argument("listp", other)),
            }
        }

        if let Some(lowered) = self.try_expand_named_let_loop(&name, &params, &inits, &args[2..])? {
            return Ok(lowered);
        }

        let lambda = Value::list(
            std::iter::once(Value::Symbol("lambda".into()))
                .chain(std::iter::once(Value::list(params)))
                .chain(if args.len() > 2 {
                    args[2..].to_vec()
                } else {
                    vec![Value::Nil]
                }),
        );
        let binding = Value::list([Value::Symbol(name.clone()), lambda]);
        let call = Value::list(std::iter::once(Value::Symbol(name)).chain(inits));

        Ok(Value::list([
            Value::Symbol("letrec".into()),
            Value::list([binding]),
            call,
        ]))
    }

    pub(super) fn try_expand_named_let_loop(
        &mut self,
        name: &str,
        params: &[Value],
        inits: &[Value],
        body: &[Value],
    ) -> Result<Option<Value>, LispError> {
        let done_tag = self.make_generated_symbol("named-let-done");
        let bindings = Value::list(
            params
                .iter()
                .cloned()
                .zip(inits.iter().cloned())
                .map(|(param, init)| Value::list([param, init])),
        );

        let loop_body = match body {
            [single] => {
                if let Ok(items) = single.to_vec()
                    && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "if")
                {
                    let then_forms = items
                        .get(2)
                        .cloned()
                        .map_or_else(Vec::new, |form| vec![form]);
                    let else_forms = if items.len() > 3 {
                        items[3..].to_vec()
                    } else {
                        vec![Value::Nil]
                    };
                    if !named_let_branch_safe_for_loop(name, &then_forms)
                        || !named_let_branch_safe_for_loop(name, &else_forms)
                    {
                        return Ok(None);
                    }
                    self.expand_named_let_loop_if(name, params, &done_tag, &items)?
                } else if let Some((prefix, args)) = named_let_tail_call(name, body) {
                    self.build_named_let_rebind(params, &args, &prefix)?
                } else {
                    return Ok(None);
                }
            }
            _ => {
                if let Some((prefix, args)) = named_let_tail_call(name, body) {
                    self.build_named_let_rebind(params, &args, &prefix)?
                } else {
                    return Ok(None);
                }
            }
        };

        Ok(Some(Value::list([
            Value::Symbol("let".into()),
            bindings,
            Value::list([
                Value::Symbol("catch".into()),
                quoted_literal(&done_tag),
                Value::list([Value::Symbol("while".into()), Value::T, loop_body]),
            ]),
        ])))
    }

    pub(super) fn expand_named_let_loop_if(
        &mut self,
        name: &str,
        params: &[Value],
        done_tag: &Value,
        items: &[Value],
    ) -> Result<Value, LispError> {
        let condition = items.get(1).cloned().unwrap_or(Value::Nil);
        let then_forms = items
            .get(2)
            .cloned()
            .map_or_else(Vec::new, |form| vec![form]);
        let else_forms = if items.len() > 3 {
            items[3..].to_vec()
        } else {
            vec![Value::Nil]
        };
        let then_branch = self.build_named_let_loop_branch(name, params, done_tag, &then_forms)?;
        let else_branch = self.build_named_let_loop_branch(name, params, done_tag, &else_forms)?;
        Ok(Value::list([
            Value::Symbol("if".into()),
            condition,
            then_branch,
            else_branch,
        ]))
    }

    pub(super) fn build_named_let_loop_branch(
        &mut self,
        name: &str,
        params: &[Value],
        done_tag: &Value,
        forms: &[Value],
    ) -> Result<Value, LispError> {
        if let Some((prefix, args)) = named_let_tail_call(name, forms) {
            self.build_named_let_rebind(params, &args, &prefix)
        } else {
            Ok(Value::list([
                Value::Symbol("throw".into()),
                quoted_literal(done_tag),
                forms_to_progn(forms),
            ]))
        }
    }

    pub(super) fn build_named_let_rebind(
        &mut self,
        params: &[Value],
        args: &[Value],
        prefix: &[Value],
    ) -> Result<Value, LispError> {
        if params.len() != args.len() {
            return Err(LispError::WrongNumberOfArgs("named-let".into(), args.len()));
        }
        let temp_symbols = (0..params.len())
            .map(|_| self.make_generated_symbol("named-let-arg"))
            .collect::<Vec<_>>();
        let temp_bindings = Value::list(
            temp_symbols
                .iter()
                .cloned()
                .zip(args.iter().cloned())
                .map(|(temp, arg)| Value::list([temp, arg])),
        );
        let mut setq_items = vec![Value::Symbol("setq".into())];
        for (param, temp) in params.iter().cloned().zip(temp_symbols) {
            setq_items.push(param);
            setq_items.push(temp);
        }
        let rebind = Value::list([
            Value::Symbol("let".into()),
            temp_bindings,
            Value::list(setq_items),
        ]);
        let forms = prefix
            .iter()
            .cloned()
            .chain(std::iter::once(rebind))
            .collect::<Vec<_>>();
        Ok(forms_to_progn(&forms))
    }

    pub(super) fn expand_cl_case(
        &mut self,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        let expr = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("cl-case".into(), 0))?;
        let temp = self.make_generated_symbol("cl-case");
        let mut clauses = Vec::with_capacity(args.len().saturating_sub(1));

        for (index, clause) in args[1..].iter().enumerate() {
            let clause_items = clause.to_vec()?;
            let (keys, body) = match clause_items.split_first() {
                Some((keys, body)) => (keys, body),
                None => (&Value::Nil, &[][..]),
            };
            let test = self.expand_cl_case_clause_test(
                &temp,
                keys,
                index + 1 == args.len().saturating_sub(1),
            )?;
            let body = if body.is_empty() {
                vec![Value::Nil]
            } else {
                body.to_vec()
            };
            clauses.push(Value::list(std::iter::once(test).chain(body)));
        }

        Ok(Value::list([
            Value::Symbol("let".into()),
            Value::list([Value::list([temp.clone(), expr.clone()])]),
            Value::list(std::iter::once(Value::Symbol("cond".into())).chain(clauses)),
        ]))
    }

    pub(super) fn expand_cl_case_clause_test(
        &self,
        temp: &Value,
        keys: &Value,
        final_clause: bool,
    ) -> Result<Value, LispError> {
        if matches!(keys, Value::Symbol(name) if name == "t" || name == "otherwise") {
            if final_clause {
                return Ok(Value::T);
            }
            return Err(LispError::Signal(
                "Misplaced t or `otherwise' clause".into(),
            ));
        }

        if keys.is_nil() {
            return Ok(Value::Nil);
        }

        if let Value::Cons(_, _) = keys {
            let keys = keys.to_vec()?;
            let mut tests = Vec::with_capacity(keys.len());
            for key in keys {
                tests.push(Self::cl_case_key_test(temp, key));
            }
            return Ok(match tests.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                _ => Value::list(std::iter::once(Value::Symbol("or".into())).chain(tests)),
            });
        }

        Ok(Self::cl_case_key_test(temp, keys.clone()))
    }

    pub(super) fn cl_case_key_test(temp: &Value, key: Value) -> Value {
        Value::list([
            Value::Symbol("eql".into()),
            temp.clone(),
            quoted_literal(&key),
        ])
    }

    pub(super) fn sf_skip_unless(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Ok(Value::Nil)
        } else {
            Err(LispError::TestSkipped("Test skipped".into()))
        }
    }

    pub(super) fn sf_skip_when(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Err(LispError::TestSkipped("Test skipped".into()))
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_rx(&mut self, items: &[Value], env: &Env) -> Result<Value, LispError> {
        Ok(Value::String(rx::compile_rx_sequence(
            self,
            env,
            &items[1..],
        )?))
    }

    pub(super) fn sf_rx_define(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "rx-define needs name and definition".into(),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let binding = match &items[2..] {
            [definition] => Value::list([definition.clone()]),
            [params, definition] => Value::list([params.clone(), definition.clone()]),
            _ => {
                return Err(LispError::Signal(format!(
                    "Bad `rx' definition of {name}: {}",
                    Value::list(items[2..].iter().cloned())
                )));
            }
        };
        self.put_symbol_property(&name, "rx-definition", binding);
        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_rx_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal("rx-let needs bindings and a body".into()));
        }
        let bindings = items[1].to_vec()?;
        let mut saved = Vec::new();
        for binding in bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::Signal("Bad `rx-let' binding".into()));
            }
            let name = parts[0].as_symbol()?.to_string();
            let value = match &parts[1..] {
                [definition] => Value::list([definition.clone()]),
                [params, definition] => Value::list([params.clone(), definition.clone()]),
                _ => {
                    return Err(LispError::Signal(format!(
                        "Bad `rx-let' definition of {name}: {}",
                        Value::list(parts[1..].iter().cloned())
                    )));
                }
            };
            saved.push((
                name.clone(),
                self.get_symbol_property(&name, "rx-definition"),
            ));
            self.put_symbol_property(&name, "rx-definition", value);
        }

        let result = self.sf_progn(&items[2..], env);
        for (name, previous) in saved.into_iter().rev() {
            if let Some(previous) = previous {
                self.put_symbol_property(&name, "rx-definition", previous);
            } else {
                self.remove_symbol_property(&name, "rx-definition");
            }
        }
        result
    }
}

// GNU oclosure-lambda rejects duplicate slot INITIALIZERS at expansion
// time ("Duplicate slot: fst").
fn validate_oclosure_lambda_slots(args: &[Value]) -> Result<(), LispError> {
    let Some(spec) = args.first() else {
        return Ok(());
    };
    let Ok(spec_items) = spec.to_vec() else {
        return Ok(());
    };
    let mut seen: Vec<String> = Vec::new();
    for binding in spec_items.get(1..).unwrap_or(&[]) {
        let slot = match binding {
            Value::Symbol(name) => Some(name.clone()),
            other => other.to_vec().ok().and_then(|parts| {
                parts
                    .first()
                    .and_then(|v| v.as_symbol().ok().map(String::from))
            }),
        };
        let Some(slot) = slot else { continue };
        if seen.contains(&slot) {
            return Err(LispError::Signal(format!("Duplicate slot: {slot}")));
        }
        seen.push(slot);
    }
    Ok(())
}

impl Interpreter {
    // GNU oclosure-define rejects duplicate slot NAMES — within the new
    // slots and against inherited parent slots ("Duplicate slot name: a").
    pub(super) fn validate_oclosure_define_slots(&self, args: &[Value]) -> Result<(), LispError> {
        let Some(name_form) = args.first() else {
            return Ok(());
        };
        let mut parent: Option<String> = None;
        if let Ok(parts) = name_form.to_vec() {
            for option in parts.get(1..).unwrap_or(&[]) {
                if let Ok(option_parts) = option.to_vec()
                    && matches!(option_parts.first(), Some(Value::Symbol(key)) if key == ":parent")
                {
                    parent = option_parts
                        .get(1)
                        .and_then(|v| v.as_symbol().ok().map(String::from));
                }
            }
        }
        let mut seen: Vec<String> = parent
            .as_ref()
            .and_then(|parent| self.get_symbol_property(parent, "emaxx-oclosure-slots"))
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|value| value.as_symbol().ok().map(String::from))
            .collect();
        for slot in args.get(1..).unwrap_or(&[]) {
            let slot_name = match slot {
                Value::Symbol(name) => Some(name.clone()),
                Value::Cons(_, _) => slot.to_vec().ok().and_then(|parts| {
                    parts
                        .first()
                        .and_then(|v| v.as_symbol().ok().map(String::from))
                }),
                _ => None,
            };
            let Some(slot_name) = slot_name else { continue };
            if seen.contains(&slot_name) {
                return Err(LispError::Signal(format!(
                    "Duplicate slot name: {slot_name}"
                )));
            }
            seen.push(slot_name);
        }
        Ok(())
    }
}

fn macro_environment_expander(macro_environment: Option<&Value>, name: &str) -> Option<Value> {
    let mut entries = macro_environment?.clone();
    while let Value::Cons(_, _) = &entries {
        let entry = entries.car().ok()?;
        if let Value::Cons(_, _) = entry {
            let symbol = entry.car().ok()?;
            if symbol.as_symbol().ok()? == name {
                return entry.cdr().ok();
            }
        }
        entries = entries.cdr().ok()?;
    }
    None
}

impl Interpreter {
    // Compute the function names a cl-defstruct generates and wrap the
    // native definition in a progn of `defalias' stubs so macro-expanded
    // output has GNU's shape (the stubs are immediately overridden by the
    // native definer that follows them).
    pub(super) fn cl_defstruct_expansion_with_stubs(args: &[Value]) -> Value {
        let (name, options) = match args.first() {
            Some(Value::Symbol(name)) => (name.clone(), Vec::new()),
            Some(spec @ Value::Cons(_, _)) => {
                let parts = spec.to_vec().unwrap_or_default();
                let name = parts
                    .first()
                    .and_then(|head| head.as_symbol().ok().map(str::to_string))
                    .unwrap_or_default();
                (name, parts[1..].to_vec())
            }
            _ => (String::new(), Vec::new()),
        };
        let mut conc_name = format!("{name}-");
        let mut predicate = Some(format!("{name}-p"));
        let mut copier = Some(format!("copy-{name}"));
        let mut constructors: Vec<String> = Vec::new();
        let mut suppress_default_constructor = false;
        for option in &options {
            let Ok(parts) = option.to_vec() else { continue };
            match parts.first().and_then(|key| key.as_symbol().ok()) {
                Some(":conc-name") => {
                    conc_name = match parts.get(1) {
                        Some(Value::Symbol(prefix)) => prefix.clone(),
                        Some(Value::String(prefix)) => prefix.clone(),
                        _ => String::new(),
                    }
                }
                Some(":predicate") => {
                    predicate = parts.get(1).and_then(|v| match v {
                        Value::Symbol(name) => Some(name.clone()),
                        _ => None,
                    })
                }
                Some(":copier") => {
                    copier = parts.get(1).and_then(|v| match v {
                        Value::Symbol(name) => Some(name.clone()),
                        _ => None,
                    })
                }
                Some(":constructor") => match parts.get(1) {
                    Some(Value::Symbol(ctor)) => constructors.push(ctor.clone()),
                    Some(Value::Nil) | None => suppress_default_constructor = true,
                    _ => {}
                },
                _ => {}
            }
        }
        if !suppress_default_constructor {
            constructors.push(format!("make-{name}"));
        }
        let mut generated = constructors;
        generated.extend(predicate);
        generated.extend(copier);
        for slot in args.iter().skip(1) {
            let slot_name = match slot {
                Value::Symbol(slot_name) => Some(slot_name.clone()),
                Value::Cons(_, _) => slot
                    .car()
                    .ok()
                    .and_then(|head| head.as_symbol().ok().map(str::to_string)),
                _ => None,
            };
            if let Some(slot_name) = slot_name {
                generated.push(format!("{conc_name}{slot_name}"));
            }
        }
        let mut forms = vec![Value::Symbol("progn".into())];
        for function in generated {
            forms.push(Value::list([
                Value::Symbol("defalias".into()),
                Value::list([Value::Symbol("quote".into()), Value::Symbol(function)]),
                Value::list([
                    Value::Symbol("function".into()),
                    Value::Symbol("ignore".into()),
                ]),
            ]));
        }
        forms.push(Value::list(
            std::iter::once(Value::Symbol("emaxx--cl-defstruct".into()))
                .chain(args.iter().cloned())
                .collect::<Vec<_>>(),
        ));
        Value::list(forms)
    }
}

// Replace non-shadowed variable references to symbol-macro names with
// their expansions (GNU cl-symbol-macrolet semantics, scoped to the
// shapes generator.el produces: let/let*/lambda shadowing, quote
// opacity, and setq name rewriting).
fn substitute_symbol_macros(form: &Value, substitutions: &[(String, Value)]) -> Value {
    if substitutions.is_empty() {
        return form.clone();
    }
    match form {
        Value::Symbol(name) => substitutions
            .iter()
            .find(|(macro_name, _)| macro_name == name)
            .map(|(_, expansion)| expansion.clone())
            .unwrap_or_else(|| form.clone()),
        Value::Cons(_, _) => {
            let Ok(items) = form.to_vec() else {
                // Dotted pair: substitute both sides.
                if let Some((car, cdr)) = form.cons_values() {
                    return Value::cons(
                        substitute_symbol_macros(&car, substitutions),
                        substitute_symbol_macros(&cdr, substitutions),
                    );
                }
                return form.clone();
            };
            match items.first() {
                Some(Value::Symbol(head)) if head == "quote" => form.clone(),
                Some(Value::Symbol(head)) if head == "lambda" && items.len() >= 2 => {
                    let params: Vec<String> = items[1]
                        .to_vec()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|p| p.as_symbol().ok().map(str::to_string))
                        .collect();
                    let inner: Vec<(String, Value)> = substitutions
                        .iter()
                        .filter(|(name, _)| !params.contains(name))
                        .cloned()
                        .collect();
                    let mut rebuilt = vec![items[0].clone(), items[1].clone()];
                    for body in &items[2..] {
                        rebuilt.push(substitute_symbol_macros(body, &inner));
                    }
                    Value::list(rebuilt)
                }
                Some(Value::Symbol(head))
                    if (head == "let" || head == "let*") && items.len() >= 2 =>
                {
                    let mut bound = Vec::new();
                    let mut new_bindings = Vec::new();
                    for binding in items[1].to_vec().unwrap_or_default() {
                        match &binding {
                            Value::Symbol(name) => {
                                bound.push(name.clone());
                                new_bindings.push(binding.clone());
                            }
                            Value::Cons(_, _) => {
                                let parts = binding.to_vec().unwrap_or_default();
                                let name = parts
                                    .first()
                                    .and_then(|n| n.as_symbol().ok())
                                    .unwrap_or_default()
                                    .to_string();
                                // In let* a later initform sees earlier
                                // shadows; approximate with the outer set
                                // for `let' and the progressive set for
                                // `let*'.
                                let active: Vec<(String, Value)> = if head == "let*" {
                                    substitutions
                                        .iter()
                                        .filter(|(n, _)| !bound.contains(n))
                                        .cloned()
                                        .collect()
                                } else {
                                    substitutions.to_vec()
                                };
                                let mut rebuilt_binding = vec![parts[0].clone()];
                                for init in &parts[1..] {
                                    rebuilt_binding.push(substitute_symbol_macros(init, &active));
                                }
                                bound.push(name);
                                new_bindings.push(Value::list(rebuilt_binding));
                            }
                            _ => new_bindings.push(binding.clone()),
                        }
                    }
                    let inner: Vec<(String, Value)> = substitutions
                        .iter()
                        .filter(|(name, _)| !bound.contains(name))
                        .cloned()
                        .collect();
                    let mut rebuilt = vec![items[0].clone(), Value::list(new_bindings)];
                    for body in &items[2..] {
                        rebuilt.push(substitute_symbol_macros(body, &inner));
                    }
                    Value::list(rebuilt)
                }
                Some(Value::Symbol(head)) if head == "setq" && items.len() >= 3 => {
                    let mut rebuilt = vec![items[0].clone()];
                    for pair in items[1..].chunks(2) {
                        rebuilt.push(substitute_symbol_macros(&pair[0], substitutions));
                        if let Some(value) = pair.get(1) {
                            rebuilt.push(substitute_symbol_macros(value, substitutions));
                        }
                    }
                    Value::list(rebuilt)
                }
                _ => Value::list(
                    items
                        .iter()
                        .map(|item| substitute_symbol_macros(item, substitutions))
                        .collect::<Vec<_>>(),
                ),
            }
        }
        _ => form.clone(),
    }
}

// Turn a backquote template into constructor code, GNU bq_process style:
// `(a b ,x ,@y . z) => (append (list 'a 'b x) y 'z).  Unquoted leaves
// become quoted literals; self-evaluating atoms stay as-is.
fn backquote_template_code(template: &Value) -> Value {
    fn quote_literal(value: &Value) -> Value {
        match value {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::StringObject(_) => value.clone(),
            _ => Value::list([Value::Symbol("quote".into()), value.clone()]),
        }
    }
    if let Some((kind, inner)) = backquote_unquote_form(template) {
        if kind == "comma" {
            return inner;
        }
        // A top-level ,@ is invalid; keep it quoted.
        return quote_literal(template);
    }
    if nested_backquote_body(template).is_some() {
        return quote_literal(template);
    }
    // A quoted form (or vector literal) is only opaque when nothing
    // inside it unquotes: `',(f) reads as (quote (comma (f))) and must
    // still evaluate the unquote.
    if is_backquote_atomic_cons_tail(template) && !template_tree_unquotes(template) {
        return quote_literal(template);
    }
    if !matches!(template, Value::Cons(_, _)) {
        return quote_literal(template);
    }
    // Walk the list spine, batching plain elements into (list ...) chunks
    // and splicing ,@ elements and dotted tails through (append ...).
    let mut segments: Vec<Value> = Vec::new();
    let mut chunk: Vec<Value> = Vec::new();
    let mut tail = template.clone();
    loop {
        if let Some((kind, inner)) = backquote_unquote_form(&tail) {
            // Dotted unquote tail: `(a . ,x)
            if kind == "comma" {
                if !chunk.is_empty() {
                    segments.push(Value::list(
                        std::iter::once(Value::Symbol("list".into()))
                            .chain(chunk.drain(..))
                            .collect::<Vec<_>>(),
                    ));
                }
                segments.push(inner);
                tail = Value::Nil;
            }
            break;
        }
        match tail.cons_values() {
            Some((car, cdr)) => {
                if let Some((kind, inner)) = backquote_unquote_form(&car) {
                    if kind == "comma-at" {
                        if !chunk.is_empty() {
                            segments.push(Value::list(
                                std::iter::once(Value::Symbol("list".into()))
                                    .chain(chunk.drain(..))
                                    .collect::<Vec<_>>(),
                            ));
                        }
                        segments.push(inner);
                    } else {
                        chunk.push(inner);
                    }
                } else {
                    chunk.push(backquote_template_code(&car));
                }
                tail = cdr;
            }
            None => break,
        }
    }
    if !chunk.is_empty() {
        segments.push(Value::list(
            std::iter::once(Value::Symbol("list".into()))
                .chain(chunk.drain(..))
                .collect::<Vec<_>>(),
        ));
    }
    if !tail.is_nil() {
        segments.push(quote_literal(&tail));
    }
    match segments.len() {
        0 => Value::Nil,
        1 => segments.pop().expect("one segment"),
        _ => Value::list(
            std::iter::once(Value::Symbol("append".into()))
                .chain(segments)
                .collect::<Vec<_>>(),
        ),
    }
}

// Whether a template subtree contains a comma/comma-at marker outside
// nested backquotes.
fn template_tree_unquotes(form: &Value) -> bool {
    if backquote_unquote_form(form).is_some() {
        return true;
    }
    if nested_backquote_body(form).is_some() {
        return false;
    }
    let mut tail = form.clone();
    while let Some((car, cdr)) = tail.cons_values() {
        if template_tree_unquotes(&car) {
            return true;
        }
        if backquote_unquote_form(&cdr).is_some() {
            return true;
        }
        tail = cdr;
    }
    false
}
