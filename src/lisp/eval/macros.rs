use super::*;

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
        if let Some((kind, value)) = backquote_unquote_form(expr) {
            if depth == 0 {
                return self.eval(&value, env);
            }
            return Ok(Value::list([
                Value::Symbol(kind.into()),
                self.eval_backquote_with_depth(&value, env, depth - 1)?,
            ]));
        }

        if let Some(body) = nested_backquote_body(expr) {
            return Ok(Value::list([
                Value::Symbol("backquote".into()),
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
                                if let Ok(elems) = evaled.to_vec() {
                                    result.extend(elems);
                                }
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
        // Skip (declare ...) forms
        let body_start = if body_start < items.len() {
            if let Value::Cons(_, _) = &items[body_start] {
                if let Ok(decl) = items[body_start].to_vec() {
                    if let Some(Value::Symbol(s)) = decl.first() {
                        if s == "declare" {
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
        self.macros.push((name.clone(), params, body));
        Ok(Value::Symbol(name))
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
        let name = quoted_symbol_name(&items[1])
            .or_else(|| items[1].as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::TypeError("symbol".into(), items[1].type_name()))?;
        let function = self.eval(&items[2], env)?;
        self.validate_function_binding(&name, &function)?;
        self.set_function_binding(&name, Some(function));
        Ok(Value::Symbol(name))
    }

    pub(super) fn try_macroexpand(
        &mut self,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        let mut attempted_autoload = false;
        let (params, body) = loop {
            if let Some(expanded) = self.try_builtin_macroexpand(name, args, env)? {
                return Ok(Some(expanded));
            }

            if let Some(binding) = self.resolve_macro_binding(name) {
                break binding;
            }

            if attempted_autoload {
                return Ok(None);
            }
            let Ok(function) = self.lookup_function(name, env) else {
                return Ok(None);
            };
            let Some((file, _, _kind)) = crate::lisp::primitives::autoload_parts(&function) else {
                return Ok(None);
            };
            let loads_macro =
                crate::lisp::primitives::autoload_is_macro(self, Some(name), &function);
            if !loads_macro {
                return Ok(None);
            }
            self.load_target(&file)?;
            attempted_autoload = true;
        };

        // Bind params to unevaluated args
        let mut frame = Vec::new();
        let mut arg_idx = 0;
        let mut rest = false;

        for param in &params {
            if param == "&optional" {
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
            } else {
                Value::Nil
            };
            frame.push((param.clone(), val));
            arg_idx += 1;
        }

        env.push(frame);
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

    pub(crate) fn macroexpand_1_form(
        &mut self,
        form: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(Value::Symbol(name)) = items.first() else {
            return Ok(form.clone());
        };
        Ok(self
            .try_macroexpand(name, &items[1..], env)?
            .unwrap_or_else(|| form.clone()))
    }

    pub(crate) fn macroexpand_all_form(
        &mut self,
        form: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(head) = items.first() else {
            return Ok(Value::Nil);
        };
        if let Value::Symbol(name) = head {
            match name.as_str() {
                "quote" | "function" => return Ok(form.clone()),
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
                _ => {}
            }
            if let Some(expanded) = self.try_macroexpand(name, &items[1..], env)? {
                return self.macroexpand_all_form(&expanded, env);
            }
        }

        if matches!(head, Value::Symbol(name) if name == "lambda") {
            let mut expanded = Vec::with_capacity(items.len());
            expanded.push(items[0].clone());
            if let Some(params) = items.get(1) {
                expanded.push(params.clone());
            }
            for item in &items[2..] {
                expanded.push(self.macroexpand_all_form(item, env)?);
            }
            return Ok(Value::list(expanded));
        }

        let mut expanded = Vec::with_capacity(items.len());
        if matches!(head, Value::Symbol(_)) {
            expanded.push(items[0].clone());
            for item in &items[1..] {
                expanded.push(self.macroexpand_all_form(item, env)?);
            }
        } else {
            for item in &items {
                expanded.push(self.macroexpand_all_form(item, env)?);
            }
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
            "cl-case" => self.expand_cl_case(args, env).map(Some),
            "cl-with-gensyms" => self.expand_cl_with_gensyms(args, env).map(Some),
            "ert-simulate-keys" => self.expand_ert_simulate_keys(args).map(Some),
            "c-lang-const" => self.expand_c_lang_const(args, env).map(Some),
            "c-lang-defconst-eval-immediately" => self
                .expand_c_lang_defconst_eval_immediately(args, env)
                .map(Some),
            "letrec" => self.expand_letrec(args).map(Some),
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
