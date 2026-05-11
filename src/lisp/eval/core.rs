use super::*;

impl Interpreter {
    pub fn eval(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::StringObject(_) => Ok(expr.clone()),

            Value::BuiltinFunc(_)
            | Value::Lambda(_, _, _)
            | Value::Buffer(_, _)
            | Value::Marker(_)
            | Value::Overlay(_)
            | Value::CharTable(_)
            | Value::Record(_)
            | Value::Finalizer(_) => Ok(expr.clone()),

            Value::Symbol(name) => self.lookup(name, env),

            Value::Cons(_, _) => {
                let items = expr.to_vec()?;
                if items.is_empty() {
                    return Ok(Value::Nil);
                }

                if matches!(items.first(), Some(Value::Symbol(name)) if name == "vector-literal") {
                    return Ok(expr.clone());
                }
                if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if name == "bool-vector-literal"
                ) {
                    return Ok(self.create_record("bool-vector", items[1..].to_vec()));
                }
                if is_record_literal_reader_form(expr) {
                    return self.eval_record_literal_form(&items[1..], env);
                }

                // Check for special forms first
                if let Value::Symbol(ref name) = items[0] {
                    match name.as_str() {
                        "quote" => return self.sf_quote(&items),
                        "if" | "static-if" => return self.sf_if(&items, env),
                        "if-let" => return self.sf_if_let(&items, env),
                        "if-let*" => return self.sf_if_let_star(&items, env),
                        "when" | "static-when" => return self.sf_when(&items, env),
                        "when-let" => return self.sf_when_let(&items, env),
                        "when-let*" => return self.sf_when_let_star(&items, env),
                        "unless" | "static-unless" => return self.sf_unless(&items, env),
                        "bound-and-true-p" => return self.sf_bound_and_true_p(&items, env),
                        "cond" => return self.sf_cond(&items, env),
                        "pcase" => return self.sf_pcase(&items, env),
                        "pcase-defmacro" => return self.sf_pcase_defmacro(&items, env),
                        "pcase-exhaustive" => return self.sf_pcase_exhaustive(&items, env),
                        "and-let*" => return self.sf_and_let_star(&items, env),
                        "and" => return self.sf_and(&items, env),
                        "or" => return self.sf_or(&items, env),
                        "not" => return self.sf_not(&items, env),
                        "progn" => return self.sf_progn(&items[1..], env),
                        "prog1" => return self.sf_prog1(&items, env),
                        "prog2" => return self.sf_prog2(&items, env),
                        "let" | "dlet" => return self.sf_let(&items, env),
                        "let*" => return self.sf_letstar(&items, env),
                        "cl-progv" => return self.sf_cl_progv(&items, env),
                        "pcase-let" => return self.sf_pcase_let(&items, env, false),
                        "pcase-let*" => return self.sf_pcase_let(&items, env, true),
                        "let-alist" => return self.sf_let_alist(&items, env),
                        "setq" => return self.sf_setq(&items, env),
                        "setq-default" => return self.sf_setq_default(&items, env),
                        "setq-local" => return self.sf_setq_local(&items, env),
                        "setopt" => return self.sf_setopt(&items, env),
                        "setf" => return self.sf_setf(&items, env),
                        "incf" | "cl-incf" => return self.sf_incf(&items, env, 1),
                        "decf" | "cl-decf" => return self.sf_incf(&items, env, -1),
                        "setcar" => return self.sf_setcar(&items, env),
                        "defvar" | "defconst" | "defcustom" => {
                            return self.sf_defvar(&items, env);
                        }
                        "defvar-local" => return self.sf_defvar_local(&items, env),
                        "defgroup" => return self.sf_defgroup(&items),
                        "defface" => return self.sf_defface(&items),
                        "defvar-keymap" => return self.sf_defvar_keymap(&items, env),
                        "define-short-documentation-group" => return self.sf_defgroup(&items),
                        "eval" => return self.sf_eval_function(&items, env),
                        "insert" => return self.sf_insert_function(&items, env, false, false),
                        "insert-and-inherit" => {
                            return self.sf_insert_function(&items, env, true, false);
                        }
                        "insert-char" => return self.sf_insert_char_function(&items, env),
                        "insert-before-markers" => {
                            return self.sf_insert_function(&items, env, false, true);
                        }
                        "insert-before-markers-and-inherit" => {
                            return self.sf_insert_function(&items, env, true, true);
                        }
                        "define-minor-mode"
                        | "define-globalized-minor-mode"
                        | "define-derived-mode" => {
                            return self.sf_define_mode(&items);
                        }
                        "defclass" => return self.sf_defclass(&items),
                        "defun" | "defsubst" => return self.sf_defun(&items, env),
                        "cl-defun" => return self.sf_cl_defun(&items, env),
                        "cl-defmacro" => return self.sf_cl_defmacro(&items, env),
                        "cl-generic-define-generalizer" => {
                            return self.sf_cl_generic_define_generalizer(&items);
                        }
                        "cl-defgeneric" => return self.sf_cl_defgeneric(&items, env),
                        "cl-defmethod" => return self.sf_cl_defmethod(&items, env),
                        "cl-generic-define-context-rewriter" => return Ok(Value::Nil),
                        "oclosure-define" => return self.sf_oclosure_define(&items),
                        "oclosure-lambda" => return self.sf_oclosure_lambda(&items, env),
                        "define-inline" => return self.sf_define_inline(&items, env),
                        "defmacro" => return self.sf_defmacro(&items),
                        "with-memoization" => return self.sf_with_memoization(&items, env),
                        "easy-menu-define" => return self.sf_easy_menu_define(&items, env),
                        "cl-defstruct" => return self.sf_cl_defstruct(&items),
                        "defalias" => return self.sf_defalias(&items, env),
                        "backquote" => return self.eval_backquote(&items[1], env),
                        "comma" => {
                            if let Some(value) = items.get(1) {
                                return self.eval(value, env);
                            }
                            return Ok(Value::Nil);
                        }
                        "lambda" => return self.sf_lambda(&items, env),
                        "call-interactively" => {
                            return self.sf_call_interactively(&items, env);
                        }
                        "function" | "function-quote" => {
                            // #'foo or (function foo)
                            if items.len() >= 2 {
                                if let Value::Symbol(name) = &items[1] {
                                    return Ok(Value::Symbol(name.clone()));
                                }
                                if let Ok(name) = function_name_from_binding_form(&items[1]) {
                                    return Ok(Value::Symbol(name));
                                }
                                return self.eval(&items[1], env);
                            }
                            return Ok(Value::Nil);
                        }
                        "while" => return self.sf_while(&items, env),
                        "dolist" => return self.sf_dolist(&items, env),
                        "pcase-dolist" => return self.sf_pcase_dolist(&items, env),
                        "dotimes" => return self.sf_dotimes(&items, env),
                        "cl-loop" => return self.sf_cl_loop(&items, env),
                        "unwind-protect" => return self.sf_unwind_protect(&items, env),
                        "ignore-error" => return self.sf_ignore_error(&items, env),
                        "ignore-errors" => return self.sf_ignore_errors(&items, env),
                        "condition-case" | "condition-case-unless-debug" => {
                            return self.sf_condition_case(&items, env);
                        }
                        "handler-bind" => return self.sf_handler_bind(&items, env),
                        "cl-assert" => return self.sf_cl_assert(&items, env),
                        "with-temp-buffer" => return self.sf_with_temp_buffer(&items, env),
                        "ert-with-test-buffer" => {
                            return self.sf_ert_with_test_buffer(&items, env);
                        }
                        "ert-with-temp-directory" => {
                            return self.sf_ert_with_temp_directory(&items, env);
                        }
                        "ert-with-message-capture" => {
                            return self.sf_ert_with_message_capture(&items, env);
                        }
                        "with-environment-variables" => {
                            return self.sf_with_environment_variables(&items, env);
                        }
                        "with-output-to-string" => {
                            return self.sf_with_output_to_string(&items, env);
                        }
                        "with-mutex" => return self.sf_with_mutex(&items, env),
                        "with-temp-file" => return self.sf_with_temp_file(&items, env),
                        "ert-with-temp-file" => return self.sf_ert_with_temp_file(&items, env),
                        "with-current-buffer" => return self.sf_with_current_buffer(&items, env),
                        "with-restriction" => return self.sf_with_restriction(&items, env),
                        "without-restriction" => return self.sf_without_restriction(&items, env),
                        "add-function" => return self.sf_add_function(&items, env),
                        "with-selected-window" => return self.sf_progn(&items[2..], env),
                        "with-syntax-table" => return self.sf_with_syntax_table(&items, env),
                        "save-match-data" => return self.sf_save_match_data(&items, env),
                        "save-excursion" => return self.sf_save_excursion(&items, env),
                        "save-window-excursion" => {
                            return self.sf_save_window_excursion(&items, env);
                        }
                        "save-current-buffer" => return self.sf_save_current_buffer(&items, env),
                        "save-restriction" => return self.sf_save_restriction(&items, env),
                        "with-suppressed-warnings" => {
                            return self.sf_with_suppressed_warnings(&items, env);
                        }
                        "with-demoted-errors" => {
                            return self.sf_with_demoted_errors(&items, env);
                        }
                        "with-coding-priority" => {
                            return self.sf_with_coding_priority(&items, env);
                        }
                        "with-silent-modifications" => {
                            return self.sf_with_silent_modifications(&items, env);
                        }
                        "combine-change-calls" => return self.sf_combine_change_calls(&items, env),
                        "cl-destructuring-bind" => {
                            return self.sf_cl_destructuring_bind(&items, env);
                        }
                        "cl-letf" => return self.sf_cl_letf(&items, env),
                        "aset" => return self.sf_aset(&items, env),
                        "cl-flet" | "cl-labels" => return self.sf_cl_flet(&items, env),
                        "cl-macrolet" => return self.sf_cl_macrolet(&items, env),
                        "cl-symbol-macrolet" => return self.sf_cl_symbol_macrolet(&items, env),
                        "push" => {
                            // (push NEWELT PLACE)
                            if items.len() < 3 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "push".into(),
                                    items.len() - 1,
                                ));
                            }
                            let val = self.eval(&items[1], env)?;
                            let place = self.resolve_setf_place(&items[2], env)?;
                            let cur = self.eval_resolved_setf_place_current_value(&place, env)?;
                            let new_val = Value::cons(val, cur);
                            self.set_resolved_setf_place_value(&place, new_val.clone(), env)?;
                            return Ok(new_val);
                        }
                        "cl-pushnew" => return self.sf_cl_pushnew(&items, env),
                        "pop" => {
                            // (pop PLACE)
                            if items.len() < 2 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "pop".into(),
                                    items.len() - 1,
                                ));
                            }
                            let place = self.resolve_setf_place(&items[1], env)?;
                            let cur = self.eval_resolved_setf_place_current_value(&place, env)?;
                            let result = cur.car()?;
                            let rest = cur.cdr()?;
                            self.set_resolved_setf_place_value(&place, rest, env)?;
                            return Ok(result);
                        }
                        "catch" => return self.sf_catch(&items, env),
                        "add-to-list" => return self.sf_add_to_list(&items, env),
                        "ert-deftest" => return self.sf_ert_deftest(&items, env),
                        "should" => return self.sf_should(&items, env),
                        "should-not" => return self.sf_should_not(&items, env),
                        "should-error" => return self.sf_should_error(&items, env),
                        "skip-unless" | "ert--skip-unless" => {
                            return self.sf_skip_unless(&items, env);
                        }
                        "skip-when" | "ert--skip-when" => return self.sf_skip_when(&items, env),
                        "rx" => return self.sf_rx(&items, env),
                        "rx-define" => return self.sf_rx_define(&items),
                        "require" => {
                            if let Some(feature_expr) = items.get(1) {
                                let feature_value = self.eval(feature_expr, env)?;
                                let feature = feature_value.as_symbol()?.to_string();
                                let target = match items.get(2) {
                                    Some(expr) => {
                                        let value = self.eval(expr, env)?;
                                        if value.is_nil() {
                                            None
                                        } else {
                                            Some(primitives::string_text(&value)?)
                                        }
                                    }
                                    None => None,
                                };
                                return self
                                    .require_feature_with_target(&feature, target.as_deref());
                            }
                            return Ok(Value::Nil);
                        }
                        "provide" => {
                            if let Some(feature_expr) = items.get(1) {
                                let feature_value = self.eval(feature_expr, env)?;
                                let feature = feature_value.as_symbol()?.to_string();
                                return self.provide_feature_with_after_load(&feature);
                            }
                            return Ok(Value::Nil);
                        }
                        "with-eval-after-load" => {
                            return self.sf_with_eval_after_load(&items, env);
                        }
                        "with-no-warnings" => return self.sf_progn(&items[1..], env),
                        "declare"
                        | "declare-function"
                        | "cl-declaim"
                        | "declaim"
                        | "cl-deftype"
                        | "def-edebug-elem-spec"
                        | "def-edebug-spec" => {
                            return Ok(Value::Nil);
                        }
                        "eval-and-compile" | "eval-when-compile" => {
                            return self.sf_progn(&items[1..], env);
                        }
                        "ert-info" => {
                            // (ert-info (msg) body...) — just run the body
                            return self.sf_progn(&items[2..], env);
                        }
                        "minibuffer-with-setup-hook" => {
                            if items.len() < 3 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "minibuffer-with-setup-hook".into(),
                                    items.len().saturating_sub(1),
                                ));
                            }
                            let hook = self.eval(&items[1], env)?;
                            let call = vec![hook];
                            self.eval_call(&call, env)?;
                            return self.sf_progn(&items[2..], env);
                        }
                        _ => {}
                    }
                }

                // Check for macro expansion
                if let Value::Symbol(name) = &items[0]
                    && let Some(expanded) = self.try_macroexpand(name, &items[1..], env)?
                {
                    return self.eval(&expanded, env);
                }

                // Regular function call
                self.eval_call(&items, env)
            }
        }
    }

    pub(super) fn eval_call(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let func = if let Value::Symbol(name) = &items[0] {
            self.lookup_function(name, env)?
        } else {
            self.eval(&items[0], env)?
        };
        let mut args = Vec::new();
        for item in &items[1..] {
            args.push(self.eval(item, env)?);
        }
        let original_name = match &items[0] {
            Value::Symbol(name) => Some(name.as_str()),
            _ => None,
        };
        self.call_function_value(func, original_name, &args, env)
    }

    pub fn call_function_value(
        &mut self,
        func: Value,
        original_name: Option<&str>,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let func = match func {
            Value::Symbol(name) => self.lookup_function(&name, env)?,
            other => other,
        };
        let func = match func {
            Value::Cons(_, _) => {
                let func = if is_lambda_form(&func) {
                    self.eval(&func, env)?
                } else {
                    func
                };
                if let Some((file, _, _)) = crate::lisp::primitives::autoload_parts(&func) {
                    let Some(name) = original_name else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("invalid-function".into()),
                            func,
                        ])));
                    };
                    self.load_target(&file)?;
                    self.lookup_function(name, env)?
                } else {
                    func
                }
            }
            other => other,
        };

        match func {
            Value::BuiltinFunc(ref name) if name == "selected-window" => {
                if !args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.clone(), args.len()));
                }
                Ok(self.selected_window_value())
            }
            Value::BuiltinFunc(ref name) => match primitives::call(self, name, args, env) {
                Ok(value) => Ok(value),
                Err(error) => self.dispatch_handler_bindings(error, env),
            },
            Value::Record(id)
                if self
                    .find_record(id)
                    .is_some_and(|record| record.type_name == "byte-code-function") =>
            {
                let Some(record) = self.find_record(id) else {
                    unreachable!("checked record presence");
                };
                let Some(inner) = record.slots.first() else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("invalid-function".into()),
                        Value::Record(id),
                    ])));
                };
                self.call_function_value(inner.clone(), original_name, args, env)
            }
            Value::Lambda(ref params, ref body, ref closure_env) => {
                if params.len() != args.len() {
                    let min_params = params
                        .iter()
                        .position(|p| p == "&optional" || p == "&rest")
                        .unwrap_or(params.len());
                    if args.len() < min_params {
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
                    }
                }

                let mut frame = Vec::new();
                let mut arg_idx = 0;
                let mut optional = false;
                let mut rest = false;

                for param in params {
                    if param == "&optional" {
                        optional = true;
                        continue;
                    }
                    if param == "&rest" {
                        rest = true;
                        continue;
                    }
                    if rest {
                        let rest_args: Vec<Value> = args.get(arg_idx..).unwrap_or(&[]).to_vec();
                        frame.push((param.clone(), Self::stored_value(Value::list(rest_args))));
                        break;
                    }
                    let consumed_arg = arg_idx < args.len();
                    let val = if consumed_arg {
                        args[arg_idx].clone()
                    } else if optional {
                        Value::Nil
                    } else {
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
                    };
                    frame.push((param.clone(), Self::stored_value(val)));
                    if consumed_arg {
                        arg_idx += 1;
                    }
                }

                let backtrace_function = original_name
                    .map(|name| Value::Symbol(name.to_string()))
                    .unwrap_or_else(|| func.clone());
                self.push_backtrace_frame(backtrace_function, args.to_vec());
                let captured_snapshot = closure_env.borrow().clone();
                let result = if captured_snapshot.is_empty() {
                    let mut call_env = env.clone();
                    call_env.push(frame);
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    call_env.pop();
                    env.clear();
                    env.extend(call_env);
                    result
                } else {
                    let frame_mapping = Self::align_captured_frames(&captured_snapshot, env);
                    let mut call_env =
                        Self::merge_lexical_lambda_env(env, &captured_snapshot, &frame_mapping);
                    call_env.push(frame);
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    call_env.pop();
                    {
                        let mut stored_env = closure_env.borrow_mut();
                        if stored_env.len() != captured_snapshot.len() {
                            stored_env.clear();
                            stored_env.extend(captured_snapshot.clone());
                        }
                        for (captured_index, updated) in call_env.iter().enumerate() {
                            if captured_index >= stored_env.len() {
                                break;
                            }
                            stored_env[captured_index] = updated.clone();
                            if let Some(current_index) = frame_mapping[captured_index]
                                && current_index < env.len()
                            {
                                env[current_index] = updated.clone();
                            }
                        }
                    }
                    result
                };
                self.pop_backtrace_frame();
                result
            }
            other => Err(LispError::SignalValue(Value::list([
                Value::Symbol("invalid-function".into()),
                other,
            ]))),
        }
    }

    // ── Special forms ──
}
