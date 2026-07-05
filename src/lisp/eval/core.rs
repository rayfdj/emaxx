use super::*;
use crate::lisp::types::SharedEnv;

fn byte_code_function_uses_dynamic_binding(record: &RecordState) -> bool {
    matches!(record.slots.get(2), Some(Value::Symbol(symbol)) if symbol == "dynamic-binding")
}

impl Interpreter {
    // This evaluator recurses once per subform rather than once per
    // funcall/eval level like GNU Emacs, so the same Lisp program nests
    // several times deeper here.  Scale the user-visible limit so honest
    // deep recursion still fits while runaway recursion keeps signaling
    // the GNU error instead of exhausting the Rust stack.
    // GNU 30 additionally grows `max-lisp-eval-depth' dynamically while C
    // stack remains, so honest recursion tens of thousands of calls deep
    // succeeds interpreted (cl-macs--labels recurses 42000 deep); the batch
    // thread's large stack backs the same headroom here.
    const LISP_EVAL_DEPTH_SCALE: usize = 384;

    pub fn eval(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        if !matches!(expr, Value::Cons(_, _)) {
            return self.eval_inner(expr, env);
        }
        self.lisp_eval_depth += 1;
        if self.lisp_eval_depth > 800 * Self::LISP_EVAL_DEPTH_SCALE
            && self.lisp_eval_depth > self.max_lisp_eval_depth()
        {
            self.lisp_eval_depth -= 1;
            return Err(LispError::Signal(
                "Lisp nesting exceeds `max-lisp-eval-depth'".into(),
            ));
        }
        let result = self.eval_inner(expr, env);
        self.lisp_eval_depth -= 1;
        result
    }

    fn max_lisp_eval_depth(&self) -> usize {
        self.global_value("max-lisp-eval-depth")
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(1600)
            .saturating_mul(Self::LISP_EVAL_DEPTH_SCALE)
    }

    fn eval_inner(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::StringObject(_) => Ok(expr.clone()),

            // Evaluating a string literal yields a string object with its
            // own identity, so `eq' distinguishes evaluations of distinct
            // literals while `(memq (car l) l)' still finds the element the
            // evaluation put there (GNU strings are always heap objects).
            Value::String(_) => Ok(Self::stored_value(expr.clone())),

            Value::BuiltinFunc(_)
            | Value::Lambda(_, _, _)
            | Value::Buffer(_, _)
            | Value::Marker(_)
            | Value::Overlay(_)
            | Value::CharTable(_)
            | Value::Record(_)
            | Value::Finalizer(_)
            | Value::Unbound => Ok(expr.clone()),

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
                        "cond" => {
                            // Keep the in-progress form visible in
                            // backtraces like GNU's eval frames.
                            self.push_backtrace_frame_with_evald(
                                items[0].clone(),
                                items[1..].to_vec(),
                                false,
                            );
                            let result = self.sf_cond(&items, env);
                            self.pop_backtrace_frame();
                            return result;
                        }
                        "pcase" => return self.sf_pcase(&items, env),
                        "pcase-defmacro" => return self.sf_pcase_defmacro(&items, env),
                        "pcase-exhaustive" => return self.sf_pcase_exhaustive(&items, env),
                        "and-let*" => return self.sf_and_let_star(&items, env),
                        "and" => return self.sf_and(&items, env),
                        "or" => return self.sf_or(&items, env),
                        "not" => return self.sf_not(&items, env),
                        "progn" => return self.sf_progn(&items[1..], env),
                        "delay-mode-hooks" => {
                            Self::push_marked_frame(
                                env,
                                vec![("delay-mode-hooks".into(), Value::T)],
                            );
                            let result = self.sf_progn(&items[1..], env);
                            env.pop();
                            return result;
                        }
                        "atomic-change-group" => {
                            return self.sf_atomic_change_group(&items[1..], env);
                        }
                        "cl-return" => return self.sf_cl_return(&items, env),
                        "cl-return-from" => return self.sf_cl_return_from(&items, env),
                        "throw" => return self.sf_throw(&items, env),
                        "prog1" => return self.sf_prog1(&items, env),
                        "prog2" => return self.sf_prog2(&items, env),
                        "let" | "dlet" => {
                            // Keep the in-progress form visible in
                            // backtraces like GNU's eval frames.
                            self.push_backtrace_frame_with_evald(
                                items[0].clone(),
                                items[1..].to_vec(),
                                false,
                            );
                            let result = self.sf_let(&items, env);
                            self.pop_backtrace_frame();
                            return result;
                        }
                        "letrec" => return self.sf_letrec(&items, env),
                        "let*" => {
                            // Keep the in-progress form visible in
                            // backtraces like GNU's eval frames.
                            self.push_backtrace_frame_with_evald(
                                items[0].clone(),
                                items[1..].to_vec(),
                                false,
                            );
                            let result = self.sf_letstar(&items, env);
                            self.pop_backtrace_frame();
                            return result;
                        }
                        "cl-progv" => return self.sf_cl_progv(&items, env),
                        "pcase-let" => return self.sf_pcase_let(&items, env, false),
                        "pcase-let*" => return self.sf_pcase_let(&items, env, true),
                        "let-alist" => return self.sf_let_alist(&items, env),
                        "setq" => {
                            self.push_backtrace_frame_with_evald(
                                Value::Symbol("setq".into()),
                                items[1..].to_vec(),
                                false,
                            );
                            let result = self.sf_setq(&items, env);
                            self.pop_backtrace_frame();
                            return result;
                        }
                        "setq-default" => return self.sf_setq_default(&items, env),
                        "setq-local" => return self.sf_setq_local(&items, env),
                        "setopt" => return self.sf_setopt(&items, env),
                        "setf" => return self.sf_setf(&items, env),
                        "incf" | "cl-incf" => return self.sf_incf(&items, env, 1),
                        "decf" | "cl-decf" => return self.sf_incf(&items, env, -1),
                        "cl-callf" => return self.sf_cl_callf(&items, env),
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
                        "define-advice" => return self.sf_define_advice(&items, env),
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
                        "backquote" | "`" => return self.eval_backquote(&items[1], env),
                        "comma" | "," => {
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
                        "while" => {
                            // Keep the in-progress form visible in
                            // backtraces like GNU's eval frames.
                            self.push_backtrace_frame_with_evald(
                                items[0].clone(),
                                items[1..].to_vec(),
                                false,
                            );
                            let result = self.sf_while(&items, env);
                            self.pop_backtrace_frame();
                            return result;
                        }
                        "dolist" => {
                            // Keep the in-progress form visible in
                            // backtraces like GNU's eval frames.
                            self.push_backtrace_frame_with_evald(
                                items[0].clone(),
                                items[1..].to_vec(),
                                false,
                            );
                            let result = self.sf_dolist(&items, env);
                            self.pop_backtrace_frame();
                            return result;
                        }
                        "dolist-with-progress-reporter" => {
                            return self.sf_dolist_with_progress_reporter(&items, env);
                        }
                        "pcase-dolist" => return self.sf_pcase_dolist(&items, env),
                        "dotimes" => return self.sf_dotimes(&items, env),
                        // The preloaded GNU `cl-loop' macro takes precedence;
                        // the native special form remains as a bootstrap
                        // fallback before simple_compat.el is loaded.
                        "cl-loop" if !self.has_lisp_macro("cl-loop") => {
                            return self.sf_cl_loop(&items, env);
                        }
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
                        "with-current-buffer-window" => {
                            return self.sf_with_current_buffer_window(&items, env);
                        }
                        "with-restriction" => return self.sf_with_restriction(&items, env),
                        "without-restriction" => return self.sf_without_restriction(&items, env),
                        "add-function" => return self.sf_add_function(&items, env),
                        "with-selected-window" => return self.sf_with_selected_window(&items, env),
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
                        "cl-flet" if !self.has_lisp_macro("cl-flet") => {
                            return self.sf_cl_flet(&items, env);
                        }
                        "cl-labels" if !self.has_lisp_macro("cl-labels") => {
                            return self.sf_cl_labels(&items, env);
                        }
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
                        "rx-let" => return self.sf_rx_let(&items, env),
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
                                return self.require_feature_with_target(
                                    &feature,
                                    target.as_deref(),
                                    env,
                                );
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
                        "declare" | "declare-function" | "cl-declaim" | "declaim" => {
                            return Ok(Value::Nil);
                        }
                        "def-edebug-spec" => {
                            // (def-edebug-spec SYMBOL SPEC), both unevaluated.
                            if let (Some(symbol), Some(spec)) = (items.get(1), items.get(2))
                                && let Ok(symbol_name) = symbol.as_symbol()
                            {
                                let symbol_name = symbol_name.to_string();
                                self.put_symbol_property(
                                    &symbol_name,
                                    "edebug-form-spec",
                                    spec.clone(),
                                );
                            }
                            return Ok(Value::Nil);
                        }
                        "def-edebug-elem-spec" => {
                            return self.sf_def_edebug_elem_spec(&items, env);
                        }
                        "cl-deftype" => return self.sf_cl_deftype(&items, env),
                        "eval-and-compile" | "eval-when-compile" => {
                            return self.sf_progn(&items[1..], env);
                        }
                        "while-no-input" => return self.sf_progn(&items[1..], env),
                        "ert-info" => {
                            // (ert-info (MESSAGE-FORM &key ((:prefix P) "Info: "))
                            //   BODY...): push (PREFIX . MESSAGE) onto the
                            // `ert--infos' the failure reporter displays.
                            let spec = items
                                .get(1)
                                .and_then(|value| value.to_vec().ok())
                                .unwrap_or_default();
                            let message_form = spec.first().cloned().unwrap_or(Value::Nil);
                            let mut prefix_form = Value::String("Info: ".into());
                            let mut index = 1usize;
                            while index + 1 < spec.len() {
                                if matches!(&spec[index], Value::Symbol(key) if key == ":prefix") {
                                    prefix_form = spec[index + 1].clone();
                                }
                                index += 2;
                            }
                            let message = self.eval(&message_form, env)?;
                            let prefix = self.eval(&prefix_form, env)?;
                            let existing = self.lookup_var("ert--infos", env).unwrap_or(Value::Nil);
                            let infos = Value::cons(Value::cons(prefix, message), existing);
                            // `ert--infos' is a defvar; GNU's expansion is a
                            // dynamic let so the failure handler sees it.
                            let restore = self.bind_special_variable("ert--infos", infos, env)?;
                            let result = self.sf_progn(&items[2..], env);
                            self.restore_special_binding(restore, env)?;
                            return result;
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
        // While the arguments evaluate, the call is visible in backtraces as
        // an in-progress frame with its unevaluated argument forms, the way
        // GNU records the eval of a list form.
        let unevald_frame = matches!(&items[0], Value::Symbol(_));
        if unevald_frame {
            self.push_backtrace_frame_with_evald(items[0].clone(), items[1..].to_vec(), false);
        }
        let mut args = Vec::new();
        let mut arg_error = None;
        for item in &items[1..] {
            match self.eval(item, env) {
                Ok(value) => args.push(value),
                Err(error) => {
                    arg_error = Some(error);
                    break;
                }
            }
        }
        if unevald_frame {
            self.pop_backtrace_frame();
        }
        if let Some(error) = arg_error {
            return Err(error);
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
        let mut resolved_original_name = original_name.map(str::to_string);
        let func = match func {
            Value::Symbol(name) => {
                if resolved_original_name.is_none() {
                    resolved_original_name = Some(name.clone());
                }
                self.lookup_function(&name, env)?
            }
            other => other,
        };
        let original_name = resolved_original_name.as_deref();
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
            Value::BuiltinFunc(ref name) => {
                let backtrace_function = original_name
                    .map(|name| Value::Symbol(name.to_string()))
                    .unwrap_or_else(|| Value::Symbol(name.clone()));
                self.push_backtrace_frame(backtrace_function, args.to_vec());
                let result = match primitives::call(self, name, args, env) {
                    Ok(value) => Ok(value),
                    Err(error @ LispError::Throw(_, _)) => Err(error),
                    Err(error) => self.dispatch_handler_bindings(error, env),
                };
                self.pop_backtrace_frame();
                result
            }
            Value::Record(id)
                if self
                    .find_record(id)
                    .is_some_and(|record| record.type_name == "byte-code-function") =>
            {
                let (inner, uses_dynamic_binding) = {
                    let Some(record) = self.find_record(id) else {
                        unreachable!("checked record presence");
                    };
                    let Some(inner) = record.slots.first().cloned() else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("invalid-function".into()),
                            Value::Record(id),
                        ])));
                    };
                    (inner, byte_code_function_uses_dynamic_binding(record))
                };
                if uses_dynamic_binding {
                    self.push_lambda_capture_override(false);
                    let result = self.call_function_value(inner, original_name, args, env);
                    self.pop_lambda_capture_override();
                    result
                } else {
                    self.call_function_value(inner, original_name, args, env)
                }
            }
            Value::Lambda(ref params, ref body, ref closure_env) => {
                if is_semantic_lambda_params(params) {
                    return self.call_semantic_lambda(body, closure_env, args);
                }
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
                self.push_backtrace_frame_with_locals(
                    backtrace_function,
                    args.to_vec(),
                    frame.clone(),
                    true,
                );
                frame.push(Self::fresh_frame_identity());
                let previous_activation = self.enter_activation();
                let result = if closure_env.borrow().is_empty() {
                    // Truncate (not pop) at the call boundary: a non-local
                    // exit can leave binding frames above the argument frame,
                    // and those must not leak into the caller's environment.
                    let caller_len = env.len();
                    let mut call_env = env.clone();
                    call_env.push(frame);
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    call_env.truncate(caller_len);
                    env.clear();
                    env.extend(call_env);
                    result
                } else if body_has_marker(body, ":closure-transparent-env") {
                    // Advice wrappers are plumbing: run them on the caller's
                    // environment chain with the wrapper's captured frames
                    // appended, so lexical mutations made below the wrapper
                    // still reach the calling scope.
                    let caller_len = env.len();
                    let mut call_env = env.clone();
                    call_env.extend(closure_env.borrow().iter().cloned());
                    let captured_len = call_env.len();
                    call_env.push(frame);
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    call_env.truncate(captured_len);
                    let captured: Vec<_> = call_env.drain(caller_len..).collect();
                    env.clear();
                    env.extend(call_env);
                    {
                        let mut stored = closure_env.borrow_mut();
                        stored.clear();
                        stored.extend(captured);
                    }
                    result
                } else if body_has_marker(body, ":closure-isolated-current-env") {
                    let mut call_env = closure_env.borrow().clone();
                    let captured_len = call_env.len();
                    call_env.push(vec![("__closure-isolated-current-env".into(), Value::T)]);
                    call_env.push(frame);
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    call_env.truncate(captured_len);
                    result
                } else {
                    self.eval_with_closure_env(closure_env, env, |interp, call_env| {
                        let depth = call_env.len();
                        call_env.push(frame);
                        let result = interp.sf_progn(function_executable_body(body), call_env);
                        call_env.truncate(depth);
                        result
                    })
                };
                self.leave_activation(previous_activation);
                self.pop_backtrace_frame();
                result
            }
            other => Err(LispError::SignalValue(Value::list([
                Value::Symbol("invalid-function".into()),
                other,
            ]))),
        }
    }

    fn call_semantic_lambda(
        &mut self,
        body: &[Value],
        closure_env: &SharedEnv,
        args: &[Value],
    ) -> Result<Value, LispError> {
        if args.len() != 3 {
            return Err(LispError::WrongNumberOfArgs(
                "lambda".to_string(),
                args.len(),
            ));
        }
        if let Some(value) = eval_generated_semantic_lambda_body(body, args)? {
            return Ok(value);
        }
        let mut call_env = closure_env.borrow().clone();
        call_env.push(vec![
            ("vals".into(), Self::stored_value(args[0].clone())),
            ("start".into(), Self::stored_value(args[1].clone())),
            ("end".into(), Self::stored_value(args[2].clone())),
        ]);
        self.sf_progn(function_executable_body(body), &mut call_env)
    }

    // ── Special forms ──
}

fn is_semantic_lambda_params(params: &[String]) -> bool {
    params == ["vals", "start", "end"]
}

fn eval_generated_semantic_lambda_body(
    body: &[Value],
    args: &[Value],
) -> Result<Option<Value>, LispError> {
    let executable = function_executable_body(body);
    if executable.len() != 2 || !is_ignore_vals_form(&executable[0]) {
        return Ok(None);
    }
    match eval_semantic_action_expr(&executable[1], args) {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

fn is_ignore_vals_form(value: &Value) -> bool {
    value.to_vec().is_ok_and(|items| {
        matches!(
            items.as_slice(),
            [Value::Symbol(head), Value::Symbol(arg)]
                if head == "ignore" && arg == "vals"
        )
    })
}

fn eval_semantic_action_expr(expr: &Value, args: &[Value]) -> Result<Value, LispError> {
    match expr {
        Value::Symbol(symbol) if symbol == "vals" => Ok(args[0].clone()),
        Value::Symbol(symbol) if symbol == "start" => Ok(args[1].clone()),
        Value::Symbol(symbol) if symbol == "end" => Ok(args[2].clone()),
        Value::Cons(_, _) => {
            let items = expr.to_vec()?;
            let Some(Value::Symbol(head)) = items.first() else {
                return Err(LispError::Signal("unsupported semantic action".into()));
            };
            match head.as_str() {
                "quote" if items.len() == 2 => Ok(items[1].clone()),
                "nth" if items.len() == 3 => {
                    let index = match eval_semantic_action_expr(&items[1], args)? {
                        Value::Integer(index) if index >= 0 => index as usize,
                        _ => return Err(LispError::Signal("invalid nth index".into())),
                    };
                    let values = eval_semantic_action_expr(&items[2], args)?;
                    Ok(values.to_vec()?.get(index).cloned().unwrap_or(Value::Nil))
                }
                "car" if items.len() == 2 => Ok(eval_semantic_action_expr(&items[1], args)?
                    .car()
                    .unwrap_or(Value::Nil)),
                "cdr" if items.len() == 2 => Ok(eval_semantic_action_expr(&items[1], args)?
                    .cdr()
                    .unwrap_or(Value::Nil)),
                "list" => {
                    let values = items[1..]
                        .iter()
                        .map(|item| eval_semantic_action_expr(item, args))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Value::list(values))
                }
                "append" => {
                    let mut values = Vec::new();
                    for item in &items[1..] {
                        let value = eval_semantic_action_expr(item, args)?;
                        if value.is_nil() {
                            continue;
                        }
                        match value.to_vec() {
                            Ok(list) => values.extend(list),
                            Err(_) => values.push(value),
                        }
                    }
                    Ok(Value::list(values))
                }
                "cons" if items.len() == 3 => Ok(Value::cons(
                    eval_semantic_action_expr(&items[1], args)?,
                    eval_semantic_action_expr(&items[2], args)?,
                )),
                "1+" if items.len() == 2 => match eval_semantic_action_expr(&items[1], args)? {
                    Value::Integer(value) => Ok(Value::Integer(value + 1)),
                    _ => Err(LispError::Signal("invalid 1+ argument".into())),
                },
                "concat" => {
                    let mut text = String::new();
                    for item in &items[1..] {
                        text.push_str(&semantic_action_string(&eval_semantic_action_expr(
                            item, args,
                        )?)?);
                    }
                    Ok(Value::String(text))
                }
                "if" if items.len() >= 3 => {
                    if eval_semantic_action_expr(&items[1], args)?.is_truthy() {
                        eval_semantic_action_expr(&items[2], args)
                    } else if let Some(else_expr) = items.get(3) {
                        eval_semantic_action_expr(else_expr, args)
                    } else {
                        Ok(Value::Nil)
                    }
                }
                "member" if items.len() == 3 => {
                    let needle = eval_semantic_action_expr(&items[1], args)?;
                    let haystack = eval_semantic_action_expr(&items[2], args)?;
                    let found = haystack
                        .to_vec()
                        .ok()
                        .is_some_and(|items| items.iter().any(|item| item == &needle));
                    Ok(if found { Value::T } else { Value::Nil })
                }
                "delete" if items.len() == 3 => {
                    let needle = eval_semantic_action_expr(&items[1], args)?;
                    let list = eval_semantic_action_expr(&items[2], args)?;
                    let items = list
                        .to_vec()?
                        .into_iter()
                        .filter(|item| item != &needle)
                        .collect::<Vec<_>>();
                    Ok(Value::list(items))
                }
                "semantic-tag" => semantic_action_tag(&items[1..], args),
                "semantic-tag-new-variable" => {
                    semantic_action_typed_tag("variable", &items[1..], args)
                }
                "semantic-tag-new-function" => {
                    semantic_action_typed_tag("function", &items[1..], args)
                }
                "semantic-tag-new-type" => semantic_action_type_tag(&items[1..], args),
                "semantic-tag-new-include" => semantic_action_include_tag(&items[1..], args),
                _ => Err(LispError::Signal("unsupported semantic action".into())),
            }
        }
        _ => Ok(expr.clone()),
    }
}

fn semantic_action_string(value: &Value) -> Result<String, LispError> {
    match value {
        Value::String(text) | Value::Symbol(text) => Ok(text.clone()),
        Value::Integer(value) => Ok(value.to_string()),
        _ => Err(LispError::TypeError("string".into(), value.type_name())),
    }
}

fn semantic_action_tag(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    if exprs.len() < 2 {
        return Err(LispError::WrongNumberOfArgs(
            "semantic-tag".into(),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let class = eval_semantic_action_expr(&exprs[1], args)?;
    let attrs = semantic_action_plist(&exprs[2..], args)?;
    Ok(Value::list([name, class, attrs, Value::Nil, Value::Nil]))
}

fn semantic_action_typed_tag(
    class: &str,
    exprs: &[Value],
    args: &[Value],
) -> Result<Value, LispError> {
    if exprs.len() < 3 {
        return Err(LispError::WrongNumberOfArgs(
            format!("semantic-tag-new-{class}"),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let type_value = eval_semantic_action_expr(&exprs[1], args)?;
    let third_key = if class == "function" {
        ":arguments"
    } else {
        ":default-value"
    };
    let third_value = eval_semantic_action_expr(&exprs[2], args)?;
    let mut attrs = vec![
        Value::Symbol(":type".into()),
        type_value,
        Value::Symbol(third_key.into()),
        third_value,
    ];
    attrs.extend(semantic_action_plist_items(&exprs[3..], args)?);
    Ok(Value::list([
        name,
        Value::Symbol(class.into()),
        semantic_action_filter_plist(attrs),
        Value::Nil,
        Value::Nil,
    ]))
}

fn semantic_action_type_tag(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    if exprs.len() < 4 {
        return Err(LispError::WrongNumberOfArgs(
            "semantic-tag-new-type".into(),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let type_value = eval_semantic_action_expr(&exprs[1], args)?;
    let members = eval_semantic_action_expr(&exprs[2], args)?;
    let parents = eval_semantic_action_expr(&exprs[3], args)?;
    let superclasses = parents.car().unwrap_or(Value::Nil);
    let interfaces = parents.cdr().unwrap_or(Value::Nil);
    let mut attrs = vec![
        Value::Symbol(":type".into()),
        type_value,
        Value::Symbol(":members".into()),
        members,
        Value::Symbol(":superclasses".into()),
        superclasses,
        Value::Symbol(":interfaces".into()),
        interfaces,
    ];
    attrs.extend(semantic_action_plist_items(&exprs[4..], args)?);
    Ok(Value::list([
        name,
        Value::Symbol("type".into()),
        semantic_action_filter_plist(attrs),
        Value::Nil,
        Value::Nil,
    ]))
}

fn semantic_action_include_tag(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    if exprs.len() < 2 {
        return Err(LispError::WrongNumberOfArgs(
            "semantic-tag-new-include".into(),
            exprs.len(),
        ));
    }
    let name = eval_semantic_action_expr(&exprs[0], args)?;
    let system_flag = eval_semantic_action_expr(&exprs[1], args)?;
    let mut attrs = vec![Value::Symbol(":system-flag".into()), system_flag];
    attrs.extend(semantic_action_plist_items(&exprs[2..], args)?);
    Ok(Value::list([
        name,
        Value::Symbol("include".into()),
        semantic_action_filter_plist(attrs),
        Value::Nil,
        Value::Nil,
    ]))
}

fn semantic_action_plist(exprs: &[Value], args: &[Value]) -> Result<Value, LispError> {
    Ok(semantic_action_filter_plist(semantic_action_plist_items(
        exprs, args,
    )?))
}

fn semantic_action_plist_items(exprs: &[Value], args: &[Value]) -> Result<Vec<Value>, LispError> {
    exprs
        .iter()
        .map(|expr| eval_semantic_action_expr(expr, args))
        .collect()
}

fn semantic_action_filter_plist(items: Vec<Value>) -> Value {
    let mut filtered = Vec::new();
    let mut iter = items.into_iter();
    while let Some(key) = iter.next() {
        let Some(value) = iter.next() else {
            break;
        };
        let skip = value.is_nil()
            || matches!(&value, Value::String(text) if text.is_empty())
            || matches!(value, Value::Integer(0));
        if !skip {
            filtered.insert(0, value);
            filtered.insert(0, key);
        }
    }
    Value::list(filtered)
}
