use super::*;
use crate::lisp::reader;

impl Interpreter {
    pub(super) fn sf_quote(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        // GNU's quote returns its argument as-is, sharing structure.  The
        // emaxx reader leaves marker forms (circular labels, `#s(hash-table
        // ...)' literals) that must be resolved first, but marker-free
        // templates — the common case — are returned directly.  The verdict
        // is cached per template so hot code doesn't rescan large constants.
        if let Value::Cons(car_cell, _) = &items[1] {
            let key = std::rc::Rc::as_ptr(car_cell) as usize;
            if self.plain_quote_templates.contains_key(&key) {
                return Ok(items[1].clone());
            }
            if !reader::quote_template_needs_resolution(&items[1]) {
                if self.plain_quote_templates.len() >= (1 << 20) {
                    self.plain_quote_templates.clear();
                }
                self.plain_quote_templates.insert(key, items[1].clone());
                return Ok(items[1].clone());
            }
        } else if !reader::quote_template_needs_resolution(&items[1]) {
            return Ok(items[1].clone());
        }
        let value = reader::resolve_circular_read_syntax(items[1].clone())?;
        // GNU's reader creates real hash tables for `#s(hash-table ...)'
        // literals at READ time; emaxx's reader leaves a marker form, so
        // quoted data materializes it here (in place, like a constant).
        let value = crate::lisp::primitives::materialize_read_hash_table_literals(self, &value)?;
        crate::lisp::primitives::materialize_read_char_table_literals(self, &value)
    }

    pub(super) fn sf_if(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let Some(test_form) = items.get(1) else {
            return Ok(Value::Nil);
        };
        // The tail-alias machinery guards a self-mutating test form
        // ((setcdr X ...) where X aliases the `if' form's own tail);
        // an allocation-free pre-scan skips it for every ordinary `if'.
        let mut scan_budget = 512u32;
        let tail_aliases = if crate::lisp::eval::form_mentions_setcdr(test_form, &mut scan_budget) {
            setcdr_tail_aliases(self, test_form, &Value::list(items[1..].to_vec()), env)
        } else {
            Vec::new()
        };
        let saved_aliases = snapshot_tail_alias_values(self, &tail_aliases, env);
        let cond_result = self.eval(test_form, env);
        let tail_became_improper =
            !tail_aliases.is_empty() && tail_aliases_became_improper(self, &tail_aliases, env);
        restore_tail_alias_values(self, &saved_aliases, env);
        let cond = cond_result?;
        if tail_became_improper {
            return Err(LispError::Void("if".into()));
        }
        if cond.is_truthy() {
            items
                .get(2)
                .map_or(Ok(Value::Nil), |then_form| self.eval(then_form, env))
        } else {
            // else branches
            self.sf_progn(items.get(3..).unwrap_or(&[]), env)
        }
    }

    pub(super) fn sf_when(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_truthy() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_if_let_star(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "if-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        self.eval_if_let_star_bindings(bindings, &items[2], items.get(3..).unwrap_or(&[]), env)
    }

    /// Execute the shared semantics of the native `if-let*', `if-let', and
    /// `when-let' fallbacks without constructing a new macro form and
    /// sending it back through `eval'.  The latter made a fresh cons-cell
    /// callsite on every loop iteration, defeating the macro-expansion cache
    /// whenever GNU's preloaded `if-let*' macro was present.
    fn eval_if_let_star_bindings(
        &mut self,
        bindings: Vec<Value>,
        then_form: &Value,
        else_forms: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let frame_index = env.len();
        Self::push_marked_frame(env, Vec::new());
        let mut special_restores = Vec::new();
        let mut all_non_nil = true;

        let result = (|| -> Result<Value, LispError> {
            for binding in bindings {
                let (binding_name, value) = match binding {
                    Value::Symbol(name) => {
                        let value = if all_non_nil {
                            self.lookup(&name, env)?
                        } else {
                            Value::Nil
                        };
                        (Some(name), value)
                    }
                    Value::Cons(_, _) => {
                        let parts = binding.to_vec()?;
                        match parts.as_slice() {
                            [expr] => {
                                let value = if all_non_nil {
                                    self.eval(expr, env)?
                                } else {
                                    Value::Nil
                                };
                                (None, value)
                            }
                            [Value::Symbol(name), expr] => {
                                let value = if all_non_nil {
                                    self.eval(expr, env)?
                                } else {
                                    Value::Nil
                                };
                                let binding_name = (name != "_").then(|| name.clone());
                                (binding_name, value)
                            }
                            _ => {
                                return Err(LispError::Signal("Invalid if-let* binding".into()));
                            }
                        }
                    }
                    _ => {
                        return Err(LispError::Signal("Invalid if-let* binding".into()));
                    }
                };

                if let Some(name) = binding_name {
                    if self.binding_is_dynamic(&name, env) {
                        special_restores.push(self.bind_special_variable(
                            &name,
                            value.clone(),
                            env,
                        )?);
                    } else if let Some(frame) = env.get_mut(frame_index) {
                        frame.push((name, Self::stored_value(value.clone())));
                    }
                }
                all_non_nil &= value.is_truthy();
            }

            if all_non_nil {
                self.eval(then_form, env)
            } else {
                self.sf_progn(else_forms, env)
            }
        })();

        env.truncate(frame_index);
        let mut restore_error = None;
        for restore in special_restores.into_iter().rev() {
            if let Err(error) = self.restore_special_binding(restore, env)
                && restore_error.is_none()
            {
                restore_error = Some(error);
            }
        }
        match (result, restore_error) {
            (Err(error), _) => Err(error),
            (Ok(_), Some(error)) => Err(error),
            (Ok(value), None) => Ok(value),
        }
    }

    pub(super) fn sf_if_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "if-let".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = normalize_if_let_spec(&items[1])?;
        self.eval_if_let_star_bindings(bindings, &items[2], items.get(3..).unwrap_or(&[]), env)
    }

    pub(super) fn sf_and_let_star(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "and-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        Self::push_marked_frame(env, Vec::new());
        let mut last_value = Value::T;
        for binding in bindings {
            let value = match binding {
                Value::Symbol(name) => self.lookup(&name, env)?,
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    match parts.as_slice() {
                        [expr] => self.eval(expr, env)?,
                        [Value::Symbol(name), expr] => {
                            let value = self.eval(expr, env)?;
                            if name != "_"
                                && let Some(frame) = env.last_mut()
                            {
                                frame.push((name.clone(), Self::stored_value(value.clone())));
                            }
                            value
                        }
                        _ => {
                            env.pop();
                            return Err(LispError::Signal("Invalid and-let* binding".into()));
                        }
                    }
                }
                _ => {
                    env.pop();
                    return Err(LispError::Signal("Invalid and-let* binding".into()));
                }
            };

            if !value.is_truthy() {
                env.pop();
                return Ok(Value::Nil);
            }
            last_value = value;
        }

        let result = if items.len() > 2 {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(last_value)
        };
        env.pop();
        result
    }

    pub(super) fn sf_when_let(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "when-let".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = normalize_if_let_spec(&items[1])?;
        let body = forms_to_progn(items.get(2..).unwrap_or(&[]));
        self.eval_if_let_star_bindings(bindings, &body, &[], env)
    }

    pub(super) fn sf_when_let_star(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "when-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        let body = forms_to_progn(items.get(2..).unwrap_or(&[]));
        self.eval_if_let_star_bindings(bindings, &body, &[], env)
    }

    pub(super) fn sf_unless(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_nil() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_bound_and_true_p(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() != 2 {
            return Err(LispError::WrongNumberOfArgs(
                "bound-and-true-p".into(),
                items.len().saturating_sub(1),
            ));
        }
        let symbol = quoted_symbol_name(&items[1])
            .or_else(|| items[1].as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::TypeError("symbol".into(), items[1].type_name()))?;
        Ok(self
            .lookup_var(&symbol, env)
            .filter(|value| value.is_truthy())
            .unwrap_or(Value::Nil))
    }

    pub(super) fn sf_cond(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for (clause_index, clause) in items[1..].iter().enumerate() {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let tail_aliases = setcdr_tail_aliases(
                self,
                &clause_items[0],
                &Value::list(items[clause_index + 1..].to_vec()),
                env,
            );
            let saved_aliases = snapshot_tail_alias_values(self, &tail_aliases, env);
            let test_result = self.eval(&clause_items[0], env);
            let tail_became_improper = tail_aliases_became_improper(self, &tail_aliases, env);
            restore_tail_alias_values(self, &saved_aliases, env);
            let test = test_result?;
            if tail_became_improper {
                return Err(LispError::Void("cond".into()));
            }
            if test.is_truthy() {
                if clause_items.len() == 1 {
                    return Ok(test);
                }
                return self.sf_progn(&clause_items[1..], env);
            }
        }
        Ok(Value::Nil)
    }

    /// GNU pcase.el owns the pcase family once it is loadable: its
    /// expansions carry the memq/member optimizations and branch pruning
    /// pcase-tests macroexpands for.  The native special forms stay as the
    /// no-file fallback (unit tests run without a GNU load-path).
    pub(crate) fn ensure_gnu_pcase_loaded(&mut self) {
        if self.gnu_pcase_load_attempted {
            return;
        }
        self.gnu_pcase_load_attempted = true;
        if self.has_macro_binding("pcase") {
            return;
        }
        let Some(path) = self.resolve_load_target("pcase") else {
            return;
        };
        if crate::lisp::load_file_strict(self, &path).is_err() {
            return;
        }
        // GNU cl-macs.el integrates with pcase once loaded: the cl-type
        // pattern and the cl-typep-aware exclusivity advice (it prunes
        // shadowed quoted branches).  emaxx's cl machinery is native, so
        // cl-macs.el never loads; install the pieces here.  The GNU
        // advice's defstruct-predicate branches are OMITTED: they call
        // cl-struct-sequence-type, whose autoload stub would drag
        // cl-macs.el over the native cl machinery (pruning is an
        // optimization, so omitting them only costs precision).
        const CL_MACS_PCASE_INTEGRATION: &str = r#"
(progn
  (defun cl--pcase-mutually-exclusive-p (orig pred1 pred2)
    "Extra special cases for `cl-typep' predicates."
    (let* ((x1 pred1) (x2 pred2)
           (t1
            (and (eq 'cl-typep (car-safe x1))    (setq x1 (cdr x1))
                 (eq '_ (car-safe x1))           (setq x1 (cdr x1))
                 (null (cdr-safe x1))            (setq x1 (car x1))
                 (eq 'quote (car-safe x1))       (cadr x1)))
           (t2
            (and (eq 'cl-typep (car-safe x2))    (setq x2 (cdr x2))
                 (eq '_ (car-safe x2))           (setq x2 (cdr x2))
                 (null (cdr-safe x2))            (setq x2 (car x2))
                 (eq 'quote (car-safe x2))       (cadr x2))))
      (or
       (and (symbolp t1) (symbolp t2)
            (let ((c1 (cl--find-class t1))
                  (c2 (cl--find-class t2)))
              (and c1 c2
                   (not (or (memq t1 (cl--class-allparents c2))
                            (memq t2 (cl--class-allparents c1)))))))
       (funcall orig pred1 pred2))))
  ;; GNU installs this with advice-add, but advice-add autoloads
  ;; nadvice.el and this blob can run in the middle of nadvice's own
  ;; load (its first pcase form pulls pcase.el in); wrap by plain
  ;; redefinition instead.
  (defalias 'pcase--mutually-exclusive-p--emaxx-orig
    (symbol-function 'pcase--mutually-exclusive-p))
  (defun pcase--mutually-exclusive-p (pred1 pred2)
    (cl--pcase-mutually-exclusive-p
     #'pcase--mutually-exclusive-p--emaxx-orig pred1 pred2))
  (defun cl-struct-sequence-type (struct-type)
    "Return the sequence used to build STRUCT-TYPE.
STRUCT-TYPE is a symbol naming a struct type.  Return values are
either `vector', `list' or nil (and the latter indicates a
`record' struct type."
    (unless (get struct-type 'emaxx-struct-slots)
      (error "%s is not a struct type" struct-type))
    (get struct-type 'emaxx-struct-sequence-type))
  (pcase-defmacro cl-struct (type &rest fields)
    "Pcase patterns that match cl-struct EXPVAL of type TYPE.
Elements of FIELDS can be of the form (NAME PAT) in which case the
contents of field NAME is matched against PAT, or they can be of
the form NAME which is a shorthand for (NAME NAME)."
    (declare (debug (sexp &rest [&or (sexp pcase-PAT) sexp])))
    `(and (pred (cl-typep _ ',type))
          ,@(mapcar
             (lambda (field)
               (let* ((name (if (consp field) (car field) field))
                      (pat (if (consp field) (cadr field) field)))
                 `(app ,(if (eq (cl-struct-sequence-type type) 'list)
                            `(nth ,(cl-struct-slot-offset type name))
                          `(aref _ ,(cl-struct-slot-offset type name)))
                       ,pat)))
             fields)))
  (pcase-defmacro cl-type (type)
    "Pcase pattern that matches objects of TYPE.
TYPE is a type descriptor as accepted by `cl-typep', which see."
    `(pred (cl-typep _ ',type))))
"#;
        if let Ok(forms) = crate::lisp::reader::Reader::new(CL_MACS_PCASE_INTEGRATION).read_all() {
            let mut env = Env::new();
            for form in forms {
                let _ = self.eval(&form, &mut env);
            }
        }
    }

    /// GNU rx.el owns the rx family once loadable: its translator handles
    /// every documented atom (category, intersection, submatch, eval,
    /// regexp, seq nesting, repeat forms, rx-let/rx-define...) the native
    /// sf_rx cannot.  The native forms remain the no-file fallback.
    pub(crate) fn ensure_gnu_rx_loaded(&mut self) {
        if self.gnu_rx_load_attempted {
            return;
        }
        self.gnu_rx_load_attempted = true;
        if self.has_macro_binding("rx") {
            return;
        }
        let Some(path) = self.resolve_load_target("rx") else {
            return;
        };
        let _ = crate::lisp::load_file_strict(self, &path);
    }

    /// Load a Lisp-owned macro advertised by an autoload before falling back
    /// to Emaxx's file-less bootstrap implementation.
    pub(crate) fn ensure_autoloaded_macro_loaded(&mut self, name: &str) {
        if self.has_macro_binding(name) {
            return;
        }
        let Some(binding) = self.logical_function_binding(name, &Env::new()) else {
            return;
        };
        let Some((file, _, _)) = crate::lisp::primitives::autoload_parts(&binding) else {
            return;
        };
        if !crate::lisp::primitives::autoload_is_macro(self, Some(name), &binding) {
            return;
        }
        let _ = self.load_target(&file);
    }

    pub(super) fn sf_pcase(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_pcase_like(items, env, false)
    }

    pub(super) fn sf_pcase_defmacro(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::WrongNumberOfArgs(
                "pcase-defmacro".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let params = self.parse_params(&items[2])?;
        let body_start = if items.len() > 4 {
            if matches!(&items[3], Value::String(_) | Value::StringObject(_)) {
                4
            } else {
                3
            }
        } else {
            3
        };
        let body_start = if body_start < items.len() && is_function_declare_form(&items[body_start])
        {
            body_start + 1
        } else {
            body_start
        };
        let body = items[body_start..].to_vec();
        let expander_name = format!("{name}--pcase-macroexpander");
        let expander = Value::Lambda(params, body.into(), shared_env(env.clone()));
        self.validate_function_binding(&expander_name, &expander)?;
        self.set_function_binding(&expander_name, Some(expander));
        self.put_symbol_property(&name, "pcase-macroexpander", Value::Symbol(expander_name));
        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_pcase_exhaustive(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.sf_pcase_like(items, env, true)
    }

    pub(super) fn sf_pcase_like(
        &mut self,
        items: &[Value],
        env: &mut Env,
        exhaustive: bool,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let value = self.eval(&items[1], env)?;
        for clause in &items[2..] {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let mut bindings = Vec::new();
            if pcase_pattern_bindings(self, env, &clause_items[0], &value, &mut bindings)? {
                Self::push_marked_frame(env, bindings);
                let result = self.sf_progn(&clause_items[1..], env);
                env.pop();
                return result;
            }
        }
        if exhaustive {
            Err(LispError::Signal(
                "pcase-exhaustive: no matching clause".into(),
            ))
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_and(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::T;
        for item in &items[1..] {
            result = self.eval(item, env)?;
            if result.is_nil() {
                return Ok(Value::Nil);
            }
        }
        Ok(result)
    }

    pub(super) fn sf_or(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for item in &items[1..] {
            let val = self.eval(item, env)?;
            if val.is_truthy() {
                return Ok(val);
            }
        }
        Ok(Value::Nil)
    }

    pub(super) fn sf_not(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("not".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        Ok(if val.is_nil() { Value::T } else { Value::Nil })
    }

    pub(super) fn sf_progn(&mut self, body: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        for expr in body {
            result = self.eval(expr, env)?;
        }
        Ok(result)
    }

    pub(super) fn sf_atomic_change_group(
        &mut self,
        body: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer = self.buffer.clone();
        match self.sf_progn(body, env) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.buffer = saved_buffer;
                Err(error)
            }
        }
    }

    pub(super) fn sf_catch(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("catch".into(), 0));
        }
        let tag = self.eval(&items[1], env)?;
        let depth = env.len();
        self.active_catch_tags.push(tag.clone());
        let result = self.sf_progn(&items[2..], env);
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
            .active_catch_tags
            .iter()
            .rev()
            .any(|candidate| crate::lisp::primitives::values_eq_in_env(self, candidate, &tag, env))
        {
            Err(LispError::Throw(tag, value))
        } else {
            self.dispatch_handler_bindings(
                LispError::SignalValue(Value::list([Value::Symbol("no-catch".into()), tag, value])),
                env,
            )
        }
    }

    pub(super) fn sf_cl_return(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() > 2 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-return".into(),
                items.len().saturating_sub(1),
            ));
        }
        let value = if let Some(value) = items.get(1) {
            self.eval(value, env)?
        } else {
            Value::Nil
        };
        Err(LispError::Throw(
            Value::Symbol("--cl-block-nil--".into()),
            value,
        ))
    }

    pub(super) fn sf_cl_return_from(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if !(2..=3).contains(&items.len()) {
            return Err(LispError::WrongNumberOfArgs(
                "cl-return-from".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?;
        let value = if let Some(value) = items.get(2) {
            self.eval(value, env)?
        } else {
            Value::Nil
        };
        Err(LispError::Throw(
            Value::Symbol(format!("--cl-block-{name}--")),
            value,
        ))
    }

    pub(super) fn sf_prog1(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let result = self.eval(&items[1], env)?;
        let tracked_symbol = items[1].as_symbol().ok().map(str::to_string);
        for expr in &items[2..] {
            self.eval(expr, env)?;
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

    pub(super) fn sf_prog2(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "prog2".into(),
                items.len().saturating_sub(1),
            ));
        }
        self.eval(&items[1], env)?;
        let result = self.eval(&items[2], env)?;
        let tracked_symbol = items[2].as_symbol().ok().map(str::to_string);
        for expr in &items[3..] {
            self.eval(expr, env)?;
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
                Value::Symbol(name.to_string()),
            ])));
        }
        Ok(())
    }

    pub(super) fn sf_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let mut frame = Vec::new();
        let mut special_bindings = Vec::new();

        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    Self::check_let_binding_name(name)?;
                    if self.binding_is_dynamic(name, env) {
                        special_bindings.push((name.clone(), Value::Nil));
                    } else {
                        frame.push((name.clone(), Value::Nil));
                    }
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad let binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    Self::check_let_binding_name(&name)?;
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    if self.binding_is_dynamic(&name, env) {
                        special_bindings.push((name, val));
                    } else {
                        frame.push((name, Self::stored_value(val)));
                    }
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }

        let mut restores = Vec::new();
        for (name, value) in special_bindings {
            restores.push(self.bind_special_variable(&name, value, env)?);
        }
        Self::push_marked_frame(env, frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    /// GNU `dlet' expands to a `defvar' per binder followed by `let', so
    /// every binding is dynamic; the binder values still evaluate in the
    /// surrounding lexical scope, in parallel like `let'.
    pub(super) fn sf_dlet(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let mut evaluated = Vec::with_capacity(bindings.len());
        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    Self::check_let_binding_name(name)?;
                    evaluated.push((name.clone(), Value::Nil));
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad dlet binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    Self::check_let_binding_name(&name)?;
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    evaluated.push((name, val));
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }
        let mut restores = Vec::with_capacity(evaluated.len());
        let mut entered: Vec<String> = Vec::with_capacity(evaluated.len());
        for (name, value) in evaluated {
            self.enter_dlet_name(&name);
            entered.push(name.clone());
            match self.bind_special_variable(&name, value, env) {
                Ok(restore) => restores.push(restore),
                Err(error) => {
                    for name in entered.iter().rev() {
                        self.leave_dlet_name(name);
                    }
                    return Err(error);
                }
            }
        }
        let result = self.sf_progn(&items[2..], env);
        let mut restore_error = None;
        for restore in restores.into_iter().rev() {
            if let Err(error) = self.restore_special_binding(restore, env)
                && restore_error.is_none()
            {
                restore_error = Some(error);
            }
        }
        for name in entered.iter().rev() {
            self.leave_dlet_name(name);
        }
        match result {
            Ok(value) => restore_error.map_or(Ok(value), Err),
            Err(error) => Err(error),
        }
    }

    /// let* that always binds lexically, used for generated function-
    /// argument bindings (GNU keeps arguments statically scoped even when
    /// they shadow special variables, bug#47552).
    pub(super) fn sf_letstar_forced_lexical(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let original_depth = env.len();
        let setup = (|| -> Result<(), LispError> {
            for binding in &bindings {
                let (name, value) = match binding {
                    Value::Symbol(name) => {
                        Self::check_let_binding_name(name)?;
                        (name.clone(), Value::Nil)
                    }
                    Value::Cons(_, _) => {
                        let parts = binding.to_vec()?;
                        let Some(name_value) = parts.first() else {
                            return Err(LispError::ReadError("bad let* binding".into()));
                        };
                        let name = name_value.as_symbol()?.to_string();
                        Self::check_let_binding_name(&name)?;
                        let value = if parts.len() > 1 {
                            self.eval(&parts[1], env)?
                        } else {
                            Value::Nil
                        };
                        (name, value)
                    }
                    _ => return Err(wrong_type_argument("listp", binding.clone())),
                };
                Self::push_marked_frame(env, vec![(name, Self::stored_value(value))]);
            }
            Ok(())
        })();
        let result = match setup {
            Ok(()) => self.sf_progn(&items[2..], env),
            Err(error) => Err(error),
        };
        env.truncate(original_depth);
        result
    }

    pub(super) fn sf_letrec(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("letrec".into(), 0));
        }
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let mut names = Vec::with_capacity(bindings.len());
        let mut initializers = Vec::with_capacity(bindings.len());
        let mut frame = Vec::with_capacity(bindings.len());

        for binding in bindings {
            match binding {
                Value::Symbol(name) => {
                    frame.push((name.clone(), Value::Nil));
                    names.push(name);
                    initializers.push(None);
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad letrec binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    frame.push((name.clone(), Value::Nil));
                    names.push(name);
                    initializers.push(parts.get(1).cloned());
                }
                _ => return Err(wrong_type_argument("listp", binding)),
            }
        }

        Self::push_marked_frame(env, frame);
        for (name, initializer) in names.iter().zip(initializers.iter()) {
            let value = if let Some(initializer) = initializer {
                self.eval(initializer, env)?
            } else {
                Value::Nil
            };
            let frame = env.last_mut().expect("letrec frame just pushed");
            if let Some((_, existing)) = frame.iter_mut().rev().find(|(key, _)| key == name) {
                *existing = Self::stored_value(value);
            }
        }
        {
            let frame = env.last().expect("letrec frame just pushed");
            patch_letrec_lambda_captures(frame, &names);
        }
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    pub(super) fn sf_letstar(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let original_depth = env.len();
        let mut restores = Vec::new();
        let setup = (|| -> Result<(), LispError> {
            for binding in &bindings {
                let (name, value) = match binding {
                    Value::Symbol(name) => {
                        Self::check_let_binding_name(name)?;
                        (name.clone(), Value::Nil)
                    }
                    Value::Cons(_, _) => {
                        let parts = binding.to_vec()?;
                        let Some(name_value) = parts.first() else {
                            return Err(LispError::ReadError("bad let* binding".into()));
                        };
                        let name = name_value.as_symbol()?.to_string();
                        Self::check_let_binding_name(&name)?;
                        let value = if parts.len() > 1 {
                            self.eval(&parts[1], env)?
                        } else {
                            Value::Nil
                        };
                        (name, value)
                    }
                    _ => return Err(wrong_type_argument("listp", binding.clone())),
                };
                if self.binding_is_dynamic(&name, env) {
                    restores.push(self.bind_special_variable(&name, value, env)?);
                } else {
                    Self::push_marked_frame(env, vec![(name, Self::stored_value(value))]);
                }
            }
            Ok(())
        })();

        let result = match setup {
            Ok(()) => self.sf_progn(&items[2..], env),
            Err(error) => Err(error),
        };
        env.truncate(original_depth);
        let mut restore_error = None;
        for restore in restores.into_iter().rev() {
            if let Err(error) = self.restore_special_binding(restore, env)
                && restore_error.is_none()
            {
                restore_error = Some(error);
            }
        }
        match result {
            Ok(value) => restore_error.map_or(Ok(value), Err),
            Err(error) => Err(error),
        }
    }

    pub(super) fn sf_cl_progv(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-progv".into(),
                items.len().saturating_sub(1),
            ));
        }
        let symbols = self.eval(&items[1], env)?.to_vec()?;
        let values = self.eval(&items[2], env)?.to_vec()?;
        let mut restores = Vec::new();
        for (index, symbol) in symbols.iter().enumerate() {
            let name = symbol.as_symbol()?;
            let value = values.get(index).cloned().unwrap_or(Value::Nil);
            restores.push(self.bind_special_variable(name, value, env)?);
        }
        let result = self.sf_progn(&items[3..], env);
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    pub(super) fn sf_pcase_let(
        &mut self,
        items: &[Value],
        env: &mut Env,
        sequential: bool,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let bindings = items[1].to_vec()?;
        if sequential {
            Self::push_marked_frame(env, Vec::new());
            for binding in &bindings {
                let parts = binding.to_vec()?;
                if parts.len() < 2 {
                    return Err(LispError::ReadError("bad pcase-let* binding".into()));
                }
                let value = self.eval(&parts[1], env)?;
                let mut frame_bindings = Vec::new();
                if !pcase_pattern_bindings_lenient_list(
                    self,
                    env,
                    &parts[0],
                    &value,
                    &mut frame_bindings,
                )? {
                    env.pop();
                    return Err(LispError::Signal("pcase-let*: no matching clause".into()));
                }
                let frame = env.last_mut().expect("env frame just pushed");
                frame.extend(
                    frame_bindings
                        .into_iter()
                        .map(|(name, value)| (name, Self::stored_value(value))),
                );
            }
            let result = self.sf_progn(&items[2..], env);
            env.pop();
            return result;
        }

        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::ReadError("bad pcase-let binding".into()));
            }
            let value = self.eval(&parts[1], env)?;
            if !pcase_pattern_bindings_lenient_list(self, env, &parts[0], &value, &mut frame)? {
                return Err(LispError::Signal("pcase-let: no matching clause".into()));
            }
        }
        env.push(
            frame
                .into_iter()
                .map(|(name, value)| (name, Self::stored_value(value)))
                .collect(),
        );
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    pub(super) fn sf_let_alist(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let alist = self.eval(&items[1], env)?;
        let mut frame = Vec::new();
        let mut bound = HashSet::new();
        for entry in alist.to_vec().unwrap_or_default() {
            let Some((car, cdr)) = entry.cons_values() else {
                continue;
            };
            let Ok(symbol) = car.as_symbol() else {
                continue;
            };
            // GNU binds each `.key' to (cdr (assq 'key alist)) verbatim;
            // a single-element-list cdr stays a list, and duplicate keys
            // retain the first entry just as `assq' does.
            if !bound.insert(symbol.to_string()) {
                continue;
            }
            frame.push((format!(".{symbol}"), Self::stored_value(cdr)));
        }
        Self::push_marked_frame(env, frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }
}

fn patch_letrec_lambda_captures(frame: &[(String, Value)], names: &[String]) {
    for (_, value) in frame {
        if let Value::Lambda(_, _, closure_env) = value {
            let mut captured_env = closure_env.borrow_mut();
            for captured_frame in captured_env.iter_mut() {
                for name in names {
                    let Some((_, replacement)) = frame.iter().find(|(key, _)| key == name) else {
                        continue;
                    };
                    if let Some((_, captured_value)) =
                        captured_frame.iter_mut().rev().find(|(key, _)| key == name)
                    {
                        *captured_value = replacement.clone();
                    }
                }
            }
        }
    }
}
