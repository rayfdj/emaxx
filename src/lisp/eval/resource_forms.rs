use super::*;

impl Interpreter {
    pub(super) fn sf_unwind_protect(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let result = self.eval(&items[1], env);
        if matches!(result, Err(LispError::Terminate(_))) {
            return result;
        }
        // Always run cleanup forms.  If a cleanup itself exits nonlocally,
        // GNU lets that newer exit supersede the protected form's result
        // (including an older error/throw), and does not run later cleanup
        // forms from this unwind-protect.
        for form in &items[2..] {
            self.eval(form, env)?;
        }
        result
    }

    pub(super) fn sf_condition_case(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // (condition-case var bodyform handlers...)
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let var = match &items[1] {
            Value::Symbol(s) => Some(s.clone()),
            Value::Nil => None,
            other => return Err(wrong_type_argument("symbolp", other.clone())),
        };

        // Register the clause heads so signal-time `handler-bind' dispatch
        // can see this frame, like GNU's handlerlist.
        let clause_heads = items[3..]
            .iter()
            .filter_map(|handler| {
                let head = handler.to_vec().ok()?.first().cloned()?;
                if matches!(&head, Value::Symbol(symbol) if symbol == ":success") {
                    None
                } else {
                    Some(head)
                }
            })
            .collect::<Vec<_>>();
        let handler_start = self.push_condition_case_handler(clause_heads);
        let depth = env.len();
        let body_result = self.eval(&items[2], env);
        self.pop_handler_bindings(handler_start);
        // An error unwinds any binding frames the body pushed before
        // signaling, like GNU's unbind_to at the handler point.
        if env.len() > depth {
            env.truncate(depth);
        }
        match body_result {
            Ok(val) => {
                for handler in &items[3..] {
                    let parts = handler.to_vec()?;
                    if !matches!(parts.first(), Some(Value::Symbol(symbol)) if symbol == ":success")
                    {
                        continue;
                    }
                    return self.eval_condition_case_handler(
                        var.as_ref().map(|name| name.as_str()),
                        val.clone(),
                        &parts[1..],
                        env,
                    );
                }
                Ok(val)
            }
            Err(e) => {
                if self.take_condition_case_suspend() {
                    return Err(e);
                }
                // `throw' passes through `condition-case' untouched; only
                // signals are eligible for the handlers.
                if matches!(
                    e,
                    LispError::Throw(_, _) | LispError::VmReturn(_) | LispError::Terminate(_)
                ) {
                    return Err(e);
                }
                let condition = e.condition_type();
                // GNU matches a handler when it is `memq' in the signaled
                // symbol's `error-conditions' (or is `t'); fall back to the
                // legacy condition-or-error rule when no property is defined.
                let condition_list = self.error_condition_names(&condition);
                // Try to find a matching handler
                for handler in &items[3..] {
                    let parts = handler.to_vec()?;
                    if parts.is_empty() {
                        continue;
                    }
                    if !Self::clause_head_matches(&parts[0], &condition, &condition_list) {
                        continue;
                    }
                    self.clear_batch_error_backtrace();
                    return self.eval_condition_case_handler(
                        var.as_ref().map(|name| name.as_str()),
                        error_condition_value(&e),
                        &parts[1..],
                        env,
                    );
                }
                Err(e)
            }
        }
    }

    /// Evaluate a GNU `condition-case' handler with the binding mode selected
    /// by eval.c:internal_lisp_condition_case.  A non-nil interpreter
    /// environment gets a lexical binding; a nil environment uses specbind
    /// and must unwind the value cell before a returned dynamic lambda runs.
    fn eval_condition_case_handler(
        &mut self,
        variable: Option<&str>,
        value: Value,
        body: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(variable) = variable else {
            return self.sf_progn(body, env);
        };
        if self.interpreter_environment_is_lexical(env) {
            Self::push_marked_frame(env, vec![(variable.to_string(), value)]);
            let result = self.sf_progn(body, env);
            env.pop();
            return result;
        }

        let restore = self.bind_special_variable(variable, value, env)?;
        let result = self.sf_progn(body, env);
        let restore_result = self.restore_special_binding(restore, env);
        match result {
            Ok(value) => restore_result.map(|()| value),
            Err(error) => Err(error),
        }
    }

    pub(super) fn sf_save_excursion(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let saved_pt = self.buffer.point();
        let saved_marker = self.make_marker();
        let saved_marker_id = match saved_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        self.set_marker(saved_marker_id, Some(saved_pt), Some(saved_buffer_id))?;
        let result = self.sf_progn(&items[1..], env);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.set_current_buffer_id(saved_buffer_id);
            let restore_pt = self
                .marker_position(saved_marker_id)
                .unwrap_or(saved_pt)
                .clamp(self.buffer.point_min(), self.buffer.point_max());
            self.buffer.goto_char(restore_pt);
        }
        let _ = self.set_marker(saved_marker_id, None, None);
        result
    }

    pub(super) fn sf_save_current_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let result = self.sf_progn(&items[1..], env);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.set_current_buffer_id(saved_buffer_id);
        }
        result
    }

    pub(super) fn sf_save_restriction(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let saved_begv = self.buffer.point_min();
        let saved_zv = self.buffer.point_max();
        let saved_labeled = self.labeled_restrictions_snapshot(saved_buffer_id);
        // GNU save-restriction-save: a wide buffer is saved as "no
        // restriction" and simply re-widened on exit; tracking the old
        // bounds with markers would spuriously re-narrow after edits
        // (e.g. insert-before-markers at BEGV pushes both markers).
        let was_wide = saved_begv == 1 && saved_zv == self.buffer.size_total() + 1;
        if was_wide {
            let result = self.sf_progn(&items[1..], env);
            let final_buffer_id = self.current_buffer_id();
            if self.has_buffer_id(saved_buffer_id) {
                if final_buffer_id != saved_buffer_id {
                    let _ = self.set_current_buffer_id(saved_buffer_id);
                }
                let full_end = self.buffer.size_total() + 1;
                self.buffer.restore_restriction(1, full_end);
                if final_buffer_id != saved_buffer_id && self.has_buffer_id(final_buffer_id) {
                    let _ = self.set_current_buffer_id(final_buffer_id);
                }
                self.restore_labeled_restrictions(saved_buffer_id, saved_labeled);
            }
            return result;
        }
        let beg_marker = self.make_marker();
        let end_marker = self.make_marker();
        let beg_id = match beg_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        let end_id = match end_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        let _ = self.set_marker(beg_id, Some(saved_begv), Some(saved_buffer_id));
        let _ = self.set_marker(end_id, Some(saved_zv), Some(saved_buffer_id));
        self.set_marker_insertion_type(end_id, true);
        let result = self.sf_progn(&items[1..], env);
        let final_buffer_id = self.current_buffer_id();
        let restore_begv = self.marker_position(beg_id).unwrap_or(saved_begv);
        let restore_zv = self.marker_position(end_id).unwrap_or(saved_zv);
        if self.has_buffer_id(saved_buffer_id) {
            if final_buffer_id != saved_buffer_id {
                let _ = self.set_current_buffer_id(saved_buffer_id);
            }
            self.buffer.restore_restriction(restore_begv, restore_zv);
            self.restore_labeled_restrictions(saved_buffer_id, saved_labeled);
            if final_buffer_id != saved_buffer_id && self.has_buffer_id(final_buffer_id) {
                let _ = self.set_current_buffer_id(final_buffer_id);
            }
        }
        let _ = self.set_marker(beg_id, None, None);
        let _ = self.set_marker(end_id, None, None);
        result
    }

    // ── cl-destructuring-bind ──
    // (cl-destructuring-bind (var1 var2 ... &optional opt1 ...) expr body...)

    // ── cl-flet ──
    // (cl-flet ((name (args) body...) ...) body...)

    // ── cl-labels ──
    // (cl-labels ((name (args) body...) ...) body...)

    // ── cl-macrolet ──
    // (cl-macrolet ((name (args) body...) ...) body...)

    // ── Backquote ──
}
