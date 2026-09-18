use super::core::{list_car, list_cdr, list_cons_count, list_forms, list_next, list_nth};
use super::*;

impl Interpreter {
    pub(crate) fn save_excursion_state(&mut self) -> SavedExcursion {
        let buffer_id = self.current_buffer_id();
        let point = self.buffer.point();
        let marker_id = match self.make_marker() {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        self.set_marker(marker_id, Some(point), Some(buffer_id))
            .expect("a freshly allocated marker accepts a live buffer");
        SavedExcursion {
            buffer_id,
            point,
            marker_id,
        }
    }

    pub(crate) fn restore_excursion_state(&mut self, saved: SavedExcursion) {
        if self.has_buffer_id(saved.buffer_id) {
            let _ = self.set_current_buffer_id(saved.buffer_id);
            let point = self
                .marker_position(saved.marker_id)
                .unwrap_or(saved.point)
                .clamp(self.buffer.point_min(), self.buffer.point_max());
            self.buffer.goto_char(point);
        }
        let _ = self.set_marker(saved.marker_id, None, None);
    }

    pub(crate) fn save_restriction_state(&mut self) -> SavedRestriction {
        let buffer_id = self.current_buffer_id();
        let beginning = self.buffer.point_min();
        let end = self.buffer.point_max();
        let labeled = self.labeled_restrictions_snapshot(buffer_id);
        let bounds = if beginning == 1 && end == self.buffer.size_total() + 1 {
            SavedRestrictionBounds::Wide
        } else {
            let beginning_marker_id = match self.make_marker() {
                Value::Marker(id) => id,
                _ => unreachable!("make_marker returns a marker"),
            };
            let end_marker_id = match self.make_marker() {
                Value::Marker(id) => id,
                _ => unreachable!("make_marker returns a marker"),
            };
            self.set_marker(beginning_marker_id, Some(beginning), Some(buffer_id))
                .expect("a freshly allocated marker accepts a live buffer");
            self.set_marker(end_marker_id, Some(end), Some(buffer_id))
                .expect("a freshly allocated marker accepts a live buffer");
            self.set_marker_insertion_type(end_marker_id, true);
            SavedRestrictionBounds::Narrow {
                beginning,
                end,
                beginning_marker_id,
                end_marker_id,
            }
        };
        SavedRestriction {
            buffer_id,
            bounds,
            labeled,
        }
    }

    pub(crate) fn restore_restriction_state(&mut self, saved: SavedRestriction) {
        let final_buffer_id = self.current_buffer_id();
        if self.has_buffer_id(saved.buffer_id) {
            if final_buffer_id != saved.buffer_id {
                let _ = self.set_current_buffer_id(saved.buffer_id);
            }
            match saved.bounds {
                SavedRestrictionBounds::Wide => {
                    let full_end = self.buffer.size_total() + 1;
                    self.buffer.restore_restriction(1, full_end);
                }
                SavedRestrictionBounds::Narrow {
                    beginning,
                    end,
                    beginning_marker_id,
                    end_marker_id,
                } => {
                    let beginning = self
                        .marker_position(beginning_marker_id)
                        .unwrap_or(beginning);
                    let end = self.marker_position(end_marker_id).unwrap_or(end);
                    self.buffer.restore_restriction(beginning, end);
                    let _ = self.set_marker(beginning_marker_id, None, None);
                    let _ = self.set_marker(end_marker_id, None, None);
                }
            }
            self.restore_labeled_restrictions(saved.buffer_id, saved.labeled);
            if final_buffer_id != saved.buffer_id && self.has_buffer_id(final_buffer_id) {
                let _ = self.set_current_buffer_id(final_buffer_id);
            }
        } else if let SavedRestrictionBounds::Narrow {
            beginning_marker_id,
            end_marker_id,
            ..
        } = saved.bounds
        {
            let _ = self.set_marker(beginning_marker_id, None, None);
            let _ = self.set_marker(end_marker_id, None, None);
        }
    }

    pub(super) fn sf_unwind_protect(
        &mut self,
        args: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some((body_form, cleanup)) = list_next(args) else {
            return Err(LispError::WrongNumberOfArgs("unwind-protect".into(), 0));
        };
        let result = self.eval(&body_form, env);
        if matches!(result, Err(LispError::Terminate(_))) {
            return result;
        }
        // Always run cleanup forms.  If a cleanup itself exits nonlocally,
        // GNU lets that newer exit supersede the protected form's result
        // (including an older error/throw), and does not run later cleanup
        // forms from this unwind-protect.
        for form in list_forms(&cleanup) {
            self.eval(&form, env)?;
        }
        result
    }

    pub(super) fn sf_condition_case(
        &mut self,
        args: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        // (condition-case var bodyform handlers...)
        if list_cons_count(args) < 2 {
            return Ok(Value::Nil);
        }
        let var = match list_car(args) {
            Value::Symbol(s) => Some(s.clone()),
            Value::Nil => None,
            other => return Err(wrong_type_argument("symbolp", other.clone())),
        };
        let bodyform = list_nth(args, 1);
        let handlers = list_cdr(&list_cdr(args));

        // Register the clause heads so signal-time `handler-bind' dispatch
        // can see this frame, like GNU's handlerlist.
        let mut clause_heads = Vec::new();
        for handler in list_forms(&handlers) {
            let Some(head) = handler
                .to_vec()
                .ok()
                .and_then(|parts| parts.first().cloned())
            else {
                continue;
            };
            if !matches!(&head, Value::Symbol(symbol) if symbol == ":success") {
                clause_heads.push(head);
            }
        }
        let handler_start = self.push_condition_case_handler(clause_heads);
        let depth = env.len();
        let body_result = self.eval(&bodyform, env);
        self.pop_handler_bindings(handler_start);
        // An error unwinds any binding frames the body pushed before
        // signaling, like GNU's unbind_to at the handler point.
        if env.len() > depth {
            env.truncate(depth);
        }
        match body_result {
            Ok(val) => {
                for handler in list_forms(&handlers) {
                    let parts = handler.to_vec()?;
                    if !matches!(parts.first(), Some(Value::Symbol(symbol)) if symbol == ":success")
                    {
                        continue;
                    }
                    return self.eval_condition_case_handler(
                        var.as_ref(),
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
                if matches!(e, LispError::Throw(_, _) | LispError::Terminate(_)) {
                    return Err(e);
                }
                let condition = e.condition_type();
                // GNU matches a handler when it is `memq' in the signaled
                // symbol's `error-conditions' (or is `t'); fall back to the
                // legacy condition-or-error rule when no property is defined.
                let condition_list = self.error_condition_names(&condition);
                // Try to find a matching handler
                for handler in list_forms(&handlers) {
                    let parts = handler.to_vec()?;
                    if parts.is_empty() {
                        continue;
                    }
                    if !Self::clause_head_matches(&parts[0], &condition, &condition_list) {
                        continue;
                    }
                    self.clear_batch_error_backtrace();
                    return self.eval_condition_case_handler(
                        var.as_ref(),
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
        variable: Option<&SymbolName>,
        value: Value,
        body: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(variable) = variable else {
            return self.sf_progn(body, env);
        };
        if self.interpreter_environment_is_lexical(env) {
            Self::push_marked_frame(env, vec![(variable.clone(), value)]);
            let result = self.sf_progn(body, env);
            env.pop();
            return result;
        }

        let count = self.specpdl_index();
        self.specbind_symbol(variable, value, env)?;
        let result = self.sf_progn(body, env);
        let unbind = self.unbind_to(count, env);
        match result {
            Ok(value) => unbind.map(|()| value),
            Err(error) => Err(error),
        }
    }

    pub(super) fn sf_save_excursion(
        &mut self,
        args: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved = self.save_excursion_state();
        let result = self.progn_list(args, env);
        self.restore_excursion_state(saved);
        result
    }

    pub(super) fn sf_save_current_buffer(
        &mut self,
        args: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let result = self.progn_list(args, env);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.set_current_buffer_id(saved_buffer_id);
        }
        result
    }

    pub(super) fn sf_save_restriction(
        &mut self,
        args: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved = self.save_restriction_state();
        let result = self.progn_list(args, env);
        self.restore_restriction_state(saved);
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
