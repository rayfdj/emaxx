use super::core::{list_car, list_cdr, list_cons_count, list_forms, list_next, list_nth};
use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

impl Interpreter {
    pub(crate) fn save_excursion_state(&mut self) -> SavedExcursion {
        let buffer_id = self.current_buffer_id();
        let point = self.buffer.borrow().point();
        let marker_id = match self.make_marker().kind() {
            Kind::Marker(id) => id,
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
                .clamp(
                    self.buffer.borrow().point_min(),
                    self.buffer.borrow().point_max(),
                );
            self.buffer.borrow_mut().goto_char(point);
        }
        let _ = self.set_marker(saved.marker_id, None, None);
    }

    pub(crate) fn save_restriction_state(&mut self) -> SavedRestriction {
        let buffer_id = self.current_buffer_id();
        let beginning = self.buffer.borrow().point_min();
        let end = self.buffer.borrow().point_max();
        let labeled = self.labeled_restrictions_snapshot(buffer_id);
        let bounds = if beginning == 1 && end == self.buffer.borrow().size_total() + 1 {
            SavedRestrictionBounds::Wide
        } else {
            let beginning_marker_id = match self.make_marker().kind() {
                Kind::Marker(id) => id,
                _ => unreachable!("make_marker returns a marker"),
            };
            let end_marker_id = match self.make_marker().kind() {
                Kind::Marker(id) => id,
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
        // editfns.c:save_restriction_restore_1: changing the bounds
        // invalidates any recorded current column.
        self.invalidate_current_column();
        let final_buffer_id = self.current_buffer_id();
        if self.has_buffer_id(saved.buffer_id) {
            if final_buffer_id != saved.buffer_id {
                let _ = self.set_current_buffer_id(saved.buffer_id);
            }
            match saved.bounds {
                SavedRestrictionBounds::Wide => {
                    let full_end = self.buffer.borrow().size_total() + 1;
                    self.buffer.borrow_mut().restore_restriction(1, full_end);
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
                    self.buffer.borrow_mut().restore_restriction(beginning, end);
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
        if matches!(
            result.as_ref().map_err(LispError::kind),
            Err(LispErrorKind::Terminate(_))
        ) {
            return result;
        }
        // Always run cleanup forms.  If a cleanup itself exits nonlocally,
        // GNU lets that newer exit supersede the protected form's result
        // (including an older error/throw), and does not run later cleanup
        // forms from this unwind-protect.
        // GNU unbind_to preserves the protected value while cleanup runs.
        // Errors keep their Lisp payload inside a Rust box, which the
        // conservative stack scan cannot follow.
        self.with_lisp_stack_roots(&result, |interp| {
            for form in list_forms(&cleanup) {
                interp.eval(&form, env)?;
            }
            Ok::<(), LispError>(())
        })?;
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
        // eval.c:internal_lisp_condition_case.  VAR is a bare symbol once
        // maybe_remove_pos_from_symbol has looked at it; nil binds nothing.
        let var = crate::lisp::primitives::checked_symbol_identity(self, &list_car(args), env)?;
        let var = (var != crate::lisp::types::symbols::builtin_boolean(false)).then_some(var);
        let bodyform = list_nth(args, 1);
        let handlers = list_cdr(&list_cdr(args));
        let success = cached_symbol!(":success").with(|symbol| Value::Symbol(*symbol));

        // Each clause is nil or a list whose car is a symbol or a list of
        // them; the last `:success' clause is the one that runs, and the
        // others' conditions are the heads signal-time dispatch (and
        // `handler-bind') sees on the handler list.
        let mut success_handler = None;
        let mut heads = 0usize;
        let mut first_head = Value::Nil;
        let mut tail = handlers;
        while let Some(cell) = tail.cons_ref() {
            let clause = cell.car.get();
            tail = cell.cdr.get();
            let Some(clause_cell) = clause.cons_ref() else {
                if clause.is_nil() {
                    continue;
                }
                return Err(self.invalid_condition_handler(clause, env));
            };
            let head = clause_cell.car.get();
            if !(head.is_symbol()
                || head.is_cons()
                || (matches!(head.kind(), Kind::SymbolWithPos(_))
                    && crate::lisp::primitives::symbols_with_pos_enabled(self, env)))
            {
                return Err(self.invalid_condition_handler(clause, env));
            }
            if head.word() == success.word() {
                success_handler = Some(clause_cell.cdr.get());
            } else {
                if heads == 0 {
                    first_head = head;
                }
                heads += 1;
            }
        }
        let case_heads = if heads == 1 {
            crate::lisp::eval::CaseHeads::One(first_head)
        } else {
            crate::lisp::eval::CaseHeads::Many(
                list_forms(&handlers)
                    .filter_map(|clause| clause.cons_ref().map(|cell| cell.car.get()))
                    .filter(|head| head.word() != success.word())
                    .collect(),
            )
        };
        let handler_start = self.push_condition_case_handler(case_heads);
        let depth = env.len();
        // A signal no frame boundary settled (a variable's, say) has
        // signal_or_quit's work done while these clauses are active.
        let body_result = self
            .eval(&bodyform, env)
            .map_err(|error| self.signal_or_quit(error, env));
        self.pop_handler_bindings(handler_start);
        // An error unwinds any binding frames the body pushed before
        // signaling, like GNU's unbind_to at the handler point.
        if env.len() > depth {
            env.truncate(depth);
        }
        match body_result {
            Ok(val) => match success_handler {
                Some(body) => self.eval_condition_case_handler(var.as_ref(), val, &body, env),
                None => Ok(val),
            },
            Err(e) => {
                if self.take_condition_case_suspend() {
                    return Err(e);
                }
                // `throw' passes through `condition-case' untouched; only
                // signals are eligible for the handlers.
                if matches!(
                    e.kind(),
                    LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                ) {
                    return Err(e);
                }
                let symbol = e.condition_symbol();
                // find_handler_clause: a clause whose condition is `t' or
                // `memq' in the signaled symbol's `error-conditions', the
                // first clause first.
                let condition_list = self.error_condition_names_of(&symbol);
                let mut tail = handlers;
                while let Some(cell) = tail.cons_ref() {
                    let clause = cell.car.get();
                    tail = cell.cdr.get();
                    let Some(clause_cell) = clause.cons_ref() else {
                        continue;
                    };
                    let head = clause_cell.car.get();
                    if head.word() == success.word()
                        || !Self::clause_head_matches(&head, &condition_list)
                    {
                        continue;
                    }
                    let body = clause_cell.cdr.get();
                    self.clear_batch_error_backtrace();
                    return self.eval_condition_case_handler(
                        var.as_ref(),
                        error_condition_value(&e),
                        &body,
                        env,
                    );
                }
                Err(e)
            }
        }
    }

    /// internal_lisp_condition_case's `error ("Invalid condition handler:
    /// %s", SDATA (Fprin1_to_string (tem, Qt, Qnil)))'.
    #[cold]
    fn invalid_condition_handler(&mut self, clause: Value, env: &mut Env) -> LispError {
        match crate::lisp::primitives::prin1_to_string(self, env, &clause, true, None)
            .and_then(|text| text.as_string())
        {
            Ok(text) => LispError::Signal(format!("Invalid condition handler: {text}")),
            Err(error) => error,
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
        body: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(variable) = variable else {
            return self.progn_list(body, env);
        };
        if self.interpreter_environment_is_lexical(env) {
            Self::push_bindings(env, vec![(*variable, value)]);
            let result = self.progn_list(body, env);
            env.pop();
            return result;
        }

        let count = self.specpdl_index();
        self.specbind_symbol(variable, value, env)?;
        let result = self.progn_list(body, env);
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
