use super::*;

impl Interpreter {
    pub(super) fn sf_unwind_protect(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let result = self.eval(&items[1], env);
        // Always run cleanup forms
        for form in &items[2..] {
            let _ = self.eval(form, env);
        }
        result
    }

    pub(super) fn sf_ignore_errors(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let handler_start = self.push_condition_case_handler(vec![Value::Symbol("error".into())]);
        let result = self.sf_progn(&items[1..], env);
        self.pop_handler_bindings(handler_start);
        match result {
            Ok(value) => Ok(value),
            Err(error @ LispError::Throw(_, _)) => Err(error),
            Err(error) => {
                if self.take_condition_case_suspend() {
                    return Err(error);
                }
                Ok(Value::Nil)
            }
        }
    }

    pub(super) fn sf_ignore_error(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "ignore-error".into(),
                items.len().saturating_sub(1),
            ));
        }

        let handler_start = self.push_condition_case_handler(vec![items[1].clone()]);
        let result = self.sf_progn(&items[2..], env);
        self.pop_handler_bindings(handler_start);
        match result {
            Ok(value) => Ok(value),
            Err(error @ LispError::Throw(_, _)) => Err(error),
            Err(error) => {
                if self.take_condition_case_suspend() {
                    return Err(error);
                }
                let condition = error.condition_type();
                let condition_list = self.error_condition_names(&condition);
                if Self::clause_head_matches(&items[1], &condition, &condition_list) {
                    Ok(Value::Nil)
                } else {
                    Err(error)
                }
            }
        }
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
                    if let Some(ref var_name) = var {
                        Self::push_marked_frame(env, vec![(var_name.clone(), val.clone())]);
                    }
                    let result = self.sf_progn(&parts[1..], env);
                    if let (Some(var_name), Ok(value)) = (&var, &result)
                        && let Some(current_value) = current_frame_binding(env, var_name).cloned()
                    {
                        patch_returned_closure_binding(value, var_name, &current_value);
                    }
                    if var.is_some() {
                        env.pop();
                    }
                    return result;
                }
                Ok(val)
            }
            Err(e) => {
                if self.take_condition_case_suspend() {
                    return Err(e);
                }
                // `throw' passes through `condition-case' untouched; only
                // signals are eligible for the handlers.
                if matches!(e, LispError::Throw(_, _)) {
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
                    if let Some(ref var_name) = var {
                        Self::push_marked_frame(
                            env,
                            vec![(var_name.clone(), error_condition_value(&e))],
                        );
                    }
                    let result = self.sf_progn(&parts[1..], env);
                    if let (Some(var_name), Ok(value)) = (&var, &result)
                        && let Some(current_value) = current_frame_binding(env, var_name).cloned()
                    {
                        patch_returned_closure_binding(value, var_name, &current_value);
                    }
                    if var.is_some() {
                        env.pop();
                    }
                    return result;
                }
                Err(e)
            }
        }
    }

    pub(super) fn sf_handler_bind(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "handler-bind".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        let mut active = Vec::new();
        for binding in bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::Signal("handler-bind: invalid binding".into()));
            }
            // CONDITIONS is a condition name or a list of condition names.
            let conditions: Vec<String> = match parts[0].to_vec() {
                Ok(symbols) if !symbols.is_empty() => symbols
                    .iter()
                    .map(|symbol| symbol.as_symbol().map(str::to_string))
                    .collect::<Result<_, _>>()?,
                _ => vec![parts[0].as_symbol()?.to_string()],
            };
            let handler = self.eval(&parts[1], env)?;
            for condition in conditions {
                active.push((condition, handler.clone()));
            }
        }
        let start = self.push_handler_bindings(&active);
        let result = self.sf_progn(&items[2..], env);
        self.pop_handler_bindings(start);
        result
    }

    pub(super) fn sf_with_temp_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id;
        let base_name = " *temp*";
        let temp_name = if self.has_buffer(base_name) {
            let mut n = 2;
            loop {
                let candidate = format!("{}<{}>", base_name, n);
                if !self.has_buffer(&candidate) {
                    break candidate;
                }
                n += 1;
            }
        } else {
            base_name.to_string()
        };
        let (temp_id, _) = self.create_buffer(&temp_name);
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        let result = self.sf_progn(&items[1..], env);
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        result
    }

    pub(super) fn sf_ert_with_test_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-test-buffer".into(),
                items.len().saturating_sub(1),
            ));
        }

        let spec = items[1].to_vec().unwrap_or_default();
        let mut name_form = None;
        let mut selected_form = None;
        let mut index = 0usize;
        while index + 1 < spec.len() {
            let key = spec[index].as_symbol()?;
            match key {
                ":name" => name_form = Some(spec[index + 1].clone()),
                ":selected" => selected_form = Some(spec[index + 1].clone()),
                _ => {}
            }
            index += 2;
        }

        let base_name = if let Some(form) = name_form {
            let value = self.eval(&form, env)?;
            if value.is_nil() {
                None
            } else {
                Some(primitives::string_text(&value)?)
            }
        } else {
            None
        };
        if let Some(form) = selected_form {
            let _ = self.eval(&form, env)?;
        }

        // GNU ert--format-test-buffer-name uses `ert-running-test': the
        // TOP-LEVEL test currently executing.  The native runner records
        // that name; tests run recursively through GNU `ert-run-test'
        // appear in `ert--running-tests' (outermost last).
        let running_test_name = self.current_ert_test_name.clone().or_else(|| {
            self.lookup_var("ert--running-tests", env)
                .and_then(|tests| tests.to_vec().ok())
                .and_then(|tests| tests.last().cloned())
                .and_then(|test| match &test {
                    Value::Record(record_id) => self
                        .find_record(*record_id)
                        .filter(|record| record.type_name == "ert-test")
                        .and_then(|record| record.slots.first().cloned()),
                    _ => None,
                })
                .and_then(|name| name.as_symbol().ok().map(str::to_string))
        });
        let buffer_name = format!(
            "*Test buffer ({}){}*",
            running_test_name.unwrap_or_else(|| "<anonymous test>".into()),
            base_name
                .map(|name| format!(": {name}"))
                .unwrap_or_default()
        );

        let buffer = crate::lisp::primitives::call(
            self,
            "generate-new-buffer",
            &[Value::String(buffer_name)],
            env,
        )?;
        let temp_id = self.resolve_buffer_id(&buffer)?;
        // GNU registers every test buffer in `ert--test-buffers' and only
        // deregisters (and kills) it when the body finishes successfully.
        let test_buffers = self.lookup_var("ert--test-buffers", env);
        if let Some(table) = &test_buffers {
            let _ = crate::lisp::primitives::call(
                self,
                "puthash",
                &[buffer.clone(), Value::T, table.clone()],
                env,
            );
        }
        let saved_buffer_id = self.current_buffer_id();
        self.switch_to_buffer_id(temp_id)?;
        let result = self.sf_progn(&items[2..], env);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.switch_to_buffer_id(saved_buffer_id);
        }
        if result.is_ok() && self.has_buffer_id(temp_id) {
            self.kill_buffer_id(temp_id);
            if let Some(table) = &test_buffers {
                let _ = crate::lisp::primitives::call(
                    self,
                    "remhash",
                    &[buffer.clone(), table.clone()],
                    env,
                );
            }
        }
        result
    }

    pub(super) fn sf_ert_with_temp_directory(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-temp-directory".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        // GNU rejects :text among the leading keywords at expansion time.
        let mut keyword_index = 2usize;
        while let Some(Value::Symbol(keyword)) = items.get(keyword_index) {
            if !keyword.starts_with(':') {
                break;
            }
            if keyword == ":text" {
                return Err(LispError::Signal(
                    "Invalid keyword for directory: :text".into(),
                ));
            }
            keyword_index += 2;
        }
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LispError::Signal(error.to_string()))?
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("emaxx-ert-dir-{}-{}", std::process::id(), stamp));
        fs::create_dir_all(&path).map_err(|error| LispError::Signal(error.to_string()))?;
        env.push(vec![(
            name,
            Value::String(crate::lisp::primitives::file_name_as_directory(
                &path.display().to_string(),
            )),
        )]);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        let _ = fs::remove_dir_all(&path);
        result
    }

    pub(super) fn sf_ert_with_message_capture(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-message-capture".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        // Upstream expands to `(let* ((VAR "")) ...)`, so special variables
        // get a live dynamic binding for the whole body.
        let special_restore = if self.is_special_variable(&name) {
            Some(self.bind_special_variable(&name, Value::String(String::new()), env)?)
        } else {
            Self::push_marked_frame(env, vec![(name.clone(), Value::String(String::new()))]);
            None
        };
        self.message_capture_stack.push(MessageCapture {
            text: String::new(),
            live_var: special_restore.is_some().then(|| name.clone()),
        });
        let mut last = Value::Nil;
        let mut result = Ok(());
        for form in &items[2..] {
            match self.eval(form, env) {
                Ok(value) => {
                    last = value;
                    if special_restore.is_none()
                        && let Some(captured) = self
                            .message_capture_stack
                            .last()
                            .map(|capture| capture.text.clone())
                        && let Some(frame) = env.last_mut()
                        && let Some((_, binding)) = frame.iter_mut().find(|(var, _)| var == &name)
                    {
                        *binding = Value::String(captured);
                    }
                }
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        self.message_capture_stack.pop();
        match special_restore {
            Some(restore) => {
                if let Err(error) = self.restore_special_binding(restore, env)
                    && result.is_ok()
                {
                    result = Err(error);
                }
            }
            None => {
                env.pop();
            }
        }
        result.map(|()| last)
    }

    pub(super) fn sf_with_output_to_string(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id;
        let base_name = " *with-output-to-string*";
        let temp_name = if self.has_buffer(base_name) {
            let mut n = 2;
            loop {
                let candidate = format!("{}<{}>", base_name, n);
                if !self.has_buffer(&candidate) {
                    break candidate;
                }
                n += 1;
            }
        } else {
            base_name.to_string()
        };
        let (temp_id, _) = self.create_buffer(&temp_name);
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        env.push(vec![(
            "standard-output".into(),
            Value::Buffer(temp_id, temp_name.clone()),
        )]);
        let body_result = self.sf_progn(&items[1..], env);
        env.pop();
        let output = Value::String(self.buffer.buffer_string());
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        body_result?;
        Ok(output)
    }

    pub(super) fn sf_with_temp_file(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-temp-file".into(),
                items.len().saturating_sub(1),
            ));
        }
        let file = self.eval(&items[1], env)?;
        let file = primitives::string_text(&file)?;
        let saved_buffer_id = self.current_buffer_id;
        let (temp_id, _) = self.create_buffer(" *temp file*");
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        let body_result = self.sf_progn(&items[2..], env);
        let write_result = if body_result.is_ok() {
            crate::lisp::primitives::call(
                self,
                "write-region",
                &[
                    Value::Nil,
                    Value::Nil,
                    Value::String(file),
                    Value::Nil,
                    Value::Integer(0),
                ],
                env,
            )
            .map(|_| ())
        } else {
            Ok(())
        };
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        let body_value = body_result?;
        write_result?;
        Ok(body_value)
    }

    pub(super) fn sf_ert_with_temp_file(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-temp-file".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let mut index = 2usize;
        let mut prefix = Value::String("emaxx-".into());
        let mut suffix = Value::String(".tmp".into());
        let mut directory = Value::Nil;
        let mut text = Value::Nil;
        let mut buffer_name = None;

        while let Some(Value::Symbol(keyword)) = items.get(index) {
            if !keyword.starts_with(':') {
                break;
            }
            let value_expr = items.get(index + 1).ok_or_else(|| {
                LispError::Signal(format!("ert-with-temp-file missing value for {keyword}"))
            })?;
            match keyword.as_str() {
                ":prefix" => prefix = self.eval(value_expr, env)?,
                ":suffix" => suffix = self.eval(value_expr, env)?,
                ":directory" => directory = self.eval(value_expr, env)?,
                ":text" => text = self.eval(value_expr, env)?,
                ":buffer" => buffer_name = Some(value_expr.as_symbol()?.to_string()),
                ":coding" => {
                    let _ = self.eval(value_expr, env)?;
                }
                _ => {
                    return Err(LispError::Signal(format!(
                        "ert-with-temp-file invalid keyword: {keyword}"
                    )));
                }
            }
            index += 2;
        }

        let path_value = primitives::call(
            self,
            "make-temp-file",
            &[prefix, directory.clone(), suffix, text],
            env,
        )?;
        let path = primitives::string_text(&path_value)?;
        let mut frame = vec![(name, path_value.clone())];
        if let Some(buffer_name) = buffer_name {
            let buffer = primitives::call(
                self,
                "find-file-noselect",
                std::slice::from_ref(&path_value),
                env,
            )?;
            frame.push((buffer_name, Self::stored_value(buffer)));
        }
        Self::push_marked_frame(env, frame);
        let result = self.sf_progn(&items[index..], env);
        env.pop();
        let _ = if directory.is_truthy() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        result
    }

    pub(super) fn sf_with_suppressed_warnings(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.sf_progn(&items[2..], env)
    }

    pub(super) fn sf_with_demoted_errors(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let default_format = Value::String("Error: %S".into());
        let (format_form, body) = if items.len() >= 3 {
            (&items[1], &items[2..])
        } else {
            (&default_format, &items[1..])
        };
        if self
            .lookup_var("debug-on-error", env)
            .is_some_and(|value| value.is_truthy())
        {
            return self.sf_progn(body, env);
        }
        match self.sf_progn(body, env) {
            Ok(value) => Ok(value),
            Err(LispError::Throw(tag, value)) => Err(LispError::Throw(tag, value)),
            Err(error) => {
                let format = if std::ptr::eq(format_form, &default_format) {
                    default_format
                } else {
                    self.eval(format_form, env)?
                };
                let _ = primitives::call(
                    self,
                    "message",
                    &[format, error_condition_value(&error)],
                    env,
                )?;
                Ok(Value::Nil)
            }
        }
    }

    pub(super) fn sf_with_coding_priority(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let codings = self.eval(&items[1], env)?.to_vec()?;
        let saved = self.coding_system_priority_list();
        let requested = codings
            .into_iter()
            .map(|coding| coding.as_symbol().map(|name| name.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        self.set_coding_system_priority(&requested)?;
        let result = self.sf_progn(&items[2..], env);
        let _ = self.set_coding_system_priority(&saved);
        result
    }

    pub(super) fn sf_with_current_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        // The macro expands to (save-current-buffer (set-buffer BUF) ...):
        // the current buffer is saved BEFORE evaluating BUF, so a BUF form
        // that switches buffers as a side effect must not leak.
        let saved_buffer_id = self.current_buffer_id;
        let restore_on_err = |interp: &mut Self, err: LispError| {
            let _ = interp.switch_to_buffer_id(saved_buffer_id);
            Err(err)
        };
        let target = match self.eval(&items[1], env) {
            Ok(value) => value,
            Err(err) => return restore_on_err(self, err),
        };
        let target_id = match self.resolve_buffer_id(&target) {
            Ok(id) => id,
            Err(err) => return restore_on_err(self, err),
        };
        self.switch_to_buffer_id(target_id)?;
        let result = if items.len() == 2 {
            // The macro expands to (save-current-buffer (set-buffer BUF)),
            // so with an empty BODY the value is `set-buffer's: the buffer.
            let buffer_name = self
                .get_buffer_by_id(target_id)
                .map(|buffer| buffer.name.clone())
                .unwrap_or_default();
            Ok(Value::Buffer(target_id, buffer_name))
        } else {
            self.sf_progn(&items[2..], env)
        };
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        result
    }

    pub(super) fn sf_with_current_buffer_window(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 5 {
            return Ok(Value::Nil);
        }
        let saved_buffer_id = self.current_buffer_id;
        let target = self.eval(&items[1], env)?;
        let target_id = match self.resolve_buffer_id(&target) {
            Ok(id) => id,
            Err(_) => {
                let name = crate::lisp::primitives::string_text(&target)?;
                self.find_buffer(&name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| self.create_buffer(&name).0)
            }
        };
        self.switch_to_buffer_id(target_id)?;
        let value = self.sf_progn(&items[4..], env);
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        let value = value?;
        let quit_function = self.eval(&items[3], env)?;
        if quit_function.is_truthy() {
            self.call_function_value(
                quit_function,
                None,
                &[self.selected_window_value(), value],
                env,
            )
        } else {
            Ok(value)
        }
    }

    pub(super) fn sf_with_selected_window(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let window = self.eval(&items[1], env)?;
        let Some(window_id) = crate::lisp::primitives::window_record_id_from_value(self, &window)
        else {
            return Err(LispError::TypeError("window".into(), window.type_name()));
        };
        let target_buffer_id = crate::lisp::primitives::window_buffer_id(self, &window)
            .unwrap_or_else(|| self.current_buffer_id());
        let saved_window_id = self.selected_window_id();
        let saved_buffer_id = self.current_buffer_id();
        let saved_minibuffer_selected = self
            .lookup_var("emaxx-minibuffer-selected-window", env)
            .unwrap_or(Value::Nil);
        let minibuffer_window = self
            .lookup_var("emaxx-minibuffer-window", env)
            .unwrap_or(Value::Nil);
        if minibuffer_window == window {
            self.set_global_binding(
                "emaxx-minibuffer-selected-window",
                Value::Record(saved_window_id),
            );
        }
        self.set_selected_window_id(window_id);
        self.switch_to_buffer_id(target_buffer_id)?;
        let result = self.sf_progn(&items[2..], env);
        self.set_selected_window_id(saved_window_id);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.switch_to_buffer_id(saved_buffer_id);
        }
        self.set_global_binding(
            "emaxx-minibuffer-selected-window",
            saved_minibuffer_selected,
        );
        result
    }

    pub(super) fn sf_with_syntax_table(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let table = self.eval(&items[1], env)?;
        let Value::CharTable(table_id) = table else {
            return Err(LispError::TypeError("char-table".into(), table.type_name()));
        };
        let saved_table_id = self.current_syntax_table_id();
        self.set_current_syntax_table(table_id);
        let result = self.sf_progn(&items[2..], env);
        self.set_current_syntax_table(saved_table_id);
        result
    }

    pub(super) fn sf_add_function(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 || items.len() > 5 {
            return Err(LispError::WrongNumberOfArgs(
                "add-function".into(),
                items.len().saturating_sub(1),
            ));
        }
        let how = self.eval(&items[1], env)?;
        let place = match &items[2] {
            Value::Symbol(symbol) => Value::Symbol(symbol.clone()),
            other => self.eval(other, env)?,
        };
        let function = self.eval(&items[3], env)?;
        let props = if let Some(props) = items.get(4) {
            vec![self.eval(props, env)?]
        } else {
            Vec::new()
        };
        let mut args = Vec::with_capacity(3 + props.len());
        args.push(how);
        args.push(place);
        args.push(function);
        args.extend(props);
        primitives::call(self, "add-function", &args, env)
    }

    pub(super) fn sf_define_advice(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::WrongNumberOfArgs(
                "define-advice".into(),
                items.len().saturating_sub(1),
            ));
        }
        let target = items[1].as_symbol()?.to_string();
        let args = items[2].to_vec()?;
        if args.len() < 2 || args.len() > 4 {
            return Err(LispError::WrongNumberOfArgs(
                "define-advice".into(),
                args.len(),
            ));
        }
        let how = args[0].clone();
        let params = self.parse_params(&args[1])?;
        let advice_name = match args.get(2) {
            Some(Value::String(name)) => Some(name.clone()),
            Some(Value::Symbol(name)) => Some(name.clone()),
            Some(Value::Nil) | None => None,
            Some(other) => {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("wrong-type-argument".into()),
                    Value::Symbol("symbol".into()),
                    other.clone(),
                ])));
            }
        };
        let advice = if let Some(name) = advice_name {
            let function_name = format!("{target}@{name}");
            let lambda = Value::Lambda(params, items[3..].to_vec(), shared_env(env.clone()));
            self.push_function_binding(&function_name, lambda);
            Value::Symbol(function_name)
        } else {
            Value::Lambda(params, items[3..].to_vec(), shared_env(env.clone()))
        };
        primitives::call(
            self,
            "advice-add",
            &[Value::Symbol(target.clone()), how, advice],
            env,
        )?;
        Ok(Value::Symbol(target))
    }

    pub(super) fn sf_with_environment_variables(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-environment-variables".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        let mut updated_environment = primitives::process_environment_entries(
            &self
                .lookup_var("process-environment", env)
                .unwrap_or(Value::Nil),
        )?;
        let mut os_restores: Vec<(String, Option<OsString>, Option<String>)> = Vec::new();
        for binding in bindings {
            let pair = binding.to_vec()?;
            if pair.len() != 2 {
                return Err(LispError::Signal(format!("Invalid VARIABLES: {}", binding)));
            }
            let name = primitives::string_text(&self.eval(&pair[0], env)?)?;
            let value = self.eval(&pair[1], env)?;
            let value = if value.is_nil() {
                None
            } else {
                Some(primitives::string_text(&value)?)
            };
            primitives::setenv_in_environment_entries(
                &mut updated_environment,
                &name,
                value.as_deref(),
                true,
            );
            let previous = std::env::var_os(&name);
            os_restores.push((name, previous, value));
        }
        let restore = self.bind_special_variable(
            "process-environment",
            primitives::process_environment_from_entries(&updated_environment),
            env,
        )?;
        for (name, _, value) in &os_restores {
            unsafe {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
        }
        let result = self.sf_progn(&items[2..], env);
        for (name, previous, _) in os_restores.into_iter().rev() {
            unsafe {
                if let Some(value) = previous {
                    std::env::set_var(&name, value);
                } else {
                    std::env::remove_var(&name);
                }
            }
        }
        let _ = self.restore_special_binding(restore, env);
        result
    }

    pub(super) fn sf_with_restriction(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let start = self.eval(&items[1], env)?.as_integer()? as usize;
        let end = self.eval(&items[2], env)?.as_integer()? as usize;
        let mut body_index = 3;
        let label = if matches!(items.get(3), Some(Value::Symbol(s)) if s == ":label") {
            body_index = 5;
            match items.get(4) {
                Some(Value::Symbol(symbol)) => symbol.clone(),
                Some(Value::Cons(_, _)) => {
                    let quoted = items[4].to_vec()?;
                    quoted
                        .get(1)
                        .ok_or_else(|| LispError::Signal("Invalid with-restriction label".into()))?
                        .as_symbol()?
                        .to_string()
                }
                _ => "default".into(),
            }
        } else {
            "default".into()
        };
        let saved = (self.buffer.point_min(), self.buffer.point_max());
        // The outer bounds must survive edits inside the body the way GNU's
        // marker-based ZV does: if the buffer was wide, stay wide on exit;
        // otherwise track the old bounds with markers so insertions inside
        // the inner restriction don't push text out of the outer one.
        let was_wide = saved.0 == 1 && saved.1 == self.buffer.size_total() + 1;
        let saved_buffer_id = self.current_buffer_id();
        let markers = if was_wide {
            None
        } else {
            let beg_marker = self.make_marker();
            let end_marker = self.make_marker();
            let (Value::Marker(beg_id), Value::Marker(end_id)) = (beg_marker, end_marker) else {
                unreachable!("make_marker returns a marker");
            };
            let _ = self.set_marker(beg_id, Some(saved.0), Some(saved_buffer_id));
            let _ = self.set_marker(end_id, Some(saved.1), Some(saved_buffer_id));
            self.set_marker_insertion_type(end_id, true);
            Some((beg_id, end_id))
        };
        let current = self
            .effective_labeled_restriction(self.current_buffer_id(), None)
            .unwrap_or(saved);
        let effective = (start.max(current.0), end.min(current.1));
        self.labeled_restrictions
            .push((self.current_buffer_id(), label, effective.0, effective.1));
        self.buffer.narrow_to_region(effective.0, effective.1);
        let result = self.sf_progn(&items[body_index..], env);
        self.labeled_restrictions.pop();
        match markers {
            None => self.buffer.widen(),
            Some((beg_id, end_id)) => {
                let restore_begv = self.marker_position(beg_id).unwrap_or(saved.0);
                let restore_zv = self.marker_position(end_id).unwrap_or(saved.1);
                self.buffer.restore_restriction(restore_begv, restore_zv);
            }
        }
        result
    }

    pub(super) fn sf_without_restriction(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let mut body_index = 1;
        let label = if matches!(items.get(1), Some(Value::Symbol(s)) if s == ":label") {
            body_index = 3;
            match items.get(2) {
                Some(Value::Symbol(symbol)) => symbol.clone(),
                Some(Value::Cons(_, _)) => {
                    let quoted = items[2].to_vec()?;
                    quoted
                        .get(1)
                        .ok_or_else(|| {
                            LispError::Signal("Invalid without-restriction label".into())
                        })?
                        .as_symbol()?
                        .to_string()
                }
                _ => "default".into(),
            }
        } else {
            "default".into()
        };
        let saved = (self.buffer.point_min(), self.buffer.point_max());
        let pos = self
            .labeled_restrictions
            .iter()
            .rposition(|(buffer_id, active_label, _, _)| {
                *buffer_id == self.current_buffer_id() && *active_label == label
            });
        let removed = pos.map(|index| self.labeled_restrictions.remove(index));
        if let Some((start, end)) =
            self.effective_labeled_restriction(self.current_buffer_id(), None)
        {
            self.buffer.narrow_to_region(start, end);
        } else {
            self.buffer.widen();
        }
        let result = self.sf_progn(&items[body_index..], env);
        if let Some(entry) = removed {
            self.labeled_restrictions.push(entry);
        }
        self.buffer.restore_restriction(saved.0, saved.1);
        result
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
            let _ = self.switch_to_buffer_id(saved_buffer_id);
            let restore_pt = self
                .marker_position(saved_marker_id)
                .unwrap_or(saved_pt)
                .clamp(self.buffer.point_min(), self.buffer.point_max());
            self.buffer.goto_char(restore_pt);
        }
        let _ = self.set_marker(saved_marker_id, None, None);
        result
    }

    pub(super) fn sf_save_match_data(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved = self.last_match_data.clone();
        let saved_buffer_id = self.last_match_data_buffer_id;
        let saved_markers = if let (Some(buffer_id), Some(match_data)) = (saved_buffer_id, &saved) {
            let mut marker_data = Vec::new();
            for entry in match_data {
                if let Some((start, end)) = entry {
                    let Value::Marker(start_marker) = self.make_marker() else {
                        unreachable!("make_marker always returns a marker")
                    };
                    let Value::Marker(end_marker) = self.make_marker() else {
                        unreachable!("make_marker always returns a marker")
                    };
                    self.set_marker(start_marker, Some(*start), Some(buffer_id))?;
                    self.set_marker(end_marker, Some(*end), Some(buffer_id))?;
                    marker_data.push(Some((start_marker, end_marker)));
                } else {
                    marker_data.push(None);
                }
            }
            Some(marker_data)
        } else {
            None
        };
        let result = self.sf_progn(&items[1..], env);
        self.last_match_data = if let Some(marker_data) = saved_markers {
            let mut restored = Vec::new();
            for entry in marker_data {
                restored.push(match entry {
                    Some((start_marker, end_marker)) => {
                        let start = self.marker_position(start_marker);
                        let end = self.marker_position(end_marker);
                        let _ = self.set_marker(start_marker, None, None);
                        let _ = self.set_marker(end_marker, None, None);
                        match (start, end) {
                            (Some(start), Some(end)) => Some((start, end)),
                            _ => None,
                        }
                    }
                    None => None,
                });
            }
            Some(restored)
        } else {
            saved
        };
        self.last_match_data_buffer_id = saved_buffer_id;
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
            let _ = self.switch_to_buffer_id(saved_buffer_id);
        }
        result
    }

    pub(super) fn sf_save_window_excursion(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let snapshot = self.snapshot_window_configuration();
        let result = self.sf_progn(&items[1..], env);
        let _ = self.restore_window_configuration(snapshot);
        result
    }

    pub(super) fn sf_with_silent_modifications(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let was_modified = self.buffer.is_modified();
        let was_autosaved = self.buffer.is_autosaved();
        // The GNU macro also binds inhibit-read-only and
        // inhibit-modification-hooks around BODY (dynamically, so callees
        // observe them).
        let restore_read_only = self.bind_special_variable("inhibit-read-only", Value::T, env)?;
        let restore_hooks =
            self.bind_special_variable("inhibit-modification-hooks", Value::T, env)?;
        let result = self.sf_progn(&items[1..], env);
        self.restore_special_binding(restore_hooks, env)?;
        self.restore_special_binding(restore_read_only, env)?;
        if !was_modified {
            self.buffer.set_unmodified();
        } else if was_autosaved {
            self.buffer.set_modified();
            self.buffer.set_autosaved();
        } else {
            self.buffer.set_modified();
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
                    let _ = self.switch_to_buffer_id(saved_buffer_id);
                }
                let full_end = self.buffer.size_total() + 1;
                self.buffer.restore_restriction(1, full_end);
                if final_buffer_id != saved_buffer_id && self.has_buffer_id(final_buffer_id) {
                    let _ = self.switch_to_buffer_id(final_buffer_id);
                }
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
        self.buffer.push_undo_meta(Value::cons(
            Value::Marker(beg_id),
            Value::Integer(-(saved_begv as i64)),
        ));
        self.buffer.push_undo_meta(Value::cons(
            Value::Marker(end_id),
            Value::Integer(saved_zv as i64),
        ));
        let result = self.sf_progn(&items[1..], env);
        let final_buffer_id = self.current_buffer_id();
        let restore_begv = self.marker_position(beg_id).unwrap_or(saved_begv);
        let restore_zv = self.marker_position(end_id).unwrap_or(saved_zv);
        if self.has_buffer_id(saved_buffer_id) {
            if final_buffer_id != saved_buffer_id {
                let _ = self.switch_to_buffer_id(saved_buffer_id);
            }
            self.buffer.restore_restriction(restore_begv, restore_zv);
            if final_buffer_id != saved_buffer_id && self.has_buffer_id(final_buffer_id) {
                let _ = self.switch_to_buffer_id(final_buffer_id);
            }
        }
        let _ = self.set_marker(beg_id, None, None);
        let _ = self.set_marker(end_id, None, None);
        result
    }

    pub(super) fn sf_combine_change_calls(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let start_undo = self.buffer.undo_len();
        let result = self.sf_progn(&items[3..], env)?;
        let entries = self.buffer.take_undo_entries_since(start_undo);
        if !entries.is_empty() {
            self.buffer
                .push_undo_entry(crate::buffer::UndoEntry::Combined {
                    display: combined_undo_display(&entries),
                    entries,
                });
        }
        Ok(result)
    }

    pub(super) fn sf_cl_assert(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let result = self.eval(&items[1], env)?;
        if result.is_truthy() {
            Ok(result)
        } else {
            Err(LispError::SignalValue(Value::list([
                Value::Symbol("cl-assertion-failed".into()),
                items[1].clone(),
            ])))
        }
    }

    // ── cl-destructuring-bind ──
    // (cl-destructuring-bind (var1 var2 ... &optional opt1 ...) expr body...)
    pub(super) fn sf_cl_destructuring_bind(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-destructuring-bind".into(),
                items.len() - 1,
            ));
        }
        let val = self.eval(&items[2], env)?;
        let mut frame = Vec::new();
        self.bind_cl_pattern(&items[1], val, &mut frame)?;

        Self::push_marked_frame(env, frame);
        let result = self.sf_progn(&items[3..], env);
        env.pop();
        result
    }

    pub(super) fn sf_cl_letf(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-letf".into(),
                items.len() - 1,
            ));
        }
        let bindings = items[1].to_vec()?;
        Self::push_marked_frame(env, Vec::new());
        let mut rebound = Vec::new();
        let mut rebound_places = Vec::new();
        let mut special_restores = Vec::new();
        let setup = (|| -> Result<(), LispError> {
            for binding in &bindings {
                let parts = binding.to_vec()?;
                if parts.len() < 2 {
                    continue;
                }
                match &parts[0] {
                    Value::Symbol(name) => {
                        // GNU cl-letf treats a plain symbol place like `let`:
                        // special variables get a dynamic binding.
                        if self.is_special_variable(name) {
                            let value = self.eval(&parts[1], env)?;
                            special_restores.push(self.bind_special_variable(name, value, env)?);
                            continue;
                        }
                        let value = Self::stored_value(self.eval(&parts[1], env)?);
                        let frame = env
                            .last_mut()
                            .expect("cl-letf pushes a temporary binding frame");
                        if let Some((_, existing)) =
                            frame.iter_mut().rev().find(|(bound, _)| bound == name)
                        {
                            *existing = value;
                        } else {
                            frame.push((name.clone(), value));
                        }
                    }
                    Value::Cons(_, _) => {
                        let place = parts[0].to_vec()?;
                        if matches!(
                            place.first(),
                            Some(Value::Symbol(name)) if name == "symbol-function"
                        ) {
                            let Some(target) = place.get(1) else {
                                return Err(LispError::Signal("Unsupported cl-letf place".into()));
                            };
                            let function_name = function_name_from_binding_form(target)?;
                            let value = self.eval(&parts[1], env)?;
                            self.push_function_binding(&function_name, value);
                            rebound.push(function_name);
                        } else {
                            let place = self.resolve_setf_place(&parts[0], env)?;
                            let current =
                                self.eval_resolved_setf_place_current_value(&place, env)?;
                            let value = self.eval(&parts[1], env)?;
                            self.set_resolved_setf_place_value(&place, value, env)?;
                            rebound_places.push((place, current));
                        }
                    }
                    _ => return Err(LispError::Signal("Unsupported cl-letf place".into())),
                }
            }
            Ok(())
        })();
        let result = match setup {
            Ok(()) => self.sf_progn(&items[2..], env),
            Err(error) => Err(error),
        };
        env.pop();
        let mut restore_error = None;
        for restore in special_restores.into_iter().rev() {
            if let Err(error) = self.restore_special_binding(restore, env)
                && restore_error.is_none()
            {
                restore_error = Some(error);
            }
        }
        for (place, value) in rebound_places.into_iter().rev() {
            if let Err(error) = self.set_setf_place_value(&place, value, env)
                && restore_error.is_none()
            {
                restore_error = Some(error);
            }
        }
        for name in rebound.into_iter().rev() {
            self.pop_function_binding(&name);
        }
        match result {
            Ok(value) => restore_error.map_or(Ok(value), Err),
            Err(error) => Err(error),
        }
    }

    pub(super) fn sf_aset(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 4 {
            return Err(LispError::WrongNumberOfArgs("aset".into(), items.len() - 1));
        }
        if let Value::Symbol(name) = &items[1] {
            let current = self.lookup(name, env)?;
            let new_value = self.eval(&items[3], env)?;
            let index_value = self.eval(&items[2], env)?;
            self.push_backtrace_frame(
                Value::Symbol("aset".into()),
                vec![current.clone(), index_value.clone(), new_value.clone()],
            );
            let result = if matches!(current, Value::CharTable(_) | Value::Record(_))
                || primitives::record_literal_items(&current).is_some()
                || primitives::is_vector_value(&current)
            {
                primitives::call(
                    self,
                    "aset",
                    &[current, index_value, new_value.clone()],
                    env,
                )
                .map(|_| new_value.clone())
            } else {
                match index_value.as_integer() {
                    Ok(index) => {
                        let index = index as usize;
                        if matches!(current, Value::String(_) | Value::StringObject(_)) {
                            primitives::aset_string_value(&current, index, &new_value).map(
                                |updated| {
                                    self.set_variable(name, updated, env);
                                    new_value.clone()
                                },
                            )
                        } else {
                            match current.to_vec() {
                                Ok(mut entries) => {
                                    let tagged = matches!(
                                        entries.first(),
                                        Some(Value::Symbol(symbol))
                                            if symbol == "vector-literal"
                                    );
                                    let slot = if tagged { index + 1 } else { index };
                                    if slot >= entries.len() {
                                        Err(LispError::Signal("Args out of range".into()))
                                    } else {
                                        entries[slot] = new_value.clone();
                                        self.set_variable(name, Value::list(entries), env);
                                        Ok(new_value.clone())
                                    }
                                }
                                Err(error) => Err(error),
                            }
                        }
                    }
                    Err(error) => Err(error),
                }
            };
            let result = match result {
                Ok(value) => Ok(value),
                Err(error @ LispError::Throw(_, _)) => Err(error),
                Err(error) => self.dispatch_handler_bindings(error, env),
            };
            self.pop_backtrace_frame();
            return result;
        }

        let vector = self.eval(&items[1], env)?;
        let index = self.eval(&items[2], env)?;
        let new_value = self.eval(&items[3], env)?;
        self.push_backtrace_frame(
            Value::Symbol("aset".into()),
            vec![vector.clone(), index.clone(), new_value.clone()],
        );
        let result = match primitives::call(self, "aset", &[vector, index, new_value.clone()], env)
        {
            Ok(_) => Ok(new_value),
            Err(error @ LispError::Throw(_, _)) => Err(error),
            Err(error) => self.dispatch_handler_bindings(error, env),
        };
        self.pop_backtrace_frame();
        result
    }

    // ── cl-flet ──
    // (cl-flet ((name (args) body...) ...) body...)
    pub(super) fn sf_cl_flet(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-flet".into(),
                items.len() - 1,
            ));
        }
        let bindings = items[1].to_vec()?;
        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let fname = parts[0].as_symbol()?.to_string();
            let params_val = parts[1].to_vec()?;
            let mut params = Vec::new();
            for p in &params_val {
                params.push(p.as_symbol()?.to_string());
            }
            let body: Vec<Value> = parts[2..].to_vec();
            let lambda = Value::Lambda(params, body, shared_env(env.clone()));
            frame.push((fname, lambda));
        }
        frame.push((
            crate::lisp::eval::bindings::FUNCTION_FRAME_MARKER.to_string(),
            Value::T,
        ));
        Self::push_marked_frame(env, frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    // ── cl-labels ──
    // (cl-labels ((name (args) body...) ...) body...)
    pub(super) fn sf_cl_labels(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-labels".into(),
                items.len() - 1,
            ));
        }
        let bindings = items[1].to_vec()?;
        let closure_env = shared_env(Vec::new());
        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let fname = parts[0].as_symbol()?.to_string();
            let params_val = parts[1].to_vec()?;
            let mut params = Vec::new();
            for p in &params_val {
                params.push(p.as_symbol()?.to_string());
            }
            let body: Vec<Value> = parts[2..].to_vec();
            frame.push((fname, Value::Lambda(params, body, closure_env.clone())));
        }

        frame.push((
            crate::lisp::eval::bindings::FUNCTION_FRAME_MARKER.to_string(),
            Value::T,
        ));
        let mut captured = env.clone();
        captured.push(frame.clone());
        *closure_env.borrow_mut() = captured;

        Self::push_marked_frame(env, frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    // ── cl-macrolet ──
    // (cl-macrolet ((name (args) body...) ...) body...)
    pub(super) fn sf_cl_macrolet(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-macrolet".into(),
                items.len() - 1,
            ));
        }
        let local_macros = self.parse_cl_macrolet_bindings(&items[1])?;

        let mut result = Value::Nil;
        for form in &items[2..] {
            let (local_start, local_count) = self.push_local_macros(&local_macros);

            let expanded_form = self.cl_macrolet_form_with_expanded_function_body(form, env);
            let eval_result = match expanded_form {
                Ok(form) => self.eval(&form, env),
                Err(error) => Err(error),
            };
            self.drain_local_macros(local_start, local_count);
            result = eval_result?;
        }

        Ok(result)
    }

    pub(super) fn parse_cl_macrolet_bindings(
        &mut self,
        bindings_value: &Value,
    ) -> Result<Vec<MacroBinding>, LispError> {
        let bindings = bindings_value.to_vec()?;
        let mut parsed = Vec::with_capacity(bindings.len());
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let mname = parts[0].as_symbol()?.to_string();
            let params_val = parts[1].to_vec()?;
            let mut params = Vec::new();
            for p in &params_val {
                params.push(p.as_symbol()?.to_string());
            }
            let body: Vec<Value> = parts[2..].to_vec();
            parsed.push((mname, params, body));
        }
        Ok(parsed)
    }

    fn cl_macrolet_form_with_expanded_function_body(
        &mut self,
        form: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        if !matches!(
            items.first(),
            Some(Value::Symbol(name)) if name == "defun" || name == "defsubst"
        ) {
            return Ok(form.clone());
        }

        let body_start = if items.len() > 3 && matches!(items[3], Value::String(_)) {
            4
        } else {
            3
        };
        let mut expanded = Vec::with_capacity(items.len());
        expanded.extend(items[..body_start].iter().cloned());
        for body in &items[body_start..] {
            expanded.push(self.macroexpand_all_form_with_environment(body, None, env)?);
        }
        Ok(Value::list(expanded))
    }

    pub(super) fn sf_cl_symbol_macrolet(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-symbol-macrolet".into(),
                items.len().saturating_sub(1),
            ));
        }
        let mut expansions = HashMap::new();
        for binding in items[1].to_vec()? {
            let parts = binding.to_vec()?;
            if parts.len() != 2 {
                return Err(LispError::Signal(
                    "Invalid cl-symbol-macrolet binding".into(),
                ));
            }
            expansions.insert(parts[0].as_symbol()?.to_string(), parts[1].clone());
        }
        let body = items[2..]
            .iter()
            .map(|form| substitute_symbol_macros(form, &expansions))
            .collect::<Result<Vec<_>, _>>()?;
        self.sf_progn(&body, env)
    }

    // ── Backquote ──
}

fn current_frame_binding<'a>(env: &'a Env, name: &str) -> Option<&'a Value> {
    env.last()
        .and_then(|frame| frame.iter().find(|(symbol, _)| symbol == name))
        .map(|(_, value)| value)
}

fn patch_returned_closure_binding(value: &Value, name: &str, current_value: &Value) {
    match value {
        Value::Lambda(_, _, closure_env) => {
            let mut closure_env = closure_env.borrow_mut();
            if closure_env.is_empty() {
                closure_env.push(vec![(name.to_string(), current_value.clone())]);
                return;
            }
            for frame in closure_env.iter_mut() {
                for (symbol, captured) in frame {
                    if symbol == name {
                        *captured = current_value.clone();
                    }
                }
            }
        }
        Value::Cons(car, cdr) => {
            patch_returned_closure_binding(&car.borrow(), name, current_value);
            patch_returned_closure_binding(&cdr.borrow(), name, current_value);
        }
        _ => {}
    }
}
