use super::*;

impl Interpreter {
    pub(super) fn sf_with_eval_after_load(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-eval-after-load".into(),
                0,
            ));
        }
        let feature_value = self.eval(&items[1], env)?;
        let feature = match feature_value {
            Value::Symbol(name) => name,
            Value::String(name) => name,
            Value::StringObject(state) => state.borrow().text.clone(),
            other => {
                return Err(LispError::TypeError(
                    "string-or-symbol".into(),
                    other.type_name(),
                ));
            }
        };
        if self.has_feature(&feature) {
            self.sf_progn(&items[2..], env)
        } else {
            self.after_load_forms
                .push((feature, items[2..].to_vec(), env.clone()));
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_with_memoization(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "with-memoization".into(),
                items.len().saturating_sub(1),
            ));
        }
        let place = self.resolve_setf_place(&items[1], env)?;
        let current = self.eval_resolved_setf_place_current_value(&place, env)?;
        if current.is_truthy() {
            return Ok(current);
        }
        let value = self.sf_progn(&items[2..], env)?;
        self.set_resolved_setf_place_value(&place, value.clone(), env)?;
        Ok(value)
    }

    pub(super) fn sf_with_mutex(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-mutex".into(),
                items.len().saturating_sub(1),
            ));
        }
        let mutex_value = self.eval(&items[1], env)?;
        let mutex_id = self.resolve_mutex_id(&mutex_value)?;
        self.lock_mutex_for_current_thread(mutex_id, env)?;
        let result = self.sf_progn(&items[2..], env);
        let _ = self.unlock_mutex_for_current_thread(mutex_id);
        result
    }

    // ── ERT support ──

    pub(super) fn sf_ert_deftest(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let name = match &items[1] {
            Value::Symbol(s) => s.clone(),
            _ => return Ok(Value::Nil),
        };
        if self.ert_tests.iter().any(|test| test.name == name) {
            return Err(LispError::Signal(format!(
                "Test `{name}` redefined (or loaded twice)"
            )));
        }

        // items[2] is the param list (always empty for ert-deftest)
        // items[3..] is docstring, keyword metadata, then body forms.
        let mut cursor = 3;
        if items
            .get(cursor)
            .is_some_and(|value| matches!(value, Value::String(_) | Value::StringObject(_)))
        {
            cursor += 1;
        }

        let mut tags = Vec::new();
        let mut expected_result = ":passed".to_string();
        while cursor + 1 < items.len()
            && items
                .get(cursor)
                .and_then(keyword_symbol_name)
                .is_some_and(|name| name.starts_with(':'))
        {
            let keyword = keyword_symbol_name(&items[cursor]).unwrap_or_default();
            let value = &items[cursor + 1];
            match keyword.as_str() {
                // ert-deftest is a macro in GNU: metadata expressions become
                // arguments to make-ert-test and are evaluated when the test
                // is defined.  This matters for conditional tag forms such
                // as `(and (null (getenv "CI")) '(:unstable))'.
                ":tags" => tags = parse_ert_tags(&self.eval(value, env)?),
                ":expected-result" => expected_result = selector_atom(&self.eval(value, env)?),
                _ => {}
            }
            cursor += 2;
        }

        let closure_env = shared_env(env.clone());
        if self
            .lookup_var("lexical-binding", env)
            .is_some_and(|value| value.is_truthy())
        {
            self.mark_lexical_closure_env(&closure_env);
        }
        let body = Value::Lambda(Vec::new(), items[cursor..].to_vec().into(), closure_env);
        // Mirror `ert-set-test': tests are also reachable through the
        // `ert--test' symbol property as an `ert-test' struct, which
        // `ert-get-test' and the struct accessors read.
        let docstring = items
            .get(3)
            .filter(|value| matches!(value, Value::String(_) | Value::StringObject(_)))
            .cloned()
            .unwrap_or(Value::Nil);
        let record = self.create_record(
            "ert-test",
            vec![
                Value::Symbol(name.clone()),
                docstring,
                body.clone(),
                Value::Nil,
                Value::Symbol(expected_result.clone()),
                Value::list(
                    tags.iter()
                        .map(|tag| Value::Symbol(tag.clone()))
                        .collect::<Vec<_>>(),
                ),
                self.current_load_file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ],
        );
        self.put_symbol_property(&name, "ert--test", record);
        self.ert_tests.push(ErtTestDefinition {
            name,
            body,
            source_file: self.current_load_file.clone(),
            tags,
            expected_result,
        });
        Ok(Value::Nil)
    }

    // GNU `ert-set-test' stores an `ert-test' struct under the symbol's
    // `ert--test' property; ert-font-lock's deftest macros register tests
    // this way (bypassing `ert-deftest'), so mirror the definition into the
    // native test registry too.
    pub(crate) fn ert_set_test(&mut self, name: &str, test: &Value) -> Result<Value, LispError> {
        let Value::Record(record_id) = test else {
            return Err(LispError::TypeError("ert-test".into(), test.type_name()));
        };
        let slots = self
            .find_record(*record_id)
            .filter(|record| record.type_name == "ert-test")
            .map(|record| record.slots.clone())
            .ok_or_else(|| LispError::TypeError("ert-test".into(), test.type_name()))?;
        let body = slots.get(2).cloned().unwrap_or(Value::Nil);
        let expected_result = slots
            .get(4)
            .and_then(|value| value.as_symbol().ok().map(str::to_string))
            .unwrap_or_else(|| ":passed".to_string());
        let tags = slots
            .get(5)
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|tag| tag.as_symbol().ok().map(str::to_string))
            .collect::<Vec<_>>();
        let source_file = slots
            .get(6)
            .and_then(|value| primitives::string_like(value).map(|text| text.text))
            .or_else(|| self.current_load_file.clone());
        self.put_symbol_property(name, "ert--test", test.clone());
        self.ert_tests.retain(|existing| existing.name != name);
        self.ert_tests.push(ErtTestDefinition {
            name: name.to_string(),
            body,
            source_file,
            tags,
            expected_result,
        });
        Ok(test.clone())
    }

    pub(super) fn sf_should(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            // GNU `should' returns the value of FORM.
            Ok(val)
        } else {
            Err(LispError::ErtTestFailed(format!(
                "Test failed: expected truthy value from {}",
                items[1]
            )))
        }
    }

    pub(super) fn sf_should_not(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should-not".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        if val.is_nil() {
            Ok(Value::Nil)
        } else {
            Err(LispError::ErtTestFailed(format!(
                "Test failed: expected nil from {}; got {}",
                items[1], val
            )))
        }
    }

    pub(super) fn sf_should_error(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should-error".into(), 0));
        }
        // GNU `should-error' expands to a `condition-case' with an `error'
        // clause; register it so signal-time `handler-bind' dispatch (e.g.
        // ert's own test-runner handlers) leaves the error to us.
        let handler_start = self.push_condition_case_handler(vec![Value::Symbol("error".into())]);
        let body_result = self.eval(&items[1], env);
        self.pop_handler_bindings(handler_start);
        match body_result {
            Err(error @ LispError::Terminate(_)) => Err(error),
            Err(e) => {
                if self.take_condition_case_suspend() {
                    return Err(e);
                }
                // GNU matches when the expected type is memq in the
                // signaled condition's `error-conditions' (every error
                // derives from `error').
                let condition = e.condition_type();
                let condition_names = self
                    .get_symbol_property(&condition, "error-conditions")
                    .and_then(|value| value.to_vec().ok())
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| item.as_symbol().ok().map(str::to_string))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if let Some(expected_types) = should_error_types(items)
                    && !expected_types.iter().any(|expected| {
                        expected == &condition
                            || condition_names.iter().any(|name| name == expected)
                            || (expected == "error" && condition_names.is_empty())
                    })
                {
                    return Err(LispError::ErtTestFailed(format!(
                        "Test failed: expected error type {} but got {}",
                        expected_types.join(" or "),
                        e.condition_type()
                    )));
                }
                Ok(error_condition_value(&e))
            }
            Ok(val) => Err(LispError::ErtTestFailed(format!(
                "Test failed: expected error but got {}",
                val
            ))),
        }
    }

    pub fn discovered_tests(&self) -> Vec<DiscoveredTest> {
        self.ert_tests
            .iter()
            .map(ErtTestDefinition::discovered)
            .collect()
    }

    /// Run all collected ERT tests. Returns (passed, failed, total).
    pub fn run_ert_tests(&mut self) -> (usize, usize, usize) {
        let summary = self.run_ert_tests_with_selector(None);
        (summary.passed, summary.failed, summary.total)
    }

    pub fn run_ert_tests_with_selector(&mut self, selector: Option<&Value>) -> BatchSummary {
        let mut tests: Vec<ErtTestDefinition> = self
            .ert_tests
            .iter()
            .filter(|test| selector.is_none_or(|selector| selector_matches(selector, test)))
            .cloned()
            .collect();
        // GNU's selector `t' enumerates tests through `apropos-internal',
        // which yields symbols in alphabetical order.
        tests.sort_by(|left, right| left.name.cmp(&right.name));
        let mut summary = BatchSummary::default();
        self.test_results.clear();
        self.last_selected_tests = tests.iter().map(|test| test.name.clone()).collect();
        summary.total = tests.len();

        for test in &tests {
            let mut env: Env = Vec::new();
            // GNU can deliver SIGCHLD-driven process state changes between
            // ERT tests.  Emaxx owns those transitions in its cooperative
            // event pump, so run one nonblocking cycle at the same safe
            // boundary.  Otherwise a completed child left by one test
            // remains spuriously `run' throughout every later test.
            let _ = primitives::pump_external_process_output(self, &mut env);
            if std::env::var("EMAXX_DEBUG_ERT").is_ok() {
                eprintln!("ERT running: {}", test.name);
            }
            // Timers a test scheduled but never reached firing conditions
            // for must not leak into later tests.
            self.pending_timers.clear();
            let previous =
                std::mem::replace(&mut self.ert_test_source_file, test.source_file.clone());
            let previous_name = self.current_ert_test_name.replace(test.name.clone());
            // GNU pushes the executing test onto `ert--running-tests';
            // helpers like `ert-running-test' read it.
            let test_struct = self
                .get_symbol_property(&test.name, "ert--test")
                .unwrap_or(Value::Nil);
            let previous_running = self
                .lookup_var("ert--running-tests", &env)
                .unwrap_or(Value::Nil);
            self.set_variable("ert--running-tests", Value::list([test_struct]), &mut env);
            // GNU's ert--run-test-internal gives each test its own temp
            // buffer ("For now, each test gets its own temp buffer ...
            // just to be safe"); erc's helpers rely on starting in one.
            let saved_buffer_id = self.current_buffer_id();
            let temp_buffer_result = primitives::call(
                self,
                "generate-new-buffer",
                &[Value::String(" *temp*".into())],
                &mut env,
            );
            let temp_buffer_id = temp_buffer_result
                .ok()
                .and_then(|buffer| self.resolve_buffer_id(&buffer).ok());
            if let Some(id) = temp_buffer_id {
                let _ = self.switch_to_buffer_id(id);
            }
            // GNU `ert--run-test-internal' explicitly binds
            // `lexical-binding' to t inside the per-test temp buffer before
            // invoking the test body.  This is an ERT runner contract, not a
            // property to impose on every lexical closure invocation.
            let lexical_restore = self.bind_special_variable("lexical-binding", Value::T, &mut env);
            let mut result = match lexical_restore.as_ref() {
                Ok(_) => self.call_function_value(test.body.clone(), None, &[], &mut env),
                Err(error) => Err(error.clone()),
            };
            // GNU's native `kill-emacs` exits immediately: ERT never turns it
            // into a failed test and never runs its ordinary per-test unwind
            // cleanup.  The process-owning batch boundary consumes the
            // pending request after this runner returns.
            if matches!(result, Err(LispError::Terminate(_))) {
                return summary;
            }
            if let Ok(restore) = lexical_restore
                && let Err(error) = self.restore_special_binding(restore, &mut env)
                && result.is_ok()
            {
                result = Err(error);
            }
            if self.has_buffer_id(saved_buffer_id) {
                let _ = self.switch_to_buffer_id(saved_buffer_id);
            }
            if let Some(id) = temp_buffer_id
                && self.has_buffer_id(id)
            {
                let _ = primitives::call(
                    self,
                    "kill-buffer",
                    &[Value::Buffer(id, String::new())],
                    &mut env,
                );
            }
            self.set_variable("ert--running-tests", previous_running, &mut env);
            self.ert_test_source_file = previous;
            self.current_ert_test_name = previous_name;
            // GNU wraps each test body in (catch 'ert--pass ...); `ert-pass'
            // terminates the test successfully by throwing to that tag.
            if let Err(LispError::Throw(tag, _)) = &result
                && matches!(tag, Value::Symbol(name) if name == "ert--pass")
            {
                result = Ok(Value::Nil);
            }
            match result {
                Ok(_) => {
                    summary.passed += 1;
                    if test.expected_result == ":failed" {
                        summary.unexpected += 1;
                    }
                    self.test_results.push(TestOutcome {
                        name: test.name.clone(),
                        status: TestStatus::Passed,
                        condition_type: None,
                        message: None,
                    });
                }
                Err(e) => {
                    // `ert-skip' signals ert-test-skipped directly.
                    let status = match &e {
                        LispError::TestSkipped(_) => TestStatus::Skipped,
                        LispError::SignalValue(condition)
                            if matches!(condition.car(), Ok(Value::Symbol(kind))
                                if kind == "ert-test-skipped") =>
                        {
                            TestStatus::Skipped
                        }
                        _ => TestStatus::Failed,
                    };
                    let expected_failure = test.expected_result == ":failed";
                    match status {
                        TestStatus::Passed => summary.passed += 1,
                        TestStatus::Failed => {
                            summary.failed += 1;
                            if !expected_failure {
                                summary.unexpected += 1;
                            }
                        }
                        TestStatus::Skipped => summary.skipped += 1,
                    }
                    self.test_results.push(TestOutcome {
                        name: test.name.clone(),
                        status,
                        condition_type: Some(e.condition_type()),
                        message: Some(e.to_string()),
                    });
                }
            }
            // Give children created by this test the same boundary delivery
            // opportunity before the next test begins.  This is deliberately
            // nonblocking: long-running subprocesses remain live.
            let _ = primitives::pump_external_process_output(self, &mut env);
        }
        summary
    }
}

fn selector_atom(value: &Value) -> String {
    match unquote(value) {
        Value::Symbol(name) => name.clone(),
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn parse_ert_tags(value: &Value) -> Vec<String> {
    match unquote(value) {
        Value::Nil => Vec::new(),
        Value::Cons(_, _) => unquote(value)
            .to_vec()
            .map(|values| values.iter().map(selector_atom).collect())
            .unwrap_or_default(),
        other => vec![selector_atom(&other)],
    }
}

fn should_error_types(items: &[Value]) -> Option<Vec<String>> {
    let mut cursor = 2;
    while cursor + 1 < items.len() {
        match keyword_symbol_name(&items[cursor]).as_deref() {
            Some(":type") => {
                let raw = unquote(&items[cursor + 1]);
                if let Ok(values) = raw.to_vec() {
                    let names = values
                        .into_iter()
                        .map(|value| selector_atom(&value))
                        .collect();
                    return Some(names);
                }
                return Some(vec![selector_atom(&raw)]);
            }
            Some(_) => cursor += 2,
            None => break,
        }
    }
    None
}

fn selector_matches(selector: &Value, test: &ErtTestDefinition) -> bool {
    match unquote(selector) {
        Value::Nil => false,
        Value::T => true,
        Value::Symbol(name) if name == "t" => true,
        Value::Symbol(name) if name == "nil" => false,
        Value::Symbol(name) => test.name == name,
        Value::String(pattern) => Regex::new(&pattern)
            .map(|regex| regex.is_match(&test.name))
            .unwrap_or(false),
        Value::Cons(_, _) => {
            let Ok(items) = unquote(selector).to_vec() else {
                return false;
            };
            if items.is_empty() {
                return false;
            }
            match symbol_name(&items[0]).as_deref() {
                Some("tag") => items
                    .get(1)
                    .map(|tag| {
                        test.tags
                            .iter()
                            .any(|candidate| candidate == &selector_atom(tag))
                    })
                    .unwrap_or(false),
                Some("not") => items
                    .get(1)
                    .is_some_and(|inner| !selector_matches(inner, test)),
                Some("or") => items[1..].iter().any(|inner| selector_matches(inner, test)),
                Some("and") => items[1..].iter().all(|inner| selector_matches(inner, test)),
                Some("member") => items[1..]
                    .iter()
                    .any(|item| selector_atom(item) == test.name),
                Some("eql") => items
                    .get(1)
                    .is_some_and(|item| selector_atom(item) == test.name),
                _ => false,
            }
        }
        _ => false,
    }
}
