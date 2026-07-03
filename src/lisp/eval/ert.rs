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
        env: &Env,
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
            .is_some_and(|value| matches!(value, Value::String(_)))
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
                ":tags" => tags = parse_ert_tags(value),
                ":expected-result" => expected_result = selector_atom(value),
                _ => {}
            }
            cursor += 2;
        }

        let body = Value::Lambda(
            Vec::new(),
            items[cursor..].to_vec(),
            shared_env(env.clone()),
        );
        self.ert_tests.push(ErtTestDefinition {
            name,
            body,
            source_file: self.current_load_file.clone(),
            tags,
            expected_result,
        });
        Ok(Value::Nil)
    }

    pub(super) fn sf_should(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Ok(Value::T)
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
        match self.eval(&items[1], env) {
            Err(e) => {
                if let Some(expected_types) = should_error_types(items)
                    && !expected_types
                        .iter()
                        .any(|expected| expected == &e.condition_type())
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
        let tests: Vec<ErtTestDefinition> = self
            .ert_tests
            .iter()
            .filter(|test| selector.is_none_or(|selector| selector_matches(selector, test)))
            .cloned()
            .collect();
        let mut summary = BatchSummary::default();
        self.test_results.clear();
        self.last_selected_tests = tests.iter().map(|test| test.name.clone()).collect();
        summary.total = tests.len();

        for test in &tests {
            let mut env: Env = Vec::new();
            // Timers a test scheduled but never reached firing conditions
            // for must not leak into later tests.
            self.pending_timers.clear();
            let previous =
                std::mem::replace(&mut self.ert_test_source_file, test.source_file.clone());
            let result = self.call_function_value(test.body.clone(), None, &[], &mut env);
            self.ert_test_source_file = previous;
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
                    let status = match e {
                        LispError::TestSkipped(_) => TestStatus::Skipped,
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
