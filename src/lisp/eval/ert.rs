use super::*;

impl Interpreter {
    // ── ERT support ──

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
            .filter(|record| record.has_symbol_type("ert-test"))
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

    fn set_ert_test_most_recent_result(&mut self, test: &Value, result: Value) {
        let Value::Record(record_id) = test else {
            return;
        };
        let Some(record) = self.find_record_mut(*record_id) else {
            return;
        };
        debug_assert!(record.has_symbol_type("ert-test"));
        debug_assert!(record.slots.len() > 3);
        if record.has_symbol_type("ert-test") && record.slots.len() > 3 {
            record.slots[3] = result;
        }
    }

    fn make_ert_test_result(
        &mut self,
        type_name: &str,
        error: Option<&LispError>,
        duration: std::time::Duration,
    ) -> Value {
        // These are the inherited ert-test-result slots: messages,
        // should-forms, and duration.  Condition-bearing subclasses append
        // condition, backtrace, and infos in that order.
        let mut slots = vec![Value::Nil, Value::Nil, Value::float(duration.as_secs_f64())];
        if let Some(error) = error {
            slots.extend([error_condition_value(error), Value::Nil, Value::Nil]);
        }
        self.create_record(type_name, slots)
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

        // GNU keeps the selector and per-run state dynamically visible while
        // each test runs.  Some upstream suites use that public ERT context to
        // decide whether optional expensive subcases were selected.
        let mut stats_env = Vec::new();
        let stats_tests = Value::list(
            tests
                .iter()
                .map(|test| {
                    self.get_symbol_property(&test.name, "ert--test")
                        .unwrap_or(Value::Nil)
                })
                .collect::<Vec<_>>(),
        );
        let stats_selector = selector.map(unquote).unwrap_or(Value::T);
        let run_stats = self
            .lookup_function("ert--make-stats", &stats_env)
            .and_then(|function| {
                self.call_function_value(
                    function,
                    Some("ert--make-stats"),
                    &[stats_tests, stats_selector],
                    &mut stats_env,
                )
            })
            .unwrap_or(Value::Nil);

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
            let stats_restore =
                self.bind_special_variable("ert--current-run-stats", run_stats.clone(), &mut env);
            // GNU pushes the executing test onto `ert--running-tests';
            // helpers like `ert-running-test' read it.
            let test_struct = self
                .get_symbol_property(&test.name, "ert--test")
                .unwrap_or(Value::Nil);
            self.set_ert_test_most_recent_result(&test_struct, Value::Nil);
            let previous_running = self
                .lookup_var("ert--running-tests", &env)
                .unwrap_or(Value::Nil);
            self.set_variable(
                "ert--running-tests",
                Value::list([test_struct.clone()]),
                &mut env,
            );
            // GNU's ert--run-test-internal gives each test its own temp
            // buffer ("For now, each test gets its own temp buffer ...
            // just to be safe"); erc's helpers rely on starting in one.
            let saved_buffer_id = self.current_buffer_id();
            let temp_buffer_result = self
                .call_function_value(
                    Value::Symbol("generate-new-buffer".into()),
                    Some("generate-new-buffer"),
                    &[Value::String(" *temp*".into())],
                    &mut env,
                )
                .and_then(|buffer| self.resolve_buffer_id(&buffer));
            let temp_buffer_id = temp_buffer_result.as_ref().ok().copied();
            if let Some(id) = temp_buffer_id {
                let _ = self.set_current_buffer_id(id);
            }
            // GNU runs every test inside `save-window-excursion', nested in
            // the per-test `with-temp-buffer'.  Keep window creation,
            // selection, and display-buffer side effects test-local too.
            let window_configuration = self.snapshot_window_configuration();
            // GNU `ert--run-test-internal' explicitly binds
            // `lexical-binding' to t inside the per-test temp buffer before
            // invoking the test body.  This is an ERT runner contract, not a
            // property to impose on every lexical closure invocation.
            let lexical_restore = self.bind_special_variable("lexical-binding", Value::T, &mut env);
            // GNU's `ert--run-test-internal' evaluates the body inside
            // `(catch 'ert--pass ...)'.  Register the catch while the body is
            // running so Lisp `ert-pass' reaches this runner boundary instead
            // of being rejected early as an unhandled `no-catch'.
            self.active_catch_tags
                .push(Value::Symbol("ert--pass".into()));
            let test_started = std::time::Instant::now();
            let mut result = match (
                stats_restore.as_ref(),
                lexical_restore.as_ref(),
                temp_buffer_result.as_ref(),
            ) {
                (Ok(_), Ok(_), Ok(_)) => {
                    self.call_function_value(test.body.clone(), None, &[], &mut env)
                }
                (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => Err(error.clone()),
            };
            self.active_catch_tags.pop();
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
            if let Err(error) = self.restore_window_configuration(window_configuration)
                && result.is_ok()
            {
                result = Err(error);
            }
            if self.has_buffer_id(saved_buffer_id) {
                let _ = self.set_current_buffer_id(saved_buffer_id);
            }
            if let Some(id) = temp_buffer_id
                && self.has_buffer_id(id)
            {
                let _ = primitives::call(
                    self,
                    "kill-buffer",
                    &[Value::buffer(id, String::new())],
                    &mut env,
                );
            }
            if let Ok(restore) = stats_restore
                && let Err(error) = self.restore_special_binding(restore, &mut env)
                && result.is_ok()
            {
                result = Err(error);
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
                    let result =
                        self.make_ert_test_result("ert-test-passed", None, test_started.elapsed());
                    self.set_ert_test_most_recent_result(&test_struct, result);
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
                    let result_type = match &status {
                        TestStatus::Skipped => "ert-test-skipped",
                        TestStatus::Failed => "ert-test-failed",
                        TestStatus::Passed => unreachable!("errors cannot produce passed results"),
                    };
                    self.test_results.push(TestOutcome {
                        name: test.name.clone(),
                        status,
                        condition_type: Some(e.condition_type()),
                        message: Some(e.to_string()),
                    });
                    let result =
                        self.make_ert_test_result(result_type, Some(&e), test_started.elapsed());
                    self.set_ert_test_most_recent_result(&test_struct, result);
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
        Value::Symbol(name) => name.to_string(),
        Value::String(value) => value.to_string(),
        other => other.to_string(),
    }
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
        Value::Cons(_) => {
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
