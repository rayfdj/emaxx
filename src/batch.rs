use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::compat::{self, BatchReport, FileStatus, TestOutcome, TestStatus};
use crate::lisp;
use crate::lisp::eval::Interpreter;
use crate::lisp::reader::Reader;
use crate::lisp::types::{Env, Value};
use crate::perf::{self, PERF_RESULT_FILE_ENV, PerfRunReport};

#[derive(Clone, Debug, Default)]
pub struct BatchRunOptions {
    pub load_path: Vec<PathBuf>,
    pub load: Vec<String>,
    pub eval: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PerfRequest {
    scenario_id: String,
    n: usize,
    warmup: u32,
    samples: u32,
}

pub fn run_batch(options: BatchRunOptions) -> Result<i32, String> {
    let mut interpreter = initialize_batch_interpreter(&options)?;
    let mut loaded_test_file: Option<PathBuf> = None;
    let (selector, saw_ert_runner) = parse_selector_requests(&options.eval)?;
    let perf_request = parse_perf_request(&options.eval)?;
    let selector_string = selector.to_string();

    for target in &options.load {
        let resolved = resolve_load_target(target, &options.load_path)?;
        if target != "ert" && loaded_test_file.is_none() {
            loaded_test_file = Some(resolved.clone());
        }
        if let Err(error) = lisp::load_file_strict(&mut interpreter, &resolved) {
            let mut error_text = error.to_string();
            let backtrace = format_backtrace_summary(&interpreter);
            if !backtrace.is_empty() {
                error_text.push_str(" | backtrace: ");
                error_text.push_str(&backtrace);
            }
            let report = BatchReport {
                runner: "emaxx".into(),
                file: report_file_name(&resolved),
                selector: selector_string.clone(),
                file_status: FileStatus::LoadError,
                file_error: Some(error_text),
                discovered_tests: interpreter.discovered_tests(),
                selected_tests: Vec::new(),
                results: Vec::new(),
                summary: Default::default(),
            };
            emit_artifacts(&report)?;
            emit_human_log(&report);
            write_junit_report_if_requested(&report)?;
            return Ok(2);
        }
    }

    let mut eval_env: Env = Vec::new();
    for expression in &options.eval {
        let forms = Reader::new(expression)
            .read_all()
            .map_err(|error| format!("parse --eval expression `{expression}`: {error}"))?;
        for form in forms {
            if extract_ert_batch_selector(&form).is_none()
                && extract_perf_request_from_form(&form).is_none()
            {
                interpreter.eval(&form, &mut eval_env).map_err(|error| {
                    format!("evaluate --eval expression `{expression}`: {error}")
                })?;
            }
        }
    }

    if let Some(request) = perf_request {
        let report = perf::run_emaxx_batch_scenario(
            &request.scenario_id,
            request.n,
            request.warmup,
            request.samples,
        )?;
        emit_perf_artifacts(&report)?;
        emit_perf_human_log(&report);
        return Ok(match report.status {
            perf::PerfRunStatus::Completed | perf::PerfRunStatus::Unsupported => 0,
            perf::PerfRunStatus::Failed => 1,
        });
    }

    let Some(test_file) = loaded_test_file else {
        return Err("batch mode needs at least one `-l <test file>` target or an `(emaxx-perf-run-batch ...)` request".into());
    };

    let relative_file = report_file_name(&test_file);
    let report = if saw_ert_runner {
        let summary = interpreter.run_ert_tests_with_selector(Some(&selector));
        BatchReport {
            runner: "emaxx".into(),
            file: relative_file.clone(),
            selector: selector_string,
            file_status: FileStatus::Loaded,
            file_error: None,
            discovered_tests: interpreter.discovered_tests(),
            selected_tests: interpreter.last_selected_tests.clone(),
            results: apply_backtrace_limit(interpreter.test_results.clone()),
            summary,
        }
    } else {
        BatchReport {
            runner: "emaxx".into(),
            file: relative_file,
            selector: selector_string,
            file_status: FileStatus::Loaded,
            file_error: None,
            discovered_tests: interpreter.discovered_tests(),
            selected_tests: Vec::new(),
            results: Vec::new(),
            summary: Default::default(),
        }
    };

    emit_artifacts(&report)?;
    emit_human_log(&report);
    write_junit_report_if_requested(&report)?;

    if report.file_status == FileStatus::LoadError {
        Ok(2)
    } else if report.summary.unexpected == 0 {
        Ok(0)
    } else {
        Ok(1)
    }
}

fn initialize_batch_interpreter(options: &BatchRunOptions) -> Result<Interpreter, String> {
    let mut interpreter = Interpreter::new();
    interpreter.set_load_path(options.load_path.clone());
    interpreter.set_variable("noninteractive", Value::T, &mut Vec::new());
    interpreter.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
    preload_batch_compat_libraries(&mut interpreter)?;
    Ok(interpreter)
}

fn preload_batch_compat_libraries(interpreter: &mut Interpreter) -> Result<(), String> {
    for feature in ["button", "backquote", "seq"] {
        if interpreter.has_feature(feature) || interpreter.resolve_load_target(feature).is_none() {
            continue;
        }
        let _ = interpreter.load_target(feature);
    }

    let faces_compat = compat::project_root().join("src/lisp/faces_compat.el");
    lisp::load_file_strict(interpreter, &faces_compat)
        .map_err(|error| format!("load {}: {error}", faces_compat.display()))?;

    Ok(())
}

fn parse_selector_requests(expressions: &[String]) -> Result<(Value, bool), String> {
    let mut selector = Value::T;
    let mut saw_ert_runner = false;
    for expression in expressions {
        let forms = Reader::new(expression)
            .read_all()
            .map_err(|error| format!("parse --eval expression `{expression}`: {error}"))?;
        for form in forms {
            if let Some(found_selector) = extract_ert_batch_selector(&form) {
                selector = found_selector;
                saw_ert_runner = true;
            }
        }
    }
    Ok((selector, saw_ert_runner))
}

fn format_backtrace_summary(interpreter: &Interpreter) -> String {
    interpreter
        .backtrace_frames_snapshot()
        .into_iter()
        .take(8)
        .map(|(function, args, _)| {
            let name = function.unwrap_or_else(|| "<anonymous>".into());
            if args.is_empty() {
                name
            } else {
                let rendered = args
                    .into_iter()
                    .take(2)
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{name}({rendered})")
            }
        })
        .collect::<Vec<_>>()
        .join(" <- ")
}

fn parse_perf_request(expressions: &[String]) -> Result<Option<PerfRequest>, String> {
    let mut request = None;
    for expression in expressions {
        let forms = Reader::new(expression)
            .read_all()
            .map_err(|error| format!("parse --eval expression `{expression}`: {error}"))?;
        for form in forms {
            if let Some(found) = extract_perf_request_from_form(&form) {
                request = Some(found);
            }
        }
    }
    Ok(request)
}

fn resolve_load_target(target: &str, load_path: &[PathBuf]) -> Result<PathBuf, String> {
    let direct = PathBuf::from(target);
    if direct.is_file() {
        return compat::canonicalize_path(&direct);
    }

    let with_el = if target.ends_with(".el") {
        None
    } else {
        Some(format!("{target}.el"))
    };
    for root in load_path {
        let candidate = root.join(target);
        if candidate.is_file() {
            return compat::canonicalize_path(&candidate);
        }
        if let Some(with_el) = &with_el {
            let candidate = root.join(with_el);
            if candidate.is_file() {
                return compat::canonicalize_path(&candidate);
            }
        }
    }

    Err(format!("cannot resolve load target `{target}`"))
}

fn extract_ert_batch_selector(form: &Value) -> Option<Value> {
    let items = form.to_vec().ok()?;
    let head = items.first()?.as_symbol().ok()?;
    if head != "ert-run-tests-batch-and-exit" {
        return None;
    }
    items.get(1).cloned().or(Some(Value::T))
}

fn extract_perf_request_from_form(form: &Value) -> Option<PerfRequest> {
    let items = form.to_vec().ok()?;
    let head = items.first()?.as_symbol().ok()?;
    if head != "emaxx-perf-run-batch" {
        return None;
    }
    let scenario_id = match items.get(1)? {
        Value::String(value) => value.clone(),
        Value::Symbol(value) => value.clone(),
        _ => return None,
    };
    let n = value_to_usize(items.get(2)).unwrap_or(4096);
    let warmup = value_to_u32(items.get(3)).unwrap_or(1);
    let samples = value_to_u32(items.get(4)).unwrap_or(5);
    Some(PerfRequest {
        scenario_id,
        n,
        warmup,
        samples,
    })
}

fn value_to_usize(value: Option<&Value>) -> Option<usize> {
    match value? {
        Value::Integer(number) if *number >= 0 => usize::try_from(*number).ok(),
        _ => None,
    }
}

fn value_to_u32(value: Option<&Value>) -> Option<u32> {
    match value? {
        Value::Integer(number) if *number >= 0 => u32::try_from(*number).ok(),
        _ => None,
    }
}

fn report_file_name(path: &Path) -> String {
    match env::var("EMACS_TEST_DIRECTORY") {
        Ok(test_directory) => {
            let root = PathBuf::from(test_directory);
            let repo_root = root.parent().unwrap_or(&root);
            compat::relative_test_path(repo_root, path)
                .unwrap_or_else(|_| path.display().to_string())
        }
        Err(_) => path.display().to_string(),
    }
}

fn emit_artifacts(report: &BatchReport) -> Result<(), String> {
    if let Ok(result_file) = env::var(compat::BATCH_RESULT_FILE_ENV) {
        report.write_json(Path::new(&result_file))?;
    }
    Ok(())
}

fn emit_perf_artifacts(report: &PerfRunReport) -> Result<(), String> {
    if let Ok(result_file) = env::var(PERF_RESULT_FILE_ENV) {
        report.write_json(Path::new(&result_file))?;
    }
    Ok(())
}

fn emit_human_log(report: &BatchReport) {
    if !verbose_mode() {
        return;
    }
    eprintln!("runner: {}", report.runner);
    eprintln!("file: {}", report.file);
    eprintln!("selector: {}", report.selector);
    eprintln!("file-status: {:?}", report.file_status);
    if let Some(error) = &report.file_error {
        eprintln!("load-error: {error}");
    }
    for result in &report.results {
        eprintln!(
            "{:?}: {}{}",
            result.status,
            result.name,
            result
                .message
                .as_ref()
                .map(|message| format!(" -- {message}"))
                .unwrap_or_default()
        );
    }
    eprintln!(
        "summary: total={} passed={} failed={} skipped={} unexpected={}",
        report.summary.total,
        report.summary.passed,
        report.summary.failed,
        report.summary.skipped,
        report.summary.unexpected
    );
}

fn emit_perf_human_log(report: &PerfRunReport) {
    if !verbose_mode() {
        return;
    }
    eprintln!("runner: {}", report.runner);
    eprintln!("scenario: {}", report.scenario_id);
    eprintln!("status: {:?}", report.status);
    for case in &report.cases {
        eprintln!(
            "{:?}: {}{}",
            case.status,
            case.case_id,
            case.notes
                .as_ref()
                .map(|notes| format!(" -- {notes}"))
                .unwrap_or_default()
        );
    }
}

fn verbose_mode() -> bool {
    matches!(
        env::var("EMACS_TEST_VERBOSE").ok().as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn apply_backtrace_limit(results: Vec<TestOutcome>) -> Vec<TestOutcome> {
    let Some(limit) = env::var("TEST_BACKTRACE_LINE_LENGTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return results;
    };
    results
        .into_iter()
        .map(|mut result| {
            if let Some(message) = result.message.take() {
                let trimmed = message
                    .lines()
                    .map(|line| {
                        let mut chars = line.chars();
                        let collected = chars.by_ref().take(limit).collect::<String>();
                        if chars.next().is_some() {
                            format!("{collected}...")
                        } else {
                            collected
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                result.message = Some(trimmed);
            }
            result
        })
        .collect()
}

fn write_junit_report_if_requested(report: &BatchReport) -> Result<(), String> {
    let Ok(path) = env::var("EMACS_TEST_JUNIT_REPORT") else {
        return Ok(());
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create junit directory {}: {error}", parent.display()))?;
    }

    let tests = report.summary.total;
    let failures = report.summary.failed;
    let skipped = report.summary.skipped;
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
    xml.push_str(&format!(
        "<testsuite name=\"{}\" tests=\"{}\" failures=\"{}\" skipped=\"{}\">\n",
        xml_escape(&report.file),
        tests,
        failures,
        skipped
    ));
    for result in &report.results {
        xml.push_str(&format!(
            "  <testcase name=\"{}\">",
            xml_escape(&result.name)
        ));
        match result.status {
            TestStatus::Passed => {}
            TestStatus::Skipped => {
                xml.push_str(&format!(
                    "<skipped message=\"{}\"/>",
                    xml_escape(result.message.as_deref().unwrap_or("skipped"))
                ));
            }
            TestStatus::Failed => {
                xml.push_str(&format!(
                    "<failure type=\"{}\" message=\"{}\"/>",
                    xml_escape(result.condition_type.as_deref().unwrap_or("error")),
                    xml_escape(result.message.as_deref().unwrap_or("failed"))
                ));
            }
        }
        xml.push_str("</testcase>\n");
    }
    xml.push_str("</testsuite>\n");
    fs::write(&path, xml).map_err(|error| format!("write junit report {}: {error}", path.display()))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn extracts_perf_request_from_eval_form() {
        let forms = Reader::new("(emaxx-perf-run-batch \"noverlay/perf-marker-suite\" 2048 1 5)")
            .read_all()
            .expect("read perf eval");
        let request = extract_perf_request_from_form(&forms[0]).expect("perf request");
        assert_eq!(request.scenario_id, "noverlay/perf-marker-suite");
        assert_eq!(request.n, 2048);
        assert_eq!(request.warmup, 1);
        assert_eq!(request.samples, 5);
    }

    #[test]
    fn extracts_selector_from_ert_batch_eval() {
        let form = Reader::new("(ert-run-tests-batch-and-exit (quote (not (tag :unstable))))")
            .read_all()
            .expect("read eval")
            .remove(0);
        let selector = extract_ert_batch_selector(&form).expect("selector");
        assert_eq!(selector.to_string(), "(quote (not (tag :unstable)))");
    }

    #[test]
    fn batch_runtime_binds_command_line_args_left_to_nil() {
        let options = BatchRunOptions::default();
        let interpreter = initialize_batch_interpreter(&options).expect("init batch interpreter");
        assert_eq!(
            interpreter.lookup_var("command-line-args-left", &Vec::new()),
            Some(Value::Nil)
        );
    }

    #[test]
    fn batch_runtime_preloads_button_when_available_on_load_path() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("emaxx-batch-button-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&root).expect("create temp root");
        let button = root.join("button.el");
        fs::write(
            &button,
            "(defun insert-text-button (&rest _args) 'loaded)\n(provide 'button)\n",
        )
        .expect("write button preload");

        let options = BatchRunOptions {
            load_path: vec![root.clone()],
            ..Default::default()
        };
        let interpreter = initialize_batch_interpreter(&options).expect("init batch interpreter");

        assert!(interpreter.has_feature("button"));
        assert!(
            interpreter
                .lookup_function("insert-text-button", &Vec::new())
                .is_ok()
        );

        fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn batch_runtime_can_load_ert_helpers() {
        let emacs_repo = PathBuf::from("/Users/alpha/CodexProjects/emacs");
        let options = BatchRunOptions {
            load_path: compat::emaxx_upstream_load_path(&emacs_repo).expect("upstream load path"),
            ..Default::default()
        };
        let mut interpreter =
            initialize_batch_interpreter(&options).expect("init batch interpreter");
        let ert = resolve_load_target("ert", &options.load_path).expect("resolve ert");
        lisp::load_file_strict(&mut interpreter, &ert).expect("load ert");

        assert!(
            interpreter
                .lookup_function("ert-test-erts-file", &Vec::new())
                .is_ok()
        );
    }

    #[test]
    fn batch_runtime_can_load_align_stack() {
        let emacs_repo = PathBuf::from("/Users/alpha/CodexProjects/emacs");
        let options = BatchRunOptions {
            load_path: compat::emaxx_upstream_load_path(&emacs_repo).expect("upstream load path"),
            ..Default::default()
        };
        let mut interpreter =
            initialize_batch_interpreter(&options).expect("init batch interpreter");

        for target in ["ert", "ert-x", "align", "test/lisp/align-tests.el"] {
            let resolved = resolve_load_target(target, &options.load_path)
                .unwrap_or_else(|error| panic!("resolve {target}: {error}"));
            lisp::load_file_strict(&mut interpreter, &resolved)
                .unwrap_or_else(|error| panic!("load {target} ({}): {error}", resolved.display()));
        }
    }
}
