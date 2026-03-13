use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::compat::{self, BatchReport, FileStatus, TestOutcome, TestStatus};
use crate::lisp;
use crate::lisp::eval::Interpreter;
use crate::lisp::reader::Reader;
use crate::lisp::types::{Env, Value};

#[derive(Clone, Debug, Default)]
pub struct BatchRunOptions {
    pub load_path: Vec<PathBuf>,
    pub load: Vec<String>,
    pub eval: Vec<String>,
}

pub fn run_batch(options: BatchRunOptions) -> Result<i32, String> {
    let mut interpreter = Interpreter::new();
    let mut loaded_test_file: Option<PathBuf> = None;
    let (selector, saw_ert_runner) = parse_selector_requests(&options.eval)?;
    let selector_string = selector.to_string();

    for target in &options.load {
        if target == "ert" {
            continue;
        }
        let resolved = resolve_load_target(target, &options.load_path)?;
        if loaded_test_file.is_none() {
            loaded_test_file = Some(resolved.clone());
        }
        if let Err(error) = lisp::load_file_strict(&mut interpreter, &resolved) {
            let report = BatchReport {
                runner: "emaxx".into(),
                file: report_file_name(&resolved),
                selector: selector_string.clone(),
                file_status: FileStatus::LoadError,
                file_error: Some(error.to_string()),
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

    let Some(test_file) = loaded_test_file else {
        return Err("batch mode needs at least one `-l <test file>` target".into());
    };

    let mut eval_env: Env = Vec::new();
    for expression in &options.eval {
        let forms = Reader::new(expression)
            .read_all()
            .map_err(|error| format!("parse --eval expression `{expression}`: {error}"))?;
        for form in forms {
            if extract_ert_batch_selector(&form).is_none() {
                interpreter
                    .eval(&form, &mut eval_env)
                    .map_err(|error| format!("evaluate --eval expression `{expression}`: {error}"))?;
            }
        }
    }

    let report = if saw_ert_runner {
        let summary = interpreter.run_ert_tests_with_selector(Some(&selector));
        BatchReport {
            runner: "emaxx".into(),
            file: report_file_name(&test_file),
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
            file: report_file_name(&test_file),
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

fn resolve_load_target(target: &str, load_path: &[PathBuf]) -> Result<PathBuf, String> {
    let direct = PathBuf::from(target);
    if direct.exists() {
        return compat::canonicalize_path(&direct);
    }

    let with_el = if target.ends_with(".el") {
        None
    } else {
        Some(format!("{target}.el"))
    };
    for root in load_path {
        let candidate = root.join(target);
        if candidate.exists() {
            return compat::canonicalize_path(&candidate);
        }
        if let Some(with_el) = &with_el {
            let candidate = root.join(with_el);
            if candidate.exists() {
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

fn report_file_name(path: &Path) -> String {
    match env::var("EMACS_TEST_DIRECTORY") {
        Ok(test_directory) => {
            let root = PathBuf::from(test_directory);
            let repo_root = root.parent().unwrap_or(&root);
            compat::relative_test_path(repo_root, path).unwrap_or_else(|_| path.display().to_string())
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

    #[test]
    fn extracts_selector_from_ert_batch_eval() {
        let form = Reader::new("(ert-run-tests-batch-and-exit (quote (not (tag :unstable))))")
            .read_all()
            .expect("read eval")
            .remove(0);
        let selector = extract_ert_batch_selector(&form).expect("selector");
        assert_eq!(selector.to_string(), "(quote (not (tag :unstable)))");
    }
}
