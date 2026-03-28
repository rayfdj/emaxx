pub mod eval;
pub mod json;
pub mod primitives;
pub mod reader;
pub mod sqlite;
pub mod types;

use std::path::Path;

use crate::compat::TestStatus;

/// One test's outcome: name, passed, optional error message.
pub type TestResult = (String, bool, Option<String>);

fn read_source(path: &Path) -> Result<String, types::LispError> {
    std::fs::read_to_string(path)
        .map_err(|e| types::LispError::Signal(format!("Cannot read {}: {}", path.display(), e)))
}

fn file_lexical_binding(path: &Path) -> Result<bool, types::LispError> {
    let source = read_source(path)?;
    Ok(source
        .lines()
        .take(2)
        .any(|line| line.contains("lexical-binding: t")))
}

pub fn read_forms(path: &Path) -> Result<Vec<types::Value>, types::LispError> {
    let source = read_source(path)?;
    reader::Reader::new(&source).read_all()
}

pub fn load_file_strict(
    interp: &mut eval::Interpreter,
    path: &Path,
) -> Result<(), types::LispError> {
    let previous = interp.set_current_load_file(Some(path.display().to_string()));
    let lexical_binding = file_lexical_binding(path)?;
    interp.set_global_binding(
        "lexical-binding",
        if lexical_binding {
            types::Value::T
        } else {
            types::Value::Nil
        },
    );
    let forms = match read_forms(path) {
        Ok(forms) => forms,
        Err(error) => {
            interp.set_current_load_file(previous);
            return Err(error);
        }
    };
    for form in &forms {
        let mut env = types::Env::new();
        if let Err(error) = interp.eval(form, &mut env) {
            interp.set_current_load_file(previous);
            return Err(types::LispError::Signal(format!(
                "{error} [while loading form {}]",
                form
            )));
        }
    }
    interp.set_current_load_file(previous);
    Ok(())
}

/// Load and run an ERT test file, returning (passed, failed, total) and
/// detailed results for each test.
pub fn run_ert_file(
    path: &Path,
) -> Result<(usize, usize, usize, Vec<TestResult>), types::LispError> {
    let mut interp = eval::Interpreter::new();
    let previous = interp.set_current_load_file(Some(path.display().to_string()));
    let lexical_binding = file_lexical_binding(path)?;
    interp.set_global_binding(
        "lexical-binding",
        if lexical_binding {
            types::Value::T
        } else {
            types::Value::Nil
        },
    );
    let forms = match read_forms(path) {
        Ok(forms) => forms,
        Err(error) => {
            interp.set_current_load_file(previous);
            return Err(error);
        }
    };

    // Evaluate all top-level forms (this collects ert-deftest definitions)
    for form in &forms {
        let mut env = types::Env::new();
        // Ignore errors in top-level forms (e.g. require of missing features)
        let _ = interp.eval(form, &mut env);
    }
    interp.set_current_load_file(previous);

    // Run the collected tests
    let (passed, failed, total) = interp.run_ert_tests();
    let results = interp
        .test_results
        .iter()
        .map(|result| {
            (
                result.name.clone(),
                result.status == TestStatus::Passed,
                result.message.clone(),
            )
        })
        .collect();

    Ok((passed, failed, total, results))
}
