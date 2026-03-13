pub mod eval;
pub mod primitives;
pub mod reader;
pub mod types;

use std::path::Path;

use crate::compat::TestStatus;

/// One test's outcome: name, passed, optional error message.
pub type TestResult = (String, bool, Option<String>);

pub fn read_forms(path: &Path) -> Result<Vec<types::Value>, types::LispError> {
    let source = std::fs::read_to_string(path)
        .map_err(|e| types::LispError::Signal(format!("Cannot read {}: {}", path.display(), e)))?;
    reader::Reader::new(&source).read_all()
}

pub fn load_file_strict(
    interp: &mut eval::Interpreter,
    path: &Path,
) -> Result<(), types::LispError> {
    let forms = read_forms(path)?;
    let mut env = types::Env::new();
    for form in &forms {
        interp.eval(form, &mut env)?;
    }
    Ok(())
}

/// Load and run an ERT test file, returning (passed, failed, total) and
/// detailed results for each test.
pub fn run_ert_file(
    path: &Path,
) -> Result<(usize, usize, usize, Vec<TestResult>), types::LispError> {
    let mut interp = eval::Interpreter::new();
    let forms = read_forms(path)?;

    // Evaluate all top-level forms (this collects ert-deftest definitions)
    let mut env = types::Env::new();
    for form in &forms {
        // Ignore errors in top-level forms (e.g. require of missing features)
        let _ = interp.eval(form, &mut env);
    }

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
