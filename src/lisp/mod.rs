pub mod eval;
pub mod primitives;
pub mod reader;
pub mod types;

use std::path::Path;

/// One test's outcome: name, passed, optional error message.
pub type TestResult = (String, bool, Option<String>);

/// Load and run an ERT test file, returning (passed, failed, total) and
/// detailed results for each test.
pub fn run_ert_file(
    path: &Path,
) -> Result<(usize, usize, usize, Vec<TestResult>), types::LispError> {
    let source = std::fs::read_to_string(path)
        .map_err(|e| types::LispError::Signal(format!("Cannot read {}: {}", path.display(), e)))?;

    let mut interp = eval::Interpreter::new();
    let forms = reader::Reader::new(&source).read_all()?;

    // Evaluate all top-level forms (this collects ert-deftest definitions)
    let mut env = types::Env::new();
    for form in &forms {
        // Ignore errors in top-level forms (e.g. require of missing features)
        let _ = interp.eval(form, &mut env);
    }

    // Run the collected tests
    let (passed, failed, total) = interp.run_ert_tests();
    let results = interp.test_results.clone();

    Ok((passed, failed, total, results))
}
