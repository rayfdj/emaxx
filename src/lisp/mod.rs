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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SourceFileSettings {
    lexical_binding: bool,
    read_symbol_shorthands: Vec<(String, String)>,
}

fn read_source(path: &Path) -> Result<String, types::LispError> {
    std::fs::read_to_string(path)
        .map_err(|e| types::LispError::Signal(format!("Cannot read {}: {}", path.display(), e)))
}

fn source_settings(source: &str) -> Result<SourceFileSettings, types::LispError> {
    let lexical_binding = source
        .lines()
        .take(2)
        .any(|line| line.contains("lexical-binding: t"));
    let read_symbol_shorthands = match extract_file_local_variable(source, "read-symbol-shorthands")
    {
        Some(raw_value) => parse_symbol_shorthands(&raw_value)?,
        None => Vec::new(),
    };
    Ok(SourceFileSettings {
        lexical_binding,
        read_symbol_shorthands,
    })
}

fn extract_file_local_variable(source: &str, variable: &str) -> Option<String> {
    let mut inside_block = false;
    for line in source.lines().rev() {
        let trimmed = line.trim_start();
        let comment_text = trimmed.trim_start_matches(';').trim_start();
        if comment_text == "End:" {
            inside_block = true;
            continue;
        }
        if !inside_block {
            continue;
        }
        if comment_text == "Local Variables:" {
            break;
        }
        let (name, value) = comment_text.split_once(':')?;
        if name.trim() == variable {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn parse_shorthand_string(value: &types::Value) -> Result<String, types::LispError> {
    match value {
        types::Value::String(text) => Ok(text.clone()),
        types::Value::StringObject(state) => Ok(state.borrow().text.clone()),
        other => Err(types::LispError::TypeError(
            "string".into(),
            other.type_name(),
        )),
    }
}

fn parse_symbol_shorthands(raw_value: &str) -> Result<Vec<(String, String)>, types::LispError> {
    let mut reader = reader::Reader::new(raw_value);
    let Some(value) = reader.read()? else {
        return Ok(Vec::new());
    };
    let entries = value.to_vec()?;
    let mut shorthands = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some((from, to)) = entry.cons_values() else {
            return Err(types::LispError::TypeError(
                "cons".into(),
                entry.type_name(),
            ));
        };
        shorthands.push((parse_shorthand_string(&from)?, parse_shorthand_string(&to)?));
    }
    Ok(shorthands)
}

fn read_symbol_shorthands_value(shorthands: &[(String, String)]) -> types::Value {
    types::Value::list(shorthands.iter().map(|(from, to)| {
        types::Value::cons(
            types::Value::String(from.clone()),
            types::Value::String(to.clone()),
        )
    }))
}

pub fn read_forms(path: &Path) -> Result<Vec<types::Value>, types::LispError> {
    let source = read_source(path)?;
    let settings = source_settings(&source)?;
    reader::Reader::with_symbol_shorthands(&source, settings.read_symbol_shorthands).read_all()
}

pub fn load_file_strict(
    interp: &mut eval::Interpreter,
    path: &Path,
) -> Result<(), types::LispError> {
    let source = read_source(path)?;
    let settings = source_settings(&source)?;
    let previous = interp.set_current_load_file(Some(path.display().to_string()));
    let previous_load_list = interp
        .lookup_var("current-load-list", &types::Env::new())
        .unwrap_or(types::Value::Nil);
    let previous_read_symbol_shorthands = interp
        .lookup_var("read-symbol-shorthands", &types::Env::new())
        .unwrap_or(types::Value::Nil);
    interp.set_global_binding(
        "lexical-binding",
        if settings.lexical_binding {
            types::Value::T
        } else {
            types::Value::Nil
        },
    );
    interp.set_global_binding(
        "read-symbol-shorthands",
        read_symbol_shorthands_value(&settings.read_symbol_shorthands),
    );
    interp.set_global_binding(
        "current-load-list",
        types::Value::list([types::Value::String(path.display().to_string())]),
    );
    let forms = match reader::Reader::with_symbol_shorthands(
        &source,
        settings.read_symbol_shorthands.clone(),
    )
    .read_all()
    {
        Ok(forms) => forms,
        Err(error) => {
            interp.set_global_binding("read-symbol-shorthands", previous_read_symbol_shorthands);
            interp.set_global_binding("current-load-list", previous_load_list);
            interp.set_current_load_file(previous);
            return Err(error);
        }
    };
    for form in &forms {
        let mut env = types::Env::new();
        if let Err(error) = interp.eval(form, &mut env) {
            interp.set_global_binding("read-symbol-shorthands", previous_read_symbol_shorthands);
            interp.set_global_binding("current-load-list", previous_load_list);
            interp.set_current_load_file(previous);
            return Err(error);
        }
    }
    interp.set_global_binding("read-symbol-shorthands", previous_read_symbol_shorthands);
    interp.set_global_binding("current-load-list", previous_load_list);
    interp.set_current_load_file(previous);
    Ok(())
}

/// Load and run an ERT test file, returning (passed, failed, total) and
/// detailed results for each test.
pub fn run_ert_file(
    path: &Path,
) -> Result<(usize, usize, usize, Vec<TestResult>), types::LispError> {
    let source = read_source(path)?;
    let settings = source_settings(&source)?;
    let mut interp = eval::Interpreter::new();
    let previous = interp.set_current_load_file(Some(path.display().to_string()));
    let previous_load_list = interp
        .lookup_var("current-load-list", &types::Env::new())
        .unwrap_or(types::Value::Nil);
    let previous_read_symbol_shorthands = interp
        .lookup_var("read-symbol-shorthands", &types::Env::new())
        .unwrap_or(types::Value::Nil);
    interp.set_global_binding(
        "lexical-binding",
        if settings.lexical_binding {
            types::Value::T
        } else {
            types::Value::Nil
        },
    );
    interp.set_global_binding(
        "read-symbol-shorthands",
        read_symbol_shorthands_value(&settings.read_symbol_shorthands),
    );
    interp.set_global_binding(
        "current-load-list",
        types::Value::list([types::Value::String(path.display().to_string())]),
    );
    let forms = match reader::Reader::with_symbol_shorthands(
        &source,
        settings.read_symbol_shorthands.clone(),
    )
    .read_all()
    {
        Ok(forms) => forms,
        Err(error) => {
            interp.set_global_binding("read-symbol-shorthands", previous_read_symbol_shorthands);
            interp.set_global_binding("current-load-list", previous_load_list);
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
    interp.set_global_binding("read-symbol-shorthands", previous_read_symbol_shorthands);
    interp.set_global_binding("current-load-list", previous_load_list);
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

#[cfg(test)]
mod tests {
    use super::{extract_file_local_variable, parse_symbol_shorthands, source_settings};

    #[test]
    fn parses_read_symbol_shorthands_from_local_variables_block() {
        let source = r#"
(ert-deftest ft-sample ())

;; Local Variables:
;; read-symbol-shorthands: (("ft-" . "fns-tests-"))
;; End:
"#;

        assert_eq!(
            extract_file_local_variable(source, "read-symbol-shorthands"),
            Some(r#"(("ft-" . "fns-tests-"))"#.into())
        );
        assert_eq!(
            parse_symbol_shorthands(r#"(("ft-" . "fns-tests-"))"#).unwrap(),
            vec![("ft-".into(), "fns-tests-".into())]
        );
        assert_eq!(
            source_settings(source).unwrap().read_symbol_shorthands,
            vec![("ft-".into(), "fns-tests-".into())]
        );
    }
}
