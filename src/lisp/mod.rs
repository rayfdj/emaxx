pub mod eval;
pub mod json;
pub mod primitives;
pub mod reader;
pub mod sqlite;
pub mod types;

use std::collections::HashMap;
use std::path::Path;

use crate::compat::TestStatus;

/// One test's outcome: name, passed, optional error message.
pub type TestResult = (String, bool, Option<String>);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SourceFileSettings {
    lexical_binding: bool,
    read_symbol_shorthands: Vec<(String, String)>,
}

fn append_message(interp: &mut eval::Interpreter, text: &str) {
    let buffer_id = interp
        .find_buffer("*Messages*")
        .map(|(id, _)| id)
        .unwrap_or_else(|| interp.create_buffer("*Messages*").0);
    if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let end = buffer.point_max();
        buffer.goto_char(end);
        buffer.insert(&(text.to_string() + "\n"));
    }
}

fn unescaped_char_literal_warning(path: &Path, source: &str) -> Option<String> {
    let pairs = [
        ('"', "`?\"'", "`?\\\"'"),
        ('(', "`?('", "`?\\('"),
        (')', "`?)'", "`?\\)'"),
        (';', "`?;'", "`?\\;'"),
        ('[', "`?['", "`?\\['"),
        (']', "`?]'", "`?\\]'"),
    ];
    let detected = pairs
        .iter()
        .filter(|(ch, _, _)| source.contains(&format!("?{ch}")))
        .collect::<Vec<_>>();
    if detected.is_empty() {
        return None;
    }
    let actual = detected
        .iter()
        .map(|(_, actual, _)| *actual)
        .collect::<Vec<_>>()
        .join(", ");
    let expected = detected
        .iter()
        .map(|(_, _, expected)| *expected)
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "Loading `{}': unescaped character literals {} detected, {} expected!",
        path.display(),
        actual,
        expected
    ))
}

fn lisp_string_literal(text: &str) -> String {
    let mut rendered = String::from("\"");
    for ch in text.chars() {
        match ch {
            '\\' => rendered.push_str("\\\\"),
            '"' => rendered.push_str("\\\""),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            _ => rendered.push(ch),
        }
    }
    rendered.push('"');
    rendered
}

fn rewrite_lazy_doc_refs(
    text: &str,
    path: &Path,
    docs: &HashMap<usize, String>,
    force_load_doc_strings: bool,
) -> String {
    let path_literal = lisp_string_literal(&path.display().to_string());
    let bytes = text.as_bytes();
    let mut out = String::new();
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index..].starts_with(b"(#$ . ") {
            let digits_start = index + 6;
            let mut cursor = digits_start;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor > digits_start && cursor < bytes.len() && bytes[cursor] == b')' {
                let offset = text[digits_start..cursor].parse::<usize>().ok();
                if force_load_doc_strings
                    && let Some(offset) = offset
                    && let Some(doc) = docs.get(&offset)
                {
                    out.push_str(&lisp_string_literal(doc));
                } else {
                    out.push('(');
                    out.push_str(&path_literal);
                    out.push_str(" . ");
                    out.push_str(&text[digits_start..cursor]);
                    out.push(')');
                }
                index = cursor + 1;
                continue;
            }
        }

        if bytes[index..].starts_with(b"#$") {
            out.push_str(&path_literal);
            index += 2;
            continue;
        }

        out.push(bytes[index] as char);
        index += 1;
    }

    out
}

fn preprocess_lazy_doc_source(path: &Path, source: &str, force_load_doc_strings: bool) -> String {
    let bytes = source.as_bytes();
    let mut docs = HashMap::new();
    let mut out = String::new();
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index..].starts_with(b"#@") {
            let digits_start = index + 2;
            let mut cursor = digits_start;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor > digits_start {
                let count = source[digits_start..cursor].parse::<usize>().unwrap_or(0);
                if count == 0 {
                    out.push_str("nil");
                    break;
                }
                if cursor < bytes.len() {
                    cursor += 1;
                }
                let content_start = cursor;
                let content_end = content_start.saturating_add(count).min(bytes.len());
                let mut doc =
                    String::from_utf8_lossy(&bytes[content_start..content_end]).into_owned();
                if doc.ends_with('\n') {
                    doc.pop();
                }
                if doc.ends_with('\u{1f}') {
                    doc.pop();
                }
                docs.insert(content_start, doc);
                index = content_end;
                continue;
            }
        }

        out.push(bytes[index] as char);
        index += 1;
    }

    rewrite_lazy_doc_refs(&out, path, &docs, force_load_doc_strings)
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
    let force_load_doc_strings = interp
        .lookup_var("load-force-doc-strings", &types::Env::new())
        .is_some_and(|value| value.is_truthy());
    let source = if source.starts_with(";ELC") {
        preprocess_lazy_doc_source(path, &source, force_load_doc_strings)
    } else {
        source
    };
    let warning_message = unescaped_char_literal_warning(path, &source);
    let previous = interp.set_current_load_file(Some(path.display().to_string()));
    let previous_load_list = interp
        .lookup_var("current-load-list", &types::Env::new())
        .unwrap_or(types::Value::Nil);
    let previous_load_history = interp
        .lookup_var("load-history", &types::Env::new())
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
    interp.set_global_binding(
        "load-history",
        types::Value::cons(
            types::Value::list([types::Value::String(path.display().to_string())]),
            previous_load_history,
        ),
    );
    if let Some(message) = warning_message {
        append_message(interp, &message);
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
