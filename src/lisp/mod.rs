pub mod bytecode;
pub mod eval;
pub mod json;
pub mod primitives;
pub mod reader;
pub mod sqlite;
pub mod types;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::rc::Rc;

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

fn unescaped_char_literal_warning_parts(source: &str) -> Option<(String, String)> {
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
    Some((actual, expected))
}

fn unescaped_char_literal_warning(path: &Path, source: &str) -> Option<String> {
    let (actual, expected) = unescaped_char_literal_warning_parts(source)?;
    Some(format!(
        "Loading `{}': unescaped character literals {} detected, {} expected!",
        path.display(),
        actual,
        expected
    ))
}

pub(crate) fn byte_compile_unescaped_char_literal_warning(source: &str) -> Option<String> {
    let (actual, expected) = unescaped_char_literal_warning_parts(source)?;
    Some(format!(
        "unescaped character literals {actual} detected, {expected} expected!"
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

pub(crate) fn preprocess_lazy_doc_source(
    path: &Path,
    source: &str,
    force_load_doc_strings: bool,
) -> String {
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

fn contains_gnu_byte_code_literal(value: &types::Value) -> bool {
    fn quoted_byte_code_function(value: &types::Value) -> bool {
        value.to_vec().ok().is_some_and(|items| {
            matches!(
                items.as_slice(),
                [types::Value::Symbol(quote), types::Value::Symbol(kind)]
                    if quote == "quote" && kind == "byte-code-function"
            )
        })
    }

    fn visit(value: &types::Value, seen: &mut HashSet<usize>) -> bool {
        let types::Value::Cons(car, cdr) = value else {
            return false;
        };
        let identity = Rc::as_ptr(car) as usize;
        if !seen.insert(identity) {
            return false;
        }
        let head = car.borrow().clone();
        let tail = cdr.borrow().clone();
        if matches!(&head, types::Value::Symbol(name) if name == reader::RECORD_LITERAL_SYMBOL)
            && let types::Value::Cons(kind, _) = &tail
            && quoted_byte_code_function(&kind.borrow())
        {
            return true;
        }
        visit(&head, seen) || visit(&tail, seen)
    }

    visit(value, &mut HashSet::new())
}

fn headered_elc_is_interpretable_lisp(path: &Path, source: &str) -> bool {
    let source = preprocess_lazy_doc_source(path, source, false);
    reader::Reader::new(&source)
        .read_all()
        .is_ok_and(|forms| !forms.iter().any(contains_gnu_byte_code_literal))
}

fn read_source(path: &Path) -> Result<String, types::LispError> {
    String::from_utf8(read_source_bytes(path)?).map_err(|error| {
        types::LispError::Signal(format!("Cannot read {}: {}", path.display(), error))
    })
}

fn read_source_bytes(path: &Path) -> Result<Vec<u8>, types::LispError> {
    std::fs::read(path).map_err(|error| {
        types::LispError::Signal(format!("Cannot read {}: {}", path.display(), error))
    })
}

fn source_settings(source: &str) -> Result<SourceFileSettings, types::LispError> {
    let lexical_binding =
        extract_mode_line_variable(source, "lexical-binding").as_deref() == Some("t");
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

fn extract_mode_line_variable(source: &str, variable: &str) -> Option<String> {
    for line in source.lines().take(2) {
        let Some(start) = line.find("-*-") else {
            continue;
        };
        let contents = &line[start + 3..];
        let Some(end) = contents.find("-*-") else {
            continue;
        };
        for field in contents[..end].split(';') {
            let Some((name, value)) = field.split_once(':') else {
                continue;
            };
            if name.trim() == variable {
                return Some(value.trim().to_string());
            }
        }
    }
    None
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
        let Some((name, value)) = comment_text.split_once(':') else {
            continue;
        };
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
    let requested_source = read_source_bytes(path)?;
    // A versioned `;ELC' header does not by itself imply bytecode:
    // `byte-compile-insert-header' is also used for files containing ordinary
    // readable Lisp.  Execute those directly.  Genuine `#[...]' bytecode
    // executes on the VM when EMAXX_BYTECODE_VM=1 or when no sibling `.el'
    // exists; otherwise the sibling source remains the default until the
    // VM path is sweep-validated end to end.
    let compiled_source_path = path.with_extension("el");
    let versioned_elc = requested_source.starts_with(b";ELC\x1e");
    let source = if versioned_elc && compiled_source_path.is_file() {
        let vm_enabled = std::env::var_os("EMAXX_BYTECODE_VM").is_some_and(|flag| flag == "1");
        match String::from_utf8(requested_source) {
            Ok(source) if vm_enabled || headered_elc_is_interpretable_lisp(path, &source) => source,
            Ok(_) | Err(_) => read_source(&compiled_source_path)?,
        }
    } else {
        String::from_utf8(requested_source).map_err(|error| {
            types::LispError::Signal(format!("Cannot read {}: {}", path.display(), error))
        })?
    };
    // GNU readevalloop decides this once, before reading the first form: it
    // eagerly expands source only when macroexp.el's owner function is
    // already installed, and never for byte-compiled input.  A nested
    // `require' that defines the function must not retroactively change the
    // policy for the outer file.
    let macroexpander_ready = interp
        .lookup_function("internal-macroexpand-for-load", &types::Env::new())
        .is_ok();
    let eager_macroexpand = macroexpander_ready
        && !versioned_elc
        && path.extension().is_none_or(|extension| extension != "elc");
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
    let load_file = path.display().to_string();
    let previous = interp.set_current_load_file(Some(load_file.clone()));
    let previous_load_list = interp
        .lookup_var("current-load-list", &types::Env::new())
        .unwrap_or(types::Value::Nil);
    let previous_read_symbol_shorthands = interp
        .lookup_var("read-symbol-shorthands", &types::Env::new())
        .unwrap_or(types::Value::Nil);
    let mut env = types::Env::new();
    // GNU `load' establishes these as real specbind layers.  A Rust-only
    // current-file side channel is insufficient: an outer Lisp binding such
    // as `(let ((load-file-name nil)) (load ...))' must be shadowed by the
    // file being loaded, then restored on every exit path.
    let mut dynamic_restores = Vec::with_capacity(5);
    for (name, value) in [
        ("load-file-name", types::Value::String(load_file.clone())),
        (
            "load-true-file-name",
            types::Value::String(load_file.clone()),
        ),
        ("inhibit-file-name-operation", types::Value::Nil),
        ("load-in-progress", types::Value::T),
        (
            "lexical-binding",
            if settings.lexical_binding {
                types::Value::T
            } else {
                types::Value::Nil
            },
        ),
    ] {
        match interp.bind_special_dynamic(name, value, &mut env) {
            Ok(restore) => dynamic_restores.push(restore),
            Err(error) => {
                let _ = restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                interp.set_current_load_file(previous);
                return Err(error);
            }
        }
    }
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
            let _ = restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
            restore_load_dynamic_bindings(
                interp,
                previous_read_symbol_shorthands,
                previous_load_list,
            );
            interp.set_current_load_file(previous);
            return Err(error);
        }
    };
    // GNU's reader interns ordinary symbols as it constructs each form.
    // Emaxx deliberately keeps parsing independent from an Interpreter, so
    // reproduce that reader side effect at the common file-load boundary
    // before evaluation can inspect symbol identity via `intern-soft'.
    for form in &forms {
        interp.intern_symbols_in_value(form);
    }
    for (form_index, form) in forms.iter().enumerate() {
        let result = if eager_macroexpand {
            primitives::eager_expand_eval(interp, form, &mut env)
        } else {
            interp.eval(form, &mut env)
        };
        if let Err(error) = result {
            if std::env::var_os("EMAXX_TRACE_LOAD_ERRORS").is_some() {
                let head = form
                    .car()
                    .ok()
                    .and_then(|value| value.as_symbol().ok().map(str::to_owned))
                    .unwrap_or_else(|| form.type_name());
                eprintln!(
                    "load error in {} at form {} ({head}): {error:?}",
                    path.display(),
                    form_index + 1
                );
            }
            let _ = restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
            restore_load_dynamic_bindings(
                interp,
                previous_read_symbol_shorthands,
                previous_load_list,
            );
            interp.set_current_load_file(previous);
            return Err(error);
        }
    }
    let current_load_list = interp
        .lookup_var("current-load-list", &types::Env::new())
        .unwrap_or_else(|| types::Value::list([types::Value::String(path.display().to_string())]));
    interp.commit_entire_load_history(&load_file, current_load_list);
    if let Some(message) = warning_message {
        append_message(interp, &message);
    }
    restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env)?;
    restore_load_dynamic_bindings(interp, previous_read_symbol_shorthands, previous_load_list);
    interp.set_current_load_file(previous);
    Ok(())
}

fn restore_special_dynamic_bindings(
    interp: &mut eval::Interpreter,
    restores: &mut Vec<eval::SpecialBindingRestore>,
    env: &mut types::Env,
) -> Result<(), types::LispError> {
    while let Some(restore) = restores.pop() {
        interp.restore_special_dynamic(restore, env)?;
    }
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
    let mut env = types::Env::new();
    let lexical_restore = interp.bind_special_dynamic(
        "lexical-binding",
        if settings.lexical_binding {
            types::Value::T
        } else {
            types::Value::Nil
        },
        &mut env,
    )?;
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
            let _ = interp.restore_special_dynamic(lexical_restore, &mut env);
            restore_load_dynamic_bindings(
                &mut interp,
                previous_read_symbol_shorthands,
                previous_load_list,
            );
            interp.set_current_load_file(previous);
            return Err(error);
        }
    };

    // Evaluate all top-level forms (this collects ert-deftest definitions)
    for form in &forms {
        interp.intern_symbols_in_value(form);
    }
    for form in &forms {
        // Ignore errors in top-level forms (e.g. require of missing features)
        let _ = interp.eval(form, &mut env);
    }
    interp.restore_special_dynamic(lexical_restore, &mut env)?;
    restore_load_dynamic_bindings(
        &mut interp,
        previous_read_symbol_shorthands,
        previous_load_list,
    );
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

fn restore_load_dynamic_bindings(
    interp: &mut eval::Interpreter,
    previous_read_symbol_shorthands: types::Value,
    previous_load_list: types::Value,
) {
    interp.set_global_binding("read-symbol-shorthands", previous_read_symbol_shorthands);
    interp.set_global_binding("current-load-list", previous_load_list);
}

#[cfg(test)]
mod tests {
    use super::{extract_file_local_variable, parse_symbol_shorthands, source_settings};

    #[test]
    fn parses_compact_lexical_binding_modelines() {
        for source in [
            ";;; -*- lexical-binding:t -*-\n",
            "#!/bin/sh\n;;; -*- mode: emacs-lisp; lexical-binding: t; -*-\n",
        ] {
            assert!(
                source_settings(source)
                    .expect("source settings should parse a valid modeline")
                    .lexical_binding
            );
        }

        for source in [
            ";;; -*- lexical-binding:nil -*-\n",
            ";;; -*- not-lexical-binding: t -*-\n",
        ] {
            assert!(
                !source_settings(source)
                    .expect("source settings should reject unrelated or nil fields")
                    .lexical_binding
            );
        }
    }

    #[test]
    fn parses_read_symbol_shorthands_from_local_variables_block() {
        let source = r#"
(ert-deftest ft-sample ())

;; Local Variables:
;; read-symbol-shorthands: (("ft-" . "fns-tests-"))
;; This comment is not a variable assignment.
;; End:
"#;

        assert_eq!(
            extract_file_local_variable(source, "read-symbol-shorthands"),
            Some(r#"(("ft-" . "fns-tests-"))"#.into())
        );
        assert_eq!(
            parse_symbol_shorthands(r#"(("ft-" . "fns-tests-"))"#)
                .expect("symbol shorthand alist should parse"),
            vec![("ft-".into(), "fns-tests-".into())]
        );
        assert_eq!(
            source_settings(source)
                .expect("source settings should parse file-local symbol shorthands")
                .read_symbol_shorthands,
            vec![("ft-".into(), "fns-tests-".into())]
        );
    }
}
