use super::regexp;
use super::*;

define_dispatch!(
    pub(super) fn call(interp: &mut Interpreter, name: &str) -> Result<Value, LispError> {
        match name {
            "text-mode" => {
                derived_mode_set_parent(interp, "text-mode", Some("fundamental-mode"));
                activate_text_mode(interp)
            }
            "outline-mode" => {
                derived_mode_set_parent(interp, "outline-mode", Some("text-mode"));
                activate_major_mode(interp, "outline-mode", "Outline");
                Ok(Value::Nil)
            }
            "mhtml-mode" => {
                derived_mode_set_parent(interp, "mhtml-mode", Some("html-mode"));
                activate_major_mode(interp, "mhtml-mode", "HTML+");
                Ok(Value::Nil)
            }
            "tcl-mode" => {
                derived_mode_set_parent(interp, "tcl-mode", Some("prog-mode"));
                activate_hash_comment_mode(interp, "tcl-mode", "Tcl")
            }
            "awk-mode" => {
                derived_mode_set_parent(interp, "awk-mode", Some("prog-mode"));
                activate_hash_comment_mode(interp, "awk-mode", "AWK")
            }
            "sh-base-mode" => {
                derived_mode_set_parent(interp, "sh-base-mode", Some("prog-mode"));
                activate_hash_comment_mode(interp, "sh-base-mode", "Shell-script")
            }
            "sh-mode" => {
                derived_mode_set_parent(interp, "sh-base-mode", Some("prog-mode"));
                derived_mode_set_parent(interp, "sh-mode", Some("sh-base-mode"));
                let result = activate_hash_comment_mode(interp, "sh-mode", "Shell-script")?;
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "sh-shell",
                    Value::Symbol("sh".into()),
                );
                Ok(result)
            }
            "makefile-mode" => {
                derived_mode_set_parent(interp, "makefile-mode", Some("prog-mode"));
                activate_hash_comment_mode(interp, "makefile-mode", "Makefile")
            }
            "makefile-gmake-mode" => {
                derived_mode_set_parent(interp, "makefile-mode", Some("prog-mode"));
                derived_mode_set_parent(interp, "makefile-gmake-mode", Some("makefile-mode"));
                activate_hash_comment_mode(interp, "makefile-gmake-mode", "GNUmakefile")
            }
            "c-mode" => {
                derived_mode_set_parent(interp, "c-mode", Some("prog-mode"));
                activate_c_family_mode(interp, "c-mode", "C")
            }
            "c++-mode" => {
                derived_mode_set_parent(interp, "c-mode", Some("prog-mode"));
                derived_mode_set_parent(interp, "c++-mode", Some("c-mode"));
                activate_c_family_mode(interp, "c++-mode", "C++")
            }
            #[dispatch(builtin_override)]
            "java-mode" => {
                derived_mode_set_parent(interp, "java-mode", Some("prog-mode"));
                activate_c_family_mode(interp, "java-mode", "Java")
            }
            #[dispatch(builtin_override)]
            "js-mode" => {
                derived_mode_set_parent(interp, "js-mode", Some("prog-mode"));
                let result =
                    activate_c_family_mode_with_semantic(interp, "js-mode", "Javascript", false)?;
                let buffer_id = interp.current_buffer_id();
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start",
                    Value::String("// ".into()),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-end",
                    Value::String(String::new().into()),
                );
                interp.set_buffer_local_value(buffer_id, "c-basic-offset", Value::Integer(4));
                interp.set_buffer_local_value(
                    buffer_id,
                    "electric-indent-chars",
                    Value::list("{}():;,\n".chars().map(|ch| Value::Integer(ch as i64))),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "electric-layout-rules",
                    Value::list([
                        Value::cons(Value::Integer(';' as i64), Value::Symbol("after".into())),
                        Value::cons(Value::Integer('{' as i64), Value::Symbol("after".into())),
                        Value::cons(Value::Integer('}' as i64), Value::Symbol("before".into())),
                    ]),
                );
                Ok(result)
            }
            // GNU js.el makes `javascript-mode' a defalias for `js-mode'.
            #[dispatch(builtin_override)]
            "javascript-mode" => call(interp, "js-mode"),
            "ruby-mode" => {
                derived_mode_set_parent(interp, "ruby-mode", Some("prog-mode"));
                activate_ruby_mode(interp)
            }
            "makefile-bsdmake-mode" => {
                activate_hash_comment_mode(interp, "makefile-bsdmake-mode", "BSDmakefile")
            }
            "srecode-template-mode" => activate_semicolon_comment_mode(interp, name, "SRecode"),
            "tex-mode" => {
                derived_mode_set_parent(interp, "tex-mode", Some("text-mode"));
                let buffer_id = interp.current_buffer_id();
                activate_major_mode(interp, "tex-mode", "TeX");
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start",
                    Value::String("%".into()),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-end",
                    Value::String(String::new().into()),
                );
                Ok(Value::Nil)
            }
            "texinfo-mode" => {
                derived_mode_set_parent(interp, "texinfo-mode", Some("text-mode"));
                let buffer_id = interp.current_buffer_id();
                activate_major_mode(interp, "texinfo-mode", "Texinfo");
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start",
                    Value::String("@c ".into()),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start-skip",
                    Value::String("@c\\(?:omment\\)?\\s-*".into()),
                );
                activate_semantic_buffer_if_enabled(interp, buffer_id)?;
                Ok(Value::Nil)
            }
            "wisent-grammar-mode" => {
                let buffer_id = interp.current_buffer_id();
                let result = activate_semicolon_comment_mode(interp, name, "Wisent")?;
                interp.set_buffer_local_value(
                    buffer_id,
                    "semantic-new-buffer-fcn-was-run",
                    Value::T,
                );
                if interp
                    .lookup_function("semantic-lex-init", &Vec::new())
                    .is_ok()
                {
                    call_function_value(
                        interp,
                        &Value::Symbol("semantic-lex-init".into()),
                        &[],
                        &mut Vec::new(),
                    )?;
                }
                Ok(result)
            }
            "css-base-mode" => {
                derived_mode_set_parent(interp, "css-base-mode", Some("prog-mode"));
                activate_c_block_comment_mode(interp, "css-base-mode", "CSS")
            }
            "css-mode" => {
                derived_mode_set_parent(interp, "css-mode", Some("css-base-mode"));
                activate_c_block_comment_mode(interp, "css-mode", "CSS")
            }
            "latex-mode" => {
                derived_mode_set_parent(interp, "latex-mode", Some("tex-mode"));
                let buffer_id = interp.current_buffer_id();
                activate_major_mode(interp, "latex-mode", "LaTeX");
                interp.set_buffer_local_value(buffer_id, "indent-tabs-mode", Value::Nil);
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start",
                    Value::String("%".into()),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-end",
                    Value::String(String::new().into()),
                );
                Ok(Value::Nil)
            }
            "html-mode" => {
                derived_mode_set_parent(interp, "html-mode", Some("text-mode"));
                let buffer_id = interp.current_buffer_id();
                activate_major_mode(interp, "html-mode", "HTML");
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start",
                    Value::String("<!--".into()),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-end",
                    Value::String("-->".into()),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start-skip",
                    Value::String("<!--+\\s-*".into()),
                );
                activate_semantic_buffer_if_enabled(interp, buffer_id)?;
                Ok(Value::Nil)
            }
            "python-base-mode" => {
                derived_mode_set_parent(interp, "python-base-mode", Some("prog-mode"));
                activate_hash_comment_mode_with_semantic(
                    interp,
                    "python-base-mode",
                    "Python",
                    false,
                )
            }
            "python-mode" => {
                derived_mode_set_parent(interp, "python-mode", Some("python-base-mode"));
                let result = activate_hash_comment_mode_with_semantic(
                    interp,
                    "python-mode",
                    "Python",
                    false,
                );
                // GNU python.el marks triple-quote fences via
                // syntax-propertize; sexp motion needs them.
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "syntax-propertize-function",
                    Value::Symbol("emaxx--python-syntax-propertize".into()),
                );
                result
            }
            "conf-toml-mode" => {
                derived_mode_set_parent(interp, "conf-toml-mode", Some("conf-mode"));
                let buffer_id = interp.current_buffer_id();
                activate_major_mode(interp, "conf-toml-mode", "Conf[TOML]");
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start",
                    Value::String("#".into()),
                );
                interp.set_buffer_local_value(
                    buffer_id,
                    "comment-start-skip",
                    Value::String("#+\\s-*".into()),
                );
                Ok(Value::Nil)
            }
        }
    }
);

fn activate_hash_comment_mode(
    interp: &mut Interpreter,
    mode: &str,
    mode_name: &str,
) -> Result<Value, LispError> {
    activate_hash_comment_mode_with_semantic(interp, mode, mode_name, true)
}

fn activate_text_mode(interp: &mut Interpreter) -> Result<Value, LispError> {
    activate_major_mode(interp, "text-mode", "Text");
    let Value::CharTable(syntax_table_id) =
        interp.make_char_table(Some("syntax-table".into()), Value::Nil)
    else {
        unreachable!("make_char_table returns a char-table");
    };
    interp.set_char_table_parent(syntax_table_id, Some(interp.standard_syntax_table_id()))?;
    interp.char_table_set(syntax_table_id, '"' as u32, Value::String(".".into()))?;
    interp.char_table_set(syntax_table_id, '`' as u32, Value::String(".".into()))?;
    interp.char_table_set(syntax_table_id, '\'' as u32, Value::String("w".into()))?;
    interp.set_current_syntax_table(syntax_table_id);
    Ok(Value::Nil)
}

fn activate_hash_comment_mode_with_semantic(
    interp: &mut Interpreter,
    mode: &str,
    mode_name: &str,
    call_semantic_setup: bool,
) -> Result<Value, LispError> {
    if interp
        .get_symbol_property(mode, "derived-mode-parent")
        .is_none()
    {
        derived_mode_set_parent(interp, mode, Some("prog-mode"));
    }
    let buffer_id = interp.current_buffer_id();
    activate_major_mode(interp, mode, mode_name);
    interp.set_buffer_local_value(buffer_id, "indent-tabs-mode", Value::Nil);
    interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("# ".into()));
    interp.set_buffer_local_value(
        buffer_id,
        "comment-start-skip",
        Value::String("#+\\s-*".into()),
    );
    let Value::CharTable(syntax_table_id) =
        interp.make_char_table(Some("syntax-table".into()), Value::Nil)
    else {
        unreachable!("make_char_table returns a char-table");
    };
    interp.set_char_table_parent(syntax_table_id, Some(interp.standard_syntax_table_id()))?;
    interp.char_table_set(syntax_table_id, '#' as u32, Value::String("< b".into()))?;
    interp.char_table_set(syntax_table_id, '\n' as u32, Value::String("> b".into()))?;
    interp.char_table_set(syntax_table_id, '\\' as u32, Value::String("\\".into()))?;
    interp.set_current_syntax_table(syntax_table_id);
    if call_semantic_setup {
        activate_semantic_buffer_if_enabled(interp, buffer_id)?;
    } else {
        mark_semantic_buffer_active_if_enabled(interp, buffer_id)?;
    }
    Ok(Value::Nil)
}

fn activate_ruby_mode(interp: &mut Interpreter) -> Result<Value, LispError> {
    activate_hash_comment_mode_with_semantic(interp, "ruby-mode", "Ruby", false)?;
    let syntax_table_id = interp.current_syntax_table_id();
    for quote in ['\'', '"', '`'] {
        interp.char_table_set(syntax_table_id, quote as u32, Value::String("\"".into()))?;
    }
    for symbol_quote in ['$', ':', '@'] {
        interp.char_table_set(
            syntax_table_id,
            symbol_quote as u32,
            Value::String("'".into()),
        )?;
    }
    for punctuation in ['<', '>', '&', '|', '%', '=', '/', '+', '*', '-', ';'] {
        interp.char_table_set(
            syntax_table_id,
            punctuation as u32,
            Value::String(".".into()),
        )?;
    }
    for (open, close) in [('(', ')'), ('{', '}'), ('[', ']')] {
        interp.char_table_set(
            syntax_table_id,
            open as u32,
            Value::String(format!("({close}").into()),
        )?;
        interp.char_table_set(
            syntax_table_id,
            close as u32,
            Value::String(format!("){open}").into()),
        )?;
    }
    Ok(Value::Nil)
}

fn activate_semicolon_comment_mode(
    interp: &mut Interpreter,
    mode: &str,
    mode_name: &str,
) -> Result<Value, LispError> {
    derived_mode_set_parent(interp, mode, Some("prog-mode"));
    let buffer_id = interp.current_buffer_id();
    activate_major_mode(interp, mode, mode_name);
    interp.set_buffer_local_value(buffer_id, "comment-start", Value::String(";;".into()));
    interp.set_buffer_local_value(
        buffer_id,
        "comment-start-skip",
        Value::String(";;+\\s-*".into()),
    );
    activate_semantic_buffer_if_enabled(interp, buffer_id)?;
    Ok(Value::Nil)
}

fn activate_c_block_comment_mode(
    interp: &mut Interpreter,
    mode: &str,
    mode_name: &str,
) -> Result<Value, LispError> {
    let buffer_id = interp.current_buffer_id();
    activate_major_mode(interp, mode, mode_name);
    interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("/*".into()));
    interp.set_buffer_local_value(
        buffer_id,
        "comment-start-skip",
        Value::String("/\\*+[ \t]*".into()),
    );
    interp.set_buffer_local_value(buffer_id, "comment-end", Value::String("*/".into()));
    interp.set_buffer_local_value(
        buffer_id,
        "comment-end-skip",
        Value::String("[ \t]*\\*+/".into()),
    );
    Ok(Value::Nil)
}

fn activate_major_mode(interp: &mut Interpreter, mode: &str, mode_name: &str) {
    let buffer_id = interp.current_buffer_id();
    interp.set_buffer_local_value(buffer_id, "major-mode", Value::Symbol(mode.into()));
    interp.set_buffer_local_value(buffer_id, "mode-name", Value::String(mode_name.into()));
}

fn activate_semantic_buffer_if_enabled(
    interp: &mut Interpreter,
    buffer_id: u64,
) -> Result<(), LispError> {
    if !interp
        .lookup_var("semantic-mode", &Vec::new())
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    if interp
        .lookup_function("semantic-new-buffer-fcn", &Vec::new())
        .is_ok()
    {
        call_function_value(
            interp,
            &Value::Symbol("semantic-new-buffer-fcn".into()),
            &[],
            &mut Vec::new(),
        )?;
        return Ok(());
    }
    interp.set_buffer_local_value(buffer_id, "semantic-new-buffer-fcn-was-run", Value::T);
    if interp
        .lookup_function("semantic-lex-init", &Vec::new())
        .is_ok()
    {
        call_function_value(
            interp,
            &Value::Symbol("semantic-lex-init".into()),
            &[],
            &mut Vec::new(),
        )?;
    }
    Ok(())
}

fn activate_c_family_mode(
    interp: &mut Interpreter,
    mode: &str,
    mode_name: &str,
) -> Result<Value, LispError> {
    activate_c_family_mode_with_semantic(interp, mode, mode_name, true)
}

fn activate_c_family_mode_with_semantic(
    interp: &mut Interpreter,
    mode: &str,
    mode_name: &str,
    call_semantic_setup: bool,
) -> Result<Value, LispError> {
    if !interp.has_feature("newcomment") && interp.resolve_load_target("newcomment").is_some() {
        interp.load_target("newcomment")?;
    }
    let buffer_id = interp.current_buffer_id();
    activate_major_mode(interp, mode, mode_name);
    // cc-vars.el declares this style variable through
    // `custom-declare-variable', making it special.  Native mode activation
    // must provide that binding contract so a caller's `let' remains visible
    // inside separately defined electric-indent code.  The native indenter's
    // style fallback remains separate; forcing one style value here would
    // incorrectly override GNU's per-style resolution.
    interp.mark_special_variable("c-basic-offset");
    // CC Mode advertises a real, reindenting line function.  Electric Indent
    // deliberately suppresses modes that still advertise `indent-relative',
    // so leaving the fundamental-mode default here disables layout/indent
    // cooperation even though the native C indenter exists.
    interp.set_buffer_local_value(
        buffer_id,
        "indent-line-function",
        Value::Symbol("c-indent-line".into()),
    );
    interp.set_buffer_local_value(buffer_id, "indent-tabs-mode", Value::T);
    interp.set_buffer_local_value(
        buffer_id,
        "c-line-comment-starter",
        Value::String("//".into()),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "c-block-comment-starter",
        Value::String("/*".into()),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "c-block-comment-ender",
        Value::String("*/".into()),
    );
    interp.set_buffer_local_value(buffer_id, "c-block-comment-flag", Value::T);
    interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("/* ".into()));
    interp.set_buffer_local_value(buffer_id, "comment-end", Value::String(" */".into()));
    interp.set_buffer_local_value(
        buffer_id,
        "comment-start-skip",
        Value::String("\\(?://+\\|/\\*+\\)\\s *".into()),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "comment-end-skip",
        Value::String("[ \t]*\\*+/".into()),
    );
    interp.set_buffer_local_value(buffer_id, "comment-use-syntax", Value::T);
    interp.set_buffer_local_value(buffer_id, "comment-style", Value::Symbol("indent".into()));
    interp.set_buffer_local_value(buffer_id, "comment-multi-line", Value::T);
    interp.set_buffer_local_value(buffer_id, "font-lock-mode", Value::T);
    interp.set_buffer_local_value(buffer_id, "jit-lock-mode", Value::T);
    if interp
        .buffer_local_value(buffer_id, "jit-lock-functions")
        .is_none()
    {
        interp.set_buffer_local_value(
            buffer_id,
            "jit-lock-functions",
            Value::list([Value::Symbol("ignore".into())]),
        );
    }
    interp.set_buffer_local_value(buffer_id, "font-lock-fontified", Value::T);
    let Value::CharTable(syntax_table_id) =
        interp.make_char_table(Some("syntax-table".into()), Value::Nil)
    else {
        unreachable!("make_char_table returns a char-table");
    };
    interp.set_char_table_parent(syntax_table_id, Some(interp.standard_syntax_table_id()))?;
    interp.char_table_set(syntax_table_id, '/' as u32, Value::String(". 124b".into()))?;
    interp.char_table_set(syntax_table_id, '*' as u32, Value::String(". 23".into()))?;
    interp.char_table_set(syntax_table_id, '\n' as u32, Value::String("> b".into()))?;
    interp.char_table_set(syntax_table_id, '\\' as u32, Value::String("\\".into()))?;
    interp.set_current_syntax_table(syntax_table_id);
    if call_semantic_setup {
        activate_semantic_buffer_if_enabled(interp, buffer_id)?;
    } else {
        mark_semantic_buffer_active_if_enabled(interp, buffer_id)?;
    }
    Ok(Value::Nil)
}

fn mark_semantic_buffer_active_if_enabled(
    interp: &mut Interpreter,
    buffer_id: u64,
) -> Result<(), LispError> {
    if !interp
        .lookup_var("semantic-mode", &Vec::new())
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    interp.set_buffer_local_value(buffer_id, "semantic-new-buffer-fcn-was-run", Value::T);
    Ok(())
}

fn auto_mode_symbol_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(symbol) => Some(symbol.to_string()),
        Value::Cons(_) => value.to_vec().ok().and_then(|parts| {
            if parts.len() == 2
                && matches!(parts.first(), Some(Value::Symbol(keyword)) if keyword == "quote")
            {
                parts[1].as_symbol().ok().map(str::to_string)
            } else {
                None
            }
        }),
        _ => None,
    }
}

fn auto_mode_function_from_entries(
    interp: &Interpreter,
    env: &Env,
    path: &str,
    entries: &Value,
) -> Result<Option<String>, LispError> {
    for candidate in auto_mode_candidates(interp, env, path) {
        for entry in entries.to_vec()? {
            let Some((pattern, mode)) = (entry).cons_cells() else {
                continue;
            };
            let pattern = pattern.borrow().clone();
            let mode = mode.borrow().clone();
            let Some(pattern) = string_like(&pattern) else {
                continue;
            };
            regexp::validate_elisp_regex(&pattern.text)?;
            let regex = regexp::compile_elisp_regex(interp, &pattern, env, "", true)?;
            if regex
                .is_match(&candidate)
                .map_err(|error| LispError::Signal(error.to_string()))?
                && let Some(mode_symbol) = auto_mode_symbol_from_value(&mode)
            {
                return Ok(Some(mode_symbol));
            }
        }
    }
    Ok(None)
}

pub(super) fn auto_mode_function_for_file_name(
    interp: &Interpreter,
    env: &Env,
    path: &str,
) -> Result<Option<String>, LispError> {
    let Some(entries) = interp.lookup_var("auto-mode-alist", env) else {
        return Ok(None);
    };
    auto_mode_function_from_entries(interp, env, path, &entries)
}

/// GNU's `set-auto-mode' executes every mode cookie from left to right.
/// Header cookies take precedence over the trailing Local Variables block.
pub(super) fn file_local_mode_functions(source: &str) -> (Vec<String>, Vec<String>) {
    fn normalized_mode(text: &str) -> Option<String> {
        let name = text.trim().to_ascii_lowercase();
        (!name.is_empty()).then(|| {
            if name.ends_with("-mode") {
                name
            } else {
                format!("{name}-mode")
            }
        })
    }

    fn modes_in_spec(spec: &str) -> Vec<String> {
        if !spec.contains(':') {
            return normalized_mode(spec).into_iter().collect();
        }
        spec.split(';')
            .filter_map(|setting| {
                let (name, value) = setting.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("mode")
                    .then(|| normalized_mode(value))
                    .flatten()
            })
            .collect()
    }

    let lines = source.lines().collect::<Vec<_>>();
    let header_line = match lines.as_slice() {
        [first, second, ..]
            if first.trim_start().starts_with("#!") || first.trim_start().starts_with("'\"") =>
        {
            *second
        }
        [first, ..] => *first,
        [] => "",
    };
    let header = header_line
        .find("-*-")
        .and_then(|start| {
            let rest = &header_line[start + 3..];
            rest.find("-*-").map(|end| modes_in_spec(&rest[..end]))
        })
        .unwrap_or_default();

    let mut tail = Vec::new();
    let mut in_locals = false;
    for line in lines {
        let trimmed = line.trim();
        if !in_locals {
            in_locals = trimmed.eq_ignore_ascii_case("local variables:");
            continue;
        }
        if trimmed.eq_ignore_ascii_case("end:") {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':')
            && name.trim().eq_ignore_ascii_case("mode")
            && let Some(mode) = normalized_mode(value)
        {
            tail.push(mode);
        }
    }
    (header, tail)
}

pub(super) fn directory_local_auto_mode(
    interp: &Interpreter,
    env: &Env,
    path: &str,
) -> Result<Option<String>, LispError> {
    let Some(parent) = Path::new(path).parent() else {
        return Ok(None);
    };
    for directory in parent.ancestors() {
        let file = directory.join(".dir-locals.el");
        let Ok(source) = fs::read_to_string(file) else {
            continue;
        };
        let Some(form) = crate::lisp::reader::Reader::new(&source).read()? else {
            return Ok(None);
        };
        for entry in form.to_vec()? {
            let Some((key, entries)) = entry.cons_values() else {
                continue;
            };
            if key == Value::Symbol("auto-mode-alist".into())
                && let Some(mode) = auto_mode_function_from_entries(interp, env, path, &entries)?
                && mode.ends_with("-mode")
                && mode != "dired-mode"
            {
                return Ok(Some(mode));
            }
        }
        return Ok(None);
    }
    Ok(None)
}

fn mode_from_interpreter_alist(
    interp: &Interpreter,
    env: &Env,
    interpreter: &str,
) -> Result<Option<String>, LispError> {
    let Some(entries) = interp.lookup_var("interpreter-mode-alist", env) else {
        return Ok(None);
    };
    for entry in entries.to_vec()? {
        let Some((pattern, mode)) = entry.cons_values() else {
            continue;
        };
        let Some(pattern) = string_like(&pattern) else {
            continue;
        };
        let anchored = StringLike {
            text: format!(r"\`\(?:{}\)\'", pattern.text),
            props: Vec::new(),
            multibyte: pattern.multibyte,
        };
        let regex = regexp::compile_elisp_regex(interp, &anchored, env, "", true)?;
        if regex
            .is_match(interpreter)
            .map_err(|error| LispError::Signal(error.to_string()))?
        {
            return Ok(auto_mode_symbol_from_value(&mode));
        }
    }
    Ok(None)
}

pub(super) fn interpreter_mode_function(
    interp: &Interpreter,
    env: &Env,
    source: &str,
) -> Result<Option<(String, Option<String>)>, LispError> {
    let Some(first_line) = source.lines().next() else {
        return Ok(None);
    };
    let Some(command) = first_line.trim_start().strip_prefix("#!") else {
        return Ok(None);
    };
    let mut words = command.split_whitespace().collect::<Vec<_>>();
    let Some(program) = words.first().copied() else {
        return Ok(None);
    };
    let mut interpreter = file_name_nondirectory(program);
    if interpreter == "env" {
        words.remove(0);
        let mut index = 0;
        while index < words.len() {
            let word = words[index];
            if let Some(split) = word.strip_prefix("--split-string=") {
                let mut expansion = split.split_whitespace().collect::<Vec<_>>();
                expansion.extend_from_slice(&words[index + 1..]);
                words = expansion;
                index = 0;
                continue;
            }
            if let Some(suffix) = word
                .strip_prefix('-')
                .and_then(|options| options.find('S').map(|at| &options[at + 1..]))
                && !suffix.is_empty()
            {
                let mut expansion = vec![suffix];
                expansion.extend_from_slice(&words[index + 1..]);
                words = expansion;
                index = 0;
                continue;
            }
            if word.starts_with('-') || word.contains('=') {
                index += 1;
                continue;
            }
            interpreter = file_name_nondirectory(word);
            break;
        }
    }

    let dynamic = mode_from_interpreter_alist(interp, env, &interpreter)?;
    let fallback = match interpreter.as_str() {
        "awk" => Some("awk-mode"),
        "make" => Some("makefile-gmake-mode"),
        "python" | "python2" | "python3" => Some("python-mode"),
        "bash" | "bash2" | "rbash" | "rbash2" | "sh" | "sh5" | "dash" | "ksh" | "mksh" | "zsh"
        | "ash" | "csh" | "tcsh" => Some("sh-mode"),
        _ => None,
    };
    let Some(mode) = dynamic.or_else(|| fallback.map(str::to_string)) else {
        return Ok(None);
    };
    let dialect = (mode == "sh-mode").then(|| match interpreter.as_str() {
        "bash" | "bash2" | "rbash" | "rbash2" => "bash".to_string(),
        other => other.to_string(),
    });
    Ok(Some((mode, dialect)))
}

pub(super) fn magic_mode_function(
    interp: &mut Interpreter,
    env: &mut Env,
    source: &str,
    variable: &str,
) -> Result<Option<String>, LispError> {
    if let Some(entries) = interp.lookup_var(variable, env) {
        let limit = interp
            .lookup_var("magic-mode-regexp-match-limit", env)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(4000)
            .max(0) as usize;
        let beginning = source.chars().take(limit).collect::<String>();
        for entry in entries.to_vec()? {
            let Some((matcher, mode)) = entry.cons_values() else {
                continue;
            };
            let matched = if let Some(pattern) = string_like(&matcher) {
                let mut regexp_env = env.clone();
                regexp_env.push(vec![("case-fold-search".into(), Value::Nil)].into());
                let regex = regexp::compile_elisp_regex(interp, &pattern, &regexp_env, "", true)?;
                regex
                    .captures(&beginning)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                    .and_then(|captures| captures.get(0).map(|found| found.start() == 0))
                    .unwrap_or(false)
            } else {
                interp
                    .call_function_value(matcher, None, &[], env)
                    .is_ok_and(|value| value.is_truthy())
            };
            if matched {
                return Ok(auto_mode_symbol_from_value(&mode));
            }
        }
    }
    if variable == "magic-fallback-mode-alist"
        && source
            .trim_start_matches(|ch: char| ch.is_ascii_whitespace())
            .to_ascii_lowercase()
            .starts_with("<!doctype html")
    {
        return Ok(Some("mhtml-mode".into()));
    }
    Ok(None)
}

pub(super) fn auto_mode_function_for_contents(bytes: &[u8]) -> Option<&'static str> {
    let zip_split =
        bytes.starts_with(b"PK\x07\x08PK\x03\x04") || bytes.starts_with(b"PK00PK\x03\x04");
    if bytes.starts_with(b"PK\x03\x04") || zip_split {
        return Some("archive-mode");
    }
    if bytes.starts_with(b"!<arch>\n")
        || bytes.starts_with(b"Rar!")
        || bytes.starts_with(b"7z\xbc\xaf\x27\x1c")
    {
        return Some("archive-mode");
    }
    None
}
