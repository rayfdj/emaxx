use super::regexp;
use super::*;

pub(super) fn is_major_mode_builtin(name: &str) -> bool {
    matches!(
        name,
        "text-mode"
            | "c-mode"
            | "c++-mode"
            | "java-mode"
            | "js-mode"
            | "javascript-mode"
            | "makefile-bsdmake-mode"
            | "ruby-mode"
            | "srecode-template-mode"
            | "tex-mode"
            | "texinfo-mode"
            | "wisent-grammar-mode"
            | "css-base-mode"
            | "css-mode"
            | "latex-mode"
            | "html-mode"
            | "python-base-mode"
            | "python-mode"
            | "conf-toml-mode"
    )
}

pub(super) fn call_major_mode(interp: &mut Interpreter, name: &str) -> Result<Value, LispError> {
    match name {
        "text-mode" => {
            derived_mode_set_parent(interp, "text-mode", Some("fundamental-mode"));
            activate_major_mode(interp, "text-mode", "Text");
            Ok(Value::Nil)
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
        "java-mode" => {
            derived_mode_set_parent(interp, "java-mode", Some("prog-mode"));
            activate_c_family_mode(interp, "java-mode", "Java")
        }
        "js-mode" => {
            derived_mode_set_parent(interp, "js-mode", Some("prog-mode"));
            let result =
                activate_c_family_mode_with_semantic(interp, "js-mode", "Javascript", false)?;
            let buffer_id = interp.current_buffer_id();
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("// ".into()));
            interp.set_buffer_local_value(buffer_id, "comment-end", Value::String(String::new()));
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
        "javascript-mode" => {
            derived_mode_set_parent(interp, "javascript-mode", Some("prog-mode"));
            activate_c_family_mode_with_semantic(interp, "javascript-mode", "JavaScript", false)
        }
        "ruby-mode" => {
            derived_mode_set_parent(interp, "ruby-mode", Some("prog-mode"));
            activate_hash_comment_mode_with_semantic(interp, "ruby-mode", "Ruby", false)
        }
        "makefile-bsdmake-mode" => {
            activate_hash_comment_mode(interp, "makefile-bsdmake-mode", "BSDmakefile")
        }
        "srecode-template-mode" => activate_semicolon_comment_mode(interp, name, "SRecode"),
        "tex-mode" => {
            derived_mode_set_parent(interp, "tex-mode", Some("text-mode"));
            let buffer_id = interp.current_buffer_id();
            activate_major_mode(interp, "tex-mode", "TeX");
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("%".into()));
            interp.set_buffer_local_value(buffer_id, "comment-end", Value::String(String::new()));
            Ok(Value::Nil)
        }
        "texinfo-mode" => {
            derived_mode_set_parent(interp, "texinfo-mode", Some("text-mode"));
            let buffer_id = interp.current_buffer_id();
            activate_major_mode(interp, "texinfo-mode", "Texinfo");
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("@c ".into()));
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
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("%".into()));
            interp.set_buffer_local_value(buffer_id, "comment-end", Value::String(String::new()));
            Ok(Value::Nil)
        }
        "html-mode" => {
            derived_mode_set_parent(interp, "html-mode", Some("text-mode"));
            let buffer_id = interp.current_buffer_id();
            activate_major_mode(interp, "html-mode", "HTML");
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("<!--".into()));
            interp.set_buffer_local_value(buffer_id, "comment-end", Value::String("-->".into()));
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
            activate_hash_comment_mode_with_semantic(interp, "python-base-mode", "Python", false)
        }
        "python-mode" => {
            derived_mode_set_parent(interp, "python-mode", Some("python-base-mode"));
            activate_hash_comment_mode_with_semantic(interp, "python-mode", "Python", false)
        }
        "conf-toml-mode" => {
            derived_mode_set_parent(interp, "conf-toml-mode", Some("conf-mode"));
            let buffer_id = interp.current_buffer_id();
            activate_major_mode(interp, "conf-toml-mode", "Conf[TOML]");
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String("#".into()));
            interp.set_buffer_local_value(
                buffer_id,
                "comment-start-skip",
                Value::String("#+\\s-*".into()),
            );
            Ok(Value::Nil)
        }
        _ => Err(LispError::Signal(format!("Void function: {name}"))),
    }
}

fn activate_hash_comment_mode(
    interp: &mut Interpreter,
    mode: &str,
    mode_name: &str,
) -> Result<Value, LispError> {
    activate_hash_comment_mode_with_semantic(interp, mode, mode_name, true)
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
    interp.set_buffer_local_value(buffer_id, "indent-tabs-mode", Value::T);
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
        Value::Symbol(symbol) => Some(symbol.clone()),
        Value::Cons(_, _) => value.to_vec().ok().and_then(|parts| {
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

pub(super) fn auto_mode_function_for_file_name(
    interp: &Interpreter,
    env: &Env,
    path: &str,
) -> Result<Option<String>, LispError> {
    let Some(entries) = interp.lookup_var("auto-mode-alist", env) else {
        return Ok(None);
    };
    for candidate in auto_mode_candidates(interp, env, path) {
        for entry in entries.to_vec()? {
            let Value::Cons(pattern, mode) = entry else {
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
