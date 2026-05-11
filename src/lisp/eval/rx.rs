use super::*;

pub(super) fn compile_rx_sequence(
    interp: &mut Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<String, LispError> {
    let mut regex = String::new();
    for item in items {
        regex.push_str(&compile_rx_form(interp, env, item)?);
    }
    Ok(regex)
}

pub(crate) fn compile_rx_to_string(
    interp: &mut Interpreter,
    form: &Value,
    env: &Env,
    _no_group: bool,
) -> Result<String, LispError> {
    compile_rx_form(interp, env, form)
}

fn expand_rx_splice_markers(
    interp: &Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<Vec<Value>, LispError> {
    let mut expanded = Vec::new();
    let mut index = 0usize;
    while index < items.len() {
        if matches!(&items[index], Value::Symbol(symbol) if symbol == ",") {
            let Some(source) = items.get(index + 1) else {
                return Err(LispError::Signal(
                    "rx splice marker needs a following value".into(),
                ));
            };
            let value = match source {
                Value::Symbol(name) => interp
                    .lookup_var(name, env)
                    .ok_or_else(|| LispError::Void(name.clone()))?,
                other => other.clone(),
            };
            if let Ok(values) = value.to_vec() {
                expanded.extend(values);
            } else {
                expanded.push(value);
            }
            index += 2;
            continue;
        }
        expanded.push(items[index].clone());
        index += 1;
    }
    Ok(expanded)
}

fn compile_rx_literal_form(
    interp: &mut Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<String, LispError> {
    if items.len() != 2 {
        return Err(LispError::Signal("rx `literal' needs one argument".into()));
    }
    let mut literal_env = env.clone();
    let value = interp.eval(&items[1], &mut literal_env)?;
    match value {
        Value::String(text) => Ok(quote_rx_string_literal(&text)),
        Value::StringObject(state) => Ok(quote_rx_string_literal(&state.borrow().text)),
        other => Err(LispError::TypeError("string".into(), other.type_name())),
    }
}

fn compile_rx_regexp_form(
    interp: &mut Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<String, LispError> {
    if items.len() != 2 {
        return Err(LispError::Signal("rx `regexp' needs one string".into()));
    }
    let mut regexp_env = env.clone();
    let value = interp.eval(&items[1], &mut regexp_env)?;
    match value {
        Value::String(text) => Ok(text),
        Value::StringObject(state) => Ok(state.borrow().text.clone()),
        other => Err(LispError::TypeError("string".into(), other.type_name())),
    }
}

fn quote_rx_string_literal(text: &str) -> String {
    let mut quoted = String::new();
    for ch in text.chars() {
        match ch {
            '.' | '[' | '*' | '+' | '?' | '^' | '$' | '\\' => {
                quoted.push('\\');
                quoted.push(ch);
            }
            _ => quoted.push(ch),
        }
    }
    quoted
}

fn rx_char_class_name(symbol: &str) -> Option<&'static str> {
    match symbol {
        "digit" | "numeric" | "num" => Some("digit"),
        "control" | "cntrl" => Some("cntrl"),
        "hex-digit" | "hex" | "xdigit" => Some("xdigit"),
        "blank" => Some("blank"),
        "graphic" | "graph" => Some("graph"),
        "printing" | "print" => Some("print"),
        "alphanumeric" | "alnum" => Some("alnum"),
        "letter" | "alphabetic" | "alpha" => Some("alpha"),
        "ascii" => Some("ascii"),
        "nonascii" => Some("nonascii"),
        "lower" | "lower-case" => Some("lower"),
        "punctuation" | "punct" => Some("punct"),
        "space" | "whitespace" | "white" => Some("space"),
        "upper" | "upper-case" => Some("upper"),
        "word" | "wordchar" => Some("word"),
        "unibyte" => Some("unibyte"),
        "multibyte" => Some("multibyte"),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RxCharInterval {
    start: char,
    end: char,
}

fn rx_codepoint_to_char(codepoint: i64) -> Result<char, LispError> {
    char::from_u32(codepoint as u32)
        .ok_or_else(|| LispError::Signal(format!("Invalid rx character: {codepoint}")))
}

fn rx_interval_string(text: &str, index: usize) -> String {
    text.chars().skip(index).take(3).collect()
}

fn append_rx_string_intervals(
    intervals: &mut Vec<RxCharInterval>,
    text: &str,
) -> Result<(), LispError> {
    let chars: Vec<char> = text.chars().collect();
    let mut index = 0usize;
    while index < chars.len() {
        if index + 2 < chars.len() && chars[index + 1] == '-' {
            let start = chars[index];
            let end = chars[index + 2];
            if start > end {
                return Err(LispError::Signal(format!(
                    "Invalid rx `any' range: {}",
                    rx_interval_string(text, index)
                )));
            }
            intervals.push(RxCharInterval { start, end });
            index += 3;
        } else {
            let ch = chars[index];
            intervals.push(RxCharInterval { start: ch, end: ch });
            index += 1;
        }
    }
    Ok(())
}

fn parse_rx_char_class_items(
    items: &[Value],
) -> Result<(Vec<RxCharInterval>, Vec<&'static str>), LispError> {
    let mut intervals = Vec::new();
    let mut classes = Vec::new();

    for item in items {
        match item {
            Value::String(text) => append_rx_string_intervals(&mut intervals, text)?,
            Value::StringObject(state) => {
                append_rx_string_intervals(&mut intervals, &state.borrow().text)?
            }
            Value::Integer(codepoint) => {
                let ch = rx_codepoint_to_char(*codepoint)?;
                intervals.push(RxCharInterval { start: ch, end: ch });
            }
            Value::Symbol(symbol) => {
                let Some(name) = rx_char_class_name(symbol) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported rx charset fragment: {}",
                        item.type_name()
                    )));
                };
                if !classes.contains(&name) {
                    classes.push(name);
                }
            }
            Value::Cons(_, _) => {
                let (start, end) = item.cons_values().ok_or_else(|| {
                    LispError::Signal("Unsupported rx charset fragment: cons".into())
                })?;
                let start = rx_codepoint_to_char(start.as_integer()?)?;
                let end = rx_codepoint_to_char(end.as_integer()?)?;
                if start > end {
                    return Err(LispError::Signal(format!(
                        "Invalid rx `any' range: {}-{}",
                        start, end
                    )));
                }
                intervals.push(RxCharInterval { start, end });
            }
            other => {
                return Err(LispError::Signal(format!(
                    "Unsupported rx charset fragment: {}",
                    other.type_name()
                )));
            }
        }
    }

    intervals.sort_by_key(|interval| (u32::from(interval.start), u32::from(interval.end)));
    let mut merged: Vec<RxCharInterval> = Vec::new();
    for interval in intervals {
        if let Some(last) = merged.last_mut() {
            let interval_start = u32::from(interval.start);
            let last_end = u32::from(last.end);
            if interval_start <= last_end.saturating_add(1) {
                if interval.end > last.end {
                    last.end = interval.end;
                }
                continue;
            }
        }
        merged.push(interval);
    }

    Ok((merged, classes))
}

fn rx_prev_char(ch: char) -> Option<char> {
    char::from_u32(u32::from(ch).checked_sub(1)?)
}

fn rx_next_char(ch: char) -> Option<char> {
    char::from_u32(u32::from(ch).checked_add(1)?)
}

fn append_rx_char_class_char(regex: &mut String, ch: char) {
    if ch == ']' {
        regex.push('\\');
    }
    regex.push(ch);
}

fn append_rx_char_class_boundary(regex: &mut String, ch: char) {
    regex.push(ch);
}

fn compile_rx_char_class(items: &[Value], negated: bool) -> Result<String, LispError> {
    let (mut intervals, classes) = parse_rx_char_class_items(items)?;
    if intervals.is_empty() && classes.is_empty() {
        return Err(LispError::Signal("rx character set cannot be empty".into()));
    }
    if !negated
        && classes.is_empty()
        && intervals.len() == 1
        && intervals[0].start == intervals[0].end
    {
        return Ok(quote_rx_string_literal(&intervals[0].start.to_string()));
    }

    let mut prefix = String::new();
    let mut suffix = String::new();
    let mut emitted_prefix = false;

    let mut index = 0usize;
    while index < intervals.len() {
        let interval = &mut intervals[index];
        if interval.start == ']' {
            prefix.push(']');
            emitted_prefix = true;
            if interval.end == ']' {
                intervals.remove(index);
                continue;
            }
            interval.start = rx_next_char(']').expect("']' has a successor");
        }
        if interval.end == ']' {
            prefix.push(']');
            emitted_prefix = true;
            interval.end = rx_prev_char(']').expect("']' has a predecessor");
        }
        if interval.start == '-' {
            suffix.push('-');
            if interval.end == '-' {
                intervals.remove(index);
                continue;
            }
            interval.start = rx_next_char('-').expect("'-' has a successor");
        }
        if interval.end == '-' {
            suffix.push('-');
            interval.end = rx_prev_char('-').expect("'-' has a predecessor");
        }
        index += 1;
    }

    let mut regex = String::new();
    regex.push('[');
    if negated {
        regex.push('^');
    }
    regex.push_str(&prefix);
    for name in &classes {
        regex.push_str("[:");
        regex.push_str(name);
        regex.push_str(":]");
    }

    let mut emitted_body = emitted_prefix || !classes.is_empty();
    for interval in &intervals {
        if interval.start == '^' && !negated && !emitted_body {
            if interval.end == '^' {
                suffix.push('^');
                continue;
            }
            append_rx_char_class_boundary(
                &mut regex,
                rx_next_char('^').expect("'^' has a successor"),
            );
            regex.push('-');
            append_rx_char_class_boundary(&mut regex, interval.end);
            suffix.push('^');
        } else if interval.start == interval.end {
            append_rx_char_class_char(&mut regex, interval.start);
        } else {
            append_rx_char_class_boundary(&mut regex, interval.start);
            regex.push('-');
            append_rx_char_class_boundary(&mut regex, interval.end);
        }
        emitted_body = true;
    }

    regex.push_str(&suffix);
    regex.push(']');
    Ok(regex)
}

fn compile_rx_form(
    interp: &mut Interpreter,
    env: &Env,
    value: &Value,
) -> Result<String, LispError> {
    match value {
        Value::String(text) => Ok(quote_rx_string_literal(text)),
        Value::StringObject(state) => Ok(quote_rx_string_literal(&state.borrow().text)),
        Value::Integer(codepoint) => {
            let ch = char::from_u32(*codepoint as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid rx character: {codepoint}")))?;
            Ok(quote_rx_string_literal(&ch.to_string()))
        }
        Value::Symbol(symbol) => match symbol.as_str() {
            "bol" => Ok("^".into()),
            "eol" => Ok("$".into()),
            "bos" | "string-start" | "bot" | "buffer-start" => Ok("\\`".into()),
            "eos" | "string-end" | "eot" | "buffer-end" => Ok("\\'".into()),
            "bow" | "eow" => Ok("\\b".into()),
            "digit" => Ok("[0-9]".into()),
            "xdigit" => Ok("[0-9A-Fa-f]".into()),
            "blank" => Ok("[[:blank:]]".into()),
            "space" => Ok("[[:space:]]".into()),
            "nonl" | "not-newline" => Ok(".".into()),
            "symbol-start" => Ok("\\_<".into()),
            "symbol-end" => Ok("\\_>".into()),
            other if rx_char_class_name(other).is_some() => Ok(format!(
                "[[:{}:]]",
                rx_char_class_name(other).unwrap_or_default()
            )),
            other => {
                if let Some(expanded) = expand_rx_definition(interp, other, &[])? {
                    compile_rx_form(interp, env, &expanded)
                } else {
                    Err(LispError::Signal(format!("Unsupported rx atom: {other}")))
                }
            }
        },
        Value::Cons(_, _) => {
            let items = expand_rx_splice_markers(interp, env, &value.to_vec()?)?;
            let head = match items.first() {
                Some(Value::Symbol(head)) => Some(head.as_str()),
                Some(Value::Integer(codepoint)) if *codepoint == ' ' as i64 => Some("?"),
                _ => None,
            };
            let Some(head) = head else {
                return compile_rx_sequence(interp, env, &items);
            };
            match head {
                "group" => Ok(format!(
                    "\\({}\\)",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "group-n" | "submatch-n" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal(
                            "rx `group-n' needs a group number and a form".into(),
                        ));
                    }
                    let number = items[1].as_integer()?;
                    if number <= 0 {
                        return Err(LispError::Signal(
                            "rx `group-n' needs a positive group number".into(),
                        ));
                    }
                    Ok(format!(
                        "\\(?{}:{}\\)",
                        number,
                        compile_rx_sequence(interp, env, &items[2..])?
                    ))
                }
                "+" | "1+" | "one-or-more" => Ok(format!(
                    "\\(?:{}\\)+",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "+?" => Ok(format!(
                    "\\(?:{}\\)+?",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "*" | "0+" | "zero-or-more" => Ok(format!(
                    "\\(?:{}\\)*",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "*?" => Ok(format!(
                    "\\(?:{}\\)*?",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "?" | "zero-or-one" | "opt" | "optional" => Ok(format!(
                    "\\(?:{}\\)?",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "??" => Ok(format!(
                    "\\(?:{}\\)??",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "seq" | ":" => compile_rx_sequence(interp, env, &items[1..]),
                "regexp" => compile_rx_regexp_form(interp, env, &items),
                "literal" => compile_rx_literal_form(interp, env, &items),
                "repeat" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal(
                            "rx `repeat' needs a count and a form".into(),
                        ));
                    }
                    let min = items[1].as_integer()?;
                    if min < 0 {
                        return Err(LispError::Signal("rx repetition count must be >= 0".into()));
                    }

                    let (max, body_start) = match items.get(2) {
                        Some(Value::Integer(max)) => {
                            if *max < min {
                                return Err(LispError::Signal(
                                    "rx repetition max must be >= min".into(),
                                ));
                            }
                            (Some(*max), 3usize)
                        }
                        Some(Value::Nil) => (None, 3usize),
                        _ => (Some(min), 2usize),
                    };
                    if body_start >= items.len() {
                        return Err(LispError::Signal(
                            "rx `repeat' needs a repeated form".into(),
                        ));
                    }

                    let body = compile_rx_sequence(interp, env, &items[body_start..])?;
                    let quantifier = match max {
                        Some(max) if max == min => format!("\\{{{min}\\}}"),
                        Some(max) => format!("\\{{{min},{max}\\}}"),
                        None => format!("\\{{{min},\\}}"),
                    };
                    Ok(format!("\\(?:{body}\\){quantifier}"))
                }
                "or" | "|" if items.len() == 1 => Ok("\\`a\\`".into()),
                "or" | "|" => Ok(format!(
                    "\\(?:{}\\)",
                    items[1..]
                        .iter()
                        .map(|item| compile_rx_form(interp, env, item))
                        .collect::<Result<Vec<_>, _>>()?
                        .join("\\|")
                )),
                "any" | "in" | "char" => compile_rx_char_class(&items[1..], false),
                "syntax" => compile_rx_syntax_form(&items, false),
                "not-syntax" => compile_rx_syntax_form(&items, true),
                "not" => {
                    if items.len() != 2 {
                        return Err(LispError::Signal("rx `not' needs one argument".into()));
                    }
                    match &items[1] {
                        Value::Cons(_, _) => {
                            let charset = items[1].to_vec()?;
                            let Some(Value::Symbol(kind)) = charset.first() else {
                                return Err(LispError::Signal("Unsupported rx `not' form".into()));
                            };
                            if !matches!(kind.as_str(), "any" | "in" | "char") {
                                return Err(LispError::Signal("Unsupported rx `not' form".into()));
                            }
                            compile_rx_char_class(&charset[1..], true)
                        }
                        other => compile_rx_char_class(std::slice::from_ref(other), true),
                    }
                }
                "=" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal("rx `=' needs a count and a form".into()));
                    }
                    let count = items[1].as_integer()?;
                    if count < 0 {
                        return Err(LispError::Signal("rx repetition count must be >= 0".into()));
                    }
                    Ok(format!(
                        "\\(?:{}\\)\\{{{}\\}}",
                        compile_rx_sequence(interp, env, &items[2..])?,
                        count
                    ))
                }
                ">=" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal("rx `>=' needs a count and a form".into()));
                    }
                    let count = items[1].as_integer()?;
                    if count < 0 {
                        return Err(LispError::Signal("rx repetition count must be >= 0".into()));
                    }
                    Ok(format!(
                        "\\(?:{}\\)\\{{{},\\}}",
                        compile_rx_sequence(interp, env, &items[2..])?,
                        count
                    ))
                }
                _ => {
                    if let Some(expanded) = expand_rx_definition(interp, head, &items[1..])? {
                        compile_rx_form(interp, env, &expanded)
                    } else {
                        compile_rx_sequence(interp, env, &items)
                    }
                }
            }
        }
        other => Err(LispError::Signal(format!(
            "Unsupported rx form: {}",
            other.type_name()
        ))),
    }
}

fn rx_syntax_code_from_name(name: &str) -> Option<char> {
    match name {
        "whitespace" | "space" | "white" => Some('-'),
        "punctuation" => Some('.'),
        "word" | "wordchar" => Some('w'),
        "symbol" => Some('_'),
        "open-parenthesis" => Some('('),
        "close-parenthesis" => Some(')'),
        "expression-prefix" => Some('\''),
        "string-quote" => Some('"'),
        "paired-delimiter" => Some('$'),
        "escape" => Some('\\'),
        "character-quote" => Some('/'),
        "comment-start" => Some('<'),
        "comment-end" => Some('>'),
        "string-delimiter" => Some('|'),
        "comment-delimiter" => Some('!'),
        _ if name.len() == 1 => {
            let ch = name.chars().next()?;
            match ch {
                '-' | '.' | 'w' | '_' | '(' | ')' | '\'' | '"' | '$' | '\\' | '/' | '<' | '>'
                | '|' | '!' => Some(ch),
                _ => None,
            }
        }
        _ => None,
    }
}

fn rx_syntax_code_from_value(value: &Value) -> Result<char, LispError> {
    match value {
        Value::Symbol(symbol) => rx_syntax_code_from_name(symbol)
            .ok_or_else(|| LispError::Signal(format!("Unknown rx syntax name `{symbol}`"))),
        Value::Integer(codepoint) => {
            let ch = char::from_u32(*codepoint as u32).ok_or_else(|| {
                LispError::Signal(format!("Invalid rx syntax character: {codepoint}"))
            })?;
            rx_syntax_code_from_name(&ch.to_string())
                .ok_or_else(|| LispError::Signal(format!("Unknown rx syntax name `{ch}`")))
        }
        _ => Err(LispError::Signal(
            "rx `syntax' form takes a syntax name or syntax character".into(),
        )),
    }
}

fn compile_rx_syntax_form(items: &[Value], negated: bool) -> Result<String, LispError> {
    if items.len() != 2 {
        let form = if negated { "not-syntax" } else { "syntax" };
        return Err(LispError::Signal(format!(
            "rx `{form}` form takes exactly one argument"
        )));
    }
    let syntax = rx_syntax_code_from_value(&items[1])?;
    if syntax == 'w' {
        Ok(format!(r"\{}", if negated { 'W' } else { 'w' }))
    } else {
        Ok(format!(r"\{}{}", if negated { 'S' } else { 's' }, syntax))
    }
}

fn expand_rx_definition(
    interp: &Interpreter,
    name: &str,
    args: &[Value],
) -> Result<Option<Value>, LispError> {
    let Some(binding) = interp.get_symbol_property(name, "rx-definition") else {
        return Ok(None);
    };
    let items = binding.to_vec()?;
    match items.as_slice() {
        [definition] if args.is_empty() => Ok(Some(definition.clone())),
        [params, definition] => {
            let params = params.to_vec()?;
            Ok(Some(expand_rx_template(definition, &params, args)?))
        }
        _ => Err(LispError::Signal(format!(
            "Bad `rx' definition of {name}: {binding}"
        ))),
    }
}

fn expand_rx_template(form: &Value, params: &[Value], args: &[Value]) -> Result<Value, LispError> {
    let mut bindings = Vec::new();
    let mut arg_index = 0usize;
    let mut rest = false;

    for param in params {
        let name = param.as_symbol()?.to_string();
        if name == "&rest" {
            rest = true;
            continue;
        }
        let values = if rest {
            args[arg_index..].to_vec()
        } else {
            let value = args.get(arg_index).cloned().unwrap_or(Value::Nil);
            arg_index += 1;
            vec![value]
        };
        bindings.push((name, values, rest));
        if rest {
            break;
        }
    }

    expand_rx_template_value(form, &bindings)
}

fn expand_rx_template_value(
    form: &Value,
    bindings: &[(String, Vec<Value>, bool)],
) -> Result<Value, LispError> {
    match form {
        Value::Symbol(name) => {
            if let Some((_, values, is_rest)) =
                bindings.iter().find(|(binding, _, _)| binding == name)
            {
                if *is_rest {
                    Ok(if values.len() == 1 {
                        values[0].clone()
                    } else {
                        Value::list(values.clone())
                    })
                } else {
                    Ok(values.first().cloned().unwrap_or(Value::Nil))
                }
            } else {
                Ok(form.clone())
            }
        }
        Value::Cons(_, _) => {
            let items = form.to_vec()?;
            let mut expanded = Vec::new();
            for item in items {
                if let Value::Symbol(name) = &item
                    && let Some((_, values, true)) =
                        bindings.iter().find(|(binding, _, _)| binding == name)
                {
                    expanded.extend(values.clone());
                    continue;
                }
                expanded.push(expand_rx_template_value(&item, bindings)?);
            }
            Ok(Value::list(expanded))
        }
        _ => Ok(form.clone()),
    }
}
