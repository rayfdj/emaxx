use super::*;

const REGEX_WORD_CLASS: &str = r"[\p{Alphabetic}\p{Number}_\x{2620}]";
const REGEX_NON_WORD_CLASS: &str = r"[^\p{Alphabetic}\p{Number}_\x{2620}]";
const REGEX_SYMBOL_CLASS: &str = r"[\p{Alphabetic}\p{Number}_\-\x{2620}]";
const REGEX_NON_SYMBOL_CLASS: &str = r"[^\p{Alphabetic}\p{Number}_\-\x{2620}]";
const REGEX_WHITESPACE_CLASS: &str = r"[\p{White_Space}]";
const REGEX_NON_WHITESPACE_CLASS: &str = r"[^\p{White_Space}]";

#[derive(Clone)]
enum RegexClassAtom {
    Char(char),
    Posix(String),
}

pub(super) fn translate_elisp_regex(pattern: &str) -> String {
    translate_elisp_regex_with_point(pattern, "", r"\A")
}

fn contains_point_assertion(pattern: &str) -> bool {
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' && chars.next() == Some('=') {
            return true;
        }
    }
    false
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegexGroupPrefix {
    Capturing(Option<usize>),
    NonCapturing,
}

fn consume_regex_group_prefix(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Result<RegexGroupPrefix, LispError> {
    if chars.peek() != Some(&'?') {
        return Ok(RegexGroupPrefix::Capturing(None));
    }

    let mut preview = chars.clone();
    preview.next();
    if preview.peek() == Some(&':') {
        chars.next();
        chars.next();
        return Ok(RegexGroupPrefix::NonCapturing);
    }

    let mut digits = String::new();
    while let Some(ch) = preview.peek().copied() {
        if !ch.is_ascii_digit() {
            break;
        }
        digits.push(ch);
        preview.next();
    }
    if digits.is_empty() || preview.peek() != Some(&':') {
        return Ok(RegexGroupPrefix::Capturing(None));
    }

    let explicit = digits
        .parse::<usize>()
        .map_err(|_| invalid_regexp_error("Invalid explicit regexp group number"))?;
    if explicit == 0 {
        return Err(invalid_regexp_error("Invalid explicit regexp group number"));
    }

    chars.next();
    for _ in 0..digits.len() {
        chars.next();
    }
    chars.next();
    Ok(RegexGroupPrefix::Capturing(Some(explicit)))
}

fn translate_elisp_regex_with_point(
    pattern: &str,
    point_assertion: &str,
    absolute_start_assertion: &str,
) -> String {
    let mut translated = String::new();
    let mut chars = pattern.chars().peekable();
    let mut at_branch_start = true;
    let mut can_repeat_previous = false;
    let mut last_was_quantifier = false;
    while let Some(ch) = chars.next() {
        if ch == '[' {
            translated.push_str(&translate_bracket_expression(&mut chars));
            at_branch_start = false;
            can_repeat_previous = true;
            last_was_quantifier = false;
            continue;
        }
        if ch == '\\' {
            match chars.next() {
                Some('`') => {
                    translated.push_str(absolute_start_assertion);
                    can_repeat_previous =
                        literalize_postfix_after_absolute_anchor(&mut translated, &mut chars);
                    at_branch_start = false;
                    last_was_quantifier = false;
                }
                Some('\'') => {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, r"\z"));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('(') => {
                    match consume_regex_group_prefix(&mut chars) {
                        Ok(RegexGroupPrefix::NonCapturing) => {
                            translated.push_str("(?:");
                            at_branch_start = true;
                            can_repeat_previous = false;
                            last_was_quantifier = false;
                            continue;
                        }
                        Ok(RegexGroupPrefix::Capturing(_)) => {}
                        Err(_) => return "(".into(),
                    }
                    translated.push('(');
                    at_branch_start = true;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some(')') => {
                    translated.push(')');
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('|') => {
                    translated.push('|');
                    at_branch_start = true;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('{') => {
                    translated.push('{');
                    // Emacs `\{,N\}' means `{0,N}'; the Rust regex parser
                    // rejects an empty lower bound.
                    if chars.peek() == Some(&',') {
                        translated.push('0');
                    }
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('}') => {
                    translated.push('}');
                    if chars.peek() == Some(&'?') {
                        chars.next();
                        if lazy_interval_has_following_context(&chars) {
                            translated.push('?');
                        }
                    } else if let Some(next) = chars.peek().copied()
                        && matches!(next, '*' | '+')
                    {
                        translated.push('\\');
                        translated.push(next);
                        chars.next();
                    }
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = true;
                }
                Some('s') => {
                    translated.push_str(regex_syntax_class(&mut chars, false));
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('S') => {
                    translated.push_str(regex_syntax_class(&mut chars, true));
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('w') => {
                    translated.push_str(REGEX_WORD_CLASS);
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('W') => {
                    translated.push_str(REGEX_NON_WORD_CLASS);
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('b') => {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, r"\b"));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('B') => {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, r"\B"));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('<') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        r"(?<![\p{Alphabetic}\p{Number}_\x{2620}])(?=[\p{Alphabetic}\p{Number}_\x{2620}])",
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('>') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        r"(?<=[\p{Alphabetic}\p{Number}_\x{2620}])(?![\p{Alphabetic}\p{Number}_\x{2620}])",
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('_') => match chars.next() {
                    Some('<') => {
                        translated.push_str(&translate_zero_width_assertion(
                            &mut chars,
                            &format!("(?<!{})(?={})", REGEX_SYMBOL_CLASS, REGEX_SYMBOL_CLASS),
                        ));
                        at_branch_start = false;
                        can_repeat_previous = false;
                        last_was_quantifier = false;
                    }
                    Some('>') => {
                        translated.push_str(&translate_zero_width_assertion(
                            &mut chars,
                            &format!("(?<={})(?!{})", REGEX_SYMBOL_CLASS, REGEX_SYMBOL_CLASS),
                        ));
                        at_branch_start = false;
                        can_repeat_previous = false;
                        last_was_quantifier = false;
                    }
                    Some(other) => {
                        translated.push_str(r"\_");
                        translated.push(other);
                        at_branch_start = false;
                        can_repeat_previous = true;
                        last_was_quantifier = false;
                    }
                    None => {
                        translated.push_str(r"\_");
                        at_branch_start = false;
                        can_repeat_previous = true;
                        last_was_quantifier = false;
                    }
                },
                Some('=') => {
                    translated
                        .push_str(&translate_zero_width_assertion(&mut chars, point_assertion));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some(other) => {
                    if other.is_ascii_alphabetic() {
                        translated.push(other);
                    } else {
                        translated.push('\\');
                        translated.push(other);
                    }
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                None => {
                    translated.push('\\');
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
            }
            continue;
        }

        match ch {
            '^' => {
                if at_branch_start {
                    translated.push('^');
                    can_repeat_previous =
                        literalize_postfix_after_absolute_anchor(&mut translated, &mut chars);
                } else {
                    translated.push_str(r"\^");
                    can_repeat_previous = true;
                }
                at_branch_start = false;
                last_was_quantifier = false;
            }
            '$' => {
                if is_dollar_anchor_position(&chars) {
                    translated.push_str(&translate_zero_width_assertion(&mut chars, "$"));
                    can_repeat_previous = false;
                } else {
                    translated.push_str(r"\$");
                    can_repeat_previous = true;
                }
                at_branch_start = false;
                last_was_quantifier = false;
            }
            '*' | '+' | '?' => {
                if can_repeat_previous {
                    if last_was_quantifier {
                        match ch {
                            '?' => translated.push('?'),
                            '*' => {}
                            '+' => {
                                translated.push('\\');
                                translated.push('+');
                                can_repeat_previous = true;
                                last_was_quantifier = false;
                            }
                            _ => {}
                        }
                    } else {
                        translated.push(ch);
                        last_was_quantifier = true;
                    }
                } else {
                    translated.push('\\');
                    translated.push(ch);
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                at_branch_start = false;
            }
            '(' | ')' | '{' | '}' | '|' => {
                translated.push('\\');
                translated.push(ch);
                at_branch_start = false;
                can_repeat_previous = true;
                last_was_quantifier = false;
            }
            _ => {
                translated.push(ch);
                at_branch_start = false;
                can_repeat_previous = true;
                last_was_quantifier = false;
            }
        }
    }
    translated
}

fn is_dollar_anchor_position(chars: &std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let mut preview = chars.clone();
    match preview.next() {
        None => true,
        Some('\\') => matches!(preview.next(), Some(')') | Some('|')),
        _ => false,
    }
}

fn literalize_postfix_after_absolute_anchor(
    translated: &mut String,
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> bool {
    if let Some(next) = chars.peek().copied()
        && matches!(next, '*' | '+' | '?')
    {
        translated.push('\\');
        translated.push(next);
        chars.next();
        return true;
    }
    false
}

fn lazy_interval_has_following_context(chars: &std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let mut preview = chars.clone();
    match preview.next() {
        None => false,
        Some('\\') => !matches!(preview.next(), Some(')') | Some('|')),
        _ => true,
    }
}

fn translate_zero_width_assertion(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    assertion: &str,
) -> String {
    match chars.peek().copied() {
        Some('*') => {
            chars.next();
            if chars.peek() == Some(&'?') {
                chars.next();
            }
            if assertion.is_empty() {
                String::new()
            } else {
                format!("(?:{assertion}|)")
            }
        }
        Some('+') => {
            chars.next();
            if chars.peek() == Some(&'?') {
                chars.next();
            }
            if assertion.is_empty() {
                String::new()
            } else {
                assertion.to_string()
            }
        }
        Some('?') => {
            chars.next();
            if assertion.is_empty() {
                String::new()
            } else {
                format!("(?:{assertion}|)")
            }
        }
        _ => {
            if assertion.is_empty() {
                "(?:)".into()
            } else {
                format!("(?:{assertion})")
            }
        }
    }
}

// Rewrite syntax atoms whose meaning cannot be represented by a fixed Rust
// regex character class.  Comment delimiters come from explicit table
// entries; symbol constituents must distinguish GNU's `\s_' from `\sw' and
// include mode-specific ASCII entries such as `:' in Emacs Lisp mode.
fn resolve_table_syntax_classes(interp: &Interpreter, pattern: &str) -> String {
    if !pattern.contains("\\s<")
        && !pattern.contains("\\S<")
        && !pattern.contains("\\s>")
        && !pattern.contains("\\S>")
        && !pattern.contains("\\s_")
        && !pattern.contains("\\S_")
    {
        return pattern.to_string();
    }
    let class_expansion = |class_char: char, negated: bool| -> String {
        let chars = if class_char == '_' {
            super::syntax::syntax_class_ascii_chars(interp, class_char)
        } else {
            super::syntax::syntax_class_explicit_chars(interp, class_char)
        };
        if chars.is_empty() {
            return if negated {
                // Anything (GNU: no character has the class).
                "\\(?:.\\|\n\\)".to_string()
            } else {
                // Nothing can match.
                "\\`X\\`".to_string()
            };
        }
        let mut set = String::new();
        if negated {
            set.push('^');
        }
        // This bracket expression is an internal bridge to the delegate,
        // not an Emacs regexp returned to Lisp.  Keep `-' last so our Emacs
        // bracket parser treats it as a literal member.  Escaping it in its
        // ASCII sort position can form a spurious `\\-/' range and silently
        // drop both symbol constituents.
        let contains_hyphen = chars.contains(&'-');
        for ch in chars.into_iter().filter(|ch| *ch != '-') {
            if matches!(ch, ']' | '^' | '\\') {
                set.push('\\');
            }
            set.push(ch);
        }
        if contains_hyphen {
            set.push('-');
        }
        format!("[{set}]")
    };
    let mut result = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            result.push(ch);
            continue;
        }
        match chars.peek() {
            Some('s') | Some('S') => {
                let escape = *chars.peek().expect("peeked");
                let mut lookahead = chars.clone();
                lookahead.next();
                match lookahead.peek() {
                    Some('<') | Some('>') | Some('_') => {
                        chars.next();
                        let class_char = chars.next().expect("peeked class");
                        result.push_str(&class_expansion(class_char, escape == 'S'));
                    }
                    _ => {
                        result.push(ch);
                    }
                }
            }
            Some(_) => {
                result.push(ch);
                if let Some(next) = chars.next() {
                    result.push(next);
                }
            }
            None => result.push(ch),
        }
    }
    result
}

fn regex_syntax_class(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    negated: bool,
) -> &'static str {
    match chars.next() {
        Some('w') => {
            if negated {
                REGEX_NON_WORD_CLASS
            } else {
                REGEX_WORD_CLASS
            }
        }
        Some(' ' | '-') => {
            if negated {
                REGEX_NON_WHITESPACE_CLASS
            } else {
                REGEX_WHITESPACE_CLASS
            }
        }
        Some('_') => {
            if negated {
                REGEX_NON_SYMBOL_CLASS
            } else {
                REGEX_SYMBOL_CLASS
            }
        }
        Some('(') => {
            if negated {
                r"[^(\[\{]"
            } else {
                r"[\(\[\{]"
            }
        }
        Some(')') => {
            if negated {
                r"[^)\]\}]"
            } else {
                r"[\)\]\}]"
            }
        }
        Some('.') => {
            if negated {
                r"[\p{Alphabetic}\p{Number}\p{White_Space}_]"
            } else {
                r"[^\p{Alphabetic}\p{Number}\p{White_Space}_]"
            }
        }
        Some('"') => {
            if negated {
                r#"[^"]"#
            } else {
                r#"["]"#
            }
        }
        Some('<') => {
            if negated {
                r#"[^/#]"#
            } else {
                r#"[/#]"#
            }
        }
        Some('>') => {
            if negated {
                r#"[^\n]"#
            } else {
                r#"[\n]"#
            }
        }
        Some('\\') => {
            if negated {
                r#"[^\\]"#
            } else {
                r#"[\\]"#
            }
        }
        Some(_) | None => {
            if negated {
                REGEX_NON_WORD_CLASS
            } else {
                REGEX_WORD_CLASS
            }
        }
    }
}

fn regex_posix_class_fragment(name: &str) -> Option<&'static str> {
    match name {
        "alnum" => Some(r"\p{Alphabetic}\p{Number}"),
        "alpha" => Some(r"\p{Alphabetic}"),
        "ascii" => Some(r"\x00-\x7F"),
        "blank" => Some(r"\t\p{Zs}"),
        "cntrl" => Some(r"\x00-\x1F"),
        "digit" => Some("0-9"),
        "graph" => Some(r"\p{Alphabetic}\p{Number}\p{Punctuation}\p{Symbol}\p{Mark}"),
        "lower" => Some(r"\p{Lowercase}"),
        "multibyte" => Some(r"\x{0080}-\x{D7FF}\x{E100}-\x{10FFFF}"),
        "nonascii" => Some(r"\x{0080}-\x{10FFFF}"),
        "print" => Some(r"\p{Alphabetic}\p{Number}\p{Punctuation}\p{Symbol}\p{Mark}\p{Zs}"),
        "punct" => Some(r"\p{Punctuation}"),
        "space" => Some(r"\p{White_Space}"),
        "unibyte" => Some(r"\x00-\x7F\x{E080}-\x{E0FF}"),
        "upper" => Some(r"\p{Uppercase}"),
        "word" => Some(r"\p{Alphabetic}\p{Number}_\x{2620}"),
        "xdigit" => Some("0-9A-Fa-f"),
        _ => None,
    }
}

fn translate_bracket_expression(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut translated = String::from("[");
    let mut saw_atom = false;
    let mut emitted_atom = false;
    let mut negated = false;
    if chars.peek() == Some(&'^') {
        translated.push('^');
        negated = true;
        chars.next();
    }

    while let Some(ch) = chars.peek().copied() {
        if ch == ']' && saw_atom {
            chars.next();
            if !emitted_atom {
                return if negated {
                    r"[\s\S]".into()
                } else {
                    "(?!)".into()
                };
            }
            translated.push(']');
            return translated;
        }
        let atom_is_first = !saw_atom;
        if atom_is_first {
            let mut preview = chars.clone();
            if preview.next() == Some('-')
                && preview.next() == Some('-')
                && preview.peek().copied() != Some(']')
                && let Some(RegexClassAtom::Char(end)) = consume_regex_class_atom(&mut preview)
                && let Some(range) = bracket_range_fragment('-', end)
            {
                translated.push_str(&range);
                *chars = preview;
                saw_atom = true;
                emitted_atom = true;
                continue;
            }
        }
        let Some(atom) = consume_regex_class_atom(chars) else {
            break;
        };
        let mut preview = chars.clone();
        if preview.next() == Some('-')
            && preview.peek().copied() != Some(']')
            && !(atom_is_first && matches!(atom, RegexClassAtom::Char('-' | ']')))
            && let Some(end_atom) = consume_regex_class_atom(&mut preview)
            && let (RegexClassAtom::Char(start), RegexClassAtom::Char(end)) = (&atom, &end_atom)
        {
            if let Some(range) = bracket_range_fragment(*start, *end) {
                translated.push_str(&range);
                emitted_atom = true;
            } else if is_empty_unicode_raw_range(*start, *end) {
                if !negated {
                    return "(?!)".into();
                }
            } else {
                if !negated && start > end {
                    // Emacs treats reversed ranges as empty.  Keep parsing so
                    // other class members such as the `a' in `[az-c]' remain.
                } else if negated && start > end {
                    // The negation of an empty range excludes nothing.
                } else {
                    return "[".into();
                }
            }
            *chars = preview;
            saw_atom = true;
            continue;
        }
        if let Some(fragment) = regex_class_atom_fragment(&atom) {
            translated.push_str(&fragment);
            emitted_atom = true;
        } else {
            return "[".into();
        }
        saw_atom = true;
    }

    "[".into()
}

fn consume_regex_class_atom(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<RegexClassAtom> {
    match chars.next()? {
        '[' if chars.peek() == Some(&':') => {
            chars.next();
            let mut name = String::new();
            while let Some(ch) = chars.next() {
                if ch == ':' && chars.peek() == Some(&']') {
                    chars.next();
                    return Some(RegexClassAtom::Posix(name));
                }
                name.push(ch);
            }
            Some(RegexClassAtom::Char('['))
        }
        '\\' => Some(RegexClassAtom::Char('\\')),
        ch => Some(RegexClassAtom::Char(ch)),
    }
}

fn regex_class_atom_fragment(atom: &RegexClassAtom) -> Option<String> {
    match atom {
        RegexClassAtom::Char(ch) => Some(bracket_char_fragment(*ch)),
        RegexClassAtom::Posix(name) => regex_posix_class_fragment(name)
            .map(str::to_string)
            .or(None),
    }
}

fn bracket_char_fragment(ch: char) -> String {
    match ch {
        '[' => r"\[".into(),
        '\\' => r"\\".into(),
        '-' => r"\-".into(),
        ']' => r"\]".into(),
        '^' => r"\^".into(),
        _ => ch.to_string(),
    }
}

fn bracket_range_endpoint_fragment(ch: char) -> String {
    match ch {
        '\\' => r"\\".into(),
        ']' => r"\]".into(),
        _ => ch.to_string(),
    }
}

fn bracket_range_fragment(start: char, end: char) -> Option<String> {
    if (start as u32) <= 0x7F && (end as u32) <= 0x7F && start <= end {
        let mut expanded = String::new();
        for code in (start as u32)..=(end as u32) {
            expanded.push_str(&bracket_char_fragment(char::from_u32(code)?));
        }
        return Some(expanded);
    }
    match (
        raw_byte_from_regex_char(start),
        raw_byte_from_regex_char(end),
    ) {
        (Some(start), Some(end)) if start <= end => Some(format!(
            "{}-{}",
            bracket_range_endpoint_fragment(raw_byte_regex_char(start)),
            bracket_range_endpoint_fragment(raw_byte_regex_char(end))
        )),
        (None, Some(end)) if (start as u32) <= 0x7F => Some(format!(
            "{}-\\x7F{}-{}",
            bracket_range_endpoint_fragment(start),
            bracket_range_endpoint_fragment(raw_byte_regex_char(0x80)),
            bracket_range_endpoint_fragment(raw_byte_regex_char(end))
        )),
        (None, None) if start <= end => Some(format!(
            "{}-{}",
            bracket_range_endpoint_fragment(start),
            bracket_range_endpoint_fragment(end)
        )),
        _ => None,
    }
}

fn is_empty_unicode_raw_range(start: char, end: char) -> bool {
    match (
        raw_byte_from_regex_char(start),
        raw_byte_from_regex_char(end),
    ) {
        (None, Some(_)) => (start as u32) > 0x7F,
        (Some(_), None) => (end as u32) > 0x7F,
        _ => false,
    }
}

pub(super) fn regexp_quote_elisp(pattern: &str) -> String {
    // GNU search.c Fregexp_quote: exactly [ * . \ ? + ^ $ get a
    // backslash; ( ) { } | ] are literal in elisp regexp syntax.
    let mut quoted = String::new();
    for ch in pattern.chars() {
        match ch {
            '[' | '*' | '.' | '\\' | '?' | '+' | '^' | '$' => {
                quoted.push('\\');
                quoted.push(ch);
            }
            _ => quoted.push(ch),
        }
    }
    quoted
}

fn invalid_regexp_error(message: impl Into<String>) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("invalid-regexp".into()),
        Value::String(message.into()),
    ]))
}

pub(super) fn non_subregexp_context_error(error: &LispError) -> bool {
    let rendered = error.to_string();
    rendered.contains("Unmatched [ or [^")
        || rendered.contains("Unmatched \\{")
        || rendered.contains("Trailing backslash")
}

pub(super) fn validate_elisp_regex(pattern: &str) -> Result<(), LispError> {
    let mut chars = pattern.chars().peekable();
    let mut max_group = 0usize;
    let mut max_closed_group = 0usize;
    let mut open_groups: Vec<Option<usize>> = Vec::new();
    let mut in_class = false;
    while let Some(ch) = chars.next() {
        if in_class {
            match ch {
                '[' if chars.peek() == Some(&':') => {
                    chars.next();
                    let mut name = String::new();
                    while let Some(next) = chars.next() {
                        if next == ':' && chars.peek() == Some(&']') {
                            chars.next();
                            if regex_posix_class_fragment(&name).is_none() {
                                return Err(invalid_regexp_error("Invalid character class"));
                            }
                            break;
                        }
                        name.push(next);
                    }
                }
                ']' => in_class = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '[' => in_class = true,
            '\\' => match chars.next() {
                Some('(') => match consume_regex_group_prefix(&mut chars)? {
                    RegexGroupPrefix::NonCapturing => open_groups.push(None),
                    RegexGroupPrefix::Capturing(explicit) => {
                        let group = explicit.unwrap_or(max_group + 1);
                        max_group = max_group.max(group);
                        open_groups.push(Some(group));
                    }
                },
                Some(')') => {
                    let Some(group) = open_groups.pop() else {
                        return Err(invalid_regexp_error("Unmatched )"));
                    };
                    if let Some(group) = group {
                        max_closed_group = max_closed_group.max(group);
                    }
                }
                Some(digit @ '1'..='9') => {
                    let backref = digit.to_digit(10).unwrap_or(0) as usize;
                    if backref > max_closed_group {
                        return Err(invalid_regexp_error("Invalid back reference"));
                    }
                }
                Some('{') => {
                    let mut preview = chars.clone();
                    let mut found_close = false;
                    while let Some(next) = preview.next() {
                        if next == '\\' && preview.next() == Some('}') {
                            found_close = true;
                            break;
                        }
                    }
                    if !found_close {
                        return Err(invalid_regexp_error("Unmatched \\{"));
                    }
                }
                Some(_) => {}
                None => return Err(invalid_regexp_error("Trailing backslash")),
            },
            _ => {}
        }
    }
    if in_class {
        return Err(invalid_regexp_error("Unmatched [ or [^"));
    }
    if !open_groups.is_empty() {
        return Err(invalid_regexp_error("Unmatched ("));
    }
    Ok(())
}

fn enforce_elisp_repeat_limit(pattern: &str) -> Result<(), LispError> {
    static REPEAT_PATTERN: OnceLock<Regex> = OnceLock::new();
    let regex = REPEAT_PATTERN.get_or_init(|| {
        Regex::new(r"\\\{([0-9]+)(?:,([0-9]*))?\\\}").expect("repeat limit regex is valid")
    });
    for captures in regex.captures_iter(pattern) {
        let lower = captures
            .get(1)
            .and_then(|value| value.as_str().parse::<usize>().ok())
            .unwrap_or(0);
        let upper = captures.get(2).and_then(|value| {
            let raw = value.as_str();
            if raw.is_empty() {
                None
            } else {
                raw.parse::<usize>().ok()
            }
        });
        if lower > 65_535 || upper.is_some_and(|value| value > 65_535) {
            return Err(invalid_regexp_error("Repeat count too large"));
        }
    }
    Ok(())
}

fn elisp_capture_mapping(pattern: &str) -> Result<Vec<usize>, LispError> {
    let mut chars = pattern.chars().peekable();
    let mut max_group = 0usize;
    let mut mapping = Vec::new();
    let mut in_class = false;
    while let Some(ch) = chars.next() {
        if in_class {
            match ch {
                '[' if chars.peek() == Some(&':') => {
                    chars.next();
                    while let Some(next) = chars.next() {
                        if next == ':' && chars.peek() == Some(&']') {
                            chars.next();
                            break;
                        }
                    }
                }
                ']' => in_class = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '[' => in_class = true,
            '\\' if chars.next() == Some('(') => {
                if let RegexGroupPrefix::Capturing(explicit) =
                    consume_regex_group_prefix(&mut chars)?
                {
                    let group = explicit.unwrap_or(max_group + 1);
                    max_group = max_group.max(group);
                    mapping.push(group);
                }
            }
            _ => {}
        }
    }
    Ok(mapping)
}

#[derive(Clone)]
pub(super) struct CompiledElispRegex {
    regex: FancyRegex,
    capture_mapping: Vec<usize>,
}

impl CompiledElispRegex {
    pub(super) fn captures<'h>(
        &self,
        haystack: &'h str,
    ) -> Result<Option<fancy_regex::Captures<'h>>, fancy_regex::Error> {
        self.regex.captures(haystack)
    }

    pub(super) fn captures_from_pos<'h>(
        &self,
        haystack: &'h str,
        start: usize,
    ) -> Result<Option<fancy_regex::Captures<'h>>, fancy_regex::Error> {
        self.regex.captures_from_pos(haystack, start)
    }

    pub(super) fn is_match(&self, haystack: &str) -> Result<bool, fancy_regex::Error> {
        self.regex.is_match(haystack)
    }

    pub(super) fn capture_mapping(&self) -> &[usize] {
        &self.capture_mapping
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct CompiledElispRegexKey {
    pattern: String,
    point_assertion: String,
    at_absolute_start: bool,
    case_fold: bool,
}

const COMPILED_ELISP_REGEX_CACHE_LIMIT: usize = 256;

#[derive(Default)]
struct CompiledElispRegexCache {
    entries: std::collections::HashMap<CompiledElispRegexKey, (CompiledElispRegex, u64)>,
    use_counter: u64,
}

impl CompiledElispRegexCache {
    fn get(&mut self, key: &CompiledElispRegexKey) -> Option<CompiledElispRegex> {
        self.use_counter = self.use_counter.wrapping_add(1);
        let (compiled, last_used) = self.entries.get_mut(key)?;
        *last_used = self.use_counter;
        Some(compiled.clone())
    }

    fn insert(&mut self, key: CompiledElispRegexKey, compiled: CompiledElispRegex) {
        self.use_counter = self.use_counter.wrapping_add(1);
        if self.entries.len() >= COMPILED_ELISP_REGEX_CACHE_LIMIT
            && !self.entries.contains_key(&key)
            && let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(key, _)| key.clone())
        {
            self.entries.remove(&victim);
        }
        self.entries.insert(key, (compiled, self.use_counter));
    }
}

thread_local! {
    static COMPILED_ELISP_REGEX_CACHE: RefCell<CompiledElispRegexCache> =
        RefCell::new(CompiledElispRegexCache::default());
}

pub(super) fn compile_elisp_regex(
    interp: &Interpreter,
    pattern: &StringLike,
    env: &Env,
    point_assertion: &str,
    at_absolute_start: bool,
) -> Result<CompiledElispRegex, LispError> {
    // GNU resolves syntax atoms against the current buffer's syntax table.
    // The rewritten pattern doubles as the cache key, so changing tables
    // cannot reuse a stale class expansion.
    let pattern_text = resolve_table_syntax_classes(interp, &pattern.text);
    let case_fold = interp
        .lookup_var("case-fold-search", env)
        .is_some_and(|value| value.is_truthy());
    let key = CompiledElispRegexKey {
        pattern: pattern_text.clone(),
        point_assertion: point_assertion.to_string(),
        at_absolute_start,
        case_fold,
    };
    if let Some(compiled) = COMPILED_ELISP_REGEX_CACHE.with(|cache| cache.borrow_mut().get(&key)) {
        return Ok(compiled);
    }

    validate_elisp_regex(&pattern.text)?;
    enforce_elisp_repeat_limit(&pattern.text)?;
    let translated = translate_elisp_regex_with_point(
        &pattern_text,
        point_assertion,
        if at_absolute_start { r"\A" } else { r"(?!)" },
    );
    let rendered = if case_fold {
        format!("(?mi:{translated})")
    } else {
        format!("(?m:{translated})")
    };
    let compiled = CompiledElispRegex {
        // Bounded repetitions over Unicode classes (cc-mode uses
        // `\{,1000\}' on symbol-char classes) overflow the delegate's
        // default 10MB compiled-program budget; GNU regexps have no such
        // limit, so give the delegate more room.
        regex: fancy_regex::RegexBuilder::new(&rendered)
            .delegate_size_limit(512 * 1024 * 1024)
            .build()
            .map_err(|error| invalid_regexp_error(error.to_string()))?,
        capture_mapping: elisp_capture_mapping(&pattern.text)?,
    };
    COMPILED_ELISP_REGEX_CACHE.with(|cache| cache.borrow_mut().insert(key, compiled.clone()));
    Ok(compiled)
}

fn regex_pattern_with_search_spaces(
    interp: &Interpreter,
    pattern: &StringLike,
    env: &Env,
) -> StringLike {
    let Some(search_spaces_regexp) = interp
        .lookup_var("search-spaces-regexp", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .filter(|text| !text.is_empty())
    else {
        return pattern.clone();
    };

    StringLike {
        text: expand_search_spaces_regexp(&pattern.text, &search_spaces_regexp),
        props: pattern.props.clone(),
        multibyte: pattern.multibyte,
    }
}

fn expand_search_spaces_regexp(pattern: &str, replacement: &str) -> String {
    let mut expanded = String::new();
    let mut chars = pattern.chars().peekable();
    let mut in_bracket = false;

    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                expanded.push('\\');
                if let Some(next) = chars.next() {
                    expanded.push(next);
                }
            }
            '[' if !in_bracket => {
                in_bracket = true;
                expanded.push('[');
            }
            ']' if in_bracket => {
                in_bracket = false;
                expanded.push(']');
            }
            ' ' if !in_bracket => {
                expanded.push_str(r"\(?:");
                expanded.push_str(replacement);
                expanded.push_str(r"\)");
                while chars.peek() == Some(&' ') {
                    chars.next();
                }
            }
            _ => expanded.push(ch),
        }
    }

    expanded
}

pub(super) fn match_data_from_captures(
    start_pos: usize,
    haystack: &str,
    captures: &fancy_regex::Captures<'_>,
    capture_mapping: &[usize],
) -> Vec<Option<(usize, usize)>> {
    let mut match_data = vec![None; capture_mapping.iter().copied().max().unwrap_or(0) + 1];
    for index in 0..captures.len() {
        let Some(matched) = captures.get(index) else {
            continue;
        };
        let start = start_pos + haystack[..matched.start()].chars().count();
        let end = start_pos + haystack[..matched.end()].chars().count();
        let target_index = if index == 0 {
            0
        } else {
            capture_mapping.get(index - 1).copied().unwrap_or(index)
        };
        if match_data.len() <= target_index {
            match_data.resize(target_index + 1, None);
        }
        match_data[target_index] = Some((start, end));
    }
    match_data
}

pub(super) fn set_match_data(
    interp: &mut Interpreter,
    start_pos: usize,
    haystack: &str,
    captures: &fancy_regex::Captures<'_>,
    capture_mapping: &[usize],
    source_buffer_id: Option<u64>,
) {
    interp.last_match_data = Some(match_data_from_captures(
        start_pos,
        haystack,
        captures,
        capture_mapping,
    ));
    interp.last_match_data_buffer_id = source_buffer_id;
}

pub(super) fn string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
    update_match_data: bool,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            if update_match_data {
                "string-match".into()
            } else {
                "string-match-p".into()
            },
            args.len(),
        ));
    }
    let pattern = string_like(&args[0])
        .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
    let haystack = string_like(&args[1])
        .ok_or_else(|| LispError::TypeError("string".into(), args[1].type_name()))?;
    let haystack_len = haystack.text.chars().count() as i64;
    let start = normalize_string_index(args.get(2), 0, haystack_len)? as usize;
    let tail: String = haystack.text.chars().skip(start).collect();
    let regex = compile_elisp_regex(interp, &pattern, env, "", start == 0)?;
    let captures = regex
        .captures(&tail)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if let Some(captures) = captures
        && let Some(matched) = captures.get(0)
    {
        let match_start = start + tail[..matched.start()].chars().count();
        if update_match_data {
            set_match_data(
                interp,
                start,
                &tail,
                &captures,
                regex.capture_mapping(),
                None,
            );
        }
        Ok(Value::Integer(match_start as i64))
    } else {
        Ok(Value::Nil)
    }
}

fn trim_regexp_pattern(regexp: Option<&Value>, anchored_left: bool) -> Result<String, LispError> {
    let default_pattern = "[ \t\n\r]+";
    let pattern = regexp
        .filter(|value| !value.is_nil())
        .map(string_text)
        .transpose()?
        .unwrap_or_else(|| default_pattern.to_string());
    Ok(if anchored_left {
        format!(r"\`\(?:{pattern}\)")
    } else {
        format!(r"\(?:{pattern}\)\'")
    })
}

pub(super) fn string_trim_left_value(
    interp: &mut Interpreter,
    value: &Value,
    regexp: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let text = string_text(value)?;
    let pattern = StringLike {
        text: trim_regexp_pattern(regexp, true)?,
        props: Vec::new(),
        multibyte: false,
    };
    let regex = compile_elisp_regex(interp, &pattern, env, "", true)?;
    let captures = regex
        .captures(&text)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let Some(captures) = captures else {
        return Ok(Value::String(text));
    };
    let Some(matched) = captures.get(0) else {
        return Ok(Value::String(text));
    };
    let start = text[..matched.end()].chars().count();
    Ok(Value::String(slice_string_chars(
        &text,
        start,
        text.chars().count(),
    )))
}

pub(super) fn string_trim_right_value(
    interp: &mut Interpreter,
    value: &Value,
    regexp: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let text = string_text(value)?;
    let pattern = StringLike {
        text: trim_regexp_pattern(regexp, false)?,
        props: Vec::new(),
        multibyte: false,
    };
    let regex = compile_elisp_regex(interp, &pattern, env, "", true)?;
    let captures = regex
        .captures(&text)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let Some(captures) = captures else {
        return Ok(Value::String(text));
    };
    let Some(matched) = captures.get(0) else {
        return Ok(Value::String(text));
    };
    let end = text[..matched.start()].chars().count();
    Ok(Value::String(slice_string_chars(&text, 0, end)))
}

pub(super) fn isearch_no_upper_case_p(text: &str, regexp_flag: bool) -> bool {
    let mut quote_flag = false;
    for ch in text.chars() {
        if regexp_flag && ch == '\\' {
            quote_flag = !quote_flag;
            continue;
        }
        if !quote_flag && ch.is_uppercase() {
            return false;
        }
        quote_flag = false;
    }
    !(regexp_flag && (text.contains("[:upper:]") || text.contains("[:lower:]")))
}

pub(super) fn split_string_impl(
    interp: &Interpreter,
    string: &Value,
    separator: Option<&Value>,
    omit_nulls: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let source = string_like(string)
        .ok_or_else(|| LispError::TypeError("string".into(), string.type_name()))?;
    let text = source.text.clone();
    let props = source.props.clone();
    let multibyte = source.multibyte;
    let part_value = |text: String, start: usize, end: usize| {
        let sliced_props = slice_string_props(&props, start, end);
        if sliced_props.is_empty() {
            Value::String(text)
        } else {
            string_like_value_with_multibyte(text, sliced_props, multibyte)
        }
    };
    let byte_to_char = |byte: usize| text[..byte].chars().count();
    let separator = separator
        .filter(|value| !value.is_nil())
        .map(|value| {
            string_like(value)
                .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))
        })
        .transpose()?;
    let omit_nulls = omit_nulls.is_some_and(Value::is_truthy);
    let parts = if let Some(separator) = separator {
        if separator.text.is_empty() {
            text.chars()
                .enumerate()
                .map(|(index, ch)| part_value(ch.to_string(), index, index + 1))
                .collect::<Vec<_>>()
        } else {
            let regex = compile_elisp_regex(interp, &separator, env, "", true)?;
            let mut parts = Vec::new();
            let mut last_end = 0usize;
            let mut search_start = 0usize;

            while search_start <= text.len() {
                let captures = regex
                    .captures_from_pos(&text, search_start)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let Some(captures) = captures else {
                    break;
                };
                let Some(matched) = captures.get(0) else {
                    break;
                };

                let start = byte_to_char(last_end);
                let end = byte_to_char(matched.start());
                let part = &text[last_end..matched.start()];
                if !(omit_nulls && part.is_empty()) {
                    parts.push(part_value(part.to_string(), start, end));
                }
                last_end = matched.end();

                if matched.start() == matched.end() {
                    let Some(ch) = text[matched.end()..].chars().next() else {
                        break;
                    };
                    search_start = matched.end() + ch.len_utf8();
                } else {
                    search_start = matched.end();
                }
            }

            let start = byte_to_char(last_end);
            let end = text.chars().count();
            let tail = &text[last_end..];
            if !(omit_nulls && tail.is_empty()) {
                parts.push(part_value(tail.to_string(), start, end));
            }
            parts
        }
    } else {
        let mut parts = Vec::new();
        let mut part_start = None;
        let mut last_index = 0usize;
        for (index, ch) in text.chars().enumerate() {
            if ch.is_whitespace() {
                if let Some(start) = part_start.take() {
                    let part = text.chars().skip(start).take(index - start).collect();
                    parts.push(part_value(part, start, index));
                }
            } else if part_start.is_none() {
                part_start = Some(index);
            }
            last_index = index + 1;
        }
        if let Some(start) = part_start {
            let part = text.chars().skip(start).collect();
            parts.push(part_value(part, start, last_index));
        }
        parts
    };
    Ok(Value::list(parts))
}

struct SkipCharsSpec {
    negate: bool,
    literals: Vec<char>,
    ranges: Vec<(char, char)>,
    classes: Vec<String>,
}

fn parse_skip_chars_spec(spec: &str) -> SkipCharsSpec {
    let mut chars = spec.chars().peekable();
    let negate = if chars.peek() == Some(&'^') {
        chars.next();
        true
    } else {
        false
    };
    let mut literals = Vec::new();
    let mut ranges = Vec::new();
    let mut classes = Vec::new();

    while let Some(ch) = chars.next() {
        if ch == '[' && chars.peek() == Some(&':') {
            chars.next();
            let mut name = String::new();
            while let Some(next) = chars.next() {
                if next == ':' && chars.peek() == Some(&']') {
                    chars.next();
                    classes.push(name);
                    break;
                }
                name.push(next);
            }
            continue;
        }
        let mut preview = chars.clone();
        if preview.next() == Some('-')
            && let Some(end) = preview.next()
        {
            chars.next();
            chars.next();
            ranges.push((ch, end));
            continue;
        }
        literals.push(ch);
    }

    SkipCharsSpec {
        negate,
        literals,
        ranges,
        classes,
    }
}

fn skip_char_matches_class(ch: char, class: &str) -> bool {
    let code = raw_byte_from_regex_char(ch)
        .map(u32::from)
        .unwrap_or(ch as u32);
    match class {
        "alnum" => ch.is_alphanumeric(),
        "alpha" => ch.is_alphabetic(),
        "ascii" => code <= 0x7F,
        "blank" => matches!(ch, ' ' | '\t') || (ch.is_whitespace() && ch != '\n' && ch != '\r'),
        "cntrl" => code <= 0x1F,
        "digit" => ch.is_ascii_digit(),
        "graph" => !skip_char_matches_class(ch, "space") && !skip_char_matches_class(ch, "cntrl"),
        "lower" => ch.is_lowercase(),
        "multibyte" => !is_raw_byte_regex_char(ch) && (ch as u32) > 0xFF,
        "nonascii" => code > 0x7F || (!is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F),
        "print" => {
            !skip_char_matches_class(ch, "cntrl")
                && !matches!(ch, '\n' | '\r' | '\t' | '\u{000B}' | '\u{000C}')
        }
        "punct" => ch.is_ascii_punctuation(),
        "space" => ch.is_whitespace(),
        "unibyte" => code <= 0xFF,
        "upper" => ch.is_uppercase(),
        "word" => ch.is_alphanumeric() || ch == '_' || ch == '\u{2620}',
        "xdigit" => ch.is_ascii_hexdigit(),
        _ => false,
    }
}

fn skip_char_matches_spec(ch: char, spec: &SkipCharsSpec) -> bool {
    let literal_match = spec.literals.contains(&ch);
    let range_match = spec
        .ranges
        .iter()
        .any(|(start, end)| *start <= ch && ch <= *end);
    let class_match = spec
        .classes
        .iter()
        .any(|class| skip_char_matches_class(ch, class));
    let matched = literal_match || range_match || class_match;
    if spec.negate { !matched } else { matched }
}

pub(super) fn skip_chars_forward_impl(
    interp: &mut Interpreter,
    spec_value: &Value,
    limit_value: Option<&Value>,
) -> Result<Value, LispError> {
    let spec = parse_skip_chars_spec(&string_text(spec_value)?);
    let limit = if let Some(limit_value) = limit_value {
        if limit_value.is_nil() {
            interp.buffer.point_max()
        } else {
            position_from_value(interp, limit_value)?
        }
    } else {
        interp.buffer.point_max()
    };
    let start = interp.buffer.point();
    while interp.buffer.point() < limit {
        let Some(ch) = interp.buffer.char_at(interp.buffer.point()) else {
            break;
        };
        if !skip_char_matches_spec(ch, &spec) {
            break;
        }
        let _ = interp.buffer.forward_char(1);
    }
    Ok(Value::Integer(
        interp.buffer.point().saturating_sub(start) as i64
    ))
}

pub(super) fn skip_chars_backward_impl(
    interp: &mut Interpreter,
    spec_value: &Value,
    limit_value: Option<&Value>,
) -> Result<Value, LispError> {
    let spec = parse_skip_chars_spec(&string_text(spec_value)?);
    let limit = if let Some(limit_value) = limit_value {
        if limit_value.is_nil() {
            interp.buffer.point_min()
        } else {
            position_from_value(interp, limit_value)?
        }
    } else {
        interp.buffer.point_min()
    };
    let start = interp.buffer.point();
    while interp.buffer.point() > limit {
        let Some(ch) = interp.buffer.char_before() else {
            break;
        };
        if !skip_char_matches_spec(ch, &spec) {
            break;
        }
        let _ = interp.buffer.forward_char(-1);
    }
    Ok(Value::Integer(interp.buffer.point() as i64 - start as i64))
}

pub(super) fn match_string_impl(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 3 {
        return Err(LispError::WrongNumberOfArgs(
            "match-string".into(),
            args.len(),
        ));
    }
    let index = args[0].as_integer()?;
    if index < 0 {
        return Err(LispError::Signal("Args out of range".into()));
    }
    let match_data = interp
        .last_match_data
        .as_ref()
        .ok_or_else(|| LispError::Signal("No match data, because no search succeeded".into()))?;
    let Some((start, end)) = match_data.get(index as usize).and_then(|entry| *entry) else {
        return Ok(Value::Nil);
    };
    if let Some(string) = args.get(1).filter(|value| !value.is_nil()) {
        let string = string_like(string)
            .ok_or_else(|| LispError::TypeError("string".into(), string.type_name()))?;
        let chars: Vec<char> = string.text.chars().collect();
        if end > chars.len() {
            return Ok(Value::Nil);
        }
        let text = chars[start..end].iter().collect::<String>();
        return Ok(make_shared_string_value_with_multibyte(
            text,
            Vec::new(),
            string.multibyte,
        ));
    }
    let text = interp
        .buffer
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let multibyte = text
        .chars()
        .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7F);
    Ok(make_shared_string_value_with_multibyte(
        text,
        Vec::new(),
        multibyte,
    ))
}

pub(super) fn looking_at_impl(
    interp: &mut Interpreter,
    pattern_value: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let pattern = string_like(pattern_value)
        .ok_or_else(|| LispError::TypeError("string".into(), pattern_value.type_name()))?;
    let pattern = regex_pattern_with_search_spaces(interp, &pattern, env);
    let pos = interp.buffer.point();
    let regex = compile_elisp_regex(
        interp,
        &pattern,
        env,
        r"\A",
        pos == interp.buffer.point_min(),
    )?;
    let tail = interp
        .buffer
        .buffer_substring(pos, interp.buffer.point_max())
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if let Some(captures) = regex
        .captures(&tail)
        .map_err(|error| LispError::Signal(error.to_string()))?
        && let Some(matched) = captures.get(0)
        && matched.start() == 0
    {
        set_match_data(
            interp,
            pos,
            &tail,
            &captures,
            regex.capture_mapping(),
            Some(interp.current_buffer_id()),
        );
        Ok(Value::T)
    } else {
        Ok(Value::Nil)
    }
}

pub(super) fn looking_back_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
) -> Result<Value, LispError> {
    need_arg_range("looking-back", args, 1, 3)?;
    let pattern = string_like(&args[0])
        .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
    let pos = interp.buffer.point();
    let limit = args
        .get(1)
        .filter(|value| !value.is_nil())
        .map(|value| position_from_value(interp, value))
        .transpose()?
        .unwrap_or_else(|| interp.buffer.point_min())
        .clamp(interp.buffer.point_min(), pos);
    let haystack = interp
        .buffer
        .buffer_substring(limit, pos)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let regex = compile_elisp_regex(
        interp,
        &pattern,
        env,
        "",
        limit == interp.buffer.point_min(),
    )?;
    let greedy = args.get(2).is_some_and(Value::is_truthy);
    let mut best = None;
    let mut starts = haystack
        .char_indices()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    starts.push(haystack.len());
    let mut empty_fallback = None;
    for start in starts {
        if let Some(captures) = regex
            .captures_from_pos(&haystack, start)
            .map_err(|error| LispError::Signal(error.to_string()))?
            && let Some(matched) = captures.get(0)
            && matched.end() == haystack.len()
        {
            let absolute_start = limit + haystack[..matched.start()].chars().count();
            // GNU prefers the latest-starting NON-EMPTY match ending at
            // point; a zero-length match only counts when nothing else
            // matches (pp-fill's "#[sf]?" probe must find the "#").
            if matched.start() == matched.end() {
                empty_fallback = Some((absolute_start, captures));
            } else {
                best = Some((absolute_start, captures));
                if greedy {
                    break;
                }
            }
        }
    }
    if best.is_none() {
        best = empty_fallback;
    }
    if let Some((_absolute_start, captures)) = best {
        // Captures are haystack-relative: the base for match data is the
        // haystack origin (LIMIT), not the match start.
        set_match_data(
            interp,
            limit,
            &haystack,
            &captures,
            regex.capture_mapping(),
            Some(interp.current_buffer_id()),
        );
        Ok(Value::T)
    } else {
        Ok(Value::Nil)
    }
}

pub(super) fn buffer_regex_search(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
    forward: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            if forward {
                "re-search-forward".into()
            } else {
                "re-search-backward".into()
            },
            args.len(),
        ));
    }
    let pattern = string_like(&args[0])
        .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
    let pattern = regex_pattern_with_search_spaces(interp, &pattern, env);
    let regex = compile_elisp_regex(
        interp,
        &pattern,
        env,
        if forward { r"\A" } else { r"\z" },
        if forward {
            interp.buffer.point() == interp.buffer.point_min()
        } else {
            true
        },
    )?;
    let noerror = args.get(2).is_some_and(Value::is_truthy);
    let move_on_failure = search_noerror_moves(args.get(2));
    if forward {
        let start = interp.buffer.point();
        let limit = match args.get(1) {
            // GNU clamps a BOUND outside the accessible region.
            Some(Value::Integer(pos)) if *pos < interp.buffer.point_min() as i64 => {
                interp.buffer.point_min()
            }
            Some(value) if !value.is_nil() => position_from_value(interp, value)?,
            _ => interp.buffer.point_max(),
        };
        let limit = limit.min(interp.buffer.point_max());
        let count = args
            .get(3)
            .filter(|value| !value.is_nil())
            .map(Value::as_integer)
            .transpose()?
            .unwrap_or(1);
        if count == 0 {
            let point = interp.buffer.point();
            interp.last_match_data = Some(vec![Some((point, point))]);
            interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
            return Ok(Value::Integer(point as i64));
        }
        if count < 0 {
            let mut backward_args = args.to_vec();
            if backward_args.len() < 4 {
                backward_args.resize(4, Value::Nil);
            }
            backward_args[3] = Value::Integer(-count);
            return buffer_regex_search(interp, &backward_args, env, false);
        }
        if limit < start {
            return if noerror {
                if move_on_failure {
                    interp.buffer.goto_char(limit);
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::SignalValue(Value::list([
                    Value::Symbol("search-failed".into()),
                    Value::String(pattern.text.clone()),
                ])))
            };
        }
        // GNU `\=' asserts the buffer position where this search began,
        // wherever it occurs in the regexp (often in an alternative).  Make
        // that position the delegate haystack's origin so translated `\A'
        // retains the assertion without preventing other alternatives from
        // searching forward.  Looking only at a leading `\=' breaks Eshell's
        // `(?:\=\|...)' delimiter patterns.
        let point_asserted = contains_point_assertion(&pattern.text);
        let haystack_start = if point_asserted {
            start
        } else {
            interp.buffer.point_min()
        };
        let haystack = interp
            .buffer
            .buffer_substring(haystack_start, limit)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        // `captures_from_pos' takes a BYTE offset; positions are chars.
        let start_chars = start.saturating_sub(haystack_start);
        let mut search_offset = haystack
            .char_indices()
            .nth(start_chars)
            .map(|(byte, _)| byte)
            .unwrap_or(haystack.len());
        for _ in 0..count {
            let Some(captures) = regex
                .captures_from_pos(&haystack, search_offset)
                .map_err(|error| LispError::Signal(error.to_string()))?
            else {
                return if noerror {
                    if move_on_failure {
                        interp.buffer.goto_char(limit);
                    }
                    Ok(Value::Nil)
                } else {
                    Err(LispError::SignalValue(Value::list([
                        Value::Symbol("search-failed".into()),
                        Value::String(pattern.text.clone()),
                    ])))
                };
            };
            let Some(matched) = captures.get(0) else {
                break;
            };
            let pos = haystack_start + haystack[..matched.end()].chars().count();
            set_match_data(
                interp,
                haystack_start,
                &haystack,
                &captures,
                regex.capture_mapping(),
                Some(interp.current_buffer_id()),
            );
            interp.buffer.goto_char(pos);
            search_offset = if matched.end() > search_offset {
                matched.end()
            } else {
                haystack[search_offset..]
                    .char_indices()
                    .nth(1)
                    .map(|(offset, _)| search_offset + offset)
                    .unwrap_or(haystack.len())
            };
        }
        Ok(Value::Integer(interp.buffer.point() as i64))
    } else {
        let count = args
            .get(3)
            .filter(|value| !value.is_nil())
            .map(Value::as_integer)
            .transpose()?
            .unwrap_or(1);
        if count == 0 {
            let point = interp.buffer.point();
            interp.last_match_data = Some(vec![Some((point, point))]);
            interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
            return Ok(Value::Integer(point as i64));
        }
        if count < 0 {
            let mut forward_args = args.to_vec();
            if forward_args.len() < 4 {
                forward_args.resize(4, Value::Nil);
            }
            forward_args[3] = Value::Integer(-count);
            return buffer_regex_search(interp, &forward_args, env, true);
        }
        let limit = match args.get(1) {
            Some(Value::Integer(pos)) if *pos < interp.buffer.point_min() as i64 => {
                interp.buffer.point_min()
            }
            Some(value) if !value.is_nil() => position_from_value(interp, value)?,
            _ => interp.buffer.point_min(),
        };
        let limit = limit.max(interp.buffer.point_min());
        if limit > interp.buffer.point() {
            return if noerror {
                if move_on_failure {
                    interp.buffer.goto_char(limit);
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::SignalValue(Value::list([
                    Value::Symbol("search-failed".into()),
                    Value::String(pattern.text.clone()),
                ])))
            };
        }
        for _ in 0..count {
            let absolute_start = interp.buffer.point_min();
            let prefix = interp
                .buffer
                .buffer_substring(absolute_start, interp.buffer.point())
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let empty_line_pattern = pattern.text == "^$";
            if empty_line_pattern
                && let Some(pos) = last_empty_line_match_position(absolute_start, &prefix, limit)
            {
                interp.last_match_data = Some(vec![Some((pos, pos))]);
                interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                interp.buffer.goto_char(pos);
                continue;
            }
            let mut best_match: Option<(usize, usize, usize)> = None;
            for start_byte in prefix
                .char_indices()
                .map(|(index, _)| index)
                .chain(std::iter::once(prefix.len()))
            {
                let Some(captures) = regex
                    .captures_from_pos(&prefix, start_byte)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                else {
                    continue;
                };
                let Some(matched) = captures.get(0) else {
                    continue;
                };
                let Some(match_start) = backward_match_position(
                    absolute_start,
                    &prefix,
                    matched.start(),
                    empty_line_pattern,
                ) else {
                    continue;
                };
                if match_start < limit {
                    continue;
                }
                let Some(match_end) = backward_match_position(
                    absolute_start,
                    &prefix,
                    matched.end(),
                    empty_line_pattern,
                ) else {
                    continue;
                };
                if best_match.is_none_or(|(best_start, best_end, _)| {
                    match_start > best_start || (match_start == best_start && match_end > best_end)
                }) {
                    best_match = Some((match_start, match_end, matched.start()));
                }
            }
            if let Some((match_start, _, start_byte)) = best_match
                && let Some(captures) = regex
                    .captures_from_pos(&prefix, start_byte)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                && captures
                    .get(0)
                    .is_some_and(|matched| matched.start() == start_byte)
            {
                set_backward_match_data(
                    interp,
                    absolute_start,
                    &prefix,
                    &captures,
                    regex.capture_mapping(),
                    Some(interp.current_buffer_id()),
                    empty_line_pattern,
                );
                interp.buffer.goto_char(match_start);
                continue;
            }
            return if noerror {
                if move_on_failure {
                    interp.buffer.goto_char(limit);
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::SignalValue(Value::list([
                    Value::Symbol("search-failed".into()),
                    Value::String(pattern.text.clone()),
                ])))
            };
        }
        Ok(Value::Integer(interp.buffer.point() as i64))
    }
}

fn search_noerror_moves(noerror: Option<&Value>) -> bool {
    noerror.is_some_and(|value| value.is_truthy() && !matches!(value, Value::T))
}

fn last_empty_line_match_position(
    absolute_start: usize,
    haystack: &str,
    limit: usize,
) -> Option<usize> {
    if haystack.is_empty() && absolute_start >= limit {
        return Some(absolute_start);
    }

    let mut previous_was_newline = true;
    let mut best = None;
    for (char_offset, ch) in haystack.chars().enumerate() {
        if ch == '\n' && previous_was_newline {
            let pos = absolute_start + char_offset;
            if pos >= limit {
                best = Some(pos);
            }
        }
        previous_was_newline = ch == '\n';
    }
    best
}

fn set_backward_match_data(
    interp: &mut Interpreter,
    absolute_start: usize,
    haystack: &str,
    captures: &fancy_regex::Captures<'_>,
    capture_mapping: &[usize],
    source_buffer_id: Option<u64>,
    empty_line_pattern: bool,
) {
    let mut match_data = vec![None; capture_mapping.iter().copied().max().unwrap_or(0) + 1];
    for index in 0..captures.len() {
        let Some(matched) = captures.get(index) else {
            continue;
        };
        let Some(start) = backward_match_position(
            absolute_start,
            haystack,
            matched.start(),
            empty_line_pattern,
        ) else {
            continue;
        };
        let Some(end) =
            backward_match_position(absolute_start, haystack, matched.end(), empty_line_pattern)
        else {
            continue;
        };
        let target_index = if index == 0 {
            0
        } else {
            capture_mapping.get(index - 1).copied().unwrap_or(index)
        };
        if match_data.len() <= target_index {
            match_data.resize(target_index + 1, None);
        }
        match_data[target_index] = Some((start, end));
    }
    interp.last_match_data = Some(match_data);
    interp.last_match_data_buffer_id = source_buffer_id;
}

fn backward_match_position(
    absolute_start: usize,
    haystack: &str,
    byte_index: usize,
    empty_line_pattern: bool,
) -> Option<usize> {
    if empty_line_pattern && byte_index > 0 && haystack[..byte_index].ends_with('\n') {
        let newline_byte = haystack[..byte_index].rfind('\n')?;
        if newline_byte == 0 || haystack[..newline_byte].ends_with('\n') {
            return Some(absolute_start + haystack[..newline_byte].chars().count());
        }
        return None;
    }
    Some(absolute_start + haystack[..byte_index].chars().count())
}

pub(super) fn expand_replace_match(
    interp: &Interpreter,
    replacement: &str,
    match_data: &[Option<(usize, usize)>],
    literal: bool,
) -> Result<String, LispError> {
    if literal {
        return Ok(replacement.to_string());
    }
    let chars: Vec<char> = replacement.chars().collect();
    let mut expanded = String::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '\\'
            && let Some(next) = chars.get(index + 1).copied()
        {
            match next {
                '&' => expanded.push_str(&match_text_from_buffer(interp, match_data, 0)?),
                '1'..='9' => {
                    let capture_index = next.to_digit(10).unwrap_or(0) as usize;
                    expanded.push_str(&match_text_from_buffer(interp, match_data, capture_index)?);
                }
                '\\' => expanded.push('\\'),
                other => expanded.push(other),
            }
            index += 2;
            continue;
        }
        expanded.push(chars[index]);
        index += 1;
    }
    Ok(expanded)
}

pub(super) fn expand_replace_match_text(
    replacement: &str,
    match_data: &[Option<(usize, usize)>],
    literal: bool,
    source: &str,
) -> Result<String, LispError> {
    if literal {
        return Ok(replacement.to_string());
    }
    let chars: Vec<char> = replacement.chars().collect();
    let mut expanded = String::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '\\'
            && let Some(next) = chars.get(index + 1).copied()
        {
            match next {
                '&' => expanded.push_str(&match_text_from_string(source, match_data, 0)),
                '1'..='9' => {
                    let capture_index = next.to_digit(10).unwrap_or(0) as usize;
                    expanded.push_str(&match_text_from_string(source, match_data, capture_index));
                }
                '\\' => expanded.push('\\'),
                other => expanded.push(other),
            }
            index += 2;
            continue;
        }
        expanded.push(chars[index]);
        index += 1;
    }
    Ok(expanded)
}

fn match_text_from_buffer(
    interp: &Interpreter,
    match_data: &[Option<(usize, usize)>],
    index: usize,
) -> Result<String, LispError> {
    let Some((start, end)) = match_data.get(index).and_then(|entry| *entry) else {
        return Ok(String::new());
    };
    interp
        .buffer
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn match_text_from_string(
    source: &str,
    match_data: &[Option<(usize, usize)>],
    index: usize,
) -> String {
    let Some((start, end)) = match_data.get(index).and_then(|entry| *entry) else {
        return String::new();
    };
    slice_string_chars(source, start, end)
}

pub(super) fn slice_string_chars(source: &str, start: usize, end: usize) -> String {
    source
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

pub(super) fn byte_index_for_char(source: &str, char_index: usize) -> usize {
    source
        .char_indices()
        .nth(char_index)
        .map(|(byte_index, _)| byte_index)
        .unwrap_or(source.len())
}

pub(super) fn update_match_data_after_replace(
    match_data: &[Option<(usize, usize)>],
    replace_index: usize,
    start: usize,
    end: usize,
    replacement_len: usize,
) -> Vec<Option<(usize, usize)>> {
    let new_end = start + replacement_len;
    let delta = replacement_len as isize - end.saturating_sub(start) as isize;
    match_data
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let Some((group_start, group_end)) = entry else {
                return None;
            };
            if index == replace_index {
                return Some((start, new_end));
            }
            if *group_start == *group_end && *group_start == start && start == end {
                return Some((start, new_end));
            }
            if start == end && *group_start == start && *group_end > end {
                return Some((start, group_end.saturating_add_signed(delta)));
            }
            if start == end && *group_end == start && *group_start < start {
                return Some((*group_start, group_end.saturating_add_signed(delta)));
            }
            if *group_end <= start {
                return Some((*group_start, *group_end));
            }
            if *group_start >= end {
                return Some((
                    group_start.saturating_add_signed(delta),
                    group_end.saturating_add_signed(delta),
                ));
            }
            if *group_start >= start && *group_end <= end {
                return Some((start, new_end));
            }
            let updated_start = if *group_start > start {
                start
            } else {
                *group_start
            };
            let updated_end = if *group_end < end {
                new_end
            } else {
                group_end.saturating_add_signed(delta)
            };
            Some((updated_start, updated_end))
        })
        .collect()
}

pub(super) fn expand_symbol_at(haystack: &str, found: usize, prefix: &str) -> Option<String> {
    let tail = &haystack[found..];
    let end = tail
        .char_indices()
        .take_while(|(_, ch)| ch.is_alphanumeric() || *ch == '-' || *ch == '_')
        .last()
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(prefix.len());
    let expansion = &tail[..end];
    if expansion.starts_with(prefix) {
        Some(expansion.to_string())
    } else {
        None
    }
}
