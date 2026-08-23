use super::*;

const REGEX_WORD_CLASS: &str = r"[\p{Alphabetic}\p{Number}_\x{2620}]";
const REGEX_NON_WORD_CLASS: &str = r"[^\p{Alphabetic}\p{Number}_\x{2620}]";
const REGEX_SYMBOL_CLASS: &str = r"[\p{Alphabetic}\p{Number}_\-\x{2620}]";
const REGEX_NON_SYMBOL_CLASS: &str = r"[^\p{Alphabetic}\p{Number}_\-\x{2620}]";
const REGEX_WHITESPACE_CLASS: &str = r"[\p{White_Space}]";
const REGEX_NON_WHITESPACE_CLASS: &str = r"[^\p{White_Space}]";
// A syntax/category opcode always has a one-character consuming shape even
// when no character belongs to its class.  A bare `(?! )'-style assertion is
// zero-width and cannot legally receive a repeat operator in the delegate.
const NEVER_MATCH_ONE_CHAR: &str = r"(?:(?![\s\S])[\s\S])";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SyntaxPropertySentinel {
    original: char,
    class: char,
    sentinel: char,
}

#[derive(Clone, Debug)]
struct SyntaxPropertyEncoding {
    haystack: String,
    sentinels: Vec<SyntaxPropertySentinel>,
}

impl SyntaxPropertyEncoding {
    fn original_sentinels(&self, original: char, case_fold: bool) -> Vec<char> {
        self.sentinels
            .iter()
            .filter_map(|entry| {
                chars_equal_for_regexp(entry.original, original, case_fold)
                    .then_some(entry.sentinel)
            })
            .collect()
    }
}

fn boundary_character_class(
    base: &str,
    encoding: Option<&SyntaxPropertyEncoding>,
    syntax_classes: &[char],
) -> String {
    let Some(encoding) = encoding else {
        return base.to_string();
    };
    let sentinels = encoding
        .sentinels
        .iter()
        .filter(|entry| syntax_classes.contains(&entry.class))
        .map(|entry| format!(r"\x{{{:x}}}", entry.sentinel as u32))
        .collect::<String>();
    if sentinels.is_empty() {
        return base.to_string();
    }

    let prefix = base
        .strip_suffix(']')
        .expect("regexp boundary classes are bracket expressions");
    format!("{prefix}{sentinels}]")
}

fn chars_equal_for_regexp(left: char, right: char, case_fold: bool) -> bool {
    left == right || (case_fold && left.to_lowercase().eq(right.to_lowercase()))
}

#[derive(Clone)]
enum RegexClassAtom {
    Char(char),
    Posix(String),
}

pub(super) fn translate_elisp_regex(pattern: &str) -> String {
    translate_elisp_regex_with_point(pattern, "", r"\A", None, false, None, None)
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
enum RegexpCategoryScope {
    Standard,
    CurrentBuffer,
}

impl RegexpCategoryScope {
    fn table_id(self, interp: &Interpreter) -> Option<u64> {
        match self {
            Self::Standard => interp.initialized_standard_category_table_id(),
            Self::CurrentBuffer => interp.initialized_current_category_table_id(),
        }
    }
}

fn pattern_depends_on_category_table(pattern: &str) -> bool {
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            continue;
        }
        match chars.next() {
            Some('\\') => {}
            Some('c' | 'C') if chars.next().is_some() => return true,
            Some(_) | None => {}
        }
    }
    false
}

fn category_set_contains(interp: &Interpreter, value: &Value, category: char) -> bool {
    match value {
        Value::String(text) => text.chars().any(|member| member == category),
        Value::Record(id) => interp.find_record(*id).is_some_and(|record| {
            record.kind == crate::lisp::eval::RecordKind::BoolVector
                && record
                    .slots
                    .get(category as usize)
                    .is_some_and(Value::is_truthy)
        }),
        _ => false,
    }
}

fn category_regex_ranges(interp: &Interpreter, table_id: u64, category: char) -> Vec<(u32, u32)> {
    const SCALAR_END: u32 = char::MAX as u32 + 1;
    let mut boundaries = vec![0, 0xd800, 0xe000, SCALAR_END];
    let mut table = Some(table_id);
    let mut seen = HashSet::new();
    while let Some(id) = table {
        if !seen.insert(id) {
            break;
        }
        let Some(state) = interp.find_char_table(id) else {
            break;
        };
        for entry in &state.entries {
            if entry.start < SCALAR_END {
                boundaries.push(entry.start);
            }
            if entry.end < char::MAX as u32 {
                boundaries.push(entry.end + 1);
            }
        }
        table = state.parent;
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut ranges = Vec::<(u32, u32)>::new();
    for window in boundaries.windows(2) {
        let start = window[0];
        let end = window[1] - 1;
        if char::from_u32(start).is_none()
            || !interp
                .char_table_get(table_id, start)
                .is_some_and(|value| category_set_contains(interp, &value, category))
        {
            continue;
        }
        if let Some((_, previous_end)) = ranges.last_mut()
            && previous_end.saturating_add(1) == start
        {
            *previous_end = end;
        } else {
            ranges.push((start, end));
        }
    }
    ranges
}

fn category_regex_fragment(
    interp: Option<&Interpreter>,
    table_id: Option<u64>,
    category: char,
    negated: bool,
) -> String {
    let ranges = interp
        .zip(table_id)
        .map(|(interp, table_id)| category_regex_ranges(interp, table_id, category))
        .unwrap_or_default();
    if ranges.is_empty() {
        return if negated {
            r"[\s\S]".to_string()
        } else {
            NEVER_MATCH_ONE_CHAR.to_string()
        };
    }

    let members = ranges
        .into_iter()
        .map(|(start, end)| {
            if start == end {
                format!(r"\x{{{start:x}}}")
            } else {
                format!(r"\x{{{start:x}}}-\x{{{end:x}}}")
            }
        })
        .collect::<String>();
    let class = if negated {
        format!("[^{members}]")
    } else {
        format!("[{members}]")
    };
    // Category membership is independent of `case-fold-search'.
    format!("(?-i:{class})")
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

fn append_translated_literal_with_sentinels(
    translated: &mut String,
    rendered_literal: &str,
    original: char,
    encoding: Option<&SyntaxPropertyEncoding>,
    case_fold: bool,
) {
    let sentinels = encoding
        .map(|encoding| encoding.original_sentinels(original, case_fold))
        .unwrap_or_default();
    if sentinels.is_empty() {
        translated.push_str(rendered_literal);
        return;
    }

    translated.push_str("(?:");
    translated.push_str(rendered_literal);
    for sentinel in sentinels {
        translated.push('|');
        translated.push_str(&format!(r"\x{{{:x}}}", sentinel as u32));
    }
    translated.push(')');
}

fn translate_elisp_regex_with_point(
    pattern: &str,
    point_assertion: &str,
    absolute_start_assertion: &str,
    encoding: Option<&SyntaxPropertyEncoding>,
    case_fold: bool,
    interp: Option<&Interpreter>,
    category_table_id: Option<u64>,
) -> String {
    let rendered_syntax_classes = interp
        .filter(|_| pattern_depends_on_syntax_table(pattern))
        .map(|interp| rendered_table_syntax_classes(interp, encoding));
    let word_class = rendered_syntax_classes.as_ref().map_or_else(
        || boundary_character_class(REGEX_WORD_CLASS, encoding, &['w']),
        |rendered| table_syntax_class_fragment(rendered, super::syntax::SyntaxClass::Word, false),
    );
    let non_word_class = rendered_syntax_classes.as_ref().map_or_else(
        || boundary_character_class(REGEX_NON_WORD_CLASS, encoding, &['w']),
        |rendered| table_syntax_class_fragment(rendered, super::syntax::SyntaxClass::Word, true),
    );
    let symbol_boundary_class = rendered_syntax_classes.as_ref().map_or_else(
        || boundary_character_class(REGEX_SYMBOL_CLASS, encoding, &['w', '_']),
        table_symbol_class_fragment,
    );
    let property_newline_sentinels = encoding
        .map(|encoding| encoding.original_sentinels('\n', false))
        .unwrap_or_default();
    let newline_sentinel_class = property_newline_sentinels
        .iter()
        .map(|ch| format!(r"\x{{{:x}}}", *ch as u32))
        .collect::<String>();
    let mut translated = String::new();
    let mut chars = pattern.chars().peekable();
    let mut at_branch_start = true;
    let mut can_repeat_previous = false;
    let mut last_was_quantifier = false;
    let mut inside_interval = false;
    while let Some(ch) = chars.next() {
        // The digits and comma between Emacs `\{' and `\}' are repeat
        // metadata, not searchable literals.  Keep them under the grammar
        // owner here so syntax-property literal preservation cannot rewrite
        // a valid interval.
        if inside_interval && ch != '\\' {
            translated.push(ch);
            continue;
        }
        if ch == '[' {
            translated.push_str(&translate_bracket_expression(
                &mut chars,
                encoding,
                case_fold,
                interp,
                rendered_syntax_classes.as_ref(),
            ));
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
                            let mut preview = chars.clone();
                            if preview.next() == Some('\\') && preview.next() == Some(')') {
                                chars.next();
                                chars.next();
                                // `rx' legitimately emits empty shy groups.
                                // fancy-regex rejects a quantifier applied to
                                // one, although every repetition of the empty
                                // language is still empty.  Consume both at
                                // this semantic boundary.
                                translated
                                    .push_str(&translate_zero_width_assertion(&mut chars, ""));
                                at_branch_start = false;
                                can_repeat_previous = false;
                                last_was_quantifier = false;
                                continue;
                            }
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
                    inside_interval = true;
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('}') => {
                    translated.push('}');
                    inside_interval = false;
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
                    translated.push_str(&regex_syntax_class(
                        &mut chars,
                        false,
                        encoding,
                        rendered_syntax_classes.as_ref(),
                    ));
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('S') => {
                    translated.push_str(&regex_syntax_class(
                        &mut chars,
                        true,
                        encoding,
                        rendered_syntax_classes.as_ref(),
                    ));
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some(category_escape @ ('c' | 'C')) => {
                    let category = chars.next().unwrap_or('\0');
                    translated.push_str(&category_regex_fragment(
                        interp,
                        category_table_id,
                        category,
                        category_escape == 'C',
                    ));
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('w') => {
                    translated.push_str(&word_class);
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('W') => {
                    translated.push_str(&non_word_class);
                    at_branch_start = false;
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                Some('b') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        &format!(
                            "(?:(?<!{word_class})(?={word_class})|(?<={word_class})(?!{word_class}))"
                        ),
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('B') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        &format!(
                            "(?:(?<={word_class})(?={word_class})|(?<!{word_class})(?!{word_class}))"
                        ),
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('<') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        &format!("(?<!{word_class})(?={word_class})"),
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('>') => {
                    translated.push_str(&translate_zero_width_assertion(
                        &mut chars,
                        &format!("(?<={word_class})(?!{word_class})"),
                    ));
                    at_branch_start = false;
                    can_repeat_previous = false;
                    last_was_quantifier = false;
                }
                Some('_') => match chars.next() {
                    Some('<') => {
                        translated.push_str(&translate_zero_width_assertion(
                            &mut chars,
                            &format!("(?<!{symbol_boundary_class})(?={symbol_boundary_class})"),
                        ));
                        at_branch_start = false;
                        can_repeat_previous = false;
                        last_was_quantifier = false;
                    }
                    Some('>') => {
                        translated.push_str(&translate_zero_width_assertion(
                            &mut chars,
                            &format!("(?<={symbol_boundary_class})(?!{symbol_boundary_class})"),
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
                    let rendered = if other.is_ascii_alphabetic() {
                        other.to_string()
                    } else {
                        format!(r"\{other}")
                    };
                    append_translated_literal_with_sentinels(
                        &mut translated,
                        &rendered,
                        other,
                        encoding,
                        case_fold,
                    );
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
                    if newline_sentinel_class.is_empty() {
                        translated.push('^');
                    } else {
                        translated.push_str(&format!("(?:^|(?<=[{newline_sentinel_class}]))"));
                    }
                    can_repeat_previous =
                        literalize_postfix_after_absolute_anchor(&mut translated, &mut chars);
                } else {
                    append_translated_literal_with_sentinels(
                        &mut translated,
                        r"\^",
                        '^',
                        encoding,
                        case_fold,
                    );
                    can_repeat_previous = true;
                }
                at_branch_start = false;
                last_was_quantifier = false;
            }
            '$' => {
                if is_dollar_anchor_position(&chars) {
                    let assertion = if newline_sentinel_class.is_empty() {
                        "$".to_string()
                    } else {
                        format!("(?:$|(?=[{newline_sentinel_class}]))")
                    };
                    translated.push_str(&translate_zero_width_assertion(&mut chars, &assertion));
                    can_repeat_previous = false;
                } else {
                    append_translated_literal_with_sentinels(
                        &mut translated,
                        r"\$",
                        '$',
                        encoding,
                        case_fold,
                    );
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
                                append_translated_literal_with_sentinels(
                                    &mut translated,
                                    r"\+",
                                    '+',
                                    encoding,
                                    case_fold,
                                );
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
                    let rendered = format!(r"\{ch}");
                    append_translated_literal_with_sentinels(
                        &mut translated,
                        &rendered,
                        ch,
                        encoding,
                        case_fold,
                    );
                    can_repeat_previous = true;
                    last_was_quantifier = false;
                }
                at_branch_start = false;
            }
            '.' if !newline_sentinel_class.is_empty() => {
                // Keep the original character's newline identity even when
                // syntax-property encoding replaces it with a sentinel.
                // The assertion and delegate dot form one regexp atom: a
                // following quantifier must repeat both, otherwise `.*'
                // checks only its first position and can cross an encoded
                // newline later in the run.
                translated.push_str(&format!("(?:(?![{newline_sentinel_class}]).)"));
                at_branch_start = false;
                can_repeat_previous = true;
                last_was_quantifier = false;
            }
            '(' | ')' | '{' | '}' | '|' => {
                let rendered = format!(r"\{ch}");
                append_translated_literal_with_sentinels(
                    &mut translated,
                    &rendered,
                    ch,
                    encoding,
                    case_fold,
                );
                at_branch_start = false;
                can_repeat_previous = true;
                last_was_quantifier = false;
            }
            _ => {
                append_translated_literal_with_sentinels(
                    &mut translated,
                    &ch.to_string(),
                    ch,
                    encoding,
                    case_fold,
                );
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

pub(super) fn pattern_depends_on_syntax_table(pattern: &str) -> bool {
    // POSIX `word' is the bracket-expression spelling of the same current
    // syntax-table predicate as `\w'.  This conservative text probe may do
    // unnecessary table work for an escaped literal, but cannot change its
    // meaning; the bracket grammar below remains the authority that decides
    // whether the token is actually an atom.
    if pattern.contains("[:word:]") {
        return true;
    }
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            continue;
        }
        match chars.next() {
            // A quoted backslash is a searchable literal, not the start of
            // another regexp escape.
            Some('\\') => {}
            Some('w' | 'W') => return true,
            Some('_') if matches!(chars.next(), Some('<' | '>')) => return true,
            Some('s' | 'S') if chars.next().is_some() => return true,
            Some(_) | None => {}
        }
    }
    false
}

/// Render every effective one-character syntax class for the current syntax
/// table.  GNU's regexp opcodes compare the current character's syntax code
/// at match time; fixed Unicode classes cannot reproduce mode-specific table
/// entries, inherited wide ranges, or the standard table's non-Unicode
/// choices (`$' and `%' are words, while `_` is a symbol).
///
/// The table and all of its parents are piecewise constant.  Collecting their
/// range boundaries lets one pass produce exact, compact delegate classes for
/// all 16 GNU syntax codes without scanning every Unicode scalar value.
fn rendered_table_syntax_classes(
    interp: &Interpreter,
    encoding: Option<&SyntaxPropertyEncoding>,
) -> [String; 16] {
    let table_id = interp.current_syntax_table_id();
    if encoding.is_none()
        && let Some(rendered) = interp.cached_regexp_syntax_classes(table_id)
    {
        return rendered;
    }
    #[cfg(test)]
    REGEXP_SYNTAX_CLASS_RENDER_COUNT.with(|count| count.set(count.get() + 1));

    const SCALAR_END: u32 = char::MAX as u32 + 1;
    // Every standard-table default transition is in ASCII; all GNU
    // multibyte characters are words by default.  Individual ASCII
    // boundaries therefore make the terminal synthesized default constant
    // within every segment, while the other boundaries exclude surrogates.
    let mut boundaries = (0..=0x80).collect::<Vec<_>>();
    boundaries.extend([0xd800, 0xe000, SCALAR_END]);
    let mut current = Some(table_id);
    let mut seen = HashSet::new();
    let mut cacheable = encoding.is_none();
    while let Some(id) = current {
        if !seen.insert(id) {
            break;
        }
        let Some(table) = interp.find_char_table(id) else {
            cacheable = false;
            break;
        };
        // GNU character-table entries are ordinary live Lisp objects.  An
        // in-place mutation of a cons or mutable string does not pass through
        // Emaxx's character-table mutation door, so never retain a rendering
        // derived from either representation.  Ordinary modify-syntax-entry
        // strings are immutable SharedText and take the cached path.
        cacheable &= !matches!(table.default, Value::Cons(_) | Value::StringObject(_))
            && table
                .entries
                .iter()
                .all(|entry| !matches!(entry.value, Value::Cons(_) | Value::StringObject(_)));
        for entry in &table.entries {
            if entry.start < SCALAR_END {
                boundaries.push(entry.start);
            }
            if entry.end < char::MAX as u32 {
                boundaries.push(entry.end + 1);
            }
        }
        current = table.parent;
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut segments = Vec::<(u32, u32, super::syntax::SyntaxClass)>::new();
    for window in boundaries.windows(2) {
        let start = window[0];
        let end = window[1] - 1;
        if char::from_u32(start).is_none() {
            continue;
        }
        let class = super::syntax::syntax_entry_for_code(interp, table_id, start).class;
        if let Some((_, previous_end, previous_class)) = segments.last_mut()
            && *previous_class == class
            && previous_end.saturating_add(1) == start
        {
            *previous_end = end;
        } else {
            segments.push((start, end, class));
        }
    }

    let range_members = |start: u32, end: u32| {
        if start == end {
            format!(r"\x{{{start:x}}}")
        } else {
            format!(r"\x{{{start:x}}}-\x{{{end:x}}}")
        }
    };
    let mut members: [String; 16] = std::array::from_fn(|_| String::new());
    for (start, end, class) in segments {
        members[class as usize].push_str(&range_members(start, end));
    }
    let mut rendered = members.map(|members| {
        if members.is_empty() {
            NEVER_MATCH_ONE_CHAR.to_string()
        } else {
            format!("[{members}]")
        }
    });
    if cacheable {
        interp.cache_regexp_syntax_classes(table_id, rendered.clone());
    }

    let Some(encoding) = encoding else {
        return rendered;
    };
    let all_sentinels = encoding
        .sentinels
        .iter()
        .map(|entry| format!(r"\x{{{:x}}}", entry.sentinel as u32))
        .collect::<String>();
    for (index, class_pattern) in rendered.iter_mut().enumerate() {
        let matching_sentinels = encoding
            .sentinels
            .iter()
            .filter(|entry| {
                super::syntax::syntax_class_from_char(entry.class)
                    .is_some_and(|class| class as usize == index)
            })
            .map(|entry| format!(r"\x{{{:x}}}", entry.sentinel as u32))
            .collect::<String>();
        let guarded = format!("(?![{all_sentinels}])(?:{class_pattern})");
        *class_pattern = if matching_sentinels.is_empty() {
            guarded
        } else {
            format!("(?:{guarded}|[{matching_sentinels}])")
        };
    }
    rendered
}

fn table_syntax_class_fragment(
    rendered: &[String; 16],
    class: super::syntax::SyntaxClass,
    negated: bool,
) -> String {
    let positive = &rendered[class as usize];
    let fragment = if negated {
        format!("(?!{positive})[\\s\\S]")
    } else {
        positive.clone()
    };
    format!("(?-i:{fragment})")
}

fn table_symbol_class_fragment(rendered: &[String; 16]) -> String {
    let word = &rendered[super::syntax::SyntaxClass::Word as usize];
    let symbol = &rendered[super::syntax::SyntaxClass::Symbol as usize];
    format!("(?-i:(?:{word}|{symbol}))")
}

fn regex_syntax_class(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    negated: bool,
    encoding: Option<&SyntaxPropertyEncoding>,
    rendered_syntax_classes: Option<&[String; 16]>,
) -> String {
    let class_char = chars.next();
    if let Some(rendered) = rendered_syntax_classes {
        return class_char
            .and_then(super::syntax::syntax_class_from_char)
            .map_or_else(
                || {
                    if negated {
                        r"[\s\S]".to_string()
                    } else {
                        NEVER_MATCH_ONE_CHAR.to_string()
                    }
                },
                |class| table_syntax_class_fragment(rendered, class, negated),
            );
    }
    let base = match class_char {
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
    };
    let Some(encoding) = encoding else {
        return base.to_string();
    };
    let Some(class) = class_char.and_then(super::syntax::syntax_class_from_char) else {
        return base.to_string();
    };

    let sentinel_class = |sentinels: &[char]| {
        sentinels
            .iter()
            .map(|ch| format!(r"\x{{{:x}}}", *ch as u32))
            .collect::<String>()
    };
    let all = encoding
        .sentinels
        .iter()
        .map(|entry| entry.sentinel)
        .collect::<Vec<_>>();
    let matching = encoding
        .sentinels
        .iter()
        .filter_map(|entry| {
            let class_matches = super::syntax::syntax_class_from_char(entry.class) == Some(class);
            (class_matches != negated).then_some(entry.sentinel)
        })
        .collect::<Vec<_>>();
    let guarded = format!("(?![{}])(?:{base})", sentinel_class(&all));
    if matching.is_empty() {
        guarded
    } else {
        format!("(?:{guarded}|[{}])", sentinel_class(&matching))
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
        // GNU regex-emacs.c defines ASCII punct as every printable
        // non-alphanumeric byte.  Unicode's Punctuation property alone omits
        // ASCII symbols such as `|', `+', and `$'.
        "punct" => Some(r"\x21-\x2F\x3A-\x40\x5B-\x60\x7B-\x7E\p{Punctuation}"),
        "space" => Some(r"\p{White_Space}"),
        "unibyte" => Some(r"\x00-\x7F\x{E080}-\x{E0FF}"),
        "upper" => Some(r"\p{Uppercase}"),
        "word" => Some(r"\p{Alphabetic}\p{Number}_\x{2620}"),
        "xdigit" => Some("0-9A-Fa-f"),
        _ => None,
    }
}

enum PosixClassParse {
    Literal,
    Named(String),
    Malformed,
}

fn consume_posix_class(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> PosixClassParse {
    if chars.peek() != Some(&':') {
        return PosixClassParse::Literal;
    }
    let mut preview = chars.clone();
    preview.next();
    let mut name = String::new();
    while let Some(ch) = preview.next() {
        if ch == ':' && preview.peek() == Some(&']') {
            preview.next();
            *chars = preview;
            return PosixClassParse::Named(name);
        }
        if ch == ']' {
            // A bare `]' normally means the `[:' opener was literal.  If it
            // is itself followed by `:]', however, GNU treats the complete
            // class-like construct (notably `[[:]:]]') as a malformed POSIX
            // class name rather than as literal bracket members.
            return if preview.next() == Some(':') && preview.peek() == Some(&']') {
                PosixClassParse::Malformed
            } else {
                PosixClassParse::Literal
            };
        }
        name.push(ch);
    }
    PosixClassParse::Literal
}

fn translate_bracket_expression(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    encoding: Option<&SyntaxPropertyEncoding>,
    case_fold: bool,
    interp: Option<&Interpreter>,
    rendered_syntax_classes: Option<&[String; 16]>,
) -> String {
    let mut translated = String::from("[");
    let mut saw_atom = false;
    let mut emitted_atom = false;
    let mut emitted_delegate_atom = false;
    let mut has_table_word_class = false;
    let mut negated = false;
    let mut sentinel_original_members = encoding
        .map(|encoding| vec![false; encoding.sentinels.len()])
        .unwrap_or_default();
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
            let translated = if has_table_word_class {
                let ordinary = if emitted_delegate_atom {
                    if negated {
                        translated.remove(1);
                    }
                    translated.push(']');
                    Some(translated)
                } else {
                    None
                };
                let positive = ordinary.map_or_else(
                    || {
                        rendered_syntax_classes.map_or_else(
                            || REGEX_WORD_CLASS.to_string(),
                            |rendered| {
                                table_syntax_class_fragment(
                                    rendered,
                                    super::syntax::SyntaxClass::Word,
                                    false,
                                )
                            },
                        )
                    },
                    |ordinary| {
                        let word = rendered_syntax_classes.map_or_else(
                            || REGEX_WORD_CLASS.to_string(),
                            |rendered| {
                                table_syntax_class_fragment(
                                    rendered,
                                    super::syntax::SyntaxClass::Word,
                                    false,
                                )
                            },
                        );
                        format!("(?:{ordinary}|{word})")
                    },
                );
                if negated {
                    format!("(?!{positive})[\\s\\S]")
                } else {
                    positive
                }
            } else {
                translated.push(']');
                translated
            };
            return preserve_bracket_membership_for_sentinels(
                translated,
                negated,
                encoding,
                &sentinel_original_members,
            );
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
                record_sentinel_range_members(
                    '-',
                    end,
                    encoding,
                    case_fold,
                    &mut sentinel_original_members,
                );
                *chars = preview;
                saw_atom = true;
                emitted_atom = true;
                emitted_delegate_atom = true;
                continue;
            }
        }
        let Some(atom) = consume_regex_class_atom(chars) else {
            break;
        };
        let mut preview = chars.clone();
        if preview.next() == Some('-')
            && preview.peek().copied() != Some(']')
            && !(atom_is_first && matches!(atom, RegexClassAtom::Char('-')))
            && let Some(end_atom) = consume_regex_class_atom(&mut preview)
            && let (RegexClassAtom::Char(start), RegexClassAtom::Char(end)) = (&atom, &end_atom)
        {
            if let Some(range) = bracket_range_fragment(*start, *end) {
                translated.push_str(&range);
                record_sentinel_range_members(
                    *start,
                    *end,
                    encoding,
                    case_fold,
                    &mut sentinel_original_members,
                );
                emitted_atom = true;
                emitted_delegate_atom = true;
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
        if matches!(&atom, RegexClassAtom::Posix(name) if name == "word") && interp.is_some() {
            has_table_word_class = true;
            record_sentinel_atom_members(
                &atom,
                encoding,
                case_fold,
                interp,
                &mut sentinel_original_members,
            );
            emitted_atom = true;
        } else if let Some(fragment) = regex_class_atom_fragment(&atom) {
            translated.push_str(&fragment);
            record_sentinel_atom_members(
                &atom,
                encoding,
                case_fold,
                interp,
                &mut sentinel_original_members,
            );
            emitted_atom = true;
            emitted_delegate_atom = true;
        } else {
            return "[".into();
        }
        saw_atom = true;
    }

    "[".into()
}

fn record_sentinel_atom_members(
    atom: &RegexClassAtom,
    encoding: Option<&SyntaxPropertyEncoding>,
    case_fold: bool,
    interp: Option<&Interpreter>,
    members: &mut [bool],
) {
    let Some(encoding) = encoding else {
        return;
    };
    for (member, entry) in members.iter_mut().zip(&encoding.sentinels) {
        let matches = match atom {
            // The syntax renderer can emit the sentinel itself for a
            // matching `\\sX' atom.  Source patterns cannot contain these
            // dynamically selected sentinels.
            RegexClassAtom::Char(ch) => {
                chars_equal_for_regexp(entry.original, *ch, case_fold) || entry.sentinel == *ch
            }
            RegexClassAtom::Posix(class) => {
                (if class == "word" && interp.is_some() {
                    entry.class == 'w'
                } else {
                    skip_char_matches_class(entry.original, class)
                }) || (case_fold
                    && entry
                        .original
                        .to_lowercase()
                        .chain(entry.original.to_uppercase())
                        .any(|candidate| {
                            if class == "word" && interp.is_some() {
                                entry.class == 'w'
                            } else {
                                skip_char_matches_class(candidate, class)
                            }
                        }))
            }
        };
        *member |= matches;
    }
}

fn bracket_range_contains(start: char, end: char, candidate: char) -> bool {
    if (start as u32) <= 0x7F && (end as u32) <= 0x7F && start <= end {
        return start <= candidate && candidate <= end;
    }
    match (
        raw_byte_from_regex_char(start),
        raw_byte_from_regex_char(end),
        raw_byte_from_regex_char(candidate),
    ) {
        (Some(start), Some(end), Some(candidate)) if start <= end => {
            start <= candidate && candidate <= end
        }
        (None, Some(end), candidate_raw) if (start as u32) <= 0x7F => {
            ((start as u32)..=0x7F).contains(&(candidate as u32))
                || candidate_raw.is_some_and(|candidate| (0x80..=end).contains(&candidate))
        }
        (None, None, None) if start <= end => start <= candidate && candidate <= end,
        _ => false,
    }
}

fn record_sentinel_range_members(
    start: char,
    end: char,
    encoding: Option<&SyntaxPropertyEncoding>,
    case_fold: bool,
    members: &mut [bool],
) {
    let Some(encoding) = encoding else {
        return;
    };
    for (member, entry) in members.iter_mut().zip(&encoding.sentinels) {
        *member |= bracket_range_contains(start, end, entry.original)
            || (case_fold
                && entry
                    .original
                    .to_lowercase()
                    .chain(entry.original.to_uppercase())
                    .any(|candidate| bracket_range_contains(start, end, candidate)));
    }
}

fn preserve_bracket_membership_for_sentinels(
    translated: String,
    negated: bool,
    encoding: Option<&SyntaxPropertyEncoding>,
    original_members: &[bool],
) -> String {
    let Some(encoding) = encoding else {
        return translated;
    };
    let sentinel_class = |sentinels: &[char]| {
        sentinels
            .iter()
            .map(|ch| format!(r"\x{{{:x}}}", *ch as u32))
            .collect::<String>()
    };
    let all = encoding
        .sentinels
        .iter()
        .map(|entry| entry.sentinel)
        .collect::<Vec<_>>();
    let matching = encoding
        .sentinels
        .iter()
        .zip(original_members)
        .filter_map(|(entry, member)| (*member != negated).then_some(entry.sentinel))
        .collect::<Vec<_>>();
    let guarded = format!("(?![{}]){translated}", sentinel_class(&all));
    if matching.is_empty() {
        guarded
    } else {
        format!("(?:{guarded}|[{}])", sentinel_class(&matching))
    }
}

fn consume_regex_class_atom(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<RegexClassAtom> {
    match chars.next()? {
        '[' => Some(match consume_posix_class(chars) {
            PosixClassParse::Named(name) => RegexClassAtom::Posix(name),
            PosixClassParse::Literal | PosixClassParse::Malformed => RegexClassAtom::Char('['),
        }),
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
    let message: String = message.into();
    LispError::SignalValue(Value::list([
        Value::Symbol("invalid-regexp".into()),
        Value::String(message.into()),
    ]))
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
                '[' => match consume_posix_class(&mut chars) {
                    PosixClassParse::Named(name) if regex_posix_class_fragment(&name).is_none() => {
                        return Err(invalid_regexp_error("Invalid character class name"));
                    }
                    PosixClassParse::Malformed => {
                        return Err(invalid_regexp_error("Invalid character class name"));
                    }
                    PosixClassParse::Literal | PosixClassParse::Named(_) => {}
                },
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
                Some('c' | 'C' | 's' | 'S') => {
                    if chars.next().is_none() {
                        return Err(invalid_regexp_error("Premature end of regular expression"));
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
    linear_boundary_prefilter: Option<LinearBoundaryPrefilter>,
    capture_mapping: Vec<usize>,
}

#[derive(Clone)]
struct LinearBoundaryPrefilter {
    regex: Regex,
    exact_at_start: FancyRegex,
    exact_after_one_char: FancyRegex,
}

impl CompiledElispRegex {
    pub(super) fn captures<'h>(
        &self,
        haystack: &'h str,
    ) -> Result<Option<fancy_regex::Captures<'h>>, fancy_regex::Error> {
        self.captures_from_pos(haystack, 0)
    }

    pub(super) fn captures_from_pos<'h>(
        &self,
        haystack: &'h str,
        start: usize,
    ) -> Result<Option<fancy_regex::Captures<'h>>, fancy_regex::Error> {
        let Some(prefilter) = &self.linear_boundary_prefilter else {
            return self.regex.captures_from_pos(haystack, start);
        };

        // Lookaround makes fancy-regex try the exact expression at every
        // character.  GNU boundary-heavy searches (notably NEWS scans) are
        // linear instead: first find a plausible textual match, then test
        // the zero-width syntax assertions at that one position.  Advance
        // one character from a rejected candidate so overlapping matches
        // such as the second `aa' in `aaa ' remain visible.
        let mut next_candidate = start.min(haystack.len());
        while next_candidate <= haystack.len() {
            let Some(candidate) = prefilter.regex.find_at(haystack, next_candidate) else {
                return Ok(None);
            };
            let candidate_start = candidate.start();
            let exact = if candidate_start == 0 {
                prefilter.exact_at_start.is_match(haystack)?
            } else {
                let previous_start = haystack[..candidate_start]
                    .char_indices()
                    .next_back()
                    .map(|(index, _)| index)
                    .unwrap_or(0);
                prefilter
                    .exact_after_one_char
                    .is_match(&haystack[previous_start..])?
            };
            if exact
                && let Some(captures) = self.regex.captures_from_pos(haystack, candidate_start)?
                && captures
                    .get(0)
                    .is_some_and(|matched| matched.start() == candidate_start)
            {
                return Ok(Some(captures));
            }

            let Some(next) = haystack[candidate_start..].chars().next() else {
                return Ok(None);
            };
            next_candidate = candidate_start + next.len_utf8();
        }
        Ok(None)
    }

    pub(super) fn is_match(&self, haystack: &str) -> Result<bool, fancy_regex::Error> {
        Ok(self.captures(haystack)?.is_some())
    }

    pub(super) fn capture_mapping(&self) -> &[usize] {
        &self.capture_mapping
    }
}

fn build_fancy_regex(rendered: &str) -> Result<FancyRegex, fancy_regex::Error> {
    // Bounded repetitions over Unicode classes (cc-mode uses `\{,1000\}'
    // on symbol-char classes) overflow the delegate's default 10MB
    // compiled-program budget; GNU regexps have no such limit.  The
    // backtrack budget likewise must not fail searches GNU completes:
    // help-fns scans multi-megabyte NEWS files with alternation patterns
    // that exceed the delegate's default one-million-step cap.
    fancy_regex::RegexBuilder::new(rendered)
        .delegate_size_limit(512 * 1024 * 1024)
        .backtrack_limit(u32::MAX as usize)
        .build()
}

fn linear_boundary_prefilter(rendered: &str) -> Option<LinearBoundaryPrefilter> {
    let word_start = format!("(?<!{REGEX_WORD_CLASS})(?={REGEX_WORD_CLASS})");
    let word_end = format!("(?<={REGEX_WORD_CLASS})(?!{REGEX_WORD_CLASS})");
    let symbol_start = format!("(?<!{REGEX_SYMBOL_CLASS})(?={REGEX_SYMBOL_CLASS})");
    let symbol_end = format!("(?<={REGEX_SYMBOL_CLASS})(?!{REGEX_SYMBOL_CLASS})");
    let mut coarse = rendered.to_string();
    coarse = coarse.replace(r"(?=[^\x00-\x7F])", "(?:)");
    for assertion in [word_start, word_end, symbol_start, symbol_end] {
        coarse = coarse.replace(&assertion, "(?:)");
    }
    if coarse == rendered {
        return None;
    }

    let regex = regex::RegexBuilder::new(&coarse)
        .size_limit(512 * 1024 * 1024)
        .build()
        .ok()?;
    let exact_at_start = build_fancy_regex(&format!(r"\A(?:{rendered})")).ok()?;
    // Include exactly one preceding character in the anchored probe so a
    // boundary at the beginning of the match sees its real left context.
    let exact_after_one_char = build_fancy_regex(&format!(r"\A(?:[\s\S])(?:{rendered})")).ok()?;
    Some(LinearBoundaryPrefilter {
        regex,
        exact_at_start,
        exact_after_one_char,
    })
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct CompiledElispRegexKey {
    pattern: String,
    syntax_property_sentinels: Vec<SyntaxPropertySentinel>,
    // search.c compile_pattern re-checks its cached entry with EQ against
    // the current syntax table before reuse; the analog here is the pair
    // of table identities plus two write generations, so a hit costs a
    // hash instead of re-running the whole translation.  The char-table
    // generation observes writes through the table door; the definition
    // generation observes interior mutation of shared structure (setcar
    // on a cons stored as a table entry bumps it), so no route to
    // changing what a class renders as escapes the key.
    syntax_table_id: u64,
    category_table_id: u64,
    char_table_generation: u64,
    definition_generation: u64,
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
    #[cfg(test)]
    static REGEXP_SYNTAX_CLASS_RENDER_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_regexp_syntax_class_render_count() {
    REGEXP_SYNTAX_CLASS_RENDER_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(super) fn regexp_syntax_class_render_count() -> usize {
    REGEXP_SYNTAX_CLASS_RENDER_COUNT.with(std::cell::Cell::get)
}

fn encode_syntax_property_haystack(
    interp: &Interpreter,
    env: &Env,
    start: usize,
    haystack: &str,
    pattern: &str,
) -> Option<SyntaxPropertyEncoding> {
    if !pattern_depends_on_syntax_table(pattern)
        || !interp
            .lookup_var("parse-sexp-lookup-properties", env)
            .is_some_and(|value| value.is_truthy())
    {
        return None;
    }

    let mut forbidden = haystack
        .chars()
        .chain(pattern.chars())
        .collect::<HashSet<_>>();
    let mut next_sentinel = 0xF0100u32;
    let mut sentinels = Vec::<SyntaxPropertySentinel>::new();
    let mut encoded = String::with_capacity(haystack.len());
    // One scan for the whole walk: per-character cost drops to memoized
    // table lookups plus interval-crossing property refreshes.
    let mut scan = super::syntax::SyntaxScan::new(interp, interp.current_syntax_table_id());
    for (offset, original) in haystack.chars().enumerate() {
        let position = start + offset;
        let Some((table_class, effective_class)) =
            super::syntax::syntax_class_chars_with_scan(interp, &mut scan, position)
        else {
            encoded.push(original);
            continue;
        };
        if table_class == effective_class {
            encoded.push(original);
            continue;
        }
        let sentinel = sentinels
            .iter()
            .find(|entry| entry.original == original && entry.class == effective_class)
            .map(|entry| entry.sentinel)
            .unwrap_or_else(|| {
                loop {
                    let candidate = char::from_u32(next_sentinel)
                        .expect("plane-15 private-use sentinel is a valid character");
                    next_sentinel += 1;
                    if !forbidden.contains(&candidate) {
                        forbidden.insert(candidate);
                        sentinels.push(SyntaxPropertySentinel {
                            original,
                            class: effective_class,
                            sentinel: candidate,
                        });
                        break candidate;
                    }
                }
            });
        encoded.push(sentinel);
    }
    (!sentinels.is_empty()).then_some(SyntaxPropertyEncoding {
        haystack: encoded,
        sentinels,
    })
}

/// Project a buffer slice into the scalar representation used by the regexp
/// engine while preserving one scalar per buffer character.
///
/// GNU regexp byte escapes match bytes in a unibyte buffer and the equivalent
/// eight-bit characters in a multibyte buffer.  Emaxx represents those bytes
/// with private-use scalars; normalize both buffer representations here so
/// the regexp translator remains the single pattern-grammar owner and match
/// positions stay identical to buffer positions.
#[derive(Clone, PartialEq, Eq)]
struct RegexpHaystackKey {
    buffer_id: u64,
    start: usize,
    end: usize,
    chars_modiff: crate::buffer::ModCount,
    multibyte: bool,
}

const REGEXP_HAYSTACK_CACHE_LIMIT: usize = 8;

thread_local! {
    static REGEXP_HAYSTACK_CACHE: RefCell<Vec<(RegexpHaystackKey, std::rc::Rc<str>)>> =
        const { RefCell::new(Vec::new()) };
}

// GNU's re_search runs directly over the buffer text and never copies it;
// this runtime's regex engine needs one contiguous string, so the mapped
// haystack is built once per (buffer, range, text-modification) state and
// shared.  CHARS_MODIFF only advances on text changes, and the mapping
// below depends on nothing but the characters and the multibyte flag, so
// a hit hands back byte-identical content to a fresh build.
fn buffer_regexp_haystack(
    interp: &Interpreter,
    start: usize,
    end: usize,
) -> Result<std::rc::Rc<str>, LispError> {
    let key = RegexpHaystackKey {
        buffer_id: interp.current_buffer_id(),
        start,
        end,
        chars_modiff: interp.buffer.chars_modification_count(),
        multibyte: interp.buffer.is_multibyte(),
    };
    let cached = REGEXP_HAYSTACK_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(index) = cache.iter().position(|(entry, _)| *entry == key) {
            let hit = cache.remove(index);
            let text = hit.1.clone();
            cache.push(hit);
            Some(text)
        } else {
            None
        }
    });
    if let Some(text) = cached {
        return Ok(text);
    }
    let built: std::rc::Rc<str> = build_buffer_regexp_haystack(interp, start, end)?.into();
    REGEXP_HAYSTACK_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= REGEXP_HAYSTACK_CACHE_LIMIT {
            cache.remove(0);
        }
        cache.push((key, built.clone()));
    });
    Ok(built)
}

fn build_buffer_regexp_haystack(
    interp: &Interpreter,
    start: usize,
    end: usize,
) -> Result<String, LispError> {
    let text = interp
        .buffer
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let extended_chars = interp.buffer.substring_extended_chars(start, end);
    if interp.buffer.is_multibyte() && extended_chars.is_empty() {
        return Ok(text);
    }

    let mut raw_codes = vec![None; text.chars().count()];
    for (offset, code) in extended_chars {
        raw_codes[offset] = Some(i64::from(code));
    }

    Ok(text
        .chars()
        .enumerate()
        .map(|(offset, original)| {
            let public = raw_codes[offset].unwrap_or(original as i64);
            if (RAW_BYTE8_BASE as i64..=RAW_BYTE8_BASE as i64 + 0xFF).contains(&public) {
                raw_byte_regex_char((public - RAW_BYTE8_BASE as i64) as u8)
            } else if !interp.buffer.is_multibyte()
                && raw_byte_from_regex_char(original).is_none()
                && (0x80..=0xFF).contains(&public)
            {
                raw_byte_regex_char(public as u8)
            } else {
                original
            }
        })
        .collect())
}

pub(super) fn compile_elisp_regex(
    interp: &Interpreter,
    pattern: &StringLike,
    env: &Env,
    point_assertion: &str,
    at_absolute_start: bool,
) -> Result<CompiledElispRegex, LispError> {
    compile_elisp_regex_with_syntax_properties(
        interp,
        pattern,
        env,
        point_assertion,
        at_absolute_start,
        None,
        RegexpCategoryScope::Standard,
    )
}

fn compile_elisp_regex_with_syntax_properties(
    interp: &Interpreter,
    pattern: &StringLike,
    env: &Env,
    point_assertion: &str,
    at_absolute_start: bool,
    encoding: Option<&SyntaxPropertyEncoding>,
    category_scope: RegexpCategoryScope,
) -> Result<CompiledElispRegex, LispError> {
    let pattern_text = pattern.text.clone();
    let case_fold = interp
        .lookup_var("case-fold-search", env)
        .is_some_and(|value| value.is_truthy());
    let category_table_id = category_scope.table_id(interp);
    // Translation is the single owner of Emacs regexp grammar.  A pattern
    // that depends on mutable runtime tables is keyed by the identity of
    // those tables plus the shared write generation (a table-independent
    // pattern keys the same either way), so a regexp compiled under one
    // syntax or category table can never leak into another table's search
    // -- and a cache hit no longer pays the translation it cached.
    let depends_on_tables = pattern_depends_on_syntax_table(&pattern_text)
        || pattern_depends_on_category_table(&pattern_text);
    let key = CompiledElispRegexKey {
        pattern: pattern_text.clone(),
        // Bracket expressions, line anchors, dot, and ordinary literals all
        // depend on what each encoded character originally was.  The same
        // sentinel scalar is intentionally reused by separate searches, so
        // omitting this mapping lets one buffer poison another's compiled
        // regexp cache entry.
        syntax_property_sentinels: encoding
            .map(|encoding| encoding.sentinels.clone())
            .unwrap_or_default(),
        syntax_table_id: if depends_on_tables {
            interp.current_syntax_table_id()
        } else {
            0
        },
        category_table_id: if depends_on_tables {
            category_table_id.unwrap_or(0)
        } else {
            0
        },
        // case-folded rendering reads the case tables, so it shares the
        // generation guard.
        char_table_generation: if depends_on_tables || case_fold {
            interp.char_table_generation()
        } else {
            0
        },
        definition_generation: if depends_on_tables {
            interp.current_definition_generation()
        } else {
            0
        },
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
        encoding,
        case_fold,
        Some(interp),
        category_table_id,
    );
    let rendered = if case_fold {
        format!("(?mi:{translated})")
    } else {
        format!("(?m:{translated})")
    };
    let compiled = CompiledElispRegex {
        regex: build_fancy_regex(&rendered)
            .map_err(|error| invalid_regexp_error(error.to_string()))?,
        linear_boundary_prefilter: linear_boundary_prefilter(&rendered),
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
        extended_chars: pattern.extended_chars.clone(),
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
    let ascii = haystack.is_ascii();
    for index in 0..captures.len() {
        let Some(matched) = captures.get(index) else {
            continue;
        };
        let start = start_pos
            + if ascii {
                matched.start()
            } else {
                haystack[..matched.start()].chars().count()
            };
        let end = start_pos
            + if ascii {
                matched.end()
            } else {
                haystack[..matched.end()].chars().count()
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

#[derive(Clone, Copy)]
enum SearchPointBoundary {
    None,
    Start,
    End,
    EndBeforeTrailingContext,
}

impl SearchPointBoundary {
    fn ordinary_assertion(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Start => r"\A",
            Self::End => r"\z",
            // Backward searches retain the character immediately after the
            // search point so `$' and boundary assertions see real buffer
            // context.  `\=' still denotes the search point, one scalar
            // before the delegate haystack ends.
            Self::EndBeforeTrailingContext => r"(?=[\s\S]\z)",
        }
    }

    fn boundary_byte(self, haystack: &str) -> Option<usize> {
        match self {
            Self::None => None,
            Self::Start => Some(0),
            Self::End => Some(haystack.len()),
            Self::EndBeforeTrailingContext => haystack
                .char_indices()
                .next_back()
                .map(|(byte, _)| byte)
                .or(Some(0)),
        }
    }

    fn candidate_assertion(
        self,
        match_start_byte: usize,
        candidate_end_byte: usize,
        boundary_byte: Option<usize>,
    ) -> &'static str {
        match self {
            Self::None => "",
            Self::Start if Some(match_start_byte) == boundary_byte => r"\A",
            Self::End | Self::EndBeforeTrailingContext
                if Some(candidate_end_byte) == boundary_byte =>
            {
                r"\z"
            }
            Self::Start | Self::End | Self::EndBeforeTrailingContext => r"(?!)",
        }
    }
}

struct PosixMatch {
    start_byte: usize,
    end_byte: usize,
    start_position: usize,
    end_position: usize,
    match_data: Vec<Option<(usize, usize)>>,
}

struct PosixMatchContext<'a> {
    position_base: usize,
    point_boundary: SearchPointBoundary,
    haystack_at_absolute_start: bool,
    category_scope: RegexpCategoryScope,
    env: &'a Env,
}

/// Find the ordinary match's earliest start, then require the regexp to
/// consume the longest possible prefix at that start.  Appending only the
/// end anchor is intentional: prepending an Emacs `\`` anchor would confuse
/// a candidate slice with the actual beginning of the string or buffer.
fn posix_longest_match(
    interp: &Interpreter,
    pattern: &StringLike,
    haystack: &str,
    search_offset: usize,
    context: PosixMatchContext<'_>,
) -> Result<Option<PosixMatch>, LispError> {
    let ordinary = compile_elisp_regex_with_syntax_properties(
        interp,
        pattern,
        context.env,
        context.point_boundary.ordinary_assertion(),
        context.haystack_at_absolute_start,
        None,
        context.category_scope,
    )?;
    let end_anchored_pattern = StringLike {
        text: format!(r"\(?:{}\)\'", pattern.text),
        props: pattern.props.clone(),
        multibyte: pattern.multibyte,
        extended_chars: pattern.extended_chars.clone(),
    };
    let boundary_byte = context.point_boundary.boundary_byte(haystack);
    let maximum_match_end = match context.point_boundary {
        SearchPointBoundary::End | SearchPointBoundary::EndBeforeTrailingContext => {
            boundary_byte.unwrap_or(haystack.len())
        }
        SearchPointBoundary::None | SearchPointBoundary::Start => haystack.len(),
    };
    let mut ordinary_search_offset = search_offset;

    while ordinary_search_offset <= maximum_match_end {
        let Some(first) = ordinary
            .captures_from_pos(haystack, ordinary_search_offset)
            .map_err(|error| LispError::Signal(error.to_string()))?
        else {
            return Ok(None);
        };
        let Some(first_match) = first.get(0) else {
            return Ok(None);
        };
        let match_start_byte = first_match.start();
        if match_start_byte > maximum_match_end {
            return Ok(None);
        }
        let match_start_chars = haystack[..match_start_byte].chars().count();
        let remainder = &haystack[match_start_byte..maximum_match_end];
        let mut candidate_ends = std::iter::once(0)
            .chain(
                remainder
                    .char_indices()
                    .map(|(byte, ch)| byte + ch.len_utf8()),
            )
            .collect::<Vec<_>>();
        candidate_ends.reverse();

        for candidate_end in candidate_ends {
            let absolute_candidate_end = match_start_byte + candidate_end;
            let candidate = &remainder[..candidate_end];
            let exact = compile_elisp_regex_with_syntax_properties(
                interp,
                &end_anchored_pattern,
                context.env,
                context.point_boundary.candidate_assertion(
                    match_start_byte,
                    absolute_candidate_end,
                    boundary_byte,
                ),
                context.haystack_at_absolute_start && match_start_byte == 0,
                None,
                context.category_scope,
            )?;
            let Some(captures) = exact
                .captures(candidate)
                .map_err(|error| LispError::Signal(error.to_string()))?
            else {
                continue;
            };
            let Some(matched) = captures.get(0) else {
                continue;
            };
            if matched.start() != 0 || matched.end() != candidate.len() {
                continue;
            }
            let match_position_base = context.position_base + match_start_chars;
            let match_data = match_data_from_captures(
                match_position_base,
                candidate,
                &captures,
                exact.capture_mapping(),
            );
            let Some((start_position, end_position)) = match_data.first().and_then(|entry| *entry)
            else {
                continue;
            };
            return Ok(Some(PosixMatch {
                start_byte: match_start_byte,
                end_byte: absolute_candidate_end,
                start_position,
                end_position,
                match_data,
            }));
        }

        // Retaining right context can expose an ordinary match that crosses
        // the backward-search point.  It is not a candidate, but a later
        // start may still produce one, so advance by one scalar and retry.
        let Some(next) = haystack[match_start_byte..].chars().next() else {
            return Ok(None);
        };
        ordinary_search_offset = match_start_byte + next.len_utf8();
    }

    Ok(None)
}

pub(super) fn string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
    update_match_data: bool,
) -> Result<Value, LispError> {
    let max_args = if update_match_data { 4 } else { 3 };
    if args.len() < 2 || args.len() > max_args {
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
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0].clone()))?;
    let haystack = string_like(&args[1])
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[1].clone()))?;
    // The overwhelmingly common no-START path searches the original string.
    // Counting and copying the full haystack made it O(n) before the regex
    // engine even ran (particularly painful for large buffers/Unicode data).
    let start = if let Some(start) = args.get(2) {
        normalize_string_index(Some(start), 0, haystack.text.chars().count() as i64)? as usize
    } else {
        0
    };
    // GNU searches the whole string from START: `^' matches only at the
    // string's beginning or after a newline, never bare at START, and
    // `\\`' means the true string start.  Slicing the tail off made the
    // slice boundary a bogus beginning-of-line (org-persist's
    // `(replace-regexp-in-string "^.." ...)' split every two characters).
    let text = haystack.text.as_str();
    let byte_start = if start == 0 {
        0
    } else if text.is_ascii() {
        start
    } else {
        text.char_indices()
            .nth(start)
            .map(|(byte, _)| byte)
            .unwrap_or(text.len())
    };
    let regex = compile_elisp_regex(interp, &pattern, env, "", true)?;
    let captures = regex
        .captures_from_pos(text, byte_start)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if let Some(captures) = captures
        && let Some(matched) = captures.get(0)
    {
        let match_start = if text.is_ascii() {
            matched.start()
        } else {
            text[..matched.start()].chars().count()
        };
        if update_match_data && !args.get(3).is_some_and(Value::is_truthy) {
            set_match_data(interp, 0, text, &captures, regex.capture_mapping(), None);
        }
        Ok(Value::Integer(match_start as i64))
    } else {
        Ok(Value::Nil)
    }
}

pub(super) fn posix_string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "posix-string-match".into(),
            args.len(),
        ));
    }
    let pattern = string_like(&args[0])
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0].clone()))?;
    let haystack = string_like(&args[1])
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[1].clone()))?;
    let start = if let Some(start) = args.get(2) {
        normalize_string_index(Some(start), 0, haystack.text.chars().count() as i64)? as usize
    } else {
        0
    };
    let text = haystack.text.as_str();
    let byte_start = if start == 0 {
        0
    } else if text.is_ascii() {
        start
    } else {
        text.char_indices()
            .nth(start)
            .map(|(byte, _)| byte)
            .unwrap_or(text.len())
    };

    if let Some(selected) = posix_longest_match(
        interp,
        &pattern,
        text,
        byte_start,
        PosixMatchContext {
            position_base: 0,
            point_boundary: SearchPointBoundary::None,
            haystack_at_absolute_start: true,
            category_scope: RegexpCategoryScope::Standard,
            env,
        },
    )? {
        if !args.get(3).is_some_and(Value::is_truthy) {
            interp.last_match_data = Some(selected.match_data);
            interp.last_match_data_buffer_id = None;
        }
        return Ok(Value::Integer(selected.start_position as i64));
    }

    Ok(Value::Nil)
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
    let skipped = interp
        .buffer
        .skip_forward_while(limit, |ch| skip_char_matches_spec(ch, &spec));
    Ok(Value::Integer(skipped as i64))
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
    let skipped = interp
        .buffer
        .skip_backward_while(limit, |ch| skip_char_matches_spec(ch, &spec));
    Ok(Value::Integer(-(skipped as i64)))
}

pub(super) fn looking_at_impl(
    interp: &mut Interpreter,
    pattern_value: &Value,
    posix: bool,
    update_match_data: bool,
    env: &Env,
) -> Result<Value, LispError> {
    let pattern = string_like(pattern_value)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), pattern_value.clone()))?;
    let pattern = regex_pattern_with_search_spaces(interp, &pattern, env);
    let pos = interp.buffer.point();
    let tail = buffer_regexp_haystack(interp, pos, interp.buffer.point_max())?;
    if posix {
        let Some(selected) = posix_longest_match(
            interp,
            &pattern,
            &tail,
            0,
            PosixMatchContext {
                position_base: pos,
                point_boundary: SearchPointBoundary::Start,
                haystack_at_absolute_start: pos == interp.buffer.point_min(),
                category_scope: RegexpCategoryScope::CurrentBuffer,
                env,
            },
        )?
        else {
            return Ok(Value::Nil);
        };
        if selected.start_byte != 0 {
            return Ok(Value::Nil);
        }
        if update_match_data {
            interp.last_match_data = Some(selected.match_data);
            interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
        }
        return Ok(Value::T);
    }

    // A regexp anchored at point still needs the character immediately to
    // its left in order to evaluate zero-width word/symbol-end assertions.
    // Keep exactly that one character as context and translate `\=' to the
    // boundary after it.  This also preserves `\=' in the middle of a
    // pattern: it becomes false after the regexp has consumed anything.
    let haystack_start = pos.saturating_sub(1).max(interp.buffer.point_min());
    let haystack = buffer_regexp_haystack(interp, haystack_start, interp.buffer.point_max())?;
    let has_left_context = haystack_start < pos;
    let point_assertion = if has_left_context {
        r"(?<=\A[\s\S])"
    } else {
        r"\A"
    };
    let syntax_encoding =
        encode_syntax_property_haystack(interp, env, haystack_start, &haystack, &pattern.text);
    let regex = compile_elisp_regex_with_syntax_properties(
        interp,
        &pattern,
        env,
        point_assertion,
        pos == interp.buffer.point_min(),
        syntax_encoding.as_ref(),
        RegexpCategoryScope::CurrentBuffer,
    )?;
    let haystack = syntax_encoding
        .as_ref()
        .map(|encoding| std::rc::Rc::<str>::from(encoding.haystack.as_str()))
        .unwrap_or(haystack);
    // The syntax-property encoder preserves one Unicode scalar per buffer
    // character, but a sentinel can occupy more UTF-8 bytes than the ASCII
    // character it replaces.  Derive the regex engine's byte offset from the
    // final haystack, never from the pre-encoding string.
    let search_offset = if has_left_context {
        haystack.chars().next().map(char::len_utf8).unwrap_or(0)
    } else {
        0
    };
    if let Some(captures) = regex
        .captures_from_pos(&haystack, search_offset)
        .map_err(|error| LispError::Signal(error.to_string()))?
        && let Some(matched) = captures.get(0)
        && matched.start() == search_offset
    {
        if update_match_data {
            set_match_data(
                interp,
                haystack_start,
                &haystack,
                &captures,
                regex.capture_mapping(),
                Some(interp.current_buffer_id()),
            );
        }
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
    posix: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            if posix && forward {
                "posix-search-forward".into()
            } else if posix {
                "posix-search-backward".into()
            } else if forward {
                "re-search-forward".into()
            } else {
                "re-search-backward".into()
            },
            args.len(),
        ));
    }
    let pattern = string_like(&args[0])
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0].clone()))?;
    let pattern = regex_pattern_with_search_spaces(interp, &pattern, env);
    let noerror = args.get(2).is_some_and(Value::is_truthy);
    let move_on_failure = search_noerror_moves(args.get(2));
    let original_point = interp.buffer.point();
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
            return buffer_regex_search(interp, &backward_args, env, false, posix);
        }
        if let Some((line_anchored, negated, syntax_class)) =
            single_syntax_class_pattern(&pattern.text)
        {
            for _ in 0..count {
                let Some(match_start) = next_single_syntax_class_match(
                    interp,
                    env,
                    interp.buffer.point(),
                    limit,
                    line_anchored,
                    negated,
                    syntax_class,
                ) else {
                    return buffer_regex_search_failure(
                        interp,
                        &pattern.text,
                        original_point,
                        limit,
                        noerror,
                        move_on_failure,
                    );
                };
                let match_end = match_start + 1;
                interp.last_match_data = Some(vec![Some((match_start, match_end))]);
                interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                interp.buffer.goto_char(match_end);
            }
            return Ok(Value::Integer(interp.buffer.point() as i64));
        }
        if limit < start {
            return buffer_regex_search_failure(
                interp,
                &pattern.text,
                original_point,
                limit,
                noerror,
                move_on_failure,
            );
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
        let haystack = buffer_regexp_haystack(interp, haystack_start, limit)?;
        let syntax_encoding = (!posix)
            .then(|| {
                encode_syntax_property_haystack(
                    interp,
                    env,
                    haystack_start,
                    &haystack,
                    &pattern.text,
                )
            })
            .flatten();
        let regex = compile_elisp_regex_with_syntax_properties(
            interp,
            &pattern,
            env,
            r"\A",
            interp.buffer.point() == interp.buffer.point_min(),
            syntax_encoding.as_ref(),
            RegexpCategoryScope::CurrentBuffer,
        )?;
        let haystack = syntax_encoding
            .as_ref()
            .map(|encoding| std::rc::Rc::<str>::from(encoding.haystack.as_str()))
            .unwrap_or(haystack);
        // `captures_from_pos' takes a BYTE offset; positions are chars.
        let start_chars = start.saturating_sub(haystack_start);
        let mut search_offset = haystack
            .char_indices()
            .nth(start_chars)
            .map(|(byte, _)| byte)
            .unwrap_or(haystack.len());
        for _ in 0..count {
            if posix {
                let Some(selected) = posix_longest_match(
                    interp,
                    &pattern,
                    &haystack,
                    search_offset,
                    PosixMatchContext {
                        position_base: haystack_start,
                        point_boundary: SearchPointBoundary::Start,
                        haystack_at_absolute_start: haystack_start == interp.buffer.point_min(),
                        category_scope: RegexpCategoryScope::CurrentBuffer,
                        env,
                    },
                )?
                else {
                    return buffer_regex_search_failure(
                        interp,
                        &pattern.text,
                        original_point,
                        limit,
                        noerror,
                        move_on_failure,
                    );
                };
                interp.last_match_data = Some(selected.match_data);
                interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                interp.buffer.goto_char(selected.end_position);
                search_offset = if selected.end_byte > search_offset {
                    selected.end_byte
                } else {
                    haystack[search_offset..]
                        .char_indices()
                        .nth(1)
                        .map(|(offset, _)| search_offset + offset)
                        .unwrap_or(haystack.len())
                };
                continue;
            }
            let Some(captures) = regex
                .captures_from_pos(&haystack, search_offset)
                .map_err(|error| LispError::Signal(error.to_string()))?
            else {
                return buffer_regex_search_failure(
                    interp,
                    &pattern.text,
                    original_point,
                    limit,
                    noerror,
                    move_on_failure,
                );
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
            return buffer_regex_search(interp, &forward_args, env, true, posix);
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
            return buffer_regex_search_failure(
                interp,
                &pattern.text,
                original_point,
                limit,
                noerror,
                move_on_failure,
            );
        }
        if let Some((line_anchored, negated, syntax_class)) =
            single_syntax_class_pattern(&pattern.text)
        {
            for _ in 0..count {
                let Some(match_start) = previous_single_syntax_class_match(
                    interp,
                    env,
                    interp.buffer.point(),
                    limit,
                    line_anchored,
                    negated,
                    syntax_class,
                ) else {
                    return buffer_regex_search_failure(
                        interp,
                        &pattern.text,
                        original_point,
                        limit,
                        noerror,
                        move_on_failure,
                    );
                };
                interp.last_match_data = Some(vec![Some((match_start, match_start + 1))]);
                interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                interp.buffer.goto_char(match_start);
            }
            return Ok(Value::Integer(interp.buffer.point() as i64));
        }
        for _ in 0..count {
            let search_point = interp.buffer.point();
            let absolute_start = interp.buffer.point_min();
            // A backward match must end at or before SEARCH_POINT, but the
            // regexp engine still needs the following character to decide
            // line-end, word/symbol-boundary, and absolute-end assertions.
            // Truncating the delegate haystack at point made its artificial
            // end look like a real `$' (for example, a nonblank line looked
            // blank when point was at its beginning).
            let context_end = if search_point < interp.buffer.point_max() {
                search_point + 1
            } else {
                search_point
            };
            let has_trailing_context = context_end > search_point;
            let prefix = buffer_regexp_haystack(interp, absolute_start, context_end)?;
            let syntax_encoding = (!posix)
                .then(|| {
                    encode_syntax_property_haystack(
                        interp,
                        env,
                        absolute_start,
                        &prefix,
                        &pattern.text,
                    )
                })
                .flatten();
            let point_boundary = if has_trailing_context {
                SearchPointBoundary::EndBeforeTrailingContext
            } else {
                SearchPointBoundary::End
            };
            let regex = compile_elisp_regex_with_syntax_properties(
                interp,
                &pattern,
                env,
                point_boundary.ordinary_assertion(),
                true,
                syntax_encoding.as_ref(),
                RegexpCategoryScope::CurrentBuffer,
            )?;
            let prefix = syntax_encoding
                .as_ref()
                .map(|encoding| std::rc::Rc::<str>::from(encoding.haystack.as_str()))
                .unwrap_or(prefix);
            let empty_line_pattern = pattern.text == "^$";
            if empty_line_pattern
                && let Some(pos) =
                    last_empty_line_match_position(absolute_start, &prefix, limit, search_point)
            {
                interp.last_match_data = Some(vec![Some((pos, pos))]);
                interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                interp.buffer.goto_char(pos);
                continue;
            }
            if posix {
                let mut best_match = None;
                let mut search_byte = 0usize;
                while search_byte <= prefix.len() {
                    let Some(selected) = posix_longest_match(
                        interp,
                        &pattern,
                        &prefix,
                        search_byte,
                        PosixMatchContext {
                            position_base: absolute_start,
                            point_boundary,
                            haystack_at_absolute_start: true,
                            category_scope: RegexpCategoryScope::CurrentBuffer,
                            env,
                        },
                    )?
                    else {
                        break;
                    };
                    let selected_start_byte = selected.start_byte;
                    if selected.start_position >= limit
                        && selected.end_position <= search_point
                        && best_match.as_ref().is_none_or(|best: &PosixMatch| {
                            selected.start_position > best.start_position
                                || (selected.start_position == best.start_position
                                    && selected.end_position > best.end_position)
                        })
                    {
                        best_match = Some(selected);
                    }
                    let Some(next) = prefix[selected_start_byte..].chars().next() else {
                        break;
                    };
                    search_byte = selected_start_byte + next.len_utf8();
                }
                if let Some(selected) = best_match {
                    interp.last_match_data = Some(selected.match_data);
                    interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
                    interp.buffer.goto_char(selected.start_position);
                    continue;
                }
                return buffer_regex_search_failure(
                    interp,
                    &pattern.text,
                    original_point,
                    limit,
                    noerror,
                    move_on_failure,
                );
            }
            let mut best_match: Option<(usize, usize, usize)> = None;
            let mut search_byte = 0usize;
            while search_byte <= prefix.len() {
                let Some(captures) = regex
                    .captures_from_pos(&prefix, search_byte)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                else {
                    break;
                };
                let Some(matched) = captures.get(0) else {
                    break;
                };
                let Some(match_start) = backward_match_position(
                    absolute_start,
                    &prefix,
                    matched.start(),
                    empty_line_pattern,
                ) else {
                    break;
                };
                let Some(match_end) = backward_match_position(
                    absolute_start,
                    &prefix,
                    matched.end(),
                    empty_line_pattern,
                ) else {
                    break;
                };
                if match_start >= limit
                    && match_end <= search_point
                    && best_match.is_none_or(|(best_start, best_end, _)| {
                        match_start > best_start
                            || (match_start == best_start && match_end > best_end)
                    })
                {
                    best_match = Some((match_start, match_end, matched.start()));
                }

                // Move from the match's START, not its end: backward search
                // must notice overlapping candidates and ultimately select
                // the rightmost start.  Each iteration nevertheless moves
                // monotonically, unlike the former per-character loop that
                // restarted an unanchored search at every buffer position.
                let Some(next) = prefix[matched.start()..].chars().next() else {
                    break;
                };
                search_byte = matched.start() + next.len_utf8();
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
            return buffer_regex_search_failure(
                interp,
                &pattern.text,
                original_point,
                limit,
                noerror,
                move_on_failure,
            );
        }
        Ok(Value::Integer(interp.buffer.point() as i64))
    }
}

fn buffer_regex_search_failure(
    interp: &mut Interpreter,
    pattern: &str,
    original_point: usize,
    limit: usize,
    noerror: bool,
    move_on_failure: bool,
) -> Result<Value, LispError> {
    // GNU's search_buffer is transactional across COUNT repetitions: a later
    // miss does not leave point at an earlier partial match.  Only a non-t,
    // non-nil NOERROR asks search_command to move point to the bound.
    interp.buffer.goto_char(if move_on_failure {
        limit
    } else {
        original_point
    });
    if noerror {
        Ok(Value::Nil)
    } else {
        Err(LispError::SignalValue(Value::list([
            Value::Symbol("search-failed".into()),
            Value::String(pattern.into()),
        ])))
    }
}

fn single_syntax_class_pattern(pattern: &str) -> Option<(bool, bool, char)> {
    let (line_anchored, pattern) = pattern
        .strip_prefix('^')
        .map_or((false, pattern), |rest| (true, rest));
    let mut chars = pattern.chars();
    if chars.next()? != '\\' {
        return None;
    }
    let negated = match chars.next()? {
        's' => false,
        'S' => true,
        _ => return None,
    };
    let class = chars.next()?;
    (chars.next().is_none() && super::syntax::syntax_class_from_char(class).is_some()).then_some((
        line_anchored,
        negated,
        class,
    ))
}

fn next_single_syntax_class_match(
    interp: &Interpreter,
    env: &Env,
    start: usize,
    limit: usize,
    line_anchored: bool,
    negated: bool,
    syntax_class: char,
) -> Option<usize> {
    let point_min = interp.buffer.point_min();
    let mut candidate = start;
    if line_anchored && candidate > point_min && interp.buffer.char_at(candidate - 1) != Some('\n')
    {
        while candidate < limit && interp.buffer.char_at(candidate) != Some('\n') {
            candidate += 1;
        }
        candidate += usize::from(candidate < limit);
    }
    while candidate < limit {
        let matches = super::syntax::syntax_class_at_buffer_position_matches(
            interp,
            env,
            candidate,
            syntax_class,
        );
        if matches != negated {
            return Some(candidate);
        }
        if !line_anchored {
            candidate += 1;
            continue;
        }
        while candidate < limit && interp.buffer.char_at(candidate) != Some('\n') {
            candidate += 1;
        }
        candidate += usize::from(candidate < limit);
    }
    None
}

fn previous_single_syntax_class_match(
    interp: &Interpreter,
    env: &Env,
    start: usize,
    limit: usize,
    line_anchored: bool,
    negated: bool,
    syntax_class: char,
) -> Option<usize> {
    let point_min = interp.buffer.point_min();
    let mut candidate = start;
    while candidate > limit {
        candidate -= 1;
        if line_anchored
            && candidate > point_min
            && interp.buffer.char_at(candidate - 1) != Some('\n')
        {
            continue;
        }
        let matches = super::syntax::syntax_class_at_buffer_position_matches(
            interp,
            env,
            candidate,
            syntax_class,
        );
        if matches != negated {
            return Some(candidate);
        }
    }
    None
}

fn search_noerror_moves(noerror: Option<&Value>) -> bool {
    noerror.is_some_and(|value| value.is_truthy() && !matches!(value, Value::T))
}

fn last_empty_line_match_position(
    absolute_start: usize,
    haystack: &str,
    limit: usize,
    search_point: usize,
) -> Option<usize> {
    if haystack.is_empty() && absolute_start >= limit && absolute_start <= search_point {
        return Some(absolute_start);
    }

    let mut previous_was_newline = true;
    let mut best = None;
    for (char_offset, ch) in haystack.chars().enumerate() {
        if ch == '\n' && previous_was_newline {
            let pos = absolute_start + char_offset;
            if pos >= limit && pos <= search_point {
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
                _ => {
                    return Err(LispError::Signal(
                        "Invalid use of `\\' in replacement text".into(),
                    ));
                }
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
                '?' => {
                    expanded.push('\\');
                    expanded.push('?');
                }
                _ => {
                    return Err(LispError::Signal(
                        "Invalid use of `\\' in replacement text".into(),
                    ));
                }
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
