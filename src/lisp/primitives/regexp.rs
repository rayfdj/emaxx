use super::*;

pub(super) fn string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
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
    super::search::string_match_1(
        interp,
        env,
        &args[0],
        &args[1],
        args.get(2),
        false,
        update_match_data && !args.get(3).is_some_and(Value::is_truthy),
    )
}

pub(super) fn posix_string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "posix-string-match".into(),
            args.len(),
        ));
    }
    super::search::string_match_1(
        interp,
        env,
        &args[0],
        &args[1],
        args.get(2),
        true,
        !args.get(3).is_some_and(Value::is_truthy),
    )
}

struct SkipCharsSpec {
    negate: bool,
    literals: Vec<char>,
    ranges: Vec<(char, char)>,
    classes: Vec<String>,
}

/// The `space' and `word' POSIX classes are syntax-table predicates in GNU:
/// syntax.c:2258 routes `skip-chars-forward' through the same
/// `re_iswctype' used by the regexp engine, so ISSPACE/ISWORD
/// (regex-emacs.c:151-153) decide membership.  The scan borrows the buffer,
/// so resolve the two classes into a lookup before the loop starts.
struct SkipSyntaxSnapshot {
    /// Sorted, non-overlapping (start, end, class) segments covering the
    /// scalar range.  Resolved once from the syntax table so the scan --
    /// which holds a mutable borrow of the buffer -- needs no interpreter,
    /// and so the cost is O(table entries) per call rather than O(buffer).
    segments: std::rc::Rc<Vec<(u32, u32, super::syntax::SyntaxClass)>>,
}

impl SkipSyntaxSnapshot {
    fn capture(interp: &Interpreter, spec: &SkipCharsSpec) -> Option<Self> {
        if !spec
            .classes
            .iter()
            .any(|class| class == "space" || class == "word" || class == "punct")
        {
            return None;
        }
        const SCALAR_END: u32 = char::MAX as u32 + 1;
        let table_id = interp.current_syntax_table_id();
        // Keyed on the table and its current contents, like the rendered
        // class cache: resolving the segments per call made skip-chars
        // hundreds of times slower than its literal-spec path.
        if let Some(segments) = interp.cached_syntax_segments(table_id) {
            return Some(Self { segments });
        }
        // Surrogates are not scalars; keep the same boundary seeds the class
        // renderer uses so segment starts are always valid characters.
        //
        // Every ASCII code point is also a boundary.  Each window below is
        // classified by sampling its START, which is only correct where the
        // class is uniform across the window -- and the fallback
        // `default_syntax_entry' varies PER CHARACTER inside ASCII (digits
        // word, punctuation punct, space whitespace...) while being uniformly
        // Word above 0x80.  Boundaries drawn only from explicit char-table
        // entries left ASCII gaps spanning several default classes, and the
        // window holding the digits sampled as punctuation: skip-chars
        // `[:word:]' stopped dead at `0' while `char-syntax' said w.  The
        // regression reached the frozen measurement as
        // regex-tests-word-character-class, the one file that moved between
        // the 2026-08-25 and 2026-08-27 baselines.  128 extra boundaries make
        // the sample exact everywhere and coalescing merges them right back.
        let mut boundaries = vec![0u32, 0xd800, 0xe000];
        boundaries.extend(0..=0x80u32);
        let mut current = Some(table_id);
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = current {
            if !seen.insert(id) {
                break;
            }
            let table = id;
            for entry in &table.ranges() {
                if entry.start < SCALAR_END {
                    boundaries.push(entry.start);
                }
                if entry.end.saturating_add(1) < SCALAR_END {
                    boundaries.push(entry.end.saturating_add(1));
                }
            }
            current = table.parent();
        }
        boundaries.push(SCALAR_END);
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut segments: Vec<(u32, u32, super::syntax::SyntaxClass)> = Vec::new();
        for window in boundaries.windows(2) {
            let (start, end) = (window[0], window[1] - 1);
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
        let segments = std::rc::Rc::new(segments);
        interp.cache_syntax_segments(table_id, segments.clone());
        Some(Self { segments })
    }

    fn class_of(&self, ch: char) -> Option<super::syntax::SyntaxClass> {
        let code = ch as u32;
        let index = self
            .segments
            .partition_point(|(start, _, _)| *start <= code)
            .checked_sub(1)?;
        let (_, end, class) = self.segments[index];
        (code <= end).then_some(class)
    }
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

fn skip_char_matches_class_with_syntax(
    ch: char,
    class: &str,
    syntax: Option<super::syntax::SyntaxClass>,
) -> bool {
    match (class, syntax) {
        // GNU shares one predicate between the regexp engine and
        // `skip-chars-forward'; both consult the syntax table for exactly
        // these two classes.  The previous fixed tables here disagreed with
        // the regexp path, and the `word' table additionally hardcoded
        // U+2620 -- the single codepoint test/src/regex-emacs-tests.el uses
        // as its word-character fixture, which made that one test pass while
        // every neighbouring codepoint answered wrongly.
        ("space", Some(resolved)) => resolved == super::syntax::SyntaxClass::Whitespace,
        ("word", Some(resolved)) => resolved == super::syntax::SyntaxClass::Word,
        // ISPUNCT beyond ASCII is `BUFFER_SYNTAX (c) != Sword'.
        ("punct", Some(resolved)) if !ch.is_ascii() => resolved != super::syntax::SyntaxClass::Word,
        _ => skip_char_matches_class(ch, class),
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
        // Table-less fallback only (no interpreter available); the live
        // path above resolves `word' through the syntax table.
        "word" => ch.is_alphanumeric() || ch == '_',
        "xdigit" => ch.is_ascii_hexdigit(),
        _ => false,
    }
}

fn skip_char_matches_spec_with_syntax(
    ch: char,
    spec: &SkipCharsSpec,
    syntax: Option<super::syntax::SyntaxClass>,
) -> bool {
    let literal_match = spec.literals.contains(&ch);
    let range_match = spec
        .ranges
        .iter()
        .any(|(start, end)| *start <= ch && ch <= *end);
    let class_match = spec
        .classes
        .iter()
        .any(|class| skip_char_matches_class_with_syntax(ch, class, syntax));
    (literal_match || range_match || class_match) != spec.negate
}

pub(super) fn skip_chars_forward_impl(
    interp: &mut Interpreter,
    spec_value: &Value,
    limit_value: Option<&Value>,
) -> Result<Value, LispError> {
    let spec = parse_skip_chars_spec(&string_text(spec_value)?);
    let limit = if let Some(limit_value) = limit_value {
        if limit_value.is_nil() {
            interp.buffer.borrow().point_max()
        } else {
            position_from_value(interp, limit_value)?
        }
    } else {
        interp.buffer.borrow().point_max()
    };
    // Resolve the syntax table into range segments before the scan: the
    // closure below holds a mutable borrow of the buffer, so it cannot reach
    // back into the interpreter.
    let snapshot = SkipSyntaxSnapshot::capture(interp, &spec);
    let skipped = interp.buffer.borrow_mut().skip_forward_while(limit, |ch| {
        skip_char_matches_spec_with_syntax(
            ch,
            &spec,
            snapshot.as_ref().and_then(|snapshot| snapshot.class_of(ch)),
        )
    });
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
            interp.buffer.borrow().point_min()
        } else {
            position_from_value(interp, limit_value)?
        }
    } else {
        interp.buffer.borrow().point_min()
    };
    // Resolve the syntax table into range segments before the scan: the
    // closure below holds a mutable borrow of the buffer, so it cannot reach
    // back into the interpreter.
    let snapshot = SkipSyntaxSnapshot::capture(interp, &spec);
    let skipped = interp.buffer.borrow_mut().skip_backward_while(limit, |ch| {
        skip_char_matches_spec_with_syntax(
            ch,
            &spec,
            snapshot.as_ref().and_then(|snapshot| snapshot.class_of(ch)),
        )
    });
    Ok(Value::Integer(-(skipped as i64)))
}

pub(super) fn looking_at_impl(
    interp: &mut Interpreter,
    pattern_value: &Value,
    posix: bool,
    update_match_data: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    super::search::looking_at_1(interp, env, pattern_value, posix, update_match_data)
}

pub(super) fn buffer_regex_search(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
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
    super::search::search_command(interp, env, args, if forward { 1 } else { -1 }, true, posix)
}

pub(super) fn slice_string_chars(source: &str, start: usize, end: usize) -> String {
    source
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

/// search.c update_search_regs, which replace_range runs for
/// `replace-match': [OLDSTART, OLDEND) became [OLDSTART, NEWEND).  A
/// start before or at OLDSTART stays (the replaced group is taken to
/// enclose it), one inside goes to OLDSTART and one after shifts; an end
/// at or after OLDEND shifts and one inside goes to OLDSTART.
pub(super) fn update_search_regs(
    match_data: &mut [Option<(usize, usize)>],
    oldstart: usize,
    oldend: usize,
    newend: usize,
) {
    let shift = |position: usize| position + newend - oldend;
    for (start, end) in match_data.iter_mut().flatten() {
        if *start > oldstart {
            *start = if *start >= oldend {
                shift(*start)
            } else {
                oldstart
            };
        }
        if *end >= oldend {
            *end = shift(*end);
        } else if *end > oldstart {
            *end = oldstart;
        }
    }
}
