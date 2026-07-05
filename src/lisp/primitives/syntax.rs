use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SyntaxClass {
    Whitespace = 0,
    Punctuation = 1,
    Word = 2,
    Symbol = 3,
    OpenParen = 4,
    CloseParen = 5,
    Quote = 6,
    StringQuote = 7,
    PairedDelimiter = 8,
    Escape = 9,
    CharQuote = 10,
    CommentStart = 11,
    CommentEnd = 12,
    Inherit = 13,
    GenericCommentDelimiter = 14,
    GenericStringDelimiter = 15,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SyntaxEntry {
    pub(super) class: SyntaxClass,
    matching: Option<char>,
    start_first: bool,
    start_second: bool,
    end_first: bool,
    end_second: bool,
    nested: bool,
    style_b: bool,
}

impl Default for SyntaxEntry {
    fn default() -> Self {
        Self {
            class: SyntaxClass::Punctuation,
            matching: None,
            start_first: false,
            start_second: false,
            end_first: false,
            end_second: false,
            nested: false,
            style_b: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommentKind {
    Single {
        line: bool,
    },
    Block {
        end_first: char,
        end_second: char,
        nested: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommentStart {
    kind: CommentKind,
    len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommentState {
    kind: CommentKind,
    start_pos: usize,
    depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StringState {
    quote: char,
    start_pos: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParseStackEntry {
    open_pos: usize,
    close_char: char,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ParseState {
    base_depth: i64,
    min_depth: i64,
    stack: Vec<ParseStackEntry>,
    comment: Option<CommentState>,
    string: Option<StringState>,
}

impl ParseState {
    fn depth(&self) -> i64 {
        self.base_depth + self.stack.len() as i64
    }
}

fn syntax_class_from_char(ch: char) -> Option<SyntaxClass> {
    Some(match ch {
        ' ' => SyntaxClass::Whitespace,
        '.' => SyntaxClass::Punctuation,
        'w' => SyntaxClass::Word,
        '_' => SyntaxClass::Symbol,
        '(' => SyntaxClass::OpenParen,
        ')' => SyntaxClass::CloseParen,
        '\'' => SyntaxClass::Quote,
        '"' => SyntaxClass::StringQuote,
        '$' => SyntaxClass::PairedDelimiter,
        '\\' => SyntaxClass::Escape,
        '/' => SyntaxClass::CharQuote,
        '<' => SyntaxClass::CommentStart,
        '>' => SyntaxClass::CommentEnd,
        '@' => SyntaxClass::Inherit,
        '!' => SyntaxClass::GenericCommentDelimiter,
        '|' => SyntaxClass::GenericStringDelimiter,
        _ => return None,
    })
}

pub(super) fn syntax_class_char(class: SyntaxClass) -> char {
    match class {
        SyntaxClass::Whitespace => ' ',
        SyntaxClass::Punctuation => '.',
        SyntaxClass::Word => 'w',
        SyntaxClass::Symbol => '_',
        SyntaxClass::OpenParen => '(',
        SyntaxClass::CloseParen => ')',
        SyntaxClass::Quote => '\'',
        SyntaxClass::StringQuote => '"',
        SyntaxClass::PairedDelimiter => '$',
        SyntaxClass::Escape => '\\',
        SyntaxClass::CharQuote => '/',
        SyntaxClass::CommentStart => '<',
        SyntaxClass::CommentEnd => '>',
        SyntaxClass::Inherit => '@',
        SyntaxClass::GenericCommentDelimiter => '!',
        SyntaxClass::GenericStringDelimiter => '|',
    }
}

pub(super) fn parse_syntax_spec(spec: &str) -> Option<SyntaxEntry> {
    let chars: Vec<char> = spec.chars().collect();
    let class = syntax_class_from_char(*chars.first()?)?;
    let mut entry = SyntaxEntry {
        class,
        ..SyntaxEntry::default()
    };
    let mut index = 1usize;
    if matches!(class, SyntaxClass::OpenParen | SyntaxClass::CloseParen) && chars.len() > 1 {
        entry.matching = Some(chars[1]);
        index = 2;
    }
    for flag in chars.iter().skip(index) {
        match flag {
            '1' => entry.start_first = true,
            '2' => entry.start_second = true,
            '3' => entry.end_first = true,
            '4' => entry.end_second = true,
            'n' => entry.nested = true,
            'b' => entry.style_b = true,
            _ => {}
        }
    }
    Some(entry)
}

fn syntax_entry_code(entry: SyntaxEntry) -> i64 {
    let mut code = entry.class as i64;
    if entry.start_first {
        code |= 1 << 16;
    }
    if entry.start_second {
        code |= 1 << 17;
    }
    if entry.end_first {
        code |= 1 << 18;
    }
    if entry.end_second {
        code |= 1 << 19;
    }
    if entry.style_b {
        code |= 1 << 21;
    }
    if entry.nested {
        code |= 1 << 22;
    }
    code
}

pub(super) fn syntax_entry_value(entry: SyntaxEntry) -> Value {
    let code = Value::Integer(syntax_entry_code(entry));
    match entry.matching {
        Some(matching) => Value::cons(code, Value::Integer(matching as i64)),
        None => code,
    }
}

pub(super) fn char_table_public_value(interp: &Interpreter, table_id: u64, value: Value) -> Value {
    if interp.char_table_subtype(table_id).flatten().as_deref() == Some("syntax-table")
        && let Some(spec) = string_like(&value)
        && let Some(entry) = parse_syntax_spec(&spec.text)
    {
        return syntax_entry_value(entry);
    }
    value
}

fn default_syntax_entry(ch: char) -> SyntaxEntry {
    let class = match ch {
        ' ' | '\t' | '\n' | '\r' | '\u{000B}' | '\u{000C}' => SyntaxClass::Whitespace,
        '_' => SyntaxClass::Symbol,
        ch if ch.is_alphanumeric() => SyntaxClass::Word,
        _ => SyntaxClass::Punctuation,
    };
    SyntaxEntry {
        class,
        ..SyntaxEntry::default()
    }
}

// Characters explicitly assigned CLASS in the buffer's current syntax
// table (following the parent chain).  The standard table maps no
// character to the comment classes, so `\s<'/`\s>' regexp atoms resolve
// from these explicit entries like GNU.
pub(crate) fn syntax_class_explicit_chars(interp: &Interpreter, class_char: char) -> Vec<char> {
    let mut chars: Vec<char> = Vec::new();
    let mut table_id = Some(interp.current_syntax_table_id());
    let mut seen = std::collections::HashSet::new();
    while let Some(id) = table_id {
        if !seen.insert(id) {
            break;
        }
        let Some(table) = interp.find_char_table(id) else {
            break;
        };
        for entry in &table.entries {
            // Comment-class assignments are single characters in practice;
            // ignore wide ranges to keep the expansion bounded.
            if entry.end.saturating_sub(entry.start) > 8 {
                continue;
            }
            let is_class = string_like(&entry.value)
                .map(|spec| spec.text.starts_with(class_char))
                .unwrap_or(false);
            if !is_class {
                continue;
            }
            for code in entry.start..=entry.end {
                if let Some(ch) = char::from_u32(code)
                    && !chars.contains(&ch)
                {
                    chars.push(ch);
                }
            }
        }
        table_id = table.parent;
    }
    chars
}

pub(super) fn syntax_entry_for_code(interp: &Interpreter, table_id: u64, code: u32) -> SyntaxEntry {
    let Some(ch) = char::from_u32(code) else {
        return SyntaxEntry::default();
    };
    interp
        .char_table_get(table_id, code)
        .and_then(|value| string_like(&value).and_then(|value| parse_syntax_spec(&value.text)))
        .unwrap_or_else(|| default_syntax_entry(ch))
}

fn syntax_entry_for_char(interp: &Interpreter, table_id: u64, ch: char) -> SyntaxEntry {
    syntax_entry_for_code(interp, table_id, ch as u32)
}

fn syntax_entry_from_value(value: &Value) -> Option<SyntaxEntry> {
    match value {
        Value::Integer(code) => {
            let class = syntax_class_from_code(*code)?;
            Some(SyntaxEntry {
                class,
                start_first: code & (1 << 16) != 0,
                start_second: code & (1 << 17) != 0,
                end_first: code & (1 << 18) != 0,
                end_second: code & (1 << 19) != 0,
                style_b: code & (1 << 21) != 0,
                nested: code & (1 << 22) != 0,
                ..SyntaxEntry::default()
            })
        }
        Value::Cons(_, _) => {
            let code = value.car().ok()?.as_integer().ok()?;
            let matching = value
                .cdr()
                .ok()
                .and_then(|cdr| cdr.as_integer().ok())
                .and_then(|code| char::from_u32(code as u32));
            let mut entry = syntax_entry_from_value(&Value::Integer(code))?;
            entry.matching = matching;
            Some(entry)
        }
        _ => value
            .to_vec()
            .ok()
            .and_then(|items| items.first().cloned())
            .and_then(|item| syntax_entry_from_value(&item)),
    }
}

fn syntax_class_from_code(code: i64) -> Option<SyntaxClass> {
    match code & 0xffff {
        0 => Some(SyntaxClass::Whitespace),
        1 => Some(SyntaxClass::Punctuation),
        2 => Some(SyntaxClass::Word),
        3 => Some(SyntaxClass::Symbol),
        4 => Some(SyntaxClass::OpenParen),
        5 => Some(SyntaxClass::CloseParen),
        6 => Some(SyntaxClass::Quote),
        7 => Some(SyntaxClass::StringQuote),
        8 => Some(SyntaxClass::PairedDelimiter),
        9 => Some(SyntaxClass::Escape),
        10 => Some(SyntaxClass::CharQuote),
        11 => Some(SyntaxClass::CommentStart),
        12 => Some(SyntaxClass::CommentEnd),
        13 => Some(SyntaxClass::Inherit),
        14 => Some(SyntaxClass::GenericCommentDelimiter),
        15 => Some(SyntaxClass::GenericStringDelimiter),
        _ => None,
    }
}

fn syntax_entry_at_buffer_position(
    interp: &Interpreter,
    table_id: u64,
    ch: char,
    pos: usize,
) -> SyntaxEntry {
    let property = buffer_char_property_at(interp, &interp.buffer, pos, "syntax-table");
    syntax_entry_from_value(&property)
        .unwrap_or_else(|| syntax_entry_for_char(interp, table_id, ch))
}

fn matching_close_char(ch: char, entry: SyntaxEntry) -> Option<char> {
    entry.matching.or(match ch {
        '(' => Some(')'),
        '[' => Some(']'),
        '{' => Some('}'),
        _ => None,
    })
}

fn matching_open_char(ch: char, entry: SyntaxEntry) -> Option<char> {
    entry.matching.or(match ch {
        ')' => Some('('),
        ']' => Some('['),
        '}' => Some('{'),
        _ => None,
    })
}

fn newline_ends_comments(interp: &Interpreter, table_id: u64) -> bool {
    syntax_entry_for_code(interp, table_id, '\n' as u32).class == SyntaxClass::CommentEnd
}

fn comment_start_at(
    interp: &Interpreter,
    table_id: u64,
    chars: &[char],
    idx: usize,
) -> Option<CommentStart> {
    let ch = *chars.get(idx)?;
    let entry = syntax_entry_for_char(interp, table_id, ch);
    if entry.class == SyntaxClass::CommentStart {
        return Some(CommentStart {
            kind: CommentKind::Single {
                line: entry.style_b && newline_ends_comments(interp, table_id),
            },
            len: 1,
        });
    }
    let next = *chars.get(idx + 1)?;
    let next_entry = syntax_entry_for_char(interp, table_id, next);
    if !(entry.start_first && next_entry.start_second) {
        return None;
    }
    if ch == next && entry.style_b && next_entry.style_b && newline_ends_comments(interp, table_id)
    {
        return Some(CommentStart {
            kind: CommentKind::Single { line: true },
            len: 2,
        });
    }
    if !next_entry.end_first {
        return None;
    }
    let end_second = if entry.end_second {
        ch
    } else {
        entry.matching.unwrap_or(ch)
    };
    Some(CommentStart {
        kind: CommentKind::Block {
            end_first: next,
            end_second,
            nested: entry.nested || next_entry.nested,
        },
        len: 2,
    })
}

fn preceded_by_odd_backslashes(chars: &[char], idx: usize) -> bool {
    let mut count = 0usize;
    let mut cursor = idx;
    while cursor > 0 && chars[cursor - 1] == '\\' {
        count += 1;
        cursor -= 1;
    }
    count % 2 == 1
}

fn skip_comment_with_status(
    interp: &Interpreter,
    table_id: u64,
    chars: &[char],
    idx: usize,
    start: CommentStart,
    comment_end_can_be_escaped: bool,
) -> (usize, bool) {
    let mut cursor = idx + start.len;
    match start.kind {
        CommentKind::Single { line } => {
            while cursor < chars.len() {
                let entry = syntax_entry_for_char(interp, table_id, chars[cursor]);
                if entry.class == SyntaxClass::CommentEnd {
                    if line
                        && chars[cursor] == '\n'
                        && comment_end_can_be_escaped
                        && preceded_by_odd_backslashes(chars, cursor)
                    {
                        cursor += 1;
                        continue;
                    }
                    return (cursor + 1, true);
                }
                cursor += 1;
            }
            (chars.len(), false)
        }
        CommentKind::Block {
            end_first,
            end_second,
            nested,
        } => {
            let mut depth = 1usize;
            while cursor < chars.len() {
                if nested
                    && let Some(nested_start) = comment_start_at(interp, table_id, chars, cursor)
                    && matches!(
                        nested_start.kind,
                        CommentKind::Block {
                            end_first: nested_end_first,
                            end_second: nested_end_second,
                            ..
                        } if nested_end_first == end_first && nested_end_second == end_second
                    )
                {
                    depth += 1;
                    cursor += nested_start.len;
                    continue;
                }
                if cursor + 1 < chars.len()
                    && chars[cursor] == end_first
                    && chars[cursor + 1] == end_second
                {
                    if comment_end_can_be_escaped && preceded_by_odd_backslashes(chars, cursor) {
                        cursor += 1;
                        continue;
                    }
                    depth -= 1;
                    cursor += 2;
                    if depth == 0 {
                        return (cursor, true);
                    }
                    continue;
                }
                cursor += 1;
            }
            (chars.len(), false)
        }
    }
}

fn skip_whitespace_forward(
    interp: &Interpreter,
    table_id: u64,
    chars: &[char],
    pos: usize,
) -> usize {
    let mut idx = pos.saturating_sub(1);
    while idx < chars.len() {
        let entry = syntax_entry_for_char(interp, table_id, chars[idx]);
        if entry.class != SyntaxClass::Whitespace
            && !(entry.class == SyntaxClass::CommentEnd && chars[idx] == '\n')
        {
            break;
        }
        idx += 1;
    }
    idx + 1
}

fn skip_whitespace_backward(
    interp: &Interpreter,
    table_id: u64,
    chars: &[char],
    pos: usize,
) -> usize {
    let mut idx = pos.saturating_sub(1);
    while idx > 0 {
        let entry = syntax_entry_for_char(interp, table_id, chars[idx - 1]);
        if entry.class != SyntaxClass::Whitespace
            && !(entry.class == SyntaxClass::CommentEnd && chars[idx - 1] == '\n')
        {
            break;
        }
        idx -= 1;
    }
    idx + 1
}

fn comment_state_value(comment: CommentState) -> Value {
    match comment.kind {
        CommentKind::Block { nested: true, .. } => Value::Integer(comment.depth as i64),
        _ => Value::T,
    }
}

fn encode_parse_state(state: &ParseState) -> Value {
    let stack_value = Value::list(state.stack.iter().map(|entry| {
        Value::cons(
            Value::Integer(entry.open_pos as i64),
            Value::Integer(entry.close_char as i64),
        )
    }));
    let comment_value = match state.comment {
        Some(CommentState {
            kind: CommentKind::Single { line },
            start_pos,
            depth,
        }) => Value::list([
            if line {
                Value::Symbol("line".into())
            } else {
                Value::Symbol("single".into())
            },
            Value::Integer(start_pos as i64),
            Value::Integer(depth as i64),
        ]),
        Some(CommentState {
            kind:
                CommentKind::Block {
                    end_first,
                    end_second,
                    nested,
                },
            start_pos,
            depth,
        }) => Value::list([
            Value::Symbol("block".into()),
            Value::Integer(start_pos as i64),
            Value::Integer(depth as i64),
            Value::Integer(end_first as i64),
            Value::Integer(end_second as i64),
            if nested { Value::T } else { Value::Nil },
        ]),
        None => Value::Nil,
    };
    let string_value = state
        .string
        .map(|string| {
            Value::list([
                Value::Integer(string.quote as i64),
                Value::Integer(string.start_pos as i64),
            ])
        })
        .unwrap_or(Value::Nil);
    let mut hidden = vec![
        stack_value,
        comment_value,
        Value::Integer(state.base_depth),
        Value::Integer(state.min_depth),
    ];
    if state.string.is_some() {
        hidden.push(string_value);
    }
    Value::list([
        Value::Integer(state.depth()),
        state
            .stack
            .last()
            .map(|entry| Value::Integer(entry.open_pos as i64))
            .unwrap_or(Value::Nil),
        Value::Nil,
        state
            .string
            .map(|string| Value::Integer(string.quote as i64))
            .unwrap_or(Value::Nil),
        state.comment.map(comment_state_value).unwrap_or(Value::Nil),
        Value::Nil,
        Value::Integer(state.min_depth),
        Value::Nil,
        state
            .comment
            .map(|comment| Value::Integer(comment.start_pos as i64))
            .or_else(|| {
                state
                    .string
                    .map(|string| Value::Integer(string.start_pos as i64))
            })
            .unwrap_or(Value::Nil),
        Value::list(
            state
                .stack
                .iter()
                .map(|entry| Value::Integer(entry.open_pos as i64)),
        ),
        Value::list(hidden),
    ])
}

fn decode_parse_state(value: Option<&Value>) -> ParseState {
    let Some(value) = value else {
        return ParseState::default();
    };
    let Ok(items) = value.to_vec() else {
        return ParseState::default();
    };
    let Some(hidden) = items.get(10) else {
        return ParseState::default();
    };
    let Ok(hidden_items) = hidden.to_vec() else {
        return ParseState::default();
    };
    let mut state = ParseState {
        base_depth: items
            .first()
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(0),
        min_depth: items
            .get(6)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(0),
        ..Default::default()
    };
    if let Some(stack_value) = hidden_items.first()
        && let Ok(entries) = stack_value.to_vec()
    {
        for entry in entries {
            let Value::Cons(open_pos, close_char) = entry else {
                continue;
            };
            let Ok(open_pos) = open_pos.borrow().as_integer() else {
                continue;
            };
            let Ok(close_char) = close_char.borrow().as_integer() else {
                continue;
            };
            let Some(close_char) = char::from_u32(close_char as u32) else {
                continue;
            };
            state.stack.push(ParseStackEntry {
                open_pos: open_pos.max(1) as usize,
                close_char,
            });
        }
    }
    if let Some(comment_value) = hidden_items.get(1)
        && !comment_value.is_nil()
        && let Ok(entries) = comment_value.to_vec()
        && let Some(Value::Symbol(kind)) = entries.first()
    {
        let start_pos = entries
            .get(1)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(1)
            .max(1) as usize;
        let depth = entries
            .get(2)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(1)
            .max(1) as usize;
        state.comment = match kind.as_str() {
            "single" => Some(CommentState {
                kind: CommentKind::Single { line: false },
                start_pos,
                depth,
            }),
            "line" => Some(CommentState {
                kind: CommentKind::Single { line: true },
                start_pos,
                depth,
            }),
            "block" => {
                let end_first = entries
                    .get(3)
                    .and_then(|value| value.as_integer().ok())
                    .and_then(|value| char::from_u32(value as u32));
                let end_second = entries
                    .get(4)
                    .and_then(|value| value.as_integer().ok())
                    .and_then(|value| char::from_u32(value as u32));
                match (end_first, end_second) {
                    (Some(end_first), Some(end_second)) => Some(CommentState {
                        kind: CommentKind::Block {
                            end_first,
                            end_second,
                            nested: entries.get(5).is_some_and(Value::is_truthy),
                        },
                        start_pos,
                        depth,
                    }),
                    _ => None,
                }
            }
            _ => None,
        };
    }
    if let Some(base_depth) = hidden_items
        .get(2)
        .and_then(|value| value.as_integer().ok())
    {
        state.base_depth = base_depth;
    }
    if let Some(min_depth) = hidden_items
        .get(3)
        .and_then(|value| value.as_integer().ok())
    {
        state.min_depth = min_depth;
    }
    if let Some(string_value) = hidden_items.get(4)
        && !string_value.is_nil()
        && let Ok(entries) = string_value.to_vec()
    {
        let quote = entries
            .first()
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| char::from_u32(value as u32));
        let start_pos = entries
            .get(1)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(1)
            .max(1) as usize;
        if let Some(quote) = quote {
            state.string = Some(StringState { quote, start_pos });
        }
    }
    state
}

pub(super) fn scan_sexps_position(interp: &Interpreter, from: usize, count: i64) -> Option<usize> {
    let chars = interp.buffer.buffer_string().chars().collect::<Vec<_>>();
    let min = interp.buffer.point_min();
    let max = interp.buffer.point_max();
    let mut pos = from.clamp(min, max);
    if count >= 0 {
        for _ in 0..count {
            pos = scan_one_sexp_forward(interp, &chars, pos, max)?;
        }
    } else {
        for _ in 0..(-count) {
            pos = scan_one_sexp_backward(interp, &chars, pos, min)?;
        }
    }
    Some(pos)
}

pub(super) fn scan_sexps_position_for_scan_sexps(
    interp: &Interpreter,
    from: usize,
    count: i64,
) -> Result<Option<usize>, LispError> {
    let chars = interp.buffer.buffer_string().chars().collect::<Vec<_>>();
    let min = interp.buffer.point_min();
    let max = interp.buffer.point_max();
    let mut pos = from.clamp(min, max);
    if count >= 0 {
        for _ in 0..count {
            match scan_one_sexp_forward_for_scan_sexps(interp, &chars, pos, max)? {
                Some(next) => pos = next,
                None => return Ok(None),
            }
        }
    } else {
        for _ in 0..(-count) {
            match scan_one_sexp_backward_for_scan_sexps(interp, &chars, pos, min)? {
                Some(next) => pos = next,
                None => return Ok(None),
            }
        }
    }
    Ok(Some(pos))
}

fn scan_sexps_premature_error(position: usize) -> LispError {
    let start = position.saturating_sub(1).max(1);
    LispError::SignalValue(Value::list([
        Value::Symbol("scan-error".into()),
        Value::String("Containing expression ends prematurely".into()),
        Value::Integer(start as i64),
        Value::Integer(position as i64),
    ]))
}

fn scan_one_sexp_forward_for_scan_sexps(
    interp: &Interpreter,
    chars: &[char],
    from: usize,
    max: usize,
) -> Result<Option<usize>, LispError> {
    let end = max.saturating_sub(1).min(chars.len());
    let idx = skip_ignored_forward(interp, chars, from.saturating_sub(1), end);
    if idx >= end {
        return Ok(None);
    }
    let table_id = interp.current_syntax_table_id();
    let entry = syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1);
    if entry.class != SyntaxClass::StringQuote && is_lisp_expression_prefix(chars[idx]) {
        return scan_one_sexp_forward_for_scan_sexps(interp, chars, idx + 2, max);
    }
    match entry.class {
        SyntaxClass::CloseParen => Err(scan_sexps_premature_error(idx + 2)),
        SyntaxClass::OpenParen => scan_balanced_forward_for_scan_sexps(interp, chars, idx, end),
        _ => Ok(scan_one_sexp_forward(interp, chars, from, max)),
    }
}

fn scan_balanced_forward_for_scan_sexps(
    interp: &Interpreter,
    chars: &[char],
    open_idx: usize,
    end: usize,
) -> Result<Option<usize>, LispError> {
    let table_id = interp.current_syntax_table_id();
    let open_entry =
        syntax_entry_at_buffer_position(interp, table_id, chars[open_idx], open_idx + 1);
    let Some(first_close) = matching_close_char(chars[open_idx], open_entry) else {
        return Ok(None);
    };
    let mut stack = vec![first_close];
    let mut saw_mismatch = false;
    let ignore_comments = interp
        .symbol_value_cell("parse-sexp-ignore-comments")
        .map(|value| value.is_truthy())
        .unwrap_or(false);
    let mut idx = open_idx + 1;
    while idx < end {
        if ignore_comments && let Some(start) = comment_start_at(interp, table_id, chars, idx) {
            let (next, _closed) =
                skip_comment_with_status(interp, table_id, chars, idx, start, false);
            if next > idx {
                idx = next;
                continue;
            }
        }
        let entry = syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1);
        match entry.class {
            SyntaxClass::StringQuote => {
                let Some(next) = scan_string_forward(chars, idx, end) else {
                    return Ok(None);
                };
                idx = next;
            }
            SyntaxClass::OpenParen => {
                if let Some(close) = matching_close_char(chars[idx], entry) {
                    stack.push(close);
                }
            }
            SyntaxClass::CloseParen => {
                let Some(expected) = stack.last().copied() else {
                    return Err(scan_sexps_premature_error(idx + 2));
                };
                let Some(actual) = matching_open_char(chars[idx], entry).map(|_| chars[idx]) else {
                    return Err(scan_sexps_premature_error(idx + 2));
                };
                if expected == actual {
                    stack.pop();
                    if stack.is_empty() {
                        return if saw_mismatch {
                            Err(scan_sexps_premature_error(idx + 2))
                        } else {
                            Ok(Some(idx + 2))
                        };
                    }
                } else {
                    saw_mismatch = true;
                }
            }
            _ => {}
        }
        idx += 1;
    }
    Ok(None)
}

fn scan_one_sexp_backward_for_scan_sexps(
    interp: &Interpreter,
    chars: &[char],
    from: usize,
    min: usize,
) -> Result<Option<usize>, LispError> {
    let mut idx = from.saturating_sub(2);
    let min_idx = min.saturating_sub(1);
    while idx >= min_idx && chars.get(idx).is_some_and(|ch| ch.is_whitespace()) {
        if idx == 0 {
            return Ok(None);
        }
        idx -= 1;
    }
    let table_id = interp.current_syntax_table_id();
    let Some(&ch) = chars.get(idx) else {
        return Ok(None);
    };
    let entry = syntax_entry_at_buffer_position(interp, table_id, ch, idx + 1);
    match entry.class {
        SyntaxClass::OpenParen => Err(scan_sexps_premature_error(idx + 1)),
        _ => Ok(scan_one_sexp_backward(interp, chars, from, min)),
    }
}

// Skip whitespace, and — when `parse-sexp-ignore-comments' is set, as
// lisp modes do — comments, before sexp scanning (GNU scan_sexps_forward).
fn skip_ignored_forward(interp: &Interpreter, chars: &[char], mut idx: usize, end: usize) -> usize {
    let ignore_comments = interp
        .symbol_value_cell("parse-sexp-ignore-comments")
        .map(|value| value.is_truthy())
        .unwrap_or(false);
    let table_id = interp.current_syntax_table_id();
    loop {
        while idx < end && chars[idx].is_whitespace() {
            idx += 1;
        }
        if !ignore_comments || idx >= end {
            return idx;
        }
        let Some(start) = comment_start_at(interp, table_id, chars, idx) else {
            return idx;
        };
        let (next, _closed) = skip_comment_with_status(interp, table_id, chars, idx, start, false);
        if next <= idx {
            return idx;
        }
        idx = next.min(chars.len());
    }
}

// Whether [from, buffer end) holds only whitespace and (ignored) comments;
// GNU forward-sexp then moves to the buffer end instead of signaling.
pub(super) fn rest_of_buffer_is_ignorable(interp: &Interpreter, from: usize) -> bool {
    let chars = interp.buffer.buffer_string().chars().collect::<Vec<_>>();
    let end = interp.buffer.point_max().saturating_sub(1).min(chars.len());
    skip_ignored_forward(interp, &chars, from.saturating_sub(1), end) >= end
}

// Whether [buffer start, to) holds only whitespace and (ignored) comments.
pub(super) fn buffer_before_is_ignorable(interp: &Interpreter, to: usize) -> bool {
    let chars = interp.buffer.buffer_string().chars().collect::<Vec<_>>();
    let start = interp.buffer.point_min().saturating_sub(1);
    let end = to.saturating_sub(1).min(chars.len());
    skip_ignored_forward(interp, &chars, start, end) >= end
}

fn scan_one_sexp_forward(
    interp: &Interpreter,
    chars: &[char],
    from: usize,
    max: usize,
) -> Option<usize> {
    let end = max.saturating_sub(1).min(chars.len());
    let mut idx = skip_ignored_forward(interp, chars, from.saturating_sub(1), end);
    if idx >= end {
        return None;
    }
    let table_id = interp.current_syntax_table_id();
    let entry = syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1);
    if entry.class != SyntaxClass::StringQuote && is_lisp_expression_prefix(chars[idx]) {
        return scan_one_sexp_forward(interp, chars, idx + 2, max);
    }
    match entry.class {
        SyntaxClass::OpenParen => scan_balanced_forward(interp, chars, idx, end).map(|idx| idx + 1),
        SyntaxClass::StringQuote => scan_string_forward(chars, idx, end).map(|idx| idx + 2),
        SyntaxClass::CloseParen => None,
        _ => {
            while idx < end
                && !chars[idx].is_whitespace()
                && !matches!(
                    syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1).class,
                    SyntaxClass::OpenParen | SyntaxClass::CloseParen | SyntaxClass::StringQuote
                )
            {
                idx += 1;
            }
            Some(idx + 1)
        }
    }
}

fn is_lisp_expression_prefix(ch: char) -> bool {
    matches!(ch, '\'' | '`' | ',')
}

fn scan_balanced_forward(
    interp: &Interpreter,
    chars: &[char],
    open_idx: usize,
    end: usize,
) -> Option<usize> {
    let table_id = interp.current_syntax_table_id();
    let open_entry =
        syntax_entry_at_buffer_position(interp, table_id, chars[open_idx], open_idx + 1);
    let mut stack = vec![matching_close_char(chars[open_idx], open_entry)?];
    let ignore_comments = interp
        .symbol_value_cell("parse-sexp-ignore-comments")
        .map(|value| value.is_truthy())
        .unwrap_or(false);
    let mut idx = open_idx + 1;
    while idx < end {
        if ignore_comments && let Some(start) = comment_start_at(interp, table_id, chars, idx) {
            let (next, _closed) =
                skip_comment_with_status(interp, table_id, chars, idx, start, false);
            if next > idx {
                idx = next;
                continue;
            }
        }
        let entry = syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1);
        match entry.class {
            SyntaxClass::StringQuote => idx = scan_string_forward(chars, idx, end)?,
            SyntaxClass::OpenParen => stack.push(matching_close_char(chars[idx], entry)?),
            SyntaxClass::CloseParen => {
                let expected = stack.pop()?;
                let actual = matching_open_char(chars[idx], entry).map(|_| chars[idx])?;
                if expected != actual {
                    return None;
                }
                if stack.is_empty() {
                    return Some(idx + 1);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn scan_string_forward(chars: &[char], quote_idx: usize, end: usize) -> Option<usize> {
    let quote = *chars.get(quote_idx)?;
    let mut idx = quote_idx + 1;
    let mut escaped = false;
    while idx < end {
        let ch = chars[idx];
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == quote {
            return Some(idx);
        }
        idx += 1;
    }
    None
}

fn scan_one_sexp_backward(
    interp: &Interpreter,
    chars: &[char],
    from: usize,
    min: usize,
) -> Option<usize> {
    let mut idx = from.saturating_sub(2);
    let min_idx = min.saturating_sub(1);
    while idx >= min_idx && chars.get(idx).is_some_and(|ch| ch.is_whitespace()) {
        if idx == 0 {
            return None;
        }
        idx -= 1;
    }
    let table_id = interp.current_syntax_table_id();
    let entry = syntax_entry_at_buffer_position(interp, table_id, *chars.get(idx)?, idx + 1);
    match entry.class {
        SyntaxClass::CloseParen => scan_balanced_backward(chars, idx, min_idx).map(|idx| idx + 1),
        SyntaxClass::StringQuote => scan_string_backward(chars, idx, min_idx).map(|idx| idx + 1),
        SyntaxClass::OpenParen => None,
        _ => {
            while idx > min_idx
                && chars.get(idx - 1).is_some_and(|ch| !ch.is_whitespace())
                && matches!(
                    syntax_entry_at_buffer_position(interp, table_id, chars[idx - 1], idx).class,
                    SyntaxClass::Word | SyntaxClass::Symbol
                )
            {
                idx -= 1;
            }
            Some(idx + 1)
        }
    }
}

fn scan_balanced_backward(chars: &[char], close_idx: usize, min_idx: usize) -> Option<usize> {
    let mut stack = vec![matching_delimiter(chars[close_idx])?];
    let mut idx = close_idx;
    while idx > min_idx {
        idx -= 1;
        match chars[idx] {
            '"' => idx = scan_string_backward(chars, idx, min_idx)?,
            ')' | ']' | '}' => stack.push(matching_delimiter(chars[idx])?),
            '(' | '[' | '{' => {
                if stack.pop()? != chars[idx] {
                    return None;
                }
                if stack.is_empty() {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}

fn scan_string_backward(chars: &[char], quote_idx: usize, min_idx: usize) -> Option<usize> {
    let quote = *chars.get(quote_idx)?;
    let mut idx = quote_idx;
    while idx > min_idx {
        idx -= 1;
        if chars[idx] == quote && !is_escaped(chars, idx) {
            return Some(idx);
        }
    }
    None
}

fn matching_delimiter(ch: char) -> Option<char> {
    match ch {
        '(' => Some(')'),
        '[' => Some(']'),
        '{' => Some('}'),
        ')' => Some('('),
        ']' => Some('['),
        '}' => Some('{'),
        _ => None,
    }
}

fn is_escaped(chars: &[char], idx: usize) -> bool {
    let mut count = 0usize;
    let mut scan = idx;
    while scan > 0 && chars[scan - 1] == '\\' {
        count += 1;
        scan -= 1;
    }
    count % 2 == 1
}

pub(super) fn parse_forward(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    target_depth: Option<i64>,
    oldstate: Option<&Value>,
    commentstop: bool,
    env: &Env,
) -> Result<Value, LispError> {
    if from > to {
        return Err(LispError::Signal("`from` is greater than `to`".into()));
    }
    let chars: Vec<char> = interp.buffer.buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let comment_end_can_be_escaped = interp
        .lookup_var("comment-end-can-be-escaped", env)
        .is_some_and(|value| value.is_truthy());
    let mut state = decode_parse_state(oldstate);
    let mut idx = from.saturating_sub(1);
    let end = to.saturating_sub(1).min(chars.len());

    while idx < end {
        if let Some(string) = state.string {
            let ch = chars[idx];
            let entry = syntax_entry_for_char(interp, table_id, ch);
            if entry.class == SyntaxClass::StringQuote
                && ch == string.quote
                && !is_escaped(&chars, idx)
            {
                state.string = None;
            }
            idx += 1;
            continue;
        }

        if let Some(comment) = state.comment {
            match comment.kind {
                CommentKind::Single { line } => {
                    let entry = syntax_entry_for_char(interp, table_id, chars[idx]);
                    if entry.class == SyntaxClass::CommentEnd {
                        if line
                            && chars[idx] == '\n'
                            && comment_end_can_be_escaped
                            && preceded_by_odd_backslashes(&chars, idx)
                        {
                            idx += 1;
                            continue;
                        }
                        idx += 1;
                        state.comment = None;
                        if commentstop {
                            interp.buffer.goto_char(idx + 1);
                            return Ok(encode_parse_state(&state));
                        }
                        continue;
                    }
                    idx += 1;
                    continue;
                }
                CommentKind::Block {
                    end_first,
                    end_second,
                    nested,
                } => {
                    if nested
                        && let Some(start) = comment_start_at(interp, table_id, &chars, idx)
                        && matches!(
                            start.kind,
                            CommentKind::Block {
                                end_first: nested_end_first,
                                end_second: nested_end_second,
                                ..
                            } if nested_end_first == end_first && nested_end_second == end_second
                        )
                    {
                        if let Some(comment) = state.comment.as_mut() {
                            comment.depth += 1;
                        }
                        idx += start.len;
                        continue;
                    }
                    if idx + 1 < chars.len()
                        && chars[idx] == end_first
                        && chars[idx + 1] == end_second
                    {
                        if comment_end_can_be_escaped && preceded_by_odd_backslashes(&chars, idx) {
                            idx += 1;
                            continue;
                        }
                        if let Some(comment) = state.comment.as_mut()
                            && comment.depth > 1
                        {
                            comment.depth -= 1;
                            idx += 2;
                            continue;
                        }
                        idx += 2;
                        state.comment = None;
                        if commentstop {
                            interp.buffer.goto_char(idx + 1);
                            return Ok(encode_parse_state(&state));
                        }
                        continue;
                    }
                    idx += 1;
                    continue;
                }
            }
        }

        if let Some(start) = comment_start_at(interp, table_id, &chars, idx) {
            state.comment = Some(CommentState {
                kind: start.kind,
                start_pos: idx + 1,
                depth: 1,
            });
            idx += start.len;
            if commentstop {
                interp.buffer.goto_char(idx + 1);
                return Ok(encode_parse_state(&state));
            }
            continue;
        }

        let ch = chars[idx];
        let entry = syntax_entry_for_char(interp, table_id, ch);
        match entry.class {
            SyntaxClass::StringQuote => {
                if !is_escaped(&chars, idx) {
                    state.string = Some(StringState {
                        quote: ch,
                        start_pos: idx + 1,
                    });
                }
                idx += 1;
            }
            SyntaxClass::OpenParen => {
                let close_char = matching_close_char(ch, entry)
                    .ok_or_else(|| LispError::Signal("Unbalanced parentheses".into()))?;
                state.stack.push(ParseStackEntry {
                    open_pos: idx + 1,
                    close_char,
                });
                idx += 1;
            }
            SyntaxClass::CloseParen => {
                let Some(open) = state.stack.last() else {
                    state.base_depth -= 1;
                    state.min_depth = state.min_depth.min(state.depth());
                    idx += 1;
                    continue;
                };
                if open.close_char != ch
                    && matching_open_char(ch, entry)
                        .is_some_and(|open_char| open_char != chars[open.open_pos - 1])
                {
                    state.stack.pop();
                    state.min_depth = state.min_depth.min(state.depth());
                    idx += 1;
                    continue;
                }
                state.stack.pop();
                idx += 1;
                if target_depth.is_some_and(|depth| depth == state.depth()) {
                    interp.buffer.goto_char(idx + 1);
                    return Ok(encode_parse_state(&state));
                }
            }
            _ => idx += 1,
        }
    }

    interp.buffer.goto_char(end + 1);
    Ok(encode_parse_state(&state))
}

fn find_comment_ending_at(
    interp: &Interpreter,
    table_id: u64,
    chars: &[char],
    point: usize,
    comment_end_can_be_escaped: bool,
) -> Option<usize> {
    if point <= 1 {
        return None;
    }
    let mut best_start = None;
    let mut idx = point.saturating_sub(2).min(chars.len().saturating_sub(1));
    loop {
        if let Some(start) = comment_start_at(interp, table_id, chars, idx) {
            let (end, closed) = skip_comment_with_status(
                interp,
                table_id,
                chars,
                idx,
                start,
                comment_end_can_be_escaped,
            );
            if closed && end + 1 == point {
                best_start = Some(idx + 1);
            }
        }
        if idx == 0 {
            break;
        }
        idx -= 1;
    }
    if best_start.is_some() {
        return best_start;
    }

    if point >= 3 && point - 2 < chars.len() {
        let end_first = chars[point - 3];
        let end_second = chars[point - 2];
        let mut line_start = point.saturating_sub(2).min(chars.len().saturating_sub(1));
        while line_start > 0 && chars[line_start - 1] != '\n' {
            line_start -= 1;
        }
        let mut fallback = None;
        let mut idx = point.saturating_sub(2).min(chars.len().saturating_sub(1));
        loop {
            if let Some(start) = comment_start_at(interp, table_id, chars, idx)
                && let CommentKind::Block {
                    end_first: candidate_end_first,
                    end_second: candidate_end_second,
                    ..
                } = start.kind
                && candidate_end_first == end_first
                && candidate_end_second == end_second
                && point >= idx + start.len + 3
                && !(comment_end_can_be_escaped && preceded_by_odd_backslashes(chars, point - 3))
            {
                fallback = Some(idx + 1);
            }
            if idx == line_start {
                break;
            }
            idx -= 1;
        }
        return fallback;
    }

    None
}

fn syntax_class_char_matches(interp: &Interpreter, class: char, ch: char) -> bool {
    let entry = syntax_entry_for_char(interp, interp.current_syntax_table_id(), ch);
    match class {
        ' ' => entry.class == SyntaxClass::Whitespace,
        'w' => entry.class == SyntaxClass::Word,
        '_' => entry.class == SyntaxClass::Symbol,
        '.' => entry.class == SyntaxClass::Punctuation,
        '(' => entry.class == SyntaxClass::OpenParen,
        ')' => entry.class == SyntaxClass::CloseParen,
        '"' => entry.class == SyntaxClass::StringQuote,
        '\\' => entry.class == SyntaxClass::Escape,
        '\'' => entry.class == SyntaxClass::Quote,
        '<' => entry.class == SyntaxClass::CommentStart,
        '>' => entry.class == SyntaxClass::CommentEnd,
        '$' => entry.class == SyntaxClass::PairedDelimiter,
        '/' => entry.class == SyntaxClass::CharQuote,
        _ => false,
    }
}

fn syntax_class_matches(interp: &Interpreter, spec: &str, ch: char) -> bool {
    let (negated, classes) = spec
        .strip_prefix('^')
        .map(|rest| (true, rest))
        .unwrap_or((false, spec));
    let matched = classes
        .chars()
        .any(|class| syntax_class_char_matches(interp, class, ch));
    if negated { !matched } else { matched }
}

pub(super) fn skip_syntax_impl(
    interp: &mut Interpreter,
    syntax_value: &Value,
    limit_value: Option<&Value>,
    forward: bool,
) -> Result<Value, LispError> {
    let syntax = string_text(syntax_value)?;
    let limit = if let Some(limit_value) = limit_value {
        if limit_value.is_nil() {
            if forward {
                interp.buffer.point_max()
            } else {
                interp.buffer.point_min()
            }
        } else {
            position_from_value(interp, limit_value)?
        }
    } else if forward {
        interp.buffer.point_max()
    } else {
        interp.buffer.point_min()
    };
    let start = interp.buffer.point();
    if forward {
        while interp.buffer.point() < limit {
            let Some(ch) = interp.buffer.char_at(interp.buffer.point()) else {
                break;
            };
            if !syntax_class_matches(interp, &syntax, ch) {
                break;
            }
            let _ = interp.buffer.forward_char(1);
        }
    } else {
        while interp.buffer.point() > limit {
            let Some(ch) = interp.buffer.char_before() else {
                break;
            };
            if !syntax_class_matches(interp, &syntax, ch) {
                break;
            }
            let _ = interp.buffer.forward_char(-1);
        }
    }
    Ok(Value::Integer(interp.buffer.point() as i64 - start as i64))
}

pub(super) fn scan_lists_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
) -> Result<Value, LispError> {
    need_args("scan-lists", args, 3)?;
    let start_pos = position_from_value(interp, &args[0])?;
    let count = args[1].as_integer()?;
    let depth = args[2].as_integer()?;
    if depth != 0 || !matches!(count, -1 | 1) {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("scan-error".into()),
            Value::String("Unsupported scan-lists request".into()),
        ])));
    }

    let chars: Vec<char> = interp.buffer.buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let comment_end_can_be_escaped = interp
        .lookup_var("comment-end-can-be-escaped", env)
        .is_some_and(|value| value.is_truthy());

    if count > 0 {
        let idx = start_pos.saturating_sub(1);
        let Some(&ch) = chars.get(idx) else {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("scan-error".into()),
                Value::String("Unbalanced parentheses".into()),
            ])));
        };
        let entry = syntax_entry_for_char(interp, table_id, ch);
        if entry.class != SyntaxClass::OpenParen {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("scan-error".into()),
                Value::String("Unbalanced parentheses".into()),
            ])));
        }
        let close_char = matching_close_char(ch, entry).ok_or_else(|| {
            LispError::SignalValue(Value::list([
                Value::Symbol("scan-error".into()),
                Value::String("Unbalanced parentheses".into()),
            ]))
        })?;
        let mut state = ParseState {
            base_depth: 0,
            min_depth: 0,
            stack: vec![ParseStackEntry {
                open_pos: start_pos,
                close_char,
            }],
            comment: None,
            string: None,
        };
        let mut cursor = idx + 1;
        while cursor < chars.len() {
            if let Some(comment) = state.comment {
                match comment.kind {
                    CommentKind::Single { line } => {
                        let entry = syntax_entry_for_char(interp, table_id, chars[cursor]);
                        if entry.class == SyntaxClass::CommentEnd {
                            if line
                                && chars[cursor] == '\n'
                                && comment_end_can_be_escaped
                                && preceded_by_odd_backslashes(&chars, cursor)
                            {
                                cursor += 1;
                                continue;
                            }
                            state.comment = None;
                        }
                        cursor += 1;
                        continue;
                    }
                    CommentKind::Block {
                        end_first,
                        end_second,
                        nested,
                    } => {
                        if nested
                            && let Some(start) = comment_start_at(interp, table_id, &chars, cursor)
                            && matches!(
                                start.kind,
                                CommentKind::Block {
                                    end_first: nested_end_first,
                                    end_second: nested_end_second,
                                    ..
                                } if nested_end_first == end_first && nested_end_second == end_second
                            )
                        {
                            if let Some(comment) = state.comment.as_mut() {
                                comment.depth += 1;
                            }
                            cursor += start.len;
                            continue;
                        }
                        if cursor + 1 < chars.len()
                            && chars[cursor] == end_first
                            && chars[cursor + 1] == end_second
                        {
                            if comment_end_can_be_escaped
                                && preceded_by_odd_backslashes(&chars, cursor)
                            {
                                cursor += 1;
                                continue;
                            }
                            if let Some(comment) = state.comment.as_mut()
                                && comment.depth > 1
                            {
                                comment.depth -= 1;
                                cursor += 2;
                                continue;
                            }
                            state.comment = None;
                            cursor += 2;
                            continue;
                        }
                        cursor += 1;
                        continue;
                    }
                }
            }
            if let Some(start) = comment_start_at(interp, table_id, &chars, cursor) {
                state.comment = Some(CommentState {
                    kind: start.kind,
                    start_pos: cursor + 1,
                    depth: 1,
                });
                cursor += start.len;
                continue;
            }
            let ch = chars[cursor];
            let entry = syntax_entry_for_char(interp, table_id, ch);
            match entry.class {
                SyntaxClass::OpenParen => {
                    let close_char = matching_close_char(ch, entry).ok_or_else(|| {
                        LispError::SignalValue(Value::list([
                            Value::Symbol("scan-error".into()),
                            Value::String("Unbalanced parentheses".into()),
                        ]))
                    })?;
                    state.stack.push(ParseStackEntry {
                        open_pos: cursor + 1,
                        close_char,
                    });
                    cursor += 1;
                }
                SyntaxClass::CloseParen => {
                    let Some(open) = state.stack.pop() else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("scan-error".into()),
                            Value::String("Unbalanced parentheses".into()),
                        ])));
                    };
                    if open.close_char != ch {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("scan-error".into()),
                            Value::String("Unbalanced parentheses".into()),
                        ])));
                    }
                    cursor += 1;
                    if state.stack.is_empty() {
                        return Ok(Value::Integer((cursor + 1) as i64));
                    }
                }
                _ => cursor += 1,
            }
        }
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("scan-error".into()),
            Value::String("Unbalanced parentheses".into()),
        ])));
    }

    if start_pos <= 1 {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("scan-error".into()),
            Value::String("Unbalanced parentheses".into()),
        ])));
    }
    let close_idx = start_pos - 2;
    let Some(&close_char) = chars.get(close_idx) else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("scan-error".into()),
            Value::String("Unbalanced parentheses".into()),
        ])));
    };
    let close_entry = syntax_entry_for_char(interp, table_id, close_char);
    if close_entry.class != SyntaxClass::CloseParen {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("scan-error".into()),
            Value::String("Unbalanced parentheses".into()),
        ])));
    }
    let expected_open = matching_open_char(close_char, close_entry).ok_or_else(|| {
        LispError::SignalValue(Value::list([
            Value::Symbol("scan-error".into()),
            Value::String("Unbalanced parentheses".into()),
        ]))
    })?;
    let mut stack = vec![expected_open];
    let mut cursor = close_idx;
    while cursor > 0 {
        if let Some(comment_start) = find_comment_ending_at(
            interp,
            table_id,
            &chars,
            cursor + 2,
            comment_end_can_be_escaped,
        ) {
            cursor = comment_start.saturating_sub(1);
            continue;
        }
        cursor -= 1;
        let ch = chars[cursor];
        let entry = syntax_entry_for_char(interp, table_id, ch);
        match entry.class {
            SyntaxClass::CloseParen => {
                let expected = matching_open_char(ch, entry).ok_or_else(|| {
                    LispError::SignalValue(Value::list([
                        Value::Symbol("scan-error".into()),
                        Value::String("Unbalanced parentheses".into()),
                    ]))
                })?;
                stack.push(expected);
            }
            SyntaxClass::OpenParen => {
                let Some(expected) = stack.pop() else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("scan-error".into()),
                        Value::String("Unbalanced parentheses".into()),
                    ])));
                };
                if ch != expected {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("scan-error".into()),
                        Value::String("Unbalanced parentheses".into()),
                    ])));
                }
                if stack.is_empty() {
                    return Ok(Value::Integer((cursor + 1) as i64));
                }
            }
            _ => {}
        }
    }

    Err(LispError::SignalValue(Value::list([
        Value::Symbol("scan-error".into()),
        Value::String("Unbalanced parentheses".into()),
    ])))
}

pub(super) fn down_list_impl(
    interp: &mut Interpreter,
    count_value: Option<&Value>,
    _env: &Env,
) -> Result<Value, LispError> {
    let count = count_value.map_or(Ok(1), Value::as_integer)?;
    let chars: Vec<char> = interp.buffer.buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    if count < 0 {
        // Move backward down a list level: stop just before the close paren
        // of the previous list.
        for _ in 0..count.unsigned_abs() {
            let mut cursor = interp.buffer.point().checked_sub(2);
            let mut found = None;
            while let Some(idx) = cursor {
                let ch = chars[idx];
                let entry = syntax_entry_for_char(interp, table_id, ch);
                match entry.class {
                    SyntaxClass::StringQuote => {
                        let mut scan = idx.checked_sub(1);
                        while let Some(inner) = scan {
                            if chars[inner] == ch && !(inner > 0 && chars[inner - 1] == '\\') {
                                break;
                            }
                            scan = inner.checked_sub(1);
                        }
                        cursor = scan.and_then(|inner| inner.checked_sub(1));
                    }
                    SyntaxClass::CloseParen => {
                        found = Some(idx + 1);
                        break;
                    }
                    _ => cursor = idx.checked_sub(1),
                }
            }
            let Some(position) = found else {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("scan-error".into()),
                    Value::String("No containing expression".into()),
                ])));
            };
            interp.buffer.goto_char(position);
        }
        return Ok(Value::Nil);
    }
    for _ in 0..count {
        let mut idx = interp.buffer.point().saturating_sub(1);
        let mut found = None;
        while idx < chars.len() {
            let ch = chars[idx];
            let entry = syntax_entry_for_char(interp, table_id, ch);
            match entry.class {
                SyntaxClass::StringQuote => {
                    idx = scan_string_forward(&chars, idx, chars.len())
                        .map(|end| end + 1)
                        .unwrap_or(chars.len());
                }
                SyntaxClass::OpenParen => {
                    found = Some(idx + 2);
                    break;
                }
                _ => idx += 1,
            }
        }
        let Some(position) = found else {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("scan-error".into()),
                Value::String("No containing expression".into()),
            ])));
        };
        interp.buffer.goto_char(position);
    }
    Ok(Value::Nil)
}

pub(super) fn up_list_impl(
    interp: &mut Interpreter,
    count_value: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let count = count_value.map_or(Ok(1), Value::as_integer)?;
    if count < 0 {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("scan-error".into()),
            Value::String("Unsupported negative up-list".into()),
        ])));
    }
    let chars: Vec<char> = interp.buffer.buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    for _ in 0..count {
        let point_idx = interp.buffer.point().saturating_sub(1).min(chars.len());
        let mut stack: Vec<usize> = Vec::new();
        let mut idx = 0;
        while idx < point_idx {
            let ch = chars[idx];
            let entry = syntax_entry_for_char(interp, table_id, ch);
            match entry.class {
                SyntaxClass::StringQuote => {
                    idx = scan_string_forward(&chars, idx, point_idx)
                        .map(|end| end + 1)
                        .unwrap_or(point_idx);
                }
                SyntaxClass::OpenParen => {
                    stack.push(idx + 1);
                    idx += 1;
                }
                SyntaxClass::CloseParen => {
                    stack.pop();
                    idx += 1;
                }
                _ => idx += 1,
            }
        }
        let Some(open_pos) = stack.last().copied() else {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("scan-error".into()),
                Value::String("No containing expression".into()),
            ])));
        };
        let close_pos = scan_lists_impl(
            interp,
            &[
                Value::Integer(open_pos as i64),
                Value::Integer(1),
                Value::Integer(0),
            ],
            env,
        )?
        .as_integer()? as usize;
        interp.buffer.goto_char(close_pos);
    }
    Ok(Value::Nil)
}

pub(super) fn forward_comment_impl(
    interp: &mut Interpreter,
    count_value: Option<&Value>,
    env: &Env,
) -> Result<Value, LispError> {
    let count = count_value.map_or(Ok(1), Value::as_integer)?;
    if count == 0 {
        return Ok(Value::T);
    }

    let chars: Vec<char> = interp.buffer.buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let comment_end_can_be_escaped = interp
        .lookup_var("comment-end-can-be-escaped", env)
        .is_some_and(|value| value.is_truthy());
    let original_point = interp.buffer.point();

    if count > 0 {
        let mut point = original_point;
        for _ in 0..count {
            let candidate = skip_whitespace_forward(interp, table_id, &chars, point);
            let idx = candidate.saturating_sub(1);
            let Some(start) = comment_start_at(interp, table_id, &chars, idx) else {
                // GNU stops before the non-comment token, keeping the
                // whitespace crossed so far behind point.
                interp.buffer.goto_char(candidate);
                return Ok(Value::Nil);
            };
            let (end, closed) = skip_comment_with_status(
                interp,
                table_id,
                &chars,
                idx,
                start,
                comment_end_can_be_escaped,
            );
            point = end + 1;
            if !closed {
                interp.buffer.goto_char(point);
                return Ok(Value::Nil);
            }
        }
        interp.buffer.goto_char(point);
        return Ok(Value::T);
    }

    let mut point = original_point;
    for _ in 0..count.unsigned_abs() {
        if let Some(start_pos) =
            find_comment_ending_at(interp, table_id, &chars, point, comment_end_can_be_escaped)
        {
            point = start_pos;
            continue;
        }
        let candidate = skip_whitespace_backward(interp, table_id, &chars, point);
        if let Some(start_pos) = find_comment_ending_at(
            interp,
            table_id,
            &chars,
            candidate,
            comment_end_can_be_escaped,
        ) {
            point = start_pos;
            continue;
        }
        interp.buffer.goto_char(candidate);
        return Ok(Value::Nil);
    }
    interp.buffer.goto_char(point);
    Ok(Value::T)
}
