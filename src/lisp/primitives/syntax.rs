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
    style_c: bool,
    pub(super) prefix: bool,
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
            style_c: false,
            prefix: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommentKind {
    Single {
        line: bool,
    },
    Fence,
    Block {
        end_first: char,
        end_second: char,
        nested: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommentStart {
    kind: CommentKind,
    style: u8,
    len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommentState {
    kind: CommentKind,
    style: u8,
    start_pos: usize,
    depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StringState {
    quote: char,
    start_pos: usize,
    // Generic string fence (syntax class `|'): closes at the next
    // fence-classed character, and nth 3 reports `t' like GNU.
    fence: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParseStackEntry {
    open_pos: usize,
    close_char: char,
    // Start of the last complete sexp seen INSIDE this list (parse state
    // element 2 when this is the innermost level).
    last_sexp: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ParseState {
    base_depth: i64,
    min_depth: i64,
    stack: Vec<ParseStackEntry>,
    comment: Option<CommentState>,
    string: Option<StringState>,
    // Start of the last complete sexp at top level (element 2 when the
    // stack is empty).
    base_last_sexp: Option<usize>,
}

impl ParseState {
    fn depth(&self) -> i64 {
        self.base_depth + self.stack.len() as i64
    }

    fn last_sexp(&self) -> Option<usize> {
        match self.stack.last() {
            Some(entry) => entry.last_sexp,
            None => self.base_last_sexp,
        }
    }

    fn set_last_sexp(&mut self, position: usize) {
        match self.stack.last_mut() {
            Some(entry) => entry.last_sexp = Some(position),
            None => self.base_last_sexp = Some(position),
        }
    }
}

pub(super) fn syntax_class_from_char(ch: char) -> Option<SyntaxClass> {
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
            'c' => entry.style_c = true,
            'p' => entry.prefix = true,
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
    if entry.prefix {
        code |= 1 << 20;
    }
    if entry.style_b {
        code |= 1 << 21;
    }
    if entry.nested {
        code |= 1 << 22;
    }
    if entry.style_c {
        code |= 1 << 23;
    }
    code
}

pub(super) fn syntax_entry_value(entry: SyntaxEntry) -> Value {
    if entry.class == SyntaxClass::Inherit {
        return Value::Nil;
    }
    let code = Value::Integer(syntax_entry_code(entry));
    Value::cons(
        code,
        entry
            .matching
            .map(|matching| Value::Integer(matching as i64))
            .unwrap_or(Value::Nil),
    )
}

pub(super) fn describe_syntax_value(value: &Value) -> (String, bool) {
    if value.is_nil() {
        return ("default".into(), false);
    }
    if matches!(value, Value::CharTable(_)) {
        return ("deeper char-table ...".into(), false);
    }
    let Value::Cons(car, cdr) = value else {
        return ("invalid".into(), false);
    };
    let Value::Integer(raw_code) = *car.borrow() else {
        return ("invalid".into(), false);
    };
    let matching = match &*cdr.borrow() {
        Value::Nil => None,
        Value::Integer(code) => u32::try_from(*code).ok().and_then(char::from_u32),
        _ => return ("invalid".into(), false),
    };
    if !cdr.borrow().is_nil() && matching.is_none() {
        return ("invalid".into(), false);
    }
    let syntax_code = raw_code & i64::from(i32::MAX);
    let Some(class) = syntax_class_from_code(syntax_code & 0xff) else {
        return ("invalid".into(), false);
    };
    let entry = SyntaxEntry {
        class,
        matching,
        start_first: syntax_code & (1 << 16) != 0,
        start_second: syntax_code & (1 << 17) != 0,
        end_first: syntax_code & (1 << 18) != 0,
        end_second: syntax_code & (1 << 19) != 0,
        prefix: syntax_code & (1 << 20) != 0,
        style_b: syntax_code & (1 << 21) != 0,
        nested: syntax_code & (1 << 22) != 0,
        style_c: syntax_code & (1 << 23) != 0,
    };
    let mut description = String::new();
    description.push(syntax_class_char(class));
    description.push(entry.matching.unwrap_or(' '));
    if entry.start_first {
        description.push('1');
    }
    if entry.start_second {
        description.push('2');
    }
    if entry.end_first {
        description.push('3');
    }
    if entry.end_second {
        description.push('4');
    }
    if entry.prefix {
        description.push('p');
    }
    if entry.style_b {
        description.push('b');
    }
    if entry.style_c {
        description.push('c');
    }
    if entry.nested {
        description.push('n');
    }
    let meaning = match class {
        SyntaxClass::Whitespace => "whitespace",
        SyntaxClass::Punctuation => "punctuation",
        SyntaxClass::Word => "word",
        SyntaxClass::Symbol => "symbol",
        SyntaxClass::OpenParen => "open",
        SyntaxClass::CloseParen => "close",
        SyntaxClass::Quote => "prefix",
        SyntaxClass::StringQuote => "string",
        SyntaxClass::PairedDelimiter => "math",
        SyntaxClass::Escape => "escape",
        SyntaxClass::CharQuote => "charquote",
        SyntaxClass::CommentStart => "comment",
        SyntaxClass::CommentEnd => "endcomment",
        SyntaxClass::Inherit => "inherit",
        SyntaxClass::GenericCommentDelimiter => "comment fence",
        SyntaxClass::GenericStringDelimiter => "string fence",
    };
    description.push_str("\twhich means: ");
    description.push_str(meaning);
    if let Some(matching) = entry.matching {
        description.push_str(", matches ");
        description.push(matching);
    }
    if entry.start_first {
        description.push_str(",\n\t  is the first character of a comment-start sequence");
    }
    if entry.start_second {
        description.push_str(",\n\t  is the second character of a comment-start sequence");
    }
    if entry.end_first {
        description.push_str(",\n\t  is the first character of a comment-end sequence");
    }
    if entry.end_second {
        description.push_str(",\n\t  is the second character of a comment-end sequence");
    }
    if entry.style_b {
        description.push_str(" (comment style b)");
    }
    if entry.style_c {
        description.push_str(" (comment style c)");
    }
    if entry.nested {
        description.push_str(" (nestable)");
    }
    (description, entry.prefix)
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

// GNU standard-syntax-table classes for characters a syntax table leaves
// unset: `$'/`%' are word constituents, `&*+-/<=>|_' symbol constituents,
// alongside the usual delimiter/escape assignments.
fn default_syntax_entry(ch: char) -> SyntaxEntry {
    let class = match ch {
        ' ' | '\t' | '\n' | '\r' | '\u{000B}' | '\u{000C}' => SyntaxClass::Whitespace,
        '_' | '&' | '*' | '+' | '-' | '/' | '<' | '=' | '>' | '|' => SyntaxClass::Symbol,
        '$' | '%' => SyntaxClass::Word,
        '"' => SyntaxClass::StringQuote,
        '\\' => SyntaxClass::Escape,
        '(' | '[' | '{' => SyntaxClass::OpenParen,
        ')' | ']' | '}' => SyntaxClass::CloseParen,
        ch if ch.is_alphanumeric() => SyntaxClass::Word,
        _ => SyntaxClass::Punctuation,
    };
    let matching = match ch {
        '(' => Some(')'),
        '[' => Some(']'),
        '{' => Some('}'),
        ')' => Some('('),
        ']' => Some('['),
        '}' => Some('{'),
        _ => None,
    };
    SyntaxEntry {
        class,
        matching,
        ..SyntaxEntry::default()
    }
}

pub(crate) fn standard_syntax_table_default_value(code: u32) -> Option<Value> {
    char::from_u32(code).map(|ch| syntax_entry_value(default_syntax_entry(ch)))
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

/// ASCII characters whose effective entry in the current syntax table has
/// CLASS_CHAR.  Regexp syntax atoms such as `\s_' are table-dependent; a
/// fixed Unicode "word or symbol" approximation cannot distinguish `\sw'
/// from `\s_' and misses Lisp constituents such as `:'.
pub(crate) fn syntax_class_ascii_chars(interp: &Interpreter, class_char: char) -> Vec<char> {
    let Some(class) = syntax_class_from_char(class_char) else {
        return Vec::new();
    };
    let table_id = interp.current_syntax_table_id();
    (0..=0x7F)
        .filter(|&code| syntax_entry_for_code(interp, table_id, code).class == class)
        .map(|code| char::from_u32(code).expect("ASCII codepoint"))
        .collect()
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

pub(super) fn current_syntax_word_char(
    interp: &Interpreter,
    code: u32,
    include_symbols: bool,
) -> bool {
    let class = syntax_entry_for_code(interp, interp.current_syntax_table_id(), code).class;
    class == SyntaxClass::Word || (include_symbols && class == SyntaxClass::Symbol)
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
                prefix: code & (1 << 20) != 0,
                style_b: code & (1 << 21) != 0,
                nested: code & (1 << 22) != 0,
                style_c: code & (1 << 23) != 0,
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
    match property {
        Value::CharTable(property_table_id) => {
            Some(syntax_entry_for_char(interp, property_table_id, ch))
        }
        property => syntax_entry_from_value(&property),
    }
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
    let entry = syntax_entry_at_buffer_position(interp, table_id, ch, idx + 1);
    if entry.class == SyntaxClass::GenericCommentDelimiter {
        return Some(CommentStart {
            kind: CommentKind::Fence,
            style: 2,
            len: 1,
        });
    }
    if entry.class == SyntaxClass::CommentStart {
        return Some(CommentStart {
            kind: CommentKind::Single {
                line: entry.style_b && newline_ends_comments(interp, table_id),
            },
            style: scan_comment_style(&entry, None),
            len: 1,
        });
    }
    let next = *chars.get(idx + 1)?;
    let next_entry = syntax_entry_at_buffer_position(interp, table_id, next, idx + 2);
    if !(entry.start_first && next_entry.start_second) {
        return None;
    }
    if ch == next && entry.style_b && next_entry.style_b && newline_ends_comments(interp, table_id)
    {
        return Some(CommentStart {
            kind: CommentKind::Single { line: true },
            style: scan_comment_style(&next_entry, Some(&entry)),
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
        style: scan_comment_style(&next_entry, Some(&entry)),
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
                let entry =
                    syntax_entry_at_buffer_position(interp, table_id, chars[cursor], cursor + 1);
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
        CommentKind::Fence => {
            while cursor < chars.len() {
                let entry =
                    syntax_entry_at_buffer_position(interp, table_id, chars[cursor], cursor + 1);
                if entry.class == SyntaxClass::GenericCommentDelimiter {
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
                    && nested_start.style == start.style
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
                    let first = syntax_entry_at_buffer_position(
                        interp,
                        table_id,
                        chars[cursor],
                        cursor + 1,
                    );
                    let second = syntax_entry_at_buffer_position(
                        interp,
                        table_id,
                        chars[cursor + 1],
                        cursor + 2,
                    );
                    if scan_comment_style(&first, Some(&second)) != start.style {
                        cursor += 1;
                        continue;
                    }
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
    minimum: usize,
) -> usize {
    let mut idx = pos.saturating_sub(1);
    while idx >= minimum {
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
            ..
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
            kind: CommentKind::Fence,
            start_pos,
            depth,
            ..
        }) => Value::list([
            Value::Symbol("fence".into()),
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
            ..
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
                if string.fence { Value::T } else { Value::Nil },
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
        state
            .last_sexp()
            .map(|position| Value::Integer(position as i64))
            .unwrap_or(Value::Nil),
        state
            .string
            .map(|string| {
                if string.fence {
                    Value::T
                } else {
                    Value::Integer(string.quote as i64)
                }
            })
            .unwrap_or(Value::Nil),
        state.comment.map(comment_state_value).unwrap_or(Value::Nil),
        Value::Nil,
        Value::Integer(state.min_depth),
        state.comment.map_or(Value::Nil, |comment| {
            if comment.kind == CommentKind::Fence {
                Value::Symbol("syntax-table".into())
            } else if comment.style != 0 {
                Value::Integer(comment.style as i64)
            } else {
                Value::Nil
            }
        }),
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
    let public_depth = items
        .first()
        .and_then(|value| value.as_integer().ok())
        .unwrap_or(0);
    let mut state = ParseState {
        base_depth: public_depth,
        min_depth: items
            .get(6)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(0),
        ..Default::default()
    };
    let comment_style = items
        .get(7)
        .and_then(|value| value.as_integer().ok())
        .map(|style| style.clamp(0, u8::MAX as i64) as u8)
        .unwrap_or(0);
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
                last_sexp: None,
            });
        }
    }
    // Element 2 carries the current level's last complete sexp; like GNU,
    // outer levels' values are not recoverable from an old state.
    if let Some(last_sexp) = items
        .get(2)
        .and_then(|value| value.as_integer().ok())
        .filter(|position| *position > 0)
    {
        state.set_last_sexp(last_sexp as usize);
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
                style: comment_style,
                start_pos,
                depth,
            }),
            "line" => Some(CommentState {
                kind: CommentKind::Single { line: true },
                style: comment_style,
                start_pos,
                depth,
            }),
            "fence" => Some(CommentState {
                kind: CommentKind::Fence,
                style: 2,
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
                        style: comment_style,
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
            state.string = Some(StringState {
                quote,
                start_pos,
                fence: entries.get(2).is_some_and(Value::is_truthy),
            });
        }
    }
    // GNU internalizes element 0 of OLDSTATE directly.  Callers such as
    // sgml--syntax-propertize-ppss deliberately mutate that public depth
    // before resuming a parse, so Emaxx's private continuation payload must
    // preserve its stack detail without overriding the visible mutation.
    state.base_depth = public_depth - state.stack.len() as i64;
    state
}

// ── GNU syntax.c scan_lists port ──

fn scan_signal(message: &str, last_good: i64, from: i64) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("scan-error".into()),
        Value::String(message.into()),
        Value::Integer(last_good),
        Value::Integer(from),
    ]))
}

fn scan_char(chars: &[char], pos: i64) -> char {
    if pos < 1 {
        return '\0';
    }
    chars.get((pos - 1) as usize).copied().unwrap_or('\0')
}

fn scan_entry(interp: &Interpreter, table_id: u64, chars: &[char], pos: i64) -> SyntaxEntry {
    if pos < 1 {
        return SyntaxEntry::default();
    }
    match chars.get((pos - 1) as usize) {
        Some(&ch) => syntax_entry_at_buffer_position(interp, table_id, ch, pos as usize),
        None => SyntaxEntry::default(),
    }
}

// GNU SYNTAX_FLAGS_COMMENT_STYLE over a one- or two-char sequence.
fn scan_comment_style(first: &SyntaxEntry, second: Option<&SyntaxEntry>) -> u8 {
    u8::from(first.style_b || second.is_some_and(|entry| entry.style_b))
        | (u8::from(first.style_c || second.is_some_and(|entry| entry.style_c)) << 1)
}

// GNU syntax.c char_quoted: whether the char at POS is preceded by an odd
// number of escape/char-quote characters.
fn scan_char_quoted(
    interp: &Interpreter,
    table_id: u64,
    chars: &[char],
    pos: i64,
    beg: i64,
) -> bool {
    let mut quoted = false;
    let mut cursor = pos;
    while cursor > beg {
        cursor -= 1;
        let entry = scan_entry(interp, table_id, chars, cursor);
        if !matches!(entry.class, SyntaxClass::Escape | SyntaxClass::CharQuote) {
            break;
        }
        quoted = !quoted;
    }
    quoted
}

// GNU syntax.c forw_comment: FROM is just past the comment starter.  On
// success returns (true, position of the last comment-ender char); when the
// comment never ends returns (false, STOP).
fn scan_forw_comment(
    interp: &Interpreter,
    table_id: u64,
    chars: &[char],
    mut from: i64,
    stop: i64,
    comnested: bool,
    style: u8,
) -> (bool, i64) {
    let mut nesting: i64 = if comnested { 1 } else { -1 };
    loop {
        if from >= stop {
            return (false, stop);
        }
        let entry = scan_entry(interp, table_id, chars, from);
        let code = entry.class;
        if code == SyntaxClass::CommentEnd && scan_comment_style(&entry, None) == style {
            let ends = if entry.nested {
                if nesting > 0 {
                    nesting -= 1;
                    nesting == 0
                } else {
                    false
                }
            } else {
                nesting < 0
            };
            if ends {
                return (true, from);
            }
        }
        if code == SyntaxClass::GenericCommentDelimiter && style == 2 {
            return (true, from);
        }
        if nesting > 0
            && code == SyntaxClass::CommentStart
            && entry.nested
            && scan_comment_style(&entry, None) == style
        {
            nesting += 1;
        }
        from += 1;
        // Two-char comment ender.
        if from < stop && entry.end_first {
            let second = scan_entry(interp, table_id, chars, from);
            if second.end_second
                && scan_comment_style(&entry, Some(&second)) == style
                && (if entry.nested || second.nested {
                    nesting > 0
                } else {
                    nesting < 0
                })
            {
                nesting -= 1;
                if nesting <= 0 {
                    return (true, from);
                }
                from += 1;
                continue;
            }
        }
        // Two-char nested comment starter inside a nestable comment.
        if nesting > 0 && from < stop && entry.start_first {
            let second = scan_entry(interp, table_id, chars, from);
            if second.start_second
                && scan_comment_style(&entry, Some(&second)) == style
                && (entry.nested || second.nested)
            {
                from += 1;
                nesting += 1;
            }
        }
    }
}

// GNU syntax.c back_comment, answered through the parse-partial-sexp
// engine: FROM sits on a comment-ender char; when that position really is
// inside a comment, return the position of the comment starter.
fn scan_back_comment(interp: &mut Interpreter, env: &Env, from: i64) -> Option<i64> {
    let begv = interp.buffer.point_min() as i64;
    if from <= begv {
        return None;
    }
    let saved_point = interp.buffer.point();
    let state = parse_forward(
        interp,
        begv as usize,
        from as usize,
        None,
        false,
        None,
        CommentStop::No,
        env,
    );
    interp.buffer.goto_char(saved_point);
    let items = state.ok()?.to_vec().ok()?;
    let in_string = items.get(3).is_some_and(|value| value.is_truthy());
    let in_comment = items.get(4).is_some_and(|value| value.is_truthy());
    if in_string || !in_comment {
        return None;
    }
    items.get(8).and_then(|value| value.as_integer().ok())
}

// GNU scan primitives propertize lazily via UPDATE_SYNTAX_TABLE; run
// `syntax-propertize' up front when the buffer defines one.
pub(super) fn ensure_syntax_propertized(interp: &mut Interpreter, env: &mut Env) {
    if interp
        .lookup_var("syntax-propertize-function", env)
        .is_some_and(|value| value.is_truthy())
        && interp.has_lisp_function("syntax-propertize")
    {
        let max = interp.buffer.point_max();
        let _ = interp.call_function_value(
            Value::Symbol("syntax-propertize".into()),
            Some("syntax-propertize"),
            &[Value::Integer(max as i64)],
            env,
        );
    }
}

fn ensure_syntax_propertized_preserving_match_data(interp: &mut Interpreter, env: &mut Env) {
    let match_data = interp.last_match_data.clone();
    let match_buffer = interp.last_match_data_buffer_id;
    ensure_syntax_propertized(interp, env);
    interp.last_match_data = match_data;
    interp.last_match_data_buffer_id = match_buffer;
}

// Verbatim port of GNU syntax.c scan_lists: returns the position past the
// COUNTth object, nil when the scan ran off the buffer end at depth 0, and
// signals scan-error with GNU's obstacle positions otherwise.
pub(super) fn scan_lists_gnu(
    interp: &mut Interpreter,
    env: &mut Env,
    from0: i64,
    count0: i64,
    depth0: i64,
    sexpflag: bool,
) -> Result<Option<usize>, LispError> {
    // GNU scan-sexps may invoke a mode's lazy propertizer, but the scan
    // primitive itself does not expose regexp match-data changes made by
    // that propertizer to its caller.
    ensure_syntax_propertized_preserving_match_data(interp, env);
    let chars: Vec<char> = interp.buffer.buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let begv = interp.buffer.point_min() as i64;
    let zv = interp.buffer.point_max() as i64;
    let ignore_comments = interp
        .lookup_var("parse-sexp-ignore-comments", env)
        .is_some_and(|value| value.is_truthy());

    let mut count = count0;
    let mut depth = depth0;
    let min_depth = depth.min(0);
    let mut last_good = from0;
    let mut from = from0.clamp(begv, zv);
    let mut mathexit = false;

    while count > 0 {
        let stop = zv;
        let mut done = false;
        'forward: while from < stop {
            let c = scan_char(&chars, from);
            let entry = scan_entry(interp, table_id, &chars, from);
            let mut code = entry.class;
            let mut comstyle = scan_comment_style(&entry, None);
            let mut comnested = entry.nested;
            if depth == min_depth {
                last_good = from;
            }
            from += 1;
            if from < stop && entry.start_first && ignore_comments {
                let second = scan_entry(interp, table_id, &chars, from);
                if second.start_second {
                    code = SyntaxClass::CommentStart;
                    comstyle = scan_comment_style(&second, Some(&entry));
                    comnested = comnested || second.nested;
                    from += 1;
                }
            }
            if entry.prefix {
                continue;
            }
            match code {
                SyntaxClass::Escape
                | SyntaxClass::CharQuote
                | SyntaxClass::Word
                | SyntaxClass::Symbol => {
                    if matches!(code, SyntaxClass::Escape | SyntaxClass::CharQuote) {
                        if from == stop {
                            return Err(scan_signal("Unbalanced parentheses", last_good, from));
                        }
                        // The escaped char counts as a word constituent.
                        from += 1;
                    }
                    if depth != 0 || !sexpflag {
                        continue;
                    }
                    // This word counts as a sexp; stop at its end.
                    while from < stop {
                        let inner = scan_entry(interp, table_id, &chars, from);
                        match inner.class {
                            SyntaxClass::CharQuote | SyntaxClass::Escape => {
                                from += 1;
                                if from == stop {
                                    return Err(scan_signal(
                                        "Unbalanced parentheses",
                                        last_good,
                                        from,
                                    ));
                                }
                            }
                            SyntaxClass::Word | SyntaxClass::Symbol | SyntaxClass::Quote => {}
                            _ => break,
                        }
                        from += 1;
                    }
                    done = true;
                    break 'forward;
                }
                SyntaxClass::GenericCommentDelimiter | SyntaxClass::CommentStart => {
                    if code == SyntaxClass::GenericCommentDelimiter {
                        comstyle = 2;
                    }
                    if !ignore_comments {
                        continue;
                    }
                    let (found, out) = scan_forw_comment(
                        interp, table_id, &chars, from, stop, comnested, comstyle,
                    );
                    from = out;
                    if !found {
                        // Unterminated comment: end of sexp at depth 0
                        // (GNU returns the position), unbalanced otherwise.
                        if depth == 0 {
                            done = true;
                            break 'forward;
                        }
                        return Err(scan_signal("Unbalanced parentheses", last_good, from));
                    }
                    from += 1;
                }
                SyntaxClass::PairedDelimiter if sexpflag => {
                    if from != stop && c == scan_char(&chars, from) {
                        from += 1;
                    }
                    if mathexit {
                        mathexit = false;
                        depth -= 1;
                        if depth == 0 {
                            done = true;
                            break 'forward;
                        }
                        if depth < min_depth {
                            return Err(scan_signal(
                                "Containing expression ends prematurely",
                                last_good,
                                from,
                            ));
                        }
                    } else {
                        mathexit = true;
                        depth += 1;
                        if depth == 0 {
                            done = true;
                            break 'forward;
                        }
                    }
                }
                SyntaxClass::OpenParen => {
                    depth += 1;
                    if depth == 0 {
                        done = true;
                        break 'forward;
                    }
                }
                SyntaxClass::CloseParen => {
                    depth -= 1;
                    if depth == 0 {
                        done = true;
                        break 'forward;
                    }
                    if depth < min_depth {
                        return Err(scan_signal(
                            "Containing expression ends prematurely",
                            last_good,
                            from,
                        ));
                    }
                }
                SyntaxClass::StringQuote | SyntaxClass::GenericStringDelimiter => {
                    let stringterm = c;
                    loop {
                        if from >= stop {
                            return Err(scan_signal("Unbalanced parentheses", last_good, from));
                        }
                        let inner = scan_entry(interp, table_id, &chars, from);
                        let terminates = if code == SyntaxClass::StringQuote {
                            scan_char(&chars, from) == stringterm
                                && inner.class == SyntaxClass::StringQuote
                        } else {
                            inner.class == SyntaxClass::GenericStringDelimiter
                        };
                        if terminates {
                            break;
                        }
                        if matches!(inner.class, SyntaxClass::CharQuote | SyntaxClass::Escape) {
                            from += 1;
                        }
                        from += 1;
                    }
                    from += 1;
                    if depth == 0 && sexpflag {
                        done = true;
                        break 'forward;
                    }
                }
                _ => {}
            }
        }
        if !done {
            // Reached end of buffer: error if within an object, nil between.
            if depth != 0 {
                return Err(scan_signal("Unbalanced parentheses", last_good, from));
            }
            return Ok(None);
        }
        count -= 1;
    }

    while count < 0 {
        let stop = begv;
        let mut done = false;
        'backward: while from > stop {
            from -= 1;
            let c = scan_char(&chars, from);
            let entry = scan_entry(interp, table_id, &chars, from);
            let mut code = entry.class;
            if depth == min_depth {
                last_good = from;
            }
            if from > stop && entry.end_second && ignore_comments {
                let prev = scan_entry(interp, table_id, &chars, from - 1);
                if prev.end_first {
                    from -= 1;
                    code = SyntaxClass::CommentEnd;
                }
            }
            // Quoting turns anything except a comment-ender into a word
            // character (cannot hold if FROM was decremented above).
            if code != SyntaxClass::CommentEnd
                && scan_char_quoted(interp, table_id, &chars, from, stop)
            {
                from -= 1;
                code = SyntaxClass::Word;
            } else if entry.prefix {
                continue;
            }
            match code {
                SyntaxClass::Word
                | SyntaxClass::Symbol
                | SyntaxClass::Escape
                | SyntaxClass::CharQuote => {
                    if depth != 0 || !sexpflag {
                        continue;
                    }
                    // This word counts as a sexp; stop after passing it.
                    while from > stop {
                        let before = scan_entry(interp, table_id, &chars, from - 1);
                        // Don't allow a comment-end to be quoted.
                        if before.class == SyntaxClass::CommentEnd {
                            break;
                        }
                        let quoted = scan_char_quoted(interp, table_id, &chars, from - 1, stop);
                        if quoted {
                            from -= 1;
                        }
                        if !quoted {
                            match scan_entry(interp, table_id, &chars, from - 1).class {
                                SyntaxClass::Word | SyntaxClass::Symbol | SyntaxClass::Quote => {}
                                _ => break,
                            }
                        }
                        from -= 1;
                    }
                    done = true;
                    break 'backward;
                }
                SyntaxClass::PairedDelimiter if sexpflag => {
                    if from > begv && from != stop && c == scan_char(&chars, from - 1) {
                        from -= 1;
                    }
                    if mathexit {
                        mathexit = false;
                        depth -= 1;
                        if depth == 0 {
                            done = true;
                            break 'backward;
                        }
                        if depth < min_depth {
                            return Err(scan_signal(
                                "Containing expression ends prematurely",
                                last_good,
                                from,
                            ));
                        }
                    } else {
                        mathexit = true;
                        depth += 1;
                        if depth == 0 {
                            done = true;
                            break 'backward;
                        }
                    }
                }
                SyntaxClass::CloseParen => {
                    depth += 1;
                    if depth == 0 {
                        done = true;
                        break 'backward;
                    }
                }
                SyntaxClass::OpenParen => {
                    depth -= 1;
                    if depth == 0 {
                        done = true;
                        break 'backward;
                    }
                    if depth < min_depth {
                        return Err(scan_signal(
                            "Containing expression ends prematurely",
                            last_good,
                            from,
                        ));
                    }
                }
                SyntaxClass::CommentEnd => {
                    if ignore_comments && let Some(start) = scan_back_comment(interp, env, from) {
                        from = start;
                    }
                }
                SyntaxClass::GenericCommentDelimiter | SyntaxClass::GenericStringDelimiter => {
                    let fence_class = code;
                    loop {
                        if from == stop {
                            return Err(scan_signal("Unbalanced parentheses", last_good, from));
                        }
                        from -= 1;
                        if !scan_char_quoted(interp, table_id, &chars, from, stop)
                            && scan_entry(interp, table_id, &chars, from).class == fence_class
                        {
                            break;
                        }
                    }
                    if fence_class == SyntaxClass::GenericStringDelimiter && depth == 0 && sexpflag
                    {
                        done = true;
                        break 'backward;
                    }
                }
                SyntaxClass::StringQuote => {
                    let stringterm = c;
                    loop {
                        if from == stop {
                            return Err(scan_signal("Unbalanced parentheses", last_good, from));
                        }
                        from -= 1;
                        if !scan_char_quoted(interp, table_id, &chars, from, stop)
                            && scan_char(&chars, from) == stringterm
                            && scan_entry(interp, table_id, &chars, from).class
                                == SyntaxClass::StringQuote
                        {
                            break;
                        }
                    }
                    if depth == 0 && sexpflag {
                        done = true;
                        break 'backward;
                    }
                }
                _ => {}
            }
        }
        if !done {
            // Reached start of buffer: error if within an object, nil between.
            if depth != 0 {
                return Err(scan_signal("Unbalanced parentheses", last_good, from));
            }
            return Ok(None);
        }
        count += 1;
    }

    Ok(Some(from as usize))
}

pub(super) fn scan_sexps_position(
    interp: &mut Interpreter,
    env: &mut Env,
    from: usize,
    count: i64,
) -> Option<usize> {
    scan_lists_gnu(interp, env, from as i64, count, 0, true)
        .ok()
        .flatten()
}

pub(super) fn scan_sexps_position_for_scan_sexps(
    interp: &mut Interpreter,
    env: &mut Env,
    from: usize,
    count: i64,
) -> Result<Option<usize>, LispError> {
    scan_lists_gnu(interp, env, from as i64, count, 0, true)
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

fn is_escaped(chars: &[char], idx: usize) -> bool {
    let mut count = 0usize;
    let mut scan = idx;
    while scan > 0 && chars[scan - 1] == '\\' {
        count += 1;
        scan -= 1;
    }
    count % 2 == 1
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CommentStop {
    No,
    // Any other non-nil COMMENTSTOP: stop at comment boundaries.
    Plain,
    // The symbol `syntax-table': also stop after a string starts or ends.
    SyntaxTable,
}

impl CommentStop {
    pub(super) fn from_value(value: Option<&Value>) -> Self {
        match value {
            None | Some(Value::Nil) => CommentStop::No,
            Some(Value::Symbol(name)) if name == "syntax-table" => CommentStop::SyntaxTable,
            Some(other) if other.is_truthy() => CommentStop::Plain,
            Some(_) => CommentStop::No,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn parse_forward(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    target_depth: Option<i64>,
    stopbefore: bool,
    oldstate: Option<&Value>,
    commentstop: CommentStop,
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
    // Whether we are inside a word/symbol token; token STARTS record the
    // level's last-sexp position (parse state element 2).
    let mut in_symbol = false;

    while idx < end {
        if let Some(string) = state.string {
            let ch = chars[idx];
            let entry = syntax_entry_at_buffer_position(interp, table_id, ch, idx + 1);
            if string.fence {
                if entry.class == SyntaxClass::GenericStringDelimiter
                    && !scan_char_quoted(
                        interp,
                        table_id,
                        &chars,
                        idx as i64 + 1,
                        interp.buffer.point_min() as i64,
                    )
                {
                    state.string = None;
                    idx += 1;
                    if commentstop == CommentStop::SyntaxTable {
                        interp.buffer.goto_char(idx + 1);
                        return Ok(encode_parse_state(&state));
                    }
                    continue;
                }
                idx += 1;
                continue;
            }
            if entry.class == SyntaxClass::StringQuote
                && ch == string.quote
                && !scan_char_quoted(
                    interp,
                    table_id,
                    &chars,
                    idx as i64 + 1,
                    interp.buffer.point_min() as i64,
                )
            {
                state.string = None;
                idx += 1;
                if commentstop == CommentStop::SyntaxTable {
                    interp.buffer.goto_char(idx + 1);
                    return Ok(encode_parse_state(&state));
                }
                continue;
            }
            idx += 1;
            continue;
        }

        if let Some(comment) = state.comment {
            match comment.kind {
                CommentKind::Single { line } => {
                    let entry =
                        syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1);
                    if entry.class == SyntaxClass::CommentEnd
                        && scan_comment_style(&entry, None) == comment.style
                    {
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
                        if commentstop != CommentStop::No {
                            interp.buffer.goto_char(idx + 1);
                            return Ok(encode_parse_state(&state));
                        }
                        continue;
                    }
                    idx += 1;
                    continue;
                }
                CommentKind::Fence => {
                    let entry =
                        syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1);
                    if entry.class == SyntaxClass::GenericCommentDelimiter {
                        idx += 1;
                        state.comment = None;
                        if commentstop != CommentStop::No {
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
                        && start.style == comment.style
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
                        let first =
                            syntax_entry_at_buffer_position(interp, table_id, chars[idx], idx + 1);
                        let second = syntax_entry_at_buffer_position(
                            interp,
                            table_id,
                            chars[idx + 1],
                            idx + 2,
                        );
                        if scan_comment_style(&first, Some(&second)) != comment.style {
                            idx += 1;
                            continue;
                        }
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
                        if commentstop != CommentStop::No {
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
                style: start.style,
                start_pos: idx + 1,
                depth: 1,
            });
            idx += start.len;
            if commentstop != CommentStop::No {
                interp.buffer.goto_char(idx + 1);
                return Ok(encode_parse_state(&state));
            }
            continue;
        }

        let ch = chars[idx];
        // syntax-table TEXT PROPERTIES override the char table (generic
        // string fences from syntax-propertize live there).
        let entry = syntax_entry_at_buffer_position(interp, table_id, ch, idx + 1);
        if entry.class == SyntaxClass::GenericStringDelimiter {
            state.set_last_sexp(idx + 1);
            state.string = Some(StringState {
                quote: ch,
                start_pos: idx + 1,
                fence: true,
            });
            in_symbol = false;
            idx += 1;
            if commentstop == CommentStop::SyntaxTable {
                interp.buffer.goto_char(idx + 1);
                return Ok(encode_parse_state(&state));
            }
            continue;
        }
        // GNU's STOPBEFORE: stop with point before any character that
        // begins a sexp (symbol continuations excluded).
        if stopbefore
            && match entry.class {
                SyntaxClass::OpenParen | SyntaxClass::StringQuote | SyntaxClass::Quote => true,
                SyntaxClass::Word
                | SyntaxClass::Symbol
                | SyntaxClass::Escape
                | SyntaxClass::CharQuote => !in_symbol,
                _ => false,
            }
        {
            interp.buffer.goto_char(idx + 1);
            return Ok(encode_parse_state(&state));
        }
        match entry.class {
            SyntaxClass::StringQuote => {
                if !scan_char_quoted(
                    interp,
                    table_id,
                    &chars,
                    idx as i64 + 1,
                    interp.buffer.point_min() as i64,
                ) {
                    // GNU records the string as the level's last sexp at
                    // its opening quote.
                    state.set_last_sexp(idx + 1);
                    state.string = Some(StringState {
                        quote: ch,
                        start_pos: idx + 1,
                        fence: false,
                    });
                    in_symbol = false;
                    idx += 1;
                    if commentstop == CommentStop::SyntaxTable {
                        interp.buffer.goto_char(idx + 1);
                        return Ok(encode_parse_state(&state));
                    }
                    continue;
                }
                in_symbol = false;
                idx += 1;
            }
            SyntaxClass::OpenParen => {
                let close_char = matching_close_char(ch, entry)
                    .ok_or_else(|| LispError::Signal("Unbalanced parentheses".into()))?;
                state.stack.push(ParseStackEntry {
                    open_pos: idx + 1,
                    close_char,
                    last_sexp: None,
                });
                in_symbol = false;
                idx += 1;
                // GNU stops when the depth crossing reaches TARGETDEPTH in
                // either direction.
                if target_depth.is_some_and(|depth| depth == state.depth()) {
                    interp.buffer.goto_char(idx + 1);
                    return Ok(encode_parse_state(&state));
                }
            }
            SyntaxClass::CloseParen => {
                in_symbol = false;
                let Some(open) = state.stack.last() else {
                    state.base_depth -= 1;
                    state.min_depth = state.min_depth.min(state.depth());
                    idx += 1;
                    if target_depth.is_some_and(|depth| depth == state.depth()) {
                        interp.buffer.goto_char(idx + 1);
                        return Ok(encode_parse_state(&state));
                    }
                    continue;
                };
                if open.close_char != ch
                    && matching_open_char(ch, entry)
                        .is_some_and(|open_char| open_char != chars[open.open_pos - 1])
                {
                    let closed = state.stack.pop().expect("stack non-empty");
                    state.set_last_sexp(closed.open_pos);
                    state.min_depth = state.min_depth.min(state.depth());
                    idx += 1;
                    if target_depth.is_some_and(|depth| depth == state.depth()) {
                        interp.buffer.goto_char(idx + 1);
                        return Ok(encode_parse_state(&state));
                    }
                    continue;
                }
                let closed = state.stack.pop().expect("stack non-empty");
                // The list just closed becomes the enclosing level's last
                // complete sexp.
                state.set_last_sexp(closed.open_pos);
                idx += 1;
                if target_depth.is_some_and(|depth| depth == state.depth()) {
                    interp.buffer.goto_char(idx + 1);
                    return Ok(encode_parse_state(&state));
                }
            }
            SyntaxClass::Word | SyntaxClass::Symbol => {
                if !in_symbol {
                    // A word/symbol token starts here; GNU records it as
                    // the level's last sexp immediately.
                    state.set_last_sexp(idx + 1);
                    in_symbol = true;
                }
                idx += 1;
            }
            SyntaxClass::Escape | SyntaxClass::CharQuote => {
                if !in_symbol {
                    state.set_last_sexp(idx + 1);
                    in_symbol = true;
                }
                // The escape consumes the following character too.
                idx += 2;
            }
            _ => {
                in_symbol = false;
                idx += 1;
            }
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
    minimum: usize,
    comment_end_can_be_escaped: bool,
) -> Option<usize> {
    if point <= minimum {
        return None;
    }
    let minimum_index = minimum.saturating_sub(1);
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
        if idx == minimum_index {
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
        while line_start > minimum_index && chars[line_start - 1] != '\n' {
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

fn syntax_entry_class_matches(entry: SyntaxEntry, class: char) -> bool {
    match class {
        // GNU accepts both ` ' and `-' as the whitespace class designator.
        ' ' | '-' => entry.class == SyntaxClass::Whitespace,
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
        '@' => entry.class == SyntaxClass::Inherit,
        '!' => entry.class == SyntaxClass::GenericCommentDelimiter,
        '|' => entry.class == SyntaxClass::GenericStringDelimiter,
        _ => false,
    }
}

fn syntax_class_char_matches(interp: &Interpreter, class: char, ch: char) -> bool {
    syntax_entry_class_matches(
        syntax_entry_for_char(interp, interp.current_syntax_table_id(), ch),
        class,
    )
}

pub(super) fn syntax_class_at_buffer_position_matches(
    interp: &Interpreter,
    env: &Env,
    position: usize,
    class: char,
) -> bool {
    let Some(ch) = interp.buffer.char_at(position) else {
        return false;
    };
    let table_id = interp.current_syntax_table_id();
    let entry = if interp
        .lookup_var("parse-sexp-lookup-properties", env)
        .is_some_and(|value| value.is_truthy())
    {
        syntax_entry_at_buffer_position(interp, table_id, ch, position)
    } else {
        syntax_entry_for_char(interp, table_id, ch)
    };
    syntax_entry_class_matches(entry, class)
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
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("scan-lists", args, 3)?;
    let from = args[0].as_integer()?;
    let count = args[1].as_integer()?;
    let depth = args[2].as_integer()?;
    Ok(scan_lists_gnu(interp, env, from, count, depth, false)?
        .map(|position| Value::Integer(position as i64))
        .unwrap_or(Value::Nil))
}

pub(super) fn down_list_impl(
    interp: &mut Interpreter,
    count_value: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let count = count_value.map_or(Ok(1), Value::as_integer)?;
    if count > 0 {
        // GNU lisp.el down-list: (goto-char (scan-lists (point) 1 -1)),
        // which honors `parse-sexp-ignore-comments' (a `(' inside a comment
        // must not count as a list opener).
        for _ in 0..count {
            let from = interp.buffer.point() as i64;
            match scan_lists_gnu(interp, env, from, 1, -1, false)? {
                Some(position) => {
                    interp.buffer.goto_char(position);
                }
                None => {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("scan-error".into()),
                        Value::String("No containing expression".into()),
                    ])));
                }
            }
        }
        return Ok(Value::Nil);
    }
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
    env: &mut Env,
) -> Result<Value, LispError> {
    let count = count_value.map_or(Ok(1), Value::as_integer)?;
    let chars: Vec<char> = interp.buffer.buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    for _ in 0..count.unsigned_abs() {
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
        if count < 0 {
            // Negative COUNT moves backward out of the enclosing list,
            // landing before its open paren like GNU.
            interp.buffer.goto_char(open_pos);
            continue;
        }
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

    let minimum = interp.buffer.point_min();
    let mut chars: Vec<char> = interp.buffer.full_buffer_string().chars().collect();
    chars.truncate(interp.buffer.point_max().saturating_sub(1));
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
        if let Some(start_pos) = find_comment_ending_at(
            interp,
            table_id,
            &chars,
            point,
            minimum,
            comment_end_can_be_escaped,
        ) {
            point = start_pos;
            continue;
        }
        let candidate = skip_whitespace_backward(interp, table_id, &chars, point, minimum);
        if let Some(start_pos) = find_comment_ending_at(
            interp,
            table_id,
            &chars,
            candidate,
            minimum,
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

// GNU `backward-prefix-chars': move point backward over any number of
// characters with quote or prefix syntax (', #, \` and , in Lisp).
pub(super) fn backward_prefix_chars(interp: &mut Interpreter) -> Result<Value, LispError> {
    // Point and point-min are absolute buffer positions even while narrowed.
    // Index the full buffer rather than the accessible substring.
    let chars: Vec<char> = interp.buffer.full_buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let minimum = interp.buffer.point_min();
    let mut position = interp.buffer.point();
    while position > minimum {
        let ch = chars[position - 2];
        let entry = syntax_entry_for_char(interp, table_id, ch);
        if !(entry.class == SyntaxClass::Quote || entry.prefix) || is_escaped(&chars, position - 2)
        {
            break;
        }
        position -= 1;
    }
    interp.buffer.goto_char(position);
    Ok(Value::Nil)
}
