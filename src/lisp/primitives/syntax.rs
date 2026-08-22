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
        // GNU syntax.c accepts both descriptor spellings.  `-' is commonly
        // used by syntax propertizers because it is visible in tables and
        // regular expressions, unlike a leading space.
        ' ' | '-' => SyntaxClass::Whitespace,
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
    let mut chars = spec.chars();
    let class = syntax_class_from_char(chars.next()?)?;
    let mut entry = SyntaxEntry {
        class,
        ..SyntaxEntry::default()
    };
    if matches!(class, SyntaxClass::OpenParen | SyntaxClass::CloseParen) {
        entry.matching = chars.next();
    }
    for flag in chars {
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

#[cfg(test)]
mod syntax_spec_tests {
    use super::{SyntaxClass, parse_syntax_spec};

    #[test]
    fn parses_matching_characters_and_flags_without_changing_the_grammar() {
        let entry = parse_syntax_spec("(λ1np").expect("valid open-delimiter syntax");
        assert_eq!(entry.class, SyntaxClass::OpenParen);
        assert_eq!(entry.matching, Some('λ'));
        assert!(entry.start_first);
        assert!(entry.nested);
        assert!(entry.prefix);

        let word = parse_syntax_spec("w2c").expect("valid word syntax");
        assert_eq!(word.class, SyntaxClass::Word);
        assert!(word.start_second);
        assert!(word.style_c);
        assert_eq!(
            parse_syntax_spec("-")
                .expect("dash is GNU whitespace syntax")
                .class,
            SyntaxClass::Whitespace
        );
        assert!(parse_syntax_spec("").is_none());
        assert!(parse_syntax_spec("?").is_none());
    }
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
    let Some((car, cdr)) = (value).cons_cells() else {
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
        ' ' | '\t' | '\n' | '\r' | '\u{000C}' => SyntaxClass::Whitespace,
        '_' | '&' | '*' | '+' | '-' | '/' | '<' | '=' | '>' | '|' => SyntaxClass::Symbol,
        '$' | '%' => SyntaxClass::Word,
        '"' => SyntaxClass::StringQuote,
        '\\' => SyntaxClass::Escape,
        '(' | '[' | '{' => SyntaxClass::OpenParen,
        ')' | ']' | '}' => SyntaxClass::CloseParen,
        '0'..='9' | 'A'..='Z' | 'a'..='z' => SyntaxClass::Word,
        // GNU initializes the complete multibyte range as word syntax,
        // independent of Unicode alphabetic properties.
        ch if ch as u32 >= 0x80 => SyntaxClass::Word,
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

pub(super) fn syntax_entry_for_code(interp: &Interpreter, table_id: u64, code: u32) -> SyntaxEntry {
    // GNU character codes include the raw-byte range above Unicode's scalar
    // limit.  Keep the public code as the char-table key, but use the shared
    // character boundary to obtain Emaxx's internal marker when a default
    // syntax entry has to be derived.
    let Ok(ch) = char_from_integer(i64::from(code)) else {
        return SyntaxEntry::default();
    };
    let Some((explicit, terminal)) = interp.char_table_explicit_or_terminal(table_id, code) else {
        return default_syntax_entry(ch);
    };
    let value = match explicit {
        Some(value) => value,
        None if terminal.id == interp.standard_syntax_table_id() && terminal.default.is_nil() => {
            return default_syntax_entry(ch);
        }
        None => &terminal.default,
    };
    let entry = match value {
        Value::Nil => SyntaxEntry {
            // A nil entry in a syntax table denotes whitespace.  In
            // particular, `(make-char-table 'syntax-table nil)' is the
            // intentionally blank table used by syntax propertizers to make
            // every character insignificant except explicitly installed
            // delimiters.
            class: SyntaxClass::Whitespace,
            ..SyntaxEntry::default()
        },
        value => syntax_entry_from_value(value).unwrap_or_else(|| default_syntax_entry(ch)),
    };
    if entry.class == SyntaxClass::Inherit && table_id != interp.standard_syntax_table_id() {
        syntax_entry_for_code(interp, interp.standard_syntax_table_id(), code)
    } else {
        entry
    }
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
        Value::Cons(_) => {
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
        _ => string_like(value)
            .and_then(|value| parse_syntax_spec(&value.text))
            .or_else(|| {
                value
                    .to_vec()
                    .ok()
                    .and_then(|items| items.first().cloned())
                    .and_then(|item| syntax_entry_from_value(&item))
            }),
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

// syntax.h's gl_state, per scan: `parse-sexp-lookup-properties' is read
// once at setup (GNU reads a C global, never a per-character variable),
// and for property-armed scans the effective syntax source is resolved
// once per text-property interval -- UPDATE_SYNTAX_TABLE's
// b_property/e_property bounds -- instead of per character.  An ASCII
// memo stands in for GNU's SYNTAX(c) array indexing, since this
// runtime's tables store descriptors that would otherwise be re-decoded
// per character.  A mid-scan table or property mutation is observed at
// the next interval crossing, exactly as in GNU.
pub(super) struct SyntaxScan {
    table_id: u64,
    use_properties: bool,
    b_property: usize,
    e_property: usize,
    effective: EffectiveSyntax,
    ascii_memo: [Option<SyntaxEntry>; 128],
    // Plain buffer-table entries, property-independent: the scan's table
    // is fixed for its lifetime, so this memo never invalidates.
    plain_memo: [Option<SyntaxEntry>; 128],
}

#[derive(Clone, Copy)]
enum EffectiveSyntax {
    Table(u64),
    Direct(SyntaxEntry),
}

impl SyntaxScan {
    pub(super) fn table_id(&self) -> u64 {
        self.table_id
    }

    pub(super) fn new(interp: &Interpreter, table_id: u64) -> Self {
        // syntax.c:253 (SETUP_SYNTAX_TABLE): the property machinery arms
        // only when `parse-sexp-lookup-properties' is non-nil.
        let use_properties = interp
            .lookup_var("parse-sexp-lookup-properties", &Vec::new())
            .is_some_and(|value| value.is_truthy());
        SyntaxScan {
            table_id,
            use_properties,
            b_property: 1,
            e_property: 0, // empty interval: first query refreshes
            effective: EffectiveSyntax::Table(table_id),
            ascii_memo: [None; 128],
            plain_memo: [None; 128],
        }
    }

    pub(super) fn plain_table_entry(&mut self, interp: &Interpreter, ch: char) -> SyntaxEntry {
        let index = ch as usize;
        if index < 128 {
            if let Some(entry) = self.plain_memo[index] {
                return entry;
            }
            let entry = syntax_entry_for_char(interp, self.table_id, ch);
            self.plain_memo[index] = Some(entry);
            return entry;
        }
        syntax_entry_for_char(interp, self.table_id, ch)
    }

    fn refresh(&mut self, interp: &Interpreter, pos: usize) {
        let (start, end) = interp.buffer.text_property_interval_around(pos);
        self.b_property = start;
        self.e_property = end.max(pos + 1);
        // syntax.c:374 (update_syntax_table): the property comes off the
        // interval plist with `textget' -- buffer text properties (with
        // category indirection) only; overlays never feed syntax scans.
        let property = crate::lisp::primitives::strings::buffer_property_at_with_category(
            interp,
            &interp.buffer,
            pos,
            "syntax-table",
        )
        .unwrap_or(Value::Nil);
        self.effective = match property {
            Value::CharTable(property_table_id) => EffectiveSyntax::Table(property_table_id),
            Value::Nil => EffectiveSyntax::Table(self.table_id),
            property => syntax_entry_from_value(&property)
                .map(EffectiveSyntax::Direct)
                .unwrap_or(EffectiveSyntax::Table(self.table_id)),
        };
        self.ascii_memo = [None; 128];
    }

    pub(super) fn entry_at(&mut self, interp: &Interpreter, ch: char, pos: usize) -> SyntaxEntry {
        if !self.use_properties {
            return self.table_entry(interp, self.table_id, ch);
        }
        if pos < self.b_property || pos >= self.e_property {
            self.refresh(interp, pos);
        }
        match self.effective {
            EffectiveSyntax::Table(id) => self.table_entry(interp, id, ch),
            EffectiveSyntax::Direct(entry) => entry,
        }
    }

    fn table_entry(&mut self, interp: &Interpreter, table_id: u64, ch: char) -> SyntaxEntry {
        let index = ch as usize;
        if index < 128 {
            if let Some(entry) = self.ascii_memo[index] {
                return entry;
            }
            let entry = syntax_entry_for_char(interp, table_id, ch);
            self.ascii_memo[index] = Some(entry);
            return entry;
        }
        syntax_entry_for_char(interp, table_id, ch)
    }
}

fn syntax_entry_at_buffer_position(
    interp: &Interpreter,
    table_id: u64,
    ch: char,
    pos: usize,
) -> SyntaxEntry {
    // One-shot form for cold callers -- no scan state, no memo array;
    // hot loops hold a SyntaxScan instead.
    if !interp
        .lookup_var("parse-sexp-lookup-properties", &Vec::new())
        .is_some_and(|value| value.is_truthy())
    {
        return syntax_entry_for_char(interp, table_id, ch);
    }
    let property = crate::lisp::primitives::strings::buffer_property_at_with_category(
        interp,
        &interp.buffer,
        pos,
        "syntax-table",
    )
    .unwrap_or(Value::Nil);
    match property {
        Value::CharTable(property_table_id) => {
            Some(syntax_entry_for_char(interp, property_table_id, ch))
        }
        Value::Nil => None,
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

fn newline_comment_end_style(interp: &Interpreter, table_id: u64) -> Option<u8> {
    let entry = syntax_entry_for_code(interp, table_id, '\n' as u32);
    (entry.class == SyntaxClass::CommentEnd).then(|| scan_comment_style(&entry, None))
}

fn comment_start_at(
    interp: &Interpreter,
    scan: &mut SyntaxScan,
    chars: &[char],
    idx: usize,
) -> Option<CommentStart> {
    let table_id = scan.table_id();
    let ch = *chars.get(idx)?;
    let entry = scan.entry_at(interp, ch, idx + 1);
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
                line: newline_comment_end_style(interp, table_id)
                    == Some(scan_comment_style(&entry, None)),
            },
            style: scan_comment_style(&entry, None),
            len: 1,
        });
    }
    let next = *chars.get(idx + 1)?;
    let next_entry = scan.entry_at(interp, next, idx + 2);
    if !(entry.start_first && next_entry.start_second) {
        return None;
    }
    let style = scan_comment_style(&next_entry, Some(&entry));
    if ch == next && newline_comment_end_style(interp, table_id) == Some(style) {
        return Some(CommentStart {
            kind: CommentKind::Single { line: true },
            style,
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
    scan: &mut SyntaxScan,
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
                    scan.entry_at(interp, chars[cursor], cursor + 1);
                if entry.class == SyntaxClass::CommentEnd
                    && scan_comment_style(&entry, None) == start.style
                {
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
                    scan.entry_at(interp, chars[cursor], cursor + 1);
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
                    && let Some(nested_start) = comment_start_at(interp, scan, chars, cursor)
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
                    let first = scan.entry_at(interp, chars[cursor], cursor + 1);
                    let second = scan.entry_at(interp, chars[cursor + 1], cursor + 2);
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
            let Some((open_pos, close_char)) = (entry).cons_cells() else {
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

fn scan_entry(
    interp: &Interpreter,
    scan: &mut SyntaxScan,
    chars: &[char],
    pos: i64,
) -> SyntaxEntry {
    if pos < 1 {
        return SyntaxEntry::default();
    }
    match chars.get((pos - 1) as usize) {
        Some(&ch) => scan.entry_at(interp, ch, pos as usize),
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
    scan: &mut SyntaxScan,
    chars: &[char],
    pos: i64,
    beg: i64,
) -> bool {
    let mut quoted = false;
    let mut cursor = pos;
    while cursor > beg {
        cursor -= 1;
        let entry = scan_entry(interp, scan, chars, cursor);
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
#[derive(Clone, Copy)]
struct ForwardCommentOptions {
    nested: bool,
    style: u8,
    end_can_be_escaped: bool,
}

fn scan_forw_comment(
    interp: &Interpreter,
    scan: &mut SyntaxScan,
    chars: &[char],
    mut from: i64,
    stop: i64,
    options: ForwardCommentOptions,
) -> (bool, i64) {
    let mut nesting: i64 = if options.nested { 1 } else { -1 };
    loop {
        if from >= stop {
            return (false, stop);
        }
        let entry = scan_entry(interp, scan, chars, from);
        let entry_position = from;
        let code = entry.class;
        let escaped = options.end_can_be_escaped
            && usize::try_from(entry_position - 1)
                .ok()
                .is_some_and(|index| preceded_by_odd_backslashes(chars, index));
        if code == SyntaxClass::CommentEnd
            && scan_comment_style(&entry, None) == options.style
            && !escaped
        {
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
        if code == SyntaxClass::GenericCommentDelimiter && options.style == 2 {
            return (true, from);
        }
        if nesting > 0
            && code == SyntaxClass::CommentStart
            && entry.nested
            && scan_comment_style(&entry, None) == options.style
        {
            nesting += 1;
        }
        from += 1;
        // Two-char comment ender.
        if from < stop && entry.end_first {
            let second = scan_entry(interp, scan, chars, from);
            if second.end_second
                && scan_comment_style(&entry, Some(&second)) == options.style
                && !escaped
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
            let second = scan_entry(interp, scan, chars, from);
            if second.start_second
                && scan_comment_style(&entry, Some(&second)) == options.style
                && (entry.nested || second.nested)
            {
                from += 1;
                nesting += 1;
            }
        }
    }
}

// GNU syntax.c back_comment: FROM sits on a comment-ender char; when that
// position really ends a comment, return the position of the comment
// starter.  This thin wrapper derives the ender's style and nestedness for
// scan_lists' single-char call site; forward-comment computes them itself
// (two-char enders included) exactly like Fforward_comment.
fn scan_back_comment(interp: &mut Interpreter, env: &mut Env, from: i64) -> Option<i64> {
    let begv = interp.buffer.point_min();
    if from <= begv as i64 {
        return None;
    }
    let chars: Vec<char> = interp.buffer.full_buffer_string().chars().collect();
    let from = usize::try_from(from).ok()?;
    let ch = *chars.get(from - 1)?;
    let table_id = interp.current_syntax_table_id();
    let entry = syntax_entry_at_buffer_position(interp, table_id, ch, from);
    let comment_end_can_be_escaped = interp
        .lookup_var("comment-end-can-be-escaped", env)
        .is_some_and(|value| value.is_truthy());
    back_comment_gnu(
        interp,
        env,
        table_id,
        &chars,
        from,
        begv,
        entry.nested,
        gnu_comment_style(&entry, None),
        comment_end_can_be_escaped,
    )
    .map(|start| start as i64)
}

// SYNTAX_FLAGS_COMMENT_STYLE (syntax.h): style b comes from FLAGS itself,
// style c from either operand.  (The pre-existing `scan_comment_style'
// ORs b across both chars; the ported scanners follow GNU's exact rule.)
fn gnu_comment_style(flags: &SyntaxEntry, other: Option<&SyntaxEntry>) -> u8 {
    u8::from(flags.style_b) | (u8::from(flags.style_c || other.is_some_and(|o| o.style_c)) << 1)
}

fn comment_use_syntax_ppss_enabled(interp: &Interpreter) -> bool {
    interp
        .lookup_var("comment-use-syntax-ppss", &Vec::new())
        .is_some_and(|value| value.is_truthy())
}

fn open_paren_defun_start_enabled(interp: &Interpreter) -> bool {
    interp
        .lookup_var("open-paren-in-column-0-is-defun-start", &Vec::new())
        .is_some_and(|value| value.is_truthy())
}

// parse-partial-sexp element 7 (comment style) as GNU's comstyle code:
// nil = style a, a fixnum carries the b|c style bits verbatim, and
// `syntax-table' marks a generic (fence) comment, which this runtime's
// comment encoding shares with style c (comment_start_at's Fence = 2).
fn ppss_style_code(value: Option<&Value>) -> u8 {
    match value {
        Some(Value::Symbol(name)) if name == "syntax-table" => 2,
        Some(Value::Integer(style)) => (*style as u8) & 3,
        _ => 0,
    }
}

// syntax.c:570 find_defun_start.  Under the default non-nil
// `comment-use-syntax-ppss' the anchor comes from syntax-ppss, whose
// syntax.el cache bounds repeated queries; a bare runtime without that
// Lisp falls back to the `open-paren-in-column-0-is-defun-start' scan,
// the same algorithm GNU runs when the variable is nil.  (GNU's
// find_start_* memo cache is an optimization we skip; the ppss path
// caches in Lisp and the heuristic path is the rare fallback.)
fn find_defun_start_gnu(
    interp: &mut Interpreter,
    env: &mut Env,
    chars: &[char],
    pos: usize,
    begv: usize,
) -> usize {
    if comment_use_syntax_ppss_enabled(interp) && interp.has_lisp_function("syntax-ppss") {
        let saved_point = interp.buffer.point();
        let result = interp.call_function_value(
            Value::Symbol("syntax-ppss".into()),
            Some("syntax-ppss"),
            &[Value::Integer(pos as i64)],
            env,
        );
        interp.buffer.goto_char(saved_point);
        if let Ok(state) = result
            && let Ok(items) = state.to_vec()
            && let Some(start) = items.get(8).and_then(|value| value.as_integer().ok())
            && start >= begv as i64
        {
            return start as usize;
        }
        return pos;
    }
    if !open_paren_defun_start_enabled(interp) {
        return begv;
    }
    // Scan back line-by-line for `^\s(' -- an open-paren in column 0.
    let table_id = interp.current_syntax_table_id();
    let mut line_start = pos.min(chars.len() + 1);
    while line_start > begv && chars.get(line_start - 2).copied() != Some('\n') {
        line_start -= 1;
    }
    loop {
        if let Some(&c) = chars.get(line_start - 1)
            && syntax_entry_at_buffer_position(interp, table_id, c, line_start).class
                == SyntaxClass::OpenParen
        {
            return line_start;
        }
        if line_start <= begv {
            return begv;
        }
        line_start -= 1;
        while line_start > begv && chars.get(line_start - 2).copied() != Some('\n') {
            line_start -= 1;
        }
    }
}

// syntax.c:680 back_comment.  COMMENT_END is the 1-based position of the
// ender's first character; the scan examines characters strictly before
// it, counting string-quote parity and recording comment starters.  When
// the backward scan cannot decide (mixed string delimiters, overlapping
// two-char markers), decode forwards from find_defun_start exactly as GNU
// does.  Returns the opener's position, or None when the ender does not
// close a comment.
#[allow(clippy::too_many_arguments)]
fn back_comment_gnu(
    interp: &mut Interpreter,
    env: &mut Env,
    table_id: u64,
    chars: &[char],
    comment_end: usize,
    stop: usize,
    comnested: bool,
    comstyle: u8,
    comment_end_can_be_escaped: bool,
) -> Option<usize> {
    // Parity keys: the concrete quote char, or the two fence styles.
    const STRING_FENCE: i64 = -2;
    const COMMENT_FENCE: i64 = -3;
    let mut string_style: i64 = -1;
    let mut string_lossage = false;
    let mut comment_lossage = false;
    let mut comstart_pos: usize = 0;
    let mut defun_start: usize = 0;
    let mut nesting: i64 = 1;
    let mut from = comment_end;
    let mut prev_entry: Option<SyntaxEntry> = None;
    let mut lossage = false;
    let mut scan = SyntaxScan::new(interp, table_id);

    while from != stop {
        from -= 1;
        let c = chars[from - 1];
        let entry = scan.entry_at(interp, c, from);
        let last_entry = prev_entry;
        prev_entry = Some(entry);
        let mut code = entry.class;

        let com2start = entry.start_first
            && last_entry.is_some_and(|last| {
                last.start_second
                    && comstyle == gnu_comment_style(&last, Some(&entry))
                    && (last.nested || entry.nested) == comnested
            });
        let mut com2end =
            entry.end_first && last_entry.is_some_and(|last| last.end_second);
        let comstart = com2start || code == SyntaxClass::CommentStart;

        // Overlapping two-char sequences (snmp-mode's --, C's |*|): don't
        // try to be clever.
        if from > stop && (com2end || comstart) {
            let next_c = chars[from - 2];
            let next_entry = scan.entry_at(interp, next_c, from - 1);
            if ((comstart || comnested) && entry.end_second && next_entry.end_first)
                || ((com2end || comnested)
                    && entry.start_second
                    && comstyle == gnu_comment_style(&entry, last_entry.as_ref())
                    && next_entry.start_first)
            {
                lossage = true;
                break;
            }
        }

        if com2start && comstart_pos == 0 {
            // First sight of a starter that is also an ender (snmp-mode):
            // starter now, ender on subsequent sightings.
            com2end = false;
        }
        if com2end {
            code = SyntaxClass::CommentEnd;
        } else if com2start {
            code = SyntaxClass::CommentStart;
        } else if code == SyntaxClass::CommentStart
            && (comstyle != gnu_comment_style(&entry, None) || entry.nested != comnested)
        {
            // Comment starter of a different style.
            continue;
        }

        // Ignore escaped characters, except enders which cannot be escaped.
        if (comment_end_can_be_escaped || code != SyntaxClass::CommentEnd)
            && scan_char_quoted(interp, &mut scan, chars, from as i64, stop as i64)
        {
            continue;
        }

        match code {
            SyntaxClass::GenericStringDelimiter
            | SyntaxClass::GenericCommentDelimiter
            | SyntaxClass::StringQuote => {
                let key = match code {
                    SyntaxClass::GenericStringDelimiter => STRING_FENCE,
                    SyntaxClass::GenericCommentDelimiter => COMMENT_FENCE,
                    _ => c as i64,
                };
                if string_style == -1 {
                    string_style = key;
                } else if string_style == key {
                    string_style = -1;
                } else {
                    // Two kinds of string delimiters: no way to grok this
                    // scanning backwards.
                    string_lossage = true;
                }
            }
            SyntaxClass::CommentStart => {
                if string_style != -1 || comment_lossage || string_lossage {
                    // Odd string quotes involved (Pascal: " { " a { " }).
                    lossage = true;
                    break;
                }
                if !comnested {
                    comstart_pos = from;
                } else {
                    nesting -= 1;
                    if nesting <= 0 {
                        // Nested comments balance: this starter is ours.
                        return Some(from);
                    }
                }
            }
            SyntaxClass::CommentEnd => {
                let same_style = gnu_comment_style(&entry, None) == comstyle
                    && ((com2end && last_entry.is_some_and(|last| last.nested)) || entry.nested)
                        == comnested;
                if same_style {
                    if comnested {
                        nesting += 1;
                    } else {
                        // Anything earlier would match this ender, not ours.
                        from = stop;
                        continue;
                    }
                } else if comstart_pos != 0 || c != '\n' {
                    // Mixing comment styles: be careful ({ (* } *)).
                    comment_lossage = true;
                }
            }
            SyntaxClass::OpenParen => {
                if open_paren_defun_start_enabled(interp)
                    && !comment_use_syntax_ppss_enabled(interp)
                    && (from == stop || chars[from - 2] == '\n')
                {
                    // A defun-start is assumed to be outside of strings.
                    defun_start = from;
                    from = stop;
                    continue;
                }
            }
            _ => {}
        }
    }

    if !lossage {
        return (comstart_pos != 0).then_some(comstart_pos);
    }

    // Mixed delimiters or overlapping markers: decode going forwards from
    // a known safe place, as GNU's lossage path does.
    let saved_point = interp.buffer.point();
    let mut defun_start = if defun_start != 0 {
        defun_start
    } else {
        find_defun_start_gnu(interp, env, chars, comment_end, stop)
    };
    let mut from = comment_end;
    loop {
        let state = parse_forward(
            interp,
            defun_start,
            comment_end,
            None,
            false,
            None,
            CommentStop::No,
            env,
        );
        let items = match state.ok().and_then(|value| value.to_vec().ok()) {
            Some(items) => items,
            None => break,
        };
        defun_start = comment_end;
        let incomment_matches = if comnested {
            matches!(items.get(4), Some(Value::Integer(1)))
        } else {
            matches!(items.get(4), Some(Value::T))
        };
        let comstr_start = items
            .get(8)
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok());
        if incomment_matches && ppss_style_code(items.get(7)) == comstyle {
            if let Some(start) = comstr_start {
                from = start;
            }
        } else {
            from = comment_end;
            if items.get(4).is_some_and(Value::is_truthy)
                && let Some(start) = comstr_start
            {
                // Our ender may sit inside a surrounding comment; retry
                // from within it (syntax.c: { a (* " *)).
                defun_start = start + 2;
            }
        }
        if defun_start >= comment_end {
            break;
        }
    }
    interp.buffer.goto_char(saved_point);
    (from != comment_end).then_some(from)
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
    // Buffer positions remain absolute while narrowed.  Keep this text
    // indexed in the same coordinate system and use BEGV/ZV as the scan
    // bounds, just as GNU's scan_lists does.
    let chars: Vec<char> = interp.buffer.full_buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let mut scan = SyntaxScan::new(interp, table_id);
    let begv = interp.buffer.point_min() as i64;
    let zv = interp.buffer.point_max() as i64;
    let ignore_comments = interp
        .lookup_var("parse-sexp-ignore-comments", env)
        .is_some_and(|value| value.is_truthy());
    let comment_end_can_be_escaped = interp
        .lookup_var("comment-end-can-be-escaped", env)
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
            let entry = scan_entry(interp, &mut scan, &chars, from);
            let mut code = entry.class;
            let mut comstyle = scan_comment_style(&entry, None);
            let mut comnested = entry.nested;
            if depth == min_depth {
                last_good = from;
            }
            from += 1;
            if from < stop && entry.start_first && ignore_comments {
                let second = scan_entry(interp, &mut scan, &chars, from);
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
                        let inner = scan_entry(interp, &mut scan, &chars, from);
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
                        interp,
                        &mut scan,
                        &chars,
                        from,
                        stop,
                        ForwardCommentOptions {
                            nested: comnested,
                            style: comstyle,
                            end_can_be_escaped: comment_end_can_be_escaped,
                        },
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
                        let inner = scan_entry(interp, &mut scan, &chars, from);
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
            let entry = scan_entry(interp, &mut scan, &chars, from);
            let mut code = entry.class;
            if depth == min_depth {
                last_good = from;
            }
            if from > stop && entry.end_second && ignore_comments {
                let prev = scan_entry(interp, &mut scan, &chars, from - 1);
                if prev.end_first {
                    from -= 1;
                    code = SyntaxClass::CommentEnd;
                }
            }
            // Quoting turns anything except a comment-ender into a word
            // character (cannot hold if FROM was decremented above).
            if code != SyntaxClass::CommentEnd
                && scan_char_quoted(interp, &mut scan, &chars, from, stop)
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
                        let before = scan_entry(interp, &mut scan, &chars, from - 1);
                        // Don't allow a comment-end to be quoted.
                        if before.class == SyntaxClass::CommentEnd {
                            break;
                        }
                        let quoted = scan_char_quoted(interp, &mut scan, &chars, from - 1, stop);
                        if quoted {
                            from -= 1;
                        }
                        if !quoted {
                            match scan_entry(interp, &mut scan, &chars, from - 1).class {
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
                        if !scan_char_quoted(interp, &mut scan, &chars, from, stop)
                            && scan_entry(interp, &mut scan, &chars, from).class == fence_class
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
                        if !scan_char_quoted(interp, &mut scan, &chars, from, stop)
                            && scan_char(&chars, from) == stringterm
                            && scan_entry(interp, &mut scan, &chars, from).class
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

pub(super) fn scan_sexps_position_for_scan_sexps(
    interp: &mut Interpreter,
    env: &mut Env,
    from: usize,
    count: i64,
) -> Result<Option<usize>, LispError> {
    scan_lists_gnu(interp, env, from as i64, count, 0, true)
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
    // FROM and TO are absolute buffer positions even under narrowing.
    let mut chars: Vec<char> = interp.buffer.full_buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let mut scan = SyntaxScan::new(interp, table_id);
    let comment_end_can_be_escaped = interp
        .lookup_var("comment-end-can-be-escaped", env)
        .is_some_and(|value| value.is_truthy());
    let mut state = decode_parse_state(oldstate);
    let mut idx = from.saturating_sub(1);
    let end = to.saturating_sub(1).min(chars.len());
    // The scan must not read past TO: a two-char comment or string
    // delimiter whose second character sits beyond the parse end does not
    // exist for this parse (GNU's scan_sexps_forward never fetches past
    // END, so `(parse-partial-sexp 1 4)' over "#|#|#" reports depth 1,
    // not a phantom depth 2 opened by the "#|" straddling the boundary).
    chars.truncate(end);
    // Whether we are inside a word/symbol token; token STARTS record the
    // level's last-sexp position (parse state element 2).
    let mut in_symbol = false;

    while idx < end {
        if let Some(string) = state.string {
            let ch = chars[idx];
            let entry = scan.entry_at(interp, ch, idx + 1);
            if string.fence {
                if entry.class == SyntaxClass::GenericStringDelimiter
                    && !scan_char_quoted(
                        interp,
                        &mut scan,
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
                    &mut scan,
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
                        scan.entry_at(interp, chars[idx], idx + 1);
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
                        scan.entry_at(interp, chars[idx], idx + 1);
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
                        && let Some(start) = comment_start_at(interp, &mut scan, &chars, idx)
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
                            scan.entry_at(interp, chars[idx], idx + 1);
                        let second = scan.entry_at(interp, chars[idx + 1], idx + 2);
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

        if let Some(start) = comment_start_at(interp, &mut scan, &chars, idx) {
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
        let entry = scan.entry_at(interp, ch, idx + 1);
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
                    &mut scan,
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

pub(super) fn syntax_class_chars_at_buffer_position(
    interp: &Interpreter,
    position: usize,
) -> Option<(char, char)> {
    let table_id = interp.current_syntax_table_id();
    let mut effective = SyntaxScan::new(interp, table_id);
    syntax_class_chars_with_scan(interp, &mut effective, position)
}

// Scan-holding form of the pair above for per-character loops (the
// regexp haystack encoder): the plain-table class comes from the scan's
// ASCII memo, the effective class through its interval state.
pub(super) fn syntax_class_chars_with_scan(
    interp: &Interpreter,
    effective: &mut SyntaxScan,
    position: usize,
) -> Option<(char, char)> {
    let ch = interp.buffer.char_at(position)?;
    let table_class = syntax_class_char(effective.plain_table_entry(interp, ch).class);
    let effective_class = syntax_class_char(effective.entry_at(interp, ch, position).class);
    Some((table_class, effective_class))
}

fn syntax_classes_at_position_match(
    interp: &Interpreter,
    spec: &str,
    ch: char,
    position: usize,
    lookup_properties: bool,
) -> bool {
    let (negated, classes) = spec
        .strip_prefix('^')
        .map(|rest| (true, rest))
        .unwrap_or((false, spec));
    let table_id = interp.current_syntax_table_id();
    let entry = if lookup_properties {
        syntax_entry_at_buffer_position(interp, table_id, ch, position)
    } else {
        syntax_entry_for_char(interp, table_id, ch)
    };
    let matched = classes
        .chars()
        .any(|class| syntax_entry_class_matches(entry, class));
    if negated { !matched } else { matched }
}

pub(super) fn skip_syntax_impl(
    interp: &mut Interpreter,
    syntax_value: &Value,
    limit_value: Option<&Value>,
    forward: bool,
    env: &Env,
) -> Result<Value, LispError> {
    let syntax = string_text(syntax_value)?;
    let lookup_properties = interp
        .lookup_var("parse-sexp-lookup-properties", env)
        .is_some_and(|value| value.is_truthy());
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
            if !syntax_classes_at_position_match(
                interp,
                &syntax,
                ch,
                interp.buffer.point(),
                lookup_properties,
            ) {
                break;
            }
            let _ = interp.buffer.forward_char(1);
        }
    } else {
        while interp.buffer.point() > limit {
            let Some(ch) = interp.buffer.char_before() else {
                break;
            };
            if !syntax_classes_at_position_match(
                interp,
                &syntax,
                ch,
                interp.buffer.point() - 1,
                lookup_properties,
            ) {
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

pub(super) fn forward_comment_impl(
    interp: &mut Interpreter,
    count_value: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let count = count_value.map_or(Ok(1), Value::as_integer)?;
    if count == 0 {
        return Ok(Value::T);
    }

    // GNU's backward comment scan consults `syntax-ppss', which lazily runs
    // the mode's syntax propertizer.  Do that before taking the syntax/text
    // snapshot so position-specific syntax (for example, a shell `#' inside
    // a word) participates even when no caller has parsed the buffer first.
    // Like the GNU scan primitive, do not expose regexp match-data changes
    // made internally by the propertizer.
    ensure_syntax_propertized_preserving_match_data(interp, env);

    let minimum = interp.buffer.point_min();
    let mut chars: Vec<char> = interp.buffer.full_buffer_string().chars().collect();
    chars.truncate(interp.buffer.point_max().saturating_sub(1));
    let table_id = interp.current_syntax_table_id();
    let mut scan = SyntaxScan::new(interp, table_id);
    let comment_end_can_be_escaped = interp
        .lookup_var("comment-end-can-be-escaped", env)
        .is_some_and(|value| value.is_truthy());
    let original_point = interp.buffer.point();

    if count > 0 {
        let mut point = original_point;
        for _ in 0..count {
            let candidate = skip_whitespace_forward(interp, table_id, &chars, point);
            let idx = candidate.saturating_sub(1);
            let Some(start) = comment_start_at(interp, &mut scan, &chars, idx) else {
                // GNU stops before the non-comment token, keeping the
                // whitespace crossed so far behind point.
                interp.buffer.goto_char(candidate);
                return Ok(Value::Nil);
            };
            let (end, closed) = skip_comment_with_status(
                interp,
                &mut scan,
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

    // Backward branch: syntax.c Fforward_comment's while (count1 < 0)
    // loop, with back_comment as the grammar owner -- a local backward
    // parity scan, falling forward from a defun-start anchor only on the
    // ambiguous cases, never a whole-buffer reparse per query.
    let mut from = original_point;
    for _ in 0..count.unsigned_abs() {
        loop {
            if from <= minimum {
                interp.buffer.goto_char(minimum);
                return Ok(Value::Nil);
            }
            from -= 1;
            let c = chars[from - 1];
            let quoted = scan_char_quoted(interp, &mut scan, &chars, from as i64, minimum as i64);
            let entry = scan.entry_at(interp, c, from);
            let mut code = entry.class;
            let mut comstyle = 0u8;
            let mut comnested = entry.nested;
            if code == SyntaxClass::CommentEnd {
                comstyle = gnu_comment_style(&entry, None);
            }
            // Two-char comment ender: this is the second char, the first
            // sits before it (and must itself be unquoted).
            let mut two_char_ender = false;
            if from > minimum && entry.end_second {
                let first_pos = from - 1;
                let first_c = chars[first_pos - 1];
                let first_entry =
                    scan.entry_at(interp, first_c, first_pos);
                if first_entry.end_first
                    && !scan_char_quoted(
                        interp,
                        &mut scan,
                        &chars,
                        first_pos as i64,
                        minimum as i64,
                    )
                {
                    from = first_pos;
                    code = SyntaxClass::CommentEnd;
                    two_char_ender = true;
                    comstyle = gnu_comment_style(&first_entry, Some(&entry));
                    comnested = comnested || first_entry.nested;
                }
            }

            if code == SyntaxClass::GenericCommentDelimiter {
                // Skip to the first preceding unquoted comment fence.
                let ini = from;
                let mut fence_found = false;
                while from > minimum {
                    from -= 1;
                    let fence_c = chars[from - 1];
                    let fence_entry =
                        scan.entry_at(interp, fence_c, from);
                    if fence_entry.class == SyntaxClass::GenericCommentDelimiter
                        && !scan_char_quoted(
                            interp,
                            &mut scan,
                            &chars,
                            from as i64,
                            minimum as i64,
                        )
                    {
                        fence_found = true;
                        break;
                    }
                }
                if !fence_found {
                    interp.buffer.goto_char(ini + 1);
                    return Ok(Value::Nil);
                }
                // We have skipped one comment.
                break;
            } else if code == SyntaxClass::CommentEnd {
                let found = if !quoted || !comment_end_can_be_escaped {
                    back_comment_gnu(
                        interp,
                        env,
                        table_id,
                        &chars,
                        from,
                        minimum,
                        comnested,
                        comstyle,
                        comment_end_can_be_escaped,
                    )
                } else {
                    None
                };
                match found {
                    Some(start) => {
                        // We have skipped one comment.
                        from = start;
                        break;
                    }
                    None => {
                        if c == '\n' {
                            // This end-of-line is not an end-of-comment:
                            // treat it like whitespace (CC-mode relies on
                            // this).
                            continue;
                        }
                        // Back to the end of this not-quite-endcomment.
                        if two_char_ender {
                            from += 1;
                        }
                        interp.buffer.goto_char(from + 1);
                        return Ok(Value::Nil);
                    }
                }
            } else if code == SyntaxClass::Whitespace && !quoted {
                continue;
            } else {
                interp.buffer.goto_char(from + 1);
                return Ok(Value::Nil);
            }
        }
    }
    interp.buffer.goto_char(from);
    Ok(Value::T)
}

// GNU `backward-prefix-chars': move point backward over any number of
// characters with quote or prefix syntax (', #, \` and , in Lisp).
pub(super) fn backward_prefix_chars(interp: &mut Interpreter) -> Result<Value, LispError> {
    // Point and point-min are absolute buffer positions even while narrowed.
    // Index the full buffer rather than the accessible substring.
    let chars: Vec<char> = interp.buffer.full_buffer_string().chars().collect();
    let table_id = interp.current_syntax_table_id();
    let mut scan = SyntaxScan::new(interp, table_id);
    let minimum = interp.buffer.point_min();
    let mut position = interp.buffer.point();
    while position > minimum {
        let ch = chars[position - 2];
        let char_position = position - 1;
        let entry = scan.entry_at(interp, ch, char_position);
        if !(entry.class == SyntaxClass::Quote || entry.prefix)
            || scan_char_quoted(
                interp,
                &mut scan,
                &chars,
                char_position as i64,
                minimum as i64,
            )
        {
            break;
        }
        position -= 1;
    }
    interp.buffer.goto_char(position);
    Ok(Value::Nil)
}
