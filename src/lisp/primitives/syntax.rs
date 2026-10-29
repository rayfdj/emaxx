use super::*;
use crate::lisp::types::CharTableRef;
use crate::lisp::types::Kind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyntaxClass {
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
    entry.matching = chars.next().filter(|ch| *ch != ' ' && *ch != '\0');
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

pub(super) fn syntax_entry_value(interp: &Interpreter, entry: SyntaxEntry) -> Value {
    if entry.class == SyntaxClass::Inherit {
        return Value::Nil;
    }
    let raw_code = syntax_entry_code(entry);
    if raw_code < 16 && entry.matching.is_none() {
        return interp.syntax_code_object(raw_code as usize);
    }
    let code = Value::Integer(raw_code);
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
    if matches!(value.kind(), Kind::CharTable(_)) {
        return ("deeper char-table ...".into(), false);
    }
    let Some((car, cdr)) = (value).cons_cells() else {
        return ("invalid".into(), false);
    };
    let Kind::Integer(raw_code) = car.get().kind() else {
        return ("invalid".into(), false);
    };
    let matching = match cdr.get().kind() {
        Kind::Nil => None,
        Kind::Integer(code) => u32::try_from(code).ok().and_then(char::from_u32),
        _ => return ("invalid".into(), false),
    };
    if !cdr.get().is_nil() && matching.is_none() {
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

// GNU standard-syntax-table classes for characters a syntax table leaves
// unset: `$'/`%' are word constituents, `&*+-/<=>|_' symbol constituents,
// alongside the usual delimiter/escape assignments.
fn default_syntax_entry(code: u32) -> SyntaxEntry {
    // GNU initializes the complete multibyte range, characters beyond
    // Unicode and raw bytes included, as word syntax, independent of
    // Unicode alphabetic properties.
    let Some(ch) = u8::try_from(code).ok().filter(u8::is_ascii).map(char::from) else {
        return SyntaxEntry {
            class: SyntaxClass::Word,
            ..SyntaxEntry::default()
        };
    };
    let class = match ch {
        ' ' | '\t' | '\n' | '\r' | '\u{000C}' => SyntaxClass::Whitespace,
        '_' | '&' | '*' | '+' | '-' | '/' | '<' | '=' | '>' | '|' => SyntaxClass::Symbol,
        '$' | '%' => SyntaxClass::Word,
        '"' => SyntaxClass::StringQuote,
        '\\' => SyntaxClass::Escape,
        '(' | '[' | '{' => SyntaxClass::OpenParen,
        ')' | ']' | '}' => SyntaxClass::CloseParen,
        '0'..='9' | 'A'..='Z' | 'a'..='z' => SyntaxClass::Word,
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

pub(super) fn syntax_entry_for_code(
    interp: &Interpreter,
    table_id: CharTableRef,
    code: u32,
) -> SyntaxEntry {
    let value = table_id.get(code);
    let entry = match value.kind() {
        Kind::Nil => SyntaxEntry {
            // A nil entry in a syntax table denotes whitespace.  In
            // particular, `(make-char-table 'syntax-table nil)' is the
            // intentionally blank table used by syntax propertizers to make
            // every character insignificant except explicitly installed
            // delimiters.
            class: SyntaxClass::Whitespace,
            ..SyntaxEntry::default()
        },
        value => {
            syntax_entry_from_value(&value.value()).unwrap_or_else(|| default_syntax_entry(code))
        }
    };
    if entry.class == SyntaxClass::Inherit && table_id != interp.standard_syntax_table_id() {
        syntax_entry_for_code(interp, interp.standard_syntax_table_id(), code)
    } else {
        entry
    }
}

pub(super) use super::syntax_scan::current_syntax_word_char;

fn syntax_entry_from_value(value: &Value) -> Option<SyntaxEntry> {
    match value.kind() {
        Kind::Integer(code) => {
            let class = syntax_class_from_code(code)?;
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
        Kind::Cons(_) => {
            let code = value.car_if_list()?.integer_value()?;
            let matching = value
                .cdr()
                .ok()
                .and_then(|cdr| cdr.integer_value())
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

        let word = parse_syntax_spec("w 2c").expect("valid word syntax");
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
