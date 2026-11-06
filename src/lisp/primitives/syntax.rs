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

/// syntax.c:syntax_spec_code: the class a descriptor letter names, 0377
/// for no letter; the table's bytes past 0177 are zero, whitespace.
fn syntax_spec_code(letter: u8) -> u8 {
    match letter {
        b' ' | b'-' => 0,
        b'.' => 1,
        b'w' | b'W' => 2,
        b'_' => 3,
        b'(' => 4,
        b')' => 5,
        b'\'' => 6,
        b'"' => 7,
        b'$' => 8,
        b'\\' => 9,
        b'/' => 10,
        b'<' => 11,
        b'>' => 12,
        b'@' => 13,
        b'!' => 14,
        b'|' => 15,
        0x80.. => 0,
        _ => 0o377,
    }
}

/// character.h:string_char_and_length's arithmetic on the bytes at P,
/// whatever they are (a unibyte string's raw byte decodes as GNU's
/// formula makes it, reading the bytes after it); bytes past P's end
/// read as the NULs a C string ends with.
fn string_char_and_length(p: &[u8]) -> (i64, usize) {
    let byte = |index: usize| i64::from(p.get(index).copied().unwrap_or(0));
    let c = byte(0);
    if c & 0x80 == 0 {
        return (c, 1);
    }
    let mut d = (c << 6) + byte(1) - ((0xc0 << 6) + 0x80);
    if c & 0x20 == 0 {
        return (d + if c < 0xc2 { 0x3fff80 } else { 0 }, 2);
    }
    d = (d << 6) + byte(2) - ((0x20 << 12) + 0x80);
    if c & 0x10 == 0 {
        return (d, 3);
    }
    d = (d << 6) + byte(3) - ((0x10 << 18) + 0x80);
    if c & 0x08 == 0 {
        return (d, 4);
    }
    d = (d << 6) + byte(4) - ((0x08 << 24) + 0x80);
    (d, 5)
}

/// syntax.c:Fstring_to_syntax: STRING's bytes read as the C string
/// SDATA is, so that a NUL ends them: the first byte's class
/// (syntax_spec_code), nil for `@', the matching character
/// string_char_and_length makes of the bytes after it (nil for a space),
/// then the flags among the rest; a shared syntax code object when there
/// is no match.
pub(super) fn string_to_syntax(interp: &Interpreter, string: &Value) -> Result<Value, LispError> {
    let Kind::StringObject(state) = string.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *string));
    };
    let bytes = state.borrow().bytes().to_vec();
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    let p = &bytes[..end];
    let first = p.first().copied().unwrap_or(0);
    let mut val = i64::from(syntax_spec_code(first));
    if val == 0o377 {
        return Err(LispError::Signal(format!(
            "Invalid syntax description letter: {}",
            char::from(first)
        )));
    }
    if val == 13 {
        return Ok(Value::Nil);
    }
    let mut rest = p.get(1..).unwrap_or_default();
    let mut matching = Value::Nil;
    if !rest.is_empty() {
        let (character, length) = string_char_and_length(rest);
        if character != i64::from(b' ') {
            matching = Value::Integer(character);
        }
        rest = rest.get(length..).unwrap_or_default();
    }
    for flag in rest {
        val |= match flag {
            b'1' => 1 << 16,
            b'2' => 1 << 17,
            b'3' => 1 << 18,
            b'4' => 1 << 19,
            b'p' => 1 << 20,
            b'b' => 1 << 21,
            b'n' => 1 << 22,
            b'c' => 1 << 23,
            _ => 0,
        };
    }
    if val < 16 && matching.is_nil() {
        Ok(interp.syntax_code_object(val as usize))
    } else {
        Ok(Value::cons(Value::Integer(val), matching))
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

/// One insertion of Finternal_describe_syntax_value: a text (insert,
/// insert_string) or insert_char of a character code.
pub(super) enum DescribePiece {
    Text(String),
    Char(i64),
}

impl From<String> for DescribePiece {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for DescribePiece {
    fn from(text: &str) -> Self {
        Self::Text(text.into())
    }
}

/// syntax.c:Finternal_describe_syntax_value's text, as the pieces it
/// inserts one by one (each an insertion of its own, with its own change
/// hooks), and whether the prefix flag's text follows.  The matching
/// character is any CHARACTERP code, raw bytes and codes past Unicode
/// included, inserted by insert_char.
pub(super) fn describe_syntax_value(value: &Value) -> (Vec<DescribePiece>, bool) {
    if value.is_nil() {
        return (vec!["default".into()], false);
    }
    if matches!(value.kind(), Kind::CharTable(_)) {
        return (vec!["deeper char-table ...".into()], false);
    }
    let Some((car, cdr)) = (value).cons_cells() else {
        return (vec!["invalid".into()], false);
    };
    let Kind::Integer(raw_code) = car.get().kind() else {
        return (vec!["invalid".into()], false);
    };
    // !(NILP (match_lisp) || CHARACTERP (match_lisp)).
    let matching_code = match cdr.get().kind() {
        Kind::Nil => None,
        Kind::Integer(code) if (0..=0x3F_FFFF).contains(&code) => Some(code),
        _ => return (vec!["invalid".into()], false),
    };
    let matching = matching_code
        .and_then(|code| u32::try_from(code).ok())
        .and_then(char::from_u32);
    let syntax_code = raw_code & i64::from(i32::MAX);
    let Some(class) = syntax_class_from_code(syntax_code & 0xff) else {
        return (vec!["invalid".into()], false);
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
    let mut description: Vec<DescribePiece> = Vec::new();
    description.push(String::from(syntax_class_char(class)).into());
    description.push(match matching_code {
        Some(code) => DescribePiece::Char(code),
        None => " ".into(),
    });
    if entry.start_first {
        description.push(String::from('1').into());
    }
    if entry.start_second {
        description.push(String::from('2').into());
    }
    if entry.end_first {
        description.push(String::from('3').into());
    }
    if entry.end_second {
        description.push(String::from('4').into());
    }
    if entry.prefix {
        description.push(String::from('p').into());
    }
    if entry.style_b {
        description.push(String::from('b').into());
    }
    if entry.style_c {
        description.push(String::from('c').into());
    }
    if entry.nested {
        description.push(String::from('n').into());
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
    description.push("\twhich means: ".into());
    description.push(meaning.into());
    if let Some(code) = matching_code {
        description.push(", matches ".into());
        description.push(DescribePiece::Char(code));
    }
    if entry.start_first {
        description.push(",\n\t  is the first character of a comment-start sequence".into());
    }
    if entry.start_second {
        description.push(",\n\t  is the second character of a comment-start sequence".into());
    }
    if entry.end_first {
        description.push(",\n\t  is the first character of a comment-end sequence".into());
    }
    if entry.end_second {
        description.push(",\n\t  is the second character of a comment-end sequence".into());
    }
    if entry.style_b {
        description.push(" (comment style b)".into());
    }
    if entry.style_c {
        description.push(" (comment style c)".into());
    }
    if entry.nested {
        description.push(" (nestable)".into());
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

/// syntax.c check_syntax_table: VALUE's table when it is a char-table of
/// purpose `syntax-table', else `(wrong-type-argument syntax-table-p VALUE)'.
pub(crate) fn check_syntax_table(value: &Value) -> Result<CharTableRef, LispError> {
    match value.kind() {
        Kind::CharTable(id) if id.has_purpose("syntax-table") => Ok(id),
        _ => Err(LispError::WrongTypeArgument(
            "syntax-table-p".into(),
            *value,
        )),
    }
}

/// syntax.c:Fmatching_paren's answer for CODE: the entry's matching
/// character (SYNTAX_MATCH) when the current syntax table makes it an
/// open or close parenthesis, nil otherwise.
pub(super) fn matching_paren_value(interp: &Interpreter, code: u32) -> Value {
    let entry = syntax_entry_for_code(interp, interp.current_syntax_table_id(), code);
    match entry.class {
        SyntaxClass::OpenParen | SyntaxClass::CloseParen => {
            entry.matching.map_or(Value::Nil, |matching| {
                Value::Integer(i64::from(u32::from(matching)))
            })
        }
        _ => Value::Nil,
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
