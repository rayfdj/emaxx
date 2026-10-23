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

pub(super) fn syntax_entry_for_code(
    interp: &Interpreter,
    table_id: CharTableRef,
    code: u32,
) -> SyntaxEntry {
    // GNU character codes include the raw-byte range above Unicode's scalar
    // limit.  Keep the public code as the char-table key, but use the shared
    // character boundary to obtain Emaxx's internal marker when a default
    // syntax entry has to be derived.
    let Ok(ch) = char_from_integer(i64::from(code)) else {
        return SyntaxEntry::default();
    };
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
            syntax_entry_from_value(&value.value()).unwrap_or_else(|| default_syntax_entry(ch))
        }
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

fn syntax_entry_for_char(interp: &Interpreter, table_id: CharTableRef, ch: char) -> SyntaxEntry {
    syntax_entry_for_code(interp, table_id, ch as u32)
}

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
    table_id: CharTableRef,
    use_properties: bool,
    b_property: usize,
    e_property: usize,
    effective: EffectiveSyntax,
    ascii_memo: [Option<SyntaxEntry>; 128],
}

#[derive(Clone, Copy)]
enum EffectiveSyntax {
    Table(CharTableRef),
    Direct(SyntaxEntry),
}

impl SyntaxScan {
    pub(super) fn new(interp: &Interpreter, table_id: CharTableRef) -> Self {
        // syntax.c:253 (SETUP_SYNTAX_TABLE): the property machinery arms
        // only when `parse-sexp-lookup-properties' is non-nil.
        let use_properties = interp
            .lookup_var_key(
                cached_symbol!("parse-sexp-lookup-properties"),
                &crate::lisp::types::Env::new(),
            )
            .is_some_and(|value| value.is_truthy());
        SyntaxScan {
            table_id,
            use_properties,
            b_property: 1,
            e_property: 0, // empty interval: first query refreshes
            effective: EffectiveSyntax::Table(table_id),
            ascii_memo: [None; 128],
        }
    }

    fn refresh(&mut self, interp: &Interpreter, pos: usize) {
        let (start, end) = interp.buffer.borrow().text_property_interval_around(pos);
        self.b_property = start;
        self.e_property = end.max(pos + 1);
        let property = syntax_table_property_at(interp, pos);
        self.effective = match property.kind() {
            Kind::CharTable(property_table_id) => EffectiveSyntax::Table(property_table_id),
            Kind::Nil => EffectiveSyntax::Table(self.table_id),
            property => syntax_entry_from_value(&property.value())
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

    /// The syntax class of character code CODE at POS: `entry_at' for a
    /// character, and for a code beyond Unicode's scalars (a raw byte) the
    /// effective table read by code.
    pub(super) fn class_at_code(
        &mut self,
        interp: &Interpreter,
        code: u32,
        pos: usize,
    ) -> SyntaxClass {
        if let Some(ch) = char::from_u32(code) {
            return self.entry_at(interp, ch, pos).class;
        }
        if !self.use_properties {
            return syntax_entry_for_code(interp, self.table_id, code).class;
        }
        if pos < self.b_property || pos >= self.e_property {
            self.refresh(interp, pos);
        }
        match self.effective {
            EffectiveSyntax::Table(id) => syntax_entry_for_code(interp, id, code).class,
            EffectiveSyntax::Direct(entry) => entry.class,
        }
    }

    fn table_entry(
        &mut self,
        interp: &Interpreter,
        table_id: CharTableRef,
        ch: char,
    ) -> SyntaxEntry {
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

/// syntax.c:374 (update_syntax_table): the property comes off the
/// interval plist with `textget' -- buffer text properties (with category
/// indirection) only; overlays never feed syntax scans.  interval_of finds
/// no interval in a buffer without intervals or at its end, and then no
/// property applies, `default-text-properties' included.
fn syntax_table_property_at(interp: &Interpreter, pos: usize) -> Value {
    let buffer = interp.buffer.borrow();
    if !buffer.has_interval_tree() || pos >= buffer.point_max() {
        return Value::Nil;
    }
    crate::lisp::primitives::strings::buffer_property_at_with_category(
        interp,
        &buffer,
        pos,
        "syntax-table",
    )
    .unwrap_or(Value::Nil)
}

fn syntax_entry_at_buffer_position(
    interp: &Interpreter,
    table_id: CharTableRef,
    ch: char,
    pos: usize,
) -> SyntaxEntry {
    // One-shot form for cold callers -- no scan state, no memo array;
    // hot loops hold a SyntaxScan instead.
    if !interp
        .lookup_var_key(
            cached_symbol!("parse-sexp-lookup-properties"),
            &crate::lisp::types::Env::new(),
        )
        .is_some_and(|value| value.is_truthy())
    {
        return syntax_entry_for_char(interp, table_id, ch);
    }
    let property = syntax_table_property_at(interp, pos);
    match property.kind() {
        Kind::CharTable(property_table_id) => {
            Some(syntax_entry_for_char(interp, property_table_id, ch))
        }
        Kind::Nil => None,
        property => syntax_entry_from_value(&property.value()),
    }
    .unwrap_or_else(|| syntax_entry_for_char(interp, table_id, ch))
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
    let Some(ch) = interp.buffer.borrow().char_at(position) else {
        return false;
    };
    let table_id = interp.current_syntax_table_id();
    let entry = if interp
        .lookup_var_key(cached_symbol!("parse-sexp-lookup-properties"), env)
        .is_some_and(|value| value.is_truthy())
    {
        syntax_entry_at_buffer_position(interp, table_id, ch, position)
    } else {
        syntax_entry_for_char(interp, table_id, ch)
    };
    syntax_entry_class_matches(entry, class)
}
