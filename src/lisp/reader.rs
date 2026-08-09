use super::types::{
    LispError, SharedStringState, StringPropertySpan, Value, make_uninterned_symbol_name,
};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::{cell::RefCell, collections::HashMap, rc::Rc};
use unicode_names2::character as unicode_character;

const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
const INVALID_UNICODE_SENTINEL: char = '\u{F8FF}';
const CIRCULAR_READ_SYNTAX_SYMBOL: &str = "emaxx--circular-read-syntax";
const HASH_TABLE_LITERAL_SYMBOL: &str = "emaxx--hash-table-literal";
pub(crate) const CHAR_TABLE_LITERAL_SYMBOL: &str = "emaxx--char-table-literal";
pub(crate) const SUB_CHAR_TABLE_LITERAL_SYMBOL: &str = "emaxx--sub-char-table-literal";
pub(crate) const RECORD_LITERAL_SYMBOL: &str = "emaxx--record-literal";
const BOOL_VECTOR_LITERAL_SYMBOL: &str = "bool-vector-literal";
static READER_UNINTERNED_SYMBOL_COUNTER: AtomicU64 = AtomicU64::new(1);

fn structure_slot_eval_form(value: Value) -> Value {
    match value {
        Value::Nil
        | Value::T
        | Value::Integer(_)
        | Value::BigInteger(_)
        | Value::Float(_)
        | Value::String(_)
        | Value::StringObject(_)
        | Value::Buffer(_)
        | Value::Marker(_)
        | Value::Overlay(_)
        | Value::CharTable(_)
        | Value::Frame(_)
        | Value::Terminal(_)
        | Value::Record(_)
        | Value::Finalizer(_)
        | Value::BuiltinFunc(_)
        | Value::Lambda(_)
        | Value::Unbound => value,
        Value::Symbol(symbol) => Value::list([Value::symbol("quote"), Value::Symbol(symbol)]),
        Value::Cons(_) => {
            if let Ok(items) = value.to_vec()
                && matches!(
                    items.first(),
                    Some(Value::Symbol(symbol))
                        if symbol == "vector-literal"
                            || symbol == BOOL_VECTOR_LITERAL_SYMBOL
                            || symbol == CHAR_TABLE_LITERAL_SYMBOL
                            || symbol == SUB_CHAR_TABLE_LITERAL_SYMBOL
                            || symbol == RECORD_LITERAL_SYMBOL
                            || symbol == "quote"
                )
            {
                value
            } else {
                Value::list([Value::symbol("quote"), value])
            }
        }
    }
}

fn circular_read_label_form(value: &Value) -> Option<(u32, Value)> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(symbol), Value::Integer(id), payload]
            if symbol == CIRCULAR_READ_SYNTAX_SYMBOL && *id >= 0 =>
        {
            Some((*id as u32, payload.clone()))
        }
        _ => None,
    }
}

fn circular_read_ref_form(value: &Value) -> Option<u32> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(symbol), Value::Integer(id)]
            if symbol == CIRCULAR_READ_SYNTAX_SYMBOL && *id >= 0 =>
        {
            Some(*id as u32)
        }
        _ => None,
    }
}

fn interpreted_closure_code_syntax(value: &Value) -> bool {
    if let Some((_id, payload)) = circular_read_label_form(value) {
        return interpreted_closure_code_syntax(&payload);
    }
    let Some((_, cdr)) = (value).cons_cells() else {
        return false;
    };
    // Before Emacs 30, lazily loaded BYTECODE could occupy this slot as a
    // (FILE . OFFSET) pair.  It is still bytecode, whereas an interpreted
    // closure's code slot is a (proper) list of body forms.
    !matches!(&*cdr.borrow(), Value::Integer(_))
}

fn invalid_circular_read_syntax() -> LispError {
    LispError::ReadError("invalid-read-syntax".into())
}

pub(crate) fn contains_circular_read_syntax(value: &Value) -> bool {
    if circular_read_ref_form(value).is_some() || circular_read_label_form(value).is_some() {
        return true;
    }
    match value {
        Value::Cons(_) => value.cons_values().is_some_and(|(car, cdr)| {
            contains_circular_read_syntax(&car) || contains_circular_read_syntax(&cdr)
        }),
        _ => false,
    }
}

fn quoted_hash_table_literal(value: &Value) -> Option<Value> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(symbol), literal] if symbol == "quote" => {
            let literal_items = literal.to_vec().ok()?;
            matches!(
                literal_items.first(),
                Some(Value::Symbol(symbol)) if symbol == HASH_TABLE_LITERAL_SYMBOL
            )
            .then_some(literal.clone())
        }
        _ => None,
    }
}

fn circular_vector_skeleton(len: usize) -> Value {
    let mut tail = Value::Nil;
    for _ in 0..len {
        tail = Value::cons(Value::Nil, tail);
    }
    Value::cons(Value::symbol("vector-literal"), tail)
}

fn fill_circular_label_value(
    template: &Value,
    target: &Value,
    labels: &mut HashMap<u32, Value>,
) -> Result<(), LispError> {
    if let Ok(items) = template.to_vec()
        && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector-literal")
    {
        let Some((_, target_cdr)) = target.cons_cells() else {
            return Err(invalid_circular_read_syntax());
        };
        let mut current = target_cdr.borrow().clone();
        for item in &items[1..] {
            let Some((slot, next)) = current.cons_cells() else {
                return Err(invalid_circular_read_syntax());
            };
            *slot.borrow_mut() = resolve_circular_read_syntax_inner(item, labels)?;
            current = next.borrow().clone();
        }
        return Ok(());
    }

    let Some((template_car, template_cdr)) = template.cons_values() else {
        return Err(invalid_circular_read_syntax());
    };
    let Some((target_car, target_cdr)) = target.cons_cells() else {
        return Err(invalid_circular_read_syntax());
    };
    *target_car.borrow_mut() = resolve_circular_read_syntax_inner(&template_car, labels)?;
    *target_cdr.borrow_mut() = resolve_circular_read_syntax_inner(&template_cdr, labels)?;
    Ok(())
}

fn resolve_circular_read_syntax_inner(
    value: &Value,
    labels: &mut HashMap<u32, Value>,
) -> Result<Value, LispError> {
    if let Some(literal) = quoted_hash_table_literal(value)
        && contains_circular_read_syntax(&literal)
    {
        return Err(invalid_circular_read_syntax());
    }

    if let Some(id) = circular_read_ref_form(value) {
        return labels
            .get(&id)
            .cloned()
            .ok_or_else(invalid_circular_read_syntax);
    }

    if let Some((id, template)) = circular_read_label_form(value) {
        if labels.contains_key(&id) {
            return Err(invalid_circular_read_syntax());
        }
        if circular_read_ref_form(&template) == Some(id) {
            return Err(invalid_circular_read_syntax());
        }

        let placeholder = if let Ok(items) = template.to_vec() {
            if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector-literal") {
                circular_vector_skeleton(items.len().saturating_sub(1))
            } else {
                Value::cons(Value::Nil, Value::Nil)
            }
        } else {
            let resolved = resolve_circular_read_syntax_inner(&template, labels)?;
            labels.insert(id, resolved.clone());
            return Ok(resolved);
        };

        labels.insert(id, placeholder.clone());
        fill_circular_label_value(&template, &placeholder, labels)?;
        return Ok(placeholder);
    }

    if let Ok(items) = value.to_vec()
        && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector-literal")
    {
        return Ok(Value::list(
            std::iter::once(Value::symbol("vector-literal")).chain(
                items[1..]
                    .iter()
                    .map(|item| resolve_circular_read_syntax_inner(item, labels))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        ));
    }

    match value {
        Value::Cons(_) => {
            let Some((car, cdr)) = value.cons_values() else {
                return Err(invalid_circular_read_syntax());
            };
            Ok(Value::cons(
                resolve_circular_read_syntax_inner(&car, labels)?,
                resolve_circular_read_syntax_inner(&cdr, labels)?,
            ))
        }
        // Propertized string literals carry arbitrary values in their
        // property plists; `#N=' labels and `#N#' references may appear
        // there (print-circle output shares prop values).
        Value::StringObject(state) => {
            let spans = state.borrow().props.clone();
            let mut resolved_spans = Vec::with_capacity(spans.len());
            for span in spans {
                let mut resolved_props = Vec::with_capacity(span.props.len());
                for (key, prop_value) in span.props {
                    resolved_props.push((
                        key,
                        resolve_circular_read_syntax_inner(&prop_value, labels)?,
                    ));
                }
                resolved_spans.push(StringPropertySpan {
                    props: resolved_props,
                    ..span
                });
            }
            state.borrow_mut().props = resolved_spans;
            Ok(value.clone())
        }
        _ => Ok(value.clone()),
    }
}

pub(crate) fn resolve_circular_read_syntax(value: Value) -> Result<Value, LispError> {
    resolve_circular_read_syntax_inner(&value, &mut HashMap::new())
}

/// True when a quoted template contains reader marker forms (`#N='/`#N#'
/// circular labels, hash/character tables, or record literals) that `quote' must
/// resolve.  Marker-free templates — the common case — can be returned
/// as-is, sharing structure exactly like GNU's quote.
pub(crate) fn quote_template_needs_resolution(value: &Value) -> bool {
    let mut seen = std::collections::HashSet::new();
    let mut stack = vec![value.clone()];
    while let Some(current) = stack.pop() {
        match &current {
            Value::Cons(cons_cell) => {
                let car_cell = &cons_cell.car;
                let cdr_cell = &cons_cell.cdr;
                let ptr = crate::lisp::types::ConsCell::identity(cons_cell);
                if !seen.insert(ptr) {
                    continue;
                }
                {
                    let car = car_cell.borrow();
                    if let Value::Symbol(symbol) = &*car
                        && (symbol == CIRCULAR_READ_SYNTAX_SYMBOL
                            || symbol == HASH_TABLE_LITERAL_SYMBOL
                            || symbol == CHAR_TABLE_LITERAL_SYMBOL
                            || symbol == RECORD_LITERAL_SYMBOL)
                    {
                        return true;
                    }
                }
                stack.push(car_cell.borrow().clone());
                stack.push(cdr_cell.borrow().clone());
            }
            Value::StringObject(state) => {
                for span in &state.borrow().props {
                    for (_, prop_value) in &span.props {
                        stack.push(prop_value.clone());
                    }
                }
            }
            _ => {}
        }
    }
    false
}

fn encode_raw_byte(byte: u8) -> char {
    char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
        .expect("raw byte regex marker is a valid private-use character")
}

fn raw_byte_from_source_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&code) {
        Some((code - RAW_BYTE_REGEX_BASE) as u8)
    } else {
        None
    }
}

/// A simple s-expression reader. Handles the subset of Elisp syntax
/// that appears in ERT test files: atoms, lists, strings, quotes,
/// backquote, characters, and comments.
pub struct Reader<'a> {
    input: &'a [u8],
    pos: usize,
    symbol_shorthands: Vec<(String, String)>,
    backquote_depth: usize,
    raw_quote_symbols: bool,
}

impl<'a> Reader<'a> {
    pub fn new(input: &'a str) -> Self {
        Self::with_symbol_shorthands(input, Vec::new())
    }

    pub fn with_symbol_shorthands(
        input: &'a str,
        symbol_shorthands: Vec<(String, String)>,
    ) -> Self {
        Reader {
            input: input.as_bytes(),
            pos: 0,
            symbol_shorthands,
            backquote_depth: 0,
            // GNU's reader always encodes quote shorthands with the raw
            // `\``/`\,'/`\,@' symbols; pcase.el's pattern expanders are
            // registered under those names.
            raw_quote_symbols: true,
        }
    }

    pub fn with_raw_quote_symbols(input: &'a str) -> Self {
        let mut reader = Self::new(input);
        reader.raw_quote_symbols = true;
        reader
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let ch = self.input.get(self.pos).copied()?;
        self.pos += 1;
        Some(ch)
    }

    fn peek_char(&self) -> Option<char> {
        match self.peek()? {
            ch if ch < 0x80 => Some(ch as char),
            _ => {
                let first = *self.input.get(self.pos)?;
                let len = if first < 0xE0 {
                    2
                } else if first < 0xF0 {
                    3
                } else {
                    4
                };
                let s = std::str::from_utf8(self.input.get(self.pos..self.pos + len)?).ok()?;
                s.chars().next()
            }
        }
    }

    fn advance_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while let Some(ch) = self.peek_char() {
                if ch.is_whitespace() {
                    self.advance_char();
                } else {
                    break;
                }
            }
            // Skip line comments (;)
            if self.peek() == Some(b';') {
                while let Some(ch) = self.advance() {
                    if ch == b'\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Read one s-expression. Returns None at end of input.
    pub fn read(&mut self) -> Result<Option<Value>, LispError> {
        self.skip_whitespace_and_comments();

        match self.peek() {
            None => Ok(None),
            Some(b'(') => self.read_list(),
            Some(b'[') => self.read_vector(),
            Some(b')') => Err(LispError::ReadError("unexpected ')'".into())),
            Some(b']') => Err(LispError::ReadError("unexpected ']'".into())),
            Some(b'"') => self.read_string(),
            Some(b'\'') => self.read_quote("quote"),
            Some(b'`') => self.read_quote("backquote"),
            Some(b',') => {
                self.advance();
                if self.peek() == Some(b'@') {
                    self.advance();
                    self.read_quote("comma-at")
                } else {
                    self.read_quote("comma")
                }
            }
            Some(b'?') => self.read_character(),
            Some(b'#') => self.read_hash(),
            _ => self.read_atom(),
        }
    }

    /// Read all expressions from the input.
    pub fn read_all(&mut self) -> Result<Vec<Value>, LispError> {
        let mut forms = Vec::new();
        while let Some(val) = self.read()? {
            forms.push(val);
        }
        Ok(forms)
    }

    fn apply_symbol_shorthands(&self, token: String) -> String {
        // GNU deliberately preserves the traditional punctuation-only core
        // symbols even when one of their characters is configured as a
        // shorthand prefix.  For example, a `-` shorthand may expand
        // `-foo`, but must not rename `/=` or `---`.
        if token.bytes().all(|byte| {
            matches!(
                byte,
                b'^' | b'*' | b'+' | b'-' | b'/' | b'<' | b'=' | b'>' | b'_' | b'|'
            )
        }) {
            return token;
        }
        for (short, long) in &self.symbol_shorthands {
            if let Some(rest) = token.strip_prefix(short) {
                return format!("{long}{rest}");
            }
        }
        token
    }

    fn read_list(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume '('
        let mut items = Vec::new();
        let mut dotted_end: Option<Value> = None;

        loop {
            self.skip_whitespace_and_comments();
            match self.peek() {
                None => return Err(LispError::EndOfInput),
                Some(b')') => {
                    self.advance();
                    break;
                }
                _ => {
                    // Check for dotted pair
                    if self.peek() == Some(b'.') && !items.is_empty() {
                        let saved = self.pos;
                        self.advance();
                        // Only a dot if followed by whitespace or paren
                        match self.peek_char() {
                            Some(ch) if ch.is_whitespace() || ch == ')' => {
                                let val = self.read()?.ok_or(LispError::EndOfInput)?;
                                dotted_end = Some(val);
                                self.skip_whitespace_and_comments();
                                if self.peek() == Some(b')') {
                                    self.advance();
                                    break;
                                }
                                return Err(LispError::ReadError(
                                    "expected ')' after dotted pair".into(),
                                ));
                            }
                            Some(',') if self.backquote_depth > 0 => {
                                let val = self.read()?.ok_or(LispError::EndOfInput)?;
                                dotted_end = Some(val);
                                self.skip_whitespace_and_comments();
                                if self.peek() == Some(b')') {
                                    self.advance();
                                    break;
                                }
                                return Err(LispError::ReadError(
                                    "expected ')' after dotted pair".into(),
                                ));
                            }
                            _ => {
                                // Not a dot separator, it's an atom starting with '.'
                                self.pos = saved;
                                let val = self.read()?.ok_or(LispError::EndOfInput)?;
                                items.push(val);
                            }
                        }
                    } else {
                        let val = self.read()?.ok_or(LispError::EndOfInput)?;
                        items.push(val);
                    }
                }
            }
        }

        // Build the list from the end
        let mut result = dotted_end.unwrap_or(Value::Nil);
        for item in items.into_iter().rev() {
            result = Value::cons(item, result);
        }
        Ok(Some(result))
    }

    fn read_vector(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume '['
        let mut items = vec![Value::symbol("vector-literal")];
        loop {
            self.skip_whitespace_and_comments();
            match self.peek() {
                None => return Err(LispError::EndOfInput),
                Some(b']') => {
                    self.advance();
                    break;
                }
                _ => {
                    let val = self.read()?.ok_or(LispError::EndOfInput)?;
                    items.push(val);
                }
            }
        }
        Ok(Some(Value::list(items)))
    }

    fn read_string(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume opening '"'
        let mut s = String::new();
        let mut has_explicit_multibyte = false;
        let mut has_raw_bytes = false;
        let mut has_invalid_unicode = false;
        loop {
            match self.peek() {
                None => return Err(LispError::EndOfInput),
                Some(b'"') => {
                    self.advance();
                    // A Lisp string is an object even when it has no text
                    // properties.  Keeping ordinary source literals as the
                    // scalar `Value::String` shortcut loses object identity
                    // when a quoted list is traversed or macro-expanded.
                    // GNU's reader allocates the object once, so repeated
                    // evaluation of the same literal returns that object.
                    return Ok(Some(Value::StringObject(Rc::new(RefCell::new(
                        SharedStringState {
                            text: s,
                            props: Vec::new(),
                            multibyte: has_explicit_multibyte || has_invalid_unicode,
                        },
                    )))));
                }
                Some(b'\\') => {
                    self.advance();
                    if self
                        .peek()
                        .is_some_and(|ch| matches!(ch, b'C' | b'M' | b'^'))
                        && (self.peek() == Some(b'^')
                            || self.input.get(self.pos + 1).copied() == Some(b'-'))
                    {
                        let code = self.read_string_modified_escape()?;
                        Self::push_string_escape_code(
                            &mut s,
                            code,
                            &mut has_explicit_multibyte,
                            &mut has_raw_bytes,
                            &mut has_invalid_unicode,
                        );
                        continue;
                    }
                    match self.advance() {
                        None => return Err(LispError::EndOfInput),
                        Some(b'n') => s.push('\n'),
                        Some(b't') => s.push('\t'),
                        Some(b'r') => s.push('\r'),
                        Some(b'e') => s.push('\x1B'),
                        Some(b'\n') => {}
                        Some(b'\r') => {
                            if self.peek() == Some(b'\n') {
                                self.advance();
                            }
                        }
                        Some(b'\\') => s.push('\\'),
                        Some(b'"') => s.push('"'),
                        // GNU treats backslash-space in a string as a
                        // zero-width escape.  It is commonly used to
                        // terminate a numeric escape without inserting a
                        // separator, as in "\\0\\ 29".
                        Some(b' ') => {}
                        Some(b's') => s.push(' '),
                        Some(b'a') => s.push('\x07'),
                        Some(b'b') => s.push('\x08'),
                        Some(b'f') => s.push('\x0C'),
                        Some(b'^') => {
                            let code = self.read_string_control_escape()?;
                            if code <= 0x7F {
                                s.push(char::from_u32(code).unwrap_or(char::REPLACEMENT_CHARACTER));
                            } else if code <= 0xFF {
                                has_raw_bytes = true;
                                s.push(encode_raw_byte(code as u8));
                            } else if valid_unicode_scalar(code) {
                                let c = char::from_u32(code).expect("validated scalar");
                                has_explicit_multibyte = true;
                                s.push(c);
                            } else {
                                has_invalid_unicode = true;
                                s.push(INVALID_UNICODE_SENTINEL);
                            }
                        }
                        Some(b'x') => {
                            // Emacs reads as many contiguous hex digits as it can here.
                            let hex = self.read_hex_digits(usize::MAX);
                            if hex <= 0x7F {
                                s.push(char::from_u32(hex).unwrap_or(char::REPLACEMENT_CHARACTER));
                            } else if hex <= 0xFF {
                                has_raw_bytes = true;
                                s.push(encode_raw_byte(hex as u8));
                            } else if (0x3F_FF00..=0x3F_FFFF).contains(&hex) {
                                // Emacs eight-bit (raw byte) codepoints
                                // #x3FFF00..#x3FFFFF map to the internal
                                // raw-byte marker.
                                has_raw_bytes = true;
                                s.push(encode_raw_byte((hex - 0x3F_FF00) as u8));
                            } else if valid_unicode_scalar(hex) {
                                let c = char::from_u32(hex).expect("validated scalar");
                                has_explicit_multibyte = true;
                                s.push(c);
                            } else {
                                has_invalid_unicode = true;
                                s.push(INVALID_UNICODE_SENTINEL);
                            }
                        }
                        Some(b'u') => {
                            // Unicode escape: \uNNNN
                            let hex = self.read_hex_digits(4);
                            if valid_unicode_scalar(hex) {
                                let c = char::from_u32(hex).expect("validated scalar");
                                if hex > 0x7F {
                                    has_explicit_multibyte = true;
                                }
                                s.push(c);
                            } else {
                                has_invalid_unicode = true;
                                s.push(INVALID_UNICODE_SENTINEL);
                            }
                        }
                        Some(b'U') => {
                            // Unicode escape: \UNNNNNNNN
                            let hex = self.read_hex_digits(8);
                            if valid_unicode_scalar(hex) {
                                let c = char::from_u32(hex).expect("validated scalar");
                                if hex > 0x7F {
                                    has_explicit_multibyte = true;
                                }
                                s.push(c);
                            } else {
                                has_invalid_unicode = true;
                                s.push(INVALID_UNICODE_SENTINEL);
                            }
                        }
                        Some(b'N') => {
                            if self.peek() == Some(b'{') {
                                let code = self.read_named_character_code()?;
                                if valid_unicode_scalar(code) {
                                    let c = char::from_u32(code).expect("validated scalar");
                                    if code > 0x7F {
                                        has_explicit_multibyte = true;
                                    }
                                    s.push(c);
                                } else {
                                    has_invalid_unicode = true;
                                    s.push(INVALID_UNICODE_SENTINEL);
                                }
                            } else {
                                s.push('N');
                            }
                        }
                        Some(ch) if ch.is_ascii_digit() => {
                            // Octal escape
                            let mut val = (ch - b'0') as u32;
                            for _ in 0..2 {
                                match self.peek() {
                                    Some(d) if d.is_ascii_digit() && d < b'8' => {
                                        self.advance();
                                        val = val * 8 + (d - b'0') as u32;
                                    }
                                    _ => break,
                                }
                            }
                            if val <= 0x7F {
                                s.push(char::from_u32(val).unwrap_or(char::REPLACEMENT_CHARACTER));
                            } else if val <= 0xFF {
                                has_raw_bytes = true;
                                s.push(encode_raw_byte(val as u8));
                            } else if let Some(c) = char::from_u32(val) {
                                has_explicit_multibyte = true;
                                s.push(c);
                            } else {
                                s.push(char::REPLACEMENT_CHARACTER);
                            }
                        }
                        Some(ch) => {
                            s.push(ch as char);
                        }
                    }
                }
                Some(ch) if ch < 0x80 => {
                    self.advance();
                    s.push(ch as char);
                }
                Some(_) => {
                    // Multi-byte UTF-8: decode properly
                    if let Some(c) = self.read_utf8_char() {
                        if raw_byte_from_source_char(c).is_some() {
                            has_raw_bytes = true;
                        } else {
                            has_explicit_multibyte = true;
                        }
                        s.push(c);
                    } else {
                        self.advance(); // skip invalid byte
                        s.push(char::REPLACEMENT_CHARACTER);
                    }
                }
            }
        }
    }

    fn read_string_control_escape(&mut self) -> Result<u32, LispError> {
        let Some(ch) = self.advance() else {
            return Err(LispError::EndOfInput);
        };
        Ok(match ch {
            b'?' => 0x7F,
            b'a'..=b'z' => (ch - b'a' + 1) as u32,
            b'A'..=b'Z' => (ch - b'A' + 1) as u32,
            _ => (ch & 0x1F) as u32,
        })
    }

    fn push_string_escape_code(
        s: &mut String,
        code: u32,
        has_explicit_multibyte: &mut bool,
        has_raw_bytes: &mut bool,
        has_invalid_unicode: &mut bool,
    ) {
        if code <= 0x7F {
            s.push(char::from_u32(code).unwrap_or(char::REPLACEMENT_CHARACTER));
        } else if code <= 0xFF {
            *has_raw_bytes = true;
            s.push(encode_raw_byte(code as u8));
        } else if valid_unicode_scalar(code) {
            let c = char::from_u32(code).expect("validated scalar");
            *has_explicit_multibyte = true;
            s.push(c);
        } else {
            *has_invalid_unicode = true;
            s.push(INVALID_UNICODE_SENTINEL);
        }
    }

    fn read_string_modified_escape(&mut self) -> Result<u32, LispError> {
        const META_BIT: i64 = 1 << 27;

        let mut modifiers = 0i64;
        let mut ctrl_count = 0u32;
        let mut first = true;
        loop {
            if !first {
                // GNU chains modifiers only through another backslash escape
                // ("\C-\M-a"); a bare `C-', `M-' or `^' after the first
                // modifier is the target character ("\C-^" is control-^).
                let chained = self.peek() == Some(b'\\')
                    && match self.input.get(self.pos + 1).copied() {
                        Some(b'^') => true,
                        Some(b'C' | b'M') => self.input.get(self.pos + 2).copied() == Some(b'-'),
                        _ => false,
                    };
                if !chained {
                    break;
                }
                self.advance();
            }
            first = false;
            match (self.peek(), self.input.get(self.pos + 1).copied()) {
                (Some(b'C'), Some(b'-')) => {
                    ctrl_count += 1;
                    self.pos += 2;
                }
                (Some(b'M'), Some(b'-')) => {
                    modifiers |= META_BIT;
                    self.pos += 2;
                }
                (Some(b'^'), _) => {
                    ctrl_count += 1;
                    self.pos += 1;
                }
                _ => break,
            }
        }

        let mut value = if self.peek() == Some(b'\\') {
            self.read_escaped_character_code()?
        } else {
            self.read_literal_character_code()?
        };

        // GNU read_char_escape folds each control prefix over letters and
        // the 0x40..0x5F block; anything else keeps the control modifier
        // bit, which only `\C-SPC' (-> NUL) survives inside a string.
        let mut ctrl_pending = false;
        for _ in 0..ctrl_count {
            value = match value {
                0x3f => 0x7f,
                n if (0x40..=0x5f).contains(&n) || (0x61..=0x7a).contains(&n) => n & 0x1f,
                n => {
                    ctrl_pending = true;
                    n
                }
            };
        }
        if ctrl_pending {
            if value == 0x20 && modifiers == 0 {
                return Ok(0);
            }
            return Err(LispError::ReadError("Invalid modifier in string".into()));
        }

        if modifiers == META_BIT && value <= 0x7F {
            value |= 0x80;
            modifiers &= !META_BIT;
        }

        if modifiers != 0 {
            return Err(LispError::ReadError("Invalid modifier in string".into()));
        }

        Ok(value as u32)
    }

    fn read_hex_digits(&mut self, max: usize) -> u32 {
        let mut val: u32 = 0;
        let mut remaining = max;
        let unlimited = max == usize::MAX;
        while unlimited || remaining > 0 {
            match self.peek() {
                Some(ch) if ch.is_ascii_hexdigit() => {
                    self.advance();
                    let digit = match ch {
                        b'0'..=b'9' => ch - b'0',
                        b'a'..=b'f' => ch - b'a' + 10,
                        b'A'..=b'F' => ch - b'A' + 10,
                        _ => unreachable!(),
                    };
                    val = val.saturating_mul(16).saturating_add(digit as u32);
                    if !unlimited {
                        remaining -= 1;
                    }
                }
                _ => break,
            }
        }
        val
    }

    fn read_utf8_char(&mut self) -> Option<char> {
        let start = self.pos;
        let first = *self.input.get(self.pos)?;
        let len = if first < 0x80 {
            1
        } else if first < 0xE0 {
            2
        } else if first < 0xF0 {
            3
        } else {
            4
        };
        if self.pos + len > self.input.len() {
            return None;
        }
        let s = std::str::from_utf8(&self.input[start..start + len]).ok()?;
        self.pos += len;
        s.chars().next()
    }

    fn read_quote(&mut self, name: &str) -> Result<Option<Value>, LispError> {
        if name != "comma" && name != "comma-at" {
            self.advance(); // consume the quote/backquote char
        }
        let inner = if name == "backquote" {
            self.backquote_depth += 1;
            let result = self
                .read()
                .and_then(|value| value.ok_or(LispError::EndOfInput));
            self.backquote_depth = self.backquote_depth.saturating_sub(1);
            result?
        } else {
            self.read()?.ok_or(LispError::EndOfInput)?
        };
        let symbol = match (self.raw_quote_symbols, name) {
            (true, "backquote") => "`",
            (true, "comma") => ",",
            (true, "comma-at") => ",@",
            _ => name,
        };
        Ok(Some(Value::list([Value::symbol(symbol), inner])))
    }

    fn read_character(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume '?'
        match self.peek() {
            None => Err(LispError::EndOfInput),
            Some(b'\\') => {
                const ALT_BIT: i64 = 1 << 22;
                const SUPER_BIT: i64 = 1 << 23;
                const HYPER_BIT: i64 = 1 << 24;
                const SHIFT_BIT: i64 = 1 << 25;
                const CTRL_BIT: i64 = 1 << 26;
                const META_BIT: i64 = 1 << 27;

                let mut modifiers = 0i64;
                let mut ctrl_count = 0u8;
                let mut saw_modifier = false;
                loop {
                    let escaped_modifier_start = self.peek() == Some(b'\\')
                        && matches!(
                            (
                                self.input.get(self.pos + 1).copied(),
                                self.input.get(self.pos + 2).copied(),
                            ),
                            (Some(b'^'), _)
                                | (Some(b'A' | b'S' | b'C' | b'H' | b'M' | b's'), Some(b'-'))
                        );
                    if !escaped_modifier_start {
                        // GNU chains modifiers only through another
                        // backslash escape (?\C-\M-a); a bare `M-' or `^'
                        // after the first modifier is the target character.
                        break;
                    }
                    self.advance();
                    match (self.peek(), self.input.get(self.pos + 1).copied()) {
                        (Some(b'A'), Some(b'-')) => {
                            saw_modifier = true;
                            modifiers |= ALT_BIT;
                            self.pos += 2;
                        }
                        (Some(b'S'), Some(b'-')) => {
                            saw_modifier = true;
                            modifiers |= SHIFT_BIT;
                            self.pos += 2;
                        }
                        (Some(b'C'), Some(b'-')) => {
                            saw_modifier = true;
                            ctrl_count = ctrl_count.saturating_add(1);
                            self.pos += 2;
                        }
                        (Some(b'H'), Some(b'-')) => {
                            saw_modifier = true;
                            modifiers |= HYPER_BIT;
                            self.pos += 2;
                        }
                        (Some(b'M'), Some(b'-')) => {
                            saw_modifier = true;
                            modifiers |= META_BIT;
                            self.pos += 2;
                        }
                        (Some(b's'), Some(b'-')) => {
                            saw_modifier = true;
                            modifiers |= SUPER_BIT;
                            self.pos += 2;
                        }
                        (Some(b'^'), _) => {
                            saw_modifier = true;
                            ctrl_count = ctrl_count.saturating_add(1);
                            self.pos += 1;
                        }
                        _ => break,
                    }
                }
                let mut value = if !saw_modifier || self.peek() == Some(b'\\') {
                    self.read_escaped_character_code()?
                } else {
                    self.read_literal_character_code()?
                };
                // GNU read_char_escape folds each control prefix over
                // letters and the 0x40..0x5F block and turns `?' into DEL;
                // any other target keeps the control modifier bit.
                for _ in 0..ctrl_count {
                    value = match value {
                        0x3f => 0x7f,
                        n if (0x40..=0x5f).contains(&n) || (0x61..=0x7a).contains(&n) => n & 0x1f,
                        n => {
                            modifiers |= CTRL_BIT;
                            n
                        }
                    };
                }
                value |= modifiers;
                Ok(Some(Value::Integer(value)))
            }
            Some(ch) if ch < 0x80 => {
                self.advance();
                Ok(Some(Value::Integer(ch as i64)))
            }
            Some(_) => {
                // Multi-byte UTF-8 character like ?± or ?Ā
                Ok(Some(Value::Integer(self.read_literal_character_code()?)))
            }
        }
    }

    fn read_literal_character_code(&mut self) -> Result<i64, LispError> {
        match self.peek() {
            None => Err(LispError::EndOfInput),
            Some(ch) if ch < 0x80 => {
                self.advance();
                Ok(ch as i64)
            }
            Some(_) => {
                if let Some(c) = self.read_utf8_char() {
                    Ok(raw_byte_from_source_char(c).map_or(c as i64, i64::from))
                } else {
                    let byte = self.advance().ok_or(LispError::EndOfInput)?;
                    Ok(byte as i64)
                }
            }
        }
    }

    fn read_escaped_character_code(&mut self) -> Result<i64, LispError> {
        if self.peek() == Some(b'\\') {
            self.advance();
            if matches!(self.peek(), Some(b'A' | b'S' | b'C' | b'H' | b'M'))
                && self.input.get(self.pos + 1).copied() != Some(b'-')
            {
                return Err(LispError::ReadError(
                    "invalid character modifier syntax".into(),
                ));
            }
            if self.peek() == Some(b's') && self.input.get(self.pos + 1).copied() == Some(b'-') {
                return Err(LispError::ReadError(
                    "invalid character modifier syntax".into(),
                ));
            }
            if self.peek() == Some(b'\\') && self.input.get(self.pos + 1).copied() == Some(b'\'') {
                self.pos += 2;
                return Ok('\'' as i64);
            }
        }
        if self.peek().is_some_and(|ch| ch >= 0x80) {
            return self.read_utf8_char().map_or_else(
                || {
                    Err(LispError::ReadError(
                        "invalid UTF-8 in character literal".into(),
                    ))
                },
                |ch| Ok(raw_byte_from_source_char(ch).map_or(ch as i64, i64::from)),
            );
        }
        match self.advance() {
            None => Err(LispError::EndOfInput),
            Some(b'\n') | Some(b'\r') => Err(LispError::ReadError(
                "invalid escaped line feed in character literal".into(),
            )),
            Some(b'n') => Ok('\n' as i64),
            Some(b't') => Ok('\t' as i64),
            Some(b'r') => Ok('\r' as i64),
            Some(b'a') => Ok('\x07' as i64),
            Some(b'd') => Ok('\x7F' as i64),
            Some(b'e') => Ok('\x1B' as i64),
            Some(b'b') => Ok('\x08' as i64),
            Some(b'f') => Ok('\x0C' as i64),
            Some(b's') => Ok(' ' as i64),
            Some(b' ') => Ok(' ' as i64),
            Some(b'v') => Ok('\x0B' as i64),
            Some(b'\\') => Ok('\\' as i64),
            Some(b'N') => {
                if self.peek() != Some(b'{') {
                    return Err(LispError::ReadError(
                        "invalid named character escape".into(),
                    ));
                }
                Ok(self.read_named_character_code()? as i64)
            }
            Some(b'x') => {
                let start = self.pos;
                let value = self.read_hex_digits(6);
                if self.pos == start {
                    return Err(LispError::ReadError(
                        "missing hex digits in character escape".into(),
                    ));
                }
                Ok(value as i64)
            }
            Some(b'u') => {
                let start = self.pos;
                let value = self.read_hex_digits(4);
                if self.pos - start != 4 {
                    return Err(LispError::ReadError(
                        "unicode character escape must have four hex digits".into(),
                    ));
                }
                Ok(value as i64)
            }
            Some(b'U') => {
                let start = self.pos;
                let value = self.read_hex_digits(8);
                if self.pos - start != 8 {
                    return Err(LispError::ReadError(
                        "unicode character escape must have eight hex digits".into(),
                    ));
                }
                Ok(value as i64)
            }
            Some(ch) if ch.is_ascii_digit() => {
                let mut val = (ch - b'0') as i64;
                for _ in 0..2 {
                    match self.peek() {
                        Some(d) if d.is_ascii_digit() && d < b'8' => {
                            self.advance();
                            val = val * 8 + (d - b'0') as i64;
                        }
                        _ => break,
                    }
                }
                Ok(val)
            }
            Some(ch) => Ok(ch as i64),
        }
    }

    fn read_named_character_code(&mut self) -> Result<u32, LispError> {
        debug_assert_eq!(self.peek(), Some(b'{'));
        self.advance(); // consume '{'
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch == b'}' {
                let name = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|error| LispError::ReadError(error.to_string()))?;
                self.advance(); // consume '}'
                return resolve_named_character_code(name).ok_or_else(|| {
                    LispError::ReadError(format!("unknown character name {{{name}}}"))
                });
            }
            self.advance();
        }
        Err(LispError::EndOfInput)
    }

    fn read_hash(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume '#'
        match self.peek() {
            None => Err(LispError::ReadError("invalid-read-syntax".into())),
            Some(b'#') => {
                self.advance();
                Ok(Some(Value::Symbol("".into())))
            }
            Some(b'_') => {
                self.advance();
                match self.peek() {
                    None
                    | Some(
                        b' ' | b'\t' | b'\n' | b'\r' | 0x0C | b'(' | b')' | b'[' | b']' | b'"'
                        | b'\'' | b';' | b'#' | b'`' | b',',
                    ) => Ok(Some(Value::Symbol(String::new().into()))),
                    Some(_) => self.read_atom_with_shorthands(false),
                }
            }
            Some(b'\'') => {
                // #'symbol — function quote, treat as (function sym)
                self.advance();
                let inner = self.read()?.ok_or(LispError::EndOfInput)?;
                Ok(Some(Value::list([Value::symbol("function"), inner])))
            }
            Some(b'<') => {
                self.advance();
                Err(LispError::SignalValue(Value::list([
                    Value::Symbol("invalid-read-syntax".into()),
                    Value::String("#<".into()),
                    Value::Integer(1),
                    Value::Integer(2),
                ])))
            }
            Some(b'@') => {
                self.advance();
                let count_start = self.pos;
                let count = self.read_unsigned_decimal();
                if self.pos == count_start {
                    return Err(LispError::ReadError("invalid-read-syntax".into()));
                }
                if count == 0 {
                    self.pos = self.input.len();
                    return Ok(Some(Value::Nil));
                }
                Err(LispError::ReadError("unsupported #@ syntax".into()))
            }
            Some(b'^') => {
                self.advance();
                let marker = if self.peek() == Some(b'^') {
                    self.advance();
                    SUB_CHAR_TABLE_LITERAL_SYMBOL
                } else {
                    CHAR_TABLE_LITERAL_SYMBOL
                };
                if self.peek() != Some(b'[') {
                    return Err(LispError::ReadError("invalid-read-syntax".into()));
                }
                let fields = self.read_bracketed_fields()?;
                if marker == CHAR_TABLE_LITERAL_SYMBOL {
                    if fields.len() < 68 {
                        return Err(LispError::ReadError("invalid size char-table".into()));
                    }
                } else {
                    let Some(Value::Integer(depth @ 1..=3)) = fields.first() else {
                        return Err(LispError::ReadError(
                            "invalid depth in sub-char-table".into(),
                        ));
                    };
                    let expected_contents = [0usize, 16, 32, 128][*depth as usize];
                    if fields.len() != expected_contents + 2 {
                        return Err(LispError::ReadError(
                            "invalid size in sub-char-table".into(),
                        ));
                    }
                    if !matches!(fields.get(1), Some(Value::Integer(0..=0x3f_ffff))) {
                        return Err(LispError::ReadError(
                            "invalid minimum character in sub-char-table".into(),
                        ));
                    }
                }
                Ok(Some(Value::list(
                    std::iter::once(Value::symbol(marker)).chain(fields),
                )))
            }
            Some(b'[') => {
                self.advance();
                let mut fields = Vec::new();
                loop {
                    self.skip_whitespace_and_comments();
                    match self.peek() {
                        None => return Err(LispError::EndOfInput),
                        Some(b']') => {
                            self.advance();
                            break;
                        }
                        _ => {
                            let value = self.read()?.ok_or(LispError::EndOfInput)?;
                            fields.push(value);
                        }
                    }
                }
                // GNU uses the same `#[...]' syntax for both kinds of
                // closure pseudovector.  Byte-compiled functions have a
                // string in the code slot and at least four fields, while an
                // interpreted lexical closure has a cons-list body and can
                // contain only ARGS, BODY, and ENV.  The latter occurs in
                // ordinary `.elc' constants (notably cl-generic.elc), so it
                // must not be rejected as malformed bytecode.
                let interpreted =
                    fields.len() == 3 || fields.get(1).is_some_and(interpreted_closure_code_syntax);
                let valid_len = if interpreted {
                    (3..=6).contains(&fields.len())
                } else {
                    (4..=6).contains(&fields.len())
                };
                if !valid_len {
                    return Err(LispError::ReadError(
                        "invalid byte-code object syntax".into(),
                    ));
                }
                Ok(Some(Value::list(
                    std::iter::once(Value::symbol(RECORD_LITERAL_SYMBOL))
                        .chain(std::iter::once(structure_slot_eval_form(Value::symbol(
                            if interpreted {
                                "interpreted-function"
                            } else {
                                "byte-code-function"
                            },
                        ))))
                        .chain(fields.into_iter().map(structure_slot_eval_form)),
                )))
            }
            Some(b'&') => {
                self.advance();
                let len_start = self.pos;
                let len = self.read_unsigned_decimal() as usize;
                if self.pos == len_start {
                    return Err(LispError::ReadError("missing bool vector length".into()));
                }
                let bytes = match self.read()?.ok_or(LispError::EndOfInput)? {
                    Value::String(text) => text,
                    Value::StringObject(state) => state.borrow().text.clone().into(),
                    other => {
                        return Err(LispError::ReadError(format!(
                            "invalid bool vector literal bytes: expected string, got {}",
                            other.type_name()
                        )));
                    }
                };

                let mut bits = Vec::with_capacity(len);
                for ch in bytes.chars() {
                    let byte = u32::from(ch);
                    if byte > 0xFF {
                        return Err(LispError::ReadError(
                            "bool vector literal byte was out of range".into(),
                        ));
                    }
                    for bit in 0..8 {
                        if bits.len() == len {
                            break;
                        }
                        bits.push((byte & (1 << bit)) != 0);
                    }
                    if bits.len() == len {
                        break;
                    }
                }
                bits.resize(len, false);

                Ok(Some(Value::list(
                    std::iter::once(Value::symbol(BOOL_VECTOR_LITERAL_SYMBOL)).chain(
                        bits.into_iter()
                            .map(|bit| if bit { Value::T } else { Value::Nil }),
                    ),
                )))
            }
            Some(b':') => {
                self.advance();
                let symbol = self.read_atom()?.ok_or(LispError::EndOfInput)?;
                let Value::Symbol(base) = symbol else {
                    return Err(LispError::ReadError(
                        "invalid uninterned symbol syntax".into(),
                    ));
                };
                let id = READER_UNINTERNED_SYMBOL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(Some(Value::Symbol(
                    make_uninterned_symbol_name(&base, id).into(),
                )))
            }
            Some(b'(') => {
                // #(...) — either a self-evaluating vector literal or a
                // string literal with text properties.
                self.advance(); // consume '('
                let mut items = Vec::new();
                loop {
                    self.skip_whitespace_and_comments();
                    match self.peek() {
                        None => return Err(LispError::EndOfInput),
                        Some(b')') => {
                            self.advance();
                            break;
                        }
                        _ => {
                            let val = self.read()?.ok_or(LispError::EndOfInput)?;
                            items.push(val);
                        }
                    }
                }
                if let Some(string) = self.try_read_string_literal_with_properties(&items)? {
                    Ok(Some(string))
                } else {
                    Ok(Some(Value::list(
                        std::iter::once(Value::symbol("vector-literal")).chain(items),
                    )))
                }
            }
            Some(ch) if ch.is_ascii_digit() => {
                let base = self.read_unsigned_decimal();
                let radix = match self.peek() {
                    Some(b'r') | Some(b'R') => {
                        self.advance();
                        base
                    }
                    Some(b'=') => {
                        self.advance();
                        let value = self.read()?.ok_or(LispError::EndOfInput)?;
                        return Ok(Some(Value::list([
                            Value::symbol(CIRCULAR_READ_SYNTAX_SYMBOL),
                            Value::Integer(base as i64),
                            value,
                        ])));
                    }
                    Some(b'#') => {
                        self.advance();
                        return Ok(Some(Value::list([
                            Value::symbol(CIRCULAR_READ_SYNTAX_SYMBOL),
                            Value::Integer(base as i64),
                        ])));
                    }
                    _ => {
                        return Err(LispError::ReadError(
                            "unsupported # syntax after numeric prefix".into(),
                        ));
                    }
                };
                Ok(Some(self.read_radix_integer(radix)?))
            }
            Some(b'x') | Some(b'X') => {
                // #xNN or #x-NN — hexadecimal integer
                self.advance();
                let neg = self.peek() == Some(b'-');
                if neg {
                    self.advance();
                }
                let mut val = BigInt::from(0u8);
                let mut any = false;
                while let Some(ch) = self.peek() {
                    if ch.is_ascii_hexdigit() {
                        self.advance();
                        any = true;
                        let digit = match ch {
                            b'0'..=b'9' => ch - b'0',
                            b'a'..=b'f' => ch - b'a' + 10,
                            b'A'..=b'F' => ch - b'A' + 10,
                            _ => unreachable!(),
                        };
                        val = val * 16u8 + BigInt::from(digit);
                    } else {
                        break;
                    }
                }
                if !any {
                    return Err(LispError::ReadError("no digits after #x".into()));
                }
                if neg {
                    val = -val;
                }
                Ok(Some(normalize_bigint(val)))
            }
            Some(b'o') | Some(b'O') => {
                // #oNN or #o-NN — octal integer
                self.advance();
                let neg = self.peek() == Some(b'-');
                if neg {
                    self.advance();
                }
                let mut val = BigInt::from(0u8);
                let mut any = false;
                while let Some(ch) = self.peek() {
                    if (b'0'..=b'7').contains(&ch) {
                        self.advance();
                        any = true;
                        val = val * 8u8 + BigInt::from(ch - b'0');
                    } else {
                        break;
                    }
                }
                if !any {
                    return Err(LispError::ReadError("no digits after #o".into()));
                }
                if neg {
                    val = -val;
                }
                Ok(Some(normalize_bigint(val)))
            }
            Some(b'b') | Some(b'B') => {
                // #bNN or #b-NN — binary integer
                self.advance();
                let neg = self.peek() == Some(b'-');
                if neg {
                    self.advance();
                }
                let mut val = BigInt::from(0u8);
                let mut any = false;
                while let Some(ch) = self.peek() {
                    if ch == b'0' || ch == b'1' {
                        self.advance();
                        any = true;
                        val = val * 2u8 + BigInt::from(ch - b'0');
                    } else {
                        break;
                    }
                }
                if !any {
                    return Err(LispError::ReadError("no digits after #b".into()));
                }
                if neg {
                    val = -val;
                }
                Ok(Some(normalize_bigint(val)))
            }
            Some(b's') | Some(b'S') => {
                self.advance();
                self.skip_whitespace_and_comments();
                if self.peek() != Some(b'(') {
                    return Err(LispError::ReadError("unsupported #s syntax".into()));
                }
                self.advance(); // consume '('
                self.skip_whitespace_and_comments();
                let Some(kind) = self.read()? else {
                    return Err(LispError::EndOfInput);
                };
                let mut fields = Vec::new();
                loop {
                    self.skip_whitespace_and_comments();
                    match self.peek() {
                        None => return Err(LispError::EndOfInput),
                        Some(b')') => {
                            self.advance();
                            break;
                        }
                        _ => {
                            let value = self.read()?.ok_or(LispError::EndOfInput)?;
                            fields.push(value);
                        }
                    }
                }
                if matches!(&kind, Value::Symbol(kind_name) if kind_name == "hash-table") {
                    let literal = Value::list(
                        std::iter::once(Value::symbol(HASH_TABLE_LITERAL_SYMBOL)).chain(fields),
                    );
                    Ok(Some(Value::list([Value::symbol("quote"), literal])))
                } else {
                    Ok(Some(Value::list(
                        std::iter::once(Value::symbol(RECORD_LITERAL_SYMBOL))
                            .chain(std::iter::once(structure_slot_eval_form(kind)))
                            .chain(fields.into_iter().map(structure_slot_eval_form)),
                    )))
                }
            }
            _ => {
                // Treat unknown hash-prefixed syntax as a symbol token that starts with '#'.
                self.pos = self.pos.saturating_sub(1);
                self.read_atom()
            }
        }
    }

    fn read_bracketed_fields(&mut self) -> Result<Vec<Value>, LispError> {
        debug_assert_eq!(self.peek(), Some(b'['));
        self.advance();
        let mut fields = Vec::new();
        loop {
            self.skip_whitespace_and_comments();
            match self.peek() {
                None => return Err(LispError::EndOfInput),
                Some(b']') => {
                    self.advance();
                    return Ok(fields);
                }
                _ => fields.push(self.read()?.ok_or(LispError::EndOfInput)?),
            }
        }
    }

    fn try_read_string_literal_with_properties(
        &self,
        items: &[Value],
    ) -> Result<Option<Value>, LispError> {
        let Some(first) = items.first() else {
            return Ok(None);
        };
        let (text, mut props, multibyte) = match first {
            Value::String(text) => (text.to_string(), Vec::new(), false),
            Value::StringObject(state) => {
                let state = state.borrow();
                (state.text.clone(), state.props.clone(), state.multibyte)
            }
            _ => return Ok(None),
        };
        if !(items.len() - 1).is_multiple_of(3) {
            return Ok(None);
        }
        let mut index = 1usize;
        while index + 2 < items.len() {
            let start = items[index].as_integer()?;
            let end = items[index + 1].as_integer()?;
            let plist = items[index + 2].to_vec()?;
            let mut span_props = Vec::new();
            let mut cursor = 0usize;
            while cursor + 1 < plist.len() {
                span_props.push((
                    plist[cursor].as_symbol()?.to_string(),
                    plist[cursor + 1].clone(),
                ));
                cursor += 2;
            }
            props.push(StringPropertySpan {
                start: start.max(0) as usize,
                end: end.max(0) as usize,
                props: span_props,
            });
            index += 3;
        }
        Ok(Some(Value::StringObject(Rc::new(RefCell::new(
            SharedStringState {
                text,
                props,
                multibyte,
            },
        )))))
    }

    fn read_atom(&mut self) -> Result<Option<Value>, LispError> {
        self.read_atom_with_shorthands(true)
    }

    fn read_atom_with_shorthands(
        &mut self,
        apply_shorthands: bool,
    ) -> Result<Option<Value>, LispError> {
        let mut token = String::new();
        let mut saw_escape = false;
        while let Some(ch) = self.peek() {
            // GNU's reader also ends a symbol at unescaped quote, backquote,
            // and comma (read0 stops at "\"';()[]#`,"), so `,flags, hop-real'
            // reads as two unquotes rather than a symbol named "flags,".
            if ch == b' '
                || ch == b'\t'
                || ch == b'\n'
                || ch == b'\r'
                || ch == 0x0C
                || ch == b'('
                || ch == b')'
                || ch == b'['
                || ch == b']'
                || ch == b'"'
                || ch == b';'
                || (!token.is_empty() && (ch == b',' || ch == b'\'' || ch == b'`'))
            {
                break;
            }
            if ch == b'\\' {
                saw_escape = true;
                self.advance();
                match self.peek() {
                    None => return Err(LispError::EndOfInput),
                    Some(next) if next < 0x80 => {
                        self.advance();
                        token.push(next as char);
                    }
                    Some(_) => {
                        if let Some(next) = self.read_utf8_char() {
                            token.push(next);
                        } else {
                            return Err(LispError::ReadError(
                                "invalid UTF-8 escape in symbol".into(),
                            ));
                        }
                    }
                }
                continue;
            }
            if ch < 0x80 {
                self.advance();
                token.push(ch as char);
            } else if let Some(next) = self.read_utf8_char() {
                token.push(next);
            } else {
                return Err(LispError::ReadError("invalid UTF-8 in symbol".into()));
            }
        }

        if token.is_empty() {
            return Err(LispError::EndOfInput);
        }

        if saw_escape {
            let token = if apply_shorthands {
                self.apply_symbol_shorthands(token)
            } else {
                token
            };
            return Ok(Some(match token.as_str() {
                "nil" => Value::Nil,
                "t" => Value::T,
                _ => Value::Symbol(token.into()),
            }));
        }

        // Try parsing as integer
        if apply_shorthands {
            if let Ok(n) = token.parse::<i64>() {
                return Ok(Some(Value::Integer(n)));
            }
            if is_integer_token(&token)
                && let Ok(n) = token.parse::<BigInt>()
            {
                return Ok(Some(normalize_bigint(n)));
            }

            if let Some(f) = parse_special_float_token(&token) {
                return Ok(Some(Value::Float(f)));
            }
            if let Some(number) = parse_decimal_token(&token) {
                return Ok(Some(number));
            }
        }

        let token = if apply_shorthands {
            self.apply_symbol_shorthands(token)
        } else {
            token
        };

        // Special atoms
        match token.as_str() {
            "nil" => Ok(Some(Value::Nil)),
            "t" => Ok(Some(Value::T)),
            _ => Ok(Some(Value::Symbol(token.into()))),
        }
    }

    fn read_unsigned_decimal(&mut self) -> u32 {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                self.advance();
            } else {
                break;
            }
        }
        std::str::from_utf8(&self.input[start..self.pos])
            .ok()
            .and_then(|digits| digits.parse::<u32>().ok())
            .unwrap_or(10)
    }

    fn read_radix_integer(&mut self, base: u32) -> Result<Value, LispError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.advance();
        }
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() {
                self.advance();
            } else {
                break;
            }
        }
        let token = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|e| LispError::ReadError(e.to_string()))?;
        parse_radix_integer(base, token)
    }
}

fn is_integer_token(token: &str) -> bool {
    let digits = token.strip_prefix(['+', '-']).unwrap_or(token);
    !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())
}

fn parse_decimal_token(token: &str) -> Option<Value> {
    if let Some(integer) = token.strip_suffix('.') {
        let digits = integer.strip_prefix(['+', '-']).unwrap_or(integer);
        if !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()) {
            if let Ok(value) = integer.parse::<i64>() {
                return Some(Value::Integer(value));
            }
            if let Ok(value) = integer.parse::<BigInt>() {
                return Some(normalize_bigint(value));
            }
        }
    }
    if (token.contains('.') || token.contains('e') || token.contains('E'))
        && let Ok(value) = token.parse::<f64>()
    {
        return Some(Value::Float(value));
    }
    None
}

pub(crate) fn parse_special_float_token(token: &str) -> Option<f64> {
    let (mantissa, suffix) = token.split_once(['e', 'E'])?;
    let mantissa_value = mantissa.parse::<f64>().ok()?;
    let upper_suffix = suffix.to_ascii_uppercase();
    if upper_suffix == "+NAN" {
        let sign = if mantissa_value.is_sign_negative() {
            -1.0
        } else {
            1.0
        };
        return Some(f64::NAN.copysign(sign));
    }
    if upper_suffix == "+INF" {
        return Some(if mantissa_value.is_sign_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    None
}

fn normalize_bigint(value: BigInt) -> Value {
    value
        .to_i64()
        .map(Value::Integer)
        .unwrap_or(Value::BigInteger(value.into()))
}

fn valid_unicode_scalar(value: u32) -> bool {
    value <= 0x10_FFFF && !(0xD800..=0xDFFF).contains(&value)
}

fn resolve_named_character_code(name: &str) -> Option<u32> {
    let trimmed = name.trim_matches(|ch: char| ch.is_ascii_whitespace());
    if let Some(hex) = trimmed
        .strip_prefix("U+")
        .or_else(|| trimmed.strip_prefix("u+"))
    {
        if hex.is_empty() || !hex.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return None;
        }
        let value = u32::from_str_radix(hex, 16).ok()?;
        return valid_unicode_scalar(value).then_some(value);
    }

    unicode_character(trimmed).map(|ch| ch as u32)
}

fn parse_radix_integer(base: u32, token: &str) -> Result<Value, LispError> {
    if !(2..=36).contains(&base) {
        return Err(LispError::ReadError(format!("invalid radix {}", base)));
    }
    let (negative, digits) = token
        .strip_prefix('-')
        .map_or((false, token), |rest| (true, rest));
    if digits.is_empty() {
        return Err(LispError::ReadError("missing radix digits".into()));
    }
    let mut value = BigInt::from(0u8);
    for ch in digits.chars() {
        let digit = ch
            .to_digit(base)
            .ok_or_else(|| LispError::ReadError(format!("invalid radix digit {}", ch)))?;
        value = value * BigInt::from(base) + BigInt::from(digit);
    }
    if negative {
        value = -value;
    }
    Ok(normalize_bigint(value))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn read_one(s: &str) -> Value {
        Reader::new(s).read().unwrap().unwrap()
    }

    #[test]
    fn atoms() {
        assert_eq!(read_one("42"), Value::Integer(42));
        assert_eq!(read_one("-7"), Value::Integer(-7));
        assert_eq!(read_one("nil"), Value::Nil);
        assert_eq!(read_one("t"), Value::T);
        assert_eq!(read_one("foo"), Value::Symbol("foo".into()));
        assert_eq!(
            read_one("buffer-string"),
            Value::Symbol("buffer-string".into())
        );
        assert_eq!(read_one("##"), Value::Symbol("".into()));
    }

    #[test]
    fn uninterned_symbols() {
        let value = read_one("#:a");
        let Value::Symbol(symbol) = value else {
            panic!("expected symbol");
        };
        assert_eq!(crate::lisp::types::visible_symbol_name(&symbol), "a");
        assert_ne!(symbol, "a");
        assert_ne!(symbol, ":a");
    }

    #[test]
    fn strings() {
        let ordinary = read_one(r#""hello""#);
        assert_eq!(ordinary, Value::String("hello".into()));
        assert!(
            matches!(ordinary, Value::StringObject(_)),
            "source string literals must retain Lisp object identity"
        );
        assert_eq!(read_one(r#""a\nb""#), Value::String("a\nb".into()));
        assert_eq!(read_one(r#""a\"b""#), Value::String("a\"b".into()));
        assert_eq!(read_one("\"a\\\nb\""), Value::String("ab".into()));
        assert_eq!(
            read_one(r#""\0\ 29""#),
            Value::String("\x00\x32\x39".into())
        );
        assert_eq!(read_one(r#""\(""#), Value::String("(".into()));
        assert_eq!(read_one(r#""\\(""#), Value::String("\\(".into()));
        assert_eq!(
            read_one(r#""\^@\^H\^?""#),
            Value::String("\0\x08\x7f".into())
        );
    }

    #[test]
    fn lists() {
        let val = read_one("(1 2 3)");
        let items = val.to_vec().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], Value::Integer(1));
        assert_eq!(items[1], Value::Integer(2));
        assert_eq!(items[2], Value::Integer(3));
    }

    #[test]
    fn nested_lists() {
        let val = read_one("(+ 1 (+ 2 3))");
        let items = val.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("+".into()));
        assert_eq!(items[1], Value::Integer(1));
        assert!(items[2].is_cons());
    }

    #[test]
    fn quoted() {
        let val = read_one("'foo");
        let items = val.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("quote".into()));
        assert_eq!(items[1], Value::Symbol("foo".into()));
    }

    #[test]
    fn characters() {
        assert_eq!(read_one("?a"), Value::Integer(b'a' as i64));
        assert_eq!(read_one("?\\n"), Value::Integer(b'\n' as i64));
        assert_eq!(read_one("?\\s"), Value::Integer(b' ' as i64));
        assert_eq!(read_one("?\\ "), Value::Integer(b' ' as i64));
        assert_eq!(read_one("?\\\\"), Value::Integer(b'\\' as i64));
        assert_eq!(read_one("?\\'"), Value::Integer(b'\'' as i64));
        assert_eq!(read_one("?\\a"), Value::Integer(b'\x07' as i64));
        assert_eq!(read_one("?\\b"), Value::Integer(b'\x08' as i64));
        assert_eq!(read_one("?\\d"), Value::Integer(b'\x7F' as i64));
        assert_eq!(read_one("?\\e"), Value::Integer(b'\x1B' as i64));
        assert_eq!(read_one("?\\f"), Value::Integer(b'\x0C' as i64));
        assert_eq!(read_one("?\\v"), Value::Integer(b'\x0B' as i64));
    }

    #[test]
    fn invalid_character_escapes_signal_errors() {
        for input in [
            "?\\",
            "?\\C",
            "?\\M",
            "?\\S",
            "?\\H",
            "?\\A",
            "?\\C-\\M",
            "?\\x",
            "?\\u",
            "?\\u234",
            "?\\U",
            "?\\U0010010",
            "?\\N",
        ] {
            assert!(Reader::new(input).read().is_err(), "{input} should fail");
        }
    }

    #[test]
    fn hash_dispatch_edge_cases() {
        assert!(Reader::new("#").read().is_err());
        assert_eq!(read_one("#_"), Value::Symbol(String::new().into()));
        assert_eq!(read_one("#@00 ignored"), Value::Nil);
        assert!(Reader::new("#[0 \"\"]").read().is_err());
    }

    #[test]
    fn reads_three_field_interpreted_closure_syntax() {
        let value = read_one("#[(_tag &rest _) ('(t)) (dynamic-name t)]");
        let items = value.to_vec().expect("closure reader form");
        assert!(matches!(
            items.as_slice(),
            [Value::Symbol(marker), kind, ..]
                if marker == RECORD_LITERAL_SYMBOL
                    && kind.to_vec().ok().is_some_and(|quoted| matches!(
                        quoted.as_slice(),
                        [Value::Symbol(quote), Value::Symbol(name)]
                            if quote == "quote" && name == "interpreted-function"
                    ))
        ));
    }

    #[test]
    fn trailing_dot_decimals_read_as_integers() {
        assert_eq!(read_one("13."), Value::Integer(13));
        assert_eq!(read_one("+13."), Value::Integer(13));
        assert_eq!(read_one("-13."), Value::Integer(-13));
    }

    #[test]
    fn nonbreaking_space_counts_as_whitespace_after_dot() {
        assert_eq!(
            read_one("(a .\u{00A0}b)"),
            Value::cons(Value::Symbol("a".into()), Value::Symbol("b".into()))
        );
    }

    #[test]
    fn escaped_backslash_character_does_not_consume_following_delimiter() {
        let val = read_one("(?\\\\)");
        let items = val.to_vec().unwrap();
        assert_eq!(items, vec![Value::Integer(b'\\' as i64)]);
    }

    #[test]
    fn escaped_multibyte_character_literal_consumes_full_scalar() {
        let val = read_one("(?\\‘)");
        let items = val.to_vec().unwrap();
        assert_eq!(items, vec![Value::Integer('‘' as i64)]);
    }

    #[test]
    fn characters_with_modifiers() {
        assert_eq!(read_one("?\\A-x"), Value::Integer((1 << 22) | ('x' as i64)));
        assert_eq!(read_one("?\\C-\\0"), Value::Integer(1 << 26));
        assert_eq!(read_one("?\\C-x"), Value::Integer(24));
        assert_eq!(read_one("?\\H-x"), Value::Integer((1 << 24) | ('x' as i64)));
        assert_eq!(read_one("?\\M-c"), Value::Integer((1 << 27) | ('c' as i64)));
        assert_eq!(read_one("?\\^C"), Value::Integer(3));
        assert_eq!(read_one("?\\^?"), Value::Integer(127));
        assert_eq!(read_one("?\\s-c"), Value::Integer((1 << 23) | ('c' as i64)));
        assert_eq!(read_one("?\\S-c"), Value::Integer((1 << 25) | ('c' as i64)));
        assert_eq!(read_one("?\\M-\\C-x"), Value::Integer((1 << 27) | 24));
    }

    #[test]
    fn reads_named_unicode_character_escapes() {
        assert_eq!(read_one("?\\N{SNOWFLAKE}"), Value::Integer('❄' as i64));
        assert_eq!(read_one("?\\N{U+A817}"), Value::Integer(0xA817));
    }

    #[test]
    fn reads_named_unicode_string_escapes() {
        let Value::StringObject(state) = read_one(r#""\N{SNOWFLAKE}""#) else {
            panic!("expected a string object");
        };
        let state = state.borrow();
        assert_eq!(state.text, "❄");
        assert!(state.multibyte);
    }

    #[test]
    fn comments_skipped() {
        let val = read_one("; this is a comment\n42");
        assert_eq!(val, Value::Integer(42));
    }

    #[test]
    fn reads_escape_character_string_literals() {
        assert_eq!(read_one(r#""\e[33m""#), Value::String("\x1B[33m".into()));
    }

    #[test]
    fn reads_control_escape_string_literals() {
        assert_eq!(read_one(r#""\C-x\C-f""#), Value::String("\x18\x06".into()));
    }

    #[test]
    fn form_feed_is_treated_as_whitespace() {
        let forms = Reader::new("foo\x0Cbar").read_all().unwrap();
        assert_eq!(
            forms,
            vec![Value::Symbol("foo".into()), Value::Symbol("bar".into())]
        );
    }

    #[test]
    fn dotted_pair() {
        let val = read_one("(a . b)");
        assert_eq!(val.car().unwrap(), Value::Symbol("a".into()));
        assert_eq!(val.cdr().unwrap(), Value::Symbol("b".into()));
    }

    #[test]
    fn dotted_pair_with_modified_character_literal_cdr() {
        let val = read_one("((?A . ?\\A-\\0) (?C . ?\\C-\\0) (?H . ?\\H-\\0) (?M . ?\\M-\\0))");
        let items = val.to_vec().unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].car().unwrap(), Value::Integer('A' as i64));
        assert_eq!(items[0].cdr().unwrap(), Value::Integer(1 << 22));
        assert_eq!(items[1].car().unwrap(), Value::Integer('C' as i64));
        assert_eq!(items[1].cdr().unwrap(), Value::Integer(1 << 26));
        assert_eq!(items[2].car().unwrap(), Value::Integer('H' as i64));
        assert_eq!(items[2].cdr().unwrap(), Value::Integer(1 << 24));
        assert_eq!(items[3].car().unwrap(), Value::Integer('M' as i64));
        assert_eq!(items[3].cdr().unwrap(), Value::Integer(1 << 27));
    }

    #[test]
    fn backquote_dot_comma_reads_as_dotted_comma_tail() {
        assert_eq!(
            read_one("`(t .,t)"),
            Value::list([
                Value::Symbol("`".into()),
                Value::list([Value::T, Value::Symbol(",".into()), Value::T,]),
            ])
        );
    }

    #[test]
    fn read_multiple() {
        let forms = Reader::new("1 2 3").read_all().unwrap();
        assert_eq!(forms.len(), 3);
    }

    #[test]
    fn bare_vector_syntax() {
        assert_eq!(
            read_one("[1 2 foo]"),
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Integer(1),
                Value::Integer(2),
                Value::Symbol("foo".into()),
            ])
        );
    }

    #[test]
    fn reads_strings_with_text_properties_from_hash_syntax() {
        let Value::StringObject(state) = read_one(r#"#("abc" 0 1 (face bold))"#) else {
            panic!("expected a string object");
        };
        let state = state.borrow();
        assert_eq!(state.text, "abc");
        assert_eq!(
            state.props,
            vec![StringPropertySpan {
                start: 0,
                end: 1,
                props: vec![("face".into(), Value::Symbol("bold".into()))],
            }]
        );
    }

    #[test]
    fn reads_hash_radix_integers() {
        assert_eq!(read_one("#16r3FFFFF"), Value::Integer(0x3F_FFFF));
    }

    #[test]
    fn bare_radix_shaped_tokens_remain_symbols() {
        // GNU only recognizes radix integers after the `#' dispatch
        // character.  Tokens such as Artist's local variable `2rx2' are
        // ordinary symbols, even when the prefix before `r' is numeric.
        assert_eq!(read_one("2rx2"), Value::Symbol("2rx2".into()));
        assert_eq!(read_one("16rFF"), Value::Symbol("16rFF".into()));
        assert_eq!(read_one("10r12"), Value::Symbol("10r12".into()));
        assert_eq!(read_one("#16rFF"), Value::Integer(255));
    }

    #[test]
    fn reads_bool_vector_literals() {
        assert_eq!(
            read_one(r#"#&8"\1""#),
            Value::list([
                Value::Symbol(BOOL_VECTOR_LITERAL_SYMBOL.into()),
                Value::T,
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::Nil,
            ])
        );
    }

    #[test]
    fn reads_and_validates_character_table_literals() {
        let ascii = std::iter::repeat_n("nil", 128)
            .collect::<Vec<_>>()
            .join(" ");
        let roots = std::iter::repeat_n("nil", 64).collect::<Vec<_>>().join(" ");
        let literal = read_one(&format!(
            "#^[fallback nil purpose #^^[3 0 {ascii}] {roots}]"
        ));
        let fields = literal.to_vec().expect("character-table marker");
        assert_eq!(
            fields.first(),
            Some(&Value::Symbol(CHAR_TABLE_LITERAL_SYMBOL.into()))
        );
        assert_eq!(fields.len(), 1 + 68);
        assert!(matches!(
            fields[4].to_vec().ok().as_deref(),
            Some([Value::Symbol(marker), Value::Integer(3), Value::Integer(0), ..])
                if marker == SUB_CHAR_TABLE_LITERAL_SYMBOL
        ));

        assert!(matches!(
            Reader::new("#^[nil]").read(),
            Err(LispError::ReadError(message)) if message == "invalid size char-table"
        ));
        assert!(matches!(
            Reader::new("#^^[2 0 nil]").read(),
            Err(LispError::ReadError(message)) if message == "invalid size in sub-char-table"
        ));
    }

    #[test]
    fn reads_reader_labels_as_circular_syntax_forms() {
        let value = read_one("'#1=((a . 1) . #1#)");
        let items = value.to_vec().expect("quote form");
        assert!(matches!(
            items.first(),
            Some(Value::Symbol(symbol)) if symbol == "quote"
        ));
        assert!(circular_read_label_form(&items[1]).is_some());
    }

    #[test]
    fn reads_hash_table_structure_syntax_as_self_evaluating_literal() {
        assert_eq!(
            read_one("#s(hash-table test equal data (\"bla\" \"ble\"))"),
            Value::list([
                Value::Symbol("quote".into()),
                Value::list([
                    Value::Symbol(HASH_TABLE_LITERAL_SYMBOL.into()),
                    Value::Symbol("test".into()),
                    Value::Symbol("equal".into()),
                    Value::Symbol("data".into()),
                    Value::list([Value::String("bla".into()), Value::String("ble".into())]),
                ]),
            ])
        );
    }

    #[test]
    fn reads_record_structure_syntax_as_record_literal_form() {
        assert_eq!(
            read_one("#s(a b #s(c d) [e])"),
            Value::list([
                Value::Symbol(RECORD_LITERAL_SYMBOL.into()),
                Value::list([Value::Symbol("quote".into()), Value::Symbol("a".into())]),
                Value::list([Value::Symbol("quote".into()), Value::Symbol("b".into())]),
                Value::list([
                    Value::Symbol(RECORD_LITERAL_SYMBOL.into()),
                    Value::list([Value::Symbol("quote".into()), Value::Symbol("c".into())]),
                    Value::list([Value::Symbol("quote".into()), Value::Symbol("d".into())]),
                ]),
                Value::list([
                    Value::Symbol("vector-literal".into()),
                    Value::Symbol("e".into()),
                ]),
            ])
        );
    }

    #[test]
    fn expands_symbol_shorthands_while_reading_atoms() {
        let mut reader = Reader::with_symbol_shorthands(
            "(ft--helper 'ft-hash-table-weakness /= - #_ft-raw #_123)",
            vec![("ft-".into(), "fns-tests-".into())],
        );
        assert_eq!(
            reader.read_all().unwrap(),
            vec![Value::list([
                Value::Symbol("fns-tests--helper".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("fns-tests-hash-table-weakness".into()),
                ]),
                Value::Symbol("/=".into()),
                Value::Symbol("-".into()),
                Value::Symbol("ft-raw".into()),
                Value::Symbol("123".into()),
            ])]
        );
    }

    #[test]
    fn symbol_shorthands_preserve_punctuation_only_symbols() {
        let mut reader = Reader::with_symbol_shorthands(
            "(/= 42 (-foo 42))",
            vec![("-".into(), "fooey-".into())],
        );
        assert_eq!(
            reader.read_all().unwrap(),
            vec![Value::list([
                Value::Symbol("/=".into()),
                Value::Integer(42),
                Value::list([Value::Symbol("fooey-foo".into()), Value::Integer(42),]),
            ])]
        );
    }

    #[test]
    fn reads_long_hex_string_escapes() {
        let Value::StringObject(state) = read_one(r#""\x110000""#) else {
            panic!("expected a string object");
        };
        assert_eq!(state.borrow().text, INVALID_UNICODE_SENTINEL.to_string());
    }

    #[test]
    fn ert_deftest_structure() {
        let src = r#"
        (ert-deftest my-test ()
          (with-temp-buffer
            (insert "hello")
            (should (= (point) 6))))
        "#;
        let val = read_one(src);
        let items = val.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("ert-deftest".into()));
        assert_eq!(items[1], Value::Symbol("my-test".into()));
    }
}
