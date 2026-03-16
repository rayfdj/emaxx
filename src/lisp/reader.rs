use super::types::{LispError, SharedStringState, Value};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::{cell::RefCell, rc::Rc};

const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
const INVALID_UNICODE_SENTINEL: char = '\u{F8FF}';
const CIRCULAR_READ_SYNTAX_SYMBOL: &str = "emaxx--circular-read-syntax";
const HASH_TABLE_LITERAL_SYMBOL: &str = "emaxx--hash-table-literal";

fn encode_raw_byte(byte: u8) -> char {
    char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
        .expect("raw byte regex marker is a valid private-use character")
}

/// A simple s-expression reader. Handles the subset of Elisp syntax
/// that appears in ERT test files: atoms, lists, strings, quotes,
/// backquote, characters, and comments.
pub struct Reader<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(input: &'a str) -> Self {
        Reader {
            input: input.as_bytes(),
            pos: 0,
        }
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

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while let Some(ch) = self.peek() {
                if ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r' || ch == 0x0C {
                    self.advance();
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
                        match self.peek() {
                            Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r') | Some(b')')
                            | Some(0x0C) => {
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
                    if has_explicit_multibyte || has_raw_bytes || has_invalid_unicode {
                        return Ok(Some(Value::StringObject(Rc::new(RefCell::new(
                            SharedStringState {
                                text: s,
                                props: Vec::new(),
                                multibyte: has_explicit_multibyte || has_invalid_unicode,
                            },
                        )))));
                    }
                    return Ok(Some(Value::String(s)));
                }
                Some(b'\\') => {
                    self.advance();
                    match self.advance() {
                        None => return Err(LispError::EndOfInput),
                        Some(b'n') => s.push('\n'),
                        Some(b't') => s.push('\t'),
                        Some(b'r') => s.push('\r'),
                        Some(b'\n') => {}
                        Some(b'\r') => {
                            if self.peek() == Some(b'\n') {
                                self.advance();
                            }
                        }
                        Some(b'\\') => s.push('\\'),
                        Some(b'"') => s.push('"'),
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
                        has_explicit_multibyte = true;
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
        let inner = self.read()?.ok_or(LispError::EndOfInput)?;
        Ok(Some(Value::list([Value::symbol(name), inner])))
    }

    fn read_character(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume '?'
        match self.peek() {
            None => Err(LispError::EndOfInput),
            Some(b'\\') => {
                const SUPER_BIT: i64 = 1 << 23;
                const SHIFT_BIT: i64 = 1 << 25;
                const CTRL_BIT: i64 = 1 << 26;
                const META_BIT: i64 = 1 << 27;

                let mut modifiers = 0i64;
                let mut saw_modifier = false;
                loop {
                    if self.peek() == Some(b'\\')
                        && matches!(
                            self.input.get(self.pos + 1).copied(),
                            Some(b'S' | b'C' | b'M' | b's' | b'^')
                        )
                    {
                        self.advance();
                    }
                    match (self.peek(), self.input.get(self.pos + 1).copied()) {
                        (Some(b'S'), Some(b'-')) => {
                            saw_modifier = true;
                            modifiers |= SHIFT_BIT;
                            self.pos += 2;
                        }
                        (Some(b'C'), Some(b'-')) => {
                            saw_modifier = true;
                            modifiers |= CTRL_BIT;
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
                            modifiers |= CTRL_BIT;
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
                if modifiers & CTRL_BIT != 0 && value != 0 {
                    value = match value {
                        0x3f => 0x7f,
                        n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
                        n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
                        n => n & 0x1f,
                    };
                    modifiers &= !CTRL_BIT;
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
                    Ok(c as i64)
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
        }
        if self.peek().is_some_and(|ch| ch >= 0x80) {
            return self
                .read_utf8_char()
                .map(|ch| ch as i64)
                .ok_or_else(|| LispError::ReadError("invalid UTF-8 in character literal".into()));
        }
        match self.advance() {
            None => Err(LispError::EndOfInput),
            Some(b'n') => Ok('\n' as i64),
            Some(b't') => Ok('\t' as i64),
            Some(b'r') => Ok('\r' as i64),
            Some(b's') => Ok(' ' as i64),
            Some(b' ') => Ok(' ' as i64),
            Some(b'\\') => Ok('\\' as i64),
            Some(b'N') => {
                if self.peek() != Some(b'{') {
                    return Ok('N' as i64);
                }
                self.advance(); // consume '{'
                let start = self.pos;
                while let Some(ch) = self.peek() {
                    if ch == b'}' {
                        let name = std::str::from_utf8(&self.input[start..self.pos])
                            .map_err(|e| LispError::ReadError(e.to_string()))?;
                        self.advance(); // consume '}'
                        let ch = match name {
                            "LATIN SMALL LETTER E WITH ACUTE" => '\u{00E9}',
                            "GREEK SMALL LETTER LAMDA" => '\u{03BB}',
                            _ => {
                                return Err(LispError::ReadError(format!(
                                    "unknown character name {{{name}}}"
                                )));
                            }
                        };
                        return Ok(ch as i64);
                    }
                    self.advance();
                }
                Err(LispError::EndOfInput)
            }
            Some(b'x') => Ok(self.read_hex_digits(6) as i64),
            Some(b'u') => Ok(self.read_hex_digits(4) as i64),
            Some(b'U') => Ok(self.read_hex_digits(8) as i64),
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

    fn read_hash(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume '#'
        match self.peek() {
            Some(b'\'') => {
                // #'symbol — function quote, treat as (function sym)
                self.advance();
                let inner = self.read()?.ok_or(LispError::EndOfInput)?;
                Ok(Some(Value::list([Value::symbol("function"), inner])))
            }
            Some(b'(') => {
                // #(...) — read as a self-evaluating vector literal.
                self.advance(); // consume '('
                let mut items = vec![Value::symbol("vector-literal")];
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
                Ok(Some(Value::list(items)))
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
                        let _ = self.read()?.ok_or(LispError::EndOfInput)?;
                        return Ok(Some(Value::symbol(CIRCULAR_READ_SYNTAX_SYMBOL)));
                    }
                    Some(b'#') => {
                        self.advance();
                        return Ok(Some(Value::symbol(CIRCULAR_READ_SYNTAX_SYMBOL)));
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
                let Value::Symbol(kind_name) = kind else {
                    return Err(LispError::ReadError("invalid #s structure name".into()));
                };
                if kind_name != "hash-table" {
                    return Err(LispError::ReadError(format!(
                        "unsupported #s structure `{kind_name}`"
                    )));
                }
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
                let literal = Value::list(
                    std::iter::once(Value::symbol(HASH_TABLE_LITERAL_SYMBOL)).chain(fields),
                );
                Ok(Some(Value::list([Value::symbol("quote"), literal])))
            }
            _ => {
                // Skip unknown hash syntax, try to read as atom
                let val = self.read()?.ok_or(LispError::EndOfInput)?;
                Ok(Some(val))
            }
        }
    }

    fn read_atom(&mut self) -> Result<Option<Value>, LispError> {
        let mut token = String::new();
        while let Some(ch) = self.peek() {
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
            {
                break;
            }
            if ch == b'\\' {
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

        // Try parsing as integer
        if let Ok(n) = token.parse::<i64>() {
            return Ok(Some(Value::Integer(n)));
        }
        if is_integer_token(&token)
            && let Ok(n) = token.parse::<BigInt>()
        {
            return Ok(Some(normalize_bigint(n)));
        }

        if let Some((radix, digits)) = token.split_once(['r', 'R'])
            && let Ok(base) = radix.parse::<u32>()
        {
            return Ok(Some(parse_radix_integer(base, digits)?));
        }

        // Try parsing as float
        if let Some(f) = parse_special_float_token(&token) {
            return Ok(Some(Value::Float(f)));
        }
        if (token.contains('.') || token.contains('e') || token.contains('E'))
            && let Ok(f) = token.parse::<f64>()
        {
            return Ok(Some(Value::Float(f)));
        }

        // Special atoms
        match token.as_str() {
            "nil" => Ok(Some(Value::Nil)),
            "t" => Ok(Some(Value::T)),
            _ => Ok(Some(Value::Symbol(token))),
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

fn parse_special_float_token(token: &str) -> Option<f64> {
    let (mantissa, suffix) = token.split_once(['e', 'E'])?;
    let mantissa_value = mantissa.parse::<f64>().ok()?;
    let upper_suffix = suffix.to_ascii_uppercase();
    if upper_suffix == "+NAN" || upper_suffix == "NAN" {
        let sign = if mantissa_value.is_sign_negative() {
            -1.0
        } else {
            1.0
        };
        return Some(f64::NAN.copysign(sign));
    }
    if upper_suffix == "+INF" || upper_suffix == "INF" {
        return Some(if mantissa_value.is_sign_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    if upper_suffix == "-INF" {
        return Some(if mantissa_value.is_sign_negative() {
            f64::INFINITY
        } else {
            f64::NEG_INFINITY
        });
    }
    if upper_suffix == "-NAN" {
        let sign = if mantissa_value.is_sign_negative() {
            1.0
        } else {
            -1.0
        };
        return Some(f64::NAN.copysign(sign));
    }
    None
}

fn normalize_bigint(value: BigInt) -> Value {
    value
        .to_i64()
        .map(Value::Integer)
        .unwrap_or(Value::BigInteger(value))
}

fn valid_unicode_scalar(value: u32) -> bool {
    value <= 0x10_FFFF && !(0xD800..=0xDFFF).contains(&value)
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
    }

    #[test]
    fn strings() {
        assert_eq!(read_one(r#""hello""#), Value::String("hello".into()));
        assert_eq!(read_one(r#""a\nb""#), Value::String("a\nb".into()));
        assert_eq!(read_one(r#""a\"b""#), Value::String("a\"b".into()));
        assert_eq!(read_one("\"a\\\nb\""), Value::String("ab".into()));
        assert_eq!(read_one(r#""\^@\^H\^?""#), Value::String("\0\x08\x7f".into()));
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
        assert_eq!(read_one("?\\C-x"), Value::Integer(24));
        assert_eq!(read_one("?\\M-c"), Value::Integer((1 << 27) | ('c' as i64)));
        assert_eq!(read_one("?\\^C"), Value::Integer(3));
        assert_eq!(read_one("?\\^?"), Value::Integer(127));
        assert_eq!(read_one("?\\s-c"), Value::Integer((1 << 23) | ('c' as i64)));
        assert_eq!(read_one("?\\S-c"), Value::Integer((1 << 25) | ('c' as i64)));
        assert_eq!(read_one("?\\M-\\C-x"), Value::Integer((1 << 27) | 24));
    }

    #[test]
    fn comments_skipped() {
        let val = read_one("; this is a comment\n42");
        assert_eq!(val, Value::Integer(42));
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
    fn reads_hash_radix_integers() {
        assert_eq!(read_one("#16r3FFFFF"), Value::Integer(0x3F_FFFF));
    }

    #[test]
    fn reads_reader_labels_as_circular_syntax_placeholders() {
        assert_eq!(
            read_one("'#1=((a . 1) . #1#)"),
            Value::list([
                Value::Symbol("quote".into()),
                Value::Symbol(CIRCULAR_READ_SYNTAX_SYMBOL.into()),
            ])
        );
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
