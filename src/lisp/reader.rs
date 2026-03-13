use super::types::{LispError, Value};

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
                if ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r' {
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
            Some(b')') => Err(LispError::ReadError("unexpected ')'".into())),
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
                            Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b')') => {
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

    fn read_string(&mut self) -> Result<Option<Value>, LispError> {
        self.advance(); // consume opening '"'
        let mut s = String::new();
        loop {
            match self.peek() {
                None => return Err(LispError::EndOfInput),
                Some(b'"') => {
                    self.advance();
                    return Ok(Some(Value::String(s)));
                }
                Some(b'\\') => {
                    self.advance();
                    match self.advance() {
                        None => return Err(LispError::EndOfInput),
                        Some(b'n') => s.push('\n'),
                        Some(b't') => s.push('\t'),
                        Some(b'r') => s.push('\r'),
                        Some(b'\\') => s.push('\\'),
                        Some(b'"') => s.push('"'),
                        Some(b'a') => s.push('\x07'),
                        Some(b'b') => s.push('\x08'),
                        Some(b'f') => s.push('\x0C'),
                        Some(b'x') => {
                            // Hex escape: \xNN
                            let hex = self.read_hex_digits(2);
                            if let Some(c) = char::from_u32(hex) {
                                s.push(c);
                            } else {
                                s.push(char::REPLACEMENT_CHARACTER);
                            }
                        }
                        Some(b'u') => {
                            // Unicode escape: \uNNNN
                            let hex = self.read_hex_digits(4);
                            if let Some(c) = char::from_u32(hex) {
                                s.push(c);
                            } else {
                                s.push(char::REPLACEMENT_CHARACTER);
                            }
                        }
                        Some(b'U') => {
                            // Unicode escape: \UNNNNNNNN
                            let hex = self.read_hex_digits(8);
                            if let Some(c) = char::from_u32(hex) {
                                s.push(c);
                            } else {
                                s.push(char::REPLACEMENT_CHARACTER);
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
                            if let Some(c) = char::from_u32(val) {
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
                        s.push(c);
                    } else {
                        self.advance(); // skip invalid byte
                        s.push(char::REPLACEMENT_CHARACTER);
                    }
                }
            }
        }
    }

    fn read_hex_digits(&mut self, max: usize) -> u32 {
        let mut val: u32 = 0;
        for _ in 0..max {
            match self.peek() {
                Some(ch) if ch.is_ascii_hexdigit() => {
                    self.advance();
                    let digit = match ch {
                        b'0'..=b'9' => ch - b'0',
                        b'a'..=b'f' => ch - b'a' + 10,
                        b'A'..=b'F' => ch - b'A' + 10,
                        _ => unreachable!(),
                    };
                    val = val * 16 + digit as u32;
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
        match self.advance() {
            None => Err(LispError::EndOfInput),
            Some(b'\\') => {
                // Escaped character
                match self.advance() {
                    None => Err(LispError::EndOfInput),
                    Some(b'n') => Ok(Some(Value::Integer(b'\n' as i64))),
                    Some(b't') => Ok(Some(Value::Integer(b'\t' as i64))),
                    Some(b'r') => Ok(Some(Value::Integer(b'\r' as i64))),
                    Some(b' ') => Ok(Some(Value::Integer(b' ' as i64))),
                    Some(b'\\') => Ok(Some(Value::Integer(b'\\' as i64))),
                    Some(ch) => Ok(Some(Value::Integer(ch as i64))),
                }
            }
            Some(ch) => Ok(Some(Value::Integer(ch as i64))),
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
                // #(...) — read as (vector ...) for now
                // We'll represent vectors as (vector el1 el2 ...)
                self.advance(); // consume '('
                let mut items = vec![Value::symbol("vector")];
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
            _ => {
                // Skip unknown hash syntax, try to read as atom
                let val = self.read()?.ok_or(LispError::EndOfInput)?;
                Ok(Some(val))
            }
        }
    }

    fn read_atom(&mut self) -> Result<Option<Value>, LispError> {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch == b' '
                || ch == b'\t'
                || ch == b'\n'
                || ch == b'\r'
                || ch == b'('
                || ch == b')'
                || ch == b'"'
                || ch == b';'
            {
                break;
            }
            self.advance();
        }

        let token = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|e| LispError::ReadError(e.to_string()))?;

        if token.is_empty() {
            return Err(LispError::EndOfInput);
        }

        // Try parsing as integer
        if let Ok(n) = token.parse::<i64>() {
            return Ok(Some(Value::Integer(n)));
        }

        // Try parsing as float
        if (token.contains('.') || token.contains('e') || token.contains('E'))
            && let Ok(f) = token.parse::<f64>()
        {
            return Ok(Some(Value::Float(f)));
        }

        // Special atoms
        match token {
            "nil" => Ok(Some(Value::Nil)),
            "t" => Ok(Some(Value::T)),
            _ => Ok(Some(Value::Symbol(token.to_string()))),
        }
    }
}

#[cfg(test)]
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
        assert_eq!(read_one("?\\ "), Value::Integer(b' ' as i64));
    }

    #[test]
    fn comments_skipped() {
        let val = read_one("; this is a comment\n42");
        assert_eq!(val, Value::Integer(42));
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
