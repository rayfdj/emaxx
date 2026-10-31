use super::eval::Interpreter;
use super::types::{Kind, LispError, ReaderForm, Value, visible_symbol_name};
use num_bigint::BigInt;
use num_traits::ToPrimitive;

pub(crate) const INVALID_UNICODE_SENTINEL: char = '\u{F8FF}';
const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
const JSON_SERIALIZATION_MAX_DEPTH: i32 = 50;

#[derive(Clone, Debug)]
pub(crate) enum JsonObjectType {
    HashTable,
    Alist,
    Plist,
}

#[derive(Clone, Debug)]
pub(crate) enum JsonArrayType {
    Vector,
    List,
}

#[derive(Clone, Debug)]
pub(crate) struct JsonParseOptions {
    pub(crate) object_type: JsonObjectType,
    pub(crate) array_type: JsonArrayType,
    pub(crate) null_object: Value,
    pub(crate) false_object: Value,
}

/// data.c wrong_choice: "One of A, B or C should be specified", WRONG.
fn wrong_choice(choices: &[&str], wrong: Value) -> LispError {
    let mut message = String::from("One of ");
    for (index, choice) in choices.iter().enumerate() {
        message.push_str(choice);
        message.push_str(match choices.len() - index {
            1 => " should be specified",
            2 => " or ",
            _ => ", ",
        });
    }
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::String(message.into()),
        wrong,
    ]))
}

/// json.c json_parse_args: the keyword/value pairs ARGS read from the
/// back, so a keyword's first value takes precedence; PARSE_OBJECT_TYPES
/// admits :object-type and :array-type.
pub(crate) fn parse_args(
    args: &[Value],
    parse_object_types: bool,
) -> Result<JsonParseOptions, LispError> {
    if !args.len().is_multiple_of(2) {
        return Err(LispError::WrongTypeArgument(
            "plistp".into(),
            Value::list(args.iter().copied()),
        ));
    }
    let mut conf = JsonParseOptions {
        object_type: JsonObjectType::HashTable,
        array_type: JsonArrayType::Vector,
        null_object: Value::symbol(":null"),
        false_object: Value::symbol(":false"),
    };
    let is =
        |value: &Value, name: &str| matches!(value.kind(), Kind::Symbol(symbol) if symbol == name);
    for pair in args.as_chunks::<2>().0.iter().rev() {
        let (key, value) = (pair[0], pair[1]);
        if parse_object_types && is(&key, ":object-type") {
            conf.object_type = if is(&value, "hash-table") {
                JsonObjectType::HashTable
            } else if is(&value, "alist") {
                JsonObjectType::Alist
            } else if is(&value, "plist") {
                JsonObjectType::Plist
            } else {
                return Err(wrong_choice(&["hash-table", "alist", "plist"], value));
            };
        } else if parse_object_types && is(&key, ":array-type") {
            conf.array_type = if is(&value, "array") {
                JsonArrayType::Vector
            } else if is(&value, "list") {
                JsonArrayType::List
            } else {
                return Err(wrong_choice(&["array", "list"], value));
            };
        } else if is(&key, ":null-object") {
            conf.null_object = value;
        } else if is(&key, ":false-object") {
            conf.false_object = value;
        } else if parse_object_types {
            return Err(wrong_choice(
                &[
                    ":object-type",
                    ":array-type",
                    ":null-object",
                    ":false-object",
                ],
                value,
            ));
        } else {
            return Err(wrong_choice(&[":null-object", ":false-object"], value));
        }
    }
    Ok(conf)
}

/// json_out_t's output: JSON's UTF-8 bytes.
#[derive(Clone, Debug)]
pub(crate) struct JsonSerialized {
    pub(crate) bytes: Vec<u8>,
}

impl JsonSerialized {
    /// The output as text: json_out_string writes Unicode only, as UTF-8.
    pub(crate) fn text(&self) -> &str {
        std::str::from_utf8(&self.bytes).expect("json_out_string writes Unicode as UTF-8")
    }

    /// The output's bytes as a unibyte text, a raw byte as its marker.
    pub(crate) fn unibyte_text(&self) -> String {
        utf8_bytes_to_unibyte_text(&self.bytes)
    }

    /// Fjson_serialize's make_unibyte_string of the output.
    pub(crate) fn into_value(self) -> Value {
        Value::StringObject(crate::lisp::types::StringObjectRef::from_unibyte(
            self.bytes,
        ))
    }
}

/// json.c's json_plain_char: printable ASCII and DEL but `"' and `\',
/// which a string holds as they stand.
fn json_plain_char(byte: u8) -> bool {
    (0x20..=0x7F).contains(&byte) && byte != b'"' && byte != b'\\'
}

/// json.c:json_is_token_char.
fn json_is_token_char(c: i32) -> bool {
    matches!(c, 0x41..=0x5A | 0x61..=0x7A | 0x30..=0x39 | 0x2D)
}

/// json.c's json_parser: the input's bytes as Emacs holds them, read
/// byte by byte with the line and column the error data report; each
/// value made as it is read, an array's elements and an object's members
/// gathered in the object workspace (`objects') until the vector or hash
/// table that holds them is made, a string's or a number's bytes in the
/// byte workspace (`bytes').
struct JsonParser<'a> {
    input: &'a [u8],
    current: usize,
    current_line: i64,
    current_column: i64,
    point_of_current_line: i64,
    available_depth: i32,
    interp: &'a mut Interpreter,
    options: &'a JsonParseOptions,
    objects: Vec<Value>,
    bytes: Vec<u8>,
    /// fns.c's hashtest_equal, looked up at the first object.
    equal_test: Option<&'static crate::lisp::alloc::vectors::hash_tables::HashTableTest>,
    env: crate::lisp::types::Env,
}

/// json_parse's PARSE_END_BEHAVIOR.
#[derive(Clone, Copy, PartialEq)]
enum ParseEnd {
    CheckForGarbage,
    MovePoint,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a [u8], interp: &'a mut Interpreter, options: &'a JsonParseOptions) -> Self {
        JsonParser {
            input,
            current: 0,
            current_line: 1,
            current_column: 0,
            point_of_current_line: 0,
            available_depth: 10000,
            interp,
            options,
            objects: Vec::new(),
            bytes: Vec::new(),
            equal_test: None,
            env: crate::lisp::types::Env::new(),
        }
    }

    /// json_signal_error: ERROR with the line, the column and the
    /// position reached.
    fn error(&self, error: &str) -> LispError {
        LispError::SignalValue(Value::list([
            Value::Symbol(error.into()),
            Value::Integer(self.current_line),
            Value::Integer(self.current_column),
            Value::Integer(self.point_of_current_line + self.current_column),
        ]))
    }

    fn at_eof(&self) -> bool {
        self.current >= self.input.len()
    }

    /// json_input_get.
    fn get(&mut self) -> Result<u8, LispError> {
        match self.input.get(self.current) {
            Some(&byte) => {
                self.current += 1;
                Ok(byte)
            }
            None => Err(self.error("json-end-of-file")),
        }
    }

    /// json_input_get_if_possible: -1 at the end.
    fn get_if_possible(&mut self) -> i32 {
        match self.input.get(self.current) {
            Some(&byte) => {
                self.current += 1;
                i32::from(byte)
            }
            None => -1,
        }
    }

    /// json_input_put_back.
    fn put_back(&mut self) {
        self.current -= 1;
    }

    /// json_skip_whitespace_internal: whether C is not whitespace.
    fn skip_whitespace_internal(&mut self, c: u8) -> bool {
        self.current_column += 1;
        match c {
            0x20 | 0x09 | 0x0D => false,
            0x0A => {
                self.current_line += 1;
                self.point_of_current_line += self.current_column;
                self.current_column = 0;
                false
            }
            _ => true,
        }
    }

    /// json_skip_whitespace.
    fn skip_whitespace(&mut self) -> Result<u8, LispError> {
        loop {
            let c = self.get()?;
            if self.skip_whitespace_internal(c) {
                return Ok(c);
            }
        }
    }

    /// json_skip_whitespace_if_possible: -1 at the end.
    fn skip_whitespace_if_possible(&mut self) -> i32 {
        loop {
            let c = self.get_if_possible();
            if c < 0 || self.skip_whitespace_internal(c as u8) {
                return c;
            }
        }
    }

    /// json_parse_unicode: the CCCC of 쳌.
    fn parse_unicode(&mut self) -> Result<u32, LispError> {
        let mut value = 0;
        for _ in 0..4 {
            let c = self.get()?;
            self.current_column += 1;
            let digit = (c as char)
                .to_digit(16)
                .ok_or_else(|| self.error("json-escape-sequence-error"))?;
            value = value << 4 | digit;
        }
        Ok(value)
    }

    /// The UTF-8 of CODE into the byte workspace: how many bytes more than
    /// one it took.
    fn put_utf8(&mut self, code: u32) -> usize {
        let ch = char::from_u32(code).expect("a scalar value");
        let mut buf = [0; 4];
        let encoded = ch.encode_utf8(&mut buf);
        self.bytes.extend_from_slice(encoded.as_bytes());
        encoded.len() - 1
    }

    /// The byte workspace as a multibyte string of NCHARS characters
    /// (make_multibyte_string).
    fn make_string(&self, nchars: usize) -> Value {
        Value::StringObject(
            crate::lisp::types::StringObjectRef::multibyte_from_internal(&self.bytes, nchars),
        )
    }

    /// The symbol the byte workspace names (intern_c_multibyte).
    fn intern(&self) -> Value {
        Value::Symbol(
            std::str::from_utf8(&self.bytes)
                .expect("strings are read as checked UTF-8")
                .into(),
        )
    }

    /// json_parse_string: the string literal after the opening quote,
    /// strictly UTF-8, into the byte workspace, `:'-prefixed for
    /// LEADING_COLON; how many characters it holds.
    fn parse_string(&mut self, leading_colon: bool) -> Result<usize, LispError> {
        self.bytes.clear();
        if leading_colon {
            self.bytes.push(b':');
        }
        // nbytes - nchars.
        let mut chars_delta = 0;
        loop {
            let c = self.get()?;
            self.current_column += 1;
            if json_plain_char(c) {
                self.bytes.push(c);
                continue;
            }
            if c == b'"' {
                return Ok(self.bytes.len() - chars_delta);
            }
            if c & 0x80 != 0 {
                self.bytes.push(c);
                let c1 = self.get()?;
                if c1 & 0xC0 != 0x80 {
                    return Err(self.error("json-utf8-decode-error"));
                }
                self.bytes.push(c1);
                if c <= 0xC1 {
                    return Err(self.error("json-utf8-decode-error"));
                } else if c <= 0xDF {
                    chars_delta += 1;
                } else if c <= 0xEF {
                    let c2 = self.get()?;
                    if c2 & 0xC0 != 0x80 {
                        return Err(self.error("json-utf8-decode-error"));
                    }
                    let v = (u32::from(c & 0x0F) << 12)
                        + (u32::from(c1 & 0x3F) << 6)
                        + u32::from(c2 & 0x3F);
                    if v < 0x800 || (0xD800..=0xDFFF).contains(&v) {
                        return Err(self.error("json-utf8-decode-error"));
                    }
                    self.bytes.push(c2);
                    chars_delta += 2;
                } else if c <= 0xF7 {
                    let c2 = self.get()?;
                    let c3 = self.get()?;
                    if c2 & 0xC0 != 0x80 || c3 & 0xC0 != 0x80 {
                        return Err(self.error("json-utf8-decode-error"));
                    }
                    let v = (u32::from(c & 0x07) << 18)
                        + (u32::from(c1 & 0x3F) << 12)
                        + (u32::from(c2 & 0x3F) << 6)
                        + u32::from(c3 & 0x3F);
                    if !(0x10000..=0x10FFFF).contains(&v) {
                        return Err(self.error("json-utf8-decode-error"));
                    }
                    self.bytes.extend_from_slice(&[c2, c3]);
                    chars_delta += 3;
                } else {
                    return Err(self.error("json-utf8-decode-error"));
                }
            } else if c == b'\\' {
                let c = self.get()?;
                self.current_column += 1;
                match c {
                    b'"' | b'\\' | b'/' => self.bytes.push(c),
                    b'b' => self.bytes.push(0x08),
                    b'f' => self.bytes.push(0x0C),
                    b'n' => self.bytes.push(b'\n'),
                    b'r' => self.bytes.push(b'\r'),
                    b't' => self.bytes.push(b'\t'),
                    b'u' => {
                        let mut num = self.parse_unicode()?;
                        if (0xD800..0xDC00).contains(&num) {
                            // The first half of a surrogate pair.
                            self.current_column += 1;
                            if self.get()? != b'\\' {
                                return Err(self.error("json-invalid-surrogate-error"));
                            }
                            self.current_column += 1;
                            if self.get()? != b'u' {
                                return Err(self.error("json-invalid-surrogate-error"));
                            }
                            let num2 = self.parse_unicode()?;
                            if !(0xDC00..0xE000).contains(&num2) {
                                return Err(self.error("json-invalid-surrogate-error"));
                            }
                            num = 0x10000 + ((num - 0xD800) << 10 | (num2 - 0xDC00));
                        } else if (0xDC00..0xE000).contains(&num) {
                            // The second half without the first.
                            return Err(self.error("json-invalid-surrogate-error"));
                        }
                        chars_delta += self.put_utf8(num);
                    }
                    _ => return Err(self.error("json-escape-sequence-error")),
                }
            } else {
                return Err(self.error("json-parse-error"));
            }
        }
    }

    /// json_create_integer: the summed VALUE when nothing overflowed,
    /// else the byte workspace read as string_to_number reads it.
    fn create_integer(&mut self, overflow: bool, negative: bool, value: u64) -> Value {
        if !overflow {
            if !negative {
                return match i64::try_from(value) {
                    Ok(value) => Value::Integer(value),
                    Err(_) => Value::BigInteger(BigInt::from(value).into()),
                };
            }
            if value <= i64::MAX as u64 + 1 {
                return Value::Integer((value as i64).wrapping_neg());
            }
        }
        let digits = std::str::from_utf8(&self.bytes).expect("ASCII digits");
        let number: BigInt = digits.parse().expect("JSON integer syntax");
        match number.to_i64() {
            Some(value) => Value::Integer(value),
            None => Value::BigInteger(number.into()),
        }
    }

    /// json_create_float: strtod of the byte workspace, out of range
    /// where it overflows.
    fn create_float(&self) -> Result<Value, LispError> {
        let text = std::str::from_utf8(&self.bytes).expect("ASCII number");
        let value: f64 = text.parse().map_err(|_| self.error("json-error"))?;
        if value.is_infinite() {
            return Err(self.error("json-number-out-of-range-error"));
        }
        Ok(Value::float(value))
    }

    /// json_parse_number: the number whose first character C is read.
    fn parse_number(&mut self, mut c: u8) -> Result<Value, LispError> {
        self.bytes.clear();
        self.bytes.push(c);
        let mut negative = false;
        if c == b'-' {
            negative = true;
            c = self.get()?;
            self.bytes.push(c);
            self.current_column += 1;
        }
        if !c.is_ascii_digit() {
            return Err(self.error("json-parse-error"));
        }
        let mut integer = u64::from(c - b'0');
        let mut overflow = false;
        if integer == 0 {
            if self.at_eof() {
                return Ok(Value::Integer(0));
            }
            c = self.get()?;
        } else {
            loop {
                if self.at_eof() {
                    return Ok(self.create_integer(overflow, negative, integer));
                }
                c = self.get()?;
                if !c.is_ascii_digit() {
                    break;
                }
                self.bytes.push(c);
                self.current_column += 1;
                let (times, o1) = integer.overflowing_mul(10);
                let (sum, o2) = times.overflowing_add(u64::from(c - b'0'));
                overflow |= o1 | o2;
                integer = sum;
            }
        }
        let mut is_float = false;
        if c == b'.' {
            self.bytes.push(c);
            self.current_column += 1;
            is_float = true;
            c = self.get()?;
            self.bytes.push(c);
            self.current_column += 1;
            if !c.is_ascii_digit() {
                return Err(self.error("json-parse-error"));
            }
            loop {
                if self.at_eof() {
                    return self.create_float();
                }
                c = self.get()?;
                if !c.is_ascii_digit() {
                    break;
                }
                self.bytes.push(c);
                self.current_column += 1;
            }
        }
        if c == b'e' || c == b'E' {
            self.bytes.push(c);
            self.current_column += 1;
            is_float = true;
            c = self.get()?;
            self.bytes.push(c);
            self.current_column += 1;
            if c == b'-' || c == b'+' {
                c = self.get()?;
                self.bytes.push(c);
                self.current_column += 1;
            }
            if !c.is_ascii_digit() {
                return Err(self.error("json-parse-error"));
            }
            loop {
                if self.at_eof() {
                    return self.create_float();
                }
                c = self.get()?;
                if !c.is_ascii_digit() {
                    break;
                }
                self.bytes.push(c);
                self.current_column += 1;
            }
        }
        // C is not part of the number.
        self.put_back();
        if is_float {
            self.create_float()
        } else {
            Ok(self.create_integer(overflow, negative, integer))
        }
    }

    /// json_parse_array.
    fn parse_array(&mut self) -> Result<Value, LispError> {
        let mut c = self.skip_whitespace()?;
        let first = self.objects.len();
        if c != b']' {
            self.available_depth -= 1;
            if self.available_depth < 0 {
                return Err(self.error("json-object-too-deep"));
            }
            loop {
                let element = self.parse_value(c)?;
                self.objects.push(element);
                c = self.skip_whitespace()?;
                if c == b']' {
                    self.available_depth += 1;
                    break;
                }
                if c != b',' {
                    return Err(self.error("json-parse-error"));
                }
                c = self.skip_whitespace()?;
            }
        }
        let elements = self.objects.drain(first..);
        Ok(match self.options.array_type {
            JsonArrayType::Vector => Value::vector(elements.collect::<Vec<_>>()),
            JsonArrayType::List => Value::list(elements),
        })
    }

    /// json_parse_object_member_value: the ": value" of a member.
    fn parse_member_value(&mut self) -> Result<Value, LispError> {
        if self.skip_whitespace()? != b':' {
            return Err(self.error("json-parse-error"));
        }
        let c = self.skip_whitespace()?;
        self.parse_value(c)
    }

    /// json_parse_object: a hash table's later member with an equal key
    /// sets the earlier one's value (hash_put); an alist and a plist keep
    /// every member.
    fn parse_object(&mut self) -> Result<Value, LispError> {
        let mut c = self.skip_whitespace()?;
        let first = self.objects.len();
        if c != b'}' {
            self.available_depth -= 1;
            if self.available_depth < 0 {
                return Err(self.error("json-object-too-deep"));
            }
            loop {
                if c != b'"' {
                    return Err(self.error("json-parse-error"));
                }
                let key = match self.options.object_type {
                    JsonObjectType::HashTable => {
                        let nchars = self.parse_string(false)?;
                        self.make_string(nchars)
                    }
                    JsonObjectType::Alist => {
                        self.parse_string(false)?;
                        self.intern()
                    }
                    JsonObjectType::Plist => {
                        self.parse_string(true)?;
                        self.intern()
                    }
                };
                self.objects.push(key);
                let value = self.parse_member_value()?;
                self.objects.push(value);
                c = self.skip_whitespace()?;
                if c == b'}' {
                    self.available_depth += 1;
                    break;
                }
                if c != b',' {
                    return Err(self.error("json-parse-error"));
                }
                c = self.skip_whitespace()?;
            }
        }
        let members = &self.objects[first..];
        let result = match self.options.object_type {
            JsonObjectType::HashTable => {
                let test = match self.equal_test {
                    Some(test) => test,
                    None => {
                        let test = crate::lisp::primitives::hash_table_test_descriptor(
                            self.interp,
                            Value::symbol("equal"),
                            &self.env,
                        )
                        .expect("the `equal' test");
                        *self.equal_test.insert(test)
                    }
                };
                // make_hash_table of the members' count, duplicates
                // included.
                let table = crate::lisp::primitives::hash_table_from_test(
                    test,
                    members.len() / 2,
                    0,
                    Value::Nil,
                )
                .expect("an `equal' table");
                let Kind::HashTable(hash) = table.kind() else {
                    unreachable!()
                };
                for pair in members.as_chunks::<2>().0 {
                    assert!(
                        matches!(
                            self.interp
                                .equal_hash_put(hash, pair[0], pair[1], &self.env),
                            Ok(true)
                        ),
                        "an `equal' table takes any key"
                    );
                }
                table
            }
            JsonObjectType::Alist => Value::list(
                members
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| Value::cons(pair[0], pair[1])),
            ),
            JsonObjectType::Plist => Value::list(members.iter().copied()),
        };
        self.objects.truncate(first);
        Ok(result)
    }

    /// json_parse_value: the value whose first character C is read.
    fn parse_value(&mut self, c: u8) -> Result<Value, LispError> {
        match c {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            // json.c returns ordinary mutable multibyte strings, including
            // ASCII strings. LSP clients attach completion metadata to them.
            b'"' => {
                let nchars = self.parse_string(false)?;
                Ok(self.make_string(nchars))
            }
            b'0'..=b'9' | b'-' => self.parse_number(c),
            _ => {
                let c = i32::from(c);
                let c2 = self.get_if_possible();
                let c3 = self.get_if_possible();
                let c4 = self.get_if_possible();
                let c5 = self.get_if_possible();
                let is = |c: i32, b: u8| c == i32::from(b);
                if is(c, b't')
                    && is(c2, b'r')
                    && is(c3, b'u')
                    && is(c4, b'e')
                    && (c5 < 0 || !json_is_token_char(c5))
                {
                    if c5 >= 0 {
                        self.put_back();
                    }
                    self.current_column += 3;
                    return Ok(Value::T);
                }
                if is(c, b'n')
                    && is(c2, b'u')
                    && is(c3, b'l')
                    && is(c4, b'l')
                    && (c5 < 0 || !json_is_token_char(c5))
                {
                    if c5 >= 0 {
                        self.put_back();
                    }
                    self.current_column += 3;
                    return Ok(self.options.null_object);
                }
                if is(c, b'f') && is(c2, b'a') && is(c3, b'l') && is(c4, b's') && is(c5, b'e') {
                    let c6 = self.get_if_possible();
                    if c6 < 0 || !json_is_token_char(c6) {
                        if c6 >= 0 {
                            self.put_back();
                        }
                        self.current_column += 4;
                        return Ok(self.options.false_object);
                    }
                }
                Err(self.error("json-parse-error"))
            }
        }
    }

    /// json_parse: the value, then nothing but whitespace or the point
    /// past the value.
    fn parse(&mut self, end: ParseEnd) -> Result<Value, LispError> {
        let c = self.skip_whitespace()?;
        let value = self.parse_value(c)?;
        if end == ParseEnd::CheckForGarbage && self.skip_whitespace_if_possible() >= 0 {
            return Err(self.error("json-trailing-content"));
        }
        Ok(value)
    }
}

/// Emacs's internal bytes of buffer TEXT, the bytes json.c reads from
/// the buffer: a multibyte buffer's raw byte its two-byte form (C0 or C1
/// then a continuation) and a character beyond Unicode its four- or
/// five-byte form, a unibyte buffer's raw bytes the bytes they stand for;
/// EXTENDED names the characters of TEXT's side table.
fn buffer_internal_bytes<'t>(
    text: &'t str,
    multibyte: bool,
    extended: &[(usize, u32)],
) -> std::borrow::Cow<'t, [u8]> {
    if extended.is_empty() && memchr::memchr(0xEE, text.as_bytes()).is_none() {
        return std::borrow::Cow::Borrowed(text.as_bytes());
    }
    let mut bytes = Vec::with_capacity(text.len());
    let mut extended = extended.iter().peekable();
    for (index, ch) in text.chars().enumerate() {
        let code = match extended.peek() {
            Some(&&(at, code)) if at == index => {
                extended.next();
                code
            }
            _ => match raw_byte_from_regex_char(ch) {
                Some(byte) => 0x3F_FF00 + u32::from(byte),
                None => ch as u32,
            },
        };
        if multibyte {
            let (encoded, width) = crate::lisp::types::string_data::encode_character(code)
                .expect("a buffer holds characters");
            bytes.extend_from_slice(&encoded[..width]);
        } else {
            bytes.push(code.checked_sub(0x3F_FF00).unwrap_or(code) as u8);
        }
    }
    std::borrow::Cow::Owned(bytes)
}

/// Fjson_parse_string of a string's bytes (SDATA).
pub(crate) fn parse_string_bytes(
    interp: &mut Interpreter,
    input: &[u8],
    options: &JsonParseOptions,
) -> Result<Value, LispError> {
    JsonParser::new(input, interp, options).parse(ParseEnd::CheckForGarbage)
}

/// Fjson_parse_buffer of TEXT, the current buffer's from point to Z (its
/// side-table characters EXTENDED): the value and how many positions
/// point moves past it, characters of multibyte text and bytes of
/// unibyte.
pub(crate) fn parse_buffer_text(
    interp: &mut Interpreter,
    text: &str,
    multibyte: bool,
    extended: &[(usize, u32)],
    options: &JsonParseOptions,
) -> Result<(Value, usize), LispError> {
    let input = buffer_internal_bytes(text, multibyte, extended);
    let mut parser = JsonParser::new(&input, interp, options);
    let value = parser.parse(ParseEnd::MovePoint)?;
    let moved = if multibyte {
        (parser.point_of_current_line + parser.current_column) as usize
    } else {
        parser.current
    };
    Ok((value, moved))
}

/// json.c json_serialize: OBJECT's JSON as its UTF-8 bytes.
pub(crate) fn serialize(
    interp: &mut Interpreter,
    value: &Value,
    null_object: &Value,
    false_object: &Value,
) -> Result<JsonSerialized, LispError> {
    let mut out = JsonOut {
        buf: Vec::new(),
        maxdepth: JSON_SERIALIZATION_MAX_DEPTH,
        null_object: *null_object,
        false_object: *false_object,
        interp: &*interp,
        env: crate::lisp::types::Env::new(),
    };
    out.something(value)?;
    Ok(JsonSerialized { bytes: out.buf })
}

pub(crate) fn make_hash_table(
    interp: &mut Interpreter,
    test: &str,
    entries: Vec<(Value, Value)>,
) -> Value {
    make_hash_table_with_capacity(interp, test, entries, 0)
}

pub(crate) fn make_hash_table_with_capacity(
    interp: &mut Interpreter,
    test: &str,
    entries: Vec<(Value, Value)>,
    capacity: usize,
) -> Value {
    let capacity = crate::lisp::eval::gnu_hash_grown_capacity(capacity, entries.len());
    let env = crate::lisp::types::Env::new();
    let value = crate::lisp::primitives::make_hash_table_value(
        interp,
        Value::symbol(test),
        capacity,
        Value::Nil,
        Value::Nil,
        &env,
    )
    .expect("validated internal hash-table test");
    let Kind::HashTable(table) = value.kind() else {
        unreachable!()
    };
    for (key, value) in entries {
        assert!(
            matches!(interp.equal_hash_put(table, key, value, &env), Ok(true)),
            "internal constructor uses a standard test"
        );
    }
    value
}

pub(crate) fn hash_table_entries(
    _interp: &Interpreter,
    value: &Value,
) -> Option<(String, Vec<(Value, Value)>)> {
    let Kind::HashTable(table) = value.kind() else {
        return None;
    };
    Some((
        table.test_name().symbol_str()?.to_owned(),
        table
            .entries()
            .map(|(_, key, value)| (key, value))
            .collect(),
    ))
}

/// json.c json_out_t: the output's bytes, the nesting depth left and
/// the configured null and false objects.
struct JsonOut<'a> {
    buf: Vec<u8>,
    maxdepth: i32,
    null_object: Value,
    false_object: Value,
    interp: &'a Interpreter,
    env: crate::lisp::types::Env,
}

impl JsonOut<'_> {
    fn eq(&self, left: &Value, right: &Value) -> bool {
        crate::lisp::primitives::values_eq_in_env(self.interp, left, right, &self.env)
    }

    /// json_out_nest.
    fn nest(&mut self) -> Result<(), LispError> {
        self.maxdepth -= 1;
        if self.maxdepth < 0 {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::String("Maximum JSON serialization depth exceeded".into()),
            ])));
        }
        Ok(())
    }

    /// json_out_unnest.
    fn unnest(&mut self) {
        self.maxdepth += 1;
    }

    /// json_out_something.
    fn something(&mut self, value: &Value) -> Result<(), LispError> {
        if self.eq(value, &self.null_object) {
            self.buf.extend_from_slice(b"null");
            return Ok(());
        }
        if self.eq(value, &self.false_object) {
            self.buf.extend_from_slice(b"false");
            return Ok(());
        }
        match value.kind() {
            Kind::T => self.buf.extend_from_slice(b"true"),
            Kind::Nil => self.buf.extend_from_slice(b"{}"),
            Kind::Integer(number) => {
                use std::io::Write;
                write!(self.buf, "{number}").expect("a Vec takes any write");
            }
            Kind::StringObject(string) => {
                let string = string.borrow();
                json_out_string(&mut self.buf, string.bytes(), string.is_multibyte(), 0)
                    .map_err(|()| not_json_value(value))?;
            }
            Kind::Cons(_) => self.object_cons(value)?,
            // json_out_float, with `float-output-format' bound to nil
            // around the output: the fewest digits that read back.
            Kind::Float(number) if !number.get().is_finite() => {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("error"),
                    Value::String("JSON does not allow Inf or NaN".into()),
                    *value,
                ])));
            }
            Kind::Float(number) => self
                .buf
                .extend_from_slice(crate::lisp::types::format_float(number.get()).as_bytes()),
            Kind::HashTable(table) => {
                self.nest()?;
                self.buf.push(b'{');
                let mut first = true;
                for (_, key, entry) in table.entries() {
                    self.member(&mut first, &key, &entry)?;
                }
                self.buf.push(b'}');
                self.unnest();
            }
            Kind::ReaderForm(form) => match form.as_ref() {
                ReaderForm::HashTable { fields } => self.object_hash_literal(fields)?,
                _ => return Err(not_json_value(value)),
            },
            Kind::Vector(vector) => {
                self.nest()?;
                self.buf.push(b'[');
                for (index, item) in vector.slots().enumerate() {
                    if index > 0 {
                        self.buf.push(b',');
                    }
                    self.something(&item)?;
                }
                self.buf.push(b']');
                self.unnest();
            }
            Kind::BigInteger(number) => self.buf.extend_from_slice(number.to_string().as_bytes()),
            _ => return Err(not_json_value(value)),
        }
        Ok(())
    }

    /// One member of json_out_object_hash: KEY must be a string
    /// (CHECK_STRING); uniqueness is the caller's business.
    fn member(&mut self, first: &mut bool, key: &Value, value: &Value) -> Result<(), LispError> {
        if !*first {
            self.buf.push(b',');
        }
        *first = false;
        let Kind::StringObject(string) = key.kind() else {
            return Err(LispError::WrongTypeArgument("stringp".into(), *key));
        };
        {
            let string = string.borrow();
            json_out_string(&mut self.buf, string.bytes(), string.is_multibyte(), 0)
                .map_err(|()| not_json_value(key))?;
        }
        self.buf.push(b':');
        self.something(value)
    }

    /// A `#s(hash-table ...)' literal not yet made a table: its data's
    /// pairs written as json_out_object_hash writes a table's.
    fn object_hash_literal(&mut self, fields: &[Value]) -> Result<(), LispError> {
        let mut data = Vec::new();
        for pair in fields.as_chunks::<2>().0 {
            if pair[0].as_symbol()? == "data" {
                data = list_to_flat_pairs(&pair[1])?;
            }
        }
        self.nest()?;
        self.buf.push(b'{');
        let mut first = true;
        for (key, value) in &data {
            self.member(&mut first, key, value)?;
        }
        self.buf.push(b'}');
        self.unnest();
        Ok(())
    }

    /// json_out_object_cons: an alist when the first element is a cons,
    /// otherwise a plist.  Every key must be a symbol, a plist key's
    /// leading `:' is dropped unless it is the whole name while an alist
    /// key keeps it, and a later occurrence of the same symbol (by
    /// identity, the symset) is skipped; an alist element must be a cons
    /// and a plist key must have its value, the list must be proper, and
    /// FOR_EACH_TAIL's cycle check signals `circular-list'.  Each value is
    /// written as its member is reached.
    fn object_cons(&mut self, object: &Value) -> Result<(), LispError> {
        self.nest()?;
        self.buf.push(b'{');
        let is_alist = matches!(object.car().map(|v| v.kind()), Ok(Kind::Cons(_)));
        let mut symset = Symset::new();
        let mut first = true;
        let mut tail = *object;
        let mut tortoise = *object;
        let mut steps = 0usize;
        while let Some((head, rest)) = tail.cons_values() {
            let (key, value, next) = if is_alist {
                let (key, value) = head
                    .cons_values()
                    .ok_or_else(|| LispError::WrongTypeArgument("consp".into(), head))?;
                (key, value, rest)
            } else {
                let (value, next) = rest
                    .cons_values()
                    .ok_or_else(|| LispError::WrongTypeArgument("consp".into(), rest))?;
                (head, value, next)
            };
            let key = match key.kind() {
                Kind::SymbolWithPos(symbol)
                    if crate::lisp::primitives::symbols_with_pos_enabled(
                        self.interp,
                        &self.env,
                    ) =>
                {
                    symbol.symbol()
                }
                _ => key,
            };
            let name = match key.kind() {
                Kind::Nil | Kind::T | Kind::Symbol(_) => key.as_symbol()?,
                _ => return Err(LispError::WrongTypeArgument("symbolp".into(), key)),
            };
            if symset.add(key.word() as u64) {
                if !first {
                    self.buf.push(b',');
                }
                first = false;
                let name = visible_symbol_name(name);
                let skip = usize::from(!is_alist && name.len() > 1 && name.starts_with(':'));
                json_out_string(&mut self.buf, name.as_bytes(), !name.is_ascii(), skip)
                    .map_err(|()| not_json_value(&key))?;
                self.buf.push(b':');
                self.something(&value)?;
            }
            tail = next;
            steps += 1;
            if steps.is_multiple_of(2) {
                tortoise = tortoise.cdr().unwrap_or(Value::Nil);
            }
            if let (Kind::Cons(current), Kind::Cons(lagging)) = (tail.kind(), tortoise.kind())
                && crate::lisp::types::SharedCons::ptr_eq(&current, &lagging)
            {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("circular-list".into()),
                    *object,
                ])));
            }
        }
        if !tail.is_nil() {
            return Err(LispError::WrongTypeArgument("listp".into(), *object));
        }
        self.buf.push(b'}');
        self.unnest();
        Ok(())
    }
}

/// json.c symset_t: the keys an object has written, by identity, in an
/// open-addressed table of 2^BITS slots at most half full.
struct Symset {
    count: usize,
    bits: u32,
    table: Vec<Option<u64>>,
}

impl Symset {
    fn new() -> Self {
        Symset {
            count: 0,
            bits: 4,
            table: vec![None; 1 << 4],
        }
    }

    /// symset_hash: knuth_hash of the word reduced to 32 bits.
    fn hash(word: u64, bits: u32) -> usize {
        let reduced = (word ^ (word >> 32)) as u32;
        (reduced.wrapping_mul(2_654_435_769) >> (32 - bits)) as usize
    }

    /// symset_add: whether WORD was not yet in the set, adding it.
    fn add(&mut self, word: u64) -> bool {
        if self.count >= self.table.len() >> 1 {
            // symset_expand.
            self.bits += 1;
            let old = std::mem::replace(&mut self.table, vec![None; 1 << self.bits]);
            let mask = self.table.len() - 1;
            for entry in old.into_iter().flatten() {
                let mut j = Self::hash(entry, self.bits);
                while self.table[j].is_some() {
                    j = (j + 1) & mask;
                }
                self.table[j] = Some(entry);
            }
        }
        let mask = self.table.len() - 1;
        let mut i = Self::hash(word, self.bits);
        loop {
            match self.table[i] {
                Some(entry) if entry == word => return false,
                Some(_) => i = (i + 1) & mask,
                None => {
                    self.table[i] = Some(word);
                    self.count += 1;
                    return true;
                }
            }
        }
    }
}

fn not_json_value(value: &Value) -> LispError {
    LispError::WrongTypeArgument("json-value-p".into(), *value)
}

/// json.c json_out_string of a string's internal BYTES from SKIP on:
/// printable ASCII but `"' and `\\' as is, the other ASCII characters
/// escaped, and a MULTIBYTE string's Unicode characters copied as UTF-8; a
/// raw byte, a surrogate, a character beyond Unicode or any non-ASCII byte
/// of a unibyte string is an error, `(wrong-type-argument json-value-p
/// STRING)' to the caller.
fn json_out_string(
    out: &mut Vec<u8>,
    bytes: &[u8],
    multibyte: bool,
    skip: usize,
) -> Result<(), ()> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.reserve(bytes.len() + 2);
    out.push(b'"');
    let mut p = skip;
    while p < bytes.len() {
        let c = bytes[p];
        if json_plain_char(c) {
            out.push(c);
            p += 1;
        } else if c > 0x7F {
            if !multibyte {
                return Err(());
            }
            let byte =
                |offset: usize| u32::from(bytes.get(p + offset).copied().unwrap_or(0) & 0x3F);
            let n = if c <= 0xC1 {
                return Err(());
            } else if c <= 0xDF {
                2
            } else if c <= 0xEF {
                let v = (u32::from(c & 0x0F) << 12) + (byte(1) << 6) + byte(2);
                if (0xD800..=0xDFFF).contains(&v) {
                    return Err(());
                }
                3
            } else if c <= 0xF7 {
                let v = (u32::from(c & 0x07) << 18) + (byte(1) << 12) + (byte(2) << 6) + byte(3);
                if v > 0x10_FFFF {
                    return Err(());
                }
                4
            } else {
                return Err(());
            };
            out.extend_from_slice(&bytes[p..(p + n).min(bytes.len())]);
            p += n;
        } else {
            out.push(b'\\');
            match c {
                b'"' | b'\\' => out.push(c),
                0x08 => out.push(b'b'),
                b'\t' => out.push(b't'),
                b'\n' => out.push(b'n'),
                0x0C => out.push(b'f'),
                b'\r' => out.push(b'r'),
                _ => out.extend_from_slice(&[
                    b'u',
                    b'0',
                    b'0',
                    HEX[usize::from(c >> 4)],
                    HEX[usize::from(c & 0xF)],
                ]),
            }
            p += 1;
        }
    }
    out.push(b'"');
    Ok(())
}

fn raw_byte_from_regex_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if (RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&code) {
        Some((code - RAW_BYTE_REGEX_BASE) as u8)
    } else {
        None
    }
}

fn utf8_bytes_to_unibyte_text(bytes: &[u8]) -> String {
    let mut text = String::new();
    for &byte in bytes {
        if byte <= 0x7F {
            text.push(byte as char);
        } else {
            text.push(
                char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
                    .expect("raw byte sentinel must be valid"),
            );
        }
    }
    text
}

fn list_to_flat_pairs(value: &Value) -> Result<Vec<(Value, Value)>, LispError> {
    let items = value.to_vec()?;
    let mut pairs = Vec::new();
    let mut index = 0usize;
    while index + 1 < items.len() {
        pairs.push((items[index], items[index + 1]));
        index += 2;
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsed_strings_are_mutable_multibyte_objects() {
        let mut interp = Interpreter::new();
        let options = JsonParseOptions {
            object_type: JsonObjectType::Plist,
            array_type: JsonArrayType::List,
            null_object: Value::Nil,
            false_object: Value::symbol(":false"),
        };
        let string = parse_string_bytes(&mut interp, b"\"alpha\"", &options).expect("JSON string");
        let alias = string;
        let mut env = crate::lisp::types::Env::new();
        for (name, args) in [
            (
                "put-text-property",
                vec![
                    Value::Integer(0),
                    Value::Integer(1),
                    Value::symbol("item"),
                    Value::T,
                    string,
                ],
            ),
            (
                "aset",
                vec![string, Value::Integer(0), Value::Integer('A' as i64)],
            ),
        ] {
            crate::lisp::primitives::call(&mut interp, name, &args, &mut env)
                .expect("parsed JSON strings should support mutation");
        }
        let text =
            crate::lisp::primitives::string_like(&alias).expect("the alias should remain a string");
        assert_eq!(text.text, "Alpha");
        assert!(text.multibyte);
        assert_eq!(
            crate::lisp::primitives::string_property_at(&alias, 0, "item"),
            Some(Value::T)
        );
    }

    #[test]
    fn serializer_emits_utf8_byte_strings() {
        let mut interp = Interpreter::new();
        let serialized = serialize(
            &mut interp,
            &Value::String("abcα".into()),
            &Value::Symbol(":null".into()),
            &Value::Symbol(":false".into()),
        )
        .expect("serialization should succeed");
        assert_eq!(serialized.text(), "\"abcα\"");
        assert!(
            serialized
                .unibyte_text()
                .chars()
                .any(|ch| raw_byte_from_regex_char(ch).is_some())
        );
    }

    #[test]
    fn serializer_bounds_nested_circular_objects_without_recursing_the_rust_stack() {
        let mut interp = Interpreter::new();
        let cycle = Value::list([Value::symbol("a"), Value::Nil]);
        cycle
            .cdr()
            .expect("cycle second cell")
            .set_car(cycle)
            .expect("close nested alist cycle");
        let error = serialize(
            &mut interp,
            &Value::list([cycle]),
            &Value::Symbol(":null".into()),
            &Value::Symbol(":false".into()),
        )
        .expect_err("nested cycle should hit GNU's serialization-depth limit");
        assert_eq!(error.condition_type(), "error");
    }

    #[test]
    fn serializer_custom_sentinels_use_identity_not_structural_equality() {
        let mut interp = Interpreter::new();
        let value = Value::String("same text".into());
        let distinct_equal_null = Value::String("same text".into());
        let serialized = serialize(
            &mut interp,
            &value,
            &distinct_equal_null,
            &Value::Symbol(":false".into()),
        )
        .expect("distinct equal string is ordinary JSON data");
        assert_eq!(serialized.text(), "\"same text\"");
    }

    #[test]
    fn serializer_uses_the_visible_name_of_uninterned_object_keys() {
        let mut interp = Interpreter::new();
        let uninterned = Value::Symbol("key\u{1f}123".into());
        let serialized = serialize(
            &mut interp,
            &Value::list([Value::cons(uninterned, Value::Integer(1))]),
            &Value::Symbol(":null".into()),
            &Value::Symbol(":false".into()),
        )
        .expect("uninterned symbol should be a JSON object key");
        assert_eq!(serialized.text(), "{\"key\":1}");
    }

    #[test]
    fn native_json_error_symbols_follow_json_c_hierarchy() {
        let interp = Interpreter::new();
        for (name, conditions, message) in [
            (
                "json-error",
                &["json-error", "error"][..],
                "generic JSON error",
            ),
            (
                "json-parse-error",
                &["json-parse-error", "json-error", "error"][..],
                "could not parse JSON stream",
            ),
            (
                "json-end-of-file",
                &[
                    "json-end-of-file",
                    "json-parse-error",
                    "json-error",
                    "error",
                ][..],
                "end of JSON stream",
            ),
            (
                "json-trailing-content",
                &[
                    "json-trailing-content",
                    "json-parse-error",
                    "json-error",
                    "error",
                ][..],
                "trailing content after JSON stream",
            ),
            (
                "json-utf8-decode-error",
                &["json-utf8-decode-error", "json-error", "error"][..],
                "invalid utf-8 encoding",
            ),
        ] {
            assert_eq!(
                interp.get_symbol_property(name, "error-conditions"),
                Some(Value::list(
                    conditions.iter().map(|name| Value::symbol(name))
                ))
            );
            assert_eq!(
                interp.get_symbol_property(name, "error-message"),
                Some(Value::String(message.into()))
            );
        }
    }

    #[test]
    fn parser_reports_the_positions_point_moves() {
        let options = JsonParseOptions {
            object_type: JsonObjectType::HashTable,
            array_type: JsonArrayType::Vector,
            null_object: Value::Symbol(":null".into()),
            false_object: Value::Symbol(":false".into()),
        };
        // Characters of multibyte text, bytes of unibyte text.
        for (text, multibyte, moved) in [
            ("[123] [456]", true, 5),
            ("\"\u{e9}\u{e9}\" 1", true, 4),
            ("\"\u{e0c3}\u{e0a9}\u{e0c3}\u{e0a9}\" 1", false, 6),
        ] {
            let mut interp = Interpreter::new();
            let (_, consumed) = parse_buffer_text(&mut interp, text, multibyte, &[], &options)
                .expect("parsing should succeed");
            assert_eq!(consumed, moved, "{text:?}");
        }
    }

    #[test]
    fn short_invalid_inputs_report_json_parse_error() {
        let cases = [
            "a", "ab", "abc", "abcd", "\0", "\u{0001}", "t", "tr", "tru", "truE", "truee", "n",
            "nu", "nul", "nulL", "nulll", "f", "fa", "fal", "fals", "falsE", "falsee",
        ];
        for case in cases {
            let mut interp = Interpreter::new();
            let error = parse_string_bytes(
                &mut interp,
                case.as_bytes(),
                &JsonParseOptions {
                    object_type: JsonObjectType::HashTable,
                    array_type: JsonArrayType::Vector,
                    null_object: Value::Symbol(":null".into()),
                    false_object: Value::Symbol(":false".into()),
                },
            )
            .expect_err(case);
            assert_eq!(error.condition_type(), "json-parse-error", "case={case:?}");
        }
    }
}
