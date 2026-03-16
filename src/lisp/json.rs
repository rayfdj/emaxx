use super::eval::Interpreter;
use super::primitives::{make_shared_string_value_with_multibyte, string_like, vector_items};
use super::types::{LispError, Value};
use num_bigint::BigInt;
use num_traits::ToPrimitive;

pub(crate) const INVALID_UNICODE_SENTINEL: char = '\u{F8FF}';
pub(crate) const CIRCULAR_READ_SYNTAX_SYMBOL: &str = "emaxx--circular-read-syntax";
pub(crate) const HASH_TABLE_LITERAL_SYMBOL: &str = "emaxx--hash-table-literal";
const HASH_TABLE_RECORD_TYPE: &str = "hash-table";
const RAW_BYTE_REGEX_BASE: u32 = 0xE000;

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

#[derive(Clone, Debug)]
pub(crate) struct JsonParseSuccess {
    pub(crate) value: Value,
    pub(crate) consumed_source_pos: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct JsonSerialized {
    pub(crate) text: String,
    pub(crate) bytes_text: String,
    pub(crate) bytes_value: Value,
}

#[derive(Clone, Debug)]
enum JsonNode {
    Null,
    False,
    True,
    Integer(i64),
    BigInteger(BigInt),
    Float(f64),
    String(String),
    Array(Vec<JsonNode>),
    Object(Vec<(String, JsonNode)>),
}

#[derive(Clone, Debug)]
struct ConvertedSource {
    bytes: Vec<u8>,
    byte_positions: Vec<usize>,
    end_position: usize,
}

struct JsonParser<'a> {
    source: &'a ConvertedSource,
    index: usize,
}

impl<'a> JsonParser<'a> {
    fn new(source: &'a ConvertedSource) -> Self {
        Self { source, index: 0 }
    }

    fn parse_value(&mut self) -> Result<JsonNode, LispError> {
        self.skip_whitespace();
        match self.peek() {
            None => Err(json_error(
                "json-end-of-file",
                "Unexpected end of JSON input",
                self.pos(),
            )),
            Some(b'n') => self.parse_literal(b"null", JsonNode::Null),
            Some(b'f') => self.parse_literal(b"false", JsonNode::False),
            Some(b't') => self.parse_literal(b"true", JsonNode::True),
            Some(b'"') => Ok(JsonNode::String(self.parse_string()?)),
            Some(b'[') => self.parse_array(),
            Some(b'{') => self.parse_object(),
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            Some(_) => Err(json_error(
                "json-parse-error",
                "Unexpected character while parsing JSON",
                self.pos(),
            )),
        }
    }

    fn parse_literal(&mut self, expected: &[u8], node: JsonNode) -> Result<JsonNode, LispError> {
        for &byte in expected {
            match self.peek() {
                Some(actual) if actual == byte => {
                    self.index += 1;
                }
                Some(_) | None => {
                    return Err(json_error(
                        "json-parse-error",
                        "Invalid JSON literal",
                        self.pos(),
                    ));
                }
            }
        }
        Ok(node)
    }

    fn parse_number(&mut self) -> Result<JsonNode, LispError> {
        let start = self.index;
        if self.peek() == Some(b'-') {
            self.index += 1;
        }
        match self.peek() {
            Some(b'0') => {
                self.index += 1;
            }
            Some(b'1'..=b'9') => {
                self.index += 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.index += 1;
                }
            }
            _ => {
                return Err(json_error(
                    "json-parse-error",
                    "Invalid JSON number",
                    self.pos(),
                ));
            }
        }

        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.index += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(json_error(
                    "json-parse-error",
                    "Invalid JSON number",
                    self.pos(),
                ));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.index += 1;
            }
        }

        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_float = true;
            self.index += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.index += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(json_error(
                    "json-parse-error",
                    "Invalid JSON number",
                    self.pos(),
                ));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.index += 1;
            }
        }

        let raw = std::str::from_utf8(&self.source.bytes[start..self.index]).map_err(|_| {
            json_error(
                "json-parse-error",
                "Invalid JSON number",
                self.position_for_byte_index(start),
            )
        })?;
        if is_float {
            return raw
                .parse::<f64>()
                .map(JsonNode::Float)
                .map_err(|_| json_error("json-parse-error", "Invalid JSON number", self.pos()));
        }
        let bigint = raw.parse::<BigInt>().map_err(|_| {
            json_error(
                "json-parse-error",
                "Invalid JSON number",
                self.position_for_byte_index(start),
            )
        })?;
        Ok(bigint
            .to_i64()
            .map(JsonNode::Integer)
            .unwrap_or(JsonNode::BigInteger(bigint)))
    }

    fn parse_string(&mut self) -> Result<String, LispError> {
        self.expect_byte(b'"')?;
        let mut result = String::new();
        loop {
            let Some(byte) = self.peek() else {
                return Err(json_error(
                    "json-end-of-file",
                    "Unexpected end of JSON string",
                    self.source.end_position,
                ));
            };
            match byte {
                b'"' => {
                    self.index += 1;
                    return Ok(result);
                }
                b'\\' => {
                    self.index += 1;
                    let escaped = self.parse_escape()?;
                    result.push_str(&escaped);
                }
                0x00..=0x1F => {
                    return Err(json_error(
                        "json-parse-error",
                        "Unescaped control character in JSON string",
                        self.pos(),
                    ));
                }
                0x80..=0xFF => {
                    let ch = self.decode_utf8_char()?;
                    result.push(ch);
                }
                _ => {
                    self.index += 1;
                    result.push(byte as char);
                }
            }
        }
    }

    fn parse_escape(&mut self) -> Result<String, LispError> {
        let Some(byte) = self.peek() else {
            return Err(json_error(
                "json-parse-error",
                "Incomplete JSON escape sequence",
                self.source.end_position,
            ));
        };
        self.index += 1;
        match byte {
            b'"' => Ok("\"".into()),
            b'\\' => Ok("\\".into()),
            b'/' => Ok("/".into()),
            b'b' => Ok("\u{0008}".into()),
            b'f' => Ok("\u{000C}".into()),
            b'n' => Ok("\n".into()),
            b'r' => Ok("\r".into()),
            b't' => Ok("\t".into()),
            b'u' => {
                let first = self.read_hex_u16()?;
                if (0xD800..=0xDBFF).contains(&first) {
                    if self.peek() != Some(b'\\') || self.peek_next() != Some(b'u') {
                        return Err(json_error(
                            "json-utf8-decode-error",
                            "Invalid UTF-16 surrogate pair in JSON string",
                            self.pos(),
                        ));
                    }
                    self.index += 2;
                    let second = self.read_hex_u16()?;
                    if !(0xDC00..=0xDFFF).contains(&second) {
                        return Err(json_error(
                            "json-utf8-decode-error",
                            "Invalid UTF-16 surrogate pair in JSON string",
                            self.pos(),
                        ));
                    }
                    let code =
                        0x1_0000 + (((first as u32 - 0xD800) << 10) | (second as u32 - 0xDC00));
                    let ch = char::from_u32(code).ok_or_else(|| {
                        json_error(
                            "json-utf8-decode-error",
                            "Invalid Unicode scalar value in JSON string",
                            self.pos(),
                        )
                    })?;
                    Ok(ch.to_string())
                } else if (0xDC00..=0xDFFF).contains(&first) {
                    Err(json_error(
                        "json-utf8-decode-error",
                        "Invalid UTF-16 surrogate pair in JSON string",
                        self.pos(),
                    ))
                } else {
                    let ch = char::from_u32(first as u32).ok_or_else(|| {
                        json_error(
                            "json-utf8-decode-error",
                            "Invalid Unicode scalar value in JSON string",
                            self.pos(),
                        )
                    })?;
                    Ok(ch.to_string())
                }
            }
            _ => Err(json_error(
                "json-parse-error",
                "Invalid JSON escape sequence",
                self.pos(),
            )),
        }
    }

    fn parse_array(&mut self) -> Result<JsonNode, LispError> {
        self.expect_byte(b'[')?;
        self.skip_whitespace();
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.index += 1;
            return Ok(JsonNode::Array(items));
        }
        loop {
            items.push(self.parse_value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.index += 1;
                }
                Some(b']') => {
                    self.index += 1;
                    return Ok(JsonNode::Array(items));
                }
                None => {
                    return Err(json_error(
                        "json-end-of-file",
                        "Unexpected end of JSON array",
                        self.source.end_position,
                    ));
                }
                _ => {
                    return Err(json_error(
                        "json-parse-error",
                        "Expected ',' or ']' while parsing JSON array",
                        self.pos(),
                    ));
                }
            }
        }
    }

    fn parse_object(&mut self) -> Result<JsonNode, LispError> {
        self.expect_byte(b'{')?;
        self.skip_whitespace();
        let mut entries = Vec::new();
        if self.peek() == Some(b'}') {
            self.index += 1;
            return Ok(JsonNode::Object(entries));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(json_error(
                    "json-parse-error",
                    "Expected string key while parsing JSON object",
                    self.pos(),
                ));
            }
            let key = self.parse_string()?;
            self.skip_whitespace();
            if self.peek() != Some(b':') {
                return Err(json_error(
                    "json-parse-error",
                    "Expected ':' while parsing JSON object",
                    self.pos(),
                ));
            }
            self.index += 1;
            let value = self.parse_value()?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.index += 1;
                }
                Some(b'}') => {
                    self.index += 1;
                    return Ok(JsonNode::Object(entries));
                }
                None => {
                    return Err(json_error(
                        "json-end-of-file",
                        "Unexpected end of JSON object",
                        self.source.end_position,
                    ));
                }
                _ => {
                    return Err(json_error(
                        "json-parse-error",
                        "Expected ',' or '}' while parsing JSON object",
                        self.pos(),
                    ));
                }
            }
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), LispError> {
        match self.peek() {
            Some(actual) if actual == expected => {
                self.index += 1;
                Ok(())
            }
            Some(_) | None => Err(json_error(
                "json-parse-error",
                "Unexpected character while parsing JSON",
                self.pos(),
            )),
        }
    }

    fn read_hex_u16(&mut self) -> Result<u16, LispError> {
        let mut value = 0u16;
        for _ in 0..4 {
            let Some(byte) = self.peek() else {
                return Err(json_error(
                    "json-parse-error",
                    "Incomplete JSON unicode escape",
                    self.source.end_position,
                ));
            };
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => {
                    return Err(json_error(
                        "json-parse-error",
                        "Incomplete JSON unicode escape",
                        self.pos(),
                    ));
                }
            };
            value = value * 16 + digit as u16;
            self.index += 1;
        }
        Ok(value)
    }

    fn decode_utf8_char(&mut self) -> Result<char, LispError> {
        let start = self.index;
        let first = self.source.bytes[start];
        let (len, min_codepoint) = match first {
            0xC2..=0xDF => (2usize, 0x80u32),
            0xE0..=0xEF => (3usize, 0x800u32),
            0xF0..=0xF4 => (4usize, 0x1_0000u32),
            _ => {
                return Err(json_error(
                    "json-utf8-decode-error",
                    "Invalid UTF-8 in JSON input",
                    self.pos(),
                ));
            }
        };
        if start + len > self.source.bytes.len() {
            return Err(json_error(
                "json-utf8-decode-error",
                "Invalid UTF-8 in JSON input",
                self.pos(),
            ));
        }
        let mut code = (first & (0x7F >> len)) as u32;
        for offset in 1..len {
            let byte = self.source.bytes[start + offset];
            if byte & 0xC0 != 0x80 {
                return Err(json_error(
                    "json-utf8-decode-error",
                    "Invalid UTF-8 in JSON input",
                    self.position_for_byte_index(start + offset),
                ));
            }
            code = (code << 6) | (byte & 0x3F) as u32;
        }
        if code < min_codepoint || code > 0x10_FFFF || (0xD800..=0xDFFF).contains(&code) {
            return Err(json_error(
                "json-utf8-decode-error",
                "Invalid UTF-8 in JSON input",
                self.position_for_byte_index(start),
            ));
        }
        let ch = char::from_u32(code).ok_or_else(|| {
            json_error(
                "json-utf8-decode-error",
                "Invalid UTF-8 in JSON input",
                self.position_for_byte_index(start),
            )
        })?;
        self.index += len;
        Ok(ch)
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.index += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.source.bytes.get(self.index).copied()
    }

    fn peek_next(&self) -> Option<u8> {
        self.source.bytes.get(self.index + 1).copied()
    }

    fn pos(&self) -> usize {
        self.position_for_byte_index(self.index)
    }

    fn position_for_byte_index(&self, index: usize) -> usize {
        self.source
            .byte_positions
            .get(index)
            .copied()
            .unwrap_or(self.source.end_position)
    }
}

pub(crate) fn parse_value_source(
    interp: &mut Interpreter,
    value: &Value,
    options: &JsonParseOptions,
    require_eof: bool,
) -> Result<JsonParseSuccess, LispError> {
    let source = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    parse_text_source(interp, &source.text, source.multibyte, options, require_eof)
}

pub(crate) fn parse_text_source(
    interp: &mut Interpreter,
    text: &str,
    multibyte: bool,
    options: &JsonParseOptions,
    require_eof: bool,
) -> Result<JsonParseSuccess, LispError> {
    let source = convert_source(text, multibyte)?;
    let mut parser = JsonParser::new(&source);
    let node = parser.parse_value()?;
    let consumed = parser.position_for_byte_index(parser.index);
    if require_eof {
        let before_skip = parser.index;
        parser.skip_whitespace();
        if parser.peek().is_some() {
            let condition = if before_skip == parser.index
                && matches!(
                    parser.peek(),
                    Some(b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'_')
                ) {
                "json-parse-error"
            } else {
                "json-trailing-content"
            };
            return Err(json_error(
                condition,
                "Trailing content after JSON value",
                parser.pos(),
            ));
        }
    }
    Ok(JsonParseSuccess {
        value: node_to_lisp(interp, node, options),
        consumed_source_pos: consumed,
    })
}

pub(crate) fn serialize(
    interp: &mut Interpreter,
    value: &Value,
    null_object: &Value,
    false_object: &Value,
) -> Result<JsonSerialized, LispError> {
    let options = SerializeOptions {
        null_object,
        false_object,
    };
    let text = serialize_value(interp, value, &options)?;
    let bytes_text = utf8_bytes_to_unibyte_text(text.as_bytes());
    let bytes_value = if bytes_text == text {
        Value::String(bytes_text.clone())
    } else {
        make_shared_string_value_with_multibyte(bytes_text.clone(), Vec::new(), false)
    };
    Ok(JsonSerialized {
        text,
        bytes_text,
        bytes_value,
    })
}

pub(crate) fn is_hash_table(interp: &Interpreter, value: &Value) -> bool {
    match value {
        Value::Record(id) => interp
            .find_record(*id)
            .is_some_and(|record| record.type_name == HASH_TABLE_RECORD_TYPE),
        _ => false,
    }
}

pub(crate) fn make_hash_table(
    interp: &mut Interpreter,
    test: &str,
    entries: Vec<(Value, Value)>,
) -> Value {
    interp.create_record(
        HASH_TABLE_RECORD_TYPE,
        vec![Value::Symbol(test.to_string()), entries_to_list(entries)],
    )
}

pub(crate) fn hash_table_entries(
    interp: &Interpreter,
    value: &Value,
) -> Option<(String, Vec<(Value, Value)>)> {
    let Value::Record(id) = value else {
        return None;
    };
    let record = interp.find_record(*id)?;
    if record.type_name != HASH_TABLE_RECORD_TYPE {
        return None;
    }
    let test = record
        .slots
        .first()
        .and_then(|value| value.as_symbol().ok())
        .unwrap_or("eql")
        .to_string();
    let entries = record
        .slots
        .get(1)
        .map(list_to_entries)
        .transpose()
        .ok()?
        .unwrap_or_default();
    Some((test, entries))
}

pub(crate) fn hash_table_put(
    interp: &mut Interpreter,
    table: &Value,
    key: Value,
    value: Value,
) -> Result<Value, LispError> {
    let Value::Record(id) = table else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    let Some((test, mut entries)) = hash_table_entries(interp, table) else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    if let Some((_, existing_value)) = entries
        .iter_mut()
        .find(|(existing_key, _)| hash_key_equal(interp, &test, existing_key, &key))
    {
        *existing_value = value.clone();
    } else {
        entries.push((key, value.clone()));
    }
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::TypeError("hash-table".into(), table.type_name()));
    };
    if record.slots.len() < 2 {
        record.slots.resize(2, Value::Nil);
    }
    record.slots[1] = entries_to_list(entries);
    Ok(value)
}

fn convert_source(text: &str, multibyte: bool) -> Result<ConvertedSource, LispError> {
    let mut bytes = Vec::new();
    let mut byte_positions = Vec::new();
    let mut source_pos = 1usize;
    for ch in text.chars() {
        if ch == INVALID_UNICODE_SENTINEL {
            return Err(json_error(
                "json-utf8-decode-error",
                "Invalid Unicode scalar value in JSON input",
                source_pos,
            ));
        }
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            if multibyte {
                return Err(json_error(
                    "json-utf8-decode-error",
                    "Invalid raw byte in multibyte JSON input",
                    source_pos,
                ));
            }
            bytes.push(byte);
            byte_positions.push(source_pos);
            source_pos += 1;
        } else {
            let mut buf = [0u8; 4];
            let encoded = ch.encode_utf8(&mut buf);
            for (offset, byte) in encoded.bytes().enumerate() {
                bytes.push(byte);
                byte_positions.push(if multibyte {
                    source_pos
                } else {
                    source_pos + offset
                });
            }
            source_pos += if multibyte { 1 } else { encoded.len() };
            continue;
        }
    }
    Ok(ConvertedSource {
        bytes,
        byte_positions,
        end_position: source_pos,
    })
}

fn node_to_lisp(interp: &mut Interpreter, node: JsonNode, options: &JsonParseOptions) -> Value {
    match node {
        JsonNode::Null => options.null_object.clone(),
        JsonNode::False => options.false_object.clone(),
        JsonNode::True => Value::T,
        JsonNode::Integer(value) => Value::Integer(value),
        JsonNode::BigInteger(value) => Value::BigInteger(value),
        JsonNode::Float(value) => Value::Float(value),
        JsonNode::String(value) => Value::String(value),
        JsonNode::Array(items) => {
            let items: Vec<Value> = items
                .into_iter()
                .map(|item| node_to_lisp(interp, item, options))
                .collect();
            match options.array_type {
                JsonArrayType::Vector => Value::list(
                    std::iter::once(Value::Symbol("vector-literal".into())).chain(items),
                ),
                JsonArrayType::List => Value::list(items),
            }
        }
        JsonNode::Object(entries) => match options.object_type {
            JsonObjectType::HashTable => {
                let mut deduped = Vec::new();
                for (key, value) in entries {
                    let value = node_to_lisp(interp, value, options);
                    if let Some((_, existing)) = deduped.iter_mut().find(|(name, _)| *name == key) {
                        *existing = value;
                    } else {
                        deduped.push((key, value));
                    }
                }
                make_hash_table(
                    interp,
                    "equal",
                    deduped
                        .into_iter()
                        .map(|(key, value)| (Value::String(key), value))
                        .collect(),
                )
            }
            JsonObjectType::Alist => Value::list(entries.into_iter().map(|(key, value)| {
                Value::cons(Value::Symbol(key), node_to_lisp(interp, value, options))
            })),
            JsonObjectType::Plist => {
                let mut items = Vec::new();
                for (key, value) in entries {
                    items.push(Value::Symbol(format!(":{key}")));
                    items.push(node_to_lisp(interp, value, options));
                }
                Value::list(items)
            }
        },
    }
}

struct SerializeOptions<'a> {
    null_object: &'a Value,
    false_object: &'a Value,
}

fn serialize_value(
    interp: &mut Interpreter,
    value: &Value,
    options: &SerializeOptions<'_>,
) -> Result<String, LispError> {
    if value == options.null_object {
        return Ok("null".into());
    }
    if value == options.false_object {
        return Ok("false".into());
    }
    match value {
        Value::Nil => Ok("{}".into()),
        Value::T => Ok("true".into()),
        Value::Integer(number) => Ok(number.to_string()),
        Value::BigInteger(number) => Ok(number.to_string()),
        Value::Float(number) => Ok(number.to_string()),
        Value::Symbol(symbol) if symbol == CIRCULAR_READ_SYNTAX_SYMBOL => Err(json_error(
            "circular-list",
            "Circular list is not serializable as JSON",
            1,
        )),
        Value::Record(_) if is_hash_table(interp, value) => {
            serialize_hash_table(interp, value, options)
        }
        Value::Cons(_, _) => {
            if let Some(rendered) = serialize_hash_table_literal(interp, value, options)? {
                return Ok(rendered);
            }
            if let Ok(items) = vector_items(value)
                && matches!(value.to_vec().ok().and_then(|v| v.first().cloned()), Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal")
            {
                return serialize_array(interp, &items, options);
            }
            if string_like(value).is_some() {
                return serialize_string(value);
            }
            serialize_list_object(interp, value, options)
        }
        _ if string_like(value).is_some() => serialize_string(value),
        _ => Err(LispError::TypeError("json-value".into(), value.type_name())),
    }
}

fn serialize_array(
    interp: &mut Interpreter,
    items: &[Value],
    options: &SerializeOptions<'_>,
) -> Result<String, LispError> {
    let mut rendered = Vec::with_capacity(items.len());
    for item in items {
        rendered.push(serialize_value(interp, item, options)?);
    }
    Ok(format!("[{}]", rendered.join(",")))
}

fn serialize_string(value: &Value) -> Result<String, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    let mut rendered = String::from("\"");
    for ch in string.text.chars() {
        if ch == INVALID_UNICODE_SENTINEL || raw_byte_from_regex_char(ch).is_some() {
            return Err(LispError::TypeError(
                "json-string".into(),
                value.type_name(),
            ));
        }
        match ch {
            '"' => rendered.push_str("\\\""),
            '\\' => rendered.push_str("\\\\"),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            '\u{0008}' => rendered.push_str("\\b"),
            '\u{000C}' => rendered.push_str("\\f"),
            ch if (ch as u32) < 0x20 => {
                rendered.push_str(&format!("\\u{:04X}", ch as u32));
            }
            ch => rendered.push(ch),
        }
    }
    rendered.push('"');
    Ok(rendered)
}

fn serialize_hash_table(
    interp: &mut Interpreter,
    value: &Value,
    options: &SerializeOptions<'_>,
) -> Result<String, LispError> {
    let Some((_, entries)) = hash_table_entries(interp, value) else {
        return Err(LispError::TypeError("hash-table".into(), value.type_name()));
    };
    serialize_object_entries(
        interp,
        entries
            .iter()
            .map(|(key, value)| Ok((hash_table_key_string(key)?, value)))
            .collect::<Result<Vec<_>, LispError>>()?,
        options,
        false,
    )
}

fn serialize_hash_table_literal(
    interp: &mut Interpreter,
    value: &Value,
    options: &SerializeOptions<'_>,
) -> Result<Option<String>, LispError> {
    let items = value.to_vec().ok();
    let Some(items) = items else {
        return Ok(None);
    };
    if !matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == HASH_TABLE_LITERAL_SYMBOL)
    {
        return Ok(None);
    }
    let mut test = "eql".to_string();
    let mut data = Vec::new();
    let mut index = 1usize;
    while index + 1 < items.len() {
        let key = items[index].as_symbol()?.to_string();
        let value = items[index + 1].clone();
        match key.as_str() {
            "test" => test = value.as_symbol()?.to_string(),
            "data" => data = list_to_flat_pairs(&value)?,
            _ => {}
        }
        index += 2;
    }
    if test.is_empty() {
        test = "eql".into();
    }
    let _ = test;
    serialize_object_entries(
        interp,
        data.iter()
            .map(|(key, value)| Ok((hash_table_key_string(key)?, value)))
            .collect::<Result<Vec<_>, LispError>>()?,
        options,
        false,
    )
    .map(Some)
}

fn serialize_list_object(
    interp: &mut Interpreter,
    value: &Value,
    options: &SerializeOptions<'_>,
) -> Result<String, LispError> {
    let items = value.to_vec()?;
    if items.is_empty() {
        return Ok("{}".into());
    }
    if items.iter().all(is_alist_entry) {
        let mut entries = Vec::new();
        for entry in items {
            let (key, entry_value) = alist_entry_parts(&entry)?;
            let name = object_key_string(&key)?;
            entries.push((name, entry_value));
        }
        return serialize_object_entries(
            interp,
            entries
                .iter()
                .map(|(name, value)| (name.clone(), value))
                .collect(),
            options,
            true,
        );
    }
    if items.len().is_multiple_of(2)
        && items
            .iter()
            .step_by(2)
            .all(|item| matches!(item, Value::Symbol(_)))
    {
        let mut entries = Vec::new();
        let mut index = 0usize;
        while index + 1 < items.len() {
            entries.push((object_key_string(&items[index])?, items[index + 1].clone()));
            index += 2;
        }
        return serialize_object_entries(
            interp,
            entries
                .iter()
                .map(|(name, value)| (name.clone(), value))
                .collect(),
            options,
            true,
        );
    }
    Err(LispError::TypeError(
        "json-object".into(),
        value.type_name(),
    ))
}

fn serialize_object_entries(
    interp: &mut Interpreter,
    entries: Vec<(String, &Value)>,
    options: &SerializeOptions<'_>,
    skip_duplicates: bool,
) -> Result<String, LispError> {
    let mut seen = Vec::new();
    let mut rendered = Vec::new();
    for (key, value) in entries {
        if skip_duplicates && seen.iter().any(|existing| existing == &key) {
            continue;
        }
        seen.push(key.clone());
        rendered.push(format!(
            "{}:{}",
            serialize_string(&Value::String(key))?,
            serialize_value(interp, value, options)?
        ));
    }
    Ok(format!("{{{}}}", rendered.join(",")))
}

fn is_alist_entry(value: &Value) -> bool {
    matches!(value.cons_values(), Some((Value::Symbol(_), _)))
}

fn alist_entry_parts(entry: &Value) -> Result<(Value, Value), LispError> {
    entry
        .cons_values()
        .ok_or_else(|| LispError::TypeError("cons".into(), entry.type_name()))
}

fn object_key_string(value: &Value) -> Result<String, LispError> {
    let symbol = value.as_symbol()?;
    Ok(symbol.trim_start_matches(':').to_string())
}

fn hash_table_key_string(value: &Value) -> Result<String, LispError> {
    let string = string_like(value)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))?;
    if string
        .text
        .chars()
        .any(|ch| ch == INVALID_UNICODE_SENTINEL || raw_byte_from_regex_char(ch).is_some())
    {
        return Err(LispError::TypeError("string".into(), value.type_name()));
    }
    Ok(string.text)
}

fn raw_byte_from_regex_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&code) {
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

fn entries_to_list(entries: Vec<(Value, Value)>) -> Value {
    Value::list(
        entries
            .into_iter()
            .map(|(key, value)| Value::cons(key, value)),
    )
}

fn list_to_entries(value: &Value) -> Result<Vec<(Value, Value)>, LispError> {
    value
        .to_vec()?
        .into_iter()
        .map(|entry| {
            entry
                .cons_values()
                .ok_or_else(|| LispError::TypeError("cons".into(), entry.type_name()))
        })
        .collect()
}

fn list_to_flat_pairs(value: &Value) -> Result<Vec<(Value, Value)>, LispError> {
    let items = value.to_vec()?;
    let mut pairs = Vec::new();
    let mut index = 0usize;
    while index + 1 < items.len() {
        pairs.push((items[index].clone(), items[index + 1].clone()));
        index += 2;
    }
    Ok(pairs)
}

fn hash_key_equal(interp: &Interpreter, test: &str, left: &Value, right: &Value) -> bool {
    match test {
        "equal" => super::primitives::values_equal(interp, left, right),
        _ => left == right,
    }
}

fn json_error(condition: &str, message: &str, position: usize) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol(condition.into()),
        Value::String(message.into()),
        Value::Nil,
        Value::Integer(position as i64),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(serialized.text, "\"abcα\"");
        assert!(
            serialized
                .bytes_text
                .chars()
                .any(|ch| raw_byte_from_regex_char(ch).is_some())
        );
    }

    #[test]
    fn parser_reports_consumed_source_positions() {
        let mut interp = Interpreter::new();
        let parsed = parse_text_source(
            &mut interp,
            "[123] [456]",
            true,
            &JsonParseOptions {
                object_type: JsonObjectType::HashTable,
                array_type: JsonArrayType::Vector,
                null_object: Value::Symbol(":null".into()),
                false_object: Value::Symbol(":false".into()),
            },
            false,
        )
        .expect("parsing should succeed");
        assert_eq!(parsed.consumed_source_pos, 6);
    }

    #[test]
    fn short_invalid_inputs_report_json_parse_error() {
        let cases = [
            "a", "ab", "abc", "abcd", "\0", "\u{0001}", "t", "tr", "tru", "truE", "truee", "n",
            "nu", "nul", "nulL", "nulll", "f", "fa", "fal", "fals", "falsE", "falsee",
        ];
        for case in cases {
            let mut interp = Interpreter::new();
            let error = parse_text_source(
                &mut interp,
                case,
                true,
                &JsonParseOptions {
                    object_type: JsonObjectType::HashTable,
                    array_type: JsonArrayType::Vector,
                    null_object: Value::Symbol(":null".into()),
                    false_object: Value::Symbol(":false".into()),
                },
                true,
            )
            .expect_err(case);
            assert_eq!(error.condition_type(), "json-parse-error", "case={case:?}");
        }
    }
}
