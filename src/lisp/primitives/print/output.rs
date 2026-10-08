//! Transient printer output in GNU's internal character encoding.
//!
//! Rust String cannot hold every Emacs character. Keep the emitted bytes
//! authoritative until the destination accepts them, instead of converting
//! strings to a lossy Unicode view before print.c's character decisions.
//! This replaces the printer's temporary host strings, not Lisp storage.

use super::*;
use crate::lisp::types::string_data::{
    decode_bytes, decode_character, encode_character, encode_text,
};

#[derive(Default)]
pub(crate) struct PrintOutput {
    bytes: Vec<u8>,
    // print.c:strout counts unibyte C-string octets as characters. Keep
    // that origin when no multibyte character has joined the output.
    multibyte: bool,
    // strout's C char -> int -> unsigned printchar conversion can pass
    // values outside the Lisp character range to a function stream. These
    // sparse callback arguments are not encoded into Lisp string storage.
    function_codes: Vec<(usize, u32)>,
}

impl PrintOutput {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            multibyte: false,
            function_codes: Vec::new(),
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub(crate) fn push_code(&mut self, code: u32) {
        self.multibyte |= code >= 128;
        let (bytes, count) = encode_character(code).expect("printer emits valid Emacs characters");
        self.bytes.extend_from_slice(&bytes[..count]);
    }

    pub(crate) fn push(&mut self, character: char) {
        self.push_code(character as u32);
    }

    /// Host-generated punctuation, numbers and existing host object names.
    /// Lisp string contents take the code/byte path, never this conversion.
    pub(crate) fn push_str(&mut self, text: &str) {
        if text.is_ascii() {
            self.bytes.extend_from_slice(text.as_bytes());
        } else {
            self.multibyte = true;
            self.bytes
                .extend_from_slice(&encode_text(text, true, &[]).expect("valid host printer text"));
        }
    }

    /// print.c:print_c_string sends each C byte as one unibyte character.
    /// Preserve its octets through the canonical stream, then restore an
    /// unibyte Lisp string if the complete output never became multibyte.
    pub(crate) fn push_c_string(&mut self, text: &str) {
        for byte in text.bytes() {
            let code = if byte < 128 {
                u32::from(byte)
            } else {
                0x3fff00 + u32::from(byte)
            };
            let (bytes, count) = encode_character(code).expect("valid C-string byte");
            self.bytes.extend_from_slice(&bytes[..count]);
        }
    }

    pub(crate) fn push_c_function_byte(&mut self, byte: u8) {
        let argument = byte as std::ffi::c_char as i32 as u32;
        if argument != u32::from(byte) {
            self.function_codes.push((self.bytes.len(), argument));
        }
        self.push_code(u32::from(byte));
    }

    pub(crate) fn append(&mut self, other: &Self) {
        self.multibyte |= other.multibyte;
        self.function_codes.extend(
            other
                .function_codes
                .iter()
                .map(|(offset, code)| (self.bytes.len() + offset, *code)),
        );
        self.bytes.extend_from_slice(other.bytes());
    }

    pub(crate) fn join(parts: &[Self], separator: &str) -> Self {
        let mut output = Self::default();
        for (index, part) in parts.iter().enumerate() {
            if index != 0 {
                output.push_str(separator);
            }
            output.append(part);
        }
        output
    }

    pub(crate) fn enclosed(self, before: &str, after: &str) -> Self {
        let mut result = Self::with_capacity(before.len() + self.bytes.len() + after.len());
        result.push_str(before);
        result.append(&self);
        result.push_str(after);
        result
    }

    pub(crate) fn dotted(parts: &[Self], tail: Self) -> Self {
        let mut output = Self::join(parts, " ").enclosed("(", "");
        output.push_str(" . ");
        output.append(&tail);
        output.push(')');
        output
    }

    pub(crate) fn codes(&self) -> impl Iterator<Item = u32> + '_ {
        let mut remaining = self.bytes();
        let mut function_codes = self.function_codes.iter().peekable();
        std::iter::from_fn(move || {
            if remaining.is_empty() {
                return None;
            }
            let offset = self.bytes.len() - remaining.len();
            let (code, width) = decode_character(remaining).expect("canonical printer output");
            remaining = &remaining[width..];
            Some(
                if function_codes.peek().is_some_and(|(at, _)| *at == offset) {
                    function_codes.next().expect("matching C byte position").1
                } else {
                    code
                },
            )
        })
    }

    /// Existing buffer APIs use a transient Unicode view plus exact positions
    /// for characters that Rust str cannot represent. Keep both together.
    pub(crate) fn text_parts(&self) -> (String, Vec<(usize, u32)>) {
        decode_bytes(self.bytes(), true).expect("canonical printer output")
    }

    /// Legacy host-text consumers remain separate from Lisp printer results.
    /// Their migration is not certified by the canonical stream repair.
    pub(crate) fn host_text(&self) -> String {
        self.text_parts().0
    }

    pub(crate) fn into_value(self, interp: &Interpreter, env: &Env) -> Result<Value, LispError> {
        let characters = self.codes().count();
        let multibyte = self.multibyte;
        let bytes = if multibyte || self.bytes.is_ascii() {
            self.bytes
        } else {
            self.codes()
                .map(|code| {
                    if code >= 0x3fff80 {
                        (code - 0x3fff00) as u8
                    } else {
                        code as u8
                    }
                })
                .collect()
        };
        crate::lisp::types::StringObjectRef::from_storage(bytes, characters, multibyte)
            .map(Value::StringObject)
            .map_err(|error| string_storage_error(interp, env, error))
    }
}

impl From<&str> for PrintOutput {
    fn from(text: &str) -> Self {
        let mut result = Self::with_capacity(text.len());
        result.push_str(text);
        result
    }
}

impl From<String> for PrintOutput {
    fn from(text: String) -> Self {
        if text.is_ascii() {
            Self {
                bytes: text.into_bytes(),
                multibyte: false,
                function_codes: Vec::new(),
            }
        } else {
            Self::from(text.as_str())
        }
    }
}
