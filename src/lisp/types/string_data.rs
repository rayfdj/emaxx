//! The bytes of a Lisp string, in character.h's internal encoding.
//!
//! Unibyte characters are their actual octets. Multibyte strings use GNU's
//! one-to-five-byte encoding, including byte8 and non-Unicode characters.
//! Rust text and its non-Unicode side list are transient consumer views;
//! neither is retained beside the authoritative bytes.

use super::{Kind, LispError, StringPropertySpan, Value};

mod concat;

#[derive(Clone, Debug, PartialEq)]
pub struct SharedStringState {
    bytes: Vec<u8>,
    characters: usize,
    multibyte: bool,
    pub props: Vec<StringPropertySpan>,
}

impl SharedStringState {
    /// character.c:Fstring validates all characters before allocating the
    /// payload. Only an entirely ASCII result has one byte per character.
    pub(crate) fn from_characters(characters: &[Value]) -> Result<Self, LispError> {
        let mut nbytes = 0usize;
        for &character in characters {
            let (_, width) = encode_character(character_code(character)?)?;
            nbytes = nbytes.checked_add(width).ok_or_else(string_overflow)?;
        }
        let mut bytes = allocate_bytes(nbytes)?;
        for &character in characters {
            let (encoded, width) = encode_character(character_code(character)?)?;
            bytes.extend_from_slice(&encoded[..width]);
        }
        Ok(Self {
            bytes,
            characters: characters.len(),
            multibyte: nbytes != characters.len(),
            props: Vec::new(),
        })
    }

    /// alloc.c:Fmake_string encodes INIT once, then repeats its actual bytes.
    pub(crate) fn repeated_character(
        code: u32,
        length: usize,
        force_multibyte: bool,
    ) -> Result<Self, LispError> {
        let (encoded, width) = encode_character(code)?;
        let nbytes = length.checked_mul(width).ok_or_else(string_overflow)?;
        let mut bytes = allocate_bytes(nbytes)?;
        if width == 1 {
            bytes.resize(nbytes, encoded[0]);
        } else if nbytes != 0 {
            bytes.extend_from_slice(&encoded[..width]);
            while bytes.len() < nbytes {
                let count = bytes.len().min(nbytes - bytes.len());
                bytes.extend_from_within(..count);
            }
        }
        Ok(Self {
            bytes,
            characters: length,
            multibyte: force_multibyte || width != 1,
            props: Vec::new(),
        })
    }

    pub(crate) fn from_unibyte(bytes: Vec<u8>) -> Self {
        Self {
            characters: bytes.len(),
            bytes,
            multibyte: false,
            props: Vec::new(),
        }
    }

    pub(crate) fn from_storage(
        bytes: Vec<u8>,
        characters: usize,
        multibyte: bool,
    ) -> Result<Self, LispError> {
        let mut count = 0;
        let mut offset = 0;
        while offset < bytes.len() {
            offset += if multibyte {
                decode_character(&bytes[offset..])?.1
            } else {
                1
            };
            count += 1;
        }
        if count != characters {
            return Err(LispError::Signal(
                "String character count does not match its bytes".into(),
            ));
        }
        Ok(Self {
            bytes,
            characters,
            multibyte,
            props: Vec::new(),
        })
    }

    pub(crate) fn new(
        text: String,
        props: Vec<StringPropertySpan>,
        multibyte: bool,
        extended_chars: Vec<(usize, u32)>,
    ) -> Self {
        let bytes = encode_text(&text, multibyte, &extended_chars)
            .expect("string constructor must supply valid Lisp characters");
        Self {
            bytes,
            characters: text.chars().count(),
            multibyte,
            props,
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn len(&self) -> usize {
        self.characters
    }
    pub(crate) fn is_multibyte(&self) -> bool {
        self.multibyte
    }
    pub(crate) fn storage_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// character.c:str_as_unibyte preserves every internal byte except
    /// the two-byte byte8 forms, which become their one-byte equivalent.
    pub(crate) fn as_unibyte_bytes(&self) -> Vec<u8> {
        if !self.multibyte {
            return self.bytes.clone();
        }
        let mut result = Vec::with_capacity(self.bytes.len());
        let mut offset = 0;
        while offset < self.bytes.len() {
            let (code, width) = decode_character(&self.bytes[offset..]).expect("stored character");
            if (0x3fff80..=0x3fffff).contains(&code) {
                result.push((code - 0x3fff00) as u8);
            } else {
                result.extend_from_slice(&self.bytes[offset..offset + width]);
            }
            offset += width;
        }
        result
    }

    pub(crate) fn text_parts(&self) -> (String, Vec<(usize, u32)>) {
        decode_bytes(&self.bytes, self.multibyte)
            .expect("stored string bytes have the internal encoding")
    }

    pub(crate) fn text(&self) -> String {
        self.text_parts().0
    }

    pub(crate) fn clone_without_properties(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            characters: self.characters,
            multibyte: self.multibyte,
            props: Vec::new(),
        }
    }

    /// fns.c:Fsubstring copies the actual byte range and preserves encoding.
    /// The primitive validates both character bounds before reaching here.
    pub(crate) fn substring(&self, from: usize, to: usize, properties: bool) -> Self {
        assert!(from <= to && to <= self.characters);
        let offset = |index| {
            if index == self.characters {
                self.bytes.len()
            } else {
                self.byte_offset(index).expect("validated character bound")
            }
        };
        let bytes = self.bytes[offset(from)..offset(to)].to_vec();
        let props = if properties {
            self.props
                .iter()
                .filter_map(|span| {
                    let start = span.start.max(from);
                    let end = span.end.min(to);
                    (start < end).then(|| StringPropertySpan {
                        start: start - from,
                        end: end - from,
                        // copy_text_properties adds each pair at the head.
                        props: span.props.iter().rev().cloned().collect(),
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        Self {
            bytes,
            characters: to - from,
            multibyte: self.multibyte,
            props,
        }
    }

    pub(crate) fn character_at(&self, index: usize) -> Option<i64> {
        if !self.multibyte {
            return self.bytes.get(index).map(|byte| i64::from(*byte));
        }
        let offset = self.byte_offset(index)?;
        decode_character(&self.bytes[offset..])
            .ok()
            .map(|(code, _)| i64::from(code))
    }

    fn byte_offset(&self, index: usize) -> Option<usize> {
        if index >= self.characters {
            return None;
        }
        if !self.multibyte {
            return Some(index);
        }
        let mut offset = 0;
        for _ in 0..index {
            offset += decode_character(&self.bytes[offset..]).ok()?.1;
        }
        Some(offset)
    }

    /// data.c:Faset. The caller checks the index and character first.
    /// False means a non-ASCII unibyte string cannot be promoted in place.
    pub(crate) fn store_character(&mut self, index: usize, code: u32) -> bool {
        assert!(index < self.characters && code <= 0x3f_ffff);
        if !self.multibyte && code <= 255 {
            self.bytes[index] = code as u8;
            return true;
        }
        let (offset, old_width) = if self.multibyte {
            let offset = self.byte_offset(index).expect("checked string index");
            (
                offset,
                decode_character(&self.bytes[offset..])
                    .expect("stored character")
                    .1,
            )
        } else {
            if !self.bytes.is_ascii() {
                return false;
            }
            self.multibyte = true;
            (index, 1)
        };
        let (encoded, width) = encode_character(code).expect("checked Lisp character");
        if width == old_width {
            self.bytes[offset..offset + width].copy_from_slice(&encoded[..width]);
        } else {
            self.bytes
                .splice(offset..offset + old_width, encoded[..width].iter().copied());
        }
        true
    }

    pub(crate) fn replace_text(
        &mut self,
        text: String,
        multibyte: bool,
        extended: Vec<(usize, u32)>,
    ) {
        self.bytes = encode_text(&text, multibyte, &extended)
            .expect("string replacement must supply valid Lisp characters");
        self.characters = text.chars().count();
        self.multibyte = multibyte;
    }

    /// fns.c:Fclear_string clears actual storage bytes and makes it unibyte.
    pub(crate) fn clear(&mut self) {
        self.bytes.fill(0);
        self.characters = self.bytes.len();
        self.multibyte = false;
    }
}

pub(crate) fn character_code(value: Value) -> Result<u32, LispError> {
    match value.kind() {
        Kind::Integer(code) if (0..=0x3f_ffff).contains(&code) => Ok(code as u32),
        _ => Err(LispError::WrongTypeArgument("characterp".into(), value)),
    }
}

fn string_overflow() -> LispError {
    LispError::Signal("Maximum string size exceeded".into())
}

fn allocate_bytes(length: usize) -> Result<Vec<u8>, LispError> {
    if length > isize::MAX as usize {
        return Err(string_overflow());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| LispError::Signal("Memory exhausted".into()))?;
    Ok(bytes)
}

pub(crate) fn encode_text(
    text: &str,
    multibyte: bool,
    extended: &[(usize, u32)],
) -> Result<Vec<u8>, LispError> {
    let mut bytes = Vec::with_capacity(text.len());
    for (index, ch) in text.chars().enumerate() {
        let code = extended
            .binary_search_by_key(&index, |(position, _)| *position)
            .ok()
            .map(|found| extended[found].1)
            .unwrap_or_else(|| {
                crate::lisp::primitives::string_character_code(multibyte, ch) as u32
            });
        if multibyte {
            let (encoded, width) = encode_character(code)?;
            bytes.extend_from_slice(&encoded[..width]);
        } else if code <= 255 {
            bytes.push(code as u8);
        } else {
            return Err(LispError::Signal(format!(
                "unibyte string holds character {code:#x}, not a byte"
            )));
        }
    }
    Ok(bytes)
}

fn encode_character(code: u32) -> Result<([u8; 5], usize), LispError> {
    let mut bytes = [0; 5];
    let width = match code {
        0..=0x7f => {
            bytes[0] = code as u8;
            1
        }
        0x80..=0x7ff => {
            bytes[0] = 0xc0 | (code >> 6) as u8;
            bytes[1] = 0x80 | (code & 63) as u8;
            2
        }
        0x800..=0xffff => {
            bytes[0] = 0xe0 | (code >> 12) as u8;
            bytes[1] = 0x80 | ((code >> 6) & 63) as u8;
            bytes[2] = 0x80 | (code & 63) as u8;
            3
        }
        0x10000..=0x1fffff => {
            bytes[0] = 0xf0 | (code >> 18) as u8;
            bytes[1] = 0x80 | ((code >> 12) & 63) as u8;
            bytes[2] = 0x80 | ((code >> 6) & 63) as u8;
            bytes[3] = 0x80 | (code & 63) as u8;
            4
        }
        0x200000..=0x3fff7f => {
            bytes[0] = 0xf8;
            bytes[1] = 0x80 | ((code >> 18) & 15) as u8;
            bytes[2] = 0x80 | ((code >> 12) & 63) as u8;
            bytes[3] = 0x80 | ((code >> 6) & 63) as u8;
            bytes[4] = 0x80 | (code & 63) as u8;
            5
        }
        0x3fff80..=0x3fffff => {
            let raw = code - 0x3fff80;
            bytes[0] = 0xc0 | (raw >> 6) as u8;
            bytes[1] = 0x80 | (raw & 63) as u8;
            2
        }
        _ => return Err(LispError::Signal("Invalid character".into())),
    };
    Ok((bytes, width))
}

fn decode_character(bytes: &[u8]) -> Result<(u32, usize), LispError> {
    let Some(&lead) = bytes.first() else {
        return Err(LispError::Signal("Truncated string character".into()));
    };
    let (width, mut code) = match lead {
        0..=0x7f => (1, u32::from(lead)),
        0xc0..=0xdf => (2, u32::from(lead & 31)),
        0xe0..=0xef => (3, u32::from(lead & 15)),
        0xf0..=0xf7 => (4, u32::from(lead & 7)),
        0xf8 => (5, 0),
        _ => {
            return Err(LispError::Signal(format!(
                "Invalid internal multibyte lead {lead:#x}"
            )));
        }
    };
    let tail = bytes
        .get(1..width)
        .ok_or_else(|| LispError::Signal("Truncated string character".into()))?;
    for &byte in tail {
        if byte & 0xc0 != 0x80 {
            return Err(LispError::Signal(
                "Invalid internal multibyte continuation".into(),
            ));
        }
        code = (code << 6) | u32::from(byte & 63);
    }
    if width == 2 && lead <= 0xc1 {
        code += 0x3fff80;
    }
    if code > 0x3fffff {
        return Err(LispError::Signal("Invalid internal character".into()));
    }
    Ok((code, width))
}

pub(crate) fn decode_bytes(
    bytes: &[u8],
    multibyte: bool,
) -> Result<(String, Vec<(usize, u32)>), LispError> {
    let mut text = String::with_capacity(bytes.len());
    let mut extended = Vec::new();
    let mut offset = 0;
    let mut index = 0;
    while offset < bytes.len() {
        let (code, width) = if multibyte {
            decode_character(&bytes[offset..])?
        } else {
            (u32::from(bytes[offset]), 1)
        };
        offset += width;
        if !multibyte && code >= 128 {
            text.push(crate::lisp::primitives::raw_byte_regex_char(code as u8));
        } else if (0x3fff80..=0x3fffff).contains(&code) {
            text.push(crate::lisp::primitives::raw_byte_regex_char(
                (code - 0x3fff00) as u8,
            ));
        } else if let Some(ch) = char::from_u32(code) {
            text.push(ch);
            // A transient Rust text view uses this private-use range for
            // byte8. Preserve actual Unicode characters in the same range
            // explicitly so rebuilding the view cannot turn them into bytes.
            if crate::lisp::primitives::raw_byte_from_regex_char(ch).is_some() {
                extended.push((index, code));
            }
        } else {
            text.push(crate::lisp::json::INVALID_UNICODE_SENTINEL);
            extended.push((index, code));
        }
        index += 1;
    }
    Ok((text, extended))
}
