//! Transient printer output in GNU's internal character encoding.
//!
//! Rust String cannot hold every Emacs character. Keep the emitted bytes
//! authoritative until the destination accepts them, instead of converting
//! strings to a lossy Unicode view before print.c's character decisions.
//! This replaces the printer's temporary host strings, not Lisp storage.
//!
//! The output also remembers how print.c wrote it: which bytes one strout
//! wrote as a block, which of those blocks are a C string's octets, that
//! strout was called at all, and every other character as a printchar's.
//! stdout is written in those pieces (an fwrite a character, an fwrite a
//! block), and `terpri' asks for the last character printchar wrote.

use super::*;
use crate::lisp::types::string_data::{
    decode_bytes, decode_character, encode_character, encode_text,
};

/// The bytes one call of print.c:strout wrote.
#[derive(Clone, Copy)]
struct Strout {
    start: usize,
    end: usize,
    /// The block is a C string's octets, each counted a character (strout
    /// with `size == size_byte'): a stdio stream is given them as they
    /// were, not as the raw-byte characters the output holds.
    octets: bool,
}

/// A stretch of an output as print.c wrote it.
pub(crate) enum Piece<'a> {
    /// Characters printchar wrote, one call each.
    Chars(&'a [u8]),
    /// One strout's block of text.
    Strout(&'a [u8]),
    /// One strout's block of a C string's octets, held as the characters
    /// of those codes (an octet past 127 as its raw-byte character).
    Octets(&'a [u8]),
}

#[derive(Clone, Default)]
pub(crate) struct PrintOutput {
    bytes: Vec<u8>,
    // print.c:strout counts unibyte C-string octets as characters. Keep
    // that origin when no multibyte character has joined the output.
    multibyte: bool,
    // strout's C char -> int -> unsigned printchar conversion can pass
    // values outside the Lisp character range to a function stream. These
    // sparse callback arguments are not encoded into Lisp string storage.
    function_codes: Vec<(usize, u32)>,
    /// The strouts' blocks, in order; every byte outside them is a
    /// printchar's.  Not kept by a plain output.
    strouts: Vec<Strout>,
    /// strout was called, if only with nothing.
    strout_called: bool,
    /// The output keeps no record of its pieces: print_buffer's text and
    /// what a function is handed, where only the characters count.
    plain: bool,
    /// print_buffer's `pos' where it is not the number of characters the
    /// bytes hold: print_unwind copies a text set aside over the start
    /// of a later print's and leaves that print's `pos', so the first
    /// bytes (how many) count as that many characters whatever they
    /// are now.
    counted: Option<(usize, usize)>,
}

impl PrintOutput {
    /// An output that keeps its text alone, not how it was written.
    pub(crate) fn plain() -> Self {
        Self {
            plain: true,
            ..Self::default()
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// The bytes the output holds (print_buffer's `pos_byte').
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Nothing written, with the room and the kind of output kept.
    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.multibyte = false;
        self.function_codes.clear();
        self.strouts.clear();
        self.strout_called = false;
        self.counted = None;
    }

    /// The bytes the output has room for.
    pub(crate) fn room(&self) -> usize {
        self.bytes.capacity()
    }

    /// Whether a multibyte character joined the output; otherwise each of
    /// its codes is an octet print.c's strout counted as a character.
    pub(crate) fn is_multibyte(&self) -> bool {
        self.multibyte
    }

    /// The octets of an output no multibyte character joined, a raw byte
    /// as its byte.
    pub(crate) fn octets(&self) -> Vec<u8> {
        self.codes()
            .map(|code| {
                if code >= 0x3fff80 {
                    (code - 0x3fff00) as u8
                } else {
                    code as u8
                }
            })
            .collect()
    }

    /// print.c:printchar of the character CODE.
    #[inline]
    pub(crate) fn push_code(&mut self, code: u32) {
        if code < 0x80 {
            self.bytes.push(code as u8);
            return;
        }
        self.multibyte = true;
        let (bytes, count) = encode_character(code).expect("printer emits valid Emacs characters");
        self.bytes.extend_from_slice(&bytes[..count]);
    }

    /// printchar of CHARACTER.
    #[inline]
    pub(crate) fn push(&mut self, character: char) {
        self.push_code(character as u32);
    }

    /// One strout of TEXT: host-generated punctuation, numbers and
    /// existing host object names.  Lisp string contents take the
    /// code/byte path, never this conversion.
    pub(crate) fn push_str(&mut self, text: &str) {
        let start = self.bytes.len();
        if text.is_ascii() {
            self.bytes.extend_from_slice(text.as_bytes());
        } else {
            self.multibyte = true;
            self.bytes
                .extend_from_slice(&encode_text(text, true, &[]).expect("valid host printer text"));
        }
        self.mark_strout(start, false);
    }

    /// One strout of the characters WRITE pushes.
    pub(crate) fn strout_block(&mut self, write: impl FnOnce(&mut Self)) {
        let start = self.bytes.len();
        write(self);
        self.mark_strout(start, false);
    }

    /// ASCII characters, as push_code of each.
    pub(crate) fn push_ascii(&mut self, ascii: &[u8]) {
        debug_assert!(ascii.is_ascii());
        self.bytes.extend_from_slice(ascii);
    }

    /// One strout of ASCII text.
    pub(crate) fn strout_ascii(&mut self, ascii: &[u8]) {
        debug_assert!(ascii.is_ascii());
        let start = self.bytes.len();
        self.bytes.extend_from_slice(ascii);
        self.mark_strout(start, false);
    }

    /// One strout of OCTETS, each counted a character.
    pub(crate) fn push_octets(&mut self, octets: &[u8]) {
        let start = self.bytes.len();
        for &byte in octets {
            let code = if byte < 128 {
                u32::from(byte)
            } else {
                0x3fff00 + u32::from(byte)
            };
            let (bytes, count) = encode_character(code).expect("valid C-string byte");
            self.bytes.extend_from_slice(&bytes[..count]);
        }
        self.mark_strout(start, true);
    }

    /// A C string's byte for a function printed to: strout hands it over
    /// as `int ch = ptr[i]', the `char' it is, made unsigned by printchar.
    pub(crate) fn push_c_function_byte(&mut self, byte: u8) {
        let argument = c_char_argument(byte);
        if argument != u32::from(byte) {
            self.function_codes.push((self.bytes.len(), argument));
        }
        self.push_code(u32::from(byte));
    }

    /// One strout of OTHER's text.
    pub(crate) fn append_strout(&mut self, other: &Self) {
        let start = self.bytes.len();
        self.multibyte |= other.multibyte;
        self.function_codes.extend(
            other
                .function_codes
                .iter()
                .map(|(offset, code)| (start + offset, *code)),
        );
        self.bytes.extend_from_slice(other.bytes());
        self.mark_strout(start, false);
    }

    /// The bytes from START on are one strout's block.
    #[inline]
    fn mark_strout(&mut self, start: usize, octets: bool) {
        self.strout_called = true;
        if self.plain {
            return;
        }
        self.strouts.push(Strout {
            start,
            end: self.bytes.len(),
            octets,
        });
    }

    /// The output as print.c wrote it: each strout's block, and between
    /// them the characters printchar wrote.
    pub(crate) fn pieces(&self) -> impl Iterator<Item = Piece<'_>> {
        let mut at = 0;
        let mut strouts = self.strouts.iter();
        let mut pending: Option<&Strout> = None;
        std::iter::from_fn(move || {
            loop {
                if let Some(strout) = pending.take() {
                    at = strout.end;
                    if strout.end > strout.start {
                        let block = &self.bytes[strout.start..strout.end];
                        return Some(if strout.octets {
                            Piece::Octets(block)
                        } else {
                            Piece::Strout(block)
                        });
                    }
                    continue;
                }
                match strouts.next() {
                    Some(strout) => {
                        pending = Some(strout);
                        if strout.start > at {
                            return Some(Piece::Chars(&self.bytes[at..strout.start]));
                        }
                    }
                    None if at < self.bytes.len() => {
                        let chars = &self.bytes[at..];
                        at = self.bytes.len();
                        return Some(Piece::Chars(chars));
                    }
                    None => return None,
                }
            }
        })
    }

    /// The last character printchar wrote (`printchar_stdout_last' once
    /// the output is on stdout): none when every byte is a strout's.
    pub(crate) fn last_printchar(&self) -> Option<u32> {
        let mut end = self.bytes.len();
        for strout in self.strouts.iter().rev() {
            if strout.end < end {
                break;
            }
            end = strout.start;
        }
        let chars = &self.bytes[..end];
        // The last character's first byte: no trailing byte of a
        // multibyte form.
        let start = chars.iter().rposition(|byte| byte & 0xC0 != 0x80)?;
        decode_character(&chars[start..]).ok().map(|(code, _)| code)
    }

    /// Whether printchar or strout was called for this output.
    pub(crate) fn written(&self) -> bool {
        self.strout_called || !self.bytes.is_empty()
    }

    /// Whether strout was called for this output, if only with nothing.
    pub(crate) fn strout_called(&self) -> bool {
        self.strout_called
    }

    /// Whether a strout of a C string's octets that are not all ASCII is
    /// among the output.
    pub(crate) fn has_octets(&self) -> bool {
        self.strouts
            .iter()
            .any(|strout| strout.octets && !self.bytes[strout.start..strout.end].is_ascii())
    }

    /// The numbers a function printed to is called with, one a character.
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

    pub(crate) fn into_value(
        self,
        interp: &mut Interpreter,
        env: &Env,
    ) -> Result<Value, LispError> {
        // Every ASCII byte is a character of its own.
        let characters = if self.bytes.is_ascii() {
            self.bytes.len()
        } else {
            self.codes().count()
        };
        let multibyte = self.multibyte;
        let bytes = if multibyte || self.bytes.is_ascii() {
            self.bytes
        } else {
            self.octets()
        };
        crate::lisp::types::StringObjectRef::from_storage(bytes, characters, multibyte)
            .map(Value::StringObject)
            .map_err(|error| string_storage_error(interp, env, error))
    }

    /// print.c:print_unwind onto print_buffer holding this text: SAVED's
    /// first SCHARS bytes copied over its start, its length kept.  (A
    /// copy that ends inside a character leaves bytes that are no
    /// character: each stays as the raw byte it is.)
    pub(crate) fn overwritten_by(mut self, saved: &Self) -> Self {
        let count = saved.restored_len().min(self.bytes.len());
        if count == 0 {
            return self;
        }
        let pos = self.pos();
        self.bytes[..count].copy_from_slice(&saved.bytes[..count]);
        self.strouts.clear();
        self.function_codes.clear();
        if self.bytes.is_ascii() {
            self.multibyte = false;
        } else {
            self.bytes = canonical_text(&self.bytes);
            self.multibyte = self.multibyte || saved.multibyte;
        }
        self.counted = (pos != characters(&self.bytes)).then_some((self.bytes.len(), pos));
        self
    }

    /// print_buffer's `pos': the characters printchar and strout counted
    /// into the text.
    pub(crate) fn pos(&self) -> usize {
        match self.counted {
            Some((bytes, pos)) => pos + characters(&self.bytes[bytes.min(self.bytes.len())..]),
            None => characters(&self.bytes),
        }
    }

    /// Whether `pos' and `pos_byte' agree for a text that holds
    /// multibyte characters: counted that way after print_unwind, its
    /// bytes are a character each to print_finish.
    pub(crate) fn bytes_counted_as_characters(&self) -> bool {
        self.counted.is_some() && !self.bytes.is_ascii() && self.pos() == self.bytes.len()
    }

    /// How many of this text's bytes print_unwind copies back when it is
    /// the text set aside: SCHARS of the string print_prepare made of it.
    pub(crate) fn restored_len(&self) -> usize {
        self.pos().min(self.bytes.len())
    }

    /// The text MEMORY, bytes of print_buffer, holds: MULTIBYTE as the
    /// text they were written as.  (Bytes that are no character, where
    /// one print's text ends inside another's character, each stay as the
    /// raw byte they are.)
    pub(crate) fn from_memory(memory: &[u8], multibyte: bool) -> Self {
        if memory.is_ascii() {
            return Self {
                bytes: memory.to_vec(),
                ..Self::default()
            };
        }
        Self {
            bytes: canonical_text(memory),
            multibyte,
            ..Self::default()
        }
    }
}

/// A C string's BYTE as strout hands it to a function printed to: `int
/// ch = ptr[i]', the `char' it is on the host, which printchar takes as
/// an unsigned int.
pub(crate) fn c_char_argument(byte: u8) -> u32 {
    byte as std::ffi::c_char as i32 as u32
}

/// The characters of the text BYTES: every byte but the trailing ones of
/// a multibyte form.
fn characters(bytes: &[u8]) -> usize {
    if bytes.is_ascii() {
        return bytes.len();
    }
    bytes.iter().filter(|byte| *byte & 0xC0 != 0x80).count()
}

/// BYTES as text: what decodes stays, any other byte becomes its raw-byte
/// character.
fn canonical_text(bytes: &[u8]) -> Vec<u8> {
    let mut canonical = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    while !rest.is_empty() {
        match decode_character(rest) {
            Ok((_, width)) if width <= rest.len() => {
                canonical.extend_from_slice(&rest[..width]);
                rest = &rest[width..];
            }
            _ => {
                let (bytes, count) = encode_character(0x3fff00 + u32::from(rest[0]))
                    .expect("a raw byte is a character");
                canonical.extend_from_slice(&bytes[..count]);
                rest = &rest[1..];
            }
        }
    }
    canonical
}

impl From<&str> for PrintOutput {
    /// TEXT as one strout wrote it.
    fn from(text: &str) -> Self {
        let mut result = Self::with_capacity(text.len());
        result.push_str(text);
        result
    }
}
