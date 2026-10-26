//! The reader's borrowed text or callable character source. Function streams
//! keep only bounded lookahead, and return it through the Lisp callback.

use super::{INVALID_UNICODE_SENTINEL, encode_raw_byte};
use crate::lisp::types::{LispError, Value};

/// One exclusive borrow owns calls and symbol interning. A callback may
/// evaluate Lisp, collect, redefine the stream, or change the active obarray.
/// The stream is also the read's context (the objects it builds, `#$').
pub(crate) trait ReaderStream: super::ReadContext {
    fn read_character(&mut self) -> Result<Option<i64>, LispError>;
    fn unread_character(&mut self, character: i64) -> Result<(), LispError>;
    fn intern_symbol(&mut self, name: &str, shorthand: bool) -> Result<Value, LispError>;
}

#[derive(Clone, Copy, Default)]
struct Character {
    code: i64,
    bytes: [u8; 4],
    len: usize,
}

impl Character {
    fn new(code: i64) -> Self {
        let character = if (0x3fff80..=0x3fffff).contains(&code) {
            encode_raw_byte(code as u8)
        } else {
            u32::try_from(code)
                .ok()
                .and_then(char::from_u32)
                .unwrap_or(INVALID_UNICODE_SENTINEL)
        };
        let mut bytes = [0; 4];
        let len = character.encode_utf8(&mut bytes).len();
        Self { code, bytes, len }
    }
}

pub(super) struct FunctionInput<'a> {
    pub(super) stream: &'a mut dyn ReaderStream,
    // The parser looks ahead at most two bytes beyond its current byte.
    // Four characters also leave room for its one-byte syntax rewind.
    characters: [Character; 4],
    len: usize,
    first_byte: usize,
    eof: bool,
    position: i64,
}

impl FunctionInput<'_> {
    fn peek(&mut self, mut offset: usize) -> Result<Option<u8>, LispError> {
        offset += self.first_byte;
        for character in &self.characters[..self.len] {
            if offset < character.len {
                return Ok(Some(character.bytes[offset]));
            }
            offset -= character.len;
        }
        while !self.eof {
            let Some(code) = self.stream.read_character()? else {
                self.eof = true;
                break;
            };
            let character = Character::new(code);
            assert!(self.len < self.characters.len(), "reader lookahead bound");
            self.characters[self.len] = character;
            self.len += 1;
            if offset < character.len {
                return Ok(Some(character.bytes[offset]));
            }
            offset -= character.len;
        }
        Ok(None)
    }

    fn advance(&mut self) -> Result<Option<u8>, LispError> {
        let Some(byte) = self.peek(0)? else {
            return Ok(None);
        };
        self.first_byte += 1;
        if self.first_byte == self.characters[0].len {
            self.characters.copy_within(1..self.len, 0);
            self.len -= 1;
            self.first_byte = 0;
            self.position += 1;
        }
        Ok(Some(byte))
    }

    fn retreat_ascii(&mut self, byte: u8) {
        assert_eq!(self.first_byte, 0);
        assert!(byte.is_ascii() && self.len < self.characters.len());
        self.characters.copy_within(0..self.len, 1);
        self.characters[0] = Character::new(i64::from(byte));
        self.len += 1;
        self.position -= 1;
    }

    fn unread_lookahead(&mut self) -> Result<(), LispError> {
        assert_eq!(self.first_byte, 0, "unread only whole characters");
        while self.len > 0 {
            self.len -= 1;
            self.stream
                .unread_character(self.characters[self.len].code)?;
        }
        self.eof = false;
        Ok(())
    }
}

pub(super) enum Input<'a> {
    Text(&'a [u8]),
    /// Borrow the authoritative Lisp string/blob bytes. Their encoding is
    /// Emacs's full character range, not Rust UTF-8.
    Encoded {
        bytes: &'a [u8],
        multibyte: bool,
        /// Bytes that need not be well formed, taken as lread.c
        /// readchar's read_multibyte takes a file's: a byte that begins no
        /// whole character is the raw byte (BYTE8_TO_CHAR).
        lenient: bool,
    },
    Function(FunctionInput<'a>),
    /// A buffer's text read in place, as lread.c's readchar reads it from
    /// the buffer: the chunk holding the last byte read is kept at hand.
    Rope(RopeInput<'a>),
}

pub(super) struct RopeInput<'a> {
    pub(super) rope: ropey::RopeSlice<'a>,
    chunk: &'a [u8],
    chunk_start: usize,
}

impl<'a> RopeInput<'a> {
    fn byte(&mut self, position: usize) -> Option<u8> {
        if let Some(byte) = position
            .checked_sub(self.chunk_start)
            .and_then(|offset| self.chunk.get(offset))
        {
            return Some(*byte);
        }
        if position >= self.rope.len_bytes() {
            return None;
        }
        let (chunk, chunk_start, _, _) = self.rope.chunk_at_byte(position);
        self.chunk = chunk.as_bytes();
        self.chunk_start = chunk_start;
        self.chunk.get(position - chunk_start).copied()
    }
}

impl<'a> Input<'a> {
    pub(super) fn rope(rope: ropey::RopeSlice<'a>) -> Self {
        Self::Rope(RopeInput {
            rope,
            chunk: &[],
            chunk_start: 0,
        })
    }

    pub(super) fn function(stream: &'a mut dyn ReaderStream) -> Self {
        Self::Function(FunctionInput {
            stream,
            characters: [Character::default(); 4],
            len: 0,
            first_byte: 0,
            eof: false,
            position: 0,
        })
    }

    pub(super) fn peek(&mut self, position: usize, offset: usize) -> Result<Option<u8>, LispError> {
        match self {
            Self::Text(bytes) | Self::Encoded { bytes, .. } => {
                Ok(bytes.get(position + offset).copied())
            }
            Self::Function(input) => input.peek(offset),
            Self::Rope(input) => Ok(input.byte(position + offset)),
        }
    }

    pub(super) fn advance(&mut self, position: usize) -> Result<Option<u8>, LispError> {
        match self {
            Self::Text(bytes) | Self::Encoded { bytes, .. } => Ok(bytes.get(position).copied()),
            Self::Function(input) => input.advance(),
            Self::Rope(input) => Ok(input.byte(position)),
        }
    }

    /// The position just past the next newline at or after POSITION (the
    /// end when there is none), for input read in place; None for a
    /// function stream, read a character at a time.
    pub(super) fn skip_line(&mut self, position: usize) -> Option<usize> {
        match self {
            Self::Text(bytes) | Self::Encoded { bytes, .. } => {
                let rest = bytes.get(position..)?;
                Some(memchr::memchr(b'\n', rest).map_or(bytes.len(), |at| position + at + 1))
            }
            Self::Rope(input) => {
                let mut position = position;
                while input.byte(position).is_some() {
                    let offset = position - input.chunk_start;
                    if let Some(at) = memchr::memchr(b'\n', &input.chunk[offset..]) {
                        return Some(position + at + 1);
                    }
                    position = input.chunk_start + input.chunk.len();
                }
                Some(position)
            }
            Self::Function(_) => None,
        }
    }

    pub(super) fn retreat_ascii(&mut self, byte: u8) {
        if let Self::Function(input) = self {
            input.retreat_ascii(byte);
        }
    }

    pub(super) fn unread_lookahead(&mut self) -> Result<(), LispError> {
        match self {
            Self::Text(_) | Self::Encoded { .. } | Self::Rope(_) => Ok(()),
            Self::Function(input) => input.unread_lookahead(),
        }
    }

    pub(super) fn function_code(&mut self) -> Result<Option<i64>, LispError> {
        match self {
            Self::Text(_) | Self::Encoded { .. } | Self::Rope(_) => Ok(None),
            Self::Function(input) => Ok(input.peek(0)?.map(|_| input.characters[0].code)),
        }
    }

    pub(super) fn function_position(&self) -> Option<i64> {
        match self {
            Self::Text(_) | Self::Encoded { .. } | Self::Rope(_) => None,
            Self::Function(input) => Some(input.position),
        }
    }

    pub(super) fn encoded_character(
        &self,
        position: usize,
    ) -> Result<Option<(u32, usize)>, LispError> {
        let Self::Encoded {
            bytes,
            multibyte,
            lenient,
        } = self
        else {
            return Ok(None);
        };
        let Some(&byte) = bytes.get(position) else {
            return Ok(None);
        };
        if *lenient {
            Ok(Some(read_multibyte(&bytes[position..])))
        } else if *multibyte {
            crate::lisp::types::string_data::decode_character(&bytes[position..]).map(Some)
        } else {
            Ok(Some((u32::from(byte), 1)))
        }
    }
}

/// lread.c readchar's read_multibyte over a file's bytes: an ASCII byte,
/// the character a lead byte and its BYTES_BY_CHAR_HEAD - 1 trailing
/// codes spell, or else the lead byte as a raw byte (BYTE8_TO_CHAR), the
/// bytes after it read again.  A lead STRING_CHAR cannot decode (a
/// trailing code, or one spelling a code past MAX_CHAR) is taken as a raw
/// byte too.
fn read_multibyte(bytes: &[u8]) -> (u32, usize) {
    let lead = bytes[0];
    if lead < 0x80 {
        return (u32::from(lead), 1);
    }
    crate::lisp::types::string_data::decode_character(bytes)
        .unwrap_or((0x3F_FF00 + u32::from(lead), 1))
}
