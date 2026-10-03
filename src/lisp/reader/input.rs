//! The reader's borrowed text or callable character source. Function streams
//! keep only bounded lookahead, and return it through the Lisp callback.

use super::{INVALID_UNICODE_SENTINEL, encode_raw_byte};
use crate::lisp::alloc::RootedVec;
use crate::lisp::types::{LispError, Value};

/// One exclusive borrow owns calls and symbol interning. A callback may
/// evaluate Lisp, collect, redefine the stream, or change the active obarray.
pub(crate) trait ReaderStream {
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
    // Like lread.c:rdstack: completed siblings stay rooted while the next
    // callback runs. A completed parent replaces its children in this stack.
    roots: RootedVec<Value>,
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
    },
    Function(FunctionInput<'a>),
}

impl<'a> Input<'a> {
    pub(super) fn function(stream: &'a mut dyn ReaderStream) -> Self {
        Self::Function(FunctionInput {
            stream,
            characters: [Character::default(); 4],
            len: 0,
            first_byte: 0,
            eof: false,
            position: 0,
            roots: RootedVec::new(),
        })
    }

    pub(super) fn peek(&mut self, position: usize, offset: usize) -> Result<Option<u8>, LispError> {
        match self {
            Self::Text(bytes) | Self::Encoded { bytes, .. } => {
                Ok(bytes.get(position + offset).copied())
            }
            Self::Function(input) => input.peek(offset),
        }
    }

    pub(super) fn advance(&mut self, position: usize) -> Result<Option<u8>, LispError> {
        match self {
            Self::Text(bytes) | Self::Encoded { bytes, .. } => Ok(bytes.get(position).copied()),
            Self::Function(input) => input.advance(),
        }
    }

    pub(super) fn retreat_ascii(&mut self, byte: u8) {
        if let Self::Function(input) = self {
            input.retreat_ascii(byte);
        }
    }

    pub(super) fn unread_lookahead(&mut self) -> Result<(), LispError> {
        match self {
            Self::Text(_) | Self::Encoded { .. } => Ok(()),
            Self::Function(input) => input.unread_lookahead(),
        }
    }

    pub(super) fn function_code(&mut self) -> Result<Option<i64>, LispError> {
        match self {
            Self::Text(_) | Self::Encoded { .. } => Ok(None),
            Self::Function(input) => Ok(input.peek(0)?.map(|_| input.characters[0].code)),
        }
    }

    pub(super) fn function_position(&self) -> Option<i64> {
        match self {
            Self::Text(_) | Self::Encoded { .. } => None,
            Self::Function(input) => Some(input.position),
        }
    }

    pub(super) fn root_depth(&self) -> usize {
        match self {
            Self::Text(_) | Self::Encoded { .. } => 0,
            Self::Function(input) => input.roots.len(),
        }
    }

    pub(super) fn root_result(&mut self, depth: usize, value: Option<Value>) {
        if let Self::Function(input) = self {
            input.roots.truncate(depth);
            if let Some(value) = value {
                input.roots.push(value);
            }
        }
    }

    pub(super) fn encoded_character(
        &self,
        position: usize,
    ) -> Result<Option<(u32, usize)>, LispError> {
        let Self::Encoded { bytes, multibyte } = self else {
            return Ok(None);
        };
        let Some(&byte) = bytes.get(position) else {
            return Ok(None);
        };
        if *multibyte {
            crate::lisp::types::string_data::decode_character(&bytes[position..]).map(Some)
        } else {
            Ok(Some((u32::from(byte), 1)))
        }
    }
}
