//! A decoder's source as coding.c's ONE_MORE_BYTE reads it, and the
//! charbuf that decode_coding fills a call at a time.
//!
//! ONE_MORE_BYTE gives the byte of a unibyte source; of a multibyte one
//! (coding->src_multibyte) it gives an ASCII character, and a raw-byte
//! character's two-byte form, as the byte, and any other character as its
//! negated code, which every decoder handles as its invalid code.  A
//! decoder runs until its source ends or its charbuf (`charbuf_size' ints,
//! less the room it keeps for annotations) is full; decode_coding calls
//! it again from where it stopped, so its local state (the byte after a
//! CR, the open `charset' run) starts afresh at each call.

use super::charset_table::CharsetBuiltin;
use super::{CharsetInfo, DecodedText, RAW_BYTE8_BASE, RAW_BYTE8_CHARS};
use crate::lisp::types::SymbolName;

/// ONE_MORE_BYTE: the next unit of SRC at P, or ON_END (no_more_source).
macro_rules! one_more {
    ($src:expr, $p:ident, $on_end:expr) => {{
        if $p == $src.len() {
            $on_end
        } else {
            let c = $src.at($p);
            $p += 1;
            c
        }
    }};
}

/// coding.c MAX_CHARBUF_SIZE.
const MAX_CHARBUF_SIZE: usize = 0x4000;
/// coding.c MAX_ANNOTATION_LENGTH.
pub(crate) const MAX_ANNOTATION_LENGTH: usize = 5;
/// coding.c MAX_CHARBUF_EXTRA_SIZE.
const MAX_CHARBUF_EXTRA_SIZE: usize = MAX_ANNOTATION_LENGTH * 3 + 1;

/// decode_coding's ALLOC_CONVERSION_WORK_AREA: a charbuf of as many ints
/// as SRC's bytes (coding->src_bytes) and MAX_CHARBUF_EXTRA_SIZE more, up
/// to MAX_CHARBUF_SIZE, so that a short source's annotations can fill it.
pub(crate) fn charbuf_size<S: Units + ?Sized>(src: &S) -> usize {
    (src.source_bytes() + MAX_CHARBUF_EXTRA_SIZE).min(MAX_CHARBUF_SIZE)
}
/// The ints ADD_CHARSET_DATA stores.
pub(crate) const CHARSET_DATA_LENGTH: usize = 4;

pub(crate) trait Units {
    fn len(&self) -> usize;
    /// The bytes of the source (its multibyte form's, for a multibyte one).
    fn source_bytes(&self) -> usize;
    /// ONE_MORE_BYTE's unit at INDEX.
    fn at(&self, index: usize) -> i32;
    /// The bytes of a unibyte source, for the decoders' fast paths.
    fn bytes(&self) -> Option<&[u8]>;
    /// How many bytes of the source the unit at INDEX is.
    fn source_len_at(&self, index: usize) -> usize;
    /// The source bytes of the units FROM..TO, each as decode_coding_iso_2022
    /// stores an undecodable character's (an ASCII byte as itself, any other
    /// as its eight-bit character); the count stored.
    fn push_source_bytes(&self, from: usize, to: usize, out: &mut DecodedText) -> usize;
    /// decode_coding's flush of the units from FROM on as binary
    /// characters: a raw-byte character as itself, every other byte of the
    /// source as its eight-bit character.
    fn flush(&self, from: usize, out: &mut DecodedText);
}

impl Units for [u8] {
    #[inline]
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn source_bytes(&self) -> usize {
        <[u8]>::len(self)
    }

    #[inline]
    fn at(&self, index: usize) -> i32 {
        i32::from(self[index])
    }

    #[inline]
    fn bytes(&self) -> Option<&[u8]> {
        Some(self)
    }

    fn source_len_at(&self, _index: usize) -> usize {
        1
    }

    fn push_source_bytes(&self, from: usize, to: usize, out: &mut DecodedText) -> usize {
        for &byte in &self[from..to] {
            out.push_byte(byte);
        }
        to - from
    }

    fn flush(&self, from: usize, out: &mut DecodedText) {
        for &byte in &self[from..] {
            out.push_byte(byte);
        }
    }
}

/// A multibyte source's units, and the bytes of its multibyte form.
pub(crate) struct CharUnits(Vec<i32>, usize);

impl CharUnits {
    /// The units of the character CODES (raw-byte characters as
    /// #x3FFF80..#x3FFFFF).
    pub(crate) fn from_codes(codes: impl Iterator<Item = u32>) -> Self {
        let mut bytes = 0;
        let units = codes
            .map(|code| {
                // CHAR_BYTES.
                bytes += match code {
                    0..0x80 => 1,
                    0x80..0x800 => 2,
                    0x800..0x1_0000 => 3,
                    0x1_0000..0x20_0000 => 4,
                    _ if RAW_BYTE8_CHARS.contains(&code) => 2,
                    _ => 5,
                };
                if code < 0x80 {
                    code as i32
                } else if RAW_BYTE8_CHARS.contains(&code) {
                    (code - RAW_BYTE8_BASE) as i32
                } else {
                    -(code as i32)
                }
            })
            .collect();
        CharUnits(units, bytes)
    }
}

/// CHAR_STRING: the multibyte form of the character C.
fn char_string(c: u32, bytes: &mut Vec<u8>) {
    if c < 0x80 {
        bytes.push(c as u8);
    } else if c < 0x800 {
        bytes.extend([0xC0 | (c >> 6) as u8, 0x80 | (c & 0x3F) as u8]);
    } else if c < 0x1_0000 {
        bytes.extend([
            0xE0 | (c >> 12) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]);
    } else if c < 0x20_0000 {
        bytes.extend([
            0xF0 | (c >> 18) as u8,
            0x80 | ((c >> 12) & 0x3F) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]);
    } else if RAW_BYTE8_CHARS.contains(&c) {
        let byte = (c - RAW_BYTE8_BASE) as u8;
        bytes.extend([0xC0 | ((byte >> 6) & 1), 0x80 | (byte & 0x3F)]);
    } else {
        bytes.extend([
            0xF8,
            0x80 | ((c >> 18) & 0x0F) as u8,
            0x80 | ((c >> 12) & 0x3F) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]);
    }
}

impl CharUnits {
    /// The multibyte form of the unit at INDEX.
    fn unit_bytes(&self, index: usize, bytes: &mut Vec<u8>) {
        let unit = self.0[index];
        if unit < 0 {
            char_string((-unit) as u32, bytes);
        } else if unit < 0x80 {
            bytes.push(unit as u8);
        } else {
            char_string(RAW_BYTE8_BASE + unit as u32, bytes);
        }
    }
}

impl Units for CharUnits {
    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    fn source_bytes(&self) -> usize {
        self.1
    }

    #[inline]
    fn at(&self, index: usize) -> i32 {
        self.0[index]
    }

    fn bytes(&self) -> Option<&[u8]> {
        None
    }

    fn source_len_at(&self, index: usize) -> usize {
        let mut bytes = Vec::new();
        self.unit_bytes(index, &mut bytes);
        bytes.len()
    }

    fn push_source_bytes(&self, from: usize, to: usize, out: &mut DecodedText) -> usize {
        let mut bytes = Vec::new();
        for index in from..to {
            self.unit_bytes(index, &mut bytes);
        }
        for &byte in &bytes {
            out.push_byte(byte);
        }
        bytes.len()
    }

    fn flush(&self, from: usize, out: &mut DecodedText) {
        let mut bytes = Vec::new();
        for index in from..self.0.len() {
            let unit = self.0[index];
            if unit >= 0 {
                out.push_byte(unit as u8);
            } else {
                bytes.clear();
                char_string((-unit) as u32, &mut bytes);
                for &byte in &bytes {
                    out.push_byte(byte);
                }
            }
        }
    }
}

/// How a decoder's call ended.
pub(crate) enum CallEnd {
    /// Its charbuf filled at CONSUMED (coding->consumed), the source not
    /// done.
    Full(usize),
    /// no_more_source at CONSUMED: the source ended, with the units from
    /// CONSUMED on (if any) unfinished (CODING_RESULT_INSUFFICIENT_SRC).
    Ended(usize),
    /// CODING_RESULT_INSUFFICIENT_DST at CONSUMED: output is pending, so
    /// decode_coding calls again even where the source has ended.
    Again(usize),
}

/// The `charset' run of one decoder call (last_id, last_offset), its
/// characters counted as char_offset counts them: the ones stored, and the
/// ones DECODE_CHAR could not decode, stored as -1, which produce_chars
/// skips as an annotation of length one.
pub(crate) struct CallRuns {
    last: Option<SymbolName>,
    /// The charset record of the open run, compared by identity, when it
    /// was opened from one.
    last_info: *const CharsetInfo,
    last_offset: usize,
    dropped: usize,
    uncounted: usize,
}

impl CallRuns {
    pub(crate) fn new() -> Self {
        CallRuns {
            last: None,
            last_info: std::ptr::null(),
            last_offset: 0,
            dropped: 0,
            uncounted: 0,
        }
    }

    /// The charset->id != charset_ascii && last_id != charset->id test of
    /// a character of CHARSET, by its record.
    #[inline]
    pub(crate) fn opens(&self, charset: &CharsetInfo) -> bool {
        !std::ptr::eq(self.last_info, charset) && charset.builtin != CharsetBuiltin::Ascii
    }

    /// Open a run of CHARSET (see `open').
    pub(crate) fn open_info(&mut self, charset: &CharsetInfo, out: &mut DecodedText) -> usize {
        let used = self.open(charset.name, out);
        self.last_info = charset;
        used
    }

    /// char_offset at OUT's end.
    #[inline]
    pub(crate) fn char_offset(&self, out: &DecodedText) -> usize {
        out.char_count() + self.dropped - self.uncounted
    }

    /// A character stored as -1.
    pub(crate) fn drop_char(&mut self) {
        self.dropped += 1;
    }

    /// COUNT characters stored without char_offset counting them.
    pub(crate) fn uncount(&mut self, count: usize) {
        self.uncounted += count;
    }

    pub(crate) fn is(&self, charset: SymbolName) -> bool {
        self.last == Some(charset)
    }

    /// ADD_CHARSET_DATA for the open run, whose annotation produce_charset
    /// puts over the NCHARS characters before the ones stored so far; the
    /// charbuf ints it takes.
    pub(crate) fn close(&mut self, out: &mut DecodedText) -> usize {
        self.last_info = std::ptr::null();
        let Some(charset) = self.last.take() else {
            return 0;
        };
        let pos = out.char_count();
        let nchars = self.char_offset(out) - self.last_offset;
        out.charsets
            .push((pos.saturating_sub(nchars), pos, charset));
        CHARSET_DATA_LENGTH
    }

    /// Open a run of CHARSET at char_offset (closing the open one; the
    /// charbuf ints that takes).
    pub(crate) fn open(&mut self, charset: SymbolName, out: &mut DecodedText) -> usize {
        let used = self.close(out);
        self.last_offset = self.char_offset(out);
        self.last = Some(charset);
        used
    }

    /// The ADD_CHARSET_DATA at no_more_source or the charbuf's end.
    pub(crate) fn finish(mut self, out: &mut DecodedText) {
        self.close(out);
    }
}
