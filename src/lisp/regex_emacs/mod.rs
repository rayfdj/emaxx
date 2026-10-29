//! GNU Emacs's regular expression engine, ported from regex-emacs.c.
//!
//! The compiler turns an Emacs regexp into GNU's byte code (`exactn',
//! `charset', `on_failure_jump', ...), and the matcher runs that byte code
//! against the text with GNU's backtracking machine and failure stack.  The
//! one change of representation is the text itself: GNU reads its internal
//! multibyte form in at most two contiguous strings (the buffer's text on
//! either side of the gap); this engine reads Emaxx's UTF-8 text, a string
//! or a rope's chunks, in place.  Raw bytes are the private-use characters
//! U+E080..U+E0FF there (`raw_byte_regex_char'), and `exactn' stores its
//! characters in that same form so a literal still matches byte for byte.
//! Syntax, categories, case translation and point come from the caller
//! through [`MatchEnv`], position by position, as SYNTAX and
//! UPDATE_SYNTAX_TABLE read them in GNU.

mod analyze;
mod compile;
mod exec;
#[cfg(test)]
mod tests;

pub(crate) use compile::compile;
pub(crate) use exec::{re_match, re_search};

/// The opcodes of a compiled pattern (regex-emacs.c's `re_opcode_t'), in
/// GNU's order.
pub(crate) mod op {
    pub(crate) const NO_OP: u8 = 0;
    pub(crate) const SUCCEED: u8 = 1;
    pub(crate) const EXACTN: u8 = 2;
    pub(crate) const ANYCHAR: u8 = 3;
    pub(crate) const CHARSET: u8 = 4;
    pub(crate) const CHARSET_NOT: u8 = 5;
    pub(crate) const START_MEMORY: u8 = 6;
    pub(crate) const STOP_MEMORY: u8 = 7;
    pub(crate) const DUPLICATE: u8 = 8;
    pub(crate) const BEGLINE: u8 = 9;
    pub(crate) const ENDLINE: u8 = 10;
    pub(crate) const BEGBUF: u8 = 11;
    pub(crate) const ENDBUF: u8 = 12;
    pub(crate) const JUMP: u8 = 13;
    pub(crate) const ON_FAILURE_JUMP: u8 = 14;
    pub(crate) const ON_FAILURE_KEEP_STRING_JUMP: u8 = 15;
    pub(crate) const ON_FAILURE_JUMP_LOOP: u8 = 16;
    pub(crate) const ON_FAILURE_JUMP_NASTYLOOP: u8 = 17;
    pub(crate) const ON_FAILURE_JUMP_SMART: u8 = 18;
    pub(crate) const SUCCEED_N: u8 = 19;
    pub(crate) const JUMP_N: u8 = 20;
    pub(crate) const SET_NUMBER_AT: u8 = 21;
    pub(crate) const WORDBEG: u8 = 22;
    pub(crate) const WORDEND: u8 = 23;
    pub(crate) const WORDBOUND: u8 = 24;
    pub(crate) const NOTWORDBOUND: u8 = 25;
    pub(crate) const SYMBEG: u8 = 26;
    pub(crate) const SYMEND: u8 = 27;
    pub(crate) const SYNTAXSPEC: u8 = 28;
    pub(crate) const NOTSYNTAXSPEC: u8 = 29;
    pub(crate) const AT_DOT: u8 = 30;
    pub(crate) const CATEGORYSPEC: u8 = 31;
    pub(crate) const NOTCATEGORYSPEC: u8 = 32;
}

/// Syntax classes as syntax.h's `enum syntaxcode' numbers them.
pub(crate) mod syntax_code {
    pub(crate) const WHITESPACE: u8 = 0;
    pub(crate) const PUNCT: u8 = 1;
    pub(crate) const WORD: u8 = 2;
    pub(crate) const SYMBOL: u8 = 3;
    pub(crate) const OPEN: u8 = 4;
    pub(crate) const CLOSE: u8 = 5;
    pub(crate) const QUOTE: u8 = 6;
    pub(crate) const STRING: u8 = 7;
    pub(crate) const MATH: u8 = 8;
    pub(crate) const ESCAPE: u8 = 9;
    pub(crate) const CHARQUOTE: u8 = 10;
    pub(crate) const COMMENT: u8 = 11;
    pub(crate) const ENDCOMMENT: u8 = 12;
    pub(crate) const INHERIT: u8 = 13;
    pub(crate) const COMMENT_FENCE: u8 = 14;
    pub(crate) const STRING_FENCE: u8 = 15;
}

/// syntax.c's `syntax_spec_code': the class a `\sC' letter names, 0377 for
/// a letter that names none (GNU compiles it without complaint, and the
/// resulting `syntaxspec' never matches).
pub(crate) fn syntax_spec_code(c: u32) -> u8 {
    use syntax_code::*;
    match c {
        0x20 | 0x2d => WHITESPACE, // ' ' '-'
        0x21 => COMMENT_FENCE,     // '!'
        0x22 => STRING,            // '"'
        0x24 => MATH,              // '$'
        0x27 => QUOTE,             // '\''
        0x28 => OPEN,              // '('
        0x29 => CLOSE,             // ')'
        0x2e => PUNCT,             // '.'
        0x2f => CHARQUOTE,         // '/'
        0x3c => COMMENT,           // '<'
        0x3e => ENDCOMMENT,        // '>'
        0x40 => INHERIT,           // '@'
        0x5f => SYMBOL,            // '_'
        0x5c => ESCAPE,            // '\\'
        0x57 | 0x77 => WORD,       // 'W' 'w'
        0x7c => STRING_FENCE,      // '|'
        _ => 0o377,
    }
}

/// regex-emacs.c's `reg_errcode_t', the errors `regex_compile' returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegError {
    BadPat,
    ECtype,
    EEscape,
    ESubreg,
    EBrack,
    EParen,
    EBrace,
    BadBr,
    EEnd,
    ESize,
    ERParen,
    ESizeBr,
}

impl RegError {
    /// `re_error_msgid': the message `invalid-regexp' carries.
    pub(crate) fn message(self) -> &'static str {
        match self {
            RegError::BadPat => "Invalid regular expression",
            RegError::ECtype => "Invalid character class name",
            RegError::EEscape => "Trailing backslash",
            RegError::ESubreg => "Invalid back reference",
            RegError::EBrack => "Unmatched [ or [^",
            RegError::EParen => "Unmatched ( or \\(",
            RegError::EBrace => "Unmatched \\{",
            RegError::BadBr | RegError::ESizeBr => "Invalid content of \\{\\}",
            RegError::EEnd => "Premature end of regular expression",
            RegError::ESize => "Regular expression too big",
            RegError::ERParen => "Unmatched ) or \\)",
        }
    }
}

/// `re_pattern_buffer': a compiled pattern.  The matcher writes the
/// counters of `succeed_n'/`jump_n' into `buffer' as it runs and turns an
/// `on_failure_jump_smart' into the loop it chose, as GNU does, so a pattern
/// is matched through `&mut' and a cache hands each entry to one matcher at
/// a time.
#[derive(Clone, Debug)]
pub(crate) struct Pattern {
    pub(crate) buffer: Vec<u8>,
    /// Number of subexpressions found by the compiler.
    pub(crate) re_nsub: usize,
    pub(crate) fastmap: [bool; 256],
    pub(crate) fastmap_accurate: bool,
    /// The bytes the fastmap admits, counted when it is computed: an
    /// untranslated search for a pattern only one byte can start skips
    /// to that byte's next occurrence.
    pub(crate) fastmap_leads: FastmapLeads,
    /// Whether the pattern can match the empty string (only as precise as
    /// `re_search_2' needs: true means "don't use the fastmap").
    pub(crate) can_be_null: bool,
    /// Whether compiling read the syntax table (`[:space:]', `[:word:]'),
    /// so the pattern is valid for the current syntax table only.
    pub(crate) used_syntax: bool,
    /// Whether the pattern string was multibyte.
    pub(crate) multibyte: bool,
    /// Whether matching translates text characters (case folding).
    pub(crate) translate: bool,
    /// The fastmap's verdict on each ASCII character of a text, for a
    /// translation stamp and a multibyte or unibyte text: bit B is set
    /// when B, translated, can start a match.
    pub(crate) ascii_admits: Option<(u64, bool, u128)>,
}

/// How many bytes a pattern's fastmap admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FastmapLeads {
    None,
    One(u8),
    Many,
}

/// The largest compiled pattern: jump offsets are two-byte numbers.
pub(crate) const MAX_BUF_SIZE: usize = 1 << 15;

/// The first raw-byte character, `BYTE8_TO_CHAR (0x80)'.
pub(crate) const BYTE8_FIRST: u32 = 0x3F_FF80;
/// `BYTE8_TO_CHAR (0)': raw byte B is the character BYTE8_BASE + B.
pub(crate) const BYTE8_BASE: u32 = 0x3F_FF00;
pub(crate) const MAX_CHAR: u32 = 0x3F_FFFF;
/// The private-use character a raw byte is stored as: U+E000 + byte.
pub(crate) const RAW_BYTE_STORAGE_BASE: u32 = 0xE000;
/// The placeholder stored for a character beyond Unicode, whose code a
/// side table keeps (`json::INVALID_UNICODE_SENTINEL').
pub(crate) const EXTENDED_PLACEHOLDER: char = '\u{F8FF}';

/// CHAR_BYTE8_P: whether C is a raw-byte character.
#[inline]
pub(crate) fn char_byte8_p(c: u32) -> bool {
    c >= BYTE8_FIRST
}

/// RE_CHAR_TO_UNIBYTE (CHAR_TO_BYTE_SAFE): the byte an ASCII or raw-byte
/// character is, None for any other character.
#[inline]
pub(crate) fn char_to_unibyte(c: u32) -> Option<u32> {
    if c < 0x80 {
        Some(c)
    } else if char_byte8_p(c) {
        Some(c - BYTE8_BASE)
    } else {
        None
    }
}

/// RE_CHAR_TO_MULTIBYTE (UNIBYTE_TO_CHAR): byte B as a character.
#[inline]
pub(crate) fn unibyte_to_char(b: u32) -> u32 {
    if b < 0x80 { b } else { BYTE8_BASE + b }
}

/// The Emacs character a stored character is, when no side-table entry
/// overrides it.
#[inline]
pub(crate) fn code_of_stored(ch: char) -> u32 {
    let code = ch as u32;
    if (RAW_BYTE_STORAGE_BASE + 0x80..=RAW_BYTE_STORAGE_BASE + 0xFF).contains(&code) {
        BYTE8_BASE + (code - RAW_BYTE_STORAGE_BASE)
    } else {
        code
    }
}

/// Whether a stored character may stand for something else: a raw-byte
/// marker or the placeholder, which a side-table entry can override.
#[inline]
pub(crate) fn stored_char_is_ambiguous(ch: char) -> bool {
    let code = ch as u32;
    ch == EXTENDED_PLACEHOLDER
        || (RAW_BYTE_STORAGE_BASE + 0x80..=RAW_BYTE_STORAGE_BASE + 0xFF).contains(&code)
}

/// The stored character for Emacs character C, None for one the text
/// stores only through a side table (beyond Unicode, or a real U+E080..
/// U+E0FF, which reads as a raw byte without one).
#[inline]
pub(crate) fn stored_char_of(c: u32) -> Option<char> {
    if char_byte8_p(c) {
        char::from_u32(RAW_BYTE_STORAGE_BASE + (c - BYTE8_BASE))
    } else if (RAW_BYTE_STORAGE_BASE + 0x80..=RAW_BYTE_STORAGE_BASE + 0xFF).contains(&c)
        || c == EXTENDED_PLACEHOLDER as u32
    {
        None
    } else {
        char::from_u32(c)
    }
}

/// CHAR_LEADING_CODE in the stored form: the first byte of C's encoding,
/// the byte the fastmap is indexed by.
#[inline]
pub(crate) fn leading_byte(c: u32) -> u8 {
    if c < 0x80 {
        return c as u8;
    }
    let stored = match stored_char_of(c) {
        Some(ch) => ch,
        None if c > 0x10_FFFF => EXTENDED_PLACEHOLDER,
        // A real U+E080..U+E0FF is stored as itself.
        None => char::from_u32(c).unwrap_or(EXTENDED_PLACEHOLDER),
    };
    let mut bytes = [0u8; 4];
    stored.encode_utf8(&mut bytes).as_bytes()[0]
}

/// The first and last lead bytes of a stored non-ASCII character
/// (MIN_MULTIBYTE_LEADING_CODE, MAX_MULTIBYTE_LEADING_CODE).
pub(crate) const MIN_LEADING_BYTE: u8 = 0xC2;
pub(crate) const MAX_LEADING_BYTE: u8 = 0xF4;

/// Decode the stored character at the start of BYTES, which hold whole
/// characters of valid UTF-8.
#[inline]
pub(crate) fn decode_stored(bytes: &[u8]) -> (char, usize) {
    let lead = bytes[0];
    if lead < 0x80 {
        return (lead as char, 1);
    }
    let (len, init) = if lead < 0xE0 {
        (2, u32::from(lead & 0x1F))
    } else if lead < 0xF0 {
        (3, u32::from(lead & 0x0F))
    } else {
        (4, u32::from(lead & 0x07))
    };
    let mut code = init;
    for &byte in &bytes[1..len] {
        code = (code << 6) | u32::from(byte & 0x3F);
    }
    // SAFETY: the bytes are valid UTF-8, so the scalar is a char.
    (unsafe { char::from_u32_unchecked(code) }, len)
}

/// The byte length of the stored character a lead byte starts
/// (BYTES_BY_CHAR_HEAD).
#[inline]
pub(crate) fn bytes_by_char_head(lead: u8) -> usize {
    if lead < 0x80 {
        1
    } else if lead < 0xE0 {
        2
    } else if lead < 0xF0 {
        3
    } else {
        4
    }
}

/// The text a pattern is matched against: a string or the accessible part
/// of a buffer, read in place a chunk at a time.  Byte offsets are relative
/// to its start (search.c's offsets from BEGV_BYTE).
#[derive(Clone, Copy)]
pub(crate) struct Text<'a> {
    source: TextSource<'a>,
    size: usize,
    /// Whether characters of the text sit in a side table, so a raw-byte
    /// marker or the placeholder must be checked against it.
    has_side_table: bool,
    /// Whether the text is multibyte (`enable-multibyte-characters', or the
    /// string's multibyteness): RE_TARGET_MULTIBYTE_P.
    pub(crate) multibyte: bool,
    /// Whether the text is a unibyte buffer's, which stores the bytes 0x80
    /// to 0xFF as U+0080 to U+00FF where a string stores raw-byte markers:
    /// the matcher reads those as the raw-byte characters, and never
    /// compares their stored bytes with a pattern's.
    pub(super) latin1_bytes: bool,
}

/// Where the text's bytes are: one run (a string's data, read in place as
/// GNU's re_search_2 reads SDATA) or a rope's chunks.
#[derive(Clone, Copy)]
pub(super) enum TextSource<'a> {
    Str(&'a str),
    Rope(RopeRange<'a>),
}

/// Bytes START to START + LEN of a rope, BEGV_BYTE to ZV_BYTE of a buffer's
/// text: read a chunk at a time as re_search_2 reads the buffer's two
/// halves in place, with no ropey::RopeSlice built for it (whose
/// construction counts the characters and lines of the leaf it lands in).
#[derive(Clone, Copy)]
pub(crate) struct RopeRange<'a> {
    rope: &'a ropey::Rope,
    start: usize,
    len: usize,
    /// The character index of START.
    start_char: usize,
    /// A chunk of the rope already found and its first byte, read without
    /// walking the tree (see `with_chunk').
    known: Option<(&'a [u8], usize)>,
}

impl<'a> RopeRange<'a> {
    /// Bytes START..END of ROPE, START being the character START_CHAR.
    pub(crate) fn new(rope: &'a ropey::Rope, start: usize, end: usize, start_char: usize) -> Self {
        debug_assert!(start <= end && end <= rope.len_bytes());
        RopeRange {
            rope,
            start,
            len: end - start,
            start_char,
            known: None,
        }
    }

    /// The range with CHUNK of its rope, starting at the rope's byte
    /// CHUNK_START, known: a read inside it walks no tree.
    pub(crate) fn with_chunk(self, chunk: &'a str, chunk_start: usize) -> Self {
        RopeRange {
            known: Some((chunk.as_bytes(), chunk_start)),
            ..self
        }
    }

    #[inline]
    pub(crate) fn len_bytes(&self) -> usize {
        self.len
    }

    /// The chunk holding byte D of the range, cut to the range, and the
    /// range offset its first byte has.
    #[inline]
    pub(crate) fn chunk_at_byte(&self, d: usize) -> (&'a [u8], usize) {
        let at = self.start + d;
        let (bytes, chunk_start) = match self.known {
            Some((bytes, chunk_start)) if at >= chunk_start && at < chunk_start + bytes.len() => {
                (bytes, chunk_start)
            }
            _ => {
                let (chunk, chunk_start, _, _) = self.rope.chunk_at_byte(at);
                (chunk.as_bytes(), chunk_start)
            }
        };
        let low = self.start.max(chunk_start);
        let high = (self.start + self.len).min(chunk_start + bytes.len());
        (
            &bytes[low - chunk_start..high - chunk_start],
            low - self.start,
        )
    }

    /// The character offset of byte offset D from the range's start.
    #[inline]
    pub(crate) fn byte_to_char(&self, d: usize) -> usize {
        self.rope.byte_to_char(self.start + d) - self.start_char
    }
}

impl<'a> Text<'a> {
    pub(crate) fn from_str(text: &'a str, multibyte: bool, has_side_table: bool) -> Self {
        Text {
            source: TextSource::Str(text),
            size: text.len(),
            has_side_table,
            multibyte,
            latin1_bytes: false,
        }
    }

    pub(crate) fn from_rope(rope: RopeRange<'a>, multibyte: bool, has_side_table: bool) -> Self {
        Text {
            size: rope.len_bytes(),
            source: TextSource::Rope(rope),
            has_side_table,
            multibyte,
            latin1_bytes: false,
        }
    }

    /// The text of a unibyte buffer holding bytes above 0x7F (see
    /// `latin1_bytes').
    pub(crate) fn with_latin1_bytes(mut self, latin1_bytes: bool) -> Self {
        self.latin1_bytes = latin1_bytes;
        self
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.size
    }

    /// Byte offset D as a character offset.
    #[cfg(test)]
    pub(crate) fn char_offset(&self, d: usize) -> usize {
        match self.source {
            TextSource::Str(text) => ropey::str_utils::byte_to_char_idx(text, d),
            TextSource::Rope(rope) => rope.byte_to_char(d),
        }
    }

    pub(super) fn source(&self) -> TextSource<'a> {
        self.source
    }
}

/// What the matcher asks of the buffer or string it searches: GNU reads
/// these from the current buffer and `re_match_object'.
pub(crate) trait MatchEnv {
    /// RE_TRANSLATE with the pattern's table (case canonicalization).
    fn translate(&mut self, c: u32) -> u32;
    /// A count that changes whenever `translate' may map a character
    /// differently.
    fn translate_stamp(&self) -> u64;
    /// RE_SETUP_SYNTAX_TABLE_FOR_OBJECT: ready `syntax' for a match or
    /// search starting at byte offset AT.
    fn setup_syntax(&mut self, at: usize);
    /// SYNTAX: the syntax class of C, the character at byte offset AT, with
    /// syntax-table properties when `parse-sexp-lookup-properties' is set
    /// (UPDATE_SYNTAX_TABLE).
    fn syntax(&mut self, c: u32, at: usize) -> u8;
    /// BUFFER_SYNTAX: the syntax class of C in the buffer's own table.
    fn buffer_syntax(&mut self, c: u32) -> u8;
    /// CHAR_HAS_CATEGORY.
    fn has_category(&mut self, c: u32, category: u8) -> bool;
    /// category.c's word_boundary_p.
    fn word_boundary_p(&mut self, c1: u32, c2: u32) -> bool;
    /// The `unicode-category-table' class predicates of character.c.
    fn alphabeticp(&mut self, c: u32) -> bool;
    fn alphanumericp(&mut self, c: u32) -> bool;
    fn graphicp(&mut self, c: u32) -> bool;
    fn printablep(&mut self, c: u32) -> bool;
    fn blankp(&mut self, c: u32) -> bool;
    /// buffer.h's lowercasep and uppercasep, from the case tables.
    fn lowercasep(&mut self, c: u32) -> bool;
    fn uppercasep(&mut self, c: u32) -> bool;
    /// Point's byte offset in the text, None when it is not a buffer's
    /// (`\=' then never matches).
    fn point(&self) -> Option<usize>;
    /// The side table's character at byte offset AT, for a stored
    /// character `stored_char_is_ambiguous' accepts.
    fn side_table_char(&mut self, at: usize) -> Option<u32>;
    /// maybe_quit, at the matcher's jumps and failures.
    fn maybe_quit(&mut self) -> Result<(), crate::lisp::types::LispError>;
}

/// re_match_2 and re_search_2's result for a match that did not overflow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Registers {
    /// Byte offsets of each group's start and end, None for a group that
    /// did not match; group 0 is the whole match.
    pub(crate) regs: Vec<Option<(usize, usize)>>,
}

/// Why a search found no answer: nothing matched, or GNU's -2 (the
/// failure stack would grow past `emacs_re_max_failures').
#[derive(Debug)]
pub(crate) enum MatchError {
    Overflow,
    Lisp(crate::lisp::types::LispError),
}

impl From<crate::lisp::types::LispError> for MatchError {
    fn from(error: crate::lisp::types::LispError) -> Self {
        MatchError::Lisp(error)
    }
}
