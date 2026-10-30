//! search.c's search for a plain string: `search_buffer_non_re',
//! `simple_search' and `boyer_moore', which `search-forward' and
//! `search-backward' use, and the regexp commands for a trivial regexp.
//!
//! GNU compares the bytes of its internal multibyte form.  These compare
//! the buffer's stored form, UTF-8 with a raw byte as U+E080..U+E0FF, which
//! is that form for every character up to U+10FFFF: characters that agree
//! but for `c & 0x3F' still differ only in their last byte, as
//! `boyer_moore' requires.  A unibyte buffer's text is read one byte per
//! character.  Where the stored form is ambiguous (a pattern character
//! with no stored form of its own, or a raw byte where the text keeps a
//! side table) the search compares characters, as `simple_search' does.

use super::*;
use crate::lisp::regex_emacs::{
    GapRange, char_byte8_p, code_of_stored, decode_stored, stored_char_is_ambiguous,
    stored_char_of, unibyte_to_char,
};

/// trivial_regexp_p: whether REGEXP (its bytes) matches just one constant
/// string.
pub(super) fn trivial_regexp_p(regexp: &[u8]) -> bool {
    let mut s = regexp.iter();
    while let Some(&c) = s.next() {
        match c {
            b'.' | b'*' | b'+' | b'?' | b'[' | b'^' | b'$' => return false,
            b'\\' => match s.next() {
                None => return false,
                Some(
                    b'|'
                    | b'('
                    | b')'
                    | b'`'
                    | b'\''
                    | b'b'
                    | b'B'
                    | b'<'
                    | b'>'
                    | b'w'
                    | b'W'
                    | b's'
                    | b'S'
                    | b'='
                    | b'{'
                    | b'}'
                    | b'_'
                    | b'c'
                    | b'C'
                    | b'1'..=b'9',
                ) => return false,
                Some(_) => {}
            },
            _ => {}
        }
    }
    true
}

/// TRANSLATE: TRT's character for D, D itself without a table.
#[inline]
fn translate(trt: Option<&Canon>, d: u32) -> u32 {
    trt.map_or(d, |trt| trt.get(d))
}

/// CHAR_HEAD_P.
#[inline(always)]
fn char_head_p(byte: u8) -> bool {
    byte & 0xC0 != 0x80
}

/// The searched text, a byte at a time (FETCH_BYTE), at offsets from
/// BEGV_BYTE.
trait SearchBytes {
    fn byte(&mut self, at: usize) -> u8;

    /// The bytes read in place around AT, from the offset returned with
    /// them: a scan reads them without a load each, as GNU's pointer
    /// runs to BUFFER_CEILING_OF.
    fn run(&mut self, at: usize) -> (&[u8], usize);
}

/// A multibyte buffer's text, or a unibyte one's that is all ASCII: its
/// stored bytes, read in place a part (before the gap, after it) at a
/// time.
struct StoredBytes<'a> {
    parts: GapRange<'a>,
    chunk: &'a [u8],
    start: usize,
}

impl<'a> StoredBytes<'a> {
    fn new(parts: GapRange<'a>) -> Self {
        StoredBytes {
            parts,
            chunk: &[],
            start: 0,
        }
    }

    #[inline(always)]
    fn ensure(&mut self, at: usize) {
        if at.wrapping_sub(self.start) >= self.chunk.len() {
            self.load(at);
        }
    }

    #[cold]
    #[inline(never)]
    fn load(&mut self, at: usize) {
        let (chunk, start) = self.parts.chunk_at_byte(at);
        self.chunk = chunk;
        self.start = start;
    }

    /// The stored character at AT and its length.  The parts hold whole
    /// characters.
    #[inline]
    fn stored(&mut self, at: usize) -> (char, usize) {
        self.ensure(at);
        decode_stored(&self.chunk[at - self.start..])
    }

    /// The start of the character that ends at AT (AT > 0).
    #[inline]
    fn prev_start(&mut self, at: usize) -> usize {
        self.ensure(at - 1);
        let mut i = at - 1 - self.start;
        while i > 0 && !char_head_p(self.chunk[i]) {
            i -= 1;
        }
        self.start + i
    }
}

impl SearchBytes for StoredBytes<'_> {
    #[inline(always)]
    fn byte(&mut self, at: usize) -> u8 {
        self.ensure(at);
        self.chunk[at - self.start]
    }

    #[inline]
    fn run(&mut self, at: usize) -> (&[u8], usize) {
        self.ensure(at);
        (self.chunk, self.start)
    }
}

/// A unibyte buffer's text holding bytes above 0x7F, which the text
/// stores as U+0080..U+00FF: a window of a part read back to its bytes,
/// at character offsets.
struct UnibyteBytes<'a> {
    parts: GapRange<'a>,
    /// The characters before the gap, counted at the first load.
    first_chars: Option<usize>,
    bytes: Vec<u8>,
    start: usize,
    /// The character and byte offsets of the window's start, which the
    /// next window's is counted from.
    window: (usize, usize),
}

/// The characters a window of `UnibyteBytes' reads back.
const UNIBYTE_WINDOW: usize = 4096;

impl<'a> UnibyteBytes<'a> {
    fn new(parts: GapRange<'a>) -> Self {
        UnibyteBytes {
            parts,
            first_chars: None,
            bytes: Vec::new(),
            start: 0,
            window: (0, 0),
        }
    }

    #[cold]
    #[inline(never)]
    fn load(&mut self, at: usize) {
        let (first, second) = self.parts.parts();
        let first_chars = *self
            .first_chars
            .get_or_insert_with(|| crate::buffer::count_chars(first));
        // The part holding AT, and its first character's offset.
        let (part, part_char, part_byte) = if at < first_chars || second.is_empty() {
            (first, 0, 0)
        } else {
            (second, first_chars, first.len())
        };
        // The window around AT, counted from the last window's start when
        // it is in this part, else from the part's.
        let wanted = at.saturating_sub(UNIBYTE_WINDOW / 2).max(part_char) - part_char;
        let (known_char, known_byte) = match self.window {
            (char, byte)
                if char >= part_char && byte >= part_byte && byte - part_byte <= part.len() =>
            {
                (char - part_char, byte - part_byte)
            }
            _ => (0, 0),
        };
        let byte = if wanted >= known_char {
            known_byte + crate::buffer::forward_bytes(&part[known_byte..], wanted - known_char)
        } else {
            crate::buffer::backward_bytes(part, known_byte, known_char - wanted)
        };
        self.window = (part_char + wanted, part_byte + byte);
        self.start = part_char + wanted;
        self.bytes.clear();
        // SAFETY: a part holds whole characters, from BYTE on too.
        let text = unsafe { std::str::from_utf8_unchecked(&part[byte..]) };
        self.bytes
            .extend(text.chars().take(UNIBYTE_WINDOW).map(|ch| {
                let code = code_of_stored(ch);
                crate::lisp::regex_emacs::char_to_unibyte(code).unwrap_or(code & 0xFF) as u8
            }));
    }
}

impl SearchBytes for UnibyteBytes<'_> {
    #[inline(always)]
    fn byte(&mut self, at: usize) -> u8 {
        if at.wrapping_sub(self.start) >= self.bytes.len() {
            self.load(at);
        }
        self.bytes[at - self.start]
    }

    #[inline]
    fn run(&mut self, at: usize) -> (&[u8], usize) {
        if at.wrapping_sub(self.start) >= self.bytes.len() {
            self.load(at);
        }
        (&self.bytes, self.start)
    }
}

/// How a search ended: the offsets of the last match when all N were
/// found, else the (negative) count that remained, search_buffer's -X.
enum Outcome {
    Found(usize, usize),
    Remaining(i64),
}

/// search_buffer_non_re: the position after the Nth occurrence of STRING
/// from POS (before it, searching backward), not past LIM, or -X when X
/// occurrences remain unfound.  RE says STRING is a trivial regexp, whose
/// backslashes quote the next character.
#[allow(clippy::too_many_arguments)]
pub(super) fn search_buffer_non_re(
    interp: &mut Interpreter,
    env: &mut Env,
    string: &Value,
    pos: usize,
    lim: usize,
    n: i64,
    re: bool,
    trt: Option<&Canon>,
    inverse_trt: Option<&Canon>,
) -> Result<i64, LispError> {
    let snapshot = BufferSnapshot::take(interp);
    let multibyte = snapshot.multibyte;
    let (bytes, string_multibyte, _) = string_bytes(string)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *string))?;
    // MULTIBYTE says whether the text to be searched is multibyte.  We
    // must convert PATTERN to match that, or we will not really find
    // things right.
    let mut raw_pattern = pattern_codes(bytes, string_multibyte);
    if multibyte && !string_multibyte {
        for c in &mut raw_pattern {
            *c = unibyte_to_char(*c);
        }
    } else if !multibyte && string_multibyte {
        // copy_text from multibyte to single-byte.
        for c in &mut raw_pattern {
            if *c >= 0x80 {
                *c &= 0xFF;
            }
        }
    }

    // Set to positive if we find a non-ASCII char that need translation.
    // Otherwise set to zero later.
    let mut char_base: i64 = -1;
    let mut boyer_moore_ok = true;
    let mut pat: Vec<u32> = Vec::with_capacity(raw_pattern.len());
    let mut base_pat = raw_pattern.iter().copied();
    if multibyte {
        // Fill PAT by translated characters in STRING while checking if we
        // can use boyer-moore search.  If TRT is non-nil, we can use
        // boyer-moore search only if TRT can be represented by the byte
        // array of 256 elements.  For that, all non-ASCII case-equivalents
        // of all case-sensitive characters in STRING must belong to the
        // same character group (two characters belong to the same group
        // iff their multibyte forms are the same except for the last byte;
        // i.e. every 64 characters form a group; U+0000..U+003F,
        // U+0040..U+007F, U+0080..U+00BF, ...).
        while let Some(mut c) = base_pat.next() {
            // If we got here and the RE flag is set, it's because we're
            // dealing with a regexp known to be trivial, so the backslash
            // just quotes the next character.
            if re && c == u32::from(b'\\') {
                c = base_pat
                    .next()
                    .expect("a trivial regexp ends in no backslash");
            }
            let translated = match (trt, inverse_trt) {
                (Some(trt), Some(inverse_trt)) => {
                    // Check if C has any other case-equivalents.
                    let mut inverse = inverse_trt.get(c);
                    // If so, check if we can use boyer-moore.
                    if c != inverse && boyer_moore_ok {
                        // Check if all equivalents belong to the same group
                        // of characters.  Note that the check of C itself is
                        // done by the last iteration.
                        let mut this_char_base: i64 = -1;
                        while boyer_moore_ok {
                            if inverse < 0x80 {
                                if this_char_base > 0 {
                                    boyer_moore_ok = false;
                                } else {
                                    this_char_base = 0;
                                }
                            } else if char_byte8_p(inverse) || inverse > MAX_UNICODE_CHAR {
                                // Boyer-moore search can't handle a
                                // translation of an eight-bit character;
                                // nor, here, of one with no stored form.
                                boyer_moore_ok = false;
                            } else if this_char_base < 0 {
                                this_char_base = i64::from(inverse & !0x3F);
                                if char_base < 0 {
                                    char_base = this_char_base;
                                } else if this_char_base != char_base {
                                    boyer_moore_ok = false;
                                }
                            } else if i64::from(inverse & !0x3F) != this_char_base {
                                boyer_moore_ok = false;
                            }
                            if c == inverse {
                                break;
                            }
                            inverse = inverse_trt.get(inverse);
                        }
                    }
                    trt.get(c)
                }
                (trt, _) => translate(trt, c),
            };
            // Store this character into the translated pattern.
            pat.push(translated);
        }
        // If char_base is still negative we didn't find any translated
        // non-ASCII characters.
        if char_base < 0 {
            char_base = 0;
        }
    } else {
        // Unibyte buffer.
        char_base = 0;
        while let Some(mut c) = base_pat.next() {
            if re && c == u32::from(b'\\') {
                c = base_pat
                    .next()
                    .expect("a trivial regexp ends in no backslash");
            }
            let translated = translate(trt, c);
            pat.push(translated & 0xFF);
            // Check that none of C's equivalents violates the assumptions
            // of boyer_moore.
            let mut inverse = translate(inverse_trt, c);
            loop {
                if inverse >= 0o200 {
                    boyer_moore_ok = false;
                    break;
                }
                if c == inverse {
                    break;
                }
                inverse = translate(inverse_trt, inverse);
            }
        }
    }

    // The pattern in the text's form: the stored bytes of a multibyte
    // pattern, whose characters must each have a stored form the text
    // cannot read otherwise for boyer_moore to compare bytes.
    let mut pat_bytes: Vec<u8> = Vec::with_capacity(pat.len());
    let mut buf = [0u8; 4];
    if multibyte {
        for &c in &pat {
            let stored = stored_char_of(c);
            match stored {
                Some(ch) if !(snapshot.side && stored_char_is_ambiguous(ch)) => {
                    pat_bytes.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                }
                _ => {
                    boyer_moore_ok = false;
                    // The text's stored form of such a character: a raw
                    // byte's marker or the placeholder, three bytes.
                    let ch = stored.unwrap_or(crate::lisp::regex_emacs::EXTENDED_PLACEHOLDER);
                    pat_bytes.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                }
            }
        }
    } else {
        pat_bytes.extend(pat.iter().map(|&c| c as u8));
    }
    let char_base = u32::try_from(char_base).expect("a character group");
    if char_base != 0 && char::from_u32(char_base).is_none() {
        boyer_moore_ok = false;
    }

    let parts = snapshot.range().parts;
    let size = parts.len_bytes();
    // A unibyte text's bytes above 0x7F are stored as two-byte sequences,
    // none of whose bytes is ASCII.  An ASCII pattern whose characters'
    // equivalents are all ASCII (boyer_moore's condition here) matches
    // exactly where it matches the stored bytes, which boyer_moore then
    // reads in place instead of each chunk converted back to its bytes.
    let unibyte_bytes =
        !multibyte && !snapshot.ascii && !(boyer_moore_ok && pat_bytes.iter().all(u8::is_ascii));
    let begv = snapshot.begv;
    let offset = |pos: usize| {
        if unibyte_bytes {
            pos - begv
        } else {
            snapshot.offset(pos)
        }
    };
    let buffer_ref = interp.buffer;
    let position = |offset: usize| {
        if unibyte_bytes {
            begv + offset
        } else {
            snapshot.position_in(&buffer_ref.borrow(), offset)
        }
    };
    let (pos_byte, lim_byte) = (offset(pos), offset(lim));
    let forward = n > 0;

    let (outcome, last_hit) = if boyer_moore_ok {
        let mut quit = || interp.maybe_quit(env);
        if unibyte_bytes {
            let mut text = UnibyteBytes::new(parts);
            let size = snapshot.zv - snapshot.begv;
            boyer_moore(
                &mut text,
                size,
                n,
                &pat_bytes,
                trt,
                inverse_trt,
                pos_byte,
                lim_byte,
                char_base,
                multibyte,
                &mut quit,
            )?
        } else {
            let mut text = StoredBytes::new(parts);
            boyer_moore(
                &mut text,
                size,
                n,
                &pat_bytes,
                trt,
                inverse_trt,
                pos_byte,
                lim_byte,
                char_base,
                multibyte,
                &mut quit,
            )?
        }
    } else if multibyte {
        let mut text = StoredBytes::new(parts);
        let buffer = interp.buffer.borrow();
        let mut side = |at: usize| buffer.extended_char_at(snapshot.position(at));
        let outcome = simple_search_multibyte(
            &mut text,
            snapshot.side,
            &mut side,
            n,
            &pat,
            pat_bytes.len(),
            trt,
            (pos, pos_byte),
            (lim, lim_byte),
        );
        (outcome, None)
    } else if unibyte_bytes {
        let mut text = UnibyteBytes::new(parts);
        (
            simple_search_unibyte(&mut text, n, &pat, trt, pos_byte, lim_byte),
            None,
        )
    } else {
        let mut text = StoredBytes::new(parts);
        (
            simple_search_unibyte(&mut text, n, &pat, trt, pos_byte, lim_byte),
            None,
        )
    };

    let set_search_regs = |interp: &mut Interpreter, (start, end): (usize, usize)| {
        if modify_match_data_allowed(interp) {
            let mut match_data = interp.last_match_data.take().unwrap_or_default();
            match_data.clear();
            match_data.push(Some((position(start), position(end))));
            interp.last_match_data = Some(match_data);
            interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
        }
    };
    match outcome {
        Outcome::Found(start, end) => {
            set_search_regs(interp, (start, end));
            Ok(position(if forward { end } else { start }) as i64)
        }
        Outcome::Remaining(remaining) => {
            // boyer_moore records each occurrence it finds as it goes.
            if let Some(hit) = last_hit {
                set_search_regs(interp, hit);
            }
            Ok(remaining)
        }
    }
}

/// simple_search in a multibyte buffer: N times for the characters PAT
/// (already translated), whose stored length is LEN_BYTE, from POS (a
/// position and its offset) until LIM.  This kind of search works
/// regardless of what is in PAT and regardless of what is in TRT.
#[allow(clippy::too_many_arguments)]
fn simple_search_multibyte(
    text: &mut StoredBytes<'_>,
    has_side: bool,
    side: &mut dyn FnMut(usize) -> Option<u32>,
    mut n: i64,
    pat: &[u32],
    len_byte: usize,
    trt: Option<&Canon>,
    (mut pos, mut pos_byte): (usize, usize),
    (lim, lim_byte): (usize, usize),
) -> Outcome {
    let len = pat.len();
    let forward = n > 0;
    // Number of buffer bytes matched.  Note that this may be different
    // from LEN_BYTE.
    let mut match_byte = 0usize;
    let mut char_at = |text: &mut StoredBytes<'_>, at: usize| {
        let (ch, charlen) = text.stored(at);
        let mut code = code_of_stored(ch);
        if has_side
            && stored_char_is_ambiguous(ch)
            && let Some(actual) = side(at)
        {
            code = actual;
        }
        (code, charlen)
    };

    'stop: {
        if lim > pos {
            while n > 0 {
                loop {
                    // Try matching at position POS.
                    let mut this_pos_byte = pos_byte;
                    let mut this_len = len;
                    if pos + len > lim || pos_byte + len_byte > lim_byte {
                        break 'stop;
                    }
                    let mut p = 0;
                    while this_len > 0 {
                        let pat_ch = pat[p];
                        let (buf_ch, buf_charlen) = char_at(text, this_pos_byte);
                        if translate(trt, buf_ch) != pat_ch {
                            break;
                        }
                        this_len -= 1;
                        p += 1;
                        this_pos_byte += buf_charlen;
                    }
                    if this_len == 0 {
                        match_byte = this_pos_byte - pos_byte;
                        pos += len;
                        pos_byte += match_byte;
                        break;
                    }
                    pos += 1;
                    pos_byte += text.stored(pos_byte).1;
                }
                n -= 1;
            }
        } else if lim < pos {
            // Backwards search.
            while n < 0 {
                loop {
                    // Try matching at position POS.
                    let mut this_pos = pos;
                    let mut this_pos_byte = pos_byte;
                    let mut this_len = len;
                    let mut p = len;
                    if this_pos < lim + len || pos_byte < lim_byte + len_byte {
                        break 'stop;
                    }
                    while this_len > 0 {
                        this_pos -= 1;
                        this_pos_byte = text.prev_start(this_pos_byte);
                        p -= 1;
                        let pat_ch = pat[p];
                        let (buf_ch, _) = char_at(text, this_pos_byte);
                        if translate(trt, buf_ch) != pat_ch {
                            break;
                        }
                        this_len -= 1;
                    }
                    if this_len == 0 {
                        match_byte = pos_byte - this_pos_byte;
                        pos = this_pos;
                        pos_byte = this_pos_byte;
                        break;
                    }
                    pos -= 1;
                    pos_byte = text.prev_start(pos_byte);
                }
                n += 1;
            }
        }
    }

    if n == 0 {
        if forward {
            Outcome::Found(pos_byte - match_byte, pos_byte)
        } else {
            Outcome::Found(pos_byte, pos_byte + match_byte)
        }
    } else if n > 0 {
        Outcome::Remaining(-n)
    } else {
        Outcome::Remaining(n)
    }
}

/// simple_search in a unibyte buffer, at offsets that are its positions.
fn simple_search_unibyte<B: SearchBytes>(
    text: &mut B,
    mut n: i64,
    pat: &[u32],
    trt: Option<&Canon>,
    mut pos: usize,
    lim: usize,
) -> Outcome {
    let len = pat.len();
    let forward = n > 0;

    'stop: {
        if lim > pos {
            while n > 0 {
                loop {
                    // Try matching at position POS.
                    let mut this_pos = pos;
                    let mut this_len = len;
                    if pos + len > lim {
                        break 'stop;
                    }
                    let mut p = 0;
                    while this_len > 0 {
                        let pat_ch = pat[p];
                        p += 1;
                        let buf_ch = u32::from(text.byte(this_pos));
                        if translate(trt, buf_ch) != pat_ch {
                            break;
                        }
                        this_len -= 1;
                        this_pos += 1;
                    }
                    if this_len == 0 {
                        pos += len;
                        break;
                    }
                    pos += 1;
                }
                n -= 1;
            }
        } else if lim < pos {
            while n < 0 {
                loop {
                    // Try matching at position POS.
                    if pos < lim + len {
                        break 'stop;
                    }
                    let mut this_pos = pos - len;
                    let mut this_len = len;
                    let mut p = 0;
                    while this_len > 0 {
                        let pat_ch = pat[p];
                        p += 1;
                        let buf_ch = u32::from(text.byte(this_pos));
                        if translate(trt, buf_ch) != pat_ch {
                            break;
                        }
                        this_len -= 1;
                        this_pos += 1;
                    }
                    if this_len == 0 {
                        pos -= len;
                        break;
                    }
                    pos -= 1;
                }
                n += 1;
            }
        }
    }

    if n == 0 {
        if forward {
            Outcome::Found(pos - len, pos)
        } else {
            Outcome::Found(pos, pos + len)
        }
    } else if n > 0 {
        Outcome::Remaining(-n)
    } else {
        Outcome::Remaining(n)
    }
}

/// The byte at AT, its last byte translated through SIMPLE_TRANSLATE when
/// it ends a character the tables cover (an ASCII one, or one of
/// CHAR_BASE's group, whose leading bytes are PREV).
#[inline(always)]
fn translated_byte<B: SearchBytes>(
    text: &mut B,
    at: usize,
    size: usize,
    multibyte: bool,
    prev: [u8; 3],
    simple_translate: &[u8; 256],
) -> u8 {
    let byte = text.byte(at);
    // Translate only the last byte of a character.
    let last = !multibyte
        || ((at + 1 == size || char_head_p(text.byte(at + 1)))
            && (char_head_p(byte)
                // Check if this is the last byte of a translatable
                // character.
                || (prev[0] == text.byte(at - 1)
                    && (char_head_p(prev[0])
                        || (prev[1] == text.byte(at - 2)
                            && (char_head_p(prev[1]) || prev[2] == text.byte(at - 3)))))));
    if last {
        simple_translate[usize::from(byte)]
    } else {
        byte
    }
}

/// boyer_moore: N times for the bytes BASE_PAT (already translated by TRT)
/// from offset POS_BYTE until LIM_BYTE, in a text of SIZE bytes.  This
/// kind of search works if all the characters in BASE_PAT that have
/// nontrivial translation are the same aside from the last byte, so just
/// the last byte of a character is translated after a simple test of the
/// context; CHAR_BASE is nonzero if there is such a non-ASCII character.
/// Also the offsets of the last occurrence found, which GNU records as
/// the match data each time.
#[allow(clippy::too_many_arguments)]
fn boyer_moore<B: SearchBytes>(
    text: &mut B,
    size: usize,
    mut n: i64,
    base_pat: &[u8],
    trt: Option<&Canon>,
    inverse_trt: Option<&Canon>,
    pos_byte: usize,
    lim_byte: usize,
    char_base: u32,
    multibyte: bool,
    quit: &mut dyn FnMut() -> Result<(), LispError>,
) -> Result<(Outcome, Option<(usize, usize)>), LispError> {
    let direction: isize = if n > 0 { 1 } else { -1 };
    let len_byte = base_pat.len() as isize;
    let dirlen = len_byte * direction;
    let mut stride_for_teases: isize = 0;
    let mut last_hit = None;

    // These are set to the preceding bytes of a byte to be translated if
    // char_base is nonzero.  The stored form of a character is at most four
    // bytes, so at most three previous bytes are checked.
    let mut prev = [0u8; 3];

    // A character that does not appear in the pattern induces a stride
    // equal to the pattern length.
    let mut bm_tab = [dirlen; 256];
    // We use this for translation, instead of TRT itself.  We fill this in
    // to handle the characters that actually occur in the pattern.  Others
    // don't matter anyway!
    let mut simple_translate: [u8; 256] = std::array::from_fn(|i| i as u8);

    // Record position after the end of the pattern.
    let pat_end = len_byte;
    // BASE is the index of the character we start scanning from: the first
    // in a forward search, the last in a backward search.
    let base: isize = if direction < 0 { pat_end - 1 } else { 0 };
    let pat = |i: isize| base_pat[(base + i) as usize];

    if char_base != 0 {
        // Setup PREV from CHAR_BASE.  Only a byte following them is the
        // target of translation.
        let mut str = [0u8; 4];
        let str = char::from_u32(char_base)
            .expect("a stored character group")
            .encode_utf8(&mut str)
            .as_bytes();
        let cblen = str.len();
        prev[0] = str[cblen - 2];
        if cblen > 2 {
            prev[1] = str[cblen - 3];
            if cblen > 3 {
                prev[2] = str[cblen - 4];
            }
        }
    }

    let mut i: isize = 0;
    while i != dirlen {
        let ptr = (base + i) as usize;
        i += direction;
        if let Some(inverse_trt) = inverse_trt.filter(|_| trt.is_some()) {
            // If the byte currently looking at is the last of a character
            // to check case-equivalents, set CH to that character.  An
            // ASCII character and a non-ASCII character matching with
            // CHAR_BASE are to be checked.
            let mut ch: i64 = -1;
            if base_pat[ptr] < 0x80 || !multibyte {
                ch = i64::from(base_pat[ptr]);
            } else if char_base != 0
                && (pat_end - ptr as isize == 1 || char_head_p(base_pat[ptr + 1]))
            {
                let mut charstart = ptr - 1;
                while !char_head_p(base_pat[charstart]) {
                    charstart -= 1;
                }
                ch = i64::from(code_of_stored(decode_stored(&base_pat[charstart..]).0));
                if i64::from(char_base) != (ch & !0x3F) {
                    ch = -1;
                }
            }
            let mut j = if ch >= 0o200 && multibyte {
                ((ch & 0x3F) | 0o200) as usize
            } else {
                usize::from(base_pat[ptr])
            };
            if i == dirlen {
                stride_for_teases = bm_tab[j];
            }
            bm_tab[j] = dirlen - i;
            // A translation table is accompanied by its inverse -- see
            // comment following downcase_table for details.
            if ch >= 0 {
                let starting_ch = ch;
                let starting_j = j;
                loop {
                    ch = i64::from(inverse_trt.get(ch as u32));
                    j = if ch >= 0o200 && multibyte {
                        ((ch & 0x3F) | 0o200) as usize
                    } else {
                        ch as usize & 0xFF
                    };
                    // For all the characters that map into CH, set up
                    // simple_translate to map the last byte into
                    // STARTING_J.
                    simple_translate[j] = starting_j as u8;
                    if ch == starting_ch {
                        break;
                    }
                    bm_tab[j] = dirlen - i;
                }
            }
        } else {
            let j = usize::from(base_pat[ptr]);
            if i == dirlen {
                stride_for_teases = bm_tab[j];
            }
            bm_tab[j] = dirlen - i;
        }
        // stride_for_teases tells how much to stride if we get a match on
        // the far character but are subsequently disappointed, by recording
        // what the stride would have been for that character if the last
        // character had been different.
    }

    let size = size as isize;
    let lim_byte = lim_byte as isize;
    let forward_one = if direction > 0 { 1 } else { 0 };
    let mut pos_byte = pos_byte as isize + dirlen - forward_one;
    let found = |position: isize| (position as usize, (position + len_byte) as usize);
    // Loop invariant: POS_BYTE points at where last char (first char if
    // reverse) of pattern would align in a possible match.
    while n != 0 {
        if (lim_byte - pos_byte - forward_one) * direction < 0 {
            return Ok((Outcome::Remaining(n * -(direction as i64)), last_hit));
        }
        // First we do the part we can by pointers (maybe nothing): a run
        // of bytes on one side of the gap at a time, as BUFFER_CEILING_OF
        // and BUFFER_FLOOR_OF bound it, and LIMIT the last value POS_BYTE
        // can take on.
        quit()?;
        let limit = if direction > 0 {
            (size - 1).min(pos_byte + 20000).min(lim_byte - 1)
        } else {
            0.max(pos_byte - 20000).max(lim_byte)
        };
        if (limit - pos_byte) * direction > 20 {
            let p_limit = limit;
            let mut cursor = pos_byte;
            // Use one cursor setting as long as I can.
            loop {
                let mut hit = false;
                // The strides, a run of bytes in place at a time.
                if direction > 0 {
                    'scan: while cursor <= p_limit {
                        let (run, start) = text.run(cursor as usize);
                        let run = &run[..run.len().min(p_limit as usize + 1 - start)];
                        let mut at = cursor as usize - start;
                        while let Some(&byte) = run.get(at) {
                            let stride = bm_tab[usize::from(byte)];
                            if stride == 0 {
                                cursor = (start + at) as isize;
                                hit = true;
                                break 'scan;
                            }
                            at += stride as usize;
                        }
                        cursor = (start + at) as isize;
                    }
                } else {
                    'scan: while cursor >= p_limit {
                        let (run, start) = text.run(cursor as usize);
                        let first = (p_limit as usize).max(start) as isize;
                        let start = start as isize;
                        while cursor >= first {
                            let stride = bm_tab[usize::from(run[(cursor - start) as usize])];
                            if stride == 0 {
                                hit = true;
                                break 'scan;
                            }
                            cursor += stride;
                        }
                    }
                }
                // If you are here without a hit, cursor is beyond the end
                // of the searched region.
                if !hit {
                    break;
                }
                let mut i = dirlen - direction;
                if trt.is_some() {
                    loop {
                        i -= direction;
                        if i + direction == 0 {
                            break;
                        }
                        cursor -= direction;
                        let ch = translated_byte(
                            text,
                            cursor as usize,
                            size as usize,
                            multibyte,
                            prev,
                            &simple_translate,
                        );
                        if pat(i) != ch {
                            break;
                        }
                    }
                } else {
                    loop {
                        i -= direction;
                        if i + direction == 0 {
                            break;
                        }
                        cursor -= direction;
                        if pat(i) != text.byte(cursor as usize) {
                            break;
                        }
                    }
                }
                // Fix cursor.
                cursor += dirlen - i - direction;
                if i + direction == 0 {
                    cursor -= direction;
                    let position = cursor + if direction > 0 { 1 - len_byte } else { 0 };
                    last_hit = Some(found(position));
                    n -= direction as i64;
                    if n != 0 {
                        // To resume search.
                        cursor += dirlen;
                    } else {
                        let (start, end) = found(position);
                        return Ok((Outcome::Found(start, end), last_hit));
                    }
                } else {
                    // <sigh> we lose -
                    cursor += stride_for_teases;
                }
            }
            pos_byte = cursor;
        } else {
            // Now we'll pick up a clump that has to be done the hard way
            // because it is short.
            let limit = if direction > 0 {
                (size - 1 + len_byte).min(lim_byte - 1)
            } else {
                (0 - len_byte).max(lim_byte)
            };
            // LIMIT is now the last value POS_BYTE can have and still be
            // valid for a possible match.
            loop {
                // This loop can be coded for space rather than speed
                // because it will usually run only once.
                let mut hit = false;
                while (limit - pos_byte) * direction >= 0 {
                    let stride = bm_tab[usize::from(text.byte(pos_byte as usize))];
                    if stride == 0 {
                        hit = true;
                        break;
                    }
                    pos_byte += stride;
                }
                if !hit {
                    // Ran off the end.
                    break;
                }
                // Found what might be a match.
                let mut i = dirlen - direction;
                loop {
                    i -= direction;
                    if i + direction == 0 {
                        break;
                    }
                    pos_byte -= direction;
                    let ch = translated_byte(
                        text,
                        pos_byte as usize,
                        size as usize,
                        multibyte,
                        prev,
                        &simple_translate,
                    );
                    if pat(i) != ch {
                        break;
                    }
                }
                // Above loop has moved POS_BYTE part or all the way back to
                // the first pos (last pos if reverse).  Set it once again at
                // the last (first if reverse) char.
                pos_byte += dirlen - i - direction;
                if i + direction == 0 {
                    pos_byte -= direction;
                    let position = pos_byte + if direction > 0 { 1 - len_byte } else { 0 };
                    last_hit = Some(found(position));
                    n -= direction as i64;
                    if n != 0 {
                        // To resume search.
                        pos_byte += dirlen;
                    } else {
                        let (start, end) = found(position);
                        return Ok((Outcome::Found(start, end), last_hit));
                    }
                } else {
                    pos_byte += stride_for_teases;
                }
            }
        }
        // We have done one clump.  Can we continue?
        if (lim_byte - pos_byte) * direction < 0 {
            return Ok((Outcome::Remaining(-n * direction as i64), last_hit));
        }
    }
    let at = pos_byte as usize;
    Ok((Outcome::Found(at, at), last_hit))
}
