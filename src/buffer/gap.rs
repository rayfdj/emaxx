//! insdel.c's buffer text: the text's bytes in one allocation with a gap
//! where the last change was made.  An insertion moves the gap to its
//! position and copies the new bytes into it; a deletion widens it over
//! the deleted bytes.  The text before the gap and the text after it are
//! each whole characters, as GNU keeps the gap at a character boundary,
//! and a scan reads them as re_search_2 reads its two strings.
//!
//! A scan that must not hold the buffer (Lisp may run while it reads)
//! shares the bytes: a change while one does copies them first.

use std::rc::Rc;

/// buffer.h GAP_BYTES_DFL: what a growth of the gap adds beyond the
/// insertion that needed it.
const GAP_BYTES_DFL: usize = 2000;

/// A buffer's text: the bytes before the gap, the gap, the bytes after,
/// with the character counts of the whole and of the part before the gap
/// (Z and GPT beside Z_BYTE and GPT_BYTE).
#[derive(Clone, Default)]
pub struct GapText {
    data: Rc<GapData>,
}

#[derive(Clone, Default)]
struct GapData {
    bytes: Vec<u8>,
    /// GPT_BYTE - BEG_BYTE: where the gap starts.
    gap_start: usize,
    /// Where the text after the gap starts.
    gap_end: usize,
    /// GPT - BEG: the characters before the gap.
    gap_char: usize,
    /// Z - BEG.
    chars: usize,
}

impl GapData {
    #[inline]
    fn len(&self) -> usize {
        self.bytes.len() - (self.gap_end - self.gap_start)
    }

    /// gap_left and gap_right: the gap moved to byte AT of the text, the
    /// character AT_CHAR.
    fn move_gap(&mut self, at: usize, at_char: usize) {
        if at < self.gap_start {
            let count = self.gap_start - at;
            self.bytes
                .copy_within(at..self.gap_start, self.gap_end - count);
            self.gap_start = at;
            self.gap_end -= count;
        } else if at > self.gap_start {
            let count = at - self.gap_start;
            self.bytes
                .copy_within(self.gap_end..self.gap_end + count, self.gap_start);
            self.gap_start = at;
            self.gap_end += count;
        }
        self.gap_char = at_char;
    }

    /// make_gap_larger: a gap of at least NEEDED bytes, grown by what the
    /// insertion needs and GAP_BYTES_DFL, or by a quarter of the text
    /// when that is more, so that a long run of insertions copies the
    /// text a bounded number of times (GNU leaves that to realloc).
    fn make_gap(&mut self, needed: usize) {
        let gap = self.gap_end - self.gap_start;
        if gap >= needed {
            return;
        }
        let added = (needed - gap + GAP_BYTES_DFL).max(self.len() / 4);
        let after = self.bytes.len() - self.gap_end;
        self.bytes.resize(self.bytes.len() + added, 0);
        // The text after the gap moves to the end of the larger space.
        let new_end = self.gap_end + added;
        self.bytes
            .copy_within(self.gap_end..self.gap_end + after, new_end);
        self.gap_end = new_end;
    }
}

/// The characters of TEXT.
#[inline]
pub(crate) fn count_chars(text: &[u8]) -> usize {
    // SAFETY: only the counts of the bytes are read; str_indices counts
    // the bytes that do not continue a character, which a slice of whole
    // characters has one of each.
    ropey::str_utils::byte_to_char_idx(unsafe { std::str::from_utf8_unchecked(text) }, text.len())
}

/// The byte offset of the character N characters into TEXT (its length
/// when it has fewer).
#[inline]
pub(crate) fn forward_bytes(text: &[u8], n: usize) -> usize {
    // SAFETY: as `count_chars'.
    ropey::str_utils::char_to_byte_idx(unsafe { std::str::from_utf8_unchecked(text) }, n)
}

/// The byte offset of the character N characters before offset AT of
/// TEXT, AT a character's start.
#[inline]
pub(crate) fn backward_bytes(text: &[u8], mut at: usize, n: usize) -> usize {
    for _ in 0..n {
        at -= 1;
        while text[at] & 0xC0 == 0x80 {
            at -= 1;
        }
    }
    at
}

impl GapText {
    /// TEXT, with no gap.
    pub(crate) fn from_str(text: &str) -> Self {
        let bytes = text.as_bytes().to_vec();
        let len = bytes.len();
        let chars = count_chars(text.as_bytes());
        GapText {
            data: Rc::new(GapData {
                bytes,
                gap_start: len,
                gap_end: len,
                gap_char: chars,
                chars,
            }),
        }
    }

    /// Z_BYTE - BEG_BYTE.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Z - BEG.
    #[inline]
    pub(crate) fn chars(&self) -> usize {
        self.data.chars
    }

    /// Whether each character is one byte.
    #[inline]
    pub(crate) fn is_ascii(&self) -> bool {
        self.data.chars == self.data.len()
    }

    /// GPT_BYTE - BEG_BYTE.
    #[inline]
    pub(crate) fn gap_byte(&self) -> usize {
        self.data.gap_start
    }

    /// GPT - BEG.
    #[inline]
    pub(crate) fn gap_char(&self) -> usize {
        self.data.gap_char
    }

    /// The text before the gap and the text after it.
    #[inline]
    pub(crate) fn halves(&self) -> (&str, &str) {
        let data = &*self.data;
        // SAFETY: each half holds whole characters of the valid UTF-8 the
        // text was made and changed with: the gap only ever stands at a
        // character boundary.
        unsafe {
            (
                std::str::from_utf8_unchecked(&data.bytes[..data.gap_start]),
                std::str::from_utf8_unchecked(&data.bytes[data.gap_end..]),
            )
        }
    }

    /// The byte at AT.
    #[inline]
    pub(crate) fn byte(&self, at: usize) -> u8 {
        let data = &*self.data;
        if at < data.gap_start {
            data.bytes[at]
        } else {
            data.bytes[at + (data.gap_end - data.gap_start)]
        }
    }

    /// The half holding byte AT, with the byte and character offsets of
    /// its start; for AT at the end, the half the end is in.
    #[inline]
    pub(crate) fn chunk_at_byte(&self, at: usize) -> (&str, usize, usize) {
        let (before, after) = self.halves();
        if at < before.len() || after.is_empty() {
            (before, 0, 0)
        } else {
            (after, before.len(), self.data.gap_char)
        }
    }

    /// The half holding character INDEX, as `chunk_at_byte'.
    #[inline]
    pub(crate) fn chunk_at_char(&self, index: usize) -> (&str, usize, usize) {
        let (before, after) = self.halves();
        if index < self.data.gap_char || after.is_empty() {
            (before, 0, 0)
        } else {
            (after, before.len(), self.data.gap_char)
        }
    }

    /// The bytes of byte RANGE before the gap and after it.  The range's
    /// ends are character boundaries.
    #[inline]
    pub(crate) fn range_parts(&self, range: std::ops::Range<usize>) -> (&[u8], &[u8]) {
        let data = &*self.data;
        let split = data.gap_start;
        let first = &data.bytes[range.start.min(split)..range.end.min(split)];
        let second = &data.bytes[data.gap_end + (range.start.max(split) - split)
            ..data.gap_end + (range.end.max(split) - split)];
        (first, second)
    }

    /// `range_parts' as text.
    #[inline]
    pub(crate) fn range_halves(&self, range: std::ops::Range<usize>) -> (&str, &str) {
        debug_assert!(self.is_boundary(range.start) && self.is_boundary(range.end));
        let (first, second) = self.range_parts(range);
        // SAFETY: the range's ends and the gap are character boundaries of
        // the text's valid UTF-8.
        unsafe {
            (
                std::str::from_utf8_unchecked(first),
                std::str::from_utf8_unchecked(second),
            )
        }
    }

    /// Whether byte AT starts a character or ends the text.
    fn is_boundary(&self, at: usize) -> bool {
        at >= self.len() || self.byte(at) & 0xC0 != 0x80
    }

    /// The text of byte RANGE.
    pub(crate) fn string(&self, range: std::ops::Range<usize>) -> String {
        let (first, second) = self.range_halves(range);
        let mut text = String::with_capacity(first.len() + second.len());
        text.push_str(first);
        text.push_str(second);
        text
    }

    /// The characters of byte RANGE.
    pub(crate) fn chars_in(&self, range: std::ops::Range<usize>) -> usize {
        if self.is_ascii() {
            return range.len();
        }
        let (first, second) = self.range_halves(range);
        count_chars(first.as_bytes()) + count_chars(second.as_bytes())
    }

    /// The known positions conversions count from: the text's start, the
    /// gap and the end (marker.c's BEG, GPT and Z).
    #[inline]
    fn known(&self) -> [(usize, usize); 3] {
        let data = &*self.data;
        [
            (0, 0),
            (data.gap_char, data.gap_start),
            (data.chars, data.len()),
        ]
    }

    /// buf_charpos_to_bytepos: the byte offset of char INDEX, counted from
    /// the nearest of the known positions and the pairs of MORE.
    pub(crate) fn char_to_byte_near(
        &self,
        index: usize,
        more: impl Iterator<Item = (usize, usize)>,
    ) -> usize {
        self.char_to_byte_counting(index, more).0
    }

    /// `char_to_byte_near', and how many characters it counted: as
    /// buf_charpos_to_bytepos CONSIDERs each known position, the nearest
    /// below INDEX and above it bracket it, and when the text between them
    /// is all one-byte characters (as many bytes as characters) the
    /// offset is interpolated; else it is counted from the nearer.
    pub(crate) fn char_to_byte_counting(
        &self,
        index: usize,
        more: impl Iterator<Item = (usize, usize)>,
    ) -> (usize, usize) {
        if self.is_ascii() {
            return (index, 0);
        }
        let mut below = (0, 0);
        let mut above = (self.data.chars, self.data.len());
        for (char, byte) in self.known().into_iter().chain(more) {
            if char == index {
                return (byte, 0);
            } else if char > index {
                if char < above.0 {
                    above = (char, byte);
                }
            } else if char > below.0 {
                below = (char, byte);
            }
        }
        if above.0 - below.0 == above.1 - below.1 {
            return (below.1 + (index - below.0), 0);
        }
        let from = if index - below.0 < above.0 - index {
            below
        } else {
            above
        };
        (self.char_to_byte_from(from, index), index.abs_diff(from.0))
    }

    /// `char_to_byte_near' with the known positions alone.
    #[inline]
    pub(crate) fn char_to_byte(&self, index: usize) -> usize {
        self.char_to_byte_near(index, std::iter::empty())
    }

    /// The byte offset of char INDEX, counted from FROM, a known pair of
    /// a character and its byte offset.
    fn char_to_byte_from(&self, (mut from, mut from_byte): (usize, usize), index: usize) -> usize {
        let data = &*self.data;
        let (before, after) = self.halves();
        let (before, after) = (before.as_bytes(), after.as_bytes());
        if index >= from {
            if from_byte < data.gap_start {
                if index - from <= data.gap_char - from {
                    return from_byte + forward_bytes(&before[from_byte..], index - from);
                }
                (from, from_byte) = (data.gap_char, data.gap_start);
            }
            let at = from_byte - data.gap_start;
            from_byte + forward_bytes(&after[at..], index - from)
        } else {
            if from_byte > data.gap_start {
                if index >= data.gap_char {
                    let at = from_byte - data.gap_start;
                    return data.gap_start + backward_bytes(after, at, from - index);
                }
                (from, from_byte) = (data.gap_char, data.gap_start);
            }
            backward_bytes(before, from_byte, from - index)
        }
    }

    /// buf_bytepos_to_charpos: the character byte offset BYTE starts or
    /// lies in, counted from the nearest of the known positions and the
    /// pairs of MORE.
    pub(crate) fn byte_to_char_near(
        &self,
        byte: usize,
        more: impl Iterator<Item = (usize, usize)>,
    ) -> usize {
        self.byte_to_char_counting(byte, more).0
    }

    /// `byte_to_char_near', and how many bytes it counted, bracketing BYTE
    /// as `char_to_byte_counting' brackets a character.
    pub(crate) fn byte_to_char_counting(
        &self,
        byte: usize,
        more: impl Iterator<Item = (usize, usize)>,
    ) -> (usize, usize) {
        if self.is_ascii() {
            return (byte, 0);
        }
        let mut below = (0, 0);
        let mut above = (self.data.chars, self.data.len());
        for (char, at) in self.known().into_iter().chain(more) {
            if at == byte {
                return (char, 0);
            } else if at > byte {
                if at < above.1 {
                    above = (char, at);
                }
            } else if at > below.1 {
                below = (char, at);
            }
        }
        if above.0 - below.0 == above.1 - below.1 {
            return (below.0 + (byte - below.1), 0);
        }
        let (from, from_byte) = if byte - below.1 < above.1 - byte {
            below
        } else {
            above
        };
        let char = if byte >= from_byte {
            from + self.chars_in(from_byte..byte)
        } else {
            from - self.chars_in(byte..from_byte)
        };
        (char, byte.abs_diff(from_byte))
    }

    /// `byte_to_char_near' with the known positions alone.
    #[inline]
    pub(crate) fn byte_to_char(&self, byte: usize) -> usize {
        self.byte_to_char_near(byte, std::iter::empty())
    }

    /// The bytes, for a change: copied first while a scan shares them.
    #[inline]
    fn data_mut(&mut self) -> &mut GapData {
        Rc::make_mut(&mut self.data)
    }

    /// insert_1_both's move of the gap and copy: TEXT, of CHARS
    /// characters, inserted at byte AT, the character AT_CHAR.
    pub(crate) fn insert(&mut self, at: usize, at_char: usize, text: &str, chars: usize) {
        if text.is_empty() {
            return;
        }
        debug_assert!(at <= self.len() && at_char <= self.chars());
        let data = self.data_mut();
        data.move_gap(at, at_char);
        data.make_gap(text.len());
        data.bytes[data.gap_start..data.gap_start + text.len()].copy_from_slice(text.as_bytes());
        data.gap_start += text.len();
        data.gap_char += chars;
        data.chars += chars;
    }

    /// del_range_2's widening of the gap over byte RANGE, the characters
    /// CHARS: the gap moved next to the range or inside it, then over it.
    pub(crate) fn delete(&mut self, range: std::ops::Range<usize>, chars: std::ops::Range<usize>) {
        if range.is_empty() {
            return;
        }
        debug_assert!(range.end <= self.len() && chars.end <= self.chars());
        let data = self.data_mut();
        if data.gap_start < range.start {
            data.move_gap(range.start, chars.start);
        } else if data.gap_start > range.end {
            data.move_gap(range.end, chars.end);
        }
        data.gap_end += range.end - data.gap_start;
        data.gap_start = range.start;
        data.gap_char = chars.start;
        data.chars -= chars.len();
    }

    /// Whether a scan shares the bytes.
    #[cfg(test)]
    fn shared(&self) -> bool {
        Rc::strong_count(&self.data) > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(gap: &GapText) -> String {
        gap.string(0..gap.len())
    }

    #[test]
    fn insertions_and_deletions_keep_the_text_around_the_gap() {
        let mut gap = GapText::from_str("hello world");
        gap.insert(5, 5, ",", 1);
        assert_eq!(text(&gap), "hello, world");
        assert_eq!((gap.gap_byte(), gap.gap_char()), (6, 6));
        gap.insert(0, 0, "\u{e9}", 1);
        assert_eq!(text(&gap), "\u{e9}hello, world");
        assert_eq!((gap.len(), gap.chars()), (14, 13));
        gap.delete(2..4, 1..3);
        assert_eq!(text(&gap), "\u{e9}llo, world");
        gap.delete(9..12, 8..11);
        assert_eq!(text(&gap), "\u{e9}llo, wo");
        let end = gap.len();
        gap.insert(end, gap.chars(), "!", 1);
        assert_eq!(text(&gap), "\u{e9}llo, wo!");
        let (before, after) = gap.halves();
        assert_eq!(format!("{before}{after}"), "\u{e9}llo, wo!");
        assert_eq!(gap.byte(0), 0xC3);
        assert_eq!(gap.byte(gap.len() - 1), b'!');
        assert_eq!(gap.range_halves(2..4), ("ll", ""));
    }

    #[test]
    fn conversions_count_across_the_gap_from_the_nearest_known_position() {
        let source = "a\u{e9}b\u{4e2d}c\u{1f600}d";
        for split in 0..=7 {
            let mut gap = GapText::from_str(source);
            // A deletion and reinsertion that leave the gap after the
            // SPLITth character.
            let byte = source
                .char_indices()
                .nth(split)
                .map_or(source.len(), |(b, _)| b);
            let next = source[byte..].chars().next();
            if let Some(next) = next {
                let end = byte + next.len_utf8();
                gap.delete(byte..end, split..split + 1);
                gap.insert(byte, split, &source[byte..end], 1);
                gap.delete(end..end, split + 1..split + 1);
            }
            assert_eq!(text(&gap), source);
            for (index, (at, _)) in source
                .char_indices()
                .chain([(source.len(), ' ')])
                .enumerate()
            {
                assert_eq!(gap.char_to_byte(index), at, "split {split} char {index}");
                assert_eq!(gap.byte_to_char(at), index, "split {split} byte {at}");
                for known in 0..=7 {
                    let known_byte = source
                        .char_indices()
                        .nth(known)
                        .map_or(source.len(), |(b, _)| b);
                    assert_eq!(
                        gap.char_to_byte_near(index, std::iter::once((known, known_byte))),
                        at
                    );
                    assert_eq!(
                        gap.byte_to_char_near(at, std::iter::once((known, known_byte))),
                        index
                    );
                }
            }
        }
    }

    #[test]
    fn a_long_run_of_insertions_grows_the_gap_geometrically() {
        let mut gap = GapText::default();
        for n in 0..100_000 {
            gap.insert(n, n, "x", 1);
        }
        assert_eq!((gap.len(), gap.chars()), (100_000, 100_000));
        assert!(gap.data.bytes.len() < 200_000);
    }

    #[test]
    fn a_change_while_a_scan_shares_the_bytes_copies_them() {
        let mut gap = GapText::from_str("abc");
        let scan = gap.clone();
        assert!(gap.shared());
        gap.insert(1, 1, "x", 1);
        assert_eq!(text(&gap), "axbc");
        assert_eq!(text(&scan), "abc");
        assert!(!gap.shared());
    }
}
