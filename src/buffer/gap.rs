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

/// buffer.c:Fget_buffer_create's BUF_GAP_SIZE of a new buffer.
const NEW_BUFFER_GAP: usize = 20;

/// A buffer's text, buffer.h's struct buffer_text's beg, gpt, z,
/// gpt_byte, z_byte and gap_size: the bytes before the gap, the gap and
/// the bytes after it in one allocation, which a scan shares (GNU's
/// `inhibit_shrinking' keeps its own from moving), and the positions,
/// counted from BEG, of the gap and the end.
///
/// The text owns its bytes through the Rc; a `GapText<&[u8]>' is a view
/// of them (`view'), which a matcher reads in place.
#[derive(Clone, Copy, Default)]
pub struct GapText<B = Rc<Vec<u8>>> {
    /// BEG_ADDR's bytes, the gap included.
    beg: B,
    /// GPT - BEG: the characters before the gap.
    gpt: usize,
    /// GPT_BYTE - BEG_BYTE: where the gap starts.
    gpt_byte: usize,
    /// Z - BEG.
    z: usize,
    /// Z_BYTE - BEG_BYTE.
    z_byte: usize,
    /// GAP_SIZE.
    gap_size: usize,
}

/// Where a gap text's bytes are: its own allocation, or a view of one.
pub trait GapBytes {
    fn gap_bytes(&self) -> &[u8];
}

impl GapBytes for Rc<Vec<u8>> {
    #[inline(always)]
    fn gap_bytes(&self) -> &[u8] {
        self
    }
}

impl GapBytes for &[u8] {
    #[inline(always)]
    fn gap_bytes(&self) -> &[u8] {
        self
    }
}

/// The byte offsets of a text's newlines, kept until the text changes.
/// Scans count the bytes they read; once they have read as many as the
/// text holds, one more pass records every newline and later questions
/// about lines are answered from the record.  A text that changes
/// between each question never has one made.
#[derive(Clone, Default)]
pub(crate) struct NewlineIndex {
    scanned: std::cell::Cell<usize>,
    offsets: std::cell::OnceCell<Rc<[u32]>>,
}

impl NewlineIndex {
    /// The byte offsets of TEXT's newlines when they are recorded,
    /// recording them first once the scans since the last change have
    /// read as many bytes as the text holds; None while scanning is the
    /// cheaper answer.
    pub(crate) fn offsets(&self, text: &GapText) -> Option<&[u32]> {
        if let Some(offsets) = self.offsets.get() {
            return Some(offsets);
        }
        let len = text.len();
        if self.scanned.get() < len || u32::try_from(len).is_err() {
            return None;
        }
        let (first, second) = text.halves();
        let offsets = memchr::memchr_iter(b'\n', first.as_bytes())
            .chain(memchr::memchr_iter(b'\n', second.as_bytes()).map(|at| at + first.len()))
            .map(|at| at as u32)
            .collect();
        Some(self.offsets.get_or_init(|| offsets))
    }

    /// Count BYTES read by a scan for newlines toward recording them.
    #[inline]
    pub(crate) fn note_scan(&self, bytes: usize) {
        let scanned = &self.scanned;
        scanned.set(scanned.get().saturating_add(bytes));
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
    /// A new buffer's text (Fget_buffer_create's 20-byte gap) with TEXT
    /// inserted, the gap after it as make_gap_larger leaves it.
    pub(crate) fn from_str(text: &str) -> Self {
        let len = text.len();
        let gap = if len <= NEW_BUFFER_GAP {
            NEW_BUFFER_GAP - len
        } else {
            GAP_BYTES_DFL
        };
        let mut bytes = Vec::with_capacity(len + gap);
        bytes.extend_from_slice(text.as_bytes());
        bytes.resize(len + gap, 0);
        let chars = count_chars(text.as_bytes());
        GapText {
            beg: Rc::new(bytes),
            gpt: chars,
            gpt_byte: len,
            z: chars,
            z_byte: len,
            gap_size: gap,
        }
    }

    /// The text as a view of its bytes.
    #[inline]
    pub(crate) fn view(&self) -> GapText<&[u8]> {
        GapText {
            beg: &self.beg[..],
            gpt: self.gpt,
            gpt_byte: self.gpt_byte,
            z: self.z,
            z_byte: self.z_byte,
            gap_size: self.gap_size,
        }
    }

    /// `view' with no lifetime of its own: a reader's that holds the bytes
    /// in place while nothing changes the text.
    ///
    /// # Safety
    /// The text does not change, and is not dropped, while the view is
    /// used, unless a share of its bytes (`share_bytes') taken before the
    /// first change keeps them.
    #[inline]
    pub(crate) unsafe fn view_unbounded<'a>(&self) -> GapText<&'a [u8]> {
        GapText {
            // SAFETY: as the caller's contract: the allocation stays where
            // it is, unchanged, while the view is used.
            beg: unsafe { &*std::ptr::from_ref::<[u8]>(&self.beg[..]) },
            gpt: self.gpt,
            gpt_byte: self.gpt_byte,
            z: self.z,
            z_byte: self.z_byte,
            gap_size: self.gap_size,
        }
    }

    /// A share of the bytes: a change of the text while it is held copies
    /// them first, so that a view taken before keeps reading them.
    #[inline]
    pub(crate) fn share_bytes(&self) -> Rc<Vec<u8>> {
        Rc::clone(&self.beg)
    }

    /// Whether VIEW reads this text's bytes as they now are.
    pub(crate) fn is_viewed_by(&self, view: &GapText<&[u8]>) -> bool {
        std::ptr::eq(self.beg.as_ptr(), view.beg.as_ptr())
            && (self.gpt_byte, self.z_byte, self.gap_size)
                == (view.gpt_byte, view.z_byte, view.gap_size)
    }
}

impl<B: GapBytes> GapText<B> {
    /// Z_BYTE - BEG_BYTE.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.z_byte
    }

    /// Z - BEG.
    #[inline]
    pub(crate) fn chars(&self) -> usize {
        self.z
    }

    /// Whether each character is one byte (Z == Z_BYTE).
    #[inline]
    pub(crate) fn is_ascii(&self) -> bool {
        self.z == self.z_byte
    }

    /// GPT_BYTE - BEG_BYTE.
    #[inline]
    pub(crate) fn gap_byte(&self) -> usize {
        self.gpt_byte
    }

    /// GPT - BEG.
    #[inline]
    pub(crate) fn gap_char(&self) -> usize {
        self.gpt
    }

    /// GAP_SIZE.
    #[inline]
    pub(crate) fn gap_size(&self) -> usize {
        self.gap_size
    }

    /// GAP_END_ADDR's offset: where the text after the gap starts.
    #[inline]
    fn gap_end(&self) -> usize {
        self.gpt_byte + self.gap_size
    }

    /// The text before the gap and the text after it.
    #[inline]
    pub(crate) fn halves(&self) -> (&str, &str) {
        let bytes = self.beg.gap_bytes();
        // SAFETY: each half holds whole characters of the valid UTF-8 the
        // text was made and changed with: the gap only ever stands at a
        // character boundary.
        unsafe {
            (
                std::str::from_utf8_unchecked(&bytes[..self.gpt_byte]),
                std::str::from_utf8_unchecked(&bytes[self.gap_end()..]),
            )
        }
    }

    /// The byte at AT.
    #[inline]
    pub(crate) fn byte(&self, at: usize) -> u8 {
        let bytes = self.beg.gap_bytes();
        if at < self.gpt_byte {
            bytes[at]
        } else {
            bytes[at + self.gap_size]
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
            (after, before.len(), self.gpt)
        }
    }

    /// The half holding character INDEX, as `chunk_at_byte'.
    #[inline]
    pub(crate) fn chunk_at_char(&self, index: usize) -> (&str, usize, usize) {
        let (before, after) = self.halves();
        if index < self.gpt || after.is_empty() {
            (before, 0, 0)
        } else {
            (after, before.len(), self.gpt)
        }
    }

    /// The bytes of byte RANGE before the gap and after it.  The range's
    /// ends are character boundaries.
    #[inline]
    pub(crate) fn range_parts(&self, range: std::ops::Range<usize>) -> (&[u8], &[u8]) {
        let bytes = self.beg.gap_bytes();
        let split = self.gpt_byte;
        let gap_end = self.gap_end();
        let first = &bytes[range.start.min(split)..range.end.min(split)];
        let second = &bytes
            [gap_end + (range.start.max(split) - split)..gap_end + (range.end.max(split) - split)];
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
        [(0, 0), (self.gpt, self.gpt_byte), (self.z, self.z_byte)]
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
        let mut above = (self.z, self.z_byte);
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
        let (gpt, gpt_byte) = (self.gpt, self.gpt_byte);
        let (before, after) = self.halves();
        let (before, after) = (before.as_bytes(), after.as_bytes());
        if index >= from {
            if from_byte < gpt_byte {
                if index - from <= gpt - from {
                    return from_byte + forward_bytes(&before[from_byte..], index - from);
                }
                (from, from_byte) = (gpt, gpt_byte);
            }
            let at = from_byte - gpt_byte;
            from_byte + forward_bytes(&after[at..], index - from)
        } else {
            if from_byte > gpt_byte {
                if index >= gpt {
                    let at = from_byte - gpt_byte;
                    return gpt_byte + backward_bytes(after, at, from - index);
                }
                (from, from_byte) = (gpt, gpt_byte);
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
        let mut above = (self.z, self.z_byte);
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
}

impl GapText {
    /// The bytes, for a change: copied first while a scan shares them.
    #[inline]
    fn bytes_mut(&mut self) -> &mut Vec<u8> {
        Rc::make_mut(&mut self.beg)
    }

    /// gap_left and gap_right: the gap of GAP_SIZE bytes at GPT_BYTE, the
    /// character GPT, of the text's own BYTES (`bytes_mut') moved to byte
    /// AT, the character AT_CHAR.
    #[inline]
    fn move_gap_in(
        bytes: &mut [u8],
        (gpt, gpt_byte): (&mut usize, &mut usize),
        gap_size: usize,
        (at, at_char): (usize, usize),
    ) {
        let gap_end = *gpt_byte + gap_size;
        if at < *gpt_byte {
            bytes.copy_within(at..*gpt_byte, gap_end - (*gpt_byte - at));
        } else if at > *gpt_byte {
            bytes.copy_within(gap_end..gap_end + (at - *gpt_byte), *gpt_byte);
        }
        *gpt_byte = at;
        *gpt = at_char;
    }

    /// make_gap_larger's enlarge_buffer_text for a gap of NEEDED bytes:
    /// the room reserved, or, when xrealloc would fail, the text's new
    /// size, which memory_full receives.
    fn try_make_gap(&mut self, needed: usize) -> Result<(), usize> {
        if self.gap_size >= needed {
            return Ok(());
        }
        let added = needed - self.gap_size + GAP_BYTES_DFL;
        let new_nbytes = self
            .beg
            .len()
            .checked_add(added)
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or(usize::MAX)?;
        self.bytes_mut().try_reserve(added).map_err(|_| new_nbytes)
    }

    /// make_gap_larger: the gap of GAP_SIZE bytes at GPT_BYTE of the
    /// text's own BYTES made at least NEEDED bytes, grown by what the
    /// insertion needs and GAP_BYTES_DFL (the vector's own growth keeps
    /// the reallocations few, as GNU leaves them to realloc).
    #[inline]
    fn make_gap_in(bytes: &mut Vec<u8>, gpt_byte: usize, gap_size: &mut usize, needed: usize) {
        if *gap_size >= needed {
            return;
        }
        let added = needed - *gap_size + GAP_BYTES_DFL;
        let gap_end = gpt_byte + *gap_size;
        if bytes.is_empty() && bytes.capacity() < added {
            // A fresh allocation, which reads as zeros without being
            // filled.
            *bytes = vec![0; added];
            *gap_size = added;
            return;
        }
        let after = bytes.len() - gap_end;
        bytes.resize(bytes.len() + added, 0);
        // The text after the gap moves to the end of the larger space.
        bytes.copy_within(gap_end..gap_end + after, gap_end + added);
        *gap_size += added;
    }

    /// insert_1_both's make_gap for an insertion of NEEDED bytes, without
    /// aborting: Err with the text's new size when the room cannot be
    /// had, as enlarge_buffer_text hands it to memory_full.
    pub(crate) fn try_reserve_gap(&mut self, needed: usize) -> Result<(), usize> {
        self.try_make_gap(needed)
    }

    /// insert_1_both's move of the gap and copy: TEXT, of CHARS
    /// characters, inserted at byte AT, the character AT_CHAR.
    pub(crate) fn insert(&mut self, at: usize, at_char: usize, text: &str, chars: usize) {
        self.insert_with(at, at_char, text.len(), chars, |gap| {
            gap.copy_from_slice(text.as_bytes())
        });
    }

    /// `insert' of LEN bytes of CHARS characters that WRITE puts in the
    /// gap, given that much of it.
    pub(crate) fn insert_with(
        &mut self,
        at: usize,
        at_char: usize,
        len: usize,
        chars: usize,
        write: impl FnOnce(&mut [u8]),
    ) {
        if len == 0 {
            return;
        }
        debug_assert!(at <= self.len() && at_char <= self.chars());
        // The bytes taken once for the move, the growth and the copy.
        let GapText {
            beg,
            gpt,
            gpt_byte,
            z,
            z_byte,
            gap_size,
        } = self;
        let bytes = Rc::make_mut(beg);
        Self::move_gap_in(bytes, (gpt, gpt_byte), *gap_size, (at, at_char));
        Self::make_gap_in(bytes, *gpt_byte, gap_size, len);
        write(&mut bytes[*gpt_byte..*gpt_byte + len]);
        *gpt_byte += len;
        *gap_size -= len;
        *gpt += chars;
        *z += chars;
        *z_byte += len;
    }

    /// del_range_2's widening of the gap over byte RANGE, the characters
    /// CHARS: the gap moved next to the range or inside it, then over it.
    /// The bytes stay where they are: a scan sharing them reads its own
    /// gap.
    pub(crate) fn delete(&mut self, range: std::ops::Range<usize>, chars: std::ops::Range<usize>) {
        if range.is_empty() {
            return;
        }
        debug_assert!(range.end <= self.len() && chars.end <= self.chars());
        let to = if self.gpt_byte < range.start {
            Some((range.start, chars.start))
        } else if self.gpt_byte > range.end {
            Some((range.end, chars.end))
        } else {
            None
        };
        if let Some(to) = to {
            let bytes = Rc::make_mut(&mut self.beg);
            Self::move_gap_in(
                bytes,
                (&mut self.gpt, &mut self.gpt_byte),
                self.gap_size,
                to,
            );
        }
        self.gap_size += range.len();
        self.gpt_byte = range.start;
        self.gpt = chars.start;
        self.z -= chars.len();
        self.z_byte -= range.len();
    }

    /// Whether a scan shares the bytes.
    #[cfg(test)]
    fn shared(&self) -> bool {
        Rc::strong_count(&self.beg) > 1
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
    fn the_gap_starts_and_grows_as_gnu_s() {
        // Fget_buffer_create's 20 bytes, then make_gap_larger's
        // GAP_BYTES_DFL beyond each insertion that needs more.
        assert_eq!(GapText::from_str("").gap_size(), 20);
        assert_eq!(GapText::from_str("abc").gap_size(), 17);
        assert_eq!(GapText::from_str(&"x".repeat(100)).gap_size(), 2000);
        let mut gap = GapText::from_str("");
        for n in 0..100_000 {
            gap.insert(n, n, "x", 1);
        }
        assert_eq!((gap.len(), gap.chars()), (100_000, 100_000));
        assert!(gap.gap_size() <= GAP_BYTES_DFL);
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
