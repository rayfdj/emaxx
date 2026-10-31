#![allow(dead_code)]

use crate::intervals::{self, Intervals, PropertyRecorder, PropertySetType, PropertyVariables};
use crate::lisp::types::{BufferRef, Kind, MarkerRef};
use crate::lisp::types::{PropertyKey, PropertyName, Value};
use std::cell::{Cell, RefCell};

mod gap;
pub use gap::GapText;
pub(crate) use gap::{backward_bytes, count_chars, forward_bytes};
use std::time::SystemTime;

/// One edit of a buffer: the characters [START, OLD_END) became
/// [START, NEW_END) (1-based positions after the edit for NEW_END, before
/// it for OLD_END), by a text change (`text') or a text-property change
/// (the range's characters unchanged, OLD_END == NEW_END).  SERIAL is the
/// buffer's edit serial after the edit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EditRecord {
    pub serial: u64,
    pub start: usize,
    pub old_end: usize,
    pub new_end: usize,
    pub text: bool,
}

const EDIT_LOG_LIMIT: usize = 64;

/// Modification counter. Bumped on every edit, used to detect
/// whether a buffer has changed since some snapshot (e.g. last save).
pub type ModCount = i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileModTime {
    pub modified: SystemTime,
}

enum BufferMark {
    Position(Option<usize>),
    Object { marker: MarkerRef, owner: BufferRef },
}

impl BufferMark {
    fn position(&self) -> Option<usize> {
        match self {
            Self::Position(position) => *position,
            Self::Object { marker, .. } => marker.position(),
        }
    }
}

impl Clone for BufferMark {
    fn clone(&self) -> Self {
        // Buffer::clone is a text snapshot; it must not share a mutable mark.
        Self::Position(self.position())
    }
}

/// A single editing buffer.
///
/// Positions are 1-based to match Emacs semantics: position 1 is
/// before the first character, position (len+1) is after the last.
/// Internally we convert to 0-based char indices into the text.
pub struct Buffer {
    /// Human-visible name (e.g. "*scratch*" or "main.rs").
    pub name: String,

    /// Previous name, set by rename-buffer.
    pub last_name: Option<String>,

    /// The text.
    text: BufferText,

    /// Whether Fkill_buffer killed the buffer (GNU's nil name).
    pub(crate) killed: bool,

    /// BUF_Z - BUF_BEG, BUF_BEGV and BUF_ZV as a killed buffer's freed
    /// text left them: GNU's free_buffer_text frees the text and keeps its
    /// positions.
    killed_size: usize,
    killed_bounds: (usize, usize),

    /// A killed buffer's always-local slots held apart from its fields, as
    /// they were when it was killed: the fixed slots in their order
    /// (`FIXED_SLOT_NAMES'), None for one that was not a binding, then
    /// `mark-active'; None for a live buffer.
    #[allow(clippy::type_complexity)]
    pub(crate) killed_locals: Option<Box<[Option<Value>; crate::lisp::eval::FIXED_SLOTS + 1]>>,

    /// Derived character view of the text around the last character read.
    /// Syntax and motion code commonly scan adjacent characters through
    /// `char_at`; retaining a decoded window avoids a conversion per
    /// character while keeping the text the sole authority.
    char_cache: RefCell<RopeCharCache>,

    /// Current cursor position (1-based char offset).
    pt: usize,

    /// A standalone buffer has no Lisp allocation. Installation replaces its
    /// position with the actual mark object, without retaining a position copy.
    mark: BufferMark,

    /// BVAR (b, mark_active): any Lisp object, non-nil when the region
    /// between point and mark is active.
    mark_active: Value,

    /// Bumped on every modification.
    modiff: ModCount,

    /// Updated when buffer characters change, but not for metadata-only
    /// modifications such as `set-buffer-modified-p'.
    chars_modiff: ModCount,

    /// Counts of this process's edits, monotonic: `modiff' is Lisp-visible
    /// and `internal--set-buffer-modified-tick' can move it backwards, so a
    /// cache keyed on it could take an old entry for a new state.  The text
    /// serial advances with the characters, the edit serial with the
    /// characters and the text properties.  Neither is dumped: a loaded
    /// buffer starts both at zero in a process without cache entries.
    edit_serial: u64,
    text_edit_serial: u64,

    /// The last `EDIT_LOG_LIMIT' edits, oldest first, one record per
    /// serial: a cache holding a view of an earlier state replays the
    /// records since its serial instead of reading the whole buffer
    /// again.  Emptied wherever the text is replaced without a record
    /// (a swap of buffer texts, a change of representation), which a
    /// gap in the serials then reports as unreachable.  Not dumped.
    edits: std::collections::VecDeque<EditRecord>,

    /// Value of modiff at last save.
    save_modiff: ModCount,

    /// BUF_AUTOSAVE_MODIFF: the modification count at the last auto-save
    /// (0 before any).
    autosave_modiff: ModCount,

    /// auto_save_failure_time: when an auto-save last took over a minute
    /// (seconds since the epoch), 0 for none.
    pub(crate) auto_save_failure_time: i64,

    /// Narrowing: accessible region [begv, zv] (1-based, inclusive of begv,
    /// exclusive of zv in the sense that zv is one past the last accessible char).
    begv: usize,
    zv: usize,

    /// Path to the visited file, if any.
    pub file: Option<String>,

    /// Canonical path to the visited file, if known.
    pub file_truename: Option<String>,

    /// Last known on-disk modification time for the visited file.
    visited_file_modtime: Option<FileModTime>,

    /// BVAR (b, undo_list), `buffer-undo-list' itself: t while no undo is
    /// kept, otherwise the list undo.c's record_* functions push onto.
    undo_list: Value,

    /// keyboard.c's `point_before_last_command_or_undo', held by the one
    /// buffer `buffer_before_last_command_or_undo' names (the interpreter
    /// clears it in the buffer that held it before): where point stood
    /// before the last command or `undo-boundary', which record_point
    /// records into the first change after a boundary.
    point_before_last_command_or_undo: Option<usize>,

    /// Overlays attached to this buffer.
    pub overlays: crate::overlay::OverlayTree,

    /// buffer_intervals: the intervals of the whole text [BEG, Z), none
    /// until a property is first given, and none again once a deletion
    /// takes the whole text (adjust_intervals_for_deletion).
    intervals: Option<Intervals>,

    /// Sparse 1-based character positions for GNU Emacs characters outside
    /// Unicode's scalar range.  The text holds one placeholder scalar at each
    /// position; this typed sidecar is runtime representation, never a Lisp
    /// text property.
    extended_chars: Vec<(usize, u32)>,

    /// When true, suppress creation/kill buffer hooks for this buffer.
    pub inhibit_hooks: bool,

    /// buffer.h's Lisp field and independent per-buffer local flag.
    /// A C bset changes the field without creating a Lisp local binding.
    pub(crate) text_conversion_style: Value,
    pub(crate) text_conversion_style_is_local: bool,

    /// Whether positions in this buffer are interpreted as multibyte character positions.
    multibyte: bool,
}

// A Buffer clone is a text snapshot, without linked Lisp overlay objects.
// The image copier relocates the original buffer's overlay graph explicitly.
impl Clone for Buffer {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            last_name: self.last_name.clone(),
            text: self.text.clone(),
            killed: self.killed,
            killed_size: self.killed_size,
            killed_bounds: self.killed_bounds,
            killed_locals: self.killed_locals.clone(),
            char_cache: self.char_cache.clone(),
            pt: self.pt,
            mark: self.mark.clone(),
            mark_active: self.mark_active,
            modiff: self.modiff,
            chars_modiff: self.chars_modiff,
            edit_serial: self.edit_serial,
            text_edit_serial: self.text_edit_serial,
            edits: self.edits.clone(),
            save_modiff: self.save_modiff,
            autosave_modiff: self.autosave_modiff,
            auto_save_failure_time: self.auto_save_failure_time,
            begv: self.begv,
            zv: self.zv,
            file: self.file.clone(),
            file_truename: self.file_truename.clone(),
            visited_file_modtime: self.visited_file_modtime,
            undo_list: self.undo_list,
            point_before_last_command_or_undo: self.point_before_last_command_or_undo,
            overlays: crate::overlay::OverlayTree::default(),
            intervals: self.intervals.clone(),
            extended_chars: self.extended_chars.clone(),
            inhibit_hooks: self.inhibit_hooks,
            text_conversion_style: self.text_conversion_style,
            text_conversion_style_is_local: self.text_conversion_style_is_local,
            multibyte: self.multibyte,
        }
    }
}

/// The buffer's text with its character and byte counts, GNU's Z and
/// Z_BYTE: the bytes in a gap buffer (`gap::GapText'), as insdel.c keeps
/// them, with the count of the raw-byte markers among them.  All changes
/// go through `insert' and `remove', which keep the counts.
#[derive(Clone, Default)]
struct BufferText {
    text: GapText,
    /// The raw-byte markers (U+E080..U+E0FF) in the text, whose stored
    /// form is a byte longer than GNU's: a marker's byte position needs
    /// their count before it only when there are any.
    raw_markers: usize,
    /// Recent char/byte conversions of a text whose characters are not
    /// all one byte.
    positions: Cell<PositionCache>,
}

/// editfns.c:transpose_markers' move of POSITION: in the first region by
/// the second's length and the distance between them, between them by the
/// difference of the lengths, in the second back by the first's length
/// and that distance.
fn transposed_position(
    position: usize,
    start1: usize,
    end1: usize,
    start2: usize,
    end2: usize,
) -> usize {
    if position < start1 || position >= end2 {
        position
    } else if position < end1 {
        position + (end2 - start2) + (start2 - end1)
    } else if position < start2 {
        position + (end2 - start2) - (end1 - start1)
    } else {
        position - (end1 - start1) - (start2 - end1)
    }
}

/// marker.c buf_charpos_to_bytepos's cache, for a text whose characters
/// are not all one byte: the char and byte offsets of recent conversions,
/// shifted by an edit before them and dropped by one over them.
#[derive(Clone, Copy, Default)]
struct PositionCache {
    entries: [(usize, usize); 4],
    len: usize,
    next: usize,
}

impl PositionCache {
    fn entries(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.entries[..self.len].iter().copied()
    }

    fn by_char(&self, index: usize) -> Option<usize> {
        self.entries[..self.len]
            .iter()
            .find(|entry| entry.0 == index)
            .map(|entry| entry.1)
    }

    fn by_byte(&self, byte: usize) -> Option<usize> {
        self.entries[..self.len]
            .iter()
            .find(|entry| entry.1 == byte)
            .map(|entry| entry.0)
    }

    fn record(&mut self, index: usize, byte: usize) {
        self.entries[self.next] = (index, byte);
        self.next = (self.next + 1) % self.entries.len();
        self.len = (self.len + 1).min(self.entries.len());
    }

    /// CHARS characters, BYTES bytes, inserted at char INDEX.
    fn inserted(&mut self, index: usize, chars: usize, bytes: usize) {
        for entry in &mut self.entries[..self.len] {
            if entry.0 > index {
                *entry = (entry.0 + chars, entry.1 + bytes);
            }
        }
    }

    /// The characters of RANGE, BYTES bytes, removed.
    fn removed(&mut self, range: std::ops::Range<usize>, bytes: usize) {
        let mut kept = 0;
        for at in 0..self.len {
            let (index, byte) = self.entries[at];
            let entry = if index <= range.start {
                (index, byte)
            } else if index >= range.end {
                (index - range.len(), byte - bytes)
            } else {
                continue;
            };
            self.entries[kept] = entry;
            kept += 1;
        }
        self.len = kept;
        self.next = kept % self.entries.len();
    }
}

/// A buffer's text as a scan reads it apart from the buffer (see
/// `Buffer::scan_text'): the gap buffer's bytes, shared until the next
/// change.
pub type ScanText = GapText;

/// What the maker of a text to insert already counted of it: its
/// characters and its raw-byte markers (`count_raw_markers'), which the
/// buffer then need not scan it for.
#[derive(Clone, Copy, Debug)]
pub struct TextCounts {
    pub chars: usize,
    pub raw_markers: usize,
}

/// The raw-byte markers U+E080..U+E0FF (EE 82 80 .. EE 83 BF) in BYTES,
/// which hold whole characters.  From the first EE on, every pair of
/// bytes is tested, 32 pairs a block in byte-wide counts the compiler
/// vectorizes, where a stop at each EE made a text of many raw bytes (a
/// unibyte buffer's non-ASCII file) cost a stop a byte.
fn count_raw_markers(bytes: &[u8]) -> usize {
    let Some(first) = memchr::memchr(0xEE, bytes) else {
        return 0;
    };
    let bytes = &bytes[first..];
    let marker = |lead: u8, next: u8| (lead == 0xEE) & (next & 0xFE == 0x82);
    let pairs = bytes.len() - 1;
    let mut count = 0;
    let mut at = 0;
    while at + 32 <= pairs {
        let leads: &[u8; 32] = bytes[at..at + 32].try_into().expect("32 bytes");
        let nexts: &[u8; 32] = bytes[at + 1..at + 33].try_into().expect("32 bytes");
        let mut block = 0u8;
        for (&lead, &next) in leads.iter().zip(nexts) {
            block = block.wrapping_add(u8::from(marker(lead, next)));
        }
        count += usize::from(block);
        at += 32;
    }
    count
        + (at..pairs)
            .filter(|&k| marker(bytes[k], bytes[k + 1]))
            .count()
}

impl BufferText {
    fn from_str(text: &str) -> Self {
        Self {
            text: GapText::from_str(text),
            raw_markers: count_raw_markers(text.as_bytes()),
            positions: Cell::default(),
        }
    }

    /// The raw-byte markers in the first BYTES bytes of the text.
    fn raw_markers_before(&self, bytes: usize) -> usize {
        if self.raw_markers == 0 {
            return 0;
        }
        let (first, second) = self.text.range_halves(0..bytes);
        count_raw_markers(first.as_bytes()) + count_raw_markers(second.as_bytes())
    }

    /// The raw-byte markers among the characters of RANGE.
    fn raw_markers_in(&self, range: std::ops::Range<usize>) -> usize {
        if self.raw_markers == 0 {
            return 0;
        }
        let start = self.char_to_byte(range.start);
        let end = self.char_to_byte(range.end);
        let (first, second) = self.text.range_halves(start..end);
        count_raw_markers(first.as_bytes()) + count_raw_markers(second.as_bytes())
    }

    #[inline]
    fn len_chars(&self) -> usize {
        self.text.chars()
    }

    #[inline]
    fn len_bytes(&self) -> usize {
        self.text.len()
    }

    /// The half of the text holding char index INDEX, with the byte and
    /// char indices of its start: the text before the gap or after it.
    #[inline]
    fn chunk_at_char(&self, index: usize) -> (&str, usize, usize) {
        self.text.chunk_at_char(index)
    }

    /// The half holding byte index INDEX, as `chunk_at_char'.
    #[inline]
    fn chunk_at_byte(&self, index: usize) -> (&str, usize, usize) {
        self.text.chunk_at_byte(index)
    }

    /// The text from byte BYTE to the end of the half holding it.
    #[inline]
    fn rest_at_byte(&self, byte: usize) -> &str {
        let (chunk, chunk_byte, _) = self.text.chunk_at_byte(byte);
        &chunk[byte - chunk_byte..]
    }

    /// The text before byte BYTE back to the start of the half holding
    /// the byte before it.
    #[inline]
    fn before_byte(&self, byte: usize) -> &str {
        let (chunk, chunk_byte, _) = self.text.chunk_at_byte(byte.saturating_sub(1));
        &chunk[..byte - chunk_byte]
    }

    #[inline]
    fn char(&self, index: usize) -> char {
        self.rest_at_byte(self.char_to_byte(index))
            .chars()
            .next()
            .expect("char index within the text")
    }

    /// buf_charpos_to_bytepos: the byte index of char INDEX, counted from
    /// the nearest position known (the text's ends, the gap, a recent
    /// conversion).
    #[inline]
    fn char_to_byte(&self, index: usize) -> usize {
        if self.text.is_ascii() {
            return index;
        }
        self.char_to_byte_counted(index)
    }

    /// Remember that char INDEX starts at byte BYTE, found by a scan that
    /// counted both, as find_newline returns both positions.
    fn note_position(&self, index: usize, byte: usize) {
        if self.text.is_ascii() {
            return;
        }
        let mut positions = self.positions.get();
        if positions.by_char(index).is_none() {
            positions.record(index, byte);
            self.positions.set(positions);
        }
    }

    fn char_to_byte_counted(&self, index: usize) -> usize {
        // CONSIDER of BEG and Z, which buf_charpos_to_bytepos returns
        // before it looks further.
        if index == 0 {
            return 0;
        }
        if index == self.text.chars() {
            return self.text.len();
        }
        let mut positions = self.positions.get();
        if let Some(byte) = positions.by_char(index) {
            return byte;
        }
        let byte = self.text.char_to_byte_near(index, positions.entries());
        positions.record(index, byte);
        self.positions.set(positions);
        byte
    }

    /// buf_bytepos_to_charpos: the char index of the character byte index
    /// BYTE lies in.
    #[inline]
    fn byte_to_char(&self, byte: usize) -> usize {
        if self.text.is_ascii() {
            return byte;
        }
        self.byte_to_char_counted(byte)
    }

    fn byte_to_char_counted(&self, byte: usize) -> usize {
        if byte == 0 {
            return 0;
        }
        if byte == self.text.len() {
            return self.text.chars();
        }
        let mut positions = self.positions.get();
        if let Some(index) = positions.by_byte(byte) {
            return index;
        }
        // A byte inside a character belongs to the character it continues.
        let mut start = byte;
        while start > 0 && start < self.text.len() && self.text.byte(start) & 0xC0 == 0x80 {
            start -= 1;
        }
        let index = self.text.byte_to_char_near(start, positions.entries());
        // A byte that starts its character is that character's position,
        // remembered as buf_bytepos_to_charpos remembers the last it
        // converted (a match's registers then count from each other).
        if start == byte {
            positions.record(index, byte);
            self.positions.set(positions);
        }
        index
    }

    /// The text of RANGE (char indices) as a String.
    fn string(&self, range: std::ops::Range<usize>) -> String {
        let start = self.char_to_byte(range.start);
        let end = self.char_to_byte(range.end);
        self.text.string(start..end)
    }

    /// The text of RANGE (char indices) before the gap and after it.
    fn range_halves(&self, range: std::ops::Range<usize>) -> (&str, &str) {
        let start = self.char_to_byte(range.start);
        let end = self.char_to_byte(range.end);
        self.text.range_halves(start..end)
    }

    /// The newlines in byte RANGE.
    fn newlines_in(&self, range: std::ops::Range<usize>) -> usize {
        let (first, second) = self.text.range_halves(range);
        memchr::memchr_iter(b'\n', first.as_bytes()).count()
            + memchr::memchr_iter(b'\n', second.as_bytes()).count()
    }

    /// The text's lines: its newlines and one.
    fn len_lines(&self) -> usize {
        self.newlines_in(0..self.text.len()) + 1
    }

    /// The char index of the start of 0-based line INDEX: just after the
    /// INDEXth newline, the end when there are fewer.
    fn line_to_char(&self, index: usize) -> usize {
        if index == 0 {
            return 0;
        }
        let (before, after) = self.text.halves();
        let mut left = index;
        for (half, start) in [(before, 0), (after, before.len())] {
            for at in memchr::memchr_iter(b'\n', half.as_bytes()) {
                left -= 1;
                if left == 0 {
                    return self.byte_to_char(start + at + 1);
                }
            }
        }
        self.text.chars()
    }

    /// The 0-based line char index INDEX is on: the newlines before it.
    fn char_to_line(&self, index: usize) -> usize {
        self.newlines_in(0..self.char_to_byte(index))
    }

    /// The text of 0-based line INDEX, its newline included.
    fn line(&self, index: usize) -> String {
        let start = self.line_to_char(index);
        let end = self.line_to_char(index + 1);
        self.string(start..end)
    }

    /// The text for a scan that must not hold the buffer: its bytes,
    /// shared until the next change.
    fn scan_text(&self) -> ScanText {
        self.text.clone()
    }

    fn whole_string(&self) -> String {
        self.text.string(0..self.text.len())
    }

    fn insert(&mut self, char_index: usize, text: &str) {
        self.insert_counted(char_index, text, None);
    }

    /// `insert' of a TEXT whose COUNTS its maker already has.
    fn insert_counted(&mut self, char_index: usize, text: &str, counts: Option<TextCounts>) {
        let (added, raw_markers) = match counts {
            Some(counts) => {
                debug_assert_eq!(
                    (counts.chars, counts.raw_markers),
                    (
                        ropey::str_utils::byte_to_char_idx(text, text.len()),
                        count_raw_markers(text.as_bytes())
                    )
                );
                (counts.chars, counts.raw_markers)
            }
            None => (
                ropey::str_utils::byte_to_char_idx(text, text.len()),
                count_raw_markers(text.as_bytes()),
            ),
        };
        let byte = self.char_to_byte(char_index);
        self.text.insert(byte, char_index, text, added);
        self.positions
            .get_mut()
            .inserted(char_index, added, text.len());
        self.raw_markers += raw_markers;
        self.check_counts();
    }

    fn remove(&mut self, range: std::ops::Range<usize>) {
        let start = self.char_to_byte(range.start);
        let end = self.char_to_byte(range.end);
        self.raw_markers -= if range.len() == self.text.chars() {
            self.raw_markers
        } else if self.raw_markers == 0 {
            0
        } else {
            let (first, second) = self.text.range_halves(start..end);
            count_raw_markers(first.as_bytes()) + count_raw_markers(second.as_bytes())
        };
        self.delete(range, start, end - start);
    }

    /// `remove' for a caller that already holds the removed text.
    fn remove_text(&mut self, range: std::ops::Range<usize>, removed: &str) {
        self.raw_markers -= if range.len() == self.text.chars() {
            self.raw_markers
        } else {
            count_raw_markers(removed.as_bytes())
        };
        let start = self.char_to_byte(range.start);
        self.delete(range, start, removed.len());
    }

    /// `remove' of RANGE, BYTES long, whose raw-byte markers the caller
    /// has counted.
    fn remove_bytes(&mut self, range: std::ops::Range<usize>, bytes: usize) {
        let start = self.char_to_byte(range.start);
        self.delete(range, start, bytes);
    }

    /// The characters of RANGE, BYTES bytes from byte START, deleted.
    fn delete(&mut self, range: std::ops::Range<usize>, start: usize, bytes: usize) {
        self.text.delete(start..start + bytes, range.clone());
        self.positions.get_mut().removed(range, bytes);
        self.check_counts();
    }

    fn check_counts(&self) {
        // A recount walks all the text, which at every edit would make a
        // million insertions quadratic (undo-tests' flush-lines buffer):
        // a short text's counts are checked.
        if cfg!(debug_assertions) && self.text.len() <= 4096 {
            let (before, after) = self.text.halves();
            let before_chars = gap::count_chars(before.as_bytes());
            assert_eq!(self.text.gap_char(), before_chars);
            assert_eq!(
                self.text.chars(),
                before_chars + gap::count_chars(after.as_bytes())
            );
            assert_eq!(
                self.raw_markers,
                count_raw_markers(before.as_bytes()) + count_raw_markers(after.as_bytes())
            );
        }
    }
}

/// The characters `char_at' decodes before the one asked for, and in all.
const CHAR_CACHE_BEHIND: usize = 64;
const CHAR_CACHE_WINDOW: usize = 256;

#[derive(Clone, Default)]
struct RopeCharCache {
    start: usize,
    chars: Vec<char>,
    valid: bool,
    /// The last character read outside the window: a read next to it
    /// continues a scan, which fills the window, where a read elsewhere
    /// decodes its character alone.
    last_single: Option<usize>,
}

impl RopeCharCache {
    fn character(&self, index: usize) -> Option<char> {
        if !self.valid {
            return None;
        }
        self.chars.get(index.checked_sub(self.start)?).copied()
    }
}

/// GNU's `(t . TIME)' first-change undo entry: TIME is the visited
/// file's modtime, which `primitive-undo' compares against
/// `visited-file-modtime' to restore the unmodified state.
fn undo_first_change_marker(modtime: Option<FileModTime>) -> Value {
    let Some(duration) =
        modtime.and_then(|value| value.modified.duration_since(std::time::UNIX_EPOCH).ok())
    else {
        // `visited-file-modtime' returns the integer 0 when there is no
        // recorded timestamp, so GNU records the dotted pair (t . 0).
        return Value::cons(Value::T, Value::Integer(0));
    };
    let seconds = duration.as_secs() as i64;
    let nanoseconds = duration.subsec_nanos() as i64;
    Value::list([
        Value::T,
        Value::Integer(seconds >> 16),
        Value::Integer(seconds & 0xffff),
        Value::Integer(nanoseconds.div_euclid(1_000)),
        Value::Integer(nanoseconds.rem_euclid(1_000) * 1_000),
    ])
}

/// A buffer's fields as pdumper.c's `dump_buffer' copies them from the
/// `struct buffer': the text and the positions, counters, file
/// metadata, undo list and property spans that are the buffer's own
/// state.  The overlays are not here (GNU refuses to dump a buffer that
/// has any; the writer checks `overlays' itself), and neither are the
/// derived caches, which a restored buffer rebuilds.
pub(crate) struct BufferImage {
    pub(crate) name: String,
    pub(crate) text: String,
    pub(crate) pt: usize,
    pub(crate) mark: Option<usize>,
    pub(crate) mark_active: Value,
    pub(crate) modiff: ModCount,
    pub(crate) chars_modiff: ModCount,
    pub(crate) save_modiff: ModCount,
    pub(crate) autosave_modiff: ModCount,
    pub(crate) begv: usize,
    pub(crate) zv: usize,
    pub(crate) file: Option<String>,
    pub(crate) file_truename: Option<String>,
    pub(crate) visited_file_modtime: Option<FileModTime>,
    pub(crate) undo_list: Value,
    pub(crate) intervals: Option<Intervals>,
    pub(crate) extended_chars: Vec<(usize, u32)>,
    pub(crate) inhibit_hooks: bool,
    pub(crate) text_conversion_style: Value,
    pub(crate) text_conversion_style_is_local: bool,
    pub(crate) multibyte: bool,
}

/// One interval of an object's text: [START, END) and its plist.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PropertyInterval {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) plist: Value,
    /// The interval's index in its object's intervals, where the next and
    /// previous ones are.
    index: usize,
}

impl PropertyInterval {
    pub(crate) fn index(&self) -> usize {
        self.index
    }
}

/// An object's intervals as textprop.c walks them: find_interval, then
/// next_interval and previous_interval from where it is.
#[derive(Clone, Copy)]
pub(crate) struct PropertyIntervals<'a> {
    intervals: &'a Intervals,
}

impl<'a> PropertyIntervals<'a> {
    pub(crate) fn new(intervals: &'a Intervals) -> Self {
        Self { intervals }
    }

    pub(crate) fn intervals(&self) -> &'a Intervals {
        self.intervals
    }

    /// The interval at INDEX.
    #[inline]
    pub(crate) fn at(&self, index: usize) -> PropertyInterval {
        let (start, end, plist) = self.intervals.bounds(index);
        PropertyInterval {
            start,
            end,
            plist,
            index,
        }
    }

    /// intervals.c:find_interval: the interval holding POS, the last one
    /// for POS at the end of the text.
    pub(crate) fn find(&self, pos: usize) -> PropertyInterval {
        let pos = pos.clamp(self.intervals.origin(), self.intervals.end());
        self.at(self.intervals.find(pos))
    }

    /// `find' of a position near the interval at INDEX.
    pub(crate) fn find_near(&self, pos: usize, index: usize) -> PropertyInterval {
        let intervals = self.intervals;
        let pos = pos.clamp(intervals.origin(), intervals.end());
        let mut index = index.min(intervals.count() - 1);
        for _ in 0..4 {
            if pos < intervals.start(index) {
                index -= 1;
            } else if pos >= intervals.last_pos(index) && index + 1 < intervals.count() {
                index += 1;
            } else {
                return self.at(index);
            }
        }
        self.find(pos)
    }

    /// next_interval.
    #[inline]
    pub(crate) fn next(&self, interval: &PropertyInterval) -> Option<PropertyInterval> {
        self.intervals
            .next(interval.index)
            .map(|index| self.at(index))
    }

    /// previous_interval.
    #[inline]
    pub(crate) fn previous(&self, interval: &PropertyInterval) -> Option<PropertyInterval> {
        self.intervals
            .previous(interval.index)
            .map(|index| self.at(index))
    }
}

/// What an insertion gives the buffer's intervals for its text
/// (graft_intervals_into_buffer's SOURCE and INHERIT).
#[derive(Clone, Copy)]
pub(crate) enum InsertedProperties<'a> {
    /// No intervals: without INHERIT the text gets no properties, with it
    /// what offset_intervals gave it.
    None { inherit: bool },
    /// A string's intervals, from 0 over the inserted text: copied over
    /// it, or with INHERIT merged into what offset_intervals gave it.
    Intervals {
        source: &'a Intervals,
        inherit: bool,
    },
}

impl InsertedProperties<'_> {
    pub(crate) const PLAIN: Self = Self::None { inherit: false };
}

/// undo.c:record_property_change into a buffer's undo list.
struct UndoRecorder<'a> {
    undo_list: &'a mut Value,
}

impl PropertyRecorder for UndoRecorder<'_> {
    fn record(&mut self, start: usize, length: usize, prop: Value, value: Value) {
        let entry = Value::cons(
            Value::Nil,
            Value::cons(
                prop,
                Value::cons(
                    value,
                    Value::cons(
                        Value::Integer(start as i64),
                        Value::Integer((start + length) as i64),
                    ),
                ),
            ),
        );
        *self.undo_list = Value::cons(entry, *self.undo_list);
    }
}

/// Errors that buffer operations can produce.
#[derive(Debug, PartialEq)]
pub enum BufferError {
    BeginningOfBuffer,
    EndOfBuffer,
    ReadOnly, // placeholder for later
    InvalidPosition(usize),
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferError::BeginningOfBuffer => write!(f, "Beginning of buffer"),
            BufferError::EndOfBuffer => write!(f, "End of buffer"),
            BufferError::ReadOnly => write!(f, "Buffer is read-only"),
            BufferError::InvalidPosition(p) => write!(f, "Invalid position: {}", p),
        }
    }
}

impl std::error::Error for BufferError {}

impl Buffer {
    /// Create an empty buffer with the given name.
    pub fn new(name: &str) -> Self {
        let text = BufferText::default();
        Buffer {
            name: name.to_string(),
            last_name: None,
            text,
            killed: false,
            killed_size: 0,
            killed_bounds: (1, 1),
            killed_locals: None,
            char_cache: RefCell::new(RopeCharCache::default()),
            pt: 1,
            mark: BufferMark::Position(None),
            mark_active: Value::Nil,
            modiff: 1,
            chars_modiff: 1,
            save_modiff: 1,
            edit_serial: 0,
            text_edit_serial: 0,
            edits: std::collections::VecDeque::new(),
            autosave_modiff: 0,
            auto_save_failure_time: 0,
            begv: 1,
            zv: 1, // empty buffer: zv = 1
            file: None,
            file_truename: None,
            visited_file_modtime: None,
            undo_list: Value::Nil,
            point_before_last_command_or_undo: None,
            overlays: crate::overlay::OverlayTree::default(),
            intervals: None,
            extended_chars: Vec::new(),
            inhibit_hooks: false,
            text_conversion_style: Value::Nil,
            text_conversion_style_is_local: false,
            multibyte: true,
        }
    }

    /// Create a buffer from a string (useful for tests and loading files).
    pub fn from_text(name: &str, s: &str) -> Self {
        let text = BufferText::from_str(s);
        let len = text.len_chars();
        Buffer {
            name: name.to_string(),
            last_name: None,
            text,
            killed: false,
            killed_size: 0,
            killed_bounds: (1, 1),
            killed_locals: None,
            char_cache: RefCell::new(RopeCharCache::default()),
            pt: 1,
            mark: BufferMark::Position(None),
            mark_active: Value::Nil,
            modiff: 1,
            chars_modiff: 1,
            save_modiff: 1,
            edit_serial: 0,
            text_edit_serial: 0,
            edits: std::collections::VecDeque::new(),
            autosave_modiff: 0,
            auto_save_failure_time: 0,
            begv: 1,
            zv: len + 1,
            file: None,
            file_truename: None,
            visited_file_modtime: None,
            undo_list: Value::Nil,
            point_before_last_command_or_undo: None,
            overlays: crate::overlay::OverlayTree::default(),
            intervals: None,
            extended_chars: Vec::new(),
            inhibit_hooks: false,
            text_conversion_style: Value::Nil,
            text_conversion_style_is_local: false,
            multibyte: true,
        }
    }

    // ── Position queries (Emacs: point, point-min, point-max, buffer-size) ──

    /// Current point (1-based).
    pub fn point(&self) -> usize {
        self.pt
    }

    /// First accessible position (1-based). Equals 1 unless narrowed.
    pub fn point_min(&self) -> usize {
        self.begv
    }

    /// One past last accessible position (1-based).
    pub fn point_max(&self) -> usize {
        self.zv
    }

    pub fn restriction(&self) -> (usize, usize) {
        (self.begv, self.zv)
    }

    /// Number of accessible characters.
    pub fn buffer_size(&self) -> usize {
        self.zv - self.begv
    }

    pub fn is_multibyte(&self) -> bool {
        self.multibyte
    }

    /// CHARS_MODIFF analog: bumped only when buffer text changes, so a
    /// text-derived cache keyed on it can never serve stale content.
    /// This process's count of text edits (see `edit_serial').
    pub fn text_edit_serial(&self) -> u64 {
        self.text_edit_serial
    }

    /// This process's count of text and text-property edits.
    pub fn edit_serial(&self) -> u64 {
        self.edit_serial
    }

    /// The edits after SERIAL, oldest first, or None when the log no
    /// longer reaches back to SERIAL (more than `EDIT_LOG_LIMIT' edits
    /// since, or a text replacement that emptied it): the records
    /// SERIAL+1 ..= the current serial, each present.
    pub fn edits_since(&self, serial: u64) -> Option<Vec<EditRecord>> {
        if serial == self.edit_serial {
            return Some(Vec::new());
        }
        if serial > self.edit_serial {
            return None;
        }
        let first = self.edits.partition_point(|record| record.serial <= serial);
        let records = self.edits.range(first..).copied().collect::<Vec<_>>();
        let contiguous = records
            .first()
            .is_some_and(|record| record.serial == serial + 1)
            && records
                .last()
                .is_some_and(|record| record.serial == self.edit_serial)
            && records.len() as u64 == self.edit_serial - serial;
        contiguous.then_some(records)
    }

    /// Advance the edit serial (and the text serial when TEXT) and record
    /// the edit of [START, OLD_END) into [START, NEW_END).
    fn log_edit(&mut self, start: usize, old_end: usize, new_end: usize, text: bool) {
        self.edit_serial += 1;
        if text {
            self.text_edit_serial += 1;
        }
        if self.edits.len() >= EDIT_LOG_LIMIT {
            self.edits.pop_front();
        }
        self.edits.push_back(EditRecord {
            serial: self.edit_serial,
            start,
            old_end,
            new_end,
            text,
        });
    }

    /// Advance both serials for a text replacement no record describes
    /// and empty the log, so no view of an earlier state can be replayed
    /// across it.
    fn text_replaced_without_record(&mut self) {
        self.edit_serial += 1;
        self.text_edit_serial += 1;
        self.edits.clear();
    }

    pub fn chars_modification_count(&self) -> ModCount {
        self.chars_modiff
    }

    pub fn set_multibyte(&mut self, enabled: bool) {
        self.multibyte = enabled;
        self.refresh_marker_byte_positions();
    }

    /// Replace the internal character view while preserving the underlying
    /// byte sequence.  POSITION_MAP contains the new 1-based position for
    /// every old character boundary, including point-max; EXTENDED holds
    /// the new text's characters its view cannot carry, by position.
    pub fn set_multibyte_representation(
        &mut self,
        enabled: bool,
        text: String,
        position_map: &[usize],
        extended: Vec<(usize, u32)>,
    ) {
        let remap = |position: usize| {
            position_map
                .get(position.saturating_sub(1))
                .copied()
                .unwrap_or_else(|| text.chars().count() + 1)
        };

        self.pt = remap(self.pt);
        self.begv = remap(self.begv);
        self.zv = remap(self.zv);
        if let BufferMark::Position(mark) = &mut self.mark {
            *mark = mark.map(remap);
        } else if let BufferMark::Object { owner, .. } = self.mark {
            for marker in owner.markers() {
                marker.set_positions(remap(marker.last_position()), marker.bytepos());
            }
        }
        // Overlay endpoints already use the text's character coordinates.
        // The unibyte overlay API translates those coordinates at its public
        // boundary, so remapping them here would apply the byte-to-character
        // conversion twice.
        // intervals.c:set_intervals_multibyte: each interval's bounds in
        // the new positions, one left with no text gone.
        if let Some(intervals) = &mut self.intervals {
            intervals.remap(remap);
        }
        self.extended_chars = extended;

        self.text = BufferText::from_str(&text);
        self.invalidate_char_cache();
        self.multibyte = enabled;
        self.text_replaced_without_record();
    }

    /// Total characters in the buffer (ignoring narrowing).
    pub fn size_total(&self) -> usize {
        self.text.len_chars()
    }

    /// A killed buffer's size as Fkill_buffer left it (editfns.c's
    /// buffer-size reads it).
    pub(crate) fn killed_size(&self) -> usize {
        self.killed_size
    }

    /// A killed buffer's BEGV and ZV as Fkill_buffer left them.
    pub(crate) fn killed_bounds(&self) -> (usize, usize) {
        self.killed_bounds
    }

    // ── Position predicates ──

    /// Point is at beginning of accessible region.
    pub fn bobp(&self) -> bool {
        self.pt == self.begv
    }

    /// Point is at end of accessible region.
    pub fn eobp(&self) -> bool {
        self.pt == self.zv
    }

    /// Point is at beginning of a line.
    pub fn bolp(&self) -> bool {
        if self.pt == self.begv {
            return true;
        }
        // char just before point is a newline
        let idx = self.pt - 1; // 1-based to 0-based, then -1 for previous char
        if idx == 0 {
            return true;
        }
        self.text.char(idx - 1) == '\n'
    }

    /// Point is at end of a line.
    pub fn eolp(&self) -> bool {
        if self.pt == self.zv {
            return true;
        }
        self.text.char(self.pt - 1) == '\n'
    }

    // ── Point movement ──

    /// Move point to an absolute position (1-based). Clamps to accessible region.
    /// Returns the new point.
    pub fn goto_char(&mut self, pos: usize) -> usize {
        self.pt = pos.clamp(self.begv, self.zv);
        self.pt
    }

    /// Move point forward by n characters. Errors if hitting the boundary.
    pub fn forward_char(&mut self, n: isize) -> Result<usize, BufferError> {
        let new_pt = if n >= 0 {
            self.pt.saturating_add(n as usize)
        } else {
            self.pt.saturating_sub((-n) as usize)
        };

        if new_pt < self.begv {
            self.pt = self.begv;
            return Err(BufferError::BeginningOfBuffer);
        }
        if new_pt > self.zv {
            self.pt = self.zv;
            return Err(BufferError::EndOfBuffer);
        }

        self.pt = new_pt;
        Ok(self.pt)
    }

    /// Advance toward LIMIT while PREDICATE accepts each character.
    ///
    /// The text's two halves are read in place, as syntax.c's skip_chars
    /// reads the buffer text directly.
    pub fn skip_forward_while(
        &mut self,
        limit: usize,
        mut predicate: impl FnMut(char) -> bool,
    ) -> usize {
        let start = self.pt;
        let end = limit.clamp(start, self.zv);
        let mut remaining = end - start;
        let mut byte = self.text.char_to_byte(start - 1);
        let mut count = 0;
        'chunks: while remaining > 0 && byte < self.text.len_bytes() {
            let rest = self.text.rest_at_byte(byte);
            for ch in rest.chars() {
                if remaining == 0 || !predicate(ch) {
                    break 'chunks;
                }
                count += 1;
                remaining -= 1;
                byte += ch.len_utf8();
            }
        }
        self.pt += count;
        count
    }

    /// Retreat toward LIMIT while PREDICATE accepts each character.
    pub fn skip_backward_while(
        &mut self,
        limit: usize,
        mut predicate: impl FnMut(char) -> bool,
    ) -> usize {
        let start = self.pt;
        let end = limit.clamp(self.begv, start);
        let mut remaining = start - end;
        let mut byte = self.text.char_to_byte(start - 1);
        let mut count = 0;
        'chunks: while remaining > 0 && byte > 0 {
            let before = self.text.before_byte(byte);
            for ch in before.chars().rev() {
                if remaining == 0 || !predicate(ch) {
                    break 'chunks;
                }
                count += 1;
                remaining -= 1;
                byte -= ch.len_utf8();
            }
        }
        self.pt -= count;
        count
    }

    /// The text of 0-based character range [FROM, TO).
    fn text_range_string(&self, from: usize, to: usize) -> String {
        self.text_range_with_bytes(from, to).0
    }

    /// `text_range_string' with the store byte offsets of the range's
    /// ends, the end's counted from FROM's.
    fn text_range_with_bytes(&self, from: usize, to: usize) -> (String, usize, usize) {
        let to = to.min(self.text.len_chars()).max(from);
        let start = self.text.char_to_byte(from);
        let end = if self.text_is_ascii() {
            to
        } else {
            self.text
                .text
                .char_to_byte_near(to, std::iter::once((from, start)))
        };
        (self.text.text.string(start..end), start, end)
    }

    /// search.c's find_newline forward: the 0-based index of the first
    /// newline at or after char index FROM, scanned a half at a time with
    /// memchr and its position counted from FROM's.
    fn newline_at_or_after(&self, from: usize) -> Option<usize> {
        if from >= self.text.len_chars() {
            return None;
        }
        let start = self.text.char_to_byte(from);
        let mut byte = start;
        while byte < self.text.len_bytes() {
            let rest = self.text.rest_at_byte(byte);
            if let Some(offset) = memchr::memchr(b'\n', rest.as_bytes()) {
                let found = byte + offset;
                let index = from + self.text.text.chars_in(start..found);
                self.text.note_position(index, found);
                return Some(index);
            }
            byte += rest.len();
        }
        None
    }

    /// Whether every character of the text is one byte, so that a char
    /// index within a chunk is its byte index (the counts are kept).
    fn text_is_ascii(&self) -> bool {
        self.text.len_chars() == self.text.len_bytes()
    }

    /// search.c's find_newline backward: the 0-based index of the last
    /// newline before char index BEFORE.
    fn newline_before(&self, before: usize) -> Option<usize> {
        let before = before.min(self.text.len_chars());
        let end = self.text.char_to_byte(before);
        let mut byte = end;
        while byte > 0 {
            let text = self.text.before_byte(byte);
            let start = byte - text.len();
            if let Some(offset) = memchr::memrchr(b'\n', text.as_bytes()) {
                let found = start + offset;
                let index = before - self.text.text.chars_in(found..end);
                self.text.note_position(index, found);
                return Some(index);
            }
            byte = start;
        }
        None
    }

    /// Move to the beginning of the current line. Returns new point.
    pub fn beginning_of_line(&mut self) -> usize {
        self.pt = self.line_start_at(self.pt);
        self.pt
    }

    /// Return the beginning of the line containing POS without moving point.
    pub fn line_start_at(&self, pos: usize) -> usize {
        let pt = pos.clamp(self.begv, self.zv);
        let char_index = pt.saturating_sub(1).min(self.text.len_chars());
        let line_start = self
            .newline_before(char_index)
            .map_or(1, |newline| newline + 2);
        line_start.max(self.begv)
    }

    /// Move to the end of the current line. Returns new point.
    pub fn end_of_line(&mut self) -> usize {
        if self.pt == self.zv {
            return self.pt;
        }
        let idx0 = self.pt - 1; // 0-based
        self.pt = match self.newline_at_or_after(idx0) {
            // The 1-based position of the newline character.
            Some(newline) => (newline + 1).min(self.zv),
            None => self.zv,
        };
        self.pt
    }

    /// Move forward n lines. Returns the number of lines we couldn't move
    /// (0 means we moved all of them, like Emacs forward-line).
    pub fn forward_line(&mut self, n: isize) -> isize {
        if n == 0 {
            self.beginning_of_line();
            return 0;
        }

        let mut remaining = n.unsigned_abs();
        if n > 0 {
            while remaining > 0 {
                if self.pt >= self.zv {
                    return remaining as isize;
                }
                // Find next newline from current point, inside the
                // accessible region.
                let idx0 = self.pt - 1;
                let found = match self.newline_at_or_after(idx0) {
                    Some(newline) if newline + 1 < self.zv => {
                        // The position after the newline, 1-based.
                        self.pt = (newline + 2).min(self.zv);
                        true
                    }
                    _ => false,
                };
                if !found {
                    // GNU counts the unterminated final line as a line that
                    // can be crossed: moving from anywhere on it to ZV
                    // satisfies one requested forward-line step.  Only a
                    // call already at ZV has the full remaining shortage.
                    let crossed_final_line = self.pt < self.zv;
                    self.pt = self.zv;
                    return remaining.saturating_sub(usize::from(crossed_final_line)) as isize;
                }
                remaining -= 1;
            }
            0
        } else {
            // negative: move backward
            while remaining > 0 {
                if self.pt <= self.begv {
                    return -(remaining as isize);
                }
                // Go to beginning of current line first
                self.beginning_of_line();
                if self.pt <= self.begv {
                    return -(remaining as isize);
                }
                // Step back one char (over the newline before this line)
                self.pt -= 1;
                self.beginning_of_line();
                remaining -= 1;
            }
            0
        }
    }

    // ── Mark ──

    pub(crate) fn mark_object(&self) -> Option<MarkerRef> {
        match self.mark {
            BufferMark::Position(_) => None,
            BufferMark::Object { marker, .. } => Some(marker),
        }
    }

    pub(crate) fn install_mark_object(&mut self, owner: BufferRef, restored: Option<MarkerRef>) {
        let position = self.mark();
        if let Some(old) = self.mark_object() {
            old.detach();
        }
        let marker = restored.unwrap_or_else(MarkerRef::new);
        self.mark = BufferMark::Object { marker, owner };
        if restored.is_none()
            && let Some(position) = position
        {
            marker.attach(owner, position, self.marker_byte_position(position));
        }
    }

    /// GNU's byte position of character position POSITION
    /// (buf_charpos_to_bytepos): the stored text's byte offset, less a byte
    /// for each raw byte before it (stored in three bytes, two in GNU), and
    /// adjusted for each character the side table holds: one beyond
    /// Unicode is a three-byte placeholder for GNU's four or five bytes, a
    /// genuine U+E080..U+E0FF counted as a raw byte is three in both.
    pub(crate) fn marker_byte_position(&self, position: usize) -> usize {
        if !self.multibyte || self.text.len_bytes() == self.text.len_chars() {
            return position;
        }
        self.byte_position_at_stored(position, self.text.char_to_byte(position - 1))
    }

    /// `marker_byte_position' of POSITION, whose character starts at byte
    /// STORED of the text as stored.
    fn byte_position_at_stored(&self, position: usize, stored: usize) -> usize {
        if !self.multibyte || self.text.len_bytes() == self.text.len_chars() {
            return position;
        }
        let mut bytes = stored - self.text.raw_markers_before(stored);
        for &(at, code) in &self.extended_chars {
            if at >= position {
                break;
            }
            let length = crate::lisp::primitives::emacs_multibyte_char_len(code)
                .expect("valid buffer character");
            bytes = bytes + length + usize::from((0xE080..=0xE0FF).contains(&code)) - 3;
        }
        1 + bytes
    }

    fn refresh_marker_byte_positions(&self) {
        if let BufferMark::Object { owner, .. } = self.mark {
            for marker in owner.markers() {
                let position = marker.last_position();
                marker.set_positions(position, self.marker_byte_position(position));
            }
        }
    }

    pub fn mark(&self) -> Option<usize> {
        self.mark.position()
    }

    pub fn mark_active(&self) -> bool {
        !self.mark_active.is_nil()
    }

    /// `mark-active' as stored, any Lisp object.
    pub fn mark_active_value(&self) -> Value {
        self.mark_active
    }

    pub fn set_mark(&mut self, pos: usize) {
        self.set_mark_position(pos);
        self.mark_active = Value::T;
    }

    /// Move the buffer's mark marker without changing whether the region is
    /// active.  GNU's `set-marker' and the first half of `push-mark' do this;
    /// `set-mark' is the separate operation that activates it.
    pub fn set_mark_position(&mut self, pos: usize) {
        let pos = pos.clamp(1, self.size_total() + 1);
        match self.mark {
            BufferMark::Position(_) => self.mark = BufferMark::Position(Some(pos)),
            BufferMark::Object { marker, owner } => {
                marker.attach(owner, pos, self.marker_byte_position(pos))
            }
        }
    }

    /// A store into the slot: buffer.c keeps whatever `mark-active' is
    /// set to, mark or no mark.
    pub fn set_mark_active_value(&mut self, value: Value) {
        self.mark_active = value;
    }

    pub fn clear_mark(&mut self) {
        match self.mark {
            BufferMark::Position(_) => self.mark = BufferMark::Position(None),
            BufferMark::Object { marker, .. } => marker.detach(),
        }
        self.mark_active = Value::Nil;
    }

    pub fn deactivate_mark(&mut self) {
        self.mark_active = Value::Nil;
    }

    pub fn region(&self) -> Option<(usize, usize)> {
        self.mark().map(|m| {
            let a = self.pt.min(m);
            let b = self.pt.max(m);
            (a, b)
        })
    }

    // ── Text access ──

    /// Get the full buffer text as a String.
    pub fn buffer_string(&self) -> String {
        let start = self.begv - 1;
        let end = self.zv - 1;
        self.text.string(start..end)
    }

    /// The text for a scan apart from the buffer: the gap buffer's bytes,
    /// shared until the next change copies them.
    pub fn scan_text(&self) -> ScanText {
        self.text.scan_text()
    }

    /// The stored text's characters and UTF-8 bytes, which the buffer
    /// keeps.
    pub fn text_counts(&self) -> (usize, usize) {
        (self.text.len_chars(), self.text.len_bytes())
    }

    /// The text of [FROM, TO) (1-based), read past any narrowing.
    pub(crate) fn absolute_text_range(&self, from: usize, to: usize) -> String {
        self.text.string(from - 1..to - 1)
    }

    pub fn full_buffer_string(&self) -> String {
        self.text.whole_string()
    }

    /// Text of up to COUNT lines starting at 1-based LINE, one string per
    /// line without its terminator.
    pub fn lines_from(&self, line: usize, count: usize) -> Vec<String> {
        let total = self.text.len_lines();
        let start = line.saturating_sub(1).min(total);
        let end = start.saturating_add(count).min(total);
        (start..end)
            .map(|index| {
                let mut text = self.text.line(index);
                while text.ends_with(['\n', '\r']) {
                    text.pop();
                }
                text
            })
            .collect()
    }

    /// The stored text's byte offset of POS plus one: what a consumer of
    /// the UTF-8 text itself (tree-sitter) indexes by, which differs from
    /// GNU's byte position where raw bytes or characters beyond Unicode
    /// sit before POS.
    pub fn storage_byte_position(&self, pos: usize) -> Option<usize> {
        let char_len = self.text.len_chars();
        if pos == 0 || pos > char_len + 1 {
            return None;
        }
        Some(1 + self.text.char_to_byte(pos - 1))
    }

    /// The position of the character the stored text's byte offset
    /// BYTE - 1 lies in: `storage_byte_position' inverted.
    pub fn storage_byte_to_position(&self, byte: usize) -> Option<usize> {
        if byte == 0 || byte > self.text.len_bytes() + 1 {
            return None;
        }
        Some(self.text.byte_to_char(byte - 1) + 1)
    }

    /// Fposition_bytes: GNU's byte position of POS in [BEG, Z].
    pub fn position_bytes(&self, pos: usize) -> Option<usize> {
        let char_len = self.text.len_chars();
        if pos == 0 || pos > char_len + 1 {
            return None;
        }
        Some(self.marker_byte_position(pos))
    }

    /// Fbyte_to_position: the character at GNU byte position BYTE in
    /// [BEG_BYTE, Z_BYTE], the one a byte inside a character belongs to.
    pub fn byte_to_position(&self, byte: usize) -> Option<usize> {
        let end = self.text.len_chars() + 1;
        if byte == 0 || byte > self.marker_byte_position(end) {
            return None;
        }
        let (mut low, mut high) = (1, end);
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if self.marker_byte_position(middle) <= byte {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        Some(low)
    }

    /// The characters `buffer_substring' returns for [FROM, TO), read from
    /// the text's halves in place.
    pub fn substring_chars(
        &self,
        from: usize,
        to: usize,
    ) -> Result<impl Iterator<Item = char> + '_, BufferError> {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from > to {
            return Err(BufferError::InvalidPosition(from));
        }
        let (first, second) = self.text.range_halves(from - 1..to - 1);
        Ok(first.chars().chain(second.chars()))
    }

    /// The byte offsets in the text of positions FROM and TO (1-based).
    pub(crate) fn span_bytes(&self, from: usize, to: usize) -> (usize, usize) {
        (
            self.text.char_to_byte(from - 1),
            self.text.char_to_byte(to - 1),
        )
    }

    /// The text of byte offsets [START, END) as it stands, the part before
    /// the gap and the part after.
    pub(crate) fn byte_halves(&self, start: usize, end: usize) -> (&str, &str) {
        self.text.text.range_halves(start..end)
    }

    /// Remember that position POS (1-based) starts at byte offset BYTE, as
    /// a scan that counted both finds it (GNU keeps PT_BYTE with PT).
    pub(crate) fn note_position(&self, pos: usize, byte: usize) {
        self.text.note_position(pos - 1, byte);
    }

    /// [FROM, TO) (1-based) of an ASCII text as a slice of the half of the
    /// text holding it, so nothing is decoded; None when the text has a
    /// multibyte character or the range crosses the gap.
    pub(crate) fn ascii_chunk_range(&self, from: usize, to: usize) -> Option<&str> {
        if self.text.len_bytes() != self.text.len_chars() || from == 0 || from > to {
            return None;
        }
        let (start, end) = (from - 1, to - 1);
        if end > self.text.len_bytes() {
            return None;
        }
        if start == end {
            return Some("");
        }
        let (chunk, chunk_start, _) = self.text.chunk_at_byte(start);
        chunk.get(start - chunk_start..end - chunk_start)
    }

    /// Get a substring. Positions are 1-based, range is [from, to).
    pub fn buffer_substring(&self, from: usize, to: usize) -> Result<String, BufferError> {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from > to {
            return Err(BufferError::InvalidPosition(from));
        }
        Ok(self.text.string((from - 1)..(to - 1)))
    }

    /// The text of [FROM, TO) where it stands, when one side of the gap
    /// holds all of it.
    pub(crate) fn contiguous_text(&self, from: usize, to: usize) -> Option<&str> {
        match self.text.range_halves((from - 1)..(to - 1)) {
            (text, "") | ("", text) => Some(text),
            _ => None,
        }
    }

    pub fn extended_char_at(&self, pos: usize) -> Option<u32> {
        self.extended_chars
            .binary_search_by_key(&pos, |(position, _)| *position)
            .ok()
            .map(|index| self.extended_chars[index].1)
    }

    /// Whether any character of [FROM, TO) has a sidecar entry (a code
    /// outside Unicode's scalars), which the regexp haystack of the span
    /// then renders differently from the stored text.
    pub fn has_extended_chars_in(&self, from: usize, to: usize) -> bool {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        let first = self
            .extended_chars
            .partition_point(|(position, _)| *position < from);
        self.extended_chars
            .get(first)
            .is_some_and(|(position, _)| *position < to)
    }

    /// Return character-indexed sidecar entries relative to FROM.
    pub fn substring_extended_chars(&self, from: usize, to: usize) -> Vec<(usize, u32)> {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        self.extended_chars
            .iter()
            .filter(|(position, _)| from <= *position && *position < to)
            .map(|(position, code)| (position - from, *code))
            .collect()
    }

    pub fn set_inserted_extended_chars(&mut self, start: usize, chars: &[(usize, u32)]) {
        self.extended_chars
            .extend(chars.iter().map(|(offset, code)| (start + *offset, *code)));
        self.extended_chars
            .sort_unstable_by_key(|(position, _)| *position);
        if !chars.is_empty() {
            self.refresh_marker_byte_positions();
        }
    }

    /// `has_text_property_named' for the intervals meeting FROM to TO.
    pub fn has_text_property_named_in(&self, property: &str, from: usize, to: usize) -> bool {
        let Some(symbol) = crate::lisp::types::SymbolName::find(property) else {
            return false;
        };
        let Some(intervals) = &self.intervals else {
            return false;
        };
        let name = Value::Symbol(symbol);
        let from = from.clamp(intervals.origin(), intervals.end());
        let mut i = intervals.find(from);
        loop {
            if intervals.start(i) >= to {
                return false;
            }
            if plist_has_name(intervals.plist(i), name) {
                return true;
            }
            match intervals.next(i) {
                Some(next) => i = next,
                None => return false,
            }
        }
    }

    pub fn has_text_property_named(&self, property: &str) -> bool {
        // The symbol PROPERTY names, found once (no symbol has that name
        // when none exists): each stored name is then the same symbol or
        // not, as EQ compares them.
        let Some(symbol) = crate::lisp::types::SymbolName::find(property) else {
            return false;
        };
        let name = Value::Symbol(symbol);
        self.intervals
            .as_ref()
            .is_some_and(|intervals| intervals.plists().any(|plist| plist_has_name(plist, name)))
    }

    /// Rewrite every Lisp value stored in this buffer through COPY
    /// (image-template deep clone; see `Interpreter::deep_clone_image').
    pub(crate) fn rewrite_lisp_values(&mut self, copy: &mut impl FnMut(&Value) -> Value) {
        self.text_conversion_style = copy(&self.text_conversion_style);
        self.mark_active = copy(&self.mark_active);
        if let Some(intervals) = &mut self.intervals {
            intervals.rewrite_plists(|plist| copy(&plist));
        }
        self.undo_list = copy(&self.undo_list);
    }

    /// Visit every Lisp value retained by this buffer.  Garbage collection
    /// uses the same ownership boundary as `rewrite_lisp_values`: the text,
    /// positions and file metadata are native data, while the intervals'
    /// plists, the undo list and attached overlay objects are Lisp roots.
    pub(crate) fn visit_lisp_values(&self, visit: &mut impl FnMut(&Value)) {
        visit(&self.text_conversion_style);
        visit(&self.mark_active);
        if let Some(marker) = self.mark_object() {
            visit(&Value::Marker(marker));
        }
        if let Some(intervals) = &self.intervals {
            for plist in intervals.plists() {
                visit(&plist);
            }
        }
        visit(&self.undo_list);
        for value in self
            .killed_locals
            .iter()
            .flat_map(|slots| slots.iter().flatten())
        {
            visit(value);
        }
        self.overlays.visit_lisp_values(visit);
    }

    /// How many intervals this buffer holds (the census's `intervals').
    pub(crate) fn text_property_span_count(&self) -> usize {
        self.intervals.as_ref().map_or(0, Intervals::count)
    }

    /// Whether any text property is set.
    pub fn has_text_properties(&self) -> bool {
        self.intervals
            .as_ref()
            .is_some_and(|intervals| intervals.plists().any(|plist| !plist.is_nil()))
    }

    /// Whether the buffer has intervals (buffer_intervals non-null), every
    /// plist of which may be empty.
    pub fn has_interval_tree(&self) -> bool {
        self.intervals.is_some()
    }

    /// buffer_intervals.
    pub(crate) fn intervals(&self) -> Option<&Intervals> {
        self.intervals.as_ref()
    }

    /// set_buffer_intervals.
    pub(crate) fn set_intervals(&mut self, intervals: Option<Intervals>) {
        self.intervals = intervals;
    }

    /// PROP in the plist of the character at POS (none outside the
    /// accessible text), by the interval's own plist alone.
    pub fn text_property_at(&self, pos: usize, prop: impl PropertyKey) -> Option<Value> {
        let mut tail = self.text_plist_at(pos);
        while let Some(cell) = tail.cons_ref() {
            let Some(value) = cell.cdr.get().cons_ref() else {
                break;
            };
            if prop.is(cell.car.get()) {
                return Some(value.car.get());
            }
            tail = value.cdr.get();
        }
        None
    }

    /// The plist of the interval holding the character at POS, the
    /// interval's own list; nil outside the accessible text or without
    /// intervals.
    pub(crate) fn text_plist_at(&self, pos: usize) -> Value {
        if pos < self.point_min() || pos >= self.point_max() {
            return Value::Nil;
        }
        match &self.intervals {
            Some(intervals) => intervals.plist(intervals.find(pos)),
            None => Value::Nil,
        }
    }

    /// F over each interval from the one holding FROM (find_interval)
    /// while it starts before TO, as textprop.c's loops walk them: its
    /// start, end and plist.  Nothing without intervals.
    pub(crate) fn try_for_each_interval<E>(
        &self,
        from: usize,
        to: usize,
        mut f: impl FnMut(usize, usize, Value) -> Result<(), E>,
    ) -> Result<(), E> {
        let Some(intervals) = &self.intervals else {
            return Ok(());
        };
        let mut i = intervals.find(from.clamp(intervals.origin(), intervals.end()));
        loop {
            f(
                intervals.start(i),
                intervals.last_pos(i),
                intervals.plist(i),
            )?;
            match intervals.next(i) {
                Some(next) if intervals.start(next) < to => i = next,
                _ => return Ok(()),
            }
        }
    }

    /// The interval holding the character at POS, by its index, and its
    /// plist: None without intervals.
    pub(crate) fn interval_at(&self, pos: usize) -> Option<(usize, Value)> {
        let intervals = self.intervals.as_ref()?;
        let i = intervals.find(pos.clamp(intervals.origin(), intervals.end()));
        Some((i, intervals.plist(i)))
    }

    /// The buffer's intervals over its whole text [BEG, Z), narrowing
    /// aside; None where buffer_intervals is null.
    pub(crate) fn property_intervals(&self) -> Option<PropertyIntervals<'_>> {
        self.intervals.as_ref().map(PropertyIntervals::new)
    }

    /// The interval [start, end) around POS, clipped to the accessible
    /// text: where the plist is constant (syntax.h's b_property and
    /// e_property for update_syntax_table), the whole accessible text
    /// without intervals.
    pub(crate) fn text_property_interval_around(&self, pos: usize) -> (usize, usize) {
        let begv = self.point_min();
        let zv = self.point_max();
        let Some(intervals) = &self.intervals else {
            return (begv, zv);
        };
        let i = intervals.find(pos.clamp(intervals.origin(), intervals.end()));
        let start = intervals.start(i).max(begv);
        let end = intervals.last_pos(i).min(zv).max(start);
        (start, end)
    }

    /// textprop.c:modify_text_properties's part of a property change:
    /// the first change since a save recorded, the modification counted.
    fn begin_text_property_change(&mut self, start: usize, end: usize) {
        if !self.is_modified() {
            self.record_first_change();
        }
        modiff_incr(&mut self.modiff, 1);
        self.log_edit(start, end, end, false);
    }

    /// The intervals [START, END) of a change made, created where there
    /// are none (validate_interval_range with FORCE): None for no text in
    /// the range.
    fn intervals_for_change(
        &mut self,
        start: usize,
        end: usize,
        force: bool,
    ) -> Option<(usize, usize)> {
        let start = start.max(self.point_min());
        let end = end.min(self.point_max());
        if start >= end {
            return None;
        }
        if self.intervals.is_none() {
            if !force {
                return None;
            }
            self.intervals = Some(Intervals::root(1, self.size_total() + 1));
        }
        Some((start, end))
    }

    /// Whether add_text_properties_1 of PROPERTIES (validated) over
    /// [START, END) would change a plist.
    pub(crate) fn would_add_text_properties(
        &self,
        start: usize,
        end: usize,
        properties: Value,
    ) -> bool {
        let (start, end) = (start.max(self.point_min()), end.min(self.point_max()));
        if start >= end || properties.is_nil() {
            return false;
        }
        match &self.intervals {
            // A new root interval has no properties.
            None => true,
            Some(intervals) => {
                intervals::would_add_properties(intervals, start, end - start, properties)
                    .unwrap_or(true)
            }
        }
    }

    /// Whether removing the properties of PLIST (or of LIST's names)
    /// from [START, END) would change a plist.
    pub(crate) fn would_remove_text_properties(
        &self,
        start: usize,
        end: usize,
        plist: Value,
        list: Value,
    ) -> bool {
        let (start, end) = (start.max(self.point_min()), end.min(self.point_max()));
        match &self.intervals {
            Some(intervals) if start < end => {
                intervals::would_remove_properties(intervals, start, end - start, plist, list)
                    .unwrap_or(true)
            }
            _ => false,
        }
    }

    /// textprop.c:add_text_properties_1's change of the buffer, after its
    /// hooks: PROPERTIES (validated, non-nil) over [START, END), counted
    /// and recorded for undo as modify_text_properties and add_properties
    /// do.  Whether a plist changed.
    pub(crate) fn add_text_properties_range(
        &mut self,
        start: usize,
        end: usize,
        properties: Value,
        set_type: PropertySetType,
        destructive: bool,
    ) -> Result<bool, crate::lisp::types::LispError> {
        if !self.would_add_text_properties(start, end, properties) {
            return Ok(false);
        }
        let Some((start, end)) = self.intervals_for_change(start, end, true) else {
            return Ok(false);
        };
        self.begin_text_property_change(start, end);
        let disabled = self.undo_recording_disabled();
        let intervals = self.intervals.as_mut().expect("intervals for the change");
        if disabled {
            intervals::add_text_properties_range(
                intervals,
                start,
                end - start,
                properties,
                &mut intervals::NoRecord,
                set_type,
                destructive,
            )
        } else {
            let mut recorder = UndoRecorder {
                undo_list: &mut self.undo_list,
            };
            intervals::add_text_properties_range(
                intervals,
                start,
                end - start,
                properties,
                &mut recorder,
                set_type,
                destructive,
            )
        }
    }

    /// textprop.c:set_text_properties's change of the buffer: PROPERTIES
    /// (validated) the plist of [START, END), intervals made for
    /// properties to set; a COHERENT change (Fset_text_properties)
    /// counted as modify_text_properties counts it.  Whether it set
    /// anything.
    pub(crate) fn set_text_properties_range(
        &mut self,
        start: usize,
        end: usize,
        properties: Value,
        coherent: bool,
    ) -> Result<bool, crate::lisp::types::LispError> {
        let Some((start, end)) = self.intervals_for_change(start, end, !properties.is_nil()) else {
            return Ok(false);
        };
        if coherent {
            self.begin_text_property_change(start, end);
        }
        self.set_text_properties_1(start, end, properties)?;
        Ok(true)
    }

    /// textprop.c:set_text_properties_1 over [START, END) of the buffer's
    /// intervals (which there are), undo recorded.
    fn set_text_properties_1(
        &mut self,
        start: usize,
        end: usize,
        properties: Value,
    ) -> Result<(), crate::lisp::types::LispError> {
        let disabled = self.undo_recording_disabled();
        let Some(intervals) = self.intervals.as_mut() else {
            return Ok(());
        };
        if disabled {
            intervals::set_text_properties_range(
                intervals,
                start,
                end - start,
                properties,
                &mut intervals::NoRecord,
            )
        } else {
            let mut recorder = UndoRecorder {
                undo_list: &mut self.undo_list,
            };
            intervals::set_text_properties_range(
                intervals,
                start,
                end - start,
                properties,
                &mut recorder,
            )
        }
    }

    /// Fremove_text_properties' (PLIST the properties) and
    /// Fremove_list_of_text_properties' (PLIST nil, LIST the names) change
    /// of the buffer after their hooks: counted, recorded for undo.
    /// Whether one went.
    pub(crate) fn remove_text_properties_range(
        &mut self,
        start: usize,
        end: usize,
        plist: Value,
        list: Value,
    ) -> Result<bool, crate::lisp::types::LispError> {
        if !self.would_remove_text_properties(start, end, plist, list) {
            return Ok(false);
        }
        let (start, end) = (start.max(self.point_min()), end.min(self.point_max()));
        self.begin_text_property_change(start, end);
        let disabled = self.undo_recording_disabled();
        let intervals = self.intervals.as_mut().expect("intervals with properties");
        if disabled {
            intervals::remove_text_properties_range(
                intervals,
                start,
                end - start,
                plist,
                list,
                &mut intervals::NoRecord,
            )
        } else {
            let mut recorder = UndoRecorder {
                undo_list: &mut self.undo_list,
            };
            intervals::remove_text_properties_range(
                intervals,
                start,
                end - start,
                plist,
                list,
                &mut recorder,
            )
        }
    }

    /// Fput_text_property of NAME's VALUE over [START, END), for callers
    /// that are not a Lisp primitive (its hooks the caller's).
    pub fn put_text_property(
        &mut self,
        start: usize,
        end: usize,
        name: PropertyName,
        value: Value,
    ) {
        let _ = self.add_text_properties_range(
            start,
            end,
            Value::list([name.value(), value]),
            PropertySetType::Replace,
            true,
        );
    }

    /// Fadd_text_properties of PROPS (pairs) over [START, END), for
    /// callers that are not a Lisp primitive.
    pub fn add_text_properties(
        &mut self,
        start: usize,
        end: usize,
        props: &[(PropertyName, Value)],
    ) {
        if props.is_empty() {
            return;
        }
        let plist = Value::list(
            props
                .iter()
                .flat_map(|(name, value)| [name.value(), *value]),
        );
        let _ = self.add_text_properties_range(start, end, plist, PropertySetType::Replace, true);
    }

    /// Fset_text_properties of PROPS (pairs) over [START, END), for
    /// callers that are not a Lisp primitive.
    pub fn set_text_properties(
        &mut self,
        start: usize,
        end: usize,
        props: &[(PropertyName, Value)],
    ) {
        let plist = Value::list(
            props
                .iter()
                .flat_map(|(name, value)| [name.value(), *value]),
        );
        let _ = self.set_text_properties_range(start, end, plist, true);
    }

    /// Fremove_list_of_text_properties of NAMES over [START, END), for
    /// callers that are not a Lisp primitive.
    pub fn remove_list_of_text_properties(
        &mut self,
        start: usize,
        end: usize,
        names: &[PropertyName],
    ) {
        let list = Value::list(names.iter().map(|name| name.value()));
        let _ = self.remove_text_properties_range(start, end, Value::Nil, list);
    }

    /// The intervals of [FROM, TO) as a string's (copy_intervals, from 0);
    /// none where the buffer has none there.
    pub(crate) fn copy_intervals(&self, from: usize, to: usize) -> Option<Intervals> {
        let intervals = self.intervals.as_ref()?;
        if from >= to {
            return None;
        }
        intervals.copy(from, to - from).ok().flatten()
    }

    /// editfns.c:make_buffer_string_both's properties for [FROM, TO):
    /// its intervals, unless no property changes before TO
    /// (next-property-change) and the text at FROM has none.
    pub(crate) fn string_intervals(&self, from: usize, to: usize) -> Option<Intervals> {
        let tree = self.intervals.as_ref()?;
        if from >= to {
            return None;
        }
        let first = tree.find(from);
        let plist = tree.plist(first);
        let changes = std::iter::successors(tree.next(first), |&i| tree.next(i))
            .take_while(|&i| tree.start(i) < to)
            .any(|i| !intervals::intervals_equal(plist, tree.plist(i)));
        if !changes && plist.is_nil() {
            return None;
        }
        self.copy_intervals(from, to)
    }

    fn invalidate_char_cache(&mut self) {
        self.char_cache.get_mut().valid = false;
    }

    /// Character at position (1-based). None if out of range.
    pub fn char_at(&self, pos: usize) -> Option<char> {
        if pos < self.begv || pos >= self.zv {
            return None;
        }
        let index = pos - 1;
        // An ASCII text's character is its byte, read in place as
        // FETCH_CHAR reads it, with no chunk decoded into the cache.
        if self.text.len_chars() == self.text.len_bytes() {
            return Some(char::from(self.text.text.byte(index)));
        }
        if let Some(ch) = self.char_cache.borrow().character(index) {
            return Some(ch);
        }
        // A read away from the last one (`following-char' at each line's
        // start) decodes its character where it stands, as FETCH_CHAR does.
        {
            let mut cache = self.char_cache.borrow_mut();
            let scanning = cache
                .last_single
                .is_some_and(|last| last.abs_diff(index) <= 1);
            cache.last_single = Some(index);
            if !scanning {
                drop(cache);
                let byte = self.text.char_to_byte(index);
                return self.text.rest_at_byte(byte).chars().next();
            }
        }

        // A window of the half around INDEX, decoded once for the reads
        // near it (a scan's next characters or its previous ones).
        let (_, _, chunk_start) = self.text.chunk_at_char(index);
        let first = index.saturating_sub(CHAR_CACHE_BEHIND).max(chunk_start);
        let byte = self.text.char_to_byte(first);
        let mut cache = self.char_cache.borrow_mut();
        cache.start = first;
        cache.chars.clear();
        cache
            .chars
            .extend(self.text.rest_at_byte(byte).chars().take(CHAR_CACHE_WINDOW));
        cache.valid = true;
        cache.character(index)
    }

    /// Character just after point.
    pub fn char_after(&self) -> Option<char> {
        self.char_at(self.pt)
    }

    /// Character just before point.
    pub fn char_before(&self) -> Option<char> {
        if self.pt <= self.begv {
            None
        } else {
            self.char_at(self.pt - 1)
        }
    }

    // ── Insertion ──

    /// Insert text at point and advance point past it.
    pub fn insert(&mut self, s: &str) -> usize {
        self.insert_text(
            s,
            InsertedProperties::PLAIN,
            false,
            None,
            &PropertyVariables::NONE,
        )
    }

    /// `insert' with the properties PROPS gives the text and the
    /// variables VARS that its stickiness reads.
    pub(crate) fn insert_with(
        &mut self,
        s: &str,
        props: InsertedProperties<'_>,
        before_markers: bool,
        vars: &PropertyVariables,
    ) -> usize {
        self.insert_text(s, props, before_markers, None, vars)
    }

    /// `insert' of a text whose COUNTS its maker already has.
    pub fn insert_counted(&mut self, s: &str, counts: TextCounts) -> usize {
        self.insert_text(
            s,
            InsertedProperties::PLAIN,
            false,
            Some(counts),
            &PropertyVariables::NONE,
        )
    }

    /// `insert_counted' with the variables VARS an insertion reads.
    pub(crate) fn insert_counted_with(
        &mut self,
        s: &str,
        counts: TextCounts,
        vars: &PropertyVariables,
    ) -> usize {
        self.insert_text(s, InsertedProperties::PLAIN, false, Some(counts), vars)
    }

    pub(crate) fn insert_before_markers(&mut self, s: &str) -> usize {
        self.insert_text(
            s,
            InsertedProperties::PLAIN,
            true,
            None,
            &PropertyVariables::NONE,
        )
    }

    /// intervals.c:offset_intervals for NCHARS characters inserted at
    /// POS, then graft_intervals_into_buffer of PROPS over them.
    fn adjust_intervals_for_insert(
        &mut self,
        pos: usize,
        nchars: usize,
        props: InsertedProperties<'_>,
        vars: &PropertyVariables,
    ) {
        if nchars == 0 {
            return;
        }
        if let Some(intervals) = &mut self.intervals {
            let mut host = *vars;
            let _ = intervals.adjust_for_insertion(&mut host, pos, nchars);
        }
        self.graft_intervals(props, pos, nchars);
    }

    /// intervals.c:graft_intervals_into_buffer of PROPS over the LENGTH
    /// characters just inserted at POSITION.
    fn graft_intervals(&mut self, props: InsertedProperties<'_>, position: usize, length: usize) {
        match props {
            InsertedProperties::None { inherit } => {
                // Without inheritance, the new text's properties are
                // cleared out.
                if !inherit && self.intervals.is_some() && length > 0 {
                    let _ = self.set_text_properties_1(position, position + length, Value::Nil);
                }
            }
            InsertedProperties::Intervals { source, inherit } => {
                if self.size_total() == length {
                    // The inserted text is the whole buffer: a copy of the
                    // interval structure.
                    self.intervals = Some(intervals::reproduce(source, 1));
                    return;
                }
                if self.intervals.is_none() {
                    self.intervals = Some(Intervals::root(1, self.size_total() + 1));
                }
                let tree = self.intervals.as_mut().expect("intervals for the graft");
                let _ = intervals::graft_into(tree, source, position, inherit);
            }
        }
    }

    fn insert_text(
        &mut self,
        s: &str,
        props: InsertedProperties<'_>,
        before_markers: bool,
        counts: Option<TextCounts>,
        vars: &PropertyVariables,
    ) -> usize {
        if s.is_empty() {
            return self.pt;
        }

        let insert_at = self.pt;
        let idx0 = self.pt - 1; // 0-based
        // The text keeps its length; what it grew by is the insertion's.
        let before = self.text.len_chars();
        self.text.insert_counted(idx0, s, counts);
        let nchars = self.text.len_chars() - before;
        self.invalidate_char_cache();
        self.record_insert(insert_at, nchars);

        // Advance point past insertion
        self.pt += nchars;

        // Adjust zv (the buffer grew)
        self.zv += nchars;

        match &mut self.mark {
            BufferMark::Position(mark) => {
                if let Some(mark) = mark
                    && *mark > insert_at
                {
                    *mark += nchars;
                }
            }
            BufferMark::Object { owner, .. } => {
                // A unibyte buffer's character is a byte.
                let nbytes = if self.multibyte {
                    crate::lisp::primitives::lisp_string_storage_byte_len(s, true, &[])
                } else {
                    nchars
                };
                for marker in owner.markers() {
                    let position = marker.last_position();
                    if position > insert_at || (position == insert_at && marker.insertion_type()) {
                        marker.set_positions(position + nchars, marker.bytepos() + nbytes);
                    }
                }
            }
        }

        // Adjust overlays
        self.overlays.insert_gap(insert_at, nchars, before_markers);

        self.adjust_intervals_for_insert(insert_at, nchars, props, vars);
        self.adjust_extended_chars_for_insert(insert_at, nchars);

        modiff_incr(&mut self.modiff, nchars);
        self.chars_modiff = self.modiff;
        self.log_edit(insert_at, insert_at, insert_at + nchars, true);
        self.pt
    }

    /// Replace [FROM, TO) by TEXT of the same length in place, EXTENDED
    /// (offsets into TEXT) the side-table entries of its characters, as
    /// casefiddle.c and translate-region do: insdel.c's modify_text, the
    /// change recorded as one record_change unless NOUNDO, then the text.
    /// Text properties and markers are untouched.
    pub fn replace_region_in_place(
        &mut self,
        from: usize,
        to: usize,
        text: &str,
        extended: &[(usize, u32)],
        noundo: bool,
    ) {
        if noundo {
            modiff_incr(&mut self.modiff, to - from);
            self.chars_modiff = self.modiff;
        } else {
            self.modify_text(from, to);
            self.record_change(from, to - from);
        }
        self.replace_text_in_place(from, to, text, extended);
    }

    /// insdel.c:modify_text's count of a change to [START, END), the first
    /// change since a save recorded before it.
    pub(crate) fn modify_text(&mut self, start: usize, end: usize) {
        if !self.is_modified() {
            self.record_first_change();
        }
        modiff_incr(&mut self.modiff, end - start);
        self.chars_modiff = self.modiff;
    }

    /// editfns.c:subst-char-in-region's modify_text under NOUNDO (a list
    /// of t, so no first change), which leaves a buffer unmodified when
    /// the count moved by one.
    pub(crate) fn modify_text_unrecorded(&mut self, start: usize, end: usize) {
        modiff_incr(&mut self.modiff, end - start);
        self.chars_modiff = self.modiff;
        if self.save_modiff == self.modiff - 1 {
            self.save_modiff = self.modiff;
        }
        if self.autosave_modiff == self.modiff - 1 {
            self.autosave_modiff = self.modiff;
        }
    }

    /// undo.c:record_change: the LENGTH characters at BEG about to be
    /// replaced by as many, recorded as their deletion and an insertion.
    /// Their string (make_buffer_string, with its properties) is made
    /// before record_delete looks at the undo list, so a buffer without
    /// undo still allocates it, as casify_region's record_delete does.
    pub(crate) fn record_change(&mut self, beg: usize, length: usize) {
        let old = self.text_range_string(beg - 1, beg - 1 + length);
        let string = self.deleted_text_value(&old, beg, beg + length);
        if self.undo_recording_disabled() {
            return;
        }
        self.record_delete(beg, string, length);
        self.record_insert(beg, length);
    }

    /// editfns.c:Ftranspose_regions after prepare_to_modify_buffer, with
    /// START1 <= END1 <= START2 <= END2 and the regions not adjacent with
    /// one empty.  modify_text and record_change as GNU calls them in each
    /// case (both regions adjacent, separate regions of equal byte length,
    /// the second longer or shorter in bytes), each region's (and the
    /// text between's) intervals copied, the properties cleared where GNU
    /// clears them, the text rearranged where it stands and the copies
    /// grafted without inheritance where GNU grafts them; then
    /// transpose_markers (point and the mark included, overlays not)
    /// unless LEAVE_MARKERS.  GIVEN is the regions as the caller gave them
    /// (each in order, the two not swapped): GNU clears the properties of
    /// the span from the first's start to the second's end
    /// (validate_interval_range of STARTR1 and ENDR2, nothing when they
    /// are equal), or of each given region when they are equal in bytes.
    pub(crate) fn transpose_regions(
        &mut self,
        (start1, end1): (usize, usize),
        (start2, end2): (usize, usize),
        given: [(usize, usize); 2],
        leave_markers: bool,
    ) {
        let (len1, len2, len_mid) = (end1 - start1, end2 - start2, start2 - end1);
        let byte = |position: usize| self.marker_byte_position(position);
        let (len1_byte, len2_byte) = (byte(end1) - byte(start1), byte(end2) - byte(start2));
        let piece = |from: usize, to: usize| {
            (
                self.text.string((from - 1)..(to - 1)),
                self.substring_extended_chars(from, to),
            )
        };
        let (text1, extended1) = piece(start1, end1);
        let (text_mid, extended_mid) = piece(end1, start2);
        let (text2, extended2) = piece(start2, end2);
        let mut text = String::with_capacity(text1.len() + text_mid.len() + text2.len());
        let mut extended = Vec::new();
        for (part, part_extended) in [
            (&text2, &extended2),
            (&text_mid, &extended_mid),
            (&text1, &extended1),
        ] {
            let offset = text.chars().count();
            text.push_str(part);
            extended.extend(part_extended.iter().map(|(at, code)| (offset + at, *code)));
        }
        let clear = |buffer: &mut Self, from: usize, to: usize| {
            if from != to {
                let _ = buffer.set_text_properties_1(from.min(to), from.max(to), Value::Nil);
            }
        };
        let [(given_start1, given_end1), (given_start2, given_end2)] = given;
        let graft =
            |buffer: &mut Self, tree: &Option<Intervals>, position: usize, length: usize| {
                let props = match tree {
                    Some(source) => InsertedProperties::Intervals {
                        source,
                        inherit: false,
                    },
                    None => InsertedProperties::PLAIN,
                };
                buffer.graft_intervals(props, position, length);
            };
        if end1 == start2 {
            self.modify_text(start1, end2);
            self.record_change(start1, len1 + len2);
            let tree1 = self.copy_intervals(start1, end1);
            let tree2 = self.copy_intervals(start2, end2);
            clear(self, given_start1, given_end2);
            self.replace_text_in_place(start1, end2, &text, &extended);
            graft(self, &tree1, start1 + len2, len1);
            graft(self, &tree2, start1, len2);
        } else if len1_byte == len2_byte {
            self.modify_text(start1, end2);
            self.record_change(start1, len1);
            self.record_change(start2, len2);
            let tree1 = self.copy_intervals(start1, end1);
            let tree2 = self.copy_intervals(start2, end2);
            clear(self, given_start1, given_end1);
            clear(self, given_start2, given_end2);
            self.replace_text_in_place(start1, end2, &text, &extended);
            // GNU's positions, which assume equal lengths in characters.
            graft(self, &tree1, start2, len1);
            graft(self, &tree2, start1, len2);
        } else {
            // The second region shorter: record_change before modify_text.
            if len1_byte < len2_byte {
                self.modify_text(start1, end2);
                self.record_change(start1, end2 - start1);
            } else {
                self.record_change(start1, end2 - start1);
                self.modify_text(start1, end2);
            }
            let tree1 = self.copy_intervals(start1, end1);
            let tree_mid = self.copy_intervals(end1, start2);
            let tree2 = self.copy_intervals(start2, end2);
            clear(self, given_start1, given_end2);
            self.replace_text_in_place(start1, end2, &text, &extended);
            graft(self, &tree1, end2 - len1, len1);
            graft(self, &tree_mid, start1 + len2, len_mid);
            graft(self, &tree2, start1, len2);
        }
        if !leave_markers {
            self.transpose_markers(start1, end1, start2, end2);
        }
    }

    /// editfns.c:transpose_markers: point and each marker in a region
    /// moved by the other's length and the distance between them, those
    /// between by the difference of the lengths.
    fn transpose_markers(&mut self, start1: usize, end1: usize, start2: usize, end2: usize) {
        self.pt = transposed_position(self.pt, start1, end1, start2, end2);
        self.transpose_marker_positions(start1, end1, start2, end2);
    }

    /// transpose_markers over another buffer of the text's indirect
    /// family, whose point and bounds are markers in GNU while it is not
    /// current: they move with the mark and the markers.
    pub(crate) fn transpose_family_positions(
        &mut self,
        start1: usize,
        end1: usize,
        start2: usize,
        end2: usize,
    ) {
        let moved = |position| transposed_position(position, start1, end1, start2, end2);
        self.pt = moved(self.pt);
        self.begv = moved(self.begv);
        self.zv = moved(self.zv).max(self.begv);
        self.pt = self.pt.clamp(self.begv, self.zv);
        self.transpose_marker_positions(start1, end1, start2, end2);
    }

    fn transpose_marker_positions(
        &mut self,
        start1: usize,
        end1: usize,
        start2: usize,
        end2: usize,
    ) {
        let moved = |position| transposed_position(position, start1, end1, start2, end2);
        match &mut self.mark {
            BufferMark::Position(mark) => *mark = mark.map(moved),
            BufferMark::Object { owner, .. } => {
                for marker in owner.markers() {
                    let position = moved(marker.last_position());
                    marker.set_positions(position, self.marker_byte_position(position));
                }
            }
        }
    }

    /// Another buffer of this text's indirect family changed it where it
    /// stands: [FROM, OLD_TO) is now TEXT (EXTENDED its side table) and the
    /// whole text has INTERVALS, as GNU's family shares one buffer_text.
    /// Point, the bounds, the mark and the markers keep their character
    /// positions, as markers do across an edit in place and
    /// replace_range_2; nothing is recorded or counted here.
    pub(crate) fn mirror_in_place_edit(
        &mut self,
        from: usize,
        old_to: usize,
        text: &str,
        extended: &[(usize, u32)],
        intervals: Option<Intervals>,
    ) {
        let inschars = text.chars().count();
        self.text.remove(from - 1..old_to - 1);
        self.text.insert(from - 1, text);
        self.invalidate_char_cache();
        self.adjust_extended_chars_for_delete(from, old_to);
        self.adjust_extended_chars_for_insert(from, inschars);
        self.extended_chars
            .extend(extended.iter().map(|(offset, code)| (from + offset, *code)));
        self.extended_chars
            .sort_unstable_by_key(|(position, _)| *position);
        self.intervals = intervals;
        let z = self.size_total() + 1;
        self.zv = self.zv.min(z);
        self.begv = self.begv.min(self.zv);
        self.pt = self.pt.clamp(self.begv, self.zv);
        self.refresh_marker_byte_positions();
        self.log_edit(from, old_to, from + inschars, true);
    }

    /// The whole text's intervals, for another buffer of its family.
    pub(crate) fn intervals_snapshot(&self) -> Option<Intervals> {
        self.intervals.clone()
    }

    /// The stored text of [FROM, TO).
    pub(crate) fn stored_text(&self, from: usize, to: usize) -> String {
        self.text_range_string(from - 1, to - 1)
    }

    /// casefiddle.c:casify_region's record_delete of [FROM, TO) as
    /// make_buffer_string makes it, before the text is cased: the string
    /// made even in a buffer without undo, as in record_change.
    pub(crate) fn record_casing_deletion(&mut self, from: usize, to: usize) {
        let old = self.text_range_string(from - 1, to - 1);
        let string = self.deleted_text_value(&old, from, to);
        if self.undo_recording_disabled() {
            return;
        }
        self.record_delete(from, string, to - from);
    }

    /// casefiddle.c:casify_region's record_insert of the LENGTH characters
    /// the casing left at FROM.
    pub(crate) fn record_casing_insertion(&mut self, from: usize, length: usize) {
        self.record_insert(from, length);
    }

    /// insdel.c:replace_range_2 without MARKERS, as
    /// do_casify_multibyte_region calls it: the character at FROM replaced
    /// by TEXT (EXTENDED its side table) where it stands, unrecorded.
    /// Markers and overlays keep their character positions (marker byte
    /// positions follow the new text), the intervals are offset at FROM
    /// (offset_intervals), point moves as a marker would, and the change
    /// counts as the characters deleted and inserted.
    pub(crate) fn replace_char_unrecorded(
        &mut self,
        from: usize,
        text: &str,
        extended: &[(usize, u32)],
        vars: &PropertyVariables,
    ) {
        let inschars = text.chars().count();
        let to = from + 1;
        self.text.remove(from - 1..to - 1);
        self.text.insert(from - 1, text);
        self.invalidate_char_cache();
        self.adjust_extended_chars_for_delete(from, to);
        self.adjust_extended_chars_for_insert(from, inschars);
        self.extended_chars
            .extend(extended.iter().map(|(offset, code)| (from + offset, *code)));
        self.extended_chars
            .sort_unstable_by_key(|(position, _)| *position);
        self.zv = self.zv + inschars - 1;
        self.offset_intervals(from, inschars as isize - 1, vars);
        if from < self.pt && inschars != 1 {
            self.pt = self.pt + inschars - 1;
        }
        self.refresh_marker_byte_positions();
        modiff_incr(&mut self.modiff, 1 + inschars);
        self.chars_modiff = self.modiff;
        self.log_edit(from, to, from + inschars, true);
    }

    /// [FROM, TO) replaced by TEXT of the same length where it stands,
    /// unrecorded and uncounted.
    pub(crate) fn replace_text_in_place(
        &mut self,
        from: usize,
        to: usize,
        text: &str,
        extended: &[(usize, u32)],
    ) {
        self.text.remove(from - 1..to - 1);
        self.text.insert(from - 1, text);
        self.extended_chars
            .retain(|(position, _)| *position < from || *position >= to);
        self.extended_chars
            .extend(extended.iter().map(|(offset, code)| (from + offset, *code)));
        self.extended_chars
            .sort_unstable_by_key(|(position, _)| *position);
        self.invalidate_char_cache();
        self.refresh_marker_byte_positions();
        self.log_edit(from, to, to, true);
    }

    /// insdel.c:replace_range's buffer change: [FROM, TO) replaced by
    /// TEXT (EXTENDED its side table, offsets into TEXT), clamped to the
    /// accessible region, without the change hooks or text properties
    /// (the caller grafts the new text's).  The undo list records the
    /// insertion first, then the deletion, so undoing deletes after it
    /// reinserts; with MARKERS, markers inside the old text move to FROM
    /// and those at or after TO shift (adjust_markers_for_replace), and
    /// overlays take the insertion as if made before markers at TO, then
    /// the deletion.  Point moves as a marker would.  Returns FROM.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replace_range(
        &mut self,
        from: usize,
        to: usize,
        text: &str,
        extended: &[(usize, u32)],
        markers: bool,
        props: InsertedProperties<'_>,
        vars: &PropertyVariables,
    ) -> usize {
        let from = from.max(self.begv);
        let to = to.min(self.zv).max(from);
        let nchars_del = to - from;
        let inschars = text.chars().count();
        if nchars_del == 0 && inschars == 0 {
            return from;
        }
        let from0 = from - 1;
        let old_pt = self.pt;
        let deleted = self.text_range_string(from0, to - 1);
        // The insertion is recorded first, so that undoing reinserts the
        // deletion before it deletes the insertion; both whenever undo is
        // kept, either of them empty.
        if !self.undo_recording_disabled() {
            let string = self.deleted_text_value(&deleted, from, to);
            self.record_insert(from + nchars_del, inschars);
            self.record_delete(from, string, nchars_del);
        }
        let from_byte = self.marker_byte_position(from);
        let to_byte = self.marker_byte_position(to);
        if nchars_del > 0 {
            self.text.remove_text(from0..to - 1, &deleted);
        }
        if inschars > 0 {
            self.text.insert(from0, text);
        }
        self.invalidate_char_cache();
        let new_bytes =
            crate::lisp::primitives::lisp_string_storage_byte_len(text, self.multibyte, extended);
        let shift = |position: usize| position + inschars - nchars_del;
        if markers {
            match &mut self.mark {
                BufferMark::Position(mark) => {
                    if let Some(mark) = mark {
                        if *mark >= to {
                            *mark = shift(*mark);
                        } else if *mark > from {
                            *mark = from;
                        }
                    }
                }
                BufferMark::Object { owner, .. } => {
                    for marker in owner.markers() {
                        let position = marker.last_position();
                        if position >= to {
                            marker.set_positions(
                                shift(position),
                                marker.bytepos() + new_bytes - (to_byte - from_byte),
                            );
                        } else if position > from {
                            marker.set_positions(from, from_byte);
                        }
                    }
                }
            }
        }
        if from < old_pt {
            self.pt = from + inschars + old_pt - old_pt.min(to);
        }
        self.overlays.insert_gap(to, inschars, true);
        if nchars_del > 0 {
            self.overlays.delete_gap(from, nchars_del);
        }
        self.adjust_extended_chars_for_delete(from, to);
        self.adjust_extended_chars_for_insert(from, inschars);
        self.extended_chars
            .extend(extended.iter().map(|(offset, code)| (from + offset, *code)));
        self.extended_chars
            .sort_unstable_by_key(|(position, _)| *position);
        // insdel.c:replace_range: offset_intervals by the change in length
        // at FROM, then graft_intervals_into_buffer of the new text's.
        self.offset_intervals(from, inschars as isize - nchars_del as isize, vars);
        self.graft_intervals(props, from, inschars);
        self.zv = self.zv + inschars - nchars_del;
        self.refresh_marker_byte_positions();
        modiff_incr(&mut self.modiff, nchars_del + inschars);
        self.chars_modiff = self.modiff;
        self.log_edit(from, to, from + inschars, true);
        from
    }

    /// Insert a single character at point.
    pub fn insert_char(&mut self, c: char) -> usize {
        let mut buf = [0u8; 4];
        let s = c.encode_utf8(&mut buf);
        self.insert(s)
    }

    // ── Deletion ──

    /// Delete characters in the range [from, to) (1-based). Returns deleted text.
    pub fn delete_region(&mut self, from: usize, to: usize) -> Result<String, BufferError> {
        self.delete_region_1(from, to, true)
            .map(Option::unwrap_or_default)
    }

    /// `delete_region' for a caller that has no use for the deleted text,
    /// which is then read only for the undo list (del_range_1 without
    /// RET_STRING).
    pub fn delete_region_unreturned(&mut self, from: usize, to: usize) -> Result<(), BufferError> {
        self.delete_region_1(from, to, false).map(drop)
    }

    fn delete_region_1(
        &mut self,
        from: usize,
        to: usize,
        ret_string: bool,
    ) -> Result<Option<String>, BufferError> {
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from >= to {
            return Ok(ret_string.then(String::new));
        }

        let from0 = from - 1;
        let to0 = to - 1;

        // Grab text for undo before deleting, with the byte offsets of its
        // ends in the same walk.
        let (deleted, from_stored, to_stored) = if ret_string || !self.undo_recording_disabled() {
            let (deleted, from_stored, to_stored) = self.text_range_with_bytes(from0, to0);
            (Some(deleted), from_stored, to_stored)
        } else {
            (
                None,
                self.text.char_to_byte(from0),
                self.text.char_to_byte(to0),
            )
        };
        let nchars = to - from;

        if let Some(deleted) = &deleted
            && !self.undo_recording_disabled()
        {
            let string = self.deleted_text_value(deleted, from, to);
            self.record_delete(from, string, nchars);
        }

        let from_byte = self.byte_position_at_stored(from, from_stored);
        let to_byte = self.byte_position_at_stored(to, to_stored);
        match &deleted {
            Some(deleted) => self.text.remove_text(from0..to0, deleted),
            None => self.text.remove(from0..to0),
        }
        self.invalidate_char_cache();

        // Adjust point
        if self.pt > to {
            self.pt -= nchars;
        } else if self.pt > from {
            self.pt = from;
        }

        match &mut self.mark {
            BufferMark::Position(mark) => {
                if let Some(mark) = mark {
                    *mark = if *mark > to {
                        *mark - nchars
                    } else {
                        (*mark).min(from)
                    };
                }
            }
            BufferMark::Object { owner, .. } => {
                for marker in owner.markers() {
                    let position = marker.last_position();
                    if position > to {
                        marker.set_positions(
                            position - nchars,
                            marker.bytepos() - (to_byte - from_byte),
                        );
                    } else if position > from {
                        marker.set_positions(from, from_byte);
                    }
                }
            }
        }

        // Adjust overlays and evaporate empty ones
        self.overlays.delete_gap(from, to.saturating_sub(from));
        self.offset_intervals(from, -(nchars as isize), &PropertyVariables::NONE);
        self.adjust_extended_chars_for_delete(from, to);

        // Shrink accessible region
        self.zv -= nchars;

        modiff_incr(&mut self.modiff, nchars);
        self.chars_modiff = self.modiff;
        self.log_edit(from, to, from, true);
        Ok(deleted)
    }

    /// Delete n characters forward from point (like Emacs delete-char).
    pub fn delete_char(&mut self, n: isize) -> Result<String, BufferError> {
        if n >= 0 {
            let to = self.pt + n as usize;
            if to > self.zv {
                return Err(BufferError::EndOfBuffer);
            }
            self.delete_region(self.pt, to)
        } else {
            let count = (-n) as usize;
            if self.pt < self.begv + count {
                return Err(BufferError::BeginningOfBuffer);
            }
            let from = self.pt - count;
            self.delete_region(from, self.pt)
        }
    }

    // ── Narrowing ──

    /// Restrict the accessible portion of the buffer.
    pub fn narrow_to_region(&mut self, start: usize, end: usize) {
        let lower = start.min(end);
        let upper = start.max(end);
        let start = lower.max(1).min(self.text.len_chars() + 1);
        let end = upper.max(start).min(self.text.len_chars() + 1);
        self.begv = start;
        self.zv = end;
        // Clamp point into the new region
        self.pt = self.pt.clamp(self.begv, self.zv);
    }

    /// Remove narrowing.
    pub fn widen(&mut self) {
        self.begv = 1;
        self.zv = self.text.len_chars() + 1;
    }

    pub fn restore_restriction(&mut self, start: usize, end: usize) {
        let lower = start.min(end);
        let upper = start.max(end);
        self.begv = lower.max(1).min(self.text.len_chars() + 1);
        self.zv = upper.max(self.begv).min(self.text.len_chars() + 1);
        self.pt = self.pt.clamp(self.begv, self.zv);
        // GNU narrowing moves point, but does not move markers outside it.
    }

    // ── Modification state ──

    pub fn is_modified(&self) -> bool {
        self.save_modiff < self.modiff
    }

    pub fn set_unmodified(&mut self) {
        self.save_modiff = self.modiff;
    }

    pub fn visited_file_modtime(&self) -> Option<FileModTime> {
        self.visited_file_modtime
    }

    pub fn set_visited_file_modtime(&mut self, modtime: Option<FileModTime>) {
        self.visited_file_modtime = modtime;
    }

    /// Frestore_buffer_modified_p's non-nil FLAG: when SAVE_MODIFF >=
    /// MODIFF, SAVE_MODIFF = modiff_incr (&MODIFF, 1).
    pub fn set_modified(&mut self) {
        if self.save_modiff >= self.modiff {
            self.save_modiff = self.modiff;
            modiff_incr(&mut self.modiff, 1);
        }
    }

    /// Fbuffer_modified_p's `autosaved': modified, and auto-saved since.
    pub fn is_autosaved(&self) -> bool {
        self.is_modified() && self.autosave_modiff == self.modiff
    }

    /// Frecent_auto_save_p: auto-saved since last read or saved.
    pub fn recent_auto_save_p(&self) -> bool {
        self.save_modiff < self.autosave_modiff
    }

    /// BUF_AUTOSAVE_MODIFF = MODIFF.
    pub fn set_autosaved(&mut self) {
        self.autosave_modiff = self.modiff;
    }

    /// BUF_AUTOSAVE_MODIFF = COUNT, a count of the text this buffer
    /// shares as an indirect family's base.
    pub(crate) fn set_autosave_modiff(&mut self, count: ModCount) {
        self.autosave_modiff = count;
    }

    /// The counts of struct buffer_text: MODIFF, CHARS_MODIFF and
    /// SAVE_MODIFF, which an indirect buffer shares with its base.
    pub(crate) fn text_counters(&self) -> [ModCount; 3] {
        [self.modiff, self.chars_modiff, self.save_modiff]
    }

    pub(crate) fn set_text_counters(&mut self, [modiff, chars_modiff, save_modiff]: [ModCount; 3]) {
        self.modiff = modiff;
        self.chars_modiff = chars_modiff;
        self.save_modiff = save_modiff;
    }

    /// Fdo_auto_save's test: changed since the last real save and since
    /// the last auto-save.
    pub(crate) fn changed_since_saves(&self) -> bool {
        self.save_modiff < self.modiff && self.autosave_modiff < self.modiff
    }

    // ── Undo (undo.c) ──

    /// BVAR (b, undo_list).
    pub fn undo_list(&self) -> Value {
        self.undo_list
    }

    /// bset_undo_list: `buffer-undo-list' set, bound or restored.
    pub fn set_undo_list(&mut self, list: Value) {
        self.undo_list = list;
    }

    /// Whether `buffer-undo-list' is t, which each record_* function and
    /// insdel.c's run_undoable_change test first.
    pub fn undo_recording_disabled(&self) -> bool {
        matches!(self.undo_list.kind(), Kind::T)
    }

    /// buffer.c:Fbuffer_enable_undo: a list of t becomes nil.
    pub fn enable_undo(&mut self) {
        if self.undo_recording_disabled() {
            self.undo_list = Value::Nil;
        }
    }

    /// Fcons ENTRY onto the list, as each bset_undo_list of undo.c does.
    fn push_undo(&mut self, entry: Value) {
        self.undo_list = Value::cons(entry, self.undo_list);
    }

    /// keyboard.c's `point_before_last_command_or_undo' for this buffer:
    /// Some only while `buffer_before_last_command_or_undo' is this one.
    pub(crate) fn set_point_before_last_command_or_undo(&mut self, point: Option<usize>) {
        self.point_before_last_command_or_undo = point;
    }

    /// undo.c:record_point: a change after a boundary (or into an empty
    /// list) records where point stood before the last command or
    /// `undo-boundary', when that was in this buffer and is not BEG; the
    /// first change since the buffer was saved records its (t . TIME)
    /// first, the boundary test made before it.
    fn record_point(&mut self, beg: usize) {
        let at_boundary = self
            .undo_list
            .cons_values()
            .is_none_or(|(head, _)| head.is_nil());
        if !self.is_modified() {
            self.record_first_change();
        }
        if at_boundary
            && let Some(point) = self.point_before_last_command_or_undo
            && point != beg
        {
            self.push_undo(Value::Integer(point as i64));
        }
    }

    /// undo.c:record_insert: LENGTH characters inserted at BEG, merged
    /// into the newest record when that is an insertion ending at BEG
    /// (its END changed in place, as Lisp holding the list sees it).
    fn record_insert(&mut self, beg: usize, length: usize) {
        if self.undo_recording_disabled() {
            return;
        }
        self.record_point(beg);
        let list = self.undo_list;
        if let Some((head, _)) = list.cons_values()
            && let Some((start, end)) = head.cons_cells()
            && start.get().fixnum().is_some()
            && end.get().fixnum() == Some(beg as i64)
        {
            end.set(Value::Integer((beg + length) as i64));
            return;
        }
        self.push_undo(Value::cons(
            Value::Integer(beg as i64),
            Value::Integer((beg + length) as i64),
        ));
    }

    /// undo.c:record_delete: STRING, the CHARS characters about to be
    /// deleted at BEG with their properties (insdel.c's
    /// `make_buffer_string (from, to, true)'), at -BEG when point is at
    /// their end.
    fn record_delete(&mut self, beg: usize, string: Value, chars: usize) {
        if self.undo_recording_disabled() {
            return;
        }
        self.record_point(beg);
        let position = if self.pt == beg + chars {
            -(beg as i64)
        } else {
            beg as i64
        };
        self.push_undo(Value::cons(string, Value::Integer(position)));
    }

    /// The text of [FROM, TO) as record_delete records it: a string of
    /// this buffer's multibyteness with the text's properties.
    fn deleted_text_value(&self, text: &str, from: usize, to: usize) -> Value {
        Value::StringObject(crate::lisp::types::StringObjectRef::from_text_ref(
            text,
            crate::lisp::types::string_data::StringProperties::new(self.string_intervals(from, to)),
            self.multibyte,
            &self.substring_extended_chars(from, to),
        ))
    }

    /// undo.c:record_marker_adjustments' (MARKER . ADJUSTMENT) entries for
    /// the deletion just recorded, under it as record_delete leaves them:
    /// `primitive-undo' reads them right after the deletion.
    pub(crate) fn record_marker_adjustments(
        &mut self,
        adjustments: impl IntoIterator<Item = Value>,
    ) {
        if self.undo_recording_disabled() {
            return;
        }
        let list = self.undo_list;
        let Some((_, rest)) = list.cons_cells() else {
            return;
        };
        let spliced = adjustments
            .into_iter()
            .fold(rest.get(), |tail, adjustment| Value::cons(adjustment, tail));
        rest.set(spliced);
    }

    /// undo.c:record_first_change: (t . TIME), TIME the visited file's
    /// modtime, which `primitive-undo' compares with `visited-file-modtime'.
    fn record_first_change(&mut self) {
        if self.undo_recording_disabled() {
            return;
        }
        self.push_undo(undo_first_change_marker(self.visited_file_modtime));
    }

    /// undo.c:Fundo_boundary's change to the list, for a list that is not
    /// t: nil consed on unless its car is nil already.  Err with the list
    /// when it is not one, whose car Fcar refuses.
    pub fn undo_boundary(&mut self) -> Result<(), Value> {
        match self.undo_list.cons_values() {
            Some((head, _)) if !head.is_nil() => self.push_undo(Value::Nil),
            Some(_) => {}
            None if self.undo_list.is_nil() => {}
            None => return Err(self.undo_list),
        }
        Ok(())
    }

    /// Release editing storage when the buffer dies, even if Lisp still
    /// retains the buffer object.  GNU Fkill_buffer clears undo, intervals
    /// and overlays and frees text; file slots remain readable afterwards.
    pub(crate) fn release_killed_storage(&mut self) {
        self.killed = true;
        self.killed_size = self.size_total();
        self.killed_bounds = (self.point_min(), self.point_max());
        self.text = BufferText::default();
        *self.char_cache.get_mut() = RopeCharCache::default();
        self.edits = std::collections::VecDeque::new();
        self.undo_list = Value::Nil;
        self.point_before_last_command_or_undo = None;
        self.intervals = None;
        self.text_conversion_style = Value::Nil;
        self.text_conversion_style_is_local = false;
        self.extended_chars = Vec::new();
        self.overlays.clear();
        // Fkill_buffer detaches only markers whose buffer is dying. Its
        // mark can have been moved into another buffer and must survive there.
        self.mark_active = Value::Nil;
        self.mark = BufferMark::Position(None);
        self.pt = 1;
        self.begv = 1;
        self.zv = 1;
        self.last_name = Some(self.name.clone());
    }

    /// The fields `dump_buffer' writes, copied out of this buffer.
    pub(crate) fn image_parts(&self) -> BufferImage {
        BufferImage {
            name: self.name.clone(),
            text: self.text.whole_string(),
            pt: self.pt,
            mark: self.mark(),
            mark_active: self.mark_active,
            modiff: self.modiff,
            chars_modiff: self.chars_modiff,
            save_modiff: self.save_modiff,
            autosave_modiff: self.autosave_modiff,
            begv: self.begv,
            zv: self.zv,
            file: self.file.clone(),
            file_truename: self.file_truename.clone(),
            visited_file_modtime: self.visited_file_modtime,
            undo_list: self.undo_list,
            intervals: self.intervals.clone(),
            extended_chars: self.extended_chars.clone(),
            inhibit_hooks: self.inhibit_hooks,
            text_conversion_style: self.text_conversion_style,
            text_conversion_style_is_local: self.text_conversion_style_is_local,
            multibyte: self.multibyte,
        }
    }

    /// A buffer from the fields an image holds: `dump_buffer' cleared
    /// `last_name' before writing, the overlays were refused, and the
    /// character cache is rebuilt on demand.
    pub(crate) fn from_image_parts(parts: BufferImage) -> Self {
        Buffer {
            name: parts.name,
            last_name: None,
            text: BufferText::from_str(&parts.text),
            killed: false,
            killed_size: 0,
            killed_bounds: (1, 1),
            killed_locals: None,
            char_cache: RefCell::new(RopeCharCache::default()),
            pt: parts.pt,
            mark: BufferMark::Position(parts.mark),
            mark_active: parts.mark_active,
            modiff: parts.modiff,
            chars_modiff: parts.chars_modiff,
            edit_serial: 0,
            text_edit_serial: 0,
            edits: std::collections::VecDeque::new(),
            save_modiff: parts.save_modiff,
            autosave_modiff: parts.autosave_modiff,
            auto_save_failure_time: 0,
            begv: parts.begv,
            zv: parts.zv,
            file: parts.file,
            file_truename: parts.file_truename,
            visited_file_modtime: parts.visited_file_modtime,
            undo_list: parts.undo_list,
            point_before_last_command_or_undo: None,
            overlays: crate::overlay::OverlayTree::default(),
            intervals: parts.intervals,
            extended_chars: parts.extended_chars,
            inhibit_hooks: parts.inhibit_hooks,
            text_conversion_style: parts.text_conversion_style,
            text_conversion_style_is_local: parts.text_conversion_style_is_local,
            multibyte: parts.multibyte,
        }
    }

    pub fn modified_tick(&self) -> ModCount {
        self.modiff
    }

    pub fn chars_modified_tick(&self) -> ModCount {
        self.chars_modiff
    }

    pub fn set_modified_tick(&mut self, tick: ModCount) {
        self.modiff = tick;
    }

    pub(crate) fn set_chars_modified_tick(&mut self, tick: ModCount) {
        self.chars_modiff = tick;
    }

    pub fn swap_text_state(&mut self, other: &mut Buffer) {
        std::mem::swap(&mut self.text, &mut other.text);
        std::mem::swap(&mut self.char_cache, &mut other.char_cache);
        std::mem::swap(&mut self.pt, &mut other.pt);
        std::mem::swap(&mut self.mark_active, &mut other.mark_active);
        std::mem::swap(&mut self.modiff, &mut other.modiff);
        std::mem::swap(&mut self.chars_modiff, &mut other.chars_modiff);
        std::mem::swap(&mut self.save_modiff, &mut other.save_modiff);
        std::mem::swap(&mut self.begv, &mut other.begv);
        std::mem::swap(&mut self.zv, &mut other.zv);
        std::mem::swap(&mut self.undo_list, &mut other.undo_list);
        std::mem::swap(&mut self.overlays, &mut other.overlays);
        std::mem::swap(&mut self.intervals, &mut other.intervals);
        std::mem::swap(&mut self.extended_chars, &mut other.extended_chars);
        std::mem::swap(&mut self.multibyte, &mut other.multibyte);
        match (&mut self.mark, &mut other.mark) {
            (
                BufferMark::Object { marker, owner },
                BufferMark::Object {
                    marker: other_marker,
                    owner: other_owner,
                },
            ) => {
                std::mem::swap(marker, other_marker);
                owner.swap_marker_chains(*other_owner);
                self.overlays.set_owner(*owner);
                other.overlays.set_owner(*other_owner);
            }
            (BufferMark::Position(mark), BufferMark::Position(other_mark)) => {
                std::mem::swap(mark, other_mark)
            }
            _ => {
                // A standalone Rust snapshot cannot own Lisp markers. Detach
                // the installed buffer's chain and keep each side's owner.
                let mark = self.mark();
                let other_mark = other.mark();
                if let BufferMark::Object { owner, .. } = self.mark {
                    owner.detach_markers();
                }
                if let BufferMark::Object { owner, .. } = other.mark {
                    owner.detach_markers();
                }
                if let Some(pos) = other_mark {
                    self.set_mark_position(pos);
                } else {
                    self.clear_mark();
                }
                if let Some(pos) = mark {
                    other.set_mark_position(pos);
                } else {
                    other.clear_mark();
                }
            }
        }

        // Each buffer's serials stay its own and count on: the text behind
        // them changed without an edit, so every view keyed on either
        // buffer's serial is stale.
        self.text_replaced_without_record();
        other.text_replaced_without_record();
    }

    // ── Line/column helpers ──

    /// Return the line number (1-based) at the given position (1-based).
    pub fn line_number_at_pos(&self, pos: usize) -> usize {
        let pos0 = (pos - 1).min(self.text.len_chars());
        self.text.char_to_line(pos0) + 1
    }

    /// Start position (1-based) of the 1-based LINE, clamped to the text.
    pub fn line_start_of(&self, line: usize) -> usize {
        let total = self.text.len_lines();
        let index = line.saturating_sub(1).min(total.saturating_sub(1));
        self.text.line_to_char(index) + 1
    }

    /// Where display of the 1-based LINE begins: its start, or BEGV when
    /// the accessible region begins inside it (the display iterator never
    /// looks before BEGV).
    pub fn display_line_start_of(&self, line: usize) -> usize {
        self.line_start_of(line).max(self.begv)
    }

    /// search.c's find_newline backward from Z over the whole text: the
    /// position just after the COUNTth newline before the end, or None when
    /// the text has fewer. Only newlines count as line breaks.
    pub fn after_newlines_from_end(&self, count: usize) -> Option<usize> {
        let newlines = self.text.len_lines() - 1;
        (count >= 1 && count <= newlines).then(|| self.text.line_to_char(newlines - count + 1) + 1)
    }

    /// Current column (0-based) of point.
    pub fn current_column(&self) -> usize {
        let idx0 = self.pt - 1;
        let line_start = self.text.line_to_char(self.text.char_to_line(idx0));
        idx0 - line_start
    }

    /// Number of lines in the accessible region.
    pub fn count_lines(&self) -> usize {
        let start = self.begv - 1;
        let end = (self.zv - 1).min(self.text.len_chars());
        if start >= end {
            return 0;
        }
        let (start_byte, end_byte) = (self.text.char_to_byte(start), self.text.char_to_byte(end));
        // Count newlines + 1 (last line might not end with newline)
        let newlines = self.text.newlines_in(start_byte..end_byte);
        if end > start { newlines + 1 } else { 0 }
    }

    /// intervals.c:offset_intervals: LENGTH characters inserted (positive)
    /// at START, or -LENGTH deleted from it; a deletion of the whole text
    /// leaves no intervals.
    fn offset_intervals(&mut self, start: usize, length: isize, vars: &PropertyVariables) {
        let Some(intervals) = &mut self.intervals else {
            return;
        };
        if length > 0 {
            let mut host = *vars;
            let _ = intervals.adjust_for_insertion(&mut host, start, length as usize);
        } else if length < 0 && !intervals.adjust_for_deletion(start, length.unsigned_abs()) {
            self.intervals = None;
        }
    }

    fn adjust_extended_chars_for_insert(&mut self, pos: usize, nchars: usize) {
        for (position, _) in &mut self.extended_chars {
            if *position >= pos {
                *position += nchars;
            }
        }
    }

    fn adjust_extended_chars_for_delete(&mut self, from: usize, to: usize) {
        let removed = to.saturating_sub(from);
        self.extended_chars.retain_mut(|(position, _)| {
            if *position < from {
                true
            } else if *position >= to {
                *position -= removed;
                true
            } else {
                false
            }
        });
    }
}

/// lisp.h modiff_incr: a change of LEN characters counts one more than
/// LEN's binary logarithm, so a large change counts more than a small one.
fn modiff_incr(count: &mut ModCount, len: usize) {
    *count += if len == 0 {
        1
    } else {
        i64::from(len.ilog2()) + 1
    };
}

/// Whether PLIST has a property named NAME (by EQ).
fn plist_has_name(plist: Value, name: Value) -> bool {
    let mut tail = plist;
    while let Some(cell) = tail.cons_ref() {
        if cell.car.get().word() == name.word() {
            return true;
        }
        let Some(value) = cell.cdr.get().cons_ref() else {
            return false;
        };
        tail = value.cdr.get();
    }
    false
}

fn position_to_byte_in_text(text: &str, pos: usize) -> Option<usize> {
    let char_len = text.chars().count();
    if pos == 0 || pos > char_len + 1 {
        return None;
    }
    Some(
        1 + text
            .chars()
            .take(pos - 1)
            .map(char::len_utf8)
            .sum::<usize>(),
    )
}

fn byte_to_position_in_text(text: &str, byte: usize) -> Option<usize> {
    if byte == 0 {
        return None;
    }
    let total_bytes = text.len();
    if byte > total_bytes + 1 {
        return None;
    }
    if byte == total_bytes + 1 {
        return Some(text.chars().count() + 1);
    }
    let mut current_byte = 1usize;
    for (index, ch) in text.chars().enumerate() {
        let next = current_byte + ch.len_utf8();
        if byte < next {
            return Some(index + 1);
        }
        current_byte = next;
    }
    Some(text.chars().count() + 1)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn raw_markers_are_counted_across_block_edges() {
        // Every length and offset around the 32-pair blocks, against the
        // markers counted one place at a time; a lone EE, and one before a
        // byte that leads no raw byte, count nothing.
        let unit = "a\u{E080}\u{E0FF}é\u{E07F}\u{E100}b\u{E0C3}";
        let text = unit.repeat(9);
        let bytes = text.as_bytes();
        for start in 0..40 {
            for end in start..bytes.len().min(start + 120) {
                let run = &bytes[start..end];
                let expected = (0..run.len().saturating_sub(1))
                    .filter(|&k| run[k] == 0xEE && matches!(run[k + 1], 0x82 | 0x83))
                    .count();
                assert_eq!(count_raw_markers(run), expected, "{start}..{end}");
            }
        }
        assert_eq!(count_raw_markers(&[0xEE]), 0);
        assert_eq!(count_raw_markers(&[0xEE, 0x81, 0x80]), 0);
    }

    #[test]
    fn byte_conversions_remember_only_character_starts() {
        // A text with its gap moved about converted byte by byte, in and out of order
        // and in the middle of characters, after edits: the conversions a
        // character's first byte records must not answer for another byte.
        let mut buf = Buffer::from_text("test", &"aé€𝄞\n".repeat(600));
        for round in 0..2 {
            let text = buf.buffer_string();
            let len = text.len();
            let order: Vec<usize> = (0..len)
                .step_by(7)
                .chain((0..len).rev().step_by(13))
                .chain([0, len, len / 2, len / 2 + 1, len / 2 + 2, len / 2 + 1])
                .collect();
            for byte in order {
                // The character a byte lies in: a middle byte answers the
                // character it is part of.
                let start = (0..=byte)
                    .rev()
                    .find(|&at| text.is_char_boundary(at))
                    .unwrap();
                assert_eq!(
                    buf.storage_byte_to_position(byte + 1),
                    Some(text[..start].chars().count() + 1),
                    "round {round} byte {byte}"
                );
            }
            buf.goto_char(700);
            buf.insert("λ𝄞");
            buf.delete_region(50, 53).unwrap();
        }
    }

    // -- Tests derived from Emacs test/src/editfns-tests.el --

    #[test]
    fn point_and_goto_char() {
        // editfns-tests--point-and-goto-char
        let mut buf = Buffer::from_text("test", "abc");
        assert_eq!(buf.point(), 1);
        assert_eq!(buf.goto_char(2), 2);
        assert_eq!(buf.point(), 2);
        assert_eq!(buf.goto_char(4), 4); // one past last char
        assert_eq!(buf.point(), 4);
    }

    #[test]
    fn point_min_max_buffer_size() {
        // editfns-tests--point-min-max-and-buffer-size
        let buf = Buffer::from_text("test", "abc");
        assert_eq!(buf.point_min(), 1);
        assert_eq!(buf.point_max(), 4); // len + 1
        assert_eq!(buf.buffer_size(), 3);
    }

    #[test]
    fn region_beginning_end() {
        // editfns-tests--region-beginning-end
        let mut buf = Buffer::from_text("test", "abcd");
        buf.goto_char(3);
        buf.set_mark(1);
        let (beg, end) = buf.region().unwrap();
        assert_eq!(beg, 1);
        assert_eq!(end, 3);
    }

    #[test]
    fn buffer_string_and_substring() {
        // editfns-tests--buffer-string-compare-substrings
        let mut buf = Buffer::from_text("test", "abc");
        assert_eq!(buf.buffer_string(), "abc");
        assert_eq!(buf.buffer_substring(1, 3).unwrap(), "ab");
        assert_eq!(buf.buffer_substring(2, 4).unwrap(), "bc");
        assert_eq!(buf.buffer_substring(4, 2).unwrap(), "bc");

        buf.add_text_properties(2, 3, &[("sample".into(), Value::T)]);
        let copied = buf.copy_intervals(2, 4).expect("intervals of [2, 4)");
        let spans: Vec<_> = copied.iter().map(|(start, end, _)| (start, end)).collect();
        assert_eq!(spans, [(0, 1), (1, 2)]);
        assert!(copied.plist(1).is_nil());
    }

    #[test]
    fn sparse_text_property_lookup_respects_span_boundaries() {
        let mut buf = Buffer::from_text("properties", &"x".repeat(400));
        for index in 0..64 {
            let start = 2 + index * 5;
            buf.set_text_properties(
                start,
                start + 2,
                &[("slot".into(), Value::Integer(index as i64))],
            );
        }

        assert!(buf.text_plist_at(1).is_nil());
        assert!(buf.text_plist_at(buf.point_max()).is_nil());
        for index in 0..64 {
            let start = 2 + index * 5;
            for pos in [start, start + 1] {
                assert_eq!(
                    buf.text_property_at(pos, "slot"),
                    Some(Value::Integer(index as i64))
                );
            }
            assert!(buf.text_plist_at(start + 2).is_nil());
        }

        // Fill the first gap with the preceding span's properties.  As in
        // set_text_properties_1, the changed intervals merge only with each
        // other, so the equal interval before them stays separate.
        buf.set_text_properties(4, 7, &[("slot".into(), Value::Integer(0))]);
        assert_eq!(buf.text_property_at(6, "slot"), Some(Value::Integer(0)));
        assert_eq!(buf.text_property_at(7, "slot"), Some(Value::Integer(1)));
        let spans: Vec<_> = buf
            .intervals()
            .expect("intervals")
            .iter()
            .map(|(start, end, _)| (start, end))
            .take(4)
            .collect();
        assert_eq!(spans, [(1, 2), (2, 4), (4, 7), (7, 9)]);
    }

    #[test]
    fn line_boundaries() {
        // editfns-tests--line-boundaries
        let mut buf = Buffer::from_text("test", "ab\ncd\n");
        assert!(buf.bobp());
        assert!(buf.bolp());
        assert!(!buf.eobp());
        assert!(!buf.eolp());

        buf.goto_char(3); // the newline
        assert!(buf.eolp());

        buf.goto_char(4); // 'c'
        assert!(buf.bolp());

        buf.goto_char(7); // past the last newline
        assert!(buf.eobp());
    }

    #[test]
    fn insert_and_point_advance() {
        let mut buf = Buffer::new("test");
        assert_eq!(buf.point(), 1);
        buf.insert("hello");
        assert_eq!(buf.point(), 6);
        assert_eq!(buf.buffer_string(), "hello");
        assert_eq!(buf.buffer_size(), 5);
    }

    #[test]
    fn insert_in_middle() {
        let mut buf = Buffer::from_text("test", "ac");
        buf.goto_char(2); // between 'a' and 'c'
        buf.insert("b");
        assert_eq!(buf.buffer_string(), "abc");
        assert_eq!(buf.point(), 3); // after 'b'
    }

    #[test]
    fn character_cache_tracks_text_mutation_and_swap() {
        let text = format!("{}λ{}", "a".repeat(1500), "b".repeat(1500));
        let mut buf = Buffer::from_text("cached", &text);
        // A read alone decodes its character where it stands; the next
        // one beside it scans, filling the window.
        assert_eq!(buf.char_at(1500), Some('a'));
        assert!(!buf.char_cache.borrow().valid);
        assert_eq!(buf.char_at(1501), Some('λ'));
        assert!(buf.char_cache.borrow().valid);
        assert_eq!(buf.char_at(3001), Some('b'));
        assert_eq!(buf.char_cache.borrow().start, 1500 - CHAR_CACHE_BEHIND);

        buf.goto_char(1);
        buf.insert("z");
        assert!(!buf.char_cache.borrow().valid);
        assert_eq!(buf.char_at(1502), Some('λ'));

        buf.replace_region_in_place(1502, 1503, "Ω", &[], false);
        assert!(!buf.char_cache.borrow().valid);
        assert_eq!(buf.char_at(1502), Some('Ω'));

        assert_eq!(buf.delete_region(1, 2), Ok("z".into()));
        assert_eq!(buf.char_at(1501), Some('Ω'));

        let mut other = Buffer::from_text("other", "q");
        assert_eq!(other.char_at(1), Some('q'));
        buf.swap_text_state(&mut other);
        assert_eq!(buf.char_at(1), Some('q'));
        assert_eq!(other.char_at(1501), Some('Ω'));
    }

    #[test]
    fn position_cache_follows_edits_before_and_over_it() {
        // Conversions recorded since an edit stay right across the edits
        // after them: shifted by an edit before, dropped by one over them;
        // and each counts right across the gap the edits leave.
        let mut model: Vec<char> = "aé€bc\n".repeat(400).chars().collect();
        let mut text = BufferText::from_str(&model.iter().collect::<String>());
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = |bound: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % bound.max(1) as u64) as usize
        };
        let byte_of = |model: &[char], index: usize| -> usize {
            model[..index].iter().map(|ch| ch.len_utf8()).sum()
        };
        for round in 0..400 {
            for _ in 0..3 {
                let index = next(model.len() + 1);
                assert_eq!(text.char_to_byte(index), byte_of(&model, index));
                let byte = byte_of(&model, index);
                assert_eq!(text.byte_to_char(byte), index);
            }
            let at = next(model.len());
            if round % 2 == 0 {
                let inserted = ["x", "λ", "€é", "\n"][next(4)];
                text.insert(at, inserted);
                for (offset, ch) in inserted.chars().enumerate() {
                    model.insert(at + offset, ch);
                }
            } else {
                let end = (at + 1 + next(3)).min(model.len());
                text.remove(at..end);
                model.drain(at..end);
            }
            assert_eq!(text.len_chars(), model.len());
        }
    }

    #[test]
    fn a_text_answers_through_insertions_and_deletions_around_its_gap() {
        // Each step answers as the text it holds, multibyte characters and
        // lines included, wherever the last edit left the gap; a scan's
        // copy taken before an edit keeps the text it was taken of.
        fn check(buf: &Buffer, expected: &str) {
            assert_eq!(buf.full_buffer_string(), expected);
            assert_eq!(buf.size_total(), expected.chars().count());
            let scan = buf.scan_text();
            let (before, after) = scan.halves();
            assert_eq!(format!("{before}{after}"), expected);
            assert_eq!(scan.chars(), expected.chars().count());
            let lines: Vec<&str> = expected.split('\n').collect();
            assert_eq!(buf.text.len_lines(), lines.len());
            assert_eq!(buf.lines_from(2, 1), vec![lines[1].to_string()]);
            let second_line = expected
                .find('\n')
                .map_or(0, |at| expected[..=at].chars().count());
            assert_eq!(buf.text.line_to_char(1), second_line);
            assert_eq!(buf.text.char_to_line(second_line), 1);
            for (index, ch) in expected.chars().enumerate().step_by(97) {
                assert_eq!(buf.char_at(index + 1), Some(ch));
                let byte = expected.char_indices().nth(index).expect("char").0;
                assert_eq!(buf.position_bytes(index + 1), Some(byte + 1));
            }
            let end = buf.size_total() + 1;
            assert_eq!(
                buf.buffer_substring(1, end).as_deref(),
                Ok(expected),
                "substring of the whole text"
            );
        }
        let mut buf = Buffer::from_text("small", "λx\nab\n");
        check(&buf, "λx\nab\n");

        let line = "abcλ\u{1F600}def\n";
        let mut expected = String::from("λx\nab\n");
        let first = buf.scan_text();
        while expected.len() <= 4096 {
            buf.goto_char(3);
            buf.insert(line);
            expected.insert_str(expected.char_indices().nth(2).expect("char").0, line);
            check(&buf, &expected);
        }
        let (before, after) = first.halves();
        assert_eq!(format!("{before}{after}"), "λx\nab\n");

        while expected.len() > 1024 {
            let from = 3;
            let to = from + line.chars().count();
            assert_eq!(buf.delete_region(from, to).as_deref(), Ok(line));
            let start = expected.char_indices().nth(from - 1).expect("char").0;
            expected.replace_range(start..start + line.len(), "");
            check(&buf, &expected);
        }
    }

    #[test]
    fn inherited_text_properties_obey_directional_stickiness() {
        let mut prompt = Buffer::from_text("prompt", "p");
        prompt.set_text_properties(
            1,
            2,
            &[
                ("field".into(), Value::Symbol("prompt".into())),
                ("read-only".into(), Value::T),
                (
                    "front-sticky".into(),
                    Value::list([
                        Value::Symbol("field".into()),
                        Value::Symbol("read-only".into()),
                    ]),
                ),
                (
                    "rear-nonsticky".into(),
                    Value::list([
                        Value::Symbol("field".into()),
                        Value::Symbol("read-only".into()),
                    ]),
                ),
            ],
        );
        prompt.goto_char(2);
        prompt.insert_with(
            "x",
            InsertedProperties::None { inherit: true },
            false,
            &PropertyVariables::NONE,
        );
        assert_eq!(prompt.text_property_at(2, "field"), None);
        assert_eq!(prompt.text_property_at(2, "read-only"), None);

        fn adjacent_buffer() -> Buffer {
            let mut buffer = Buffer::from_text("adjacent", "ab");
            buffer.set_text_properties(
                1,
                2,
                &[
                    ("from-rear".into(), Value::Integer(1)),
                    (
                        "rear-nonsticky".into(),
                        Value::list([Value::Symbol("face".into())]),
                    ),
                ],
            );
            buffer.set_text_properties(
                2,
                3,
                &[
                    ("from-front".into(), Value::Integer(2)),
                    (
                        "front-sticky".into(),
                        Value::list([Value::Symbol("from-front".into())]),
                    ),
                ],
            );
            buffer
        }

        let mut adjacent = adjacent_buffer();
        adjacent.goto_char(2);
        adjacent.insert_with(
            "x",
            InsertedProperties::None { inherit: true },
            false,
            &PropertyVariables::NONE,
        );
        assert_eq!(
            adjacent.text_property_at(2, "from-rear"),
            Some(Value::Integer(1))
        );
        assert_eq!(
            adjacent.text_property_at(2, "from-front"),
            Some(Value::Integer(2))
        );

        let vars = PropertyVariables {
            default_nonsticky: Value::list([Value::cons(
                Value::Symbol("from-rear".into()),
                Value::T,
            )]),
            ..PropertyVariables::NONE
        };
        let mut nonsticky = adjacent_buffer();
        nonsticky.goto_char(2);
        nonsticky.insert_with(
            "x",
            InsertedProperties::None { inherit: true },
            false,
            &vars,
        );
        assert_eq!(nonsticky.text_property_at(2, "from-rear"), None);
        assert_eq!(
            nonsticky.text_property_at(2, "from-front"),
            Some(Value::Integer(2))
        );
    }

    #[test]
    fn delete_region_basic() {
        let mut buf = Buffer::from_text("test", "abcde");
        let deleted = buf.delete_region(2, 4).unwrap();
        assert_eq!(deleted, "bc");
        assert_eq!(buf.buffer_string(), "ade");
    }

    #[test]
    fn delete_char_forward() {
        let mut buf = Buffer::from_text("test", "abc");
        buf.goto_char(2);
        buf.delete_char(1).unwrap();
        assert_eq!(buf.buffer_string(), "ac");
        assert_eq!(buf.point(), 2);
    }

    #[test]
    fn delete_char_backward() {
        let mut buf = Buffer::from_text("test", "abc");
        buf.goto_char(3); // after 'b'
        buf.delete_char(-1).unwrap();
        assert_eq!(buf.buffer_string(), "ac");
        assert_eq!(buf.point(), 2);
    }

    #[test]
    fn forward_char_errors_at_boundary() {
        let mut buf = Buffer::from_text("test", "ab");
        buf.goto_char(3); // end of buffer
        assert_eq!(buf.forward_char(1), Err(BufferError::EndOfBuffer));

        buf.goto_char(1);
        assert_eq!(buf.forward_char(-1), Err(BufferError::BeginningOfBuffer));
    }

    #[test]
    fn forward_line_basic() {
        // forward-line returns 0 on success, shortage on failure
        let mut buf = Buffer::from_text("test", "aa\nbb\ncc");
        buf.goto_char(1);
        assert_eq!(buf.forward_line(1), 0);
        assert_eq!(buf.point(), 4); // start of "bb"

        assert_eq!(buf.forward_line(1), 0);
        assert_eq!(buf.point(), 7); // start of "cc"

        // The unterminated final line can be crossed successfully to ZV.
        assert_eq!(buf.forward_line(1), 0);
        assert_eq!(buf.point(), buf.point_max());

        // Only another step from ZV has a shortage.
        assert_eq!(buf.forward_line(1), 1);
    }

    #[test]
    fn narrowing() {
        let mut buf = Buffer::from_text("test", "abcdef");
        buf.narrow_to_region(2, 5); // accessible: "bcd"
        assert_eq!(buf.point_min(), 2);
        assert_eq!(buf.point_max(), 5);
        assert_eq!(buf.buffer_size(), 3);
        assert_eq!(buf.buffer_string(), "bcd");

        buf.widen();
        assert_eq!(buf.point_min(), 1);
        assert_eq!(buf.point_max(), 7);
        assert_eq!(buf.buffer_string(), "abcdef");
    }

    #[test]
    fn byte_positions_use_absolute_buffer_positions_when_narrowed() {
        let mut buf = Buffer::from_text("test", "abcdef");
        buf.narrow_to_region(3, 6); // accessible: "cde"

        assert_eq!(buf.position_bytes(3), Some(3));
        assert_eq!(buf.position_bytes(6), Some(6));
        assert_eq!(buf.byte_to_position(3), Some(3));
        assert_eq!(buf.byte_to_position(6), Some(6));
    }

    /// The list's entries, newest first.
    fn undo_entries(buf: &Buffer) -> Vec<Value> {
        buf.undo_list().to_vec().expect("undo list")
    }

    #[test]
    fn insertion_records_gnu_undo_entries() {
        let mut buf = Buffer::new("test");
        buf.insert("hello");
        assert_eq!(buf.buffer_string(), "hello");
        // undo.c: (BEG . END) over the first change's (t . 0).
        assert_eq!(
            undo_entries(&buf),
            vec![
                Value::cons(Value::Integer(1), Value::Integer(6)),
                Value::cons(Value::T, Value::Integer(0)),
            ]
        );
    }

    #[test]
    fn adjacent_inserts_share_one_gnu_undo_record_and_public_pair() {
        let mut buf = Buffer::new("test");
        buf.insert("a");
        let first_insert = buf.undo_list().car().expect("first undo record");
        buf.insert("bc");

        assert_eq!(undo_entries(&buf).len(), 2, "save marker plus insertion");
        let second_insert = buf.undo_list().car().expect("coalesced undo record");
        assert!(matches!(
            (first_insert.kind(), second_insert.kind()),
            (Kind::Cons(left), Kind::Cons(right)) if crate::lisp::types::SharedCons::ptr_eq(&left, &right)
        ));
        assert_eq!(
            second_insert,
            Value::cons(Value::Integer(1), Value::Integer(4))
        );
    }

    #[test]
    fn deletion_records_its_text_and_position() {
        let mut buf = Buffer::from_text("test", "abc");
        buf.goto_char(2);
        buf.delete_char(1).unwrap();
        assert_eq!(buf.buffer_string(), "ac");
        let entries = undo_entries(&buf);
        let (text, position) = entries[0].cons_values().expect("deletion record");
        assert_eq!(text, Value::String("b".into()));
        assert_eq!(position, Value::Integer(2));
        assert_eq!(entries[1], Value::cons(Value::T, Value::Integer(0)));
        buf.goto_char(2);
        buf.delete_char(-1).unwrap();
        // Point at the end of the deleted text: the position is negative.
        let (text, position) = buf.undo_list().car().unwrap().cons_values().unwrap();
        assert_eq!(text, Value::String("a".into()));
        assert_eq!(position, Value::Integer(-1));
    }

    #[test]
    fn deletion_record_keeps_text_properties() {
        let mut buf = Buffer::from_text("test", "abc");
        buf.add_text_properties(2, 3, &[("markup".into(), Value::String("x".into()))]);
        buf.undo_boundary().unwrap();
        buf.goto_char(2);
        buf.delete_char(1).unwrap();
        assert_eq!(buf.buffer_string(), "ac");
        let (text, _) = buf.undo_list().car().unwrap().cons_values().unwrap();
        let string = crate::lisp::primitives::string_like(&text).expect("deleted text");
        assert_eq!(string.text, "b");
        let intervals = string.props.as_ref().expect("deleted text's intervals");
        let spans: Vec<_> = intervals
            .iter()
            .map(|(start, end, _)| (start, end))
            .collect();
        assert_eq!(spans, [(0, 1)]);
        assert_eq!(
            intervals::plist_get(intervals.plist(0), Value::Symbol("markup".into())),
            Value::String("x".into())
        );
    }

    #[test]
    fn extended_character_metadata_tracks_edits_and_undo() {
        let mut buf = Buffer::new("test");
        buf.insert("\u{e000}");
        buf.set_inserted_extended_chars(1, &[(0, 0x3f_ffff)]);

        buf.goto_char(1);
        buf.insert("x");
        assert_eq!(buf.extended_char_at(2), Some(0x3f_ffff));
        buf.delete_region(1, 2).unwrap();
        assert_eq!(buf.extended_char_at(1), Some(0x3f_ffff));

        buf.set_undo_list(Value::Nil);
        buf.delete_region(1, 2).unwrap();
        assert_eq!(buf.extended_char_at(1), None);
        let (text, _) = buf.undo_list().car().unwrap().cons_values().unwrap();
        let Kind::StringObject(state) = text.kind() else {
            panic!("deleted text is a string");
        };
        assert_eq!(state.borrow().character_at(0), Some(0x3f_ffff));
    }

    #[test]
    fn modification_tracking() {
        let mut buf = Buffer::new("test");
        assert!(!buf.is_modified());
        buf.insert("x");
        assert!(buf.is_modified());
        buf.set_unmodified();
        assert!(!buf.is_modified());
    }

    #[test]
    fn inverse_edits_remain_modified_until_explicitly_marked_clean() {
        let mut buf = Buffer::from_text("test", "ab");
        buf.set_unmodified();
        buf.goto_char(1);
        let deleted = buf.delete_char(1).unwrap();
        buf.insert(&deleted);

        assert_eq!(buf.full_buffer_string(), "ab");
        assert!(buf.is_modified());
        buf.set_unmodified();
        assert!(!buf.is_modified());
    }

    #[test]
    fn modification_tracking_ignores_narrowing() {
        let mut buf = Buffer::from_text("test", "abcdef");
        buf.set_unmodified();
        buf.narrow_to_region(2, 5);

        assert!(!buf.is_modified());
        assert_eq!(buf.buffer_string(), "bcd");
        assert_eq!(buf.full_buffer_string(), "abcdef");
    }

    #[test]
    fn char_access() {
        let buf = Buffer::from_text("test", "abc");
        assert_eq!(buf.char_at(1), Some('a'));
        assert_eq!(buf.char_at(2), Some('b'));
        assert_eq!(buf.char_at(3), Some('c'));
        assert_eq!(buf.char_at(4), None); // past end
    }

    #[test]
    fn skip_while_walks_long_rope_slices_in_both_directions() {
        let mut buf = Buffer::from_text(
            "test",
            &format!("{}x{}", " ".repeat(10_000), "\t".repeat(10_000)),
        );
        assert_eq!(
            buf.skip_forward_while(buf.point_max(), |ch| ch == ' '),
            10_000
        );
        assert_eq!(buf.char_after(), Some('x'));

        buf.goto_char(buf.point_max());
        assert_eq!(
            buf.skip_backward_while(buf.point_min(), |ch| ch == '\t'),
            10_000
        );
        assert_eq!(buf.char_before(), Some('x'));
    }

    #[test]
    fn current_column_tracking() {
        // editfns-tests--current-column-move-to-column
        let mut buf = Buffer::from_text("test", "abcd\nefgh");
        assert_eq!(buf.current_column(), 0);
        buf.goto_char(3);
        assert_eq!(buf.current_column(), 2);
        buf.goto_char(6); // 'e'
        assert_eq!(buf.current_column(), 0);
        buf.goto_char(8); // 'g'
        assert_eq!(buf.current_column(), 2);
    }

    #[test]
    fn beginning_and_end_of_line() {
        let mut buf = Buffer::from_text("test", "abc\ndef\nghi");
        buf.goto_char(5); // 'd'
        buf.beginning_of_line();
        assert_eq!(buf.point(), 5);
        buf.end_of_line();
        assert_eq!(buf.point(), 8); // position of '\n' after "def"

        buf.goto_char(2); // 'b'
        buf.beginning_of_line();
        assert_eq!(buf.point(), 1);

        buf.narrow_to_region(6, 12); // Start in the middle of "def".
        buf.goto_char(7);
        assert_eq!(buf.beginning_of_line(), 6);

        let mut trailing_newline = Buffer::from_text("test", "abc\n");
        trailing_newline.goto_char(trailing_newline.point_max());
        assert_eq!(trailing_newline.beginning_of_line(), 5);

        // GNU buffer motion recognizes LF as the line separator.  A lone CR
        // is ordinary buffer text (terminal filters may interpret it later).
        let mut carriage_return = Buffer::from_text("test", "hello\rgoodbye\n");
        carriage_return.goto_char(7);
        assert_eq!(carriage_return.beginning_of_line(), 1);
        assert_eq!(carriage_return.end_of_line(), 14);
    }

    #[test]
    fn self_insert_negative_arg() {
        // cmds-tests: self-insert-command-with-negative-argument
        // In our case, insert_char doesn't take a count, but delete_char(-n)
        // with n > point should error
        let mut buf = Buffer::from_text("test", "x");
        buf.goto_char(1);
        assert!(buf.delete_char(-1).is_err());
    }

    #[test]
    fn multibyte_basic() {
        let mut buf = Buffer::from_text("test", "héllo");
        assert_eq!(buf.buffer_size(), 5); // 5 chars, not 6 bytes
        assert_eq!(buf.char_at(2), Some('é'));
        buf.goto_char(3);
        buf.insert("X");
        assert_eq!(buf.buffer_string(), "héXllo");
    }

    #[test]
    fn empty_buffer_predicates() {
        let buf = Buffer::new("empty");
        assert!(buf.bobp());
        assert!(buf.eobp());
        assert!(buf.bolp());
        assert!(buf.eolp());
        assert_eq!(buf.buffer_size(), 0);
        assert_eq!(buf.point(), 1);
        assert_eq!(buf.point_min(), 1);
        assert_eq!(buf.point_max(), 1);
    }
}
