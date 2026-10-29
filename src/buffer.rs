#![allow(dead_code)]

use crate::lisp::types::{BufferRef, Kind, MarkerRef};
use crate::lisp::types::{PropertyKey, PropertyName, Value};
use ropey::Rope;
use std::cell::{Cell, RefCell};
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
/// Internally we convert to 0-based char indices into the rope.
pub struct Buffer {
    /// Human-visible name (e.g. "*scratch*" or "main.rs").
    pub name: String,

    /// Previous name, set by rename-buffer.
    pub last_name: Option<String>,

    /// The text.
    text: BufferText,

    /// Derived character view of the most recently read Rope chunk. Syntax
    /// and motion code commonly scan adjacent characters through `char_at`;
    /// retaining one decoded chunk avoids a tree walk per character while
    /// keeping Rope as the sole text authority.
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

    /// Explicit modified flag used by `set-buffer-modified-p'.
    forced_modified: bool,

    /// True when the buffer is modified but its contents were auto-saved.
    autosaved: bool,

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

    /// Sparse text property spans over [start, end) buffer positions.
    text_properties: Vec<TextPropertySpan>,

    /// Whether GNU's buffer_intervals would be set: true once any text
    /// has had a property, kept while every interval's plist is empty,
    /// and false again when a deletion takes the whole text
    /// (adjust_intervals_for_deletion).  `set-text-properties' modifies,
    /// so signals and counts, such a buffer even when no plist changes.
    interval_tree: bool,

    /// Sparse 1-based character positions for GNU Emacs characters outside
    /// Unicode's scalar range.  The rope holds one placeholder scalar at each
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
            forced_modified: self.forced_modified,
            autosaved: self.autosaved,
            begv: self.begv,
            zv: self.zv,
            file: self.file.clone(),
            file_truename: self.file_truename.clone(),
            visited_file_modtime: self.visited_file_modtime,
            undo_list: self.undo_list,
            point_before_last_command_or_undo: self.point_before_last_command_or_undo,
            overlays: crate::overlay::OverlayTree::default(),
            text_properties: self.text_properties.clone(),
            interval_tree: self.interval_tree,
            extended_chars: self.extended_chars.clone(),
            inhibit_hooks: self.inhibit_hooks,
            text_conversion_style: self.text_conversion_style,
            text_conversion_style_is_local: self.text_conversion_style_is_local,
            multibyte: self.multibyte,
        }
    }
}

/// The buffer's text with its character and byte counts, GNU's Z and
/// Z_BYTE kept beside it.  A text of up to `SMALL_TEXT' bytes is one
/// string: ropey keeps such a text in a single leaf, whose counts it finds
/// by scanning the whole leaf on every bounds check (twice or three times
/// an edit, once a chunk lookup).  A longer text is a rope, and becomes a
/// string again when deletions bring it under half that.  All changes go
/// through `insert' and `remove', which keep the counts.
#[derive(Clone, Default)]
struct BufferText {
    store: TextStore,
    chars: usize,
    bytes: usize,
    /// The raw-byte markers (U+E080..U+E0FF) in the text, whose stored
    /// form is a byte longer than GNU's: a marker's byte position needs
    /// their count before it only when there are any.
    raw_markers: usize,
    /// A small text's rope for `text_rope', built when a scan asks for
    /// one and dropped by the next change.
    rope_view: RefCell<Option<Rope>>,
    /// The leaf of `rope_view' when that rope is the one leaf, found when
    /// it is built: a scan of it reads that chunk throughout, which
    /// ropey's own lookups count afresh at each bounds check.
    rope_leaf: Cell<Option<ChunkHint>>,
    /// Recent char/byte conversions of a rope's text.
    positions: Cell<PositionCache>,
    /// The rope's chunk the last walk reached.
    chunk: Cell<Option<ChunkView>>,
}

/// The chunk of a rope's text the last walk of its tree reached, with the
/// char and byte offsets it starts at: a lookup inside it scans from the
/// nearest position known there instead of walking the tree again, as
/// GNU's buf_charpos_to_bytepos scans from the known position nearest.
/// TEXT addresses the rope's leaf, which lives and does not change while
/// no `&mut' method runs (each of them forgets the view).
#[derive(Clone, Copy)]
struct ChunkView {
    text: usize,
    len: usize,
    byte: usize,
    char: usize,
    /// The chunk's characters, counted when a lookup by character first
    /// needs them (UNCOUNTED until then).
    chars: usize,
}

/// ChunkView::chars before the chunk's characters are counted.
const UNCOUNTED: usize = usize::MAX;

impl ChunkView {
    fn new(chunk: &str, byte: usize, char: usize) -> Self {
        Self {
            text: chunk.as_ptr() as usize,
            len: chunk.len(),
            byte,
            char,
            chars: UNCOUNTED,
        }
    }

    /// The chunk's text.
    ///
    /// # Safety
    ///
    /// The rope the view was taken from has not changed since.
    unsafe fn text<'t>(&self) -> &'t str {
        // SAFETY: the caller's guarantee keeps the leaf's bytes, valid
        // UTF-8 as a chunk of the rope, where they were.
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.text as *const u8,
                self.len,
            ))
        }
    }

    fn holds_byte(&self, byte: usize) -> bool {
        byte >= self.byte && byte < self.byte + self.len
    }
}

/// marker.c buf_charpos_to_bytepos's cache, for a rope whose characters
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

#[derive(Clone)]
enum TextStore {
    Small(String),
    Rope(Rope),
}

/// A buffer's text as a scan reads it apart from the buffer (see
/// `Buffer::scan_text').
pub enum ScanText {
    Small(String),
    Rope(Rope),
}

/// The rope leaf the buffer's last walk reached, as `Buffer::scan_hint'
/// hands it to a scan of a `ScanText::Rope' taken with it: the leaf's
/// address and length, and its first byte in the text.
#[derive(Clone, Copy)]
pub struct ChunkHint {
    text: usize,
    len: usize,
    byte: usize,
}

impl ChunkHint {
    /// The leaf's text and its first byte.
    ///
    /// # Safety
    ///
    /// ROPE is the `ScanText::Rope' taken from the buffer under the same
    /// borrow as the hint.  A clone of a rope shares its leaves, and ropey
    /// copies a shared leaf before an edit changes it, so the leaf stays
    /// where it was, unchanged, while ROPE lives.
    pub unsafe fn chunk<'r>(&self, _rope: &'r Rope) -> (&'r str, usize) {
        // SAFETY: the caller's guarantee; the leaf is a chunk of the rope,
        // valid UTF-8.
        let text = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.text as *const u8,
                self.len,
            ))
        };
        (text, self.byte)
    }
}

impl Default for TextStore {
    fn default() -> Self {
        TextStore::Small(String::new())
    }
}

/// The longest text kept as one string: about ropey's leaf size.
const SMALL_TEXT: usize = 1024;

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
            store: if text.len() <= SMALL_TEXT {
                TextStore::Small(text.to_owned())
            } else {
                TextStore::Rope(Rope::from_str(text))
            },
            chars: ropey::str_utils::byte_to_char_idx(text, text.len()),
            bytes: text.len(),
            raw_markers: count_raw_markers(text.as_bytes()),
            rope_view: RefCell::new(None),
            rope_leaf: Cell::new(None),
            positions: Cell::default(),
            chunk: Cell::new(None),
        }
    }

    /// The chunk of ROPE, this text's, holding char INDEX: the one the last
    /// walk reached when it does.
    fn view_at_char(&self, rope: &Rope, index: usize) -> ChunkView {
        if let Some(view) = self.chunk.get()
            && index >= view.char
            // A chunk holds no more characters than bytes.
            && index - view.char < view.len
        {
            let mut view = view;
            if view.chars == UNCOUNTED {
                // SAFETY: the view is of this unchanged rope.
                let text = unsafe { view.text() };
                view.chars = ropey::str_utils::byte_to_char_idx(text, text.len());
                self.chunk.set(Some(view));
            }
            if index - view.char < view.chars {
                return view;
            }
        }
        let (chunk, byte, char, _) = rope.chunk_at_char(index);
        let view = ChunkView::new(chunk, byte, char);
        self.chunk.set(Some(view));
        view
    }

    /// The chunk of ROPE holding byte BYTE, as `view_at_char'.
    fn view_at_byte(&self, rope: &Rope, byte: usize) -> ChunkView {
        if let Some(view) = self.chunk.get()
            && view.holds_byte(byte)
        {
            return view;
        }
        let (chunk, byte, char, _) = rope.chunk_at_byte(byte);
        let view = ChunkView::new(chunk, byte, char);
        self.chunk.set(Some(view));
        view
    }

    /// The raw-byte markers in the first BYTES bytes of the text.
    fn raw_markers_before(&self, bytes: usize) -> usize {
        if self.raw_markers == 0 {
            return 0;
        }
        match &self.store {
            TextStore::Small(text) => count_raw_markers(&text.as_bytes()[..bytes]),
            TextStore::Rope(rope) => rope
                .byte_slice(..bytes)
                .chunks()
                .map(|chunk| count_raw_markers(chunk.as_bytes()))
                .sum(),
        }
    }

    /// The raw-byte markers among the characters of RANGE.
    fn raw_markers_in(&self, range: std::ops::Range<usize>) -> usize {
        if self.raw_markers == 0 {
            return 0;
        }
        match &self.store {
            TextStore::Small(text) => {
                let start = ropey::str_utils::char_to_byte_idx(text, range.start);
                let end = ropey::str_utils::char_to_byte_idx(text, range.end);
                count_raw_markers(&text.as_bytes()[start..end])
            }
            TextStore::Rope(rope) => rope
                .slice(range)
                .chunks()
                .map(|chunk| count_raw_markers(chunk.as_bytes()))
                .sum(),
        }
    }

    #[inline]
    fn len_chars(&self) -> usize {
        self.chars
    }

    #[inline]
    fn len_bytes(&self) -> usize {
        self.bytes
    }

    /// The byte index of char index INDEX in a small TEXT.
    #[inline]
    fn small_byte(&self, text: &str, index: usize) -> usize {
        if self.chars == self.bytes {
            index
        } else {
            ropey::str_utils::char_to_byte_idx(text, index)
        }
    }

    /// The chunk holding char index INDEX, with the byte, char and line
    /// indices of its start (ropey's `chunk_at_char').
    #[inline]
    fn chunk_at_char(&self, index: usize) -> (&str, usize, usize, usize) {
        match &self.store {
            TextStore::Small(text) => (text, 0, 0, 0),
            TextStore::Rope(rope) if index < self.chars => {
                let view = self.view_at_char(rope, index);
                // SAFETY: the view is of this unchanged rope.
                (unsafe { view.text() }, view.byte, view.char, 0)
            }
            TextStore::Rope(rope) => rope.chunk_at_char(index),
        }
    }

    /// The chunk holding byte index INDEX, as `chunk_at_char'.
    #[inline]
    fn chunk_at_byte(&self, index: usize) -> (&str, usize, usize, usize) {
        match &self.store {
            TextStore::Small(text) => (text, 0, 0, 0),
            TextStore::Rope(rope) if index < self.bytes => {
                let view = self.view_at_byte(rope, index);
                // SAFETY: the view is of this unchanged rope.
                (unsafe { view.text() }, view.byte, view.char, 0)
            }
            TextStore::Rope(rope) => rope.chunk_at_byte(index),
        }
    }

    #[inline]
    fn char(&self, index: usize) -> char {
        match &self.store {
            TextStore::Small(text) => text[self.small_byte(text, index)..]
                .chars()
                .next()
                .expect("char index within the text"),
            TextStore::Rope(rope) => rope.char(index),
        }
    }

    #[inline]
    fn char_to_byte(&self, index: usize) -> usize {
        match &self.store {
            TextStore::Small(text) => self.small_byte(text, index),
            TextStore::Rope(rope) => {
                // The text's ends and an ASCII text need no walk; nor does
                // a position converted since the last edit before it.
                if self.chars == self.bytes || index == 0 {
                    return index;
                }
                if index == self.chars {
                    return self.bytes;
                }
                let mut positions = self.positions.get();
                if let Some(byte) = positions.by_char(index) {
                    return byte;
                }
                // Scan the chunk holding INDEX from the nearest position
                // known before it there.
                let view = self.view_at_char(rope, index);
                let (from, from_byte) = positions
                    .entries()
                    .filter(|entry| entry.0 >= view.char && entry.0 <= index)
                    .fold((view.char, view.byte), |best, entry| {
                        if entry.0 > best.0 { entry } else { best }
                    });
                // SAFETY: the view is of this unchanged rope.
                let text = unsafe { view.text() };
                let byte = from_byte
                    + ropey::str_utils::char_to_byte_idx(
                        &text[from_byte - view.byte..],
                        index - from,
                    );
                positions.record(index, byte);
                self.positions.set(positions);
                byte
            }
        }
    }

    /// The char index of the character byte index BYTE lies in.
    fn byte_to_char(&self, byte: usize) -> usize {
        match &self.store {
            TextStore::Small(text) => ropey::str_utils::byte_to_char_idx(text, byte),
            TextStore::Rope(rope) => {
                if self.chars == self.bytes || byte == 0 {
                    return byte;
                }
                if byte == self.bytes {
                    return self.chars;
                }
                let mut positions = self.positions.get();
                if let Some(index) = positions.by_byte(byte) {
                    return index;
                }
                let view = self.view_at_byte(rope, byte);
                let (from, from_byte) = positions
                    .entries()
                    .filter(|entry| entry.1 >= view.byte && entry.1 <= byte)
                    .fold((view.char, view.byte), |best, entry| {
                        if entry.1 > best.1 { entry } else { best }
                    });
                // SAFETY: the view is of this unchanged rope.
                let text = unsafe { view.text() };
                let index = from
                    + ropey::str_utils::byte_to_char_idx(
                        &text[from_byte - view.byte..],
                        byte - from_byte,
                    );
                // A byte that starts its character is that character's
                // position, remembered as buf_bytepos_to_charpos remembers
                // the last it converted (a match's registers then count
                // from each other); one in the middle names none.
                if text.is_char_boundary(byte - view.byte) {
                    positions.record(index, byte);
                    self.positions.set(positions);
                }
                index
            }
        }
    }

    #[inline]
    fn slice(&self, range: std::ops::Range<usize>) -> ropey::RopeSlice<'_> {
        match &self.store {
            TextStore::Small(text) => ropey::RopeSlice::from(
                &text[self.small_byte(text, range.start)..self.small_byte(text, range.end)],
            ),
            TextStore::Rope(rope) => rope.slice(range),
        }
    }

    /// The text of RANGE (char indices) as a String. A small store's bytes
    /// are copied as they are: a slice of them counts their chars and lines
    /// first.
    fn string(&self, range: std::ops::Range<usize>) -> String {
        match &self.store {
            TextStore::Small(text) => text
                [self.small_byte(text, range.start)..self.small_byte(text, range.end)]
                .to_owned(),
            TextStore::Rope(rope) => String::from(rope.slice(range)),
        }
    }

    fn chars(&self) -> ropey::iter::Chars<'_> {
        match &self.store {
            TextStore::Small(text) => ropey::RopeSlice::from(text.as_str()).chars(),
            TextStore::Rope(rope) => rope.chars(),
        }
    }

    fn len_lines(&self) -> usize {
        match &self.store {
            TextStore::Small(text) => ropey::str_utils::byte_to_line_idx(text, text.len()) + 1,
            TextStore::Rope(rope) => rope.len_lines(),
        }
    }

    fn line(&self, index: usize) -> ropey::RopeSlice<'_> {
        match &self.store {
            TextStore::Small(text) => ropey::RopeSlice::from(
                &text[ropey::str_utils::line_to_byte_idx(text, index)
                    ..ropey::str_utils::line_to_byte_idx(text, index + 1)],
            ),
            TextStore::Rope(rope) => rope.line(index),
        }
    }

    fn line_to_char(&self, index: usize) -> usize {
        match &self.store {
            TextStore::Small(text) => ropey::str_utils::line_to_char_idx(text, index),
            TextStore::Rope(rope) => rope.line_to_char(index),
        }
    }

    fn char_to_line(&self, index: usize) -> usize {
        match &self.store {
            TextStore::Small(text) => ropey::str_utils::char_to_line_idx(text, index),
            TextStore::Rope(rope) => rope.char_to_line(index),
        }
    }

    /// The text for a scan that must not hold the buffer: a rope's shared
    /// tree, or a small text's own copy, which costs less than the rope
    /// `rope' would build of it after each change.
    fn scan_text(&self) -> ScanText {
        match &self.store {
            TextStore::Small(text) => match self.rope_view.borrow().as_ref() {
                Some(rope) => ScanText::Rope(rope.clone()),
                None => ScanText::Small(text.clone()),
            },
            TextStore::Rope(rope) => ScanText::Rope(rope.clone()),
        }
    }

    /// The chunk the last walk of the rope reached, when it holds byte
    /// offset BYTE.
    fn scan_hint(&self, byte: usize) -> Option<ChunkHint> {
        let TextStore::Rope(_) = &self.store else {
            // A small text's rope, `scan_text''s when it has one, of one
            // leaf holds every byte.
            return self.rope_leaf();
        };
        self.chunk
            .get()
            .filter(|view| view.holds_byte(byte))
            .map(|view| ChunkHint {
                text: view.text,
                len: view.len,
                byte: view.byte,
            })
    }

    /// The text as a rope that never changes (a small text's is built
    /// once until the next change).
    fn rope(&self) -> Rope {
        match &self.store {
            TextStore::Small(text) => self
                .rope_view
                .borrow_mut()
                .get_or_insert_with(|| {
                    let rope = Rope::from_str(text);
                    let (leaf, _, _, _) = rope.chunk_at_byte(0);
                    self.rope_leaf
                        .set((leaf.len() == text.len()).then_some(ChunkHint {
                            text: leaf.as_ptr() as usize,
                            len: leaf.len(),
                            byte: 0,
                        }));
                    rope
                })
                .clone(),
            TextStore::Rope(rope) => rope.clone(),
        }
    }

    /// The single leaf of the rope `rope' answers, for a small text whose
    /// rope is one (see `rope_leaf').
    fn rope_leaf(&self) -> Option<ChunkHint> {
        match &self.store {
            TextStore::Small(_) if self.rope_view.borrow().is_some() => self.rope_leaf.get(),
            _ => None,
        }
    }

    fn whole_string(&self) -> String {
        match &self.store {
            TextStore::Small(text) => text.clone(),
            TextStore::Rope(rope) => String::from(rope),
        }
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
        let ascii = self.chars == self.bytes;
        let grown = match &mut self.store {
            TextStore::Small(small) => {
                let byte = if ascii {
                    char_index
                } else {
                    ropey::str_utils::char_to_byte_idx(small, char_index)
                };
                if self.bytes + text.len() > SMALL_TEXT {
                    // The rope is built from the pieces, the text not
                    // copied into the string first.
                    let mut builder = ropey::RopeBuilder::new();
                    builder.append(&small[..byte]);
                    builder.append(text);
                    builder.append(&small[byte..]);
                    Some(builder.finish())
                } else {
                    small.insert_str(byte, text);
                    None
                }
            }
            TextStore::Rope(rope) => {
                rope.insert(char_index, text);
                None
            }
        };
        if let Some(rope) = grown {
            self.store = TextStore::Rope(rope);
        }
        self.positions
            .get_mut()
            .inserted(char_index, added, text.len());
        *self.chunk.get_mut() = None;
        self.chars += added;
        self.bytes += text.len();
        self.raw_markers += raw_markers;
        *self.rope_view.get_mut() = None;
        self.check_counts();
    }

    fn remove(&mut self, range: std::ops::Range<usize>) {
        self.raw_markers -= if range.len() == self.chars {
            self.raw_markers
        } else {
            self.raw_markers_in(range.clone())
        };
        let bytes = match &mut self.store {
            TextStore::Small(_) if self.chars == self.bytes => range.len(),
            TextStore::Small(text) => {
                ropey::str_utils::char_to_byte_idx(text, range.end)
                    - ropey::str_utils::char_to_byte_idx(text, range.start)
            }
            // The rope's own count, read after the removal (in a rope of
            // more than one leaf, from the root's children).
            TextStore::Rope(rope) => {
                let before = self.bytes;
                rope.remove(range.clone());
                let bytes = before - rope.len_bytes();
                self.removed(range, bytes);
                return;
            }
        };
        self.remove_bytes(range, bytes);
    }

    /// `remove' for a caller that already holds the removed text.
    fn remove_text(&mut self, range: std::ops::Range<usize>, removed: &str) {
        self.raw_markers -= if range.len() == self.chars {
            self.raw_markers
        } else {
            count_raw_markers(removed.as_bytes())
        };
        self.remove_bytes(range, removed.len());
    }

    fn remove_bytes(&mut self, range: std::ops::Range<usize>, bytes: usize) {
        match &mut self.store {
            TextStore::Small(text) => {
                let start = if self.chars == self.bytes {
                    range.start
                } else {
                    ropey::str_utils::char_to_byte_idx(text, range.start)
                };
                text.replace_range(start..start + bytes, "");
            }
            TextStore::Rope(rope) => rope.remove(range.clone()),
        }
        self.removed(range, bytes);
    }

    /// The counts after RANGE, BYTES long, was removed; a rope that fell
    /// under half the small size becomes a string.
    fn removed(&mut self, range: std::ops::Range<usize>, bytes: usize) {
        self.positions.get_mut().removed(range.clone(), bytes);
        *self.chunk.get_mut() = None;
        self.chars -= range.len();
        self.bytes -= bytes;
        if let TextStore::Rope(rope) = &self.store
            && self.bytes <= SMALL_TEXT / 2
        {
            self.store = TextStore::Small(String::from(rope));
        }
        *self.rope_view.get_mut() = None;
        self.check_counts();
    }

    fn check_counts(&self) {
        if cfg!(debug_assertions) {
            let (chars, bytes) = match &self.store {
                TextStore::Small(text) => (
                    ropey::str_utils::byte_to_char_idx(text, text.len()),
                    text.len(),
                ),
                TextStore::Rope(rope) => (rope.len_chars(), rope.len_bytes()),
            };
            assert_eq!((self.chars, self.bytes), (chars, bytes));
            // Recounting a rope's raw-byte markers walks all its text,
            // which at every edit made a million insertions quadratic
            // (undo-tests' flush-lines buffer); a small text's are
            // recounted.
            if let TextStore::Small(text) = &self.store {
                assert_eq!(self.raw_markers, count_raw_markers(text.as_bytes()));
            }
        }
    }
}

/// REMAINING characters of a rope from char index NEXT, read chunk by
/// chunk in place: a ropey `Chars' iterator allocates its node stack. In
/// an ASCII rope a char index is a byte index, found without counting.
struct RopeChars<'a> {
    rope: &'a BufferText,
    ascii: bool,
    chars: std::str::Chars<'a>,
    next: usize,
    remaining: usize,
}

impl Iterator for RopeChars<'_> {
    type Item = char;

    fn next(&mut self) -> Option<char> {
        if self.remaining == 0 {
            return None;
        }
        let character = match self.chars.next() {
            Some(character) => character,
            None => {
                let (chunk, byte) = if self.ascii {
                    let (chunk, chunk_start, _, _) = self.rope.chunk_at_byte(self.next);
                    (chunk, self.next - chunk_start)
                } else {
                    let (chunk, _, chunk_start, _) = self.rope.chunk_at_char(self.next);
                    let byte = ropey::str_utils::char_to_byte_idx(chunk, self.next - chunk_start);
                    (chunk, byte)
                };
                self.chars = chunk[byte..].chars();
                self.chars.next()?
            }
        };
        self.next += 1;
        self.remaining -= 1;
        Some(character)
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
    pub(crate) forced_modified: bool,
    pub(crate) autosaved: bool,
    pub(crate) begv: usize,
    pub(crate) zv: usize,
    pub(crate) file: Option<String>,
    pub(crate) file_truename: Option<String>,
    pub(crate) visited_file_modtime: Option<FileModTime>,
    pub(crate) undo_list: Value,
    pub(crate) text_properties: Vec<TextPropertySpan>,
    pub(crate) extended_chars: Vec<(usize, u32)>,
    pub(crate) inhibit_hooks: bool,
    pub(crate) text_conversion_style: Value,
    pub(crate) text_conversion_style_is_local: bool,
    pub(crate) multibyte: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TextPropertySpan {
    pub start: usize,
    pub end: usize,
    pub props: Vec<(PropertyName, Value)>,
}

/// A buffer's or a string's span of text with a non-empty plist.
pub(crate) trait PropertySpan {
    fn bounds(&self) -> (usize, usize);
    fn plist(&self) -> &[(PropertyName, Value)];
}

impl PropertySpan for TextPropertySpan {
    fn bounds(&self) -> (usize, usize) {
        (self.start, self.end)
    }

    fn plist(&self) -> &[(PropertyName, Value)] {
        &self.props
    }
}

impl PropertySpan for crate::lisp::types::StringPropertySpan {
    fn bounds(&self) -> (usize, usize) {
        (self.start, self.end)
    }

    fn plist(&self) -> &[(PropertyName, Value)] {
        &self.props
    }
}

/// One interval of an object's text: [START, END) and its plist.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PropertyInterval<'a> {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) plist: &'a [(PropertyName, Value)],
    /// How many spans start at or before the interval's start: where the
    /// next and previous intervals are looked for.
    index: usize,
}

impl PropertyInterval<'_> {
    /// The count of spans starting at or before the interval, for a later
    /// `find_near' of a position near it.
    pub(crate) fn index(&self) -> usize {
        self.index
    }
}

/// An object's interval tree as textprop.c walks it: the whole text
/// [BEG, END), never empty, split at the bounds of its sorted, disjoint
/// property SPANS, each stretch between them an interval with the empty
/// plist.  (GNU may keep adjacent intervals with equal plists apart; no
/// walk here tells them from one.)
#[derive(Clone, Copy)]
pub(crate) struct PropertyIntervals<'a, S> {
    spans: &'a [S],
    beg: usize,
    end: usize,
}

impl<'a, S: PropertySpan> PropertyIntervals<'a, S> {
    pub(crate) fn new(spans: &'a [S], beg: usize, end: usize) -> Self {
        debug_assert!(beg < end);
        Self { spans, beg, end }
    }

    /// intervals.c:find_interval: the interval holding POS, the last one
    /// for POS at the end of the text.
    pub(crate) fn find(&self, pos: usize) -> PropertyInterval<'a> {
        let pos = pos.clamp(self.beg, self.end - 1);
        let index = self.spans.partition_point(|span| span.bounds().0 <= pos);
        self.interval_at(pos, index)
    }

    /// `find' from INDEX, the count of spans starting at or before a
    /// position near POS: the count for POS stepped to from it, as
    /// next_interval follows the tree from where it is, and searched for
    /// only when it lies further off.
    pub(crate) fn find_near(&self, pos: usize, index: usize) -> PropertyInterval<'a> {
        let spans = self.spans;
        let pos = pos.clamp(self.beg, self.end - 1);
        let mut index = index.min(spans.len());
        for _ in 0..4 {
            if spans.get(index).is_some_and(|span| span.bounds().0 <= pos) {
                index += 1;
            } else if index > 0 && spans[index - 1].bounds().0 > pos {
                index -= 1;
            } else {
                return self.interval_at(pos, index);
            }
        }
        self.find(pos)
    }

    /// The interval holding POS, of which INDEX spans start at or before
    /// it.
    fn interval_at(&self, pos: usize, index: usize) -> PropertyInterval<'a> {
        let spans: &'a [S] = self.spans;
        let before = index.checked_sub(1).map(|index| &spans[index]);
        if let Some(span) = before
            && pos < span.bounds().1
        {
            let (start, end) = span.bounds();
            return PropertyInterval {
                start,
                end,
                plist: span.plist(),
                index,
            };
        }
        PropertyInterval {
            start: before.map_or(self.beg, |span| span.bounds().1),
            end: spans.get(index).map_or(self.end, |span| span.bounds().0),
            plist: &[],
            index,
        }
    }

    /// next_interval.
    pub(crate) fn next(&self, interval: &PropertyInterval<'_>) -> Option<PropertyInterval<'a>> {
        (interval.end < self.end).then(|| self.find_near(interval.end, interval.index))
    }

    /// previous_interval.
    pub(crate) fn previous(&self, interval: &PropertyInterval<'_>) -> Option<PropertyInterval<'a>> {
        (interval.start > self.beg).then(|| self.find_near(interval.start - 1, interval.index))
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
            forced_modified: false,
            autosaved: false,
            begv: 1,
            zv: 1, // empty buffer: zv = 1
            file: None,
            file_truename: None,
            visited_file_modtime: None,
            undo_list: Value::Nil,
            point_before_last_command_or_undo: None,
            overlays: crate::overlay::OverlayTree::default(),
            text_properties: Vec::new(),
            interval_tree: false,
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
            forced_modified: false,
            autosaved: false,
            begv: 1,
            zv: len + 1,
            file: None,
            file_truename: None,
            visited_file_modtime: None,
            undo_list: Value::Nil,
            point_before_last_command_or_undo: None,
            overlays: crate::overlay::OverlayTree::default(),
            text_properties: Vec::new(),
            interval_tree: false,
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
    /// every old character boundary, including point-max.
    pub fn set_multibyte_representation(
        &mut self,
        enabled: bool,
        text: String,
        position_map: &[usize],
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
        // Overlay endpoints already use the rope's character coordinates.
        // The unibyte overlay API translates those coordinates at its public
        // boundary, so remapping them here would apply the byte-to-character
        // conversion twice.
        for span in &mut self.text_properties {
            span.start = remap(span.start);
            span.end = remap(span.end);
        }
        for (position, _) in &mut self.extended_chars {
            *position = remap(*position);
        }

        self.text = BufferText::from_str(&text);
        self.invalidate_char_cache();
        self.multibyte = enabled;
        self.text_replaced_without_record();
    }

    /// Total characters in the buffer (ignoring narrowing).
    pub fn size_total(&self) -> usize {
        self.text.len_chars()
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
    /// Iterating a Rope slice avoids paying for a tree lookup per character,
    /// which matters for primitives such as `skip-chars-forward' that often
    /// scan long regions of a large buffer.
    pub fn skip_forward_while(
        &mut self,
        limit: usize,
        mut predicate: impl FnMut(char) -> bool,
    ) -> usize {
        // The rope's chunks in place (syntax.c's skip_chars reads the
        // buffer text directly): a rope slice per call counted the
        // lines of its end chunks first, most of a short skip.
        let start = self.pt;
        let end = limit.clamp(start, self.zv);
        let mut remaining = end - start;
        let mut index = start - 1;
        let mut count = 0;
        'chunks: while remaining > 0 && index < self.text.len_chars() {
            let (chunk, _, chunk_start, _) = self.text.chunk_at_char(index);
            let byte = ropey::str_utils::char_to_byte_idx(chunk, index - chunk_start);
            if byte >= chunk.len() {
                break;
            }
            for ch in chunk[byte..].chars() {
                if remaining == 0 || !predicate(ch) {
                    break 'chunks;
                }
                count += 1;
                remaining -= 1;
                index += 1;
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
        let mut index = start - 1;
        let mut count = 0;
        'chunks: while remaining > 0 && index > 0 {
            let (chunk, _, chunk_start, _) = self.text.chunk_at_char(index - 1);
            let byte = ropey::str_utils::char_to_byte_idx(chunk, index - chunk_start);
            for ch in chunk[..byte].chars().rev() {
                if remaining == 0 || !predicate(ch) {
                    break 'chunks;
                }
                count += 1;
                remaining -= 1;
                index -= 1;
            }
        }
        self.pt -= count;
        count
    }

    /// The text of 0-based character range [FROM, TO), copied chunk by
    /// chunk: a rope slice counts the lines of its end chunks before
    /// anything is read, most of a short deletion's record.
    fn text_range_string(&self, from: usize, to: usize) -> String {
        self.text_range_with_bytes(from, to).0
    }

    /// `text_range_string' with the store byte offsets of the range's
    /// ends, from one walk to FROM's chunk when the range lies in it (as a
    /// short deletion's does): the characters are counted from FROM, not
    /// over the whole chunk.
    fn text_range_with_bytes(&self, from: usize, to: usize) -> (String, usize, usize) {
        let to = to.min(self.text.len_chars());
        // An ASCII text's char indexes are its byte indexes.
        let ascii = self.text_is_ascii();
        let mut out = String::with_capacity(to.saturating_sub(from));
        let mut index = from;
        let mut first = None;
        loop {
            let (chunk, chunk_byte, chunk_start, _) = self.text.chunk_at_char(index);
            let start = if ascii {
                index - chunk_start
            } else {
                ropey::str_utils::char_to_byte_idx(chunk, index - chunk_start)
            };
            let start_byte = *first.get_or_insert(chunk_byte + start);
            let rest = &chunk[start..];
            let wanted = to - index;
            let take = if ascii {
                wanted.min(rest.len())
            } else {
                ropey::str_utils::char_to_byte_idx(rest, wanted)
            };
            out.push_str(&rest[..take]);
            let taken = if ascii {
                take
            } else if take < rest.len() {
                wanted
            } else {
                // The range runs to the chunk's end or past it: count what
                // the chunk held.
                ropey::str_utils::byte_to_char_idx(rest, rest.len())
            };
            index += taken;
            if index >= to || taken == 0 {
                return (out, start_byte, chunk_byte + start + take);
            }
        }
    }

    /// search.c's find_newline forward: the 0-based index of the first
    /// newline at or after char index FROM, scanned a chunk at a time
    /// with memchr.
    fn newline_at_or_after(&self, from: usize) -> Option<usize> {
        let ascii = self.text_is_ascii();
        let mut index = from;
        while index < self.text.len_chars() {
            let (chunk, _, chunk_start, _) = self.text.chunk_at_char(index);
            let byte = if ascii {
                index - chunk_start
            } else {
                ropey::str_utils::char_to_byte_idx(chunk, index - chunk_start)
            };
            if let Some(offset) = memchr::memchr(b'\n', &chunk.as_bytes()[byte..]) {
                return Some(
                    chunk_start
                        + if ascii {
                            byte + offset
                        } else {
                            ropey::str_utils::byte_to_char_idx(chunk, byte + offset)
                        },
                );
            }
            let chunk_chars = if ascii {
                chunk.len()
            } else {
                ropey::str_utils::byte_to_char_idx(chunk, chunk.len())
            };
            if chunk_chars == 0 {
                break;
            }
            index = chunk_start + chunk_chars;
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
        let ascii = self.text_is_ascii();
        let mut index = before.min(self.text.len_chars());
        while index > 0 {
            let (chunk, _, chunk_start, _) = self.text.chunk_at_char(index - 1);
            let byte = if ascii {
                index - chunk_start
            } else {
                ropey::str_utils::char_to_byte_idx(chunk, index - chunk_start)
            };
            if let Some(offset) = memchr::memrchr(b'\n', &chunk.as_bytes()[..byte]) {
                return Some(
                    chunk_start
                        + if ascii {
                            offset
                        } else {
                            ropey::str_utils::byte_to_char_idx(chunk, offset)
                        },
                );
            }
            index = chunk_start;
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

    /// A shared handle on the text as it is now: ropey's rope clones in
    /// constant time and never changes once cloned, so a scan can read
    /// characters by index without holding the buffer borrowed.
    pub fn text_rope(&self) -> Rope {
        self.text.rope()
    }

    /// `text_rope' with its leaf when it is a single one (a small text's),
    /// which `ChunkHint::chunk' reads without asking the rope.
    pub fn text_rope_and_leaf(&self) -> (Rope, Option<ChunkHint>) {
        let rope = self.text.rope();
        (rope, self.text.rope_leaf())
    }

    /// The text for a scan apart from the buffer: the rope, or a small
    /// text's copy.
    pub fn scan_text(&self) -> ScanText {
        self.text.scan_text()
    }

    /// The rope leaf holding the stored text's byte offset BYTE when the
    /// buffer's last walk reached it (point's, after a conversion of
    /// point), for a scan of `scan_text''s rope taken under the same
    /// borrow to start in without walking the tree (see `ChunkHint').
    pub fn scan_hint(&self, byte: usize) -> Option<ChunkHint> {
        self.text.scan_hint(byte)
    }

    /// The stored text's characters and UTF-8 bytes, which the buffer
    /// keeps: a rope of one leaf (a small text's) counts its text anew for
    /// each length or chunk query.
    pub fn text_counts(&self) -> (usize, usize) {
        (self.text.len_chars(), self.text.len_bytes())
    }

    pub fn full_buffer_string(&self) -> String {
        self.text.whole_string()
    }

    /// Text of up to COUNT lines starting at 1-based LINE, one string per
    /// line without its terminator.  The rope resolves each line seek in
    /// O(log n), so a redisplay window costs the window — never the file.
    pub fn lines_from(&self, line: usize, count: usize) -> Vec<String> {
        let total = self.text.len_lines();
        let start = line.saturating_sub(1).min(total);
        let end = start.saturating_add(count).min(total);
        (start..end)
            .map(|index| {
                let mut text = String::from(self.text.line(index));
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
    /// the rope in place.
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
        Ok(RopeChars {
            rope: &self.text,
            ascii: self.text.len_bytes() == self.text.len_chars(),
            chars: "".chars(),
            next: from - 1,
            remaining: to - from,
        })
    }

    /// [FROM, TO) (1-based) of an ASCII text as a slice of the one rope
    /// chunk holding it, so nothing is decoded; None when the text has a
    /// multibyte character or the range crosses a chunk.
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
        let (chunk, chunk_start, _, _) = self.text.chunk_at_byte(start);
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

    pub fn substring_property_spans(&self, from: usize, to: usize) -> Vec<TextPropertySpan> {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from >= to {
            return Vec::new();
        }
        // The spans meeting [from, to), by binary search.
        let lo = self
            .text_properties
            .partition_point(|span| span.end <= from);
        let hi = self.text_properties.partition_point(|span| span.start < to);
        let mut spans = Vec::new();
        for span in &self.text_properties[lo..hi] {
            let start = span.start.max(from);
            let end = span.end.min(to);
            if start < end {
                spans.push(TextPropertySpan {
                    start: start - from,
                    end: end - from,
                    props: span.props.clone(),
                });
            }
        }
        merge_adjacent_spans(spans)
    }

    pub fn full_property_spans(&self) -> Vec<TextPropertySpan> {
        self.text_properties.clone()
    }

    pub fn extended_char_at(&self, pos: usize) -> Option<u32> {
        self.extended_chars
            .binary_search_by_key(&pos, |(position, _)| *position)
            .ok()
            .map(|index| self.extended_chars[index].1)
    }

    /// Whether any character of [FROM, TO) has a sidecar entry (a code
    /// outside Unicode's scalars), which the regexp haystack of the span
    /// then renders differently from the rope's text.
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

    /// `has_text_property_named' for the spans meeting FROM to TO.
    pub fn has_text_property_named_in(&self, property: &str, from: usize, to: usize) -> bool {
        let Some(symbol) = crate::lisp::types::SymbolName::find(property) else {
            return false;
        };
        let name = PropertyName::new(Value::Symbol(symbol));
        let lo = self
            .text_properties
            .partition_point(|span| span.end <= from);
        let hi = self.text_properties.partition_point(|span| span.start < to);
        self.text_properties
            .get(lo..hi)
            .unwrap_or_default()
            .iter()
            .any(|span| span.props.iter().any(|(candidate, _)| *candidate == name))
    }

    pub fn has_text_property_named(&self, property: &str) -> bool {
        // The symbol PROPERTY names, found once (no symbol has that name
        // when none exists): each stored name is then the same symbol or
        // not, as EQ compares them, where comparing each name's text took
        // a call of memcmp a property.
        let Some(symbol) = crate::lisp::types::SymbolName::find(property) else {
            return false;
        };
        let name = PropertyName::new(Value::Symbol(symbol));
        self.text_properties
            .iter()
            .any(|span| span.props.iter().any(|(candidate, _)| *candidate == name))
    }

    /// Rewrite every Lisp value stored in this buffer through COPY
    /// (image-template deep clone; see `Interpreter::deep_clone_image').
    pub(crate) fn rewrite_lisp_values(&mut self, copy: &mut impl FnMut(&Value) -> Value) {
        fn rewrite_properties(
            props: &mut [(PropertyName, Value)],
            copy: &mut impl FnMut(&Value) -> Value,
        ) {
            for (name, value) in props {
                *name = PropertyName::new(copy(&name.value()));
                *value = copy(value);
            }
        }
        self.text_conversion_style = copy(&self.text_conversion_style);
        self.mark_active = copy(&self.mark_active);
        for span in &mut self.text_properties {
            rewrite_properties(&mut span.props, copy);
        }
        self.undo_list = copy(&self.undo_list);
    }

    /// Visit every Lisp value retained by this buffer.  Garbage collection
    /// uses the same ownership boundary as `rewrite_lisp_values`: the rope,
    /// positions and file metadata are native data, while properties, the
    /// undo list and attached overlay objects are Lisp roots.
    pub(crate) fn visit_lisp_values(&self, visit: &mut impl FnMut(&Value)) {
        visit(&self.text_conversion_style);
        visit(&self.mark_active);
        if let Some(marker) = self.mark_object() {
            visit(&Value::Marker(marker));
        }
        for span in &self.text_properties {
            for (name, value) in &span.props {
                visit(&name.value());
                visit(value);
            }
        }
        visit(&self.undo_list);
        self.overlays.visit_lisp_values(visit);
    }

    /// How many text-property spans this buffer holds (the census's
    /// `intervals' analogue for buffer text).
    pub(crate) fn text_property_span_count(&self) -> usize {
        self.text_properties.len()
    }

    /// Whether any text property is set.
    pub fn has_text_properties(&self) -> bool {
        !self.text_properties.is_empty()
    }

    /// Whether the buffer has intervals in GNU's terms, every plist of
    /// which may be empty (`interval_tree').
    pub fn has_interval_tree(&self) -> bool {
        self.interval_tree || !self.text_properties.is_empty()
    }

    pub fn text_property_at(&self, pos: usize, prop: impl PropertyKey) -> Option<Value> {
        self.text_properties_at_ref(pos)
            .iter()
            .find(|(name, _)| prop.names(name))
            .map(|(_, value)| *value)
    }

    pub(crate) fn text_properties_at_ref(&self, pos: usize) -> &[(PropertyName, Value)] {
        if pos < self.point_min() || pos >= self.point_max() {
            return &[];
        }
        text_property_span_at(&self.text_properties, pos)
            .map(|span| span.props.as_slice())
            .unwrap_or_default()
    }

    /// F over the intervals meeting [FROM, TO) as textprop.c walks them
    /// (find_interval, then next_interval): each span clipped to the
    /// range with its plist, and each stretch between spans as an
    /// interval with none.  Stops at F's first error.
    pub(crate) fn try_for_each_property_interval<E>(
        &self,
        from: usize,
        to: usize,
        mut f: impl FnMut(usize, usize, &[(PropertyName, Value)]) -> Result<(), E>,
    ) -> Result<(), E> {
        let spans = &self.text_properties;
        let mut cursor = from;
        let lo = spans.partition_point(|span| span.end <= from);
        for span in &spans[lo..] {
            if span.start >= to {
                break;
            }
            if cursor < span.start {
                f(cursor, span.start, &[])?;
                cursor = span.start;
            }
            let end = span.end.min(to);
            f(cursor, end, &span.props)?;
            cursor = end;
        }
        if cursor < to {
            f(cursor, to, &[])?;
        }
        Ok(())
    }

    /// The buffer's intervals over its whole text [BEG, Z), narrowing
    /// aside; None where buffer_intervals is null (no interval tree, or no
    /// text).
    pub(crate) fn property_intervals(&self) -> Option<PropertyIntervals<'_, TextPropertySpan>> {
        let z = self.size_total() + 1;
        (self.has_interval_tree() && z > 1)
            .then(|| PropertyIntervals::new(&self.text_properties, 1, z))
    }

    pub fn text_properties_at(&self, pos: usize) -> Vec<(PropertyName, Value)> {
        self.text_properties_at_ref(pos).to_vec()
    }

    /// The interval [start, end) around POS over which the text-property
    /// list is constant: a covering span's own bounds, or the
    /// property-free gap up to the neighboring spans (syntax.h's
    /// b_property/e_property discipline for update_syntax_table).
    pub(crate) fn text_property_interval_around(&self, pos: usize) -> (usize, usize) {
        let begv = self.point_min();
        let zv = self.point_max();
        let spans = &self.text_properties;
        let insertion = spans.partition_point(|span| span.start <= pos);
        if let Some(span) = insertion.checked_sub(1).and_then(|index| spans.get(index))
            && pos < span.end
        {
            return (span.start.max(begv), span.end.min(zv));
        }
        let start = insertion
            .checked_sub(1)
            .and_then(|index| spans.get(index))
            .map(|span| span.end)
            .unwrap_or(0);
        let end = spans.get(insertion).map(|span| span.start).unwrap_or(zv);
        (start.max(begv), end.min(zv).max(start.max(begv)))
    }

    pub fn add_text_properties(
        &mut self,
        start: usize,
        end: usize,
        props: &[(PropertyName, Value)],
    ) {
        self.modify_text_properties(start, end, |mut current| {
            // GNU replaces existing properties in place and CONSES new
            // ones onto the front of the interval plist, so
            // `text-properties-at' lists later additions first.
            for (name, value) in props {
                if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == name) {
                    *existing = *value;
                } else {
                    current.insert(0, (*name, *value));
                }
            }
            current
        });
    }

    /// F over the plist of each interval of [START, END), its result the
    /// interval's new plist, in one pass (recorded and counted as a
    /// property change where one changes).
    pub(crate) fn update_text_properties<F>(&mut self, start: usize, end: usize, f: F)
    where
        F: FnMut(Vec<(PropertyName, Value)>) -> Vec<(PropertyName, Value)>,
    {
        self.modify_text_properties(start, end, f);
    }

    pub fn put_text_property(
        &mut self,
        start: usize,
        end: usize,
        name: PropertyName,
        value: Value,
    ) {
        self.modify_text_properties(start, end, |mut current| {
            if let Some((_, existing)) = current.iter_mut().find(|(key, _)| *key == name) {
                *existing = value;
            } else {
                current.insert(0, (name, value));
            }
            current
        });
    }

    pub fn set_text_properties(
        &mut self,
        start: usize,
        end: usize,
        props: &[(PropertyName, Value)],
    ) {
        let tick = self.modiff;
        self.modify_text_properties(start, end, |_| props.to_vec());
        // textprop.c set_text_properties modifies a buffer that has
        // intervals whether or not a plist changes.
        let (start, end) = (start.max(self.point_min()), end.min(self.point_max()));
        if self.modiff == tick && start < end && self.has_interval_tree() {
            if !self.is_modified() {
                self.record_first_change();
            }
            self.count_text_property_change(start, end);
        }
    }

    /// textprop.c modify_text_properties's count of a property change.
    fn count_text_property_change(&mut self, start: usize, end: usize) {
        modiff_incr(&mut self.modiff, 1);
        self.log_edit(start, end, end, false);
        self.autosaved = false;
    }

    pub fn remove_list_of_text_properties(
        &mut self,
        start: usize,
        end: usize,
        names: &[PropertyName],
    ) {
        self.modify_text_properties(start, end, |current| {
            current
                .into_iter()
                .filter(|(key, _)| !names.iter().any(|name| name == key))
                .collect()
        });
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
            let (chunk, chunk_byte, _, _) = self.text.chunk_at_byte(index);
            return Some(char::from(chunk.as_bytes()[index - chunk_byte]));
        }
        if let Some(ch) = self.char_cache.borrow().character(index) {
            return Some(ch);
        }

        // A window of the chunk around INDEX, decoded once for the reads
        // near it (a scan's next characters or its previous ones), where
        // decoding the whole chunk cost each column count after an edit
        // a pass over up to a kilobyte.
        let (chunk, _, chunk_start, _) = self.text.chunk_at_char(index);
        let first = index.saturating_sub(CHAR_CACHE_BEHIND).max(chunk_start);
        let byte = ropey::str_utils::char_to_byte_idx(chunk, first - chunk_start);
        let mut cache = self.char_cache.borrow_mut();
        cache.start = first;
        cache.chars.clear();
        cache
            .chars
            .extend(chunk[byte..].chars().take(CHAR_CACHE_WINDOW));
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
        self.insert_with_properties(s, None)
    }

    /// Properties inherited by `insert-and-inherit' at POS.
    ///
    /// GNU text-property inheritance is directional: rear-sticky values on
    /// the preceding character normally win, front-sticky values on the
    /// following character fill gaps, and the two stickiness-control
    /// properties never propagate to the inserted text themselves.
    pub fn inherited_text_properties(
        &self,
        pos: usize,
        default_nonsticky: Option<&Value>,
    ) -> Vec<(PropertyName, Value)> {
        let previous = if pos > self.point_min() {
            self.text_properties_at(pos - 1)
        } else {
            Vec::new()
        };
        let following = if pos < self.point_max() {
            self.text_properties_at(pos)
        } else {
            Vec::new()
        };
        let rear_nonsticky = property_value(&previous, "rear-nonsticky").cloned();
        let front_sticky = property_value(&following, "front-sticky").cloned();
        let mut inherited = Vec::new();

        for (name, value) in previous {
            if is_stickiness_control(&name)
                || default_property_nonsticky(default_nonsticky, &name)
                || property_named_by_stickiness(rear_nonsticky.as_ref(), &name)
                || value.is_nil()
            {
                continue;
            }
            inherited.push((name, value));
        }
        for (name, value) in following {
            if is_stickiness_control(&name)
                || !property_named_by_stickiness(front_sticky.as_ref(), &name)
                || value.is_nil()
                || inherited.iter().any(|(existing, _)| existing == &name)
            {
                continue;
            }
            inherited.push((name, value));
        }
        inherited
    }

    pub fn insert_with_properties(
        &mut self,
        s: &str,
        props: Option<Vec<(PropertyName, Value)>>,
    ) -> usize {
        self.insert_with_properties_and_markers(s, props, false)
    }

    /// `insert' of a text whose COUNTS its maker already has.
    pub fn insert_counted(&mut self, s: &str, counts: TextCounts) -> usize {
        self.insert_text(s, None, false, Some(counts))
    }

    pub(crate) fn insert_with_properties_and_markers(
        &mut self,
        s: &str,
        props: Option<Vec<(PropertyName, Value)>>,
        before_markers: bool,
    ) -> usize {
        self.insert_text(s, props, before_markers, None)
    }

    fn insert_text(
        &mut self,
        s: &str,
        props: Option<Vec<(PropertyName, Value)>>,
        before_markers: bool,
        counts: Option<TextCounts>,
    ) -> usize {
        if s.is_empty() {
            return self.pt;
        }

        let insert_at = self.pt;
        let idx0 = self.pt - 1; // 0-based
        // The rope keeps its length; what it grew by is the insertion's.
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

        self.adjust_text_properties_for_insert(insert_at, nchars);
        self.adjust_extended_chars_for_insert(insert_at, nchars);
        if let Some(props) = props
            && !props.is_empty()
        {
            // Fresh text: keep the given plist order (GNU grafts intervals).
            self.graft_text_properties(insert_at, insert_at + nchars, &props);
        }

        modiff_incr(&mut self.modiff, nchars);
        self.chars_modiff = self.modiff;
        self.log_edit(insert_at, insert_at, insert_at + nchars, true);
        self.autosaved = false;
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
    }

    /// undo.c:record_change: the LENGTH characters at BEG about to be
    /// replaced by as many, recorded as their deletion and an insertion.
    pub(crate) fn record_change(&mut self, beg: usize, length: usize) {
        if self.undo_recording_disabled() {
            return;
        }
        let old = self.text_range_string(beg - 1, beg - 1 + length);
        let string = self.deleted_text_value(&old, beg, beg + length);
        self.record_delete(beg, string, length);
        self.record_insert(beg, length);
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
        self.autosaved = false;
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
    pub fn replace_range(
        &mut self,
        from: usize,
        to: usize,
        text: &str,
        extended: &[(usize, u32)],
        markers: bool,
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
        self.adjust_text_properties_for_delete(from, to);
        self.adjust_extended_chars_for_delete(from, to);
        self.adjust_text_properties_for_insert(from, inschars);
        self.adjust_extended_chars_for_insert(from, inschars);
        self.extended_chars
            .extend(extended.iter().map(|(offset, code)| (from + offset, *code)));
        self.extended_chars
            .sort_unstable_by_key(|(position, _)| *position);
        if self.text.len_chars() == 0 {
            self.interval_tree = false;
        }
        self.zv = self.zv + inschars - nchars_del;
        self.refresh_marker_byte_positions();
        modiff_incr(&mut self.modiff, nchars_del + inschars);
        self.chars_modiff = self.modiff;
        self.log_edit(from, to, from + inschars, true);
        self.autosaved = false;
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
        self.adjust_text_properties_for_delete(from, to);
        if self.text.len_chars() == 0 {
            self.interval_tree = false;
        }
        self.adjust_extended_chars_for_delete(from, to);

        // Shrink accessible region
        self.zv -= nchars;

        modiff_incr(&mut self.modiff, nchars);
        self.chars_modiff = self.modiff;
        self.log_edit(from, to, from, true);
        self.autosaved = false;
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
        self.forced_modified || self.modiff != self.save_modiff
    }

    pub fn set_unmodified(&mut self) {
        self.save_modiff = self.modiff;
        self.forced_modified = false;
        self.autosaved = false;
    }

    pub fn visited_file_modtime(&self) -> Option<FileModTime> {
        self.visited_file_modtime
    }

    pub fn set_visited_file_modtime(&mut self, modtime: Option<FileModTime>) {
        self.visited_file_modtime = modtime;
    }

    pub fn set_modified(&mut self) {
        if !self.is_modified() {
            self.modiff = self.modiff.saturating_add(1);
        }
        self.forced_modified = true;
        self.autosaved = false;
    }

    pub fn is_autosaved(&self) -> bool {
        self.is_modified() && self.autosaved
    }

    pub fn set_autosaved(&mut self) {
        if self.is_modified() {
            self.autosaved = true;
        }
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
            self.substring_property_spans(from, to)
                .into_iter()
                .map(|span| crate::lisp::types::StringPropertySpan {
                    start: span.start,
                    end: span.end,
                    props: span.props,
                })
                .collect(),
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
        self.text = BufferText::default();
        *self.char_cache.get_mut() = RopeCharCache::default();
        self.edits = std::collections::VecDeque::new();
        self.undo_list = Value::Nil;
        self.point_before_last_command_or_undo = None;
        self.text_properties = Vec::new();
        self.interval_tree = false;
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
            forced_modified: self.forced_modified,
            autosaved: self.autosaved,
            begv: self.begv,
            zv: self.zv,
            file: self.file.clone(),
            file_truename: self.file_truename.clone(),
            visited_file_modtime: self.visited_file_modtime,
            undo_list: self.undo_list,
            text_properties: self.text_properties.clone(),
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
            forced_modified: parts.forced_modified,
            autosaved: parts.autosaved,
            begv: parts.begv,
            zv: parts.zv,
            file: parts.file,
            file_truename: parts.file_truename,
            visited_file_modtime: parts.visited_file_modtime,
            undo_list: parts.undo_list,
            point_before_last_command_or_undo: None,
            overlays: crate::overlay::OverlayTree::default(),
            interval_tree: !parts.text_properties.is_empty(),
            text_properties: parts.text_properties,
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
        std::mem::swap(&mut self.forced_modified, &mut other.forced_modified);
        std::mem::swap(&mut self.autosaved, &mut other.autosaved);
        std::mem::swap(&mut self.begv, &mut other.begv);
        std::mem::swap(&mut self.zv, &mut other.zv);
        std::mem::swap(&mut self.undo_list, &mut other.undo_list);
        std::mem::swap(&mut self.overlays, &mut other.overlays);
        std::mem::swap(&mut self.text_properties, &mut other.text_properties);
        std::mem::swap(&mut self.interval_tree, &mut other.interval_tree);
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
    /// the text has fewer. The rope counts only newlines as line breaks.
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
        let slice = self.text.slice(start..end);
        // Count newlines + 1 (last line might not end with newline)
        let newlines = slice.chars().filter(|&c| c == '\n').count();
        if end > start { newlines + 1 } else { 0 }
    }

    /// intervals.c:adjust_intervals_for_insertion, then the plain insertion's
    /// `graft_intervals_into_buffer' with no intervals to graft: the
    /// inserted text has no properties, so the spans after the point move
    /// and the one the point falls inside splits around the new text.  In
    /// place: the whole list used to be cloned, plist by plist, rebuilt,
    /// sorted and re-merged on every insertion (0.36 ms a
    /// `decode-coding-region' in a dired listing of 4,000 spans).
    fn adjust_text_properties_for_insert(&mut self, pos: usize, nchars: usize) {
        if nchars == 0 {
            return;
        }
        let spans = &mut self.text_properties;
        let first = spans.partition_point(|span| span.end <= pos);
        for span in &mut spans[first..] {
            if span.start >= pos {
                span.start += nchars;
                span.end += nchars;
            }
        }
        // Only the first affected span can hold the point strictly
        // inside; it did not move above.
        if first < spans.len() && spans[first].start < pos {
            let tail = TextPropertySpan {
                start: pos + nchars,
                end: spans[first].end + nchars,
                props: spans[first].props.clone(),
            };
            spans[first].end = pos;
            spans.insert(first + 1, tail);
        }
    }

    /// intervals.c:adjust_intervals_for_deletion: the spans inside the
    /// deleted text go, the ones straddling its ends are cut to it, the
    /// ones after it move, and the two cut ends merge when they carry the
    /// same properties.  In place, over the spans the deletion meets (see
    /// `adjust_text_properties_for_insert').
    fn adjust_text_properties_for_delete(&mut self, from: usize, to: usize) {
        if from >= to {
            return;
        }
        let removed = to - from;
        self.interval_tree |= !self.text_properties.is_empty();
        let spans = &mut self.text_properties;
        let lo = spans.partition_point(|span| span.end <= from);
        let hi = spans.partition_point(|span| span.start < to);
        let mut pieces = Vec::new();
        if lo < hi {
            if spans[lo].start < from {
                pieces.push(TextPropertySpan {
                    start: spans[lo].start,
                    end: from,
                    props: spans[lo].props.clone(),
                });
            }
            if spans[hi - 1].end > to {
                pieces.push(TextPropertySpan {
                    start: from,
                    end: spans[hi - 1].end - removed,
                    props: spans[hi - 1].props.clone(),
                });
            }
        }
        for span in &mut spans[hi..] {
            span.start -= removed;
            span.end -= removed;
        }
        // The cut ends meet each other and their untouched neighbours;
        // merge over that window only.
        let splice_lo = lo.saturating_sub(1);
        let splice_hi = (hi + 1).min(spans.len());
        let mut window: Vec<TextPropertySpan> = spans[splice_lo..lo].to_vec();
        window.extend(pieces);
        window.extend(spans[hi..splice_hi].iter().cloned());
        let window = merge_adjacent_spans(window);
        spans.splice(splice_lo..splice_hi, window);
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

    fn modify_text_properties<F>(&mut self, start: usize, end: usize, f: F)
    where
        F: FnMut(Vec<(PropertyName, Value)>) -> Vec<(PropertyName, Value)>,
    {
        self.modify_text_properties_with_undo(start, end, true, f);
    }

    /// intervals.c graft_intervals_into_buffer: freshly inserted text
    /// takes PROPS as part of its insertion, which alone is recorded and
    /// counted.
    pub(crate) fn graft_text_properties(
        &mut self,
        start: usize,
        end: usize,
        props: &[(PropertyName, Value)],
    ) {
        self.modify_text_properties_with_undo(start, end, false, |_| props.to_vec());
    }

    /// RECORD: a change of the properties alone (textprop.c
    /// modify_text_properties), recorded for undo and counted; otherwise
    /// a graft into text just inserted.
    fn modify_text_properties_with_undo<F>(
        &mut self,
        start: usize,
        end: usize,
        record: bool,
        mut f: F,
    ) where
        F: FnMut(Vec<(PropertyName, Value)>) -> Vec<(PropertyName, Value)>,
    {
        let start = start.max(self.point_min());
        let end = end.min(self.point_max());
        if start >= end {
            return;
        }
        self.interval_tree |= !self.text_properties.is_empty();

        // The spans are sorted by start and disjoint: only those meeting
        // [start, end), and the neighbour on either side for the merge,
        // are read or rewritten.  Cloning and rebuilding the whole list
        // per write cost dired's listing of 12,000 files half a
        // millisecond per `put-text-property' (textprop.c splits and
        // merges the intervals at the edges alone).
        let spans = &self.text_properties;
        let lo = spans.partition_point(|span| span.end <= start);
        let hi = spans.partition_point(|span| span.start < end);
        let affected = &spans[lo..hi];

        let mut pieces = Vec::with_capacity(affected.len() + 3);
        if let Some(first) = affected.first()
            && first.start < start
        {
            pieces.push(TextPropertySpan {
                start: first.start,
                end: start,
                props: first.props.clone(),
            });
        }
        let mut boundaries = vec![start, end];
        for span in affected {
            boundaries.push(span.start.max(start));
            boundaries.push(span.end.min(end));
        }
        boundaries.sort_unstable();
        boundaries.dedup();

        let mut undo_records = Vec::new();
        let mut changed = false;
        for window in boundaries.windows(2) {
            let seg_start = window[0];
            let seg_end = window[1];
            if seg_start >= seg_end {
                continue;
            }
            let current = properties_at_from(affected, seg_start);
            let next = f(current.clone());
            if !text_property_plists_eq(&current, &next) {
                changed = true;
                if record && !self.undo_recording_disabled() {
                    for (name, old_value) in &current {
                        let value_changed = next
                            .iter()
                            .find(|(candidate, _)| candidate == name)
                            .is_none_or(|(_, new_value)| {
                                !text_property_values_eq(old_value, new_value)
                            });
                        if value_changed {
                            undo_records
                                .push(property_undo_entry(name, *old_value, seg_start, seg_end));
                        }
                    }
                    for (name, _) in &next {
                        if !current.iter().any(|(candidate, _)| candidate == name) {
                            undo_records.push(property_undo_entry(
                                name,
                                Value::Nil,
                                seg_start,
                                seg_end,
                            ));
                        }
                    }
                }
            }
            if !next.is_empty() {
                pieces.push(TextPropertySpan {
                    start: seg_start,
                    end: seg_end,
                    props: next,
                });
            }
        }
        if let Some(last) = affected.last()
            && last.end > end
        {
            pieces.push(TextPropertySpan {
                start: end,
                end: last.end,
                props: last.props.clone(),
            });
        }
        if !changed {
            return;
        }
        // textprop.c:modify_text_properties records the first change
        // since a save, then each interval's record_property_change.
        if record && !self.undo_recording_disabled() {
            if !self.is_modified() {
                self.record_first_change();
            }
            for entry in undo_records {
                self.push_undo(entry);
            }
        }
        // Merge with the untouched neighbours on either side, as the
        // whole-list merge did, by taking them into the rewritten window.
        let splice_lo = lo.saturating_sub(1);
        let splice_hi = (hi + 1).min(self.text_properties.len());
        let mut window: Vec<TextPropertySpan> = self.text_properties[splice_lo..lo].to_vec();
        window.extend(pieces);
        window.extend(self.text_properties[hi..splice_hi].iter().cloned());
        let window = merge_adjacent_spans(window);
        self.text_properties.splice(splice_lo..splice_hi, window);
        self.interval_tree |= !self.text_properties.is_empty();
        if record {
            self.count_text_property_change(start, end);
        }
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

fn property_undo_entry(name: &PropertyName, value: Value, start: usize, end: usize) -> Value {
    Value::cons(
        Value::Nil,
        Value::cons(
            name.value(),
            Value::cons(
                value,
                Value::cons(Value::Integer(start as i64), Value::Integer(end as i64)),
            ),
        ),
    )
}

fn text_property_span_at(spans: &[TextPropertySpan], pos: usize) -> Option<&TextPropertySpan> {
    let insertion = spans.partition_point(|span| span.start <= pos);
    insertion
        .checked_sub(1)
        .and_then(|index| spans.get(index))
        .filter(|span| pos < span.end)
}

fn properties_at_from(spans: &[TextPropertySpan], pos: usize) -> Vec<(PropertyName, Value)> {
    text_property_span_at(spans, pos)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

fn property_value<'a>(props: &'a [(PropertyName, Value)], name: &str) -> Option<&'a Value> {
    props
        .iter()
        .find(|(property, _)| property == name)
        .map(|(_, value)| value)
}

fn is_stickiness_control(name: &PropertyName) -> bool {
    *name == "front-sticky" || *name == "rear-nonsticky"
}

/// textprop.c's `!NILP (Fmemq (prop, stickiness))' for a stickiness
/// property's value: t names every property.
fn property_named_by_stickiness(setting: Option<&Value>, name: &PropertyName) -> bool {
    match setting.map(|v| v.kind()) {
        Some(Kind::T) => true,
        Some(value @ Kind::Cons(_)) => value
            .value()
            .to_vec()
            .is_ok_and(|items| items.iter().any(|item| PropertyName::new(*item) == *name)),
        _ => false,
    }
}

/// textprop.c's `Fassq (prop, Vtext_property_default_nonsticky)' with a
/// non-nil cdr.
fn default_property_nonsticky(defaults: Option<&Value>, name: &PropertyName) -> bool {
    let Some(defaults) = defaults.and_then(|value| value.to_vec().ok()) else {
        return false;
    };
    defaults.iter().any(|entry| {
        let Some((property, nonsticky)) = (entry).cons_cells() else {
            return false;
        };
        PropertyName::new(property.get()) == *name && nonsticky.get().is_truthy()
    })
}

pub(crate) fn text_property_plists_eq(
    left: &[(PropertyName, Value)],
    right: &[(PropertyName, Value)],
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(name, value)| {
            right
                .iter()
                .find(|(candidate, _)| candidate == name)
                .is_some_and(|(_, candidate)| text_property_values_eq(value, candidate))
        })
}

pub(crate) fn text_property_values_eq(left: &Value, right: &Value) -> bool {
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
        (Kind::Integer(left), Kind::Integer(right)) => left == right,
        (Kind::BigInteger(left), Kind::BigInteger(right)) => left == right,
        (Kind::Float(left), Kind::Float(right)) => left == right,
        (Kind::Symbol(left), Kind::Symbol(right)) => left == right,
        (Kind::BuiltinFunc(left), Kind::BuiltinFunc(right)) => left == right,
        (Kind::NativeFunction(left), Kind::NativeFunction(right)) => left.ptr_eq(&right),
        (Kind::NativeCompUnit(left), Kind::NativeCompUnit(right)) => left.ptr_eq(&right),
        // GNU's interval code compares property values with EQ, so a range
        // propertized with ONE string object is a single run.  Emaxx string
        // clones share their backing store, preserving that identity; the
        // missing arm here fell to `false' and fragmented every
        // string-valued span per character (shr-zoom-image's
        // next-single-property-change saw a "change" at every char of the
        // alt text and replaced two characters of a twenty-char image).
        (Kind::StringObject(left), Kind::StringObject(right)) => left.ptr_eq(&right),
        (Kind::Cons(left), Kind::Cons(right)) => {
            crate::lisp::types::SharedCons::ptr_eq(&left, &right)
        }
        (Kind::Closure(left), Kind::Closure(right)) => left.ptr_eq(&right),
        (Kind::Buffer(left), Kind::Buffer(right)) => left.ptr_eq(&right),
        (Kind::Marker(left), Kind::Marker(right)) => left == right,
        (Kind::Overlay(left), Kind::Overlay(right)) => left.ptr_eq(&right),
        (Kind::CharTable(left), Kind::CharTable(right)) => left == right,
        (Kind::SubCharTable(left), Kind::SubCharTable(right)) => left == right,
        (Kind::Frame(left), Kind::Frame(right)) => left == right,
        (Kind::Terminal(left), Kind::Terminal(right)) => left.ptr_eq(&right),
        (Kind::Finalizer(left), Kind::Finalizer(right)) => left == right,
        (Kind::Record(left), Kind::Record(right)) => left.ptr_eq(&right),
        (Kind::LispRecord(left), Kind::LispRecord(right)) => left.ptr_eq(&right),
        (Kind::Unbound, Kind::Unbound) => true,
        _ => false,
    }
}

fn merge_adjacent_spans(mut spans: Vec<TextPropertySpan>) -> Vec<TextPropertySpan> {
    spans.retain(|span| span.start < span.end && !span.props.is_empty());
    spans.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<TextPropertySpan> = Vec::new();
    for span in spans {
        if let Some(last) = merged.last_mut() {
            debug_assert!(
                last.end <= span.start,
                "text property spans must be non-overlapping"
            );
            if last.end == span.start && text_property_plists_eq(&last.props, &span.props) {
                last.end = span.end;
                continue;
            }
        }
        merged.push(span);
    }
    merged
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
        // A rope of many chunks converted byte by byte, in and out of order
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
        assert_eq!(buf.substring_property_spans(4, 2).len(), 1);
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

        assert!(buf.text_properties_at_ref(0).is_empty());
        assert!(buf.text_properties_at_ref(1).is_empty());
        assert!(buf.text_properties_at_ref(buf.point_max()).is_empty());
        for index in 0..64 {
            let start = 2 + index * 5;
            let expected = [("slot".into(), Value::Integer(index as i64))];
            assert_eq!(buf.text_properties_at_ref(start), expected.as_slice());
            assert_eq!(buf.text_properties_at_ref(start + 1), expected.as_slice());
            assert!(buf.text_properties_at_ref(start + 2).is_empty());
            assert_eq!(buf.text_properties_at(start), expected);
            assert_eq!(
                buf.text_property_at(start, "slot"),
                Some(Value::Integer(index as i64))
            );
        }

        // Fill the first gap with the preceding span's properties.  The
        // mutation authority must merge the adjacent equal intervals while
        // retaining a sharp boundary at the differently-valued next span.
        buf.set_text_properties(4, 7, &[("slot".into(), Value::Integer(0))]);
        assert_eq!(buf.text_property_at(6, "slot"), Some(Value::Integer(0)));
        assert_eq!(buf.text_property_at(7, "slot"), Some(Value::Integer(1)));
        assert!(
            buf.text_properties
                .windows(2)
                .all(|spans| spans[0].end <= spans[1].start)
        );
        assert_eq!(
            (buf.text_properties[0].start, buf.text_properties[0].end),
            (2, 7)
        );
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
    fn rope_character_cache_tracks_text_mutation_and_swap() {
        let text = format!("{}λ{}", "a".repeat(1500), "b".repeat(1500));
        let mut buf = Buffer::from_text("cached", &text);
        assert_eq!(buf.char_at(1501), Some('λ'));
        assert!(buf.char_cache.borrow().valid);

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
    fn rope_position_cache_follows_edits_before_and_over_it() {
        // Conversions recorded since an edit stay right across the edits
        // after them: shifted by an edit before, dropped by one over them.
        let mut model: Vec<char> = "aé€bc\n".repeat(400).chars().collect();
        let mut text = BufferText::from_str(&model.iter().collect::<String>());
        assert!(matches!(text.store, TextStore::Rope(_)));
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
    fn small_texts_are_strings_and_grow_into_ropes_and_back() {
        // A text of up to SMALL_TEXT bytes is one string; past it a rope,
        // and a string again under half.  Each step answers as the text
        // it holds, multibyte characters and lines included.
        fn check(buf: &Buffer, expected: &str) {
            assert_eq!(buf.full_buffer_string(), expected);
            assert_eq!(buf.size_total(), expected.chars().count());
            assert_eq!(String::from(&buf.text_rope()), expected);
            // A small text's rope of one leaf comes with that leaf, its
            // whole text.
            let (rope, leaf) = buf.text_rope_and_leaf();
            match leaf {
                Some(leaf) => {
                    // SAFETY: the rope and its leaf were taken together.
                    assert_eq!(unsafe { leaf.chunk(&rope) }, (expected, 0));
                    assert!(matches!(buf.text.store, TextStore::Small(_)));
                }
                None => assert!(
                    matches!(buf.text.store, TextStore::Rope(_)) || rope.chunks().count() > 1
                ),
            }
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
        assert!(matches!(buf.text.store, TextStore::Small(_)));
        check(&buf, "λx\nab\n");

        let line = "abcλ\u{1F600}def\n";
        let mut expected = String::from("λx\nab\n");
        while expected.len() <= SMALL_TEXT {
            buf.goto_char(3);
            buf.insert(line);
            expected.insert_str(expected.char_indices().nth(2).expect("char").0, line);
            check(&buf, &expected);
        }
        assert!(matches!(buf.text.store, TextStore::Rope(_)));

        while expected.len() > SMALL_TEXT / 4 {
            let from = 3;
            let to = from + line.chars().count();
            assert_eq!(buf.delete_region(from, to).as_deref(), Ok(line));
            let start = expected.char_indices().nth(from - 1).expect("char").0;
            expected.replace_range(start..start + line.len(), "");
            check(&buf, &expected);
        }
        assert!(matches!(buf.text.store, TextStore::Small(_)));
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
        assert!(prompt.inherited_text_properties(2, None).is_empty());

        let mut adjacent = Buffer::from_text("adjacent", "ab");
        adjacent.set_text_properties(
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
        adjacent.set_text_properties(
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
        assert_eq!(
            adjacent.inherited_text_properties(2, None),
            vec![
                ("from-rear".into(), Value::Integer(1)),
                ("from-front".into(), Value::Integer(2)),
            ]
        );

        let defaults = Value::list([Value::cons(Value::Symbol("from-rear".into()), Value::T)]);
        assert_eq!(
            adjacent.inherited_text_properties(2, Some(&defaults)),
            vec![("from-front".into(), Value::Integer(2))]
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
        assert_eq!(string.props.len(), 1);
        assert_eq!((string.props[0].start, string.props[0].end), (0, 1));
        assert_eq!(string.props[0].props[0].1, Value::String("x".into()));
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
