#![allow(dead_code)]

use crate::lisp::types::Value;
use ropey::Rope;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::SystemTime;

/// Modification counter. Bumped on every edit, used to detect
/// whether a buffer has changed since some snapshot (e.g. last save).
pub type ModCount = i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileModTime {
    pub modified: SystemTime,
}

/// A single editing buffer.
///
/// Positions are 1-based to match Emacs semantics: position 1 is
/// before the first character, position (len+1) is after the last.
/// Internally we convert to 0-based char indices into the rope.
#[derive(Clone)]
pub struct Buffer {
    /// Human-visible name (e.g. "*scratch*" or "main.rs").
    pub name: String,

    /// Previous name, set by rename-buffer.
    pub last_name: Option<String>,

    /// The text.
    text: Rope,

    /// Current cursor position (1-based char offset).
    pt: usize,

    /// Mark position (1-based), or None if no mark set.
    mark: Option<usize>,

    /// True when the region between point and mark is active.
    mark_active: bool,

    /// Bumped on every modification.
    modiff: ModCount,

    /// Updated when buffer characters change, but not for metadata-only
    /// modifications such as `set-buffer-modified-p'.
    chars_modiff: ModCount,

    /// Value of modiff at last save.
    save_modiff: ModCount,

    /// Snapshot of buffer text when last marked unmodified.
    saved_text: String,

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

    /// Undo log. Each entry records enough to reverse one operation.
    undo_list: Vec<UndoEntry>,

    /// Stable Lisp view of the undo log.  GNU change-group handles retain a
    /// tail of `buffer-undo-list' and later compare/mutate that exact cons
    /// structure, so rebuilding an equal-looking list on every read is not
    /// sufficient.  Appending a native undo entry extends this view at the
    /// front while preserving the identity of its existing tail.
    undo_list_view: UndoListViewCache,

    /// When true, don't record undo entries.
    undo_disabled: bool,

    /// Overlays attached to this buffer.
    pub overlays: Vec<crate::overlay::Overlay>,

    /// Sparse text property spans over [start, end) buffer positions.
    text_properties: Vec<TextPropertySpan>,

    /// When true, suppress creation/kill buffer hooks for this buffer.
    pub inhibit_hooks: bool,

    /// Whether positions in this buffer are interpreted as multibyte character positions.
    multibyte: bool,
}

#[derive(Clone, Debug)]
pub enum UndoEntry {
    /// Inserted n chars starting at pos (1-based). To undo: delete them.
    Insert { pos: usize, len: usize },
    /// Deleted text that was at pos (1-based). To undo: re-insert it.
    Delete {
        pos: usize,
        text: String,
        props: Vec<TextPropertySpan>,
        markers: Vec<UndoMarker>,
    },
    /// A logical grouped change that should appear as a single Lisp undo entry.
    Combined {
        display: Value,
        entries: Vec<UndoEntry>,
    },
    /// A Lisp-visible undo entry we don't know how to replay.
    Opaque(Value),
    /// Boundary between undo groups.
    Boundary,
}

#[derive(Clone, Debug)]
struct UndoListView {
    value: Value,
    undo_len: usize,
    file_present: bool,
    has_file_marker: bool,
}

#[derive(Default)]
struct UndoListViewCache(RefCell<Option<UndoListView>>);

impl Clone for UndoListViewCache {
    fn clone(&self) -> Self {
        // A cloned buffer is an independent text/undo snapshot.  Rebuild its
        // view lazily so destructive Lisp mutation cannot cross buffer state.
        Self::default()
    }
}

fn undo_entry_lisp_value(entry: &UndoEntry) -> Value {
    match entry {
        // GNU records an insertion as (BEG . END).
        UndoEntry::Insert { pos, len } => Value::cons(
            Value::Integer(*pos as i64),
            Value::Integer((*pos + *len) as i64),
        ),
        UndoEntry::Delete { pos, text, .. } => {
            Value::cons(Value::String(text.clone()), Value::Integer(*pos as i64))
        }
        UndoEntry::Combined { display, .. } | UndoEntry::Opaque(display) => display.clone(),
        UndoEntry::Boundary => Value::Nil,
    }
}

fn undo_file_marker() -> Value {
    Value::list([
        Value::T,
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(0),
    ])
}

#[derive(Clone, Debug)]
pub struct UndoState {
    entries: Vec<UndoEntry>,
    disabled: bool,
    view: Option<UndoListView>,
}

#[derive(Clone, Debug)]
pub struct UndoMarker {
    pub id: u64,
    pub original_pos: usize,
    pub collapsed_pos: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TextPropertySpan {
    pub start: usize,
    pub end: usize,
    pub props: Vec<(String, Value)>,
}

/// Errors that buffer operations can produce.
#[derive(Debug, PartialEq)]
pub enum BufferError {
    BeginningOfBuffer,
    EndOfBuffer,
    ReadOnly, // placeholder for later
    InvalidPosition(usize),
    NoFurtherUndoInformation,
    UnrecognizedUndoEntry(String),
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferError::BeginningOfBuffer => write!(f, "Beginning of buffer"),
            BufferError::EndOfBuffer => write!(f, "End of buffer"),
            BufferError::ReadOnly => write!(f, "Buffer is read-only"),
            BufferError::InvalidPosition(p) => write!(f, "Invalid position: {}", p),
            BufferError::NoFurtherUndoInformation => write!(f, "No further undo information"),
            BufferError::UnrecognizedUndoEntry(entry) => {
                write!(f, "Unrecognized entry in undo list {}", entry)
            }
        }
    }
}

impl std::error::Error for BufferError {}

impl Buffer {
    /// Create an empty buffer with the given name.
    pub fn new(name: &str) -> Self {
        let text = Rope::new();
        Buffer {
            name: name.to_string(),
            last_name: None,
            text,
            pt: 1,
            mark: None,
            mark_active: false,
            modiff: 1,
            chars_modiff: 1,
            save_modiff: 1,
            saved_text: String::new(),
            forced_modified: false,
            autosaved: false,
            begv: 1,
            zv: 1, // empty buffer: zv = 1
            file: None,
            file_truename: None,
            visited_file_modtime: None,
            undo_list: Vec::new(),
            undo_list_view: UndoListViewCache::default(),
            undo_disabled: false,
            overlays: Vec::new(),
            text_properties: Vec::new(),
            inhibit_hooks: false,
            multibyte: true,
        }
    }

    /// Create a buffer from a string (useful for tests and loading files).
    pub fn from_text(name: &str, s: &str) -> Self {
        let text = Rope::from_str(s);
        let len = text.len_chars();
        Buffer {
            name: name.to_string(),
            last_name: None,
            text,
            pt: 1,
            mark: None,
            mark_active: false,
            modiff: 1,
            chars_modiff: 1,
            save_modiff: 1,
            saved_text: s.to_string(),
            forced_modified: false,
            autosaved: false,
            begv: 1,
            zv: len + 1,
            file: None,
            file_truename: None,
            visited_file_modtime: None,
            undo_list: Vec::new(),
            undo_list_view: UndoListViewCache::default(),
            undo_disabled: false,
            overlays: Vec::new(),
            text_properties: Vec::new(),
            inhibit_hooks: false,
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

    pub fn set_multibyte(&mut self, enabled: bool) {
        self.multibyte = enabled;
    }

    /// Replace the internal character view while preserving the underlying
    /// byte sequence.  POSITION_MAP contains the new 1-based position for
    /// every old character boundary, including point-max.
    pub fn set_multibyte_representation(
        &mut self,
        enabled: bool,
        text: String,
        saved_text: String,
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
        self.mark = self.mark.map(remap);
        // Overlay endpoints already use the rope's character coordinates.
        // The unibyte overlay API translates those coordinates at its public
        // boundary, so remapping them here would apply the byte-to-character
        // conversion twice.
        for span in &mut self.text_properties {
            span.start = remap(span.start);
            span.end = remap(span.end);
        }

        self.text = Rope::from_str(&text);
        self.saved_text = saved_text;
        self.multibyte = enabled;
        self.clear_undo_history();
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
        let start = self.pt;
        let end = limit.clamp(start, self.zv);
        let count = self
            .text
            .slice(start - 1..end - 1)
            .chars()
            .take_while(|ch| predicate(*ch))
            .count();
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
        let slice = self.text.slice(end - 1..start - 1);
        let count = slice
            .chars_at(slice.len_chars())
            .reversed()
            .take_while(|ch| predicate(*ch))
            .count();
        self.pt -= count;
        count
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
            .text
            .slice(..char_index)
            .chars_at(char_index)
            .reversed()
            .position(|ch| ch == '\n')
            .map_or(1, |distance| char_index - distance + 1);
        line_start.max(self.begv)
    }

    /// Move to the end of the current line. Returns new point.
    pub fn end_of_line(&mut self) -> usize {
        if self.pt == self.zv {
            return self.pt;
        }
        let idx0 = self.pt - 1; // 0-based
        // Search forward for newline
        for (i, ch) in self.text.chars_at(idx0).enumerate() {
            if ch == '\n' {
                let result = idx0 + i + 1; // 1-based position of the newline char
                self.pt = result.min(self.zv);
                return self.pt;
            }
        }
        // No newline, go to end
        self.pt = self.zv;
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
                // Find next newline from current point
                let idx0 = self.pt - 1;
                let mut found = false;
                for (i, ch) in self.text.chars_at(idx0).enumerate() {
                    if idx0 + i + 1 > self.zv - 1 {
                        // hit end of accessible region
                        break;
                    }
                    if ch == '\n' {
                        self.pt = idx0 + i + 1 + 1; // position after the newline, 1-based
                        self.pt = self.pt.min(self.zv);
                        found = true;
                        break;
                    }
                }
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

    pub fn mark(&self) -> Option<usize> {
        self.mark
    }

    pub fn mark_active(&self) -> bool {
        self.mark_active
    }

    pub fn set_mark(&mut self, pos: usize) {
        self.mark = Some(pos.clamp(self.begv, self.zv));
        self.mark_active = true;
    }

    pub fn set_mark_active(&mut self, active: bool) {
        self.mark_active = active && self.mark.is_some();
    }

    pub fn clear_mark(&mut self) {
        self.mark = None;
        self.mark_active = false;
    }

    pub fn deactivate_mark(&mut self) {
        self.mark_active = false;
    }

    pub fn region(&self) -> Option<(usize, usize)> {
        self.mark.map(|m| {
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
        self.text.slice(start..end).to_string()
    }

    pub fn full_buffer_string(&self) -> String {
        self.text.to_string()
    }

    pub fn position_bytes(&self, pos: usize) -> Option<usize> {
        let char_len = self.text.len_chars();
        if pos == 0 || pos > char_len + 1 {
            return None;
        }
        Some(
            1 + self
                .text
                .slice(..(pos - 1))
                .chars()
                .map(char::len_utf8)
                .sum::<usize>(),
        )
    }

    pub fn byte_to_position(&self, byte: usize) -> Option<usize> {
        if byte == 0 {
            return None;
        }
        let total_bytes = self.text.len_bytes();
        if byte > total_bytes + 1 {
            return None;
        }
        if byte == total_bytes + 1 {
            return Some(self.text.len_chars() + 1);
        }
        let mut current_byte = 1usize;
        for (index, ch) in self.text.chars().enumerate() {
            let next = current_byte + ch.len_utf8();
            if byte == current_byte || byte < next {
                return Some(index + 1);
            }
            current_byte = next;
        }
        Some(self.text.len_chars() + 1)
    }

    /// Get a substring. Positions are 1-based, range is [from, to).
    pub fn buffer_substring(&self, from: usize, to: usize) -> Result<String, BufferError> {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from > to {
            return Err(BufferError::InvalidPosition(from));
        }
        Ok(self.text.slice((from - 1)..(to - 1)).to_string())
    }

    pub fn substring_property_spans(&self, from: usize, to: usize) -> Vec<TextPropertySpan> {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from >= to {
            return Vec::new();
        }
        let mut spans = Vec::new();
        for span in &self.text_properties {
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

    pub fn has_text_property_named(&self, property: &str) -> bool {
        self.text_properties
            .iter()
            .any(|span| span.props.iter().any(|(name, _)| name == property))
    }

    pub fn text_property_at(&self, pos: usize, prop: &str) -> Option<Value> {
        if pos < self.point_min() || pos >= self.point_max() {
            return None;
        }
        self.text_properties
            .iter()
            .find(|span| span.start <= pos && pos < span.end)
            .and_then(|span| {
                span.props
                    .iter()
                    .find(|(name, _)| name == prop)
                    .map(|(_, value)| value.clone())
            })
    }

    pub fn text_properties_at(&self, pos: usize) -> Vec<(String, Value)> {
        if pos < self.point_min() || pos >= self.point_max() {
            return Vec::new();
        }
        self.text_properties
            .iter()
            .find(|span| span.start <= pos && pos < span.end)
            .map(|span| span.props.clone())
            .unwrap_or_default()
    }

    pub fn add_text_properties(&mut self, start: usize, end: usize, props: &[(String, Value)]) {
        self.modify_text_properties(start, end, |mut current| {
            // GNU replaces existing properties in place and CONSES new
            // ones onto the front of the interval plist, so
            // `text-properties-at' lists later additions first.
            for (name, value) in props {
                if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == name) {
                    *existing = value.clone();
                } else {
                    current.insert(0, (name.clone(), value.clone()));
                }
            }
            current
        });
    }

    pub fn put_text_property(&mut self, start: usize, end: usize, name: &str, value: Value) {
        self.modify_text_properties(start, end, |mut current| {
            if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == name) {
                *existing = value.clone();
            } else {
                current.insert(0, (name.to_string(), value.clone()));
            }
            current
        });
    }

    pub fn set_text_properties(&mut self, start: usize, end: usize, props: &[(String, Value)]) {
        self.modify_text_properties(start, end, |_| props.to_vec());
    }

    pub fn remove_list_of_text_properties(&mut self, start: usize, end: usize, names: &[String]) {
        self.modify_text_properties(start, end, |current| {
            current
                .into_iter()
                .filter(|(key, _)| !names.iter().any(|name| name == key))
                .collect()
        });
    }

    /// Character at position (1-based). None if out of range.
    pub fn char_at(&self, pos: usize) -> Option<char> {
        if pos < self.begv || pos >= self.zv {
            return None;
        }
        Some(self.text.char(pos - 1))
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
    ) -> Vec<(String, Value)> {
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
        props: Option<Vec<(String, Value)>>,
    ) -> usize {
        let nchars = s.chars().count();
        if nchars == 0 {
            return self.pt;
        }

        let insert_at = self.pt;
        let idx0 = self.pt - 1; // 0-based
        self.text.insert(idx0, s);

        // Record undo
        if !self.undo_disabled {
            self.push_undo_entry(UndoEntry::Insert {
                pos: insert_at,
                len: nchars,
            });
        }

        // Advance point past insertion
        self.pt += nchars;

        // Adjust zv (the buffer grew)
        self.zv += nchars;

        // Adjust mark if it's at or after the insertion point
        if let Some(ref mut m) = self.mark
            && *m >= self.pt - nchars
        {
            *m += nchars;
        }

        // Adjust overlays
        crate::overlay::adjust_for_insert(&mut self.overlays, insert_at, nchars);

        self.adjust_text_properties_for_insert(insert_at, nchars);
        if let Some(props) = props
            && !props.is_empty()
        {
            // Fresh text: keep the given plist order (GNU grafts intervals).
            self.set_text_properties(insert_at, insert_at + nchars, &props);
        }

        self.modiff += 1;
        self.chars_modiff = self.modiff;
        self.autosaved = false;
        self.pt
    }

    /// Replace [from, to) with TEXT of the same character length, in place:
    /// text properties and markers are untouched (GNU subst-char-in-region
    /// substitutes characters without disturbing intervals).
    pub fn replace_region_in_place(&mut self, from: usize, to: usize, text: &str, noundo: bool) {
        let from0 = from - 1;
        let to0 = to - 1;
        if !self.undo_disabled && !noundo {
            let old: String = self.text.slice(from0..to0).to_string();
            let props = self.substring_property_spans(from, to);
            self.push_undo_entry(UndoEntry::Delete {
                pos: from,
                text: old,
                props,
                markers: Vec::new(),
            });
            self.push_undo_entry(UndoEntry::Insert {
                pos: from,
                len: text.chars().count(),
            });
        }
        self.text.remove(from0..to0);
        self.text.insert(from0, text);
        self.modiff += 1;
        self.chars_modiff = self.modiff;
        self.autosaved = false;
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
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from >= to {
            return Ok(String::new());
        }

        let from0 = from - 1;
        let to0 = to - 1;

        // Grab text for undo before deleting
        let deleted: String = self.text.slice(from0..to0).to_string();
        let deleted_props = self.substring_property_spans(from, to);
        let nchars = to - from;

        if !self.undo_disabled {
            self.push_undo_entry(UndoEntry::Delete {
                pos: from,
                text: deleted.clone(),
                props: deleted_props,
                markers: Vec::new(),
            });
        }

        self.text.remove(from0..to0);

        // Adjust point
        if self.pt > to {
            self.pt -= nchars;
        } else if self.pt > from {
            self.pt = from;
        }

        // Adjust mark
        if let Some(ref mut m) = self.mark {
            if *m > to {
                *m -= nchars;
            } else if *m > from {
                *m = from;
            }
        }

        // Adjust overlays and evaporate empty ones
        crate::overlay::adjust_for_delete(&mut self.overlays, from, to);
        crate::overlay::evaporate(&mut self.overlays);
        self.adjust_text_properties_for_delete(from, to);

        // Shrink accessible region
        self.zv -= nchars;

        self.modiff += 1;
        self.chars_modiff = self.modiff;
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

    // ── Undo ──

    pub fn undo(&mut self) -> Result<(), BufferError> {
        let group = self.take_undo_group(None)?;
        self.push_undo_boundary();
        for entry in group.iter().rev() {
            self.apply_undo_entry(entry)?;
        }

        if !self.is_modified() {
            self.autosaved = false;
        }
        Ok(())
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
        if let Some(mark) = &mut self.mark {
            *mark = (*mark).clamp(self.begv, self.zv);
        }
    }

    // ── Modification state ──

    pub fn is_modified(&self) -> bool {
        self.forced_modified || self.modiff != self.save_modiff
    }

    pub fn set_unmodified(&mut self) {
        self.save_modiff = self.modiff;
        self.saved_text = self.full_buffer_string();
        self.forced_modified = false;
        self.autosaved = false;
    }

    pub fn saved_text(&self) -> &str {
        &self.saved_text
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

    pub fn enable_undo(&mut self) {
        if self.undo_disabled {
            self.invalidate_undo_list_view();
        }
        self.undo_disabled = false;
    }

    pub fn undo_enabled(&self) -> bool {
        !self.undo_disabled
    }

    pub fn disable_undo(&mut self) {
        self.undo_disabled = true;
        self.undo_list.clear();
        self.invalidate_undo_list_view();
    }

    pub fn undo_entries(&self) -> &[UndoEntry] {
        &self.undo_list
    }

    pub fn undo_groups(&self) -> Vec<Vec<UndoEntry>> {
        self.collect_undo_groups()
    }

    pub fn undo_list_value(&self) -> Value {
        if self.undo_disabled {
            return Value::T;
        }

        let has_file_marker = self.file.is_some()
            && self
                .undo_list
                .iter()
                .any(|entry| matches!(entry, UndoEntry::Insert { .. } | UndoEntry::Delete { .. }));
        if let Some(view) = self.undo_list_view.0.borrow().as_ref()
            && view.undo_len == self.undo_list.len()
            && view.file_present == self.file.is_some()
            && view.has_file_marker == has_file_marker
        {
            return view.value.clone();
        }

        let mut entries = self
            .undo_list
            .iter()
            .rev()
            .map(undo_entry_lisp_value)
            .collect::<Vec<_>>();
        if has_file_marker {
            entries.push(undo_file_marker());
        }
        let value = Value::list(entries);
        *self.undo_list_view.0.borrow_mut() = Some(UndoListView {
            value: value.clone(),
            undo_len: self.undo_list.len(),
            file_present: self.file.is_some(),
            has_file_marker,
        });
        value
    }

    pub fn invalidate_undo_list_view(&self) {
        *self.undo_list_view.0.borrow_mut() = None;
    }

    /// Preserve the exact Lisp object assigned to `buffer-undo-list` after
    /// synchronizing its entries into the native undo representation.
    ///
    /// GNU exposes the actual undo-list conses: Lisp such as CC Mode retains
    /// a tail and later uses `eq` to detect when cleanup has reached it.  An
    /// equal-looking rebuilt list is therefore observably different and can
    /// turn a bounded undo loop into an infinite one.
    pub(crate) fn set_undo_list_view(&self, value: Value) {
        let has_file_marker = self.file.is_some()
            && self
                .undo_list
                .iter()
                .any(|entry| matches!(entry, UndoEntry::Insert { .. } | UndoEntry::Delete { .. }));
        *self.undo_list_view.0.borrow_mut() = Some(UndoListView {
            value,
            undo_len: self.undo_list.len(),
            file_present: self.file.is_some(),
            has_file_marker,
        });
    }

    pub fn push_undo_entry(&mut self, entry: UndoEntry) {
        let entry_is_text = matches!(entry, UndoEntry::Insert { .. } | UndoEntry::Delete { .. });
        let entry_value = undo_entry_lisp_value(&entry);
        self.undo_list.push(entry);
        let file_present = self.file.is_some();
        let mut view = self.undo_list_view.0.borrow_mut();
        if let Some(view) = view.as_mut() {
            let has_file_marker = file_present && (view.has_file_marker || entry_is_text);
            if view.undo_len + 1 == self.undo_list.len()
                && view.file_present == file_present
                && view.has_file_marker == has_file_marker
            {
                view.value = Value::cons(entry_value, view.value.clone());
                view.undo_len += 1;
                return;
            }
        }
        *view = None;
    }

    pub fn undo_len(&self) -> usize {
        self.undo_list.len()
    }

    pub fn take_undo_entries_since(&mut self, start: usize) -> Vec<UndoEntry> {
        let entries = self.undo_list.split_off(start);
        self.invalidate_undo_list_view();
        entries
    }

    pub fn attach_markers_to_last_delete(&mut self, markers: Vec<UndoMarker>) {
        if markers.is_empty() {
            return;
        }
        if let Some(UndoEntry::Delete {
            markers: delete_markers,
            ..
        }) = self.undo_list.last_mut()
        {
            delete_markers.extend(markers);
        }
    }

    pub fn push_undo_boundary(&mut self) {
        if !matches!(self.undo_list.last(), Some(UndoEntry::Boundary)) {
            self.push_undo_entry(UndoEntry::Boundary);
        }
    }

    pub fn clear_undo_history(&mut self) {
        self.undo_list.clear();
        self.invalidate_undo_list_view();
    }

    pub fn take_undo_state(&mut self) -> UndoState {
        let view = self.undo_list_view.0.borrow_mut().take();
        UndoState {
            entries: std::mem::take(&mut self.undo_list),
            disabled: std::mem::replace(&mut self.undo_disabled, false),
            view,
        }
    }

    pub fn restore_undo_state(&mut self, state: UndoState) {
        self.undo_list = state.entries;
        self.undo_disabled = state.disabled;
        *self.undo_list_view.0.borrow_mut() = state.view;
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

    pub fn swap_text_state(&mut self, other: &mut Buffer) {
        std::mem::swap(&mut self.text, &mut other.text);
        std::mem::swap(&mut self.pt, &mut other.pt);
        std::mem::swap(&mut self.mark, &mut other.mark);
        std::mem::swap(&mut self.mark_active, &mut other.mark_active);
        std::mem::swap(&mut self.modiff, &mut other.modiff);
        std::mem::swap(&mut self.chars_modiff, &mut other.chars_modiff);
        std::mem::swap(&mut self.save_modiff, &mut other.save_modiff);
        std::mem::swap(&mut self.saved_text, &mut other.saved_text);
        std::mem::swap(&mut self.forced_modified, &mut other.forced_modified);
        std::mem::swap(&mut self.autosaved, &mut other.autosaved);
        std::mem::swap(&mut self.begv, &mut other.begv);
        std::mem::swap(&mut self.zv, &mut other.zv);
        std::mem::swap(&mut self.undo_list, &mut other.undo_list);
        std::mem::swap(&mut self.undo_disabled, &mut other.undo_disabled);
        std::mem::swap(&mut self.overlays, &mut other.overlays);
        std::mem::swap(&mut self.text_properties, &mut other.text_properties);
        std::mem::swap(&mut self.multibyte, &mut other.multibyte);
    }

    // ── Line/column helpers ──

    /// Return the line number (1-based) at the given position (1-based).
    pub fn line_number_at_pos(&self, pos: usize) -> usize {
        let pos0 = (pos - 1).min(self.text.len_chars());
        self.text.char_to_line(pos0) + 1
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

    fn adjust_text_properties_for_insert(&mut self, pos: usize, nchars: usize) {
        if nchars == 0 {
            return;
        }
        let mut updated = Vec::new();
        for span in self.text_properties.clone() {
            if span.end <= pos {
                updated.push(span);
            } else if span.start >= pos {
                updated.push(TextPropertySpan {
                    start: span.start + nchars,
                    end: span.end + nchars,
                    props: span.props,
                });
            } else {
                updated.push(TextPropertySpan {
                    start: span.start,
                    end: pos,
                    props: span.props.clone(),
                });
                updated.push(TextPropertySpan {
                    start: pos + nchars,
                    end: span.end + nchars,
                    props: span.props,
                });
            }
        }
        self.text_properties = merge_adjacent_spans(updated);
    }

    fn adjust_text_properties_for_delete(&mut self, from: usize, to: usize) {
        if from >= to {
            return;
        }
        let removed = to - from;
        let mut updated = Vec::new();
        for span in self.text_properties.clone() {
            if span.end <= from {
                updated.push(span);
            } else if span.start >= to {
                updated.push(TextPropertySpan {
                    start: span.start - removed,
                    end: span.end - removed,
                    props: span.props,
                });
            } else {
                if span.start < from {
                    updated.push(TextPropertySpan {
                        start: span.start,
                        end: from,
                        props: span.props.clone(),
                    });
                }
                if span.end > to {
                    updated.push(TextPropertySpan {
                        start: from,
                        end: span.end - removed,
                        props: span.props,
                    });
                }
            }
        }
        self.text_properties = merge_adjacent_spans(updated);
    }

    fn modify_text_properties<F>(&mut self, start: usize, end: usize, mut f: F)
    where
        F: FnMut(Vec<(String, Value)>) -> Vec<(String, Value)>,
    {
        let start = start.max(self.point_min());
        let end = end.min(self.point_max());
        if start >= end {
            return;
        }

        let original = self.text_properties.clone();
        let mut updated = Vec::new();
        for span in &original {
            if span.end <= start || span.start >= end {
                updated.push(span.clone());
            } else {
                if span.start < start {
                    updated.push(TextPropertySpan {
                        start: span.start,
                        end: start,
                        props: span.props.clone(),
                    });
                }
                if span.end > end {
                    updated.push(TextPropertySpan {
                        start: end,
                        end: span.end,
                        props: span.props.clone(),
                    });
                }
            }
        }

        let mut boundaries = vec![start, end];
        for span in &original {
            if span.end <= start || span.start >= end {
                continue;
            }
            boundaries.push(span.start.max(start));
            boundaries.push(span.end.min(end));
        }
        boundaries.sort_unstable();
        boundaries.dedup();

        for window in boundaries.windows(2) {
            let seg_start = window[0];
            let seg_end = window[1];
            if seg_start >= seg_end {
                continue;
            }
            let current = properties_at_from(&original, seg_start);
            let next = f(current);
            if !next.is_empty() {
                updated.push(TextPropertySpan {
                    start: seg_start,
                    end: seg_end,
                    props: next,
                });
            }
        }

        self.text_properties = merge_adjacent_spans(updated);
    }
}

impl Buffer {
    pub fn take_undo_group(
        &mut self,
        region: Option<(usize, usize)>,
    ) -> Result<Vec<UndoEntry>, BufferError> {
        self.take_undo_group_with_skip(region, 0)
    }

    pub fn take_undo_group_with_skip(
        &mut self,
        region: Option<(usize, usize)>,
        skip_newest_groups: usize,
    ) -> Result<Vec<UndoEntry>, BufferError> {
        if region.is_none() || !self.mark_active {
            if skip_newest_groups == 0 {
                return self.pop_latest_undo_group();
            }
            return self.pop_undo_group_skipping(skip_newest_groups);
        }
        let mut groups = self.collect_undo_groups();
        if groups.is_empty() {
            return Err(BufferError::NoFurtherUndoInformation);
        }

        let selected = region
            .and_then(|region| self.select_undo_group_for_region(&groups, region))
            .unwrap_or(groups.len() - 1);
        let group = map_group_through_newer(&groups[selected], &groups[selected + 1..]);
        groups.remove(selected);
        self.restore_undo_groups(&groups);
        if group.is_empty() {
            return Err(BufferError::NoFurtherUndoInformation);
        }
        Ok(group)
    }

    fn pop_latest_undo_group(&mut self) -> Result<Vec<UndoEntry>, BufferError> {
        self.invalidate_undo_list_view();
        while matches!(self.undo_list.last(), Some(UndoEntry::Boundary)) {
            self.undo_list.pop();
        }
        if self.undo_list.is_empty() {
            return Err(BufferError::NoFurtherUndoInformation);
        }

        let mut group = Vec::new();
        while let Some(entry) = self.undo_list.pop() {
            match entry {
                UndoEntry::Boundary => break,
                other => group.push(other),
            }
        }
        group.reverse();
        if group.is_empty() {
            return Err(BufferError::NoFurtherUndoInformation);
        }
        Ok(group)
    }

    fn pop_undo_group_skipping(
        &mut self,
        skip_newest_groups: usize,
    ) -> Result<Vec<UndoEntry>, BufferError> {
        self.invalidate_undo_list_view();
        let mut end = self.undo_list.len();
        while end > 0 && matches!(self.undo_list[end - 1], UndoEntry::Boundary) {
            end -= 1;
        }
        if end == 0 {
            return Err(BufferError::NoFurtherUndoInformation);
        }

        for _ in 0..skip_newest_groups {
            while end > 0 && !matches!(self.undo_list[end - 1], UndoEntry::Boundary) {
                end -= 1;
            }
            while end > 0 && matches!(self.undo_list[end - 1], UndoEntry::Boundary) {
                end -= 1;
            }
            if end == 0 {
                return Err(BufferError::NoFurtherUndoInformation);
            }
        }

        let mut start = end;
        while start > 0 && !matches!(self.undo_list[start - 1], UndoEntry::Boundary) {
            start -= 1;
        }
        let group = self.undo_list[start..end].to_vec();
        self.undo_list.drain(start..end);
        while self
            .undo_list
            .get(start)
            .is_some_and(|entry| matches!(entry, UndoEntry::Boundary))
            && start > 0
            && matches!(self.undo_list[start - 1], UndoEntry::Boundary)
        {
            self.undo_list.remove(start);
        }
        if group.is_empty() {
            return Err(BufferError::NoFurtherUndoInformation);
        }
        Ok(group)
    }

    fn collect_undo_groups(&self) -> Vec<Vec<UndoEntry>> {
        let mut groups = Vec::new();
        let mut current = Vec::new();
        for entry in &self.undo_list {
            match entry {
                UndoEntry::Boundary => {
                    if !current.is_empty() {
                        groups.push(std::mem::take(&mut current));
                    }
                }
                other => current.push(other.clone()),
            }
        }
        if !current.is_empty() {
            groups.push(current);
        }
        groups
    }

    fn restore_undo_groups(&mut self, groups: &[Vec<UndoEntry>]) {
        self.invalidate_undo_list_view();
        self.undo_list.clear();
        for (index, group) in groups.iter().enumerate() {
            self.undo_list.extend(group.iter().cloned());
            if index + 1 < groups.len() {
                self.undo_list.push(UndoEntry::Boundary);
            }
        }
    }

    fn select_undo_group_for_region(
        &self,
        groups: &[Vec<UndoEntry>],
        region: (usize, usize),
    ) -> Option<usize> {
        for index in (0..groups.len()).rev() {
            if group_intersects_region(&groups[index], &groups[index + 1..], region) {
                return Some(index);
            }
        }
        None
    }

    fn apply_undo_entry(&mut self, entry: &UndoEntry) -> Result<(), BufferError> {
        match entry {
            UndoEntry::Insert { pos, len } => {
                self.goto_char(*pos);
                self.delete_region(*pos, *pos + *len)?;
                Ok(())
            }
            UndoEntry::Delete {
                pos, text, props, ..
            } => {
                self.goto_char(*pos);
                let insert_at = self.point();
                self.insert(text);
                for span in props {
                    self.add_text_properties(
                        insert_at + span.start,
                        insert_at + span.end,
                        &span.props,
                    );
                }
                Ok(())
            }
            UndoEntry::Combined { entries, .. } => {
                for inner in entries.iter().rev() {
                    self.apply_undo_entry(inner)?;
                }
                Ok(())
            }
            UndoEntry::Opaque(value) => Err(BufferError::UnrecognizedUndoEntry(format!("{value}"))),
            UndoEntry::Boundary => Ok(()),
        }
    }
}

fn group_intersects_region(
    group: &[UndoEntry],
    newer_groups: &[Vec<UndoEntry>],
    region: (usize, usize),
) -> bool {
    let (region_start, region_end) = region;
    for (mut start, mut end) in leaf_ranges(group) {
        for later_group in newer_groups {
            for entry in later_group {
                start = map_position_through_entry(start, entry, false);
                end = map_position_through_entry(end, entry, false);
            }
        }
        let lower = start.min(end);
        let upper = start.max(end);
        if lower == upper {
            if region_start <= lower && lower <= region_end {
                return true;
            }
        } else if lower >= region_start && upper <= region_end {
            return true;
        }
    }
    false
}

fn leaf_ranges(entries: &[UndoEntry]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    for entry in entries {
        match entry {
            UndoEntry::Insert { pos, len } => ranges.push((*pos, *pos + *len)),
            // Re-inserting deleted text affects the current buffer at a point,
            // not across the deleted span that no longer exists.
            UndoEntry::Delete { pos, .. } => ranges.push((*pos, *pos)),
            UndoEntry::Combined { entries, .. } => ranges.extend(leaf_ranges(entries)),
            UndoEntry::Opaque(_) | UndoEntry::Boundary => {}
        }
    }
    ranges
}

fn map_position_through_entry(position: usize, entry: &UndoEntry, is_end: bool) -> usize {
    match entry {
        UndoEntry::Insert { pos, len } => {
            if position > *pos || (is_end && position == *pos) {
                position + *len
            } else {
                position
            }
        }
        UndoEntry::Delete { pos, text, .. } => {
            let deleted = text.chars().count();
            let to = *pos + deleted;
            if position > to {
                position - deleted
            } else if position > *pos {
                *pos
            } else {
                position
            }
        }
        UndoEntry::Combined { entries, .. } => entries.iter().fold(position, |mapped, inner| {
            map_position_through_entry(mapped, inner, is_end)
        }),
        UndoEntry::Opaque(_) | UndoEntry::Boundary => position,
    }
}

fn map_group_through_newer(group: &[UndoEntry], newer_groups: &[Vec<UndoEntry>]) -> Vec<UndoEntry> {
    group
        .iter()
        .map(|entry| map_undo_entry_through_newer(entry, newer_groups))
        .collect()
}

fn map_undo_entry_through_newer(entry: &UndoEntry, newer_groups: &[Vec<UndoEntry>]) -> UndoEntry {
    match entry {
        UndoEntry::Insert { pos, len } => UndoEntry::Insert {
            pos: map_position_through_groups(*pos, newer_groups, false),
            len: *len,
        },
        UndoEntry::Delete {
            pos,
            text,
            props,
            markers,
        } => UndoEntry::Delete {
            pos: map_position_through_groups(*pos, newer_groups, false),
            text: text.clone(),
            props: props.clone(),
            markers: markers.clone(),
        },
        UndoEntry::Combined { display, entries } => UndoEntry::Combined {
            display: display.clone(),
            entries: map_group_through_newer(entries, newer_groups),
        },
        UndoEntry::Opaque(value) => UndoEntry::Opaque(value.clone()),
        UndoEntry::Boundary => UndoEntry::Boundary,
    }
}

fn map_position_through_groups(
    position: usize,
    newer_groups: &[Vec<UndoEntry>],
    is_end: bool,
) -> usize {
    newer_groups.iter().fold(position, |mapped, group| {
        group.iter().fold(mapped, |inner, entry| {
            map_position_through_entry(inner, entry, is_end)
        })
    })
}

fn properties_at_from(spans: &[TextPropertySpan], pos: usize) -> Vec<(String, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

fn property_value<'a>(props: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    props
        .iter()
        .find(|(property, _)| property == name)
        .map(|(_, value)| value)
}

fn is_stickiness_control(name: &str) -> bool {
    matches!(name, "front-sticky" | "rear-nonsticky")
}

fn property_named_by_stickiness(setting: Option<&Value>, name: &str) -> bool {
    match setting {
        Some(Value::T) => true,
        Some(value @ Value::Cons(_, _)) => value.to_vec().is_ok_and(|items| {
            items
                .iter()
                .any(|item| matches!(item, Value::Symbol(property) if property == name))
        }),
        _ => false,
    }
}

fn default_property_nonsticky(defaults: Option<&Value>, name: &str) -> bool {
    let Some(defaults) = defaults.and_then(|value| value.to_vec().ok()) else {
        return false;
    };
    defaults.iter().any(|entry| {
        let Value::Cons(property, nonsticky) = entry else {
            return false;
        };
        matches!(&*property.borrow(), Value::Symbol(candidate) if candidate == name)
            && nonsticky.borrow().is_truthy()
    })
}

pub(crate) fn text_property_plists_eq(left: &[(String, Value)], right: &[(String, Value)]) -> bool {
    left.len() == right.len()
        && left.iter().all(|(name, value)| {
            right
                .iter()
                .find(|(candidate, _)| candidate == name)
                .is_some_and(|(_, candidate)| text_property_values_eq(value, candidate))
        })
}

pub(crate) fn text_property_values_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(left), Value::Integer(right)) => left == right,
        (Value::BigInteger(left), Value::BigInteger(right)) => left == right,
        (Value::Float(left), Value::Float(right)) => left == right,
        (Value::Symbol(left), Value::Symbol(right))
        | (Value::BuiltinFunc(left), Value::BuiltinFunc(right)) => left == right,
        (Value::StringObject(left), Value::StringObject(right)) => Rc::ptr_eq(left, right),
        (Value::Cons(left_car, left_cdr), Value::Cons(right_car, right_cdr)) => {
            Rc::ptr_eq(left_car, right_car) && Rc::ptr_eq(left_cdr, right_cdr)
        }
        (
            Value::Lambda(left_params, left_body, left_env),
            Value::Lambda(right_params, right_body, right_env),
        ) => {
            left_params == right_params
                && left_body == right_body
                && Rc::ptr_eq(left_env, right_env)
        }
        (Value::Buffer(left, _), Value::Buffer(right, _))
        | (Value::Marker(left), Value::Marker(right))
        | (Value::Overlay(left), Value::Overlay(right))
        | (Value::CharTable(left), Value::CharTable(right))
        | (Value::Frame(left), Value::Frame(right))
        | (Value::Terminal(left), Value::Terminal(right))
        | (Value::Record(left), Value::Record(right))
        | (Value::Finalizer(left), Value::Finalizer(right)) => left == right,
        (Value::Unbound, Value::Unbound) => true,
        _ => false,
    }
}

fn merge_adjacent_spans(mut spans: Vec<TextPropertySpan>) -> Vec<TextPropertySpan> {
    spans.retain(|span| span.start < span.end && !span.props.is_empty());
    spans.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<TextPropertySpan> = Vec::new();
    for span in spans {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && text_property_plists_eq(&last.props, &span.props)
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
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

    #[test]
    fn undo_insert() {
        let mut buf = Buffer::new("test");
        buf.insert("hello");
        assert_eq!(buf.buffer_string(), "hello");
        buf.undo().unwrap();
        assert_eq!(buf.buffer_string(), "");
    }

    #[test]
    fn undo_delete() {
        let mut buf = Buffer::from_text("test", "abc");
        buf.goto_char(2);
        buf.delete_char(1).unwrap();
        assert_eq!(buf.buffer_string(), "ac");
        buf.undo().unwrap();
        assert_eq!(buf.buffer_string(), "abc");
    }

    #[test]
    fn undo_delete_restores_text_properties() {
        let mut buf = Buffer::from_text("test", "abc");
        buf.add_text_properties(2, 3, &[("markup".into(), Value::String("x".into()))]);
        buf.goto_char(2);
        buf.delete_char(1).unwrap();
        assert_eq!(buf.buffer_string(), "ac");
        buf.undo().unwrap();
        assert_eq!(buf.buffer_string(), "abc");
        assert_eq!(
            buf.text_property_at(2, "markup"),
            Some(Value::String("x".into()))
        );
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
