#![allow(dead_code)]

use ropey::Rope;

/// Modification counter. Bumped on every edit, used to detect
/// whether a buffer has changed since some snapshot (e.g. last save).
pub type ModCount = u64;

/// A single editing buffer.
///
/// Positions are 1-based to match Emacs semantics: position 1 is
/// before the first character, position (len+1) is after the last.
/// Internally we convert to 0-based char indices into the rope.
pub struct Buffer {
    /// Human-visible name (e.g. "*scratch*" or "main.rs").
    pub name: String,

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

    /// Value of modiff at last save.
    save_modiff: ModCount,

    /// Narrowing: accessible region [begv, zv] (1-based, inclusive of begv,
    /// exclusive of zv in the sense that zv is one past the last accessible char).
    begv: usize,
    zv: usize,

    /// Path to the visited file, if any.
    pub file: Option<String>,

    /// Undo log. Each entry records enough to reverse one operation.
    undo_list: Vec<UndoEntry>,

    /// When true, don't record undo entries.
    undo_disabled: bool,
}

#[derive(Clone, Debug)]
pub enum UndoEntry {
    /// Inserted n chars starting at pos (1-based). To undo: delete them.
    Insert { pos: usize, len: usize },
    /// Deleted text that was at pos (1-based). To undo: re-insert it.
    Delete { pos: usize, text: String },
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
        let text = Rope::new();
        Buffer {
            name: name.to_string(),
            text,
            pt: 1,
            mark: None,
            mark_active: false,
            modiff: 0,
            save_modiff: 0,
            begv: 1,
            zv: 1, // empty buffer: zv = 1
            file: None,
            undo_list: Vec::new(),
            undo_disabled: false,
        }
    }

    /// Create a buffer from a string (useful for tests and loading files).
    pub fn from_text(name: &str, s: &str) -> Self {
        let text = Rope::from_str(s);
        let len = text.len_chars();
        Buffer {
            name: name.to_string(),
            text,
            pt: 1,
            mark: None,
            mark_active: false,
            modiff: 0,
            save_modiff: 0,
            begv: 1,
            zv: len + 1,
            file: None,
            undo_list: Vec::new(),
            undo_disabled: false,
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

    /// Number of accessible characters.
    pub fn buffer_size(&self) -> usize {
        self.zv - self.begv
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

    /// Move to the beginning of the current line. Returns new point.
    pub fn beginning_of_line(&mut self) -> usize {
        if self.pt == self.begv {
            return self.pt;
        }
        // Search backwards for newline
        let idx0 = self.pt - 1; // 0-based index of char before point
        if idx0 == 0 {
            self.pt = self.begv;
            return self.pt;
        }
        // Walk backwards from char before point
        let slice = self.text.slice(..idx0);
        // Find the last newline in the slice
        let mut pos = idx0;
        for ch in slice.chars_at(idx0).reversed() {
            pos -= 1;
            if ch == '\n' {
                // newline found at pos (0-based), line starts at pos+1
                self.pt = (pos + 1) + 1; // to 1-based
                return self.pt;
            }
        }
        // No newline found, go to beginning
        self.pt = self.begv;
        self.pt
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
                    if idx0 + i + 1 >= self.zv - 1 + 1 {
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
                    self.pt = self.zv;
                    return remaining as isize;
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

    pub fn set_mark(&mut self, pos: usize) {
        self.mark = Some(pos.clamp(self.begv, self.zv));
        self.mark_active = true;
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

    /// Get a substring. Positions are 1-based, range is [from, to).
    pub fn buffer_substring(&self, from: usize, to: usize) -> Result<String, BufferError> {
        let from = from.max(self.begv);
        let to = to.min(self.zv);
        if from > to {
            return Err(BufferError::InvalidPosition(from));
        }
        Ok(self.text.slice((from - 1)..(to - 1)).to_string())
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
        let nchars = s.chars().count();
        if nchars == 0 {
            return self.pt;
        }

        let idx0 = self.pt - 1; // 0-based
        self.text.insert(idx0, s);

        // Record undo
        if !self.undo_disabled {
            self.undo_list.push(UndoEntry::Insert {
                pos: self.pt,
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

        self.modiff += 1;
        self.pt
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
        let nchars = to - from;

        if !self.undo_disabled {
            self.undo_list.push(UndoEntry::Delete {
                pos: from,
                text: deleted.clone(),
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

        // Shrink accessible region
        self.zv -= nchars;

        self.modiff += 1;
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
        let entry = match self.undo_list.pop() {
            Some(e) => e,
            None => return Ok(()), // nothing to undo
        };

        // Temporarily disable undo recording while undoing
        self.undo_disabled = true;
        match entry {
            UndoEntry::Insert { pos, len } => {
                self.goto_char(pos);
                self.delete_region(pos, pos + len)?;
            }
            UndoEntry::Delete { pos, text } => {
                self.goto_char(pos);
                self.insert(&text);
            }
        }
        self.undo_disabled = false;
        Ok(())
    }

    // ── Narrowing ──

    /// Restrict the accessible portion of the buffer.
    pub fn narrow_to_region(&mut self, start: usize, end: usize) {
        let start = start.max(1).min(self.text.len_chars() + 1);
        let end = end.max(start).min(self.text.len_chars() + 1);
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

    // ── Modification state ──

    pub fn is_modified(&self) -> bool {
        self.modiff != self.save_modiff
    }

    pub fn set_unmodified(&mut self) {
        self.save_modiff = self.modiff;
    }

    pub fn modified_tick(&self) -> ModCount {
        self.modiff
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
        let buf = Buffer::from_text("test", "abc");
        assert_eq!(buf.buffer_string(), "abc");
        assert_eq!(buf.buffer_substring(1, 3).unwrap(), "ab");
        assert_eq!(buf.buffer_substring(2, 4).unwrap(), "bc");
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

        // trying to go one more line - no newline after "cc"
        assert_eq!(buf.forward_line(1), 1); // shortage of 1
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
    fn modification_tracking() {
        let mut buf = Buffer::new("test");
        assert!(!buf.is_modified());
        buf.insert("x");
        assert!(buf.is_modified());
        buf.set_unmodified();
        assert!(!buf.is_modified());
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
