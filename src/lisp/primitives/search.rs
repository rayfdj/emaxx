//! search.c's regexp entry points over the regex-emacs engine: the cache
//! of compiled patterns (`compile_pattern'), `looking_at_1',
//! `string_match_1' and `search_buffer_re', and the tables the matcher
//! reads through [`MatchEnv`]: case canonicalization, syntax with
//! `syntax-table' properties, categories, `word_boundary_p' and the
//! Unicode classes.  The text is matched in place: a buffer's rope, or a
//! string's bytes when they are its stored form already.

use super::*;
use crate::lisp::regex_emacs::{self as rx, MatchEnv, MatchError, Pattern, Registers, Text};
use crate::lisp::types::{CharTableRef, Kind};
use std::cell::{Cell, RefCell};
use std::rc::Rc;

mod non_re;

/// REGEXP_CACHE_SIZE.
const REGEXP_CACHE_SIZE: usize = 20;

/// One compiled pattern of the cache (`struct regexp_cache').
struct CacheEntry {
    /// The pattern string's bytes in their internal encoding.
    regexp: Vec<u8>,
    multibyte: bool,
    /// The canonicalization the pattern was compiled to fold with.
    translate: Option<usize>,
    posix: bool,
    /// `search-spaces-regexp' when the pattern was compiled.
    whitespace: Option<(Vec<u8>, bool)>,
    /// The syntax table whose contents the pattern holds (`[:space:]',
    /// `[:word:]'), as GNU's `syntax_table' field.
    syntax_table: Option<CharTableRef>,
    pattern: Pattern,
}

/// A canonicalization table read at a char-table store epoch: the
/// table's identity, the epoch and its entries.
type CanonSlot = Option<(usize, u64, Rc<Canon>)>;

/// A cell of searchbufs: its compiled pattern, if any, and the next cell
/// in most-recently-used order.  A busy cell's pattern is lent to the
/// search running it (GNU's `busy'), so a search Lisp code runs inside
/// the matcher compiles its own copy into another cell.
struct SearchBuf {
    entry: Option<Box<CacheEntry>>,
    busy: bool,
    next: Option<u8>,
}

/// searchbufs and searchbuf_head: REGEXP_CACHE_SIZE cells linked most
/// recently used first.  Cells stay where they are; only the links move.
#[derive(Default)]
struct SearchBufs {
    cells: Vec<SearchBuf>,
    head: Option<u8>,
}

/// A compiled pattern lent to one search by its busy cache cell, which
/// holds it again when the search is done however it ends (GNU's
/// unfreeze_pattern, run by an unwind-protect).
struct LentPattern {
    entry: std::mem::ManuallyDrop<Box<CacheEntry>>,
    cell: u8,
}

impl Drop for LentPattern {
    fn drop(&mut self) {
        // SAFETY: the entry is taken once, here, and not used after.
        let entry = unsafe { std::mem::ManuallyDrop::take(&mut self.entry) };
        release_cell(self.cell, Some(entry));
    }
}

impl std::ops::Deref for LentPattern {
    type Target = CacheEntry;
    fn deref(&self) -> &CacheEntry {
        &self.entry
    }
}

impl std::ops::DerefMut for LentPattern {
    fn deref_mut(&mut self) -> &mut CacheEntry {
        &mut self.entry
    }
}

thread_local! {
    static SEARCHBUFS: RefCell<SearchBufs> = RefCell::new(SearchBufs::default());
    /// The canonicalization tables last read, with the char-table store
    /// epoch they were read at: a search folding case reads the
    /// case_canon_table and its case_eqv_table.
    static CANON_CACHE: RefCell<[CanonSlot; 2]> = const { RefCell::new([None, None]) };
}

#[cfg(test)]
thread_local! {
    static COMPILES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The patterns compile_pattern has compiled on this thread.
#[cfg(test)]
pub(crate) fn elisp_regex_compile_count() -> usize {
    COMPILES.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_elisp_regex_compile_count() {
    COMPILES.with(|count| count.set(0));
}

/// The bytes of a string value, read in place.  Nothing that could store
/// into the string or collect runs while they are in use: the matcher calls
/// Lisp only for `syntax-propertize' in a buffer.
fn string_bytes(value: &Value) -> Option<(&[u8], bool, usize)> {
    match value.kind() {
        Kind::StringObject(state) => {
            let (multibyte, chars) = {
                let borrowed = state.borrow();
                (borrowed.is_multibyte(), borrowed.len())
            };
            // SAFETY: as above; the value keeps the string alive.
            let bytes = unsafe { state.bytes_unguarded() };
            Some((bytes, multibyte, chars))
        }
        _ => None,
    }
}

/// The character codes of a pattern string: a multibyte string's
/// characters, a unibyte string's bytes.
fn pattern_codes(bytes: &[u8], multibyte: bool) -> Vec<u32> {
    if !multibyte || bytes.is_ascii() {
        return bytes.iter().map(|&byte| u32::from(byte)).collect();
    }
    let mut codes = Vec::with_capacity(bytes.len());
    let mut offset = 0;
    while offset < bytes.len() {
        let (code, width) = crate::lisp::types::string_data::decode_character(&bytes[offset..])
            .expect("a string's bytes have the internal encoding");
        codes.push(code);
        offset += width;
    }
    codes
}

/// A string's text in the matcher's stored form: its own bytes when they
/// are that form already (ASCII, or multibyte UTF-8 with no character the
/// stored form reads otherwise), else the decoded text and its side table.
enum StringText<'v> {
    /// The string's data, whose character and byte indices convert
    /// through fns.c's string_char_to_byte cache.
    InPlace(&'v str, StringObjectRef),
    Decoded(String, Vec<(usize, u32)>),
}

/// Whether BYTES, a string's data, are the matcher's stored form.
fn stored_form_in_place(bytes: &[u8], multibyte: bool) -> bool {
    bytes.is_ascii()
        || (multibyte
            && std::str::from_utf8(bytes).is_ok_and(|text| {
                !text.chars().any(|ch| {
                    rx_storage_ambiguous(ch) && ch != crate::lisp::json::INVALID_UNICODE_SENTINEL
                })
            }))
}

impl StringText<'_> {
    fn new(value: &Value) -> Option<StringText<'_>> {
        let (bytes, multibyte, chars) = string_bytes(value)?;
        let Kind::StringObject(state) = value.kind() else {
            return None;
        };
        // A multibyte string of a byte a character is ASCII, and a
        // unibyte one is in place when it is; otherwise the scan is
        // remembered while the string holds the cache, as a search
        // repeated from later starts (`split-string') matches one string
        // again and again.  A short unibyte string is scanned at once,
        // for less than the cache costs: `apropos-internal' matches each
        // symbol's name once.
        const SHORT: usize = 64;
        if (multibyte && bytes.len() == chars)
            || (!multibyte && bytes.len() <= SHORT && bytes.is_ascii())
            || state
                .borrow()
                .stored_text_in_place(|bytes| stored_form_in_place(bytes, multibyte))
        {
            // SAFETY: stored_form_in_place found the bytes UTF-8.
            return Some(StringText::InPlace(
                unsafe { std::str::from_utf8_unchecked(bytes) },
                state,
            ));
        }
        let (text, extended) =
            crate::lisp::types::string_data::decode_bytes(bytes, multibyte).ok()?;
        Some(StringText::Decoded(text, extended))
    }

    fn text(&self) -> &str {
        match self {
            StringText::InPlace(text, _) => text,
            StringText::Decoded(text, _) => text,
        }
    }

    fn extended(&self) -> &[(usize, u32)] {
        match self {
            StringText::InPlace(..) => &[],
            StringText::Decoded(_, extended) => extended,
        }
    }

    /// The string read in place, whose positions convert through the cache.
    fn in_place(&self) -> Option<StringObjectRef> {
        match self {
            StringText::InPlace(_, state) => Some(*state),
            StringText::Decoded(..) => None,
        }
    }

    /// The byte offset in `text' of character INDEX.
    fn char_to_byte(&self, index: usize) -> usize {
        match self {
            StringText::InPlace(_, state) => state.borrow().char_to_byte(index),
            StringText::Decoded(text, _) => ropey::str_utils::char_to_byte_idx(text, index),
        }
    }

    /// The character index of byte offset OFFSET in `text'.
    fn byte_to_char(&self, offset: usize) -> usize {
        match self {
            StringText::InPlace(_, state) => state.borrow().byte_to_char(offset),
            StringText::Decoded(text, _) => ropey::str_utils::byte_to_char_idx(text, offset),
        }
    }
}

/// Whether a character's stored form is a raw-byte marker.
fn rx_storage_ambiguous(ch: char) -> bool {
    (0xE080..=0xE0FF).contains(&(ch as u32))
}

/// What the matcher searches.
enum Object<'t> {
    /// Compiling: no text.
    None,
    Buffer {
        text: SnapshotRange<'t>,
        /// BEGV, the position of the text's first character.
        begv: usize,
        /// Point's byte offset in the text.
        point: usize,
        /// The text is ASCII: byte offsets are character offsets.
        ascii: bool,
        /// A byte offset and its character offset, point's to begin with
        /// and then the one converted last: the position
        /// buf_bytepos_to_charpos counts a nearby one from.
        known: Cell<(usize, usize)>,
    },
    String {
        /// The string's data, read in place.
        text: &'t str,
        extended: &'t [(usize, u32)],
        /// The string whose own bytes TEXT is, for its position cache.
        in_place: Option<StringObjectRef>,
        /// The string object (`re_match_object'), None for text without
        /// one (GNU's t).
        string: Option<Value>,
    },
}

/// The translate table a pattern folds with, a case_canon_table, read as
/// char_table_translate does: a character value, else the character.
struct Canon {
    table: CharTableRef,
    ascii: [u32; 128],
}

impl Canon {
    fn new(table: CharTableRef) -> Self {
        let mut this = Canon {
            table,
            ascii: [0; 128],
        };
        for c in 0..128u32 {
            this.ascii[c as usize] = this.compute(c);
        }
        this
    }

    fn compute(&self, c: u32) -> u32 {
        self.table
            .get(c)
            .integer_value()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|&value| value <= rx::MAX_CHAR)
            .unwrap_or(c)
    }

    #[inline]
    fn get(&self, c: u32) -> u32 {
        if c < 128 {
            return self.ascii[c as usize];
        }
        self.get_other(c)
    }

    /// RE_TRANSLATE of a character beyond ASCII: char_table_translate's
    /// char_table_ref, which reads the table's tree directly.
    #[inline(never)]
    fn get_other(&self, c: u32) -> u32 {
        self.compute(c)
    }
}

/// The entries of canonicalization TABLE, shared while no char-table
/// changes.
fn canon_for(table: CharTableRef) -> Rc<Canon> {
    let epoch = crate::lisp::alloc::vectors::char_tables::char_table_store_epoch();
    CANON_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (index, slot) in cache.iter().enumerate() {
            if let Some((id, at, canon)) = slot
                && *id == table.identity()
                && *at == epoch
            {
                let canon = canon.clone();
                // Most recently used first.
                cache.swap(0, index);
                return canon;
            }
        }
        let canon = Rc::new(Canon::new(table));
        cache.swap(0, 1);
        cache[0] = Some((table.identity(), epoch, canon.clone()));
        canon
    })
}

/// The matcher's view of the interpreter (GNU's current buffer, gl_state
/// and the case and category tables).
pub(super) struct LispMatchEnv<'a, 't> {
    interp: &'a mut Interpreter,
    env: &'a mut Env,
    canon: Option<Rc<Canon>>,
    syntax_table: CharTableRef,
    /// gl_state as RE_SETUP_SYNTAX_TABLE_FOR_OBJECT leaves it, when
    /// `parse-sexp-lookup-properties' is set; without it SYNTAX reads the
    /// buffer's table.
    gl: Option<Box<super::syntax_scan::GlState>>,
    object: Object<'t>,
    /// An error from `internal--syntax-propertize', raised at the next
    /// quit check.
    error: Option<LispError>,
    categories: Option<CharTableRef>,
}

impl Drop for LispMatchEnv<'_, '_> {
    fn drop(&mut self) {
        if let Some(gl) = self.gl.take() {
            super::syntax_scan::GlState::release_for_regexp(gl);
        }
    }
}

impl<'a, 't> LispMatchEnv<'a, 't> {
    /// An environment folding case with the canonicalization CANON (a
    /// case_canon_table), or not at all.
    fn new(interp: &'a mut Interpreter, env: &'a mut Env, canon: Option<CharTableRef>) -> Self {
        let canon = canon.map(canon_for);
        let syntax_table = interp.current_syntax_table_id();
        LispMatchEnv {
            interp,
            env,
            canon,
            syntax_table,
            gl: None,
            object: Object::None,
            error: None,
            categories: None,
        }
    }

    /// BUFFER_SYNTAX: the syntax class of C in the buffer's own table.
    #[inline]
    fn plain_class(&self, c: u32) -> u8 {
        (super::syntax_scan::entry_flags(self.syntax_table.get(c)) & 0xff) as u8
    }

    /// The character position of byte offset AT in the text.
    fn charpos(&self, at: usize) -> usize {
        match &self.object {
            Object::Buffer {
                text,
                begv,
                ascii,
                known,
                ..
            } => {
                if *ascii {
                    begv + at
                } else {
                    begv + text.char_offset_from(known, at)
                }
            }
            Object::String {
                in_place: Some(state),
                ..
            } => state.borrow().byte_to_char(at),
            Object::String { text, .. } => ropey::str_utils::byte_to_char_idx(text, at),
            Object::None => at,
        }
    }

    fn category_table(&mut self) -> CharTableRef {
        if let Some(table) = self.categories {
            return table;
        }
        let table = current_category_table(self.interp);
        self.categories = Some(table);
        table
    }

    /// The `unicode-category-table' value for C, GNU's general category
    /// number.
    fn general_category(&self, c: u32) -> Option<i64> {
        let table = self
            .interp
            .symbol_value_key(cached_symbol!("unicode-category-table"))?;
        match table.kind() {
            Kind::CharTable(table) => table.get(c).integer_value(),
            _ => None,
        }
    }
}

/// The current buffer's category table.
pub(super) fn current_category_table(interp: &mut Interpreter) -> CharTableRef {
    interp
        .initialized_current_category_table_id()
        .unwrap_or_else(|| interp.ensure_standard_category_table())
}

/// CATEGORY_MEMBER of a category set (a bool-vector).
pub(crate) fn category_member(set: &Value, category: u8) -> bool {
    match set.kind() {
        Kind::BoolVector(set) => set.get(usize::from(category)) == Some(true),
        _ => false,
    }
}

/// The variables category.c's word_boundary_p reads:
/// `char-script-table', `word-separating-categories' and
/// `word-combining-categories'.
#[derive(Clone, Copy)]
pub(super) struct WordBoundaryVars {
    scripts: Option<CharTableRef>,
    separating: Value,
    combining: Value,
}

impl WordBoundaryVars {
    pub(super) fn read(interp: &Interpreter) -> Self {
        let scripts = match interp
            .symbol_value_key(cached_symbol!("char-script-table"))
            .map(|value| value.kind())
        {
            Some(Kind::CharTable(table)) => Some(table),
            _ => None,
        };
        let list = |key| interp.symbol_value_key(key).unwrap_or(Value::Nil);
        WordBoundaryVars {
            scripts,
            separating: list(cached_symbol!("word-separating-categories")),
            combining: list(cached_symbol!("word-combining-categories")),
        }
    }
}

/// category.c's word_boundary_p, TABLE the current category table.
pub(super) fn word_boundary_p(interp: &Interpreter, table: CharTableRef, c1: u32, c2: u32) -> bool {
    word_boundary_p_with(WordBoundaryVars::read(interp), table, c1, c2)
}

/// word_boundary_p with the variables VARS read.
pub(super) fn word_boundary_p_with(
    vars: WordBoundaryVars,
    table: CharTableRef,
    c1: u32,
    c2: u32,
) -> bool {
    let same_script = match vars.scripts {
        Some(scripts) => scripts.get(c1).word() == scripts.get(c2).word(),
        None => true,
    };
    let (tail, default_result) = if same_script {
        (vars.separating, false)
    } else {
        (vars.combining, true)
    };
    let set1 = table.get(c1);
    if set1.is_nil() {
        return default_result;
    }
    let set2 = table.get(c2);
    if set2.is_nil() {
        return default_result;
    }
    let category = |value: Value| {
        value
            .integer_value()
            .filter(|code| (0x20..0x7F).contains(code))
            .map(|code| code as u8)
    };
    let mut tail = tail;
    while let Ok(elt) = tail.car() {
        if let (Ok(first), Ok(second)) = (elt.car(), elt.cdr())
            && elt.is_cons()
        {
            let first_ok = first.is_nil()
                || category(first)
                    .is_some_and(|cat| category_member(&set1, cat) && !category_member(&set2, cat));
            let second_ok = second.is_nil()
                || category(second)
                    .is_some_and(|cat| !category_member(&set1, cat) && category_member(&set2, cat));
            if first_ok && second_ok {
                return !default_result;
            }
        }
        let Ok(next) = tail.cdr() else { break };
        if !next.is_cons() {
            break;
        }
        tail = next;
    }
    default_result
}

/// character.h's general categories the predicates test.
mod unicode {
    pub(super) const LU: i64 = 1;
    pub(super) const LL: i64 = 2;
    pub(super) const LT: i64 = 3;
    pub(super) const LM: i64 = 4;
    pub(super) const LO: i64 = 5;
    pub(super) const MN: i64 = 6;
    pub(super) const MC: i64 = 7;
    pub(super) const ME: i64 = 8;
    pub(super) const ND: i64 = 9;
    pub(super) const NL: i64 = 10;
    pub(super) const ZS: i64 = 23;
    pub(super) const ZL: i64 = 24;
    pub(super) const ZP: i64 = 25;
    pub(super) const CC: i64 = 26;
    pub(super) const CS: i64 = 28;
    pub(super) const CN: i64 = 30;
}

impl MatchEnv for LispMatchEnv<'_, '_> {
    #[inline]
    fn translate(&mut self, c: u32) -> u32 {
        match &self.canon {
            Some(canon) => canon.get(c),
            None => c,
        }
    }

    fn translate_stamp(&self) -> u64 {
        match &self.canon {
            // The canonicalization is read again when any char-table
            // changes.
            Some(_) => crate::lisp::alloc::vectors::char_tables::char_table_store_epoch(),
            None => 0,
        }
    }

    fn setup_syntax(&mut self, at: usize) {
        use super::syntax_scan::{GlObject, GlState};
        if let Some(gl) = self.gl.take() {
            GlState::release_for_regexp(gl);
        }
        let object = match &self.object {
            Object::Buffer { .. } => GlObject::Buffer,
            Object::String {
                string: Some(string),
                ..
            } if matches!(string.kind(), Kind::StringObject(state)
                    if state.borrow().props.has_intervals()) =>
            {
                GlObject::String(*string)
            }
            // Text without intervals has no `syntax-table' property:
            // update_syntax_table's interval_of finds none and returns,
            // leaving the buffer's table, which the matcher reads without
            // gl_state too.
            Object::String { .. } | Object::None => return,
        };
        if !self
            .interp
            .symbol_value_key(cached_symbol!("parse-sexp-lookup-properties"))
            .is_some_and(|value| value.is_truthy())
        {
            return;
        }
        let (gl, error) = GlState::for_regexp(self.interp, object, || self.charpos(at));
        if let Some(error) = error {
            self.error.get_or_insert(error);
        }
        self.gl = Some(gl);
    }

    fn syntax(&mut self, c: u32, at: usize) -> u8 {
        let Some(lookup) = self.gl.as_ref().map(|gl| gl.lookup_properties()) else {
            return self.plain_class(c);
        };
        if lookup {
            // RE_SYNTAX_TABLE_BYTE_TO_CHAR and UPDATE_SYNTAX_TABLE.
            let charpos = self.charpos(at);
            let gl = self.gl.as_mut().expect("gl_state was set up");
            if let Err(error) = gl.update(self.interp, self.env, charpos) {
                self.error.get_or_insert(error);
            }
        }
        self.gl.as_mut().expect("gl_state was set up").syntax(c)
    }

    fn buffer_syntax(&mut self, c: u32) -> u8 {
        self.plain_class(c)
    }

    fn buffer_syntax_ascii(&mut self) -> [u8; 128] {
        self.syntax_table
            .ascii_get_all()
            .map(|entry| (super::syntax_scan::entry_flags(entry) & 0xff) as u8)
    }

    fn has_category(&mut self, c: u32, category: u8) -> bool {
        let table = self.category_table();
        let set = table.get(c);
        category_member(&set, category)
    }

    fn word_boundary_p(&mut self, c1: u32, c2: u32) -> bool {
        let table = self.category_table();
        word_boundary_p(self.interp, table, c1, c2)
    }

    fn alphabeticp(&mut self, c: u32) -> bool {
        use unicode::*;
        matches!(
            self.general_category(c),
            Some(LU | LL | LT | LM | LO | MN | MC | ME | NL)
        )
    }

    fn alphanumericp(&mut self, c: u32) -> bool {
        use unicode::*;
        matches!(
            self.general_category(c),
            Some(LU | LL | LT | LM | LO | MN | MC | ME | NL | ND)
        )
    }

    fn graphicp(&mut self, c: u32) -> bool {
        use unicode::*;
        self.general_category(c)
            .is_some_and(|cat| !matches!(cat, ZS | ZL | ZP | CC | CS | CN))
    }

    fn printablep(&mut self, c: u32) -> bool {
        use unicode::*;
        self.general_category(c)
            .is_some_and(|cat| !matches!(cat, CC | CS | CN))
    }

    fn blankp(&mut self, c: u32) -> bool {
        self.general_category(c) == Some(unicode::ZS)
    }

    fn lowercasep(&mut self, c: u32) -> bool {
        super::case::current_case_classes(self.interp, c).0
    }

    fn uppercasep(&mut self, c: u32) -> bool {
        super::case::current_case_classes(self.interp, c).1
    }

    fn point(&self) -> Option<usize> {
        match &self.object {
            Object::Buffer { point, .. } => Some(*point),
            _ => None,
        }
    }

    fn side_table_char(&mut self, at: usize) -> Option<u32> {
        match &self.object {
            Object::Buffer { .. } => {
                let pos = self.charpos(at);
                self.interp.buffer.borrow().extended_char_at(pos)
            }
            Object::String { extended, .. } => {
                let index = self.charpos(at);
                extended
                    .binary_search_by_key(&index, |(position, _)| *position)
                    .ok()
                    .map(|found| extended[found].1)
            }
            Object::None => None,
        }
    }

    #[inline]
    fn maybe_quit(&mut self) -> Result<(), LispError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        self.interp.maybe_quit(self.env)
    }
}

fn invalid_regexp(message: &str) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("invalid-regexp".into()),
        Value::String(message.into()),
    ]))
}

/// matcher_overflow.
fn matcher_overflow() -> LispError {
    LispError::Signal("Stack overflow in regexp matcher".into())
}

fn match_error(error: MatchError) -> LispError {
    match error {
        MatchError::Overflow => matcher_overflow(),
        MatchError::Lisp(error) => error,
    }
}

/// compile_pattern: the cached compilation of REGEXP for the current
/// buffer's tables, its cell busy until `release_pattern'.  The cells are
/// looked at most recently used first; a pattern not found is compiled
/// into the first empty cell or else the least recently used one that is
/// not busy, and the cell used goes to the front.
/// Fstring_equal's test of two patterns' bytes: the lengths, then the
/// bytes.  Two empty ones are equal without memcmp: an empty pattern's
/// saved copy is an empty vector, whose pointer is a placeholder rather
/// than an address, and a zero-length memcmp through it measured some 200
/// cycles on the Xeon these numbers come from, against 5 through real
/// addresses (the `""' of `apropos-internal' paid that twice a symbol).
#[inline]
fn same_bytes(saved: &[u8], now: &[u8]) -> bool {
    saved.len() == now.len() && (saved.is_empty() || saved == now)
}

fn compile_pattern(
    menv: &mut LispMatchEnv<'_, '_>,
    regexp: &Value,
    posix: bool,
) -> Result<LentPattern, LispError> {
    let (bytes, multibyte, _) = string_bytes(regexp)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *regexp))?;
    let translate = menv.canon.as_ref().map(|canon| canon.table.identity());
    let fold = translate.is_some();
    let whitespace = menv
        .interp
        .symbol_value_key(cached_symbol!("search-spaces-regexp"))
        .filter(|value| matches!(value.kind(), Kind::StringObject(_)));
    let whitespace_bytes = whitespace
        .as_ref()
        .and_then(string_bytes)
        .map(|(bytes, multibyte, _)| (bytes, multibyte));
    let syntax_table = menv.syntax_table;

    let matches = |entry: &CacheEntry| {
        same_bytes(&entry.regexp, bytes)
            && entry.multibyte == multibyte
            && entry.translate == translate
            && entry.posix == posix
            && entry.syntax_table.is_none_or(|table| table == syntax_table)
            && match (&entry.whitespace, whitespace_bytes) {
                (None, None) => true,
                (Some((saved, saved_multibyte)), Some((now, now_multibyte))) => {
                    same_bytes(saved, now) && *saved_multibyte == now_multibyte
                }
                _ => false,
            }
    };
    // The cell found or to compile into, marked busy and moved to the
    // front; with it, its pattern when it holds this one.  None when
    // every cell is busy.
    let lent = SEARCHBUFS.with(|cache| {
        let mut cache = cache.borrow_mut();
        let cache = &mut *cache;
        if cache.cells.is_empty() {
            cache.cells = (0..REGEXP_CACHE_SIZE)
                .map(|index| SearchBuf {
                    entry: None,
                    busy: false,
                    next: (index + 1 < REGEXP_CACHE_SIZE).then_some((index + 1) as u8),
                })
                .collect();
            cache.head = Some(0);
        }
        let mut previous = None;
        let mut current = cache.head;
        let mut least_recent = None;
        let mut chosen = None;
        while let Some(index) = current {
            let cell = &cache.cells[usize::from(index)];
            if !cell.busy {
                match &cell.entry {
                    None => {
                        chosen = Some((previous, index));
                        break;
                    }
                    Some(entry) if matches(entry) => {
                        chosen = Some((previous, index));
                        break;
                    }
                    Some(_) => least_recent = Some((previous, index)),
                }
            }
            previous = current;
            current = cell.next;
        }
        let (previous, index) = chosen.or(least_recent)?;
        if let Some(previous) = previous {
            cache.cells[usize::from(previous)].next = cache.cells[usize::from(index)].next;
            cache.cells[usize::from(index)].next = cache.head;
            cache.head = Some(index);
        }
        let cell = &mut cache.cells[usize::from(index)];
        cell.busy = true;
        Some((index, cell.entry.take().filter(|entry| matches(entry))))
    });
    let Some((cell, found)) = lent else {
        return Err(LispError::Signal("Too much matching reentrancy".into()));
    };
    if let Some(entry) = found {
        return Ok(LentPattern {
            entry: std::mem::ManuallyDrop::new(entry),
            cell,
        });
    }

    #[cfg(test)]
    COMPILES.with(|count| count.set(count.get() + 1));
    let codes = pattern_codes(bytes, multibyte);
    let whitespace_codes =
        whitespace_bytes.map(|(bytes, multibyte)| pattern_codes(bytes, multibyte));
    let pattern = rx::compile(
        &codes,
        multibyte,
        posix,
        whitespace_codes.as_deref(),
        fold,
        menv,
    )
    .map_err(|error| {
        // compile_pattern_1 leaves the cell empty.
        release_cell(cell, None);
        invalid_regexp(error.message())
    })?;
    let used_syntax = pattern.used_syntax;
    let entry = Box::new(CacheEntry {
        regexp: bytes.to_vec(),
        multibyte,
        translate,
        posix,
        whitespace: whitespace_bytes.map(|(bytes, multibyte)| (bytes.to_vec(), multibyte)),
        syntax_table: used_syntax.then_some(syntax_table),
        pattern,
    });
    Ok(LentPattern {
        entry: std::mem::ManuallyDrop::new(entry),
        cell,
    })
}

/// search.c:clear_regexp_cache, after a syntax table changes: every
/// pattern holding syntax-table contents is compiled again, but for the
/// busy ones.
pub(crate) fn clear_regexp_cache() {
    SEARCHBUFS.with(|cache| {
        for cell in &mut cache.borrow_mut().cells {
            if !cell.busy
                && cell
                    .entry
                    .as_ref()
                    .is_some_and(|entry| entry.syntax_table.is_some())
            {
                cell.entry = None;
            }
        }
    });
}

/// A search is done with its pattern: its cell is no longer busy.
fn release_pattern(lent: LentPattern) {
    drop(lent);
}

/// CELL holds ENTRY again and is not busy.
fn release_cell(cell: u8, entry: Option<Box<CacheEntry>>) {
    let _ = SEARCHBUFS.try_with(|cache| {
        let cell = &mut cache.borrow_mut().cells[usize::from(cell)];
        cell.entry = entry;
        cell.busy = false;
    });
}

/// The buffer's accessible text, a snapshot the matcher reads without
/// holding the buffer: the buffer's rope, or a small text's own copy.
struct BufferSnapshot {
    text: crate::buffer::ScanText,
    begv: usize,
    begv_byte: usize,
    zv: usize,
    zv_byte: usize,
    point: usize,
    /// Point's byte, as GNU's PT_BYTE: the matcher starts there.
    point_byte: usize,
    multibyte: bool,
    side: bool,
    ascii: bool,
    /// The buffer's chars-modified tick when taken: its text is the
    /// snapshot's while the tick stays.
    tick: crate::buffer::ModCount,
    /// The rope leaf holding point, as the buffer's conversion of point
    /// left it.
    hint: Option<crate::buffer::ChunkHint>,
}

/// A snapshot's accessible text as the matcher's object reads it.
#[derive(Clone, Copy)]
enum SnapshotRange<'t> {
    Rope(crate::lisp::regex_emacs::RopeRange<'t>),
    Str(&'t str),
}

/// How far in bytes from the offset it knows `char_offset_from' counts
/// characters rather than asking the text: a rope's conversion counts
/// within the leaf it lands in, after walking to it.
const CHAR_COUNT_DISTANCE: usize = 1024;

impl SnapshotRange<'_> {
    /// The character offset of byte offset AT from the range's start.
    fn byte_to_char(&self, at: usize) -> usize {
        match self {
            SnapshotRange::Rope(rope) => rope.byte_to_char(at),
            SnapshotRange::Str(text) => ropey::str_utils::byte_to_char_idx(text, at),
        }
    }

    /// The character offset of byte offset AT, counted from KNOWN's offset
    /// when AT is near it, as buf_bytepos_to_charpos counts from the
    /// nearest position it knows; AT is then the one known.
    fn char_offset_from(&self, known: &Cell<(usize, usize)>, at: usize) -> usize {
        let (byte, char) = known.get();
        let offset = if at.abs_diff(byte) > CHAR_COUNT_DISTANCE {
            self.byte_to_char(at)
        } else if at >= byte {
            char + self.chars_in(byte, at)
        } else {
            char - self.chars_in(at, byte)
        };
        known.set((at, offset));
        offset
    }

    /// The characters bytes FROM..TO of the range begin.
    fn chars_in(&self, from: usize, to: usize) -> usize {
        fn starts(bytes: &[u8]) -> usize {
            bytes.iter().filter(|&&byte| byte & 0xC0 != 0x80).count()
        }
        match self {
            SnapshotRange::Str(text) => starts(&text.as_bytes()[from..to]),
            SnapshotRange::Rope(rope) => {
                let mut count = 0;
                let mut at = from;
                while at < to {
                    let (chunk, chunk_start) = rope.chunk_at_byte(at);
                    let end = (chunk_start + chunk.len()).min(to);
                    count += starts(&chunk[at - chunk_start..end - chunk_start]);
                    at = end;
                }
                count
            }
        }
    }
}

impl BufferSnapshot {
    fn take(interp: &Interpreter) -> Self {
        let buffer = interp.buffer.borrow();
        let text = buffer.scan_text();
        let (begv, zv, point) = (buffer.point_min(), buffer.point_max(), buffer.point());
        // The counts the buffer keeps, where a one-leaf rope would count
        // its text twice.
        let (chars, bytes) = buffer.text_counts();
        let ascii = chars == bytes;
        // An ASCII text's bytes are its characters; otherwise the buffer's
        // conversions, which know its ends and the positions converted
        // since its last edit before them.
        let byte = |pos: usize| {
            if ascii {
                return pos - 1;
            }
            buffer
                .storage_byte_position(pos)
                .expect("an accessible position")
                - 1
        };
        let point_byte = byte(point);
        BufferSnapshot {
            begv,
            begv_byte: byte(begv),
            zv,
            zv_byte: byte(zv),
            point,
            point_byte,
            hint: buffer.scan_hint(point_byte),
            multibyte: buffer.is_multibyte(),
            side: buffer.has_extended_chars_in(begv, zv),
            ascii,
            tick: buffer.chars_modified_tick(),
            text,
        }
    }

    fn slice(&self) -> ropey::RopeSlice<'_> {
        match &self.text {
            crate::buffer::ScanText::Rope(rope) => rope.byte_slice(self.begv_byte..self.zv_byte),
            crate::buffer::ScanText::Small(text) => {
                ropey::RopeSlice::from(&text[self.begv_byte..self.zv_byte])
            }
        }
    }

    /// The accessible text as the matcher reads it, with no RopeSlice built.
    fn range(&self) -> SnapshotRange<'_> {
        match &self.text {
            crate::buffer::ScanText::Rope(rope) => {
                let range = crate::lisp::regex_emacs::RopeRange::new(
                    rope,
                    self.begv_byte,
                    self.zv_byte,
                    self.begv - 1,
                );
                SnapshotRange::Rope(match self.hint {
                    Some(hint) => {
                        // SAFETY: `take' took the hint with this rope.
                        let (chunk, start) = unsafe { hint.chunk(rope) };
                        range.with_chunk(chunk, start)
                    }
                    None => range,
                })
            }
            crate::buffer::ScanText::Small(text) => {
                SnapshotRange::Str(&text[self.begv_byte..self.zv_byte])
            }
        }
    }

    /// The matcher's text for this snapshot's accessible text.
    fn matcher_text(&self) -> Text<'_> {
        let text = match self.range() {
            SnapshotRange::Rope(rope) => Text::from_rope(rope, self.multibyte, self.side),
            SnapshotRange::Str(text) => Text::from_str(text, self.multibyte, self.side),
        };
        text.with_latin1_bytes(!self.multibyte && !self.ascii)
    }

    /// The text offset of buffer position POS.
    fn offset(&self, pos: usize) -> usize {
        if self.ascii {
            pos - self.begv
        } else if pos == self.point {
            self.point_byte - self.begv_byte
        } else if pos == self.zv {
            self.zv_byte - self.begv_byte
        } else if pos == self.begv {
            0
        } else {
            let byte = match &self.text {
                crate::buffer::ScanText::Rope(rope) => rope.char_to_byte(pos - 1),
                crate::buffer::ScanText::Small(text) => {
                    ropey::str_utils::char_to_byte_idx(text, pos - 1)
                }
            };
            byte - self.begv_byte
        }
    }

    /// The buffer position of text offset OFFSET.
    fn position(&self, offset: usize) -> usize {
        let byte = self.begv_byte + offset;
        if self.ascii {
            self.begv + offset
        } else if byte == self.point_byte {
            self.point
        } else if byte == self.zv_byte {
            self.zv
        } else if offset == 0 {
            self.begv
        } else {
            match &self.text {
                crate::buffer::ScanText::Rope(rope) => rope.byte_to_char(byte) + 1,
                crate::buffer::ScanText::Small(text) => {
                    ropey::str_utils::byte_to_char_idx(text, byte) + 1
                }
            }
        }
    }

    /// POSITION through BUFFER, the buffer this snapshot was taken of,
    /// while its text is the snapshot's: the buffer's own conversion
    /// (BYTE_TO_CHAR), which starts from the chunk and the positions it
    /// converted last (point's, which `take' converted, then each
    /// register's), where the snapshot's rope would walk its tree for
    /// each offset.
    fn position_in(&self, buffer: &crate::buffer::Buffer, offset: usize) -> usize {
        let byte = self.begv_byte + offset;
        if self.ascii || buffer.chars_modified_tick() != self.tick {
            return self.position(offset);
        }
        if byte == self.point_byte {
            return self.point;
        }
        buffer
            .storage_byte_to_position(byte + 1)
            .unwrap_or_else(|| self.position(offset))
    }

    /// The matcher's object for this snapshot's text.
    fn object(&self) -> Object<'_> {
        let point = self.offset(self.point);
        Object::Buffer {
            text: self.range(),
            begv: self.begv,
            point,
            ascii: self.ascii,
            known: Cell::new((point, self.point - self.begv)),
        }
    }
}

/// Whether match data may change (`inhibit-changing-match-data' nil).
fn modify_match_data_allowed(interp: &Interpreter) -> bool {
    !interp
        .symbol_value_key(cached_symbol!("inhibit-changing-match-data"))
        .is_some_and(|value| value.is_truthy())
}

/// Store REGISTERS as the match data, through POSITION from text offsets.
fn set_search_regs(
    interp: &mut Interpreter,
    registers: &Registers,
    position: impl Fn(usize) -> usize,
    buffer: Option<u64>,
) {
    let mut match_data = interp.last_match_data.take().unwrap_or_default();
    match_data.clear();
    match_data.extend(
        registers
            .regs
            .iter()
            .map(|reg| reg.map(|(start, end)| (position(start), position(end)))),
    );
    interp.last_match_data = Some(match_data);
    interp.last_match_data_buffer_id = buffer;
}

/// set_search_regs for REGISTERS, offsets in SNAPSHOT's text, the current
/// buffer's when it was taken: converted by the buffer (see
/// `BufferSnapshot::position_in').
fn set_buffer_search_regs(
    interp: &mut Interpreter,
    registers: &Registers,
    snapshot: &BufferSnapshot,
    buffer_id: u64,
) {
    let buffer = interp.buffer;
    let buffer = buffer.borrow();
    let same = interp.current_buffer_id() == buffer_id;
    set_search_regs(
        interp,
        registers,
        |offset| {
            if same {
                snapshot.position_in(&buffer, offset)
            } else {
                snapshot.position(offset)
            }
        },
        Some(buffer_id),
    );
}

/// search.c's `&search_regs' argument to the matcher: the held register
/// buffer while the match data may change (MODIFY), else none.  Given back
/// with `give_back_registers' however the search ends.
fn take_registers(interp: &mut Interpreter, modify: bool) -> Option<Registers> {
    modify.then(|| std::mem::take(&mut interp.search_registers))
}

fn give_back_registers(interp: &mut Interpreter, registers: Option<Registers>) {
    if let Some(registers) = registers {
        interp.search_registers = registers;
    }
}

/// looking_at_1.
pub(super) fn looking_at_1(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    posix: bool,
    modify_data: bool,
) -> Result<Value, LispError> {
    if !matches!(regexp.kind(), Kind::StringObject(_)) {
        return Err(LispError::WrongTypeArgument("stringp".into(), *regexp));
    }
    let modify_match_data = modify_match_data_allowed(interp) && modify_data;
    let case_table = interp
        .case_fold_search_active(env)
        .then(|| interp.current_case_tables().canon);
    let buffer_id = interp.current_buffer_id();
    let snapshot = BufferSnapshot::take(interp);
    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let mut entry = compile_pattern(&mut menv, regexp, posix)?;
    // Do a pending quit right away, to avoid paradoxical behavior.
    if let Err(error) = menv.maybe_quit() {
        release_pattern(entry);
        return Err(error);
    }
    let text = snapshot.matcher_text();
    menv.object = snapshot.object();
    let pos = snapshot.offset(snapshot.point);
    let mut registers = take_registers(menv.interp, modify_match_data);
    let result = rx::re_match(
        &mut entry.pattern,
        &text,
        pos,
        text.len(),
        registers.as_mut(),
        &mut menv,
    );
    let pending = menv.error.take();
    drop(menv);
    release_pattern(entry);
    let found = match (pending, result.map_err(match_error)) {
        (Some(error), _) | (None, Err(error)) => Err(error),
        (None, Ok(end)) => Ok(end.is_some()),
    };
    if let (Ok(true), Some(registers)) = (&found, &registers) {
        set_buffer_search_regs(interp, registers, &snapshot, buffer_id);
    }
    give_back_registers(interp, registers);
    Ok(if found? { Value::T } else { Value::Nil })
}

/// string_match_1.
pub(super) fn string_match_1(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    string: &Value,
    start: Option<&Value>,
    posix: bool,
    modify_data: bool,
) -> Result<Value, LispError> {
    let modify_match_data = modify_match_data_allowed(interp) && modify_data;
    if !matches!(regexp.kind(), Kind::StringObject(_)) {
        return Err(LispError::WrongTypeArgument("stringp".into(), *regexp));
    }
    let Some((_, string_multibyte, chars)) = string_bytes(string) else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *string));
    };
    let pos = match start.filter(|value| !value.is_nil()) {
        None => 0,
        Some(start) => {
            let Kind::Integer(index) = start.kind() else {
                return Err(LispError::WrongTypeArgument("fixnump".into(), *start));
            };
            let len = chars as i64;
            if index < 0 && -index <= len {
                (len + index) as usize
            } else if (0..=len).contains(&index) {
                index as usize
            } else {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("args-out-of-range"),
                    *string,
                    *start,
                ])));
            }
        }
    };
    let case_table = interp
        .case_fold_search_active(env)
        .then(|| interp.current_case_tables().canon);
    let string_text = StringText::new(string).expect("a string");
    let haystack = string_text.text();
    let ascii = haystack.len() == chars;
    let pos_byte = if ascii {
        pos
    } else {
        string_text.char_to_byte(pos)
    };
    let text = Text::from_str(
        haystack,
        string_multibyte,
        !string_text.extended().is_empty(),
    );

    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let mut entry = compile_pattern(&mut menv, regexp, posix)?;
    menv.object = Object::String {
        text: haystack,
        extended: string_text.extended(),
        in_place: string_text.in_place(),
        string: Some(*string),
    };
    let mut registers = take_registers(menv.interp, modify_match_data);
    let result = rx::re_search(
        &mut entry.pattern,
        &text,
        pos_byte,
        (text.len() - pos_byte) as isize,
        text.len(),
        registers.as_mut(),
        &mut menv,
    );
    drop(menv);
    release_pattern(entry);
    let to_char = |offset: usize| {
        if ascii {
            offset
        } else {
            string_text.byte_to_char(offset)
        }
    };
    // last_thing_searched is Qt once the match data may change, whether
    // or not this search changes it.
    if modify_match_data {
        interp.last_match_data_buffer_id = None;
    }
    if let (Ok(Some(_)), Some(registers)) = (&result, &registers) {
        set_search_regs(interp, registers, to_char, None);
    }
    give_back_registers(interp, registers);
    let Some(start) = result.map_err(match_error)? else {
        return Ok(Value::Nil);
    };
    Ok(Value::Integer(to_char(start) as i64))
}

/// search_command: `search-forward', `re-search-forward' and the others.
/// DIRECTION is 1 or -1; RE says STRING is a regexp, POSIX that it is
/// matched with POSIX backtracking.
pub(super) fn search_command(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
    direction: i64,
    re: bool,
    posix: bool,
) -> Result<Value, LispError> {
    let string = args[0];
    let bound = args.get(1).copied().unwrap_or(Value::Nil);
    let noerror = args.get(2).copied().unwrap_or(Value::Nil);
    let count = args.get(3).copied().unwrap_or(Value::Nil);
    let mut n = direction;
    if !count.is_nil() {
        let Kind::Integer(count) = count.kind() else {
            return Err(LispError::WrongTypeArgument("fixnump".into(), count));
        };
        n *= count;
    }
    if !matches!(string.kind(), Kind::StringObject(_)) {
        return Err(LispError::WrongTypeArgument("stringp".into(), string));
    }
    let (pt, begv, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point(), buffer.point_min(), buffer.point_max())
    };
    let lim = if bound.is_nil() {
        if n > 0 { zv } else { begv }
    } else {
        let lim = match bound.kind() {
            Kind::Integer(pos) => pos,
            Kind::Marker(_) => position_from_value(interp, &bound)? as i64,
            _ => {
                return Err(LispError::WrongTypeArgument(
                    "integer-or-marker-p".into(),
                    bound,
                ));
            }
        };
        if if n > 0 {
            lim < pt as i64
        } else {
            lim > pt as i64
        } {
            return Err(LispError::Signal(
                "Invalid search bound (wrong side of point)".into(),
            ));
        }
        lim.clamp(begv as i64, zv as i64) as usize
    };

    let np = search_buffer(interp, env, &string, pt, lim, n, re, posix)?;
    if np <= 0 {
        if noerror.is_nil() {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("search-failed"),
                string,
            ])));
        }
        if noerror != Value::T {
            interp.buffer.borrow_mut().goto_char(lim);
        }
        return Ok(Value::Nil);
    }
    interp.buffer.borrow_mut().goto_char(np as usize);
    Ok(Value::Integer(np))
}

/// search_buffer: search for the Nth occurrence of STRING in the current
/// buffer from position POS until LIM, forward when N is positive (LIM
/// past POS), else backward.  RE says to look for matches against the
/// regular expression STRING, POSIX to backtrack as POSIX does.  The
/// position at the end (searching forward) or the beginning (backward) of
/// the Nth occurrence, or -X if there are X remaining occurrences.
#[allow(clippy::too_many_arguments)]
fn search_buffer(
    interp: &mut Interpreter,
    env: &mut Env,
    string: &Value,
    pos: usize,
    lim: usize,
    n: i64,
    re: bool,
    posix: bool,
) -> Result<i64, LispError> {
    let (bytes, _, chars) = string_bytes(string)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *string))?;
    // Searching 0 times means don't move.  Null string is found at
    // starting position.
    if n == 0 || chars == 0 {
        if modify_match_data_allowed(interp) {
            interp.last_match_data = Some(vec![Some((pos, pos))]);
            interp.last_match_data_buffer_id = Some(interp.current_buffer_id());
        }
        return Ok(pos as i64);
    }
    if re
        && !(non_re::trivial_regexp_p(bytes)
            && interp
                .symbol_value_key(cached_symbol!("search-spaces-regexp"))
                .is_none_or(|value| value.is_nil()))
    {
        return search_buffer_re(interp, env, string, pos, lim, n, posix);
    }
    let tables = interp
        .case_fold_search_active(env)
        .then(|| interp.current_case_tables());
    let trt = tables.map(|tables| canon_for(tables.canon));
    let inverse_trt = tables.map(|tables| canon_for(tables.eqv));
    non_re::search_buffer_non_re(
        interp,
        env,
        string,
        pos,
        lim,
        n,
        re,
        trt.as_deref(),
        inverse_trt.as_deref(),
    )
}

/// search_buffer_re: search_buffer for a regexp that is not trivial.
fn search_buffer_re(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    pos: usize,
    lim: usize,
    mut n: i64,
    posix: bool,
) -> Result<i64, LispError> {
    let preserve_match_data = modify_match_data_allowed(interp);
    let buffer_id = interp.current_buffer_id();
    let case_table = interp
        .case_fold_search_active(env)
        .then(|| interp.current_case_tables().canon);
    let snapshot = BufferSnapshot::take(interp);
    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let mut entry = compile_pattern(&mut menv, regexp, posix)?;
    let text = snapshot.matcher_text();
    menv.object = snapshot.object();
    let lim_byte = snapshot.offset(lim);
    let mut pos_byte = snapshot.offset(pos);
    // search_regs, or search_regs_1 when the match data is to stay: each
    // match needs its group 0 to go on from.
    let mut registers = std::mem::take(&mut menv.interp.search_registers);
    let mut found_any = false;

    // The count left, or (every match found) the last match's end, to
    // convert once the matching is done.
    let outcome = (|| -> Result<Result<i64, usize>, LispError> {
        menv.maybe_quit()?;
        while n < 0 {
            let found = rx::re_search(
                &mut entry.pattern,
                &text,
                pos_byte,
                lim_byte as isize - pos_byte as isize,
                // Don't allow a match past the current point.
                pos_byte,
                Some(&mut registers),
                &mut menv,
            )
            .map_err(match_error)?;
            if found.is_none() {
                return Ok(Ok(n));
            }
            pos_byte = registers.regs[0].expect("group 0").0;
            found_any = true;
            n += 1;
            menv.maybe_quit()?;
        }
        while n > 0 {
            let found = rx::re_search(
                &mut entry.pattern,
                &text,
                pos_byte,
                lim_byte as isize - pos_byte as isize,
                lim_byte,
                Some(&mut registers),
                &mut menv,
            )
            .map_err(match_error)?;
            if found.is_none() {
                return Ok(Ok(-n));
            }
            pos_byte = registers.regs[0].expect("group 0").1;
            found_any = true;
            n -= 1;
            menv.maybe_quit()?;
        }
        Ok(Err(pos_byte))
    })();
    let pending = menv.error.take();
    drop(menv);
    release_pattern(entry);
    // Each match found updated the match data, the last one standing.
    if preserve_match_data && found_any {
        set_buffer_search_regs(interp, &registers, &snapshot, buffer_id);
    }
    interp.search_registers = registers;
    if let Some(error) = pending {
        return Err(error);
    }
    Ok(match outcome? {
        Ok(count) => count,
        Err(end) => {
            let buffer = interp.buffer;
            let position = if interp.current_buffer_id() == buffer_id {
                snapshot.position_in(&buffer.borrow(), end)
            } else {
                snapshot.position(end)
            };
            position as i64
        }
    })
}

/// re--describe-compiled: the compiled form of REGEXP, its bytes when RAW;
/// a description needs GNU's checking build.
pub(super) fn describe_compiled(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    raw: bool,
) -> Result<Value, LispError> {
    let case_table = interp
        .case_fold_search_active(env)
        .then(|| interp.current_case_tables().canon);
    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let entry = compile_pattern(&mut menv, regexp, false)?;
    let bytes = entry.pattern.buffer.clone();
    drop(menv);
    release_pattern(entry);
    if raw {
        Ok(super::coding::bytes_to_shared_unibyte_value(&bytes))
    } else {
        Err(LispError::Signal(
            "Not available: rebuild with --enable-checking".into(),
        ))
    }
}

/// fast_string_match_internal: where REGEXP first matches STRING (a
/// character index), folding case with CASE_TABLE's canonicalization when
/// given; the match data unchanged.  GNU's -2, an overflow of the failure
/// stack, is no match to most callers and an error when SIGNAL_OVERFLOW.
pub(crate) fn fast_string_match(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    string: &Value,
    case_table: Option<CharTableRef>,
    signal_overflow: bool,
) -> Result<Option<usize>, LispError> {
    let Some((_, multibyte, _)) = string_bytes(string) else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *string));
    };
    let string_text = StringText::new(string).expect("a string");
    let extended = string_text.extended().to_vec();
    text_match(
        interp,
        env,
        regexp,
        Some(*string),
        string_text.in_place(),
        string_text.text(),
        &extended,
        multibyte,
        case_table,
        signal_overflow,
    )
}

/// `fast_string_match' on a string's text as Emaxx renders it: raw bytes
/// as U+E080..U+E0FF, characters beyond Unicode as the placeholder EXTENDED
/// lists by character index.  The text has no string object, so no text
/// properties (GNU's t as `re_match_object').
#[allow(clippy::too_many_arguments)]
pub(crate) fn fast_text_match(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    haystack: &str,
    extended: &[(usize, u32)],
    multibyte: bool,
    case_table: Option<CharTableRef>,
    signal_overflow: bool,
) -> Result<Option<usize>, LispError> {
    text_match(
        interp,
        env,
        regexp,
        None,
        None,
        haystack,
        extended,
        multibyte,
        case_table,
        signal_overflow,
    )
}

/// fast_string_match_internal on HAYSTACK, the text of STRING when given
/// (its own bytes when IN_PLACE names it).
#[allow(clippy::too_many_arguments)]
fn text_match(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    string: Option<Value>,
    in_place: Option<StringObjectRef>,
    haystack: &str,
    extended: &[(usize, u32)],
    multibyte: bool,
    case_table: Option<CharTableRef>,
    signal_overflow: bool,
) -> Result<Option<usize>, LispError> {
    let text = Text::from_str(haystack, multibyte, !extended.is_empty());
    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let mut entry = compile_pattern(&mut menv, regexp, false)?;
    menv.object = Object::String {
        text: haystack,
        extended,
        in_place,
        string,
    };
    let result = rx::re_search(
        &mut entry.pattern,
        &text,
        0,
        text.len() as isize,
        text.len(),
        None,
        &mut menv,
    );
    drop(menv);
    release_pattern(entry);
    let start = match result {
        Ok(Some(start)) => start,
        Ok(None) => return Ok(None),
        Err(MatchError::Overflow) if !signal_overflow => return Ok(None),
        Err(error) => return Err(match_error(error)),
    };
    Ok(Some(match in_place {
        Some(state) => state.borrow().byte_to_char(start),
        None if haystack.is_ascii() => start,
        None => ropey::str_utils::byte_to_char_idx(haystack, start),
    }))
}

/// fast_c_string_match_ignore_case: whether REGEXP matches the unibyte
/// text BYTES, folding case with the standard case table.
pub(crate) fn fast_c_string_match_ignore_case(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    bytes: &[u8],
) -> Result<bool, LispError> {
    // GNU converts the regexp with string_make_unibyte first, even when its
    // low bytes change the regexp's syntax.
    let regexp = match string_bytes(regexp) {
        Some((_, true, _)) => super::call(
            interp,
            "string-make-unibyte",
            std::slice::from_ref(regexp),
            env,
        )?,
        _ => *regexp,
    };
    let regexp = &regexp;
    let (haystack, _) = crate::lisp::types::string_data::decode_bytes(bytes, false)?;
    let text = Text::from_str(&haystack, false, false);
    // Vascii_canon_table.
    let standard = interp.standard_case_tables().canon;
    let mut menv = LispMatchEnv::new(interp, env, Some(standard));
    let mut entry = compile_pattern(&mut menv, regexp, false)?;
    menv.object = Object::String {
        text: &haystack,
        extended: &[],
        in_place: None,
        string: None,
    };
    let result = rx::re_search(
        &mut entry.pattern,
        &text,
        0,
        text.len() as isize,
        text.len(),
        None,
        &mut menv,
    );
    drop(menv);
    release_pattern(entry);
    Ok(matches!(result, Ok(Some(_))))
}

/// fast_looking_at: the length in characters of REGEXP's match at POS in
/// STRING, or in the current buffer when STRING is nil, not past LIMIT;
/// case is not folded and the match data is unchanged.
pub(crate) fn fast_looking_at(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    pos: usize,
    limit: usize,
    string: &Value,
) -> Result<Option<usize>, LispError> {
    if string.is_nil() {
        let snapshot = BufferSnapshot::take(interp);
        let text = snapshot.matcher_text();
        let mut menv = LispMatchEnv::new(interp, env, None);
        let mut entry = compile_pattern(&mut menv, regexp, false)?;
        menv.object = snapshot.object();
        let start = snapshot.offset(pos);
        let result = rx::re_match(
            &mut entry.pattern,
            &text,
            start,
            snapshot.offset(limit),
            None,
            &mut menv,
        );
        drop(menv);
        release_pattern(entry);
        let Some(end) = result.map_err(match_error)? else {
            return Ok(None);
        };
        return Ok(Some(snapshot.position(end) - pos));
    }
    let Some((_, multibyte, chars)) = string_bytes(string) else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *string));
    };
    let string_text = StringText::new(string).expect("a string");
    let haystack = string_text.text();
    let ascii = haystack.len() == chars;
    let byte = |index: usize| {
        if ascii {
            index
        } else {
            string_text.char_to_byte(index)
        }
    };
    let text = Text::from_str(haystack, multibyte, !string_text.extended().is_empty());
    let mut menv = LispMatchEnv::new(interp, env, None);
    let mut entry = compile_pattern(&mut menv, regexp, false)?;
    menv.object = Object::String {
        text: haystack,
        extended: string_text.extended(),
        in_place: string_text.in_place(),
        string: Some(*string),
    };
    let result = rx::re_match(
        &mut entry.pattern,
        &text,
        byte(pos),
        byte(limit),
        None,
        &mut menv,
    );
    drop(menv);
    release_pattern(entry);
    let Some(end) = result.map_err(match_error)? else {
        return Ok(None);
    };
    Ok(Some(
        if ascii {
            end
        } else {
            string_text.byte_to_char(end)
        } - pos,
    ))
}

#[cfg(test)]
mod c_string_match_tests {
    use super::*;

    #[test]
    fn c_string_match_folds_only_ascii_and_preserves_match_data() {
        let mut interp = Interpreter::new();
        interp.set_variable("case-fold-search", Value::Nil, &mut Env::new());
        let saved = Some(vec![Some((3, 7)), None]);
        interp.last_match_data = saved.clone();
        let saved_buffer = Some(interp.current_buffer_id());
        interp.last_match_data_buffer_id = saved_buffer;
        // casetab.c:init_casetab_once gives Vascii_canon_table mappings
        // only for ASCII. Every other byte must remain distinct.
        for pattern_byte in 0..=u8::MAX {
            let literal = super::super::coding::bytes_to_shared_unibyte_value(&[pattern_byte]);
            let pattern =
                super::super::call(&mut interp, "regexp-quote", &[literal], &mut Env::new())
                    .expect("quote one byte through the ordinary C primitive");
            for input_byte in 0..=u8::MAX {
                assert_eq!(
                    fast_c_string_match_ignore_case(
                        &mut interp,
                        &mut Env::new(),
                        &pattern,
                        &[input_byte]
                    )
                    .expect("search a quoted byte"),
                    pattern_byte.eq_ignore_ascii_case(&input_byte),
                    "pattern byte {pattern_byte:#x}, input byte {input_byte:#x}"
                );
            }
        }
        assert_eq!(interp.last_match_data, saved);
        assert_eq!(interp.last_match_data_buffer_id, saved_buffer);
    }

    #[test]
    fn c_string_match_converts_the_pattern_to_unibyte_before_compilation() {
        let mut interp = Interpreter::new();
        // fns.c:string_make_unibyte / copy_text retain the low byte of
        // multibyte characters. 0x141 becomes ASCII A, not Unicode L.
        for (pattern, input, expected) in [
            ("\u{141}", b'a', true),
            ("\u{141}", b'L', false),
            ("\u{e9}", 0xe9, true),
            ("\u{e9}", 0xc9, false),
            ("[\u{141}-\u{143}]", b'b', true),
            ("[\u{c0}-\u{df}]", 0xe9, false),
        ] {
            assert_eq!(
                fast_c_string_match_ignore_case(
                    &mut interp,
                    &mut Env::new(),
                    &Value::string(pattern),
                    &[input]
                )
                .expect("compile the converted byte pattern"),
                expected,
                "pattern {pattern:?}, input byte {input:#x}"
            );
        }
    }
}
