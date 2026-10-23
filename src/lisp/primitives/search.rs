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
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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

/// A syntax table's entries for the ASCII characters, as read.
type AsciiEntries = [Option<Value>; 128];

thread_local! {
    /// The cache, most recently used first.  A pattern in use is taken out
    /// (GNU's `busy'), so a search Lisp code runs inside the matcher
    /// compiles its own copy.
    static SEARCHBUFS: RefCell<Vec<CacheEntry>> = const { RefCell::new(Vec::new()) };
    static CANON_CACHE: RefCell<Option<(usize, u64, Rc<Canon>)>> = const { RefCell::new(None) };
    /// The ASCII entries of a syntax table as its slots held them at a
    /// char-table store count: a store or a new table changes the count, so
    /// while it holds, each entry is still the table's (and alive with it).
    static SYNTAX_CACHE: RefCell<Option<(usize, u64, AsciiEntries)>> =
        const { RefCell::new(None) };
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
    InPlace(&'v str),
    Decoded(String, Vec<(usize, u32)>),
}

impl StringText<'_> {
    fn new(value: &Value) -> Option<StringText<'_>> {
        let (bytes, multibyte, _) = string_bytes(value)?;
        if bytes.is_ascii() {
            // SAFETY: ASCII is UTF-8.
            return Some(StringText::InPlace(unsafe {
                std::str::from_utf8_unchecked(bytes)
            }));
        }
        if multibyte
            && let Ok(text) = std::str::from_utf8(bytes)
            && !text.chars().any(|ch| {
                rx_storage_ambiguous(ch) && ch != crate::lisp::json::INVALID_UNICODE_SENTINEL
            })
        {
            return Some(StringText::InPlace(text));
        }
        let (text, extended) =
            crate::lisp::types::string_data::decode_bytes(bytes, multibyte).ok()?;
        Some(StringText::Decoded(text, extended))
    }

    fn text(&self) -> &str {
        match self {
            StringText::InPlace(text) => text,
            StringText::Decoded(text, _) => text,
        }
    }

    fn extended(&self) -> &[(usize, u32)] {
        match self {
            StringText::InPlace(_) => &[],
            StringText::Decoded(_, extended) => extended,
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
        rope: ropey::RopeSlice<'t>,
        /// BEGV, the position of the text's first character.
        begv: usize,
        zv: usize,
        /// Point's byte offset in the text.
        point: usize,
        /// The text is ASCII: byte offsets are character offsets.
        ascii: bool,
        /// `syntax-propertize--done' as last read.
        propertized: Option<i64>,
    },
    String {
        rope: ropey::RopeSlice<'t>,
        extended: &'t [(usize, u32)],
    },
}

/// The translate table a pattern folds with, a case_canon_table, read as
/// char_table_translate does: a character value, else the character.
struct Canon {
    table: CharTableRef,
    ascii: [u32; 128],
    other: RefCell<HashMap<u32, u32>>,
}

impl Canon {
    fn new(table: CharTableRef) -> Self {
        let mut this = Canon {
            table,
            ascii: [0; 128],
            other: RefCell::new(HashMap::new()),
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
        if let Some(&value) = self.other.borrow().get(&c) {
            return value;
        }
        let value = self.compute(c);
        self.other.borrow_mut().insert(c, value);
        value
    }
}

/// The entries of canonicalization TABLE, shared while no char-table
/// changes.
fn canon_for(table: CharTableRef) -> Rc<Canon> {
    let epoch = crate::lisp::alloc::vectors::char_tables::char_table_store_epoch();
    CANON_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((id, at, canon)) = cache.as_ref()
            && *id == table.identity()
            && *at == epoch
        {
            return canon.clone();
        }
        let canon = Rc::new(Canon::new(table));
        *cache = Some((table.identity(), epoch, canon.clone()));
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
    syntax_epoch: u64,
    /// The ASCII entries read from SYNTAX_TABLE, for SYNTAX_CACHE.
    syntax_ascii: AsciiEntries,
    syntax_learned: bool,
    /// `parse-sexp-lookup-properties', read at the first syntax lookup.
    properties: Option<bool>,
    scan: Option<super::syntax::SyntaxScan>,
    object: Object<'t>,
    /// An error from `internal--syntax-propertize', raised at the next
    /// quit check.
    error: Option<LispError>,
    categories: Option<CharTableRef>,
}

impl<'a, 't> LispMatchEnv<'a, 't> {
    /// An environment folding case with the canonicalization CANON (a
    /// case_canon_table), or not at all.
    fn new(interp: &'a mut Interpreter, env: &'a mut Env, canon: Option<CharTableRef>) -> Self {
        let canon = canon.map(canon_for);
        let syntax_table = interp.current_syntax_table_id();
        let syntax_epoch = crate::lisp::alloc::vectors::char_tables::char_table_store_epoch();
        let syntax_ascii = SYNTAX_CACHE.with(|cache| match cache.borrow().as_ref() {
            Some((id, epoch, entries))
                if *id == syntax_table.identity() && *epoch == syntax_epoch =>
            {
                *entries
            }
            _ => [None; 128],
        });
        LispMatchEnv {
            interp,
            env,
            canon,
            syntax_table,
            syntax_epoch,
            syntax_ascii,
            syntax_learned: false,
            properties: None,
            scan: None,
            object: Object::None,
            error: None,
            categories: None,
        }
    }

    /// The syntax class of C in the buffer's own table.  An ASCII entry
    /// comes from the table once per store count, and its class is read
    /// from it each time, as SYNTAX reads the entry's car: a descriptor
    /// changed in place (`setcar') is seen at once.
    fn plain_class(&mut self, c: u32) -> u8 {
        if c < 128 {
            let entry = match self.syntax_ascii[c as usize] {
                Some(entry) => entry,
                None => {
                    let entry = self.syntax_table.get(c);
                    self.syntax_ascii[c as usize] = Some(entry);
                    self.syntax_learned = true;
                    entry
                }
            };
            let code = match entry.kind() {
                Kind::Nil => Some(0),
                Kind::Integer(code) => Some(code),
                Kind::Cons(_) => entry.car().ok().and_then(|car| car.integer_value()),
                _ => None,
            };
            // Inherit (13) and malformed entries take the general path.
            if let Some(class) = code.map(|code| code & 0xffff)
                && (0..16).contains(&class)
                && class != 13
            {
                return class as u8;
            }
        }
        super::syntax::syntax_entry_for_code(self.interp, self.syntax_table, c).class as u8
    }

    fn lookup_properties(&mut self) -> bool {
        *self.properties.get_or_insert_with(|| {
            self.interp
                .lookup_var_key(cached_symbol!("parse-sexp-lookup-properties"), self.env)
                .is_some_and(|value| value.is_truthy())
        })
    }

    /// parse_sexp_propertize: run `internal--syntax-propertize' when
    /// CHARPOS is past what `syntax-propertize' has done.
    fn propertize(&mut self, charpos: usize) {
        let Object::Buffer {
            zv, propertized, ..
        } = &mut self.object
        else {
            return;
        };
        let zv = *zv as i64;
        let done = match *propertized {
            Some(done) => done,
            None => {
                let done = self
                    .interp
                    .lookup_var("syntax-propertize--done", self.env)
                    .and_then(|value| value.integer_value())
                    .unwrap_or(i64::MAX);
                *propertized = Some(done);
                done
            }
        };
        let charpos = charpos as i64;
        if !(done <= charpos && done < zv) {
            return;
        }
        if !self.interp.has_lisp_function("internal--syntax-propertize") {
            if let Object::Buffer { propertized, .. } = &mut self.object {
                *propertized = Some(i64::MAX);
            }
            return;
        }
        let modiffs = self.interp.buffer.borrow().chars_modified_tick();
        // safe_calln: an error in the propertizer is not the search's.
        let _ = self.interp.call_function_value(
            Value::Symbol("internal--syntax-propertize".into()),
            Some("internal--syntax-propertize"),
            &[Value::Integer(zv.min(charpos + 1))],
            self.env,
        );
        if modiffs != self.interp.buffer.borrow().chars_modified_tick() {
            self.error = Some(LispError::Signal(
                "internal--syntax-propertize modified the buffer!".into(),
            ));
        }
        let done = self
            .interp
            .lookup_var("syntax-propertize--done", self.env)
            .and_then(|value| value.integer_value())
            .unwrap_or(i64::MAX);
        if let Object::Buffer { propertized, .. } = &mut self.object {
            // A propertizer that did not move would be asked again at
            // every position; GNU signals.
            *propertized = Some(if done <= charpos && done < zv {
                self.error.get_or_insert(LispError::Signal(
                    "internal--syntax-propertize did not move syntax-propertize--done".into(),
                ));
                i64::MAX
            } else {
                done
            });
        }
        // The property intervals may have changed.
        self.scan = None;
    }

    /// The character position of byte offset AT in the text.
    fn charpos(&self, at: usize) -> usize {
        match &self.object {
            Object::Buffer {
                rope, begv, ascii, ..
            } => {
                if *ascii {
                    begv + at
                } else {
                    begv + rope.byte_to_char(at)
                }
            }
            Object::String { rope, .. } => rope.byte_to_char(at),
            Object::None => at,
        }
    }

    fn category_table(&mut self) -> CharTableRef {
        if let Some(table) = self.categories {
            return table;
        }
        let interp = &mut *self.interp;
        let table = interp
            .initialized_current_category_table_id()
            .unwrap_or_else(|| interp.ensure_standard_category_table());
        self.categories = Some(table);
        table
    }

    /// CATEGORY_MEMBER of a category set (a bool-vector).
    fn category_member(&self, set: &Value, category: u8) -> bool {
        match set.kind() {
            Kind::Record(id) => self.interp.find_record(id).is_some_and(|record| {
                record.kind == crate::lisp::eval::RecordKind::BoolVector
                    && record
                        .slots
                        .get(usize::from(category))
                        .is_some_and(Value::is_truthy)
            }),
            _ => false,
        }
    }

    /// The `unicode-category-table' value for C, GNU's general category
    /// number.
    fn general_category(&self, c: u32) -> Option<i64> {
        let table = self
            .interp
            .lookup_var_key(cached_symbol!("unicode-category-table"), self.env)?;
        match table.kind() {
            Kind::CharTable(table) => table.get(c).integer_value(),
            _ => None,
        }
    }
}

impl Drop for LispMatchEnv<'_, '_> {
    fn drop(&mut self) {
        if self.syntax_learned {
            SYNTAX_CACHE.with(|cache| {
                *cache.borrow_mut() = Some((
                    self.syntax_table.identity(),
                    self.syntax_epoch,
                    self.syntax_ascii,
                ));
            });
        }
    }
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

    fn syntax(&mut self, c: u32, at: usize) -> u8 {
        if !matches!(self.object, Object::Buffer { .. }) || !self.lookup_properties() {
            return self.plain_class(c);
        }
        let charpos = self.charpos(at);
        self.propertize(charpos);
        let interp = &*self.interp;
        let table = self.syntax_table;
        self.scan
            .get_or_insert_with(|| super::syntax::SyntaxScan::new(interp, table))
            .class_at_code(interp, c, charpos) as u8
    }

    fn buffer_syntax(&mut self, c: u32) -> u8 {
        self.plain_class(c)
    }

    fn has_category(&mut self, c: u32, category: u8) -> bool {
        let table = self.category_table();
        let set = table.get(c);
        self.category_member(&set, category)
    }

    /// category.c's word_boundary_p.
    fn word_boundary_p(&mut self, c1: u32, c2: u32) -> bool {
        let scripts = self
            .interp
            .lookup_var_key(cached_symbol!("char-script-table"), self.env);
        let same_script = match scripts.map(|value| value.kind()) {
            Some(Kind::CharTable(table)) => table.get(c1) == table.get(c2),
            _ => true,
        };
        let (tail, default_result) = if same_script {
            (
                self.interp
                    .lookup_var_key(cached_symbol!("word-separating-categories"), self.env),
                false,
            )
        } else {
            (
                self.interp
                    .lookup_var_key(cached_symbol!("word-combining-categories"), self.env),
                true,
            )
        };
        let table = self.category_table();
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
        let mut tail = tail.unwrap_or(Value::Nil);
        while let Ok(elt) = tail.car() {
            if let (Ok(first), Ok(second)) = (elt.car(), elt.cdr())
                && elt.is_cons()
            {
                let first_ok = first.is_nil()
                    || category(first).is_some_and(|cat| {
                        self.category_member(&set1, cat) && !self.category_member(&set2, cat)
                    });
                let second_ok = second.is_nil()
                    || category(second).is_some_and(|cat| {
                        !self.category_member(&set1, cat) && self.category_member(&set2, cat)
                    });
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
/// buffer's tables, taken out of the cache until `release_pattern'.
fn compile_pattern(
    menv: &mut LispMatchEnv<'_, '_>,
    regexp: &Value,
    posix: bool,
) -> Result<CacheEntry, LispError> {
    let (bytes, multibyte, _) = string_bytes(regexp)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *regexp))?;
    let translate = menv.canon.as_ref().map(|canon| canon.table.identity());
    let fold = translate.is_some();
    let whitespace = menv
        .interp
        .lookup_var_key(cached_symbol!("search-spaces-regexp"), menv.env)
        .filter(|value| matches!(value.kind(), Kind::StringObject(_)));
    let whitespace_bytes = whitespace
        .as_ref()
        .and_then(string_bytes)
        .map(|(bytes, multibyte, _)| (bytes, multibyte));
    let syntax_table = menv.syntax_table;

    let found = SEARCHBUFS.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = cache.iter().position(|entry| {
            entry.regexp.as_slice() == bytes
                && entry.multibyte == multibyte
                && entry.translate == translate
                && entry.posix == posix
                && entry.syntax_table.is_none_or(|table| table == syntax_table)
                && match (&entry.whitespace, whitespace_bytes) {
                    (None, None) => true,
                    (Some((saved, saved_multibyte)), Some((now, now_multibyte))) => {
                        saved.as_slice() == now && *saved_multibyte == now_multibyte
                    }
                    _ => false,
                }
        });
        index.map(|index| cache.remove(index))
    });
    if let Some(entry) = found {
        return Ok(entry);
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
    .map_err(|error| invalid_regexp(error.message()))?;
    let used_syntax = pattern.used_syntax;
    Ok(CacheEntry {
        regexp: bytes.to_vec(),
        multibyte,
        translate,
        posix,
        whitespace: whitespace_bytes.map(|(bytes, multibyte)| (bytes.to_vec(), multibyte)),
        syntax_table: used_syntax.then_some(syntax_table),
        pattern,
    })
}

/// search.c:clear_regexp_cache, after a syntax table changes: every
/// pattern holding syntax-table contents is compiled again.  A pattern in
/// use is out of the cache and returns to it; GNU skips `busy' entries.
pub(crate) fn clear_regexp_cache() {
    SEARCHBUFS.with(|cache| {
        cache
            .borrow_mut()
            .retain(|entry| entry.syntax_table.is_none());
    });
}

/// Return a pattern to the cache as its most recently used entry.
fn release_pattern(entry: CacheEntry) {
    SEARCHBUFS.with(|cache| {
        let mut cache = cache.borrow_mut();
        cache.insert(0, entry);
        cache.truncate(REGEXP_CACHE_SIZE);
    });
}

/// The buffer's accessible text, a snapshot the matcher reads without
/// holding the buffer.
struct BufferSnapshot {
    rope: ropey::Rope,
    begv: usize,
    zv: usize,
    begv_byte: usize,
    zv_byte: usize,
    point: usize,
    multibyte: bool,
    side: bool,
    ascii: bool,
}

impl BufferSnapshot {
    fn take(interp: &Interpreter) -> Self {
        let buffer = interp.buffer.borrow();
        let rope = buffer.text_rope();
        let (begv, zv) = (buffer.point_min(), buffer.point_max());
        let begv_byte = rope.char_to_byte(begv - 1);
        let zv_byte = rope.char_to_byte(zv - 1);
        BufferSnapshot {
            begv,
            zv,
            begv_byte,
            zv_byte,
            point: buffer.point(),
            multibyte: buffer.is_multibyte(),
            side: buffer.has_extended_chars_in(begv, zv),
            ascii: rope.len_bytes() == rope.len_chars(),
            rope,
        }
    }

    fn slice(&self) -> ropey::RopeSlice<'_> {
        self.rope.byte_slice(self.begv_byte..self.zv_byte)
    }

    /// The text offset of buffer position POS.
    fn offset(&self, pos: usize) -> usize {
        if self.ascii {
            pos - self.begv
        } else {
            self.rope.char_to_byte(pos - 1) - self.begv_byte
        }
    }

    /// The buffer position of text offset OFFSET.
    fn position(&self, offset: usize) -> usize {
        if self.ascii {
            self.begv + offset
        } else {
            self.rope.byte_to_char(self.begv_byte + offset) + 1
        }
    }

    fn object(&self) -> Object<'_> {
        Object::Buffer {
            rope: self.slice(),
            begv: self.begv,
            zv: self.zv,
            point: self.offset(self.point),
            ascii: self.ascii,
            propertized: None,
        }
    }
}

/// Whether match data may change (`inhibit-changing-match-data' nil).
fn modify_match_data_allowed(interp: &Interpreter, env: &Env) -> bool {
    !interp
        .lookup_var_key(cached_symbol!("inhibit-changing-match-data"), env)
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
    let modify_match_data = modify_match_data_allowed(interp, env) && modify_data;
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
    let text = Text::from_rope(snapshot.slice(), snapshot.multibyte, snapshot.side)
        .with_latin1_bytes(!snapshot.multibyte && !snapshot.ascii);
    menv.object = snapshot.object();
    let pos = snapshot.offset(snapshot.point);
    let result = rx::re_match(&mut entry.pattern, &text, pos, text.len(), &mut menv);
    let pending = menv.error.take();
    drop(menv);
    release_pattern(entry);
    if let Some(error) = pending {
        return Err(error);
    }
    let Some(registers) = result.map_err(match_error)? else {
        return Ok(Value::Nil);
    };
    if modify_match_data {
        set_search_regs(
            interp,
            &registers,
            |offset| snapshot.position(offset),
            Some(buffer_id),
        );
    }
    Ok(Value::T)
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
    let modify_match_data = modify_match_data_allowed(interp, env) && modify_data;
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
    let rope = ropey::RopeSlice::from(haystack);
    let ascii = haystack.len() == chars;
    let pos_byte = if ascii { pos } else { rope.char_to_byte(pos) };
    let text = Text::from_str(
        haystack,
        string_multibyte,
        !string_text.extended().is_empty(),
    );

    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let mut entry = compile_pattern(&mut menv, regexp, posix)?;
    menv.object = Object::String {
        rope,
        extended: string_text.extended(),
    };
    let result = rx::re_search(
        &mut entry.pattern,
        &text,
        pos_byte,
        (text.len() - pos_byte) as isize,
        text.len(),
        &mut menv,
    );
    drop(menv);
    release_pattern(entry);
    let Some(registers) = result.map_err(match_error)? else {
        return Ok(Value::Nil);
    };
    let to_char = |offset: usize| {
        if ascii {
            offset
        } else {
            rope.byte_to_char(offset)
        }
    };
    let start = registers.regs[0].expect("group 0").0;
    if modify_match_data {
        set_search_regs(interp, &registers, to_char, None);
    }
    Ok(Value::Integer(to_char(start) as i64))
}

/// search_command for a regexp (`re-search-forward' and the others):
/// DIRECTION is 1 or -1.
pub(super) fn re_search_command(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
    direction: i64,
    posix: bool,
) -> Result<Value, LispError> {
    let regexp = args[0];
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
    if !matches!(regexp.kind(), Kind::StringObject(_)) {
        return Err(LispError::WrongTypeArgument("stringp".into(), regexp));
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

    let np = search_buffer_re(interp, env, &regexp, pt, lim, n, posix)?;
    if np <= 0 {
        if noerror.is_nil() {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("search-failed"),
                regexp,
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

/// search_buffer for a regexp: the position after the Nth match (before
/// it, searching backward), or -X when X matches remain unfound.
fn search_buffer_re(
    interp: &mut Interpreter,
    env: &mut Env,
    regexp: &Value,
    pos: usize,
    lim: usize,
    mut n: i64,
    posix: bool,
) -> Result<i64, LispError> {
    let preserve_match_data = modify_match_data_allowed(interp, env);
    let buffer_id = interp.current_buffer_id();
    let regexp_chars = string_bytes(regexp).map_or(0, |(_, _, chars)| chars);
    // Searching 0 times means don't move; the null string is found at the
    // starting position.
    if n == 0 || regexp_chars == 0 {
        if preserve_match_data {
            interp.last_match_data = Some(vec![Some((pos, pos))]);
            interp.last_match_data_buffer_id = Some(buffer_id);
        }
        return Ok(pos as i64);
    }

    let case_table = interp
        .case_fold_search_active(env)
        .then(|| interp.current_case_tables().canon);
    let snapshot = BufferSnapshot::take(interp);
    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let mut entry = compile_pattern(&mut menv, regexp, posix)?;
    let text = Text::from_rope(snapshot.slice(), snapshot.multibyte, snapshot.side)
        .with_latin1_bytes(!snapshot.multibyte && !snapshot.ascii);
    menv.object = snapshot.object();
    let lim_byte = snapshot.offset(lim);
    let mut pos_byte = snapshot.offset(pos);
    let mut pos = pos;
    let mut last: Option<Registers> = None;

    let outcome = (|| -> Result<i64, LispError> {
        menv.maybe_quit()?;
        while n < 0 {
            let found = rx::re_search(
                &mut entry.pattern,
                &text,
                pos_byte,
                lim_byte as isize - pos_byte as isize,
                // Don't allow a match past the current point.
                pos_byte,
                &mut menv,
            )
            .map_err(match_error)?;
            let Some(registers) = found else {
                return Ok(n);
            };
            pos_byte = registers.regs[0].expect("group 0").0;
            pos = snapshot.position(pos_byte);
            last = Some(registers);
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
                &mut menv,
            )
            .map_err(match_error)?;
            let Some(registers) = found else {
                return Ok(-n);
            };
            pos_byte = registers.regs[0].expect("group 0").1;
            pos = snapshot.position(pos_byte);
            last = Some(registers);
            n -= 1;
            menv.maybe_quit()?;
        }
        Ok(pos as i64)
    })();
    let pending = menv.error.take();
    drop(menv);
    release_pattern(entry);
    // Each match found updated the match data, the last one standing.
    if preserve_match_data && let Some(registers) = &last {
        set_search_regs(
            interp,
            registers,
            |offset| snapshot.position(offset),
            Some(buffer_id),
        );
    }
    if let Some(error) = pending {
        return Err(error);
    }
    outcome
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
    fast_text_match(
        interp,
        env,
        regexp,
        string_text.text(),
        &extended,
        multibyte,
        case_table,
        signal_overflow,
    )
}

/// `fast_string_match' on a string's text as Emaxx renders it: raw bytes
/// as U+E080..U+E0FF, characters beyond Unicode as the placeholder EXTENDED
/// lists by character index.
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
    let rope = ropey::RopeSlice::from(haystack);
    let text = Text::from_str(haystack, multibyte, !extended.is_empty());
    let mut menv = LispMatchEnv::new(interp, env, case_table);
    let mut entry = compile_pattern(&mut menv, regexp, false)?;
    menv.object = Object::String { rope, extended };
    let result = rx::re_search(
        &mut entry.pattern,
        &text,
        0,
        text.len() as isize,
        text.len(),
        &mut menv,
    );
    drop(menv);
    release_pattern(entry);
    let registers = match result {
        Ok(Some(registers)) => registers,
        Ok(None) => return Ok(None),
        Err(MatchError::Overflow) if !signal_overflow => return Ok(None),
        Err(error) => return Err(match_error(error)),
    };
    let start = registers.regs[0].expect("group 0").0;
    Ok(Some(if haystack.is_ascii() {
        start
    } else {
        rope.byte_to_char(start)
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
        rope: ropey::RopeSlice::from(haystack.as_str()),
        extended: &[],
    };
    let result = rx::re_search(
        &mut entry.pattern,
        &text,
        0,
        text.len() as isize,
        text.len(),
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
        let text = Text::from_rope(snapshot.slice(), snapshot.multibyte, snapshot.side)
            .with_latin1_bytes(!snapshot.multibyte && !snapshot.ascii);
        let mut menv = LispMatchEnv::new(interp, env, None);
        let mut entry = compile_pattern(&mut menv, regexp, false)?;
        menv.object = snapshot.object();
        let start = snapshot.offset(pos);
        let result = rx::re_match(
            &mut entry.pattern,
            &text,
            start,
            snapshot.offset(limit),
            &mut menv,
        );
        drop(menv);
        release_pattern(entry);
        let Some(registers) = result.map_err(match_error)? else {
            return Ok(None);
        };
        let end = registers.regs[0].expect("group 0").1;
        return Ok(Some(snapshot.position(end) - pos));
    }
    let Some((_, multibyte, chars)) = string_bytes(string) else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *string));
    };
    let string_text = StringText::new(string).expect("a string");
    let haystack = string_text.text();
    let rope = ropey::RopeSlice::from(haystack);
    let ascii = haystack.len() == chars;
    let byte = |index: usize| {
        if ascii {
            index
        } else {
            rope.char_to_byte(index)
        }
    };
    let text = Text::from_str(haystack, multibyte, !string_text.extended().is_empty());
    let mut menv = LispMatchEnv::new(interp, env, None);
    let mut entry = compile_pattern(&mut menv, regexp, false)?;
    menv.object = Object::String {
        rope,
        extended: string_text.extended(),
    };
    let result = rx::re_match(&mut entry.pattern, &text, byte(pos), byte(limit), &mut menv);
    drop(menv);
    release_pattern(entry);
    let Some(registers) = result.map_err(match_error)? else {
        return Ok(None);
    };
    let end = registers.regs[0].expect("group 0").1;
    Ok(Some(if ascii { end } else { rope.byte_to_char(end) } - pos))
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
