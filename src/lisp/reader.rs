use super::types::{
    IdentityBuildHasher, Kind, LispError, ReaderClosureKind, ReaderForm, StringObjectRef, Value,
    make_uninterned_symbol_name,
};
mod input;
use input::Input;
pub(crate) use input::ReaderStream;

use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use unicode_names2::character as unicode_character;

const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
const INVALID_UNICODE_SENTINEL: char = '\u{F8FF}';
// lisp.h's character modifier bits.
const CHAR_ALT: i64 = 0x040_0000;
const CHAR_SUPER: i64 = 0x080_0000;
const CHAR_HYPER: i64 = 0x100_0000;
const CHAR_SHIFT: i64 = 0x200_0000;
const CHAR_CTL: i64 = 0x400_0000;
const CHAR_META: i64 = 0x800_0000;
const CHAR_MODIFIER_MASK: i64 =
    CHAR_ALT | CHAR_SUPER | CHAR_HYPER | CHAR_SHIFT | CHAR_CTL | CHAR_META;
// BYTE8_TO_CHAR's offset: the eight-bit character of byte B is B + 0x3FFF00.
const BYTE8_BASE: i64 = 0x3F_FF00;
// lread.c's bound on the length of a Unicode character name.
const UNICODE_CHARACTER_NAME_LENGTH_BOUND: usize = 200;

/// character.h CHAR_BYTE8_P: a character past MAX_5_BYTE_CHAR.
fn char_byte8_p(c: i64) -> bool {
    c > 0x3F_FF7F
}

/// The text of character CODE in an INVALID_SYNTAX_WITH_BUFFER datum.
fn reader_code_text(code: i64) -> String {
    if char_byte8_p(code) {
        return encode_raw_byte((code - BYTE8_BASE) as u8).to_string();
    }
    u32::try_from(code)
        .ok()
        .and_then(char::from_u32)
        .map_or_else(String::new, String::from)
}

/// read0's test for the end of a `#:' or `#_' symbol: the end of input, a
/// character up to SPC, NO-BREAK SPACE or one of "';#()[]`,.
fn symbol_terminator_code(code: Option<i64>) -> bool {
    match code {
        None => true,
        Some(c) => {
            c <= 32
                || c == 0xA0
                || matches!(
                    u8::try_from(c),
                    Ok(b'"' | b'\'' | b';' | b'#' | b'(' | b')' | b'[' | b']' | b'`' | b',')
                )
        }
    }
}

/// lread.c char_hexdigit.
fn hex_digit_value(c: i64) -> Option<i64> {
    u8::try_from(c)
        .ok()
        .and_then(|byte| (byte as char).to_digit(16))
        .map(i64::from)
}

/// lread.c character_name_to_code: "U+" and hex digits, or the code
/// `(char-from-name NAME t)' gives, which CONTEXT calls in its Lisp world
/// (a reader with none looks NAME up in Unicode's names); anything else,
/// a surrogate or a code past Unicode is `\N{NAME}' invalid syntax.
fn character_name_to_code(
    name: &str,
    context: Option<&mut dyn ReadContext>,
) -> Result<i64, LispError> {
    let code = match name.strip_prefix("U+") {
        Some(hex) => (!hex.is_empty() && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| u32::from_str_radix(hex, 16).ok())
            .flatten(),
        None => match context {
            Some(context) => match context.char_from_name(name)?.kind() {
                Kind::Integer(code) => u32::try_from(code).ok(),
                _ => None,
            },
            None => unicode_character(name).map(|ch| ch as u32),
        },
    };
    match code {
        Some(code) if valid_unicode_scalar(code) => Ok(i64::from(code)),
        _ => Err(LispError::ReadError(format!("\\N{{{name}}}"))),
    }
}
static READER_UNINTERNED_SYMBOL_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, PartialEq, Eq)]
enum AtomSyntax {
    Ordinary,
    ShorthandExempt,
    Uninterned,
}

/// What the reader asks of the Lisp world as it reads: lread.c's read0
/// interns each symbol in the current obarray, and builds each `#s(...)',
/// `#[...]', `#&...' and `#^[...]' object when its syntax closes.  A
/// reader without a context leaves `ReaderForm' placeholders for the
/// interpreter to finish (`materialize_read_object_literals').
pub(crate) trait ReadContext {
    /// read0's intern of NAME in the current obarray, a new symbol named
    /// by a string of the multibyteness readchar reported (MULTIBYTE).
    fn intern(&mut self, name: &str, multibyte: bool) -> Result<Value, LispError>;
    fn build(&mut self, form: ReaderForm) -> Result<Value, LispError>;
    /// Vload_file_name, the value `#$' reads as.
    fn load_file_name(&mut self) -> Value;
    /// load_force_doc_strings.
    fn force_doc_strings(&mut self) -> bool;
    /// get_lazy_string's fallback, doc.c get_doc_string (REFERENCE, true):
    /// the unibyte text the (FILE . POSITION) REFERENCE names in its file.
    fn doc_string(&mut self, reference: Value) -> Result<Value, LispError>;
    /// character_name_to_code's `(char-from-name NAME t)'.
    fn char_from_name(&mut self, name: &str) -> Result<Value, LispError>;
    /// Vread_circle: whether `#N=' and `#N#' are read.
    fn read_circle(&mut self) -> bool {
        true
    }
    /// read_char_literal's record of an unescaped `(', `)', `[', `]', `"'
    /// or `;' CHARACTER on `lread--unescaped-character-literals', as it is
    /// read.
    fn unescaped_character_literal(&mut self, character: u8) -> Result<(), LispError>;
    /// symbols_with_pos_enabled: whether SYMBOLP takes a symbol with
    /// position for its symbol.
    fn symbols_with_pos_enabled(&mut self) -> bool {
        false
    }
}

/// One of lread.c's saved_strings: the bytes `#@' skipped in a file while
/// `load-force-doc-strings' was set, and the file position they start at.
struct SavedString {
    position: usize,
    bytes: Vec<u8>,
}

thread_local! {
    /// lread.c saved_strings: the last two, the most recent first.
    static SAVED_STRINGS: std::cell::RefCell<[Option<SavedString>; 2]> =
        const { std::cell::RefCell::new([None, None]) };
}

/// skip_lazy_string's record of a skipped doc string.
fn save_lazy_string(position: usize, bytes: Vec<u8>) {
    SAVED_STRINGS.with_borrow_mut(|saved| {
        saved.swap(0, 1);
        saved[0] = Some(SavedString { position, bytes });
    });
}

/// get_lazy_string from the saved strings: the text at file POSITION up to
/// its ^_, the ^A quoting undone in the saved bytes themselves, as GNU's
/// loop rewrites them (a second reference to the same text reads what the
/// first left); None when no saved string holds it.
fn saved_lazy_string(position: usize) -> Option<Vec<u8>> {
    SAVED_STRINGS.with_borrow_mut(|saved| {
        let saved = saved.iter_mut().flatten().find(|saved| {
            position >= saved.position && position - saved.position < saved.bytes.len()
        })?;
        let bytes = &mut saved.bytes;
        let start = position - saved.position;
        let (mut from, mut to) = (start, start);
        while let Some(&byte) = bytes.get(from).filter(|byte| **byte != 0o37) {
            from += 1;
            let character = if byte == 1 {
                let Some(&quoted) = bytes.get(from) else {
                    break;
                };
                from += 1;
                match quoted {
                    b'0' => 0,
                    b'_' => 0o37,
                    other => other,
                }
            } else {
                byte
            };
            bytes[to] = character;
            to += 1;
        }
        Some(bytes[start..to].to_vec())
    })
}

thread_local! {
    /// The `load's of a file stream running (lread.c's `infile' set):
    /// bytecode_from_rev_list reads lazily loaded code for any read while
    /// one runs, from a string as from the file.
    static INFILE_LOADS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Fload's `infile = &input' for a file stream, given back as
/// close_infile_unwind does when the guard goes.
pub(crate) struct InfileLoad(());

impl InfileLoad {
    pub(crate) fn enter() -> Self {
        INFILE_LOADS.with(|loads| loads.set(loads.get() + 1));
        InfileLoad(())
    }
}

impl Drop for InfileLoad {
    fn drop(&mut self) {
        INFILE_LOADS.with(|loads| loads.set(loads.get().saturating_sub(1)));
    }
}

fn infile_load_running() -> bool {
    INFILE_LOADS.with(|loads| loads.get() > 0)
}

/// Fload's end: the strings saved while it read are freed.
pub(crate) fn forget_saved_lazy_strings() {
    SAVED_STRINGS.with_borrow_mut(|saved| *saved = [None, None]);
}

fn interpreted_closure_code_syntax(value: &Value) -> bool {
    let Some((_, cdr)) = (value).cons_cells() else {
        return false;
    };
    // Before Emacs 30, lazily loaded BYTECODE could occupy this slot as a
    // (FILE . OFFSET) pair.  It is still bytecode, whereas an interpreted
    // closure's code slot is a (proper) list of body forms.
    !matches!(cdr.get().kind(), Kind::Integer(_))
}

/// The host character an encoded input's character CODE is held as: a raw
/// byte as its sentinel, a code past Unicode as the invalid sentinel.
fn encoded_code_char(code: u32) -> char {
    if (0x3fff80..=0x3fffff).contains(&code) {
        encode_raw_byte(code as u8)
    } else {
        char::from_u32(code).unwrap_or(INVALID_UNICODE_SENTINEL)
    }
}

/// The objects substitute_object_recurse does not look inside: symbols
/// (a symbol with position too, SYMBOLP's, only while
/// `symbols-with-pos-enabled' is non-nil, SYMBOLS_WITH_POS), numbers and
/// strings without text properties.
fn atomic_for_substitution(value: &Value, symbols_with_pos: bool) -> bool {
    match value.kind() {
        Kind::Nil
        | Kind::T
        | Kind::Symbol(_)
        | Kind::Integer(_)
        | Kind::BigInteger(_)
        | Kind::Float(_) => true,
        Kind::SymbolWithPos(_) => symbols_with_pos,
        Kind::StringObject(string) => !string.borrow().props.has_intervals(),
        _ => false,
    }
}

/// lread.c's struct subst: the object a `#N=' label stands for, the
/// placeholder `#N#' read as inside it, the labelled objects (a cycle can
/// be entered only through one of them) and those of them already seen.
struct Substitution<'c> {
    object: Value,
    placeholder: Value,
    completed: &'c HashSet<usize, IdentityBuildHasher>,
    seen: HashSet<usize, IdentityBuildHasher>,
    /// symbols_with_pos_enabled, under which SYMBOLP takes a symbol with
    /// position.
    symbols_with_pos: bool,
    /// The walk's signal (a symbol with position met while SYMBOLP takes
    /// none, an unknown pseudovector to it): the walk stops, and the read
    /// signals it.
    failure: Option<LispError>,
}

impl Substitution<'_> {
    /// substitute_object_recurse: SUBTREE, with each placeholder in it
    /// replaced by the object, in place.
    fn recurse(&mut self, subtree: Value) -> Value {
        if self.failure.is_some() {
            return subtree;
        }
        if subtree.eq_value(self.placeholder) {
            return self.object;
        }
        if atomic_for_substitution(&subtree, self.symbols_with_pos) || !self.enter(&subtree) {
            return subtree;
        }
        match subtree.kind() {
            Kind::Cons(_) => self.recurse_list(subtree),
            // An unknown pseudovector: `wrong_type_argument (Qsequencep,
            // subtree)', as Flength once signaled.
            Kind::SymbolWithPos(_) => {
                self.failure = Some(LispError::WrongTypeArgument("sequencep".into(), subtree));
            }
            Kind::Vector(vector) => {
                for index in 0..vector.len() {
                    let slot = vector.get(index).expect("an index below the length");
                    vector.set(index, self.recurse(slot));
                }
            }
            Kind::Closure(closure) => {
                for (index, slot) in closure.slots().collect::<Vec<_>>().into_iter().enumerate() {
                    closure.initialize_slot(index, self.recurse(slot));
                }
            }
            Kind::LispRecord(record) => {
                for (index, slot) in record.slots().collect::<Vec<_>>().into_iter().enumerate() {
                    record.set(index, self.recurse(slot));
                }
            }
            Kind::CharTable(table) => {
                for (index, slot) in table.slots().collect::<Vec<_>>().into_iter().enumerate() {
                    table.set_slot(index, self.recurse(slot));
                }
            }
            Kind::SubCharTable(table) => {
                for (index, slot) in table.slots().collect::<Vec<_>>().into_iter().enumerate() {
                    table.set_slot(index, self.recurse(slot));
                }
            }
            Kind::StringObject(string) => {
                // substitute_in_interval: each interval's plist.
                let intervals = string.borrow_mut().props.take();
                if let Some(mut intervals) = intervals {
                    intervals.rewrite_plists(|plist| self.recurse(plist));
                    string.borrow_mut().props.set(Some(intervals));
                }
            }
            Kind::ReaderForm(form) => {
                // A literal read without a context, its fields still open.
                let len = reader_form_fields(&form).len();
                for index in 0..len {
                    let field = reader_form_fields(&form)[index];
                    let field = self.recurse(field);
                    // SAFETY: the reader made the form and holds no other
                    // reference to its fields while this store runs.
                    unsafe { set_reader_form_field(&form, index, field) };
                }
            }
            // GNU 30's hash tables have no Lisp slots in their header (the
            // walk leaves their entries alone), nor do bool vectors and
            // the other kinds.
            _ => {}
        }
        subtree
    }

    /// Whether to look inside SUBTREE: not when it was seen, and a
    /// labelled object is remembered as seen as it is entered.
    fn enter(&mut self, subtree: &Value) -> bool {
        let word = subtree.word();
        if self.seen.contains(&word) {
            return false;
        }
        if self.completed.contains(&word) {
            self.seen.insert(word);
        }
        true
    }

    /// The cons case, along the cdrs without a Rust frame per element: the
    /// car, then the cdr, as GNU's recursion does them.
    fn recurse_list(&mut self, list: Value) {
        let mut cursor = list;
        while let Some((car, cdr)) = cursor.cons_cells() {
            car.set(self.recurse(car.get()));
            let tail = cdr.get();
            if tail.eq_value(self.placeholder) {
                cdr.set(self.object);
                return;
            }
            if !matches!(tail.kind(), Kind::Cons(_)) {
                cdr.set(self.recurse(tail));
                return;
            }
            if !self.enter(&tail) {
                return;
            }
            cursor = tail;
        }
    }
}

/// The fields of a literal read without a context.
fn reader_form_fields(form: &crate::lisp::types::ReaderFormRef) -> &[Value] {
    match form.as_ref() {
        ReaderForm::HashTable { fields }
        | ReaderForm::CharTable { fields }
        | ReaderForm::SubCharTable { fields }
        | ReaderForm::Record { slots: fields }
        | ReaderForm::Closure { slots: fields, .. } => fields,
    }
}

/// Store VALUE as field INDEX of a literal read without a context.
///
/// # Safety
///
/// No reference to the form's fields is alive while the store runs.
unsafe fn set_reader_form_field(
    form: &crate::lisp::types::ReaderFormRef,
    index: usize,
    value: Value,
) {
    // SAFETY: the caller's guarantee; the payload is the form's.
    match unsafe { &mut *form.as_ptr() } {
        ReaderForm::HashTable { fields }
        | ReaderForm::CharTable { fields }
        | ReaderForm::SubCharTable { fields }
        | ReaderForm::Record { slots: fields }
        | ReaderForm::Closure { slots: fields, .. } => fields[index] = value,
    }
}

/// Whether a parsed object still contains reader forms requiring runtime
/// allocation. Resolve these once at the read boundary, as lread.c does,
/// before evaluation or returning the object from a Lisp reader primitive.
pub(crate) fn read_object_needs_resolution(value: &Value) -> bool {
    let mut seen = std::collections::HashSet::new();
    let mut seen_vectors = std::collections::HashSet::new();
    let mut stack = vec![*value];
    while let Some(current) = stack.pop() {
        match current.kind() {
            Kind::ReaderForm(_) => return true,
            Kind::Cons(cons_cell) => {
                let car_cell = &cons_cell.car;
                let cdr_cell = &cons_cell.cdr;
                let ptr = crate::lisp::types::ConsCell::identity(&cons_cell);
                if !seen.insert(ptr) {
                    continue;
                }
                stack.push(car_cell.get());
                stack.push(cdr_cell.get());
            }
            Kind::Vector(vector) if seen_vectors.insert(vector.identity()) => {
                stack.extend(vector.slots());
            }
            Kind::StringObject(state) => {
                if let Some(intervals) = state.borrow().props.intervals() {
                    stack.extend(intervals.plists());
                }
            }
            _ => {}
        }
    }
    false
}

fn encode_raw_byte(byte: u8) -> char {
    if byte < 0x80 {
        return byte as char;
    }
    char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
        .expect("raw byte regex marker is a valid private-use character")
}

fn raw_byte_from_source_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if (RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&code) {
        Some((code - RAW_BYTE_REGEX_BASE) as u8)
    } else {
        None
    }
}

pub(crate) fn apply_symbol_shorthands_to_token(
    token: String,
    symbol_shorthands: &[(String, String)],
) -> String {
    // GNU deliberately preserves the traditional punctuation-only core
    // symbols even when one of their characters is configured as a
    // shorthand prefix.  For example, a `-` shorthand may expand `-foo`,
    // but must not rename `/=` or `---`.
    if token.bytes().all(|byte| {
        matches!(
            byte,
            b'^' | b'*' | b'+' | b'-' | b'/' | b'<' | b'=' | b'>' | b'_' | b'|'
        )
    }) {
        return token;
    }
    for (short, long) in symbol_shorthands {
        if let Some(rest) = token.strip_prefix(short) {
            return format!("{long}{rest}");
        }
    }
    token
}

/// An entry of read0's read stack: the syntactic context the next object
/// read goes into, the innermost entry last, so nested data takes heap
/// rather than Rust frames, as lread.c's rdstack does.  The values an
/// entry holds stay rooted on the read root stack from its BASE, as
/// mark_lread marks rdstack's entries: a list's first cons, a sequence's
/// elements, a label's placeholder.
struct ReadFrame {
    entry: ReadEntry,
    base: usize,
}

/// lread.c's read_entry_type, with each type's data.
enum ReadEntry {
    /// RE_list_start (no cons yet), RE_list and, once DOTTED, RE_list_dot:
    /// the list's first and last conses.
    List {
        cells: Option<(Value, Value)>,
        dotted: bool,
    },
    /// RE_vector, RE_record, RE_char_table, RE_sub_char_table,
    /// RE_byte_code and RE_string_props: the elements are the read roots
    /// from the entry's base on; OLD_LOCATE is the locate_syms the opener
    /// saved.
    Sequence {
        syntax: SequenceSyntax,
        old_locate: bool,
    },
    /// RE_special: `'', `#'', `\`', `,' or `,@', the object read making
    /// (SYMBOL OBJECT).
    Special { symbol: &'static str },
    /// RE_numbered: `#N=', with the label's placeholder.
    Numbered { id: u64, placeholder: Value },
}

/// The bracketed syntaxes whose elements read0 collects.
#[derive(Clone, Copy)]
enum SequenceSyntax {
    /// `[': a vector.
    Vector,
    /// `#s(': a record or a hash table.
    Record,
    /// `#^[': a char-table.
    CharTable,
    /// `#^^[': a sub-char-table.
    SubCharTable,
    /// `#[': a byte-code function or an interpreted closure.
    ByteCode,
    /// `#(': a string with text properties.
    StringProps,
}

impl SequenceSyntax {
    /// The character that closes the syntax.
    fn closer(self) -> u8 {
        match self {
            SequenceSyntax::Record | SequenceSyntax::StringProps => b')',
            _ => b']',
        }
    }
}

/// What read0's read_obj makes of the syntax at point: an object, a read
/// stack entry for the objects that follow, nothing yet (`#!' and `#@'
/// skip text and read on), or the end of the input.
enum ReadStep {
    Object(Value),
    Open(ReadEntry),
    Again,
    End,
}

impl ReadStep {
    /// The step of an object a reading function returned, None at the end
    /// of the input.
    fn of(value: Option<Value>) -> Self {
        value.map_or(ReadStep::End, ReadStep::Object)
    }
}

/// A simple s-expression reader. Handles the subset of Elisp syntax
/// that appears in ERT test files: atoms, lists, strings, quotes,
/// backquote, characters, and comments.
pub struct Reader<'a> {
    input: Input<'a>,
    /// The bytes of a text or encoded input, read at POS directly (empty
    /// for a stream or a buffer's text, read through INPUT).
    slice: &'a [u8],
    pos: usize,
    symbol_shorthands: Vec<(String, String)>,
    context: Option<&'a mut dyn ReadContext>,
    /// read0's LOCATE_SYMS: allocate a symbol-with-position for each
    /// occurrence except the canonical nil symbol.
    locate_symbols: bool,
    /// Added to each recorded character offset (a buffer read reports
    /// buffer positions, a string read zero-based string offsets).
    position_base: i64,
    /// Monotonic byte→char cursor so positions cost O(n) over the input.
    position_cursor_byte: usize,
    position_cursor_char: i64,
    /// Whether any datum read so far is a `ReaderForm' placeholder (a
    /// circular label or reference, a hash-table, record, char-table,
    /// bool-vector or closure literal): only then
    /// does the read result need Interpreter-owned object materialization.
    emitted_reader_forms: bool,
    /// read0's read_objects_map: each `#N=' label of this top-level read,
    /// mapped to its placeholder until the labelled object is complete and
    /// to the object after; a `#N#' naming no label is invalid syntax.
    read_objects_map: HashMap<u64, Value, IdentityBuildHasher>,
    /// read0's read_objects_completed: the objects labelled so far, by
    /// word, through which alone a substitution walk can enter a cycle.
    read_objects_completed: HashSet<usize, IdentityBuildHasher>,
    /// The `read' calls in progress: a top-level read leaves the label
    /// maps empty, as readevalloop and read_internal_start do.
    read_depth: usize,
    /// FROM_FILE_P: the bytes of a file `load' reads directly, where `#@'
    /// skips by byte count and lazily loaded code is read at once.
    from_file: bool,
    /// What lread.c's readchar reports through READCHAR_REPORT_MULTIBYTE
    /// for the source's characters: a multibyte string or buffer, or a
    /// file `load' reads, but not a unibyte string or buffer or a
    /// function.  read0 makes a new symbol's name of this multibyteness.
    multibyte_source: bool,
    /// lread.c's rdstack, as mark_lread marks it: each completed datum of
    /// a read in progress stays rooted while a later one is read, for Lisp
    /// can run in the middle of a read (a function stream's callbacks,
    /// `char-from-name' for a `\N{NAME}' escape) and collect.  A completed
    /// entry's object replaces the values it held here.  The reader's own,
    /// so that reads suspended in different Lisp threads (a stream's
    /// callback yielding) keep their entries apart.
    roots: crate::lisp::alloc::RootedVec<Value>,
}

impl<'a> Reader<'a> {
    pub fn new(input: &'a str) -> Self {
        Self::with_symbol_shorthands(input, Vec::new())
    }

    pub fn with_symbol_shorthands(
        input: &'a str,
        symbol_shorthands: Vec<(String, String)>,
    ) -> Self {
        Reader {
            input: Input::Text(input.as_bytes()),
            slice: input.as_bytes(),
            pos: 0,
            symbol_shorthands,
            context: None,
            // GNU's reader always encodes quote shorthands with the raw
            // `\``/`\,'/`\,@' symbols; pcase.el's pattern expanders are
            // registered under those names.
            emitted_reader_forms: false,
            read_objects_map: HashMap::default(),
            read_objects_completed: HashSet::default(),
            read_depth: 0,
            roots: crate::lisp::alloc::RootedVec::new(),
            locate_symbols: false,
            position_base: 0,
            position_cursor_byte: 0,
            position_cursor_char: 0,
            from_file: false,
            multibyte_source: false,
        }
    }

    /// This reader with readchar's report for its source's characters
    /// set to MULTIBYTE: a buffer's text read through its internal bytes.
    pub(crate) fn reporting_multibyte(mut self, multibyte: bool) -> Self {
        self.multibyte_source = multibyte;
        self
    }

    /// A reader of text `load' decodes into a multibyte buffer
    /// (load-with-code-conversion), whose characters readchar reports as
    /// multibyte.
    pub(crate) fn from_multibyte_text(
        input: &'a str,
        symbol_shorthands: Vec<(String, String)>,
    ) -> Self {
        let mut reader = Self::with_symbol_shorthands(input, symbol_shorthands);
        reader.multibyte_source = true;
        reader
    }

    pub(crate) fn from_stream(stream: &'a mut dyn ReaderStream, locate_symbols: bool) -> Self {
        let mut reader = Self::new("");
        reader.input = Input::function(stream);
        reader.locate_symbols = locate_symbols;
        reader
    }

    pub(crate) fn from_encoded(
        bytes: &'a [u8],
        multibyte: bool,
        symbol_shorthands: Vec<(String, String)>,
    ) -> Self {
        let mut reader = Self::with_symbol_shorthands("", symbol_shorthands);
        reader.input = Input::Encoded {
            bytes,
            multibyte,
            lenient: false,
        };
        reader.slice = bytes;
        reader.multibyte_source = multibyte;
        reader
    }

    /// Fload's direct read of a file (readevalloop on Qget_file_char): its
    /// BYTES from byte START, each character as readchar's read_multibyte
    /// takes it.
    pub(crate) fn from_file(
        bytes: &'a [u8],
        start: usize,
        symbol_shorthands: Vec<(String, String)>,
    ) -> Self {
        let mut reader = Self::from_text_bytes(bytes, symbol_shorthands);
        reader.pos = start;
        reader.position_cursor_byte = start;
        reader.from_file = true;
        reader
    }

    /// Text in Emacs's internal representation that need not be well
    /// formed, read as decode-coding-string's `utf-8-emacs' would hold it:
    /// a byte that begins no whole character is a raw byte.
    pub(crate) fn from_text_bytes(
        bytes: &'a [u8],
        symbol_shorthands: Vec<(String, String)>,
    ) -> Self {
        let mut reader = Self::with_symbol_shorthands("", symbol_shorthands);
        reader.input = Input::Encoded {
            bytes,
            multibyte: true,
            lenient: true,
        };
        reader.slice = bytes;
        reader.multibyte_source = true;
        reader
    }

    /// A buffer's text, read in place in its two parts; MULTIBYTE is the
    /// buffer's `enable-multibyte-characters'.
    pub(crate) fn from_buffer_parts(
        parts: crate::lisp::regex_emacs::GapRange<'a>,
        multibyte: bool,
        symbol_shorthands: Vec<(String, String)>,
    ) -> Self {
        let mut reader = Self::with_symbol_shorthands("", symbol_shorthands);
        reader.input = Input::buffer(parts);
        reader.multibyte_source = multibyte;
        reader
    }

    /// `read-positioning-symbols' on a buffer's text, as on encoded text.
    pub(crate) fn from_buffer_parts_positioned(
        parts: crate::lisp::regex_emacs::GapRange<'a>,
        multibyte: bool,
        symbol_shorthands: Vec<(String, String)>,
        position_base: i64,
    ) -> Self {
        let mut reader = Self::from_buffer_parts(parts, multibyte, symbol_shorthands);
        reader.locate_symbols = true;
        reader.position_base = position_base;
        reader
    }

    /// `read-positioning-symbols' on an encoded source: each symbol
    /// occurrence gets its character position, offset by BASE.
    pub(crate) fn from_encoded_positioned(
        bytes: &'a [u8],
        multibyte: bool,
        symbol_shorthands: Vec<(String, String)>,
        position_base: i64,
    ) -> Self {
        let mut reader = Self::from_encoded(bytes, multibyte, symbol_shorthands);
        reader.locate_symbols = true;
        reader.position_base = position_base;
        reader
    }

    pub(crate) fn characters_consumed(&mut self) -> usize {
        (self.character_position() - self.position_base) as usize
    }

    pub(crate) fn finish_stream(&mut self) -> Result<(), LispError> {
        self.input.unread_lookahead()
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    /// Resolve atom names through the active obarray and build literal
    /// objects as lread.c's oblookup/intern_driver and read0 do. The
    /// borrowed context avoids a second object graph and post-read walks.
    pub(crate) fn with_read_context(mut self, context: &'a mut dyn ReadContext) -> Self {
        self.context = Some(context);
        self
    }

    /// The Lisp world the reader asks: the context it was given, else a
    /// function stream's.
    fn read_context(&mut self) -> Option<&mut dyn ReadContext> {
        if let Some(context) = self.context.as_deref_mut() {
            return Some(context);
        }
        match &mut self.input {
            Input::Function(input) => Some(&mut *input.stream),
            _ => None,
        }
    }

    /// read0's `#$': the value of `load-file-name'.
    fn load_file_name(&mut self) -> Value {
        self.read_context()
            .map_or(Value::Nil, |context| context.load_file_name())
    }

    /// lread.c get_lazy_string: the unibyte string the (FILE . POSITION)
    /// REFERENCE names, from the doc strings `#@' saved in this load,
    /// else read from FILE.
    fn get_lazy_string(&mut self, reference: Value) -> Result<Value, LispError> {
        let position = reference
            .cons_values()
            .and_then(|(_, position)| position.integer_value())
            .map_or(0, i64::unsigned_abs);
        if let Some(text) = usize::try_from(position).ok().and_then(saved_lazy_string) {
            return Ok(Value::StringObject(StringObjectRef::from_unibyte(text)));
        }
        match self.read_context() {
            Some(context) => context.doc_string(reference),
            None => Ok(Value::Nil),
        }
    }

    /// bytecode_from_rev_list's reading of lazily loaded byte-code (the
    /// `byte-compile-dynamic' of Emacs before 30) from a file: its code
    /// slot a (FILE . POSITION) reference is read now, and code whose
    /// constants slot is nil is the printed (BYTECODE . CONSTANTS) of
    /// that string, decoded as `utf-8-emacs'.
    fn read_lazy_closure_slots(&mut self, slots: &mut [Value]) -> Result<(), LispError> {
        if slots[1]
            .cons_values()
            .is_some_and(|(_, position)| matches!(position.kind(), Kind::Integer(_)))
        {
            slots[1] = self.get_lazy_string(slots[1])?;
        }
        let Some(constants) = slots.get(2) else {
            return Ok(());
        };
        if !constants.is_nil() {
            return Ok(());
        }
        let Kind::StringObject(code) = slots[1].kind() else {
            return Ok(());
        };
        let bytes = code.borrow().bytes().to_vec();
        let shorthands = self.symbol_shorthands.clone();
        let pair = {
            let mut reader = Reader::from_text_bytes(&bytes, shorthands);
            if let Some(context) = self.read_context() {
                reader = reader.with_read_context(context);
            }
            reader.read()?
        };
        let Some((code, constants)) = pair.and_then(|pair| pair.cons_values()) else {
            return Err(LispError::ReadError("Invalid byte-code object".into()));
        };
        slots[1] = code;
        slots[2] = constants;
        Ok(())
    }

    fn resolve_symbol(&mut self, name: &str) -> Result<Value, LispError> {
        if let Input::Function(input) = &mut self.input {
            return input.stream.intern_symbol(name, false);
        }
        // `##' interns lread.c's empty_unibyte_string.
        let multibyte = self.multibyte_source && !name.is_empty();
        match self.context.as_mut() {
            Some(context) => context.intern(name, multibyte),
            None => Ok(Value::symbol(name)),
        }
    }

    /// read0's RE_numbered: OBJECT, read after `#N=', replaces PLACEHOLDER.
    /// A cons takes the placeholder's place itself (its car and cdr copied
    /// in), as the `#N#' inside it already are the placeholder; any other
    /// object is put everywhere the placeholder is inside it.
    fn complete_numbered(
        &mut self,
        id: u64,
        placeholder: Value,
        object: Value,
    ) -> Result<Value, LispError> {
        if let Some((car, cdr)) = object.cons_values() {
            if object.eq_value(placeholder) {
                // Catch silly games like #1=#1#.
                return Err(LispError::ReadError("nonsensical self-reference".into()));
            }
            let (placeholder_car, placeholder_cdr) =
                placeholder.cons_cells().expect("a placeholder is a cons");
            placeholder_car.set(car);
            placeholder_cdr.set(cdr);
            self.read_objects_completed.insert(placeholder.word());
            return Ok(placeholder);
        }
        // If it can be recursive, remember it for future substitutions.
        let symbols_with_pos = self
            .read_context()
            .is_some_and(|context| context.symbols_with_pos_enabled());
        if !atomic_for_substitution(&object, symbols_with_pos) {
            self.read_objects_completed.insert(object.word());
        }
        let mut substitution = Substitution {
            object,
            placeholder,
            completed: &self.read_objects_completed,
            seen: HashSet::default(),
            symbols_with_pos,
            failure: None,
        };
        substitution.recurse(object);
        if let Some(failure) = substitution.failure {
            return Err(failure);
        }
        self.read_objects_map.insert(id, object);
        Ok(object)
    }

    /// The object a literal's syntax stands for, built now (read0); without
    /// a context the placeholder waits for the interpreter.
    fn literal_object(&mut self, form: ReaderForm) -> Result<Value, LispError> {
        if let Some(context) = self.read_context() {
            return context.build(form);
        }
        Ok(Value::ReaderForm(
            crate::lisp::alloc::VectorlikeRef::allocate(form),
        ))
    }

    fn peek(&mut self) -> Result<Option<u8>, LispError> {
        self.peek_at(0)
    }

    #[inline]
    fn peek_at(&mut self, offset: usize) -> Result<Option<u8>, LispError> {
        if let Some(&byte) = self.slice.get(self.pos + offset) {
            return Ok(Some(byte));
        }
        self.input.peek(self.pos, offset)
    }

    #[inline]
    fn advance(&mut self) -> Result<Option<u8>, LispError> {
        if let Some(&byte) = self.slice.get(self.pos) {
            self.pos += 1;
            return Ok(Some(byte));
        }
        let byte = self.input.advance(self.pos)?;
        self.pos += usize::from(byte.is_some());
        Ok(byte)
    }

    fn advance_bytes(&mut self, count: usize) -> Result<(), LispError> {
        for _ in 0..count {
            self.advance()?;
        }
        Ok(())
    }

    fn retreat_ascii(&mut self, byte: u8) {
        self.pos -= 1;
        self.input.retreat_ascii(byte);
    }

    fn peek_char(&mut self) -> Result<Option<char>, LispError> {
        if let Input::Text(bytes) | Input::Encoded { bytes, .. } = &self.input
            && let Some(&byte) = bytes.get(self.pos)
            && byte < 0x80
        {
            return Ok(Some(byte as char));
        }
        if matches!(self.input, Input::Encoded { .. }) {
            return Ok(self
                .input
                .encoded_character(self.pos)?
                .map(|(code, _)| encoded_code_char(code)));
        }
        let Some(first) = self.peek()? else {
            return Ok(None);
        };
        let len = if first < 0x80 {
            1
        } else if first < 0xE0 {
            2
        } else if first < 0xF0 {
            3
        } else {
            4
        };
        let mut bytes = [0; 4];
        bytes[0] = first;
        for (index, byte) in bytes.iter_mut().enumerate().take(len).skip(1) {
            let Some(next) = self.peek_at(index)? else {
                return Ok(None);
            };
            *byte = next;
        }
        Ok(std::str::from_utf8(&bytes[..len])
            .ok()
            .and_then(|s| s.chars().next()))
    }

    fn advance_char(&mut self) -> Result<Option<char>, LispError> {
        // An encoded input's character, decoded once.
        if let Some((code, width)) = self.input.encoded_character(self.pos)? {
            self.pos += width;
            return Ok(Some(encoded_code_char(code)));
        }
        let character = self.peek_char()?;
        if let Some(character) = character {
            let width = self
                .input
                .encoded_character(self.pos)?
                .map_or(character.len_utf8(), |(_, width)| width);
            self.advance_bytes(width)?;
        }
        Ok(character)
    }

    fn skip_whitespace_and_comments(&mut self) -> Result<(), LispError> {
        loop {
            // Skip whitespace: ASCII a byte at a time (in place when the
            // input's bytes are at hand), NO-BREAK SPACE as a character.
            let slice: &'a [u8] = self.slice;
            while slice.get(self.pos).is_some_and(|byte| *byte <= b' ') {
                self.pos += 1;
            }
            while let Some(byte) = self.peek()? {
                if byte <= b' ' {
                    self.advance()?;
                } else {
                    break;
                }
            }
            while let Some(ch) = self.peek_char()? {
                if ch <= ' ' || ch == '\u{a0}' {
                    self.advance_char()?;
                } else {
                    break;
                }
            }
            // Skip line comments (;)
            if self.peek()? == Some(b';') {
                if let Some(next) = self.input.skip_line(self.pos) {
                    self.pos = next;
                    continue;
                }
                while let Some(ch) = self.advance()? {
                    if ch == b'\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// The depth of the read root stack.
    #[inline]
    fn roots_depth(&self) -> usize {
        self.roots.len()
    }

    /// The read roots back to DEPTH, the depth before a datum's read, and
    /// that datum, VALUE, rooted in its children's place.
    #[inline]
    fn root_read_result(&mut self, depth: usize, value: Option<Value>) {
        self.roots.truncate(depth);
        if let Some(value) = value {
            self.roots.push(value);
        }
    }

    /// Read one s-expression. Returns None at end of input.
    pub fn read(&mut self) -> Result<Option<Value>, LispError> {
        let roots = self.roots_depth();
        self.read_depth += 1;
        let result = self.read_datum();
        self.read_depth -= 1;
        if self.read_depth == 0 && !self.read_objects_map.is_empty() {
            self.read_objects_map.clear();
            self.read_objects_completed.clear();
        }
        // A datum within another stays rooted until its parent replaces
        // it; a whole one goes to the caller, as read0 leaves rdstack at
        // its base.
        let datum = result.as_ref().ok().copied().flatten();
        self.root_read_result(roots, datum.filter(|_| self.read_depth != 0));
        let value = result?;
        // Every placeholder is the value of some `read' call, nested ones
        // included (lists, vectors and quotes read their elements here).
        if matches!(value.map(|v| v.kind()), Some(Kind::ReaderForm(_))) {
            self.emitted_reader_forms = true;
        }
        Ok(value)
    }

    /// Whether a datum read so far contains a placeholder that
    /// the Interpreter must materialize. False for the
    /// common marker-free text, whose value is final as read.
    pub fn emitted_reader_forms(&self) -> bool {
        self.emitted_reader_forms
    }

    /// read0: one object, the data nested in it read on the read stack.
    /// None at the end of the input before any object.
    fn read_datum(&mut self) -> Result<Option<Value>, LispError> {
        let mut stack = Vec::new();
        let locate_symbols = self.locate_symbols;
        let result = self.read_object(&mut stack);
        // The entries an error leaves go, and the locate_syms they saved
        // with them (read0's locals).
        if result.is_err() {
            self.locate_symbols = locate_symbols;
        }
        result
    }

    /// read0's loop: read_obj reads an object or pushes an entry for the
    /// objects that follow it, and each object read goes into the
    /// innermost entry, an entry it completes giving the object for the
    /// next, until the stack is empty.
    fn read_object(&mut self, stack: &mut Vec<ReadFrame>) -> Result<Option<Value>, LispError> {
        // Whether read0 has consumed syntax yet (a `#!' line or a `#@'
        // skip): from then on the end of the input is end_of_file_error,
        // as it is inside any entry.
        let mut consumed = false;
        loop {
            let mut obj = match self.read_step(stack)? {
                ReadStep::Object(obj) => obj,
                ReadStep::Open(entry) => {
                    let base = self.roots_depth();
                    if let ReadEntry::Numbered { placeholder, .. } = entry {
                        self.root_read_result(base, Some(placeholder));
                    }
                    stack.push(ReadFrame { entry, base });
                    continue;
                }
                ReadStep::Again => {
                    consumed = true;
                    continue;
                }
                ReadStep::End if stack.is_empty() && !consumed => return Ok(None),
                ReadStep::End => return Err(LispError::EndOfInput()),
            };
            // We have read an object in OBJ: the stack decides what to do
            // with it.
            loop {
                // Every placeholder is an object read here, nested ones
                // included.
                if matches!(obj.kind(), Kind::ReaderForm(_)) {
                    self.emitted_reader_forms = true;
                }
                let Some(frame) = stack.last_mut() else {
                    return Ok(Some(obj));
                };
                let base = frame.base;
                // OBJ stays rooted until its entry holds it: a sequence's
                // element until the sequence is complete.
                self.roots.push(obj);
                match &mut frame.entry {
                    ReadEntry::List {
                        cells: cells @ None,
                        ..
                    } => {
                        let cell = Value::cons(obj, Value::Nil);
                        *cells = Some((cell, cell));
                        self.root_read_result(base, Some(cell));
                        break;
                    }
                    ReadEntry::List {
                        cells: Some((_, tail)),
                        dotted: false,
                    } => {
                        let cell = Value::cons(obj, Value::Nil);
                        tail.cons_cells().expect("a list cell").1.set(cell);
                        *tail = cell;
                        // The list's first cons holds it.
                        self.root_read_result(base + 1, None);
                        break;
                    }
                    ReadEntry::List {
                        cells: Some((head, tail)),
                        dotted: true,
                    } => {
                        let (head, tail) = (*head, *tail);
                        // RE_list_dot: skip_space_and_comments, which
                        // signals at the end, then the character read.
                        self.skip_whitespace_and_comments()?;
                        self.input.unread_lookahead()?;
                        match self.read_code()? {
                            Some(0x29) => {}
                            None => return Err(LispError::EndOfInput()),
                            Some(_) => {
                                return Err(LispError::ReadError("expected )".into()));
                            }
                        }
                        tail.cons_cells().expect("a list cell").1.set(obj);
                        stack.pop();
                        obj = self.list_dot_result(head)?;
                    }
                    ReadEntry::Sequence { .. } => break,
                    ReadEntry::Special { symbol } => {
                        let symbol = *symbol;
                        stack.pop();
                        obj = Value::list([Value::symbol(symbol), obj]);
                    }
                    ReadEntry::Numbered { id, placeholder } => {
                        let (id, placeholder) = (*id, *placeholder);
                        stack.pop();
                        obj = self.complete_numbered(id, placeholder, obj)?;
                    }
                }
                // The completed entry's object takes its roots' place.
                self.root_read_result(base, None);
            }
        }
    }

    /// read_obj: the syntax at point, after the whitespace and comments
    /// before it.
    fn read_step(&mut self, stack: &mut Vec<ReadFrame>) -> Result<ReadStep, LispError> {
        self.skip_whitespace_and_comments()?;
        Ok(match self.peek()? {
            None => ReadStep::End,
            Some(b'(') => {
                self.advance()?;
                ReadStep::Open(ReadEntry::List {
                    cells: None,
                    dotted: false,
                })
            }
            Some(b'[') => {
                self.advance()?;
                self.open_sequence(SequenceSyntax::Vector)
            }
            // read0 reads the closer before it looks at the stack.
            Some(close @ (b')' | b']')) => {
                self.advance()?;
                ReadStep::Object(self.close_entry(stack, close)?)
            }
            Some(b'"') => ReadStep::of(self.read_string()?),
            // lread.c: the heads are the symbols named "`", "," and ",@".
            Some(b'\'') => {
                self.advance()?;
                ReadStep::Open(ReadEntry::Special { symbol: "quote" })
            }
            Some(b'`') => {
                self.advance()?;
                ReadStep::Open(ReadEntry::Special { symbol: "`" })
            }
            Some(b',') => {
                self.advance()?;
                let symbol = if self.peek()? == Some(b'@') {
                    self.advance()?;
                    ",@"
                } else {
                    self.input.unread_lookahead()?;
                    ","
                };
                ReadStep::Open(ReadEntry::Special { symbol })
            }
            Some(b'?') => ReadStep::of(self.read_character()?),
            Some(b'#') => self.read_hash()?,
            Some(b'.') => {
                if !self.read_dot_separator()? {
                    // The dot lookahead has already happened: this atom is
                    // read directly.
                    return Ok(ReadStep::of(self.read_atom()?));
                }
                // A separate dot after a list's element starts its tail
                // (RE_list becomes RE_list_dot); anywhere else it is
                // invalid syntax.
                match stack.last_mut() {
                    Some(ReadFrame {
                        entry:
                            ReadEntry::List {
                                cells: Some(_),
                                dotted,
                            },
                        ..
                    }) if !*dotted => {
                        *dotted = true;
                        ReadStep::Again
                    }
                    _ => return Err(LispError::ReadError(".".into())),
                }
            }
            _ => ReadStep::of(self.read_atom()?),
        })
    }

    /// read0's `)' and `]': the object of the innermost entry they close,
    /// popped, its roots back to its base; for any other entry, or none,
    /// invalid syntax.
    fn close_entry(&mut self, stack: &mut Vec<ReadFrame>, close: u8) -> Result<Value, LispError> {
        let invalid = || LispError::ReadError(if close == b')' { ")" } else { "]" }.into());
        let Some(frame) = stack.last() else {
            return Err(invalid());
        };
        let base = frame.base;
        let obj = match frame.entry {
            ReadEntry::List { cells: None, .. } if close == b')' => Value::Nil,
            ReadEntry::List {
                cells: Some((head, _)),
                dotted: false,
            } if close == b')' => head,
            ReadEntry::Sequence { syntax, old_locate } if syntax.closer() == close => {
                self.locate_symbols = old_locate;
                let elements = self.roots[base..].to_vec();
                self.sequence_object(syntax, elements)?
            }
            _ => return Err(invalid()),
        };
        stack.pop();
        self.root_read_result(base, None);
        Ok(obj)
    }

    /// A sequence's read stack entry, the locate_syms it saves, which each
    /// opener but `[' then clears.
    fn open_sequence(&mut self, syntax: SequenceSyntax) -> ReadStep {
        let old_locate = self.locate_symbols;
        if !matches!(syntax, SequenceSyntax::Vector) {
            self.locate_symbols = false;
        }
        ReadStep::Open(ReadEntry::Sequence { syntax, old_locate })
    }

    /// The object a sequence's ELEMENTS make: vector_from_rev_list,
    /// record_from_list or hash_table_from_plist, char_table_from_rev_list,
    /// sub_char_table_from_rev_list, bytecode_from_rev_list and
    /// string_props_from_rev_list.
    fn sequence_object(
        &mut self,
        syntax: SequenceSyntax,
        elements: Vec<Value>,
    ) -> Result<Value, LispError> {
        match syntax {
            SequenceSyntax::Vector => Ok(Value::vector(elements)),
            SequenceSyntax::Record => {
                // RE_record: a `#s(' closed with no elements is invalid
                // syntax named "#s".
                let Some((&kind, fields)) = elements.split_first() else {
                    return Err(LispError::ReadError("#s".into()));
                };
                // `BASE_EQ (XCAR (elems), Qhash_table)'.
                if matches!(kind.kind(), Kind::Symbol(kind_name) if kind_name.is_named("hash-table"))
                {
                    let fields = fields.to_vec();
                    self.literal_object(ReaderForm::HashTable { fields })
                } else {
                    self.literal_object(ReaderForm::Record { slots: elements })
                }
            }
            SequenceSyntax::CharTable => {
                if !(68..=crate::lisp::alloc::vectors::char_tables::CHAR_TABLE_MAX_SLOTS)
                    .contains(&elements.len())
                {
                    return Err(LispError::ReadError("Invalid size char-table".into()));
                }
                self.literal_object(ReaderForm::CharTable { fields: elements })
            }
            SequenceSyntax::SubCharTable => {
                // lread.c sub_char_table_from_rev_list signals `error'.
                if elements.len() < 2 {
                    return Err(LispError::Signal("Invalid size of sub-char-table".into()));
                }
                let Some(Kind::Integer(depth @ 1..=3)) = elements.first().map(|v| v.kind()) else {
                    return Err(LispError::Signal("Invalid depth in sub-char-table".into()));
                };
                let expected_contents = [0usize, 16, 32, 128][depth as usize];
                if elements.len() != expected_contents + 2 {
                    return Err(LispError::Signal("Invalid size in sub-char-table".into()));
                }
                if !matches!(
                    elements.get(1).map(|v| v.kind()),
                    Some(Kind::Integer(0..=0x3f_ffff))
                ) {
                    return Err(LispError::Signal(
                        "Invalid minimum character in sub-char-table".into(),
                    ));
                }
                self.literal_object(ReaderForm::SubCharTable { fields: elements })
            }
            SequenceSyntax::ByteCode => {
                let mut fields = elements;
                if (self.from_file || infile_load_running()) && fields.len() >= 2 {
                    self.read_lazy_closure_slots(&mut fields)?;
                }
                // GNU uses the same `#[...]' syntax for both kinds of
                // closure pseudovector.  Byte-compiled functions have a
                // string in the code slot and at least four fields, while an
                // interpreted lexical closure has a cons-list body and can
                // contain only ARGS, BODY, and ENV.  The latter occurs in
                // ordinary `.elc' constants (notably cl-generic.elc), so it
                // must not be rejected as malformed bytecode.
                let interpreted =
                    fields.len() == 3 || fields.get(1).is_some_and(interpreted_closure_code_syntax);
                let valid_len = if interpreted {
                    (3..=6).contains(&fields.len())
                } else {
                    (4..=6).contains(&fields.len())
                };
                if !valid_len {
                    return Err(LispError::ReadError("Invalid byte-code object".into()));
                }
                self.literal_object(ReaderForm::Closure {
                    kind: if interpreted {
                        ReaderClosureKind::Interpreted
                    } else {
                        ReaderClosureKind::ByteCode
                    },
                    slots: fields,
                })
            }
            SequenceSyntax::StringProps => {
                // lread.c string_props_from_rev_list: the first element
                // must be a string, then each BEG END PLIST triple is
                // Fset_text_properties on the string read itself, in
                // order, a triple cut short invalid when it is reached.
                let Some((&string, mut properties)) = elements
                    .split_first()
                    .filter(|(first, _)| matches!(first.kind(), Kind::StringObject(_)))
                else {
                    return Err(LispError::ReadError("#".into()));
                };
                while !properties.is_empty() {
                    let [start, end, plist, rest @ ..] = properties else {
                        return Err(LispError::ReadError("Invalid string property list".into()));
                    };
                    crate::lisp::primitives::textprop::set_string_text_properties_value(
                        &string, start, end, *plist,
                    )?;
                    properties = rest;
                }
                Ok(string)
            }
        }
    }

    /// RE_list_dot's hack: a (#$ . FIXNUM) read while
    /// `load-force-doc-strings' is set is the doc string it names, at once.
    fn list_dot_result(&mut self, list: Value) -> Result<Value, LispError> {
        let lazy_head = list.cons_values().and_then(|(first, end)| {
            (!first.is_nil() && matches!(end.kind(), Kind::Integer(_))).then_some(first)
        });
        if let Some(head) = lazy_head
            && self.read_context().is_some_and(|context| {
                context.force_doc_strings() && context.load_file_name().eq_value(head)
            })
        {
            return self.get_lazy_string(list);
        }
        Ok(list)
    }

    /// Read all expressions from the input.
    /// Every form of the text, in a vector the collector scans while it
    /// lives: the forms read ahead of the evaluation of the first survive
    /// the collections that evaluation runs (lread.c reads one form at a
    /// time; a vector read ahead is rooted as SAFE_ALLOCA_LISP roots one).
    pub fn read_all(&mut self) -> Result<crate::lisp::alloc::RootedVec<Value>, LispError> {
        let mut forms = crate::lisp::alloc::RootedVec::new();
        while let Some(val) = self.read()? {
            forms.push(val);
        }
        Ok(forms)
    }

    fn apply_symbol_shorthands(&self, token: String) -> String {
        if matches!(self.input, Input::Function(_)) {
            token
        } else {
            apply_symbol_shorthands_to_token(token, &self.symbol_shorthands)
        }
    }

    /// lread.c:read0 always reads and unreads the character after a dot,
    /// even when the dot starts an ordinary symbol or number.
    fn read_dot_separator(&mut self) -> Result<bool, LispError> {
        self.advance()?;
        let next = self.peek_char()?;
        self.input.unread_lookahead()?;
        let separator = next.is_none_or(|ch| {
            ch <= ' '
                || ch == '\u{a0}'
                || matches!(ch, '"' | '\'' | ';' | '(' | '[' | '#' | '?' | '`' | ',')
        });
        if !separator {
            self.retreat_ascii(b'.');
        }
        Ok(separator)
    }

    fn read_string(&mut self) -> Result<Option<Value>, LispError> {
        self.advance()?; // consume opening '"'
        let mut s = String::new();
        let mut extended_chars = Vec::new();
        let mut has_explicit_multibyte = false;
        let mut has_raw_bytes = false;
        let mut has_invalid_unicode = false;
        let slice: &'a [u8] = self.slice;
        loop {
            // A run of ASCII other than the quote and the backslash, copied
            // in place when the input's bytes are at hand.
            let run = slice
                .get(self.pos..)
                .unwrap_or_default()
                .iter()
                .take_while(|byte| **byte < 0x80 && **byte != b'"' && **byte != b'\\')
                .count();
            if run > 0 {
                // SAFETY: the bytes are ASCII.
                s.push_str(unsafe {
                    std::str::from_utf8_unchecked(&slice[self.pos..self.pos + run])
                });
                self.pos += run;
            }
            match self.peek()? {
                None => return Err(LispError::EndOfInput()),
                Some(b'"') => {
                    self.advance()?;
                    // alloc.c canonicalizes every zero-length unibyte string
                    // to empty_unibyte_string.  It is the one reader literal
                    // whose identity is intentionally shared globally.
                    if s.is_empty()
                        && extended_chars.is_empty()
                        && !has_explicit_multibyte
                        && !has_invalid_unicode
                    {
                        return Ok(Some(Value::String("".into())));
                    }
                    // A Lisp string is an object even when it has no text
                    // properties.  Keeping ordinary source literals as the
                    // scalar `Value::String` shortcut loses object identity
                    // when a quoted list is traversed or macro-expanded.
                    // GNU's reader allocates the object once, so repeated
                    // evaluation of the same literal returns that object.
                    return Ok(Some(Value::StringObject(StringObjectRef::from_text(
                        s,
                        Default::default(),
                        has_explicit_multibyte || has_invalid_unicode,
                        extended_chars,
                    ))));
                }
                Some(b'\\') => {
                    self.advance()?;
                    // lread.c read_string_literal: the string-specific
                    // rules first (`\s' a space, `\SPC' and `\LF' nothing),
                    // then read_char_escape.
                    let next = self.read_code()?;
                    if next == Some(i64::from(b's')) {
                        s.push(' ');
                        continue;
                    }
                    if matches!(next, Some(0x20 | 0x0A)) {
                        continue;
                    }
                    let escaped = self.read_char_escape(next)?;
                    let mut modifiers = escaped & CHAR_MODIFIER_MASK;
                    let mut ch = escaped & !CHAR_MODIFIER_MASK;
                    if !char_byte8_p(ch) && ch < 0x80 {
                        // Allow `\C-SPC' and `\^SPC'.
                        if modifiers == CHAR_CTL && ch == i64::from(b' ') {
                            ch = 0;
                            modifiers = 0;
                        }
                        if modifiers & CHAR_SHIFT != 0 {
                            // Shift modifier is valid only with [A-Za-z].
                            if (i64::from(b'A')..=i64::from(b'Z')).contains(&ch) {
                                modifiers &= !CHAR_SHIFT;
                            } else if (i64::from(b'a')..=i64::from(b'z')).contains(&ch) {
                                ch -= i64::from(b'a' - b'A');
                                modifiers &= !CHAR_SHIFT;
                            }
                        }
                        if modifiers & CHAR_META != 0 {
                            // Move the meta bit to the right place for a
                            // string.
                            modifiers &= !CHAR_META;
                            ch = BYTE8_BASE + (ch | 0x80);
                        }
                    }
                    // Any modifiers remaining are invalid.
                    if modifiers != 0 {
                        return Err(LispError::ReadError("Invalid modifier in string".into()));
                    }
                    Self::push_string_character(
                        &mut s,
                        ch,
                        &mut has_explicit_multibyte,
                        &mut has_raw_bytes,
                        &mut has_invalid_unicode,
                        &mut extended_chars,
                    );
                }
                Some(ch) if ch < 0x80 => {
                    self.advance()?;
                    s.push(ch as char);
                }
                Some(_) => {
                    self.push_non_ascii_string_source(
                        &mut s,
                        &mut has_explicit_multibyte,
                        &mut has_raw_bytes,
                        &mut has_invalid_unicode,
                        &mut extended_chars,
                    )?;
                }
            }
        }
    }

    fn push_non_ascii_string_source(
        &mut self,
        text: &mut String,
        multibyte: &mut bool,
        raw_bytes: &mut bool,
        invalid_unicode: &mut bool,
        extended: &mut Vec<(usize, u32)>,
    ) -> Result<(), LispError> {
        let (code, character) = match self.input.encoded_character(self.pos)? {
            // An encoded input's character, decoded once.
            Some((code, width)) => {
                self.pos += width;
                (Some(i64::from(code)), encoded_code_char(code))
            }
            None => {
                let code = self.input.function_code()?;
                let character = self
                    .read_utf8_char()?
                    .ok_or_else(|| LispError::ReadError("invalid UTF-8 in string".into()))?;
                (code, character)
            }
        };
        if code.map_or_else(
            || raw_byte_from_source_char(character).is_some(),
            |code| (0x3fff80..=0x3fffff).contains(&code),
        ) {
            *raw_bytes = true;
        } else if let Some(code) =
            code.filter(|code| u32::try_from(*code).ok().and_then(char::from_u32).is_none())
        {
            *invalid_unicode = true;
            extended.push((text.chars().count(), code as u32));
        } else {
            *multibyte = true;
            // Genuine private-use scalars must not be reinterpreted as
            // the host adapter's raw-byte sentinels during construction.
            if raw_byte_from_source_char(character).is_some() {
                extended.push((text.chars().count(), character as u32));
            }
        }
        text.push(character);
        Ok(())
    }

    /// lread.c READCHAR's character: the next character's code without
    /// consuming it, a multibyte source's raw byte as its eight-bit
    /// character and a unibyte string's byte as itself; None at the end.
    fn peek_code(&mut self) -> Result<Option<i64>, LispError> {
        if let Some((code, _)) = self.input.encoded_character(self.pos)? {
            return Ok(Some(i64::from(code)));
        }
        if let Some(code) = self.input.function_code()? {
            return Ok(Some(code));
        }
        Ok(self.peek_char()?.map(|ch| {
            raw_byte_from_source_char(ch).map_or(ch as i64, |byte| BYTE8_BASE + i64::from(byte))
        }))
    }

    /// READCHAR: the code `peek_code' reads, consumed.
    fn read_code(&mut self) -> Result<Option<i64>, LispError> {
        if let Some(byte) = self.peek()?
            && byte < 0x80
        {
            self.advance()?;
            return Ok(Some(i64::from(byte)));
        }
        let code = self.peek_code()?;
        if code.is_some() {
            self.advance_char()?;
        }
        Ok(code)
    }

    /// lread.c read_char_escape: the character, with its modifier bits, of
    /// an escape sequence whose backslash has been read and whose next
    /// character is NEXT_CHAR (None at the end of the input).
    fn read_char_escape(&mut self, mut next_char: Option<i64>) -> Result<i64, LispError> {
        let mut modifiers = 0i64;
        let mut ncontrol = 0usize;
        let mut chr = loop {
            let Some(c) = next_char else {
                return Err(LispError::EndOfInput());
            };
            let ascii = u8::try_from(c).ok().filter(u8::is_ascii);
            break match ascii {
                Some(b'a') => 0x07,
                Some(b'b') => 0x08,
                Some(b'd') => 127,
                Some(b'e') => 27,
                Some(b'f') => 0x0C,
                Some(b'n') => i64::from(b'\n'),
                Some(b'r') => i64::from(b'\r'),
                Some(b't') => i64::from(b'\t'),
                Some(b'v') => 0x0B,
                // ?\LF is an error; it's probably a user mistake.
                Some(b'\n') => {
                    return Err(LispError::Signal(
                        "Invalid escape char syntax: \\<newline>".into(),
                    ));
                }
                // \M-x etc: set modifier bit and parse the char to which it
                // applies, allowing for chains such as \M-\S-\A-\H-\s-\C-q.
                Some(letter @ (b'M' | b'S' | b'H' | b'A' | b's')) => {
                    if self.peek_code()? != Some(i64::from(b'-')) {
                        if letter == b's' {
                            // \s not followed by a hyphen is SPC.
                            break i64::from(b' ');
                        }
                        // \M, \S, \H, \A not followed by a hyphen is an
                        // error.
                        self.read_code()?;
                        return Err(LispError::Signal(format!(
                            "Invalid escape char syntax: \\{} not followed by -",
                            letter as char
                        )));
                    }
                    self.read_code()?;
                    modifiers |= match letter {
                        b'M' => CHAR_META,
                        b'S' => CHAR_SHIFT,
                        b'H' => CHAR_HYPER,
                        b'A' => CHAR_ALT,
                        _ => CHAR_SUPER,
                    };
                    let c1 = self.read_code()?;
                    if c1 == Some(i64::from(b'\\')) {
                        next_char = self.read_code()?;
                        continue;
                    }
                    c1.unwrap_or(-1)
                }
                // Control modifiers (\C-x or \^x) are messy and not actually
                // idempotent.  Keep a count of them and apply them
                // separately.
                Some(prefix @ (b'C' | b'^')) => {
                    if prefix == b'C' && self.read_code()? != Some(i64::from(b'-')) {
                        return Err(LispError::Signal(
                            "Invalid escape char syntax: \\C not followed by -".into(),
                        ));
                    }
                    ncontrol += 1;
                    let c1 = self.read_code()?;
                    if c1 == Some(i64::from(b'\\')) {
                        next_char = self.read_code()?;
                        continue;
                    }
                    c1.unwrap_or(-1)
                }
                // 1-3 octal digits.  Values in 0x80..0xff are encoded as raw
                // bytes.
                Some(digit @ b'0'..=b'7') => {
                    let mut i = i64::from(digit - b'0');
                    for _ in 0..2 {
                        match self.peek_code()? {
                            Some(c @ 0x30..=0x37) => {
                                self.read_code()?;
                                i = (i << 3) + (c - 0x30);
                            }
                            _ => break,
                        }
                    }
                    if (0x80..0x100).contains(&i) {
                        i += BYTE8_BASE;
                    }
                    i
                }
                // 1 or more hex digits.  Values may encode modifiers.
                // Values in 0x80..0xff using 2 hex digits are encoded as raw
                // bytes.
                Some(b'x') => {
                    let mut i: i64 = 0;
                    let mut count = 0;
                    while let Some(digit) = self.peek_code()?.and_then(hex_digit_value) {
                        self.read_code()?;
                        i = (i << 4) + digit;
                        // Allow hex escapes as large as ?\xfffffff, because
                        // some packages use them to denote characters with
                        // modifiers.
                        if i > (CHAR_META | (CHAR_META - 1)) {
                            return Err(LispError::Signal(format!(
                                "Hex character out of range: \\x{i:x}..."
                            )));
                        }
                        count += usize::from(count < 3);
                    }
                    if count == 0 {
                        return Err(LispError::Signal(
                            "Invalid escape char syntax: \\x not followed by hex digit".into(),
                        ));
                    }
                    if count < 3 && i >= 0x80 {
                        i += BYTE8_BASE;
                    }
                    modifiers |= i & CHAR_MODIFIER_MASK;
                    i & !CHAR_MODIFIER_MASK
                }
                // 4-digit \uHHHH and 8-digit \UHHHHHHHH Unicode escapes.
                Some(letter @ (b'u' | b'U')) => {
                    let mut i: i64 = 0;
                    for _ in 0..if letter == b'u' { 4 } else { 8 } {
                        let Some(c) = self.read_code()? else {
                            return Err(LispError::Signal(format!(
                                "Malformed Unicode escape: \\{}{i:x}",
                                letter as char
                            )));
                        };
                        let Some(digit) = hex_digit_value(c) else {
                            let shown = u32::try_from(c)
                                .ok()
                                .and_then(char::from_u32)
                                .unwrap_or(INVALID_UNICODE_SENTINEL);
                            return Err(LispError::Signal(format!(
                                "Non-hex character used for Unicode escape: {shown} ({c})"
                            )));
                        };
                        i = (i << 4) + digit;
                    }
                    if i > 0x10_FFFF {
                        return Err(LispError::Signal(format!("Non-Unicode character: 0x{i:x}")));
                    }
                    i
                }
                // Named character: \N{name}
                Some(b'N') => {
                    if self.read_code()? != Some(i64::from(b'{')) {
                        return Err(LispError::ReadError(
                            "Expected opening brace after \\N".into(),
                        ));
                    }
                    let mut name = String::new();
                    let mut whitespace = false;
                    loop {
                        let Some(c) = self.read_code()? else {
                            return Err(LispError::EndOfInput());
                        };
                        if c == i64::from(b'}') {
                            break;
                        }
                        if c >= 0x80 {
                            return Err(LispError::ReadError(format!(
                                "Invalid character U+{c:04X} in character name"
                            )));
                        }
                        // Treat multiple adjacent whitespace characters as
                        // a single space character.
                        let mut c = c as u8;
                        if matches!(c, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r') {
                            if whitespace {
                                continue;
                            }
                            c = b' ';
                            whitespace = true;
                        } else {
                            whitespace = false;
                        }
                        name.push(c as char);
                        if name.len() > UNICODE_CHARACTER_NAME_LENGTH_BOUND {
                            return Err(LispError::ReadError("Character name too long".into()));
                        }
                    }
                    if name.is_empty() {
                        return Err(LispError::ReadError("Empty character name".into()));
                    }
                    character_name_to_code(&name, self.read_context())?
                }
                _ => c,
            };
        };
        // Apply Control modifiers: \C-X is ascii_ctrl(X) when X is one of
        // A-Z a-z ? @ [ \ ] ^ _, X | ctrl_modifier otherwise.
        for _ in 0..ncontrol {
            if (0x40..=0x5F).contains(&chr) || (0x61..=0x7A).contains(&chr) {
                chr &= 0x1F;
            } else if chr == i64::from(b'?') {
                chr = 127;
            } else {
                modifiers |= CHAR_CTL;
            }
        }
        Ok(chr | modifiers)
    }

    /// Append the character CHR, read from a string literal, to the text
    /// view S: an eight-bit character as its raw-byte marker (the string
    /// unibyte unless another character makes it multibyte), a character
    /// the view cannot carry with its side-table entry.
    fn push_string_character(
        s: &mut String,
        chr: i64,
        has_explicit_multibyte: &mut bool,
        has_raw_bytes: &mut bool,
        has_invalid_unicode: &mut bool,
        extended_chars: &mut Vec<(usize, u32)>,
    ) {
        if char_byte8_p(chr) {
            *has_raw_bytes = true;
            s.push(encode_raw_byte((chr - BYTE8_BASE) as u8));
            return;
        }
        let code = chr as u32;
        match char::from_u32(code) {
            Some(ch) if code < 0x80 => s.push(ch),
            Some(ch) => {
                *has_explicit_multibyte = true;
                if raw_byte_from_source_char(ch).is_some() {
                    extended_chars.push((s.chars().count(), code));
                }
                s.push(ch);
            }
            None => {
                *has_invalid_unicode = true;
                extended_chars.push((s.chars().count(), code));
                s.push(INVALID_UNICODE_SENTINEL);
            }
        }
    }

    fn read_utf8_char(&mut self) -> Result<Option<char>, LispError> {
        self.advance_char()
    }

    fn read_character(&mut self) -> Result<Option<Value>, LispError> {
        let space_or_tab = matches!(self.peek_at(1)?, Some(b' ' | b'\t'));
        let value = self.read_character_body()?;
        if !space_or_tab {
            // lread.c:read_char_literal reads and unreads one terminator,
            // including when the character literal is nested in a list.
            let next = self.peek_char()?;
            self.input.unread_lookahead()?;
            if next.is_some_and(|ch| {
                ch > ' '
                    && !matches!(
                        ch,
                        '\"' | '\'' | ';' | '(' | ')' | '[' | ']' | '#' | '?' | '`' | ',' | '.'
                    )
            }) {
                return Err(LispError::ReadError("?".into()));
            }
        }
        Ok(value)
    }

    fn read_character_body(&mut self) -> Result<Option<Value>, LispError> {
        self.advance()?; // consume '?'
        match self.peek()? {
            None => Err(LispError::EndOfInput()),
            Some(b'\\') => {
                // lread.c read_char_literal: read_char_escape, then an
                // eight-bit character becomes its byte, the modifiers kept.
                self.advance()?;
                let next = self.read_code()?;
                let ch = self.read_char_escape(next)?;
                let modifiers = ch & CHAR_MODIFIER_MASK;
                let mut ch = ch & !CHAR_MODIFIER_MASK;
                if char_byte8_p(ch) {
                    ch -= BYTE8_BASE;
                }
                Ok(Some(Value::Integer(ch | modifiers)))
            }
            Some(ch) if ch < 0x80 => {
                self.advance()?;
                if matches!(ch, b'(' | b')' | b'[' | b']' | b'"' | b';')
                    && let Some(context) = self.read_context()
                {
                    context.unescaped_character_literal(ch)?;
                }
                Ok(Some(Value::Integer(ch as i64)))
            }
            Some(_) => {
                // Multi-byte UTF-8 character like ?± or ?Ā
                Ok(Some(Value::Integer(self.read_literal_character_code()?)))
            }
        }
    }

    fn read_literal_character_code(&mut self) -> Result<i64, LispError> {
        if let Some(code) = self
            .input
            .encoded_character(self.pos)?
            .map(|(code, _)| i64::from(code))
            .or(self.input.function_code()?)
        {
            self.advance_char()?;
            return Ok(if (0x3fff80..=0x3fffff).contains(&code) {
                code & 0xff
            } else {
                code
            });
        }
        match self.peek()? {
            None => Err(LispError::EndOfInput()),
            Some(ch) if ch < 0x80 => {
                self.advance()?;
                Ok(ch as i64)
            }
            Some(_) => {
                if let Some(c) = self.read_utf8_char()? {
                    Ok(raw_byte_from_source_char(c).map_or(c as i64, i64::from))
                } else {
                    let byte = self.advance()?.ok_or(LispError::EndOfInput())?;
                    Ok(byte as i64)
                }
            }
        }
    }

    fn read_hash(&mut self) -> Result<ReadStep, LispError> {
        self.advance()?; // consume '#'
        match self.peek()? {
            None => Err(LispError::ReadError("#".into())),
            Some(b'#') => {
                self.advance()?;
                self.resolve_symbol("").map(ReadStep::Object)
            }
            // lread.c read0: `#!' (the shebang line of an executable
            // script) skips the rest of the line and reads on.
            Some(b'!') => {
                while let Some(byte) = self.peek()? {
                    self.advance()?;
                    if byte == b'\n' {
                        break;
                    }
                }
                Ok(ReadStep::Again)
            }
            Some(b'_') => {
                self.advance()?;
                if symbol_terminator_code(self.peek_code()?) {
                    // No symbol character follows: this is the empty
                    // symbol.
                    self.input.unread_lookahead()?;
                    self.resolve_symbol("").map(ReadStep::Object)
                } else {
                    self.read_atom_in_syntax(AtomSyntax::ShorthandExempt)
                        .map(ReadStep::of)
                }
            }
            Some(b'\'') => {
                // #'X -- special syntax for (function X).
                self.advance()?;
                Ok(ReadStep::Open(ReadEntry::Special { symbol: "function" }))
            }
            Some(b'<') => {
                // read0's default: INVALID_SYNTAX_WITH_BUFFER.
                self.advance()?;
                Err(LispError::ReadError("#<".into()))
            }
            Some(b'@') => {
                // lread.c skip_lazy_string: #@00 skips to the end and reads
                // as nil; #@NUMBER skips the NUMBER bytes after the digits
                // (a dynamic doc string or byte-code), the first of them
                // the character just read, and reads on.
                self.advance()?;
                let mut nskip: usize = 0;
                let mut digits = 0;
                loop {
                    match self.peek_code()? {
                        Some(c @ 0x30..=0x39) => {
                            self.read_code()?;
                            nskip = nskip
                                .checked_mul(10)
                                .and_then(|n| n.checked_add((c - 0x30) as usize))
                                .ok_or_else(|| LispError::ReadError("#@".into()))?;
                            digits += 1;
                            if digits == 2 && nskip == 0 {
                                // skip_dyn_eof.
                                if self.from_file {
                                    self.pos = self.slice.len();
                                } else {
                                    while self.read_code()?.is_some() {}
                                }
                                return Ok(ReadStep::Object(Value::Nil));
                            }
                        }
                        _ => {
                            if nskip > 0 {
                                self.read_code()?;
                                nskip -= 1;
                            }
                            break;
                        }
                    }
                }
                if self.from_file {
                    // skip_dyn_bytes from a file; with
                    // `load-force-doc-strings' the bytes are saved for
                    // get_lazy_string.
                    let start = self.pos.min(self.slice.len());
                    let end = start.saturating_add(nskip).min(self.slice.len());
                    if self
                        .read_context()
                        .is_some_and(|context| context.force_doc_strings())
                    {
                        save_lazy_string(start, self.slice[start..end].to_vec());
                    }
                    self.pos = end;
                } else {
                    // Not a file: the bytes cannot be counted in this
                    // representation, but the string skipped always ends
                    // in \037.
                    while let Some(c) = self.read_code()? {
                        if c == 0o37 {
                            break;
                        }
                    }
                }
                Ok(ReadStep::Again)
            }
            Some(b'^') => {
                self.advance()?;
                let sub_table = if self.peek()? == Some(b'^') {
                    self.advance()?;
                    true
                } else {
                    false
                };
                if self.peek()? != Some(b'[') {
                    // INVALID_SYNTAX_WITH_BUFFER: `#^' and the character
                    // READ_AND_BUFFER took (`#^^' alone for a sub-table).
                    if sub_table {
                        return Err(LispError::ReadError("#^^".into()));
                    }
                    // The character is buffered, then unread.
                    let Some(offender) = self.peek_code()? else {
                        return Err(LispError::ReadError("#^".into()));
                    };
                    return Err(LispError::ReadError(format!(
                        "#^{}",
                        reader_code_text(offender)
                    )));
                }
                self.advance()?; // consume '['
                Ok(self.open_sequence(if sub_table {
                    SequenceSyntax::SubCharTable
                } else {
                    SequenceSyntax::CharTable
                }))
            }
            Some(b'[') => {
                // #[...] -- byte-code.
                self.advance()?;
                Ok(self.open_sequence(SequenceSyntax::ByteCode))
            }
            Some(b'&') => {
                // lread.c read_bool_vector: the length's digits and then at
                // once the string of its bytes.
                self.advance()?;
                let mut length: usize = 0;
                loop {
                    let c = self.read_code()?;
                    match c {
                        Some(c @ 0x30..=0x39) => {
                            length = length
                                .checked_mul(10)
                                .and_then(|n| n.checked_add((c - 0x30) as usize))
                                .ok_or_else(|| LispError::ReadError("#&".into()))?;
                        }
                        Some(0x22) => break,
                        _ => return Err(LispError::ReadError("#&".into())),
                    }
                }
                self.retreat_ascii(b'"');
                let string = self.read_string()?.ok_or(LispError::EndOfInput())?;
                let Kind::StringObject(state) = string.kind() else {
                    return Err(LispError::ReadError("#&...".into()));
                };
                let (multibyte, bytes) = {
                    let state = state.borrow();
                    (state.is_multibyte(), state.bytes().to_vec())
                };
                let size_in_chars = length.div_ceil(8);
                // We used to print 1 char too many when the number of bits
                // was a multiple of 8; accept such input.
                if multibyte
                    || !(size_in_chars == bytes.len()
                        || (!bytes.is_empty() && length == (bytes.len() - 1) * 8))
                {
                    return Err(LispError::ReadError("#&...".into()));
                }
                // make_uninit_bool_vector, the string's bytes copied in, the
                // bits past the length cleared.
                let vector = crate::lisp::types::BoolVectorRef::new(length);
                for bit in 0..length {
                    if bytes[bit / 8] & (1 << (bit % 8)) != 0 {
                        vector.set(bit, true);
                    }
                }
                Ok(ReadStep::Object(Value::BoolVector(vector)))
            }
            Some(b':') => {
                self.advance()?;
                if symbol_terminator_code(self.peek_code()?) {
                    self.input.unread_lookahead()?;
                    // lread.c's empty #: branch returns a bare fresh symbol.
                    let id = READER_UNINTERNED_SYMBOL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
                    Ok(ReadStep::Object(Value::Symbol(
                        crate::lisp::types::SymbolName::make_uninterned_with_text(
                            make_uninterned_symbol_name("", id),
                            None,
                        ),
                    )))
                } else {
                    self.read_atom_in_syntax(AtomSyntax::Uninterned)
                        .map(ReadStep::of)
                }
            }
            Some(b'(') => {
                // #(...) -- a string with text properties.
                self.advance()?; // consume '('
                Ok(self.open_sequence(SequenceSyntax::StringProps))
            }
            Some(ch) if ch.is_ascii_digit() => {
                // #N=OBJ, #N# or #NrDIGITS: the number N first, each
                // character buffered for INVALID_SYNTAX_WITH_BUFFER.
                let mut token = String::from("#");
                let mut n: Option<i64> = Some(0);
                let c = loop {
                    let Some(c) = self.read_code()? else {
                        return Err(LispError::ReadError(token));
                    };
                    token.push_str(&reader_code_text(c));
                    if !(0x30..=0x39).contains(&c) {
                        break c;
                    }
                    n = n.and_then(|n| n.checked_mul(10)?.checked_add(c - 0x30));
                    if n.is_none() {
                        return Err(LispError::ReadError(token));
                    }
                };
                let n = n.expect("checked above");
                if c == i64::from(b'r') || c == i64::from(b'R') {
                    // #NrDIGITS -- radix-N number.
                    if n > 36 {
                        return Err(LispError::ReadError(format!("integer, radix {n}")));
                    }
                    return Ok(ReadStep::Object(self.read_integer(n as u32)?));
                }
                // read0 reads a label only up to MOST_POSITIVE_FIXNUM and
                // while `read-circle' is non-nil.
                let read_circle = self
                    .read_context()
                    .is_none_or(|context| context.read_circle());
                if n > crate::lisp::primitives::MOST_POSITIVE_FIXNUM || !read_circle {
                    return Err(LispError::ReadError(token));
                }
                let id = n as u64;
                if c == i64::from(b'=') {
                    // #N=OBJ -- assign number N to OBJ: a placeholder for
                    // the `#N#' inside OBJ until OBJ is read.
                    let placeholder = Value::cons(Value::Nil, Value::Nil);
                    self.read_objects_map.insert(id, placeholder);
                    return Ok(ReadStep::Open(ReadEntry::Numbered { id, placeholder }));
                }
                if c == i64::from(b'#')
                    && let Some(object) = self.read_objects_map.get(&id)
                {
                    // #N# -- reference to numbered object.
                    return Ok(ReadStep::Object(*object));
                }
                Err(LispError::ReadError(token))
            }
            Some(b'x' | b'X') => {
                self.advance()?;
                Ok(ReadStep::Object(self.read_integer(16)?))
            }
            Some(b'o' | b'O') => {
                self.advance()?;
                Ok(ReadStep::Object(self.read_integer(8)?))
            }
            Some(b'b' | b'B') => {
                self.advance()?;
                Ok(ReadStep::Object(self.read_integer(2)?))
            }
            Some(b's') => {
                self.advance()?;
                // READ_AND_BUFFER: the character after `#s' must be `('.
                if self.peek()? != Some(b'(') {
                    // The character is buffered, then unread.
                    let Some(offender) = self.peek_code()? else {
                        return Err(LispError::ReadError("#s".into()));
                    };
                    return Err(LispError::ReadError(format!(
                        "#s{}",
                        reader_code_text(offender)
                    )));
                }
                // #s(...) -- a record or hash table.
                self.advance()?; // consume '('
                Ok(self.open_sequence(SequenceSyntax::Record))
            }
            // `#$': the reference to the file being loaded.
            Some(b'$') => {
                self.advance()?;
                Ok(ReadStep::Object(self.load_file_name()))
            }
            // read0's default: INVALID_SYNTAX_WITH_BUFFER, `#' and the
            // character after it.
            Some(_) => {
                let offender = self.read_code()?.unwrap_or(-1);
                Err(LispError::ReadError(format!(
                    "#{}",
                    reader_code_text(offender)
                )))
            }
        }
    }

    fn read_atom(&mut self) -> Result<Option<Value>, LispError> {
        self.read_atom_in_syntax(AtomSyntax::Ordinary)
    }

    /// Text positions use a monotonic scan; callable positions count source
    /// characters, independently of the UTF-8 bytes in bounded lookahead.
    fn character_position(&mut self) -> i64 {
        if let Some(position) = self.input.function_position() {
            return self.position_base + position;
        }
        if let Input::Encoded {
            bytes,
            multibyte,
            lenient: false,
        } = &self.input
        {
            // A string's bytes: each character one byte, or else one lead
            // byte and its trailing codes.
            let span = &bytes[self.position_cursor_byte..self.pos];
            self.position_cursor_char += if *multibyte {
                span.iter().filter(|byte| **byte & 0xC0 != 0x80).count()
            } else {
                span.len()
            } as i64;
            self.position_cursor_byte = self.pos;
            return self.position_base + self.position_cursor_char;
        }
        if matches!(self.input, Input::Encoded { .. }) {
            while self.position_cursor_byte < self.pos {
                let (_, width) = self
                    .input
                    .encoded_character(self.position_cursor_byte)
                    .expect("already consumed valid character")
                    .expect("position within the source");
                self.position_cursor_byte += width;
                self.position_cursor_char += 1;
            }
            return self.position_base + self.position_cursor_char;
        }
        if let Input::Buffer(input) = &self.input {
            // The bytes consumed are whole characters of the buffer's
            // text, counted on from the last position found.
            if self.position_cursor_byte > self.pos {
                self.position_cursor_byte = 0;
                self.position_cursor_char = 0;
            }
            self.position_cursor_char += input
                .parts
                .chars_between(self.position_cursor_byte, self.pos)
                as i64;
            self.position_cursor_byte = self.pos;
            return self.position_base + self.position_cursor_char;
        }
        let Input::Text(bytes) = &self.input else {
            unreachable!()
        };
        let span = &bytes[self.position_cursor_byte..self.pos];
        self.position_cursor_char += std::str::from_utf8(span)
            .map(|text| text.chars().count() as i64)
            .unwrap_or(span.len() as i64);
        self.position_cursor_byte = self.pos;
        self.position_base + self.position_cursor_char
    }

    fn read_symbol_value(
        &mut self,
        position: i64,
        name: &str,
        syntax: AtomSyntax,
    ) -> Result<Value, LispError> {
        let symbol = if syntax == AtomSyntax::Uninterned {
            let id = READER_UNINTERNED_SYMBOL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let marked = make_uninterned_symbol_name(name, id);
            // read0's Fmake_symbol of make_specified_string's name, of the
            // multibyteness readchar reported.
            // `#:' before a terminator makes its symbol of
            // empty_unibyte_string.
            let multibyte = self.multibyte_source
                && !name.is_empty()
                && !matches!(self.input, Input::Function(_));
            let lisp_name = if multibyte == !name.is_ascii() {
                None
            } else {
                crate::lisp::types::string_data::encode_text(name, multibyte, &[])
                    .ok()
                    .and_then(|bytes| {
                        crate::lisp::primitives::make_specified_string(bytes, multibyte).ok()
                    })
            };
            Value::Symbol(crate::lisp::types::SymbolName::make_uninterned_with_text(
                marked, lisp_name,
            ))
        } else if let Input::Function(input) = &mut self.input {
            input
                .stream
                .intern_symbol(name, syntax == AtomSyntax::Ordinary)?
        } else {
            self.resolve_symbol(name)?
        };
        Ok(if self.locate_symbols && !symbol.is_nil() {
            Value::positioned_symbol(symbol, Value::Integer(position))
        } else {
            symbol
        })
    }

    /// The atom at point read in place when it is ASCII without escapes,
    /// the common case: the bytes up to read0's terminators.  None leaves
    /// the input where it was, for the general loop.
    fn ascii_token(&mut self) -> Option<&'a str> {
        let slice: &'a [u8] = self.slice;
        let start = self.pos;
        let mut end = start;
        while let Some(&byte) = slice.get(end) {
            match byte {
                0..=b' ' | b'(' | b')' | b'[' | b']' | b'"' | b';' => break,
                b',' | b'\'' | b'`' | b'#' if end > start => break,
                b'\\' | 0x80.. => return None,
                _ => end += 1,
            }
        }
        if end == start {
            return None;
        }
        self.pos = end;
        // SAFETY: the bytes are ASCII.
        Some(unsafe { std::str::from_utf8_unchecked(&slice[start..end]) })
    }

    fn read_atom_in_syntax(&mut self, syntax: AtomSyntax) -> Result<Option<Value>, LispError> {
        let token_start = if self.locate_symbols {
            self.character_position()
        } else {
            0
        };
        if let Some(token) = self.ascii_token() {
            return self.finish_atom(token_start, token, false, syntax);
        }
        let mut token = String::new();
        let mut saw_escape = false;
        while let Some(ch) = self.peek()? {
            // GNU's reader also ends a symbol at unescaped quote, backquote,
            // and comma (read0 stops at "\"';()[]#`,"), so `,flags, hop-real'
            // reads as two unquotes rather than a symbol named "flags,".
            if ch <= b' '
                // NO-BREAK SPACE: C2 A0 in each representation read here.
                || (ch == 0xC2 && self.peek_char()? == Some('\u{a0}'))
                || ch == b'('
                || ch == b')'
                || ch == b'['
                || ch == b']'
                || ch == b'"'
                || ch == b';'
                || (!token.is_empty() && (ch == b',' || ch == b'\'' || ch == b'`' || ch == b'#'))
            {
                break;
            }
            if ch == b'\\' {
                saw_escape = true;
                self.advance()?;
                match self.peek()? {
                    None => return Err(LispError::EndOfInput()),
                    Some(next) if next < 0x80 => {
                        self.advance()?;
                        token.push(next as char);
                    }
                    Some(_) => {
                        if let Some(next) = self.read_symbol_character()? {
                            token.push(next);
                        } else {
                            return Err(LispError::ReadError(
                                "invalid UTF-8 escape in symbol".into(),
                            ));
                        }
                    }
                }
                continue;
            }
            if ch < 0x80 {
                self.advance()?;
                token.push(ch as char);
            } else if let Some(next) = self.read_symbol_character()? {
                token.push(next);
            } else {
                return Err(LispError::ReadError("invalid UTF-8 in symbol".into()));
            }
        }

        self.finish_atom(token_start, &token, saw_escape, syntax)
    }

    /// The object an atom's token TOKEN reads as: read0 unreads the
    /// terminator, then tries a number (unless an escape was read), then
    /// the symbol, its name expanded by the shorthands.
    fn finish_atom(
        &mut self,
        token_start: i64,
        token: &str,
        saw_escape: bool,
        syntax: AtomSyntax,
    ) -> Result<Option<Value>, LispError> {
        self.input.unread_lookahead()?;
        if token.is_empty() {
            return Err(LispError::EndOfInput());
        }

        // Try parsing as integer.  lread.c string_to_number accepts only a
        // token that starts with a sign, a digit or a period.
        if !saw_escape
            && syntax == AtomSyntax::Ordinary
            && matches!(token.as_bytes()[0], b'+' | b'-' | b'.' | b'0'..=b'9')
        {
            if let Ok(n) = token.parse::<i64>() {
                return Ok(Some(Value::Integer(n)));
            }
            if is_integer_token(token)
                && let Ok(n) = token.parse::<BigInt>()
            {
                return Ok(Some(normalize_bigint(n)));
            }

            if let Some(f) = parse_special_float_token(token) {
                return Ok(Some(Value::float(f)));
            }
            if let Some(number) = parse_decimal_token(token) {
                return Ok(Some(number));
            }
        }

        if syntax == AtomSyntax::Ordinary && !self.symbol_shorthands.is_empty() {
            let token = self.apply_symbol_shorthands(token.to_owned());
            return self
                .read_symbol_value(token_start, &token, syntax)
                .map(Some);
        }
        self.read_symbol_value(token_start, token, syntax).map(Some)
    }

    fn read_symbol_character(&mut self) -> Result<Option<char>, LispError> {
        let code = self.input.function_code()?;
        let character = self.read_utf8_char()?;
        Ok(match code {
            // readchar's function branch leaves `multibyte` false. read0
            // writes each character to one unsigned byte in the symbol name.
            Some(code) => Some(if (code as u8) < 128 {
                code as u8 as char
            } else {
                encode_raw_byte(code as u8)
            }),
            None => character,
        })
    }

    /// lread.c read_integer: an optional sign, then RADIX digits; a
    /// letter or digit past RADIX is read into the token and makes it
    /// invalid ("integer, radix N").
    fn read_integer(&mut self, radix: u32) -> Result<Value, LispError> {
        let mut token = String::new();
        let mut valid: i8 = -1;
        let mut c = self.peek_code()?;
        if let Some(sign @ (0x2B | 0x2D)) = c {
            self.read_code()?;
            token.push(sign as u8 as char);
            c = self.peek_code()?;
        }
        if c == Some(0x30) {
            token.push('0');
            valid = 1;
            // Ignore redundant leading zeros.
            while c == Some(0x30) {
                self.read_code()?;
                c = self.peek_code()?;
            }
        }
        while let Some(code) = c {
            let Some(digit) = u8::try_from(code)
                .ok()
                .and_then(|byte| (byte as char).to_digit(36))
            else {
                break;
            };
            if digit >= radix {
                valid = 0;
            }
            if valid < 0 {
                valid = 1;
            }
            self.read_code()?;
            token.push(code as u8 as char);
            c = self.peek_code()?;
        }
        self.input.unread_lookahead()?;
        if valid != 1 {
            return Err(LispError::ReadError(format!("integer, radix {radix}")));
        }
        parse_radix_integer(radix, &token)
    }
}

fn is_integer_token(token: &str) -> bool {
    let digits = token.strip_prefix(['+', '-']).unwrap_or(token);
    !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())
}

fn parse_decimal_token(token: &str) -> Option<Value> {
    if let Some(integer) = token.strip_suffix('.') {
        let digits = integer.strip_prefix(['+', '-']).unwrap_or(integer);
        if !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()) {
            if let Ok(value) = integer.parse::<i64>() {
                return Some(Value::Integer(value));
            }
            if let Ok(value) = integer.parse::<BigInt>() {
                return Some(normalize_bigint(value));
            }
        }
    }
    if (token.contains('.') || token.contains('e') || token.contains('E'))
        && let Ok(value) = token.parse::<f64>()
    {
        return Some(Value::float(value));
    }
    None
}

/// lread.c:string_to_number's `e+INF' and `e+NaN' floats: after digits
/// with an optional point (a digit before or after it), `+INF' and
/// `+NaN' spelled exactly so.  The NaN is quiet, its payload the integer
/// before the point (modulo 2^64, -2 as unsigned with no digit there), and a
/// minus sign negates either.
pub(crate) fn parse_special_float_token(token: &str) -> Option<f64> {
    let (mantissa, suffix) = token.split_once(['e', 'E'])?;
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let (lead, trail) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if !lead.bytes().all(|byte| byte.is_ascii_digit())
        || !trail.bytes().all(|byte| byte.is_ascii_digit())
        || lead.is_empty() && trail.is_empty()
    {
        return None;
    }
    let value = match suffix {
        "+INF" => f64::INFINITY,
        "+NaN" => {
            // digit_to_number's -2 for the point, as unsigned.
            let n = if lead.is_empty() {
                u64::MAX - 1
            } else {
                lead.bytes().fold(0u64, |n, digit| {
                    n.wrapping_mul(10).wrapping_add(u64::from(digit - b'0'))
                })
            };
            f64::from_bits(0x7FF8_0000_0000_0000 | (n & 0x0007_FFFF_FFFF_FFFF))
        }
        _ => return None,
    };
    Some(if negative { -value } else { value })
}

fn normalize_bigint(value: BigInt) -> Value {
    value
        .to_i64()
        .map(Value::Integer)
        .unwrap_or(Value::BigInteger(value.into()))
}

fn valid_unicode_scalar(value: u32) -> bool {
    value <= 0x10_FFFF && !(0xD800..=0xDFFF).contains(&value)
}

fn parse_radix_integer(base: u32, token: &str) -> Result<Value, LispError> {
    if base == 1 {
        // read_integer has checked every digit is `0'.
        return Ok(Value::Integer(0));
    }
    if !(2..=36).contains(&base) {
        return Err(LispError::ReadError(format!("integer, radix {base}")));
    }
    let (negative, digits) = token
        .strip_prefix('-')
        .map_or((false, token.strip_prefix('+').unwrap_or(token)), |rest| {
            (true, rest)
        });
    if digits.is_empty() {
        return Err(LispError::ReadError(format!("integer, radix {base}")));
    }
    let mut value = BigInt::from(0u8);
    for ch in digits.chars() {
        let digit = ch
            .to_digit(base)
            .ok_or_else(|| LispError::ReadError(format!("integer, radix {base}")))?;
        value = value * BigInt::from(base) + BigInt::from(digit);
    }
    if negative {
        value = -value;
    }
    Ok(normalize_bigint(value))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::lisp::types::LispErrorKind;

    fn read_one(s: &str) -> Value {
        Reader::new(s).read().unwrap().unwrap()
    }

    #[test]
    fn positioning_reader_returns_final_native_words_without_materialization() {
        let mut reader =
            Reader::from_encoded_positioned("(alpha [β alpha t nil])".as_bytes(), true, vec![], 17);
        let form = reader.read().expect("read positions").expect("one form");
        assert!(!reader.emitted_reader_forms());
        assert!(!read_object_needs_resolution(&form));
        let items = form.to_vec().expect("outer list");
        let Kind::Vector(vector) = items[1].kind() else {
            panic!("nested vector");
        };
        let slots = vector.slots().collect::<Vec<_>>();
        assert_eq!(slots[3], Value::Nil);
        let objects = [items[0], slots[0], slots[1], slots[2]];
        for (object, (name, position)) in
            objects
                .into_iter()
                .zip([("alpha", 18), ("β", 25), ("alpha", 27), ("t", 33)])
        {
            // lisp.h:Lisp_Symbol_With_Pos is already available to native
            // loads before any interpreter or materialization is involved.
            assert_eq!(object.word() & 7, 5);
            let fields = (object.word() & !7) as *const usize;
            let header = unsafe { fields.read() };
            assert_eq!((header >> 24) & 0x3f, 6);
            assert_eq!(header & 0xfff, 2);
            assert_eq!(unsafe { fields.add(1).read() }, Value::symbol(name).word());
            assert_eq!(
                unsafe { fields.add(2).read() },
                Value::Integer(position).word()
            );
        }
        assert_ne!(objects[0].word(), objects[2].word(), "distinct occurrences");
        let before = objects.map(|object| object.word());
        let mut interpreter = crate::lisp::eval::Interpreter::new();
        interpreter.intern_symbols_in_value(&form);
        let after = interpreter
            .materialize_read_object_literals(form, &mut crate::lisp::types::Env::new())
            .expect("no replacement objects");
        assert_eq!(after.word(), form.word());
        let items = after.to_vec().expect("same list");
        let Kind::Vector(vector) = items[1].kind() else {
            panic!("same vector");
        };
        let slots = vector.slots().collect::<Vec<_>>();
        assert_eq!(
            [items[0], slots[0], slots[1], slots[2]].map(|v| v.word()),
            before
        );
    }

    #[test]
    fn emitted_reader_forms_says_whether_the_result_needs_resolution() {
        let emitted = |text: &str| {
            let mut reader = Reader::new(text);
            reader.read().unwrap().unwrap();
            reader.emitted_reader_forms()
        };
        // Marker-free text, nested lists and vectors included, is final as read.
        assert!(!emitted("(a (b [c d] \"e\") . f)"));
        assert!(!emitted("'(quote (1 2))"));
        // So is a labelled object: read0's placeholder becomes the object.
        assert!(!emitted("#1=(a . #1#)"));
        // Literal placeholders nested anywhere set the flag.
        assert!(emitted("(x (y #s(hash-table data (1 2))))"));
        assert!(emitted("[1 #s(record a) 2]"));
        // A bool vector is the object itself, made as read0 makes it.
        assert!(!emitted("(f #&3\"\\7\")"));
    }

    #[test]
    fn atoms() {
        assert_eq!(read_one("42"), Value::Integer(42));
        assert_eq!(read_one("-7"), Value::Integer(-7));
        assert_eq!(read_one("nil"), Value::Nil);
        assert_eq!(read_one("t"), Value::T);
        assert_eq!(read_one("foo"), Value::Symbol("foo".into()));
        assert_eq!(
            read_one("buffer-string"),
            Value::Symbol("buffer-string".into())
        );
        assert_eq!(read_one("##"), Value::Symbol("".into()));
    }

    #[test]
    fn uninterned_symbols() {
        let value = read_one("#:a");
        let Kind::Symbol(symbol) = value.kind() else {
            panic!("expected symbol");
        };
        assert_eq!(
            crate::lisp::types::visible_symbol_name(symbol.as_str()),
            "a"
        );
        assert!(!symbol.is_named("a"));
        assert!(!symbol.is_named(":a"));
    }

    #[test]
    fn strings() {
        let ordinary = read_one(r#""hello""#);
        assert_eq!(ordinary, Value::String("hello".into()));
        assert!(
            matches!(ordinary.kind(), Kind::StringObject(_)),
            "source string literals must retain Lisp object identity"
        );
        assert_eq!(read_one(r#""a\nb""#), Value::String("a\nb".into()));
        assert_eq!(read_one(r#""a\"b""#), Value::String("a\"b".into()));
        assert_eq!(read_one("\"a\\\nb\""), Value::String("ab".into()));
        assert_eq!(
            read_one(r#""\0\ 29""#),
            Value::String("\x00\x32\x39".into())
        );
        assert_eq!(read_one(r#""\(""#), Value::String("(".into()));
        assert_eq!(read_one(r#""\\(""#), Value::String("\\(".into()));
        assert_eq!(
            read_one(r#""\^@\^H\^?""#),
            Value::String("\0\x08\x7f".into())
        );
    }

    #[test]
    fn lists() {
        let val = read_one("(1 2 3)");
        let items = val.to_vec().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], Value::Integer(1));
        assert_eq!(items[1], Value::Integer(2));
        assert_eq!(items[2], Value::Integer(3));
    }

    #[test]
    fn nested_lists() {
        let val = read_one("(+ 1 (+ 2 3))");
        let items = val.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("+".into()));
        assert_eq!(items[1], Value::Integer(1));
        assert!(items[2].is_cons());
    }

    #[test]
    fn nested_data_takes_the_read_stack_not_a_rust_frame_a_level() {
        // read0's read stack: each level of a list holding a vector holding
        // a backquoted comma is an entry in the heap, so a reader on a
        // 256 KB stack reads two hundred thousand levels, which a Rust
        // frame a level has no room for, and its levels are the ones
        // written.
        std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                let depth = 200_000;
                let text = format!("{}x{}", "([`,".repeat(depth), "])".repeat(depth));
                let mut value = read_one(&text);
                let mut levels = 0;
                while let Some((vector, _)) = value.cons_values() {
                    let Kind::Vector(vector) = vector.kind() else {
                        panic!("level {levels}: not a vector");
                    };
                    let quoted = vector.get(0).expect("the vector's element");
                    let items = quoted.to_vec().unwrap();
                    assert_eq!(items[0], Value::Symbol("`".into()), "level {levels}");
                    let items = items[1].to_vec().unwrap();
                    assert_eq!(items[0], Value::Symbol(",".into()), "level {levels}");
                    value = items[1];
                    levels += 1;
                }
                assert_eq!((levels, value), (depth, Value::Symbol("x".into())));
                // A dot after the last element starts the tail at any depth,
                // and a closer the innermost entry does not take is invalid
                // syntax.
                let text = format!("{}nil{}", "(a . ".repeat(depth), ")".repeat(depth));
                assert_eq!(read_one(&text).to_vec().unwrap().len(), depth);
                let text = format!("{}]", "(".repeat(depth));
                assert!(matches!(
                    Reader::new(&text).read().as_ref().map_err(LispError::kind),
                    Err(LispErrorKind::ReadError(message)) if message == "]"
                ));
            })
            .expect("small-stack reader")
            .join()
            .expect("nested data reads without a stack overflow");
    }

    #[test]
    fn quoted() {
        let val = read_one("'foo");
        let items = val.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("quote".into()));
        assert_eq!(items[1], Value::Symbol("foo".into()));
    }

    #[test]
    fn characters() {
        assert_eq!(read_one("?a"), Value::Integer(b'a' as i64));
        assert_eq!(read_one("?\\n"), Value::Integer(b'\n' as i64));
        assert_eq!(read_one("?\\s"), Value::Integer(b' ' as i64));
        assert_eq!(read_one("?\\ "), Value::Integer(b' ' as i64));
        assert_eq!(read_one("?\\\\"), Value::Integer(b'\\' as i64));
        assert_eq!(read_one("?\\'"), Value::Integer(b'\'' as i64));
        assert_eq!(read_one("?\\a"), Value::Integer(b'\x07' as i64));
        assert_eq!(read_one("?\\b"), Value::Integer(b'\x08' as i64));
        assert_eq!(read_one("?\\d"), Value::Integer(b'\x7F' as i64));
        assert_eq!(read_one("?\\e"), Value::Integer(b'\x1B' as i64));
        assert_eq!(read_one("?\\f"), Value::Integer(b'\x0C' as i64));
        assert_eq!(read_one("?\\v"), Value::Integer(b'\x0B' as i64));
    }

    #[test]
    fn invalid_character_escapes_signal_errors() {
        for input in [
            "?\\",
            "?\\C",
            "?\\M",
            "?\\S",
            "?\\H",
            "?\\A",
            "?\\C-\\M",
            "?\\x",
            "?\\u",
            "?\\u234",
            "?\\U",
            "?\\U0010010",
            "?\\N",
        ] {
            assert!(Reader::new(input).read().is_err(), "{input} should fail");
        }
    }

    #[test]
    fn hash_dispatch_edge_cases() {
        assert!(Reader::new("#").read().is_err());
        assert_eq!(read_one("#_"), Value::Symbol(String::new().into()));
        assert_eq!(read_one("#@00 ignored"), Value::Nil);
        assert!(Reader::new("#[0 \"\"]").read().is_err());
    }

    #[test]
    fn reads_three_field_interpreted_closure_syntax() {
        let value = read_one("#[(_tag &rest _) ('(t)) (dynamic-name t)]");
        let Kind::ReaderForm(form) = value.kind() else {
            panic!("closure syntax must remain typed until materialization");
        };
        let ReaderForm::Closure { kind, slots } = form.as_ref() else {
            panic!("expected a closure reader form");
        };
        assert_eq!(*kind, ReaderClosureKind::Interpreted);
        assert!(slots.first().is_some_and(|params| {
            params.to_vec().ok().is_some_and(
                |params| matches!(params.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name.is_named("_tag")),
            )
        }));
    }

    #[test]
    fn trailing_dot_decimals_read_as_integers() {
        assert_eq!(read_one("13."), Value::Integer(13));
        assert_eq!(read_one("+13."), Value::Integer(13));
        assert_eq!(read_one("-13."), Value::Integer(-13));
    }

    #[test]
    fn nonbreaking_space_counts_as_whitespace_after_dot() {
        assert_eq!(
            read_one("(a .\u{00A0}b)"),
            Value::cons(Value::Symbol("a".into()), Value::Symbol("b".into()))
        );
    }

    #[test]
    fn escaped_backslash_character_does_not_consume_following_delimiter() {
        let val = read_one("(?\\\\)");
        let items = val.to_vec().unwrap();
        assert_eq!(items, vec![Value::Integer(b'\\' as i64)]);
    }

    #[test]
    fn escaped_multibyte_character_literal_consumes_full_scalar() {
        let val = read_one("(?\\‘)");
        let items = val.to_vec().unwrap();
        assert_eq!(items, vec![Value::Integer('‘' as i64)]);
    }

    #[test]
    fn characters_with_modifiers() {
        assert_eq!(read_one("?\\A-x"), Value::Integer((1 << 22) | ('x' as i64)));
        assert_eq!(read_one("?\\C-\\0"), Value::Integer(1 << 26));
        assert_eq!(read_one("?\\C-x"), Value::Integer(24));
        assert_eq!(read_one("?\\H-x"), Value::Integer((1 << 24) | ('x' as i64)));
        assert_eq!(read_one("?\\M-c"), Value::Integer((1 << 27) | ('c' as i64)));
        assert_eq!(read_one("?\\^C"), Value::Integer(3));
        assert_eq!(read_one("?\\^?"), Value::Integer(127));
        assert_eq!(read_one("?\\s-c"), Value::Integer((1 << 23) | ('c' as i64)));
        assert_eq!(read_one("?\\S-c"), Value::Integer((1 << 25) | ('c' as i64)));
        assert_eq!(read_one("?\\M-\\C-x"), Value::Integer((1 << 27) | 24));
    }

    #[test]
    fn reads_named_unicode_character_escapes() {
        assert_eq!(read_one("?\\N{SNOWFLAKE}"), Value::Integer('❄' as i64));
        assert_eq!(read_one("?\\N{U+A817}"), Value::Integer(0xA817));
        assert_eq!(
            read_one("?\\N{SYLOTI  NAGRI LETTER \n DHO}"),
            Value::Integer(0xA817)
        );
    }

    #[test]
    fn reads_named_unicode_string_escapes() {
        let Kind::StringObject(state) = read_one(r#""\N{SNOWFLAKE}""#).kind() else {
            panic!("expected a string object");
        };
        let state = state.borrow();
        assert_eq!(state.text(), "❄");
        assert!(state.is_multibyte());
    }

    #[test]
    fn named_character_errors_preserve_gnu_invalid_syntax_data() {
        for (source, expected) in [
            ("?\\N{}", "Empty character name"),
            ("?\\N{U+110000}", "\\N{U+110000}"),
            (
                "?\\N{LATIN CAPITAL LETTER Ø}",
                "Invalid character U+00D8 in character name",
            ),
        ] {
            assert!(matches!(
                Reader::new(source).read().as_ref().map_err(LispError::kind),
                Err(LispErrorKind::ReadError(message)) if message == expected
            ));
        }
    }

    #[test]
    fn comments_skipped() {
        let val = read_one("; this is a comment\n42");
        assert_eq!(val, Value::Integer(42));
    }

    #[test]
    fn reads_escape_character_string_literals() {
        assert_eq!(read_one(r#""\e[33m""#), Value::String("\x1B[33m".into()));
        assert_eq!(read_one(r#""\d""#), Value::String("\x7F".into()));
        assert_eq!(read_one(r#""\v""#), Value::String("\x0B".into()));
    }

    #[test]
    fn reads_control_escape_string_literals() {
        assert_eq!(read_one(r#""\C-x\C-f""#), Value::String("\x18\x06".into()));
    }

    #[test]
    fn form_feed_is_treated_as_whitespace() {
        let forms = Reader::new("foo\x0Cbar").read_all().unwrap();
        assert_eq!(
            forms,
            vec![Value::Symbol("foo".into()), Value::Symbol("bar".into())]
        );
    }

    #[test]
    fn dotted_pair() {
        let val = read_one("(a . b)");
        assert_eq!(val.car().unwrap(), Value::Symbol("a".into()));
        assert_eq!(val.cdr().unwrap(), Value::Symbol("b".into()));
    }

    #[test]
    fn dotted_pair_with_modified_character_literal_cdr() {
        let val = read_one("((?A . ?\\A-\\0) (?C . ?\\C-\\0) (?H . ?\\H-\\0) (?M . ?\\M-\\0))");
        let items = val.to_vec().unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].car().unwrap(), Value::Integer('A' as i64));
        assert_eq!(items[0].cdr().unwrap(), Value::Integer(1 << 22));
        assert_eq!(items[1].car().unwrap(), Value::Integer('C' as i64));
        assert_eq!(items[1].cdr().unwrap(), Value::Integer(1 << 26));
        assert_eq!(items[2].car().unwrap(), Value::Integer('H' as i64));
        assert_eq!(items[2].cdr().unwrap(), Value::Integer(1 << 24));
        assert_eq!(items[3].car().unwrap(), Value::Integer('M' as i64));
        assert_eq!(items[3].cdr().unwrap(), Value::Integer(1 << 27));
    }

    #[test]
    fn backquote_dot_comma_reads_as_dotted_comma_tail() {
        assert_eq!(
            read_one("`(t .,t)"),
            Value::list([
                Value::Symbol("`".into()),
                Value::list([Value::T, Value::Symbol(",".into()), Value::T,]),
            ])
        );
    }

    #[test]
    fn read_multiple() {
        let forms = Reader::new("1 2 3").read_all().unwrap();
        assert_eq!(forms.len(), 3);
    }

    #[test]
    fn bare_vector_syntax() {
        assert_eq!(
            read_one("[1 2 foo]"),
            Value::vector([
                Value::Integer(1),
                Value::Integer(2),
                Value::Symbol("foo".into()),
            ])
        );
    }

    #[test]
    fn reads_strings_with_text_properties_from_hash_syntax() {
        let Kind::StringObject(state) = read_one(r#"#("abc" 0 1 (face bold))"#).kind() else {
            panic!("expected a string object");
        };
        let state = state.borrow();
        assert_eq!(state.text(), "abc");
        let intervals = state.props.intervals().expect("intervals");
        let shape: Vec<_> = intervals
            .iter()
            .map(|(start, end, plist)| (start, end, plist.to_string()))
            .collect();
        assert_eq!(
            shape,
            vec![(0, 1, "(face bold)".to_string()), (1, 3, "nil".to_string())]
        );
    }

    #[test]
    fn reads_hash_radix_integers() {
        assert_eq!(read_one("#16r3FFFFF"), Value::Integer(0x3F_FFFF));
    }

    #[test]
    fn empty_integer_literals_preserve_gnu_invalid_syntax_data() {
        for (source, expected) in [
            ("#", "#"),
            ("#b", "integer, radix 2"),
            ("#o", "integer, radix 8"),
            ("#x", "integer, radix 16"),
            ("#24r", "integer, radix 24"),
        ] {
            assert!(matches!(
                Reader::new(source).read().as_ref().map_err(LispError::kind),
                Err(LispErrorKind::ReadError(message)) if message == expected
            ));
        }
    }

    #[test]
    fn bare_radix_shaped_tokens_remain_symbols() {
        // GNU only recognizes radix integers after the `#' dispatch
        // character.  Tokens such as Artist's local variable `2rx2' are
        // ordinary symbols, even when the prefix before `r' is numeric.
        assert_eq!(read_one("2rx2"), Value::Symbol("2rx2".into()));
        assert_eq!(read_one("16rFF"), Value::Symbol("16rFF".into()));
        assert_eq!(read_one("10r12"), Value::Symbol("10r12".into()));
        assert_eq!(read_one("#16rFF"), Value::Integer(255));
    }

    #[test]
    fn reads_bool_vector_literals() {
        // lread.c reads the bool vector itself, the string's bits past the
        // length cleared.
        let Kind::BoolVector(vector) = read_one(r#"#&3"\377""#).kind() else {
            panic!("a bool vector literal reads as a bool vector");
        };
        assert_eq!(vector.bits().collect::<Vec<_>>(), [true, true, true]);
        assert_eq!(vector.words()[0].get(), 7);
    }

    #[test]
    fn reads_and_validates_character_table_literals() {
        let ascii = std::iter::repeat_n("nil", 128)
            .collect::<Vec<_>>()
            .join(" ");
        let roots = std::iter::repeat_n("nil", 64).collect::<Vec<_>>().join(" ");
        let literal = read_one(&format!(
            "#^[fallback nil purpose #^^[3 0 {ascii}] {roots}]"
        ));
        let Kind::ReaderForm(form) = literal.kind() else {
            panic!("character-table syntax must remain typed until materialization");
        };
        let ReaderForm::CharTable { fields } = form.as_ref() else {
            panic!("expected a character-table reader form");
        };
        assert_eq!(fields.len(), 68);
        assert!(matches!(
            fields[3].kind(),
            Kind::ReaderForm(form)
                if matches!(
                    form.as_ref(),
                    ReaderForm::SubCharTable { fields }
                        if matches!(crate::lisp::types::kinds(fields).as_slice(), [Kind::Integer(3), Kind::Integer(0), ..])
                )
        ));

        // make-char-table limits the property to ten extras; literal reader
        // objects carry their own size and may have more (lread.c).
        let wide = read_one(&format!(
            "#^[{}]",
            std::iter::repeat_n("nil", 88).collect::<Vec<_>>().join(" ")
        ));
        let mut interp = crate::lisp::eval::Interpreter::new();
        let materialized = interp
            .materialize_read_object_literals(wide, &mut crate::lisp::types::Env::new())
            .expect("wide char-table literal");
        let Kind::CharTable(table) = materialized.kind() else {
            panic!("canonical char-table")
        };
        assert_eq!(table.extra_count(), 20);
        assert_eq!(table.extra(19), Some(Value::Nil));

        // lread.c: char_table_from_rev_list's invalid_syntax, and
        // sub_char_table_from_rev_list's `error'.
        assert!(matches!(
            Reader::new("#^[nil]").read().as_ref().map_err(LispError::kind),
            Err(LispErrorKind::ReadError(message)) if message == "Invalid size char-table"
        ));
        assert!(matches!(
            Reader::new("#^^[2 0 nil]").read().as_ref().map_err(LispError::kind),
            Err(LispErrorKind::Signal(message)) if message == "Invalid size in sub-char-table"
        ));
    }

    #[test]
    fn reads_labels_as_the_objects_they_name() {
        // read0's placeholder becomes the labelled cons itself.
        let value = read_one("'#1=((a . 1) . #1#)");
        let items = value.to_vec().expect("quote form");
        assert!(matches!(
            items.first().map(|v| v.kind()),
            Some(Kind::Symbol(symbol)) if symbol.is_named("quote")
        ));
        let (_, tail) = items[1].cons_values().expect("the labelled list");
        assert!(tail.eq_value(items[1]));
        // Any other object takes the placeholder's places inside it.
        let vector = read_one("#1=[a #1#]");
        let Kind::Vector(slots) = vector.kind() else {
            panic!("a vector");
        };
        assert!(slots.get(1).expect("two slots").eq_value(vector));
        // A literal read without a context, its fields still open.
        let record = read_one("#1=#s(r #1#)");
        let Kind::ReaderForm(form) = record.kind() else {
            panic!("a record form");
        };
        let ReaderForm::Record { slots } = form.as_ref() else {
            panic!("a record form");
        };
        assert!(slots[1].eq_value(record));
        // `#N#' before its label, and `#1=#1#', are invalid.
        assert!(Reader::new("(#1# #1=a)").read().is_err());
        assert!(matches!(
            Reader::new("#1=#1#").read().as_ref().map_err(LispError::kind),
            Err(LispErrorKind::ReadError(message)) if message == "nonsensical self-reference"
        ));
    }

    #[test]
    fn circular_label_numbers_can_be_reused_within_one_top_level_form() {
        let resolved = read_one("(#1=(a #1#) #1=(b #1#))");
        let objects = resolved.to_vec().expect("outer list");
        assert_eq!(objects.len(), 2);

        let object_ids = objects
            .iter()
            .map(|object| object.cons_cells().expect("circular list").0.cell_id())
            .collect::<Vec<_>>();
        assert_ne!(object_ids[0], object_ids[1]);

        for (object, expected_head) in objects.iter().zip(["a", "b"]) {
            let (head, tail) = object.cons_values().expect("circular list head");
            assert!(matches!(head.kind(), Kind::Symbol(symbol) if symbol.is_named(expected_head)));
            let (recursive, end) = tail.cons_values().expect("circular list tail");
            assert_eq!(end, Value::Nil);
            assert_eq!(
                recursive
                    .cons_cells()
                    .expect("recursive reference")
                    .0
                    .cell_id(),
                object.cons_cells().expect("circular list").0.cell_id()
            );
        }
    }

    #[test]
    fn labels_belong_to_one_top_level_read() {
        // readevalloop and read_internal_start empty the maps around each
        // top-level read, so a later form cannot name an earlier label.
        let mut reader = Reader::new("#1=(a) #1#");
        assert!(reader.read().expect("first form").is_some());
        assert!(reader.read().is_err());
    }

    #[test]
    fn reads_hash_table_structure_syntax_as_self_evaluating_literal() {
        let Kind::ReaderForm(form) =
            read_one("#s(hash-table test equal data (\"bla\" \"ble\"))").kind()
        else {
            panic!("hash-table syntax must remain typed until materialization");
        };
        let ReaderForm::HashTable { fields } = form.as_ref() else {
            panic!("expected a hash-table reader form");
        };
        assert_eq!(
            fields,
            &vec![
                Value::Symbol("test".into()),
                Value::Symbol("equal".into()),
                Value::Symbol("data".into()),
                Value::list([Value::String("bla".into()), Value::String("ble".into())]),
            ]
        );
    }

    #[test]
    fn reads_record_structure_syntax_as_record_literal_form() {
        let Kind::ReaderForm(form) = read_one("#s(a b #s(c d) [e])").kind() else {
            panic!("record syntax must remain typed until materialization");
        };
        let ReaderForm::Record { slots } = form.as_ref() else {
            panic!("expected a record reader form");
        };
        assert!(matches!(
            slots.as_slice().iter().map(|v| v.kind()).collect::<Vec<_>>().as_slice(),
            [Kind::Symbol(kind), Kind::Symbol(field), Kind::ReaderForm(nested), vector]
                if kind.is_named("a")
                    && field.is_named("b")
                    && matches!(nested.as_ref(), ReaderForm::Record { .. })
                    && crate::lisp::primitives::vector_items(&vector.value()).is_ok_and(|items| matches!(
                        crate::lisp::types::kinds(&items).as_slice(),
                        [Kind::Symbol(item)] if item.is_named("e")
                    ))
        ));
    }

    #[test]
    fn expands_symbol_shorthands_while_reading_atoms() {
        let mut reader = Reader::with_symbol_shorthands(
            "(ft--helper 'ft-hash-table-weakness /= - #_ft-raw #_123)",
            vec![("ft-".into(), "fns-tests-".into())],
        );
        assert_eq!(
            reader.read_all().unwrap(),
            vec![Value::list([
                Value::Symbol("fns-tests--helper".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("fns-tests-hash-table-weakness".into()),
                ]),
                Value::Symbol("/=".into()),
                Value::Symbol("-".into()),
                Value::Symbol("ft-raw".into()),
                Value::Symbol("123".into()),
            ])]
        );
    }

    #[test]
    fn symbol_shorthands_preserve_punctuation_only_symbols() {
        let mut reader = Reader::with_symbol_shorthands(
            "(/= 42 (-foo 42))",
            vec![("-".into(), "fooey-".into())],
        );
        assert_eq!(
            reader.read_all().unwrap(),
            vec![Value::list([
                Value::Symbol("/=".into()),
                Value::Integer(42),
                Value::list([Value::Symbol("fooey-foo".into()), Value::Integer(42),]),
            ])]
        );
    }

    #[test]
    fn reads_long_hex_string_escapes() {
        let Kind::StringObject(state) = read_one(r#""\x110000""#).kind() else {
            panic!("expected a string object");
        };
        assert_eq!(state.borrow().text(), INVALID_UNICODE_SENTINEL.to_string());
    }

    #[test]
    fn ert_deftest_structure() {
        let src = r#"
        (ert-deftest my-test ()
          (with-temp-buffer
            (insert "hello")
            (should (= (point) 6))))
        "#;
        let val = read_one(src);
        let items = val.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("ert-deftest".into()));
        assert_eq!(items[1], Value::Symbol("my-test".into()));
    }
}
