use super::*;
use crate::lisp::types::CharTableRef;
use crate::lisp::types::Kind;

pub(crate) const RAW_BYTE8_BASE: u32 = 0x3FFF00;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CaseAction {
    Up,
    Down,
    Capitalize,
    UpcaseInitials,
}

pub(crate) fn is_raw_like_byte_char(code: u32) -> bool {
    matches!(code, 0x00CF | 0x00EF | 0x00FF)
}

/// The eight-bit characters, #x3FFF80 to #x3FFFFF (CHAR_BYTE8_P); #x3FFF00
/// to #x3FFF7F are ordinary characters.
pub(crate) const RAW_BYTE8_CHARS: std::ops::RangeInclusive<u32> =
    RAW_BYTE8_BASE + 0x80..=RAW_BYTE8_BASE + 0xFF;

/// The stored markers of the eight-bit characters, U+E080 to U+E0FF; a
/// U+E000 to U+E07F is the private-use character itself.
pub(crate) const RAW_BYTE_MARKERS: std::ops::RangeInclusive<u32> =
    RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xFF;

pub(crate) fn raw_case_byte(key: u32) -> Option<u32> {
    if RAW_BYTE8_CHARS.contains(&key) {
        Some(key - RAW_BYTE8_BASE)
    } else if RAW_BYTE_MARKERS.contains(&key) {
        Some(key - RAW_BYTE_REGEX_BASE)
    } else {
        None
    }
}

pub(crate) fn normalize_case_key(key: u32) -> u32 {
    raw_case_byte(key).unwrap_or(key)
}

pub(crate) fn alternate_case_key(key: u32) -> Option<u32> {
    if (0x80..=0xFF).contains(&key) {
        Some(RAW_BYTE8_BASE + key)
    } else if RAW_BYTE8_CHARS.contains(&key) {
        Some(key - RAW_BYTE8_BASE)
    } else if RAW_BYTE_MARKERS.contains(&key) {
        Some(RAW_BYTE8_BASE + (key - RAW_BYTE_REGEX_BASE))
    } else {
        None
    }
}

/// The stored form of BYTE: its marker above 0x7F, an ASCII byte itself.
pub(crate) fn raw_byte_regex_char(byte: u8) -> char {
    if byte < 0x80 {
        return byte as char;
    }
    char::from_u32(RAW_BYTE_REGEX_BASE + byte as u32)
        .expect("raw byte regex marker is a valid private-use character")
}

pub(crate) fn raw_byte_from_regex_char(ch: char) -> Option<u8> {
    let code = ch as u32;
    if RAW_BYTE_MARKERS.contains(&code) {
        Some((code - RAW_BYTE_REGEX_BASE) as u8)
    } else {
        None
    }
}

pub(crate) fn is_raw_byte_regex_char(ch: char) -> bool {
    raw_byte_from_regex_char(ch).is_some()
}

pub(crate) fn public_buffer_char_code(ch: char, multibyte: bool) -> i64 {
    match raw_byte_from_regex_char(ch) {
        Some(byte) if multibyte => i64::from(RAW_BYTE8_BASE + u32::from(byte)),
        Some(byte) => i64::from(byte),
        None => ch as i64,
    }
}

/// Return the Lisp-visible character at a buffer position.
///
/// Most characters live directly in the text.  Emacs characters outside
/// Unicode's scalar range use a placeholder plus typed buffer metadata, so
/// every public character accessor must consult that sidecar through this
/// one boundary rather than leaking the placeholder value.
pub(crate) fn public_buffer_char_code_at(interp: &Interpreter, position: usize) -> Option<i64> {
    interp
        .buffer
        .borrow()
        .extended_char_at(position)
        .map(i64::from)
        .or_else(|| {
            interp
                .buffer
                .borrow()
                .char_at(position)
                .map(|ch| public_buffer_char_code(ch, interp.buffer.borrow().is_multibyte()))
        })
}

/// Ffollowing_char: the character after point, 0 at ZV.
pub(crate) fn following_char_code(interp: &Interpreter) -> i64 {
    let (point, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point(), buffer.point_max())
    };
    if point >= zv {
        return 0;
    }
    public_buffer_char_code_at(interp, point).unwrap_or(0)
}

/// Fprevious_char: the character before point, 0 at BEGV.
pub(crate) fn preceding_char_code(interp: &Interpreter) -> i64 {
    let (point, begv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point(), buffer.point_min())
    };
    if point <= begv {
        return 0;
    }
    public_buffer_char_code_at(interp, point - 1).unwrap_or(0)
}

pub(crate) fn single_char_case_mapping(iter: impl Iterator<Item = char>, fallback: u32) -> u32 {
    let mut iter = iter;
    match (iter.next(), iter.next()) {
        (Some(mapped), None) => mapped as u32,
        _ => fallback,
    }
}

pub(crate) fn simple_upcase_char(code: u32) -> u32 {
    let code = normalize_case_key(code);
    match code {
        0x00DF => 0x1E9E,
        0x01C4..=0x01C6 => 0x01C4,
        0x03C2 | 0x03C3 => 0x03A3,
        0x2177 => 0x2167,
        _ if is_raw_like_byte_char(code) => code,
        _ => char::from_u32(code)
            .map(|ch| single_char_case_mapping(ch.to_uppercase(), code))
            .unwrap_or(code),
    }
}

/// casefiddle.c's `struct casing_context': the Unicode property tables the
/// current casing operation may consult, resolved once up front.  GNU builds
/// this per operation (`prepare_casing_context'), never per character, and
/// leaves a table nil when the operation cannot use it.
#[derive(Default, Clone, Copy)]
pub(crate) struct CasingContext {
    titlecase: Option<CharTableRef>,
    special_upcase: Option<CharTableRef>,
    special_downcase: Option<CharTableRef>,
    special_titlecase: Option<CharTableRef>,
}

impl CasingContext {
    /// Mirror of casefiddle.c:70-85, including which tables each action nils.
    pub(crate) fn prepare(interp: &mut Interpreter, action: CaseAction, env: &mut Env) -> Self {
        // A table that is not a case table any more keeps its slots, as
        // GNU's upcase/downcase go on reading them.
        let _ = super::casetab::prepare_case_tables(interp);
        let capitalizing = matches!(action, CaseAction::Capitalize | CaseAction::UpcaseInitials);
        let table = |interp: &mut Interpreter,
                     property: crate::lisp::types::symbols::SymbolKey,
                     env: &mut Env| {
            let property = property.with(|symbol| Value::Symbol(*symbol));
            crate::lisp::primitives::dispatch::strings::uniprop_table_id(interp, property, env)
        };
        Self {
            titlecase: capitalizing
                .then(|| table(interp, cached_symbol!("titlecase"), env))
                .flatten(),
            special_upcase: (!matches!(action, CaseAction::Down))
                .then(|| table(interp, cached_symbol!("special-uppercase"), env))
                .flatten(),
            special_downcase: (!matches!(action, CaseAction::Up))
                .then(|| table(interp, cached_symbol!("special-lowercase"), env))
                .flatten(),
            special_titlecase: capitalizing
                .then(|| table(interp, cached_symbol!("special-titlecase"), env))
                .flatten(),
        }
    }

    fn titlecase_char(&self, interp: &Interpreter, code: u32) -> Option<u32> {
        let value = crate::lisp::primitives::dispatch::strings::uniprop_table_ref(
            interp,
            self.titlecase?,
            code,
        )?;
        u32::try_from(value.integer_value()?).ok()
    }
}

pub(crate) fn current_case_table_ids(
    interp: &mut Interpreter,
) -> Result<(CharTableRef, CharTableRef), LispError> {
    // BVAR (current_buffer, downcase_table) and upcase_table.
    let tables = interp.current_case_tables();
    Ok((tables.down, tables.up))
}

pub(crate) fn case_table_mapping(
    interp: &Interpreter,
    table_id: CharTableRef,
    code: u32,
) -> Option<u32> {
    for candidate in [Some(code), alternate_case_key(code)].into_iter().flatten() {
        let Some(Kind::Integer(mapped)) = interp
            .char_table_explicit_get(table_id, candidate)
            .map(|v| v.kind())
        else {
            continue;
        };
        let mapped = u32::try_from(mapped).ok()?;
        return Some(normalize_case_key(mapped));
    }
    interp
        .char_table_get(table_id, code)
        .and_then(|value| value.integer_value())
        .and_then(|mapped| u32::try_from(mapped).ok())
        .map(normalize_case_key)
}

/// buffer.h: uppercasep and lowercasep use the current case tables, not
/// Unicode character properties. A character can belong to only one class.
pub(crate) fn current_case_classes(interp: &Interpreter, code: u32) -> (bool, bool) {
    let Some(tables) = interp.initialized_current_case_tables() else {
        return (matches!(code, 0x61..=0x7a), matches!(code, 0x41..=0x5a));
    };
    let original = normalize_case_key(code);
    let uppercase = case_table_mapping(interp, tables.down, code).unwrap_or(original) != original;
    let lowercase =
        !uppercase && case_table_mapping(interp, tables.up, code).unwrap_or(original) != original;
    (lowercase, uppercase)
}

/// casefiddle.c's DEFVAR_BOOL case_symbols_as_words.
pub(crate) fn case_symbols_as_words_enabled(interp: &Interpreter) -> bool {
    interp
        .symbol_value_key(cached_symbol!("case-symbols-as-words"))
        .is_some_and(|value| value.is_truthy())
}

/// Whether CH is a word constituent as casefiddle.c and Freplace_match
/// read it: SYNTAX of the character, a raw byte as its eight-bit character
/// (make_char_multibyte), not the byte's Latin-1 namesake.
pub(crate) fn case_word_char(interp: &Interpreter, ch: char, case_symbols_as_words: bool) -> bool {
    syntax::current_syntax_word_char(
        interp,
        string_character_code(true, ch) as u32,
        case_symbols_as_words,
    )
}

pub(crate) fn simple_case_char_for_action(
    interp: &Interpreter,
    context: &CasingContext,
    down_table: CharTableRef,
    up_table: CharTableRef,
    code: u32,
    action: CaseAction,
) -> u32 {
    // This path receives an actual Lisp character, not a host-text byte
    // placeholder. buffer.h:upcase/downcase consult exactly that table
    // entry (including its default/parent) and leave a non-natnum unmapped.
    let mapping = |table: CharTableRef| match table.get(code).kind() {
        Kind::Integer(mapped) if mapped >= 0 => mapped as u32,
        _ => code,
    };
    match action {
        CaseAction::Up => mapping(up_table),
        CaseAction::Down => mapping(down_table),
        // casefiddle.c:case_character_impl checks the `titlecase' uniprop
        // before falling back to the case-table upcase mapping.
        CaseAction::Capitalize | CaseAction::UpcaseInitials => context
            .titlecase_char(interp, code)
            .unwrap_or_else(|| mapping(up_table)),
    }
}

/// syntax.h SYNTAX_FLAGS_PREFIX: a syntax entry's prefix flag (`p').
const SYNTAX_PREFIX_FLAG: i32 = 1 << 20;
const SYNTAX_CLASS_WORD: i32 = 2;
const SYNTAX_CLASS_SYMBOL: i32 = 3;
const GREEK_CAPITAL_LETTER_SIGMA: u32 = 0x03A3;
const GREEK_SMALL_LETTER_FINAL_SIGMA: u32 = 0x03C2;

/// casefiddle.c's casing_str_buf: a character's casing, at most
/// sizeof data's six bytes of characters.
#[derive(Clone, Copy)]
struct Cased {
    codes: [u32; 6],
    len: usize,
}

impl Cased {
    fn one(code: u32) -> Self {
        let mut codes = [0; 6];
        codes[0] = code;
        Cased { codes, len: 1 }
    }

    fn codes(&self) -> &[u32] {
        &self.codes[..self.len]
    }
}

/// casefiddle.c's struct casing_context, with the current buffer's case
/// and syntax tables and case-symbols-as-words read once.
struct Casing<'a> {
    interp: &'a Interpreter,
    tables: CasingContext,
    flag: CaseAction,
    inbuffer: bool,
    inword: bool,
    down: CharTableRef,
    up: CharTableRef,
    syntax: CharTableRef,
    symbols_as_words: bool,
}

/// buffer.h downcase and upcase: TABLE's entry for CH when a natnum.
fn case_table_natnum(table: CharTableRef, ch: u32) -> u32 {
    match table.get(ch).kind() {
        Kind::Integer(mapped) if mapped >= 0 => mapped as u32,
        _ => ch,
    }
}

impl<'a> Casing<'a> {
    /// prepare_casing_context: CONTEXT the tables FLAG may consult,
    /// INBUFFER whether a buffer is cased.
    fn new(
        interp: &'a mut Interpreter,
        context: CasingContext,
        flag: CaseAction,
        inbuffer: bool,
    ) -> Result<Self, LispError> {
        let symbols_as_words = case_symbols_as_words_enabled(interp);
        let (down, up) = current_case_table_ids(interp)?;
        let syntax = interp.current_syntax_table_id();
        Ok(Casing {
            interp,
            tables: context,
            flag,
            inbuffer,
            inword: false,
            down,
            up,
            syntax,
            symbols_as_words,
        })
    }

    fn syntax_flags(&self, ch: u32) -> i32 {
        super::syntax_scan::entry_flags(self.syntax.get(ch))
    }

    /// case_ch_is_word of a syntax entry's FLAGS.
    fn is_word(&self, flags: i32) -> bool {
        let class = flags & 0xff;
        class == SYNTAX_CLASS_WORD || (self.symbols_as_words && class == SYNTAX_CLASS_SYMBOL)
    }

    /// case_character_impl: CH cased, the special casing strings read
    /// when SPECIAL (a casing_str_buf given); whether it changed.
    fn case_impl(&mut self, ch: u32, special: bool) -> (Cased, bool) {
        let was_inword = self.inword;
        let flags = self.syntax_flags(ch);
        self.inword = self.is_word(flags)
            && (!self.inbuffer || was_inword || flags & SYNTAX_PREFIX_FLAG == 0);
        let flag = match self.flag {
            CaseAction::Capitalize if was_inword => CaseAction::Down,
            CaseAction::UpcaseInitials if was_inword => return (Cased::one(ch), false),
            CaseAction::UpcaseInitials => CaseAction::Capitalize,
            flag => flag,
        };
        if special {
            let table = match flag {
                CaseAction::Up => self.tables.special_upcase,
                CaseAction::Down => self.tables.special_downcase,
                _ => self.tables.special_titlecase,
            };
            if let Some(table) = table
                && let Some(prop) = crate::lisp::primitives::dispatch::strings::uniprop_table_ref(
                    self.interp,
                    table,
                    ch,
                )
                && let Kind::StringObject(string) = prop.kind()
            {
                let string = string.borrow();
                if string.bytes().len() <= 6 {
                    let mut cased = Cased {
                        codes: [0; 6],
                        len: 0,
                    };
                    for code in string.codes() {
                        cased.codes[cased.len] = code;
                        cased.len += 1;
                    }
                    return (cased, true);
                }
            }
        }
        let cased = if flag == CaseAction::Down {
            case_table_natnum(self.down, ch)
        } else {
            self.tables
                .titlecase_char(self.interp, ch)
                .filter(|&cased| cased <= 0x3F_FFFF)
                .unwrap_or_else(|| case_table_natnum(self.up, ch))
        };
        (Cased::one(cased), cased != ch)
    }

    /// case_single_character.
    fn case_single(&mut self, ch: u32) -> u32 {
        self.case_impl(ch, false).0.codes[0]
    }

    /// case_character: case_impl with special casing, and a capital sigma
    /// that ends a word (NEXT, the next character, not a word one, or
    /// none) a final sigma.
    fn case_character(&mut self, ch: u32, next: Option<u32>) -> (Cased, bool) {
        let was_inword = self.inword;
        let (mut cased, changed) = self.case_impl(ch, true);
        if was_inword
            && ch == GREEK_CAPITAL_LETTER_SIGMA
            && changed
            && next.is_none_or(|next| !self.is_word(self.syntax_flags(next)))
        {
            cased = Cased::one(GREEK_SMALL_LETTER_FINAL_SIGMA);
        }
        (cased, changed)
    }
}

/// The code of the character CH at INDEX of a multibyte text view whose
/// side table is EXTENDED (from NEXT_EXTENDED on): a raw byte's marker
/// is its eight-bit character.
fn view_code(ch: char, index: usize, extended: &[(usize, u32)], next_extended: &mut usize) -> u32 {
    if let Some(&(at, code)) = extended.get(*next_extended)
        && at == index
    {
        *next_extended += 1;
        return code;
    }
    match raw_byte_from_regex_char(ch) {
        Some(byte) => RAW_BYTE8_BASE + u32::from(byte),
        None => ch as u32,
    }
}

/// A multibyte text cased: the text and its side table, how many
/// characters each character became, and do_casify_multibyte_region's
/// account of the change (positions counted from 0).
struct CasedText {
    text: String,
    extended: Vec<(usize, u32)>,
    pieces: Vec<u8>,
    /// The first and last (exclusive, in the cased text) characters that
    /// changed, if any did.
    changed: Option<(usize, usize)>,
    /// Characters added.
    added: isize,
    /// Whether a character became another number of characters or of
    /// bytes, which replace_range_2 replaces.
    replace: bool,
    /// The cased characters' codes, when asked for.
    codes: Vec<u32>,
}

/// do_casify_multibyte_string and do_casify_multibyte_region's loop over
/// TEXT (a multibyte view, side table EXTENDED), the cased codes kept when
/// RECORD.
fn casify_multibyte_text(
    casing: &mut Casing<'_>,
    text: &str,
    extended: &[(usize, u32)],
    record: bool,
) -> CasedText {
    let mut out = CasedText {
        text: String::with_capacity(text.len()),
        extended: Vec::new(),
        pieces: Vec::with_capacity(text.len()),
        changed: None,
        added: 0,
        replace: false,
        codes: Vec::new(),
    };
    let mut next_extended = 0;
    let mut pos = 0;
    let mut chars = text.chars().enumerate().peekable();
    while let Some((index, ch)) = chars.next() {
        let code = view_code(ch, index, extended, &mut next_extended);
        let next = if code == GREEK_CAPITAL_LETTER_SIGMA {
            chars.peek().map(|&(next_index, next_ch)| {
                let mut lookahead = next_extended;
                view_code(next_ch, next_index, extended, &mut lookahead)
            })
        } else {
            None
        };
        let has_next = chars.peek().is_some();
        let (cased, changed) = casing.case_character(code, if has_next { next } else { None });
        for &cased_code in cased.codes() {
            push_character_code(&mut out.text, &mut out.extended, pos, cased_code);
            pos += 1;
        }
        if record {
            out.codes.extend_from_slice(cased.codes());
        }
        out.pieces.push(cased.len as u8);
        if changed {
            let start = pos - cased.len;
            out.changed = Some((out.changed.map_or(start, |(first, _)| first), pos));
            let bytes = crate::lisp::primitives::buffers::multibyte_char_bytes;
            if !(cased.len == 1 && bytes(cased.codes[0]) == bytes(code)) {
                out.replace = true;
                out.added += cased.len as isize - 1;
            }
        }
    }
    out
}

/// do_casify_unibyte_region's loop over a unibyte buffer's TEXT: each byte
/// cased as its character (make_char_multibyte) by case_single_character
/// and stored as the byte make_char_unibyte makes of the result.
fn casify_unibyte_text(casing: &mut Casing<'_>, text: &str) -> CasedText {
    let mut out = CasedText {
        text: String::with_capacity(text.len()),
        extended: Vec::new(),
        pieces: Vec::new(),
        changed: None,
        added: 0,
        replace: false,
        codes: Vec::new(),
    };
    for (pos, ch) in text.chars().enumerate() {
        let byte = raw_byte_from_regex_char(ch).unwrap_or(ch as u8);
        let code = if byte < 0x80 {
            u32::from(byte)
        } else {
            RAW_BYTE8_BASE + u32::from(byte)
        };
        let cased = casing.case_single(code);
        if cased == code {
            out.text.push(ch);
            continue;
        }
        out.changed = Some((out.changed.map_or(pos, |(first, _)| first), pos + 1));
        // make_char_unibyte: ASCII as is, else CHAR_TO_BYTE8.
        let byte = if cased < 0x80 {
            cased as u8
        } else if RAW_BYTE8_CHARS.contains(&cased) {
            (cased - RAW_BYTE8_BASE) as u8
        } else {
            (cased & 0xFF) as u8
        };
        out.text.push(raw_byte_regex_char(byte));
    }
    out
}

pub(crate) fn casify_value(
    interp: &mut Interpreter,
    value: &Value,
    action: CaseAction,
    env: &mut Env,
) -> Result<Value, LispError> {
    let context = CasingContext::prepare(interp, action, env);
    if let Kind::Integer(integer) = value.kind()
        && integer >= 0
    {
        // casefiddle.c:do_casify_natnum narrows to C int, strips the six
        // event modifier bits before lookup, and restores them only when
        // casing changes the character. Higher bits otherwise preserve OBJ.
        const FLAG_BITS: u32 = 0x0fc0_0000;
        let character = integer as i32;
        if !(0..=FLAG_BITS as i32).contains(&character) {
            return Ok(*value);
        }
        let flags = character as u32 & FLAG_BITS;
        let mut code = character as u32 & !FLAG_BITS;
        let multibyte = code >= 256 || interp.buffer.borrow().is_multibyte();
        if !multibyte && code >= 128 {
            code += RAW_BYTE8_BASE;
        }
        let (down_table, up_table) = current_case_table_ids(interp)?;
        let mut mapped =
            simple_case_char_for_action(interp, &context, down_table, up_table, code, action);
        if mapped == code {
            return Ok(*value);
        }
        if !multibyte {
            mapped &= 0xff;
        }
        return Ok(Value::Integer(i64::from(mapped | flags)));
    }
    let input = string_like(value)
        .ok_or_else(|| LispError::WrongTypeArgument("char-or-string-p".into(), *value))?;
    if !input.multibyte {
        // casefiddle.c:do_casify_unibyte_string cases a Fcopy_sequence of
        // the string one byte for one, its intervals copied with their
        // plists. In locales such as Turkish, an ASCII-to-wide mapping
        // falls back to the Unicode ASCII table.
        let props = match &input.props {
            Some(intervals) => intervals.copy(0, intervals.total_length())?,
            None => None,
        };
        return interp.with_lisp_stack_roots(value, |interp| {
            let (down, up) = current_case_table_ids(interp)?;
            let symbols_as_words = case_symbols_as_words_enabled(interp);
            let mut in_word = false;
            let mut output = String::new();
            for ch in input.text.chars() {
                let byte = string_character_code(false, ch) as u32;
                let code = if byte < 128 {
                    byte
                } else {
                    RAW_BYTE8_BASE + byte
                };
                let was_in_word = in_word;
                in_word = case_word_char(interp, ch, symbols_as_words);
                let flag = match action {
                    CaseAction::Capitalize if was_in_word => CaseAction::Down,
                    CaseAction::UpcaseInitials if was_in_word => {
                        output.push(ch);
                        continue;
                    }
                    other => other,
                };
                // GNU converts high bytes to byte8 character codes before
                // indexing the table. A Latin-1 mapping at the byte's numeric
                // value must not act as a fallback for an unset byte8 entry.
                let table = if flag == CaseAction::Down { down } else { up };
                let mapped = interp
                    .char_table_get(table, code)
                    .and_then(|value| value.integer_value())
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(code);
                let mut mapped =
                    if matches!(flag, CaseAction::Capitalize | CaseAction::UpcaseInitials) {
                        context.titlecase_char(interp, code).unwrap_or(mapped)
                    } else {
                        mapped
                    };
                if code < 128 && mapped >= 256 {
                    let property = if flag == CaseAction::Down {
                        cached_symbol!("lowercase")
                    } else {
                        cached_symbol!("uppercase")
                    }
                    .with(|symbol| Value::Symbol(*symbol));
                    mapped = dispatch::strings::uniprop_table_id(interp, property, env)
                        .and_then(|table| interp.char_table_get(table, code))
                        .and_then(|value| value.integer_value())
                        .and_then(|value| u32::try_from(value).ok())
                        .unwrap_or(code);
                }
                let byte = (mapped & 0xff) as u8;
                output.push(if byte < 128 {
                    char::from(byte)
                } else {
                    raw_byte_regex_char(byte)
                });
            }
            Ok(make_shared_string_value_with_multibyte(
                output, props, false,
            ))
        });
    }
    let cased = {
        let mut casing = Casing::new(interp, context, action, false)?;
        casify_multibyte_text(&mut casing, &input.text, &input.extended_chars, false)
    };
    // casefiddle.c:do_casify_multibyte_string makes the cased text a new
    // string (make_multibyte_string), without the source's intervals.
    Ok(string_like_value_with_extended_chars(
        cased.text,
        None,
        input.multibyte,
        cased.extended,
    ))
}

pub(crate) fn replace_buffer_region_with_text(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    text: &str,
    extended: &[(usize, u32)],
) -> Result<usize, LispError> {
    interp.buffer.borrow_mut().goto_char(start);
    interp
        .buffer
        .borrow_mut()
        .delete_region(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    interp.buffer.borrow_mut().insert(text);
    interp
        .buffer
        .borrow_mut()
        .set_inserted_extended_chars(start, extended);
    Ok(start + text.chars().count())
}

pub(crate) fn casify_buffer_region(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    action: CaseAction,
    env: &mut Env,
) -> Result<usize, LispError> {
    let lo = start.min(end);
    let hi = start.max(end);
    if lo >= hi {
        return Ok(hi);
    }
    // casefiddle.c:casify_region: after the hooks, modify_text over the
    // region and record_delete of its text as make_buffer_string makes it
    // (update_buffer_properties first), changed or not; the text cased as
    // do_casify_multibyte_region cases it; record_insert of the result;
    // then a signal of the characters that changed, none when none did.
    modify_current_buffer(interp, lo, hi, false, env, |interp, _, env| {
        let hi = hi.min(interp.buffer.borrow().point_max()).max(lo);
        interp.buffer.borrow_mut().modify_text(lo, hi);
        let context = CasingContext::prepare(interp, action, env);
        crate::lisp::primitives::textprop::update_buffer_properties(interp, lo, hi, env)?;
        let hi = hi.min(interp.buffer.borrow().point_max()).max(lo);
        let multibyte = interp.buffer.borrow().is_multibyte();
        interp.buffer.borrow_mut().record_casing_deletion(lo, hi);
        if !multibyte {
            interp
                .buffer
                .borrow_mut()
                .record_casing_insertion(lo, hi - lo);
        }
        let (text, extended) = {
            let buffer = interp.buffer.borrow();
            (
                buffer
                    .buffer_substring(lo, hi)
                    .map_err(|error| LispError::Signal(error.to_string()))?,
                buffer.substring_extended_chars(lo, hi),
            )
        };
        let cased = {
            let mut casing = Casing::new(interp, context, action, true)?;
            if multibyte {
                casify_multibyte_text(&mut casing, &text, &extended, false)
            } else {
                casify_unibyte_text(&mut casing, &text)
            }
        };
        if cased.replace {
            // do_casify_multibyte_region: a character cased to another
            // number of characters or of bytes is replaced as
            // replace_range_2 replaces it, its properties kept, and point
            // follows it as opoint does; the rest are cased where they
            // stand.  The casing is done again for the characters each
            // became.
            let codes = {
                let mut casing = Casing::new(interp, context, action, true)?;
                casify_multibyte_text(&mut casing, &text, &extended, true).codes
            };
            let vars = interp.insertion_property_variables();
            let mut opoint = interp.buffer.borrow().point();
            let mut next_extended = 0;
            let (mut pos, mut out) = (lo, 0);
            let bytes = crate::lisp::primitives::buffers::multibyte_char_bytes;
            for ((index, ch), &count) in text.chars().enumerate().zip(&cased.pieces) {
                let old = view_code(ch, index, &extended, &mut next_extended);
                let count = usize::from(count);
                let new = &codes[out..out + count];
                if new != [old] {
                    let (mut piece, mut piece_extended) = (String::new(), Vec::new());
                    for (offset, &code) in new.iter().enumerate() {
                        push_character_code(&mut piece, &mut piece_extended, offset, code);
                    }
                    let mut buffer = interp.buffer.borrow_mut();
                    if count == 1 && bytes(old) == bytes(new[0]) {
                        buffer.replace_text_in_place(pos, pos + 1, &piece, &piece_extended);
                    } else {
                        buffer.replace_char_unrecorded(pos, &piece, &piece_extended, &vars);
                        if opoint > pos {
                            opoint = opoint + count - 1;
                        }
                    }
                }
                pos += count;
                out += count;
            }
            let mut buffer = interp.buffer.borrow_mut();
            if buffer.point() != opoint {
                buffer.goto_char(opoint);
            }
        } else if cased.changed.is_some() {
            interp
                .buffer
                .borrow_mut()
                .replace_text_in_place(lo, hi, &cased.text, &cased.extended);
        }
        let new_end = (hi as isize + cased.added) as usize;
        if multibyte {
            interp
                .buffer
                .borrow_mut()
                .record_casing_insertion(lo, new_end - lo);
        }
        // The family shares the text, cased or not, and its counts.
        if cased.changed.is_some() {
            interp.mirror_in_place_edit_to_related_buffers(lo, hi, new_end);
        } else {
            interp.share_text_counters(interp.current_buffer_id());
        }
        // casify_region: signal_after_change (start, end - start - added,
        // end - start) over the first to the last character changed.
        Ok(match cased.changed {
            None => (None, new_end),
            Some((first, last)) => (
                Some(AfterChange::new(
                    lo + first,
                    (last as isize - first as isize - cased.added) as usize,
                    last - first,
                )),
                new_end,
            ),
        })
    })
}
