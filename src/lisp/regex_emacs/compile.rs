//! regex-emacs.c's `regex_compile' and its helpers.  The pattern arrives as
//! Emacs character codes; positions in it and in the compiled buffer are
//! indices where GNU keeps pointers.

use super::analyze::{analyze_first, skip_one_char};
use super::*;

/// Bits of a range table's flags word: the classes a multibyte character
/// is tested against (BIT_WORD ...).
pub(super) const BIT_WORD: u32 = 0x1;
pub(super) const BIT_LOWER: u32 = 0x2;
pub(super) const BIT_PUNCT: u32 = 0x4;
pub(super) const BIT_SPACE: u32 = 0x8;
pub(super) const BIT_UPPER: u32 = 0x10;
pub(super) const BIT_MULTIBYTE: u32 = 0x20;
pub(super) const BIT_ALPHA: u32 = 0x40;
pub(super) const BIT_ALNUM: u32 = 0x80;
pub(super) const BIT_GRAPH: u32 = 0x100;
pub(super) const BIT_PRINT: u32 = 0x200;
pub(super) const BIT_BLANK: u32 = 0x400;

const RE_DUP_MAX: i64 = 0xffff;
const MAX_REGNUM: i64 = 255;
/// MAX_MULTIBYTE_LENGTH: the room a character takes at most.
const MAX_MULTIBYTE_LENGTH: usize = 5;

/// re_wctype_t: the character classes of `[:NAME:]'.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WcType {
    Error = 0,
    Alnum,
    Alpha,
    Word,
    Graph,
    Print,
    Lower,
    Upper,
    Punct,
    Cntrl,
    Digit,
    XDigit,
    Blank,
    Space,
    Multibyte,
    NonAscii,
    Ascii,
    Unibyte,
}

/// re_wctype_parse: whether the pattern at P begins with "[:NAME:]".  None
/// when it does not; Some(class, end) when it does, the class being
/// `Error' for an unknown NAME.
pub(crate) fn re_wctype_parse(pattern: &[u32], p: usize) -> Option<(WcType, usize)> {
    let rest = &pattern[p..];
    if rest.len() < 4 || rest[0] != u32::from(b'[') || rest[1] != u32::from(b':') {
        return None;
    }
    let beg = 2;
    let mut limit = rest.len() - 3;
    let mut it = beg;
    while rest[it] != u32::from(b':') || rest[it + 1] != u32::from(b']') {
        limit -= 1;
        if limit == 0 {
            return None;
        }
        it += 1;
    }
    // The NAME compared where it stands, as the C switch's memcmp does.
    let name = &rest[beg..it];
    const CLASSES: [(&[u8], WcType); 17] = [
        (b"word", WcType::Word),
        (b"alnum", WcType::Alnum),
        (b"alpha", WcType::Alpha),
        (b"space", WcType::Space),
        (b"digit", WcType::Digit),
        (b"blank", WcType::Blank),
        (b"upper", WcType::Upper),
        (b"lower", WcType::Lower),
        (b"punct", WcType::Punct),
        (b"ascii", WcType::Ascii),
        (b"graph", WcType::Graph),
        (b"print", WcType::Print),
        (b"cntrl", WcType::Cntrl),
        (b"xdigit", WcType::XDigit),
        (b"unibyte", WcType::Unibyte),
        (b"nonascii", WcType::NonAscii),
        (b"multibyte", WcType::Multibyte),
    ];
    let class = CLASSES
        .iter()
        .find(|(spelling, _)| {
            spelling.len() == name.len()
                && spelling
                    .iter()
                    .zip(name)
                    .all(|(&byte, &c)| u32::from(byte) == c)
        })
        .map_or(WcType::Error, |&(_, class)| class);
    Some((class, p + it + 2))
}

/// IS_REAL_ASCII.
#[inline]
const fn is_real_ascii(c: u32) -> bool {
    c < 0o200
}

/// SINGLE_BYTE_CHAR_P.
#[inline]
const fn single_byte_char_p(c: u32) -> bool {
    c < 0x100
}

/// ISALNUM of a real ASCII character.
const fn ascii_alnum(c: u32) -> bool {
    matches!(c, 0x61..=0x7a | 0x41..=0x5a | 0x30..=0x39)
}

/// ISALPHA of a real ASCII character.
const fn ascii_alpha(c: u32) -> bool {
    matches!(c, 0x61..=0x7a | 0x41..=0x5a)
}

/// ISBLANK of a real ASCII character.
const fn ascii_blank(c: u32) -> bool {
    c == 0x20 || c == 0x09
}

/// ISGRAPH of a single-byte character.
const fn unibyte_graph(c: u32) -> bool {
    c > 0x20 && !matches!(c, 0o177..=0o240)
}

/// ISPRINT of a single-byte character.
const fn unibyte_print(c: u32) -> bool {
    c >= 0x20 && !matches!(c, 0o177..=0o237)
}

/// ISPUNCT of a real ASCII character.
const fn ascii_punct(c: u32) -> bool {
    c > 0x20 && c < 0o177 && !ascii_alnum(c)
}

pub(super) fn isalnum<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    if is_real_ascii(c) {
        ascii_alnum(c)
    } else {
        env.alphanumericp(c)
    }
}

pub(super) fn isalpha<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    if is_real_ascii(c) {
        ascii_alpha(c)
    } else {
        env.alphabeticp(c)
    }
}

pub(super) fn isblank<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    if is_real_ascii(c) {
        ascii_blank(c)
    } else {
        env.blankp(c)
    }
}

pub(super) fn isgraph<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    if single_byte_char_p(c) {
        unibyte_graph(c)
    } else {
        env.graphicp(c)
    }
}

pub(super) fn isprint<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    if single_byte_char_p(c) {
        unibyte_print(c)
    } else {
        env.printablep(c)
    }
}

pub(super) fn ispunct<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    if is_real_ascii(c) {
        ascii_punct(c)
    } else {
        env.buffer_syntax(c) != syntax_code::WORD
    }
}

pub(super) fn isspace<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    env.buffer_syntax(c) == syntax_code::WHITESPACE
}

pub(super) fn isword<E: MatchEnv + ?Sized>(env: &mut E, c: u32) -> bool {
    env.buffer_syntax(c) == syntax_code::WORD
}

/// ISXDIGIT (char_hexdigit).
const fn isxdigit(c: u32) -> bool {
    matches!(c, 0x30..=0x39 | 0x41..=0x46 | 0x61..=0x66)
}

/// re_iswctype of the ASCII character C for a CLASS that reads no table
/// there; None for `[:lower:]', `[:upper:]', `[:space:]' and `[:word:]',
/// which read the case or syntax table.
pub(super) const fn ascii_class_fixed(class: WcType, c: u32) -> Option<bool> {
    Some(match class {
        WcType::Alnum => ascii_alnum(c),
        WcType::Alpha => ascii_alpha(c),
        WcType::Blank => ascii_blank(c),
        WcType::Cntrl => c < 0x20,
        WcType::Digit => matches!(c, 0x30..=0x39),
        WcType::Graph => unibyte_graph(c),
        WcType::Print => unibyte_print(c),
        WcType::Punct => ascii_punct(c),
        WcType::XDigit => isxdigit(c),
        WcType::Ascii => is_real_ascii(c),
        WcType::NonAscii => !is_real_ascii(c),
        WcType::Unibyte => single_byte_char_p(c),
        WcType::Multibyte => !single_byte_char_p(c),
        WcType::Error => false,
        WcType::Lower | WcType::Upper | WcType::Space | WcType::Word => return None,
    })
}

/// Every class, for the tables below.
pub(super) const WC_TYPES: [WcType; 18] = [
    WcType::Error,
    WcType::Alnum,
    WcType::Alpha,
    WcType::Word,
    WcType::Graph,
    WcType::Print,
    WcType::Lower,
    WcType::Upper,
    WcType::Punct,
    WcType::Cntrl,
    WcType::Digit,
    WcType::XDigit,
    WcType::Blank,
    WcType::Space,
    WcType::Multibyte,
    WcType::NonAscii,
    WcType::Ascii,
    WcType::Unibyte,
];

/// The ASCII characters of each class `ascii_class_fixed' answers, bit C
/// for character C, by the class's discriminant.
pub(super) const ASCII_CLASS_MASKS: [u128; 18] = {
    let mut masks = [0; 18];
    let mut i = 0;
    while i < WC_TYPES.len() {
        let class = WC_TYPES[i];
        let mut c = 0;
        while c < 0x80 {
            if let Some(true) = ascii_class_fixed(class, c) {
                masks[class as usize] |= 1 << c;
            }
            c += 1;
        }
        i += 1;
    }
    masks
};

/// re_iswctype: whether CH is in class CC.
pub(crate) fn re_iswctype<E: MatchEnv + ?Sized>(env: &mut E, ch: u32, cc: WcType) -> bool {
    match cc {
        WcType::Alnum => isalnum(env, ch),
        WcType::Alpha => isalpha(env, ch),
        WcType::Blank => isblank(env, ch),
        WcType::Cntrl => ch < 0x20,
        WcType::Digit => matches!(ch, 0x30..=0x39),
        WcType::Graph => isgraph(env, ch),
        WcType::Lower => env.lowercasep(ch),
        WcType::Print => isprint(env, ch),
        WcType::Punct => ispunct(env, ch),
        WcType::Space => isspace(env, ch),
        WcType::Upper => env.uppercasep(ch),
        WcType::XDigit => isxdigit(ch),
        WcType::Ascii => is_real_ascii(ch),
        WcType::NonAscii => !is_real_ascii(ch),
        WcType::Unibyte => single_byte_char_p(ch),
        WcType::Multibyte => !single_byte_char_p(ch),
        WcType::Word => isword(env, ch),
        WcType::Error => false,
    }
}

/// re_wctype_to_bit: the range-table bit that matches multibyte
/// characters of class CC.
fn re_wctype_to_bit(cc: WcType) -> u32 {
    match cc {
        WcType::NonAscii | WcType::Multibyte => BIT_MULTIBYTE,
        WcType::Alpha => BIT_ALPHA,
        WcType::Alnum => BIT_ALNUM,
        WcType::Word => BIT_WORD,
        WcType::Lower => BIT_LOWER,
        WcType::Upper => BIT_UPPER,
        WcType::Punct => BIT_PUNCT,
        WcType::Space => BIT_SPACE,
        WcType::Graph => BIT_GRAPH,
        WcType::Print => BIT_PRINT,
        WcType::Blank => BIT_BLANK,
        WcType::Ascii
        | WcType::Digit
        | WcType::XDigit
        | WcType::Cntrl
        | WcType::Unibyte
        | WcType::Error => 0,
    }
}

/// STORE_NUMBER: a two-byte little-endian number.
#[inline]
pub(super) fn store_number(buffer: &mut [u8], at: usize, number: i32) {
    buffer[at..at + 2].copy_from_slice(&number_bytes(number));
}

/// The two bytes STORE_NUMBER stores for NUMBER.
#[inline]
fn number_bytes(number: i32) -> [u8; 2] {
    [(number & 0o377) as u8, ((number >> 8) & 0o377) as u8]
}

/// EXTRACT_NUMBER: the signed two-byte number at AT.
#[inline]
pub(super) fn extract_number(buffer: &[u8], at: usize) -> i32 {
    (buffer[at + 1] as i8 as i32) * 256 + buffer[at] as i32
}

/// extract_address: where the jump whose offset is at AT goes.
#[inline]
pub(super) fn extract_address(buffer: &[u8], at: usize) -> usize {
    (at as isize + 2 + extract_number(buffer, at) as isize) as usize
}

/// EXTRACT_CHARACTER: a three-byte character.
#[inline]
pub(super) fn extract_character(buffer: &[u8], at: usize) -> u32 {
    u32::from(buffer[at]) | (u32::from(buffer[at + 1]) << 8) | (u32::from(buffer[at + 2]) << 16)
}

/// CHARSET_BITMAP_SIZE.
#[inline]
pub(super) fn charset_bitmap_size(buffer: &[u8], p: usize) -> usize {
    usize::from(buffer[p + 1] & 0x7F)
}

/// CHARSET_RANGE_TABLE_EXISTS_P.
#[inline]
pub(super) fn charset_range_table_exists_p(buffer: &[u8], p: usize) -> bool {
    buffer[p + 1] & 0x80 != 0
}

/// CHARSET_RANGE_TABLE: where the range count is stored.
#[inline]
pub(super) fn charset_range_table(buffer: &[u8], p: usize) -> usize {
    p + 4 + charset_bitmap_size(buffer, p)
}

/// CHARSET_RANGE_TABLE_BITS.
#[inline]
pub(super) fn charset_range_table_bits(buffer: &[u8], p: usize) -> u32 {
    let size = charset_bitmap_size(buffer, p);
    u32::from(buffer[p + 2 + size]) + u32::from(buffer[p + 3 + size]) * 0x100
}

/// One open group on the compile stack.
struct CompileStackElt {
    begalt_offset: usize,
    fixup_alt_jump: Option<usize>,
    laststart_offset: usize,
    regnum: i64,
}

/// The range table being built for one bracket expression.
#[derive(Default)]
struct RangeTableWork {
    table: Vec<u32>,
    bits: u32,
}

impl RangeTableWork {
    fn set(&mut self, from: u32, to: u32) {
        self.table.push(from);
        self.table.push(to);
    }
}

struct Compiler<'e> {
    b: Vec<u8>,
    translate: bool,
    env: &'e mut dyn MatchEnv,
    multibyte: bool,
    re_nsub: i64,
    used_syntax: bool,
    /// The ASCII characters of `[:space:]' and `[:word:]', from the
    /// buffer's syntax table when a class first needs them: no Lisp runs
    /// while a pattern compiles, so the table stays the one read.
    ascii_syntax_masks: Option<(u128, u128)>,
}

impl Compiler<'_> {
    /// The ASCII characters re_iswctype admits for CLASS, bit C for
    /// character C.
    fn ascii_class_mask(&mut self, class: WcType) -> u128 {
        if !matches!(class, WcType::Space | WcType::Word) {
            if matches!(class, WcType::Lower | WcType::Upper) {
                return (0..0x80u32)
                    .filter(|&c| re_iswctype(self.env, c, class))
                    .fold(0, |mask, c| mask | 1 << c);
            }
            return ASCII_CLASS_MASKS[class as usize];
        }
        let (space, word) = *self.ascii_syntax_masks.get_or_insert_with(|| {
            let mut masks = (0, 0);
            for (c, code) in self.env.buffer_syntax_ascii().into_iter().enumerate() {
                if code == syntax_code::WHITESPACE {
                    masks.0 |= 1 << c;
                } else if code == syntax_code::WORD {
                    masks.1 |= 1 << c;
                }
            }
            masks
        });
        if class == WcType::Space { space } else { word }
    }

    #[inline]
    fn translate(&mut self, c: u32) -> u32 {
        if self.translate {
            self.env.translate(c)
        } else {
            c
        }
    }

    /// GET_BUFFER_SPACE: fail when the pattern would outgrow MAX_BUF_SIZE.
    fn space(&self, n: usize) -> Result<(), RegError> {
        if self.b.len() + n > MAX_BUF_SIZE {
            Err(RegError::ESize)
        } else {
            Ok(())
        }
    }

    fn push(&mut self, byte: u8) -> Result<(), RegError> {
        self.space(1)?;
        self.b.push(byte);
        Ok(())
    }

    fn push2(&mut self, b1: u8, b2: u8) -> Result<(), RegError> {
        self.space(2)?;
        self.b.push(b1);
        self.b.push(b2);
        Ok(())
    }

    /// store_op1 at LOC (the bytes exist already).
    #[inline]
    fn store_op1(&mut self, op: u8, loc: usize, arg: i32) {
        let [lo, hi] = number_bytes(arg);
        self.b[loc..loc + 3].copy_from_slice(&[op, lo, hi]);
    }

    /// store_op2 at LOC.
    #[inline]
    fn store_op2(&mut self, op: u8, loc: usize, arg1: i32, arg2: i32) {
        let [lo1, hi1] = number_bytes(arg1);
        let [lo2, hi2] = number_bytes(arg2);
        self.b[loc..loc + 5].copy_from_slice(&[op, lo1, hi1, lo2, hi2]);
    }

    /// STORE_JUMP: append-or-overwrite a jump at LOC to TO.  The bytes at
    /// LOC.. are reserved by the caller; they are created here when LOC is
    /// the buffer's end.
    #[inline]
    fn store_jump(&mut self, op: u8, loc: usize, to: usize) {
        if self.b.len() < loc + 3 {
            self.b.resize(loc + 3, 0);
        }
        self.store_op1(op, loc, to as i32 - loc as i32 - 3);
    }

    fn store_jump2(&mut self, op: u8, loc: usize, to: usize, arg: i32) {
        if self.b.len() < loc + 5 {
            self.b.resize(loc + 5, 0);
        }
        self.store_op2(op, loc, to as i32 - loc as i32 - 3, arg);
    }

    /// The loop insert_op1 and insert_op2 move the bytes from LOC up
    /// with: N more bytes at the end, the ones from LOC copied N up.
    #[inline]
    fn open_at(&mut self, loc: usize, n: usize) {
        let end = self.b.len();
        self.b.resize(end + n, 0);
        self.b.copy_within(loc..end, loc + n);
    }

    /// insert_op1: open three bytes at LOC for OP and ARG.
    fn insert_op1(&mut self, op: u8, loc: usize, arg: i32) -> Result<(), RegError> {
        self.space(3)?;
        self.open_at(loc, 3);
        self.store_op1(op, loc, arg);
        Ok(())
    }

    /// insert_op2: open five bytes at LOC for OP, ARG1 and ARG2.
    fn insert_op2(&mut self, op: u8, loc: usize, arg1: i32, arg2: i32) -> Result<(), RegError> {
        self.space(5)?;
        self.open_at(loc, 5);
        self.store_op2(op, loc, arg1, arg2);
        Ok(())
    }

    /// INSERT_JUMP: a jump inserted at LOC to TO, TO in the coordinates
    /// before the insertion, as GNU computes it.
    fn insert_jump(&mut self, op: u8, loc: usize, to: usize) -> Result<(), RegError> {
        self.insert_op1(op, loc, to as i32 - loc as i32 - 3)
    }

    fn insert_jump2(&mut self, op: u8, loc: usize, to: usize, arg: i32) -> Result<(), RegError> {
        self.insert_op2(op, loc, to as i32 - loc as i32 - 3, arg)
    }

    /// SETUP_ASCII_RANGE.
    fn setup_ascii_range(
        &mut self,
        work: &mut RangeTableWork,
        map: &mut Bitmap,
        from: u32,
        to: u32,
    ) {
        for c0 in from..=to {
            let mut c1 = self.translate(c0);
            if c1 >= 0x80 {
                work.set(c1, c1);
                c1 = match char_to_unibyte(c1) {
                    Some(byte) => byte,
                    None => c0,
                };
            }
            set_list_bit(map, c1);
        }
    }

    /// SETUP_UNIBYTE_RANGE: FROM and TO are bytes 0x80..0xFF.
    fn setup_unibyte_range(
        &mut self,
        work: &mut RangeTableWork,
        map: &mut Bitmap,
        from: u32,
        to: u32,
    ) {
        let used = work.table.len();
        for c0 in from..=to {
            let c1 = unibyte_to_char(c0);
            if char_byte8_p(c1) {
                set_list_bit(map, c0);
            } else {
                let c2 = self.translate(c1);
                let byte = if c2 == c1 {
                    c0
                } else {
                    char_to_unibyte(c2).unwrap_or(c0)
                };
                set_list_bit(map, byte);
                merge_into_range_table(work, used, c2);
            }
        }
    }

    /// SETUP_MULTIBYTE_RANGE.
    fn setup_multibyte_range(
        &mut self,
        work: &mut RangeTableWork,
        map: &mut Bitmap,
        from: u32,
        to: u32,
    ) {
        let used = work.table.len();
        work.set(from, to);
        for c0 in from..=to {
            let c1 = self.translate(c0);
            let byte =
                char_to_unibyte(c1).or_else(|| if c1 != c0 { char_to_unibyte(c0) } else { None });
            if let Some(byte) = byte {
                set_list_bit(map, byte);
            }
            if c1 >= from && c1 <= to {
                continue;
            }
            merge_into_range_table(work, used, c1);
        }
    }
}

/// A bracket expression's bitmap of the bytes it matches, built apart
/// from the buffer and copied in without its trailing zero bytes.
type Bitmap = [u8; 32];

/// SET_LIST_BIT: C is a byte, or an ASCII character.
#[inline]
fn set_list_bit(map: &mut Bitmap, c: u32) {
    debug_assert!(c < 0x100);
    let c = c as u8;
    map[usize::from(c >> 3)] |= 1 << (c & 7);
}

/// The bytes of MAP up to its last nonzero one.
#[inline]
fn bitmap_used(map: &Bitmap) -> usize {
    let half = |at: usize| u128::from_le_bytes(map[at..at + 16].try_into().expect("16 bytes"));
    let (low, high) = (half(0), half(16));
    if high != 0 {
        32 - high.leading_zeros() as usize / 8
    } else {
        16 - low.leading_zeros() as usize / 8
    }
}

/// The loop SETUP_UNIBYTE_RANGE and SETUP_MULTIBYTE_RANGE share: widen a
/// range added since USED that C extends by one, else add C alone.
fn merge_into_range_table(work: &mut RangeTableWork, used: usize, c: u32) {
    let mut i = work.table.len() as isize - 2;
    while i >= used as isize {
        let index = i as usize;
        let from = work.table[index];
        let to = work.table[index + 1];
        if c + 1 >= from && c <= to + 1 {
            if c + 1 == from {
                work.table[index] -= 1;
            } else if c == to + 1 {
                work.table[index + 1] += 1;
            }
            return;
        }
        i -= 2;
    }
    work.set(c, c);
}

/// at_begline_loc_p: whether the `^' just before P (PATTERN[P - 1]) comes
/// after an alternative or the opening of a group.
fn at_begline_loc_p(pattern: &[u32], p: usize) -> bool {
    let mut prev = p as isize - 2;
    match pattern[prev as usize] {
        0x28 | 0x7c => {} // '(' '|'
        0x3a => {
            // ':' -- after a shy group, past an optional group number.
            while prev > 0 && (0x30..=0x39).contains(&pattern[prev as usize - 1]) {
                prev -= 1;
            }
            if !(prev > 1
                && pattern[prev as usize - 1] == u32::from(b'?')
                && pattern[prev as usize - 2] == u32::from(b'('))
            {
                return false;
            }
            prev -= 2;
        }
        _ => return false,
    }
    // Count the backslashes before.
    let p = prev;
    while prev > 0 && pattern[prev as usize - 1] == u32::from(b'\\') {
        prev -= 1;
    }
    (p - prev) & 1 == 1
}

/// at_endline_loc_p: whether the `$' before P is followed by `\)' or `\|'.
fn at_endline_loc_p(pattern: &[u32], p: usize) -> bool {
    pattern[p] == u32::from(b'\\')
        && p + 1 < pattern.len()
        && (pattern[p + 1] == u32::from(b')') || pattern[p + 1] == u32::from(b'|'))
}

fn group_in_compile_stack(stack: &[CompileStackElt], regnum: i64) -> bool {
    stack.iter().any(|elt| elt.regnum == regnum)
}

std::thread_local! {
    /// The compile stack and the range table work area, kept from one
    /// compilation for the next.
    static WORK: std::cell::Cell<(Vec<CompileStackElt>, RangeTableWork)> = const {
        std::cell::Cell::new((Vec::new(), RangeTableWork { table: Vec::new(), bits: 0 }))
    };
}

/// regex_compile: compile PATTERN, character codes of a string whose
/// multibyteness is MULTIBYTE, into BUFFER (a cache cell's pattern buffer,
/// which re_compile_pattern compiles over).  WHITESPACE,
/// `search-spaces-regexp', stands for each run of spaces.  TRANSLATE says
/// whether ENV's translation (case folding) applies; ENV also answers the
/// class predicates `[:space:]' and `[:word:]' read from the syntax table.
pub(crate) fn compile(
    pattern: &[u32],
    multibyte: bool,
    posix: bool,
    whitespace: Option<&[u32]>,
    translate: bool,
    env: &mut dyn MatchEnv,
    mut buffer: Vec<u8>,
) -> Result<Pattern, RegError> {
    buffer.clear();
    buffer.reserve(32);
    let mut cc = Compiler {
        b: buffer,
        translate,
        env,
        multibyte,
        re_nsub: 0,
        used_syntax: false,
        ascii_syntax_masks: None,
    };
    let (mut compile_stack, mut range_table_work) = WORK.take();
    compile_stack.clear();
    let compiled = regex_compile(
        &mut cc,
        pattern,
        posix,
        whitespace,
        &mut compile_stack,
        &mut range_table_work,
    );
    WORK.set((compile_stack, range_table_work));
    compiled?;
    Ok(Pattern {
        buffer: cc.b,
        re_nsub: cc.re_nsub as usize,
        fastmap: [false; 256],
        fastmap_accurate: false,
        fastmap_leads: FastmapLeads::Many,
        ascii_admits: None,
        can_be_null: false,
        used_syntax: cc.used_syntax,
        multibyte,
        translate,
    })
}

fn regex_compile(
    cc: &mut Compiler<'_>,
    main_pattern: &[u32],
    posix: bool,
    whitespace: Option<&[u32]>,
    compile_stack: &mut Vec<CompileStackElt>,
    range_table_work: &mut RangeTableWork,
) -> Result<(), RegError> {
    // The pattern being read: the main one, or the whitespace regexp
    // spliced in for a run of spaces (`in_subpattern').
    let mut pattern: &[u32] = main_pattern;
    let mut p = 0usize;
    let mut pend = pattern.len();
    let mut in_subpattern = false;
    let mut main_p = 0usize;

    let mut pending_exact: Option<usize> = None;
    let mut laststart: Option<usize> = None;
    let mut begalt = 0usize;
    let mut fixup_alt_jump: Option<usize> = None;

    // PEND is the end of PATTERN, whichever pattern is being read.
    macro_rules! patfetch {
        () => {{
            let Some(&c) = pattern.get(p) else {
                return Err(RegError::EEnd);
            };
            p += 1;
            c
        }};
    }

    macro_rules! fixup_alt_jump {
        () => {
            if let Some(at) = fixup_alt_jump {
                let to = cc.b.len();
                cc.store_jump(op::JUMP, at, to);
            }
        };
    }

    loop {
        if p == pend {
            if in_subpattern {
                in_subpattern = false;
                pattern = main_pattern;
                p = main_p;
                pend = pattern.len();
                continue;
            }
            break;
        }

        let mut c = patfetch!();
        // Set when a character is to be compiled as itself.
        let mut normal = false;

        match c {
            0x20 => {
                // ' ': a run of spaces may stand for `search-spaces-regexp'.
                match whitespace {
                    Some(ws) if !in_subpattern => {
                        let mut p1 = p;
                        while p1 != pend && pattern[p1] == 0x20 {
                            p1 += 1;
                        }
                        if p1 != pend
                            && (matches!(pattern[p1], 0x2a | 0x2b | 0x3f)
                                || (pattern[p1] == u32::from(b'\\')
                                    && p1 + 1 != pend
                                    && pattern[p1 + 1] == u32::from(b'{')))
                        {
                            normal = true;
                        } else {
                            in_subpattern = true;
                            main_p = p1;
                            pattern = ws;
                            p = 0;
                            pend = ws.len();
                        }
                    }
                    _ => normal = true,
                }
            }

            0x5e => {
                // '^'
                if !(p == 1 || at_begline_loc_p(pattern, p)) {
                    normal = true;
                } else {
                    // Postfix ops after ^ become literals.
                    laststart = None;
                    cc.push(op::BEGLINE)?;
                }
            }

            0x24 => {
                // '$'
                if !(p == pend || at_endline_loc_p(pattern, p)) {
                    normal = true;
                } else {
                    laststart = Some(cc.b.len());
                    cc.push(op::ENDLINE)?;
                }
            }

            0x2b | 0x3f | 0x2a => {
                // '+' '?' '*'
                let Some(start) = laststart else {
                    compile_normal(cc, &mut pending_exact, &mut laststart, pattern, p, c)?;
                    continue;
                };
                let mut zero_times_ok = false;
                let mut many_times_ok = false;
                let mut greedy = true;
                loop {
                    if c == 0x3f && (zero_times_ok || many_times_ok) {
                        greedy = false;
                    } else {
                        zero_times_ok |= c != 0x2b;
                        many_times_ok |= c != 0x3f;
                    }
                    if !(p < pend && matches!(pattern[p], 0x2a | 0x2b | 0x3f)) {
                        break;
                    }
                    c = pattern[p];
                    p += 1;
                }

                // A repetition of the empty pattern is the empty pattern.
                if start == cc.b.len() {
                    pending_exact = None;
                    continue;
                }

                if greedy {
                    if many_times_ok {
                        let simple = skip_one_char(&cc.b, start) == Some(cc.b.len());
                        let mut startoffset = 0usize;
                        let ofj = if simple || !analyze_first(&cc.b, start, cc.b.len(), None, false)
                        {
                            op::ON_FAILURE_JUMP
                        } else {
                            op::ON_FAILURE_JUMP_LOOP
                        };

                        if !zero_times_ok && simple {
                            // P+ with a simple P becomes PP*.
                            let end = cc.b.len();
                            startoffset = end - start;
                            cc.space(startoffset)?;
                            cc.b.extend_from_within(start..end);
                            zero_times_ok = true;
                        }

                        cc.space(6)?;
                        let b = cc.b.len();
                        if !zero_times_ok {
                            // A + loop.
                            cc.store_jump(ofj, b, b + 6);
                        } else {
                            cc.insert_jump(
                                if simple {
                                    op::ON_FAILURE_JUMP_SMART
                                } else {
                                    ofj
                                },
                                start + startoffset,
                                b + 6,
                            )?;
                        }
                        let b = b + 3;
                        cc.store_jump(op::JUMP, b, start + startoffset);
                    } else {
                        // A simple ? pattern.
                        let b = cc.b.len();
                        cc.insert_jump(op::ON_FAILURE_JUMP, start, b + 3)?;
                    }
                } else {
                    cc.space(7)?;
                    if many_times_ok {
                        let emptyp = analyze_first(&cc.b, start, cc.b.len(), None, false);
                        if emptyp {
                            cc.push(op::NO_OP)?;
                        }
                        let b = cc.b.len();
                        cc.store_jump(
                            if emptyp {
                                op::ON_FAILURE_JUMP_NASTYLOOP
                            } else {
                                op::ON_FAILURE_JUMP
                            },
                            b,
                            start,
                        );
                        if zero_times_ok {
                            let b = cc.b.len();
                            cc.insert_jump(op::JUMP, start, b)?;
                        }
                    } else {
                        // Non-greedy a??.
                        let b = cc.b.len();
                        cc.insert_jump(op::JUMP, start, b + 3)?;
                        cc.insert_jump(op::ON_FAILURE_JUMP, start, start + 6)?;
                    }
                }
                pending_exact = None;
            }

            0x2e => {
                // '.'
                laststart = Some(cc.b.len());
                cc.push(op::ANYCHAR)?;
            }

            0x5b => {
                // '['
                range_table_work.table.clear();
                range_table_work.bits = 0;
                if p == pend {
                    return Err(RegError::EBrack);
                }
                cc.space(34)?;
                let start = cc.b.len();
                laststart = Some(start);
                cc.push(if pattern[p] == 0x5e {
                    op::CHARSET_NOT
                } else {
                    op::CHARSET
                })?;
                if pattern[p] == 0x5e {
                    p += 1;
                }
                let p1 = p;
                cc.push(32)?;
                let mut map: Bitmap = [0; 32];

                loop {
                    let p2 = p;
                    if p == pend {
                        return Err(RegError::EBrack);
                    }
                    if let Some((class, end)) = re_wctype_parse(pattern, p) {
                        p = end;
                        if class == WcType::Error {
                            return Err(RegError::ECtype);
                        }
                        if p == pend {
                            return Err(RegError::EBrack);
                        }
                        // Each ASCII character of the class, with its
                        // translation; without one, the class's bits alone.
                        let mut admitted = cc.ascii_class_mask(class);
                        if !cc.translate {
                            for (byte, bits) in map.iter_mut().zip(admitted.to_le_bytes()) {
                                *byte |= bits;
                            }
                            admitted = 0;
                        }
                        while admitted != 0 {
                            let ch = admitted.trailing_zeros();
                            admitted &= admitted - 1;
                            set_list_bit(&mut map, ch);
                            let c1 = cc.translate(ch);
                            if c1 == ch {
                                continue;
                            }
                            if c1 < 0x80 {
                                set_list_bit(&mut map, c1);
                            } else if let Some(byte) = char_to_unibyte(c1) {
                                set_list_bit(&mut map, byte);
                            }
                        }
                        range_table_work.bits |= re_wctype_to_bit(class);
                        if matches!(class, WcType::Space | WcType::Word) {
                            cc.used_syntax = true;
                        }
                        continue;
                    }

                    // Not translated yet: TRANSLATE(X..Y) is not always
                    // TRANSLATE(X)..TRANSLATE(Y).
                    c = patfetch!();
                    if c == 0x5d && p2 != p1 {
                        break;
                    }
                    let c1;
                    // At the pattern's end, GNU reads the string's
                    // terminating NUL as p[1], not ']': the range is taken
                    // and fetching its end fails.
                    if p < pend && pattern[p] == 0x2d && pattern.get(p + 1) != Some(&0x5d) {
                        let _ = patfetch!(); // the '-'
                        c1 = patfetch!();
                        if char_byte8_p(c1) && c >= 0x80 && !char_byte8_p(c) {
                            // A range from a multibyte character to a raw
                            // byte is empty.
                            c = c1 + 1;
                        }
                    } else {
                        c1 = c;
                    }

                    if c <= c1 {
                        if c < 128 {
                            let ch = c1.min(127);
                            cc.setup_ascii_range(range_table_work, &mut map, c, ch);
                            c = ch + 1;
                            if char_byte8_p(c1) {
                                c = BYTE8_BASE + 128;
                            }
                        }
                        if c <= c1 {
                            if char_byte8_p(c) {
                                let from = c - BYTE8_BASE;
                                let to = c1 - BYTE8_BASE;
                                for byte in from..=to {
                                    set_list_bit(&mut map, byte);
                                }
                            } else if cc.multibyte {
                                cc.setup_multibyte_range(range_table_work, &mut map, c, c1);
                            } else {
                                cc.setup_unibyte_range(range_table_work, &mut map, c, c1);
                            }
                        }
                    }
                }

                // Discard the trailing zero bytes of the map.
                let size = bitmap_used(&map);
                cc.b[start + 1] = size as u8;
                cc.b.extend_from_slice(&map[..size]);

                if !range_table_work.table.is_empty() || range_table_work.bits != 0 {
                    let used = range_table_work.table.len();
                    cc.space(4 + used * 3)?;
                    cc.b[start + 1] |= 0x80;
                    cc.b.push((range_table_work.bits & 0xff) as u8);
                    cc.b.push((range_table_work.bits >> 8) as u8);
                    let count_at = cc.b.len();
                    cc.b.extend_from_slice(&[0, 0]);
                    store_number(&mut cc.b, count_at, (used / 2) as i32);
                    for &ch in &range_table_work.table {
                        cc.b.push((ch & 0o377) as u8);
                        cc.b.push(((ch >> 8) & 0o377) as u8);
                        cc.b.push((ch >> 16) as u8);
                    }
                }
            }

            0x5c => {
                // '\\'
                if p == pend {
                    return Err(RegError::EEscape);
                }
                // Not translated, so that \B stays distinct from \b.
                c = patfetch!();
                match c {
                    0x28 => {
                        // '('
                        let mut shy = false;
                        let mut regnum: i64 = 0;
                        if p + 1 < pend && pattern[p] == 0x3f {
                            let _ = patfetch!(); // the '?'
                            while !shy {
                                c = patfetch!();
                                match c {
                                    0x3a => shy = true,
                                    0x30..=0x39 => {
                                        if c == 0x30 && regnum == 0 {
                                            return Err(RegError::BadPat);
                                        }
                                        regnum = regnum
                                            .checked_mul(10)
                                            .and_then(|n| n.checked_add(i64::from(c - 0x30)))
                                            .filter(|n| *n <= i64::from(i32::MAX))
                                            .ok_or(RegError::ESize)?;
                                    }
                                    _ => return Err(RegError::BadPat),
                                }
                            }
                        }

                        if !shy {
                            cc.re_nsub += 1;
                            regnum = cc.re_nsub;
                        } else if regnum != 0 {
                            // Explicitly numbered.
                            if regnum > cc.re_nsub {
                                cc.re_nsub = regnum;
                            } else if group_in_compile_stack(compile_stack, regnum) {
                                return Err(RegError::BadPat);
                            }
                        } else {
                            regnum = -cc.re_nsub;
                        }

                        compile_stack.push(CompileStackElt {
                            begalt_offset: begalt,
                            fixup_alt_jump,
                            laststart_offset: cc.b.len(),
                            regnum,
                        });
                        if regnum <= MAX_REGNUM && regnum > 0 {
                            cc.push2(op::START_MEMORY, regnum as u8)?;
                        }
                        fixup_alt_jump = None;
                        laststart = None;
                        begalt = cc.b.len();
                        pending_exact = None;
                    }

                    0x29 => {
                        // ')'
                        if compile_stack.is_empty() {
                            return Err(RegError::ERParen);
                        }
                        fixup_alt_jump!();
                        let top = compile_stack.pop().expect("checked non-empty");
                        begalt = top.begalt_offset;
                        fixup_alt_jump = top.fixup_alt_jump;
                        laststart = Some(top.laststart_offset);
                        pending_exact = None;
                        if top.regnum <= MAX_REGNUM && top.regnum > 0 {
                            cc.push2(op::STOP_MEMORY, top.regnum as u8)?;
                        }
                    }

                    0x7c => {
                        // '|'
                        cc.space(3)?;
                        let b = cc.b.len();
                        cc.insert_jump(op::ON_FAILURE_JUMP, begalt, b + 6)?;
                        pending_exact = None;
                        fixup_alt_jump!();
                        fixup_alt_jump = Some(cc.b.len());
                        cc.space(3)?;
                        let end = cc.b.len();
                        cc.b.resize(end + 3, 0);
                        laststart = None;
                        begalt = cc.b.len();
                    }

                    0x7b => {
                        // '{'
                        let beg_interval = p;
                        let mut lower_bound: i64 = 0;
                        let mut upper_bound: i64 = -1;

                        // GET_INTERVAL_COUNT
                        macro_rules! interval_count {
                            ($num:ident) => {{
                                if p == pend {
                                    return Err(RegError::EBrace);
                                }
                                c = patfetch!();
                                while (0x30..=0x39).contains(&c) {
                                    if $num < 0 {
                                        $num = 0;
                                    }
                                    if RE_DUP_MAX / 10
                                        - i64::from(RE_DUP_MAX % 10 < i64::from(c - 0x30))
                                        < $num
                                    {
                                        return Err(RegError::ESizeBr);
                                    }
                                    $num = $num * 10 + i64::from(c - 0x30);
                                    if p == pend {
                                        return Err(RegError::EBrace);
                                    }
                                    c = patfetch!();
                                }
                            }};
                        }

                        interval_count!(lower_bound);
                        if c == 0x2c {
                            interval_count!(upper_bound);
                        } else {
                            upper_bound = lower_bound;
                        }

                        if lower_bound < 0
                            || (0 <= upper_bound && upper_bound < lower_bound)
                            || c != 0x5c
                        {
                            return Err(RegError::BadBr);
                        }
                        if p == pend {
                            return Err(RegError::EEscape);
                        }
                        let close = pattern[p];
                        p += 1;
                        if close != 0x7d {
                            return Err(RegError::BadBr);
                        }

                        let Some(start) = laststart else {
                            // unfetch_interval: match `{' literally.
                            p = beg_interval;
                            c = 0x7b;
                            compile_normal(cc, &mut pending_exact, &mut laststart, pattern, p, c)?;
                            continue;
                        };

                        if upper_bound == 0 {
                            cc.b.truncate(start);
                        } else if lower_bound == 1 && upper_bound == 1 {
                            // Match once: nothing to do.
                        } else {
                            let nbytes: usize = if upper_bound < 0 {
                                3
                            } else if upper_bound > 1 {
                                5
                            } else {
                                0
                            };
                            let mut startoffset = 0usize;
                            cc.space(20)?;

                            if lower_bound == 0 {
                                let b = cc.b.len();
                                cc.insert_jump(op::ON_FAILURE_JUMP_LOOP, start, b + 3 + nbytes)?;
                            } else {
                                let b = cc.b.len();
                                cc.insert_jump2(
                                    op::SUCCEED_N,
                                    start,
                                    b + 5 + nbytes,
                                    lower_bound as i32,
                                )?;
                                cc.insert_op2(op::SET_NUMBER_AT, start, 5, lower_bound as i32)?;
                                startoffset += 5;
                            }

                            if upper_bound < 0 {
                                let b = cc.b.len();
                                cc.store_jump(op::JUMP, b, start + startoffset);
                            } else if upper_bound > 1 {
                                let b = cc.b.len();
                                cc.store_jump2(
                                    op::JUMP_N,
                                    b,
                                    start + startoffset,
                                    (upper_bound - 1) as i32,
                                );
                                let b = cc.b.len();
                                cc.insert_op2(
                                    op::SET_NUMBER_AT,
                                    start,
                                    (b - start) as i32,
                                    (upper_bound - 1) as i32,
                                )?;
                            }
                        }
                        pending_exact = None;
                    }

                    0x3d => {
                        // '='
                        laststart = Some(cc.b.len());
                        cc.push(op::AT_DOT)?;
                    }

                    0x73 | 0x53 => {
                        // 's' 'S'
                        laststart = Some(cc.b.len());
                        let class = patfetch!();
                        cc.push2(
                            if c == 0x73 {
                                op::SYNTAXSPEC
                            } else {
                                op::NOTSYNTAXSPEC
                            },
                            if class < 0x100 {
                                syntax_spec_code(class)
                            } else {
                                0o377
                            },
                        )?;
                    }

                    0x63 | 0x43 => {
                        // 'c' 'C'
                        laststart = Some(cc.b.len());
                        let category = patfetch!();
                        cc.push2(
                            if c == 0x63 {
                                op::CATEGORYSPEC
                            } else {
                                op::NOTCATEGORYSPEC
                            },
                            category as u8,
                        )?;
                    }

                    0x77 => {
                        // 'w'
                        laststart = Some(cc.b.len());
                        cc.push2(op::SYNTAXSPEC, syntax_code::WORD)?;
                    }

                    0x57 => {
                        // 'W'
                        laststart = Some(cc.b.len());
                        cc.push2(op::NOTSYNTAXSPEC, syntax_code::WORD)?;
                    }

                    0x3c => {
                        laststart = Some(cc.b.len());
                        cc.push(op::WORDBEG)?;
                    }

                    0x3e => {
                        laststart = Some(cc.b.len());
                        cc.push(op::WORDEND)?;
                    }

                    0x5f => {
                        // '_'
                        laststart = Some(cc.b.len());
                        let next = patfetch!();
                        if next == 0x3c {
                            cc.push(op::SYMBEG)?;
                        } else if next == 0x3e {
                            cc.push(op::SYMEND)?;
                        } else {
                            return Err(RegError::BadPat);
                        }
                    }

                    0x62 => {
                        laststart = Some(cc.b.len());
                        cc.push(op::WORDBOUND)?;
                    }

                    0x42 => {
                        laststart = Some(cc.b.len());
                        cc.push(op::NOTWORDBOUND)?;
                    }

                    0x60 => {
                        // '`': postfix ops after it become literals.
                        laststart = None;
                        cc.push(op::BEGBUF)?;
                    }

                    0x27 => {
                        laststart = Some(cc.b.len());
                        cc.push(op::ENDBUF)?;
                    }

                    0x31..=0x39 => {
                        let reg = i64::from(c - 0x30);
                        if reg > cc.re_nsub || reg < 1 || group_in_compile_stack(compile_stack, reg)
                        {
                            return Err(RegError::ESubreg);
                        }
                        laststart = Some(cc.b.len());
                        cc.push2(op::DUPLICATE, reg as u8)?;
                    }

                    _ => normal = true,
                }
            }

            _ => normal = true,
        }

        if normal {
            compile_normal(cc, &mut pending_exact, &mut laststart, pattern, p, c)?;
        }
    }

    fixup_alt_jump!();
    if !compile_stack.is_empty() {
        return Err(RegError::EParen);
    }
    if !posix {
        cc.push(op::SUCCEED)?;
    }
    Ok(())
}

/// The `normal_char' case: add C to the `exactn' being built, or start one.
fn compile_normal(
    cc: &mut Compiler<'_>,
    pending_exact: &mut Option<usize>,
    laststart: &mut Option<usize>,
    pattern: &[u32],
    p: usize,
    c: u32,
) -> Result<(), RegError> {
    // The stored form of the character to match: GNU's exactn bytes.
    let stored = if cc.multibyte {
        cc.translate(c)
    } else {
        let mut byte = c;
        let c1 = unibyte_to_char(c);
        if !char_byte8_p(c1) {
            let c2 = cc.translate(c1);
            if c1 != c2
                && let Some(b) = char_to_unibyte(c2)
            {
                byte = b;
            }
        }
        unibyte_to_char(byte)
    };

    let Some(ch) = stored_char_of(stored) else {
        // A character the text stores through a side table cannot be
        // compared byte for byte: it is matched as a one-character set,
        // whose range table holds its code.
        *pending_exact = None;
        let start = cc.b.len();
        *laststart = Some(start);
        cc.space(10)?;
        cc.b.extend_from_slice(&[op::CHARSET, 0x80, 0, 0, 1, 0]);
        cc.b.push((stored & 0o377) as u8);
        cc.b.push(((stored >> 8) & 0o377) as u8);
        cc.b.push((stored >> 16) as u8);
        cc.b.push((stored & 0o377) as u8);
        cc.b.push(((stored >> 8) & 0o377) as u8);
        cc.b.push((stored >> 16) as u8);
        return Ok(());
    };

    let followed_by_repetition =
        matches!(pattern[p..], [0x2a | 0x2b | 0x3f, ..] | [0x5c, 0x7b, ..]);
    let end = cc.b.len();
    let at = match *pending_exact {
        Some(at)
            if at + usize::from(cc.b[at]) + 1 == end
                && usize::from(cc.b[at]) < 256 - MAX_MULTIBYTE_LENGTH
                && !followed_by_repetition =>
        {
            at
        }
        _ => {
            *laststart = Some(end);
            cc.push2(op::EXACTN, 0)?;
            *pending_exact = Some(end + 1);
            end + 1
        }
    };
    cc.space(MAX_MULTIBYTE_LENGTH)?;
    let len = ch.len_utf8();
    if len == 1 {
        cc.b.push(ch as u8);
    } else {
        cc.b.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
    }
    cc.b[at] += len as u8;
    Ok(())
}
