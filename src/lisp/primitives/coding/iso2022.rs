//! coding.c's ISO-2022 codec (decode_coding_iso_2022 and
//! encode_coding_iso_2022) over the attributes mule.el's
//! `define-coding-system' hands `define-coding-system-internal': the
//! initial designations, the register usage, the request alist and the
//! flag bits.  The decoder reports the `charset' runs GNU annotates the
//! decoded text with; the encoder honors a `charset' text property as
//! the preferred charset, like CODING_ANNOTATE_CHARSET_MASK does.

use super::*;
use crate::lisp::eval::coding::*;

const ISO_CODE_SO: u8 = 0x0E;
const ISO_CODE_SI: u8 = 0x0F;
const ISO_CODE_SS2_7: u8 = 0x19;
const ISO_CODE_ESC: u8 = 0x1B;
const ISO_CODE_SS2: u8 = 0x8E;
const ISO_CODE_SS3: u8 = 0x8F;
const ISO_CODE_CSI: u8 = 0x9B;
const MAX_COMPOSITION_COMPONENTS: usize = 16;

/// The attributes coding.c keeps for an iso-2022 coding system, with
/// setup_iso_safe_charsets' register table already computed.
#[derive(Clone, Debug)]
pub(crate) struct IsoAttributes {
    pub(crate) flags: u32,
    pub(crate) initial: [Option<String>; 4],
    pub(crate) charset_list: Vec<String>,
    /// charset -> graphic register (setup_iso_safe_charsets).
    pub(crate) safe: HashMap<String, u8>,
    pub(crate) ascii_compatible: bool,
    pub(crate) default_char: u32,
}

impl IsoAttributes {
    fn safe_charset(&self, charset: &str) -> bool {
        self.safe.contains_key(charset)
    }

    /// CODING_ISO_REQUEST: the register the charset asks for, if it is a
    /// usable one.
    fn request(&self, charset: &str) -> Option<u8> {
        self.safe.get(charset).copied().filter(|reg| *reg < 4)
    }
}

pub(crate) fn charset_iso_chars_96(interp: &Interpreter, charset: &str) -> bool {
    // charset.c: iso_chars_96 = (code_space[2] == 96), the size of the
    // least significant byte range.
    charset_code_space(interp, charset)
        .and_then(|bounds| bounds.first().copied())
        .is_some_and(|(min, max)| max.saturating_sub(min) + 1 == 96)
}

pub(crate) fn charset_dimension(interp: &Interpreter, charset: &str) -> u32 {
    charset_plist_property(interp, charset, ":dimension")
        .and_then(|value| value.as_integer().ok())
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| charset_code_space(interp, charset).map(|bounds| bounds.len() as u32))
        .unwrap_or(1)
}

/// The charset's ISO final byte, from its plist or (for the C-defined
/// `ascii') from the ISO charset table.
fn charset_iso_final(interp: &Interpreter, charset: &str) -> Option<u8> {
    charset_plist_property(interp, charset, ":iso-final-char")
        .and_then(|value| value.as_integer().ok())
        .and_then(|value| u8::try_from(value).ok())
        .or_else(|| {
            interp
                .iso_charset_entry_for(charset)
                .map(|(_, _, final_char)| final_char as u8)
        })
}

fn charset_iso_revision(interp: &Interpreter, charset: &str) -> Option<u8> {
    charset_plist_property(interp, charset, ":iso-revision-number")
        .and_then(|value| value.as_integer().ok())
        .and_then(|value| u8::try_from(value).ok())
}

pub(crate) fn iso_attributes(interp: &Interpreter, coding: &str) -> Option<IsoAttributes> {
    let state = interp.coding_system(coding)?;
    let args = &state.type_args;
    let mut initial: [Option<String>; 4] = Default::default();
    if let Some(items) = args.first().and_then(|value| vector_items(value).ok()) {
        for (slot, item) in initial.iter_mut().zip(items) {
            *slot = item
                .as_symbol()
                .ok()
                .and_then(|name| interp.charset_canonical_name(name));
        }
    }
    let reg_usage = args
        .get(1)
        .and_then(Value::cons_values)
        .map(|(reg94, reg96)| {
            (
                reg94.as_integer().unwrap_or(4),
                reg96.as_integer().unwrap_or(4),
            )
        })
        .unwrap_or((4, 4));
    let request: Vec<(String, u8)> = args
        .get(2)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default()
        .iter()
        .filter_map(|pair| {
            let (charset, reg) = pair.cons_values()?;
            Some((
                interp.charset_canonical_name(charset.as_symbol().ok()?)?,
                u8::try_from(reg.as_integer().ok()?).ok()?,
            ))
        })
        .collect();
    let full_support = matches!(&state.charset_list, Value::Symbol(name) if name == "iso-2022");
    let flags = args
        .get(3)
        .and_then(|flags| flags.as_integer().ok())
        .unwrap_or(0) as u32
        | if full_support {
            ISO_FLAG_FULL_SUPPORT
        } else {
            0
        };
    let charset_list: Vec<String> = if full_support {
        interp.iso_2022_charset_list()
    } else {
        state
            .charset_list
            .to_vec()
            .unwrap_or_default()
            .iter()
            .filter_map(|value| interp.charset_canonical_name(value.as_symbol().ok()?))
            .collect()
    };
    let mut safe = HashMap::new();
    if full_support {
        // setup_iso_safe_charsets, which the encoder and decoder re-run
        // once `set-charset-priority' has replaced Viso_2022_charset_list:
        // the requested register, else the 96- or 94-charset register when
        // one is usable.
        for charset in &charset_list {
            if let Some((_, reg)) = request.iter().find(|(name, _)| name == charset) {
                safe.insert(charset.clone(), *reg);
            } else if charset_iso_chars_96(interp, charset) {
                if reg_usage.1 < 4 {
                    safe.insert(charset.clone(), reg_usage.1 as u8);
                }
            } else if reg_usage.0 < 4 {
                safe.insert(charset.clone(), reg_usage.0 as u8);
            }
        }
    } else {
        // Fdefine_coding_system_internal seeds `safe_charsets' with
        // register 0 for every listed charset before the iso-2022 branch
        // runs, and setup_iso_safe_charsets returns at once when that
        // string exists: the :request registers and :reg-usage of an
        // explicit charset list never take effect (the oracle designates
        // iso-2022-kr's KSC5601 to G0 with ESC $ ( C, no locking shift).
        for charset in &charset_list {
            safe.insert(charset.clone(), 0);
        }
    }
    Some(IsoAttributes {
        flags,
        initial,
        charset_list,
        safe,
        ascii_compatible: coding_system_is_ascii_compatible(interp, coding),
        default_char: state.default_char,
    })
}

#[derive(Clone, Debug, PartialEq)]
enum Designation {
    /// CODING_ISO_DESIGNATION -1: nothing designated.
    Empty,
    /// -2: an invalid designation was seen.
    Invalid,
    Charset(String),
}

impl Designation {
    fn charset(&self) -> Option<&str> {
        match self {
            Designation::Charset(name) => Some(name),
            _ => None,
        }
    }
}

struct IsoState {
    designation: [Designation; 4],
    invocation: [i32; 2],
    single_shifting: bool,
    bol: bool,
}

impl IsoState {
    fn new(attrs: &IsoAttributes) -> Self {
        // setup_coding_system: register 0 to plane 0, register 1 to
        // plane 1 unless 7-bit, the designations at their initial state.
        let designation = std::array::from_fn(|reg| match &attrs.initial[reg] {
            Some(charset) => Designation::Charset(charset.clone()),
            None => Designation::Empty,
        });
        IsoState {
            designation,
            invocation: [
                0,
                if attrs.flags & ISO_FLAG_SEVEN_BITS != 0 {
                    -1
                } else {
                    1
                },
            ],
            single_shifting: false,
            bol: true,
        }
    }

    /// CODING_ISO_INVOKED_CHARSET.
    fn invoked_charset(&self, plane: usize) -> Option<String> {
        let reg = self.invocation[plane];
        if reg < 0 {
            return None;
        }
        self.designation[reg as usize].charset().map(str::to_string)
    }

    fn designated(&self, reg: usize) -> bool {
        matches!(self.designation[reg], Designation::Charset(_))
    }
}

/// The `charset' runs decode_coding_iso_2022 annotates: a run opens at
/// the first non-ASCII charset character and closes when a character of
/// a different non-ASCII charset appears (ASCII never closes one).
struct CharsetRuns {
    last: Option<String>,
    last_offset: usize,
    spans: Vec<(usize, usize, String)>,
}

impl CharsetRuns {
    fn new() -> Self {
        CharsetRuns {
            last: None,
            last_offset: 0,
            spans: Vec::new(),
        }
    }

    fn note(&mut self, charset: &str, offset: usize) {
        if charset == "ascii" || self.last.as_deref() == Some(charset) {
            return;
        }
        self.close(offset);
        self.last = Some(charset.to_string());
        self.last_offset = offset;
    }

    fn close(&mut self, offset: usize) {
        if let Some(previous) = self.last.take()
            && offset > self.last_offset
        {
            self.spans.push((self.last_offset, offset, previous));
        }
    }

    fn finish(mut self, offset: usize) -> Vec<(usize, usize, String)> {
        self.close(offset);
        self.spans
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ComposingState {
    No,
    Char,
    Rule,
    ComponentChar,
    ComponentRule,
}

enum CompositionElement {
    Char(char),
    Rule(Vec<u8>),
    AltEnd,
}

struct CompositionStatus {
    state: ComposingState,
    method: u8,
    nchars: usize,
    ncomps: usize,
    elements: Vec<CompositionElement>,
}

impl CompositionStatus {
    fn new() -> Self {
        CompositionStatus {
            state: ComposingState::No,
            method: b'0',
            nchars: 0,
            ncomps: 0,
            elements: Vec::new(),
        }
    }
}

pub(crate) struct Decoded {
    pub(crate) text: String,
    pub(crate) charsets: Vec<(usize, usize, String)>,
}

struct Output {
    text: String,
    char_offset: usize,
}

impl Output {
    fn push_char(&mut self, ch: char) {
        self.text.push(ch);
        self.char_offset += 1;
    }

    fn push_byte(&mut self, byte: u8) {
        self.push_char(if byte.is_ascii() {
            char::from(byte)
        } else {
            raw_byte_regex_char(byte)
        });
    }
}

/// finish_composition: the composition in progress is invalid, so its
/// start sequence and everything stored for it come back as text.
fn finish_composition(out: &mut Output, cmp: &mut CompositionStatus) {
    if cmp.state == ComposingState::No {
        return;
    }
    out.push_char(char::from(ISO_CODE_ESC));
    out.push_char(char::from(cmp.method));
    for element in std::mem::take(&mut cmp.elements) {
        match element {
            CompositionElement::Char(ch) => out.push_char(ch),
            CompositionElement::Rule(bytes) => {
                for byte in bytes {
                    out.push_byte(byte);
                }
            }
            CompositionElement::AltEnd => {
                out.push_char(char::from(ISO_CODE_ESC));
                out.push_char('0');
            }
        }
    }
    cmp.state = ComposingState::No;
}

pub(crate) fn decode(interp: &Interpreter, bytes: &[u8], coding: &str) -> Decoded {
    let Some(attrs) = iso_attributes(interp, coding) else {
        return Decoded {
            text: decode_raw_text_bytes(bytes),
            charsets: Vec::new(),
        };
    };
    let flags = attrs.flags;
    let mut state = IsoState::new(&attrs);
    let mut charset_id_0 = state.invoked_charset(0);
    let mut charset_id_1 = state.invoked_charset(1);
    let mut runs = CharsetRuns::new();
    let mut cmp = CompositionStatus::new();
    let mut out = Output {
        text: String::new(),
        char_offset: 0,
    };
    let mut extsegment_len = 0usize;
    let mut embedded_utf_8 = false;
    let use_roman = flags & ISO_FLAG_USE_ROMAN != 0;
    let use_oldjis = flags & ISO_FLAG_USE_OLDJIS != 0;
    let roman = interp.charset_canonical_name("latin-jisx0201");
    let jisx0208_1978 = interp.charset_canonical_name("japanese-jisx0208-1978");
    let jisx0208 = interp.charset_canonical_name("japanese-jisx0208");
    let end = bytes.len();
    let mut p = 0usize;
    let mut src_base;

    // DECODE_DESIGNATION: returns false when the escape sequence should be
    // kept (chars_96 = -1 in the C macro).
    let decode_designation =
        |state: &mut IsoState, reg: usize, dim: i64, chars_96: bool, final_char: u8| -> bool {
            let charset = if !(b'0'..128).contains(&final_char) {
                None
            } else {
                interp.iso_charset_for(dim, chars_96, final_char)
            };
            let Some(mut id) = charset.filter(|charset| attrs.safe_charset(charset)) else {
                state.designation[reg] = Designation::Invalid;
                return false;
            };
            let prev = state.designation[reg].clone();
            if Some(&id) == roman.as_ref() {
                if use_roman {
                    id = "ascii".into();
                }
            } else if Some(&id) == jisx0208_1978.as_ref()
                && use_oldjis
                && let Some(modern) = &jisx0208
            {
                id = modern.clone();
            }
            state.designation[reg] = Designation::Charset(id.clone());
            !(prev == Designation::Invalid && id == "ascii")
        };

    'main: loop {
        src_base = p;
        let mut invalid = false;
        'step: {
            let Some(&c1) = bytes.get(p) else {
                break 'main;
            };
            p += 1;

            if extsegment_len > 0 {
                out.push_byte(c1);
                extsegment_len -= 1;
                break 'step;
            }

            if embedded_utf_8 {
                if c1 == ISO_CODE_ESC {
                    if p + 1 >= end {
                        break 'main;
                    }
                    out.push_char(char::from(ISO_CODE_ESC));
                    if bytes[p] == b'%' && bytes[p + 1] == b'@' {
                        p += 2;
                        out.push_char('%');
                        out.push_char('@');
                        embedded_utf_8 = false;
                    }
                } else {
                    out.push_byte(c1);
                }
                break 'step;
            }

            if matches!(
                cmp.state,
                ComposingState::Rule | ComposingState::ComponentRule
            ) && c1 != ISO_CODE_ESC
            {
                // DECODE_COMPOSITION_RULE / STORE_COMPOSITION_RULE.
                let Some(rule) = c1.checked_sub(32) else {
                    invalid = true;
                    break 'step;
                };
                let bytes_of_rule = if rule < 81 {
                    vec![c1]
                } else {
                    let Some(&b) = bytes.get(p) else {
                        break 'main;
                    };
                    p += 1;
                    let gref = rule - 81;
                    let nref = b.wrapping_sub(32);
                    if gref >= 12 || b < 32 || nref >= 12 {
                        invalid = true;
                        break 'step;
                    }
                    vec![c1, b]
                };
                cmp.elements.push(CompositionElement::Rule(bytes_of_rule));
                cmp.state = match cmp.state {
                    ComposingState::Rule => ComposingState::Char,
                    _ => ComposingState::ComponentChar,
                };
                break 'step;
            }

            // The charset and first position code of the character to
            // produce, after the switch over iso_code_class.
            let charset: String;
            let mut first = c1;
            match c1 {
                0x20 | 0x7F => {
                    charset = match &charset_id_0 {
                        Some(id) if charset_iso_chars_96(interp, id) => id.clone(),
                        _ => "ascii".into(),
                    };
                }
                0x21..=0x7E => {
                    charset = charset_id_0.clone().unwrap_or_else(|| "ascii".into());
                }
                0xA0 | 0xFF => match &charset_id_1 {
                    Some(id)
                        if charset_iso_chars_96(interp, id) && flags & ISO_FLAG_SEVEN_BITS == 0 =>
                    {
                        charset = id.clone();
                    }
                    _ => {
                        invalid = true;
                        break 'step;
                    }
                },
                0xA1..=0xFE => {
                    let Some(id) = &charset_id_1 else {
                        invalid = true;
                        break 'step;
                    };
                    charset = id.clone();
                }
                ISO_CODE_SO => {
                    if flags & ISO_FLAG_LOCKING_SHIFT == 0 || !state.designated(1) {
                        invalid = true;
                        break 'step;
                    }
                    state.invocation[0] = 1;
                    charset_id_0 = state.invoked_charset(0);
                    break 'step;
                }
                ISO_CODE_SI => {
                    if flags & ISO_FLAG_LOCKING_SHIFT == 0 {
                        invalid = true;
                        break 'step;
                    }
                    state.invocation[0] = 0;
                    charset_id_0 = state.invoked_charset(0);
                    break 'step;
                }
                0x80..=0x9F if !matches!(c1, ISO_CODE_SS2 | ISO_CODE_SS3 | ISO_CODE_CSI) => {
                    invalid = true;
                    break 'step;
                }
                _ => {
                    // Control codes (including ESC, SS2/SS3/CSI and the
                    // 7-bit SS2) and escape sequences.
                    let mut escape = match c1 {
                        ISO_CODE_ESC => {
                            let Some(&next) = bytes.get(p) else {
                                break 'main;
                            };
                            p += 1;
                            next
                        }
                        ISO_CODE_SS2_7 => {
                            if flags & ISO_FLAG_SEVEN_BITS == 0
                                || flags & ISO_FLAG_SINGLE_SHIFT == 0
                            {
                                invalid = true;
                                break 'step;
                            }
                            b'N'
                        }
                        ISO_CODE_SS2 | ISO_CODE_SS3 => {
                            if flags & ISO_FLAG_SINGLE_SHIFT == 0 {
                                invalid = true;
                                break 'step;
                            }
                            if c1 == ISO_CODE_SS2 { b'N' } else { b'O' }
                        }
                        ISO_CODE_CSI => b'[',
                        _ => {
                            // ISO_control_0.
                            finish_composition(&mut out, &mut cmp);
                            out.push_byte(c1);
                            break 'step;
                        }
                    };
                    // label_escape_sequence, re-entered after a revision
                    // prefix.
                    loop {
                        match escape {
                            b'&' => {
                                let Some(&rev) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                if !(b'@'..=b'~').contains(&rev) {
                                    invalid = true;
                                    break 'step;
                                }
                                let Some(&esc) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                if esc != ISO_CODE_ESC {
                                    invalid = true;
                                    break 'step;
                                }
                                let Some(&next) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                escape = next;
                                continue;
                            }
                            b'$' => {
                                if flags & ISO_FLAG_DESIGNATION == 0 {
                                    invalid = true;
                                    break 'step;
                                }
                                let Some(&mut_c) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                let (reg, chars96, final_char) = match mut_c {
                                    b'@'..=b'B' => (0usize, false, mut_c),
                                    0x28..=0x2F => {
                                        let Some(&final_char) = bytes.get(p) else {
                                            break 'main;
                                        };
                                        p += 1;
                                        if mut_c <= 0x2B {
                                            ((mut_c - 0x28) as usize, false, final_char)
                                        } else {
                                            ((mut_c - 0x2C) as usize, true, final_char)
                                        }
                                    }
                                    _ => {
                                        invalid = true;
                                        break 'step;
                                    }
                                };
                                let keep =
                                    decode_designation(&mut state, reg, 2, chars96, final_char);
                                if reg == 0 {
                                    charset_id_0 = state.invoked_charset(0);
                                } else if reg == 1 {
                                    charset_id_1 = state.invoked_charset(1);
                                }
                                if !keep {
                                    invalid = true;
                                }
                                break 'step;
                            }
                            b'n' | b'o' => {
                                let reg = if escape == b'n' { 2 } else { 3 };
                                if flags & ISO_FLAG_LOCKING_SHIFT == 0 || !state.designated(reg) {
                                    invalid = true;
                                    break 'step;
                                }
                                state.invocation[0] = reg as i32;
                                charset_id_0 = state.invoked_charset(0);
                                break 'step;
                            }
                            b'N' | b'O' => {
                                let reg = if escape == b'N' { 2 } else { 3 };
                                if flags & ISO_FLAG_SINGLE_SHIFT == 0 || !state.designated(reg) {
                                    invalid = true;
                                    break 'step;
                                }
                                charset = state.designation[reg]
                                    .charset()
                                    .map(str::to_string)
                                    .unwrap_or_else(|| "ascii".into());
                                let Some(&next) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                first = next;
                                if first < 0x20
                                    || (0x80..0xA0).contains(&first)
                                    || (flags & ISO_FLAG_SEVEN_BITS == 0
                                        && if flags & ISO_FLAG_LEVEL_4 != 0 {
                                            first >= 0x80
                                        } else {
                                            first < 0x80
                                        })
                                {
                                    invalid = true;
                                    break 'step;
                                }
                                break;
                            }
                            b'0' | b'2' | b'3' | b'4' => {
                                if flags & ISO_FLAG_COMPOSITION == 0 {
                                    invalid = true;
                                    break 'step;
                                }
                                runs.close(out.char_offset);
                                // DECODE_COMPOSITION_START.
                                if escape == b'0'
                                    && ((cmp.state == ComposingState::ComponentChar
                                        && cmp.method == b'3')
                                        || (cmp.state == ComposingState::ComponentRule
                                            && cmp.method == b'4'))
                                {
                                    cmp.elements.push(CompositionElement::AltEnd);
                                    cmp.state = ComposingState::Char;
                                } else {
                                    finish_composition(&mut out, &mut cmp);
                                    cmp.method = escape;
                                    cmp.state = if escape <= b'2' {
                                        ComposingState::Char
                                    } else {
                                        ComposingState::ComponentChar
                                    };
                                    cmp.nchars = 0;
                                    cmp.ncomps = 0;
                                    cmp.elements.clear();
                                }
                                break 'step;
                            }
                            b'1' => {
                                if cmp.state == ComposingState::No {
                                    invalid = true;
                                    break 'step;
                                }
                                // DECODE_COMPOSITION_END.
                                if cmp.nchars == 0
                                    || ((cmp.state == ComposingState::Char) == (cmp.method == b'2'))
                                {
                                    finish_composition(&mut out, &mut cmp);
                                    invalid = true;
                                    break 'step;
                                }
                                // The composed characters become text; the
                                // alternate characters and rules stay in the
                                // (unported) composition annotation.
                                let elements = std::mem::take(&mut cmp.elements);
                                let text_start = if cmp.method >= b'3' {
                                    elements
                                        .iter()
                                        .position(|element| {
                                            matches!(element, CompositionElement::AltEnd)
                                        })
                                        .map_or(elements.len(), |index| index + 1)
                                } else {
                                    0
                                };
                                for element in &elements[text_start..] {
                                    if let CompositionElement::Char(ch) = element {
                                        out.push_char(*ch);
                                    }
                                }
                                cmp.state = ComposingState::No;
                                break 'step;
                            }
                            b'[' => {
                                if flags & ISO_FLAG_DIRECTION == 0 {
                                    invalid = true;
                                    break 'step;
                                }
                                let Some(&next) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                match next {
                                    b']' => {}
                                    b'0' | b'1' | b'2' => {
                                        let Some(&close) = bytes.get(p) else {
                                            break 'main;
                                        };
                                        p += 1;
                                        if close != b']' {
                                            invalid = true;
                                            break 'step;
                                        }
                                    }
                                    _ => {
                                        invalid = true;
                                        break 'step;
                                    }
                                }
                                break 'step;
                            }
                            b'%' => {
                                let Some(&next) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                if next == b'/' {
                                    let Some(&dim) = bytes.get(p) else {
                                        break 'main;
                                    };
                                    p += 1;
                                    if !(b'0'..=b'4').contains(&dim) {
                                        invalid = true;
                                        break 'step;
                                    }
                                    let Some(&m) = bytes.get(p) else {
                                        break 'main;
                                    };
                                    p += 1;
                                    if m < 128 {
                                        invalid = true;
                                        break 'step;
                                    }
                                    let Some(&l) = bytes.get(p) else {
                                        break 'main;
                                    };
                                    p += 1;
                                    if l < 128 {
                                        invalid = true;
                                        break 'step;
                                    }
                                    out.push_char(char::from(ISO_CODE_ESC));
                                    out.push_char('%');
                                    out.push_char('/');
                                    out.push_char(char::from(dim));
                                    out.push_byte(m);
                                    out.push_byte(l);
                                    extsegment_len =
                                        usize::from(m - 128) * 128 + usize::from(l - 128);
                                } else if next == b'G' {
                                    out.push_char(char::from(ISO_CODE_ESC));
                                    out.push_char('%');
                                    out.push_char('G');
                                    embedded_utf_8 = true;
                                } else {
                                    invalid = true;
                                }
                                break 'step;
                            }
                            _ => {
                                if flags & ISO_FLAG_DESIGNATION == 0 {
                                    invalid = true;
                                    break 'step;
                                }
                                let (reg, chars96) = match escape {
                                    0x28..=0x2B => ((escape - 0x28) as usize, false),
                                    0x2C..=0x2F => ((escape - 0x2C) as usize, true),
                                    _ => {
                                        invalid = true;
                                        break 'step;
                                    }
                                };
                                let Some(&final_char) = bytes.get(p) else {
                                    break 'main;
                                };
                                p += 1;
                                let keep =
                                    decode_designation(&mut state, reg, 1, chars96, final_char);
                                if reg == 0 {
                                    charset_id_0 = state.invoked_charset(0);
                                } else if reg == 1 {
                                    charset_id_1 = state.invoked_charset(1);
                                }
                                if !keep {
                                    invalid = true;
                                }
                                break 'step;
                            }
                        }
                    }
                }
            }

            if cmp.state == ComposingState::No {
                runs.note(&charset, out.char_offset);
            }

            // Now we know CHARSET and the first position code; fetch the
            // second and third codes if the charset needs them.
            let dim = charset_dimension(interp, &charset);
            let mut code = u32::from(first);
            if dim > 1 {
                let Some(&c2) = bytes.get(p) else {
                    break 'main;
                };
                p += 1;
                if c2 < 0x20 || (0x80..0xA0).contains(&c2) || (first & 0x80) != (c2 & 0x80) {
                    invalid = true;
                    break 'step;
                }
                if dim == 2 {
                    code = (code << 8) | u32::from(c2);
                } else {
                    let Some(&c3) = bytes.get(p) else {
                        break 'main;
                    };
                    p += 1;
                    if c3 < 0x20 || (0x80..0xA0).contains(&c3) || (first & 0x80) != (c3 & 0x80) {
                        invalid = true;
                        break 'step;
                    }
                    // coding.c:3909 really combines C2 twice for a
                    // three-byte charset.
                    code = (code << 16) | (u32::from(c2) << 8) | u32::from(c2);
                }
            }
            code &= 0x7F7F7F;
            let decoded = decode_charset_code(interp, &charset, code).and_then(char::from_u32);
            match decoded {
                None => {
                    finish_composition(&mut out, &mut cmp);
                    for &byte in &bytes[src_base..p] {
                        out.push_byte(byte);
                    }
                }
                Some(ch) if cmp.state == ComposingState::No => out.push_char(ch),
                Some(ch)
                    if (if cmp.state == ComposingState::Char {
                        cmp.nchars
                    } else {
                        cmp.ncomps
                    }) >= MAX_COMPOSITION_COMPONENTS =>
                {
                    // Too long composition.
                    finish_composition(&mut out, &mut cmp);
                    out.push_char(ch);
                }
                Some(ch) => {
                    // STORE_COMPOSITION_CHAR.
                    cmp.elements.push(CompositionElement::Char(ch));
                    if cmp.state == ComposingState::Char {
                        cmp.nchars += 1;
                    } else {
                        cmp.ncomps += 1;
                    }
                    if cmp.method == b'2'
                        || (cmp.method == b'4' && cmp.state == ComposingState::ComponentChar)
                    {
                        cmp.state = match cmp.state {
                            ComposingState::Char => ComposingState::Rule,
                            ComposingState::ComponentChar => ComposingState::ComponentRule,
                            other => other,
                        };
                    }
                }
            }
        }
        if invalid {
            // invalid_code: the byte at the start of the sequence comes
            // through as itself, and the designation state resets to the
            // safest one (ASCII in G0, invoked to plane 0).
            finish_composition(&mut out, &mut cmp);
            p = src_base + 1;
            out.push_byte(bytes[src_base]);
            state.invocation[0] = 0;
            state.designation[0] = Designation::Charset("ascii".into());
            charset_id_0 = Some("ascii".into());
        }
    }
    // no_more_source with the last block: an unfinished composition is
    // invalid, and decode_coding flushes the unconsumed tail as bytes.
    finish_composition(&mut out, &mut cmp);
    for &byte in &bytes[src_base..] {
        out.push_byte(byte);
    }
    let charsets = runs.finish(out.char_offset);
    Decoded {
        text: out.text,
        charsets,
    }
}

/// char_charset over an explicit charset list: the first charset that
/// encodes C.
pub(crate) fn char_charset_in(
    interp: &Interpreter,
    charset_list: &[String],
    ch: u32,
) -> Option<(String, u32)> {
    charset_list.iter().find_map(|charset| {
        encode_charset_char(interp, charset, ch).map(|code| (charset.clone(), code))
    })
}

struct Encoder<'a> {
    interp: &'a Interpreter,
    attrs: &'a IsoAttributes,
    state: IsoState,
    out: Vec<u8>,
}

impl Encoder<'_> {
    fn flags(&self) -> u32 {
        self.attrs.flags
    }

    /// ENCODE_DESIGNATION.
    fn encode_designation(&mut self, charset: &str, reg: usize) {
        let final_char = charset_iso_final(self.interp, charset).unwrap_or(b'B');
        let chars_96 = charset_iso_chars_96(self.interp, charset);
        let intermediate_94 = b"()*+";
        let intermediate_96 = b",-./";
        if self.flags() & ISO_FLAG_REVISION != 0
            && let Some(revision) = charset_iso_revision(self.interp, charset)
        {
            self.out.extend([ISO_CODE_ESC, b'&', b'@' + revision]);
        }
        self.out.push(ISO_CODE_ESC);
        if charset_dimension(self.interp, charset) == 1 {
            self.out.push(if chars_96 {
                intermediate_96[reg]
            } else {
                intermediate_94[reg]
            });
        } else {
            self.out.push(b'$');
            if !chars_96 {
                if self.flags() & ISO_FLAG_LONG_FORM != 0
                    || reg != 0
                    || !(b'@'..=b'B').contains(&final_char)
                {
                    self.out.push(intermediate_94[reg]);
                }
            } else {
                self.out.push(intermediate_96[reg]);
            }
        }
        self.out.push(final_char);
        self.state.designation[reg] = Designation::Charset(charset.to_string());
    }

    fn shift_in(&mut self) {
        self.out.push(ISO_CODE_SI);
        self.state.invocation[0] = 0;
    }

    /// encode_invocation_designation.
    fn encode_invocation_designation(&mut self, charset: &str) {
        let reg = match self
            .state
            .designation
            .iter()
            .position(|designation| designation.charset() == Some(charset))
        {
            Some(reg) => reg,
            None => {
                let reg = usize::from(self.attrs.request(charset).unwrap_or(0));
                self.encode_designation(charset, reg);
                reg
            }
        };
        if self.state.invocation[0] != reg as i32 && self.state.invocation[1] != reg as i32 {
            match reg {
                0 => self.shift_in(),
                1 => {
                    self.out.push(ISO_CODE_SO);
                    self.state.invocation[0] = 1;
                }
                2 | 3 => {
                    if self.flags() & ISO_FLAG_SINGLE_SHIFT != 0 {
                        if self.flags() & ISO_FLAG_SEVEN_BITS != 0 {
                            self.out
                                .extend([ISO_CODE_ESC, if reg == 2 { b'N' } else { b'O' }]);
                        } else {
                            self.out
                                .push(if reg == 2 { ISO_CODE_SS2 } else { ISO_CODE_SS3 });
                        }
                        self.state.single_shifting = true;
                    } else {
                        // coding.c's ENCODE_LOCKING_SHIFT_3 emits ESC n too.
                        self.out.extend([ISO_CODE_ESC, b'n']);
                        self.state.invocation[0] = reg as i32;
                    }
                }
                _ => {}
            }
        }
    }

    /// ENCODE_RESET_PLANE_AND_REGISTER.
    fn reset_plane_and_register(&mut self) {
        if self.state.invocation[0] != 0 {
            self.shift_in();
        }
        for reg in 0..4 {
            if let Some(initial) = self.attrs.initial[reg].clone()
                && self.state.designation[reg].charset() != Some(initial.as_str())
            {
                self.encode_designation(&initial, reg);
            }
        }
    }

    /// ENCODE_ISO_CHARACTER.
    fn encode_iso_character(&mut self, charset: &str, code: u32) {
        let mut charset = charset.to_string();
        let dimension = charset_dimension(self.interp, &charset);
        if dimension == 1 {
            if self.flags() & ISO_FLAG_USE_ROMAN != 0
                && charset == "ascii"
                && let Some(roman) = self.interp.charset_canonical_name("latin-jisx0201")
            {
                charset = roman;
            }
        } else if self.flags() & ISO_FLAG_USE_OLDJIS != 0
            && Some(charset.as_str())
                == self
                    .interp
                    .charset_canonical_name("japanese-jisx0208")
                    .as_deref()
            && let Some(old) = self.interp.charset_canonical_name("japanese-jisx0208-1978")
        {
            charset = old;
        }
        let bytes: Vec<u8> = if dimension == 1 {
            vec![(code & 0xFF) as u8]
        } else {
            vec![((code >> 8) & 0xFF) as u8, (code & 0xFF) as u8]
        };
        for _ in 0..6 {
            if self.state.single_shifting {
                if self.flags() & ISO_FLAG_SEVEN_BITS != 0 {
                    self.out.extend(bytes.iter().map(|byte| byte & 0x7F));
                } else {
                    self.out.extend(bytes.iter().map(|byte| byte | 0x80));
                }
                self.state.single_shifting = false;
                return;
            } else if self.state.invoked_charset(0).as_deref() == Some(charset.as_str()) {
                self.out.extend(bytes.iter().map(|byte| byte & 0x7F));
                return;
            } else if self.state.invoked_charset(1).as_deref() == Some(charset.as_str()) {
                self.out.extend(bytes.iter().map(|byte| byte | 0x80));
                return;
            }
            self.encode_invocation_designation(&charset);
        }
    }

    /// encode_designation_at_bol: designate, ahead of the line, the
    /// charsets its characters request.
    fn encode_designation_at_bol(&mut self, chars: &[u32]) {
        let mut registers: [Option<String>; 4] = Default::default();
        let mut found = 0;
        for &c in chars {
            if found >= 4 {
                break;
            }
            if c == u32::from(b'\n') {
                break;
            }
            let Some((charset, _)) = char_charset_in(self.interp, &self.attrs.charset_list, c)
            else {
                continue;
            };
            if let Some(reg) = self.attrs.request(&charset)
                && registers[usize::from(reg)].is_none()
            {
                found += 1;
                registers[usize::from(reg)] = Some(charset);
            }
        }
        if found > 0 {
            for (reg, requested) in registers.iter().enumerate() {
                if let Some(charset) = requested.clone()
                    && self.state.designation[reg].charset() != Some(charset.as_str())
                {
                    self.encode_designation(&charset, reg);
                }
            }
        }
    }
}

/// encode_coding_iso_2022.  TEXT already carries the eol convention;
/// EOL_TYPE says which (2 = mac, whose CR ends a line).  PREFERRED holds,
/// per character, the `charset' text property GNU would have consumed as
/// an annotation.
pub(crate) fn encode(
    interp: &Interpreter,
    text: &str,
    coding: &str,
    eol_type: Option<i64>,
    preferred: &[Option<String>],
) -> Result<Vec<u8>, LispError> {
    let Some(attrs) = iso_attributes(interp, coding) else {
        return encode_raw_text_bytes(text);
    };
    let flags = attrs.flags;
    let ascii_compatible =
        attrs.ascii_compatible && flags & (ISO_FLAG_DESIGNATION | ISO_FLAG_LOCKING_SHIFT) == 0;
    let chars: Vec<u32> = text
        .chars()
        .map(|ch| {
            raw_byte_from_regex_char(ch)
                .map(|byte| RAW_BYTE_REGEX_BASE + u32::from(byte))
                .unwrap_or(ch as u32)
        })
        .collect();
    let mut encoder = Encoder {
        interp,
        state: IsoState::new(&attrs),
        attrs: &attrs,
        out: Vec::with_capacity(text.len()),
    };
    let mut bol_designation = flags & ISO_FLAG_DESIGNATE_AT_BOL != 0 && encoder.state.bol;
    for (index, &c) in chars.iter().enumerate() {
        if bol_designation {
            encoder.encode_designation_at_bol(&chars[index..]);
            bol_designation = false;
        }
        if c < 0x20 || c == 0x7F {
            if c == u32::from(b'\n') || (c == u32::from(b'\r') && eol_type == Some(2)) {
                if flags & ISO_FLAG_RESET_AT_EOL != 0 {
                    encoder.reset_plane_and_register();
                }
                if flags & ISO_FLAG_INIT_AT_BOL != 0 {
                    for reg in 0..4 {
                        encoder.state.designation[reg] = match &attrs.initial[reg] {
                            Some(charset) => Designation::Charset(charset.clone()),
                            None => Designation::Empty,
                        };
                    }
                }
                bol_designation = flags & ISO_FLAG_DESIGNATE_AT_BOL != 0;
            } else if flags & ISO_FLAG_RESET_AT_CNTL != 0 {
                encoder.reset_plane_and_register();
            }
            encoder.out.push(c as u8);
        } else if c < 0x80 {
            if ascii_compatible {
                encoder.out.push(c as u8);
            } else {
                encoder.encode_iso_character("ascii", c);
            }
        } else if (RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&c) {
            encoder.out.push((c - RAW_BYTE_REGEX_BASE) as u8);
        } else {
            let mut found = preferred
                .get(index)
                .and_then(Option::as_deref)
                .and_then(|name| interp.charset_canonical_name(name))
                .filter(|charset| attrs.charset_list.contains(charset))
                .and_then(|charset| {
                    encode_charset_char(interp, &charset, c).map(|code| (charset, code))
                })
                .or_else(|| char_charset_in(interp, &attrs.charset_list, c));
            if found.is_none() {
                // CODING_MODE_SAFE_ENCODING (the `safe' flag) writes the
                // inhibit-substitution marker `?'; otherwise the default
                // character stands in.
                found = if flags & ISO_FLAG_SAFE != 0 {
                    Some(("ascii".into(), u32::from(b'?')))
                } else {
                    char_charset_in(interp, &attrs.charset_list, attrs.default_char)
                        .or_else(|| Some(("ascii".into(), u32::from(b' '))))
                };
            }
            if let Some((charset, code)) = found {
                encoder.encode_iso_character(&charset, code);
            }
        }
    }
    if flags & ISO_FLAG_RESET_AT_EOL != 0 {
        encoder.reset_plane_and_register();
    }
    Ok(encoder.out)
}

/// Whether the iso-2022 coding can encode character C (raw bytes and
/// ASCII always can).
pub(crate) fn char_encodable(interp: &Interpreter, coding: &str, c: u32) -> bool {
    if c < 0x80 || (RAW_BYTE_REGEX_BASE + 0x80..=RAW_BYTE_REGEX_BASE + 0xFF).contains(&c) {
        return true;
    }
    iso_attributes(interp, coding)
        .is_some_and(|attrs| char_charset_in(interp, &attrs.charset_list, c).is_some())
}
