//! coding.c's ISO-2022 codec (decode_coding_iso_2022 and
//! encode_coding_iso_2022) over the attributes mule.el's
//! `define-coding-system' hands `define-coding-system-internal': the
//! initial designations, the register usage, the request alist and the
//! flag bits.  The decoder reports the `charset' runs GNU annotates the
//! decoded text with; the encoder honors a `charset' text property as
//! the preferred charset, like CODING_ANNOTATE_CHARSET_MASK does.

use super::units::{CallEnd, CallRuns, MAX_ANNOTATION_LENGTH, Units, charbuf_size};
use super::*;
use crate::lisp::eval::coding::*;
use crate::lisp::types::Kind;

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
#[derive(Clone)]
pub(crate) struct IsoAttributes {
    pub(crate) flags: u32,
    pub(crate) initial: [Option<String>; 4],
    pub(crate) charset_list: Vec<String>,
    /// charset -> graphic register (setup_iso_safe_charsets).
    pub(crate) safe: HashMap<String, u8>,
    pub(crate) ascii_compatible: bool,
    pub(crate) default_char: u32,
    /// The charset list's records, resolved once (setup_coding_system).
    charsets: Vec<std::rc::Rc<CharsetInfo>>,
    /// latin-jisx0201, japanese-jisx0208 and japanese-jisx0208-1978, for
    /// the use-roman and use-oldjis flags.
    roman: Option<String>,
    jisx0208: Option<String>,
    jisx0208_1978: Option<String>,
}

impl IsoAttributes {
    /// char_charset (C, CODING_ATTR_CHARSET_LIST, &code): the first of
    /// the coding's charsets that has C, with its code.
    /// char_charset over the coding's charset list; C is a character code.
    fn char_charset(&self, interp: &Interpreter, c: u32) -> Option<(&CharsetInfo, u32)> {
        self.charsets
            .iter()
            .find_map(|charset| charset.encode(interp, c).map(|code| (&**charset, code)))
    }

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
    charset_info(interp, charset).map_or(1, |info| info.dimension)
}

/// The charset's ISO final byte (CHARSET_ISO_FINAL), else the ISO charset
/// table's.
pub(super) fn charset_iso_final(interp: &Interpreter, charset: &str) -> Option<u8> {
    charset_info(interp, charset)
        .and_then(|info| info.iso_final)
        .or_else(|| {
            interp
                .iso_charset_entry_for(charset)
                .map(|(_, _, final_char)| final_char as u8)
        })
}

fn charset_iso_revision(interp: &Interpreter, charset: &str) -> Option<u8> {
    charset_info(interp, charset).and_then(|info| info.iso_revision)
}

pub(crate) fn iso_attributes(
    interp: &Interpreter,
    coding: &str,
) -> Option<std::rc::Rc<IsoAttributes>> {
    let state = interp.coding_system(coding)?;
    state.derived.iso(interp.charset_epoch.get(), || {
        make_iso_attributes(interp, coding)
    })
}

/// setup_coding_system's ISO-2022 attributes of CODING.
fn make_iso_attributes(interp: &Interpreter, coding: &str) -> Option<IsoAttributes> {
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
                interp.charset_canonical_name(charset.symbol_str()?)?,
                u8::try_from(reg.integer_value()?).ok()?,
            ))
        })
        .collect();
    let full_support =
        matches!(state.charset_list.kind(), Kind::Symbol(name) if name == "iso-2022");
    let flags = args
        .get(3)
        .and_then(|flags| flags.integer_value())
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
            .filter_map(|value| interp.charset_canonical_name(value.symbol_str()?))
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
    let charsets = charset_list
        .iter()
        .filter_map(|charset| charset_info(interp, charset))
        .collect();
    Some(IsoAttributes {
        flags,
        initial,
        charset_list,
        safe,
        ascii_compatible: coding_system_is_ascii_compatible(interp, coding),
        default_char: state.default_char,
        charsets,
        roman: interp.charset_canonical_name("latin-jisx0201"),
        jisx0208: interp.charset_canonical_name("japanese-jisx0208"),
        jisx0208_1978: interp.charset_canonical_name("japanese-jisx0208-1978"),
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

    /// Whether CHARSET is CODING_ISO_INVOKED_CHARSET (PLANE).
    fn invoked_charset_is(&self, plane: usize, charset: &str) -> bool {
        let reg = self.invocation[plane];
        reg >= 0 && self.designation[reg as usize].charset() == Some(charset)
    }

    fn designated(&self, reg: usize) -> bool {
        matches!(self.designation[reg], Designation::Charset(_))
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
    Char(u32),
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

    /// cmp_status->length: the charbuf ints of the composition in
    /// progress, its annotation header, characters, [-2 RULE] pairs and
    /// [-1 -1] included.
    fn length(&self) -> usize {
        if self.state == ComposingState::No {
            return 0;
        }
        MAX_ANNOTATION_LENGTH
            + self
                .elements
                .iter()
                .map(|element| match element {
                    CompositionElement::Char(_) => 1,
                    CompositionElement::Rule(_) | CompositionElement::AltEnd => 2,
                })
                .sum::<usize>()
    }
}

/// DECODE_COMPOSITION_RULE's rule for the bytes of one, as
/// produce_composition gives it (COMPOSITION_ENCODE_RULE, the new
/// format's #x100 dropped).
fn decoded_rule(bytes: &[u8]) -> u32 {
    let rule = u32::from(bytes[0]) - 32;
    match bytes.get(1) {
        None => {
            let (mut gref, mut nref) = (rule / 9, rule % 9);
            if gref == 4 {
                gref = 10;
            }
            if nref == 4 {
                nref = 10;
            }
            gref * 12 + nref
        }
        Some(&b) => (rule - 81) * 12 + (u32::from(b) - 32),
    }
}

/// DECODE_COMPOSITION_END of a valid composition: its characters become
/// text and produce_composition's annotation is recorded over them, the
/// components the alternate characters (methods 3 and 4) or the composed
/// characters and rules (method 2); the charbuf ints the composition took
/// that are not characters.
fn end_composition(out: &mut Output, cmp: &mut CompositionStatus) -> usize {
    let length = cmp.length();
    let elements = std::mem::take(&mut cmp.elements);
    let text_start = if cmp.method >= b'3' {
        elements
            .iter()
            .position(|element| matches!(element, CompositionElement::AltEnd))
            .map_or(elements.len(), |index| index + 1)
    } else {
        0
    };
    let components = if cmp.method == b'0' {
        CompositionComponents::Relative
    } else {
        let mut items = Vec::new();
        let mut all_chars = true;
        for element in &elements {
            match element {
                CompositionElement::Char(ch) => items.push(*ch),
                CompositionElement::Rule(bytes) => {
                    all_chars = false;
                    items.push(decoded_rule(bytes));
                }
                CompositionElement::AltEnd => break,
            }
        }
        if all_chars {
            CompositionComponents::Chars(items)
        } else {
            CompositionComponents::Vector(items)
        }
    };
    let start = out.char_count();
    for element in &elements[text_start..] {
        if let CompositionElement::Char(ch) = element {
            out.push_code(*ch);
        }
    }
    out.compositions.push(DecodedComposition {
        seq: out.charsets.len(),
        start,
        end: start + cmp.nchars,
        components,
    });
    cmp.state = ComposingState::No;
    length - cmp.nchars
}

type Output = DecodedText;

/// finish_composition: the composition in progress is invalid, so its
/// start sequence and everything stored for it come back as text; the
/// charbuf ints it took that are not characters.
fn finish_composition(out: &mut Output, cmp: &mut CompositionStatus) -> usize {
    if cmp.state == ComposingState::No {
        return 0;
    }
    let length = cmp.length();
    let before = out.char_count();
    out.push_code(u32::from(ISO_CODE_ESC));
    out.push_code(u32::from(cmp.method));
    for element in std::mem::take(&mut cmp.elements) {
        match element {
            CompositionElement::Char(ch) => out.push_code(ch),
            CompositionElement::Rule(bytes) => {
                for byte in bytes {
                    out.push_byte(byte);
                }
            }
            CompositionElement::AltEnd => {
                out.push_code(u32::from(ISO_CODE_ESC));
                out.push_code(u32::from(b'0'));
            }
        }
    }
    cmp.state = ComposingState::No;
    length - (out.char_count() - before)
}

/// coding->spec.iso_2022 as decode_coding_iso_2022 leaves it for the next
/// block: the designations and invocations, the composition in progress,
/// what is left of a compound-text extended segment, and whether embedded
/// UTF-8 is passing through.
pub(crate) struct Iso2022Carry {
    state: IsoState,
    cmp: CompositionStatus,
    extsegment_len: usize,
    embedded_utf_8: bool,
}

/// coding.c decode_coding_iso_2022, one call over SRC's units from START:
/// the decoder resumes in the state SPEC carries and leaves its state
/// there, the composition in progress included (one the last block has
/// not finished when the call ends is invalid).  A multibyte source's
/// character is invalid_code's, as is any unit a sequence cannot take.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_call<S: Units + ?Sized>(
    interp: &Interpreter,
    src: &S,
    start: usize,
    coding: &str,
    eol_dos: bool,
    last: bool,
    spec: &mut DecoderSpec,
    out: &mut DecodedText,
) -> CallEnd {
    let Some(attrs) = iso_attributes(interp, coding) else {
        return decoders::raw_text_call(src, start, eol_dos, out);
    };
    let flags = attrs.flags;
    let Iso2022Carry {
        mut state,
        mut cmp,
        mut extsegment_len,
        mut embedded_utf_8,
    } = spec
        .iso2022
        .take()
        .map(|carry| *carry)
        .unwrap_or_else(|| Iso2022Carry {
            state: IsoState::new(&attrs),
            cmp: CompositionStatus::new(),
            extsegment_len: 0,
            embedded_utf_8: false,
        });
    let mut charset_id_0 = state.invoked_charset(0);
    let mut charset_id_1 = state.invoked_charset(1);
    let mut runs = CallRuns::new();
    // The charbuf: the characters stored and the annotations' ints.
    let limit = charbuf_size(src) - MAX_ANNOTATION_LENGTH * 3;
    let base = out.char_count();
    let mut annotations = 0;
    if cmp.length() >= limit {
        // A carried composition the charbuf has no room after: GNU's
        // emacs_abort, or a call that carries it again for ever.  It ends
        // as a last block's would.
        annotations += finish_composition(out, &mut cmp);
    }
    let mut byte_after_cr: i32 = -1;
    let use_roman = flags & ISO_FLAG_USE_ROMAN != 0;
    let use_oldjis = flags & ISO_FLAG_USE_OLDJIS != 0;
    let roman = interp.charset_canonical_name("latin-jisx0201");
    let jisx0208_1978 = interp.charset_canonical_name("japanese-jisx0208-1978");
    let jisx0208 = interp.charset_canonical_name("japanese-jisx0208");
    let mut p = start;
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

    let end = 'main: loop {
        src_base = p;
        if out.char_count() - base + annotations + cmp.length() >= limit {
            if byte_after_cr >= 0 {
                src_base -= 1;
            }
            break 'main CallEnd::Full(src_base);
        }
        let mut invalid = false;
        'step: {
            let c1 = if byte_after_cr >= 0 {
                std::mem::replace(&mut byte_after_cr, -1)
            } else {
                one_more!(src, p, break 'main CallEnd::Ended(src_base))
            };
            let Ok(c1) = u8::try_from(c1) else {
                invalid = true;
                break 'step;
            };

            if extsegment_len > 0 {
                out.push_byte(c1);
                extsegment_len -= 1;
                break 'step;
            }

            if embedded_utf_8 {
                if c1 == ISO_CODE_ESC {
                    // `src + 1 >= src_end' counts the source's bytes.
                    if p == src.len() || (p + 1 == src.len() && src.source_len_at(p) == 1) {
                        break 'main CallEnd::Ended(src_base);
                    }
                    out.push_code(u32::from(ISO_CODE_ESC));
                    if src.at(p) == i32::from(b'%') && src.at(p + 1) == i32::from(b'@') {
                        p += 2;
                        out.push_code(u32::from(b'%'));
                        out.push_code(u32::from(b'@'));
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
                    let b = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                    let Ok(b) = u8::try_from(b) else {
                        invalid = true;
                        break 'step;
                    };
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
                            let next = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                            let Ok(next) = u8::try_from(next) else {
                                invalid = true;
                                break 'step;
                            };
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
                            if eol_dos && c1 == b'\r' {
                                byte_after_cr =
                                    one_more!(src, p, break 'main CallEnd::Ended(src_base));
                            }
                            annotations += finish_composition(out, &mut cmp);
                            out.push_byte(c1);
                            break 'step;
                        }
                    };
                    // label_escape_sequence, re-entered after a revision
                    // prefix.
                    loop {
                        match escape {
                            b'&' => {
                                let rev = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                let Ok(rev) = u8::try_from(rev) else {
                                    invalid = true;
                                    break 'step;
                                };
                                if !(b'@'..=b'~').contains(&rev) {
                                    invalid = true;
                                    break 'step;
                                }
                                let esc = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                let Ok(esc) = u8::try_from(esc) else {
                                    invalid = true;
                                    break 'step;
                                };
                                if esc != ISO_CODE_ESC {
                                    invalid = true;
                                    break 'step;
                                }
                                let next = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                let Ok(next) = u8::try_from(next) else {
                                    invalid = true;
                                    break 'step;
                                };
                                escape = next;
                                continue;
                            }
                            b'$' => {
                                if flags & ISO_FLAG_DESIGNATION == 0 {
                                    invalid = true;
                                    break 'step;
                                }
                                let mut_c = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                let Ok(mut_c) = u8::try_from(mut_c) else {
                                    invalid = true;
                                    break 'step;
                                };
                                let (reg, chars96, final_char) = match mut_c {
                                    b'@'..=b'B' => (0usize, false, mut_c),
                                    0x28..=0x2F => {
                                        let final_char =
                                            one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                        // DECODE_DESIGNATION: a character is no final byte.
                                        let final_char = u8::try_from(final_char).unwrap_or(0);
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
                                let next = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                let Ok(next) = u8::try_from(next) else {
                                    invalid = true;
                                    break 'step;
                                };
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
                                // CODING_ANNOTATE_COMPOSITION_MASK, which
                                // setup_coding_system also sets for the
                                // designation flag: CODING_ANNOTATE_CHARSET_MASK
                                // (3) has its bit.
                                if flags & (ISO_FLAG_COMPOSITION | ISO_FLAG_DESIGNATION) == 0 {
                                    invalid = true;
                                    break 'step;
                                }
                                annotations += runs.close(out);
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
                                    annotations += finish_composition(out, &mut cmp);
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
                                    annotations += finish_composition(out, &mut cmp);
                                    invalid = true;
                                    break 'step;
                                }
                                annotations += end_composition(out, &mut cmp);
                                break 'step;
                            }
                            b'[' => {
                                if flags & ISO_FLAG_DIRECTION == 0 {
                                    invalid = true;
                                    break 'step;
                                }
                                let next = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                let Ok(next) = u8::try_from(next) else {
                                    invalid = true;
                                    break 'step;
                                };
                                match next {
                                    b']' => {}
                                    b'0' | b'1' | b'2' => {
                                        let close =
                                            one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                        let Ok(close) = u8::try_from(close) else {
                                            invalid = true;
                                            break 'step;
                                        };
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
                                let next = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                let Ok(next) = u8::try_from(next) else {
                                    invalid = true;
                                    break 'step;
                                };
                                if next == b'/' {
                                    let dim =
                                        one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                    let Ok(dim) = u8::try_from(dim) else {
                                        invalid = true;
                                        break 'step;
                                    };
                                    if !(b'0'..=b'4').contains(&dim) {
                                        invalid = true;
                                        break 'step;
                                    }
                                    let m = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                    let Ok(m) = u8::try_from(m) else {
                                        invalid = true;
                                        break 'step;
                                    };
                                    if m < 128 {
                                        invalid = true;
                                        break 'step;
                                    }
                                    let l = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                    let Ok(l) = u8::try_from(l) else {
                                        invalid = true;
                                        break 'step;
                                    };
                                    if l < 128 {
                                        invalid = true;
                                        break 'step;
                                    }
                                    if out.char_count() - base + annotations + cmp.length() + 6
                                        > limit
                                    {
                                        break 'main CallEnd::Full(src_base);
                                    }
                                    out.push_code(u32::from(ISO_CODE_ESC));
                                    out.push_code(u32::from(b'%'));
                                    out.push_code(u32::from(b'/'));
                                    out.push_code(u32::from(dim));
                                    out.push_byte(m);
                                    out.push_byte(l);
                                    extsegment_len =
                                        usize::from(m - 128) * 128 + usize::from(l - 128);
                                } else if next == b'G' {
                                    if out.char_count() - base + annotations + cmp.length() + 3
                                        > limit
                                    {
                                        break 'main CallEnd::Full(src_base);
                                    }
                                    out.push_code(u32::from(ISO_CODE_ESC));
                                    out.push_code(u32::from(b'%'));
                                    out.push_code(u32::from(b'G'));
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
                                let final_char =
                                    one_more!(src, p, break 'main CallEnd::Ended(src_base));
                                // DECODE_DESIGNATION: a character is no final byte.
                                let final_char = u8::try_from(final_char).unwrap_or(0);
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

            if cmp.state == ComposingState::No && charset != "ascii" && !runs.is(&charset) {
                annotations += runs.open(&charset, out);
            }

            // Now we know CHARSET and the first position code; fetch the
            // second and third codes if the charset needs them.
            let dim = charset_dimension(interp, &charset);
            let mut code = u32::from(first);
            if dim > 1 {
                let c2 = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                let Ok(c2) = u8::try_from(c2) else {
                    invalid = true;
                    break 'step;
                };
                if c2 < 0x20 || (0x80..0xA0).contains(&c2) || (first & 0x80) != (c2 & 0x80) {
                    invalid = true;
                    break 'step;
                }
                if dim == 2 {
                    code = (code << 8) | u32::from(c2);
                } else {
                    let c3 = one_more!(src, p, break 'main CallEnd::Ended(src_base));
                    let Ok(c3) = u8::try_from(c3) else {
                        invalid = true;
                        break 'step;
                    };
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
            let decoded = decode_charset_code(interp, &charset, code);
            match decoded {
                None => {
                    annotations += finish_composition(out, &mut cmp);
                    src.push_source_bytes(src_base, p, out);
                }
                Some(ch) if cmp.state == ComposingState::No => out.push_code(ch),
                Some(ch)
                    if (if cmp.state == ComposingState::Char {
                        cmp.nchars
                    } else {
                        cmp.ncomps
                    }) >= MAX_COMPOSITION_COMPONENTS =>
                {
                    // Too long composition.
                    annotations += finish_composition(out, &mut cmp);
                    out.push_code(ch);
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
            annotations += finish_composition(out, &mut cmp);
            p = src_base;
            let c = one_more!(src, p, break 'main CallEnd::Ended(src_base));
            if c < 0 {
                out.push_code((-c) as u32);
            } else {
                out.push_byte(c as u8);
            }
            state.invocation[0] = 0;
            state.designation[0] = Designation::Charset("ascii".into());
            charset_id_0 = Some("ascii".into());
        }
    };
    // no_more_source (or the charbuf's end): the last block's unfinished
    // composition is invalid, another's waits for the next call; the
    // charset run closes only when no composition is in progress.
    if cmp.state != ComposingState::No {
        if last {
            finish_composition(out, &mut cmp);
        }
    } else {
        runs.finish(out);
    }
    spec.iso2022 = Some(Box::new(Iso2022Carry {
        state,
        cmp,
        extsegment_len,
        embedded_utf_8,
    }));
    end
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
    fn encode_iso_character(&mut self, charset: &str, dimension: u32, code: u32) {
        let attrs = self.attrs;
        let mut charset = charset;
        if dimension == 1 {
            if self.flags() & ISO_FLAG_USE_ROMAN != 0
                && charset == "ascii"
                && let Some(roman) = attrs.roman.as_deref()
            {
                charset = roman;
            }
        } else if self.flags() & ISO_FLAG_USE_OLDJIS != 0
            && Some(charset) == attrs.jisx0208.as_deref()
            && let Some(old) = attrs.jisx0208_1978.as_deref()
        {
            charset = old;
        }
        let pair = [((code >> 8) & 0xFF) as u8, (code & 0xFF) as u8];
        let bytes = if dimension == 1 {
            &pair[1..]
        } else {
            &pair[..]
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
            } else if self.state.invoked_charset_is(0, charset) {
                self.out.extend(bytes.iter().map(|byte| byte & 0x7F));
                return;
            } else if self.state.invoked_charset_is(1, charset) {
                self.out.extend(bytes.iter().map(|byte| byte | 0x80));
                return;
            }
            self.encode_invocation_designation(charset);
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
            let Some((charset, _)) = self.attrs.char_charset(self.interp, c) else {
                continue;
            };
            if let Some(reg) = self.attrs.request(&charset.name)
                && registers[usize::from(reg)].is_none()
            {
                found += 1;
                registers[usize::from(reg)] = Some(charset.name.clone());
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

/// encode_coding_iso_2022 over consume_chars' character codes CHARS,
/// which already carry the eol convention; EOL_TYPE says which (2 = mac,
/// whose CR ends a line).  PREFERRED holds, per code, the `charset' text
/// property GNU would have consumed as an annotation.
/// coding->spec.iso_2022 as encode_coding_iso_2022 leaves it when the
/// block is not the last: the designations, invocations and CODING_ISO_BOL
/// the next block's encoding resumes with.
pub(crate) struct Iso2022EncodeCarry {
    state: IsoState,
}

/// coding.c encode_coding_iso_2022.  CARRY holds the state a stream's
/// last block left (none for a fresh coding struct) and takes this one's
/// unless LAST, whose end resets the planes and registers under
/// ISO_FLAG_RESET_AT_EOL.
pub(crate) fn encode(
    interp: &Interpreter,
    chars: &[u32],
    coding: &str,
    eol_type: Option<i64>,
    preferred: &[Option<String>],
    carry: &mut Option<Box<Iso2022EncodeCarry>>,
    last: bool,
) -> Result<Vec<u8>, LispError> {
    let Some(attrs) = iso_attributes(interp, coding) else {
        return super::encode_coding_raw_text(chars, true);
    };
    let flags = attrs.flags;
    let ascii_compatible =
        attrs.ascii_compatible && flags & (ISO_FLAG_DESIGNATION | ISO_FLAG_LOCKING_SHIFT) == 0;
    let state = carry
        .take()
        .map_or_else(|| IsoState::new(&attrs), |carry| carry.state);
    let mut encoder = Encoder {
        interp,
        state,
        attrs: &attrs,
        out: Vec::with_capacity(chars.len()),
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
                encoder.encode_iso_character("ascii", 1, c);
            }
        } else if RAW_BYTE8_CHARS.contains(&c) {
            encoder.out.push((c - RAW_BYTE8_BASE) as u8);
        } else {
            let preferred_charset = preferred
                .get(index)
                .and_then(Option::as_deref)
                .and_then(|name| charset_info(interp, name))
                .filter(|charset| attrs.charset_list.contains(&charset.name));
            let found = match &preferred_charset {
                Some(charset) => charset.encode(interp, c).map(|code| (&**charset, code)),
                None => None,
            }
            .or_else(|| attrs.char_charset(interp, c));
            match found {
                Some((charset, code)) => {
                    encoder.encode_iso_character(&charset.name, charset.dimension, code);
                }
                // CODING_MODE_SAFE_ENCODING (the `safe' flag) writes the
                // inhibit-substitution marker `?'; otherwise the default
                // character stands in.
                None if flags & ISO_FLAG_SAFE != 0 => {
                    encoder.encode_iso_character("ascii", 1, u32::from(b'?'));
                }
                None => match attrs.char_charset(interp, attrs.default_char) {
                    Some((charset, code)) => {
                        encoder.encode_iso_character(&charset.name, charset.dimension, code);
                    }
                    None => encoder.encode_iso_character("ascii", 1, u32::from(b' ')),
                },
            }
        }
    }
    if last && flags & ISO_FLAG_RESET_AT_EOL != 0 {
        encoder.reset_plane_and_register();
    }
    // CODING_ISO_BOL (coding) = bol_designation.
    encoder.state.bol = bol_designation;
    if !last {
        *carry = Some(Box::new(Iso2022EncodeCarry {
            state: encoder.state,
        }));
    }
    Ok(encoder.out)
}
