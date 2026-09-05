//! coding.c's coding-system detection: the per-category detectors
//! (detect_coding_utf_8, _utf_16, _iso_2022, _charset, _sjis, _big5,
//! _ccl, _emacs_mule) and the two drivers built on them --
//! `detect_coding_system' behind detect-coding-string/region and
//! `detect_coding' behind a decode with an undecided coding system.
//!
//! The source is modelled as coding.c's ONE_MORE_BYTE sees it: a unit is
//! a byte (0..=255) or, when the source is multibyte text, the negated
//! character code of a multibyte character (a raw byte8 character is its
//! own byte, as the macro recovers it from its C0/C1 lead).

use super::iso2022::{IsoAttributes, charset_dimension, iso_attributes};
use super::*;
use crate::lisp::eval::coding::*;

const fn mask(category: usize) -> u32 {
    1 << category
}

const MASK_ISO_7: u32 = mask(CODING_CATEGORY_ISO_7);
const MASK_ISO_7_TIGHT: u32 = mask(CODING_CATEGORY_ISO_7_TIGHT);
const MASK_ISO_8_1: u32 = mask(CODING_CATEGORY_ISO_8_1);
const MASK_ISO_8_2: u32 = mask(CODING_CATEGORY_ISO_8_2);
const MASK_ISO_7_ELSE: u32 = mask(CODING_CATEGORY_ISO_7_ELSE);
const MASK_ISO_8_ELSE: u32 = mask(CODING_CATEGORY_ISO_8_ELSE);
const MASK_UTF_8_AUTO: u32 = mask(CODING_CATEGORY_UTF_8_AUTO);
const MASK_UTF_8_NOSIG: u32 = mask(CODING_CATEGORY_UTF_8_NOSIG);
const MASK_UTF_8_SIG: u32 = mask(CODING_CATEGORY_UTF_8_SIG);
const MASK_UTF_16_AUTO: u32 = mask(CODING_CATEGORY_UTF_16_AUTO);
const MASK_UTF_16_BE: u32 = mask(CODING_CATEGORY_UTF_16_BE);
const MASK_UTF_16_LE: u32 = mask(CODING_CATEGORY_UTF_16_LE);
const MASK_UTF_16_BE_NOSIG: u32 = mask(CODING_CATEGORY_UTF_16_BE_NOSIG);
const MASK_UTF_16_LE_NOSIG: u32 = mask(CODING_CATEGORY_UTF_16_LE_NOSIG);
const MASK_CHARSET: u32 = mask(CODING_CATEGORY_CHARSET);
const MASK_SJIS: u32 = mask(CODING_CATEGORY_SJIS);
const MASK_BIG5: u32 = mask(CODING_CATEGORY_BIG5);
const MASK_CCL: u32 = mask(CODING_CATEGORY_CCL);
const MASK_EMACS_MULE: u32 = mask(CODING_CATEGORY_EMACS_MULE);
const MASK_RAW_TEXT: u32 = mask(CODING_CATEGORY_RAW_TEXT);
const MASK_ANY: u32 = (1 << CODING_CATEGORY_RAW_TEXT) - 1;
const MASK_ISO_7BIT: u32 = MASK_ISO_7 | MASK_ISO_7_TIGHT;
const MASK_ISO_8BIT: u32 = MASK_ISO_8_1 | MASK_ISO_8_2;
const MASK_ISO_ELSE: u32 = MASK_ISO_7_ELSE | MASK_ISO_8_ELSE;
const MASK_ISO_ESCAPE: u32 = MASK_ISO_7 | MASK_ISO_7_TIGHT | MASK_ISO_7_ELSE | MASK_ISO_8_ELSE;
const MASK_ISO: u32 = MASK_ISO_7BIT | MASK_ISO_8BIT | MASK_ISO_ELSE;
const MASK_UTF_16: u32 = MASK_UTF_16_AUTO
    | MASK_UTF_16_BE
    | MASK_UTF_16_LE
    | MASK_UTF_16_BE_NOSIG
    | MASK_UTF_16_LE_NOSIG;
const MASK_UTF_8: u32 = MASK_UTF_8_AUTO | MASK_UTF_8_NOSIG | MASK_UTF_8_SIG;

const ISO_CODE_SO: i32 = 0x0E;
const ISO_CODE_SI: i32 = 0x0F;
const ISO_CODE_ESC: i32 = 0x1B;
const ISO_CODE_SS2: i32 = 0x8E;
const ISO_CODE_SS3: i32 = 0x8F;
const ISO_CODE_CSI: i32 = 0x9B;
const MAX_COMPOSITION_COMPONENTS: i32 = 16;
const MAX_MULTIBYTE_LEADING_CODE: i32 = 0xF8;

pub(crate) const EOL_SEEN_NONE: u8 = 0;
pub(crate) const EOL_SEEN_LF: u8 = 1;
pub(crate) const EOL_SEEN_CR: u8 = 2;
pub(crate) const EOL_SEEN_CRLF: u8 = 4;
const MAX_EOL_CHECK_COUNT: usize = 3;

#[derive(Default, Clone, Copy)]
struct DetectInfo {
    checked: u32,
    found: u32,
    rejected: u32,
}

/// The text under detection, as coding.c's byte reader sees it.
pub(crate) struct DetectSource {
    units: Vec<i32>,
    src_bytes: usize,
    src_chars: usize,
}

impl DetectSource {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        DetectSource {
            units: bytes.iter().map(|byte| i32::from(*byte)).collect(),
            src_bytes: bytes.len(),
            src_chars: bytes.len(),
        }
    }

    /// A Lisp string's own bytes: a unibyte string is its bytes, a
    /// multibyte one contributes its internal spelling.
    pub(crate) fn from_text(text: &str, multibyte: bool) -> Self {
        if !multibyte {
            let bytes = encode_raw_text_bytes(text).unwrap_or_default();
            return Self::from_bytes(&bytes);
        }
        let mut units = Vec::with_capacity(text.len());
        let mut src_bytes = 0;
        let mut src_chars = 0;
        for ch in text.chars() {
            src_chars += 1;
            if let Some(byte) = raw_byte_from_regex_char(ch) {
                units.push(i32::from(byte));
                src_bytes += 2;
            } else if ch.is_ascii() {
                units.push(ch as i32);
                src_bytes += 1;
            } else {
                units.push(-(ch as i32));
                src_bytes += ch.len_utf8();
            }
        }
        DetectSource {
            units,
            src_bytes,
            src_chars,
        }
    }

    /// The internal bytes, for the eol scans that read raw octets.
    fn internal_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.src_bytes);
        for &unit in &self.units {
            if unit >= 0 {
                bytes.push(unit as u8);
            } else if let Some(ch) = char::from_u32((-unit) as u32) {
                let mut buffer = [0u8; 4];
                bytes.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
            }
        }
        bytes
    }
}

/// The per-category state coding.c keeps in `coding_categories': each
/// category's representative coding system.
struct Categories {
    representatives: Vec<Option<String>>,
    priorities: Vec<usize>,
}

impl Categories {
    fn new(interp: &Interpreter) -> Self {
        Categories {
            representatives: interp.coding_category_representatives.clone(),
            priorities: interp.coding_category_priorities.clone(),
        }
    }

    fn representative(&self, category: usize) -> Option<&str> {
        self.representatives[category].as_deref()
    }
}

fn latin_extra_code_table(interp: &Interpreter, env: &Env) -> Option<Vec<Value>> {
    interp
        .lookup_var("latin-extra-code-table", env)
        .and_then(|table| vector_items(&table).ok())
}

fn latin_extra_code(table: &Option<Vec<Value>>, c: i32) -> bool {
    table
        .as_ref()
        .and_then(|table| usize::try_from(c).ok().and_then(|index| table.get(index)))
        .is_some_and(Value::is_truthy)
}

fn truthy_var(interp: &Interpreter, name: &str, env: &Env) -> bool {
    interp
        .lookup_var(name, env)
        .is_some_and(|value| value.is_truthy())
}

/// coding.c encode_inhibit_flag / inhibit_flag: nil never inhibits, t
/// always does, anything else (the C-defined 0) asks the variable.
fn inhibit_flag(attribute: Option<&Value>, variable: bool) -> bool {
    let encoded = match attribute {
        None | Some(Value::Nil) => -1,
        Some(Value::T) => 1,
        Some(_) => 0,
    };
    encoded + i32::from(variable) > 0
}

// --- The detectors -------------------------------------------------------

fn detect_utf_8(src: &DetectSource, head_ascii: usize, info: &mut DetectInfo) -> bool {
    let units = &src.units;
    let end = units.len();
    let mut p = head_ascii;
    let mut nchars = head_ascii;
    let mut bom_found = false;
    info.checked |= MASK_UTF_8;
    if p == 0 && p + 3 < end && units[0] == 0xEF && units[1] == 0xBB && units[2] == 0xBF {
        bom_found = true;
        p += 3;
        nchars += 1;
    }
    let src_base;
    loop {
        let base = p;
        let Some(&c) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        if c < 0 || c < 0x80 {
            nchars += 1;
            if c == i32::from(b'\r') && p < end && units[p] == i32::from(b'\n') {
                p += 1;
                nchars += 1;
            }
            continue;
        }
        let extra = |c: i32| c >= 0 && (c & 0xC0) == 0x80;
        let Some(&c1) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        if !extra(c1) {
            info.rejected |= MASK_UTF_8;
            return false;
        }
        if (c & 0xE0) == 0xC0 {
            nchars += 1;
            continue;
        }
        let Some(&c2) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        if !extra(c2) {
            info.rejected |= MASK_UTF_8;
            return false;
        }
        if (c & 0xF0) == 0xE0 {
            nchars += 1;
            continue;
        }
        let Some(&c3) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        if !extra(c3) {
            info.rejected |= MASK_UTF_8;
            return false;
        }
        if (c & 0xF8) == 0xF0 {
            nchars += 1;
            continue;
        }
        let Some(&c4) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        if !extra(c4) {
            info.rejected |= MASK_UTF_8;
            return false;
        }
        if (c & 0xFC) == 0xF8 && c < MAX_MULTIBYTE_LEADING_CODE {
            nchars += 1;
            continue;
        }
        info.rejected |= MASK_UTF_8;
        return false;
    }
    // no_more_source (always the last block).
    if src_base < p {
        info.rejected |= MASK_UTF_8;
        return false;
    }
    if bom_found {
        info.found |= MASK_UTF_8_AUTO | MASK_UTF_8_SIG | MASK_UTF_8_NOSIG;
    } else {
        info.rejected |= MASK_UTF_8_SIG;
        if nchars < src.src_bytes {
            info.found |= MASK_UTF_8_AUTO | MASK_UTF_8_NOSIG;
        }
    }
    true
}

/// TWO_MORE_BYTES: skip multibyte characters for the first byte, and
/// report a multibyte character in the second as -1.
fn two_more_bytes(units: &[i32], p: &mut usize) -> Option<(i32, i32)> {
    let c1 = loop {
        let &c = units.get(*p)?;
        *p += 1;
        if c >= 0 {
            break c;
        }
    };
    let &c2 = units.get(*p)?;
    *p += 1;
    Some((c1, if c2 < 0 { -1 } else { c2 }))
}

fn detect_utf_16(src: &DetectSource, info: &mut DetectInfo) -> bool {
    let units = &src.units;
    let mut p = 0usize;
    info.checked |= MASK_UTF_16;
    if src.src_chars & 1 == 1 {
        info.rejected |= MASK_UTF_16;
        return false;
    }
    let Some((c1, c2)) = two_more_bytes(units, &mut p) else {
        return true;
    };
    if c1 == 0xFF && c2 == 0xFE {
        info.found |= MASK_UTF_16_LE | MASK_UTF_16_AUTO;
        info.rejected |= MASK_UTF_16_BE | MASK_UTF_16_BE_NOSIG | MASK_UTF_16_LE_NOSIG;
        true
    } else if c1 == 0xFE && c2 == 0xFF {
        info.found |= MASK_UTF_16_BE | MASK_UTF_16_AUTO;
        info.rejected |= MASK_UTF_16_LE | MASK_UTF_16_BE_NOSIG | MASK_UTF_16_LE_NOSIG;
        true
    } else if c2 < 0 {
        info.rejected |= MASK_UTF_16;
        false
    } else {
        // The dispersion of the even and odd bytes: both high means
        // binary data.
        let mut e = [false; 256];
        let mut o = [false; 256];
        let mut e_num = 1;
        let mut o_num = 1;
        e[c1 as usize] = true;
        o[c2 as usize] = true;
        info.rejected |= MASK_UTF_16_AUTO | MASK_UTF_16_BE | MASK_UTF_16_LE;
        while info.rejected & MASK_UTF_16 != MASK_UTF_16 {
            let Some((c1, c2)) = two_more_bytes(units, &mut p) else {
                return true;
            };
            if c2 < 0 {
                break;
            }
            if !e[c1 as usize] {
                e[c1 as usize] = true;
                e_num += 1;
                if e_num >= 128 {
                    info.rejected |= MASK_UTF_16_BE_NOSIG;
                }
            }
            if !o[c2 as usize] {
                o[c2 as usize] = true;
                o_num += 1;
                if o_num >= 128 {
                    info.rejected |= MASK_UTF_16_LE_NOSIG;
                }
            }
        }
        false
    }
}

fn detect_emacs_mule(
    interp: &Interpreter,
    src: &DetectSource,
    head_ascii: usize,
    info: &mut DetectInfo,
) -> bool {
    let layout = emacs_mule_layout(interp);
    let units = &src.units;
    let mut p = head_ascii;
    let mut found = 0;
    info.checked |= MASK_EMACS_MULE;
    let src_base;
    loop {
        let base = p;
        let Some(&mut_c) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        let mut c = mut_c;
        if c < 0 {
            continue;
        }
        if c == 0x80 {
            // Perhaps the start of a composite character: only check that
            // it spans more than four bytes.
            loop {
                let src_start = p;
                loop {
                    let Some(&next) = units.get(p) else {
                        // ONE_MORE_BYTE inside the composition scan.
                        info.rejected |= MASK_EMACS_MULE;
                        return false;
                    };
                    p += 1;
                    c = next;
                    if c < 0xA0 {
                        break;
                    }
                }
                if p - src_start <= 4 {
                    info.rejected |= MASK_EMACS_MULE;
                    return false;
                }
                found = MASK_EMACS_MULE;
                if c != 0x80 {
                    break;
                }
            }
        }
        if c < 0x80 {
            if c < 0x20 && (c == ISO_CODE_ESC || c == ISO_CODE_SI || c == ISO_CODE_SO) {
                info.rejected |= MASK_EMACS_MULE;
                return false;
            }
        } else {
            let mut more_bytes = layout.lengths[c as usize].saturating_sub(1);
            while more_bytes > 0 {
                let Some(&next) = units.get(p) else {
                    info.rejected |= MASK_EMACS_MULE;
                    return false;
                };
                p += 1;
                if next < 0xA0 {
                    p -= 1;
                    break;
                }
                more_bytes -= 1;
            }
            if more_bytes != 0 {
                info.rejected |= MASK_EMACS_MULE;
                return false;
            }
            found = MASK_EMACS_MULE;
        }
    }
    if src_base < p {
        info.rejected |= MASK_EMACS_MULE;
        return false;
    }
    info.found |= found;
    true
}

fn detect_sjis(
    src: &DetectSource,
    head_ascii: usize,
    max_first_byte_of_2_byte_code: i32,
    info: &mut DetectInfo,
) -> bool {
    let units = &src.units;
    let mut p = head_ascii;
    let mut found = 0;
    info.checked |= MASK_SJIS;
    let src_base;
    loop {
        let base = p;
        let Some(&c) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        if c < 0x80 {
            continue;
        }
        if (0x81..=0x9F).contains(&c) || (0xE0..=max_first_byte_of_2_byte_code).contains(&c) {
            let Some(&c2) = units.get(p) else {
                src_base = base;
                break;
            };
            p += 1;
            if c2 < 0x40 || c2 == 0x7F || c2 > 0xFC {
                info.rejected |= MASK_SJIS;
                return false;
            }
            found = MASK_SJIS;
        } else if (0xA0..0xE0).contains(&c) {
            found = MASK_SJIS;
        } else {
            info.rejected |= MASK_SJIS;
            return false;
        }
    }
    if src_base < p {
        info.rejected |= MASK_SJIS;
        return false;
    }
    info.found |= found;
    true
}

fn detect_big5(src: &DetectSource, head_ascii: usize, info: &mut DetectInfo) -> bool {
    let units = &src.units;
    let mut p = head_ascii;
    let mut found = 0;
    info.checked |= MASK_BIG5;
    let src_base;
    loop {
        let base = p;
        let Some(&c) = units.get(p) else {
            src_base = base;
            break;
        };
        p += 1;
        if c < 0x80 {
            continue;
        }
        if c >= 0xA1 {
            let Some(&c2) = units.get(p) else {
                src_base = base;
                break;
            };
            p += 1;
            if c2 < 0x40 || (0x7F..=0xA0).contains(&c2) {
                // coding.c returns without rejecting here.
                return false;
            }
            found = MASK_BIG5;
        } else {
            info.rejected |= MASK_BIG5;
            return false;
        }
    }
    if src_base < p {
        info.rejected |= MASK_BIG5;
        return false;
    }
    info.found |= found;
    true
}

/// The `:valids' table of the ccl category's representative: 0 for an
/// invalid byte, 1 for a valid one.
fn ccl_valids(interp: &Interpreter, coding: &str) -> Option<[u8; 256]> {
    let state = interp.coding_system(coding)?;
    let ranges = state.type_args.get(2)?.to_vec().ok()?;
    let mut valids = [0u8; 256];
    for range in ranges {
        let (from, to) = match &range {
            Value::Integer(byte) => (*byte, *byte),
            other => {
                let (from, to) = other.cons_values()?;
                (from.as_integer().ok()?, to.as_integer().ok()?)
            }
        };
        for byte in from.max(0)..=to.min(255) {
            valids[byte as usize] = 1;
        }
    }
    Some(valids)
}

fn detect_ccl(
    interp: &Interpreter,
    src: &DetectSource,
    head_ascii: usize,
    representative: &str,
    info: &mut DetectInfo,
) -> bool {
    info.checked |= MASK_CCL;
    let Some(valids) = ccl_valids(interp, representative) else {
        info.rejected |= MASK_CCL;
        return false;
    };
    let units = &src.units;
    let mut p = if coding_system_is_ascii_compatible(interp, representative) {
        head_ascii
    } else {
        0
    };
    let mut found = 0;
    while let Some(&c) = units.get(p) {
        p += 1;
        if c < 0 || valids[c as usize] == 0 {
            info.rejected |= MASK_CCL;
            return false;
        }
        if valids[c as usize] > 1 {
            found = MASK_CCL;
        }
    }
    info.found |= found;
    true
}

/// Fdefine_coding_system_internal's `charset_valids' vector for a
/// charset-type coding: per first byte, the charsets it may start,
/// smaller dimensions first.
fn charset_valids(interp: &Interpreter, coding: &str) -> Vec<Option<Vec<String>>> {
    let mut valids: Vec<Option<Vec<String>>> = vec![None; 256];
    for charset in coding_system_charset_names(interp, coding) {
        let Some(canonical) = interp.charset_canonical_name(&charset) else {
            continue;
        };
        let dim = charset_dimension(interp, &canonical) as usize;
        let Some(bounds) = charset_code_space(interp, &canonical) else {
            continue;
        };
        let Some(&(min, max)) = bounds.get(dim - 1) else {
            continue;
        };
        for byte in min..=max.min(255) {
            let entry = valids[byte as usize].get_or_insert_with(Vec::new);
            let position = entry
                .iter()
                .position(|existing| charset_dimension(interp, existing) as usize > dim)
                .unwrap_or(entry.len());
            entry.insert(position, canonical.clone());
        }
    }
    valids
}

fn detect_charset(
    interp: &Interpreter,
    src: &DetectSource,
    head_ascii: usize,
    representative: &str,
    latin_extra: &Option<Vec<Value>>,
    info: &mut DetectInfo,
) -> bool {
    info.checked |= MASK_CHARSET;
    let valids = charset_valids(interp, representative);
    let check_latin_extra =
        representative.starts_with("iso-8859-") || representative.starts_with("iso-latin-");
    let units = &src.units;
    let end = units.len();
    let mut p = if coding_system_is_ascii_compatible(interp, representative) {
        head_ascii
    } else {
        0
    };
    let mut found = 0;
    while let Some(&c) = units.get(p) {
        p += 1;
        if c < 0 {
            continue;
        }
        let Some(charsets) = &valids[c as usize] else {
            info.rejected |= MASK_CHARSET;
            return false;
        };
        if c >= 0x80 {
            if c < 0xA0 && check_latin_extra && !latin_extra_code(latin_extra, c) {
                info.rejected |= MASK_CHARSET;
                return false;
            }
            found = MASK_CHARSET;
        }
        // The trailing bytes must fit one of the charsets' code spaces.
        let mut idx = 1usize;
        let mut matched = false;
        for charset in charsets {
            let dim = charset_dimension(interp, charset) as usize;
            let bounds = charset_code_space(interp, charset).unwrap_or_default();
            while idx < dim {
                if p == end {
                    // too_short
                    info.rejected |= MASK_CHARSET;
                    return false;
                }
                let c = units[p];
                p += 1;
                let (min, max) = bounds.get(dim - 1 - idx).copied().unwrap_or((0, 0));
                if c < min as i32 || c > max as i32 {
                    break;
                }
                idx += 1;
            }
            if idx == dim {
                matched = true;
                break;
            }
        }
        if !matched {
            info.rejected |= MASK_CHARSET;
            return false;
        }
    }
    info.found |= found;
    true
}

/// The ISO-2022 categories' representatives, as detect_coding_iso_2022
/// consults them.
struct IsoCategories {
    attrs: [Option<IsoAttributes>; 6],
}

impl IsoCategories {
    fn new(interp: &Interpreter, categories: &Categories) -> Self {
        IsoCategories {
            attrs: std::array::from_fn(|category| {
                categories
                    .representative(category)
                    .and_then(|coding| iso_attributes(interp, coding))
            }),
        }
    }

    fn safe(&self, category: usize, charset: &str) -> bool {
        self.attrs[category]
            .as_ref()
            .is_some_and(|attrs| attrs.safe.contains_key(charset))
    }

    fn flags(&self, category: usize) -> u32 {
        self.attrs[category].as_ref().map_or(0, |attrs| attrs.flags)
    }
}

fn detect_iso_2022(
    interp: &Interpreter,
    src: &DetectSource,
    head_ascii: usize,
    iso: &IsoCategories,
    inhibit_iso_escape_detection: bool,
    latin_extra: &Option<Vec<Value>>,
    info: &mut DetectInfo,
) -> bool {
    let units = &src.units;
    let end = units.len();
    let mut p = head_ascii;
    let mut single_shifting = false;
    let mut rejected: u32 = 0;
    let mut found: u32 = 0;
    let mut composition_count: i32 = -1;
    info.checked |= MASK_ISO;

    let designation_found = |rejected: &mut u32, found: &mut u32, charset: &str| {
        *rejected |= MASK_ISO_8BIT;
        for (category, bit) in [
            (CODING_CATEGORY_ISO_7, MASK_ISO_7),
            (CODING_CATEGORY_ISO_7_TIGHT, MASK_ISO_7_TIGHT),
            (CODING_CATEGORY_ISO_7_ELSE, MASK_ISO_7_ELSE),
            (CODING_CATEGORY_ISO_8_ELSE, MASK_ISO_8_ELSE),
        ] {
            if iso.safe(category, charset) {
                *found |= bit;
            } else {
                *rejected |= bit;
            }
        }
    };

    macro_rules! one_more_byte {
        () => {{
            let Some(&c) = units.get(p) else {
                info.rejected |= rejected;
                info.found |= found & !rejected;
                return true;
            };
            p += 1;
            c
        }};
    }

    while rejected != MASK_ISO {
        let c = one_more_byte!();
        match c {
            ISO_CODE_ESC => {
                if inhibit_iso_escape_detection {
                    continue;
                }
                single_shifting = false;
                let c = one_more_byte!();
                if c == i32::from(b'N') || c == i32::from(b'O') {
                    single_shifting = true;
                    rejected |= MASK_ISO_7BIT | MASK_ISO_8BIT;
                } else if c == i32::from(b'1') {
                    if !(0..=MAX_COMPOSITION_COMPONENTS).contains(&composition_count) {
                        continue;
                    }
                    composition_count = -1;
                    found |= MASK_ISO;
                } else if (i32::from(b'0')..=i32::from(b'4')).contains(&c) {
                    composition_count = 0;
                } else {
                    let charset = if (i32::from(b'(')..=i32::from(b'/')).contains(&c) {
                        let c1 = one_more_byte!();
                        if c1 < i32::from(b' ') || c1 >= 0x80 {
                            if c1 >= 0x80 {
                                rejected |= MASK_ISO_7BIT | MASK_ISO_7_ELSE;
                            }
                            continue;
                        }
                        match interp.iso_charset_for(1, c >= i32::from(b','), c1 as u8) {
                            Some(charset) => charset,
                            None => continue,
                        }
                    } else if c == i32::from(b'$') {
                        let c = one_more_byte!();
                        if (i32::from(b'@')..=i32::from(b'B')).contains(&c) {
                            match interp.iso_charset_for(2, false, c as u8) {
                                Some(charset) => charset,
                                None => continue,
                            }
                        } else if (i32::from(b'(')..=i32::from(b'/')).contains(&c) {
                            let c1 = one_more_byte!();
                            if c1 < i32::from(b' ') || c1 >= 0x80 {
                                if c1 >= 0x80 {
                                    rejected |= MASK_ISO_7BIT | MASK_ISO_7_ELSE;
                                }
                                continue;
                            }
                            match interp.iso_charset_for(2, c >= i32::from(b','), c1 as u8) {
                                Some(charset) => charset,
                                None => continue,
                            }
                        } else {
                            if c >= 0x80 {
                                rejected |= MASK_ISO_7BIT | MASK_ISO_7_ELSE;
                            }
                            continue;
                        }
                    } else {
                        if c >= 0x80 {
                            rejected |= MASK_ISO_7BIT | MASK_ISO_7_ELSE;
                        }
                        continue;
                    };
                    designation_found(&mut rejected, &mut found, &charset);
                }
            }
            ISO_CODE_SO | ISO_CODE_SI => {
                if inhibit_iso_escape_detection {
                    continue;
                }
                single_shifting = false;
                rejected |= MASK_ISO_7BIT | MASK_ISO_8BIT;
            }
            ISO_CODE_CSI => {
                single_shifting = false;
                rejected |= MASK_ISO_7BIT | MASK_ISO_7_ELSE;
                found |= MASK_ISO_8_ELSE;
                // check_extra_latin
                if !latin_extra_code(latin_extra, c) {
                    rejected = MASK_ISO;
                    continue;
                }
                if iso.flags(CODING_CATEGORY_ISO_8_1) & ISO_FLAG_LATIN_EXTRA != 0 {
                    found |= MASK_ISO_8_1;
                } else {
                    rejected |= MASK_ISO_8_1;
                }
                rejected |= MASK_ISO_8_2;
            }
            ISO_CODE_SS2 | ISO_CODE_SS3 => {
                if inhibit_iso_escape_detection {
                    continue;
                }
                single_shifting = false;
                rejected |= MASK_ISO_7BIT | MASK_ISO_7_ELSE;
                if iso.flags(CODING_CATEGORY_ISO_8_1) & ISO_FLAG_SINGLE_SHIFT != 0 {
                    found |= MASK_ISO_8_1;
                    single_shifting = true;
                }
                if iso.flags(CODING_CATEGORY_ISO_8_2) & ISO_FLAG_SINGLE_SHIFT != 0 {
                    found |= MASK_ISO_8_2;
                    single_shifting = true;
                }
                if single_shifting {
                    continue;
                }
                // check_extra_latin
                if !latin_extra_code(latin_extra, c) {
                    rejected = MASK_ISO;
                    continue;
                }
                if iso.flags(CODING_CATEGORY_ISO_8_1) & ISO_FLAG_LATIN_EXTRA != 0 {
                    found |= MASK_ISO_8_1;
                } else {
                    rejected |= MASK_ISO_8_1;
                }
                rejected |= MASK_ISO_8_2;
            }
            _ => {
                if c < 0 {
                    continue;
                }
                if c < 0x80 {
                    if composition_count >= 0 {
                        composition_count += 1;
                    }
                    single_shifting = false;
                    continue;
                }
                rejected |= MASK_ISO_7BIT | MASK_ISO_7_ELSE;
                if c >= 0xA0 {
                    found |= MASK_ISO_8_1;
                    // The length of the run of 0xA0..0xFF codes: even
                    // runs also fit a two-byte 8-bit charset.
                    if !single_shifting && rejected & MASK_ISO_8_2 == 0 {
                        let mut len = 1;
                        while p < end {
                            let base = p;
                            let c = one_more_byte!();
                            if c < 0xA0 {
                                p = base;
                                break;
                            }
                            len += 1;
                        }
                        if len & 1 == 1 && p < end {
                            rejected |= MASK_ISO_8_2;
                            if composition_count >= 0 {
                                composition_count += len;
                            }
                        } else {
                            found |= MASK_ISO_8_2;
                            if composition_count >= 0 {
                                composition_count += len / 2;
                            }
                        }
                    }
                    continue;
                }
                // check_extra_latin
                if !latin_extra_code(latin_extra, c) {
                    rejected = MASK_ISO;
                    continue;
                }
                if iso.flags(CODING_CATEGORY_ISO_8_1) & ISO_FLAG_LATIN_EXTRA != 0 {
                    found |= MASK_ISO_8_1;
                } else {
                    rejected |= MASK_ISO_8_1;
                }
                rejected |= MASK_ISO_8_2;
            }
        }
    }
    info.rejected |= MASK_ISO;
    false
}

// --- The drivers ---------------------------------------------------------

/// The category a coding system's attributes carry, and its own
/// undecided-type detection controls.
struct UndecidedSpec {
    inhibit_nbd: bool,
    inhibit_ied: bool,
    prefer_utf_8: bool,
}

fn undecided_spec(interp: &Interpreter, coding: &str, env: &Env) -> UndecidedSpec {
    let args = interp
        .coding_system(coding)
        .map(|state| state.type_args)
        .unwrap_or_default();
    UndecidedSpec {
        inhibit_nbd: inhibit_flag(
            args.first(),
            truthy_var(interp, "inhibit-null-byte-detection", env),
        ),
        inhibit_ied: inhibit_flag(
            args.get(1),
            truthy_var(interp, "inhibit-iso-escape-detection", env),
        ),
        prefer_utf_8: args.get(2).is_some_and(Value::is_truthy),
    }
}

fn sjis_max_first_byte(interp: &Interpreter, coding: &str) -> i32 {
    let charsets = interp
        .coding_system(coding)
        .and_then(|state| state.charset_list.to_vec().ok())
        .map_or(0, |list| list.len());
    if charsets <= 3 { 0xEF } else { 0xFC }
}

struct Detector<'a> {
    interp: &'a Interpreter,
    categories: Categories,
    iso: IsoCategories,
    latin_extra: Option<Vec<Value>>,
    inhibit_iso_escape_detection: bool,
}

impl<'a> Detector<'a> {
    fn new(interp: &'a Interpreter, env: &'a Env) -> Self {
        let categories = Categories::new(interp);
        let iso = IsoCategories::new(interp, &categories);
        Detector {
            interp,
            categories,
            iso,
            latin_extra: latin_extra_code_table(interp, env),
            inhibit_iso_escape_detection: truthy_var(interp, "inhibit-iso-escape-detection", env),
        }
    }

    /// `(*(this->detector)) (coding, &detect_info)' for CATEGORY, whose
    /// representative is known to exist.  SJIS_CODING is the coding whose
    /// charset list bounds the two-byte lead range.
    fn run_detector(
        &self,
        category: usize,
        src: &DetectSource,
        head_ascii: usize,
        sjis_coding: &str,
        info: &mut DetectInfo,
    ) -> bool {
        let representative = self.categories.representative(category).unwrap_or("");
        match category {
            CODING_CATEGORY_ISO_7
            | CODING_CATEGORY_ISO_7_TIGHT
            | CODING_CATEGORY_ISO_8_1
            | CODING_CATEGORY_ISO_8_2
            | CODING_CATEGORY_ISO_7_ELSE
            | CODING_CATEGORY_ISO_8_ELSE => detect_iso_2022(
                self.interp,
                src,
                head_ascii,
                &self.iso,
                self.inhibit_iso_escape_detection,
                &self.latin_extra,
                info,
            ),
            CODING_CATEGORY_UTF_8_AUTO
            | CODING_CATEGORY_UTF_8_NOSIG
            | CODING_CATEGORY_UTF_8_SIG => detect_utf_8(src, head_ascii, info),
            CODING_CATEGORY_UTF_16_AUTO
            | CODING_CATEGORY_UTF_16_BE
            | CODING_CATEGORY_UTF_16_LE
            | CODING_CATEGORY_UTF_16_BE_NOSIG
            | CODING_CATEGORY_UTF_16_LE_NOSIG => detect_utf_16(src, info),
            CODING_CATEGORY_CHARSET => detect_charset(
                self.interp,
                src,
                head_ascii,
                representative,
                &self.latin_extra,
                info,
            ),
            CODING_CATEGORY_SJIS => detect_sjis(
                src,
                head_ascii,
                sjis_max_first_byte(self.interp, sjis_coding),
                info,
            ),
            CODING_CATEGORY_BIG5 => detect_big5(src, head_ascii, info),
            CODING_CATEGORY_CCL => detect_ccl(self.interp, src, head_ascii, representative, info),
            CODING_CATEGORY_EMACS_MULE => detect_emacs_mule(self.interp, src, head_ascii, info),
            _ => false,
        }
    }

    /// The head scan shared by both drivers: skip ASCII, note null and
    /// 8-bit bytes, and try ISO-2022 at the first ESC/SO/SI.  Returns the
    /// head_ascii count, the null/eight-bit flags and the eol bits seen.
    fn head_scan(
        &self,
        src: &DetectSource,
        spec: &UndecidedSpec,
        info: &mut DetectInfo,
        track_eol: bool,
    ) -> (usize, bool, bool, u8) {
        let units = &src.units;
        let mut head_ascii = 0usize;
        let mut null_byte_found = false;
        let mut eight_bit_found = false;
        let mut eol_seen = EOL_SEEN_NONE;
        let mut i = 0usize;
        while i < units.len() {
            let c = units[i];
            if c < 0 || c & 0x80 != 0 {
                eight_bit_found = true;
                if null_byte_found {
                    break;
                }
            } else if c < 0x20 {
                if (c == ISO_CODE_ESC || c == ISO_CODE_SI || c == ISO_CODE_SO)
                    && !spec.inhibit_ied
                    && info.checked == 0
                {
                    if detect_iso_2022(
                        self.interp,
                        src,
                        head_ascii,
                        &self.iso,
                        self.inhibit_iso_escape_detection,
                        &self.latin_extra,
                        info,
                    ) {
                        // The whole data was scanned.
                        if info.rejected & MASK_ISO_7_ELSE == 0 {
                            head_ascii = src.src_bytes;
                        }
                        info.rejected |= !MASK_ISO_ESCAPE;
                        break;
                    }
                } else if c == 0 && !spec.inhibit_nbd {
                    null_byte_found = true;
                    if eight_bit_found {
                        break;
                    }
                } else if track_eol {
                    if c == i32::from(b'\r') {
                        if i + 1 < units.len() && units[i + 1] == i32::from(b'\n') {
                            eol_seen |= EOL_SEEN_CRLF;
                            i += 1;
                            if !eight_bit_found {
                                head_ascii += 1;
                            }
                        } else {
                            eol_seen |= EOL_SEEN_CR;
                        }
                    } else if c == i32::from(b'\n') {
                        eol_seen |= EOL_SEEN_LF;
                    }
                }
                if !eight_bit_found {
                    head_ascii += 1;
                }
            } else if !eight_bit_found {
                head_ascii += 1;
            }
            i += 1;
        }
        (head_ascii, null_byte_found, eight_bit_found, eol_seen)
    }
}

/// coding.c detect_eol for the non-UTF-16 categories.
pub(crate) fn detect_eol(bytes: &[u8]) -> u8 {
    let mut eol_seen = EOL_SEEN_NONE;
    let mut total = 0;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        i += 1;
        if c == b'\n' || c == b'\r' {
            let this_eol = if c == b'\n' {
                EOL_SEEN_LF
            } else if i >= bytes.len() || bytes[i] != b'\n' {
                EOL_SEEN_CR
            } else {
                i += 1;
                EOL_SEEN_CRLF
            };
            if eol_seen == EOL_SEEN_NONE {
                eol_seen = this_eol;
            } else if eol_seen != this_eol {
                // Allow for stray ^M characters in DOS EOL files.
                if (eol_seen == EOL_SEEN_CR && this_eol == EOL_SEEN_CRLF)
                    || (eol_seen == EOL_SEEN_CRLF && this_eol == EOL_SEEN_CR)
                {
                    eol_seen = EOL_SEEN_CRLF;
                } else {
                    eol_seen = EOL_SEEN_LF;
                    break;
                }
            }
            total += 1;
            if total == MAX_EOL_CHECK_COUNT {
                break;
            }
        }
    }
    eol_seen
}

/// detect_eol over UTF-16 code units (MSB is the even or odd byte).
fn detect_eol_utf_16(bytes: &[u8], little_endian: bool) -> u8 {
    let (msb, lsb) = if little_endian { (1, 0) } else { (0, 1) };
    let mut eol_seen = EOL_SEEN_NONE;
    let mut total = 0;
    let mut i = 0;
    while i + 1 < bytes.len() {
        let c = bytes[i + lsb];
        if bytes[i + msb] == 0 && (c == b'\n' || c == b'\r') {
            let this_eol = if c == b'\n' {
                EOL_SEEN_LF
            } else if i + 3 >= bytes.len() || bytes[i + msb + 2] != 0 || bytes[i + lsb + 2] != b'\n'
            {
                EOL_SEEN_CR
            } else {
                i += 2;
                EOL_SEEN_CRLF
            };
            if eol_seen == EOL_SEEN_NONE {
                eol_seen = this_eol;
            } else if eol_seen != this_eol {
                if (eol_seen == EOL_SEEN_CR && this_eol == EOL_SEEN_CRLF)
                    || (eol_seen == EOL_SEEN_CRLF && this_eol == EOL_SEEN_CR)
                {
                    eol_seen = EOL_SEEN_CRLF;
                } else {
                    eol_seen = EOL_SEEN_LF;
                    break;
                }
            }
            total += 1;
            if total == MAX_EOL_CHECK_COUNT {
                break;
            }
        }
        i += 2;
    }
    eol_seen
}

/// The eol type index (0 unix, 1 dos, 2 mac) adjust_coding_eol_type
/// picks for an EOL_SEEN mask, LF first.
pub(crate) fn eol_type_for_seen(eol_seen: u8) -> Option<i64> {
    if eol_seen & EOL_SEEN_LF != 0 {
        Some(0)
    } else if eol_seen & EOL_SEEN_CRLF != 0 {
        Some(1)
    } else if eol_seen & EOL_SEEN_CR != 0 {
        Some(2)
    } else {
        None
    }
}

/// The name a coding system takes once an eol convention is decided:
/// the subsidiary of an undecided-eol base, or itself.
fn name_with_eol(interp: &Interpreter, coding: &str, this_eol: u8) -> String {
    if interp.coding_system_eol_type_value(coding).is_some() {
        return coding.to_string();
    }
    let base = interp
        .coding_system_base_name(coding)
        .unwrap_or_else(|| coding.to_string());
    match eol_type_for_seen(this_eol) {
        Some(eol) => coding_variant_name(interp, &base, Some(eol)),
        None => coding.to_string(),
    }
}

fn utf_16_category(category: usize) -> Option<bool> {
    match category {
        CODING_CATEGORY_UTF_16_BE | CODING_CATEGORY_UTF_16_BE_NOSIG => Some(false),
        CODING_CATEGORY_UTF_16_LE | CODING_CATEGORY_UTF_16_LE_NOSIG => Some(true),
        _ => None,
    }
}

/// coding.c detect_coding_system: the coding systems that may have
/// encoded SRC, in priority order (or just the best one), each named
/// with the eol convention detected for it.
pub(crate) fn detect_coding_system(
    interp: &Interpreter,
    src: &DetectSource,
    highest: bool,
    coding_system: Option<&str>,
    env: &Env,
) -> Vec<String> {
    let requested = coding_system.unwrap_or("undecided");
    let canonical = interp
        .coding_system_canonical_name(requested)
        .unwrap_or_else(|| requested.to_string());
    let state = interp.coding_system(&canonical);
    let base_category = state
        .as_ref()
        .map_or(CODING_CATEGORY_UNDECIDED, |state| state.category);
    let requested_eol = interp.coding_system_eol_type_value(&canonical);
    let detector = Detector::new(interp, env);
    let categories = &detector.categories;
    let mut info = DetectInfo::default();
    let mut null_byte_found = false;
    // The candidates as (category, coding name) pairs.
    let mut val: Vec<(usize, String)> = Vec::new();

    if base_category == CODING_CATEGORY_UNDECIDED {
        let spec = undecided_spec(interp, &canonical, env);
        let (head_ascii, null, eight_bit_found, _) =
            detector.head_scan(src, &spec, &mut info, false);
        null_byte_found = null;
        let mut category = CODING_CATEGORY_UNDECIDED;
        let mut this: Option<String> = None;
        let mut i = 0usize;
        if null_byte_found || eight_bit_found || head_ascii < src.src_bytes || info.found != 0 {
            if head_ascii == src.src_bytes {
                // All bytes are 7-bit: only the ISO-2022 findings matter.
                while i < CODING_CATEGORY_RAW_TEXT {
                    category = categories.priorities[i];
                    this = categories.representative(category).map(str::to_string);
                    if info.found & mask(category) != 0 {
                        break;
                    }
                    i += 1;
                }
            } else {
                if null_byte_found {
                    info.checked |= !MASK_UTF_16;
                    info.rejected |= !MASK_UTF_16;
                } else if spec.prefer_utf_8 && detect_utf_8(src, head_ascii, &mut info) {
                    info.checked |= !MASK_UTF_8;
                    info.rejected |= !MASK_UTF_8;
                }
                while i < CODING_CATEGORY_RAW_TEXT {
                    category = categories.priorities[i];
                    this = categories.representative(category).map(str::to_string);
                    if this.is_none() {
                        info.rejected |= mask(category);
                    } else if category >= CODING_CATEGORY_RAW_TEXT {
                    } else if info.checked & mask(category) != 0 {
                        if highest && info.found & mask(category) != 0 {
                            break;
                        }
                    } else if detector
                        .run_detector(category, src, head_ascii, &canonical, &mut info)
                        && highest
                        && info.found & mask(category) != 0
                    {
                        if category == CODING_CATEGORY_UTF_16_AUTO {
                            category = if info.found & MASK_UTF_16_LE != 0 {
                                CODING_CATEGORY_UTF_16_LE
                            } else {
                                CODING_CATEGORY_UTF_16_BE
                            };
                        }
                        break;
                    }
                    i += 1;
                }
            }
        }

        if info.rejected & MASK_ANY == MASK_ANY || null_byte_found {
            info.found = MASK_RAW_TEXT;
            val.push((CODING_CATEGORY_RAW_TEXT, "no-conversion".into()));
        } else if info.rejected == 0 && info.found == 0 {
            info.found = MASK_ANY;
            if let Some(name) = categories.representative(CODING_CATEGORY_UNDECIDED) {
                val.push((CODING_CATEGORY_UNDECIDED, name.to_string()));
            }
        } else if highest {
            if info.found != 0 {
                info.found = mask(category);
                if let Some(name) = this {
                    val.push((category, name));
                }
            } else {
                for &category in &categories.priorities[..CODING_CATEGORY_RAW_TEXT] {
                    if info.rejected & mask(category) == 0 {
                        info.found = mask(category);
                        if let Some(name) = categories.representative(category) {
                            val.push((category, name.to_string()));
                        }
                        break;
                    }
                }
            }
        } else {
            let unmasked = info.rejected | info.found;
            let mut found = 0;
            let mut unseen: Option<(usize, String)> = None;
            for &category in categories.priorities[..CODING_CATEGORY_RAW_TEXT]
                .iter()
                .rev()
            {
                if unmasked & mask(category) == 0 {
                    found |= mask(category);
                    if let Some(name) = categories.representative(category) {
                        unseen = Some((category, name.to_string()));
                    }
                }
            }
            val.extend(unseen);
            for &category in categories.priorities[..CODING_CATEGORY_RAW_TEXT]
                .iter()
                .rev()
            {
                if info.found & mask(category) != 0
                    && let Some(name) = categories.representative(category)
                {
                    val.insert(0, (category, name.to_string()));
                }
            }
            info.found |= found;
        }
    } else if base_category == CODING_CATEGORY_UTF_8_AUTO {
        if detect_utf_8(src, 0, &mut info) {
            let category = if info.found & MASK_UTF_8_SIG != 0 {
                CODING_CATEGORY_UTF_8_SIG
            } else {
                CODING_CATEGORY_UTF_8_NOSIG
            };
            if let Some(name) = categories.representative(category) {
                val.push((category, name.to_string()));
            }
        }
    } else if base_category == CODING_CATEGORY_UTF_16_AUTO {
        if detect_utf_16(src, &mut info) {
            let category = if info.found & MASK_UTF_16_LE != 0 {
                CODING_CATEGORY_UTF_16_LE
            } else if info.found & MASK_UTF_16_BE != 0 {
                CODING_CATEGORY_UTF_16_BE
            } else if info.rejected & MASK_UTF_16_LE_NOSIG != 0 {
                CODING_CATEGORY_UTF_16_BE_NOSIG
            } else {
                CODING_CATEGORY_UTF_16_LE_NOSIG
            };
            if let Some(name) = categories.representative(category) {
                val.push((category, name.to_string()));
            }
        }
    } else {
        info.found = mask(base_category);
        val.push((base_category, canonical.clone()));
    }

    // Then detect the eol format where the coding system leaves it open.
    let bytes = src.internal_bytes();
    let (normal_eol, utf_16_be_eol, utf_16_le_eol) = match requested_eol {
        None => {
            let normal = if info.found & !MASK_UTF_16 != 0 {
                if null_byte_found {
                    EOL_SEEN_LF
                } else {
                    detect_eol(&bytes)
                }
            } else {
                EOL_SEEN_NONE
            };
            let be = if info.found & (MASK_UTF_16_BE | MASK_UTF_16_BE_NOSIG) != 0 {
                detect_eol_utf_16(&bytes, false)
            } else {
                EOL_SEEN_NONE
            };
            let le = if info.found & (MASK_UTF_16_LE | MASK_UTF_16_LE_NOSIG) != 0 {
                detect_eol_utf_16(&bytes, true)
            } else {
                EOL_SEEN_NONE
            };
            (normal, be, le)
        }
        Some(0) => (EOL_SEEN_LF, EOL_SEEN_LF, EOL_SEEN_LF),
        Some(1) => (EOL_SEEN_CRLF, EOL_SEEN_CRLF, EOL_SEEN_CRLF),
        Some(_) => (EOL_SEEN_CR, EOL_SEEN_CR, EOL_SEEN_CR),
    };
    let names: Vec<String> = val
        .into_iter()
        .map(|(_, name)| {
            let category = interp
                .coding_system(&name)
                .map_or(CODING_CATEGORY_UNDECIDED, |state| state.category);
            let this_eol = match utf_16_category(category) {
                Some(false) => utf_16_be_eol,
                Some(true) => utf_16_le_eol,
                None => normal_eol,
            };
            name_with_eol(interp, &name, this_eol)
        })
        .collect();
    if highest {
        names.into_iter().take(1).collect()
    } else {
        names
    }
}

/// coding.c detect_coding, the decode-time detection: the coding system
/// the text decodes with (None when nothing decided anything, so the
/// requested coding stands) and the eol bits the head scan saw.
pub(crate) fn detect_coding(
    interp: &Interpreter,
    src: &DetectSource,
    coding: &str,
    env: &Env,
) -> Option<String> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .unwrap_or_else(|| coding.to_string());
    let state = interp.coding_system(&canonical)?;
    let detector = Detector::new(interp, env);
    let categories = &detector.categories;
    match state.kind.as_str() {
        "undecided" => {}
        _ if state.category == CODING_CATEGORY_UTF_8_AUTO => {
            // An auto-BOM utf-8: the BOM decides between the pair.
            let mut info = DetectInfo::default();
            let pair = coding_system_property(interp, &canonical, ":bom")
                .and_then(|bom| bom.cons_values())
                .and_then(|(sig, nosig)| {
                    Some((
                        sig.as_symbol().ok()?.to_string(),
                        nosig.as_symbol().ok()?.to_string(),
                    ))
                })?;
            let ascii = src.units.iter().all(|unit| (0..0x80).contains(unit));
            return Some(if ascii {
                pair.1
            } else if detect_utf_8(src, 0, &mut info) {
                if info.found & MASK_UTF_8_SIG != 0 {
                    pair.0
                } else {
                    pair.1
                }
            } else {
                return None;
            });
        }
        _ if state.category == CODING_CATEGORY_UTF_16_AUTO => {
            let mut info = DetectInfo::default();
            let pair = coding_system_property(interp, &canonical, ":bom")
                .and_then(|bom| bom.cons_values())
                .and_then(|(le, be)| {
                    Some((
                        le.as_symbol().ok()?.to_string(),
                        be.as_symbol().ok()?.to_string(),
                    ))
                })?;
            if detect_utf_16(src, &mut info) {
                if info.found & MASK_UTF_16_LE != 0 {
                    return Some(pair.0);
                } else if info.found & MASK_UTF_16_BE != 0 {
                    return Some(pair.1);
                }
            }
            return None;
        }
        _ => return None,
    }
    let spec = undecided_spec(interp, &canonical, env);
    let mut info = DetectInfo::default();
    let track_eol = !truthy_var(interp, "disable-ascii-optimization", env)
        && !truthy_var(interp, "inhibit-eol-conversion", env);
    let (head_ascii, null_byte_found, eight_bit_found, _eol_seen) =
        detector.head_scan(src, &spec, &mut info, track_eol);
    if !(null_byte_found || eight_bit_found || head_ascii < src.src_bytes || info.found != 0) {
        return None;
    }
    let mut category = CODING_CATEGORY_UNDECIDED;
    let mut this: Option<String> = None;
    let mut i = 0usize;
    if head_ascii == src.src_bytes {
        while i < CODING_CATEGORY_RAW_TEXT {
            category = categories.priorities[i];
            this = categories.representative(category).map(str::to_string);
            if info.found & mask(category) != 0 {
                break;
            }
            i += 1;
        }
    } else {
        if null_byte_found {
            info.checked |= !MASK_UTF_16;
            info.rejected |= !MASK_UTF_16;
        } else if spec.prefer_utf_8 && detect_utf_8(src, head_ascii, &mut info) {
            info.checked |= !MASK_UTF_8;
            info.rejected |= !MASK_UTF_8;
        }
        while i < CODING_CATEGORY_RAW_TEXT {
            category = categories.priorities[i];
            this = categories.representative(category).map(str::to_string);
            let Some(representative) = this.clone() else {
                info.rejected |= mask(category);
                i += 1;
                continue;
            };
            if info.checked & mask(category) != 0 {
                if info.found & mask(category) != 0 {
                    break;
                }
            } else if detector.run_detector(category, src, head_ascii, &representative, &mut info)
                && info.found & mask(category) != 0
            {
                break;
            }
            i += 1;
        }
    }
    if i < CODING_CATEGORY_RAW_TEXT {
        let this = this?;
        if category == CODING_CATEGORY_UTF_8_AUTO {
            if let Some((sig, nosig)) =
                coding_system_property(interp, &this, ":bom").and_then(|bom| bom.cons_values())
            {
                let pick = if info.found & MASK_UTF_8_SIG != 0 {
                    sig
                } else {
                    nosig
                };
                return pick.as_symbol().ok().map(str::to_string).or(Some(this));
            }
            Some(this)
        } else if category == CODING_CATEGORY_UTF_16_AUTO {
            if let Some((le, be)) =
                coding_system_property(interp, &this, ":bom").and_then(|bom| bom.cons_values())
            {
                let pick = if info.found & MASK_UTF_16_LE != 0 {
                    Some(le)
                } else if info.found & MASK_UTF_16_BE != 0 {
                    Some(be)
                } else {
                    None
                };
                return pick.and_then(|value| value.as_symbol().ok().map(str::to_string));
            }
            Some(this)
        } else {
            Some(this)
        }
    } else if null_byte_found {
        Some("no-conversion".into())
    } else if info.rejected & MASK_ANY == MASK_ANY {
        Some("raw-text".into())
    } else if info.rejected != 0 {
        categories.priorities[..CODING_CATEGORY_RAW_TEXT]
            .iter()
            .find(|category| info.rejected & mask(**category) == 0)
            .and_then(|category| categories.representative(*category))
            .map(str::to_string)
    } else {
        None
    }
}
